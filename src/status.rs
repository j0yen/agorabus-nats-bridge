//! `wm-busbridge status` — live fleet dashboard from the NATS KV registry.
//!
//! Reads every entry in the `WM_NODES` KV bucket (the constellation dispatch
//! capability registry) plus the `WM_WORK` JetStream work-queue depth, then
//! prints a human-readable table or machine-readable JSON.
//!
//! # Exit codes
//!
//! - 0 — all present nodes are `up`.
//! - 1 — at least one node is `STALE` or `DOWN`.
//! - 2 — the NATS hub is unreachable (connection timeout).

// JetStream is a proper noun; the subcommand intentionally prints to stdout.
#![allow(clippy::doc_markdown)]
#![allow(clippy::print_stdout)]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::dispatch::{NodeCapability, NodeRole, NODES_BUCKET, WORK_STREAM};

// ─── public options ──────────────────────────────────────────────────────────

/// Options forwarded from the CLI to [`run`].
#[derive(Debug, Clone)]
pub struct StatusOptions {
    /// NATS server URL (e.g. `nats://127.0.0.1:4222`).
    pub nats_url: String,
    /// Age threshold (seconds) beyond which a node is considered `STALE`.
    pub stale_after_secs: u64,
    /// Emit JSON instead of the human-readable table.
    pub json: bool,
    /// Optional path to `fleet-nodes.conf` (for expected-node list).
    pub fleet_nodes_conf: Option<String>,
}

impl Default for StatusOptions {
    fn default() -> Self {
        Self {
            nats_url: "nats://127.0.0.1:4222".to_string(),
            stale_after_secs: 30,
            json: false,
            fleet_nodes_conf: None,
        }
    }
}

// ─── output data model ───────────────────────────────────────────────────────

/// Per-node health classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeStatus {
    /// Heartbeat is recent and the node is reachable.
    Up,
    /// Heartbeat is older than `stale_after_secs`.
    Stale,
    /// Expected node has no entry in the KV bucket at all.
    Down,
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Up => f.write_str("up"),
            Self::Stale => f.write_str("STALE"),
            Self::Down => f.write_str("DOWN"),
        }
    }
}

/// Flattened row as it appears in both the table and the JSON output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRow {
    /// Node name (KV key without the `node.` prefix).
    pub name: String,
    /// Semantic role string (e.g. `cloud_worker`, `laptop`).
    pub role: String,
    /// Logical CPU count.
    pub cores: u32,
    /// Total RAM in GiB.
    pub ram_gb: u32,
    /// 1-minute load average.
    pub load1: f64,
    /// Whether a GPU is present.
    pub gpu: bool,
    /// Seconds since the last heartbeat (`None` when the node is `DOWN`).
    pub hb_age_secs: Option<u64>,
    /// Health classification.
    pub status: NodeStatus,
}

/// Work-queue statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkQueueStats {
    /// Number of messages waiting to be claimed.
    pub pending: u64,
    /// Number of messages currently in-flight (claimed but not yet acked).
    pub in_flight: u64,
}

/// Top-level JSON output structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusReport {
    /// Per-node rows.
    pub nodes: Vec<NodeRow>,
    /// Work-queue depth.
    pub work_queue: WorkQueueStats,
}

// ─── role display helper ──────────────────────────────────────────────────────

fn role_display(cap: &NodeCapability) -> String {
    cap.role
        .as_ref()
        .map(|r| match r {
            NodeRole::LocalLlm => "local_llm",
            NodeRole::CloudWorker => "cloud_worker",
            NodeRole::Laptop => "laptop",
            NodeRole::GpuInfer => "gpu_infer",
        })
        .unwrap_or("unknown")
        .to_string()
}

// ─── pure business logic (testable without NATS) ─────────────────────────────

/// Build a [`NodeRow`] from a `(name, capability)` pair.
///
/// `now_secs` is the caller's wall-clock time so tests can inject arbitrary
/// timestamps without touching system time.
#[must_use]
pub fn make_node_row(name: &str, cap: &NodeCapability, now_secs: u64, stale_after_secs: u64) -> NodeRow {
    let hb_age_secs = now_secs.saturating_sub(cap.ts);
    let status = if cap.is_stale(now_secs, stale_after_secs) {
        NodeStatus::Stale
    } else {
        NodeStatus::Up
    };
    NodeRow {
        name: name.to_string(),
        role: role_display(cap),
        cores: cap.cores,
        ram_gb: cap.ram_gb,
        load1: cap.load1,
        gpu: cap.gpu,
        hb_age_secs: Some(hb_age_secs),
        status,
    }
}

/// Build a synthetic `DOWN` row for an expected node that has no KV entry.
#[must_use]
pub fn make_down_row(name: &str) -> NodeRow {
    NodeRow {
        name: name.to_string(),
        role: "unknown".to_string(),
        cores: 0,
        ram_gb: 0,
        load1: 0.0,
        gpu: false,
        hb_age_secs: None,
        status: NodeStatus::Down,
    }
}

/// Render the [`StatusReport`] as a human-readable table.
///
/// Uses `writeln!` so the caller can redirect output; returns the formatted
/// string so tests can inspect it without side effects.
///
/// # Errors
///
/// Returns `Err` if `write!` / `writeln!` fails (practically infallible for
/// `String`, but the signature is required by `write!`).
pub fn render_table(report: &StatusReport) -> Result<String> {
    use std::fmt::Write as FmtWrite;
    let mut out = String::with_capacity(512);

    // Header
    writeln!(
        out,
        "{:<16} {:<12} {:>5} {:>7} {:>6} {:>4} {:>8}  {}",
        "node", "role", "cores", "ram", "load", "gpu", "hb-age", "status"
    )?;

    for row in &report.nodes {
        let ram_str = format!("{}Gi", row.ram_gb);
        let gpu_str = if row.gpu { "yes" } else { "none" };
        let age_str = row.hb_age_secs.map_or_else(|| "—".to_string(), |s| format!("{s}s"));
        writeln!(
            out,
            "{:<16} {:<12} {:>5} {:>7} {:>6.1} {:>4} {:>8}  {}",
            row.name, row.role, row.cores, ram_str, row.load1, gpu_str, age_str, row.status,
        )?;
    }

    // Separator + queue footer
    writeln!(out, "{}", "─".repeat(70))?;
    writeln!(
        out,
        "work-queue: {} pending · {} in-flight",
        report.work_queue.pending, report.work_queue.in_flight,
    )?;

    Ok(out)
}

/// Returns the appropriate process exit code for a [`StatusReport`]:
/// - 0 — all nodes up.
/// - 1 — at least one `STALE` or `DOWN` node.
#[must_use]
pub fn exit_code_for_report(report: &StatusReport) -> i32 {
    let any_unhealthy = report
        .nodes
        .iter()
        .any(|n| matches!(n.status, NodeStatus::Stale | NodeStatus::Down));
    if any_unhealthy { 1 } else { 0 }
}

// ─── NATS-connected entry point ───────────────────────────────────────────────

/// Connect to NATS, read the KV + work-queue, build + emit the report,
/// and return the appropriate exit code.
///
/// # Errors
///
/// Returns `Err` only for hard failures (serialize, render). Hub-unreachable
/// is handled by returning exit code 2 with a printed message.
pub async fn run(opts: StatusOptions) -> Result<i32> {
    // 5-second connect timeout so we don't hang forever when the hub is down.
    let connect_timeout = Duration::from_secs(5);
    let nc_result = tokio::time::timeout(
        connect_timeout,
        async_nats::connect(&opts.nats_url),
    )
    .await;

    let client = match nc_result {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => {
            println!("hub unreachable: {e}");
            return Ok(2);
        }
        Err(_elapsed) => {
            println!("hub unreachable: connection timed out after {}s", connect_timeout.as_secs());
            return Ok(2);
        }
    };

    let js = async_nats::jetstream::new(client);

    // ── read WM_NODES KV ──────────────────────────────────────────────────

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let mut rows: Vec<NodeRow> = Vec::new();

    match js.get_key_value(NODES_BUCKET).await {
        Ok(kv) => {
            // Collect all keys to iterate
            use futures::StreamExt as _;
            let mut keys = kv.keys().await?;
            while let Some(key_result) = keys.next().await {
                let key = key_result?;
                if let Ok(Some(entry)) = kv.get(&key).await {
                    match serde_json::from_slice::<NodeCapability>(&entry) {
                        Ok(cap) => {
                            // Strip "node." prefix from the key if present
                            let display_name = key
                                .strip_prefix("node.")
                                .unwrap_or(&key)
                                .to_string();
                            rows.push(make_node_row(&display_name, &cap, now_secs, opts.stale_after_secs));
                        }
                        Err(e) => {
                            tracing::warn!(key = %key, error = %e, "failed to decode NodeCapability");
                        }
                    }
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "WM_NODES KV bucket not found or inaccessible");
        }
    }

    // Add expected-but-missing DOWN rows if fleet-nodes.conf is available
    if let Some(conf_path) = &opts.fleet_nodes_conf {
        if let Ok(text) = std::fs::read_to_string(conf_path) {
            for line in text.lines() {
                let name = line.trim();
                if name.is_empty() || name.starts_with('#') {
                    continue;
                }
                if !rows.iter().any(|r| r.name == name) {
                    rows.push(make_down_row(name));
                }
            }
        }
    }

    // Sort by name for stable output
    rows.sort_by(|a, b| a.name.cmp(&b.name));

    // ── read WM_WORK queue depth ──────────────────────────────────────────

    let wq = read_work_queue_stats(&js).await;

    let report = StatusReport { nodes: rows, work_queue: wq };

    // ── emit output ───────────────────────────────────────────────────────

    if opts.json {
        let json = serde_json::to_string_pretty(&report)?;
        println!("{json}");
    } else {
        let table = render_table(&report)?;
        print!("{table}");
    }

    Ok(exit_code_for_report(&report))
}

/// Read pending + in-flight counts from the `WM_WORK` JetStream stream.
///
/// Degrades gracefully to zeros when the stream does not exist.
async fn read_work_queue_stats(js: &async_nats::jetstream::Context) -> WorkQueueStats {
    match js.get_stream(WORK_STREAM).await {
        Ok(mut stream) => {
            match stream.info().await {
                Ok(info) => {
                    let state = &info.state;
                    // For a work-queue stream, `messages` is the total pending count.
                    // Consumer-level in-flight tracking requires a consumer query;
                    // degrade gracefully to 0 for now.
                    let pending = state.messages;
                    let in_flight = 0u64;
                    WorkQueueStats { pending, in_flight }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to read WM_WORK stream info");
                    WorkQueueStats { pending: 0, in_flight: 0 }
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "WM_WORK stream not found");
            WorkQueueStats { pending: 0, in_flight: 0 }
        }
    }
}

// ─── unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::{NodeCapability, NodeRole};

    fn make_cap(ts: u64, load1: f64, cores: u32, role: Option<NodeRole>) -> NodeCapability {
        NodeCapability {
            cores,
            ram_gb: 16,
            gpu: false,
            vram_gb: 0,
            load1,
            queue_depth: 0,
            ts,
            no_build: false,
            role,
        }
    }

    // ── AC2: table rendering ─────────────────────────────────────────────────

    #[test]
    fn table_includes_all_nodes() {
        let report = StatusReport {
            nodes: vec![
                NodeRow {
                    name: "nomad".to_string(),
                    role: "laptop".to_string(),
                    cores: 8,
                    ram_gb: 15,
                    load1: 0.4,
                    gpu: false,
                    hb_age_secs: Some(3),
                    status: NodeStatus::Up,
                },
                NodeRow {
                    name: "hub".to_string(),
                    role: "cloud_worker".to_string(),
                    cores: 4,
                    ram_gb: 8,
                    load1: 0.1,
                    gpu: false,
                    hb_age_secs: Some(2),
                    status: NodeStatus::Up,
                },
                NodeRow {
                    name: "forge".to_string(),
                    role: "cloud_worker".to_string(),
                    cores: 16,
                    ram_gb: 32,
                    load1: 7.2,
                    gpu: false,
                    hb_age_secs: Some(48),
                    status: NodeStatus::Stale,
                },
            ],
            work_queue: WorkQueueStats { pending: 2, in_flight: 1 },
        };

        let table = render_table(&report).expect("render must succeed");
        assert!(table.contains("nomad"), "table must include nomad");
        assert!(table.contains("hub"), "table must include hub");
        assert!(table.contains("forge"), "table must include forge");
        assert!(table.contains("role"), "header must include role column");
        assert!(table.contains("cores"), "header must include cores column");
        assert!(table.contains("hb-age"), "header must include hb-age column");
        assert!(table.contains("2 pending"), "footer must show pending count");
        assert!(table.contains("1 in-flight"), "footer must show in-flight count");
    }

    // ── AC3: stale detection forces exit 1 ───────────────────────────────────

    #[test]
    fn stale_node_detected_and_forces_exit_1() {
        let now = 1_000u64;
        let stale_after = 30u64;

        // ts = 900 → age 100s > 30s → STALE
        let stale_cap = make_cap(900, 0.5, 4, None);
        // ts = 990 → age 10s < 30s → up
        let fresh_cap = make_cap(990, 0.1, 4, None);

        let stale_row = make_node_row("forge", &stale_cap, now, stale_after);
        let fresh_row = make_node_row("hub", &fresh_cap, now, stale_after);

        assert_eq!(stale_row.status, NodeStatus::Stale, "forge must be STALE");
        assert_eq!(fresh_row.status, NodeStatus::Up, "hub must be up");

        let report = StatusReport {
            nodes: vec![stale_row, fresh_row],
            work_queue: WorkQueueStats { pending: 0, in_flight: 0 },
        };
        assert_eq!(exit_code_for_report(&report), 1, "any STALE node must give exit 1");
    }

    #[test]
    fn all_up_nodes_give_exit_0() {
        let now = 1_000u64;
        let cap = make_cap(990, 0.1, 4, None); // age = 10s < 30s → up
        let row = make_node_row("alpha", &cap, now, 30);
        assert_eq!(row.status, NodeStatus::Up);
        let report = StatusReport {
            nodes: vec![row],
            work_queue: WorkQueueStats { pending: 0, in_flight: 0 },
        };
        assert_eq!(exit_code_for_report(&report), 0);
    }

    #[test]
    fn down_node_forces_exit_1() {
        let report = StatusReport {
            nodes: vec![make_down_row("missing-node")],
            work_queue: WorkQueueStats { pending: 0, in_flight: 0 },
        };
        assert_eq!(exit_code_for_report(&report), 1, "DOWN node must give exit 1");
    }

    // ── AC4: JSON output round-trips ─────────────────────────────────────────

    #[test]
    fn json_output_round_trips() {
        let report = StatusReport {
            nodes: vec![NodeRow {
                name: "alpha".to_string(),
                role: "cloud_worker".to_string(),
                cores: 8,
                ram_gb: 16,
                load1: 0.5,
                gpu: false,
                hb_age_secs: Some(5),
                status: NodeStatus::Up,
            }],
            work_queue: WorkQueueStats { pending: 3, in_flight: 1 },
        };

        let json_str = serde_json::to_string(&report).expect("serialize must succeed");
        let parsed: StatusReport = serde_json::from_str(&json_str).expect("round-trip must succeed");

        assert_eq!(parsed.nodes.len(), 1);
        assert_eq!(parsed.nodes[0].name, "alpha");
        assert_eq!(parsed.work_queue.pending, 3);
        assert_eq!(parsed.work_queue.in_flight, 1);
    }

    #[test]
    fn json_contains_nodes_and_work_queue_keys() {
        let report = StatusReport {
            nodes: vec![],
            work_queue: WorkQueueStats { pending: 0, in_flight: 0 },
        };
        let json_str = serde_json::to_string(&report).expect("serialize must succeed");
        assert!(json_str.contains("\"nodes\""), "JSON must have 'nodes' key");
        assert!(json_str.contains("\"work_queue\""), "JSON must have 'work_queue' key");
    }

    // ── AC6: SIGPIPE safety (structural) ────────────────────────────────────

    /// SIGPIPE safety is ensured by `sigpipe::reset()` at the top of `main()`.
    /// This test verifies that `render_table` itself returns a `Result`, so
    /// the output path can propagate errors without panic.
    #[test]
    fn render_table_returns_result_not_panics() {
        let report = StatusReport {
            nodes: vec![],
            work_queue: WorkQueueStats { pending: 0, in_flight: 0 },
        };
        let result = render_table(&report);
        assert!(result.is_ok(), "render_table must return Ok, not panic");
    }

    // ── make_node_row edge cases ─────────────────────────────────────────────

    #[test]
    fn node_row_role_display() {
        let cap = make_cap(0, 0.0, 1, Some(NodeRole::LocalLlm));
        let row = make_node_row("llm", &cap, 100, 30);
        assert_eq!(row.role, "local_llm");
    }

    #[test]
    fn node_row_no_role_displays_unknown() {
        let cap = make_cap(0, 0.0, 1, None);
        let row = make_node_row("anon", &cap, 100, 30);
        assert_eq!(row.role, "unknown");
    }

    #[test]
    fn node_row_hb_age_computed_correctly() {
        let cap = make_cap(950, 0.0, 1, None);
        let row = make_node_row("n", &cap, 1000, 30);
        assert_eq!(row.hb_age_secs, Some(50));
    }
}
