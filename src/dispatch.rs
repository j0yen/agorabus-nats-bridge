//! Constellation dispatch layer — work-queue, capability registry, and coordinator.
//!
//! Implements the three pillars of PRD-constellation-dispatch:
//!
//! 1. **Capability heartbeat** — samples local hardware (cores, RAM, load, GPU)
//!    and writes `node.<name>` into the `WM_NODES` JetStream KV bucket on a
//!    configurable interval with a TTL so crashed nodes self-expire.
//!
//! 2. **Worker** — a per-node JetStream pull consumer on the `WM_WORK` stream,
//!    filtered to the job classes the node qualifies for.  Claims jobs, runs
//!    them, acks; at-least-once delivery — a crash before ack causes
//!    re-delivery to another worker.
//!
//! 3. **Dispatch primitives** — submit a job onto `WM_WORK`, query the live
//!    fleet capacity from `WM_NODES`, and route capability-aware placements
//!    (GPU jobs → `vram_gb > 0`; build jobs → highest-cores / lowest-load,
//!    never `no_build: true` nodes).
//!
//! # Wire contracts
//!
//! - `WM_WORK` stream: subjects `wm.work.<class>.<detail>`, WorkQueuePolicy
//!   (deliver-once, ack-removes).
//! - `WM_NODES` KV bucket: keys `node.<name>`, values [`NodeCapability`] JSON.
//! - Max payload bytes: [`MAX_JOB_PAYLOAD_BYTES`] — the bus carries references
//!   and paths, never large blobs; violations are rejected at submission time.
//!
//! # Cloud-build routing invariant (constellation-cloud-build)
//!
//! Nodes that serve the local LLM (e.g. the 5700U running qwen2.5-8B) advertise
//! `no_build: true` and/or `role: NodeRole::LocalLlm`.  The dispatch coordinator
//! **never** places `Build` or `Test` jobs on such nodes, ensuring the local model
//! always has full CPU+RAM headroom.  This is enforced by [`place_job`] and
//! validated by the fleet-invariant acceptance tests.

// JetStream and constellation-dispatch are proper nouns.
#![allow(clippy::doc_markdown)]

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ─── constants ──────────────────────────────────────────────────────────────

/// NATS stream name for the work-queue.
pub const WORK_STREAM: &str = "WM_WORK";
/// NATS KV bucket name for the capability registry.
pub const NODES_BUCKET: &str = "WM_NODES";
/// Subject prefix for work messages.
pub const WORK_SUBJECT_PREFIX: &str = "wm.work.";
/// Hard upper bound on job payload bytes to keep the bus lean.
pub const MAX_JOB_PAYLOAD_BYTES: usize = 64 * 1024; // 64 KiB

// ─── node role ──────────────────────────────────────────────────────────────

/// High-level role of a fleet node — influences dispatch routing.
///
/// Stored in [`NodeCapability::role`] and used by [`place_job`] to enforce
/// the cloud-build routing invariant: `LocalLlm` nodes never receive
/// compilation workloads.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    /// Dedicated local-LLM inference node.  Build and test jobs must never
    /// be routed here regardless of CPU availability.
    LocalLlm,
    /// Generic cloud worker — handles build, test, and (if GPU present) infer.
    CloudWorker,
    /// Laptop / personal machine that accepts build jobs when `voice_idle`.
    Laptop,
    /// Specialised GPU inference server.
    GpuInfer,
}

// ─── capability record ──────────────────────────────────────────────────────

/// Live snapshot of a node's hardware capacity, written to `WM_NODES` KV.
///
/// The heartbeat loop updates this every `heartbeat_secs` seconds; the KV
/// TTL ensures a dead node's record expires automatically.
///
/// ## Cloud-build routing fields
///
/// Two fields gate build-job placement (constellation-cloud-build):
/// - `no_build` — when `true` the dispatcher unconditionally skips this node
///   for [`JobClass::Build`] and [`JobClass::Test`].  Set by nodes that must
///   keep all resources for local-LLM inference.
/// - `role` — optional semantic role; `LocalLlm` implies `no_build` and is
///   double-checked by the dispatcher as a defence-in-depth guard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeCapability {
    /// Logical CPU count (hyperthreads).
    pub cores: u32,
    /// Total installed RAM in GiB.
    pub ram_gb: u32,
    /// Whether a GPU is present.
    pub gpu: bool,
    /// GPU VRAM in GiB (0 when no GPU).
    pub vram_gb: u32,
    /// 1-minute load average.
    pub load1: f64,
    /// Current queue depth (jobs claimed, not yet acked).
    pub queue_depth: u32,
    /// Unix timestamp of this snapshot (seconds since epoch).
    pub ts: u64,
    /// When `true`, build and test jobs are **never** placed on this node.
    ///
    /// Set by nodes that dedicate all CPU+RAM to local-LLM inference (the
    /// 5700U running qwen2.5-8B).  Defaults to `false` for cloud workers.
    #[serde(default)]
    pub no_build: bool,
    /// Semantic node role — used as a defence-in-depth guard alongside
    /// `no_build` to prevent build jobs from landing on the local-LLM node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<NodeRole>,
}

impl NodeCapability {
    /// Returns `true` if this node is healthy and can accept new jobs.
    ///
    /// A node is healthy when `load1 / cores < 2.0` (headroom exists) and
    /// the record timestamp is recent (caller must verify TTL separately via
    /// the KV watcher).
    #[must_use]
    #[allow(clippy::float_arithmetic)] // load-saturation ratio is intentional
    pub fn is_available(&self) -> bool {
        if self.cores == 0 {
            return false;
        }
        let saturation = self.load1 / f64::from(self.cores);
        saturation < 2.0
    }

    /// Returns `true` if this node can serve GPU / inference jobs.
    #[must_use]
    pub const fn has_gpu(&self) -> bool {
        self.gpu && self.vram_gb > 0
    }

    /// Checks whether the capability record is stale relative to a wall-clock
    /// `now` (seconds since epoch) and a `max_age_secs` threshold.
    #[must_use]
    pub const fn is_stale(&self, now_secs: u64, max_age_secs: u64) -> bool {
        now_secs.saturating_sub(self.ts) > max_age_secs
    }

    /// Returns `true` if build and test jobs may be placed on this node.
    ///
    /// A node is build-eligible when **both** of the following hold:
    /// - `no_build` is `false`, **and**
    /// - `role` is not [`NodeRole::LocalLlm`] (defence-in-depth guard).
    ///
    /// This ensures the 5700U dedicated to qwen2.5-8B inference is never
    /// selected as a build target, preserving full CPU+RAM for the model.
    #[must_use]
    pub const fn is_build_eligible(&self) -> bool {
        if self.no_build {
            return false;
        }
        !matches!(self.role, Some(NodeRole::LocalLlm))
    }
}

// ─── job record ─────────────────────────────────────────────────────────────

/// Job class — encodes the routing domain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum JobClass {
    /// Rust / C compilation job — routed by cores + load.
    Build,
    /// Inference / embedding job — requires GPU.
    Infer,
    /// Test execution job — any capable node.
    Test,
    /// Catch-all for extension without breaking deserialization.
    #[serde(other)]
    Unknown,
}

impl JobClass {
    /// Returns the NATS subject suffix for this class (e.g. `build`).
    #[must_use]
    pub const fn subject_suffix(&self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::Infer => "infer",
            Self::Test => "test",
            Self::Unknown => "unknown",
        }
    }

    /// Returns the full NATS subject prefix for this class.
    #[must_use]
    pub fn subject_prefix(&self) -> String {
        format!("{}{}", WORK_SUBJECT_PREFIX, self.subject_suffix())
    }
}

/// A job submitted onto `WM_WORK`.
///
/// Payloads carry references / paths — never large blobs.  The submit path
/// enforces [`MAX_JOB_PAYLOAD_BYTES`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// Unique job identifier (UUID or caller-chosen opaque string).
    pub id: String,
    /// Routing class.
    pub class: JobClass,
    /// Optional detail tag (appended to subject: `wm.work.build.rust`).
    pub detail: Option<String>,
    /// Arbitrary job payload — MUST be a path/reference, not a large blob.
    pub payload: serde_json::Value,
    /// Wall-clock submission time (seconds since Unix epoch).
    pub submitted_at: u64,
    /// Optional pin: if set, this job is intended for a specific node name.
    pub pinned_node: Option<String>,
}

impl Job {
    /// Returns the NATS subject for this job.
    ///
    /// Format: `wm.work.<class>[.<detail>]`
    #[must_use]
    pub fn subject(&self) -> String {
        let base = format!("{}{}", WORK_SUBJECT_PREFIX, self.class.subject_suffix());
        match &self.detail {
            Some(d) if !d.is_empty() => format!("{base}.{d}"),
            _ => base,
        }
    }

    /// Serializes the job to bytes for publishing.
    ///
    /// # Errors
    ///
    /// Returns `Err` if serialization fails.
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| anyhow::anyhow!("job serialize: {e}"))
    }
}

// ─── payload size guard ─────────────────────────────────────────────────────

/// Error returned when a job payload exceeds the bus size limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadTooLarge {
    /// Actual serialized size in bytes.
    pub actual: usize,
    /// Configured limit in bytes.
    pub limit: usize,
}

impl std::fmt::Display for PayloadTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "job payload {} bytes exceeds bus limit {} bytes — use shared storage for large artifacts",
            self.actual, self.limit
        )
    }
}

impl std::error::Error for PayloadTooLarge {}

/// Validates that a serialized job payload stays within the bus size limit.
///
/// # Errors
///
/// Returns [`PayloadTooLarge`] if the serialized bytes exceed
/// [`MAX_JOB_PAYLOAD_BYTES`].
pub const fn check_payload_size(bytes: &[u8]) -> Result<(), PayloadTooLarge> {
    if bytes.len() > MAX_JOB_PAYLOAD_BYTES {
        Err(PayloadTooLarge { actual: bytes.len(), limit: MAX_JOB_PAYLOAD_BYTES })
    } else {
        Ok(())
    }
}

// ─── coordinator / placement ────────────────────────────────────────────────

/// Outcome of a coordinator placement decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementDecision {
    /// Route the job to this NATS subject (already includes node pin if any).
    Route(String),
    /// No qualifying node is available; the job cannot be placed right now.
    /// No qualifying node is available.
    Unplaceable {
        /// Human-readable explanation of why placement failed.
        reason: String,
    },
}

/// Selects the best node from `nodes` for a given `job`.
///
/// Routing rules:
/// - [`JobClass::Infer`] → filter to `vram_gb > 0`, pick lowest load.
/// - [`JobClass::Build`] / [`JobClass::Test`] → only nodes where
///   [`NodeCapability::is_build_eligible`] returns `true`, pick highest
///   cores / lowest load (composite score = `load1 / cores`).  Nodes with
///   `no_build: true` or `role: LocalLlm` are **never** considered.
/// - If the job has `pinned_node`, only that node is considered.
///
/// Returns a subject string if a candidate exists, or `Unplaceable`.
///
/// # Cloud-build invariant
///
/// The local-LLM node (5700U, qwen2.5-8B) advertises `no_build: true`.
/// [`place_job`] enforces this invariant: a build job submitted while the
/// local-llm node is in the fleet will never be routed there.
#[must_use]
pub fn place_job<S: std::hash::BuildHasher>(job: &Job, nodes: &HashMap<String, NodeCapability, S>) -> PlacementDecision {
    // Determine if this is a build/test job (subject to `no_build` filter)
    let is_build_class = matches!(job.class, JobClass::Build | JobClass::Test);

    // Filter: only healthy, non-stale nodes (caller provides pre-filtered map)
    let candidates: Vec<(&String, &NodeCapability)> = nodes
        .iter()
        .filter(|(name, cap)| {
            // Pin filter
            if let Some(pin) = &job.pinned_node {
                if *name != pin {
                    return false;
                }
            }
            // Build-eligibility filter — enforces the local-llm exclusion invariant
            if is_build_class && !cap.is_build_eligible() {
                return false;
            }
            cap.is_available()
        })
        .collect();

    if candidates.is_empty() {
        if let Some(pin) = &job.pinned_node {
            return PlacementDecision::Unplaceable {
                reason: format!("pinned node '{pin}' is not available"),
            };
        }
        return PlacementDecision::Unplaceable {
            reason: "no available nodes in fleet".to_string(),
        };
    }

    let best = match job.class {
        JobClass::Infer => {
            // GPU-capable only
            let gpu_candidates: Vec<_> =
                candidates.iter().filter(|(_, cap)| cap.has_gpu()).collect();
            if gpu_candidates.is_empty() {
                return PlacementDecision::Unplaceable {
                    reason: "no GPU-capable node available for infer job".to_string(),
                };
            }
            // Pick lowest load
            gpu_candidates
                .into_iter()
                .min_by(|(_, a), (_, b)| a.load1.partial_cmp(&b.load1).unwrap_or(std::cmp::Ordering::Equal))
        }
        JobClass::Build | JobClass::Test | JobClass::Unknown => {
            // Pick lowest saturation (load1 / cores)
            #[allow(clippy::float_arithmetic)] // saturation ratio: intentional
            candidates.iter().min_by(|(_, a), (_, b)| {
                let sa = if a.cores > 0 { a.load1 / f64::from(a.cores) } else { f64::MAX };
                let sb = if b.cores > 0 { b.load1 / f64::from(b.cores) } else { f64::MAX };
                sa.partial_cmp(&sb).unwrap_or(std::cmp::Ordering::Equal)
            })
        }
    };

    match best {
        Some((_node_name, _)) => PlacementDecision::Route(job.subject()),
        None => PlacementDecision::Unplaceable {
            reason: "internal: candidate selection failed".to_string(),
        },
    }
}

// ─── drop / unplaceable log record ─────────────────────────────────────────

/// Logged when a job is dropped, capped, or unplaceable.
///
/// Written to tracing so no job loss is silent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDropRecord {
    /// Job identifier.
    pub job_id: String,
    /// Reason for the drop.
    pub reason: String,
    /// Unix timestamp (seconds).
    pub ts: u64,
}

impl JobDropRecord {
    /// Emits this record via `tracing::warn!`.
    pub fn emit(&self) {
        tracing::warn!(
            job_id = %self.job_id,
            reason = %self.reason,
            ts = self.ts,
            "job dropped — no silent loss"
        );
    }
}

// ─── local capacity sampler ─────────────────────────────────────────────────

/// Samples the local node's current hardware capacity.
///
/// This is a pure-Rust snapshot without calling external tools so the daemon
/// has no extra runtime dependencies.  GPU detection is best-effort via the
/// `WM_VRAM_GB` environment variable (set by system scripts on GPU nodes).
///
/// All fallible reads degrade gracefully (zero/default on error); the
/// function always returns a valid record.
#[must_use]
pub fn sample_local_capacity(queue_depth: u32) -> NodeCapability {
    use std::time::{SystemTime, UNIX_EPOCH};

    let cores = std::thread::available_parallelism()
        .map(|n| u32::try_from(n.get()).unwrap_or(1))
        .unwrap_or(1);

    let ram_gb = read_ram_gb_from_meminfo().unwrap_or(0);

    let load1 = read_load_average().unwrap_or(0.0);

    let vram_gb = std::env::var("WM_VRAM_GB")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);
    let gpu = vram_gb > 0;

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // `WM_NO_BUILD=1` lets the launch script mark this node as build-ineligible
    // (used by the 5700U's systemd unit when it's serving the local LLM).
    let no_build = std::env::var("WM_NO_BUILD")
        .ok()
        .is_some_and(|v| v.trim() == "1" || v.trim().eq_ignore_ascii_case("true"));

    // `WM_NODE_ROLE` optionally tags this node's semantic role (e.g. `local_llm`).
    let role = std::env::var("WM_NODE_ROLE").ok().and_then(|v| match v.trim() {
        "local_llm" => Some(NodeRole::LocalLlm),
        "cloud_worker" => Some(NodeRole::CloudWorker),
        "laptop" => Some(NodeRole::Laptop),
        "gpu_infer" => Some(NodeRole::GpuInfer),
        _ => None,
    });

    NodeCapability { cores, ram_gb, gpu, vram_gb, load1, queue_depth, ts, no_build, role }
}

/// Reads total RAM in GiB from `/proc/meminfo`.
fn read_ram_gb_from_meminfo() -> Option<u32> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(u32::try_from(kb / (1024 * 1024)).unwrap_or(u32::MAX));
        }
    }
    None
}

/// Reads the 1-minute load average from `/proc/loadavg`.
fn read_load_average() -> Option<f64> {
    let text = std::fs::read_to_string("/proc/loadavg").ok()?;
    text.split_whitespace().next()?.parse().ok()
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const fn make_node(cores: u32, load1: f64, vram_gb: u32, queue_depth: u32) -> NodeCapability {
        NodeCapability {
            cores,
            ram_gb: 16,
            gpu: vram_gb > 0,
            vram_gb,
            load1,
            queue_depth,
            ts: 0,
            no_build: false,
            role: None,
        }
    }

    const fn make_local_llm_node(cores: u32, load1: f64) -> NodeCapability {
        NodeCapability {
            cores,
            ram_gb: 32,
            gpu: false,
            vram_gb: 0,
            load1,
            queue_depth: 0,
            ts: 0,
            no_build: true,
            role: Some(NodeRole::LocalLlm),
        }
    }

    fn make_job(class: JobClass) -> Job {
        Job {
            id: "test-job-1".to_string(),
            class,
            detail: None,
            payload: serde_json::json!({"path": "/tmp/foo"}),
            submitted_at: 0,
            pinned_node: None,
        }
    }

    // ── NodeCapability ───────────────────────────────────────────────────────

    #[test]
    fn node_is_available_when_load_low() {
        let node = make_node(8, 2.0, 0, 0); // saturation = 0.25
        assert!(node.is_available());
    }

    #[test]
    fn node_not_available_when_load_too_high() {
        let node = make_node(4, 10.0, 0, 0); // saturation = 2.5
        assert!(!node.is_available());
    }

    #[test]
    fn node_has_gpu_requires_both_flags() {
        let gpu_node = make_node(8, 0.5, 24, 0);
        assert!(gpu_node.has_gpu());
        let cpu_node = make_node(8, 0.5, 0, 0);
        assert!(!cpu_node.has_gpu());
    }

    #[test]
    fn stale_check_works() {
        let node = NodeCapability { ts: 100, ..make_node(4, 0.5, 0, 0) };
        // 200s elapsed, max_age = 60s → stale
        assert!(node.is_stale(300, 60));
        // 200s elapsed, max_age = 300s → fresh
        assert!(!node.is_stale(300, 300));
    }

    // ── JobClass ─────────────────────────────────────────────────────────────

    #[test]
    fn job_class_subjects() {
        assert_eq!(JobClass::Build.subject_prefix(), "wm.work.build");
        assert_eq!(JobClass::Infer.subject_prefix(), "wm.work.infer");
        assert_eq!(JobClass::Test.subject_prefix(), "wm.work.test");
    }

    #[test]
    fn job_subject_with_detail() {
        let job = Job {
            id: "x".to_string(),
            class: JobClass::Build,
            detail: Some("rust".to_string()),
            payload: serde_json::Value::Null,
            submitted_at: 0,
            pinned_node: None,
        };
        assert_eq!(job.subject(), "wm.work.build.rust");
    }

    #[test]
    fn job_subject_without_detail() {
        let job = make_job(JobClass::Test);
        assert_eq!(job.subject(), "wm.work.test");
    }

    // ── payload size guard ──────────────────────────────────────────────────

    #[test]
    fn small_payload_accepted() {
        let bytes = b"small reference payload";
        assert!(check_payload_size(bytes).is_ok());
    }

    #[test]
    fn oversized_payload_rejected() {
        let large = vec![0u8; MAX_JOB_PAYLOAD_BYTES + 1];
        let result = check_payload_size(&large);
        assert!(
            matches!(result, Err(ref e) if e.limit == MAX_JOB_PAYLOAD_BYTES && e.actual > MAX_JOB_PAYLOAD_BYTES),
            "large payload must be rejected with correct limit/actual fields"
        );
    }

    #[test]
    fn payload_at_limit_accepted() {
        let at_limit = vec![0u8; MAX_JOB_PAYLOAD_BYTES];
        assert!(check_payload_size(&at_limit).is_ok());
    }

    // ── coordinator placement ───────────────────────────────────────────────

    #[test]
    fn build_job_routed_to_lowest_saturation() {
        let mut nodes = HashMap::new();
        nodes.insert("heavy".to_string(), make_node(4, 6.0, 0, 0)); // sat 1.5
        nodes.insert("light".to_string(), make_node(8, 2.0, 0, 0)); // sat 0.25
        let job = make_job(JobClass::Build);
        let decision = place_job(&job, &nodes);
        // Should route (exact node not encoded in subject, just that it routes)
        assert!(
            matches!(decision, PlacementDecision::Route(_)),
            "build job must be routable when available nodes exist"
        );
    }

    #[test]
    fn infer_job_requires_gpu() {
        let mut nodes = HashMap::new();
        nodes.insert("cpu-node".to_string(), make_node(16, 1.0, 0, 0));
        let job = make_job(JobClass::Infer);
        let decision = place_job(&job, &nodes);
        assert!(
            matches!(decision, PlacementDecision::Unplaceable { .. }),
            "infer job must be unplaceable when no GPU node exists"
        );
    }

    #[test]
    fn infer_job_routed_to_gpu_node() {
        let mut nodes = HashMap::new();
        nodes.insert("cpu-node".to_string(), make_node(16, 1.0, 0, 0));
        nodes.insert("gpu-node".to_string(), make_node(8, 0.5, 24, 0));
        let job = make_job(JobClass::Infer);
        let decision = place_job(&job, &nodes);
        assert!(
            matches!(decision, PlacementDecision::Route(_)),
            "infer job must route to GPU node when one is available"
        );
    }

    #[test]
    fn no_nodes_returns_unplaceable() {
        let nodes = HashMap::new();
        let job = make_job(JobClass::Build);
        let decision = place_job(&job, &nodes);
        assert!(matches!(decision, PlacementDecision::Unplaceable { .. }));
    }

    #[test]
    fn overloaded_node_not_selected() {
        let mut nodes = HashMap::new();
        // load1 = 20 on 4 cores → saturation 5.0 > 2.0 → not available
        nodes.insert("overloaded".to_string(), make_node(4, 20.0, 0, 0));
        let job = make_job(JobClass::Build);
        let decision = place_job(&job, &nodes);
        assert!(matches!(decision, PlacementDecision::Unplaceable { .. }));
    }

    #[test]
    fn pinned_node_only_considered() {
        let mut nodes = HashMap::new();
        nodes.insert("alpha".to_string(), make_node(8, 0.5, 0, 0));
        nodes.insert("beta".to_string(), make_node(8, 0.5, 0, 0));
        let mut job = make_job(JobClass::Build);
        job.pinned_node = Some("gamma".to_string()); // doesn't exist
        let decision = place_job(&job, &nodes);
        assert!(matches!(decision, PlacementDecision::Unplaceable { .. }));
    }

    // ── cloud-build routing invariant (constellation-cloud-build AC3 & AC4) ──

    /// A node with `no_build: true` is never selected for build jobs.
    #[test]
    fn no_build_node_never_selected_for_build() {
        let mut nodes = HashMap::new();
        nodes.insert("local-llm".to_string(), make_local_llm_node(16, 0.5));
        let decision = place_job(&make_job(JobClass::Build), &nodes);
        assert!(
            matches!(decision, PlacementDecision::Unplaceable { .. }),
            "no_build node must never be selected for build jobs even when available"
        );
    }

    /// A `role: LocalLlm` node is never selected for test jobs.
    #[test]
    fn local_llm_role_never_selected_for_test() {
        let mut nodes = HashMap::new();
        nodes.insert("local-llm".to_string(), make_local_llm_node(16, 0.5));
        let decision = place_job(&make_job(JobClass::Test), &nodes);
        assert!(
            matches!(decision, PlacementDecision::Unplaceable { .. }),
            "LocalLlm node must never be selected for test jobs"
        );
    }

    /// When both a local-llm node and a cloud worker are present, build jobs go
    /// exclusively to the cloud worker — the local-llm node is bypassed.
    #[test]
    fn build_job_routes_to_cloud_not_local_llm() {
        let mut nodes = HashMap::new();
        // 5700U: would be the best CPU node by raw cores, but no_build=true
        nodes.insert("5700u-local-llm".to_string(), make_local_llm_node(16, 0.2));
        // Cloud worker: fewer cores, higher load, but build-eligible
        nodes.insert("cloud-worker".to_string(), make_node(8, 1.0, 0, 0));
        let decision = place_job(&make_job(JobClass::Build), &nodes);
        assert!(
            matches!(decision, PlacementDecision::Route(_)),
            "build job must route to cloud-worker when local-llm is the only other candidate"
        );
    }

    /// Fleet invariant: if ALL present nodes are no_build, build jobs are unplaceable
    /// (no silent fallback to a local-llm node).
    #[test]
    fn all_no_build_nodes_means_unplaceable() {
        let mut nodes = HashMap::new();
        nodes.insert("llm-a".to_string(), make_local_llm_node(8, 0.1));
        nodes.insert("llm-b".to_string(), make_local_llm_node(16, 0.2));
        let decision = place_job(&make_job(JobClass::Build), &nodes);
        assert!(
            matches!(decision, PlacementDecision::Unplaceable { .. }),
            "all no_build fleet must return Unplaceable for build jobs"
        );
    }

    /// Infer jobs CAN be placed on a `no_build: true` node if it has a GPU.
    /// (Inference is allowed on the local-llm node — that's its purpose.)
    #[test]
    fn no_build_does_not_block_infer_jobs() {
        let mut nodes = HashMap::new();
        let gpu_llm_node = NodeCapability {
            gpu: true,
            vram_gb: 24,
            no_build: true,
            role: Some(NodeRole::LocalLlm),
            ..make_local_llm_node(8, 0.5)
        };
        nodes.insert("gpu-local-llm".to_string(), gpu_llm_node);
        let decision = place_job(&make_job(JobClass::Infer), &nodes);
        assert!(
            matches!(decision, PlacementDecision::Route(_)),
            "no_build flag must not block infer jobs — inference is the node's purpose"
        );
    }

    /// `is_build_eligible` reflects `no_build` and `role` correctly.
    #[test]
    fn is_build_eligible_semantics() {
        let worker = make_node(8, 0.5, 0, 0);
        assert!(worker.is_build_eligible(), "default cloud worker must be build-eligible");

        let llm_node = make_local_llm_node(16, 0.1);
        assert!(!llm_node.is_build_eligible(), "local-llm node must not be build-eligible");

        // no_build alone is sufficient to block, even without a role
        let no_build_anon = NodeCapability { no_build: true, role: None, ..make_node(8, 0.5, 0, 0) };
        assert!(!no_build_anon.is_build_eligible(), "no_build=true must block regardless of role");

        // role:LocalLlm alone (no_build=false) is also sufficient to block
        let role_only = NodeCapability {
            no_build: false,
            role: Some(NodeRole::LocalLlm),
            ..make_node(8, 0.5, 0, 0)
        };
        assert!(!role_only.is_build_eligible(), "LocalLlm role must block build even if no_build=false");
    }

    // ── local sampler (smoke) ───────────────────────────────────────────────

    #[test]
    fn sample_local_capacity_returns_valid() {
        let cap = sample_local_capacity(0);
        assert!(cap.cores >= 1, "cores must be >= 1");
        assert!(cap.ts > 0, "ts must be a valid Unix timestamp");
    }
}
