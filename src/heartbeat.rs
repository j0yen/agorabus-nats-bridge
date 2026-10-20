//! Capability heartbeat — publishes this node's [`NodeCapability`] to the
//! `WM_NODES` JetStream KV bucket so `wm-busbridge status` can list the fleet.
//!
//! The writer and the reader ([`crate::status`]) share [`NODES_BUCKET`] and the
//! `node.<name>` key helpers in [`crate::dispatch`], so they cannot drift.
//! The bucket is get-or-created with a short `max_age` so a dead node's record
//! expires on its own. Every failure is logged and retried on the next tick;
//! the heartbeat never takes the bridge down.

// JetStream is a proper noun.
#![allow(clippy::doc_markdown)]

use std::time::Duration;

use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, kv};
use tracing::{debug, info, warn};

use crate::dispatch::{NODES_BUCKET, fleet_js, node_key, sample_local_capacity};

/// Seconds between heartbeats.
pub const HEARTBEAT_INTERVAL_SECS: u64 = 30;
/// KV entry lifetime; four missed beats and the node disappears.
pub const NODES_MAX_AGE_SECS: u64 = 120;

/// Resolve this node's name: `WM_NODE_NAME`, else the kernel hostname.
#[must_use]
pub fn local_node_name() -> String {
    if let Ok(v) = std::env::var("WM_NODE_NAME") {
        let v = v.trim();
        if !v.is_empty() {
            return v.to_string();
        }
    }
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok().filter(|s| !s.is_empty()))
        .unwrap_or_else(|| "unknown".to_string())
}

/// Serialize the current capability snapshot to the KV wire format.
///
/// # Errors
///
/// Returns `Err` if JSON serialization fails (practically infallible).
pub fn heartbeat_payload(queue_depth: u32) -> Result<Vec<u8>> {
    serde_json::to_vec(&sample_local_capacity(queue_depth)).context("serializing NodeCapability")
}

/// Get the `WM_NODES` bucket, creating it if it does not exist.
async fn ensure_bucket(js: &jetstream::Context) -> Result<kv::Store> {
    if let Ok(store) = js.get_key_value(NODES_BUCKET).await {
        return Ok(store);
    }
    js.create_key_value(kv::Config {
        bucket: NODES_BUCKET.to_string(),
        description: "wintermute fleet capability registry".to_string(),
        history: 1,
        max_age: Duration::from_secs(NODES_MAX_AGE_SECS),
        ..Default::default()
    })
    .await
    .with_context(|| format!("creating KV bucket {NODES_BUCKET}"))
}

/// Run the heartbeat loop forever. Never returns an error and never panics;
/// callers spawn it and abort the task on shutdown.
pub async fn run(nats: async_nats::Client) {
    let js = fleet_js(nats);
    let key = node_key(&local_node_name());
    let mut store: Option<kv::Store> = None;
    let mut tick = tokio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    info!(key = %key, bucket = NODES_BUCKET, "capability heartbeat started");
    loop {
        tick.tick().await;
        if store.is_none() {
            match ensure_bucket(&js).await {
                Ok(s) => store = Some(s),
                Err(e) => {
                    warn!(error = %e, "capability heartbeat: bucket unavailable (will retry)");
                    continue;
                }
            }
        }
        let Some(s) = store.as_ref() else { continue };
        // Queue depth is not cheaply available in the bridge; report 0.
        let payload = match heartbeat_payload(0) {
            Ok(p) => p,
            Err(e) => {
                warn!(error = %e, "capability heartbeat: serialize failed");
                continue;
            }
        };
        match s.put(&key, payload.into()).await {
            Ok(rev) => debug!(key = %key, rev, "capability heartbeat written"),
            Err(e) => {
                warn!(error = %e, "capability heartbeat: put failed (will retry)");
                // Bucket may have been deleted/recreated; re-resolve next tick.
                store = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::{NodeCapability, node_name_from_key};
    use crate::status::make_node_row;

    #[test]
    fn writer_key_round_trips_through_reader_parse() {
        let key = node_key("ryzen7");
        assert_eq!(key, "node.ryzen7");
        assert_eq!(node_name_from_key(&key), "ryzen7");
        // A key without the prefix is shown as-is by the reader.
        assert_eq!(node_name_from_key("legacy"), "legacy");
    }

    #[test]
    fn payload_decodes_as_node_capability_and_renders_up() {
        let bytes = heartbeat_payload(3).expect("payload must serialize");
        let cap: NodeCapability = serde_json::from_slice(&bytes).expect("reader must decode");
        assert_eq!(cap.queue_depth, 3);
        let row = make_node_row(&node_name_from_key(&node_key("n")), &cap, cap.ts, 30);
        assert_eq!(row.name, "n");
        assert_eq!(row.status, crate::status::NodeStatus::Up);
    }

    #[test]
    fn expiry_is_longer_than_stale_threshold_and_interval() {
        assert!(NODES_MAX_AGE_SECS > HEARTBEAT_INTERVAL_SECS * 2);
    }

    #[test]
    fn fleet_domain_defaults_to_hub() {
        // Only asserts the default when the override is unset in the test env.
        if std::env::var(crate::dispatch::FLEET_JS_DOMAIN_ENV).is_err() {
            assert_eq!(crate::dispatch::fleet_domain(), "hub");
        }
    }

    #[test]
    fn local_node_name_is_nonempty() {
        assert!(!local_node_name().is_empty());
    }
}
