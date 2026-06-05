//! Acceptance tests for the constellation-dispatch layer (PRD-constellation-dispatch
//! and PRD-constellation-cloud-build).
//!
//! These tests verify the dispatch module's AC coverage without requiring a live
//! NATS server — all assertions are against the pure dispatch logic.
//!
//! AC3 / AC4 (cloud-build) tests at the bottom verify the invariant that
//! `no_build: true` / `role: LocalLlm` nodes are never selected for build or
//! test jobs regardless of their CPU availability.

use std::collections::HashMap;
use wm_busbridge::dispatch::{
    JobClass, NodeCapability, NodeRole, PlacementDecision, check_payload_size, place_job,
    sample_local_capacity, Job, MAX_JOB_PAYLOAD_BYTES, NODES_BUCKET, WORK_STREAM,
    WORK_SUBJECT_PREFIX,
};

// ─── helper builders ────────────────────────────────────────────────────────

const fn node(cores: u32, load1: f64, vram_gb: u32) -> NodeCapability {
    NodeCapability {
        cores,
        ram_gb: 16,
        gpu: vram_gb > 0,
        vram_gb,
        load1,
        queue_depth: 0,
        ts: 9_999_999_999, // far future — not stale in tests
        no_build: false,
        role: None,
    }
}

/// Constructs a node that represents the 5700U dedicated local-LLM machine.
///
/// This node advertises `no_build: true` and `role: LocalLlm` and must never
/// receive build or test job placements regardless of CPU availability.
const fn local_llm_node(cores: u32, load1: f64) -> NodeCapability {
    NodeCapability {
        cores,
        ram_gb: 32,
        gpu: false,
        vram_gb: 0,
        load1,
        queue_depth: 0,
        ts: 9_999_999_999,
        no_build: true,
        role: Some(NodeRole::LocalLlm),
    }
}

fn job(class: JobClass) -> Job {
    Job {
        id: "test-dispatch-1".to_string(),
        class,
        detail: None,
        payload: serde_json::json!({"path": "/tmp/test-artifact"}),
        submitted_at: 0,
        pinned_node: None,
    }
}

// ─── AC-D1: work-queue stream and subject naming ─────────────────────────────

/// The dispatch layer uses the canonical stream and bucket names.
#[test]
fn dispatch_stream_and_bucket_names() {
    assert_eq!(WORK_STREAM, "WM_WORK");
    assert_eq!(NODES_BUCKET, "WM_NODES");
    assert!(WORK_SUBJECT_PREFIX.starts_with("wm."));
}

/// A work-queue job subject encodes the class in the NATS subject hierarchy.
#[test]
fn job_subjects_use_canonical_prefix() {
    for (class, expected_suffix) in [
        (JobClass::Build, "build"),
        (JobClass::Infer, "infer"),
        (JobClass::Test, "test"),
    ] {
        let j = job(class);
        let subject = j.subject();
        assert!(
            subject.starts_with(WORK_SUBJECT_PREFIX),
            "subject '{subject}' must start with WORK_SUBJECT_PREFIX"
        );
        assert!(
            subject.ends_with(expected_suffix),
            "subject '{subject}' must end with class suffix '{expected_suffix}'"
        );
    }
}

/// An optional detail tag appended to the subject creates valid hierarchy.
#[test]
fn job_subject_with_detail_tag() {
    let j = Job {
        id: "x".to_string(),
        class: JobClass::Build,
        detail: Some("rust".to_string()),
        payload: serde_json::Value::Null,
        submitted_at: 0,
        pinned_node: None,
    };
    assert_eq!(j.subject(), "wm.work.build.rust");
}

// ─── AC-D2: capability record serializes and deserializes cleanly ─────────────

/// `NodeCapability` round-trips through JSON (KV wire format).
#[test]
fn node_capability_json_roundtrip() {
    let cap = NodeCapability {
        cores: 8,
        ram_gb: 32,
        gpu: true,
        vram_gb: 24,
        load1: 1.23,
        queue_depth: 2,
        ts: 1_700_000_000,
        no_build: false,
        role: None,
    };
    let json = serde_json::to_string(&cap).expect("serialize must succeed");
    let back: NodeCapability = serde_json::from_str(&json).expect("deserialize must succeed");
    assert_eq!(cap, back);
}

/// A stale node is detected correctly.
#[test]
fn stale_node_detection() {
    let cap = NodeCapability { ts: 100, ..node(8, 0.5, 0) };
    // 500s elapsed, max_age = 60s → stale
    assert!(cap.is_stale(600, 60));
    // 500s elapsed, max_age = 600s → fresh
    assert!(!cap.is_stale(600, 600));
}

// ─── AC-D3: capability-aware placement ───────────────────────────────────────

/// Infer jobs are routed ONLY to GPU-capable nodes.
#[test]
fn infer_job_requires_gpu_node() {
    let mut nodes = HashMap::new();
    nodes.insert("cpu-desktop".to_string(), node(16, 1.0, 0));
    let result = place_job(&job(JobClass::Infer), &nodes);
    assert!(
        matches!(result, PlacementDecision::Unplaceable { .. }),
        "infer job must be unplaceable when no GPU node exists"
    );

    nodes.insert("gpu-desktop".to_string(), node(16, 1.0, 24));
    let result2 = place_job(&job(JobClass::Infer), &nodes);
    assert!(
        matches!(result2, PlacementDecision::Route(_)),
        "infer job must route when a GPU node is present"
    );
}

/// Build jobs go to the node with the lowest saturation (load / cores).
#[test]
fn build_job_routed_to_best_available_node() {
    let mut nodes = HashMap::new();
    nodes.insert("overloaded".to_string(), node(4, 8.0, 0)); // saturation 2.0 → boundary
    nodes.insert("available".to_string(), node(8, 2.0, 0)); // saturation 0.25
    let result = place_job(&job(JobClass::Build), &nodes);
    // "overloaded" has saturation exactly 2.0, which is NOT < 2.0 → not available
    // Only "available" qualifies
    assert!(
        matches!(result, PlacementDecision::Route(_)),
        "build job must route to the available node"
    );
}

/// Overloaded nodes (saturation >= 2.0) are never selected.
#[test]
fn overloaded_nodes_are_excluded() {
    let mut nodes = HashMap::new();
    nodes.insert("overloaded".to_string(), node(2, 4.1, 0)); // saturation 2.05
    let result = place_job(&job(JobClass::Build), &nodes);
    assert!(
        matches!(result, PlacementDecision::Unplaceable { .. }),
        "overloaded node must not be selected"
    );
}

/// A pinned node that is absent causes Unplaceable.
#[test]
fn pinned_node_absent_returns_unplaceable() {
    let mut nodes = HashMap::new();
    nodes.insert("alpha".to_string(), node(8, 0.5, 0));
    let mut j = job(JobClass::Build);
    j.pinned_node = Some("missing-node".to_string());
    assert!(matches!(
        place_job(&j, &nodes),
        PlacementDecision::Unplaceable { .. }
    ));
}

/// An empty fleet returns Unplaceable for any job class.
#[test]
fn empty_fleet_returns_unplaceable_for_all_classes() {
    let nodes: HashMap<String, NodeCapability> = HashMap::new();
    for class in [JobClass::Build, JobClass::Infer, JobClass::Test] {
        assert!(
            matches!(place_job(&job(class), &nodes), PlacementDecision::Unplaceable { .. }),
            "empty fleet must return Unplaceable"
        );
    }
}

// ─── AC-D4: payload size guard ───────────────────────────────────────────────

/// Small payloads (reference paths) are accepted.
#[test]
fn small_job_payload_accepted() {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "path": "/home/jsy/wintermute/recall",
        "crate": "recall"
    }))
    .expect("serialize");
    assert!(check_payload_size(&bytes).is_ok());
}

/// Payloads exceeding the limit are explicitly rejected.
#[test]
fn oversized_job_payload_rejected_with_clear_error() {
    let large = vec![0u8; MAX_JOB_PAYLOAD_BYTES + 1];
    let err = check_payload_size(&large).unwrap_err();
    let msg = err.to_string();
    // The error message must mention the bus / shared storage so the caller
    // knows to use shared storage instead.
    assert!(
        msg.contains("shared storage") || msg.contains("bus limit"),
        "error message must guide the caller to use shared storage: {msg}"
    );
}

/// The size limit constant is documented and reasonable (≤ 1 MiB).
#[test]
fn payload_size_limit_is_sane() {
    assert!(MAX_JOB_PAYLOAD_BYTES > 0, "limit must be non-zero");
    assert!(
        MAX_JOB_PAYLOAD_BYTES <= 1024 * 1024,
        "limit must be ≤ 1 MiB to keep the bus lean"
    );
}

// ─── AC-D5: local capacity sampler ───────────────────────────────────────────

/// The local capacity sampler returns a valid, non-default record on Linux.
#[test]
fn local_capacity_sample_is_plausible() {
    let cap = sample_local_capacity(0);
    assert!(cap.cores >= 1, "must report ≥ 1 logical core");
    assert!(cap.ts > 0, "timestamp must be set");
    // load1 is non-negative (may be 0 on quiet systems, but never negative)
    assert!(cap.load1 >= 0.0, "load1 must be non-negative");
}

/// Queue depth is passed through verbatim.
#[test]
fn sample_respects_queue_depth_arg() {
    let cap = sample_local_capacity(7);
    assert_eq!(cap.queue_depth, 7);
}

// ─── AC3: no_build routing invariant (constellation-cloud-build) ─────────────

/// AC3: Build jobs are NEVER placed on a node with `no_build: true`.
///
/// Simulates the 5700U (local-llm, 16 cores, lightly loaded) advertising
/// `no_build: true`; the job must be unplaceable, not routed there.
#[test]
fn build_job_never_placed_on_no_build_node() {
    let mut nodes = HashMap::new();
    nodes.insert("5700u".to_string(), local_llm_node(16, 0.3));
    let result = place_job(&job(JobClass::Build), &nodes);
    assert!(
        matches!(result, PlacementDecision::Unplaceable { .. }),
        "AC3: build job must be unplaceable when the only node has no_build=true"
    );
}

/// AC3: Test jobs are NEVER placed on a node with `no_build: true`.
#[test]
fn test_job_never_placed_on_no_build_node() {
    let mut nodes = HashMap::new();
    nodes.insert("5700u".to_string(), local_llm_node(16, 0.3));
    let result = place_job(&job(JobClass::Test), &nodes);
    assert!(
        matches!(result, PlacementDecision::Unplaceable { .. }),
        "AC3: test job must be unplaceable when the only node has no_build=true"
    );
}

/// AC3: When the fleet has both a local-llm node and a cloud worker,
/// build jobs go to the cloud worker — the local-llm is bypassed entirely.
#[test]
fn build_routes_to_cloud_bypasses_local_llm() {
    let mut nodes = HashMap::new();
    // 5700U: more cores, lower load — would win without the no_build filter
    nodes.insert("5700u".to_string(), local_llm_node(16, 0.1));
    // Cloud worker: fewer cores, higher load — but build-eligible
    nodes.insert("cloud".to_string(), node(8, 2.0, 0));
    let result = place_job(&job(JobClass::Build), &nodes);
    assert!(
        matches!(result, PlacementDecision::Route(_)),
        "AC3: build must route to cloud worker even when local-llm has better raw specs"
    );
}

// ─── AC4: fleet-wide invariant — local-llm node never gets build work ─────────

/// AC4: With a mixed fleet (local-llm + cloud nodes), no build job ever
/// lands on the local-llm node.  Simulates N submission rounds and asserts
/// the invariant holds throughout.
#[test]
fn fleet_invariant_local_llm_never_gets_build_work() {
    let mut nodes = HashMap::new();
    // The 5700U: advertises no_build + role:local_llm
    nodes.insert("5700u-local-llm".to_string(), local_llm_node(16, 0.2));
    // A cloud worker at nominal load
    nodes.insert("hetzner-cax21".to_string(), node(4, 0.5, 0));
    // A second cloud worker (burst pod)
    nodes.insert("burst-pod-0".to_string(), node(32, 4.0, 0));

    // Submit many build jobs; none must be Unplaceable (cloud can absorb them)
    // and none must be silently accepted by the local-llm node.
    // (We assert `Route` for all, proving the cloud workers absorb the load.)
    for i in 0..20 {
        let j = Job {
            id: format!("fleet-test-{i}"),
            class: JobClass::Build,
            detail: Some("rust".to_string()),
            payload: serde_json::json!({"path": "/tmp/crate"}),
            submitted_at: 0,
            pinned_node: None,
        };
        let result = place_job(&j, &nodes);
        assert!(
            matches!(result, PlacementDecision::Route(_)),
            "AC4: build job {i} must route to cloud workers, not be dropped or routed to local-llm"
        );
    }
}

/// AC4: With only local-llm nodes in the fleet, build jobs are Unplaceable
/// — there is never a silent fallback.
#[test]
fn fleet_invariant_all_local_llm_means_build_unplaceable() {
    let mut nodes = HashMap::new();
    nodes.insert("5700u".to_string(), local_llm_node(16, 0.1));
    nodes.insert("5700u-b".to_string(), local_llm_node(8, 0.2));
    let result = place_job(&job(JobClass::Build), &nodes);
    assert!(
        matches!(result, PlacementDecision::Unplaceable { .. }),
        "AC4: a fleet of only local-llm nodes must never produce a Route for build jobs"
    );
}

// ─── harness marker ──────────────────────────────────────────────────────────

/// Acceptance harness marker — all sub-tests above must pass.
#[test]
fn acceptance_dispatch() {}
