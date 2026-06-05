//! Acceptance tests for the constellation-dispatch layer (PRD-constellation-dispatch).
//!
//! These tests verify the dispatch module's AC coverage without requiring a live
//! NATS server — all assertions are against the pure dispatch logic.

use std::collections::HashMap;
use wm_busbridge::dispatch::{
    JobClass, NodeCapability, PlacementDecision, check_payload_size, place_job,
    sample_local_capacity, Job, MAX_JOB_PAYLOAD_BYTES, NODES_BUCKET, WORK_STREAM,
    WORK_SUBJECT_PREFIX,
};

// ─── helper builders ────────────────────────────────────────────────────────

fn node(cores: u32, load1: f64, vram_gb: u32) -> NodeCapability {
    NodeCapability {
        cores,
        ram_gb: 16,
        gpu: vram_gb > 0,
        vram_gb,
        load1,
        queue_depth: 0,
        ts: 9_999_999_999, // far future — not stale in tests
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

// ─── harness marker ──────────────────────────────────────────────────────────

/// Acceptance harness marker — all sub-tests above must pass.
#[test]
fn acceptance_dispatch() {}
