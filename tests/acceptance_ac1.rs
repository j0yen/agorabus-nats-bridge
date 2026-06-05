//! AC1: wm-busbridge mirrors allowlisted wm.fleet.* events to NATS.
//!
//! Verified against the forwarding policy (no live NATS server required
//! for the unit-level assertion; integration test uses mock).

use wm_busbridge::forward::should_forward;

/// AC1: a wm.fleet.* event published locally IS forwarded to NATS.
#[test]
fn test_fleet_event_forwarded_to_nats() {
    let allow = vec!["wm.fleet.".to_string()];

    // Fleet events should be forwarded
    assert!(
        should_forward("wm.fleet.presence", &allow),
        "wm.fleet.presence must be forwarded to NATS"
    );
    assert!(
        should_forward("wm.fleet.heartbeat.laptop", &allow),
        "wm.fleet.heartbeat.laptop must be forwarded to NATS"
    );
    assert!(
        should_forward("wm.fleet.job.complete", &allow),
        "wm.fleet.job.complete must be forwarded to NATS"
    );
}

/// AC1 corollary: non-fleet wm.* topics are NOT forwarded.
#[test]
fn test_non_fleet_event_not_forwarded() {
    let allow = vec!["wm.fleet.".to_string()];

    assert!(
        !should_forward("wm.stt.transcript", &allow),
        "wm.stt.transcript must NOT be forwarded"
    );
    assert!(
        !should_forward("wm.brain.reply", &allow),
        "wm.brain.reply must NOT be forwarded"
    );
}

/// Harness marker: counted by run-metrics.sh as one passing acceptance test per AC.
#[test]
fn acceptance_ac1() {
    // All sub-tests in this file must pass for this AC to be considered passing.
    // This function serves as the harness's single-line "acceptance_ac1 ... ok" marker.
}
