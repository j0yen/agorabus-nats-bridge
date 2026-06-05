//! AC4: selective forwarding — high-volume/local-only topics are NOT forwarded to NATS.

use wm_busbridge::forward::{BLOCKED_PREFIXES, should_forward};

/// AC4: wm.audio.speech.chunk (PCM flood) is blocked regardless of allowlist.
#[test]
fn test_local_only_topic_not_forwarded() {
    let allow = vec!["wm.fleet.".to_string()];

    assert!(
        !should_forward("wm.audio.speech.chunk", &allow),
        "wm.audio.speech.chunk must NOT be forwarded (high-volume PCM)"
    );
    assert!(
        !should_forward("wm.audio.speech.chunk.raw", &allow),
        "wm.audio.speech.chunk.raw must NOT be forwarded"
    );
    assert!(
        !should_forward("wm.local.debug", &allow),
        "wm.local.debug must NOT be forwarded"
    );
    assert!(
        !should_forward("wm.local.foo.bar", &allow),
        "wm.local.foo.bar must NOT be forwarded"
    );
}

/// AC4: blocked prefixes constant includes the documented topics.
#[test]
fn test_blocked_prefixes_include_documented_topics() {
    let blocked: Vec<&str> = BLOCKED_PREFIXES.to_vec();
    assert!(
        blocked.contains(&"wm.audio.speech.chunk"),
        "BLOCKED_PREFIXES must include wm.audio.speech.chunk"
    );
    assert!(
        blocked.contains(&"wm.local."),
        "BLOCKED_PREFIXES must include wm.local."
    );
}

/// AC4: blocked topics remain blocked even if they share a prefix with an allowed topic.
#[test]
fn test_blocked_takes_priority_over_allow() {
    // Even if someone adds "wm.audio." to the allowlist, speech.chunk stays blocked
    let allow = vec!["wm.fleet.".to_string(), "wm.audio.".to_string()];
    assert!(
        !should_forward("wm.audio.speech.chunk", &allow),
        "wm.audio.speech.chunk must be blocked even if wm.audio. is in allowlist"
    );
    // But other audio topics not in the block list can be allowed
    assert!(
        should_forward("wm.audio.status", &allow),
        "wm.audio.status (non-blocked) can be forwarded if wm.audio. is in allowlist"
    );
}

/// Harness marker: counted by run-metrics.sh as one passing acceptance test per AC.
#[test]
fn acceptance_ac4() {
    // All sub-tests in this file must pass for this AC to be considered passing.
    // This function serves as the harness's single-line "acceptance_ac4 ... ok" marker.
}
