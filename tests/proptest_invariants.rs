//! Proptest invariants for wm-busbridge.

use proptest::prelude::*;
use wm_busbridge::forward::{BridgedPayload, should_forward};

proptest! {
    /// Invariant: BLOCKED_PREFIXES always block regardless of allowlist.
    #[test]
    fn blocked_prefix_always_blocks(suffix in "[a-z.]{0,20}") {
        let allow = vec!["wm.fleet.".to_string(), "wm.audio.".to_string()];
        let topic = format!("wm.audio.speech.chunk{suffix}");
        // wm.audio.speech.chunk is in BLOCKED_PREFIXES — must never be forwarded
        prop_assert!(!should_forward(&topic, &allow));
    }

    /// Invariant: wm.local.* is always blocked.
    #[test]
    fn local_topics_always_blocked(suffix in "[a-z.]{0,20}") {
        let allow = vec!["wm.fleet.".to_string(), "wm.local.".to_string()];
        let topic = format!("wm.local.{suffix}");
        prop_assert!(!should_forward(&topic, &allow));
    }

    /// Invariant: BridgedPayload round-trips through JSON correctly.
    #[test]
    fn bridged_payload_roundtrips(
        topic in "[a-z.]{1,40}",
        from in "[a-z-]{1,20}",
    ) {
        let original = BridgedPayload::new(
            topic.clone(),
            serde_json::json!({"test": true}),
            from,
        );
        let val = serde_json::to_value(&original).unwrap();
        prop_assert!(BridgedPayload::is_bridged(&val));
        prop_assert_eq!(
            val.get("topic").and_then(|v| v.as_str()),
            Some(topic.as_str())
        );
    }

    /// Invariant: non-bridged payloads are never detected as bridged.
    #[test]
    fn non_bridged_never_detected(key in "[a-z_]{1,20}", value in "[a-z]{1,20}") {
        // Exclude the bridge marker key from this test
        prop_assume!(key != "_wm_bridged");
        let payload = serde_json::json!({ key: value });
        prop_assert!(!BridgedPayload::is_bridged(&payload));
    }
}
