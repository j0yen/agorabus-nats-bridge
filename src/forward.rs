//! Topic forwarding policy — determines which agorabus topics cross to NATS.
//!
//! The loop guard is implemented here: a message that arrived from NATS
//! carries a `_wm_bridged` marker and is NOT re-forwarded back to NATS.

use serde::{Deserialize, Serialize};

/// Header key used to tag messages that originated from NATS.
///
/// When the bridge injects a NATS event into the local agorabus bus, it
/// publishes a JSON payload that wraps the original with this field set to
/// `true`. On the agorabus→NATS path, any event whose payload contains
/// `_wm_bridged: true` is silently dropped — this is the loop guard.
pub const BRIDGE_MARKER_KEY: &str = "_wm_bridged";

/// Topics (or prefixes) that must NEVER cross to NATS regardless of allowlist.
///
/// High-volume or privacy-sensitive local chatter.
pub const BLOCKED_PREFIXES: &[&str] = &[
    "wm.audio.speech.chunk",
    "wm.local.",
];

/// A bridged event payload — wraps an original agorabus `data` field with
/// the loop-guard marker so the bridge can detect its own injected messages.
///
/// The `bridged` field serializes as `_wm_bridged` to match the wire protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgedPayload {
    /// Always `true` — signals "this came from the bridge / NATS side".
    /// Serialized as `_wm_bridged` on the wire.
    #[serde(rename = "_wm_bridged")]
    pub bridged: bool,
    /// The original topic from the source side.
    pub topic: String,
    /// The original data payload.
    pub data: serde_json::Value,
    /// The `session_id` of the original publisher.
    pub from: String,
}

impl BridgedPayload {
    /// Construct a new bridged payload.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // String isn't const-constructible on stable
    pub fn new(topic: String, data: serde_json::Value, from: String) -> Self {
        Self {
            bridged: true,
            topic,
            data,
            from,
        }
    }

    /// Returns true if `value` has the bridge marker set to `true`.
    /// Used on the agorabus-receive side to detect and drop loop-back messages.
    #[must_use]
    pub fn is_bridged(value: &serde_json::Value) -> bool {
        value
            .as_object()
            .and_then(|obj| obj.get(BRIDGE_MARKER_KEY))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    }
}

/// Returns `true` if `topic` should be forwarded to NATS based on the allow list.
///
/// Rules (in order):
/// 1. If the topic matches any `BLOCKED_PREFIXES`, return `false` (never forward).
/// 2. If the topic matches any prefix in `allow_prefixes`, return `true`.
/// 3. Otherwise return `false`.
#[must_use]
pub fn should_forward(topic: &str, allow_prefixes: &[String]) -> bool {
    // Rule 1: explicit block list
    for blocked in BLOCKED_PREFIXES {
        if topic.starts_with(blocked) {
            return false;
        }
    }
    // Rule 2: allowlist
    for prefix in allow_prefixes {
        if topic.starts_with(prefix.as_str()) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_allow() -> Vec<String> {
        vec!["wm.fleet.".to_string()]
    }

    #[test]
    fn fleet_topic_is_forwarded() {
        assert!(should_forward("wm.fleet.presence", &default_allow()));
        assert!(should_forward("wm.fleet.heartbeat.laptop", &default_allow()));
    }

    #[test]
    fn blocked_audio_chunk_not_forwarded() {
        assert!(!should_forward("wm.audio.speech.chunk", &default_allow()));
        assert!(!should_forward("wm.audio.speech.chunk.raw", &default_allow()));
    }

    #[test]
    fn local_topics_not_forwarded() {
        assert!(!should_forward("wm.local.debug", &default_allow()));
        assert!(!should_forward("wm.local.foo.bar", &default_allow()));
    }

    #[test]
    fn unknown_topic_not_forwarded() {
        assert!(!should_forward("wm.stt.transcript", &default_allow()));
        assert!(!should_forward("wm.brain.reply", &default_allow()));
    }

    #[test]
    fn bridge_marker_detected() {
        let val = serde_json::json!({ "_wm_bridged": true, "topic": "wm.fleet.x", "data": {}, "from": "x" });
        assert!(BridgedPayload::is_bridged(&val));
    }

    #[test]
    fn non_bridged_payload_not_detected() {
        let val = serde_json::json!({ "topic": "wm.fleet.x", "data": {} });
        assert!(!BridgedPayload::is_bridged(&val));
    }

    #[test]
    fn extra_allow_prefix_forwarded() {
        let allow = vec!["wm.fleet.".to_string(), "wm.presence.".to_string()];
        assert!(should_forward("wm.presence.announce", &allow));
        assert!(!should_forward("wm.audio.raw", &allow));
    }
}
