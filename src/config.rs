//! Bridge configuration.

use std::path::PathBuf;

/// Configuration for the bridge daemon.
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    /// Path to the agorabus Unix-domain socket.
    pub socket_path: PathBuf,
    /// NATS server URL for the local leaf node.
    pub nats_url: String,
    /// Additional allowlisted fleet topic prefixes (beyond "wm.fleet.").
    pub extra_allow: Vec<String>,
}

impl BridgeConfig {
    /// Returns the allowlist of topic prefixes that cross to NATS.
    ///
    /// Always includes `wm.fleet.`. Additional prefixes come from `extra_allow`.
    #[must_use]
    pub fn fleet_allow_prefixes(&self) -> Vec<String> {
        let mut prefixes = vec!["wm.fleet.".to_string()];
        prefixes.extend(self.extra_allow.clone());
        prefixes
    }
}
