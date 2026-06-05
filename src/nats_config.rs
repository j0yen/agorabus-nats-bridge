//! NATS topology configuration rendering.
//!
//! Generates hub and leaf NATS server configuration files
//! with proper JetStream domain settings for the constellation fleet.

// JetStream is a proper noun in NATS documentation; suppress doc_markdown for this module.
#![allow(clippy::doc_markdown)]

use clap::ValueEnum;

/// Which NATS config variant to render.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ConfigVariant {
    /// Hub config: cloud node, JetStream enabled with domain=hub, leafnode listener.
    Hub,
    /// Leaf config: per-machine, dials the hub, own JS domain.
    Leaf,
}

/// Parameters for generating a NATS topology config.
#[derive(Debug, Clone)]
pub struct NatsTopologyConfig {
    /// Which variant to render.
    pub variant: ConfigVariant,
    /// JetStream domain for this node.
    pub js_domain: String,
    /// Hub URL (used in leaf config; e.g. "nats://cloud.example.ts.net:7422").
    pub hub_url: String,
    /// Path to the credentials file (e.g. "$WM_NATS_CREDS").
    pub creds_path: String,
}

impl NatsTopologyConfig {
    /// Render the NATS config as a string.
    ///
    /// The hub config enables JetStream with `domain: "hub"` and opens a
    /// leafnode listener. The leaf config connects to the hub URL using
    /// the credentials file and sets its own JetStream domain.
    ///
    /// **No plaintext credentials are embedded.** Credentials are referenced
    /// by path (e.g. an env var or a path to an sops-encrypted file extracted
    /// at runtime).
    ///
    /// JetStream domain note: hub clients use the domain-qualified API prefix
    /// `$JS.hub.API.>` to reach hub streams from a leaf connection. This is
    /// required because JetStream is domain-scoped across leaf boundaries.
    #[must_use]
    pub fn render(&self) -> String {
        match self.variant {
            ConfigVariant::Hub => self.render_hub(),
            ConfigVariant::Leaf => self.render_leaf(),
        }
    }

    fn render_hub(&self) -> String {
        format!(
            r#"# constellation-bus: NATS hub configuration
# Install on the cloud hub node (e.g. via Ansible role constellation-cloud).
# JetStream domain "hub" — leaf clients reach hub streams via $JS.hub.API.>

server_name: wm-hub

jetstream {{
  domain: "{domain}"
  store_dir: /var/lib/nats/jetstream
}}

# Leafnode listener — TLS + per-node credentials enforced.
# Credentials are generated per-node by constellation-cloud's PKI.
leafnodes {{
  listen: "0.0.0.0:7422"
  tls {{
    cert_file: "$WM_NATS_HUB_TLS_CERT"
    key_file:  "$WM_NATS_HUB_TLS_KEY"
    ca_file:   "$WM_NATS_HUB_CA"
  }}
}}

# No plaintext credentials in this file.
# Per-node credentials are issued via the hub's PKI and stored in the
# constellation encrypted store (age/sops). Never commit .creds or .nkey files.
authorization {{
  # Credentials path references env vars; resolved at daemon startup.
  # Each node's credential scopes its pub/sub to wm.> only.
  users: []  # populated by Ansible from per-node credential inventory
}}
"#,
            domain = self.js_domain,
        )
    }

    fn render_leaf(&self) -> String {
        format!(
            r#"# constellation-bus: NATS leaf configuration
# Install on each fleet machine (e.g. via Ansible role constellation-mesh).
# JetStream domain "{domain}" — change per-machine for isolation.
# Hub streams reachable via: $JS.hub.API.> (domain-qualified prefix)

server_name: wm-leaf-${{HOSTNAME}}

jetstream {{
  domain: "{domain}"
  store_dir: /var/lib/nats/jetstream
}}

# Local client listener — only 127.0.0.1 (loopback).
# wm-busbridge connects here; no remote clients on this port.
listen: "127.0.0.1:4222"

# Leafnode connection to the hub.
# credentials path is resolved at startup from the encrypted store.
leafnodes {{
  remotes [
    {{
      url: "{hub_url}"
      credentials: "{creds_path}"
      tls {{
        ca_file: "$WM_NATS_CA"
      }}
    }}
  ]
}}

# No plaintext credentials embedded.
# creds_path above must point to a file extracted from the age/sops store
# at service startup (e.g. via ExecStartPre in the systemd unit).
"#,
            domain = self.js_domain,
            hub_url = self.hub_url,
            creds_path = self.creds_path,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hub_config_has_jetstream_domain() {
        let cfg = NatsTopologyConfig {
            variant: ConfigVariant::Hub,
            js_domain: "hub".to_string(),
            hub_url: String::new(),
            creds_path: String::new(),
        };
        let rendered = cfg.render();
        assert!(rendered.contains("domain: \"hub\""), "hub config must set JS domain");
        assert!(rendered.contains("leafnodes"), "hub config must have leafnode listener");
        assert!(rendered.contains("7422"), "hub must listen on leafnode port 7422");
    }

    #[test]
    fn leaf_config_has_jetstream_domain_and_hub_url() {
        let cfg = NatsTopologyConfig {
            variant: ConfigVariant::Leaf,
            js_domain: "leaf-laptop".to_string(),
            hub_url: "nats://cloud.example.ts.net:7422".to_string(),
            creds_path: "/run/secrets/nats-leaf.creds".to_string(),
        };
        let rendered = cfg.render();
        assert!(rendered.contains("domain: \"leaf-laptop\""), "leaf config must set its own JS domain");
        assert!(rendered.contains("nats://cloud.example.ts.net:7422"), "leaf config must include hub URL");
        assert!(rendered.contains("/run/secrets/nats-leaf.creds"), "leaf config must reference creds path");
        assert!(!rendered.contains(".nkey"), "leaf config must not embed nkey");
    }

    #[test]
    fn no_plaintext_creds_in_configs() {
        let hub = NatsTopologyConfig {
            variant: ConfigVariant::Hub,
            js_domain: "hub".to_string(),
            hub_url: String::new(),
            creds_path: String::new(),
        };
        let leaf = NatsTopologyConfig {
            variant: ConfigVariant::Leaf,
            js_domain: "leaf".to_string(),
            hub_url: "nats://hub:7422".to_string(),
            creds_path: "$WM_NATS_CREDS".to_string(),
        };
        // Neither config should contain actual credential bytes
        for rendered in [hub.render(), leaf.render()] {
            assert!(!rendered.contains("NATSXX"), "must not contain raw nkey seed");
            assert!(!rendered.contains("-----BEGIN"), "must not contain PEM block inline");
        }
    }

    #[test]
    fn hub_domain_qualified_api_prefix_documented() {
        let cfg = NatsTopologyConfig {
            variant: ConfigVariant::Hub,
            js_domain: "hub".to_string(),
            hub_url: String::new(),
            creds_path: String::new(),
        };
        let rendered = cfg.render();
        // The config must document the domain-qualified API prefix
        assert!(rendered.contains("$JS.hub.API"), "hub config must document domain-qualified JS API prefix");
    }
}
