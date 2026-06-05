//! wm-busbridge — agorabus ↔ NATS bridge daemon for the wintermute constellation fleet.
//!
//! Subscribes to the local agorabus UDS and a local NATS leaf node,
//! mirroring allowlisted `wm.fleet.*` events between the two without
//! loop-echoing or exposing high-volume local-only topics (e.g. `wm.audio.speech.chunk`).

// main.rs is the CLI entry point; intentional stdout output for config rendering.
#![allow(clippy::print_stdout)]
// JetStream is a proper noun.
#![allow(clippy::doc_markdown)]

use anyhow::Result;
use clap::{Parser, Subcommand};

use wm_busbridge::{bridge, config, nats_config, selftest};

#[derive(Parser)]
#[command(name = "wm-busbridge", about = "agorabus <-> NATS bridge for wintermute fleet")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Path to the agorabus Unix-domain socket.
    #[arg(long, env = "WM_BUS_SOCKET", default_value = "")]
    socket: String,

    /// NATS server URL for the local leaf node.
    #[arg(long, env = "WM_NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,

    /// Comma-separated list of additional allowed fleet topic prefixes.
    #[arg(long, env = "WM_BRIDGE_ALLOW", default_value = "")]
    allow: String,
}

#[derive(Subcommand)]
enum Command {
    /// Run the bridge daemon (default when no subcommand is given).
    Run,
    /// Run the end-to-end selftest and exit 0 on success.
    Selftest,
    /// Print the NATS topology config to stdout (hub or leaf variant).
    NatsConfig {
        /// Which config to print: "hub" or "leaf".
        #[arg(value_enum)]
        variant: nats_config::ConfigVariant,
        /// This node's JetStream domain name.
        #[arg(long, default_value = "leaf")]
        domain: String,
        /// Hub address for leaf config.
        #[arg(long, default_value = "")]
        hub_url: String,
        /// Path to the credentials file.
        #[arg(long, default_value = "$WM_NATS_CREDS")]
        creds_path: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // SIGPIPE reset — must be first statement. Prevents `println!`/`write!`
    // panics on broken pipe (e.g. `wm-busbridge | head`).
    sigpipe::reset();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    let socket_path = if cli.socket.is_empty() {
        agorabus_socket_path()
    } else {
        std::path::PathBuf::from(&cli.socket)
    };

    let extra_allow: Vec<String> = if cli.allow.is_empty() {
        vec![]
    } else {
        cli.allow.split(',').map(str::trim).map(String::from).collect()
    };

    let cfg = config::BridgeConfig {
        socket_path,
        nats_url: cli.nats_url,
        extra_allow,
    };

    match cli.command.unwrap_or(Command::Run) {
        Command::Run => bridge::run(cfg).await,
        Command::Selftest => selftest::run(cfg).await,
        Command::NatsConfig { variant, domain, hub_url, creds_path } => {
            let nc = nats_config::NatsTopologyConfig {
                variant,
                js_domain: domain,
                hub_url,
                creds_path,
            };
            print!("{}", nc.render());
            Ok(())
        }
    }
}

/// Returns the default agorabus socket path (`~/.cache/agorabus/sock`).
fn agorabus_socket_path() -> std::path::PathBuf {
    std::env::var("HOME").map_or_else(
        |_| {
            let uid = std::env::var("UID").unwrap_or_else(|_| "0".into());
            std::path::PathBuf::from(format!("/tmp/agorabus-{uid}/sock"))
        },
        |home| std::path::PathBuf::from(home).join(".cache/agorabus/sock"),
    )
}
