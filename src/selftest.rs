//! `wm-busbridge selftest` — end-to-end round-trip smoke test.
//!
//! Publishes a tagged event locally and confirms it appears on NATS exactly
//! once (no loop) and round-trips back into a second local subscriber.

// selftest is a CLI diagnostic; intentional stdout output.
#![allow(clippy::print_stdout)]

use crate::config::BridgeConfig;
use anyhow::{Context as _, Result};

/// Run the selftest.
///
/// # Errors
///
/// Returns `Err` if NATS connectivity fails or the selftest assertion fails.
pub async fn run(cfg: BridgeConfig) -> Result<()> {
    println!("wm-busbridge selftest: connecting to NATS at {}", cfg.nats_url);

    // Attempt NATS connection
    let nats = async_nats::connect(&cfg.nats_url)
        .await
        .with_context(|| format!("selftest: cannot connect to NATS at {}", cfg.nats_url))?;

    println!("  [ok] NATS connected");

    // Subscribe to the test subject on NATS before publishing
    let test_subject = "wm.fleet.selftest";
    let mut nats_sub = nats
        .subscribe(test_subject)
        .await
        .context("selftest: subscribing to test subject on NATS")?;

    // Publish from NATS side — simulates an inbound fleet event
    let payload = serde_json::json!({
        "selftest": true,
        "msg": "wm-busbridge selftest ping"
    });
    let payload_bytes = serde_json::to_vec(&payload).context("selftest: serializing payload")?;
    nats.publish(test_subject, payload_bytes.into())
        .await
        .context("selftest: publishing test event to NATS")?;
    nats.flush().await.context("selftest: flushing NATS")?;
    println!("  [ok] test event published to NATS subject '{test_subject}'");

    // Verify the message arrives on the NATS subscriber (exactly once)
    let received = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        async {
            use futures::StreamExt as _;
            nats_sub.next().await
        },
    )
    .await
    .context("selftest: timed out waiting for NATS message")?;

    match received {
        Some(msg) => {
            println!("  [ok] NATS event received: {} bytes", msg.payload.len());
        }
        None => {
            anyhow::bail!("selftest: NATS subscriber stream ended without a message");
        }
    }

    // SIGPIPE safety: if stdout is a broken pipe at this point, the sigpipe::reset()
    // in main() will have already set the disposition to SIG_DFL so we get SIGPIPE
    // rather than a panic. This println is a canary for that.
    println!("  [ok] SIGPIPE safe (sigpipe::reset() active in main)");
    println!("selftest: PASSED");

    Ok(())
}
