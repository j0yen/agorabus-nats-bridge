//! Core bridge loop: connects agorabus UDS ↔ NATS leaf and forwards events.

use crate::config::BridgeConfig;
use crate::forward::{BridgedPayload, should_forward};
use anyhow::{Context as _, Result};
use futures::StreamExt as _;
use std::sync::Arc;
use tokio::io::AsyncBufReadExt as _;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Run the bridge daemon indefinitely.
///
/// Connects to the local agorabus UDS and local NATS leaf, then:
/// - Subscribes to all `wm.*` topics on agorabus; forwards allowlisted ones to NATS.
/// - Subscribes to `wm.>` on NATS; injects events into agorabus (with loop guard).
///
/// # Errors
///
/// Returns `Err` on fatal setup failures (NATS connect, agorabus connect).
/// Transient errors on individual events are logged and skipped.
#[allow(clippy::similar_names)] // pub vs sub are distinct enough
pub async fn run(cfg: BridgeConfig) -> Result<()> {
    info!(
        socket = %cfg.socket_path.display(),
        nats_url = %cfg.nats_url,
        "wm-busbridge starting"
    );

    // Connect to NATS
    let nats = async_nats::connect(&cfg.nats_url)
        .await
        .with_context(|| format!("connecting to NATS at {}", cfg.nats_url))?;

    info!("connected to NATS");

    // Connect to agorabus: two separate connections, one for pub, one for sub
    let bus_publisher = Arc::new(Mutex::new(connect_agorabus(&cfg).await?));
    let bus_subscriber = connect_agorabus_sub(&cfg).await?;

    info!("connected to agorabus");

    let allow = cfg.fleet_allow_prefixes();

    // Spawn the NATS→agorabus direction
    let nats_to_bus = {
        let nats_clone = nats.clone();
        let pub_clone = Arc::clone(&bus_publisher);
        let allow_clone = allow.clone();
        tokio::spawn(async move {
            if let Err(e) = nats_to_agorabus_loop(nats_clone, pub_clone, allow_clone).await {
                error!(error = %e, "NATS→agorabus loop failed");
            }
        })
    };

    // Run the agorabus→NATS direction in the main task
    let result = agorabus_to_nats_loop(bus_subscriber, nats, allow).await;

    nats_to_bus.abort();
    result
}

/// A minimal agorabus connection wrapper for publishing.
///
/// We use raw tokio UDS + line-framed JSON, mirroring agorabus's `Client`.
struct AgorabusPublisher {
    write: tokio::net::unix::OwnedWriteHalf,
}

impl AgorabusPublisher {
    /// Publish a `wm.*` event into the local bus.
    ///
    /// # Errors
    ///
    /// Returns `Err` on serialization or write failure.
    async fn publish(&mut self, topic: &str, data: serde_json::Value) -> Result<()> {
        use tokio::io::AsyncWriteExt as _;
        let msg = serde_json::json!({
            "op": "publish",
            "topic": topic,
            "data": data
        });
        let mut buf = serde_json::to_vec(&msg).context("serializing publish")?;
        buf.push(b'\n');
        self.write.write_all(&buf).await.context("writing to agorabus")?;
        self.write.flush().await.context("flushing agorabus write")?;
        Ok(())
    }
}

/// A minimal agorabus subscriber that reads `ServerEvent` lines.
struct AgorabusSubscriber {
    reader: tokio::io::Lines<tokio::io::BufReader<tokio::net::unix::OwnedReadHalf>>,
}

impl AgorabusSubscriber {
    /// Read the next event from the bus.
    ///
    /// # Errors
    ///
    /// Returns `Err` on I/O failure or if the stream ends unexpectedly.
    async fn next_event(&mut self) -> Result<Option<(String, serde_json::Value, String)>> {
        loop {
            let Some(line) = self.reader.next_line().await.context("reading from agorabus")? else {
                return Ok(None);
            };
            if line.trim().is_empty() {
                continue;
            }
            let val: serde_json::Value = match serde_json::from_str(&line) {
                Ok(v) => v,
                Err(e) => {
                    warn!(line = %line, error = %e, "failed to parse agorabus event");
                    continue;
                }
            };
            // Skip Replies (ack messages) — only forward ServerEvent
            if val.get("topic").is_some() && val.get("data").is_some() {
                let topic = val.get("topic")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let data = val.get("data").cloned().unwrap_or(serde_json::Value::Null);
                let from = val.get("from")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown")
                    .to_string();
                return Ok(Some((topic, data, from)));
            }
        }
    }
}

async fn connect_agorabus(cfg: &BridgeConfig) -> Result<AgorabusPublisher> {
    use tokio::io::AsyncWriteExt as _;
    let stream = tokio::net::UnixStream::connect(&cfg.socket_path)
        .await
        .with_context(|| format!("connecting to agorabus at {}", cfg.socket_path.display()))?;
    let (_, mut write) = stream.into_split();

    // Announce ourselves
    let announce = serde_json::json!({
        "op": "announce",
        "session_id": "wm-busbridge-pub",
        "pid": std::process::id(),
        "cwd": "/",
        "intent": "fleet bridge publisher"
    });
    let mut buf = serde_json::to_vec(&announce).context("serializing announce")?;
    buf.push(b'\n');
    write.write_all(&buf).await.context("writing announce")?;
    write.flush().await.context("flushing announce")?;

    Ok(AgorabusPublisher { write })
}

async fn connect_agorabus_sub(cfg: &BridgeConfig) -> Result<AgorabusSubscriber> {
    use tokio::io::{AsyncWriteExt as _, BufReader};
    let stream = tokio::net::UnixStream::connect(&cfg.socket_path)
        .await
        .with_context(|| format!("connecting to agorabus at {}", cfg.socket_path.display()))?;
    let (read, mut write) = stream.into_split();

    // Announce ourselves
    let announce = serde_json::json!({
        "op": "announce",
        "session_id": "wm-busbridge-sub",
        "pid": std::process::id(),
        "cwd": "/",
        "intent": "fleet bridge subscriber"
    });
    let mut buf = serde_json::to_vec(&announce).context("serializing announce")?;
    buf.push(b'\n');
    write.write_all(&buf).await.context("writing announce")?;
    write.flush().await.context("flushing announce")?;

    // Subscribe to all wm.* topics
    let sub_msg = serde_json::json!({
        "op": "subscribe",
        "prefix": "wm."
    });
    let mut sub_buf = serde_json::to_vec(&sub_msg).context("serializing subscribe")?;
    sub_buf.push(b'\n');
    write.write_all(&sub_buf).await.context("writing subscribe")?;
    write.flush().await.context("flushing subscribe")?;

    let reader = BufReader::new(read).lines();
    Ok(AgorabusSubscriber { reader })
}

/// agorabus → NATS direction.
async fn agorabus_to_nats_loop(
    mut sub: AgorabusSubscriber,
    nats: async_nats::Client,
    allow: Vec<String>,
) -> Result<()> {
    info!("agorabus→NATS loop started");
    loop {
        match sub.next_event().await {
            Ok(Some((topic, data, _from))) => {
                // Loop guard: skip events that came from the NATS→agorabus path
                if BridgedPayload::is_bridged(&data) {
                    debug!(topic = %topic, "skipping bridged-back event (loop guard)");
                    continue;
                }
                // Forwarding policy
                if !should_forward(&topic, &allow) {
                    debug!(topic = %topic, "topic not in allowlist, skipping");
                    continue;
                }
                // Forward to NATS as raw JSON bytes
                let payload_bytes = match serde_json::to_vec(&data) {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(error = %e, "failed to serialize payload for NATS");
                        continue;
                    }
                };
                debug!(topic = %topic, "forwarding to NATS");
                if let Err(e) = nats.publish(topic.clone(), payload_bytes.into()).await {
                    error!(topic = %topic, error = %e, "failed to publish to NATS");
                }
            }
            Ok(None) => {
                info!("agorabus subscriber stream ended");
                return Ok(());
            }
            Err(e) => {
                error!(error = %e, "error reading from agorabus");
                return Err(e);
            }
        }
    }
}

/// NATS → agorabus direction.
async fn nats_to_agorabus_loop(
    nats: async_nats::Client,
    publisher: Arc<Mutex<AgorabusPublisher>>,
    allow: Vec<String>,
) -> Result<()> {
    info!("NATS→agorabus loop started");
    let mut sub = nats
        .subscribe("wm.>")
        .await
        .context("subscribing to wm.> on NATS")?;

    while let Some(msg) = sub.next().await {
        let topic = msg.subject.to_string();
        // Only inject allowlisted topics back into the bus
        if !should_forward(&topic, &allow) {
            debug!(topic = %topic, "NATS topic not in allowlist, skipping");
            continue;
        }
        let original_data: serde_json::Value = serde_json::from_slice(&msg.payload)
            .unwrap_or(serde_json::Value::Null);
        // Wrap with bridge marker so the agorabus→NATS loop ignores it
        let bridged = BridgedPayload::new(topic.clone(), original_data, "nats".to_string());
        let wrapped = match serde_json::to_value(&bridged) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "failed to serialize bridged payload");
                continue;
            }
        };
        debug!(topic = %topic, "injecting NATS event into agorabus");
        let mut locked_pub = publisher.lock().await;
        if let Err(e) = locked_pub.publish(&topic, wrapped).await {
            error!(topic = %topic, error = %e, "failed to publish to agorabus");
        }
    }
    info!("NATS subscription stream ended");
    Ok(())
}
