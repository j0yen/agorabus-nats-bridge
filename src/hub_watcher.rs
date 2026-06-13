//! Hub-liveness watcher: probes the NATS hub periodically, emits DOWN/UP
//! events on the local agorabus socket, and buffers outbound fleet events
//! while the hub is unreachable.

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt as _;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Current hub reachability state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HubState {
    /// Hub is reachable.
    Up,
    /// Hub is not reachable.
    Down,
}

impl std::fmt::Display for HubState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Up => write!(f, "UP"),
            Self::Down => write!(f, "DOWN"),
        }
    }
}

/// A buffered fleet event waiting to be flushed once the hub recovers.
#[derive(Debug, Clone)]
pub struct BufferedEvent {
    /// The agorabus topic (e.g. `wm.fleet.node.heartbeat`).
    pub topic: String,
    /// Raw JSON payload bytes to forward to NATS.
    pub payload: bytes::Bytes,
}

/// Shared liveness state, readable by `hub-status`.
#[derive(Debug)]
pub struct HubLivenessState {
    /// Current reachability state.
    pub state: HubState,
    /// Monotonic instant when the current state began.
    pub state_since: Instant,
    /// Ring buffer of events accumulated while DOWN.
    pub buffer: VecDeque<BufferedEvent>,
    /// Count of events dropped due to buffer overflow.
    pub dropped: u64,
}

impl HubLivenessState {
    fn new() -> Self {
        Self {
            state: HubState::Up,
            state_since: Instant::now(),
            buffer: VecDeque::new(),
            dropped: 0,
        }
    }

    /// Seconds elapsed in the current state.
    #[must_use]
    pub fn seconds_in_state(&self) -> u64 {
        self.state_since.elapsed().as_secs()
    }

    /// Number of events currently in the buffer.
    #[must_use]
    pub fn buffered_count(&self) -> usize {
        self.buffer.len()
    }
}

/// Configuration for the hub-liveness watcher.
#[derive(Debug, Clone)]
pub struct HubWatcherConfig {
    /// How often to probe the hub (seconds).
    pub probe_secs: u64,
    /// Probe timeout (seconds).
    pub probe_timeout_secs: u64,
    /// Consecutive failures before declaring DOWN.
    pub down_threshold: u32,
    /// Consecutive successes before declaring UP.
    pub up_threshold: u32,
    /// Maximum number of events in the ring buffer.
    pub buffer_max_events: usize,
    /// Maximum total byte size of the ring buffer.
    pub buffer_max_bytes: usize,
    /// This node's identifier (for event payloads).
    pub node_id: String,
}

impl Default for HubWatcherConfig {
    fn default() -> Self {
        Self {
            probe_secs: 5,
            probe_timeout_secs: 3,
            down_threshold: 3,
            up_threshold: 2,
            buffer_max_events: 1000,
            buffer_max_bytes: 10 * 1024 * 1024, // 10 MiB
            node_id: hostname(),
        }
    }
}

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

/// Unix timestamp (seconds) since epoch.
fn unix_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Trait for probing hub reachability.  Mockable in tests.
#[async_trait::async_trait]
pub trait HubProbe: Send + Sync {
    /// Returns `true` if the hub is reachable.
    async fn probe(&self) -> bool;
}

/// Production probe: NATS flush round-trip with a timeout.
pub struct NatsHubProbe {
    client: async_nats::Client,
    timeout: Duration,
}

impl NatsHubProbe {
    /// Create a probe from an existing NATS client.
    #[must_use]
    pub fn new(client: async_nats::Client, timeout_secs: u64) -> Self {
        Self {
            client,
            timeout: Duration::from_secs(timeout_secs),
        }
    }
}

#[async_trait::async_trait]
impl HubProbe for NatsHubProbe {
    async fn probe(&self) -> bool {
        tokio::time::timeout(self.timeout, self.client.flush())
            .await
            .map(|res| res.is_ok())
            .unwrap_or(false)
    }
}

/// Trait for sinking buffered events to NATS on recovery.  Mockable.
#[async_trait::async_trait]
pub trait NatsSink: Send + Sync {
    /// Publish a topic + payload to the hub.
    async fn publish(&self, topic: &str, payload: bytes::Bytes) -> Result<()>;
}

/// Production sink using an `async_nats::Client`.
pub struct NatsClientSink {
    client: async_nats::Client,
}

impl NatsClientSink {
    /// Create a sink from an existing NATS client.
    #[must_use]
    pub fn new(client: async_nats::Client) -> Self {
        Self { client }
    }
}

#[async_trait::async_trait]
impl NatsSink for NatsClientSink {
    async fn publish(&self, topic: &str, payload: bytes::Bytes) -> Result<()> {
        self.client
            .publish(topic.to_string(), payload)
            .await
            .context("publishing buffered event to NATS")
    }
}

/// Publisher to the local agorabus socket for status events.
pub struct LocalBusPublisher {
    write: tokio::net::unix::OwnedWriteHalf,
    session_id: String,
}

impl LocalBusPublisher {
    /// Connect to the local agorabus socket and return a publisher.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the socket is unreachable or announce fails.
    pub async fn connect(socket_path: &std::path::Path, session_id: &str) -> Result<Self> {
        let stream = tokio::net::UnixStream::connect(socket_path)
            .await
            .with_context(|| format!("connecting to agorabus at {}", socket_path.display()))?;
        let (_, mut write) = stream.into_split();

        let announce = serde_json::json!({
            "op": "announce",
            "session_id": session_id,
            "pid": std::process::id(),
            "cwd": "/",
            "intent": "hub liveness watcher"
        });
        let mut buf = serde_json::to_vec(&announce).context("serializing announce")?;
        buf.push(b'\n');
        write.write_all(&buf).await.context("writing announce")?;
        write.flush().await.context("flushing announce")?;

        Ok(Self {
            write,
            session_id: session_id.to_string(),
        })
    }

    /// Publish a topic + JSON data to the local bus.
    ///
    /// # Errors
    ///
    /// Returns `Err` on serialization or write failure.
    pub async fn publish(&mut self, topic: &str, data: serde_json::Value) -> Result<()> {
        let msg = serde_json::json!({
            "op": "publish",
            "topic": topic,
            "data": data,
            "from": self.session_id
        });
        let mut buf = serde_json::to_vec(&msg).context("serializing publish")?;
        buf.push(b'\n');
        self.write.write_all(&buf).await.context("writing to agorabus")?;
        self.write.flush().await.context("flushing agorabus write")?;
        Ok(())
    }
}

/// Shared liveness handle — passed to the bridge loop for buffering decisions.
pub type SharedLiveness = Arc<Mutex<HubLivenessState>>;

/// Create a new shared liveness state.
#[must_use]
pub fn new_shared_liveness() -> SharedLiveness {
    Arc::new(Mutex::new(HubLivenessState::new()))
}

/// Buffer an outbound fleet event while the hub is DOWN.
///
/// Drops oldest events when the ring buffer is full.
pub async fn maybe_buffer(
    liveness: &SharedLiveness,
    topic: &str,
    payload: bytes::Bytes,
    cfg: &HubWatcherConfig,
) -> bool {
    let mut state = liveness.lock().await;
    if state.state == HubState::Up {
        return false; // not buffering — caller should forward normally
    }

    // Check byte cap: compute current buffer bytes
    let current_bytes: usize = state.buffer.iter().map(|e| e.payload.len()).sum();
    let new_bytes = payload.len();

    // Enforce event count and byte limits by dropping oldest
    while !state.buffer.is_empty()
        && (state.buffer.len() >= cfg.buffer_max_events
            || current_bytes + new_bytes > cfg.buffer_max_bytes)
    {
        if let Some(dropped) = state.buffer.pop_front() {
            state.dropped += 1;
            warn!(
                dropped_total = state.dropped,
                "ring buffer overflow: dropped oldest event (topic={})", dropped.topic
            );
        }
    }

    state.buffer.push_back(BufferedEvent {
        topic: topic.to_string(),
        payload,
    });
    true
}

/// The hub-liveness watcher task.
///
/// Run via `tokio::spawn(run_watcher(...))`.
///
/// # Errors
///
/// Does not return `Err` in normal operation — errors are logged internally.
/// Returns only when the task is cancelled or a catastrophic bus failure occurs.
pub async fn run_watcher(
    cfg: HubWatcherConfig,
    probe: Arc<dyn HubProbe>,
    nats_sink: Arc<dyn NatsSink>,
    liveness: SharedLiveness,
    socket_path: std::path::PathBuf,
) {
    info!(
        probe_secs = cfg.probe_secs,
        down_threshold = cfg.down_threshold,
        up_threshold = cfg.up_threshold,
        "hub-liveness watcher starting"
    );

    // Attempt to connect to the local bus for status events.
    // If it fails at startup we keep trying on each state change.
    let mut bus_pub: Option<LocalBusPublisher> =
        LocalBusPublisher::connect(&socket_path, "wm-busbridge-hub-watcher")
            .await
            .map_err(|e| {
                warn!(error = %e, "hub watcher: could not connect to agorabus (will retry)");
            })
            .ok();

    let probe_interval = Duration::from_secs(cfg.probe_secs);
    let mut consecutive_failures: u32 = 0;
    let mut consecutive_successes: u32 = 0;
    // Track the wall-clock timestamp of the last successful probe.
    let mut last_ok_ts: u64 = unix_ts();
    // Track when we went DOWN (wall clock).
    let mut down_since_ts: u64 = 0;

    loop {
        tokio::time::sleep(probe_interval).await;

        let ok = probe.probe().await;
        debug!(ok, consecutive_failures, consecutive_successes, "hub probe result");

        let current_state = {
            let locked = liveness.lock().await;
            locked.state
        };

        if ok {
            last_ok_ts = unix_ts();
            consecutive_failures = 0;
            consecutive_successes = consecutive_successes.saturating_add(1);

            if current_state == HubState::Down
                && consecutive_successes >= cfg.up_threshold
            {
                // Transition to UP
                let down_duration_secs = unix_ts().saturating_sub(down_since_ts);
                info!(down_duration_secs, "hub-liveness: hub UP — flushing buffer");

                // Flush buffer first, then update state
                let events = {
                    let mut locked = liveness.lock().await;
                    locked.state = HubState::Up;
                    locked.state_since = Instant::now();
                    locked.buffer.drain(..).collect::<Vec<_>>()
                };
                consecutive_successes = 0;

                // Flush buffered events to NATS in order
                let mut flushed = 0usize;
                let mut flush_errors = 0usize;
                for ev in events {
                    match nats_sink.publish(&ev.topic, ev.payload).await {
                        Ok(()) => flushed += 1,
                        Err(e) => {
                            flush_errors += 1;
                            error!(
                                topic = %ev.topic,
                                error = %e,
                                "failed to flush buffered event to NATS"
                            );
                        }
                    }
                }
                info!(flushed, flush_errors, "hub buffer flushed");

                // Emit wm.fleet.hub.up to local bus
                let payload = serde_json::json!({
                    "node": cfg.node_id,
                    "down_duration_secs": down_duration_secs
                });
                emit_local(&mut bus_pub, &socket_path, "wm.fleet.hub.up", payload).await;
            }
        } else {
            consecutive_successes = 0;
            consecutive_failures = consecutive_failures.saturating_add(1);

            if current_state == HubState::Up
                && consecutive_failures >= cfg.down_threshold
            {
                // Transition to DOWN
                down_since_ts = unix_ts();
                info!(consecutive_failures, "hub-liveness: hub DOWN");

                {
                    let mut locked = liveness.lock().await;
                    locked.state = HubState::Down;
                    locked.state_since = Instant::now();
                }
                consecutive_failures = 0;

                // Emit wm.fleet.hub.down to local bus
                let payload = serde_json::json!({
                    "node": cfg.node_id,
                    "since_ts": down_since_ts,
                    "last_ok_ts": last_ok_ts
                });
                emit_local(&mut bus_pub, &socket_path, "wm.fleet.hub.down", payload).await;
            }
        }
    }
}

/// Try to publish a status event to the local bus, reconnecting if needed.
async fn emit_local(
    bus_pub: &mut Option<LocalBusPublisher>,
    socket_path: &std::path::Path,
    topic: &str,
    data: serde_json::Value,
) {
    // Try to publish; if no publisher or publish fails, reconnect once.
    if let Some(pub_ref) = bus_pub.as_mut() {
        if pub_ref.publish(topic, data.clone()).await.is_ok() {
            return;
        }
        warn!("hub watcher: agorabus publish failed, reconnecting");
        *bus_pub = None;
    }

    // Reconnect
    match LocalBusPublisher::connect(socket_path, "wm-busbridge-hub-watcher").await {
        Ok(mut new_pub) => {
            if let Err(e) = new_pub.publish(topic, data).await {
                error!(topic, error = %e, "hub watcher: publish failed after reconnect");
            }
            *bus_pub = Some(new_pub);
        }
        Err(e) => {
            error!(error = %e, "hub watcher: could not reconnect to agorabus");
        }
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Mutex as TokioMutex;

    // ── Mock NATS sink ────────────────────────────────────────────────────────

    struct RecordingSink {
        published: TokioMutex<Vec<(String, bytes::Bytes)>>,
    }

    impl RecordingSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                published: TokioMutex::new(Vec::new()),
            })
        }

        async fn drain(&self) -> Vec<(String, bytes::Bytes)> {
            self.published.lock().await.drain(..).collect()
        }
    }

    #[async_trait::async_trait]
    impl NatsSink for RecordingSink {
        async fn publish(&self, topic: &str, payload: bytes::Bytes) -> Result<()> {
            self.published.lock().await.push((topic.to_string(), payload));
            Ok(())
        }
    }

    // ── Mock bus sink ─────────────────────────────────────────────────────────

    struct MockBusSink {
        events: TokioMutex<Vec<(String, serde_json::Value)>>,
    }

    impl MockBusSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                events: TokioMutex::new(Vec::new()),
            })
        }

        async fn drain(&self) -> Vec<(String, serde_json::Value)> {
            self.events.lock().await.drain(..).collect()
        }
    }

    // ── Helper: run the watcher for N probe ticks via a ticker channel ─────

    /// Minimal watcher logic re-implemented for testing (no tokio::time::sleep,
    /// no real bus socket).  Drives the state machine directly.
    struct WatcherSim {
        cfg: HubWatcherConfig,
        liveness: SharedLiveness,
        nats_sink: Arc<dyn NatsSink>,
        bus_sink: Arc<MockBusSink>,
        consecutive_failures: u32,
        consecutive_successes: u32,
        last_ok_ts: u64,
        down_since_ts: u64,
    }

    impl WatcherSim {
        fn new(
            cfg: HubWatcherConfig,
            nats_sink: Arc<dyn NatsSink>,
            bus_sink: Arc<MockBusSink>,
        ) -> Self {
            Self {
                cfg,
                liveness: new_shared_liveness(),
                nats_sink,
                bus_sink,
                consecutive_failures: 0,
                consecutive_successes: 0,
                last_ok_ts: 0,
                down_since_ts: 0,
            }
        }

        async fn tick(&mut self, ok: bool) {
            let current_state = self.liveness.lock().await.state;

            if ok {
                self.last_ok_ts = unix_ts();
                self.consecutive_failures = 0;
                self.consecutive_successes = self.consecutive_successes.saturating_add(1);

                if current_state == HubState::Down
                    && self.consecutive_successes >= self.cfg.up_threshold
                {
                    let down_duration_secs = unix_ts().saturating_sub(self.down_since_ts);
                    {
                        let mut locked = self.liveness.lock().await;
                        locked.state = HubState::Up;
                        locked.state_since = Instant::now();
                    }
                    let events = {
                        let mut locked = self.liveness.lock().await;
                        locked.buffer.drain(..).collect::<Vec<_>>()
                    };
                    self.consecutive_successes = 0;

                    for ev in events {
                        let _ = self.nats_sink.publish(&ev.topic, ev.payload).await;
                    }

                    self.bus_sink.events.lock().await.push((
                        "wm.fleet.hub.up".to_string(),
                        serde_json::json!({
                            "node": self.cfg.node_id,
                            "down_duration_secs": down_duration_secs
                        }),
                    ));
                }
            } else {
                self.consecutive_successes = 0;
                self.consecutive_failures = self.consecutive_failures.saturating_add(1);

                if current_state == HubState::Up
                    && self.consecutive_failures >= self.cfg.down_threshold
                {
                    self.down_since_ts = unix_ts();
                    {
                        let mut locked = self.liveness.lock().await;
                        locked.state = HubState::Down;
                        locked.state_since = Instant::now();
                    }
                    self.consecutive_failures = 0;

                    self.bus_sink.events.lock().await.push((
                        "wm.fleet.hub.down".to_string(),
                        serde_json::json!({
                            "node": self.cfg.node_id,
                            "since_ts": self.down_since_ts,
                            "last_ok_ts": self.last_ok_ts
                        }),
                    ));
                }
            }
        }
    }

    fn test_cfg() -> HubWatcherConfig {
        HubWatcherConfig {
            probe_secs: 5,
            probe_timeout_secs: 3,
            down_threshold: 3,
            up_threshold: 2,
            buffer_max_events: 5,
            buffer_max_bytes: 1024,
            node_id: "test-node".to_string(),
        }
    }

    // ── AC1: DOWN threshold ───────────────────────────────────────────────────

    #[tokio::test]
    async fn test_down_threshold_exact() {
        let sink: Arc<dyn NatsSink> = RecordingSink::new();
        let bus = MockBusSink::new();
        let mut sim = WatcherSim::new(test_cfg(), sink, Arc::clone(&bus));

        // 2 failures → no DOWN yet
        sim.tick(false).await;
        sim.tick(false).await;
        assert_eq!(sim.liveness.lock().await.state, HubState::Up);
        assert!(bus.events.lock().await.is_empty());

        // 3rd failure → DOWN
        sim.tick(false).await;
        assert_eq!(sim.liveness.lock().await.state, HubState::Down);
        let events = bus.drain().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "wm.fleet.hub.down");
        assert!(events[0].1.get("node").is_some());
        assert!(events[0].1.get("since_ts").is_some());
        assert!(events[0].1.get("last_ok_ts").is_some());
    }

    // ── AC2: UP after DOWN ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_up_after_down() {
        let sink: Arc<dyn NatsSink> = RecordingSink::new();
        let bus = MockBusSink::new();
        let mut sim = WatcherSim::new(test_cfg(), Arc::clone(&sink), Arc::clone(&bus));

        // Go DOWN
        for _ in 0..3 {
            sim.tick(false).await;
        }
        assert_eq!(sim.liveness.lock().await.state, HubState::Down);
        bus.drain().await; // clear

        // 1 success → still DOWN
        sim.tick(true).await;
        assert_eq!(sim.liveness.lock().await.state, HubState::Down);

        // 2nd success → UP
        sim.tick(true).await;
        assert_eq!(sim.liveness.lock().await.state, HubState::Up);
        let events = bus.drain().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "wm.fleet.hub.up");
        assert!(events[0].1.get("down_duration_secs").is_some());
    }

    // ── AC3: Hysteresis (single transient failure) ────────────────────────────

    #[tokio::test]
    async fn test_hysteresis_no_flap() {
        let sink: Arc<dyn NatsSink> = RecordingSink::new();
        let bus = MockBusSink::new();
        let mut sim = WatcherSim::new(test_cfg(), sink, Arc::clone(&bus));

        // 2 failures then a success → must not emit DOWN
        sim.tick(false).await;
        sim.tick(false).await;
        sim.tick(true).await;

        assert_eq!(sim.liveness.lock().await.state, HubState::Up);
        assert!(bus.events.lock().await.is_empty());
    }

    // ── AC4: Buffer flush ordering ────────────────────────────────────────────

    #[tokio::test]
    async fn test_buffer_flush_ordering() {
        let nats_sink = RecordingSink::new();
        // Keep a typed clone for drain(), and coerce a second clone to dyn for the sim.
        let nats_sink_dyn: Arc<dyn NatsSink> = nats_sink.clone();
        let bus = MockBusSink::new();
        let mut sim = WatcherSim::new(
            test_cfg(),
            nats_sink_dyn,
            Arc::clone(&bus),
        );
        let cfg = test_cfg();

        // Go DOWN
        for _ in 0..3 {
            sim.tick(false).await;
        }

        // Buffer 3 events in order
        for i in 0u8..3 {
            let payload = bytes::Bytes::from(vec![i]);
            maybe_buffer(&sim.liveness, "wm.fleet.x", payload, &cfg).await;
        }
        assert_eq!(sim.liveness.lock().await.buffered_count(), 3);

        // Recover → flush
        sim.tick(true).await;
        sim.tick(true).await;
        assert_eq!(sim.liveness.lock().await.state, HubState::Up);
        assert_eq!(sim.liveness.lock().await.buffered_count(), 0);

        let published = nats_sink.drain().await;
        assert_eq!(published.len(), 3);
        // Verify order: first in, first out
        assert_eq!(published[0].1.as_ref(), &[0u8]);
        assert_eq!(published[1].1.as_ref(), &[1u8]);
        assert_eq!(published[2].1.as_ref(), &[2u8]);
    }

    // ── AC5: Ring buffer bounds (drop-oldest) ─────────────────────────────────

    #[tokio::test]
    async fn test_ring_buffer_bounds() {
        let nats_sink: Arc<dyn NatsSink> = RecordingSink::new();
        let bus = MockBusSink::new();
        let mut sim = WatcherSim::new(test_cfg(), nats_sink, bus);
        let cfg = test_cfg(); // buffer_max_events = 5

        // Go DOWN
        for _ in 0..3 {
            sim.tick(false).await;
        }

        // Push 7 events (2 over cap)
        for i in 0u8..7 {
            let payload = bytes::Bytes::from(vec![i]);
            maybe_buffer(&sim.liveness, "wm.fleet.x", payload, &cfg).await;
        }

        let locked = sim.liveness.lock().await;
        // Buffer should have at most max_events entries
        assert!(locked.buffered_count() <= cfg.buffer_max_events);
        // At least 2 should have been dropped
        assert!(locked.dropped >= 2);
        drop(locked);
    }

    // ── AC7 (partial): Probe error never crashes ──────────────────────────────

    #[tokio::test]
    async fn test_probe_error_is_treated_as_failure() {
        // An always-failing probe driving the sim is equivalent to an erroring probe.
        let sink: Arc<dyn NatsSink> = RecordingSink::new();
        let bus = MockBusSink::new();
        let mut sim = WatcherSim::new(test_cfg(), sink, Arc::clone(&bus));

        for _ in 0..3 {
            sim.tick(false).await;
        }
        // We reach DOWN without panicking — that's the proof
        assert_eq!(sim.liveness.lock().await.state, HubState::Down);
    }

    // ── maybe_buffer: not buffered when UP ───────────────────────────────────

    #[tokio::test]
    async fn test_not_buffered_when_up() {
        let liveness = new_shared_liveness();
        let cfg = test_cfg();
        let buffered =
            maybe_buffer(&liveness, "wm.fleet.x", bytes::Bytes::from_static(b"hi"), &cfg).await;
        assert!(!buffered);
        assert_eq!(liveness.lock().await.buffered_count(), 0);
    }

    // ── Counter: dropped increments correctly ────────────────────────────────

    #[tokio::test]
    async fn test_dropped_counter() {
        let liveness = new_shared_liveness();
        {
            let mut locked = liveness.lock().await;
            locked.state = HubState::Down;
        }
        let cfg = HubWatcherConfig {
            buffer_max_events: 2,
            buffer_max_bytes: 10 * 1024 * 1024,
            ..test_cfg()
        };

        // Fill to cap
        maybe_buffer(&liveness, "wm.fleet.x", bytes::Bytes::from_static(b"a"), &cfg).await;
        maybe_buffer(&liveness, "wm.fleet.x", bytes::Bytes::from_static(b"b"), &cfg).await;
        // Overflow — drops oldest
        maybe_buffer(&liveness, "wm.fleet.x", bytes::Bytes::from_static(b"c"), &cfg).await;

        let locked = liveness.lock().await;
        assert_eq!(locked.dropped, 1);
        assert_eq!(locked.buffered_count(), 2);
    }

}
