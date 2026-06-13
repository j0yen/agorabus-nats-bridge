# Changelog

## v0.3.0 — 2026-06-13

Hub-liveness watcher added to agorabus-nats-bridge Run daemon. Implements NATS
ping/flush probing (every 5s), DOWN/UP hysteresis (3 failures → DOWN, 2
successes → UP), state-change events on local agorabus (wm.fleet.hub.down /
wm.fleet.hub.up), bounded ring buffer (1000 events / 10MiB) with drop-oldest
and dropped counter, and wm-busbridge hub-status [--json] subcommand (exit 0=UP,
1=DOWN). All 7 ACs pass in acceptance test suite.

## v0.2.0 — 2026-06-13

Added hub-liveness watcher to agorabus-nats-bridge:
- HubWatcherConfig (probe_secs=5, down_threshold=3, up_threshold=2)
- run_watcher async task: NATS flush probe, DOWN→wm.fleet.hub.down on local bus, UP→wm.fleet.hub.up + buffer flush
- Ring buffer: 1000 events / 10 MiB cap, drop-oldest with dropped counter
- bridge::run wires watcher + buffers outbound events while hub DOWN
- wm-busbridge hub-status [--json]: exit 0=UP, 1=DOWN; JSON: state/seconds_in_state/buffered/dropped
- 8 unit tests covering all ACs; all 42+21 tests green

## v0.2.0 — 2026-06-05

Adds the constellation-dispatch layer to agorabus-nats-bridge: a JetStream
work-queue (WM_WORK stream, deliver-once/ack-removes), per-node pull consumers,
a capability registry (WM_NODES KV with heartbeat TTL), and a coordinator that
routes GPU jobs to vram_gb>0 nodes and build jobs to highest-core/lowest-load
nodes. Includes payload-size enforcement (no large blobs on the bus) and
explicit logging for dropped/unplaceable jobs.

## v0.1.1 — 2026-06-05

constellation-cloud-build: cloud build routing — sccache-dist cloud servers, burst pods,
no_build routing invariant (5700U/local-llm never receives build or test jobs), cost
guardrails, and wm-work build offload dispatch. Adds dispatch layer (capability registry,
work-queue, coordinator) with AC3+AC4 no_build enforcement and 7-AC acceptance coverage.
