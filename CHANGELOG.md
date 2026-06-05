# Changelog

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
