# Changelog

## v0.2.0 — 2026-06-05

Adds the constellation-dispatch layer to agorabus-nats-bridge: a JetStream
work-queue (WM_WORK stream, deliver-once/ack-removes), per-node pull consumers,
a capability registry (WM_NODES KV with heartbeat TTL), and a coordinator that
routes GPU jobs to vram_gb>0 nodes and build jobs to highest-core/lowest-load
nodes. Includes payload-size enforcement (no large blobs on the bus) and
explicit logging for dropped/unplaceable jobs.
