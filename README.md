# agorabus-nats-bridge

agorabus ↔ NATS bridge daemon that makes the wintermute bus fleet-wide.
A small `wm-busbridge` sidecar per machine mirrors allowlisted `wm.fleet.*`
events between the local agorabus Unix-domain socket and a NATS leaf node,
while local-only high-volume topics (e.g. `wm.audio.speech.chunk`) stay on
the UDS — existing local clients require no changes.

## Features

- **Bridge daemon** — subscribes to the local agorabus UDS and a NATS leaf
  at `127.0.0.1:4222`, identity-mapping `wm.<topic>` subjects bidirectionally.
- **Loop guard** — events arriving *from* NATS are not re-published back to
  NATS; each message traverses the bridge at most once per direction.
- **Selective forwarding** — only `wm.fleet.>` and an optional allowlist cross
  to NATS; local-only and high-volume topics stay local.
- **NATS topology config** — `wm-busbridge nats-config hub|leaf` prints the
  ready-to-use NATS server config with JetStream domain for hub and leaf nodes.
- **End-to-end selftest** — `wm-busbridge selftest` publishes a tagged event
  locally and confirms it appears on NATS exactly once, then round-trips back.
- **SIGPIPE-safe** — `sigpipe::reset()` first line of `main()`; no broken-pipe
  panics when piping output.

## Acceptance criteria

1. `wm-busbridge` mirrors an allowlisted `wm.fleet.*` event onto the matching
   NATS subject — verified against an embedded/test NATS server.
2. Existing agorabus UDS clients (e.g. `agorabus peers`) work identically with
   the bridge running, requiring no code or socket change.
3. Loop guard: an event injected from NATS into the local bus is NOT
   re-published back to NATS (exactly-once per direction).
4. Selective forwarding: high-volume / local-only topics are NOT forwarded to
   NATS; only allowlisted fleet topics cross.
5. NATS topology config sets a JetStream domain on the hub and leaves; a leaf
   client can reach a hub JetStream stream via the domain-qualified API prefix.
6. Per-node NATS credentials come from the encrypted store; no plaintext creds
   in the repo (grep-asserted).
7. `wm-busbridge selftest` passes end-to-end and the daemon does not panic on a
   closed pipe (SIGPIPE reset present).

## Install

```sh
cargo build --release
install -Dm755 target/release/wm-busbridge ~/.local/bin/wm-busbridge
```

## Usage

```sh
# Run the bridge daemon
wm-busbridge --socket ~/.cache/agorabus/sock --nats-url nats://127.0.0.1:4222

# Run end-to-end selftest
wm-busbridge selftest

# Print hub NATS config
wm-busbridge nats-config hub --domain hub --hub-url "" --creds-path /etc/nats/hub.creds

# Print leaf NATS config
wm-busbridge nats-config leaf --domain laptop --hub-url nats://cloud.ts.example.com:7422
```

Environment variables:
- `WM_BUS_SOCKET` — path to the agorabus UDS (default: `~/.cache/agorabus/sock`)
- `WM_NATS_URL` — NATS server URL (default: `nats://127.0.0.1:4222`)
- `WM_BRIDGE_ALLOW` — comma-separated additional fleet topic prefixes to forward

## License

MIT OR Apache-2.0 — Joe Yen
