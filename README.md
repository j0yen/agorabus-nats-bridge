# agorabus-nats-bridge

A per-machine sidecar (`wm-busbridge`) that mirrors fleet events between a local [agorabus](https://github.com/j0yen/agorabus) Unix-domain socket and a NATS leaf node, so a single-host bus becomes a fleet-wide one.

## Why it exists

agorabus coordinates Claude sessions on one machine over a Unix socket. Once there's more than one machine, the sessions on each are blind to the others again — the same problem agorabus solved locally, now a level up. The bridge answers it without changing anything local: it mirrors only the `wm.fleet.*` events that other machines need to see onto NATS, and leaves everything else — high-volume topics like `wm.audio.speech.chunk`, anything not allowlisted — on the local socket. Existing agorabus clients don't know the bridge exists; `agorabus peers` works the same whether or not it's running.

Two properties make that safe. A **loop guard** ensures an event arriving from NATS is never re-published back to NATS, so a message crosses the bridge at most once per direction. And forwarding is **selective by default** — only `wm.fleet.>` plus an optional allowlist crosses; nothing else leaks off the machine.

## Install

```sh
cargo build --release
install -Dm755 target/release/wm-busbridge ~/.local/bin/wm-busbridge
```

Needs `cargo` / `rustc` 1.85+.

## Quickstart

Run the bridge against the local agorabus socket and a NATS leaf:

```sh
wm-busbridge --socket ~/.cache/agorabus/sock --nats-url nats://127.0.0.1:4222
```

With agorabus and the bridge both up, a `wm.fleet.*` event published on the local bus appears on the matching NATS subject, and `agorabus peers --fleet` on any node merges in the remote peers. To check the wiring end to end:

```sh
wm-busbridge selftest   # publishes a tagged event locally, confirms it on NATS exactly once, round-trips it back; exit 0 on success
```

## Subcommands

| Command | What it does |
|---------|--------------|
| `run` | run the bridge daemon (the default when no subcommand is given) |
| `selftest` | end-to-end check: local publish → NATS exactly-once → round-trip; exit 0 on pass |
| `nats-config hub\|leaf` | print a ready-to-use NATS server config with the JetStream domain set |
| `hub-status` | print hub reachability — exit 0 if UP, 1 if DOWN; `--json` for the structured form |
| `status` | the live fleet dashboard — KV node registry plus work-queue depth, with STALE detection |

```sh
# NATS topology config (hub and leaf variants)
wm-busbridge nats-config hub  --domain hub    --hub-url "" --creds-path /etc/nats/hub.creds
wm-busbridge nats-config leaf --domain laptop --hub-url nats://cloud.ts.example.com:7422

# Fleet dashboard; nodes silent longer than --stale-secs are flagged STALE
wm-busbridge status --stale-secs 90
wm-busbridge status --json
```

### Hub-liveness watcher

The `run` daemon probes the hub continuously — a NATS ping/flush every 5s — with hysteresis so a single blip doesn't flap the state: three failures in a row mark the hub DOWN, two successes bring it back UP. Each transition publishes a state-change event on the local bus (`wm.fleet.hub.down` / `wm.fleet.hub.up`), so local sessions can react to losing the fleet. Events are held in a bounded ring buffer (1000 events / 10 MiB, drop-oldest with a dropped counter) so a long outage can't grow memory without bound.

## Configuration

| Variable | Meaning |
|----------|---------|
| `WM_BUS_SOCKET` | path to the agorabus UDS (default `~/.cache/agorabus/sock`) |
| `WM_NATS_URL` | NATS server URL (default `nats://127.0.0.1:4222`) |
| `WM_BRIDGE_ALLOW` | comma-separated extra fleet-topic prefixes to forward, beyond `wm.fleet.>` |

Per-node NATS credentials come from the encrypted credential store — there are no plaintext credentials in this repo.

## Where it fits

This is the fleet-scale layer above [agorabus](https://github.com/j0yen/agorabus) in the wintermute constellation: agorabus is the single-host bus, `wm-busbridge` is the sidecar that joins each host to the others over NATS, and the hub is the rendezvous point the leaves dial. It was built through the [autobuilder](https://github.com/j0yen/autobuilder) pipeline.

## License

Dual-licensed under MIT or Apache-2.0, at your option.
