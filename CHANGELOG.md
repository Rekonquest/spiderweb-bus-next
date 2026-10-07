# Changelog

All notable changes to the Spiderweb Bus. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/); the project follows semver
once it reaches `1.0.0`.

## [Unreleased] — 0.1.0 path

The in-process core is robust, observable, and tested. The distributed edge
layer is an honest trusted-network MVP. See
[PRODUCTION-READINESS.md](PRODUCTION-READINESS.md) for the full tracking board.

### Added — architecture
- Typed pub/sub kernel with fan-out by `TypeId`; std-only, zero-dep,
  `#![forbid(unsafe_code)]`.
- Emergent **fabric**: threads (desire-paths), intersections, transfer policies
  (`Stay`/`HopTo`/`MergeAll`); `Bus::fabric_snapshot()` + weighted `adjacency()`.
- **Vibrations** (`Backpressure`, `Starved`) on `SYS_VIBRATION`.
- **Highway** parallel layer (on-ramp/off-ramp lane pool, backpressure).
- **Preload** read-ahead source (starvation signal).
- **Self-registration** (`advertise!`) + `bus.auto_connect()` (via the
  `BusAutoConnect` extension trait).
- 3D spatial overlay + `spiderweb-viz` (JSON / interactive HTML scene).
- **`spiderweb-edge`**: cross-process/P2P transport over TCP (std-only) with
  bounded reliable delivery (session + seq + resend ring + dedup), including
  **session-ID restart recovery** (a restarted sender is recovered, not blacked
  out) and clean `EdgeListener::Drop` teardown.
- **`spiderweb-lanes`**: external protocol **lanes** as on-ramps/off-ramps —
  a unifying `Lane` contract (`install() -> LaneParts`) plus: **HTTP** (std-only
  HTTP/1.1 server + client), **API** (typed request/reply RPC over the bus,
  exposed over HTTP as JSON), **P2P mesh** (N-peer, per-origin exactly-once +
  loop suppression), **NAT relay** (`relayd` rendezvous, traverses NAT by
  dialing out), and **SSH** (bus over a composed `ssh -L`/`-R` tunnel — encrypted
  with zero crypto deps). std-only except the SSH lane's `std::process` of `ssh`.
- **`spiderweb-heat`**: graph heat-kernel attenuation (`H_t = exp(−tL)`).

### Hardened — production readiness (Phases 1–2)
- **Panic isolation**: a panicking strand becomes a `Crashed` lifecycle event
  (spider policy applies), never a silent worker death; panic message recovered.
- **Graceful shutdown**: cooperative flag + bounded best-effort worker join, so
  strands are dropped (`Drop` runs); `BusHandle::is_shutting_down()`.
- **Kernel poison-tolerance**: all locks recover from poison; one panic can't
  cascade-kill the bus.
- **No-panic untrusted input**: `read_frame` bounds-checks before allocating;
  `read_frame`/`write_frame` made `impl Read`/`Write`-generic for testing.
- **Pull-able telemetry** `BusStats` (kernel-wide + lifecycle + per-strand).
- **Bounded-everything** audit; fixed an unbounded subscriber-map leak under
  crash/restart churn (lazy pruning).
- **`webctl top`** live dashboard; `EdgeListener::Drop` clean teardown.

### Hardened — lanes (post-audit)
A multi-agent adversarial audit of `spiderweb-lanes` found 16 confirmed
reliability issues; all fixed + regression-tested. The systemic one: the kernel
has no per-tick watchdog, so a blocking `run()` wedges that strand permanently —
several findings were permanent-wedge DoS vectors.
- **HTTP**: response writes moved off the strand tick onto a worker pool (a slow
  consumer can't head-of-line-block others); write timeout on parked sockets;
  parked-connection table reaped (orphans can't reach `MAX_PENDING` → 503).
- **API**: `ApiClient`/`ApiHttpBridge` pending maps reaped (no corpse wedge);
  per-instance RPC id prefixes (two clients on one bus never mis-route).
- **mesh**: egress does network I/O outside the `PeerBook` lock (one slow peer
  can't wedge the tick); per-connection reader threads (a node serves all
  inbound peers, not just the first).
- **NAT**: write timeouts on relay peers; `serve_until`/`Drop` teardown stops
  lingering reader threads; bounded relay-reader connect.
- **SSH**: `--` before the host (option-injection guard).

### Tested — quality gates (Phase 4)
- Randomized **fuzz** of the wire parser (200k adversarial inputs, std-only).
- **Property tests**: exactly-once fan-out (random shapes); heat-kernel mass
  conservation (3000 random graphs).
- **CI**: `fmt --check` + `clippy -D warnings` + `test` on every push/PR.

### Known gaps (roadmap)
- Edge transport is **plaintext, trusted-network only** — no per-frame MAC
  (auth), no TLS (where the zero-dep story ends and crypto deps begin).
- Edge reliability is a bounded resend ring (sender restarts handled via session
  IDs); unbounded/no-head-of-line-blocking reliability (RLNC tier-2) and
  cross-machine causality (ITC) are the remaining tiers.
- Lanes: HTTP/API/mesh/NAT are plaintext — the **SSH lane** is how you encrypt
  any of them today (composes system `ssh`, zero crypto deps). The HTTP lane is
  HTTP/1.1 keep-alive (one request in flight per connection, no pipelining); the
  NAT relay is a coordination convenience (single point of failure, double
  bandwidth), not a trust boundary.
