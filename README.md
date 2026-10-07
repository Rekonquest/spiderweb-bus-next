# Spiderweb Bus

A layered, organic **in-process message bus for Rust**. No Tokio. No async. Just
threads, channels, and types. The kernel is `std`-only, **zero external
dependencies**, and `#![forbid(unsafe_code)]`.

Strands (lightweight worker units) publish and subscribe by **type** on named
sockets. The bus fans out, forms emergent **threads** (the desire-paths messages
trace), grows **intersections** where they cross, and propagates **vibrations**
(backpressure, starvation) across the web — and a **spider** orchestrator
supervises it all, restarting or quarantining strands without the kernel knowing
any policy.

> **Status:** the in-process core is robust + observable + tested (see
> [PRODUCTION-READINESS.md](PRODUCTION-READINESS.md)). The distributed edge layer
> (`spiderweb-edge`) is an honest **trusted-network MVP** — reliable framing, but
> no auth/encryption yet (documented below).

## Quickstart

```rust
use spiderweb::{Bus, BusHandle, Socket, Strand, StrandError};
use std::time::Duration;

struct Source { left: usize }
impl Strand for Source {
    fn name(&self) -> &str { "source" }
    fn inputs(&self) -> &[Socket] { &[] }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<String>("greeting");
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        if self.left == 0 { return Err(StrandError::Detach); }
        bus.publish_static("greeting", format!("hello #{}", self.left))?;
        self.left -= 1;
        bus.sleep(Duration::from_millis(50));
        Ok(())
    }
}

struct Sink;
impl Strand for Sink {
    fn name(&self) -> &str { "sink" }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<String>("greeting");
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] { &[] }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for msg in bus.recv::<String>("greeting")? {
            bus.log(&format!("got: {msg}"));
        }
        bus.sleep(Duration::from_millis(50));
        Ok(())
    }
}

fn main() {
    let mut bus = Bus::open();
    bus.register(Source { left: 5 });
    bus.register(Sink);
    bus.run_until(Some(Duration::from_secs(2)));
}
```

No wiring: the sink subscribes to `String` on `"greeting"`, the source publishes
there, and the bus connects them by type. Add a `Spider` and crashed strands get
restarted; add a `Highway` and a batch fans across parallel lanes.

## Architecture in one breath

Ground level is the comfy bus (typed pub/sub). Above it, an optional **highway**
lifts a batch into parallel lanes and drops the results back down. The **fabric**
(threads + intersections) forms from real traffic — it is observed, never
declared. **Vibrations** carry cross-layer signals. The **spider** sits in the
middle and reacts. Mechanism lives in the kernel; *all* policy lives in the
spider. See [`crates/spiderweb/src/lib.rs`](crates/spiderweb/src/lib.rs) for the
kernel and the architecture notes throughout.

## Crates

| Crate | What it is |
|---|---|
| **`spiderweb`** | The kernel: typed pub/sub, fan-out, the fabric, lifecycle, `BusStats` telemetry. std-only, zero-dep, forbid-unsafe. |
| **`spider`** | The orchestrator strand — restart/quarantine policy over `sys.lifecycle` / `sys.control`. |
| **`highway`** | Layer-2 on-ramp/off-ramp around a parallel lane pool; emits backpressure vibrations. |
| **`preload`** | Background read-ahead source; emits starvation vibrations. |
| **`spiderweb-registry`** | Self-registration (`advertise!`) + `bus.auto_connect()` (via the `BusAutoConnect` extension trait; quarantines the one `inventory` dep). |
| **`spiderweb-edge`** | Cross-process / P2P transport over TCP (std-only). Reliable framing; **trusted-network MVP**. |
| **`spiderweb-lanes`** | External protocol **lanes** — HTTP, JSON-RPC API, P2P mesh, NAT relay, SSH tunnel — as on-ramps/off-ramps to the outside world. std-only (the SSH lane composes the system `ssh`). |
| **`spiderweb-heat`** | Graph heat-kernel attenuation (`H_t = exp(−tL)`) — topology-aware vibration decay. |
| **`spiderweb-viz`** | Export the fabric as a 3D scene (JSON / interactive HTML); `render_top` dashboard. |
| **`tools/webctl`** | CLI: `list`, `run <demo>`, `lanes`, `top` (live `BusStats` dashboard), `topology`. |
| **`cap-*`** | Demo strands (source/sink/slow-sink/crasher/image pipeline). |

## Lanes — talking to the outside world

`spiderweb-lanes` makes the bus reach past one process. Every lane is the same
shape: an **ingress** strand (external → decode → publish) + an **egress** strand
(subscribe → encode → send), generalizing the edge sender/listener. Install one
with `bus.add_lane("name", &lane)`.

| Lane | What it does |
|---|---|
| **`tcp`** | The reference lane — re-expresses the unchanged `spiderweb-edge` transport. |
| **`http`** | std-only HTTP/1.1 server (requests → bus, bus → responses) + client (bus → outbound calls → bus, on a worker pool). |
| **`api`** | Typed request/reply RPC over the bus (`ApiServer`/`ApiCaller`), exposed over HTTP as JSON (`POST /api {"method","params"}`). |
| **`mesh`** | N-peer P2P: each node originates + forwards; per-origin dedup delivers each message everywhere exactly once and suppresses loops. |
| **`nat`** | NAT traversal by **rendezvous relay** — both peers dial out to a relay (`relayd`) and federate by room. No inbound port needed. |
| **`ssh`** | Bus over a composed `ssh -L`/`-R` port-forward — encrypted + authenticated with **zero crypto deps** (drives the system `ssh`). |

```sh
cargo run -p spiderweb-lanes --example http_api    # curl JSON-RPC at :8787
cargo run -p spiderweb-lanes --example p2p_mesh     # 3-node mesh, exactly-once
cargo run -p spiderweb-lanes --bin relayd -- 0.0.0.0:9000   # the NAT relay
```

## What's solid vs. honest gaps

**Solid (tested, reviewed):** panic isolation, graceful shutdown + bounded
worker join, kernel poison-tolerance, no-panic untrusted-input parsing, pull-able
`BusStats`, bounded-everything (incl. a fixed subscriber-leak), `EdgeListener`
clean teardown, **session-ID restart recovery** (a restarted sender is recovered,
not blacked out), fuzzed wire parser, exactly-once + mass-conservation property
tests, CI (fmt + clippy `-D warnings` + test).

**Honest gaps (roadmap):** the edge layer is **plaintext, trusted-network only** —
no per-frame MAC (auth), no TLS yet (this is where the zero-dep story ends and
crypto deps begin). Edge reliability is a bounded resend ring (recovers up to
`RING_CAP` frames of disconnect, with session IDs handling sender restarts);
unbounded/no-head-of-line-blocking reliability (RLNC tier-2) and cross-machine
causality (ITC) are the remaining tiers. The HTTP/API/mesh/NAT lanes are likewise
plaintext (trusted network); the **SSH lane** is how you get encryption today —
it tunnels any of them through the system `ssh` with zero crypto deps. The NAT
relay is a coordination convenience (single point of failure, double bandwidth),
not a trust boundary.

## Build & test

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo run -p highway --example highway      # ~5x vs serial
cargo run -p spiderweb-viz --example top    # live BusStats dashboard
```

MSRV: Rust **1.91** (const `TypeId` in `Socket::new`).

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option.
