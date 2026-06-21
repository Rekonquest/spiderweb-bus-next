# Spiderweb Bus — Architecture

> "Like ComfyUI, but a web instead of a graph."

A plug-and-play runtime for Rust crates. Crates don't know about each other.
The kernel routes by type. The spider watches the web and reacts to broken
or congested strands. The user only declares **what** they need — the kernel
and the spider figure out **how** it connects.

---

## The metaphor

A spiderweb:

- **Strands** carry vibrations.
- Every vibration is a **typed message** moving along strands the kernel manages.
- A **spider** sits in the center, feels every vibration, and knows when
  something is benign, a threat, broken, or just idle.
- The spider doesn't have to be smart. A **deterministic classifier** decides
  what each vibration means. The kernel acts on the classification.

The web is **decentralized**: every strand is a self-contained crate. The
spider is **centralized**: it sees everything.

---

## The components

### Strand (the unit)

A Rust trait every crate implements. One struct, one impl. The kernel does
the rest.

```rust
pub trait Strand: Send + 'static {
    fn name(&self) -> &str;
    fn inputs(&self)  -> &[Socket];
    fn outputs(&self) -> &[Socket];
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError>;
}
```

A strand:

- Declares its **name** (a string the spider uses for logs and topology).
- Declares its **inputs** (sockets it subscribes to).
- Declares its **outputs** (sockets it publishes to).
- Implements `run()` — its main loop.

A strand does **not** know about other strands, the spider, or the topology.
It only knows the `BusHandle` and the types it speaks.

### Socket (the contract)

A socket is a **named, typed channel**. The name is human-readable, the type
is the address.

```rust
pub struct Socket {
    name: &'static str,
    type_id: TypeId,
}
```

Two sockets are "compatible" if their `TypeId`s match. The kernel uses this
to fan out publishes.

### BusHandle (the transport)

The `BusHandle` is what a strand sees. It exposes:

```rust
impl BusHandle {
    pub fn publish<T: 'static + Clone + Send>(&mut self, socket: &'static str, value: T) -> Result<(), BusError>;
    pub fn publish_static<T: 'static + Clone + Send>(&mut self, socket: &'static str, value: T) -> Result<(), BusError>;
    pub fn recv<T: 'static + Send>(&mut self, socket: &'static str) -> Result<Vec<T>, BusError>;
    pub fn recv_all<T: 'static + Send>(&mut self) -> Result<Vec<(&'static str, T)>, BusError>;
    pub fn log(&self, msg: &str);
    pub fn sleep(&self, d: Duration);
}
```

Internally, the bus uses **typed channels** (one per socket name). Publish
goes to every subscriber.

### Kernel (the executor)

The kernel is **a small set of threads and a registry**. No Tokio. No
`async`/`await`. No `Pin`. No `Send` futures.

What it owns:

- A **type registry** — `HashMap<TypeId, Vec<Subscriber>>` for both publishers
  and subscribers.
- A **per-strand inbox** — `mpsc::Sender<Envelope>` from the kernel to each
  strand's worker thread.
- A **slot table** — name → factory + done channel + state.
- A **control inbox** — kernel listens for `ControlMsg` on `sys.control`.
- A **clone registry** — `HashMap<TypeId, CloneFn>` for fan-out.

What it does, per tick:

1. Drain each strand's outbox (publishes from `run()`).
2. For each publish, look up subscribers of the matching socket type.
3. Clone the value via the **clone registry**.
4. Enqueue the message in each subscriber's inbox (fan-out).
5. Drain control messages from `sys.control` (Restart, Quarantine, Shutdown).
6. Drain worker exits. On crash, mark the slot as `dead` and wait for the spider.
7. Notify the spider of state changes via lifecycle events.

### Clone registry (v1.2)

A global `HashMap<TypeId, CloneFn>` that maps each registered type to a
function that clones it. Built-in types (`String`, `i64`, `u64`, `bool`,
`LifecycleEvent`, `ControlMsg`) are registered automatically. User types
must call `spiderweb::register_clone_type::<T>()` at boot.

This is how fan-out works for arbitrary `Clone` types. v1.1 hardcoded a
fixed set; v1.2 makes it extensible.

### Spider (the orchestrator, v1.2)

The spider is **a regular strand** that subscribes to `sys.lifecycle` and
publishes to `sys.control`. Its job:

- Watch `LifecycleEvent::Crashed { name, reason }`.
- On the 1st, 2nd, 3rd crash: publish `ControlMsg::Restart { name }`.
- On the 4th crash: publish `ControlMsg::Quarantine { name, reason }`.
- Log every event for the operator.

The kernel executes the spider's decisions. The spider is registered
explicitly via `Bus::register_spider(spec)`; the kernel tracks its name in
`kernel.spider_name` and uses that to decide whether to fall back to
auto-restart on crash (no spider present) or to wait for the spider to
issue a `ControlMsg`. v1.2 fallback: if no spider is registered, the
kernel auto-restarts strands up to 3 times (v1.1 behavior). v1.3 will
remove the fallback.

### webctl (the CLI)

`webctl` is a launcher CLI for the demos. It compiles a release binary that
runs `cargo run -p <package> --example <name>` for a given demo name. v1.2
also exposes `list`, `topology`, and `version` commands. v2 will add live
introspection of a running web (requires network transport).

---

## Locked decisions (v1.2)

| Decision | Choice | Why |
|---|---|---|
| Async model | Plain threads + channels, no `async`/`await` | Matches the metaphor, no `Pin`/`Send` gymnastics |
| Transport | In-process pub/sub via typed channels | Fastest, simplest. Edge transport is v2 |
| Routing | TypeId-based, fan-out to all subscribers | No wires to draw; kernel owns the topology |
| Multi-instance | Each strand instance is its own subscriber | Free redundancy |
| Routing policy | v1.2: fan-out to all. v2: least-loaded + heartbeat | Simple first |
| Crash policy | v1.2: spider decides via sys.control. Kernel fallback: 3 restarts, then quarantine | Spider is the authority; kernel is the muscle. The spider is registered explicitly via `Bus::register_spider(spec)`; the kernel tracks its name in `kernel.spider_name`. |
| Spawn policy | User declares at boot, kernel reacts | Auto-spawn is a footgun |
| Strand params | None in v1.2 | ComfyUI params add a config surface; the web should be invisible |
| Strand types | Live (no factory, non-restartable) and Factory-based (restartable) | Two registration paths; kernel marks live strands dead on crash |
| Strand naming | Each strand must have a unique name. `Bus::register` and `Bus::register_spec` panic on collision. | Silent overwrite used to leak the first slot's `done_rx` and strand the kernel's main loop. |
| Topic namespace | Free string names; reserved `sys.*` prefix | Fastest to start |
| Request/response | Pub/sub only in v1.2; request/response is a pattern on top | Different semantics |
| Crate dependencies | Zero external runtime deps (just `std`) for the kernel | The point of "no Tokio" is staying small |
| Spider placement | In-process, runs as a regular strand | Simplest |
| Payload cloning | v1.2: T: Clone or register_clone_type::<T>() | v1.1 was limited to a fixed set |
| Pending buffer | Bounded (32/type), drop-oldest, ALWAYS populated, replay-to-every-late-subscriber | Late subscribers get all messages published before they arrived. v1.2 (pre-hardening) drained the buffer on the first subscribe; v1.2 (post-hardening) keeps the buffer and clones per subscriber. |
| Slot table keys | Owned `String` | v1.2 used `&'static str` via `Box::leak`; removed 2026-06-12 |
| Control plane | `sys.control` socket, kernel is sole subscriber, spider publishes | Spider drives the bus |
| Shutdown | `Bus::run()` blocks forever (Ctrl-C); `Bus::run_until(d)` bounds demos; `ControlMsg::Shutdown` is parsed | v1.2 in-bus Shutdown is parsed but rare in demos |
| CLI | `webctl list | run | topology | version` | Real launcher for demos |

---

## Non-goals (v1.2)

- **Network transport** between processes.
- **Persistence** of strand state across restarts.
- **Configuration files**. Everything is code.
- **Dynamic loading** of strands.
- **LLM in the spider**.
- **Type-erased payloads** in the hot path.
- **Async I/O in strands**. Strands block on their inbox.
- **Live introspection** of a running web (requires network).
- **TTL on the pending buffer**. Currently bounded by count.

---

## The boot sequence

A user's main:

```rust
fn main() {
    spiderweb::register_clone_type::<MyPayload>();
    let mut bus = Bus::open();
    bus.register(CapSource::new());
    bus.register(CapSink::new());
    bus.register_spec(StrandSpec::new("my-strand", || Box::new(MyStrand::new())));
    bus.register_spider(StrandSpec::new("spider", || Box::new(Spider::new())));
    bus.run_until(Some(Duration::from_secs(60)));
}
```

The kernel:

1. Creates worker threads for each registered strand.
2. Builds the type-to-strand routing tables.
3. Subscribes itself to `sys.control`.
4. Starts the spider (if registered).
5. Pumps messages.
6. On Ctrl-C, max-duration, or `ControlMsg::Shutdown`, shuts down cleanly.

---

## Build order (see ROADMAP.md)

1. `spiderweb` — the kernel + `Strand` + `BusHandle` + `Socket` + clone registry.
2. `spider` — the orchestrator.
3. `cap-source`, `cap-sink`, `cap-slow-sink`, `cap-crasher`, `cap-image` — example strands.
4. `webctl` — the CLI.
5. Demos: `demo`, `demo_v1_1`, `backpressure`, `image-pipeline`.

Each step ends with a runnable test.
