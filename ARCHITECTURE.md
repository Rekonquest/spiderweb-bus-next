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

The kernel executes the spider's decisions. **Restart policy lives
entirely in the spider — the kernel is a dumb primitive.** On a crash the
kernel marks the slot dead and waits for the spider's `ControlMsg`
(Restart/Quarantine) on `sys.control`; it never decides on its own. With
no spider registered, a crashed strand simply stays dead. (v1.2 had a
3-retry-then-quarantine fallback baked into the kernel; **v1.3 removed
it** so the kernel carries no crash policy — composition doctrine:
mechanism in the primitive, policy in the orchestrator.) The spider is
registered explicitly via `Bus::register_spider(spec)`, which enforces at
most one.

### Highway (Layer 2, the parallel layer — v1.3)

Everything above is the **ground level**: local streets, where strands
tick at their own pace and messages fan out by type. The highway is the
fast lane **above** it. It lives in its own crate (`highway`) that
depends on `spiderweb` — the kernel never depends on the highway, since
a higher layer wraps the lower one, not the reverse.

```rust
pub struct Highway<I, O> { /* a pool of lane threads running one transform */ }

impl<I, O> Highway<I, O> {
    pub fn new<F: Fn(I) -> O + Send + Sync + 'static>(lanes: usize, f: F) -> Self;
    pub fn on_ramp(&self, item: I) -> Result<(), HighwayError>; // promote to the highway
    pub fn off_ramp(&self) -> Vec<O>;                           // demote finished results
    pub fn lanes(&self) -> usize;
}
```

- **On-ramp** — `on_ramp(item)` lifts one item off the ground and onto
  the highway. The next free lane picks it up.
- **Lanes** — `lanes` worker threads, each running the same transform
  `I -> O` in parallel. They pull from one shared job queue, holding
  the lock only for a non-blocking `try_recv` so the transform itself
  runs fully in parallel. Idle lanes park 1ms and retry — the kernel's
  own polling idiom, no Tokio.
- **Off-ramp** — `off_ramp()` collects whatever the lanes have finished,
  in **completion order** (unordered, like ground-level fan-out).
- Dropping a `Highway` closes the on-ramp and joins every lane. No
  leaked threads.

The point is throughput: a batch of `N` items across `lanes` lanes
finishes in `ceil(N / lanes)` rounds instead of `N` serial steps. The
`highway` demo shows 12 × 150ms work finishing in ~320ms on 6 lanes vs.
~1.8s serial.

#### HighwayStrand — the ramps on the bus

`HighwayStrand<I, O>` is a regular `Strand` that hosts a `Highway`
segment and wires it to the ground bus. Each tick it on-ramps every
inbound message of type `I` and off-ramps finished results as type `O`:

```rust
bus.register(HighwayStrand::<Work, Done>::new(
    "square-highway", "in.work", "out.done", /* lanes = */ 4,
    |Work(n)| Done(n * n),
));
```

Routing is by `TypeId`, so `I` and `O` must be **distinct newtypes**,
and both must be registered with `register_clone_type::<T>()` at boot
(unless built-in). The strand is pure elevation — it does no work
itself; the parallelism lives in the highway's lane threads, which run
independently of the strand's tick.

### Vibrations — backpressure (v1.3)

A spiderweb's real power is that vibrations propagate: touch one strand
and the rest of the web feels it. The first concrete vibration is
**backpressure**.

A `Highway` has a soft `capacity`. A `HighwayStrand` pulls inbound work
into its own holding buffer and on-ramps only while the highway
`has_room()`. When the highway fills, the held work stays in the
strand's buffer — never dropped — and the strand emits a `Vibration`
on the reserved `sys.vibration` socket:

```rust
pub enum Vibration {
    Backpressure { node: String, queued: usize, in_flight: usize, capacity: usize },
}
```

The `Vibration` type and `SYS_VIBRATION` socket live in the kernel
crate, not the highway — vibration is a web-wide concept (backpressure
now, heartbeat staleness later), so any layer can emit one and any
strand can feel one. The spider subscribes to `sys.vibration` and logs
backpressure; a future spider could react by pausing the source or
rerouting around the congested node. This is **vertical signal
propagation**: a Layer 2 highway tells Layer 3 orchestration that it is
saturated, over the same pub/sub the rest of the bus uses.

To avoid spamming the web, a strand emits a vibration only when its
backlog depth *changes*, so a steady jam produces a clean rise-and-fall
of signals rather than a flood.

### Threads, intersections, the fabric (v1.3)

The web is not a declared graph — it draws itself from traffic. As
messages flow they trace **threads** (desire paths through many nodes);
where two threads cross a node, an **intersection** emerges. Together
they are the **fabric**, which is empty at boot and accretes purely from
flow.

```rust
pub type ThreadId = u64;
pub type Tag = Option<ThreadId>;          // rides PARALLEL to the payload
pub struct Thread { pub id: ThreadId, pub direction: Vec<NodeId> }
pub struct Intersection { pub node: NodeId, pub threads: Vec<ThreadId>, pub transfer: TransferPolicy }
pub enum TransferPolicy { Stay, HopTo(ThreadId), MergeAll }   // default Stay = no-op
```

**Provenance is kernel-transparent.** The carrier tuple widened from
`(socket, Envelope)` to `(socket, Tag, Envelope)`. The `Tag` rides
*parallel* to the type-erased payload, never inside it, so the clone
registry and `recv::<T>()` downcast are untouched and `publish`/`recv`
keep identical signatures. **Ordinary strand authors never see a tag.**

**Causality without inference.** The kernel can't see which input
produced which output inside a strand's opaque `run()`. So it doesn't
guess: each `BusHandle` carries a `current_cause`, seeded from the inbox
and updated by `recv`/`recv_all` to the last item handed out. `publish`
stamps that cause. Since every strand does recv-then-publish, outputs
continue the right thread for free. A parentless `publish` (a source, or
any publish before a `recv`) carries `None`; the kernel mints a fresh
thread on delivery, rooted at that node.

**Paths grow at arrival.** The worker records each inbound tagged
message into the fabric as it drains its inbox, so a node lands on a
thread's `direction` the moment a message reaches it — capturing
replayed messages too. An intersection emerges the instant a node's
observed-thread set goes from one to two.

**The highway carries the tag through its lanes.** `on_ramp_tagged` /
`off_ramp_tagged` glue the tag to the item, so provenance survives lane
reordering and the input-tick≠output-tick gap — a result demoted three
ticks late still names its origin thread. The author's `Fn(I) -> O`
never sees the tag.

**Transfer policy governs hops.** At an intersection, `Stay` (default)
keeps threads on their identity — so enabling the fabric changes no
routing. `HopTo`/`MergeAll` re-parent a node's outputs; set them
explicitly via `Bus::set_transfer` or have the spider publish
`ControlMsg::SetTransfer` — Layer-3 orchestration governing the weave
without any strand knowing.

The fabric lives in the kernel behind its own leaf mutex, mutated only
*outside* the deliver/subscribe exactly-once critical section (so the
double-delivery fix is preserved). Read it with `Bus::fabric_snapshot()`;
the spider feels `ThreadFormed` / `IntersectionEmerged` over
`sys.lifecycle`. Reserved `sys.*` sockets are never threaded.

### Preloading — the fabric working ahead of time (v1.3)

Read-ahead: the source-side dual of the highway. The `preload` crate
(depends on `spiderweb`, std-only) prefetches data *before it is asked
for*, like a CPU pipeline prefetching from cache.

```rust
pub struct Preloader<T> { /* background fetch threads + bounded ready-buffer */ }

impl<T> Preloader<T> {
    pub fn new<F: FnMut(u64) -> Option<T>>(depth: usize, fetch: F) -> Self;          // one fetcher, FIFO
    pub fn with_workers<F>(depth: usize, workers: usize, make: impl Fn(usize)->F) -> Self; // parallel, unordered
    pub fn take(&self) -> Option<T>;        // O(1) — the fetch already ran
    pub fn ready_len(&self) -> usize;       // occupancy gauge
}
```

- **Bounded read-ahead.** A `sync_channel(depth)` ready-buffer: a fetcher
  blocks once `depth` items are banked, so memory is bounded and the
  fetch paces itself to demand — no busy-spin, no unbounded growth.
- **`PreloadStrand<T>`** is a source strand. Its tick is O(1) — drain
  ready items and publish; the fetch happens on background threads, so
  the tick never blocks. It detaches when the source is exhausted and
  emits **`Vibration::Starved`** (the dual of the highway's
  `Backpressure`) when the buffer runs dry, so the web feels a
  source-too-slow rate mismatch the same way it feels a consumer-too-slow
  one.

**What it honestly buys.** With one worker thread per strand, adjacent
stages already overlap and the per-strand inbox is itself a buffer — so
single-thread read-ahead does **not** raise steady-state throughput
(bounded by the slowest stage). Its real value in isolation is jitter
absorption (clipping the *max* inter-arrival gap). On the bus, the
dramatic win is **parallel read-ahead feeding the parallel layer**: a
slow per-item fetch starves a K-lane highway, but
`Preloader::with_workers(K)` raises the source rate to keep all K lanes
full (~3× in the `preload` demo). Read-ahead alone can't beat a single
slow fetch; parallel fetch can — and the two layers compose purely by
type: `PreloadStrand → HighwayStrand → sink`.

### Self-registration — crates advertise themselves (v1.3)

Connection between crates is already automatic: a strand declares the
*types* it consumes/produces and the kernel routes by `TypeId` — no
wires. The last manual step was *announcing* a strand (`bus.register(...)`
in `main()`). The `spiderweb-registry` companion crate removes it.

A strand crate advertises itself with one line, at module scope:

```rust
spiderweb_registry::advertise!("cap-source", || Box::new(CapSource::new()) as Box<dyn Strand>);
```

and the binary collects every advert at boot:

```rust
let mut bus = Bus::open();
bus.auto_connect();   // registers everything advertised; main() has no list
```

Under the hood it's a distributed-slice (`inventory`): `advertise!`
deposits a `StrandAdvert { name, factory, role }` into a static slice;
`auto_connect` walks the slice and calls the existing `register_spec` /
`register_spider`. **This maps onto the control doctrine exactly:** the
advert *is* a self-advertisement (a capability — name + factory, and the
in/out sockets surface over the unchanged `sys.lifecycle` stream); the
registry is **plumbing**; the **spider** orchestrates the advertised set.

**Kernel purity is preserved.** `inventory` (and its linker-section
machinery) is quarantined in `spiderweb-registry` — the kernel crate
gains *zero* dependencies and stays `forbid(unsafe_code)`. Auto-connect
and manual `register` coexist; nothing existing changes.

**Honest caveat.** This is not drop-a-file-on-disk discovery. The binary
must depend on each strand crate *and reference it*. The Cargo dependency
is **necessary but not sufficient** — the **load-bearing part is the
anchor** (`use crate as _;`): a crate that is only a dependency, with no
symbol referenced, gets dead-stripped by the linker, advert and all
(compiles, runs, exits 0, strand silently absent). `auto_connect` prints
the collected count + names to stderr, and `auto_connect_expecting(&[…])`
turns a missing advert into a loud panic. True no-recompile drop-in needs
dynamic loading (dlopen), a v2 step that would cross the std-only / unsafe
line.

### 3D spatial awareness (v1.3)

A real spiderweb is 3D, and vibrations propagate through 3D space,
attenuating with distance. The bus gets a geometric overlay.

```rust
pub struct Position { pub x: f64, pub y: f64, pub z: f64 }   // a node's place in the web
pub fn attenuation(origin: Option<Position>, receiver: Option<Position>) -> f64; // 1/(1+d)
```

- **Nodes advertise a position** — through the same self-registration
  (`advertise_at!`), or `Bus::place(name, pos)` (operator override). The
  kernel records it in the fabric; `fabric_snapshot` exposes
  `positions`, `position(node)`, and `distance(a, b)`.
- **The fabric becomes geometric.** Each `Thread.direction` resolves
  through positions into a 3D polyline; `spiderweb-viz::scene_json`
  exports the weave as a scene, and `scene_html` produces a
  self-contained, **interactive viewer** (three.js + OrbitControls —
  drag to rotate, scroll to zoom) with the scene embedded. A separate
  std-only crate — the kernel stays dependency-free.
- **The Z-axis makes elevation literal:** ground ~ z=0, highway ~ z=1,
  orchestration ~ z=2 — an on-ramp is a real coordinate delta.
- **Vibrations attenuate with distance.** `attenuation` is the physics
  *spreading-activation* primitive — a **dumb pure function** (it decays
  `1/(1+d)`, makes no decision). A receiver (the spider, or any strand)
  reads it — resolving a vibration's origin via `snapshot.position(node)`
  since the emitter's name is already in every `Vibration` — and decides
  how to react. Mechanism here, policy there. So the `Vibration` enum and
  `Strand` trait are untouched.

**Honest framing.** In a single-process bus every node is in the same
RAM, so distance has **no latency** — positions are a *logical* overlay
whose value now is visualization, organization, and spider policy (e.g.
weighting reaction by how near congestion is). They gain physical meaning
with edge transport (v2), where a position maps to a real machine/GPU and
distance approximates transport/resource cost — the foundation for
distance/cost-aware routing (the "3D fabric routing" concept).

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
| Crash policy | v1.3: spider owns ALL of it via sys.control. Kernel marks the slot dead and waits — no in-kernel fallback. With no spider, a crashed strand stays dead | Spider is the authority; kernel is the muscle. Mechanism in the primitive, policy in the orchestrator (composition doctrine). The spider is registered explicitly via `Bus::register_spider(spec)`. |
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
3. `highway` — Layer 2: `Highway` + `HighwayStrand` (on-ramps, lanes, off-ramps).
4. `cap-source`, `cap-sink`, `cap-slow-sink`, `cap-crasher`, `cap-image` — example strands.
5. `webctl` — the CLI.
6. Demos: `demo`, `demo_v1_1`, `backpressure`, `image-pipeline`, `highway`.

Each step ends with a runnable test.
