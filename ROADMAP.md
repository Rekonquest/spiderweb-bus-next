# Spiderweb Bus — Roadmap

Build order. Each step ends with a runnable test. No step is "done" until
the test passes.

---

## v1.0 — Core: kernel, fan-out, restart

Status: **Complete** (2026-06-10).

- `Strand` trait, `BusHandle`, `Socket`, `Kernel`.
- TypeId-based routing. Fan-out to all subscribers.
- Buffer-and-flush for no-subscriber publishes.
- `cap-source` + `cap-sink` prove fan-out.
- `demo` proves end-to-end.

## v1.1 — Self-heal: kernel restarts, spider logs

Status: **Complete** (2026-06-10).

- `LifecycleEvent` is public, published on `sys.lifecycle`.
- `StrandSpec` + `Bus::register_spec` for restartable strands.
- Kernel auto-restarts crashed strands up to 3 times, then quarantines.
- `spider` crate subscribes to `sys.lifecycle`, logs every event.
- `cap-crasher` provably crashes on Nth tick.
- `demo_v1_1` shows the full crash-recovery pipeline.

## v1.2 — Spider takes the wheel + real workloads

Status: **Complete** (2026-06-10), hardened 2026-06-12.

- Spider drives the restart/quarantine policy via `ControlMsg` on `sys.control`.
- Kernel fallback to v1.1 auto-restart if no spider is registered.
- Clone registry: `register_clone_type::<T>()` for user-defined payload types.
- `cap-slow-sink` + backpressure demo.
- `cap-image` (real PNG load/resize/save) + `image-pipeline` demo.
- `webctl` CLI (list, run, topology, version).
- All v1.1 demos still work; v1.1 demo now spider-driven.

### v1.2 hardening (2026-06-12)

- **Slot table keys switched to owned `String`.** Removed the
  `Box::leak` hack in `Bus::register` and the second one in
  `run_until` for `ControlMsg::Restart`. Slot names are owned
  end-to-end; no more leaks for repeated strand names.
- **`start_live` and `install_slot` unified.** Live (non-restartable)
  strands and factory-based strands now go through a single
  `install_live` / `install_slot` / `restart_slot` API. Live
  strands have `factory: None`; the kernel marks them dead on
  crash but cannot restart them.
- **Bounded pending buffer, always populated, replayed to every
  late subscriber.** The kernel's `deliver` now always appends
  to a per-type buffer (32 entries, drop-oldest) and ALSO
  delivers to current subscribers. Every new subscriber gets a
  replay of the entire current buffer. This fixes the bug where
  the second (and later) subscribers saw nothing because the
  buffer was drained on the first subscribe.
- **Deadlock fix in `subscribe()`.** The buffer-replay path
  called `publish_lifecycle` while holding both `subscribers`
  and `pending` mutexes, and `publish_lifecycle` re-locks
  `subscribers`. `std::sync::Mutex` is not reentrant; this
  deadlocked any deployment that subscribed strands after at
  least one buffered message. Fixed by building the lifecycle
  event first, dropping both locks, then publishing.
- **Strand name collisions now panic.** Two strands with the
  same name used to silently overwrite the first slot in the
  map, leaking its `done_rx` and making its `WorkerExit`
  unprocessable. Now `Bus::register` and `Bus::register_spec`
  panic at registration time.
- **Explicit `register_spider()` API.** The kernel's "is a
  spider present?" check used to look at any
  `LifecycleEvent` subscriber, which conflated the spider
  with arbitrary lifecycle observers. Now the spider is
  registered via a dedicated `Bus::register_spider(spec)`
  method that sets `kernel.spider_name`. At most one spider
  per bus; double registration panics.
- **Real test suite.** 12 kernel-level unit tests + 4
  integration tests exercising the spider-driven crash-recovery
  cycle. The test suite is part of the contract; `cargo test
  --workspace` is the regression net. Tests are exact (e.g.
  `assert_eq!(got, 2)` not `assert!(got >= 1)`) so a
  regression in fan-out or buffer-replay is caught.
- **`image-pipeline` fan-out made real.** The two sinks in
  the pipeline demo previously wrote to the same output path.
  The sinks now take a `with_suffix("-A")` / `with_suffix("-B")`
  tag; the suffix is used both to rewrite the output path AND
  to derive a unique strand name. The fan-out is observable
  in the file system: 3 publishes produce 6 saved files.
- **Dead `leaked_for_lookup` helper removed.** It was a v1.2
  placeholder for the v1.2 limitation we just fixed.

## v1.3 — The highway: the parallel layer (Layer 2, in progress)

Status: **In progress** (started 2026-06-21).

Everything before this is Layer 0/1 — local streets. Messages fan out
by type and strands tick at their own pace. v1.3 builds the **highway
above it**: the parallel layer where a batch of work is lifted off the
ground, processed across many lanes at once, and dropped back down.

- `highway` crate. Depends on `spiderweb`; the kernel does **not**
  depend on it (higher layer wraps lower).
- `Highway<I, O>` — a standalone pool of lane threads running one
  transform. `on_ramp(item)` promotes work; `off_ramp()` collects
  finished results in completion order. Plain `std` threads/channels,
  no new deps. Lanes poll a shared job queue with a 1ms park (the
  kernel's own idiom); the lock is held only for the non-blocking
  `try_recv`, so lanes run `f` in true parallel. `Drop` closes the
  on-ramp and joins every lane — no leaked threads.
- `HighwayStrand<I, O>` — the on-ramp/off-ramp pair as a `Strand`. It
  subscribes to an input type (on-ramp), feeds each message onto the
  highway, and re-publishes results as an output type (off-ramp). Use
  distinct newtypes for `I` and `O` — routing is by `TypeId`.
- `highway` demo: 12 parcels × 150ms across 6 lanes finishes in ~two
  rounds (~320ms) vs. ~1.8s serial. The speedup is the point.

### Vibration: backpressure (done)

The first cross-layer vibration. A `Highway` has a soft `capacity`; a
`HighwayStrand` holds inbound work it can't on-ramp yet (never dropping
it) and emits `Vibration::Backpressure` on the reserved `sys.vibration`
socket when its backlog changes. The `Vibration` type lives in the
kernel (web-wide vocabulary, like `LifecycleEvent`). The spider feels it
and logs it — Layer 2 → Layer 3 vertical propagation.

- `congestion` demo: a burst of 24 overruns a 2-lane, capacity-4
  highway; the spider logs the backlog draining (21→19→…→1) and every
  parcel still arrives.
- Tests across the slice: 7 highway unit (parallelism, correctness,
  capacity/`has_room`, boundary panics) + 3 bus integration (round
  trip, backpressure-fires-and-loses-nothing, roomy-is-silent).

### Kernel fix: exactly-once delivery (done)

The `congestion` demo surfaced a pre-existing deliver/subscribe race in
the kernel: a burst published while a subscriber was registering could
be delivered **twice** (once via buffer replay, once direct) or lost.
`deliver` now appends to the replay buffer and snapshots subscribers
atomically, and `subscribe` replays-and-inserts atomically, under the
same two locks in a consistent order — so every (message, subscriber)
pair is delivered by exactly one path. Guarded by a looped
`burst_at_startup_delivers_exactly_once` regression test.

### Threads & intersections: the emergent fabric (done)

The web draws itself from traffic. Every message carries a `Tag`
(`Option<ThreadId>`) parallel to its payload — invisible to strand
authors; `publish`/`recv` are unchanged. The kernel mints a thread when
a parentless message enters the fabric and continues it via a per-handle
`current_cause` stitched across the universal recv→publish idiom. The
worker records each arrival, so a `Thread`'s `direction` grows through
the nodes it actually traverses — a desire path, not a declared edge.

- **Threads** form as messages flow: `Thread { id, direction: Vec<NodeId> }`.
- **Intersections** emerge where a node first sees a second distinct
  thread: `Intersection { node, threads, transfer }`.
- **TransferPolicy** (`Stay` default = no-op, `HopTo`, `MergeAll`)
  governs hops; set via `Bus::set_transfer` or `ControlMsg::SetTransfer`.
- The **highway carries the tag through its lanes** (`on_ramp_tagged` /
  `off_ramp_tagged`), so provenance survives reordering and the
  input-tick≠output-tick gap.
- The spider feels `ThreadFormed` / `IntersectionEmerged` over
  `sys.lifecycle`. `Bus::fabric_snapshot()` returns a read-only view.
- `weave` demo + 6 fabric tests (form-through-many-nodes,
  emergence, Stay/MergeAll transfer, highway provenance, no-flow).

A kernel-transparency win along the way: provenance forced a careful
audit of the deliver/subscribe carrier, and the exactly-once invariant
holds under the widened tuple (the `burst_at_startup` regression still
passes).

### Review hardening (done)

A multi-agent adversarial review of the fabric change surfaced 13
issues, all fixed:

- **Fan-out forking.** A thread delivered to several distinct nodes
  used to be linearized into one fabricated chain `[S, A, B]` with
  scheduler-dependent order. Now each extra Stay-fan-out branch forks a
  sibling thread sharing the prefix, so every thread stays a true linear
  desire path. (`fan_out_branches_into_sibling_threads`)
- **Reserved sockets never touch causality.** `current_cause` now skips
  `sys.*` carriers when seeding/refining, so feeling a vibration or
  lifecycle event can't sever a strand's work thread.
- **No orphan threads.** A parentless message at a `HopTo`/`MergeAll`
  node no longer mints a dead-end thread or fires a misleading
  `ThreadFormed`; `Fabric::outbound` returns the real born thread.
- **MergeAll roots its fused thread at the real node** (not a synthetic
  `node⊗` label); **HopTo to an unminted id** falls back to ordinary
  threading instead of fabricating a ghost.
- **Bounded fabric.** Retention is capped (`Fabric::THREAD_CAP = 1024`,
  oldest evicted and scrubbed from the indices), mirroring
  `PENDING_BUFFER_CAP`. (`fabric_is_bounded_under_a_flood`)
- New tests for HopTo (both arms), 3-way intersection growth, and the
  reserved-recv causality fix. 38 tests green.

### Preloading: the fabric working ahead of time (done)

A read-ahead layer — the source-side dual of the highway. The `preload`
crate (depends on `spiderweb`, std-only) mirrors the highway's
two-piece shape:

- `Preloader<T>` — background fetch threads run an expensive/jittery
  `fetch(i) -> Option<T>` ahead of demand into a bounded
  `sync_channel(depth)` ready-buffer, so the next item is materialized
  before it's asked for. `new` = one fetch thread (FIFO); `with_workers`
  = parallel fetch (completion order). `take`/`take_ready`/`ready_len`;
  `Drop` stops + joins every fetcher (no leaks, even under a blocked send).
- `PreloadStrand<T>` — a source strand: each tick drains up to `burst`
  ready items and publishes them (O(1) tick, never blocks on fetch),
  detaches on exhaustion, and emits `Vibration::Starved` (the source-side
  dual of backpressure) when the buffer runs dry. The spider feels it.

**Honest about the benefit.** With one worker thread per strand, adjacent
stages already overlap and the per-strand inbox is itself an unbounded
buffer — so single-thread read-ahead can't raise steady-state throughput
(the `jitter_is_absorbed` unit test proves its real value — clipping the
*max* inter-arrival gap — on the `Preloader` in isolation). The dramatic,
honest win on the bus is **parallel read-ahead feeding the parallel
layer**: a 40ms-per-item fetch starves a 4-lane highway, but
`with_workers(4)` keeps all lanes full — the `preload` demo measures
~3× (723ms → 252ms). Read-ahead alone can't beat a single slow fetch;
parallel fetch can.

- New `Vibration::Starved { node, ready, depth }` (kernel signal
  vocabulary, like `Backpressure`); the spider logs it.
- 10 tests (ordering, bounded buffer, zero-wait handoff, jitter
  absorption, parallel completeness, drop-joins-under-block, starvation
  vibration, exhaustion-detach, zero-depth/zero-worker panics).

### Still to build for v1.3

- **Per-message precise fan-in** is opt-in (`recv_tagged` /
  `publish_tagged`); the default fan-in attribution is last-recv'd
  (honest, recorded as an intersection). A future ergonomic
  `publish_continuing(parent)` could make precision a one-liner.
- **Late fan-out subscribers** replay the base thread (they continue it
  rather than forking) — a documented edge of the bounded replay buffer.

## What's next (v2, not now)

- **Edge transport** (TCP/QUIC) for cross-process strands.
- **Persistent strand state** across restarts.
- **In-bus Shutdown** properly tested.
- **`webctl` live introspection** — `webctl top` shows the live topology of
  a running web.
- **Spider declarative policy** — config file describes restart rules instead
  of hardcoded in the spider.
- **Heartbeat staleness** detection (strand alive but stuck).
- **Optional reactor-backed I/O** for non-blocking reads.
- **Generic Arc<T> payload support** — the kernel can clone Arc<T> for
  fan-out without requiring T: Clone.
- **TTL on the pending buffer** — currently bounded by count (32 per
  type), could be time-bounded too.

---

## Demos

Run via `webctl run <name>` or `cargo run -p <pkg> --example <name>`.

| Demo | Package | What it proves |
|---|---|---|
| `demo` | cap-source | Fan-out by type, buffer-flush on subscriber arrival |
| `demo-v1-1` | cap-source | Crash recovery, restart attempts, quarantine |
| `backpressure` | cap-source | Fast source + slow sink; independent subscribers |
| `image-pipeline` | cap-image | Real bytes (base64) through the bus; load/resize/save; observable fan-out via `-A` / `-B` suffix |
| `highway` | highway | On-ramp a batch to the parallel layer, process across lanes, off-ramp results; ~5–6× faster than serial |
| `congestion` | highway | Burst overruns a small highway; it emits backpressure vibrations the spider feels, and drops nothing |
| `weave` | highway | Threads form as messages flow; intersections emerge where they cross; a MergeAll hop fuses threads downstream |
| `preload` | preload | Read ahead so the next data is ready before it's asked for; parallel read-ahead keeps a starved highway full (~3×) |

## Tests

Run with `cargo test --workspace`. The spiderweb crate has 12
kernel-level unit tests and 4 integration tests (in `tests/`)
that exercise the full crash-recovery cycle against the
`spider` crate. The other crates have no tests because they
are demo strands; their behavior is exercised via the
`webctl run` demos.

## Spider policy (v1.2)

The default `Spider` strand uses this rule:

| Crash # | Action |
|---|---|
| 1 | `ControlMsg::Restart` |
| 2 | `ControlMsg::Restart` |
| 3 | `ControlMsg::Restart` |
| 4+ | `ControlMsg::Quarantine` |

The user can write their own spider and register it instead.
