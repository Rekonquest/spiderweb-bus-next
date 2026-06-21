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
- Tests: 5 unit (parallelism proof, correctness, single-lane boundary,
  zero-lane panic) + 1 bus integration (round trip through the ramps).

### Still to build for v1.3

- **Threads** — paths that form as messages flow (desire paths through
  many nodes), not just a single on-ramp→off-ramp hop.
- **Intersections** — where two threads cross at a node, with a
  transfer policy for hopping between them.
- **Vibration / backpressure** — when lanes are saturated, signal
  upstream to slow or reroute instead of letting the on-ramp queue grow
  unbounded.
- **Preloading** — prefetch the next node's input while the current
  lane computes.

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
