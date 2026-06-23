# Spiderweb Bus — Production Readiness

> Definition of done + tracked punch-list. The bar: a genuinely deployable,
> robust, observable system — not a label. Grounded in the security/stability
> audit (which read the real code: 57 `unwrap`/`expect` sites, no `Drop`/no
> worker join, no auth on `sys.control`, `spider.log`-only observability).
>
> Working repo: this fork (`Spiderweb-Bus-Next`) is the canonical product line —
> it is the superset (in-process core + `spiderweb-edge` + `spiderweb-heat`).
> The baseline `Spiderweb Bus` is the historical stable point.
>
> Honest scope note: the in-process core stays std-only / zero-dep / forbid-
> unsafe. **Distributed production (Phase 3) is where the zero-dep story ends at
> the network boundary** — auth/encryption need crypto. That is expected and
> was predicted by the audit; it is the trust boundary being *earned*.

Status legend: `[ ]` todo · `[~]` in progress · `[x]` done (tests + review)

---

## Phase 1 — Robustness (the bus must not fall over)
The highest-priority gap. A production bus must survive any single strand
misbehaving, and must shut down cleanly.

- [x] **Panic isolation.** `strand.run()` runs under `catch_unwind`; a
  panicking strand becomes a `Crashed` lifecycle event (spider restart/
  quarantine policy applies) instead of silently killing its worker — and,
  under `Bus::run()`, hanging the whole bus forever (done_count never reaches
  total_slots). The panic message is recovered into the `Crashed` reason (real
  forensics, not a bare "panicked"); a panicked run's half-built outbox is
  dropped, never delivered. Holds under the default `panic = "unwind"`;
  documented that `panic = "abort"` is uncatchable by design. Tests:
  `spider_recovery.rs` — `panicking_strand_is_quarantined_like_a_crash`,
  `run_winds_down_after_a_panic_with_no_spider`,
  `a_panicking_strand_does_not_kill_its_neighbor`,
  `panic_message_is_recovered_into_the_crash_reason`. Reviewed (single-agent).
- [x] **Graceful shutdown.** Every `run_until` exit (max-duration, all-done,
  `ControlMsg::Shutdown`) sets a cooperative `shutdown` flag and JOINs the
  worker threads, so strands are dropped (their `Drop` runs). Workers check the
  flag at the top of each tick; `BusHandle::is_shutting_down()` lets a
  long/blocking strand cooperate. The join is **bounded best-effort**: a strand
  that wedges inside `run()` (uncancellable in safe Rust) is detached after a
  5s deadline with a warning rather than hanging the bus forever. This makes
  the strand actually drop, which is the prerequisite for the Phase 3
  `EdgeListener::Drop` reader-thread/port teardown. Tests: `spider_recovery.rs`
  — `clean_strand_is_dropped_on_graceful_shutdown`,
  `spec_strand_is_dropped_on_graceful_shutdown`,
  `shutdown_message_winds_down_and_drops_strands`,
  `shutdown_joins_a_parked_crashed_slot_without_hanging`,
  `cooperative_strand_exits_via_is_shutting_down`,
  `wedged_strand_does_not_hang_shutdown`. Reviewed (single-agent).
- [x] **Kernel poison-tolerance.** All 41 kernel `.lock().unwrap()` sites now
  use a `LockTolerant::lock_tolerant()` helper that recovers the inner guard on
  poison (`unwrap_or_else(into_inner)`), so a panic in any critical section
  can't cascade into every later lock site and kill the bus. Guarded state is
  plain data (maps/vecs/fabric) — partial-update-safe under `forbid(unsafe)` —
  and the sections are panic-free by design (strands run lock-free); this is
  defense-in-depth. Audited the other crates: all remaining `.lock().unwrap()`
  are test/example code (edge production code uses no shared mutexes). Test:
  `lock_tolerant_recovers_a_poisoned_mutex`.
- [x] **No-panic error paths.** Triaged every `unwrap`/`expect`/`panic!`/
  `unreachable!` in production code (independent sweep of kernel, fabric graph
  math, heat kernel, edge, spider). Finding: the one untrusted-input surface —
  the edge wire parser `read_frame` — was already panic-free and bounds-checks
  every length *before* allocating. The rest are genuine invariants
  (`unreachable!` after a `TypeId` guard; closure keyed by its own `TypeId`;
  `registry()` self-initializes so `as_ref().unwrap()` is always `Some`),
  setup-time fail-fast guards (duplicate name / second spider, one-shot run),
  or panics now contained by `catch_unwind` (unregistered fan-out type → clean
  `Crashed`). No reachable-on-bad-input panic remained to convert. Hardened the
  parser's testability: `read_frame`/`write_frame` are now `impl Read`/`Write`
  generic so the untrusted parser is unit-testable in isolation, with 8
  robustness tests (oversized tag/payload rejected pre-allocation, seq-0,
  non-UTF8 tag, truncation, empty stream, MAX boundary, round-trip). This also
  pre-advances Phase 4's fuzz target.

## Phase 2 — Observability & operability (can't run what you can't see)
- [x] **Structured telemetry.** Pull-able `BusStats` (`Bus::stats()` +
  `BusHandle::stats()` for live mid-run reads) — the countable replacement for
  `spider.log(&str)`. Kernel-wide lock-free atomics: `delivered`,
  `pending_evictions`, `vibrations`; lifecycle counts by kind (registered/
  restarted/crashed/detached/quarantined/buffered/flushed/threads_formed/
  intersections_emerged); per-strand `processed` throughput; structural
  (active_strands, threads, intersections, nodes, edges, edge_traffic_total).
  All `Relaxed` (observational, no decisions); `stats()` takes fabric then slots
  locks non-nested (no deadlock — reviewed). 7 tests (`bus_stats.rs`). Edge
  bytes/frames deferred to the Phase-3 edge-stats source.
- [x] **`webctl top`.** Live per-node health + topology of a running bus.
  `spiderweb_viz::render_top(&BusStats)` formats a compact terminal dashboard
  (web-wide counters, lifecycle tally, per-strand throughput table); the
  `spiderweb-viz` `top` example runs a representative web + a `TopReporter`
  strand that pulls `BusHandle::stats()` and re-renders each interval (live,
  introspected from inside the bus); `webctl top` launches it (`topology` stays
  the static reference). Verified end-to-end (a real frame renders with coherent
  counts — crash 4 / restart 3 / quarantine 1 matching spider policy). 2 render
  tests. (Running it caught + fixed a misleading `pending_evictions` doc: it is
  replay-buffer churn, not necessarily lost work.)
- [x] **Bounded-everything audit.** Audited every kernel map/queue. **Found +
  fixed a real leak:** the subscriber map was never pruned, so each crash/
  restart left a dead subscriber (per input socket) — unbounded growth *and*
  growing per-delivery cost under churn. Fixed with lazy pruning: a subscriber
  whose send fails (receiver gone) is removed by `Arc` identity on the next
  delivery (loses nothing — a disconnected receiver can't receive; steady-state
  hot path pays zero). Made observable as `BusStats.subscribers`; regression
  test `subscriber_map_stays_bounded_under_restart_churn` drives heavy churn and
  asserts the count stays flat while `restarted` climbs. Verified the rest:
  `threads` → `THREAD_CAP` (eviction scrubs `node_threads`/`intersections`/
  `merged`); `positions`/`transfers`/`node_threads`/`intersections`/`merged` are
  `NodeId`-keyed → bounded by distinct strand names, which a restart reuses
  (churn-invariant); `edge_traffic` is node-pair-keyed (O(nodes²), cumulative
  u64); `pending` → per-type `PENDING_BUFFER_CAP`; `slots` → name-keyed, restart
  replaces. (Also widened the wind-down failsafe timeouts 5s→10s so the suite
  doesn't flake under parallel load.)

## Phase 3 — Edge transport to production (the distributed story)
- [ ] **Clean teardown.** `EdgeListener::Drop` stops its reader thread (needs
  Phase 1 graceful shutdown so the strand is actually dropped).
- [ ] **Authenticity/integrity.** Per-frame MAC (keyed hash) — reject forged/
  tampered frames. (Introduces the first crypto dependency at the edge.)
- [ ] **Confidentiality.** TLS (rustls) — or an explicit, documented
  "plaintext, trusted-network-only" stance with the threat model spelled out.
- [ ] **Reliability tier 2.** Unbounded reliability w/ no head-of-line blocking
  (sliding-window RLNC) OR end-to-end ACK windows; frees the ring by receiver
  progress, not age.
- [ ] **Session IDs.** Fix the sender-restart blackout (restarted sender's
  reset seqs are dedup'd until they pass the receiver high-water mark).
- [ ] **Cross-machine causality.** Interval Tree Clock stamp on the wire.

## Phase 4 — Quality gates (proof, not vibes)
- [ ] **Fuzz the wire parser.** `cargo-fuzz` on `read_frame` (untrusted input).
- [ ] **Soak/chaos.** Long-run with injected disconnects, panics, floods.
- [ ] **Property tests.** Exactly-once delivery; heat-kernel mass conservation;
  fabric conservation invariants under random traffic.
- [ ] **CI.** `test` + `clippy -D warnings` + `fmt --check` + fuzz corpus on
  every change.

## Phase 5 — Packaging & docs (shippable)
- [ ] **Rustdoc complete**; per-crate README; a quickstart example.
- [ ] **Versioning + CHANGELOG**; semver discipline.
- [ ] **Consolidate**: decide baseline-retire vs fork-merge-back; one line.
- [ ] **Release checklist**: license headers, `cargo publish --dry-run` clean.

---

## Working method
Each item: branch → build with tests → independent review (single-agent if the
review workflow is rate-limited) → fix findings → merge to fork `main` → tick
the box here. No item is `[x]` without tests AND a review pass.

## Done = 
Every Phase 1–4 box checked, Phase 5 packaging complete, `cargo test
--workspace` + `clippy -D warnings` green, the fuzz target runs clean, and the
distributed path has a stated, honest threat model. At that point the in-process
core is shippable as an embeddable library and the distributed layer is
deployable on a trusted network (or any network once TLS lands).
