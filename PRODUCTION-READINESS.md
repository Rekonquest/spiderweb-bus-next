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

## Deliverable verdict (2026-06-23)

**The in-process bus is deliverable.** Phases 1 (Robustness), 2 (Observability),
4 (Quality gates), and 5 (Packaging) are complete; the workspace is green
(`cargo test --workspace`, `clippy -D warnings`, `fmt --check`), the demos run
(highway ~5.5×, weave, heat, `webctl top`), it is documented (README +
quickstart + rustdoc), dual-licensed, CHANGELOG'd, and CI-gated. Ship it as an
embeddable git dependency today (crates.io needs only a one-line rename — the
name `spiderweb` is taken).

**The edge transport is a hardened trusted-network MVP.** Both non-crypto
Phase-3 reliability items are done (`EdgeListener::Drop` teardown + session-ID
restart recovery). The remaining Phase-3 items are deferred by scope, not
overlooked: **auth (MAC) + encryption (TLS)** are where the zero-dep story ends
and a crypto-dependency decision is owed; **RLNC tier-2 + ITC causality** are
heavier distributed features. All four are documented with an honest threat
model (`spiderweb-edge` is plaintext, trusted-network only). Deploy the edge on
a trusted network now; harden for a hostile network when the crypto direction is
chosen.

### Post-audit hardening pass (2026-06-23)

A multi-agent deliverable audit (four area auditors reading the real fork code)
confirmed every Phase 1/2/3/4/5 claim and surfaced five genuine findings — all
now fixed and regression-tested:

- **HIGH** — worker outbox delivery ran *outside* `catch_unwind`, so a fan-out
  clone of an unregistered type (`clone_env` panics) killed the worker silently
  and could hang `Bus::run()`. Delivery moved inside the panic boundary →
  clean `Crashed`. Test: `unregistered_fan_out_type_becomes_clean_crashed_not_silent_death`.
- **MEDIUM** — two raw `.lock().unwrap()` survived the poison sweep (hot
  `Restart` arm + boot) → both `lock_tolerant()`.
- **MEDIUM** — session-dedup oscillation (A→B→A) re-delivered a replayed ring
  as duplicates (exactly-once violation) → bounded per-session high-water map.
  Test: `oscillating_sessions_do_not_re_deliver_a_replayed_ring`.
- **LOW** — `write_frame` length cast to `u32` without bounds → added the caps
  `read_frame` already enforces.
- **LOW/doc** — half-open-connection 30s recovery-latency caveat documented.

(The audit's synthesis agent emitted a spurious "not deliverable" verdict — it
checked the *baseline* repo at `159ddca` instead of the fork; the four area
auditors all read the fork via absolute paths and verified it. The verdict was a
wrong-directory artifact, not a real gap.)

### External lanes (2026-06-25)

`spiderweb-lanes` reaches the bus past one process. A unifying `Lane` contract
(`install() -> LaneParts { strands, guard, bound }`, `bus.add_lane(...)`)
generalizes the edge sender/listener; six lanes built lane-by-lane (branch →
build → test → clippy → fmt → commit), each with a real-socket or pure test:

- **tcp** — re-expresses the unchanged edge transport as the reference Lane
  (`lane_trait_round_trips_over_tcp_loopback`).
- **http** — std-only HTTP/1.1 server + client; `http_round_trips_over_a_real_socket`.
- **api** — bus-native request/reply RPC + JSON-over-HTTP bridge;
  `rpc_round_trips_on_one_bus`, `api_over_http_round_trips`.
- **mesh** — N-peer P2P, per-origin exactly-once + loop suppression;
  `triangle_delivers_each_message_exactly_once`. (Fixed a real bug found here:
  the reader must thread-per-connection, else a node services only its first
  inbound peer — fatal for a mesh.)
- **nat** — rendezvous-relay traversal (`relayd`); `message_federates_through_the_relay`
  + room-msg fuzz.
- **ssh** — bus over a composed system-`ssh` `-L`/`-R` tunnel (encryption with
  zero crypto deps); exact-argv tests + `ssh`-absent → typed error.

std-only except the SSH lane's `std::process` of `ssh`. Demos `http_api`
(curl JSON-RPC) + `p2p_mesh`; `webctl lanes`. Workspace green: 156 tests across
41 binaries, clippy `-D warnings`, fmt. Honest gaps: HTTP/API/mesh/NAT are
plaintext (use the SSH lane to encrypt); the relay is a SPOF; HTTP is
HTTP/1.1 keep-alive (no pipelining); no end-to-end ACK beyond the edge ring.

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
  replaces. (Also widened the **test harness** `recv_timeout`s 5s→10s in
  `spider_recovery.rs` so the suite doesn't flake under parallel load — the
  kernel's own `WORKER_JOIN_DEADLINE` stays 5s, per the Phase-1 note above.)

## Phase 3 — Edge transport to production (the distributed story)
- [x] **Clean teardown.** `EdgeListener::Drop` stops its reader thread and
  releases the bound port (now that Phase-1 graceful shutdown actually drops the
  strand). Drop order: set the shutdown flag (`Release`/`Acquire`), **drop the
  receiver first** (unblocks a reader stuck in a full `tx.send()` — the
  deadlock an independent review caught), self-connect to wake a reader blocked
  in `accept()`, then join (which drops the `TcpListener`, freeing the port).
  Prompt in the idle/full-channel cases; a silent-peer-mid-read is bounded by
  the 30s slow-loris cap (a blocking `read` can't be cancelled in safe std —
  documented). Tests: `dropping_the_listener_releases_the_port`,
  `dropping_the_listener_with_a_flooding_peer_does_not_deadlock` (watchdog-
  bounded). Reviewed (single-agent; CRITICAL deadlock found + fixed).
- [ ] **Authenticity/integrity.** Per-frame MAC (keyed hash) — reject forged/
  tampered frames. (Introduces the first crypto dependency at the edge.)
- [ ] **Confidentiality.** TLS (rustls) — or an explicit, documented
  "plaintext, trusted-network-only" stance with the threat model spelled out.
- [ ] **Reliability tier 2.** Unbounded reliability w/ no head-of-line blocking
  (sliding-window RLNC) OR end-to-end ACK windows; frees the ring by receiver
  progress, not age.
- [x] **Session IDs.** Fixed the sender-restart blackout. Each frame now carries
  a per-process **session id** (wall-clock nanos, distinct across restarts); the
  wire frame is `[u64 session][u64 seq][u32 tag_len][tag][u32 payload_len][payload]`.
  The receiver keeps a **bounded per-session high-water map** (`SESSION_TRACK`
  entries, oldest evicted) and delivers a frame only if its `seq` exceeds its
  OWN session's mark. A restarted sender (new session, seqs reset to 1) starts at
  mark 0 and is recovered cleanly instead of having its low seqs skipped against
  the old high-water (the multi-thousand-frame blackout). A session that briefly
  **reappears** (oscillation A→B→A, or two senders sharing one listener) resumes
  its prior mark rather than resetting to 0 — so it cannot re-deliver a replayed
  ring as duplicates (exactly-once across sessions). Bounded state. Regression
  tests `restarted_sender_recovers_instead_of_blacking_out` (5 frames across a
  simulated restart, all delivered, no dups) and
  `oscillating_sessions_do_not_re_deliver_a_replayed_ring` (A→B→A replay adds
  nothing). Caveat: the single reader thread reads one connection to EOF/timeout
  before accepting the next, so a **half-open** prior connection can delay a
  restarted sender's recovery up to `READ_TIMEOUT_SECS` (30s) — latency, not loss.
- [ ] **Cross-machine causality.** Interval Tree Clock stamp on the wire.

## Phase 4 — Quality gates (proof, not vibes)
- [x] **Fuzz the wire parser.** Std-only, stable-Rust, deterministic randomized
  fuzz of `read_frame`: 200k adversarial byte streams (uniform-random AND
  header-shaped with attacker-chosen length prefixes), asserting it never panics
  and that any accepted frame honors its bounds (seq≠0, tag≤MAX_TAG,
  payload≤MAX_PAYLOAD). Runs in CI with no nightly/deps; a `cargo-fuzz` target is
  the documented optional follow-on. (`read_frame_never_panics_on_adversarial_input`.)
- [x] **Property tests.** Randomized, seeded, std-only: **exactly-once fan-out**
  across random message/subscriber shapes (`fan_out_is_exactly_once_across_random_shapes`);
  **heat-kernel mass conservation** over 3000 random graphs at multiple `t`
  (`mass_conservation_property_on_random_graphs`, also asserts felt-strengths ∈
  [0,1]). (Fabric edge-conductance invariants already covered by the
  `adjacency_*` unit tests under all transfer policies.)
- [~] **Soak/chaos.** Covered by the existing chaos suite — sustained crash/
  restart churn (`subscriber_map_stays_bounded_under_restart_churn`), a wedged
  strand (`wedged_strand_does_not_hang_shutdown`), panic-next-to-neighbor, and a
  flooding-peer teardown — plus the randomized property tests. A single long-run
  combined soak harness is the remaining nice-to-have.
- [x] **CI.** `.github/workflows/ci.yml` — `fmt --all --check` + `clippy
  --workspace --all-targets -D warnings` + `cargo test --workspace` on every
  push/PR, with build cache. Workspace `fmt`-normalized so the gate is green.

## Phase 5 — Packaging & docs (shippable)
- [x] **Docs + quickstart.** Top-level `README.md` (what it is, runnable
  quickstart, architecture-in-a-breath, crate map, solid-vs-honest-gaps, build/
  test, MSRV). Every crate carries rich `//!` module docs (rustdoc). Demos are
  runnable examples (`cargo run -p <crate> --example <name>`).
- [x] **Versioning + CHANGELOG.** `CHANGELOG.md` (Keep-a-Changelog style),
  workspace pinned at `0.1.0`, semver-from-1.0 stated.
- [x] **Consolidate.** Decision recorded: the fork **`Spiderweb-Bus-Next` is the
  canonical product line** (superset = in-process core + edge + heat); the
  baseline `Spiderweb Bus` is the historical stable point. No merge-back needed.
- [x] **Release checklist.** `LICENSE-MIT` + `LICENSE-APACHE` added (dual,
  matching the manifest). `cargo publish --dry-run -p spiderweb` **packages +
  verifies clean** (builds in isolation, 9 files). Honest finding: the bare name
  `spiderweb` is **already taken on crates.io**, so the repo is deliverable as a
  git dependency as-is, and a crates.io publish needs a one-line rename (e.g.
  `spiderweb-bus`). Adding a `repository` URL clears the last manifest warning.

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
