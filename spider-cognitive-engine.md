# Spider-Cognitive-Engine — Design

Status: design only. No source file changes. This document specifies a
proposed replacement for the spider strand in `crates/spider`; every
claim about *current* behaviour cites `file:line` in this repo and every
claim about the engine cites the atom-cognitive-engine skill.

The "spiderweb bus" is dual-purpose: a more secure bus, and — because
every strand is typed pub/sub and the fabric records traffic — one large
sensor. The spider is the reader of that sensor. Today it reads almost
nothing.

---

## 0. What the spider is today

Read `crates/spider/src/lib.rs` (167 lines). It is a regular strand that:

- subscribes `sys.lifecycle` and `sys.vibration` (`crates/spider/src/lib.rs:49-53`),
- publishes only `ControlMsg` on `sys.control` (`:56-59`, `:93-105`),
- implements exactly one policy: on `LifecycleEvent::Crashed`, `Restart`
  for attempts 1–3, `Quarantine` on the 4th (`:89-106`),
- logs every other lifecycle event and both vibration variants and does
  nothing else (`:67-135`, `:141-162`),
- sleeps 50 ms per tick (`:164`).

So the current spider has a single deterministic wall (crash counting)
and no cognition. It never reacts to `Vibration::Backpressure`
(`crates/spiderweb/src/lib.rs:277-282`) or `Vibration::Starved`
(`:287-291`); it only formats them into log lines. ARCHITECTURE.md
already promises the stronger version: "a spider sits in the center,
feels every vibration, and knows when something is benign, a threat,
broken, or just idle. The spider doesn't have to be smart. A
**deterministic classifier** decides what each vibration means"
(`ARCHITECTURE.md:18-21`). This document is that classifier, built out
of the engine's eight roots.

### Non-goal carried forward

`ARCHITECTURE.md:449` lists "**LLM in the spider**" as a non-goal. This
design honours it: the spider is deterministic, closed-form arithmetic
over typed events. There is no model, no sampling, no free text in the
decision path.

---

## 1. Root → function mapping

The engine's eight roots (`atom-cognitive-engine/SKILL.md:25-38`) map
onto the spider as follows. "Input" names the concrete datum the root
consumes; "output" names what changes. Nothing here is aspiration — each
row names the field or event it touches.

| Root | Function in the spider | Input | Output / observable effect |
|---|---|---|---|
| ATTEND | Weight how much each inbound vibration or lifecycle event deserves processing this tick | `Vibration`, `LifecycleEvent` | A salience scalar per event, fed to GATE. Never a decision. |
| BIND | Nucleate a rhythm trace for a node the first time it is observed | node name + first vibration/lifecycle tick | A new per-node trace; nothing routed. |
| REINFORCE | Strengthen a node's rhythm trace when an observation matches its prediction | predicted ≈ observed | Trace support/mass up (rate bounded, `SKILL.md:26`). |
| DECAY | Weaken a trace with time, disuse, or contradiction; retire at floors | elapsed ticks / silence | Support decays (`engine-api.md:8-10` defaults). |
| PREDICT | Hold each node's expected next-vibration rhythm and produce a **surprise** error on mismatch | expected rhythm vs observed | Surprise scalar; mismatch boosts salience ×1.5 (`engine-api.md:31-34`). **This is the intrusion signal.** |
| REVISE | Resolve a contradiction — a "threat" belief must dissolve before a "benign" belief replaces it | a recovered/renormalized node | Threat trace dissolved first, then replaced (dissolve-first, `SKILL.md:28-30`). |
| GATE | The **only** decider: pass / block / redirect, emitted as `ControlMsg` | weighted candidates + evidence | `ControlMsg` on `sys.control`, or nothing. |
| CONSERVE | Renormalize onto an alert budget after writes; weakest-first eviction | pending alerts + budget | At most `budget` alerts live; weakest dropped first. |

`EXPLORE` is absent by design: the engine demoted it in v0.2 (D20) as
behaviourally inert (`SKILL.md:33-35`). The spider inherits that
demotion — no seeded tie-breaks, no cooling schedule.

### PREDICT is the intrusion signal (the load-bearing argument)

This is the core claim of the design, so state it plainly.

A node's *normal* behaviour is a rhythm: `cap-source` publishes at
~1000 ms (`crates/cap-source/src/lib.rs:10-15, 32-38`); a
`HighwayStrand` oscillates on its backlog; a `PreloadStrand` on its
read-ahead gauge. Each of these is a timing series the spider can
observe over `sys.vibration` and `sys.lifecycle`.

PREDICT holds, per node, the expected cadence and magnitude of the next
vibration. When the observed event matches, REINFORCE confirms the
rhythm. When it does not, PREDICT emits a **surprise** = (predicted ≠
observed) error. Surprise is not a heuristic the spider invents: the
engine makes PREDICT load-bearing by construction — mismatch boosts
salience ×1.5 (`SKILL.md:63-65`, `engine-api.md:31-34`), and that boost
is ablation-gated (remove PREDICT and the gate fails). The spider uses
that same boost: an event that surprises the model is the one worth
looking at.

Why this is the intrusion signal and not merely congestion:

- Congestion is *explained* by the rhythm — a slow sink draining a
  fast source raises backlog monotonically and the rise is predicted by
  the known rates. Surprise ≈ 0. Benign.
- An intrusion is *unexplained* deviation — a strand whose output rate,
  timing, or participation pattern departs from its own learned
  baseline, at a node whose role predicts otherwise. Surprise > 0.

The prediction target is deliberately narrow: **per-node rhythm**, not
message content. The spider cannot read payloads (it subscribes only to
`LifecycleEvent` and `Vibration`, not to work sockets), so it never
needs to. Content-level judgment is the atom stacks' job (§8).

---

## 2. GATE is the only decider

The engine's law is "ATTEND weights, GATE decides" (`SKILL.md:32`).
ATTEND is a *weighting* root: it multiplies salience and produces no
output on its own. If ATTEND could emit a `ControlMsg`, the two roots
would collapse into one and the ablation gate that proves ATTEND is
load-bearing would become meaningless. So: **ATTEND never publishes.**

GATE consumes the weighted candidate set plus its evidence and produces
exactly one of three outcomes. Each maps to a concrete `ControlMsg`
variant — and only to variants that already exist in
`crates/spiderweb/src/lib.rs:242-258`. No new `ControlMsg` is proposed.

| GATE outcome | Meaning in the web | Emitted control | Notes |
|---|---|---|---|
| **pass** | Vibration is benign / explained / steady | *nothing* — no `ControlMsg` | Silence is the pass. There is no "OK" message; the control channel stays clean. |
| **abstain** | Evidence is thin (§3) | *nothing* — no `ControlMsg` | Abstention is also a pass. Same wire behaviour as pass, different internal record. |
| **block** | A node is judged a threat to the web | `ControlMsg::Quarantine { name, reason }` (`:246-249`) | The existing hard stop. |
| **redirect (instance)** | The node's *behaviour* is wrong but the node is legitimate — re-run it | `ControlMsg::Restart { name }` (`:243-245`) | Reuses the existing restart path; the 3-then-quarantine counter still applies. |
| **redirect (topology)** | Congestion should be routed around, not stopped | `ControlMsg::SetTransfer { node, policy }` (`:254-257`) | Sets `TransferPolicy::HopTo(_)` / `MergeAll` (`:341-350`). The node keeps running; only the weave changes. |

Three constraints on GATE:

1. **One outcome per episode.** A threat episode yields one block and
   one message. Re-emission only on a state *transition* (see CONSERVE,
   §4), mirroring the change-only rule the emitters already use
   (`crates/highway/src/lib.rs:479-496`, `crates/preload/src/lib.rs:444-462`).
2. **GATE cannot invent a message kind.** The five variants above are
   the complete action vocabulary. Anything the spider wants to say that
   is not one of them is a log line, not a control message.
3. **GATE is downstream of the deterministic walls.** A
   `LifecycleEvent::Crashed` is a hard fact, not a candidate (§6, E36).

### The crash wall stays deterministic

The current 3-restart-then-quarantine rule (`crates/spider/src/lib.rs:89-106`)
must NOT be routed through PREDICT/GATE in the new spider. A crash is a
binary observed fact from the kernel (`crates/spiderweb/src/lib.rs:124-127`),
already certified. Cognition operates on the *residual* — the
vibration stream and the rhythm model — not on facts the kernel already
established. This keeps the one existing load-bearing policy untouched
and is what makes D42 possible (§6).

---

## 3. Abstention — `assert-or-abstain`

The engine's query cycle ends in `assert_or_abstain`; a query never
asserts before GATE and returns `claim: None` on unknown ground
(`SKILL.md:60-65`, `references/roots-and-ordering.md:37`). Applied here:
when the spider cannot support a block/redirect, it **abstains**, and an
abstention is a **pass** — no `ControlMsg`, no alert.

"Thin evidence" for vibrations, concretely. Abstain when **any** holds:

| Condition | Concrete test |
|---|---|
| No baseline | The node has produced fewer than `warmup_events` observed vibrations/lifecycle events since `Registered` (`:68-72`). No trace exists to predict from. |
| Low reliability | The node's rhythm trace reliability is below the engine floor `min_reliability = 0.16` (`engine-api.md:11`, `:48-50`). |
| Single sample | Exactly one deviating observation and no corroborating second class (§8). One deviation is a sample, not a pattern. |
| Silenced node | The last event was a `Detached` (`:80-82`) or the node is `Quarantined` (`:111-113`) — a down node is not an intruder. |
| Budget starved | CONSERVE has no budget to raise the alert this window (§4). **Only when the engine is in its strained context** (§6, E36). |

The asymmetry is deliberate and is the whole safety story of an alarm:
**a false positive costs the operator's trust; a false negative costs
nothing that did not already exist.** The alarm is additive (§10), so
the safe default on insufficient evidence is to stay silent. This is
the "abstain toward the safe default" discipline, and it matches the
harness protocol where `INSUFFICIENT EVIDENCE` is a valid result.

Two consequences to accept honestly:

- Warmup means the first minutes of a web are blind. That is correct —
  you cannot call a deviation a deviation before you know the mean.
- A patient attacker who stays inside the envelope during warmup is not
  seen. This is a named limit (§10), not a bug.

---

## 4. CONSERVE — the alert budget

The web must never be flooded with alerts by the very mechanism meant to
protect it. Two layers of defence, one already in the bus.

**Layer 1 — already true: emission is change-only.** A strand emits a
`Vibration` only when backlog depth changes
(`crates/highway/src/lib.rs:479-496`), and `Vibration::Starved` re-emits
only when the drought depth changes (`crates/preload/src/lib.rs:444-462`,
comment: "a true dual of the highway's re-emit-on-change backpressure").
So a *steady* jam produces a clean rise-and-fall, not a stream
(`ARCHITECTURE.md:225-227`). The spider's input is therefore already
edge-triggered, not level-triggered. (The atom-os-kernel doctrine names
the same law: "Signal wire is edge-triggered (one fire per condemnation
episode) — level-triggering floods the … queue and starves other signal
sources.")

**Layer 2 — proposed: the output is edge-triggered too.** GATE emits on
a *transition* only:

| Internal transition | Output |
|---|---|
| pass → block | one `Quarantine` |
| block → pass (REVISE dissolved the threat) | one log line; no control message |
| pass → redirect | one `Restart` or `SetTransfer` |
| state unchanged | nothing |

**Budget and eviction.** CONSERVE renormalizes onto a rolling window
budget `alert_budget` (proposed default: a small constant, e.g. 4
control messages per window, operator-tunable and validated). When the
budget is exhausted, the incoming candidate competes with pending
candidates and the **weakest-first eviction** rule applies
(`SKILL.md:33`): the lowest-salience pending concern is dropped, not the
newest. This stops a burst of small anomalies from evicting a single
severe one.

The budget is not only a rate limit — it is an *epistemic admission
control*. An alert that cannot fit the budget is, by construction,
evidence the engine does not consider worth the operator's attention.
That is the same posture as the query side's abstention.

---

## 5. Ordering laws for the spider's cycles

The engine gives canonical cycles and ten behavioral laws
(`references/roots-and-ordering.md:31-40, 44-55`). The spider must run
a cycle that respects them. Two cycles, mirroring the engine's.

### Spider observe cycle (the hot path, every tick)

```
validate  →  predict  →  decay  →  attend  →  gate  →  write  →  conserve  →  verify
```

| Stage | Concrete spider action |
|---|---|
| validate | Frame the inbound `LifecycleEvent`/`Vibration`; reject events for unknown/detached nodes. |
| predict | Compute expected rhythm per node; emit surprise on mismatch (×1.5 salience). |
| decay | Time passes **first** — before any write. Silence ages every trace. |
| attend | Weight each candidate by salience (weighted, never decisive). |
| gate | pass / abstain / block / redirect → `ControlMsg` or nothing (§2). |
| write | BIND (new node) / REINFORCE (match) / REVISE (contradiction). |
| conserve | Budget after the write; weakest-first eviction (§4). |
| verify | Boundary check / digest of the tick (§ law O9). |

### Spider query cycle (operator asks "is node X anomalous?")

```
validate  →  attend  →  gate(eligibility)  →  retrieve  →  assert_or_abstain  →  verify
```

Read-only except for stigmergy-style marking; it must not mutate the
rhythm model (the engine's Stage-9 law: queries leave state
byte-identical, `SKILL.md:81-83`).

### Which laws are load-bearing for the spider, and why

| Law | Spider obligation |
|---|---|
| **O1** Time passes before learning | DECAY precedes the BIND/REINFORCE write, so a node's baseline is aged, never inflated by its own history. |
| **O2** Order/structure before ATTEND | A node's trace must exist (validate) before ATTEND can weight it. |
| **O3** GATE before blend/route-merge | GATE runs before any `SetTransfer`-style routing change; admission ≠ license. |
| **O6** CONSERVE last | The budget is applied after the write, never before. |
| **O7** Never assert before retrieval + eligibility | **The spider never emits a `ControlMsg` before GATE.** This is the "never assert before GATE" rule and it has a direct wire consequence: no control message is ever built on ATTEND output alone. |
| **O9** Verify at boundaries | Each tick ends with a digest/boundary check so reordered state is rejected. |
| **O10** Weaken harsher than strengthen | The invariant is arithmetic: `(1 − contradiction_support_factor) > reinforcement_support`, i.e. cost 0.18 > gain 0.12 (`SKILL.md:56-58`). Applied: a *contradiction* of a threat belief (a node recovered) damages the threat trace harder than a single confirming observation builds it. A one-off blip cannot establish a threat; a genuine recovery cannot be outvoted by accumulated suspicion. |

O4 (load before traffic), O5 (acyclic compositions), and O8 (settle
before assert) are structural or already satisfied by the bus's
construction; the spider must not break them but they impose no new
work on a single orchestrator.

---

## 6. Discipline checklist (implementation must satisfy)

These are gates, not guidelines. If the implementation cannot check a
box, that box names the failing behaviour.

| # | Discipline | Concrete obligation |
|---|---|---|
| 1 | **E36 — deterministic walls first, cognitive judge after, strained context only** | (a) `Crashed` → restart/quarantine stays a deterministic wall, unchanged from `crates/spider/src/lib.rs:89-106`. (b) A cognitive PREDICT/GATE pass runs **only** on the residual after walls. (c) The judge runs only when the web is strained (budget available / evidence above floor); otherwise abstain. (`atom-os-kernel/SKILL.md:27`: "Walls are dumb and total; destruction requires proof … abstain = starved but alive.") |
| 2 | **Abstain toward the safe default** | On insufficient evidence → no `ControlMsg` (§3). Abstention is a pass. |
| 3 | **Anti-collapse law** | Geometric/structural facts (topology, `attenuation` (`crates/spiderweb/src/lib.rs:386-410`), change-only emission) and statistical scores (PREDICT surprise, trace reliability) are **separate epistemic classes**. They must never be averaged, summed, or fused into one confidence number. A join between them is an explicit, typed warrant with a named relation — never a scalar. (`atom-harness-discipline`: "Never average their geometries, scores, or intermediate contracts into one generic retrieval result.") |
| 4 | **D42 — defaults-off, byte-identical when off** | With cognition disabled, the spider's observable behaviour is bit-for-bit today's: same logs, same `Restart`/`Quarantine` cadence, same 50 ms tick (`:164`). Every new knob has a config field, validation range, and a gate proving both directions (on/off). No default changes silently. (`SKILL.md:84-86`, `references/roots-and-ordering.md:70-82`.) |
| 5 | **Root admission filter + demotion law for any NEW vibration type** | Any new `Vibration` variant must be irreducible, load-bearing, composable, and evidenced (`references/roots-and-ordering.md:3-16`). A candidate that fails is demoted or stays out — as `EXPLORE` was demoted (D20) for being behaviourally inert. See §7 for which proposed variants fail. |

---

## 7. New vibration types

`Vibration` currently has two variants (`crates/spiderweb/src/lib.rs:271-292`)
and is a built-in clone type (`:955`), so a new variant needs no clone
registration change — only the enum and its emitters. `ARCHITECTURE.md`
and `ROADMAP.md` both explicitly reserve the next one.

### Proposed — admit

**`Vibration::HeartbeatStale { node, since_ms, expected_ms }`**

Reserved by name: `ARCHITECTURE.md:218` ("backpressure now, heartbeat
staleness later") and `ROADMAP.md:383` ("Heartbeat staleness detection
(strand alive but stuck)"). This is a genuinely different failure from a
crash: a crashed strand is a `LifecycleEvent` the kernel owns, while a
stuck strand is alive and silent, which today is invisible.

- **Invariant:** emitted when `now − last_tick_age(node) > k ·
  expected_ms` and re-emitted only when the *staleness class* changes
  (change-only, matching `crates/highway/src/lib.rs:483` and
  `crates/preload/src/lib.rs:444-462`).
- **Irreducible** — no existing vibration expresses liveness-without-
  progress; `Backpressure`/`Starved` are both rate mismatches with the
  node still moving.
- **Load-bearing** — its ablation gate is concrete: a stuck strand must
  be detected; without this vibration the spider cannot see it.
- **Composable** — a plain typed event, same socket, no ordering
  constraint beyond the observer's cycle.
- **Evidenced** — reserved in the architecture, demanded by the roadmap.

A heartbeat source is already available: `StrandStats.processed`
(`crates/spiderweb/src/lib.rs:167-171`) is a per-strand counter and
`Bus::stats` (`:178-235`) exposes it; a rising `processed` is a
heartbeat, a flat one is stagnation. Whether the emitter is the kernel,
`Bus::stats` polled by a small strand, or a `spiderweb-heat`-style
gauge is an implementation choice, not a design change.

### Proposed — admit (topology, optional)

**`Vibration::FabricBurst { threads, window_ms }`** — a sudden change in
thread count (`LifecycleEvent::ThreadFormed`, `:147-150`) inside a
window, i.e. many parentless messages arriving at once. This is a *fact
about the fabric*, not a score, so it satisfies the anti-collapse law.
It is lower priority than heartbeat and should only be added if the
proof plan shows a fabric burst that heartbeat cannot catch.

### Propose and REJECT — must stay out

These fail the admission filter and must NOT be added as `Vibration`
variants:

| Candidate | Fails because | Verdict |
|---|---|---|
| `Vibration::RhythmDeviation { node, score }` | It is a *derived prediction output*, not a sensory primitive. The spider computes it from heartbeat/backpressure timing; emitting it would move the judge into the sensor and make PREDICT non-irreducible. | **Stay out.** The spider computes surprise internally. |
| `Vibration::AnomalyScore(f64)` | A single fused confidence number over structural facts and statistical scores — a direct violation of the anti-collapse law (§6, #3). | **Stay out.** |
| `Vibration::Alert` / `Vibration::Intrusion` | An alert is the *judge's output* and already has a channel: `sys.control` / `ControlMsg`. Putting it on `sys.vibration` collapses sensor and judge into one class and lets any strand forge an alarm. | **Stay out.** |
| `Vibration::Taint { node, class }` | Semantic taint is the atom stacks' detector's claim, not a bus-level vibration the spider may author (§8). | **Stay out of the kernel**; belongs to the stack's own typed evidence. |

The rule behind all four rejections: a `Vibration` is something the web
**feels**. An alert is something the spider **decides**. Keeping them in
different types is what makes the "deterministic classifier"
(`ARCHITECTURE.md:20-21`) auditable.

---

## 8. What the atom stacks contribute (and how separation is kept)

The spider must never invent judgment. Its own machinery produces
exactly one thing: **per-node rhythm surprise**. Everything that
requires knowing what a message *means* comes from outside.

Where external evidence enters:

- The atom stacks' detector organs — the E36 seam detector + judge, the
  E30 chaos organ / permutation entropy, the envelope, and lightcone
  geometry (`atom-os-kernel/SKILL.md:3, 27`) — publish their findings as
  their own typed events with provenance.
- The spider subscribes to those events the same way it subscribes to
  `sys.vibration`: as **evidence**, carried on a socket, never as its own
  prediction.

The separation is structural, and it is the same rule the harness
discipline states: keep the owners distinct; stored or retrieved text
"carries no causal warrant on its own"; join components only through an
explicit typed warrant (`atom-harness-discipline`).

| Epistemic class | Owner | Bridged to the other by |
|---|---|---|
| Structural fact (topology, traffic, attenuation, heartbeat) | the bus / kernel | direct observation, no inference |
| The spider's own statistical prediction (rhythm, surprise) | the spider's PREDICT | internal only |
| The detector's verdict (seam, entropy, envelope) | the atom stack | a typed evidence event on a socket |

The spider may **weight** a detector's evidence when forming salience.
It may **not** synthesize the detector's verdict, average it into its
own surprise, or present one as the other. If a claim needs both
classes, the join is an explicit typed relation (`supports`,
`corroborates`, `contradicts`, …) — never a scalar product. This is why
an atom stack's verdict can escalate a candidate to `block` while the
spider's own surprise alone generally cannot (surprise alone justifies a
`redirect`, not a kill).

Honest caveat on provenance: `Vibration` and `LifecycleEvent` carry a
node *name*, not an authenticated identity, and `spiderweb-edge` is an
unauthenticated trusted-network MVP. The spider therefore reasons about
*behaviour of named nodes*, not about *who is speaking*. Authentication
is a separate layer (Imperium's hybrid PQC/AEAD handshake), out of
scope here, and the design does not pretend otherwise.

---

## 9. Proof plan — the capability harness

In the style of `cap-crasher`, `cap-slow-sink`, `cap-source`. The
harness is a set of capability strands plus assertions on the
spider's `ControlMsg` output and `Bus::stats` counters
(`crates/spiderweb/src/lib.rs:178-235`), following the change-only and
countable-telemetry idiom the bus already uses.

### Required harness pieces

| Piece | Role |
|---|---|
| `cap-flood` (proposed) | A source whose publish cadence collapses from a steady period to a burst (a flooding source / deviating rhythm). Modelled on `cap-source` (`crates/cap-source/src/lib.rs`) with a rate schedule. |
| `cap-stuck` (proposed) | A strand that stops advancing while staying alive — the `HeartbeatStale` trigger. Distinct from `cap-crasher`, which exits with `StrandError::Crashed` (`crates/cap-crasher/src/lib.rs:43-51`). |
| `cap-slow-sink` (existing) | The benign heavy load — a steady jam. |
| `cap-source` (existing) | The benign baseline — a clean 1000 ms rhythm. |
| assertion strand | Records every `ControlMsg` seen on `sys.control` and every `Vibration`/`HeartbeatStale` count from `Bus::stats`. |

### Intruder cases — MUST trip the alarm

| Case | Stimulus | Pass criterion |
|---|---|---|
| Flooding source | `cap-flood` bursts far outside its learned period | Within the detection window the assertion strand records **at least one** `ControlMsg` (`Restart` or `Quarantine`) naming the flooding node, and `stats.vibrations` rose. **Fail** if no control message appears by the window end. |
| Deviating rhythm | A node whose inter-arrival period changes sign pattern (period doubling) | Same criterion: a control message naming that node within the window. |
| Silenced/stuck node | `cap-stuck` stops advancing while alive | A `HeartbeatStale` vibration is observed **and** GATE emits a control message for it. **Fail** if the stale node is never named. |

### Benign controls — MUST NOT trip the alarm

| Case | Stimulus | Pass criterion |
|---|---|---|
| Clean baseline | `cap-source` at its declared 1000 ms into `cap-sink` | **Zero** control messages for the whole run. Any alert is a false positive and fails the gate. |
| Steady jam | `cap-source` fast into `cap-slow-sink` | `Backpressure` vibrations appear (the web feels it) **but** no control message: the backlog rise is explained by the known rate mismatch → GATE passes. **Fail** if an alert fires. |
| Normal start/stop | A strand registers, runs, then `Detached` | No alert on `Detached` (`crates/spiderweb/src/lib.rs:121-123`); a down node is not an intruder. |
| Post-recovery | An intruder case, then traffic returns to baseline | REVISE dissolves the threat: exactly one alert per episode (change-only), and **no repeat** while the node stays normal. **Fail** if alerts continue after recovery. |

The two-sided requirement is the point: a detector that never fires and
a detector that always fires both fail. Both an intruder case and a
benign control are mandatory for every mechanism, matching the engine's
"matched baseline, ablation, adversarial test" admission protocol
(`SKILL.md:98-103`).

### Ablation gates (per root)

Each root in §1 needs a gate that fails without it (`SKILL.md:36-38`).
For the spider the natural ablations are: remove PREDICT → flooding
source no longer trips (surprise path gone); remove GATE → `ControlMsg`
is emitted from ATTEND (proves ATTEND-only assertion is caught); remove
CONSERVE → the steady-jam control floods with alerts (proves the budget
is load-bearing); remove REVISE → the post-recovery control keeps
alerting.

---

## 10. Honest limits

**What this does NOT detect.** State it plainly:

- **Authorized-looking traffic.** A well-formed message from a node
  emitting at its normal rhythm is invisible. The spider sees behaviour,
  not intent.
- **Forgery of identity.** A node name is not authenticated (§8). An
  attacker who emits from a name the web already trusts, at that node's
  rhythm, is the baseline.
- **Low-and-slow.** An adversary that stays inside the rhythm envelope
  for the entire observation window never produces surprise. If it
  deviates only after the longest supported window, detection is
  delayed by design.
- **Attacks that are silent by nature.** A compromised strand that stops
  emitting its own vibrations suppresses its own signal; heartbeat
  staleness helps only if a heartbeat source treats the silence as
  anomalous rather than as idleness. "Just idle" and "just compromised"
  are the same observation without external evidence.
- **Semantic and authorization attacks.** These are the atom stacks'
  class (§8), not the spider's. Without a detector organ, the spider
  contributes nothing here.
- **Single-shot events.** One event is a sample, not a pattern (§3). The
  abstention rule deliberately lets a lone blip through.
- **The spider as a target.** The alarm is another strand; a flood aimed
  at the spider's own inbox is a congestion problem, not a detection
  win.

**Why the alarm is additive, not load-bearing.** With the cognitive
spider disabled the bus routes identically: mechanism in the primitive,
policy in the orchestrator. The only load-bearing spider function today
is the crash policy (`crates/spider/src/lib.rs:89-106`), and this design
keeps it as a deterministic wall outside cognition (§6, #1). Everything
in §§1–5 is an *added* reader of a sensor that already exists. That is
the right posture for an alarm: a false negative degrades detection and
nothing else; transport, routing, and restart are unaffected. It is also
what makes D42 checkable — turn cognition off and the bus is
byte-identical to v1.3.

The alarm is therefore real but bounded: it widens the sensor
(`ARCHITECTURE.md:103-108`) into a classifier, and its power comes from
coupling with the atom stacks' authenticated, semantic detectors — not
from the spider alone. The spider contributes the one thing no detector
organ has: the whole web's rhythm, seen from the center.
