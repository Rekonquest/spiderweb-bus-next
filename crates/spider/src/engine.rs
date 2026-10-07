//! The cognitive engine — PREDICT-surprise as the intrusion signal.
//!
//! The spiderweb bus is dual-purpose: a secure bus, and — because every strand
//! is typed pub/sub and the fabric records traffic — one large sensor. This
//! module is the reader of that sensor: a deterministic, closed-form classifier
//! over typed events (no model, no sampling, no free text), built from the
//! eight roots of `atom-cognitive-engine`.
//!
//! Root mapping (design §1):
//!
//! | Root | Here |
//! |---|---|
//! | ATTEND | `attend_weight` — a salience scalar, never a decision |
//! | BIND | a trace nucleated on first observation of a node |
//! | REINFORCE | a matching observation raises support/mass (rate bounded) |
//! | DECAY | time passes first: support/mass age every tick, traces retire at floors |
//! | PREDICT | per-node rhythm (`Ewma`) and the surprise on mismatch — the intrusion signal |
//! | REVISE | a recovered node dissolves the threat belief before a benign one replaces it |
//! | GATE | `crate::gate::gate` — the only decider |
//! | CONSERVE | `crate::conserve::AlertBudget` — the alert budget, weakest-first |
//!
//! Everything is deterministic: the engine owns a logical clock advanced by
//! [`CognitiveEngine::tick`], so a test drives it tick-by-tick and gets the same
//! result every time. There is no wall-clock reading and no I/O in this module.

use crate::conserve::AlertBudget;
use crate::gate::{attend_weight, gate, AbstainReason, Candidate, Decision, Flavor, Verdict};
use spiderweb::TransferPolicy;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::hash::{Hash, Hasher};

/// Engine-default numbers mirrored from `atom-cognitive-engine`
/// (`references/engine-api.md`): initial mass 1.0 / support 0.7, reinforce
/// +0.18 mass / +0.12 support, contradiction factors 0.82 / 0.55, surprise
/// boost 1.5, decay 0.035 / 0.02, expire floor 0.10, `min_reliability` 0.16.
const INITIAL_SUPPORT: f64 = 0.7;
const INITIAL_MASS: f64 = 1.0;
const MASS_CONTRADICTION_FACTOR: f64 = 0.55;

/// A configuration error. `validate` is fail-closed: an out-of-range knob is a
/// construction error, and [`crate::Spider::with_cognition`] turns an invalid
/// config into cognition-OFF rather than a crash (abstain toward the safe
/// default).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    OutOfRange(&'static str),
    WeakAsymmetry,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::OutOfRange(k) => write!(f, "cognitive config: `{k}` out of range"),
            ConfigError::WeakAsymmetry => write!(
                f,
                "cognitive config: O10 violated — (1 - contradiction_factor) must exceed reinforce_support"
            ),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Every knob of the cognitive path. `enabled: false` is the default, and is
/// the D42 switch: with cognition off the spider is byte-identical to v1.3.
#[derive(Debug, Clone, PartialEq)]
pub struct CognitiveConfig {
    /// Master switch. `Spider::new()` uses the default (off).
    pub enabled: bool,
    /// Observations required before a node has a baseline. Below this the
    /// spider abstains rather than alerting (design §3, "No baseline").
    pub warmup_events: u64,
    /// Relative surprise above which an observation is a mismatch (`> 0`).
    pub surprise_band: f64,
    /// Consecutive deviating observations required to corroborate (`>= 2`:
    /// one deviation is a sample, not a pattern).
    pub min_corroboration: u32,
    /// Trace reliability floor (`> 0`).
    pub min_reliability: f64,
    /// PREDICT's mismatch salience boost (`>= 1.0`; the engine uses 1.5).
    pub surprise_boost: f64,
    /// EWMA smoothing factor for the rhythm, in `(0, 1)`.
    pub ewma_alpha: f64,
    /// Denominator floor for relative magnitude surprise (`> 0`).
    pub magnitude_floor: f64,
    /// Denominator floor for relative cadence surprise (`> 0`).
    pub cadence_floor: f64,
    /// Alerts admitted per `alert_window` ticks (`>= 1`).
    pub alert_budget: usize,
    /// Length of the CONSERVE window, in ticks (`>= 1`).
    pub alert_window: u64,
    /// Per-tick support decay in `(0, 1)`.
    pub decay_support: f64,
    /// Per-tick mass decay in `(0, 1)`.
    pub decay_mass: f64,
    /// Per-match support gain in `(0, 1]`.
    pub reinforce_support: f64,
    /// Per-match mass gain in `(0, 1]`.
    pub reinforce_mass: f64,
    /// Support multiplier applied on a contradiction, in `(0, 1)`.
    pub contradiction_factor: f64,
    /// Support at or below which a trace retires (`> 0`).
    pub support_floor: f64,
    /// Ticks of no progress before a previously-progressing node is stale
    /// (`>= 1`). The stuck-but-alive detector.
    pub heartbeat_stall_ticks: u64,
    /// Topology policy applied when GATE redirects a congested node.
    pub transfer_policy: TransferPolicy,
}

impl Default for CognitiveConfig {
    fn default() -> Self {
        CognitiveConfig {
            enabled: false,
            warmup_events: 6,
            surprise_band: 0.5,
            min_corroboration: 2,
            min_reliability: 0.16,
            surprise_boost: 1.5,
            ewma_alpha: 0.3,
            magnitude_floor: 1.0,
            cadence_floor: 1.0,
            alert_budget: 4,
            alert_window: 20,
            decay_support: 0.035,
            decay_mass: 0.02,
            reinforce_support: 0.12,
            reinforce_mass: 0.18,
            contradiction_factor: 0.82,
            support_floor: 0.10,
            heartbeat_stall_ticks: 8,
            transfer_policy: TransferPolicy::MergeAll,
        }
    }
}

impl CognitiveConfig {
    /// Fail-closed validation. Also enforces the engine's O10 invariant:
    /// weakening must be harsher than strengthening, i.e.
    /// `(1 − contradiction_factor) > reinforce_support` (0.18 > 0.12 by
    /// default).
    pub fn validate(&self) -> Result<(), ConfigError> {
        let unit_pos = |v: f64| v.is_finite() && v > 0.0;
        if self.warmup_events < 2 {
            return Err(ConfigError::OutOfRange("warmup_events"));
        }
        if !unit_pos(self.surprise_band) {
            return Err(ConfigError::OutOfRange("surprise_band"));
        }
        if self.min_corroboration < 2 {
            return Err(ConfigError::OutOfRange("min_corroboration"));
        }
        if !unit_pos(self.min_reliability) || self.min_reliability > 1.0 {
            return Err(ConfigError::OutOfRange("min_reliability"));
        }
        if !self.surprise_boost.is_finite() || self.surprise_boost < 1.0 {
            return Err(ConfigError::OutOfRange("surprise_boost"));
        }
        if !unit_pos(self.ewma_alpha) || self.ewma_alpha >= 1.0 {
            return Err(ConfigError::OutOfRange("ewma_alpha"));
        }
        if !unit_pos(self.magnitude_floor) {
            return Err(ConfigError::OutOfRange("magnitude_floor"));
        }
        if !unit_pos(self.cadence_floor) {
            return Err(ConfigError::OutOfRange("cadence_floor"));
        }
        if self.alert_budget < 1 {
            return Err(ConfigError::OutOfRange("alert_budget"));
        }
        if self.alert_window < 1 {
            return Err(ConfigError::OutOfRange("alert_window"));
        }
        if !unit_pos(self.decay_support) || self.decay_support >= 1.0 {
            return Err(ConfigError::OutOfRange("decay_support"));
        }
        if !unit_pos(self.decay_mass) || self.decay_mass >= 1.0 {
            return Err(ConfigError::OutOfRange("decay_mass"));
        }
        if !unit_pos(self.reinforce_support) || self.reinforce_support > 1.0 {
            return Err(ConfigError::OutOfRange("reinforce_support"));
        }
        if !unit_pos(self.reinforce_mass) || self.reinforce_mass > 1.0 {
            return Err(ConfigError::OutOfRange("reinforce_mass"));
        }
        if !unit_pos(self.contradiction_factor) || self.contradiction_factor >= 1.0 {
            return Err(ConfigError::OutOfRange("contradiction_factor"));
        }
        if !unit_pos(self.support_floor) || self.support_floor >= INITIAL_SUPPORT {
            return Err(ConfigError::OutOfRange("support_floor"));
        }
        if self.heartbeat_stall_ticks < 1 {
            return Err(ConfigError::OutOfRange("heartbeat_stall_ticks"));
        }
        // O10: weaken harsher than strengthen.
        if (1.0 - self.contradiction_factor) <= self.reinforce_support {
            return Err(ConfigError::WeakAsymmetry);
        }
        Ok(())
    }
}

/// A tiny exponentially-weighted mean/variance tracker — one dimension of a
/// node's learned rhythm.
#[derive(Debug, Clone, Copy, Default)]
struct Ewma {
    mean: f64,
    var: f64,
    n: u64,
}

impl Ewma {
    fn update(&mut self, x: f64, alpha: f64) {
        if self.n == 0 {
            self.mean = x;
            self.var = 0.0;
        } else {
            let delta = x - self.mean;
            self.mean += alpha * delta;
            self.var = (1.0 - alpha) * (self.var + alpha * delta * delta);
        }
        self.n += 1;
    }
}

/// Compact identity of the alert kind a node currently has live, for the
/// change-only (edge-triggered) emission rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum GateKind {
    Restart,
    Quarantine,
    Transfer,
}

fn kind_of(d: &Decision) -> Option<GateKind> {
    match d {
        Decision::Restart { .. } => Some(GateKind::Restart),
        Decision::Block { .. } => Some(GateKind::Quarantine),
        Decision::SetTransfer { .. } => Some(GateKind::Transfer),
        _ => None,
    }
}

/// The learned state of one node: its vibration rhythm, its heartbeat, and its
/// current threat belief.
#[derive(Debug, Clone)]
struct NodeRhythm {
    // --- vibration rhythm (PREDICT's target) ---
    observations: u64,
    last_obs_tick: Option<u64>,
    cadence: Ewma,
    magnitude: Ewma,
    // --- trace weights ---
    support: f64,
    mass: f64,
    hits: u64,
    // --- threat belief ---
    deviations: u32,
    belief_threat: bool,
    emitted_kind: Option<GateKind>,
    // --- heartbeat (liveness without progress) ---
    last_processed: Option<u64>,
    last_progress_tick: u64,
    stalled: u64,
    heartbeat_seen: bool,
}

impl NodeRhythm {
    fn new() -> Self {
        NodeRhythm {
            observations: 0,
            last_obs_tick: None,
            cadence: Ewma::default(),
            magnitude: Ewma::default(),
            support: INITIAL_SUPPORT,
            mass: INITIAL_MASS,
            hits: 0,
            deviations: 0,
            belief_threat: false,
            emitted_kind: None,
            last_processed: None,
            last_progress_tick: 0,
            stalled: 0,
            heartbeat_seen: false,
        }
    }

    /// The engine's reliability formula (`references/engine-api.md`):
    /// `0.45·support + 0.30·min(mass,1) + 0.25·(1−e^(−0.55·hits))`.
    fn reliability(&self) -> f64 {
        0.45 * self.support
            + 0.30 * self.mass.min(1.0)
            + 0.25 * (1.0 - (-0.55 * self.hits as f64).exp())
    }
}

/// A buffered inbound fact, consumed on the next [`CognitiveEngine::tick`].
#[derive(Debug, Clone)]
enum Observation {
    Vibration {
        node: String,
        magnitude: f64,
        flavor: Flavor,
    },
    Heartbeat {
        node: String,
        processed: u64,
    },
}

/// Per-node aggregation of one tick's observations, produced by PREDICT.
#[derive(Debug, Clone, Default)]
struct Evidence {
    skip: bool,
    had_vibration: bool,
    deviating: bool,
    flavor: Option<Flavor>,
    representative: f64,
    surprise: f64,
    during_warmup: bool,
    heartbeat_progress: bool,
}

/// What one observe cycle produced.
#[derive(Debug, Clone, Default)]
pub struct TickOutcome {
    /// Alerts actually admitted by CONSERVE — the only decisions that reach the
    /// wire. `emitted.len() <= alert_budget` per window.
    pub emitted: Vec<Decision>,
    /// Decisions that emit nothing: abstentions (thin evidence) and alerts
    /// CONSERVE dropped for lack of budget.
    pub abstained: Vec<Decision>,
    /// Candidates ATTEND weighted this tick.
    pub considered: usize,
}

/// A read-only answer to "is node X anomalous?" — the query cycle's
/// `assert_or_abstain`. `None` is an abstention.
#[derive(Debug, Clone, PartialEq)]
pub struct Assessment {
    pub node: String,
    pub reliability: f64,
    pub threat: bool,
    pub observations: u64,
}

/// The cognitive engine. Deterministic and bus-independent: feed it facts,
/// advance it with [`tick`](Self::tick), read the decisions it emits.
#[derive(Debug)]
pub struct CognitiveEngine {
    cfg: CognitiveConfig,
    clock: u64,
    nodes: BTreeMap<String, NodeRhythm>,
    silenced: BTreeSet<String>,
    /// External typed warrants awaiting the next GATE (design §8).
    pending_verdict: HashMap<String, Verdict>,
    pending: Vec<Observation>,
    budget: AlertBudget,
    last_digest: u64,
}

impl CognitiveEngine {
    /// Build an engine. Fail-closed: an invalid config is an error.
    pub fn new(cfg: CognitiveConfig) -> Result<Self, ConfigError> {
        cfg.validate()?;
        let budget = AlertBudget::new(cfg.alert_budget, cfg.alert_window);
        Ok(CognitiveEngine {
            cfg,
            clock: 0,
            nodes: BTreeMap::new(),
            silenced: BTreeSet::new(),
            pending_verdict: HashMap::new(),
            pending: Vec::new(),
            budget,
            last_digest: 0,
        })
    }

    pub fn config(&self) -> &CognitiveConfig {
        &self.cfg
    }

    /// The logical tick counter. Advances once per [`tick`](Self::tick).
    pub fn clock(&self) -> u64 {
        self.clock
    }

    // -- inbound facts (validate happens at tick) ----------------------------

    /// A node came online. Nucleates a fresh trace (BIND) so warmup starts now.
    pub fn note_registered(&mut self, node: &str) {
        self.silenced.remove(node);
        self.nodes.entry(node.to_string()).or_insert_with(NodeRhythm::new);
    }

    /// A node left cleanly. Marks it silenced and dissolves its belief: a down
    /// node is not an intruder.
    pub fn note_detached(&mut self, node: &str) {
        self.silence(node);
    }

    /// A node was quarantined. Same as detached for the spider's purposes.
    pub fn note_quarantined(&mut self, node: &str) {
        self.silence(node);
    }

    fn silence(&mut self, node: &str) {
        self.silenced.insert(node.to_string());
        self.nodes.remove(node);
        self.pending_verdict.remove(node);
        self.pending.retain(|o| match o {
            Observation::Vibration { node: n, .. } | Observation::Heartbeat { node: n, .. } => {
                n != node
            }
        });
    }

    /// An external detector's typed warrant (design §8). The spider weights it
    /// at GATE; it never synthesizes one.
    pub fn note_verdict(&mut self, node: &str, verdict: Verdict) {
        self.pending_verdict.insert(node.to_string(), verdict);
    }

    /// A vibration observed at `node` with the given `magnitude` and `flavor`.
    /// `magnitude` is a non-negative scalar distilled from the typed vibration
    /// (`queued` depth for backpressure, dry depth for starvation).
    pub fn note_vibration(&mut self, node: &str, magnitude: f64, flavor: Flavor) {
        self.pending.push(Observation::Vibration {
            node: node.to_string(),
            magnitude: if magnitude.is_finite() && magnitude >= 0.0 {
                magnitude
            } else {
                0.0
            },
            flavor,
        });
    }

    /// A per-strand heartbeat: the node's drained-message counter this tick.
    /// A rising counter is progress; a flat one after progress is stagnation.
    pub fn note_heartbeat(&mut self, node: &str, processed: u64) {
        self.pending.push(Observation::Heartbeat {
            node: node.to_string(),
            processed,
        });
    }

    // -- the cycle -----------------------------------------------------------

    /// Run one observe cycle (`validate → predict → decay → attend → gate →
    /// write → conserve → verify`) and return what it produced.
    pub fn tick(&mut self) -> TickOutcome {
        self.clock += 1;
        let now = self.clock;

        let observations = std::mem::take(&mut self.pending);

        // Deterministic grouping by node.
        let mut by_node: BTreeMap<String, Vec<Observation>> = BTreeMap::new();
        for o in observations {
            let node = match &o {
                Observation::Vibration { node, .. } | Observation::Heartbeat { node, .. } => {
                    node.clone()
                }
            };
            by_node.entry(node).or_default().push(o);
        }

        let mut abstained: Vec<Decision> = Vec::new();
        let mut evidence: BTreeMap<String, Evidence> = BTreeMap::new();

        // --- validate (structure before ATTEND, law O2) + predict -----------
        for (node, list) in &by_node {
            if self.silenced.contains(node) {
                abstained.push(Decision::Abstain {
                    node: node.clone(),
                    reason: AbstainReason::SilencedNode,
                });
                evidence.insert(
                    node.clone(),
                    Evidence {
                        skip: true,
                        ..Evidence::default()
                    },
                );
                continue;
            }

            let r = self.nodes.entry(node.clone()).or_insert_with(NodeRhythm::new);
            let mut ev = Evidence::default();

            for o in list {
                match o {
                    Observation::Vibration {
                        magnitude, flavor, ..
                    } => {
                        ev.had_vibration = true;
                        ev.representative = ev.representative.max(*magnitude);
                        ev.flavor = Some(*flavor);
                    }
                    Observation::Heartbeat { processed, .. } => {
                        Self::heartbeat_predict(
                            r,
                            *processed,
                            now,
                            self.cfg.heartbeat_stall_ticks,
                            &mut ev,
                        );
                    }
                }
            }

            // PREDICT: compare this tick's vibration rhythm to the learned one.
            if ev.had_vibration {
                if r.observations < self.cfg.warmup_events {
                    ev.during_warmup = true;
                }
                if r.observations == 0 {
                    // First ever sample: nothing to predict from.
                    ev.deviating = false;
                } else {
                    let mag_surprise = (ev.representative - r.magnitude.mean).abs()
                        / r.magnitude.mean.abs().max(self.cfg.magnitude_floor);
                    let cadence_surprise = match (r.last_obs_tick, r.cadence.n) {
                        (Some(t), n) if n >= 1 => {
                            let observed = now.saturating_sub(t) as f64;
                            (observed - r.cadence.mean).abs()
                                / r.cadence.mean.abs().max(self.cfg.cadence_floor)
                        }
                        _ => 0.0,
                    };
                    ev.surprise = mag_surprise.max(cadence_surprise);
                    ev.deviating = ev.surprise > self.cfg.surprise_band;
                }
            }

            evidence.insert(node.clone(), ev);
        }

        // --- decay: time passes FIRST, before any write (law O1) ------------
        let config = self.cfg.clone();
        let mut retire: Vec<String> = Vec::new();
        for (name, r) in self.nodes.iter_mut() {
            r.support *= 1.0 - config.decay_support;
            r.mass *= 1.0 - config.decay_mass;
            if r.support < config.support_floor {
                retire.push(name.clone());
            }
        }
        for name in retire {
            self.nodes.remove(&name);
            self.pending_verdict.remove(&name);
        }

        // --- attend → gate → write ------------------------------------------
        let mut considered = 0usize;
        let mut pending_alerts: Vec<(f64, Decision)> = Vec::new();

        for (node, ev) in &evidence {
            if ev.skip {
                continue;
            }
            let r = match self.nodes.get_mut(node) {
                Some(r) => r,
                // The trace retired in DECAY; a fresh observation re-binds it.
                None => self.nodes.entry(node.clone()).or_insert_with(NodeRhythm::new),
            };

            // A node with no vibration, no heartbeat advance, and no detected
            // deviation this tick produced no evidence for ATTEND to weight.
            if !ev.had_vibration && !ev.heartbeat_progress && !ev.deviating {
                continue;
            }

            if ev.deviating {
                if ev.during_warmup {
                    // Thin evidence: no baseline yet.
                    abstained.push(Decision::Abstain {
                        node: node.clone(),
                        reason: AbstainReason::NoBaseline,
                    });
                    continue;
                }
                if r.reliability() < config.min_reliability {
                    abstained.push(Decision::Abstain {
                        node: node.clone(),
                        reason: AbstainReason::LowReliability,
                    });
                    continue;
                }

                // Contradiction: REVISE in miniature — weaken, then (below) the
                // GATE decision decides whether it was a blip or a pattern.
                r.deviations += 1;
                r.hits += 1;
                r.support *= config.contradiction_factor;
                r.mass *= MASS_CONTRADICTION_FACTOR;
                r.belief_threat = true;
                // Advance the arrival clock even on a deviation: cadence is the
                // rhythm of *arrivals*, learned from matches but measured on
                // every arrival (otherwise a burst would fake a false cadence
                // for the next tick).
                if ev.had_vibration {
                    r.last_obs_tick = Some(now);
                }

                // ATTEND weights it; GATE decides. Base weight grows with the
                // vibration's magnitude; the mismatch boost is ×surprise_boost.
                let base = 1.0 + ev.representative;
                let salience = base * attend_weight(true, config.surprise_boost);
                considered += 1;

                let corroborated = r.deviations >= config.min_corroboration;
                // An external warrant is only consumed when GATE can act on it;
                // an uncorroborated sample abstains without spending the verdict.
                let verdict = self.pending_verdict.get(node).copied();
                let candidate = Candidate {
                    node: node.clone(),
                    salience,
                    flavor: ev.flavor.unwrap_or(Flavor::RhythmDeviation),
                    corroborated,
                    verdict,
                    policy: config.transfer_policy,
                };
                let decision = gate(Some(&candidate));
                if corroborated {
                    self.pending_verdict.remove(node);
                }

                match kind_of(&decision) {
                    Some(kind) => {
                        // Change-only emission: one alert per episode. A repeat
                        // of the same live kind is a no-op.
                        if r.emitted_kind != Some(kind) {
                            pending_alerts.push((salience, decision));
                        }
                    }
                    None => abstained.push(decision),
                }
            } else {
                // A match: REINFORCE the rhythm, and REVISE any live threat
                // belief (dissolve-before-replace).
                if ev.had_vibration {
                    if let Some(t) = r.last_obs_tick {
                        r.cadence
                            .update(now.saturating_sub(t) as f64, config.ewma_alpha);
                    }
                    r.magnitude.update(ev.representative, config.ewma_alpha);
                    r.last_obs_tick = Some(now);
                    r.observations += 1;
                    r.hits += 1;
                    r.support = (r.support + config.reinforce_support).min(1.0);
                    r.mass = (r.mass + config.reinforce_mass).min(1.0);
                } else {
                    r.hits += 1;
                    r.support = (r.support + config.reinforce_support).min(1.0);
                    r.mass = (r.mass + config.reinforce_mass).min(1.0);
                }
                r.deviations = 0;
                r.belief_threat = false;
                r.emitted_kind = None;
            }
        }

        // --- conserve: budget after the write (law O6), weakest-first --------
        let admit_now = self.clock;
        let to_admit = std::mem::take(&mut pending_alerts);
        let dropped: Vec<(f64, Decision)> = {
            // Split the strongest `remaining` from the rest before admitting, so
            // we can report the dropped ones as BudgetStarved.
            let remaining = self.budget.remaining(admit_now);
            if remaining >= to_admit.len() {
                Vec::new()
            } else {
                let mut sorted = to_admit.clone();
                sorted.sort_by(|a, b| {
                    b.0.total_cmp(&a.0)
                        .then_with(|| a.1.node().unwrap_or("").cmp(b.1.node().unwrap_or("")))
                });
                sorted.split_off(remaining)
            }
        };
        let emitted = self.budget.admit(admit_now, to_admit);
        for d in &emitted {
            if let (Some(node), Some(kind)) = (d.node(), kind_of(d)) {
                if let Some(r) = self.nodes.get_mut(node) {
                    r.emitted_kind = Some(kind);
                }
            }
        }
        for (_, d) in dropped {
            if let Some(node) = d.node() {
                abstained.push(Decision::Abstain {
                    node: node.to_string(),
                    reason: AbstainReason::BudgetStarved,
                });
            }
        }

        // --- verify: boundary digest (law O9) -------------------------------
        self.last_digest = self.compute_digest();

        TickOutcome {
            emitted,
            abstained,
            considered,
        }
    }

    /// PREDICT over the heartbeat: progress is a match, a flat counter after
    /// progress is a deviation. A node that never progressed has no baseline
    /// and is never called stale (idle is not the same as stuck).
    fn heartbeat_predict(
        r: &mut NodeRhythm,
        processed: u64,
        now: u64,
        stall_threshold: u64,
        ev: &mut Evidence,
    ) {
        match r.last_processed {
            None => {
                r.last_processed = Some(processed);
                if processed > 0 {
                    r.heartbeat_seen = true;
                    r.last_progress_tick = now;
                    ev.heartbeat_progress = true;
                }
            }
            Some(prev) if processed > prev => {
                r.last_processed = Some(processed);
                r.heartbeat_seen = true;
                r.last_progress_tick = now;
                r.stalled = 0;
                ev.heartbeat_progress = true;
            }
            Some(prev) if processed < prev => {
                // Restart: the slot was replaced (a fresh counter). Re-baseline.
                r.last_processed = Some(processed);
                r.stalled = 0;
                ev.heartbeat_progress = true;
            }
            Some(_) => {
                r.last_processed = Some(processed);
                r.stalled += 1;
            }
        }

        if r.heartbeat_seen && r.stalled > stall_threshold {
            // Alive but not advancing: the stuck-node signal.
            ev.deviating = true;
            ev.flavor = Some(Flavor::RhythmDeviation);
            ev.representative = ev.representative.max(r.stalled as f64);
        }
    }

    /// Read-only query (`assert_or_abstain`): a node's assessment, or `None` to
    /// abstain. Never mutates state (Stage-9 law — queries leave traces
    /// byte-identical).
    pub fn assess(&self, node: &str) -> Option<Assessment> {
        let r = self.nodes.get(node)?;
        let reliability = r.reliability();
        if r.observations < self.cfg.warmup_events || reliability < self.cfg.min_reliability {
            return None;
        }
        Some(Assessment {
            node: node.to_string(),
            reliability,
            threat: r.belief_threat,
            observations: r.observations,
        })
    }

    /// A deterministic digest of the engine's state, for the O9 boundary check
    /// and for replay-identity tests.
    pub fn state_digest(&self) -> u64 {
        self.last_digest
    }

    fn compute_digest(&self) -> u64 {
        let mut h = DefaultHasher::new();
        self.clock.hash(&mut h);
        for (name, r) in &self.nodes {
            name.hash(&mut h);
            r.observations.hash(&mut h);
            r.hits.hash(&mut h);
            r.deviations.hash(&mut h);
            r.belief_threat.hash(&mut h);
            r.emitted_kind.hash(&mut h);
            r.support.to_bits().hash(&mut h);
            r.mass.to_bits().hash(&mut h);
            r.magnitude.mean.to_bits().hash(&mut h);
            r.magnitude.var.to_bits().hash(&mut h);
            r.cadence.mean.to_bits().hash(&mut h);
        }
        h.finish()
    }
}