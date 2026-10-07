//! GATE — the only decider.
//!
//! The engine's law is *ATTEND weights, GATE decides* (`atom-cognitive-engine`,
//! `SKILL.md:32`). ATTEND produces a [`Candidate`] — a weighted concern — and
//! never a [`Decision`]. Only [`gate`] turns a candidate (plus its evidence)
//! into one of the action outcomes below, and only [`Decision::control`] turns
//! an outcome into a `ControlMsg`.
//!
//! The five outcomes map onto the *existing* `ControlMsg` vocabulary
//! (`crates/spiderweb/src/lib.rs`), so the cognitive spider can never invent a
//! message kind:
//!
//! | GATE outcome | `ControlMsg` |
//! |---|---|
//! | pass | *nothing* |
//! | abstain | *nothing* (abstention is a pass on the wire) |
//! | block | `ControlMsg::Quarantine` |
//! | redirect (instance) | `ControlMsg::Restart` |
//! | redirect (topology) | `ControlMsg::SetTransfer` |
//!
//! This module is pure: no clock, no I/O, no state. That is what makes the
//! mapping directly testable.

use spiderweb::{ControlMsg, TransferPolicy};

/// What kind of anomaly a candidate represents. The flavour selects the
/// redirect: a node whose *behaviour* is wrong is re-run (`Restart`); a node
/// that is overflowing but legitimate is routed around (`SetTransfer`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    /// The node's own rhythm (cadence or magnitude) departed its baseline, or
    /// it went stale-but-alive. The node itself is the problem.
    RhythmDeviation,
    /// A rate-mismatch signal (backpressure / starvation) departed the node's
    /// learned rate relationship. The node is legitimate; the weave is not.
    SustainedCongestion,
}

/// A typed warrant from outside the spider — the atom stacks' detector organs
/// (E36 seam detector, E30 chaos organ, envelope, lightcone) publish their
/// findings as their own events; the spider may *weight* them but must never
/// synthesize them (§8 of the design). `Threat` escalates a candidate to a
/// `block`; `Benign` clears it. There is deliberately no scalar "score"
/// variant — that would violate the anti-collapse law.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Benign,
    Threat,
}

/// Why the spider abstained. An abstention is a **pass**: it emits nothing.
/// The reasons mirror the thin-evidence table in the design (§3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbstainReason {
    /// Fewer than `warmup_events` observations since registration — no baseline.
    NoBaseline,
    /// The node's rhythm-trace reliability is below `min_reliability`.
    LowReliability,
    /// Exactly one deviating observation and no corroborating second.
    SingleSample,
    /// The node is `Detached` or `Quarantined` — a down node is not an intruder.
    SilencedNode,
    /// CONSERVE had no budget for the alert this window.
    BudgetStarved,
}

/// GATE's decision. Exactly one per episode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// Benign / explained / steady. Nothing on the wire.
    Pass,
    /// Thin evidence. Also nothing on the wire.
    Abstain { node: String, reason: AbstainReason },
    /// A node judged a threat to the web. Emits `Quarantine`.
    Block { name: String, reason: String },
    /// The node's behaviour is wrong but the node is legitimate. Emits `Restart`.
    Restart { name: String },
    /// Congestion should be routed around. Emits `SetTransfer`.
    SetTransfer { node: String, policy: TransferPolicy },
}

impl Decision {
    /// The **only** mapping from a GATE outcome to the wire. Pass and abstain
    /// return `None`; there is no "OK" message and the control channel stays
    /// clean.
    pub fn control(&self) -> Option<ControlMsg> {
        match self {
            Decision::Pass | Decision::Abstain { .. } => None,
            Decision::Block { name, reason } => Some(ControlMsg::Quarantine {
                name: name.clone(),
                reason: reason.clone(),
            }),
            Decision::Restart { name } => Some(ControlMsg::Restart { name: name.clone() }),
            Decision::SetTransfer { node, policy } => Some(ControlMsg::SetTransfer {
                node: node.clone(),
                policy: *policy,
            }),
        }
    }

    /// Whether this outcome would put a message on `sys.control`.
    pub fn is_alert(&self) -> bool {
        matches!(
            self,
            Decision::Block { .. } | Decision::Restart { .. } | Decision::SetTransfer { .. }
        )
    }

    /// The node this decision names, if any.
    pub fn node(&self) -> Option<&str> {
        match self {
            Decision::Pass => None,
            Decision::Abstain { node, .. } => Some(node),
            Decision::Block { name, .. } => Some(name),
            Decision::Restart { name } => Some(name),
            Decision::SetTransfer { node, .. } => Some(node),
        }
    }
}

/// A weighted concern produced by ATTEND. It carries salience and evidence but
/// is not a decision — GATE turns it into one.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub node: String,
    /// ATTEND's weight: base 1.0, ×`surprise_boost` on a PREDICT mismatch.
    pub salience: f64,
    pub flavor: Flavor,
    /// At least `min_corroboration` consecutive deviating observations. A
    /// single deviation is a sample, not a pattern.
    pub corroborated: bool,
    /// External typed warrant, if any (never synthesized by the spider).
    pub verdict: Option<Verdict>,
    /// The topology policy to apply if GATE redirects a congested node.
    pub policy: TransferPolicy,
}

/// GATE: the only decider. A pure function of the weighted candidate.
///
/// * no candidate → pass
/// * uncorroborated candidate → abstain (single sample)
/// * external `Threat` warrant → block (kill requires outside proof)
/// * external `Benign` warrant → pass (a verdict can clear as well as convict)
/// * corroborated intrinsic surprise → redirect (`Restart` for a rhythm
///   deviation, `SetTransfer` for unexplained congestion) — never a kill:
///   "surprise alone justifies a redirect, not a kill" (design §8)
pub fn gate(candidate: Option<&Candidate>) -> Decision {
    let Some(c) = candidate else {
        return Decision::Pass;
    };

    if !c.corroborated {
        return Decision::Abstain {
            node: c.node.clone(),
            reason: AbstainReason::SingleSample,
        };
    }

    match c.verdict {
        Some(Verdict::Threat) => Decision::Block {
            name: c.node.clone(),
            reason: "external detector verdict: threat".to_string(),
        },
        Some(Verdict::Benign) => Decision::Pass,
        None => match c.flavor {
            Flavor::RhythmDeviation => Decision::Restart {
                name: c.node.clone(),
            },
            Flavor::SustainedCongestion => Decision::SetTransfer {
                node: c.node.clone(),
                policy: c.policy,
            },
        },
    }
}

/// ATTEND's weighting root: a pure salience scalar, never a decision.
///
/// Kept as a standalone function so the "ATTEND weights, GATE decides" split is
/// auditable: this returns a `f64`, and a `f64` cannot be published.
pub fn attend_weight(mismatch: bool, surprise_boost: f64) -> f64 {
    if mismatch {
        surprise_boost
    } else {
        1.0
    }
}