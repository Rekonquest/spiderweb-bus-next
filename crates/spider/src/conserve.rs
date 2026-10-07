//! CONSERVE — the alert budget.
//!
//! The web must never be flooded with alerts by the very mechanism meant to
//! protect it. Emission is already edge-triggered upstream (a strand emits a
//! `Vibration` only when its depth changes), and GATE emits only on a state
//! transition; this module adds the last layer: a rolling-window budget with
//! **weakest-first** admission.
//!
//! When more candidates want an emission slot than the window has left,
//! candidates are ranked by salience (descending, node name as a deterministic
//! tie-break) and only the strongest fit. The weakest are dropped — so a burst
//! of small anomalies can never evict a single severe one.
//!
//! The budget is epistemic admission control, not merely a rate limit: a
//! candidate that cannot fit is, by construction, evidence the engine does not
//! consider worth the operator's attention. That is the same posture as the
//! query side's abstention.

use crate::gate::Decision;

/// A rolling-window budget over emitted control messages.
#[derive(Debug, Clone)]
pub struct AlertBudget {
    capacity: usize,
    window: u64,
    /// Logical tick of every emission still inside the window, oldest first.
    emitted: Vec<u64>,
}

impl AlertBudget {
    /// `capacity` = messages per `window` ticks. Both are validated `>= 1`
    /// by `CognitiveConfig::validate`.
    pub fn new(capacity: usize, window: u64) -> Self {
        AlertBudget {
            capacity,
            window,
            emitted: Vec::new(),
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn window(&self) -> u64 {
        self.window
    }

    /// Drop emissions that have aged out of the window ending at `now`.
    fn prune(&mut self, now: u64) {
        let window = self.window;
        self.emitted
            .retain(|&t| now.saturating_sub(t) < window);
    }

    /// How many emissions are still counted against the budget at `now`.
    pub fn in_window(&mut self, now: u64) -> usize {
        self.prune(now);
        self.emitted.len()
    }

    /// How many emission slots remain at `now`.
    pub fn remaining(&mut self, now: u64) -> usize {
        let used = self.in_window(now);
        self.capacity.saturating_sub(used)
    }

    /// Admit the strongest candidates that fit the remaining budget at `now`.
    ///
    /// `candidates` is `(salience, decision)` for every alert GATE produced
    /// this tick. The returned `Vec` is the subset actually emitted (highest
    /// salience first); the rest are dropped (weakest-first eviction).
    pub fn admit(&mut self, now: u64, mut candidates: Vec<(f64, Decision)>) -> Vec<Decision> {
        self.prune(now);
        let mut remaining = self.capacity.saturating_sub(self.emitted.len());
        if remaining == 0 {
            return Vec::new();
        }

        // Strongest first. `total_cmp` is a total order on f64 (no NaN panic);
        // the node-name tie-break keeps the result deterministic.
        candidates.sort_by(|a, b| {
            b.0.total_cmp(&a.0)
                .then_with(|| node_of(&a.1).cmp(node_of(&b.1)))
        });

        let mut out = Vec::new();
        for (_, decision) in candidates {
            if remaining == 0 {
                break;
            }
            remaining -= 1;
            self.emitted.push(now);
            out.push(decision);
        }
        out
    }
}

/// The node a decision names, for the deterministic tie-break.
fn node_of(d: &Decision) -> &str {
    d.node().unwrap_or("")
}