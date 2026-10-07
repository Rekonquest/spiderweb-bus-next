//! `spider` — the default orchestrator for the spiderweb bus, now a cognitive
//! engine.
//!
//! The spider is a regular strand that subscribes to `sys.lifecycle` and
//! `sys.vibration` and publishes `ControlMsg` on `sys.control`.
//!
//! Two things live here, with a hard wall between them:
//!
//! 1. **The crash policy** — a deterministic wall. On each `Crashed` event the
//!    spider publishes `ControlMsg::Restart` for attempts 1–3 and
//!    `ControlMsg::Quarantine` on the 4th. This is unchanged from v1.3 and stays
//!    outside cognition: a crash is a binary fact the kernel already
//!    established, not a candidate for a model.
//!
//! 2. **Cognition** — an *added* reader of a sensor that already exists. The
//!    spider learns each node's normal rhythm from the vibration stream and
//!    treats **surprise (predicted ≠ observed) as the intrusion signal**. See
//!    [`engine`] for the root mapping, [`gate`] for the decision mapping, and
//!    [`conserve`] for the alert budget.
//!
//! **D42 discipline.** Cognition is **off by default**. [`Spider::new`] builds
//! the v1.3 spider byte-for-byte; [`Spider::with_cognition`] enables the
//! cognitive path with a validated config. With cognition off, the spider's
//! observable behaviour — logs, `Restart`/`Quarantine` cadence, the 50 ms tick —
//! is identical to v1.3.
//!
//! The kernel executes the spider's decisions but carries no policy of its own.
//! On a crash the kernel waits for the spider's `ControlMsg`; with no spider
//! registered, a crashed strand simply stays dead.

#![forbid(unsafe_code)]

pub mod conserve;
pub mod engine;
pub mod gate;

pub use conserve::AlertBudget;
pub use engine::{Assessment, CognitiveConfig, CognitiveEngine, ConfigError, TickOutcome};
pub use gate::{attend_weight, gate, AbstainReason, Candidate, Decision, Flavor, Verdict};

use spiderweb::{
    BusHandle, ControlMsg, LifecycleEvent, Socket, Strand, StrandError, Vibration, SYS_CONTROL,
    SYS_LIFECYCLE, SYS_VIBRATION,
};
use std::collections::HashMap;
use std::time::Duration;

/// The spider, optionally carrying a cognitive engine.
pub struct Spider {
    /// Per-strand restart counter, so we can log clean messages. Part of the
    /// deterministic crash wall; never consulted by cognition.
    restarts: HashMap<String, u32>,
    /// The cognitive path, or `None` when cognition is disabled (the default).
    engine: Option<CognitiveEngine>,
}

impl Spider {
    /// The v1.3 spider: crash policy only, cognition **off** (D42 defaults-off).
    pub fn new() -> Self {
        Spider {
            restarts: HashMap::new(),
            engine: None,
        }
    }

    /// Build a spider with the cognitive path enabled, using `cfg`.
    ///
    /// If `cfg.enabled` is `false`, or the config fails validation, the result
    /// is exactly [`Spider::new`] — an invalid config degrades to cognition-off
    /// rather than crashing (abstain toward the safe default). Use
    /// [`Spider::try_with_cognition`] when you want the validation error.
    pub fn with_cognition(cfg: CognitiveConfig) -> Self {
        let engine = if cfg.enabled {
            CognitiveEngine::new(cfg).ok()
        } else {
            None
        };
        Spider {
            restarts: HashMap::new(),
            engine,
        }
    }

    /// Fail-closed variant: returns the config error instead of silently
    /// disabling cognition.
    pub fn try_with_cognition(cfg: CognitiveConfig) -> Result<Self, ConfigError> {
        let engine = if cfg.enabled {
            Some(CognitiveEngine::new(cfg)?)
        } else {
            None
        };
        Ok(Spider {
            restarts: HashMap::new(),
            engine,
        })
    }

    /// Whether the cognitive path is active. `Spider::new()` is always `false`.
    pub fn cognition_enabled(&self) -> bool {
        self.engine.is_some()
    }

    /// Read-only access to the cognitive engine, for tests and introspection.
    pub fn engine(&self) -> Option<&CognitiveEngine> {
        self.engine.as_ref()
    }
}

impl Default for Spider {
    fn default() -> Self {
        Self::new()
    }
}

impl Strand for Spider {
    fn name(&self) -> &str {
        "spider"
    }

    fn inputs(&self) -> &[Socket] {
        // The spider feels two things: lifecycle events (who is online,
        // who crashed) and vibrations (the web's backpressure and other
        // cross-layer signals).
        const SOCKS: [Socket; 2] = [
            Socket::new::<LifecycleEvent>(SYS_LIFECYCLE),
            Socket::new::<Vibration>(SYS_VIBRATION),
        ];
        &SOCKS
    }

    fn outputs(&self) -> &[Socket] {
        const SOCK: Socket = Socket::new::<ControlMsg>(SYS_CONTROL);
        std::slice::from_ref(&SOCK)
    }

    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        let events = bus.recv_all::<LifecycleEvent>()?;
        let vibrations = bus.recv_all::<Vibration>()?;

        // --- the deterministic crash wall (unchanged from v1.3) --------------
        for (sock, evt) in &events {
            if *sock != SYS_LIFECYCLE {
                continue;
            }
            match evt {
                LifecycleEvent::Registered {
                    name,
                    inputs,
                    outputs,
                } => {
                    bus.log(&format!(
                        "👁  {} is online (in: {}, out: {})",
                        name,
                        inputs.join(", "),
                        outputs.join(", ")
                    ));
                }
                LifecycleEvent::Detached { name } => {
                    bus.log(&format!("👋 {} detached (clean exit)", name));
                }
                LifecycleEvent::Crashed { name, reason } => {
                    bus.log(&format!("💥 {} crashed: {}", name, reason));
                    // The spider owns the restart policy: 3 restarts,
                    // then quarantine. It sends the control message; the
                    // kernel only executes it (the kernel decides nothing
                    // about crashes on its own). This is a hard fact, not a
                    // candidate: it is NOT routed through PREDICT/GATE.
                    let count = self.restarts.entry(name.clone()).or_insert(0);
                    *count += 1;
                    if *count <= 3 {
                        bus.log(&format!("🔄 {} sending Restart (attempt {})", name, count));
                        bus.publish_static(
                            SYS_CONTROL,
                            ControlMsg::Restart { name: name.clone() },
                        )?;
                    } else {
                        bus.log(&format!("🛑 {} sending Quarantine (4th crash)", name));
                        bus.publish_static(
                            SYS_CONTROL,
                            ControlMsg::Quarantine {
                                name: name.clone(),
                                reason: format!("4 crashes: {}", reason),
                            },
                        )?;
                    }
                }
                LifecycleEvent::Restarted { name, attempt } => {
                    bus.log(&format!("🔄 {} restarted (attempt {})", name, attempt));
                }
                LifecycleEvent::Quarantined { name, reason } => {
                    bus.log(&format!("🛑 {} quarantined: {}", name, reason));
                }
                LifecycleEvent::Buffered { socket, from } => {
                    bus.log(&format!(
                        "⏳ buffered `{}` from {} (no subscriber yet)",
                        socket, from
                    ));
                }
                LifecycleEvent::Flushed { socket, to, count } => {
                    bus.log(&format!(
                        "📨 flushed {} pending on `{}` to {}",
                        count, socket, to
                    ));
                }
                LifecycleEvent::ThreadFormed { thread, origin } => {
                    bus.log(&format!("🧵 thread {} born at {}", thread, origin));
                }
                LifecycleEvent::IntersectionEmerged { node, threads } => {
                    bus.log(&format!(
                        "✕ intersection emerged at {} (threads {:?})",
                        node, threads
                    ));
                }
            }
        }

        // Feel the web's vibrations. v1.3: backpressure. The spider logs it
        // for the operator; the cognitive path (when enabled) also learns from
        // it.
        for (sock, vib) in &vibrations {
            if *sock != SYS_VIBRATION {
                continue;
            }
            match vib {
                Vibration::Backpressure {
                    node,
                    queued,
                    in_flight,
                    capacity,
                } => {
                    bus.log(&format!(
                        "🌊 backpressure from {node}: {queued} queued ({in_flight}/{capacity} in flight)"
                    ));
                }
                Vibration::Starved { node, ready, depth } => {
                    bus.log(&format!(
                        "💨 starvation at {node}: read-ahead buffer dry ({ready}/{depth} ready)"
                    ));
                }
            }
        }

        // --- the cognitive path (only when enabled; D42) ---------------------
        //
        // Everything below is an ADDITION. With cognition off this block does
        // not execute, so the spider is byte-identical to v1.3.
        if self.engine.is_some() {
            // Feed lifecycle facts: registration starts a baseline, a clean exit
            // or a quarantine silences the node (a down node is not an intruder).
            for (sock, evt) in &events {
                if *sock != SYS_LIFECYCLE {
                    continue;
                }
                match evt {
                    LifecycleEvent::Registered { name, .. } => {
                        self.note_registered(name);
                    }
                    LifecycleEvent::Detached { name } => self.note_detached(name),
                    LifecycleEvent::Quarantined { name, .. } => self.note_quarantined(name),
                    _ => {}
                }
            }

            // Feed vibrations as rhythm observations. The typed vibration names
            // the node it concerns; its magnitude is the depth the web felt.
            for (sock, vib) in &vibrations {
                if *sock != SYS_VIBRATION {
                    continue;
                }
                match vib {
                    Vibration::Backpressure { node, queued, .. } => {
                        self.note_vibration(node, *queued as f64, Flavor::SustainedCongestion);
                    }
                    Vibration::Starved { node, ready, depth } => {
                        let dry = depth.saturating_sub(*ready);
                        self.note_vibration(node, dry as f64, Flavor::SustainedCongestion);
                    }
                }
            }

            // Feed the heartbeat: a node whose drained-message counter stalls
            // while it is alive is stuck, which is invisible to a crash counter.
            // The spider's own slot is excluded — restarted spiders are a
            // disaster, and the spider's job is to watch others, not itself.
            let self_name = self.name().to_string();
            let stats = bus.stats();
            for slot in &stats.per_strand {
                if slot.name != self_name {
                    self.note_heartbeat(&slot.name, slot.processed);
                }
            }

            // Run one observe cycle and emit what GATE decided.
            let outcome = self.engine.as_mut().unwrap().tick();
            for decision in &outcome.emitted {
                if let Some(control) = decision.control() {
                    bus.log(&format!("🚨 spider alert: {decision:?}"));
                    bus.publish_static(SYS_CONTROL, control)?;
                }
            }
        }

        bus.sleep(Duration::from_millis(50));
        Ok(())
    }
}

// --- small forwarding helpers, so the run loop reads cleanly ----------------

impl Spider {
    fn note_registered(&mut self, name: &str) {
        if let Some(e) = self.engine.as_mut() {
            e.note_registered(name);
        }
    }
    fn note_detached(&mut self, name: &str) {
        if let Some(e) = self.engine.as_mut() {
            e.note_detached(name);
        }
    }
    fn note_quarantined(&mut self, name: &str) {
        if let Some(e) = self.engine.as_mut() {
            e.note_quarantined(name);
        }
    }
    fn note_vibration(&mut self, node: &str, magnitude: f64, flavor: Flavor) {
        if let Some(e) = self.engine.as_mut() {
            e.note_vibration(node, magnitude, flavor);
        }
    }
    fn note_heartbeat(&mut self, node: &str, processed: u64) {
        if let Some(e) = self.engine.as_mut() {
            e.note_heartbeat(node, processed);
        }
    }
}