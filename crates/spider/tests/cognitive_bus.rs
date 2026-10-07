//! Bus-level tests for the cognitive spider: the crash wall is unchanged, the
//! D42 on/off switch is load-bearing, and a deviating rhythm on the real
//! `sys.vibration` socket trips the alarm end-to-end.
//!
//! These use only the v1.3 public surface (a `Strand` that subscribes to
//! `ControlMsg`), the same "assertion strand" shape the design's proof plan
//! calls for.

use spider::{CognitiveConfig, Spider};
use spiderweb::{
    Bus, BusHandle, ControlMsg, Socket, Strand, StrandError, StrandSpec, Vibration, SYS_CONTROL,
    SYS_VIBRATION,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The assertion strand: records every `ControlMsg` it sees on `sys.control`.
struct ControlRecorder {
    log: Arc<Mutex<Vec<ControlMsg>>>,
}

impl Strand for ControlRecorder {
    fn name(&self) -> &str {
        "assertion-strand"
    }
    fn inputs(&self) -> &[Socket] {
        const SOCK: Socket = Socket::new::<ControlMsg>(SYS_CONTROL);
        std::slice::from_ref(&SOCK)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for c in bus.recv::<ControlMsg>(SYS_CONTROL)? {
            self.log.lock().unwrap().push(c);
        }
        bus.sleep(Duration::from_millis(10));
        Ok(())
    }
}

/// A cognitive config tuned for bus tests: short warmup, and heartbeat
/// detection effectively disabled so the assertion strand's own inbox activity
/// cannot itself register as a stall (the heartbeat path is proven separately
/// by `tests/cognitive.rs`).
fn bus_cfg() -> CognitiveConfig {
    CognitiveConfig {
        enabled: true,
        warmup_events: 4,
        heartbeat_stall_ticks: 1_000_000,
        ..Default::default()
    }
}

/// A strand that always crashes, immediately.
struct AlwaysCrash;

impl Strand for AlwaysCrash {
    fn name(&self) -> &str {
        "always-crash"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, _bus: &mut BusHandle) -> Result<(), StrandError> {
        Err(StrandError::Crashed("always".into()))
    }
}

/// A strand that emits `sys.vibration` on a steady rhythm, then bursts far
/// outside it — a deviating rhythm (the intrusion stimulus).
struct Rhythmic {
    tick: u64,
}

impl Strand for Rhythmic {
    fn name(&self) -> &str {
        "rhythm-node"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<Vibration>(SYS_VIBRATION);
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        self.tick += 1;
        // Steady for a while, then a burst well outside the learned magnitude.
        let queued = if self.tick <= 12 { 1 } else { 200 };
        bus.publish_static(
            SYS_VIBRATION,
            Vibration::Backpressure {
                node: "rhythm-node".to_string(),
                queued,
                in_flight: 0,
                capacity: 4,
            },
        )?;
        bus.sleep(Duration::from_millis(50));
        Ok(())
    }
}

/// A strand that keeps a steady vibration rhythm — the benign control.
struct Steady;

impl Strand for Steady {
    fn name(&self) -> &str {
        "steady-node"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<Vibration>(SYS_VIBRATION);
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        bus.publish_static(
            SYS_VIBRATION,
            Vibration::Backpressure {
                node: "steady-node".to_string(),
                queued: 1,
                in_flight: 0,
                capacity: 4,
            },
        )?;
        bus.sleep(Duration::from_millis(50));
        Ok(())
    }
}

fn alert_names(log: &Arc<Mutex<Vec<ControlMsg>>>) -> Vec<String> {
    log.lock()
        .unwrap()
        .iter()
        .filter_map(|c| match c {
            ControlMsg::Restart { name } => Some(name.clone()),
            ControlMsg::Quarantine { name, .. } => Some(name.clone()),
            ControlMsg::SetTransfer { node, .. } => Some(node.clone()),
            ControlMsg::Shutdown => None,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The crash wall is deterministic and unchanged: Restart ×3, then Quarantine.
// ---------------------------------------------------------------------------

#[test]
fn crash_wall_is_restart_three_then_quarantine() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut bus = Bus::open();
    bus.register(ControlRecorder { log: log.clone() });
    bus.register_spec(StrandSpec::new("always-crash", || Box::new(AlwaysCrash)));
    bus.register_spider(StrandSpec::new("spider", || Box::new(Spider::new())));
    bus.run_until(Some(Duration::from_millis(900)));

    let seq: Vec<String> = log
        .lock()
        .unwrap()
        .iter()
        .map(|c| match c {
            ControlMsg::Restart { .. } => "restart".to_string(),
            ControlMsg::Quarantine { .. } => "quarantine".to_string(),
            ControlMsg::SetTransfer { .. } => "transfer".to_string(),
            ControlMsg::Shutdown => "shutdown".to_string(),
        })
        .collect();

    assert_eq!(
        seq,
        vec!["restart", "restart", "restart", "quarantine"],
        "the crash wall must stay exactly Restart x3 then Quarantine"
    );
}

// ---------------------------------------------------------------------------
// D42: off is silent under the intruder stimulus; on is not. Both directions.
// ---------------------------------------------------------------------------

#[test]
fn cognition_off_never_alerts_under_a_deviating_rhythm() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut bus = Bus::open();
    bus.register(ControlRecorder { log: log.clone() });
    bus.register_spec(StrandSpec::new("rhythm-node", || Box::new(Rhythmic { tick: 0 })));
    // The v1.3 spider: cognition OFF.
    bus.register_spider(StrandSpec::new("spider", || Box::new(Spider::new())));
    bus.run_until(Some(Duration::from_millis(2500)));

    assert!(
        log.lock().unwrap().is_empty(),
        "with cognition off the spider must emit nothing but crash policy: {:?}",
        log.lock().unwrap()
    );
}

#[test]
fn cognition_on_alerts_on_a_deviating_rhythm() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut bus = Bus::open();
    bus.register(ControlRecorder { log: log.clone() });
    bus.register_spec(StrandSpec::new("rhythm-node", || Box::new(Rhythmic { tick: 0 })));
    let cfg = bus_cfg();
    bus.register_spider(StrandSpec::new("cog-spider", move || {
        Box::new(Spider::with_cognition(cfg.clone()))
    }));
    bus.run_until(Some(Duration::from_millis(2500)));

    let names = alert_names(&log);
    assert!(
        names.iter().any(|n| n == "rhythm-node"),
        "the cognitive spider must alert on the deviating node; saw {names:?}"
    );
}

/// The same cognitive spider must NOT alert on a node that keeps a steady
/// rhythm — the benign control, at the bus level.
#[test]
fn cognition_on_stays_silent_on_a_steady_rhythm() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut bus = Bus::open();
    bus.register(ControlRecorder { log: log.clone() });
    bus.register_spec(StrandSpec::new("steady-node", || Box::new(Steady)));
    let cfg = bus_cfg();
    bus.register_spider(StrandSpec::new("cog-spider", move || {
        Box::new(Spider::with_cognition(cfg.clone()))
    }));
    bus.run_until(Some(Duration::from_millis(2500)));

    assert!(
        log.lock().unwrap().is_empty(),
        "a steady rhythm must never alert (false positive): {:?}",
        log.lock().unwrap()
    );
}