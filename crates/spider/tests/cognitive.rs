//! Behavioural tests for the spider-cognitive-engine.
//!
//! These drive the engine's deterministic logical clock tick-by-tick, so every
//! assertion is exact and reproducible. The two-sided requirement is the point:
//! an intruder case that must trip the alarm and a benign control that must not
//! are both mandatory.

use spider::{
    attend_weight, gate, AbstainReason, Candidate, CognitiveConfig, CognitiveEngine, Decision,
    Flavor, Verdict,
};
use spiderweb::{ControlMsg, TransferPolicy};

fn cfg() -> CognitiveConfig {
    CognitiveConfig {
        enabled: true,
        ..Default::default()
    }
}

fn eng() -> CognitiveEngine {
    CognitiveEngine::new(cfg()).expect("default cognitive config must validate")
}

fn count_alerts(o: &spider::TickOutcome) -> usize {
    o.emitted.iter().filter(|d| d.is_alert()).count()
}

fn has_abstain(o: &spider::TickOutcome, reason: AbstainReason) -> bool {
    o.abstained
        .iter()
        .any(|d| matches!(d, Decision::Abstain { reason: r, .. } if *r == reason))
}

// ---------------------------------------------------------------------------
// The intruder case: a deviating rhythm TRIPS the alarm.
// ---------------------------------------------------------------------------

#[test]
fn deviating_rhythm_trips_the_alarm() {
    let mut e = eng();

    // Learn a clean baseline: magnitude 1.0, every tick, for the warmup.
    for _ in 0..6 {
        e.note_vibration("victim", 1.0, Flavor::RhythmDeviation);
        let o = e.tick();
        assert!(o.emitted.is_empty(), "a steady baseline must not alert");
    }

    // First deviation: a sample, not a pattern. GATE abstains.
    e.note_vibration("victim", 100.0, Flavor::RhythmDeviation);
    let first = e.tick();
    assert!(
        first.emitted.is_empty(),
        "a single deviating observation must not alert"
    );
    assert!(
        has_abstain(&first, AbstainReason::SingleSample),
        "one deviation is a sample: expected a SingleSample abstention, got {:?}",
        first.abstained
    );

    // Second consecutive deviation: corroborated -> the alarm trips.
    e.note_vibration("victim", 100.0, Flavor::RhythmDeviation);
    let second = e.tick();
    assert_eq!(count_alerts(&second), 1, "the second deviation must trip");
    match second.emitted[0] {
        Decision::Restart { ref name } => assert_eq!(name, "victim"),
        ref other => panic!("expected a Restart for the deviating node, got {other:?}"),
    }
    // And the mapping to the wire is exactly Restart.
    match second.emitted[0].control() {
        Some(ControlMsg::Restart { name }) => assert_eq!(name, "victim"),
        other => panic!("expected ControlMsg::Restart, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The benign control: a steady rhythm does NOT trip the alarm.
// ---------------------------------------------------------------------------

#[test]
fn steady_rhythm_does_not_trip_the_alarm() {
    let mut e = eng();
    for _ in 0..40 {
        e.note_vibration("calm", 1.0, Flavor::RhythmDeviation);
        let o = e.tick();
        assert!(
            o.emitted.is_empty(),
            "a steady, normal rhythm must never alert (false positive)"
        );
    }
}

// ---------------------------------------------------------------------------
// Thin evidence: a warmup period abstains rather than alerting.
// ---------------------------------------------------------------------------

#[test]
fn warmup_abstains_rather_than_alerting() {
    let mut e = eng();
    // Five steady observations — one short of `warmup_events` (6).
    for _ in 0..5 {
        e.note_vibration("newborn", 1.0, Flavor::RhythmDeviation);
        e.tick();
    }
    // A wild value arrives before any baseline exists.
    e.note_vibration("newborn", 500.0, Flavor::RhythmDeviation);
    let o = e.tick();
    assert!(
        o.emitted.is_empty(),
        "no baseline means no alert — abstention is a pass"
    );
    assert!(
        has_abstain(&o, AbstainReason::NoBaseline),
        "expected a NoBaseline abstention, got {:?}",
        o.abstained
    );
}

#[test]
fn silenced_node_abstains() {
    let mut e = eng();
    for _ in 0..6 {
        e.note_vibration("gone", 1.0, Flavor::RhythmDeviation);
        e.tick();
    }
    e.note_detached("gone");
    // A down node is not an intruder, even if it emits.
    for _ in 0..3 {
        e.note_vibration("gone", 900.0, Flavor::RhythmDeviation);
        let o = e.tick();
        assert!(o.emitted.is_empty());
        assert!(has_abstain(&o, AbstainReason::SilencedNode));
    }
}

// ---------------------------------------------------------------------------
// GATE maps every outcome to exactly the design's ControlMsg; ATTEND emits
// nothing.
// ---------------------------------------------------------------------------

fn cand(flavor: Flavor, corroborated: bool, verdict: Option<Verdict>) -> Candidate {
    Candidate {
        node: "n".to_string(),
        salience: 1.5,
        flavor,
        corroborated,
        verdict,
        policy: TransferPolicy::MergeAll,
    }
}

#[test]
fn gate_maps_each_outcome_to_the_exact_control_message() {
    // pass -> nothing
    assert_eq!(gate(None), Decision::Pass);
    assert!(gate(None).control().is_none());

    // abstain (single sample) -> nothing
    let abstain = gate(Some(&cand(Flavor::RhythmDeviation, false, None)));
    assert_eq!(
        abstain,
        Decision::Abstain {
            node: "n".to_string(),
            reason: AbstainReason::SingleSample
        }
    );
    assert!(abstain.control().is_none());

    // block -> Quarantine (only reachable with an external threat warrant)
    let block = gate(Some(&cand(Flavor::RhythmDeviation, true, Some(Verdict::Threat))));
    match block.control() {
        Some(ControlMsg::Quarantine { name, .. }) => assert_eq!(name, "n"),
        other => panic!("block must map to Quarantine, got {other:?}"),
    }

    // redirect (instance) -> Restart
    let restart = gate(Some(&cand(Flavor::RhythmDeviation, true, None)));
    match restart.control() {
        Some(ControlMsg::Restart { name }) => assert_eq!(name, "n"),
        other => panic!("rhythm redirect must map to Restart, got {other:?}"),
    }

    // redirect (topology) -> SetTransfer
    let transfer = gate(Some(&cand(Flavor::SustainedCongestion, true, None)));
    match transfer.control() {
        Some(ControlMsg::SetTransfer { node, policy }) => {
            assert_eq!(node, "n");
            assert_eq!(policy, TransferPolicy::MergeAll);
        }
        other => panic!("congestion redirect must map to SetTransfer, got {other:?}"),
    }

    // an external benign verdict also clears a candidate -> nothing
    let cleared = gate(Some(&cand(Flavor::RhythmDeviation, true, Some(Verdict::Benign))));
    assert_eq!(cleared, Decision::Pass);
    assert!(cleared.control().is_none());
}

#[test]
fn attend_weights_but_never_emits() {
    // ATTEND's output is a scalar factor — the ×1.5 surprise boost — and a
    // scalar cannot be published.
    assert_eq!(attend_weight(true, 1.5), 1.5);
    assert_eq!(attend_weight(false, 1.5), 1.0);

    // Even a very high-salience candidate emits nothing unless GATE decides to.
    let mut c = cand(Flavor::RhythmDeviation, false, None);
    c.salience = 10_000.0;
    let d = gate(Some(&c));
    assert!(
        d.control().is_none(),
        "salience alone must never put anything on the wire"
    );
}

#[test]
fn external_threat_verdict_escalates_to_quarantine() {
    // The intrinsic path can only redirect; a threat warrant from a detector
    // organ escalates it to a kill (design §8).
    let mut e = eng();
    for _ in 0..6 {
        e.note_vibration("suspect", 1.0, Flavor::RhythmDeviation);
        e.tick();
    }
    e.note_verdict("suspect", Verdict::Threat);
    e.note_vibration("suspect", 100.0, Flavor::RhythmDeviation);
    e.tick(); // first contradiction: uncorroborated -> abstain
    e.note_vibration("suspect", 100.0, Flavor::RhythmDeviation);
    let o = e.tick();
    assert_eq!(count_alerts(&o), 1);
    match o.emitted[0].control() {
        Some(ControlMsg::Quarantine { name, .. }) => assert_eq!(name, "suspect"),
        other => panic!("a threat warrant must escalate to Quarantine, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// REVISE: a recovered node dissolves the threat belief (dissolve-before-replace).
// ---------------------------------------------------------------------------

#[test]
fn recovery_dissolves_the_threat_and_one_alert_per_episode() {
    let mut e = eng();
    for _ in 0..6 {
        e.note_vibration("flaky", 1.0, Flavor::RhythmDeviation);
        e.tick();
    }
    // Episode 1: two deviations trip one alert, and further deviations do NOT
    // re-emit (change-only).
    let mut episode1_alerts = 0;
    for _ in 0..6 {
        e.note_vibration("flaky", 100.0, Flavor::RhythmDeviation);
        episode1_alerts += count_alerts(&e.tick());
    }
    assert_eq!(episode1_alerts, 1, "one alert per episode, change-only");

    // Recovery: the mean is still 1.0, so a return to 1.0 is a match -> REVISE
    // dissolves the threat belief.
    for _ in 0..3 {
        e.note_vibration("flaky", 1.0, Flavor::RhythmDeviation);
        let o = e.tick();
        assert!(
            o.emitted.is_empty(),
            "a recovered node must not keep alerting"
        );
    }

    // Episode 2 after recovery: a fresh deviation pair trips again.
    let mut episode2_alerts = 0;
    for _ in 0..4 {
        e.note_vibration("flaky", 100.0, Flavor::RhythmDeviation);
        episode2_alerts += count_alerts(&e.tick());
    }
    assert_eq!(
        episode2_alerts, 1,
        "a new episode after recovery yields exactly one new alert"
    );
}

// ---------------------------------------------------------------------------
// CONSERVE: the alert budget bounds emissions under a flood.
// ---------------------------------------------------------------------------

#[test]
fn conserve_bounds_alerts_under_a_flood() {
    let budget = 4usize;
    let mut e = CognitiveEngine::new(CognitiveConfig {
        enabled: true,
        alert_budget: budget,
        alert_window: 20,
        ..Default::default()
    })
    .unwrap();

    let nodes: Vec<String> = (0..20).map(|i| format!("n{i:02}")).collect();

    // Warmup: every node steady in the same tick.
    for _ in 0..6 {
        for n in &nodes {
            e.note_vibration(n, 1.0, Flavor::RhythmDeviation);
        }
        e.tick();
    }

    // Tick A: everyone deviates once -> each is a single sample -> no alerts.
    for (i, n) in nodes.iter().enumerate() {
        e.note_vibration(n, (i + 2) as f64, Flavor::RhythmDeviation);
    }
    let a = e.tick();
    assert_eq!(count_alerts(&a), 0, "single samples must not flood the web");

    // Tick B: everyone deviates again -> 20 corroborated candidates, one tick.
    for (i, n) in nodes.iter().enumerate() {
        e.note_vibration(n, (i + 2) as f64, Flavor::RhythmDeviation);
    }
    let b = e.tick();
    assert_eq!(
        count_alerts(&b),
        budget,
        "CONSERVE must bound a same-tick flood to the budget"
    );
    assert!(
        has_abstain(&b, AbstainReason::BudgetStarved),
        "the dropped candidates must be recorded as BudgetStarved"
    );

    // Weakest-first: the admitted set is the highest-salience nodes. Salience is
    // (1 + magnitude) × surprise_boost, so the largest magnitudes win.
    let mut admitted: Vec<String> = b
        .emitted
        .iter()
        .filter_map(|d| d.node().map(str::to_string))
        .collect();
    admitted.sort();
    assert_eq!(
        admitted,
        vec!["n16", "n17", "n18", "n19"],
        "weakest-first admission must keep the strongest candidates"
    );

    // Tick C, still inside the window: the budget is exhausted -> nothing emitted.
    for (i, n) in nodes.iter().enumerate() {
        e.note_vibration(n, (i + 2) as f64, Flavor::RhythmDeviation);
    }
    let c = e.tick();
    assert_eq!(
        count_alerts(&c),
        0,
        "within the window the budget must stay exhausted"
    );
    assert!(has_abstain(&c, AbstainReason::BudgetStarved));
}

// ---------------------------------------------------------------------------
// The stuck-but-alive node (heartbeat staleness) — no crash required.
// ---------------------------------------------------------------------------

#[test]
fn a_stalled_heartbeat_trips_the_alarm() {
    let mut e = CognitiveEngine::new(CognitiveConfig {
        enabled: true,
        heartbeat_stall_ticks: 2,
        ..Default::default()
    })
    .unwrap();

    // The node makes progress for a while, establishing a liveness baseline.
    for p in 0..10u64 {
        e.note_heartbeat("worker", p);
        let o = e.tick();
        assert!(o.emitted.is_empty());
    }

    // Then it stops advancing while staying alive.
    let mut alerts = 0;
    for _ in 0..6 {
        e.note_heartbeat("worker", 9); // frozen counter
        alerts += count_alerts(&e.tick());
    }
    assert_eq!(alerts, 1, "a stuck-but-alive node must be detected exactly once");
}

#[test]
fn an_idle_node_with_no_baseline_is_never_called_stuck() {
    // A strand that never advanced (no inputs) has no liveness baseline; idle
    // is not the same as stuck, so it must never trip.
    let mut e = CognitiveEngine::new(CognitiveConfig {
        enabled: true,
        heartbeat_stall_ticks: 2,
        ..Default::default()
    })
    .unwrap();
    for _ in 0..40 {
        e.note_heartbeat("idle", 0);
        let o = e.tick();
        assert!(o.emitted.is_empty(), "an idle node is not an intruder");
    }
}

// ---------------------------------------------------------------------------
// D42 / config discipline.
// ---------------------------------------------------------------------------

#[test]
fn cognition_is_off_by_default() {
    assert!(!spider::Spider::new().cognition_enabled());
    assert!(!spider::Spider::default().cognition_enabled());
    // An explicit config with `enabled: false` is also off.
    let off = spider::Spider::with_cognition(CognitiveConfig::default());
    assert!(!off.cognition_enabled());
    // And enabling it turns it on.
    let on = spider::Spider::with_cognition(cfg());
    assert!(on.cognition_enabled());
}

#[test]
fn invalid_config_is_rejected_and_degrades_to_off() {
    let bad = CognitiveConfig {
        enabled: true,
        warmup_events: 1,
        ..Default::default()
    };
    assert!(
        CognitiveEngine::new(bad.clone()).is_err(),
        "warmup_events < 2 must fail validation"
    );
    // Try is fail-closed...
    assert!(spider::Spider::try_with_cognition(bad.clone()).is_err());
    // ...while the infallible constructor degrades to cognition-off.
    assert!(!spider::Spider::with_cognition(bad).cognition_enabled());
}

#[test]
fn o10_weaken_harsher_than_strengthen_is_enforced() {
    let bad = CognitiveConfig {
        enabled: true,
        contradiction_factor: 0.95, // (1 - 0.95) = 0.05 <= reinforce_support 0.12
        ..Default::default()
    };
    assert_eq!(
        CognitiveEngine::new(bad).unwrap_err(),
        spider::ConfigError::WeakAsymmetry
    );
}

// ---------------------------------------------------------------------------
// The query cycle is read-only (Stage-9 law) and the engine is deterministic.
// ---------------------------------------------------------------------------

#[test]
fn queries_leave_state_byte_identical() {
    let mut e = eng();
    for _ in 0..8 {
        e.note_vibration("n", 1.0, Flavor::RhythmDeviation);
        e.tick();
    }
    let before = e.state_digest();
    for _ in 0..25 {
        let _ = e.assess("n");
        let _ = e.assess("does-not-exist");
    }
    assert_eq!(
        e.state_digest(),
        before,
        "assess() must not mutate the rhythm model"
    );
}

#[test]
fn identical_input_replays_to_identical_state() {
    let mut a = eng();
    let mut b = eng();
    for tick in 0..20u64 {
        let mag = if tick % 4 == 3 { 50.0 } else { 1.0 };
        a.note_vibration("n", mag, Flavor::RhythmDeviation);
        b.note_vibration("n", mag, Flavor::RhythmDeviation);
        let oa = a.tick();
        let ob = b.tick();
        assert_eq!(dump(&oa), dump(&ob));
    }
    assert_eq!(a.state_digest(), b.state_digest());
}

fn dump(o: &spider::TickOutcome) -> Vec<String> {
    let mut v: Vec<String> = o.emitted.iter().map(|d| format!("{d:?}")).collect();
    let mut ab: Vec<String> = o.abstained.iter().map(|d| format!("{d:?}")).collect();
    v.append(&mut ab);
    v
}