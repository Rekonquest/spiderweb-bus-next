//! Supervision of a strand that crashes as soon as it starts.
//!
//! This asserts the crash policy end to end: crash → restart ×3 → quarantine.
//!
//! **It is NOT a regression test for the install-order window**, and I want that
//! on the record because it looks like one. The failure mode is real — a
//! lifecycle event reaches only the subscribers registered at the instant it is
//! published, because `publish_lifecycle` goes through `deliver_internal`, which
//! (unlike the work-traffic path in `deliver`) does not append to the per-type
//! replay buffer. So a strand that crashes before the spider has *subscribed*
//! publishes `Crashed` to nobody, its slot is parked waiting for a `ControlMsg`
//! that never comes, and it is never restarted or quarantined.
//!
//! But the window is the gap between `install_slot` returning and the spider's
//! worker thread actually calling `subscribe` — a thread spawn plus a hashmap
//! insert, i.e. microseconds. Publishing the victim's spec before the spider's
//! does not reliably lose that race, so this file passed both with and without
//! the install-order hoist in `run_until`. It was observed failing under a full
//! parallel `cargo test --workspace` run (a 60-binary load widens the window),
//! which is where it was first diagnosed from.
//!
//! Closing it properly means buffering lifecycle events for late subscribers,
//! the way work traffic already is. That is a real behaviour change to shared
//! delivery code and it needs a deterministic reproduction first — one that does
//! not depend on thread timing — so it is deliberately NOT attempted here. Open
//! questions for that change, recorded rather than guessed at:
//!
//! - Does `subscribe`'s existing exactly-once guarantee (append + subscriber
//!   snapshot under both locks, in one critical section) extend cleanly to
//!   `publish_lifecycle`, or does the replay path need its own guard?
//! - `publish_control` shares the same `deliver_internal` path. Replaying stale
//!   `ControlMsg` to a late subscriber is probably wrong, so the buffering is
//!   likely lifecycle-only — which means `deliver_internal` grows a flag.
//! - A crash that happens before `register_spider` is *terminal* today: with no
//!   spider registered the kernel removes the slot instead of parking it, so
//!   buffering alone would not recover it. That is a separate, register-order
//!   question.
//!
//! This test documents the end-to-end crash policy that exists today.

use std::time::Duration;

use spiderweb::{Bus, BusHandle, Socket, Strand, StrandError, StrandSpec};

/// Crashes on every tick, so the spider must restart it until quarantine.
struct AlwaysCrashes;

impl Strand for AlwaysCrashes {
    fn name(&self) -> &str {
        "always-crashes"
    }

    fn inputs(&self) -> &[Socket] {
        &[]
    }

    fn outputs(&self) -> &[Socket] {
        &[]
    }

    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        // Shutdown-aware so the worker still exits promptly if the run loop
        // stops before quarantine is reached.
        if bus.is_shutting_down() {
            return Err(StrandError::Detach);
        }
        Err(StrandError::Crashed("crash-policy drill".into()))
    }
}

#[test]
fn a_strand_that_crashes_from_its_first_tick_is_restarted_three_times_then_quarantined() {
    let mut bus = Bus::open();
    bus.register_spec(StrandSpec::new("always-crashes", || {
        Box::new(AlwaysCrashes)
    }));
    bus.register_spider(StrandSpec::new("spider", || Box::new(spider::Spider::new())));

    // Enough time for four crash → restart cycles; the loop exits on the
    // deadline because a parked slot is never "done".
    bus.run_until(Some(Duration::from_secs(5)));

    let stats = bus.stats();
    assert_eq!(
        stats.crashed, 4,
        "every generation crashes: the first on install, then one per restart"
    );
    assert_eq!(
        stats.restarted, 3,
        "three restarts precede quarantine (crash 1..=3 restart, crash 4 quarantines)"
    );
    assert_eq!(stats.quarantined, 1, "the fourth crash quarantines the slot");
}

