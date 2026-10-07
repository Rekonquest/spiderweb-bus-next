//! Integration tests for the spider-driven crash-recovery cycle.
//!
//! These live in `tests/` (not the kernel's `mod tests`) because
//! they pull in the `spider` crate as a dep, and the spider has
//! its own `spiderweb` dependency. Keeping them in a separate
//! integration test file avoids pulling spider into the kernel
//! lib's unit-test build and avoids trait-version mismatches.
//!
//! Each test sets up a Bus with a small custom strand, registers
//! the spider, and asserts on lifecycle behavior.

use spider::Spider;
use spiderweb::{
    Bus, BusHandle, ControlMsg, LifecycleEvent, Socket, Strand, StrandError, StrandSpec,
    SYS_CONTROL, SYS_LIFECYCLE,
};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
struct CrashCounter {
    ticks: Arc<AtomicU32>,
    failures_left: Arc<AtomicU32>,
}

struct CrashThenOk(CrashCounter);

impl Strand for CrashThenOk {
    fn name(&self) -> &str {
        "crash-then-ok"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        self.0.ticks.fetch_add(1, Ordering::SeqCst);
        let left = self.0.failures_left.load(Ordering::SeqCst);
        if left > 0 {
            self.0.failures_left.fetch_sub(1, Ordering::SeqCst);
            bus.log(&format!("intentional crash ({} left)", left - 1));
            return Err(StrandError::Crashed(format!(
                "intentional ({} left)",
                left - 1
            )));
        }
        bus.log("ok, no more failures");
        bus.sleep(Duration::from_millis(100));
        Ok(())
    }
}

/// A strand that crashes N times, then succeeds. The spider should
/// restart it N times. The total ticks across all instances should
/// be N + 1 (N crashes + 1 success).
#[test]
fn spider_restarts_then_succeeds() {
    let ticks = Arc::new(AtomicU32::new(0));
    let failures_left = Arc::new(AtomicU32::new(2));
    let counter = CrashCounter {
        ticks: ticks.clone(),
        failures_left: failures_left.clone(),
    };
    let counter_for_factory = counter.clone();

    let mut bus = Bus::open();
    bus.register_spec(StrandSpec::new("crash-then-ok", move || {
        Box::new(CrashThenOk(counter_for_factory.clone()))
    }));
    bus.register_spider(StrandSpec::new("spider", || Box::new(Spider::new())));
    bus.run_until(Some(Duration::from_millis(500)));

    let final_ticks = ticks.load(Ordering::SeqCst);
    assert!(
        final_ticks >= 3,
        "expected at least 3 ticks (2 crashes + 1 ok), got {}",
        final_ticks
    );
}

struct AlwaysCrash {
    ticks: Arc<AtomicU32>,
}

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
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        self.ticks.fetch_add(1, Ordering::SeqCst);
        bus.log("always crash");
        Err(StrandError::Crashed("always".into()))
    }
}

/// A strand that always crashes should be restarted 3 times by
/// the spider and then quarantined on the 4th crash. Total ticks
/// should be >= 4.
#[test]
fn spider_quarantines_after_max_crashes() {
    let ticks = Arc::new(AtomicU32::new(0));
    let ticks_factory = ticks.clone();

    let mut bus = Bus::open();
    bus.register_spec(StrandSpec::new("always-crash", move || {
        Box::new(AlwaysCrash {
            ticks: ticks_factory.clone(),
        })
    }));
    bus.register_spider(StrandSpec::new("spider", || Box::new(Spider::new())));
    bus.run_until(Some(Duration::from_millis(800)));

    let final_ticks = ticks.load(Ordering::SeqCst);
    assert!(
        final_ticks >= 4,
        "expected at least 4 ticks (4 crashes -> quarantine), got {}",
        final_ticks
    );
}

/// With NO spider registered, a crashed strand stays dead — the kernel
/// never auto-restarts it. Restart policy lives entirely in the spider
/// (the kernel is a dumb primitive; the v1.2 in-kernel fallback was
/// removed in v1.3).
#[test]
fn no_spider_means_no_auto_restart() {
    let ticks = Arc::new(AtomicU32::new(0));
    let ticks_factory = ticks.clone();

    let mut bus = Bus::open();
    bus.register_spec(StrandSpec::new("always-crash", move || {
        Box::new(AlwaysCrash {
            ticks: ticks_factory.clone(),
        })
    }));
    // Deliberately NO register_spider.
    bus.run_until(Some(Duration::from_millis(400)));

    assert_eq!(
        ticks.load(Ordering::SeqCst),
        1,
        "with no spider, the crashed strand should run exactly once and stay dead"
    );
}

/// `Bus::run()` is run-forever (`run_until(None)`): it returns only when
/// every strand is terminally done. A crashed strand with no spider to
/// revive it IS terminal, so `run()` must wind down rather than hang. The
/// kernel counts the no-spider crash toward shutdown; without that
/// accounting this would loop forever. We run it on a worker thread and
/// require completion within a generous window so a regression surfaces
/// as a failed assertion, not a hung test suite.
#[test]
fn run_winds_down_without_spider_after_crash() {
    use std::sync::mpsc;

    let ticks = Arc::new(AtomicU32::new(0));
    let ticks_factory = ticks.clone();
    let (done_tx, done_rx) = mpsc::channel();

    let worker = std::thread::spawn(move || {
        let mut bus = Bus::open();
        bus.register_spec(StrandSpec::new("always-crash", move || {
            Box::new(AlwaysCrash {
                ticks: ticks_factory.clone(),
            })
        }));
        // No spider, and run-forever: the only way out is terminal
        // accounting of the crash.
        bus.run();
        let _ = done_tx.send(());
    });

    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("Bus::run() should wind down after the lone strand crashes (no spider)");
    worker.join().unwrap();

    assert_eq!(
        ticks.load(Ordering::SeqCst),
        1,
        "the strand should crash exactly once before the web winds down"
    );
}

struct LiveCrasher {
    count: Arc<AtomicU32>,
}

impl Strand for LiveCrasher {
    fn name(&self) -> &str {
        "live-crasher"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, _bus: &mut BusHandle) -> Result<(), StrandError> {
        self.count.fetch_add(1, Ordering::SeqCst);
        Err(StrandError::Crashed("live strand crash".into()))
    }
}

/// A live (non-restartable) strand registered via `Bus::register`
/// should crash exactly once. The kernel marks the slot as dead
/// (no factory = no auto-restart). The spider sees the crash event
/// and can quarantine it.
#[test]
fn live_strand_crash_is_not_restarted() {
    let count = Arc::new(AtomicU32::new(0));
    let counter = count.clone();

    let mut bus = Bus::open();
    bus.register(LiveCrasher { count: counter });
    bus.register_spider(StrandSpec::new("spider", || Box::new(Spider::new())));
    bus.run_until(Some(Duration::from_millis(300)));

    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "live strand should crash exactly once, not be restarted"
    );
}

struct LiveQuiet;

impl Strand for LiveQuiet {
    fn name(&self) -> &str {
        "live-quiet"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, _bus: &mut BusHandle) -> Result<(), StrandError> {
        Ok(())
    }
}

/// A live strand that runs cleanly should not crash and should
/// not be restarted. The bus should shut down cleanly when the
/// max_duration elapses.
#[test]
fn live_strand_runs_normally() {
    let mut bus = Bus::open();
    bus.register(LiveQuiet);
    bus.register_spider(StrandSpec::new("spider", || Box::new(Spider::new())));
    bus.run_until(Some(Duration::from_millis(100)));
    // No assertion on counts — we just want to verify this
    // doesn't panic or hang. If we get here, the bus exited
    // cleanly.
}

/// `Bus::run()` must wind down when a crashed strand has NO spider AND no
/// factory (a live strand registered via `Bus::register`). This is a
/// distinct branch of the Crashed arm from the spec-strand case: a live
/// strand can never be revived by anyone, so the no-spider crash is
/// terminal and must be counted toward shutdown. Without that, `run()`
/// would hang on a single live-strand crash.
#[test]
fn run_winds_down_without_spider_when_live_strand_crashes() {
    use std::sync::mpsc;

    let count = Arc::new(AtomicU32::new(0));
    let counter = count.clone();
    let (done_tx, done_rx) = mpsc::channel();

    let worker = std::thread::spawn(move || {
        let mut bus = Bus::open();
        bus.register(LiveCrasher { count: counter });
        // No spider, live strand: the crash is terminal.
        bus.run();
        let _ = done_tx.send(());
    });

    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("Bus::run() should wind down after the lone live strand crashes (no spider)");
    worker.join().unwrap();

    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "the live strand should crash exactly once before the web winds down"
    );
}

/// A minimal test orchestrator: it quarantines the first strand it sees
/// crash, then detaches (clean exit). Registered as the spider so a
/// crashed strand is PARKED (not counted at crash time) — the only thing
/// that can then retire the parked victim is the `done_count` increment
/// in the kernel's Quarantine arm. If that accounting regresses, the run
/// loop can never reach `done_count >= total_slots` and `Bus::run()`
/// hangs. This is the quarantine-path analogue of the crash-path
/// wind-down test.
struct QuarantineThenDetach;

impl Strand for QuarantineThenDetach {
    fn name(&self) -> &str {
        "quarantiner"
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<LifecycleEvent>(SYS_LIFECYCLE);
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<ControlMsg>(SYS_CONTROL);
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for (sock, evt) in bus.recv_all::<LifecycleEvent>()? {
            if sock != SYS_LIFECYCLE {
                continue;
            }
            if let LifecycleEvent::Crashed { name, reason } = evt {
                bus.publish_static(
                    SYS_CONTROL,
                    ControlMsg::Quarantine {
                        name: name.clone(),
                        reason: format!("test-quarantine: {}", reason),
                    },
                )?;
                // Work done — exit cleanly. The only slot left to retire
                // is the parked victim, via the Quarantine accounting.
                return Err(StrandError::Detach);
            }
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

#[test]
fn quarantine_winds_down_run_via_accounting() {
    use std::sync::mpsc;

    let ticks = Arc::new(AtomicU32::new(0));
    let ticks_factory = ticks.clone();
    let (done_tx, done_rx) = mpsc::channel();

    let worker = std::thread::spawn(move || {
        let mut bus = Bus::open();
        bus.register_spec(StrandSpec::new("always-crash", move || {
            Box::new(AlwaysCrash {
                ticks: ticks_factory.clone(),
            })
        }));
        bus.register_spider(StrandSpec::new("quarantiner", || {
            Box::new(QuarantineThenDetach)
        }));
        bus.run();
        let _ = done_tx.send(());
    });

    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("Bus::run() should wind down once the parked strand is quarantined and the quarantiner detaches");
    worker.join().unwrap();

    assert_eq!(
        ticks.load(Ordering::SeqCst),
        1,
        "the victim crashes once, is parked, then quarantined — never restarted"
    );
}

/// A strand that publishes `ControlMsg::Quarantine` for a name that is
/// not in the slot table. Used to prove a stale/duplicate Quarantine
/// does NOT advance shutdown accounting.
struct GhostQuarantiner {
    sent: bool,
}

impl Strand for GhostQuarantiner {
    fn name(&self) -> &str {
        "ghost-quarantiner"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<ControlMsg>(SYS_CONTROL);
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        if !self.sent {
            for _ in 0..8 {
                bus.publish_static(
                    SYS_CONTROL,
                    ControlMsg::Quarantine {
                        name: "ghost-never-existed".to_string(),
                        reason: "stale".to_string(),
                    },
                )?;
            }
            self.sent = true;
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

struct Heartbeat {
    beats: Arc<AtomicU32>,
}

impl Strand for Heartbeat {
    fn name(&self) -> &str {
        "heartbeat"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        self.beats.fetch_add(1, Ordering::SeqCst);
        bus.sleep(Duration::from_millis(10));
        Ok(())
    }
}

/// A Quarantine for a name that is not in the slot table must NOT advance
/// shutdown accounting. Otherwise a stale/duplicate Quarantine inflates
/// `done_count` and can trip `done_count >= total_slots`, shutting the
/// bus down while live strands are still running. Here 8 ghost
/// quarantines would (unguarded) push `done_count` past `total_slots`
/// almost instantly; with the guard the heartbeat keeps ticking until
/// max_duration.
#[test]
fn stale_quarantine_does_not_shut_down_the_bus() {
    let beats = Arc::new(AtomicU32::new(0));

    let mut bus = Bus::open();
    bus.register(Heartbeat {
        beats: beats.clone(),
    });
    bus.register_spider(StrandSpec::new("ghost-quarantiner", || {
        Box::new(GhostQuarantiner { sent: false })
    }));
    bus.run_until(Some(Duration::from_millis(300)));

    let n = beats.load(Ordering::SeqCst);
    assert!(
        n >= 5,
        "heartbeat should keep ticking (got {} beats); a stale quarantine must not shut the bus down",
        n
    );
}

// ---------------------------------------------------------------------------
// Panic isolation.
//
// A strand whose `run()` PANICS (not an explicit `Err(Crashed)`, but a real
// `panic!` / unwrap / index-out-of-bounds) must be caught by the worker and
// converted into a `Crashed` lifecycle event — identical treatment to an
// explicit crash. The alternative the worker is guarding against: the panic
// unwinds past `run()`, silently kills the worker thread, skips the Crashed
// event (so the spider never gets its restart/quarantine decision), and under
// `Bus::run()` hangs the whole bus forever because the dead slot never counts
// toward `done_count`.
//
// (These tests rely on unwinding panics — the default. Under `panic = "abort"`
// the catch is impossible by design; that is documented for deployers.)
//
// NOTE: each caught panic still prints its message + (if enabled) a backtrace
// to stderr via the default panic hook. That stderr noise during these three
// tests is EXPECTED, not a failure.
// ---------------------------------------------------------------------------

struct AlwaysPanic {
    ticks: Arc<AtomicU32>,
}

impl Strand for AlwaysPanic {
    fn name(&self) -> &str {
        "always-panic"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, _bus: &mut BusHandle) -> Result<(), StrandError> {
        self.ticks.fetch_add(1, Ordering::SeqCst);
        panic!("intentional panic inside a strand");
    }
}

/// A strand that PANICS every tick must be treated exactly like one that
/// returns `Err(Crashed)`: the spider restarts it up to its limit, then
/// quarantines it. This is the panic-path analogue of
/// `spider_quarantines_after_max_crashes`. Total ticks should be >= 4
/// (3 restarts + the quarantining crash). If the panic instead killed the
/// worker silently, the spider would see exactly one crash and no more — the
/// count would stall at 1 and the restart/quarantine cycle would never run.
#[test]
fn panicking_strand_is_quarantined_like_a_crash() {
    let ticks = Arc::new(AtomicU32::new(0));
    let ticks_factory = ticks.clone();

    let mut bus = Bus::open();
    bus.register_spec(StrandSpec::new("always-panic", move || {
        Box::new(AlwaysPanic {
            ticks: ticks_factory.clone(),
        })
    }));
    bus.register_spider(StrandSpec::new("spider", || Box::new(Spider::new())));
    bus.run_until(Some(Duration::from_millis(800)));

    let final_ticks = ticks.load(Ordering::SeqCst);
    assert!(
        final_ticks >= 4,
        "a panicking strand should be restarted then quarantined like a crash \
         (expected >= 4 ticks, got {}); a stall at 1 means the panic killed the \
         worker silently instead of becoming a Crashed event",
        final_ticks
    );
}

/// `Bus::run()` (run-forever) must wind down after a lone strand PANICS with no
/// spider to revive it — the panic is terminal accounting, just like a crash.
/// The danger this guards: a panic that escapes `run()` kills the worker
/// without ever incrementing `done_count`, so `done_count >= total_slots` is
/// never reached and `run()` hangs forever. We run on a worker thread with a
/// generous timeout so a regression surfaces as a failed assertion, not a hung
/// suite.
#[test]
fn run_winds_down_after_a_panic_with_no_spider() {
    use std::sync::mpsc;

    let ticks = Arc::new(AtomicU32::new(0));
    let ticks_factory = ticks.clone();
    let (done_tx, done_rx) = mpsc::channel();

    let worker = std::thread::spawn(move || {
        let mut bus = Bus::open();
        // Live strand (no factory) + no spider: the panic-as-crash is terminal.
        bus.register(AlwaysPanic {
            ticks: ticks_factory,
        });
        bus.run();
        let _ = done_tx.send(());
    });

    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("Bus::run() should wind down after the lone strand panics (no spider)");
    worker.join().unwrap();

    assert_eq!(
        ticks.load(Ordering::SeqCst),
        1,
        "the strand should panic exactly once before the web winds down"
    );
}

/// Panic isolation is per-worker: a strand that panics on every tick must NOT
/// take down a healthy strand sitting next to it. The spider keeps restarting
/// the panicker (3x then quarantine) while the heartbeat ticks on undisturbed.
/// If a panic could escape its worker and poison shared kernel state, the
/// heartbeat would stop; we require it to keep beating.
#[test]
fn a_panicking_strand_does_not_kill_its_neighbor() {
    let beats = Arc::new(AtomicU32::new(0));
    let panic_ticks = Arc::new(AtomicU32::new(0));
    let panic_factory = panic_ticks.clone();

    let mut bus = Bus::open();
    bus.register(Heartbeat {
        beats: beats.clone(),
    });
    bus.register_spec(StrandSpec::new("always-panic", move || {
        Box::new(AlwaysPanic {
            ticks: panic_factory.clone(),
        })
    }));
    bus.register_spider(StrandSpec::new("spider", || Box::new(Spider::new())));
    bus.run_until(Some(Duration::from_millis(300)));

    let n = beats.load(Ordering::SeqCst);
    assert!(
        n >= 5,
        "the heartbeat should keep ticking (got {} beats) while the neighbor \
         panics repeatedly — a panic must stay isolated to its own worker",
        n
    );
    assert!(
        panic_ticks.load(Ordering::SeqCst) >= 1,
        "sanity: the panicking strand should have actually run and panicked"
    );
}

/// Watches the lifecycle bus and records the `reason` of every `Crashed`
/// event it sees, so a test can assert the panic message survived the
/// `catch_unwind` round-trip into the `Crashed` reason.
struct CrashReasonWatcher {
    reasons: Arc<std::sync::Mutex<Vec<String>>>,
}

impl Strand for CrashReasonWatcher {
    fn name(&self) -> &str {
        "crash-reason-watcher"
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<LifecycleEvent>(SYS_LIFECYCLE);
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for (sock, evt) in bus.recv_all::<LifecycleEvent>()? {
            if sock != SYS_LIFECYCLE {
                continue;
            }
            if let LifecycleEvent::Crashed { reason, .. } = evt {
                self.reasons.lock().unwrap().push(reason);
            }
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

/// The panic message must survive into the `Crashed` reason — otherwise a
/// quarantined strand's forensic trail says only "panicked" and tells an
/// operator nothing. `AlwaysPanic` panics with a known literal; the recovered
/// reason should carry it (prefixed with "panicked:").
#[test]
fn panic_message_is_recovered_into_the_crash_reason() {
    let reasons = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let ticks = Arc::new(AtomicU32::new(0));

    let mut bus = Bus::open();
    // Live panicker (no factory) + no spider: it crashes exactly once, which
    // is enough to emit one Crashed lifecycle event for the watcher to catch.
    bus.register(AlwaysPanic {
        ticks: ticks.clone(),
    });
    bus.register(CrashReasonWatcher {
        reasons: reasons.clone(),
    });
    bus.run_until(Some(Duration::from_millis(300)));

    let captured = reasons.lock().unwrap().clone();
    assert!(
        captured
            .iter()
            .any(|r| r.contains("intentional panic inside a strand")),
        "the Crashed reason should carry the panic message; got {:?}",
        captured
    );
    assert!(
        captured.iter().any(|r| r.starts_with("panicked:")),
        "a panic-derived crash reason should be tagged 'panicked:'; got {:?}",
        captured
    );
}

// ---------------------------------------------------------------------------
// Graceful shutdown.
//
// When `run_until` winds down, the kernel must signal every worker to stop and
// JOIN it, so each strand is actually DROPPED (its `Drop` runs). Without this,
// a clean, still-running strand spins forever after `run_until` returns and is
// never dropped — leaking its thread and any resource its `Drop` would release
// (sockets, child reader threads, bound ports). A strand's `Drop` running is
// the observable proxy for "the worker was joined".
// ---------------------------------------------------------------------------

struct DropMarker {
    dropped: Arc<AtomicBool>,
    ran: Arc<AtomicU32>,
}

impl Strand for DropMarker {
    fn name(&self) -> &str {
        "drop-marker"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        self.ran.fetch_add(1, Ordering::SeqCst);
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

impl Drop for DropMarker {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

/// A clean, never-crashing live strand must be dropped when `run_until` winds
/// down on max-duration. If the worker were left spinning (the pre-graceful-
/// shutdown behavior), `Drop` would never run and `dropped` would stay false.
#[test]
fn clean_strand_is_dropped_on_graceful_shutdown() {
    let dropped = Arc::new(AtomicBool::new(false));
    let ran = Arc::new(AtomicU32::new(0));

    let mut bus = Bus::open();
    bus.register(DropMarker {
        dropped: dropped.clone(),
        ran: ran.clone(),
    });
    bus.run_until(Some(Duration::from_millis(100)));

    assert!(
        ran.load(Ordering::SeqCst) >= 1,
        "sanity: the strand should have ticked at least once before shutdown"
    );
    assert!(
        dropped.load(Ordering::SeqCst),
        "a clean still-running strand must be dropped when run_until winds down \
         (worker signaled + joined); false means the worker leaked and Drop never ran"
    );
}

/// The same guarantee for a factory (spec) strand run under a spider: when the
/// run loop exits, the spider, the worker, and the strand are all joined and
/// dropped.
#[test]
fn spec_strand_is_dropped_on_graceful_shutdown() {
    let dropped = Arc::new(AtomicBool::new(false));
    let ran = Arc::new(AtomicU32::new(0));
    let dropped_f = dropped.clone();
    let ran_f = ran.clone();

    let mut bus = Bus::open();
    bus.register_spec(StrandSpec::new("drop-marker", move || {
        Box::new(DropMarker {
            dropped: dropped_f.clone(),
            ran: ran_f.clone(),
        })
    }));
    bus.register_spider(StrandSpec::new("spider", || Box::new(Spider::new())));
    bus.run_until(Some(Duration::from_millis(100)));

    assert!(
        dropped.load(Ordering::SeqCst),
        "a spec strand must be dropped when run_until winds down; false means its \
         worker was never joined"
    );
}

/// Publishes `ControlMsg::Shutdown` once after a short delay, then keeps
/// ticking. Used to exercise the Shutdown control path, which now `break 'run`s
/// into the join logic (it used to bare-`return`, skipping the drop/join).
struct ShutdownAfterOneTick {
    sent: bool,
}

impl Strand for ShutdownAfterOneTick {
    fn name(&self) -> &str {
        "shutter"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<ControlMsg>(SYS_CONTROL);
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        if !self.sent {
            bus.sleep(Duration::from_millis(20));
            bus.publish_static(SYS_CONTROL, ControlMsg::Shutdown)?;
            self.sent = true;
        }
        bus.sleep(Duration::from_millis(10));
        Ok(())
    }
}

/// A `ControlMsg::Shutdown` must wind the bus down through the graceful path:
/// exit the run loop AND drop the strands. We give a long max_duration window
/// so the only way the bus exits quickly is via the Shutdown message, and we
/// assert both prompt return and that the live strand was dropped.
#[test]
fn shutdown_message_winds_down_and_drops_strands() {
    let dropped = Arc::new(AtomicBool::new(false));
    let ran = Arc::new(AtomicU32::new(0));

    let mut bus = Bus::open();
    bus.register(DropMarker {
        dropped: dropped.clone(),
        ran: ran.clone(),
    });
    bus.register(ShutdownAfterOneTick { sent: false });

    let start = std::time::Instant::now();
    bus.run_until(Some(Duration::from_secs(10)));
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(5),
        "the bus should wind down promptly via ControlMsg::Shutdown, not wait out \
         the 10s window (took {:?})",
        elapsed
    );
    assert!(
        dropped.load(Ordering::SeqCst),
        "strands must be dropped after a ControlMsg::Shutdown wind-down"
    );
}

/// A spider that observes lifecycle events but never acts — never restarts,
/// never quarantines. A strand that crashes under it stays PARKED in the slot
/// map (its worker already returned).
struct PassiveSpider;

impl Strand for PassiveSpider {
    fn name(&self) -> &str {
        "passive-spider"
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<LifecycleEvent>(SYS_LIFECYCLE);
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        let _ = bus.recv_all::<LifecycleEvent>()?;
        bus.sleep(Duration::from_millis(10));
        Ok(())
    }
}

/// Shutdown must join a PARKED crashed slot without hanging. A parked slot's
/// worker already returned (it sent WorkerExit::Crashed), so its retained
/// JoinHandle joins instantly — but the slot is still in the map, so the
/// shutdown snapshot picks it up. We prove the join completes (no hang) and the
/// co-located live strand is still dropped.
#[test]
fn shutdown_joins_a_parked_crashed_slot_without_hanging() {
    let dropped = Arc::new(AtomicBool::new(false));
    let ran = Arc::new(AtomicU32::new(0));
    let crash_ticks = Arc::new(AtomicU32::new(0));
    let crash_factory = crash_ticks.clone();

    let mut bus = Bus::open();
    bus.register(DropMarker {
        dropped: dropped.clone(),
        ran: ran.clone(),
    });
    bus.register_spec(StrandSpec::new("always-crash", move || {
        Box::new(AlwaysCrash {
            ticks: crash_factory.clone(),
        })
    }));
    bus.register_spider(StrandSpec::new("passive-spider", || {
        Box::new(PassiveSpider)
    }));

    let start = std::time::Instant::now();
    bus.run_until(Some(Duration::from_millis(200)));
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(3),
        "shutdown must not hang on a parked crashed slot (took {:?})",
        elapsed
    );
    assert!(
        dropped.load(Ordering::SeqCst),
        "the live strand must still be dropped past the parked crashed slot"
    );
    assert!(
        crash_ticks.load(Ordering::SeqCst) >= 1,
        "sanity: the strand should have crashed at least once"
    );
}

/// A strand that does long-running work INSIDE `run()` but cooperates by
/// polling `is_shutting_down()` must exit promptly on shutdown and be joined
/// (its `Drop` runs) — no detach, no timeout. This is the intended way for a
/// blocking/long strand to honor graceful shutdown.
struct CooperativeWorker {
    dropped: Arc<AtomicBool>,
}

impl Strand for CooperativeWorker {
    fn name(&self) -> &str {
        "cooperative"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        // Ongoing work that would otherwise run past shutdown — but it checks
        // the flag and returns when the bus is winding down.
        while !bus.is_shutting_down() {
            bus.sleep(Duration::from_millis(5));
        }
        Ok(())
    }
}

impl Drop for CooperativeWorker {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[test]
fn cooperative_strand_exits_via_is_shutting_down() {
    let dropped = Arc::new(AtomicBool::new(false));

    let mut bus = Bus::open();
    bus.register(CooperativeWorker {
        dropped: dropped.clone(),
    });

    let start = std::time::Instant::now();
    bus.run_until(Some(Duration::from_millis(100)));
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(2),
        "a cooperative strand should exit promptly on shutdown (took {:?})",
        elapsed
    );
    assert!(
        dropped.load(Ordering::SeqCst),
        "a strand that polls is_shutting_down() and returns must be joined and dropped"
    );
}

/// A strand that WEDGES inside `run()` (blocks far past the join deadline,
/// ignoring `is_shutting_down()`) must NOT hang shutdown. The kernel's bounded
/// best-effort join detaches it after its deadline and lets `run_until` return.
/// This is the safety net behind the cooperative contract: a buggy strand
/// degrades to one leaked thread, never a frozen bus. (This test takes a few
/// seconds — it waits out the kernel's join deadline by design.)
struct WedgedWorker;

impl Strand for WedgedWorker {
    fn name(&self) -> &str {
        "wedged"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        // Deliberately ignores is_shutting_down() and blocks far longer than
        // the kernel's join deadline.
        bus.sleep(Duration::from_secs(60));
        Ok(())
    }
}

#[test]
fn wedged_strand_does_not_hang_shutdown() {
    use std::sync::mpsc;

    let (done_tx, done_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut bus = Bus::open();
        bus.register(WedgedWorker);
        bus.run_until(Some(Duration::from_millis(100)));
        let _ = done_tx.send(());
    });

    // The strand blocks 60s, but the kernel's bounded join must let run_until
    // return well before that. Generous outer timeout to avoid flakiness.
    done_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("run_until must return despite a strand wedged for 60s (bounded join)");
    worker.join().unwrap();
}
