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
use spiderweb::{Bus, BusHandle, Socket, Strand, StrandError, StrandSpec};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
struct CrashCounter {
    ticks: Arc<AtomicU32>,
    failures_left: Arc<AtomicU32>,
}

struct CrashThenOk(CrashCounter);

impl Strand for CrashThenOk {
    fn name(&self) -> &str { "crash-then-ok" }
    fn inputs(&self) -> &[Socket] { &[] }
    fn outputs(&self) -> &[Socket] { &[] }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        self.0.ticks.fetch_add(1, Ordering::SeqCst);
        let left = self.0.failures_left.load(Ordering::SeqCst);
        if left > 0 {
            self.0.failures_left.fetch_sub(1, Ordering::SeqCst);
            bus.log(&format!("intentional crash ({} left)", left - 1));
            return Err(StrandError::Crashed(format!("intentional ({} left)", left - 1)));
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
    let counter = CrashCounter { ticks: ticks.clone(), failures_left: failures_left.clone() };
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

struct AlwaysCrash { ticks: Arc<AtomicU32> }

impl Strand for AlwaysCrash {
    fn name(&self) -> &str { "always-crash" }
    fn inputs(&self) -> &[Socket] { &[] }
    fn outputs(&self) -> &[Socket] { &[] }
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
        Box::new(AlwaysCrash { ticks: ticks_factory.clone() })
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
        .recv_timeout(Duration::from_secs(5))
        .expect("Bus::run() should wind down after the lone strand crashes (no spider)");
    worker.join().unwrap();

    assert_eq!(
        ticks.load(Ordering::SeqCst),
        1,
        "the strand should crash exactly once before the web winds down"
    );
}

struct LiveCrasher { count: Arc<AtomicU32> }

impl Strand for LiveCrasher {
    fn name(&self) -> &str { "live-crasher" }
    fn inputs(&self) -> &[Socket] { &[] }
    fn outputs(&self) -> &[Socket] { &[] }
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
    fn name(&self) -> &str { "live-quiet" }
    fn inputs(&self) -> &[Socket] { &[] }
    fn outputs(&self) -> &[Socket] { &[] }
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
