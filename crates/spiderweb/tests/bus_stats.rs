//! Integration tests for pull-able `BusStats` telemetry.
//!
//! These prove the countable-metrics replacement for `spider.log` strings:
//! kernel-wide delivery/eviction/vibration counters, lifecycle-by-kind counts,
//! per-strand throughput, and structural fabric metrics — pulled both after a
//! run (`Bus::stats`) and live from inside a strand (`BusHandle::stats`).

use spider::Spider;
use spiderweb::{
    Bus, BusHandle, BusStats, Socket, Strand, StrandError, StrandSpec, Vibration, SYS_VIBRATION,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Emits `left` Strings on "note", then detaches.
struct Source {
    left: usize,
}
impl Strand for Source {
    fn name(&self) -> &str {
        "source"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<String>("note");
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        if self.left == 0 {
            return Err(StrandError::Detach);
        }
        bus.publish_static("note", format!("m{}", self.left))?;
        self.left -= 1;
        bus.sleep(Duration::from_millis(2));
        Ok(())
    }
}

/// Drains Strings from "note".
struct Sink {
    got: Arc<AtomicU32>,
}
impl Strand for Sink {
    fn name(&self) -> &str {
        "sink"
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<String>("note");
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for _ in bus.recv::<String>("note")? {
            self.got.fetch_add(1, Ordering::SeqCst);
        }
        bus.sleep(Duration::from_millis(2));
        Ok(())
    }
}

#[test]
fn stats_count_deliveries_registrations_and_per_strand_throughput() {
    let got = Arc::new(AtomicU32::new(0));

    let mut bus = Bus::open();
    bus.register(Source { left: 5 });
    bus.register(Sink { got: got.clone() });
    bus.run_until(Some(Duration::from_millis(300)));

    let stats = bus.stats();
    assert!(
        stats.delivered >= 5,
        "expected >= 5 physical deliveries, got {}",
        stats.delivered
    );
    assert!(
        stats.registered >= 2,
        "source + sink should both register, got {}",
        stats.registered
    );
    // The sink persists (live strand, joined at shutdown but not removed), so
    // its per-strand throughput is visible.
    let sink = stats
        .per_strand
        .iter()
        .find(|s| s.name == "sink")
        .expect("sink should appear in per_strand");
    assert!(
        sink.processed >= 5,
        "sink should have processed all 5 notes, got {}",
        sink.processed
    );
    // The work edge source->sink exists in the fabric.
    assert!(
        stats.nodes >= 2,
        "expected >= 2 fabric nodes, got {}",
        stats.nodes
    );
    assert!(
        stats.edges >= 1,
        "expected >= 1 fabric edge, got {}",
        stats.edges
    );
    assert!(
        stats.edge_traffic_total >= 5,
        "edge traffic should total >= 5, got {}",
        stats.edge_traffic_total
    );
    // The first message of a type always lands in the pending buffer, so the
    // empty->non-empty transition is counted at least once.
    assert!(
        stats.buffered >= 1,
        "the first 'note' should record a buffer transition, got {}",
        stats.buffered
    );
    // A thread forms for the source's parentless messages.
    assert!(
        stats.threads_formed >= 1,
        "a source->sink flow should form >= 1 thread, got {}",
        stats.threads_formed
    );
    // active_strands is exactly per_strand.len() by construction.
    assert_eq!(
        stats.active_strands,
        stats.per_strand.len(),
        "active_strands must equal per_strand.len()"
    );
}

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
        Err(StrandError::Crashed("boom".into()))
    }
}

#[test]
fn stats_count_crash_restart_and_quarantine_lifecycle() {
    let mut bus = Bus::open();
    bus.register_spec(StrandSpec::new("always-crash", || Box::new(AlwaysCrash)));
    bus.register_spider(StrandSpec::new("spider", || Box::new(Spider::new())));
    bus.run_until(Some(Duration::from_millis(800)));

    let stats = bus.stats();
    assert!(
        stats.crashed >= 1,
        "a crashing strand should register crashes, got {}",
        stats.crashed
    );
    assert!(
        stats.restarted >= 1,
        "the spider should restart it at least once, got {}",
        stats.restarted
    );
    assert!(
        stats.quarantined >= 1,
        "the spider should eventually quarantine it, got {}",
        stats.quarantined
    );
}

/// Publishes 40 Strings to a socket no one subscribes to, overflowing the
/// per-type pending buffer (cap 32) so evictions are counted.
struct Voider {
    left: usize,
}
impl Strand for Voider {
    fn name(&self) -> &str {
        "voider"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<String>("void");
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        if self.left == 0 {
            return Err(StrandError::Detach);
        }
        // Burst the whole flood in ONE tick so the 32-cap buffer overflows
        // regardless of tick timing.
        for i in 0..self.left {
            bus.publish_static("void", format!("v{}", i))?;
        }
        self.left = 0;
        Ok(())
    }
}

#[test]
fn stats_count_pending_evictions_when_no_subscriber() {
    let mut bus = Bus::open();
    bus.register(Voider { left: 40 });
    bus.run_until(Some(Duration::from_millis(300)));

    let stats = bus.stats();
    // 40 messages, buffer cap 32 → at least 8 evictions.
    assert!(
        stats.pending_evictions >= 1,
        "an unsubscribed flood should evict from the pending buffer, got {}",
        stats.pending_evictions
    );
}

/// Emits a vibration every tick, then detaches.
struct Buzzer {
    left: usize,
}
impl Strand for Buzzer {
    fn name(&self) -> &str {
        "buzzer"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<Vibration>(SYS_VIBRATION);
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        if self.left == 0 {
            return Err(StrandError::Detach);
        }
        bus.publish_static(
            SYS_VIBRATION,
            Vibration::Starved {
                node: "buzzer".into(),
                ready: 0,
                depth: 4,
            },
        )?;
        self.left -= 1;
        bus.sleep(Duration::from_millis(2));
        Ok(())
    }
}

#[test]
fn stats_count_vibrations() {
    let mut bus = Bus::open();
    bus.register(Buzzer { left: 3 });
    bus.run_until(Some(Duration::from_millis(300)));

    let stats = bus.stats();
    assert!(
        stats.vibrations >= 3,
        "3 emitted vibrations should be counted, got {}",
        stats.vibrations
    );
}

/// Pulls `BusStats` live from inside a strand (the `webctl top` / spider path)
/// and parks the latest snapshot into shared state for the test to inspect.
struct Reporter {
    latest: Arc<Mutex<Option<BusStats>>>,
}
impl Strand for Reporter {
    fn name(&self) -> &str {
        "reporter"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        *self.latest.lock().unwrap() = Some(bus.stats());
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

#[test]
fn stats_are_pullable_live_from_a_strand() {
    let latest: Arc<Mutex<Option<BusStats>>> = Arc::new(Mutex::new(None));
    let got = Arc::new(AtomicU32::new(0));

    let mut bus = Bus::open();
    bus.register(Source { left: 5 });
    bus.register(Sink { got });
    bus.register(Reporter {
        latest: latest.clone(),
    });
    bus.run_until(Some(Duration::from_millis(300)));

    let snap = latest
        .lock()
        .unwrap()
        .clone()
        .expect("reporter should have captured a live BusStats");
    assert!(
        snap.delivered >= 1,
        "a live BusStats pulled mid-run should see deliveries, got {}",
        snap.delivered
    );
    assert!(
        snap.active_strands >= 1,
        "live stats should see active strands, got {}",
        snap.active_strands
    );
}

/// A second source on the same socket, so two distinct thread lineages cross at
/// the shared sink and an intersection emerges.
struct SourceB {
    left: usize,
}
impl Strand for SourceB {
    fn name(&self) -> &str {
        "source-b"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<String>("note");
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        if self.left == 0 {
            return Err(StrandError::Detach);
        }
        bus.publish_static("note", format!("b{}", self.left))?;
        self.left -= 1;
        bus.sleep(Duration::from_millis(2));
        Ok(())
    }
}

#[test]
fn stats_count_threads_and_intersections() {
    let got = Arc::new(AtomicU32::new(0));

    let mut bus = Bus::open();
    bus.register(Source { left: 3 });
    bus.register(SourceB { left: 3 });
    bus.register(Sink { got });
    bus.run_until(Some(Duration::from_millis(300)));

    let stats = bus.stats();
    assert!(
        stats.threads_formed >= 2,
        "two sources should form at least two threads, got {}",
        stats.threads_formed
    );
    assert!(
        stats.intersections_emerged >= 1,
        "two thread lineages crossing at the sink should emerge an intersection, got {}",
        stats.intersections_emerged
    );
    assert!(
        stats.intersections >= 1,
        "the structural intersection count should be >= 1, got {}",
        stats.intersections
    );
}

#[test]
fn per_strand_drops_detached_keeps_live_after_shutdown() {
    let got = Arc::new(AtomicU32::new(0));

    let mut bus = Bus::open();
    bus.register(Source { left: 3 }); // detaches when drained
    bus.register(Sink { got }); // live — never detaches
    bus.run_until(Some(Duration::from_millis(300)));

    let stats = bus.stats();
    // The detached source's slot was removed; the live sink's slot remains.
    assert!(
        stats.per_strand.iter().all(|s| s.name != "source"),
        "a detached strand should be absent from per_strand, got {:?}",
        stats.per_strand
    );
    assert!(
        stats.per_strand.iter().any(|s| s.name == "sink"),
        "the live sink should still be present in per_strand, got {:?}",
        stats.per_strand
    );
    assert!(
        stats.detached >= 1,
        "the source detaching should be counted, got {}",
        stats.detached
    );
}
