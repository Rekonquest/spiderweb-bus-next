//! Backpressure / vibration end-to-end tests.
//!
//! A burst source overruns a slow, small-capacity highway. The strand
//! holds the overflow and emits `Vibration::Backpressure` so the web can
//! feel it — but never drops work: every item still completes. A roomy
//! highway, by contrast, stays silent.

use highway::HighwayStrand;
use spiderweb::{
    register_clone_type, Bus, BusHandle, Socket, Strand, StrandError, Vibration, SYS_VIBRATION,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone)]
struct Task(u64);

#[derive(Debug, Clone)]
struct Outcome(u64);

/// Publishes `n` tasks in one burst, then detaches.
struct Burst {
    n: u64,
    sent: bool,
}

impl Strand for Burst {
    fn name(&self) -> &str {
        "burst"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const SOCK: Socket = Socket::new::<Task>("in.task");
        std::slice::from_ref(&SOCK)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        if self.sent {
            return Err(StrandError::Detach);
        }
        for i in 0..self.n {
            bus.publish("in.task", Task(i))?;
        }
        self.sent = true;
        Ok(())
    }
}

/// Collects the actual outcome values, so we can assert nothing is
/// lost or corrupted — not just that the right count arrived.
struct Sink {
    got: Arc<Mutex<Vec<u64>>>,
}

impl Strand for Sink {
    fn name(&self) -> &str {
        "sink"
    }
    fn inputs(&self) -> &[Socket] {
        const SOCK: Socket = Socket::new::<Outcome>("out.done");
        std::slice::from_ref(&SOCK)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for (_sock, Outcome(v)) in bus.recv_all::<Outcome>()? {
            self.got.lock().unwrap().push(v);
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

/// Counts backpressure vibrations felt on the web.
struct Seismograph {
    felt: Arc<AtomicU32>,
}

impl Strand for Seismograph {
    fn name(&self) -> &str {
        "seismograph"
    }
    fn inputs(&self) -> &[Socket] {
        const SOCK: Socket = Socket::new::<Vibration>(SYS_VIBRATION);
        std::slice::from_ref(&SOCK)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for (sock, vib) in bus.recv_all::<Vibration>()? {
            if sock == SYS_VIBRATION {
                if let Vibration::Backpressure { .. } = vib {
                    self.felt.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

/// A burst that overruns a slow, small-capacity highway must produce
/// at least one backpressure vibration AND still complete every item.
#[test]
fn backpressure_fires_and_loses_nothing() {
    register_clone_type::<Task>();
    register_clone_type::<Outcome>();

    let got = Arc::new(Mutex::new(Vec::new()));
    let felt = Arc::new(AtomicU32::new(0));
    const N: u64 = 12;

    let mut bus = Bus::open();
    bus.register(Burst { n: N, sent: false });
    bus.register(HighwayStrand::<Task, Outcome>::with_capacity(
        "slow-highway",
        "in.task",
        "out.done",
        2, // lanes
        3, // capacity — far below the burst of 12
        |Task(n)| {
            std::thread::sleep(Duration::from_millis(30));
            Outcome(n)
        },
    ));
    bus.register(Sink { got: got.clone() });
    bus.register(Seismograph { felt: felt.clone() });
    bus.run_until(Some(Duration::from_millis(1500)));

    let mut outcomes = got.lock().unwrap().clone();
    outcomes.sort_unstable();
    let want: Vec<u64> = (0..N).collect();
    assert_eq!(
        outcomes, want,
        "every task must complete despite backpressure (no loss, no corruption)"
    );
    assert!(
        felt.load(Ordering::SeqCst) >= 1,
        "the web should have felt at least one backpressure vibration"
    );
}

/// A highway with ample capacity never congests, so the web stays
/// quiet — no backpressure vibrations.
#[test]
fn roomy_highway_is_silent() {
    register_clone_type::<Task>();
    register_clone_type::<Outcome>();

    let got = Arc::new(Mutex::new(Vec::new()));
    let felt = Arc::new(AtomicU32::new(0));
    const N: u64 = 6;

    let mut bus = Bus::open();
    bus.register(Burst { n: N, sent: false });
    bus.register(HighwayStrand::<Task, Outcome>::with_capacity(
        "roomy-highway",
        "in.task",
        "out.done",
        4,  // lanes
        16, // capacity — well above the burst of 6
        |Task(n)| {
            std::thread::sleep(Duration::from_millis(5));
            Outcome(n)
        },
    ));
    bus.register(Sink { got: got.clone() });
    bus.register(Seismograph { felt: felt.clone() });
    bus.run_until(Some(Duration::from_millis(800)));

    assert_eq!(got.lock().unwrap().len(), N as usize, "all tasks complete");
    assert_eq!(
        felt.load(Ordering::SeqCst),
        0,
        "an uncongested highway should emit no backpressure"
    );
}
