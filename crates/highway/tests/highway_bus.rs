//! End-to-end test: ground bus → on-ramp → lanes → off-ramp → ground bus.
//!
//! A source strand publishes `Work` items. A `HighwayStrand` promotes
//! them to the parallel layer, squares each on a lane, and demotes the
//! `Done` results back to the bus. A sink collects them. We assert
//! every item made the round trip with the right value.

use highway::HighwayStrand;
use spiderweb::{register_clone_type, Bus, BusHandle, Socket, Strand, StrandError};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Promoted traffic. A distinct newtype so the kernel routes it by
/// type, separate from `Done`.
#[derive(Debug, Clone)]
struct Work(u64);

/// Demoted traffic.
#[derive(Debug, Clone)]
struct Done(u64);

/// Publishes `n` `Work` items in a single burst, then detaches — so
/// the whole batch hits the on-ramp at once and the lanes parallelize
/// it.
struct Burst {
    n: u64,
    sent: bool,
}

impl Strand for Burst {
    fn name(&self) -> &str {
        "burst-source"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const SOCK: Socket = Socket::new::<Work>("out.work");
        std::slice::from_ref(&SOCK)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        if self.sent {
            return Err(StrandError::Detach);
        }
        for i in 0..self.n {
            bus.publish("out.work", Work(i))?;
        }
        self.sent = true;
        Ok(())
    }
}

/// Collects every `Done` it sees into a shared vec.
struct Collect {
    seen: Arc<Mutex<Vec<u64>>>,
    count: Arc<AtomicU32>,
}

impl Strand for Collect {
    fn name(&self) -> &str {
        "collect-sink"
    }
    fn inputs(&self) -> &[Socket] {
        const SOCK: Socket = Socket::new::<Done>("out.done");
        std::slice::from_ref(&SOCK)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for (_sock, Done(v)) in bus.recv_all::<Done>()? {
            self.seen.lock().unwrap().push(v);
            self.count.fetch_add(1, Ordering::SeqCst);
        }
        bus.sleep(Duration::from_millis(10));
        Ok(())
    }
}

#[test]
fn work_round_trips_through_the_highway() {
    register_clone_type::<Work>();
    register_clone_type::<Done>();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let count = Arc::new(AtomicU32::new(0));

    let mut bus = Bus::open();
    bus.register(Burst { n: 20, sent: false });
    bus.register(HighwayStrand::<Work, Done>::new(
        "square-highway",
        "out.work",
        "out.done",
        4,
        |Work(n)| Done(n * n),
    ));
    bus.register(Collect {
        seen: seen.clone(),
        count: count.clone(),
    });
    bus.run_until(Some(Duration::from_millis(600)));

    let mut got = seen.lock().unwrap().clone();
    got.sort_unstable();
    let want: Vec<u64> = (0..20).map(|n| n * n).collect();
    assert_eq!(
        got, want,
        "all 20 work items should round-trip through the highway, squared"
    );
}
