//! The highway preserves provenance through its lanes.
//!
//! Each source emission is its own thread (lineage). The highway's lanes
//! run in parallel and finish out of submission order, and a result may
//! be demoted several ticks after its input arrived. The decisive
//! property: despite that reordering and the input-tick≠output-tick gap,
//! every item's thread must still trace the full path source → hw → sink
//! — the tag rides glued to its item, never collapsed onto a tick's
//! "current" thread.

use highway::HighwayStrand;
use spiderweb::{register_clone_type, Bus, BusHandle, Socket, Strand, StrandError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone)]
struct Work(u64);

#[derive(Clone)]
struct Done(u64);

/// Bursts N work items in one tick (so each is its own thread), then idles.
struct Burst {
    n: u64,
    sent: bool,
}

impl Strand for Burst {
    fn name(&self) -> &str {
        "source"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<Work>("in.work");
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        if !self.sent {
            for i in 0..self.n {
                bus.publish("in.work", Work(i))?;
            }
            self.sent = true;
        }
        bus.sleep(Duration::from_millis(10));
        Ok(())
    }
}

struct Sink {
    got: Arc<Mutex<Vec<u64>>>,
}

impl Strand for Sink {
    fn name(&self) -> &str {
        "sink"
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<Done>("out.done");
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for (_sock, Done(n)) in bus.recv_all::<Done>()? {
            self.got.lock().unwrap().push(n);
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

#[test]
fn highway_preserves_each_thread_through_lanes() {
    register_clone_type::<Work>();
    register_clone_type::<Done>();
    const N: u64 = 12;

    let got = Arc::new(Mutex::new(Vec::new()));

    let mut bus = Bus::open();
    bus.register(Sink { got: got.clone() });
    bus.register(HighwayStrand::<Work, Done>::new(
        "hw",
        "in.work",
        "out.done",
        4,
        |Work(n)| {
            // Vary the work so lanes finish out of submission order,
            // stressing the tag-rides-the-item property.
            std::thread::sleep(Duration::from_millis(10 + (n % 5) * 8));
            Done(n)
        },
    ));
    bus.register(Burst { n: N, sent: false });
    bus.run_until(Some(Duration::from_millis(1500)));

    // Every item also actually arrived at the sink (no loss).
    let mut values = got.lock().unwrap().clone();
    values.sort_unstable();
    assert_eq!(
        values,
        (0..N).collect::<Vec<_>>(),
        "all items should arrive"
    );

    let snap = bus.fabric_snapshot();
    let full_path = vec!["source".to_string(), "hw".to_string(), "sink".to_string()];
    let through = snap
        .threads
        .iter()
        .filter(|t| t.direction == full_path)
        .count();

    assert_eq!(
        snap.threads.len(),
        N as usize,
        "each source emission should be exactly one thread, no spurious ones; got {:#?}",
        snap.threads
    );
    assert_eq!(
        through, N as usize,
        "all {N} item-threads must trace source->hw->sink despite lane reordering; got {:#?}",
        snap.threads
    );
}
