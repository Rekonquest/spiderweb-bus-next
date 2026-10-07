//! Highway demo: watch a batch of work get promoted to the parallel
//! layer and finish far faster than it would on the ground.
//!
//! Run with: `cargo run -p highway --example highway`
//!
//! What happens:
//! 1. A source bursts 12 `Parcel`s onto the bus at once.
//! 2. A `HighwayStrand` with 6 lanes on-ramps every parcel, runs the
//!    (deliberately slow, 150ms) work across its lanes in parallel, and
//!    off-ramps each `Delivered` result back onto the bus.
//! 3. A sink collects them and reports the wall-clock.
//!
//! 12 parcels × 150ms is 1.8s of work. On the ground, serially, that is
//! what it would take. Across 6 lanes it finishes in about two rounds —
//! ~300ms. The printed summary shows the speedup.

use highway::HighwayStrand;
use spiderweb::{register_clone_type, Bus, BusHandle, Socket, Strand, StrandError};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const PARCELS: u64 = 12;
const LANES: usize = 6;
const WORK: Duration = Duration::from_millis(150);

/// A unit of work heading onto the highway.
#[derive(Debug, Clone)]
struct Parcel(u64);

/// A finished unit coming back off the highway.
#[derive(Debug, Clone)]
struct Delivered(u64);

/// Bursts every parcel onto the bus in one tick, then detaches.
struct Dispatcher {
    sent: bool,
}

impl Strand for Dispatcher {
    fn name(&self) -> &str {
        "dispatcher"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const SOCK: Socket = Socket::new::<Parcel>("road.parcel");
        std::slice::from_ref(&SOCK)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        if self.sent {
            return Err(StrandError::Detach);
        }
        bus.log(&format!(
            "dispatching {PARCELS} parcels onto the road (burst)"
        ));
        for i in 0..PARCELS {
            bus.publish("road.parcel", Parcel(i))?;
        }
        self.sent = true;
        Ok(())
    }
}

/// Collects delivered parcels and prints the wall-clock once the whole
/// batch is back.
struct Depot {
    start: Instant,
    count: Arc<AtomicU32>,
    announced: bool,
}

impl Strand for Depot {
    fn name(&self) -> &str {
        "depot"
    }
    fn inputs(&self) -> &[Socket] {
        const SOCK: Socket = Socket::new::<Delivered>("road.delivered");
        std::slice::from_ref(&SOCK)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for (_sock, Delivered(v)) in bus.recv_all::<Delivered>()? {
            let n = self.count.fetch_add(1, Ordering::SeqCst) + 1;
            bus.log(&format!("← delivered parcel {v} ({n}/{PARCELS})"));
        }
        if !self.announced && self.count.load(Ordering::SeqCst) >= PARCELS as u32 {
            self.announced = true;
            let elapsed = self.start.elapsed();
            let serial = WORK * PARCELS as u32;
            bus.log("──────────────────────────────────────────");
            bus.log(&format!("all {PARCELS} parcels delivered in {elapsed:?}"));
            bus.log(&format!(
                "serial would be ~{serial:?} ({PARCELS} × {WORK:?}); {LANES} lanes ≈ {:.1}× faster",
                serial.as_secs_f64() / elapsed.as_secs_f64()
            ));
            bus.log("──────────────────────────────────────────");
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

fn main() {
    register_clone_type::<Parcel>();
    register_clone_type::<Delivered>();

    let count = Arc::new(AtomicU32::new(0));

    let mut bus = Bus::open();
    bus.register(Dispatcher { sent: false });
    bus.register(HighwayStrand::<Parcel, Delivered>::new(
        "delivery-highway",
        "road.parcel",
        "road.delivered",
        LANES,
        |Parcel(n)| {
            // The slow work each lane runs, in parallel with the others.
            std::thread::sleep(WORK);
            Delivered(n)
        },
    ));
    bus.register(Depot {
        start: Instant::now(),
        count: count.clone(),
        announced: false,
    });

    // Generous bound: the batch should finish in well under a second.
    bus.run_until(Some(Duration::from_secs(3)));
}
