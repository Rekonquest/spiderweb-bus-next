//! Congestion demo: watch the web *feel* backpressure and recover.
//!
//! Run with: `cargo run -p highway --example congestion`
//!
//! What happens:
//! 1. A dispatcher bursts 24 parcels onto the road at once.
//! 2. A `HighwayStrand` with only 2 lanes and capacity 4 can't take
//!    them all — it on-ramps what fits and holds the rest, emitting a
//!    `Vibration::Backpressure` whenever its backlog changes.
//! 3. The spider, sitting at the center of the web, feels every
//!    vibration and logs it (`🌊 backpressure ...`).
//! 4. As the 2 lanes chew through the work, the backlog shrinks, the
//!    vibrations subside, and every parcel still arrives — nothing is
//!    dropped.
//!
//! The point: congestion isn't silently queued forever. The web senses
//! it and the rest of the web can react.

use highway::HighwayStrand;
use spider::Spider;
use spiderweb::{register_clone_type, Bus, BusHandle, Socket, Strand, StrandError, StrandSpec};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

const PARCELS: u64 = 24;
const LANES: usize = 2;
const CAPACITY: usize = 4;
const WORK: Duration = Duration::from_millis(80);

#[derive(Debug, Clone)]
struct Parcel(u64);

#[derive(Debug, Clone)]
struct Delivered(u64);

/// Bursts every parcel onto the road in one tick, then detaches.
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
            "dispatching {PARCELS} parcels at a {LANES}-lane highway (capacity {CAPACITY})"
        ));
        for i in 0..PARCELS {
            bus.publish("road.parcel", Parcel(i))?;
        }
        self.sent = true;
        Ok(())
    }
}

/// Collects delivered parcels and announces when the whole batch is in.
struct Depot {
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
        let mut just_arrived = 0u32;
        let mut latest = None;
        for (_sock, Delivered(id)) in bus.recv_all::<Delivered>()? {
            just_arrived += 1;
            latest = Some(id);
        }
        if just_arrived > 0 {
            let total = self.count.fetch_add(just_arrived, Ordering::SeqCst) + just_arrived;
            if let Some(id) = latest {
                bus.log(&format!("← {total}/{PARCELS} delivered (latest #{id})"));
            }
        }
        if !self.announced && self.count.load(Ordering::SeqCst) >= PARCELS as u32 {
            self.announced = true;
            bus.log("✓ all parcels delivered — backpressure held the line, nothing dropped");
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
    bus.register(HighwayStrand::<Parcel, Delivered>::with_capacity(
        "delivery-highway",
        "road.parcel",
        "road.delivered",
        LANES,
        CAPACITY,
        |Parcel(n)| {
            std::thread::sleep(WORK);
            Delivered(n)
        },
    ));
    bus.register(Depot {
        count: count.clone(),
        announced: false,
    });
    // The spider feels the backpressure vibrations.
    bus.register_spider(StrandSpec::new("spider", || Box::new(Spider::new())));

    bus.run_until(Some(Duration::from_secs(3)));
}
