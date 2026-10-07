//! Spatial weave demo: the web in 3D.
//!
//! Run with: `cargo run -p spiderweb-viz --example spatial_weave`
//!
//! Places ground strands at z=0, the highway at z=1, and the spider at
//! z=2 — so the on-ramp/off-ramp "elevation" metaphor becomes a literal
//! coordinate delta. Drives the highway into backpressure, then measures
//! how strongly that vibration is FELT by a near observer vs a far one
//! (the physics spreading-activation primitive: it decays with distance).
//! Finally exports the geometric fabric as a 3D scene.
//!
//! HONEST: these are COORDINATES, a DISTANCE, and an ATTENUATION FACTOR.
//! Every node is an mpsc channel in the same RAM — none of this changes
//! timing. Positions are a logical overlay for visualization, organization,
//! and spider policy; they gain physical (latency) meaning only with edge
//! transport in v2.

use highway::HighwayStrand;
use spider::Spider;
use spiderweb::{
    attenuation, register_clone_type, Bus, BusHandle, Position, Socket, Strand, StrandError,
    StrandSpec,
};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
struct Parcel(u64);
#[derive(Clone)]
struct Delivered(u64);

struct Dispatcher {
    name: &'static str,
    base: u64,
    sent: bool,
}
impl Strand for Dispatcher {
    fn name(&self) -> &str {
        self.name
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<Parcel>("road.parcel");
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        if !self.sent {
            for i in 0..12 {
                bus.publish("road.parcel", Parcel(self.base + i))?;
            }
            self.sent = true;
        }
        bus.sleep(Duration::from_millis(20));
        Ok(())
    }
}

struct Depot {
    count: Arc<AtomicU32>,
    last: Arc<AtomicU64>,
}
impl Strand for Depot {
    fn name(&self) -> &str {
        "depot"
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<Delivered>("road.delivered");
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for (_sock, Delivered(n)) in bus.recv_all::<Delivered>()? {
            self.count.fetch_add(1, Ordering::SeqCst);
            self.last.store(n, Ordering::SeqCst);
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

fn main() {
    register_clone_type::<Parcel>();
    register_clone_type::<Delivered>();

    let mut bus = Bus::open();
    bus.register(Dispatcher {
        name: "orders",
        base: 0,
        sent: false,
    });
    bus.register(Dispatcher {
        name: "refunds",
        base: 100,
        sent: false,
    });
    bus.register(HighwayStrand::<Parcel, Delivered>::with_capacity(
        "highway",
        "road.parcel",
        "road.delivered",
        2, // lanes
        4, // capacity — small, so a burst of 24 backpressures
        |Parcel(n)| {
            std::thread::sleep(Duration::from_millis(60));
            Delivered(n)
        },
    ));
    let delivered = Arc::new(AtomicU32::new(0));
    let last = Arc::new(AtomicU64::new(0));
    bus.register(Depot {
        count: delivered.clone(),
        last: last.clone(),
    });
    bus.register_spider(StrandSpec::new("spider", || Box::new(Spider::new())));

    // Place the web in 3D: two ground sources at z=0, the highway lifted
    // to z=1, the spider overseeing at z=2 — so the threads climb from
    // the sources, cross at the highway, and drop to the depot.
    bus.place("orders", Position::new(0.0, 3.0, 0.0));
    bus.place("refunds", Position::new(0.0, -3.0, 0.0));
    bus.place("highway", Position::new(5.0, 0.0, 1.0));
    bus.place("depot", Position::new(10.0, 0.0, 0.0));
    bus.place("spider", Position::new(5.0, 0.0, 2.0));

    bus.run_until(Some(Duration::from_secs(3)));

    println!(
        "\ndelivered {} parcels through the highway (last #{})",
        delivered.load(Ordering::SeqCst),
        last.load(Ordering::SeqCst)
    );

    let snap = bus.fabric_snapshot();
    let pos = |n: &str| snap.position(n).unwrap();

    println!("\n── the web in 3D (logical coordinates, not latency) ──");
    println!("  orders      {:?}", pos("orders"));
    println!("  highway     {:?}", pos("highway"));
    println!("  spider      {:?}", pos("spider"));
    println!(
        "  elevation:  ground z={} < highway z={} < spider z={}  (on-ramp = a real z-delta)",
        pos("orders").z,
        pos("highway").z,
        pos("spider").z
    );
    assert!(pos("orders").z < pos("highway").z && pos("highway").z < pos("spider").z);

    println!(
        "\n  distance(orders, highway) = {:.3}",
        snap.distance("orders", "highway").unwrap()
    );

    // The highway emitted backpressure. How strongly is it felt near vs far?
    let highway_pos = pos("highway");
    let near = Position::new(5.0, 1.0, 1.0); // d=1 from the highway
    let far = Position::new(5.0, 20.0, 1.0); // d=20 from the highway
    let felt_near = attenuation(Some(highway_pos), Some(near));
    let felt_far = attenuation(Some(highway_pos), Some(far));
    println!("\n── the highway's backpressure, felt through 3D space ──");
    println!("  near observer (d=1):   felt = {felt_near:.3}");
    println!("  far  observer (d=20):  felt = {felt_far:.3}");
    println!(
        "  the near node shudders {:.1}x harder than the far one — the touch decays with distance",
        felt_near / felt_far
    );
    assert!(felt_near > felt_far);

    let scene = spiderweb_viz::scene_json(&snap);
    let json_path = std::env::temp_dir().join("spiderweb-scene.json");
    std::fs::write(&json_path, &scene).expect("write scene json");

    // The interactive, rotatable viewer (self-contained HTML).
    let html = spiderweb_viz::scene_html(&snap);
    let html_path = std::env::temp_dir().join("spiderweb-weave.html");
    std::fs::write(&html_path, &html).expect("write viewer");

    println!(
        "\n  3D scene:   {} ({} bytes)",
        json_path.display(),
        scene.len()
    );
    println!(
        "  3D viewer:  {} — open it and drag to rotate",
        html_path.display()
    );
    println!(
        "\n  (coordinates + distance + attenuation only — all nodes are in RAM, timing unchanged)"
    );
}
