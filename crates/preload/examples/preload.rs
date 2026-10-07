//! Preload demo: parallel read-ahead keeps the parallel layer fed.
//!
//! Run with: `cargo run -p preload --example preload`
//!
//! A "fetch" (loading/decoding a parcel) takes 40ms. It feeds a 4-lane
//! highway that processes each parcel in 40ms. The highway COULD finish
//! 4 parcels per 40ms — but only if it's kept fed.
//!
//! Arm A (single-thread read-ahead): one fetch thread produces a parcel
//! every 40ms, so the highway only ever has ~1 parcel to work on — 3
//! lanes sit idle and the run is fetch-bound (~40ms/parcel).
//!
//! Arm B (parallel read-ahead, `with_workers(4)`): four fetch threads
//! read ahead in parallel, producing ~4 parcels per 40ms, so all 4
//! highway lanes stay full — the run is now highway-bound (~10ms/parcel).
//!
//! The win is real and honest: read-ahead alone can't beat a single
//! slow fetch (that's the source-rate ceiling), but PARALLEL read-ahead
//! raises the source rate to feed the parallel layer. ~3× in practice.

use highway::HighwayStrand;
use preload::PreloadStrand;
use spiderweb::{register_clone_type, Bus, BusHandle, Socket, Strand, StrandError};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const N: u64 = 16;
const LANES: usize = 4;
const FETCH: Duration = Duration::from_millis(40);
const WORK: Duration = Duration::from_millis(40);

#[derive(Clone)]
struct Parcel(u64);
#[derive(Clone)]
struct Delivered(u64);

/// Collects delivered parcels; records the wall-clock when the whole
/// batch is in.
struct Depot {
    count: Arc<AtomicU32>,
    done: Arc<Mutex<Option<Duration>>>,
    start: Instant,
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
        for (_sock, Delivered(_n)) in bus.recv_all::<Delivered>()? {
            let c = self.count.fetch_add(1, Ordering::SeqCst) + 1;
            if c == N as u32 {
                *self.done.lock().unwrap() = Some(self.start.elapsed());
            }
        }
        bus.sleep(Duration::from_millis(2));
        Ok(())
    }
}

fn run_arm(parallel: bool) -> Duration {
    register_clone_type::<Parcel>();
    register_clone_type::<Delivered>();

    let count = Arc::new(AtomicU32::new(0));
    let done = Arc::new(Mutex::new(None));
    let start = Instant::now();

    let mut bus = Bus::open();
    bus.register(Depot {
        count,
        done: done.clone(),
        start,
    });
    bus.register(HighwayStrand::<Parcel, Delivered>::new(
        "highway",
        "road.parcel",
        "road.delivered",
        LANES,
        |Parcel(n)| {
            std::thread::sleep(WORK);
            Delivered(n)
        },
    ));

    let fetch = |i: u64| {
        if i < N {
            std::thread::sleep(FETCH);
            Some(Parcel(i))
        } else {
            None
        }
    };
    let loader = if parallel {
        // Four fetch threads read ahead in parallel; burst >= lanes so
        // the highway gets a full batch each tick.
        PreloadStrand::<Parcel>::with_workers("loader", "road.parcel", 8, LANES, LANES, move |_w| {
            fetch
        })
    } else {
        // One fetch thread: a parcel every 40ms, highway starved.
        PreloadStrand::<Parcel>::new("loader", "road.parcel", 8, fetch)
    };
    bus.register(loader);

    bus.run_until(Some(Duration::from_secs(4)));
    let elapsed = done.lock().unwrap().unwrap_or_else(|| start.elapsed());
    elapsed
}

fn main() {
    println!("Loading {N} parcels (fetch {FETCH:?} each) through a {LANES}-lane highway (work {WORK:?} each).\n");

    let serial = run_arm(false);
    println!("Arm A — single-thread read-ahead: {serial:?}");
    println!("        (highway starved: ~1 parcel in flight, 3 lanes idle)\n");

    let parallel = run_arm(true);
    println!("Arm B — parallel read-ahead (with_workers({LANES})): {parallel:?}");
    println!("        (highway fed: all {LANES} lanes full)\n");

    println!(
        "──────────────────────────────────────────────────────────\nparallel read-ahead kept the parallel layer fed: {:.1}× faster\n(read-ahead alone can't beat a single slow fetch — PARALLEL fetch can)",
        serial.as_secs_f64() / parallel.as_secs_f64()
    );
}
