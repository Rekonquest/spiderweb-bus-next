//! Weave demo: watch the fabric draw itself from traffic.
//!
//! Run with: `cargo run -p highway --example weave`
//!
//! Topology (never declared to the fabric — it discovers all of this):
//!
//!   orders  ┐
//!           ├─> ledger ─> enrich (highway, 3 lanes) ─> sink
//!   refunds ┘
//!
//! Two sources each start their own threads. Both feed a shared
//! `ledger`, so an intersection EMERGES there. Work then crosses the
//! parallel `enrich` highway — and each item's thread survives the lane
//! reordering — before landing at `sink`. The spider, watching
//! sys.lifecycle, narrates threads being born and intersections
//! emerging as they happen.
//!
//! Then we run it again with a `MergeAll` transfer policy at `ledger`,
//! and the fabric shows the governed hop: downstream of the ledger, the
//! per-origin threads fuse into one.

use highway::HighwayStrand;
use spider::Spider;
use spiderweb::{
    register_clone_type, Bus, BusHandle, FabricSnapshot, Socket, Strand, StrandError, StrandSpec,
    TransferPolicy,
};
use std::time::Duration;

#[derive(Clone)]
struct Entry(u64);
#[derive(Clone)]
struct Posted(u64);
#[derive(Clone)]
struct Settled(u64);

/// A source of ledger entries. Bursts three, each its own thread.
struct Source {
    name: &'static str,
    base: u64,
    sent: bool,
}

impl Strand for Source {
    fn name(&self) -> &str {
        self.name
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<Entry>("entry");
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        if !self.sent {
            for i in 0..3 {
                bus.publish("entry", Entry(self.base + i))?;
            }
            self.sent = true;
            bus.log("posted 3 entries");
        }
        bus.sleep(Duration::from_millis(20));
        Ok(())
    }
}

/// The shared node both sources feed — where their threads cross.
/// Uses the tagged API so each entry continues its own thread precisely.
struct Ledger;

impl Strand for Ledger {
    fn name(&self) -> &str {
        "ledger"
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<Entry>("entry");
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<Posted>("posted");
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for (_sock, tag, Entry(n)) in bus.recv_tagged::<Entry>()? {
            bus.publish_tagged("posted", tag, Posted(n))?;
        }
        bus.sleep(Duration::from_millis(10));
        Ok(())
    }
}

struct Sink;

impl Strand for Sink {
    fn name(&self) -> &str {
        "sink"
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<Settled>("settled");
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for (_sock, Settled(n)) in bus.recv_all::<Settled>()? {
            bus.log(&format!("settled #{n}"));
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

fn build_and_run(policy: Option<TransferPolicy>) -> FabricSnapshot {
    register_clone_type::<Entry>();
    register_clone_type::<Posted>();
    register_clone_type::<Settled>();

    let mut bus = Bus::open();
    if let Some(p) = policy {
        bus.set_transfer("ledger", p);
    }
    bus.register(Sink);
    bus.register(HighwayStrand::<Posted, Settled>::new(
        "enrich",
        "posted",
        "settled",
        3,
        |Posted(n)| {
            std::thread::sleep(Duration::from_millis(30));
            Settled(n)
        },
    ));
    bus.register(Ledger);
    bus.register(Source {
        name: "orders",
        base: 1,
        sent: false,
    });
    bus.register(Source {
        name: "refunds",
        base: 101,
        sent: false,
    });
    bus.register_spider(StrandSpec::new("spider", || Box::new(Spider::new())));
    bus.run_until(Some(Duration::from_secs(2)));
    bus.fabric_snapshot()
}

fn print_fabric(title: &str, snap: &FabricSnapshot) {
    println!("\n──────────────────────────────────────────────────────────");
    println!("{title}");
    println!("──────────────────────────────────────────────────────────");
    println!("threads that formed (desire paths):");
    let mut threads = snap.threads.clone();
    threads.sort_by(|a, b| a.direction.cmp(&b.direction));
    for t in &threads {
        println!("  thread {:>2}:  {}", t.id, t.direction.join(" → "));
    }
    println!("intersections that emerged (where threads crossed):");
    if snap.intersections.is_empty() {
        println!("  (none)");
    } else {
        let mut isx = snap.intersections.clone();
        isx.sort_by(|a, b| a.node.cmp(&b.node));
        for i in &isx {
            println!(
                "  at {:<8} {} threads cross  (transfer: {:?})",
                i.node,
                i.threads.len(),
                i.transfer
            );
        }
    }
}

fn main() {
    println!("Run 1: default policy (Stay) — threads keep their identity across the web.");
    let stay = build_and_run(None);
    print_fabric(
        "DEFAULT (Stay): per-origin threads weave through the web",
        &stay,
    );

    println!("\n\nRun 2: MergeAll at `ledger` — a governed hop fuses threads downstream.");
    let merged = build_and_run(Some(TransferPolicy::MergeAll));
    print_fabric(
        "MergeAll at `ledger`: downstream threads fuse into one",
        &merged,
    );
}
