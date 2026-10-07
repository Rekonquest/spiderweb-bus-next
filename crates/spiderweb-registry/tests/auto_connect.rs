//! Strands advertised at module scope are collected and auto-connected
//! onto a real bus — no `main()` registration list.

use spiderweb::{Bus, BusHandle, Position, Socket, Strand, StrandError};
use spiderweb_registry::{
    advertise, advertise_at, advertise_spider, advertised, BusAutoConnect, Role, StrandAdvert,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

static SRC_RAN: AtomicU32 = AtomicU32::new(0);
static SNK_GOT: AtomicU32 = AtomicU32::new(0);

struct AutoSrc {
    sent: bool,
}
impl Strand for AutoSrc {
    fn name(&self) -> &str {
        "auto-src"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<u64>("auto.ping");
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        SRC_RAN.fetch_add(1, Ordering::SeqCst);
        if !self.sent {
            bus.publish("auto.ping", 7u64)?;
            self.sent = true;
        }
        bus.sleep(Duration::from_millis(10));
        Ok(())
    }
}

struct AutoSnk;
impl Strand for AutoSnk {
    fn name(&self) -> &str {
        "auto-snk"
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<u64>("auto.ping");
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        let n = bus.recv_all::<u64>()?.len() as u32;
        SNK_GOT.fetch_add(n, Ordering::SeqCst);
        bus.sleep(Duration::from_millis(10));
        Ok(())
    }
}

/// A strand that advertises a 3D position via `advertise_at!`.
struct AutoPlaced;
impl Strand for AutoPlaced {
    fn name(&self) -> &str {
        "auto-placed"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        bus.sleep(Duration::from_millis(10));
        Ok(())
    }
}

// Each crate advertises itself — here at the test module's scope. No
// main() edit; `auto_connect` finds them.
advertise!("auto-src", || Box::new(AutoSrc { sent: false })
    as Box<dyn Strand>);
advertise!("auto-snk", || Box::new(AutoSnk) as Box<dyn Strand>);
advertise_at!(
    "auto-placed",
    || Box::new(AutoPlaced) as Box<dyn Strand>,
    Position::new(7.0, 8.0, 9.0)
);
advertise_spider!("auto-spider", || Box::new(spider::Spider::new())
    as Box<dyn Strand>);

#[test]
fn advertised_set_is_collected() {
    let names: std::collections::BTreeSet<&str> = advertised().map(|a| a.name).collect();
    assert!(names.contains("auto-src"), "auto-src advertised");
    assert!(names.contains("auto-snk"), "auto-snk advertised");
    assert!(names.contains("auto-placed"), "auto-placed advertised");
    assert!(names.contains("auto-spider"), "spider advertised");
    // Pin cardinality so an accidentally added/removed advert fails loudly.
    assert_eq!(
        advertised().filter(|a| a.role == Role::Strand).count(),
        3,
        "exactly three strands advertised"
    );
    assert_eq!(
        advertised().filter(|a| a.role == Role::Spider).count(),
        1,
        "exactly one spider advertised"
    );
    assert_eq!(advertised().count(), 4, "no extra or missing adverts");
    // advertise_at! stored the position on the advert.
    let placed = advertised().find(|a| a.name == "auto-placed").unwrap();
    assert_eq!(placed.position, Some(Position::new(7.0, 8.0, 9.0)));
}

#[test]
fn auto_connect_starts_and_places_advertised_strands() {
    let mut bus = Bus::open();
    // The loud variant: panics if any of these weren't advertised.
    bus.auto_connect_expecting(&["auto-src", "auto-snk"]);
    bus.run_until(Some(Duration::from_millis(400)));

    assert!(
        SRC_RAN.load(Ordering::SeqCst) > 0,
        "the advertised source should have run with no manual register"
    );
    assert!(
        SNK_GOT.load(Ordering::SeqCst) > 0,
        "the advertised sink should have received the source's message — connected by type alone"
    );
    // The advertised position was forwarded to the geometric fabric.
    assert_eq!(
        bus.fabric_snapshot().position("auto-placed"),
        Some(Position::new(7.0, 8.0, 9.0)),
        "auto_connect should forward an advertised position to Bus::place"
    );
}

/// Each `auto_connect` builds a FRESH strand instance (so a crashed
/// strand can be restarted from the same advert). Two factory calls must
/// produce distinct heap allocations.
#[test]
fn factory_yields_independent_instances() {
    let adv = advertised().find(|a| a.name == "auto-src").unwrap();
    let s1 = (adv.factory)();
    let s2 = (adv.factory)();
    let p1 = (&*s1 as *const dyn Strand) as *const ();
    let p2 = (&*s2 as *const dyn Strand) as *const ();
    assert_ne!(
        p1, p2,
        "each factory call must build a fresh strand, not a shared one"
    );
}

/// Two advertised spiders must panic — the at-most-one rule holds through
/// auto-connect. Driven via the explicit-advert seam so it doesn't touch
/// the process-global slice.
#[test]
#[should_panic(expected = "register_spider called twice")]
fn two_advertised_spiders_panic() {
    fn make_spider() -> Box<dyn Strand> {
        Box::new(spider::Spider::new())
    }
    let adverts = [
        StrandAdvert {
            name: "s1",
            factory: make_spider,
            role: Role::Spider,
            position: None,
        },
        StrandAdvert {
            name: "s2",
            factory: make_spider,
            role: Role::Spider,
            position: None,
        },
    ];
    let mut bus = Bus::open();
    bus.auto_connect_from(&adverts);
}
