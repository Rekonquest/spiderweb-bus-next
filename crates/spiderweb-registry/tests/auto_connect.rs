//! Strands advertised at module scope are collected and auto-connected
//! onto a real bus — no `main()` registration list.

use spiderweb::{Bus, BusHandle, Socket, Strand, StrandError};
use spiderweb_registry::{
    advertise, advertise_spider, advertised, BusAutoConnect, Role, StrandAdvert,
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

// Each crate advertises itself — here at the test module's scope. No
// main() edit; `auto_connect` finds them.
advertise!("auto-src", || Box::new(AutoSrc { sent: false }) as Box<dyn Strand>);
advertise!("auto-snk", || Box::new(AutoSnk) as Box<dyn Strand>);
advertise_spider!("auto-spider", || Box::new(spider::Spider::new()) as Box<dyn Strand>);

#[test]
fn advertised_set_is_collected() {
    let names: std::collections::BTreeSet<&str> = advertised().map(|a| a.name).collect();
    assert!(names.contains("auto-src"), "auto-src advertised");
    assert!(names.contains("auto-snk"), "auto-snk advertised");
    assert!(names.contains("auto-spider"), "spider advertised");
    // Pin cardinality so an accidentally added/removed advert fails loudly.
    assert_eq!(
        advertised().filter(|a| a.role == Role::Strand).count(),
        2,
        "exactly two strands advertised"
    );
    assert_eq!(
        advertised().filter(|a| a.role == Role::Spider).count(),
        1,
        "exactly one spider advertised"
    );
    assert_eq!(advertised().count(), 3, "no extra or missing adverts");
}

#[test]
fn auto_connect_starts_advertised_strands() {
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
    assert_ne!(p1, p2, "each factory call must build a fresh strand, not a shared one");
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
        },
        StrandAdvert {
            name: "s2",
            factory: make_spider,
            role: Role::Spider,
        },
    ];
    let mut bus = Bus::open();
    bus.auto_connect_from(&adverts);
}
