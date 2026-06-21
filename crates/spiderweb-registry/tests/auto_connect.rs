//! Strands advertised at module scope are collected and auto-connected
//! onto a real bus — no `main()` registration list.

use spiderweb::{Bus, BusHandle, Socket, Strand, StrandError};
use spiderweb_registry::{advertise, advertise_spider, advertised, BusAutoConnect, Role};
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
    assert_eq!(
        advertised().filter(|a| a.role == Role::Spider).count(),
        1,
        "exactly one spider advertised"
    );
}

#[test]
fn auto_connect_starts_advertised_strands() {
    let mut bus = Bus::open();
    bus.auto_connect();
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
