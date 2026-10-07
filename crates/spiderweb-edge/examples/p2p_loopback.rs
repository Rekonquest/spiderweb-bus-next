//! Two SEPARATE bus instances federated over a real TCP socket.
//!
//! Run with: `cargo run -p spiderweb-edge --example p2p_loopback`
//!
//! Bus A produces "orders" and bridges them across the wire; bus B, a wholly
//! separate `Bus` (its own kernel, its own RAM, reached only through the
//! socket), receives and prints them. This is the edge-transport MVP: the
//! in-process fabric, now crossing a process boundary.

use spiderweb::{register_clone_type, Bus, BusHandle, Socket, Strand, StrandError};
use spiderweb_edge::Edge;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[allow(clippy::ptr_arg)]
fn enc(s: &String) -> Vec<u8> {
    s.clone().into_bytes()
}
fn dec(b: &[u8]) -> Option<String> {
    String::from_utf8(b.to_vec()).ok()
}

/// Bus A: emits a handful of orders, once each.
struct Producer {
    next: u32,
}
impl Strand for Producer {
    fn name(&self) -> &str {
        "producer"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<String>("order");
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        if self.next < 5 {
            let order = format!("order-#{}", self.next);
            bus.log(&format!("📤 bus A emits {order}"));
            bus.publish("order", order)?;
            self.next += 1;
        }
        bus.sleep(Duration::from_millis(80));
        Ok(())
    }
}

/// Bus B: prints every order that arrives across the wire.
struct Depot {
    seen: Arc<AtomicU32>,
}
impl Strand for Depot {
    fn name(&self) -> &str {
        "depot"
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<String>("order");
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for (_s, order) in bus.recv_all::<String>()? {
            self.seen.fetch_add(1, Ordering::SeqCst);
            bus.log(&format!(
                "📥 bus B received {order}  (crossed a real socket)"
            ));
        }
        bus.sleep(Duration::from_millis(10));
        Ok(())
    }
}

fn main() {
    println!("\n── two buses, federated over TCP ──\n");

    // Bus B (consumer): bind the listener now so we know the port.
    let listener = Edge::new()
        .bridge::<String>("order", "order", enc, dec)
        .listener("edge-in", "127.0.0.1:0")
        .expect("bind listener");
    let addr = listener.local_addr();
    println!("  bus B listening on {addr}\n");

    let seen = Arc::new(AtomicU32::new(0));
    let seen_b = seen.clone();
    let bus_b = thread::spawn(move || {
        register_clone_type::<String>();
        let mut bus = Bus::open();
        bus.register(listener);
        bus.register(Depot { seen: seen_b });
        bus.run_until(Some(Duration::from_secs(2)));
    });

    // Bus A (producer): forward its orders to bus B's address.
    let bus_a = thread::spawn(move || {
        register_clone_type::<String>();
        let mut bus = Bus::open();
        bus.register(Producer { next: 0 });
        bus.register(
            Edge::new()
                .bridge::<String>("order", "order", enc, dec)
                .sender("edge-out", format!("127.0.0.1:{}", addr.port())),
        );
        bus.run_until(Some(Duration::from_secs(2)));
    });

    bus_a.join().unwrap();
    bus_b.join().unwrap();

    println!(
        "\n  bus B received {} orders from bus A across the socket — edge transport works.\n",
        seen.load(Ordering::SeqCst)
    );
}
