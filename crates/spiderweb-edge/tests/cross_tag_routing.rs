//! Integration test (drives the crate from outside, as a user would): two
//! bridges of the same Rust type, with DISTINCT wire tags and DISTINCT local
//! socket names on each side. The wire routes by TAG, so a message published on
//! the sender's `wire.cmd` (tag `cmd`) must arrive on the listener's `cmd.in`,
//! never on `note.in`. Regression for the recv_all-by-TypeId cross-tag bug.

use spiderweb::{register_clone_type, Bus, BusHandle, Socket, Strand, StrandError};
use spiderweb_edge::Edge;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[allow(clippy::ptr_arg)]
fn enc(s: &String) -> Vec<u8> {
    s.clone().into_bytes()
}
fn dec(b: &[u8]) -> Option<String> {
    String::from_utf8(b.to_vec()).ok()
}

/// Bus A: publishes a distinct String on two different local sockets.
struct Source;
impl Strand for Source {
    fn name(&self) -> &str {
        "source"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const SS: [Socket; 2] = [
            Socket::new::<String>("wire.note"),
            Socket::new::<String>("wire.cmd"),
        ];
        &SS
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        bus.publish("wire.note", String::from("NOTE-payload"))?;
        bus.publish("wire.cmd", String::from("CMD-payload"))?;
        bus.sleep(Duration::from_millis(10));
        Ok(())
    }
}

/// Bus B: records which local socket each received String landed on. One String
/// subscription receives every String (routing is by type); `recv_all` reports
/// each with its true socket name.
struct Collector {
    sink: Arc<Mutex<Vec<(String, String)>>>,
}
impl Strand for Collector {
    fn name(&self) -> &str {
        "collector"
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<String>("note.in");
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for (s, v) in bus.recv_all::<String>()? {
            self.sink.lock().unwrap().push((s.to_string(), v));
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

#[test]
fn distinct_tags_route_to_distinct_sockets_across_the_wire() {
    let collected = Arc::new(Mutex::new(Vec::<(String, String)>::new()));

    let listener = Edge::new()
        .bridge::<String>("note", "note.in", enc, dec)
        .bridge::<String>("cmd", "cmd.in", enc, dec)
        .listener("edge-in", "127.0.0.1:0")
        .expect("bind listener");
    let port = listener.local_addr().port();

    let collected_b = collected.clone();
    let recv_bus = thread::spawn(move || {
        register_clone_type::<String>();
        let mut bus = Bus::open();
        bus.register(listener);
        bus.register(Collector { sink: collected_b });
        bus.run_until(Some(Duration::from_millis(900)));
    });

    let send_bus = thread::spawn(move || {
        register_clone_type::<String>();
        let mut bus = Bus::open();
        bus.register(Source);
        bus.register(
            Edge::new()
                .bridge::<String>("note", "wire.note", enc, dec)
                .bridge::<String>("cmd", "wire.cmd", enc, dec)
                .sender("edge-out", format!("127.0.0.1:{port}")),
        );
        bus.run_until(Some(Duration::from_millis(900)));
    });

    send_bus.join().unwrap();
    recv_bus.join().unwrap();

    let got = collected.lock().unwrap();
    let on = |sock: &str, val: &str| got.iter().filter(|(s, v)| s == sock && v == val).count();
    assert!(
        on("cmd.in", "CMD-payload") > 0,
        "cmd tag must transmit to cmd.in, got {:?}",
        *got
    );
    assert!(
        on("note.in", "NOTE-payload") > 0,
        "note tag must transmit to note.in, got {:?}",
        *got
    );
    assert_eq!(
        on("note.in", "CMD-payload"),
        0,
        "CMD must NOT be mis-tagged onto note.in, got {:?}",
        *got
    );
    assert_eq!(
        on("cmd.in", "NOTE-payload"),
        0,
        "NOTE must NOT be mis-tagged onto cmd.in, got {:?}",
        *got
    );
}
