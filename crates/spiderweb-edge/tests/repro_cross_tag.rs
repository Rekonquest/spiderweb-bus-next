// TEMP reproduction: two same-Rust-type bridges with distinct tags/sockets.
// Verifies whether `cmd` payloads are mis-tagged as `note` (the reported bug).

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

// Source publishes on BOTH wire.note and wire.cmd every tick.
struct Source;
impl Strand for Source {
    fn name(&self) -> &str {
        "source"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const N: Socket = Socket::new::<String>("wire.note");
        const C: Socket = Socket::new::<String>("wire.cmd");
        static SS: [Socket; 2] = [N, C];
        &SS
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        bus.publish("wire.note", String::from("NOTE-payload"))?;
        bus.publish("wire.cmd", String::from("CMD-payload"))?;
        bus.sleep(Duration::from_millis(10));
        Ok(())
    }
}

// Collector on the receiving bus records which socket each String arrived on.
struct Collector {
    sink: Arc<Mutex<Vec<(String, String)>>>, // (socket, value)
}
impl Strand for Collector {
    fn name(&self) -> &str {
        "collector"
    }
    fn inputs(&self) -> &[Socket] {
        const N: Socket = Socket::new::<String>("note.in");
        const C: Socket = Socket::new::<String>("cmd.in");
        static SS: [Socket; 2] = [N, C];
        &SS
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
fn two_same_type_bridges_keep_their_tags() {
    let collected = Arc::new(Mutex::new(Vec::<(String, String)>::new()));

    // Receiver bus B: two bridges, distinct wire tags -> distinct local sockets.
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

    // Sender bus A: same two bridges. Source publishes on wire.note + wire.cmd.
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
    let note_on_noteln = got.iter().filter(|(s, v)| s == "note.in" && v == "NOTE-payload").count();
    let cmd_on_cmdln = got.iter().filter(|(s, v)| s == "cmd.in" && v == "CMD-payload").count();
    let cmd_on_noteln = got.iter().filter(|(s, v)| s == "note.in" && v == "CMD-payload").count();
    let note_on_cmdln = got.iter().filter(|(s, v)| s == "cmd.in" && v == "NOTE-payload").count();

    eprintln!("--- delivered (socket, value) pairs ---");
    for (s, v) in got.iter() {
        eprintln!("  {s} <- {v}");
    }
    eprintln!("NOTE-payload on note.in : {note_on_noteln}");
    eprintln!("CMD-payload  on cmd.in  : {cmd_on_cmdln}");
    eprintln!("CMD-payload  on note.in (MIS-TAG): {cmd_on_noteln}");
    eprintln!("NOTE-payload on cmd.in  (MIS-TAG): {note_on_cmdln}");

    // The CORRECT outcome: CMD-payload arrives on cmd.in, NOTE on note.in,
    // and NEITHER appears under the wrong socket. If the bug is real, we expect
    // CMD-payload to be shipped under tag "note" and land on note.in instead.
    assert!(cmd_on_cmdln > 0, "cmd channel never transmitted (CMD-payload missing on cmd.in)");
    assert_eq!(cmd_on_noteln, 0, "CMD-payload mis-tagged and delivered on note.in");
}
