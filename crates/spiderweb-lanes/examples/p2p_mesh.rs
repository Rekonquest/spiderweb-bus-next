//! P2P mesh lane demo: three in-process buses form a triangle; node A
//! broadcasts, and B and C each receive every message EXACTLY ONCE despite the
//! looping mesh edges (the per-origin dedup absorbs echoes and second paths).

use spiderweb::{register_clone_type, Bus, BusHandle, Socket, Strand, StrandError};
use spiderweb_lanes::mesh::Mesh;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn enc(s: &String) -> Vec<u8> {
    s.as_bytes().to_vec()
}
fn dec(b: &[u8]) -> Option<String> {
    String::from_utf8(b.to_vec()).ok()
}

/// Publishes a few notes onto the mesh, then idles.
struct Broadcaster {
    notes: Vec<String>,
    i: usize,
}
impl Strand for Broadcaster {
    fn name(&self) -> &str {
        "broadcaster"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<String>("mesh.tx");
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        if self.i < self.notes.len() {
            bus.publish_static("mesh.tx", self.notes[self.i].clone())?;
            self.i += 1;
        }
        bus.sleep(Duration::from_millis(120));
        Ok(())
    }
}

/// Collects what this node received off the mesh.
struct Collector {
    who: &'static str,
    sink: Arc<Mutex<Vec<String>>>,
}
impl Strand for Collector {
    fn name(&self) -> &str {
        "collector"
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<String>("mesh.rx");
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for m in bus.recv::<String>("mesh.rx").unwrap_or_default() {
            println!("  {} received: {m}", self.who);
            self.sink.lock().unwrap().push(m);
        }
        bus.sleep(Duration::from_millis(20));
        Ok(())
    }
}

fn node(name: &'static str, peers: Vec<String>) -> spiderweb_lanes::mesh::MeshNode {
    Mesh::new()
        .bridge::<String>("note", "mesh.tx", "mesh.rx", enc, dec)
        .node(name, "127.0.0.1:0", peers)
        .expect("bind mesh node")
}

fn main() {
    register_clone_type::<String>();
    println!("── Spiderweb P2P mesh lane ───────────────────────────────────");
    println!("3 buses (A,B,C) form a triangle; A broadcasts, B & C receive once each");
    println!("──────────────────────────────────────────────────────────────");

    let b = node("B", vec![]);
    let c = node("C", vec![]);
    let (pb, pc) = (b.local_addr().port(), c.local_addr().port());
    let a = node(
        "A",
        vec![format!("127.0.0.1:{pb}"), format!("127.0.0.1:{pc}")],
    );
    let pa = a.local_addr().port();
    b.peers().add(format!("127.0.0.1:{pc}"));
    b.peers().add(format!("127.0.0.1:{pa}"));
    c.peers().add(format!("127.0.0.1:{pb}"));
    c.peers().add(format!("127.0.0.1:{pa}"));

    let sink_b = Arc::new(Mutex::new(Vec::new()));
    let sink_c = Arc::new(Mutex::new(Vec::new()));
    let (sb, sc) = (sink_b.clone(), sink_c.clone());

    let tb = thread::spawn(move || {
        let mut bus = Bus::open();
        bus.register(b);
        bus.register(Collector { who: "B", sink: sb });
        bus.run_until(Some(Duration::from_millis(1600)));
    });
    let tc = thread::spawn(move || {
        let mut bus = Bus::open();
        bus.register(c);
        bus.register(Collector { who: "C", sink: sc });
        bus.run_until(Some(Duration::from_millis(1600)));
    });
    let ta = thread::spawn(move || {
        let mut bus = Bus::open();
        bus.register(a);
        bus.register(Broadcaster {
            notes: vec!["alpha".into(), "bravo".into(), "charlie".into()],
            i: 0,
        });
        bus.run_until(Some(Duration::from_millis(1600)));
    });
    ta.join().unwrap();
    tb.join().unwrap();
    tc.join().unwrap();

    println!("──────────────────────────────────────────────────────────────");
    println!(
        "B got {} messages: {:?}",
        sink_b.lock().unwrap().len(),
        *sink_b.lock().unwrap()
    );
    println!(
        "C got {} messages: {:?}",
        sink_c.lock().unwrap().len(),
        *sink_c.lock().unwrap()
    );
}
