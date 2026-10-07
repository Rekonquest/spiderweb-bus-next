//! TCP lane — the reference [`Lane`], re-expressing the **unchanged**
//! `spiderweb-edge` transport. This is the proof that the lane contract fits the
//! prototype with zero edge changes: `install` yields `EdgeListener` as the
//! ingress strand and `EdgeSender` as the egress strand.
//!
//! Because `Edge` is *consumed* by both `.sender()` and `.listener()` (each
//! takes `self` by value), a `TcpLane` stores its `.bridge()` calls as replayable
//! appliers and rebuilds a fresh `Edge` per side it installs.

use crate::lane::{Bridge, Lane, LaneError, LaneParts};
use spiderweb::Strand;
use spiderweb_edge::Edge;
use std::sync::Arc;

/// A replayable `.bridge()` call: applies one bridged type to a fresh `Edge`.
/// Stored so a `TcpLane` can rebuild the consumed `Edge` once per installed side.
type EdgeApplier = Arc<dyn Fn(Edge) -> Edge + Send + Sync>;

/// A TCP peer link as a [`Lane`]. Configure the bridged types with
/// [`TcpLane::bridge`], then choose ingress ([`TcpLane::listen`]) and/or egress
/// ([`TcpLane::connect`]). A bus that only listens omits `connect`; a bus that
/// only sends omits `listen`.
#[derive(Clone)]
pub struct TcpLane {
    bridges: Vec<Bridge>,
    appliers: Vec<EdgeApplier>,
    listen: Option<String>,
    connect: Option<String>,
}

impl Default for TcpLane {
    fn default() -> Self {
        Self::new()
    }
}

impl TcpLane {
    pub fn new() -> Self {
        TcpLane {
            bridges: Vec::new(),
            appliers: Vec::new(),
            listen: None,
            connect: None,
        }
    }

    /// Bridge messages of type `T` across the wire under a stable `tag`,
    /// published/received locally on `socket`. Same signature as `Edge::bridge`.
    pub fn bridge<T: 'static + Send + Clone>(
        mut self,
        tag: &'static str,
        socket: &'static str,
        encode: fn(&T) -> Vec<u8>,
        decode: fn(&[u8]) -> Option<T>,
    ) -> Self {
        self.bridges.push(Bridge { tag, socket });
        self.appliers.push(Arc::new(move |e: Edge| {
            e.bridge::<T>(tag, socket, encode, decode)
        }));
        self
    }

    /// Listen for inbound peers on `addr` (the ingress side). `addr` may be
    /// `host:0` for an ephemeral port — the bound address is returned in
    /// [`LaneParts::bound`].
    pub fn listen(mut self, addr: impl Into<String>) -> Self {
        self.listen = Some(addr.into());
        self
    }

    /// Connect out to `peer` (the egress side); the sender retries until the
    /// peer is up, exactly as `EdgeSender` does.
    pub fn connect(mut self, peer: impl Into<String>) -> Self {
        self.connect = Some(peer.into());
        self
    }

    fn build_edge(&self) -> Edge {
        let mut e = Edge::new();
        for apply in &self.appliers {
            e = apply(e);
        }
        e
    }
}

impl Lane for TcpLane {
    fn kind(&self) -> &'static str {
        "tcp"
    }

    fn bridges(&self) -> &[Bridge] {
        &self.bridges
    }

    fn install(&self) -> Result<LaneParts, LaneError> {
        let mut strands: Vec<(String, Box<dyn Strand>)> = Vec::new();
        let mut bound = None;
        if let Some(addr) = &self.listen {
            let listener = self
                .build_edge()
                .listener("tcp-in", addr.as_str())
                .map_err(LaneError::Bind)?;
            bound = Some(listener.local_addr());
            strands.push(("in".into(), Box::new(listener)));
        }
        if let Some(peer) = &self.connect {
            let sender = self.build_edge().sender("tcp-out", peer.clone());
            strands.push(("out".into(), Box::new(sender)));
        }
        if strands.is_empty() {
            return Err(LaneError::Config(
                "TcpLane needs at least one of .listen() / .connect()".into(),
            ));
        }
        Ok(LaneParts {
            strands,
            guard: None,
            bound,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lane::BusLanes;
    use spiderweb::{register_clone_type, Bus, BusHandle, Socket, StrandError};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    fn enc(s: &String) -> Vec<u8> {
        s.as_bytes().to_vec()
    }
    fn dec(b: &[u8]) -> Option<String> {
        String::from_utf8(b.to_vec()).ok()
    }

    struct Source {
        sent: bool,
    }
    impl Strand for Source {
        fn name(&self) -> &str {
            "src"
        }
        fn inputs(&self) -> &[Socket] {
            &[]
        }
        fn outputs(&self) -> &[Socket] {
            const S: Socket = Socket::new::<String>("note");
            std::slice::from_ref(&S)
        }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            if !self.sent {
                self.sent = true;
                bus.publish_static("note", "hello-over-a-lane".to_string())?;
            }
            bus.sleep(Duration::from_millis(10));
            Ok(())
        }
    }

    struct Collector {
        sink: Arc<Mutex<Vec<String>>>,
    }
    impl Strand for Collector {
        fn name(&self) -> &str {
            "collector"
        }
        fn inputs(&self) -> &[Socket] {
            const S: Socket = Socket::new::<String>("note");
            std::slice::from_ref(&S)
        }
        fn outputs(&self) -> &[Socket] {
            &[]
        }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            for m in bus.recv::<String>("note").unwrap_or_default() {
                self.sink.lock().unwrap().push(m);
            }
            bus.sleep(Duration::from_millis(10));
            Ok(())
        }
    }

    /// A `String` crosses a real TCP socket from one bus to another, wired
    /// entirely through the `Lane` contract (`add_lane`) rather than `Edge`
    /// directly — proving the abstraction fits the edge prototype.
    #[test]
    fn lane_trait_round_trips_over_tcp_loopback() {
        let collected = Arc::new(Mutex::new(Vec::<String>::new()));

        // Receiver bus: a TcpLane that only listens.
        let listen_lane = TcpLane::new()
            .bridge::<String>("wire.note", "note", enc, dec)
            .listen("127.0.0.1:0");
        let c = collected.clone();
        let (addr_tx, addr_rx) = std::sync::mpsc::channel();
        let recv_bus = thread::spawn(move || {
            register_clone_type::<String>();
            let mut bus = Bus::open();
            let (bound, _guard) = bus
                .add_lane("link", &listen_lane)
                .expect("install listener");
            addr_tx
                .send(bound.expect("listener reports a bound addr"))
                .ok();
            bus.register(Collector { sink: c });
            bus.run_until(Some(Duration::from_millis(700)));
        });

        let addr = addr_rx.recv().expect("bound addr");

        // Sender bus: a TcpLane that only connects out to the receiver.
        let send_bus = thread::spawn(move || {
            register_clone_type::<String>();
            let connect_lane = TcpLane::new()
                .bridge::<String>("wire.note", "note", enc, dec)
                .connect(addr.to_string());
            let mut bus = Bus::open();
            bus.add_lane("link", &connect_lane).expect("install sender");
            bus.register(Source { sent: false });
            bus.run_until(Some(Duration::from_millis(700)));
        });

        send_bus.join().unwrap();
        recv_bus.join().unwrap();

        let got = collected.lock().unwrap().clone();
        assert!(
            got.contains(&"hello-over-a-lane".to_string()),
            "the message must cross the TCP lane; got {got:?}"
        );
    }
}
