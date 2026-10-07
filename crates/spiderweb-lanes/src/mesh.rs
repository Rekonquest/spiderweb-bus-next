//! P2P mesh lane — generalizes the point-to-point edge into an N-peer mesh.
//!
//! One [`MeshNode`] strand both *originates* local traffic (pulled off a tx
//! socket, fanned out to every peer) and *receives* the mesh's traffic (decoded
//! and republished on an rx socket, and flood-forwarded to peers). It reuses the
//! edge wire frame VERBATIM ([`crate::frame`]), reinterpreting the frame's
//! `session` field as a stable **origin** id: stamped once by the node that
//! first published a message, and copied through unchanged by every forwarder.
//! Dedup on `(origin, seq)` (edge's per-session high-water, reused exactly) makes
//! each message reach every node's bus EXACTLY ONCE and suppresses loops — for up
//! to `SESSION_TRACK` concurrently-tracked origins. Beyond that, the oldest
//! origin's mark is evicted, so a frame from a long-silent (evicted) origin that
//! reappears could be re-delivered; `SESSION_TRACK` is sized (256) well above any
//! realistic mesh fan-in.
//!
//! Two sockets per bridge (not one): a single-bus node that pulled and published
//! on the same socket would re-pull its own republished frames and re-originate
//! them. So local producers publish on `tx_socket` (the node subscribes there to
//! originate) and local consumers subscribe on `rx_socket` (the node publishes
//! there on receipt) — loop-free by construction.

use crate::frame::{
    make_pull, make_push, new_session, read_frame, write_frame, Frame, Pull, Push,
    EDGE_CHANNEL_CAP, READ_TIMEOUT_SECS, RING_CAP, SESSION_TRACK,
};
use spiderweb::{BusHandle, Socket, Strand, StrandError};
use std::collections::VecDeque;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

struct Entry {
    tag: &'static str,
    type_id: std::any::TypeId,
    tx_sock: Socket,
    pull: Pull,
    push: Push,
}

/// Builder for a mesh node. Same `.bridge()` shape as `Edge`, but each bridge
/// names a `tx_socket` (local → mesh) and an `rx_socket` (mesh → local).
#[derive(Default)]
pub struct Mesh {
    entries: Vec<Entry>,
}

impl Mesh {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bridge messages of type `T` under a stable `tag`. Local producers publish
    /// on `tx_socket`; received mesh messages are republished on `rx_socket`.
    pub fn bridge<T: 'static + Send + Clone>(
        mut self,
        tag: &'static str,
        tx_socket: &'static str,
        rx_socket: &'static str,
        encode: fn(&T) -> Vec<u8>,
        decode: fn(&[u8]) -> Option<T>,
    ) -> Self {
        self.entries.push(Entry {
            tag,
            type_id: std::any::TypeId::of::<T>(),
            tx_sock: Socket::new::<T>(tx_socket),
            pull: make_pull::<T>(tx_socket, encode),
            push: make_push::<T>(rx_socket, decode),
        });
        self
    }

    /// Build the mesh node strand: bind `listen` (accepts many peers) and seed
    /// the dial-out set with `peers`.
    pub fn node(
        self,
        name: &'static str,
        listen: impl ToSocketAddrs,
        peers: impl IntoIterator<Item = String>,
    ) -> std::io::Result<MeshNode> {
        let listener = TcpListener::bind(listen)?;
        let local_addr = listener.local_addr()?;
        let (tx, rx) = mpsc::sync_channel::<Frame>(EDGE_CHANNEL_CAP);
        let shutdown = Arc::new(AtomicBool::new(false));
        let sd = shutdown.clone();
        let reader = thread::spawn(move || reader_loop(listener, tx, sd));

        let mut in_socks = Vec::new();
        let mut seen_types = std::collections::HashSet::new();
        let mut pulls = Vec::new();
        let mut pushes = Vec::new();
        for e in self.entries {
            if seen_types.insert(e.type_id) {
                in_socks.push(e.tx_sock);
            }
            pulls.push((e.tag, e.pull));
            pushes.push((e.tag, e.push));
        }
        let book = PeerBook {
            peers: peers.into_iter().map(|a| (a, None)).collect(),
        };
        Ok(MeshNode {
            name,
            origin: new_session(),
            next_seq: 1,
            ring: VecDeque::new(),
            peers: Arc::new(Mutex::new(book)),
            rx: Some(rx),
            seen: Vec::new(),
            pulls,
            pushes,
            in_socks,
            local_addr,
            shutdown,
            reader: Some(reader),
        })
    }
}

/// The mesh node strand.
pub struct MeshNode {
    name: &'static str,
    origin: u64,
    next_seq: u64,
    /// Own recent frames `(seq, tag, payload)` for replay to a fresh peer.
    ring: VecDeque<(u64, &'static str, Vec<u8>)>,
    peers: Arc<Mutex<PeerBook>>,
    rx: Option<Receiver<Frame>>,
    /// Per-FOREIGN-origin high-water for dedup/loop-suppression (our own origin
    /// is dropped explicitly, so it is never tracked here).
    seen: Vec<(u64, u64)>,
    pulls: Vec<(&'static str, Pull)>,
    pushes: Vec<(&'static str, Push)>,
    in_socks: Vec<Socket>,
    local_addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    reader: Option<JoinHandle<()>>,
}

impl MeshNode {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Override the random origin id (e.g. for a deterministic test).
    pub fn with_origin(mut self, origin: u64) -> Self {
        self.origin = origin;
        self
    }

    /// A handle to grow/shrink the peer set at runtime (policy lives in the
    /// caller / spider; the node is dumb mechanism).
    pub fn peers(&self) -> PeerHandle {
        PeerHandle(self.peers.clone())
    }
}

impl Strand for MeshNode {
    fn name(&self) -> &str {
        self.name
    }
    fn inputs(&self) -> &[Socket] {
        &self.in_socks
    }
    fn outputs(&self) -> &[Socket] {
        // The node both subscribes (tx) and publishes (rx); declaring the tx
        // sockets as inputs is what matters for delivery. rx publishes need no
        // declaration (publish is open). Report inputs as outputs too so the
        // fabric sees the node as a participant on those channels.
        &self.in_socks
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        // ---- INGRESS: drain received frames, dedup, deliver, queue-to-forward.
        let frames: Vec<Frame> = match &self.rx {
            Some(rx) => rx.try_iter().collect(),
            None => Vec::new(),
        };
        let mut to_forward: Vec<Frame> = Vec::new();
        for (origin, seq, tag, payload) in frames {
            if origin == self.origin {
                continue; // our own frame echoed back — drop (no self-loop).
            }
            let idx = match self.seen.iter().position(|(o, _)| *o == origin) {
                Some(i) => i,
                None => {
                    if self.seen.len() >= SESSION_TRACK {
                        self.seen.remove(0);
                    }
                    self.seen.push((origin, 0));
                    self.seen.len() - 1
                }
            };
            if seq <= self.seen[idx].1 {
                continue; // already delivered (looped / second path) — drop.
            }
            self.seen[idx].1 = seq;
            for (t, push) in &self.pushes {
                if *t == tag {
                    push(bus, &payload);
                    break;
                }
            }
            to_forward.push((origin, seq, tag, payload));
        }

        // ---- EGRESS step 1: originate this tick's own local traffic.
        let mut own_fresh: Vec<(u64, &'static str, Vec<u8>)> = Vec::new();
        for (tag, pull) in &self.pulls {
            for payload in pull(bus) {
                let seq = self.next_seq;
                self.next_seq += 1;
                self.ring.push_back((seq, tag, payload.clone()));
                if self.ring.len() > RING_CAP {
                    self.ring.pop_front();
                }
                own_fresh.push((seq, *tag, payload));
            }
        }

        // ---- EGRESS step 2+3: fan out to every peer (lazy connect + replay).
        // Don't begin a round of (bounded but real) network I/O while winding
        // down — the kernel has no per-tick watchdog, so a blocked tick wedges
        // this strand permanently.
        if bus.is_shutting_down() {
            return Ok(());
        }
        // Take the streams OUT for unlocked I/O (dial/write must never hold the
        // lock — that would stall every PeerHandle and, with no per-tick
        // watchdog, wedge this tick) but LEAVE the addr entries in the book, so
        // a concurrent PeerHandle add/remove during the I/O window is honored:
        // a removed addr won't be found on restore (its stream is dropped); an
        // added addr stays (dialed next tick).
        let origin = self.origin;
        let mut work: Vec<(String, Option<TcpStream>)> = {
            let mut book = self.peers.lock().unwrap_or_else(|p| p.into_inner());
            book.peers
                .iter_mut()
                .map(|(addr, stream)| (addr.clone(), stream.take()))
                .collect()
        };
        for (addr, stream_opt) in work.iter_mut() {
            let just_connected = if stream_opt.is_none() {
                match dial(addr) {
                    Some(s) => {
                        *stream_opt = Some(s);
                        true
                    }
                    None => continue, // dead peer; retry next tick.
                }
            } else {
                false
            };
            let stream = stream_opt.as_mut().unwrap();
            let mut ok = true;
            if just_connected {
                // Replay our whole ring (own origin) so a fresh peer catches up.
                for (seq, tag, payload) in &self.ring {
                    if write_frame(stream, origin, *seq, tag, payload).is_err() {
                        ok = false;
                        break;
                    }
                }
            } else {
                // Live peer: only this tick's fresh own frames.
                for (seq, tag, payload) in &own_fresh {
                    if write_frame(stream, origin, *seq, tag, payload).is_err() {
                        ok = false;
                        break;
                    }
                }
            }
            // Forward others' frames (origin preserved) to every peer.
            if ok {
                for (o, seq, tag, payload) in &to_forward {
                    if write_frame(stream, *o, *seq, tag, payload).is_err() {
                        ok = false;
                        break;
                    }
                }
            }
            if !ok {
                *stream_opt = None; // drop this one peer; reconnect next tick.
            }
        }
        // Restore the streams to the addrs still present; a peer removed during
        // the I/O window is simply not found, so its stream is dropped (closed)
        // — the removal is honored. Added peers keep their `None` stream.
        {
            let mut book = self.peers.lock().unwrap_or_else(|p| p.into_inner());
            for (addr, stream) in work {
                if let Some(slot) = book.peers.iter_mut().find(|(a, _)| *a == addr) {
                    slot.1 = stream;
                }
            }
        }

        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

impl Drop for MeshNode {
    fn drop(&mut self) {
        // EdgeListener teardown order: flag, drop rx, wake accept(), join.
        self.shutdown.store(true, Ordering::Release);
        drop(self.rx.take());
        let wake = match self.local_addr {
            SocketAddr::V4(a) if a.ip().is_unspecified() => {
                SocketAddr::from((Ipv4Addr::LOCALHOST, a.port()))
            }
            SocketAddr::V6(a) if a.ip().is_unspecified() => {
                SocketAddr::from((Ipv6Addr::LOCALHOST, a.port()))
            }
            other => other,
        };
        let _ = TcpStream::connect_timeout(&wake, Duration::from_millis(500));
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

/// Connect to a peer with bounded blocking, so a slow/dead peer never stalls a
/// tick for the OS default connect timeout: resolve, connect with a short
/// timeout, set a write timeout.
fn dial(addr: &str) -> Option<TcpStream> {
    let sa = addr.to_socket_addrs().ok()?.next()?;
    let s = TcpStream::connect_timeout(&sa, Duration::from_secs(1)).ok()?;
    let _ = s.set_write_timeout(Some(Duration::from_secs(5)));
    Some(s)
}

/// The dial-out set: `addr -> lazily-connected stream`.
struct PeerBook {
    peers: Vec<(String, Option<TcpStream>)>,
}

/// A cloneable handle to add/remove peers at runtime.
#[derive(Clone)]
pub struct PeerHandle(Arc<Mutex<PeerBook>>);

impl PeerHandle {
    /// Add a peer to dial on the next tick (idempotent).
    pub fn add(&self, addr: impl Into<String>) {
        let addr = addr.into();
        let mut book = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if !book.peers.iter().any(|(a, _)| *a == addr) {
            book.peers.push((addr, None));
        }
    }

    /// Remove a peer and drop its outbound stream.
    pub fn remove(&self, addr: &str) {
        let mut book = self.0.lock().unwrap_or_else(|p| p.into_inner());
        book.peers.retain(|(a, _)| a != addr);
    }

    /// The current peer addresses.
    pub fn list(&self) -> Vec<String> {
        let book = self.0.lock().unwrap_or_else(|p| p.into_inner());
        book.peers.iter().map(|(a, _)| a.clone()).collect()
    }
}

/// Accept peers and read EACH on its own thread, so a node receives from ALL
/// its inbound peers concurrently. (A single reader that read one connection to
/// EOF before accepting the next — the edge point-to-point pattern — would only
/// ever service the first peer that connected, starving the rest: fatal for a
/// mesh.) The per-connection threads are daemon-like: they exit on EOF (peer
/// close at shutdown), on the read timeout, or when the channel is gone.
fn reader_loop(listener: TcpListener, tx: SyncSender<Frame>, shutdown: Arc<AtomicBool>) {
    for conn in listener.incoming() {
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        let mut stream = match conn {
            Ok(s) => s,
            Err(_) => continue,
        };
        let tx = tx.clone();
        let sd = shutdown.clone();
        thread::spawn(move || {
            let _ = stream.set_read_timeout(Some(Duration::from_secs(READ_TIMEOUT_SECS)));
            while let Ok(frame) = read_frame(&mut stream) {
                if sd.load(Ordering::Acquire) || tx.send(frame).is_err() {
                    return;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spiderweb::{register_clone_type, Bus};
    use std::sync::Mutex as StdMutex;

    fn enc(s: &String) -> Vec<u8> {
        s.as_bytes().to_vec()
    }
    fn dec(b: &[u8]) -> Option<String> {
        String::from_utf8(b.to_vec()).ok()
    }

    struct Source {
        msg: String,
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
            const S: Socket = Socket::new::<String>("mesh.tx");
            std::slice::from_ref(&S)
        }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            if !self.sent {
                self.sent = true;
                bus.publish_static("mesh.tx", self.msg.clone())?;
            }
            bus.sleep(Duration::from_millis(10));
            Ok(())
        }
    }

    struct Collector {
        sink: Arc<StdMutex<Vec<String>>>,
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
                self.sink.lock().unwrap().push(m);
            }
            bus.sleep(Duration::from_millis(10));
            Ok(())
        }
    }

    /// A triangle of 3 buses: A originates one message; B and C each receive it
    /// EXACTLY ONCE despite the looping edges (A↔B↔C↔A) — the per-origin dedup
    /// absorbs every echo and second path.
    #[test]
    fn triangle_delivers_each_message_exactly_once() {
        register_clone_type::<String>();
        let sink_b = Arc::new(StdMutex::new(Vec::<String>::new()));
        let sink_c = Arc::new(StdMutex::new(Vec::<String>::new()));

        // Bind B and C first so their ports are known.
        let node_b = Mesh::new()
            .bridge::<String>("note", "mesh.tx", "mesh.rx", enc, dec)
            .node("B", "127.0.0.1:0", [])
            .unwrap();
        let node_c = Mesh::new()
            .bridge::<String>("note", "mesh.tx", "mesh.rx", enc, dec)
            .node("C", "127.0.0.1:0", [])
            .unwrap();
        let (pb, pc) = (node_b.local_addr().port(), node_c.local_addr().port());

        // A dials B and C.
        let node_a = Mesh::new()
            .bridge::<String>("note", "mesh.tx", "mesh.rx", enc, dec)
            .node(
                "A",
                "127.0.0.1:0",
                [format!("127.0.0.1:{pb}"), format!("127.0.0.1:{pc}")],
            )
            .unwrap();
        let pa = node_a.local_addr().port();
        // Close the triangle (these are loop edges; dedup must absorb them).
        node_b.peers().add(format!("127.0.0.1:{pc}"));
        node_b.peers().add(format!("127.0.0.1:{pa}"));
        node_c.peers().add(format!("127.0.0.1:{pb}"));
        node_c.peers().add(format!("127.0.0.1:{pa}"));

        let (sb, sc) = (sink_b.clone(), sink_c.clone());
        let tb = thread::spawn(move || {
            let mut bus = Bus::open();
            bus.register(node_b);
            bus.register(Collector { sink: sb });
            bus.run_until(Some(Duration::from_millis(1500)));
        });
        let tc = thread::spawn(move || {
            let mut bus = Bus::open();
            bus.register(node_c);
            bus.register(Collector { sink: sc });
            bus.run_until(Some(Duration::from_millis(1500)));
        });
        let ta = thread::spawn(move || {
            let mut bus = Bus::open();
            bus.register(node_a);
            bus.register(Source {
                msg: "hello-mesh".into(),
                sent: false,
            });
            bus.run_until(Some(Duration::from_millis(1500)));
        });
        ta.join().unwrap();
        tb.join().unwrap();
        tc.join().unwrap();

        let got_b = sink_b.lock().unwrap().clone();
        let got_c = sink_c.lock().unwrap().clone();
        assert_eq!(got_b, vec!["hello-mesh"], "B must receive it exactly once");
        assert_eq!(got_c, vec!["hello-mesh"], "C must receive it exactly once");
    }
}
