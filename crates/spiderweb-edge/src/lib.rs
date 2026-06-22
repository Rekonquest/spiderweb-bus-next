//! `spiderweb-edge` — cross-process / P2P lanes over a real socket.
//!
//! The in-process bus routes type-erased payloads by `TypeId` within one
//! process. `TypeId` is meaningless across builds and machines, so to leave
//! the process a message needs three things this crate supplies: a **stable
//! cross-process tag**, **serialization**, and a **network L0**. A peer link
//! is just an on-ramp/off-ramp over the wire — an [`EdgeSender`] strand taps
//! the local bus for the bridged types, serializes each, and ships it; an
//! [`EdgeListener`] strand on the far bus deserializes and re-publishes it,
//! so the far bus's local subscribers receive it exactly as if it were local.
//!
//! Doctrine: the **kernel stays std-only and dependency-free**; the network +
//! serialization live here, quarantined (exactly how `spiderweb-registry`
//! quarantines `inventory`). The MVP itself is also std-only, **zero external
//! dependencies**, and `#![forbid(unsafe_code)]` — `std::net` TCP, `std::thread`,
//! and `std::sync::mpsc` are all in `std`.
//!
//! ## Wire frame
//! Length-prefixed, big-endian: `[u32 tag_len][tag bytes][u32 payload_len][payload]`.
//!
//! ## Honest MVP scope (these are the *next* hardened layers, not done here)
//! - **Bounded reliable delivery** (was at-most-once): each frame carries a
//!   monotonic sequence number and is held in a bounded sender ring
//!   (`RING_CAP`); on (re)connect the sender replays the ring so the peer
//!   catches up on anything missed during a disconnect (or before the first
//!   connect), and the receiver dedups by sequence — so a recovered frame is
//!   never delivered twice. This recovers losses up to `RING_CAP` frames of
//!   disconnect. **Honest limits:** beyond the ring bound, frames are lost; it
//!   assumes ONE long-lived sender whose sequence is monotonic across
//!   reconnects. A sender *restart* (new process, seq reset to 1) against a
//!   still-running listener is NOT a brief hiccup: EVERY frame from the
//!   restarted sender is silently dropped until its seq climbs past the
//!   receiver's high-water mark — potentially thousands of messages of
//!   blackout. Fixing that needs a per-session id keyed dedup (the next tier).
//!   And there is no end-to-end ACK, so the ring frees by age, not by receiver
//!   progress. Unbounded reliability with no head-of-line blocking
//!   (sliding-window RLNC / RaptorQ) is the next tier.
//! - **No authentication / no encryption**: the wire is plaintext and the peer
//!   is trusted. Auth (BLS / threshold) + TLS/QUIC are follow-ups — this is the
//!   trust boundary the bus must *earn*, not assert.
//! - **No cross-machine causality**: the lineage `Tag` does not cross the wire
//!   (Interval Tree Clocks are the follow-up).
//! - The listener's reader thread (and its bound port) **leak until process
//!   exit**, unconditionally. The kernel never joins or drops strand workers on
//!   shutdown — `run_until` just returns and the worker loops forever — so the
//!   `EdgeListener` (and the `Receiver` it owns) is never dropped, and the
//!   reader's `send`-fails-when-the-receiver-is-dropped stop therefore never
//!   fires. A real teardown needs **kernel cooperation** (a worker shutdown
//!   signal); harmless for short-lived tests/demos. The bound socket is not set
//!   `SO_REUSEADDR`, so an immediate same-process re-bind to a *fixed* port
//!   could fail (the tests/demo bind port 0).
//!
//! ## Bounded by construction (the untrusted-input guards)
//! Frame headers are capped (`MAX_TAG`, `MAX_PAYLOAD`) so a hostile length
//! can't trigger a huge alloc; the reader→strand handoff is a **bounded**
//! `sync_channel` (`EDGE_CHANNEL_CAP`), so a flooding peer hits backpressure
//! (blocked `send` → unread socket → TCP flow control) rather than unbounded
//! memory growth; and a read timeout (`READ_TIMEOUT_SECS`) caps a mid-frame
//! stall. The peer is still *trusted* (no auth) — these guards keep a buggy or
//! noisy peer from degrading the host, not a determined attacker.

#![forbid(unsafe_code)]

use spiderweb::{BusHandle, Socket, Strand, StrandError};
use std::any::TypeId;
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;

/// Drain the bridged type off the local bus and return one encoded payload
/// per message. (Used by [`EdgeSender`].)
type Pull = Box<dyn Fn(&mut BusHandle) -> Vec<Vec<u8>> + Send>;
/// Decode one payload and publish it onto the local bus. (Used by
/// [`EdgeListener`].)
type Push = Box<dyn Fn(&mut BusHandle, &[u8]) + Send>;

/// Sanity caps so a corrupt/hostile frame header can't trigger a huge alloc.
const MAX_TAG: usize = 1024;
const MAX_PAYLOAD: usize = 4 * 1024 * 1024;
/// Bound the reader-thread -> strand handoff so a flooding peer can't grow it
/// without limit: when full, the reader's `send` blocks, which stops draining
/// the socket, which backpressures the peer via TCP flow control. Worst-case
/// buffered bytes ≈ `EDGE_CHANNEL_CAP * MAX_PAYLOAD`.
const EDGE_CHANNEL_CAP: usize = 256;
/// A peer that opens a connection then stalls mid-frame must not block the
/// reader thread forever; a generous read timeout caps the stall and drops the
/// connection (the peer can reconnect).
const READ_TIMEOUT_SECS: u64 = 30;
/// Sender resend ring: how many recent frames are retained for replay on
/// (re)connect. Recovers losses up to this many frames of disconnect; worst-
/// case retained bytes ≈ `RING_CAP * MAX_PAYLOAD`.
const RING_CAP: usize = 256;

/// A peer link configuration: which types bridge across the wire and how each
/// serializes. Build it the same way on both ends, then call [`Edge::sender`]
/// on the producing bus and [`Edge::listener`] on the consuming bus.
pub struct Edge {
    entries: Vec<Entry>,
}

struct Entry {
    tag: &'static str,
    /// The bridged Rust type. One socket per distinct type is declared as the
    /// strand's input (kernel subscription is by `TypeId`); `type_id` lets the
    /// builder dedup them so the strand is not subscribed twice for one type
    /// (which would double-deliver every message of that type).
    type_id: TypeId,
    socket: Socket,
    pull: Pull,
    push: Push,
}

impl Default for Edge {
    fn default() -> Self {
        Self::new()
    }
}

impl Edge {
    pub fn new() -> Self {
        Edge { entries: Vec::new() }
    }

    /// Bridge messages of type `T` across the wire under a stable `tag`.
    /// `socket` is the local channel name `T` is published/received on;
    /// `encode`/`decode` are the wire codec (kept explicit and dumb so the
    /// crate needs no `serde` dependency — `decode` returns `None` to drop a
    /// malformed payload rather than panic).
    pub fn bridge<T: 'static + Send + Clone>(
        mut self,
        tag: &'static str,
        socket: &'static str,
        encode: fn(&T) -> Vec<u8>,
        decode: fn(&[u8]) -> Option<T>,
    ) -> Self {
        let socket_obj = Socket::new::<T>(socket);
        let pull: Pull = Box::new(move |bus: &mut BusHandle| {
            // SOCKET-scoped drain (NOT `recv_all::<T>` by type): take only this
            // socket's messages, so two bridges that share one Rust type don't
            // vacuum / mis-tag each other's traffic. `encode` runs under
            // catch_unwind so a panicking codec drops the frame instead of
            // unwinding (and silently killing) the worker.
            bus.recv::<T>(socket)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|v| {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| encode(&v))).ok()
                })
                .collect()
        });
        let push: Push = Box::new(move |bus: &mut BusHandle, bytes: &[u8]| {
            // `decode` under catch_unwind too: a panicking codec degrades to a
            // dropped frame, not a dead bus.
            let decoded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| decode(bytes)))
                .ok()
                .flatten();
            if let Some(v) = decoded {
                let _ = bus.publish(socket, v);
            }
        });
        self.entries.push(Entry {
            tag,
            type_id: TypeId::of::<T>(),
            socket: socket_obj,
            pull,
            push,
        });
        self
    }

    /// The producing-side strand: subscribes to every bridged type, connects
    /// to `peer` (retrying each tick until it's up), and forwards each message
    /// over the socket.
    pub fn sender(self, name: &'static str, peer: impl Into<String>) -> EdgeSender {
        let mut socks = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut pulls = Vec::new();
        for e in self.entries {
            // Declare each distinct type as an input ONCE (subscription is by
            // TypeId), so the strand isn't double-subscribed for a shared type.
            // Every entry still gets its own socket-scoped pull.
            if seen.insert(e.type_id) {
                socks.push(e.socket);
            }
            pulls.push((e.tag, e.pull));
        }
        EdgeSender {
            name,
            peer: peer.into(),
            stream: None,
            socks,
            pulls,
            next_seq: 1,
            ring: VecDeque::new(),
        }
    }

    /// The consuming-side strand: binds `addr`, accepts a peer, and republishes
    /// every received frame onto the local bus. Binding happens now (so the
    /// port is open before the sender connects); a background thread does the
    /// blocking reads and hands frames to the strand over a channel.
    pub fn listener(self, name: &'static str, addr: impl std::net::ToSocketAddrs) -> std::io::Result<EdgeListener> {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        // Bounded: see EDGE_CHANNEL_CAP — backpressure, not unbounded growth.
        let (tx, rx) = mpsc::sync_channel::<(u64, String, Vec<u8>)>(EDGE_CHANNEL_CAP);
        thread::spawn(move || reader_loop(listener, tx));

        let mut socks = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut pushes = Vec::new();
        for e in self.entries {
            if seen.insert(e.type_id) {
                socks.push(e.socket);
            }
            pushes.push((e.tag, e.push));
        }
        Ok(EdgeListener {
            name,
            rx,
            socks,
            pushes,
            local_addr,
            delivered: 0,
        })
    }
}

/// Producing-side bridge strand (local bus -> wire). See [`Edge::sender`].
pub struct EdgeSender {
    name: &'static str,
    peer: String,
    stream: Option<TcpStream>,
    socks: Vec<Socket>,
    pulls: Vec<(&'static str, Pull)>,
    /// Next sequence to assign (monotonic; starts at 1, so 0 means "none yet").
    next_seq: u64,
    /// Bounded resend ring of recent frames, replayed on (re)connect.
    ring: VecDeque<(u64, &'static str, Vec<u8>)>,
}

impl Strand for EdgeSender {
    fn name(&self) -> &str {
        self.name
    }
    fn inputs(&self) -> &[Socket] {
        &self.socks
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        // 1. Pull fresh frames off the local bus, assign monotonic seqs, and
        //    append to the bounded resend ring.
        let mut fresh: Vec<(u64, &'static str, Vec<u8>)> = Vec::new();
        for (tag, pull) in &self.pulls {
            for payload in pull(bus) {
                let seq = self.next_seq;
                self.next_seq += 1;
                fresh.push((seq, *tag, payload));
            }
        }
        for (seq, tag, payload) in &fresh {
            self.ring.push_back((*seq, *tag, payload.clone()));
        }
        while self.ring.len() > RING_CAP {
            self.ring.pop_front();
        }

        // 2. (Re)connect. On a FRESH connection, replay the whole ring so the
        //    peer catches up on anything it missed during a disconnect (or
        //    before the first connect); it dedups by seq. On a live connection,
        //    send only this tick's fresh frames.
        let reconnecting = self.stream.is_none();
        if reconnecting {
            self.stream = TcpStream::connect(&self.peer).ok();
        }
        if let Some(stream) = self.stream.as_mut() {
            let result = if reconnecting {
                self.ring
                    .iter()
                    .try_for_each(|(seq, tag, payload)| write_frame(stream, *seq, tag, payload))
            } else {
                fresh
                    .iter()
                    .try_for_each(|(seq, tag, payload)| write_frame(stream, *seq, tag, payload))
            };
            if result.is_err() {
                // Peer dropped: reconnect next tick and replay then. Nothing is
                // lost beyond the ring's bound.
                self.stream = None;
            }
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

/// Consuming-side bridge strand (wire -> local bus). See [`Edge::listener`].
pub struct EdgeListener {
    name: &'static str,
    rx: Receiver<(u64, String, Vec<u8>)>,
    socks: Vec<Socket>,
    pushes: Vec<(&'static str, Push)>,
    local_addr: SocketAddr,
    /// High-water mark of delivered sequence numbers. A reconnect replay
    /// re-sends already-seen seqs; we skip any `seq <= delivered` so a recovered
    /// frame is never delivered twice.
    delivered: u64,
}

impl EdgeListener {
    /// The address actually bound (useful when binding to port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Strand for EdgeListener {
    fn name(&self) -> &str {
        self.name
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        &self.socks
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for (seq, tag, payload) in self.rx.try_iter() {
            // Dedup: seqs are monotonic from the sender, so a reconnect replay
            // re-sends already-seen seqs — skip them.
            if seq <= self.delivered {
                continue;
            }
            self.delivered = seq;
            for (t, push) in &self.pushes {
                if *t == tag {
                    push(bus, &payload);
                    break;
                }
            }
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

/// Accept connections and stream their frames to `tx`. Re-accepts after a peer
/// disconnects; exits when the receiver (the strand) is dropped.
fn reader_loop(listener: TcpListener, tx: mpsc::SyncSender<(u64, String, Vec<u8>)>) {
    for conn in listener.incoming() {
        let mut stream = match conn {
            Ok(s) => s,
            Err(_) => continue,
        };
        // Cap a mid-frame stall (slow-loris) so one wedged connection can't
        // block the reader (and thus the listener) forever.
        let _ = stream.set_read_timeout(Some(Duration::from_secs(READ_TIMEOUT_SECS)));
        // Read frames until EOF/error/timeout, then fall through to accept the
        // next connection. `send` blocks when the bounded channel is full
        // (backpressure) and returns Err when the strand is dropped (stop).
        while let Ok(frame) = read_frame(&mut stream) {
            if tx.send(frame).is_err() {
                return; // strand dropped: stop.
            }
        }
    }
}

fn write_frame(stream: &mut TcpStream, seq: u64, tag: &str, payload: &[u8]) -> std::io::Result<()> {
    stream.write_all(&seq.to_be_bytes())?;
    let tag_b = tag.as_bytes();
    stream.write_all(&(tag_b.len() as u32).to_be_bytes())?;
    stream.write_all(tag_b)?;
    stream.write_all(&(payload.len() as u32).to_be_bytes())?;
    stream.write_all(payload)?;
    stream.flush()
}

fn read_frame(stream: &mut TcpStream) -> std::io::Result<(u64, String, Vec<u8>)> {
    use std::io::{Error, ErrorKind};
    let mut seq8 = [0u8; 8];
    stream.read_exact(&mut seq8)?;
    let seq = u64::from_be_bytes(seq8);
    if seq == 0 {
        // 0 is the reserved "none yet" sentinel; a real frame's seq is >= 1.
        return Err(Error::new(ErrorKind::InvalidData, "seq 0 is reserved"));
    }
    let mut len4 = [0u8; 4];
    stream.read_exact(&mut len4)?;
    let tag_len = u32::from_be_bytes(len4) as usize;
    if tag_len > MAX_TAG {
        return Err(Error::new(ErrorKind::InvalidData, "tag too long"));
    }
    let mut tag_b = vec![0u8; tag_len];
    stream.read_exact(&mut tag_b)?;
    let tag = String::from_utf8(tag_b).map_err(|_| Error::new(ErrorKind::InvalidData, "tag not utf8"))?;

    stream.read_exact(&mut len4)?;
    let payload_len = u32::from_be_bytes(len4) as usize;
    if payload_len > MAX_PAYLOAD {
        return Err(Error::new(ErrorKind::InvalidData, "payload too long"));
    }
    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload)?;
    Ok((seq, tag, payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use spiderweb::{register_clone_type, Bus};
    use std::sync::{Arc, Mutex};

    struct Source;
    impl Strand for Source {
        fn name(&self) -> &str {
            "source"
        }
        fn inputs(&self) -> &[Socket] {
            &[]
        }
        fn outputs(&self) -> &[Socket] {
            const S: Socket = Socket::new::<String>("wire.note");
            std::slice::from_ref(&S)
        }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            // Publish every tick so a message lands AFTER the sender connects
            // (the MVP is at-most-once; this is the test's robustness, not a
            // bus guarantee).
            bus.publish("wire.note", String::from("hello-from-A"))?;
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
            const S: Socket = Socket::new::<String>("wire.note");
            std::slice::from_ref(&S)
        }
        fn outputs(&self) -> &[Socket] {
            &[]
        }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            for (_s, v) in bus.recv_all::<String>()? {
                self.sink.lock().unwrap().push(v);
            }
            bus.sleep(Duration::from_millis(5));
            Ok(())
        }
    }

    // `&String` (not `&str`) is required: the codec signature is `fn(&T)` and
    // here `T = String`, so the `&String` is forced by the generic API.
    #[allow(clippy::ptr_arg)]
    fn enc(s: &String) -> Vec<u8> {
        s.clone().into_bytes()
    }
    fn dec(b: &[u8]) -> Option<String> {
        String::from_utf8(b.to_vec()).ok()
    }

    /// A message published on bus A is received by bus B across a REAL TCP
    /// socket (127.0.0.1, ephemeral port) — the edge-transport MVP end to end.
    #[test]
    fn message_crosses_a_real_socket() {
        let collected = Arc::new(Mutex::new(Vec::<String>::new()));

        // Receiver bus B: bind the listener now so we know the port.
        let listener = Edge::new()
            .bridge::<String>("note", "wire.note", enc, dec)
            .listener("edge-in", "127.0.0.1:0")
            .expect("bind listener");
        let port = listener.local_addr().port();

        let collected_b = collected.clone();
        let recv_bus = thread::spawn(move || {
            register_clone_type::<String>();
            let mut bus = Bus::open();
            bus.register(listener);
            bus.register(Collector { sink: collected_b });
            bus.run_until(Some(Duration::from_millis(800)));
        });

        // Sender bus A: a Source publishes, the EdgeSender forwards to B.
        let send_bus = thread::spawn(move || {
            register_clone_type::<String>();
            let mut bus = Bus::open();
            bus.register(Source);
            bus.register(
                Edge::new()
                    .bridge::<String>("note", "wire.note", enc, dec)
                    .sender("edge-out", format!("127.0.0.1:{port}")),
            );
            bus.run_until(Some(Duration::from_millis(800)));
        });

        send_bus.join().unwrap();
        recv_bus.join().unwrap();

        let got = collected.lock().unwrap();
        assert!(
            got.iter().any(|s| s == "hello-from-A"),
            "bus B should receive the message bus A published over the socket, got {:?}",
            *got
        );
    }

    /// Publishes a distinct String on two different sockets, same Rust type.
    struct TwoSource;
    impl Strand for TwoSource {
        fn name(&self) -> &str {
            "two-source"
        }
        fn inputs(&self) -> &[Socket] {
            &[]
        }
        fn outputs(&self) -> &[Socket] {
            const S: [Socket; 2] = [Socket::new::<String>("wire.a"), Socket::new::<String>("wire.b")];
            &S
        }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            bus.publish("wire.a", String::from("msg-a"))?;
            bus.publish("wire.b", String::from("msg-b"))?;
            bus.sleep(Duration::from_millis(10));
            Ok(())
        }
    }

    /// Collects Strings from ONE named socket (socket-scoped recv).
    struct SocketCollector {
        name: &'static str,
        socket: &'static str,
        socket_obj: Socket,
        sink: Arc<Mutex<Vec<String>>>,
    }
    impl SocketCollector {
        fn new(name: &'static str, socket: &'static str, sink: Arc<Mutex<Vec<String>>>) -> Self {
            SocketCollector { name, socket, socket_obj: Socket::new::<String>(socket), sink }
        }
    }
    impl Strand for SocketCollector {
        fn name(&self) -> &str {
            self.name
        }
        fn inputs(&self) -> &[Socket] {
            std::slice::from_ref(&self.socket_obj)
        }
        fn outputs(&self) -> &[Socket] {
            &[]
        }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            for v in bus.recv::<String>(self.socket)? {
                self.sink.lock().unwrap().push(v);
            }
            bus.sleep(Duration::from_millis(5));
            Ok(())
        }
    }

    /// Two bridges of the SAME Rust type (String) on different sockets/tags must
    /// route INDEPENDENTLY across the wire — no cross-contamination, no dead
    /// channel. (Regression for the recv_all-by-TypeId bug: the old code had the
    /// first bridge vacuum every String and mis-tag it, so `wire.b` traffic
    /// arrived on `wire.a` and `wire.b` got nothing.)
    #[test]
    fn two_same_type_bridges_route_independently() {
        let a_sink = Arc::new(Mutex::new(Vec::<String>::new()));
        let b_sink = Arc::new(Mutex::new(Vec::<String>::new()));

        let listener = Edge::new()
            .bridge::<String>("a", "wire.a", enc, dec)
            .bridge::<String>("b", "wire.b", enc, dec)
            .listener("edge-in", "127.0.0.1:0")
            .expect("bind listener");
        let port = listener.local_addr().port();

        let a2 = a_sink.clone();
        let b2 = b_sink.clone();
        let recv_bus = thread::spawn(move || {
            register_clone_type::<String>();
            let mut bus = Bus::open();
            bus.register(listener);
            bus.register(SocketCollector::new("col-a", "wire.a", a2));
            bus.register(SocketCollector::new("col-b", "wire.b", b2));
            bus.run_until(Some(Duration::from_millis(800)));
        });

        let send_bus = thread::spawn(move || {
            register_clone_type::<String>();
            let mut bus = Bus::open();
            bus.register(TwoSource);
            bus.register(
                Edge::new()
                    .bridge::<String>("a", "wire.a", enc, dec)
                    .bridge::<String>("b", "wire.b", enc, dec)
                    .sender("edge-out", format!("127.0.0.1:{port}")),
            );
            bus.run_until(Some(Duration::from_millis(800)));
        });

        send_bus.join().unwrap();
        recv_bus.join().unwrap();

        let a = a_sink.lock().unwrap();
        let b = b_sink.lock().unwrap();
        assert!(a.iter().any(|s| s == "msg-a"), "wire.a should receive msg-a, got {:?}", *a);
        assert!(!a.iter().any(|s| s == "msg-b"), "wire.a must NOT receive msg-b (cross-tag leak), got {:?}", *a);
        assert!(b.iter().any(|s| s == "msg-b"), "wire.b should receive msg-b, got {:?}", *b);
        assert!(!b.iter().any(|s| s == "msg-a"), "wire.b must NOT receive msg-a (cross-tag leak), got {:?}", *b);
    }

    /// Publishes 10 distinct messages, once each.
    struct CountSource {
        next: u32,
    }
    impl Strand for CountSource {
        fn name(&self) -> &str {
            "count-source"
        }
        fn inputs(&self) -> &[Socket] {
            &[]
        }
        fn outputs(&self) -> &[Socket] {
            const S: Socket = Socket::new::<String>("wire.count");
            std::slice::from_ref(&S)
        }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            if self.next < 10 {
                bus.publish("wire.count", format!("msg-{}", self.next))?;
                self.next += 1;
            }
            bus.sleep(Duration::from_millis(20));
            Ok(())
        }
    }

    /// EVERY distinct message published on bus A arrives on bus B exactly once —
    /// none dropped by connect timing, none duplicated. Each is published a
    /// single time; the bounded resend ring + replay-on-connect recovers any
    /// produced before the sender connected, and seq-dedup prevents duplicates.
    /// (Under the old at-most-once MVP, messages published before the sender
    /// connected were silently lost.)
    #[test]
    fn all_published_messages_arrive_exactly_once() {
        let sink = Arc::new(Mutex::new(Vec::<String>::new()));

        let listener = Edge::new()
            .bridge::<String>("count", "wire.count", enc, dec)
            .listener("edge-in", "127.0.0.1:0")
            .expect("bind listener");
        let port = listener.local_addr().port();

        let sink_b = sink.clone();
        let recv_bus = thread::spawn(move || {
            register_clone_type::<String>();
            let mut bus = Bus::open();
            bus.register(listener);
            bus.register(SocketCollector::new("col", "wire.count", sink_b));
            bus.run_until(Some(Duration::from_millis(900)));
        });

        let send_bus = thread::spawn(move || {
            register_clone_type::<String>();
            let mut bus = Bus::open();
            bus.register(CountSource { next: 0 });
            bus.register(
                Edge::new()
                    .bridge::<String>("count", "wire.count", enc, dec)
                    .sender("edge-out", format!("127.0.0.1:{port}")),
            );
            bus.run_until(Some(Duration::from_millis(900)));
        });

        send_bus.join().unwrap();
        recv_bus.join().unwrap();

        let got = sink.lock().unwrap();
        for i in 0..10 {
            let m = format!("msg-{i}");
            assert_eq!(
                got.iter().filter(|s| **s == m).count(),
                1,
                "{m} should arrive exactly once; got {:?}",
                *got
            );
        }
    }
}
