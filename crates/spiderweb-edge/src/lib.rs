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
//! - **At-most-once** delivery: a frame produced while the peer is disconnected
//!   is dropped (no retransmit/queue). Reliability (sliding-window RLNC /
//!   RaptorQ) is a follow-up.
//! - **No authentication / no encryption**: the wire is plaintext and the peer
//!   is trusted. Auth (BLS / threshold) + TLS/QUIC are follow-ups — this is the
//!   trust boundary the bus must *earn*, not assert.
//! - **No cross-machine causality**: the lineage `Tag` does not cross the wire
//!   (Interval Tree Clocks are the follow-up).
//! - The listener's reader thread, when blocked in `accept()` with no peer
//!   connected, is not torn down on bus shutdown (a `Drop`-based stop is a
//!   follow-up); harmless for tests/demos. (When it is reading or blocked
//!   sending to a full channel, it exits cleanly once the strand is dropped.)
//!
//! ## Bounded by construction (the untrusted-input guards)
//! Frame headers are capped (`MAX_TAG`, `MAX_PAYLOAD`) so a hostile length
//! can't trigger a huge alloc; the reader→strand handoff is a **bounded**
//! `sync_channel` (`EDGE_CHANNEL_CAP`), so a flooding peer hits backpressure
//! (blocked `send` → unread socket → TCP flow control) rather than unbounded
//! memory growth; and a read timeout (`READ_TIMEOUT_SECS`) caps a mid-frame
//! stall. The peer is still *trusted* (no auth) — these guards keep a buggy or
//! noisy peer from degrading the host, not a determined attacker.

use spiderweb::{BusHandle, Socket, Strand, StrandError};
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

/// A peer link configuration: which types bridge across the wire and how each
/// serializes. Build it the same way on both ends, then call [`Edge::sender`]
/// on the producing bus and [`Edge::listener`] on the consuming bus.
pub struct Edge {
    entries: Vec<Entry>,
}

struct Entry {
    tag: &'static str,
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
            bus.recv_all::<T>()
                .unwrap_or_default()
                .into_iter()
                .map(|(_s, v)| encode(&v))
                .collect()
        });
        let push: Push = Box::new(move |bus: &mut BusHandle, bytes: &[u8]| {
            if let Some(v) = decode(bytes) {
                let _ = bus.publish(socket, v);
            }
        });
        self.entries.push(Entry { tag, socket: socket_obj, pull, push });
        self
    }

    /// The producing-side strand: subscribes to every bridged type, connects
    /// to `peer` (retrying each tick until it's up), and forwards each message
    /// over the socket.
    pub fn sender(self, name: &'static str, peer: impl Into<String>) -> EdgeSender {
        let mut socks = Vec::new();
        let mut pulls = Vec::new();
        for e in self.entries {
            socks.push(e.socket);
            pulls.push((e.tag, e.pull));
        }
        EdgeSender {
            name,
            peer: peer.into(),
            stream: None,
            socks,
            pulls,
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
        let (tx, rx) = mpsc::sync_channel::<(String, Vec<u8>)>(EDGE_CHANNEL_CAP);
        thread::spawn(move || reader_loop(listener, tx));

        let mut socks = Vec::new();
        let mut pushes = Vec::new();
        for e in self.entries {
            socks.push(e.socket);
            pushes.push((e.tag, e.push));
        }
        Ok(EdgeListener {
            name,
            rx,
            socks,
            pushes,
            local_addr,
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
        if self.stream.is_none() {
            self.stream = TcpStream::connect(&self.peer).ok();
        }

        // Pull every bridged type off the local bus first (borrows `pulls`),
        // then write (borrows `stream`).
        let mut frames: Vec<(&'static str, Vec<u8>)> = Vec::new();
        for (tag, pull) in &self.pulls {
            for payload in pull(bus) {
                frames.push((*tag, payload));
            }
        }
        if !frames.is_empty() {
            if let Some(stream) = self.stream.as_mut() {
                for (tag, payload) in &frames {
                    if write_frame(stream, tag, payload).is_err() {
                        // Peer dropped: reconnect next tick. Frames pulled
                        // this tick are lost (at-most-once MVP).
                        self.stream = None;
                        break;
                    }
                }
            }
            // Not connected => frames are dropped (at-most-once MVP).
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

/// Consuming-side bridge strand (wire -> local bus). See [`Edge::listener`].
pub struct EdgeListener {
    name: &'static str,
    rx: Receiver<(String, Vec<u8>)>,
    socks: Vec<Socket>,
    pushes: Vec<(&'static str, Push)>,
    local_addr: SocketAddr,
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
        for (tag, payload) in self.rx.try_iter() {
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
fn reader_loop(listener: TcpListener, tx: mpsc::SyncSender<(String, Vec<u8>)>) {
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

fn write_frame(stream: &mut TcpStream, tag: &str, payload: &[u8]) -> std::io::Result<()> {
    let tag_b = tag.as_bytes();
    stream.write_all(&(tag_b.len() as u32).to_be_bytes())?;
    stream.write_all(tag_b)?;
    stream.write_all(&(payload.len() as u32).to_be_bytes())?;
    stream.write_all(payload)?;
    stream.flush()
}

fn read_frame(stream: &mut TcpStream) -> std::io::Result<(String, Vec<u8>)> {
    use std::io::{Error, ErrorKind};
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
    Ok((tag, payload))
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
}
