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
//! Length-prefixed, big-endian:
//! `[u64 session][u64 seq][u32 tag_len][tag bytes][u32 payload_len][payload]`.
//!
//! ## Honest MVP scope (these are the *next* hardened layers, not done here)
//! - **Bounded reliable delivery** (was at-most-once): each frame carries a
//!   **session id** + a monotonic sequence number and is held in a bounded
//!   sender ring (`RING_CAP`); on (re)connect the sender replays the ring so the
//!   peer catches up on anything missed during a disconnect (or before the first
//!   connect), and the receiver dedups by `(session, seq)` — so a recovered
//!   frame is never delivered twice. This recovers losses up to `RING_CAP`
//!   frames of disconnect. A sender **restart** (new process, seqs reset to 1)
//!   is now recovered cleanly too: the fresh session id makes the receiver reset
//!   its dedup high-water instead of blacking out the restarted sender's low
//!   seqs (regression: `restarted_sender_recovers_instead_of_blacking_out`).
//!   **Honest limits:** beyond the ring bound, frames are lost; and there is no
//!   end-to-end ACK, so the ring frees by age, not by receiver progress.
//!   Unbounded reliability with no head-of-line blocking (sliding-window RLNC /
//!   RaptorQ) is the next tier.
//! - **No authentication / no encryption**: the wire is plaintext and the peer
//!   is trusted. Auth (BLS / threshold) + TLS/QUIC are follow-ups — this is the
//!   trust boundary the bus must *earn*, not assert.
//! - **No cross-machine causality**: the lineage `Tag` does not cross the wire
//!   (Interval Tree Clocks are the follow-up).
//!
//! (The reader thread + bound port used to leak until process exit; Phase-1
//! graceful shutdown now drops the strand and [`EdgeListener::drop`] joins the
//! reader and frees the port. The bound socket is not `SO_REUSEADDR`, so an
//! immediate same-process re-bind to a *fixed* port could briefly fail; the
//! tests/demos bind port 0.)
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
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A per-sender-process session id. A sender that RESTARTS resets its sequence
/// numbers to 1; without a session id the receiver would dedup those reset seqs
/// against its old high-water mark and silently drop everything until the new
/// seqs climbed past it (a multi-thousand-frame blackout). Stamping each frame
/// with a fresh session id (monotonic wall-clock nanos, distinct across
/// restarts) lets the receiver detect the new session and reset its dedup
/// high-water — so a restarted sender is recovered cleanly. Collision needs two
/// senders to start within the same nanosecond; acceptable for this MVP.
fn new_session() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1)
}

/// Drain the bridged type off the local bus and return one encoded payload
/// per message. (Used by [`EdgeSender`].)
type Pull = Box<dyn Fn(&mut BusHandle) -> Vec<Vec<u8>> + Send>;
/// Decode one payload and publish it onto the local bus. (Used by
/// [`EdgeListener`].)
type Push = Box<dyn Fn(&mut BusHandle, &[u8]) + Send>;
/// One wire frame in flight, reader thread → listener strand:
/// `(session, seq, tag, payload)`.
type Frame = (u64, u64, String, Vec<u8>);

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
/// How many distinct sender sessions the listener tracks high-waters for at
/// once (oldest evicted when full). >1 so session OSCILLATION (a session
/// briefly reappearing, or two senders sharing one listener) resumes each
/// session's own mark instead of resetting to 0 and re-delivering its ring;
/// bounded so a stream of distinct sessions can't grow the tracker forever.
const SESSION_TRACK: usize = 16;

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
        Edge {
            entries: Vec::new(),
        }
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
            session: new_session(),
            next_seq: 1,
            ring: VecDeque::new(),
        }
    }

    /// The consuming-side strand: binds `addr`, accepts a peer, and republishes
    /// every received frame onto the local bus. Binding happens now (so the
    /// port is open before the sender connects); a background thread does the
    /// blocking reads and hands frames to the strand over a channel.
    pub fn listener(
        self,
        name: &'static str,
        addr: impl std::net::ToSocketAddrs,
    ) -> std::io::Result<EdgeListener> {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        // Bounded: see EDGE_CHANNEL_CAP — backpressure, not unbounded growth.
        let (tx, rx) = mpsc::sync_channel::<Frame>(EDGE_CHANNEL_CAP);
        let shutdown = Arc::new(AtomicBool::new(false));
        let reader_shutdown = shutdown.clone();
        let reader = thread::spawn(move || reader_loop(listener, tx, reader_shutdown));

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
            rx: Some(rx),
            socks,
            pushes,
            local_addr,
            sessions: Vec::new(),
            shutdown,
            reader: Some(reader),
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
    /// Per-process session id, stamped on every frame so a sender RESTART is
    /// recovered cleanly by the receiver instead of blacked out ([`new_session`]).
    session: u64,
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
        let session = self.session;
        if let Some(stream) = self.stream.as_mut() {
            let result = if reconnecting {
                self.ring.iter().try_for_each(|(seq, tag, payload)| {
                    write_frame(stream, session, *seq, tag, payload)
                })
            } else {
                fresh.iter().try_for_each(|(seq, tag, payload)| {
                    write_frame(stream, session, *seq, tag, payload)
                })
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
    /// `Option` so `Drop` can `take()` and drop the receiver FIRST — that
    /// disconnects the channel, instantly unblocking a reader stuck in a full
    /// `tx.send()`, before we join it (otherwise the receiver wouldn't drop
    /// until after `Drop` returns, deadlocking the join).
    rx: Option<Receiver<Frame>>,
    socks: Vec<Socket>,
    pushes: Vec<(&'static str, Push)>,
    local_addr: SocketAddr,
    /// Per-session delivery high-water marks `(session, high_water)`, one entry
    /// per recently-seen sender session, oldest first. A frame's `seq` is
    /// delivered only if it exceeds its OWN session's mark — so a reconnect
    /// replay is deduped, a restarted sender (new session, seqs reset to 1) is
    /// recovered (its mark starts at 0), and a session that briefly reappears
    /// (oscillation A→B→A, or two senders on one listener) resumes its prior
    /// mark instead of resetting to 0 and re-delivering its whole ring as
    /// duplicates. Bounded to `SESSION_TRACK` (oldest evicted) so a churn of
    /// distinct sessions can't grow it without limit.
    sessions: Vec<(u64, u64)>,
    /// Set on drop to tell the reader thread to stop accepting.
    shutdown: Arc<AtomicBool>,
    /// The background reader thread, joined on drop. `Option` so `Drop` can
    /// `take()` it.
    reader: Option<JoinHandle<()>>,
}

impl EdgeListener {
    /// The address actually bound (useful when binding to port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Drop for EdgeListener {
    /// Stop the reader thread and release the bound port. Until Phase-1
    /// graceful shutdown actually dropped strands this could never run; now it
    /// does. The order matters:
    ///
    /// 1. Set the shutdown flag (`Release`, paired with the reader's `Acquire`
    ///    loads).
    /// 2. **Drop the receiver NOW** (`rx.take()`), disconnecting the channel so
    ///    a reader blocked in a full `tx.send()` unblocks immediately with
    ///    `Err`. Without this the receiver wouldn't drop until after `Drop`
    ///    returns, and the join below would deadlock against the blocked send.
    /// 3. Self-connect to the bound port to wake a reader blocked in
    ///    `accept()` so it observes the flag and returns.
    /// 4. Join the reader, which drops the `TcpListener` and frees the port.
    ///
    /// Promptness: a reader idle in `accept()` (the common case) or blocked on
    /// a full `send` tears down immediately. The one slow path is a reader
    /// mid-frame on a peer that is connected but silent: a blocking `read` can't
    /// be cancelled in safe `std`, so it returns only when the peer disconnects
    /// or at the `READ_TIMEOUT_SECS` (30s) slow-loris cap — so worst-case
    /// teardown is bounded by that timeout. (At a normal shutdown the peer
    /// disconnects, so this resolves at once.)
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        // (2) Disconnect the channel so a send-blocked reader unblocks now.
        drop(self.rx.take());
        // (3) Wake a blocked accept(). Connect to a loopback form of the bound
        // address (an unspecified 0.0.0.0/:: bind isn't directly connectable).
        let wake_addr = match self.local_addr {
            SocketAddr::V4(a) if a.ip().is_unspecified() => {
                SocketAddr::from((Ipv4Addr::LOCALHOST, a.port()))
            }
            SocketAddr::V6(a) if a.ip().is_unspecified() => {
                SocketAddr::from((Ipv6Addr::LOCALHOST, a.port()))
            }
            other => other,
        };
        let _ = TcpStream::connect_timeout(&wake_addr, Duration::from_millis(500));
        // (4) Join — the reader drops the TcpListener, freeing the port.
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
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
        // `rx` is always `Some` while the strand is alive; it is only taken in
        // `Drop`, after which `run` is never called again.
        let frames: Vec<Frame> = match &self.rx {
            Some(rx) => rx.try_iter().collect(),
            None => Vec::new(),
        };
        for (session, seq, tag, payload) in frames {
            // Resolve this frame to its OWN session's high-water, tracking a
            // newly-seen session (evicting the oldest if the bounded set is
            // full). A restarted sender arrives as a new session (mark 0, so its
            // reset seqs deliver — recovery, not blackout); a returning session
            // resumes its existing mark, so oscillation can't re-deliver a ring.
            let idx = match self.sessions.iter().position(|(s, _)| *s == session) {
                Some(i) => i,
                None => {
                    if self.sessions.len() >= SESSION_TRACK {
                        self.sessions.remove(0); // evict oldest-tracked session
                    }
                    self.sessions.push((session, 0));
                    self.sessions.len() - 1
                }
            };
            // Dedup: seqs are monotonic within a session, so a reconnect replay
            // re-sends already-seen seqs — skip any at or below this session's
            // mark.
            if seq <= self.sessions[idx].1 {
                continue;
            }
            self.sessions[idx].1 = seq;
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
/// disconnects; exits when the receiver (the strand) is dropped OR when
/// `shutdown` is set (the listener is being dropped — `Drop` self-connects to
/// wake a blocked `accept()` so this check is reached promptly).
fn reader_loop(listener: TcpListener, tx: mpsc::SyncSender<Frame>, shutdown: Arc<AtomicBool>) {
    for conn in listener.incoming() {
        if shutdown.load(Ordering::Acquire) {
            return; // listener dropped: release the port (drop `listener`).
        }
        let mut stream = match conn {
            Ok(s) => s,
            Err(_) => continue,
        };
        // Cap a mid-frame stall (slow-loris) so one wedged connection can't
        // block the reader (and thus the listener) forever.
        let _ = stream.set_read_timeout(Some(Duration::from_secs(READ_TIMEOUT_SECS)));
        // Read frames until EOF/error/timeout, then fall through to accept the
        // next connection. `send` blocks when the bounded channel is full
        // (backpressure) and returns Err when the receiver (the strand) is
        // dropped — which `EdgeListener::Drop` does up front, so a send-blocked
        // reader unblocks at teardown instead of deadlocking the join.
        while let Ok(frame) = read_frame(&mut stream) {
            if shutdown.load(Ordering::Acquire) || tx.send(frame).is_err() {
                return; // strand/listener dropped: stop.
            }
        }
    }
}

/// Write one wire frame to any byte sink. Generic over [`Write`] (not tied to
/// [`TcpStream`]) so frames can be built into an in-memory buffer for tests and
/// round-trip checks against [`read_frame`].
fn write_frame<W: Write>(
    w: &mut W,
    session: u64,
    seq: u64,
    tag: &str,
    payload: &[u8],
) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    let tag_b = tag.as_bytes();
    // Enforce the SAME caps the reader enforces, on the write side. The length
    // prefixes are `u32`; an oversized tag/payload would truncate on the cast
    // and desync the stream (the receiver mis-frames the next bytes). Refuse it
    // instead — defensive symmetry with read_frame's bounds checks.
    if tag_b.len() > MAX_TAG {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "edge: tag exceeds MAX_TAG",
        ));
    }
    if payload.len() > MAX_PAYLOAD {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "edge: payload exceeds MAX_PAYLOAD",
        ));
    }
    w.write_all(&session.to_be_bytes())?;
    w.write_all(&seq.to_be_bytes())?;
    w.write_all(&(tag_b.len() as u32).to_be_bytes())?;
    w.write_all(tag_b)?;
    w.write_all(&(payload.len() as u32).to_be_bytes())?;
    w.write_all(payload)?;
    w.flush()
}

/// Parse one wire frame from any byte source. Generic over [`Read`] (not tied
/// to [`TcpStream`]) so the untrusted-input parser can be unit-tested and
/// fuzzed in isolation against in-memory byte slices. Every length is bounds-
/// checked against its cap BEFORE allocating, so a hostile peer cannot induce
/// a huge allocation or a panic — malformed input always returns `Err`.
fn read_frame<R: Read>(r: &mut R) -> std::io::Result<Frame> {
    use std::io::{Error, ErrorKind};
    let mut s8 = [0u8; 8];
    r.read_exact(&mut s8)?;
    let session = u64::from_be_bytes(s8);
    let mut seq8 = [0u8; 8];
    r.read_exact(&mut seq8)?;
    let seq = u64::from_be_bytes(seq8);
    if seq == 0 {
        // 0 is the reserved "none yet" sentinel; a real frame's seq is >= 1.
        return Err(Error::new(ErrorKind::InvalidData, "seq 0 is reserved"));
    }
    let mut len4 = [0u8; 4];
    r.read_exact(&mut len4)?;
    let tag_len = u32::from_be_bytes(len4) as usize;
    if tag_len > MAX_TAG {
        return Err(Error::new(ErrorKind::InvalidData, "tag too long"));
    }
    let mut tag_b = vec![0u8; tag_len];
    r.read_exact(&mut tag_b)?;
    let tag =
        String::from_utf8(tag_b).map_err(|_| Error::new(ErrorKind::InvalidData, "tag not utf8"))?;

    r.read_exact(&mut len4)?;
    let payload_len = u32::from_be_bytes(len4) as usize;
    if payload_len > MAX_PAYLOAD {
        return Err(Error::new(ErrorKind::InvalidData, "payload too long"));
    }
    let mut payload = vec![0u8; payload_len];
    r.read_exact(&mut payload)?;
    Ok((session, seq, tag, payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use spiderweb::{register_clone_type, Bus};
    use std::sync::{Arc, Mutex};

    // ----- Wire-parser robustness (untrusted input) -------------------------
    //
    // `read_frame` parses bytes straight off the network. A hostile or buggy
    // peer must NEVER be able to make it panic or induce a huge allocation —
    // malformed input must always come back as `Err`. These tests drive the
    // (now `impl Read`-generic) parser against in-memory byte slices.

    /// Randomized fuzz of the untrusted wire parser: hammer `read_frame` with a
    /// huge number of adversarial byte streams (uniform-random AND
    /// header-shaped, where a plausible-but-hostile length prefix is the real
    /// attack) and assert it NEVER panics and NEVER violates its bounds. A
    /// std-only, deterministic, stable-Rust stand-in for cargo-fuzz so it runs
    /// in CI with no nightly toolchain or external deps. (A cargo-fuzz target is
    /// the documented optional follow-on for continuous fuzzing.)
    #[test]
    fn read_frame_never_panics_on_adversarial_input() {
        // xorshift64*: deterministic, zero-dep.
        let mut s: u64 = 0x9E3779B97F4A7C15;
        let mut next = || {
            s ^= s >> 12;
            s ^= s << 25;
            s ^= s >> 27;
            s.wrapping_mul(0x2545F4914F6CDD1D)
        };
        for iter in 0..200_000u32 {
            let mut bytes = Vec::new();
            if iter % 3 == 0 {
                // Header-shaped: real seq, then an ATTACKER-CHOSEN length prefix
                // (often enormous) — the case that must be rejected pre-alloc.
                bytes.extend_from_slice(&next().to_be_bytes()); // session
                bytes.extend_from_slice(&(1u64 + (next() & 0xFF)).to_be_bytes()); // seq
                bytes.extend_from_slice(&(next() as u32).to_be_bytes()); // tag_len
                let take = (next() % 40) as usize;
                for _ in 0..take {
                    bytes.push(next() as u8);
                }
                bytes.extend_from_slice(&(next() as u32).to_be_bytes()); // payload_len
                let take2 = (next() % 40) as usize;
                for _ in 0..take2 {
                    bytes.push(next() as u8);
                }
            } else {
                // Uniform-random of random length.
                let len = (next() % 64) as usize;
                for _ in 0..len {
                    bytes.push(next() as u8);
                }
            }
            let mut cur = std::io::Cursor::new(bytes);
            // The contract: returns, never panics. On success the bounds hold
            // (this is what protects against the giant-allocation attack).
            if let Ok((_session, seq, tag, payload)) = read_frame(&mut cur) {
                assert!(seq != 0, "seq 0 is reserved and must never parse");
                assert!(tag.len() <= MAX_TAG, "tag exceeded MAX_TAG");
                assert!(payload.len() <= MAX_PAYLOAD, "payload exceeded MAX_PAYLOAD");
            }
        }
    }

    /// A well-formed frame round-trips through write_frame → read_frame.
    #[test]
    fn frame_round_trips() {
        let mut buf = Vec::new();
        write_frame(&mut buf, 7, 42, "wire.note", b"hello").unwrap();
        let mut cur = std::io::Cursor::new(buf);
        let (session, seq, tag, payload) = read_frame(&mut cur).expect("valid frame should parse");
        assert_eq!(session, 7);
        assert_eq!(seq, 42);
        assert_eq!(tag, "wire.note");
        assert_eq!(payload, b"hello");
    }

    /// seq 0 is the reserved sentinel and must be rejected, not delivered.
    #[test]
    fn frame_rejects_reserved_seq_zero() {
        let mut buf = Vec::new();
        // Hand-build a seq-0 frame (write_frame would happily encode it).
        buf.extend_from_slice(&1u64.to_be_bytes()); // session
        buf.extend_from_slice(&0u64.to_be_bytes()); // seq (reserved 0)
        buf.extend_from_slice(&3u32.to_be_bytes());
        buf.extend_from_slice(b"abc");
        buf.extend_from_slice(&0u32.to_be_bytes());
        let mut cur = std::io::Cursor::new(buf);
        assert!(read_frame(&mut cur).is_err(), "seq 0 must be rejected");
    }

    /// A declared tag length past MAX_TAG must be rejected BEFORE allocating —
    /// the parser must not try to read/allocate a 4 GiB tag.
    #[test]
    fn frame_rejects_oversized_tag_without_allocating() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_be_bytes()); // session
        buf.extend_from_slice(&1u64.to_be_bytes()); // seq
                                                    // Declare a tag far larger than MAX_TAG; provide no tag bytes.
        buf.extend_from_slice(&(u32::MAX).to_be_bytes());
        let mut cur = std::io::Cursor::new(buf);
        let err = read_frame(&mut cur).expect_err("oversized tag must be rejected");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    /// A declared payload length past MAX_PAYLOAD must be rejected before
    /// allocating, even with a valid tag preceding it.
    #[test]
    fn frame_rejects_oversized_payload_without_allocating() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_be_bytes()); // session
        buf.extend_from_slice(&1u64.to_be_bytes()); // seq
        buf.extend_from_slice(&3u32.to_be_bytes());
        buf.extend_from_slice(b"abc");
        buf.extend_from_slice(&(u32::MAX).to_be_bytes()); // payload_len
        let mut cur = std::io::Cursor::new(buf);
        let err = read_frame(&mut cur).expect_err("oversized payload must be rejected");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    /// A tag whose bytes are not valid UTF-8 must be rejected, not lossily
    /// decoded.
    #[test]
    fn frame_rejects_non_utf8_tag() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_be_bytes()); // session
        buf.extend_from_slice(&1u64.to_be_bytes()); // seq
        buf.extend_from_slice(&2u32.to_be_bytes());
        buf.extend_from_slice(&[0xff, 0xfe]); // invalid UTF-8
        buf.extend_from_slice(&0u32.to_be_bytes());
        let mut cur = std::io::Cursor::new(buf);
        assert!(
            read_frame(&mut cur).is_err(),
            "non-utf8 tag must be rejected"
        );
    }

    /// A truncated frame (header promises more bytes than exist) must return
    /// `Err` from `read_exact`, never hang or panic.
    #[test]
    fn frame_rejects_truncation() {
        // Full header declaring a 10-byte payload, but only 2 bytes follow.
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_be_bytes()); // session
        buf.extend_from_slice(&1u64.to_be_bytes()); // seq
        buf.extend_from_slice(&3u32.to_be_bytes());
        buf.extend_from_slice(b"abc");
        buf.extend_from_slice(&10u32.to_be_bytes());
        buf.extend_from_slice(b"xy"); // only 2 of 10 payload bytes
        let mut cur = std::io::Cursor::new(buf);
        assert!(read_frame(&mut cur).is_err(), "truncated frame must error");
    }

    /// An empty stream (peer connected then sent nothing / closed) yields an
    /// EOF error, cleanly.
    #[test]
    fn frame_handles_empty_stream() {
        let mut cur = std::io::Cursor::new(Vec::<u8>::new());
        assert!(
            read_frame(&mut cur).is_err(),
            "empty stream must error, not hang"
        );
    }

    /// Max-size boundary: a tag of exactly MAX_TAG and a payload of exactly
    /// MAX_PAYLOAD are accepted (the bound is inclusive). Uses a small payload
    /// check at the tag boundary to keep the test fast.
    #[test]
    fn frame_accepts_max_tag_boundary() {
        let tag = "t".repeat(MAX_TAG);
        let mut buf = Vec::new();
        write_frame(&mut buf, 1, 1, &tag, b"").unwrap();
        let mut cur = std::io::Cursor::new(buf);
        let (_session, _seq, got_tag, _payload) =
            read_frame(&mut cur).expect("a tag of exactly MAX_TAG must be accepted");
        assert_eq!(got_tag.len(), MAX_TAG);
    }

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

    /// Dropping an `EdgeListener` must stop its reader thread and release the
    /// bound port. Before Phase-1 graceful shutdown dropped strands, the reader
    /// thread + bound port leaked until process exit. We bind a listener, drop
    /// it, and require the port to be re-bindable — which only succeeds once the
    /// reader thread has exited and dropped the `TcpListener`. `Drop` joins the
    /// reader synchronously (waking its blocked `accept()` via self-connect), so
    /// the port is free as soon as `drop` returns; the short retry only absorbs
    /// OS-level release lag.
    #[test]
    fn dropping_the_listener_releases_the_port() {
        let listener = Edge::new()
            .bridge::<String>("note", "wire.note", enc, dec)
            .listener("edge-in", "127.0.0.1:0")
            .expect("bind listener");
        let addr = listener.local_addr();

        // Sanity: the port is bound, so a second bind to it fails right now.
        assert!(
            TcpListener::bind(addr).is_err(),
            "the port should be in use while the listener is alive"
        );

        drop(listener);

        let mut rebound = false;
        for _ in 0..50 {
            if TcpListener::bind(addr).is_ok() {
                rebound = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(
            rebound,
            "the bound port must be released after the listener is dropped (reader thread joined)"
        );
    }

    /// Teardown must not deadlock when a peer is connected and has filled the
    /// bounded channel while nobody is draining it (the strand's `run` isn't
    /// being called). The reader blocks in `tx.send()`; `Drop` must drop the
    /// receiver FIRST so that send unblocks with `Err`, letting the join
    /// complete. A watchdog bounds the test so a regression shows up as a failed
    /// assertion, not a hung suite.
    #[test]
    fn dropping_the_listener_with_a_flooding_peer_does_not_deadlock() {
        let listener = Edge::new()
            .bridge::<String>("note", "wire.note", enc, dec)
            .listener("edge-in", "127.0.0.1:0")
            .expect("bind listener");
        let addr = listener.local_addr();

        // Connect a peer and flood far more than the channel cap. Nobody is
        // draining (the listener is not in a Bus), so the reader fills the
        // bounded channel and then blocks in tx.send().
        let mut peer = TcpStream::connect(addr).expect("connect peer");
        for i in 1..=(EDGE_CHANNEL_CAP as u64 + 50) {
            write_frame(&mut peer, 1, i, "note", b"x").expect("write frame");
        }
        // Let the reader pull frames into the now-full channel and block.
        std::thread::sleep(Duration::from_millis(100));

        // Drop on a watchdog thread; it must return promptly, not deadlock.
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            drop(listener);
            let _ = done_tx.send(());
        });
        assert!(
            done_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "dropping the listener with a full channel + connected peer must not deadlock"
        );
        drop(peer);
    }

    /// A sender RESTART (new session id, seqs reset to 1) must NOT black out. The
    /// receiver detects the new session and resets its dedup high-water, so the
    /// restarted sender's low seqs are delivered instead of skipped against the
    /// old session's mark. Without session ids, "b1"/"b2" (seq 1,2 <= old
    /// high-water 3) would be silently dropped — the multi-thousand-frame
    /// blackout this feature exists to kill.
    #[test]
    fn restarted_sender_recovers_instead_of_blacking_out() {
        use std::io::Write as _;
        let collected = Arc::new(Mutex::new(Vec::<String>::new()));
        let listener = Edge::new()
            .bridge::<String>("note", "wire.note", enc, dec)
            .listener("edge-in", "127.0.0.1:0")
            .expect("bind listener");
        let port = listener.local_addr().port();
        let c = collected.clone();
        let bus_thread = thread::spawn(move || {
            let mut bus = Bus::open();
            bus.register(listener);
            bus.register(Collector { sink: c });
            bus.run_until(Some(Duration::from_millis(900)));
        });
        thread::sleep(Duration::from_millis(80)); // let the reader thread start

        // Session A (the original sender): seqs 1,2,3 -> high-water 3.
        {
            let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect A");
            for (seq, p) in [(1u64, "a1"), (2, "a2"), (3, "a3")] {
                write_frame(&mut s, 100, seq, "note", p.as_bytes()).expect("write A");
            }
            let _ = s.flush();
        } // drop closes the connection — the sender "stops"
        thread::sleep(Duration::from_millis(120));

        // Session B (the RESTARTED sender): fresh session, seqs reset to 1,2.
        {
            let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect B");
            for (seq, p) in [(1u64, "b1"), (2, "b2")] {
                write_frame(&mut s, 200, seq, "note", p.as_bytes()).expect("write B");
            }
            let _ = s.flush();
        }
        thread::sleep(Duration::from_millis(220));
        bus_thread.join().unwrap();

        let mut got = collected.lock().unwrap().clone();
        assert!(
            got.contains(&"b1".to_string()) && got.contains(&"b2".to_string()),
            "restarted-sender frames were blacked out (session dedup-reset failed); got {got:?}"
        );
        got.sort();
        got.dedup();
        assert_eq!(
            got,
            vec!["a1", "a2", "a3", "b1", "b2"],
            "expected exactly the 5 messages across the restart"
        );
    }

    /// Session OSCILLATION must not re-deliver. After session A (1,2,3) then
    /// session B (1,2), session A RECONNECTS and replays its whole ring (1,2,3
    /// again) — the case where two senders share one listener, or a restart
    /// reuses an earlier session id. A single `(session, delivered)` pair would
    /// reset to 0 on the return to A and re-deliver a1/a2/a3 as DUPLICATES (an
    /// exactly-once violation). The per-session high-water remembers A's mark
    /// (3), so the replay delivers nothing new. Regression for the bounded
    /// per-session dedup map.
    #[test]
    fn oscillating_sessions_do_not_re_deliver_a_replayed_ring() {
        use std::io::Write as _;
        let collected = Arc::new(Mutex::new(Vec::<String>::new()));
        let listener = Edge::new()
            .bridge::<String>("note", "wire.note", enc, dec)
            .listener("edge-in", "127.0.0.1:0")
            .expect("bind listener");
        let port = listener.local_addr().port();
        let c = collected.clone();
        let bus_thread = thread::spawn(move || {
            let mut bus = Bus::open();
            bus.register(listener);
            bus.register(Collector { sink: c });
            bus.run_until(Some(Duration::from_millis(1200)));
        });
        thread::sleep(Duration::from_millis(80)); // let the reader thread start

        // Session A: seqs 1,2,3 -> high-water 3.
        {
            let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect A");
            for (seq, p) in [(1u64, "a1"), (2, "a2"), (3, "a3")] {
                write_frame(&mut s, 100, seq, "note", p.as_bytes()).expect("write A");
            }
            let _ = s.flush();
        }
        thread::sleep(Duration::from_millis(140));

        // Session B: seqs 1,2.
        {
            let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect B");
            for (seq, p) in [(1u64, "b1"), (2, "b2")] {
                write_frame(&mut s, 200, seq, "note", p.as_bytes()).expect("write B");
            }
            let _ = s.flush();
        }
        thread::sleep(Duration::from_millis(140));

        // Session A RECONNECTS and replays its entire ring (1,2,3 again).
        {
            let mut s = TcpStream::connect(("127.0.0.1", port)).expect("reconnect A");
            for (seq, p) in [(1u64, "a1"), (2, "a2"), (3, "a3")] {
                write_frame(&mut s, 100, seq, "note", p.as_bytes()).expect("rewrite A");
            }
            let _ = s.flush();
        }
        thread::sleep(Duration::from_millis(240));
        bus_thread.join().unwrap();

        let got = collected.lock().unwrap().clone();
        // The A-replay must add NOTHING: exactly the 5 distinct messages, no dup.
        assert_eq!(
            got.len(),
            5,
            "session oscillation re-delivered a replayed ring (duplicates): {got:?}"
        );
        let mut sorted = got;
        sorted.sort();
        assert_eq!(
            sorted,
            vec!["a1", "a2", "a3", "b1", "b2"],
            "expected exactly the 5 distinct messages, each once"
        );
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
            const S: [Socket; 2] = [
                Socket::new::<String>("wire.a"),
                Socket::new::<String>("wire.b"),
            ];
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
            SocketCollector {
                name,
                socket,
                socket_obj: Socket::new::<String>(socket),
                sink,
            }
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
        assert!(
            a.iter().any(|s| s == "msg-a"),
            "wire.a should receive msg-a, got {:?}",
            *a
        );
        assert!(
            !a.iter().any(|s| s == "msg-b"),
            "wire.a must NOT receive msg-b (cross-tag leak), got {:?}",
            *a
        );
        assert!(
            b.iter().any(|s| s == "msg-b"),
            "wire.b should receive msg-b, got {:?}",
            *b
        );
        assert!(
            !b.iter().any(|s| s == "msg-a"),
            "wire.b must NOT receive msg-a (cross-tag leak), got {:?}",
            *b
        );
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
