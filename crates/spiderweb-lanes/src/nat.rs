//! NAT-traversal lane via a rendezvous **relay**.
//!
//! Hole-punching needs a STUN/coordination dance and is unreliable; a relay is
//! the honest, std-only deliverable. Both peers **dial out** to a public relay
//! (outbound TCP traverses most NATs by construction) and join a numbered
//! *room*; the relay is a switchboard that forwards each peer's frames to the
//! other peers in the same room. No peer needs an inbound-reachable port.
//!
//! Two nested layers:
//! - **inner**: the edge wire frame, reused byte-for-byte ([`crate::frame`]) —
//!   so session/seq dedup, restart recovery, and A→B→A oscillation are handled
//!   exactly as in the edge lane; the relay forwards the inner frame untouched.
//! - **outer**: a tiny room envelope on the peer↔relay link — `JOIN` then a
//!   stream of `DATA` blobs (the relay treats each blob as opaque).
//!
//! Honest limits: the relay is a single point of failure and pays the bandwidth
//! of every federated message twice (in + out). It is a coordination/forwarding
//! convenience, not a trust boundary — the link is still plaintext (see the SSH
//! lane to encrypt it).

use crate::frame::{
    make_pull, make_push, new_session, read_frame, write_frame, Frame, Pull, Push,
    EDGE_CHANNEL_CAP, MAX_PAYLOAD, MAX_TAG, RING_CAP, SESSION_TRACK,
};
use spiderweb::{BusHandle, Socket, Strand, StrandError};
use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const KIND_JOIN: u8 = 0x01;
const KIND_DATA: u8 = 0x02;
const KIND_LEAVE: u8 = 0x03;
/// Outer cap on a DATA blob (an inner edge frame). The relay never parses the
/// inner frame, so this is its only size bound.
const MAX_ROOM_BLOB: usize = MAX_PAYLOAD + MAX_TAG + 64;
/// Bound a relay→peer forward write, so a slow/stuck target can't block the
/// forwarding peer's loop (and thus the whole room) indefinitely.
const RELAY_WRITE_TIMEOUT_SECS: u64 = 5;

/// Where a peer rendezvouses: the relay address, the room, and this peer's id.
#[derive(Clone, Debug)]
pub struct RoomAddr {
    pub relay: String,
    pub room: u64,
    pub peer_id: u64,
}

impl RoomAddr {
    pub fn new(relay: impl Into<String>, room: u64, peer_id: u64) -> Self {
        RoomAddr {
            relay: relay.into(),
            room,
            peer_id,
        }
    }
}

/// One decoded outer room message.
enum RoomMsg {
    Join { room: u64, peer_id: u64 },
    Data(Vec<u8>),
    Leave,
}

fn write_join<W: Write>(w: &mut W, room: u64, peer_id: u64) -> std::io::Result<()> {
    w.write_all(&[KIND_JOIN])?;
    w.write_all(&room.to_be_bytes())?;
    w.write_all(&peer_id.to_be_bytes())?;
    w.flush()
}

fn write_data<W: Write>(w: &mut W, blob: &[u8]) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    if blob.len() > MAX_ROOM_BLOB {
        return Err(Error::new(ErrorKind::InvalidInput, "room blob too large"));
    }
    w.write_all(&[KIND_DATA])?;
    w.write_all(&(blob.len() as u32).to_be_bytes())?;
    w.write_all(blob)?;
    w.flush()
}

/// Parse one outer room message. Every length is bounds-checked before
/// allocating (mirrors `read_frame`'s defensive style).
fn read_room_msg<R: Read>(r: &mut R) -> std::io::Result<RoomMsg> {
    use std::io::{Error, ErrorKind};
    let mut k = [0u8; 1];
    r.read_exact(&mut k)?;
    match k[0] {
        KIND_JOIN => {
            let mut b8 = [0u8; 8];
            r.read_exact(&mut b8)?;
            let room = u64::from_be_bytes(b8);
            r.read_exact(&mut b8)?;
            let peer_id = u64::from_be_bytes(b8);
            Ok(RoomMsg::Join { room, peer_id })
        }
        KIND_DATA => {
            let mut len4 = [0u8; 4];
            r.read_exact(&mut len4)?;
            let len = u32::from_be_bytes(len4) as usize;
            if len > MAX_ROOM_BLOB {
                return Err(Error::new(ErrorKind::InvalidData, "room blob too large"));
            }
            let mut blob = vec![0u8; len];
            r.read_exact(&mut blob)?;
            Ok(RoomMsg::Data(blob))
        }
        KIND_LEAVE => Ok(RoomMsg::Leave),
        _ => Err(Error::new(ErrorKind::InvalidData, "unknown room msg kind")),
    }
}

// ----------------------------------------------------------------------------
// The relay server
// ----------------------------------------------------------------------------

struct RelayPeer {
    peer_id: u64,
    /// A write-half clone of the peer's socket (the reader uses the read-half),
    /// so forwarding to this peer never deadlocks against its own blocked read.
    write: Arc<Mutex<TcpStream>>,
}

type Rooms = Arc<Mutex<HashMap<u64, Vec<RelayPeer>>>>;

/// A std-only rendezvous relay: accepts peers, reads their `JOIN`, and forwards
/// each `DATA` blob to every other peer in the same room. No bus involved.
pub struct Relay {
    listener: TcpListener,
}

impl Relay {
    pub fn bind(addr: impl ToSocketAddrs) -> std::io::Result<Self> {
        Ok(Relay {
            listener: TcpListener::bind(addr)?,
        })
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Run forever (used by the `relayd` binary).
    pub fn serve(self) -> std::io::Result<()> {
        self.serve_until(Arc::new(AtomicBool::new(false)))
    }

    /// Run until `stop` is set. After setting `stop`, connect once to the bound
    /// address to wake the blocked `accept()`.
    pub fn serve_until(self, stop: Arc<AtomicBool>) -> std::io::Result<()> {
        let rooms: Rooms = Arc::new(Mutex::new(HashMap::new()));
        for conn in self.listener.incoming() {
            if stop.load(Ordering::Acquire) {
                break;
            }
            let stream = match conn {
                Ok(s) => s,
                Err(_) => continue,
            };
            let rooms = rooms.clone();
            let stop = stop.clone();
            thread::spawn(move || handle_peer(stream, rooms, stop));
        }
        // Stop set: the detached per-peer reader threads are blocked in
        // read_room_msg on idle connections and won't observe `stop` until a
        // byte arrives. Shut each peer's socket down so they get EOF and exit
        // promptly instead of lingering.
        let guard = rooms.lock().unwrap_or_else(|p| p.into_inner());
        for peers in guard.values() {
            for p in peers {
                let s = p.write.lock().unwrap_or_else(|x| x.into_inner());
                let _ = s.shutdown(std::net::Shutdown::Both);
            }
        }
        Ok(())
    }
}

fn handle_peer(mut read_half: TcpStream, rooms: Rooms, stop: Arc<AtomicBool>) {
    // The first message must be JOIN.
    let (room, peer_id) = match read_room_msg(&mut read_half) {
        Ok(RoomMsg::Join { room, peer_id }) => (room, peer_id),
        _ => return,
    };
    let write_half = match read_half.try_clone() {
        Ok(w) => {
            // A write to a slow/stuck target must not block this peer's forward
            // loop forever; bound it (we leave the READ side blocking so a
            // merely-idle peer is not disconnected).
            let _ = w.set_write_timeout(Some(Duration::from_secs(RELAY_WRITE_TIMEOUT_SECS)));
            Arc::new(Mutex::new(w))
        }
        Err(_) => return,
    };
    rooms
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .entry(room)
        .or_default()
        .push(RelayPeer {
            peer_id,
            write: write_half.clone(),
        });

    loop {
        if stop.load(Ordering::Acquire) {
            break;
        }
        match read_room_msg(&mut read_half) {
            Ok(RoomMsg::Data(blob)) => {
                // Forward to every OTHER peer in the room.
                let targets: Vec<Arc<Mutex<TcpStream>>> = {
                    let guard = rooms.lock().unwrap_or_else(|p| p.into_inner());
                    guard
                        .get(&room)
                        .map(|peers| {
                            peers
                                .iter()
                                .filter(|p| p.peer_id != peer_id)
                                .map(|p| p.write.clone())
                                .collect()
                        })
                        .unwrap_or_default()
                };
                for w in targets {
                    let mut s = w.lock().unwrap_or_else(|p| p.into_inner());
                    let _ = write_data(&mut *s, &blob); // a dead peer is reaped on its own thread
                }
            }
            Ok(RoomMsg::Join { .. }) => {} // ignore a re-JOIN
            Ok(RoomMsg::Leave) | Err(_) => break,
        }
    }

    // Deregister this peer.
    let mut guard = rooms.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(peers) = guard.get_mut(&room) {
        peers.retain(|p| !Arc::ptr_eq(&p.write, &write_half));
        if peers.is_empty() {
            guard.remove(&room);
        }
    }
}

// ----------------------------------------------------------------------------
// The lane (sender + listener strands)
// ----------------------------------------------------------------------------

struct Entry {
    tag: &'static str,
    type_id: std::any::TypeId,
    sock: Socket,
    pull: Pull,
    push: Push,
}

/// Builder for a relay-federated lane. Same `.bridge()` shape as `Edge`.
#[derive(Default)]
pub struct NatLane {
    entries: Vec<Entry>,
}

impl NatLane {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bridge<T: 'static + Send + Clone>(
        mut self,
        tag: &'static str,
        socket: &'static str,
        encode: fn(&T) -> Vec<u8>,
        decode: fn(&[u8]) -> Option<T>,
    ) -> Self {
        self.entries.push(Entry {
            tag,
            type_id: std::any::TypeId::of::<T>(),
            sock: Socket::new::<T>(socket),
            pull: make_pull::<T>(socket, encode),
            push: make_push::<T>(socket, decode),
        });
        self
    }

    /// The egress strand: bus → relay (dials out, joins the room, sends frames).
    pub fn sender(self, name: &'static str, addr: RoomAddr) -> NatRelaySender {
        let mut socks = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut pulls = Vec::new();
        for e in self.entries {
            if seen.insert(e.type_id) {
                socks.push(e.sock);
            }
            pulls.push((e.tag, e.pull));
        }
        NatRelaySender {
            name,
            addr,
            stream: None,
            socks,
            pulls,
            session: new_session(),
            next_seq: 1,
            ring: VecDeque::new(),
        }
    }

    /// The ingress strand: relay → bus (dials out, joins the room, republishes).
    pub fn listener(self, name: &'static str, addr: RoomAddr) -> NatRelayListener {
        let mut socks = Vec::new();
        let mut pushes = Vec::new();
        for e in self.entries {
            socks.push(e.sock);
            pushes.push((e.tag, e.push));
        }
        let (tx, rx) = mpsc::sync_channel::<Frame>(EDGE_CHANNEL_CAP);
        let shutdown = Arc::new(AtomicBool::new(false));
        let conn: Arc<Mutex<Option<TcpStream>>> = Arc::new(Mutex::new(None));
        let reader = {
            let addr = addr.clone();
            let shutdown = shutdown.clone();
            let conn = conn.clone();
            thread::spawn(move || relay_reader(addr, tx, shutdown, conn))
        };
        NatRelayListener {
            name,
            rx: Some(rx),
            socks,
            pushes,
            sessions: Vec::new(),
            shutdown,
            conn,
            reader: Some(reader),
        }
    }
}

/// Egress strand: pull local traffic, dial the relay, JOIN, send room-wrapped
/// edge frames. Reuses the edge sender's ring/session/seq for restart recovery.
pub struct NatRelaySender {
    name: &'static str,
    addr: RoomAddr,
    stream: Option<TcpStream>,
    socks: Vec<Socket>,
    pulls: Vec<(&'static str, Pull)>,
    session: u64,
    next_seq: u64,
    ring: VecDeque<(u64, &'static str, Vec<u8>)>,
}

impl NatRelaySender {
    fn frame_blob(&self, seq: u64, tag: &str, payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        // write_frame only fails on an over-cap tag/payload; ignore (sender data).
        let _ = write_frame(&mut buf, self.session, seq, tag, payload);
        buf
    }
}

impl Strand for NatRelaySender {
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
        // Pull fresh local traffic, assign seqs, ring it.
        let mut fresh: Vec<(u64, &'static str, Vec<u8>)> = Vec::new();
        for (tag, pull) in &self.pulls {
            for payload in pull(bus) {
                let seq = self.next_seq;
                self.next_seq += 1;
                self.ring.push_back((seq, tag, payload.clone()));
                if self.ring.len() > RING_CAP {
                    self.ring.pop_front();
                }
                fresh.push((seq, tag, payload));
            }
        }

        if self.stream.is_none() {
            // (Re)dial + JOIN + replay the whole ring.
            if let Ok(mut s) = TcpStream::connect(&self.addr.relay) {
                let _ = s.set_write_timeout(Some(Duration::from_secs(5)));
                let mut ok = write_join(&mut s, self.addr.room, self.addr.peer_id).is_ok();
                if ok {
                    let ring: Vec<_> = self.ring.iter().cloned().collect();
                    for (seq, tag, payload) in ring {
                        let blob = self.frame_blob(seq, tag, &payload);
                        if write_data(&mut s, &blob).is_err() {
                            ok = false;
                            break;
                        }
                    }
                }
                if ok {
                    self.stream = Some(s);
                }
            }
        } else {
            // Live: send only this tick's fresh frames.
            let session = self.session;
            let mut ok = true;
            for (seq, tag, payload) in &fresh {
                let mut blob = Vec::new();
                let _ = write_frame(&mut blob, session, *seq, tag, payload);
                if write_data(self.stream.as_mut().unwrap(), &blob).is_err() {
                    ok = false;
                    break;
                }
            }
            if !ok {
                self.stream = None; // re-dial + replay next tick.
            }
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

/// Ingress strand: a background thread dials the relay, JOINs, and streams the
/// inner frames in; `run` republishes them with per-session dedup (verbatim the
/// edge listener guarantees, which the relay forwards transparently).
pub struct NatRelayListener {
    name: &'static str,
    rx: Option<Receiver<Frame>>,
    socks: Vec<Socket>,
    pushes: Vec<(&'static str, Push)>,
    sessions: Vec<(u64, u64)>,
    shutdown: Arc<AtomicBool>,
    conn: Arc<Mutex<Option<TcpStream>>>,
    reader: Option<JoinHandle<()>>,
}

impl Strand for NatRelayListener {
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
        let frames: Vec<Frame> = match &self.rx {
            Some(rx) => rx.try_iter().collect(),
            None => Vec::new(),
        };
        for (session, seq, tag, payload) in frames {
            let idx = match self.sessions.iter().position(|(s, _)| *s == session) {
                Some(i) => i,
                None => {
                    if self.sessions.len() >= SESSION_TRACK {
                        self.sessions.remove(0);
                    }
                    self.sessions.push((session, 0));
                    self.sessions.len() - 1
                }
            };
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

impl Drop for NatRelayListener {
    fn drop(&mut self) {
        // The reader blocks reading its OUTBOUND relay stream (no accept to
        // wake). Set the flag, then shut the socket down to unblock the read.
        self.shutdown.store(true, Ordering::Release);
        if let Some(s) = self.conn.lock().unwrap_or_else(|p| p.into_inner()).take() {
            let _ = s.shutdown(std::net::Shutdown::Both);
        }
        drop(self.rx.take());
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

fn relay_reader(
    addr: RoomAddr,
    tx: SyncSender<Frame>,
    shutdown: Arc<AtomicBool>,
    conn: Arc<Mutex<Option<TcpStream>>>,
) {
    while !shutdown.load(Ordering::Acquire) {
        // Bounded connect so a dead/firewalled relay can't block this thread (and
        // thus the listener's Drop join) for the OS default connect timeout.
        let mut s = match addr
            .relay
            .to_socket_addrs()
            .ok()
            .and_then(|mut a| a.next())
            .and_then(|sa| TcpStream::connect_timeout(&sa, Duration::from_secs(2)).ok())
        {
            Some(s) => s,
            None => {
                thread::sleep(Duration::from_millis(200));
                continue;
            }
        };
        // Stash a clone so Drop can shut the socket down to unblock our read.
        if let Ok(c) = s.try_clone() {
            *conn.lock().unwrap_or_else(|p| p.into_inner()) = Some(c);
        }
        if write_join(&mut s, addr.room, addr.peer_id).is_err() {
            continue;
        }
        // Close the Drop-vs-conn-store race: if shutdown was set between storing
        // `conn` and here, exit now instead of blocking in the read below (where
        // Drop's shutdown(conn) might have already fired against the old None).
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        loop {
            match read_room_msg(&mut s) {
                Ok(RoomMsg::Data(blob)) => {
                    if let Ok(frame) = read_frame(&mut std::io::Cursor::new(blob)) {
                        if tx.send(frame).is_err() {
                            return; // strand dropped.
                        }
                    }
                }
                Ok(_) => {}
                Err(_) => break, // EOF/shutdown: reconnect (or exit if flagged).
            }
            if shutdown.load(Ordering::Acquire) {
                return;
            }
        }
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

    struct Source;
    impl Strand for Source {
        fn name(&self) -> &str {
            "src"
        }
        fn inputs(&self) -> &[Socket] {
            &[]
        }
        fn outputs(&self) -> &[Socket] {
            const S: Socket = Socket::new::<String>("wire.note");
            std::slice::from_ref(&S)
        }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            bus.publish_static("wire.note", "hello-via-relay".to_string())?;
            bus.sleep(Duration::from_millis(40));
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
            const S: Socket = Socket::new::<String>("wire.note");
            std::slice::from_ref(&S)
        }
        fn outputs(&self) -> &[Socket] {
            &[]
        }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            for m in bus.recv::<String>("wire.note").unwrap_or_default() {
                self.sink.lock().unwrap().push(m);
            }
            bus.sleep(Duration::from_millis(10));
            Ok(())
        }
    }

    /// Two buses, each dialing OUT to a relay in the same room, federate a
    /// message across it — the NAT-traversal deliverable end to end.
    #[test]
    fn message_federates_through_the_relay() {
        let relay = Relay::bind("127.0.0.1:0").expect("bind relay");
        let relay_addr = relay.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_c = stop.clone();
        let relay_thread = thread::spawn(move || {
            let _ = relay.serve_until(stop_c);
        });

        let collected = Arc::new(StdMutex::new(Vec::<String>::new()));
        let room = 42u64;

        let c = collected.clone();
        let relay_r = relay_addr.to_string();
        let recv = thread::spawn(move || {
            register_clone_type::<String>();
            let mut bus = Bus::open();
            bus.register(
                NatLane::new()
                    .bridge::<String>("note", "wire.note", enc, dec)
                    .listener("nat-in", RoomAddr::new(relay_r, room, 2)),
            );
            bus.register(Collector { sink: c });
            bus.run_until(Some(Duration::from_millis(1000)));
        });

        let relay_s = relay_addr.to_string();
        let send = thread::spawn(move || {
            register_clone_type::<String>();
            let mut bus = Bus::open();
            bus.register(Source);
            bus.register(
                NatLane::new()
                    .bridge::<String>("note", "wire.note", enc, dec)
                    .sender("nat-out", RoomAddr::new(relay_s, room, 1)),
            );
            bus.run_until(Some(Duration::from_millis(1000)));
        });

        send.join().unwrap();
        recv.join().unwrap();
        stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(relay_addr); // wake accept() so the relay exits
        let _ = relay_thread.join();

        let got = collected.lock().unwrap().clone();
        assert!(
            got.iter().any(|s| s == "hello-via-relay"),
            "bus B should receive A's message via the relay; got {got:?}"
        );
    }

    /// The relay's outer parser never panics on adversarial bytes and honors its
    /// blob cap (ported style of edge's read_frame fuzz).
    #[test]
    fn read_room_msg_never_panics_on_adversarial_input() {
        let mut s: u64 = 0xD1B54A32D192ED03;
        let mut next = || {
            s ^= s >> 12;
            s ^= s << 25;
            s ^= s >> 27;
            s.wrapping_mul(0x2545F4914F6CDD1D)
        };
        for _ in 0..100_000u32 {
            let len = (next() % 48) as usize;
            let mut bytes = Vec::with_capacity(len);
            for _ in 0..len {
                bytes.push(next() as u8);
            }
            let mut cur = std::io::Cursor::new(bytes);
            if let Ok(RoomMsg::Data(blob)) = read_room_msg(&mut cur) {
                assert!(blob.len() <= MAX_ROOM_BLOB);
            }
        }
    }
}
