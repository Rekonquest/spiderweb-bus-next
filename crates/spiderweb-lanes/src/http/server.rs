//! HTTP server lane: turn inbound HTTP/1.1 requests into bus messages and write
//! bus responses back to the right connection.
//!
//! Two strands share one bounded pending table:
//! - [`HttpIngress`] owns the acceptor thread, which serves each connection on
//!   its own thread: a persistent reader parses successive requests off a
//!   keep-alive connection, parks a write-half clone per request keyed by a
//!   globally-unique `reply_id`, and publishes each [`HttpRequest`] on the bus.
//! - [`HttpResponder`] subscribes to [`HttpResponse`], pops the matching
//!   write-half out of the table, and writes the HTTP/1.1 reply on a worker pool
//!   (off the strand tick) — keeping the connection open for the next request
//!   (keep-alive) or closing it (`Connection: close` / HTTP idle timeout).
//!
//! Persistent connections are supported (HTTP/1.1 default keep-alive). One
//! request is in flight per connection at a time (no pipelined overlap); the
//! `Content-Length` on every response lets the client find the boundary.

use super::wire::{parse_request, write_response, HttpRequest, HttpResponse};
use highway::Highway;
use spiderweb::{register_clone_type, BusHandle, Socket, Strand, StrandError};
use std::collections::{HashMap, VecDeque};
use std::io::BufReader;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Cap a slow client that opens a connection then stalls mid-request, AND a
/// slow client that stops reading its response (a write to a full send buffer
/// would otherwise hang the responder tick forever — the kernel has no per-tick
/// watchdog, so a blocked `run()` wedges that strand permanently).
const READ_TIMEOUT_SECS: u64 = 30;
/// Bound the accept-thread → strand handoff (backpressure, like the edge lane).
const ACCEPT_CHANNEL_CAP: usize = 256;
/// Cap connections parked awaiting a handler reply, so a flood (or a handler
/// that never answers some `reply_id`) can't hold unbounded open sockets.
const MAX_PENDING: usize = 1024;
/// Reap a parked connection whose handler never replied after this long, so
/// orphans can't accumulate to `MAX_PENDING` and permanently 503 every new
/// request. Set well beyond any reasonable handler latency.
const PENDING_TTL: Duration = Duration::from_secs(120);

/// One in-flight request, keyed by `reply_id`: a WRITE-half clone of the
/// connection, whether to keep it open after the reply (keep-alive), when it was
/// parked (for the reaper), and a one-shot `done` sender fired once the reply is
/// written — the connection thread waits on it before reading the next request,
/// so exactly one request is in flight per connection and responses can't
/// reorder (pipelining safety). Shared by the per-connection reader threads
/// (insert) and the responder (remove + reap).
type Pending = Arc<Mutex<HashMap<u64, (TcpStream, bool, Instant, Sender<()>)>>>;

/// How many response writes run concurrently off the responder tick. Response
/// writes can BLOCK (a client that stops reading fills the send buffer), so they
/// must not run inside `run()` — the kernel has no per-tick watchdog, and even a
/// timed-out write would head-of-line-block every other client's reply. A worker
/// pool decouples a slow consumer from delivery to everyone else.
const RESPONDER_LANES: usize = 8;

/// A queued response write: send `resp` on `stream`, keeping the connection open
/// iff `keep_alive`, then fire `done` so the connection thread may read the next
/// request.
type WriteJob = (TcpStream, HttpResponse, bool, Sender<()>);

/// The blocking write, run on a [`Highway`] worker (off the strand tick). The
/// stream carries a write timeout (set per connection), so a stuck client bounds
/// a worker, never the tick. The write-half clone drops here; the socket stays
/// open while the connection's read-half lives (keep-alive) and closes once all
/// handles drop (Connection: close → the reader has already stopped). Firing
/// `done` (after the write completes) is what releases the connection thread to
/// read its next request — keeping exactly one reply in flight, in order.
fn write_job((mut stream, resp, keep_alive, done): WriteJob) {
    let _ = write_response(&mut stream, &resp, keep_alive);
    let _ = done.send(());
}

/// Builder for the server side. `req_socket` is where parsed requests are
/// published; `resp_socket` is where handler replies are read. One `HttpServer`
/// yields a matched [`HttpIngress`]/[`HttpResponder`] pair that share state.
pub struct HttpServer {
    req_socket: &'static str,
    resp_socket: &'static str,
    pending: Pending,
}

impl HttpServer {
    pub fn new(req_socket: &'static str, resp_socket: &'static str) -> Self {
        // Register the lane's own bus types so publishing them can never hit the
        // unregistered-fan-out-type crash.
        register_clone_type::<HttpRequest>();
        register_clone_type::<HttpResponse>();
        HttpServer {
            req_socket,
            resp_socket,
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Bind `addr` now (the port is known before any client connects, like
    /// `Edge::listener`), spawn the accept/parse thread, and return the ingress
    /// strand that publishes parsed requests.
    pub fn ingress(&self, addr: impl ToSocketAddrs) -> std::io::Result<HttpIngress> {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        let (tx, rx) = mpsc::sync_channel::<HttpRequest>(ACCEPT_CHANNEL_CAP);
        let shutdown = Arc::new(AtomicBool::new(false));
        let pending = self.pending.clone();
        let sd = shutdown.clone();
        let accept = thread::spawn(move || acceptor(listener, tx, pending, sd));
        Ok(HttpIngress {
            req_socket: self.req_socket,
            out_socks: vec![Socket::new::<HttpRequest>(self.req_socket)],
            rx: Some(rx),
            local_addr,
            shutdown,
            accept: Some(accept),
        })
    }

    /// The responder strand: writes each [`HttpResponse`] back to the connection
    /// whose request carried the same `reply_id` — on a worker pool, off-tick.
    pub fn responder(&self) -> HttpResponder {
        HttpResponder {
            resp_socket: self.resp_socket,
            in_socks: vec![Socket::new::<HttpResponse>(self.resp_socket)],
            pending: self.pending.clone(),
            pool: Highway::new(RESPONDER_LANES, write_job),
            backlog: VecDeque::new(),
        }
    }
}

/// Accept connections and serve EACH on its own thread, so one keep-alive
/// connection (which lingers idle between requests) can't block accepting
/// others. `reply_id`s are minted from one shared counter so they are unique
/// across all connections (the responder routes globally by `reply_id`).
fn acceptor(
    listener: TcpListener,
    tx: SyncSender<HttpRequest>,
    pending: Pending,
    shutdown: Arc<AtomicBool>,
) {
    let next_id = Arc::new(AtomicU64::new(1));
    for conn in listener.incoming() {
        if shutdown.load(Ordering::Acquire) {
            return; // listener dropped: release the port.
        }
        let stream = match conn {
            Ok(s) => s,
            Err(_) => continue,
        };
        let tx = tx.clone();
        let pending = pending.clone();
        let shutdown = shutdown.clone();
        let next_id = next_id.clone();
        thread::spawn(move || serve_connection(stream, tx, pending, shutdown, next_id));
    }
}

/// Serve one connection: parse requests off a persistent reader until the client
/// closes (EOF), asks to close (`Connection: close`), or goes idle past the read
/// timeout (the keep-alive idle bound). Each request parks a fresh write-half
/// clone keyed by a globally-unique `reply_id`; the responder writes + drops it.
fn serve_connection(
    stream: TcpStream,
    tx: SyncSender<HttpRequest>,
    pending: Pending,
    shutdown: Arc<AtomicBool>,
    next_id: Arc<AtomicU64>,
) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(READ_TIMEOUT_SECS)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(READ_TIMEOUT_SECS)));
    // Read on a private clone wrapped in a persistent BufReader (so bytes
    // buffered between requests aren't lost); each parked write uses its own
    // clone, so concurrent read (here) + write (responder pool) never conflict.
    let read_half = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut reader = BufReader::new(read_half);
    loop {
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        let reply_id = next_id.fetch_add(1, Ordering::Relaxed);
        let req = match parse_request(&mut reader, reply_id) {
            Ok(r) => r,
            Err(_) => return, // EOF / idle-timeout / malformed: close.
        };
        let keep_alive = req.wants_keep_alive();
        let mut write_half = match stream.try_clone() {
            Ok(s) => s,
            Err(_) => return,
        };
        let (done_tx, done_rx) = mpsc::channel::<()>();
        // Bound the parked table: refuse (503) rather than grow it.
        {
            let mut map = pending.lock().unwrap_or_else(|p| p.into_inner());
            if map.len() >= MAX_PENDING {
                let _ =
                    write_response(&mut write_half, &HttpResponse::status(reply_id, 503), false);
                return; // shed load by closing this connection
            }
            map.insert(reply_id, (write_half, keep_alive, Instant::now(), done_tx));
        }
        // Hand the request to the strand. If the receiver is gone (teardown) or
        // we're shutting down, reclaim the parked write-half and stop.
        if shutdown.load(Ordering::Acquire) || tx.send(req).is_err() {
            pending
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&reply_id);
            return;
        }
        if !keep_alive {
            return; // client asked to close after this request.
        }
        // Keep-alive: wait until THIS reply has been written before reading the
        // next request, so only one reply is in flight per connection (responses
        // can't reorder) and a pipelining client's buffered request is parsed in
        // order. A reply that never comes within the idle bound closes the conn.
        if done_rx
            .recv_timeout(Duration::from_secs(READ_TIMEOUT_SECS))
            .is_err()
        {
            return;
        }
    }
}

/// Ingress strand: drains parsed requests off the accept thread and publishes
/// them on the bus. Never blocks (the blocking reads live in the accept thread).
pub struct HttpIngress {
    req_socket: &'static str,
    out_socks: Vec<Socket>,
    rx: Option<Receiver<HttpRequest>>,
    local_addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
}

impl HttpIngress {
    /// The address actually bound (resolves a `:0` ephemeral port).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Strand for HttpIngress {
    fn name(&self) -> &str {
        "http-ingress"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        &self.out_socks
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        let reqs: Vec<HttpRequest> = match &self.rx {
            Some(rx) => rx.try_iter().collect(),
            None => Vec::new(),
        };
        for req in reqs {
            let _ = bus.publish_static(self.req_socket, req);
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

impl Drop for HttpIngress {
    fn drop(&mut self) {
        // Same teardown order as EdgeListener: flag, drop rx to unblock a
        // send-blocked accept thread, self-connect to wake a blocked accept(),
        // join (which drops the listener and frees the port).
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
        if let Some(accept) = self.accept.take() {
            let _ = accept.join();
        }
    }
}

/// Responder strand: writes each [`HttpResponse`] back to the parked connection
/// with the matching `reply_id`, then closes it (Connection: close).
pub struct HttpResponder {
    resp_socket: &'static str,
    in_socks: Vec<Socket>,
    pending: Pending,
    /// Worker pool that performs the (potentially blocking) response writes off
    /// the strand tick, so one slow consumer can't stall delivery to others.
    pool: Highway<WriteJob, ()>,
    /// Responses waiting for a free pool lane (bounded by in-flight count).
    backlog: VecDeque<WriteJob>,
}

impl Strand for HttpResponder {
    fn name(&self) -> &str {
        "http-responder"
    }
    fn inputs(&self) -> &[Socket] {
        &self.in_socks
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        // Reap orphaned parked connections (handler never answered) so they
        // can't accumulate to MAX_PENDING and permanently 503 new requests; the
        // 504 writes go through the same off-tick pool.
        for job in self.reap_stale(Instant::now()) {
            self.backlog.push_back(job);
        }

        // Match each response to its parked connection and queue the write.
        for resp in bus
            .recv::<HttpResponse>(self.resp_socket)
            .unwrap_or_default()
        {
            let parked = self
                .pending
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&resp.reply_id);
            if let Some((stream, keep_alive, _, done)) = parked {
                self.backlog.push_back((stream, resp, keep_alive, done));
            }
            // An unknown reply_id (already answered / timed out) is dropped.
        }

        // Feed the write pool while it has room — never block the tick.
        while self.pool.has_room() {
            match self.backlog.pop_front() {
                Some(job) => {
                    if self.pool.on_ramp(job).is_err() {
                        break;
                    }
                }
                None => break,
            }
        }
        let _ = self.pool.off_ramp(); // drain completions

        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

impl HttpResponder {
    /// Remove parked connections older than [`PENDING_TTL`] (a handler that never
    /// answered) and return a 504 write job for each, so the writes go off-tick
    /// like all others. Takes `now` so it is unit-testable without a real clock.
    fn reap_stale(&self, now: Instant) -> Vec<WriteJob> {
        let stale: Vec<u64> = {
            let map = self.pending.lock().unwrap_or_else(|p| p.into_inner());
            map.iter()
                .filter(|(_, (_, _, t, _))| now.duration_since(*t) >= PENDING_TTL)
                .map(|(id, _)| *id)
                .collect()
        };
        let mut jobs = Vec::new();
        for id in stale {
            let entry = self
                .pending
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&id);
            if let Some((stream, _, _, done)) = entry {
                jobs.push((stream, HttpResponse::status(id, 504), false, done));
            }
        }
        jobs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A parked connection whose handler never replies is reaped after the TTL,
    /// so orphans can't fill MAX_PENDING and permanently 503 the server.
    #[test]
    fn orphaned_parked_connections_are_reaped() {
        let server = HttpServer::new("http.req", "http.resp");
        let responder = server.responder();

        // Park a real, connected server-side stream (keep the client end alive).
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = TcpStream::connect(addr).unwrap();
        let (parked, _) = listener.accept().unwrap();
        let t0 = Instant::now();
        let (done_tx, _done_rx) = mpsc::channel::<()>();
        responder
            .pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(7, (parked, false, t0, done_tx));

        responder.reap_stale(t0); // fresh — kept
        assert_eq!(
            responder
                .pending
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .len(),
            1
        );

        responder.reap_stale(t0 + PENDING_TTL + Duration::from_secs(1)); // stale — reaped
        assert!(responder
            .pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_empty());
    }

    /// A client that requests a large body then never reads it must NOT stall
    /// delivery to other clients — response writes run off the strand tick on a
    /// worker pool, so one stuck consumer occupies a lane, never the responder.
    #[test]
    fn a_slow_client_does_not_stall_a_fast_one() {
        use spiderweb::Bus;
        use std::io::{Read, Write};

        let server = HttpServer::new("slow.req", "slow.resp");
        let ingress = server.ingress("127.0.0.1:0").expect("bind");
        let port = ingress.local_addr().port();
        let responder = server.responder();

        // Handler: /big → a 2MB body (exceeds the loopback send buffer, so a
        // non-reading client blocks the write); anything else → "ok".
        struct Handler;
        impl Strand for Handler {
            fn name(&self) -> &str {
                "handler"
            }
            fn inputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<HttpRequest>("slow.req");
                std::slice::from_ref(&S)
            }
            fn outputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<HttpResponse>("slow.resp");
                std::slice::from_ref(&S)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                for r in bus.recv::<HttpRequest>("slow.req").unwrap_or_default() {
                    let body = if r.path == "/big" {
                        vec![b'x'; 2_000_000]
                    } else {
                        b"ok".to_vec()
                    };
                    let _ = bus.publish_static(
                        "slow.resp",
                        HttpResponse {
                            reply_id: r.reply_id,
                            status: 200,
                            headers: Vec::new(),
                            body,
                        },
                    );
                }
                bus.sleep(Duration::from_millis(5));
                Ok(())
            }
        }

        let bus_thread = thread::spawn(move || {
            let mut bus = Bus::open();
            bus.register(ingress);
            bus.register(responder);
            bus.register(Handler);
            bus.run_until(Some(Duration::from_millis(2500)));
        });
        thread::sleep(Duration::from_millis(80));

        // Slow client: ask for the big body, then never read it.
        let mut slow = TcpStream::connect(("127.0.0.1", port)).expect("connect slow");
        slow.write_all(b"GET /big HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .unwrap();
        slow.flush().unwrap();
        thread::sleep(Duration::from_millis(150));

        // Fast client: must still be served promptly.
        let mut fast = TcpStream::connect(("127.0.0.1", port)).expect("connect fast");
        fast.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        fast.write_all(b"GET /small HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .unwrap();
        fast.flush().unwrap();
        let mut buf = Vec::new();
        let _ = fast.read_to_end(&mut buf); // bounded by the read timeout

        // Release the stuck worker before shutdown (so the pool join is prompt).
        drop(slow);
        bus_thread.join().unwrap();

        let text = String::from_utf8_lossy(&buf);
        assert!(
            text.starts_with("HTTP/1.1 200"),
            "the fast client must be served despite a stalled slow client; got {text:?}"
        );
        assert!(text.ends_with("ok"), "fast client body, got {text:?}");
    }

    /// Read exactly one HTTP response (status line + headers + Content-Length
    /// body) off a persistent reader, without consuming the next response.
    fn read_http_response<R: std::io::BufRead>(r: &mut R) -> String {
        let mut head = String::new();
        loop {
            let mut line = String::new();
            if r.read_line(&mut line).unwrap() == 0 {
                break; // EOF
            }
            head.push_str(&line);
            if line == "\r\n" {
                break; // end of headers
            }
        }
        let len: usize = head
            .lines()
            .find_map(|l| {
                let low = l.to_ascii_lowercase();
                low.strip_prefix("content-length:")
                    .and_then(|v| v.trim().parse().ok())
            })
            .unwrap_or(0);
        let mut body = vec![0u8; len];
        std::io::Read::read_exact(r, &mut body).unwrap();
        format!("{head}{}", String::from_utf8_lossy(&body))
    }

    /// TWO requests on ONE connection are both answered without reconnecting —
    /// HTTP/1.1 keep-alive end to end. The first defaults to keep-alive; the
    /// second asks to close.
    #[test]
    fn one_connection_serves_two_keepalive_requests() {
        use spiderweb::Bus;
        use std::io::{BufReader, Write};

        let server = HttpServer::new("ka.req", "ka.resp");
        let ingress = server.ingress("127.0.0.1:0").expect("bind");
        let port = ingress.local_addr().port();
        let responder = server.responder();

        struct Echo;
        impl Strand for Echo {
            fn name(&self) -> &str {
                "echo"
            }
            fn inputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<HttpRequest>("ka.req");
                std::slice::from_ref(&S)
            }
            fn outputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<HttpResponse>("ka.resp");
                std::slice::from_ref(&S)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                for r in bus.recv::<HttpRequest>("ka.req").unwrap_or_default() {
                    let _ = bus.publish_static(
                        "ka.resp",
                        HttpResponse::text(r.reply_id, format!("echo:{}", r.path)),
                    );
                }
                bus.sleep(Duration::from_millis(5));
                Ok(())
            }
        }

        let bus_thread = thread::spawn(move || {
            let mut bus = Bus::open();
            bus.register(ingress);
            bus.register(responder);
            bus.register(Echo);
            bus.run_until(Some(Duration::from_millis(2500)));
        });
        thread::sleep(Duration::from_millis(80));

        let conn = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        conn.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut writer = conn.try_clone().unwrap();
        let mut reader = BufReader::new(conn);

        // Request 1 — keep-alive (HTTP/1.1 default).
        writer
            .write_all(b"GET /one HTTP/1.1\r\nHost: x\r\n\r\n")
            .unwrap();
        let r1 = read_http_response(&mut reader);
        assert!(r1.ends_with("echo:/one"), "first reply: {r1:?}");
        assert!(
            r1.contains("Connection: keep-alive"),
            "first reply should keep the connection open: {r1:?}"
        );

        // Request 2 on the SAME connection — asks to close.
        writer
            .write_all(b"GET /two HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .unwrap();
        let r2 = read_http_response(&mut reader);
        assert!(
            r2.ends_with("echo:/two"),
            "second reply on same conn: {r2:?}"
        );
        assert!(r2.contains("Connection: close"), "second reply: {r2:?}");

        bus_thread.join().unwrap();
    }

    /// A PIPELINING client (sends both requests before reading) must get its
    /// responses IN ORDER. The first reply is large (slow write); under the old
    /// in-order-not-guaranteed design the small second reply could land first and
    /// desync the stream. Per-connection serialization makes ordering impossible
    /// to violate.
    #[test]
    fn pipelined_requests_get_in_order_responses() {
        use spiderweb::Bus;
        use std::io::{BufReader, Write};

        let server = HttpServer::new("pl.req", "pl.resp");
        let ingress = server.ingress("127.0.0.1:0").expect("bind");
        let port = ingress.local_addr().port();
        let responder = server.responder();

        struct Sized;
        impl Strand for Sized {
            fn name(&self) -> &str {
                "sized"
            }
            fn inputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<HttpRequest>("pl.req");
                std::slice::from_ref(&S)
            }
            fn outputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<HttpResponse>("pl.resp");
                std::slice::from_ref(&S)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                for r in bus.recv::<HttpRequest>("pl.req").unwrap_or_default() {
                    // /first → a large body (slow write); else → tiny.
                    let body = if r.path == "/first" {
                        vec![b'A'; 600_000]
                    } else {
                        b"second".to_vec()
                    };
                    let _ = bus.publish_static(
                        "pl.resp",
                        HttpResponse {
                            reply_id: r.reply_id,
                            status: 200,
                            headers: Vec::new(),
                            body,
                        },
                    );
                }
                bus.sleep(Duration::from_millis(5));
                Ok(())
            }
        }

        let bus_thread = thread::spawn(move || {
            let mut bus = Bus::open();
            bus.register(ingress);
            bus.register(responder);
            bus.register(Sized);
            bus.run_until(Some(Duration::from_millis(3000)));
        });
        thread::sleep(Duration::from_millis(80));

        let conn = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        conn.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        let mut writer = conn.try_clone().unwrap();
        let mut reader = BufReader::new(conn);

        // Pipeline BOTH requests before reading either response.
        writer
            .write_all(b"GET /first HTTP/1.1\r\nHost: x\r\n\r\n")
            .unwrap();
        writer
            .write_all(b"GET /second HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .unwrap();
        writer.flush().unwrap();

        // Responses MUST arrive in request order: the large /first, then /second.
        let r1 = read_http_response(&mut reader);
        assert!(
            r1.contains("Content-Length: 600000"),
            "first response must be the large /first reply, not the small one: {:?}",
            &r1[..r1.len().min(120)]
        );
        let r2 = read_http_response(&mut reader);
        assert!(r2.ends_with("second"), "second response: {r2:?}");

        bus_thread.join().unwrap();
    }
}
