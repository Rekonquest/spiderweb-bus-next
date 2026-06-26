//! HTTP server lane: turn inbound HTTP/1.1 requests into bus messages and write
//! bus responses back to the right connection.
//!
//! Two strands share one bounded pending-socket table:
//! - [`HttpIngress`] owns the accept/parse thread (the `EdgeListener` pattern):
//!   it parks each still-open connection in the table keyed by a `reply_id` and
//!   publishes the parsed [`HttpRequest`] on the bus.
//! - [`HttpResponder`] subscribes to [`HttpResponse`], pops the matching
//!   connection out of the table, writes the HTTP/1.1 reply, and closes it.
//!
//! MVP scope: `Connection: close` (one request per connection), so correlation
//! is exactly the `reply_id`. Keep-alive pipelining is a documented follow-on.

use super::wire::{parse_request, write_response, HttpRequest, HttpResponse};
use spiderweb::{register_clone_type, BusHandle, Socket, Strand, StrandError};
use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
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

/// Connections parked between request-publish and response-write, keyed by
/// `reply_id`, each stamped with when it was parked (for the reaper). Shared by
/// the accept thread (inserts) and the responder (removes + reaps).
type Pending = Arc<Mutex<HashMap<u64, (TcpStream, Instant)>>>;

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
        let accept = thread::spawn(move || accept_loop(listener, tx, pending, sd));
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
    /// whose request carried the same `reply_id`.
    pub fn responder(&self) -> HttpResponder {
        HttpResponder {
            resp_socket: self.resp_socket,
            in_socks: vec![Socket::new::<HttpResponse>(self.resp_socket)],
            pending: self.pending.clone(),
        }
    }
}

/// Accept connections; parse one request per connection (Connection: close);
/// park the still-open socket in `pending` and hand the request to the strand.
fn accept_loop(
    listener: TcpListener,
    tx: SyncSender<HttpRequest>,
    pending: Pending,
    shutdown: Arc<AtomicBool>,
) {
    let mut next_id: u64 = 1;
    for conn in listener.incoming() {
        if shutdown.load(Ordering::Acquire) {
            return; // listener dropped: release the port.
        }
        let mut stream = match conn {
            Ok(s) => s,
            Err(_) => continue,
        };
        let _ = stream.set_read_timeout(Some(Duration::from_secs(READ_TIMEOUT_SECS)));
        // A write timeout too: the responder writes this parked stream INSIDE
        // its tick, so a client that stops reading must not be able to hang it.
        let _ = stream.set_write_timeout(Some(Duration::from_secs(READ_TIMEOUT_SECS)));
        let reply_id = next_id;
        next_id += 1;
        let req = match parse_request(&mut stream, reply_id) {
            Ok(r) => r,
            Err(_) => continue, // malformed/EOF/timeout: drop the connection.
        };
        // Bound the parked-connection table: refuse (503) rather than grow it.
        {
            let mut map = pending.lock().unwrap_or_else(|p| p.into_inner());
            if map.len() >= MAX_PENDING {
                let _ = write_response(&mut stream, &HttpResponse::status(reply_id, 503));
                continue;
            }
            map.insert(reply_id, (stream, Instant::now()));
        }
        // Hand the request to the strand. If the receiver is gone (teardown) or
        // we're shutting down, reclaim the parked socket and stop.
        if shutdown.load(Ordering::Acquire) || tx.send(req).is_err() {
            pending
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&reply_id);
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
        // can't accumulate to MAX_PENDING and permanently 503 new requests.
        self.reap_stale(Instant::now());

        for resp in bus
            .recv::<HttpResponse>(self.resp_socket)
            .unwrap_or_default()
        {
            let parked = self
                .pending
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&resp.reply_id);
            if let Some((mut stream, _)) = parked {
                let _ = write_response(&mut stream, &resp);
                // stream drops here → connection closes.
            }
            // An unknown reply_id (already answered / timed out) is dropped.
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

impl HttpResponder {
    /// Drop parked connections older than [`PENDING_TTL`] (a handler that never
    /// answered), writing a 504 before closing so the client isn't left hanging.
    /// Takes `now` so it is unit-testable without a real clock.
    fn reap_stale(&self, now: Instant) {
        let stale: Vec<u64> = {
            let map = self.pending.lock().unwrap_or_else(|p| p.into_inner());
            map.iter()
                .filter(|(_, (_, t))| now.duration_since(*t) >= PENDING_TTL)
                .map(|(id, _)| *id)
                .collect()
        };
        for id in stale {
            let entry = self
                .pending
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&id);
            if let Some((mut stream, _)) = entry {
                let _ = write_response(&mut stream, &HttpResponse::status(id, 504));
            }
        }
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
        responder
            .pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(7, (parked, t0));

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
}
