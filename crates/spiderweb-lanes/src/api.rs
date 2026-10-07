//! API lane — typed request/reply RPC over the fire-and-forget bus, plus a
//! JSON-over-HTTP surface that rides the [`crate::http`] lane.
//!
//! Two layers, each a dumb primitive:
//! 1. **Bus-native RPC**: [`Request`]/[`Response`] are typed values correlated
//!    by an `id` field (NOT the kernel's lineage tag). [`ApiServer`] dispatches
//!    by method name to registered handlers; [`ApiClient`] holds a futures-free
//!    pending map and [`ApiCaller`] is a blocking-with-timeout handle usable
//!    from outside a strand. `params`/`result` are opaque bytes — the caller
//!    picks the codec (same dumb-codec doctrine as the edge bridge).
//! 2. **JSON-over-HTTP** ([`ApiHttpBridge`]): `POST {path}` with body
//!    `{"method":..,"params":"<base64>"}` → `200 {"result":"<base64>"}` (or an
//!    error object). The HTTP framing is owned by the HTTP lane; this only
//!    touches the JSON body.

use crate::http::{HttpRequest, HttpResponse};
use spiderweb::{register_clone_type, BusHandle, Socket, Strand, StrandError};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Socket the RPC request rides on (client → server).
pub const REQUEST_SOCKET: &str = "api.request";
/// Socket the RPC response rides on (server → client).
pub const RESPONSE_SOCKET: &str = "api.response";

/// Process-global RPC instance counter. Every `ApiClient` and `ApiHttpBridge`
/// claims a distinct instance so their request ids never collide on a shared
/// bus — two clients both numbering ids from 1 would mis-route each other's
/// responses (both subscribe `RESPONSE_SOCKET`).
static RPC_INSTANCE: AtomicU64 = AtomicU64::new(0);

/// First id for a fresh RPC instance: a unique instance in the high 16 bits, the
/// per-instance sequence in the low 48 — so distinct instances never collide.
fn instance_id_base() -> u64 {
    (RPC_INSTANCE.fetch_add(1, Ordering::Relaxed) << 48) | 1
}

/// An RPC request. `id` correlates it to its [`Response`]; `method` selects the
/// handler; `params` is an opaque, caller-chosen byte payload.
#[derive(Clone, Debug)]
pub struct Request {
    pub id: u64,
    pub method: String,
    pub params: Vec<u8>,
}

/// An RPC response. `ok=false` carries an error message in `result`.
#[derive(Clone, Debug)]
pub struct Response {
    pub id: u64,
    pub ok: bool,
    pub result: Vec<u8>,
}

/// Why an [`ApiCaller::call`] did not return a successful response.
#[derive(Debug)]
pub enum ApiError {
    /// No response arrived within the timeout.
    Timeout,
    /// The client strand is gone (bus stopped).
    Closed,
    /// Too many calls are already in flight.
    TooManyInFlight,
    /// The handler reported an error (`ok=false`).
    Remote(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::Timeout => write!(f, "api call timed out"),
            ApiError::Closed => write!(f, "api client is closed"),
            ApiError::TooManyInFlight => write!(f, "too many api calls in flight"),
            ApiError::Remote(m) => write!(f, "api handler error: {m}"),
        }
    }
}
impl std::error::Error for ApiError {}

type Handler = Box<dyn Fn(&[u8]) -> Result<Vec<u8>, String> + Send>;

/// Server strand: dispatches each [`Request`] to a handler registered by method
/// name and publishes the [`Response`]. A handler error (or panic) becomes an
/// `ok=false` response — it never crashes the strand.
pub struct ApiServer {
    name: &'static str,
    in_socks: Vec<Socket>,
    out_socks: Vec<Socket>,
    handlers: HashMap<String, Handler>,
}

impl ApiServer {
    pub fn new(name: &'static str) -> Self {
        register_clone_type::<Request>();
        register_clone_type::<Response>();
        ApiServer {
            name,
            in_socks: vec![Socket::new::<Request>(REQUEST_SOCKET)],
            out_socks: vec![Socket::new::<Response>(RESPONSE_SOCKET)],
            handlers: HashMap::new(),
        }
    }

    /// Register a method handler (chainable). The handler is a dumb primitive:
    /// `params bytes -> Ok(result bytes) | Err(message)`.
    pub fn handle<F>(mut self, method: &str, f: F) -> Self
    where
        F: Fn(&[u8]) -> Result<Vec<u8>, String> + Send + 'static,
    {
        self.handlers.insert(method.to_string(), Box::new(f));
        self
    }
}

impl Strand for ApiServer {
    fn name(&self) -> &str {
        self.name
    }
    fn inputs(&self) -> &[Socket] {
        &self.in_socks
    }
    fn outputs(&self) -> &[Socket] {
        &self.out_socks
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for req in bus.recv::<Request>(REQUEST_SOCKET).unwrap_or_default() {
            let resp = match self.handlers.get(&req.method) {
                Some(h) => {
                    // A panicking handler degrades to an error response, never a
                    // dead strand (the edge-codec guard applied to dispatch).
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| h(&req.params)))
                    {
                        Ok(Ok(bytes)) => Response {
                            id: req.id,
                            ok: true,
                            result: bytes,
                        },
                        Ok(Err(msg)) => Response {
                            id: req.id,
                            ok: false,
                            result: msg.into_bytes(),
                        },
                        Err(_) => Response {
                            id: req.id,
                            ok: false,
                            result: b"handler panicked".to_vec(),
                        },
                    }
                }
                None => Response {
                    id: req.id,
                    ok: false,
                    result: format!("unknown method: {}", req.method).into_bytes(),
                },
            };
            let _ = bus.publish_static(RESPONSE_SOCKET, resp);
        }
        bus.sleep(Duration::from_millis(2));
        Ok(())
    }
}

/// A cheap, cloneable handle that publishes calls into the [`ApiClient`] strand
/// and blocks (with timeout) for the matching response. Usable from any thread,
/// not just inside a strand.
#[derive(Clone)]
pub struct ApiCaller {
    tx: Sender<(Request, Sender<Response>)>,
    next_id: Arc<AtomicU64>,
}

impl ApiCaller {
    /// Publish a call and block until the matching response or `timeout`.
    pub fn call(
        &self,
        method: &str,
        params: Vec<u8>,
        timeout: Duration,
    ) -> Result<Response, ApiError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let req = Request {
            id,
            method: method.to_string(),
            params,
        };
        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send((req, reply_tx))
            .map_err(|_| ApiError::Closed)?;
        match reply_rx.recv_timeout(timeout) {
            Ok(resp) if resp.ok => Ok(resp),
            Ok(resp) => Err(ApiError::Remote(
                String::from_utf8_lossy(&resp.result).into_owned(),
            )),
            Err(RecvTimeoutError::Timeout) => Err(ApiError::Timeout),
            Err(RecvTimeoutError::Disconnected) => Err(ApiError::Closed),
        }
    }
}

/// How long a pending call may wait for its response before the client reaps
/// it. A call whose caller already timed out (dropped its receiver) and whose
/// response never arrives would otherwise sit in `pending` forever; without a
/// reaper, `inflight_cap` corpses would permanently wedge the client. Set well
/// beyond any sane call timeout.
const PENDING_TTL: Duration = Duration::from_secs(120);

/// Client strand: bridges blocking [`ApiCaller`] handles to the bus. Holds the
/// futures-free correlation map (`id -> (reply channel, queued-at)`) across
/// ticks, reaping entries older than [`PENDING_TTL`].
pub struct ApiClient {
    name: &'static str,
    in_socks: Vec<Socket>,
    out_socks: Vec<Socket>,
    intake_rx: Receiver<(Request, Sender<Response>)>,
    pending: HashMap<u64, (Sender<Response>, Instant)>,
    inflight_cap: usize,
}

impl ApiClient {
    /// Build the client strand and a caller handle bound to it.
    pub fn new(name: &'static str) -> (Self, ApiCaller) {
        register_clone_type::<Request>();
        register_clone_type::<Response>();
        let (tx, rx) = mpsc::channel();
        let caller = ApiCaller {
            tx,
            next_id: Arc::new(AtomicU64::new(instance_id_base())),
        };
        let client = ApiClient {
            name,
            in_socks: vec![Socket::new::<Response>(RESPONSE_SOCKET)],
            out_socks: vec![Socket::new::<Request>(REQUEST_SOCKET)],
            intake_rx: rx,
            pending: HashMap::new(),
            inflight_cap: 1024,
        };
        (client, caller)
    }

    /// Override the in-flight cap (default 1024).
    pub fn with_inflight_cap(mut self, cap: usize) -> Self {
        self.inflight_cap = cap.max(1);
        self
    }

    /// Drop pending entries older than [`PENDING_TTL`] — abandoned calls whose
    /// response never arrived. Pure (takes `now`) so it is unit-testable without
    /// a bus or a real clock.
    fn reap_stale(&mut self, now: Instant) {
        self.pending
            .retain(|_, (_, queued)| now.duration_since(*queued) < PENDING_TTL);
    }
}

impl Strand for ApiClient {
    fn name(&self) -> &str {
        self.name
    }
    fn inputs(&self) -> &[Socket] {
        &self.in_socks
    }
    fn outputs(&self) -> &[Socket] {
        &self.out_socks
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        // (a) reap abandoned calls so corpses can't permanently fill `pending`.
        self.reap_stale(Instant::now());
        // (b) accept new calls, register their reply channel, publish the request.
        while let Ok((req, reply_tx)) = self.intake_rx.try_recv() {
            if self.pending.len() >= self.inflight_cap {
                let _ = reply_tx.send(Response {
                    id: req.id,
                    ok: false,
                    result: b"too many in flight".to_vec(),
                });
                continue;
            }
            self.pending.insert(req.id, (reply_tx, Instant::now()));
            let _ = bus.publish_static(REQUEST_SOCKET, req);
        }
        // (c) route each response to its waiting caller by id (unknown id dropped).
        for resp in bus.recv::<Response>(RESPONSE_SOCKET).unwrap_or_default() {
            if let Some((tx, _)) = self.pending.remove(&resp.id) {
                let _ = tx.send(resp);
            }
        }
        bus.sleep(Duration::from_millis(2));
        Ok(())
    }
}

/// Bridge strand: exposes the bus-native RPC over HTTP. It consumes
/// [`HttpRequest`]s on `http_req_socket` whose path matches `api_path`, turns
/// the JSON body into a [`Request`], and turns the matching [`Response`] back
/// into an [`HttpResponse`] on `http_resp_socket`.
pub struct ApiHttpBridge {
    name: &'static str,
    api_path: &'static str,
    http_req_socket: &'static str,
    http_resp_socket: &'static str,
    in_socks: Vec<Socket>,
    out_socks: Vec<Socket>,
    next_id: u64,
    /// api request id -> (HTTP reply_id awaiting it, queued-at) for reaping.
    map: HashMap<u64, (u64, Instant)>,
}

impl ApiHttpBridge {
    pub fn new(
        name: &'static str,
        api_path: &'static str,
        http_req_socket: &'static str,
        http_resp_socket: &'static str,
    ) -> Self {
        register_clone_type::<Request>();
        register_clone_type::<Response>();
        register_clone_type::<HttpRequest>();
        register_clone_type::<HttpResponse>();
        ApiHttpBridge {
            name,
            api_path,
            http_req_socket,
            http_resp_socket,
            in_socks: vec![
                Socket::new::<HttpRequest>(http_req_socket),
                Socket::new::<Response>(RESPONSE_SOCKET),
            ],
            out_socks: vec![
                Socket::new::<Request>(REQUEST_SOCKET),
                Socket::new::<HttpResponse>(http_resp_socket),
            ],
            // A distinct RPC instance so the bridge's ids never collide with an
            // ApiClient (or another bridge) sharing this bus.
            next_id: instance_id_base(),
            map: HashMap::new(),
        }
    }

    fn json_headers() -> Vec<(String, String)> {
        vec![("Content-Type".to_string(), "application/json".to_string())]
    }
}

impl Strand for ApiHttpBridge {
    fn name(&self) -> &str {
        self.name
    }
    fn inputs(&self) -> &[Socket] {
        &self.in_socks
    }
    fn outputs(&self) -> &[Socket] {
        &self.out_socks
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        // Reap api requests whose RPC Response never arrived (e.g. no ApiServer),
        // so `map` can't grow unbounded; 504 the waiting HTTP connection.
        let now = Instant::now();
        let stale: Vec<(u64, u64)> = self
            .map
            .iter()
            .filter(|(_, (_, t))| now.duration_since(*t) >= PENDING_TTL)
            .map(|(id, (reply_id, _))| (*id, *reply_id))
            .collect();
        for (id, reply_id) in stale {
            self.map.remove(&id);
            let _ = bus.publish_static(
                self.http_resp_socket,
                HttpResponse {
                    reply_id,
                    status: 504,
                    headers: Self::json_headers(),
                    body: json::encode_error("api timeout").into_bytes(),
                },
            );
        }

        // HTTP in -> RPC request out.
        for hreq in bus
            .recv::<HttpRequest>(self.http_req_socket)
            .unwrap_or_default()
        {
            if hreq.path != self.api_path {
                continue; // left for another handler
            }
            let body = String::from_utf8_lossy(&hreq.body);
            match json::obj_get_str(&body, "method") {
                Some(method) => {
                    let params = json::obj_get_str(&body, "params")
                        .and_then(b64::decode)
                        .unwrap_or_default();
                    let id = self.next_id;
                    self.next_id += 1;
                    self.map.insert(id, (hreq.reply_id, Instant::now()));
                    let _ = bus.publish_static(
                        REQUEST_SOCKET,
                        Request {
                            id,
                            method: method.to_string(),
                            params,
                        },
                    );
                }
                None => {
                    let _ = bus.publish_static(
                        self.http_resp_socket,
                        HttpResponse {
                            reply_id: hreq.reply_id,
                            status: 400,
                            headers: Self::json_headers(),
                            body: json::encode_error("missing \"method\"").into_bytes(),
                        },
                    );
                }
            }
        }
        // RPC response in -> HTTP out.
        for resp in bus.recv::<Response>(RESPONSE_SOCKET).unwrap_or_default() {
            if let Some((reply_id, _)) = self.map.remove(&resp.id) {
                let (status, body) = if resp.ok {
                    (200, json::encode_result(&b64::encode(&resp.result)))
                } else {
                    (
                        400,
                        json::encode_error(&String::from_utf8_lossy(&resp.result)),
                    )
                };
                let _ = bus.publish_static(
                    self.http_resp_socket,
                    HttpResponse {
                        reply_id,
                        status,
                        headers: Self::json_headers(),
                        body: body.into_bytes(),
                    },
                );
            }
        }
        bus.sleep(Duration::from_millis(2));
        Ok(())
    }
}

/// Std-only base64 (standard alphabet) for the JSON value bytes — keeps the
/// hand-rolled JSON parser free of an escape/unicode minefield.
mod b64 {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    pub fn encode(data: &[u8]) -> String {
        let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
        for chunk in data.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = *chunk.get(1).unwrap_or(&0) as u32;
            let b2 = *chunk.get(2).unwrap_or(&0) as u32;
            let n = (b0 << 16) | (b1 << 8) | b2;
            out.push(ALPHABET[((n >> 18) & 63) as usize] as char);
            out.push(ALPHABET[((n >> 12) & 63) as usize] as char);
            out.push(if chunk.len() > 1 {
                ALPHABET[((n >> 6) & 63) as usize] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                ALPHABET[(n & 63) as usize] as char
            } else {
                '='
            });
        }
        out
    }

    pub fn decode(s: &str) -> Option<Vec<u8>> {
        fn val(c: u8) -> Option<u32> {
            match c {
                b'A'..=b'Z' => Some((c - b'A') as u32),
                b'a'..=b'z' => Some((c - b'a' + 26) as u32),
                b'0'..=b'9' => Some((c - b'0' + 52) as u32),
                b'+' => Some(62),
                b'/' => Some(63),
                _ => None,
            }
        }
        let bytes: Vec<u8> = s.trim().bytes().filter(|&c| c != b'=').collect();
        let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
        for chunk in bytes.chunks(4) {
            if chunk.len() < 2 {
                return None; // a lone trailing char can't decode to a byte
            }
            let mut n = 0u32;
            for (i, &c) in chunk.iter().enumerate() {
                n |= val(c)? << (18 - 6 * i);
            }
            out.push((n >> 16) as u8);
            if chunk.len() >= 3 {
                out.push((n >> 8) as u8);
            }
            if chunk.len() >= 4 {
                out.push(n as u8);
            }
        }
        Some(out)
    }
}

/// Minimal flat-object JSON helpers. Handles exactly `{"k":"v",...}` with
/// string values (no nesting, no arrays, no escapes inside values — values are
/// base64 or plain method names). A documented limit, not a general JSON lib.
pub mod json {
    /// Extract the string value for `key` from a flat object. Values must not
    /// contain `"` (base64 and method names don't).
    pub fn obj_get_str<'a>(src: &'a str, key: &str) -> Option<&'a str> {
        let needle = format!("\"{key}\"");
        let after_key = &src[src.find(&needle)? + needle.len()..];
        let after_colon = &after_key[after_key.find(':')? + 1..];
        let after_q1 = &after_colon[after_colon.find('"')? + 1..];
        let end = after_q1.find('"')?;
        Some(&after_q1[..end])
    }

    /// `{"result":"<b64>"}`.
    pub fn encode_result(result_b64: &str) -> String {
        format!("{{\"result\":\"{result_b64}\"}}")
    }

    /// `{"error":"<escaped msg>"}`.
    pub fn encode_error(msg: &str) -> String {
        let escaped = msg.replace('\\', "\\\\").replace('"', "\\\"");
        format!("{{\"error\":\"{escaped}\"}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spiderweb::Bus;
    use std::thread;

    #[test]
    fn base64_round_trips() {
        for case in [
            &b""[..],
            b"f",
            b"fo",
            b"foo",
            b"foob",
            b"fooba",
            b"foobar",
            &[0, 255, 16],
        ] {
            let enc = b64::encode(case);
            assert_eq!(b64::decode(&enc).as_deref(), Some(case), "case {case:?}");
        }
    }

    #[test]
    fn json_extracts_flat_string_values() {
        let body = r#"{"method":"add","params":"AgM="}"#;
        assert_eq!(json::obj_get_str(body, "method"), Some("add"));
        assert_eq!(json::obj_get_str(body, "params"), Some("AgM="));
        assert_eq!(json::obj_get_str(body, "missing"), None);
    }

    /// An abandoned call (caller timed out, response never came) is reaped after
    /// the TTL, so corpses can't permanently fill `pending` and wedge the client.
    #[test]
    fn abandoned_pending_calls_are_reaped() {
        let (mut client, _caller) = ApiClient::new("api-client");
        let (tx, _rx) = mpsc::channel::<Response>();
        let t0 = Instant::now();
        client.pending.insert(7, (tx, t0));

        client.reap_stale(t0); // fresh — kept
        assert_eq!(client.pending.len(), 1);

        client.reap_stale(t0 + PENDING_TTL + Duration::from_secs(1)); // stale — reaped
        assert!(
            client.pending.is_empty(),
            "a pending entry past the TTL must be reaped"
        );
    }

    /// Two ApiClients on one bus must not mis-route each other's responses
    /// (both subscribe RESPONSE_SOCKET). Distinct per-instance id prefixes keep
    /// each client's responses matching only its own pending map.
    #[test]
    fn two_clients_on_one_bus_do_not_misroute() {
        let server =
            ApiServer::new("api-server").handle("double", |p| Ok(vec![p[0].wrapping_mul(2)]));
        let (c1, caller1) = ApiClient::new("api-client-1");
        let (c2, caller2) = ApiClient::new("api-client-2");

        let bus_thread = thread::spawn(move || {
            let mut bus = Bus::open();
            bus.register(server);
            bus.register(c1);
            bus.register(c2);
            bus.run_until(Some(Duration::from_millis(900)));
        });
        thread::sleep(Duration::from_millis(60));

        let h1 =
            thread::spawn(move || caller1.call("double", vec![10], Duration::from_millis(600)));
        let h2 =
            thread::spawn(move || caller2.call("double", vec![21], Duration::from_millis(600)));
        let r1 = h1.join().unwrap().expect("client 1 call");
        let r2 = h2.join().unwrap().expect("client 2 call");
        assert_eq!(r1.result, vec![20], "client 1 must get its own result");
        assert_eq!(r2.result, vec![42], "client 2 must get its own result");
        bus_thread.join().unwrap();
    }

    /// Bus-native RPC: `ApiServer("add")` + `ApiClient` on one bus; a blocking
    /// `ApiCaller::call("add", [2,3])` returns `[5]`. No HTTP, no socket.
    #[test]
    fn rpc_round_trips_on_one_bus() {
        let server = ApiServer::new("api-server").handle("add", |p| {
            if p.len() != 2 {
                return Err("need exactly 2 bytes".into());
            }
            Ok(vec![p[0].wrapping_add(p[1])])
        });
        let (client, caller) = ApiClient::new("api-client");

        let bus_thread = thread::spawn(move || {
            let mut bus = Bus::open();
            bus.register(server);
            bus.register(client);
            bus.run_until(Some(Duration::from_millis(800)));
        });

        // Give the bus a moment to start ticking, then make the blocking call.
        thread::sleep(Duration::from_millis(60));
        let resp = caller
            .call("add", vec![2, 3], Duration::from_millis(500))
            .expect("call should succeed");
        assert_eq!(resp.result, vec![5]);

        // An unknown method comes back as a remote error, not a hang.
        let err = caller
            .call("nope", vec![], Duration::from_millis(500))
            .unwrap_err();
        assert!(matches!(err, ApiError::Remote(_)), "got {err:?}");

        bus_thread.join().unwrap();
    }

    /// API over HTTP end to end: a raw HTTP `POST /api {"method":"add",
    /// "params":"AgM="}` (base64 of `[2,3]`) returns `200 {"result":"BQ=="}`
    /// (base64 of `[5]`), routed HTTP → bridge → RPC server → bridge → HTTP.
    #[test]
    fn api_over_http_round_trips() {
        use crate::http::HttpServer;
        use std::io::{Read, Write};
        use std::net::TcpStream;

        let server = HttpServer::new("http.req", "http.resp");
        let ingress = server.ingress("127.0.0.1:0").expect("bind");
        let port = ingress.local_addr().port();
        let responder = server.responder();
        let bridge = ApiHttpBridge::new("api-bridge", "/api", "http.req", "http.resp");
        let api = ApiServer::new("api-server").handle("add", |p| {
            if p.len() != 2 {
                return Err("need 2 bytes".into());
            }
            Ok(vec![p[0].wrapping_add(p[1])])
        });

        let bus_thread = thread::spawn(move || {
            let mut bus = Bus::open();
            bus.register(ingress);
            bus.register(responder);
            bus.register(bridge);
            bus.register(api);
            bus.run_until(Some(Duration::from_millis(1500)));
        });
        thread::sleep(Duration::from_millis(80));

        // Raw HTTP client: POST the JSON-RPC envelope.
        let body = br#"{"method":"add","params":"AgM="}"#;
        let req = format!(
            "POST /api HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream.write_all(req.as_bytes()).unwrap();
        stream.write_all(body).unwrap();
        stream.flush().unwrap();

        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).expect("read response");
        bus_thread.join().unwrap();

        let text = String::from_utf8_lossy(&raw);
        assert!(text.starts_with("HTTP/1.1 200"), "status line: {text:?}");
        assert!(
            text.contains(r#"{"result":"BQ=="}"#),
            "expected base64([5])=BQ== in body; got {text:?}"
        );
    }
}
