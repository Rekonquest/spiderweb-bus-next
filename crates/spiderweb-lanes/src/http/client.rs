//! HTTP client lane: perform outbound HTTP/1.1 requests requested on the bus.
//!
//! [`HttpEgress`] subscribes to [`HttpCall`] and runs each blocking round trip
//! on a [`Highway`] worker pool, so a strand tick is never blocked on the
//! network; results are published as [`HttpResult`]. Plaintext `http://` only —
//! TLS is a real wall for a zero-dep crate (the SSH lane is how encrypted
//! transport is reached without crypto deps).

use super::wire::{HttpCall, HttpResult, MAX_BODY, MAX_HEAD};
use highway::Highway;
use spiderweb::{register_clone_type, BusHandle, Socket, Strand, StrandError};
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Cap a stalled connect/read on an outbound call.
const CALL_TIMEOUT_SECS: u64 = 30;

/// Builder for the client side. Calls published on `call_socket` are performed
/// and their results published on `result_socket`.
pub struct HttpClient {
    call_socket: &'static str,
    result_socket: &'static str,
}

impl HttpClient {
    pub fn new(call_socket: &'static str, result_socket: &'static str) -> Self {
        register_clone_type::<HttpCall>();
        register_clone_type::<HttpResult>();
        HttpClient {
            call_socket,
            result_socket,
        }
    }

    /// The egress strand, backed by a `lanes`-wide worker pool.
    pub fn egress(&self, lanes: usize) -> HttpEgress {
        HttpEgress {
            result_socket: self.result_socket,
            in_socks: vec![Socket::new::<HttpCall>(self.call_socket)],
            call_socket: self.call_socket,
            pool: Highway::new(lanes.max(1), http_round_trip),
            backlog: VecDeque::new(),
        }
    }
}

/// The blocking worker: perform one call, never panicking — any failure becomes
/// an `HttpResult` with `error: Some(..)`.
fn http_round_trip(call: HttpCall) -> HttpResult {
    match do_call(&call) {
        Ok((status, headers, body)) => HttpResult {
            id: call.id,
            status,
            headers,
            body,
            error: None,
        },
        Err(e) => HttpResult {
            id: call.id,
            status: 0,
            headers: Vec::new(),
            body: Vec::new(),
            error: Some(e),
        },
    }
}

#[allow(clippy::type_complexity)]
fn do_call(call: &HttpCall) -> Result<(u16, Vec<(String, String)>, Vec<u8>), String> {
    // --- parse http://host[:port]/path (plaintext only) ---
    let rest = call
        .url
        .strip_prefix("http://")
        .ok_or("only http:// URLs are supported (no TLS)")?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = match authority.rfind(':') {
        Some(i) => (
            &authority[..i],
            authority[i + 1..].parse::<u16>().map_err(|_| "bad port")?,
        ),
        None => (authority, 80u16),
    };

    // --- connect + send the request ---
    let mut stream = TcpStream::connect((host, port)).map_err(|e| e.to_string())?;
    let to = Some(Duration::from_secs(CALL_TIMEOUT_SECS));
    let _ = stream.set_read_timeout(to);
    let _ = stream.set_write_timeout(to);

    let mut req: Vec<u8> = Vec::new();
    let _ = write!(req, "{} {} HTTP/1.1\r\n", call.method, path);
    let _ = write!(req, "Host: {host}\r\n");
    let _ = write!(req, "Connection: close\r\n");
    let _ = write!(req, "Content-Length: {}\r\n", call.body.len());
    for (k, v) in &call.headers {
        if k.eq_ignore_ascii_case("host")
            || k.eq_ignore_ascii_case("content-length")
            || k.eq_ignore_ascii_case("connection")
        {
            continue; // we set these ourselves to keep framing consistent
        }
        let _ = write!(req, "{k}: {v}\r\n");
    }
    req.extend_from_slice(b"\r\n");
    req.extend_from_slice(&call.body);
    stream.write_all(&req).map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())?;

    // --- read the whole response (Connection: close → read to EOF), bounded ---
    let mut raw: Vec<u8> = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = stream.read(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&buf[..n]);
        if raw.len() > MAX_HEAD + MAX_BODY {
            return Err("response exceeds size cap".into());
        }
    }

    // --- split head/body and parse the status line + headers ---
    let sep = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("no header terminator in response")?;
    if sep > MAX_HEAD {
        return Err("response head exceeds cap".into());
    }
    let head = String::from_utf8_lossy(&raw[..sep]);
    let body = raw[sep + 4..].to_vec();
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or("malformed status line")?;
    let headers = lines
        .filter_map(|l| {
            l.split_once(':')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        })
        .collect();
    Ok((status, headers, body))
}

/// Egress strand: drains [`HttpCall`]s onto a worker pool and republishes each
/// [`HttpResult`]. The tick never blocks — round trips run on the pool's lanes.
pub struct HttpEgress {
    call_socket: &'static str,
    result_socket: &'static str,
    in_socks: Vec<Socket>,
    pool: Highway<HttpCall, HttpResult>,
    backlog: VecDeque<HttpCall>,
}

impl Strand for HttpEgress {
    fn name(&self) -> &str {
        "http-egress"
    }
    fn inputs(&self) -> &[Socket] {
        &self.in_socks
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        // 1. take new calls (socket-scoped) into the backlog.
        for call in bus.recv::<HttpCall>(self.call_socket).unwrap_or_default() {
            self.backlog.push_back(call);
        }
        // 2. feed the pool while it has room (bounded concurrency, never block).
        while self.pool.has_room() {
            match self.backlog.pop_front() {
                Some(c) => {
                    if self.pool.on_ramp(c).is_err() {
                        break;
                    }
                }
                None => break,
            }
        }
        // 3. publish finished results.
        for r in self.pool.off_ramp() {
            let _ = bus.publish_static(self.result_socket, r);
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}
