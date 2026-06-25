//! Std-only HTTP/1.1 wire layer: the message types that ride the bus plus a
//! bounded request parser and response serializer. No `hyper`, no deps — just
//! `std::io`. This is pure mechanism (parse bytes ⇄ structs); the lane strands
//! (in `super`) carry the policy (correlation, who replies, concurrency).
//!
//! Scope: HTTP/1.1, `Content-Length`-delimited bodies, one request per parse
//! call. Chunked transfer-encoding and keep-alive pipelining are documented
//! non-goals for the MVP (the server answers `Connection: close`). Every length
//! is bounded before allocation so a hostile peer can't trigger a huge alloc.

use std::io::{self, BufRead, BufReader, Read, Write};

/// Cap the request line + all headers so a peer can't make us buffer forever.
pub const MAX_HEAD: usize = 64 * 1024;
/// Cap a body (and our own responses) so a bogus `Content-Length` can't OOM us.
pub const MAX_BODY: usize = 16 * 1024 * 1024;

/// An inbound HTTP request, published onto the bus by the server lane. `id` is
/// a per-server correlation id; a handler echoes it back in [`HttpResponse`] so
/// the server can route the reply to the right still-open connection.
#[derive(Clone, Debug)]
pub struct HttpRequest {
    pub id: u64,
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// A handler's reply, published onto the bus; the server writes it back to the
/// connection whose request carried the same `id`.
#[derive(Clone, Debug)]
pub struct HttpResponse {
    pub id: u64,
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// Convenience: a `text/plain` 200 reply correlated to `id`.
    pub fn text(id: u64, body: impl Into<String>) -> Self {
        let body = body.into().into_bytes();
        HttpResponse {
            id,
            status: 200,
            headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
            body,
        }
    }

    /// Convenience: a status-only reply (empty body), e.g. 404 / 503.
    pub fn status(id: u64, status: u16) -> Self {
        HttpResponse {
            id,
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }
}

impl HttpRequest {
    /// Case-insensitive header lookup (HTTP header names are case-insensitive).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Parse one HTTP/1.1 request from `r`. `id` is stamped onto the result for
/// correlation. Returns `Err` (which the caller turns into a dropped/!closed
/// connection) on malformed input, an over-cap head/body, or EOF. Never panics
/// and never allocates a body larger than `MAX_BODY`.
pub fn parse_request<R: Read>(r: R, id: u64) -> io::Result<HttpRequest> {
    let mut reader = BufReader::new(r);

    // --- request line + headers: read lines until a blank line, bounded ---
    let mut head_bytes = 0usize;
    let mut line = String::new();
    head_bytes += read_line_bounded(&mut reader, &mut line, MAX_HEAD - head_bytes)?;
    let request_line = line.trim_end().to_string();
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| inval("empty request line"))?
        .to_string();
    let path = parts
        .next()
        .ok_or_else(|| inval("missing request target"))?
        .to_string();
    // The HTTP version token (parts.next()) is accepted but not enforced.

    let mut headers: Vec<(String, String)> = Vec::new();
    loop {
        line.clear();
        head_bytes += read_line_bounded(&mut reader, &mut line, MAX_HEAD - head_bytes)?;
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break; // blank line terminates the header block
        }
        if let Some((k, v)) = trimmed.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
        // A header line without a colon is skipped rather than fatal.
    }

    // --- body: Content-Length bytes, bounded ---
    let content_length = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse::<usize>().ok())
        .unwrap_or(0);
    if content_length > MAX_BODY {
        return Err(inval("content-length exceeds MAX_BODY"));
    }
    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body)?;

    Ok(HttpRequest {
        id,
        method,
        path,
        headers,
        body,
    })
}

/// Read one `\n`-terminated line into `buf`, returning the byte count, but error
/// out if it would exceed `budget` — so neither a single giant line nor the
/// header block as a whole can grow without bound.
fn read_line_bounded<R: BufRead>(r: &mut R, buf: &mut String, budget: usize) -> io::Result<usize> {
    // read_line can over-grow on a hostile peer; cap via a Take.
    let mut limited = r.take(budget as u64 + 1);
    let n = limited.read_line(buf)?;
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof in head"));
    }
    if n > budget {
        return Err(inval("request head exceeds MAX_HEAD"));
    }
    Ok(n)
}

/// Serialize an [`HttpResponse`] to `w` as HTTP/1.1 with `Connection: close`
/// and a `Content-Length`. Any caller-supplied `Content-Length`/`Connection`
/// headers are dropped (we set them) to keep the framing self-consistent.
pub fn write_response<W: Write>(w: &mut W, resp: &HttpResponse) -> io::Result<()> {
    let reason = reason_phrase(resp.status);
    write!(w, "HTTP/1.1 {} {}\r\n", resp.status, reason)?;
    for (k, v) in &resp.headers {
        if k.eq_ignore_ascii_case("content-length") || k.eq_ignore_ascii_case("connection") {
            continue;
        }
        write!(w, "{}: {}\r\n", k, v)?;
    }
    write!(w, "Content-Length: {}\r\n", resp.body.len())?;
    write!(w, "Connection: close\r\n")?;
    w.write_all(b"\r\n")?;
    w.write_all(&resp.body)?;
    w.flush()
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Status",
    }
}

fn inval(msg: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_get_with_no_body() {
        let raw = b"GET /hello?x=1 HTTP/1.1\r\nHost: localhost\r\nAccept: */*\r\n\r\n";
        let req = parse_request(&raw[..], 7).expect("parse");
        assert_eq!(req.id, 7);
        assert_eq!(req.method, "GET");
        assert_eq!(req.path, "/hello?x=1");
        assert_eq!(req.header("host"), Some("localhost"));
        assert!(req.body.is_empty());
    }

    #[test]
    fn parses_a_post_with_content_length_body() {
        let raw =
            b"POST /api HTTP/1.1\r\nContent-Length: 5\r\nContent-Type: text/plain\r\n\r\nhello";
        let req = parse_request(&raw[..], 1).expect("parse");
        assert_eq!(req.method, "POST");
        assert_eq!(req.body, b"hello");
        assert_eq!(req.header("CONTENT-TYPE"), Some("text/plain")); // case-insensitive
    }

    #[test]
    fn rejects_an_oversized_content_length_without_allocating() {
        let raw = format!(
            "POST /x HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            MAX_BODY + 1
        );
        let err = parse_request(raw.as_bytes(), 0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn rejects_a_head_over_the_cap() {
        // One header line larger than MAX_HEAD must error, not buffer forever.
        let mut raw = b"GET / HTTP/1.1\r\nX-Big: ".to_vec();
        raw.extend(std::iter::repeat_n(b'a', MAX_HEAD + 16));
        raw.extend_from_slice(b"\r\n\r\n");
        let err = parse_request(&raw[..], 0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn round_trips_a_response() {
        let resp = HttpResponse::text(42, "pong");
        let mut buf = Vec::new();
        write_response(&mut buf, &resp).expect("write");
        let s = String::from_utf8(buf).unwrap();
        assert!(s.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(s.contains("Content-Length: 4\r\n"));
        assert!(s.contains("Connection: close\r\n"));
        assert!(s.ends_with("\r\n\r\npong"));
    }

    #[test]
    fn status_only_response_has_empty_body() {
        let resp = HttpResponse::status(9, 404);
        let mut buf = Vec::new();
        write_response(&mut buf, &resp).expect("write");
        let s = String::from_utf8(buf).unwrap();
        assert!(s.starts_with("HTTP/1.1 404 Not Found\r\n"));
        assert!(s.contains("Content-Length: 0\r\n"));
    }
}
