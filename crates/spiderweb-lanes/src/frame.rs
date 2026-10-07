//! Shared binary framing for the wire lanes (mesh, NAT-inner).
//!
//! A near-**verbatim copy** of `spiderweb-edge`'s private framing — the length-
//! prefixed frame, the codec-box helpers, the session-id minter, and the sanity
//! caps — lifted here so the mesh and NAT lanes can speak the exact same wire as
//! the edge transport without making the production-green edge crate expose its
//! internals. Both crates keep an independently-fuzzed copy (see
//! `read_frame_never_panics_on_adversarial_input`, ported from edge).
//!
//! ONE intentional divergence from edge: `SESSION_TRACK` is 256 here (vs 16 in
//! edge) because a mesh node dedups against MANY origins at once, not one
//! restarting sender. The wire format and bounds are otherwise identical, so the
//! two fuzzed copies must not be blindly re-synced on this constant.

use spiderweb::BusHandle;
use std::io::{Read, Write};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::{SystemTime, UNIX_EPOCH};

/// Drain a bridged type off the local bus and return one encoded payload per
/// message. (Used by the egress side.)
pub(crate) type Pull = Box<dyn Fn(&mut BusHandle) -> Vec<Vec<u8>> + Send>;
/// Decode one payload and publish it onto the local bus. (Used by the ingress
/// side.)
pub(crate) type Push = Box<dyn Fn(&mut BusHandle, &[u8]) + Send>;
/// One wire frame in flight, reader thread → strand: `(session, seq, tag, payload)`.
pub(crate) type Frame = (u64, u64, String, Vec<u8>);

/// Sanity caps so a corrupt/hostile frame header can't trigger a huge alloc.
pub(crate) const MAX_TAG: usize = 1024;
pub(crate) const MAX_PAYLOAD: usize = 4 * 1024 * 1024;
/// Bound the reader-thread → strand handoff (backpressure via TCP flow control).
pub(crate) const EDGE_CHANNEL_CAP: usize = 256;
/// Cap a mid-frame stall (slow-loris).
pub(crate) const READ_TIMEOUT_SECS: u64 = 30;
/// Sender resend ring: recent frames retained for replay on (re)connect.
pub(crate) const RING_CAP: usize = 256;
/// How many distinct origins a node tracks high-waters for at once.
pub(crate) const SESSION_TRACK: usize = 256;

/// A per-origin id (wall-clock nanos, distinct across restarts).
pub(crate) fn new_session() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1)
}

/// Build the egress `Pull` for a bridged type `T` on `socket`: socket-scoped
/// drain, each value encoded under `catch_unwind` so a panicking codec drops the
/// frame instead of killing the worker.
pub(crate) fn make_pull<T: 'static + Send + Clone>(
    socket: &'static str,
    encode: fn(&T) -> Vec<u8>,
) -> Pull {
    Box::new(move |bus: &mut BusHandle| {
        bus.recv::<T>(socket)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| catch_unwind(AssertUnwindSafe(|| encode(&v))).ok())
            .collect()
    })
}

/// Build the ingress `Push` for a bridged type `T` on `socket`: decode under
/// `catch_unwind`, publish on success.
pub(crate) fn make_push<T: 'static + Send + Clone>(
    socket: &'static str,
    decode: fn(&[u8]) -> Option<T>,
) -> Push {
    Box::new(move |bus: &mut BusHandle, bytes: &[u8]| {
        let decoded = catch_unwind(AssertUnwindSafe(|| decode(bytes)))
            .ok()
            .flatten();
        if let Some(v) = decoded {
            let _ = bus.publish(socket, v);
        }
    })
}

/// Write one wire frame. Enforces the same caps the reader enforces (the `u32`
/// length prefixes would otherwise truncate and desync the stream).
pub(crate) fn write_frame<W: Write>(
    w: &mut W,
    session: u64,
    seq: u64,
    tag: &str,
    payload: &[u8],
) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    let tag_b = tag.as_bytes();
    if tag_b.len() > MAX_TAG {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "lanes: tag exceeds MAX_TAG",
        ));
    }
    if payload.len() > MAX_PAYLOAD {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "lanes: payload exceeds MAX_PAYLOAD",
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

/// Parse one wire frame. Every length is bounds-checked before allocating, so a
/// hostile peer cannot induce a huge allocation or a panic.
pub(crate) fn read_frame<R: Read>(r: &mut R) -> std::io::Result<Frame> {
    use std::io::{Error, ErrorKind};
    let mut s8 = [0u8; 8];
    r.read_exact(&mut s8)?;
    let session = u64::from_be_bytes(s8);
    let mut seq8 = [0u8; 8];
    r.read_exact(&mut seq8)?;
    let seq = u64::from_be_bytes(seq8);
    if seq == 0 {
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

    /// Ported verbatim from spiderweb-edge: 200k adversarial inputs, asserting
    /// `read_frame` never panics and never violates its bounds.
    #[test]
    fn read_frame_never_panics_on_adversarial_input() {
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
                bytes.extend_from_slice(&next().to_be_bytes());
                bytes.extend_from_slice(&(1u64 + (next() & 0xFF)).to_be_bytes());
                bytes.extend_from_slice(&(next() as u32).to_be_bytes());
                let take = (next() % 40) as usize;
                for _ in 0..take {
                    bytes.push(next() as u8);
                }
                bytes.extend_from_slice(&(next() as u32).to_be_bytes());
                let take2 = (next() % 40) as usize;
                for _ in 0..take2 {
                    bytes.push(next() as u8);
                }
            } else {
                let len = (next() % 64) as usize;
                for _ in 0..len {
                    bytes.push(next() as u8);
                }
            }
            let mut cur = std::io::Cursor::new(bytes);
            if let Ok((_s, seq, tag, payload)) = read_frame(&mut cur) {
                assert!(seq != 0);
                assert!(tag.len() <= MAX_TAG);
                assert!(payload.len() <= MAX_PAYLOAD);
            }
        }
    }

    #[test]
    fn frame_round_trips() {
        let mut buf = Vec::new();
        write_frame(&mut buf, 7, 42, "wire.note", b"hello").unwrap();
        let mut cur = std::io::Cursor::new(buf);
        let (session, seq, tag, payload) = read_frame(&mut cur).expect("valid frame");
        assert_eq!(
            (session, seq, tag.as_str(), payload.as_slice()),
            (7, 42, "wire.note", &b"hello"[..])
        );
    }
}
