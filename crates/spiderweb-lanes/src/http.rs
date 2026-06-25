//! HTTP lane — a std-only HTTP/1.1 on-ramp/off-ramp.
//!
//! - [`wire`] holds the pure mechanism: the [`HttpRequest`]/[`HttpResponse`]
//!   bus types and a bounded, dependency-free parser/serializer.
//! - The server and client lane strands (ingress + egress) are layered on top
//!   of `wire` and land alongside it.

pub mod wire;

pub use wire::{HttpRequest, HttpResponse};
