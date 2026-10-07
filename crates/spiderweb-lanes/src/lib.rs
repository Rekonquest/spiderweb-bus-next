//! `spiderweb-lanes` — external protocol lanes for the Spiderweb Bus.
//!
//! A **lane** is an on-ramp/off-ramp between the in-process bus and an
//! outside-world transport. It generalizes the [`spiderweb-edge`] sender/
//! listener pair: every lane is two strands —
//!
//! - an **ingress** strand: external transport → decode → publish on the bus,
//! - an **egress** strand: subscribe on the bus → encode → send out.
//!
//! The kernel is never touched; lanes are companion strands. The crate is
//! std-only and `#![forbid(unsafe_code)]`. The one place that leaves `std` is
//! the SSH lane, which **composes** with the system `ssh` binary (SSH crypto is
//! a real wall for a zero-dependency crate) rather than reimplementing it — the
//! bus traffic rides an `ssh -L`/`-R` port-forward, encrypted and authenticated
//! with zero crypto dependencies.
//!
//! Lanes (added incrementally, each behind its own module):
//! - `http` — std-only HTTP/1.1 server + client lane.
//! - `api` — typed request/reply RPC over the bus, exposed over HTTP.
//! - `p2p` — multi-peer mesh (generalizes the point-to-point edge).
//! - `nat` — rendezvous-relay traversal (peers dial out, relay switchboards).
//! - `ssh` — edge over a composed system-`ssh` tunnel.

#![forbid(unsafe_code)]

mod frame;

pub mod lane;
pub mod tcp;

pub mod api;
pub mod http;
pub mod mesh;
pub mod nat;
pub mod ssh;

pub use api::{ApiCaller, ApiClient, ApiError, ApiHttpBridge, ApiServer, Request, Response};
pub use http::HttpLane;
pub use lane::{Bridge, BusLanes, Lane, LaneError, LaneParts, LaneStrand};
pub use mesh::{Mesh, MeshNode, PeerHandle};
pub use nat::{NatLane, NatRelayListener, NatRelaySender, Relay, RoomAddr};
pub use ssh::{Direction, SshForward, SshLane, SshLaneError, SshTunnel};
pub use tcp::TcpLane;
