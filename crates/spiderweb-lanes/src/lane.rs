//! The unifying **Lane** contract every external lane conforms to.
//!
//! A lane is a factory that, on [`Lane::install`], hands the bus a set of named
//! strands (its on-ramps/off-ramps) plus an optional keep-alive `guard` for any
//! external resource (an `ssh` child, a relay handle) that must outlive those
//! strands. This generalizes the `spiderweb-edge` sender/listener pair: edge is
//! simply a `Lane` whose `install` yields `[("in", EdgeListener), ("out",
//! EdgeSender)]` (see [`crate::tcp::TcpLane`]).
//!
//! Why a trait and not a single builder: the five transports differ wildly
//! (HTTP, RPC, mesh, NAT relay, SSH tunnel) but share one bus-facing shape — a
//! `Vec` of [`Strand`]s. The trait captures that invariant while leaving the
//! transport open; each lane keeps its own ergonomic builder that produces the
//! `Lane`-implementing value.

use spiderweb::{Bus, BusHandle, Socket, Strand, StrandError};
use std::any::Any;
use std::net::SocketAddr;

/// One bridged type's wire identity: a stable cross-process `tag` (a `TypeId`
/// is meaningless off-process) and the local `socket` it rides on. Descriptive
/// metadata for tooling; the actual codec is type-erased into the strands at
/// build time, keeping this object-safe.
#[derive(Clone, Copy, Debug)]
pub struct Bridge {
    pub tag: &'static str,
    pub socket: &'static str,
}

/// Why a lane failed to install.
#[derive(Debug)]
pub enum LaneError {
    /// The ingress side could not bind/listen on its address.
    Bind(std::io::Error),
    /// The egress side could not establish its connection (rare; usually lazy).
    Connect(std::io::Error),
    /// Composing a system executor failed (e.g. spawning `ssh`).
    Spawn(std::io::Error),
    /// The lane configuration was malformed.
    Config(String),
}

impl std::fmt::Display for LaneError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LaneError::Bind(e) => write!(f, "lane ingress bind failed: {e}"),
            LaneError::Connect(e) => write!(f, "lane egress connect failed: {e}"),
            LaneError::Spawn(e) => write!(f, "lane process spawn failed: {e}"),
            LaneError::Config(s) => write!(f, "lane config error: {s}"),
        }
    }
}

impl std::error::Error for LaneError {}

/// What a lane hands back when installed: the strands the bus must run, an
/// optional resource `guard`, and (for an ingress that bound a port-0 socket)
/// the resolved address.
///
/// `guard` is the reconciliation seam for any lane that owns a non-strand
/// resource (an `ssh` child, a relay handle): it is `None` for the pure-std
/// lanes and `Some` for a resource-owning one, whose `Drop` tears the resource
/// down. The caller keeps it alive alongside the bus so teardown order is: drop
/// strands first (firing their `EdgeListener`-style `Drop`), then drop the guard.
/// (The SSH lane currently hands its `SshTunnel` back through its own
/// `SshLane::sender`/`listener` return value rather than a `Lane` impl; a
/// `Lane`-shaped SSH wrapper would route that tunnel through this slot — the slot
/// is exercised by `add_lane_returns_a_guard_for_resource_owning_lanes`.)
pub struct LaneParts {
    /// `(suffix, strand)` pairs — the suffix ("in"/"out"/"resp"/"client"/…)
    /// disambiguates a lane's multiple strands in the kernel's name space.
    pub strands: Vec<(String, Box<dyn Strand>)>,
    /// A resource that must live exactly as long as the strands (e.g. an `ssh`
    /// child). `None` for pure-std lanes.
    pub guard: Option<Box<dyn Any + Send>>,
    /// The ingress address actually bound, when the lane listens (the port-0
    /// case, mirroring `EdgeListener::local_addr`).
    pub bound: Option<SocketAddr>,
}

impl LaneParts {
    /// A lane that yields strands but owns no external resource and binds no
    /// address (the common case).
    pub fn strands_only(strands: Vec<(String, Box<dyn Strand>)>) -> Self {
        LaneParts {
            strands,
            guard: None,
            bound: None,
        }
    }
}

/// Every external lane is a factory that produces [`LaneParts`]. Object-safe, so
/// a heterogeneous `Vec<Box<dyn Lane>>` of HTTP + SSH + mesh registers uniformly.
pub trait Lane {
    /// Stable lane kind, e.g. `"tcp"`, `"http"`, `"api"`, `"mesh"`, `"nat"`,
    /// `"ssh"`. Used by `webctl` listing and the strand name space.
    fn kind(&self) -> &'static str;

    /// The types this lane bridges (descriptive metadata for tooling).
    fn bridges(&self) -> &[Bridge];

    /// Build and bind everything (may spawn threads, bind ports, spawn a child
    /// process). The last step once all addresses are known; fallible because
    /// binding/spawning can fail.
    fn install(&self) -> Result<LaneParts, LaneError>;
}

/// Newtype that supplies a unique kernel strand name and owns the inner boxed
/// strand. No `Drop` here: the [`Strand`] trait has no `Drop` method, so a
/// lane's teardown rides the box-ownership chain — dropping `LaneStrand` drops
/// `inner`, firing the inner strand's own `Drop` (e.g. `EdgeListener::drop`).
pub struct LaneStrand {
    name: String,
    inner: Box<dyn Strand>,
}

impl Strand for LaneStrand {
    fn name(&self) -> &str {
        &self.name
    }
    fn inputs(&self) -> &[Socket] {
        self.inner.inputs()
    }
    fn outputs(&self) -> &[Socket] {
        self.inner.outputs()
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        self.inner.run(bus)
    }
}

/// What [`BusLanes::add_lane`] hands back: the bound ingress address (if the
/// lane listens) and the keep-alive guard (if the lane owns an external
/// resource like an `ssh` child) — the caller MUST hold the guard for the life
/// of the bus.
pub type LaneInstall = (Option<SocketAddr>, Option<Box<dyn Any + Send>>);

/// `Bus` extension mirroring `spiderweb-registry`'s `BusAutoConnect`: install a
/// lane's strands in one call, each named `"<kind>:<name>:<suffix>"` so two
/// lanes of the same kind never collide (the kernel panics on duplicate names).
pub trait BusLanes {
    fn add_lane(&mut self, name: &str, lane: &dyn Lane) -> Result<LaneInstall, LaneError>;
}

impl BusLanes for Bus {
    fn add_lane(&mut self, name: &str, lane: &dyn Lane) -> Result<LaneInstall, LaneError> {
        let parts = lane.install()?;
        for (suffix, inner) in parts.strands {
            self.register(LaneStrand {
                name: format!("{}:{name}:{suffix}", lane.kind()),
                inner,
            });
        }
        Ok((parts.bound, parts.guard))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spiderweb::Bus;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    /// A trivial strand that flips a flag on its first tick, so we can prove the
    /// lane's strand was actually registered and run by the bus.
    struct Pinger {
        ran: Arc<AtomicBool>,
    }
    impl Strand for Pinger {
        fn name(&self) -> &str {
            "pinger"
        }
        fn inputs(&self) -> &[Socket] {
            &[]
        }
        fn outputs(&self) -> &[Socket] {
            &[]
        }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            self.ran.store(true, Ordering::SeqCst);
            bus.sleep(Duration::from_millis(5));
            Ok(())
        }
    }

    /// An egress-only lane: one strand, no ingress, no guard, no bound addr.
    struct OneStrandLane {
        ran: Arc<AtomicBool>,
        bridges: Vec<Bridge>,
    }
    impl Lane for OneStrandLane {
        fn kind(&self) -> &'static str {
            "test"
        }
        fn bridges(&self) -> &[Bridge] {
            &self.bridges
        }
        fn install(&self) -> Result<LaneParts, LaneError> {
            Ok(LaneParts::strands_only(vec![(
                "out".into(),
                Box::new(Pinger {
                    ran: self.ran.clone(),
                }),
            )]))
        }
    }

    #[test]
    fn add_lane_registers_and_runs_the_lane_strand() {
        let ran = Arc::new(AtomicBool::new(false));
        let lane = OneStrandLane {
            ran: ran.clone(),
            bridges: vec![Bridge {
                tag: "wire.note",
                socket: "note",
            }],
        };
        let mut bus = Bus::open();
        let (bound, guard) = bus.add_lane("demo", &lane).expect("install");
        assert!(bound.is_none(), "this lane binds no address");
        assert!(guard.is_none(), "this lane owns no external resource");
        bus.run_until(Some(Duration::from_millis(120)));
        assert!(
            ran.load(Ordering::SeqCst),
            "the lane's strand must have been registered and ticked"
        );
    }

    /// A lane that owns an external resource hands a `guard` back through
    /// `add_lane` (the seam the SSH lane uses to keep its `ssh` child alive).
    #[test]
    fn add_lane_returns_a_guard_for_resource_owning_lanes() {
        struct GuardedLane {
            ran: Arc<AtomicBool>,
            bridges: Vec<Bridge>,
        }
        impl Lane for GuardedLane {
            fn kind(&self) -> &'static str {
                "guarded"
            }
            fn bridges(&self) -> &[Bridge] {
                &self.bridges
            }
            fn install(&self) -> Result<LaneParts, LaneError> {
                Ok(LaneParts {
                    strands: vec![(
                        "out".into(),
                        Box::new(Pinger {
                            ran: self.ran.clone(),
                        }),
                    )],
                    // A stand-in for SSH's SshTunnel: dropping it would tear the
                    // external resource down.
                    guard: Some(Box::new(String::from("external-resource-handle"))),
                    bound: Some("127.0.0.1:4242".parse().unwrap()),
                })
            }
        }

        let lane = GuardedLane {
            ran: Arc::new(AtomicBool::new(false)),
            bridges: Vec::new(),
        };
        let mut bus = Bus::open();
        let (bound, guard) = bus.add_lane("g", &lane).expect("install");
        assert_eq!(
            bound.map(|a| a.to_string()),
            Some("127.0.0.1:4242".to_string())
        );
        let held = guard.expect("a resource-owning lane returns a guard");
        assert!(
            held.downcast_ref::<String>().is_some(),
            "the guard is handed back to the caller to keep alive"
        );
    }

    #[test]
    fn lane_describes_its_kind_and_bridges() {
        let lane = OneStrandLane {
            ran: Arc::new(AtomicBool::new(false)),
            bridges: vec![Bridge {
                tag: "wire.note",
                socket: "note",
            }],
        };
        assert_eq!(lane.kind(), "test");
        assert_eq!(lane.bridges().len(), 1);
        assert_eq!(lane.bridges()[0].tag, "wire.note");
    }
}
