//! `spiderweb` — a plug-and-play bus for Rust strands.
//!
//! Primitives: threads, channels, a type registry. No Tokio. No `async`.
//!
//! A crate implements [`Strand`], calls [`Bus::register`], and the kernel
//! does the rest. Messages flow by type. Subscribers fan out automatically.
//! Multiple instances of the same strand are separate subscribers (free
//! redundancy). Crashed strands are restarted by the kernel up to a
//! configurable limit; the [`spider`](https://docs.rs/spider) crate is the
//! default orchestrator that decides when to restart vs. quarantine.
//!
//! v1.2 supports fan-out of any `T: Clone + Send + 'static`. For non-Clone
//! payloads (large buffers, etc.), serialize to a `String` or upgrade to
//! v1.3+ which will add transparent Arc<T> support.
//!
//! See `ARCHITECTURE.md` at the workspace root for the design.

#![forbid(unsafe_code)]

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

// -----------------------------------------------------------------------------
// Poison-tolerant locking
// -----------------------------------------------------------------------------

/// Lock a [`Mutex`] without propagating poison.
///
/// `std::sync::Mutex` poisons when a thread panics while holding the guard.
/// With the usual `.lock_tolerant()`, the *next* thread to lock then panics
/// too — so a single fault anywhere in a kernel critical section cascades into
/// every lock site and kills the whole bus. That is the opposite of what a
/// production bus should do.
///
/// All of the kernel's guarded state is plain data — `HashMap`s, `Vec`s, the
/// fabric graph. A panic mid-update can leave it partially written but never
/// memory-unsafe (this crate is `#![forbid(unsafe_code)]`), so recovering the
/// inner guard and carrying on is strictly safer than propagating the poison.
/// This is the defensive half; the kernel also keeps panics *out* of these
/// sections in the first place (strands run lock-free; the sections do only
/// infallible map/vec/graph ops). Belt and braces.
trait LockTolerant<T> {
    /// Acquire the guard, recovering it even if the mutex was poisoned.
    fn lock_tolerant(&self) -> std::sync::MutexGuard<'_, T>;
}

impl<T> LockTolerant<T> for Mutex<T> {
    #[inline]
    fn lock_tolerant(&self) -> std::sync::MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

// -----------------------------------------------------------------------------
// Socket
// -----------------------------------------------------------------------------

/// A typed, named channel endpoint. The type is the routing key.
pub struct Socket {
    name: &'static str,
    type_id: TypeId,
}

impl Socket {
    /// Declare a socket. The type parameter `T` is the message type.
    pub const fn new<T: 'static + Send>(name: &'static str) -> Self {
        Socket {
            name,
            type_id: TypeId::of::<T>(),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn type_id(&self) -> TypeId {
        self.type_id
    }
}

impl fmt::Debug for Socket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Socket")
            .field("name", &self.name)
            .field("type_id", &self.type_id)
            .finish()
    }
}

/// Reserved socket name. Lifecycle events are published here.
pub const SYS_LIFECYCLE: &str = "sys.lifecycle";

/// Reserved socket name. Control messages from orchestrator → kernel.
pub const SYS_CONTROL: &str = "sys.control";

/// Reserved socket name. Vibrations — cross-layer signals like
/// backpressure — propagate here. Any layer can emit a [`Vibration`];
/// any strand (the spider, typically) can subscribe and react. This is
/// the web *feeling itself*: when one strand is touched, the rest can
/// sense it without a direct wire.
pub const SYS_VIBRATION: &str = "sys.vibration";

// -----------------------------------------------------------------------------
// Lifecycle events
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum LifecycleEvent {
    Registered {
        name: String,
        inputs: Vec<String>,
        outputs: Vec<String>,
    },
    Detached { name: String },
    Crashed { name: String, reason: String },
    Restarted { name: String, attempt: u32 },
    Quarantined { name: String, reason: String },
    Buffered { socket: &'static str, from: String },
    Flushed { socket: &'static str, to: String, count: usize },
    /// A new thread (desire path) was born — a message with no parent
    /// entered the fabric at `origin`. v1.3.
    ThreadFormed { thread: ThreadId, origin: NodeId },
    /// An intersection emerged: `node` is where the fabric first
    /// observed two distinct threads cross. v1.3.
    IntersectionEmerged { node: NodeId, threads: Vec<ThreadId> },
}

// -----------------------------------------------------------------------------
// Observability — pull-able BusStats
// -----------------------------------------------------------------------------

/// Per-strand counters. One entry per slot present at snapshot time.
/// `processed` is how many messages this strand's *current* instance has
/// drained from its inbox (it resets if the spider restarts the strand, since
/// a restart installs a fresh slot).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StrandStats {
    pub name: String,
    pub processed: u64,
}

/// A read-only snapshot of bus-wide telemetry — the countable replacement for
/// `spider.log(&str)` strings. Pull it with [`Bus::stats`], or from inside a
/// strand during a run with [`BusHandle::stats`]. Cheap: the kernel-wide
/// counters are lock-free atomics and the structural fields derive from the
/// same bounded state [`Bus::fabric_snapshot`] reads. Everything here is
/// bounded — the counters are a fixed set and `per_strand` is bounded by the
/// fabric's `THREAD_CAP`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BusStats {
    // --- kernel-wide cumulative counters (monotonic) ---
    /// Total physical deliveries of strand-published messages to subscribers —
    /// work, vibrations, and control — where a fan-out to N subscribers counts
    /// N. Excludes the kernel's own internal lifecycle fan-out, which is
    /// counted precisely by kind in the fields below.
    pub delivered: u64,
    /// Messages aged out of a per-type **replay buffer** at
    /// `PENDING_BUFFER_CAP`. The kernel buffers a bounded tail of every type so
    /// a *late* subscriber can be caught up; once more than the cap have flowed,
    /// the oldest age out. This is normal replay-buffer churn, NOT necessarily
    /// lost work — any already-subscribed strand received the message by direct
    /// delivery. It means a lost delivery only for a subscriber that arrives
    /// *after* its message has already aged out.
    pub pending_evictions: u64,
    /// Total [`Vibration`] messages delivered on [`SYS_VIBRATION`].
    pub vibrations: u64,
    // --- lifecycle event counts by kind ---
    pub registered: u64,
    pub restarted: u64,
    pub crashed: u64,
    pub detached: u64,
    pub quarantined: u64,
    /// Times a message *type*'s pending buffer transitioned empty→non-empty
    /// (the first message of a type-burst), NOT the count of individual
    /// buffered messages — it mirrors `LifecycleEvent::Buffered`.
    pub buffered: u64,
    /// Times a newly-arriving subscriber was replayed a non-empty pending
    /// buffer (mirrors `LifecycleEvent::Flushed`).
    pub flushed: u64,
    pub threads_formed: u64,
    pub intersections_emerged: u64,
    // --- structural, derived from the fabric + slot table ---
    /// Strands present in the slot table at snapshot time (includes a parked
    /// crashed slot awaiting the spider's decision). Always equal to
    /// `per_strand.len()` — provided for convenience.
    pub active_strands: usize,
    /// Live subscriber registrations across all message types. Bounded: dead
    /// subscribers are pruned lazily on the next delivery to their socket, so
    /// crash/restart churn does not grow this without bound. Watching it stay
    /// flat under churn is the bounded-subscriber-map invariant made visible.
    pub subscribers: usize,
    pub threads: usize,
    pub intersections: usize,
    /// Distinct fabric vertices. Derived from [`FabricSnapshot::adjacency`], so
    /// it counts every node that appears in any thread OR as an edge endpoint —
    /// including endpoints whose thread has since been evicted. It can
    /// therefore exceed what `threads` alone would imply; do not assume
    /// `nodes == threads`.
    pub nodes: usize,
    pub edges: usize,
    /// Sum of every fabric edge's delivery count.
    pub edge_traffic_total: u64,
    pub per_strand: Vec<StrandStats>,
}

// -----------------------------------------------------------------------------
// Control messages
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum ControlMsg {
    Restart { name: String },
    Quarantine { name: String, reason: String },
    Shutdown,
    /// Set the transfer policy at an intersection `node`. Lets Layer-3
    /// orchestration (the spider) govern how threads hop at a crossing
    /// without any strand knowing. v1.3.
    SetTransfer { node: NodeId, policy: TransferPolicy },
}

// -----------------------------------------------------------------------------
// Vibrations
// -----------------------------------------------------------------------------

/// A vibration on the web: a cross-layer signal that propagates so the
/// rest of the web can feel what one part is experiencing. Emitted on
/// [`SYS_VIBRATION`]; routed like any other typed message, so emitting
/// and feeling a vibration uses the same pub/sub the rest of the bus
/// does. v1.3 carries backpressure; future variants will carry
/// heartbeat staleness and other cross-cutting signals.
#[derive(Debug, Clone)]
pub enum Vibration {
    /// A node can't keep up: work is arriving faster than it drains.
    /// `queued` items are waiting upstream of the node while
    /// `in_flight` are already accepted against a soft `capacity`.
    /// An upstream strand or the spider can react by slowing, pausing,
    /// or (later) rerouting.
    Backpressure {
        node: String,
        queued: usize,
        in_flight: usize,
        capacity: usize,
    },
    /// A node is starving: its read-ahead buffer is running dry, so its
    /// source/fetch can't keep up with downstream demand. The dual of
    /// [`Vibration::Backpressure`] — same socket, opposite end of a rate
    /// mismatch. v1.3.
    Starved {
        node: String,
        ready: usize,
        depth: usize,
    },
}

// -----------------------------------------------------------------------------
// Threads, intersections, the fabric (v1.3)
// -----------------------------------------------------------------------------

/// Identifies one thread — the lineage of a single message as it flows
/// through the web. Minted by the kernel when a parentless message
/// enters the fabric; never declared.
pub type ThreadId = u64;

/// A node on the web. It is the strand's name (already an owned
/// `String` in the slot table), aliased for intent.
pub type NodeId = String;

/// Provenance carried alongside (never inside) a message's payload.
/// `None` means "no parent yet" — the kernel mints a fresh thread for
/// it on delivery. `Some(id)` continues an existing thread. It is
/// `Copy`, rides the carrier tuple parallel to the type-erased
/// payload, and is invisible to ordinary strand authors.
pub type Tag = Option<ThreadId>;

/// A thread: the ordered path a message lineage has actually traversed
/// through the web. It *forms* as messages flow — a desire path — and
/// is never declared up front. The first node is where the thread was
/// born; each later node is one the lineage was delivered to.
#[derive(Debug, Clone)]
pub struct Thread {
    pub id: ThreadId,
    pub direction: Vec<NodeId>,
}

/// An intersection: a node where two or more distinct threads have been
/// observed to cross. It *emerges* from real traffic — the instant a
/// second thread reaches a node a first thread already passed through —
/// and is never preconfigured.
#[derive(Debug, Clone)]
pub struct Intersection {
    pub node: NodeId,
    pub threads: Vec<ThreadId>,
    pub transfer: TransferPolicy,
}

/// How a message hops between threads at an intersection. The default,
/// [`TransferPolicy::Stay`], is a pure no-op: threads cross but nobody
/// hops, so enabling the fabric changes no routing or timing. A
/// non-default policy is only ever set explicitly via
/// [`Bus::set_transfer`] or [`ControlMsg::SetTransfer`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransferPolicy {
    /// Outputs keep the thread they arrived on. Default.
    #[default]
    Stay,
    /// Outputs are re-parented onto a specific existing thread.
    HopTo(ThreadId),
    /// Outputs are fused onto a single merged thread for this node —
    /// the natural fan-in case.
    MergeAll,
}

/// A node's position in the web's 3D space (v1.3). A *logical* overlay:
/// in a single-process bus every node is in the same RAM, so distance is
/// a number you compute — for visualization, organization, and policy —
/// not a latency you feel. (It gains physical meaning with edge transport
/// in v2, where a position maps to a real machine/GPU.) The Z-axis
/// encodes the layer/elevation metaphor: ground ~ z=0, highway ~ z=1,
/// orchestration ~ z=2.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Position {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl Position {
    pub const ORIGIN: Position = Position { x: 0.0, y: 0.0, z: 0.0 };

    pub const fn new(x: f64, y: f64, z: f64) -> Self {
        Position { x, y, z }
    }

    /// Euclidean distance to another position.
    pub fn distance(&self, other: Position) -> f64 {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        let dz = self.z - other.z;
        (dx * dx + dy * dy + dz * dz).sqrt()
    }
}

/// How strongly a vibration emitted at `origin` is felt by a node at
/// `receiver` — a scalar in `(0, 1]`. A real spiderweb transmits a touch
/// that decays with distance: `1 / (1 + d)` (finite at `d = 0`,
/// monotonically decreasing). If either position is unknown the vibration
/// is felt at full strength (`1.0`) — an unplaced node feels everything.
///
/// This is the physics *spreading-activation / decay-with-distance*
/// primitive: a dumb pure function. It computes felt-strength; it makes
/// no decision. The orchestrator (the spider, or any receiver) reads it
/// and decides how to react — mechanism here, policy there. The decay
/// kernel ([`attenuation`] = `1/(1+d)`, [`attenuation_within`] = radius
/// cutoff) is a swappable generator in one primitive slot.
///
/// Result is in `[0, 1]`: `1.0` when either position is unknown, in
/// `(0, 1]` for a finite distance, and `0.0` for an infinite/non-finite
/// distance (an unreachable node feels nothing). Positions placed via
/// [`Bus::place`] are guarded finite, so the non-finite case only arises
/// for hand-built `Position`s.
pub fn attenuation(origin: Option<Position>, receiver: Option<Position>) -> f64 {
    match (origin, receiver) {
        (Some(o), Some(r)) => {
            let d = o.distance(r);
            if d.is_finite() {
                1.0 / (1.0 + d)
            } else {
                0.0
            }
        }
        _ => 1.0,
    }
}

/// Like [`attenuation`] but with a hard `radius` cutoff: a vibration is
/// felt nothing (`0.0`) strictly beyond `radius` from its origin — the
/// edge of the web's reach. The boundary is inclusive (`d == radius` is
/// still felt); a non-finite distance reads as beyond reach (`0.0`).
pub fn attenuation_within(
    origin: Option<Position>,
    receiver: Option<Position>,
    radius: f64,
) -> f64 {
    match (origin, receiver) {
        (Some(o), Some(r)) => {
            let d = o.distance(r);
            // `d <= radius` is false for NaN, so a non-finite distance
            // correctly falls to 0.0 (beyond reach) rather than NaN.
            if d <= radius {
                1.0 / (1.0 + d)
            } else {
                0.0
            }
        }
        _ => 1.0,
    }
}

/// A read-only snapshot of the fabric for tests, demos, and tooling.
/// The fabric itself lives in the kernel and accretes from traffic;
/// this is a cloned view that holds no locks.
#[derive(Debug, Clone, Default)]
pub struct FabricSnapshot {
    pub threads: Vec<Thread>,
    pub intersections: Vec<Intersection>,
    /// Node positions in the web's 3D space (v1.3). Only placed nodes
    /// appear; an unplaced node is simply absent.
    pub positions: Vec<(NodeId, Position)>,
    /// Per-edge traffic `(from, to, deliveries)`: how many messages have
    /// crossed each directed edge, sorted by `(from, to)`. The raw
    /// observed-traffic weight behind [`FabricSnapshot::adjacency`].
    pub edge_traffic: Vec<(NodeId, NodeId, u64)>,
}

impl FabricSnapshot {
    /// The thread with this id, if it exists.
    pub fn thread(&self, id: ThreadId) -> Option<&Thread> {
        self.threads.iter().find(|t| t.id == id)
    }

    /// The intersection at this node, if one has emerged.
    pub fn intersection(&self, node: &str) -> Option<&Intersection> {
        self.intersections.iter().find(|i| i.node == node)
    }

    /// This node's position, if it has been placed.
    pub fn position(&self, node: &str) -> Option<Position> {
        self.positions
            .iter()
            .find(|(n, _)| n == node)
            .map(|(_, p)| *p)
    }

    /// Euclidean distance between two placed nodes (`None` if either is
    /// unplaced).
    pub fn distance(&self, a: &str, b: &str) -> Option<f64> {
        Some(self.position(a)?.distance(self.position(b)?))
    }

    /// The fabric as a weighted graph: its vertex set and its directed,
    /// weighted edge set — the topology a graph-Laplacian / heat-kernel
    /// vibration generator consumes (build `L = D − A` over
    /// [`FabricAdjacency::nodes`] and propagate one column of
    /// `H_t = exp(−t·L)` by sparse mat-vecs).
    ///
    /// An edge `(from, to)` exists once at least one message has been
    /// delivered across it, and its **conductance** is that real
    /// per-delivery count ([`FabricSnapshot::edge_traffic`]) — counted at
    /// the physical publisher->subscriber hop. So it is honest observed
    /// traffic: one delivery weighs `1.0`, a worn path weighs more, a
    /// fan-out counts each downstream edge once **without inflating the
    /// upstream edge**, and it stays correct under every transfer policy
    /// (`HopTo`/`MergeAll` re-parent the lineage tag, never the physical
    /// hop). Counts are message deliveries, **not bytes** — a single-process
    /// payload is a typed value with no serialized size (a real wall, not a
    /// placeholder); byte/bandwidth-weighted conductance arrives with edge
    /// transport as a drop-in swap of this same count.
    ///
    /// The edges are **directed** (faithful to message flow); a generator
    /// that wants a symmetric `A` folds reciprocal pairs as *it* decides
    /// (sum / average / max) — that symmetrization is the generator's
    /// policy, not the fabric's. Pure mechanism: the kernel reports its own
    /// traffic counts and makes no routing or attenuation decision; no
    /// positions are involved.
    ///
    /// `nodes` is every node seen in any thread plus every edge endpoint,
    /// so a one-node thread (a lineage that never moved) is an isolated
    /// degree-`0` vertex — present in `nodes`, absent from `edges`.
    /// Deterministic, like every collection in the snapshot: `nodes`
    /// sorted/deduplicated, `edges` sorted by `(from, to)`.
    pub fn adjacency(&self) -> FabricAdjacency {
        use std::collections::BTreeSet;

        let mut nodes: BTreeSet<&NodeId> = BTreeSet::new();
        for thread in &self.threads {
            for node in &thread.direction {
                nodes.insert(node);
            }
        }
        for (from, to, _) in &self.edge_traffic {
            // Edge endpoints are vertices even if their thread has since
            // been evicted (edge_traffic is cumulative).
            nodes.insert(from);
            nodes.insert(to);
        }

        let mut edges: Vec<FabricEdge> = self
            .edge_traffic
            .iter()
            .map(|(from, to, count)| FabricEdge {
                from: from.clone(),
                to: to.clone(),
                conductance: *count as f64,
            })
            .collect();
        edges.sort_by(|a, b| (&a.from, &a.to).cmp(&(&b.from, &b.to)));

        FabricAdjacency {
            nodes: nodes.into_iter().cloned().collect(),
            edges,
        }
    }
}

/// One directed, weighted edge of the emergent fabric: a node-pair and
/// its conductance — the real count of messages delivered across it.
/// Produced by [`FabricSnapshot::adjacency`].
#[derive(Debug, Clone, PartialEq)]
pub struct FabricEdge {
    pub from: NodeId,
    pub to: NodeId,
    /// Edge weight: how many messages have been delivered across
    /// `from → to` (observed traffic). `1.0` for a single delivery;
    /// accumulates for a worn path. Counted at delivery, so a fan-out does
    /// not inflate the upstream edge (see [`FabricSnapshot::adjacency`]).
    pub conductance: f64,
}

/// The fabric viewed as a weighted directed graph — the vertex set plus
/// the weighted edge set, derived from observed per-edge traffic. The
/// topology a heat-kernel / graph-Laplacian vibration generator builds
/// `L = D − A` over. Read-only and deterministic; see
/// [`FabricSnapshot::adjacency`] for the exact semantics.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FabricAdjacency {
    /// Every node seen in any thread, sorted and deduplicated. Index these
    /// `0..nodes.len()` to map node names onto Laplacian matrix rows.
    pub nodes: Vec<NodeId>,
    /// Directed weighted edges, sorted by `(from, to)`.
    pub edges: Vec<FabricEdge>,
}

/// The emergent weave. Owned by the kernel behind a mutex; empty at
/// boot and grown purely by `record_arrival` as messages flow. Not
/// part of the public API — callers read it via [`Bus::fabric_snapshot`].
#[derive(Default)]
struct Fabric {
    threads: HashMap<ThreadId, Thread>,
    intersections: HashMap<NodeId, Intersection>,
    /// Which distinct threads have been seen at each node. The set
    /// going from one to two is exactly when an intersection emerges.
    node_threads: HashMap<NodeId, std::collections::BTreeSet<ThreadId>>,
    /// Explicitly configured transfer policies, by node. Absent = Stay.
    transfers: HashMap<NodeId, TransferPolicy>,
    /// Lazily-minted fused thread per MergeAll node.
    merged: HashMap<NodeId, ThreadId>,
    /// Node positions in the web's 3D space. Pure metadata, parallel to
    /// the routing path; set via [`Bus::place`] or an advertised position.
    positions: HashMap<NodeId, Position>,
    /// Per-edge traffic: how many messages have actually been delivered
    /// across each directed edge `(from, to)`, incremented by
    /// [`Fabric::record_traffic`] from [`Kernel::deliver`] at the PHYSICAL
    /// publisher->subscriber hop. This is the real observed-traffic weight
    /// behind [`FabricSnapshot::adjacency`]'s conductance. Counting the
    /// physical hop (not a thread's lineage) keeps it correct under every
    /// transfer policy — `HopTo`/`MergeAll` re-parent the tag, never the
    /// delivery — and a fan-out naturally counts each downstream edge once
    /// without inflating the upstream edge. Counts are message deliveries,
    /// **not bytes**: in a single-process bus a payload is a typed value
    /// with no serialized size — a real wall, not a placeholder;
    /// byte/bandwidth-weighted conductance arrives with edge transport as a
    /// drop-in swap of this same count. Cumulative (survives thread
    /// eviction) and, like the other node-keyed indices, bounded in
    /// practice by the live strand topology.
    edge_traffic: HashMap<(NodeId, NodeId), u64>,
    next_thread: ThreadId,
    /// Eviction cursor: the next thread id to consider evicting. Ids
    /// are monotonic, so the oldest live thread is the lowest id at or
    /// above this cursor.
    evict_cursor: ThreadId,
}

impl Fabric {
    /// Max threads retained. Like [`Kernel::PENDING_BUFFER_CAP`], this
    /// bounds memory on a long-lived bus: the fabric is an
    /// observational window over recent lineages, not an unbounded log.
    /// Beyond this, the oldest threads are evicted and scrubbed from the
    /// node/intersection indices.
    const THREAD_CAP: usize = 1024;

    /// Mint a fresh thread born at `origin`. Returns its id.
    fn mint(&mut self, origin: &str) -> ThreadId {
        let id = self.next_thread;
        self.next_thread += 1;
        self.threads.insert(
            id,
            Thread {
                id,
                direction: vec![origin.to_string()],
            },
        );
        self.evict_if_needed();
        id
    }

    /// Fork a child thread off `parent` at a fan-out point: the child
    /// inherits the parent's path so far, then diverges down its own
    /// branch. This keeps each thread a true linear desire path — a
    /// fan-out becomes sibling threads sharing a prefix, not one
    /// fabricated chain through parallel nodes. Returns the child id, or
    /// `None` if the parent is unknown (e.g. already evicted).
    fn fork(&mut self, parent: ThreadId) -> Option<ThreadId> {
        let prefix = self.threads.get(&parent)?.direction.clone();
        let id = self.next_thread;
        self.next_thread += 1;
        self.threads.insert(id, Thread { id, direction: prefix });
        self.evict_if_needed();
        Some(id)
    }

    /// Evict the oldest threads while over [`Self::THREAD_CAP`],
    /// scrubbing each evicted id from the node and intersection indices
    /// so they never reference a dead thread.
    fn evict_if_needed(&mut self) {
        while self.threads.len() > Self::THREAD_CAP {
            while self.evict_cursor < self.next_thread
                && !self.threads.contains_key(&self.evict_cursor)
            {
                self.evict_cursor += 1;
            }
            if self.evict_cursor >= self.next_thread {
                break;
            }
            let oldest = self.evict_cursor;
            self.evict_cursor += 1;
            self.threads.remove(&oldest);
            for set in self.node_threads.values_mut() {
                set.remove(&oldest);
            }
            for isx in self.intersections.values_mut() {
                isx.threads.retain(|&t| t != oldest);
            }
            self.merged.retain(|_, v| *v != oldest);
        }
    }

    /// Count one physical message delivery across the edge `from -> to`.
    /// This is the honest per-edge traffic weight behind
    /// [`FabricSnapshot::adjacency`]'s conductance. It is recorded at the
    /// PHYSICAL deliver site (publisher -> subscriber, see
    /// [`Kernel::deliver`]) rather than from a thread's lineage, so it is
    /// correct under every transfer policy: a tag re-parented by `HopTo`
    /// or fused by `MergeAll` changes the *lineage*, never the physical
    /// hop a message actually took. A self-delivery (`from == to`) is not
    /// a traffic edge and is skipped, keeping the adjacency diagonal zero.
    fn record_traffic(&mut self, from: &str, to: &str) {
        if from == to {
            return;
        }
        *self
            .edge_traffic
            .entry((from.to_string(), to.to_string()))
            .or_insert(0) += 1;
    }

    /// Record that thread `tid` was delivered to `node`. Grows the
    /// thread's path (deduping consecutive repeats) and detects an
    /// intersection when `node` first sees a second distinct thread.
    /// Returns any lifecycle events the caller should publish AFTER
    /// dropping the fabric lock.
    fn record_arrival(&mut self, tid: ThreadId, node: &str) -> Vec<LifecycleEvent> {
        let mut events = Vec::new();

        if let Some(t) = self.threads.get_mut(&tid) {
            if t.direction.last().map(String::as_str) != Some(node) {
                t.direction.push(node.to_string());
            }
        }

        let set = self.node_threads.entry(node.to_string()).or_default();
        let is_new = set.insert(tid);
        if is_new && set.len() == 2 {
            let threads: Vec<ThreadId> = set.iter().copied().collect();
            let policy = self.transfers.get(node).copied().unwrap_or_default();
            self.intersections.insert(
                node.to_string(),
                Intersection {
                    node: node.to_string(),
                    threads: threads.clone(),
                    transfer: policy,
                },
            );
            events.push(LifecycleEvent::IntersectionEmerged {
                node: node.to_string(),
                threads,
            });
        } else if is_new && set.len() > 2 {
            if let Some(isx) = self.intersections.get_mut(node) {
                isx.threads = set.iter().copied().collect();
            }
        }

        events
    }

    /// The tag a message leaves `node` with, given the tag it arrived
    /// with (`None` = parentless). Applies the node's transfer policy.
    /// Returns `(out_tag, newly_minted_origin)` — the second element is
    /// `Some` only when a brand-new *origin* thread was born here (so
    /// the caller emits `ThreadFormed` for the right thread, never an
    /// orphan). A parentless message only mints when the policy keeps
    /// it (`Stay`, or `HopTo` to an unknown target); under `HopTo` to a
    /// live thread or `MergeAll` the policy's own thread is used and
    /// nothing is orphaned.
    fn outbound(&mut self, node: &str, incoming: Tag) -> (Tag, Option<ThreadId>) {
        let policy = self.transfers.get(node).copied().unwrap_or_default();
        match policy {
            TransferPolicy::Stay => self.continue_or_mint(node, incoming),
            TransferPolicy::HopTo(target) => {
                if self.threads.contains_key(&target) {
                    (Some(target), None)
                } else {
                    // Hop target was never minted — don't fabricate a
                    // dangling lineage; fall back to ordinary threading.
                    self.continue_or_mint(node, incoming)
                }
            }
            TransferPolicy::MergeAll => (Some(self.merged_thread(node)), None),
        }
    }

    /// Continue an existing thread, or mint a fresh origin for a
    /// parentless message.
    fn continue_or_mint(&mut self, node: &str, incoming: Tag) -> (Tag, Option<ThreadId>) {
        match incoming {
            Some(_) => (incoming, None),
            None => {
                let id = self.mint(node);
                (Some(id), Some(id))
            }
        }
    }

    /// The fused thread for a `MergeAll` node, minted on first use and
    /// rooted at the real node (not a synthetic label).
    fn merged_thread(&mut self, node: &str) -> ThreadId {
        if let Some(id) = self.merged.get(node) {
            if self.threads.contains_key(id) {
                return *id;
            }
        }
        let id = self.next_thread;
        self.next_thread += 1;
        self.threads.insert(
            id,
            Thread {
                id,
                direction: vec![node.to_string()],
            },
        );
        self.merged.insert(node.to_string(), id);
        self.evict_if_needed();
        id
    }

    /// Whether `node` has the default (`Stay`) transfer policy, so a
    /// fan-out from it should fork sibling threads rather than fuse.
    fn is_stay(&self, node: &str) -> bool {
        matches!(
            self.transfers.get(node).copied().unwrap_or_default(),
            TransferPolicy::Stay
        )
    }

    fn set_transfer(&mut self, node: &str, policy: TransferPolicy) {
        self.transfers.insert(node.to_string(), policy);
        if let Some(isx) = self.intersections.get_mut(node) {
            isx.transfer = policy;
        }
    }

    fn set_position(&mut self, node: &str, pos: Position) {
        // Ignore a non-finite position so the fabric never holds NaN/inf:
        // the node simply stays unplaced (felt at full strength) rather
        // than poisoning the distance/attenuation/scene math downstream.
        if pos.x.is_finite() && pos.y.is_finite() && pos.z.is_finite() {
            self.positions.insert(node.to_string(), pos);
        }
    }

    fn snapshot(&self) -> FabricSnapshot {
        // Sort every collection so a snapshot is deterministic across
        // runs (the underlying maps have nondeterministic iteration
        // order) — golden tests and scene exports stay stable.
        let mut threads: Vec<Thread> = self.threads.values().cloned().collect();
        threads.sort_by_key(|t| t.id);
        let mut intersections: Vec<Intersection> = self.intersections.values().cloned().collect();
        intersections.sort_by(|a, b| a.node.cmp(&b.node));
        let mut positions: Vec<(NodeId, Position)> =
            self.positions.iter().map(|(n, p)| (n.clone(), *p)).collect();
        positions.sort_by(|a, b| a.0.cmp(&b.0));
        let mut edge_traffic: Vec<(NodeId, NodeId, u64)> = self
            .edge_traffic
            .iter()
            .map(|((from, to), n)| (from.clone(), to.clone(), *n))
            .collect();
        edge_traffic.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        FabricSnapshot {
            threads,
            intersections,
            positions,
            edge_traffic,
        }
    }
}

/// True for the reserved `sys.*` sockets (lifecycle, control,
/// vibration). The fabric tracks only real work flow, so deliveries on
/// these sockets are never threaded.
fn is_reserved(socket: &str) -> bool {
    socket.starts_with("sys.")
}

// -----------------------------------------------------------------------------
// Strand trait
// -----------------------------------------------------------------------------

#[derive(Debug)]
pub enum StrandError {
    Detach,
    Crashed(String),
}

impl fmt::Display for StrandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StrandError::Detach => write!(f, "detach"),
            StrandError::Crashed(s) => write!(f, "crashed: {}", s),
        }
    }
}

impl std::error::Error for StrandError {}

pub trait Strand: Send + 'static {
    fn name(&self) -> &str;
    fn inputs(&self) -> &[Socket];
    fn outputs(&self) -> &[Socket];
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError>;
}

// -----------------------------------------------------------------------------
// StrandSpec
// -----------------------------------------------------------------------------

type StrandFactory = Arc<dyn Fn() -> Box<dyn Strand> + Send + Sync>;

pub struct StrandSpec {
    name: &'static str,
    factory: StrandFactory,
}

impl StrandSpec {
    pub fn new<F>(name: &'static str, factory: F) -> Self
    where
        F: Fn() -> Box<dyn Strand> + Send + Sync + 'static,
    {
        StrandSpec {
            name,
            factory: Arc::new(factory),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }
}

// -----------------------------------------------------------------------------
// Envelope + clone registry
// -----------------------------------------------------------------------------

type Envelope = Box<dyn Any + Send>;

/// A message on the wire: the socket name, its provenance [`Tag`], and
/// the type-erased payload. The kernel threads this tuple end-to-end
/// (channels, pending buffer, inbox/outbox); the `Tag` rides PARALLEL
/// to the payload so the clone registry and `recv::<T>()` downcast are
/// untouched, and ordinary strands never see it.
type Carrier = (&'static str, Tag, Envelope);

/// A function that clones an envelope. The kernel looks these up by
/// TypeId at fan-out time. v1.2 pre-registers `String`, primitives,
/// and the system types. User-defined `Clone` types can be registered
/// via [`register_clone_type`].
type CloneFn = Box<dyn Fn(&Envelope) -> Envelope + Send + Sync>;

struct CloneRegistry {
    map: HashMap<TypeId, CloneFn>,
}

impl CloneRegistry {
    fn new() -> Self {
        let mut r = CloneRegistry {
            map: HashMap::new(),
        };
        r.register_builtin::<String>();
        r.register_builtin::<i64>();
        r.register_builtin::<u64>();
        r.register_builtin::<bool>();
        r.register_builtin::<LifecycleEvent>();
        r.register_builtin::<ControlMsg>();
        r.register_builtin::<Vibration>();
        r
    }

    fn register_builtin<T: Any + Clone + Send + Sync>(&mut self) {
        self.map.insert(
            TypeId::of::<T>(),
            Box::new(|env: &Envelope| -> Envelope {
                let v = env
                    .downcast_ref::<T>()
                    .expect("clone_registry: type id mismatch");
                Box::new(v.clone())
            }),
        );
    }

    fn clone_env(&self, env: &Envelope) -> Envelope {
        let type_id = (**env).type_id();
        match self.map.get(&type_id) {
            Some(f) => f(env),
            None => panic!(
                "spiderweb v1.2: payload of type `{}` is not registered for fan-out. \
                 Call `spiderweb::register_clone_type::<T>()` at boot, or use a \
                 pre-registered type (String, primitives, LifecycleEvent, ControlMsg).",
                std::any::type_name_of_val(&**env)
            ),
        }
    }
}

static CLONE_REGISTRY: Mutex<Option<CloneRegistry>> = Mutex::new(None);

fn registry() -> std::sync::MutexGuard<'static, Option<CloneRegistry>> {
    let mut guard = CLONE_REGISTRY.lock_tolerant();
    if guard.is_none() {
        *guard = Some(CloneRegistry::new());
    }
    guard
}

/// Register a custom type for fan-out. The kernel will use this to
/// clone the value when delivering to multiple subscribers.
pub fn register_clone_type<T: Any + Clone + Send + Sync>() {
    registry().as_mut().unwrap().register_builtin::<T>();
}

// -----------------------------------------------------------------------------
// BusHandle
// -----------------------------------------------------------------------------

pub struct BusHandle {
    strand_name: String,
    outbox: Vec<Carrier>,
    inbox: Vec<Carrier>,
    /// The thread a fresh `publish` continues this tick. Seeded from
    /// the inbox and updated to the last item handed out by `recv` /
    /// `recv_all`, so the universal recv-then-publish idiom threads
    /// provenance with zero work from the strand author. `None` for a
    /// source tick — the kernel mints a new thread on delivery.
    current_cause: Tag,
    kernel: Arc<Kernel>,
}

impl BusHandle {
    fn new(strand_name: String, inbox: Vec<Carrier>, kernel: Arc<Kernel>) -> Self {
        // Seed the cause from the first NON-reserved inbox tag, so a
        // strand that publishes before recv-ing still continues the
        // inbound work thread. Reserved sys.* carriers (which arrive
        // untagged anyway) must never seed the cause — they are not
        // threaded. recv/recv_all refine this to the last item consumed.
        let current_cause = inbox
            .iter()
            .find(|(sock, _, _)| !is_reserved(sock))
            .and_then(|(_, tag, _)| *tag);
        BusHandle {
            strand_name,
            outbox: Vec::new(),
            inbox,
            current_cause,
            kernel,
        }
    }

    pub fn publish<T: 'static + Send + Clone>(
        &mut self,
        socket: &'static str,
        value: T,
    ) -> Result<(), BusError> {
        self.outbox.push((socket, self.current_cause, Box::new(value)));
        Ok(())
    }

    /// Same as `publish` but takes a `&'static str` socket name to
    /// satisfy the `BusError::TypeMismatch` lifetime requirement.
    pub fn publish_static<T: 'static + Send + Clone>(
        &mut self,
        socket: &'static str,
        value: T,
    ) -> Result<(), BusError> {
        self.publish(socket, value)
    }

    /// Publish carrying an explicit provenance [`Tag`], overriding the
    /// tick's inferred cause. Framework seam for layer-builders (the
    /// highway uses it to re-emit a result on the same thread the input
    /// arrived on, across its buffering lanes). Ordinary strands should
    /// use [`publish`](Self::publish) and ignore tags entirely.
    pub fn publish_tagged<T: 'static + Send + Clone>(
        &mut self,
        socket: &'static str,
        tag: Tag,
        value: T,
    ) -> Result<(), BusError> {
        self.outbox.push((socket, tag, Box::new(value)));
        Ok(())
    }

    pub fn recv<T: 'static + Send>(&mut self, socket: &'static str) -> Result<Vec<T>, BusError> {
        let mut out = Vec::new();
        let mut kept = Vec::with_capacity(self.inbox.len());
        let mut last_tag = None;
        for (sock, tag, env) in self.inbox.drain(..) {
            if sock == socket {
                if (*env).type_id() != TypeId::of::<T>() {
                    return Err(BusError::TypeMismatch {
                        socket,
                        expected: std::any::type_name::<T>(),
                    });
                }
                match env.downcast::<T>() {
                    Ok(v) => {
                        out.push(*v);
                        // Reserved sockets are never threaded, so they
                        // must not refine the work cause.
                        if !is_reserved(socket) {
                            last_tag = Some(tag);
                        }
                    }
                    Err(_) => unreachable!("type id verified above"),
                }
            } else {
                kept.push((sock, tag, env));
            }
        }
        self.inbox = kept;
        if let Some(tag) = last_tag {
            self.current_cause = tag;
        }
        Ok(out)
    }

    pub fn recv_all<T: 'static + Send>(&mut self) -> Result<Vec<(&'static str, T)>, BusError> {
        let mut out = Vec::new();
        let mut kept = Vec::new();
        let mut last_tag = None;
        for (sock, tag, env) in self.inbox.drain(..) {
            if (*env).type_id() == TypeId::of::<T>() {
                match env.downcast::<T>() {
                    Ok(v) => {
                        out.push((sock, *v));
                        if !is_reserved(sock) {
                            last_tag = Some(tag);
                        }
                    }
                    Err(_) => unreachable!("type id matched"),
                }
            } else {
                kept.push((sock, tag, env));
            }
        }
        self.inbox = kept;
        if let Some(tag) = last_tag {
            self.current_cause = tag;
        }
        Ok(out)
    }

    /// Like [`recv_all`](Self::recv_all) but also hands back each item's
    /// provenance [`Tag`]. Framework seam for layer-builders (the
    /// highway uses it to carry a tag through its lanes). Ordinary
    /// strands don't need this.
    pub fn recv_tagged<T: 'static + Send>(
        &mut self,
    ) -> Result<Vec<(&'static str, Tag, T)>, BusError> {
        let mut out = Vec::new();
        let mut kept = Vec::new();
        let mut last_tag = None;
        for (sock, tag, env) in self.inbox.drain(..) {
            if (*env).type_id() == TypeId::of::<T>() {
                match env.downcast::<T>() {
                    Ok(v) => {
                        out.push((sock, tag, *v));
                        if !is_reserved(sock) {
                            last_tag = Some(tag);
                        }
                    }
                    Err(_) => unreachable!("type id matched"),
                }
            } else {
                kept.push((sock, tag, env));
            }
        }
        self.inbox = kept;
        if let Some(tag) = last_tag {
            self.current_cause = tag;
        }
        Ok(out)
    }

    /// The thread this strand is currently continuing, if any. A
    /// power-user accessor; ordinary strands ignore provenance.
    pub fn thread_id(&self) -> Tag {
        self.current_cause
    }

    pub fn log(&self, msg: &str) {
        let elapsed = self.kernel.boot.elapsed().as_millis();
        println!("[{:>5}ms] [{}] {}", elapsed, self.strand_name, msg);
    }

    pub fn sleep(&self, d: Duration) {
        thread::sleep(d);
    }

    /// Whether the bus has begun winding down. A strand that does any
    /// long-running or blocking work *inside* `run()` (a loop, a polled
    /// socket read with a timeout) should check this and return promptly
    /// when it is `true`, so graceful shutdown can join its worker. The
    /// kernel exposes the flag (mechanism); honoring it is the strand's
    /// job (policy). The kernel cannot forcibly cancel a thread blocked in
    /// a syscall — that is a real wall in safe Rust — so a strand that
    /// ignores this and blocks forever will be detached after a bounded
    /// wait rather than joined.
    pub fn is_shutting_down(&self) -> bool {
        self.kernel
            .shutdown
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Pull a live [`BusStats`] telemetry snapshot from inside a strand — the
    /// way an orchestrator strand (the spider) or a `webctl top` reporter reads
    /// web-wide health while the bus is running, instead of parsing log lines.
    pub fn stats(&self) -> BusStats {
        self.kernel.stats()
    }

    fn take_outbox(&mut self) -> Vec<Carrier> {
        std::mem::take(&mut self.outbox)
    }
}

#[derive(Debug)]
pub enum BusError {
    TypeMismatch {
        socket: &'static str,
        expected: &'static str,
    },
}

impl fmt::Display for BusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BusError::TypeMismatch { socket, expected } => {
                write!(f, "type mismatch on socket `{}` (expected {})", socket, expected)
            }
        }
    }
}

impl std::error::Error for BusError {}

impl From<BusError> for StrandError {
    fn from(e: BusError) -> Self {
        StrandError::Crashed(e.to_string())
    }
}

// -----------------------------------------------------------------------------
// Subscriber slot
// -----------------------------------------------------------------------------

struct Subscriber {
    sender: Sender<Carrier>,
    name: String,
}

impl Subscriber {
    fn try_send(&self, socket: &'static str, tag: Tag, env: Envelope) -> Result<(), Envelope> {
        match self.sender.send((socket, tag, env)) {
            Ok(()) => Ok(()),
            Err(e) => Err(e.0 .2),
        }
    }
}

// -----------------------------------------------------------------------------
// Per-strand runtime state
// -----------------------------------------------------------------------------

struct StrandSlot {
    /// Owned copy of the strand's name. Used as the slot-map key
    /// and for log/thread naming.
    name: String,
    /// `None` for live (non-restartable) strands registered via
    /// [`Bus::register`]. `Some` for factory-based strands registered
    /// via [`Bus::register_spec`].
    factory: Option<StrandFactory>,
    done_rx: Mutex<Option<Receiver<WorkerExit>>>,
    generation: std::sync::atomic::AtomicUsize,
    join: Mutex<Option<JoinHandle<()>>>,
    /// Messages this slot's current worker has drained from the inbox, for
    /// per-strand throughput in [`BusStats`]. Lock-free; `Relaxed` (telemetry,
    /// not synchronization).
    processed: AtomicU64,
}

#[derive(Debug)]
enum WorkerExit {
    Detach,
    Crashed(String),
}

/// Lock-free kernel-wide counters behind [`BusStats`]. Every field is an
/// independent monotonic tally read/written `Relaxed` — they are for
/// observability, not synchronization, so a reader seeing a slightly stale mix
/// across fields is fine and expected.
#[derive(Default)]
struct KernelStats {
    delivered: AtomicU64,
    pending_evictions: AtomicU64,
    vibrations: AtomicU64,
    registered: AtomicU64,
    restarted: AtomicU64,
    crashed: AtomicU64,
    detached: AtomicU64,
    quarantined: AtomicU64,
    buffered: AtomicU64,
    flushed: AtomicU64,
    threads_formed: AtomicU64,
    intersections_emerged: AtomicU64,
}

// -----------------------------------------------------------------------------
// Kernel
// -----------------------------------------------------------------------------

struct Kernel {
    boot: Instant,
    subscribers: Mutex<HashMap<TypeId, Vec<Arc<Subscriber>>>>,
    pending: Mutex<HashMap<TypeId, Vec<Carrier>>>,
    slots: Mutex<HashMap<String, Arc<StrandSlot>>>,
    /// The emergent fabric: threads and intersections, grown purely
    /// from traffic. Empty at boot. A leaf lock — never held while
    /// holding `subscribers`/`pending`. v1.3.
    fabric: Mutex<Fabric>,
    /// Receiver for control messages. The kernel itself is the only
    /// subscriber to `sys.control`. Strands (the spider, in v1.2)
    /// publish `ControlMsg` here to direct the kernel.
    control_rx: Mutex<Option<Receiver<Carrier>>>,
    /// Name of the registered spider (if any). Set by
    /// [`Bus::register_spider`], which enforces at most one. The kernel
    /// does NOT use it for crash policy — restart is entirely the
    /// spider's job (the kernel always waits for a `ControlMsg` on
    /// crash). It is kept as the single-spider guard and for
    /// introspection.
    spider_name: Mutex<Option<String>>,
    /// Cooperative shutdown flag. Set once when `run_until` is winding
    /// down (max duration, all-done, or `ControlMsg::Shutdown`). Every
    /// strand worker checks it at the top of each tick and returns when
    /// it is set, so the worker thread ends and its strand is DROPPED
    /// (its `Drop` runs — releasing sockets, joining child threads, etc.).
    /// Without this, workers spin forever after `run_until` returns and
    /// strands are never dropped (leaked threads/ports). Relaxed ordering
    /// is sufficient: it is a one-way latch, and the subsequent thread
    /// `join` is the real happens-before edge.
    shutdown: std::sync::atomic::AtomicBool,
    /// Lock-free observability counters, read out by [`Bus::stats`].
    stats: KernelStats,
}

impl Kernel {
    fn new() -> Arc<Self> {
        // The kernel subscribes to its own `sys.control` socket. We
        // allocate the channel up front so the kernel can read from
        // it during `run_until`.
        let (control_tx, control_rx) = mpsc::channel();
        let sub = Arc::new(Subscriber {
            sender: control_tx,
            name: "kernel".to_string(),
        });

        let kernel = Arc::new(Kernel {
            boot: Instant::now(),
            subscribers: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            slots: Mutex::new(HashMap::new()),
            fabric: Mutex::new(Fabric::default()),
            control_rx: Mutex::new(Some(control_rx)),
            spider_name: Mutex::new(None),
            shutdown: std::sync::atomic::AtomicBool::new(false),
            stats: KernelStats::default(),
        });

        kernel
            .subscribers
            .lock()
            .unwrap()
            .entry(TypeId::of::<ControlMsg>())
            .or_default()
            .push(sub);

        kernel
    }

    fn install_slot(
        self: &Arc<Self>,
        name: &str,
        factory: Option<StrandFactory>,
    ) -> Arc<StrandSlot> {
        let (inbox_tx, inbox_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        let slot = Arc::new(StrandSlot {
            name: name.to_string(),
            factory,
            done_rx: Mutex::new(Some(done_rx)),
            generation: std::sync::atomic::AtomicUsize::new(0),
            join: Mutex::new(None),
            processed: AtomicU64::new(0),
        });

        let first_strand = match slot.factory.as_ref() {
            Some(f) => f(),
            None => {
                // No factory → caller MUST provide the first strand
                // via the alternate install_with_strand path. We
                // never hit this branch in practice; this is
                // documentation more than a code path.
                panic!(
                    "install_slot called without a factory and no initial strand; \
                     use install_with_strand for live (non-restartable) strands"
                );
            }
        };
        let strand_name = first_strand.name().to_string();

        for sock in first_strand.inputs() {
            let sub = Arc::new(Subscriber {
                sender: inbox_tx.clone(),
                name: strand_name.clone(),
            });
            self.subscribe(sock.type_id(), sub);
        }

        let kernel_c = self.clone();
        let slot_c = slot.clone();
        let done_tx_c = done_tx.clone();
        let handle = thread::Builder::new()
            .name(format!("strand-{}", slot.name))
            .spawn(move || run_strand_worker(first_strand, kernel_c, slot_c, done_tx_c, inbox_rx))
            .expect("failed to spawn strand thread");
        *slot.join.lock_tolerant() = Some(handle);

        slot
    }

    /// Install a slot wrapping a *live* strand (one that is not
    /// restartable). Used by [`Bus::register`]. The slot has no
    /// factory, so the kernel can never restart it on crash. The
    /// spider will still see the crash event and can quarantine or
    /// do nothing.
    fn install_live(self: &Arc<Self>, strand: Box<dyn Strand>) -> Arc<StrandSlot> {
        let name = strand.name().to_string();
        let (inbox_tx, inbox_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        let slot = Arc::new(StrandSlot {
            name: name.clone(),
            factory: None,
            done_rx: Mutex::new(Some(done_rx)),
            generation: std::sync::atomic::AtomicUsize::new(0),
            join: Mutex::new(None),
            processed: AtomicU64::new(0),
        });

        for sock in strand.inputs() {
            let sub = Arc::new(Subscriber {
                sender: inbox_tx.clone(),
                name: name.clone(),
            });
            self.subscribe(sock.type_id(), sub);
        }

        let kernel_c = self.clone();
        let slot_c = slot.clone();
        let done_tx_c = done_tx.clone();
        let handle = thread::Builder::new()
            .name(format!("strand-{}", name))
            .spawn(move || run_strand_worker(strand, kernel_c, slot_c, done_tx_c, inbox_rx))
            .expect("failed to spawn strand thread");
        *slot.join.lock_tolerant() = Some(handle);

        slot
    }

    /// Restart an existing slot using its stored factory. Returns
    /// the new slot (caller inserts it into the map). If the slot
    /// has no factory (live strand), returns `None`.
    fn restart_slot(
        self: &Arc<Self>,
        name: &str,
        factory: StrandFactory,
    ) -> Arc<StrandSlot> {
        self.install_slot(name, Some(factory))
    }

    /// Maximum number of messages to keep in the per-type pending
    /// buffer. When the buffer is full, the oldest message is
    /// dropped to make room for the new one. This bounds memory
    /// use and prevents stale messages from accumulating forever.
    const PENDING_BUFFER_CAP: usize = 32;

    fn subscribe(&self, type_id: TypeId, sub: Arc<Subscriber>) {
        // Replay the buffer to the new subscriber AND add it to the
        // subscriber list as a single critical section, holding both
        // locks the whole time. This is atomic with respect to
        // `deliver`, which holds the same two locks (same order:
        // subscribers → pending) while it appends to the buffer and
        // snapshots subscribers. The atomicity guarantees every
        // message reaches this subscriber by exactly ONE path: if a
        // concurrent `deliver` runs entirely before this section, its
        // message is in the buffer and gets replayed here; if it runs
        // entirely after, this subscriber is already in the list and
        // gets a direct send. Splitting replay from the list-insert
        // (as before) let a message slip into both paths — a double
        // delivery — or neither — a lost message.
        let flush_event = {
            let mut subs = self.subscribers.lock_tolerant();
            let pending = self.pending.lock_tolerant();

            let evt = match pending.get(&type_id) {
                Some(buf) if !buf.is_empty() => {
                    let count = buf.len();
                    for (socket, tag, env) in buf {
                        let cloned = registry().as_ref().unwrap().clone_env(env);
                        if let Err(returned) = sub.try_send(socket, *tag, cloned) {
                            let _ = returned;
                        }
                    }
                    Some(LifecycleEvent::Flushed {
                        socket: "<pending>",
                        to: sub.name.clone(),
                        count,
                    })
                }
                _ => None,
            };

            subs.entry(type_id).or_default().push(sub);
            evt
        };

        // Emit the Flushed event AFTER releasing both locks:
        // publish_lifecycle → deliver_internal re-locks
        // self.subscribers, and std::sync::Mutex is not reentrant.
        if let Some(evt) = flush_event {
            self.publish_lifecycle(evt);
        }
    }

    fn publish_lifecycle(&self, evt: LifecycleEvent) {
        // Tally by kind for BusStats before the event is moved into the
        // envelope. This is the single choke point for every lifecycle event,
        // so counting here catches them all exactly once.
        let counter = match &evt {
            LifecycleEvent::Registered { .. } => &self.stats.registered,
            LifecycleEvent::Restarted { .. } => &self.stats.restarted,
            LifecycleEvent::Crashed { .. } => &self.stats.crashed,
            LifecycleEvent::Detached { .. } => &self.stats.detached,
            LifecycleEvent::Quarantined { .. } => &self.stats.quarantined,
            LifecycleEvent::Buffered { .. } => &self.stats.buffered,
            LifecycleEvent::Flushed { .. } => &self.stats.flushed,
            LifecycleEvent::ThreadFormed { .. } => &self.stats.threads_formed,
            LifecycleEvent::IntersectionEmerged { .. } => &self.stats.intersections_emerged,
        };
        counter.fetch_add(1, Ordering::Relaxed);

        let env: Envelope = Box::new(evt);
        self.deliver_internal(SYS_LIFECYCLE, env, "kernel");
    }

    /// Build a [`BusStats`] snapshot: lock-free counter reads + one fabric
    /// snapshot + a pass over the slot table. The fabric and slot locks are
    /// taken (and released) separately — never nested — so this never
    /// deadlocks against `deliver`/`run_until`. Safe to call from a strand
    /// mid-run or from the embedder after the run.
    fn stats(&self) -> BusStats {
        let s = &self.stats;
        let fab = self.fabric.lock_tolerant().snapshot();
        let adj = fab.adjacency();
        let edge_traffic_total: u64 = fab.edge_traffic.iter().map(|(_, _, n)| *n).sum();

        // Live subscriber count — locked separately (never nested with fabric
        // or slots), so this adds no deadlock surface.
        let subscribers: usize = self
            .subscribers
            .lock_tolerant()
            .values()
            .map(|v| v.len())
            .sum();

        let mut per_strand: Vec<StrandStats> = self
            .slots
            .lock_tolerant()
            .values()
            .map(|slot| StrandStats {
                name: slot.name.clone(),
                processed: slot.processed.load(Ordering::Relaxed),
            })
            .collect();
        per_strand.sort_by(|a, b| a.name.cmp(&b.name)); // deterministic
        let active_strands = per_strand.len();

        BusStats {
            delivered: s.delivered.load(Ordering::Relaxed),
            pending_evictions: s.pending_evictions.load(Ordering::Relaxed),
            vibrations: s.vibrations.load(Ordering::Relaxed),
            registered: s.registered.load(Ordering::Relaxed),
            restarted: s.restarted.load(Ordering::Relaxed),
            crashed: s.crashed.load(Ordering::Relaxed),
            detached: s.detached.load(Ordering::Relaxed),
            quarantined: s.quarantined.load(Ordering::Relaxed),
            buffered: s.buffered.load(Ordering::Relaxed),
            flushed: s.flushed.load(Ordering::Relaxed),
            threads_formed: s.threads_formed.load(Ordering::Relaxed),
            intersections_emerged: s.intersections_emerged.load(Ordering::Relaxed),
            active_strands,
            subscribers,
            threads: fab.threads.len(),
            intersections: fab.intersections.len(),
            nodes: adj.nodes.len(),
            edges: adj.edges.len(),
            edge_traffic_total,
            per_strand,
        }
    }

    /// Publish a control message. The kernel is the only subscriber
    /// to `sys.control`, so this is effectively a private API.
    /// External orchestrators (the spider) call this through their
    /// strand's `BusHandle::publish`.
    #[allow(dead_code)] // used by v2 control plane
    fn publish_control(&self, msg: ControlMsg) {
        let env: Envelope = Box::new(msg);
        self.deliver_internal(SYS_CONTROL, env, "kernel");
    }

    fn deliver(&self, socket: &'static str, tag: Tag, env: Envelope, from_name: &str) {
        let type_id = (*env).type_id();

        // Clone the value that goes into the replay buffer. The
        // per-subscriber clones happen below, outside the locks.
        let buffered = registry().as_ref().unwrap().clone_env(&env);

        // Decide the tag this message LEAVES `from_name` with. Reserved
        // sys.* sockets are never threaded (pass through). For real
        // work, `Fabric::outbound` continues the thread (or mints a
        // fresh origin for a parentless message) and applies the node's
        // transfer policy, returning any newly-born origin thread so we
        // emit `ThreadFormed` for the right one. The fabric is a leaf
        // lock taken (and released) BEFORE the subscribers/pending
        // critical section, so it never nests with them.
        let reserved = is_reserved(socket);
        let (out_tag, formed): (Tag, Option<ThreadId>) = if reserved {
            (tag, None)
        } else {
            self.fabric.lock_tolerant().outbound(from_name, tag)
        };
        // A fan-out from a default (Stay) node branches the lineage:
        // each extra subscriber gets its own forked sibling thread, so
        // every thread stays a true linear path instead of one
        // fabricated chain through parallel nodes.
        let forking = !reserved && out_tag.is_some() && {
            // Cheap pre-check; the real per-branch forks happen below
            // once we know how many distinct subscribers there are.
            self.fabric.lock_tolerant().is_stay(from_name)
        };

        // Critical section: append to the buffer and snapshot the
        // current subscriber set ATOMICALLY, under both locks in the
        // same order `subscribe` uses (subscribers → pending). This is
        // what makes delivery exactly-once: a strand subscribing
        // concurrently either appears in this snapshot (and gets a
        // direct send below) or replays this message from the buffer
        // when it subscribes — never both, never neither. We snapshot
        // the subscriber Arcs and send AFTER releasing the locks, so we
        // never hold the registry lock while holding these two.
        let (snapshot, was_empty) = {
            let subs = self.subscribers.lock_tolerant();
            let mut pending = self.pending.lock_tolerant();

            let buf = pending.entry(type_id).or_default();
            let was_empty = buf.is_empty();
            if buf.len() >= Self::PENDING_BUFFER_CAP {
                buf.remove(0);
                self.stats.pending_evictions.fetch_add(1, Ordering::Relaxed);
            }
            buf.push((socket, out_tag, buffered));

            let snapshot: Vec<Arc<Subscriber>> =
                subs.get(&type_id).cloned().unwrap_or_default();
            (snapshot, was_empty)
        };

        // Compute the tag each subscriber receives. The first keeps the
        // base thread; under a Stay fan-out, each additional distinct
        // subscriber forks a sibling thread so the lineage is a tree,
        // not a false chain. The replay buffer keeps the base tag (a
        // late fan-out subscriber continues the base — a documented
        // edge), so forking only changes the live sends.
        let sub_tags: Vec<Tag> = if forking && snapshot.len() > 1 {
            let parent = out_tag.expect("forking implies out_tag is Some");
            let mut fabric = self.fabric.lock_tolerant();
            snapshot
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    if i == 0 {
                        out_tag
                    } else {
                        fabric.fork(parent).map(Some).unwrap_or(out_tag)
                    }
                })
                .collect()
        } else {
            vec![out_tag; snapshot.len()]
        };

        // Send to the snapshot outside the locks. Each subscriber gets
        // its own clone carrying its (possibly forked) tag.
        let mut dead: Vec<Arc<Subscriber>> = Vec::new();
        for (sub, sub_tag) in snapshot.iter().zip(sub_tags) {
            let cloned = registry().as_ref().unwrap().clone_env(&env);
            if let Err(returned) = sub.try_send(socket, sub_tag, cloned) {
                eprintln!(
                    "[{:>5}ms] [kernel] ⚠️  {} disconnected mid-deliver",
                    self.boot.elapsed().as_millis(),
                    sub.name
                );
                let _ = returned;
                dead.push(sub.clone());
            }
        }
        // Prune disconnected subscribers so crash/restart churn cannot grow the
        // subscriber map (or per-delivery cost) without bound: every restart
        // installs a fresh subscriber, and the old one's receiver is dropped
        // when its worker exits, leaving a dead entry that nothing else removes.
        // A disconnected subscriber's receiver is gone, so dropping it loses no
        // message, and we match by Arc identity so a live or newly-subscribed
        // entry is never touched. Only runs when a send actually failed, so the
        // steady-state hot path pays nothing.
        if !dead.is_empty() {
            let mut subs = self.subscribers.lock_tolerant();
            if let Some(list) = subs.get_mut(&type_id) {
                list.retain(|s| !dead.iter().any(|d| Arc::ptr_eq(s, d)));
            }
        }
        // Telemetry: count physical deliveries (one per subscriber), and
        // vibrations as a class (one per emitted message, regardless of how
        // many feel it) so the spider/webctl can see web-wide backpressure/
        // starvation volume without parsing log strings.
        self.stats
            .delivered
            .fetch_add(snapshot.len() as u64, Ordering::Relaxed);
        if socket == SYS_VIBRATION {
            self.stats.vibrations.fetch_add(1, Ordering::Relaxed);
        }

        // Record the physical traffic edges this delivery crossed:
        // `from_name -> each subscriber`. Counted HERE, at the physical
        // deliver site, not from a thread's lineage — so the per-edge
        // conductance follows the real publisher->subscriber hop and stays
        // correct under HopTo/MergeAll (which re-parent the tag, never the
        // physical delivery). Reserved sys.* sockets carry no work traffic.
        // (Edge case: a buffer-replay to a late subscriber happens in
        // `subscribe`, which no longer knows the original sender, so those
        // replays are not counted — a minor under-count on relative
        // conductance, not a corruption.)
        if !reserved && !snapshot.is_empty() {
            let mut fabric = self.fabric.lock_tolerant();
            for sub in &snapshot {
                fabric.record_traffic(from_name, &sub.name);
            }
        }

        // Emit lifecycle events after releasing every lock
        // (publish_lifecycle re-locks self.subscribers).
        if was_empty {
            self.publish_lifecycle(LifecycleEvent::Buffered {
                socket,
                from: from_name.to_string(),
            });
        }
        if let Some(id) = formed {
            self.publish_lifecycle(LifecycleEvent::ThreadFormed {
                thread: id,
                origin: from_name.to_string(),
            });
        }
    }

    /// Record, into the fabric, that each tagged work message in
    /// `inbox` was delivered to `node` — growing the thread paths and
    /// emerging intersections. Called by the worker as it drains its
    /// inbox, so it captures replayed messages too. Reserved sockets
    /// and untagged messages are ignored.
    fn record_arrivals(&self, node: &str, inbox: &[Carrier]) {
        let mut events = Vec::new();
        {
            let mut fabric = self.fabric.lock_tolerant();
            for (sock, tag, _env) in inbox {
                if is_reserved(sock) {
                    continue;
                }
                if let Some(tid) = tag {
                    events.extend(fabric.record_arrival(*tid, node));
                }
            }
        }
        for evt in events {
            self.publish_lifecycle(evt);
        }
    }

    fn deliver_internal(&self, socket: &'static str, env: Envelope, from_name: &str) {
        let type_id = (*env).type_id();
        let subs = self.subscribers.lock_tolerant();
        let candidates = match subs.get(&type_id) {
            Some(c) if !c.is_empty() => c,
            _ => return,
        };
        for sub in candidates.iter() {
            let cloned = registry().as_ref().unwrap().clone_env(&env);
            // System messages (lifecycle/control) carry no thread tag.
            if let Err(returned) = sub.try_send(socket, None, cloned) {
                let _ = returned;
            }
        }
        let _ = from_name;
    }
}

// -----------------------------------------------------------------------------
// Worker thread
// -----------------------------------------------------------------------------

fn run_strand_worker(
    mut strand: Box<dyn Strand>,
    kernel: Arc<Kernel>,
    slot: Arc<StrandSlot>,
    done_tx: Sender<WorkerExit>,
    inbox_rx: Receiver<Carrier>,
) {
    let name = strand.name().to_string();
    slot.generation
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let gen = slot.generation.load(std::sync::atomic::Ordering::SeqCst);

    if gen == 1 {
        let inputs: Vec<String> = strand.inputs().iter().map(|s| s.name().to_string()).collect();
        let outputs: Vec<String> = strand.outputs().iter().map(|s| s.name().to_string()).collect();
        println!(
            "[{:>5}ms] [kernel] 🕸️  {} registered ({} inputs, {} outputs)",
            kernel.boot.elapsed().as_millis(),
            name,
            inputs.len(),
            outputs.len()
        );
        kernel.publish_lifecycle(LifecycleEvent::Registered {
            name: name.clone(),
            inputs,
            outputs,
        });
    } else {
        println!(
            "[{:>5}ms] [kernel] 🕸️  {} restarted (gen {})",
            kernel.boot.elapsed().as_millis(),
            name,
            gen
        );
    }

    loop {
        // Cooperative shutdown: the kernel has begun winding down. Return
        // so this thread ends and `strand` is dropped (its `Drop` runs).
        // This is neither a crash nor a detach — emit no lifecycle event;
        // `run_until` has already left its accounting loop and is now
        // joining us. (A strand that blocks indefinitely inside `run()`
        // will delay this check by exactly that long — the shutdown
        // contract is cooperative, one tick of latency at most for a
        // well-behaved strand.)
        if kernel.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }

        let mut inbox: Vec<Carrier> = Vec::new();
        for (sock, tag, env) in inbox_rx.try_iter() {
            inbox.push((sock, tag, env));
        }

        // Per-strand throughput telemetry: count what this tick drained,
        // before `inbox` is moved into the handle.
        if !inbox.is_empty() {
            slot.processed
                .fetch_add(inbox.len() as u64, Ordering::Relaxed);
        }

        // Record arrivals into the fabric before the strand runs, so a
        // node lands on a thread's path the instant a message reaches
        // it (capturing replayed messages too).
        kernel.record_arrivals(&name, &inbox);

        let mut handle = BusHandle::new(name.clone(), inbox, kernel.clone());
        // Isolate the strand's panic. A panicking strand must NOT silently
        // kill its worker — that would skip the Crashed lifecycle event, deny
        // the spider its restart/quarantine decision, and (under Bus::run())
        // hang the whole bus forever, since the slot would never count toward
        // done_count. Catch it and treat it exactly like an explicit crash.
        // (Relies on unwinding panics; `panic = "abort"` is uncatchable, by
        // design — document that for production deployers.)
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| strand.run(&mut handle)));
        let panicked = outcome.is_err();
        let result = match outcome {
            Ok(r) => r,
            Err(payload) => {
                // Recover the panic message so the Crashed reason — which the
                // spider logs and stores in the Quarantined event — is a real
                // forensic signal, not a bare "panicked". Standard panics carry
                // a `&str` (panic!("lit")) or `String` (panic!("{}", x)); a
                // non-string payload is rare (explicit panic_any) and we fall
                // back to a marker.
                let msg = payload
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "panicked (non-string payload)".to_string());
                Err(StrandError::Crashed(format!("panicked: {}", msg)))
            }
        };

        // Deliver this tick's output — but NEVER a half-built outbox from a
        // panicked run (its work did not complete and may be inconsistent).
        if !panicked {
            let outbox = handle.take_outbox();
            for (socket, tag, env) in outbox {
                kernel.deliver(socket, tag, env, &name);
            }
        }

        match result {
            Ok(()) => {
                thread::sleep(Duration::from_millis(10));
                continue;
            }
            Err(StrandError::Detach) => {
                println!(
                    "[{:>5}ms] [kernel] 👋 {} detached",
                    kernel.boot.elapsed().as_millis(),
                    name
                );
                kernel.publish_lifecycle(LifecycleEvent::Detached { name: name.clone() });
                let _ = done_tx.send(WorkerExit::Detach);
                return;
            }
            Err(StrandError::Crashed(reason)) => {
                eprintln!(
                    "[{:>5}ms] [kernel] 💥 {} crashed: {}",
                    kernel.boot.elapsed().as_millis(),
                    name,
                    reason
                );
                kernel.publish_lifecycle(LifecycleEvent::Crashed {
                    name: name.clone(),
                    reason: reason.clone(),
                });
                let _ = done_tx.send(WorkerExit::Crashed(reason));
                return;
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Bus — the user-facing entry point
// -----------------------------------------------------------------------------

pub struct Bus {
    kernel: Arc<Kernel>,
    specs: Vec<StrandSpec>,
}

impl Bus {
    pub fn open() -> Self {
        // Ensure the registry is initialized.
        drop(registry());
        Bus {
            kernel: Kernel::new(),
            specs: Vec::new(),
        }
    }

    pub fn register<S: Strand>(&mut self, strand: S) {
        let name = strand.name().to_string();
        self.detect_collision(&name);
        let strand_box: Box<dyn Strand> = Box::new(strand);
        let slot = self.kernel.install_live(strand_box);
        self.kernel.slots.lock_tolerant().insert(name, slot);
    }

    pub fn register_spec(&mut self, spec: StrandSpec) {
        self.detect_collision_str(spec.name);
        self.specs.push(spec);
    }

    /// Register a strand spec as the orchestrator (spider). The spider
    /// owns ALL crash policy: on a crash the kernel marks the slot dead
    /// and waits for the spider to issue a `ControlMsg`
    /// (Restart/Quarantine) on `sys.control`. With no spider registered,
    /// a crashed strand simply stays dead — the kernel never
    /// auto-restarts (it is a dumb primitive; policy lives in the
    /// orchestrator).
    ///
    /// At most one spider can be registered. Calling this twice
    /// is a panic.
    pub fn register_spider(&mut self, spec: StrandSpec) {
        self.detect_collision_str(spec.name);
        let mut spider_name = self.kernel.spider_name.lock_tolerant();
        if spider_name.is_some() {
            panic!(
                "register_spider called twice; only one spider is supported in v1.2"
            );
        }
        *spider_name = Some(spec.name.to_string());
        drop(spider_name);
        self.specs.push(spec);
    }

    /// Reject silent strand-name collisions. The kernel used to
    /// silently overwrite, which leaked the first slot's
    /// `done_rx` and made its `WorkerExit` unprocessable.
    fn detect_collision(&self, name: &str) {
        let slots = self.kernel.slots.lock_tolerant();
        if slots.contains_key(name) {
            panic!(
                "strand name collision: `{}` is already registered; \
                 each strand must have a unique name",
                name
            );
        }
        let specs = &self.specs;
        for s in specs.iter() {
            if s.name == name {
                panic!(
                    "strand name collision: `{}` is already registered as a spec; \
                     each strand must have a unique name",
                    name
                );
            }
        }
    }

    /// Same as [`detect_collision`] but takes the `&'static str`
    /// that `StrandSpec::new` produces (since specs are
    /// typically built from string literals).
    fn detect_collision_str(&self, name: &'static str) {
        self.detect_collision(name);
    }

    /// A read-only snapshot of the emergent fabric — the threads that
    /// have formed and the intersections that have emerged so far. The
    /// fabric accretes from traffic, so call this after (or during) a
    /// run. Holds no locks once returned.
    pub fn fabric_snapshot(&self) -> FabricSnapshot {
        self.kernel.fabric.lock_tolerant().snapshot()
    }

    /// Pull a [`BusStats`] telemetry snapshot — countable kernel-wide,
    /// lifecycle, and per-strand metrics. Cheap (lock-free counters + one
    /// fabric/slot read); callable during a run (`&self`) or after it.
    pub fn stats(&self) -> BusStats {
        self.kernel.stats()
    }

    /// Set the transfer policy at an intersection `node`. May be called
    /// before the intersection emerges (it is remembered and applied
    /// when it does). The default for every node is
    /// [`TransferPolicy::Stay`] — a no-op — so the fabric stays purely
    /// observational unless explicitly governed here or via
    /// [`ControlMsg::SetTransfer`].
    pub fn set_transfer(&mut self, node: &str, policy: TransferPolicy) {
        self.kernel.fabric.lock_tolerant().set_transfer(node, policy);
    }

    /// Place `node` at a position in the web's 3D space. Pure metadata —
    /// it changes no routing or timing (every node is in the same RAM);
    /// it's read via [`Bus::fabric_snapshot`] for visualization,
    /// organization, and spider policy (e.g. distance-attenuated
    /// vibrations). Overrides any advertised position (operator wins).
    pub fn place(&mut self, node: &str, pos: Position) {
        self.kernel.fabric.lock_tolerant().set_position(node, pos);
    }

    /// Run the bus forever (until every strand is terminally done, Ctrl-C, or
    /// `ControlMsg::Shutdown`).
    ///
    /// **Panic policy (production note).** Each strand runs under
    /// `catch_unwind`, so a panicking strand becomes a `Crashed` lifecycle
    /// event (spider restart/quarantine applies) and is isolated from its
    /// neighbors. This relies on the default `panic = "unwind"`. If you build
    /// with `panic = "abort"`, a single strand panic aborts the **whole
    /// process** — no Crashed event, no spider notification, no clean shutdown.
    /// Keep `unwind` for any deployment that needs strand isolation.
    pub fn run(mut self) {
        self.run_until(None)
    }

    /// Run the bus until `max_duration` elapses (or forever if `None`),
    /// or until all strands finish, Ctrl-C, or `ControlMsg::Shutdown`.
    ///
    /// Takes `&mut self` (not `self`) so the fabric can be inspected
    /// with [`Bus::fabric_snapshot`] after the run returns — the binding
    /// must therefore be `mut`. (v1.3 changed this from by-value `self`.)
    ///
    /// **One-shot.** Despite the `&mut self` borrow, a given `Bus` runs
    /// exactly once: this method takes ownership of the kernel's control
    /// channel and latches the shutdown flag, so a second call panics. The
    /// `&mut` is only to permit post-run `fabric_snapshot` inspection, not
    /// to support re-running. Build a fresh `Bus` to run again.
    pub fn run_until(&mut self, max_duration: Option<Duration>) {
        let kernel = self.kernel.clone();
        let mut specs = std::mem::take(&mut self.specs);

        for spec in specs.drain(..) {
            let name = spec.name;
            let factory = spec.factory;
            let slot = kernel.install_slot(name, Some(factory));
            kernel.slots.lock_tolerant().insert(name.to_string(), slot);
        }

        // Per-strand restart attempt counter, used ONLY to label
        // `Restarted` lifecycle events with an attempt number when the
        // spider sends `ControlMsg::Restart`. The kernel makes no restart
        // decision of its own — all restart/quarantine policy lives in the
        // spider (the kernel is a dumb primitive). It does not drive any
        // "restart-by-default" behavior.
        let mut pending_restarts: HashMap<String, u32> = HashMap::new();
        let start = Instant::now();
        let mut done_count: usize = 0;
        let total_slots: usize = kernel.slots.lock_tolerant().len();

        // Read the control_rx out of the kernel so we can poll it
        // in the main loop.
        let control_rx = kernel.control_rx.lock_tolerant().take().expect(
            "kernel control channel already taken; this is a bug in the kernel",
        );

        'run: loop {
            if let Some(d) = max_duration {
                if start.elapsed() >= d {
                    println!(
                        "[{:>5}ms] [kernel] ⏹  max duration reached, shutting down",
                        kernel.boot.elapsed().as_millis()
                    );
                    break 'run;
                }
            }

            // 1. Drain control messages from the spider.
            for (_sock, _tag, env) in control_rx.try_iter() {
                if let Some(ctrl) = env.downcast_ref::<ControlMsg>() {
                    match ctrl {
                        ControlMsg::Restart { name } => {
                            // The spider wants this strand back.
                            // Look up the (possibly dead) slot in
                            // the map. The slot table uses owned
                            // String keys so we can look up
                            // directly from the message.
                            let factory_opt = kernel
                                .slots
                                .lock()
                                .unwrap()
                                .get(name.as_str())
                                .and_then(|s| s.factory.clone());
                            match factory_opt {
                                Some(factory) => {
                                    let new_slot = kernel.restart_slot(name.as_str(), factory);
                                    kernel.slots.lock_tolerant().insert(name.clone(), new_slot);
                                    let attempt = pending_restarts.entry(name.clone()).or_insert(0);
                                    *attempt += 1;
                                    kernel.publish_lifecycle(LifecycleEvent::Restarted {
                                        name: name.clone(),
                                        attempt: *attempt,
                                    });
                                }
                                None => {
                                    eprintln!(
                                        "[{:>5}ms] [kernel] ⚠️  Restart for non-restartable or unknown strand `{}`",
                                        kernel.boot.elapsed().as_millis(),
                                        name
                                    );
                                }
                            }
                        }
                        ControlMsg::Quarantine { name, reason } => {
                            // The spider gave up on this strand: drop it.
                            // Only count it toward shutdown if it was
                            // actually present — a parked crashed strand
                            // was NOT counted at crash time, so its removal
                            // here balances that. A stale/duplicate
                            // Quarantine (name already gone) must NOT
                            // increment `done_count`, or the phantom count
                            // could reach `total_slots` and shut the bus
                            // down while live strands are still running.
                            kernel.publish_lifecycle(LifecycleEvent::Quarantined {
                                name: name.clone(),
                                reason: reason.clone(),
                            });
                            let removed =
                                kernel.slots.lock_tolerant().remove(name.as_str()).is_some();
                            if removed {
                                done_count += 1;
                            }
                        }
                        ControlMsg::Shutdown => {
                            println!(
                                "[{:>5}ms] [kernel] ⏹  shutdown requested via sys.control",
                                kernel.boot.elapsed().as_millis()
                            );
                            // Wind down through the shared path (signal +
                            // join), not a bare return — so strands are
                            // dropped here too.
                            break 'run;
                        }
                        ControlMsg::SetTransfer { node, policy } => {
                            kernel.fabric.lock_tolerant().set_transfer(node, *policy);
                            println!(
                                "[{:>5}ms] [kernel] 🔀 transfer policy at `{}` set to {:?}",
                                kernel.boot.elapsed().as_millis(),
                                node,
                                policy
                            );
                        }
                    }
                }
            }

            // 2. Drain worker exits. On crash, the kernel does NOT
            // restart — the spider does, by sending ControlMsg::Restart.
            // The kernel removes the slot. If the spider doesn't act
            // within a tick, the slot stays gone.
            let slots = kernel.slots.lock_tolerant().clone();

            for (name, slot) in slots.iter() {
                let done_rx_opt = slot.done_rx.lock_tolerant().take();
                let Some(done_rx) = done_rx_opt else { continue };
                match done_rx.try_recv() {
                    Ok(WorkerExit::Detach) => {
                        kernel.slots.lock_tolerant().remove(name);
                        done_count += 1;
                    }
                    Ok(WorkerExit::Crashed(_reason)) => {
                        // The kernel is plumbing: it never decides restart
                        // policy. The worker already published the
                        // `Crashed` lifecycle event. Restart lives ENTIRELY
                        // in the spider — the kernel never auto-restarts.
                        // (v1.2 had a 3-retry-then-quarantine fallback baked
                        // in here; removed in v1.3 so the kernel stays a
                        // dumb primitive and all crash policy is the
                        // spider's.)
                        //
                        // The one thing the kernel still owns is its own
                        // run-loop termination accounting (`done_count`):
                        // it must know whether this crash is terminal. A
                        // crashed strand can be revived only by a spider
                        // sending `Restart` — so if a spider is registered,
                        // park the slot (its `done_rx` was taken above, so
                        // it's skipped every later tick) and wait for the
                        // spider's `ControlMsg`. The slot stays in the map
                        // so its factory is available for that Restart.
                        //
                        // With NO spider, nothing can ever revive it, so
                        // the crash is final: drop the slot and count it
                        // done, so a spiderless `Bus::run()` still winds
                        // down when its strands die instead of hanging
                        // forever. This is accounting, not policy — the
                        // kernel still restarts nothing on its own.
                        let has_spider =
                            kernel.spider_name.lock_tolerant().is_some();
                        if !has_spider {
                            kernel.slots.lock_tolerant().remove(name);
                            done_count += 1;
                        }
                    }
                    Err(_) => {
                        *slot.done_rx.lock_tolerant() = Some(done_rx);
                    }
                }
            }

            if total_slots > 0 && done_count >= total_slots {
                println!(
                    "[{:>5}ms] [kernel] ⏹  all strands done, shutting down",
                    kernel.boot.elapsed().as_millis()
                );
                break 'run;
            }

            thread::sleep(Duration::from_millis(50));
        }

        // Graceful shutdown. Every exit from the run loop lands here.
        // Signal all workers to stop, then JOIN them so each strand is
        // actually dropped (its `Drop` runs — e.g. an EdgeListener tears
        // down its reader thread and releases its bound port) before
        // `run_until` returns. Workers that already exited (detached,
        // crashed, quarantined) had their slots removed from the map; we
        // join whatever live workers remain. Parked (crashed-with-spider)
        // slots stay in the map; their thread already returned, so the
        // join completes instantly.
        kernel
            .shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let joins: Vec<JoinHandle<()>> = {
            let slots = kernel.slots.lock_tolerant();
            slots
                .values()
                .filter_map(|slot| slot.join.lock_tolerant().take())
                .collect()
        };
        let pending = joins.len();
        if pending > 0 {
            // Best-effort BOUNDED join. A well-behaved strand returns within
            // a tick of seeing the flag, so this resolves almost instantly.
            // But a strand that blocks indefinitely inside `run()` (a wedged
            // socket read, a too-long sleep) can never be force-killed in
            // safe Rust — joining it directly would hang shutdown forever.
            // So we join on a helper thread and cap the wait: past the
            // deadline we warn and return, leaving the stuck worker detached
            // (it dies with the process). The bus shutting down cleanly is
            // worth more than waiting on one misbehaving strand.
            const WORKER_JOIN_DEADLINE: Duration = Duration::from_secs(5);
            let (fin_tx, fin_rx) = mpsc::channel();
            thread::Builder::new()
                .name("strand-joiner".to_string())
                .spawn(move || {
                    for handle in joins {
                        let _ = handle.join();
                    }
                    let _ = fin_tx.send(());
                })
                .expect("failed to spawn joiner thread");
            match fin_rx.recv_timeout(WORKER_JOIN_DEADLINE) {
                Ok(()) => {
                    println!(
                        "[{:>5}ms] [kernel] 🧹 joined {} worker(s); strands dropped",
                        kernel.boot.elapsed().as_millis(),
                        pending
                    );
                }
                Err(_) => {
                    eprintln!(
                        "[{:>5}ms] [kernel] ⚠️  {} worker(s) did not stop within {:?}; \
                         detaching — a strand is blocking inside run() and ignoring \
                         is_shutting_down()",
                        kernel.boot.elapsed().as_millis(),
                        pending,
                        WORKER_JOIN_DEADLINE,
                    );
                }
            }
        }
    }
}

impl Default for Bus {
    fn default() -> Self {
        Self::open()
    }
}

// =============================================================================
// Tests
// =============================================================================
//
// These are kernel-level unit tests. They exercise the public API
// of the `spiderweb` crate, which is the surface a strand author
// touches. The internal types (Kernel, StrandSlot) are not exposed
// and are not tested directly here — they're tested transitively
// through Bus run_until and lifecycle observation.
//
// To run with the kernel's println! noise visible:
//   cargo test -p spiderweb -- --nocapture

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    // ----- Poison tolerance -------------------------------------------------

    /// `lock_tolerant` must recover a poisoned mutex instead of panicking, so a
    /// single panic in a kernel critical section cannot cascade into every
    /// later lock site and kill the bus. We poison a mutex (panic while holding
    /// the guard on another thread), confirm a plain `.lock()` now reports
    /// poison, then confirm `lock_tolerant()` still hands back the inner value.
    #[test]
    fn lock_tolerant_recovers_a_poisoned_mutex() {
        let m = Arc::new(Mutex::new(7u32));
        let m2 = m.clone();

        // Poison it: take the guard, then panic. The panic is caught by the
        // joining thread; the mutex is left poisoned.
        let _ = std::thread::spawn(move || {
            let _guard = m2.lock().unwrap();
            panic!("poison the mutex");
        })
        .join();

        assert!(
            m.lock().is_err(),
            "sanity: the mutex should be poisoned after a panic-while-held"
        );
        let guard = m.lock_tolerant();
        assert_eq!(
            *guard, 7,
            "lock_tolerant must recover the inner value despite poison"
        );
    }

    // ----- Test strands -----------------------------------------------------

    /// A test source that publishes a fixed list of strings and
    /// then detaches. Used by fan-out and buffer-and-flush tests.
    struct TestSource {
        messages: Vec<String>,
        index: usize,
    }

    impl TestSource {
        fn new(messages: Vec<String>) -> Self {
            TestSource { messages, index: 0 }
        }
    }

    impl Strand for TestSource {
        fn name(&self) -> &str { "test-source" }
        fn inputs(&self) -> &[Socket] { &[] }
        fn outputs(&self) -> &[Socket] {
            const SOCK: Socket = Socket::new::<String>("out.test");
            std::slice::from_ref(&SOCK)
        }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            if self.index >= self.messages.len() {
                return Err(StrandError::Detach);
            }
            let msg = self.messages[self.index].clone();
            self.index += 1;
            bus.publish("out.test", msg)?;
            bus.sleep(Duration::from_millis(20));
            Ok(())
        }
    }

    /// A test sink that counts how many messages it received and
    /// stores the last one for assertion. Used by fan-out tests.
    struct TestSink {
        tag: &'static str,
        count: Arc<AtomicU32>,
        last: Arc<Mutex<String>>,
    }

    impl TestSink {
        fn new(tag: &'static str) -> (Self, Arc<AtomicU32>, Arc<Mutex<String>>) {
            let count = Arc::new(AtomicU32::new(0));
            let last = Arc::new(Mutex::new(String::new()));
            (
                TestSink { tag, count: count.clone(), last: last.clone() },
                count,
                last,
            )
        }
    }

    impl Strand for TestSink {
        fn name(&self) -> &str { self.tag }
        fn inputs(&self) -> &[Socket] {
            const SOCK: Socket = Socket::new::<String>("in.test");
            std::slice::from_ref(&SOCK)
        }
        fn outputs(&self) -> &[Socket] { &[] }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            let msgs = bus.recv_all::<String>()?;
            for (_sock, msg) in msgs {
                self.count.fetch_add(1, Ordering::SeqCst);
                *self.last.lock_tolerant() = msg;
            }
            bus.sleep(Duration::from_millis(20));
            Ok(())
        }
    }

    /// A test sink that subscribes to a SECOND socket type. Used to
    /// verify that type-based routing doesn't cross-contaminate.
    struct TestSinkOther {
        count: Arc<AtomicU32>,
    }

    impl Strand for TestSinkOther {
        fn name(&self) -> &str { "test-sink-other" }
        fn inputs(&self) -> &[Socket] {
            const SOCK: Socket = Socket::new::<i64>("in.other");
            std::slice::from_ref(&SOCK)
        }
        fn outputs(&self) -> &[Socket] { &[] }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            let msgs = bus.recv_all::<i64>()?;
            self.count.fetch_add(msgs.len() as u32, Ordering::SeqCst);
            bus.sleep(Duration::from_millis(20));
            Ok(())
        }
    }

    /// A custom Clone type used to verify the clone registry works
    /// for user-defined payloads (not just the pre-registered ones).
    #[derive(Debug, Clone, PartialEq)]
    struct CustomPayload {
        n: u64,
        s: String,
    }

    /// A source that publishes one CustomPayload and detaches.
    struct CustomSource {
        payload: CustomPayload,
        published: Arc<AtomicU32>,
    }

    impl Strand for CustomSource {
        fn name(&self) -> &str { "custom-source" }
        fn inputs(&self) -> &[Socket] { &[] }
        fn outputs(&self) -> &[Socket] {
            const SOCK: Socket = Socket::new::<CustomPayload>("out.custom");
            std::slice::from_ref(&SOCK)
        }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            self.published.fetch_add(1, Ordering::SeqCst);
            bus.publish("out.custom", self.payload.clone())?;
            Err(StrandError::Detach)
        }
    }

    /// A sink that receives CustomPayload and stores the last one
    /// for assertion. Verifies the clone registry produced a
    /// correct copy (not just any value).
    struct CustomSink {
        name: String,
        last: Arc<Mutex<Option<CustomPayload>>>,
    }

    impl Strand for CustomSink {
        fn name(&self) -> &str { &self.name }
        fn inputs(&self) -> &[Socket] {
            const SOCK: Socket = Socket::new::<CustomPayload>("in.custom");
            std::slice::from_ref(&SOCK)
        }
        fn outputs(&self) -> &[Socket] { &[] }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            let msgs = bus.recv_all::<CustomPayload>()?;
            for (_sock, msg) in msgs {
                *self.last.lock_tolerant() = Some(msg);
            }
            bus.sleep(Duration::from_millis(20));
            Ok(())
        }
    }

    /// A sink that observes `sys.lifecycle` and counts events. Used
    /// to assert that the kernel publishes lifecycle events when
    /// strands register.
    struct LifecycleObserver {
        registered: Arc<AtomicU32>,
        detached: Arc<AtomicU32>,
    }

    impl Strand for LifecycleObserver {
        fn name(&self) -> &str { "lifecycle-observer" }
        fn inputs(&self) -> &[Socket] {
            const SOCK: Socket = Socket::new::<LifecycleEvent>(SYS_LIFECYCLE);
            std::slice::from_ref(&SOCK)
        }
        fn outputs(&self) -> &[Socket] { &[] }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            let events = bus.recv_all::<LifecycleEvent>()?;
            for (_sock, evt) in events {
                match evt {
                    LifecycleEvent::Registered { .. } => {
                        self.registered.fetch_add(1, Ordering::SeqCst);
                    }
                    LifecycleEvent::Detached { .. } => {
                        self.detached.fetch_add(1, Ordering::SeqCst);
                    }
                    _ => {}
                }
            }
            bus.sleep(Duration::from_millis(20));
            Ok(())
        }
    }

    // ----- Socket-level unit tests (no Bus) --------------------------------

    /// Two sockets with the same name but different types must
    /// produce different TypeIds. This is the property the kernel
    /// uses to route messages.
    #[test]
    fn socket_type_id_uses_type_not_name() {
        let s_str: Socket = Socket::new::<String>("shared");
        let s_i64: Socket = Socket::new::<i64>("shared");
        assert_ne!(s_str.type_id(), s_i64.type_id());
        assert_eq!(s_str.name(), s_i64.name());
    }

    /// Two sockets of the same type with different names must
    /// produce the same TypeId. The kernel uses TypeId as the
    /// routing key, so name-only routing would be a bug.
    #[test]
    fn socket_type_id_independent_of_name() {
        let a: Socket = Socket::new::<String>("alpha");
        let b: Socket = Socket::new::<String>("beta");
        assert_eq!(a.type_id(), b.type_id());
    }

    // ----- Clone registry tests -------------------------------------------

    /// Registering a custom `Clone` type is observable (it doesn't
    /// panic, doesn't error, doesn't replace existing entries).
    /// The actual fan-out is exercised in `custom_clone_type_fans_out`
    /// below.
    #[test]
    fn register_clone_type_does_not_panic() {
        register_clone_type::<CustomPayload>();
        // Idempotent: registering twice is a no-op because the
        // registry overwrites. We just verify it doesn't panic.
        register_clone_type::<CustomPayload>();
    }

    // ----- Integration tests (Bus-level) -----------------------------------

    /// One source, two sinks. Both sinks receive every message.
    /// This is the foundational fan-out property of the bus.
    #[test]
    fn fan_out_by_type() {
        let (sink_a, count_a, last_a) = TestSink::new("sink-A");
        let (sink_b, count_b, last_b) = TestSink::new("sink-B");

        let mut bus = Bus::open();
        bus.register(TestSource::new(vec![
            "first".into(),
            "second".into(),
            "third".into(),
        ]));
        bus.register(sink_a);
        bus.register(sink_b);
        bus.run_until(Some(Duration::from_millis(300)));

        assert_eq!(count_a.load(Ordering::SeqCst), 3, "sink-A should see all 3 messages");
        assert_eq!(count_b.load(Ordering::SeqCst), 3, "sink-B should see all 3 messages");
        assert_eq!(*last_a.lock_tolerant(), "third");
        assert_eq!(*last_b.lock_tolerant(), "third");
    }

    /// The kernel buffers publishes that arrive before a subscriber
    /// is registered and replays the buffer to the new subscriber.
    /// This is the property that lets strands start in any order.
    /// The test registers a source that publishes two messages,
    /// then a sink that should receive both via the buffer-flush
    /// path. Asserts `got == 2` (exact) so a regression that
    /// breaks replay is caught.
    #[test]
    fn buffer_and_flush_on_subscriber_arrival() {
        // Use a sink that wraps the TestSink in an Arc<Mutex> so
        // it can be cloned into the factory closure.
        let count = Arc::new(AtomicU32::new(0));
        let last = Arc::new(Mutex::new(String::new()));
        let sink_state = (count.clone(), last.clone());
        let sink_factory = move || {
            let (c, l) = sink_state.clone();
            Box::new(SinkFacade { count: c, last: l }) as Box<dyn Strand>
        };

        let mut bus = Bus::open();
        // Register the source first. The source's worker thread
        // starts immediately and begins publishing. If the sink
        // is registered AFTER the source has published, the
        // messages are in the buffer; the sink should receive
        // them via replay.
        bus.register(TestSource::new(vec!["buffered-1".into(), "buffered-2".into()]));
        // The sink is registered via register_spec so it
        // exists in the spec list and gets installed at the
        // same time as the source. (Whether the sink is online
        // before the source publishes is timing-dependent;
        // either way, deliver() now always buffers, so the
        // replay-on-subscribe path is the only path.)
        bus.register_spec(StrandSpec::new("late-sink", sink_factory));
        bus.run_until(Some(Duration::from_millis(300)));

        // Both messages must reach the sink.
        let got = count.load(Ordering::SeqCst);
        assert_eq!(got, 2, "sink should have received exactly 2 messages (got {})", got);
        // And the last message we observed should be one we sent.
        let last_str = last.lock_tolerant().clone();
        assert!(
            last_str.starts_with("buffered-"),
            "last message should be one we sent, got `{}`",
            last_str
        );
    }

    /// A simpler sink wrapper that holds Arc state and is
    /// Clone-friendly. Used by the buffer-and-flush test to
    /// satisfy the `Fn` (not `FnOnce`) bound on StrandSpec.
    #[derive(Clone)]
    struct SinkFacade {
        count: Arc<AtomicU32>,
        last: Arc<Mutex<String>>,
    }

    impl Strand for SinkFacade {
        fn name(&self) -> &str { "sink-facade" }
        fn inputs(&self) -> &[Socket] {
            const SOCK: Socket = Socket::new::<String>("in.test");
            std::slice::from_ref(&SOCK)
        }
        fn outputs(&self) -> &[Socket] { &[] }
        fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
            let msgs = bus.recv_all::<String>()?;
            for (_sock, msg) in msgs {
                self.count.fetch_add(1, Ordering::SeqCst);
                *self.last.lock_tolerant() = msg;
            }
            bus.sleep(Duration::from_millis(20));
            Ok(())
        }
    }

    /// Type-based routing must not cross-contaminate. A sink
    /// subscribed to `i64` must not receive a `String` published
    /// on a same-named socket.
    #[test]
    fn type_routing_does_not_cross_contaminate() {
        let (sink_str, count_str, _) = TestSink::new("str-sink");
        let other_count = Arc::new(AtomicU32::new(0));
        let sink_other = TestSinkOther { count: other_count.clone() };

        let mut bus = Bus::open();
        bus.register(TestSource::new(vec!["hello".into(), "world".into()]));
        bus.register(sink_str);
        bus.register(sink_other);
        bus.run_until(Some(Duration::from_millis(200)));

        assert_eq!(count_str.load(Ordering::SeqCst), 2);
        // The other sink has no source feeding it i64, so it
        // should see zero messages.
        assert_eq!(other_count.load(Ordering::SeqCst), 0);
    }

    /// When a strand registers, the kernel publishes a
    /// `LifecycleEvent::Registered` to `sys.lifecycle`. A strand
    /// subscribed to sys.lifecycle can observe this.
    #[test]
    fn lifecycle_registered_event_published() {
        let observer = LifecycleObserver {
            registered: Arc::new(AtomicU32::new(0)),
            detached: Arc::new(AtomicU32::new(0)),
        };
        let registered = observer.registered.clone();
        let detached = observer.detached.clone();

        let mut bus = Bus::open();
        bus.register(observer);
        bus.register(TestSource::new(vec!["one".into(), "two".into()]));
        bus.run_until(Some(Duration::from_millis(200)));

        let got = registered.load(Ordering::SeqCst);
        // The observer itself + the source should both register.
        assert!(got >= 2, "expected at least 2 Registered events (observer + source), got {}", got);
        // The source detaches after publishing.
        let detached_count = detached.load(Ordering::SeqCst);
        assert!(detached_count >= 1, "expected at least 1 Detached event from the source, got {}", detached_count);
    }

    /// A source that returns `StrandError::Detach` exits cleanly.
    /// The kernel removes the slot and the run loop continues for
    /// any remaining strands.
    #[test]
    fn clean_detach_does_not_crash() {
        let (sink, count, _) = TestSink::new("post-detach-sink");
        let mut bus = Bus::open();
        bus.register(TestSource::new(vec!["only".into()]));
        bus.register(sink);
        // The source detaches after one message, the sink
        // continues to exist. The bus should not panic; run_until
        // bounds the duration.
        bus.run_until(Some(Duration::from_millis(200)));
        // Sink should have received the one message.
        assert!(count.load(Ordering::SeqCst) >= 1);
    }

    /// A custom `Clone` type that has been registered with
    /// `register_clone_type` must fan out correctly. This is the
    /// primary extension point for user-defined payloads.
    #[test]
    fn custom_clone_type_fans_out() {
        register_clone_type::<CustomPayload>();
        let payload = CustomPayload { n: 42, s: "hello".into() };

        let last_a = Arc::new(Mutex::new(None::<CustomPayload>));
        let last_b = Arc::new(Mutex::new(None::<CustomPayload>));

        let sink_a = CustomSink { name: "custom-sink-A".into(), last: last_a.clone() };
        let sink_b = CustomSink { name: "custom-sink-B".into(), last: last_b.clone() };
        let published = Arc::new(AtomicU32::new(0));
        let source = CustomSource {
            payload: payload.clone(),
            published: published.clone(),
        };

        let mut bus = Bus::open();
        bus.register(source);
        bus.register(sink_a);
        bus.register(sink_b);
        bus.run_until(Some(Duration::from_millis(200)));

        assert_eq!(published.load(Ordering::SeqCst), 1);
        let a = last_a.lock_tolerant().clone();
        let b = last_b.lock_tolerant().clone();
        assert_eq!(a, Some(payload.clone()), "sink-A should see the exact payload");
        assert_eq!(b, Some(payload.clone()), "sink-B should see the exact payload");
    }

    /// A burst published at startup, while a subscriber is still being
    /// registered, must be delivered EXACTLY once — no duplicate, no
    /// loss. This guards the deliver/subscribe race: if the buffer
    /// append and the subscriber-list insert are not atomic with
    /// respect to each other, a message can land in both the replay
    /// and the direct-send paths (double delivery) or in neither
    /// (loss). The race is timing-dependent, so we loop. The bug it
    /// guards against was caught by the highway `congestion` demo
    /// delivering 25 items from a burst of 24.
    #[test]
    fn burst_at_startup_delivers_exactly_once() {
        struct BurstSource {
            n: usize,
            sent: bool,
        }
        impl Strand for BurstSource {
            fn name(&self) -> &str {
                "burst-source"
            }
            fn inputs(&self) -> &[Socket] {
                &[]
            }
            fn outputs(&self) -> &[Socket] {
                const SOCK: Socket = Socket::new::<String>("burst.out");
                std::slice::from_ref(&SOCK)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                if self.sent {
                    return Err(StrandError::Detach);
                }
                for i in 0..self.n {
                    bus.publish("burst.out", format!("m{i}"))?;
                }
                self.sent = true;
                Ok(())
            }
        }

        // N stays under PENDING_BUFFER_CAP (32) so the bounded buffer
        // never drops a message and "exactly N" is the correct
        // expectation.
        const N: usize = 25;
        for iter in 0..12 {
            let (sink, count, _last) = TestSink::new("burst-sink");
            let mut bus = Bus::open();
            bus.register(BurstSource { n: N, sent: false });
            bus.register(sink);
            bus.run_until(Some(Duration::from_millis(150)));
            let got = count.load(Ordering::SeqCst) as usize;
            assert_eq!(got, N, "iteration {iter}: expected exactly {N}, got {got}");
        }
    }

    // ----- Fabric: threads & intersections (v1.3) ------------------------

    /// A thread forms as a message flows and grows through MANY nodes —
    /// a desire path, not a single A->B hop. A linear source -> mapA ->
    /// mapB -> sink chain must produce exactly one thread whose
    /// direction is the ordered node path.
    #[test]
    fn thread_forms_through_many_nodes() {
        #[derive(Clone)]
        struct A;
        #[derive(Clone)]
        struct B;
        #[derive(Clone)]
        struct C;
        register_clone_type::<A>();
        register_clone_type::<B>();
        register_clone_type::<C>();

        struct Src {
            sent: bool,
        }
        impl Strand for Src {
            fn name(&self) -> &str {
                "source"
            }
            fn inputs(&self) -> &[Socket] {
                &[]
            }
            fn outputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<A>("a");
                std::slice::from_ref(&S)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                if !self.sent {
                    bus.publish("a", A)?;
                    self.sent = true;
                }
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }
        struct MapA;
        impl Strand for MapA {
            fn name(&self) -> &str {
                "mapA"
            }
            fn inputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<A>("a");
                std::slice::from_ref(&S)
            }
            fn outputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<B>("b");
                std::slice::from_ref(&S)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                for _ in bus.recv_all::<A>()? {
                    bus.publish("b", B)?;
                }
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }
        struct MapB;
        impl Strand for MapB {
            fn name(&self) -> &str {
                "mapB"
            }
            fn inputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<B>("b");
                std::slice::from_ref(&S)
            }
            fn outputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<C>("c");
                std::slice::from_ref(&S)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                for _ in bus.recv_all::<B>()? {
                    bus.publish("c", C)?;
                }
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }
        struct Snk;
        impl Strand for Snk {
            fn name(&self) -> &str {
                "sink"
            }
            fn inputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<C>("c");
                std::slice::from_ref(&S)
            }
            fn outputs(&self) -> &[Socket] {
                &[]
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                let _ = bus.recv_all::<C>()?;
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }

        let mut bus = Bus::open();
        bus.register(Snk);
        bus.register(MapB);
        bus.register(MapA);
        bus.register(Src { sent: false });
        bus.run_until(Some(Duration::from_millis(400)));

        let snap = bus.fabric_snapshot();
        assert_eq!(
            snap.threads.len(),
            1,
            "exactly one thread should form, got {:?}",
            snap.threads
        );
        assert_eq!(
            snap.threads[0].direction,
            vec![
                "source".to_string(),
                "mapA".to_string(),
                "mapB".to_string(),
                "sink".to_string()
            ],
            "the thread should be the ordered desire path through all four nodes"
        );
    }

    /// No traffic, no thread. The fabric is a residue of real flow.
    #[test]
    fn thread_does_not_form_without_flow() {
        #[derive(Clone)]
        struct Z;
        register_clone_type::<Z>();
        struct Idle;
        impl Strand for Idle {
            fn name(&self) -> &str {
                "idle"
            }
            fn inputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<Z>("z");
                std::slice::from_ref(&S)
            }
            fn outputs(&self) -> &[Socket] {
                &[]
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                let _ = bus.recv_all::<Z>()?;
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }
        let mut bus = Bus::open();
        bus.register(Idle);
        bus.run_until(Some(Duration::from_millis(100)));
        assert!(
            bus.fabric_snapshot().threads.is_empty(),
            "no message flowed, so no thread should have formed"
        );
    }

    /// An intersection emerges where two distinct threads cross a node —
    /// discovered from traffic, never declared. Two sources feeding one
    /// joiner make exactly that node an intersection, and a node only
    /// one thread reaches is NOT one.
    #[test]
    fn intersection_emerges_at_fan_in() {
        #[derive(Clone)]
        struct X;
        register_clone_type::<X>();

        struct Src {
            name: &'static str,
            sent: bool,
        }
        impl Strand for Src {
            fn name(&self) -> &str {
                self.name
            }
            fn inputs(&self) -> &[Socket] {
                &[]
            }
            fn outputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<X>("x");
                std::slice::from_ref(&S)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                if !self.sent {
                    bus.publish("x", X)?;
                    self.sent = true;
                }
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }
        struct Join;
        impl Strand for Join {
            fn name(&self) -> &str {
                "joiner"
            }
            fn inputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<X>("x");
                std::slice::from_ref(&S)
            }
            fn outputs(&self) -> &[Socket] {
                &[]
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                let _ = bus.recv_all::<X>()?;
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }

        let mut bus = Bus::open();
        bus.register(Join);
        bus.register(Src {
            name: "s1",
            sent: false,
        });
        bus.register(Src {
            name: "s2",
            sent: false,
        });
        bus.run_until(Some(Duration::from_millis(300)));

        let snap = bus.fabric_snapshot();
        let isx = snap
            .intersection("joiner")
            .expect("an intersection should have emerged at the joiner");
        assert_eq!(
            isx.threads.len(),
            2,
            "the joiner should bind both source threads, got {:?}",
            isx.threads
        );
        assert!(
            snap.intersection("s1").is_none(),
            "a node only one thread reaches is not an intersection"
        );
    }

    /// Helper used by both transfer-policy tests: two sources -> a
    /// precise fan-in node `N` -> one sink. `N` uses the tagged API so
    /// each input continues its own thread; the transfer policy then
    /// governs what leaves `N`.
    fn run_transfer_flow(policy: Option<TransferPolicy>) -> FabricSnapshot {
        #[derive(Clone)]
        struct X;
        #[derive(Clone)]
        struct Y;
        register_clone_type::<X>();
        register_clone_type::<Y>();

        struct Src {
            name: &'static str,
            sent: bool,
        }
        impl Strand for Src {
            fn name(&self) -> &str {
                self.name
            }
            fn inputs(&self) -> &[Socket] {
                &[]
            }
            fn outputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<X>("x");
                std::slice::from_ref(&S)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                if !self.sent {
                    bus.publish("x", X)?;
                    self.sent = true;
                }
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }
        struct N;
        impl Strand for N {
            fn name(&self) -> &str {
                "N"
            }
            fn inputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<X>("x");
                std::slice::from_ref(&S)
            }
            fn outputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<Y>("y");
                std::slice::from_ref(&S)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                for (_sock, tag, _x) in bus.recv_tagged::<X>()? {
                    bus.publish_tagged("y", tag, Y)?;
                }
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }
        struct Snk;
        impl Strand for Snk {
            fn name(&self) -> &str {
                "sink"
            }
            fn inputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<Y>("y");
                std::slice::from_ref(&S)
            }
            fn outputs(&self) -> &[Socket] {
                &[]
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                let _ = bus.recv_all::<Y>()?;
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }

        let mut bus = Bus::open();
        if let Some(p) = policy {
            bus.set_transfer("N", p);
        }
        bus.register(Snk);
        bus.register(N);
        bus.register(Src {
            name: "s1",
            sent: false,
        });
        bus.register(Src {
            name: "s2",
            sent: false,
        });
        bus.run_until(Some(Duration::from_millis(400)));
        bus.fabric_snapshot()
    }

    /// Default `Stay`: threads cross at `N` but keep their identity, so
    /// both source threads flow through to the sink. Enabling the
    /// fabric changes no routing.
    #[test]
    fn transfer_policy_stay_continues_through() {
        let snap = run_transfer_flow(None);
        let source_threads: Vec<&Thread> = snap
            .threads
            .iter()
            .filter(|t| matches!(t.direction.first().map(String::as_str), Some("s1") | Some("s2")))
            .collect();
        assert_eq!(source_threads.len(), 2, "two source threads expected");
        for t in source_threads {
            assert!(
                t.direction.iter().any(|n| n == "sink"),
                "under Stay, source thread {:?} should reach the sink",
                t.direction
            );
        }
    }

    /// `MergeAll`: outputs of `N` are fused onto one merged thread, so
    /// the source threads end at `N` and a single fused thread carries
    /// the work onward to the sink — a governed hop.
    #[test]
    fn transfer_policy_mergeall_fuses() {
        let snap = run_transfer_flow(Some(TransferPolicy::MergeAll));
        for t in &snap.threads {
            let origin = t.direction.first().map(String::as_str);
            if matches!(origin, Some("s1") | Some("s2")) {
                assert!(
                    !t.direction.iter().any(|n| n == "sink"),
                    "under MergeAll, source thread {:?} must NOT continue to the sink",
                    t.direction
                );
            }
        }
        // The fused thread is rooted at the real merge node `N` and is
        // the one that carries work onward to the sink.
        let fused_reaches_sink = snap.threads.iter().any(|t| {
            t.direction.first().map(String::as_str) == Some("N")
                && t.direction.iter().any(|n| n == "sink")
        });
        assert!(
            fused_reaches_sink,
            "a fused merge thread rooted at N should carry work to the sink, threads: {:?}",
            snap.threads
        );
    }

    /// Fan-out branches the lineage: one source feeding two distinct
    /// sinks produces two sibling threads sharing the [source] prefix —
    /// NOT one fabricated chain [source, sink-A, sink-B]. Guards the
    /// fan-out forking fix.
    #[test]
    fn fan_out_branches_into_sibling_threads() {
        #[derive(Clone)]
        struct S;
        register_clone_type::<S>();

        struct Src {
            sent: bool,
        }
        impl Strand for Src {
            fn name(&self) -> &str {
                "source"
            }
            fn inputs(&self) -> &[Socket] {
                &[]
            }
            fn outputs(&self) -> &[Socket] {
                const SK: Socket = Socket::new::<S>("s");
                std::slice::from_ref(&SK)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                if !self.sent {
                    bus.publish("s", S)?;
                    self.sent = true;
                }
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }
        struct Snk {
            tag: &'static str,
        }
        impl Strand for Snk {
            fn name(&self) -> &str {
                self.tag
            }
            fn inputs(&self) -> &[Socket] {
                const SK: Socket = Socket::new::<S>("s");
                std::slice::from_ref(&SK)
            }
            fn outputs(&self) -> &[Socket] {
                &[]
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                let _ = bus.recv_all::<S>()?;
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }

        let mut bus = Bus::open();
        bus.register(Snk { tag: "sink-A" });
        bus.register(Snk { tag: "sink-B" });
        bus.register(Src { sent: false });
        bus.run_until(Some(Duration::from_millis(300)));

        let snap = bus.fabric_snapshot();
        // No thread may list both sinks — they received in parallel, so
        // they are siblings, never a chain.
        for t in &snap.threads {
            let has_a = t.direction.iter().any(|n| n == "sink-A");
            let has_b = t.direction.iter().any(|n| n == "sink-B");
            assert!(
                !(has_a && has_b),
                "fan-out siblings must not be on one thread: {:?}",
                t.direction
            );
        }
        // Exactly two branches, one per sink, both rooted at source.
        let reaches = |sink: &str| {
            snap.threads.iter().any(|t| {
                t.direction.first().map(String::as_str) == Some("source")
                    && t.direction.last().map(String::as_str) == Some(sink)
            })
        };
        assert!(reaches("sink-A"), "a branch should reach sink-A: {:?}", snap.threads);
        assert!(reaches("sink-B"), "a branch should reach sink-B: {:?}", snap.threads);
    }

    /// HopTo a live thread routes onto it; HopTo an unminted id falls
    /// back to ordinary threading and never fabricates a ghost.
    #[test]
    fn transfer_policy_hopto_handles_known_and_unknown_targets() {
        #[derive(Clone)]
        struct A;
        #[derive(Clone)]
        struct B;

        fn run_with(target: ThreadId) -> FabricSnapshot {
            register_clone_type::<A>();
            register_clone_type::<B>();
            struct Src {
                sent: bool,
            }
            impl Strand for Src {
                fn name(&self) -> &str {
                    "S"
                }
                fn inputs(&self) -> &[Socket] {
                    &[]
                }
                fn outputs(&self) -> &[Socket] {
                    const SK: Socket = Socket::new::<A>("a");
                    std::slice::from_ref(&SK)
                }
                fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                    if !self.sent {
                        bus.publish("a", A)?;
                        self.sent = true;
                    }
                    bus.sleep(Duration::from_millis(10));
                    Ok(())
                }
            }
            struct M;
            impl Strand for M {
                fn name(&self) -> &str {
                    "M"
                }
                fn inputs(&self) -> &[Socket] {
                    const SK: Socket = Socket::new::<A>("a");
                    std::slice::from_ref(&SK)
                }
                fn outputs(&self) -> &[Socket] {
                    const SK: Socket = Socket::new::<B>("b");
                    std::slice::from_ref(&SK)
                }
                fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                    for _ in bus.recv_all::<A>()? {
                        bus.publish("b", B)?;
                    }
                    bus.sleep(Duration::from_millis(10));
                    Ok(())
                }
            }
            struct Snk;
            impl Strand for Snk {
                fn name(&self) -> &str {
                    "sink"
                }
                fn inputs(&self) -> &[Socket] {
                    const SK: Socket = Socket::new::<B>("b");
                    std::slice::from_ref(&SK)
                }
                fn outputs(&self) -> &[Socket] {
                    &[]
                }
                fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                    let _ = bus.recv_all::<B>()?;
                    bus.sleep(Duration::from_millis(10));
                    Ok(())
                }
            }
            let mut bus = Bus::open();
            bus.set_transfer("M", TransferPolicy::HopTo(target));
            bus.register(Snk);
            bus.register(M);
            bus.register(Src { sent: false });
            bus.run_until(Some(Duration::from_millis(300)));
            bus.fabric_snapshot()
        }

        // Known target: the single source mints thread 0 first, M hops
        // onto it (valid arm) — the thread reaches the sink, no ghost.
        let known = run_with(0);
        assert_eq!(known.threads.len(), 1, "only thread 0, no ghosts: {:?}", known.threads);
        assert!(
            known.threads[0].direction.iter().any(|n| n == "sink"),
            "hopped thread should reach the sink: {:?}",
            known.threads
        );

        // Unknown target: M falls back to continuing the inbound thread;
        // no thread with the bogus id is ever created.
        let unknown = run_with(999_999);
        assert!(
            unknown.thread(999_999).is_none(),
            "HopTo to an unminted id must not fabricate a ghost thread"
        );
        assert!(
            unknown.threads.iter().any(|t| t.direction.iter().any(|n| n == "sink")),
            "fallback should still carry work to the sink: {:?}",
            unknown.threads
        );
    }

    /// An intersection keeps growing as more threads cross: three
    /// sources into one joiner bind all three.
    #[test]
    fn intersection_grows_past_two_threads() {
        #[derive(Clone)]
        struct X;
        register_clone_type::<X>();
        struct Src {
            name: &'static str,
            sent: bool,
        }
        impl Strand for Src {
            fn name(&self) -> &str {
                self.name
            }
            fn inputs(&self) -> &[Socket] {
                &[]
            }
            fn outputs(&self) -> &[Socket] {
                const SK: Socket = Socket::new::<X>("x");
                std::slice::from_ref(&SK)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                if !self.sent {
                    bus.publish("x", X)?;
                    self.sent = true;
                }
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }
        struct Join;
        impl Strand for Join {
            fn name(&self) -> &str {
                "joiner"
            }
            fn inputs(&self) -> &[Socket] {
                const SK: Socket = Socket::new::<X>("x");
                std::slice::from_ref(&SK)
            }
            fn outputs(&self) -> &[Socket] {
                &[]
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                let _ = bus.recv_all::<X>()?;
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }
        let mut bus = Bus::open();
        bus.register(Join);
        bus.register(Src { name: "s1", sent: false });
        bus.register(Src { name: "s2", sent: false });
        bus.register(Src { name: "s3", sent: false });
        bus.run_until(Some(Duration::from_millis(400)));

        let snap = bus.fabric_snapshot();
        let isx = snap.intersection("joiner").expect("intersection at joiner");
        assert_eq!(isx.threads.len(), 3, "all three threads should bind: {:?}", isx.threads);
    }

    /// Consuming a reserved sys.* message must NOT sever a strand's work
    /// thread: a strand that feels the web (sys.lifecycle) and emits work
    /// still continues its inbound thread. Guards the reserved-socket
    /// causality fix.
    #[test]
    fn reserved_recv_does_not_sever_work_thread() {
        #[derive(Clone)]
        struct Work;
        #[derive(Clone)]
        struct Done;
        register_clone_type::<Work>();
        register_clone_type::<Done>();

        struct Src {
            sent: bool,
        }
        impl Strand for Src {
            fn name(&self) -> &str {
                "S"
            }
            fn inputs(&self) -> &[Socket] {
                &[]
            }
            fn outputs(&self) -> &[Socket] {
                const SK: Socket = Socket::new::<Work>("work");
                std::slice::from_ref(&SK)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                if !self.sent {
                    bus.publish("work", Work)?;
                    self.sent = true;
                }
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }
        // Subscribes to BOTH a work socket and sys.lifecycle; recvs the
        // reserved socket LAST before publishing — which, before the fix,
        // would reset current_cause to None and mint a fresh thread.
        struct Watcher;
        impl Strand for Watcher {
            fn name(&self) -> &str {
                "W"
            }
            fn inputs(&self) -> &[Socket] {
                const SKS: [Socket; 2] = [
                    Socket::new::<Work>("work"),
                    Socket::new::<LifecycleEvent>(SYS_LIFECYCLE),
                ];
                &SKS
            }
            fn outputs(&self) -> &[Socket] {
                const SK: Socket = Socket::new::<Done>("done");
                std::slice::from_ref(&SK)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                let work = bus.recv_all::<Work>()?;
                let _ = bus.recv_all::<LifecycleEvent>()?; // reserved, recv'd last
                for _ in work {
                    bus.publish("done", Done)?;
                }
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }
        struct Snk;
        impl Strand for Snk {
            fn name(&self) -> &str {
                "sink"
            }
            fn inputs(&self) -> &[Socket] {
                const SK: Socket = Socket::new::<Done>("done");
                std::slice::from_ref(&SK)
            }
            fn outputs(&self) -> &[Socket] {
                &[]
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                let _ = bus.recv_all::<Done>()?;
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }

        let mut bus = Bus::open();
        bus.register(Snk);
        bus.register(Watcher);
        bus.register(Src { sent: false });
        bus.run_until(Some(Duration::from_millis(400)));

        let snap = bus.fabric_snapshot();
        // The work thread born at S must continue unbroken through W to
        // the sink — one thread spanning all three nodes.
        assert!(
            snap.threads.iter().any(|t| {
                t.direction.first().map(String::as_str) == Some("S")
                    && t.direction.iter().any(|n| n == "sink")
            }),
            "the work thread must survive a reserved recv: {:?}",
            snap.threads
        );
    }

    /// The fabric is bounded: a flood of parentless messages mints many
    /// threads, but retention is capped (oldest evicted), so it never
    /// grows without limit.
    #[test]
    fn fabric_is_bounded_under_a_flood() {
        #[derive(Clone)]
        struct P;
        register_clone_type::<P>();
        struct Flood {
            sent: bool,
        }
        impl Strand for Flood {
            fn name(&self) -> &str {
                "flood"
            }
            fn inputs(&self) -> &[Socket] {
                &[]
            }
            fn outputs(&self) -> &[Socket] {
                const SK: Socket = Socket::new::<P>("p");
                std::slice::from_ref(&SK)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                if !self.sent {
                    for _ in 0..1200 {
                        bus.publish("p", P)?; // each parentless => its own thread
                    }
                    self.sent = true;
                    return Err(StrandError::Detach);
                }
                Ok(())
            }
        }
        let mut bus = Bus::open();
        bus.register(Flood { sent: false });
        bus.run_until(Some(Duration::from_millis(300)));

        // 1200 parentless publishes mint 1200 threads, but the fabric
        // caps retention at 1024 (FABRIC_THREAD_CAP), evicting oldest.
        let snap = bus.fabric_snapshot();
        assert!(
            snap.threads.len() <= 1024,
            "fabric must stay bounded, got {} threads",
            snap.threads.len()
        );
    }

    // ----- 3D spatial awareness (v1.3) -----------------------------------

    /// Euclidean distance is exact on a 3-4-5 triple (f64, no flake).
    #[test]
    fn position_distance_is_euclidean() {
        let a = Position::new(0.0, 0.0, 0.0);
        let b = Position::new(3.0, 4.0, 0.0);
        assert_eq!(a.distance(b), 5.0);
        assert_eq!(a.distance(a), 0.0);
    }

    /// An unplaced node (either end) feels a vibration at full strength.
    #[test]
    fn attenuation_none_is_full() {
        let p = Position::new(1.0, 2.0, 3.0);
        assert_eq!(attenuation(None, Some(p)), 1.0);
        assert_eq!(attenuation(Some(p), None), 1.0);
        assert_eq!(attenuation(None, None), 1.0);
    }

    /// Attenuation is 1.0 at d=0, strictly decreasing, always in (0,1];
    /// the radius variant cuts to 0 strictly beyond the radius.
    #[test]
    fn attenuation_decays_with_distance() {
        let o = Position::ORIGIN;
        let near = Position::new(1.0, 0.0, 0.0); // d=1
        let far = Position::new(9.0, 0.0, 0.0); // d=9
        assert_eq!(attenuation(Some(o), Some(o)), 1.0);
        let a_near = attenuation(Some(o), Some(near));
        let a_far = attenuation(Some(o), Some(far));
        assert!(a_near > a_far, "near felt more strongly than far");
        assert!(a_far > 0.0 && a_near <= 1.0);
        // radius cutoff: felt within, nothing beyond.
        assert!(attenuation_within(Some(o), Some(near), 5.0) > 0.0);
        assert_eq!(attenuation_within(Some(o), Some(far), 5.0), 0.0);
        // boundary is inclusive: at d == radius the vibration is still felt.
        let edge = Position::new(5.0, 0.0, 0.0); // d == radius == 5
        assert_eq!(attenuation_within(Some(o), Some(edge), 5.0), 1.0 / 6.0);
    }

    /// Non-finite coordinates never produce NaN: attenuation reads them as
    /// unreachable (0.0), and the radius cutoff isn't defeated.
    #[test]
    fn attenuation_is_nan_safe() {
        let o = Position::ORIGIN;
        let bad = Position::new(f64::NAN, 0.0, 0.0);
        let inf = Position::new(f64::INFINITY, 0.0, 0.0);
        assert_eq!(attenuation(Some(o), Some(bad)), 0.0);
        assert_eq!(attenuation(Some(o), Some(inf)), 0.0);
        assert_eq!(attenuation_within(Some(o), Some(bad), 5.0), 0.0);
        assert_eq!(attenuation_within(Some(o), Some(inf), 5.0), 0.0);
    }

    /// `place` ignores a non-finite position — the node stays unplaced
    /// rather than poisoning the fabric with NaN.
    #[test]
    fn place_ignores_non_finite() {
        let mut bus = Bus::open();
        bus.place("ok", Position::new(1.0, 2.0, 3.0));
        bus.place("bad", Position::new(f64::NAN, 0.0, 0.0));
        let snap = bus.fabric_snapshot();
        assert_eq!(snap.position("ok"), Some(Position::new(1.0, 2.0, 3.0)));
        assert_eq!(snap.position("bad"), None, "a non-finite placement is ignored");
    }

    /// The fabric records a placed position; `place` overrides an earlier
    /// placement (operator wins); an unplaced node is absent.
    #[test]
    fn fabric_records_and_overrides_position() {
        let mut bus = Bus::open();
        bus.place("a", Position::new(1.0, 2.0, 3.0));
        bus.place("b", Position::new(3.0, 4.0, 0.0));
        bus.place("a", Position::new(9.0, 9.0, 9.0)); // override
        let snap = bus.fabric_snapshot();
        assert_eq!(snap.position("a"), Some(Position::new(9.0, 9.0, 9.0)));
        assert_eq!(snap.position("b"), Some(Position::new(3.0, 4.0, 0.0)));
        assert_eq!(snap.position("unplaced"), None);
    }

    /// Snapshot distance between two placed nodes is Euclidean; unplaced
    /// yields None.
    #[test]
    fn snapshot_distance_between_nodes() {
        let mut bus = Bus::open();
        bus.place("a", Position::new(0.0, 0.0, 0.0));
        bus.place("b", Position::new(3.0, 4.0, 0.0));
        let snap = bus.fabric_snapshot();
        assert_eq!(snap.distance("a", "b"), Some(5.0));
        assert_eq!(snap.distance("a", "ghost"), None);
    }

    // ----- Fabric adjacency (heat-kernel topology) -----------------------

    fn edge(from: &str, to: &str, conductance: f64) -> FabricEdge {
        FabricEdge { from: from.into(), to: to.into(), conductance }
    }

    fn et(from: &str, to: &str, n: u64) -> (NodeId, NodeId, u64) {
        (from.into(), to.into(), n)
    }

    /// Build a snapshot directly from a set of isolated node names (as
    /// one-node threads) and a raw per-edge delivery table — the shape
    /// `Fabric::snapshot` produces.
    fn snap_with(isolated: &[&str], traffic: Vec<(NodeId, NodeId, u64)>) -> FabricSnapshot {
        let threads = isolated
            .iter()
            .enumerate()
            .map(|(i, n)| Thread { id: i as ThreadId, direction: vec![(*n).into()] })
            .collect();
        FabricSnapshot { threads, edge_traffic: traffic, ..Default::default() }
    }

    /// Edges and their conductances come from the per-edge delivery counts;
    /// every edge endpoint is a vertex.
    #[test]
    fn adjacency_from_edge_traffic() {
        let snap = snap_with(&[], vec![et("S", "A", 1), et("A", "B", 1)]);
        let adj = snap.adjacency();
        assert_eq!(adj.nodes, vec!["A".to_string(), "B".into(), "S".into()]);
        assert_eq!(adj.edges, vec![edge("A", "B", 1.0), edge("S", "A", 1.0)]);
    }

    /// Conductance is the real delivery count: an edge crossed N times
    /// weighs N. Worn desire-paths weigh more.
    #[test]
    fn adjacency_conductance_is_the_delivery_count() {
        let snap = snap_with(&[], vec![et("A", "B", 3), et("B", "C", 1)]);
        let adj = snap.adjacency();
        assert_eq!(adj.edges, vec![edge("A", "B", 3.0), edge("B", "C", 1.0)]);
    }

    /// Edges are directed: A→B and B→A are distinct (a generator
    /// symmetrizes; the fabric reports faithfully).
    #[test]
    fn adjacency_keeps_direction() {
        let snap = snap_with(&[], vec![et("A", "B", 1), et("B", "A", 1)]);
        let adj = snap.adjacency();
        assert_eq!(adj.edges, vec![edge("A", "B", 1.0), edge("B", "A", 1.0)]);
    }

    /// A one-node thread (a lineage that never moved) is an isolated
    /// vertex: present in `nodes`, absent from `edges`.
    #[test]
    fn adjacency_includes_isolated_node() {
        let snap = snap_with(&["lonely"], vec![et("A", "B", 1)]);
        let adj = snap.adjacency();
        assert_eq!(adj.nodes, vec!["A".to_string(), "B".into(), "lonely".into()]);
        assert_eq!(adj.edges, vec![edge("A", "B", 1.0)]);
    }

    /// Empty fabric → empty adjacency.
    #[test]
    fn adjacency_of_empty_fabric_is_empty() {
        let adj = FabricSnapshot::default().adjacency();
        assert!(adj.nodes.is_empty());
        assert!(adj.edges.is_empty());
    }

    /// `nodes` and `edges` are deterministically sorted regardless of input
    /// order.
    #[test]
    fn adjacency_is_sorted_and_deterministic() {
        let snap = snap_with(&[], vec![et("Z", "Y", 1), et("M", "A", 1)]);
        let adj = snap.adjacency();
        assert_eq!(adj.nodes, vec!["A".to_string(), "M".into(), "Y".into(), "Z".into()]);
        assert_eq!(adj.edges, vec![edge("M", "A", 1.0), edge("Z", "Y", 1.0)]);
    }

    /// End-to-end: a real two-hop flow through the bus produces an
    /// adjacency that mirrors the desire-path the message actually took.
    #[test]
    fn adjacency_reflects_real_flow() {
        #[derive(Clone)]
        struct P;
        #[derive(Clone)]
        struct Q;
        register_clone_type::<P>();
        register_clone_type::<Q>();

        struct Src {
            sent: bool,
        }
        impl Strand for Src {
            fn name(&self) -> &str {
                "source"
            }
            fn inputs(&self) -> &[Socket] {
                &[]
            }
            fn outputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<P>("p");
                std::slice::from_ref(&S)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                if !self.sent {
                    bus.publish("p", P)?;
                    self.sent = true;
                }
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }
        struct Relay;
        impl Strand for Relay {
            fn name(&self) -> &str {
                "relay"
            }
            fn inputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<P>("p");
                std::slice::from_ref(&S)
            }
            fn outputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<Q>("q");
                std::slice::from_ref(&S)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                for _ in bus.recv_all::<P>()? {
                    bus.publish("q", Q)?;
                }
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }
        struct Snk;
        impl Strand for Snk {
            fn name(&self) -> &str {
                "sink"
            }
            fn inputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<Q>("q");
                std::slice::from_ref(&S)
            }
            fn outputs(&self) -> &[Socket] {
                &[]
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                let _ = bus.recv_all::<Q>()?;
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }

        let mut bus = Bus::open();
        bus.register(Snk);
        bus.register(Relay);
        bus.register(Src { sent: false });
        bus.run_until(Some(Duration::from_millis(400)));

        let adj = bus.fabric_snapshot().adjacency();
        // The lineage flowed source -> relay -> sink, one message each hop,
        // so each edge has conductance exactly 1.0 (one real delivery).
        let w = |from: &str, to: &str| {
            adj.edges
                .iter()
                .find(|e| e.from == from && e.to == to)
                .map(|e| e.conductance)
        };
        assert_eq!(w("source", "relay"), Some(1.0), "edges: {:?}", adj.edges);
        assert_eq!(w("relay", "sink"), Some(1.0), "edges: {:?}", adj.edges);
    }

    /// Conductance is the real per-DELIVERY count, so a Stay fan-out
    /// increments each downstream edge once and leaves the upstream edge at
    /// 1 — it does NOT inflate the upstream edge by the branch count (the
    /// thing the older thread-segment derivation got wrong). One message
    /// into a relay with three subscribers: `source->relay` = 1, each
    /// `relay->sink` = 1.
    #[test]
    fn adjacency_fan_out_does_not_inflate_upstream() {
        #[derive(Clone)]
        struct P;
        #[derive(Clone)]
        struct Q;
        register_clone_type::<P>();
        register_clone_type::<Q>();

        struct Src {
            sent: bool,
        }
        impl Strand for Src {
            fn name(&self) -> &str {
                "source"
            }
            fn inputs(&self) -> &[Socket] {
                &[]
            }
            fn outputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<P>("p");
                std::slice::from_ref(&S)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                if !self.sent {
                    bus.publish("p", P)?;
                    self.sent = true;
                }
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }
        struct Relay;
        impl Strand for Relay {
            fn name(&self) -> &str {
                "relay"
            }
            fn inputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<P>("p");
                std::slice::from_ref(&S)
            }
            fn outputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<Q>("q");
                std::slice::from_ref(&S)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                for _ in bus.recv_all::<P>()? {
                    bus.publish("q", Q)?;
                }
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }
        struct Snk {
            tag: &'static str,
        }
        impl Strand for Snk {
            fn name(&self) -> &str {
                self.tag
            }
            fn inputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<Q>("q");
                std::slice::from_ref(&S)
            }
            fn outputs(&self) -> &[Socket] {
                &[]
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                let _ = bus.recv_all::<Q>()?;
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }

        let mut bus = Bus::open();
        bus.register(Snk { tag: "sink-a" });
        bus.register(Snk { tag: "sink-b" });
        bus.register(Snk { tag: "sink-c" });
        bus.register(Relay);
        bus.register(Src { sent: false });
        bus.run_until(Some(Duration::from_millis(400)));

        let adj = bus.fabric_snapshot().adjacency();
        let w = |from: &str, to: &str| {
            adj.edges
                .iter()
                .find(|e| e.from == from && e.to == to)
                .map(|e| e.conductance)
        };
        // Upstream edge counted ONCE (one real delivery into the relay),
        // NOT inflated to the branch count.
        assert_eq!(w("source", "relay"), Some(1.0), "edges: {:?}", adj.edges);
        // Each downstream branch counted once (one delivery each).
        assert_eq!(w("relay", "sink-a"), Some(1.0), "edges: {:?}", adj.edges);
        assert_eq!(w("relay", "sink-b"), Some(1.0), "edges: {:?}", adj.edges);
        assert_eq!(w("relay", "sink-c"), Some(1.0), "edges: {:?}", adj.edges);
    }

    /// Counting the PHYSICAL hop (not the lineage tag) keeps conductance
    /// correct under MergeAll: two sources fuse at `N`, so `N->sink` carries
    /// two real deliveries and weighs 2 — even though MergeAll fuses both
    /// lineages onto one thread whose tail would otherwise eat the second
    /// delivery. (The lineage-based counter undercounted this trunk to 1.)
    #[test]
    fn adjacency_conductance_correct_under_mergeall() {
        let snap = run_transfer_flow(Some(TransferPolicy::MergeAll));
        let adj = snap.adjacency();
        let w = |from: &str, to: &str| {
            adj.edges
                .iter()
                .find(|e| e.from == from && e.to == to)
                .map(|e| e.conductance)
        };
        assert_eq!(w("s1", "N"), Some(1.0), "edges: {:?}", adj.edges);
        assert_eq!(w("s2", "N"), Some(1.0), "edges: {:?}", adj.edges);
        assert_eq!(
            w("N", "sink"),
            Some(2.0),
            "the fused trunk must count BOTH physical deliveries, edges: {:?}",
            adj.edges
        );
    }

    /// Under HopTo (which re-parents the lineage tag onto another thread),
    /// conductance still reflects only PHYSICAL hops: no phantom edges from
    /// the re-parented lineage, and the real deliveries are counted. (The
    /// lineage-based counter fabricated edges no message crossed and dropped
    /// the real ones.)
    #[test]
    fn adjacency_conductance_correct_under_hopto() {
        let snap = run_transfer_flow(Some(TransferPolicy::HopTo(0)));
        let adj = snap.adjacency();
        let w = |from: &str, to: &str| {
            adj.edges
                .iter()
                .find(|e| e.from == from && e.to == to)
                .map(|e| e.conductance)
        };
        assert_eq!(w("s1", "N"), Some(1.0), "edges: {:?}", adj.edges);
        assert_eq!(w("s2", "N"), Some(1.0), "edges: {:?}", adj.edges);
        assert_eq!(w("N", "sink"), Some(2.0), "edges: {:?}", adj.edges);
        // Every edge is one of the three real physical hops — no phantoms.
        for e in &adj.edges {
            assert!(
                matches!(
                    (e.from.as_str(), e.to.as_str()),
                    ("s1", "N") | ("s2", "N") | ("N", "sink")
                ),
                "unexpected (phantom) edge {:?} in {:?}",
                e,
                adj.edges
            );
        }
    }

    // ----- Strand-name collision detection (H4) ---------------------------

    /// Registering two live strands with the same name must
    /// panic, not silently overwrite the first slot. Silent
    /// overwrite leaked the first slot's `done_rx` and made its
    /// `WorkerExit` unprocessable by the kernel.
    #[test]
    #[should_panic(expected = "strand name collision")]
    fn live_strand_name_collision_panics() {
        struct Dup1;
        impl Strand for Dup1 {
            fn name(&self) -> &str { "dup-name" }
            fn inputs(&self) -> &[Socket] { &[] }
            fn outputs(&self) -> &[Socket] { &[] }
            fn run(&mut self, _bus: &mut BusHandle) -> Result<(), StrandError> { Ok(()) }
        }
        struct Dup2;
        impl Strand for Dup2 {
            fn name(&self) -> &str { "dup-name" }
            fn inputs(&self) -> &[Socket] { &[] }
            fn outputs(&self) -> &[Socket] { &[] }
            fn run(&mut self, _bus: &mut BusHandle) -> Result<(), StrandError> { Ok(()) }
        }
        let mut bus = Bus::open();
        bus.register(Dup1);
        bus.register(Dup2);
    }

    /// Registering two spec-based strands with the same name must
    /// panic at registration time, not at run time.
    #[test]
    #[should_panic(expected = "strand name collision")]
    fn spec_strand_name_collision_panics() {
        let mut bus = Bus::open();
        bus.register_spec(StrandSpec::new("dup-spec", || Box::new(TestSource::new(vec!["a".into()]))));
        bus.register_spec(StrandSpec::new("dup-spec", || Box::new(TestSource::new(vec!["b".into()]))));
    }

    // ----- Explicit spider registration (H3) -----------------------------

    /// Registering a spider and then a second spider must panic.
    /// v1.2 supports exactly one orchestrator.
    #[test]
    #[should_panic(expected = "register_spider called twice")]
    fn double_spider_registration_panics() {
        let mut bus = Bus::open();
        bus.register_spider(StrandSpec::new("spider-1", || Box::new(TestSource::new(vec!["x".into()]))));
        bus.register_spider(StrandSpec::new("spider-2", || Box::new(TestSource::new(vec!["y".into()]))));
    }
}
