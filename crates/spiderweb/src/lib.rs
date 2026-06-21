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
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

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
        FabricSnapshot {
            threads,
            intersections,
            positions,
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
    let mut guard = CLONE_REGISTRY.lock().unwrap();
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
}

#[derive(Debug)]
enum WorkerExit {
    Detach,
    Crashed(String),
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
        *slot.join.lock().unwrap() = Some(handle);

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
        *slot.join.lock().unwrap() = Some(handle);

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
            let mut subs = self.subscribers.lock().unwrap();
            let pending = self.pending.lock().unwrap();

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
        let env: Envelope = Box::new(evt);
        self.deliver_internal(SYS_LIFECYCLE, env, "kernel");
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
            self.fabric.lock().unwrap().outbound(from_name, tag)
        };
        // A fan-out from a default (Stay) node branches the lineage:
        // each extra subscriber gets its own forked sibling thread, so
        // every thread stays a true linear path instead of one
        // fabricated chain through parallel nodes.
        let forking = !reserved && out_tag.is_some() && {
            // Cheap pre-check; the real per-branch forks happen below
            // once we know how many distinct subscribers there are.
            self.fabric.lock().unwrap().is_stay(from_name)
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
            let subs = self.subscribers.lock().unwrap();
            let mut pending = self.pending.lock().unwrap();

            let buf = pending.entry(type_id).or_default();
            let was_empty = buf.is_empty();
            if buf.len() >= Self::PENDING_BUFFER_CAP {
                buf.remove(0);
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
            let mut fabric = self.fabric.lock().unwrap();
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
        for (sub, sub_tag) in snapshot.iter().zip(sub_tags) {
            let cloned = registry().as_ref().unwrap().clone_env(&env);
            if let Err(returned) = sub.try_send(socket, sub_tag, cloned) {
                eprintln!(
                    "[{:>5}ms] [kernel] ⚠️  {} disconnected mid-deliver",
                    self.boot.elapsed().as_millis(),
                    sub.name
                );
                let _ = returned;
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
            let mut fabric = self.fabric.lock().unwrap();
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
        let subs = self.subscribers.lock().unwrap();
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
        let mut inbox: Vec<Carrier> = Vec::new();
        for (sock, tag, env) in inbox_rx.try_iter() {
            inbox.push((sock, tag, env));
        }

        // Record arrivals into the fabric before the strand runs, so a
        // node lands on a thread's path the instant a message reaches
        // it (capturing replayed messages too).
        kernel.record_arrivals(&name, &inbox);

        let mut handle = BusHandle::new(name.clone(), inbox, kernel.clone());
        let result = strand.run(&mut handle);

        let outbox = handle.take_outbox();
        for (socket, tag, env) in outbox {
            kernel.deliver(socket, tag, env, &name);
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
        self.kernel.slots.lock().unwrap().insert(name, slot);
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
        let mut spider_name = self.kernel.spider_name.lock().unwrap();
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
        let slots = self.kernel.slots.lock().unwrap();
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
        self.kernel.fabric.lock().unwrap().snapshot()
    }

    /// Set the transfer policy at an intersection `node`. May be called
    /// before the intersection emerges (it is remembered and applied
    /// when it does). The default for every node is
    /// [`TransferPolicy::Stay`] — a no-op — so the fabric stays purely
    /// observational unless explicitly governed here or via
    /// [`ControlMsg::SetTransfer`].
    pub fn set_transfer(&mut self, node: &str, policy: TransferPolicy) {
        self.kernel.fabric.lock().unwrap().set_transfer(node, policy);
    }

    /// Place `node` at a position in the web's 3D space. Pure metadata —
    /// it changes no routing or timing (every node is in the same RAM);
    /// it's read via [`Bus::fabric_snapshot`] for visualization,
    /// organization, and spider policy (e.g. distance-attenuated
    /// vibrations). Overrides any advertised position (operator wins).
    pub fn place(&mut self, node: &str, pos: Position) {
        self.kernel.fabric.lock().unwrap().set_position(node, pos);
    }

    pub fn run(mut self) {
        self.run_until(None)
    }

    /// Run the bus until `max_duration` elapses (or forever if `None`),
    /// or until all strands finish, Ctrl-C, or `ControlMsg::Shutdown`.
    ///
    /// Takes `&mut self` (not `self`) so the fabric can be inspected
    /// with [`Bus::fabric_snapshot`] after the run returns — the binding
    /// must therefore be `mut`. (v1.3 changed this from by-value `self`.)
    pub fn run_until(&mut self, max_duration: Option<Duration>) {
        let kernel = self.kernel.clone();
        let mut specs = std::mem::take(&mut self.specs);

        for spec in specs.drain(..) {
            let name = spec.name;
            let factory = spec.factory;
            let slot = kernel.install_slot(name, Some(factory));
            kernel.slots.lock().unwrap().insert(name.to_string(), slot);
        }

        // Pending actions: when a strand crashes, we mark it for
        // restart-by-default. The spider can override with a
        // Quarantine via `sys.control`. The kernel's "default" is
        // to restart indefinitely, but the spider sets the actual
        // policy.
        let mut pending_restarts: HashMap<String, u32> = HashMap::new();
        let start = Instant::now();
        let mut done_count: usize = 0;
        let total_slots: usize = kernel.slots.lock().unwrap().len();

        // Read the control_rx out of the kernel so we can poll it
        // in the main loop.
        let control_rx = kernel.control_rx.lock().unwrap().take().expect(
            "kernel control channel already taken; this is a bug in the kernel",
        );

        loop {
            if let Some(d) = max_duration {
                if start.elapsed() >= d {
                    println!(
                        "[{:>5}ms] [kernel] ⏹  max duration reached, shutting down",
                        kernel.boot.elapsed().as_millis()
                    );
                    break;
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
                                    kernel.slots.lock().unwrap().insert(name.clone(), new_slot);
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
                            kernel.publish_lifecycle(LifecycleEvent::Quarantined {
                                name: name.clone(),
                                reason: reason.clone(),
                            });
                            kernel.slots.lock().unwrap().remove(name.as_str());
                            done_count += 1;
                        }
                        ControlMsg::Shutdown => {
                            println!(
                                "[{:>5}ms] [kernel] ⏹  shutdown requested via sys.control",
                                kernel.boot.elapsed().as_millis()
                            );
                            return;
                        }
                        ControlMsg::SetTransfer { node, policy } => {
                            kernel.fabric.lock().unwrap().set_transfer(node, *policy);
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
            let slots = kernel.slots.lock().unwrap().clone();

            for (name, slot) in slots.iter() {
                let done_rx_opt = slot.done_rx.lock().unwrap().take();
                let Some(done_rx) = done_rx_opt else { continue };
                match done_rx.try_recv() {
                    Ok(WorkerExit::Detach) => {
                        kernel.slots.lock().unwrap().remove(name);
                        done_count += 1;
                    }
                    Ok(WorkerExit::Crashed(_reason)) => {
                        // The kernel is plumbing: it never decides restart
                        // policy. The worker already published the
                        // `Crashed` lifecycle event; here we only mark the
                        // slot dead and wait for the orchestrator's
                        // `ControlMsg` (Restart/Quarantine) on
                        // `sys.control`. The slot stays in the map so its
                        // factory is available for the spider's Restart.
                        //
                        // Restart policy lives ENTIRELY in the spider.
                        // With no spider registered, a crashed strand
                        // simply stays dead — the kernel does NOT
                        // auto-restart it. (v1.2 had a 3-retry-then-
                        // quarantine fallback baked into the kernel;
                        // removed in v1.3 so the kernel stays a dumb
                        // primitive and all crash policy is the spider's.)
                        //
                        // Mechanically: `done_rx` was taken above and is
                        // not put back, so this slot is parked — skipped
                        // on every later tick — until the spider's Restart
                        // replaces it or Quarantine removes it. Nothing to
                        // do here; the kernel just waits.
                    }
                    Err(_) => {
                        *slot.done_rx.lock().unwrap() = Some(done_rx);
                    }
                }
            }

            if total_slots > 0 && done_count >= total_slots {
                println!(
                    "[{:>5}ms] [kernel] ⏹  all strands done, shutting down",
                    kernel.boot.elapsed().as_millis()
                );
                break;
            }

            thread::sleep(Duration::from_millis(50));
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
                *self.last.lock().unwrap() = msg;
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
                *self.last.lock().unwrap() = Some(msg);
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
        assert_eq!(*last_a.lock().unwrap(), "third");
        assert_eq!(*last_b.lock().unwrap(), "third");
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
        let last_str = last.lock().unwrap().clone();
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
                *self.last.lock().unwrap() = msg;
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
        let a = last_a.lock().unwrap().clone();
        let b = last_b.lock().unwrap().clone();
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
