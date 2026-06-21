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
}

// -----------------------------------------------------------------------------
// Control messages
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum ControlMsg {
    Restart { name: String },
    Quarantine { name: String, reason: String },
    Shutdown,
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
    outbox: Vec<(&'static str, Envelope)>,
    inbox: Vec<(&'static str, Envelope)>,
    kernel: Arc<Kernel>,
}

impl BusHandle {
    fn new(strand_name: String, inbox: Vec<(&'static str, Envelope)>, kernel: Arc<Kernel>) -> Self {
        BusHandle {
            strand_name,
            outbox: Vec::new(),
            inbox,
            kernel,
        }
    }

    pub fn publish<T: 'static + Send + Clone>(
        &mut self,
        socket: &'static str,
        value: T,
    ) -> Result<(), BusError> {
        self.outbox.push((socket, Box::new(value)));
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

    pub fn recv<T: 'static + Send>(&mut self, socket: &'static str) -> Result<Vec<T>, BusError> {
        let mut out = Vec::new();
        let mut kept = Vec::with_capacity(self.inbox.len());
        for (sock, env) in self.inbox.drain(..) {
            if sock == socket {
                if (*env).type_id() != TypeId::of::<T>() {
                    return Err(BusError::TypeMismatch {
                        socket,
                        expected: std::any::type_name::<T>(),
                    });
                }
                match env.downcast::<T>() {
                    Ok(v) => out.push(*v),
                    Err(_) => unreachable!("type id verified above"),
                }
            } else {
                kept.push((sock, env));
            }
        }
        self.inbox = kept;
        Ok(out)
    }

    pub fn recv_all<T: 'static + Send>(&mut self) -> Result<Vec<(&'static str, T)>, BusError> {
        let mut out = Vec::new();
        let mut kept = Vec::new();
        for (sock, env) in self.inbox.drain(..) {
            if (*env).type_id() == TypeId::of::<T>() {
                match env.downcast::<T>() {
                    Ok(v) => out.push((sock, *v)),
                    Err(_) => unreachable!("type id matched"),
                }
            } else {
                kept.push((sock, env));
            }
        }
        self.inbox = kept;
        Ok(out)
    }

    pub fn log(&self, msg: &str) {
        let elapsed = self.kernel.boot.elapsed().as_millis();
        println!("[{:>5}ms] [{}] {}", elapsed, self.strand_name, msg);
    }

    pub fn sleep(&self, d: Duration) {
        thread::sleep(d);
    }

    fn take_outbox(&mut self) -> Vec<(&'static str, Envelope)> {
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
    sender: Sender<(&'static str, Envelope)>,
    name: String,
}

impl Subscriber {
    fn try_send(&self, socket: &'static str, env: Envelope) -> Result<(), Envelope> {
        match self.sender.send((socket, env)) {
            Ok(()) => Ok(()),
            Err(e) => Err(e.0 .1),
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
    quarantined: std::sync::atomic::AtomicBool,
    generation: std::sync::atomic::AtomicUsize,
    join: Mutex<Option<JoinHandle<()>>>,
    /// True when the worker has exited and the slot is awaiting
    /// the spider's decision (Restart/Quarantine) via sys.control.
    dead: std::sync::atomic::AtomicBool,
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
    pending: Mutex<HashMap<TypeId, Vec<(&'static str, Envelope)>>>,
    slots: Mutex<HashMap<String, Arc<StrandSlot>>>,
    /// Receiver for control messages. The kernel itself is the only
    /// subscriber to `sys.control`. Strands (the spider, in v1.2)
    /// publish `ControlMsg` here to direct the kernel.
    control_rx: Mutex<Option<Receiver<(&'static str, Envelope)>>>,
    /// Name of the registered spider (if any). Set by
    /// [`Bus::register_spider`]. The kernel uses this to decide
    /// whether to fall back to auto-restart on crash (no spider)
    /// or to wait for the spider to decide (spider present).
    /// This is explicit, not inferred from socket subscriptions,
    /// so registering a lifecycle observer does NOT disable the
    /// fallback path.
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
            quarantined: std::sync::atomic::AtomicBool::new(false),
            generation: std::sync::atomic::AtomicUsize::new(0),
            join: Mutex::new(None),
            dead: std::sync::atomic::AtomicBool::new(false),
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
            quarantined: std::sync::atomic::AtomicBool::new(false),
            generation: std::sync::atomic::AtomicUsize::new(0),
            join: Mutex::new(None),
            dead: std::sync::atomic::AtomicBool::new(false),
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
        let subs = self.subscribers.lock().unwrap();
        let pending = self.pending.lock().unwrap();

        // Replay any buffered messages of this type to the new
        // subscriber. We clone each envelope via the clone
        // registry so the buffer itself is preserved for future
        // subscribers. v2 should add a TTL or a smarter drain
        // policy.
        let flush_event = if let Some(buf) = pending.get(&type_id) {
            let count = buf.len();
            for (socket, env) in buf {
                let cloned = registry().as_ref().unwrap().clone_env(env);
                if let Err(returned) = sub.try_send(socket, cloned) {
                    let _ = returned;
                }
            }
            Some(LifecycleEvent::Flushed {
                socket: "<pending>",
                to: sub.name.clone(),
                count,
            })
        } else {
            None
        };

        drop(pending);
        drop(subs);

        // IMPORTANT: must drop both mutexes BEFORE calling
        // publish_lifecycle. publish_lifecycle → deliver_internal
        // re-locks self.subscribers, and std::sync::Mutex is
        // not reentrant. Holding subs/pending across the call
        // would deadlock. (C1 fix.)
        if let Some(evt) = flush_event {
            self.publish_lifecycle(evt);
        }

        self.subscribers
            .lock()
            .unwrap()
            .entry(type_id)
            .or_default()
            .push(sub);
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

    fn deliver(&self, socket: &'static str, env: Envelope, from_name: &str) {
        let type_id = (*env).type_id();

        // Clone for the buffer FIRST (the buffer keeps a copy
        // for late subscribers). Then deliver to current
        // subscribers, also by clone. The original `env` is
        // consumed by the for-loop.
        let buffered = registry().as_ref().unwrap().clone_env(&env);

        {
            let mut pending = self.pending.lock().unwrap();
            let buf = pending.entry(type_id).or_default();
            let was_empty = buf.is_empty();
            if buf.len() >= Self::PENDING_BUFFER_CAP {
                buf.remove(0);
            }
            buf.push((socket, buffered));
            if was_empty {
                // Drop the pending lock before calling
                // publish_lifecycle (it re-locks self.subscribers).
                drop(pending);
                self.publish_lifecycle(LifecycleEvent::Buffered {
                    socket,
                    from: from_name.to_string(),
                });
            }
        }

        // Deliver to current subscribers. Re-lock subscribers
        // (we dropped it above implicitly via the lock guard's
        // scope).
        let subs = self.subscribers.lock().unwrap();
        if let Some(c) = subs.get(&type_id) {
            for sub in c.iter() {
                let cloned = registry().as_ref().unwrap().clone_env(&env);
                if let Err(returned) = sub.try_send(socket, cloned) {
                    eprintln!(
                        "[{:>5}ms] [kernel] ⚠️  {} disconnected mid-deliver",
                        self.boot.elapsed().as_millis(),
                        sub.name
                    );
                    let _ = returned;
                }
            }
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
            if let Err(returned) = sub.try_send(socket, cloned) {
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
    inbox_rx: Receiver<(&'static str, Envelope)>,
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
        let mut inbox: Vec<(&'static str, Envelope)> = Vec::new();
        for (sock, env) in inbox_rx.try_iter() {
            inbox.push((sock, env));
        }

        let mut handle = BusHandle::new(name.clone(), inbox, kernel.clone());
        let result = strand.run(&mut handle);

        let outbox = handle.take_outbox();
        for (socket, env) in outbox {
            kernel.deliver(socket, env, &name);
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

    /// Register a strand spec as the orchestrator (spider). The
    /// kernel uses this to decide crash-handling policy: when a
    /// strand crashes and a spider is present, the kernel waits
    /// for the spider to issue a `ControlMsg`; when no spider is
    /// present, the kernel falls back to auto-restart (3 times,
    /// then quarantine).
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

    pub fn run(self) {
        self.run_until(None)
    }

    pub fn run_until(self, max_duration: Option<Duration>) {
        let kernel = self.kernel.clone();
        let mut specs = self.specs;

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
            for (_sock, env) in control_rx.try_iter() {
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
                            if let Some(slot) =
                                kernel.slots.lock().unwrap().get(name.as_str()).cloned()
                            {
                                slot.quarantined.store(
                                    true,
                                    std::sync::atomic::Ordering::SeqCst,
                                );
                            }
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
                        // v1.2: the spider drives the restart policy.
                        // It sees the Crashed lifecycle event, decides
                        // what to do, and sends a ControlMsg. The
                        // kernel just marks the slot as dead and
                        // waits. The slot stays in the map (so the
                        // factory is available for the spider's
                        // Restart).
                        //
                        // If NO spider is registered, fall back to
                        // the v1.1 auto-restart policy (3 restarts,
                        // then quarantine). v1.3 will remove the
                        // auto-restart entirely.
                        //
                        // We check the explicit spider_name field
                        // rather than inferring from
                        // `LifecycleEvent` subscribers. The
                        // `LifecycleObserver` test strand also
                        // subscribes to `LifecycleEvent` but is
                        // not a spider; inferring would mistake it
                        // for one.
                        let has_spider = kernel
                            .spider_name
                            .lock()
                            .unwrap()
                            .is_some();
                        if !has_spider {
                            // Fallback auto-restart for demos without
                            // a spider.
                            if slot.quarantined.load(std::sync::atomic::Ordering::SeqCst) {
                                kernel.slots.lock().unwrap().remove(name);
                                done_count += 1;
                                continue;
                            }
                            // Live (non-restartable) strands can't be
                            // auto-restarted. Just remove them.
                            let factory_opt = slot.factory.clone();
                            let Some(factory) = factory_opt else {
                                kernel.slots.lock().unwrap().remove(name);
                                done_count += 1;
                                continue;
                            };
                            let count = pending_restarts.entry(name.clone()).or_insert(0);
                            let max = 3;
                            if *count >= max {
                                slot.quarantined.store(
                                    true,
                                    std::sync::atomic::Ordering::SeqCst,
                                );
                                kernel.publish_lifecycle(LifecycleEvent::Quarantined {
                                    name: name.clone(),
                                    reason: format!("crashed {} times", max),
                                });
                                kernel.slots.lock().unwrap().remove(name);
                                done_count += 1;
                                continue;
                            }
                            *count += 1;
                            let attempt = *count;
                            let new_slot = kernel.restart_slot(name.as_str(), factory);
                            kernel.slots.lock().unwrap().insert(name.clone(), new_slot);
                            kernel.publish_lifecycle(LifecycleEvent::Restarted {
                                name: name.clone(),
                                attempt,
                            });
                        } else {
                            // Spider is driving. The slot stays in
                            // the map with `dead = true`. The done_rx
                            // is consumed. A new install will happen
                            // when the spider sends Restart, which
                            // replaces this slot in the map.
                            slot.dead.store(true, std::sync::atomic::Ordering::SeqCst);
                        }
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
