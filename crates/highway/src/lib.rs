//! `highway` — Layer 2 of the spiderweb bus: the parallel layer.
//!
//! The ground level ([`spiderweb`]) is local streets: strands tick on
//! their own threads and messages fan out by type. That layer is slow
//! and flexible. This crate is the **highway above it** — fast,
//! directional, and parallel.
//!
//! When a ground-level message hits an **on-ramp**, it is *promoted*
//! out of local traffic and onto the highway, where it is processed
//! across many **lanes** simultaneously. Finished work *demotes* back
//! to the ground through an **off-ramp**, re-published as a normal bus
//! message. The on-ramp and off-ramp are the elevation changes between
//! the two speed tiers; the lanes are the highway itself.
//!
//! The point, exactly as on a real highway, is that not every car needs
//! a direct road to every destination. The highway aggregates traffic,
//! moves it fast across multiple lanes, and distributes it at exits —
//! so a batch of work finishes in `ceil(N / lanes)` rounds instead of
//! `N` serial steps.
//!
//! ## Two pieces
//!
//! - [`Highway`] — the parallel layer on its own. A pool of lane
//!   threads running one transform. You [`Highway::on_ramp`] items in
//!   and [`Highway::off_ramp`] results out. No bus required; usable as
//!   a standalone parallel executor.
//! - [`HighwayStrand`] — the ramps. A [`Strand`] that hosts a
//!   `Highway` segment: it subscribes to an input type on the ground
//!   bus (the on-ramp), feeds every message onto the highway, and
//!   re-publishes finished results as an output type (the off-ramp).
//!
//! ## Routing is by type
//!
//! Like the rest of the bus, the highway routes by `TypeId`, not by
//! socket name. Give a `HighwayStrand` **distinct newtypes** for its
//! input and output (e.g. `Work(u64)` in, `Done(u64)` out) so the
//! kernel can tell promoted traffic from demoted traffic. Both types
//! must be registered for fan-out via
//! [`spiderweb::register_clone_type`] (or be one of the kernel's
//! built-in types) before the bus delivers them.
//!
//! ## No new dependencies
//!
//! The highway is built from the same primitives as the kernel: plain
//! threads and channels from `std`. No Tokio, no `async`. The lanes
//! poll a shared job queue with a short park, matching the kernel's
//! own idiom.

#![forbid(unsafe_code)]

use spiderweb::{BusHandle, Socket, Strand, StrandError, Tag, Vibration, SYS_VIBRATION};
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

/// Something went wrong moving traffic on or off the highway.
#[derive(Debug)]
pub enum HighwayError {
    /// The on-ramp is closed: every lane has shut down, so the highway
    /// can no longer accept work. This happens after the [`Highway`]
    /// has been dropped or its lanes panicked.
    Closed,
}

impl fmt::Display for HighwayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HighwayError::Closed => write!(f, "highway on-ramp is closed (no live lanes)"),
        }
    }
}

/// Default ratio of soft capacity to lanes. A highway lets a small
/// pipeline of work queue ahead of its lanes (for throughput) but not
/// an unbounded one — past `lanes * this`, [`Highway::has_room`] reports
/// the highway is full and the caller should apply backpressure.
const DEFAULT_CAPACITY_PER_LANE: usize = 4;

impl std::error::Error for HighwayError {}

// -----------------------------------------------------------------------------
// Highway — the parallel layer
// -----------------------------------------------------------------------------

/// The transform a lane applies to each item. It is cloned (via `Arc`)
/// into every lane thread, so it must be `Send + Sync`. It is called
/// once per item, in parallel across lanes; keep it stateless.
type LaneFn<I, O> = Arc<dyn Fn(I) -> O + Send + Sync>;

/// A pool of parallel lanes running one transform.
///
/// Construct it with [`Highway::new`], push work in with
/// [`Highway::on_ramp`], and collect finished results with
/// [`Highway::off_ramp`]. Results come back in **completion order**,
/// not submission order — the highway is unordered, exactly like
/// fan-out on the ground bus.
///
/// Dropping the highway closes the on-ramp and joins every lane, so a
/// `Highway` never leaks threads.
pub struct Highway<I, O> {
    /// The single producer end of the shared job queue. Held by the
    /// highway; every lane shares the receiver behind a mutex. Each job
    /// carries its provenance [`Tag`] glued to the item, so the tag
    /// survives lane reordering and the input-tick≠output-tick gap.
    job_tx: Sender<(Tag, I)>,
    /// The single consumer end of the results channel. Lanes hold
    /// cloned producer ends. Results carry the same tag the job entered
    /// with.
    result_rx: Receiver<(Tag, O)>,
    handles: Vec<JoinHandle<()>>,
    lanes: usize,
    /// Work accepted onto the highway but not yet finished by a lane:
    /// incremented on [`Highway::on_ramp`], decremented when a lane
    /// produces a result. The web's congestion gauge — when it reaches
    /// `capacity`, the highway is full.
    in_flight: Arc<AtomicUsize>,
    /// Soft ceiling on `in_flight`. Surfaced through
    /// [`Highway::has_room`]; not hard-enforced by `on_ramp`, so a
    /// caller that ignores backpressure can still over-fill (the count
    /// stays accurate either way).
    capacity: usize,
}

impl<I, O> Highway<I, O>
where
    I: Send + 'static,
    O: Send + 'static,
{
    /// Open a highway with `lanes` parallel lanes, each running `f`,
    /// and a default soft capacity of `lanes * 4`.
    ///
    /// Panics if `lanes` is zero — a highway needs at least one lane.
    pub fn new<F>(lanes: usize, f: F) -> Self
    where
        F: Fn(I) -> O + Send + Sync + 'static,
    {
        Self::with_capacity(lanes, lanes * DEFAULT_CAPACITY_PER_LANE, f)
    }

    /// Open a highway with an explicit soft `capacity` — how much work
    /// may be in flight before [`Highway::has_room`] reports the
    /// highway is full and the caller should hold back.
    ///
    /// Panics if `lanes` or `capacity` is zero.
    pub fn with_capacity<F>(lanes: usize, capacity: usize, f: F) -> Self
    where
        F: Fn(I) -> O + Send + Sync + 'static,
    {
        assert!(lanes >= 1, "a highway needs at least one lane");
        assert!(
            capacity >= 1,
            "a highway needs capacity for at least one item"
        );

        let f: LaneFn<I, O> = Arc::new(f);
        let (job_tx, job_rx) = mpsc::channel::<(Tag, I)>();
        let job_rx = Arc::new(Mutex::new(job_rx));
        let (result_tx, result_rx) = mpsc::channel::<(Tag, O)>();
        let in_flight = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::with_capacity(lanes);
        for lane in 0..lanes {
            let job_rx = Arc::clone(&job_rx);
            let result_tx = result_tx.clone();
            let f = Arc::clone(&f);
            let in_flight = Arc::clone(&in_flight);
            let handle = thread::Builder::new()
                .name(format!("highway-lane-{lane}"))
                .spawn(move || run_lane(job_rx, result_tx, f, in_flight))
                .expect("failed to spawn highway lane");
            handles.push(handle);
        }
        // `result_tx` is dropped here. Each lane holds its own clone,
        // so `result_rx` stays connected until the last lane exits.

        Highway {
            job_tx,
            result_rx,
            handles,
            lanes,
            in_flight,
            capacity,
        }
    }

    /// On-ramp: lift one item from the ground onto the highway. The
    /// next free lane will pick it up. Non-blocking. Counts the item
    /// as in flight; check [`Highway::has_room`] first if you want to
    /// respect backpressure. The item travels with no thread tag — use
    /// [`Highway::on_ramp_tagged`] to preserve provenance.
    pub fn on_ramp(&self, item: I) -> Result<(), HighwayError> {
        self.on_ramp_tagged(None, item)
    }

    /// On-ramp carrying a provenance [`Tag`]. The tag rides through the
    /// lanes glued to the item and comes back out the off-ramp, so a
    /// result demoted many ticks later still names its origin thread.
    /// Used by [`HighwayStrand`]; ordinary callers use
    /// [`Highway::on_ramp`].
    pub fn on_ramp_tagged(&self, tag: Tag, item: I) -> Result<(), HighwayError> {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        match self.job_tx.send((tag, item)) {
            Ok(()) => Ok(()),
            Err(_) => {
                // Never delivered — undo the count so the gauge stays
                // honest.
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                Err(HighwayError::Closed)
            }
        }
    }

    /// Off-ramp: take every result a lane has finished so far, in
    /// completion order. Non-blocking — returns what is ready right
    /// now and leaves the lanes running. Drops thread tags — use
    /// [`Highway::off_ramp_tagged`] to keep them.
    pub fn off_ramp(&self) -> Vec<O> {
        self.result_rx.try_iter().map(|(_tag, o)| o).collect()
    }

    /// Off-ramp keeping each result's provenance [`Tag`] (the one its
    /// item entered the on-ramp with). Used by [`HighwayStrand`] to
    /// re-publish results on their origin thread.
    pub fn off_ramp_tagged(&self) -> Vec<(Tag, O)> {
        self.result_rx.try_iter().collect()
    }

    /// How many parallel lanes this highway runs.
    pub fn lanes(&self) -> usize {
        self.lanes
    }

    /// Work accepted but not yet finished — the congestion gauge.
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }

    /// The soft capacity this highway was built with.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Whether the highway can take more work without exceeding its
    /// soft capacity. When this returns `false`, the highway is
    /// congested and the caller should hold back and let the web feel
    /// the backpressure.
    pub fn has_room(&self) -> bool {
        self.in_flight() < self.capacity
    }
}

impl<I, O> Drop for Highway<I, O> {
    fn drop(&mut self) {
        // Close the on-ramp first: swap the live `job_tx` for a sender
        // to a dead channel so the real one drops now. With the
        // producer gone, every lane's `try_recv` returns
        // `Disconnected` and the lane exits its loop. THEN we can join
        // without deadlocking on lanes that would otherwise spin
        // waiting for work.
        let (dead_tx, _dead_rx) = mpsc::channel();
        drop(std::mem::replace(&mut self.job_tx, dead_tx));
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

/// One lane's loop: pull a job, run the transform, push the result,
/// repeat until the on-ramp closes.
fn run_lane<I, O>(
    job_rx: Arc<Mutex<Receiver<(Tag, I)>>>,
    result_tx: Sender<(Tag, O)>,
    f: LaneFn<I, O>,
    in_flight: Arc<AtomicUsize>,
) {
    loop {
        // Hold the queue lock ONLY for the non-blocking `try_recv`, so
        // other lanes can pull while this lane runs `f`. Holding the
        // lock across a blocking `recv()` would serialize the whole
        // highway down to one lane.
        let job = match job_rx.lock() {
            Ok(guard) => guard.try_recv(),
            // A lane panicked and poisoned the queue. Bail out rather
            // than panic again; the rest of the highway keeps running.
            Err(_) => return,
        };

        match job {
            Ok((tag, item)) => {
                // The transform runs on the item only; the tag rides
                // through untouched, glued to its result.
                let out = f(item);
                // The item is finished: free its capacity now, before
                // we even hand off the result. A finished result
                // waiting in the off-ramp is no longer "in flight".
                in_flight.fetch_sub(1, Ordering::SeqCst);
                if result_tx.send((tag, out)).is_err() {
                    // The off-ramp (the `Highway`) is gone. Nothing
                    // left to do.
                    return;
                }
            }
            // No work right now. Park briefly and retry. This matches
            // the kernel's polling idiom rather than blocking.
            Err(TryRecvError::Empty) => thread::sleep(Duration::from_millis(1)),
            // The on-ramp closed (highway dropped). Exit.
            Err(TryRecvError::Disconnected) => return,
        }
    }
}

// -----------------------------------------------------------------------------
// HighwayStrand — the on-ramp / off-ramp pair on the bus
// -----------------------------------------------------------------------------

/// A [`Strand`] that bolts a [`Highway`] onto the ground bus.
///
/// Register one of these and the kernel treats it like any other
/// strand, but inside, every tick it:
///
/// 1. **On-ramps** — drains its input socket and lifts each message
///    onto the highway, where the lanes process them in parallel.
/// 2. **Off-ramps** — collects whatever the lanes have finished and
///    re-publishes each result on its output socket, back onto the
///    ground bus.
///
/// The strand itself does no work; it is purely the elevation between
/// the ground and the highway. The parallelism lives in the highway's
/// own lane threads, which run independently of the strand's tick.
pub struct HighwayStrand<I, O> {
    name: String,
    in_sockets: [Socket; 1],
    out_sockets: [Socket; 2],
    out_socket: &'static str,
    highway: Highway<I, O>,
    /// Items (with their provenance tags) pulled off the ground bus but
    /// not yet on-ramped, because the highway was full. The strand's own
    /// holding area: it absorbs bursts and retries each tick, so
    /// backpressure *holds* work rather than dropping it — and the tag
    /// stays glued to its item the whole way through.
    pending: VecDeque<(Tag, I)>,
    /// Backlog depth at the last vibration we emitted. We only emit
    /// when the depth changes, so a steady backlog doesn't spam the
    /// web with identical vibrations.
    last_queued: usize,
    idle: Duration,
}

impl<I, O> HighwayStrand<I, O>
where
    I: Send + 'static,
    O: Send + Clone + 'static,
{
    /// Build a highway strand.
    ///
    /// - `name` — the strand's unique name on the bus.
    /// - `in_socket` — the ground socket it listens on (the on-ramp).
    ///   Its type `I` is the routing key.
    /// - `out_socket` — the ground socket it publishes results on (the
    ///   off-ramp). Its type `O` is the routing key.
    /// - `lanes` — how many items to process in parallel.
    /// - `transform` — the work each lane runs, `I -> O`.
    ///
    /// Remember to register `I` and `O` with
    /// [`spiderweb::register_clone_type`] at boot unless they are
    /// built-in types.
    pub fn new<F>(
        name: impl Into<String>,
        in_socket: &'static str,
        out_socket: &'static str,
        lanes: usize,
        transform: F,
    ) -> Self
    where
        F: Fn(I) -> O + Send + Sync + 'static,
    {
        HighwayStrand {
            name: name.into(),
            in_sockets: [Socket::new::<I>(in_socket)],
            out_sockets: [
                Socket::new::<O>(out_socket),
                Socket::new::<Vibration>(SYS_VIBRATION),
            ],
            out_socket,
            highway: Highway::new(lanes, transform),
            pending: VecDeque::new(),
            last_queued: 0,
            idle: Duration::from_millis(5),
        }
    }

    /// Build a highway strand with an explicit soft capacity (how much
    /// work may be in flight before the strand starts holding inbound
    /// messages and emitting backpressure vibrations). See
    /// [`Highway::with_capacity`].
    pub fn with_capacity<F>(
        name: impl Into<String>,
        in_socket: &'static str,
        out_socket: &'static str,
        lanes: usize,
        capacity: usize,
        transform: F,
    ) -> Self
    where
        F: Fn(I) -> O + Send + Sync + 'static,
    {
        HighwayStrand {
            name: name.into(),
            in_sockets: [Socket::new::<I>(in_socket)],
            out_sockets: [
                Socket::new::<O>(out_socket),
                Socket::new::<Vibration>(SYS_VIBRATION),
            ],
            out_socket,
            highway: Highway::with_capacity(lanes, capacity, transform),
            pending: VecDeque::new(),
            last_queued: 0,
            idle: Duration::from_millis(5),
        }
    }

    /// How many lanes the hosted highway runs.
    pub fn lanes(&self) -> usize {
        self.highway.lanes()
    }
}

impl<I, O> Strand for HighwayStrand<I, O>
where
    I: Send + 'static,
    O: Send + Clone + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn inputs(&self) -> &[Socket] {
        &self.in_sockets
    }

    fn outputs(&self) -> &[Socket] {
        &self.out_sockets
    }

    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        // INTAKE: pull every inbound ground message into the strand's
        // holding area, keeping each item's provenance tag. We don't
        // on-ramp directly — the highway may be full, and we never drop
        // work. Using the tagged recv so the origin thread rides through
        // the lanes with its item.
        for (_sock, tag, item) in bus.recv_tagged::<I>()? {
            self.pending.push_back((tag, item));
        }

        // ON-RAMP: lift held work onto the highway while it has room.
        // When the highway fills, we stop and leave the rest in
        // `pending` for the next tick.
        while self.highway.has_room() {
            match self.pending.pop_front() {
                Some((tag, item)) => {
                    if self.highway.on_ramp_tagged(tag, item).is_err() {
                        return Err(StrandError::Crashed(
                            "highway on-ramp closed (lanes gone)".into(),
                        ));
                    }
                }
                None => break,
            }
        }

        // VIBRATION: if work is still held back, the highway is
        // congested — let the web feel it. We emit only when the
        // backlog depth changes, so a steady jam doesn't spam the bus.
        let queued = self.pending.len();
        if queued != self.last_queued {
            if queued > 0 {
                bus.publish_static(
                    SYS_VIBRATION,
                    Vibration::Backpressure {
                        node: self.name.clone(),
                        queued,
                        in_flight: self.highway.in_flight(),
                        capacity: self.highway.capacity(),
                    },
                )?;
            }
            self.last_queued = queued;
        }

        // OFF-RAMP: demote every finished result back onto the ground,
        // re-stamping it with the origin thread it carried through the
        // lanes — so provenance survives the parallel layer's
        // reordering and the input-tick≠output-tick gap.
        for (tag, out) in self.highway.off_ramp_tagged() {
            bus.publish_tagged(self.out_socket, tag, out)?;
        }

        bus.sleep(self.idle);
        Ok(())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    /// Every item pushed onto the highway comes back transformed, with
    /// nothing lost or duplicated. We sort because the highway is
    /// unordered (completion order, not submission order).
    #[test]
    fn highway_processes_every_item() {
        let highway = Highway::new(4, |n: u64| n * n);
        for n in 0..100 {
            highway.on_ramp(n).unwrap();
        }

        let mut got = Vec::new();
        let start = Instant::now();
        while got.len() < 100 && start.elapsed() < Duration::from_secs(5) {
            got.extend(highway.off_ramp());
            thread::sleep(Duration::from_millis(1));
        }

        got.sort_unstable();
        let want: Vec<u64> = (0..100).map(|n| n * n).collect();
        assert_eq!(got, want, "every item should be squared exactly once");
    }

    /// The lanes really run in parallel. Sixteen items that each take
    /// 50ms would be 800ms serially; across eight lanes they finish in
    /// roughly two 50ms rounds. We assert well under the serial time to
    /// prove the speedup without being flaky.
    #[test]
    fn lanes_run_in_parallel() {
        let lanes = 8;
        let items = 16;
        let per_item = Duration::from_millis(50);
        let serial = per_item * items as u32; // 800ms

        let highway = Highway::new(lanes, move |n: u64| {
            thread::sleep(per_item);
            n
        });

        let start = Instant::now();
        for n in 0..items {
            highway.on_ramp(n as u64).unwrap();
        }
        let mut count = 0;
        while count < items && start.elapsed() < Duration::from_secs(10) {
            count += highway.off_ramp().len();
            thread::sleep(Duration::from_millis(1));
        }
        let elapsed = start.elapsed();

        assert_eq!(count, items, "all items must complete");
        assert!(
            elapsed < serial / 2,
            "8 lanes should beat serial ({:?}) by a wide margin, took {:?}",
            serial,
            elapsed
        );
    }

    /// A single-lane highway is effectively serial but must still be
    /// correct. Guards the `lanes == 1` boundary.
    #[test]
    fn single_lane_is_correct() {
        let highway = Highway::new(1, |s: String| format!("{s}!"));
        highway.on_ramp("a".to_string()).unwrap();
        highway.on_ramp("b".to_string()).unwrap();

        let mut got = Vec::new();
        let start = Instant::now();
        while got.len() < 2 && start.elapsed() < Duration::from_secs(2) {
            got.extend(highway.off_ramp());
            thread::sleep(Duration::from_millis(1));
        }
        got.sort();
        assert_eq!(got, vec!["a!".to_string(), "b!".to_string()]);
    }

    /// `new` gives a bounded default capacity of `lanes * 4` rather
    /// than an unbounded queue.
    #[test]
    fn default_capacity_is_bounded() {
        let highway = Highway::new(3, |n: u64| n);
        assert_eq!(highway.capacity(), 12);
        assert_eq!(highway.lanes(), 3);
    }

    /// `has_room` tracks in-flight work against capacity: false when the
    /// highway fills, true again as lanes drain it. This is the gauge
    /// backpressure rides on.
    #[test]
    fn has_room_reflects_capacity() {
        // Slow lanes so work stays in flight long enough to observe.
        let highway = Highway::with_capacity(2, 4, |n: u64| {
            thread::sleep(Duration::from_millis(200));
            n
        });
        assert!(highway.has_room(), "a fresh highway has room");
        assert_eq!(highway.capacity(), 4);

        for n in 0..4 {
            highway.on_ramp(n).unwrap();
        }
        // All four are accepted but none can finish for ~200ms, so the
        // highway is full right now.
        assert_eq!(highway.in_flight(), 4);
        assert!(!highway.has_room(), "highway should be full at capacity");

        // Drain. Once lanes finish, capacity frees up again.
        let mut got = 0;
        let start = Instant::now();
        while got < 4 && start.elapsed() < Duration::from_secs(3) {
            got += highway.off_ramp().len();
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(got, 4);
        assert_eq!(highway.in_flight(), 0);
        assert!(highway.has_room(), "highway has room again after draining");
    }

    #[test]
    #[should_panic(expected = "at least one lane")]
    fn zero_lanes_panics() {
        let _highway = Highway::new(0, |n: u64| n);
    }

    #[test]
    #[should_panic(expected = "capacity for at least one item")]
    fn zero_capacity_panics() {
        let _highway = Highway::with_capacity(2, 0, |n: u64| n);
    }
}
