//! `preload` — Layer 2 of the spiderweb bus: read-ahead preloading.
//!
//! The fabric doing work ahead of time. Where the [`highway`] is a
//! sink-side fan-out pool, the [`Preloader`] is its source-side dual: a
//! pool of background fetch threads that run an expensive (or jittery)
//! `fetch` *ahead of demand* and bank the results in a bounded
//! ready-buffer, so the next item is already materialized when the bus
//! asks for it — like a CPU pipeline prefetching from cache.
//!
//! ## What it actually buys you
//!
//! This bus is push-based with one worker thread per strand, so adjacent
//! stages **already overlap** — a plain source's next fetch runs on its
//! worker thread while the downstream processes the previous item on
//! *its* thread. So read-ahead does **not** raise steady-state
//! throughput; that stays bounded by the slowest stage. The real wins:
//!
//! - **Zero-wait handoff** — a [`PreloadStrand`]'s tick is O(1)
//!   (pop a ready item and publish); it never stalls on a slow fetch.
//! - **Jitter absorption** — a depth-`D` buffer banks items during fast
//!   fetches so a later slow fetch is hidden behind them, clipping the
//!   *max* inter-arrival gap downstream even though the *mean* is
//!   unchanged.
//! - **Starvation protection for the highway** — a depth `D >= lanes`
//!   buffer lets a K-lane highway pull K already-prepared items at once
//!   across a fetch stall, keeping its lanes full.
//!
//! What it does NOT buy: throughput. A buffer can't beat a fetch whose
//! *mean* exceeds downstream demand — it just empties (correctly
//! fetch-bound). To raise the mean for a hungry highway, use
//! [`Preloader::with_workers`] (parallel fetch), named separately so it
//! is never conflated with plain read-ahead.
//!
//! ## No new dependencies
//!
//! Plain `std` threads + a bounded `sync_channel`, no Tokio, no `async`
//! — the same idiom as the kernel and the highway.
//!
//! [`highway`]: https://docs.rs/highway

#![forbid(unsafe_code)]

use spiderweb::{BusHandle, Socket, Strand, StrandError, Vibration, SYS_VIBRATION};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

/// Something went wrong driving the preloader.
#[derive(Debug)]
pub enum PreloadError {
    /// The ready-buffer is closed: every fetch thread has shut down, so
    /// no more items will arrive. Happens after the [`Preloader`] is
    /// dropped or its fetchers panicked.
    Closed,
}

impl fmt::Display for PreloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PreloadError::Closed => write!(f, "preloader is closed (no live fetchers)"),
        }
    }
}

impl std::error::Error for PreloadError {}

// -----------------------------------------------------------------------------
// Preloader — the read-ahead engine
// -----------------------------------------------------------------------------

/// A pool of background fetch threads that read ahead into a bounded
/// ready-buffer.
///
/// Build it with [`Preloader::new`] (one fetch thread, FIFO order) or
/// [`Preloader::with_workers`] (N threads, completion order). Pull
/// banked items with [`Preloader::take`] — non-blocking, because the
/// fetch already ran in the background. Dropping the preloader stops and
/// joins every fetch thread, so it never leaks threads.
pub struct Preloader<T> {
    /// The bounded ready-buffer. `sync_channel(depth)` blocks a fetcher
    /// on `send` once `depth` items are banked, so memory is bounded and
    /// the fetch paces itself to demand — no busy-spin.
    ready_rx: Receiver<T>,
    /// Signals fetchers to stop (set on drop).
    stop: Arc<AtomicBool>,
    /// Occupancy gauge — items banked but not yet taken. An UPPER bound
    /// on the channel's true occupancy (it can read `depth` + the number
    /// of fetchers mid-send); the hard memory bound is the channel.
    banked: Arc<AtomicUsize>,
    /// Fetch threads still running. Zero means the source is exhausted.
    live: Arc<AtomicUsize>,
    handles: Vec<JoinHandle<()>>,
    depth: usize,
}

impl<T: Send + 'static> Preloader<T> {
    /// Open a preloader with one fetch thread reading ahead into a
    /// bounded buffer of `depth` items. `fetch(i)` produces item `i`
    /// (indices monotonic from 0), or `None` when the source is
    /// exhausted. Single-thread, so items come out in FIFO order.
    ///
    /// Panics if `depth` is zero.
    pub fn new<F>(depth: usize, fetch: F) -> Self
    where
        F: FnMut(u64) -> Option<T> + Send + 'static,
    {
        Self::build(depth, vec![Box::new(fetch)])
    }

    /// Open a preloader with `workers` fetch threads sharing one
    /// monotonic index, all filling one `depth`-bounded buffer.
    ///
    /// This is the explicit **parallel-fetch** variant — the only way to
    /// raise the *mean* fetch rate (e.g. to keep a K-lane highway fed
    /// when a single fetch is too slow). Items come out in **completion
    /// order** (unordered), like the highway's off-ramp; the fetch for
    /// index `i` must not depend on `i-1`.
    ///
    /// `make_fetch(worker)` builds the fetch closure for each worker.
    /// Panics if `depth` or `workers` is zero.
    pub fn with_workers<F>(depth: usize, workers: usize, make_fetch: impl Fn(usize) -> F) -> Self
    where
        F: FnMut(u64) -> Option<T> + Send + 'static,
    {
        assert!(workers >= 1, "a preloader needs at least one fetch worker");
        let fetchers: Vec<Box<dyn FnMut(u64) -> Option<T> + Send>> = (0..workers)
            .map(|w| Box::new(make_fetch(w)) as Box<dyn FnMut(u64) -> Option<T> + Send>)
            .collect();
        Self::build(depth, fetchers)
    }

    fn build(depth: usize, fetchers: Vec<Box<dyn FnMut(u64) -> Option<T> + Send>>) -> Self {
        assert!(
            depth >= 1,
            "a preloader needs a buffer depth of at least one"
        );

        let (ready_tx, ready_rx) = mpsc::sync_channel::<T>(depth);
        let stop = Arc::new(AtomicBool::new(false));
        let banked = Arc::new(AtomicUsize::new(0));
        let seq = Arc::new(AtomicU64::new(0));
        let live = Arc::new(AtomicUsize::new(fetchers.len()));

        let mut handles = Vec::with_capacity(fetchers.len());
        for (w, fetch) in fetchers.into_iter().enumerate() {
            let ready_tx = ready_tx.clone();
            let stop = Arc::clone(&stop);
            let banked = Arc::clone(&banked);
            let seq = Arc::clone(&seq);
            let live = Arc::clone(&live);
            let handle = thread::Builder::new()
                .name(format!("preload-fetch-{w}"))
                .spawn(move || run_fetcher(fetch, ready_tx, stop, banked, seq, live))
                .expect("failed to spawn preload fetcher");
            handles.push(handle);
        }
        // `ready_tx` is dropped here; each fetcher holds its own clone.

        Preloader {
            ready_rx,
            stop,
            banked,
            live,
            handles,
            depth,
        }
    }

    /// Take one banked item, if any. Non-blocking — returns immediately
    /// because the fetch already ran ahead. `None` means the buffer is
    /// momentarily empty (the consumer outran the fetch).
    pub fn take(&self) -> Option<T> {
        match self.ready_rx.try_recv() {
            Ok(item) => {
                // Saturating, purely defensively — every channel item was
                // counted before its send, so this can't actually
                // underflow.
                self.banked
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                        Some(n.saturating_sub(1))
                    })
                    .ok();
                Some(item)
            }
            Err(_) => None,
        }
    }

    /// Drain every item banked right now (completion order). Lets a
    /// K-hungry consumer (a K-lane highway) pull K prepared items at
    /// once.
    pub fn take_ready(&self) -> Vec<T> {
        let mut out = Vec::new();
        while let Some(item) = self.take() {
            out.push(item);
        }
        out
    }

    /// How many items are banked and ready right now (an upper-bound
    /// gauge — see the field doc; the hard bound is the channel depth).
    pub fn ready_len(&self) -> usize {
        self.banked.load(Ordering::SeqCst)
    }

    /// The buffer depth this preloader was built with.
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Whether the ready-buffer has run below `low` — the source/fetch
    /// is failing to keep it full (starvation).
    pub fn is_starved(&self, low: usize) -> bool {
        self.ready_len() < low
    }

    /// Whether the source is exhausted: every fetch thread has finished,
    /// so no more items will ever be banked.
    pub fn is_exhausted(&self) -> bool {
        self.live.load(Ordering::SeqCst) == 0
    }
}

impl<T> Drop for Preloader<T> {
    fn drop(&mut self) {
        // Tell fetchers to stop, then disconnect the buffer so any
        // fetcher blocked on a full `send` wakes with an error and
        // exits. Replacing the receiver drops the real one, which
        // disconnects the channel. THEN join — same close-intake-then-
        // join discipline the highway uses, so a blocked send can never
        // hang the join.
        self.stop.store(true, Ordering::SeqCst);
        let (_dead_tx, dead_rx) = mpsc::sync_channel(1);
        drop(std::mem::replace(&mut self.ready_rx, dead_rx));
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

/// One fetch thread's loop: claim the next index, fetch it, bank the
/// result, repeat until exhausted or stopped.
fn run_fetcher<T>(
    mut fetch: Box<dyn FnMut(u64) -> Option<T> + Send>,
    ready_tx: SyncSender<T>,
    stop: Arc<AtomicBool>,
    banked: Arc<AtomicUsize>,
    seq: Arc<AtomicU64>,
    live: Arc<AtomicUsize>,
) {
    // Decrement `live` on EVERY exit path — normal return, any `break`,
    // OR a panic unwinding out of the `fetch` closure. Without this
    // guard a panicking fetch would skip the decrement, leaving
    // `is_exhausted()` permanently false and the `PreloadStrand`
    // immortal (it would never detach and would spam starvation). A
    // panicking fetch ends this worker; with one fetcher that cleanly
    // exhausts the source, and with `with_workers` it drops the index
    // this worker had claimed and continues on the others.
    struct LiveGuard(Arc<AtomicUsize>);
    impl Drop for LiveGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let _live = LiveGuard(live);

    loop {
        if stop.load(Ordering::Acquire) {
            break;
        }
        let i = seq.fetch_add(1, Ordering::SeqCst);
        match fetch(i) {
            Some(item) => {
                // Count it BEFORE the (possibly blocking) send so the
                // gauge is an UPPER bound on occupancy: it can briefly
                // over-read by the number of fetchers mid-send, but
                // never under-reads — so the starvation signal can't
                // false-fire. The hard memory bound is the channel
                // itself (`sync_channel(depth)`).
                banked.fetch_add(1, Ordering::SeqCst);
                if ready_tx.send(item).is_err() {
                    // The buffer was disconnected (preloader dropped).
                    banked.fetch_sub(1, Ordering::SeqCst);
                    break;
                }
            }
            None => break, // source exhausted
        }
    }
}

// -----------------------------------------------------------------------------
// PreloadStrand — the read-ahead source on the bus
// -----------------------------------------------------------------------------

/// A source [`Strand`] backed by a [`Preloader`]. It has no inputs; each
/// tick it drains up to `burst` ready items and publishes them — the
/// fetch already happened on the preloader's background threads, so the
/// tick never blocks. When the buffer runs dry it emits a
/// [`Vibration::Starved`] (the source-side dual of the highway's
/// backpressure), and when the source is exhausted it detaches cleanly.
pub struct PreloadStrand<T> {
    name: String,
    out_sockets: [Socket; 2],
    out_socket: &'static str,
    preloader: Preloader<T>,
    burst: usize,
    low_water: usize,
    starved: bool,
    /// The `ready` value at the last starvation vibration, so we re-emit
    /// only when the drought deepens/eases — a true dual of the
    /// highway's `queued != last_queued` backpressure dedup.
    last_ready: usize,
    idle: Duration,
}

impl<T: Send + Clone + 'static> PreloadStrand<T> {
    /// A preload source emitting on `out_socket`, reading ahead `depth`
    /// items with one fetch thread. Emits up to `depth` items per tick.
    pub fn new<F>(name: impl Into<String>, out_socket: &'static str, depth: usize, fetch: F) -> Self
    where
        F: FnMut(u64) -> Option<T> + Send + 'static,
    {
        Self::from_preloader(name, out_socket, depth, depth, Preloader::new(depth, fetch))
    }

    /// Like [`new`](Self::new) but with an explicit per-tick `burst` —
    /// how many ready items to publish each tick. Set `burst >= K` to
    /// feed a K-lane highway a full batch at once.
    pub fn with_burst<F>(
        name: impl Into<String>,
        out_socket: &'static str,
        depth: usize,
        burst: usize,
        fetch: F,
    ) -> Self
    where
        F: FnMut(u64) -> Option<T> + Send + 'static,
    {
        Self::from_preloader(name, out_socket, depth, burst, Preloader::new(depth, fetch))
    }

    /// A preload source over a **parallel-fetch** preloader (`workers`
    /// fetch threads). Items are emitted in completion order — pair it
    /// with an unordered consumer like the highway. Raises the mean
    /// fetch rate to keep a hungry highway fed.
    pub fn with_workers<F>(
        name: impl Into<String>,
        out_socket: &'static str,
        depth: usize,
        workers: usize,
        burst: usize,
        make_fetch: impl Fn(usize) -> F,
    ) -> Self
    where
        F: FnMut(u64) -> Option<T> + Send + 'static,
    {
        Self::from_preloader(
            name,
            out_socket,
            depth,
            burst,
            Preloader::with_workers(depth, workers, make_fetch),
        )
    }

    fn from_preloader(
        name: impl Into<String>,
        out_socket: &'static str,
        depth: usize,
        burst: usize,
        preloader: Preloader<T>,
    ) -> Self {
        assert!(
            burst >= 1,
            "a preload strand must publish at least one item per tick"
        );
        PreloadStrand {
            name: name.into(),
            out_sockets: [
                Socket::new::<T>(out_socket),
                Socket::new::<Vibration>(SYS_VIBRATION),
            ],
            out_socket,
            preloader,
            burst,
            // Below this much banked, the buffer is "running dry".
            low_water: depth.div_ceil(2).max(1),
            starved: false,
            last_ready: 0,
            idle: Duration::from_millis(5),
        }
    }

    /// How many items are prepared and waiting right now.
    pub fn ready_len(&self) -> usize {
        self.preloader.ready_len()
    }
}

impl<T: Send + Clone + 'static> Strand for PreloadStrand<T> {
    fn name(&self) -> &str {
        &self.name
    }

    fn inputs(&self) -> &[Socket] {
        &[]
    }

    fn outputs(&self) -> &[Socket] {
        &self.out_sockets
    }

    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        // EMIT: publish up to `burst` already-prepared items. O(1) per
        // item — the fetch already ran in the background.
        let mut published = 0;
        while published < self.burst {
            match self.preloader.take() {
                Some(item) => {
                    bus.publish(self.out_socket, item)?;
                    published += 1;
                }
                None => break,
            }
        }

        // EXHAUSTED: source is done and the buffer is drained → detach.
        if published == 0 && self.preloader.is_exhausted() && self.preloader.ready_len() == 0 {
            return Err(StrandError::Detach);
        }

        // STARVATION: the buffer is running dry and the source isn't
        // done — the fetch can't keep up. Snapshot the gauge ONCE so the
        // decision and the reported value agree, and re-emit when the
        // drought depth changes (a true dual of the highway's
        // re-emit-on-change backpressure), so the spider can gauge how
        // dry it is without a steady drought spamming the bus.
        let ready = self.preloader.ready_len();
        let starved = !self.preloader.is_exhausted() && ready < self.low_water;
        if starved && (!self.starved || ready != self.last_ready) {
            bus.publish_static(
                SYS_VIBRATION,
                Vibration::Starved {
                    node: self.name.clone(),
                    ready,
                    depth: self.preloader.depth(),
                },
            )?;
            self.last_ready = ready;
        }
        self.starved = starved;

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

    /// Every item is fetched once, in order (single fetcher is FIFO),
    /// with nothing lost or duplicated.
    #[test]
    fn every_item_emitted_once_in_order() {
        let pre = Preloader::new(4, |i| if i < 100 { Some(i) } else { None });
        let mut got = Vec::new();
        let start = Instant::now();
        while got.len() < 100 && start.elapsed() < Duration::from_secs(5) {
            got.extend(pre.take_ready());
            thread::sleep(Duration::from_millis(1));
        }
        let want: Vec<u64> = (0..100).collect();
        assert_eq!(got, want, "single-fetcher preload is ordered, lossless");
    }

    /// The ready-buffer is bounded: with an instant fetch and no
    /// consumer, the fetcher blocks on the full channel and occupancy
    /// never grows. The gauge is an upper bound — it may read `depth`
    /// plus the one fetcher blocked mid-send (so `depth + 1` for a
    /// single fetcher), but never more.
    #[test]
    fn buffer_is_bounded() {
        let pre = Preloader::new(4, |_| Some(1u64));
        // Give the fetcher time to fill and block on the full buffer.
        let start = Instant::now();
        while pre.ready_len() < 4 && start.elapsed() < Duration::from_secs(2) {
            thread::sleep(Duration::from_millis(1));
        }
        thread::sleep(Duration::from_millis(20));
        assert!(
            pre.ready_len() <= 5,
            "occupancy must stay bounded (depth 4 + 1 fetcher mid-send), got {}",
            pre.ready_len()
        );
    }

    /// Once banked, every handoff is O(1): no `take` waits on a fetch.
    #[test]
    fn zero_wait_handoff() {
        let pre = Preloader::new(8, |i| if i < 8 { Some(i) } else { None });
        let start = Instant::now();
        while pre.ready_len() < 8 && start.elapsed() < Duration::from_secs(2) {
            thread::sleep(Duration::from_millis(1));
        }
        // Eight items are banked; taking them all must be near-instant.
        let t = Instant::now();
        let mut n = 0;
        while n < 8 {
            if pre.take().is_some() {
                n += 1;
            }
        }
        assert!(
            t.elapsed() < Duration::from_millis(50),
            "banked handoff should be instant, took {:?}",
            t.elapsed()
        );
    }

    /// Read-ahead absorbs a fetch stall. The consumer is slower than the
    /// fast fetch (25ms vs 5ms), so a deep buffer banks items ahead and
    /// hides a 200ms stall behind them — clipping the max inter-arrival
    /// gap. Depth 1 (read-ahead disabled) exposes the full stall.
    #[test]
    fn jitter_is_absorbed() {
        const N: u64 = 20;
        // Fixed latency script: a 200ms stall on item 14, else 5ms.
        fn latency(i: u64) -> Duration {
            if i == 14 {
                Duration::from_millis(200)
            } else {
                Duration::from_millis(5)
            }
        }
        // Consume at a steady 25ms cadence; report the max gap.
        fn max_gap(depth: usize) -> Duration {
            let pre = Preloader::new(depth, move |i| {
                if i < N {
                    thread::sleep(latency(i));
                    Some(i)
                } else {
                    None
                }
            });
            let mut last = Instant::now();
            let mut max = Duration::ZERO;
            let mut n = 0;
            let start = Instant::now();
            while n < N && start.elapsed() < Duration::from_secs(15) {
                if pre.take().is_some() {
                    let now = Instant::now();
                    max = max.max(now - last);
                    last = now;
                    n += 1;
                }
                thread::sleep(Duration::from_millis(25));
            }
            max
        }

        let deep = max_gap(16);
        let shallow = max_gap(1);
        assert!(
            deep < Duration::from_millis(120),
            "deep read-ahead should clip the stall, max gap was {deep:?}"
        );
        assert!(
            shallow >= Duration::from_millis(120),
            "depth 1 (no read-ahead) should expose the 200ms stall, got {shallow:?}"
        );
    }

    /// Parallel fetch yields every item exactly once (completion order).
    #[test]
    fn parallel_workers_fetch_everything() {
        let pre = Preloader::with_workers(8, 4, |_w| {
            move |i: u64| {
                if i < 50 {
                    thread::sleep(Duration::from_millis(2 + (i % 4) * 3));
                    Some(i)
                } else {
                    None
                }
            }
        });
        let mut got = Vec::new();
        let start = Instant::now();
        while got.len() < 50 && start.elapsed() < Duration::from_secs(5) {
            got.extend(pre.take_ready());
            thread::sleep(Duration::from_millis(1));
        }
        got.sort_unstable();
        let want: Vec<u64> = (0..50).collect();
        assert_eq!(got, want, "parallel fetch is lossless (unordered)");
    }

    /// Dropping a preloader whose fetcher is blocked on a full buffer
    /// returns promptly (no hung join, no leaked thread).
    #[test]
    fn drop_joins_even_when_blocked() {
        let pre = Preloader::new(2, |_| Some(1u64));
        // Let it fill and block on the full buffer.
        let start = Instant::now();
        while pre.ready_len() < 2 && start.elapsed() < Duration::from_secs(2) {
            thread::sleep(Duration::from_millis(1));
        }
        let t = Instant::now();
        drop(pre);
        assert!(
            t.elapsed() < Duration::from_secs(1),
            "drop should join promptly even with a blocked send, took {:?}",
            t.elapsed()
        );
    }

    #[test]
    #[should_panic(expected = "buffer depth of at least one")]
    fn zero_depth_panics() {
        let _pre = Preloader::new(0, |_| Some(1u64));
    }

    #[test]
    #[should_panic(expected = "at least one fetch worker")]
    fn zero_workers_panics() {
        let _pre = Preloader::with_workers(4, 0, |_w| |_i| Some(1u64));
    }

    /// A panicking fetch closure must NOT leave the preloader immortal:
    /// the worker's `live` count is decremented (via the RAII guard) even
    /// on unwind, so `is_exhausted()` becomes true. Guards the highest
    /// review finding.
    #[test]
    fn panicking_fetch_marks_exhausted() {
        // The deliberate panic prints to stderr from the fetch thread;
        // cargo captures it on success. (We avoid set_hook so as not to
        // race the process-global panic hook with parallel tests.)
        let pre = Preloader::new(4, |i| {
            assert!(i != 2, "deliberate fetch panic for the test");
            Some(i)
        });
        let start = Instant::now();
        while !pre.is_exhausted() && start.elapsed() < Duration::from_secs(2) {
            thread::sleep(Duration::from_millis(2));
        }
        assert!(
            pre.is_exhausted(),
            "a panicking fetcher must still mark the source exhausted"
        );
    }

    /// `is_starved` reflects the buffer level against a threshold.
    #[test]
    fn is_starved_reflects_buffer() {
        // A finite, instant source fills the buffer, then runs dry.
        let pre = Preloader::new(4, |i| if i < 2 { Some(i) } else { None });
        let start = Instant::now();
        while pre.ready_len() < 2 && start.elapsed() < Duration::from_secs(2) {
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            !pre.is_starved(2),
            "two banked items meet the low-water of 2"
        );
        // Drain it; now it's below the threshold.
        while pre.take().is_some() {}
        assert!(pre.is_starved(2), "an empty buffer is starved");
    }

    /// `with_burst` builds a working strand; it constructs and reports
    /// its configured read-ahead depth.
    #[test]
    fn with_burst_constructs() {
        let strand = PreloadStrand::<u64>::with_burst("loader", "out", 6, 2, |i| {
            if i < 3 {
                Some(i)
            } else {
                None
            }
        });
        assert_eq!(strand.name(), "loader");
        // The read-ahead engine is depth 6 regardless of the burst.
        assert_eq!(strand.preloader.depth(), 6);
    }
}
