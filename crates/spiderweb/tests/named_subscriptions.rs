//! Named (exact/prefix) subscription routing and the external `BusPublisher`
//! surface — the two capabilities Imperium's topic-string bus needs.
//!
//! These exercise the PUBLIC API only: a `Bus` opened, a `BusPublisher` taken
//! from it, named subscriptions registered, and messages published from
//! ordinary synchronous code (including from inside a consumer's own delivery
//! path and from many OS threads). The TypeId path is proven unbroken by the
//! crate's own unit tests and by `one_publish_fans_out_to_typeid_and_named_alike`.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use spiderweb::{
    Bus, BusError, BusHandle, ControlMsg, MatchKind, NamedCallback, Payload, Socket, Strand,
    StrandError, SYS_CONTROL,
};

// -----------------------------------------------------------------------------
// Recording callbacks
// -----------------------------------------------------------------------------

/// A named callback that records the socket NAME of every message it receives,
/// in delivery order.
fn name_recorder() -> (Arc<Mutex<Vec<String>>>, NamedCallback) {
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let sink = log.clone();
    let cb: NamedCallback = Box::new(move |socket: &'static str, _payload: Payload| {
        sink.lock().unwrap().push(socket.to_string());
    });
    (log, cb)
}

/// A named callback that records the `String` PAYLOAD of every message it
/// receives, in delivery order.
fn string_recorder() -> (Arc<Mutex<Vec<String>>>, NamedCallback) {
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let sink = log.clone();
    let cb: NamedCallback = Box::new(move |_socket: &'static str, payload: Payload| {
        if let Ok(s) = payload.downcast::<String>() {
            sink.lock().unwrap().push(*s);
        }
    });
    (log, cb)
}

// -----------------------------------------------------------------------------
// Task 1 — named / prefix subscription routing
// -----------------------------------------------------------------------------

/// Exact registration must NOT see a longer topic; prefix registration MUST.
#[test]
fn exact_registration_does_not_receive_a_longer_topic_but_prefix_does() {
    let bus = Bus::open();
    let p = bus.publisher();

    let (exact_log, exact_cb) = name_recorder();
    let (prefix_log, prefix_cb) = name_recorder();
    p.subscribe_exact("net.transport.event", exact_cb);
    p.subscribe_prefix("net.transport.event", prefix_cb);

    p.publish("net.transport.event.wireguard", "wg".to_string())
        .unwrap();
    p.publish("net.transport.event", "evt".to_string())
        .unwrap();

    assert_eq!(
        *exact_log.lock().unwrap(),
        vec!["net.transport.event".to_string()],
        "an EXACT registration must not receive a longer topic"
    );
    assert_eq!(
        *prefix_log.lock().unwrap(),
        vec![
            "net.transport.event.wireguard".to_string(),
            "net.transport.event".to_string()
        ],
        "a PREFIX registration must receive both, in publish order"
    );
}

/// The empty prefix receives everything (Imperium's `subscribe_all`).
#[test]
fn subscribe_all_receives_every_published_topic() {
    let bus = Bus::open();
    let p = bus.publisher();
    let (log, cb) = name_recorder();
    p.subscribe_all(cb);

    p.publish("net.a.one", "1".to_string()).unwrap();
    p.publish("ui.update", "2".to_string()).unwrap();
    p.publish("totally.other", "3".to_string()).unwrap();

    let got = log.lock().unwrap().clone();
    for topic in ["net.a.one", "ui.update", "totally.other"] {
        assert!(
            got.iter().any(|t| t == topic),
            "subscribe_all missed `{topic}`; got {got:?}"
        );
    }
}

/// A `net.a.` prefix must never receive `net.b.` traffic.
#[test]
fn prefix_does_not_cross_namespaces() {
    let bus = Bus::open();
    let p = bus.publisher();
    let (log, cb) = name_recorder();
    p.subscribe_prefix("net.a.", cb);

    p.publish("net.b.thing", "b".to_string()).unwrap();
    p.publish("net.a.thing", "a".to_string()).unwrap();
    p.publish("net.b.again", "bb".to_string()).unwrap();

    assert_eq!(
        *log.lock().unwrap(),
        vec!["net.a.thing".to_string()],
        "a `net.a.` prefix must never receive `net.b.` traffic"
    );
}

/// Per-publisher FIFO: a burst published in order arrives in order.
#[test]
fn ordered_publishes_arrive_in_order_for_one_subscriber() {
    let bus = Bus::open();
    let p = bus.publisher();
    let (log, cb) = string_recorder();
    p.subscribe_prefix("ordered.", cb);

    for i in 0..32 {
        p.publish("ordered.topic", format!("msg-{i}")).unwrap();
    }

    let expected: Vec<String> = (0..32).map(|i| format!("msg-{i}")).collect();
    assert_eq!(
        *log.lock().unwrap(),
        expected,
        "per-publisher FIFO must be preserved"
    );
}

/// No replay: a subscriber registered AFTER a publish must not receive it, and
/// the earlier zero-subscriber publish is still `Ok`.
#[test]
fn late_named_subscriber_receives_no_replay() {
    let bus = Bus::open();
    let p = bus.publisher();

    p.publish("net.a.early", "early".to_string())
        .expect("publish with zero matching subscribers must be Ok");

    let (log, cb) = name_recorder();
    p.subscribe_prefix("net.a.", cb);

    p.publish("net.a.late", "late".to_string()).unwrap();

    assert_eq!(
        *log.lock().unwrap(),
        vec!["net.a.late".to_string()],
        "a subscriber registered after a publish must not receive it"
    );
}

/// Publishing with zero matching subscribers is not an error.
#[test]
fn publish_with_zero_matching_subscribers_is_ok() {
    let bus = Bus::open();
    let p = bus.publisher();

    // No subscribers at all.
    assert!(p.publish("nothing.here", "x".to_string()).is_ok());

    // A subscriber that does NOT match.
    let (log, cb) = name_recorder();
    p.subscribe_prefix("net.a.", cb);
    assert!(p.publish("net.b.elsewhere", "y".to_string()).is_ok());
    assert!(log.lock().unwrap().is_empty());
}

/// `subscribe_match` honours the explicit `MatchKind`.
#[test]
fn subscribe_match_honours_the_requested_kind() {
    let bus = Bus::open();
    let p = bus.publisher();

    let (exact_log, exact_cb) = name_recorder();
    let (prefix_log, prefix_cb) = name_recorder();
    p.subscribe_match("m.k", MatchKind::Exact, exact_cb);
    p.subscribe_match("m.k", MatchKind::Prefix, prefix_cb);

    p.publish("m.k.deep", "d".to_string()).unwrap();
    p.publish("m.k", "k".to_string()).unwrap();

    assert_eq!(*exact_log.lock().unwrap(), vec!["m.k".to_string()]);
    assert_eq!(
        *prefix_log.lock().unwrap(),
        vec!["m.k.deep".to_string(), "m.k".to_string()]
    );
}

// -----------------------------------------------------------------------------
// Task 2 — external (non-strand) publish
// -----------------------------------------------------------------------------

/// A strand that counts every `String` it drains and stays alive, so the run
/// loop keeps running until the test shuts it down.
struct CountingSink {
    count: Arc<AtomicU32>,
}

impl Strand for CountingSink {
    fn name(&self) -> &str {
        "counting-sink"
    }
    fn inputs(&self) -> &[Socket] {
        const SOCK: Socket = Socket::new::<String>("mt.in");
        std::slice::from_ref(&SOCK)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        let msgs = bus.recv_all::<String>()?;
        self.count.fetch_add(msgs.len() as u32, Ordering::SeqCst);
        bus.sleep(Duration::from_millis(2));
        Ok(())
    }
}

/// External publishes from many OS threads, while strands run, must neither
/// deadlock nor lose a message.
#[test]
fn external_publish_from_many_threads_while_strands_run_loses_nothing() {
    const THREADS: u32 = 8;
    const PER_THREAD: u32 = 100;
    const TOTAL: u32 = THREADS * PER_THREAD;

    let count = Arc::new(AtomicU32::new(0));
    let mut bus = Bus::open();
    bus.register(CountingSink {
        count: count.clone(),
    });
    let publisher = Arc::new(bus.publisher());

    // The bus runs on its own thread; publishers hammer it from OS threads.
    let bus_thread = thread::spawn(move || {
        bus.run_until(Some(Duration::from_secs(5)));
    });

    // Watchdog: if any publisher deadlocks, fail instead of hanging.
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let mut workers = Vec::new();
    for t in 0..THREADS {
        let p = publisher.clone();
        let tx = done_tx.clone();
        workers.push(thread::spawn(move || {
            for i in 0..PER_THREAD {
                p.publish("mt.topic", format!("t{t}-{i}")).unwrap();
            }
            let _ = tx.send(());
        }));
    }
    drop(done_tx);
    for _ in 0..THREADS {
        done_rx
            .recv_timeout(Duration::from_secs(4))
            .expect("an external publisher deadlocked");
    }
    for w in workers {
        w.join().unwrap();
    }

    // Every message must reach the running strand: no loss.
    let deadline = Instant::now() + Duration::from_secs(3);
    while count.load(Ordering::SeqCst) < TOTAL && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        count.load(Ordering::SeqCst),
        TOTAL,
        "external publishes must not be lost while strands run"
    );

    // Deterministic shutdown through the same external surface.
    publisher
        .publish(SYS_CONTROL, ControlMsg::Shutdown)
        .expect("shutdown publish");
    bus_thread.join().unwrap();
}

/// A subscriber that publishes during its OWN delivery must not self-deadlock.
#[test]
fn publish_from_inside_a_consumers_own_delivery_does_not_deadlock() {
    let bus = Bus::open();
    let publisher = Arc::new(bus.publisher());

    // Consumer B records replies.
    let (reply_log, reply_cb) = string_recorder();
    publisher.subscribe_exact("sw.reply", reply_cb);

    // Consumer A, on its own delivery, publishes a reply (re-entrant publish).
    let fired_a = Arc::new(AtomicBool::new(false));
    let p_a = publisher.clone();
    let flag = fired_a.clone();
    let cb_a: NamedCallback = Box::new(move |_socket: &'static str, payload: Payload| {
        let name = payload.downcast::<String>().map(|s| *s).unwrap_or_default();
        if !flag.swap(true, Ordering::SeqCst) {
            p_a.publish("sw.reply", format!("replied:{name}"))
                .expect("re-entrant publish");
        }
    });
    publisher.subscribe_exact("sw.trigger", cb_a);

    // Drive the trigger from another thread; the watchdog turns a self-deadlock
    // into a test failure instead of a hang.
    let (tx, rx) = mpsc::channel();
    let p0 = publisher.clone();
    let _driver = thread::spawn(move || {
        let ok = p0.publish("sw.trigger", "hello".to_string()).is_ok();
        let _ = tx.send(ok);
    });
    let ok = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("publish from within delivery deadlocked");
    assert!(ok);
    assert_eq!(*reply_log.lock().unwrap(), vec!["replied:hello".to_string()]);
}

// -----------------------------------------------------------------------------
// Coexistence + error contract
// -----------------------------------------------------------------------------

/// Named routing is an ADDITION to TypeId routing, not a replacement: one
/// publish reaches a TypeId strand subscriber and a named subscriber alike.
#[test]
fn one_publish_fans_out_to_typeid_and_named_alike() {
    let count = Arc::new(AtomicU32::new(0));
    let mut bus = Bus::open();
    bus.register(CountingSink {
        count: count.clone(),
    });
    let publisher = Arc::new(bus.publisher());

    let (named_log, named_cb) = name_recorder();
    publisher.subscribe_prefix("co.", named_cb);

    let bus_thread = thread::spawn(move || {
        bus.run_until(Some(Duration::from_secs(5)));
    });

    publisher
        .publish("co.topic", "payload".to_string())
        .unwrap();

    // Named delivery is inline and already done.
    assert_eq!(*named_log.lock().unwrap(), vec!["co.topic".to_string()]);

    // The TypeId (channel) delivery is asynchronous.
    let deadline = Instant::now() + Duration::from_secs(3);
    while count.load(Ordering::SeqCst) < 1 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "the TypeId path must also receive the publish"
    );

    publisher.publish(SYS_CONTROL, ControlMsg::Shutdown).unwrap();
    bus_thread.join().unwrap();
}

/// The error contract: `TypeMismatch` survives, and the `Lagged`/`Closed`
/// equivalents downstream `#[from]` conversions expect are exposed.
#[test]
fn bus_error_contract_keeps_type_mismatch_and_exposes_lagged_and_closed() {
    let tm = BusError::TypeMismatch {
        socket: "s",
        expected: "u8",
    };
    assert!(tm.to_string().contains("type mismatch"));

    let lagged = BusError::Lagged(7);
    assert!(lagged.to_string().contains('7'));

    let closed = BusError::Closed;
    assert!(closed.to_string().contains("closed"));

    // The `#[from]`-style conversion consumers rely on still works.
    let se: StrandError = tm.into();
    assert!(matches!(se, StrandError::Crashed(_)));
}
