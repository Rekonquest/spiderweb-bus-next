//! A lifecycle event published before a consumer subscribed must still reach it.
//!
//! Work traffic already has this property: `deliver` appends every message to a
//! per-type replay buffer, and `subscribe` replays that buffer to a new
//! subscriber atomically with the subscriber snapshot (so delivery is
//! exactly-once — never both a replay and a direct send, never neither).
//!
//! Lifecycle traffic does not. `publish_lifecycle` calls `deliver_internal`,
//! which fans out only to the subscribers registered at that instant and never
//! touches `pending`. So a `Crashed` published before the supervisor subscribed
//! reaches nobody: the slot is parked waiting for a `ControlMsg` that can never
//! arrive, and the strand is never restarted or quarantined. A failure report
//! that goes missing entirely is worse than one that gets acted on.
//!
//! This test is deliberately deterministic — it does not depend on thread
//! timing, unlike the real-world manifestation (which only shows up under the
//! thread pressure of a full parallel test run). It publishes the lifecycle
//! event with no subscriber alive, waits long enough that the event has
//! certainly been published, and only then introduces a consumer.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use spiderweb::{Bus, BusHandle, LifecycleEvent, Socket, Strand, StrandError};

/// Crashes the instant it runs. Registered as a LIVE strand, so its worker
/// starts — and reports its crash — before `run_until` is ever called.
struct CrashesImmediately;

impl Strand for CrashesImmediately {
    fn name(&self) -> &str {
        "crashes-immediately"
    }

    fn inputs(&self) -> &[Socket] {
        &[]
    }

    fn outputs(&self) -> &[Socket] {
        &[]
    }

    fn run(&mut self, _bus: &mut BusHandle) -> Result<(), StrandError> {
        Err(StrandError::Crashed("published while nobody listened".into()))
    }
}

/// Records every lifecycle event it is handed.
struct LifecycleRecorder {
    seen: Arc<Mutex<Vec<LifecycleEvent>>>,
}

const LIFECYCLE_IN: Socket = Socket::new::<LifecycleEvent>("sys.lifecycle");

impl Strand for LifecycleRecorder {
    fn name(&self) -> &str {
        "lifecycle-recorder"
    }

    fn inputs(&self) -> &[Socket] {
        std::slice::from_ref(&LIFECYCLE_IN)
    }

    fn outputs(&self) -> &[Socket] {
        &[]
    }

    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        if bus.is_shutting_down() {
            return Err(StrandError::Detach);
        }
        for evt in bus.recv::<LifecycleEvent>("sys.lifecycle")? {
            self.seen.lock().unwrap().push(evt);
        }
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

#[test]
fn a_lifecycle_event_published_before_a_subscriber_exists_reaches_it() {
    let mut bus = Bus::open();
    let seen = Arc::new(Mutex::new(Vec::new()));

    // A live strand starts its worker immediately, before `run_until`.
    bus.register(CrashesImmediately);

    // Deterministically let that crash be published — with no consumer alive.
    std::thread::sleep(Duration::from_millis(400));

    // Only now does a consumer exist. It subscribes on its first tick.
    bus.register(LifecycleRecorder {
        seen: Arc::clone(&seen),
    });

    bus.run_until(Some(Duration::from_millis(700)));

    let seen = seen.lock().unwrap();
    let crashes = seen
        .iter()
        .filter(|e| matches!(e, LifecycleEvent::Crashed { .. }))
        .count();
    assert_eq!(
        crashes, 1,
        "the recorder subscribed after the crash was published. Work traffic would be \
         replayed from the pending buffer; lifecycle must be too, or a crash is \
         silently unrecoverable. events seen: {seen:?}"
    );
}
