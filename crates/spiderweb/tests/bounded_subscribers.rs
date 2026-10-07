//! Bounded-everything audit: the subscriber map must not grow without bound
//! under crash/restart churn.
//!
//! Every restart installs a fresh subscriber for the strand's input sockets;
//! the previous instance's receiver is dropped when its worker exits, leaving a
//! dead subscriber that nothing used to remove. Sustained churn therefore used
//! to grow the subscriber map (and per-delivery cost) without bound. The kernel
//! now prunes a subscriber lazily the next time a send to it fails. This test
//! drives heavy churn and asserts the live subscriber count stays flat while
//! the restart count climbs.

use spiderweb::{
    Bus, BusHandle, ControlMsg, LifecycleEvent, Socket, Strand, StrandError, StrandSpec,
    SYS_CONTROL, SYS_LIFECYCLE,
};
use std::time::Duration;

/// Subscribes to "feed", then crashes every tick. Each restart re-subscribes.
struct CrashOnFeed;
impl Strand for CrashOnFeed {
    fn name(&self) -> &str {
        "crash-on-feed"
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<String>("feed");
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, _bus: &mut BusHandle) -> Result<(), StrandError> {
        Err(StrandError::Crashed("crash".into()))
    }
}

/// Publishes to "feed" every tick, forever — so deliveries keep happening and
/// dead subscribers keep getting pruned.
struct Feeder;
impl Strand for Feeder {
    fn name(&self) -> &str {
        "feeder"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<String>("feed");
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        bus.publish_static("feed", "x".to_string())?;
        bus.sleep(Duration::from_millis(3));
        Ok(())
    }
}

/// A spider that ALWAYS restarts a crashed strand and never quarantines — so we
/// get unbounded restart churn to stress the subscriber map.
struct AlwaysRestartSpider;
impl Strand for AlwaysRestartSpider {
    fn name(&self) -> &str {
        "restart-spider"
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<LifecycleEvent>(SYS_LIFECYCLE);
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<ControlMsg>(SYS_CONTROL);
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for (sock, evt) in bus.recv_all::<LifecycleEvent>()? {
            if sock != SYS_LIFECYCLE {
                continue;
            }
            if let LifecycleEvent::Crashed { name, .. } = evt {
                bus.publish_static(SYS_CONTROL, ControlMsg::Restart { name })?;
            }
        }
        bus.sleep(Duration::from_millis(2));
        Ok(())
    }
}

#[test]
fn subscriber_map_stays_bounded_under_restart_churn() {
    let mut bus = Bus::open();
    bus.register_spec(StrandSpec::new("crash-on-feed", || Box::new(CrashOnFeed)));
    bus.register(Feeder);
    bus.register_spider(StrandSpec::new("restart-spider", || {
        Box::new(AlwaysRestartSpider)
    }));
    bus.run_until(Some(Duration::from_millis(600)));

    let stats = bus.stats();

    // Sustained churn actually happened.
    assert!(
        stats.restarted >= 5,
        "expected sustained restart churn to exercise the bound, got {} restarts",
        stats.restarted
    );

    // The live subscriber map stayed flat. Live subscribers are a small fixed
    // set: the kernel's own SYS_CONTROL subscriber, the spider's SYS_LIFECYCLE
    // subscriber, and at most the current crash-on-feed instance's "feed"
    // subscriber (+ maybe one not-yet-pruned dead entry). Crucially this does
    // NOT scale with `restarted`: without pruning it would be ~restarted+.
    assert!(
        stats.subscribers <= 6,
        "subscriber map must stay bounded under churn — got {} subscribers after {} restarts \
         (would scale with restarts if dead subscribers were never pruned)",
        stats.subscribers,
        stats.restarted
    );
}
