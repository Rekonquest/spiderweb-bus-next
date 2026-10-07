//! Preload on the bus: starvation vibrations and clean exhaustion.

use preload::PreloadStrand;
use spiderweb::{
    register_clone_type, Bus, BusHandle, Socket, Strand, StrandError, Vibration, SYS_VIBRATION,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
struct Item(u64);

/// Counts items received.
struct Sink {
    got: Arc<AtomicU32>,
}

impl Strand for Sink {
    fn name(&self) -> &str {
        "sink"
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<Item>("item");
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for (_sock, Item(_n)) in bus.recv_all::<Item>()? {
            self.got.fetch_add(1, Ordering::SeqCst);
        }
        bus.sleep(Duration::from_millis(2));
        Ok(())
    }
}

/// Counts starvation vibrations felt on the web.
struct Seismograph {
    starved: Arc<AtomicU32>,
}

impl Strand for Seismograph {
    fn name(&self) -> &str {
        "seismograph"
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<Vibration>(SYS_VIBRATION);
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for (sock, vib) in bus.recv_all::<Vibration>()? {
            if sock == SYS_VIBRATION {
                if let Vibration::Starved { .. } = vib {
                    self.starved.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        bus.sleep(Duration::from_millis(2));
        Ok(())
    }
}

/// A slow fetch feeding a fast sink keeps the ready-buffer dry, so the
/// preload strand emits a starvation vibration the web can feel.
#[test]
fn starvation_vibration_fires() {
    register_clone_type::<Item>();
    let starved = Arc::new(AtomicU32::new(0));

    let mut bus = Bus::open();
    bus.register(Seismograph {
        starved: starved.clone(),
    });
    bus.register(Sink {
        got: Arc::new(AtomicU32::new(0)),
    });
    bus.register(PreloadStrand::<Item>::new("loader", "item", 4, |i| {
        if i < 6 {
            std::thread::sleep(Duration::from_millis(40)); // slow fetch
            Some(Item(i))
        } else {
            None
        }
    }));
    bus.run_until(Some(Duration::from_millis(600)));

    assert!(
        starved.load(Ordering::SeqCst) >= 1,
        "a slow fetch should make the web feel starvation"
    );
}

/// When the source is exhausted and the buffer drained, the preload
/// strand detaches cleanly — and every fetched item was delivered.
#[test]
fn exhaustion_delivers_all_then_detaches() {
    register_clone_type::<Item>();
    let got = Arc::new(AtomicU32::new(0));

    let mut bus = Bus::open();
    bus.register(Sink { got: got.clone() });
    bus.register(PreloadStrand::<Item>::new("loader", "item", 8, |i| {
        if i < 5 {
            Some(Item(i))
        } else {
            None
        }
    }));
    bus.run_until(Some(Duration::from_millis(400)));

    assert_eq!(
        got.load(Ordering::SeqCst),
        5,
        "all five fetched items should be delivered before detach"
    );
}
