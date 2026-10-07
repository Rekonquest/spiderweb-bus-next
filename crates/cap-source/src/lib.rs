//! `cap-source` — a trivial publisher. Increments a counter, publishes
//! a `String` once a second, and lets you watch the bus route it.

use spiderweb::{BusHandle, Socket, Strand, StrandError};
use std::time::Duration;

pub struct CapSource {
    counter: u64,
    interval: Duration,
}

impl CapSource {
    pub fn new() -> Self {
        CapSource {
            counter: 0,
            interval: Duration::from_millis(1000),
        }
    }

    pub fn with_interval(interval: Duration) -> Self {
        CapSource {
            counter: 0,
            interval,
        }
    }
}

impl Default for CapSource {
    fn default() -> Self {
        Self::new()
    }
}

impl Strand for CapSource {
    fn name(&self) -> &str {
        "cap-source"
    }

    fn inputs(&self) -> &[Socket] {
        &[]
    }

    fn outputs(&self) -> &[Socket] {
        const SOCK: Socket = Socket::new::<String>("out.value");
        std::slice::from_ref(&SOCK)
    }

    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        self.counter += 1;
        let msg = format!("hello #{}", self.counter);
        bus.publish("out.value", msg.clone())?;
        bus.log(&format!("→ {}", msg));
        bus.sleep(self.interval);
        Ok(())
    }
}

// This crate advertises itself to the bus. A binary that calls
// `bus.auto_connect()` gets this strand with no manual registration.
spiderweb_registry::advertise!("cap-source", || {
    Box::new(CapSource::new()) as Box<dyn spiderweb::Strand>
});
