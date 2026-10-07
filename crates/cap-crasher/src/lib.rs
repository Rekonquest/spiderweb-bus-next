//! `cap-crasher` — a strand that crashes after a configurable number of
//! ticks. Used to test the spider's restart policy.
//!
//! The strand has no inputs and no outputs. It just ticks a counter and
//! returns `StrandError::Crashed` after `crash_after` ticks. The kernel
//! will see the crash, restart the strand (up to 3 times), and then
//! quarantine it.

use spiderweb::{BusHandle, Socket, Strand, StrandError};
use std::time::Duration;

pub struct CapCrasher {
    counter: u32,
    crash_after: u32,
    tag: String,
}

impl CapCrasher {
    pub fn new(crash_after: u32) -> Self {
        CapCrasher {
            counter: 0,
            crash_after,
            tag: "cap-crasher".to_string(),
        }
    }

    pub fn with_tag(crash_after: u32, tag: impl Into<String>) -> Self {
        CapCrasher {
            counter: 0,
            crash_after,
            tag: tag.into(),
        }
    }
}

impl Strand for CapCrasher {
    fn name(&self) -> &str {
        &self.tag
    }

    fn inputs(&self) -> &[Socket] {
        &[]
    }

    fn outputs(&self) -> &[Socket] {
        &[]
    }

    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        self.counter += 1;
        bus.log(&format!("tick {}/{}", self.counter, self.crash_after));
        if self.counter >= self.crash_after {
            return Err(StrandError::Crashed(format!(
                "intentional crash after {} ticks",
                self.counter
            )));
        }
        bus.sleep(Duration::from_millis(500));
        Ok(())
    }
}
