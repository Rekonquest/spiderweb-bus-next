//! `cap-slow-sink` — a deliberately slow subscriber. Sleeps on each
//! message so the kernel sees congestion. The v1.1 kernel will
//! buffer up to its per-strand inbox capacity (default: bounded by
//! `mpsc::channel`, no explicit cap) and then drop on the next send.
//! v2 will expose `Backpressure` events so the spider can react.

use spiderweb::{BusHandle, Socket, Strand, StrandError};
use std::time::Duration;

pub struct CapSlowSink {
    tag: String,
    sleep_per_msg: Duration,
    /// Count of messages received since boot. Useful for proving
    /// fan-out didn't lose messages to the slow path.
    received: u64,
}

impl CapSlowSink {
    pub fn new(sleep_per_msg: Duration) -> Self {
        CapSlowSink {
            tag: "cap-slow-sink".to_string(),
            sleep_per_msg,
            received: 0,
        }
    }

    pub fn with_tag(sleep_per_msg: Duration, tag: impl Into<String>) -> Self {
        CapSlowSink {
            tag: tag.into(),
            sleep_per_msg,
            received: 0,
        }
    }
}

impl Strand for CapSlowSink {
    fn name(&self) -> &str {
        &self.tag
    }

    fn inputs(&self) -> &[Socket] {
        const SOCK: Socket = Socket::new::<String>("in.value");
        std::slice::from_ref(&SOCK)
    }

    fn outputs(&self) -> &[Socket] {
        &[]
    }

    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        let msgs = bus.recv_all::<String>()?;
        for (_sock, msg) in msgs {
            self.received += 1;
            bus.log(&format!(
                "← {} (received: {}, sleeping {:?})",
                msg, self.received, self.sleep_per_msg
            ));
            bus.sleep(self.sleep_per_msg);
        }
        // Brief idle sleep so we don't busy-loop on an empty inbox.
        bus.sleep(Duration::from_millis(20));
        Ok(())
    }
}
