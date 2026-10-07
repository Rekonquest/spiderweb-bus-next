//! `cap-sink` — a trivial subscriber. Drains whatever's in its inbox and
//! prints it. Subscribes to the same `out.value` socket as `cap-source`,
//! so the kernel fans out: register two `CapSink` instances and watch
//! them both receive every message.

use spiderweb::{BusHandle, Socket, Strand, StrandError};
use std::time::Duration;

pub struct CapSink {
    /// Optional tag, printed with every received message. Useful when
    /// running multiple instances so you can see fan-out.
    pub tag: String,
}

impl CapSink {
    pub fn new() -> Self {
        CapSink {
            tag: "cap-sink".to_string(),
        }
    }

    pub fn with_tag(tag: impl Into<String>) -> Self {
        CapSink { tag: tag.into() }
    }
}

impl Default for CapSink {
    fn default() -> Self {
        Self::new()
    }
}

impl Strand for CapSink {
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
            bus.log(&format!("← {}", msg));
        }
        // No messages? Don't busy-loop. Real workloads can sleep less.
        bus.sleep(Duration::from_millis(50));
        Ok(())
    }
}

// This crate advertises itself to the bus. It subscribes to `String`, so
// it auto-connects to any advertised String publisher (e.g. cap-source)
// by type alone — no wiring.
spiderweb_registry::advertise!("cap-sink", || {
    Box::new(CapSink::new()) as Box<dyn spiderweb::Strand>
});
