//! Demo: one publisher, two subscribers, no wires.
//!
//! Run with: `cargo run -p cap-source --example demo`
//!
//! Expected output: the publisher prints `→ hello #N` once a second.
//! Both subscribers print `← hello #N` for every message. That proves
//! fan-out by type works — the kernel routed one publish to two
//! subscribers because both declared the same input type.
//!
//! Runs for 5 seconds, then shuts down cleanly.

use cap_sink::CapSink;
use cap_source::CapSource;
use spiderweb::Bus;
use std::time::Duration;

fn main() {
    let mut bus = Bus::open();
    bus.register(CapSource::new());
    bus.register(CapSink::with_tag("cap-sink-A"));
    bus.register(CapSink::with_tag("cap-sink-B"));
    bus.run_until(Some(Duration::from_secs(5)));
}
