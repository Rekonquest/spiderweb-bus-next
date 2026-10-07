//! Backpressure demo: a fast source feeds a fast sink and a slow sink.
//!
//! Run with: `cargo run -p cap-source --example backpressure`
//!
//! Expected:
//! - `cap-source-fast` publishes a string every 100ms.
//! - `cap-sink-fast` keeps up (50ms poll).
//! - `cap-slow-sink` sleeps 500ms per message — it falls behind.
//! - v1.1 kernel will keep delivering to both. The slow sink's inbox
//!   will grow but `mpsc::channel` is unbounded, so we won't see drops
//!   here. The point of this demo is to *see* the slow sink fall
//!   behind and watch the difference in arrival rate.
//! - v2 will add explicit inbox capacity + drop events.

use cap_sink::CapSink;
use cap_slow_sink::CapSlowSink;
use cap_source::CapSource;
use spider::Spider;
use spiderweb::Bus;
use std::time::Duration;

fn main() {
    let mut bus = Bus::open();
    // Fast source: 10 publishes/sec.
    bus.register(CapSource::with_interval(Duration::from_millis(100)));
    // Fast sink.
    bus.register(CapSink::with_tag("cap-sink-fast"));
    // Slow sink: 2 msg/sec.
    bus.register(CapSlowSink::with_tag(
        Duration::from_millis(500),
        "cap-slow-sink",
    ));
    // The spider watches sys.lifecycle.
    let spider_spec = spiderweb::StrandSpec::new("spider", || Box::new(Spider::new()));
    bus.register_spider(spider_spec);
    bus.run_until(Some(Duration::from_secs(6)));
}
