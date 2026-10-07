//! v1.1 demo: source, two sinks, a spider that watches, and a strand that
//! crashes so we can see restart in action.
//!
//! Run with: `cargo run -p cap-source --example demo_v1_1`
//!
//! Expected: the spider logs every lifecycle event. The cap-crasher-A
//! strand ticks 3 times then crashes; the kernel restarts it (up to 3
//! times); the spider logs each restart. cap-crasher-B ticks 5 times
//! then crashes the same way but with a higher count.

use cap_crasher::CapCrasher;
use cap_sink::CapSink;
use cap_source::CapSource;
use spider::Spider;
use spiderweb::{Bus, StrandSpec};
use std::time::Duration;

fn main() {
    let mut bus = Bus::open();
    bus.register(CapSource::new());
    bus.register(CapSink::with_tag("cap-sink-A"));
    bus.register(CapSink::with_tag("cap-sink-B"));
    // A restartable crashing strand. Factory is called for first run
    // AND for every restart.
    bus.register_spec(StrandSpec::new("cap-crasher-A", || {
        Box::new(CapCrasher::with_tag(3, "cap-crasher-A"))
    }));
    bus.register_spec(StrandSpec::new("cap-crasher-B", || {
        Box::new(CapCrasher::with_tag(5, "cap-crasher-B"))
    }));
    // The spider watches sys.lifecycle.
    bus.register_spider(StrandSpec::new("spider", || Box::new(Spider::new())));
    bus.run_until(Some(Duration::from_secs(15)));
}
