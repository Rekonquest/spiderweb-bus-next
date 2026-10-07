//! Auto-connect demo: strands advertise themselves; the bus wires them.
//!
//! Run with: `cargo run -p cap-source --example auto_connect`
//!
//! Look at `main()`: it registers NOTHING. `cap-source` and `cap-sink`
//! each advertise themselves in their own crate (one `advertise!` line),
//! and they connect to each other purely by type (both speak `String`).
//! This binary only has to *link* those crates — the `use ... as _;`
//! anchors keep the linker from dead-stripping the adverts — and call
//! `auto_connect()`.
//!
//! The payoff: to add another self-advertising strand, you add its
//! dependency and a one-line anchor. `main()`'s body never changes — no
//! growing `bus.register(...)` list.

use spiderweb::{Bus, Strand};
use spiderweb_registry::{advertise_spider, BusAutoConnect};
use std::time::Duration;

// Link anchors: reference each strand crate so the linker keeps its
// advert. (These are the only lines that grow when you add a strand —
// a dependency edge, not orchestration code.)
use cap_sink as _;
use cap_source as _;

// Advertise the orchestrator from here, so the `spider` crate itself
// stays dependency-light.
advertise_spider!("spider", || Box::new(spider::Spider::new())
    as Box<dyn Strand>);

fn main() {
    let mut bus = Bus::open();
    bus.auto_connect(); // <- the entire wiring step
    bus.run_until(Some(Duration::from_secs(3)));
}
