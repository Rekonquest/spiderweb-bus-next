//! `webctl top` — a live, refreshing dashboard of a running bus.
//!
//! Spins up a small but representative web (a source feeding a sink, a strand
//! that crashes and is restarted by the spider) and a `TopReporter` strand that
//! pulls `BusStats` each interval and re-renders the terminal dashboard with
//! [`spiderweb_viz::render_top`] — live per-node throughput and web-wide health,
//! introspected from inside the running bus. Runs for a bounded window, then
//! winds down gracefully.

use spider::Spider;
use spiderweb::{Bus, BusHandle, Socket, Strand, StrandError, StrandSpec};
use std::time::Duration;

/// Publishes a String on "note" every tick.
struct Source;
impl Strand for Source {
    fn name(&self) -> &str {
        "source"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<String>("note");
        std::slice::from_ref(&S)
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        bus.publish_static("note", "tick".to_string())?;
        bus.sleep(Duration::from_millis(20));
        Ok(())
    }
}

/// Drains "note".
struct Sink;
impl Strand for Sink {
    fn name(&self) -> &str {
        "sink"
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<String>("note");
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for _ in bus.recv::<String>("note")? {}
        bus.sleep(Duration::from_millis(20));
        Ok(())
    }
}

/// Crashes every few ticks so the lifecycle counters move (the spider restarts
/// it, eventually quarantines it).
struct Flaky {
    ticks: u32,
}
impl Strand for Flaky {
    fn name(&self) -> &str {
        "flaky"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        self.ticks += 1;
        if self.ticks.is_multiple_of(3) {
            return Err(StrandError::Crashed("scheduled wobble".into()));
        }
        bus.sleep(Duration::from_millis(50));
        Ok(())
    }
}

/// Pulls `BusStats` live and re-renders the dashboard each interval.
struct TopReporter;
impl Strand for TopReporter {
    fn name(&self) -> &str {
        "top-reporter"
    }
    fn inputs(&self) -> &[Socket] {
        &[]
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        let dash = spiderweb_viz::render_top(&bus.stats());
        // Clear screen + home cursor, then paint the frame.
        print!("\x1b[2J\x1b[H{dash}\n  (live — Ctrl-C or wait for the window to end)\n");
        use std::io::Write;
        let _ = std::io::stdout().flush();
        bus.sleep(Duration::from_millis(700));
        Ok(())
    }
}

fn main() {
    println!("🕸️  starting a live web for `top` …");
    let mut bus = Bus::open();
    bus.register(Source);
    bus.register(Sink);
    bus.register_spec(StrandSpec::new("flaky", || Box::new(Flaky { ticks: 0 })));
    bus.register(TopReporter);
    bus.register_spider(StrandSpec::new("spider", || Box::new(Spider::new())));
    bus.run_until(Some(Duration::from_secs(8)));
    println!("\n🕸️  top window ended.");
}
