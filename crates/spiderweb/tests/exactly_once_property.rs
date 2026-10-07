//! Property test: exactly-once fan-out delivery under randomized traffic.
//!
//! The kernel's central correctness guarantee is that a published message
//! reaches every subscriber EXACTLY ONCE — never duplicated (the buffer-replay
//! vs. live-send race) and never lost (the subscribe-mid-burst race). The unit
//! tests pin specific races; this hammers the guarantee across many random
//! shapes (random message counts, random subscriber counts, burst publishing).
//! Each sink must end with exactly the published set, each item once.

use spiderweb::{Bus, BusHandle, Socket, Strand, StrandError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Bursts all its messages in the first tick, then detaches.
struct BurstSource {
    msgs: Vec<String>,
    sent: bool,
}
impl Strand for BurstSource {
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
        if self.sent {
            return Err(StrandError::Detach);
        }
        for m in &self.msgs {
            bus.publish_static("note", m.clone())?;
        }
        self.sent = true;
        bus.sleep(Duration::from_millis(5));
        Ok(())
    }
}

struct CollectSink {
    name: String,
    got: Arc<Mutex<Vec<String>>>,
}
impl Strand for CollectSink {
    fn name(&self) -> &str {
        &self.name
    }
    fn inputs(&self) -> &[Socket] {
        const S: Socket = Socket::new::<String>("note");
        std::slice::from_ref(&S)
    }
    fn outputs(&self) -> &[Socket] {
        &[]
    }
    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        for v in bus.recv::<String>("note")? {
            self.got.lock().unwrap().push(v);
        }
        bus.sleep(Duration::from_millis(3));
        Ok(())
    }
}

#[test]
fn fan_out_is_exactly_once_across_random_shapes() {
    let mut s: u64 = 0xDEAD_BEEF_CAFE_F00D;
    let mut rng = || {
        s ^= s >> 12;
        s ^= s << 25;
        s ^= s >> 27;
        s.wrapping_mul(0x2545F4914F6CDD1D)
    };

    for iter in 0..24 {
        let k = 1 + (rng() % 40) as usize; // 1..=40 messages
        let n_sinks = 1 + (rng() % 5) as usize; // 1..=5 sinks
        let expected: Vec<String> = (0..k).map(|i| format!("m{i}")).collect();

        let mut bus = Bus::open();
        bus.register(BurstSource {
            msgs: expected.clone(),
            sent: false,
        });
        let sinks: Vec<Arc<Mutex<Vec<String>>>> = (0..n_sinks)
            .map(|j| {
                let got = Arc::new(Mutex::new(Vec::new()));
                bus.register(CollectSink {
                    name: format!("sink{j}"),
                    got: got.clone(),
                });
                got
            })
            .collect();

        bus.run_until(Some(Duration::from_millis(250)));

        let mut want = expected.clone();
        want.sort();
        for (j, got) in sinks.iter().enumerate() {
            let mut have = got.lock().unwrap().clone();
            have.sort();
            assert_eq!(
                have,
                want,
                "iter {iter}: sink{j} did not receive each of {k} messages exactly once \
                 (n_sinks={n_sinks}); got {} items",
                have.len()
            );
        }
    }
}
