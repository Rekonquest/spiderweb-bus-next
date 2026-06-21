//! `spider` — the default orchestrator for the spiderweb bus.
//!
//! The spider is a regular strand that subscribes to `sys.lifecycle`.
//! It tracks restart counts per strand. On each `Crashed` event:
//! - 1st, 2nd, 3rd crash → publish `ControlMsg::Restart` to `sys.control`.
//! - 4th crash → publish `ControlMsg::Quarantine` to `sys.control`.
//!
//! The kernel executes the spider's decisions. v1.2: the kernel still
//! has a default auto-restart policy for demos without a spider. v1.3:
//! kernel will be policy-free and rely entirely on the spider.

use spiderweb::{
    BusHandle, ControlMsg, LifecycleEvent, Socket, Strand, StrandError, Vibration, SYS_CONTROL,
    SYS_LIFECYCLE, SYS_VIBRATION,
};
use std::collections::HashMap;
use std::time::Duration;

pub struct Spider {
    /// Per-strand restart counter, so we can log clean messages.
    restarts: HashMap<String, u32>,
}

impl Spider {
    pub fn new() -> Self {
        Spider {
            restarts: HashMap::new(),
        }
    }
}

impl Default for Spider {
    fn default() -> Self {
        Self::new()
    }
}

impl Strand for Spider {
    fn name(&self) -> &str {
        "spider"
    }

    fn inputs(&self) -> &[Socket] {
        // The spider feels two things: lifecycle events (who is online,
        // who crashed) and vibrations (the web's backpressure and other
        // cross-layer signals).
        const SOCKS: [Socket; 2] = [
            Socket::new::<LifecycleEvent>(SYS_LIFECYCLE),
            Socket::new::<Vibration>(SYS_VIBRATION),
        ];
        &SOCKS
    }

    fn outputs(&self) -> &[Socket] {
        const SOCK: Socket = Socket::new::<ControlMsg>(SYS_CONTROL);
        std::slice::from_ref(&SOCK)
    }

    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        let events = bus.recv_all::<LifecycleEvent>()?;
        for (sock, evt) in events {
            if sock != SYS_LIFECYCLE {
                continue;
            }
            match evt {
                LifecycleEvent::Registered { name, inputs, outputs } => {
                    bus.log(&format!(
                        "👁  {} is online (in: {}, out: {})",
                        name,
                        inputs.join(", "),
                        outputs.join(", ")
                    ));
                }
                LifecycleEvent::Detached { name } => {
                    bus.log(&format!("👋 {} detached (clean exit)", name));
                }
                LifecycleEvent::Crashed { name, reason } => {
                    bus.log(&format!("💥 {} crashed: {}", name, reason));
                    // v1.2: the spider drives the restart policy. We
                    // use the same 3-restart-then-quarantine rule
                    // v1.1 had, but the spider sends the control
                    // message instead of the kernel deciding.
                    let count = self.restarts.entry(name.clone()).or_insert(0);
                    *count += 1;
                    if *count <= 3 {
                        bus.log(&format!("🔄 {} sending Restart (attempt {})", name, count));
                        bus.publish_static(SYS_CONTROL, ControlMsg::Restart { name: name.clone() })?;
                    } else {
                        bus.log(&format!("🛑 {} sending Quarantine (4th crash)", name));
                        bus.publish_static(
                            SYS_CONTROL,
                            ControlMsg::Quarantine {
                                name: name.clone(),
                                reason: format!("4 crashes: {}", reason),
                            },
                        )?;
                    }
                }
                LifecycleEvent::Restarted { name, attempt } => {
                    bus.log(&format!("🔄 {} restarted (attempt {})", name, attempt));
                }
                LifecycleEvent::Quarantined { name, reason } => {
                    bus.log(&format!("🛑 {} quarantined: {}", name, reason));
                }
                LifecycleEvent::Buffered { socket, from } => {
                    bus.log(&format!(
                        "⏳ buffered `{}` from {} (no subscriber yet)",
                        socket, from
                    ));
                }
                LifecycleEvent::Flushed { socket, to, count } => {
                    bus.log(&format!(
                        "📨 flushed {} pending on `{}` to {}",
                        count, socket, to
                    ));
                }
                LifecycleEvent::ThreadFormed { thread, origin } => {
                    bus.log(&format!("🧵 thread {} born at {}", thread, origin));
                }
                LifecycleEvent::IntersectionEmerged { node, threads } => {
                    bus.log(&format!(
                        "✕ intersection emerged at {} (threads {:?})",
                        node, threads
                    ));
                }
            }
        }

        // Feel the web's vibrations. v1.3: backpressure. The spider
        // logs it for the operator; a future spider could react by
        // pausing a source or rerouting around the congested node.
        for (sock, vib) in bus.recv_all::<Vibration>()? {
            if sock != SYS_VIBRATION {
                continue;
            }
            match vib {
                Vibration::Backpressure {
                    node,
                    queued,
                    in_flight,
                    capacity,
                } => {
                    bus.log(&format!(
                        "🌊 backpressure from {node}: {queued} queued ({in_flight}/{capacity} in flight)"
                    ));
                }
                Vibration::Starved { node, ready, depth } => {
                    bus.log(&format!(
                        "💨 starvation at {node}: read-ahead buffer dry ({ready}/{depth} ready)"
                    ));
                }
            }
        }

        bus.sleep(Duration::from_millis(50));
        Ok(())
    }
}
