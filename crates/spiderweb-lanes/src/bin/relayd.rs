//! `relayd` — the Spiderweb NAT rendezvous relay.
//!
//! Run a public relay so two buses behind NAT can federate: both dial out to
//! this server and join a room; it forwards each peer's frames to the others.
//!
//! Usage: `relayd [bind-addr]`  (default `0.0.0.0:9000`)

use spiderweb_lanes::nat::Relay;

fn main() {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:9000".to_string());
    match Relay::bind(&addr) {
        Ok(relay) => {
            let shown = relay
                .local_addr()
                .map(|a| a.to_string())
                .unwrap_or_else(|_| addr.clone());
            println!("spiderweb-relayd listening on {shown} (forwarding by room)");
            if let Err(e) = relay.serve() {
                eprintln!("relay error: {e}");
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("failed to bind {addr}: {e}");
            std::process::exit(1);
        }
    }
}
