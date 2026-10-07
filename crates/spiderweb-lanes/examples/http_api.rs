//! API-over-HTTP lane demo: a bus that answers JSON-RPC over HTTP.
//!
//! Run it, then in another terminal:
//!   curl -s -XPOST http://127.0.0.1:8787/api -d '{"method":"add","params":"AgM="}'
//!   -> {"result":"BQ=="}     (base64 of [5], i.e. 2 + 3)
//!   curl -s -XPOST http://127.0.0.1:8787/api -d '{"method":"echo","params":"aGk="}'
//!   -> {"result":"aGk="}     (echoes "hi")

use spiderweb::Bus;
use spiderweb_lanes::api::{ApiHttpBridge, ApiServer};
use spiderweb_lanes::http::HttpServer;
use std::time::Duration;

fn main() {
    let port = 8787u16;
    let server = HttpServer::new("http.req", "http.resp");
    let ingress = match server.ingress(("127.0.0.1", port)) {
        Ok(i) => i,
        Err(e) => {
            eprintln!("could not bind 127.0.0.1:{port}: {e}");
            return;
        }
    };
    let responder = server.responder();
    let bridge = ApiHttpBridge::new("api-bridge", "/api", "http.req", "http.resp");
    let api = ApiServer::new("api-server")
        .handle("add", |p| {
            if p.len() == 2 {
                Ok(vec![p[0].wrapping_add(p[1])])
            } else {
                Err("add expects exactly 2 bytes".into())
            }
        })
        .handle("echo", |p| Ok(p.to_vec()));

    println!("── Spiderweb API-over-HTTP lane ──────────────────────────────");
    println!("serving JSON-RPC at http://127.0.0.1:{port}/api for 30s");
    println!("try:");
    println!(
        "  curl -s -XPOST http://127.0.0.1:{port}/api -d '{{\"method\":\"add\",\"params\":\"AgM=\"}}'"
    );
    println!("  expect: {{\"result\":\"BQ==\"}}  (base64 of [5])");
    println!("──────────────────────────────────────────────────────────────");

    let mut bus = Bus::open();
    bus.register(ingress);
    bus.register(responder);
    bus.register(bridge);
    bus.register(api);
    bus.run_until(Some(Duration::from_secs(30)));
}
