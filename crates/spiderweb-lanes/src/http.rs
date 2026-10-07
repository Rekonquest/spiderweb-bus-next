//! HTTP lane — a std-only HTTP/1.1 on-ramp/off-ramp.
//!
//! - [`wire`] — pure mechanism: the bus types and a bounded parser/serializer.
//! - [`server`] — [`HttpServer`] → [`HttpIngress`] (requests → bus) +
//!   [`HttpResponder`] (bus → responses).
//! - [`client`] — [`HttpClient`] → [`HttpEgress`] (bus → outbound calls → bus),
//!   round trips on a worker pool.
//!
//! [`HttpLane`] wraps either/both sides as a [`Lane`] for `add_lane`.

pub mod client;
pub mod server;
pub mod wire;

pub use client::{HttpClient, HttpEgress};
pub use server::{HttpIngress, HttpResponder, HttpServer};
pub use wire::{HttpCall, HttpRequest, HttpResponse, HttpResult};

use crate::lane::{Bridge, Lane, LaneError, LaneParts};
use spiderweb::Strand;

/// An HTTP lane as a [`Lane`]: configure a server side ([`HttpLane::serve`])
/// and/or a client side ([`HttpLane::client`]); `install` yields the strands.
#[derive(Default)]
pub struct HttpLane {
    server: Option<(&'static str, &'static str, String)>,
    client: Option<(&'static str, &'static str, usize)>,
    bridges: Vec<Bridge>,
}

impl HttpLane {
    pub fn new() -> Self {
        Self::default()
    }

    /// Serve inbound HTTP on `addr`: publish requests on `req_socket`, read
    /// handler replies on `resp_socket`.
    pub fn serve(
        mut self,
        req_socket: &'static str,
        resp_socket: &'static str,
        addr: impl Into<String>,
    ) -> Self {
        self.bridges.push(Bridge {
            tag: "http.request",
            socket: req_socket,
        });
        self.server = Some((req_socket, resp_socket, addr.into()));
        self
    }

    /// Perform outbound HTTP: take calls on `call_socket`, publish results on
    /// `result_socket`, with a `lanes`-wide worker pool.
    pub fn client(
        mut self,
        call_socket: &'static str,
        result_socket: &'static str,
        lanes: usize,
    ) -> Self {
        self.bridges.push(Bridge {
            tag: "http.call",
            socket: call_socket,
        });
        self.client = Some((call_socket, result_socket, lanes));
        self
    }
}

impl Lane for HttpLane {
    fn kind(&self) -> &'static str {
        "http"
    }

    fn bridges(&self) -> &[Bridge] {
        &self.bridges
    }

    fn install(&self) -> Result<LaneParts, LaneError> {
        let mut strands: Vec<(String, Box<dyn Strand>)> = Vec::new();
        let mut bound = None;
        if let Some((req, resp, addr)) = &self.server {
            let server = HttpServer::new(req, resp);
            let ingress = server.ingress(addr.as_str()).map_err(LaneError::Bind)?;
            bound = Some(ingress.local_addr());
            strands.push(("in".into(), Box::new(ingress)));
            strands.push(("resp".into(), Box::new(server.responder())));
        }
        if let Some((call, result, lanes)) = &self.client {
            let client = HttpClient::new(call, result);
            strands.push(("client".into(), Box::new(client.egress(*lanes))));
        }
        if strands.is_empty() {
            return Err(LaneError::Config(
                "HttpLane needs at least one of .serve() / .client()".into(),
            ));
        }
        Ok(LaneParts {
            strands,
            guard: None,
            bound,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spiderweb::{Bus, BusHandle, Socket, StrandError};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// Full lane end to end over a REAL TCP socket: an `HttpEgress` calls an
    /// `HttpIngress` on the same box, an `Echo` handler answers, and the
    /// `HttpResult` lands on the bus — request → bus → response → bus.
    #[test]
    fn http_round_trips_over_a_real_socket() {
        // Server side: bind first so we know the port.
        let server = HttpServer::new("http.req", "http.resp");
        let ingress = server.ingress("127.0.0.1:0").expect("bind");
        let port = ingress.local_addr().port();
        let responder = server.responder();

        // Client side.
        let client = HttpClient::new("http.call", "http.result");
        let egress = client.egress(2);

        // Handler: HttpRequest → HttpResponse echoing the path, same reply_id.
        struct Echo;
        impl Strand for Echo {
            fn name(&self) -> &str {
                "echo"
            }
            fn inputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<HttpRequest>("http.req");
                std::slice::from_ref(&S)
            }
            fn outputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<HttpResponse>("http.resp");
                std::slice::from_ref(&S)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                for r in bus.recv::<HttpRequest>("http.req").unwrap_or_default() {
                    bus.publish_static(
                        "http.resp",
                        HttpResponse::text(r.reply_id, format!("echo:{}", r.path)),
                    )?;
                }
                bus.sleep(Duration::from_millis(5));
                Ok(())
            }
        }

        // Source: fire one HttpCall at the loopback server.
        struct Once {
            fired: bool,
            port: u16,
        }
        impl Strand for Once {
            fn name(&self) -> &str {
                "once"
            }
            fn inputs(&self) -> &[Socket] {
                &[]
            }
            fn outputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<HttpCall>("http.call");
                std::slice::from_ref(&S)
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                if !self.fired {
                    self.fired = true;
                    bus.publish_static(
                        "http.call",
                        HttpCall {
                            id: 1,
                            method: "GET".into(),
                            url: format!("http://127.0.0.1:{port}/hello", port = self.port),
                            headers: Vec::new(),
                            body: Vec::new(),
                        },
                    )?;
                }
                bus.sleep(Duration::from_millis(10));
                Ok(())
            }
        }

        let got = Arc::new(Mutex::new(Vec::<HttpResult>::new()));
        struct Sink(Arc<Mutex<Vec<HttpResult>>>);
        impl Strand for Sink {
            fn name(&self) -> &str {
                "sink"
            }
            fn inputs(&self) -> &[Socket] {
                const S: Socket = Socket::new::<HttpResult>("http.result");
                std::slice::from_ref(&S)
            }
            fn outputs(&self) -> &[Socket] {
                &[]
            }
            fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
                for r in bus.recv::<HttpResult>("http.result").unwrap_or_default() {
                    self.0.lock().unwrap().push(r);
                }
                bus.sleep(Duration::from_millis(5));
                Ok(())
            }
        }

        let mut bus = Bus::open();
        bus.register(ingress);
        bus.register(responder);
        bus.register(egress);
        bus.register(Echo);
        bus.register(Sink(got.clone()));
        bus.register(Once { fired: false, port });
        bus.run_until(Some(Duration::from_millis(1500)));

        let results = got.lock().unwrap().clone();
        assert_eq!(
            results.len(),
            1,
            "exactly one HttpResult expected, got {results:?}"
        );
        let r = &results[0];
        assert_eq!(r.id, 1);
        assert_eq!(r.status, 200, "error: {:?}", r.error);
        assert_eq!(r.body, b"echo:/hello");
    }
}
