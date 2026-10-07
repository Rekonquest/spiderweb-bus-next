//! `webctl` — the spiderweb bus command-line tool.
//!
//! For v1.2 (no network transport), this is a launcher + log tail.
//! Future versions will support live introspection of a running web.
//!
//! Subcommands:
//! - `webctl list`         — list available demos
//! - `webctl run <name>`   — run a demo by name
//! - `webctl top`          — live dashboard of a running bus (BusStats)
//! - `webctl topology`     — show the static topology reference
//! - `webctl version`      — show the kernel version

use std::process::{Command, Stdio};
use std::time::Duration;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone)]
struct Demo {
    name: &'static str,
    package: &'static str,
    example: &'static str,
    description: &'static str,
    estimated_duration: Duration,
}

const DEMOS: &[Demo] = &[
    Demo {
        name: "demo",
        package: "cap-source",
        example: "demo",
        description: "v1.1: one source, two sinks, no wires. Proves fan-out by type.",
        estimated_duration: Duration::from_secs(5),
    },
    Demo {
        name: "demo-v1-1",
        package: "cap-source",
        example: "demo_v1_1",
        description: "v1.1: source, sinks, spider, and crashers. Proves restart + quarantine.",
        estimated_duration: Duration::from_secs(15),
    },
    Demo {
        name: "backpressure",
        package: "cap-source",
        example: "backpressure",
        description: "v1.2: fast source, fast sink, slow sink. Proves fan-out under congestion.",
        estimated_duration: Duration::from_secs(6),
    },
    Demo {
        name: "image-pipeline",
        package: "cap-image",
        example: "pipeline",
        description: "v1.2: load PNG, resize, save. Real bytes through the bus (base64).",
        estimated_duration: Duration::from_secs(8),
    },
    Demo {
        name: "highway",
        package: "highway",
        example: "highway",
        description: "v1.3: on-ramp a batch to the parallel layer, process across lanes, off-ramp results. Proves the highway beats serial.",
        estimated_duration: Duration::from_secs(3),
    },
    Demo {
        name: "congestion",
        package: "highway",
        example: "congestion",
        description: "v1.3: burst overruns a small highway; it emits backpressure vibrations the spider feels, and drops nothing.",
        estimated_duration: Duration::from_secs(3),
    },
    Demo {
        name: "weave",
        package: "highway",
        example: "weave",
        description: "v1.3: threads form as messages flow; intersections emerge where they cross; a MergeAll hop fuses threads downstream.",
        estimated_duration: Duration::from_secs(5),
    },
    Demo {
        name: "preload",
        package: "preload",
        example: "preload",
        description: "v1.3: read ahead so the next data is ready before it's asked for; parallel read-ahead keeps a starved highway's lanes full (~3x).",
        estimated_duration: Duration::from_secs(4),
    },
    Demo {
        name: "auto-connect",
        package: "cap-source",
        example: "auto_connect",
        description: "v1.3: strands advertise themselves; main() registers nothing — bus.auto_connect() wires them, connecting by type.",
        estimated_duration: Duration::from_secs(3),
    },
    Demo {
        name: "spatial-weave",
        package: "spiderweb-viz",
        example: "spatial_weave",
        description: "v1.3: the web in 3D — ground/highway/spider at z=0/1/2; a vibration felt strongly near, faintly far; exports a 3D scene.",
        estimated_duration: Duration::from_secs(3),
    },
    Demo {
        name: "http-api",
        package: "spiderweb-lanes",
        example: "http_api",
        description: "lanes: a bus answers JSON-RPC over HTTP on :8787 — curl it (add/echo). Shows the HTTP + API lanes.",
        estimated_duration: Duration::from_secs(30),
    },
    Demo {
        name: "p2p-mesh",
        package: "spiderweb-lanes",
        example: "p2p_mesh",
        description: "lanes: 3 in-process buses form a triangle; A broadcasts, B & C receive each message exactly once (per-origin dedup).",
        estimated_duration: Duration::from_secs(2),
    },
];

/// The external protocol lanes the `spiderweb-lanes` crate provides — each an
/// on-ramp/off-ramp between the in-process bus and an outside-world transport.
const LANES: &[(&str, &str)] = &[
    ("tcp", "point-to-point edge transport (the reference Lane)"),
    ("http", "std-only HTTP/1.1 server + client"),
    (
        "api",
        "typed request/reply RPC over the bus, exposed over HTTP (JSON)",
    ),
    ("mesh", "N-peer P2P mesh with per-origin loop suppression"),
    ("nat", "NAT traversal via a rendezvous relay (run `relayd`)"),
    (
        "ssh",
        "bus over a composed system-ssh port-forward (encrypted, zero crypto deps)",
    ),
];

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("list");

    match cmd {
        "list" | "ls" => list_demos(),
        "run" => {
            let name = match args.get(2) {
                Some(n) => n,
                None => {
                    eprintln!("usage: webctl run <demo-name>");
                    eprintln!("run `webctl list` to see available demos");
                    std::process::exit(1);
                }
            };
            run_demo(name);
        }
        "top" => run_top(),
        "topology" => show_topology(),
        "lanes" => list_lanes(),
        "version" | "--version" | "-v" => {
            println!("webctl {}", VERSION);
            println!("spiderweb v1.3");
        }
        "help" | "--help" | "-h" => {
            print_help();
        }
        _ => {
            print_help();
        }
    }
}

fn list_demos() {
    println!("🕸️  spiderweb demos (v1.3):\n");
    for (i, demo) in DEMOS.iter().enumerate() {
        let dur = format!("{}s", demo.estimated_duration.as_secs());
        println!(
            "  {}. {} ({})\n     {}",
            i + 1,
            demo.name,
            dur,
            demo.description
        );
    }
    println!("\nRun with: webctl run <name>");
}

fn list_lanes() {
    println!("🛣️  spiderweb external lanes (spiderweb-lanes):\n");
    for (kind, desc) in LANES {
        println!("  {kind:<5} — {desc}");
    }
    println!("\nEach lane is an on-ramp/off-ramp between the bus and an outside-world");
    println!("transport. See `webctl run http-api` / `webctl run p2p-mesh`.");
    println!("NAT relay server: cargo run -p spiderweb-lanes --bin relayd -- 0.0.0.0:9000");
}

fn run_demo(name: &str) {
    let demo = match DEMOS.iter().find(|d| d.name == name) {
        Some(d) => d,
        None => {
            eprintln!("unknown demo: {}", name);
            eprintln!("run `webctl list` to see available demos");
            std::process::exit(1);
        }
    };
    println!(
        "🕸️  running `{}` ({} seconds)\n",
        demo.name,
        demo.estimated_duration.as_secs()
    );
    let status = Command::new("cargo")
        .args(["run", "-p", demo.package, "--example", demo.example])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status();
    match status {
        Ok(s) if s.success() => println!("\n✓ demo `{}` exited cleanly", demo.name),
        Ok(s) => println!("\n✗ demo `{}` exited with code {:?}", demo.name, s.code()),
        Err(e) => eprintln!("\n✗ failed to run demo: {}", e),
    }
}

fn run_top() {
    // Live dashboard of a running bus: launches the `top` example (a small web
    // + a reporter strand that re-renders BusStats each interval). Introspects
    // a bus from the inside — the in-process analogue of `top` for the web.
    println!("🕸️  launching live `top` …\n");
    let status = Command::new("cargo")
        .args(["run", "-p", "spiderweb-viz", "--example", "top"])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status();
    match status {
        Ok(s) if s.success() => {}
        Ok(s) => println!("\n✗ `top` exited with code {:?}", s.code()),
        Err(e) => eprintln!("\n✗ failed to launch `top`: {}", e),
    }
}

fn show_topology() {
    println!("🕸️  spiderweb bus topology (v1.3):\n");
    println!("Strand types:");
    println!("  cap-source       — publisher (String payload)");
    println!("  cap-sink         — subscriber (String payload)");
    println!("  cap-slow-sink    — slow subscriber (sleeps per msg)");
    println!("  cap-crasher      — test harness (crashes on Nth tick)");
    println!("  cap-image-*      — image processing (load/resize/save)");
    println!("  highway-strand   — Layer 2 on-ramp/off-ramp around a parallel lane pool");
    println!("  spider           — orchestrator (subscribes to sys.lifecycle)");
    println!();
    println!("Reserved sockets:");
    println!("  sys.lifecycle    — kernel → orchestrator events");
    println!("  sys.control      — orchestrator → kernel commands");
    println!("  sys.vibration    — cross-layer signals (backpressure)");
    println!();
    println!("Run `webctl list` to see the demo topologies.");
}

fn print_help() {
    println!("🕸️  webctl — spiderweb bus CLI (v1.3)\n");
    println!("USAGE:");
    println!("    webctl <command>\n");
    println!("COMMANDS:");
    println!("    list           List available demos");
    println!("    run <name>     Run a demo");
    println!("    lanes          List the external protocol lanes (http/api/mesh/nat/ssh)");
    println!("    top            Live dashboard of a running bus (BusStats)");
    println!("    topology       Show the static topology reference");
    println!("    version        Show the version");
    println!("    help           Show this help");
}
