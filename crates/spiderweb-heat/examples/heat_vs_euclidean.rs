//! Heat-kernel vs Euclidean attenuation — visible side-by-side.
//!
//! Run with: `cargo run --release -p spiderweb-heat --example heat_vs_euclidean`
//!
//! Constructs a fabric where the Euclidean attenuation lies and the heat
//! kernel doesn't: two nodes that are CLOSE in 3D space but DISCONNECTED in
//! the graph (no edge between them), and two nodes that are FAR in 3D but
//! WORN (high-conductance) in the graph. The painted `1/(1+d)` reads the
//! wrong story in both cases.

use spiderweb::{attenuation, FabricAdjacency, FabricEdge, Position};
use spiderweb_heat::{heat_attenuation, DEFAULT_K};

fn main() {
    // Fabric:
    //   source ── (worn, conductance 10) ── relay ── (thin, conductance 1) ── depot
    //   neighbor                                                         (a ghost)
    //
    // 3D positions: source and neighbor are CLOSE (distance 1), but the
    // neighbor is a ghost — no edge from source. source is FAR from depot
    // (distance 10) but the graph connects them through "relay" with a worn
    // path. Euclidean attenuation will tell us neighbor > depot. The graph
    // heat kernel will tell us the opposite — the truth.

    let nodes = ["source", "relay", "depot", "neighbor"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let edges = vec![
        FabricEdge {
            from: "source".into(),
            to: "relay".into(),
            conductance: 10.0,
        },
        FabricEdge {
            from: "relay".into(),
            to: "depot".into(),
            conductance: 1.0,
        },
        // neighbor: present in the vertex set but UNCONNECTED.
    ];
    let adjacency = FabricAdjacency { nodes, edges };

    let pos = |x, y, z| Some(Position::new(x, y, z));
    let source = pos(0.0, 0.0, 0.0);
    let relay = pos(5.0, 0.0, 0.0);
    let depot = pos(10.0, 0.0, 0.0);
    let neighbor = pos(1.0, 0.0, 0.0); // CLOSE to source in 3D, but no edge

    let t = 0.1;

    println!("\n── two nodes, two stories ──\n");
    println!("Fabric:  source ──[c=10]── relay ──[c=1]── depot     +     neighbor (isolated)");
    println!("3D:      source(0,0,0)   relay(5,0,0)  depot(10,0,0)       neighbor(1,0,0)\n");

    println!("                       Euclidean 1/(1+d)        heat-kernel exp(-tL), t={t}");
    println!("                       ─────────────────        ───────────────────────────");
    for (name, p) in [
        ("source → relay   ", relay),
        ("source → depot   ", depot),
        ("source → neighbor", neighbor),
    ] {
        let euclid = attenuation(source, p);
        let receiver = match p {
            v if v == relay => "relay",
            v if v == depot => "depot",
            _ => "neighbor",
        };
        let heat = heat_attenuation(&adjacency, "source", receiver, t, DEFAULT_K).unwrap_or(0.0);
        println!("  {name}    {euclid:>14.4}        {heat:>14.6}");
    }

    println!("\nWhat the contrast shows:");
    println!(
        "  • neighbor is CLOSE in 3D ({} from source) → Euclidean rates it high.",
        1.0_f64
    );
    println!("    But it has NO EDGE — the graph kernel correctly reports 0.0 (vibrations");
    println!("    can't reach it through any strand). The painted Euclidean lies.");
    println!(
        "  • depot is FAR in 3D ({} from source) → Euclidean rates it low.",
        10.0_f64
    );
    println!("    But the graph connects them through 'relay' with a WORN (c=10) edge,");
    println!("    so the heat kernel reports a real share of the source's vibration");
    println!("    arriving — through the actual fabric, weighted by real traffic.");
    println!("\n  topology + observed-traffic conductance → an honest attenuation.\n");
}
