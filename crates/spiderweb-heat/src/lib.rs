//! `spiderweb-heat` — graph heat-kernel vibration propagation.
//!
//! Replaces the position-only Euclidean attenuation (`1/(1+d)`) with a
//! topology-aware decay that follows the actual strand graph messages cross.
//! Six independent domains (physics-diffusion forward-Euler, DDPM noise-decay,
//! quantum T2-decoherence, damped eventual-consistency, heat-equation
//! label-propagation, matrix-exponential of −tL) converge on the same operator
//! `H_t = exp(−t·L)` over the graph Laplacian `L = D − A`, so this is one
//! primitive serving the harvest's vibration slot.
//!
//! ## Mechanism (the dumb part)
//! - Build a sparse symmetric adjacency `A` from a [`FabricAdjacency`]: a
//!   directed edge `(from, to, w)` contributes `w` to *both* `A[from][to]` and
//!   `A[to][from]` (symmetrization-by-sum — total traffic between two nodes
//!   regardless of direction). Symmetrization is a generator-policy choice;
//!   alternatives (max, average) are equally valid and we name this one.
//! - The Laplacian acts as `(L·v)_i = d_i · v_i − Σ_j A_ij · v_j` where
//!   `d_i = Σ_j A_ij`. Sparse mat-vec; we never form `L` densely.
//! - Propagate one column of `H_t` from a source: `u(t) = exp(−tL) · e_s`,
//!   evaluated by truncated Taylor (`Σ_{k=0..K} (−tL)^k / k! · e_s`,
//!   `K = 16` default). On a connected graph mass is conserved exactly
//!   (`Σ u_i = 1` at every `t`) because `L`'s rows sum to zero.
//!
//! ## Policy (the orchestrator's part)
//! - `t` is the diffusion-time knob: small `t` keeps the vibration local; large
//!   `t` spreads it across the web. Whoever calls this picks `t`.
//! - The Taylor truncation `K` and the implicit stability bound (`t·max_degree
//!   ≲ 1` for `K=16` to stay accurate) are documented limits, not hidden.
//! - For graphs with high max-degree or large `t`, increase `K` or scale-and-
//!   square (next tier; the cheapest generator covers the common case).
//!
//! ## Honest walls
//! - **Forward-Euler / Taylor CFL.** For fixed `K`, accuracy degrades when
//!   `t · ρ(L)` exceeds the truncation's radius of convergence. The function
//!   returns the truncated sum unconditionally; callers using large `t` on
//!   high-degree graphs should raise `K` or pick a smaller `t`.
//! - **Edge-cumulative conductance.** `FabricAdjacency.edge_traffic` is
//!   cumulative deliveries (does not decay). On a long-lived bus, a once-hot
//!   now-dead edge keeps its weight forever. Decay (EWMA on `edge_traffic`)
//!   would be a separate, additive change on the kernel side.

#![forbid(unsafe_code)]

use spiderweb::{FabricAdjacency, FabricSnapshot, NodeId};
use std::collections::HashMap;

/// Default Taylor truncation depth. `K = 16` covers `t · max_degree ≲ 4`
/// with double-precision accuracy; raise it for larger `t`.
pub const DEFAULT_K: usize = 16;

/// One column of `H_t = exp(−t·L)` from `source`, returned as
/// `(node, felt_strength)` for every node in `adjacency.nodes` (sorted by node
/// name — same ordering as the input). The source itself appears with the
/// largest entry; neighbors get a share of the mass; unreachable nodes (in
/// other connected components) stay at 0.
///
/// Returns an empty `Vec` if `source` is not in `adjacency.nodes`.
///
/// `t` is the diffusion time; `0.0` returns `e_source` (1.0 at source, 0
/// elsewhere). `k` is the Taylor truncation depth; use [`DEFAULT_K`] unless
/// you need finer control.
pub fn heat_propagate(
    adjacency: &FabricAdjacency,
    source: &str,
    t: f64,
    k: usize,
) -> Vec<(NodeId, f64)> {
    let n = adjacency.nodes.len();
    if n == 0 {
        return Vec::new();
    }
    let Some(source_idx) = adjacency.nodes.iter().position(|nd| nd == source) else {
        return Vec::new();
    };

    // Index node names → matrix rows (nodes is already sorted, so the row
    // order is deterministic and matches the returned vector's order).
    let mut index: HashMap<&str, usize> = HashMap::with_capacity(n);
    for (i, nd) in adjacency.nodes.iter().enumerate() {
        index.insert(nd.as_str(), i);
    }

    // Symmetric adjacency list. A directed edge (from, to, w) contributes w to
    // BOTH A[from][to] and A[to][from] (the "sum" symmetrization). Two
    // reciprocal directed edges therefore both contribute their conductance,
    // which is the doctrine-clean reading of "total traffic between these
    // nodes regardless of direction."
    let mut a: Vec<Vec<(usize, f64)>> = vec![Vec::new(); n];
    let mut degree: Vec<f64> = vec![0.0; n];
    for edge in &adjacency.edges {
        let (Some(&i), Some(&j)) = (index.get(edge.from.as_str()), index.get(edge.to.as_str()))
        else {
            continue;
        };
        a[i].push((j, edge.conductance));
        a[j].push((i, edge.conductance));
        degree[i] += edge.conductance;
        degree[j] += edge.conductance;
    }

    // u(0) = e_source.
    let mut u = vec![0.0f64; n];
    u[source_idx] = 1.0;

    // Taylor: u(t) = Σ_{p=0..k} term_p, where term_0 = u(0) and
    //   term_{p+1} = (-t / (p+1)) · L · term_p
    // Apply L sparsely: (L·v)_i = degree_i · v_i − Σ_{(j,w) ∈ a[i]} w · v_j.
    let mut total = u.clone();
    let mut term = u;
    let mut next = vec![0.0f64; n];
    for p in 1..=k {
        // next = L · term
        for (i, slot) in next.iter_mut().enumerate() {
            let mut neighbor_sum = 0.0;
            for &(j, w) in &a[i] {
                neighbor_sum += w * term[j];
            }
            *slot = degree[i] * term[i] - neighbor_sum;
        }
        // term ← (-t / p) · next ; total += term
        let factor = -t / p as f64;
        for (term_i, next_i) in term.iter_mut().zip(next.iter()) {
            *term_i = factor * next_i;
        }
        for (total_i, term_i) in total.iter_mut().zip(term.iter()) {
            *total_i += term_i;
        }
    }

    adjacency
        .nodes
        .iter()
        .zip(total.iter())
        .map(|(nd, &v)| (nd.clone(), v))
        .collect()
}

/// Felt-strength of a vibration emitted at `source` and sensed at `receiver`
/// after diffusion time `t`. Returns `None` if either node is absent from
/// `adjacency.nodes`. This is the scalar form of [`heat_propagate`].
pub fn heat_attenuation(
    adjacency: &FabricAdjacency,
    source: &str,
    receiver: &str,
    t: f64,
    k: usize,
) -> Option<f64> {
    let propagated = heat_propagate(adjacency, source, t, k);
    if propagated.is_empty() {
        return None;
    }
    propagated
        .into_iter()
        .find(|(nd, _)| nd == receiver)
        .map(|(_, v)| v)
}

/// Convenience: propagate from a source node identified by a fabric snapshot's
/// view, defaulting to [`DEFAULT_K`] Taylor terms.
pub fn heat_attenuation_default(
    snapshot: &FabricSnapshot,
    source: &str,
    receiver: &str,
    t: f64,
) -> Option<f64> {
    let adjacency = snapshot.adjacency();
    heat_attenuation(&adjacency, source, receiver, t, DEFAULT_K)
}

#[cfg(test)]
mod tests {
    use super::*;
    use spiderweb::{FabricEdge, FabricSnapshot};

    fn adj(nodes: &[&str], edges: &[(&str, &str, f64)]) -> FabricAdjacency {
        FabricAdjacency {
            nodes: nodes.iter().map(|s| s.to_string()).collect(),
            edges: edges
                .iter()
                .map(|(f, t, c)| FabricEdge {
                    from: f.to_string(),
                    to: t.to_string(),
                    conductance: *c,
                })
                .collect(),
        }
    }

    fn felt(prop: &[(NodeId, f64)], node: &str) -> f64 {
        prop.iter()
            .find(|(n, _)| n == node)
            .map(|(_, v)| *v)
            .unwrap()
    }

    /// At t=0, no diffusion: source carries all the mass, everyone else is 0.
    #[test]
    fn t_zero_is_source_dirac() {
        let a = adj(&["A", "B", "C"], &[("A", "B", 1.0), ("B", "C", 1.0)]);
        let prop = heat_propagate(&a, "A", 0.0, DEFAULT_K);
        assert_eq!(felt(&prop, "A"), 1.0);
        assert_eq!(felt(&prop, "B"), 0.0);
        assert_eq!(felt(&prop, "C"), 0.0);
    }

    /// Total mass is conserved at any t: the Laplacian's rows sum to zero, so
    /// exp(-tL) preserves the L1 sum. (The single most important physical
    /// invariant — if this breaks, nothing else means anything.)
    #[test]
    fn mass_conservation_on_a_path() {
        let a = adj(
            &["A", "B", "C", "D"],
            &[("A", "B", 1.0), ("B", "C", 1.0), ("C", "D", 1.0)],
        );
        for t in [0.05, 0.1, 0.2] {
            let prop = heat_propagate(&a, "A", t, DEFAULT_K);
            let total: f64 = prop.iter().map(|(_, v)| v).sum();
            assert!((total - 1.0).abs() < 1e-9, "t={t} total={total}");
        }
    }

    /// PROPERTY: mass conservation holds on RANDOM graphs, not just a path.
    /// For a symmetric Laplacian `L = D − A`, `L·1 = 0`, so `exp(−tL)` is
    /// doubly stochastic and any column sums to 1 — for ANY graph, connected or
    /// not. We hammer thousands of random topologies/conductances at small `t`
    /// (kept well inside the truncated-Taylor convergence radius with
    /// `DEFAULT_K`) and assert the source column always sums to 1. If this ever
    /// fails, the generator is producing physically meaningless attenuation.
    #[test]
    fn mass_conservation_property_on_random_graphs() {
        let mut s: u64 = 0x1234_5678_9ABC_DEF0;
        let mut rng = || {
            s ^= s >> 12;
            s ^= s << 25;
            s ^= s >> 27;
            s.wrapping_mul(0x2545F4914F6CDD1D)
        };
        let names = ["A", "B", "C", "D", "E", "F", "G", "H"];
        for _ in 0..3000 {
            let n = 2 + (rng() % 7) as usize; // 2..=8 nodes
            let nodes: Vec<&str> = names[..n].to_vec();
            let mut edges: Vec<(&str, &str, f64)> = Vec::new();
            for i in 0..n {
                for j in (i + 1)..n {
                    if rng() % 100 < 45 {
                        // conductance in [0.5, 2.0]
                        let c = 0.5 + (rng() % 1500) as f64 / 1000.0;
                        edges.push((names[i], names[j], c));
                    }
                }
            }
            let a = adj(&nodes, &edges);
            // small t keeps t*max_degree inside the K-term Taylor radius.
            for t in [0.005, 0.01, 0.02] {
                let prop = heat_propagate(&a, nodes[0], t, DEFAULT_K);
                let total: f64 = prop.iter().map(|(_, v)| v).sum();
                assert!(
                    (total - 1.0).abs() < 1e-6,
                    "mass not conserved: n={n} t={t} total={total} edges={edges:?}"
                );
                // every felt-strength is a valid probability mass in [0, 1].
                for (node, v) in &prop {
                    assert!(
                        *v >= -1e-9 && *v <= 1.0 + 1e-9,
                        "felt strength out of [0,1]: {node}={v}"
                    );
                }
            }
        }
    }

    /// Decay with GRAPH distance, not Euclidean. On a path A-B-C, after
    /// propagating from A: A > B > C in felt-strength. This is the win the
    /// painted `1/(1+d)` cannot make: hop count, not straight-line distance.
    #[test]
    fn felt_strength_decays_with_graph_distance() {
        let a = adj(&["A", "B", "C"], &[("A", "B", 1.0), ("B", "C", 1.0)]);
        // Small t so the truncation is comfortably accurate.
        let prop = heat_propagate(&a, "A", 0.1, DEFAULT_K);
        let (a_felt, b_felt, c_felt) = (felt(&prop, "A"), felt(&prop, "B"), felt(&prop, "C"));
        assert!(a_felt > b_felt, "A={a_felt} should exceed B={b_felt}");
        assert!(b_felt > c_felt, "B={b_felt} should exceed C={c_felt}");
        assert!(c_felt > 0.0, "C should feel something (path is connected)");
    }

    /// An isolated node — disconnected from the source — receives ZERO
    /// vibration, no matter how Euclidean-close it might be. The exact win the
    /// graph-aware kernel buys vs. Euclidean attenuation.
    #[test]
    fn disconnected_component_feels_nothing() {
        let a = adj(
            &["A", "B", "ghost"],
            &[("A", "B", 1.0)], // ghost is isolated
        );
        let prop = heat_propagate(&a, "A", 0.2, DEFAULT_K);
        assert_eq!(felt(&prop, "ghost"), 0.0, "isolated node must feel nothing");
        assert!(felt(&prop, "B") > 0.0, "B should feel A via the edge");
    }

    /// PARALLEL PATHS strengthen attenuation. In an A-B-C path vs. an A-B-C
    /// path PLUS an extra direct A-C edge (two paths to C), the doubled
    /// connectivity makes C feel A more strongly. This is the desire-path /
    /// effective-resistance intuition from the harvest, made concrete: thicker
    /// connectivity → stronger vibration.
    #[test]
    fn parallel_paths_strengthen_attenuation() {
        let single = adj(&["A", "B", "C"], &[("A", "B", 1.0), ("B", "C", 1.0)]);
        let double = adj(
            &["A", "B", "C"],
            &[("A", "B", 1.0), ("B", "C", 1.0), ("A", "C", 1.0)],
        );
        let t = 0.1;
        let c_single = felt(&heat_propagate(&single, "A", t, DEFAULT_K), "C");
        let c_double = felt(&heat_propagate(&double, "A", t, DEFAULT_K), "C");
        assert!(
            c_double > c_single,
            "C felt with parallel paths ({c_double}) should exceed C felt on a single path ({c_single})"
        );
    }

    /// HIGHER CONDUCTANCE on an edge transmits more vibration across it. A
    /// worn desire-path (high traffic count) should carry more felt-strength
    /// than a thin one — the direct payoff of using `edge_traffic` as the
    /// generator's weight.
    #[test]
    fn higher_conductance_transmits_more() {
        let thin = adj(&["A", "B"], &[("A", "B", 1.0)]);
        let worn = adj(&["A", "B"], &[("A", "B", 10.0)]);
        let t = 0.05;
        let b_thin = felt(&heat_propagate(&thin, "A", t, DEFAULT_K), "B");
        let b_worn = felt(&heat_propagate(&worn, "A", t, DEFAULT_K), "B");
        assert!(
            b_worn > b_thin,
            "worn ({b_worn}) should exceed thin ({b_thin})"
        );
    }

    /// Empty graph → empty result. An unknown source → empty result. Both
    /// are graceful, not panics.
    #[test]
    fn graceful_on_empty_or_missing_source() {
        let empty = FabricAdjacency::default();
        assert!(heat_propagate(&empty, "anything", 0.1, DEFAULT_K).is_empty());

        let a = adj(&["A"], &[]);
        assert!(heat_propagate(&a, "missing", 0.1, DEFAULT_K).is_empty());
    }

    /// `heat_attenuation` (scalar form) agrees with `heat_propagate` (vector
    /// form), and returns `None` on unknown nodes.
    #[test]
    fn scalar_form_agrees_with_vector_form() {
        let a = adj(&["A", "B", "C"], &[("A", "B", 1.0), ("B", "C", 1.0)]);
        let prop = heat_propagate(&a, "A", 0.1, DEFAULT_K);
        for (node, expected) in &prop {
            let scalar = heat_attenuation(&a, "A", node, 0.1, DEFAULT_K).unwrap();
            assert!(
                (scalar - expected).abs() < 1e-12,
                "{node}: {scalar} vs {expected}"
            );
        }
        assert_eq!(heat_attenuation(&a, "A", "ghost", 0.1, DEFAULT_K), None);
        assert_eq!(heat_attenuation(&a, "ghost", "A", 0.1, DEFAULT_K), None);
    }

    /// Driving the convenience overload through a real `FabricSnapshot` —
    /// the form a caller in the bus actually uses.
    #[test]
    fn convenience_through_snapshot() {
        let snap = FabricSnapshot {
            edge_traffic: vec![
                ("A".to_string(), "B".to_string(), 3),
                ("B".to_string(), "C".to_string(), 1),
            ],
            ..Default::default()
        };
        let b = heat_attenuation_default(&snap, "A", "B", 0.1).unwrap();
        let c = heat_attenuation_default(&snap, "A", "C", 0.1).unwrap();
        assert!(
            b > c,
            "B (1 hop, conductance 3) should beat C (2 hops, conductance 1)"
        );
        assert!(c > 0.0, "C should still feel something through the chain");
    }
}
