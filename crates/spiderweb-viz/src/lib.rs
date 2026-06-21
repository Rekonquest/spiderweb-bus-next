//! `spiderweb-viz` — export a [`FabricSnapshot`] as a 3D scene.
//!
//! The geometric fabric made viewable. [`scene_json`] turns a snapshot
//! into a self-contained JSON scene a 3D viewer (three.js / Babylon, or a
//! Blender import) can load: nodes are points, threads are 3D polylines
//! through positioned nodes, intersections are labeled points. Std-only
//! (hand-rolled JSON, no serde) so it stays dependency-free; the kernel
//! is untouched.
//!
//! Honest note: positions are a *logical* overlay (every node is in the
//! same RAM). The scene shows where nodes *sit* and how threads route
//! through layered space — visualization and organization, not timing.

#![forbid(unsafe_code)]

use spiderweb::FabricSnapshot;

/// Render the geometric fabric as a JSON scene:
/// `{ "nodes":[{name,x,y,z}], "threads":[{id,polyline:[[x,y,z],...]}],
///    "intersections":[{node,x,y,z,threads:[...]}] }`.
///
/// A thread's polyline maps its `direction` through node positions;
/// nodes without a position are skipped (never invented), so the geometry
/// is always real.
pub fn scene_json(snap: &FabricSnapshot) -> String {
    let nodes: Vec<String> = snap
        .positions
        .iter()
        .filter(|(_, p)| finite(p))
        .map(|(n, p)| {
            format!(
                "{{\"name\":{},\"x\":{},\"y\":{},\"z\":{}}}",
                json_str(n),
                p.x,
                p.y,
                p.z
            )
        })
        .collect();

    let threads: Vec<String> = snap
        .threads
        .iter()
        .map(|t| {
            let pts: Vec<String> = t
                .direction
                .iter()
                .filter_map(|n| snap.position(n))
                .filter(finite)
                .map(|p| format!("[{},{},{}]", p.x, p.y, p.z))
                .collect();
            format!("{{\"id\":{},\"polyline\":[{}]}}", t.id, pts.join(","))
        })
        .collect();

    let intersections: Vec<String> = snap
        .intersections
        .iter()
        .filter_map(|i| {
            snap.position(&i.node).filter(finite).map(|p| {
                let tids: Vec<String> = i.threads.iter().map(|t| t.to_string()).collect();
                format!(
                    "{{\"node\":{},\"x\":{},\"y\":{},\"z\":{},\"threads\":[{}]}}",
                    json_str(&i.node),
                    p.x,
                    p.y,
                    p.z,
                    tids.join(",")
                )
            })
        })
        .collect();

    format!(
        "{{\"nodes\":[{}],\"threads\":[{}],\"intersections\":[{}]}}",
        nodes.join(","),
        threads.join(","),
        intersections.join(",")
    )
}

/// A position is renderable only if every coordinate is finite — a
/// non-finite one would emit `NaN`/`inf`, which is not valid JSON. We
/// skip it (like an unplaced node) rather than corrupt the whole scene.
fn finite(p: &spiderweb::Position) -> bool {
    p.x.is_finite() && p.y.is_finite() && p.z.is_finite()
}

/// Minimal JSON string escaping (the node names are simple, but be safe).
fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use spiderweb::{Bus, FabricSnapshot, Position};

    #[test]
    fn scene_has_nodes_and_is_valid_shape() {
        let mut bus = Bus::open();
        bus.place("a", Position::new(0.0, 0.0, 0.0));
        bus.place("b", Position::new(3.0, 4.0, 0.0));
        let snap = bus.fabric_snapshot();
        let json = scene_json(&snap);
        assert!(json.contains("\"nodes\":["));
        assert!(json.contains("\"name\":\"a\""));
        assert!(json.contains("\"name\":\"b\""));
        assert!(json.starts_with('{') && json.ends_with('}'));
    }

    /// An empty fabric exports a fixed, valid empty scene.
    #[test]
    fn empty_snapshot_is_valid_json() {
        let json = scene_json(&Bus::open().fabric_snapshot());
        assert_eq!(json, "{\"nodes\":[],\"threads\":[],\"intersections\":[]}");
    }

    /// A non-finite coordinate (in a hand-built snapshot) is skipped, never
    /// emitted as the invalid JSON tokens `NaN`/`inf`.
    #[test]
    fn non_finite_position_is_skipped_not_emitted() {
        let snap = FabricSnapshot {
            positions: vec![
                ("ok".to_string(), Position::new(1.0, 2.0, 3.0)),
                ("bad".to_string(), Position::new(f64::NAN, f64::INFINITY, 0.0)),
            ],
            ..Default::default()
        };
        let json = scene_json(&snap);
        assert!(json.contains("\"name\":\"ok\""), "finite node kept");
        assert!(!json.contains("bad"), "non-finite node dropped");
        assert!(!json.contains("NaN") && !json.contains("inf"), "no invalid JSON tokens");
    }
}
