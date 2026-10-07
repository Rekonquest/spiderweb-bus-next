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

use spiderweb::{BusStats, FabricSnapshot};

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

/// A self-contained, interactive HTML page that renders the fabric in 3D
/// — **drag to rotate, scroll to zoom, right-drag to pan**. Nodes are
/// spheres colored by layer (z), threads are 3D polylines, intersections
/// glow. The [`scene_json`] is embedded inline, so the file needs no
/// server — open it in any browser (it loads three.js from a CDN).
///
/// Honest, like the scene: the coordinates are a logical layout (z =
/// elevation: ground/highway/orchestration), not latency.
pub fn scene_html(snap: &FabricSnapshot) -> String {
    VIEWER_TEMPLATE.replace("__SCENE__", &scene_json(snap))
}

/// The viewer page. `__SCENE__` is replaced with the embedded scene JSON.
/// A raw string so the HTML/CSS/JS braces need no escaping.
const VIEWER_TEMPLATE: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Spiderweb Bus — the weave in 3D</title>
<style>
  html,body { margin:0; height:100%; overflow:hidden; background:#0a0a12;
    color:#cdd; font:13px/1.45 system-ui,Segoe UI,sans-serif; }
  #info { position:fixed; top:12px; left:14px; z-index:10; max-width:360px;
    pointer-events:none; text-shadow:0 1px 3px #000; }
  #info h1 { font-size:15px; margin:0 0 4px; color:#9ff; letter-spacing:.3px; }
  #info .hint { color:#8ab; }
  #info .note { color:#d9a; margin-top:6px; font-style:italic; }
  #legend { position:fixed; bottom:12px; left:14px; z-index:10;
    text-shadow:0 1px 3px #000; }
  #legend span { display:inline-block; margin-right:12px; }
  #legend i { display:inline-block; width:11px; height:11px; border-radius:50%;
    margin-right:5px; vertical-align:-1px; }
</style>
</head>
<body>
<div id="info">
  <h1>Spiderweb Bus — the weave in 3D</h1>
  <div class="hint">drag to rotate &middot; scroll to zoom &middot; right-drag to pan</div>
  <div class="note">logical coordinates — z is elevation (ground / highway / orchestration); distance is for layout &amp; policy, not latency.</div>
</div>
<div id="legend"></div>
<script type="importmap">
{ "imports": {
  "three": "https://unpkg.com/three@0.160.0/build/three.module.js",
  "three/addons/": "https://unpkg.com/three@0.160.0/examples/jsm/"
}}
</script>
<script type="module">
import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';

const SCENE = __SCENE__;

const renderer = new THREE.WebGLRenderer({ antialias: true });
renderer.setPixelRatio(window.devicePixelRatio);
renderer.setSize(window.innerWidth, window.innerHeight);
document.body.appendChild(renderer.domElement);

const scene = new THREE.Scene();
scene.background = new THREE.Color(0x0a0a12);

const camera = new THREE.PerspectiveCamera(55, window.innerWidth/window.innerHeight, 0.01, 5000);
camera.up.set(0, 0, 1); // z is "up" (elevation)

const controls = new OrbitControls(camera, renderer.domElement);
controls.enableDamping = true;

scene.add(new THREE.AmbientLight(0xffffff, 0.65));
const key = new THREE.DirectionalLight(0xffffff, 0.9);
key.position.set(5, 8, 12);
scene.add(key);

// grid on the XY plane (z=0), and axes for orientation
const grid = new THREE.GridHelper(40, 40, 0x224, 0x183);
grid.rotation.x = Math.PI / 2;
scene.add(grid);
scene.add(new THREE.AxesHelper(3));

const byName = new Map();
const positions = [];
for (const n of SCENE.nodes) { byName.set(n.name, n); positions.push(n); }

function colorForZ(z) {
  return new THREE.Color().setHSL(((0.34 + 0.13 * z) % 1 + 1) % 1, 0.7, 0.6);
}

function makeLabel(text, color) {
  const pad = 8, font = 28;
  const c = document.createElement('canvas');
  const ctx = c.getContext('2d');
  ctx.font = `${font}px system-ui, sans-serif`;
  c.width = Math.ceil(ctx.measureText(text).width) + pad * 2;
  c.height = font + pad * 2;
  ctx.font = `${font}px system-ui, sans-serif`;
  ctx.fillStyle = '#' + color.getHexString();
  ctx.textBaseline = 'middle';
  ctx.fillText(text, pad, c.height / 2);
  const tex = new THREE.CanvasTexture(c);
  tex.minFilter = THREE.LinearFilter;
  const spr = new THREE.Sprite(new THREE.SpriteMaterial({ map: tex, transparent: true, depthTest: false }));
  const s = 0.012;
  spr.scale.set(c.width * s, c.height * s, 1);
  return spr;
}

// nodes
const intersectionNames = new Set(SCENE.intersections.map(i => i.node));
for (const n of positions) {
  const col = colorForZ(n.z);
  const isX = intersectionNames.has(n.name);
  const r = isX ? 0.26 : 0.17;
  const mat = new THREE.MeshStandardMaterial({
    color: col, emissive: isX ? 0xff7a1a : 0x000000,
    emissiveIntensity: isX ? 0.6 : 0.0, roughness: 0.45, metalness: 0.1,
  });
  const mesh = new THREE.Mesh(new THREE.SphereGeometry(r, 24, 16), mat);
  mesh.position.set(n.x, n.y, n.z);
  scene.add(mesh);
  const label = makeLabel(n.name, col);
  label.position.set(n.x, n.y, n.z + r + 0.18);
  scene.add(label);
}

// threads as polylines
for (const t of SCENE.threads) {
  if (!t.polyline || t.polyline.length < 2) continue;
  const pts = t.polyline.map(p => new THREE.Vector3(p[0], p[1], p[2]));
  const geo = new THREE.BufferGeometry().setFromPoints(pts);
  const line = new THREE.Line(geo, new THREE.LineBasicMaterial({ color: 0x66ccff, transparent: true, opacity: 0.55 }));
  scene.add(line);
}

// fit camera to the node cloud
(function fit() {
  if (!positions.length) { camera.position.set(6, -8, 6); controls.target.set(0,0,1); controls.update(); return; }
  const box = new THREE.Box3();
  for (const n of positions) box.expandByPoint(new THREE.Vector3(n.x, n.y, n.z));
  const center = box.getCenter(new THREE.Vector3());
  const radius = Math.max(box.getSize(new THREE.Vector3()).length() * 0.6, 2);
  controls.target.copy(center);
  camera.position.set(center.x + radius * 1.6, center.y - radius * 2.0, center.z + radius * 1.4);
  controls.update();
})();

// legend by layer
const legend = document.getElementById('legend');
const zs = [...new Set(positions.map(n => Math.round(n.z)))].sort((a,b)=>a-b);
const layerName = z => (z===0?'ground':z===1?'highway':z===2?'orchestration':'layer '+z);
legend.innerHTML = zs.map(z => {
  const c = colorForZ(z);
  return `<span><i style="background:#${c.getHexString()}"></i>z=${z} ${layerName(z)}</span>`;
}).join('') + ` <span><i style="background:#ff7a1a"></i>intersection</span>`;

addEventListener('resize', () => {
  camera.aspect = window.innerWidth / window.innerHeight;
  camera.updateProjectionMatrix();
  renderer.setSize(window.innerWidth, window.innerHeight);
});

(function loop() {
  requestAnimationFrame(loop);
  controls.update();
  renderer.render(scene, camera);
})();
</script>
</body>
</html>
"#;

/// Render a [`BusStats`] as a compact terminal "top" dashboard: web-wide
/// counters, the lifecycle tally, and a per-strand throughput table. Pure
/// formatting — the caller pulls the stats (`Bus::stats()` / `BusHandle::stats()`)
/// and prints the result, optionally clearing the screen between frames for a
/// live `webctl top` view. Std-only, no dependencies.
pub fn render_top(stats: &BusStats) -> String {
    use std::fmt::Write;
    const RULE: &str = "────────────────────────────────────────────────────────";
    let mut s = String::new();
    let _ = writeln!(
        s,
        "🕸️  spiderweb top — {} strands · {} subscribers · {} nodes / {} edges",
        stats.active_strands, stats.subscribers, stats.nodes, stats.edges
    );
    let _ = writeln!(s, "{RULE}");
    let _ = writeln!(
        s,
        "  delivered  {:>10}      buffered     {:>10}",
        stats.delivered, stats.buffered
    );
    let _ = writeln!(
        s,
        "  vibrations {:>10}      flushed      {:>10}",
        stats.vibrations, stats.flushed
    );
    let _ = writeln!(
        s,
        "  evictions  {:>10}      edge traffic {:>10}",
        stats.pending_evictions, stats.edge_traffic_total
    );
    let _ = writeln!(
        s,
        "  threads    {:>10}      intersections{:>10}",
        stats.threads, stats.intersections
    );
    let _ = writeln!(s, "{RULE}");
    let _ = writeln!(
        s,
        "  lifecycle: reg {} · restart {} · crash {} · detach {} · quarantine {}",
        stats.registered, stats.restarted, stats.crashed, stats.detached, stats.quarantined
    );
    let _ = writeln!(s, "{RULE}");
    let _ = writeln!(s, "  {:<32}{:>12}", "STRAND", "PROCESSED");
    if stats.per_strand.is_empty() {
        let _ = writeln!(s, "  (no strands)");
    }
    for st in &stats.per_strand {
        let _ = writeln!(s, "  {:<32}{:>12}", st.name, st.processed);
    }
    s
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

    /// The interactive viewer embeds the scene and the three.js controls,
    /// with the placeholder fully replaced.
    #[test]
    fn scene_html_embeds_scene_and_viewer() {
        let mut bus = Bus::open();
        bus.place("a", Position::new(0.0, 0.0, 0.0));
        let html = scene_html(&bus.fabric_snapshot());
        assert!(html.starts_with("<!doctype html>"));
        assert!(html.contains("OrbitControls"), "rotate/zoom/pan controls");
        assert!(html.contains("\"name\":\"a\""), "scene embedded inline");
        assert!(!html.contains("__SCENE__"), "placeholder replaced");
    }

    /// A non-finite coordinate (in a hand-built snapshot) is skipped, never
    /// emitted as the invalid JSON tokens `NaN`/`inf`.
    #[test]
    fn non_finite_position_is_skipped_not_emitted() {
        let snap = FabricSnapshot {
            positions: vec![
                ("ok".to_string(), Position::new(1.0, 2.0, 3.0)),
                (
                    "bad".to_string(),
                    Position::new(f64::NAN, f64::INFINITY, 0.0),
                ),
            ],
            ..Default::default()
        };
        let json = scene_json(&snap);
        assert!(json.contains("\"name\":\"ok\""), "finite node kept");
        assert!(!json.contains("bad"), "non-finite node dropped");
        assert!(
            !json.contains("NaN") && !json.contains("inf"),
            "no invalid JSON tokens"
        );
    }

    /// The `top` dashboard renders the header, the counters, and a per-strand
    /// throughput row for each strand.
    #[test]
    fn render_top_shows_counters_and_per_strand() {
        let stats = BusStats {
            delivered: 1234,
            vibrations: 7,
            crashed: 2,
            restarted: 1,
            active_strands: 2,
            subscribers: 3,
            nodes: 2,
            edges: 1,
            threads: 4,
            per_strand: vec![
                spiderweb::StrandStats {
                    name: "sink".into(),
                    processed: 100,
                },
                spiderweb::StrandStats {
                    name: "source".into(),
                    processed: 42,
                },
            ],
            ..Default::default()
        };
        let out = render_top(&stats);
        assert!(out.contains("spiderweb top"), "has a header");
        assert!(out.contains("1234"), "shows delivered count");
        assert!(out.contains("crash 2"), "shows lifecycle crash count");
        assert!(
            out.contains("sink") && out.contains("100"),
            "shows a per-strand row"
        );
        assert!(
            out.contains("source") && out.contains("42"),
            "shows the other strand"
        );
    }

    /// With no strands the dashboard still renders (no panic, shows a marker).
    #[test]
    fn render_top_handles_empty() {
        let out = render_top(&BusStats::default());
        assert!(out.contains("spiderweb top"));
        assert!(out.contains("(no strands)"));
    }
}
