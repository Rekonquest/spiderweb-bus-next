//! `cap-image` — image-processing strands.
//!
//! Three strands live here:
//!
//! - [`CapImageSource`] — emits `ImageRequest`s one at a time.
//! - [`CapImage`] — subscribes to `ImageRequest`, loads + resizes,
//!   publishes `ImageResult`.
//! - [`CapImageSink`] — subscribes to `ImageResult`, saves to disk.
//!   Each instance can be tagged to rewrite the output path, which
//!   is how fan-out is demonstrated: two tagged sinks both receive
//!   the same `ImageResult` and save it to different files.
//!
//! v1.2 limitation: the kernel can fan out `Clone` types. Image pixels
//! are encoded as a base64 `String` (which is `Clone`) so the kernel
//! can route them. v1.3 will support transparent Arc<T> payloads so
//! we can ship raw bytes.
//!
//! Together they prove the bus can carry real bytes: a 30-100KB
//! base64 image string travels in an envelope, gets cloned per
//! subscriber, and is decoded by the sink strand.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use image::{imageops::FilterType, GenericImageView, ImageReader};
use spiderweb::{BusHandle, Socket, Strand, StrandError};
use std::time::Duration;

// -----------------------------------------------------------------------------
// Message types
// -----------------------------------------------------------------------------

/// Request: a compact string encoding of the resize request. The
/// format is `source|target_w|target_h|output`. v1.2 limitation:
/// the kernel can only fan out `Clone` types (String, primitives).
/// For richer payloads, use a v1.3+ with transparent Arc support
/// or encode the payload as a String.
#[derive(Debug, Clone)]
pub struct ImageRequest(pub String);

impl ImageRequest {
    pub fn new(source: &str, target_w: u32, target_h: u32, output: &str) -> Self {
        ImageRequest(format!("{}|{}|{}|{}", source, target_w, target_h, output))
    }

    pub fn parse(&self) -> Option<(String, u32, u32, String)> {
        let parts: Vec<&str> = self.0.split('|').collect();
        if parts.len() != 4 {
            return None;
        }
        let w = parts[1].parse().ok()?;
        let h = parts[2].parse().ok()?;
        Some((parts[0].to_string(), w, h, parts[3].to_string()))
    }
}

impl std::fmt::Display for ImageRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Result: the resized RGBA8 pixels (base64-encoded) + the path they
/// should be saved to. We encode as base64 so the bus can clone cheaply.
/// Note: this is a Clone type, but the kernel doesn't know about it
/// by default. The demo calls `spiderweb::register_clone_type::<ImageResult>()`
/// at boot.
#[derive(Debug, Clone)]
pub struct ImageResult {
    pub output_path: String,
    pub width: u32,
    pub height: u32,
    /// Base64-encoded RGBA8 bytes.
    pub rgba_b64: String,
}

// -----------------------------------------------------------------------------
// CapImageSource — generates requests
// -----------------------------------------------------------------------------

pub struct CapImageSource {
    /// Queue of requests to emit, in order. Once exhausted, the strand
    /// detaches. This is the "batch mode" pattern.
    requests: Vec<ImageRequest>,
    index: usize,
}

impl CapImageSource {
    pub fn new(requests: Vec<ImageRequest>) -> Self {
        CapImageSource { requests, index: 0 }
    }
}

impl Strand for CapImageSource {
    fn name(&self) -> &str {
        "cap-image-source"
    }

    fn inputs(&self) -> &[Socket] {
        &[]
    }

    fn outputs(&self) -> &[Socket] {
        const SOCK: Socket = Socket::new::<ImageRequest>("in.image.request");
        std::slice::from_ref(&SOCK)
    }

    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        if self.index >= self.requests.len() {
            bus.log("queue exhausted, detaching");
            return Err(StrandError::Detach);
        }
        let req = self.requests[self.index].clone();
        self.index += 1;
        bus.log(&format!(
            "→ request #{}/{}: {}",
            self.index,
            self.requests.len(),
            req.0
        ));
        bus.publish("in.image.request", req)?;
        bus.sleep(Duration::from_millis(200));
        Ok(())
    }
}

// -----------------------------------------------------------------------------
// CapImage — does the resize
// -----------------------------------------------------------------------------

pub struct CapImage {
    tag: String,
    processed: u32,
}

impl CapImage {
    pub fn new() -> Self {
        CapImage {
            tag: "cap-image".to_string(),
            processed: 0,
        }
    }

    pub fn with_tag(tag: impl Into<String>) -> Self {
        CapImage {
            tag: tag.into(),
            processed: 0,
        }
    }
}

impl Default for CapImage {
    fn default() -> Self {
        Self::new()
    }
}

impl Strand for CapImage {
    fn name(&self) -> &str {
        &self.tag
    }

    fn inputs(&self) -> &[Socket] {
        const SOCK: Socket = Socket::new::<ImageRequest>("in.image.request");
        std::slice::from_ref(&SOCK)
    }

    fn outputs(&self) -> &[Socket] {
        const SOCK: Socket = Socket::new::<ImageResult>("out.image.result");
        std::slice::from_ref(&SOCK)
    }

    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        let reqs = bus.recv_all::<ImageRequest>()?;
        for (_sock, req) in reqs {
            let parsed = match req.parse() {
                Some(p) => p,
                None => {
                    bus.log(&format!("✗ malformed request: {}", req));
                    continue;
                }
            };
            let (source, target_w, target_h, output) = parsed;
            let req_struct = ImageRequestStruct {
                source_path: source,
                target_width: target_w,
                target_height: target_h,
                output_path: output,
            };
            match process_request(&req_struct) {
                Ok(result) => {
                    self.processed += 1;
                    let b64_len = result.rgba_b64.len();
                    bus.log(&format!(
                        "↻ resized → {}x{} (b64 {} chars, request #{})",
                        result.width, result.height, b64_len, self.processed
                    ));
                    bus.publish("out.image.result", result)?;
                }
                Err(e) => {
                    bus.log(&format!("✗ failed to process: {}", e));
                }
            }
        }
        bus.sleep(Duration::from_millis(20));
        Ok(())
    }
}

struct ImageRequestStruct {
    source_path: String,
    target_width: u32,
    target_height: u32,
    output_path: String,
}

fn process_request(req: &ImageRequestStruct) -> Result<ImageResult, String> {
    let img = ImageReader::open(&req.source_path)
        .map_err(|e| format!("open: {}", e))?
        .with_guessed_format()
        .map_err(|e| format!("format: {}", e))?
        .decode()
        .map_err(|e| format!("decode: {}", e))?;

    let resized = img.resize_exact(req.target_width, req.target_height, FilterType::Triangle);
    let rgba = resized.to_rgba8().into_raw();
    let (w, h) = resized.dimensions();

    Ok(ImageResult {
        output_path: req.output_path.clone(),
        width: w,
        height: h,
        rgba_b64: STANDARD.encode(&rgba),
    })
}

// -----------------------------------------------------------------------------
// CapImageSink — saves the result
// -----------------------------------------------------------------------------

pub struct CapImageSink {
    /// Strand name. Stored as a `String` so each instance can
    /// have a distinct name (e.g. `cap-image-sink-A` vs
    /// `cap-image-sink-B`). Required for v1.2: the kernel
    /// rejects name collisions, and two sinks with the same
    /// socket subscriptions must have different names to
    /// demonstrate fan-out.
    name: String,
    /// Optional suffix inserted into the output path before the
    /// extension. `None` means save to the path as-is. `Some("-A")`
    /// turns `foo.png` into `foo-A.png`. This is how we make
    /// fan-out observable in the file system: two sinks with
    /// different suffixes write to different files from the same
    /// `ImageResult`.
    suffix: Option<String>,
    saved: u32,
}

impl CapImageSink {
    pub fn new() -> Self {
        CapImageSink {
            name: "cap-image-sink".to_string(),
            suffix: None,
            saved: 0,
        }
    }

    /// Tag the sink so it rewrites the output path with `suffix`
    /// before the file extension. The suffix is also used to
    /// derive a unique strand name (`cap-image-sink-A`,
    /// `cap-image-sink-B`) so two sinks can coexist on the bus.
    pub fn with_suffix(suffix: impl Into<String>) -> Self {
        let suffix = suffix.into();
        let name = format!("cap-image-sink{}", suffix);
        CapImageSink {
            name,
            suffix: Some(suffix),
            saved: 0,
        }
    }
}

impl Default for CapImageSink {
    fn default() -> Self {
        Self::new()
    }
}

impl Strand for CapImageSink {
    fn name(&self) -> &str {
        &self.name
    }

    fn inputs(&self) -> &[Socket] {
        const SOCK: Socket = Socket::new::<ImageResult>("out.image.result");
        std::slice::from_ref(&SOCK)
    }

    fn outputs(&self) -> &[Socket] {
        &[]
    }

    fn run(&mut self, bus: &mut BusHandle) -> Result<(), StrandError> {
        let results = bus.recv_all::<ImageResult>()?;
        for (_sock, result) in results {
            let path = match &self.suffix {
                Some(suffix) => rewrite_path(&result.output_path, suffix),
                None => result.output_path.clone(),
            };
            match save_to(&path, &result) {
                Ok(()) => {
                    self.saved += 1;
                    bus.log(&format!(
                        "✓ saved {} ({}x{}, total saved: {})",
                        path, result.width, result.height, self.saved
                    ));
                }
                Err(e) => {
                    bus.log(&format!("✗ failed to save {}: {}", path, e));
                }
            }
        }
        bus.sleep(Duration::from_millis(20));
        Ok(())
    }
}

fn rewrite_path(path: &str, suffix: &str) -> String {
    // Insert `suffix` before the final extension. `foo.png` →
    // `foo-A.png`. `foo` (no extension) → `foo-A`. This is good
    // enough for demo filenames, which always have an extension
    // and a stem. We don't handle multi-dot or hidden files
    // because the demo filenames are simple.
    match path.rfind('.') {
        Some(i) if i > 0 => {
            let (stem, ext) = path.split_at(i);
            format!("{}{}{}", stem, suffix, ext)
        }
        _ => format!("{}{}", path, suffix),
    }
}

fn save_to(path: &str, result: &ImageResult) -> Result<(), String> {
    let bytes = STANDARD
        .decode(&result.rgba_b64)
        .map_err(|e| format!("base64 decode: {}", e))?;
    let dyn_img =
        image::ImageBuffer::<image::Rgba<u8>, _>::from_raw(result.width, result.height, bytes)
            .ok_or_else(|| "from_raw failed".to_string())?;
    dyn_img.save(path).map_err(|e| format!("save: {}", e))
}
