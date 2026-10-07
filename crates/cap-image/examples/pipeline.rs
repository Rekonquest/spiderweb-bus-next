//! Image processing demo: source → resize → sink.
//!
//! Run with: `cargo run -p cap-image --example pipeline`
//!
//! What happens:
//! 1. We generate three test PNGs of varying sizes (200x150, 400x300, 800x600).
//! 2. Register a source strand that emits a request per image.
//! 3. Register a resize strand that does the actual work.
//! 4. Register two sink strands — both save the result to disk, but
//!    to different filenames, proving fan-out: one resize produces
//!    two outputs.
//! 5. Watch the bus route the bytes from one publisher to one
//!    processor to two consumers.

use cap_image::{CapImage, CapImageSink, CapImageSource, ImageRequest, ImageResult};
use image::{ImageBuffer, Rgb, RgbImage};
use spider::Spider;
use spiderweb::Bus;
use std::path::PathBuf;
use std::time::Duration;

fn main() {
    // Register the custom payload types so the kernel knows how to
    // clone them for fan-out.
    spiderweb::register_clone_type::<ImageRequest>();
    spiderweb::register_clone_type::<ImageResult>();

    // Generate test images.
    let workdir = std::env::temp_dir().join("spiderweb-image-demo");
    std::fs::create_dir_all(&workdir).expect("create workdir");
    let source1 = workdir.join("source_400x300.png");
    let source2 = workdir.join("source_800x600.png");
    let source3 = workdir.join("source_1600x1200.png");
    write_test_image(&source1, 400, 300, [255, 100, 100]);
    write_test_image(&source2, 800, 600, [100, 255, 100]);
    write_test_image(&source3, 1600, 1200, [100, 100, 255]);
    println!("Wrote test images to {}", workdir.display());

    let requests = vec![
        ImageRequest::new(
            source1.to_str().unwrap(),
            100,
            75,
            workdir.join("out_thumb_A_100x75.png").to_str().unwrap(),
        ),
        ImageRequest::new(
            source2.to_str().unwrap(),
            200,
            150,
            workdir.join("out_thumb_B_200x150.png").to_str().unwrap(),
        ),
        ImageRequest::new(
            source3.to_str().unwrap(),
            400,
            300,
            workdir.join("out_thumb_C_400x300.png").to_str().unwrap(),
        ),
    ];

    let mut bus = Bus::open();
    // Source generates 3 requests then detaches.
    bus.register(CapImageSource::new(requests));
    // Resize worker.
    bus.register(CapImage::new());
    // Two sinks, both receiving every ImageResult but writing to
    // different files. This is real fan-out: the kernel clones the
    // ImageResult and delivers to both. Each sink rewrites the
    // output path with its own suffix, so you get six files on
    // disk (3 inputs × 2 sinks) from three publishes.
    bus.register(CapImageSink::with_suffix("-A"));
    bus.register(CapImageSink::with_suffix("-B"));
    // Spider watches sys.lifecycle.
    let spider_spec = spiderweb::StrandSpec::new("spider", || Box::new(Spider::new()));
    bus.register_spider(spider_spec);
    bus.run_until(Some(Duration::from_secs(8)));

    println!("\nFinal workdir: {}", workdir.display());
    if let Ok(entries) = std::fs::read_dir(&workdir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            println!(
                "  {} ({} bytes)",
                path.file_name().unwrap().to_string_lossy(),
                size
            );
        }
    }
}

fn write_test_image(path: &PathBuf, w: u32, h: u32, color: [u8; 3]) {
    let mut img: RgbImage = ImageBuffer::new(w, h);
    for (x, y, pixel) in img.enumerate_pixels_mut() {
        let r = ((x as f32 / w as f32) * 255.0) as u8;
        let g = ((y as f32 / h as f32) * 255.0) as u8;
        let b = ((x ^ y) & 0xFF) as u8;
        let _ = color; // keep param to make this generic later
        *pixel = Rgb([r, g, b]);
    }
    img.save(path).expect("save test image");
}
