//! Distributed path tracer: the master side.
//!
//! Splits the image into tiles, ships each tile as a task to browser workers
//! (the scene travels as JSON inside the task input), and assembles the
//! returned RGBA tiles into a PNG. This crate carries its own WASM module,
//! so it needs no changes to the framework or to the examples crate:
//! `ModuleSource::CompileCrate` builds and ships this crate's own lib.

use anyhow::Result;
use distribute_runtime::{
    run_distributed_mapreduce_bytes_opts, JobOptions, MissingChunkPolicy, ModuleSource,
};
use examples_raytrace::{sample_scene, Scene, TileJob};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Instant;

const TILE: u32 = 80;

#[derive(Clone, Serialize, Deserialize)]
struct RenderJob {
    width: u32,
    height: u32,
    samples: u32,
    max_depth: u32,
    tile: Option<(u32, u32, u32, u32)>, // x, y, w, h
    scene: Scene,
}

#[derive(Clone, Serialize, Deserialize)]
struct Tile {
    x: u32,
    y: u32,
    w: u32,
    h: u32,
    rgba: Vec<u8>,
}

fn chunker(job: &RenderJob) -> Vec<RenderJob> {
    let mut tiles = Vec::new();
    let mut y = 0;
    while y < job.height {
        let th = TILE.min(job.height - y);
        let mut x = 0;
        while x < job.width {
            let tw = TILE.min(job.width - x);
            let mut t = job.clone();
            t.tile = Some((x, y, tw, th));
            tiles.push(t);
            x += tw;
        }
        y += th;
    }
    tiles
}

fn encode_chunk(chunk: &RenderJob) -> (Vec<u8>, serde_json::Value) {
    let (x, y, w, h) = chunk.tile.expect("chunker sets the tile rect");
    let tile_job = TileJob {
        width: chunk.width,
        height: chunk.height,
        samples: chunk.samples,
        max_depth: chunk.max_depth,
        tile_x: x,
        tile_y: y,
        tile_w: w,
        tile_h: h,
        scene: chunk.scene.clone(),
    };
    let bytes = serde_json::to_vec(&tile_job).expect("tile job serializes");
    (bytes, json!({ "x": x, "y": y, "w": w, "h": h }))
}

fn decode_result(bytes: Vec<u8>, meta: serde_json::Value) -> Tile {
    Tile {
        x: meta["x"].as_u64().unwrap_or(0) as u32,
        y: meta["y"].as_u64().unwrap_or(0) as u32,
        w: meta["w"].as_u64().unwrap_or(0) as u32,
        h: meta["h"].as_u64().unwrap_or(0) as u32,
        rgba: bytes,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let width: u32 = args.next().and_then(|s| s.parse().ok()).unwrap_or(640);
    let height: u32 = args.next().and_then(|s| s.parse().ok()).unwrap_or(360);
    let samples: u32 = args.next().and_then(|s| s.parse().ok()).unwrap_or(24);
    let out_path = args.next().unwrap_or_else(|| "raytrace_out.png".to_string());

    let job = RenderJob {
        width,
        height,
        samples,
        max_depth: 8,
        tile: None,
        scene: sample_scene(42),
    };
    println!(
        "🎨 Rendering {}x{} at {} samples/pixel across the hive...",
        width, height, samples
    );

    let out_for_reducer = out_path.clone();
    let reducer = move |tiles: Vec<Tile>| -> String {
        let mut img = image::RgbaImage::new(width, height);
        for tile in &tiles {
            for row in 0..tile.h {
                for col in 0..tile.w {
                    let src = ((row * tile.w + col) * 4) as usize;
                    if src + 3 < tile.rgba.len() {
                        img.put_pixel(
                            tile.x + col,
                            tile.y + row,
                            image::Rgba([
                                tile.rgba[src],
                                tile.rgba[src + 1],
                                tile.rgba[src + 2],
                                tile.rgba[src + 3],
                            ]),
                        );
                    }
                }
            }
        }
        if let Err(e) = img.save(&out_for_reducer) {
            eprintln!("Failed to save {}: {}", out_for_reducer, e);
        }
        out_for_reducer.clone()
    };

    let start = Instant::now();
    let saved: String = run_distributed_mapreduce_bytes_opts(
        job,
        "render_tile",
        chunker,
        reducer,
        encode_chunk,
        decode_result,
        JobOptions {
            missing_chunks: MissingChunkPolicy::Fail,
            module: ModuleSource::CompileCrate(env!("CARGO_MANIFEST_DIR").into()),
            ..JobOptions::default()
        },
    )
    .await
    .map_err(|e| anyhow::anyhow!("distributed render failed: {e}"))?;

    println!("🖼️  Rendered to {} in {:?}", saved, start.elapsed());
    Ok(())
}
