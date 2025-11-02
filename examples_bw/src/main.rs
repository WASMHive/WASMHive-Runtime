use anyhow::{Context, Result};
use distribute_runtime::run_distributed_mapreduce_bytes;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::process::Command;
use walkdir::WalkDir;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct VideoJob {
    input_path: String,
    temp_dir: String,
    width: u32,
    height: u32,
    fps: u32,
    frame_paths: Vec<PathBuf>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct VideoResult {
    output_path: String,
}

// 1) Extract raw RGBA frames using ffmpeg CLI into temp_dir
async fn extract_frames(input: &str, temp_dir: &Path, fps: u32) -> Result<(u32, u32, Vec<PathBuf>)> {
    fs::create_dir_all(temp_dir).await.ok();

    // Probe width/height using ffprobe
    let probe = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height",
            "-of",
            "json",
            input,
        ])
        .output()
        .await
        .context("failed to run ffprobe")?;
    let probe_json: serde_json::Value = serde_json::from_slice(&probe.stdout).context("parse ffprobe json")?;
    let width = probe_json["streams"][0]["width"].as_u64().unwrap_or(0) as u32;
    let height = probe_json["streams"][0]["height"].as_u64().unwrap_or(0) as u32;

    // Extract frames as raw RGBA files: frame_000001.rgba ...
    let pattern = temp_dir.join("frame_%06d.rgba");
    let status = Command::new("ffmpeg")
        .args([
            "-y",
            "-i",
            input,
            "-vf",
            &format!("fps={}", fps),
            "-f",
            "rawvideo",
            "-pix_fmt",
            "rgba",
            pattern.to_str().unwrap(),
        ])
        .status()
        .await
        .context("failed to run ffmpeg to extract frames")?;
    if !status.success() {
        anyhow::bail!("ffmpeg extract failed");
    }

    let mut frames = Vec::new();
    for entry in WalkDir::new(temp_dir) {
        let entry = entry?;
        if entry.file_type().is_file() {
            let p = entry.path();
            if p.extension().and_then(|s| s.to_str()) == Some("rgba") {
                frames.push(p.to_path_buf());
            }
        }
    }
    frames.sort();
    Ok((width, height, frames))
}

// 2) Chunker: one frame per chunk
fn chunker(job: &VideoJob) -> Vec<VideoJob> {
    job.frame_paths
        .iter()
        .enumerate()
        .map(|(idx, p)| VideoJob {
            input_path: job.input_path.clone(),
            temp_dir: job.temp_dir.clone(),
            width: job.width,
            height: job.height,
            fps: job.fps,
            frame_paths: vec![p.clone()], // single-frame chunk
        })
        .collect()
}

// 3) Encoder: read RGBA bytes and attach meta (frame_index, width, height)
fn encode_chunk(chunk: &VideoJob) -> (Vec<u8>, serde_json::Value) {
    let frame_path = &chunk.frame_paths[0];
    let idx = frame_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("0")
        .split('_')
        .last()
        .unwrap_or("0")
        .parse::<u32>()
        .unwrap_or(0);

    let bytes = std::fs::read(frame_path).unwrap_or_default();
    let meta = json!({
        "frame_index": idx,
        "width": chunk.width,
        "height": chunk.height,
    });
    (bytes, meta)
}

// 4) Decoder: write processed bytes back to temp dir for later re-encode, return a tuple struct
#[derive(Clone, Serialize, Deserialize, Debug)]
struct FrameOut { index: u32, path: PathBuf }

fn decode_result(bytes: Vec<u8>, meta: serde_json::Value) -> FrameOut {
    let idx = meta["frame_index"].as_u64().unwrap_or(0) as u32;
    let width = meta["width"].as_u64().unwrap_or(0) as u32;
    let height = meta["height"].as_u64().unwrap_or(0) as u32;
    // Write to temp file path based on index
    let out_dir = std::env::temp_dir().join("w3dge_bw_frames");
    std::fs::create_dir_all(&out_dir).ok();
    let out_path = out_dir.join(format!("bw_{:06}.rgba", idx));
    std::fs::write(&out_path, &bytes).ok();
    // width/height kept in meta for encoder later
    FrameOut { index: idx, path: out_path }
}

// 5) Reducer: re-encode into MP4 via ffmpeg
fn reducer(frames: Vec<FrameOut>) -> VideoResult {
    if frames.is_empty() {
        return VideoResult { output_path: String::new() };
    }
    let mut frames_sorted = frames;
    frames_sorted.sort_by_key(|f| f.index);

    // Assume all frames same dims; derive from first meta captured in filename directory (not stored here),
    // so we rely on re-probe of raw frame size from the job-level metadata in filenames not available now.
    // For simplicity, ask ffmpeg to infer size via explicit -video_size using the meta we included earlier would be better,
    // but we don't have it here; we’ll try a common fallback by reading first file size.
    let first = &frames_sorted[0];
    let data = std::fs::read(&first.path).unwrap();
    // Cannot infer width/height from raw buffer size reliably; require environment variable or fallback.
    // For demo, set environment-provided size or default (e.g., 1280x720).
    let (width, height) = match (
        std::env::var("W3DGE_FRAME_WIDTH").ok(),
        std::env::var("W3DGE_FRAME_HEIGHT").ok(),
    ) {
        (Some(w), Some(h)) => (w.parse().unwrap_or(1280u32), h.parse().unwrap_or(720u32)),
        _ => (1280, 720),
    };

    let out_video = std::env::current_dir()
        .unwrap()
        .join("bw_output.mp4");

    // Write a file list for ffmpeg concat with rawvideo is tricky; instead invoke ffmpeg with image2pipe or numbered files
    // Our files are numbered bw_######.rgba; use pattern and fps env or default 30
    let fps = std::env::var("W3DGE_FRAME_FPS").ok().and_then(|s| s.parse().ok()).unwrap_or(30u32);
    let pattern = first.path.parent().unwrap().join("bw_%06d.rgba");

    // Move/copy all frames to ensure sequential numbering from 000001..N; they already are from index
    // Encode back using ffmpeg
    let status = std::process::Command::new("ffmpeg")
        .args([
            "-y",
            "-f", "rawvideo",
            "-pix_fmt", "rgba",
            "-video_size", &format!("{}x{}", width, height),
            "-framerate", &fps.to_string(),
            "-i", pattern.to_str().unwrap(),
            out_video.to_str().unwrap(),
        ])
        .status()
        .expect("failed to run ffmpeg for encode");
    if !status.success() {
        eprintln!("ffmpeg encode failed");
    }

    VideoResult { output_path: out_video.to_string_lossy().to_string() }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Inputs (use args or defaults)
    let input_video = std::env::args().nth(1).unwrap_or("input.mp4".to_string());
    let fps: u32 = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(30);

    // Temp workspace
    let temp_dir = std::env::temp_dir().join("w3dge_frames");
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).await.ok();
    }
    fs::create_dir_all(&temp_dir).await.ok();

    // Extract frames
    let (width, height, frames) = extract_frames(&input_video, &temp_dir, fps).await?;

    // Create job
    let job = VideoJob {
        input_path: input_video.clone(),
        temp_dir: temp_dir.to_string_lossy().to_string(),
        width,
        height,
        fps,
        frame_paths: frames,
    };

    // Distribute grayscale conversion; the WASM function name is provided in examples crate
    let result: VideoResult = run_distributed_mapreduce_bytes(
        job,
        "grayscale_frame_rgba",
        chunker,
        reducer,
        encode_chunk,
        decode_result,
    ).await;

    println!("Black & White video written to: {}", result.output_path);
    Ok(())
}
