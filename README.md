# 🐝 WASMHive Runtime

The Rust core of **WASMHive**: a distributed computing framework that turns ordinary browser tabs into compute workers. A native Rust master splits a job into chunks, ships a WASM module plus data to browser workers over WebRTC data channels, and reduces the returned results locally. No installs on worker machines, if it has a browser it can join the hive.

The browser side (signaling server, CORS proxy, and the worker page) lives in [WASMHive-WebApp](https://github.com/WASMHive/WASMHive-WebApp). The containerized baseline we benchmark against lives in [docker-hive](https://github.com/WASMHive/docker-hive).

## 🏗️ Architecture

```
┌─────────────────┐    WebSocket     ┌──────────────────┐
│   Master Node   │◄────────────────►│    Signaling     │
│  (Rust binary)  │   Registration   │      Server      │
└─────────────────┘                  │(WASMHive-WebApp) │
         │                           └──────────────────┘
         │ WebRTC Data Channels               │
         ▼                                    ▼ Peer discovery
┌─────────────────┐                  ┌─────────────────┐
│ Worker (browser)│                  │ Worker (browser)│
│  WASM executor  │                  │  WASM executor  │
└─────────────────┘                  └─────────────────┘
```

A job is defined by four pieces the application supplies:

1. a **chunker** that splits the input,
2. a named **WASM map function** that runs on workers,
3. an **encoder/decoder** pair for the wire format (bytes + JSON meta),
4. a **reducer** that combines results on the master.

The runtime handles WASM compilation (via `wasm-pack`), worker discovery, WebRTC setup, task distribution, retries and reassignment on worker failure, and result collection.

## 📁 Workspace

| Crate | What it is |
|---|---|
| `distribute_runtime` | The runtime library: signaling client, WebRTC master, task scheduling, fault tolerance |
| `examples` | The WASM module shipped to workers (`cpu_map`, `gpu_map` via WebGPU, `grayscale_frame_rgba`, `fetch_url_title`) plus a numeric map-reduce demo |
| `examples_bw` | Distributed video grayscale: ffmpeg frame extraction, per-frame map on workers, re-encode |
| `examples_webcrawl` | Distributed URL title extraction over a URL list |
| `benchmark` | Throughput/latency benchmark suite (`run_benchmark.sh`) |

## 🚀 Quick start

Prerequisites: Rust, [`wasm-pack`](https://rustwasm.github.io/wasm-pack/), Node.js (for the signaling server), and `ffmpeg`/`ffprobe` for the video example.

1. Start the signaling server and open one or more worker tabs, following [WASMHive-WebApp](https://github.com/WASMHive/WASMHive-WebApp).
2. Run an example from this repo:

```bash
# Numeric map-reduce (CPU/GPU)
cargo run -p distributed_examples

# Video grayscale (writes bw_output.mp4)
cargo run -p examples_bw -- input.mp4 30

# Web crawl (reads crawl_these.txt, writes webcrawl_results.txt)
cargo run -p examples_webcrawl
```

Each worker tab shows the network topology, task history, and health status while jobs run.

## 📊 Benchmarking

```bash
./run_benchmark.sh
# or
cargo run -p benchmark -- --workers 1,2,4 --task-sizes 100,1000 --mode both --iterations 5
```

See `benchmark/README.md` for options. For the Docker comparison baseline, see [docker-hive](https://github.com/WASMHive/docker-hive).

## 🗺️ Roadmap

Active work is tracked in [docs/roadmap.md](docs/roadmap.md). Headline items: unifying the numeric and byte task paths, a fully generic worker dispatcher, content-hash WASM caching, pull-based scheduling, and binary wire frames.

## 📄 License

MIT
