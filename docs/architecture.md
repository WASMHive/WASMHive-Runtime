# WASMHive Architecture

## Overview

WASMHive is a master-worker map-reduce system where workers are browser tabs. The master is a native Rust binary linking `distribute_runtime`; workers load a static page (see WASMHive-WebApp) that executes WASM shipped to it at job time. A small Node.js WebSocket server handles registration, peer lists, and WebRTC signaling relay. Job data flows master to worker over WebRTC data channels, so payloads never transit the signaling server.

## Job lifecycle

1. An application calls `run_distributed_mapreduce` (numeric path) or `run_distributed_mapreduce_bytes` (general byte path) with input, a chunker, a reducer, encoder/decoder closures, and the name of the WASM map function.
2. The runtime compiles the `examples` crate with `wasm-pack` at job time and reads the produced module and JS glue.
3. The master registers with the signaling server. Workers initiate WebRTC offers to the master; the master answers, and data channels open.
4. Chunks are assigned to available workers round-robin. The WASM module and glue are embedded (base64) only in the first task sent to each worker; workers cache the module per function name. Task JSON above 30KB is split into sequenced chunks and reassembled on the worker.
5. The worker imports the JS glue from a blob URL, instantiates the WASM module, calls the named function on the decoded payload, and returns the result (PNG-encoded for image frames) over the data channel.
6. The master tracks pending tasks, retries and reassigns on timeout or worker failure, collects results, and applies the reducer locally.

## Wire protocol (binary, v1)

Signaling stays JSON over WebSocket: `welcome`, `peerList`, `registerMaster` / `registerWorker`, `offer` / `answer` / `candidate`, `allocation` (fair-share info, informational).

Everything on the data channel is a binary frame (see `distribute_runtime/src/protocol.rs`, mirrored in the WebApp worker):

```text
[magic 0xA5][version][ftype][reserved]
[id_len u16 LE][transfer id][frame_seq u32 LE][total_frames u32 LE]
[payload_len u32 LE][payload]
```

Frames of one transfer (max 60KB payload each) reassemble into a payload; sends are paced by bufferedAmount backpressure on both sides.

- Task payload: `[u32 header_len][header json][u32 wasm_len][wasm][u32 glue_len][glue][u32 input_len][input]` where the header is `{ job_id, task_id, chunk_index, map_function, meta }`. The module and glue travel only in the first task per worker per job; workers cache the loaded module by job id.
- Result payload: `[u32 header_len][header json][body]` where the header is `{ task_id, chunk_index, worker_id, error?, meta }`. An `error` marks the task failed and triggers reassignment; the body carries raw result bytes otherwise.

The worker is a pure dispatcher: it calls `wasmModule[map_function](input_bytes, meta)` (sync or async) and returns whatever bytes come back. All task-specific interpretation lives in the app-side encoder/decoder and the WASM module itself.

## Fault tolerance

Pending tasks carry a sent-at timestamp and retry count. Tasks that exceed the per-task timeout are reassigned to another worker whether or not the original channel is still open (first result wins; duplicates are dropped by task id). Structured worker errors reassign immediately; a module-miss error re-sends with the module included and is not held against the worker. Workers accumulate strikes for timeouts and failures, are excluded after three, and are rehabilitated after a cooldown if their channel is still open. Chunks that exhaust retries follow the job's `MissingChunkPolicy`: `Fail` errors the job, `AllowPartial` returns the rest in order and reports what was dropped.

## Known limitations

These are tracked in the roadmap:

- The WASM artifact location and crate are fixed (`examples` crate) rather than supplied per job.
- Static round-robin assignment; no pull-based scheduling for stragglers.
- Fixed discovery sleeps instead of event-driven readiness, which also inflates benchmark latency by ~8s per job.
- WASM executes on the worker tab's main thread.
