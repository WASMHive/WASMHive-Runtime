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

## Wire protocol (current)

- Signaling: `welcome`, `peerList`, `registerMaster` / `registerWorker`, `offer` / `answer` / `candidate`, `allocation` (fair-share info, informational).
- Tasks (numeric): `{ task_id, wasm_module, js_glue, data_chunk: [f32], map_function }`.
- Tasks (bytes): `{ task_id, wasm_module, js_glue, data_chunk_b64, map_function, meta }`.
- Chunked transport: `{ chunk_id, chunk_index, total_chunks, data }`.
- Results: `{ task_id, result: [f32], worker_id }` or `{ task_id, result_b64, worker_id, meta }`.

## Fault tolerance

Pending tasks carry a sent-at timestamp and retry count. A background check reassigns tasks whose worker's channel dropped, and numeric-path empty results are treated as failures and reassigned. Workers whose connection state degrades are marked failed and excluded from scheduling.

## Known limitations

These are tracked in the roadmap:

- Two parallel task paths (numeric and bytes) instead of one general path.
- The worker dispatcher special-cases certain function names instead of being fully generic.
- The WASM artifact location and crate are fixed (`examples` crate) rather than supplied per job.
- Static round-robin assignment; no pull-based scheduling for stragglers.
- Fixed discovery sleeps instead of event-driven readiness, which also inflates benchmark latency by ~8s per job.
- Results return as a single data-channel message, which limits maximum result size.
- Byte-path worker errors are not yet distinguished from empty successful results.
