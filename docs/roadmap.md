# Roadmap

Ordered by priority. Items marked (bug) affect correctness.

Phase 1 (July 2026) landed the binary framed protocol, the unified byte pipeline, ordered results with an explicit missing-chunk policy, and the fault-tolerance fixes below. Phase 2 (July 2026) landed content-hash module artifacts, the pull scheduler, and event-driven job start. Phase 4 (July 2026) landed the `examples_raytrace` showcase, proving the new-workload-is-one-crate model (measured 4.9s to 2.3s going from one worker to two, byte-identical output). Hosting a public hive (the former phase 3) is deferred: the project is local-first open source for now.

## P0: one runtime that generalizes to any task

- [x] Unify the numeric and byte task paths into a single byte-based path; numeric jobs are now an encoder/decoder over the general pipeline. `ExecutionMode` no longer selects function names.
- [x] Make the worker a pure dispatcher: `wasmModule[map_function](bytes, meta)` uniformly; function-name special cases and worker-side PNG encoding removed (output encoding is app-side now).
- [x] Stop hardcoding the WASM artifact: `JobOptions.module` accepts the examples crate (default), any wasm-pack `pkg/` directory, or prebuilt bytes. Tasks reference modules by sha256; workers cache by hash across jobs and pull missing modules with `need_module`. New task types are new crates, with no framework changes. (Note: wasm-pack rebuilds are not byte-identical, so the compiled-examples default memoizes per process; prebuilt artifacts get stable hashes across processes.)

## P1: correctness and performance

- [x] (bug) Byte-path worker errors are structured error results; the master treats them as failures and reassigns instead of collecting empty payloads.
- [x] (bug) Results carry framework-level chunk indices and are reassembled in order; missing chunks follow an explicit `MissingChunkPolicy` (`Fail` default, `AllowPartial` opt-in).
- [x] (bug) A reassigned task reaches its new worker with the WASM module included when that worker does not have it; a worker-side module-miss heals by re-sending with the module.
- [x] (bug) Tasks on a connected-but-stuck worker are reassigned on timeout (first result wins, duplicates deduplicated); retry-exhausted chunks are dropped from pending and counted as missing; failed workers are rehabilitated after a cooldown.
- [x] Results are chunked worker-to-master exactly like tasks master-to-worker (framed transfers both directions). Supersedes the WebApp `result-chunking` branch.
- [x] Binary data-channel frames (raw bytes, no base64) with bufferedAmount backpressure on both sides.
- [x] Fixed discovery sleeps (3s + 5s) replaced by readiness polling: jobs start as soon as `min_workers` channels are open (measured: a warm 8-task job dropped from 9.2s to 0.25s).
- [x] Pull-based scheduling: a task queue feeds each worker up to `max_inflight_per_worker`; the next chunk ships when a result returns. Workers that connect mid-job join the rotation automatically (measured: a worker joining 3s into a 600-frame job took 256 of the 600 tasks).

## P2: hygiene

- [ ] Configurable proxy URL and STUN/TURN servers (signaling URL now reads `WASMHIVE_SIGNALING_URL`; the rest is still hardcoded).
- [ ] Workers should only dial masters (peer list should carry roles); masters should not treat every peer as a worker.
- [ ] Use or remove the server's fair-share allocation broadcasts (currently informational only).
- [ ] Execute WASM in a Web Worker so heavy tasks do not freeze the tab; one Web Worker per core.
- [x] Remove dead code: legacy float task path, old message formats in the worker, duplicate helpers, base64 helpers.
- [x] Fix the GPU demo shader (squares now, matching cpu_map) and give gpu_map an in-module CPU fallback when WebGPU is unavailable.
- [ ] Tests: protocol unit tests exist; add chunker/reducer tests and a headless-browser integration test in CI.
- [ ] Security notes: authenticated master registration, proxy allowlist.
