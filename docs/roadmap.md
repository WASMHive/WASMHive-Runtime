# Roadmap

Ordered by priority. Items marked (bug) affect correctness today.

## P0: one runtime that generalizes to any task

- [ ] Unify the numeric and byte task paths into a single byte-based path; numeric jobs become an encoder/decoder pair. Remove `ExecutionMode` from the core (CPU vs GPU is a property of the WASM function, not the framework).
- [ ] Make the worker a pure dispatcher: call `wasmModule[map_function](bytes, meta)` uniformly; remove special-cased function names and move output encoding decisions (e.g. PNG) into the job spec.
- [ ] Stop hardcoding the WASM artifact: accept a prebuilt module path (or crate directory) per job, cache modules on workers by content hash, and let workers request a module they are missing. New task types then become new crates, with no framework changes.

## P1: correctness and performance

- [ ] (bug) Byte-path worker errors arrive as `{ result_b64: "", error }` and are currently collected as successes. Parse the error field, treat as failure, reassign.
- [ ] (bug) Numeric results concatenate in arrival order and partial results are silently accepted. Attach chunk indices at the framework level, reassemble in order, and make missing-chunk policy explicit.
- [ ] (bug) A reassigned task can reach a worker that never received the WASM module (module travels only with each worker's first task).
- [ ] (bug) Tasks on a connected-but-stuck worker are never reassigned; retry-exhausted tasks are never dropped from pending; failed workers are never rehabilitated.
- [ ] Chunk worker-to-master results the same way master-to-worker tasks are chunked (large PNG results can exceed data-channel message limits). A working implementation exists on the WebApp `result-chunking` branch.
- [ ] Replace fixed discovery sleeps (3s + 5s) with event-driven readiness; dispatch as soon as N channels are open.
- [ ] Pull-based scheduling: keep a task queue and send the next chunk when a worker returns a result, instead of assigning everything upfront.
- [ ] Binary data-channel frames (header + raw bytes) instead of JSON with double base64; backpressure via bufferedAmount instead of fixed sleeps.

## P2: hygiene

- [ ] Configurable signaling URL, proxy URL, and STUN/TURN servers (currently localhost-only).
- [ ] Workers should only dial masters (peer list should carry roles); masters should not treat every peer as a worker.
- [ ] Use or remove the server's fair-share allocation broadcasts (currently informational only).
- [ ] Execute WASM in a Web Worker so heavy tasks do not freeze the tab; one Web Worker per core.
- [ ] Remove dead code: legacy `execute_map_reduce` path, old message formats in the worker, duplicate helpers.
- [ ] Fix the GPU demo shader (currently doubles instead of squares) and make `cpu_map` non-trivial.
- [ ] Tests: unit tests for chunkers/reducers, one headless-browser integration test.
- [ ] Security notes: authenticated master registration, proxy allowlist.
