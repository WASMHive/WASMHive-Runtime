# Archived benchmark results

Point-in-time records used in the project evaluation. Both were produced by
`cargo run -p benchmark` on the same laptop with headless Chrome workers.

| File | System state | Headline |
|---|---|---|
| `results-2025-11.json` | Original JSON + base64 protocol, fixed 8s discovery sleeps, upfront round-robin | ~10.2s for an 8-task job (dominated by fixed sleeps) |
| `results-2026-07-binary-protocol.json` | Binary framed protocol, content-hash module store, pull scheduler, event-driven start | 1.48s cold / 0.25s warm for the same job |

The distributed ray tracer adds a scaling datapoint: 640x360 at 24 samples per
pixel renders in 4.9s with one worker tab and 2.3s with two, with byte-identical
output (see the repo README).
