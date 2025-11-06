# 🚀 Distributed Computing Framework
[](.md)
A high-performance distributed computing system built with WASM and WebRTC that enables seamless peer-to-peer computation across browser-based worker nodes.

## 🏗️ Architecture

```
┌─────────────────┐    WebSocket     ┌─────────────────┐
│   Master Node   │◄────────────────►│  Signaling      │
│  (Rust Binary)  │   Registration   │    Server       │
└─────────────────┘                  │ (Node.js/WS)    │
         │                           └─────────────────┘
         │ WebRTC Data Channels               │
         │                                    │ Peer Discovery
         ▼                                    ▼
┌─────────────────┐                  ┌─────────────────┐
│ Worker Node #1  │                  │ Worker Node #2  │
│   (Browser)     │◄─────────────────┤   (Browser)     │
└─────────────────┘   WebRTC P2P     └─────────────────┘
```

## 🚀 Quick Start

```bash
cd examples
cargo run
```

## 🛠️ Project Structure

```
├── 📁 distribute_runtime/       # Core distributed computing runtime
│   ├── Cargo.toml
│   └── src/lib.rs              # WebRTC master implementation
├── 📁 examples/                # Example applications and use cases
│   ├── Cargo.toml
│   ├── src/
│   │   ├── main.rs             # MapReduce example
│   │   └── lib.rs              # Distributed functions
├── 📁 benchmark/               # Throughput benchmark suite
│   ├── Cargo.toml
│   ├── src/main.rs             # Benchmark runner
│   └── README.md               # Benchmark documentation
├── 📁 network/                 # Network components
│   ├── 📁 server/              # Signaling server
│   │   ├── package.json        # Node.js dependencies for WebSocket server
│   │   ├── package-lock.json   # Dependency lock file
│   │   ├── node_modules/       # Node.js dependencies (ws library)
│   │   └── websocket-server.js # WebSocket server with master/worker distinction
│   └── 📁 worker/              # Browser-based worker nodes
│       ├── index.html          # Worker dashboard UI
│       └── worker.js           # WebRTC worker implementation
├── 📄 Cargo.toml               # Workspace configuration
├── 📄 run_benchmark.sh         # Benchmark runner script
└── 📄 README.md               # This file
```

## 📊 Benchmarking

The project includes a comprehensive throughput benchmark suite to measure system performance.

### Quick Start

1. **Start the WebSocket server**:
   ```bash
   cd network/server
   node websocket-server.js
   ```

2. **Open worker nodes** in browser tabs:
   ```bash
   # Open network/worker/index.html in one or more browser tabs
   # Each tab = one worker node
   ```

3. **Run benchmarks**:
   ```bash
   # Using the helper script
   ./run_benchmark.sh

   # Or directly with cargo
   cargo run --bin benchmark
   ```

### Benchmark Options

```bash
cargo run --bin benchmark -- \
  --workers 1,2,4,8 \
  --task-sizes 100,1000,10000 \
  --mode both \
  --iterations 10 \
  --output results.json
```

See `benchmark/README.md` for detailed documentation on benchmarking options and interpreting results.

## 🔧 Configuration

### WebSocket Server Port
```javascript
const wss = new WebSocket.Server({ port: 3000 });
```

### Worker Connection Limits
```javascript
// Keep only last 50 tasks to prevent memory issues
if (computeHistory.length > 50) {
    computeHistory = computeHistory.slice(0, 50);
}
```

### Master Timeouts
```rust
let timeout = tokio::time::Duration::from_secs(10);
```
