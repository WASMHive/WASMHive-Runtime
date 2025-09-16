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
├── 📁 distribute_macro/         # Procedural macros for distributed computing
│   ├── Cargo.toml
│   └── src/lib.rs
├── 📁 distribute_runtime/       # Core distributed computing runtime
│   ├── Cargo.toml
│   └── src/lib.rs              # WebRTC master implementation
├── 📁 examples/                # Example applications and use cases
│   ├── Cargo.toml
│   ├── src/
│   │   ├── main.rs             # MapReduce example
│   │   └── lib.rs              # Distributed functions
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
└── 📄 README.md               # This file
```

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
