# 🚀 Distributed Computing Framework

A high-performance distributed computing system built with Rust and WebRTC that enables seamless peer-to-peer computation across browser-based worker nodes.

## ✨ Features

- 🌐 **WebRTC Peer-to-Peer Communication** - Direct data channels between master and workers
- 🦀 **Rust-Powered Master Nodes** - High-performance coordination and task distribution
- 🌍 **Browser-Based Worker Nodes** - No installation required, works in any modern browser
- ⚡ **Automatic Work Distribution** - Intelligent chunking and load balancing
- 📊 **Real-Time Monitoring** - Live compute history and performance tracking
- 🔄 **CPU/GPU Execution Modes** - Flexible execution strategies
- 🎯 **Type-Safe Operations** - Built with Rust's type system and serde serialization
- 🔌 **Clean Disconnection** - Proper master/worker lifecycle management

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

### Components

- **Master Node**: Rust application that distributes computational tasks
- **Worker Nodes**: Browser-based compute nodes with interactive dashboards
- **Signaling Server**: WebSocket server for peer discovery and connection establishment
- **WebRTC Data Channels**: High-performance peer-to-peer communication

## 🚀 Quick Start

### Prerequisites

- Rust (latest stable)
- Node.js (for signaling server)
- Modern web browser(s) for worker nodes

### 1. Start the Signaling Server

```bash
cd /path/to/project/network/server
npm install  # Install WebSocket dependencies (ws library)
node websocket-server.js
```

The server will start on `ws://localhost:3000` and handle:
- Peer discovery and registration
- WebRTC connection signaling
- Master/worker node distinction

### 2. Launch Worker Nodes

Open `network/worker/index.html` in multiple browser tabs/windows. Each tab becomes an independent worker node with:

- **Real-time connection status**
- **Peer network visualization**
- **Scrollable compute history**
- **Task execution monitoring**

### 3. Run Distributed Computation

```bash
cd examples
cargo run
```

This executes the MapReduce example that:
- Connects to the signaling server as a master
- Discovers available worker nodes
- Distributes work across all connected workers
- Collects and aggregates results
- Properly disconnects after completion

## 📊 Example Output

```
🚀 Distributed MapReduce Master Node
====================================

📊 MapReduce Example: Sum of Squares
Input: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
Expected (sum of squares): 385

🎯 Running with GPU mode...
🌐 Dispatching work to distributed worker nodes...
🔗 Connected to worker: worker_abc123
🔗 Connected to worker: worker_def456
🔗 Connected to worker: worker_ghi789
🔍 Current workers list: ["worker_abc123", "worker_def456", "worker_ghi789"]
📊 Distributing to 3 connected workers: ["worker_abc123", "worker_def456", "worker_ghi789"]
   ✅ task_1234567890_0 -> worker_abc123 (3 elements)
   ✅ task_1234567890_1 -> worker_def456 (3 elements)
   ✅ task_1234567890_2 -> worker_ghi789 (4 elements)
⏳ Waiting for 3 results from workers...
   📥 worker_abc123 returned 3 values: [1.0, 4.0, 9.0]
   📥 worker_def456 returned 3 values: [16.0, 25.0, 36.0]
   📥 worker_ghi789 returned 4 values: [49.0, 64.0, 81.0, 100.0]
✅ All 3 workers completed! Distributed result: 385
🔌 Disconnected from signaling server
GPU Result: 385
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

## 💻 Implementation Details

### Master Node (`distribute_runtime`)

- **WebRTC Connection Management**: Establishes and maintains peer connections
- **Task Distribution**: Intelligently chunks work across available workers
- **Result Aggregation**: Collects and combines results from all workers
- **Lifecycle Management**: Proper connection setup and teardown
- **Error Handling**: Robust failure recovery and worker timeout handling

### Worker Nodes (`network/worker/`)

- **WebRTC Data Channel Handling**: Receives tasks via peer-to-peer connections
- **Compute Engine**: Executes JavaScript/WASM computations locally
- **History Tracking**: Maintains scrollable log of all completed tasks
- **Real-time UI**: Live status updates and performance monitoring
- **Multi-format Support**: Handles various task formats and communication protocols

### Signaling Server (`network/server/websocket-server.js`)

- **Peer Discovery**: Manages worker registration and discovery
- **Master/Worker Distinction**: Separates masters from workers in peer lists
- **Connection Brokering**: Facilitates WebRTC connection establishment
- **Network Monitoring**: Tracks connections and disconnections

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

## 🎯 Use Cases

- **Scientific Computing**: Distribute mathematical computations across multiple devices
- **Data Processing**: Parallel processing of large datasets
- **Machine Learning**: Distributed training and inference
- **Cryptography**: Parallel hash computations and cryptographic operations
- **Image/Video Processing**: Distributed media processing pipelines
- **Financial Modeling**: Parallel Monte Carlo simulations

## 🔄 Execution Modes

### CPU Mode
- Pure JavaScript execution on worker nodes
- Reliable cross-platform compatibility
- Suitable for general-purpose computations

### GPU Mode
- Hardware acceleration when available
- Falls back to CPU mode if GPU unavailable
- Optimal for parallel mathematical operations

## 🐛 Troubleshooting

### No Workers Available
- Ensure signaling server is running on `localhost:3000`
- Open worker nodes (`network/worker/index.html`) in browser tabs
- Check browser console for WebRTC connection errors

### Connection Issues
- Verify firewall settings allow WebRTC traffic
- Check that browsers support WebRTC (all modern browsers do)
- Ensure workers and masters can reach the signaling server

### Performance Issues
- Monitor worker node dashboards for task distribution
- Check network latency between peers
- Verify computational load balancing across workers

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🏆 Acknowledgments

- Built with [Rust](https://www.rust-lang.org/) for performance and safety
- Uses [WebRTC](https://webrtc.org/) for peer-to-peer communication
- Powered by [tokio](https://tokio.rs/) for async runtime
- UI styled with modern CSS gradients and animations