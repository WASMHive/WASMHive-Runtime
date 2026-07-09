# Throughput Benchmark Suite

This benchmark suite measures the throughput and performance characteristics of the distributed computing system.

## Metrics Collected

- **Tasks per second**: Number of tasks processed per second
- **Data throughput**: Data transfer rate in Mbps (megabits per second)
- **Latency**: End-to-end time from task submission to result completion
- **Success rate**: Percentage of successful task completions
- **Duration statistics**: Mean and standard deviation of execution times

## Usage

### Basic Usage

```bash
# Run with default settings
cargo run --bin benchmark

# Run with custom configuration
cargo run --bin benchmark -- \
  --workers 1,2,4,8 \
  --task-sizes 100,1000,10000 \
  --mode both \
  --iterations 10 \
  --warmup 3 \
  --output results.json
```

### Command Line Options

- `--workers`: Comma-separated list of worker counts to test (default: `1,2,4`)
- `--task-sizes`: Comma-separated list of task sizes (number of elements) to test (default: `100,1000,10000`)
- `--mode`: Execution mode - `cpu`, `gpu`, or `both` (default: `both`)
- `--iterations`: Number of benchmark iterations per test (default: `5`)
- `--warmup`: Number of warmup iterations before benchmarking (default: `2`)
- `--output`: Output file path for JSON results (default: `benchmark_results.json`)
- `--wait-time`: Wait time in seconds between tests (default: `2`)

### Example

```bash
# Quick benchmark with 2 workers, small tasks, CPU only
cargo run --bin benchmark -- --workers 2 --task-sizes 100 --mode cpu --iterations 3

# Comprehensive benchmark
cargo run --bin benchmark -- \
  --workers 1,2,4,8,16 \
  --task-sizes 100,1000,10000,100000 \
  --mode both \
  --iterations 10 \
  --warmup 5 \
  --output comprehensive_results.json
```

## Prerequisites

Before running benchmarks:

1. **Start the WebSocket signaling server**:
   ```bash
   # from a clone of WASMHive-WebApp
   cd server
   node websocket-server.js
   ```

2. **Open worker nodes in browsers**:
   - Open `worker/index.html` from the WASMHive-WebApp repo in one or more browser tabs
   - Each tab represents one worker node
   - Open as many tabs as the maximum worker count you want to test

3. **Run the benchmark**:
   ```bash
   cargo run --bin benchmark
   ```

## Output Format

The benchmark outputs results in JSON format with the following structure:

```json
[
  {
    "timestamp": "2024-01-01T12:00:00Z",
    "config": {
      "worker_count": 2,
      "task_size": 1000,
      "execution_mode": "gpu",
      "iterations": 5
    },
    "results": [
      {
        "iteration": 0,
        "duration_ms": 1234.56,
        "tasks_per_second": 810.23,
        "data_throughput_mbps": 6.48,
        "latency_ms": 1234.56,
        "success": true,
        "error": null
      }
    ],
    "summary": {
      "mean_duration_ms": 1200.0,
      "stddev_duration_ms": 50.0,
      "mean_tasks_per_second": 833.33,
      "mean_data_throughput_mbps": 6.67,
      "mean_latency_ms": 1200.0,
      "min_latency_ms": 1150.0,
      "max_latency_ms": 1300.0,
      "success_rate": 1.0,
      "total_tasks": 5000,
      "total_data_bytes": 40000000
    }
  }
]
```

## Interpreting Results

- **Higher tasks_per_second** = Better throughput
- **Higher data_throughput_mbps** = Better data transfer efficiency
- **Lower latency_ms** = Faster response times
- **Higher success_rate** = More reliable system
- **Lower stddev_duration_ms** = More consistent performance

## Tips

1. **Warmup runs**: The benchmark performs warmup runs to allow the system to stabilize (JIT compilation, WebRTC connection establishment, etc.)

2. **Wait time**: A wait time between tests helps prevent interference between test runs

3. **Worker count**: The actual number of workers depends on how many browser tabs you have open. The `--workers` parameter is informational for organizing results.

4. **Task size**: Larger task sizes may take longer but provide better throughput measurements for sustained workloads

5. **Multiple iterations**: More iterations provide more statistically reliable results but take longer to run

