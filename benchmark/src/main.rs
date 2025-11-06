use clap::Parser;
use distribute_runtime::{run_distributed_mapreduce, ExecutionMode};
use distributed_examples::{chunker, reducer, TestData};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of workers to test with (comma-separated list)
    #[arg(long, default_value = "1,2,4")]
    workers: String,

    /// Task sizes to test (comma-separated list of number of elements)
    #[arg(long, default_value = "100,1000,10000")]
    task_sizes: String,

    /// Execution mode: cpu, gpu, or both
    #[arg(long, default_value = "both")]
    mode: String,

    /// Number of iterations per test
    #[arg(long, default_value = "5")]
    iterations: usize,

    /// Warmup iterations before benchmarking
    #[arg(long, default_value = "2")]
    warmup: usize,

    /// Output file for results (JSON format)
    #[arg(long, default_value = "benchmark_results.json")]
    output: String,

    /// Wait time between tests (seconds)
    #[arg(long, default_value = "2")]
    wait_time: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct BenchmarkResult {
    timestamp: String,
    config: BenchmarkConfig,
    results: Vec<TestRunResult>,
    summary: SummaryStats,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct BenchmarkConfig {
    worker_count: usize,
    task_size: usize,
    execution_mode: String,
    iterations: usize,
}

#[derive(Serialize, Deserialize, Debug)]
struct TestRunResult {
    iteration: usize,
    duration_ms: f64,
    tasks_per_second: f64,
    data_throughput_mbps: f64,
    latency_ms: f64,
    success: bool,
    error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct SummaryStats {
    mean_duration_ms: f64,
    stddev_duration_ms: f64,
    mean_tasks_per_second: f64,
    mean_data_throughput_mbps: f64,
    mean_latency_ms: f64,
    min_latency_ms: f64,
    max_latency_ms: f64,
    success_rate: f64,
    total_tasks: usize,
    total_data_bytes: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("🚀 Starting Throughput Benchmark Suite");
    println!("========================================");
    println!("Configuration:");
    println!("  Workers: {}", args.workers);
    println!("  Task sizes: {}", args.task_sizes);
    println!("  Mode: {}", args.mode);
    println!("  Iterations: {}", args.iterations);
    println!("  Warmup: {}", args.warmup);
    println!("  Output: {}", args.output);
    println!();

    // Parse worker counts
    let worker_counts: Vec<usize> = args
        .workers
        .split(',')
        .map(|s| s.trim().parse().expect("Invalid worker count"))
        .collect();

    // Parse task sizes
    let task_sizes: Vec<usize> = args
        .task_sizes
        .split(',')
        .map(|s| s.trim().parse().expect("Invalid task size"))
        .collect();

    // Parse execution modes
    let modes = if args.mode == "both" {
        vec!["cpu", "gpu"]
    } else {
        vec![args.mode.as_str()]
    };

    let mut all_results: Vec<BenchmarkResult> = Vec::new();

    for &mode_str in &modes {
        let execution_mode = match mode_str {
            "cpu" => ExecutionMode::CPU,
            "gpu" => ExecutionMode::GPU,
            _ => {
                eprintln!("Invalid mode: {}", mode_str);
                continue;
            }
        };

        for &task_size in &task_sizes {
            for &worker_count in &worker_counts {
                println!(
                    "\n📊 Running benchmark: {} workers, {} elements, {} mode",
                    worker_count, task_size, mode_str
                );
                println!("{}", "=".repeat(60));

                // Note: Worker count is informational - actual workers depend on connected browsers
                let config = BenchmarkConfig {
                    worker_count,
                    task_size,
                    execution_mode: mode_str.to_string(),
                    iterations: args.iterations,
                };

                let result = run_benchmark(
                    &config,
                    execution_mode.clone(),
                    args.iterations,
                    args.warmup,
                    args.wait_time,
                )
                .await;

                all_results.push(result);

                // Wait between tests to allow system to stabilize
                if args.wait_time > 0 {
                    println!("⏳ Waiting {} seconds before next test...", args.wait_time);
                    sleep(Duration::from_secs(args.wait_time)).await;
                }
            }
        }
    }

    // Write results to file
    let json_output = serde_json::to_string_pretty(&all_results)?;
    std::fs::write(&args.output, json_output)?;
    println!("\n✅ Benchmark results written to: {}", args.output);

    // Print summary
    print_summary(&all_results);

    Ok(())
}

async fn run_benchmark(
    config: &BenchmarkConfig,
    execution_mode: ExecutionMode,
    iterations: usize,
    warmup: usize,
    wait_time: u64,
) -> BenchmarkResult {
    let mut test_results: Vec<TestRunResult> = Vec::new();

    // Warmup runs
    println!("🔥 Warmup runs: {}", warmup);
    for i in 0..warmup {
        let test_data = generate_test_data(config.task_size);
        let _ = run_single_test(&test_data, &execution_mode, i).await;
        if wait_time > 0 {
            sleep(Duration::from_secs(wait_time)).await;
        }
    }

    // Actual benchmark runs
    println!("📈 Benchmark runs: {}", iterations);
    for i in 0..iterations {
        let test_data = generate_test_data(config.task_size);
        let result = run_single_test(&test_data, &execution_mode, i).await;
        test_results.push(result);

        if wait_time > 0 && i < iterations - 1 {
            sleep(Duration::from_secs(wait_time)).await;
        }
    }

    // Calculate summary statistics
    let summary = calculate_summary(&test_results, config);

    BenchmarkResult {
        timestamp: chrono::Utc::now().to_rfc3339(),
        config: config.clone(),
        results: test_results,
        summary,
    }
}

async fn run_single_test(
    test_data: &TestData,
    execution_mode: &ExecutionMode,
    iteration: usize,
) -> TestRunResult {
    let start = Instant::now();
    let payload_data_bytes = test_data.numbers.len() * 4; // f32 = 4 bytes per number

    let result = run_distributed_mapreduce(
        test_data.clone(),
        match execution_mode {
            ExecutionMode::CPU => "cpu_map",
            ExecutionMode::GPU => "gpu_map",
        },
        chunker,
        reducer,
        execution_mode.clone(),
    )
    .await;

    let duration = start.elapsed();
    let duration_ms = duration.as_secs_f64() * 1000.0;
    let duration_secs = duration.as_secs_f64();

    let success = result.value.is_finite();
    let error = if success { None } else { Some("Invalid result".to_string()) };

    // Calculate throughput metrics
    let num_chunks = test_data.numbers.len(); // chunker creates one chunk per number
    let tasks_per_second = if duration_secs > 0.0001 {
        num_chunks as f64 / duration_secs
    } else {
        0.0
    };

    // Estimate total data transferred:
    // 1. WASM module (base64 encoded) - estimate ~100KB based on typical WASM size
    //    We can check the actual file, but for now use a reasonable estimate
    let estimated_wasm_bytes = estimate_wasm_size();
    let wasm_base64_bytes = (estimated_wasm_bytes as f64 * 1.33) as usize; // base64 encoding adds ~33%
    
    // 2. JS glue code - estimate ~10KB
    let estimated_js_glue_bytes = 10_000;
    
    // 3. Payload data: input chunks (JSON serialized with overhead)
    //    Each f32 in JSON is ~6-8 bytes (number + comma/whitespace), use 8 bytes as estimate
    let input_json_bytes = test_data.numbers.len() * 8;
    
    // 4. Output data: results (JSON serialized)
    let output_json_bytes = test_data.numbers.len() * 8; // assuming same size output
    
    // 5. JSON overhead for task structure (task_id, map_function, etc.) - estimate ~500 bytes per task
    let task_metadata_bytes = 500;
    
    // Total: WASM is sent once per worker, but for simplicity we count it per chunk
    // In reality, WASM is only sent once, but this gives us a conservative estimate
    let total_data_bytes = wasm_base64_bytes + estimated_js_glue_bytes + input_json_bytes + output_json_bytes + task_metadata_bytes;
    
    // Convert to Mbps: (bytes * 8 bits) / (seconds * 1_000_000)
    let data_throughput_mbps = if duration_secs > 0.0001 {
        (total_data_bytes as f64 * 8.0) / (duration_secs * 1_000_000.0)
    } else {
        0.0
    };

    // Latency is the duration (end-to-end)
    let latency_ms = duration_ms;

    // Debug output for first iteration or when throughput seems wrong
    if iteration == 0 || (data_throughput_mbps < 0.01 && duration_secs > 0.001) {
        println!(
            "  Iteration {}: {:.2}ms | {:.2} tasks/s | {:.4} Mbps | Latency: {:.2}ms",
            iteration + 1,
            duration_ms,
            tasks_per_second,
            data_throughput_mbps,
            latency_ms
        );
        println!(
            "    Debug: duration={:.6}s, total_bytes={}, payload_bytes={}",
            duration_secs, total_data_bytes, payload_data_bytes
        );
    } else if iteration % 5 == 0 {
        println!(
            "  Iteration {}: {:.2}ms | {:.2} tasks/s | {:.2} Mbps | Latency: {:.2}ms",
            iteration + 1,
            duration_ms,
            tasks_per_second,
            data_throughput_mbps,
            latency_ms
        );
    }

    TestRunResult {
        iteration,
        duration_ms,
        tasks_per_second,
        data_throughput_mbps,
        latency_ms,
        success,
        error,
    }
}

fn generate_test_data(size: usize) -> TestData {
    TestData {
        numbers: (1..=size).map(|i| i as f32).collect(),
    }
}

/// Estimate WASM module size by checking the actual file if available
fn estimate_wasm_size() -> usize {
    // Try to read the actual WASM file size
    let candidates = [
        "examples/pkg/distributed_examples_bg.wasm",
        "../examples/pkg/distributed_examples_bg.wasm",
        "./examples/pkg/distributed_examples_bg.wasm",
    ];
    
    for path in &candidates {
        if let Ok(metadata) = std::fs::metadata(path) {
            return metadata.len() as usize;
        }
    }
    
    // Fallback estimate: typical WASM module size is 50-200KB
    // Use 100KB as a reasonable default
    100_000
}

fn calculate_summary(results: &[TestRunResult], config: &BenchmarkConfig) -> SummaryStats {
    let successful_results: Vec<&TestRunResult> = results.iter().filter(|r| r.success).collect();
    let success_count = successful_results.len();
    let success_rate = if results.is_empty() {
        0.0
    } else {
        success_count as f64 / results.len() as f64
    };

    if successful_results.is_empty() {
        return SummaryStats {
            mean_duration_ms: 0.0,
            stddev_duration_ms: 0.0,
            mean_tasks_per_second: 0.0,
            mean_data_throughput_mbps: 0.0,
            mean_latency_ms: 0.0,
            min_latency_ms: 0.0,
            max_latency_ms: 0.0,
            success_rate,
            total_tasks: 0,
            total_data_bytes: 0,
        };
    }

    let durations: Vec<f64> = successful_results.iter().map(|r| r.duration_ms).collect();
    let tasks_per_sec: Vec<f64> = successful_results.iter().map(|r| r.tasks_per_second).collect();
    let throughput: Vec<f64> = successful_results.iter().map(|r| r.data_throughput_mbps).collect();
    let latencies: Vec<f64> = successful_results.iter().map(|r| r.latency_ms).collect();

    let mean_duration = mean(&durations);
    let stddev_duration = stddev(&durations, mean_duration);
    let mean_tasks_per_sec = mean(&tasks_per_sec);
    let mean_throughput = mean(&throughput);
    let mean_latency = mean(&latencies);
    let min_latency = latencies.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    let max_latency = latencies.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

    let total_tasks = config.task_size * success_count;
    // Use the same estimation logic as in run_single_test
    let estimated_wasm_bytes = estimate_wasm_size();
    let wasm_base64_bytes = (estimated_wasm_bytes as f64 * 1.33) as usize;
    let estimated_js_glue_bytes = 10_000;
    let input_json_bytes = config.task_size * 8;
    let output_json_bytes = config.task_size * 8;
    let task_metadata_bytes = 500;
    let total_data_bytes_per_run = wasm_base64_bytes + estimated_js_glue_bytes + input_json_bytes + output_json_bytes + task_metadata_bytes;
    let total_data_bytes = total_data_bytes_per_run * success_count;

    SummaryStats {
        mean_duration_ms: mean_duration,
        stddev_duration_ms: stddev_duration,
        mean_tasks_per_second: mean_tasks_per_sec,
        mean_data_throughput_mbps: mean_throughput,
        mean_latency_ms: mean_latency,
        min_latency_ms: min_latency,
        max_latency_ms: max_latency,
        success_rate,
        total_tasks,
        total_data_bytes,
    }
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

fn stddev(values: &[f64], mean: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let variance = values
        .iter()
        .map(|x| (x - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64;
    variance.sqrt()
}

fn print_summary(results: &[BenchmarkResult]) {
    println!("\n📊 BENCHMARK SUMMARY");
    println!("{}", "=".repeat(80));

    for result in results {
        let s = &result.summary;
        println!(
            "\n{} workers | {} elements | {} mode",
            result.config.worker_count,
            result.config.task_size,
            result.config.execution_mode
        );
        println!("  Mean Duration:     {:.2}ms ± {:.2}ms", s.mean_duration_ms, s.stddev_duration_ms);
        println!("  Throughput:        {:.2} tasks/sec", s.mean_tasks_per_second);
        println!("  Data Throughput:  {:.2} Mbps", s.mean_data_throughput_mbps);
        println!("  Latency:          {:.2}ms (min: {:.2}ms, max: {:.2}ms)", s.mean_latency_ms, s.min_latency_ms, s.max_latency_ms);
        println!("  Success Rate:     {:.1}%", s.success_rate * 100.0);
        println!("  Total Tasks:      {}", s.total_tasks);
    }
}

