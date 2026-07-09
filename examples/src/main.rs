use distribute_runtime::{run_distributed_mapreduce, ExecutionMode};
use distributed_examples::{chunker, reducer, TestData, TestResult};
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let test_data = TestData {
        numbers: vec![
            100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, 900.0, 1000.0,
        ],
    };

    println!("Running distributed MapReduce computations...\n");

    // GPU computation (x squared, summed)
    let start = Instant::now();
    let gpu_result: TestResult = run_distributed_mapreduce(
        test_data.clone(),
        "gpu_map",
        chunker,
        reducer,
        ExecutionMode::GPU,
    )
    .await
    .map_err(|e| format!("distributed GPU job failed: {e}"))?;
    let gpu_duration = start.elapsed();
    println!(
        "GPU Result (sum of x^2): {} | Time: {:?}",
        gpu_result.value, gpu_duration
    );

    Ok(())
}
