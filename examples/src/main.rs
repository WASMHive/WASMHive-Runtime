use distribute_runtime::{run_distributed_mapreduce, ExecutionMode};
use distributed_examples::{chunker, reducer, TestData, TestResult};
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let test_data = TestData {
        numbers: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
    };

    println!("Running distributed MapReduce computations...\n");

    // GPU computation (x²)
    let start = Instant::now();
    let gpu_result: TestResult = run_distributed_mapreduce(
        test_data.clone(),
        "gpu_map",
        chunker,
        reducer,
        ExecutionMode::GPU,
    )
    .await;
    let gpu_duration = start.elapsed();
    println!(
        "GPU Result (x²): {} | Time: {:?}",
        gpu_result.value, gpu_duration
    );

    Ok(())
}
