use distribute_runtime::{run_distributed_mapreduce, ExecutionMode};
use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TestData {
    pub numbers: Vec<f32>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TestResult {
    pub value: f32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let test_data = TestData {
        numbers: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
    };

    println!("Running distributed MapReduce computations...\n");

    // GPU computation (x²)
    let start = Instant::now();
    let gpu_result: TestResult =
        run_distributed_mapreduce(test_data.clone(), "gpu_map", "reduce", ExecutionMode::GPU).await;
    let gpu_duration = start.elapsed();
    println!("GPU Result (x²): {} | Time: {:?}", gpu_result.value, gpu_duration);

    // CPU computation (identity)
    let start = Instant::now();
    let cpu_result: TestResult =
        run_distributed_mapreduce(test_data.clone(), "cpu_map", "reduce", ExecutionMode::CPU).await;
    let cpu_duration = start.elapsed();
    println!("CPU Result (x): {} | Time: {:?}", cpu_result.value, cpu_duration);

    // CPU1 computation (x³)
    let start = Instant::now();
    let cpu1_result: TestResult =
        run_distributed_mapreduce(test_data.clone(), "cpu1_map", "reduce", ExecutionMode::CPU).await;
    let cpu1_duration = start.elapsed();
    println!("CPU1 Result (x³): {} | Time: {:?}", cpu1_result.value, cpu1_duration);

    println!("\nPerformance Summary:");
    println!("GPU (x²):  {:?}", gpu_duration);
    println!("CPU (x):   {:?}", cpu_duration);
    println!("CPU1 (x³): {:?}", cpu1_duration);

    Ok(())
}
