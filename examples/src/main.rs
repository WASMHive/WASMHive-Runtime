use distribute_runtime::{run_distributed_mapreduce, ExecutionMode};
use serde::{Deserialize, Serialize};

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
    println!("🚀 Distributed MapReduce Master Node");
    println!("====================================");

    // MapReduce Example: Map function cubes each number, reduce sums them
    println!("\n📊 MapReduce Example: Sum of Cubes");
    let test_data = TestData {
        numbers: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
    };

    println!("Input: {:?}", test_data.numbers);
    println!(
        "Expected (sum of cubes): {}",
        test_data.numbers.iter().map(|&x| x * x * x).sum::<f32>()
    );

    // GPU execution using gpu_map
    println!("\n🎯 Running with GPU mode...");
    let gpu_result: TestResult = run_distributed_mapreduce(
        test_data.clone(),
        "gpu_map",
        "reduce",
        ExecutionMode::GPU
    ).await;
    println!("GPU Result: {}", gpu_result.value);

    // CPU execution using cpu_map
    println!("\n🔄 Running with CPU mode...");
    let cpu_result: TestResult = run_distributed_mapreduce(
        test_data.clone(),
        "cpu_map",
        "reduce",
        ExecutionMode::CPU
    ).await;
    println!("CPU Result: {}", cpu_result.value);

    Ok(())
}
