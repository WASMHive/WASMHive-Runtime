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
    let test_data = TestData {
        numbers: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
    };

    let gpu_result: TestResult =
        run_distributed_mapreduce(test_data.clone(), "gpu_map", "reduce", ExecutionMode::GPU).await;
    println!("GPU Result: {}", gpu_result.value);

    let cpu_result: TestResult =
        run_distributed_mapreduce(test_data.clone(), "cpu_map", "reduce", ExecutionMode::CPU).await;
    println!("CPU Result: {}", cpu_result.value);

    Ok(())
}
