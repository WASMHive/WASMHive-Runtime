use distribute_runtime::ExecutionMode;
use distributed_examples::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Distributed MapReduce Master Node");
    println!("====================================");

    // Simple MapReduce Example: Sum of squares
    println!("\n📊 MapReduce Example: Sum of Squares");
    let test_data = TestData {
        numbers: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
    };

    println!("Input: {:?}", test_data.numbers);
    println!(
        "Expected (sum of squares): {}",
        test_data.numbers.iter().map(|&x| x * x).sum::<f32>()
    );

    // GPU execution
    println!("\n🎯 Running with GPU mode...");
    let gpu_result = simple_map_reduce_run_distributed(test_data.clone(), ExecutionMode::GPU).await;
    println!("GPU Result: {}", gpu_result.value);

    // CPU execution
    println!("\n🔄 Running with CPU mode...");
    let cpu_result = simple_map_reduce_run_distributed(test_data.clone(), ExecutionMode::CPU).await;
    println!("CPU Result: {}", cpu_result.value);

    Ok(())
}
