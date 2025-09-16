use distribute_macro::distribute;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TestData {
    pub numbers: Vec<f32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TestResult {
    pub value: f32,
}

// Simple MapReduce example for distributed computing
#[distribute(test_chunker, test_reducer)]
pub fn simple_map_reduce(input: TestData) -> TestResult {
    // Map: square each number, then sum (reduce)
    let result = input.numbers.iter().map(|&x| x * x).sum();
    TestResult { value: result }
}

// Chunking function for distributed processing
pub fn test_chunker(data: &TestData) -> Vec<TestData> {
    let chunk_size = (data.numbers.len() / 4).max(1);
    data.numbers
        .chunks(chunk_size)
        .map(|chunk| TestData {
            numbers: chunk.to_vec(),
        })
        .collect()
}

// Reducer function to combine results
pub fn test_reducer(results: Vec<TestResult>) -> TestResult {
    let total_sum = results.iter().map(|r| r.value).sum();
    TestResult { value: total_sum }
}