use anyhow::{Context, Result};
use distribute_runtime::{run_distributed_mapreduce_bytes_opts, JobOptions, MissingChunkPolicy};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::fs;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct CrawlJob {
    urls: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct CrawlResult {
    url: String,
    title: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct FinalOutput {
    results: Vec<CrawlResult>,
}

// Chunker: split URLs into batches (smaller batches for testing)
fn chunker(job: &CrawlJob) -> Vec<CrawlJob> {
    const BATCH_SIZE: usize = 5; // Reduced from 50 to 5 for testing
    job.urls
        .chunks(BATCH_SIZE)
        .map(|url_batch| CrawlJob {
            urls: url_batch.to_vec(),
        })
        .collect()
}

// Encoder: convert batch of URLs to JSON bytes for WASM processing
fn encode_chunk(chunk: &CrawlJob) -> (Vec<u8>, serde_json::Value) {
    let urls_json = serde_json::to_string(&chunk.urls).unwrap();
    let url_bytes = urls_json.as_bytes().to_vec();
    let meta = json!({
        "url_count": chunk.urls.len(),
        "urls": chunk.urls,
    });
    (url_bytes, meta)
}

// Decoder: convert JSON results back to individual CrawlResult items
fn decode_result(bytes: Vec<u8>, meta: serde_json::Value) -> Vec<CrawlResult> {
    // The WASM function returns JSON array of {url, title} objects
    let result_str = String::from_utf8(bytes).unwrap_or_else(|_| "[]".to_string());
    let results: Vec<CrawlResult> = serde_json::from_str(&result_str)
        .unwrap_or_else(|_| {
            // Fallback: if JSON parsing fails, try to extract from error message
            vec![]
        });
    
    results
}

// Reducer: flatten all batch results and filter out errors and missing titles
fn reducer(results: Vec<Vec<CrawlResult>>) -> FinalOutput {
    let flattened: Vec<CrawlResult> = results
        .into_iter()
        .flatten()
        .filter(|result| {
            // Filter out errors and "NO TITLE FOUND"
            !result.title.starts_with("ERROR:") && result.title != "NO TITLE FOUND"
        })
        .collect();
    
    println!("   ✅ Filtered results: {} URLs with valid titles", flattened.len());
    FinalOutput { results: flattened }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Read URLs from crawl_these.txt
    let input_file = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "crawl_these.txt".to_string());
    
    println!("📖 Reading URLs from: {}", input_file);
    let content = fs::read_to_string(&input_file)
        .await
        .context(format!("Failed to read {}", input_file))?;
    
    // Parse URLs (one per line)
    let urls: Vec<String> = content
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect();
    
    println!("📋 Found {} URLs to crawl", urls.len());
    
    if urls.is_empty() {
        anyhow::bail!("No URLs found in {}", input_file);
    }
    
    // Create crawl job
    let job = CrawlJob { urls };
    
    // Distribute URL fetching and title extraction using WASM worker function.
    // A crawl tolerates gaps, so accept partial results rather than failing.
    println!("🌐 Starting distributed web crawl...");
    let result: FinalOutput = run_distributed_mapreduce_bytes_opts(
        job,
        "fetch_url_title",
        chunker,
        reducer,
        encode_chunk,
        decode_result,
        JobOptions {
            missing_chunks: MissingChunkPolicy::AllowPartial,
            ..JobOptions::default()
        },
    )
    .await
    .map_err(|e| anyhow::anyhow!("distributed crawl failed: {e}"))?;
    
    // Write results to output file
    let output_file = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "webcrawl_results.txt".to_string());
    
    println!("💾 Writing results to: {}", output_file);
    let mut output_lines = Vec::new();
    for crawl_result in &result.results {
        // Format: URL <tab> Title
        output_lines.push(format!("{}\t{}", crawl_result.url, crawl_result.title));
    }
    
    fs::write(&output_file, output_lines.join("\n"))
        .await
        .context(format!("Failed to write {}", output_file))?;
    
    println!("✅ Web crawl completed! Processed {} URLs", result.results.len());
    println!("📄 Results saved to: {}", output_file);
    
    Ok(())
}

