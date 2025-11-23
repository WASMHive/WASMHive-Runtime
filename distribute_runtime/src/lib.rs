use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::process::Command;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use webrtc::api::APIBuilder;
use webrtc::data_channel::RTCDataChannel;
use webrtc::data_channel::data_channel_state::RTCDataChannelState;
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

#[derive(Debug, Clone)]
pub enum ExecutionMode {
    CPU,
    GPU,
}

// WebSocket signaling messages
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
enum SignalingMessage {
    #[serde(rename = "welcome")]
    Welcome {
        id: String,
        peers: Option<Vec<String>>,
    },
    #[serde(rename = "peerList")]
    PeerList { peers: Vec<String> },
    #[serde(rename = "offer")]
    Offer {
        from: String,
        to: String,
        offer: serde_json::Value,
    },
    #[serde(rename = "answer")]
    Answer {
        from: String,
        to: String,
        answer: serde_json::Value,
    },
    #[serde(rename = "candidate")]
    Candidate {
        from: String,
        to: String,
        candidate: serde_json::Value,
    },
}

// Compute task message
#[derive(Serialize, Deserialize, Debug, Clone)]
struct ComputeTask {
    task_id: String,
    wasm_module: String, // base64 encoded WASM
    js_glue: String,
    data_chunk: Vec<f32>,
    map_function: String, // "cpu_map" or "gpu_map"
}

// Byte-oriented compute task (for arbitrary binary payloads like video frames)
#[derive(Serialize, Deserialize, Debug, Clone)]
struct ComputeTaskBytes {
    task_id: String,
    wasm_module: String, // base64 encoded WASM
    js_glue: String,
    data_chunk_b64: String, // base64-encoded bytes
    map_function: String,   // e.g., "grayscale_frame"
    #[serde(skip_serializing_if = "Option::is_none")]
    meta: Option<serde_json::Value>, // optional metadata (e.g., frame_index, width, height)
}

// Chunk message for large data transfers
#[derive(Serialize, Deserialize, Debug)]
struct ChunkMessage {
    chunk_id: String,      // Unique ID for this chunked message
    chunk_index: usize,    // Index of this chunk (0-based)
    total_chunks: usize,   // Total number of chunks
    data: String,          // Base64 encoded chunk data
}

// Compute result message
#[derive(Serialize, Deserialize, Debug)]
struct ComputeResult {
    task_id: String,
    result: Vec<f32>,
    worker_id: String,
}

// Variant result type to support both numeric and binary results
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum WorkerResult {
    Floats { task_id: String, result: Vec<f32>, worker_id: String },
    Bytes { task_id: String, result_b64: String, worker_id: String, #[serde(default)] meta: Option<serde_json::Value> },
}

// Task tracking for fault tolerance
#[derive(Debug, Clone)]
struct PendingTask {
    task_id: String,
    worker_id: String,
    task: ComputeTask,
    sent_at: std::time::Instant,
    retry_count: u32,
}

#[derive(Debug, Clone)]
struct PendingTaskBytes {
    task_id: String,
    worker_id: String,
    task: ComputeTaskBytes,
    sent_at: std::time::Instant,
    retry_count: u32,
}

// Helper struct for parsing test data
#[derive(Serialize, Deserialize)]
struct TestDataForCalculation {
    numbers: Vec<f32>,
}

pub trait ComputeFunction<Input, Output> {
    fn call(&self, input: Input) -> Output;
}

impl<F, Input, Output> ComputeFunction<Input, Output> for F
where
    F: Fn(Input) -> Output,
{
    fn call(&self, input: Input) -> Output {
        self(input)
    }
}

pub async fn run_distributed_impl_with_code<F, Input, Output, ChunkFn, ReduceFn>(
    _compute_fn: F,
    input: Input,
    chunker: ChunkFn,
    reducer: ReduceFn,
    execution_mode: ExecutionMode,
    _function_body: &str,
    fn_name: &str,
) -> Output
where
    F: ComputeFunction<Input, Output> + Send + Sync + 'static,
    Input: Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static,
    Output: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    ChunkFn: Fn(&Input) -> Vec<Input> + Send + Sync,
    ReduceFn: Fn(Vec<Output>) -> Output + Send + Sync,
{
    println!("🌐 Executing distributed map-reduce using examples WASM...");

    // Compile WASM from examples directory
    match compile_examples_to_wasm(fn_name).await {
        Ok((wasm_bytes, js_glue)) => {
            println!(
                "📦 Successfully compiled examples to WASM ({} bytes)",
                wasm_bytes.len()
            );

            // Use provided chunker to split input into chunks consumable by the WASM module
            let input_chunks: Vec<Input> = chunker(&input);

            // Convert chunks into Vec<f32> that workers expect (best-effort extraction)
            fn extract_numbers_from_value(value: &serde_json::Value) -> Option<Vec<f32>> {
                if let Some(arr) = value.as_array() {
                    let mut out = Vec::with_capacity(arr.len());
                    for v in arr {
                        if let Some(n) = v.as_f64() {
                            out.push(n as f32);
                        } else {
                            return None;
                        }
                    }
                    return Some(out);
                }
                if let Some(obj) = value.as_object() {
                    if let Some(numbers) = obj.get("numbers") {
                        return extract_numbers_from_value(numbers);
                    }
                }
                None
            }

            let mut data_chunks: Vec<Vec<f32>> = Vec::new();
            for chunk in input_chunks.iter() {
                match serde_json::to_value(chunk) {
                    Ok(val) => {
                        if let Some(nums) = extract_numbers_from_value(&val) {
                            data_chunks.push(nums);
                        } else {
                            println!("⚠️ Chunk could not be converted into Vec<f32>; skipping chunk: {:?}", val);
                        }
                    }
                    Err(e) => {
                        println!("⚠️ Failed to serialize chunk; skipping. Error: {}", e);
                    }
                }
            }

            if data_chunks.is_empty() {
                println!("⚠️ No usable chunks produced by chunker; returning reducer on empty set");
                return reducer(Vec::new());
            }

            // Execute distributed map using precomputed chunks and collect mapped values
            let mapped_values_json = execute_distributed_map_reduce_with_chunks(
                data_chunks,
                &execution_mode,
                &wasm_bytes,
                &js_glue,
                fn_name,
            )
            .await;

            // Parse collected mapped values (floats) and convert to Output, then apply reducer
            match serde_json::from_str::<Vec<f32>>(&mapped_values_json) {
                Ok(float_values) => {
                    let mut converted: Vec<Output> = Vec::with_capacity(float_values.len());
                    for v in float_values {
                        // Try direct conversion from float
                        let direct: Result<Output, _> = serde_json::from_value(serde_json::Value::from(v));
                        if let Ok(o) = direct {
                            converted.push(o);
                            continue;
                        }

                        // Try common wrapper {"value": v}
                        let wrapped = serde_json::json!({"value": v});
                        match serde_json::from_value::<Output>(wrapped) {
                            Ok(o) => converted.push(o),
                            Err(_) => {
                                println!("⚠️ Unable to convert mapped float {} into Output; skipping", v);
                            }
                        }
                    }
                    reducer(converted)
                }
                Err(e) => {
                    println!("⚠️ Failed to parse collected mapped values as floats: {}", e);
                    reducer(Vec::new())
                }
            }
        }
        Err(e) => {
            println!("⚠️ WASM compilation failed: {}", e);
            // Return a default result
            reducer(Vec::new())
        }
    }
}

async fn compile_examples_to_wasm(
    _fn_name: &str,
) -> Result<(Vec<u8>, String), Box<dyn std::error::Error>> {
    println!("🔧 Compiling examples to WASM for distributed execution...");

    // Resolve the examples directory robustly
    use std::path::PathBuf;
    let current_dir = std::env::current_dir()?;
    // Allow override via env
    if let Ok(override_dir) = std::env::var("W3DGE_WASM_EXAMPLES_DIR") {
        let p = PathBuf::from(override_dir);
        if p.exists() {
            println!("📁 Using examples directory (env): {}", p.display());
            // proceed with p
            // Compile using wasm-pack
            let output = Command::new("wasm-pack")
                .args(&["build", "--target", "web", "--out-dir", "pkg"])
                .current_dir(&p)
                .output()?;
            if !output.status.success() {
                let error_msg = String::from_utf8_lossy(&output.stderr);
                return Err(format!("WASM compilation failed: {}", error_msg).into());
            }
            let wasm_file_path = p.join("pkg").join("distributed_examples_bg.wasm");
            let js_file_path = p.join("pkg").join("distributed_examples.js");
            let wasm_bytes = fs::read(&wasm_file_path)?;
            let js_glue = fs::read_to_string(&js_file_path)?;
            println!("📦 WASM module size: {} bytes", wasm_bytes.len());
            println!("📜 JS glue size: {} bytes", js_glue.len());
            return Ok((wasm_bytes, js_glue));
        }
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir.parent().unwrap_or(&manifest_dir);

    let candidates = [
        current_dir.clone(),
        current_dir.join("examples"),
        current_dir.parent().unwrap_or(&current_dir).join("examples"),
        repo_root.join("examples"),
    ];

    let examples_dir = candidates
        .iter()
        .find(|p| p.file_name().and_then(|n| n.to_str()) == Some("examples") && p.exists())
        .cloned()
        .ok_or_else(|| format!(
            "Unable to locate examples directory. Tried: {}",
            candidates
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ))?;

    println!("📁 Using examples directory: {}", examples_dir.display());

    // Compile using wasm-pack
    let output = Command::new("wasm-pack")
        .args(&["build", "--target", "web", "--out-dir", "pkg"])
        .current_dir(&examples_dir)
        .output()?;

    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        return Err(format!("WASM compilation failed: {}", error_msg).into());
    }

    println!("✅ WASM compilation successful");

    // Read the compiled WASM file and JS glue
    let wasm_file_path = examples_dir
        .join("pkg")
        .join("distributed_examples_bg.wasm");
    let js_file_path = examples_dir.join("pkg").join("distributed_examples.js");

    let wasm_bytes = fs::read(&wasm_file_path)?;
    let js_glue = fs::read_to_string(&js_file_path)?;

    println!("📦 WASM module size: {} bytes", wasm_bytes.len());
    println!("📜 JS glue size: {} bytes", js_glue.len());

    Ok((wasm_bytes, js_glue))
}

async fn execute_distributed_map_reduce_with_chunks(
    data_chunks: Vec<Vec<f32>>,
    execution_mode: &ExecutionMode,
    wasm_bytes: &[u8],
    js_glue: &str,
    _fn_name: &str,
) -> String {
    println!("🌐 Starting distributed map execution with precomputed chunks...");

    let mut distributor = match DistributedCompute::new().await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Failed to create distributed compute: {}", e);
            return String::from("[]");
        }
    };

    match distributor
        .execute_map_with_chunks(data_chunks, execution_mode, wasm_bytes, js_glue)
        .await
    {
        Ok(collected_values) => match serde_json::to_string(&collected_values) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Failed to serialize collected values: {}", e);
                String::from("[]")
            }
        },
        Err(e) => {
            eprintln!("Distributed map execution failed: {}", e);
            String::from("[]")
        }
    }
}

// Distributed compute structure for managing WebRTC connections to workers
pub struct DistributedCompute {
    ws_url: String,
    my_id: Option<String>,
    workers: Arc<Mutex<Vec<String>>>,
    peer_connections: Arc<Mutex<HashMap<String, Arc<webrtc::peer_connection::RTCPeerConnection>>>>,
    data_channels: Arc<Mutex<HashMap<String, Arc<RTCDataChannel>>>>,
    result_receiver: Option<mpsc::Receiver<WorkerResult>>,
    result_sender: mpsc::Sender<WorkerResult>,
    is_connected: bool,
    ws_sender: Option<
        Arc<
            Mutex<
                futures_util::stream::SplitSink<
                    tokio_tungstenite::WebSocketStream<
                        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
                    >,
                    Message,
                >,
            >,
        >,
    >,
    // Fault tolerance: track pending tasks
    pending_tasks: Arc<Mutex<HashMap<String, PendingTask>>>,
    pending_tasks_bytes: Arc<Mutex<HashMap<String, PendingTaskBytes>>>,
    // Track failed workers
    failed_workers: Arc<Mutex<HashMap<String, std::time::Instant>>>,
}

impl DistributedCompute {
    async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (result_sender, result_receiver) = mpsc::channel(100);

        Ok(Self {
            ws_url: "ws://localhost:3000".to_string(),
            my_id: None,
            workers: Arc::new(Mutex::new(Vec::new())),
            peer_connections: Arc::new(Mutex::new(HashMap::new())),
            data_channels: Arc::new(Mutex::new(HashMap::new())),
            result_receiver: Some(result_receiver),
            result_sender,
            is_connected: false,
            ws_sender: None,
            pending_tasks: Arc::new(Mutex::new(HashMap::new())),
            pending_tasks_bytes: Arc::new(Mutex::new(HashMap::new())),
            failed_workers: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    // Helper function to send message with automatic chunking if needed
    async fn send_message_chunked(
        channel: &Arc<RTCDataChannel>,
        message: &str,
        chunk_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        const MAX_CHUNK_SIZE: usize = 30_000; // 30KB chunks (after base64 encoding + JSON overhead will be ~40KB)

        if message.len() <= MAX_CHUNK_SIZE {
            // Message is small enough, send directly
            channel.send_text(message).await?;
            println!("   📤 Sent message directly ({} bytes)", message.len());
            return Ok(());
        }

        // Message is too large, split into chunks
        let message_bytes = message.as_bytes();
        let total_chunks = (message_bytes.len() + MAX_CHUNK_SIZE - 1) / MAX_CHUNK_SIZE;

        println!(
            "   📦 Splitting large message into {} chunks ({} bytes total)",
            total_chunks,
            message_bytes.len()
        );

        for chunk_index in 0..total_chunks {
            let start = chunk_index * MAX_CHUNK_SIZE;
            let end = (start + MAX_CHUNK_SIZE).min(message_bytes.len());
            let chunk_data = &message_bytes[start..end];

            // Encode chunk as base64
            let chunk_b64 = BASE64.encode(chunk_data);

            let chunk_message = ChunkMessage {
                chunk_id: chunk_id.to_string(),
                chunk_index,
                total_chunks,
                data: chunk_b64,
            };

            let chunk_json = serde_json::to_string(&chunk_message)?;
            channel.send_text(&chunk_json).await?;

            println!(
                "   📤 Sent chunk {}/{} ({} bytes)",
                chunk_index + 1,
                total_chunks,
                chunk_data.len()
            );

            // Small delay between chunks to avoid overwhelming the channel
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        Ok(())
    }

    // Fault tolerance: Check if a worker is available and healthy
    async fn is_worker_available(&self, worker_id: &str) -> bool {
        let data_channels = self.data_channels.lock().await;
        if let Some(channel) = data_channels.get(worker_id) {
            // Check if channel is open
            matches!(channel.ready_state(), RTCDataChannelState::Open)
        } else {
            false
        }
    }

    // Fault tolerance: Get available workers (excluding failed ones)
    async fn get_available_workers(&self) -> Vec<String> {
        let data_channels = self.data_channels.lock().await;
        let failed_workers = self.failed_workers.lock().await;
        
        data_channels
            .iter()
            .filter(|(worker_id, channel)| {
                // Check if worker is not marked as failed and channel is open
                !failed_workers.contains_key(*worker_id) &&
                matches!(channel.ready_state(), RTCDataChannelState::Open)
            })
            .map(|(worker_id, _)| worker_id.clone())
            .collect()
    }

    // Fault tolerance: Mark a worker as failed
    async fn mark_worker_failed(&self, worker_id: &str) {
        let mut failed_workers = self.failed_workers.lock().await;
        failed_workers.insert(worker_id.to_string(), std::time::Instant::now());
        println!("⚠️  Marked worker {} as failed", worker_id);
    }

    // Fault tolerance: Reassign a failed task to a new worker
    async fn reassign_task(
        &self,
        pending_task: &PendingTask,
        available_workers: &[String],
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        if available_workers.is_empty() {
            return Err("No available workers for reassignment".into());
        }

        // Find a worker that's not the failed one
        let new_worker = available_workers
            .iter()
            .find(|&w| w != &pending_task.worker_id)
            .or_else(|| available_workers.first())
            .ok_or("No suitable worker found")?;

        let data_channels = self.data_channels.lock().await;
        if let Some(channel) = data_channels.get(new_worker) {
            let task_json = serde_json::to_string(&pending_task.task)?;
            match Self::send_message_chunked(channel, &task_json, &pending_task.task.task_id).await {
                Ok(_) => {
                    println!(
                        "   🔄 Reassigned task {} from {} to {} (retry {})",
                        pending_task.task.task_id,
                        pending_task.worker_id,
                        new_worker,
                        pending_task.retry_count + 1
                    );
                    Ok(Some(new_worker.clone()))
                }
                Err(e) => {
                    println!("   ❌ Failed to reassign task to {}: {}", new_worker, e);
                    Err(e)
                }
            }
        } else {
            Err(format!("No data channel for worker: {}", new_worker).into())
        }
    }

    // Fault tolerance: Reassign a failed byte task to a new worker
    async fn reassign_task_bytes(
        &self,
        pending_task: &PendingTaskBytes,
        available_workers: &[String],
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        if available_workers.is_empty() {
            return Err("No available workers for reassignment".into());
        }

        let new_worker = available_workers
            .iter()
            .find(|&w| w != &pending_task.worker_id)
            .or_else(|| available_workers.first())
            .ok_or("No suitable worker found")?;

        let data_channels = self.data_channels.lock().await;
        if let Some(channel) = data_channels.get(new_worker) {
            let task_json = serde_json::to_string(&pending_task.task)?;
            match Self::send_message_chunked(channel, &task_json, &pending_task.task.task_id).await {
                Ok(_) => {
                    println!(
                        "   🔄 Reassigned byte task {} from {} to {} (retry {})",
                        pending_task.task.task_id,
                        pending_task.worker_id,
                        new_worker,
                        pending_task.retry_count + 1
                    );
                    Ok(Some(new_worker.clone()))
                }
                Err(e) => {
                    println!("   ❌ Failed to reassign byte task to {}: {}", new_worker, e);
                    Err(e)
                }
            }
        } else {
            Err(format!("No data channel for worker: {}", new_worker).into())
        }
    }

    // Fault tolerance: Check for and reassign timed-out tasks
    async fn check_and_reassign_timed_out_tasks(&self) {
        const TASK_TIMEOUT_SECS: u64 = 30; // 30 seconds timeout
        const MAX_RETRIES: u32 = 3;

        let available_workers = self.get_available_workers().await;
        if available_workers.is_empty() {
            return;
        }

        // Check float tasks
        let pending_tasks = self.pending_tasks.lock().await;
        let now = std::time::Instant::now();
        let mut to_reassign: Vec<(String, PendingTask)> = Vec::new();

        for (task_id, pending_task) in pending_tasks.iter() {
            if pending_task.retry_count >= MAX_RETRIES {
                println!("   ⚠️  Task {} exceeded max retries, marking worker {} as failed", task_id, pending_task.worker_id);
                self.mark_worker_failed(&pending_task.worker_id).await;
                continue;
            }

            if now.duration_since(pending_task.sent_at).as_secs() > TASK_TIMEOUT_SECS {
                // Check if worker is still available
                if !self.is_worker_available(&pending_task.worker_id).await {
                    to_reassign.push((task_id.clone(), pending_task.clone()));
                }
            }
        }

        drop(pending_tasks);

        // Reassign timed-out tasks
        for (task_id, mut pending_task) in to_reassign {
            match self.reassign_task(&pending_task, &available_workers).await {
                Ok(Some(new_worker)) => {
                    pending_task.worker_id = new_worker;
                    pending_task.sent_at = std::time::Instant::now();
                    pending_task.retry_count += 1;
                    let mut pending_tasks = self.pending_tasks.lock().await;
                    pending_tasks.insert(task_id, pending_task);
                }
                Ok(None) | Err(_) => {
                    // Mark worker as failed if reassignment fails
                    self.mark_worker_failed(&pending_task.worker_id).await;
                }
            }
        }

        // Check byte tasks
        let pending_tasks_bytes = self.pending_tasks_bytes.lock().await;
        let mut to_reassign_bytes: Vec<(String, PendingTaskBytes)> = Vec::new();

        for (task_id, pending_task) in pending_tasks_bytes.iter() {
            if pending_task.retry_count >= MAX_RETRIES {
                println!("   ⚠️  Byte task {} exceeded max retries, marking worker {} as failed", task_id, pending_task.worker_id);
                self.mark_worker_failed(&pending_task.worker_id).await;
                continue;
            }

            if now.duration_since(pending_task.sent_at).as_secs() > TASK_TIMEOUT_SECS {
                if !self.is_worker_available(&pending_task.worker_id).await {
                    to_reassign_bytes.push((task_id.clone(), pending_task.clone()));
                }
            }
        }

        drop(pending_tasks_bytes);

        for (task_id, mut pending_task) in to_reassign_bytes {
            match self.reassign_task_bytes(&pending_task, &available_workers).await {
                Ok(Some(new_worker)) => {
                    pending_task.worker_id = new_worker;
                    pending_task.sent_at = std::time::Instant::now();
                    pending_task.retry_count += 1;
                    let mut pending_tasks_bytes = self.pending_tasks_bytes.lock().await;
                    pending_tasks_bytes.insert(task_id, pending_task);
                }
                Ok(None) | Err(_) => {
                    self.mark_worker_failed(&pending_task.worker_id).await;
                }
            }
        }
    }

    async fn execute_map_reduce(
        &mut self,
        input_json: &str,
        execution_mode: &ExecutionMode,
        wasm_bytes: &[u8],
        js_glue: &str,
        _fn_name: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // Parse input
        let input: TestDataForCalculation = serde_json::from_str(input_json)?;

        // Connect to signaling server
        self.connect_to_signaling_server().await?;
        self.is_connected = true;

        // Wait for initial worker discovery
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        let workers_list = {
            let workers = self.workers.lock().await;
            workers.clone()
        };

        println!("🔍 Current workers list: {:?}", workers_list);

        if workers_list.is_empty() {
            return Err("No workers available for computation".into());
        }

        // Wait for data channels to be established
        println!("⏳ Waiting for data channels to be established...");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        // Filter workers to only include those with actual data channels
        let connected_workers = {
            let data_channels = self.data_channels.lock().await;
            workers_list
                .into_iter()
                .filter(|worker_id| data_channels.contains_key(worker_id))
                .collect::<Vec<_>>()
        };

        if connected_workers.is_empty() {
            return Err("No workers with data channels available for computation".into());
        }

        // Distribute work to connected workers
        let max_chunk_size = 1000; // Conservative limit for WebRTC data channels
        let desired_chunk_size = input.numbers.len() / connected_workers.len().max(1);
        let chunk_size = desired_chunk_size.min(max_chunk_size);
        let mut tasks = Vec::new();

        // Encode WASM as base64
        let wasm_b64 = BASE64.encode(wasm_bytes);

        for (i, worker_id) in connected_workers.iter().enumerate() {
            let start_idx = i * chunk_size;
            let end_idx = if i == connected_workers.len() - 1 {
                input.numbers.len() // Last worker gets remainder
            } else {
                (start_idx + chunk_size).min(input.numbers.len())
            };

            if start_idx < input.numbers.len() {
                let data_chunk = input.numbers[start_idx..end_idx].to_vec();
                let task = ComputeTask {
                    task_id: format!("task_{}_{}", chrono::Utc::now().timestamp_millis(), i),
                    wasm_module: wasm_b64.clone(),
                    js_glue: js_glue.to_string(),
                    data_chunk,
                    map_function: match execution_mode {
                        ExecutionMode::CPU => "cpu_map".to_string(),
                        ExecutionMode::GPU => "gpu_map".to_string(),
                    },
                };

                tasks.push((worker_id.clone(), task));
            }
        }

        // Send tasks to workers via WebRTC data channels
        let mut sent_tasks = 0;
        println!(
            "📊 Distributing to {} connected workers: {:?}",
            connected_workers.len(),
            connected_workers
        );

        for (worker_id, task) in &tasks {
            let data_channels = self.data_channels.lock().await;
            println!("🔍 Checking data channels. Total: {}, Looking for: {}", data_channels.len(), worker_id);
            if let Some(channel) = data_channels.get(worker_id) {
                println!(
                    "🔍 Attempting to send task to {}, channel state: {:?}",
                    worker_id,
                    channel.ready_state()
                );
                let task_json = serde_json::to_string(task).unwrap();

                // Use chunked sending for large messages
                match Self::send_message_chunked(channel, &task_json, &task.task_id).await {
                    Ok(_) => {
                        println!(
                            "   ✅ {} -> {} ({} elements)",
                            task.task_id,
                            worker_id,
                            task.data_chunk.len()
                        );
                        sent_tasks += 1;
                    }
                    Err(e) => {
                        println!("   ❌ Failed to send to {}: {}", worker_id, e);
                    }
                }
            } else {
                println!("   ⚠️  No data channel for worker: {}", worker_id);
            }
        }

        // If we successfully sent tasks, wait for results
        if sent_tasks > 0 {
            println!("⏳ Waiting for {} results from workers...", sent_tasks);

            // Wait for results with timeout
            let mut collected_results = Vec::new();
            let timeout = tokio::time::Duration::from_secs(10);
            let start_time = tokio::time::Instant::now();

            let mut results_received = 0;
            while results_received < sent_tasks && start_time.elapsed() < timeout {
                if let Some(mut receiver) = self.result_receiver.take() {
                    match tokio::time::timeout(
                        tokio::time::Duration::from_millis(1000),
                        receiver.recv(),
                    )
                    .await
                    {
                        Ok(Some(result_msg)) => {
                            match result_msg {
                                WorkerResult::Floats { task_id: _, result, worker_id } => {
                                    println!(
                                        "   📥 {} returned {} values: {:?}",
                                        worker_id,
                                        result.len(),
                                        result
                                    );
                                    collected_results.extend(result);
                                    results_received += 1;
                                }
                                WorkerResult::Bytes { task_id: _, result_b64: _, worker_id, meta: _ } => {
                                    println!("   ⚠️ Received bytes result from {} but float results expected; ignoring", worker_id);
                                }
                            }
                        }
                        Ok(None) => {
                            println!("   ⚠️  Result channel closed");
                            break; // Channel closed
                        }
                        Err(_) => {
                            println!(
                                "   ⏱️  Waiting for more results... ({}/{})",
                                results_received, sent_tasks
                            );
                        } // Timeout, continue waiting
                    }
                    self.result_receiver = Some(receiver);
                }
            }

            if results_received == sent_tasks {
                // Use local WASM reduce function to combine results
                let final_result = self.reduce_results_with_wasm(&collected_results).await?;
                println!(
                    "✅ All {} workers completed! Distributed result: {}",
                    sent_tasks, final_result
                );

                // Disconnect from signaling server after successful job completion
                self.disconnect_from_signaling_server().await?;

                return Ok(format!(r#"{{"value": {}}}"#, final_result));
            } else {
                println!(
                    "❌ Only {}/{} workers returned results - distributed execution failed",
                    results_received, sent_tasks
                );
            }
        }

        // Disconnect from signaling server after failed job
        self.disconnect_from_signaling_server().await?;

        // No fallback - remote execution must work
        Err("Failed to get results from distributed workers - remote execution required".into())
    }

    async fn execute_map_with_chunks(
        &mut self,
        data_chunks: Vec<Vec<f32>>,
        execution_mode: &ExecutionMode,
        wasm_bytes: &[u8],
        js_glue: &str,
    ) -> Result<Vec<f32>, Box<dyn std::error::Error + Send + Sync>> {
        // Connect to signaling server
        self.connect_to_signaling_server().await?;
        self.is_connected = true;

        // Wait for initial worker discovery
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        println!("⏳ Waiting for data channels to be established...");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        
        // Use available workers (fault tolerance: excludes failed workers)
        let mut available_workers = self.get_available_workers().await;
        if available_workers.is_empty() {
            return Err("No workers with data channels available for computation".into());
        }

        // Prepare tasks by round-robin assignment of chunks to workers
        let wasm_b64 = BASE64.encode(wasm_bytes);
        let mut tasks: Vec<(String, ComputeTask)> = Vec::new();
        // Track first task per worker to include WASM once (optimization)
        let mut worker_first_sent: std::collections::HashMap<String, bool> = std::collections::HashMap::new();
        for (i, chunk) in data_chunks.into_iter().enumerate() {
            let worker_idx = i % available_workers.len();
            let worker_id = available_workers[worker_idx].clone();
            // Only send WASM with the first task to each worker
            let include_wasm = !worker_first_sent.get(&worker_id).copied().unwrap_or(false);
            let map_function = match execution_mode {
                ExecutionMode::CPU => "cpu_map".to_string(),
                ExecutionMode::GPU => "gpu_map".to_string(),
            };
            let task = ComputeTask {
                task_id: format!("task_{}_{}", chrono::Utc::now().timestamp_millis(), i),
                wasm_module: if include_wasm { wasm_b64.clone() } else { String::new() },
                js_glue: if include_wasm { js_glue.to_string() } else { String::new() },
                data_chunk: chunk,
                map_function,
            };
            worker_first_sent.insert(worker_id.clone(), true);
            tasks.push((worker_id, task));
        }

        // Clear pending tasks tracking
        {
            let mut pending_tasks = self.pending_tasks.lock().await;
            pending_tasks.clear();
        }

        // Send tasks and track them
        let mut sent_tasks = 0usize;
        println!(
            "📊 Distributing {} chunks to {} available workers: {:?}",
            tasks.len(),
            available_workers.len(),
            available_workers
        );

        for (worker_id, task) in &tasks {
            let data_channels = self.data_channels.lock().await;
            if let Some(channel) = data_channels.get(worker_id) {
                println!(
                    "🔍 Attempting to send task to {}, channel state: ready",
                    worker_id
                );
                let task_json = serde_json::to_string(task).unwrap();
                match Self::send_message_chunked(channel, &task_json, &task.task_id).await {
                    Ok(_) => {
                        println!(
                            "   ✅ {} -> {} ({} elements)",
                            task.task_id,
                            worker_id,
                            task.data_chunk.len()
                        );
                        // Track the task for fault tolerance
                        let pending_task = PendingTask {
                            task_id: task.task_id.clone(),
                            worker_id: worker_id.clone(),
                            task: task.clone(),
                            sent_at: std::time::Instant::now(),
                            retry_count: 0,
                        };
                        let mut pending_tasks = self.pending_tasks.lock().await;
                        pending_tasks.insert(task.task_id.clone(), pending_task);
                        sent_tasks += 1;
                    }
                    Err(e) => {
                        println!("   ❌ Failed to send to {}: {}", worker_id, e);
                        self.mark_worker_failed(worker_id).await;
                    }
                }
            } else {
                println!("   ⚠️  No data channel for worker: {}", worker_id);
                self.mark_worker_failed(worker_id).await;
            }
        }

        // Collect results with fault tolerance
        if sent_tasks > 0 {
            println!("⏳ Waiting for {} results from workers...", sent_tasks);

            let mut collected_results: Vec<f32> = Vec::new();
            let timeout = tokio::time::Duration::from_secs(60); // Increased timeout for fault tolerance
            let start_time = tokio::time::Instant::now();
            let mut last_check_time = std::time::Instant::now();

            let mut results_received = 0usize;
            let mut received_task_ids = std::collections::HashSet::new();
            
            while results_received < sent_tasks && start_time.elapsed() < timeout {
                // Periodically check for timed-out tasks (every 5 seconds)
                if last_check_time.elapsed().as_secs() >= 5 {
                    self.check_and_reassign_timed_out_tasks().await;
                    last_check_time = std::time::Instant::now();
                }

                if let Some(mut receiver) = self.result_receiver.take() {
                    match tokio::time::timeout(
                        tokio::time::Duration::from_millis(1000),
                        receiver.recv(),
                    )
                    .await
                    {
                        Ok(Some(result_msg)) => {
                            match result_msg {
                                WorkerResult::Floats { task_id, result, worker_id } => {
                                    // If the worker returned an empty result, treat this as a failure
                                    // and attempt to reassign the task instead of counting it as success.
                                    if result.is_empty() {
                                        println!(
                                            "   ⚠️ {} returned 0 values for task {}; treating as failure and attempting reassignment",
                                            worker_id,
                                            task_id
                                        );

                                        // Take the pending task (if it still exists) so we can reassign it.
                                        let pending_task_opt = {
                                            let mut pending_tasks = self.pending_tasks.lock().await;
                                            pending_tasks.remove(&task_id)
                                        };

                                        if let Some(mut pending_task) = pending_task_opt {
                                            // Mark this worker as failed so it won't be selected again.
                                            self.mark_worker_failed(&worker_id).await;

                                            // Try to reassign to another available worker immediately.
                                            let available_workers = self.get_available_workers().await;
                                            match self.reassign_task(&pending_task, &available_workers).await {
                                                Ok(Some(new_worker)) => {
                                                    // Update tracking for the reassigned task.
                                                    pending_task.worker_id = new_worker;
                                                    pending_task.sent_at = std::time::Instant::now();
                                                    pending_task.retry_count += 1;
                                                    let mut pending_tasks = self.pending_tasks.lock().await;
                                                    pending_tasks.insert(task_id.clone(), pending_task);
                                                }
                                                Ok(None) | Err(_) => {
                                                    // If we couldn't reassign, keep the task pending so that
                                                    // the timeout-based fault tolerance can handle it later.
                                                    let mut pending_tasks = self.pending_tasks.lock().await;
                                                    pending_tasks.insert(task_id.clone(), pending_task);
                                                }
                                            }
                                        } else {
                                            println!(
                                                "   ⚠️ Received empty result for unknown or already completed task {}",
                                                task_id
                                            );
                                        }

                                        // Do NOT increment results_received or add to collected_results here.
                                    } else {
                                        // Successful result path: remove from pending and aggregate values.
                                        {
                                            let mut pending_tasks = self.pending_tasks.lock().await;
                                            pending_tasks.remove(&task_id);
                                        }

                                        if !received_task_ids.contains(&task_id) {
                                            println!(
                                                "   📥 {} returned {} values for task {}",
                                                worker_id,
                                                result.len(),
                                                task_id
                                            );
                                            collected_results.extend(result);
                                            received_task_ids.insert(task_id);
                                            results_received += 1;
                                        }
                                    }
                                }
                                WorkerResult::Bytes { task_id: _, result_b64: _, worker_id: _, meta: _ } => {
                                    println!("   ⚠️ Received bytes result on float path; ignoring");
                                }
                            }
                        }
                        Ok(None) => {
                            println!("   ⚠️  Result channel closed");
                            break;
                        }
                        Err(_) => {
                            // Timeout - continue waiting and checking for failures
                        }
                    }
                    self.result_receiver = Some(receiver);
                }
            }

            // Check if we have enough results (fault tolerance: continue with partial results)
            let pending_count = {
                let pending_tasks = self.pending_tasks.lock().await;
                pending_tasks.len()
            };

            if results_received > 0 {
                println!(
                    "✅ Received {}/{} results ({} pending, {} failed workers). Collected {} mapped values",
                    results_received,
                    sent_tasks,
                    pending_count,
                    self.failed_workers.lock().await.len(),
                    collected_results.len()
                );
                
                // If we have some results, return them (fault tolerance)
                if !collected_results.is_empty() {
                    self.disconnect_from_signaling_server().await?;
                    return Ok(collected_results);
                }
            }

            if pending_count > 0 && available_workers.is_empty() {
                println!(
                    "❌ No available workers remaining. {} tasks still pending",
                    pending_count
                );
            }
        }

        // Disconnect and error
        self.disconnect_from_signaling_server().await?;
        Err("Failed to get results from distributed workers - remote execution required".into())
    }

    // Byte-based execution path for arbitrary binary chunks (e.g., video frames)
    async fn execute_map_with_byte_chunks(
        &mut self,
        chunks_b64_with_meta: Vec<(String, Option<serde_json::Value>)>,
        wasm_bytes: &[u8],
        js_glue: &str,
        map_function_name: &str,
    ) -> Result<Vec<(Option<serde_json::Value>, String)>, Box<dyn std::error::Error + Send + Sync>> {
        // Connect to signaling server
        self.connect_to_signaling_server().await?;
        self.is_connected = true;

        // Wait for initial worker discovery
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        // Wait for data channels to be established and use actual data channel keys
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        
        // Use available workers (fault tolerance: excludes failed workers)
        let mut available_workers = self.get_available_workers().await;
        if available_workers.is_empty() {
            return Err("No workers with data channels available for computation".into());
        }

        // Prepare tasks by round-robin
        let wasm_b64 = BASE64.encode(wasm_bytes);
        let mut tasks: Vec<(String, ComputeTaskBytes)> = Vec::new();
        // Track first task per worker to include WASM once
        let mut worker_first_sent: std::collections::HashMap<String, bool> = std::collections::HashMap::new();
        for (i, (chunk_b64, meta)) in chunks_b64_with_meta.into_iter().enumerate() {
            let worker_idx = i % available_workers.len();
            let worker_id = available_workers[worker_idx].clone();
            // Skip sending WASM entirely for JS map functions
            let is_js_map = map_function_name.ends_with("_js");
            let include_wasm = !is_js_map && !worker_first_sent.get(&worker_id).copied().unwrap_or(false);
            let task = ComputeTaskBytes {
                task_id: format!("task_{}_{}", chrono::Utc::now().timestamp_millis(), i),
                wasm_module: if include_wasm { wasm_b64.clone() } else { String::new() },
                js_glue: if include_wasm { js_glue.to_string() } else { String::new() },
                data_chunk_b64: chunk_b64,
                map_function: map_function_name.to_string(),
                meta,
            };
            worker_first_sent.insert(worker_id.clone(), true);
            tasks.push((worker_id, task));
        }

        // Clear pending tasks tracking
        {
            let mut pending_tasks_bytes = self.pending_tasks_bytes.lock().await;
            pending_tasks_bytes.clear();
        }

        // Send tasks and track them
        let mut sent_tasks = 0usize;
        println!(
            "📊 Distributing {} byte-chunks to {} available workers",
            tasks.len(),
            available_workers.len()
        );
        for (worker_id, task) in &tasks {
            let data_channels = self.data_channels.lock().await;
            if let Some(channel) = data_channels.get(worker_id) {
                let task_json = serde_json::to_string(task).unwrap();
                match Self::send_message_chunked(channel, &task_json, &task.task_id).await {
                    Ok(_) => {
                        // Track the task for fault tolerance
                        let pending_task = PendingTaskBytes {
                            task_id: task.task_id.clone(),
                            worker_id: worker_id.clone(),
                            task: task.clone(),
                            sent_at: std::time::Instant::now(),
                            retry_count: 0,
                        };
                        let mut pending_tasks_bytes = self.pending_tasks_bytes.lock().await;
                        pending_tasks_bytes.insert(task.task_id.clone(), pending_task);
                        sent_tasks += 1;
                    }
                    Err(e) => {
                        println!("   ❌ Failed to send to {}: {}", worker_id, e);
                        self.mark_worker_failed(worker_id).await;
                    }
                }
            } else {
                println!("   ⚠️  No data channel for worker: {}", worker_id);
                self.mark_worker_failed(worker_id).await;
            }
        }

        // Collect results with fault tolerance
        let mut collected: Vec<(Option<serde_json::Value>, String)> = Vec::new();
        if sent_tasks > 0 {
            // Dynamic timeout: base 60s + 10s per task (minimum 60s, maximum 600s)
            let base_timeout = 60usize;
            let per_task_timeout = 10usize;
            let calculated_timeout = base_timeout + (sent_tasks * per_task_timeout);
            let timeout_secs = calculated_timeout.min(600).max(60); // Cap at 10 minutes, minimum 1 minute
            let timeout = tokio::time::Duration::from_secs(timeout_secs as u64);
            println!("⏱️  Timeout set to {} seconds for {} tasks", timeout_secs, sent_tasks);
            
            let start_time = tokio::time::Instant::now();
            let mut last_check_time = std::time::Instant::now();
            let mut results_received = 0usize;
            let mut received_task_ids = std::collections::HashSet::new();
            let mut consecutive_timeouts = 0usize;
            const MAX_CONSECUTIVE_TIMEOUTS: usize = 30; // Stop after 30 seconds of no activity
            let mut current_timeout = timeout;
            let mut timeout_extended = false;
            
            loop {
                // Check if we've received all results
                if results_received >= sent_tasks {
                    break;
                }
                
                // Check if we've exceeded the timeout
                let elapsed = start_time.elapsed();
                if elapsed >= current_timeout {
                    // Check if there are still pending tasks and available workers
                    let pending_count = {
                        let pending_tasks_bytes = self.pending_tasks_bytes.lock().await;
                        pending_tasks_bytes.len()
                    };
                    let available_workers_count = self.get_available_workers().await.len();
                    
                    if pending_count > 0 && available_workers_count > 0 && !timeout_extended {
                        // Still have pending tasks and workers - extend timeout and continue
                        println!("⏳ Timeout reached but {} tasks still pending with {} available workers. Extending timeout by 60s...", pending_count, available_workers_count);
                        current_timeout = current_timeout + tokio::time::Duration::from_secs(60);
                        timeout_extended = true;
                        // Continue waiting with extended timeout (don't break)
                    } else {
                        // Extended timeout also reached or no pending tasks/workers
                        if pending_count > 0 {
                            println!("⚠️  Timeout reached. {} tasks still pending. Stopping with partial results.", pending_count);
                        } else {
                            println!("⏱️  Timeout reached. No pending tasks.");
                        }
                        break;
                    }
                }
                
                // Periodically check for timed-out tasks (every 5 seconds)
                if last_check_time.elapsed().as_secs() >= 5 {
                    self.check_and_reassign_timed_out_tasks().await;
                    last_check_time = std::time::Instant::now();
                }

                if let Some(mut receiver) = self.result_receiver.take() {
                    match tokio::time::timeout(
                        tokio::time::Duration::from_millis(1000),
                        receiver.recv(),
                    )
                    .await
                    {
                        Ok(Some(result_msg)) => {
                            consecutive_timeouts = 0; // Reset timeout counter on successful receive
                            match result_msg {
                                WorkerResult::Bytes { task_id, result_b64, worker_id: _, meta } => {
                                    // Remove from pending tasks
                                    {
                                        let mut pending_tasks_bytes = self.pending_tasks_bytes.lock().await;
                                        pending_tasks_bytes.remove(&task_id);
                                    }
                                    
                                    if !received_task_ids.contains(&task_id) {
                                        collected.push((meta, result_b64));
                                        received_task_ids.insert(task_id);
                                        results_received += 1;
                                        if results_received % 10 == 0 {
                                            println!("   📊 Progress: {}/{} results received", results_received, sent_tasks);
                                        }
                                    }
                                }
                                WorkerResult::Floats { .. } => {}
                            }
                        }
                        Ok(None) => { 
                            println!("   ⚠️  Result channel closed");
                            break; 
                        }
                        Err(_) => {
                            // Timeout on receive - increment counter
                            consecutive_timeouts += 1;
                            if consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS {
                                println!("   ⚠️  No results received for {} seconds. Stopping wait loop.", MAX_CONSECUTIVE_TIMEOUTS);
                                break;
                            }
                        }
                    }
                    self.result_receiver = Some(receiver);
                } else {
                    // No receiver available - wait a bit and retry
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }

            // Check if we have enough results (fault tolerance: continue with partial results)
            let pending_count = {
                let pending_tasks_bytes = self.pending_tasks_bytes.lock().await;
                pending_tasks_bytes.len()
            };

            if results_received > 0 || !collected.is_empty() {
                println!(
                    "✅ Received {}/{} byte results ({} pending, {} failed workers)",
                    results_received,
                    sent_tasks,
                    pending_count,
                    self.failed_workers.lock().await.len()
                );
                self.disconnect_from_signaling_server().await?;
                return Ok(collected);
            }
        }

        self.disconnect_from_signaling_server().await?;
        Err("Failed to get results from distributed workers - remote execution required".into())
    }

    async fn reduce_results_with_wasm(
        &self,
        results: &[f32],
    ) -> Result<f32, Box<dyn std::error::Error + Send + Sync>> {
        println!(
            "🔧 Using local WASM reduce function to combine {} values",
            results.len()
        );

        if results.is_empty() {
            return Ok(0.0);
        }

        // For now, use simple sum reduction
        // TODO: Load and execute the actual WASM reduce function
        let total = results.iter().sum();
        println!("📊 Reduce operation completed: {}", total);

        Ok(total)
    }

    async fn connect_to_signaling_server(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = url::Url::parse(&self.ws_url)?;
        let (ws_stream, _) = connect_async(url).await?;
        let (ws_sender, mut ws_receiver) = ws_stream.split();

        let ws_sender = Arc::new(Mutex::new(ws_sender));

        // Store ws_sender for cleanup
        self.ws_sender = Some(ws_sender.clone());

        // Register as master node
        {
            let mut sender = ws_sender.lock().await;
            let register_msg = serde_json::json!({
                "type": "registerMaster"
            });
            sender.send(Message::Text(register_msg.to_string())).await?;
        }

        // Handle WebSocket messages
        let workers_arc = self.workers.clone();
        let my_id_arc = Arc::new(Mutex::new(self.my_id.clone()));
        let peer_connections_arc = self.peer_connections.clone();
        let data_channels_arc = self.data_channels.clone();
        let result_sender = self.result_sender.clone();
        let failed_workers_arc = self.failed_workers.clone();

        tokio::spawn(async move {
            while let Some(msg) = ws_receiver.next().await {
                if let Ok(Message::Text(text)) = msg {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                        if let Some(msg_type) = parsed.get("type").and_then(|v| v.as_str()) {
                            match msg_type {
                                "welcome" => {
                                    if let Some(id) = parsed.get("id").and_then(|v| v.as_str()) {
                                        let mut my_id = my_id_arc.lock().await;
                                        *my_id = Some(id.to_string());

                                        if let Some(peers) =
                                            parsed.get("peers").and_then(|v| v.as_array())
                                        {
                                            let mut workers = workers_arc.lock().await;
                                            *workers = peers
                                                .iter()
                                                .filter_map(|p| p.as_str())
                                                .filter(|&p| p != id)
                                                .map(|s| s.to_string())
                                                .collect();
                                        }
                                    }
                                }
                                "peerList" => {
                                    if let Some(peers) =
                                        parsed.get("peers").and_then(|v| v.as_array())
                                    {
                                        let my_id = my_id_arc.lock().await;
                                        let current_id =
                                            my_id.as_ref().map(|s| s.as_str()).unwrap_or("");

                                        let mut workers = workers_arc.lock().await;
                                        *workers = peers
                                            .iter()
                                            .filter_map(|p| p.as_str())
                                            .filter(|&p| p != current_id)
                                            .map(|s| s.to_string())
                                            .collect();
                                    }
                                }
                                "offer" => {
                                    // Handle offer from worker - create answer
                                    if let (Some(from), Some(to), Some(offer)) = (
                                        parsed.get("from").and_then(|v| v.as_str()),
                                        parsed.get("to").and_then(|v| v.as_str()),
                                        parsed.get("offer"),
                                    ) {
                                        let my_id = my_id_arc.lock().await;
                                        if let Some(ref current_id) = *my_id {
                                            if to == current_id {
                                                Self::handle_offer_from_worker(
                                                    from.to_string(),
                                                    offer.clone(),
                                                    ws_sender.clone(),
                                                    current_id.clone(),
                                                    peer_connections_arc.clone(),
                                                    data_channels_arc.clone(),
                                                    result_sender.clone(),
                                                    failed_workers_arc.clone(),
                                                )
                                                .await;
                                            }
                                        }
                                    }
                                }
                                "answer" => {
                                    // Handle answer from worker
                                    if let (Some(from), Some(answer)) = (
                                        parsed.get("from").and_then(|v| v.as_str()),
                                        parsed.get("answer"),
                                    ) {
                                        println!("📨 Received answer from worker: {}", from);
                                        let peer_connections = peer_connections_arc.lock().await;
                                        if let Some(pc) = peer_connections.get(from) {
                                            if let Some(sdp) =
                                                answer.get("sdp").and_then(|v| v.as_str())
                                            {
                                                let answer_desc =
                                                    RTCSessionDescription::answer(sdp.to_string())
                                                        .unwrap();
                                                let _ =
                                                    pc.set_remote_description(answer_desc).await;
                                            }
                                        }
                                    }
                                }
                                "candidate" => {
                                    // Handle ICE candidate from worker
                                    if let (Some(from), Some(candidate)) = (
                                        parsed.get("from").and_then(|v| v.as_str()),
                                        parsed.get("candidate"),
                                    ) {
                                        let peer_connections = peer_connections_arc.lock().await;
                                        if let Some(pc) = peer_connections.get(from) {
                                            if let Some(candidate_str) =
                                                candidate.get("candidate").and_then(|v| v.as_str())
                                            {
                                                let ice_candidate_init = RTCIceCandidateInit {
                                                    candidate: candidate_str.to_string(),
                                                    sdp_mid: Some("0".to_string()),
                                                    sdp_mline_index: Some(0),
                                                    username_fragment: None,
                                                };
                                                let _ =
                                                    pc.add_ice_candidate(ice_candidate_init).await;
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn handle_offer_from_worker(
        worker_id: String,
        offer: serde_json::Value,
        ws_sender: Arc<
            Mutex<
                futures_util::stream::SplitSink<
                    tokio_tungstenite::WebSocketStream<
                        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
                    >,
                    Message,
                >,
            >,
        >,
        my_id: String,
        peer_connections_arc: Arc<
            Mutex<HashMap<String, Arc<webrtc::peer_connection::RTCPeerConnection>>>,
        >,
        data_channels_arc: Arc<Mutex<HashMap<String, Arc<RTCDataChannel>>>>,
        result_sender: mpsc::Sender<WorkerResult>,
        failed_workers_arc: Arc<Mutex<HashMap<String, std::time::Instant>>>,
    ) {
        let worker_id_clone = worker_id.clone();
        let worker_id_clone2 = worker_id.clone();
        let worker_id_clone3 = worker_id.clone();

        // Create WebRTC peer connection for this worker
        let config = RTCConfiguration {
            ice_servers: vec![RTCIceServer {
                urls: vec!["stun:stun.l.google.com:19302".to_owned()],
                ..Default::default()
            }],
            ..Default::default()
        };

        let api = APIBuilder::new().build();
        if let Ok(peer_connection) = api.new_peer_connection(config).await {
            let pc = Arc::new(peer_connection);

            // Fault tolerance: Monitor connection state changes
            let worker_id_for_state = worker_id.clone();
            let failed_workers_arc_clone = failed_workers_arc.clone();
            pc.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
                let worker_id = worker_id_for_state.clone();
                let failed_workers = failed_workers_arc_clone.clone();
                println!("🔍 Connection state changed for worker {}: {:?}", worker_id, s);
                
                // Mark worker as failed if connection is closed/failed/disconnected
                if matches!(s, 
                    RTCPeerConnectionState::Closed |
                    RTCPeerConnectionState::Failed |
                    RTCPeerConnectionState::Disconnected
                ) {
                    let rt = tokio::runtime::Handle::current();
                    rt.spawn(async move {
                        let mut failed_workers = failed_workers.lock().await;
                        failed_workers.insert(worker_id.clone(), std::time::Instant::now());
                        println!("⚠️  Worker {} connection lost, marked as failed", worker_id);
                    });
                }
                Box::pin(async {})
            }));

            // Set up ICE candidate handling
            let ws_sender_clone = ws_sender.clone();
            let my_id_clone = my_id.clone();

            pc.on_ice_candidate(Box::new(move |candidate| {
                let ws_sender = ws_sender_clone.clone();
                let worker_id = worker_id_clone.clone();
                let my_id = my_id_clone.clone();

                Box::pin(async move {
                    if let Some(cand) = candidate {
                        let candidate_msg = serde_json::json!({
                            "type": "candidate",
                            "to": worker_id,
                            "from": my_id,
                            "candidate": {
                                "candidate": cand.to_string(),
                                "sdpMid": "0",
                                "sdpMLineIndex": 0
                            }
                        });

                        let mut sender = ws_sender.lock().await;
                        let _ = sender.send(Message::Text(candidate_msg.to_string())).await;
                    }
                })
            }));

            // Set up data channel handling
            pc.on_data_channel(Box::new(move |data_channel| {
                let result_sender = result_sender.clone();
                let worker_id = worker_id_clone2.clone();
                let data_channels_arc = data_channels_arc.clone();

                Box::pin(async move {
                    println!("📡 Master: on_data_channel fired for worker: {}", worker_id);
                    println!("   Channel state: {:?}", data_channel.ready_state());
                    
                    // Store the data channel
                    let mut channels = data_channels_arc.lock().await;
                    channels.insert(worker_id.clone(), data_channel.clone());
                    println!("🔗 Stored data channel for worker: {} (total channels: {})", worker_id, channels.len());

                    // Fault tolerance: Monitor data channel state changes
                    // Note: We monitor the connection state instead since on_close may not be available
                    // The connection state change handler above will catch channel closures

                    // Set up message handling
                    data_channel.on_message(Box::new(move |msg| {
                        let result_sender = result_sender.clone();

                        Box::pin(async move {
                            if let Ok(text) = String::from_utf8(msg.data.to_vec()) {
                                // Try to parse as either float or bytes result
                                match serde_json::from_str::<WorkerResult>(&text) {
                                    Ok(result) => {
                                        let _ = result_sender.send(result).await;
                                    }
                                    Err(e) => {
                                        println!("❌ Failed to parse WorkerResult from worker message: {}", e);
                                        println!("   Raw message: {}", text);
                                    }
                                }
                            } else {
                                println!("❌ Failed to convert message data to UTF-8 string");
                            }
                        })
                    }));
                })
            }));

            // Set remote description from offer
            if let Some(sdp) = offer.get("sdp").and_then(|v| v.as_str()) {
                let offer_desc = RTCSessionDescription::offer(sdp.to_string()).unwrap();

                if let Ok(_) = pc.set_remote_description(offer_desc).await {
                    // Create answer
                    if let Ok(answer) = pc.create_answer(None).await {
                        if let Ok(_) = pc.set_local_description(answer.clone()).await {
                            // Send answer back
                            let answer_msg = serde_json::json!({
                                "type": "answer",
                                "to": worker_id_clone3,
                                "from": my_id,
                                "answer": {
                                    "type": "answer",
                                    "sdp": answer.sdp
                                }
                            });

                            let mut sender = ws_sender.lock().await;
                            let _ = sender.send(Message::Text(answer_msg.to_string())).await;

                            // Store peer connection
                            let mut connections = peer_connections_arc.lock().await;
                            connections.insert(worker_id_clone3, pc);
                        }
                    }
                }
            }
        }
    }

    async fn disconnect_from_signaling_server(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(ws_sender) = &self.ws_sender {
            let mut sender = ws_sender.lock().await;
            let _ = sender.close().await;
            println!("🔌 Disconnected from signaling server");
        }
        self.ws_sender = None;
        self.is_connected = false;
        Ok(())
    }
}

/// Simplified interface for distributed map-reduce operations
/// Automatically compiles WASM functions and handles distribution
pub async fn run_distributed_mapreduce<Input, Output, ChunkFn, ReduceFn>(
    input: Input,
    map_function_name: &str,
    chunker: ChunkFn,
    reducer: ReduceFn,
    execution_mode: ExecutionMode,
) -> Output
where
    Input: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
    Output: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
    ChunkFn: Fn(&Input) -> Vec<Input> + Send + Sync,
    ReduceFn: Fn(Vec<Output>) -> Output + Send + Sync,
{
    println!(
        "🌐 Running distributed map-reduce with {} mode",
        match execution_mode {
            ExecutionMode::CPU => "CPU",
            ExecutionMode::GPU => "GPU",
        }
    );

    // Use the existing implementation with provided functions
    run_distributed_impl_with_code(
        move |_data: Input| -> Output {
            // Dummy function (not used) - create default output
            panic!("Dummy function should not be called in distributed mode")
        },
        input,
        chunker,
        reducer,
        execution_mode,
        "", // Empty function body (not used)
        map_function_name,
    )
    .await
}

/// Byte-based distributed map-reduce that supports arbitrary user-defined Input/Output
/// by providing chunk encoder/decoder closures and a target WASM map function name.
pub async fn run_distributed_mapreduce_bytes<Input, ItemOutput, FinalOutput, ChunkFn, ReduceFn, ChunkEncodeFn, ResultDecodeFn>(
    input: Input,
    map_function_name: &str,
    chunker: ChunkFn,
    reducer: ReduceFn,
    chunk_encoder: ChunkEncodeFn,
    result_decoder: ResultDecodeFn,
) -> FinalOutput
where
    Input: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
    ItemOutput: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
    FinalOutput: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
    ChunkFn: Fn(&Input) -> Vec<Input> + Send + Sync,
    ReduceFn: Fn(Vec<ItemOutput>) -> FinalOutput + Send + Sync,
    ChunkEncodeFn: Fn(&Input) -> (Vec<u8>, serde_json::Value) + Send + Sync,
    ResultDecodeFn: Fn(Vec<u8>, serde_json::Value) -> ItemOutput + Send + Sync,
{
    println!("🌐 Running distributed byte-map with function: {}", map_function_name);

    // Compile WASM from examples directory (or target dir)
    let (wasm_bytes, js_glue) = match compile_examples_to_wasm(map_function_name).await {
        Ok(v) => v,
        Err(e) => {
            println!("⚠️ WASM compilation failed: {}", e);
            return reducer(Vec::new());
        }
    };

    // Chunk and encode
    let chunks = chunker(&input);
    let mut chunks_b64_with_meta: Vec<(String, Option<serde_json::Value>)> = Vec::with_capacity(chunks.len());
    for ch in chunks.iter() {
        let (bytes, meta) = chunk_encoder(ch);
        if !bytes.is_empty() {
            chunks_b64_with_meta.push((BASE64.encode(bytes), Some(meta)));
        }
    }
    if chunks_b64_with_meta.is_empty() {
        println!("⚠️ No byte chunks produced by chunker");
        return reducer(Vec::new());
    }

    // Execute distributed
    let mut distributor = match DistributedCompute::new().await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Failed to create distributed compute: {}", e);
            return reducer(Vec::new());
        }
    };

    let results = match distributor
        .execute_map_with_byte_chunks(chunks_b64_with_meta, &wasm_bytes, &js_glue, map_function_name)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Distributed byte-map execution failed: {}", e);
            return reducer(Vec::new());
        }
    };

    // Decode to Output and reduce
    let mut outputs: Vec<ItemOutput> = Vec::with_capacity(results.len());
    for (meta_opt, result_b64) in results {
        match BASE64.decode(result_b64) {
            Ok(bytes) => {
                let meta = meta_opt.unwrap_or(serde_json::Value::Null);
                outputs.push(result_decoder(bytes, meta));
            }
            Err(e) => {
                println!("⚠️ Failed to decode base64 result: {}", e);
            }
        }
    }

    reducer(outputs)
}
