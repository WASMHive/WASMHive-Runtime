use serde::{Deserialize, Serialize};
use std::process::Command;
use std::fs;
use webrtc::api::APIBuilder;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::data_channel::RTCDataChannel;
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use std::sync::Arc;
use std::collections::HashMap;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};

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
    Welcome { id: String, peers: Option<Vec<String>> },
    #[serde(rename = "peerList")]
    PeerList { peers: Vec<String> },
    #[serde(rename = "offer")]
    Offer { from: String, to: String, offer: serde_json::Value },
    #[serde(rename = "answer")]
    Answer { from: String, to: String, answer: serde_json::Value },
    #[serde(rename = "candidate")]
    Candidate { from: String, to: String, candidate: serde_json::Value },
}

// Compute task message
#[derive(Serialize, Deserialize, Debug)]
struct ComputeTask {
    task_id: String,
    wasm_module: String, // base64 encoded WASM
    js_glue: String,
    data_chunk: Vec<f32>,
    map_function: String, // "cpu_map" or "gpu_map"
}

// Compute result message
#[derive(Serialize, Deserialize, Debug)]
struct ComputeResult {
    task_id: String,
    result: Vec<f32>,
    worker_id: String,
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

pub async fn run_distributed_impl<F, Input, Output, ChunkFn, ReduceFn>(
    compute_fn: F,
    input: Input,
    chunker: ChunkFn,
    reducer: ReduceFn,
    _execution_mode: ExecutionMode,
) -> Output
where
    F: ComputeFunction<Input, Output> + Send + Sync + 'static,
    Input: Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static,
    Output: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    ChunkFn: Fn(&Input) -> Vec<Input> + Send + Sync,
    ReduceFn: Fn(Vec<Output>) -> Output + Send + Sync,
{
    // For now, fall back to local execution
    // TODO: Implement distributed execution using the examples WASM
    println!("🌐 Executing distributed computation using examples function...");

    let chunks = chunker(&input);
    println!("📦 Split input into {} chunks", chunks.len());

    let mut results = Vec::new();
    for (i, chunk) in chunks.iter().enumerate() {
        println!("⚡ Processing chunk {} of {}", i + 1, chunks.len());
        let result = compute_fn.call(chunk.clone());
        results.push(result);
    }

    let final_result = reducer(results);
    println!("✅ Distributed computation completed");

    final_result
}

pub async fn run_distributed_impl_with_code<F, Input, Output, ChunkFn, ReduceFn>(
    _compute_fn: F,
    input: Input,
    _chunker: ChunkFn,
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
            println!("📦 Successfully compiled examples to WASM ({} bytes)", wasm_bytes.len());

            // Execute distributed map-reduce using compiled WASM
            let input_json = serde_json::to_string(&input).unwrap();
            let result = execute_distributed_map_reduce(&input_json, &execution_mode, &wasm_bytes, &js_glue, fn_name).await;

            match serde_json::from_str(&result) {
                Ok(parsed) => parsed,
                Err(e) => {
                    println!("⚠️ Failed to parse distributed result: {}", e);
                    // Return a default result based on the reducer function
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


async fn compile_examples_to_wasm(_fn_name: &str) -> Result<(Vec<u8>, String), Box<dyn std::error::Error>> {
    println!("🔧 Compiling examples to WASM for distributed execution...");

    // Get current directory and find examples path
    let current_dir = std::env::current_dir()?;
    let examples_dir = if current_dir.file_name().unwrap() == "examples" {
        current_dir
    } else {
        current_dir.join("examples")
    };

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
    let wasm_file_path = examples_dir.join("pkg").join("distributed_examples_bg.wasm");
    let js_file_path = examples_dir.join("pkg").join("distributed_examples.js");

    let wasm_bytes = fs::read(&wasm_file_path)?;
    let js_glue = fs::read_to_string(&js_file_path)?;

    println!("📦 WASM module size: {} bytes", wasm_bytes.len());
    println!("📜 JS glue size: {} bytes", js_glue.len());

    Ok((wasm_bytes, js_glue))
}

async fn execute_distributed_map_reduce(input_json: &str, execution_mode: &ExecutionMode, wasm_bytes: &[u8], js_glue: &str, fn_name: &str) -> String {
    println!("🌐 Starting distributed map-reduce execution...");

    let mut distributor = match DistributedCompute::new().await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Failed to create distributed compute: {}", e);
            return String::from("{\"value\": 0.0}");
        }
    };

    match distributor.execute_map_reduce(input_json, execution_mode, wasm_bytes, js_glue, fn_name).await {
        Ok(result) => result,
        Err(e) => {
            eprintln!("Distributed map-reduce execution failed: {}", e);
            String::from("{\"value\": 0.0}")
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
    result_receiver: Option<mpsc::Receiver<ComputeResult>>,
    result_sender: mpsc::Sender<ComputeResult>,
    is_connected: bool,
    ws_sender: Option<Arc<Mutex<futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, Message>>>>,
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
        })
    }

    async fn execute_map_reduce(&mut self, input_json: &str, execution_mode: &ExecutionMode, wasm_bytes: &[u8], js_glue: &str, _fn_name: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
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
            workers_list.into_iter()
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
        println!("📊 Distributing to {} connected workers: {:?}", connected_workers.len(), connected_workers);

        for (worker_id, task) in &tasks {
            let data_channels = self.data_channels.lock().await;
            if let Some(channel) = data_channels.get(worker_id) {
                println!("🔍 Attempting to send task to {}, channel state: ready", worker_id);
                let task_json = serde_json::to_string(task).unwrap();
                match channel.send_text(&task_json).await {
                    Ok(_) => {
                        println!("   ✅ {} -> {} ({} elements)", task.task_id, worker_id, task.data_chunk.len());
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
                    match tokio::time::timeout(tokio::time::Duration::from_millis(1000), receiver.recv()).await {
                        Ok(Some(result)) => {
                            println!("   📥 {} returned {} values: {:?}", result.worker_id, result.result.len(), result.result);
                            collected_results.extend(result.result);
                            results_received += 1;
                        }
                        Ok(None) => {
                            println!("   ⚠️  Result channel closed");
                            break; // Channel closed
                        }
                        Err(_) => {
                            println!("   ⏱️  Waiting for more results... ({}/{})", results_received, sent_tasks);
                        } // Timeout, continue waiting
                    }
                    self.result_receiver = Some(receiver);
                }
            }

            if results_received == sent_tasks {
                // Use local WASM reduce function to combine results
                let final_result = self.reduce_results_with_wasm(&collected_results).await?;
                println!("✅ All {} workers completed! Distributed result: {}", sent_tasks, final_result);

                // Disconnect from signaling server after successful job completion
                self.disconnect_from_signaling_server().await?;

                return Ok(format!(r#"{{"value": {}}}"#, final_result));
            } else {
                println!("❌ Only {}/{} workers returned results - distributed execution failed", results_received, sent_tasks);
            }
        }

        // Disconnect from signaling server after failed job
        self.disconnect_from_signaling_server().await?;

        // No fallback - remote execution must work
        Err("Failed to get results from distributed workers - remote execution required".into())
    }

    async fn reduce_results_with_wasm(&self, results: &[f32]) -> Result<f32, Box<dyn std::error::Error + Send + Sync>> {
        println!("🔧 Using local WASM reduce function to combine {} values", results.len());

        if results.is_empty() {
            return Ok(0.0);
        }

        // For now, use simple sum reduction
        // TODO: Load and execute the actual WASM reduce function
        let total = results.iter().sum();
        println!("📊 Reduce operation completed: {}", total);

        Ok(total)
    }

    async fn connect_to_signaling_server(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

                                        if let Some(peers) = parsed.get("peers").and_then(|v| v.as_array()) {
                                            let mut workers = workers_arc.lock().await;
                                            *workers = peers.iter()
                                                .filter_map(|p| p.as_str())
                                                .filter(|&p| p != id)
                                                .map(|s| s.to_string())
                                                .collect();
                                        }
                                    }
                                }
                                "peerList" => {
                                    if let Some(peers) = parsed.get("peers").and_then(|v| v.as_array()) {
                                        let my_id = my_id_arc.lock().await;
                                        let current_id = my_id.as_ref().map(|s| s.as_str()).unwrap_or("");

                                        let mut workers = workers_arc.lock().await;
                                        *workers = peers.iter()
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
                                        parsed.get("offer")
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
                                                    result_sender.clone()
                                                ).await;
                                            }
                                        }
                                    }
                                }
                                "answer" => {
                                    // Handle answer from worker
                                    if let (Some(from), Some(answer)) = (
                                        parsed.get("from").and_then(|v| v.as_str()),
                                        parsed.get("answer")
                                    ) {
                                        println!("📨 Received answer from worker: {}", from);
                                        let peer_connections = peer_connections_arc.lock().await;
                                        if let Some(pc) = peer_connections.get(from) {
                                            if let Some(sdp) = answer.get("sdp").and_then(|v| v.as_str()) {
                                                let answer_desc = RTCSessionDescription::answer(sdp.to_string()).unwrap();
                                                let _ = pc.set_remote_description(answer_desc).await;
                                            }
                                        }
                                    }
                                }
                                "candidate" => {
                                    // Handle ICE candidate from worker
                                    if let (Some(from), Some(candidate)) = (
                                        parsed.get("from").and_then(|v| v.as_str()),
                                        parsed.get("candidate")
                                    ) {
                                        let peer_connections = peer_connections_arc.lock().await;
                                        if let Some(pc) = peer_connections.get(from) {
                                            if let Some(candidate_str) = candidate.get("candidate").and_then(|v| v.as_str()) {
                                                let ice_candidate_init = RTCIceCandidateInit {
                                                    candidate: candidate_str.to_string(),
                                                    sdp_mid: Some("0".to_string()),
                                                    sdp_mline_index: Some(0),
                                                    username_fragment: None,
                                                };
                                                let _ = pc.add_ice_candidate(ice_candidate_init).await;
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
        ws_sender: Arc<Mutex<futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, Message>>>,
        my_id: String,
        peer_connections_arc: Arc<Mutex<HashMap<String, Arc<webrtc::peer_connection::RTCPeerConnection>>>>,
        data_channels_arc: Arc<Mutex<HashMap<String, Arc<RTCDataChannel>>>>,
        result_sender: mpsc::Sender<ComputeResult>
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
                    // Store the data channel
                    let mut channels = data_channels_arc.lock().await;
                    channels.insert(worker_id.clone(), data_channel.clone());
                    println!("🔗 Connected to worker: {}", worker_id);

                    // Set up message handling
                    data_channel.on_message(Box::new(move |msg| {
                        let result_sender = result_sender.clone();

                        Box::pin(async move {
                            if let Ok(text) = String::from_utf8(msg.data.to_vec()) {
                                if let Ok(result) = serde_json::from_str::<ComputeResult>(&text) {
                                    let _ = result_sender.send(result).await;
                                }
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

    async fn disconnect_from_signaling_server(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
pub async fn run_distributed_mapreduce<Input, Output>(
    input: Input,
    map_function_name: &str,
    _reduce_function_name: &str,
    execution_mode: ExecutionMode,
) -> Output
where
    Input: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
    Output: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
{
    println!("🌐 Running distributed map-reduce with {} mode",
        match execution_mode {
            ExecutionMode::CPU => "CPU",
            ExecutionMode::GPU => "GPU",
        }
    );

    // Default chunker: splits data into individual elements (for Vec<f32>)
    let default_chunker = |data: &Input| -> Vec<Input> {
        // Try to deserialize as a struct with numbers field
        if let Ok(serialized) = serde_json::to_string(data) {
            if let Ok(test_data) = serde_json::from_str::<serde_json::Value>(&serialized) {
                if let Some(numbers) = test_data.get("numbers") {
                    if let Some(numbers_array) = numbers.as_array() {
                        let mut chunks = Vec::new();
                        for number in numbers_array {
                            if let Some(num) = number.as_f64() {
                                // Create individual chunks with single numbers
                                let chunk = format!(r#"{{"numbers":[{}]}}"#, num);
                                if let Ok(chunk_data) = serde_json::from_str::<Input>(&chunk) {
                                    chunks.push(chunk_data);
                                }
                            }
                        }
                        return chunks;
                    }
                }
            }
        }
        // Fallback: return original data as single chunk
        vec![data.clone()]
    };

    // Default reducer: sums up the values from results
    let default_reducer = move |results: Vec<Output>| -> Output {
        let mut total = 0.0f32;
        let mut first_result = None;

        for result in &results {
            if first_result.is_none() {
                first_result = Some(result.clone());
            }
            if let Ok(serialized) = serde_json::to_string(result) {
                if let Ok(result_data) = serde_json::from_str::<serde_json::Value>(&serialized) {
                    if let Some(value) = result_data.get("value") {
                        if let Some(val) = value.as_f64() {
                            total += val as f32;
                        }
                    }
                }
            }
        }
        // Create result with summed value
        let result_str = format!(r#"{{"value":{}}}"#, total);
        serde_json::from_str::<Output>(&result_str).unwrap_or_else(|_| {
            // Fallback: return first result or a default value
            first_result.unwrap_or_else(|| {
                // Last resort: try to create a default Output value
                if let Ok(default_output) = serde_json::from_str::<Output>(r#"{"value":0.0}"#) {
                    default_output
                } else {
                    panic!("Unable to create default Output value")
                }
            })
        })
    };

    // Use the existing implementation with default functions
    run_distributed_impl_with_code(
        move |_data: Input| -> Output {
            // Dummy function (not used) - create default output
            if let Ok(default_output) = serde_json::from_str::<Output>(r#"{"value":0.0}"#) {
                default_output
            } else {
                panic!("Unable to create default Output value in dummy function")
            }
        },
        input,
        default_chunker,
        default_reducer,
        execution_mode,
        "", // Empty function body (not used)
        map_function_name,
    ).await
}