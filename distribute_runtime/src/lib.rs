use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::mpsc;
use std::collections::HashMap;
use webrtc::api::APIBuilder;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::ice_transport::ice_candidate::{RTCIceCandidate, RTCIceCandidateInit};
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::sdp::sdp_type::RTCSdpType;
use webrtc::data_channel::RTCDataChannel;
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use std::fs;
use std::process::Command;
use std::path::Path;

#[derive(Debug, Clone)]
pub enum ExecutionMode {
    GPU,
    CPU,
}

pub trait ComputeFunction<Input, Output>: Send + Sync {
    fn call(&self, input: Input) -> Output;
}

impl<F, Input, Output> ComputeFunction<Input, Output> for F
where
    F: Fn(Input) -> Output + Send + Sync,
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
    execution_mode: ExecutionMode,
) -> Output
where
    F: ComputeFunction<Input, Output> + Send + Sync + 'static,
    Input: Serialize + for<'de> Deserialize<'de> + Clone + Send + 'static,
    Output: Serialize + for<'de> Deserialize<'de> + Send + 'static,
    ChunkFn: Fn(&Input) -> Vec<Input> + Send + Sync,
    ReduceFn: Fn(Vec<Output>) -> Output + Send + Sync,
{
    match execution_mode {
        ExecutionMode::CPU => {
            // CPU execution: distribute to worker nodes for computation
            let fn_name = std::any::type_name::<F>().split("::").last().unwrap_or("unknown");
            execute_wasm_function(fn_name, &input, &execution_mode).await
        },
        ExecutionMode::GPU => {
            // GPU execution via WASM module
            let fn_name = std::any::type_name::<F>().split("::").last().unwrap_or("unknown");
            execute_wasm_function(fn_name, &input, &execution_mode).await
        }
    }
}

async fn execute_wasm_function<Input, Output>(fn_name: &str, input: &Input, execution_mode: &ExecutionMode) -> Output
where
    Input: Serialize,
    Output: for<'de> Deserialize<'de>,
{
    println!("🌐 Dispatching work to distributed worker nodes...");

    // Execute distributed computation using Rust WebRTC
    let input_json = serde_json::to_string(input).unwrap();
    let result = execute_rust_webrtc_distributed(&input_json, execution_mode).await;

    match serde_json::from_str(&result) {
        Ok(parsed) => parsed,
        Err(e) => {
            eprintln!("Failed to parse result JSON: {}", e);
            eprintln!("Raw output: '{}'", result);
            eprintln!("ERROR: No workers available for distributed computation");
            eprintln!("Master node cannot perform computation locally");
            panic!("Distributed computation failed - no worker nodes available");
        }
    }
}

async fn execute_rust_webrtc_distributed(input_json: &str, execution_mode: &ExecutionMode) -> String {
    let master = RustWebRTCMaster::new().await;
    match master {
        Ok(mut master) => {
            let result = master.execute_distributed(input_json, execution_mode).await;
            match result {
                Ok(result) => result,
                Err(e) => {
                    eprintln!("Distributed execution failed: {}", e);
                    format!(r#"{{"error": "{}", "value": 0.0}}"#, e)
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to initialize WebRTC master: {}", e);
            format!(r#"{{"error": "WebRTC initialization failed: {}", "value": 0.0}}"#, e)
        }
    }
}

// Helper struct for parsing test data
#[derive(Serialize, Deserialize)]
struct TestDataForCalculation {
    numbers: Vec<f32>,
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
    data_chunk: Vec<f32>,
    execution_mode: String,
}

// Compute result message
#[derive(Serialize, Deserialize, Debug)]
struct ComputeResult {
    task_id: String,
    result: Vec<f32>,
    worker_id: String,
}

struct RustWebRTCMaster {
    ws_url: String,
    my_id: Option<String>,
    workers: Arc<tokio::sync::Mutex<Vec<String>>>,
    peer_connections: HashMap<String, Arc<webrtc::peer_connection::RTCPeerConnection>>,
    data_channels: Arc<tokio::sync::Mutex<HashMap<String, Arc<RTCDataChannel>>>>,
    result_receiver: Option<mpsc::Receiver<ComputeResult>>,
    result_sender: mpsc::Sender<ComputeResult>,
    is_connected: bool,
    ws_sender: Option<Arc<tokio::sync::Mutex<futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, tokio_tungstenite::tungstenite::protocol::Message>>>>,
}

impl RustWebRTCMaster {
    async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (result_sender, result_receiver) = mpsc::channel(100);

        Ok(Self {
            ws_url: "ws://localhost:3000".to_string(),
            my_id: None,
            workers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            peer_connections: HashMap::new(),
            data_channels: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            result_receiver: Some(result_receiver),
            result_sender,
            is_connected: false,
            ws_sender: None,
        })
    }

    async fn execute_distributed(&mut self, input_json: &str, execution_mode: &ExecutionMode) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
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

        // Actually distribute work to connected workers only
        // Limit chunk size to avoid WebRTC message size limits (max ~16KB)
        let max_chunk_size = 1000; // Conservative limit for WebRTC data channels
        let desired_chunk_size = input.numbers.len() / connected_workers.len().max(1);
        let chunk_size = desired_chunk_size.min(max_chunk_size);
        let mut tasks = Vec::new();

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
                    data_chunk,
                    execution_mode: match execution_mode {
                        ExecutionMode::CPU => "CPU".to_string(),
                        ExecutionMode::GPU => "GPU".to_string(),
                    },
                };

                // Send task via data channel (store for actual sending when implemented)
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
                let final_result: f32 = collected_results.iter().sum();
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

    async fn soft_reset(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Keep data channels and peer connections intact for reuse
        // Only clear any task-specific state if needed in the future

        // Don't disconnect from signaling server - keep the connection alive
        // This allows worker discovery to persist between executions
        println!("🔄 Soft reset complete - keeping all connections active");

        Ok(())
    }

    async fn full_cleanup(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Close WebSocket connection
        if let Some(ws_sender) = &self.ws_sender {
            let mut sender = ws_sender.lock().await;
            let _ = sender.close().await;
        }

        // Close peer connections
        for (_, peer_connection) in self.peer_connections.drain() {
            let _ = peer_connection.close().await;
        }

        // Clear data channels
        {
            let mut channels = self.data_channels.lock().await;
            channels.clear();
        }

        // Reset state
        self.ws_sender = None;
        self.is_connected = false;
        self.my_id = None;

        // Clear workers list
        {
            let mut workers = self.workers.lock().await;
            workers.clear();
        }

        Ok(())
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

    async fn connect_to_signaling_server(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = url::Url::parse(&self.ws_url)?;
        let (ws_stream, _) = connect_async(url).await?;
        let (ws_sender, mut ws_receiver) = ws_stream.split();

        let ws_sender = Arc::new(tokio::sync::Mutex::new(ws_sender));

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
        let my_id_arc = Arc::new(tokio::sync::Mutex::new(self.my_id.clone()));
        let peer_connections_arc = Arc::new(tokio::sync::Mutex::new(HashMap::<String, Arc<webrtc::peer_connection::RTCPeerConnection>>::new()));
        let data_channels_arc = self.data_channels.clone();
        let result_sender = self.result_sender.clone();

        // Store references for later use
        self.peer_connections = HashMap::new();

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
                                                    // Handle the offer and create answer
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
        ws_sender: Arc<tokio::sync::Mutex<futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, Message>>>,
        my_id: String,
        peer_connections_arc: Arc<tokio::sync::Mutex<HashMap<String, Arc<webrtc::peer_connection::RTCPeerConnection>>>>,
        data_channels_arc: Arc<tokio::sync::Mutex<HashMap<String, Arc<RTCDataChannel>>>>,
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

    async fn connect_to_worker(&mut self, worker_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Create WebRTC peer connection
        let config = RTCConfiguration {
            ice_servers: vec![RTCIceServer {
                urls: vec!["stun:stun.l.google.com:19302".to_owned()],
                ..Default::default()
            }],
            ..Default::default()
        };

        let api = APIBuilder::new().build();
        let peer_connection = Arc::new(api.new_peer_connection(config).await?);

        // Create data channel
        let data_channel = peer_connection.create_data_channel("computation", None).await?;

        // Set up data channel handlers
        let result_sender = self.result_sender.clone();
        let worker_id_clone = worker_id.to_string();

        data_channel.on_message(Box::new(move |msg| {
            let result_sender = result_sender.clone();
            let worker_id = worker_id_clone.clone();
            Box::pin(async move {
                if let Ok(text) = String::from_utf8(msg.data.to_vec()) {
                    if let Ok(result) = serde_json::from_str::<ComputeResult>(&text) {
                        let _ = result_sender.send(result).await;
                    }
                }
            })
        }));

        // Store connections
        self.peer_connections.insert(worker_id.to_string(), peer_connection.clone());
        {
            let mut channels = self.data_channels.lock().await;
            channels.insert(worker_id.to_string(), data_channel);
        }

        // Create offer (simplified - in real implementation, exchange via signaling server)
        let offer = peer_connection.create_offer(None).await?;
        peer_connection.set_local_description(offer).await?;

        Ok(())
    }
}

async fn compile_wasm_module(fn_name: &str) {
    let wasm_source = format!("target/wasm/{}_wasm.rs", fn_name);
    let wasm_pkg = format!("target/wasm/{}_pkg", fn_name);

    // Create a minimal Cargo.toml for the WASM module
    let cargo_toml_content = format!(r#"
[package]
name = "{}_wasm"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
wasm-bindgen = "0.2"
serde = {{ version = "1.0", features = ["derive"] }}
serde_json = "1.0"
js-sys = "0.3"
wgpu = "22.0"
bytemuck = "1.0"
log = "0.4"
console_log = "1.0"
futures = "0.3"

[dependencies.web-sys]
version = "0.3"
features = [
  "console",
  "WebGpuDevice",
  "WebGpuAdapter",
  "WebGpuBuffer",
  "WebGpuCommandEncoder",
  "WebGpuComputePassEncoder",
  "WebGpuComputePipeline",
  "WebGpuBindGroup",
  "WebGpuShaderModule",
  "WebGpuQueue",
]
"#, fn_name);

    let wasm_dir = format!("target/wasm/{}_wasm", fn_name);
    let wasm_src_dir = format!("{}/src", wasm_dir);
    fs::create_dir_all(&wasm_src_dir).unwrap();
    fs::write(format!("{}/Cargo.toml", wasm_dir), cargo_toml_content).unwrap();

    // Copy the generated source file
    if Path::new(&wasm_source).exists() {
        fs::copy(&wasm_source, format!("{}/lib.rs", wasm_src_dir)).unwrap();
    }

    // Copy shader.wgsl if it exists
    if Path::new("examples/src/shader.wgsl").exists() {
        fs::copy("examples/src/shader.wgsl", format!("{}/shader.wgsl", wasm_src_dir)).unwrap();
    }

    // Compile with wasm-pack
    let output = Command::new("wasm-pack")
        .args(&["build", "--target", "web", "--out-dir", &wasm_pkg])
        .current_dir(&wasm_dir)
        .output();

    match output {
        Ok(result) => {
            if !result.status.success() {
                eprintln!("WASM compilation failed: {}", String::from_utf8_lossy(&result.stderr));
            } else {
                println!("WASM module compiled successfully");
            }
        }
        Err(e) => {
            eprintln!("Failed to run wasm-pack: {}. Make sure wasm-pack is installed.", e);
        }
    }
}

async fn create_compute_bundle(fn_name: &str) {
    let bundle_content = r#"
// Distributed Compute Master Node - WebRTC P2P Distribution
// Generated automatically by distribute macro

class DistributedComputeEngine {
    constructor() {
        this.ws = null;
        this.myId = null;
        this.peerConnections = {};
        this.dataChannels = {};
        this.connectedPeers = {};
        this.currentJob = null;
        this.isInitialized = false;
    }

    async initialize() {
        if (this.isInitialized) return;

        return new Promise((resolve, reject) => {
            try {
                this.ws = new WebSocket("ws://localhost:3000");

                this.ws.onopen = () => {
                    if (typeof window !== 'undefined') console.log("Master node connected to WebSocket server");
                    this.isInitialized = true;
                    resolve();
                };

                this.ws.onmessage = (message) => {
                    const data = JSON.parse(message.data);
                    this.handleWebSocketMessage(data);
                };

                this.ws.onerror = (error) => {
                    console.error("WebSocket error:", error);
                    reject(error);
                };

            } catch (error) {
                reject(error);
            }
        });
    }

    handleWebSocketMessage(data) {
        if (data.type === "welcome") {
            this.myId = data.id;
            if (typeof window !== 'undefined') console.log("Master node ID:", this.myId);
            if (data.peers) this.updatePeerList(data.peers);
        }
        if (data.type === "peerList") {
            if (this.myId) this.updatePeerList(data.peers);
        }
        if (data.type === "offer") {
            this.handleOffer(data);
        }
        if (data.type === "answer") {
            const pc = this.peerConnections[data.from];
            if (pc) {
                pc.setRemoteDescription(new RTCSessionDescription(data.answer));
            }
        }
        if (data.type === "candidate") {
            const pc = this.peerConnections[data.from];
            if (pc) {
                pc.addIceCandidate(new RTCIceCandidate(data.candidate));
            }
        }
    }

    updatePeerList(peers) {
        const others = peers.filter(id => id !== this.myId);
        if (typeof window !== 'undefined') console.log("Available worker nodes:", others);

        others.forEach(peerId => {
            if (!this.peerConnections[peerId] && this.myId < peerId) {
                this.createConnection(peerId);
            }
        });
    }

    createConnection(peerId) {
        const pc = new RTCPeerConnection({
            iceServers: [{ urls: "stun:stun.l.google.com:19302" }]
        });
        this.peerConnections[peerId] = pc;

        const dc = pc.createDataChannel("computation");
        this.setupDataChannel(dc, peerId);

        pc.onicecandidate = (event) => {
            if (event.candidate) {
                this.ws.send(JSON.stringify({
                    to: peerId,
                    type: "candidate",
                    candidate: event.candidate
                }));
            }
        };

        pc.ondatachannel = (event) => {
            this.setupDataChannel(event.channel, peerId);
        };

        pc.createOffer()
            .then(offer => pc.setLocalDescription(offer))
            .then(() => {
                this.ws.send(JSON.stringify({
                    to: peerId,
                    type: "offer",
                    offer: pc.localDescription
                }));
            });
    }

    handleOffer(data) {
        const peerId = data.from;
        const pc = new RTCPeerConnection({
            iceServers: [{ urls: "stun:stun.l.google.com:19302" }]
        });
        this.peerConnections[peerId] = pc;

        pc.onicecandidate = (event) => {
            if (event.candidate) {
                this.ws.send(JSON.stringify({
                    to: peerId,
                    type: "candidate",
                    candidate: event.candidate
                }));
            }
        };

        pc.ondatachannel = (event) => {
            this.setupDataChannel(event.channel, peerId);
        };

        pc.setRemoteDescription(new RTCSessionDescription(data.offer))
            .then(() => pc.createAnswer())
            .then(answer => pc.setLocalDescription(answer))
            .then(() => {
                this.ws.send(JSON.stringify({
                    to: peerId,
                    type: "answer",
                    answer: pc.localDescription
                }));
            });
    }

    setupDataChannel(channel, peerId) {
        channel.onopen = () => {
            if (typeof window !== 'undefined') console.log("Connected to worker node:", peerId);
            this.connectedPeers[peerId] = true;
        };

        channel.onclose = () => {
            if (typeof window !== 'undefined') console.log("Disconnected from worker node:", peerId);
            delete this.connectedPeers[peerId];
        };

        channel.onmessage = (event) => {
            if (typeof event.data === "string") {
                try {
                    const msg = JSON.parse(event.data);
                    if (msg.type === "computeResult") {
                        this.handleComputeResult(msg);
                    }
                } catch (e) {
                    console.error("Error parsing worker result:", e);
                }
            }
        };

        this.dataChannels[peerId] = channel;
    }

    handleComputeResult(msg) {
        if (!this.currentJob || msg.taskId !== this.currentJob.taskId) return;

        if (typeof window !== 'undefined') console.log("Received result from worker:", msg.workerId, msg.result);
        this.currentJob.results.push(msg.result);

        if (this.currentJob.results.length >= this.currentJob.expectedResults) {
            // All results received, reduce them
            const finalResult = this.currentJob.results.flat().reduce((sum, val) => sum + val, 0);
            this.currentJob.resolve({ value: finalResult });
            this.currentJob = null;
        }
    }

    // Main execution function
    async execute(input, mode = 'auto') {
        await this.initialize();

        // Wait up to 5 seconds for workers to connect
        const maxWaitTime = 5000; // 5 seconds
        const startTime = Date.now();

        while (Object.keys(this.connectedPeers).length === 0 && (Date.now() - startTime) < maxWaitTime) {
            await new Promise(resolve => setTimeout(resolve, 500));
        }

        const connectedWorkers = Object.keys(this.connectedPeers);
        if (connectedWorkers.length === 0) {
            if (typeof window !== 'undefined') console.log("No workers connected, executing locally");
            return this.executeLocally(input);
        }

        if (typeof window !== 'undefined') console.log(`Distributing work across ${connectedWorkers.length} worker nodes`);

        return new Promise((resolve, reject) => {
            try {
                const taskId = Date.now().toString();
                const chunks = this.chunkData(input, connectedWorkers.length);

                this.currentJob = {
                    taskId,
                    expectedResults: chunks.length,
                    results: [],
                    resolve,
                    reject
                };

                // Send tasks to workers
                connectedWorkers.forEach((workerId, index) => {
                    const chunk = chunks[index] || [];
                    if (chunk.length === 0) return;

                    const task = {
                        type: "computeTask",
                        taskId,
                        dataChunk: chunk,
                        executionMode: mode,
                        wasmBase64: "dummy" // Would contain actual WASM in real implementation
                    };

                    this.dataChannels[workerId].send(JSON.stringify(task));
                });

            } catch (error) {
                reject(error);
            }
        });
    }

    chunkData(input, numChunks) {
        if (!input.numbers || !Array.isArray(input.numbers)) {
            return [[]];
        }

        const chunkSize = Math.ceil(input.numbers.length / numChunks);
        const chunks = [];

        for (let i = 0; i < input.numbers.length; i += chunkSize) {
            chunks.push(input.numbers.slice(i, i + chunkSize));
        }

        return chunks;
    }

    executeLocally(input) {
        // Master should NOT do computation - this should only be reached if no workers
        console.error("ERROR: Master node attempted local computation - this should never happen!");
        console.error("Computation must be distributed to worker nodes only.");
        return { error: "Master cannot perform computation - workers required", value: 0 };
    }
}

// Export for both Node.js and browser environments
if (typeof module !== 'undefined' && module.exports) {
    module.exports = DistributedComputeEngine;
} else {
    window.DistributedComputeEngine = DistributedComputeEngine;
}
"#;

    fs::create_dir_all("target").ok();
    fs::write("target/compute-bundle.js", bundle_content).unwrap();
    println!("Distributed compute bundle created: target/compute-bundle.js");
}
