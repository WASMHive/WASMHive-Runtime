use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::process::Command;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use webrtc::api::APIBuilder;
use webrtc::data_channel::data_channel_state::RTCDataChannelState;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

pub mod protocol;
use protocol::{
    decode_frame, decode_result_payload, encode_task_payload, split_into_frames, Reassembler,
    TaskHeader, FRAME_RESULT, FRAME_TASK,
};

pub type DistributeError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Clone)]
pub enum ExecutionMode {
    CPU,
    GPU,
}

/// What to do when some chunks never produce a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingChunkPolicy {
    /// Fail the whole job (default). A partial sum is a wrong sum.
    Fail,
    /// Return the results that did arrive, in chunk order, and log what was dropped.
    AllowPartial,
}

#[derive(Debug, Clone)]
pub struct JobOptions {
    pub missing_chunks: MissingChunkPolicy,
    /// Per-task timeout before the task is reassigned to another worker.
    pub task_timeout_secs: u64,
    /// How many times a chunk may be reassigned before it counts as missing.
    pub max_retries: u32,
}

impl Default for JobOptions {
    fn default() -> Self {
        Self {
            missing_chunks: MissingChunkPolicy::Fail,
            task_timeout_secs: 30,
            max_retries: 3,
        }
    }
}

/// A completed worker result delivered from the data-channel handler.
#[derive(Debug)]
struct WorkerResultMsg {
    task_id: String,
    chunk_index: u32,
    worker_id: String,
    error: Option<String>,
    meta: serde_json::Value,
    bytes: Vec<u8>,
}

/// A task in flight, kept so it can be re-sent to another worker.
#[derive(Debug, Clone)]
struct PendingTask {
    chunk_index: u32,
    worker_id: String,
    sent_at: std::time::Instant,
    retry_count: u32,
}

const BUFFERED_HIGH_WATER: usize = 4 * 1024 * 1024;
const WORKER_REHAB_SECS: u64 = 60;
const WORKER_STRIKE_LIMIT: u32 = 3;

pub struct DistributedCompute {
    ws_url: String,
    my_id: Option<String>,
    workers: Arc<Mutex<Vec<String>>>,
    peer_connections: Arc<Mutex<HashMap<String, Arc<webrtc::peer_connection::RTCPeerConnection>>>>,
    data_channels: Arc<Mutex<HashMap<String, Arc<RTCDataChannel>>>>,
    result_receiver: Option<mpsc::Receiver<WorkerResultMsg>>,
    result_sender: mpsc::Sender<WorkerResultMsg>,
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
    failed_workers: Arc<Mutex<HashMap<String, std::time::Instant>>>,
}

impl DistributedCompute {
    async fn new() -> Result<Self, DistributeError> {
        let (result_sender, result_receiver) = mpsc::channel(256);
        Ok(Self {
            ws_url: std::env::var("WASMHIVE_SIGNALING_URL")
                .unwrap_or_else(|_| "ws://localhost:3000".to_string()),
            my_id: None,
            workers: Arc::new(Mutex::new(Vec::new())),
            peer_connections: Arc::new(Mutex::new(HashMap::new())),
            data_channels: Arc::new(Mutex::new(HashMap::new())),
            result_receiver: Some(result_receiver),
            result_sender,
            ws_sender: None,
            failed_workers: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Send pre-encoded frames over a channel with backpressure.
    async fn send_frames(
        channel: &Arc<RTCDataChannel>,
        frames: &[Vec<u8>],
    ) -> Result<(), DistributeError> {
        for frame in frames {
            while channel.buffered_amount().await > BUFFERED_HIGH_WATER {
                tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
            }
            channel.send(&Bytes::from(frame.clone())).await?;
        }
        Ok(())
    }

    /// Workers with an open data channel, excluding recently failed ones.
    /// A failed worker whose channel is still open is rehabilitated after a cooldown.
    async fn get_available_workers(&self) -> Vec<String> {
        let data_channels = self.data_channels.lock().await;
        let mut failed = self.failed_workers.lock().await;
        let now = std::time::Instant::now();
        failed.retain(|worker_id, since| {
            let channel_open = data_channels
                .get(worker_id)
                .map(|c| matches!(c.ready_state(), RTCDataChannelState::Open))
                .unwrap_or(false);
            !(channel_open && now.duration_since(*since).as_secs() >= WORKER_REHAB_SECS)
        });
        data_channels
            .iter()
            .filter(|(worker_id, channel)| {
                !failed.contains_key(*worker_id)
                    && matches!(channel.ready_state(), RTCDataChannelState::Open)
            })
            .map(|(worker_id, _)| worker_id.clone())
            .collect()
    }

    async fn mark_worker_failed(&self, worker_id: &str) {
        let mut failed = self.failed_workers.lock().await;
        failed.insert(worker_id.to_string(), std::time::Instant::now());
        println!("⚠️  Marked worker {} as failed", worker_id);
    }

    /// Build and send one task to one worker. The WASM module and glue are
    /// included only when the worker has not received them for this job yet.
    async fn send_task(
        &self,
        worker_id: &str,
        header: &TaskHeader,
        input: &[u8],
        wasm: &[u8],
        glue: &[u8],
        workers_with_module: &mut HashSet<String>,
    ) -> Result<(), DistributeError> {
        let channel = {
            let channels = self.data_channels.lock().await;
            channels
                .get(worker_id)
                .cloned()
                .ok_or_else(|| format!("no data channel for worker {}", worker_id))?
        };
        if !matches!(channel.ready_state(), RTCDataChannelState::Open) {
            return Err(format!("data channel to {} is not open", worker_id).into());
        }
        let include_module = !workers_with_module.contains(worker_id);
        let payload = if include_module {
            encode_task_payload(header, wasm, glue, input)
        } else {
            encode_task_payload(header, &[], &[], input)
        };
        let frames = split_into_frames(FRAME_TASK, &header.task_id, &payload);
        Self::send_frames(&channel, &frames).await?;
        if include_module {
            workers_with_module.insert(worker_id.to_string());
        }
        Ok(())
    }

    /// Run one job: distribute `chunks` (already encoded to bytes + meta),
    /// collect results in chunk order, reassigning failed or timed-out tasks.
    async fn execute_byte_job(
        &mut self,
        chunks: Vec<(Vec<u8>, serde_json::Value)>,
        wasm_bytes: &[u8],
        js_glue: &str,
        map_function_name: &str,
        opts: &JobOptions,
    ) -> Result<Vec<Option<(serde_json::Value, Vec<u8>)>>, DistributeError> {
        self.connect_to_signaling_server().await?;

        // Give discovery and channel setup a moment. Event-driven readiness
        // replaces these fixed waits in a later phase.
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        let available = self.get_available_workers().await;
        if available.is_empty() {
            self.disconnect_from_signaling_server().await?;
            return Err("no workers with open data channels available".into());
        }

        let job_id = uuid::Uuid::new_v4().to_string();
        let total = chunks.len();
        println!(
            "📊 Job {}: {} chunks across {} workers (map fn: {})",
            job_id,
            total,
            available.len(),
            map_function_name
        );

        // Job-local state.
        let mut task_store: HashMap<String, (u32, Vec<u8>, serde_json::Value)> = HashMap::new();
        let mut pending: HashMap<String, PendingTask> = HashMap::new();
        let mut results: Vec<Option<(serde_json::Value, Vec<u8>)>> = vec![None; total];
        let mut failed_chunks: BTreeSet<u32> = BTreeSet::new();
        let mut received_task_ids: HashSet<String> = HashSet::new();
        let mut workers_with_module: HashSet<String> = HashSet::new();
        let mut worker_strikes: HashMap<String, u32> = HashMap::new();
        let mut rr_cursor = 0usize;

        // Initial dispatch, round-robin over available workers.
        let mut sent = 0usize;
        for (i, (input, meta)) in chunks.into_iter().enumerate() {
            let worker_id = available[i % available.len()].clone();
            let task_id = format!("{}_{}", job_id, i);
            let header = TaskHeader {
                job_id: job_id.clone(),
                task_id: task_id.clone(),
                chunk_index: i as u32,
                map_function: map_function_name.to_string(),
                meta: meta.clone(),
            };
            match self
                .send_task(
                    &worker_id,
                    &header,
                    &input,
                    wasm_bytes,
                    js_glue.as_bytes(),
                    &mut workers_with_module,
                )
                .await
            {
                Ok(()) => {
                    pending.insert(
                        task_id.clone(),
                        PendingTask {
                            chunk_index: i as u32,
                            worker_id,
                            sent_at: std::time::Instant::now(),
                            retry_count: 0,
                        },
                    );
                    sent += 1;
                }
                Err(e) => {
                    println!("   ❌ Failed to send chunk {} to {}: {}", i, worker_id, e);
                    self.mark_worker_failed(&worker_id).await;
                    // Backdate so the next timeout pass reassigns it immediately.
                    pending.insert(
                        task_id.clone(),
                        PendingTask {
                            chunk_index: i as u32,
                            worker_id,
                            sent_at: std::time::Instant::now()
                                - std::time::Duration::from_secs(opts.task_timeout_secs + 1),
                            retry_count: 0,
                        },
                    );
                }
            }
            task_store.insert(task_id, (i as u32, input, meta));
        }
        println!("   📤 {} of {} chunks dispatched", sent, total);

        // Collect with reassignment.
        let deadline = std::time::Instant::now()
            + std::time::Duration::from_secs((10 * total as u64).clamp(120, 600));
        let mut last_activity = std::time::Instant::now();
        let mut last_timeout_check = std::time::Instant::now();
        let mut completed = 0usize;

        let mut receiver = self
            .result_receiver
            .take()
            .ok_or("result receiver already taken")?;

        while completed + failed_chunks.len() < total {
            if std::time::Instant::now() > deadline {
                println!(
                    "⏱️  Job deadline reached with {} tasks unresolved",
                    pending.len()
                );
                break;
            }
            if last_activity.elapsed().as_secs() > 90 {
                println!("⏱️  No results for 90s; stopping collection");
                break;
            }

            if last_timeout_check.elapsed().as_secs() >= 5 {
                self.reassign_timed_out(
                    &mut pending,
                    &task_store,
                    &mut failed_chunks,
                    &mut worker_strikes,
                    &mut workers_with_module,
                    &mut rr_cursor,
                    wasm_bytes,
                    js_glue,
                    map_function_name,
                    &job_id,
                    opts,
                )
                .await;
                last_timeout_check = std::time::Instant::now();
            }

            let msg = match tokio::time::timeout(
                tokio::time::Duration::from_millis(1000),
                receiver.recv(),
            )
            .await
            {
                Ok(Some(m)) => m,
                Ok(None) => {
                    println!("   ⚠️  Result channel closed");
                    break;
                }
                Err(_) => continue,
            };

            if received_task_ids.contains(&msg.task_id) {
                continue; // late duplicate after a reassignment
            }

            if let Some(err) = msg.error {
                println!(
                    "   ❌ Worker {} failed task {} (chunk {}): {}",
                    msg.worker_id, msg.task_id, msg.chunk_index, err
                );
                if err.contains("no cached module") {
                    // Coordination artifact, not a worker fault: the worker lost
                    // (or never got) the module. Re-send with the module included.
                    workers_with_module.remove(&msg.worker_id);
                } else {
                    let strikes = worker_strikes.entry(msg.worker_id.clone()).or_insert(0);
                    *strikes += 1;
                    if *strikes >= WORKER_STRIKE_LIMIT {
                        self.mark_worker_failed(&msg.worker_id).await;
                    }
                }
                if let Some(p) = pending.get_mut(&msg.task_id) {
                    // Force the next timeout pass to reassign it immediately.
                    p.sent_at = std::time::Instant::now()
                        - std::time::Duration::from_secs(opts.task_timeout_secs + 1);
                }
                last_activity = std::time::Instant::now();
                continue;
            }

            let idx = msg.chunk_index as usize;
            if idx >= results.len() {
                println!("   ⚠️  Result with out-of-range chunk index {}", idx);
                continue;
            }
            pending.remove(&msg.task_id);
            received_task_ids.insert(msg.task_id);
            if results[idx].is_none() {
                results[idx] = Some((msg.meta, msg.bytes));
                completed += 1;
                if completed % 10 == 0 || completed == total {
                    println!("   📥 {}/{} results", completed, total);
                }
            }
            last_activity = std::time::Instant::now();
        }

        self.result_receiver = Some(receiver);

        // Anything still pending counts as missing.
        for (_, p) in pending.drain() {
            failed_chunks.insert(p.chunk_index);
        }

        self.disconnect_from_signaling_server().await?;

        if !failed_chunks.is_empty() {
            let missing: Vec<u32> = failed_chunks.iter().copied().collect();
            match opts.missing_chunks {
                MissingChunkPolicy::Fail => {
                    return Err(format!(
                        "job incomplete: {}/{} chunks missing (indices {:?})",
                        missing.len(),
                        total,
                        &missing[..missing.len().min(20)]
                    )
                    .into());
                }
                MissingChunkPolicy::AllowPartial => {
                    println!(
                        "⚠️  Continuing with partial results: {}/{} chunks missing (indices {:?})",
                        missing.len(),
                        total,
                        &missing[..missing.len().min(20)]
                    );
                }
            }
        }
        println!("✅ Job complete: {}/{} chunks", completed, total);
        Ok(results)
    }

    /// Reassign tasks that exceeded the per-task timeout, regardless of
    /// whether their worker's channel is still open (a stuck worker looks
    /// exactly like a slow one from the outside). First result wins; late
    /// duplicates are deduplicated by task id.
    #[allow(clippy::too_many_arguments)]
    async fn reassign_timed_out(
        &self,
        pending: &mut HashMap<String, PendingTask>,
        task_store: &HashMap<String, (u32, Vec<u8>, serde_json::Value)>,
        failed_chunks: &mut BTreeSet<u32>,
        worker_strikes: &mut HashMap<String, u32>,
        workers_with_module: &mut HashSet<String>,
        rr_cursor: &mut usize,
        wasm_bytes: &[u8],
        js_glue: &str,
        map_function_name: &str,
        job_id: &str,
        opts: &JobOptions,
    ) {
        let now = std::time::Instant::now();
        let timed_out: Vec<String> = pending
            .iter()
            .filter(|(_, p)| now.duration_since(p.sent_at).as_secs() > opts.task_timeout_secs)
            .map(|(id, _)| id.clone())
            .collect();
        if timed_out.is_empty() {
            return;
        }

        let available = self.get_available_workers().await;
        for task_id in timed_out {
            let Some(p) = pending.get(&task_id).cloned() else {
                continue;
            };

            // Strike the worker that sat on it.
            let strikes = worker_strikes.entry(p.worker_id.clone()).or_insert(0);
            *strikes += 1;
            if *strikes >= WORKER_STRIKE_LIMIT {
                self.mark_worker_failed(&p.worker_id).await;
            }

            if p.retry_count >= opts.max_retries {
                println!(
                    "   ☠️  Chunk {} exhausted {} retries; marking missing",
                    p.chunk_index, opts.max_retries
                );
                pending.remove(&task_id);
                failed_chunks.insert(p.chunk_index);
                continue;
            }

            // Prefer a different worker; fall back to any available one.
            let candidates: Vec<&String> =
                available.iter().filter(|w| **w != p.worker_id).collect();
            let target = if !candidates.is_empty() {
                *rr_cursor += 1;
                Some(candidates[*rr_cursor % candidates.len()].clone())
            } else {
                available.first().cloned()
            };
            let Some(target) = target else {
                continue; // no workers right now; retry on a later pass
            };

            let Some((chunk_index, input, meta)) = task_store.get(&task_id) else {
                continue;
            };
            let header = TaskHeader {
                job_id: job_id.to_string(),
                task_id: task_id.clone(),
                chunk_index: *chunk_index,
                map_function: map_function_name.to_string(),
                meta: meta.clone(),
            };
            match self
                .send_task(
                    &target,
                    &header,
                    input,
                    wasm_bytes,
                    js_glue.as_bytes(),
                    workers_with_module,
                )
                .await
            {
                Ok(()) => {
                    println!(
                        "   🔄 Reassigned chunk {} from {} to {} (retry {})",
                        chunk_index,
                        p.worker_id,
                        target,
                        p.retry_count + 1
                    );
                    if let Some(entry) = pending.get_mut(&task_id) {
                        entry.worker_id = target;
                        entry.sent_at = std::time::Instant::now();
                        entry.retry_count += 1;
                    }
                }
                Err(e) => {
                    println!("   ❌ Reassignment to {} failed: {}", target, e);
                    self.mark_worker_failed(&target).await;
                }
            }
        }
    }

    async fn connect_to_signaling_server(&mut self) -> Result<(), DistributeError> {
        let url = url::Url::parse(&self.ws_url)?;
        let (ws_stream, _) = connect_async(url).await?;
        let (ws_sender, mut ws_receiver) = ws_stream.split();

        let ws_sender = Arc::new(Mutex::new(ws_sender));
        self.ws_sender = Some(ws_sender.clone());

        {
            let mut sender = ws_sender.lock().await;
            let register_msg = serde_json::json!({ "type": "registerMaster" });
            sender
                .send(Message::Text(register_msg.to_string()))
                .await?;
        }

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
                                    if let (Some(from), Some(answer)) = (
                                        parsed.get("from").and_then(|v| v.as_str()),
                                        parsed.get("answer"),
                                    ) {
                                        let peer_connections = peer_connections_arc.lock().await;
                                        if let Some(pc) = peer_connections.get(from) {
                                            if let Some(sdp) =
                                                answer.get("sdp").and_then(|v| v.as_str())
                                            {
                                                if let Ok(answer_desc) =
                                                    RTCSessionDescription::answer(sdp.to_string())
                                                {
                                                    let _ = pc
                                                        .set_remote_description(answer_desc)
                                                        .await;
                                                }
                                            }
                                        }
                                    }
                                }
                                "candidate" => {
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

    #[allow(clippy::too_many_arguments)]
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
        result_sender: mpsc::Sender<WorkerResultMsg>,
        failed_workers_arc: Arc<Mutex<HashMap<String, std::time::Instant>>>,
    ) {
        let worker_id_for_channel = worker_id.clone();
        let worker_id_for_answer = worker_id.clone();

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

            let worker_id_for_state = worker_id.clone();
            let failed_workers_clone = failed_workers_arc.clone();
            pc.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
                let worker_id = worker_id_for_state.clone();
                let failed_workers = failed_workers_clone.clone();
                println!("🔍 Connection state for worker {}: {:?}", worker_id, s);
                if matches!(
                    s,
                    RTCPeerConnectionState::Closed
                        | RTCPeerConnectionState::Failed
                        | RTCPeerConnectionState::Disconnected
                ) {
                    let rt = tokio::runtime::Handle::current();
                    rt.spawn(async move {
                        let mut failed = failed_workers.lock().await;
                        failed.insert(worker_id.clone(), std::time::Instant::now());
                        println!("⚠️  Worker {} connection lost, marked failed", worker_id);
                    });
                }
                Box::pin(async {})
            }));

            let ws_sender_clone = ws_sender.clone();
            let my_id_clone = my_id.clone();
            let worker_id_for_ice = worker_id.clone();
            pc.on_ice_candidate(Box::new(move |candidate| {
                let ws_sender = ws_sender_clone.clone();
                let worker_id = worker_id_for_ice.clone();
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

            pc.on_data_channel(Box::new(move |data_channel| {
                let result_sender = result_sender.clone();
                let worker_id = worker_id_for_channel.clone();
                let data_channels_arc = data_channels_arc.clone();

                Box::pin(async move {
                    println!("📡 Data channel from worker {}", worker_id);
                    {
                        let mut channels = data_channels_arc.lock().await;
                        channels.insert(worker_id.clone(), data_channel.clone());
                    }

                    // One reassembler per channel; results arrive as binary frames.
                    let reassembler = Arc::new(Mutex::new(Reassembler::new()));
                    let worker_id_for_msg = worker_id.clone();
                    data_channel.on_message(Box::new(move |msg| {
                        let result_sender = result_sender.clone();
                        let reassembler = reassembler.clone();
                        let worker_id = worker_id_for_msg.clone();
                        Box::pin(async move {
                            if msg.is_string {
                                return; // text frames are not part of the protocol
                            }
                            let frame = match decode_frame(&msg.data) {
                                Ok(f) => f,
                                Err(e) => {
                                    println!("❌ Bad frame from {}: {}", worker_id, e);
                                    return;
                                }
                            };
                            let completed = {
                                let mut r = reassembler.lock().await;
                                r.accept(frame)
                            };
                            if let Some((ftype, _id, payload)) = completed {
                                if ftype != FRAME_RESULT {
                                    return;
                                }
                                match decode_result_payload(&payload) {
                                    Ok((header, body)) => {
                                        let _ = result_sender
                                            .send(WorkerResultMsg {
                                                task_id: header.task_id,
                                                chunk_index: header.chunk_index,
                                                worker_id: header.worker_id,
                                                error: header.error,
                                                meta: header.meta,
                                                bytes: body,
                                            })
                                            .await;
                                    }
                                    Err(e) => {
                                        println!("❌ Bad result payload from {}: {}", worker_id, e);
                                    }
                                }
                            }
                        })
                    }));
                })
            }));

            if let Some(sdp) = offer.get("sdp").and_then(|v| v.as_str()) {
                if let Ok(offer_desc) = RTCSessionDescription::offer(sdp.to_string()) {
                    if pc.set_remote_description(offer_desc).await.is_ok() {
                        if let Ok(answer) = pc.create_answer(None).await {
                            if pc.set_local_description(answer.clone()).await.is_ok() {
                                let answer_msg = serde_json::json!({
                                    "type": "answer",
                                    "to": worker_id_for_answer,
                                    "from": my_id,
                                    "answer": { "type": "answer", "sdp": answer.sdp }
                                });
                                let mut sender = ws_sender.lock().await;
                                let _ = sender.send(Message::Text(answer_msg.to_string())).await;
                                let mut connections = peer_connections_arc.lock().await;
                                connections.insert(worker_id_for_answer, pc);
                            }
                        }
                    }
                }
            }
        }
    }

    async fn disconnect_from_signaling_server(&mut self) -> Result<(), DistributeError> {
        if let Some(ws_sender) = &self.ws_sender {
            let mut sender = ws_sender.lock().await;
            let _ = sender.close().await;
            println!("🔌 Disconnected from signaling server");
        }
        self.ws_sender = None;
        Ok(())
    }
}

async fn compile_examples_to_wasm() -> Result<(Vec<u8>, String), DistributeError> {
    use std::path::PathBuf;
    println!("🔧 Compiling examples to WASM for distributed execution...");

    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(override_dir) = std::env::var("W3DGE_WASM_EXAMPLES_DIR") {
        candidates.push(PathBuf::from(override_dir));
    }
    let current_dir = std::env::current_dir()?;
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    candidates.push(current_dir.clone());
    candidates.push(current_dir.join("examples"));
    if let Some(parent) = current_dir.parent() {
        candidates.push(parent.join("examples"));
    }
    if let Some(root) = manifest_dir.parent() {
        candidates.push(root.join("examples"));
    }

    let examples_dir = candidates
        .iter()
        .find(|p| p.file_name().and_then(|n| n.to_str()) == Some("examples") && p.exists())
        .cloned()
        .ok_or_else(|| {
            format!(
                "Unable to locate examples directory. Tried: {}",
                candidates
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;

    println!("📁 Using examples directory: {}", examples_dir.display());

    let output = Command::new("wasm-pack")
        .args(["build", "--target", "web", "--out-dir", "pkg"])
        .current_dir(&examples_dir)
        .output()?;
    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        return Err(format!("WASM compilation failed: {}", error_msg).into());
    }

    let wasm_bytes = fs::read(examples_dir.join("pkg").join("distributed_examples_bg.wasm"))?;
    let js_glue = fs::read_to_string(examples_dir.join("pkg").join("distributed_examples.js"))?;
    println!(
        "📦 WASM module {} bytes, JS glue {} bytes",
        wasm_bytes.len(),
        js_glue.len()
    );
    Ok((wasm_bytes, js_glue))
}

/// General byte-based distributed map-reduce with default options.
pub async fn run_distributed_mapreduce_bytes<
    Input,
    ItemOutput,
    FinalOutput,
    ChunkFn,
    ReduceFn,
    ChunkEncodeFn,
    ResultDecodeFn,
>(
    input: Input,
    map_function_name: &str,
    chunker: ChunkFn,
    reducer: ReduceFn,
    chunk_encoder: ChunkEncodeFn,
    result_decoder: ResultDecodeFn,
) -> Result<FinalOutput, DistributeError>
where
    Input: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
    ItemOutput: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
    FinalOutput: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
    ChunkFn: Fn(&Input) -> Vec<Input> + Send + Sync,
    ReduceFn: Fn(Vec<ItemOutput>) -> FinalOutput + Send + Sync,
    ChunkEncodeFn: Fn(&Input) -> (Vec<u8>, serde_json::Value) + Send + Sync,
    ResultDecodeFn: Fn(Vec<u8>, serde_json::Value) -> ItemOutput + Send + Sync,
{
    run_distributed_mapreduce_bytes_opts(
        input,
        map_function_name,
        chunker,
        reducer,
        chunk_encoder,
        result_decoder,
        JobOptions::default(),
    )
    .await
}

/// General byte-based distributed map-reduce.
///
/// Results are decoded and reduced in chunk order. Missing chunks follow
/// `opts.missing_chunks`: `Fail` (default) errors the job, `AllowPartial`
/// skips them while preserving the order of the rest.
#[allow(clippy::too_many_arguments)]
pub async fn run_distributed_mapreduce_bytes_opts<
    Input,
    ItemOutput,
    FinalOutput,
    ChunkFn,
    ReduceFn,
    ChunkEncodeFn,
    ResultDecodeFn,
>(
    input: Input,
    map_function_name: &str,
    chunker: ChunkFn,
    reducer: ReduceFn,
    chunk_encoder: ChunkEncodeFn,
    result_decoder: ResultDecodeFn,
    opts: JobOptions,
) -> Result<FinalOutput, DistributeError>
where
    Input: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
    ItemOutput: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
    FinalOutput: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
    ChunkFn: Fn(&Input) -> Vec<Input> + Send + Sync,
    ReduceFn: Fn(Vec<ItemOutput>) -> FinalOutput + Send + Sync,
    ChunkEncodeFn: Fn(&Input) -> (Vec<u8>, serde_json::Value) + Send + Sync,
    ResultDecodeFn: Fn(Vec<u8>, serde_json::Value) -> ItemOutput + Send + Sync,
{
    println!(
        "🌐 Running distributed byte map with function: {}",
        map_function_name
    );

    let (wasm_bytes, js_glue) = compile_examples_to_wasm().await?;

    let chunks = chunker(&input);
    let mut encoded: Vec<(Vec<u8>, serde_json::Value)> = Vec::with_capacity(chunks.len());
    for ch in chunks.iter() {
        let (bytes, meta) = chunk_encoder(ch);
        if !bytes.is_empty() {
            encoded.push((bytes, meta));
        }
    }
    if encoded.is_empty() {
        return Err("chunker produced no non-empty chunks".into());
    }

    let mut distributor = DistributedCompute::new().await?;
    let results = distributor
        .execute_byte_job(encoded, &wasm_bytes, &js_glue, map_function_name, &opts)
        .await?;

    let mut outputs: Vec<ItemOutput> = Vec::with_capacity(results.len());
    for slot in results {
        if let Some((meta, bytes)) = slot {
            outputs.push(result_decoder(bytes, meta));
        }
    }
    Ok(reducer(outputs))
}

fn extract_numbers_from_value(value: &serde_json::Value) -> Option<Vec<f32>> {
    if let Some(arr) = value.as_array() {
        let mut out = Vec::with_capacity(arr.len());
        for v in arr {
            out.push(v.as_f64()? as f32);
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

fn f32s_to_le_bytes(vals: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(vals.len() * 4);
    for v in vals {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

fn le_bytes_to_f32s(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

/// Numeric convenience API, kept for the numeric example and the benchmark.
///
/// Inputs must serialize to a JSON value containing an f32 array (either a
/// bare array or a `numbers` field). Each chunk's floats travel as raw
/// little-endian bytes over the general byte pipeline and results come back
/// in chunk order.
pub async fn run_distributed_mapreduce<Input, Output, ChunkFn, ReduceFn>(
    input: Input,
    map_function_name: &str,
    chunker: ChunkFn,
    reducer: ReduceFn,
    execution_mode: ExecutionMode,
) -> Result<Output, DistributeError>
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

    let encoder = |chunk: &Input| -> (Vec<u8>, serde_json::Value) {
        let nums = serde_json::to_value(chunk)
            .ok()
            .and_then(|v| extract_numbers_from_value(&v))
            .unwrap_or_default();
        (f32s_to_le_bytes(&nums), serde_json::json!({}))
    };
    let decoder =
        |bytes: Vec<u8>, _meta: serde_json::Value| -> Vec<f32> { le_bytes_to_f32s(&bytes) };
    let float_reducer =
        |per_chunk: Vec<Vec<f32>>| -> Vec<f32> { per_chunk.into_iter().flatten().collect() };

    let mapped: Vec<f32> = run_distributed_mapreduce_bytes(
        input,
        map_function_name,
        chunker,
        float_reducer,
        encoder,
        decoder,
    )
    .await?;

    let mut converted: Vec<Output> = Vec::with_capacity(mapped.len());
    for v in mapped {
        let direct: Result<Output, _> = serde_json::from_value(serde_json::Value::from(v));
        match direct {
            Ok(o) => converted.push(o),
            Err(_) => {
                let wrapped = serde_json::json!({ "value": v });
                match serde_json::from_value::<Output>(wrapped) {
                    Ok(o) => converted.push(o),
                    Err(_) => {
                        return Err(format!(
                            "cannot convert mapped value {} into the requested output type",
                            v
                        )
                        .into())
                    }
                }
            }
        }
    }
    Ok(reducer(converted))
}
