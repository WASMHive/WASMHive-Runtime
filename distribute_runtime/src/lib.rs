use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
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
    decode_frame, decode_result_payload, encode_module_payload, encode_task_payload,
    split_into_frames, Reassembler, TaskHeader, FRAME_CONTROL, FRAME_MODULE, FRAME_RESULT,
    FRAME_TASK,
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

/// Where the WASM module a job ships to workers comes from.
#[derive(Clone)]
pub enum ModuleSource {
    /// Compile the workspace `examples` crate with wasm-pack (the historical default).
    CompileExamplesCrate,
    /// Load a prebuilt wasm-pack `pkg/` directory (any crate, no framework changes).
    PkgDir(std::path::PathBuf),
    /// Bytes supplied directly by the caller.
    Prebuilt { wasm: Vec<u8>, glue: String },
}

impl std::fmt::Debug for ModuleSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ModuleSource::CompileExamplesCrate => write!(f, "CompileExamplesCrate"),
            ModuleSource::PkgDir(p) => write!(f, "PkgDir({})", p.display()),
            ModuleSource::Prebuilt { wasm, glue } => {
                write!(f, "Prebuilt {{ wasm: {}B, glue: {}B }}", wasm.len(), glue.len())
            }
        }
    }
}

impl ModuleSource {
    async fn resolve(&self) -> Result<(Vec<u8>, String), DistributeError> {
        match self {
            ModuleSource::CompileExamplesCrate => compile_examples_to_wasm().await,
            ModuleSource::PkgDir(dir) => load_pkg_dir(dir),
            ModuleSource::Prebuilt { wasm, glue } => Ok((wasm.clone(), glue.clone())),
        }
    }
}

/// Load `<name>_bg.wasm` + `<name>.js` from a wasm-pack output directory.
fn load_pkg_dir(dir: &std::path::Path) -> Result<(Vec<u8>, String), DistributeError> {
    let wasm_path = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with("_bg.wasm"))
                .unwrap_or(false)
        })
        .ok_or_else(|| format!("no *_bg.wasm found in {}", dir.display()))?;
    let glue_name = wasm_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap()
        .trim_end_matches("_bg.wasm")
        .to_string()
        + ".js";
    let wasm = fs::read(&wasm_path)?;
    let glue = fs::read_to_string(dir.join(glue_name))?;
    Ok((wasm, glue))
}

fn module_content_hash(wasm: &[u8], glue: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(wasm);
    hasher.update(glue.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

#[derive(Debug, Clone)]
pub struct JobOptions {
    pub missing_chunks: MissingChunkPolicy,
    /// Per-task timeout before the task is requeued for another worker.
    pub task_timeout_secs: u64,
    /// How many times a chunk may be retried before it counts as missing.
    pub max_retries: u32,
    /// Where the job's WASM module comes from.
    pub module: ModuleSource,
    /// Start dispatching once this many workers are connected.
    pub min_workers: usize,
    /// How long to wait for `min_workers` before starting (or failing if none).
    pub worker_wait_secs: u64,
    /// Tasks kept in flight per worker (2 pipelines transfer behind compute).
    pub max_inflight_per_worker: usize,
}

impl Default for JobOptions {
    fn default() -> Self {
        Self {
            missing_chunks: MissingChunkPolicy::Fail,
            task_timeout_secs: 30,
            max_retries: 3,
            module: ModuleSource::CompileExamplesCrate,
            min_workers: 1,
            worker_wait_secs: 20,
            max_inflight_per_worker: 2,
        }
    }
}

/// A control message from a worker (e.g. need_module).
#[derive(Debug)]
struct ControlMsg {
    worker_id: String,
    value: serde_json::Value,
}

/// A completed worker result delivered from the data-channel handler.
#[derive(Debug)]
struct WorkerResultMsg {
    chunk_index: u32,
    worker_id: String,
    error: Option<String>,
    meta: serde_json::Value,
    bytes: Vec<u8>,
}

/// Lifecycle of one chunk under the pull scheduler.
#[derive(Debug, Clone)]
enum ChunkState {
    Queued,
    InFlight {
        worker_id: String,
        sent_at: std::time::Instant,
    },
    Done,
    Failed,
}

struct ChunkRuntime {
    input: Vec<u8>,
    meta: serde_json::Value,
    state: ChunkState,
    retries: u32,
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
    control_receiver: Option<mpsc::Receiver<ControlMsg>>,
    control_sender: mpsc::Sender<ControlMsg>,
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
        let (control_sender, control_receiver) = mpsc::channel(64);
        Ok(Self {
            ws_url: std::env::var("WASMHIVE_SIGNALING_URL")
                .unwrap_or_else(|_| "ws://localhost:3000".to_string()),
            my_id: None,
            workers: Arc::new(Mutex::new(Vec::new())),
            peer_connections: Arc::new(Mutex::new(HashMap::new())),
            data_channels: Arc::new(Mutex::new(HashMap::new())),
            result_receiver: Some(result_receiver),
            result_sender,
            control_receiver: Some(control_receiver),
            control_sender,
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

    async fn open_channel_to(
        &self,
        worker_id: &str,
    ) -> Result<Arc<RTCDataChannel>, DistributeError> {
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
        Ok(channel)
    }

    /// Send one task to one worker. Tasks carry only the module's content
    /// hash; workers that lack the module request it with need_module.
    async fn send_task(
        &self,
        worker_id: &str,
        header: &TaskHeader,
        input: &[u8],
    ) -> Result<(), DistributeError> {
        let channel = self.open_channel_to(worker_id).await?;
        let payload = encode_task_payload(header, &[], &[], input);
        let frames = split_into_frames(FRAME_TASK, &header.task_id, &payload);
        Self::send_frames(&channel, &frames).await
    }

    /// Ship the job's module to a worker that requested it.
    async fn send_module(
        &self,
        worker_id: &str,
        module_frames: &[Vec<u8>],
    ) -> Result<(), DistributeError> {
        let channel = self.open_channel_to(worker_id).await?;
        Self::send_frames(&channel, module_frames).await
    }

    /// Run one job under the pull scheduler: workers are fed `max_inflight_per_worker`
    /// chunks at a time from a queue and get the next chunk as results return.
    /// Workers that connect mid-job join the rotation automatically. Results
    /// are collected in chunk order; timeouts and errors requeue the chunk.
    async fn execute_byte_job(
        &mut self,
        chunks: Vec<(Vec<u8>, serde_json::Value)>,
        wasm_bytes: &[u8],
        js_glue: &str,
        module_hash: &str,
        map_function_name: &str,
        opts: &JobOptions,
    ) -> Result<Vec<Option<(serde_json::Value, Vec<u8>)>>, DistributeError> {
        self.connect_to_signaling_server().await?;

        // Event-driven readiness: start as soon as enough workers have open
        // channels instead of sleeping a fixed amount.
        let min_workers = opts.min_workers.max(1);
        let wait_deadline =
            std::time::Instant::now() + std::time::Duration::from_secs(opts.worker_wait_secs);
        let ready_workers = loop {
            let available = self.get_available_workers().await;
            if available.len() >= min_workers {
                break available;
            }
            if std::time::Instant::now() > wait_deadline {
                if available.is_empty() {
                    self.disconnect_from_signaling_server().await?;
                    return Err(format!(
                        "no workers connected within {}s",
                        opts.worker_wait_secs
                    )
                    .into());
                }
                println!(
                    "⚠️  Starting with {} workers ({} requested)",
                    available.len(),
                    min_workers
                );
                break available;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        };

        let job_id = uuid::Uuid::new_v4().to_string();
        let total = chunks.len();
        println!(
            "📊 Job {}: {} chunks, {} workers ready, map fn {} (module {})",
            job_id,
            total,
            ready_workers.len(),
            map_function_name,
            &module_hash[..12.min(module_hash.len())]
        );

        // Job state.
        let mut chunk_rt: Vec<ChunkRuntime> = chunks
            .into_iter()
            .map(|(input, meta)| ChunkRuntime {
                input,
                meta,
                state: ChunkState::Queued,
                retries: 0,
            })
            .collect();
        let mut queue: VecDeque<usize> = (0..total).collect();
        let mut results: Vec<Option<(serde_json::Value, Vec<u8>)>> = vec![None; total];
        let mut inflight: HashMap<String, usize> = HashMap::new();
        let mut worker_strikes: HashMap<String, u32> = HashMap::new();
        let mut sent_modules: HashSet<String> = HashSet::new();
        let mut done = 0usize;
        let mut failed = 0usize;

        // Module frames are built once; shipped only to workers that ask.
        let module_frames = split_into_frames(
            FRAME_MODULE,
            module_hash,
            &encode_module_payload(module_hash, wasm_bytes, js_glue.as_bytes()),
        );

        let deadline = std::time::Instant::now()
            + std::time::Duration::from_secs((10 * total as u64).clamp(120, 600));
        let mut last_activity = std::time::Instant::now();
        let mut last_timeout_check = std::time::Instant::now();

        let mut result_rx = self
            .result_receiver
            .take()
            .ok_or("result receiver already taken")?;
        let mut control_rx = self
            .control_receiver
            .take()
            .ok_or("control receiver already taken")?;

        while done + failed < total {
            if std::time::Instant::now() > deadline {
                println!("⏱️  Job deadline reached with {} chunks unresolved", total - done - failed);
                break;
            }
            if last_activity.elapsed().as_secs() > 90 {
                println!("⏱️  No activity for 90s; stopping collection");
                break;
            }

            // Top up every available worker to its in-flight cap. Workers that
            // connected after the job started are picked up here.
            let available = self.get_available_workers().await;
            'workers: for worker_id in &available {
                while *inflight.get(worker_id).unwrap_or(&0) < opts.max_inflight_per_worker {
                    // Next chunk that is actually still queued.
                    let idx = loop {
                        match queue.pop_front() {
                            Some(i) => {
                                if matches!(chunk_rt[i].state, ChunkState::Queued) {
                                    break Some(i);
                                }
                            }
                            None => break None,
                        }
                    };
                    let Some(idx) = idx else { break 'workers };

                    let header = TaskHeader {
                        job_id: job_id.clone(),
                        task_id: format!("{}_{}", job_id, idx),
                        chunk_index: idx as u32,
                        map_function: map_function_name.to_string(),
                        module_hash: module_hash.to_string(),
                        meta: chunk_rt[idx].meta.clone(),
                    };
                    match self.send_task(worker_id, &header, &chunk_rt[idx].input).await {
                        Ok(()) => {
                            chunk_rt[idx].state = ChunkState::InFlight {
                                worker_id: worker_id.clone(),
                                sent_at: std::time::Instant::now(),
                            };
                            *inflight.entry(worker_id.clone()).or_insert(0) += 1;
                        }
                        Err(e) => {
                            println!("   ❌ Send to {} failed: {}", worker_id, e);
                            self.mark_worker_failed(worker_id).await;
                            queue.push_front(idx);
                            continue 'workers;
                        }
                    }
                }
            }

            // Requeue chunks whose worker sat on them past the timeout. A
            // stuck-but-connected worker looks exactly like a slow one; the
            // first result wins and duplicates are ignored via chunk state.
            if last_timeout_check.elapsed().as_secs() >= 5 {
                for idx in 0..total {
                    let ChunkState::InFlight { worker_id, sent_at } = chunk_rt[idx].state.clone()
                    else {
                        continue;
                    };
                    if sent_at.elapsed().as_secs() <= opts.task_timeout_secs {
                        continue;
                    }
                    let strikes = worker_strikes.entry(worker_id.clone()).or_insert(0);
                    *strikes += 1;
                    if *strikes >= WORKER_STRIKE_LIMIT {
                        self.mark_worker_failed(&worker_id).await;
                    }
                    if let Some(n) = inflight.get_mut(&worker_id) {
                        *n = n.saturating_sub(1);
                    }
                    chunk_rt[idx].retries += 1;
                    if chunk_rt[idx].retries > opts.max_retries {
                        println!(
                            "   ☠️  Chunk {} exhausted {} retries; marking missing",
                            idx, opts.max_retries
                        );
                        chunk_rt[idx].state = ChunkState::Failed;
                        failed += 1;
                    } else {
                        println!(
                            "   🔄 Chunk {} timed out on {}; requeued (retry {})",
                            idx, worker_id, chunk_rt[idx].retries
                        );
                        chunk_rt[idx].state = ChunkState::Queued;
                        queue.push_back(idx);
                    }
                }
                last_timeout_check = std::time::Instant::now();
            }

            tokio::select! {
                msg = result_rx.recv() => {
                    let Some(msg) = msg else {
                        println!("   ⚠️  Result channel closed");
                        break;
                    };
                    last_activity = std::time::Instant::now();
                    if let Some(n) = inflight.get_mut(&msg.worker_id) {
                        *n = n.saturating_sub(1);
                    }
                    let idx = msg.chunk_index as usize;
                    if idx >= total {
                        println!("   ⚠️  Result with out-of-range chunk index {}", idx);
                        continue;
                    }
                    if matches!(chunk_rt[idx].state, ChunkState::Done | ChunkState::Failed) {
                        continue; // late duplicate after a requeue
                    }
                    if let Some(err) = msg.error {
                        println!(
                            "   ❌ Worker {} failed chunk {}: {}",
                            msg.worker_id, idx, err
                        );
                        if err.contains("module") {
                            // Module never arrived or was lost; allow a re-send.
                            sent_modules.remove(&msg.worker_id);
                        } else {
                            let strikes = worker_strikes.entry(msg.worker_id.clone()).or_insert(0);
                            *strikes += 1;
                            if *strikes >= WORKER_STRIKE_LIMIT {
                                self.mark_worker_failed(&msg.worker_id).await;
                            }
                        }
                        chunk_rt[idx].retries += 1;
                        if chunk_rt[idx].retries > opts.max_retries {
                            chunk_rt[idx].state = ChunkState::Failed;
                            failed += 1;
                        } else {
                            chunk_rt[idx].state = ChunkState::Queued;
                            queue.push_back(idx);
                        }
                        continue;
                    }
                    chunk_rt[idx].state = ChunkState::Done;
                    results[idx] = Some((msg.meta, msg.bytes));
                    done += 1;
                    if done % 10 == 0 || done == total {
                        println!("   📥 {}/{} results", done, total);
                    }
                }
                ctl = control_rx.recv() => {
                    let Some(ctl) = ctl else { continue };
                    last_activity = std::time::Instant::now();
                    let msg_type = ctl.value.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    if msg_type == "need_module" {
                        let requested = ctl
                            .value
                            .get("module_hash")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        if requested != module_hash {
                            println!(
                                "   ⚠️  {} requested unknown module {}",
                                ctl.worker_id,
                                &requested[..12.min(requested.len())]
                            );
                        } else if sent_modules.contains(&ctl.worker_id) {
                            // Already in flight to this worker; ignore.
                        } else {
                            println!(
                                "   📦 Shipping module ({} frames) to {}",
                                module_frames.len(),
                                ctl.worker_id
                            );
                            sent_modules.insert(ctl.worker_id.clone());
                            if let Err(e) = self.send_module(&ctl.worker_id, &module_frames).await {
                                println!("   ❌ Module send to {} failed: {}", ctl.worker_id, e);
                                sent_modules.remove(&ctl.worker_id);
                                self.mark_worker_failed(&ctl.worker_id).await;
                            }
                        }
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(200)) => {}
            }
        }

        self.result_receiver = Some(result_rx);
        self.control_receiver = Some(control_rx);

        // Anything not done counts as missing.
        let mut missing: Vec<u32> = Vec::new();
        for (idx, c) in chunk_rt.iter().enumerate() {
            if !matches!(c.state, ChunkState::Done) {
                missing.push(idx as u32);
            }
        }

        self.disconnect_from_signaling_server().await?;

        if !missing.is_empty() {
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
        println!("✅ Job complete: {}/{} chunks", done, total);
        Ok(results)
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
        let control_sender = self.control_sender.clone();
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
                                                    control_sender.clone(),
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
        control_sender: mpsc::Sender<ControlMsg>,
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
                let control_sender = control_sender.clone();
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
                        let control_sender = control_sender.clone();
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
                            let Some((ftype, _id, payload)) = completed else {
                                return;
                            };
                            match ftype {
                                FRAME_RESULT => match decode_result_payload(&payload) {
                                    Ok((header, body)) => {
                                        let _ = result_sender
                                            .send(WorkerResultMsg {
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
                                },
                                FRAME_CONTROL => {
                                    match serde_json::from_slice::<serde_json::Value>(&payload) {
                                        Ok(value) => {
                                            let _ = control_sender
                                                .send(ControlMsg {
                                                    worker_id: worker_id.clone(),
                                                    value,
                                                })
                                                .await;
                                        }
                                        Err(e) => {
                                            println!(
                                                "❌ Bad control payload from {}: {}",
                                                worker_id, e
                                            );
                                        }
                                    }
                                }
                                other => {
                                    println!(
                                        "⚠️  Unexpected frame type {} from {}",
                                        other, worker_id
                                    );
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

/// Compiled-examples memo: wasm-pack output is not byte-identical across
/// rebuilds, which would defeat content-hash caching between jobs in the
/// same process (e.g. benchmark iterations). Compile once per process.
static COMPILED_EXAMPLES: std::sync::OnceLock<(Vec<u8>, String)> = std::sync::OnceLock::new();

async fn compile_examples_to_wasm() -> Result<(Vec<u8>, String), DistributeError> {
    if let Some((wasm, glue)) = COMPILED_EXAMPLES.get() {
        return Ok((wasm.clone(), glue.clone()));
    }
    let result = compile_examples_to_wasm_uncached().await?;
    let _ = COMPILED_EXAMPLES.set(result.clone());
    Ok(result)
}

async fn compile_examples_to_wasm_uncached() -> Result<(Vec<u8>, String), DistributeError> {
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

    let (wasm_bytes, js_glue) = opts.module.resolve().await?;
    let module_hash = module_content_hash(&wasm_bytes, &js_glue);

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
        .execute_byte_job(
            encoded,
            &wasm_bytes,
            &js_glue,
            &module_hash,
            map_function_name,
            &opts,
        )
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
