//! Async NDJSON helper client (one active transcription).

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{ChildStdout, Command as TokioCommand};
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio::time::timeout;
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_contracts::protocol::{
    AsrResultData, HelloData, ProtocolEnvelope, ProtocolMessageType, SegmentData, PROTOCOL_VERSION,
};

use crate::descriptor::{ConfidenceKind, DeviceDescriptor, EngineDescriptor, TimestampGranularity};
use crate::engine::{AsrEvent, AsrRawResult};
use crate::options::AsrOptions;

use super::control::WorkerControl;
use super::process::{kill_process_tree, map_protocol_error, send_to_worker};
use super::protocol_session::WorkerProtocolSession;

/// Distinct timeout budgets. Progress is not a heartbeat.
pub const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
pub const LOAD_TIMEOUT: Duration = Duration::from_secs(120);
pub const FIRST_SEGMENT_TIMEOUT: Duration = Duration::from_secs(120);
pub const INTER_SEGMENT_TIMEOUT: Duration = Duration::from_secs(600);
pub const CANCEL_GRACE: Duration = Duration::from_millis(3000);
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

pub struct WorkerClient {
    child: tokio::process::Child,
    stdin: Arc<AsyncMutex<tokio::process::ChildStdin>>,
    stdout: BufReader<ChildStdout>,
    session: WorkerProtocolSession,
    next_request_id: AtomicU64,
    next_seq: Arc<AtomicU64>,
    current_request_id: Arc<AtomicU64>,
    descriptor: Option<EngineDescriptor>,
    helper_path: PathBuf,
    engine_arg: String,
    last_request_id: Option<u64>,
    saw_first_segment: bool,
}

impl WorkerClient {
    pub async fn spawn(helper_path: &Path, engine: &str) -> VcResult<Self> {
        let command = TokioCommand::new(helper_path);
        Self::spawn_command(command, helper_path, engine).await
    }

    pub async fn spawn_python(
        python_path: &Path,
        worker_script: &Path,
        engine: &str,
    ) -> VcResult<Self> {
        if !python_path.exists() {
            return Err(VcError::new(
                ErrorCode::WorkerStartFailed,
                format!("Python runtime not found: {}", python_path.display()),
            ));
        }
        if !worker_script.exists() {
            return Err(VcError::new(
                ErrorCode::WorkerStartFailed,
                format!("Python worker not found: {}", worker_script.display()),
            ));
        }
        let mut command = TokioCommand::new(python_path);
        command.arg(worker_script).arg("--engine").arg(engine);
        Self::spawn_command(command, worker_script, engine).await
    }

    async fn spawn_command(
        mut command: TokioCommand,
        executable_path: &Path,
        engine: &str,
    ) -> VcResult<Self> {
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| {
                VcError::new(ErrorCode::WorkerStartFailed, format!("spawn helper: {e}"))
            })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| VcError::new(ErrorCode::WorkerStartFailed, "helper stdin missing"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| VcError::new(ErrorCode::WorkerStartFailed, "helper stdout missing"))?;

        let mut client = Self {
            child,
            stdin: Arc::new(AsyncMutex::new(stdin)),
            stdout: BufReader::new(stdout),
            session: WorkerProtocolSession::new(),
            next_request_id: AtomicU64::new(1),
            next_seq: Arc::new(AtomicU64::new(0)),
            current_request_id: Arc::new(AtomicU64::new(0)),
            descriptor: None,
            helper_path: executable_path.to_path_buf(),
            engine_arg: engine.to_string(),
            last_request_id: None,
            saw_first_segment: false,
        };
        client.hello().await?;
        Ok(client)
    }

    pub fn descriptor(&self) -> Option<&EngineDescriptor> {
        self.descriptor.as_ref()
    }

    pub fn session_id(&self) -> &str {
        self.session.session_id().unwrap_or("")
    }

    pub fn last_request_id(&self) -> Option<u64> {
        self.last_request_id
    }

    pub fn protocol_session(&self) -> &WorkerProtocolSession {
        &self.session
    }

    pub fn control(&self) -> WorkerControl {
        WorkerControl {
            stdin: self.stdin.clone(),
            session_id: self.session_id().to_owned(),
            next_seq: self.next_seq.clone(),
            current_request_id: self.current_request_id.clone(),
        }
    }

    fn alloc_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::SeqCst)
    }

    async fn send(
        &self,
        request_id: Option<u64>,
        msg_type: ProtocolMessageType,
        data: Option<serde_json::Value>,
    ) -> VcResult<()> {
        send_to_worker(
            &self.stdin,
            self.session_id(),
            &self.next_seq,
            request_id,
            msg_type,
            data,
        )
        .await
    }

    async fn read_envelope(&mut self) -> VcResult<ProtocolEnvelope> {
        let mut line = String::new();
        let n = self.stdout.read_line(&mut line).await.map_err(|e| {
            self.session.end_request();
            VcError::new(ErrorCode::WorkerProtocolError, format!("read stdout: {e}"))
        })?;
        if n == 0 {
            return Err(VcError::new(
                ErrorCode::WorkerCrashed,
                "helper stdout closed",
            ));
        }
        self.session.accept_line(&line)
    }

    async fn read_envelope_timeout(&mut self, dur: Duration) -> VcResult<ProtocolEnvelope> {
        match timeout(dur, self.read_envelope()).await {
            Ok(r) => r,
            Err(_) => Err(VcError::new(
                ErrorCode::WorkerTimeout,
                format!("helper timed out after {dur:?}"),
            )),
        }
    }

    pub async fn hello(&mut self) -> VcResult<&EngineDescriptor> {
        self.send(None, ProtocolMessageType::Hello, None).await?;
        let env = self.read_envelope_timeout(STARTUP_TIMEOUT).await?;
        if env.typed() != Some(ProtocolMessageType::HelloOk) {
            return Err(VcError::new(
                ErrorCode::WorkerProtocolError,
                format!("expected hello_ok, got {}", env.msg_type),
            ));
        }
        let data = env.data.unwrap_or_else(|| serde_json::json!({}));
        let hello: HelloData = serde_json::from_value(data).map_err(|e| {
            VcError::new(
                ErrorCode::WorkerProtocolError,
                format!("hello_ok payload: {e}"),
            )
        })?;

        let mut supported = std::collections::BTreeSet::new();
        supported.insert("language".into());
        supported.insert("word_timestamps".into());

        // Unknown timestamp/confidence capabilities must NOT be promoted to A2.
        let timestamp_granularity = match hello.timestamp_granularity.as_deref() {
            Some("word") => TimestampGranularity::Word,
            Some("character") => TimestampGranularity::Character,
            Some("segment") => TimestampGranularity::Segment,
            Some(other) => {
                return Err(VcError::new(
                    ErrorCode::EngineCapabilityInsufficient,
                    format!("unknown timestamp_granularity capability '{other}'"),
                ));
            }
            None => TimestampGranularity::Segment,
        };
        let confidence_kind = match hello.confidence_kind.as_deref() {
            Some("word_prob") => ConfidenceKind::WordProb,
            Some("log_prob") => ConfidenceKind::LogProb,
            Some("none") => ConfidenceKind::None,
            Some(other) => {
                return Err(VcError::new(
                    ErrorCode::EngineCapabilityInsufficient,
                    format!("unknown confidence_kind capability '{other}'"),
                ));
            }
            None => ConfidenceKind::None,
        };

        let desc = EngineDescriptor {
            protocol_version: PROTOCOL_VERSION,
            engine_id: hello.engine_id,
            adapter_version: hello.adapter_version,
            runtime_version: hello.runtime_version,
            devices: hello
                .devices
                .into_iter()
                .enumerate()
                .map(|(i, id)| DeviceDescriptor {
                    is_default: i == 0,
                    name: id.clone(),
                    id,
                })
                .collect(),
            timestamp_granularity,
            confidence_kind,
            native_vad: hello.native_vad,
            language_detection: hello.language_detection,
            streaming_events: hello.streaming_events,
            cooperative_cancel: hello.cooperative_cancel,
            max_audio_secs: hello.max_audio_secs,
            supported_options: supported,
            unavailable_reason: None,
        };
        self.descriptor = Some(desc);
        Ok(self.descriptor.as_ref().unwrap())
    }

    pub async fn load_model(&mut self, model_path: Option<&Path>) -> VcResult<()> {
        self.load_model_with_options(model_path, "cpu", "default")
            .await
    }

    pub async fn load_model_with_options(
        &mut self,
        model_path: Option<&Path>,
        device: &str,
        compute_type: &str,
    ) -> VcResult<()> {
        let req = self.alloc_request_id();
        self.session.begin_request(req)?;
        let mut payload = serde_json::json!({
            "device": device,
            "compute_type": compute_type,
        });
        if let Some(p) = model_path {
            payload["model_path"] = serde_json::json!(p.to_string_lossy());
        }
        self.send(Some(req), ProtocolMessageType::LoadModel, Some(payload))
            .await?;
        let env = self.read_envelope_timeout(LOAD_TIMEOUT).await;
        self.session.end_request();
        let env = env?;
        match env.typed() {
            Some(ProtocolMessageType::Result) => Ok(()),
            Some(ProtocolMessageType::Error) => Err(map_protocol_error(&env)),
            other => Err(VcError::new(
                ErrorCode::WorkerProtocolError,
                format!("unexpected load_model response: {other:?}"),
            )),
        }
    }

    pub async fn unload_model(&mut self) -> VcResult<()> {
        let req = self.alloc_request_id();
        self.session.begin_request(req)?;
        self.send(Some(req), ProtocolMessageType::UnloadModel, None)
            .await?;
        let env = self.read_envelope_timeout(Duration::from_secs(30)).await;
        self.session.end_request();
        let env = env?;
        match env.typed() {
            Some(ProtocolMessageType::Result) => Ok(()),
            Some(ProtocolMessageType::Error) => Err(map_protocol_error(&env)),
            _ => Ok(()),
        }
    }

    pub async fn ping(&mut self) -> VcResult<()> {
        let req = self.alloc_request_id();
        self.send(Some(req), ProtocolMessageType::Ping, None)
            .await?;
        let env = self.read_envelope_timeout(Duration::from_secs(5)).await?;
        if env.typed() != Some(ProtocolMessageType::Pong) {
            return Err(VcError::new(
                ErrorCode::WorkerProtocolError,
                format!("expected pong, got {}", env.msg_type),
            ));
        }
        Ok(())
    }

    /// Transcribe audio, streaming events to `sink`. Terminal result returned.
    pub async fn transcribe(
        &mut self,
        audio: &Path,
        opts: &AsrOptions,
        sink: mpsc::Sender<AsrEvent>,
        inject_delay_ms: Option<u64>,
    ) -> VcResult<AsrRawResult> {
        let req = self.alloc_request_id();
        self.last_request_id = Some(req);
        self.current_request_id.store(req, Ordering::SeqCst);
        self.session.begin_request(req)?;
        self.saw_first_segment = false;
        let result = self
            .transcribe_request(req, audio, opts, sink, inject_delay_ms)
            .await;
        self.current_request_id.store(0, Ordering::SeqCst);
        self.session.end_request();
        result
    }

    async fn transcribe_request(
        &mut self,
        req: u64,
        audio: &Path,
        opts: &AsrOptions,
        sink: mpsc::Sender<AsrEvent>,
        inject_delay_ms: Option<u64>,
    ) -> VcResult<AsrRawResult> {
        let mut data = serde_json::json!({
            "audio_path": audio.to_string_lossy(),
            "word_timestamps": opts.word_timestamps,
        });
        if let Some(lang) = &opts.language {
            data["language"] = serde_json::json!(lang);
        }
        if let Some(d) = inject_delay_ms {
            data["inject_delay_ms"] = serde_json::json!(d);
        }
        self.send(Some(req), ProtocolMessageType::Transcribe, Some(data))
            .await?;

        let mut language = opts.language.clone();
        let mut segments: Vec<SegmentData> = Vec::new();
        let mut words = Vec::new();
        let mut duration_ms = None;

        loop {
            let wait = if self.saw_first_segment {
                INTER_SEGMENT_TIMEOUT
            } else {
                FIRST_SEGMENT_TIMEOUT
            };
            let env = self.read_envelope_timeout(wait).await?;
            match env.typed() {
                Some(ProtocolMessageType::Pong) => {
                    // Heartbeat response during inference; not progress.
                    continue;
                }
                Some(ProtocolMessageType::Progress) => {
                    if let Some(d) = env.data {
                        let _ = sink
                            .send(AsrEvent::Progress {
                                processed_ms: d.get("processed_ms").and_then(|v| v.as_u64()),
                                total_ms: d.get("total_ms").and_then(|v| v.as_u64()),
                                message: d
                                    .get("message")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string()),
                            })
                            .await;
                    }
                }
                Some(ProtocolMessageType::Segment) => {
                    self.saw_first_segment = true;
                    if let Some(d) = env.data {
                        let seg: SegmentData = serde_json::from_value(d).map_err(|e| {
                            VcError::new(
                                ErrorCode::WorkerProtocolError,
                                format!("segment payload: {e}"),
                            )
                        })?;
                        if let Some(ws) = &seg.words {
                            words.extend(ws.iter().cloned());
                        }
                        let _ = sink.send(AsrEvent::Segment(seg.clone())).await;
                        segments.push(seg);
                    }
                }
                Some(ProtocolMessageType::Language) => {
                    if let Some(d) = env.data {
                        if let Some(l) = d.get("language").and_then(|v| v.as_str()) {
                            language = Some(l.to_string());
                            let _ = sink
                                .send(AsrEvent::Language {
                                    language: l.to_string(),
                                })
                                .await;
                        }
                    }
                }
                Some(ProtocolMessageType::Log) => {
                    if let Some(d) = env.data {
                        let level = d
                            .get("level")
                            .and_then(|v| v.as_str())
                            .unwrap_or("info")
                            .to_string();
                        let message = d
                            .get("message")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let _ = sink.send(AsrEvent::Log { level, message }).await;
                    }
                }
                Some(ProtocolMessageType::Result) => {
                    if let Some(d) = env.data.clone() {
                        if let Ok(raw) = serde_json::from_value::<AsrResultData>(d) {
                            duration_ms = raw.duration_ms;
                            if segments.is_empty() {
                                segments = raw.segments.clone();
                            }
                            if words.is_empty() {
                                for s in &raw.segments {
                                    if let Some(ws) = &s.words {
                                        words.extend(ws.iter().cloned());
                                    }
                                }
                            }
                            if language.is_none() {
                                language = raw.language;
                            }
                        }
                    }
                    let engine_id = self
                        .descriptor
                        .as_ref()
                        .map(|d| d.engine_id.clone())
                        .unwrap_or_else(|| "unknown".into());
                    return Ok(AsrRawResult {
                        language,
                        segments,
                        duration_ms,
                        words,
                        engine_id,
                        model_id: opts
                            .model_path
                            .clone()
                            .unwrap_or_else(|| "unspecified".into()),
                        model_digest: None,
                    });
                }
                Some(ProtocolMessageType::Error) => return Err(map_protocol_error(&env)),
                Some(ProtocolMessageType::Cancelled) => {
                    return Err(VcError::new(
                        ErrorCode::Cancelled,
                        "transcription cancelled",
                    ));
                }
                Some(other) => {
                    return Err(VcError::new(
                        ErrorCode::WorkerProtocolError,
                        format!(
                            "unexpected helper event during transcribe: {}",
                            other.as_str()
                        ),
                    ));
                }
                None => {
                    return Err(VcError::new(
                        ErrorCode::WorkerProtocolError,
                        format!("unknown message type: {}", env.msg_type),
                    ));
                }
            }
        }
    }

    pub async fn cancel(&self, target_request_id: u64) -> VcResult<()> {
        self.control().cancel(target_request_id).await
    }

    /// Cooperative cancel, wait [`CANCEL_GRACE`], then hard-kill the process tree.
    pub async fn cancel_with_escalation(&mut self, target_request_id: u64) -> VcResult<()> {
        let _ = self.cancel(target_request_id).await;
        match timeout(CANCEL_GRACE, self.read_envelope()).await {
            Ok(Ok(env)) if env.typed() == Some(ProtocolMessageType::Cancelled) => Ok(()),
            Ok(Ok(env)) if env.typed().is_some_and(|t| t.is_terminal()) => Ok(()),
            _ => {
                self.kill_tree().await?;
                Err(VcError::new(
                    ErrorCode::Cancelled,
                    "transcription cancelled after hard kill",
                ))
            }
        }
    }

    pub async fn shutdown(&mut self) -> VcResult<()> {
        let req = self.alloc_request_id();
        let _ = self
            .send(Some(req), ProtocolMessageType::Shutdown, None)
            .await;
        let _ = timeout(SHUTDOWN_TIMEOUT, self.child.wait()).await;
        Ok(())
    }

    /// Kill the helper process tree after cancel grace.
    pub async fn kill_tree(&mut self) -> VcResult<()> {
        if let Some(pid) = self.child.id() {
            kill_process_tree(pid);
        }
        let _ = self.child.kill().await;
        Ok(())
    }

    pub fn helper_path(&self) -> &Path {
        &self.helper_path
    }

    pub fn engine_arg(&self) -> &str {
        &self.engine_arg
    }
}
