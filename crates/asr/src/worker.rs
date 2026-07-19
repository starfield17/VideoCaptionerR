//! NDJSON worker/helper client over stdio.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout, Command as TokioCommand};
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio::time::timeout;
use tracing::{debug, warn};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_contracts::protocol::{
    AsrResultData, HelloData, ProtocolEnvelope, ProtocolMessageType, SegmentData, PROTOCOL_VERSION,
    WORKER_MAX_LINE_BYTES,
};

use crate::descriptor::{ConfidenceKind, DeviceDescriptor, EngineDescriptor, TimestampGranularity};
use crate::engine::{AsrEvent, AsrRawResult};
use crate::options::AsrOptions;

/// Async NDJSON helper client (one active transcription).
pub struct WorkerClient {
    child: tokio::process::Child,
    stdin: std::sync::Arc<AsyncMutex<ChildStdin>>,
    stdout: BufReader<ChildStdout>,
    session_id: String,
    next_request_id: AtomicU64,
    next_seq: std::sync::Arc<AtomicU64>,
    current_request_id: std::sync::Arc<AtomicU64>,
    descriptor: Option<EngineDescriptor>,
    helper_path: PathBuf,
    engine_arg: String,
    last_request_id: Option<u64>,
}

/// Control path that can be cloned before a long transcription starts.
/// It shares only the worker stdin and sequence allocator; stdout remains
/// owned by the main client reader so terminal messages stay ordered.
#[derive(Clone)]
pub struct WorkerControl {
    stdin: std::sync::Arc<AsyncMutex<ChildStdin>>,
    session_id: String,
    next_seq: std::sync::Arc<AtomicU64>,
    current_request_id: std::sync::Arc<AtomicU64>,
}

impl WorkerControl {
    async fn send(
        &self,
        request_id: Option<u64>,
        msg_type: ProtocolMessageType,
        data: Option<serde_json::Value>,
    ) -> VcResult<()> {
        send_to_worker(
            &self.stdin,
            &self.session_id,
            &self.next_seq,
            request_id,
            msg_type,
            data,
        )
        .await
    }

    pub async fn cancel_current(&self) -> VcResult<()> {
        let target = self.current_request_id.load(Ordering::SeqCst);
        if target == 0 {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                "worker has no active transcription",
            ));
        }
        self.cancel(target).await
    }

    pub async fn cancel(&self, target_request_id: u64) -> VcResult<()> {
        self.send(
            None,
            ProtocolMessageType::Cancel,
            Some(serde_json::json!({"target_request_id": target_request_id})),
        )
        .await
    }

    /// Send a heartbeat while inference is running. The response is consumed
    /// by the main reader and remains subject to the normal protocol checks.
    pub async fn ping(&self) -> VcResult<()> {
        self.send(None, ProtocolMessageType::Ping, None).await
    }
}

impl WorkerClient {
    pub async fn spawn(helper_path: &Path, engine: &str) -> VcResult<Self> {
        let command = TokioCommand::new(helper_path);
        Self::spawn_command(command, helper_path, engine).await
    }

    /// Spawn a managed Python runtime using the same versioned stdio protocol.
    /// The Python script is an adapter boundary; it never owns application
    /// retries, persistence or subtitle splitting.
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
            stdin: std::sync::Arc::new(AsyncMutex::new(stdin)),
            stdout: BufReader::new(stdout),
            session_id: String::new(),
            next_request_id: AtomicU64::new(1),
            next_seq: std::sync::Arc::new(AtomicU64::new(0)),
            current_request_id: std::sync::Arc::new(AtomicU64::new(0)),
            descriptor: None,
            helper_path: executable_path.to_path_buf(),
            engine_arg: engine.to_string(),
            last_request_id: None,
        };
        client.hello().await?;
        Ok(client)
    }

    pub fn descriptor(&self) -> Option<&EngineDescriptor> {
        self.descriptor.as_ref()
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn last_request_id(&self) -> Option<u64> {
        self.last_request_id
    }

    pub fn control(&self) -> WorkerControl {
        WorkerControl {
            stdin: self.stdin.clone(),
            session_id: self.session_id.clone(),
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
            if self.session_id.is_empty() {
                "bootstrap"
            } else {
                &self.session_id
            },
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
            VcError::new(ErrorCode::WorkerProtocolError, format!("read stdout: {e}"))
        })?;
        if n == 0 {
            return Err(VcError::new(
                ErrorCode::WorkerCrashed,
                "helper stdout closed",
            ));
        }
        if line.len() > WORKER_MAX_LINE_BYTES {
            return Err(VcError::new(
                ErrorCode::WorkerProtocolError,
                "helper line exceeds 4 MiB",
            ));
        }
        ProtocolEnvelope::from_ndjson_line(&line).map_err(|e| {
            VcError::new(
                ErrorCode::WorkerProtocolError,
                format!("protocol pollution / invalid json: {e}"),
            )
        })
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
        let env = self.read_envelope_timeout(Duration::from_secs(10)).await?;
        if env.typed() != Some(ProtocolMessageType::HelloOk) {
            return Err(VcError::new(
                ErrorCode::WorkerProtocolError,
                format!("expected hello_ok, got {}", env.msg_type),
            ));
        }
        self.session_id = env.session_id.clone();
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
            timestamp_granularity: match hello.timestamp_granularity.as_deref() {
                Some("word") => TimestampGranularity::Word,
                Some("character") => TimestampGranularity::Character,
                Some("segment") => TimestampGranularity::Segment,
                _ => TimestampGranularity::Word,
            },
            confidence_kind: match hello.confidence_kind.as_deref() {
                Some("word_prob") => ConfidenceKind::WordProb,
                Some("log_prob") => ConfidenceKind::LogProb,
                Some("none") => ConfidenceKind::None,
                _ => ConfidenceKind::WordProb,
            },
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
        let req = self.alloc_request_id();
        let data = match model_path {
            Some(p) => Some(serde_json::json!({"model_path": p.to_string_lossy()})),
            None => Some(serde_json::json!({})),
        };
        self.send(Some(req), ProtocolMessageType::LoadModel, data)
            .await?;
        let env = self.read_envelope_timeout(Duration::from_secs(120)).await?;
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
        self.send(Some(req), ProtocolMessageType::UnloadModel, None)
            .await?;
        let env = self.read_envelope_timeout(Duration::from_secs(30)).await?;
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
        let result = self
            .transcribe_request(req, audio, opts, sink, inject_delay_ms)
            .await;
        self.current_request_id.store(0, Ordering::SeqCst);
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
            let env = self.read_envelope_timeout(Duration::from_secs(600)).await?;
            if let Some(rid) = env.request_id {
                if rid != req {
                    debug!(rid, "ignoring message for other request");
                    continue;
                }
            }
            match env.typed() {
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
                    warn!(msg = other.as_str(), "unexpected helper event");
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

    pub async fn shutdown(&mut self) -> VcResult<()> {
        let req = self.alloc_request_id();
        let _ = self
            .send(Some(req), ProtocolMessageType::Shutdown, None)
            .await;
        let _ = timeout(Duration::from_secs(3), self.child.wait()).await;
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

async fn send_to_worker(
    stdin: &std::sync::Arc<AsyncMutex<ChildStdin>>,
    session_id: &str,
    next_seq: &std::sync::Arc<AtomicU64>,
    request_id: Option<u64>,
    msg_type: ProtocolMessageType,
    data: Option<serde_json::Value>,
) -> VcResult<()> {
    let env = ProtocolEnvelope {
        protocol_version: PROTOCOL_VERSION,
        session_id: if session_id.is_empty() {
            "bootstrap".into()
        } else {
            session_id.to_owned()
        },
        request_id,
        seq: next_seq.fetch_add(1, Ordering::SeqCst),
        msg_type: msg_type.as_str().to_string(),
        data,
    };
    let line = env
        .to_ndjson_line()
        .map_err(|e| VcError::new(ErrorCode::WorkerProtocolError, format!("serialize: {e}")))?;
    let mut writer = stdin.lock().await;
    writer
        .write_all(line.as_bytes())
        .await
        .map_err(|e| VcError::new(ErrorCode::WorkerProtocolError, format!("write stdin: {e}")))?;
    writer
        .flush()
        .await
        .map_err(|e| VcError::new(ErrorCode::WorkerProtocolError, format!("flush stdin: {e}")))?;
    Ok(())
}

fn map_protocol_error(env: &ProtocolEnvelope) -> VcError {
    let (code, message) = env
        .data
        .as_ref()
        .map(|d| {
            (
                d.get("code")
                    .and_then(|v| v.as_str())
                    .unwrap_or("WORKER_PROTOCOL_ERROR"),
                d.get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("worker error")
                    .to_string(),
            )
        })
        .unwrap_or(("WORKER_PROTOCOL_ERROR", "worker error".into()));
    let ec = ErrorCode::parse(code).unwrap_or(ErrorCode::WorkerProtocolError);
    VcError::new(ec, message)
}

/// Unix: kill process group. Windows: best-effort taskkill.
pub fn kill_process_tree(pid: u32) {
    #[cfg(unix)]
    {
        let _ = Command::new("pkill")
            .args(["-TERM", "-P", &pid.to_string()])
            .status();
        let _ = Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status();
        std::thread::sleep(Duration::from_millis(100));
        let _ = Command::new("pkill")
            .args(["-KILL", "-P", &pid.to_string()])
            .status();
        let _ = Command::new("kill")
            .args(["-KILL", &pid.to_string()])
            .status();
    }
    #[cfg(windows)]
    {
        let _ = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .status();
    }
}

/// Resolve helper binary next to current exe or via env / target/debug.
pub fn resolve_helper_binary() -> PathBuf {
    if let Ok(p) = std::env::var("VIDEOCAPTIONERR_HELPER") {
        return PathBuf::from(p);
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let candidate = dir.join("videocaptionerr-whisper-helper");
            if candidate.exists() {
                return candidate;
            }
            #[cfg(windows)]
            {
                let candidate = dir.join("videocaptionerr-whisper-helper.exe");
                if candidate.exists() {
                    return candidate;
                }
            }
        }
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/debug/videocaptionerr-whisper-helper")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn helper_bin() -> PathBuf {
        resolve_helper_binary()
    }

    fn python_bin() -> Option<PathBuf> {
        [
            PathBuf::from("/home/hazel/miniconda3/envs/Lab/bin/python"),
            PathBuf::from("/usr/bin/python3"),
        ]
        .into_iter()
        .find(|path| path.is_file())
    }

    fn python_worker() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/runtimes/worker_common.py")
    }

    #[tokio::test]
    async fn hello_and_transcribe_fake() {
        let bin = helper_bin();
        if !bin.exists() {
            eprintln!("skip: helper not built at {}", bin.display());
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let wav = dir.path().join("t.wav");
        let status = std::process::Command::new("ffmpeg")
            .args([
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:duration=0.3",
                "-ar",
                "16000",
                "-ac",
                "1",
                "-y",
            ])
            .arg(&wav)
            .status();
        if !status.map(|s| s.success()).unwrap_or(false) {
            eprintln!("skip: ffmpeg failed");
            return;
        }

        let mut client = WorkerClient::spawn(&bin, "fake").await.unwrap();
        assert!(client.descriptor().unwrap().supports_full_pipeline());
        client.load_model(None).await.unwrap();
        let (tx, mut rx) = mpsc::channel(32);
        let opts = AsrOptions {
            word_timestamps: true,
            language: Some("en".into()),
            ..Default::default()
        };
        let result = client.transcribe(&wav, &opts, tx, None).await.unwrap();
        assert!(!result.words.is_empty());
        let mut events = 0;
        while rx.try_recv().is_ok() {
            events += 1;
        }
        assert!(events > 0);
        client.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn python_fake_worker_supports_heartbeat_and_control_cancel() {
        let Some(python) = python_bin() else {
            eprintln!("skip: managed Python runtime not installed");
            return;
        };
        let script = python_worker();
        if !script.is_file() {
            eprintln!("skip: Python worker missing at {}", script.display());
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let wav = dir.path().join("python-worker.wav");
        let status = std::process::Command::new("ffmpeg")
            .args([
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:duration=0.3",
                "-ar",
                "16000",
                "-ac",
                "1",
                "-y",
            ])
            .arg(&wav)
            .status();
        if !status.map(|value| value.success()).unwrap_or(false) {
            eprintln!("skip: ffmpeg failed");
            return;
        }

        let mut client = WorkerClient::spawn_python(&python, &script, "fake")
            .await
            .unwrap();
        client.load_model(None).await.unwrap();
        let control = client.control();
        let (sink, _events) = mpsc::channel(32);
        let opts = AsrOptions {
            word_timestamps: true,
            ..Default::default()
        };
        let task =
            tokio::spawn(async move { client.transcribe(&wav, &opts, sink, Some(1000)).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        control.ping().await.unwrap();
        control.cancel_current().await.unwrap();
        let error = task.await.unwrap().unwrap_err();
        assert_eq!(error.code, ErrorCode::Cancelled);
    }

    #[tokio::test]
    async fn python_worker_rejects_dirty_partial_and_oversized_stdout() {
        let Some(python) = python_bin() else {
            eprintln!("skip: managed Python runtime not installed");
            return;
        };
        let cases = [
            ("print('dirty', flush=True)", ErrorCode::WorkerProtocolError),
            (
                "import sys; sys.stdout.write('{\\\"broken\\\"'); sys.stdout.flush()",
                ErrorCode::WorkerProtocolError,
            ),
            (
                "print('x' * (4 * 1024 * 1024 + 1), flush=True)",
                ErrorCode::WorkerProtocolError,
            ),
        ];
        for (index, (source, expected)) in cases.into_iter().enumerate() {
            let dir = tempfile::tempdir().unwrap();
            let script = dir.path().join(format!("bad-{index}.py"));
            fs::write(&script, source).unwrap();
            let error = match WorkerClient::spawn_python(&python, &script, "fake").await {
                Ok(mut client) => {
                    let _ = client.kill_tree().await;
                    panic!("bad worker unexpectedly completed hello")
                }
                Err(error) => error,
            };
            assert_eq!(error.code, expected);
        }
    }
}
