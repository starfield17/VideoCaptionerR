//! Isolated ASR helper process (stdio NDJSON protocol).
//!
//! stdout: protocol only
//! stderr: logs
//!
//! Engines:
//! - `fake` (default): deterministic word timestamps for protocol/e2e tests
//! - `whisper-cpp` (later): native whisper.cpp FFI behind the same protocol

use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use clap::Parser;
use serde_json::{json, Value};
use tracing::{error, info, warn};
use videocaptionerr_contracts::protocol::{
    AsrResultData, CancelData, HelloData, ProgressData, ProtocolEnvelope, ProtocolErrorData,
    ProtocolMessageType, SegmentData, SegmentWord, WORKER_MAX_LINE_BYTES,
};

#[derive(Debug, Parser)]
#[command(name = "videocaptionerr-whisper-helper")]
struct Args {
    /// Engine implementation: fake | whisper-cpp
    #[arg(long, default_value = "fake")]
    engine: String,
}

fn main() {
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    if let Err(e) = run(args) {
        error!("helper fatal: {e:#}");
        std::process::exit(1);
    }
}

struct HelperState {
    session_id: String,
    engine: String,
    seq_out: AtomicU64,
    busy: AtomicBool,
    cancel_target: Mutex<Option<u64>>,
    model_loaded: AtomicBool,
    model_path: Mutex<Option<PathBuf>>,
}

impl HelperState {
    fn new(engine: String) -> Self {
        Self {
            session_id: ulid::Ulid::new().to_string(),
            engine,
            seq_out: AtomicU64::new(0),
            busy: AtomicBool::new(false),
            cancel_target: Mutex::new(None),
            model_loaded: AtomicBool::new(false),
            model_path: Mutex::new(None),
        }
    }

    fn next_seq(&self) -> u64 {
        self.seq_out.fetch_add(1, Ordering::SeqCst)
    }

    fn emit(
        &self,
        request_id: Option<u64>,
        msg_type: ProtocolMessageType,
        data: Option<Value>,
    ) -> anyhow::Result<()> {
        let env = ProtocolEnvelope::new(
            self.session_id.clone(),
            request_id,
            self.next_seq(),
            msg_type,
            data,
        );
        let line = env.to_ndjson_line()?;
        let mut out = io::stdout().lock();
        out.write_all(line.as_bytes())?;
        out.flush()?;
        Ok(())
    }

    fn emit_error(&self, request_id: Option<u64>, code: &str, message: &str) -> anyhow::Result<()> {
        let payload = ProtocolErrorData {
            code: code.into(),
            message: message.into(),
            detail: None,
        };
        self.emit(
            request_id,
            ProtocolMessageType::Error,
            Some(serde_json::to_value(payload)?),
        )
    }
}

fn run(args: Args) -> anyhow::Result<()> {
    let state = Arc::new(HelperState::new(args.engine));
    info!(
        engine = %state.engine,
        session = %state.session_id,
        "whisper-helper started"
    );

    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    let mut line_buf = Vec::new();

    loop {
        line_buf.clear();
        let n = reader.read_until(b'\n', &mut line_buf)?;
        if n == 0 {
            info!("stdin closed; shutting down");
            break;
        }
        if line_buf.len() > WORKER_MAX_LINE_BYTES {
            state.emit_error(None, "WORKER_PROTOCOL_ERROR", "line exceeds 4 MiB")?;
            continue;
        }
        let line = String::from_utf8_lossy(&line_buf);
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            continue;
        }

        let env: ProtocolEnvelope = match ProtocolEnvelope::from_ndjson_line(line) {
            Ok(e) => e,
            Err(e) => {
                warn!("protocol pollution / bad json: {e}");
                state.emit_error(None, "WORKER_PROTOCOL_ERROR", &format!("invalid json: {e}"))?;
                continue;
            }
        };
        if let Err(e) = env.validate_version() {
            state.emit_error(env.request_id, "WORKER_PROTOCOL_ERROR", &e)?;
            continue;
        }

        let Some(msg) = env.typed() else {
            state.emit_error(
                env.request_id,
                "WORKER_PROTOCOL_ERROR",
                &format!("unknown message type: {}", env.msg_type),
            )?;
            // Unknown critical types: protocol error; caller may restart.
            continue;
        };

        match msg {
            ProtocolMessageType::Hello => handle_hello(&state, &env)?,
            ProtocolMessageType::Ping => {
                state.emit(env.request_id, ProtocolMessageType::Pong, None)?;
            }
            ProtocolMessageType::LoadModel => handle_load(&state, &env)?,
            ProtocolMessageType::UnloadModel => {
                state.model_loaded.store(false, Ordering::SeqCst);
                *state.model_path.lock().unwrap() = None;
                state.emit(
                    env.request_id,
                    ProtocolMessageType::Result,
                    Some(json!({"unloaded": true})),
                )?;
            }
            ProtocolMessageType::Transcribe => handle_transcribe(state.clone(), env)?,
            ProtocolMessageType::Cancel => {
                if let Some(data) = env.data.clone() {
                    if let Ok(c) = serde_json::from_value::<CancelData>(data) {
                        *state.cancel_target.lock().unwrap() = Some(c.target_request_id);
                    }
                }
                // Non-terminal ack is not required; cancel completes via cancelled/result.
            }
            ProtocolMessageType::Shutdown => {
                state.emit(
                    env.request_id,
                    ProtocolMessageType::Result,
                    Some(json!({"shutdown": true})),
                )?;
                break;
            }
            other => {
                state.emit_error(
                    env.request_id,
                    "WORKER_PROTOCOL_ERROR",
                    &format!("unexpected request type: {}", other.as_str()),
                )?;
            }
        }
    }
    Ok(())
}

fn handle_hello(state: &HelperState, env: &ProtocolEnvelope) -> anyhow::Result<()> {
    let hello = HelloData {
        engine_id: if state.engine == "fake" {
            "fake-helper".into()
        } else {
            "whisper-cpp".into()
        },
        adapter_version: env!("CARGO_PKG_VERSION").into(),
        runtime_version: state.engine.clone(),
        devices: vec!["cpu".into()],
        native_vad: false,
        language_detection: true,
        streaming_events: true,
        cooperative_cancel: true,
        max_audio_secs: Some(3600),
        timestamp_granularity: Some("word".into()),
        confidence_kind: Some("word_prob".into()),
    };
    state.emit(
        env.request_id,
        ProtocolMessageType::HelloOk,
        Some(serde_json::to_value(hello)?),
    )
}

fn handle_load(state: &HelperState, env: &ProtocolEnvelope) -> anyhow::Result<()> {
    if state.busy.load(Ordering::SeqCst) {
        return state.emit_error(env.request_id, "WORKER_BUSY", "worker is busy");
    }
    let path = env
        .data
        .as_ref()
        .and_then(|d| d.get("model_path"))
        .and_then(|v| v.as_str())
        .map(PathBuf::from);

    if state.engine != "fake" {
        let Some(ref p) = path else {
            return state.emit_error(env.request_id, "MODEL_NOT_FOUND", "model_path required");
        };
        if !p.is_file() {
            return state.emit_error(
                env.request_id,
                "MODEL_NOT_FOUND",
                &format!("model not found: {}", p.display()),
            );
        }
    }

    *state.model_path.lock().unwrap() = path;
    state.model_loaded.store(true, Ordering::SeqCst);
    state.emit(
        env.request_id,
        ProtocolMessageType::Result,
        Some(json!({"loaded": true})),
    )
}

fn handle_transcribe(state: Arc<HelperState>, env: ProtocolEnvelope) -> anyhow::Result<()> {
    if state.busy.swap(true, Ordering::SeqCst) {
        return state.emit_error(
            env.request_id,
            "WORKER_BUSY",
            "transcription already active",
        );
    }
    if !state.model_loaded.load(Ordering::SeqCst) && state.engine != "fake" {
        state.busy.store(false, Ordering::SeqCst);
        return state.emit_error(env.request_id, "MODEL_NOT_FOUND", "model not loaded");
    }

    let req_id = env.request_id;
    let data = env.data.clone().unwrap_or_else(|| json!({}));
    let audio = data
        .get("audio_path")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let language = data
        .get("language")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let inject_delay_ms = data
        .get("inject_delay_ms")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let engine = state.engine.clone();

    // Single blocking inference executor (dedicated thread).
    thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_transcribe(
                &state,
                req_id,
                &audio,
                language.as_deref(),
                inject_delay_ms,
                &engine,
            )
        }));
        state.busy.store(false, Ordering::SeqCst);
        *state.cancel_target.lock().unwrap() = None;
        if let Err(e) = result {
            let msg = if let Some(s) = e.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = e.downcast_ref::<String>() {
                s.clone()
            } else {
                "helper panicked".into()
            };
            let _ = state.emit_error(req_id, "WORKER_CRASHED", &msg);
        }
    });
    Ok(())
}

fn run_transcribe(
    state: &HelperState,
    req_id: Option<u64>,
    audio: &str,
    language: Option<&str>,
    inject_delay_ms: u64,
    engine: &str,
) -> anyhow::Result<()> {
    if audio.is_empty() {
        state.emit_error(req_id, "INVALID_ARGUMENT", "audio_path required")?;
        return Ok(());
    }
    let path = PathBuf::from(audio);
    if !path.is_file() {
        state.emit_error(
            req_id,
            "INPUT_NOT_FOUND",
            &format!("audio not found: {}", path.display()),
        )?;
        return Ok(());
    }

    // Cooperative cancel check helper.
    let cancelled = || {
        if let Some(t) = *state.cancel_target.lock().unwrap() {
            req_id == Some(t)
        } else {
            false
        }
    };

    if inject_delay_ms > 0 {
        let steps = (inject_delay_ms / 50).max(1);
        for i in 0..steps {
            if cancelled() {
                state.emit(req_id, ProtocolMessageType::Cancelled, None)?;
                return Ok(());
            }
            thread::sleep(Duration::from_millis(50));
            let _ = state.emit(
                req_id,
                ProtocolMessageType::Progress,
                Some(serde_json::to_value(ProgressData {
                    processed_ms: Some(i * 50),
                    total_ms: Some(inject_delay_ms),
                    message: Some("waiting".into()),
                })?),
            );
        }
    }

    if cancelled() {
        state.emit(req_id, ProtocolMessageType::Cancelled, None)?;
        return Ok(());
    }

    let (duration_ms, words) = if engine == "fake" {
        fake_transcribe(&path)?
    } else {
        // whisper-cpp path not fully linked yet: fail clearly if selected without support.
        state.emit_error(
            req_id,
            "RUNTIME_UNAVAILABLE",
            "whisper-cpp engine not built into this helper binary yet; use --engine fake or wait for M2 native link",
        )?;
        return Ok(());
    };

    if cancelled() {
        state.emit(req_id, ProtocolMessageType::Cancelled, None)?;
        return Ok(());
    }

    let lang = language.unwrap_or("en").to_string();
    state.emit(
        req_id,
        ProtocolMessageType::Language,
        Some(json!({"language": lang})),
    )?;

    let text: String = words
        .iter()
        .map(|w| w.text.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    let end_ms = words.last().map(|w| w.end_ms).unwrap_or(duration_ms);
    let segment = SegmentData {
        text: text.clone(),
        start_ms: 0,
        end_ms,
        words: Some(words.clone()),
    };
    state.emit(
        req_id,
        ProtocolMessageType::Segment,
        Some(serde_json::to_value(&segment)?),
    )?;
    state.emit(
        req_id,
        ProtocolMessageType::Progress,
        Some(serde_json::to_value(ProgressData {
            processed_ms: Some(end_ms),
            total_ms: Some(duration_ms.max(end_ms)),
            message: Some("done".into()),
        })?),
    )?;

    let result = AsrResultData {
        language: Some(lang),
        segments: vec![segment],
        duration_ms: Some(duration_ms.max(end_ms)),
    };
    state.emit(
        req_id,
        ProtocolMessageType::Result,
        Some(serde_json::to_value(result)?),
    )?;
    Ok(())
}

fn fake_transcribe(path: &PathBuf) -> anyhow::Result<(u64, Vec<SegmentWord>)> {
    let duration_ms = wav_duration_ms(path).unwrap_or(1000);
    // Deterministic placeholder aligned to duration.
    let words = vec![
        SegmentWord {
            text: "hello".into(),
            start_ms: 0,
            end_ms: (duration_ms / 4).max(1),
            prob: 0.95,
        },
        SegmentWord {
            text: "from".into(),
            start_ms: duration_ms / 4,
            end_ms: duration_ms / 2,
            prob: 0.9,
        },
        SegmentWord {
            text: "whisper".into(),
            start_ms: duration_ms / 2,
            end_ms: (duration_ms * 3) / 4,
            prob: 0.92,
        },
        SegmentWord {
            text: "helper".into(),
            start_ms: (duration_ms * 3) / 4,
            end_ms: duration_ms.max(1),
            prob: 0.91,
        },
    ];
    Ok((duration_ms.max(1), words))
}

fn wav_duration_ms(path: &PathBuf) -> Option<u64> {
    let reader = hound::WavReader::open(path).ok()?;
    let spec = reader.spec();
    let len = reader.duration() as u64; // samples per channel
    if spec.sample_rate == 0 {
        return None;
    }
    Some(len.saturating_mul(1000) / u64::from(spec.sample_rate))
}
