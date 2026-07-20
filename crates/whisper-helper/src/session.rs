//! Helper request session: hello / load / transcribe / cancel / unload / shutdown.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use serde_json::json;
use tracing::warn;
use videocaptionerr_contracts::protocol::{
    AsrResultData, CancelData, HelloData, ProgressData, ProtocolEnvelope, ProtocolMessageType,
    SegmentData, WORKER_MAX_LINE_BYTES,
};

use crate::fake_engine;
use crate::protocol::{result_loaded, result_shutdown, result_unloaded, HelperState};
use crate::whisper_cpp_engine;

pub fn handle_line(state: &Arc<HelperState>, line: &str) -> anyhow::Result<bool> {
    // Returns Ok(false) when the session should shut down.
    if line.len() > WORKER_MAX_LINE_BYTES {
        state.emit_error(None, "WORKER_PROTOCOL_ERROR", "line exceeds 4 MiB")?;
        return Ok(true);
    }
    let line = line.trim_end_matches(['\r', '\n']);
    if line.is_empty() {
        return Ok(true);
    }

    let env: ProtocolEnvelope = match ProtocolEnvelope::from_ndjson_line(line) {
        Ok(e) => e,
        Err(e) => {
            warn!("protocol pollution / bad json: {e}");
            state.emit_error(None, "WORKER_PROTOCOL_ERROR", &format!("invalid json: {e}"))?;
            return Ok(true);
        }
    };
    if let Err(e) = env.validate_version() {
        state.emit_error(env.request_id, "WORKER_PROTOCOL_ERROR", &e)?;
        return Ok(true);
    }

    let Some(msg) = env.typed() else {
        state.emit_error(
            env.request_id,
            "WORKER_PROTOCOL_ERROR",
            &format!("unknown message type: {}", env.msg_type),
        )?;
        return Ok(true);
    };

    match msg {
        ProtocolMessageType::Hello => handle_hello(state, &env)?,
        ProtocolMessageType::Ping => {
            state.emit(env.request_id, ProtocolMessageType::Pong, None)?;
        }
        ProtocolMessageType::LoadModel => handle_load(state, &env)?,
        ProtocolMessageType::UnloadModel => {
            if state.engine == "whisper-cpp" {
                whisper_cpp_engine::unload();
            }
            state.model_loaded.store(false, Ordering::SeqCst);
            *state.model_path.lock().unwrap() = None;
            state.emit(
                env.request_id,
                ProtocolMessageType::Result,
                Some(result_unloaded()),
            )?;
        }
        ProtocolMessageType::Transcribe => handle_transcribe(state.clone(), env)?,
        ProtocolMessageType::Cancel => {
            if let Some(data) = env.data.clone() {
                if let Ok(c) = serde_json::from_value::<CancelData>(data) {
                    *state.cancel_target.lock().unwrap() = Some(c.target_request_id);
                }
            }
        }
        ProtocolMessageType::Shutdown => {
            state.emit(
                env.request_id,
                ProtocolMessageType::Result,
                Some(result_shutdown()),
            )?;
            return Ok(false);
        }
        other => {
            state.emit_error(
                env.request_id,
                "WORKER_PROTOCOL_ERROR",
                &format!("unexpected request type: {}", other.as_str()),
            )?;
        }
    }
    Ok(true)
}

fn handle_hello(state: &HelperState, env: &ProtocolEnvelope) -> anyhow::Result<()> {
    let (engine_id, runtime_version, confidence) = if state.engine == "fake" {
        (
            "fake-helper".into(),
            "fake".into(),
            Some("word_prob".into()),
        )
    } else {
        (
            "whisper-cpp".into(),
            whisper_cpp_engine::runtime_version(),
            Some("word_prob".into()),
        )
    };
    let hello = HelloData {
        engine_id,
        adapter_version: env!("CARGO_PKG_VERSION").into(),
        runtime_version,
        devices: vec!["cpu".into()],
        native_vad: false,
        language_detection: true,
        streaming_events: true,
        cooperative_cancel: true,
        max_audio_secs: Some(3600),
        timestamp_granularity: Some("word".into()),
        confidence_kind: confidence,
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
    let data = env.data.clone().unwrap_or_else(|| json!({}));
    let path = data
        .get("model_path")
        .and_then(|v| v.as_str())
        .map(std::path::PathBuf::from);
    if let Some(device) = data.get("device").and_then(|v| v.as_str()) {
        *state.device.lock().unwrap() = device.to_string();
    }
    if let Some(ct) = data.get("compute_type").and_then(|v| v.as_str()) {
        *state.compute_type.lock().unwrap() = ct.to_string();
    }

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
        if let Err(e) = whisper_cpp_engine::load(p) {
            return state.emit_error(
                env.request_id,
                "RUNTIME_SMOKE_TEST_FAILED",
                &format!("model load failed: {e:#}"),
            );
        }
    }

    *state.model_path.lock().unwrap() = path;
    state.model_loaded.store(true, Ordering::SeqCst);
    state.emit(
        env.request_id,
        ProtocolMessageType::Result,
        Some(result_loaded()),
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
    let path = std::path::PathBuf::from(audio);
    if !path.is_file() {
        state.emit_error(
            req_id,
            "INPUT_NOT_FOUND",
            &format!("audio not found: {}", path.display()),
        )?;
        return Ok(());
    }

    let cancelled = || state.is_cancelled(req_id);

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

    let (duration_ms, words, lang) = if engine == "fake" {
        let (d, w) = fake_engine::transcribe(&path)?;
        (d, w, language.unwrap_or("en").to_string())
    } else {
        match whisper_cpp_engine::transcribe(&path, language, &cancelled) {
            Ok(v) => v,
            Err(e) if e.to_string().contains("cancelled") => {
                state.emit(req_id, ProtocolMessageType::Cancelled, None)?;
                return Ok(());
            }
            Err(e) if e.to_string().contains("not linked") => {
                state.emit_error(req_id, "RUNTIME_UNAVAILABLE", &e.to_string())?;
                return Ok(());
            }
            Err(e) => {
                state.emit_error(
                    req_id,
                    "ASR_FAILED",
                    &format!("transcription failed: {e:#}"),
                )?;
                return Ok(());
            }
        }
    };

    if cancelled() {
        state.emit(req_id, ProtocolMessageType::Cancelled, None)?;
        return Ok(());
    }

    // Refuse A2 claim when no word timestamps were produced.
    if words.is_empty() {
        state.emit_error(
            req_id,
            "ENGINE_CAPABILITY_INSUFFICIENT",
            "engine produced no word timestamps",
        )?;
        return Ok(());
    }

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
