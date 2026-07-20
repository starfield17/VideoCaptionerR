//! Process-tree helpers for worker lifecycle.

use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::process::ChildStdin;
use tokio::sync::Mutex as AsyncMutex;
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_contracts::protocol::{
    ProtocolEnvelope, ProtocolMessageType, PROTOCOL_VERSION,
};

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

pub(super) async fn send_to_worker(
    stdin: &Arc<AsyncMutex<ChildStdin>>,
    session_id: &str,
    next_seq: &Arc<AtomicU64>,
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

pub(super) fn map_protocol_error(env: &ProtocolEnvelope) -> VcError {
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
