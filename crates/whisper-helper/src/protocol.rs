//! Stdio NDJSON protocol helpers for the isolated ASR helper.

use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use serde_json::{json, Value};
use videocaptionerr_contracts::protocol::{
    ProtocolEnvelope, ProtocolErrorData, ProtocolMessageType,
};

pub struct HelperState {
    pub session_id: String,
    pub engine: String,
    pub seq_out: AtomicU64,
    pub busy: AtomicBool,
    pub cancel_target: Mutex<Option<u64>>,
    pub model_loaded: AtomicBool,
    pub model_path: Mutex<Option<std::path::PathBuf>>,
    pub device: Mutex<String>,
    pub compute_type: Mutex<String>,
}

impl HelperState {
    pub fn new(engine: String) -> Self {
        Self {
            session_id: ulid::Ulid::new().to_string(),
            engine,
            seq_out: AtomicU64::new(0),
            busy: AtomicBool::new(false),
            cancel_target: Mutex::new(None),
            model_loaded: AtomicBool::new(false),
            model_path: Mutex::new(None),
            device: Mutex::new("cpu".into()),
            compute_type: Mutex::new("default".into()),
        }
    }

    pub fn next_seq(&self) -> u64 {
        self.seq_out.fetch_add(1, Ordering::SeqCst)
    }

    pub fn emit(
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

    pub fn emit_error(
        &self,
        request_id: Option<u64>,
        code: &str,
        message: &str,
    ) -> anyhow::Result<()> {
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

    pub fn is_cancelled(&self, req_id: Option<u64>) -> bool {
        if let Some(t) = *self.cancel_target.lock().unwrap() {
            req_id == Some(t)
        } else {
            false
        }
    }
}

pub fn result_loaded() -> Value {
    json!({"loaded": true})
}

pub fn result_unloaded() -> Value {
    json!({"unloaded": true})
}

pub fn result_shutdown() -> Value {
    json!({"shutdown": true})
}
