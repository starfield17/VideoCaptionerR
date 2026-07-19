//! Worker / helper NDJSON protocol envelope and message types.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Protocol major version for stdio NDJSON worker/helper communication.
pub const PROTOCOL_VERSION: u32 = 1;

/// Maximum NDJSON line size (4 MiB).
pub const WORKER_MAX_LINE_BYTES: usize = 4 * 1024 * 1024;

/// Known protocol message types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolMessageType {
    // Requests
    Hello,
    Ping,
    LoadModel,
    Transcribe,
    Cancel,
    UnloadModel,
    Shutdown,
    // Responses / events
    HelloOk,
    Pong,
    Progress,
    Segment,
    Language,
    Result,
    Error,
    Cancelled,
    Log,
}

impl ProtocolMessageType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hello => "hello",
            Self::Ping => "ping",
            Self::LoadModel => "load_model",
            Self::Transcribe => "transcribe",
            Self::Cancel => "cancel",
            Self::UnloadModel => "unload_model",
            Self::Shutdown => "shutdown",
            Self::HelloOk => "hello_ok",
            Self::Pong => "pong",
            Self::Progress => "progress",
            Self::Segment => "segment",
            Self::Language => "language",
            Self::Result => "result",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
            Self::Log => "log",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Result | Self::Error | Self::Cancelled)
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "hello" => Self::Hello,
            "ping" => Self::Ping,
            "load_model" => Self::LoadModel,
            "transcribe" => Self::Transcribe,
            "cancel" => Self::Cancel,
            "unload_model" => Self::UnloadModel,
            "shutdown" => Self::Shutdown,
            "hello_ok" => Self::HelloOk,
            "pong" => Self::Pong,
            "progress" => Self::Progress,
            "segment" => Self::Segment,
            "language" => Self::Language,
            "result" => Self::Result,
            "error" => Self::Error,
            "cancelled" => Self::Cancelled,
            "log" => Self::Log,
            _ => return None,
        })
    }
}

/// NDJSON envelope shared by Python workers and Rust helpers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProtocolEnvelope {
    pub protocol_version: u32,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<u64>,
    pub seq: u64,
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl ProtocolEnvelope {
    pub fn new(
        session_id: impl Into<String>,
        request_id: Option<u64>,
        seq: u64,
        msg_type: ProtocolMessageType,
        data: Option<Value>,
    ) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            session_id: session_id.into(),
            request_id,
            seq,
            msg_type: msg_type.as_str().to_string(),
            data,
        }
    }

    pub fn typed(&self) -> Option<ProtocolMessageType> {
        ProtocolMessageType::parse(&self.msg_type)
    }

    pub fn to_ndjson_line(&self) -> Result<String, serde_json::Error> {
        let mut s = serde_json::to_string(self)?;
        s.push('\n');
        Ok(s)
    }

    pub fn from_ndjson_line(line: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(line.trim_end_matches(['\r', '\n']))
    }

    pub fn validate_version(&self) -> Result<(), String> {
        if self.protocol_version != PROTOCOL_VERSION {
            Err(format!(
                "protocol_version mismatch: got {}, want {}",
                self.protocol_version, PROTOCOL_VERSION
            ))
        } else {
            Ok(())
        }
    }
}

/// Data payload for hello / hello_ok handshake.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HelloData {
    pub engine_id: String,
    pub adapter_version: String,
    pub runtime_version: String,
    #[serde(default)]
    pub devices: Vec<String>,
    #[serde(default)]
    pub native_vad: bool,
    #[serde(default)]
    pub language_detection: bool,
    #[serde(default)]
    pub streaming_events: bool,
    #[serde(default)]
    pub cooperative_cancel: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_audio_secs: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_granularity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence_kind: Option<String>,
}

/// Cancel request payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelData {
    pub target_request_id: u64,
}

/// Progress event payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProgressData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processed_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Streamed segment during inference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentData {
    pub text: String,
    pub start_ms: u64,
    pub end_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub words: Option<Vec<SegmentWord>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentWord {
    pub text: String,
    pub start_ms: u64,
    pub end_ms: u64,
    #[serde(default = "default_prob")]
    pub prob: f32,
}

fn default_prob() -> f32 {
    -1.0
}

/// Terminal result payload (raw ASR before normalization).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AsrResultData {
    pub language: Option<String>,
    pub segments: Vec<SegmentData>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
}

/// Error payload on protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolErrorData {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn envelope_round_trip_ndjson() {
        let env = ProtocolEnvelope::new(
            "01SESSION",
            Some(42),
            7,
            ProtocolMessageType::Segment,
            Some(serde_json::json!({"text": "hi", "start_ms": 0, "end_ms": 100})),
        );
        let line = env.to_ndjson_line().unwrap();
        assert!(line.ends_with('\n'));
        assert!(!line[..line.len() - 1].contains('\n'));
        let back = ProtocolEnvelope::from_ndjson_line(&line).unwrap();
        assert_eq!(env, back);
        assert_eq!(back.typed(), Some(ProtocolMessageType::Segment));
    }

    #[test]
    fn rejects_wrong_protocol_version() {
        let mut env = ProtocolEnvelope::new("s", None, 0, ProtocolMessageType::Hello, None);
        env.protocol_version = 99;
        assert!(env.validate_version().is_err());
    }

    #[test]
    fn terminal_types() {
        assert!(ProtocolMessageType::Result.is_terminal());
        assert!(ProtocolMessageType::Error.is_terminal());
        assert!(ProtocolMessageType::Cancelled.is_terminal());
        assert!(!ProtocolMessageType::Progress.is_terminal());
    }
}
