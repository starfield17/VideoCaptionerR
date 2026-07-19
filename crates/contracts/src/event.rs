//! CLI / GUI event envelopes.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::version::SCHEMA_VERSION;

/// Machine-readable CLI event (`--json` NDJSON to stdout).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub schema_version: u32,
    pub event_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    pub timestamp: DateTime<Utc>,
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl EventEnvelope {
    pub fn new(
        event_id: impl Into<String>,
        job_id: Option<String>,
        event_type: impl Into<String>,
        data: Option<Value>,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            event_id: event_id.into(),
            job_id,
            timestamp: Utc::now(),
            event_type: event_type.into(),
            data,
        }
    }

    pub fn to_ndjson_line(&self) -> Result<String, serde_json::Error> {
        let mut s = serde_json::to_string(self)?;
        s.push('\n');
        Ok(s)
    }
}

/// Common CLI event type names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliEvent {
    JobCreated,
    JobStarted,
    StageStarted,
    StageFinished,
    Progress,
    WorkUnitStarted,
    WorkUnitFinished,
    WorkUnitFailed,
    Warning,
    Error,
    JobFinished,
    Cancelled,
}

impl CliEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::JobCreated => "job_created",
            Self::JobStarted => "job_started",
            Self::StageStarted => "stage_started",
            Self::StageFinished => "stage_finished",
            Self::Progress => "progress",
            Self::WorkUnitStarted => "work_unit_started",
            Self::WorkUnitFinished => "work_unit_finished",
            Self::WorkUnitFailed => "work_unit_failed",
            Self::Warning => "warning",
            Self::Error => "error",
            Self::JobFinished => "job_finished",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Suggested process exit codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ExitCode {
    Success = 0,
    InvalidArgs = 2,
    DependencyUnavailable = 3,
    InputFailure = 4,
    AsrFailure = 5,
    LlmFailure = 6,
    ExportFailure = 7,
    Cancelled = 8,
    PartialBatchSuccess = 9,
}

impl ExitCode {
    pub fn as_i32(self) -> i32 {
        self as i32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_round_trip() {
        let ev = EventEnvelope::new(
            "01EV",
            Some("01JOB".into()),
            CliEvent::Progress.as_str(),
            Some(serde_json::json!({"pct": 0.5})),
        );
        let line = ev.to_ndjson_line().unwrap();
        let back: EventEnvelope = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(back.event_type, "progress");
        assert_eq!(back.job_id.as_deref(), Some("01JOB"));
    }
}
