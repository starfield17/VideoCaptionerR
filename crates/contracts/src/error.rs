//! Stable error codes and structured application errors.

use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use videocaptionerr_domain::error::DomainError;

/// Error categories used for retry/degrade decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    /// Automatic retry is appropriate.
    Recoverable,
    /// Fallback preserves usable output.
    Degradable,
    /// Cannot continue without intervention.
    Fatal,
}

/// Stable uppercase error identifiers. Meanings MUST remain stable within v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    InvalidArgument,
    InvalidConfig,
    ConfigMigrationFailed,
    InstanceBusy,
    InputNotFound,
    InputUnsupported,
    ProbeFailed,
    AudioStreamNotFound,
    SourceChanged,
    DiskSpaceInsufficient,
    FfmpegUnavailable,
    FfmpegFailed,
    ModelNotFound,
    ModelDigestMismatch,
    RuntimeUnavailable,
    RuntimeSmokeTestFailed,
    DeviceUnavailable,
    EngineCapabilityInsufficient,
    OptionUnsupported,
    WorkerBusy,
    WorkerStartFailed,
    WorkerProtocolError,
    WorkerTimeout,
    WorkerCrashed,
    AsrOom,
    AsrFailed,
    TimestampInvalid,
    ArtifactCorrupt,
    ArtifactCommitFailed,
    CacheCorrupt,
    LlmAuthFailed,
    LlmModelNotFound,
    LlmRateLimited,
    LlmProviderUnavailable,
    LlmContextExceeded,
    LlmInvalidResponse,
    LlmValidationFailed,
    StaleResult,
    OutputConflict,
    ExportValidationFailed,
    ExportFailed,
    Cancelled,
    PartialBatchSuccess,
    Internal,
}

impl ErrorCode {
    /// Wire/string form of the code (stable SCREAMING_SNAKE_CASE).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::InvalidConfig => "INVALID_CONFIG",
            Self::ConfigMigrationFailed => "CONFIG_MIGRATION_FAILED",
            Self::InstanceBusy => "INSTANCE_BUSY",
            Self::InputNotFound => "INPUT_NOT_FOUND",
            Self::InputUnsupported => "INPUT_UNSUPPORTED",
            Self::ProbeFailed => "PROBE_FAILED",
            Self::AudioStreamNotFound => "AUDIO_STREAM_NOT_FOUND",
            Self::SourceChanged => "SOURCE_CHANGED",
            Self::DiskSpaceInsufficient => "DISK_SPACE_INSUFFICIENT",
            Self::FfmpegUnavailable => "FFMPEG_UNAVAILABLE",
            Self::FfmpegFailed => "FFMPEG_FAILED",
            Self::ModelNotFound => "MODEL_NOT_FOUND",
            Self::ModelDigestMismatch => "MODEL_DIGEST_MISMATCH",
            Self::RuntimeUnavailable => "RUNTIME_UNAVAILABLE",
            Self::RuntimeSmokeTestFailed => "RUNTIME_SMOKE_TEST_FAILED",
            Self::DeviceUnavailable => "DEVICE_UNAVAILABLE",
            Self::EngineCapabilityInsufficient => "ENGINE_CAPABILITY_INSUFFICIENT",
            Self::OptionUnsupported => "OPTION_UNSUPPORTED",
            Self::WorkerBusy => "WORKER_BUSY",
            Self::WorkerStartFailed => "WORKER_START_FAILED",
            Self::WorkerProtocolError => "WORKER_PROTOCOL_ERROR",
            Self::WorkerTimeout => "WORKER_TIMEOUT",
            Self::WorkerCrashed => "WORKER_CRASHED",
            Self::AsrOom => "ASR_OOM",
            Self::AsrFailed => "ASR_FAILED",
            Self::TimestampInvalid => "TIMESTAMP_INVALID",
            Self::ArtifactCorrupt => "ARTIFACT_CORRUPT",
            Self::ArtifactCommitFailed => "ARTIFACT_COMMIT_FAILED",
            Self::CacheCorrupt => "CACHE_CORRUPT",
            Self::LlmAuthFailed => "LLM_AUTH_FAILED",
            Self::LlmModelNotFound => "LLM_MODEL_NOT_FOUND",
            Self::LlmRateLimited => "LLM_RATE_LIMITED",
            Self::LlmProviderUnavailable => "LLM_PROVIDER_UNAVAILABLE",
            Self::LlmContextExceeded => "LLM_CONTEXT_EXCEEDED",
            Self::LlmInvalidResponse => "LLM_INVALID_RESPONSE",
            Self::LlmValidationFailed => "LLM_VALIDATION_FAILED",
            Self::StaleResult => "STALE_RESULT",
            Self::OutputConflict => "OUTPUT_CONFLICT",
            Self::ExportValidationFailed => "EXPORT_VALIDATION_FAILED",
            Self::ExportFailed => "EXPORT_FAILED",
            Self::Cancelled => "CANCELLED",
            Self::PartialBatchSuccess => "PARTIAL_BATCH_SUCCESS",
            Self::Internal => "INTERNAL",
        }
    }

    /// Default category for this code.
    pub fn default_category(self) -> ErrorCategory {
        match self {
            Self::WorkerTimeout
            | Self::WorkerCrashed
            | Self::AsrOom
            | Self::LlmRateLimited
            | Self::LlmProviderUnavailable
            | Self::LlmContextExceeded
            | Self::LlmInvalidResponse => ErrorCategory::Recoverable,
            Self::EngineCapabilityInsufficient
            | Self::OptionUnsupported
            | Self::LlmValidationFailed
            | Self::PartialBatchSuccess => ErrorCategory::Degradable,
            Self::Cancelled => ErrorCategory::Fatal,
            _ => ErrorCategory::Fatal,
        }
    }

    /// All baseline codes for schema generation and tests.
    pub fn all() -> &'static [ErrorCode] {
        &ALL_ERROR_CODES
    }

    /// Parse a stable code string.
    pub fn parse(s: &str) -> Option<Self> {
        ALL_ERROR_CODES.iter().copied().find(|c| c.as_str() == s)
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

const ALL_ERROR_CODES: [ErrorCode; 44] = [
    ErrorCode::InvalidArgument,
    ErrorCode::InvalidConfig,
    ErrorCode::ConfigMigrationFailed,
    ErrorCode::InstanceBusy,
    ErrorCode::InputNotFound,
    ErrorCode::InputUnsupported,
    ErrorCode::ProbeFailed,
    ErrorCode::AudioStreamNotFound,
    ErrorCode::SourceChanged,
    ErrorCode::DiskSpaceInsufficient,
    ErrorCode::FfmpegUnavailable,
    ErrorCode::FfmpegFailed,
    ErrorCode::ModelNotFound,
    ErrorCode::ModelDigestMismatch,
    ErrorCode::RuntimeUnavailable,
    ErrorCode::RuntimeSmokeTestFailed,
    ErrorCode::DeviceUnavailable,
    ErrorCode::EngineCapabilityInsufficient,
    ErrorCode::OptionUnsupported,
    ErrorCode::WorkerBusy,
    ErrorCode::WorkerStartFailed,
    ErrorCode::WorkerProtocolError,
    ErrorCode::WorkerTimeout,
    ErrorCode::WorkerCrashed,
    ErrorCode::AsrOom,
    ErrorCode::AsrFailed,
    ErrorCode::TimestampInvalid,
    ErrorCode::ArtifactCorrupt,
    ErrorCode::ArtifactCommitFailed,
    ErrorCode::CacheCorrupt,
    ErrorCode::LlmAuthFailed,
    ErrorCode::LlmModelNotFound,
    ErrorCode::LlmRateLimited,
    ErrorCode::LlmProviderUnavailable,
    ErrorCode::LlmContextExceeded,
    ErrorCode::LlmInvalidResponse,
    ErrorCode::LlmValidationFailed,
    ErrorCode::StaleResult,
    ErrorCode::OutputConflict,
    ErrorCode::ExportValidationFailed,
    ErrorCode::ExportFailed,
    ErrorCode::Cancelled,
    ErrorCode::PartialBatchSuccess,
    ErrorCode::Internal,
];

/// Structured application error with stable code and actionable message.
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
#[error("{code}: {message}")]
pub struct VcError {
    pub code: ErrorCode,
    pub category: ErrorCategory,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Retry-After supplied by an upstream provider, in milliseconds.
    ///
    /// This is structured metadata rather than part of the human message so
    /// callers can honor it without parsing logs or provider response text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

impl VcError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            category: code.default_category(),
            message: message.into(),
            detail: None,
            job_id: None,
            request_id: None,
            retry_after_ms: None,
        }
    }

    pub fn with_category(mut self, category: ErrorCategory) -> Self {
        self.category = category;
        self
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn with_job_id(mut self, job_id: impl Into<String>) -> Self {
        self.job_id = Some(job_id.into());
        self
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub fn with_retry_after_ms(mut self, retry_after_ms: u64) -> Self {
        self.retry_after_ms = Some(retry_after_ms);
        self
    }
}

pub type VcResult<T> = Result<T, VcError>;

impl From<DomainError> for VcError {
    fn from(error: DomainError) -> Self {
        let (code, message) = match error {
            DomainError::InvalidArgument(message) => (ErrorCode::InvalidArgument, message),
            DomainError::TimestampInvalid(message) => (ErrorCode::TimestampInvalid, message),
            DomainError::IllegalTransition {
                aggregate,
                from,
                to,
            } => (
                ErrorCode::InvalidArgument,
                format!("illegal {aggregate} transition from {from} to {to}"),
            ),
            DomainError::StaleRevision { expected, actual } => (
                ErrorCode::StaleResult,
                format!("stale revision: expected {expected}, actual {actual}"),
            ),
            DomainError::BatchProfileMismatch => (
                ErrorCode::InvalidArgument,
                "batch execution profile mismatch".into(),
            ),
            DomainError::AlreadyTerminal { aggregate } => (
                ErrorCode::InvalidArgument,
                format!("{aggregate} is already terminal"),
            ),
            DomainError::MemberNotFound { aggregate, id } => (
                ErrorCode::InvalidArgument,
                format!("{aggregate} member not found: {id}"),
            ),
            DomainError::LeaseConflict(message) => (ErrorCode::InvalidArgument, message),
            DomainError::LeaseRequired => (
                ErrorCode::InvalidArgument,
                "work unit has no active lease".into(),
            ),
        };
        VcError::new(code, message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_codes_are_unique_and_stable() {
        let mut seen = std::collections::BTreeSet::new();
        for code in ErrorCode::all() {
            assert!(seen.insert(code.as_str()), "duplicate: {}", code.as_str());
            assert_eq!(ErrorCode::parse(code.as_str()), Some(*code));
        }
        assert_eq!(ErrorCode::all().len(), 44);
    }

    #[test]
    fn error_round_trips_json() {
        let err = VcError::new(ErrorCode::WorkerBusy, "worker is busy")
            .with_detail("active request 7")
            .with_job_id("01JOB");
        let json = serde_json::to_string(&err).unwrap();
        let back: VcError = serde_json::from_str(&json).unwrap();
        assert_eq!(back.code, ErrorCode::WorkerBusy);
        assert_eq!(back.message, "worker is busy");
        assert_eq!(back.job_id.as_deref(), Some("01JOB"));
    }
}
