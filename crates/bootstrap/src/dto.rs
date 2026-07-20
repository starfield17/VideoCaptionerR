use std::path::PathBuf;

use serde::Serialize;
use videocaptionerr_core::ports::SubtitleLayout;

#[derive(Debug, Clone)]
pub struct TranscribeOptions {
    pub files: Vec<PathBuf>,
    pub language: Option<String>,
    pub format: String,
    pub profile: Option<String>,
    pub target_language: Option<String>,
    pub layout: SubtitleLayout,
}

#[derive(Debug, Clone)]
pub struct ProcessOptions {
    pub files: Vec<PathBuf>,
    pub language: Option<String>,
    pub target_language: String,
    pub format: String,
    pub profile: Option<String>,
}

/// Stable application DTOs used by inbound adapters. Concrete platform and
/// provider types do not cross the desktop boundary.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobSummary {
    pub id: String,
    pub source_path: String,
    pub status: String,
    pub stages: Vec<StageSummary>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StageSummary {
    pub kind: String,
    pub status: String,
    pub artifact_path: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DoctorView {
    pub version: String,
    pub home: String,
    pub database: String,
    pub ffmpeg: Option<String>,
    pub ffprobe: Option<String>,
    pub helper: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FailureView {
    pub job_id: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessView {
    pub jobs: Vec<JobSummary>,
    pub failures: Vec<FailureView>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptEditView {
    pub transcript: videocaptionerr_contracts::Transcript,
    pub stage: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CapabilityProbeView {
    pub provider_profile_id: String,
    pub profile_revision: u64,
    pub model: String,
    pub probe_hash: String,
    pub capabilities: CapabilityView,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CapabilityView {
    pub structured_mode: String,
    pub returns_usage: bool,
    pub seed: bool,
    pub supports_model_list: bool,
    pub max_context_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
}
