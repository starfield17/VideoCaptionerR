//! Artifact metadata contracts.

use serde::{Deserialize, Serialize};

use crate::version::SCHEMA_VERSION;

/// Kind of persisted stage artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    MediaProbe,
    AudioWav,
    AsrRaw,
    Transcript,
    ExportSrt,
    ExportVtt,
    ExportAss,
    ExportReport,
    PromptSnapshot,
    LlmBatch,
    ChunkPlan,
    Other,
}

impl ArtifactKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MediaProbe => "media_probe",
            Self::AudioWav => "audio_wav",
            Self::AsrRaw => "asr_raw",
            Self::Transcript => "transcript",
            Self::ExportSrt => "export_srt",
            Self::ExportVtt => "export_vtt",
            Self::ExportAss => "export_ass",
            Self::ExportReport => "export_report",
            Self::PromptSnapshot => "prompt_snapshot",
            Self::LlmBatch => "llm_batch",
            Self::ChunkPlan => "chunk_plan",
            Self::Other => "other",
        }
    }
}

/// Metadata recorded in SQLite for a committed artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactMeta {
    pub schema_version: u32,
    pub id: String,
    pub job_id: String,
    pub stage: String,
    pub kind: ArtifactKind,
    pub path: String,
    pub content_hash: String,
    pub producer_fingerprint: String,
    pub created_at: String,
    pub committed: bool,
}

impl ArtifactMeta {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: impl Into<String>,
        job_id: impl Into<String>,
        stage: impl Into<String>,
        kind: ArtifactKind,
        path: impl Into<String>,
        content_hash: impl Into<String>,
        producer_fingerprint: impl Into<String>,
        created_at: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            id: id.into(),
            job_id: job_id.into(),
            stage: stage.into(),
            kind,
            path: path.into(),
            content_hash: content_hash.into(),
            producer_fingerprint: producer_fingerprint.into(),
            created_at: created_at.into(),
            committed: false,
        }
    }
}
