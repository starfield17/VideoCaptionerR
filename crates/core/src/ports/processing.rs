use std::path::{Path, PathBuf};

use crate::application_error::AppResult;
use crate::execution_snapshot::SourceStatSnapshot;
use crate::ports::{SubtitleFormat, SubtitleLayout};
use videocaptionerr_domain::JobId;

/// Filesystem facts captured while a Job is created. The Core use case never
/// reaches into the host filesystem directly; the composition root supplies
/// this adapter for CLI and Desktop alike.
#[derive(Debug, Clone)]
pub struct PreparedMediaFile {
    pub canonical_path: PathBuf,
    pub source_stat: SourceStatSnapshot,
}

pub trait MediaFileCatalog: Send + Sync {
    fn prepare(&self, input: &Path) -> AppResult<PreparedMediaFile>;
}

/// Stable per-Job working directory chosen by the host adapter.
pub trait JobWorkspace: Send + Sync {
    fn directory_for(&self, job_id: &JobId, source_path: &Path) -> PathBuf;
}

#[derive(Debug, Clone)]
pub struct OutputPlanRequest {
    pub source_path: PathBuf,
    pub target_language: Option<String>,
    pub layout: SubtitleLayout,
    pub format: SubtitleFormat,
}

#[derive(Debug, Clone)]
pub struct PlannedOutput {
    pub path: PathBuf,
    pub conflict_policy: String,
}

/// Output naming and conflict reservation are injected policy, not Bootstrap
/// orchestration. The adapter must reserve names across one call's Batch.
pub trait OutputPlanner: Send + Sync {
    fn begin_batch(&self) -> AppResult<()> {
        Ok(())
    }

    fn plan(&self, request: &OutputPlanRequest) -> AppResult<PlannedOutput>;
}

/// Effective, immutable settings for one newly-created Batch. Secrets and
/// mutable config handles intentionally do not cross into this type.
#[derive(Debug, Clone)]
pub struct ProcessProfile {
    pub name: Option<String>,
    pub asr: crate::ports::AsrRuntimeSpec,
    pub llm: Option<crate::use_cases::LlmProcessOptions>,
    pub cache_max_bytes: u64,
}
