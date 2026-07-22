//! One-job transcription orchestration.
//!
//! The ASR session is supplied by RunBatch. This use case therefore never
//! loads, unloads, or switches an ASR model.

mod asr;
mod command;
mod commit;
mod long_audio;
mod service;

#[cfg(test)]
mod tests;

pub use command::{LlmProcessOptions, TranscribeJobCommand, TranscribeJobResponse};

pub(crate) use std::path::{Path, PathBuf};
pub(crate) use std::sync::Arc;

pub(crate) use videocaptionerr_contracts::error::{ErrorCode, VcError};
pub(crate) use videocaptionerr_domain::{
    ArtifactRef, BatchId, EngineFingerprint, Job, JobId, StageKind, StageStatus, Transcript,
    UlidStr, WorkUnit, WorkUnitStatus,
};

pub(crate) use super::llm_pipeline::LlmPipelineRequest;
pub(crate) use crate::application_error::{AppResult, ApplicationError};
pub(crate) use crate::chunking::{
    apply_chunk_offset, chunk_cache_key, retain_core_words, ChunkPlan, ChunkPlanOptions,
};
pub(crate) use crate::ports::{
    ArtifactSource, ArtifactStore, AsrCancelToken, AsrSession, AsrTranscribeRequest,
    AudioAnalysisRequest, BatchRepository, CacheRepository, ChunkPlanCommit, ChunkPlanStore, Clock,
    EventPublisher, ExpectedVersion, ExtractAudioRangeRequest, IdGenerator, JobRepository,
    LlmStage, MediaGateway, OutboxEvent, PreparedArtifact, ProbeMediaRequest, PromptSnapshot,
    SnapshotRepository, StageCommitRepository, StageCommitRequest, StructuredOutput,
    SubtitleExportRequest, SubtitleGateway, Versioned, WorkUnitRepository,
};

pub struct TranscribeJob {
    jobs: Arc<dyn JobRepository>,
    media: Arc<dyn MediaGateway>,
    artifacts: Arc<dyn ArtifactStore>,
    subtitles: Arc<dyn SubtitleGateway>,
    events: Arc<dyn EventPublisher>,
    ids: Arc<dyn IdGenerator>,
    stage_commits: Arc<dyn StageCommitRepository>,
    batches: Option<Arc<dyn BatchRepository>>,
    snapshots: Option<Arc<dyn SnapshotRepository>>,
    llm: Option<Arc<super::llm_pipeline::LlmPipeline>>,
    chunking: Option<ChunkingPorts>,
}

struct ChunkingPorts {
    plans: Arc<dyn ChunkPlanStore>,
    cache: Arc<dyn CacheRepository>,
    work_units: Arc<dyn WorkUnitRepository>,
    clock: Arc<dyn Clock>,
}

impl TranscribeJob {
    pub fn new(
        jobs: Arc<dyn JobRepository>,
        media: Arc<dyn MediaGateway>,
        artifacts: Arc<dyn ArtifactStore>,
        subtitles: Arc<dyn SubtitleGateway>,
        events: Arc<dyn EventPublisher>,
        ids: Arc<dyn IdGenerator>,
        stage_commits: Arc<dyn StageCommitRepository>,
    ) -> Self {
        Self {
            jobs,
            media,
            artifacts,
            subtitles,
            events,
            ids,
            stage_commits,
            batches: None,
            snapshots: None,
            llm: None,
            chunking: None,
        }
    }

    pub fn with_snapshots(mut self, snapshots: Arc<dyn SnapshotRepository>) -> Self {
        self.snapshots = Some(snapshots);
        self
    }

    pub fn with_batches(mut self, batches: Arc<dyn BatchRepository>) -> Self {
        self.batches = Some(batches);
        self
    }

    pub fn with_llm_pipeline(mut self, pipeline: Arc<super::llm_pipeline::LlmPipeline>) -> Self {
        self.llm = Some(pipeline);
        self
    }

    pub fn with_chunking(
        mut self,
        plans: Arc<dyn ChunkPlanStore>,
        cache: Arc<dyn CacheRepository>,
        work_units: Arc<dyn WorkUnitRepository>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        self.chunking = Some(ChunkingPorts {
            plans,
            cache,
            work_units,
            clock,
        });
        self
    }
}
