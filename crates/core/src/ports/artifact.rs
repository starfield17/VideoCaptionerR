use std::path::PathBuf;

use async_trait::async_trait;
use videocaptionerr_domain::{ArtifactRef, JobId, StageKind, UlidStr, WorkUnitId};

use crate::application_error::AppResult;
use crate::chunking::ChunkPlan;

pub struct ArtifactCommit {
    pub job_id: JobId,
    pub artifact: ArtifactRef,
    pub work_unit_id: Option<WorkUnitId>,
}

pub struct ArtifactInput {
    pub stage: StageKind,
    pub path: PathBuf,
    pub content_hash: String,
    pub schema_version: u32,
    pub producer_fingerprint: String,
}

pub struct TranscriptCommit {
    pub job_id: JobId,
    pub stage: StageKind,
    pub artifact_id: UlidStr,
    pub path: PathBuf,
    pub transcript: videocaptionerr_domain::Transcript,
    pub producer_fingerprint: String,
    pub work_unit_id: Option<WorkUnitId>,
}

pub struct ChunkPlanCommit {
    pub job_id: JobId,
    pub artifact_id: UlidStr,
    pub path: PathBuf,
    pub plan: ChunkPlan,
    pub producer_fingerprint: String,
}

#[async_trait]
pub trait ArtifactStore: Send + Sync {
    async fn commit(&self, commit: ArtifactCommit) -> AppResult<()>;
    async fn commit_transcript(&self, commit: TranscriptCommit) -> AppResult<ArtifactRef>;
    async fn load_transcript(
        &self,
        artifact: &ArtifactRef,
    ) -> AppResult<videocaptionerr_domain::Transcript>;
    async fn validate(&self, artifact: &ArtifactRef) -> AppResult<()>;
}

#[async_trait]
pub trait ChunkPlanStore: Send + Sync {
    async fn commit(&self, commit: ChunkPlanCommit) -> AppResult<ArtifactRef>;
}
