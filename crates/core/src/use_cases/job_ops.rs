//! Thin application facades for Job lifecycle operations (manual §4.7).

use std::path::PathBuf;
use std::sync::Arc;

use videocaptionerr_domain::{Job, JobId, StageKind};

use crate::application_error::{AppResult, ApplicationError};
use crate::ports::{
    ArtifactStore, JobRepository, SubtitleExportRequest, SubtitleGateway, Versioned,
};

pub struct DeleteJob {
    jobs: Arc<dyn JobRepository>,
}

impl DeleteJob {
    pub fn new(jobs: Arc<dyn JobRepository>) -> Self {
        Self { jobs }
    }

    pub async fn execute(&self, job_id: &JobId) -> AppResult<()> {
        self.jobs.delete_job(job_id).await
    }
}

pub struct ExportJobCommand {
    pub job_id: JobId,
    pub export: SubtitleExportRequest,
}

pub struct ExportJobResponse {
    pub path: PathBuf,
    pub content_hash: String,
}

pub struct ExportJob {
    jobs: Arc<dyn JobRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    subtitles: Arc<dyn SubtitleGateway>,
}

impl ExportJob {
    pub fn new(
        jobs: Arc<dyn JobRepository>,
        artifacts: Arc<dyn ArtifactStore>,
        subtitles: Arc<dyn SubtitleGateway>,
    ) -> Self {
        Self {
            jobs,
            artifacts,
            subtitles,
        }
    }

    pub async fn execute(&self, command: ExportJobCommand) -> AppResult<ExportJobResponse> {
        let job = self.jobs.load_job(&command.job_id).await?.ok_or_else(|| {
            ApplicationError::Invalid(format!("Job {} not found", command.job_id))
        })?;
        let stage = job
            .stages()
            .iter()
            .rev()
            .find(|s| {
                matches!(
                    s.kind,
                    StageKind::Translate | StageKind::Correct | StageKind::Split | StageKind::Asr
                ) && s.artifact.is_some()
            })
            .ok_or_else(|| {
                ApplicationError::Invalid(format!(
                    "Job {} has no exportable transcript stage",
                    command.job_id
                ))
            })?;
        let artifact = stage.artifact.as_ref().unwrap();
        let transcript = self.artifacts.load_transcript(artifact).await?;
        let exported = self.subtitles.export(&transcript, command.export).await?;
        Ok(ExportJobResponse {
            path: exported.path,
            content_hash: exported.content_hash,
        })
    }
}

pub struct ListJobs {
    jobs: Arc<dyn JobRepository>,
}

impl ListJobs {
    pub fn new(jobs: Arc<dyn JobRepository>) -> Self {
        Self { jobs }
    }

    pub async fn execute(&self) -> AppResult<Vec<Versioned<Job>>> {
        self.jobs.list_jobs().await
    }
}

/// Resource-lane limits (application constants; not multi-GPU speculation).
pub mod lanes {
    pub const MAX_CONCURRENT_EXTRACT: usize = 1;
    pub const MAX_CONCURRENT_ASR_SESSIONS: usize = 1;
    pub const MAX_CONCURRENT_LLM: usize = 2;
    pub const MAX_CONCURRENT_EXPORT: usize = 2;
}
