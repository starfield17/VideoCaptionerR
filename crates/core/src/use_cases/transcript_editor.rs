//! Application service for revision-CAS subtitle editing.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use videocaptionerr_domain::{
    ArtifactRef, Job, JobId, LlmTextField, StageKind, StageStatus, Transcript,
};

use crate::application_error::{AppResult, ApplicationError};
use crate::ports::{ArtifactStore, IdGenerator, JobRepository, TranscriptCommit};

#[derive(Debug, Clone)]
pub struct EditTranscriptCommand {
    pub job_id: JobId,
    pub cue_id: u32,
    pub expected_transcript_revision: u64,
    pub field: LlmTextField,
    pub value: String,
}

#[derive(Debug)]
pub struct EditTranscriptResponse {
    pub transcript: Transcript,
    pub stage: StageKind,
}

pub struct TranscriptEditor {
    jobs: Arc<dyn JobRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    ids: Arc<dyn IdGenerator>,
}

impl TranscriptEditor {
    pub fn new(
        jobs: Arc<dyn JobRepository>,
        artifacts: Arc<dyn ArtifactStore>,
        ids: Arc<dyn IdGenerator>,
    ) -> Self {
        Self {
            jobs,
            artifacts,
            ids,
        }
    }

    pub async fn load(&self, job_id: &JobId) -> AppResult<Transcript> {
        let job = self.load_job(job_id).await?;
        let (_, artifact) = latest_transcript_artifact(&job)?;
        self.artifacts.load_transcript(&artifact).await
    }

    pub async fn edit(&self, command: EditTranscriptCommand) -> AppResult<EditTranscriptResponse> {
        let mut job = self.load_job(&command.job_id).await?;
        let (stage, previous_artifact) = latest_transcript_artifact(&job)?;
        let transcript = self.artifacts.load_transcript(&previous_artifact).await?;
        let updated = match command.field {
            LlmTextField::Source => transcript.edit_text(
                command.cue_id,
                command.expected_transcript_revision,
                command.value,
            )?,
            LlmTextField::Translation => transcript.edit_translation(
                command.cue_id,
                command.expected_transcript_revision,
                command.value,
            )?,
        };
        let path = next_revision_path(&previous_artifact.path, updated.revision);
        let artifact = self
            .artifacts
            .commit_transcript(TranscriptCommit {
                job_id: command.job_id.clone(),
                stage,
                artifact_id: self.ids.next_id(),
                path,
                transcript: updated.clone(),
                producer_fingerprint: "user-edit".into(),
                work_unit_id: None,
            })
            .await?;
        job.record_transcript_revision(stage, artifact)?;
        self.jobs.save_job(&job).await?;
        Ok(EditTranscriptResponse {
            transcript: updated,
            stage,
        })
    }

    async fn load_job(&self, job_id: &JobId) -> AppResult<Job> {
        self.jobs
            .load_job(job_id)
            .await?
            .ok_or_else(|| ApplicationError::Invalid(format!("Job {job_id} not found")))
    }
}

fn latest_transcript_artifact(job: &Job) -> AppResult<(StageKind, ArtifactRef)> {
    for kind in [
        StageKind::Translate,
        StageKind::Correct,
        StageKind::Split,
        StageKind::Asr,
    ] {
        if let Some(stage) = job.stages().iter().find(|stage| stage.kind == kind) {
            if matches!(stage.status, StageStatus::Done | StageStatus::DoneDegraded)
                && stage.artifact.is_some()
            {
                return Ok((kind, stage.artifact.clone().expect("checked above")));
            }
        }
    }
    Err(ApplicationError::Invalid(format!(
        "Job {} has no committed transcript artifact",
        job.id()
    )))
}

fn next_revision_path(path: &str, revision: u64) -> PathBuf {
    let path = Path::new(path);
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("transcript");
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("json");
    path.parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("{stem}.user-r{revision}.{extension}"))
}
