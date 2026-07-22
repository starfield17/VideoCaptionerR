//! Application orchestration for importing an existing subtitle document.

use std::path::PathBuf;
use std::sync::Arc;

use videocaptionerr_domain::{ArtifactRef, Job, JobId, StageKind, SCHEMA_VERSION};

use crate::application_error::AppResult;
use crate::ports::{
    ArtifactSource, ExpectedVersion, IdGenerator, JobWorkspace, PreparedArtifact,
    StageCommitRepository, StageCommitRequest, SubtitleImportLayout, SubtitleImporter, Versioned,
};

#[derive(Debug, Clone)]
pub struct ImportSubtitleCommand {
    pub path: PathBuf,
    pub layout: SubtitleImportLayout,
}

pub struct ImportSubtitleResponse {
    pub job_id: JobId,
    pub cue_count: usize,
    pub warnings: Vec<String>,
    pub transcript_path: PathBuf,
}

/// Creates the imported Job and commits its Split-stage transcript atomically.
/// Parsing and filesystem access stay behind `SubtitleImporter` and
/// `JobWorkspace`; no host adapter or current Profile is consulted here.
pub struct ImportSubtitle {
    ids: Arc<dyn IdGenerator>,
    workspace: Arc<dyn JobWorkspace>,
    importer: Arc<dyn SubtitleImporter>,
    stage_commits: Arc<dyn StageCommitRepository>,
}

impl ImportSubtitle {
    pub fn new(
        ids: Arc<dyn IdGenerator>,
        workspace: Arc<dyn JobWorkspace>,
        importer: Arc<dyn SubtitleImporter>,
        stage_commits: Arc<dyn StageCommitRepository>,
    ) -> Self {
        Self {
            ids,
            workspace,
            importer,
            stage_commits,
        }
    }

    pub async fn execute(
        &self,
        command: ImportSubtitleCommand,
    ) -> AppResult<ImportSubtitleResponse> {
        let imported = self.importer.import(&command.path, command.layout)?;
        let job_id: JobId = self.ids.next_id();
        let profile_revision = self.ids.next_id();
        let job_dir = self.workspace.directory_for(&job_id, &imported.source_path);

        let mut job = Versioned::new(Job::new(
            job_id.clone(),
            None,
            profile_revision,
            imported.source_path.to_string_lossy(),
        ));
        for kind in [StageKind::Probe, StageKind::ExtractAudio, StageKind::Asr] {
            job.skip_stage(kind)?;
        }
        job.start()?;
        job.start_stage(StageKind::Split)?;

        let bytes = serde_json::to_vec_pretty(&imported.transcript).map_err(|error| {
            crate::application_error::ApplicationError::Invalid(format!(
                "encode imported transcript: {error}"
            ))
        })?;
        let transcript_path = job_dir.join("02_split.json");
        let artifact = ArtifactRef {
            id: self.ids.next_id(),
            stage: StageKind::Split,
            path: transcript_path.to_string_lossy().into_owned(),
            content_hash: format!("blake3:{}", blake3::hash(&bytes).to_hex()),
            schema_version: SCHEMA_VERSION,
            producer_fingerprint: "import-subtitle".into(),
        };
        job.complete_stage(StageKind::Split, artifact.clone(), false)?;
        for kind in [StageKind::Correct, StageKind::Translate] {
            job.skip_stage(kind)?;
        }

        self.stage_commits
            .commit_stage(StageCommitRequest {
                job: Some((job, ExpectedVersion::New)),
                work_unit: None,
                artifact: Some(PreparedArtifact {
                    job_id: job_id.clone(),
                    artifact,
                    source: ArtifactSource::Bytes { bytes },
                }),
                event: None,
            })
            .await?;

        Ok(ImportSubtitleResponse {
            job_id,
            cue_count: imported.transcript.cues.len(),
            warnings: imported.warnings,
            transcript_path,
        })
    }
}
