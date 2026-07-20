//! Import SRT/VTT into a Job with a durable Transcript artifact.

use std::path::{Path, PathBuf};

use ulid::Ulid;
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::application_error::ApplicationError;
use videocaptionerr_core::ports::{
    ArtifactSource, ExpectedVersion, PreparedArtifact, StageCommitRequest, Versioned,
};
use videocaptionerr_domain::{ArtifactRef, Job, JobId, StageKind, SCHEMA_VERSION};
use videocaptionerr_platform::sanitize_stem;
use videocaptionerr_platform::subtitle_io::{import_srt, import_vtt, ImportLayout, ImportOptions};

use crate::runtime::ApplicationRuntime;

pub struct ImportSubtitleResult {
    pub job_id: String,
    pub cue_count: usize,
    pub warnings: Vec<String>,
    pub transcript_path: PathBuf,
}

impl ApplicationRuntime {
    /// Import a subtitle file into a new Job. Media/ASR stages are skipped;
    /// the transcript is committed on the Split stage for export/edit.
    pub async fn import_subtitle(
        &self,
        path: &Path,
        layout: Option<&str>,
    ) -> VcResult<ImportSubtitleResult> {
        if !path.is_file() {
            return Err(VcError::new(
                ErrorCode::InputNotFound,
                format!("subtitle file not found: {}", path.display()),
            ));
        }
        let text = std::fs::read_to_string(path).map_err(|e| {
            VcError::new(
                ErrorCode::InputNotFound,
                format!("read subtitle {}: {e}", path.display()),
            )
        })?;
        let layout = parse_import_layout(layout)?;
        let opts = ImportOptions {
            layout,
            source_hash: Some(format!("blake3:{}", blake3::hash(text.as_bytes()).to_hex())),
        };
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        let (transcript, diags) = match ext.as_str() {
            "srt" => import_srt(&text, &opts)?,
            "vtt" => import_vtt(&text, &opts)?,
            other => {
                return Err(VcError::new(
                    ErrorCode::InputUnsupported,
                    format!("unsupported subtitle extension '{other}' (expected srt|vtt)"),
                ));
            }
        };

        let job_id: JobId = Ulid::new().into();
        let profile_revision = Ulid::new().into();
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("subtitle");
        let job_dir = self.paths.job_dir(job_id.as_str(), &sanitize_stem(stem));
        std::fs::create_dir_all(&job_dir).map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("create job dir: {e}"),
            )
        })?;

        let mut job = Versioned::new(Job::new(
            job_id.clone(),
            None,
            profile_revision,
            path.to_string_lossy(),
        ));
        for kind in [StageKind::Probe, StageKind::ExtractAudio, StageKind::Asr] {
            job.skip_stage(kind).map_err(VcError::from)?;
        }
        job.start().map_err(VcError::from)?;
        job.start_stage(StageKind::Split).map_err(VcError::from)?;

        let bytes = serde_json::to_vec_pretty(&transcript).map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode imported transcript: {e}"),
            )
        })?;
        let transcript_path = job_dir.join("02_split.json");
        let artifact = ArtifactRef {
            id: Ulid::new().into(),
            stage: StageKind::Split,
            path: transcript_path.to_string_lossy().into_owned(),
            content_hash: format!("blake3:{}", blake3::hash(&bytes).to_hex()),
            schema_version: SCHEMA_VERSION,
            producer_fingerprint: "import-subtitle".into(),
        };
        job.complete_stage(StageKind::Split, artifact.clone(), false)
            .map_err(VcError::from)?;
        for kind in [StageKind::Correct, StageKind::Translate] {
            job.skip_stage(kind).map_err(VcError::from)?;
        }

        let prepared = PreparedArtifact {
            job_id: job_id.clone(),
            artifact: artifact.clone(),
            source: ArtifactSource::Bytes { bytes },
        };
        self.stage_commits
            .commit_stage(StageCommitRequest {
                job: Some((job, ExpectedVersion::New)),
                work_unit: None,
                artifact: Some(prepared),
                event: None,
            })
            .await
            .map_err(ApplicationError::into_vc_error)?;

        Ok(ImportSubtitleResult {
            job_id: job_id.to_string(),
            cue_count: transcript.cues.len(),
            warnings: diags.warnings,
            transcript_path,
        })
    }
}

fn parse_import_layout(value: Option<&str>) -> VcResult<ImportLayout> {
    match value.unwrap_or("mono").to_ascii_lowercase().as_str() {
        "mono" | "source" => Ok(ImportLayout::Mono),
        "source-above" | "source_above" | "bilingual" => Ok(ImportLayout::SourceAboveTranslation),
        "translation-above" | "translation_above" => Ok(ImportLayout::TranslationAboveSource),
        other => Err(VcError::new(
            ErrorCode::InvalidArgument,
            format!("unknown import layout '{other}' (mono|source-above|translation-above)"),
        )),
    }
}
