//! Thin inbound facade for subtitle import.

use std::path::{Path, PathBuf};

use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::ports::SubtitleImportLayout;
use videocaptionerr_core::use_cases::{ImportSubtitleCommand, ImportSubtitleResponse};

use crate::runtime::ApplicationRuntime;

pub struct ImportSubtitleResult {
    pub job_id: String,
    pub cue_count: usize,
    pub warnings: Vec<String>,
    pub transcript_path: PathBuf,
}

impl ApplicationRuntime {
    /// Translate the inbound string option and delegate import orchestration
    /// to Core. The same use case is shared by CLI and Desktop callers.
    pub async fn import_subtitle(
        &self,
        path: &Path,
        layout: Option<&str>,
    ) -> VcResult<ImportSubtitleResult> {
        let layout = SubtitleImportLayout::parse(layout).ok_or_else(|| {
            VcError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "unknown import layout '{}' (mono|source-above|translation-above)",
                    layout.unwrap_or("mono")
                ),
            )
        })?;
        let result: ImportSubtitleResponse = self
            .import_subtitle_uc
            .execute(ImportSubtitleCommand {
                path: path.to_path_buf(),
                layout,
            })
            .await
            .map_err(videocaptionerr_core::application_error::ApplicationError::into_vc_error)?;
        Ok(ImportSubtitleResult {
            job_id: result.job_id.to_string(),
            cue_count: result.cue_count,
            warnings: result.warnings,
            transcript_path: result.transcript_path,
        })
    }
}
