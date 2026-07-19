use std::path::PathBuf;

use async_trait::async_trait;
use videocaptionerr_domain::Transcript;

use crate::application_error::AppResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubtitleFormat {
    Srt,
    Vtt,
    Ass,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubtitleLayout {
    SourceOnly,
    TranslationOnly,
    BilingualSourceFirst,
    BilingualTranslationFirst,
}

pub struct SubtitleExportRequest {
    pub output_path: PathBuf,
    pub format: SubtitleFormat,
    pub layout: SubtitleLayout,
    pub fallback_to_source: bool,
}

pub struct ExportedSubtitle {
    pub path: PathBuf,
    pub content_hash: String,
}

#[async_trait]
pub trait SubtitleGateway: Send + Sync {
    async fn export(
        &self,
        transcript: &Transcript,
        request: SubtitleExportRequest,
    ) -> AppResult<ExportedSubtitle>;
}
