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

impl SubtitleFormat {
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "srt" => Some(Self::Srt),
            "vtt" => Some(Self::Vtt),
            "ass" => Some(Self::Ass),
            _ => None,
        }
    }
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
