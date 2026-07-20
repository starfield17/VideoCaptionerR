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

impl SubtitleLayout {
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "source_only" | "source" => Some(Self::SourceOnly),
            "translation_only" | "translation" => Some(Self::TranslationOnly),
            "bilingual_source_first" | "bilingual" => Some(Self::BilingualSourceFirst),
            "bilingual_translation_first" => Some(Self::BilingualTranslationFirst),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::SourceOnly => "source_only",
            Self::TranslationOnly => "translation_only",
            Self::BilingualSourceFirst => "bilingual_source_first",
            Self::BilingualTranslationFirst => "bilingual_translation_first",
        }
    }
}

#[derive(Debug, Clone)]
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
