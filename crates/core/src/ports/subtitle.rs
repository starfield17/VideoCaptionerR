use std::path::{Path, PathBuf};

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

/// How a subtitle importer maps cue lines into source and translation fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SubtitleImportLayout {
    #[default]
    Mono,
    SourceAboveTranslation,
    TranslationAboveSource,
}

impl SubtitleImportLayout {
    pub fn parse(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("mono").to_ascii_lowercase().as_str() {
            "mono" | "source" => Some(Self::Mono),
            "source-above" | "source_above" | "bilingual" => Some(Self::SourceAboveTranslation),
            "translation-above" | "translation_above" => Some(Self::TranslationAboveSource),
            _ => None,
        }
    }
}

/// Parsed subtitle input returned by the host adapter to the import use case.
pub struct ImportedSubtitle {
    pub source_path: PathBuf,
    pub transcript: Transcript,
    pub warnings: Vec<String>,
}

/// Host-specific subtitle parsing and file access required by `ImportSubtitle`.
pub trait SubtitleImporter: Send + Sync {
    fn import(&self, path: &Path, layout: SubtitleImportLayout) -> AppResult<ImportedSubtitle>;
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
