//! Deterministic subtitle-file writer adapter.

use async_trait::async_trait;
use videocaptionerr_contracts::error::{ErrorCode, VcError};
use videocaptionerr_core::ports::{
    ExportedSubtitle, SubtitleExportRequest, SubtitleFormat, SubtitleGateway, SubtitleLayout,
};
use videocaptionerr_domain::Transcript;

#[derive(Debug, Clone, Default)]
pub struct FileSubtitleGateway;

#[async_trait]
impl SubtitleGateway for FileSubtitleGateway {
    async fn export(
        &self,
        transcript: &Transcript,
        request: SubtitleExportRequest,
    ) -> videocaptionerr_core::AppResult<ExportedSubtitle> {
        let transcript = transcript.clone();
        tokio::task::spawn_blocking(move || {
            let options = videocaptionerr_core::subtitle::ExportOptions {
                format: match request.format {
                    SubtitleFormat::Srt => videocaptionerr_core::subtitle::ExportFormat::Srt,
                    SubtitleFormat::Vtt => videocaptionerr_core::subtitle::ExportFormat::Vtt,
                    SubtitleFormat::Ass => videocaptionerr_core::subtitle::ExportFormat::Ass,
                },
                layout: match request.layout {
                    SubtitleLayout::SourceOnly => videocaptionerr_core::subtitle::ExportLayout::SourceOnly,
                    SubtitleLayout::TranslationOnly => videocaptionerr_core::subtitle::ExportLayout::TranslationOnly,
                    SubtitleLayout::BilingualSourceFirst => videocaptionerr_core::subtitle::ExportLayout::BilingualSourceFirst,
                    SubtitleLayout::BilingualTranslationFirst => videocaptionerr_core::subtitle::ExportLayout::BilingualTranslationFirst,
                },
                missing_translation: if request.fallback_to_source {
                    videocaptionerr_core::subtitle::export::MissingTranslationPolicy::FallbackToSource
                } else {
                    videocaptionerr_core::subtitle::export::MissingTranslationPolicy::Fail
                },
            };
            let path = request.output_path;
            let content_hash = videocaptionerr_core::subtitle::export::write_export(
                &path,
                &transcript,
                &options,
            )
            .map_err(videocaptionerr_core::ApplicationError::Adapter)?;
            Ok(ExportedSubtitle { path, content_hash })
        })
        .await
        .map_err(|e| {
            videocaptionerr_core::ApplicationError::Adapter(VcError::new(
                ErrorCode::Internal,
                format!("subtitle writer task failed: {e}"),
            ))
        })?
    }
}
