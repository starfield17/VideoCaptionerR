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
            let options = crate::subtitle_io::ExportOptions {
                format: match request.format {
                    SubtitleFormat::Srt => crate::subtitle_io::ExportFormat::Srt,
                    SubtitleFormat::Vtt => crate::subtitle_io::ExportFormat::Vtt,
                    SubtitleFormat::Ass => crate::subtitle_io::ExportFormat::Ass,
                },
                layout: match request.layout {
                    SubtitleLayout::SourceOnly => crate::subtitle_io::ExportLayout::SourceOnly,
                    SubtitleLayout::TranslationOnly => {
                        crate::subtitle_io::ExportLayout::TranslationOnly
                    }
                    SubtitleLayout::BilingualSourceFirst => {
                        crate::subtitle_io::ExportLayout::BilingualSourceFirst
                    }
                    SubtitleLayout::BilingualTranslationFirst => {
                        crate::subtitle_io::ExportLayout::BilingualTranslationFirst
                    }
                },
                missing_translation: if request.fallback_to_source {
                    crate::subtitle_io::export::MissingTranslationPolicy::FallbackToSource
                } else {
                    crate::subtitle_io::export::MissingTranslationPolicy::Fail
                },
            };
            let path = request.output_path;
            let content_hash =
                crate::subtitle_io::export::write_export(&path, &transcript, &options)
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
