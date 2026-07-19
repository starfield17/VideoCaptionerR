//! ffprobe, ffmpeg, and streaming hash adapter.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use videocaptionerr_contracts::error::{ErrorCode, VcError};
use videocaptionerr_core::ports::{
    ArtifactInput, AudioExtraction, ExtractAudioRequest, MediaGateway, ProbeMediaRequest,
    ProbedMedia,
};
use videocaptionerr_domain::StageKind;

#[derive(Debug, Clone, Default)]
pub struct FfmpegMediaGateway {
    pub ffmpeg_path: Option<PathBuf>,
    pub ffprobe_path: Option<PathBuf>,
}

#[async_trait]
impl MediaGateway for FfmpegMediaGateway {
    async fn probe(
        &self,
        request: ProbeMediaRequest,
    ) -> videocaptionerr_core::AppResult<ProbedMedia> {
        let ffprobe_path = self.ffprobe_path.clone();
        blocking(move || {
            let probe = crate::media::probe_media(&request.input, ffprobe_path.as_deref())?;
            std::fs::create_dir_all(&request.job_dir).map_err(|e| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("create probe artifact directory: {e}"),
                )
            })?;
            let artifact_path = request.job_dir.join("00_probe.json");
            let content_hash = videocaptionerr_store::atomic_write_json(&artifact_path, &probe)?;
            Ok(ProbedMedia {
                probe,
                artifact: ArtifactInput {
                    stage: StageKind::Probe,
                    path: artifact_path,
                    content_hash,
                    schema_version: videocaptionerr_domain::SCHEMA_VERSION,
                    producer_fingerprint: "ffprobe".into(),
                },
            })
        })
        .await
    }

    async fn media_hash(&self, input: &Path) -> videocaptionerr_core::AppResult<String> {
        let input = input.to_path_buf();
        blocking(move || crate::media::media_hash_file(&input)).await
    }

    async fn extract_audio(
        &self,
        request: ExtractAudioRequest,
    ) -> videocaptionerr_core::AppResult<AudioExtraction> {
        let ffmpeg_path = self.ffmpeg_path.clone();
        blocking(move || {
            let extracted = crate::media::extract_audio_wav(
                &request.input,
                &request.job_dir,
                &crate::media::ExtractOptions {
                    stream_index: request.stream_index,
                    ffmpeg_path,
                    expected_duration_ms: request.expected_duration_ms,
                    ..Default::default()
                },
            )?;
            let content_hash = videocaptionerr_store::blake3_file(&extracted.wav_path)?;
            Ok(AudioExtraction {
                wav_path: extracted.wav_path.clone(),
                pcm_hash: extracted.pcm_hash,
                artifact: ArtifactInput {
                    stage: StageKind::ExtractAudio,
                    path: extracted.wav_path,
                    content_hash,
                    schema_version: videocaptionerr_domain::SCHEMA_VERSION,
                    producer_fingerprint: "ffmpeg-pcm-s16le".into(),
                },
            })
        })
        .await
    }
}

async fn blocking<T, F>(operation: F) -> videocaptionerr_core::AppResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, VcError> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|e| {
            videocaptionerr_core::ApplicationError::Adapter(VcError::new(
                ErrorCode::Internal,
                format!("platform blocking task failed: {e}"),
            ))
        })?
        .map_err(videocaptionerr_core::ApplicationError::Adapter)
}
