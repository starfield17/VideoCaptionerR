use std::path::{Path, PathBuf};

use async_trait::async_trait;
use videocaptionerr_contracts::media::MediaProbe;

use crate::application_error::AppResult;
use crate::chunking::{EnergySample, SilenceRegion};
use crate::ports::artifact::ArtifactInput;

pub struct ProbedMedia {
    pub probe: MediaProbe,
    pub artifact: ArtifactInput,
}

pub struct ProbeMediaRequest {
    pub input: PathBuf,
    pub job_dir: PathBuf,
}

pub struct ExtractAudioRequest {
    pub input: PathBuf,
    pub stream_index: u32,
    pub expected_duration_ms: Option<u64>,
    pub job_dir: PathBuf,
}

pub struct AudioExtraction {
    pub wav_path: PathBuf,
    pub pcm_hash: String,
    pub artifact: ArtifactInput,
}

#[derive(Debug, Clone)]
pub struct AudioAnalysisRequest {
    pub audio_path: PathBuf,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub struct AudioAnalysis {
    pub silences: Vec<SilenceRegion>,
    pub energy: Vec<EnergySample>,
}

#[derive(Debug, Clone)]
pub struct ExtractAudioRangeRequest {
    pub input_wav: PathBuf,
    pub read_start_ms: u64,
    pub read_end_ms: u64,
    pub output_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct AudioRangeExtraction {
    pub wav_path: PathBuf,
    pub pcm_hash: String,
}

#[async_trait]
pub trait MediaGateway: Send + Sync {
    async fn probe(&self, request: ProbeMediaRequest) -> AppResult<ProbedMedia>;
    async fn media_hash(&self, input: &Path) -> AppResult<String>;
    async fn extract_audio(&self, request: ExtractAudioRequest) -> AppResult<AudioExtraction>;

    /// Analyze only when long-audio chunking is activated. The default keeps
    /// adapters that do not expose VAD compatible with short-audio jobs.
    async fn analyze_audio(&self, _request: AudioAnalysisRequest) -> AppResult<AudioAnalysis> {
        Ok(AudioAnalysis::default())
    }

    /// Extract a context-padded range from canonical PCM. Adapters that do
    /// not support long-audio work units fail explicitly at the application
    /// boundary instead of returning a duplicated full-file transcription.
    async fn extract_audio_range(
        &self,
        _request: ExtractAudioRangeRequest,
    ) -> AppResult<AudioRangeExtraction> {
        Err(crate::application_error::ApplicationError::Adapter(
            videocaptionerr_contracts::error::VcError::new(
                videocaptionerr_contracts::error::ErrorCode::OptionUnsupported,
                "media adapter does not support audio range extraction",
            ),
        ))
    }
}
