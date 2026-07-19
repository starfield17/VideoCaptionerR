use std::path::{Path, PathBuf};

use async_trait::async_trait;
use videocaptionerr_contracts::media::MediaProbe;

use crate::application_error::AppResult;
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

#[async_trait]
pub trait MediaGateway: Send + Sync {
    async fn probe(&self, request: ProbeMediaRequest) -> AppResult<ProbedMedia>;
    async fn media_hash(&self, input: &Path) -> AppResult<String>;
    async fn extract_audio(&self, request: ExtractAudioRequest) -> AppResult<AudioExtraction>;
}
