use std::path::PathBuf;

use async_trait::async_trait;
use videocaptionerr_domain::{BatchExecutionProfile, Transcript};

use crate::application_error::AppResult;
use crate::ports::events::EventPublisher;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsrDescriptor {
    pub engine_id: String,
    pub adapter_version: String,
    pub runtime_version: String,
    /// Complete cache-safe identity for this loaded model/runtime session.
    pub fingerprint: String,
    pub supports_word_timestamps: bool,
    pub supports_confidence: bool,
    pub cooperative_cancel: bool,
    pub max_audio_secs: Option<u32>,
}

pub struct AsrTranscribeRequest {
    pub audio_path: PathBuf,
    pub language: Option<String>,
    pub source_hash: String,
    pub duration_ms: Option<u64>,
}

pub struct NormalizedAsrResult {
    pub transcript: Transcript,
}

#[async_trait]
pub trait AsrRuntime: Send + Sync {
    async fn open_session(&self, profile: &BatchExecutionProfile)
        -> AppResult<Box<dyn AsrSession>>;
}

#[async_trait]
pub trait AsrSession: Send {
    fn descriptor(&self) -> &AsrDescriptor;

    async fn transcribe(
        &mut self,
        request: AsrTranscribeRequest,
        events: &dyn EventPublisher,
    ) -> AppResult<NormalizedAsrResult>;

    async fn close(self: Box<Self>) -> AppResult<()>;
}
