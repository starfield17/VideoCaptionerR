use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use videocaptionerr_domain::{BatchExecutionProfile, Transcript};

use crate::application_error::AppResult;
use crate::ports::events::EventPublisher;

/// Default cooperative-cancel grace before hard kill (ms).
pub const ASR_CANCEL_GRACE_MS: u64 = 3000;

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

/// Application-owned cancellation token. Core never sees a WorkerClient.
#[derive(Debug, Clone, Default)]
pub struct AsrCancelToken {
    requested: Arc<AtomicBool>,
}

impl AsrCancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn request(&self) {
        self.requested.store(true, Ordering::SeqCst);
    }

    pub fn is_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }
}

/// Application-facing control surface for cooperative cancel / heartbeat.
/// Implementations may wrap a worker control path without exposing it.
#[async_trait]
pub trait AsrSessionControl: Send + Sync {
    async fn request_cancel(&self) -> AppResult<()>;
    async fn ping(&self) -> AppResult<()>;
}

#[async_trait]
pub trait AsrRuntime: Send + Sync {
    async fn open_session(&self, profile: &BatchExecutionProfile)
        -> AppResult<Box<dyn AsrSession>>;
}

#[async_trait]
pub trait AsrSession: Send {
    fn descriptor(&self) -> &AsrDescriptor;

    /// Optional control handle for cooperative cancel during transcription.
    fn control(&self) -> Option<Arc<dyn AsrSessionControl>> {
        None
    }

    async fn transcribe(
        &mut self,
        request: AsrTranscribeRequest,
        events: &dyn EventPublisher,
        cancel: Option<AsrCancelToken>,
    ) -> AppResult<NormalizedAsrResult>;

    async fn close(self: Box<Self>) -> AppResult<()>;
}

pub fn cancel_grace() -> Duration {
    Duration::from_millis(ASR_CANCEL_GRACE_MS)
}
