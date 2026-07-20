//! Application-owned ports.
//!
//! Concrete adapters implement these interfaces from outside the application
//! crate. The ports describe application needs, not technology APIs.

pub mod artifact;
pub mod asr;
pub mod cache;
pub mod events;
pub mod llm;
pub mod media;
pub mod model;
pub mod repositories;
pub mod subtitle;
pub mod system;

pub use artifact::{
    ArtifactCommit, ArtifactInput, ArtifactRecoveryReport, ArtifactRecoveryStore, ArtifactSource,
    ArtifactStore, ChunkPlanCommit, ChunkPlanStore, PreparedArtifact, TranscriptCommit,
};
pub use asr::{
    cancel_grace, AsrCancelToken, AsrDescriptor, AsrRuntime, AsrSession, AsrSessionControl,
    AsrTranscribeRequest, NormalizedAsrResult, ASR_CANCEL_GRACE_MS,
};
pub use cache::{CacheGcResult, CacheRepository};
pub use events::{
    ApplicationEvent, EventPublisher, LiveEventSink, OutboxEvent, OutboxRepository, StoredOutboxEvent,
};
pub use llm::{
    CapabilityProbeRecord, CapabilityProbeStore, LlmCapabilities, LlmGateway, LlmMessage,
    LlmRequest, LlmRequestMetadata, LlmRequestRecorder, LlmResponse, LlmRole, LlmStage,
    PromptSnapshot, StructuredOutput,
};
pub use media::{
    AudioAnalysis, AudioAnalysisRequest, AudioExtraction, AudioRangeExtraction,
    ExtractAudioRangeRequest, ExtractAudioRequest, MediaGateway, ProbeMediaRequest, ProbedMedia,
};
pub use model::{
    asr_fingerprint, validate_spec, AsrRuntimeResolver, AsrRuntimeSpec, ModelLocator,
};
pub use repositories::{
    BatchRepository, ExpectedVersion, JobRepository, RetryTransactionRepository,
    RetryTransactionRequest, RetryTransactionResult, SnapshotRepository, StageCommitRepository,
    StageCommitRequest, StageCommitResult, Versioned, WorkUnitRepository,
};
pub use subtitle::{
    ExportedSubtitle, SubtitleExportRequest, SubtitleFormat, SubtitleGateway, SubtitleLayout,
};
pub use system::{Clock, IdGenerator};
