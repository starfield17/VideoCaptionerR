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
pub mod repositories;
pub mod subtitle;
pub mod system;

pub use artifact::{
    ArtifactCommit, ArtifactInput, ArtifactStore, ChunkPlanCommit, ChunkPlanStore, TranscriptCommit,
};
pub use asr::{AsrDescriptor, AsrRuntime, AsrSession, AsrTranscribeRequest, NormalizedAsrResult};
pub use cache::{CacheGcResult, CacheRepository};
pub use events::EventPublisher;
pub use llm::{
    CapabilityProbeRecord, CapabilityProbeStore, LlmCapabilities, LlmGateway, LlmMessage,
    LlmRequest, LlmRequestMetadata, LlmRequestRecorder, LlmResponse, LlmRole, LlmStage,
    PromptSnapshot, StructuredOutput,
};
pub use media::{
    AudioAnalysis, AudioAnalysisRequest, AudioExtraction, AudioRangeExtraction,
    ExtractAudioRangeRequest, ExtractAudioRequest, MediaGateway, ProbeMediaRequest, ProbedMedia,
};
pub use repositories::{BatchRepository, JobRepository, WorkUnitRepository};
pub use subtitle::{
    ExportedSubtitle, SubtitleExportRequest, SubtitleFormat, SubtitleGateway, SubtitleLayout,
};
pub use system::{Clock, IdGenerator};
