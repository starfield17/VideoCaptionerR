//! Application-owned ports.
//!
//! Concrete adapters implement these interfaces from outside the application
//! crate. The ports describe application needs, not technology APIs.

pub mod artifact;
pub mod asr;
pub mod events;
pub mod llm;
pub mod media;
pub mod repositories;
pub mod subtitle;
pub mod system;

pub use artifact::{ArtifactCommit, ArtifactInput, ArtifactStore, TranscriptCommit};
pub use asr::{AsrDescriptor, AsrRuntime, AsrSession, AsrTranscribeRequest, NormalizedAsrResult};
pub use events::EventPublisher;
pub use llm::{
    LlmCapabilities, LlmGateway, LlmMessage, LlmRequest, LlmResponse, LlmRole, StructuredOutput,
};
pub use media::{
    AudioExtraction, ExtractAudioRequest, MediaGateway, ProbeMediaRequest, ProbedMedia,
};
pub use repositories::{BatchRepository, JobRepository, WorkUnitRepository};
pub use subtitle::{
    ExportedSubtitle, SubtitleExportRequest, SubtitleFormat, SubtitleGateway, SubtitleLayout,
};
pub use system::{Clock, IdGenerator};
