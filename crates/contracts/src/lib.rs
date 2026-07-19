//! Shared contracts for VideoCaptionerR.
//!
//! This crate is the single source of truth for Transcript IR, worker protocols,
//! CLI/GUI event envelopes, error codes, and artifact metadata.
//! It MUST NOT depend on UI, database, scheduling, or runtime implementations.

pub mod artifact;
pub mod error;
pub mod event;
pub mod ids;
pub mod media;
pub mod protocol;
pub mod transcript;
pub mod version;

pub use artifact::{ArtifactKind, ArtifactMeta};
pub use error::{ErrorCategory, ErrorCode, VcError};
pub use event::{CliEvent, EventEnvelope};
pub use ids::{BatchId, JobId, RequestId, SessionId, UlidStr, WorkUnitId};
pub use media::{AudioStream, MediaProbe};
pub use protocol::{
    HelloData, ProtocolEnvelope, ProtocolMessageType, PROTOCOL_VERSION, WORKER_MAX_LINE_BYTES,
};
pub use transcript::{
    Cue, CueFlags, EngineFingerprint, FieldOrigin, RangeUsize, TimelineSource, Transcript, Word,
};
pub use version::{CONTRACTS_VERSION, SCHEMA_VERSION};
