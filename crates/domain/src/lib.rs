//! Pure domain model for VideoCaptionerR.
//!
//! This crate deliberately has no dependency on another VideoCaptionerR
//! crate, runtime, filesystem, process, database, or network implementation.

pub const SCHEMA_VERSION: u32 = 1;

pub mod error;
pub mod identity;
pub mod subtitle;
pub mod workflow;

pub use error::{DomainError, DomainResult};
pub use identity::{BatchId, JobId, RequestId, SessionId, UlidStr, WorkUnitId};
pub use subtitle::{
    join_word_texts, join_words, rule_split, Cue, CueFlags, CueTextUpdate, EngineFingerprint,
    FieldOrigin, LlmResultBinding, LlmTextField, RangeUsize, RuleSplitConfig, TimelineSource,
    Transcript, Word, PROB_UNAVAILABLE,
};
pub use workflow::{
    is_deterministic_work_unit_error, is_oom_error, ArtifactRef, Batch, BatchExecutionProfile,
    BatchStatus, DomainEvent, Job, JobStatus, JobTerminalStatus, StageKind, StageState,
    StageStatus, WorkUnit, WorkUnitStatus, WORK_UNIT_DEFAULT_AUTO_RETRIES,
    WORK_UNIT_OOM_STRATEGY_RETRIES,
};
