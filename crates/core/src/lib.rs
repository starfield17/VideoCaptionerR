//! Application and business services shared by CLI and GUI.
//!
//! This crate MUST NOT import Tauri, React, or terminal-rendering concerns.

pub mod application_error;
pub mod artifacts;
pub mod chunking;
pub mod constants;
pub mod execution_snapshot;
pub mod ports;
pub mod split;
pub mod text_joiner;
pub mod use_cases;
pub mod vad;

pub use application_error::{AppResult, ApplicationError};
pub use artifacts::{
    ExtractManifest, ProbeManifest, EXTRACT_MANIFEST_SCHEMA_VERSION, PROBE_MANIFEST_SCHEMA_VERSION,
};
pub use chunking::{
    apply_chunk_offset, chunk_cache_key, retain_core_words, AudioChunk, ChunkPlan,
    ChunkPlanOptions, CutReason, EnergySample, SilenceRegion,
};
pub use constants::*;
pub use execution_snapshot::{
    AsrExecutionSnapshot, AudioStreamSelection, CacheExecutionSnapshot, JobExecutionSnapshot,
    JobExecutionSnapshot as ExecutionSnapshot, LlmExecutionSnapshot, OutputPlanSnapshot,
    SourceStatSnapshot, JOB_EXECUTION_SNAPSHOT_SCHEMA_V1, JOB_EXECUTION_SNAPSHOT_SCHEMA_VERSION,
};
pub use ports::CacheGcResult;
pub use ports::{asr_fingerprint, AsrRuntimeResolver, AsrRuntimeSpec, ModelLocator};
pub use split::{rule_split, RuleSplitConfig};
pub use text_joiner::{join_word_texts, join_words};
