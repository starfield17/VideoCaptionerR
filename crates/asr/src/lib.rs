//! ASR engine traits and shared types.
//!
//! This crate MUST NOT own Job scheduling or persistence policy.

pub mod application;
pub mod descriptor;
pub mod engine;
pub mod model;
pub mod normalize;
pub mod options;
pub mod python_env;
pub mod resolver;
pub mod worker;

pub use application::WorkerAsrRuntime;
pub use descriptor::{
    CapabilityLevel, ConfidenceKind, DeviceDescriptor, EngineDescriptor, TimestampGranularity,
};
pub use engine::{AsrEngine, AsrEvent, AsrRawResult};
pub use model::{
    blake3_file, download_model, sha256_file, verify_model_file, ModelEntry, ModelLocatorKind,
    ModelManifest,
};
pub use normalize::{normalize_asr, NormalizeOptions};
pub use options::AsrOptions;
pub use python_env::{
    ensure_managed_env, lock_hash_for_family, EngineFamily, ManagedEnvConfig, ManagedPythonEnv,
};
pub use resolver::{FamilyAsrRuntimeResolver, FixedAsrRuntimeResolver};
pub use worker::{
    kill_process_tree, resolve_helper_binary, WorkerClient, WorkerControl, WorkerProtocolSession,
    CANCEL_GRACE,
};

#[cfg(test)]
mod tests_phase5;
