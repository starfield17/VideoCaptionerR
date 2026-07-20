//! ASR engine traits and shared types.
//!
//! This crate MUST NOT own Job scheduling or persistence policy.

pub mod application;
pub mod descriptor;
pub mod engine;
pub mod model;
pub mod normalize;
pub mod options;
pub mod worker;

pub use application::WorkerAsrRuntime;
pub use descriptor::{
    CapabilityLevel, ConfidenceKind, DeviceDescriptor, EngineDescriptor, TimestampGranularity,
};
pub use engine::{AsrEngine, AsrEvent, AsrRawResult};
pub use model::{download_model, verify_model_file, ModelEntry, ModelManifest};
pub use normalize::{normalize_asr, NormalizeOptions};
pub use options::AsrOptions;
pub use worker::{
    kill_process_tree, resolve_helper_binary, WorkerClient, WorkerControl, WorkerProtocolSession,
    CANCEL_GRACE,
};
