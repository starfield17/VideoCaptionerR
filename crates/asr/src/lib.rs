//! ASR engine traits and shared types.
//!
//! This crate MUST NOT own Job scheduling or persistence policy.

pub mod descriptor;
pub mod engine;
pub mod options;

pub use descriptor::{ConfidenceKind, DeviceDescriptor, EngineDescriptor, TimestampGranularity};
pub use engine::{AsrEngine, AsrEvent, AsrRawResult};
pub use options::AsrOptions;
