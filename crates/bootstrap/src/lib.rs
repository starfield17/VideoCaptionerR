//! Composition root shared by the CLI and the future desktop shell.
//!
//! This crate is the only place that assembles concrete infrastructure
//! adapters. It exposes application-shaped operations to inbound adapters.

mod batch_control;
mod capability;
mod config;
mod doctor;
mod dto;
mod import_subtitle;
mod jobs;
mod models;
mod processing;
mod recovery;
mod run_control;
mod runtime;
mod transcript;
mod wiring;

#[cfg(test)]
mod tests;

pub use config::RuntimeConfig;
pub use doctor::DoctorReport;
pub use dto::{
    CapabilityProbeView, CapabilityView, DoctorView, FailureView, JobSummary, ProcessOptions,
    ProcessView, StageSummary, TranscribeOptions, TranscriptEditView,
};
pub use jobs::RetryJobOutcome;
pub use runtime::{ApplicationRuntime, ProcessingLease};
