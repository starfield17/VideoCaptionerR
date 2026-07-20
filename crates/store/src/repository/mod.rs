//! Application-port adapters backed by the SQLite actor.

mod artifact;
mod batch;
mod capability_probe;
mod job;
mod outbox;
mod recovery;
mod snapshot;
mod stage_commit;
mod status;
mod work_unit;

#[cfg(test)]
mod tests;

pub use artifact::SqliteArtifactStore;

pub(crate) use std::fs;
pub(crate) use std::path::Path;

pub(crate) use async_trait::async_trait;
pub(crate) use chrono::{DateTime, Utc};
pub(crate) use serde_json;
pub(crate) use videocaptionerr_contracts::artifact::ArtifactKind;
pub(crate) use videocaptionerr_contracts::error::{ErrorCode, VcError};
pub(crate) use videocaptionerr_core::application_error::{AppResult, ApplicationError};
pub(crate) use videocaptionerr_core::execution_snapshot::JobExecutionSnapshot;
pub(crate) use videocaptionerr_core::ports::{
    ArtifactCommit, ArtifactRecoveryStore, ArtifactStore, BatchRepository, CapabilityProbeRecord,
    CapabilityProbeStore, ChunkPlanCommit, ChunkPlanStore, EventPublisher, ExpectedVersion,
    JobRepository, OutboxEvent, OutboxRepository, SnapshotRepository, StageCommitRepository,
    StageCommitRequest, StageCommitResult, TranscriptCommit, Versioned, WorkUnitRepository,
};
pub(crate) use videocaptionerr_domain::{
    ArtifactRef, Batch, BatchId, DomainEvent, Job, JobId, StageKind, WorkUnit,
};

pub(crate) use crate::actor::{LeaseRequest, StoreHandle, WorkUnitRecord};
pub(crate) use crate::artifact::{atomic_write_json, blake3_file};

pub(crate) use artifact::stage_name;
pub(crate) use status::StatusString;
