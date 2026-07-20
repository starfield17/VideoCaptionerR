//! Processing Workflow bounded context.
//!
//! These aggregates express lifecycle rules only. They do not start
//! processes, write files, send HTTP requests, or execute SQL.

mod batch;
mod event;
mod job;
mod stage;
mod work_unit;

#[cfg(test)]
mod tests;

pub use batch::{Batch, BatchExecutionProfile, BatchStatus};
pub use event::{DomainEvent, JobTerminalStatus};
pub use job::Job;
pub use stage::{ArtifactRef, JobStatus, StageKind, StageState, StageStatus};
pub use work_unit::{
    is_deterministic_work_unit_error, is_oom_error, Lease, WorkUnit, WorkUnitStatus,
    WORK_UNIT_DEFAULT_AUTO_RETRIES, WORK_UNIT_OOM_STRATEGY_RETRIES,
};
