//! SQLite-backed control-plane persistence.

mod artifacts;
mod batch_creation;
mod batches;
mod capability_probes;
mod jobs;
mod mapping;
mod outbox;
mod recovery;
mod snapshots;
mod stage_commit;
mod work_units;

#[cfg(test)]
mod tests;

pub(super) use std::path::{Path, PathBuf};
pub(super) use std::{collections::HashSet, fs};

pub(super) use rusqlite::{params, Connection, OptionalExtension};
pub(super) use ulid::Ulid;
pub(super) use videocaptionerr_contracts::artifact::{ArtifactKind, ArtifactMeta};
pub(super) use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
pub(super) use videocaptionerr_contracts::ids::UlidStr;
pub(super) use videocaptionerr_core::execution_snapshot::JobExecutionSnapshot;
pub(super) use videocaptionerr_core::ports::{
    ArtifactRecoveryReport, CapabilityProbeRecord, ExpectedVersion,
};
pub(super) use videocaptionerr_domain::WorkUnitStatus;

pub(super) use crate::actor::{is_constraint, stale_result, LeaseRequest, WorkUnitRecord};
pub(super) use crate::artifact::{
    blake3_file, publish_prepared_artifact_with_fault, sync_parent, StageCommitFaultPoint,
};
pub(super) use crate::migrate::migrate;
#[cfg(test)]
pub(crate) use mapping::parse_work_unit_status;
pub(crate) use mapping::{
    artifact_meta_for, job_status_name, next_version, snapshot_projection, stage_fault_at,
    stage_rank, stage_status_name, work_unit_status_name,
};
pub(crate) use stage_commit::{insert_outbox_tx, sync_stage_projection};

/// SQLite store owned by the dedicated actor thread.
pub(crate) struct SqliteStore {
    pub(super) conn: Connection,
    pub(super) fault: Option<StageCommitFaultPoint>,
    pub(super) batch_creation_fault: Option<batch_creation::BatchCreationFaultPoint>,
}

impl SqliteStore {
    pub(crate) fn open(db_path: &Path) -> VcResult<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                VcError::new(ErrorCode::InvalidConfig, format!("create db parent: {e}"))
            })?;
        }
        let conn = Connection::open(db_path).map_err(|e| {
            VcError::new(
                ErrorCode::InvalidConfig,
                format!("open db {}: {e}", db_path.display()),
            )
        })?;
        migrate(&conn)?;
        Ok(Self {
            conn,
            fault: None,
            batch_creation_fault: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn inject_stage_commit_fault(&mut self, point: StageCommitFaultPoint) {
        self.fault = Some(point);
    }

    #[cfg(test)]
    pub(crate) fn inject_batch_creation_fault(
        &mut self,
        point: batch_creation::BatchCreationFaultPoint,
    ) {
        self.batch_creation_fault = Some(point);
    }
}
