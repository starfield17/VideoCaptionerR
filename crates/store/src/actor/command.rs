//! Actor command definitions and the single-writer dispatch loop.

use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver};

use rusqlite::ErrorCode as RusqliteErrorCode;
use videocaptionerr_contracts::artifact::ArtifactMeta;
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::execution_snapshot::JobExecutionSnapshot;
use videocaptionerr_core::ports::{
    ArtifactRecoveryReport, CapabilityProbeRecord, ExpectedVersion, StageCommitRequest,
    StageCommitResult, StoredOutboxEvent,
};

use super::handle::{LeaseRequest, StoreResponse, WorkUnitRecord};
use crate::sqlite::SqliteStore;

pub(crate) fn is_constraint(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(details, _)
            if details.code == RusqliteErrorCode::ConstraintViolation
    )
}

pub(crate) fn stale_result(entity: &str, id: &str, expected: ExpectedVersion) -> VcError {
    let expected = match expected {
        ExpectedVersion::New => "new".to_owned(),
        ExpectedVersion::Exact(version) => version.to_string(),
    };
    VcError::new(
        ErrorCode::StaleResult,
        format!("stale {entity} aggregate {id}; expected version {expected}"),
    )
}

pub(super) enum StoreCommand {
    SaveJobAggregate {
        id: String,
        batch_id: Option<String>,
        status: String,
        source_path: String,
        profile_revision: String,
        execution_snapshot_id: Option<String>,
        aggregate_json: String,
        expected: ExpectedVersion,
        reply: StoreResponse<u64>,
    },
    LoadJobAggregate {
        id: String,
        reply: StoreResponse<Option<(String, u64)>>,
    },
    ListJobAggregates {
        reply: StoreResponse<Vec<(String, u64)>>,
    },
    DeleteJob {
        id: String,
        reply: StoreResponse<()>,
    },
    SaveBatchAggregate {
        id: String,
        status: String,
        asr_model: String,
        device: String,
        aggregate_json: String,
        expected: ExpectedVersion,
        reply: StoreResponse<u64>,
    },
    LoadBatchAggregate {
        id: String,
        reply: StoreResponse<Option<(String, u64)>>,
    },
    ListBatchAggregates {
        reply: StoreResponse<Vec<(String, u64)>>,
    },
    SaveWorkUnitAggregate {
        record: WorkUnitRecord,
        expected: ExpectedVersion,
        reply: StoreResponse<u64>,
    },
    LoadWorkUnitAggregate {
        id: String,
        reply: StoreResponse<Option<(String, u64)>>,
    },
    FindWorkUnitAggregate {
        job_id: String,
        stage: String,
        unit_kind: String,
        unit_index: u32,
        input_hash: String,
        reply: StoreResponse<Option<(String, u64)>>,
    },
    ListExpiredWorkUnitAggregates {
        now_rfc3339: String,
        reply: StoreResponse<Vec<(String, u64)>>,
    },
    LeaseNextReady {
        request: LeaseRequest,
        reply: StoreResponse<Option<(String, u64)>>,
    },
    RetryFailed {
        job_id: String,
        from_stage: Option<String>,
        reply: StoreResponse<u32>,
    },
    CountRetryable {
        job_id: String,
        from_stage: Option<String>,
        reply: StoreResponse<u32>,
    },
    LoadCapabilityProbe {
        provider_profile_id: String,
        model: String,
        probe_hash: String,
        reply: StoreResponse<Option<String>>,
    },
    SaveCapabilityProbe {
        record: CapabilityProbeRecord,
        reply: StoreResponse<()>,
    },
    CommitArtifact {
        meta: ArtifactMeta,
        work_unit_id: Option<String>,
        reply: StoreResponse<()>,
    },
    SaveExecutionSnapshot {
        snapshot: JobExecutionSnapshot,
        reply: StoreResponse<()>,
    },
    LoadExecutionSnapshot {
        id: String,
        reply: StoreResponse<Option<String>>,
    },
    LoadSnapshotsForBatch {
        batch_id: String,
        reply: StoreResponse<Vec<String>>,
    },
    CommitStage {
        request: Box<StageCommitRequest>,
        reply: StoreResponse<StageCommitResult>,
    },
    ListPendingOutbox {
        limit: u32,
        reply: StoreResponse<Vec<StoredOutboxEvent>>,
    },
    MarkOutboxDelivered {
        id: String,
        delivered_at: String,
        reply: StoreResponse<()>,
    },
    AppendOutbox {
        event: videocaptionerr_core::ports::OutboxEvent,
        reply: StoreResponse<()>,
    },
    RecoverArtifacts {
        roots: Vec<PathBuf>,
        reply: StoreResponse<ArtifactRecoveryReport>,
    },
}

pub(crate) fn store_actor(
    db_path: PathBuf,
    receiver: Receiver<StoreCommand>,
    ready: mpsc::SyncSender<VcResult<()>>,
) {
    let mut store = match SqliteStore::open(&db_path) {
        Ok(store) => {
            let _ = ready.send(Ok(()));
            store
        }
        Err(error) => {
            let _ = ready.send(Err(error));
            return;
        }
    };

    while let Ok(command) = receiver.recv() {
        command.execute(&mut store);
    }
}

impl StoreCommand {
    fn execute(self, store: &mut SqliteStore) {
        match self {
            Self::SaveJobAggregate {
                id,
                batch_id,
                status,
                source_path,
                profile_revision,
                execution_snapshot_id,
                aggregate_json,
                expected,
                reply,
            } => {
                let result = store.save_job_aggregate(
                    &id,
                    batch_id.as_deref(),
                    &status,
                    &source_path,
                    &profile_revision,
                    execution_snapshot_id.as_deref(),
                    &aggregate_json,
                    expected,
                );
                let _ = reply.send(result);
            }
            Self::LoadJobAggregate { id, reply } => {
                let _ = reply.send(store.load_job_aggregate(&id));
            }
            Self::ListJobAggregates { reply } => {
                let _ = reply.send(store.list_job_aggregates());
            }
            Self::DeleteJob { id, reply } => {
                let _ = reply.send(store.delete_job_record(&id));
            }
            Self::SaveBatchAggregate {
                id,
                status,
                asr_model,
                device,
                aggregate_json,
                expected,
                reply,
            } => {
                let result = store.save_batch_aggregate(
                    &id,
                    &status,
                    &asr_model,
                    &device,
                    &aggregate_json,
                    expected,
                );
                let _ = reply.send(result);
            }
            Self::LoadBatchAggregate { id, reply } => {
                let _ = reply.send(store.load_batch_aggregate(&id));
            }
            Self::ListBatchAggregates { reply } => {
                let _ = reply.send(store.list_batch_aggregates());
            }
            Self::SaveWorkUnitAggregate {
                record,
                expected,
                reply,
            } => {
                let result = store.save_work_unit_aggregate(&record, expected);
                let _ = reply.send(result);
            }
            Self::LoadWorkUnitAggregate { id, reply } => {
                let _ = reply.send(store.load_work_unit_aggregate(&id));
            }
            Self::FindWorkUnitAggregate {
                job_id,
                stage,
                unit_kind,
                unit_index,
                input_hash,
                reply,
            } => {
                let _ = reply.send(store.find_work_unit_aggregate(
                    &job_id,
                    &stage,
                    &unit_kind,
                    unit_index,
                    &input_hash,
                ));
            }
            Self::ListExpiredWorkUnitAggregates { now_rfc3339, reply } => {
                let _ = reply.send(store.list_expired_work_unit_aggregates(&now_rfc3339));
            }
            Self::LeaseNextReady { request, reply } => {
                let result = store.lease_next_ready(&request);
                let _ = reply.send(result);
            }
            Self::RetryFailed {
                job_id,
                from_stage,
                reply,
            } => {
                let _ = reply.send(store.retry_failed(&job_id, from_stage.as_deref()));
            }
            Self::CountRetryable {
                job_id,
                from_stage,
                reply,
            } => {
                let _ = reply.send(store.count_retryable(&job_id, from_stage.as_deref()));
            }
            Self::LoadCapabilityProbe {
                provider_profile_id,
                model,
                probe_hash,
                reply,
            } => {
                let _ = reply.send(store.load_capability_probe(
                    &provider_profile_id,
                    &model,
                    &probe_hash,
                ));
            }
            Self::SaveCapabilityProbe { record, reply } => {
                let _ = reply.send(store.save_capability_probe(&record));
            }
            Self::CommitArtifact {
                meta,
                work_unit_id,
                reply,
            } => {
                let _ = reply.send(store.commit_artifact_and_unit(&meta, work_unit_id.as_deref()));
            }
            Self::SaveExecutionSnapshot { snapshot, reply } => {
                let _ = reply.send(store.save_execution_snapshot(&snapshot));
            }
            Self::LoadExecutionSnapshot { id, reply } => {
                let _ = reply.send(store.load_execution_snapshot(&id));
            }
            Self::LoadSnapshotsForBatch { batch_id, reply } => {
                let _ = reply.send(store.load_snapshots_for_batch(&batch_id));
            }
            Self::CommitStage { request, reply } => {
                let _ = reply.send(store.commit_stage(*request));
            }
            Self::ListPendingOutbox { limit, reply } => {
                let _ = reply.send(store.list_pending_outbox(limit));
            }
            Self::MarkOutboxDelivered {
                id,
                delivered_at,
                reply,
            } => {
                let _ = reply.send(store.mark_outbox_delivered(&id, &delivered_at));
            }
            Self::AppendOutbox { event, reply } => {
                let _ = reply.send(store.append_outbox(&event));
            }
            Self::RecoverArtifacts { roots, reply } => {
                let _ = reply.send(store.recover_artifacts(&roots));
            }
        }
    }
}
