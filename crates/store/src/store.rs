//! Single-writer store actor over SQLite.

use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::{collections::HashSet, fs};

use rusqlite::{params, Connection, ErrorCode as RusqliteErrorCode, OptionalExtension};
use tokio::sync::oneshot;
use ulid::Ulid;
use videocaptionerr_contracts::artifact::{ArtifactKind, ArtifactMeta};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_contracts::ids::UlidStr;
use videocaptionerr_core::execution_snapshot::JobExecutionSnapshot;
use videocaptionerr_core::ports::{
    ArtifactRecoveryReport, CapabilityProbeRecord, ExpectedVersion, StageCommitRequest,
    StageCommitResult, StoredOutboxEvent,
};

use crate::artifact::{
    blake3_file, publish_prepared_artifact_with_fault, sync_parent, StageCommitFaultPoint,
};
use crate::migrate::migrate;

/// Work unit lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkUnitStatus {
    Pending,
    Running,
    Done,
    Failed,
    Cancelled,
}

impl WorkUnitStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "pending" => Self::Pending,
            "running" => Self::Running,
            "done" => Self::Done,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => return None,
        })
    }
}

/// Async handle to the dedicated SQLite store actor.
#[derive(Clone)]
pub struct StoreHandle {
    commands: Sender<StoreCommand>,
}

pub(crate) struct WorkUnitRecord {
    pub id: String,
    pub job_id: String,
    pub stage: String,
    pub unit_kind: String,
    pub unit_index: u32,
    pub input_hash: String,
    pub status: String,
    pub attempt: u32,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<String>,
    pub artifact_id: Option<String>,
    pub aggregate_json: String,
}

pub(crate) struct LeaseRequest {
    pub job_id: String,
    pub stage: String,
    pub owner: String,
    pub now_rfc3339: String,
    pub now_ms: u64,
    pub expires_rfc3339: String,
    pub expires_at_ms: u64,
}

impl StoreHandle {
    pub fn open(db_path: &Path) -> VcResult<Self> {
        let (commands, receiver) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let path = db_path.to_path_buf();
        thread::Builder::new()
            .name("videocaptionerr-store".into())
            .spawn(move || store_actor(path, receiver, ready_tx))
            .map_err(|error| {
                VcError::new(
                    ErrorCode::InvalidConfig,
                    format!("spawn store actor: {error}"),
                )
            })?;
        ready_rx.recv().map_err(|error| {
            VcError::new(
                ErrorCode::InvalidConfig,
                format!("start store actor: {error}"),
            )
        })??;
        Ok(Self { commands })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn save_job_aggregate(
        &self,
        id: &str,
        batch_id: Option<&str>,
        status: &str,
        source_path: &str,
        profile_revision: &str,
        execution_snapshot_id: Option<&str>,
        aggregate_json: &str,
        expected: ExpectedVersion,
    ) -> VcResult<u64> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::SaveJobAggregate {
            id: id.into(),
            batch_id: batch_id.map(str::to_owned),
            status: status.into(),
            source_path: source_path.into(),
            profile_revision: profile_revision.into(),
            execution_snapshot_id: execution_snapshot_id.map(str::to_owned),
            aggregate_json: aggregate_json.into(),
            expected,
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn load_job_aggregate(&self, id: &str) -> VcResult<Option<(String, u64)>> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::LoadJobAggregate {
            id: id.into(),
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn list_job_aggregates(&self) -> VcResult<Vec<(String, u64)>> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::ListJobAggregates { reply })?;
        await_response(result).await
    }

    pub(crate) async fn delete_job_record(&self, id: &str) -> VcResult<()> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::DeleteJob {
            id: id.into(),
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn save_batch_aggregate(
        &self,
        id: &str,
        status: &str,
        asr_model: &str,
        device: &str,
        aggregate_json: &str,
        expected: ExpectedVersion,
    ) -> VcResult<u64> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::SaveBatchAggregate {
            id: id.into(),
            status: status.into(),
            asr_model: asr_model.into(),
            device: device.into(),
            aggregate_json: aggregate_json.into(),
            expected,
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn load_batch_aggregate(&self, id: &str) -> VcResult<Option<(String, u64)>> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::LoadBatchAggregate {
            id: id.into(),
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn list_batch_aggregates(&self) -> VcResult<Vec<(String, u64)>> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::ListBatchAggregates { reply })?;
        await_response(result).await
    }

    pub(crate) async fn save_work_unit_aggregate(
        &self,
        record: WorkUnitRecord,
        expected: ExpectedVersion,
    ) -> VcResult<u64> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::SaveWorkUnitAggregate {
            record,
            expected,
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn load_work_unit_aggregate(
        &self,
        id: &str,
    ) -> VcResult<Option<(String, u64)>> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::LoadWorkUnitAggregate {
            id: id.into(),
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn find_work_unit_aggregate(
        &self,
        job_id: &str,
        stage: &str,
        unit_kind: &str,
        unit_index: u32,
        input_hash: &str,
    ) -> VcResult<Option<(String, u64)>> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::FindWorkUnitAggregate {
            job_id: job_id.into(),
            stage: stage.into(),
            unit_kind: unit_kind.into(),
            unit_index,
            input_hash: input_hash.into(),
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn list_expired_work_unit_aggregates(
        &self,
        now_rfc3339: &str,
    ) -> VcResult<Vec<(String, u64)>> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::ListExpiredWorkUnitAggregates {
            now_rfc3339: now_rfc3339.into(),
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn lease_next_ready_aggregate(
        &self,
        request: LeaseRequest,
    ) -> VcResult<Option<(String, u64)>> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::LeaseNextReady { request, reply })?;
        await_response(result).await
    }

    pub(crate) async fn retry_failed_aggregates(
        &self,
        job_id: &str,
        from_stage: Option<&str>,
    ) -> VcResult<u32> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::RetryFailed {
            job_id: job_id.into(),
            from_stage: from_stage.map(str::to_owned),
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn count_retryable_aggregates(
        &self,
        job_id: &str,
        from_stage: Option<&str>,
    ) -> VcResult<u32> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::CountRetryable {
            job_id: job_id.into(),
            from_stage: from_stage.map(str::to_owned),
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn load_capability_probe(
        &self,
        provider_profile_id: &str,
        model: &str,
        probe_hash: &str,
    ) -> VcResult<Option<String>> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::LoadCapabilityProbe {
            provider_profile_id: provider_profile_id.into(),
            model: model.into(),
            probe_hash: probe_hash.into(),
            reply,
        })?;
        await_response(result).await
    }

    /// Load a cached probe while opening the synchronous composition root.
    pub fn load_capability_probe_sync(
        &self,
        provider_profile_id: &str,
        model: &str,
        probe_hash: &str,
    ) -> VcResult<Option<String>> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(VcError::new(
                ErrorCode::InvalidConfig,
                "synchronous capability probe loading must run outside a Tokio runtime",
            ));
        }
        let (reply, result) = response_channel();
        self.send(StoreCommand::LoadCapabilityProbe {
            provider_profile_id: provider_profile_id.into(),
            model: model.into(),
            probe_hash: probe_hash.into(),
            reply,
        })?;
        result.blocking_recv().map_err(|_| {
            VcError::new(
                ErrorCode::Internal,
                "store actor stopped before returning a capability probe",
            )
        })?
    }

    pub(crate) async fn save_capability_probe(
        &self,
        record: CapabilityProbeRecord,
    ) -> VcResult<()> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::SaveCapabilityProbe { record, reply })?;
        await_response(result).await
    }

    pub(crate) async fn commit_artifact_and_unit(
        &self,
        meta: ArtifactMeta,
        work_unit_id: Option<String>,
    ) -> VcResult<()> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::CommitArtifact {
            meta,
            work_unit_id,
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn save_execution_snapshot(
        &self,
        snapshot: JobExecutionSnapshot,
    ) -> VcResult<()> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::SaveExecutionSnapshot { snapshot, reply })?;
        await_response(result).await
    }

    pub(crate) async fn load_execution_snapshot(&self, id: &str) -> VcResult<Option<String>> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::LoadExecutionSnapshot {
            id: id.into(),
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn load_snapshots_for_batch(&self, id: &str) -> VcResult<Vec<String>> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::LoadSnapshotsForBatch {
            batch_id: id.into(),
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn commit_stage(
        &self,
        request: StageCommitRequest,
    ) -> VcResult<StageCommitResult> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::CommitStage {
            request: Box::new(request),
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn list_pending_outbox(&self, limit: u32) -> VcResult<Vec<StoredOutboxEvent>> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::ListPendingOutbox { limit, reply })?;
        await_response(result).await
    }

    pub(crate) async fn mark_outbox_delivered(&self, id: &str, delivered_at: &str) -> VcResult<()> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::MarkOutboxDelivered {
            id: id.into(),
            delivered_at: delivered_at.into(),
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn append_outbox(
        &self,
        event: videocaptionerr_core::ports::OutboxEvent,
    ) -> VcResult<()> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::AppendOutbox { event, reply })?;
        await_response(result).await
    }

    pub(crate) async fn recover_artifacts(
        &self,
        roots: Vec<PathBuf>,
    ) -> VcResult<ArtifactRecoveryReport> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::RecoverArtifacts { roots, reply })?;
        await_response(result).await
    }

    fn send(&self, command: StoreCommand) -> VcResult<()> {
        self.commands.send(command).map_err(|_| {
            VcError::new(
                ErrorCode::Internal,
                "store actor stopped before accepting the command",
            )
        })
    }
}

type StoreResponse<T> = oneshot::Sender<VcResult<T>>;

fn is_constraint(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(details, _)
            if details.code == RusqliteErrorCode::ConstraintViolation
    )
}

fn stale_result(entity: &str, id: &str, expected: ExpectedVersion) -> VcError {
    let expected = match expected {
        ExpectedVersion::New => "new".to_owned(),
        ExpectedVersion::Exact(version) => version.to_string(),
    };
    VcError::new(
        ErrorCode::StaleResult,
        format!("stale {entity} aggregate {id}; expected version {expected}"),
    )
}

fn response_channel<T>() -> (StoreResponse<T>, oneshot::Receiver<VcResult<T>>) {
    oneshot::channel()
}

async fn await_response<T>(receiver: oneshot::Receiver<VcResult<T>>) -> VcResult<T> {
    receiver.await.map_err(|_| {
        VcError::new(
            ErrorCode::Internal,
            "store actor stopped before returning a response",
        )
    })?
}

enum StoreCommand {
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

fn store_actor(
    db_path: PathBuf,
    receiver: Receiver<StoreCommand>,
    ready: mpsc::SyncSender<VcResult<()>>,
) {
    let mut store = match Store::open(&db_path) {
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
    fn execute(self, store: &mut Store) {
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

/// SQLite-backed control plane.
pub struct Store {
    conn: Connection,
    path: PathBuf,
    fault: Option<StageCommitFaultPoint>,
}

impl Store {
    pub fn open(db_path: &Path) -> VcResult<Self> {
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
            path: db_path.to_path_buf(),
            fault: None,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    #[cfg(test)]
    pub fn inject_stage_commit_fault(&mut self, point: StageCommitFaultPoint) {
        self.fault = Some(point);
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn save_job_aggregate(
        &self,
        id: &str,
        batch_id: Option<&str>,
        status: &str,
        source_path: &str,
        profile_revision: &str,
        execution_snapshot_id: Option<&str>,
        aggregate_json: &str,
        expected: ExpectedVersion,
    ) -> VcResult<u64> {
        let now = chrono::Utc::now().to_rfc3339();
        let projection = execution_snapshot_id
            .map(|snapshot_id| {
                self.conn
                    .query_row(
                        "SELECT canonical_source_path, job_dir, profile_revision
                         FROM execution_snapshots WHERE snapshot_id = ?1",
                        [snapshot_id],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(|error| {
                        VcError::new(
                            ErrorCode::Internal,
                            format!("load execution snapshot projection: {error}"),
                        )
                    })?
                    .ok_or_else(|| {
                        VcError::new(
                            ErrorCode::InvalidArgument,
                            format!("execution snapshot {snapshot_id} not found"),
                        )
                    })
            })
            .transpose()?;
        let source_path = projection
            .as_ref()
            .map(|value| value.0.as_str())
            .unwrap_or(source_path);
        let job_dir = projection
            .as_ref()
            .map(|value| value.1.as_str())
            .unwrap_or("");
        let profile_revision = projection
            .as_ref()
            .map(|value| value.2.as_str())
            .unwrap_or(profile_revision);

        match expected {
            ExpectedVersion::New => {
                self.conn
                    .execute(
                        "INSERT INTO jobs (
                            id, batch_id, status, source_path, job_dir, profile_revision,
                            execution_snapshot_id, aggregate_json, aggregate_version,
                            created_at, updated_at
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1, ?9, ?9)",
                        params![
                            id,
                            batch_id,
                            status,
                            source_path,
                            job_dir,
                            profile_revision,
                            execution_snapshot_id,
                            aggregate_json,
                            now
                        ],
                    )
                    .map_err(|error| {
                        if is_constraint(&error) {
                            stale_result("Job", id, expected)
                        } else {
                            VcError::new(
                                ErrorCode::Internal,
                                format!("insert job aggregate: {error}"),
                            )
                        }
                    })?;
                Ok(1)
            }
            ExpectedVersion::Exact(version) => {
                let changed = self
                    .conn
                    .execute(
                        "UPDATE jobs SET
                            batch_id = ?1, status = ?2, source_path = ?3, job_dir = ?4,
                            profile_revision = ?5, execution_snapshot_id = ?6,
                            aggregate_json = ?7, aggregate_version = aggregate_version + 1,
                            updated_at = ?8
                         WHERE id = ?9 AND aggregate_version = ?10",
                        params![
                            batch_id,
                            status,
                            source_path,
                            job_dir,
                            profile_revision,
                            execution_snapshot_id,
                            aggregate_json,
                            now,
                            id,
                            version as i64
                        ],
                    )
                    .map_err(|error| {
                        VcError::new(
                            ErrorCode::Internal,
                            format!("update job aggregate: {error}"),
                        )
                    })?;
                if changed != 1 {
                    return Err(stale_result("Job", id, expected));
                }
                version.checked_add(1).ok_or_else(|| {
                    VcError::new(ErrorCode::Internal, "Job aggregate version overflow")
                })
            }
        }
    }

    pub(crate) fn save_batch_aggregate(
        &self,
        id: &str,
        status: &str,
        asr_model: &str,
        device: &str,
        aggregate_json: &str,
        expected: ExpectedVersion,
    ) -> VcResult<u64> {
        let now = chrono::Utc::now().to_rfc3339();
        match expected {
            ExpectedVersion::New => {
                self.conn
                    .execute(
                        "INSERT INTO batches (
                            id, status, asr_model_id, asr_device, aggregate_json,
                            aggregate_version, created_at, updated_at
                         ) VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?6)",
                        params![id, status, asr_model, device, aggregate_json, now],
                    )
                    .map_err(|error| {
                        if is_constraint(&error) {
                            stale_result("Batch", id, expected)
                        } else {
                            VcError::new(
                                ErrorCode::Internal,
                                format!("insert batch aggregate: {error}"),
                            )
                        }
                    })?;
                Ok(1)
            }
            ExpectedVersion::Exact(version) => {
                let changed = self
                    .conn
                    .execute(
                        "UPDATE batches SET
                            status = ?1, asr_model_id = ?2, asr_device = ?3,
                            aggregate_json = ?4, aggregate_version = aggregate_version + 1,
                            updated_at = ?5
                         WHERE id = ?6 AND aggregate_version = ?7",
                        params![
                            status,
                            asr_model,
                            device,
                            aggregate_json,
                            now,
                            id,
                            version as i64
                        ],
                    )
                    .map_err(|error| {
                        VcError::new(
                            ErrorCode::Internal,
                            format!("update batch aggregate: {error}"),
                        )
                    })?;
                if changed != 1 {
                    return Err(stale_result("Batch", id, expected));
                }
                version.checked_add(1).ok_or_else(|| {
                    VcError::new(ErrorCode::Internal, "Batch aggregate version overflow")
                })
            }
        }
    }

    pub(crate) fn save_work_unit_aggregate(
        &self,
        record: &WorkUnitRecord,
        expected: ExpectedVersion,
    ) -> VcResult<u64> {
        match expected {
            ExpectedVersion::New => {
                self.conn
                    .execute(
                        "INSERT INTO work_units (
                            id, job_id, stage, unit_kind, unit_index, input_hash, status, attempt,
                            artifact_id, lease_owner, lease_expires_at, aggregate_json,
                            aggregate_version
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 1)",
                        params![
                            record.id,
                            record.job_id,
                            record.stage,
                            record.unit_kind,
                            record.unit_index as i64,
                            record.input_hash,
                            record.status,
                            record.attempt as i64,
                            record.artifact_id,
                            record.lease_owner,
                            record.lease_expires_at,
                            record.aggregate_json,
                        ],
                    )
                    .map_err(|error| {
                        if is_constraint(&error) {
                            stale_result("WorkUnit", &record.id, expected)
                        } else {
                            VcError::new(ErrorCode::Internal, format!("insert work unit: {error}"))
                        }
                    })?;
                Ok(1)
            }
            ExpectedVersion::Exact(version) => {
                let changed = self
                    .conn
                    .execute(
                        "UPDATE work_units SET
                            job_id = ?1, stage = ?2, unit_kind = ?3, unit_index = ?4,
                            input_hash = ?5, status = ?6, attempt = ?7, artifact_id = ?8,
                            lease_owner = ?9, lease_expires_at = ?10, aggregate_json = ?11,
                            aggregate_version = aggregate_version + 1
                         WHERE id = ?12 AND aggregate_version = ?13",
                        params![
                            record.job_id,
                            record.stage,
                            record.unit_kind,
                            record.unit_index as i64,
                            record.input_hash,
                            record.status,
                            record.attempt as i64,
                            record.artifact_id,
                            record.lease_owner,
                            record.lease_expires_at,
                            record.aggregate_json,
                            record.id,
                            version as i64,
                        ],
                    )
                    .map_err(|error| {
                        VcError::new(ErrorCode::Internal, format!("update work unit: {error}"))
                    })?;
                if changed != 1 {
                    return Err(stale_result("WorkUnit", &record.id, expected));
                }
                version.checked_add(1).ok_or_else(|| {
                    VcError::new(ErrorCode::Internal, "WorkUnit aggregate version overflow")
                })
            }
        }
    }

    pub(crate) fn load_batch_aggregate(&self, id: &str) -> VcResult<Option<(String, u64)>> {
        self.conn
            .query_row(
                "SELECT aggregate_json, aggregate_version FROM batches WHERE id = ?1",
                [id],
                |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64)),
            )
            .optional()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("load batch aggregate: {e}")))
    }

    pub(crate) fn list_batch_aggregates(&self) -> VcResult<Vec<(String, u64)>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT aggregate_json, aggregate_version FROM batches
                 WHERE aggregate_json IS NOT NULL ORDER BY created_at, id",
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("list batch aggregates: {error}"),
                )
            })?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("query batch aggregates: {error}"),
                )
            })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("read batch aggregates: {error}"),
            )
        })
    }

    pub(crate) fn load_work_unit_aggregate(&self, id: &str) -> VcResult<Option<(String, u64)>> {
        self.conn
            .query_row(
                "SELECT aggregate_json, aggregate_version FROM work_units WHERE id = ?1",
                [id],
                |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64)),
            )
            .optional()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("load work unit: {e}")))
    }

    pub(crate) fn find_work_unit_aggregate(
        &self,
        job_id: &str,
        stage: &str,
        unit_kind: &str,
        unit_index: u32,
        input_hash: &str,
    ) -> VcResult<Option<(String, u64)>> {
        self.conn
            .query_row(
                "SELECT aggregate_json, aggregate_version FROM work_units
                 WHERE job_id = ?1 AND stage = ?2 AND unit_kind = ?3
                   AND unit_index = ?4 AND input_hash = ?5
                 ORDER BY id LIMIT 1",
                params![job_id, stage, unit_kind, unit_index as i64, input_hash],
                |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64)),
            )
            .optional()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("find work unit: {e}")))
    }

    pub(crate) fn list_expired_work_unit_aggregates(
        &self,
        now_rfc3339: &str,
    ) -> VcResult<Vec<(String, u64)>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT aggregate_json, aggregate_version FROM work_units
                 WHERE status = 'running'
                   AND lease_expires_at IS NOT NULL
                   AND lease_expires_at < ?1
                   AND aggregate_json IS NOT NULL
                 ORDER BY unit_index, id",
            )
            .map_err(|e| {
                VcError::new(ErrorCode::Internal, format!("prepare expired units: {e}"))
            })?;
        let rows = statement
            .query_map([now_rfc3339], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("query expired units: {e}")))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("read expired units: {e}")))
    }

    pub(crate) fn load_job_aggregate(&self, id: &str) -> VcResult<Option<(String, u64)>> {
        self.conn
            .query_row(
                "SELECT aggregate_json, aggregate_version FROM jobs WHERE id = ?1",
                [id],
                |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64)),
            )
            .optional()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("load job aggregate: {e}")))
    }

    pub(crate) fn list_job_aggregates(&self) -> VcResult<Vec<(String, u64)>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT aggregate_json, aggregate_version FROM jobs
                 WHERE aggregate_json IS NOT NULL
                 ORDER BY created_at, id",
            )
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("list job aggregates: {e}")))?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("query job aggregates: {e}")))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("read job aggregates: {e}")))
    }

    pub(crate) fn delete_job_record(&self, id: &str) -> VcResult<()> {
        self.conn
            .execute("DELETE FROM jobs WHERE id = ?1", [id])
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("delete job: {e}")))?;
        Ok(())
    }

    pub(crate) fn save_execution_snapshot(&self, snapshot: &JobExecutionSnapshot) -> VcResult<()> {
        snapshot
            .validate()
            .map_err(|error| VcError::new(ErrorCode::InvalidArgument, error))?;
        let snapshot_json = serde_json::to_string(snapshot).map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode execution snapshot: {error}"),
            )
        })?;
        let llm_json = snapshot
            .llm
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("encode LLM execution snapshot: {error}"),
                )
            })?;
        let stream_selection = serde_json::to_string(&snapshot.audio_stream).map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode audio stream selection: {error}"),
            )
        })?;
        let existing: Option<String> = self
            .conn
            .query_row(
                "SELECT snapshot_json FROM execution_snapshots WHERE snapshot_id = ?1",
                [&snapshot.snapshot_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("load existing execution snapshot: {error}"),
                )
            })?;
        if let Some(existing) = existing {
            if existing == snapshot_json {
                return Ok(());
            }
            return Err(VcError::new(
                ErrorCode::StaleResult,
                format!(
                    "execution snapshot {} is immutable and already contains different data",
                    snapshot.snapshot_id
                ),
            ));
        }

        self.conn
            .execute(
                "INSERT INTO execution_snapshots (
                    snapshot_id, schema_version, job_id, batch_id, created_at,
                    canonical_source_path, source_size, source_modified_at_ms, job_dir,
                    profile_revision, asr_engine, model_locator, model_id, model_digest,
                    device, compute_type, audio_stream_selection, source_language,
                    target_language, output_path, output_format, output_layout,
                    conflict_policy, fallback_to_source, llm_json, snapshot_json
                 ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                    ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26
                 )",
                params![
                    snapshot.snapshot_id.to_string(),
                    snapshot.schema_version as i64,
                    snapshot.job_id.to_string(),
                    snapshot.batch_id.to_string(),
                    snapshot.created_at,
                    snapshot.canonical_source_path,
                    snapshot.source_stat.size as i64,
                    snapshot
                        .source_stat
                        .modified_at_ms
                        .map(|value| value as i64),
                    snapshot.job_dir,
                    snapshot.profile_revision.to_string(),
                    snapshot.asr.engine,
                    snapshot.asr.model_locator,
                    snapshot.asr.model_id,
                    snapshot.asr.model_digest,
                    snapshot.asr.device,
                    snapshot.asr.compute_type,
                    stream_selection,
                    snapshot.source_language,
                    snapshot.target_language,
                    snapshot.output.path,
                    snapshot.output.format,
                    snapshot.output.layout,
                    snapshot.output.conflict_policy,
                    snapshot.output.fallback_to_source,
                    llm_json,
                    snapshot_json,
                ],
            )
            .map_err(|error| {
                if is_constraint(&error) {
                    VcError::new(
                        ErrorCode::StaleResult,
                        format!("execution snapshot {} already exists", snapshot.snapshot_id),
                    )
                } else {
                    VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("save execution snapshot: {error}"),
                    )
                }
            })?;
        Ok(())
    }

    pub(crate) fn load_execution_snapshot(&self, id: &str) -> VcResult<Option<String>> {
        self.conn
            .query_row(
                "SELECT snapshot_json FROM execution_snapshots WHERE snapshot_id = ?1",
                [id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("load execution snapshot: {error}"),
                )
            })
    }

    pub(crate) fn load_snapshots_for_batch(&self, batch_id: &str) -> VcResult<Vec<String>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT snapshot_json FROM execution_snapshots
                 WHERE batch_id = ?1 ORDER BY job_id",
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("prepare batch execution snapshots: {error}"),
                )
            })?;
        let rows = statement
            .query_map([batch_id], |row| row.get::<_, String>(0))
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("query batch execution snapshots: {error}"),
                )
            })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("read batch execution snapshots: {error}"),
            )
        })
    }

    /// Atomically claim the oldest pending unit and persist its domain lease.
    pub(crate) fn lease_next_ready(
        &mut self,
        request: &LeaseRequest,
    ) -> VcResult<Option<(String, u64)>> {
        let tx = self.conn.unchecked_transaction().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("begin lease transaction: {error}"),
            )
        })?;
        let selected: Option<(String, String, u64)> = tx
            .query_row(
                "SELECT id, aggregate_json, aggregate_version FROM work_units
                 WHERE job_id = ?1 AND stage = ?2 AND status = 'pending'
                   AND aggregate_json IS NOT NULL
                 ORDER BY unit_index, id LIMIT 1",
                params![request.job_id, request.stage],
                |row| Ok((row.get(0)?, row.get(1)?, row.get::<_, i64>(2)? as u64)),
            )
            .optional()
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("select ready work unit: {error}"),
                )
            })?;
        let Some((id, aggregate_json, version)) = selected else {
            tx.commit().map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("finish empty lease transaction: {error}"),
                )
            })?;
            return Ok(None);
        };

        let mut unit: videocaptionerr_domain::WorkUnit = serde_json::from_str(&aggregate_json)
            .map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCorrupt,
                    format!("decode ready work unit {id}: {error}"),
                )
            })?;
        unit.lease_for(&request.owner, request.now_ms, request.expires_at_ms)
            .map_err(VcError::from)?;
        let updated_json = serde_json::to_string(&unit).map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode leased work unit: {error}"),
            )
        })?;
        let changed = tx
            .execute(
                "UPDATE work_units SET status = 'running', attempt = ?1,
                 lease_owner = ?2, lease_expires_at = ?3, started_at = ?4,
                 aggregate_json = ?5, aggregate_version = aggregate_version + 1
                 WHERE id = ?6 AND status = 'pending' AND aggregate_version = ?7",
                params![
                    unit.attempt() as i64,
                    request.owner,
                    request.expires_rfc3339,
                    request.now_rfc3339,
                    updated_json,
                    id,
                    version as i64,
                ],
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("persist work unit lease: {error}"),
                )
            })?;
        if changed != 1 {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                "work unit was claimed by another scheduler",
            ));
        }
        tx.commit().map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("commit work unit lease: {error}"),
            )
        })?;
        Ok(Some((updated_json, version.saturating_add(1))))
    }

    /// Retry failed work units from the requested stage onward. Domain
    /// transitions are applied before the control row is updated.
    pub(crate) fn retry_failed(&mut self, job_id: &str, from_stage: Option<&str>) -> VcResult<u32> {
        let tx = self.conn.unchecked_transaction().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("begin retry transaction: {error}"),
            )
        })?;
        let mut statement = tx
            .prepare(
                "SELECT id, stage, aggregate_json FROM work_units
                 WHERE job_id = ?1 AND status IN ('failed', 'cancelled')
                   AND aggregate_json IS NOT NULL ORDER BY unit_index, id",
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("prepare failed units: {error}"),
                )
            })?;
        let rows = statement
            .query_map([job_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|error| {
                VcError::new(ErrorCode::Internal, format!("query failed units: {error}"))
            })?;
        let candidates = rows.collect::<Result<Vec<_>, _>>().map_err(|error| {
            VcError::new(ErrorCode::Internal, format!("read failed units: {error}"))
        })?;
        drop(statement);

        let mut retried = 0u32;
        for (id, stage, aggregate_json) in candidates {
            if from_stage.is_some_and(|start| stage_rank(&stage) < stage_rank(start)) {
                continue;
            }
            let mut unit: videocaptionerr_domain::WorkUnit = serde_json::from_str(&aggregate_json)
                .map_err(|error| {
                    VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode failed work unit {id}: {error}"),
                    )
                })?;
            unit.retry().map_err(VcError::from)?;
            let updated_json = serde_json::to_string(&unit).map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("encode retried work unit: {error}"),
                )
            })?;
            tx.execute(
                "UPDATE work_units SET status = 'pending', attempt = ?1,
                 error_code = NULL, error_json = NULL, artifact_id = NULL,
                 lease_owner = NULL, lease_expires_at = NULL, started_at = NULL,
                 finished_at = NULL, aggregate_json = ?2,
                 aggregate_version = aggregate_version + 1 WHERE id = ?3",
                params![unit.attempt() as i64, updated_json, id],
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("persist retried work unit: {error}"),
                )
            })?;
            retried = retried.saturating_add(1);
        }
        tx.commit().map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("commit retry transaction: {error}"),
            )
        })?;
        Ok(retried)
    }

    pub(crate) fn count_retryable(&self, job_id: &str, from_stage: Option<&str>) -> VcResult<u32> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT stage FROM work_units
                 WHERE job_id = ?1 AND status IN ('failed', 'cancelled')
                   AND aggregate_json IS NOT NULL ORDER BY unit_index, id",
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("prepare retryable work units: {error}"),
                )
            })?;
        let rows = statement
            .query_map([job_id], |row| row.get::<_, String>(0))
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("query retryable work units: {error}"),
                )
            })?;
        let mut count = 0u32;
        for row in rows {
            let stage = row.map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("read retryable work unit: {error}"),
                )
            })?;
            if from_stage.is_none_or(|start| stage_rank(&stage) >= stage_rank(start)) {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    pub(crate) fn load_capability_probe(
        &self,
        provider_profile_id: &str,
        model: &str,
        probe_hash: &str,
    ) -> VcResult<Option<String>> {
        self.conn
            .query_row(
                "SELECT result_json FROM llm_capability_probes
                 WHERE provider_profile_id = ?1 AND model = ?2 AND probe_hash = ?3",
                params![provider_profile_id, model, probe_hash],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("load LLM capability probe: {error}"),
                )
            })
    }

    pub(crate) fn save_capability_probe(&mut self, record: &CapabilityProbeRecord) -> VcResult<()> {
        self.conn
            .execute(
                "INSERT INTO llm_capability_probes (
                    id, provider_profile_id, model, probe_hash, result_json,
                    created_at, expires_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(provider_profile_id, model, probe_hash) DO UPDATE SET
                    result_json = excluded.result_json,
                    created_at = excluded.created_at,
                    expires_at = excluded.expires_at",
                params![
                    record.id,
                    record.provider_profile_id,
                    record.model,
                    record.probe_hash,
                    record.result_json,
                    record.created_at,
                    record.expires_at,
                ],
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("save LLM capability probe: {error}"),
                )
            })?;
        Ok(())
    }

    pub fn insert_job(
        &self,
        id: &str,
        batch_id: Option<&str>,
        source_path: &str,
        job_dir: &str,
        status: &str,
    ) -> VcResult<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn
            .execute(
                "INSERT INTO jobs (id, batch_id, status, source_path, job_dir, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
                params![id, batch_id, status, source_path, job_dir, now],
            )
            .map_err(|e| {
                VcError::new(ErrorCode::Internal, format!("insert job: {e}"))
            })?;
        Ok(())
    }

    pub fn get_job_status(&self, id: &str) -> VcResult<Option<String>> {
        self.conn
            .query_row("SELECT status FROM jobs WHERE id = ?1", [id], |r| r.get(0))
            .optional()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("get job: {e}")))
    }

    pub fn mark_job_done(
        &self,
        id: &str,
        source_hash: &str,
        pcm_hash: &str,
        selected_stream_index: i64,
        language: Option<&str>,
    ) -> VcResult<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn
            .execute(
                "UPDATE jobs SET status='done', source_hash=?1, pcm_hash=?2,
                 selected_stream_index=?3, language=?4, updated_at=?5, finished_at=?5
                 WHERE id=?6",
                params![
                    source_hash,
                    pcm_hash,
                    selected_stream_index,
                    language,
                    now,
                    id
                ],
            )
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("mark job done: {e}")))?;
        Ok(())
    }

    /// Insert artifact metadata and mark committed in one transaction with a work unit update.
    pub fn commit_artifact_and_unit(
        &mut self,
        meta: &ArtifactMeta,
        work_unit_id: Option<&str>,
    ) -> VcResult<()> {
        let tx = self.conn.unchecked_transaction().map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("begin commit tx: {e}"),
            )
        })?;

        tx.execute(
            "INSERT INTO artifacts (
                id, job_id, stage, kind, path, content_hash, schema_version,
                producer_fingerprint, created_at, committed
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1)",
            params![
                meta.id,
                meta.job_id,
                meta.stage,
                meta.kind.as_str(),
                meta.path,
                meta.content_hash,
                meta.schema_version as i64,
                meta.producer_fingerprint,
                meta.created_at,
            ],
        )
        .map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("insert artifact: {e}"),
            )
        })?;

        if let Some(unit_id) = work_unit_id {
            let now = chrono::Utc::now().to_rfc3339();
            let aggregate_json: String = tx
                .query_row(
                    "SELECT COALESCE(aggregate_json, '') FROM work_units WHERE id = ?1",
                    [unit_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("load work unit for artifact commit: {e}"),
                    )
                })?
                .ok_or_else(|| {
                    VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("work unit not found during artifact commit: {unit_id}"),
                    )
                })?;
            if aggregate_json.is_empty() {
                let changed = tx
                    .execute(
                        "UPDATE work_units SET status = ?1, artifact_id = ?2, finished_at = ?3,
                         lease_owner = NULL, lease_expires_at = NULL,
                         aggregate_version = aggregate_version + 1
                         WHERE id = ?4 AND status = 'running'",
                        params![WorkUnitStatus::Done.as_str(), meta.id, now, unit_id],
                    )
                    .map_err(|e| {
                        VcError::new(
                            ErrorCode::ArtifactCommitFailed,
                            format!("update legacy work unit: {e}"),
                        )
                    })?;
                if changed != 1 {
                    return Err(VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("work unit {unit_id} was not running during artifact commit"),
                    ));
                }
                return tx.commit().map_err(|e| {
                    VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("commit artifact tx: {e}"),
                    )
                });
            }
            let mut unit: videocaptionerr_domain::WorkUnit = serde_json::from_str(&aggregate_json)
                .map_err(|e| {
                    VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode work unit during artifact commit: {e}"),
                    )
                })?;
            if unit.job_id().as_str() != meta.job_id {
                return Err(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    "work unit and artifact belong to different Jobs",
                ));
            }
            let artifact_id: UlidStr = meta.id.parse().map_err(|e| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("invalid artifact id during work unit commit: {e}"),
                )
            })?;
            let artifact_stage =
                videocaptionerr_domain::StageKind::parse(&meta.stage).ok_or_else(|| {
                    VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!(
                            "invalid artifact stage during work unit commit: {}",
                            meta.stage
                        ),
                    )
                })?;
            unit.complete(videocaptionerr_domain::ArtifactRef {
                id: artifact_id,
                stage: artifact_stage,
                path: meta.path.clone(),
                content_hash: meta.content_hash.clone(),
                schema_version: meta.schema_version,
                producer_fingerprint: meta.producer_fingerprint.clone(),
            })
            .map_err(VcError::from)?;
            let updated_json = serde_json::to_string(&unit).map_err(|e| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("encode completed work unit: {e}"),
                )
            })?;
            let changed = tx
                .execute(
                    "UPDATE work_units SET status = ?1, artifact_id = ?2, finished_at = ?3,
                     lease_owner = NULL, lease_expires_at = NULL, aggregate_json = ?5,
                     aggregate_version = aggregate_version + 1
                     WHERE id = ?4 AND status = 'running'",
                    params![
                        WorkUnitStatus::Done.as_str(),
                        meta.id,
                        now,
                        unit_id,
                        updated_json,
                    ],
                )
                .map_err(|e| {
                    VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("update work unit: {e}"),
                    )
                })?;
            if changed != 1 {
                return Err(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("work unit {unit_id} was not running during artifact commit"),
                ));
            }
        }

        tx.commit().map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("commit artifact tx: {e}"),
            )
        })?;
        Ok(())
    }

    /// Publish a prepared artifact and atomically persist the control-plane
    /// state that makes it reachable. A file published by this invocation is
    /// removed if the SQLite transaction fails; a process crash between those
    /// operations is handled by `recover_artifacts` on the next startup.
    pub(crate) fn commit_stage(
        &mut self,
        request: videocaptionerr_core::ports::StageCommitRequest,
    ) -> VcResult<videocaptionerr_core::ports::StageCommitResult> {
        let fault = self.fault.take();
        let published = request
            .artifact
            .as_ref()
            .map(|artifact| publish_prepared_artifact_with_fault(artifact, fault))
            .transpose()?;
        let result = self.commit_stage_transaction(&request, fault);
        if result.is_err() && published == Some(true) && fault.is_none() {
            if let Some(artifact) = &request.artifact {
                let path = Path::new(&artifact.artifact.path);
                let _ = fs::remove_file(path);
                sync_parent(path);
            }
        }
        result
    }

    fn commit_stage_transaction(
        &mut self,
        request: &videocaptionerr_core::ports::StageCommitRequest,
        fault: Option<StageCommitFaultPoint>,
    ) -> VcResult<videocaptionerr_core::ports::StageCommitResult> {
        let tx = self.conn.unchecked_transaction().map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("begin stage commit transaction: {error}"),
            )
        })?;

        if let Some((job, expected)) = &request.job {
            if matches!(expected, ExpectedVersion::New) {
                insert_job_tx(&tx, job)?;
            }
        }
        if let Some((unit, expected)) = &request.work_unit {
            if matches!(expected, ExpectedVersion::New) {
                insert_work_unit_tx(&tx, unit)?;
            }
        }

        if let Some(artifact) = &request.artifact {
            let meta = artifact_meta_for(artifact);
            tx.execute(
                "INSERT INTO artifacts (
                    id, job_id, stage, kind, path, content_hash, schema_version,
                    producer_fingerprint, created_at, committed
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1)",
                params![
                    meta.id,
                    meta.job_id,
                    meta.stage,
                    meta.kind.as_str(),
                    meta.path,
                    meta.content_hash,
                    meta.schema_version as i64,
                    meta.producer_fingerprint,
                    meta.created_at,
                ],
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("insert stage artifact: {error}"),
                )
            })?;
            stage_fault_at(fault, StageCommitFaultPoint::AfterArtifactInsert)?;
        }

        if let Some((unit, expected)) = &request.work_unit {
            if let Some(artifact) = &request.artifact {
                if unit.value.job_id() != &artifact.job_id {
                    return Err(VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        "work unit and artifact belong to different Jobs",
                    ));
                }
            }
            if !matches!(expected, ExpectedVersion::New) {
                update_work_unit_tx(&tx, unit, *expected)?;
            }
            stage_fault_at(fault, StageCommitFaultPoint::AfterWorkUnitUpdate)?;
        }

        if let Some((job, expected)) = &request.job {
            if let Some(artifact) = &request.artifact {
                if job.value.id() != &artifact.job_id {
                    return Err(VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        "Job and artifact belong to different Jobs",
                    ));
                }
            }
            if !matches!(expected, ExpectedVersion::New) {
                update_job_tx(&tx, job, *expected)?;
            }
            sync_stage_projection(&tx, &job.value)?;
            stage_fault_at(fault, StageCommitFaultPoint::AfterJobUpdate)?;
        }

        if let Some(event) = &request.event {
            insert_outbox_tx(&tx, event)?;
            stage_fault_at(fault, StageCommitFaultPoint::AfterOutboxInsert)?;
        }

        tx.commit().map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("commit stage transaction: {error}"),
            )
        })?;
        stage_fault_at(fault, StageCommitFaultPoint::AfterDbCommit)?;

        Ok(videocaptionerr_core::ports::StageCommitResult {
            job: request.job.as_ref().map(|(job, expected)| {
                let version = next_version(job.version, *expected);
                videocaptionerr_core::ports::Versioned::with_version(job.value.clone(), version)
            }),
            work_unit: request.work_unit.as_ref().map(|(unit, expected)| {
                let version = next_version(unit.version, *expected);
                videocaptionerr_core::ports::Versioned::with_version(unit.value.clone(), version)
            }),
        })
    }

    pub(crate) fn list_pending_outbox(
        &self,
        limit: u32,
    ) -> VcResult<Vec<videocaptionerr_core::ports::StoredOutboxEvent>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT id, aggregate_type, aggregate_id, sequence, event_type,
                        payload_json, created_at, delivered_at
                 FROM outbox_events WHERE delivered_at IS NULL
                 ORDER BY created_at, id LIMIT ?1",
            )
            .map_err(|error| {
                VcError::new(ErrorCode::Internal, format!("prepare outbox: {error}"))
            })?;
        let rows = statement
            .query_map([limit as i64], |row| {
                let id: String = row.get(0)?;
                Ok(videocaptionerr_core::ports::StoredOutboxEvent {
                    id: id.parse().map_err(|_| {
                        rusqlite::Error::InvalidColumnType(
                            0,
                            "id".into(),
                            rusqlite::types::Type::Text,
                        )
                    })?,
                    aggregate_type: row.get(1)?,
                    aggregate_id: row.get(2)?,
                    sequence: row.get::<_, i64>(3)? as u64,
                    event_type: row.get(4)?,
                    payload_json: row.get(5)?,
                    created_at: row.get(6)?,
                    delivered_at: row.get(7)?,
                })
            })
            .map_err(|error| VcError::new(ErrorCode::Internal, format!("query outbox: {error}")))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| VcError::new(ErrorCode::Internal, format!("read outbox: {error}")))
    }

    pub(crate) fn mark_outbox_delivered(&self, id: &str, delivered_at: &str) -> VcResult<()> {
        self.conn
            .execute(
                "UPDATE outbox_events SET delivered_at = ?1 WHERE id = ?2",
                params![delivered_at, id],
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("mark outbox delivered: {error}"),
                )
            })?;
        Ok(())
    }

    pub(crate) fn append_outbox(
        &mut self,
        event: &videocaptionerr_core::ports::OutboxEvent,
    ) -> VcResult<()> {
        let tx = self.conn.unchecked_transaction().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("begin outbox transaction: {error}"),
            )
        })?;
        insert_outbox_tx(&tx, event)?;
        tx.commit().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("commit outbox transaction: {error}"),
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_work_unit(
        &self,
        id: &str,
        job_id: &str,
        stage: &str,
        unit_kind: &str,
        unit_index: i64,
        input_hash: &str,
        status: WorkUnitStatus,
    ) -> VcResult<()> {
        self.conn
            .execute(
                "INSERT INTO work_units (
                    id, job_id, stage, unit_kind, unit_index, input_hash, status, attempt
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0)",
                params![
                    id,
                    job_id,
                    stage,
                    unit_kind,
                    unit_index,
                    input_hash,
                    status.as_str()
                ],
            )
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("insert work unit: {e}")))?;
        Ok(())
    }

    pub fn get_work_unit_status(&self, id: &str) -> VcResult<Option<WorkUnitStatus>> {
        let s: Option<String> = self
            .conn
            .query_row("SELECT status FROM work_units WHERE id = ?1", [id], |r| {
                r.get(0)
            })
            .optional()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("get work unit: {e}")))?;
        Ok(s.and_then(|x| WorkUnitStatus::parse(&x)))
    }

    /// Expire running leases: return to Pending and increment attempt.
    pub fn recover_expired_leases(&self, now_rfc3339: &str) -> VcResult<usize> {
        let n = self
            .conn
            .execute(
                "UPDATE work_units
                 SET status = 'pending',
                     attempt = attempt + 1,
                     lease_owner = NULL,
                     lease_expires_at = NULL,
                     started_at = NULL
                 WHERE status = 'running'
                   AND lease_expires_at IS NOT NULL
                   AND lease_expires_at < ?1",
                [now_rfc3339],
            )
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("recover leases: {e}")))?;
        Ok(n)
    }

    /// Reconcile files and committed artifact metadata after an interrupted
    /// stage commit. The DB remains the authority: files without a committed
    /// row are quarantined, while invalid referenced artifacts invalidate the
    /// affected stage and dependent WorkUnits.
    pub(crate) fn recover_artifacts(
        &mut self,
        roots: &[PathBuf],
    ) -> VcResult<ArtifactRecoveryReport> {
        let mut report = ArtifactRecoveryReport::default();
        let mut referenced = HashSet::new();
        let mut statement = self
            .conn
            .prepare(
                "SELECT id, job_id, stage, path, content_hash, committed
                 FROM artifacts ORDER BY id",
            )
            .map_err(|error| {
                VcError::new(ErrorCode::Internal, format!("prepare artifacts: {error}"))
            })?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })
            .map_err(|error| {
                VcError::new(ErrorCode::Internal, format!("query artifacts: {error}"))
            })?;
        let artifact_rows = rows.collect::<Result<Vec<_>, _>>().map_err(|error| {
            VcError::new(ErrorCode::Internal, format!("read artifacts: {error}"))
        })?;
        drop(statement);

        let tx = self.conn.unchecked_transaction().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("begin artifact recovery: {error}"),
            )
        })?;
        for (id, job_id, stage, path, expected_hash, committed) in &artifact_rows {
            if *committed == 1 {
                referenced.insert(path.clone());
                let valid = Path::new(path).is_file()
                    && blake3_file(Path::new(path)).ok().as_deref() == Some(expected_hash);
                if !valid {
                    report.corrupt_artifacts.push(id.clone());
                    invalidate_artifact_references(&tx, id, job_id, stage)?;
                    tx.execute("UPDATE artifacts SET committed = 0 WHERE id = ?1", [id])
                        .map_err(|error| {
                            VcError::new(
                                ErrorCode::Internal,
                                format!("mark corrupt artifact {id}: {error}"),
                            )
                        })?;
                }
            }
        }
        tx.commit().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("commit artifact recovery: {error}"),
            )
        })?;

        for root in roots {
            let mut files = Vec::new();
            collect_files(root, &mut files)?;
            for path in files {
                if is_recovery_path(&path) {
                    continue;
                }
                let name = path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("");
                if name.ends_with(".partial") || name.ends_with(".tmp") || name.contains(".tmp.") {
                    quarantine_file(root, &path)?;
                    report.partial_files.push(path);
                } else if !referenced.contains(&path.to_string_lossy().into_owned()) {
                    quarantine_file(root, &path)?;
                    report.orphan_files.push(path);
                }
            }
        }
        Ok(report)
    }

    pub fn append_job_event(
        &self,
        job_id: &str,
        event_type: &str,
        payload_json: Option<&str>,
    ) -> VcResult<String> {
        let id = UlidStr::from(Ulid::new()).into_string();
        let next_seq: i64 = self
            .conn
            .query_row(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM job_events WHERE job_id = ?1",
                [job_id],
                |r| r.get(0),
            )
            .unwrap_or(1);
        let now = chrono::Utc::now().to_rfc3339();
        self.conn
            .execute(
                "INSERT INTO job_events (id, job_id, seq, event_type, payload_json, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![id, job_id, next_seq, event_type, payload_json, now],
            )
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("append event: {e}")))?;
        Ok(id)
    }

    pub fn new_artifact_meta(
        job_id: &str,
        stage: &str,
        kind: ArtifactKind,
        path: &str,
        content_hash: &str,
        producer_fingerprint: &str,
    ) -> ArtifactMeta {
        ArtifactMeta::new(
            UlidStr::from(Ulid::new()).into_string(),
            job_id,
            stage,
            kind,
            path,
            content_hash,
            producer_fingerprint,
            chrono::Utc::now().to_rfc3339(),
        )
    }
}

fn invalidate_artifact_references(
    tx: &rusqlite::Transaction<'_>,
    artifact_id: &str,
    job_id: &str,
    stage_name: &str,
) -> VcResult<()> {
    if let Some(stage) = videocaptionerr_domain::StageKind::parse(stage_name) {
        let job_row: Option<(Option<String>, i64)> = tx
            .query_row(
                "SELECT aggregate_json, aggregate_version FROM jobs WHERE id = ?1",
                [job_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| {
                VcError::new(ErrorCode::Internal, format!("load corrupt Job: {error}"))
            })?;
        match job_row {
            Some((Some(body), _version)) => {
                let mut job: videocaptionerr_domain::Job =
                    serde_json::from_str(&body).map_err(|error| {
                        VcError::new(
                            ErrorCode::ArtifactCorrupt,
                            format!("decode corrupt Job: {error}"),
                        )
                    })?;
                job.invalidate_stage_for_recovery(stage)
                    .map_err(VcError::from)?;
                let body = serde_json::to_string(&job).map_err(|error| {
                    VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("encode recovered Job: {error}"),
                    )
                })?;
                tx.execute(
                    "UPDATE jobs SET status = ?1, aggregate_json = ?2,
                     aggregate_version = aggregate_version + 1, updated_at = ?3
                     WHERE id = ?4",
                    params![
                        job_status_name(job.status()),
                        body,
                        chrono::Utc::now().to_rfc3339(),
                        job_id,
                    ],
                )
                .map_err(|error| {
                    VcError::new(ErrorCode::Internal, format!("recover Job: {error}"))
                })?;
                sync_stage_projection(tx, &job)?;
            }
            Some((None, _version)) => {
                tx.execute(
                    "UPDATE jobs SET status = 'pending', aggregate_version = aggregate_version + 1,
                     updated_at = ?1 WHERE id = ?2",
                    params![chrono::Utc::now().to_rfc3339(), job_id],
                )
                .map_err(|error| {
                    VcError::new(ErrorCode::Internal, format!("recover legacy Job: {error}"))
                })?;
            }
            None => {}
        }
    }

    let mut statement = tx
        .prepare("SELECT id, aggregate_json FROM work_units WHERE artifact_id = ?1")
        .map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("prepare corrupt WorkUnits: {error}"),
            )
        })?;
    let rows = statement
        .query_map([artifact_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("query corrupt WorkUnits: {error}"),
            )
        })?;
    let units = rows.collect::<Result<Vec<_>, _>>().map_err(|error| {
        VcError::new(
            ErrorCode::Internal,
            format!("read corrupt WorkUnits: {error}"),
        )
    })?;
    drop(statement);
    for (unit_id, body) in units {
        if let Some(body) = body {
            let mut unit: videocaptionerr_domain::WorkUnit =
                serde_json::from_str(&body).map_err(|error| {
                    VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode corrupt WorkUnit: {error}"),
                    )
                })?;
            unit.invalidate_artifact_for_recovery("ARTIFACT_CORRUPT")
                .map_err(VcError::from)?;
            let body = serde_json::to_string(&unit).map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("encode recovered WorkUnit: {error}"),
                )
            })?;
            tx.execute(
                "UPDATE work_units SET status = 'pending', artifact_id = NULL,
                 error_code = 'ARTIFACT_CORRUPT', aggregate_json = ?1,
                 aggregate_version = aggregate_version + 1 WHERE id = ?2",
                params![body, unit_id],
            )
        } else {
            tx.execute(
                "UPDATE work_units SET status = 'pending', artifact_id = NULL,
                 error_code = 'ARTIFACT_CORRUPT', aggregate_version = aggregate_version + 1
                 WHERE id = ?1",
                [unit_id],
            )
        }
        .map_err(|error| VcError::new(ErrorCode::Internal, format!("recover WorkUnit: {error}")))?;
    }
    Ok(())
}

fn collect_files(root: &Path, files: &mut Vec<PathBuf>) -> VcResult<()> {
    if !root.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(root).map_err(|error| {
        VcError::new(
            ErrorCode::Internal,
            format!("scan recovery root {}: {error}", root.display()),
        )
    })? {
        let path = entry
            .map_err(|error| {
                VcError::new(ErrorCode::Internal, format!("read recovery entry: {error}"))
            })?
            .path();
        if path.file_name().and_then(|value| value.to_str()) == Some(".recovery-quarantine") {
            continue;
        }
        if path.is_dir() {
            collect_files(&path, files)?;
        } else if path.is_file() {
            files.push(path);
        }
    }
    Ok(())
}

fn is_recovery_path(path: &Path) -> bool {
    path.components()
        .any(|component| component.as_os_str() == ".recovery-quarantine")
}

fn quarantine_file(root: &Path, path: &Path) -> VcResult<()> {
    let quarantine = root.join(".recovery-quarantine");
    fs::create_dir_all(&quarantine).map_err(|error| {
        VcError::new(
            ErrorCode::Internal,
            format!("create recovery quarantine: {error}"),
        )
    })?;
    let name = path.file_name().ok_or_else(|| {
        VcError::new(
            ErrorCode::Internal,
            format!("recovery path has no filename: {}", path.display()),
        )
    })?;
    let mut destination = quarantine.join(name);
    if destination.exists() {
        destination = quarantine.join(format!("{}.{}", name.to_string_lossy(), Ulid::new()));
    }
    fs::rename(path, &destination).map_err(|error| {
        VcError::new(
            ErrorCode::Internal,
            format!(
                "quarantine {} -> {}: {error}",
                path.display(),
                destination.display()
            ),
        )
    })?;
    Ok(())
}

fn next_version(current: u64, expected: ExpectedVersion) -> u64 {
    match expected {
        ExpectedVersion::New => 1,
        ExpectedVersion::Exact(_) => current.saturating_add(1),
    }
}

fn stage_fault_at(
    configured: Option<StageCommitFaultPoint>,
    point: StageCommitFaultPoint,
) -> VcResult<()> {
    if configured == Some(point) {
        return Err(VcError::new(
            ErrorCode::Internal,
            format!("injected stage commit interruption at {point:?}"),
        ));
    }
    Ok(())
}

fn artifact_meta_for(prepared: &videocaptionerr_core::ports::PreparedArtifact) -> ArtifactMeta {
    let artifact = &prepared.artifact;
    ArtifactMeta {
        schema_version: artifact.schema_version,
        id: artifact.id.to_string(),
        job_id: prepared.job_id.to_string(),
        stage: artifact.stage.as_str().into(),
        kind: artifact_kind(artifact.stage),
        path: artifact.path.clone(),
        content_hash: artifact.content_hash.clone(),
        producer_fingerprint: artifact.producer_fingerprint.clone(),
        created_at: chrono::Utc::now().to_rfc3339(),
        committed: true,
    }
}

fn artifact_kind(stage: videocaptionerr_domain::StageKind) -> ArtifactKind {
    match stage {
        videocaptionerr_domain::StageKind::Probe => ArtifactKind::MediaProbe,
        videocaptionerr_domain::StageKind::ExtractAudio => ArtifactKind::AudioWav,
        videocaptionerr_domain::StageKind::Asr => ArtifactKind::Transcript,
        videocaptionerr_domain::StageKind::Split
        | videocaptionerr_domain::StageKind::Correct
        | videocaptionerr_domain::StageKind::Translate => ArtifactKind::Transcript,
        videocaptionerr_domain::StageKind::Export => ArtifactKind::Other,
    }
}

fn snapshot_projection(
    tx: &rusqlite::Transaction<'_>,
    snapshot_id: Option<&videocaptionerr_domain::UlidStr>,
) -> VcResult<Option<(String, String, String)>> {
    let Some(snapshot_id) = snapshot_id else {
        return Ok(None);
    };
    tx.query_row(
        "SELECT canonical_source_path, job_dir, profile_revision
         FROM execution_snapshots WHERE snapshot_id = ?1",
        [snapshot_id.as_str()],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        },
    )
    .optional()
    .map_err(|error| {
        VcError::new(
            ErrorCode::Internal,
            format!("load execution snapshot projection: {error}"),
        )
    })
}

fn insert_job_tx(
    tx: &rusqlite::Transaction<'_>,
    job: &videocaptionerr_core::ports::Versioned<videocaptionerr_domain::Job>,
) -> VcResult<()> {
    let now = chrono::Utc::now().to_rfc3339();
    let projection = snapshot_projection(tx, job.value.execution_snapshot_id())?;
    let source_path = projection
        .as_ref()
        .map(|value| value.0.as_str())
        .unwrap_or(job.value.source_path());
    let job_dir = projection
        .as_ref()
        .map(|value| value.1.as_str())
        .unwrap_or("");
    let profile_revision = projection
        .as_ref()
        .map(|value| value.2.as_str())
        .unwrap_or(job.value.profile_revision().as_str());
    let aggregate_json = serde_json::to_string(&job.value).map_err(|error| {
        VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("encode Job aggregate: {error}"),
        )
    })?;
    tx.execute(
        "INSERT INTO jobs (
            id, batch_id, status, source_path, job_dir, profile_revision,
            execution_snapshot_id, aggregate_json, aggregate_version,
            created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1, ?9, ?9)",
        params![
            job.value.id().as_str(),
            job.value.batch_id().map(|id| id.as_str()),
            job_status_name(job.value.status()),
            source_path,
            job_dir,
            profile_revision,
            job.value.execution_snapshot_id().map(|id| id.as_str()),
            aggregate_json,
            now,
        ],
    )
    .map_err(|error| {
        if is_constraint(&error) {
            stale_result("Job", job.value.id().as_str(), ExpectedVersion::New)
        } else {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("insert Job aggregate: {error}"),
            )
        }
    })?;
    Ok(())
}

fn update_job_tx(
    tx: &rusqlite::Transaction<'_>,
    job: &videocaptionerr_core::ports::Versioned<videocaptionerr_domain::Job>,
    expected: ExpectedVersion,
) -> VcResult<()> {
    let ExpectedVersion::Exact(version) = expected else {
        return Ok(());
    };
    let projection = snapshot_projection(tx, job.value.execution_snapshot_id())?;
    let source_path = projection
        .as_ref()
        .map(|value| value.0.as_str())
        .unwrap_or(job.value.source_path());
    let job_dir = projection
        .as_ref()
        .map(|value| value.1.as_str())
        .unwrap_or("");
    let profile_revision = projection
        .as_ref()
        .map(|value| value.2.as_str())
        .unwrap_or(job.value.profile_revision().as_str());
    let aggregate_json = serde_json::to_string(&job.value).map_err(|error| {
        VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("encode Job aggregate: {error}"),
        )
    })?;
    let changed = tx
        .execute(
            "UPDATE jobs SET
                batch_id = ?1, status = ?2, source_path = ?3, job_dir = ?4,
                profile_revision = ?5, execution_snapshot_id = ?6,
                aggregate_json = ?7, aggregate_version = aggregate_version + 1,
                updated_at = ?8
             WHERE id = ?9 AND aggregate_version = ?10",
            params![
                job.value.batch_id().map(|id| id.as_str()),
                job_status_name(job.value.status()),
                source_path,
                job_dir,
                profile_revision,
                job.value.execution_snapshot_id().map(|id| id.as_str()),
                aggregate_json,
                chrono::Utc::now().to_rfc3339(),
                job.value.id().as_str(),
                version as i64,
            ],
        )
        .map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("update Job aggregate: {error}"),
            )
        })?;
    if changed != 1 {
        return Err(stale_result("Job", job.value.id().as_str(), expected));
    }
    Ok(())
}

fn insert_work_unit_tx(
    tx: &rusqlite::Transaction<'_>,
    unit: &videocaptionerr_core::ports::Versioned<videocaptionerr_domain::WorkUnit>,
) -> VcResult<()> {
    let json = serde_json::to_string(&unit.value).map_err(|error| {
        VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("encode WorkUnit: {error}"),
        )
    })?;
    tx.execute(
        "INSERT INTO work_units (
            id, job_id, stage, unit_kind, unit_index, input_hash, status, attempt,
            artifact_id, lease_owner, lease_expires_at, aggregate_json, aggregate_version
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 1)",
        params![
            unit.value.id().as_str(),
            unit.value.job_id().as_str(),
            unit.value.stage().as_str(),
            unit.value.unit_kind(),
            unit.value.unit_index() as i64,
            unit.value.input_hash(),
            work_unit_status_name(unit.value.status()),
            unit.value.attempt() as i64,
            unit.value.artifact().map(|artifact| artifact.id.as_str()),
            unit.value.lease().map(|lease| lease.owner.as_str()),
            unit.value.lease().and_then(|lease| {
                chrono::DateTime::<chrono::Utc>::from_timestamp_millis(lease.expires_at_ms as i64)
                    .map(|value| value.to_rfc3339())
            }),
            json,
        ],
    )
    .map_err(|error| {
        if is_constraint(&error) {
            stale_result("WorkUnit", unit.value.id().as_str(), ExpectedVersion::New)
        } else {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("insert WorkUnit: {error}"),
            )
        }
    })?;
    Ok(())
}

fn update_work_unit_tx(
    tx: &rusqlite::Transaction<'_>,
    unit: &videocaptionerr_core::ports::Versioned<videocaptionerr_domain::WorkUnit>,
    expected: ExpectedVersion,
) -> VcResult<()> {
    let ExpectedVersion::Exact(version) = expected else {
        return Ok(());
    };
    let json = serde_json::to_string(&unit.value).map_err(|error| {
        VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("encode WorkUnit: {error}"),
        )
    })?;
    let changed = tx
        .execute(
            "UPDATE work_units SET
                job_id = ?1, stage = ?2, unit_kind = ?3, unit_index = ?4,
                input_hash = ?5, status = ?6, attempt = ?7, artifact_id = ?8,
                lease_owner = ?9, lease_expires_at = ?10, aggregate_json = ?11,
                finished_at = CASE WHEN ?6 IN ('done', 'failed', 'cancelled')
                                   THEN ?12 ELSE finished_at END,
                aggregate_version = aggregate_version + 1
             WHERE id = ?13 AND aggregate_version = ?14",
            params![
                unit.value.job_id().as_str(),
                unit.value.stage().as_str(),
                unit.value.unit_kind(),
                unit.value.unit_index() as i64,
                unit.value.input_hash(),
                work_unit_status_name(unit.value.status()),
                unit.value.attempt() as i64,
                unit.value.artifact().map(|artifact| artifact.id.as_str()),
                unit.value.lease().map(|lease| lease.owner.as_str()),
                unit.value.lease().and_then(|lease| {
                    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
                        lease.expires_at_ms as i64,
                    )
                    .map(|value| value.to_rfc3339())
                }),
                json,
                chrono::Utc::now().to_rfc3339(),
                unit.value.id().as_str(),
                version as i64,
            ],
        )
        .map_err(|error| VcError::new(ErrorCode::Internal, format!("update WorkUnit: {error}")))?;
    if changed != 1 {
        return Err(stale_result("WorkUnit", unit.value.id().as_str(), expected));
    }
    Ok(())
}

fn sync_stage_projection(
    tx: &rusqlite::Transaction<'_>,
    job: &videocaptionerr_domain::Job,
) -> VcResult<()> {
    for stage in job.stages() {
        tx.execute(
            "INSERT INTO stages (id, job_id, stage, status, attempt)
             VALUES (?1, ?2, ?3, ?4, 0)
             ON CONFLICT(job_id, stage) DO UPDATE SET status = excluded.status",
            params![
                format!("{}:{}", job.id(), stage.kind.as_str()),
                job.id().as_str(),
                stage.kind.as_str(),
                stage_status_name(stage.status),
            ],
        )
        .map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("update stage projection: {error}"),
            )
        })?;
    }
    Ok(())
}

fn insert_outbox_tx(
    tx: &rusqlite::Transaction<'_>,
    event: &videocaptionerr_core::ports::OutboxEvent,
) -> VcResult<()> {
    let sequence: i64 = tx
        .query_row(
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM outbox_events
             WHERE aggregate_type = ?1 AND aggregate_id = ?2",
            params![event.aggregate_type, event.aggregate_id],
            |row| row.get(0),
        )
        .map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("next outbox sequence: {error}"),
            )
        })?;
    let id = UlidStr::from(Ulid::new()).into_string();
    tx.execute(
        "INSERT INTO outbox_events (
            id, aggregate_type, aggregate_id, sequence, event_type,
            payload_json, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            id,
            event.aggregate_type,
            event.aggregate_id,
            sequence,
            event.event_type,
            event.payload_json,
            event.created_at,
        ],
    )
    .map_err(|error| VcError::new(ErrorCode::Internal, format!("insert outbox event: {error}")))?;
    Ok(())
}

fn job_status_name(status: videocaptionerr_domain::JobStatus) -> &'static str {
    match status {
        videocaptionerr_domain::JobStatus::Pending => "pending",
        videocaptionerr_domain::JobStatus::Running => "running",
        videocaptionerr_domain::JobStatus::Done => "done",
        videocaptionerr_domain::JobStatus::DoneDegraded => "done_degraded",
        videocaptionerr_domain::JobStatus::Failed => "failed",
        videocaptionerr_domain::JobStatus::Cancelled => "cancelled",
    }
}

fn stage_status_name(status: videocaptionerr_domain::StageStatus) -> &'static str {
    match status {
        videocaptionerr_domain::StageStatus::Pending => "pending",
        videocaptionerr_domain::StageStatus::WaitingResource => "waiting_resource",
        videocaptionerr_domain::StageStatus::Running => "running",
        videocaptionerr_domain::StageStatus::Retrying => "retrying",
        videocaptionerr_domain::StageStatus::Done => "done",
        videocaptionerr_domain::StageStatus::DoneDegraded => "done_degraded",
        videocaptionerr_domain::StageStatus::Failed => "failed",
        videocaptionerr_domain::StageStatus::Skipped => "skipped",
        videocaptionerr_domain::StageStatus::Cancelled => "cancelled",
        videocaptionerr_domain::StageStatus::WaitingProvider => "waiting_provider",
    }
}

fn work_unit_status_name(status: videocaptionerr_domain::WorkUnitStatus) -> &'static str {
    match status {
        videocaptionerr_domain::WorkUnitStatus::Pending => "pending",
        videocaptionerr_domain::WorkUnitStatus::Running => "running",
        videocaptionerr_domain::WorkUnitStatus::Done => "done",
        videocaptionerr_domain::WorkUnitStatus::Failed => "failed",
        videocaptionerr_domain::WorkUnitStatus::Cancelled => "cancelled",
    }
}

fn stage_rank(stage: &str) -> u8 {
    match stage {
        "probe" => 0,
        "extract_audio" => 1,
        "asr" => 2,
        "split" => 3,
        "correct" => 4,
        "translate" => 5,
        "export" => 6,
        _ => u8::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::atomic_write_bytes;
    use tempfile::tempdir;
    use videocaptionerr_core::ports::{
        ArtifactSource, OutboxEvent, PreparedArtifact, StageCommitRequest,
    };

    fn stage_commit_fixture(
        root: &Path,
    ) -> (
        Store,
        videocaptionerr_domain::JobId,
        PathBuf,
        PathBuf,
        videocaptionerr_core::ports::StageCommitRequest,
    ) {
        let jobs_root = root.join("jobs");
        let job_dir = jobs_root.join("job1");
        fs::create_dir_all(&job_dir).unwrap();
        let store = Store::open(&root.join("state.db")).unwrap();
        let job_id: videocaptionerr_domain::JobId = Ulid::new().into();
        let mut job = videocaptionerr_domain::Job::new(
            job_id.clone(),
            None,
            Ulid::new().into(),
            "/media/a.mp4",
        );
        let job_json = serde_json::to_string(&job).unwrap();
        store
            .save_job_aggregate(
                job_id.as_str(),
                None,
                "pending",
                "/media/a.mp4",
                job.profile_revision().as_str(),
                None,
                &job_json,
                ExpectedVersion::New,
            )
            .unwrap();
        job.start().unwrap();
        job.start_stage(videocaptionerr_domain::StageKind::Probe)
            .unwrap();

        let mut unit = videocaptionerr_domain::WorkUnit::new(
            Ulid::new().into(),
            job_id.clone(),
            videocaptionerr_domain::StageKind::Probe,
            "probe-unit",
            0,
            "probe-input",
        )
        .unwrap();
        unit.lease_for("test-owner", 1_000, 2_000).unwrap();
        let lease = unit.lease().unwrap();
        store
            .save_work_unit_aggregate(
                &WorkUnitRecord {
                    id: unit.id().to_string(),
                    job_id: unit.job_id().to_string(),
                    stage: "probe".into(),
                    unit_kind: unit.unit_kind().into(),
                    unit_index: unit.unit_index(),
                    input_hash: unit.input_hash().into(),
                    status: "running".into(),
                    attempt: unit.attempt(),
                    lease_owner: Some(lease.owner.clone()),
                    lease_expires_at: Some("1970-01-01T00:00:02Z".into()),
                    artifact_id: None,
                    aggregate_json: serde_json::to_string(&unit).unwrap(),
                },
                ExpectedVersion::New,
            )
            .unwrap();

        let final_path = job_dir.join("probe.json");
        let bytes = b"probe".to_vec();
        let artifact = videocaptionerr_domain::ArtifactRef {
            id: Ulid::new().into(),
            stage: videocaptionerr_domain::StageKind::Probe,
            path: final_path.to_string_lossy().into_owned(),
            content_hash: blake3::hash(&bytes).to_hex().to_string(),
            schema_version: videocaptionerr_domain::SCHEMA_VERSION,
            producer_fingerprint: "test".into(),
        };
        job.complete_stage(
            videocaptionerr_domain::StageKind::Probe,
            artifact.clone(),
            false,
        )
        .unwrap();
        unit.complete(artifact.clone()).unwrap();
        let request = StageCommitRequest {
            job: Some((
                videocaptionerr_core::ports::Versioned::with_version(job, 1),
                ExpectedVersion::Exact(1),
            )),
            work_unit: Some((
                videocaptionerr_core::ports::Versioned::with_version(unit, 1),
                ExpectedVersion::Exact(1),
            )),
            artifact: Some(PreparedArtifact {
                job_id: job_id.clone(),
                artifact,
                source: ArtifactSource::Bytes { bytes },
            }),
            event: Some(OutboxEvent {
                aggregate_type: "Job".into(),
                aggregate_id: job_id.to_string(),
                event_type: "probe_committed".into(),
                payload_json: "{}".into(),
                created_at: chrono::Utc::now().to_rfc3339(),
            }),
        };
        (store, job_id, jobs_root, final_path, request)
    }

    #[test]
    fn injected_stage_commit_faults_converge_after_recovery() {
        let points = [
            StageCommitFaultPoint::BeforeTempWrite,
            StageCommitFaultPoint::AfterTempWrite,
            StageCommitFaultPoint::AfterRename,
            StageCommitFaultPoint::AfterArtifactInsert,
            StageCommitFaultPoint::AfterWorkUnitUpdate,
            StageCommitFaultPoint::AfterJobUpdate,
            StageCommitFaultPoint::AfterOutboxInsert,
            StageCommitFaultPoint::AfterDbCommit,
        ];
        for point in points {
            let dir = tempdir().unwrap();
            let (mut store, job_id, jobs_root, final_path, request) =
                stage_commit_fixture(dir.path());
            store.inject_stage_commit_fault(point);
            assert!(
                store.commit_stage(request).is_err(),
                "fault point: {point:?}"
            );
            let report = store
                .recover_artifacts(std::slice::from_ref(&jobs_root))
                .unwrap();
            let artifact_count: i64 = store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM artifacts WHERE committed = 1",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            if point == StageCommitFaultPoint::AfterDbCommit {
                assert_eq!(artifact_count, 1, "fault point: {point:?}");
                assert!(final_path.is_file(), "fault point: {point:?}");
                assert!(report.partial_files.is_empty(), "fault point: {point:?}");
                assert!(report.orphan_files.is_empty(), "fault point: {point:?}");
            } else {
                assert_eq!(artifact_count, 0, "fault point: {point:?}");
                assert!(!final_path.exists(), "fault point: {point:?}");
                if point == StageCommitFaultPoint::AfterTempWrite {
                    assert_eq!(report.partial_files.len(), 1, "fault point: {point:?}");
                } else if point == StageCommitFaultPoint::AfterRename
                    || point == StageCommitFaultPoint::AfterArtifactInsert
                    || point == StageCommitFaultPoint::AfterWorkUnitUpdate
                    || point == StageCommitFaultPoint::AfterJobUpdate
                    || point == StageCommitFaultPoint::AfterOutboxInsert
                {
                    assert_eq!(report.orphan_files.len(), 1, "fault point: {point:?}");
                } else {
                    assert!(report.partial_files.is_empty(), "fault point: {point:?}");
                    assert!(report.orphan_files.is_empty(), "fault point: {point:?}");
                }
            }
            let _ = store.load_job_aggregate(job_id.as_str()).unwrap();
        }
    }

    #[test]
    fn job_and_artifact_commit() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("t.db");
        let mut store = Store::open(&db).unwrap();

        store
            .insert_job("job1", None, "/media/a.mp4", "/jobs/job1", "pending")
            .unwrap();
        assert_eq!(
            store.get_job_status("job1").unwrap().as_deref(),
            Some("pending")
        );

        store
            .insert_work_unit(
                "wu1",
                "job1",
                "asr",
                "chunk",
                0,
                "hash0",
                WorkUnitStatus::Running,
            )
            .unwrap();

        let art_path = dir.path().join("transcript.json");
        let hash = atomic_write_bytes(&art_path, br#"{"ok":1}"#).unwrap();
        let meta = Store::new_artifact_meta(
            "job1",
            "asr",
            ArtifactKind::Transcript,
            art_path.to_str().unwrap(),
            &hash,
            "test@0.1.0",
        );
        store.commit_artifact_and_unit(&meta, Some("wu1")).unwrap();

        assert_eq!(
            store.get_work_unit_status("wu1").unwrap(),
            Some(WorkUnitStatus::Done)
        );
    }

    #[test]
    fn artifact_commit_updates_domain_work_unit_and_control_columns_together() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("aggregate-commit.db");
        let mut store = Store::open(&db).unwrap();
        let job_id: videocaptionerr_domain::JobId = Ulid::new().into();
        let unit_id: videocaptionerr_domain::WorkUnitId = Ulid::new().into();

        store
            .insert_job(
                job_id.as_str(),
                None,
                "/media/a.mp4",
                "/jobs/job1",
                "running",
            )
            .unwrap();

        let mut unit = videocaptionerr_domain::WorkUnit::new(
            unit_id.clone(),
            job_id.clone(),
            videocaptionerr_domain::StageKind::Asr,
            "chunk",
            0,
            "chunk-input-hash",
        )
        .unwrap();
        unit.lease_for("test-owner", 1_000, 2_000).unwrap();
        let lease = unit.lease().unwrap();
        store
            .save_work_unit_aggregate(
                &WorkUnitRecord {
                    id: unit.id().to_string(),
                    job_id: unit.job_id().to_string(),
                    stage: "asr".into(),
                    unit_kind: unit.unit_kind().into(),
                    unit_index: unit.unit_index(),
                    input_hash: unit.input_hash().into(),
                    status: "running".into(),
                    attempt: unit.attempt(),
                    lease_owner: Some(lease.owner.clone()),
                    lease_expires_at: Some("1970-01-01T00:00:02Z".into()),
                    artifact_id: None,
                    aggregate_json: serde_json::to_string(&unit).unwrap(),
                },
                ExpectedVersion::New,
            )
            .unwrap();

        let artifact_path = dir.path().join("chunk.json");
        let hash = atomic_write_bytes(&artifact_path, br#"{"chunk":0}"#).unwrap();
        let meta = Store::new_artifact_meta(
            job_id.as_str(),
            "asr",
            ArtifactKind::Transcript,
            artifact_path.to_str().unwrap(),
            &hash,
            "test@0.1.0",
        );
        store
            .commit_artifact_and_unit(&meta, Some(unit_id.as_str()))
            .unwrap();

        let (aggregate_json, _version) = store
            .load_work_unit_aggregate(unit_id.as_str())
            .unwrap()
            .unwrap();
        let completed: videocaptionerr_domain::WorkUnit =
            serde_json::from_str(&aggregate_json).unwrap();
        assert_eq!(
            completed.status(),
            videocaptionerr_domain::WorkUnitStatus::Done
        );
        assert_eq!(completed.artifact().unwrap().id.as_str(), meta.id.as_str());
        assert!(completed.lease().is_none());

        let (status, artifact_id, persisted_json): (String, Option<String>, String) = store
            .conn
            .query_row(
                "SELECT status, artifact_id, aggregate_json FROM work_units WHERE id = ?1",
                [unit_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(status, "done");
        assert_eq!(artifact_id.as_deref(), Some(meta.id.as_str()));
        assert_eq!(persisted_json, aggregate_json);
    }

    #[test]
    fn lease_recovery() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("t.db");
        let store = Store::open(&db).unwrap();
        store
            .insert_job("job1", None, "/a", "/j", "running")
            .unwrap();
        store
            .insert_work_unit(
                "wu1",
                "job1",
                "asr",
                "chunk",
                0,
                "h",
                WorkUnitStatus::Pending,
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE work_units SET status='running', lease_owner='cli',
                 lease_expires_at='2020-01-01T00:00:00Z', attempt=1 WHERE id='wu1'",
                [],
            )
            .unwrap();

        let n = store
            .recover_expired_leases("2026-01-01T00:00:00Z")
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(
            store.get_work_unit_status("wu1").unwrap(),
            Some(WorkUnitStatus::Pending)
        );
        let attempt: i64 = store
            .conn
            .query_row("SELECT attempt FROM work_units WHERE id='wu1'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(attempt, 2);
    }

    #[test]
    fn atomic_stage_commit_persists_artifact_job_stage_and_outbox_together() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("stage-commit.db");
        let mut store = Store::open(&db).unwrap();
        let job_id: videocaptionerr_domain::JobId = Ulid::new().into();
        let profile_revision: videocaptionerr_domain::UlidStr = Ulid::new().into();
        let mut initial = videocaptionerr_domain::Job::new(
            job_id.clone(),
            None,
            profile_revision,
            "/media/a.mp4",
        );
        let initial_json = serde_json::to_string(&initial).unwrap();
        store
            .save_job_aggregate(
                job_id.as_str(),
                None,
                "pending",
                "/media/a.mp4",
                initial.profile_revision().as_str(),
                None,
                &initial_json,
                ExpectedVersion::New,
            )
            .unwrap();
        initial.start().unwrap();
        initial
            .start_stage(videocaptionerr_domain::StageKind::Probe)
            .unwrap();
        let artifact_id: videocaptionerr_domain::UlidStr = Ulid::new().into();
        let final_path = dir.path().join("probe.json");
        let bytes = br#"{"duration_ms":1}"#.to_vec();
        let artifact = videocaptionerr_domain::ArtifactRef {
            id: artifact_id,
            stage: videocaptionerr_domain::StageKind::Probe,
            path: final_path.to_string_lossy().into_owned(),
            content_hash: blake3::hash(&bytes).to_hex().to_string(),
            schema_version: videocaptionerr_domain::SCHEMA_VERSION,
            producer_fingerprint: "test".into(),
        };
        initial
            .complete_stage(
                videocaptionerr_domain::StageKind::Probe,
                artifact.clone(),
                false,
            )
            .unwrap();
        let result = store
            .commit_stage(videocaptionerr_core::ports::StageCommitRequest {
                job: Some((
                    videocaptionerr_core::ports::Versioned::with_version(initial, 1),
                    ExpectedVersion::Exact(1),
                )),
                work_unit: None,
                artifact: Some(videocaptionerr_core::ports::PreparedArtifact {
                    job_id: job_id.clone(),
                    artifact: artifact.clone(),
                    source: videocaptionerr_core::ports::ArtifactSource::Bytes { bytes },
                }),
                event: Some(videocaptionerr_core::ports::OutboxEvent {
                    aggregate_type: "Job".into(),
                    aggregate_id: job_id.to_string(),
                    event_type: "probe_committed".into(),
                    payload_json: "{}".into(),
                    created_at: chrono::Utc::now().to_rfc3339(),
                }),
            })
            .unwrap();

        assert_eq!(result.job.unwrap().version, 2);
        assert!(final_path.is_file());
        let (job_json, version) = store.load_job_aggregate(job_id.as_str()).unwrap().unwrap();
        assert_eq!(version, 2);
        let job: videocaptionerr_domain::Job = serde_json::from_str(&job_json).unwrap();
        assert_eq!(
            job.stages()[0].status,
            videocaptionerr_domain::StageStatus::Done
        );
        assert_eq!(job.stages()[0].artifact.as_ref(), Some(&artifact));
        let artifact_count: i64 = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM artifacts WHERE id = ?1",
                [artifact.id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(artifact_count, 1);
        let events = store.list_pending_outbox(10).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "probe_committed");
    }

    #[test]
    fn atomic_work_unit_commit_persists_done_status_and_artifact_reference() {
        let dir = tempdir().unwrap();
        let (mut store, _job_id, jobs_root, final_path, request) = stage_commit_fixture(dir.path());
        let unit_id = request
            .work_unit
            .as_ref()
            .expect("fixture includes a WorkUnit")
            .0
            .id()
            .clone();
        let artifact = request
            .artifact
            .as_ref()
            .expect("fixture includes an artifact")
            .artifact
            .clone();

        let result = store.commit_stage(request).unwrap();

        let committed_unit = result.work_unit.expect("WorkUnit result");
        assert_eq!(
            committed_unit.status(),
            videocaptionerr_domain::WorkUnitStatus::Done
        );
        assert_eq!(committed_unit.artifact(), Some(&artifact));
        assert!(committed_unit.lease().is_none());
        let (body, _) = store
            .load_work_unit_aggregate(unit_id.as_str())
            .unwrap()
            .unwrap();
        let persisted: videocaptionerr_domain::WorkUnit = serde_json::from_str(&body).unwrap();
        assert_eq!(
            persisted.status(),
            videocaptionerr_domain::WorkUnitStatus::Done
        );
        assert_eq!(persisted.artifact(), Some(&artifact));
        assert!(final_path.is_file());
        let report = store
            .recover_artifacts(std::slice::from_ref(&jobs_root))
            .unwrap();
        assert!(report.corrupt_artifacts.is_empty());
        assert!(report.orphan_files.is_empty());
    }

    #[test]
    fn stale_stage_commit_rolls_back_artifact_metadata_and_file() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("stale-stage.db");
        let mut store = Store::open(&db).unwrap();
        let job_id: videocaptionerr_domain::JobId = Ulid::new().into();
        let profile_revision: videocaptionerr_domain::UlidStr = Ulid::new().into();
        let job = videocaptionerr_domain::Job::new(
            job_id.clone(),
            None,
            profile_revision,
            "/media/a.mp4",
        );
        let json = serde_json::to_string(&job).unwrap();
        store
            .save_job_aggregate(
                job_id.as_str(),
                None,
                "pending",
                "/media/a.mp4",
                job.profile_revision().as_str(),
                None,
                &json,
                ExpectedVersion::New,
            )
            .unwrap();
        let bytes = b"stale".to_vec();
        let artifact_id: videocaptionerr_domain::UlidStr = Ulid::new().into();
        let final_path = dir.path().join("stale.json");
        let artifact = videocaptionerr_domain::ArtifactRef {
            id: artifact_id,
            stage: videocaptionerr_domain::StageKind::Probe,
            path: final_path.to_string_lossy().into_owned(),
            content_hash: blake3::hash(&bytes).to_hex().to_string(),
            schema_version: videocaptionerr_domain::SCHEMA_VERSION,
            producer_fingerprint: "test".into(),
        };
        let error = store
            .commit_stage(videocaptionerr_core::ports::StageCommitRequest {
                job: Some((
                    videocaptionerr_core::ports::Versioned::with_version(job, 0),
                    ExpectedVersion::Exact(0),
                )),
                work_unit: None,
                artifact: Some(videocaptionerr_core::ports::PreparedArtifact {
                    job_id: job_id.clone(),
                    artifact,
                    source: videocaptionerr_core::ports::ArtifactSource::Bytes { bytes },
                }),
                event: None,
            })
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::StaleResult);
        assert!(!final_path.exists());
        let artifact_count: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM artifacts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(artifact_count, 0);
    }

    #[test]
    fn stale_work_unit_stage_commit_rolls_back_artifact_metadata() {
        let dir = tempdir().unwrap();
        let (mut store, _job_id, _jobs_root, final_path, mut request) =
            stage_commit_fixture(dir.path());
        let unit_id = request
            .work_unit
            .as_ref()
            .expect("fixture includes a WorkUnit")
            .0
            .id()
            .clone();
        request.work_unit.as_mut().unwrap().1 = ExpectedVersion::Exact(0);
        let error = store.commit_stage(request).unwrap_err();
        assert_eq!(error.code, ErrorCode::StaleResult);
        assert!(!final_path.exists());
        let artifact_count: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM artifacts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(artifact_count, 0);
        let (unit_json, _) = store
            .load_work_unit_aggregate(unit_id.as_str())
            .unwrap()
            .unwrap();
        let unit: videocaptionerr_domain::WorkUnit = serde_json::from_str(&unit_json).unwrap();
        assert_eq!(
            unit.status(),
            videocaptionerr_domain::WorkUnitStatus::Running
        );
        assert!(unit.artifact().is_none());
    }

    #[test]
    fn startup_recovery_quarantines_orphans_and_invalidates_corrupt_stage() {
        let dir = tempdir().unwrap();
        let jobs_root = dir.path().join("jobs");
        let job_dir = jobs_root.join("job1");
        fs::create_dir_all(&job_dir).unwrap();
        let db = dir.path().join("recovery.db");
        let mut store = Store::open(&db).unwrap();
        let job_id: videocaptionerr_domain::JobId = Ulid::new().into();
        let profile_revision: videocaptionerr_domain::UlidStr = Ulid::new().into();
        let mut job = videocaptionerr_domain::Job::new(
            job_id.clone(),
            None,
            profile_revision,
            "/media/a.mp4",
        );
        let json = serde_json::to_string(&job).unwrap();
        store
            .save_job_aggregate(
                job_id.as_str(),
                None,
                "pending",
                "/media/a.mp4",
                job.profile_revision().as_str(),
                None,
                &json,
                ExpectedVersion::New,
            )
            .unwrap();
        job.start().unwrap();
        job.start_stage(videocaptionerr_domain::StageKind::Probe)
            .unwrap();
        let path = job_dir.join("probe.json");
        let bytes = b"probe".to_vec();
        let artifact = videocaptionerr_domain::ArtifactRef {
            id: Ulid::new().into(),
            stage: videocaptionerr_domain::StageKind::Probe,
            path: path.to_string_lossy().into_owned(),
            content_hash: blake3::hash(&bytes).to_hex().to_string(),
            schema_version: videocaptionerr_domain::SCHEMA_VERSION,
            producer_fingerprint: "test".into(),
        };
        job.complete_stage(
            videocaptionerr_domain::StageKind::Probe,
            artifact.clone(),
            false,
        )
        .unwrap();
        store
            .commit_stage(videocaptionerr_core::ports::StageCommitRequest {
                job: Some((
                    videocaptionerr_core::ports::Versioned::with_version(job, 1),
                    ExpectedVersion::Exact(1),
                )),
                work_unit: None,
                artifact: Some(videocaptionerr_core::ports::PreparedArtifact {
                    job_id: job_id.clone(),
                    artifact,
                    source: videocaptionerr_core::ports::ArtifactSource::Bytes { bytes },
                }),
                event: None,
            })
            .unwrap();
        fs::remove_file(&path).unwrap();
        let orphan = job_dir.join("orphan.bin");
        fs::write(&orphan, b"orphan").unwrap();

        let report = store
            .recover_artifacts(std::slice::from_ref(&jobs_root))
            .unwrap();
        assert_eq!(report.corrupt_artifacts.len(), 1);
        assert!(report.orphan_files.iter().any(|value| value == &orphan));
        assert!(!orphan.exists());
        assert!(jobs_root.join(".recovery-quarantine").is_dir());
        let (recovered_json, _) = store.load_job_aggregate(job_id.as_str()).unwrap().unwrap();
        let recovered: videocaptionerr_domain::Job = serde_json::from_str(&recovered_json).unwrap();
        assert_eq!(
            recovered.status(),
            videocaptionerr_domain::JobStatus::Pending
        );
        assert_eq!(
            recovered.stages()[0].status,
            videocaptionerr_domain::StageStatus::Pending
        );
        let committed: i64 = store
            .conn
            .query_row("SELECT committed FROM artifacts LIMIT 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(committed, 0);
    }

    #[test]
    fn v4_database_migrates_to_execution_snapshots_and_aggregate_versions() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("v4.db");
        let connection = Connection::open(&db).unwrap();
        for migration in crate::migrate::MIGRATIONS.iter().take(4) {
            connection.execute_batch(migration.sql).unwrap();
            connection
                .execute(
                    "INSERT INTO schema_migrations (version, name, checksum, applied_at)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        migration.version,
                        migration.name,
                        migration.checksum(),
                        "2026-07-20T00:00:00Z"
                    ],
                )
                .unwrap();
        }
        drop(connection);

        let store = Store::open(&db).unwrap();
        let version: i64 = store
            .conn
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(version, 6);

        let columns = |table: &str| {
            let mut statement = store
                .conn
                .prepare(&format!("PRAGMA table_info({table})"))
                .unwrap();
            statement
                .query_map([], |row| row.get::<_, String>(1))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        assert!(columns("jobs")
            .iter()
            .any(|column| column == "aggregate_version"));
        assert!(columns("jobs")
            .iter()
            .any(|column| column == "execution_snapshot_id"));
        assert!(columns("batches")
            .iter()
            .any(|column| column == "aggregate_version"));
        assert!(columns("work_units")
            .iter()
            .any(|column| column == "aggregate_version"));

        let snapshot_table: String = store
            .conn
            .query_row(
                "SELECT name FROM sqlite_master
                 WHERE type = 'table' AND name = 'execution_snapshots'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(snapshot_table, "execution_snapshots");
        let outbox_table: String = store
            .conn
            .query_row(
                "SELECT name FROM sqlite_master
                 WHERE type = 'table' AND name = 'outbox_events'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(outbox_table, "outbox_events");
    }

    #[tokio::test]
    async fn store_handle_uses_typed_actor_commands() {
        let dir = tempdir().unwrap();
        let handle = StoreHandle::open(&dir.path().join("actor.db")).unwrap();

        handle
            .save_job_aggregate(
                "job1",
                None,
                "pending",
                "/media/a.wav",
                "",
                None,
                "{\"id\":1}",
                ExpectedVersion::New,
            )
            .await
            .unwrap();
        handle
            .save_batch_aggregate(
                "batch1",
                "pending",
                "fake",
                "cpu",
                "{\"id\":1}",
                ExpectedVersion::New,
            )
            .await
            .unwrap();

        assert_eq!(
            handle.load_job_aggregate("job1").await.unwrap(),
            Some(("{\"id\":1}".into(), 1))
        );
        assert_eq!(
            handle.load_batch_aggregate("batch1").await.unwrap(),
            Some(("{\"id\":1}".into(), 1))
        );
        assert_eq!(handle.list_job_aggregates().await.unwrap().len(), 1);

        let probe = CapabilityProbeRecord {
            id: "probe-1".into(),
            provider_profile_id: "primary".into(),
            model: "model-a".into(),
            probe_hash: "hash-a".into(),
            result_json: r#"{"ok":true}"#.into(),
            created_at: "2026-01-01T00:00:00Z".into(),
            expires_at: None,
        };
        handle.save_capability_probe(probe).await.unwrap();
        assert_eq!(
            handle
                .load_capability_probe("primary", "model-a", "hash-a")
                .await
                .unwrap(),
            Some(r#"{"ok":true}"#.into())
        );

        handle.delete_job_record("job1").await.unwrap();
        assert!(handle.load_job_aggregate("job1").await.unwrap().is_none());
    }

    #[test]
    fn synchronous_probe_load_is_available_before_async_runtime_startup() {
        let dir = tempdir().unwrap();
        let handle = StoreHandle::open(&dir.path().join("sync-probe.db")).unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime
            .block_on(handle.save_capability_probe(CapabilityProbeRecord {
                id: "probe-1".into(),
                provider_profile_id: "primary".into(),
                model: "model-a".into(),
                probe_hash: "hash-a".into(),
                result_json: r#"{"ok":true}"#.into(),
                created_at: "2026-01-01T00:00:00Z".into(),
                expires_at: None,
            }))
            .unwrap();
        drop(runtime);

        assert_eq!(
            handle
                .load_capability_probe_sync("primary", "model-a", "hash-a")
                .unwrap(),
            Some(r#"{"ok":true}"#.into())
        );
    }

    #[test]
    fn synchronous_probe_load_rejects_runtime_thread_without_blocking() {
        let dir = tempdir().unwrap();
        let handle = StoreHandle::open(&dir.path().join("sync-probe-runtime.db")).unwrap();
        let error = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { handle.load_capability_probe_sync("primary", "model-a", "hash-a") })
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidConfig);
    }
}
