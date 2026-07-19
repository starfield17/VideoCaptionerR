//! Single-writer store actor over SQLite.

use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;

use rusqlite::{params, Connection, OptionalExtension};
use tokio::sync::oneshot;
use ulid::Ulid;
use videocaptionerr_contracts::artifact::{ArtifactKind, ArtifactMeta};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_contracts::ids::UlidStr;
use videocaptionerr_core::ports::CapabilityProbeRecord;

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

    pub(crate) async fn save_job_aggregate(
        &self,
        id: &str,
        batch_id: Option<&str>,
        status: &str,
        source_path: &str,
        aggregate_json: &str,
    ) -> VcResult<()> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::SaveJobAggregate {
            id: id.into(),
            batch_id: batch_id.map(str::to_owned),
            status: status.into(),
            source_path: source_path.into(),
            aggregate_json: aggregate_json.into(),
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn load_job_aggregate(&self, id: &str) -> VcResult<Option<String>> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::LoadJobAggregate {
            id: id.into(),
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn list_job_aggregates(&self) -> VcResult<Vec<String>> {
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
    ) -> VcResult<()> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::SaveBatchAggregate {
            id: id.into(),
            status: status.into(),
            asr_model: asr_model.into(),
            device: device.into(),
            aggregate_json: aggregate_json.into(),
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn load_batch_aggregate(&self, id: &str) -> VcResult<Option<String>> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::LoadBatchAggregate {
            id: id.into(),
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn save_work_unit_aggregate(&self, record: WorkUnitRecord) -> VcResult<()> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::SaveWorkUnitAggregate { record, reply })?;
        await_response(result).await
    }

    pub(crate) async fn load_work_unit_aggregate(&self, id: &str) -> VcResult<Option<String>> {
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
    ) -> VcResult<Option<String>> {
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
    ) -> VcResult<Vec<String>> {
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
    ) -> VcResult<Option<String>> {
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
        aggregate_json: String,
        reply: StoreResponse<()>,
    },
    LoadJobAggregate {
        id: String,
        reply: StoreResponse<Option<String>>,
    },
    ListJobAggregates {
        reply: StoreResponse<Vec<String>>,
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
        reply: StoreResponse<()>,
    },
    LoadBatchAggregate {
        id: String,
        reply: StoreResponse<Option<String>>,
    },
    SaveWorkUnitAggregate {
        record: WorkUnitRecord,
        reply: StoreResponse<()>,
    },
    LoadWorkUnitAggregate {
        id: String,
        reply: StoreResponse<Option<String>>,
    },
    FindWorkUnitAggregate {
        job_id: String,
        stage: String,
        unit_kind: String,
        unit_index: u32,
        input_hash: String,
        reply: StoreResponse<Option<String>>,
    },
    ListExpiredWorkUnitAggregates {
        now_rfc3339: String,
        reply: StoreResponse<Vec<String>>,
    },
    LeaseNextReady {
        request: LeaseRequest,
        reply: StoreResponse<Option<String>>,
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
                aggregate_json,
                reply,
            } => {
                let result = store.save_job_aggregate(
                    &id,
                    batch_id.as_deref(),
                    &status,
                    &source_path,
                    &aggregate_json,
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
                reply,
            } => {
                let result =
                    store.save_batch_aggregate(&id, &status, &asr_model, &device, &aggregate_json);
                let _ = reply.send(result);
            }
            Self::LoadBatchAggregate { id, reply } => {
                let _ = reply.send(store.load_batch_aggregate(&id));
            }
            Self::SaveWorkUnitAggregate { record, reply } => {
                let result = store.save_work_unit_aggregate(&record);
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
        }
    }
}

/// SQLite-backed control plane.
pub struct Store {
    conn: Connection,
    path: PathBuf,
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
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn save_job_aggregate(
        &self,
        id: &str,
        batch_id: Option<&str>,
        status: &str,
        source_path: &str,
        aggregate_json: &str,
    ) -> VcResult<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn
            .execute(
                "INSERT INTO jobs (
                    id, batch_id, status, source_path, job_dir, aggregate_json,
                    created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
                 ON CONFLICT(id) DO UPDATE SET
                    batch_id = excluded.batch_id,
                    status = excluded.status,
                    source_path = excluded.source_path,
                    aggregate_json = excluded.aggregate_json,
                    updated_at = excluded.updated_at",
                params![id, batch_id, status, source_path, "", aggregate_json, now],
            )
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("save job aggregate: {e}")))?;
        Ok(())
    }

    pub(crate) fn save_batch_aggregate(
        &self,
        id: &str,
        status: &str,
        asr_model: &str,
        device: &str,
        aggregate_json: &str,
    ) -> VcResult<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn
            .execute(
                "INSERT INTO batches (
                    id, status, asr_model_id, asr_device, aggregate_json,
                    created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
                 ON CONFLICT(id) DO UPDATE SET
                    status = excluded.status,
                    asr_model_id = excluded.asr_model_id,
                    asr_device = excluded.asr_device,
                    aggregate_json = excluded.aggregate_json,
                    updated_at = excluded.updated_at",
                params![id, status, asr_model, device, aggregate_json, now],
            )
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("save batch aggregate: {e}")))?;
        Ok(())
    }

    pub(crate) fn save_work_unit_aggregate(&self, record: &WorkUnitRecord) -> VcResult<()> {
        self.conn
            .execute(
                "INSERT INTO work_units (
                    id, job_id, stage, unit_kind, unit_index, input_hash, status, attempt,
                    artifact_id, lease_owner, lease_expires_at, aggregate_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                 ON CONFLICT(id) DO UPDATE SET
                    job_id = excluded.job_id,
                    stage = excluded.stage,
                    unit_kind = excluded.unit_kind,
                    unit_index = excluded.unit_index,
                    input_hash = excluded.input_hash,
                    status = excluded.status,
                    attempt = excluded.attempt,
                    artifact_id = excluded.artifact_id,
                    lease_owner = excluded.lease_owner,
                    lease_expires_at = excluded.lease_expires_at,
                    aggregate_json = excluded.aggregate_json",
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
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("save work unit: {e}")))?;
        Ok(())
    }

    pub(crate) fn load_batch_aggregate(&self, id: &str) -> VcResult<Option<String>> {
        self.conn
            .query_row(
                "SELECT aggregate_json FROM batches WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("load batch aggregate: {e}")))
    }

    pub(crate) fn load_work_unit_aggregate(&self, id: &str) -> VcResult<Option<String>> {
        self.conn
            .query_row(
                "SELECT aggregate_json FROM work_units WHERE id = ?1",
                [id],
                |row| row.get(0),
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
    ) -> VcResult<Option<String>> {
        self.conn
            .query_row(
                "SELECT aggregate_json FROM work_units
                 WHERE job_id = ?1 AND stage = ?2 AND unit_kind = ?3
                   AND unit_index = ?4 AND input_hash = ?5
                 ORDER BY id LIMIT 1",
                params![job_id, stage, unit_kind, unit_index as i64, input_hash],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("find work unit: {e}")))
    }

    pub(crate) fn list_expired_work_unit_aggregates(
        &self,
        now_rfc3339: &str,
    ) -> VcResult<Vec<String>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT aggregate_json FROM work_units
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
            .query_map([now_rfc3339], |row| row.get::<_, String>(0))
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("query expired units: {e}")))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("read expired units: {e}")))
    }

    pub(crate) fn load_job_aggregate(&self, id: &str) -> VcResult<Option<String>> {
        self.conn
            .query_row(
                "SELECT aggregate_json FROM jobs WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("load job aggregate: {e}")))
    }

    pub(crate) fn list_job_aggregates(&self) -> VcResult<Vec<String>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT aggregate_json FROM jobs
                 WHERE aggregate_json IS NOT NULL
                 ORDER BY created_at, id",
            )
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("list job aggregates: {e}")))?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
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

    /// Atomically claim the oldest pending unit and persist its domain lease.
    pub(crate) fn lease_next_ready(&mut self, request: &LeaseRequest) -> VcResult<Option<String>> {
        let tx = self.conn.unchecked_transaction().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("begin lease transaction: {error}"),
            )
        })?;
        let selected: Option<(String, String)> = tx
            .query_row(
                "SELECT id, aggregate_json FROM work_units
                 WHERE job_id = ?1 AND stage = ?2 AND status = 'pending'
                   AND aggregate_json IS NOT NULL
                 ORDER BY unit_index, id LIMIT 1",
                params![request.job_id, request.stage],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("select ready work unit: {error}"),
                )
            })?;
        let Some((id, aggregate_json)) = selected else {
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
                 aggregate_json = ?5 WHERE id = ?6 AND status = 'pending'",
                params![
                    unit.attempt() as i64,
                    request.owner,
                    request.expires_rfc3339,
                    request.now_rfc3339,
                    updated_json,
                    id
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
        Ok(Some(updated_json))
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
                 finished_at = NULL, aggregate_json = ?2 WHERE id = ?3",
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
                         lease_owner = NULL, lease_expires_at = NULL
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
                     lease_owner = NULL, lease_expires_at = NULL, aggregate_json = ?5
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

    #[tokio::test]
    async fn store_handle_uses_typed_actor_commands() {
        let dir = tempdir().unwrap();
        let handle = StoreHandle::open(&dir.path().join("actor.db")).unwrap();

        handle
            .save_job_aggregate("job1", None, "pending", "/media/a.wav", "{\"id\":1}")
            .await
            .unwrap();
        handle
            .save_batch_aggregate("batch1", "pending", "fake", "cpu", "{\"id\":1}")
            .await
            .unwrap();

        assert_eq!(
            handle.load_job_aggregate("job1").await.unwrap(),
            Some("{\"id\":1}".into())
        );
        assert_eq!(
            handle.load_batch_aggregate("batch1").await.unwrap(),
            Some("{\"id\":1}".into())
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
}
