//! Async handle for the single-writer SQLite actor.

use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Sender};
use std::thread;

use tokio::sync::oneshot;
use videocaptionerr_contracts::artifact::ArtifactMeta;
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::execution_snapshot::JobExecutionSnapshot;
use videocaptionerr_core::ports::{
    ArtifactRecoveryReport, BatchCreationRequest, CapabilityProbeRecord, CreatedBatchGraph,
    ExpectedVersion, RetryTransactionRequest, RetryTransactionResult, StageCommitRequest,
    StageCommitResult, StoredOutboxEvent,
};

use super::command::store_actor;
use super::command::StoreCommand;

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

    pub(crate) async fn create_batch_graph(
        &self,
        request: BatchCreationRequest,
    ) -> VcResult<CreatedBatchGraph> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::CreateBatchGraph {
            request: Box::new(request),
            reply,
        })?;
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

    pub(crate) async fn apply_retry(
        &self,
        request: RetryTransactionRequest,
    ) -> VcResult<RetryTransactionResult> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::ApplyRetry {
            request: Box::new(request),
            reply,
        })?;
        await_response(result).await
    }

    pub(crate) async fn list_work_units_for_job(
        &self,
        job_id: &str,
    ) -> VcResult<Vec<(String, u64)>> {
        let (reply, result) = response_channel();
        self.send(StoreCommand::ListWorkUnitsForJob {
            job_id: job_id.into(),
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

pub(super) type StoreResponse<T> = oneshot::Sender<VcResult<T>>;

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
