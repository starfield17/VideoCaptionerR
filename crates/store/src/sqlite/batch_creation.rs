use std::collections::{HashMap, HashSet};

use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::ports::{
    BatchCreationRequest, CreatedBatchGraph, ExpectedVersion, Versioned,
};

use super::SqliteStore;
use crate::repository::StatusString;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BatchCreationFaultPoint {
    Snapshot(usize),
    Batch,
    Job(usize),
}

impl SqliteStore {
    /// Persist the complete first-write graph on one SQLite transaction.
    /// Snapshots are inserted first because Job projections derive their
    /// immutable source/output facts from them; the Batch row then satisfies
    /// the Job foreign key; member Jobs are inserted last.
    pub(crate) fn create_batch_graph(
        &mut self,
        request: BatchCreationRequest,
    ) -> VcResult<CreatedBatchGraph> {
        validate_graph(&request)?;
        let fault = self.batch_creation_fault.take();
        let tx = self.conn.unchecked_transaction().map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("begin Batch graph transaction: {error}"),
            )
        })?;

        reject_existing_rows(&tx, &request)?;

        for (index, snapshot) in request.snapshots.iter().enumerate() {
            SqliteStore::save_execution_snapshot_on(&tx, snapshot)?;
            batch_creation_fault(fault, BatchCreationFaultPoint::Snapshot(index + 1))?;
        }

        let batch_json = serde_json::to_string(&request.batch).map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode Batch aggregate: {error}"),
            )
        })?;
        SqliteStore::save_batch_aggregate_on(
            &tx,
            request.batch.id().as_str(),
            request.batch.status().as_str(),
            &request.batch.execution_profile().asr_model,
            &request.batch.execution_profile().device,
            &batch_json,
            ExpectedVersion::New,
        )?;
        batch_creation_fault(fault, BatchCreationFaultPoint::Batch)?;

        let mut persisted_jobs = Vec::with_capacity(request.jobs.len());
        for (index, job) in request.jobs.iter().enumerate() {
            let job_json = serde_json::to_string(job).map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("encode Job aggregate {}: {error}", job.id()),
                )
            })?;
            let version = SqliteStore::save_job_aggregate_on(
                &tx,
                job.id().as_str(),
                job.batch_id().map(|id| id.as_str()),
                job.status().as_str(),
                job.source_path(),
                job.profile_revision().as_str(),
                job.execution_snapshot_id().map(|id| id.as_str()),
                &job_json,
                ExpectedVersion::New,
            )?;
            persisted_jobs.push(Versioned::with_version(job.clone(), version));
            batch_creation_fault(fault, BatchCreationFaultPoint::Job(index + 1))?;
        }

        tx.commit().map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("commit Batch graph transaction: {error}"),
            )
        })?;

        Ok(CreatedBatchGraph {
            batch: Versioned::with_version(request.batch, 1),
            jobs: persisted_jobs,
        })
    }
}

fn validate_graph(request: &BatchCreationRequest) -> VcResult<()> {
    if request.jobs.len() != request.batch.job_ids().len() {
        return Err(VcError::new(
            ErrorCode::InvalidArgument,
            "Batch graph Job count does not match Batch membership",
        ));
    }
    if request.snapshots.len() != request.jobs.len() {
        return Err(VcError::new(
            ErrorCode::InvalidArgument,
            "Batch graph Snapshot count does not match Job count",
        ));
    }

    let members: HashSet<_> = request
        .batch
        .job_ids()
        .iter()
        .map(ToString::to_string)
        .collect();
    let mut jobs_by_id = HashMap::with_capacity(request.jobs.len());
    for job in &request.jobs {
        if !members.contains(job.id().as_str()) {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "Job {} is not a member of Batch {}",
                    job.id(),
                    request.batch.id()
                ),
            ));
        }
        if jobs_by_id.insert(job.id().to_string(), job).is_some() {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                format!("Batch graph contains duplicate Job {}", job.id()),
            ));
        }
        if job.batch_id() != Some(request.batch.id()) {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "Job {} does not point to Batch {}",
                    job.id(),
                    request.batch.id()
                ),
            ));
        }
        if job.execution_snapshot_id().is_none() {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                format!("Job {} has no execution snapshot", job.id()),
            ));
        }
    }

    let mut snapshots_by_id = HashMap::with_capacity(request.snapshots.len());
    for snapshot in &request.snapshots {
        snapshot
            .validate()
            .map_err(|error| VcError::new(ErrorCode::InvalidArgument, error))?;
        if snapshots_by_id
            .insert(snapshot.snapshot_id.to_string(), snapshot)
            .is_some()
        {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "Batch graph contains duplicate Snapshot {}",
                    snapshot.snapshot_id
                ),
            ));
        }
        if snapshot.batch_id != *request.batch.id() {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "Snapshot {} does not point to Batch {}",
                    snapshot.snapshot_id,
                    request.batch.id()
                ),
            ));
        }
        let Some(job) = jobs_by_id.get(snapshot.job_id.as_str()) else {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                format!("Snapshot {} has no member Job", snapshot.snapshot_id),
            ));
        };
        if job.execution_snapshot_id() != Some(&snapshot.snapshot_id) {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "Job {} does not reference Snapshot {}",
                    job.id(),
                    snapshot.snapshot_id
                ),
            ));
        }
        if job.profile_revision() != &snapshot.profile_revision {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "Job {} and Snapshot {} have different profile revisions",
                    job.id(),
                    snapshot.snapshot_id
                ),
            ));
        }
        let profile = request.batch.execution_profile();
        let runtime = snapshot.asr_runtime_spec();
        if runtime.engine_family != profile.asr_engine
            || runtime.model_id != profile.asr_model
            || runtime.device != profile.device
            || runtime.compute_type != profile.compute_type
        {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "Snapshot {} does not match Batch execution profile",
                    snapshot.snapshot_id
                ),
            ));
        }
    }

    for job_id in request.batch.job_ids() {
        let Some(job) = jobs_by_id.get(job_id.as_str()) else {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "Batch {} is missing member Job {job_id}",
                    request.batch.id()
                ),
            ));
        };
        let snapshot_id = job.execution_snapshot_id().ok_or_else(|| {
            VcError::new(
                ErrorCode::InvalidArgument,
                format!("Job {job_id} has no execution snapshot"),
            )
        })?;
        if !snapshots_by_id.contains_key(snapshot_id.as_str()) {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                format!("Job {job_id} references a missing Snapshot {snapshot_id}"),
            ));
        }
    }

    Ok(())
}

fn reject_existing_rows(
    tx: &rusqlite::Transaction<'_>,
    request: &BatchCreationRequest,
) -> VcResult<()> {
    let batch_exists: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM batches WHERE id = ?1)",
            [request.batch.id().as_str()],
            |row| row.get(0),
        )
        .map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("check Batch identity: {error}"),
            )
        })?;
    if batch_exists {
        return Err(VcError::new(
            ErrorCode::StaleResult,
            format!("Batch {} already exists", request.batch.id()),
        ));
    }
    for job in &request.jobs {
        let exists: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = ?1)",
                [job.id().as_str()],
                |row| row.get(0),
            )
            .map_err(|error| {
                VcError::new(ErrorCode::Internal, format!("check Job identity: {error}"))
            })?;
        if exists {
            return Err(VcError::new(
                ErrorCode::StaleResult,
                format!("Job {} already exists", job.id()),
            ));
        }
    }
    for snapshot in &request.snapshots {
        let exists: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM execution_snapshots WHERE snapshot_id = ?1)",
                [snapshot.snapshot_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("check Snapshot identity: {error}"),
                )
            })?;
        if exists {
            return Err(VcError::new(
                ErrorCode::StaleResult,
                format!("Snapshot {} already exists", snapshot.snapshot_id),
            ));
        }
    }
    Ok(())
}

fn batch_creation_fault(
    fault: Option<BatchCreationFaultPoint>,
    point: BatchCreationFaultPoint,
) -> VcResult<()> {
    if fault == Some(point) {
        return Err(VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("injected Batch graph failure at {point:?}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;
    use ulid::Ulid;
    use videocaptionerr_core::execution_snapshot::{
        AsrExecutionSnapshot, AudioStreamSelection, CacheExecutionSnapshot, JobExecutionSnapshot,
        OutputPlanSnapshot, SourceStatSnapshot, JOB_EXECUTION_SNAPSHOT_SCHEMA_VERSION,
    };
    use videocaptionerr_core::ports::{
        BatchCreationRepository, BatchRepository, ExpectedVersion, JobRepository, ModelLocator,
        SnapshotRepository, Versioned, WorkUnitRepository,
    };
    use videocaptionerr_core::use_cases::{
        ActiveCancellationWatcher, CancelBatch, CancelBatchCommand,
    };
    use videocaptionerr_domain::{Batch, BatchExecutionProfile, BatchId, Job, JobId, UlidStr};
    use videocaptionerr_domain::{StageKind, WorkUnit};

    use super::*;
    use crate::StoreHandle;

    fn request(job_count: usize) -> BatchCreationRequest {
        let batch_id: BatchId = Ulid::new().into();
        let profile_revision: UlidStr = Ulid::new().into();
        let profile = BatchExecutionProfile {
            asr_engine: "fake".into(),
            asr_model: "tiny".into(),
            device: "cpu".into(),
            compute_type: "default".into(),
        };
        let mut job_ids = Vec::with_capacity(job_count);
        let mut jobs = Vec::with_capacity(job_count);
        let mut snapshots = Vec::with_capacity(job_count);
        for index in 0..job_count {
            let job_id: JobId = Ulid::new().into();
            let snapshot_id: UlidStr = Ulid::new().into();
            job_ids.push(job_id.clone());
            snapshots.push(JobExecutionSnapshot {
                snapshot_id: snapshot_id.clone(),
                schema_version: JOB_EXECUTION_SNAPSHOT_SCHEMA_VERSION,
                created_at: "2026-07-22T00:00:00Z".into(),
                job_id: job_id.clone(),
                batch_id: batch_id.clone(),
                canonical_source_path: format!("/media/{index}.wav"),
                source_stat: SourceStatSnapshot {
                    size: 1,
                    modified_at_ms: None,
                },
                job_dir: format!("/jobs/{index}"),
                profile_revision: profile_revision.clone(),
                profile_name: Some("test".into()),
                asr: AsrExecutionSnapshot {
                    engine: "fake".into(),
                    model_locator: ModelLocator::file("fake:default"),
                    model_id: Some("tiny".into()),
                    model_digest: None,
                    device: "cpu".into(),
                    compute_type: "default".into(),
                },
                audio_stream: AudioStreamSelection::Auto,
                source_language: None,
                target_language: None,
                output: OutputPlanSnapshot {
                    path: format!("/out/{index}.srt"),
                    format: "srt".into(),
                    layout: "source_only".into(),
                    conflict_policy: "fail".into(),
                    fallback_to_source: false,
                },
                cache: CacheExecutionSnapshot { max_bytes: 0 },
                llm: None,
            });
            jobs.push(Job::new_with_snapshot(
                job_id,
                Some(batch_id.clone()),
                snapshot_id,
                profile_revision.clone(),
                format!("/media/{index}.wav"),
            ));
        }
        BatchCreationRequest {
            batch: Batch::new(batch_id, job_ids, profile).unwrap(),
            jobs,
            snapshots,
        }
    }

    fn counts(store: &SqliteStore) -> (i64, i64, i64) {
        let snapshots = store
            .conn
            .query_row("SELECT COUNT(*) FROM execution_snapshots", [], |row| {
                row.get(0)
            })
            .unwrap();
        let batches = store
            .conn
            .query_row("SELECT COUNT(*) FROM batches", [], |row| row.get(0))
            .unwrap();
        let jobs = store
            .conn
            .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
            .unwrap();
        (snapshots, batches, jobs)
    }

    #[test]
    fn batch_graph_faults_roll_back_snapshots_batch_and_jobs() {
        for point in [
            BatchCreationFaultPoint::Snapshot(1),
            BatchCreationFaultPoint::Batch,
            BatchCreationFaultPoint::Job(2),
        ] {
            let directory = tempdir().unwrap();
            let mut store = SqliteStore::open(&directory.path().join("state.db")).unwrap();
            store.inject_batch_creation_fault(point);
            assert!(store.create_batch_graph(request(3)).is_err(), "{point:?}");
            assert_eq!(counts(&store), (0, 0, 0), "{point:?}");
        }
    }

    #[test]
    fn batch_graph_success_commits_a_complete_consistent_graph() {
        let directory = tempdir().unwrap();
        let mut store = SqliteStore::open(&directory.path().join("state.db")).unwrap();
        let request = request(3);
        let expected_batch_id = request.batch.id().clone();
        let created = store.create_batch_graph(request).unwrap();
        assert_eq!(created.batch.version, 1);
        assert_eq!(created.jobs.len(), 3);
        assert_eq!(counts(&store), (3, 1, 3));
        assert_eq!(created.batch.id(), &expected_batch_id);
    }

    #[tokio::test]
    async fn store_actor_creates_the_same_complete_graph() {
        let directory = tempdir().unwrap();
        let handle = StoreHandle::open(&directory.path().join("state.db")).unwrap();
        let request = request(2);
        let batch_id = request.batch.id().clone();
        let created = BatchCreationRepository::create_batch_graph(&handle, request)
            .await
            .unwrap();
        assert_eq!(created.batch.version, 1);
        assert_eq!(
            BatchRepository::load_batch(&handle, &batch_id)
                .await
                .unwrap()
                .unwrap()
                .version,
            1
        );
        assert_eq!(
            SnapshotRepository::load_snapshots_for_batch(&handle, &batch_id)
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn independent_store_handles_propagate_batch_cancel_to_owner_token() {
        let directory = tempdir().unwrap();
        let db = directory.path().join("cross-process-cancel.db");
        let owner_store = StoreHandle::open(&db).unwrap();
        let control_store = StoreHandle::open(&db).unwrap();
        let request = request(1);
        let batch_id = request.batch.id().clone();
        let job_id = request.jobs[0].id().clone();
        BatchCreationRepository::create_batch_graph(&owner_store, request)
            .await
            .unwrap();

        let mut batch = BatchRepository::load_batch(&owner_store, &batch_id)
            .await
            .unwrap()
            .unwrap();
        batch.start().unwrap();
        let expected = batch.expected_version();
        BatchRepository::save_batch(&owner_store, &mut batch, expected)
            .await
            .unwrap();
        let mut job = JobRepository::load_job(&owner_store, &job_id)
            .await
            .unwrap()
            .unwrap();
        job.start().unwrap();
        let expected = job.expected_version();
        JobRepository::save_job(&owner_store, &mut job, expected)
            .await
            .unwrap();

        // Keep one pending unit in the graph so the owner-side convergence
        // below proves that cancellation covers active Job work as well.
        let unit = WorkUnit::new(
            Ulid::new().into(),
            job_id.clone(),
            StageKind::Asr,
            "chunk",
            0,
            "input-hash",
        )
        .unwrap();
        let mut unit = Versioned::new(unit);
        WorkUnitRepository::save_work_unit(&owner_store, &mut unit, ExpectedVersion::New)
            .await
            .unwrap();

        let token_control = videocaptionerr_core::ports::RunControl::new();
        let token = token_control.cancellation_token();
        let owner_jobs: Arc<dyn JobRepository> = Arc::new(owner_store.clone());
        let owner_batches: Arc<dyn BatchRepository> = Arc::new(owner_store.clone());
        let watcher = ActiveCancellationWatcher::spawn(
            owner_jobs,
            owner_batches,
            job_id.clone(),
            Some(batch_id.clone()),
            token_control,
        );

        // A Gate ASR represents an adapter that does not return until its
        // actual token is requested; the test deliberately never releases it.
        let gate_token = token.clone();
        let gate_asr = tokio::spawn(async move {
            loop {
                if gate_token.is_requested() {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });

        let control_jobs: Arc<dyn JobRepository> = Arc::new(control_store.clone());
        let control_batches: Arc<dyn BatchRepository> = Arc::new(control_store.clone());
        let cancel = CancelBatch::new(control_batches, control_jobs);
        cancel
            .execute(CancelBatchCommand {
                batch_id: batch_id.clone(),
            })
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !token.is_requested() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("owner token was not cancelled through the second Store handle");
        gate_asr.abort();
        let _ = gate_asr.await;

        // A repeated control command only reasserts the durable intent.
        let repeated = cancel
            .execute(CancelBatchCommand {
                batch_id: batch_id.clone(),
            })
            .await
            .unwrap();
        assert!(!repeated.cancel_requested);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !watcher.is_finished() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cancellation watcher did not stop after requesting the token");
        watcher.stop().await;

        // The owner performs the legal terminal transitions after the gate
        // observes cancellation; the control process never force-writes them.
        let mut unit = WorkUnitRepository::load_work_unit(&owner_store, unit.id())
            .await
            .unwrap()
            .unwrap();
        unit.cancel().unwrap();
        let expected = unit.expected_version();
        WorkUnitRepository::save_work_unit(&owner_store, &mut unit, expected)
            .await
            .unwrap();
        let mut job = JobRepository::load_job(&owner_store, &job_id)
            .await
            .unwrap()
            .unwrap();
        job.cancel().unwrap();
        let expected = job.expected_version();
        JobRepository::save_job(&owner_store, &mut job, expected)
            .await
            .unwrap();
        let mut batch = BatchRepository::load_batch(&owner_store, &batch_id)
            .await
            .unwrap()
            .unwrap();
        batch
            .record_job_terminal(
                &job_id,
                videocaptionerr_domain::JobTerminalStatus::Cancelled,
            )
            .unwrap();
        batch.finish_cancelled().unwrap();
        let expected = batch.expected_version();
        BatchRepository::save_batch(&owner_store, &mut batch, expected)
            .await
            .unwrap();
        assert_eq!(job.status(), videocaptionerr_domain::JobStatus::Cancelled);
        assert_eq!(
            unit.status(),
            videocaptionerr_domain::WorkUnitStatus::Cancelled
        );
        assert_eq!(
            batch.status(),
            videocaptionerr_domain::BatchStatus::Cancelled
        );
    }
}
