use super::*;
use rusqlite::Connection;
use ulid::Ulid;
use videocaptionerr_contracts::error::ErrorCode;
use videocaptionerr_core::execution_snapshot::{
    AsrExecutionSnapshot, AudioStreamSelection, JobExecutionSnapshot, OutputPlanSnapshot,
    SourceStatSnapshot, JOB_EXECUTION_SNAPSHOT_SCHEMA_VERSION,
};
use videocaptionerr_core::ports::{
    ArtifactStore, BatchRepository, ExpectedVersion, JobRepository, SnapshotRepository,
    TranscriptCommit, Versioned, WorkUnitRepository,
};
use videocaptionerr_domain::{
    Batch, BatchExecutionProfile, EngineFingerprint, JobId, JobStatus, StageStatus, Transcript,
    WorkUnit, WorkUnitStatus,
};

fn complete_job(job: &mut Job, asr_artifact: ArtifactRef) {
    job.start().unwrap();
    for stage in [
        StageKind::Probe,
        StageKind::ExtractAudio,
        StageKind::Asr,
        StageKind::Split,
        StageKind::Correct,
        StageKind::Translate,
        StageKind::Export,
    ] {
        job.start_stage(stage).unwrap();
        let artifact = if stage == StageKind::Asr {
            asr_artifact.clone()
        } else {
            ArtifactRef {
                id: ulid::Ulid::new().into(),
                stage,
                path: format!("{stage:?}.json"),
                content_hash: format!("{stage:?}-hash"),
                schema_version: videocaptionerr_domain::SCHEMA_VERSION,
                producer_fingerprint: "test".into(),
            }
        };
        job.complete_stage(stage, artifact, false).unwrap();
    }
    job.finish().unwrap();
}

async fn save_new_job(store: &StoreHandle, job: Job) -> Versioned<Job> {
    let mut versioned = Versioned::new(job);
    <StoreHandle as JobRepository>::save_job(store, &mut versioned, ExpectedVersion::New)
        .await
        .unwrap();
    versioned
}

async fn save_new_work_unit(store: &StoreHandle, unit: WorkUnit) -> Versioned<WorkUnit> {
    let mut versioned = Versioned::new(unit);
    <StoreHandle as WorkUnitRepository>::save_work_unit(
        store,
        &mut versioned,
        ExpectedVersion::New,
    )
    .await
    .unwrap();
    versioned
}

async fn save_new_batch(store: &StoreHandle, batch: Batch) -> Versioned<Batch> {
    let mut versioned = Versioned::new(batch);
    <StoreHandle as BatchRepository>::save_batch(store, &mut versioned, ExpectedVersion::New)
        .await
        .unwrap();
    versioned
}

fn snapshot(job_id: JobId, batch_id: videocaptionerr_domain::BatchId) -> JobExecutionSnapshot {
    JobExecutionSnapshot {
        snapshot_id: Ulid::new().into(),
        schema_version: JOB_EXECUTION_SNAPSHOT_SCHEMA_VERSION,
        created_at: "2026-07-20T00:00:00Z".into(),
        job_id,
        batch_id,
        canonical_source_path: "/media/input.mp4".into(),
        source_stat: SourceStatSnapshot {
            size: 123,
            modified_at_ms: Some(1_725_000_000_000),
        },
        job_dir: "/jobs/job-1".into(),
        profile_revision: Ulid::new().into(),
        asr: AsrExecutionSnapshot {
            engine: "fake".into(),
            model_locator: videocaptionerr_core::ModelLocator::file("fake:default"),
            model_id: Some("fake".into()),
            model_digest: None,
            device: "cpu".into(),
            compute_type: "default".into(),
        },
        audio_stream: AudioStreamSelection::Auto,
        source_language: Some("en".into()),
        target_language: Some("zh".into()),
        output: OutputPlanSnapshot {
            path: "/exports/input.zh.srt".into(),
            format: "srt".into(),
            layout: "source".into(),
            conflict_policy: "rename".into(),
            fallback_to_source: true,
        },
        llm: None,
    }
}

#[tokio::test]
async fn execution_snapshot_survives_store_reopen_and_is_immutable() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("snapshot.db");
    let job_id: JobId = Ulid::new().into();
    let batch_id: videocaptionerr_domain::BatchId = Ulid::new().into();
    let snapshot = snapshot(job_id, batch_id);
    let store = StoreHandle::open(&db_path).unwrap();

    <StoreHandle as SnapshotRepository>::save_execution_snapshot(&store, &snapshot)
        .await
        .unwrap();
    let mut job = Versioned::new(Job::new_with_snapshot(
        snapshot.job_id.clone(),
        None,
        snapshot.snapshot_id.clone(),
        snapshot.profile_revision.clone(),
        "/media/stale-input.mp4",
    ));
    <StoreHandle as JobRepository>::save_job(&store, &mut job, ExpectedVersion::New)
        .await
        .unwrap();
    let connection = Connection::open(&db_path).unwrap();
    let projection: (String, String, String, String) = connection
        .query_row(
            "SELECT source_path, job_dir, profile_revision, execution_snapshot_id
                 FROM jobs WHERE id = ?1",
            [snapshot.job_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(projection.0, snapshot.canonical_source_path);
    assert_eq!(projection.1, snapshot.job_dir);
    assert_eq!(projection.2, snapshot.profile_revision.to_string());
    assert_eq!(projection.3, snapshot.snapshot_id.to_string());

    let reopened = StoreHandle::open(&db_path).unwrap();
    let loaded = <StoreHandle as SnapshotRepository>::load_execution_snapshot(
        &reopened,
        &snapshot.snapshot_id,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(loaded, snapshot);

    let mut changed = snapshot.clone();
    changed.job_dir = "/jobs/changed".into();
    let error = <StoreHandle as SnapshotRepository>::save_execution_snapshot(&reopened, &changed)
        .await
        .unwrap_err()
        .into_vc_error();
    assert_eq!(error.code, ErrorCode::StaleResult);
}

#[tokio::test]
async fn concurrent_job_batch_and_work_unit_saves_reject_stale_versions() {
    let dir = tempfile::tempdir().unwrap();
    let store = StoreHandle::open(&dir.path().join("cas.db")).unwrap();

    let job_id: JobId = Ulid::new().into();
    let mut job_first = save_new_job(
        &store,
        Job::new(job_id.clone(), None, Ulid::new().into(), "/media/input.wav"),
    )
    .await;
    let mut job_second = <StoreHandle as JobRepository>::load_job(&store, &job_id)
        .await
        .unwrap()
        .unwrap();
    job_first.start().unwrap();
    <StoreHandle as JobRepository>::save_job(&store, &mut job_first, ExpectedVersion::Exact(1))
        .await
        .unwrap();
    let error = <StoreHandle as JobRepository>::save_job(
        &store,
        &mut job_second,
        ExpectedVersion::Exact(1),
    )
    .await
    .unwrap_err()
    .into_vc_error();
    assert_eq!(error.code, ErrorCode::StaleResult);

    let batch_id: videocaptionerr_domain::BatchId = Ulid::new().into();
    let batch = Batch::new(
        batch_id.clone(),
        vec![Ulid::new().into()],
        BatchExecutionProfile {
            asr_engine: "fake".into(),
            asr_model: "fake".into(),
            device: "cpu".into(),
            compute_type: "default".into(),
        },
    )
    .unwrap();
    let mut batch_first = save_new_batch(&store, batch).await;
    let mut batch_second = <StoreHandle as BatchRepository>::load_batch(&store, &batch_id)
        .await
        .unwrap()
        .unwrap();
    batch_first.start().unwrap();
    <StoreHandle as BatchRepository>::save_batch(
        &store,
        &mut batch_first,
        ExpectedVersion::Exact(1),
    )
    .await
    .unwrap();
    let error = <StoreHandle as BatchRepository>::save_batch(
        &store,
        &mut batch_second,
        ExpectedVersion::Exact(1),
    )
    .await
    .unwrap_err()
    .into_vc_error();
    assert_eq!(error.code, ErrorCode::StaleResult);

    let mut unit_first = save_new_work_unit(
        &store,
        WorkUnit::new(
            Ulid::new().into(),
            job_id,
            videocaptionerr_domain::StageKind::Asr,
            "chunk",
            0,
            "input-hash",
        )
        .unwrap(),
    )
    .await;
    let mut unit_second =
        <StoreHandle as WorkUnitRepository>::load_work_unit(&store, unit_first.id())
            .await
            .unwrap()
            .unwrap();
    unit_first.lease_for("first", 1_000, 2_000).unwrap();
    <StoreHandle as WorkUnitRepository>::save_work_unit(
        &store,
        &mut unit_first,
        ExpectedVersion::Exact(1),
    )
    .await
    .unwrap();
    let error = <StoreHandle as WorkUnitRepository>::save_work_unit(
        &store,
        &mut unit_second,
        ExpectedVersion::Exact(1),
    )
    .await
    .unwrap_err()
    .into_vc_error();
    assert_eq!(error.code, ErrorCode::StaleResult);
}

#[tokio::test]
async fn transcript_commit_completes_leased_chunk_and_terminal_job_has_no_running_units() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("repository.db");
    let store = StoreHandle::open(&db_path).unwrap();
    let artifact_store = SqliteArtifactStore::new(store.clone());
    let job_id: JobId = ulid::Ulid::new().into();
    let mut job = save_new_job(
        &store,
        Job::new(
            job_id.clone(),
            None,
            ulid::Ulid::new().into(),
            "/media/input.wav",
        ),
    )
    .await;

    let mut unit = save_new_work_unit(
        &store,
        WorkUnit::new(
            ulid::Ulid::new().into(),
            job_id.clone(),
            StageKind::Asr,
            "asr_chunk",
            0,
            "chunk-input-hash",
        )
        .unwrap(),
    )
    .await;
    unit.lease_for("asr:test", 0, 1_000).unwrap();
    let expected = unit.expected_version();
    <StoreHandle as WorkUnitRepository>::save_work_unit(&store, &mut unit, expected)
        .await
        .unwrap();

    let artifact = artifact_store
        .commit_transcript(TranscriptCommit {
            job_id: job_id.clone(),
            stage: StageKind::Asr,
            artifact_id: ulid::Ulid::new().into(),
            path: dir.path().join("job/asr-chunks/chunk-0000.json"),
            transcript: Transcript::new_asr(
                "source-hash",
                EngineFingerprint::unknown(),
                Vec::new(),
            ),
            producer_fingerprint: "fake@test".into(),
            work_unit_id: Some(unit.id().clone()),
        })
        .await
        .unwrap();

    let completed_unit = <StoreHandle as WorkUnitRepository>::load_work_unit(&store, unit.id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed_unit.status(), WorkUnitStatus::Done);
    assert_eq!(completed_unit.artifact(), Some(&artifact));

    complete_job(&mut job, artifact);
    let expected = job.expected_version();
    <StoreHandle as JobRepository>::save_job(&store, &mut job, expected)
        .await
        .unwrap();
    let persisted_job = <StoreHandle as JobRepository>::load_job(&store, &job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted_job.status(), JobStatus::Done);
    assert!(persisted_job
        .stages()
        .iter()
        .all(|stage| stage.status == StageStatus::Done));

    let connection = Connection::open(&db_path).unwrap();
    let non_terminal_units: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM work_units
                 WHERE job_id = ?1 AND status IN ('pending', 'running')",
            [job_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(non_terminal_units, 0);
}

#[tokio::test]
async fn transcript_commit_failure_leaves_chunk_lease_recoverable() {
    let dir = tempfile::tempdir().unwrap();
    let store = StoreHandle::open(&dir.path().join("repository.db")).unwrap();
    let artifact_store = SqliteArtifactStore::new(store.clone());
    let job_id: JobId = ulid::Ulid::new().into();
    let _job = save_new_job(
        &store,
        Job::new(
            job_id.clone(),
            None,
            ulid::Ulid::new().into(),
            "/media/input.wav",
        ),
    )
    .await;

    let mut unit = save_new_work_unit(
        &store,
        WorkUnit::new(
            ulid::Ulid::new().into(),
            job_id.clone(),
            StageKind::Asr,
            "asr_chunk",
            0,
            "chunk-input-hash",
        )
        .unwrap(),
    )
    .await;
    unit.lease_for("asr:test", 0, 1_000).unwrap();
    let expected = unit.expected_version();
    <StoreHandle as WorkUnitRepository>::save_work_unit(&store, &mut unit, expected)
        .await
        .unwrap();

    assert!(artifact_store
        .commit_transcript(TranscriptCommit {
            job_id: job_id.clone(),
            stage: StageKind::Asr,
            artifact_id: ulid::Ulid::new().into(),
            path: std::path::PathBuf::from("\0invalid-artifact.json"),
            transcript: Transcript::new_asr(
                "source-hash",
                EngineFingerprint::unknown(),
                Vec::new(),
            ),
            producer_fingerprint: "fake@test".into(),
            work_unit_id: Some(unit.id().clone()),
        })
        .await
        .is_err());

    let failed_unit = <StoreHandle as WorkUnitRepository>::load_work_unit(&store, unit.id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed_unit.status(), WorkUnitStatus::Running);
    assert_eq!(
        <StoreHandle as WorkUnitRepository>::recover_expired(&store, 1_001)
            .await
            .unwrap(),
        1
    );
    let retryable_unit = <StoreHandle as WorkUnitRepository>::load_work_unit(&store, unit.id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retryable_unit.status(), WorkUnitStatus::Pending);
}

#[tokio::test]
async fn work_unit_repository_recovery_keeps_json_and_control_state_aligned() {
    let dir = tempfile::tempdir().unwrap();
    let store = StoreHandle::open(&dir.path().join("repository.db")).unwrap();
    let job_id: JobId = ulid::Ulid::new().into();
    let _job = save_new_job(
        &store,
        Job::new(
            job_id.clone(),
            None,
            ulid::Ulid::new().into(),
            "/media/input.wav",
        ),
    )
    .await;

    let unit_id = ulid::Ulid::new().into();
    let mut unit = save_new_work_unit(
        &store,
        WorkUnit::new(
            unit_id,
            job_id.clone(),
            StageKind::Asr,
            "chunk",
            0,
            "input-hash",
        )
        .unwrap(),
    )
    .await;
    unit.lease_for("test", 0, 1_000).unwrap();
    let expected = unit.expected_version();
    <StoreHandle as WorkUnitRepository>::save_work_unit(&store, &mut unit, expected)
        .await
        .unwrap();
    assert_eq!(
        <StoreHandle as WorkUnitRepository>::count_retryable(&store, &job_id, None)
            .await
            .unwrap(),
        0
    );

    let loaded = <StoreHandle as WorkUnitRepository>::load_work_unit(&store, unit.id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.status(), WorkUnitStatus::Running);

    assert_eq!(
        <StoreHandle as WorkUnitRepository>::recover_expired(&store, 2_000)
            .await
            .unwrap(),
        1
    );
    let recovered = <StoreHandle as WorkUnitRepository>::load_work_unit(&store, unit.id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.status(), WorkUnitStatus::Pending);
    assert_eq!(recovered.attempt(), 1);
    assert!(recovered.lease().is_none());
}

#[tokio::test]
async fn actor_claims_fifo_unit_and_retries_failed_unit() {
    let dir = tempfile::tempdir().unwrap();
    let store = StoreHandle::open(&dir.path().join("scheduler.db")).unwrap();
    let job_id: JobId = ulid::Ulid::new().into();
    let _job = save_new_job(
        &store,
        Job::new(
            job_id.clone(),
            None,
            ulid::Ulid::new().into(),
            "/media/input.wav",
        ),
    )
    .await;
    let _unit = save_new_work_unit(
        &store,
        WorkUnit::new(
            ulid::Ulid::new().into(),
            job_id.clone(),
            StageKind::Asr,
            "chunk",
            0,
            "pcm-hash",
        )
        .unwrap(),
    )
    .await;

    let leased = <StoreHandle as WorkUnitRepository>::lease_next_ready(
        &store,
        &job_id,
        StageKind::Asr,
        "scheduler-1",
        1_000,
        5_000,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(leased.status(), WorkUnitStatus::Running);
    assert_eq!(leased.lease().unwrap().owner, "scheduler-1");

    let mut unit = leased;
    unit.fail("ASR_FAILED").unwrap();
    let expected = unit.expected_version();
    <StoreHandle as WorkUnitRepository>::save_work_unit(&store, &mut unit, expected)
        .await
        .unwrap();
    assert_eq!(
        <StoreHandle as WorkUnitRepository>::count_retryable(&store, &job_id, None)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        <StoreHandle as WorkUnitRepository>::retry_failed(&store, &job_id, None)
            .await
            .unwrap(),
        1
    );
    let retried = <StoreHandle as WorkUnitRepository>::load_work_unit(&store, unit.id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retried.status(), WorkUnitStatus::Pending);
    assert_eq!(retried.attempt(), 1);
    assert_eq!(
        <StoreHandle as WorkUnitRepository>::count_retryable(&store, &job_id, None)
            .await
            .unwrap(),
        0
    );
}
