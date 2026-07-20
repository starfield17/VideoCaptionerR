use super::*;
use crate::artifact::atomic_write_bytes;
use crate::StoreHandle;
use tempfile::tempdir;
use videocaptionerr_core::ports::{
    ArtifactSource, OutboxEvent, PreparedArtifact, StageCommitRequest,
};

fn stage_commit_fixture(
    root: &Path,
) -> (
    SqliteStore,
    videocaptionerr_domain::JobId,
    PathBuf,
    PathBuf,
    videocaptionerr_core::ports::StageCommitRequest,
) {
    let jobs_root = root.join("jobs");
    let job_dir = jobs_root.join("job1");
    fs::create_dir_all(&job_dir).unwrap();
    let store = SqliteStore::open(&root.join("state.db")).unwrap();
    let job_id: videocaptionerr_domain::JobId = Ulid::new().into();
    let mut job =
        videocaptionerr_domain::Job::new(job_id.clone(), None, Ulid::new().into(), "/media/a.mp4");
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
        let (mut store, job_id, jobs_root, final_path, request) = stage_commit_fixture(dir.path());
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
    let mut store = SqliteStore::open(&db).unwrap();

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
    let meta = SqliteStore::new_artifact_meta(
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
    let mut store = SqliteStore::open(&db).unwrap();
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
    let meta = SqliteStore::new_artifact_meta(
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
    let store = SqliteStore::open(&db).unwrap();
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
    let mut store = SqliteStore::open(&db).unwrap();
    let job_id: videocaptionerr_domain::JobId = Ulid::new().into();
    let profile_revision: videocaptionerr_domain::UlidStr = Ulid::new().into();
    let mut initial =
        videocaptionerr_domain::Job::new(job_id.clone(), None, profile_revision, "/media/a.mp4");
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
    let mut store = SqliteStore::open(&db).unwrap();
    let job_id: videocaptionerr_domain::JobId = Ulid::new().into();
    let profile_revision: videocaptionerr_domain::UlidStr = Ulid::new().into();
    let job =
        videocaptionerr_domain::Job::new(job_id.clone(), None, profile_revision, "/media/a.mp4");
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
    let mut store = SqliteStore::open(&db).unwrap();
    let job_id: videocaptionerr_domain::JobId = Ulid::new().into();
    let profile_revision: videocaptionerr_domain::UlidStr = Ulid::new().into();
    let mut job =
        videocaptionerr_domain::Job::new(job_id.clone(), None, profile_revision, "/media/a.mp4");
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

    let store = SqliteStore::open(&db).unwrap();
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
