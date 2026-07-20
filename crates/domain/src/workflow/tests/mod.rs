use super::*;
use crate::error::DomainError;
use crate::identity::UlidStr;
use ulid::Ulid;

fn id() -> UlidStr {
    UlidStr::from(Ulid::new())
}

fn profile() -> BatchExecutionProfile {
    BatchExecutionProfile {
        asr_engine: "fake".into(),
        asr_model: "tiny".into(),
        device: "cpu".into(),
        compute_type: "int8".into(),
    }
}

#[test]
fn batch_rejects_model_profile_switch() {
    let job = id();
    let mut batch = Batch::new(id(), vec![job], profile()).unwrap();
    batch.start().unwrap();
    let mut other = profile();
    other.asr_model = "small".into();
    assert_eq!(
        batch.require_profile(&other),
        Err(DomainError::BatchProfileMismatch)
    );
}

#[test]
fn batch_request_cancel_does_not_terminalize_until_all_jobs_finish() {
    let job = id();
    let mut batch = Batch::new(id(), vec![job.clone()], profile()).unwrap();
    batch.start().unwrap();
    batch.request_cancel().unwrap();
    assert!(batch.cancel_requested());
    assert_eq!(batch.status(), BatchStatus::Running);
    assert!(batch.finish_cancelled().is_err());
    batch
        .record_job_terminal(&job, JobTerminalStatus::Cancelled)
        .unwrap();
    let event = batch.finish_cancelled().unwrap();
    assert!(matches!(
        event,
        DomainEvent::BatchReachedTerminal {
            status: BatchStatus::Cancelled,
            ..
        }
    ));
}

#[test]
fn batch_emits_one_terminal_event_and_cannot_restart() {
    let job = id();
    let mut batch = Batch::new(id(), vec![job.clone()], profile()).unwrap();
    batch.start().unwrap();
    batch
        .record_job_terminal(&job, JobTerminalStatus::Done)
        .unwrap();
    let event = batch.finish(BatchStatus::Done).unwrap();
    assert!(matches!(
        event,
        DomainEvent::BatchReachedTerminal {
            status: BatchStatus::Done,
            ..
        }
    ));
    assert!(batch.terminal_event_emitted());
    assert!(batch.start().is_err());
    assert!(batch.finish(BatchStatus::Done).is_err());
}

#[test]
fn job_requires_stage_order_and_artifact_match() {
    let mut job = Job::new(id(), None, id(), "/media/a.wav");
    job.start().unwrap();
    assert!(job.start_stage(StageKind::Asr).is_err());
    job.start_stage(StageKind::Probe).unwrap();
    let artifact = ArtifactRef {
        id: id(),
        stage: StageKind::Probe,
        path: "probe.json".into(),
        content_hash: "h".into(),
        schema_version: 1,
        producer_fingerprint: "test".into(),
    };
    job.complete_stage(StageKind::Probe, artifact, false)
        .unwrap();
    job.start_stage(StageKind::ExtractAudio).unwrap();
}

#[test]
fn retry_resets_failed_stage_and_preserves_prerequisite() {
    let mut job = Job::new(id(), None, id(), "/media/input.wav");
    job.start().unwrap();
    job.start_stage(StageKind::Probe).unwrap();
    job.complete_stage(
        StageKind::Probe,
        ArtifactRef {
            id: id(),
            stage: StageKind::Probe,
            path: "probe.json".into(),
            content_hash: "hash".into(),
            schema_version: 1,
            producer_fingerprint: "test".into(),
        },
        false,
    )
    .unwrap();
    job.start_stage(StageKind::ExtractAudio).unwrap();
    job.fail_stage(StageKind::ExtractAudio).unwrap();
    assert_eq!(job.status(), JobStatus::Failed);
    let start = job.prepare_retry(None).unwrap();
    assert_eq!(start, StageKind::ExtractAudio);
    assert_eq!(job.status(), JobStatus::Pending);
    assert_eq!(job.stages()[0].status, StageStatus::Done);
    assert_eq!(job.stages()[1].status, StageStatus::Pending);
    assert_eq!(job.retry_attempt(), 1);
    assert_eq!(job.retry_generation(), 1);
}

#[test]
fn prepare_retry_without_failed_stage_requires_explicit_from_stage() {
    let mut job = Job::new(id(), None, id(), "/media/input.wav");
    job.start().unwrap();
    for kind in [
        StageKind::Probe,
        StageKind::ExtractAudio,
        StageKind::Asr,
        StageKind::Split,
        StageKind::Correct,
        StageKind::Translate,
        StageKind::Export,
    ] {
        if matches!(kind, StageKind::Correct | StageKind::Translate) {
            job.skip_stage(kind).unwrap();
            continue;
        }
        job.start_stage(kind).unwrap();
        job.complete_stage(
            kind,
            ArtifactRef {
                id: id(),
                stage: kind,
                path: format!("{kind:?}.json"),
                content_hash: "h".into(),
                schema_version: 1,
                producer_fingerprint: "test".into(),
            },
            kind == StageKind::Asr,
        )
        .unwrap();
    }
    job.finish().unwrap();
    assert_eq!(job.status(), JobStatus::DoneDegraded);
    // No Failed/Cancelled stage: None is rejected (no stage-0 fallback).
    assert!(job.prepare_retry(None).is_err());
    assert_eq!(
        job.prepare_retry(Some(StageKind::Export)).unwrap(),
        StageKind::Export
    );
    assert_eq!(job.stages()[6].status, StageStatus::Pending);
}

#[test]
fn batch_prepare_retry_reopens_one_job_only() {
    let job_a = id();
    let job_b = id();
    let mut batch = Batch::new(id(), vec![job_a.clone(), job_b.clone()], profile()).unwrap();
    batch.start().unwrap();
    batch
        .record_job_terminal(&job_a, JobTerminalStatus::Failed)
        .unwrap();
    batch
        .record_job_terminal(&job_b, JobTerminalStatus::Done)
        .unwrap();
    batch.finish(BatchStatus::Failed).unwrap();
    batch.prepare_retry(&job_a).unwrap();
    assert_eq!(batch.status(), BatchStatus::Pending);
    assert!(!batch.terminal_event_emitted());
    assert!(!batch.has_terminal_record(&job_a));
    // Job B remains terminal in the aggregate bookkeeping.
    assert!(batch.has_terminal_record(&job_b));
    batch.start().unwrap();
    // Cannot finish until the reopened Job is terminal again.
    assert!(batch.finish(BatchStatus::Done).is_err());
    batch
        .record_job_terminal(&job_a, JobTerminalStatus::Done)
        .unwrap();
    assert!(batch.finish(BatchStatus::Done).is_ok());
}

#[test]
fn expired_work_unit_returns_to_pending_with_new_attempt() {
    let mut unit = WorkUnit::new(id(), id(), StageKind::Asr, "chunk", 0, "pcm-hash").unwrap();
    unit.lease_for("worker", 10, 20).unwrap();
    unit.recover_expired(20).unwrap();
    assert_eq!(unit.status(), WorkUnitStatus::Pending);
    assert_eq!(unit.attempt(), 1);
    assert!(unit.lease_for("worker-2", 20, 30).is_ok());
}

#[test]
fn work_unit_auto_retries_twice_then_stops() {
    let mut unit = WorkUnit::new(id(), id(), StageKind::Asr, "chunk", 0, "pcm-hash").unwrap();
    unit.lease_for("owner", 0, 10).unwrap();
    assert!(unit.fail_with_auto_retry("ASR_FAILED").unwrap());
    assert_eq!(unit.status(), WorkUnitStatus::Pending);
    assert_eq!(unit.attempt(), 1);

    unit.lease_for("owner", 10, 20).unwrap();
    assert!(unit.fail_with_auto_retry("ASR_FAILED").unwrap());
    assert_eq!(unit.attempt(), 2);

    unit.lease_for("owner", 20, 30).unwrap();
    assert!(!unit.fail_with_auto_retry("ASR_FAILED").unwrap());
    assert_eq!(unit.status(), WorkUnitStatus::Failed);
    assert_eq!(unit.attempt(), 2);
}

#[test]
fn work_unit_deterministic_errors_are_not_auto_retried() {
    let mut unit = WorkUnit::new(id(), id(), StageKind::Asr, "chunk", 0, "pcm-hash").unwrap();
    unit.lease_for("owner", 0, 10).unwrap();
    assert!(!unit.fail_with_auto_retry("WORKER_PROTOCOL_ERROR").unwrap());
    assert_eq!(unit.status(), WorkUnitStatus::Failed);
}

#[test]
fn work_unit_oom_strategy_retry_is_at_most_once() {
    let mut unit = WorkUnit::new(id(), id(), StageKind::Asr, "chunk", 0, "pcm-hash").unwrap();
    unit.lease_for("owner", 0, 10).unwrap();
    assert!(unit.requeue_after_oom_strategy_change().unwrap());
    assert_eq!(unit.oom_strategy_retries(), 1);
    unit.lease_for("owner", 10, 20).unwrap();
    assert!(!unit.requeue_after_oom_strategy_change().unwrap());
    assert_eq!(unit.status(), WorkUnitStatus::Failed);
}

#[test]
fn batch_pause_and_resume_do_not_terminalize() {
    let job = id();
    let mut batch = Batch::new(id(), vec![job], profile()).unwrap();
    batch.start().unwrap();
    batch.request_pause().unwrap();
    assert!(batch.pause_requested());
    assert_eq!(batch.status(), BatchStatus::Running);
    batch.resume().unwrap();
    assert!(!batch.pause_requested());
    assert_eq!(batch.status(), BatchStatus::Running);
}

#[test]
fn batch_cannot_pause_after_cancel_requested() {
    let job = id();
    let mut batch = Batch::new(id(), vec![job], profile()).unwrap();
    batch.start().unwrap();
    batch.request_cancel().unwrap();
    assert!(batch.request_pause().is_err());
}
