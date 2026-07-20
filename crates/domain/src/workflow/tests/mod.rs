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
    job.prepare_retry(None).unwrap();
    assert_eq!(job.status(), JobStatus::Pending);
    assert_eq!(job.stages()[0].status, StageStatus::Done);
    assert_eq!(job.stages()[1].status, StageStatus::Pending);
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
