//! Rebuild a durable Batch execution from its immutable Job snapshots.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use videocaptionerr_domain::{Batch, BatchExecutionProfile, BatchId, BatchStatus, Job, UlidStr};

use crate::application_error::{AppResult, ApplicationError};
use crate::execution_snapshot::{AsrExecutionSnapshot, JobExecutionSnapshot};
use crate::ports::{AsrRuntimeSpec, BatchRepository, JobRepository, SnapshotRepository, Versioned};

use super::{RunBatch, RunBatchCommand, RunBatchResponse, TranscribeJobCommand};

/// Reopens a persisted Batch after an owner restart without consulting current
/// CLI/Desktop defaults. The snapshots are the only source for commands and
/// the ASR runtime identity.
pub struct ResumeBatch {
    batches: Arc<dyn BatchRepository>,
    jobs: Arc<dyn JobRepository>,
    snapshots: Arc<dyn SnapshotRepository>,
    run_batch: Arc<RunBatch>,
}

impl ResumeBatch {
    pub fn new(
        batches: Arc<dyn BatchRepository>,
        jobs: Arc<dyn JobRepository>,
        snapshots: Arc<dyn SnapshotRepository>,
        run_batch: Arc<RunBatch>,
    ) -> Self {
        Self {
            batches,
            jobs,
            snapshots,
            run_batch,
        }
    }

    pub async fn execute(&self, batch_id: BatchId) -> AppResult<RunBatchResponse> {
        let batch = self
            .batches
            .load_batch(&batch_id)
            .await?
            .ok_or_else(|| ApplicationError::Invalid(format!("Batch {batch_id} not found")))?;
        if batch.status().is_terminal() {
            return Err(ApplicationError::Invalid(format!(
                "Batch {batch_id} is already {:?}",
                batch.status()
            )));
        }

        let mut jobs = Vec::with_capacity(batch.job_ids().len());
        for job_id in batch.job_ids() {
            jobs.push(self.jobs.load_job(job_id).await?.ok_or_else(|| {
                ApplicationError::Invalid(format!("Job {job_id} not found for Batch {batch_id}"))
            })?);
        }
        let snapshots = self.snapshots.load_snapshots_for_batch(&batch_id).await?;
        let validated = validate_resume_graph(&batch, jobs, snapshots)?;
        if batch.status() == BatchStatus::Paused {
            return Err(ApplicationError::Invalid(
                "paused Batch must be resumed before execution".into(),
            ));
        }
        self.run_batch
            .execute(RunBatchCommand {
                batch: batch.value,
                jobs: validated.commands,
                asr_spec: validated.asr_spec,
            })
            .await
    }
}

#[derive(Debug)]
struct ValidatedResume {
    commands: Vec<TranscribeJobCommand>,
    asr_spec: AsrRuntimeSpec,
}

/// Validate the complete persisted graph before a new owner opens any model.
/// A resume must never silently select the first Snapshot or current Profile;
/// every member is checked against the same immutable execution identity.
fn validate_resume_graph(
    batch: &Versioned<Batch>,
    jobs: Vec<Versioned<Job>>,
    snapshots: Vec<JobExecutionSnapshot>,
) -> AppResult<ValidatedResume> {
    if batch.job_ids().is_empty() {
        return Err(invalid_resume(batch.id(), "Batch has no member Jobs"));
    }
    if jobs.len() != batch.job_ids().len() {
        return Err(invalid_resume(
            batch.id(),
            "loaded Job count does not match Batch membership",
        ));
    }

    let mut member_ids = HashSet::with_capacity(batch.job_ids().len());
    for job_id in batch.job_ids() {
        if !member_ids.insert(job_id.to_string()) {
            return Err(invalid_resume(
                batch.id(),
                format!("Batch contains duplicate member Job {job_id}"),
            ));
        }
    }

    let mut jobs_by_id = HashMap::with_capacity(jobs.len());
    for job in jobs {
        if !member_ids.contains(job.id().as_str()) {
            return Err(invalid_resume(
                batch.id(),
                format!("loaded Job {} is not a Batch member", job.id()),
            ));
        }
        if jobs_by_id.insert(job.id().to_string(), job).is_some() {
            return Err(invalid_resume(
                batch.id(),
                "Batch graph contains a duplicate loaded Job",
            ));
        }
    }

    let mut snapshots_by_id = HashMap::with_capacity(snapshots.len());
    let mut snapshot_jobs = HashSet::with_capacity(snapshots.len());
    for snapshot in snapshots {
        snapshot
            .validate()
            .map_err(|error| invalid_resume(batch.id(), error))?;
        snapshot
            .asr_runtime_spec()
            .validate()
            .map_err(|error| invalid_resume(batch.id(), error))?;
        if snapshot.batch_id != *batch.id() {
            return Err(invalid_resume(
                batch.id(),
                format!(
                    "Snapshot {} belongs to Batch {}, not {}",
                    snapshot.snapshot_id,
                    snapshot.batch_id,
                    batch.id()
                ),
            ));
        }
        if !member_ids.contains(snapshot.job_id.as_str()) {
            return Err(invalid_resume(
                batch.id(),
                format!(
                    "Snapshot {} belongs to a non-member Job",
                    snapshot.snapshot_id
                ),
            ));
        }
        if !snapshot_jobs.insert(snapshot.job_id.to_string()) {
            return Err(invalid_resume(
                batch.id(),
                format!(
                    "Batch has more than one Snapshot for Job {}",
                    snapshot.job_id
                ),
            ));
        }
        if snapshots_by_id
            .insert(snapshot.snapshot_id.to_string(), snapshot)
            .is_some()
        {
            return Err(invalid_resume(
                batch.id(),
                "Batch graph contains a duplicate Snapshot id",
            ));
        }
    }

    if snapshots_by_id.len() != batch.job_ids().len() {
        return Err(invalid_resume(
            batch.id(),
            "Snapshot count does not match Batch membership",
        ));
    }

    let mut commands = Vec::with_capacity(batch.job_ids().len());
    let mut profile_revision: Option<UlidStr> = None;
    let mut asr_identity: Option<AsrExecutionSnapshot> = None;
    let mut member_snapshot_ids = HashSet::with_capacity(batch.job_ids().len());

    for job_id in batch.job_ids() {
        let job = jobs_by_id.get(job_id.as_str()).ok_or_else(|| {
            invalid_resume(batch.id(), format!("Batch is missing member Job {job_id}"))
        })?;
        if job.batch_id() != Some(batch.id()) {
            return Err(invalid_resume(
                batch.id(),
                format!("Job {job_id} does not belong to Batch {}", batch.id()),
            ));
        }
        let snapshot_id = job.execution_snapshot_id().ok_or_else(|| {
            invalid_resume(
                batch.id(),
                format!("Job {job_id} has no execution snapshot"),
            )
        })?;
        if !member_snapshot_ids.insert(snapshot_id.to_string()) {
            return Err(invalid_resume(
                batch.id(),
                format!("multiple Jobs reference Snapshot {snapshot_id}"),
            ));
        }
        let snapshot = snapshots_by_id.get(snapshot_id.as_str()).ok_or_else(|| {
            invalid_resume(
                batch.id(),
                format!("Snapshot {snapshot_id} for Job {job_id} is missing"),
            )
        })?;
        if snapshot.job_id != *job_id {
            return Err(invalid_resume(
                batch.id(),
                format!("Snapshot {snapshot_id} does not point to Job {job_id}"),
            ));
        }
        if &snapshot.profile_revision != job.profile_revision() {
            return Err(invalid_resume(
                batch.id(),
                format!("Job {job_id} and Snapshot {snapshot_id} have different profile revisions"),
            ));
        }
        if let Some(expected) = profile_revision.as_ref() {
            if expected != job.profile_revision() {
                return Err(invalid_resume(
                    batch.id(),
                    format!("Job {job_id} has a different profile revision"),
                ));
            }
        } else {
            profile_revision = Some(job.profile_revision().clone());
        }
        validate_asr_identity(
            batch.id(),
            batch.execution_profile(),
            asr_identity.as_ref(),
            snapshot,
        )?;
        if asr_identity.is_none() {
            asr_identity = Some(snapshot.asr.clone());
        }
        commands.push(TranscribeJobCommand::from_snapshot(snapshot)?);
    }

    if member_snapshot_ids.len() != snapshots_by_id.len()
        || snapshots_by_id
            .keys()
            .any(|snapshot_id| !member_snapshot_ids.contains(snapshot_id))
    {
        return Err(invalid_resume(
            batch.id(),
            "Batch contains an unowned or unreferenced Snapshot",
        ));
    }

    let asr_spec = asr_identity
        .map(|identity| identity.to_runtime_spec())
        .ok_or_else(|| invalid_resume(batch.id(), "Batch has no resumable Jobs"))?;

    Ok(ValidatedResume { commands, asr_spec })
}

fn validate_asr_identity(
    batch_id: &BatchId,
    profile: &BatchExecutionProfile,
    expected: Option<&AsrExecutionSnapshot>,
    snapshot: &JobExecutionSnapshot,
) -> AppResult<()> {
    let asr = &snapshot.asr;
    if asr.engine != profile.asr_engine
        || asr.model_id.as_deref() != Some(profile.asr_model.as_str())
        || asr.device != profile.device
        || asr.compute_type != profile.compute_type
    {
        return Err(invalid_resume(
            batch_id,
            format!(
                "Snapshot {} ASR identity does not match BatchExecutionProfile",
                snapshot.snapshot_id
            ),
        ));
    }
    if let Some(expected) = expected {
        if asr.engine != expected.engine
            || asr.model_id != expected.model_id
            || asr.model_locator != expected.model_locator
            || asr.model_digest != expected.model_digest
            || asr.device != expected.device
            || asr.compute_type != expected.compute_type
        {
            return Err(invalid_resume(
                batch_id,
                format!(
                    "Snapshot {} ASR engine/model/locator/digest/device/compute differs from another member",
                    snapshot.snapshot_id
                ),
            ));
        }
    }
    Ok(())
}

fn invalid_resume(batch_id: &BatchId, message: impl Into<String>) -> ApplicationError {
    ApplicationError::Invalid(format!(
        "Batch {batch_id} resume validation failed: {}",
        message.into()
    ))
}

#[cfg(test)]
mod tests {
    use ulid::Ulid;
    use videocaptionerr_domain::BatchExecutionProfile;

    use super::*;
    use crate::execution_snapshot::{
        AudioStreamSelection, CacheExecutionSnapshot, OutputPlanSnapshot, SourceStatSnapshot,
        JOB_EXECUTION_SNAPSHOT_SCHEMA_VERSION,
    };
    use crate::ports::ModelLocator;

    fn graph() -> (
        Versioned<Batch>,
        Vec<Versioned<Job>>,
        Vec<JobExecutionSnapshot>,
    ) {
        let batch_id: BatchId = Ulid::new().into();
        let profile_revision: UlidStr = Ulid::new().into();
        let profile = BatchExecutionProfile {
            asr_engine: "fake".into(),
            asr_model: "tiny".into(),
            device: "cpu".into(),
            compute_type: "default".into(),
        };
        let mut jobs = Vec::new();
        let mut snapshots = Vec::new();
        let mut ids = Vec::new();
        for index in 0..2 {
            let job_id: videocaptionerr_domain::JobId = Ulid::new().into();
            let snapshot_id: UlidStr = Ulid::new().into();
            ids.push(job_id.clone());
            snapshots.push(JobExecutionSnapshot {
                snapshot_id: snapshot_id.clone(),
                schema_version: JOB_EXECUTION_SNAPSHOT_SCHEMA_VERSION,
                created_at: "2026-07-22T00:00:00Z".into(),
                job_id: job_id.clone(),
                batch_id: batch_id.clone(),
                canonical_source_path: format!("/media/{index}.wav"),
                source_stat: SourceStatSnapshot {
                    size: 10,
                    modified_at_ms: Some(1),
                },
                job_dir: format!("/jobs/{index}"),
                profile_revision: profile_revision.clone(),
                profile_name: Some("test".into()),
                asr: AsrExecutionSnapshot {
                    engine: "fake".into(),
                    model_locator: ModelLocator::file("fake:default"),
                    model_id: Some("tiny".into()),
                    model_digest: Some("digest-a".into()),
                    device: "cpu".into(),
                    compute_type: "default".into(),
                },
                audio_stream: AudioStreamSelection::Auto,
                source_language: Some("en".into()),
                target_language: None,
                output: OutputPlanSnapshot {
                    path: format!("/out/{index}.srt"),
                    format: "srt".into(),
                    layout: "source_only".into(),
                    conflict_policy: "fail".into(),
                    fallback_to_source: false,
                },
                cache: CacheExecutionSnapshot { max_bytes: 1 },
                llm: None,
            });
            jobs.push(Versioned::new(Job::new_with_snapshot(
                job_id,
                Some(batch_id.clone()),
                snapshot_id,
                profile_revision.clone(),
                format!("/media/{index}.wav"),
            )));
        }
        let batch = Versioned::new(Batch::new(batch_id, ids, profile).unwrap());
        (batch, jobs, snapshots)
    }

    #[test]
    fn resume_validates_the_complete_snapshot_graph() {
        let (batch, jobs, snapshots) = graph();
        let validated = validate_resume_graph(&batch, jobs, snapshots).unwrap();
        assert_eq!(validated.commands.len(), 2);
        assert_eq!(validated.asr_spec.engine_family, "fake");
        assert_eq!(validated.asr_spec.model_id, "tiny");
    }

    #[test]
    fn resume_rejects_second_job_with_different_engine() {
        let (batch, jobs, mut snapshots) = graph();
        snapshots[1].asr.engine = "whisper-cpp".into();
        assert!(validate_resume_graph(&batch, jobs, snapshots)
            .unwrap_err()
            .to_string()
            .contains("ASR identity"));
    }

    #[test]
    fn resume_rejects_second_job_with_different_model_digest() {
        let (batch, jobs, mut snapshots) = graph();
        snapshots[1].asr.model_digest = Some("digest-b".into());
        assert!(validate_resume_graph(&batch, jobs, snapshots)
            .unwrap_err()
            .to_string()
            .contains("ASR engine/model"));
    }

    #[test]
    fn resume_rejects_different_job_profile_revision() {
        let (batch, mut jobs, snapshots) = graph();
        jobs[1] = Versioned::new(Job::new_with_snapshot(
            jobs[1].id().clone(),
            Some(batch.id().clone()),
            snapshots[1].snapshot_id.clone(),
            Ulid::new().into(),
            jobs[1].source_path(),
        ));
        assert!(validate_resume_graph(&batch, jobs, snapshots)
            .unwrap_err()
            .to_string()
            .contains("profile revision"));
    }

    #[test]
    fn resume_rejects_snapshot_job_and_batch_mismatches() {
        let (batch, jobs, mut snapshots) = graph();
        snapshots[1].job_id = jobs[0].id().clone();
        assert!(validate_resume_graph(&batch, jobs.clone(), snapshots)
            .unwrap_err()
            .to_string()
            .contains("more than one Snapshot"));

        let (batch, jobs, mut snapshots) = graph();
        snapshots[1].batch_id = Ulid::new().into();
        assert!(validate_resume_graph(&batch, jobs, snapshots)
            .unwrap_err()
            .to_string()
            .contains("belongs to Batch"));
    }

    #[test]
    fn resume_rejects_missing_or_unowned_snapshot() {
        let (batch, jobs, mut snapshots) = graph();
        snapshots.pop();
        assert!(validate_resume_graph(&batch, jobs, snapshots)
            .unwrap_err()
            .to_string()
            .contains("Snapshot count"));

        let (batch, jobs, mut snapshots) = graph();
        snapshots[1].snapshot_id = Ulid::new().into();
        assert!(validate_resume_graph(&batch, jobs, snapshots)
            .unwrap_err()
            .to_string()
            .contains("missing"));
    }

    #[test]
    fn resume_rejects_unsupported_snapshot_schema() {
        let (batch, jobs, mut snapshots) = graph();
        snapshots[0].schema_version = 99;
        assert!(validate_resume_graph(&batch, jobs, snapshots)
            .unwrap_err()
            .to_string()
            .contains("unsupported execution snapshot schema"));
    }
}
