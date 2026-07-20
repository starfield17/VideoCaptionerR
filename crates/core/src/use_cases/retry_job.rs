//! Explicit Job retry/resume planning and atomic application.

use std::sync::Arc;

use videocaptionerr_domain::{
    Batch, BatchId, Job, JobId, JobStatus, StageKind, StageStatus, WorkUnit, WorkUnitStatus,
};

use crate::application_error::{AppResult, ApplicationError};
use crate::ports::{
    BatchRepository, ExpectedVersion, JobRepository, OutboxEvent, RetryTransactionRepository,
    RetryTransactionRequest, SnapshotRepository, Versioned, WorkUnitRepository,
};
use crate::use_cases::TranscribeJobCommand;

#[derive(Debug, Clone)]
pub struct RetryJobCommand {
    pub job_id: JobId,
    pub from_stage: Option<StageKind>,
    pub dry_run: bool,
}

/// Dry-run / applied plan for an explicit Job retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPlan {
    pub job_id: JobId,
    pub batch_id: Option<BatchId>,
    pub start_stage: StageKind,
    pub reused_artifacts: Vec<StageKind>,
    pub invalidated_stages: Vec<StageKind>,
    pub work_units_to_reset: Vec<String>,
    pub output_path: String,
    pub needs_runtime: bool,
    pub dry_run: bool,
}

pub struct RetryJobResponse {
    pub plan: RetryPlan,
    pub command: Option<TranscribeJobCommand>,
    pub batch: Option<Batch>,
    pub job: Option<Job>,
}

pub struct RetryJob {
    jobs: Arc<dyn JobRepository>,
    batches: Arc<dyn BatchRepository>,
    work_units: Arc<dyn WorkUnitRepository>,
    snapshots: Arc<dyn SnapshotRepository>,
    retry_tx: Arc<dyn RetryTransactionRepository>,
}

impl RetryJob {
    pub fn new(
        jobs: Arc<dyn JobRepository>,
        batches: Arc<dyn BatchRepository>,
        work_units: Arc<dyn WorkUnitRepository>,
        snapshots: Arc<dyn SnapshotRepository>,
        retry_tx: Arc<dyn RetryTransactionRepository>,
    ) -> Self {
        Self {
            jobs,
            batches,
            work_units,
            snapshots,
            retry_tx,
        }
    }

    pub async fn execute(&self, command: RetryJobCommand) -> AppResult<RetryJobResponse> {
        let mut job = self.jobs.load_job(&command.job_id).await?.ok_or_else(|| {
            ApplicationError::Invalid(format!("Job {} not found", command.job_id))
        })?;
        if !matches!(
            job.status(),
            JobStatus::Failed | JobStatus::DoneDegraded | JobStatus::Cancelled
        ) {
            return Err(ApplicationError::Invalid(format!(
                "Job {} is {:?}; only Failed/DoneDegraded/Cancelled Jobs can be retried",
                command.job_id,
                job.status()
            )));
        }

        let snapshot_id = job.execution_snapshot_id().cloned().ok_or_else(|| {
            ApplicationError::Invalid(format!(
                "Job {} has no execution snapshot and cannot be retried",
                command.job_id
            ))
        })?;
        let snapshot = self
            .snapshots
            .load_execution_snapshot(&snapshot_id)
            .await?
            .ok_or_else(|| {
                ApplicationError::Invalid(format!(
                    "execution snapshot {snapshot_id} not found for Job {}",
                    command.job_id
                ))
            })?;

        let start_stage = resolve_start_stage(&job, command.from_stage)?;
        let (reused, invalidated) = partition_stages(&job, start_stage);
        let units = self.list_job_work_units(&command.job_id).await?;
        let work_units_to_reset: Vec<(Versioned<WorkUnit>, ExpectedVersion)> = units
            .into_iter()
            .filter(|unit| stage_rank(unit.stage()) >= stage_rank(start_stage))
            .map(|mut unit| {
                let expected = unit.expected_version();
                match unit.status() {
                    WorkUnitStatus::Failed | WorkUnitStatus::Cancelled => {
                        unit.retry()?;
                    }
                    WorkUnitStatus::Done => {
                        // Invalidate committed units that belong to stages we
                        // are about to re-execute.
                        unit.invalidate_artifact_for_recovery("RETRY_INVALIDATED")?;
                    }
                    WorkUnitStatus::Pending => {}
                    WorkUnitStatus::Running => {
                        return Err(ApplicationError::Invalid(format!(
                            "WorkUnit {} is still Running; recover leases before retry",
                            unit.id()
                        )));
                    }
                }
                Ok::<_, ApplicationError>((unit, expected))
            })
            .collect::<AppResult<_>>()?;

        let needs_runtime = invalidated.iter().any(|stage| {
            matches!(
                stage,
                StageKind::Asr
                    | StageKind::Split
                    | StageKind::Correct
                    | StageKind::Translate
                    | StageKind::Export
                    | StageKind::Probe
                    | StageKind::ExtractAudio
            )
        }) || matches!(
            start_stage,
            StageKind::Asr
                | StageKind::Split
                | StageKind::Correct
                | StageKind::Translate
                | StageKind::Export
                | StageKind::Probe
                | StageKind::ExtractAudio
        );

        let plan = RetryPlan {
            job_id: command.job_id.clone(),
            batch_id: job.batch_id().cloned(),
            start_stage,
            reused_artifacts: reused,
            invalidated_stages: invalidated.clone(),
            work_units_to_reset: work_units_to_reset
                .iter()
                .map(|(unit, _)| unit.id().to_string())
                .collect(),
            output_path: snapshot.output.path.clone(),
            needs_runtime,
            dry_run: command.dry_run,
        };

        if command.dry_run {
            // Dry-run validates transitions in memory only. No database write
            // and no adapter side effects are permitted.
            let mut preview = job.value.clone();
            preview.prepare_retry(command.from_stage)?;
            return Ok(RetryJobResponse {
                plan,
                command: None,
                batch: None,
                job: None,
            });
        }

        let job_expected = job.expected_version();
        let start = job.prepare_retry(command.from_stage)?;
        debug_assert_eq!(start, start_stage);

        let batch_update = if let Some(batch_id) = job.batch_id().cloned() {
            let mut batch = self.batches.load_batch(&batch_id).await?.ok_or_else(|| {
                ApplicationError::Invalid(format!(
                    "Batch {batch_id} not found for Job {}",
                    command.job_id
                ))
            })?;
            let expected = batch.expected_version();
            batch.prepare_retry(&command.job_id)?;
            Some((batch, expected))
        } else {
            None
        };

        let event = OutboxEvent {
            aggregate_type: "Job".into(),
            aggregate_id: command.job_id.to_string(),
            event_type: "job_retry_prepared".into(),
            payload_json: serde_json::json!({
                "job_id": command.job_id.to_string(),
                "start_stage": start_stage.as_str(),
                "retry_generation": job.retry_generation(),
                "invalidated_stages": invalidated
                    .iter()
                    .map(|stage| stage.as_str())
                    .collect::<Vec<_>>(),
            })
            .to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        let result = self
            .retry_tx
            .apply_retry(RetryTransactionRequest {
                batch: batch_update,
                job: (job, job_expected),
                work_units: work_units_to_reset,
                event,
            })
            .await?;

        let transcribe_command = TranscribeJobCommand::from_snapshot(&snapshot)?;
        Ok(RetryJobResponse {
            plan,
            command: Some(transcribe_command),
            batch: result.batch.map(|batch| batch.value),
            job: Some(result.job.value),
        })
    }

    async fn list_job_work_units(&self, job_id: &JobId) -> AppResult<Vec<Versioned<WorkUnit>>> {
        // WorkUnitRepository has no list-by-job port yet. Use retryable count
        // helpers via a best-effort scan of known units through count/retry
        // paths is insufficient, so load by attempting to find none and rely
        // on the repository extension below when available.
        self.work_units.list_for_job(job_id).await
    }
}

fn resolve_start_stage(job: &Job, from_stage: Option<StageKind>) -> AppResult<StageKind> {
    match from_stage {
        Some(kind) => Ok(kind),
        None => job
            .stages()
            .iter()
            .find(|stage| {
                matches!(
                    stage.status,
                    StageStatus::Failed | StageStatus::Cancelled | StageStatus::WaitingProvider
                )
            })
            .map(|stage| stage.kind)
            .ok_or_else(|| {
                ApplicationError::Invalid(
                    "prepare_retry requires from_stage when no failed stage is present".into(),
                )
            }),
    }
}

fn partition_stages(job: &Job, start: StageKind) -> (Vec<StageKind>, Vec<StageKind>) {
    let start_rank = stage_rank(start);
    let mut reused = Vec::new();
    let mut invalidated = Vec::new();
    for stage in job.stages() {
        if stage_rank(stage.kind) < start_rank
            && matches!(
                stage.status,
                StageStatus::Done | StageStatus::DoneDegraded | StageStatus::Skipped
            )
        {
            reused.push(stage.kind);
        } else if stage_rank(stage.kind) >= start_rank {
            invalidated.push(stage.kind);
        }
    }
    (reused, invalidated)
}

fn stage_rank(kind: StageKind) -> u8 {
    match kind {
        StageKind::Probe => 0,
        StageKind::ExtractAudio => 1,
        StageKind::Asr => 2,
        StageKind::Split => 3,
        StageKind::Correct => 4,
        StageKind::Translate => 5,
        StageKind::Export => 6,
    }
}
