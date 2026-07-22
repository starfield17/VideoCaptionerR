//! Rebuild a durable Batch execution from its immutable Job snapshots.

use std::sync::Arc;

use videocaptionerr_domain::{BatchId, BatchStatus};

use crate::application_error::{AppResult, ApplicationError};
use crate::ports::{BatchRepository, JobRepository, SnapshotRepository};

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

        let mut commands = Vec::with_capacity(batch.job_ids().len());
        let mut asr_spec = None;
        for job_id in batch.job_ids() {
            let job = self.jobs.load_job(job_id).await?.ok_or_else(|| {
                ApplicationError::Invalid(format!("Job {job_id} not found for Batch {batch_id}"))
            })?;
            let snapshot_id = job.execution_snapshot_id().cloned().ok_or_else(|| {
                ApplicationError::Invalid(format!(
                    "Job {job_id} has no execution snapshot and cannot resume"
                ))
            })?;
            let snapshot = self
                .snapshots
                .load_execution_snapshot(&snapshot_id)
                .await?
                .ok_or_else(|| {
                    ApplicationError::Invalid(format!(
                        "execution snapshot {snapshot_id} not found for Job {job_id}"
                    ))
                })?;
            if snapshot.batch_id != batch_id {
                return Err(ApplicationError::Invalid(format!(
                    "execution snapshot {snapshot_id} belongs to another Batch"
                )));
            }
            let command = TranscribeJobCommand::from_snapshot(&snapshot)?;
            if asr_spec.is_none() {
                asr_spec = Some(snapshot.asr_runtime_spec());
            }
            commands.push(command);
        }

        let asr_spec = asr_spec.ok_or_else(|| {
            ApplicationError::Invalid(format!("Batch {batch_id} has no resumable Jobs"))
        })?;
        if batch.status() == BatchStatus::Paused {
            return Err(ApplicationError::Invalid(
                "paused Batch must be resumed before execution".into(),
            ));
        }
        self.run_batch
            .execute(RunBatchCommand {
                batch: batch.value,
                jobs: commands,
                asr_spec,
            })
            .await
    }
}
