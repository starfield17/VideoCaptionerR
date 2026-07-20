//! Batch/Job cancellation application commands.

use std::sync::Arc;

use videocaptionerr_domain::{BatchId, JobId, JobStatus, WorkUnitStatus};

use crate::application_error::{AppResult, ApplicationError};
use crate::ports::{
    AsrCancelToken, BatchRepository, JobRepository, OutboxEvent, WorkUnitRepository,
};

#[derive(Debug, Clone)]
pub struct CancelJobCommand {
    pub job_id: JobId,
}

#[derive(Debug, Clone)]
pub struct CancelBatchCommand {
    pub batch_id: BatchId,
}

#[derive(Debug, Clone)]
pub struct CancelResponse {
    pub cancel_requested: bool,
    pub job_id: Option<JobId>,
    pub batch_id: Option<BatchId>,
    pub token: AsrCancelToken,
}

/// Request Job cancellation. Cooperative cancel of the active WorkUnit is
/// signaled through the returned token; the Job becomes Cancelled only after
/// in-flight units settle.
pub struct CancelJob {
    jobs: Arc<dyn JobRepository>,
    work_units: Arc<dyn WorkUnitRepository>,
}

impl CancelJob {
    pub fn new(jobs: Arc<dyn JobRepository>, work_units: Arc<dyn WorkUnitRepository>) -> Self {
        Self { jobs, work_units }
    }

    pub async fn execute(&self, command: CancelJobCommand) -> AppResult<CancelResponse> {
        let mut job = self.jobs.load_job(&command.job_id).await?.ok_or_else(|| {
            ApplicationError::Invalid(format!("Job {} not found", command.job_id))
        })?;
        if job.status().is_terminal() {
            return Err(ApplicationError::Invalid(format!(
                "Job {} is already {:?}",
                command.job_id,
                job.status()
            )));
        }
        // Stop new work: mark non-terminal units Cancelled when they are only
        // Pending. Running units receive cooperative cancel via the token.
        let units = self.work_units.list_for_job(&command.job_id).await?;
        for mut unit in units {
            if unit.status() == WorkUnitStatus::Pending {
                let expected = unit.expected_version();
                unit.cancel()?;
                self.work_units.save_work_unit(&mut unit, expected).await?;
            }
        }
        if job.status() == JobStatus::Pending {
            job.cancel()?;
            let expected = job.expected_version();
            self.jobs.save_job(&mut job, expected).await?;
        }
        let token = AsrCancelToken::new();
        token.request();
        Ok(CancelResponse {
            cancel_requested: true,
            job_id: Some(command.job_id),
            batch_id: job.batch_id().cloned(),
            token,
        })
    }
}

/// Request Batch cancellation. The Batch becomes Cancelled only after every
/// member Job is terminal.
pub struct CancelBatch {
    batches: Arc<dyn BatchRepository>,
    jobs: Arc<dyn JobRepository>,
}

impl CancelBatch {
    pub fn new(batches: Arc<dyn BatchRepository>, jobs: Arc<dyn JobRepository>) -> Self {
        Self { batches, jobs }
    }

    pub async fn execute(&self, command: CancelBatchCommand) -> AppResult<CancelResponse> {
        let mut batch = self
            .batches
            .load_batch(&command.batch_id)
            .await?
            .ok_or_else(|| {
                ApplicationError::Invalid(format!("Batch {} not found", command.batch_id))
            })?;
        batch.request_cancel()?;
        let expected = batch.expected_version();
        self.batches.save_batch(&mut batch, expected).await?;

        let token = AsrCancelToken::new();
        token.request();
        // If every Job is already terminal, finish the Batch as Cancelled now.
        let job_ids: Vec<_> = batch.job_ids().to_vec();
        let mut all_terminal = true;
        let mut terminals = Vec::new();
        for job_id in &job_ids {
            let Some(job) = self.jobs.load_job(job_id).await? else {
                all_terminal = false;
                break;
            };
            if !job.status().is_terminal() {
                all_terminal = false;
                break;
            }
            if !batch.has_terminal_record(job_id) {
                let terminal = match job.status() {
                    JobStatus::Done => videocaptionerr_domain::JobTerminalStatus::Done,
                    JobStatus::DoneDegraded => {
                        videocaptionerr_domain::JobTerminalStatus::DoneDegraded
                    }
                    JobStatus::Failed => videocaptionerr_domain::JobTerminalStatus::Failed,
                    JobStatus::Cancelled => videocaptionerr_domain::JobTerminalStatus::Cancelled,
                    _ => continue,
                };
                terminals.push((job_id.clone(), terminal));
            }
        }
        for (job_id, terminal) in terminals {
            batch.record_job_terminal(&job_id, terminal)?;
        }
        if all_terminal && batch.status() == videocaptionerr_domain::BatchStatus::Running {
            let _event = batch.finish_cancelled()?;
            let expected = batch.expected_version();
            self.batches.save_batch(&mut batch, expected).await?;
        }

        Ok(CancelResponse {
            cancel_requested: true,
            job_id: None,
            batch_id: Some(command.batch_id),
            token,
        })
    }
}

/// Helper to emit a durable cancel outbox marker (optional for callers).
pub fn cancel_outbox_event(aggregate_type: &str, aggregate_id: &str) -> OutboxEvent {
    OutboxEvent {
        aggregate_type: aggregate_type.into(),
        aggregate_id: aggregate_id.into(),
        event_type: "cancel_requested".into(),
        payload_json: serde_json::json!({ "aggregate_id": aggregate_id }).to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
    }
}
