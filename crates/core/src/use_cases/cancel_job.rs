//! Batch/Job cancellation application commands.

use std::sync::Arc;

use videocaptionerr_domain::{BatchId, JobId, JobStatus, WorkUnitStatus};

use crate::application_error::{AppResult, ApplicationError};
use crate::ports::{
    ActiveRunRegistry, AsrCancelToken, BatchRepository, JobRepository, OutboxEvent,
    WorkUnitRepository,
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
    /// Present only when this process owns the active Job/Batch. A control
    /// process without the Owner relies on the durable cancellation intent.
    pub token: Option<AsrCancelToken>,
}

/// Request Job cancellation. Cooperative cancel of the active WorkUnit is
/// signaled through the returned token; the Job becomes Cancelled only after
/// in-flight units settle.
pub struct CancelJob {
    jobs: Arc<dyn JobRepository>,
    work_units: Arc<dyn WorkUnitRepository>,
    active_runs: Option<Arc<dyn ActiveRunRegistry>>,
}

impl CancelJob {
    pub fn new(jobs: Arc<dyn JobRepository>, work_units: Arc<dyn WorkUnitRepository>) -> Self {
        Self {
            jobs,
            work_units,
            active_runs: None,
        }
    }

    pub fn with_active_runs(mut self, active_runs: Arc<dyn ActiveRunRegistry>) -> Self {
        self.active_runs = Some(active_runs);
        self
    }

    pub async fn execute(&self, command: CancelJobCommand) -> AppResult<CancelResponse> {
        let active_token = self
            .active_runs
            .as_ref()
            .map(|registry| registry.cancel_job(&command.job_id))
            .transpose()?
            .flatten();
        let mut job = self.jobs.load_job(&command.job_id).await?.ok_or_else(|| {
            ApplicationError::Invalid(format!("Job {} not found", command.job_id))
        })?;
        if job.status().is_terminal() && job.status() != JobStatus::Cancelled {
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
        let was_cancelled = job.status() == JobStatus::Cancelled;
        if job.status() == JobStatus::Pending {
            job.cancel()?;
            let expected = job.expected_version();
            self.jobs.save_job(&mut job, expected).await?;
        } else if job.status() == JobStatus::Running && !job.cancel_requested() {
            // Running state remains durable until its owner reaches a safe
            // boundary; this avoids racing a stage CAS with a fake terminal
            // write while still making cross-process cancel observable.
            job.request_cancel()?;
            let expected = job.expected_version();
            self.jobs.save_job(&mut job, expected).await?;
        }
        if let Some(token) = &active_token {
            token.request();
        }
        Ok(CancelResponse {
            cancel_requested: !was_cancelled,
            job_id: Some(command.job_id),
            batch_id: job.batch_id().cloned(),
            token: active_token,
        })
    }
}

/// Request Batch cancellation. The Batch becomes Cancelled only after every
/// member Job is terminal.
pub struct CancelBatch {
    batches: Arc<dyn BatchRepository>,
    jobs: Arc<dyn JobRepository>,
    active_runs: Option<Arc<dyn ActiveRunRegistry>>,
}

impl CancelBatch {
    pub fn new(batches: Arc<dyn BatchRepository>, jobs: Arc<dyn JobRepository>) -> Self {
        Self {
            batches,
            jobs,
            active_runs: None,
        }
    }

    pub fn with_active_runs(mut self, active_runs: Arc<dyn ActiveRunRegistry>) -> Self {
        self.active_runs = Some(active_runs);
        self
    }

    pub async fn execute(&self, command: CancelBatchCommand) -> AppResult<CancelResponse> {
        let active_token = self
            .active_runs
            .as_ref()
            .map(|registry| registry.cancel_batch(&command.batch_id))
            .transpose()?
            .flatten();
        let mut batch = self
            .batches
            .load_batch(&command.batch_id)
            .await?
            .ok_or_else(|| {
                ApplicationError::Invalid(format!("Batch {} not found", command.batch_id))
            })?;
        let terminal = batch.status().is_terminal();
        let was_requested = batch.cancel_requested() || terminal;
        if !terminal && !was_requested {
            batch.request_cancel()?;
            let expected = batch.expected_version();
            self.batches.save_batch(&mut batch, expected).await?;
        }

        // Cancellation is a control operation, so a Pending/Paused Batch is
        // moved into the same Running aggregate path before terminal member
        // records are applied. The live owner is still the only one allowed
        // to execute work; this merely makes the state transition legal.
        match batch.status() {
            videocaptionerr_domain::BatchStatus::Pending => batch.start()?,
            videocaptionerr_domain::BatchStatus::Paused => batch.resume()?,
            videocaptionerr_domain::BatchStatus::Running
            | videocaptionerr_domain::BatchStatus::Done
            | videocaptionerr_domain::BatchStatus::Failed
            | videocaptionerr_domain::BatchStatus::Cancelled => {}
        }
        if batch.status() == videocaptionerr_domain::BatchStatus::Running {
            let expected = batch.expected_version();
            self.batches.save_batch(&mut batch, expected).await?;
        }

        // If every Job is already terminal, finish the Batch as Cancelled now.
        let job_ids: Vec<_> = batch.job_ids().to_vec();
        let mut all_terminal = true;
        let mut terminals = Vec::new();
        for job_id in &job_ids {
            let Some(mut job) = self.jobs.load_job(job_id).await? else {
                all_terminal = false;
                break;
            };
            if !job.status().is_terminal() {
                if job.status() == JobStatus::Pending {
                    job.cancel()?;
                } else {
                    job.request_cancel()?;
                }
                let expected = job.expected_version();
                self.jobs.save_job(&mut job, expected).await?;
                if !job.status().is_terminal() {
                    all_terminal = false;
                    continue;
                }
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
        if all_terminal
            && matches!(
                batch.status(),
                videocaptionerr_domain::BatchStatus::Running
                    | videocaptionerr_domain::BatchStatus::Paused
                    | videocaptionerr_domain::BatchStatus::Pending
            )
        {
            let _event = batch.finish_cancelled()?;
            let expected = batch.expected_version();
            self.batches.save_batch(&mut batch, expected).await?;
        }

        Ok(CancelResponse {
            cancel_requested: !was_requested,
            job_id: None,
            batch_id: Some(command.batch_id),
            token: active_token,
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
