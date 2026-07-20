//! Small application services for resumable work and cache maintenance.

use std::sync::Arc;

use videocaptionerr_domain::{JobId, StageKind, WorkUnit};

use crate::application_error::{AppResult, ApplicationError};
use crate::ports::{CacheGcResult, CacheRepository, Clock, JobRepository, WorkUnitRepository};

#[derive(Debug, Clone)]
pub struct RetryFailedWorkUnitsCommand {
    pub job_id: JobId,
    pub from_stage: Option<StageKind>,
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryFailedWorkUnitsResponse {
    pub job_id: JobId,
    pub from_stage: Option<StageKind>,
    pub retryable_units: u32,
    pub retried_units: u32,
    pub dry_run: bool,
}

pub struct RetryFailedWorkUnits {
    jobs: Arc<dyn JobRepository>,
    work_units: Arc<dyn WorkUnitRepository>,
}

impl RetryFailedWorkUnits {
    pub fn new(jobs: Arc<dyn JobRepository>, work_units: Arc<dyn WorkUnitRepository>) -> Self {
        Self { jobs, work_units }
    }

    pub async fn execute(
        &self,
        command: RetryFailedWorkUnitsCommand,
    ) -> AppResult<RetryFailedWorkUnitsResponse> {
        let mut job = self.jobs.load_job(&command.job_id).await?.ok_or_else(|| {
            ApplicationError::Invalid(format!("Job {} not found", command.job_id))
        })?;
        let retryable_units = self
            .work_units
            .count_retryable(&command.job_id, command.from_stage)
            .await?;

        // Validate the aggregate transition even for a dry run. No state is
        // changed until this check succeeds.
        job.prepare_retry(command.from_stage)?;
        if command.dry_run {
            return Ok(RetryFailedWorkUnitsResponse {
                job_id: command.job_id,
                from_stage: command.from_stage,
                retryable_units,
                retried_units: 0,
                dry_run: true,
            });
        }

        let retried_units = self
            .work_units
            .retry_failed(&command.job_id, command.from_stage)
            .await?;
        let expected = job.expected_version();
        self.jobs.save_job(&mut job, expected).await?;
        Ok(RetryFailedWorkUnitsResponse {
            job_id: command.job_id,
            from_stage: command.from_stage,
            retryable_units,
            retried_units,
            dry_run: false,
        })
    }
}

#[derive(Debug, Clone)]
pub struct LeaseNextWorkUnitCommand {
    pub job_id: JobId,
    pub stage: StageKind,
    pub owner: String,
    pub lease_ms: u64,
}

pub struct WorkUnitScheduler {
    work_units: Arc<dyn WorkUnitRepository>,
    clock: Arc<dyn Clock>,
}

impl WorkUnitScheduler {
    pub fn new(work_units: Arc<dyn WorkUnitRepository>, clock: Arc<dyn Clock>) -> Self {
        Self { work_units, clock }
    }

    pub async fn lease_next(
        &self,
        command: LeaseNextWorkUnitCommand,
    ) -> AppResult<Option<WorkUnit>> {
        self.work_units
            .lease_next_ready(
                &command.job_id,
                command.stage,
                &command.owner,
                self.clock.now_ms(),
                command.lease_ms,
            )
            .await
            .map(|unit| unit.map(|unit| unit.value))
    }

    pub async fn recover_expired(&self) -> AppResult<u32> {
        self.work_units.recover_expired(self.clock.now_ms()).await
    }
}

#[derive(Clone)]
pub struct CacheGc {
    cache: Arc<dyn CacheRepository>,
}

impl CacheGc {
    pub fn new(cache: Arc<dyn CacheRepository>) -> Self {
        Self { cache }
    }

    pub async fn execute(&self, max_bytes: u64) -> AppResult<CacheGcResult> {
        self.cache.gc(max_bytes).await
    }
}
