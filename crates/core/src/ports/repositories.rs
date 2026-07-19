use async_trait::async_trait;
use videocaptionerr_domain::{
    Batch, BatchId, DomainResult, Job, JobId, StageKind, WorkUnit, WorkUnitId,
};

use crate::application_error::AppResult;

#[async_trait]
pub trait BatchRepository: Send + Sync {
    async fn load_batch(&self, id: &BatchId) -> AppResult<Option<Batch>>;
    async fn save_batch(&self, batch: &Batch) -> AppResult<()>;
}

#[async_trait]
pub trait JobRepository: Send + Sync {
    async fn load_job(&self, id: &JobId) -> AppResult<Option<Job>>;
    async fn save_job(&self, job: &Job) -> AppResult<()>;
    async fn delete_job(&self, id: &JobId) -> AppResult<()>;
    async fn list_jobs(&self) -> AppResult<Vec<Job>>;
}

#[async_trait]
pub trait WorkUnitRepository: Send + Sync {
    async fn load_work_unit(&self, id: &WorkUnitId) -> AppResult<Option<WorkUnit>>;
    async fn find_work_unit(
        &self,
        job_id: &JobId,
        stage: StageKind,
        unit_kind: &str,
        unit_index: u32,
        input_hash: &str,
    ) -> AppResult<Option<WorkUnit>>;
    async fn save_work_unit(&self, unit: &WorkUnit) -> AppResult<()>;
    async fn recover_expired(&self, now_ms: u64) -> AppResult<u32>;
    async fn count_retryable(
        &self,
        job_id: &JobId,
        from_stage: Option<StageKind>,
    ) -> AppResult<u32>;
    async fn lease_next_ready(
        &self,
        job_id: &JobId,
        stage: StageKind,
        owner: &str,
        now_ms: u64,
        lease_ms: u64,
    ) -> AppResult<Option<WorkUnit>>;
    async fn retry_failed(&self, job_id: &JobId, from_stage: Option<StageKind>) -> AppResult<u32>;
}

pub fn validate_loaded<T>(value: Option<T>, name: &str) -> DomainResult<T> {
    value.ok_or_else(|| {
        videocaptionerr_domain::DomainError::InvalidArgument(format!("{name} not found"))
    })
}
