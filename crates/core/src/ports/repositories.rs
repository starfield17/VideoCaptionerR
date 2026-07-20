use async_trait::async_trait;
use videocaptionerr_domain::{
    Batch, BatchId, DomainResult, Job, JobId, StageKind, WorkUnit, WorkUnitId,
};

use crate::application_error::AppResult;
use crate::execution_snapshot::JobExecutionSnapshot;
use crate::ports::{OutboxEvent, PreparedArtifact};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Versioned<T> {
    pub value: T,
    pub version: u64,
}

impl<T> Versioned<T> {
    pub fn new(value: T) -> Self {
        Self { value, version: 0 }
    }

    pub fn with_version(value: T, version: u64) -> Self {
        Self { value, version }
    }

    pub fn expected_version(&self) -> ExpectedVersion {
        ExpectedVersion::Exact(self.version)
    }
}

impl<T> std::ops::Deref for Versioned<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> std::ops::DerefMut for Versioned<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedVersion {
    New,
    Exact(u64),
}

#[async_trait]
pub trait BatchRepository: Send + Sync {
    async fn load_batch(&self, id: &BatchId) -> AppResult<Option<Versioned<Batch>>>;
    async fn list_batches(&self) -> AppResult<Vec<Versioned<Batch>>>;
    async fn save_batch(
        &self,
        batch: &mut Versioned<Batch>,
        expected: ExpectedVersion,
    ) -> AppResult<()>;
}

#[derive(Debug, Clone)]
pub struct StageCommitRequest {
    pub job: Option<(Versioned<Job>, ExpectedVersion)>,
    pub work_unit: Option<(Versioned<WorkUnit>, ExpectedVersion)>,
    pub artifact: Option<PreparedArtifact>,
    pub event: Option<OutboxEvent>,
}

#[derive(Debug, Clone, Default)]
pub struct StageCommitResult {
    pub job: Option<Versioned<Job>>,
    pub work_unit: Option<Versioned<WorkUnit>>,
}

/// The application-owned consistency boundary for a successful stage or
/// WorkUnit result. Implementations publish the file first, then atomically
/// persist metadata, aggregate CAS updates, and the durable outbox event.
#[async_trait]
pub trait StageCommitRepository: Send + Sync {
    async fn commit_stage(&self, request: StageCommitRequest) -> AppResult<StageCommitResult>;
}

/// One SQLite transaction that CAS-updates every aggregate touched by an
/// explicit Job retry. Callers must not first retry WorkUnits and then save
/// Job/Batch separately.
#[derive(Debug, Clone)]
pub struct RetryTransactionRequest {
    pub batch: Option<(Versioned<Batch>, ExpectedVersion)>,
    pub job: (Versioned<Job>, ExpectedVersion),
    pub work_units: Vec<(Versioned<WorkUnit>, ExpectedVersion)>,
    pub event: OutboxEvent,
}

#[derive(Debug, Clone)]
pub struct RetryTransactionResult {
    pub batch: Option<Versioned<Batch>>,
    pub job: Versioned<Job>,
    pub work_units: Vec<Versioned<WorkUnit>>,
}

#[async_trait]
pub trait RetryTransactionRepository: Send + Sync {
    async fn apply_retry(
        &self,
        request: RetryTransactionRequest,
    ) -> AppResult<RetryTransactionResult>;
}

#[async_trait]
pub trait JobRepository: Send + Sync {
    async fn load_job(&self, id: &JobId) -> AppResult<Option<Versioned<Job>>>;
    async fn save_job(&self, job: &mut Versioned<Job>, expected: ExpectedVersion) -> AppResult<()>;
    async fn delete_job(&self, id: &JobId) -> AppResult<()>;
    async fn list_jobs(&self) -> AppResult<Vec<Versioned<Job>>>;
}

#[async_trait]
pub trait WorkUnitRepository: Send + Sync {
    async fn load_work_unit(&self, id: &WorkUnitId) -> AppResult<Option<Versioned<WorkUnit>>>;
    async fn find_work_unit(
        &self,
        job_id: &JobId,
        stage: StageKind,
        unit_kind: &str,
        unit_index: u32,
        input_hash: &str,
    ) -> AppResult<Option<Versioned<WorkUnit>>>;
    async fn save_work_unit(
        &self,
        unit: &mut Versioned<WorkUnit>,
        expected: ExpectedVersion,
    ) -> AppResult<()>;
    async fn recover_expired(&self, now_ms: u64) -> AppResult<u32>;
    async fn count_retryable(
        &self,
        job_id: &JobId,
        from_stage: Option<StageKind>,
    ) -> AppResult<u32>;
    async fn list_for_job(&self, job_id: &JobId) -> AppResult<Vec<Versioned<WorkUnit>>>;
    async fn lease_next_ready(
        &self,
        job_id: &JobId,
        stage: StageKind,
        owner: &str,
        now_ms: u64,
        lease_ms: u64,
    ) -> AppResult<Option<Versioned<WorkUnit>>>;
    async fn retry_failed(&self, job_id: &JobId, from_stage: Option<StageKind>) -> AppResult<u32>;
}

#[async_trait]
pub trait SnapshotRepository: Send + Sync {
    async fn load_execution_snapshot(
        &self,
        id: &videocaptionerr_domain::UlidStr,
    ) -> AppResult<Option<JobExecutionSnapshot>>;
    async fn save_execution_snapshot(&self, snapshot: &JobExecutionSnapshot) -> AppResult<()>;
    async fn load_snapshots_for_batch(&self, id: &BatchId) -> AppResult<Vec<JobExecutionSnapshot>>;
}

pub fn validate_loaded<T>(value: Option<T>, name: &str) -> DomainResult<T> {
    value.ok_or_else(|| {
        videocaptionerr_domain::DomainError::InvalidArgument(format!("{name} not found"))
    })
}
