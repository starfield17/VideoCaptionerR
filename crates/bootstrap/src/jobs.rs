use std::path::PathBuf;

use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::application_error::ApplicationError;
use videocaptionerr_core::chunking::ChunkPlan;
use videocaptionerr_core::use_cases::{
    CancelBatch, CancelBatchCommand, CancelJob, CancelJobCommand, CancelResponse, RetryJobCommand,
    RetryJobResponse, RetryPlan, RunBatchCommand, RunBatchResponse,
};
use videocaptionerr_core::CacheGcResult;
use videocaptionerr_domain::{ArtifactRef, Job, JobId, JobStatus, StageStatus};

use crate::dto::{JobSummary, StageSummary};
use crate::runtime::ApplicationRuntime;

impl ApplicationRuntime {
    pub async fn list_jobs(&self) -> Result<Vec<Job>, VcError> {
        self.jobs
            .list_jobs()
            .await
            .map(|jobs| jobs.into_iter().map(|job| job.value).collect())
            .map_err(ApplicationError::into_vc_error)
    }

    pub async fn list_job_summaries(&self) -> VcResult<Vec<JobSummary>> {
        self.list_jobs()
            .await
            .map(|jobs| jobs.iter().map(job_summary).collect())
    }

    pub async fn remove_job(&self, id: &str) -> VcResult<()> {
        let job_id: JobId = id.parse().map_err(|error| {
            VcError::new(
                ErrorCode::InvalidArgument,
                format!("invalid Job id: {error}"),
            )
        })?;
        self.jobs
            .delete_job(&job_id)
            .await
            .map_err(ApplicationError::into_vc_error)
    }

    pub async fn cancel_job(&self, id: &str) -> VcResult<CancelResponse> {
        let job_id: JobId = id.parse().map_err(|error| {
            VcError::new(
                ErrorCode::InvalidArgument,
                format!("invalid Job id: {error}"),
            )
        })?;
        CancelJob::new(self.jobs.clone(), self.work_units.clone())
            .execute(CancelJobCommand { job_id })
            .await
            .map_err(ApplicationError::into_vc_error)
    }

    pub async fn cancel_batch(&self, id: &str) -> VcResult<CancelResponse> {
        let batch_id: videocaptionerr_domain::BatchId = id.parse().map_err(|error| {
            VcError::new(
                ErrorCode::InvalidArgument,
                format!("invalid Batch id: {error}"),
            )
        })?;
        CancelBatch::new(self.batches.clone(), self.jobs.clone())
            .execute(CancelBatchCommand { batch_id })
            .await
            .map_err(ApplicationError::into_vc_error)
    }

    /// Plan and, unless dry-run, fully execute a Job retry to a terminal state.
    pub async fn retry_job(
        &self,
        id: &str,
        from_stage: Option<&str>,
        dry_run: bool,
    ) -> VcResult<RetryJobOutcome> {
        let job_id: JobId = id.parse().map_err(|error| {
            VcError::new(
                ErrorCode::InvalidArgument,
                format!("invalid Job id: {error}"),
            )
        })?;
        let stage = from_stage
            .map(|value| {
                videocaptionerr_domain::StageKind::parse(value).ok_or_else(|| {
                    VcError::new(
                        ErrorCode::InvalidArgument,
                        format!("unknown stage '{value}'"),
                    )
                })
            })
            .transpose()?;

        let prepared = self
            .retry_job_uc
            .execute(RetryJobCommand {
                job_id: job_id.clone(),
                from_stage: stage,
                dry_run,
            })
            .await
            .map_err(ApplicationError::into_vc_error)?;

        if dry_run {
            return Ok(RetryJobOutcome::DryRun(prepared.plan));
        }

        let command = prepared.command.ok_or_else(|| {
            VcError::new(
                ErrorCode::Internal,
                "retry plan did not produce a TranscribeJobCommand",
            )
        })?;
        let batch = prepared.batch.ok_or_else(|| {
            VcError::new(
                ErrorCode::InvalidArgument,
                format!("Job {job_id} has no Batch and cannot open an ASR session"),
            )
        })?;

        // One open, execute the reopened Job, finish Batch, one close.
        let result = self
            .run_batch
            .execute(RunBatchCommand {
                batch,
                jobs: vec![command],
            })
            .await
            .map_err(ApplicationError::into_vc_error)?;
        Ok(RetryJobOutcome::Executed {
            plan: prepared.plan,
            result,
        })
    }

    pub async fn gc_cache(&self, max_bytes: u64) -> VcResult<CacheGcResult> {
        self.cache_gc
            .execute(max_bytes)
            .await
            .map_err(ApplicationError::into_vc_error)
    }

    pub async fn recover_expired_work_units(&self) -> VcResult<u32> {
        self.scheduler
            .recover_expired()
            .await
            .map_err(ApplicationError::into_vc_error)
    }

    pub async fn persist_chunk_plan(
        &self,
        job_id: &str,
        path: PathBuf,
        plan: ChunkPlan,
    ) -> VcResult<ArtifactRef> {
        let job_id: JobId = job_id.parse().map_err(|error| {
            VcError::new(
                ErrorCode::InvalidArgument,
                format!("invalid Job id: {error}"),
            )
        })?;
        self.chunk_plans
            .execute(job_id, path, plan)
            .await
            .map_err(ApplicationError::into_vc_error)
    }
}

pub enum RetryJobOutcome {
    DryRun(RetryPlan),
    Executed {
        plan: RetryPlan,
        result: RunBatchResponse,
    },
}

impl RetryJobOutcome {
    pub fn plan(&self) -> &RetryPlan {
        match self {
            Self::DryRun(plan) => plan,
            Self::Executed { plan, .. } => plan,
        }
    }

    pub fn dry_run(&self) -> bool {
        matches!(self, Self::DryRun(_))
    }
}

pub(crate) fn job_summary(job: &Job) -> JobSummary {
    JobSummary {
        id: job.id().to_string(),
        source_path: job.source_path().into(),
        status: job_status(job.status()),
        stages: job
            .stages()
            .iter()
            .map(|stage| StageSummary {
                kind: stage.kind.as_str().into(),
                status: stage_status(stage.status),
                artifact_path: stage
                    .artifact
                    .as_ref()
                    .map(|artifact| artifact.path.clone()),
            })
            .collect(),
    }
}

pub(crate) fn job_status(status: JobStatus) -> String {
    match status {
        JobStatus::Pending => "pending",
        JobStatus::Running => "running",
        JobStatus::Done => "done",
        JobStatus::DoneDegraded => "done_degraded",
        JobStatus::Failed => "failed",
        JobStatus::Cancelled => "cancelled",
    }
    .into()
}

pub(crate) fn stage_status(status: StageStatus) -> String {
    match status {
        StageStatus::Pending => "pending",
        StageStatus::WaitingResource => "waiting_resource",
        StageStatus::Running => "running",
        StageStatus::Retrying => "retrying",
        StageStatus::Done => "done",
        StageStatus::DoneDegraded => "done_degraded",
        StageStatus::Failed => "failed",
        StageStatus::Skipped => "skipped",
        StageStatus::Cancelled => "cancelled",
        StageStatus::WaitingProvider => "waiting_provider",
    }
    .into()
}

// Silence unused import warning if RetryJobResponse is only used transitively.
#[allow(dead_code)]
fn _retry_response_type(_: RetryJobResponse) {}
