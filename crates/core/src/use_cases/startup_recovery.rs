//! Deterministic control-plane and artifact reconciliation performed before
//! the application accepts processing work.

use std::path::PathBuf;
use std::sync::Arc;

use videocaptionerr_domain::{BatchStatus, JobStatus};

use crate::application_error::AppResult;
use crate::ports::{
    ArtifactRecoveryReport, ArtifactRecoveryStore, BatchRepository, JobRepository,
    OutboxRepository, WorkUnitRepository,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub artifacts: ArtifactRecoveryReport,
    pub expired_work_units: u32,
    pub recovered_jobs: u32,
    pub recovered_batches: u32,
    pub pending_outbox_events: u32,
}

pub struct StartupRecovery {
    jobs: Arc<dyn JobRepository>,
    batches: Arc<dyn BatchRepository>,
    work_units: Arc<dyn WorkUnitRepository>,
    artifacts: Arc<dyn ArtifactRecoveryStore>,
    outbox: Arc<dyn OutboxRepository>,
}

impl StartupRecovery {
    pub fn new(
        jobs: Arc<dyn JobRepository>,
        batches: Arc<dyn BatchRepository>,
        work_units: Arc<dyn WorkUnitRepository>,
        artifacts: Arc<dyn ArtifactRecoveryStore>,
        outbox: Arc<dyn OutboxRepository>,
    ) -> Self {
        Self {
            jobs,
            batches,
            work_units,
            artifacts,
            outbox,
        }
    }

    pub async fn execute(&self, roots: Vec<PathBuf>) -> AppResult<RecoveryReport> {
        let artifacts = self.artifacts.recover(&roots).await?;
        let pending_outbox_events = u32::try_from(self.outbox.list_pending(u32::MAX).await?.len())
            .map_err(|_| {
                crate::application_error::ApplicationError::Invalid(
                    "pending outbox event count exceeds u32".into(),
                )
            })?;
        let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let expired_work_units = self.work_units.recover_expired(now_ms).await?;

        let mut recovered_jobs = 0;
        for mut job in self.jobs.list_jobs().await? {
            if job.status() == JobStatus::Running {
                job.recover_after_restart()?;
                let expected = job.expected_version();
                self.jobs.save_job(&mut job, expected).await?;
                recovered_jobs += 1;
            }
        }

        let mut recovered_batches = 0;
        for mut batch in self.batches.list_batches().await? {
            if batch.status() == BatchStatus::Running {
                batch.recover_after_restart()?;
                let expected = batch.expected_version();
                self.batches.save_batch(&mut batch, expected).await?;
                recovered_batches += 1;
            }
        }

        Ok(RecoveryReport {
            artifacts,
            expired_work_units,
            recovered_jobs,
            recovered_batches,
            pending_outbox_events,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use ulid::Ulid;
    use videocaptionerr_domain::{
        Batch, BatchExecutionProfile, BatchId, Job, JobId, StageKind, WorkUnit, WorkUnitId,
    };

    use super::*;
    use crate::application_error::AppResult;
    use crate::ports::{
        ArtifactRecoveryStore, BatchRepository, ExpectedVersion, JobRepository, OutboxRepository,
        StoredOutboxEvent, Versioned, WorkUnitRepository,
    };

    struct FakeArtifacts;

    #[async_trait]
    impl ArtifactRecoveryStore for FakeArtifacts {
        async fn recover(&self, _roots: &[PathBuf]) -> AppResult<ArtifactRecoveryReport> {
            Ok(ArtifactRecoveryReport {
                partial_files: vec![PathBuf::from("a.partial")],
                orphan_files: vec![PathBuf::from("orphan.json")],
                corrupt_artifacts: vec!["artifact-1".into()],
            })
        }
    }

    struct FakeJobs {
        values: Mutex<Vec<Versioned<Job>>>,
    }

    #[async_trait]
    impl JobRepository for FakeJobs {
        async fn load_job(&self, _id: &JobId) -> AppResult<Option<Versioned<Job>>> {
            Ok(None)
        }

        async fn save_job(
            &self,
            job: &mut Versioned<Job>,
            _expected: ExpectedVersion,
        ) -> AppResult<()> {
            job.version += 1;
            let mut values = self.values.lock().unwrap();
            values.clear();
            values.push(job.clone());
            Ok(())
        }

        async fn delete_job(&self, _id: &JobId) -> AppResult<()> {
            Ok(())
        }

        async fn list_jobs(&self) -> AppResult<Vec<Versioned<Job>>> {
            Ok(self.values.lock().unwrap().clone())
        }
    }

    struct FakeBatches {
        values: Mutex<Vec<Versioned<Batch>>>,
    }

    #[async_trait]
    impl BatchRepository for FakeBatches {
        async fn load_batch(&self, _id: &BatchId) -> AppResult<Option<Versioned<Batch>>> {
            Ok(None)
        }

        async fn list_batches(&self) -> AppResult<Vec<Versioned<Batch>>> {
            Ok(self.values.lock().unwrap().clone())
        }

        async fn save_batch(
            &self,
            batch: &mut Versioned<Batch>,
            _expected: ExpectedVersion,
        ) -> AppResult<()> {
            batch.version += 1;
            let mut values = self.values.lock().unwrap();
            values.clear();
            values.push(batch.clone());
            Ok(())
        }
    }

    struct FakeWorkUnits;

    struct FakeOutbox;

    #[async_trait]
    impl OutboxRepository for FakeOutbox {
        async fn list_pending(&self, _limit: u32) -> AppResult<Vec<StoredOutboxEvent>> {
            Ok(Vec::new())
        }

        async fn mark_delivered(
            &self,
            _id: &videocaptionerr_domain::UlidStr,
            _delivered_at: &str,
        ) -> AppResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl WorkUnitRepository for FakeWorkUnits {
        async fn load_work_unit(&self, _id: &WorkUnitId) -> AppResult<Option<Versioned<WorkUnit>>> {
            Ok(None)
        }

        async fn find_work_unit(
            &self,
            _job_id: &JobId,
            _stage: StageKind,
            _unit_kind: &str,
            _unit_index: u32,
            _input_hash: &str,
        ) -> AppResult<Option<Versioned<WorkUnit>>> {
            Ok(None)
        }

        async fn save_work_unit(
            &self,
            _unit: &mut Versioned<WorkUnit>,
            _expected: ExpectedVersion,
        ) -> AppResult<()> {
            Ok(())
        }

        async fn recover_expired(&self, _now_ms: u64) -> AppResult<u32> {
            Ok(3)
        }

        async fn count_retryable(
            &self,
            _job_id: &JobId,
            _from_stage: Option<StageKind>,
        ) -> AppResult<u32> {
            Ok(0)
        }

        async fn list_for_job(&self, _job_id: &JobId) -> AppResult<Vec<Versioned<WorkUnit>>> {
            Ok(Vec::new())
        }

        async fn lease_next_ready(
            &self,
            _job_id: &JobId,
            _stage: StageKind,
            _owner: &str,
            _now_ms: u64,
            _lease_ms: u64,
        ) -> AppResult<Option<Versioned<WorkUnit>>> {
            Ok(None)
        }

        async fn retry_failed(
            &self,
            _job_id: &JobId,
            _from_stage: Option<StageKind>,
        ) -> AppResult<u32> {
            Ok(0)
        }
    }

    #[tokio::test]
    async fn recovery_reconciles_files_leases_and_running_aggregates() {
        let job_id: JobId = Ulid::new().into();
        let mut job = Job::new(job_id.clone(), None, Ulid::new().into(), "/media/a.mp4");
        job.start().unwrap();
        let batch_id: BatchId = Ulid::new().into();
        let mut batch = Batch::new(
            batch_id,
            vec![job_id],
            BatchExecutionProfile {
                asr_engine: "fake".into(),
                asr_model: "fake".into(),
                device: "cpu".into(),
                compute_type: "default".into(),
            },
        )
        .unwrap();
        batch.start().unwrap();
        let jobs = Arc::new(FakeJobs {
            values: Mutex::new(vec![Versioned::with_version(job, 4)]),
        });
        let batches = Arc::new(FakeBatches {
            values: Mutex::new(vec![Versioned::with_version(batch, 7)]),
        });
        let recovery = StartupRecovery::new(
            jobs.clone(),
            batches.clone(),
            Arc::new(FakeWorkUnits),
            Arc::new(FakeArtifacts),
            Arc::new(FakeOutbox),
        );

        let report = recovery.execute(vec![]).await.unwrap();
        assert_eq!(report.expired_work_units, 3);
        assert_eq!(report.recovered_jobs, 1);
        assert_eq!(report.recovered_batches, 1);
        assert_eq!(report.pending_outbox_events, 0);
        assert_eq!(report.artifacts.partial_files.len(), 1);
        assert_eq!(
            jobs.values.lock().unwrap()[0].status(),
            videocaptionerr_domain::JobStatus::Pending
        );
        assert_eq!(
            batches.values.lock().unwrap()[0].status(),
            videocaptionerr_domain::BatchStatus::Pending
        );
    }
}
