//! SQLite implementations of application-owned repository/artifact ports.
//!
//! StoreHandle routes each operation to the dedicated SQLite actor.

use std::path::Path;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use videocaptionerr_contracts::artifact::ArtifactKind;
use videocaptionerr_contracts::error::{ErrorCode, VcError};
use videocaptionerr_core::application_error::{AppResult, ApplicationError};
use videocaptionerr_core::ports::{ArtifactCommit, ArtifactStore, JobRepository, TranscriptCommit};
use videocaptionerr_core::ports::{BatchRepository, WorkUnitRepository};
use videocaptionerr_domain::{ArtifactRef, Batch, BatchId, Job, JobId, StageKind, WorkUnit};

use crate::artifact::{atomic_write_json, blake3_file};
use crate::store::WorkUnitRecord;
use crate::StoreHandle;

#[async_trait]
impl JobRepository for StoreHandle {
    async fn load_job(&self, id: &JobId) -> AppResult<Option<Job>> {
        let json = self
            .load_job_aggregate(id.as_str())
            .await
            .map_err(ApplicationError::Adapter)?;
        json.map(|body| {
            serde_json::from_str(&body).map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCorrupt,
                    format!("decode Job aggregate: {error}"),
                ))
            })
        })
        .transpose()
    }

    async fn save_job(&self, job: &Job) -> AppResult<()> {
        let json = serde_json::to_string(job).map_err(|error| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode Job aggregate: {error}"),
            ))
        })?;
        self.save_job_aggregate(
            job.id().as_str(),
            job.batch_id().map(|id| id.as_str()),
            job.status().as_str(),
            job.source_path(),
            &json,
        )
        .await
        .map_err(ApplicationError::Adapter)
    }

    async fn delete_job(&self, id: &JobId) -> AppResult<()> {
        self.delete_job_record(id.as_str())
            .await
            .map_err(ApplicationError::Adapter)
    }

    async fn list_jobs(&self) -> AppResult<Vec<Job>> {
        let rows = self
            .list_job_aggregates()
            .await
            .map_err(ApplicationError::Adapter)?;
        rows.into_iter()
            .map(|body| {
                serde_json::from_str(&body).map_err(|error| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode Job aggregate: {error}"),
                    ))
                })
            })
            .collect()
    }
}

#[async_trait]
impl BatchRepository for StoreHandle {
    async fn load_batch(&self, id: &BatchId) -> AppResult<Option<Batch>> {
        let json = self
            .load_batch_aggregate(id.as_str())
            .await
            .map_err(ApplicationError::Adapter)?;
        json.map(|body| {
            serde_json::from_str(&body).map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCorrupt,
                    format!("decode Batch aggregate: {error}"),
                ))
            })
        })
        .transpose()
    }

    async fn save_batch(&self, batch: &Batch) -> AppResult<()> {
        let json = serde_json::to_string(batch).map_err(|error| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode Batch aggregate: {error}"),
            ))
        })?;
        self.save_batch_aggregate(
            batch.id().as_str(),
            batch.status().as_str(),
            &batch.execution_profile().asr_model,
            &batch.execution_profile().device,
            &json,
        )
        .await
        .map_err(ApplicationError::Adapter)
    }
}

#[async_trait]
impl WorkUnitRepository for StoreHandle {
    async fn load_work_unit(
        &self,
        id: &videocaptionerr_domain::WorkUnitId,
    ) -> AppResult<Option<WorkUnit>> {
        let json = self
            .load_work_unit_aggregate(id.as_str())
            .await
            .map_err(ApplicationError::Adapter)?;
        json.map(|body| {
            serde_json::from_str(&body).map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCorrupt,
                    format!("decode WorkUnit aggregate: {error}"),
                ))
            })
        })
        .transpose()
    }

    async fn save_work_unit(&self, unit: &WorkUnit) -> AppResult<()> {
        let json = serde_json::to_string(unit).map_err(|error| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode WorkUnit aggregate: {error}"),
            ))
        })?;
        let (lease_owner, lease_expires_at) = unit
            .lease()
            .map(|lease| {
                (
                    Some(lease.owner.as_str()),
                    DateTime::<Utc>::from_timestamp_millis(lease.expires_at_ms as i64)
                        .map(|value| value.to_rfc3339()),
                )
            })
            .unwrap_or((None, None));
        self.save_work_unit_aggregate(WorkUnitRecord {
            id: unit.id().to_string(),
            job_id: unit.job_id().to_string(),
            stage: stage_name(unit.stage()).into(),
            unit_kind: unit.unit_kind().into(),
            unit_index: unit.unit_index(),
            input_hash: unit.input_hash().into(),
            status: unit.status().as_str().into(),
            attempt: unit.attempt(),
            lease_owner: lease_owner.map(str::to_owned),
            lease_expires_at,
            artifact_id: unit.artifact().map(|artifact| artifact.id.to_string()),
            aggregate_json: json,
        })
        .await
        .map_err(ApplicationError::Adapter)
    }

    async fn recover_expired(&self, now_ms: u64) -> AppResult<u32> {
        let now = DateTime::<Utc>::from_timestamp_millis(now_ms as i64).ok_or_else(|| {
            ApplicationError::Invalid("recovery timestamp is outside chrono range".into())
        })?;
        let bodies = self
            .list_expired_work_unit_aggregates(&now.to_rfc3339())
            .await
            .map_err(ApplicationError::Adapter)?;
        for body in &bodies {
            let mut unit: WorkUnit = serde_json::from_str(body).map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCorrupt,
                    format!("decode expired WorkUnit aggregate: {error}"),
                ))
            })?;
            unit.recover_expired(now_ms)
                .map_err(ApplicationError::Domain)?;
            self.save_work_unit(&unit).await?;
        }
        u32::try_from(bodies.len()).map_err(|_| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::Internal,
                "expired work-unit count exceeds u32",
            ))
        })
    }
}

#[derive(Clone)]
pub struct SqliteArtifactStore {
    store: StoreHandle,
}

impl SqliteArtifactStore {
    pub fn new(store: StoreHandle) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ArtifactStore for SqliteArtifactStore {
    async fn commit(&self, commit: ArtifactCommit) -> AppResult<()> {
        self.validate(&commit.artifact).await?;
        let meta = artifact_meta(&commit.job_id, &commit.artifact)?;
        self.store
            .commit_artifact_and_unit(meta, commit.work_unit_id.as_ref().map(ToString::to_string))
            .await
            .map_err(ApplicationError::Adapter)
    }

    async fn commit_transcript(&self, commit: TranscriptCommit) -> AppResult<ArtifactRef> {
        let content_hash = atomic_write_json(&commit.path, &commit.transcript)
            .map_err(ApplicationError::Adapter)?;
        let artifact = ArtifactRef {
            id: commit.artifact_id,
            stage: commit.stage,
            path: commit.path.to_string_lossy().into_owned(),
            content_hash,
            schema_version: videocaptionerr_domain::SCHEMA_VERSION,
            producer_fingerprint: commit.producer_fingerprint,
        };
        self.commit(ArtifactCommit {
            job_id: commit.job_id,
            artifact: artifact.clone(),
            work_unit_id: None,
        })
        .await?;
        Ok(artifact)
    }

    async fn validate(&self, artifact: &ArtifactRef) -> AppResult<()> {
        let actual = blake3_file(Path::new(&artifact.path)).map_err(ApplicationError::Adapter)?;
        if actual != artifact.content_hash {
            return Err(ApplicationError::Adapter(VcError::new(
                ErrorCode::ArtifactCorrupt,
                format!("artifact hash mismatch: {}", artifact.path),
            )));
        }
        Ok(())
    }
}

fn artifact_meta(
    job_id: &JobId,
    artifact: &ArtifactRef,
) -> Result<videocaptionerr_contracts::artifact::ArtifactMeta, VcError> {
    Ok(videocaptionerr_contracts::artifact::ArtifactMeta::new(
        artifact.id.as_str(),
        job_id.as_str(),
        stage_name(artifact.stage),
        artifact_kind(artifact.stage),
        &artifact.path,
        &artifact.content_hash,
        &artifact.producer_fingerprint,
        chrono::Utc::now().to_rfc3339(),
    ))
}

fn stage_name(stage: StageKind) -> &'static str {
    match stage {
        StageKind::Probe => "probe",
        StageKind::ExtractAudio => "extract_audio",
        StageKind::Asr => "asr",
        StageKind::Split => "split",
        StageKind::Correct => "correct",
        StageKind::Translate => "translate",
        StageKind::Export => "export",
    }
}

fn artifact_kind(stage: StageKind) -> ArtifactKind {
    match stage {
        StageKind::Probe => ArtifactKind::MediaProbe,
        StageKind::ExtractAudio => ArtifactKind::AudioWav,
        StageKind::Asr => ArtifactKind::Transcript,
        StageKind::Split => ArtifactKind::Transcript,
        StageKind::Export => ArtifactKind::Other,
        StageKind::Correct | StageKind::Translate => ArtifactKind::Transcript,
    }
}

trait StatusString {
    fn as_str(&self) -> &'static str;
}

impl StatusString for videocaptionerr_domain::JobStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Done => "done",
            Self::DoneDegraded => "done_degraded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl StatusString for videocaptionerr_domain::BatchStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl StatusString for videocaptionerr_domain::WorkUnitStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use videocaptionerr_core::ports::{JobRepository, WorkUnitRepository};
    use videocaptionerr_domain::WorkUnitStatus;

    #[tokio::test]
    async fn work_unit_repository_recovery_keeps_json_and_control_state_aligned() {
        let dir = tempfile::tempdir().unwrap();
        let store = StoreHandle::open(&dir.path().join("repository.db")).unwrap();
        let job_id: JobId = ulid::Ulid::new().into();
        let job = Job::new(
            job_id.clone(),
            None,
            ulid::Ulid::new().into(),
            "/media/input.wav",
        );
        <StoreHandle as JobRepository>::save_job(&store, &job)
            .await
            .unwrap();

        let unit_id = ulid::Ulid::new().into();
        let mut unit =
            WorkUnit::new(unit_id, job_id, StageKind::Asr, "chunk", 0, "input-hash").unwrap();
        unit.lease_for("test", 0, 1_000).unwrap();
        <StoreHandle as WorkUnitRepository>::save_work_unit(&store, &unit)
            .await
            .unwrap();

        let loaded = <StoreHandle as WorkUnitRepository>::load_work_unit(&store, unit.id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.status(), WorkUnitStatus::Running);

        assert_eq!(
            <StoreHandle as WorkUnitRepository>::recover_expired(&store, 2_000)
                .await
                .unwrap(),
            1
        );
        let recovered = <StoreHandle as WorkUnitRepository>::load_work_unit(&store, unit.id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered.status(), WorkUnitStatus::Pending);
        assert_eq!(recovered.attempt(), 1);
        assert!(recovered.lease().is_none());
    }
}
