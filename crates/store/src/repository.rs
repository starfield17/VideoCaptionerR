//! SQLite implementations of application-owned repository/artifact ports.
//!
//! StoreHandle routes each operation to the dedicated SQLite actor.

use std::fs;
use std::path::Path;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use videocaptionerr_contracts::artifact::ArtifactKind;
use videocaptionerr_contracts::error::{ErrorCode, VcError};
use videocaptionerr_core::application_error::{AppResult, ApplicationError};
use videocaptionerr_core::execution_snapshot::JobExecutionSnapshot;
use videocaptionerr_core::ports::{
    ArtifactCommit, ArtifactRecoveryStore, ArtifactStore, BatchRepository, CapabilityProbeRecord,
    CapabilityProbeStore, ChunkPlanCommit, ChunkPlanStore, EventPublisher, ExpectedVersion,
    JobRepository, OutboxEvent, OutboxRepository, SnapshotRepository, StageCommitRepository,
    StageCommitRequest, StageCommitResult, TranscriptCommit, Versioned, WorkUnitRepository,
};
use videocaptionerr_domain::{
    ArtifactRef, Batch, BatchId, DomainEvent, Job, JobId, StageKind, WorkUnit,
};

use crate::artifact::{atomic_write_json, blake3_file};
use crate::store::{LeaseRequest, WorkUnitRecord};
use crate::StoreHandle;

#[async_trait]
impl JobRepository for StoreHandle {
    async fn load_job(&self, id: &JobId) -> AppResult<Option<Versioned<Job>>> {
        let row = self
            .load_job_aggregate(id.as_str())
            .await
            .map_err(ApplicationError::Adapter)?;
        row.map(|(body, version)| {
            serde_json::from_str(&body)
                .map_err(|error| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode Job aggregate: {error}"),
                    ))
                })
                .map(|value| Versioned::with_version(value, version))
        })
        .transpose()
    }

    async fn save_job(&self, job: &mut Versioned<Job>, expected: ExpectedVersion) -> AppResult<()> {
        let json = serde_json::to_string(&job.value).map_err(|error| {
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
            job.profile_revision().as_str(),
            job.execution_snapshot_id().map(|id| id.as_str()),
            &json,
            expected,
        )
        .await
        .map_err(ApplicationError::Adapter)
        .map(|version| {
            job.version = version;
        })
    }

    async fn delete_job(&self, id: &JobId) -> AppResult<()> {
        self.delete_job_record(id.as_str())
            .await
            .map_err(ApplicationError::Adapter)
    }

    async fn list_jobs(&self) -> AppResult<Vec<Versioned<Job>>> {
        let rows = self
            .list_job_aggregates()
            .await
            .map_err(ApplicationError::Adapter)?;
        rows.into_iter()
            .map(|(body, version)| {
                serde_json::from_str(&body)
                    .map_err(|error| {
                        ApplicationError::Adapter(VcError::new(
                            ErrorCode::ArtifactCorrupt,
                            format!("decode Job aggregate: {error}"),
                        ))
                    })
                    .map(|value| Versioned::with_version(value, version))
            })
            .collect()
    }
}

#[async_trait]
impl BatchRepository for StoreHandle {
    async fn load_batch(&self, id: &BatchId) -> AppResult<Option<Versioned<Batch>>> {
        let row = self
            .load_batch_aggregate(id.as_str())
            .await
            .map_err(ApplicationError::Adapter)?;
        row.map(|(body, version)| {
            serde_json::from_str(&body)
                .map_err(|error| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode Batch aggregate: {error}"),
                    ))
                })
                .map(|value| Versioned::with_version(value, version))
        })
        .transpose()
    }

    async fn list_batches(&self) -> AppResult<Vec<Versioned<Batch>>> {
        let rows = self
            .list_batch_aggregates()
            .await
            .map_err(ApplicationError::Adapter)?;
        rows.into_iter()
            .map(|(body, version)| {
                serde_json::from_str(&body)
                    .map_err(|error| {
                        ApplicationError::Adapter(VcError::new(
                            ErrorCode::ArtifactCorrupt,
                            format!("decode Batch aggregate: {error}"),
                        ))
                    })
                    .map(|value| Versioned::with_version(value, version))
            })
            .collect()
    }

    async fn save_batch(
        &self,
        batch: &mut Versioned<Batch>,
        expected: ExpectedVersion,
    ) -> AppResult<()> {
        let json = serde_json::to_string(&batch.value).map_err(|error| {
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
            expected,
        )
        .await
        .map_err(ApplicationError::Adapter)
        .map(|version| {
            batch.version = version;
        })
    }
}

#[async_trait]
impl StageCommitRepository for StoreHandle {
    async fn commit_stage(&self, request: StageCommitRequest) -> AppResult<StageCommitResult> {
        StoreHandle::commit_stage(self, request)
            .await
            .map_err(ApplicationError::Adapter)
    }
}

#[async_trait]
impl ArtifactRecoveryStore for SqliteArtifactStore {
    async fn recover(
        &self,
        roots: &[std::path::PathBuf],
    ) -> AppResult<videocaptionerr_core::ports::ArtifactRecoveryReport> {
        self.store
            .recover_artifacts(roots.to_vec())
            .await
            .map_err(ApplicationError::Adapter)
    }
}

#[async_trait]
impl OutboxRepository for StoreHandle {
    async fn list_pending(
        &self,
        limit: u32,
    ) -> AppResult<Vec<videocaptionerr_core::ports::StoredOutboxEvent>> {
        self.list_pending_outbox(limit)
            .await
            .map_err(ApplicationError::Adapter)
    }

    async fn mark_delivered(
        &self,
        id: &videocaptionerr_domain::UlidStr,
        delivered_at: &str,
    ) -> AppResult<()> {
        self.mark_outbox_delivered(id.as_str(), delivered_at)
            .await
            .map_err(ApplicationError::Adapter)
    }
}

#[async_trait]
impl EventPublisher for StoreHandle {
    async fn publish(&self, event: DomainEvent) -> AppResult<()> {
        let (aggregate_id, event_type) = match &event {
            DomainEvent::BatchReachedTerminal { batch_id, .. } => {
                (batch_id.to_string(), "batch_reached_terminal")
            }
        };
        let payload_json = serde_json::to_string(&event).map_err(|error| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::Internal,
                format!("encode domain event: {error}"),
            ))
        })?;
        self.append_outbox(OutboxEvent {
            aggregate_type: "Batch".into(),
            aggregate_id,
            event_type: event_type.into(),
            payload_json,
            created_at: Utc::now().to_rfc3339(),
        })
        .await
        .map_err(ApplicationError::Adapter)
    }
}

#[async_trait]
impl SnapshotRepository for StoreHandle {
    async fn load_execution_snapshot(
        &self,
        id: &videocaptionerr_domain::UlidStr,
    ) -> AppResult<Option<JobExecutionSnapshot>> {
        let json = self
            .load_execution_snapshot(id.as_str())
            .await
            .map_err(ApplicationError::Adapter)?;
        json.map(|body| {
            serde_json::from_str(&body).map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCorrupt,
                    format!("decode execution snapshot: {error}"),
                ))
            })
        })
        .transpose()
    }

    async fn save_execution_snapshot(&self, snapshot: &JobExecutionSnapshot) -> AppResult<()> {
        self.save_execution_snapshot(snapshot.clone())
            .await
            .map_err(ApplicationError::Adapter)
    }

    async fn load_snapshots_for_batch(&self, id: &BatchId) -> AppResult<Vec<JobExecutionSnapshot>> {
        let rows = self
            .load_snapshots_for_batch(id.as_str())
            .await
            .map_err(ApplicationError::Adapter)?;
        rows.into_iter()
            .map(|body| {
                serde_json::from_str(&body).map_err(|error| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode batch execution snapshot: {error}"),
                    ))
                })
            })
            .collect()
    }
}

#[async_trait]
impl WorkUnitRepository for StoreHandle {
    async fn load_work_unit(
        &self,
        id: &videocaptionerr_domain::WorkUnitId,
    ) -> AppResult<Option<Versioned<WorkUnit>>> {
        let row = self
            .load_work_unit_aggregate(id.as_str())
            .await
            .map_err(ApplicationError::Adapter)?;
        row.map(|(body, version)| {
            serde_json::from_str(&body)
                .map_err(|error| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode WorkUnit aggregate: {error}"),
                    ))
                })
                .map(|value| Versioned::with_version(value, version))
        })
        .transpose()
    }

    async fn find_work_unit(
        &self,
        job_id: &videocaptionerr_domain::JobId,
        stage: StageKind,
        unit_kind: &str,
        unit_index: u32,
        input_hash: &str,
    ) -> AppResult<Option<Versioned<WorkUnit>>> {
        let row = self
            .find_work_unit_aggregate(
                job_id.as_str(),
                stage_name(stage),
                unit_kind,
                unit_index,
                input_hash,
            )
            .await
            .map_err(ApplicationError::Adapter)?;
        row.map(|(body, version)| {
            serde_json::from_str(&body)
                .map_err(|error| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode WorkUnit aggregate: {error}"),
                    ))
                })
                .map(|value| Versioned::with_version(value, version))
        })
        .transpose()
    }

    async fn save_work_unit(
        &self,
        unit: &mut Versioned<WorkUnit>,
        expected: ExpectedVersion,
    ) -> AppResult<()> {
        let json = serde_json::to_string(&unit.value).map_err(|error| {
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
        self.save_work_unit_aggregate(
            WorkUnitRecord {
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
            },
            expected,
        )
        .await
        .map_err(ApplicationError::Adapter)
        .map(|version| {
            unit.version = version;
        })
    }

    async fn recover_expired(&self, now_ms: u64) -> AppResult<u32> {
        let now = DateTime::<Utc>::from_timestamp_millis(now_ms as i64).ok_or_else(|| {
            ApplicationError::Invalid("recovery timestamp is outside chrono range".into())
        })?;
        let rows = self
            .list_expired_work_unit_aggregates(&now.to_rfc3339())
            .await
            .map_err(ApplicationError::Adapter)?;
        for (body, version) in &rows {
            let mut unit: WorkUnit = serde_json::from_str(body).map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCorrupt,
                    format!("decode expired WorkUnit aggregate: {error}"),
                ))
            })?;
            unit.recover_expired(now_ms)
                .map_err(ApplicationError::Domain)?;
            let mut versioned = Versioned::with_version(unit, *version);
            let expected = versioned.expected_version();
            self.save_work_unit(&mut versioned, expected).await?;
        }
        u32::try_from(rows.len()).map_err(|_| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::Internal,
                "expired work-unit count exceeds u32",
            ))
        })
    }

    async fn count_retryable(
        &self,
        job_id: &videocaptionerr_domain::JobId,
        from_stage: Option<StageKind>,
    ) -> AppResult<u32> {
        self.count_retryable_aggregates(job_id.as_str(), from_stage.map(stage_name))
            .await
            .map_err(ApplicationError::Adapter)
    }

    async fn lease_next_ready(
        &self,
        job_id: &videocaptionerr_domain::JobId,
        stage: StageKind,
        owner: &str,
        now_ms: u64,
        lease_ms: u64,
    ) -> AppResult<Option<Versioned<WorkUnit>>> {
        let now = DateTime::<Utc>::from_timestamp_millis(now_ms as i64).ok_or_else(|| {
            ApplicationError::Invalid("lease timestamp is outside chrono range".into())
        })?;
        let expires_ms = now_ms
            .checked_add(lease_ms)
            .ok_or_else(|| ApplicationError::Invalid("lease expiry timestamp overflowed".into()))?;
        let expires =
            DateTime::<Utc>::from_timestamp_millis(expires_ms as i64).ok_or_else(|| {
                ApplicationError::Invalid("lease expiry is outside chrono range".into())
            })?;
        let row = self
            .lease_next_ready_aggregate(LeaseRequest {
                job_id: job_id.to_string(),
                stage: stage_name(stage).to_string(),
                owner: owner.to_string(),
                now_rfc3339: now.to_rfc3339(),
                now_ms,
                expires_rfc3339: expires.to_rfc3339(),
                expires_at_ms: expires_ms,
            })
            .await
            .map_err(ApplicationError::Adapter)?;
        row.map(|(body, version)| {
            serde_json::from_str(&body)
                .map_err(|error| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode leased WorkUnit: {error}"),
                    ))
                })
                .map(|value| Versioned::with_version(value, version))
        })
        .transpose()
    }

    async fn retry_failed(
        &self,
        job_id: &videocaptionerr_domain::JobId,
        from_stage: Option<StageKind>,
    ) -> AppResult<u32> {
        self.retry_failed_aggregates(job_id.as_str(), from_stage.map(stage_name))
            .await
            .map_err(ApplicationError::Adapter)
    }
}

#[async_trait]
impl CapabilityProbeStore for StoreHandle {
    async fn load(
        &self,
        provider_profile_id: &str,
        model: &str,
        probe_hash: &str,
    ) -> AppResult<Option<String>> {
        self.load_capability_probe(provider_profile_id, model, probe_hash)
            .await
            .map_err(ApplicationError::Adapter)
    }

    async fn save(&self, record: CapabilityProbeRecord) -> AppResult<()> {
        self.save_capability_probe(record)
            .await
            .map_err(ApplicationError::Adapter)
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
        <Self as ArtifactStore>::commit(
            self,
            ArtifactCommit {
                job_id: commit.job_id,
                artifact: artifact.clone(),
                work_unit_id: commit.work_unit_id,
            },
        )
        .await?;
        Ok(artifact)
    }

    async fn load_transcript(
        &self,
        artifact: &ArtifactRef,
    ) -> AppResult<videocaptionerr_domain::Transcript> {
        self.validate(artifact).await?;
        let body = fs::read_to_string(&artifact.path).map_err(|error| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::ArtifactCorrupt,
                format!("read transcript artifact {}: {error}", artifact.path),
            ))
        })?;
        let transcript: videocaptionerr_domain::Transcript =
            serde_json::from_str(&body).map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCorrupt,
                    format!("decode transcript artifact {}: {error}", artifact.path),
                ))
            })?;
        transcript.validate().map_err(ApplicationError::Domain)?;
        Ok(transcript)
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

#[async_trait]
impl ChunkPlanStore for SqliteArtifactStore {
    async fn commit(&self, commit: ChunkPlanCommit) -> AppResult<ArtifactRef> {
        let content_hash =
            atomic_write_json(&commit.path, &commit.plan).map_err(ApplicationError::Adapter)?;
        let artifact = ArtifactRef {
            id: commit.artifact_id,
            stage: StageKind::Asr,
            path: commit.path.to_string_lossy().into_owned(),
            content_hash,
            schema_version: videocaptionerr_domain::SCHEMA_VERSION,
            producer_fingerprint: commit.producer_fingerprint,
        };
        <Self as ArtifactStore>::commit(
            self,
            ArtifactCommit {
                job_id: commit.job_id,
                artifact: artifact.clone(),
                work_unit_id: None,
            },
        )
        .await?;
        Ok(artifact)
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
    use rusqlite::Connection;
    use ulid::Ulid;
    use videocaptionerr_contracts::error::ErrorCode;
    use videocaptionerr_core::execution_snapshot::{
        AsrExecutionSnapshot, AudioStreamSelection, JobExecutionSnapshot, OutputPlanSnapshot,
        SourceStatSnapshot, JOB_EXECUTION_SNAPSHOT_SCHEMA_VERSION,
    };
    use videocaptionerr_core::ports::{
        ArtifactStore, BatchRepository, ExpectedVersion, JobRepository, SnapshotRepository,
        TranscriptCommit, Versioned, WorkUnitRepository,
    };
    use videocaptionerr_domain::{
        Batch, BatchExecutionProfile, EngineFingerprint, JobId, JobStatus, StageStatus, Transcript,
        WorkUnit, WorkUnitStatus,
    };

    fn complete_job(job: &mut Job, asr_artifact: ArtifactRef) {
        job.start().unwrap();
        for stage in [
            StageKind::Probe,
            StageKind::ExtractAudio,
            StageKind::Asr,
            StageKind::Split,
            StageKind::Correct,
            StageKind::Translate,
            StageKind::Export,
        ] {
            job.start_stage(stage).unwrap();
            let artifact = if stage == StageKind::Asr {
                asr_artifact.clone()
            } else {
                ArtifactRef {
                    id: ulid::Ulid::new().into(),
                    stage,
                    path: format!("{stage:?}.json"),
                    content_hash: format!("{stage:?}-hash"),
                    schema_version: videocaptionerr_domain::SCHEMA_VERSION,
                    producer_fingerprint: "test".into(),
                }
            };
            job.complete_stage(stage, artifact, false).unwrap();
        }
        job.finish().unwrap();
    }

    async fn save_new_job(store: &StoreHandle, job: Job) -> Versioned<Job> {
        let mut versioned = Versioned::new(job);
        <StoreHandle as JobRepository>::save_job(store, &mut versioned, ExpectedVersion::New)
            .await
            .unwrap();
        versioned
    }

    async fn save_new_work_unit(store: &StoreHandle, unit: WorkUnit) -> Versioned<WorkUnit> {
        let mut versioned = Versioned::new(unit);
        <StoreHandle as WorkUnitRepository>::save_work_unit(
            store,
            &mut versioned,
            ExpectedVersion::New,
        )
        .await
        .unwrap();
        versioned
    }

    async fn save_new_batch(store: &StoreHandle, batch: Batch) -> Versioned<Batch> {
        let mut versioned = Versioned::new(batch);
        <StoreHandle as BatchRepository>::save_batch(store, &mut versioned, ExpectedVersion::New)
            .await
            .unwrap();
        versioned
    }

    fn snapshot(job_id: JobId, batch_id: videocaptionerr_domain::BatchId) -> JobExecutionSnapshot {
        JobExecutionSnapshot {
            snapshot_id: Ulid::new().into(),
            schema_version: JOB_EXECUTION_SNAPSHOT_SCHEMA_VERSION,
            created_at: "2026-07-20T00:00:00Z".into(),
            job_id,
            batch_id,
            canonical_source_path: "/media/input.mp4".into(),
            source_stat: SourceStatSnapshot {
                size: 123,
                modified_at_ms: Some(1_725_000_000_000),
            },
            job_dir: "/jobs/job-1".into(),
            profile_revision: Ulid::new().into(),
            asr: AsrExecutionSnapshot {
                engine: "fake".into(),
                model_locator: "fake:default".into(),
                model_id: Some("fake".into()),
                model_digest: None,
                device: "cpu".into(),
                compute_type: "default".into(),
            },
            audio_stream: AudioStreamSelection::Auto,
            source_language: Some("en".into()),
            target_language: Some("zh".into()),
            output: OutputPlanSnapshot {
                path: "/exports/input.zh.srt".into(),
                format: "srt".into(),
                layout: "source".into(),
                conflict_policy: "rename".into(),
                fallback_to_source: true,
            },
            llm: None,
        }
    }

    #[tokio::test]
    async fn execution_snapshot_survives_store_reopen_and_is_immutable() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("snapshot.db");
        let job_id: JobId = Ulid::new().into();
        let batch_id: videocaptionerr_domain::BatchId = Ulid::new().into();
        let snapshot = snapshot(job_id, batch_id);
        let store = StoreHandle::open(&db_path).unwrap();

        <StoreHandle as SnapshotRepository>::save_execution_snapshot(&store, &snapshot)
            .await
            .unwrap();
        let mut job = Versioned::new(Job::new_with_snapshot(
            snapshot.job_id.clone(),
            None,
            snapshot.snapshot_id.clone(),
            snapshot.profile_revision.clone(),
            "/media/stale-input.mp4",
        ));
        <StoreHandle as JobRepository>::save_job(&store, &mut job, ExpectedVersion::New)
            .await
            .unwrap();
        let connection = Connection::open(&db_path).unwrap();
        let projection: (String, String, String, String) = connection
            .query_row(
                "SELECT source_path, job_dir, profile_revision, execution_snapshot_id
                 FROM jobs WHERE id = ?1",
                [snapshot.job_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(projection.0, snapshot.canonical_source_path);
        assert_eq!(projection.1, snapshot.job_dir);
        assert_eq!(projection.2, snapshot.profile_revision.to_string());
        assert_eq!(projection.3, snapshot.snapshot_id.to_string());

        let reopened = StoreHandle::open(&db_path).unwrap();
        let loaded = <StoreHandle as SnapshotRepository>::load_execution_snapshot(
            &reopened,
            &snapshot.snapshot_id,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(loaded, snapshot);

        let mut changed = snapshot.clone();
        changed.job_dir = "/jobs/changed".into();
        let error =
            <StoreHandle as SnapshotRepository>::save_execution_snapshot(&reopened, &changed)
                .await
                .unwrap_err()
                .into_vc_error();
        assert_eq!(error.code, ErrorCode::StaleResult);
    }

    #[tokio::test]
    async fn concurrent_job_batch_and_work_unit_saves_reject_stale_versions() {
        let dir = tempfile::tempdir().unwrap();
        let store = StoreHandle::open(&dir.path().join("cas.db")).unwrap();

        let job_id: JobId = Ulid::new().into();
        let mut job_first = save_new_job(
            &store,
            Job::new(job_id.clone(), None, Ulid::new().into(), "/media/input.wav"),
        )
        .await;
        let mut job_second = <StoreHandle as JobRepository>::load_job(&store, &job_id)
            .await
            .unwrap()
            .unwrap();
        job_first.start().unwrap();
        <StoreHandle as JobRepository>::save_job(&store, &mut job_first, ExpectedVersion::Exact(1))
            .await
            .unwrap();
        let error = <StoreHandle as JobRepository>::save_job(
            &store,
            &mut job_second,
            ExpectedVersion::Exact(1),
        )
        .await
        .unwrap_err()
        .into_vc_error();
        assert_eq!(error.code, ErrorCode::StaleResult);

        let batch_id: videocaptionerr_domain::BatchId = Ulid::new().into();
        let batch = Batch::new(
            batch_id.clone(),
            vec![Ulid::new().into()],
            BatchExecutionProfile {
                asr_engine: "fake".into(),
                asr_model: "fake".into(),
                device: "cpu".into(),
                compute_type: "default".into(),
            },
        )
        .unwrap();
        let mut batch_first = save_new_batch(&store, batch).await;
        let mut batch_second = <StoreHandle as BatchRepository>::load_batch(&store, &batch_id)
            .await
            .unwrap()
            .unwrap();
        batch_first.start().unwrap();
        <StoreHandle as BatchRepository>::save_batch(
            &store,
            &mut batch_first,
            ExpectedVersion::Exact(1),
        )
        .await
        .unwrap();
        let error = <StoreHandle as BatchRepository>::save_batch(
            &store,
            &mut batch_second,
            ExpectedVersion::Exact(1),
        )
        .await
        .unwrap_err()
        .into_vc_error();
        assert_eq!(error.code, ErrorCode::StaleResult);

        let mut unit_first = save_new_work_unit(
            &store,
            WorkUnit::new(
                Ulid::new().into(),
                job_id,
                videocaptionerr_domain::StageKind::Asr,
                "chunk",
                0,
                "input-hash",
            )
            .unwrap(),
        )
        .await;
        let mut unit_second =
            <StoreHandle as WorkUnitRepository>::load_work_unit(&store, unit_first.id())
                .await
                .unwrap()
                .unwrap();
        unit_first.lease_for("first", 1_000, 2_000).unwrap();
        <StoreHandle as WorkUnitRepository>::save_work_unit(
            &store,
            &mut unit_first,
            ExpectedVersion::Exact(1),
        )
        .await
        .unwrap();
        let error = <StoreHandle as WorkUnitRepository>::save_work_unit(
            &store,
            &mut unit_second,
            ExpectedVersion::Exact(1),
        )
        .await
        .unwrap_err()
        .into_vc_error();
        assert_eq!(error.code, ErrorCode::StaleResult);
    }

    #[tokio::test]
    async fn transcript_commit_completes_leased_chunk_and_terminal_job_has_no_running_units() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("repository.db");
        let store = StoreHandle::open(&db_path).unwrap();
        let artifact_store = SqliteArtifactStore::new(store.clone());
        let job_id: JobId = ulid::Ulid::new().into();
        let mut job = save_new_job(
            &store,
            Job::new(
                job_id.clone(),
                None,
                ulid::Ulid::new().into(),
                "/media/input.wav",
            ),
        )
        .await;

        let mut unit = save_new_work_unit(
            &store,
            WorkUnit::new(
                ulid::Ulid::new().into(),
                job_id.clone(),
                StageKind::Asr,
                "asr_chunk",
                0,
                "chunk-input-hash",
            )
            .unwrap(),
        )
        .await;
        unit.lease_for("asr:test", 0, 1_000).unwrap();
        let expected = unit.expected_version();
        <StoreHandle as WorkUnitRepository>::save_work_unit(&store, &mut unit, expected)
            .await
            .unwrap();

        let artifact = artifact_store
            .commit_transcript(TranscriptCommit {
                job_id: job_id.clone(),
                stage: StageKind::Asr,
                artifact_id: ulid::Ulid::new().into(),
                path: dir.path().join("job/asr-chunks/chunk-0000.json"),
                transcript: Transcript::new_asr(
                    "source-hash",
                    EngineFingerprint::unknown(),
                    Vec::new(),
                ),
                producer_fingerprint: "fake@test".into(),
                work_unit_id: Some(unit.id().clone()),
            })
            .await
            .unwrap();

        let completed_unit = <StoreHandle as WorkUnitRepository>::load_work_unit(&store, unit.id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed_unit.status(), WorkUnitStatus::Done);
        assert_eq!(completed_unit.artifact(), Some(&artifact));

        complete_job(&mut job, artifact);
        let expected = job.expected_version();
        <StoreHandle as JobRepository>::save_job(&store, &mut job, expected)
            .await
            .unwrap();
        let persisted_job = <StoreHandle as JobRepository>::load_job(&store, &job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted_job.status(), JobStatus::Done);
        assert!(persisted_job
            .stages()
            .iter()
            .all(|stage| stage.status == StageStatus::Done));

        let connection = Connection::open(&db_path).unwrap();
        let non_terminal_units: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM work_units
                 WHERE job_id = ?1 AND status IN ('pending', 'running')",
                [job_id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(non_terminal_units, 0);
    }

    #[tokio::test]
    async fn transcript_commit_failure_leaves_chunk_lease_recoverable() {
        let dir = tempfile::tempdir().unwrap();
        let store = StoreHandle::open(&dir.path().join("repository.db")).unwrap();
        let artifact_store = SqliteArtifactStore::new(store.clone());
        let job_id: JobId = ulid::Ulid::new().into();
        let _job = save_new_job(
            &store,
            Job::new(
                job_id.clone(),
                None,
                ulid::Ulid::new().into(),
                "/media/input.wav",
            ),
        )
        .await;

        let mut unit = save_new_work_unit(
            &store,
            WorkUnit::new(
                ulid::Ulid::new().into(),
                job_id.clone(),
                StageKind::Asr,
                "asr_chunk",
                0,
                "chunk-input-hash",
            )
            .unwrap(),
        )
        .await;
        unit.lease_for("asr:test", 0, 1_000).unwrap();
        let expected = unit.expected_version();
        <StoreHandle as WorkUnitRepository>::save_work_unit(&store, &mut unit, expected)
            .await
            .unwrap();

        assert!(artifact_store
            .commit_transcript(TranscriptCommit {
                job_id: job_id.clone(),
                stage: StageKind::Asr,
                artifact_id: ulid::Ulid::new().into(),
                path: std::path::PathBuf::from("\0invalid-artifact.json"),
                transcript: Transcript::new_asr(
                    "source-hash",
                    EngineFingerprint::unknown(),
                    Vec::new(),
                ),
                producer_fingerprint: "fake@test".into(),
                work_unit_id: Some(unit.id().clone()),
            })
            .await
            .is_err());

        let failed_unit = <StoreHandle as WorkUnitRepository>::load_work_unit(&store, unit.id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed_unit.status(), WorkUnitStatus::Running);
        assert_eq!(
            <StoreHandle as WorkUnitRepository>::recover_expired(&store, 1_001)
                .await
                .unwrap(),
            1
        );
        let retryable_unit = <StoreHandle as WorkUnitRepository>::load_work_unit(&store, unit.id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retryable_unit.status(), WorkUnitStatus::Pending);
    }

    #[tokio::test]
    async fn work_unit_repository_recovery_keeps_json_and_control_state_aligned() {
        let dir = tempfile::tempdir().unwrap();
        let store = StoreHandle::open(&dir.path().join("repository.db")).unwrap();
        let job_id: JobId = ulid::Ulid::new().into();
        let _job = save_new_job(
            &store,
            Job::new(
                job_id.clone(),
                None,
                ulid::Ulid::new().into(),
                "/media/input.wav",
            ),
        )
        .await;

        let unit_id = ulid::Ulid::new().into();
        let mut unit = save_new_work_unit(
            &store,
            WorkUnit::new(
                unit_id,
                job_id.clone(),
                StageKind::Asr,
                "chunk",
                0,
                "input-hash",
            )
            .unwrap(),
        )
        .await;
        unit.lease_for("test", 0, 1_000).unwrap();
        let expected = unit.expected_version();
        <StoreHandle as WorkUnitRepository>::save_work_unit(&store, &mut unit, expected)
            .await
            .unwrap();
        assert_eq!(
            <StoreHandle as WorkUnitRepository>::count_retryable(&store, &job_id, None)
                .await
                .unwrap(),
            0
        );

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

    #[tokio::test]
    async fn actor_claims_fifo_unit_and_retries_failed_unit() {
        let dir = tempfile::tempdir().unwrap();
        let store = StoreHandle::open(&dir.path().join("scheduler.db")).unwrap();
        let job_id: JobId = ulid::Ulid::new().into();
        let _job = save_new_job(
            &store,
            Job::new(
                job_id.clone(),
                None,
                ulid::Ulid::new().into(),
                "/media/input.wav",
            ),
        )
        .await;
        let _unit = save_new_work_unit(
            &store,
            WorkUnit::new(
                ulid::Ulid::new().into(),
                job_id.clone(),
                StageKind::Asr,
                "chunk",
                0,
                "pcm-hash",
            )
            .unwrap(),
        )
        .await;

        let leased = <StoreHandle as WorkUnitRepository>::lease_next_ready(
            &store,
            &job_id,
            StageKind::Asr,
            "scheduler-1",
            1_000,
            5_000,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(leased.status(), WorkUnitStatus::Running);
        assert_eq!(leased.lease().unwrap().owner, "scheduler-1");

        let mut unit = leased;
        unit.fail("ASR_FAILED").unwrap();
        let expected = unit.expected_version();
        <StoreHandle as WorkUnitRepository>::save_work_unit(&store, &mut unit, expected)
            .await
            .unwrap();
        assert_eq!(
            <StoreHandle as WorkUnitRepository>::count_retryable(&store, &job_id, None)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            <StoreHandle as WorkUnitRepository>::retry_failed(&store, &job_id, None)
                .await
                .unwrap(),
            1
        );
        let retried = <StoreHandle as WorkUnitRepository>::load_work_unit(&store, unit.id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retried.status(), WorkUnitStatus::Pending);
        assert_eq!(retried.attempt(), 1);
        assert_eq!(
            <StoreHandle as WorkUnitRepository>::count_retryable(&store, &job_id, None)
                .await
                .unwrap(),
            0
        );
    }
}
