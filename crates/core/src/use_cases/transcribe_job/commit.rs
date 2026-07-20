use super::*;

impl TranscribeJob {
    pub(super) async fn save_job(&self, job: &mut Versioned<Job>) -> AppResult<()> {
        let expected = job.expected_version();
        self.jobs.save_job(job, expected).await
    }

    pub(super) async fn commit_bytes_stage(
        &self,
        job: &mut Versioned<Job>,
        stage: StageKind,
        path: PathBuf,
        bytes: Vec<u8>,
        producer_fingerprint: String,
    ) -> AppResult<ArtifactRef> {
        let artifact = ArtifactRef {
            id: self.ids.next_id(),
            stage,
            path: path.to_string_lossy().into_owned(),
            content_hash: blake3::hash(&bytes).to_hex().to_string(),
            schema_version: videocaptionerr_domain::SCHEMA_VERSION,
            producer_fingerprint,
        };
        let prepared = PreparedArtifact {
            job_id: job.id().clone(),
            artifact: artifact.clone(),
            source: ArtifactSource::Bytes { bytes },
        };
        let mut candidate = job.value.clone();
        candidate.complete_stage(stage, artifact.clone(), false)?;
        self.commit_atomic_job(job, candidate, Some(prepared))
            .await?;
        Ok(artifact)
    }

    pub(super) async fn commit_transcript_stage(
        &self,
        job: &mut Versioned<Job>,
        stage: StageKind,
        path: PathBuf,
        transcript: Transcript,
        producer_fingerprint: String,
        degraded: bool,
    ) -> AppResult<ArtifactRef> {
        let bytes = serde_json::to_vec_pretty(&transcript).map_err(|error| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode transcript artifact: {error}"),
            ))
        })?;
        let artifact = ArtifactRef {
            id: self.ids.next_id(),
            stage,
            path: path.to_string_lossy().into_owned(),
            content_hash: blake3::hash(&bytes).to_hex().to_string(),
            schema_version: videocaptionerr_domain::SCHEMA_VERSION,
            producer_fingerprint: producer_fingerprint.clone(),
        };
        let prepared = PreparedArtifact {
            job_id: job.id().clone(),
            artifact: artifact.clone(),
            source: ArtifactSource::Bytes { bytes },
        };
        let mut candidate = job.value.clone();
        candidate.complete_stage(stage, artifact.clone(), degraded)?;
        self.commit_atomic_job(job, candidate, Some(prepared))
            .await?;
        Ok(artifact)
    }

    pub(super) async fn commit_export_stage(
        &self,
        job: &mut Versioned<Job>,
        artifact: ArtifactRef,
    ) -> AppResult<()> {
        let prepared = PreparedArtifact {
            job_id: job.id().clone(),
            artifact: artifact.clone(),
            source: ArtifactSource::ExistingFile {
                path: PathBuf::from(&artifact.path),
            },
        };
        let mut candidate = job.value.clone();
        candidate.complete_stage(StageKind::Export, artifact, false)?;
        self.commit_atomic_job(job, candidate, Some(prepared)).await
    }

    pub(super) async fn commit_skip_stage(
        &self,
        job: &mut Versioned<Job>,
        stage: StageKind,
    ) -> AppResult<()> {
        let mut candidate = job.value.clone();
        candidate.skip_stage(stage)?;
        self.commit_atomic_job(job, candidate, None).await
    }

    pub(super) async fn commit_atomic_job(
        &self,
        job: &mut Versioned<Job>,
        candidate: Job,
        artifact: Option<PreparedArtifact>,
    ) -> AppResult<()> {
        let stage = artifact.as_ref().map(|value| value.artifact.stage);
        let event = stage_event(
            &candidate,
            stage,
            artifact.as_ref().map(|value| &value.artifact),
        );
        let request = StageCommitRequest {
            job: Some((
                Versioned::with_version(candidate.clone(), job.version),
                ExpectedVersion::Exact(job.version),
            )),
            work_unit: None,
            artifact,
            event: Some(event),
        };
        let result = self.stage_commits.commit_stage(request).await?;
        *job = result.job.ok_or_else(|| {
            ApplicationError::Invalid("atomic stage commit did not return Job".into())
        })?;
        Ok(())
    }
}

pub(super) fn stage_event(
    job: &Job,
    stage: Option<StageKind>,
    artifact: Option<&ArtifactRef>,
) -> OutboxEvent {
    let payload_json = serde_json::json!({
        "job_id": job.id().to_string(),
        "stage": stage.map(StageKind::as_str),
        "status": job.status(),
        "artifact_id": artifact.map(|value| value.id.to_string()),
    })
    .to_string();
    OutboxEvent {
        aggregate_type: "Job".into(),
        aggregate_id: job.id().to_string(),
        event_type: stage
            .map(|value| format!("stage_{}_committed", value.as_str()))
            .unwrap_or_else(|| "job_stage_updated".into()),
        payload_json,
        created_at: chrono::Utc::now().to_rfc3339(),
    }
}

pub(super) fn work_unit_event(unit: &WorkUnit, artifact: &ArtifactRef) -> OutboxEvent {
    OutboxEvent {
        aggregate_type: "WorkUnit".into(),
        aggregate_id: unit.id().to_string(),
        event_type: "work_unit_completed".into(),
        payload_json: serde_json::json!({
            "work_unit_id": unit.id().to_string(),
            "job_id": unit.job_id().to_string(),
            "stage": unit.stage().as_str(),
            "artifact_id": artifact.id.to_string(),
        })
        .to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
    }
}

pub(super) fn stage_is_pending(job: &Job, kind: StageKind) -> bool {
    job.stages()
        .iter()
        .find(|stage| stage.kind == kind)
        .is_some_and(|stage| stage.status == StageStatus::Pending)
}

pub(super) fn stage_is_done(job: &Job, kind: StageKind) -> bool {
    job.stages().iter().find(|stage| stage.kind == kind).is_some_and(
        |stage| matches!(stage.status, StageStatus::Done | StageStatus::DoneDegraded),
    )
}

pub(super) fn stage_artifact(job: &Job, kind: StageKind) -> AppResult<ArtifactRef> {
    job.stages()
        .iter()
        .find(|stage| stage.kind == kind)
        .and_then(|stage| stage.artifact.clone())
        .ok_or_else(|| ApplicationError::Invalid(format!("stage {kind:?} has no artifact")))
}

pub(super) fn normalized_options_hash(language: Option<&str>) -> String {
    let body = format!("language={language:?};word_timestamps=true");
    blake3::hash(body.as_bytes()).to_hex().to_string()
}

pub(super) fn decode_chunk_transcript(bytes: &[u8], key: &str) -> AppResult<Transcript> {
    let transcript: Transcript = serde_json::from_slice(bytes).map_err(|error| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::CacheCorrupt,
            format!("decode ASR chunk cache {key}: {error}"),
        ))
    })?;
    transcript.validate()?;
    Ok(transcript)
}
