use super::*;

#[derive(Clone)]
pub struct SqliteArtifactStore {
    pub(super) store: StoreHandle,
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

    async fn load_probe_manifest(
        &self,
        artifact: &ArtifactRef,
    ) -> AppResult<videocaptionerr_core::ProbeManifest> {
        self.validate(artifact).await?;
        let body = fs::read_to_string(&artifact.path).map_err(|error| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::ArtifactCorrupt,
                format!("read probe manifest {}: {error}", artifact.path),
            ))
        })?;
        let manifest: videocaptionerr_core::ProbeManifest = serde_json::from_str(&body)
            .map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCorrupt,
                    format!("decode probe manifest {}: {error}", artifact.path),
                ))
            })?;
        manifest
            .validate()
            .map_err(|message| ApplicationError::Adapter(VcError::new(ErrorCode::ArtifactCorrupt, message)))?;
        Ok(manifest)
    }

    async fn load_extract_manifest(
        &self,
        artifact: &ArtifactRef,
    ) -> AppResult<videocaptionerr_core::ExtractManifest> {
        self.validate(artifact).await?;
        let body = fs::read_to_string(&artifact.path).map_err(|error| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::ArtifactCorrupt,
                format!("read extract manifest {}: {error}", artifact.path),
            ))
        })?;
        let manifest: videocaptionerr_core::ExtractManifest = serde_json::from_str(&body)
            .map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCorrupt,
                    format!("decode extract manifest {}: {error}", artifact.path),
                ))
            })?;
        manifest
            .validate()
            .map_err(|message| ApplicationError::Adapter(VcError::new(ErrorCode::ArtifactCorrupt, message)))?;
        Ok(manifest)
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

pub(crate) fn stage_name(stage: StageKind) -> &'static str {
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
