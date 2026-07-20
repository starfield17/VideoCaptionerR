use super::commit::{stage_artifact, stage_is_done, stage_is_pending};
use super::*;
use crate::artifacts::{
    ExtractManifest, ProbeManifest, EXTRACT_MANIFEST_SCHEMA_VERSION, PROBE_MANIFEST_SCHEMA_VERSION,
};
use crate::execution_snapshot::{JobExecutionSnapshot, SourceStatSnapshot};

impl TranscribeJob {
    pub async fn execute(
        &self,
        command: TranscribeJobCommand,
        session: &mut dyn AsrSession,
    ) -> AppResult<TranscribeJobResponse> {
        let (mut job, new_job) = match self.jobs.load_job(&command.job_id).await? {
            Some(existing) if existing.status() == videocaptionerr_domain::JobStatus::Pending => {
                if existing.execution_snapshot_id() != Some(&command.execution_snapshot_id) {
                    return Err(ApplicationError::Invalid(format!(
                        "Job {} is bound to a different execution snapshot",
                        command.job_id
                    )));
                }
                if existing.batch_id() != command.batch_id.as_ref() {
                    return Err(ApplicationError::Invalid(format!(
                        "Job {} belongs to a different Batch",
                        command.job_id
                    )));
                }
                if existing.source_path() != command.input.to_string_lossy() {
                    return Err(ApplicationError::Invalid(format!(
                        "Job {} source identity does not match its execution snapshot",
                        command.job_id
                    )));
                }
                (existing, false)
            }
            Some(existing) => {
                return Err(ApplicationError::Invalid(format!(
                    "Job {} is {:?}; call retry before executing it again",
                    command.job_id,
                    existing.status()
                )))
            }
            None => (
                Versioned::new(Job::new_with_snapshot(
                    command.job_id.clone(),
                    command.batch_id.clone(),
                    command.execution_snapshot_id.clone(),
                    command.profile_revision.clone(),
                    command.input.to_string_lossy(),
                )),
                true,
            ),
        };
        let expected = if new_job {
            ExpectedVersion::New
        } else {
            job.expected_version()
        };
        self.jobs.save_job(&mut job, expected).await?;
        job.start()?;
        self.save_job(&mut job).await?;

        let mut current_stage = None;
        let result: AppResult<TranscribeJobResponse> = async {
            // Source identity is validated against the frozen snapshot before
            // any stage work. A changed path/size/mtime fails closed.
            if let Some(snapshot) = self.snapshot_for(&command).await? {
                validate_source_against_snapshot(&command.input, &snapshot)?;
            }

            let probe = self
                .ensure_probe(&mut job, &command, &mut current_stage)
                .await?;
            let extract = self
                .ensure_extract(&mut job, &command, &probe, &mut current_stage)
                .await?;

            let asr_pending = stage_is_pending(&job, StageKind::Asr);
            if asr_pending {
                job.start_stage(StageKind::Asr)?;
                current_stage = Some(StageKind::Asr);
            }
            if !session.descriptor().supports_word_timestamps {
                return Err(ApplicationError::Adapter(VcError::new(
                    ErrorCode::EngineCapabilityInsufficient,
                    "ASR session does not provide word timestamps",
                )));
            }
            let asr_transcript = if asr_pending {
                let transcript = self
                    .transcribe_asr(
                        &command,
                        session,
                        &probe.source_hash,
                        &extract.pcm_hash,
                        &extract.wav_path_buf(),
                        probe.probe.duration_ms,
                    )
                    .await?;
                transcript.validate()?;
                self.commit_transcript_stage(
                    &mut job,
                    StageKind::Asr,
                    command.job_dir.join("01_asr.json"),
                    transcript.clone(),
                    session.descriptor().fingerprint.clone(),
                    false,
                )
                .await?;
                transcript
            } else {
                self.load_stage_transcript(&job, StageKind::Asr).await?
            };

            let split_pending = stage_is_pending(&job, StageKind::Split);
            let mut final_transcript = if split_pending {
                job.start_stage(StageKind::Split)?;
                current_stage = Some(StageKind::Split);
                let mut transcript = videocaptionerr_domain::rule_split(
                    &asr_transcript,
                    &videocaptionerr_domain::RuleSplitConfig::default(),
                )?;
                let mut degraded = false;
                if let Some(options) = &command.llm {
                    let pipeline = self.llm.as_ref().ok_or_else(|| {
                        ApplicationError::Adapter(VcError::new(
                            ErrorCode::LlmProviderUnavailable,
                            "LLM process stages are not configured",
                        ))
                    })?;
                    let durable = Some(crate::use_cases::llm_pipeline::LlmDurableContext {
                        job_id: command.job_id.clone(),
                        job_dir: command.job_dir.clone(),
                        input_artifact_id: stage_artifact(&job, StageKind::Asr)
                            .ok()
                            .map(|a| a.id.to_string()),
                        transcript_revision: transcript.revision,
                        invalidate_plan: false,
                    });
                    let result = pipeline
                        .execute(&transcript, options.request(LlmStage::Split, durable))
                        .await?;
                    degraded = !result.degraded_cue_ids.is_empty();
                    transcript = result.transcript;
                }
                self.commit_transcript_stage(
                    &mut job,
                    StageKind::Split,
                    command.job_dir.join("02_split.json"),
                    transcript.clone(),
                    if command.llm.is_some() {
                        "llm-split".into()
                    } else {
                        "domain-rule-split".into()
                    },
                    degraded,
                )
                .await?;
                transcript
            } else if stage_is_done(&job, StageKind::Split) {
                self.load_stage_transcript(&job, StageKind::Split).await?
            } else {
                asr_transcript
            };

            if let Some(options) = &command.llm {
                let pipeline = self.llm.as_ref().ok_or_else(|| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::LlmProviderUnavailable,
                        "LLM process stages are not configured",
                    ))
                })?;

                if stage_is_pending(&job, StageKind::Correct) {
                    job.start_stage(StageKind::Correct)?;
                    current_stage = Some(StageKind::Correct);
                    let durable = Some(crate::use_cases::llm_pipeline::LlmDurableContext {
                        job_id: command.job_id.clone(),
                        job_dir: command.job_dir.clone(),
                        input_artifact_id: stage_artifact(&job, StageKind::Split)
                            .ok()
                            .map(|a| a.id.to_string()),
                        transcript_revision: final_transcript.revision,
                        invalidate_plan: false,
                    });
                    let corrected = pipeline
                        .execute(
                            &final_transcript,
                            options.request(LlmStage::Correct, durable),
                        )
                        .await?;
                    let degraded = !corrected.degraded_cue_ids.is_empty();
                    final_transcript = corrected.transcript;
                    self.commit_transcript_stage(
                        &mut job,
                        StageKind::Correct,
                        command.job_dir.join("03_correct.json"),
                        final_transcript.clone(),
                        "llm-correction".into(),
                        degraded,
                    )
                    .await?;
                } else if stage_is_done(&job, StageKind::Correct) {
                    final_transcript = self.load_stage_transcript(&job, StageKind::Correct).await?;
                }

                if stage_is_pending(&job, StageKind::Translate) {
                    job.start_stage(StageKind::Translate)?;
                    current_stage = Some(StageKind::Translate);
                    let durable = Some(crate::use_cases::llm_pipeline::LlmDurableContext {
                        job_id: command.job_id.clone(),
                        job_dir: command.job_dir.clone(),
                        input_artifact_id: stage_artifact(&job, StageKind::Correct)
                            .or_else(|_| stage_artifact(&job, StageKind::Split))
                            .ok()
                            .map(|a| a.id.to_string()),
                        transcript_revision: final_transcript.revision,
                        invalidate_plan: false,
                    });
                    let translated = pipeline
                        .execute(
                            &final_transcript,
                            options.request(LlmStage::Translate, durable),
                        )
                        .await?;
                    let degraded = !translated.degraded_cue_ids.is_empty();
                    final_transcript = translated.transcript;
                    self.commit_transcript_stage(
                        &mut job,
                        StageKind::Translate,
                        command.job_dir.join("04_translate.json"),
                        final_transcript.clone(),
                        "llm-translation".into(),
                        degraded,
                    )
                    .await?;
                } else if stage_is_done(&job, StageKind::Translate) {
                    final_transcript =
                        self.load_stage_transcript(&job, StageKind::Translate).await?;
                }
            } else {
                if stage_is_pending(&job, StageKind::Correct) {
                    current_stage = Some(StageKind::Correct);
                    self.commit_skip_stage(&mut job, StageKind::Correct).await?;
                }
                if stage_is_pending(&job, StageKind::Translate) {
                    current_stage = Some(StageKind::Translate);
                    self.commit_skip_stage(&mut job, StageKind::Translate)
                        .await?;
                }
            }

            let export_path = if stage_is_pending(&job, StageKind::Export) {
                job.start_stage(StageKind::Export)?;
                current_stage = Some(StageKind::Export);
                let exported = self
                    .subtitles
                    .export(&final_transcript, command.export)
                    .await?;
                let export_ref = ArtifactRef {
                    id: self.ids.next_id(),
                    stage: StageKind::Export,
                    path: exported.path.to_string_lossy().into_owned(),
                    content_hash: exported.content_hash,
                    schema_version: videocaptionerr_domain::SCHEMA_VERSION,
                    producer_fingerprint: "subtitle-writer".into(),
                };
                self.commit_export_stage(&mut job, export_ref.clone())
                    .await?;
                exported.path
            } else {
                let artifact = stage_artifact(&job, StageKind::Export)?;
                self.artifacts.validate(&artifact).await.map_err(|error| {
                    map_corrupt(error, "export artifact is missing or hash-invalid")
                })?;
                PathBuf::from(artifact.path)
            };
            job.finish()?;
            self.save_job(&mut job).await?;

            Ok(TranscribeJobResponse {
                job: job.value.clone(),
                transcript: final_transcript,
                export_path,
            })
        }
        .await;

        if let Err(error) = result {
            if let Some(stage) = current_stage {
                if job.fail_stage(stage).is_err() && !job.status().is_terminal() {
                    let _ = job.cancel();
                }
            } else if !job.status().is_terminal() {
                let _ = job.cancel();
            }
            let primary = error.into_vc_error();
            match self.save_job(&mut job).await {
                Ok(()) => return Err(ApplicationError::Adapter(primary)),
                Err(state) => {
                    return Err(ApplicationError::StatePersistence {
                        primary: Box::new(primary),
                        state: Box::new(state.into_vc_error()),
                    });
                }
            }
        }
        result
    }

    async fn snapshot_for(
        &self,
        command: &TranscribeJobCommand,
    ) -> AppResult<Option<JobExecutionSnapshot>> {
        let Some(loader) = &self.snapshots else {
            return Ok(None);
        };
        loader
            .load_execution_snapshot(&command.execution_snapshot_id)
            .await
    }

    async fn ensure_probe(
        &self,
        job: &mut Versioned<Job>,
        command: &TranscribeJobCommand,
        current_stage: &mut Option<StageKind>,
    ) -> AppResult<ProbeManifest> {
        if stage_is_pending(job, StageKind::Probe) {
            job.start_stage(StageKind::Probe)?;
            *current_stage = Some(StageKind::Probe);
            let source_hash = self.media.media_hash(&command.input).await?;
            let source_stat = current_source_stat(&command.input)?;
            let probed = self
                .media
                .probe(ProbeMediaRequest {
                    input: command.input.clone(),
                    job_dir: command.job_dir.clone(),
                })
                .await?;
            if !probed.probe.has_audio() {
                return Err(ApplicationError::Adapter(VcError::new(
                    ErrorCode::AudioStreamNotFound,
                    "no audio streams",
                )));
            }
            let stream_index = probed
                .probe
                .auto_select_stream()
                .or_else(|| probed.probe.default_stream())
                .ok_or_else(|| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::AudioStreamNotFound,
                        "no selectable audio stream",
                    ))
                })?
                .stream_index;
            let manifest = ProbeManifest {
                schema_version: PROBE_MANIFEST_SCHEMA_VERSION,
                source_path: command.input.to_string_lossy().into_owned(),
                source_stat,
                source_hash,
                probe: probed.probe,
                selected_stream_index: stream_index,
                producer: probed.artifact.producer_fingerprint.clone(),
            };
            manifest
                .validate()
                .map_err(|message| ApplicationError::Invalid(message))?;
            let bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("encode probe manifest: {error}"),
                ))
            })?;
            let path = command.job_dir.join("00_probe.json");
            self.commit_bytes_stage(
                job,
                StageKind::Probe,
                path,
                bytes,
                manifest.producer.clone(),
            )
            .await?;
            return Ok(manifest);
        }

        if stage_is_done(job, StageKind::Probe) {
            let artifact = stage_artifact(job, StageKind::Probe)?;
            let manifest = self
                .artifacts
                .load_probe_manifest(&artifact)
                .await
                .map_err(|error| map_corrupt(error, "probe artifact is corrupt"))?;
            let current_hash = self.media.media_hash(&command.input).await?;
            if current_hash != manifest.source_hash {
                return Err(ApplicationError::Adapter(VcError::new(
                    ErrorCode::SourceChanged,
                    format!(
                        "source content hash changed for {}",
                        command.input.display()
                    ),
                )));
            }
            return Ok(manifest);
        }

        Err(ApplicationError::Invalid(format!(
            "probe stage is {:?} and cannot be reused or executed",
            stage_status(job, StageKind::Probe)
        )))
    }

    async fn ensure_extract(
        &self,
        job: &mut Versioned<Job>,
        command: &TranscribeJobCommand,
        probe: &ProbeManifest,
        current_stage: &mut Option<StageKind>,
    ) -> AppResult<ExtractManifest> {
        if stage_is_pending(job, StageKind::ExtractAudio) {
            job.start_stage(StageKind::ExtractAudio)?;
            *current_stage = Some(StageKind::ExtractAudio);
            let extracted = self
                .media
                .extract_audio(crate::ports::ExtractAudioRequest {
                    input: command.input.clone(),
                    stream_index: probe.selected_stream_index,
                    expected_duration_ms: Some(probe.probe.duration_ms),
                    job_dir: command.job_dir.clone(),
                })
                .await?;
            let stream = probe
                .probe
                .audio_streams
                .iter()
                .find(|stream| stream.stream_index == probe.selected_stream_index)
                .ok_or_else(|| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::AudioStreamNotFound,
                        "selected stream missing from probe manifest",
                    ))
                })?;
            let probe_artifact_id = stage_artifact(job, StageKind::Probe)?.id;
            let manifest = ExtractManifest {
                schema_version: EXTRACT_MANIFEST_SCHEMA_VERSION,
                probe_artifact_id,
                stream_index: probe.selected_stream_index,
                wav_path: extracted.wav_path.to_string_lossy().into_owned(),
                wav_content_hash: extracted.artifact.content_hash.clone(),
                pcm_hash: extracted.pcm_hash,
                sample_rate: stream.sample_rate,
                channels: stream.channels,
                duration_ms: probe.probe.duration_ms,
                producer: extracted.artifact.producer_fingerprint.clone(),
            };
            manifest
                .validate()
                .map_err(|message| ApplicationError::Invalid(message))?;
            let bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("encode extract manifest: {error}"),
                ))
            })?;
            let path = command.job_dir.join("00_extract.json");
            self.commit_bytes_stage(
                job,
                StageKind::ExtractAudio,
                path,
                bytes,
                manifest.producer.clone(),
            )
            .await?;
            return Ok(manifest);
        }

        if stage_is_done(job, StageKind::ExtractAudio) {
            let artifact = stage_artifact(job, StageKind::ExtractAudio)?;
            let manifest = self
                .artifacts
                .load_extract_manifest(&artifact)
                .await
                .map_err(|error| map_corrupt(error, "extract artifact is corrupt"))?;
            // Validate the WAV body against the committed hash. Corruption
            // must surface as ARTIFACT_CORRUPT, never as a silent re-extract.
            self.artifacts
                .validate(&ArtifactRef {
                    id: artifact.id.clone(),
                    stage: StageKind::ExtractAudio,
                    path: manifest.wav_path.clone(),
                    content_hash: manifest.wav_content_hash.clone(),
                    schema_version: artifact.schema_version,
                    producer_fingerprint: artifact.producer_fingerprint.clone(),
                })
                .await
                .map_err(|error| map_corrupt(error, "extracted WAV hash mismatch"))?;
            return Ok(manifest);
        }

        Err(ApplicationError::Invalid(format!(
            "extract stage is {:?} and cannot be reused or executed",
            stage_status(job, StageKind::ExtractAudio)
        )))
    }

    async fn load_stage_transcript(
        &self,
        job: &Job,
        stage: StageKind,
    ) -> AppResult<Transcript> {
        let artifact = stage_artifact(job, stage)?;
        self.artifacts
            .load_transcript(&artifact)
            .await
            .map_err(|error| map_corrupt(error, &format!("{stage:?} transcript is corrupt")))
    }
}

fn stage_status(job: &Job, kind: StageKind) -> StageStatus {
    job.stages()
        .iter()
        .find(|stage| stage.kind == kind)
        .map(|stage| stage.status)
        .unwrap_or(StageStatus::Pending)
}

fn map_corrupt(error: ApplicationError, message: &str) -> ApplicationError {
    let vc = error.into_vc_error();
    if vc.code == ErrorCode::ArtifactCorrupt {
        ApplicationError::Adapter(vc)
    } else {
        ApplicationError::Adapter(VcError::new(ErrorCode::ArtifactCorrupt, message).with_detail(vc.message))
    }
}

fn current_source_stat(path: &Path) -> AppResult<SourceStatSnapshot> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::InputNotFound,
            format!("read source metadata {}: {error}", path.display()),
        ))
    })?;
    let modified_at_ms = metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
        .and_then(|value| u64::try_from(value.as_millis()).ok());
    Ok(SourceStatSnapshot {
        size: metadata.len(),
        modified_at_ms,
    })
}

fn validate_source_against_snapshot(
    path: &Path,
    snapshot: &JobExecutionSnapshot,
) -> AppResult<()> {
    if path.to_string_lossy() != snapshot.canonical_source_path {
        return Err(ApplicationError::Adapter(VcError::new(
            ErrorCode::SourceChanged,
            format!(
                "source path changed: expected {}, got {}",
                snapshot.canonical_source_path,
                path.display()
            ),
        )));
    }
    let current = current_source_stat(path)?;
    if current.size != snapshot.source_stat.size
        || current.modified_at_ms != snapshot.source_stat.modified_at_ms
    {
        return Err(ApplicationError::Adapter(VcError::new(
            ErrorCode::SourceChanged,
            format!(
                "source size/mtime changed for {}",
                snapshot.canonical_source_path
            ),
        )));
    }
    Ok(())
}
