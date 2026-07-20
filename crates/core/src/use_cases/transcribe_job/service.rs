use super::commit::{stage_artifact, stage_is_pending};
use super::*;

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
            let source_hash = self.media.media_hash(&command.input).await?;
            let probe_pending = stage_is_pending(&job, StageKind::Probe);
            if probe_pending {
                job.start_stage(StageKind::Probe)?;
                current_stage = Some(StageKind::Probe);
            }
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
            if probe_pending {
                self.commit_input_stage(&mut job, probed.artifact).await?;
            }

            let extract_pending = stage_is_pending(&job, StageKind::ExtractAudio);
            if extract_pending {
                job.start_stage(StageKind::ExtractAudio)?;
                current_stage = Some(StageKind::ExtractAudio);
            }
            let extracted = self
                .media
                .extract_audio(crate::ports::ExtractAudioRequest {
                    input: command.input.clone(),
                    stream_index,
                    expected_duration_ms: Some(probed.probe.duration_ms),
                    job_dir: command.job_dir.clone(),
                })
                .await?;
            if extract_pending {
                self.commit_input_stage(&mut job, extracted.artifact)
                    .await?;
            }

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
                        &source_hash,
                        &extracted.pcm_hash,
                        &extracted.wav_path,
                        probed.probe.duration_ms,
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
                let artifact = stage_artifact(&job, StageKind::Asr)?;
                self.artifacts.load_transcript(&artifact).await?
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
                    let result = pipeline
                        .execute(&transcript, options.request(LlmStage::Split))
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
            } else {
                let artifact = stage_artifact(&job, StageKind::Split)?;
                self.artifacts.load_transcript(&artifact).await?
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
                    let corrected = pipeline
                        .execute(&final_transcript, options.request(LlmStage::Correct))
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
                } else {
                    let artifact = stage_artifact(&job, StageKind::Correct)?;
                    final_transcript = self.artifacts.load_transcript(&artifact).await?;
                }

                if stage_is_pending(&job, StageKind::Translate) {
                    job.start_stage(StageKind::Translate)?;
                    current_stage = Some(StageKind::Translate);
                    let translated = pipeline
                        .execute(&final_transcript, options.request(LlmStage::Translate))
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
                } else {
                    let artifact = stage_artifact(&job, StageKind::Translate)?;
                    final_transcript = self.artifacts.load_transcript(&artifact).await?;
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
                self.artifacts.validate(&artifact).await?;
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
}
