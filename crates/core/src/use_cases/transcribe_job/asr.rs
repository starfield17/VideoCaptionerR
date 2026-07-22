use super::commit::{decode_chunk_transcript, normalized_options_hash, work_unit_event};
use super::long_audio::append_chunk_words;
use super::*;

pub(super) struct AsrExecutionContext {
    pub(super) source_hash: String,
    pub(super) pcm_hash: String,
    pub(super) audio_path: PathBuf,
    pub(super) duration_ms: u64,
    pub(super) cancel: AsrCancelToken,
}

impl TranscribeJob {
    pub(super) async fn transcribe_asr(
        &self,
        command: &TranscribeJobCommand,
        session: &mut dyn AsrSession,
        context: AsrExecutionContext,
    ) -> AppResult<Transcript> {
        let AsrExecutionContext {
            source_hash,
            pcm_hash,
            audio_path,
            duration_ms,
            cancel,
        } = context;
        let Some(max_audio_secs) = session.descriptor().max_audio_secs else {
            return self
                .transcribe_full_audio(
                    command,
                    session,
                    &source_hash,
                    audio_path,
                    duration_ms,
                    cancel.clone(),
                )
                .await;
        };
        let max_audio_ms = u64::from(max_audio_secs).saturating_mul(1000);
        if max_audio_ms == 0 || duration_ms <= max_audio_ms {
            return self
                .transcribe_full_audio(
                    command,
                    session,
                    &source_hash,
                    audio_path,
                    duration_ms,
                    cancel.clone(),
                )
                .await;
        }

        let ports = self.chunking.as_ref().ok_or_else(|| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::OptionUnsupported,
                "long-audio ASR requires chunk work-unit ports",
            ))
        })?;
        let analysis = self
            .media
            .analyze_audio(AudioAnalysisRequest {
                audio_path: audio_path.clone(),
                duration_ms,
            })
            .await?;
        self.ensure_not_cancelled(command, &cancel).await?;
        let mut options = ChunkPlanOptions::default();
        options.max_chunk_ms = options.max_chunk_ms.min(max_audio_ms);
        let plan = ChunkPlan::build(duration_ms, &analysis.silences, &analysis.energy, options)?;
        plan.validate()?;
        ports
            .plans
            .commit(ChunkPlanCommit {
                job_id: command.job_id.clone(),
                artifact_id: self.ids.next_id(),
                path: command.job_dir.join("01_chunk_plan.json"),
                plan: plan.clone(),
                producer_fingerprint: "rust-vad-chunk-planner".into(),
            })
            .await?;

        let options_hash = normalized_options_hash(command.language.as_deref());
        let mut words = Vec::new();
        let mut language = None;
        let mut engine = EngineFingerprint::unknown();
        let mut chunk_units = Vec::with_capacity(plan.chunks.len());
        for chunk in plan.chunks.iter().copied() {
            self.ensure_not_cancelled(command, &cancel).await?;
            let key = chunk_cache_key(
                &pcm_hash,
                &plan.plan_hash,
                chunk.index,
                &session.descriptor().fingerprint,
                &options_hash,
            );
            let existing = ports
                .work_units
                .find_work_unit(
                    &command.job_id,
                    StageKind::Asr,
                    "asr_chunk",
                    chunk.index,
                    &key,
                )
                .await?;
            let unit = match existing {
                Some(unit) => unit,
                None => {
                    let unit = WorkUnit::new(
                        self.ids.next_id(),
                        command.job_id.clone(),
                        StageKind::Asr,
                        "asr_chunk",
                        chunk.index,
                        key.clone(),
                    )?;
                    let mut versioned = Versioned::new(unit);
                    ports
                        .work_units
                        .save_work_unit(&mut versioned, ExpectedVersion::New)
                        .await?;
                    versioned
                }
            };
            chunk_units.push((chunk, key, unit));
        }

        for (chunk, key, unit) in chunk_units {
            self.ensure_not_cancelled(command, &cancel).await?;
            if unit.status() == WorkUnitStatus::Done {
                let raw = self.load_chunk_transcript(&unit, &key, ports).await?;
                append_chunk_words(&mut words, &mut language, &mut engine, raw, chunk)?;
                continue;
            }
            if unit.status() != WorkUnitStatus::Pending {
                return Err(ApplicationError::Invalid(format!(
                    "ASR chunk {} is {:?}; recover or retry its work unit first",
                    chunk.index,
                    unit.status()
                )));
            }

            let mut leased = ports
                .work_units
                .lease_next_ready(
                    &command.job_id,
                    StageKind::Asr,
                    &format!("asr:{}", command.job_id),
                    ports.clock.now_ms(),
                    600_000,
                )
                .await?
                .ok_or_else(|| {
                    ApplicationError::Invalid(format!(
                        "ASR chunk {} could not be leased",
                        chunk.index
                    ))
                })?;
            if leased.id() != unit.id() {
                return Err(ApplicationError::Invalid(
                    "ASR work-unit FIFO order changed while processing a ChunkPlan".into(),
                ));
            }

            let result: AppResult<Transcript> = async {
                let raw = match ports.cache.read(&key).await? {
                    Some(bytes) => decode_chunk_transcript(&bytes, &key)?,
                    None => {
                        let chunk_path = command
                            .job_dir
                            .join("asr-chunks")
                            .join(format!("chunk-{:04}.wav", chunk.index));
                        let extracted = self
                            .media
                            .extract_audio_range(ExtractAudioRangeRequest {
                                input_wav: audio_path.clone(),
                                read_start_ms: chunk.read_start_ms,
                                read_end_ms: chunk.read_end_ms,
                                output_path: chunk_path,
                            })
                            .await?;
                        self.ensure_not_cancelled(command, &cancel).await?;
                        let raw = session
                            .transcribe(
                                AsrTranscribeRequest {
                                    audio_path: extracted.wav_path,
                                    language: command.language.clone(),
                                    source_hash: source_hash.clone(),
                                    duration_ms: Some(chunk.read_end_ms - chunk.read_start_ms),
                                },
                                self.events.as_ref(),
                                Some(cancel.clone()),
                            )
                            .await?
                            .transcript;
                        raw.validate()?;
                        raw
                    }
                };
                raw.validate()?;
                self.ensure_not_cancelled(command, &cancel).await?;
                let bytes = serde_json::to_vec_pretty(&raw).map_err(|error| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("encode ASR chunk cache: {error}"),
                    ))
                })?;
                ports.cache.write(&key, &bytes).await?;
                let artifact = ArtifactRef {
                    id: self.ids.next_id(),
                    stage: StageKind::Asr,
                    path: command
                        .job_dir
                        .join("asr-chunks")
                        .join(format!("chunk-{:04}.json", chunk.index))
                        .to_string_lossy()
                        .into_owned(),
                    content_hash: blake3::hash(&bytes).to_hex().to_string(),
                    schema_version: videocaptionerr_domain::SCHEMA_VERSION,
                    producer_fingerprint: session.descriptor().fingerprint.clone(),
                };
                let prepared = PreparedArtifact {
                    job_id: command.job_id.clone(),
                    artifact: artifact.clone(),
                    source: ArtifactSource::Bytes { bytes },
                };
                self.ensure_not_cancelled(command, &cancel).await?;
                let mut completed = leased.clone();
                completed.complete(artifact.clone())?;
                let result = self
                    .stage_commits
                    .commit_stage(StageCommitRequest {
                        job: None,
                        work_unit: Some((completed, ExpectedVersion::Exact(leased.version))),
                        artifact: Some(prepared),
                        event: Some(work_unit_event(&leased.value, &artifact)),
                    })
                    .await?;
                leased = result.work_unit.ok_or_else(|| {
                    ApplicationError::Invalid(
                        "atomic ASR WorkUnit commit did not return WorkUnit".into(),
                    )
                })?;
                self.ensure_not_cancelled(command, &cancel).await?;
                Ok(raw)
            }
            .await;
            match result {
                Ok(raw) => append_chunk_words(&mut words, &mut language, &mut engine, raw, chunk)?,
                Err(error) => {
                    let primary = error.into_vc_error();
                    let state_result = if primary.code == ErrorCode::Cancelled {
                        if leased.status().is_terminal() {
                            Ok(())
                        } else {
                            leased.cancel()?;
                            let expected = leased.expected_version();
                            ports.work_units.save_work_unit(&mut leased, expected).await
                        }
                    } else {
                        match leased.fail(primary.code.as_str()) {
                            Ok(()) => {
                                let expected = leased.expected_version();
                                ports.work_units.save_work_unit(&mut leased, expected).await
                            }
                            Err(error) => Err(ApplicationError::Domain(error)),
                        }
                    };
                    match state_result {
                        Ok(()) => return Err(ApplicationError::Adapter(primary)),
                        Err(state) => {
                            return Err(ApplicationError::StatePersistence {
                                primary: Box::new(primary),
                                state: Box::new(state.into_vc_error()),
                            });
                        }
                    }
                }
            }
        }

        let mut transcript = Transcript::new_asr(source_hash.to_owned(), engine, words);
        transcript.language = language.or_else(|| command.language.clone());
        self.ensure_not_cancelled(command, &cancel).await?;
        transcript.validate()?;
        Ok(transcript)
    }

    pub(super) async fn transcribe_full_audio(
        &self,
        command: &TranscribeJobCommand,
        session: &mut dyn AsrSession,
        source_hash: &str,
        audio_path: PathBuf,
        duration_ms: u64,
        cancel: AsrCancelToken,
    ) -> AppResult<Transcript> {
        self.ensure_not_cancelled(command, &cancel).await?;
        Ok(session
            .transcribe(
                AsrTranscribeRequest {
                    audio_path,
                    language: command.language.clone(),
                    source_hash: source_hash.to_owned(),
                    duration_ms: Some(duration_ms),
                },
                self.events.as_ref(),
                Some(cancel),
            )
            .await?
            .transcript)
    }

    pub(super) async fn load_chunk_transcript(
        &self,
        unit: &WorkUnit,
        key: &str,
        ports: &ChunkingPorts,
    ) -> AppResult<Transcript> {
        if let Some(bytes) = ports.cache.read(key).await? {
            return decode_chunk_transcript(&bytes, key);
        }
        let artifact = unit.artifact().ok_or_else(|| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::ArtifactCorrupt,
                format!("completed ASR chunk {} has no artifact", unit.unit_index()),
            ))
        })?;
        self.artifacts.load_transcript(artifact).await
    }
}
