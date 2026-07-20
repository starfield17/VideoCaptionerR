//! One-job transcription orchestration.
//!
//! The ASR session is supplied by RunBatch. This use case therefore never
//! loads, unloads, or switches an ASR model.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use videocaptionerr_contracts::error::{ErrorCode, VcError};
use videocaptionerr_domain::{
    ArtifactRef, BatchId, EngineFingerprint, Job, JobId, StageKind, StageStatus, Transcript,
    UlidStr, WorkUnit, WorkUnitStatus,
};

use super::llm_pipeline::LlmPipelineRequest;
use crate::application_error::{AppResult, ApplicationError};
use crate::chunking::{
    apply_chunk_offset, chunk_cache_key, retain_core_words, ChunkPlan, ChunkPlanOptions,
};
use crate::ports::{
    ArtifactCommit, ArtifactInput, ArtifactStore, AsrSession, AsrTranscribeRequest,
    AudioAnalysisRequest, CacheRepository, ChunkPlanCommit, ChunkPlanStore, Clock, EventPublisher,
    ExtractAudioRangeRequest, IdGenerator, JobRepository, LlmStage, MediaGateway,
    ProbeMediaRequest, PromptSnapshot, StructuredOutput, SubtitleExportRequest, SubtitleGateway,
    TranscriptCommit, WorkUnitRepository,
};

#[derive(Debug, Clone)]
pub struct LlmProcessOptions {
    pub model: String,
    pub provider_profile_revision: String,
    pub split_prompt: PromptSnapshot,
    pub correct_prompt: PromptSnapshot,
    pub translate_prompt: PromptSnapshot,
    pub max_context_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub chars_per_token: f64,
    pub structured_output: StructuredOutput,
    pub seed: Option<i64>,
    pub target_language: String,
}

impl LlmProcessOptions {
    fn request(&self, stage: LlmStage) -> LlmPipelineRequest {
        let prompt = match stage {
            LlmStage::Split => self.split_prompt.clone(),
            LlmStage::Correct => self.correct_prompt.clone(),
            LlmStage::Translate => self.translate_prompt.clone(),
        };
        LlmPipelineRequest {
            stage,
            model: self.model.clone(),
            provider_profile_revision: self.provider_profile_revision.clone(),
            prompt,
            max_context_tokens: self.max_context_tokens,
            max_output_tokens: self.max_output_tokens,
            chars_per_token: self.chars_per_token,
            structured_output: self.structured_output,
            seed: self.seed,
            target_language: Some(self.target_language.clone()),
        }
    }
}

pub struct TranscribeJobCommand {
    pub job_id: JobId,
    pub batch_id: Option<BatchId>,
    pub profile_revision: UlidStr,
    pub input: PathBuf,
    pub job_dir: PathBuf,
    pub language: Option<String>,
    pub export: SubtitleExportRequest,
    pub llm: Option<LlmProcessOptions>,
}

#[derive(Debug)]
pub struct TranscribeJobResponse {
    pub job: Job,
    pub transcript: Transcript,
    pub export_path: PathBuf,
}

pub struct TranscribeJob {
    jobs: Arc<dyn JobRepository>,
    media: Arc<dyn MediaGateway>,
    artifacts: Arc<dyn ArtifactStore>,
    subtitles: Arc<dyn SubtitleGateway>,
    events: Arc<dyn EventPublisher>,
    ids: Arc<dyn IdGenerator>,
    llm: Option<Arc<super::llm_pipeline::LlmPipeline>>,
    chunking: Option<ChunkingPorts>,
}

struct ChunkingPorts {
    plans: Arc<dyn ChunkPlanStore>,
    cache: Arc<dyn CacheRepository>,
    work_units: Arc<dyn WorkUnitRepository>,
    clock: Arc<dyn Clock>,
}

impl TranscribeJob {
    pub fn new(
        jobs: Arc<dyn JobRepository>,
        media: Arc<dyn MediaGateway>,
        artifacts: Arc<dyn ArtifactStore>,
        subtitles: Arc<dyn SubtitleGateway>,
        events: Arc<dyn EventPublisher>,
        ids: Arc<dyn IdGenerator>,
    ) -> Self {
        Self {
            jobs,
            media,
            artifacts,
            subtitles,
            events,
            ids,
            llm: None,
            chunking: None,
        }
    }

    pub fn with_llm_pipeline(mut self, pipeline: Arc<super::llm_pipeline::LlmPipeline>) -> Self {
        self.llm = Some(pipeline);
        self
    }

    pub fn with_chunking(
        mut self,
        plans: Arc<dyn ChunkPlanStore>,
        cache: Arc<dyn CacheRepository>,
        work_units: Arc<dyn WorkUnitRepository>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        self.chunking = Some(ChunkingPorts {
            plans,
            cache,
            work_units,
            clock,
        });
        self
    }

    pub async fn execute(
        &self,
        command: TranscribeJobCommand,
        session: &mut dyn AsrSession,
    ) -> AppResult<TranscribeJobResponse> {
        let mut job = match self.jobs.load_job(&command.job_id).await? {
            Some(existing) if existing.status() == videocaptionerr_domain::JobStatus::Pending => {
                existing
            }
            Some(existing) => {
                return Err(ApplicationError::Invalid(format!(
                    "Job {} is {:?}; call retry before executing it again",
                    command.job_id,
                    existing.status()
                )))
            }
            None => Job::new(
                command.job_id.clone(),
                command.batch_id.clone(),
                command.profile_revision.clone(),
                command.input.to_string_lossy(),
            ),
        };
        self.jobs.save_job(&job).await?;
        job.start()?;
        self.jobs.save_job(&job).await?;

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
                let probe_artifact = self.commit_input(&command.job_id, probed.artifact).await?;
                job.complete_stage(StageKind::Probe, probe_artifact, false)?;
                self.jobs.save_job(&job).await?;
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
                let extract_artifact = self
                    .commit_input(&command.job_id, extracted.artifact)
                    .await?;
                job.complete_stage(StageKind::ExtractAudio, extract_artifact, false)?;
                self.jobs.save_job(&job).await?;
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
                let asr_artifact = self
                    .artifacts
                    .commit_transcript(TranscriptCommit {
                        job_id: command.job_id.clone(),
                        stage: StageKind::Asr,
                        artifact_id: self.ids.next_id(),
                        path: command.job_dir.join("01_asr.json"),
                        transcript: transcript.clone(),
                        producer_fingerprint: session.descriptor().fingerprint.clone(),
                        work_unit_id: None,
                    })
                    .await?;
                job.complete_stage(StageKind::Asr, asr_artifact, false)?;
                self.jobs.save_job(&job).await?;
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
                let artifact = self
                    .artifacts
                    .commit_transcript(TranscriptCommit {
                        job_id: command.job_id.clone(),
                        stage: StageKind::Split,
                        artifact_id: self.ids.next_id(),
                        path: command.job_dir.join("02_split.json"),
                        transcript: transcript.clone(),
                        producer_fingerprint: if command.llm.is_some() {
                            "llm-split".into()
                        } else {
                            "domain-rule-split".into()
                        },
                        work_unit_id: None,
                    })
                    .await?;
                job.complete_stage(StageKind::Split, artifact, degraded)?;
                self.jobs.save_job(&job).await?;
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
                    let artifact = self
                        .artifacts
                        .commit_transcript(TranscriptCommit {
                            job_id: command.job_id.clone(),
                            stage: StageKind::Correct,
                            artifact_id: self.ids.next_id(),
                            path: command.job_dir.join("03_correct.json"),
                            transcript: final_transcript.clone(),
                            producer_fingerprint: "llm-correction".into(),
                            work_unit_id: None,
                        })
                        .await?;
                    job.complete_stage(StageKind::Correct, artifact, degraded)?;
                    self.jobs.save_job(&job).await?;
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
                    let artifact = self
                        .artifacts
                        .commit_transcript(TranscriptCommit {
                            job_id: command.job_id.clone(),
                            stage: StageKind::Translate,
                            artifact_id: self.ids.next_id(),
                            path: command.job_dir.join("04_translate.json"),
                            transcript: final_transcript.clone(),
                            producer_fingerprint: "llm-translation".into(),
                            work_unit_id: None,
                        })
                        .await?;
                    job.complete_stage(StageKind::Translate, artifact, degraded)?;
                    self.jobs.save_job(&job).await?;
                } else {
                    let artifact = stage_artifact(&job, StageKind::Translate)?;
                    final_transcript = self.artifacts.load_transcript(&artifact).await?;
                }
            } else {
                if stage_is_pending(&job, StageKind::Correct) {
                    current_stage = Some(StageKind::Correct);
                    job.skip_stage(StageKind::Correct)?;
                }
                if stage_is_pending(&job, StageKind::Translate) {
                    current_stage = Some(StageKind::Translate);
                    job.skip_stage(StageKind::Translate)?;
                }
            }
            self.jobs.save_job(&job).await?;

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
                self.artifacts
                    .commit(ArtifactCommit {
                        job_id: command.job_id.clone(),
                        artifact: export_ref.clone(),
                        work_unit_id: None,
                    })
                    .await?;
                let path = exported.path;
                job.complete_stage(StageKind::Export, export_ref, false)?;
                path
            } else {
                let artifact = stage_artifact(&job, StageKind::Export)?;
                self.artifacts.validate(&artifact).await?;
                PathBuf::from(artifact.path)
            };
            job.finish()?;
            self.jobs.save_job(&job).await?;

            Ok(TranscribeJobResponse {
                job: job.clone(),
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
            let _ = self.jobs.save_job(&job).await;
            return Err(error);
        }
        result
    }

    async fn commit_input(&self, job_id: &JobId, input: ArtifactInput) -> AppResult<ArtifactRef> {
        let artifact = ArtifactRef {
            id: self.ids.next_id(),
            stage: input.stage,
            path: input.path.to_string_lossy().into_owned(),
            content_hash: input.content_hash,
            schema_version: input.schema_version,
            producer_fingerprint: input.producer_fingerprint,
        };
        self.artifacts
            .commit(ArtifactCommit {
                job_id: job_id.clone(),
                artifact: artifact.clone(),
                work_unit_id: None,
            })
            .await?;
        Ok(artifact)
    }

    async fn transcribe_asr(
        &self,
        command: &TranscribeJobCommand,
        session: &mut dyn AsrSession,
        source_hash: &str,
        pcm_hash: &str,
        audio_path: &Path,
        duration_ms: u64,
    ) -> AppResult<Transcript> {
        let Some(max_audio_secs) = session.descriptor().max_audio_secs else {
            return self
                .transcribe_full_audio(
                    command,
                    session,
                    source_hash,
                    audio_path.to_path_buf(),
                    duration_ms,
                )
                .await;
        };
        let max_audio_ms = u64::from(max_audio_secs).saturating_mul(1000);
        if max_audio_ms == 0 || duration_ms <= max_audio_ms {
            return self
                .transcribe_full_audio(
                    command,
                    session,
                    source_hash,
                    audio_path.to_path_buf(),
                    duration_ms,
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
                audio_path: audio_path.to_path_buf(),
                duration_ms,
            })
            .await?;
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
            let key = chunk_cache_key(
                pcm_hash,
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
                    ports.work_units.save_work_unit(&unit).await?;
                    unit
                }
            };
            chunk_units.push((chunk, key, unit));
        }

        for (chunk, key, unit) in chunk_units {
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
                                input_wav: audio_path.to_path_buf(),
                                read_start_ms: chunk.read_start_ms,
                                read_end_ms: chunk.read_end_ms,
                                output_path: chunk_path,
                            })
                            .await?;
                        let raw = session
                            .transcribe(
                                AsrTranscribeRequest {
                                    audio_path: extracted.wav_path,
                                    language: command.language.clone(),
                                    source_hash: source_hash.to_owned(),
                                    duration_ms: Some(chunk.read_end_ms - chunk.read_start_ms),
                                },
                                self.events.as_ref(),
                            )
                            .await?
                            .transcript;
                        raw.validate()?;
                        raw
                    }
                };
                raw.validate()?;
                let bytes = serde_json::to_vec_pretty(&raw).map_err(|error| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("encode ASR chunk cache: {error}"),
                    ))
                })?;
                ports.cache.write(&key, &bytes).await?;
                self.artifacts
                    .commit_transcript(TranscriptCommit {
                        job_id: command.job_id.clone(),
                        stage: StageKind::Asr,
                        artifact_id: self.ids.next_id(),
                        path: command
                            .job_dir
                            .join("asr-chunks")
                            .join(format!("chunk-{:04}.json", chunk.index)),
                        transcript: raw.clone(),
                        producer_fingerprint: session.descriptor().fingerprint.clone(),
                        work_unit_id: Some(leased.id().clone()),
                    })
                    .await?;
                Ok(raw)
            }
            .await;
            match result {
                Ok(raw) => append_chunk_words(&mut words, &mut language, &mut engine, raw, chunk)?,
                Err(error) => {
                    let vc_error = error.into_vc_error();
                    let _ = leased.fail(vc_error.code.as_str());
                    let _ = ports.work_units.save_work_unit(&leased).await;
                    return Err(ApplicationError::Adapter(vc_error));
                }
            }
        }

        let mut transcript = Transcript::new_asr(source_hash.to_owned(), engine, words);
        transcript.language = language.or_else(|| command.language.clone());
        transcript.validate()?;
        Ok(transcript)
    }

    async fn transcribe_full_audio(
        &self,
        command: &TranscribeJobCommand,
        session: &mut dyn AsrSession,
        source_hash: &str,
        audio_path: PathBuf,
        duration_ms: u64,
    ) -> AppResult<Transcript> {
        Ok(session
            .transcribe(
                AsrTranscribeRequest {
                    audio_path,
                    language: command.language.clone(),
                    source_hash: source_hash.to_owned(),
                    duration_ms: Some(duration_ms),
                },
                self.events.as_ref(),
            )
            .await?
            .transcript)
    }

    async fn load_chunk_transcript(
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

fn stage_is_pending(job: &Job, kind: StageKind) -> bool {
    job.stages()
        .iter()
        .find(|stage| stage.kind == kind)
        .is_some_and(|stage| stage.status == StageStatus::Pending)
}

fn stage_artifact(job: &Job, kind: StageKind) -> AppResult<ArtifactRef> {
    job.stages()
        .iter()
        .find(|stage| stage.kind == kind)
        .and_then(|stage| stage.artifact.clone())
        .ok_or_else(|| ApplicationError::Invalid(format!("stage {kind:?} has no artifact")))
}

fn normalized_options_hash(language: Option<&str>) -> String {
    let body = format!("language={language:?};word_timestamps=true");
    blake3::hash(body.as_bytes()).to_hex().to_string()
}

fn decode_chunk_transcript(bytes: &[u8], key: &str) -> AppResult<Transcript> {
    let transcript: Transcript = serde_json::from_slice(bytes).map_err(|error| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::CacheCorrupt,
            format!("decode ASR chunk cache {key}: {error}"),
        ))
    })?;
    transcript.validate()?;
    Ok(transcript)
}

fn append_chunk_words(
    words: &mut Vec<videocaptionerr_domain::Word>,
    language: &mut Option<String>,
    engine: &mut EngineFingerprint,
    raw: Transcript,
    chunk: crate::chunking::AudioChunk,
) -> AppResult<()> {
    if engine.engine_id == "unknown" {
        *engine = raw.engine.clone();
    }
    if language.is_none() {
        *language = raw.language.clone();
    }
    let shifted = apply_chunk_offset(&raw.words, chunk.read_start_ms);
    words.extend(retain_core_words(&shifted, chunk));
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    use async_trait::async_trait;
    use tempfile::tempdir;
    use ulid::Ulid;
    use videocaptionerr_contracts::media::{AudioStream, MediaProbe};
    use videocaptionerr_domain::{EngineFingerprint, Word, PROB_UNAVAILABLE, SCHEMA_VERSION};

    use super::*;
    use crate::application_error::AppResult;
    use crate::ports::{
        ArtifactStore, AsrDescriptor, AudioAnalysis, AudioExtraction, AudioRangeExtraction,
        CacheGcResult, CacheRepository, ChunkPlanCommit, ChunkPlanStore, Clock, ExportedSubtitle,
        ExtractAudioRangeRequest, ExtractAudioRequest, NormalizedAsrResult, ProbedMedia,
        SubtitleFormat, SubtitleLayout, WorkUnitRepository,
    };

    struct FakeIds;

    impl IdGenerator for FakeIds {
        fn next_id(&self) -> UlidStr {
            UlidStr::from(Ulid::new())
        }
    }

    struct FakeEvents;

    #[async_trait]
    impl EventPublisher for FakeEvents {
        async fn publish(&self, _event: videocaptionerr_domain::DomainEvent) -> AppResult<()> {
            Ok(())
        }
    }

    struct FakeJobs {
        saved: Mutex<Vec<Job>>,
    }

    #[async_trait]
    impl JobRepository for FakeJobs {
        async fn load_job(&self, _id: &JobId) -> AppResult<Option<Job>> {
            Ok(self.saved.lock().unwrap().last().cloned())
        }

        async fn save_job(&self, job: &Job) -> AppResult<()> {
            self.saved.lock().unwrap().push(job.clone());
            Ok(())
        }

        async fn delete_job(&self, _id: &JobId) -> AppResult<()> {
            Ok(())
        }

        async fn list_jobs(&self) -> AppResult<Vec<Job>> {
            Ok(self.saved.lock().unwrap().clone())
        }
    }

    struct FakeMedia;

    fn artifact(stage: StageKind, name: &str) -> ArtifactInput {
        ArtifactInput {
            stage,
            path: PathBuf::from(name),
            content_hash: format!("hash-{name}"),
            schema_version: SCHEMA_VERSION,
            producer_fingerprint: "fake-media".into(),
        }
    }

    #[async_trait]
    impl MediaGateway for FakeMedia {
        async fn probe(&self, _request: ProbeMediaRequest) -> AppResult<ProbedMedia> {
            Ok(ProbedMedia {
                probe: MediaProbe {
                    schema_version: SCHEMA_VERSION,
                    input_size: 10,
                    container: Some("wav".into()),
                    duration_ms: 1000,
                    audio_streams: vec![AudioStream {
                        stream_index: 0,
                        codec: "pcm_s16le".into(),
                        language: Some("en".into()),
                        title: None,
                        channels: 1,
                        sample_rate: 16_000,
                        is_default: true,
                    }],
                },
                artifact: artifact(StageKind::Probe, "probe.json"),
            })
        }

        async fn media_hash(&self, _input: &std::path::Path) -> AppResult<String> {
            Ok("media-hash".into())
        }

        async fn extract_audio(&self, request: ExtractAudioRequest) -> AppResult<AudioExtraction> {
            Ok(AudioExtraction {
                wav_path: request.job_dir.join("audio.wav"),
                pcm_hash: "pcm-hash".into(),
                artifact: artifact(StageKind::ExtractAudio, "audio.wav"),
            })
        }
    }

    #[derive(Default)]
    struct LongMedia {
        ranges: Mutex<Vec<(u64, u64, PathBuf)>>,
        analyses: Mutex<u32>,
    }

    #[async_trait]
    impl MediaGateway for LongMedia {
        async fn probe(&self, _request: ProbeMediaRequest) -> AppResult<ProbedMedia> {
            Ok(ProbedMedia {
                probe: MediaProbe {
                    schema_version: SCHEMA_VERSION,
                    input_size: 1,
                    container: Some("wav".into()),
                    duration_ms: 250_000,
                    audio_streams: vec![AudioStream {
                        stream_index: 0,
                        codec: "pcm_s16le".into(),
                        language: Some("en".into()),
                        title: None,
                        channels: 1,
                        sample_rate: 16_000,
                        is_default: true,
                    }],
                },
                artifact: artifact(StageKind::Probe, "probe.json"),
            })
        }

        async fn media_hash(&self, _input: &Path) -> AppResult<String> {
            Ok("media-hash".into())
        }

        async fn extract_audio(&self, request: ExtractAudioRequest) -> AppResult<AudioExtraction> {
            Ok(AudioExtraction {
                wav_path: request.job_dir.join("audio.wav"),
                pcm_hash: "pcm-hash".into(),
                artifact: artifact(StageKind::ExtractAudio, "audio.wav"),
            })
        }

        async fn analyze_audio(&self, _request: AudioAnalysisRequest) -> AppResult<AudioAnalysis> {
            *self.analyses.lock().unwrap() += 1;
            Ok(AudioAnalysis {
                silences: vec![crate::chunking::SilenceRegion {
                    start_ms: 119_000,
                    end_ms: 121_000,
                }],
                energy: Vec::new(),
            })
        }

        async fn extract_audio_range(
            &self,
            request: ExtractAudioRangeRequest,
        ) -> AppResult<AudioRangeExtraction> {
            self.ranges.lock().unwrap().push((
                request.read_start_ms,
                request.read_end_ms,
                request.output_path.clone(),
            ));
            Ok(AudioRangeExtraction {
                wav_path: request.output_path,
                pcm_hash: "chunk-pcm-hash".into(),
            })
        }
    }

    struct FakeArtifacts {
        committed: Mutex<Vec<ArtifactRef>>,
        transcripts: Mutex<HashMap<String, Transcript>>,
        fail_export_once: Option<Arc<AtomicBool>>,
    }

    #[async_trait]
    impl ArtifactStore for FakeArtifacts {
        async fn commit(&self, commit: ArtifactCommit) -> AppResult<()> {
            if commit.artifact.stage == StageKind::Export
                && self
                    .fail_export_once
                    .as_ref()
                    .is_some_and(|flag| flag.swap(false, Ordering::SeqCst))
            {
                return Err(ApplicationError::Adapter(VcError::new(
                    ErrorCode::ExportFailed,
                    "injected export artifact commit failure",
                )));
            }
            self.committed.lock().unwrap().push(commit.artifact);
            Ok(())
        }

        async fn commit_transcript(&self, commit: TranscriptCommit) -> AppResult<ArtifactRef> {
            let artifact = ArtifactRef {
                id: commit.artifact_id,
                stage: commit.stage,
                path: commit.path.to_string_lossy().into_owned(),
                content_hash: format!("transcript-{}", commit.transcript.revision),
                schema_version: SCHEMA_VERSION,
                producer_fingerprint: commit.producer_fingerprint,
            };
            self.committed.lock().unwrap().push(artifact.clone());
            self.transcripts
                .lock()
                .unwrap()
                .insert(artifact.path.clone(), commit.transcript);
            Ok(artifact)
        }

        async fn load_transcript(
            &self,
            artifact: &ArtifactRef,
        ) -> AppResult<videocaptionerr_domain::Transcript> {
            self.transcripts
                .lock()
                .unwrap()
                .get(&artifact.path)
                .cloned()
                .ok_or_else(|| ApplicationError::Invalid("fake transcript not found".into()))
        }

        async fn validate(&self, _artifact: &ArtifactRef) -> AppResult<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct LongPlans {
        values: Mutex<Vec<ChunkPlan>>,
    }

    #[async_trait]
    impl ChunkPlanStore for LongPlans {
        async fn commit(&self, commit: ChunkPlanCommit) -> AppResult<ArtifactRef> {
            self.values.lock().unwrap().push(commit.plan.clone());
            Ok(ArtifactRef {
                id: commit.artifact_id,
                stage: StageKind::Asr,
                path: commit.path.to_string_lossy().into_owned(),
                content_hash: "chunk-plan-hash".into(),
                schema_version: SCHEMA_VERSION,
                producer_fingerprint: commit.producer_fingerprint,
            })
        }
    }

    #[derive(Default)]
    struct LongCache {
        values: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl LongCache {
        fn corrupt_first(&self) {
            let key = self.values.lock().unwrap().keys().next().cloned().unwrap();
            self.values
                .lock()
                .unwrap()
                .insert(key, b"not-json".to_vec());
        }
    }

    #[async_trait]
    impl CacheRepository for LongCache {
        async fn gc(&self, _max_bytes: u64) -> AppResult<CacheGcResult> {
            Ok(CacheGcResult {
                before_bytes: 0,
                after_bytes: 0,
                deleted_entries: 0,
                skipped_leased: 0,
            })
        }

        async fn read(&self, key: &str) -> AppResult<Option<Vec<u8>>> {
            Ok(self.values.lock().unwrap().get(key).cloned())
        }

        async fn write(&self, key: &str, bytes: &[u8]) -> AppResult<()> {
            self.values
                .lock()
                .unwrap()
                .insert(key.to_owned(), bytes.to_vec());
            Ok(())
        }
    }

    #[derive(Default)]
    struct LongUnits {
        values: Mutex<Vec<WorkUnit>>,
    }

    impl LongUnits {
        fn snapshot(&self) -> Vec<WorkUnit> {
            self.values.lock().unwrap().clone()
        }

        fn complete(&self, id: &videocaptionerr_domain::WorkUnitId, artifact: ArtifactRef) {
            let mut values = self.values.lock().unwrap();
            let unit = values.iter_mut().find(|unit| unit.id() == id).unwrap();
            unit.complete(artifact).unwrap();
        }
    }

    #[async_trait]
    impl WorkUnitRepository for LongUnits {
        async fn load_work_unit(
            &self,
            id: &videocaptionerr_domain::WorkUnitId,
        ) -> AppResult<Option<WorkUnit>> {
            Ok(self
                .values
                .lock()
                .unwrap()
                .iter()
                .find(|unit| unit.id() == id)
                .cloned())
        }

        async fn find_work_unit(
            &self,
            job_id: &JobId,
            stage: StageKind,
            unit_kind: &str,
            unit_index: u32,
            input_hash: &str,
        ) -> AppResult<Option<WorkUnit>> {
            Ok(self
                .values
                .lock()
                .unwrap()
                .iter()
                .find(|unit| {
                    unit.job_id() == job_id
                        && unit.stage() == stage
                        && unit.unit_kind() == unit_kind
                        && unit.unit_index() == unit_index
                        && unit.input_hash() == input_hash
                })
                .cloned())
        }

        async fn save_work_unit(&self, unit: &WorkUnit) -> AppResult<()> {
            let mut values = self.values.lock().unwrap();
            if let Some(existing) = values
                .iter_mut()
                .find(|existing| existing.id() == unit.id())
            {
                *existing = unit.clone();
            } else {
                values.push(unit.clone());
            }
            Ok(())
        }

        async fn recover_expired(&self, _now_ms: u64) -> AppResult<u32> {
            Ok(0)
        }

        async fn count_retryable(
            &self,
            job_id: &JobId,
            from_stage: Option<StageKind>,
        ) -> AppResult<u32> {
            let count = self
                .values
                .lock()
                .unwrap()
                .iter()
                .filter(|unit| {
                    unit.job_id() == job_id
                        && matches!(
                            unit.status(),
                            WorkUnitStatus::Failed | WorkUnitStatus::Cancelled
                        )
                        && from_stage.is_none_or(|stage| unit.stage() as u8 >= stage as u8)
                })
                .count();
            Ok(count as u32)
        }

        async fn lease_next_ready(
            &self,
            job_id: &JobId,
            stage: StageKind,
            owner: &str,
            now_ms: u64,
            lease_ms: u64,
        ) -> AppResult<Option<WorkUnit>> {
            let mut values = self.values.lock().unwrap();
            let Some(index) = values.iter().position(|unit| {
                unit.job_id() == job_id
                    && unit.stage() == stage
                    && unit.status() == WorkUnitStatus::Pending
            }) else {
                return Ok(None);
            };
            let unit = &mut values[index];
            unit.lease_for(owner, now_ms, now_ms + lease_ms)
                .map_err(ApplicationError::Domain)?;
            Ok(Some(unit.clone()))
        }

        async fn retry_failed(
            &self,
            job_id: &JobId,
            from_stage: Option<StageKind>,
        ) -> AppResult<u32> {
            let mut values = self.values.lock().unwrap();
            let mut count = 0;
            for unit in values.iter_mut().filter(|unit| {
                unit.job_id() == job_id
                    && matches!(
                        unit.status(),
                        WorkUnitStatus::Failed | WorkUnitStatus::Cancelled
                    )
                    && from_stage.is_none_or(|stage| unit.stage() as u8 >= stage as u8)
            }) {
                unit.retry().map_err(ApplicationError::Domain)?;
                count += 1;
            }
            Ok(count)
        }
    }

    struct LongArtifacts {
        units: Arc<LongUnits>,
    }

    #[async_trait]
    impl ArtifactStore for LongArtifacts {
        async fn commit(&self, _commit: ArtifactCommit) -> AppResult<()> {
            Ok(())
        }

        async fn commit_transcript(&self, commit: TranscriptCommit) -> AppResult<ArtifactRef> {
            let artifact = ArtifactRef {
                id: commit.artifact_id,
                stage: commit.stage,
                path: commit.path.to_string_lossy().into_owned(),
                content_hash: format!("chunk-{}", commit.transcript.source_hash),
                schema_version: SCHEMA_VERSION,
                producer_fingerprint: commit.producer_fingerprint,
            };
            if let Some(unit_id) = commit.work_unit_id.as_ref() {
                self.units.complete(unit_id, artifact.clone());
            }
            Ok(artifact)
        }

        async fn load_transcript(&self, _artifact: &ArtifactRef) -> AppResult<Transcript> {
            Err(ApplicationError::Invalid(
                "long-audio fake does not load artifact transcripts".into(),
            ))
        }

        async fn validate(&self, _artifact: &ArtifactRef) -> AppResult<()> {
            Ok(())
        }
    }

    struct LongClock;

    impl Clock for LongClock {
        fn now_ms(&self) -> u64 {
            1_000_000
        }
    }

    struct LongSession {
        descriptor: AsrDescriptor,
        calls: Arc<Mutex<Vec<PathBuf>>>,
        fail_chunk: Option<u32>,
        failed: bool,
    }

    impl LongSession {
        fn new(fail_chunk: Option<u32>) -> Self {
            Self {
                descriptor: AsrDescriptor {
                    engine_id: "fake-long".into(),
                    adapter_version: "test".into(),
                    runtime_version: "test".into(),
                    fingerprint: "fake-long|test|test|fake|cpu".into(),
                    supports_word_timestamps: true,
                    supports_confidence: true,
                    cooperative_cancel: true,
                    max_audio_secs: Some(120),
                },
                calls: Arc::new(Mutex::new(Vec::new())),
                fail_chunk,
                failed: false,
            }
        }

        fn calls(&self) -> Vec<PathBuf> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl AsrSession for LongSession {
        fn descriptor(&self) -> &AsrDescriptor {
            &self.descriptor
        }

        async fn transcribe(
            &mut self,
            request: AsrTranscribeRequest,
            _events: &dyn EventPublisher,
        ) -> AppResult<NormalizedAsrResult> {
            let path = request.audio_path;
            self.calls.lock().unwrap().push(path.clone());
            let index = path
                .file_stem()
                .and_then(|value| value.to_str())
                .and_then(|value| value.strip_prefix("chunk-"))
                .and_then(|value| value.parse::<u32>().ok())
                .ok_or_else(|| ApplicationError::Invalid("missing fake chunk index".into()))?;
            if self.fail_chunk == Some(index) && !self.failed {
                self.failed = true;
                return Err(ApplicationError::Adapter(VcError::new(
                    ErrorCode::AsrFailed,
                    "injected long-audio chunk failure",
                )));
            }
            let words = match index {
                0 => vec![
                    Word {
                        text: "left".into(),
                        start_ms: 119_000,
                        end_ms: 119_500,
                        prob: 0.9,
                    },
                    Word {
                        text: "boundary".into(),
                        start_ms: 119_900,
                        end_ms: 120_100,
                        prob: 0.9,
                    },
                ],
                1 => vec![
                    Word {
                        text: "boundary".into(),
                        start_ms: 1_500,
                        end_ms: 1_700,
                        prob: 0.9,
                    },
                    Word {
                        text: "right".into(),
                        start_ms: 120_500,
                        end_ms: 121_000,
                        prob: 0.9,
                    },
                ],
                2 => vec![Word {
                    text: "tail".into(),
                    start_ms: 6_500,
                    end_ms: 7_000,
                    prob: 0.9,
                }],
                _ => Vec::new(),
            };
            Ok(NormalizedAsrResult {
                transcript: Transcript::new_asr("chunk", EngineFingerprint::unknown(), words),
            })
        }

        async fn close(self: Box<Self>) -> AppResult<()> {
            Ok(())
        }
    }

    fn long_use_case(
        media: Arc<LongMedia>,
        plans: Arc<LongPlans>,
        cache: Arc<LongCache>,
        units: Arc<LongUnits>,
    ) -> TranscribeJob {
        TranscribeJob::new(
            Arc::new(FakeJobs {
                saved: Mutex::new(Vec::new()),
            }),
            media,
            Arc::new(LongArtifacts {
                units: units.clone(),
            }),
            Arc::new(FakeSubtitles),
            Arc::new(FakeEvents),
            Arc::new(FakeIds),
        )
        .with_chunking(plans, cache, units, Arc::new(LongClock))
    }

    struct FakeSubtitles;

    #[async_trait]
    impl SubtitleGateway for FakeSubtitles {
        async fn export(
            &self,
            _transcript: &Transcript,
            request: SubtitleExportRequest,
        ) -> AppResult<ExportedSubtitle> {
            Ok(ExportedSubtitle {
                path: request.output_path,
                content_hash: "srt-hash".into(),
            })
        }
    }

    struct FakeSession {
        descriptor: AsrDescriptor,
        transcript: Option<Transcript>,
        fail: bool,
    }

    #[async_trait]
    impl AsrSession for FakeSession {
        fn descriptor(&self) -> &AsrDescriptor {
            &self.descriptor
        }

        async fn transcribe(
            &mut self,
            _request: AsrTranscribeRequest,
            _events: &dyn EventPublisher,
        ) -> AppResult<NormalizedAsrResult> {
            if self.fail {
                return Err(ApplicationError::Adapter(VcError::new(
                    ErrorCode::AsrFailed,
                    "fake ASR failure",
                )));
            }
            Ok(NormalizedAsrResult {
                transcript: self.transcript.take().unwrap(),
            })
        }

        async fn close(self: Box<Self>) -> AppResult<()> {
            Ok(())
        }
    }

    fn transcript() -> Transcript {
        Transcript::new_asr(
            "source",
            EngineFingerprint::unknown(),
            vec![
                Word {
                    text: "hello".into(),
                    start_ms: 0,
                    end_ms: 200,
                    prob: 0.9,
                },
                Word {
                    text: "world".into(),
                    start_ms: 220,
                    end_ms: 500,
                    prob: PROB_UNAVAILABLE,
                },
            ],
        )
    }

    fn make_use_case(fail_export_once: Option<Arc<AtomicBool>>) -> (TranscribeJob, Arc<FakeJobs>) {
        let jobs = Arc::new(FakeJobs {
            saved: Mutex::new(Vec::new()),
        });
        let artifacts = Arc::new(FakeArtifacts {
            committed: Mutex::new(Vec::new()),
            transcripts: Mutex::new(HashMap::new()),
            fail_export_once,
        });
        let use_case = TranscribeJob::new(
            jobs.clone(),
            Arc::new(FakeMedia),
            artifacts,
            Arc::new(FakeSubtitles),
            Arc::new(FakeEvents),
            Arc::new(FakeIds),
        );
        (use_case, jobs)
    }

    fn use_case() -> (TranscribeJob, Arc<FakeJobs>) {
        make_use_case(None)
    }

    fn use_case_with_export_failure() -> (TranscribeJob, Arc<FakeJobs>, Arc<AtomicBool>) {
        let fail_export_once = Arc::new(AtomicBool::new(true));
        let (use_case, jobs) = make_use_case(Some(fail_export_once.clone()));
        (use_case, jobs, fail_export_once)
    }

    fn command(dir: &std::path::Path) -> TranscribeJobCommand {
        TranscribeJobCommand {
            job_id: UlidStr::from(Ulid::new()),
            batch_id: None,
            profile_revision: UlidStr::from(Ulid::new()),
            input: dir.join("input.wav"),
            job_dir: dir.join("job"),
            language: Some("en".into()),
            export: SubtitleExportRequest {
                output_path: dir.join("output.srt"),
                format: SubtitleFormat::Srt,
                layout: SubtitleLayout::SourceOnly,
                fallback_to_source: true,
            },
            llm: None,
        }
    }

    fn session(transcript: Option<Transcript>, fail: bool) -> FakeSession {
        FakeSession {
            descriptor: AsrDescriptor {
                engine_id: "fake".into(),
                adapter_version: "test".into(),
                runtime_version: "test".into(),
                fingerprint: "fake|test|test|fake|cpu".into(),
                supports_word_timestamps: true,
                supports_confidence: true,
                cooperative_cancel: true,
                max_audio_secs: Some(3600),
            },
            transcript,
            fail,
        }
    }

    #[tokio::test]
    async fn fake_vertical_slice_completes_without_unloading_session() {
        let dir = tempdir().unwrap();
        let (use_case, jobs) = use_case();
        let mut asr = session(Some(transcript()), false);
        let result = use_case
            .execute(command(dir.path()), &mut asr)
            .await
            .unwrap();
        assert_eq!(result.job.status(), videocaptionerr_domain::JobStatus::Done);
        assert!(!result.transcript.cues.is_empty());
        assert_eq!(
            jobs.saved.lock().unwrap().last().unwrap().status(),
            videocaptionerr_domain::JobStatus::Done
        );
    }

    #[tokio::test]
    async fn retry_after_export_failure_reuses_completed_transcript_stages() {
        let dir = tempdir().unwrap();
        let (use_case, jobs, export_failure) = use_case_with_export_failure();
        let first_command = command(dir.path());
        let mut first_session = session(Some(transcript()), false);
        let error = use_case
            .execute(first_command, &mut first_session)
            .await
            .unwrap_err();
        assert_eq!(error.into_vc_error().code, ErrorCode::ExportFailed);
        assert!(!export_failure.load(Ordering::SeqCst));

        let failed_job = jobs.saved.lock().unwrap().last().unwrap().clone();
        assert_eq!(
            failed_job.status(),
            videocaptionerr_domain::JobStatus::Failed
        );
        assert_eq!(
            failed_job
                .stages()
                .iter()
                .find(|stage| stage.kind == StageKind::Split)
                .unwrap()
                .status,
            StageStatus::Done
        );
        assert_eq!(
            failed_job
                .stages()
                .iter()
                .find(|stage| stage.kind == StageKind::Export)
                .unwrap()
                .status,
            StageStatus::Failed
        );

        let mut retry_job = failed_job.clone();
        retry_job.prepare_retry(None).unwrap();
        jobs.save_job(&retry_job).await.unwrap();

        let mut retry_command = command(dir.path());
        retry_command.job_id = failed_job.id().clone();
        retry_command.profile_revision = failed_job.profile_revision().clone();
        let mut retry_session = session(None, true);
        let result = use_case
            .execute(retry_command, &mut retry_session)
            .await
            .unwrap();
        assert_eq!(result.job.status(), videocaptionerr_domain::JobStatus::Done);
        assert_eq!(
            result.transcript,
            videocaptionerr_domain::rule_split(
                &transcript(),
                &videocaptionerr_domain::RuleSplitConfig::default(),
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn asr_failure_marks_job_failed() {
        let dir = tempdir().unwrap();
        let (use_case, jobs) = use_case();
        let mut asr = session(None, true);
        let error = use_case
            .execute(command(dir.path()), &mut asr)
            .await
            .unwrap_err();
        assert_eq!(error.into_vc_error().code, ErrorCode::AsrFailed);
        assert_eq!(
            jobs.saved.lock().unwrap().last().unwrap().status(),
            videocaptionerr_domain::JobStatus::Failed
        );
    }

    #[tokio::test]
    async fn long_audio_uses_core_ownership_and_skips_cached_asr() {
        let dir = tempdir().unwrap();
        let media = Arc::new(LongMedia::default());
        let plans = Arc::new(LongPlans::default());
        let cache = Arc::new(LongCache::default());
        let units = Arc::new(LongUnits::default());
        let use_case = long_use_case(media.clone(), plans.clone(), cache.clone(), units.clone());
        let command = command(dir.path());
        let mut session = LongSession::new(None);

        let result = use_case
            .transcribe_asr(
                &command,
                &mut session,
                "media-hash",
                "pcm-hash",
                &command.job_dir.join("audio.wav"),
                250_000,
            )
            .await
            .unwrap();

        assert_eq!(
            result
                .words
                .iter()
                .map(|word| word.text.as_str())
                .collect::<Vec<_>>(),
            ["left", "boundary", "right", "tail"]
        );
        assert_eq!(*media.analyses.lock().unwrap(), 1);
        assert_eq!(plans.values.lock().unwrap().last().unwrap().chunks.len(), 3);
        let ranges = media.ranges.lock().unwrap().clone();
        assert_eq!(ranges.len(), 3);
        assert!(ranges.windows(2).all(|pair| pair[0].2 != pair[1].2));
        assert!(ranges.iter().all(|(start, end, _)| start < end));
        assert!(units
            .snapshot()
            .iter()
            .all(|unit| unit.status() == WorkUnitStatus::Done && unit.artifact().is_some()));

        let mut cached_session = LongSession::new(None);
        let cached = use_case
            .transcribe_asr(
                &command,
                &mut cached_session,
                "media-hash",
                "pcm-hash",
                &command.job_dir.join("audio.wav"),
                250_000,
            )
            .await
            .unwrap();
        assert_eq!(cached.words, result.words);
        assert!(cached_session.calls().is_empty());
        assert_eq!(media.ranges.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn failed_long_audio_chunk_is_retryable_without_rerunning_completed_chunks() {
        let dir = tempdir().unwrap();
        let media = Arc::new(LongMedia::default());
        let plans = Arc::new(LongPlans::default());
        let cache = Arc::new(LongCache::default());
        let units = Arc::new(LongUnits::default());
        let use_case = long_use_case(media, plans, cache.clone(), units.clone());
        let command = command(dir.path());
        let mut session = LongSession::new(Some(1));

        let error = use_case
            .transcribe_asr(
                &command,
                &mut session,
                "media-hash",
                "pcm-hash",
                &command.job_dir.join("audio.wav"),
                250_000,
            )
            .await
            .unwrap_err();
        assert_eq!(error.into_vc_error().code, ErrorCode::AsrFailed);
        let after_failure = units.snapshot();
        assert_eq!(after_failure.len(), 3);
        assert_eq!(
            after_failure
                .iter()
                .map(WorkUnit::status)
                .collect::<Vec<_>>(),
            [
                WorkUnitStatus::Done,
                WorkUnitStatus::Failed,
                WorkUnitStatus::Pending
            ]
        );
        assert_eq!(cache.values.lock().unwrap().len(), 1);

        assert_eq!(units.retry_failed(&command.job_id, None).await.unwrap(), 1);
        let result = use_case
            .transcribe_asr(
                &command,
                &mut session,
                "media-hash",
                "pcm-hash",
                &command.job_dir.join("audio.wav"),
                250_000,
            )
            .await
            .unwrap();
        let calls = session.calls();
        assert_eq!(calls.len(), 4);
        assert!(calls[0].ends_with("chunk-0000.wav"));
        assert!(calls[1].ends_with("chunk-0001.wav"));
        assert!(calls[2].ends_with("chunk-0001.wav"));
        assert!(calls[3].ends_with("chunk-0002.wav"));
        assert_eq!(
            result
                .words
                .iter()
                .map(|word| word.text.as_str())
                .collect::<Vec<_>>(),
            ["left", "boundary", "right", "tail"]
        );
        assert!(units
            .snapshot()
            .iter()
            .all(|unit| unit.status() == WorkUnitStatus::Done));
    }

    #[tokio::test]
    async fn corrupt_completed_chunk_cache_fails_explicitly() {
        let dir = tempdir().unwrap();
        let media = Arc::new(LongMedia::default());
        let plans = Arc::new(LongPlans::default());
        let cache = Arc::new(LongCache::default());
        let units = Arc::new(LongUnits::default());
        let use_case = long_use_case(media, plans, cache.clone(), units);
        let command = command(dir.path());
        let mut initial = LongSession::new(None);
        use_case
            .transcribe_asr(
                &command,
                &mut initial,
                "media-hash",
                "pcm-hash",
                &command.job_dir.join("audio.wav"),
                250_000,
            )
            .await
            .unwrap();
        cache.corrupt_first();

        let mut retry = LongSession::new(None);
        let error = use_case
            .transcribe_asr(
                &command,
                &mut retry,
                "media-hash",
                "pcm-hash",
                &command.job_dir.join("audio.wav"),
                250_000,
            )
            .await
            .unwrap_err();
        assert_eq!(error.into_vc_error().code, ErrorCode::CacheCorrupt);
        assert!(retry.calls().is_empty());
    }
}
