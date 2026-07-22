//! Creation and execution of a media Batch.
//!
//! This is intentionally the application boundary for path normalization,
//! source facts, output reservation, immutable snapshots, and persistence
//! ordering. Bootstrap supplies ports and translates inbound DTOs only.

use std::path::PathBuf;
use std::sync::Arc;

use chrono::TimeZone;
use videocaptionerr_domain::{Batch, BatchExecutionProfile, BatchId, Job, JobId, UlidStr};

use crate::application_error::{AppResult, ApplicationError};
use crate::execution_snapshot::{
    AsrExecutionSnapshot, AudioStreamSelection, CacheExecutionSnapshot, JobExecutionSnapshot,
    LlmExecutionSnapshot, OutputPlanSnapshot, JOB_EXECUTION_SNAPSHOT_SCHEMA_VERSION,
};
use crate::ports::{
    BatchCreationRepository, BatchCreationRequest, Clock, IdGenerator, JobWorkspace,
    MediaFileCatalog, OutputPlanRequest, OutputPlanner, ProcessProfile, SubtitleExportRequest,
    SubtitleFormat, SubtitleLayout,
};

use super::{LlmProcessOptions, RunBatch, RunBatchCommand, RunBatchResponse, TranscribeJobCommand};

#[derive(Debug, Clone)]
pub struct ProcessMediaFilesCommand {
    pub files: Vec<PathBuf>,
    pub language: Option<String>,
    pub target_language: Option<String>,
    pub format: SubtitleFormat,
    pub layout: SubtitleLayout,
    pub profile: ProcessProfile,
}

pub struct CreatedBatch {
    pub batch: Batch,
    pub jobs: Vec<TranscribeJobCommand>,
    pub asr_spec: crate::ports::AsrRuntimeSpec,
}

/// Application ports used while materializing a new media Batch. The
/// composition root supplies the repository and host-adapter implementations;
/// this type keeps the use-case constructor small and explicit.
pub struct CreateBatchDependencies {
    pub creation: Arc<dyn BatchCreationRepository>,
    pub ids: Arc<dyn IdGenerator>,
    pub clock: Arc<dyn Clock>,
    pub files: Arc<dyn MediaFileCatalog>,
    pub workspace: Arc<dyn JobWorkspace>,
    pub outputs: Arc<dyn OutputPlanner>,
}

pub struct CreateBatch {
    creation: Arc<dyn BatchCreationRepository>,
    ids: Arc<dyn IdGenerator>,
    clock: Arc<dyn Clock>,
    files: Arc<dyn MediaFileCatalog>,
    workspace: Arc<dyn JobWorkspace>,
    outputs: Arc<dyn OutputPlanner>,
}

impl CreateBatch {
    pub fn new(dependencies: CreateBatchDependencies) -> Self {
        Self {
            creation: dependencies.creation,
            ids: dependencies.ids,
            clock: dependencies.clock,
            files: dependencies.files,
            workspace: dependencies.workspace,
            outputs: dependencies.outputs,
        }
    }

    pub async fn execute(&self, command: ProcessMediaFilesCommand) -> AppResult<CreatedBatch> {
        if command.files.is_empty() {
            return Err(ApplicationError::Invalid("no input files".into()));
        }
        command
            .profile
            .asr
            .validate()
            .map_err(ApplicationError::Invalid)?;

        let batch_id: BatchId = self.ids.next_id();
        let profile_revision: UlidStr = self.ids.next_id();
        self.outputs.begin_batch()?;
        let batch_profile = BatchExecutionProfile {
            asr_engine: command.profile.asr.engine_family.clone(),
            asr_model: command.profile.asr.model_id.clone(),
            device: command.profile.asr.device.clone(),
            compute_type: command.profile.asr.compute_type.clone(),
        };

        let mut job_ids = Vec::with_capacity(command.files.len());
        let mut jobs = Vec::with_capacity(command.files.len());
        let mut snapshots = Vec::with_capacity(command.files.len());
        let created_at = timestamp(self.clock.now_ms())?;

        for file in command.files {
            let prepared = self.files.prepare(&file)?;
            let job_id: JobId = self.ids.next_id();
            let snapshot_id: UlidStr = self.ids.next_id();
            let job_dir = self
                .workspace
                .directory_for(&job_id, &prepared.canonical_path);
            let planned = self.outputs.plan(&OutputPlanRequest {
                source_path: prepared.canonical_path.clone(),
                target_language: command.target_language.clone(),
                layout: command.layout,
                format: command.format,
            })?;
            let asr = &command.profile.asr;
            let snapshot = JobExecutionSnapshot {
                snapshot_id: snapshot_id.clone(),
                schema_version: JOB_EXECUTION_SNAPSHOT_SCHEMA_VERSION,
                created_at: created_at.clone(),
                job_id: job_id.clone(),
                batch_id: batch_id.clone(),
                canonical_source_path: prepared.canonical_path.to_string_lossy().into_owned(),
                source_stat: prepared.source_stat,
                job_dir: job_dir.to_string_lossy().into_owned(),
                profile_revision: profile_revision.clone(),
                profile_name: command.profile.name.clone(),
                asr: AsrExecutionSnapshot {
                    engine: asr.engine_family.clone(),
                    model_locator: asr.locator.clone(),
                    model_id: Some(asr.model_id.clone()),
                    model_digest: asr.verified_digest.clone(),
                    device: asr.device.clone(),
                    compute_type: asr.compute_type.clone(),
                },
                audio_stream: AudioStreamSelection::Auto,
                source_language: command.language.clone(),
                target_language: command.target_language.clone(),
                output: OutputPlanSnapshot {
                    path: planned.path.to_string_lossy().into_owned(),
                    format: format_name(command.format).into(),
                    layout: command.layout.as_str().into(),
                    conflict_policy: planned.conflict_policy,
                    fallback_to_source: command.layout == SubtitleLayout::TranslationOnly,
                },
                cache: CacheExecutionSnapshot {
                    max_bytes: command.profile.cache_max_bytes,
                },
                llm: command.profile.llm.as_ref().map(llm_snapshot),
            };
            snapshots.push(snapshot);
            job_ids.push(job_id.clone());
            jobs.push(TranscribeJobCommand {
                job_id: job_id.clone(),
                batch_id: Some(batch_id.clone()),
                execution_snapshot_id: snapshot_id,
                profile_revision: profile_revision.clone(),
                input: prepared.canonical_path,
                job_dir,
                language: command.language.clone(),
                export: SubtitleExportRequest {
                    output_path: planned.path,
                    format: command.format,
                    layout: command.layout,
                    fallback_to_source: command.layout == SubtitleLayout::TranslationOnly,
                },
                llm: command.profile.llm.clone(),
            });
        }

        let batch = Batch::new(batch_id, job_ids, batch_profile)?;

        let job_entities = jobs
            .iter()
            .map(|command| {
                Job::new_with_snapshot(
                    command.job_id.clone(),
                    command.batch_id.clone(),
                    command.execution_snapshot_id.clone(),
                    command.profile_revision.clone(),
                    command.input.to_string_lossy(),
                )
            })
            .collect();
        let persisted = self
            .creation
            .create_batch_graph(BatchCreationRequest {
                batch,
                jobs: job_entities,
                snapshots,
            })
            .await?;

        Ok(CreatedBatch {
            batch: persisted.batch.value,
            jobs,
            asr_spec: command.profile.asr,
        })
    }
}

pub struct ProcessMediaFiles {
    create: CreateBatch,
    run_batch: Arc<RunBatch>,
}

impl ProcessMediaFiles {
    pub fn new(create: CreateBatch, run_batch: Arc<RunBatch>) -> Self {
        Self { create, run_batch }
    }

    pub async fn execute(&self, command: ProcessMediaFilesCommand) -> AppResult<RunBatchResponse> {
        let created = self.create.execute(command).await?;
        self.run_batch
            .execute(RunBatchCommand {
                batch: created.batch,
                jobs: created.jobs,
                asr_spec: created.asr_spec,
            })
            .await
    }
}

fn timestamp(now_ms: u64) -> AppResult<String> {
    let millis = i64::try_from(now_ms)
        .map_err(|_| ApplicationError::Invalid("clock value exceeds timestamp range".into()))?;
    chrono::Utc
        .timestamp_millis_opt(millis)
        .single()
        .map(|value| value.to_rfc3339())
        .ok_or_else(|| ApplicationError::Invalid("clock returned an invalid timestamp".into()))
}

fn format_name(format: SubtitleFormat) -> &'static str {
    match format {
        SubtitleFormat::Srt => "srt",
        SubtitleFormat::Vtt => "vtt",
        SubtitleFormat::Ass => "ass",
    }
}

fn llm_snapshot(options: &LlmProcessOptions) -> LlmExecutionSnapshot {
    LlmExecutionSnapshot {
        provider_profile_revision: options.provider_profile_revision.clone(),
        model: options.model.clone(),
        max_context_tokens: options.max_context_tokens,
        max_output_tokens: options.max_output_tokens,
        chars_per_token: options.chars_per_token,
        structured_output: options.structured_output,
        seed: options.seed,
        target_language: options.target_language.clone(),
        split_prompt: options.split_prompt.clone(),
        correct_prompt: options.correct_prompt.clone(),
        translate_prompt: options.translate_prompt.clone(),
    }
}
