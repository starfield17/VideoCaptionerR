use std::path::{Path, PathBuf};

use ulid::Ulid;
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::application_error::ApplicationError;
use videocaptionerr_core::execution_snapshot::{
    AsrExecutionSnapshot, AudioStreamSelection, JobExecutionSnapshot, LlmExecutionSnapshot,
    OutputPlanSnapshot, SourceStatSnapshot, JOB_EXECUTION_SNAPSHOT_SCHEMA_VERSION,
};
use videocaptionerr_core::ports::{
    ExpectedVersion, SubtitleExportRequest, SubtitleFormat, SubtitleLayout, Versioned,
};
use videocaptionerr_core::use_cases::{
    LlmProcessOptions, RunBatchCommand, RunBatchResponse, TranscribeJobCommand,
};
use videocaptionerr_domain::{Batch, BatchExecutionProfile, BatchId, Job, JobId, UlidStr};
use videocaptionerr_platform::subtitle_io::{ConflictPolicy, ExportFormat, OutputPlanner};

use crate::dto::{FailureView, ProcessOptions, ProcessView, TranscribeOptions};
use crate::jobs::job_summary;
use crate::runtime::ApplicationRuntime;

impl ApplicationRuntime {
    pub async fn transcribe(&self, options: TranscribeOptions) -> VcResult<RunBatchResponse> {
        self.execute_transcription(options, None).await
    }

    pub async fn process(&self, options: ProcessOptions) -> VcResult<RunBatchResponse> {
        if options.target_language.trim().is_empty() {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                "process requires --target-lang",
            ));
        }
        let defaults = self.llm_defaults.as_ref().ok_or_else(|| {
            VcError::new(
                ErrorCode::LlmProviderUnavailable,
                "no LLM provider profile is configured",
            )
        })?;
        self.execute_transcription(
            TranscribeOptions {
                files: options.files,
                language: options.language,
                format: options.format,
                profile: options.profile,
                target_language: Some(options.target_language.clone()),
                layout: SubtitleLayout::BilingualSourceFirst,
            },
            Some(defaults.with_target_language(options.target_language)),
        )
        .await
    }

    pub async fn process_files(
        &self,
        files: Vec<PathBuf>,
        target_language: Option<String>,
    ) -> VcResult<ProcessView> {
        let result = match target_language.filter(|value| !value.trim().is_empty()) {
            Some(target_language) => {
                self.process(ProcessOptions {
                    files,
                    language: None,
                    target_language,
                    format: "srt".into(),
                    profile: None,
                })
                .await?
            }
            None => {
                self.transcribe(TranscribeOptions {
                    files,
                    language: None,
                    format: "srt".into(),
                    profile: None,
                    target_language: None,
                    layout: SubtitleLayout::SourceOnly,
                })
                .await?
            }
        };
        Ok(process_view(result))
    }

    async fn execute_transcription(
        &self,
        options: TranscribeOptions,
        llm: Option<LlmProcessOptions>,
    ) -> VcResult<RunBatchResponse> {
        if options.files.is_empty() {
            return Err(VcError::new(ErrorCode::InvalidArgument, "no input files"));
        }
        let format = SubtitleFormat::parse(&options.format).ok_or_else(|| {
            VcError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "unsupported format '{}' (expected srt|vtt|ass)",
                    options.format
                ),
            )
        })?;

        let batch_id: BatchId = Ulid::new().into();
        let profile_revision: UlidStr = Ulid::new().into();
        let model = self
            .model_path
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_else(|| format!("{}:default", self.engine));
        let profile = BatchExecutionProfile {
            asr_engine: self.engine.clone(),
            asr_model: model.clone(),
            device: "cpu".into(),
            compute_type: "default".into(),
        };

        let mut planner = OutputPlanner::new(
            "{stem}.{target_lang?}.{layout}.{format}",
            ConflictPolicy::Rename,
        );
        let model_digest = self
            .model_path
            .as_deref()
            .filter(|path| path.is_file())
            .map(videocaptionerr_store::blake3_file)
            .transpose()?;
        let mut job_ids = Vec::with_capacity(options.files.len());
        let mut commands = Vec::with_capacity(options.files.len());
        for file in options.files {
            let input = canonical_input(&file)?;
            let source_stat = source_stat_snapshot(&input)?;
            let job_id: JobId = Ulid::new().into();
            let execution_snapshot_id: UlidStr = Ulid::new().into();
            let stem = input
                .file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or("media");
            let job_dir = self.paths.job_dir(
                job_id.as_str(),
                &videocaptionerr_platform::sanitize_stem(stem),
            );
            let planned = planner.plan(
                &input,
                options.target_language.as_deref(),
                options.layout.into_platform(),
                format.into_platform(),
            )?;
            let snapshot = JobExecutionSnapshot {
                snapshot_id: execution_snapshot_id.clone(),
                schema_version: JOB_EXECUTION_SNAPSHOT_SCHEMA_VERSION,
                created_at: chrono::Utc::now().to_rfc3339(),
                job_id: job_id.clone(),
                batch_id: batch_id.clone(),
                canonical_source_path: input.to_string_lossy().into_owned(),
                source_stat,
                job_dir: job_dir.to_string_lossy().into_owned(),
                profile_revision: profile_revision.clone(),
                asr: AsrExecutionSnapshot {
                    engine: self.engine.clone(),
                    model_locator: model.clone(),
                    model_id: Some(model.clone()),
                    model_digest: model_digest.clone(),
                    device: profile.device.clone(),
                    compute_type: profile.compute_type.clone(),
                },
                audio_stream: AudioStreamSelection::Auto,
                source_language: options.language.clone(),
                target_language: options.target_language.clone(),
                output: OutputPlanSnapshot {
                    path: planned.path.to_string_lossy().into_owned(),
                    format: subtitle_format_name(format).into(),
                    layout: subtitle_layout_name(options.layout).into(),
                    conflict_policy: ConflictPolicy::Rename.as_str().into(),
                    fallback_to_source: options.layout == SubtitleLayout::TranslationOnly,
                },
                llm: llm.as_ref().map(llm_snapshot),
            };
            self.snapshots
                .save_execution_snapshot(&snapshot)
                .await
                .map_err(ApplicationError::into_vc_error)?;
            job_ids.push(job_id.clone());
            commands.push(TranscribeJobCommand {
                job_id,
                batch_id: Some(batch_id.clone()),
                execution_snapshot_id,
                profile_revision: profile_revision.clone(),
                input,
                job_dir,
                language: options.language.clone(),
                export: SubtitleExportRequest {
                    output_path: planned.path,
                    format,
                    layout: options.layout,
                    fallback_to_source: options.layout == SubtitleLayout::TranslationOnly,
                },
                llm: llm.clone(),
            });
        }

        let batch = Batch::new(batch_id, job_ids, profile).map_err(VcError::from)?;
        for command in &commands {
            let mut job = Versioned::new(Job::new_with_snapshot(
                command.job_id.clone(),
                command.batch_id.clone(),
                command.execution_snapshot_id.clone(),
                command.profile_revision.clone(),
                command.input.to_string_lossy(),
            ));
            self.jobs
                .save_job(&mut job, ExpectedVersion::New)
                .await
                .map_err(ApplicationError::into_vc_error)?;
        }
        let mut persisted_batch = Versioned::new(batch.clone());
        self.batches
            .save_batch(&mut persisted_batch, ExpectedVersion::New)
            .await
            .map_err(ApplicationError::into_vc_error)?;
        self.run_batch
            .execute(RunBatchCommand {
                batch: persisted_batch.value,
                jobs: commands,
            })
            .await
            .map_err(ApplicationError::into_vc_error)
    }
}

fn process_view(result: RunBatchResponse) -> ProcessView {
    ProcessView {
        jobs: result
            .jobs
            .iter()
            .map(|job| job_summary(&job.job))
            .collect(),
        failures: result
            .failures
            .iter()
            .map(|failure| FailureView {
                job_id: failure.job_id.clone(),
                code: failure.error.code.as_str().into(),
                message: failure.error.message.clone(),
            })
            .collect(),
    }
}

fn canonical_input(path: &Path) -> VcResult<PathBuf> {
    std::fs::canonicalize(path).map_err(|error| {
        VcError::new(
            ErrorCode::InputNotFound,
            format!("input not found {}: {error}", path.display()),
        )
    })
}

fn source_stat_snapshot(path: &Path) -> VcResult<SourceStatSnapshot> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        VcError::new(
            ErrorCode::InputNotFound,
            format!("read input metadata {}: {error}", path.display()),
        )
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

fn subtitle_format_name(format: SubtitleFormat) -> &'static str {
    match format {
        SubtitleFormat::Srt => "srt",
        SubtitleFormat::Vtt => "vtt",
        SubtitleFormat::Ass => "ass",
    }
}

fn subtitle_layout_name(layout: SubtitleLayout) -> &'static str {
    match layout {
        SubtitleLayout::SourceOnly => "source_only",
        SubtitleLayout::TranslationOnly => "translation_only",
        SubtitleLayout::BilingualSourceFirst => "bilingual_source_first",
        SubtitleLayout::BilingualTranslationFirst => "bilingual_translation_first",
    }
}

trait BootstrapSubtitleFormat {
    fn into_platform(self) -> ExportFormat;
}

impl BootstrapSubtitleFormat for SubtitleFormat {
    fn into_platform(self) -> ExportFormat {
        match self {
            SubtitleFormat::Srt => ExportFormat::Srt,
            SubtitleFormat::Vtt => ExportFormat::Vtt,
            SubtitleFormat::Ass => ExportFormat::Ass,
        }
    }
}

trait BootstrapSubtitleLayout {
    fn into_platform(self) -> videocaptionerr_platform::subtitle_io::ExportLayout;
}

impl BootstrapSubtitleLayout for SubtitleLayout {
    fn into_platform(self) -> videocaptionerr_platform::subtitle_io::ExportLayout {
        match self {
            SubtitleLayout::SourceOnly => {
                videocaptionerr_platform::subtitle_io::ExportLayout::SourceOnly
            }
            SubtitleLayout::TranslationOnly => {
                videocaptionerr_platform::subtitle_io::ExportLayout::TranslationOnly
            }
            SubtitleLayout::BilingualSourceFirst => {
                videocaptionerr_platform::subtitle_io::ExportLayout::BilingualSourceFirst
            }
            SubtitleLayout::BilingualTranslationFirst => {
                videocaptionerr_platform::subtitle_io::ExportLayout::BilingualTranslationFirst
            }
        }
    }
}
