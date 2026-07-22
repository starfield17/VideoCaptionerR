//! Thin inbound facade for media processing.
//!
//! Batch creation, source preparation, output reservation, snapshot creation,
//! and persistence ordering live in Core's `ProcessMediaFiles` use case. This
//! module only validates the inbound DTO shape and resolves the already-loaded
//! immutable profile into Core port data.

use std::path::PathBuf;

use crate::dto::{ProcessOptions, ProcessView, TranscribeOptions};
use crate::jobs::job_summary;
use crate::runtime::ApplicationRuntime;
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::application_error::ApplicationError;
use videocaptionerr_core::ports::{ModelLocator, ProcessProfile, SubtitleFormat, SubtitleLayout};
use videocaptionerr_core::use_cases::{
    LlmProcessOptions, ProcessMediaFilesCommand, RunBatchResponse,
};

impl ApplicationRuntime {
    pub async fn transcribe(&self, options: TranscribeOptions) -> VcResult<RunBatchResponse> {
        let _lease = self.acquire_cli_processing_lock()?;
        self.execute_transcription(options, None).await
    }

    pub async fn process(&self, options: ProcessOptions) -> VcResult<RunBatchResponse> {
        let _lease = self.acquire_cli_processing_lock()?;
        self.process_unlocked(options).await
    }

    async fn process_unlocked(&self, options: ProcessOptions) -> VcResult<RunBatchResponse> {
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
        let _lease = self.acquire_gui_processing_lock()?;
        let result = match target_language.filter(|value| !value.trim().is_empty()) {
            Some(target_language) => {
                self.process_unlocked(ProcessOptions {
                    files,
                    language: None,
                    target_language,
                    format: "srt".into(),
                    profile: self.resolved_profile.name.clone(),
                })
                .await?
            }
            None => {
                self.execute_transcription(
                    TranscribeOptions {
                        files,
                        language: None,
                        format: "srt".into(),
                        profile: self.resolved_profile.name.clone(),
                        target_language: None,
                        layout: SubtitleLayout::SourceOnly,
                    },
                    None,
                )
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
        if options.profile != self.resolved_profile.name {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                "request profile does not match the Runtime profile",
            ));
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
        let profile = self.resolve_process_profile(llm)?;
        self.process_media_files
            .execute(ProcessMediaFilesCommand {
                files: options.files,
                language: options.language,
                target_language: options.target_language,
                format,
                layout: options.layout,
                profile,
            })
            .await
            .map_err(ApplicationError::into_vc_error)
    }

    fn resolve_process_profile(&self, llm: Option<LlmProcessOptions>) -> VcResult<ProcessProfile> {
        let locator = if let Some(path) = self.model_path.as_ref() {
            match (path.is_dir(), path.is_file(), self.engine.as_str()) {
                (true, _, _) => ModelLocator::directory(path.to_string_lossy()),
                (_, true, _) => ModelLocator::file(path.to_string_lossy()),
                (_, _, "fake") => ModelLocator::file(path.to_string_lossy()),
                _ => {
                    return Err(VcError::new(
                        ErrorCode::ModelNotFound,
                        format!("model path not found: {}", path.display()),
                    ));
                }
            }
        } else if let Some(model_id) = self.resolved_profile.model_id.as_deref() {
            let path = PathBuf::from(model_id);
            if path.is_dir() {
                ModelLocator::directory(path.to_string_lossy())
            } else if path.is_file() {
                ModelLocator::file(path.to_string_lossy())
            } else if matches!(self.engine.as_str(), "faster-whisper" | "mlx-whisper") {
                // Python engines accept a profile model id as a remote model
                // reference. The resolver performs the actual availability
                // check/download policy when the Processing Owner opens it.
                ModelLocator::hugging_face(model_id, "main", None)
            } else if self.engine == "fake" {
                ModelLocator::file("fake:default")
            } else {
                return Err(VcError::new(
                    ErrorCode::ModelNotFound,
                    format!("model path not found: {model_id}"),
                ));
            }
        } else if self.engine == "fake" {
            ModelLocator::file("fake:default")
        } else {
            return Err(VcError::new(
                ErrorCode::ModelNotFound,
                "no model selected; configure profile.model_id or pass --model",
            ));
        };
        if self.engine == "whisper-cpp" && !matches!(locator, ModelLocator::File { .. }) {
            return Err(VcError::new(
                ErrorCode::ModelNotFound,
                "whisper-cpp requires a model file",
            ));
        }
        let asr = videocaptionerr_core::ports::AsrRuntimeSpec {
            engine_family: self.engine.clone(),
            // An explicit CLI/Desktop model path is an effective override and
            // must be reflected in the immutable identity captured by the Job
            // snapshot. Profile model ids remain the identity when no override
            // was supplied.
            model_id: if self.model_path.is_some() {
                locator.display()
            } else {
                self.resolved_profile
                    .model_id
                    .clone()
                    .unwrap_or_else(|| locator.display())
            },
            verified_digest: self.model_digest.clone(),
            locator,
            device: self.resolved_profile.device.clone(),
            compute_type: self.resolved_profile.compute_type.clone(),
        };
        asr.validate().map_err(|message| {
            VcError::new(
                ErrorCode::InvalidConfig,
                format!("invalid ASR profile: {message}"),
            )
        })?;
        Ok(ProcessProfile {
            name: self.resolved_profile.name.clone(),
            asr,
            llm,
            cache_max_bytes: self.resolved_profile.cache_max_bytes,
        })
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
            .map(|failure| crate::dto::FailureView {
                job_id: failure.job_id.clone(),
                code: failure.error.code.as_str().into(),
                message: failure.error.message.clone(),
            })
            .collect(),
    }
}
