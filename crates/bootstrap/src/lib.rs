//! Composition root shared by the CLI and the future desktop shell.
//!
//! This crate is the only place that assembles concrete infrastructure
//! adapters. It exposes application-shaped operations to inbound adapters.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use ulid::Ulid;
use videocaptionerr_asr::{resolve_helper_binary, WorkerAsrRuntime};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::application_error::ApplicationError;
use videocaptionerr_core::chunking::ChunkPlan;
use videocaptionerr_core::ports::{
    ArtifactStore, AsrRuntime, BatchRepository, CacheRepository, CapabilityProbeRecord,
    CapabilityProbeStore, ChunkPlanStore, Clock, EventPublisher, IdGenerator, JobRepository,
    LlmStage, MediaGateway, PromptSnapshot, StructuredOutput, SubtitleExportRequest,
    SubtitleFormat, SubtitleGateway, SubtitleLayout, WorkUnitRepository,
};
use videocaptionerr_core::use_cases::{
    CacheGc, EditTranscriptCommand, EditTranscriptResponse, LlmPipeline, LlmProcessOptions,
    PersistChunkPlan, RetryFailedWorkUnits, RetryFailedWorkUnitsCommand,
    RetryFailedWorkUnitsResponse, RunBatch, RunBatchCommand, RunBatchResponse, TranscribeJob,
    TranscribeJobCommand, TranscriptEditor, WorkUnitScheduler,
};
use videocaptionerr_domain::{
    ArtifactRef, Batch, BatchExecutionProfile, BatchId, DomainEvent, Job, JobId, JobStatus,
    LlmTextField, StageStatus, UlidStr,
};
use videocaptionerr_llm::application::ProviderLlmGateway;
use videocaptionerr_llm::circuit::{CircuitBreaker, CircuitLlmProvider};
use videocaptionerr_llm::openai::{OpenAiConfig, OpenAiProvider};
use videocaptionerr_llm::probe::{CapabilityProbe, ProbeConfig, ProbeResult};
use videocaptionerr_llm::prompt::{PromptBundle, PromptStage};
use videocaptionerr_llm::provider::{
    LlmProvider, ProviderCapabilities, StructuredMode, CAPABILITY_PROBE_VERSION,
};
use videocaptionerr_llm::templates::ProviderTemplate;
use videocaptionerr_platform::subtitle_io::{ConflictPolicy, ExportFormat, OutputPlanner};
use videocaptionerr_platform::{
    AppConfig, AppPaths, FfmpegMediaGateway, FileLlmRequestRecorder, FileSubtitleGateway,
    InstanceLock, LlmCapabilityOverride, LlmProviderConfig, LockOwner,
};
use videocaptionerr_store::{CacheStore, SqliteArtifactStore, StoreHandle};

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub home: Option<PathBuf>,
    pub engine: String,
    pub model_path: Option<PathBuf>,
    pub helper_path: Option<PathBuf>,
    pub prompt_dir: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct TranscribeOptions {
    pub files: Vec<PathBuf>,
    pub language: Option<String>,
    pub format: String,
    pub profile: Option<String>,
    pub target_language: Option<String>,
    pub layout: SubtitleLayout,
}

#[derive(Debug, Clone)]
pub struct ProcessOptions {
    pub files: Vec<PathBuf>,
    pub language: Option<String>,
    pub target_language: String,
    pub format: String,
    pub profile: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DoctorReport {
    pub version: &'static str,
    pub paths: AppPaths,
    pub ffmpeg: Option<PathBuf>,
    pub ffprobe: Option<PathBuf>,
    pub helper: PathBuf,
}

/// Stable application DTOs used by inbound adapters. Concrete platform and
/// provider types do not cross the desktop boundary.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobSummary {
    pub id: String,
    pub source_path: String,
    pub status: String,
    pub stages: Vec<StageSummary>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StageSummary {
    pub kind: String,
    pub status: String,
    pub artifact_path: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DoctorView {
    pub version: String,
    pub home: String,
    pub database: String,
    pub ffmpeg: Option<String>,
    pub ffprobe: Option<String>,
    pub helper: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FailureView {
    pub job_id: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessView {
    pub jobs: Vec<JobSummary>,
    pub failures: Vec<FailureView>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptEditView {
    pub transcript: videocaptionerr_contracts::Transcript,
    pub stage: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CapabilityProbeView {
    pub provider_profile_id: String,
    pub profile_revision: u64,
    pub model: String,
    pub probe_hash: String,
    pub capabilities: CapabilityView,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CapabilityView {
    pub structured_mode: String,
    pub returns_usage: bool,
    pub seed: bool,
    pub supports_model_list: bool,
    pub max_context_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
}

/// Opaque RAII lease shared by CLI and desktop inbound adapters.
pub struct ProcessingLease {
    _inner: InstanceLock,
}

pub struct ApplicationRuntime {
    paths: AppPaths,
    engine: String,
    model_path: Option<PathBuf>,
    helper_path: PathBuf,
    jobs: Arc<dyn JobRepository>,
    run_batch: Arc<RunBatch>,
    retry_failed: Arc<RetryFailedWorkUnits>,
    cache_gc: Arc<CacheGc>,
    scheduler: Arc<WorkUnitScheduler>,
    chunk_plans: Arc<PersistChunkPlan>,
    capability_probes: Arc<dyn CapabilityProbeStore>,
    transcript_editor: Arc<TranscriptEditor>,
    llm_defaults: Option<LlmProcessDefaults>,
}

#[derive(Debug, Clone)]
struct LlmProcessDefaults {
    model: String,
    provider_profile_revision: String,
    split_prompt: PromptSnapshot,
    correct_prompt: PromptSnapshot,
    translate_prompt: PromptSnapshot,
    max_context_tokens: Option<u32>,
    max_output_tokens: Option<u32>,
    chars_per_token: f64,
    structured_output: StructuredOutput,
    seed: Option<i64>,
}

impl LlmProcessDefaults {
    fn with_target_language(&self, target_language: String) -> LlmProcessOptions {
        LlmProcessOptions {
            model: self.model.clone(),
            provider_profile_revision: self.provider_profile_revision.clone(),
            split_prompt: self.split_prompt.clone(),
            correct_prompt: self.correct_prompt.clone(),
            translate_prompt: self.translate_prompt.clone(),
            max_context_tokens: self.max_context_tokens,
            max_output_tokens: self.max_output_tokens,
            chars_per_token: self.chars_per_token,
            structured_output: self.structured_output,
            seed: self.seed,
            target_language,
        }
    }
}

impl ApplicationRuntime {
    pub fn open(config: RuntimeConfig) -> VcResult<Self> {
        let paths = match config.home {
            Some(home) => AppPaths::from_home(home),
            None => AppPaths::resolve()?,
        };
        paths.ensure_layout()?;
        let app_config = AppConfig::load(&paths.config_file)?;
        let prompt_dir = config.prompt_dir.clone().unwrap_or_else(default_prompt_dir);

        if config.engine != "fake" {
            let model_path = config.model_path.as_ref().ok_or_else(|| {
                VcError::new(
                    ErrorCode::ModelNotFound,
                    "no model selected; pass --model explicitly",
                )
            })?;
            if !model_path.is_file() {
                return Err(VcError::new(
                    ErrorCode::ModelNotFound,
                    format!("model file not found: {}", model_path.display()),
                ));
            }
        }

        let helper_path = config.helper_path.unwrap_or_else(resolve_helper_binary);
        let store = StoreHandle::open(&paths.db_path)?;
        let jobs: Arc<dyn JobRepository> = Arc::new(store.clone());
        let batches: Arc<dyn BatchRepository> = Arc::new(store.clone());
        let work_units: Arc<dyn WorkUnitRepository> = Arc::new(store.clone());
        let capability_probes: Arc<dyn CapabilityProbeStore> = Arc::new(store.clone());
        let cached_capabilities = load_cached_capabilities(&store, &app_config)?;
        let artifact_adapter = Arc::new(SqliteArtifactStore::new(store));
        let artifacts: Arc<dyn ArtifactStore> = artifact_adapter.clone();
        let chunk_plan_store: Arc<dyn ChunkPlanStore> = artifact_adapter;
        let cache_store = CacheStore::new(&paths.cache_dir)?;
        let cache: Arc<dyn CacheRepository> = Arc::new(cache_store);
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let media: Arc<dyn MediaGateway> = Arc::new(FfmpegMediaGateway::default());
        let subtitles: Arc<dyn SubtitleGateway> = Arc::new(FileSubtitleGateway);
        let events: Arc<dyn EventPublisher> = Arc::new(NoopEventPublisher);
        let ids: Arc<dyn IdGenerator> = Arc::new(UlidGenerator);
        let chunk_plans = Arc::new(PersistChunkPlan::new(chunk_plan_store.clone(), ids.clone()));
        let transcript_editor = Arc::new(TranscriptEditor::new(
            jobs.clone(),
            artifacts.clone(),
            ids.clone(),
        ));
        let (llm_pipeline, llm_defaults) = build_llm_pipeline(
            &app_config,
            &prompt_dir,
            &paths.logs_dir,
            ids.clone(),
            cached_capabilities,
        )?;
        let asr: Arc<dyn AsrRuntime> = Arc::new(WorkerAsrRuntime::new(
            helper_path.clone(),
            config.engine.clone(),
            config.model_path.clone(),
        ));
        let mut transcribe_service = TranscribeJob::new(
            jobs.clone(),
            media,
            artifacts,
            subtitles,
            events.clone(),
            ids,
        );
        if let Some(pipeline) = llm_pipeline {
            transcribe_service = transcribe_service.with_llm_pipeline(pipeline);
        }
        transcribe_service = transcribe_service.with_chunking(
            chunk_plan_store,
            cache.clone(),
            work_units.clone(),
            clock.clone(),
        );
        let transcribe = Arc::new(transcribe_service);
        let run_batch = Arc::new(RunBatch::new(
            batches,
            jobs.clone(),
            asr,
            transcribe,
            events,
        ));
        let retry_failed = Arc::new(RetryFailedWorkUnits::new(jobs.clone(), work_units.clone()));
        let cache_gc = Arc::new(CacheGc::new(cache));
        let scheduler = Arc::new(WorkUnitScheduler::new(work_units, clock));

        Ok(Self {
            paths,
            engine: config.engine,
            model_path: config.model_path,
            helper_path,
            jobs,
            run_batch,
            retry_failed,
            cache_gc,
            scheduler,
            chunk_plans,
            capability_probes,
            transcript_editor,
            llm_defaults,
        })
    }

    pub fn paths(&self) -> &AppPaths {
        &self.paths
    }

    fn acquire_processing_lock(&self, owner: LockOwner) -> VcResult<ProcessingLease> {
        InstanceLock::try_acquire(&self.paths.instance_lock_path(), owner)
            .map(|inner| ProcessingLease { _inner: inner })
    }

    pub fn acquire_cli_processing_lock(&self) -> VcResult<ProcessingLease> {
        self.acquire_processing_lock(LockOwner::Cli)
    }

    pub fn acquire_gui_processing_lock(&self) -> VcResult<ProcessingLease> {
        self.acquire_processing_lock(LockOwner::Gui)
    }

    pub fn doctor(&self) -> DoctorReport {
        DoctorReport {
            version: env!("CARGO_PKG_VERSION"),
            paths: self.paths.clone(),
            ffmpeg: find_on_path("ffmpeg"),
            ffprobe: find_on_path("ffprobe"),
            helper: self.helper_path.clone(),
        }
    }

    pub fn doctor_view(&self) -> DoctorView {
        let report = self.doctor();
        DoctorView {
            version: report.version.into(),
            home: report.paths.home.display().to_string(),
            database: report.paths.db_path.display().to_string(),
            ffmpeg: report.ffmpeg.map(|path| path.display().to_string()),
            ffprobe: report.ffprobe.map(|path| path.display().to_string()),
            helper: report.helper.display().to_string(),
        }
    }

    pub async fn list_jobs(&self) -> Result<Vec<videocaptionerr_domain::Job>, VcError> {
        self.jobs
            .list_jobs()
            .await
            .map_err(ApplicationError::into_vc_error)
    }

    pub async fn list_job_summaries(&self) -> VcResult<Vec<JobSummary>> {
        self.list_jobs()
            .await
            .map(|jobs| jobs.iter().map(job_summary).collect())
    }

    pub async fn remove_job(&self, id: &str) -> VcResult<()> {
        let job_id: JobId = id.parse().map_err(|error| {
            VcError::new(
                ErrorCode::InvalidArgument,
                format!("invalid Job id: {error}"),
            )
        })?;
        self.jobs
            .delete_job(&job_id)
            .await
            .map_err(ApplicationError::into_vc_error)
    }

    pub async fn retry_job(
        &self,
        id: &str,
        from_stage: Option<&str>,
        dry_run: bool,
    ) -> VcResult<RetryFailedWorkUnitsResponse> {
        let job_id: JobId = id.parse().map_err(|error| {
            VcError::new(
                ErrorCode::InvalidArgument,
                format!("invalid Job id: {error}"),
            )
        })?;
        let stage = from_stage
            .map(|value| {
                videocaptionerr_domain::StageKind::parse(value).ok_or_else(|| {
                    VcError::new(
                        ErrorCode::InvalidArgument,
                        format!("unknown stage '{value}'"),
                    )
                })
            })
            .transpose()?;
        self.retry_failed
            .execute(RetryFailedWorkUnitsCommand {
                job_id,
                from_stage: stage,
                dry_run,
            })
            .await
            .map_err(ApplicationError::into_vc_error)
    }

    pub async fn gc_cache(&self, max_bytes: u64) -> VcResult<videocaptionerr_core::CacheGcResult> {
        self.cache_gc
            .execute(max_bytes)
            .await
            .map_err(ApplicationError::into_vc_error)
    }

    pub async fn recover_expired_work_units(&self) -> VcResult<u32> {
        self.scheduler
            .recover_expired()
            .await
            .map_err(ApplicationError::into_vc_error)
    }

    pub async fn persist_chunk_plan(
        &self,
        job_id: &str,
        path: PathBuf,
        plan: ChunkPlan,
    ) -> VcResult<ArtifactRef> {
        let job_id: JobId = job_id.parse().map_err(|error| {
            VcError::new(
                ErrorCode::InvalidArgument,
                format!("invalid Job id: {error}"),
            )
        })?;
        self.chunk_plans
            .execute(job_id, path, plan)
            .await
            .map_err(ApplicationError::into_vc_error)
    }

    /// Probe a provider only when an inbound adapter explicitly requests it.
    /// Cached results are used unless `force` is true; no API key enters the
    /// cache identity or the serialized result.
    pub async fn probe_llm_capabilities(
        &self,
        provider_id: Option<&str>,
        force: bool,
    ) -> VcResult<ProbeResult> {
        let config = AppConfig::load(&self.paths.config_file)?;
        let provider_id = provider_id
            .or(config.llm.default_provider.as_deref())
            .ok_or_else(|| {
                VcError::new(
                    ErrorCode::LlmProviderUnavailable,
                    "no LLM provider profile is configured",
                )
            })?;
        let provider_config = config.llm.providers.get(provider_id).ok_or_else(|| {
            VcError::new(
                ErrorCode::InvalidConfig,
                format!("LLM provider profile '{provider_id}' is missing"),
            )
        })?;
        let probe_config = probe_config(provider_id, provider_config);
        let probe = CapabilityProbe::new(probe_config.clone());
        if !force {
            if let Some(result) = self
                .capability_probes
                .load(
                    &probe_config.provider_profile_id,
                    &probe_config.model,
                    &probe.cache_key(),
                )
                .await
                .map_err(ApplicationError::into_vc_error)?
            {
                return decode_probe_result(
                    &result,
                    &probe_config,
                    provider_config.capability_override.as_ref(),
                );
            }
        }

        let mut result = probe.run().await?;
        result.capabilities = apply_capability_override(
            result.capabilities,
            provider_config.capability_override.as_ref(),
        );
        let result_json = serde_json::to_string(&result).map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode LLM capability probe: {error}"),
            )
        })?;
        self.capability_probes
            .save(CapabilityProbeRecord {
                id: Ulid::new().to_string(),
                provider_profile_id: probe_config.provider_profile_id,
                model: probe_config.model,
                probe_hash: probe.cache_key(),
                result_json,
                created_at: chrono::Utc::now().to_rfc3339(),
                expires_at: None,
            })
            .await
            .map_err(ApplicationError::into_vc_error)?;
        Ok(result)
    }

    pub async fn probe_llm_capabilities_view(&self, force: bool) -> VcResult<CapabilityProbeView> {
        let result = self.probe_llm_capabilities(None, force).await?;
        Ok(CapabilityProbeView {
            provider_profile_id: result.provider_profile_id,
            profile_revision: result.profile_revision,
            model: result.model,
            probe_hash: result.probe_hash,
            capabilities: CapabilityView {
                structured_mode: format!("{:?}", result.capabilities.effective_structured_mode())
                    .to_ascii_lowercase(),
                returns_usage: result.capabilities.returns_usage,
                seed: result.capabilities.seed,
                supports_model_list: result.capabilities.supports_model_list,
                max_context_tokens: result.capabilities.max_context_tokens,
                max_output_tokens: result.capabilities.max_output_tokens,
            },
            warnings: result.warnings,
        })
    }

    pub async fn load_transcript(
        &self,
        job_id: &str,
    ) -> VcResult<videocaptionerr_domain::Transcript> {
        let job_id: JobId = job_id.parse().map_err(|error| {
            VcError::new(
                ErrorCode::InvalidArgument,
                format!("invalid Job id: {error}"),
            )
        })?;
        self.transcript_editor
            .load(&job_id)
            .await
            .map_err(ApplicationError::into_vc_error)
    }

    pub async fn edit_transcript(
        &self,
        job_id: &str,
        cue_id: u32,
        expected_revision: u64,
        field: &str,
        value: String,
    ) -> VcResult<EditTranscriptResponse> {
        let job_id: JobId = job_id.parse().map_err(|error| {
            VcError::new(
                ErrorCode::InvalidArgument,
                format!("invalid Job id: {error}"),
            )
        })?;
        let field = match field.trim().to_ascii_lowercase().as_str() {
            "source" | "text" => LlmTextField::Source,
            "translation" => LlmTextField::Translation,
            _ => {
                return Err(VcError::new(
                    ErrorCode::InvalidArgument,
                    "field must be source or translation",
                ))
            }
        };
        self.transcript_editor
            .edit(EditTranscriptCommand {
                job_id,
                cue_id,
                expected_transcript_revision: expected_revision,
                field,
                value,
            })
            .await
            .map_err(ApplicationError::into_vc_error)
    }

    pub async fn edit_transcript_view(
        &self,
        job_id: &str,
        cue_id: u32,
        expected_revision: u64,
        field: &str,
        value: String,
    ) -> VcResult<TranscriptEditView> {
        let result = self
            .edit_transcript(job_id, cue_id, expected_revision, field, value)
            .await?;
        Ok(TranscriptEditView {
            transcript: result.transcript,
            stage: result.stage.as_str().into(),
        })
    }

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
            asr_model: model,
            device: "cpu".into(),
            compute_type: "default".into(),
        };

        let mut planner = OutputPlanner::new(
            "{stem}.{target_lang?}.{layout}.{format}",
            ConflictPolicy::Rename,
        );
        let mut job_ids = Vec::with_capacity(options.files.len());
        let mut commands = Vec::with_capacity(options.files.len());
        for file in options.files {
            let input = canonical_input(&file)?;
            let job_id: JobId = Ulid::new().into();
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
            job_ids.push(job_id.clone());
            commands.push(TranscribeJobCommand {
                job_id,
                batch_id: Some(batch_id.clone()),
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
        self.run_batch
            .execute(RunBatchCommand {
                batch,
                jobs: commands,
            })
            .await
            .map_err(ApplicationError::into_vc_error)
    }
}

fn job_summary(job: &Job) -> JobSummary {
    JobSummary {
        id: job.id().to_string(),
        source_path: job.source_path().into(),
        status: job_status(job.status()),
        stages: job
            .stages()
            .iter()
            .map(|stage| StageSummary {
                kind: stage.kind.as_str().into(),
                status: stage_status(stage.status),
                artifact_path: stage
                    .artifact
                    .as_ref()
                    .map(|artifact| artifact.path.clone()),
            })
            .collect(),
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

fn job_status(status: JobStatus) -> String {
    match status {
        JobStatus::Pending => "pending",
        JobStatus::Running => "running",
        JobStatus::Done => "done",
        JobStatus::DoneDegraded => "done_degraded",
        JobStatus::Failed => "failed",
        JobStatus::Cancelled => "cancelled",
    }
    .into()
}

fn stage_status(status: StageStatus) -> String {
    match status {
        StageStatus::Pending => "pending",
        StageStatus::WaitingResource => "waiting_resource",
        StageStatus::Running => "running",
        StageStatus::Retrying => "retrying",
        StageStatus::Done => "done",
        StageStatus::DoneDegraded => "done_degraded",
        StageStatus::Failed => "failed",
        StageStatus::Skipped => "skipped",
        StageStatus::Cancelled => "cancelled",
        StageStatus::WaitingProvider => "waiting_provider",
    }
    .into()
}

struct UlidGenerator;

fn default_prompt_dir() -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../prompts"))
}

fn build_llm_pipeline(
    config: &AppConfig,
    prompt_dir: &Path,
    logs_dir: &Path,
    ids: Arc<dyn IdGenerator>,
    cached_capabilities: Option<ProviderCapabilities>,
) -> VcResult<(Option<Arc<LlmPipeline>>, Option<LlmProcessDefaults>)> {
    let Some(provider_id) = config.llm.default_provider.as_deref() else {
        return Ok((None, None));
    };
    let provider_config = config.llm.providers.get(provider_id).ok_or_else(|| {
        VcError::new(
            ErrorCode::InvalidConfig,
            format!("default LLM provider profile '{provider_id}' is missing"),
        )
    })?;
    let template = provider_config
        .template
        .as_deref()
        .and_then(ProviderTemplate::parse)
        .unwrap_or(ProviderTemplate::Generic);
    let mut openai_config = OpenAiConfig::new(
        provider_id,
        &provider_config.base_url,
        &provider_config.api_key,
        &provider_config.model,
    );
    let capabilities = cached_capabilities.unwrap_or_else(|| template.default_capabilities());
    openai_config.capabilities =
        apply_capability_override(capabilities, provider_config.capability_override.as_ref());
    let provider = Arc::new(OpenAiProvider::new(openai_config)?);
    let provider: Arc<dyn LlmProvider> = Arc::new(CircuitLlmProvider::new(
        provider,
        Arc::new(CircuitBreaker::new(provider_id)),
    ));
    let capabilities = provider.capabilities().clone();
    let gateway = Arc::new(ProviderLlmGateway::new(provider));
    let recorder = Arc::new(FileLlmRequestRecorder::new(
        logs_dir.join("llm-requests.ndjson"),
    ));
    let pipeline = Arc::new(LlmPipeline::new(gateway, recorder, ids));

    let split_prompt = prompt_snapshot(PromptBundle::load(prompt_dir, PromptStage::Split)?);
    let correct_prompt = prompt_snapshot(PromptBundle::load(prompt_dir, PromptStage::Correct)?);
    let translate_prompt = prompt_snapshot(PromptBundle::load(prompt_dir, PromptStage::Translate)?);
    let structured_output = match capabilities.effective_structured_mode() {
        StructuredMode::JsonSchema => StructuredOutput::JsonSchema,
        StructuredMode::JsonObject => StructuredOutput::JsonObject,
        StructuredMode::PromptOnly => StructuredOutput::PromptOnly,
    };
    let defaults = LlmProcessDefaults {
        model: provider_config.model.clone(),
        provider_profile_revision: format!("{provider_id}:{}", provider_config.profile_revision),
        split_prompt,
        correct_prompt,
        translate_prompt,
        max_context_tokens: capabilities.max_context_tokens,
        max_output_tokens: capabilities.max_output_tokens,
        chars_per_token: videocaptionerr_core::DEFAULT_CHARS_PER_TOKEN,
        structured_output,
        seed: None,
    };
    Ok((Some(pipeline), Some(defaults)))
}

fn probe_config(provider_id: &str, provider: &LlmProviderConfig) -> ProbeConfig {
    let mut config = ProbeConfig::new(
        provider_id,
        provider.profile_revision,
        &provider.base_url,
        &provider.api_key,
        &provider.model,
    );
    config.manual_override = Some(apply_capability_override(
        ProviderCapabilities::conservative_default(),
        provider.capability_override.as_ref(),
    ));
    if provider.capability_override.is_none() {
        config.manual_override = None;
    }
    config
}

fn load_cached_capabilities(
    store: &StoreHandle,
    config: &AppConfig,
) -> VcResult<Option<ProviderCapabilities>> {
    let Some(provider_id) = config.llm.default_provider.as_deref() else {
        return Ok(None);
    };
    let provider = config.llm.providers.get(provider_id).ok_or_else(|| {
        VcError::new(
            ErrorCode::InvalidConfig,
            format!("default LLM provider profile '{provider_id}' is missing"),
        )
    })?;
    let probe_config = probe_config(provider_id, provider);
    let probe = CapabilityProbe::new(probe_config.clone());
    let Some(result_json) = store.load_capability_probe_sync(
        &probe_config.provider_profile_id,
        &probe_config.model,
        &probe.cache_key(),
    )?
    else {
        return Ok(None);
    };
    let result = decode_probe_result(
        &result_json,
        &probe_config,
        provider.capability_override.as_ref(),
    )?;
    Ok(Some(result.capabilities))
}

fn decode_probe_result(
    result_json: &str,
    expected: &ProbeConfig,
    capability_override: Option<&LlmCapabilityOverride>,
) -> VcResult<ProbeResult> {
    let mut result: ProbeResult = serde_json::from_str(result_json).map_err(|error| {
        VcError::new(
            ErrorCode::CacheCorrupt,
            format!("decode cached LLM capability probe: {error}"),
        )
    })?;
    let expected_hash = CapabilityProbe::new(expected.clone()).cache_key();
    if result.probe_version != CAPABILITY_PROBE_VERSION
        || result.provider_profile_id != expected.provider_profile_id
        || result.profile_revision != expected.profile_revision
        || result.base_url.trim_end_matches('/') != expected.base_url.trim_end_matches('/')
        || result.model != expected.model
        || result.probe_hash != expected_hash
    {
        return Err(VcError::new(
            ErrorCode::CacheCorrupt,
            "cached LLM capability probe does not match its lookup identity",
        ));
    }
    result.capabilities = apply_capability_override(result.capabilities, capability_override);
    Ok(result)
}

fn apply_capability_override(
    mut capabilities: ProviderCapabilities,
    override_config: Option<&LlmCapabilityOverride>,
) -> ProviderCapabilities {
    let Some(override_config) = override_config else {
        return capabilities;
    };
    if let Some(mode) = override_config
        .structured_mode
        .as_deref()
        .and_then(parse_structured_mode)
    {
        capabilities.structured_mode = mode;
        capabilities.json_schema = mode == StructuredMode::JsonSchema;
        capabilities.json_mode = matches!(
            mode,
            StructuredMode::JsonSchema | StructuredMode::JsonObject
        );
    }
    if let Some(value) = override_config.returns_usage {
        capabilities.returns_usage = value;
    }
    if let Some(value) = override_config.supports_seed {
        capabilities.seed = value;
    }
    if let Some(value) = override_config.supports_model_list {
        capabilities.supports_model_list = value;
    }
    if override_config.max_context_tokens.is_some() {
        capabilities.max_context_tokens = override_config.max_context_tokens;
    }
    if override_config.max_output_tokens.is_some() {
        capabilities.max_output_tokens = override_config.max_output_tokens;
    }
    capabilities.manual_override = true;
    capabilities
}

fn parse_structured_mode(value: &str) -> Option<StructuredMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "json_schema" | "json-schema" => Some(StructuredMode::JsonSchema),
        "json_object" | "json-object" => Some(StructuredMode::JsonObject),
        "prompt_only" | "prompt-only" => Some(StructuredMode::PromptOnly),
        _ => None,
    }
}

fn prompt_snapshot(bundle: PromptBundle) -> PromptSnapshot {
    let stage = match bundle.stage {
        PromptStage::Split => LlmStage::Split,
        PromptStage::Correct => LlmStage::Correct,
        PromptStage::Translate => LlmStage::Translate,
    };
    PromptSnapshot {
        schema_version: bundle.schema_version,
        stage,
        files: bundle.files,
        content_hash: bundle.content_hash,
    }
}

impl IdGenerator for UlidGenerator {
    fn next_id(&self) -> UlidStr {
        Ulid::new().into()
    }
}

struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0)
    }
}

struct NoopEventPublisher;

#[async_trait]
impl EventPublisher for NoopEventPublisher {
    async fn publish(&self, _event: DomainEvent) -> videocaptionerr_core::AppResult<()> {
        Ok(())
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

fn find_on_path(command: &str) -> Option<PathBuf> {
    std::env::var_os("PATH")?.to_str().and_then(|path| {
        std::env::split_paths(path).find_map(|dir| {
            let candidate = dir.join(command);
            candidate.is_file().then_some(candidate)
        })
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use videocaptionerr_llm::provider::{ProviderCapabilities, CAPABILITY_PROBE_VERSION};

    fn probe_config() -> ProbeConfig {
        ProbeConfig::new(
            "primary",
            7,
            "https://example.test/v1/",
            "test-only-secret",
            "model-a",
        )
    }

    fn probe_fixture(config: &ProbeConfig) -> ProbeResult {
        ProbeResult {
            probe_version: CAPABILITY_PROBE_VERSION,
            provider_profile_id: config.provider_profile_id.clone(),
            profile_revision: config.profile_revision,
            base_url: config.base_url.trim_end_matches('/').into(),
            model: config.model.clone(),
            probe_hash: CapabilityProbe::new(config.clone()).cache_key(),
            capabilities: ProviderCapabilities::conservative_default(),
            warnings: vec![],
        }
    }

    #[test]
    fn cached_probe_requires_full_identity_match() {
        let config = probe_config();
        let fixture = probe_fixture(&config);
        let encoded = serde_json::to_string(&fixture).unwrap();
        let decoded = decode_probe_result(&encoded, &config, None).unwrap();
        assert_eq!(decoded.probe_hash, fixture.probe_hash);

        let mut corrupt = fixture;
        corrupt.model = "different-model".into();
        let error = decode_probe_result(&serde_json::to_string(&corrupt).unwrap(), &config, None)
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::CacheCorrupt);
    }

    #[test]
    fn manual_capability_override_wins_over_cached_auto_result() {
        let config = probe_config();
        let fixture = probe_fixture(&config);
        let override_config = LlmCapabilityOverride {
            structured_mode: Some("json_schema".into()),
            ..Default::default()
        };
        let decoded = decode_probe_result(
            &serde_json::to_string(&fixture).unwrap(),
            &config,
            Some(&override_config),
        )
        .unwrap();
        assert_eq!(
            decoded.capabilities.effective_structured_mode(),
            StructuredMode::JsonSchema
        );
        assert!(decoded.capabilities.manual_override);
    }

    #[tokio::test]
    async fn probe_without_a_profile_fails_before_any_provider_request() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = ApplicationRuntime::open(RuntimeConfig {
            home: Some(dir.path().to_path_buf()),
            engine: "fake".into(),
            model_path: None,
            helper_path: None,
            prompt_dir: None,
        })
        .unwrap();
        let error = runtime
            .probe_llm_capabilities(None, false)
            .await
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::LlmProviderUnavailable);
    }
}
