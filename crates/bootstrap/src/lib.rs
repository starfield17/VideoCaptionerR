//! Composition root shared by the CLI and the future desktop shell.
//!
//! This crate is the only place that assembles concrete infrastructure
//! adapters. It exposes application-shaped operations to inbound adapters.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use ulid::Ulid;
use videocaptionerr_asr::{resolve_helper_binary, WorkerAsrRuntime};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::application_error::ApplicationError;
use videocaptionerr_core::ports::{
    ArtifactStore, AsrRuntime, BatchRepository, EventPublisher, IdGenerator, JobRepository,
    MediaGateway, SubtitleExportRequest, SubtitleFormat, SubtitleGateway, SubtitleLayout,
};
use videocaptionerr_core::use_cases::{
    RunBatch, RunBatchCommand, RunBatchResponse, TranscribeJob, TranscribeJobCommand,
};
use videocaptionerr_domain::{Batch, BatchExecutionProfile, BatchId, DomainEvent, JobId, UlidStr};
use videocaptionerr_platform::subtitle_io::{ConflictPolicy, ExportFormat, OutputPlanner};
use videocaptionerr_platform::{
    AppPaths, FfmpegMediaGateway, FileSubtitleGateway, InstanceLock, LockOwner,
};
use videocaptionerr_store::{SqliteArtifactStore, StoreHandle};

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub home: Option<PathBuf>,
    pub engine: String,
    pub model_path: Option<PathBuf>,
    pub helper_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct TranscribeOptions {
    pub files: Vec<PathBuf>,
    pub language: Option<String>,
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

pub struct ApplicationRuntime {
    paths: AppPaths,
    engine: String,
    model_path: Option<PathBuf>,
    helper_path: PathBuf,
    jobs: Arc<dyn JobRepository>,
    run_batch: Arc<RunBatch>,
}

impl ApplicationRuntime {
    pub fn open(config: RuntimeConfig) -> VcResult<Self> {
        let paths = match config.home {
            Some(home) => AppPaths::from_home(home),
            None => AppPaths::resolve()?,
        };
        paths.ensure_layout()?;

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
        let artifacts: Arc<dyn ArtifactStore> = Arc::new(SqliteArtifactStore::new(store));
        let media: Arc<dyn MediaGateway> = Arc::new(FfmpegMediaGateway::default());
        let subtitles: Arc<dyn SubtitleGateway> = Arc::new(FileSubtitleGateway);
        let events: Arc<dyn EventPublisher> = Arc::new(NoopEventPublisher);
        let ids: Arc<dyn IdGenerator> = Arc::new(UlidGenerator);
        let asr: Arc<dyn AsrRuntime> = Arc::new(WorkerAsrRuntime::new(
            helper_path.clone(),
            config.engine.clone(),
            config.model_path.clone(),
        ));
        let transcribe = Arc::new(TranscribeJob::new(
            jobs.clone(),
            media,
            artifacts,
            subtitles,
            events.clone(),
            ids,
        ));
        let run_batch = Arc::new(RunBatch::new(
            batches,
            jobs.clone(),
            asr,
            transcribe,
            events,
        ));

        Ok(Self {
            paths,
            engine: config.engine,
            model_path: config.model_path,
            helper_path,
            jobs,
            run_batch,
        })
    }

    pub fn paths(&self) -> &AppPaths {
        &self.paths
    }

    pub fn acquire_processing_lock(&self, owner: LockOwner) -> VcResult<InstanceLock> {
        InstanceLock::try_acquire(&self.paths.instance_lock_path(), owner)
    }

    pub fn acquire_cli_processing_lock(&self) -> VcResult<InstanceLock> {
        self.acquire_processing_lock(LockOwner::Cli)
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

    pub async fn list_jobs(&self) -> Result<Vec<videocaptionerr_domain::Job>, VcError> {
        self.jobs
            .list_jobs()
            .await
            .map_err(ApplicationError::into_vc_error)
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

    pub async fn transcribe(&self, options: TranscribeOptions) -> VcResult<RunBatchResponse> {
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
                None,
                SubtitleLayout::SourceOnly.into_platform(),
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
                    layout: SubtitleLayout::SourceOnly,
                    fallback_to_source: false,
                },
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

struct UlidGenerator;

impl IdGenerator for UlidGenerator {
    fn next_id(&self) -> UlidStr {
        Ulid::new().into()
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
