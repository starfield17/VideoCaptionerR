use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use videocaptionerr_asr::{resolve_helper_binary, FamilyAsrRuntimeResolver};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::ports::{
    ActiveRunRegistry, ArtifactRecoveryStore, ArtifactStore, AsrRuntimeResolver,
    BatchCreationRepository, BatchRepository, CacheRepository, CapabilityProbeStore,
    ChunkPlanStore, Clock, EventPublisher, IdGenerator, JobRepository, JobWorkspace,
    MediaFileCatalog, OutboxRepository, OutputPlanner as OutputPlannerPort,
    RetryTransactionRepository, SnapshotRepository, StageCommitRepository, SubtitleGateway,
    SubtitleImporter, WorkUnitRepository,
};
use videocaptionerr_core::use_cases::{
    CacheGc, CreateBatch, CreateBatchDependencies, ImportSubtitle, PersistChunkPlan,
    ProcessMediaFiles, RecoveryReport, ResumeBatch, RetryJob, RunBatch, StartupRecovery,
    TranscriptEditor, WorkUnitScheduler,
};
use videocaptionerr_platform::{
    AppConfig, AppJobWorkspace, AppPaths, ConflictPolicy, FfmpegMediaGateway, FileSubtitleGateway,
    InstanceLock, LocalMediaFileCatalog, LocalSubtitleImporter, LockOwner, PlatformOutputPlanner,
    ResolvedProfile,
};
use videocaptionerr_store::{CacheStore, SqliteArtifactStore, StoreHandle};

use crate::config::{LlmProcessDefaults, RuntimeConfig};
use crate::run_control::InMemoryActiveRunRegistry;

/// Opaque RAII lease shared by CLI and desktop inbound adapters.
pub struct ProcessingLease {
    pub(crate) _inner: InstanceLock,
}

pub struct ApplicationRuntime {
    pub(crate) paths: AppPaths,
    pub(crate) engine: String,
    pub(crate) model_path: Option<PathBuf>,
    pub(crate) model_digest: Option<String>,
    pub(crate) helper_path: PathBuf,
    pub(crate) jobs: Arc<dyn JobRepository>,
    pub(crate) batches: Arc<dyn BatchRepository>,
    pub(crate) work_units: Arc<dyn WorkUnitRepository>,
    pub(crate) snapshots: Arc<dyn SnapshotRepository>,
    pub(crate) run_batch: Arc<RunBatch>,
    pub(crate) process_media_files: Arc<ProcessMediaFiles>,
    pub(crate) import_subtitle_uc: Arc<ImportSubtitle>,
    pub(crate) resume_batch_uc: Arc<ResumeBatch>,
    pub(crate) retry_job_uc: Arc<RetryJob>,
    pub(crate) cache_gc: Arc<CacheGc>,
    pub(crate) scheduler: Arc<WorkUnitScheduler>,
    pub(crate) chunk_plans: Arc<PersistChunkPlan>,
    pub(crate) capability_probes: Arc<dyn CapabilityProbeStore>,
    pub(crate) transcript_editor: Arc<TranscriptEditor>,
    pub(crate) llm_defaults: Option<LlmProcessDefaults>,
    pub(crate) resolved_profile: ResolvedProfile,
    startup_recovery: Arc<StartupRecovery>,
    recovery_report: Mutex<RecoveryReport>,
    pub(crate) active_runs: Arc<dyn ActiveRunRegistry>,
}

impl ApplicationRuntime {
    pub fn open(config: RuntimeConfig) -> VcResult<Self> {
        let paths = match config.home {
            Some(home) => AppPaths::from_home(home),
            None => AppPaths::resolve()?,
        };
        paths.ensure_layout()?;
        let app_config = AppConfig::load(&paths.config_file)?;
        let resolved_profile = app_config.resolve_profile(config.profile.as_deref())?;
        let prompt_dir = config
            .prompt_dir
            .clone()
            .unwrap_or_else(crate::wiring::default_prompt_dir);

        let engine = config
            .engine
            .clone()
            .unwrap_or_else(|| resolved_profile.preferred_engine.clone());
        let digest_path = config.model_path.clone().or_else(|| {
            resolved_profile
                .model_id
                .as_ref()
                .map(PathBuf::from)
                .filter(|path| path.is_file())
        });
        let model_digest = digest_path
            .as_deref()
            .filter(|path| path.is_file())
            .map(videocaptionerr_store::blake3_file)
            .transpose()?;

        let helper_path = config.helper_path.unwrap_or_else(resolve_helper_binary);
        let store = StoreHandle::open(&paths.db_path)?;
        let jobs: Arc<dyn JobRepository> = Arc::new(store.clone());
        let batches: Arc<dyn BatchRepository> = Arc::new(store.clone());
        let creation: Arc<dyn BatchCreationRepository> = Arc::new(store.clone());
        let work_units: Arc<dyn WorkUnitRepository> = Arc::new(store.clone());
        let snapshots: Arc<dyn SnapshotRepository> = Arc::new(store.clone());
        let outbox: Arc<dyn OutboxRepository> = Arc::new(store.clone());
        let capability_probes: Arc<dyn CapabilityProbeStore> = Arc::new(store.clone());
        let cached_capabilities = crate::wiring::load_cached_capabilities(
            &store,
            &app_config,
            resolved_profile.llm_provider.as_deref(),
        )?;
        let artifact_adapter = Arc::new(SqliteArtifactStore::new(store.clone()));
        let artifacts: Arc<dyn ArtifactStore> = artifact_adapter.clone();
        let artifact_recovery: Arc<dyn ArtifactRecoveryStore> = artifact_adapter.clone();
        let stage_commits: Arc<dyn StageCommitRepository> = Arc::new(store.clone());
        let chunk_plan_store: Arc<dyn ChunkPlanStore> = artifact_adapter;
        let cache_store = CacheStore::new(&paths.cache_dir)?;
        let cache: Arc<dyn CacheRepository> = Arc::new(cache_store);
        let clock: Arc<dyn Clock> = Arc::new(crate::wiring::SystemClock);
        let media: Arc<dyn videocaptionerr_core::ports::MediaGateway> =
            Arc::new(FfmpegMediaGateway::default());
        let subtitles: Arc<dyn SubtitleGateway> = Arc::new(FileSubtitleGateway);
        let events: Arc<dyn EventPublisher> = Arc::new(store.clone());
        let ids: Arc<dyn IdGenerator> = Arc::new(crate::wiring::UlidGenerator);
        let chunk_plans = Arc::new(PersistChunkPlan::new(chunk_plan_store.clone(), ids.clone()));
        let transcript_editor = Arc::new(TranscriptEditor::new(
            jobs.clone(),
            artifacts.clone(),
            ids.clone(),
        ));
        let (llm_pipeline, llm_defaults) = crate::wiring::build_llm_pipeline(
            &app_config,
            resolved_profile.llm_provider.as_deref(),
            &prompt_dir,
            &paths.logs_dir,
            ids.clone(),
            cached_capabilities,
        )?;
        let recovery = StartupRecovery::new(
            jobs.clone(),
            batches.clone(),
            work_units.clone(),
            artifact_recovery,
            outbox,
        );
        let startup_recovery = Arc::new(recovery);
        let active_runs: Arc<dyn ActiveRunRegistry> =
            Arc::new(InMemoryActiveRunRegistry::default());
        let runtimes_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/runtimes");
        let resolver: Arc<dyn AsrRuntimeResolver> = Arc::new(FamilyAsrRuntimeResolver::new(
            helper_path.clone(),
            runtimes_root,
            paths.envs_dir.clone(),
        ));
        let mut transcribe_service = videocaptionerr_core::use_cases::TranscribeJob::new(
            jobs.clone(),
            media,
            artifacts,
            subtitles,
            events.clone(),
            ids.clone(),
            stage_commits.clone(),
        )
        .with_snapshots(snapshots.clone());
        if let Some(pipeline) = llm_pipeline {
            // Attach control-plane ports so each LlmPlan entry becomes an llm_batch WorkUnit.
            let pipeline = match Arc::try_unwrap(pipeline) {
                Ok(p) => Arc::new(
                    p.with_work_units(work_units.clone())
                        .with_stage_commits(stage_commits.clone()),
                ),
                Err(shared) => shared,
            };
            transcribe_service = transcribe_service.with_llm_pipeline(pipeline);
        }
        transcribe_service = transcribe_service.with_chunking(
            chunk_plan_store,
            cache.clone(),
            work_units.clone(),
            clock.clone(),
        );
        transcribe_service = transcribe_service.with_batches(batches.clone());
        let transcribe = Arc::new(transcribe_service);
        let run_batch = Arc::new(
            RunBatch::new(batches.clone(), jobs.clone(), resolver, transcribe, events)
                .with_active_runs(active_runs.clone()),
        );
        let conflict =
            ConflictPolicy::parse(&resolved_profile.conflict_policy).ok_or_else(|| {
                VcError::new(
                    ErrorCode::InvalidConfig,
                    format!(
                        "unknown output conflict policy '{}'",
                        resolved_profile.conflict_policy
                    ),
                )
            })?;
        let files: Arc<dyn MediaFileCatalog> = Arc::new(LocalMediaFileCatalog);
        let workspace: Arc<dyn JobWorkspace> =
            Arc::new(AppJobWorkspace::new(paths.jobs_dir.clone()));
        let importer: Arc<dyn SubtitleImporter> = Arc::new(LocalSubtitleImporter);
        let import_subtitle_uc = Arc::new(ImportSubtitle::new(
            ids.clone(),
            workspace.clone(),
            importer,
            stage_commits.clone(),
        ));
        let outputs: Arc<dyn OutputPlannerPort> = Arc::new(PlatformOutputPlanner::new(
            resolved_profile.output_template.clone(),
            conflict,
        ));
        let process_media_files = Arc::new(ProcessMediaFiles::new(
            CreateBatch::new(CreateBatchDependencies {
                creation,
                ids: ids.clone(),
                clock: clock.clone(),
                files,
                workspace,
                outputs,
            }),
            run_batch.clone(),
        ));
        let resume_batch_uc = Arc::new(ResumeBatch::new(
            batches.clone(),
            jobs.clone(),
            snapshots.clone(),
            run_batch.clone(),
        ));
        let retry_tx: Arc<dyn RetryTransactionRepository> = Arc::new(store.clone());
        let retry_job_uc = Arc::new(RetryJob::new(
            jobs.clone(),
            batches.clone(),
            work_units.clone(),
            snapshots.clone(),
            retry_tx,
        ));
        let cache_gc = Arc::new(CacheGc::new(cache));
        let scheduler = Arc::new(WorkUnitScheduler::new(work_units.clone(), clock));

        Ok(Self {
            paths,
            engine,
            model_path: config.model_path,
            model_digest,
            helper_path,
            jobs,
            batches,
            work_units,
            snapshots,
            run_batch,
            process_media_files,
            import_subtitle_uc,
            resume_batch_uc,
            retry_job_uc,
            cache_gc,
            scheduler,
            chunk_plans,
            capability_probes,
            transcript_editor,
            llm_defaults,
            resolved_profile,
            startup_recovery,
            recovery_report: Mutex::new(RecoveryReport::default()),
            active_runs,
        })
    }

    pub fn paths(&self) -> &AppPaths {
        &self.paths
    }

    pub fn recovery_report(&self) -> RecoveryReport {
        self.recovery_report
            .lock()
            .map(|report| report.clone())
            .unwrap_or_default()
    }

    pub(crate) fn acquire_processing_lock(&self, owner: LockOwner) -> VcResult<ProcessingLease> {
        // Opening a Runtime is read-only. Recovery is deliberately performed
        // only after the OS-backed owner lock has been acquired, so another
        // process can list/show/probe while a live owner keeps Running state.
        let inner = InstanceLock::try_acquire(&self.paths.instance_lock_path(), owner)?;
        let report = match crate::recovery::run_startup_recovery_sync(
            self.startup_recovery.clone(),
            vec![self.paths.jobs_dir.clone()],
        ) {
            Ok(report) => report,
            Err(error) => {
                drop(inner);
                return Err(error);
            }
        };
        if let Ok(mut current) = self.recovery_report.lock() {
            *current = report;
        }
        Ok(ProcessingLease { _inner: inner })
    }

    pub fn acquire_cli_processing_lock(&self) -> VcResult<ProcessingLease> {
        self.acquire_processing_lock(LockOwner::Cli)
    }

    pub fn acquire_gui_processing_lock(&self) -> VcResult<ProcessingLease> {
        self.acquire_processing_lock(LockOwner::Gui)
    }
}
