//! Inbound CLI adapter.
//!
//! This module parses commands, renders responses, and calls the shared
//! bootstrap facade. It does not know about SQL, workers, ffmpeg, or files.

use std::path::PathBuf;
use std::process::ExitCode as StdExitCode;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;
use ulid::Ulid;
use videocaptionerr_bootstrap::{
    ApplicationRuntime, ProcessOptions, RuntimeConfig, TranscribeOptions,
};
use videocaptionerr_contracts::error::{ErrorCode, VcError};
use videocaptionerr_contracts::event::{CliEvent, EventEnvelope, ExitCode};

#[derive(Debug, Parser)]
#[command(
    name = "videocaptionerr",
    version,
    about = "Batch subtitle generation (ASR + LLM correction/translation)"
)]
struct Cli {
    /// Emit machine NDJSON events on stdout; human logs are written to stderr.
    #[arg(long, global = true)]
    json: bool,

    /// Override application home (same as VIDEOCAPTIONERR_HOME).
    #[arg(long, global = true, env = "VIDEOCAPTIONERR_HOME")]
    home: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Show version and workspace health.
    Doctor,
    /// Transcribe media to subtitles (ASR and rule splitting).
    Transcribe {
        #[arg(required = true)]
        files: Vec<PathBuf>,
        #[arg(long)]
        profile: Option<String>,
        /// ASR helper engine: fake (default for smoke) or whisper-cpp.
        #[arg(long, default_value = "fake")]
        engine: String,
        /// Explicit model path (required for non-fake engines; never auto-selected).
        #[arg(long)]
        model: Option<PathBuf>,
        /// Override helper binary path.
        #[arg(long, env = "VIDEOCAPTIONERR_HELPER")]
        helper: Option<PathBuf>,
        #[arg(long)]
        language: Option<String>,
        #[arg(long, default_value = "srt")]
        format: String,
    },
    /// Full process: ASR + split + correct + translate.
    Process {
        #[arg(required = true)]
        files: Vec<PathBuf>,
        #[arg(long)]
        profile: Option<String>,
        #[arg(long)]
        target_lang: Option<String>,
        #[arg(long, default_value = "fake")]
        engine: String,
        #[arg(long)]
        model: Option<PathBuf>,
        #[arg(long, env = "VIDEOCAPTIONERR_HELPER")]
        helper: Option<PathBuf>,
        #[arg(long)]
        language: Option<String>,
        #[arg(long, default_value = "srt")]
        format: String,
    },
    /// Job management.
    Jobs {
        #[command(subcommand)]
        action: JobsCmd,
    },
    /// Cache maintenance.
    Cache {
        #[command(subcommand)]
        action: CacheCmd,
    },
    /// LLM provider management.
    Providers {
        #[command(subcommand)]
        action: ProvidersCmd,
    },
    /// Batch control (pause/resume).
    Batch {
        #[command(subcommand)]
        action: BatchCmd,
    },
    /// Import SRT/VTT into a Job (no ASR).
    ImportSubtitle {
        file: PathBuf,
        /// mono | source-above | translation-above
        #[arg(long, default_value = "mono")]
        layout: String,
    },
    /// Model install / verify (explicit only; never silent).
    Models {
        #[command(subcommand)]
        action: ModelsCmd,
    },
}

#[derive(Debug, Subcommand)]
enum JobsCmd {
    List,
    Retry {
        id: String,
        /// Retry this stage and all later stages. Defaults to the first failed stage.
        #[arg(long)]
        from_stage: Option<String>,
        #[arg(long)]
        dry_run: bool,
    },
    Rm {
        id: String,
    },
}

#[derive(Debug, Subcommand)]
enum CacheCmd {
    Gc {
        #[arg(long, default_value = "20G")]
        max_size: String,
    },
}

#[derive(Debug, Subcommand)]
enum ProvidersCmd {
    /// Probe the configured provider; use --force to ignore its cached result.
    Probe {
        #[arg(long)]
        id: Option<String>,
        #[arg(long)]
        force: bool,
    },
}

#[derive(Debug, Subcommand)]
enum BatchCmd {
    /// Stop starting new Jobs; model stays loaded until Batch terminal.
    Pause { id: String },
    /// Resume a paused Batch.
    Resume { id: String },
}

#[derive(Debug, Subcommand)]
enum ModelsCmd {
    /// Download and verify a model from the manifest by id.
    Install {
        model_id: String,
        #[arg(long)]
        dest: Option<PathBuf>,
    },
    /// Verify an on-disk model file against a SHA-256 hex digest.
    Verify {
        path: PathBuf,
        #[arg(long)]
        sha256: String,
    },
}

fn main() -> StdExitCode {
    let cli = Cli::parse();
    init_tracing(cli.json);

    let code = match run(cli) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("error: {error}");
            map_error_code(&error)
        }
    };
    StdExitCode::from(code.as_i32() as u8)
}

fn init_tracing(json: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
}

fn run(cli: Cli) -> Result<ExitCode, VcError> {
    match cli.command {
        Commands::Doctor => {
            let runtime = ApplicationRuntime::open(RuntimeConfig {
                home: cli.home,
                engine: "fake".into(),
                model_path: None,
                helper_path: None,
                prompt_dir: None,
            })?;
            let report = runtime.doctor();
            println!("videocaptionerr {}", report.version);
            println!("home: {}", report.paths.home.display());
            println!("db: {}", report.paths.db_path.display());
            println!("store: ok ({})", report.paths.db_path.display());
            println!(
                "ffmpeg: {}",
                report
                    .ffmpeg
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "not found".into())
            );
            println!(
                "ffprobe: {}",
                report
                    .ffprobe
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "not found".into())
            );
            println!(
                "helper: {}",
                if report.helper_exists {
                    report.helper.display().to_string()
                } else {
                    "not found".into()
                }
            );
            println!(
                "uv: {}",
                report
                    .uv
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "not found".into())
            );
            for smoke in &report.runtime_smokes {
                println!(
                    "runtime {}: {} — {}",
                    smoke.family,
                    if smoke.ok { "ok" } else { "fail" },
                    smoke.detail
                );
            }
            Ok(ExitCode::Success)
        }
        Commands::Transcribe {
            files,
            profile,
            engine,
            model,
            helper,
            language,
            format,
        } => {
            let runtime = ApplicationRuntime::open(RuntimeConfig {
                home: cli.home,
                engine,
                model_path: model,
                helper_path: helper,
                prompt_dir: None,
            })?;
            let _lock = runtime.acquire_cli_processing_lock()?;
            let options = TranscribeOptions {
                files,
                language,
                format,
                profile,
                target_language: None,
                layout: videocaptionerr_core::ports::SubtitleLayout::SourceOnly,
            };
            let async_runtime = tokio::runtime::Runtime::new().map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("create Tokio runtime: {error}"),
                )
            })?;
            let result = async_runtime.block_on(runtime.transcribe(options))?;
            emit_batch_result(cli.json, result)
        }
        Commands::Process {
            files,
            profile,
            target_lang,
            engine,
            model,
            helper,
            language,
            format,
        } => {
            let runtime = ApplicationRuntime::open(RuntimeConfig {
                home: cli.home,
                engine,
                model_path: model,
                helper_path: helper,
                prompt_dir: None,
            })?;
            let _lock = runtime.acquire_cli_processing_lock()?;
            let target_language = target_lang.ok_or_else(|| {
                VcError::new(ErrorCode::InvalidArgument, "process requires --target-lang")
            })?;
            let async_runtime = tokio::runtime::Runtime::new().map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("create Tokio runtime: {error}"),
                )
            })?;
            let result = async_runtime.block_on(runtime.process(ProcessOptions {
                files,
                language,
                target_language,
                format,
                profile,
            }))?;
            emit_batch_result(cli.json, result)
        }
        Commands::Jobs { action } => {
            let runtime = ApplicationRuntime::open(RuntimeConfig {
                home: cli.home,
                engine: "fake".into(),
                model_path: None,
                helper_path: None,
                prompt_dir: None,
            })?;
            match action {
                JobsCmd::List => {
                    let async_runtime = tokio::runtime::Runtime::new().map_err(|error| {
                        VcError::new(
                            ErrorCode::Internal,
                            format!("create Tokio runtime: {error}"),
                        )
                    })?;
                    for job in async_runtime.block_on(runtime.list_jobs())? {
                        if cli.json {
                            emit_event(
                                CliEvent::JobListed,
                                Some(job.id().to_string()),
                                serde_json::json!({
                                    "status": format!("{:?}", job.status()).to_ascii_lowercase(),
                                    "source_path": job.source_path(),
                                }),
                            )?;
                        } else {
                            println!("{}\t{:?}\t{}", job.id(), job.status(), job.source_path());
                        }
                    }
                    Ok(ExitCode::Success)
                }
                JobsCmd::Retry {
                    id,
                    from_stage,
                    dry_run,
                } => {
                    let _lock = runtime.acquire_cli_processing_lock()?;
                    let async_runtime = tokio::runtime::Runtime::new().map_err(|error| {
                        VcError::new(
                            ErrorCode::Internal,
                            format!("create Tokio runtime: {error}"),
                        )
                    })?;
                    let outcome = async_runtime.block_on(runtime.retry_job(
                        &id,
                        from_stage.as_deref(),
                        dry_run,
                    ))?;
                    let plan = outcome.plan();
                    let terminal = match &outcome {
                        videocaptionerr_bootstrap::RetryJobOutcome::DryRun(_) => None,
                        videocaptionerr_bootstrap::RetryJobOutcome::Executed { result, .. } => {
                            result
                                .jobs
                                .first()
                                .map(|job| format!("{:?}", job.job.status()))
                        }
                    };
                    emit_or_print(
                        cli.json,
                        CliEvent::RetryFinished,
                        Some(plan.job_id.to_string()),
                        serde_json::json!({
                            "start_stage": plan.start_stage.as_str(),
                            "reused_artifacts": plan.reused_artifacts.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                            "invalidated_stages": plan.invalidated_stages.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                            "work_units_to_reset": plan.work_units_to_reset,
                            "output_path": plan.output_path,
                            "needs_runtime": plan.needs_runtime,
                            "dry_run": plan.dry_run,
                            "terminal_status": terminal,
                        }),
                        format!(
                            "job {}: {} from {} (reuse {:?}, invalidate {:?})",
                            plan.job_id,
                            if plan.dry_run {
                                "would retry"
                            } else {
                                "retried"
                            },
                            plan.start_stage.as_str(),
                            plan.reused_artifacts,
                            plan.invalidated_stages,
                        ),
                    )?;
                    Ok(ExitCode::Success)
                }
                JobsCmd::Rm { id } => {
                    let _lock = runtime.acquire_cli_processing_lock()?;
                    let async_runtime = tokio::runtime::Runtime::new().map_err(|error| {
                        VcError::new(
                            ErrorCode::Internal,
                            format!("create Tokio runtime: {error}"),
                        )
                    })?;
                    async_runtime.block_on(runtime.remove_job(&id))?;
                    Ok(ExitCode::Success)
                }
            }
        }
        Commands::Cache {
            action: CacheCmd::Gc { max_size },
        } => {
            let runtime = ApplicationRuntime::open(RuntimeConfig {
                home: cli.home,
                engine: "fake".into(),
                model_path: None,
                helper_path: None,
                prompt_dir: None,
            })?;
            let _lock = runtime.acquire_cli_processing_lock()?;
            let max_bytes = parse_size(&max_size)?;
            let async_runtime = tokio::runtime::Runtime::new().map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("create Tokio runtime: {error}"),
                )
            })?;
            let report = async_runtime.block_on(runtime.gc_cache(max_bytes))?;
            emit_or_print(
                cli.json,
                CliEvent::CacheGcFinished,
                None,
                serde_json::json!({
                    "max_bytes": max_bytes,
                    "before_bytes": report.before_bytes,
                    "after_bytes": report.after_bytes,
                    "deleted_entries": report.deleted_entries,
                    "skipped_leased": report.skipped_leased,
                }),
                format!(
                    "cache GC: {} -> {} bytes, deleted {}, skipped {} leased",
                    report.before_bytes,
                    report.after_bytes,
                    report.deleted_entries,
                    report.skipped_leased,
                ),
            )?;
            Ok(ExitCode::Success)
        }
        Commands::Providers {
            action: ProvidersCmd::Probe { id, force },
        } => {
            let runtime = ApplicationRuntime::open(RuntimeConfig {
                home: cli.home,
                engine: "fake".into(),
                model_path: None,
                helper_path: None,
                prompt_dir: None,
            })?;
            let async_runtime = tokio::runtime::Runtime::new().map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("create Tokio runtime: {error}"),
                )
            })?;
            let result =
                async_runtime.block_on(runtime.probe_llm_capabilities(id.as_deref(), force))?;
            let data = serde_json::json!({
                "provider_profile_id": result.provider_profile_id,
                "profile_revision": result.profile_revision,
                "model": result.model,
                "probe_hash": result.probe_hash,
                "capabilities": result.capabilities,
                "warnings": result.warnings,
            });
            emit_or_print(
                cli.json,
                CliEvent::ProviderProbeFinished,
                None,
                data,
                format!(
                    "provider {} / {} probed (structured: {:?})",
                    result.provider_profile_id,
                    result.model,
                    result.capabilities.effective_structured_mode(),
                ),
            )?;
            Ok(ExitCode::Success)
        }
        Commands::Batch { action } => {
            let runtime = ApplicationRuntime::open(RuntimeConfig {
                home: cli.home,
                engine: "fake".into(),
                model_path: None,
                helper_path: None,
                prompt_dir: None,
            })?;
            let async_runtime = tokio::runtime::Runtime::new().map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("create Tokio runtime: {error}"),
                )
            })?;
            match action {
                BatchCmd::Pause { id } => {
                    async_runtime.block_on(runtime.pause_batch(&id))?;
                    if cli.json {
                        emit_event(
                            CliEvent::JobListed,
                            None,
                            serde_json::json!({"batch_id": id, "action": "pause"}),
                        )?;
                    } else {
                        println!("batch {id} pause requested");
                    }
                    Ok(ExitCode::Success)
                }
                BatchCmd::Resume { id } => {
                    async_runtime.block_on(runtime.resume_batch(&id))?;
                    if cli.json {
                        emit_event(
                            CliEvent::JobListed,
                            None,
                            serde_json::json!({"batch_id": id, "action": "resume"}),
                        )?;
                    } else {
                        println!("batch {id} resumed");
                    }
                    Ok(ExitCode::Success)
                }
            }
        }
        Commands::ImportSubtitle { file, layout } => {
            let runtime = ApplicationRuntime::open(RuntimeConfig {
                home: cli.home,
                engine: "fake".into(),
                model_path: None,
                helper_path: None,
                prompt_dir: None,
            })?;
            let async_runtime = tokio::runtime::Runtime::new().map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("create Tokio runtime: {error}"),
                )
            })?;
            let result =
                async_runtime.block_on(runtime.import_subtitle(&file, Some(layout.as_str())))?;
            emit_or_print(
                cli.json,
                CliEvent::JobListed,
                Some(result.job_id.clone()),
                serde_json::json!({
                    "job_id": result.job_id,
                    "cue_count": result.cue_count,
                    "warnings": result.warnings,
                    "transcript_path": result.transcript_path,
                }),
                format!(
                    "imported {} cues into Job {} ({})",
                    result.cue_count,
                    result.job_id,
                    result.transcript_path.display()
                ),
            )?;
            Ok(ExitCode::Success)
        }
        Commands::Models { action } => {
            let runtime = ApplicationRuntime::open(RuntimeConfig {
                home: cli.home,
                engine: "fake".into(),
                model_path: None,
                helper_path: None,
                prompt_dir: None,
            })?;
            match action {
                ModelsCmd::Install { model_id, dest } => {
                    let async_runtime = tokio::runtime::Runtime::new().map_err(|error| {
                        VcError::new(
                            ErrorCode::Internal,
                            format!("create Tokio runtime: {error}"),
                        )
                    })?;
                    let result = async_runtime.block_on(runtime.install_model(&model_id, dest))?;
                    emit_or_print(
                        cli.json,
                        CliEvent::JobListed,
                        None,
                        serde_json::json!({
                            "model_id": result.model_id,
                            "path": result.path,
                            "sha256": result.sha256,
                        }),
                        format!("installed {} -> {}", result.model_id, result.path.display()),
                    )?;
                    Ok(ExitCode::Success)
                }
                ModelsCmd::Verify { path, sha256 } => {
                    runtime.verify_model(&path, &sha256)?;
                    emit_or_print(
                        cli.json,
                        CliEvent::JobListed,
                        None,
                        serde_json::json!({"path": path, "sha256": sha256, "ok": true}),
                        format!("verified {}", path.display()),
                    )?;
                    Ok(ExitCode::Success)
                }
            }
        }
    }
}

fn emit_or_print(
    json: bool,
    event: CliEvent,
    job_id: Option<String>,
    data: serde_json::Value,
    human: String,
) -> Result<(), VcError> {
    if json {
        emit_event(event, job_id, data)
    } else {
        println!("{human}");
        Ok(())
    }
}

fn emit_batch_result(
    json: bool,
    result: videocaptionerr_core::use_cases::RunBatchResponse,
) -> Result<ExitCode, VcError> {
    for job in &result.jobs {
        emit_or_print(
            json,
            CliEvent::JobFinished,
            Some(job.job.id().to_string()),
            serde_json::json!({
                "status": format!("{:?}", job.job.status()).to_ascii_lowercase(),
                "cues": job.transcript.cues.len(),
                "path": job.export_path,
            }),
            format!(
                "job {} done: {} cues -> {}",
                job.job.id(),
                job.transcript.cues.len(),
                job.export_path.display()
            ),
        )?;
    }
    for failure in &result.failures {
        if json {
            emit_event(
                CliEvent::Error,
                Some(failure.job_id.clone()),
                serde_json::json!({
                    "code": failure.error.code.as_str(),
                    "message": failure.error.message,
                }),
            )?;
        } else {
            eprintln!("job {} failed: {}", failure.job_id, failure.error);
        }
    }
    if result.failures.is_empty() {
        Ok(ExitCode::Success)
    } else if result.jobs.is_empty() {
        Err(result.failures[0].error.clone())
    } else {
        Ok(ExitCode::PartialBatchSuccess)
    }
}

fn emit_event(
    event: CliEvent,
    job_id: Option<String>,
    data: serde_json::Value,
) -> Result<(), VcError> {
    let envelope = EventEnvelope::new(Ulid::new().to_string(), job_id, event.as_str(), Some(data));
    let line = envelope.to_ndjson_line().map_err(|error| {
        VcError::new(ErrorCode::Internal, format!("serialize CLI event: {error}"))
    })?;
    print!("{line}");
    Ok(())
}

fn map_error_code(error: &VcError) -> ExitCode {
    match error.code {
        ErrorCode::InstanceBusy
        | ErrorCode::FfmpegUnavailable
        | ErrorCode::RuntimeUnavailable
        | ErrorCode::WorkerStartFailed
        | ErrorCode::DeviceUnavailable => ExitCode::DependencyUnavailable,
        ErrorCode::InputNotFound
        | ErrorCode::InputUnsupported
        | ErrorCode::ProbeFailed
        | ErrorCode::AudioStreamNotFound => ExitCode::InputFailure,
        ErrorCode::AsrFailed
        | ErrorCode::AsrOom
        | ErrorCode::WorkerCrashed
        | ErrorCode::WorkerTimeout
        | ErrorCode::WorkerProtocolError
        | ErrorCode::EngineCapabilityInsufficient => ExitCode::AsrFailure,
        ErrorCode::LlmAuthFailed
        | ErrorCode::LlmModelNotFound
        | ErrorCode::LlmRateLimited
        | ErrorCode::LlmProviderUnavailable
        | ErrorCode::LlmContextExceeded
        | ErrorCode::LlmInvalidResponse
        | ErrorCode::LlmValidationFailed => ExitCode::LlmFailure,
        ErrorCode::ExportValidationFailed | ErrorCode::ExportFailed => ExitCode::ExportFailure,
        ErrorCode::Cancelled => ExitCode::Cancelled,
        ErrorCode::PartialBatchSuccess => ExitCode::PartialBatchSuccess,
        _ => ExitCode::InvalidArgs,
    }
}

fn parse_size(input: &str) -> Result<u64, VcError> {
    let normalized = input.trim().to_ascii_uppercase();
    let (number, multiplier) = if let Some(value) = normalized.strip_suffix("TIB") {
        (value, 1024u64.pow(4))
    } else if let Some(value) = normalized.strip_suffix("TB") {
        (value, 1_000_000_000_000)
    } else if let Some(value) = normalized.strip_suffix("GIB") {
        (value, 1024u64.pow(3))
    } else if let Some(value) = normalized.strip_suffix("GB") {
        (value, 1_000_000_000)
    } else if let Some(value) = normalized.strip_suffix('G') {
        (value, 1024u64.pow(3))
    } else if let Some(value) = normalized.strip_suffix("MIB") {
        (value, 1024u64.pow(2))
    } else if let Some(value) = normalized.strip_suffix("MB") {
        (value, 1_000_000)
    } else if let Some(value) = normalized.strip_suffix('M') {
        (value, 1024u64.pow(2))
    } else if let Some(value) = normalized.strip_suffix("KIB") {
        (value, 1024)
    } else if let Some(value) = normalized.strip_suffix("KB") {
        (value, 1_000)
    } else if let Some(value) = normalized.strip_suffix('K') {
        (value, 1024)
    } else if let Some(value) = normalized.strip_suffix('B') {
        (value, 1)
    } else {
        (normalized.as_str(), 1)
    };
    let value = number.trim().parse::<u64>().map_err(|error| {
        VcError::new(
            ErrorCode::InvalidArgument,
            format!("invalid size '{input}': {error}"),
        )
    })?;
    value
        .checked_mul(multiplier)
        .ok_or_else(|| VcError::new(ErrorCode::InvalidArgument, "cache size exceeds u64"))
}
