//! Inbound CLI adapter.
//!
//! This module parses commands, renders responses, and calls the shared
//! bootstrap facade. It does not know about SQL, workers, ffmpeg, or files.

use std::path::PathBuf;
use std::process::ExitCode as StdExitCode;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;
use ulid::Ulid;
use videocaptionerr_bootstrap::{ApplicationRuntime, RuntimeConfig, TranscribeOptions};
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
    /// Full process: ASR + split + correct + translate. Implemented in a later milestone.
    Process {
        #[arg(required = true)]
        files: Vec<PathBuf>,
        #[arg(long)]
        profile: Option<String>,
        #[arg(long)]
        target_lang: Option<String>,
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
}

#[derive(Debug, Subcommand)]
enum JobsCmd {
    List,
    Retry { id: String },
    Rm { id: String },
}

#[derive(Debug, Subcommand)]
enum CacheCmd {
    Gc {
        #[arg(long, default_value = "20G")]
        max_size: String,
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
                if report.helper.exists() {
                    report.helper.display().to_string()
                } else {
                    "not found".into()
                }
            );
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
            })?;
            let _lock = runtime.acquire_cli_processing_lock()?;
            let options = TranscribeOptions {
                files,
                language,
                format,
                profile,
            };
            let async_runtime = tokio::runtime::Runtime::new().map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("create Tokio runtime: {error}"),
                )
            })?;
            let result = async_runtime.block_on(runtime.transcribe(options))?;
            for job in &result.jobs {
                emit_or_print(
                    cli.json,
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
                if cli.json {
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
        Commands::Process {
            files: _,
            profile: _,
            target_lang: _,
        } => {
            let runtime = ApplicationRuntime::open(RuntimeConfig {
                home: cli.home,
                engine: "fake".into(),
                model_path: None,
                helper_path: None,
            })?;
            let _lock = runtime.acquire_cli_processing_lock()?;
            Err(VcError::new(
                ErrorCode::InvalidArgument,
                "process pipeline is not enabled in the current milestone",
            ))
        }
        Commands::Jobs { action } => {
            let runtime = ApplicationRuntime::open(RuntimeConfig {
                home: cli.home,
                engine: "fake".into(),
                model_path: None,
                helper_path: None,
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
                JobsCmd::Retry { id } => {
                    let _lock = runtime.acquire_cli_processing_lock()?;
                    Err(VcError::new(
                        ErrorCode::InvalidArgument,
                        format!("jobs retry {id} is not enabled in the current milestone"),
                    ))
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
            })?;
            let _lock = runtime.acquire_cli_processing_lock()?;
            println!("cache gc max_size={max_size}: skeleton (M5)");
            Ok(ExitCode::Success)
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
