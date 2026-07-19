//! VideoCaptionerR CLI entry point.

use std::path::PathBuf;
use std::process::ExitCode as StdExitCode;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;
use videocaptionerr_contracts::error::ErrorCode;
use videocaptionerr_contracts::event::ExitCode;
use videocaptionerr_store::instance_lock::LockOwner;
use videocaptionerr_store::{AppPaths, InstanceLock};

#[derive(Debug, Parser)]
#[command(
    name = "videocaptionerr",
    version,
    about = "Batch subtitle generation (ASR + LLM correction/translation)"
)]
struct Cli {
    /// Emit machine NDJSON events on stdout; human logs on stderr.
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
    /// Transcribe media to subtitles (ASR only). Implemented from M2.
    Transcribe {
        #[arg(required = true)]
        files: Vec<PathBuf>,
        #[arg(long)]
        profile: Option<String>,
    },
    /// Full process: ASR + split + correct + translate. Implemented from M3/M5.
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
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            map_error_code(e.as_ref())
        }
    };
    StdExitCode::from(code.as_i32() as u8)
}

fn init_tracing(json: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .init();
    }
}

fn run(cli: Cli) -> Result<ExitCode, Box<dyn std::error::Error>> {
    if let Some(home) = &cli.home {
        std::env::set_var("VIDEOCAPTIONERR_HOME", home);
    }
    let paths = AppPaths::resolve().map_err(|e| e.to_string())?;
    paths.ensure_layout().map_err(|e| e.to_string())?;

    match cli.command {
        Commands::Doctor => {
            println!("videocaptionerr {}", env!("CARGO_PKG_VERSION"));
            println!("home: {}", paths.home.display());
            println!("db: {}", paths.db_path.display());
            let store =
                videocaptionerr_store::Store::open(&paths.db_path).map_err(|e| e.to_string())?;
            println!("store: ok ({})", store.path().display());
            println!("ffmpeg: {}", which("ffmpeg"));
            println!("ffprobe: {}", which("ffprobe"));
            Ok(ExitCode::Success)
        }
        Commands::Transcribe { .. } | Commands::Process { .. } => {
            let _lock = InstanceLock::try_acquire(&paths.instance_lock_path(), LockOwner::Cli)
                .map_err(|e| e.to_string())?;
            eprintln!("not implemented yet (milestone M2/M3/M5)");
            Ok(ExitCode::InvalidArgs)
        }
        Commands::Jobs { action } => {
            let store =
                videocaptionerr_store::Store::open(&paths.db_path).map_err(|e| e.to_string())?;
            match action {
                JobsCmd::List => {
                    let mut stmt = store
                        .conn()
                        .prepare("SELECT id, status, source_path FROM jobs ORDER BY created_at")
                        .map_err(|e| e.to_string())?;
                    let rows = stmt
                        .query_map([], |r| {
                            Ok((
                                r.get::<_, String>(0)?,
                                r.get::<_, String>(1)?,
                                r.get::<_, String>(2)?,
                            ))
                        })
                        .map_err(|e| e.to_string())?;
                    for row in rows {
                        let (id, status, src) = row.map_err(|e| e.to_string())?;
                        println!("{id}\t{status}\t{src}");
                    }
                    Ok(ExitCode::Success)
                }
                JobsCmd::Retry { id } => {
                    eprintln!("jobs retry {id}: not fully implemented (M5)");
                    Ok(ExitCode::InvalidArgs)
                }
                JobsCmd::Rm { id } => {
                    store
                        .conn()
                        .execute("DELETE FROM jobs WHERE id = ?1", [&id])
                        .map_err(|e| e.to_string())?;
                    Ok(ExitCode::Success)
                }
            }
        }
        Commands::Cache {
            action: CacheCmd::Gc { max_size },
        } => {
            println!("cache gc max_size={max_size}: skeleton (M5)");
            Ok(ExitCode::Success)
        }
    }
}

fn which(cmd: &str) -> String {
    std::process::Command::new("sh")
        .arg("-c")
        .arg(format!("command -v {cmd} || true"))
        .output()
        .ok()
        .and_then(|o| {
            let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
            if s.is_empty() {
                None
            } else {
                Some(s)
            }
        })
        .unwrap_or_else(|| "not found".into())
}

fn map_error_code(err: &dyn std::error::Error) -> ExitCode {
    let msg = err.to_string();
    if msg.contains(ErrorCode::InstanceBusy.as_str()) {
        ExitCode::DependencyUnavailable
    } else {
        // Default until richer error typing is threaded through the CLI adapter.
        ExitCode::InvalidArgs
    }
}
