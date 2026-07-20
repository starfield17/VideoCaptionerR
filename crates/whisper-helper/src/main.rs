//! Isolated ASR helper process (stdio NDJSON protocol).
//!
//! stdout: protocol only
//! stderr: logs
//!
//! Engines:
//! - `fake` (default): deterministic word timestamps for protocol/e2e tests
//! - `whisper-cpp`: native whisper.cpp FFI (feature-gated; only linked here)

mod audio;
mod fake_engine;
mod protocol;
mod session;
mod whisper_cpp_engine;

use std::io::{self, BufRead, BufReader};
use std::sync::Arc;

use clap::Parser;
use tracing::{error, info};

use protocol::HelperState;

#[derive(Debug, Parser)]
#[command(name = "videocaptionerr-whisper-helper")]
struct Args {
    /// Engine implementation: fake | whisper-cpp
    #[arg(long, default_value = "fake")]
    engine: String,
}

fn main() {
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    if let Err(e) = run(args) {
        error!("helper fatal: {e:#}");
        std::process::exit(1);
    }
}

fn run(args: Args) -> anyhow::Result<()> {
    let state = Arc::new(HelperState::new(args.engine));
    info!(
        engine = %state.engine,
        session = %state.session_id,
        "whisper-helper started"
    );

    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    let mut line_buf = Vec::new();

    loop {
        line_buf.clear();
        let n = reader.read_until(b'\n', &mut line_buf)?;
        if n == 0 {
            info!("stdin closed; shutting down");
            break;
        }
        let line = String::from_utf8_lossy(&line_buf);
        if !session::handle_line(&state, &line)? {
            break;
        }
    }
    Ok(())
}
