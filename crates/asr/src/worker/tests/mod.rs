//! Worker integration tests.

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::mpsc;
use videocaptionerr_contracts::error::ErrorCode;

use super::{resolve_helper_binary, WorkerClient};
use crate::options::AsrOptions;

fn helper_bin() -> PathBuf {
    resolve_helper_binary()
}

fn python_bin() -> Option<PathBuf> {
    [
        PathBuf::from("/home/hazel/miniconda3/envs/Lab/bin/python"),
        PathBuf::from("/usr/bin/python3"),
    ]
    .into_iter()
    .find(|path| path.is_file())
}

fn python_worker() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/runtimes/worker_common.py")
}

#[tokio::test]
async fn hello_and_transcribe_fake() {
    let bin = helper_bin();
    if !bin.exists() {
        eprintln!("skip: helper not built at {}", bin.display());
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let wav = dir.path().join("t.wav");
    let status = std::process::Command::new("ffmpeg")
        .args([
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:duration=0.3",
            "-ar",
            "16000",
            "-ac",
            "1",
            "-y",
        ])
        .arg(&wav)
        .status();
    if !status.map(|s| s.success()).unwrap_or(false) {
        eprintln!("skip: ffmpeg failed");
        return;
    }

    let mut client = WorkerClient::spawn(&bin, "fake").await.unwrap();
    assert!(client.descriptor().unwrap().supports_full_pipeline());
    client.load_model(None).await.unwrap();
    let (tx, mut rx) = mpsc::channel(32);
    let opts = AsrOptions {
        word_timestamps: true,
        language: Some("en".into()),
        ..Default::default()
    };
    let result = client.transcribe(&wav, &opts, tx, None).await.unwrap();
    assert!(!result.words.is_empty());
    let mut events = 0;
    while rx.try_recv().is_ok() {
        events += 1;
    }
    assert!(events > 0);
    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn python_fake_worker_supports_heartbeat_and_control_cancel() {
    let Some(python) = python_bin() else {
        eprintln!("skip: managed Python runtime not installed");
        return;
    };
    let script = python_worker();
    if !script.is_file() {
        eprintln!("skip: Python worker missing at {}", script.display());
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let wav = dir.path().join("python-worker.wav");
    let status = std::process::Command::new("ffmpeg")
        .args([
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:duration=0.3",
            "-ar",
            "16000",
            "-ac",
            "1",
            "-y",
        ])
        .arg(&wav)
        .status();
    if !status.map(|value| value.success()).unwrap_or(false) {
        eprintln!("skip: ffmpeg failed");
        return;
    }

    let mut client = WorkerClient::spawn_python(&python, &script, "fake")
        .await
        .unwrap();
    client.load_model(None).await.unwrap();
    let control = client.control();
    let (sink, _events) = mpsc::channel(32);
    let opts = AsrOptions {
        word_timestamps: true,
        ..Default::default()
    };
    let task =
        tokio::spawn(async move { client.transcribe(&wav, &opts, sink, Some(1000)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    control.ping().await.unwrap();
    control.cancel_current().await.unwrap();
    let error = task.await.unwrap().unwrap_err();
    assert_eq!(error.code, ErrorCode::Cancelled);
}

#[tokio::test]
async fn python_worker_rejects_dirty_partial_and_oversized_stdout() {
    let Some(python) = python_bin() else {
        eprintln!("skip: managed Python runtime not installed");
        return;
    };
    let cases = [
        ("print('dirty', flush=True)", ErrorCode::WorkerProtocolError),
        (
            "import sys; sys.stdout.write('{\\\"broken\\\"'); sys.stdout.flush()",
            ErrorCode::WorkerProtocolError,
        ),
        (
            "print('x' * (4 * 1024 * 1024 + 1), flush=True)",
            ErrorCode::WorkerProtocolError,
        ),
    ];
    for (index, (source, expected)) in cases.into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join(format!("bad-{index}.py"));
        fs::write(&script, source).unwrap();
        let error = match WorkerClient::spawn_python(&python, &script, "fake").await {
            Ok(mut client) => {
                let _ = client.kill_tree().await;
                panic!("bad worker unexpectedly completed hello")
            }
            Err(error) => error,
        };
        assert_eq!(error.code, expected);
    }
}
