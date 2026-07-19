//! End-to-end ASR-only transcription pipeline (M2).

use std::path::{Path, PathBuf};

use tokio::sync::mpsc;
use tracing::info;
use ulid::Ulid;
use videocaptionerr_asr::{
    normalize_asr, resolve_helper_binary, AsrOptions, NormalizeOptions, WorkerClient,
};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_contracts::ids::UlidStr;
use videocaptionerr_store::artifact::atomic_write_json;
use videocaptionerr_store::paths::{sanitize_stem, AppPaths};
use videocaptionerr_store::{Store, WorkUnitStatus};

use crate::media::{
    extract_audio_wav, media_hash_file, pcm_hash_file, probe_media, select_audio_stream,
    ExtractOptions,
};
use crate::split::{rule_split, RuleSplitConfig};
use crate::subtitle::export::{write_export, ExportFormat, ExportLayout, ExportOptions};
use crate::subtitle::planner::OutputPlanner;
use crate::subtitle::preflight::{ensure_exportable, preflight_export};

fn media_hash_placeholder(path: &Path) -> String {
    // Temporary unit input hash before full media hash is computed.
    format!("path:{}", path.display())
}

#[derive(Debug, Clone)]
pub struct TranscribeRequest {
    pub input: PathBuf,
    /// Required for non-fake engines. For fake helper, optional.
    pub model_path: Option<PathBuf>,
    pub engine: String,
    pub helper_path: Option<PathBuf>,
    pub language: Option<String>,
    pub export_format: ExportFormat,
}

#[derive(Debug, Clone)]
pub struct TranscribeResult {
    pub job_id: String,
    pub job_dir: PathBuf,
    pub export_path: PathBuf,
    pub media_hash: String,
    pub pcm_hash: String,
    pub language: Option<String>,
    pub cue_count: usize,
}

/// Run probe → extract → helper ASR → normalize → rule split → SRT export.
pub async fn run_transcribe(
    paths: &AppPaths,
    store: &mut Store,
    req: &TranscribeRequest,
) -> VcResult<TranscribeResult> {
    if !req.input.exists() {
        return Err(VcError::new(
            ErrorCode::InputNotFound,
            format!("input not found: {}", req.input.display()),
        ));
    }

    // Explicit model selection for non-fake engines (no default model).
    if req.engine != "fake" && req.model_path.is_none() {
        return Err(VcError::new(
            ErrorCode::ModelNotFound,
            "no model selected; pass --model explicitly (VideoCaptionerR never auto-downloads)",
        ));
    }
    if let Some(mp) = &req.model_path {
        if !mp.is_file() {
            return Err(VcError::new(
                ErrorCode::ModelNotFound,
                format!("model file not found: {}", mp.display()),
            ));
        }
    }

    let job_id = UlidStr::from(Ulid::new()).into_string();
    let stem = req
        .input
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("media");
    let job_dir = paths.job_dir(&job_id, &sanitize_stem(stem));
    std::fs::create_dir_all(&job_dir)
        .map_err(|e| VcError::new(ErrorCode::Internal, format!("create job dir: {e}")))?;

    store.insert_job(
        &job_id,
        None,
        &req.input.to_string_lossy(),
        &job_dir.to_string_lossy(),
        "running",
    )?;
    let unit_id = UlidStr::from(Ulid::new()).into_string();
    store.insert_work_unit(
        &unit_id,
        &job_id,
        "asr",
        "full",
        0,
        &media_hash_placeholder(&req.input),
        WorkUnitStatus::Running,
    )?;

    // Probe
    let probe = probe_media(&req.input, None)?;
    if !probe.has_audio() {
        return Err(VcError::new(
            ErrorCode::AudioStreamNotFound,
            "no audio streams",
        ));
    }
    let stream = select_audio_stream(&probe)?
        .or_else(|| probe.default_stream())
        .ok_or_else(|| {
            VcError::new(ErrorCode::AudioStreamNotFound, "no selectable audio stream")
        })?;
    let stream_index = stream.stream_index;

    let probe_path = job_dir.join("00_probe.json");
    atomic_write_json(&probe_path, &probe)?;

    let media_hash = media_hash_file(&req.input)?;

    // Extract
    let extract = extract_audio_wav(
        &req.input,
        &job_dir,
        &ExtractOptions {
            stream_index,
            expected_duration_ms: Some(probe.duration_ms),
            ..Default::default()
        },
    )?;
    let pcm_hash = pcm_hash_file(&extract.wav_path)?;

    // Helper ASR
    let helper = req
        .helper_path
        .clone()
        .unwrap_or_else(resolve_helper_binary);
    let mut client = WorkerClient::spawn(&helper, &req.engine).await?;
    if !client
        .descriptor()
        .is_some_and(|d| d.supports_full_pipeline())
    {
        return Err(VcError::new(
            ErrorCode::EngineCapabilityInsufficient,
            "helper does not support full subtitle pipeline (need A2+)",
        ));
    }
    client.load_model(req.model_path.as_deref()).await?;

    let (tx, mut rx) = mpsc::channel(256);
    // Drain events in background so channel does not fill.
    let drain = tokio::spawn(async move { while let Some(_ev) = rx.recv().await {} });

    let opts = AsrOptions {
        language: req.language.clone(),
        model_path: req
            .model_path
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned()),
        word_timestamps: true,
        ..Default::default()
    };
    let raw = client
        .transcribe(&extract.wav_path, &opts, tx, None)
        .await?;
    let _ = drain.await;
    client.unload_model().await.ok();
    client.shutdown().await.ok();

    // Persist raw
    let raw_path = job_dir.join("asr.raw.json");
    atomic_write_json(&raw_path, &raw)?;

    // Normalize
    let transcript = normalize_asr(
        &raw,
        &NormalizeOptions {
            source_hash: media_hash.clone(),
            duration_ms: Some(probe.duration_ms),
            device: Some("cpu".into()),
        },
    )?;
    let asr_path = job_dir.join("01_asr.json");
    atomic_write_json(&asr_path, &transcript)?;

    // Rule split
    let split = rule_split(&transcript, &RuleSplitConfig::default())?;
    let split_path = job_dir.join("02_split.json");
    atomic_write_json(&split_path, &split)?;

    // Export
    let mut planner = OutputPlanner::default();
    let planned = planner.plan(
        &req.input,
        None,
        ExportLayout::SourceOnly,
        req.export_format,
    )?;
    if let Some(parent) = planned.path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let export_opts = ExportOptions {
        format: req.export_format,
        layout: ExportLayout::SourceOnly,
        ..Default::default()
    };
    let report = preflight_export(&split, &export_opts)?;
    ensure_exportable(&report)?;
    let report_path = job_dir.join("export-report.json");
    atomic_write_json(&report_path, &report)?;
    write_export(&planned.path, &split, &export_opts)?;

    // Commit unit done
    let meta = Store::new_artifact_meta(
        &job_id,
        "asr",
        videocaptionerr_contracts::artifact::ArtifactKind::Transcript,
        &split_path.to_string_lossy(),
        &videocaptionerr_store::blake3_file(&split_path)?,
        &format!("{}@{}", raw.engine_id, env!("CARGO_PKG_VERSION")),
    );
    store.commit_artifact_and_unit(&meta, Some(&unit_id))?;

    store.mark_job_done(
        &job_id,
        &media_hash,
        &pcm_hash,
        stream_index as i64,
        split.language.as_deref(),
    )?;

    info!(
        job_id = %job_id,
        export = %planned.path.display(),
        cues = split.cues.len(),
        "transcribe complete"
    );

    Ok(TranscribeResult {
        job_id,
        job_dir,
        export_path: planned.path,
        media_hash,
        pcm_hash,
        language: split.language,
        cue_count: split.cues.len(),
    })
}

pub fn default_helper_path() -> PathBuf {
    resolve_helper_binary()
}

/// Synchronous entry for tests that only need path existence checks.
pub fn require_input(path: &Path) -> VcResult<()> {
    if path.exists() {
        Ok(())
    } else {
        Err(VcError::new(
            ErrorCode::InputNotFound,
            format!("input not found: {}", path.display()),
        ))
    }
}
