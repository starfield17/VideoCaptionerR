use std::path::PathBuf;

use serde::Deserialize;
use tauri::State;
use videocaptionerr_bootstrap::{
    ApplicationRuntime, CapabilityProbeView, DoctorView, JobSummary, ProcessView, RuntimeConfig,
    TranscriptEditView,
};
use videocaptionerr_contracts::{error::VcError, Transcript};

pub struct DesktopState {
    pub runtime: ApplicationRuntime,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessRequest {
    pub files: Vec<String>,
    pub target_language: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EditCueRequest {
    pub job_id: String,
    pub cue_id: u32,
    pub expected_revision: u64,
    pub field: String,
    pub value: String,
}

#[tauri::command]
async fn list_jobs(state: State<'_, DesktopState>) -> Result<Vec<JobSummary>, String> {
    state.runtime.list_job_summaries().await.map_err(error_text)
}

#[tauri::command]
fn doctor(state: State<'_, DesktopState>) -> DoctorView {
    state.runtime.doctor_view()
}

#[tauri::command]
async fn process_files(
    state: State<'_, DesktopState>,
    request: ProcessRequest,
) -> Result<ProcessView, String> {
    if request.files.is_empty() {
        return Err("INVALID_ARGUMENT: no input files".into());
    }
    let files = request.files.into_iter().map(PathBuf::from).collect();
    state
        .runtime
        .process_files(files, request.target_language)
        .await
        .map_err(error_text)
}

#[tauri::command]
async fn load_transcript(
    state: State<'_, DesktopState>,
    job_id: String,
) -> Result<Transcript, String> {
    state
        .runtime
        .load_transcript(&job_id)
        .await
        .map_err(error_text)
}

#[tauri::command]
async fn edit_cue(
    state: State<'_, DesktopState>,
    request: EditCueRequest,
) -> Result<TranscriptEditView, String> {
    state
        .runtime
        .edit_transcript_view(
            &request.job_id,
            request.cue_id,
            request.expected_revision,
            &request.field,
            request.value,
        )
        .await
        .map_err(error_text)
}

#[tauri::command]
async fn probe_provider(
    state: State<'_, DesktopState>,
    force: bool,
) -> Result<CapabilityProbeView, String> {
    state
        .runtime
        .probe_llm_capabilities_view(force)
        .await
        .map_err(error_text)
}

#[tauri::command]
async fn cancel_job(state: State<'_, DesktopState>, job_id: String) -> Result<(), String> {
    state
        .runtime
        .cancel_job(&job_id)
        .await
        .map_err(error_text)?;
    Ok(())
}

#[tauri::command]
async fn retry_job(
    state: State<'_, DesktopState>,
    job_id: String,
    from_stage: Option<String>,
    dry_run: bool,
) -> Result<String, String> {
    let _processing_lease = if dry_run {
        None
    } else {
        Some(
            state
                .runtime
                .acquire_gui_processing_lock()
                .map_err(error_text)?,
        )
    };
    let outcome = state
        .runtime
        .retry_job(&job_id, from_stage.as_deref(), dry_run)
        .await
        .map_err(error_text)?;
    Ok(if outcome.dry_run() {
        "dry_run".into()
    } else {
        "executed".into()
    })
}

#[tauri::command]
async fn pause_batch(state: State<'_, DesktopState>, batch_id: String) -> Result<(), String> {
    state
        .runtime
        .pause_batch(&batch_id)
        .await
        .map_err(error_text)
}

#[tauri::command]
async fn resume_batch(state: State<'_, DesktopState>, batch_id: String) -> Result<(), String> {
    state
        .runtime
        .resume_batch(&batch_id)
        .await
        .map_err(error_text)
}

#[tauri::command]
async fn cancel_batch(state: State<'_, DesktopState>, batch_id: String) -> Result<(), String> {
    state
        .runtime
        .cancel_batch(&batch_id)
        .await
        .map_err(error_text)?;
    Ok(())
}

pub fn run() {
    // Business runtime selection is profile-driven in both debug and release.
    // Tests/developers may opt into the deterministic adapter explicitly.
    let engine = std::env::var("VIDEOCAPTIONERR_ASR_ENGINE")
        .ok()
        .filter(|value| value == "fake");
    let runtime = ApplicationRuntime::open(RuntimeConfig {
        home: None,
        engine,
        model_path: None,
        helper_path: None,
        prompt_dir: None,
        profile: None,
    })
    .expect("open VideoCaptionerR application runtime");

    tauri::Builder::default()
        .manage(DesktopState { runtime })
        .invoke_handler(tauri::generate_handler![
            list_jobs,
            doctor,
            process_files,
            load_transcript,
            edit_cue,
            probe_provider,
            cancel_job,
            retry_job,
            pause_batch,
            resume_batch,
            cancel_batch
        ])
        .run(tauri::generate_context!())
        .expect("run VideoCaptionerR desktop application");
}

fn error_text(error: VcError) -> String {
    format!("{}: {}", error.code.as_str(), error.message)
}
