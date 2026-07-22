//! Durable LLM plan / prompt / batch-result persistence.
//!
//! Prompt artifacts and plans are committed before the first network call.
//! Restart loads the plan and only executes Pending/retryable batches.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use videocaptionerr_contracts::error::{ErrorCode, VcError};
use videocaptionerr_domain::JobId;

use crate::application_error::{AppResult, ApplicationError};
use crate::ports::{LlmStage, PromptSnapshot};

use super::types::{LlmPipelineRequest, LlmPlan, LlmPlanEntry};

pub const LLM_PLAN_SCHEMA_VERSION: u32 = 2;
pub const PROMPT_ARTIFACT_SCHEMA_VERSION: u32 = 1;

/// Optional durable execution context attached to an LLM stage request.
#[derive(Debug, Clone)]
pub struct LlmDurableContext {
    pub job_id: JobId,
    pub job_dir: PathBuf,
    pub input_artifact_id: Option<String>,
    pub transcript_revision: u64,
    /// When true, an existing plan is discarded and rebuilt.
    pub invalidate_plan: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptArtifactManifest {
    pub schema_version: u32,
    pub stage: LlmStage,
    pub content_hash: String,
    pub file_hash: String,
    pub provider_revision: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmBatchResultArtifact {
    pub schema_version: u32,
    pub plan_id: String,
    pub plan_hash: String,
    pub batch_index: u32,
    pub stage: LlmStage,
    pub items: std::collections::BTreeMap<u32, String>,
    pub transcript_revision: u64,
    pub input_artifact_id: Option<String>,
    pub cue_revisions: std::collections::BTreeMap<u32, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmAttemptRecord {
    pub request_id: String,
    pub work_unit_id: Option<String>,
    pub attempt: u32,
    pub request_hash: String,
    pub provider_model_revision: String,
    pub started_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub error_code: Option<String>,
    pub retry_after_ms: Option<u64>,
}

pub fn prompt_artifact_dir(job_dir: &Path, stage: LlmStage, content_hash: &str) -> PathBuf {
    job_dir
        .join("prompts")
        .join(stage.as_str())
        .join(content_hash)
}

pub fn plan_path(job_dir: &Path, stage: LlmStage) -> PathBuf {
    job_dir.join("llm").join(stage.as_str()).join("plan.json")
}

pub fn batch_result_path(job_dir: &Path, stage: LlmStage, batch_index: u32) -> PathBuf {
    job_dir
        .join("llm")
        .join(stage.as_str())
        .join(format!("batch-{batch_index:04}.json"))
}

pub fn attempts_log_path(job_dir: &Path, stage: LlmStage) -> PathBuf {
    job_dir
        .join("llm")
        .join(stage.as_str())
        .join("attempts.ndjson")
}

/// Materialize PromptSnapshot under job/prompts/<stage>/<hash>/ before any
/// provider call. Subsequent reads use only this artifact.
pub fn materialize_prompt_artifact(
    ctx: &LlmDurableContext,
    request: &LlmPipelineRequest,
) -> AppResult<PromptArtifactManifest> {
    let dir = prompt_artifact_dir(&ctx.job_dir, request.stage, &request.prompt.content_hash);
    fs::create_dir_all(&dir).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("create prompt artifact dir: {e}"),
        ))
    })?;
    let snapshot_path = dir.join("prompt.json");
    let body = serde_json::to_vec_pretty(&request.prompt).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("encode prompt snapshot: {e}"),
        ))
    })?;
    let file_hash = format!("blake3:{}", blake3::hash(&body).to_hex());
    // Atomic publish via .partial.
    let partial = dir.join("prompt.json.partial");
    fs::write(&partial, &body).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("write prompt partial: {e}"),
        ))
    })?;
    fs::rename(&partial, &snapshot_path).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("publish prompt artifact: {e}"),
        ))
    })?;
    let manifest = PromptArtifactManifest {
        schema_version: PROMPT_ARTIFACT_SCHEMA_VERSION,
        stage: request.stage,
        content_hash: request.prompt.content_hash.clone(),
        file_hash,
        provider_revision: request.provider_profile_revision.clone(),
    };
    let man_path = dir.join("manifest.json");
    let man_body = serde_json::to_vec_pretty(&manifest).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("encode prompt manifest: {e}"),
        ))
    })?;
    fs::write(man_path, man_body).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("write prompt manifest: {e}"),
        ))
    })?;
    Ok(manifest)
}

pub fn load_prompt_artifact(
    job_dir: &Path,
    stage: LlmStage,
    content_hash: &str,
) -> AppResult<PromptSnapshot> {
    let path = prompt_artifact_dir(job_dir, stage, content_hash).join("prompt.json");
    let body = fs::read(&path).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCorrupt,
            format!("load prompt artifact {}: {e}", path.display()),
        ))
    })?;
    serde_json::from_slice(&body).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCorrupt,
            format!("decode prompt artifact: {e}"),
        ))
    })
}

pub fn persist_plan(ctx: &LlmDurableContext, plan: &LlmPlan) -> AppResult<()> {
    let path = plan_path(&ctx.job_dir, plan.stage);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("create plan dir: {e}"),
            ))
        })?;
    }
    let body = serde_json::to_vec_pretty(plan).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("encode plan: {e}"),
        ))
    })?;
    let partial = path.with_extension("json.partial");
    fs::write(&partial, &body).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("write plan partial: {e}"),
        ))
    })?;
    fs::rename(&partial, &path).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("publish plan: {e}"),
        ))
    })?;
    Ok(())
}

pub fn load_plan(job_dir: &Path, stage: LlmStage) -> AppResult<Option<LlmPlan>> {
    let path = plan_path(job_dir, stage);
    if !path.is_file() {
        return Ok(None);
    }
    let body = fs::read(&path).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCorrupt,
            format!("load plan {}: {e}", path.display()),
        ))
    })?;
    let plan: LlmPlan = serde_json::from_slice(&body).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCorrupt,
            format!("decode plan: {e}"),
        ))
    })?;
    Ok(Some(plan))
}

pub fn persist_batch_result(
    ctx: &LlmDurableContext,
    result: &LlmBatchResultArtifact,
) -> AppResult<()> {
    let path = batch_result_path(&ctx.job_dir, result.stage, result.batch_index);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("create batch result dir: {e}"),
            ))
        })?;
    }
    let body = serde_json::to_vec_pretty(result).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("encode batch result: {e}"),
        ))
    })?;
    let partial = path.with_extension("json.partial");
    fs::write(&partial, &body).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("write batch result partial: {e}"),
        ))
    })?;
    fs::rename(&partial, &path).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("publish batch result: {e}"),
        ))
    })?;
    Ok(())
}

pub fn load_batch_result(
    job_dir: &Path,
    stage: LlmStage,
    batch_index: u32,
) -> AppResult<Option<LlmBatchResultArtifact>> {
    let path = batch_result_path(job_dir, stage, batch_index);
    if !path.is_file() {
        return Ok(None);
    }
    let body = fs::read(&path).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCorrupt,
            format!("load batch result {}: {e}", path.display()),
        ))
    })?;
    let result = serde_json::from_slice(&body).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCorrupt,
            format!("decode batch result: {e}"),
        ))
    })?;
    Ok(Some(result))
}

pub fn append_attempt(
    ctx: &LlmDurableContext,
    stage: LlmStage,
    record: &LlmAttemptRecord,
) -> AppResult<()> {
    // Never write API keys or full response bodies.
    let path = attempts_log_path(&ctx.job_dir, stage);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).ok();
    }
    let line = serde_json::to_string(record).map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("encode attempt: {e}"),
        ))
    })?;
    use std::io::Write;
    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("open attempts log: {e}"),
            ))
        })?;
    writeln!(f, "{line}").map_err(|e| {
        ApplicationError::Adapter(VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("write attempt: {e}"),
        ))
    })?;
    Ok(())
}

pub fn plan_hash(plan: &LlmPlan) -> String {
    let body = serde_json::json!({
        "stage": plan.stage,
        "model": plan.model,
        "provider_profile_revision": plan.provider_profile_revision,
        "prompt_bundle_hash": plan.prompt_bundle_hash,
        "prompt_artifact_hash": plan.prompt_artifact_hash,
        "input_artifact_id": plan.input_artifact_id,
        "transcript_revision": plan.transcript_revision,
        "target_language": plan.target_language,
        "entries": plan.entries.iter().map(|e| {
            serde_json::json!({
                "batch_index": e.batch_index,
                "output_cue_ids": e.output_cue_ids,
                "context_cue_ids": e.context_cue_ids,
            })
        }).collect::<Vec<_>>(),
    });
    format!(
        "blake3:{}",
        blake3::hash(body.to_string().as_bytes()).to_hex()
    )
}

pub fn work_unit_input_hash(
    plan: &LlmPlan,
    entry: &LlmPlanEntry,
    cue_revisions: &[(u32, u64)],
) -> String {
    let body = serde_json::json!({
        "plan_hash": plan.plan_hash,
        "batch_index": entry.batch_index,
        "output_cue_ids": entry.output_cue_ids,
        "context_cue_ids": entry.context_cue_ids,
        "prompt": plan.prompt_bundle_hash,
        "provider": plan.provider_profile_revision,
        "model": plan.model,
        "cue_revisions": cue_revisions,
        "transcript_revision": plan.transcript_revision,
    });
    format!(
        "blake3:{}",
        blake3::hash(body.to_string().as_bytes()).to_hex()
    )
}

/// Classify provider/transport errors for retry policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportRetryClass {
    FailFast,
    RateLimited,
    Transient,
    Cancelled,
}

pub fn classify_transport_error(code: ErrorCode) -> TransportRetryClass {
    match code {
        ErrorCode::LlmAuthFailed | ErrorCode::LlmModelNotFound => TransportRetryClass::FailFast,
        ErrorCode::LlmRateLimited => TransportRetryClass::RateLimited,
        ErrorCode::Cancelled => TransportRetryClass::Cancelled,
        ErrorCode::LlmProviderUnavailable | ErrorCode::WorkerTimeout | ErrorCode::Internal => {
            TransportRetryClass::Transient
        }
        _ => TransportRetryClass::Transient,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::LlmStage;
    use std::collections::BTreeMap;
    use ulid::Ulid;

    #[test]
    fn prompt_materialize_and_reload() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = LlmDurableContext {
            job_id: Ulid::new().into(),
            job_dir: dir.path().to_path_buf(),
            input_artifact_id: None,
            transcript_revision: 1,
            invalidate_plan: false,
        };
        let request = LlmPipelineRequest {
            stage: LlmStage::Split,
            model: "m".into(),
            provider_profile_revision: "p:1".into(),
            prompt: PromptSnapshot {
                schema_version: 1,
                stage: LlmStage::Split,
                files: BTreeMap::from([("system.txt".into(), "hello".into())]),
                content_hash: "hash-hello".into(),
            },
            max_context_tokens: None,
            max_output_tokens: None,
            chars_per_token: 4.0,
            structured_output: crate::ports::StructuredOutput::JsonObject,
            seed: None,
            target_language: None,
            durable: Some(ctx.clone()),
            cancel: None,
        };
        let man = materialize_prompt_artifact(&ctx, &request).unwrap();
        assert_eq!(man.content_hash, "hash-hello");
        let loaded = load_prompt_artifact(dir.path(), LlmStage::Split, "hash-hello").unwrap();
        assert_eq!(loaded.content_hash, "hash-hello");
        // Mutating the original editable path is irrelevant — artifact is frozen.
        assert!(loaded.files.get("system.txt").unwrap() == "hello");
    }
}
