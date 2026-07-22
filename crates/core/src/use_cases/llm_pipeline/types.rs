//! Shared LLM pipeline types and constants.
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use videocaptionerr_domain::Transcript;

use crate::ports::{
    AsrCancelToken, IdGenerator, LlmGateway, LlmRequestRecorder, LlmStage, PromptSnapshot,
    StructuredOutput,
};

pub(crate) const DEFAULT_CONTEXT_TOKENS: u32 = 8_192;
pub(crate) const DEFAULT_OUTPUT_TOKENS: u32 = 2_048;
#[derive(Debug, Clone)]
pub struct LlmPipelineRequest {
    pub stage: LlmStage,
    pub model: String,
    pub provider_profile_revision: String,
    pub prompt: PromptSnapshot,
    pub max_context_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub chars_per_token: f64,
    pub structured_output: StructuredOutput,
    pub seed: Option<i64>,
    pub target_language: Option<String>,
    /// When set, plans/prompts/batch results are durable under the Job directory.
    pub durable: Option<super::durable::LlmDurableContext>,
    /// The same Job cancellation token used by ASR. LLM checks it before each
    /// wave and retry so cancellation cannot advance to a later stage.
    pub cancel: Option<AsrCancelToken>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmPlan {
    pub schema_version: u32,
    #[serde(default)]
    pub plan_id: String,
    #[serde(default)]
    pub job_id: Option<String>,
    pub stage: LlmStage,
    #[serde(default)]
    pub input_artifact_id: Option<String>,
    #[serde(default)]
    pub transcript_revision: u64,
    pub model: String,
    pub provider_profile_revision: String,
    pub prompt_bundle_hash: String,
    #[serde(default)]
    pub prompt_artifact_hash: String,
    #[serde(default)]
    pub effective_capability: String,
    #[serde(default)]
    pub max_context_tokens: Option<u32>,
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
    #[serde(default)]
    pub target_language: Option<String>,
    pub entries: Vec<LlmPlanEntry>,
    #[serde(default)]
    pub plan_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmPlanEntry {
    pub batch_index: u32,
    pub output_cue_ids: Vec<u32>,
    pub context_cue_ids: Vec<u32>,
    pub estimated_input_tokens: u32,
    pub reserved_output_tokens: u32,
    #[serde(default)]
    pub expected_text_revisions: std::collections::BTreeMap<u32, u64>,
    #[serde(default)]
    pub expected_translation_revisions: std::collections::BTreeMap<u32, u64>,
}

#[derive(Debug, Clone)]
pub struct LlmPipelineResult {
    pub transcript: Transcript,
    pub plan: LlmPlan,
    pub degraded_cue_ids: Vec<u32>,
}

#[derive(Debug, Clone)]
pub(crate) struct BatchInput {
    pub(crate) index: u32,
    pub(crate) output: Vec<CueInput>,
    pub(crate) context: Vec<CueInput>,
}

#[derive(Debug, Clone)]
pub(crate) struct CueInput {
    pub(crate) id: u32,
    pub(crate) text: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RequestItem<'a> {
    pub(crate) id: u32,
    pub(crate) text: &'a str,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ResponseItems {
    pub(crate) items: Vec<ResponseItem>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ResponseItem {
    pub(crate) id: u32,
    pub(crate) text: String,
}

pub struct LlmPipeline {
    pub(crate) gateway: Arc<dyn LlmGateway>,
    pub(crate) recorder: Arc<dyn LlmRequestRecorder>,
    pub(crate) ids: Arc<dyn IdGenerator>,
    /// Optional durable WorkUnit control plane (llm_batch units).
    pub(crate) work_units: Option<Arc<dyn crate::ports::WorkUnitRepository>>,
    /// Optional atomic stage/work-unit commits for batch results.
    pub(crate) stage_commits: Option<Arc<dyn crate::ports::StageCommitRepository>>,
}
