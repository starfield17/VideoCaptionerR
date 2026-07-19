use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::application_error::AppResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmRole {
    System,
    User,
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmMessage {
    pub role: LlmRole,
    pub content: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StructuredOutput {
    JsonSchema,
    JsonObject,
    PromptOnly,
}

#[derive(Debug, Clone)]
pub struct LlmRequest {
    pub model: String,
    pub messages: Vec<LlmMessage>,
    pub temperature: Option<f32>,
    pub max_output_tokens: Option<u32>,
    pub seed: Option<i64>,
    pub structured_output: StructuredOutput,
    pub schema: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct LlmResponse {
    pub content: String,
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmCapabilities {
    pub structured_output: StructuredOutput,
    pub returns_usage: bool,
    pub supports_seed: bool,
    pub supports_model_list: bool,
    pub max_context_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmStage {
    Split,
    Correct,
    Translate,
}

impl LlmStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Split => "split",
            Self::Correct => "correct",
            Self::Translate => "translate",
        }
    }
}

/// Immutable prompt contents captured at the beginning of an LLM stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptSnapshot {
    pub schema_version: u32,
    pub stage: LlmStage,
    pub files: std::collections::BTreeMap<String, String>,
    pub content_hash: String,
}

/// Secret-free persistence record for an automatic provider capability probe.
/// The provider adapter owns the result schema; the application only owns the
/// cache identity and the opaque serialized payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityProbeRecord {
    pub id: String,
    pub provider_profile_id: String,
    pub model: String,
    pub probe_hash: String,
    pub result_json: String,
    pub created_at: String,
    pub expires_at: Option<String>,
}

impl PromptSnapshot {
    pub fn system_prompt(&self) -> String {
        self.files
            .get("system.txt")
            .cloned()
            .unwrap_or_else(|| self.files.values().cloned().collect::<Vec<_>>().join("\n"))
    }
}

/// Metadata-only LLM request record. Content is deliberately absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmRequestMetadata {
    pub request_id: String,
    pub stage: LlmStage,
    pub batch_index: u32,
    pub attempt: u32,
    pub model: String,
    pub request_hash: String,
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub error_code: Option<String>,
}

#[async_trait]
pub trait LlmRequestRecorder: Send + Sync {
    async fn record(&self, metadata: LlmRequestMetadata) -> AppResult<()>;
}

#[async_trait]
pub trait LlmGateway: Send + Sync {
    async fn chat(&self, request: LlmRequest) -> AppResult<LlmResponse>;
    async fn capabilities(&self) -> AppResult<LlmCapabilities>;
}

#[async_trait]
pub trait CapabilityProbeStore: Send + Sync {
    async fn load(
        &self,
        provider_profile_id: &str,
        model: &str,
        probe_hash: &str,
    ) -> AppResult<Option<String>>;

    async fn save(&self, record: CapabilityProbeRecord) -> AppResult<()>;
}
