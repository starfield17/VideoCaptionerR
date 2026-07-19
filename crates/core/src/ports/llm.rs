use async_trait::async_trait;

use crate::application_error::AppResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LlmRole {
    System,
    User,
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmMessage {
    pub role: LlmRole,
    pub content: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub max_context_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
}

#[async_trait]
pub trait LlmGateway: Send + Sync {
    async fn chat(&self, request: LlmRequest) -> AppResult<LlmResponse>;
    async fn capabilities(&self) -> AppResult<LlmCapabilities>;
}
