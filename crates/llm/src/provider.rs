//! LLM provider trait (OpenAI-compatible surface).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use videocaptionerr_contracts::error::VcResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    System,
    User,
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: Role,
    pub content: String,
}

impl ChatMessage {
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: Role::System,
            content: content.into(),
        }
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: Role::User,
            content: content.into(),
        }
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: Role::Assistant,
            content: content.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    /// When set, providers that support it should return structured JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_format_json: Option<bool>,
    /// Optional seed for providers that support it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<i64>,
    /// Requested structured-output mode. Providers may downgrade this after probing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_mode: Option<StructuredMode>,
    /// JSON Schema used when `structured_mode` is `JsonSchema`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_schema: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatResponse {
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<u32>,
}

/// How structured JSON is requested from the provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StructuredMode {
    JsonSchema,
    JsonObject,
    #[default]
    PromptOnly,
}

/// Detected / overridden provider capabilities.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProviderCapabilities {
    pub json_mode: bool,
    pub json_schema: bool,
    pub tools: bool,
    pub seed: bool,
    pub vision: bool,
    #[serde(default)]
    pub returns_usage: bool,
    #[serde(default)]
    pub supports_model_list: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_context_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    /// Manual override has priority over automatic detection.
    #[serde(default)]
    pub manual_override: bool,
    /// Effective structured-output mode (derived or overridden).
    #[serde(default)]
    pub structured_mode: StructuredMode,
}

impl ProviderCapabilities {
    /// Conservative default when nothing is known.
    pub fn conservative_default() -> Self {
        Self {
            json_mode: false,
            json_schema: false,
            tools: false,
            seed: false,
            vision: false,
            returns_usage: false,
            supports_model_list: false,
            max_context_tokens: Some(8192),
            max_output_tokens: Some(2048),
            manual_override: false,
            structured_mode: StructuredMode::PromptOnly,
        }
    }

    pub fn effective_structured_mode(&self) -> StructuredMode {
        if self.manual_override {
            return self.structured_mode;
        }
        if self.json_schema {
            StructuredMode::JsonSchema
        } else if self.json_mode {
            StructuredMode::JsonObject
        } else {
            StructuredMode::PromptOnly
        }
    }
}

/// Version of the capability probe algorithm (invalidates caches when bumped).
pub const CAPABILITY_PROBE_VERSION: u32 = 1;

#[async_trait]
pub trait LlmProvider: Send + Sync {
    fn id(&self) -> &str;
    fn model(&self) -> &str;
    fn capabilities(&self) -> &ProviderCapabilities;

    async fn chat(&self, request: &ChatRequest) -> VcResult<ChatResponse>;
}
