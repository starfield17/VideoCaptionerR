//! OpenAI-compatible chat provider.
//!
//! The client deliberately exposes only the fields needed by the application.
//! Provider-specific response bodies are parsed at this boundary so the rest
//! of the pipeline never depends on an HTTP schema.

use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use reqwest::header::{HeaderMap, RETRY_AFTER};
use serde_json::{json, Map, Value};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};

use crate::provider::{
    ChatMessage, ChatRequest, ChatResponse, LlmProvider, ProviderCapabilities, Role,
    StructuredMode,
};

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone)]
pub struct OpenAiConfig {
    pub id: String,
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub capabilities: ProviderCapabilities,
    pub timeout: Duration,
}

impl OpenAiConfig {
    pub fn new(
        id: impl Into<String>,
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            base_url: base_url.into(),
            api_key: api_key.into(),
            model: model.into(),
            capabilities: ProviderCapabilities::conservative_default(),
            timeout: DEFAULT_TIMEOUT,
        }
    }
}

/// An OpenAI-compatible provider. The API key is retained only in memory.
pub struct OpenAiProvider {
    id: String,
    base_url: String,
    api_key: String,
    model: String,
    capabilities: ProviderCapabilities,
    client: reqwest::Client,
}

impl OpenAiProvider {
    pub fn new(config: OpenAiConfig) -> VcResult<Self> {
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|e| {
                VcError::new(
                    ErrorCode::LlmProviderUnavailable,
                    format!("build HTTP client: {e}"),
                )
            })?;
        Ok(Self {
            id: config.id,
            base_url: trim_base_url(&config.base_url),
            api_key: config.api_key,
            model: config.model,
            capabilities: config.capabilities,
            client,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn chat_endpoint(&self) -> String {
        endpoint(&self.base_url, "chat/completions")
    }

    pub fn models_endpoint(&self) -> String {
        endpoint(&self.base_url, "models")
    }

    pub(crate) fn client(&self) -> &reqwest::Client {
        &self.client
    }

    pub(crate) fn api_key(&self) -> &str {
        &self.api_key
    }

    pub(crate) fn configured_model(&self) -> &str {
        &self.model
    }

    pub(crate) async fn post_chat_request(
        &self,
        request: &ChatRequest,
    ) -> VcResult<ChatResponse> {
        let body = request_body(request, self.capabilities.effective_structured_mode());
        let mut builder = self
            .client
            .post(self.chat_endpoint())
            .header(reqwest::header::CONTENT_TYPE, "application/json");
        if !self.api_key.is_empty() {
            builder = builder.bearer_auth(&self.api_key);
        }

        let response = builder.json(&body).send().await.map_err(|e| {
            if e.is_timeout() {
                VcError::new(ErrorCode::LlmProviderUnavailable, "LLM request timed out")
            } else {
                VcError::new(
                    ErrorCode::LlmProviderUnavailable,
                    "LLM provider request failed",
                )
            }
        })?;

        let retry_after_ms = retry_after_ms(response.headers());
        if !response.status().is_success() {
            return Err(http_status_error(response.status().as_u16(), retry_after_ms));
        }

        let body: Value = response.json().await.map_err(|_| {
            VcError::new(
                ErrorCode::LlmInvalidResponse,
                "LLM provider returned invalid JSON",
            )
        })?;
        parse_chat_response(&body)
    }
}

#[async_trait]
impl LlmProvider for OpenAiProvider {
    fn id(&self) -> &str {
        &self.id
    }

    fn model(&self) -> &str {
        &self.model
    }

    fn capabilities(&self) -> &ProviderCapabilities {
        &self.capabilities
    }

    async fn chat(&self, request: &ChatRequest) -> VcResult<ChatResponse> {
        self.post_chat_request(request).await
    }
}

pub(crate) fn request_body(request: &ChatRequest, effective_mode: StructuredMode) -> Value {
    let mut body = Map::new();
    body.insert(
        "model".into(),
        Value::String(request.model.clone()),
    );
    body.insert(
        "messages".into(),
        Value::Array(
            request
                .messages
                .iter()
                .map(message_value)
                .collect::<Vec<_>>(),
        ),
    );
    if let Some(value) = request.temperature {
        body.insert("temperature".into(), json!(value));
    }
    if let Some(value) = request.max_tokens {
        body.insert("max_tokens".into(), json!(value));
    }
    if let Some(value) = request.seed {
        body.insert("seed".into(), json!(value));
    }

    let mode = request.structured_mode.unwrap_or(effective_mode);
    match mode {
        StructuredMode::JsonSchema => {
            if let Some(schema) = &request.response_schema {
                body.insert(
                    "response_format".into(),
                    json!({
                        "type": "json_schema",
                        "json_schema": {
                            "name": "videocaptionerr_response",
                            "strict": true,
                            "schema": schema,
                        }
                    }),
                );
            } else {
                body.insert("response_format".into(), json!({"type": "json_object"}));
            }
        }
        StructuredMode::JsonObject => {
            body.insert("response_format".into(), json!({"type": "json_object"}));
        }
        StructuredMode::PromptOnly => {
            if request.response_format_json == Some(true) {
                body.insert("response_format".into(), json!({"type": "json_object"}));
            }
        }
    }
    Value::Object(body)
}

fn message_value(message: &ChatMessage) -> Value {
    json!({
        "role": match message.role {
            Role::System => "system",
            Role::User => "user",
            Role::Assistant => "assistant",
        },
        "content": message.content,
    })
}

fn parse_chat_response(body: &Value) -> VcResult<ChatResponse> {
    let choice = body
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .ok_or_else(|| {
            VcError::new(
                ErrorCode::LlmInvalidResponse,
                "LLM provider response contained no choices",
            )
        })?;
    let message = choice.get("message").ok_or_else(|| {
        VcError::new(
            ErrorCode::LlmInvalidResponse,
            "LLM provider response contained no message",
        )
    })?;
    let content = content_string(message.get("content"))?;
    let usage = body.get("usage");
    Ok(ChatResponse {
        content,
        finish_reason: choice
            .get("finish_reason")
            .and_then(Value::as_str)
            .map(str::to_owned),
        prompt_tokens: usage.and_then(|v| v.get("prompt_tokens")).and_then(as_u32),
        completion_tokens: usage
            .and_then(|v| v.get("completion_tokens"))
            .and_then(as_u32),
    })
}

fn content_string(value: Option<&Value>) -> VcResult<String> {
    let Some(value) = value else {
        return Err(VcError::new(
            ErrorCode::LlmInvalidResponse,
            "LLM provider response contained no content",
        ));
    };
    if let Some(text) = value.as_str() {
        return Ok(text.to_owned());
    }
    if let Some(parts) = value.as_array() {
        let mut out = String::new();
        for part in parts {
            if let Some(text) = part.get("text").and_then(Value::as_str) {
                out.push_str(text);
            }
        }
        if !out.is_empty() {
            return Ok(out);
        }
    }
    Err(VcError::new(
        ErrorCode::LlmInvalidResponse,
        "LLM provider returned unsupported message content",
    ))
}

fn as_u32(value: &Value) -> Option<u32> {
    value
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

pub(crate) fn trim_base_url(base_url: &str) -> String {
    base_url.trim().trim_end_matches('/').to_owned()
}

pub(crate) fn endpoint(base_url: &str, suffix: &str) -> String {
    let base = trim_base_url(base_url);
    if base.ends_with(suffix) {
        base
    } else {
        format!("{base}/{suffix}")
    }
}

pub(crate) fn retry_after_ms(headers: &HeaderMap) -> Option<u64> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(seconds.saturating_mul(1000));
    }
    let date = chrono::DateTime::parse_from_rfc2822(value).ok()?;
    let now = Utc::now().fixed_offset();
    let millis = (date - now).num_milliseconds();
    Some(u64::try_from(millis.max(0)).unwrap_or(0))
}

pub(crate) fn http_status_error(status: u16, retry_after_ms: Option<u64>) -> VcError {
    let code = match status {
        401 | 403 => ErrorCode::LlmAuthFailed,
        404 => ErrorCode::LlmModelNotFound,
        408 | 429 => ErrorCode::LlmRateLimited,
        500..=599 => ErrorCode::LlmProviderUnavailable,
        _ => ErrorCode::LlmProviderUnavailable,
    };
    let mut error = VcError::new(code, format!("LLM provider returned HTTP {status}"));
    if let Some(retry_after_ms) = retry_after_ms {
        error = error.with_retry_after_ms(retry_after_ms);
    }
    error
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_host_and_v1_base_urls() {
        assert_eq!(endpoint("https://example.test", "chat/completions"), "https://example.test/chat/completions");
        assert_eq!(endpoint("https://example.test/v1/", "chat/completions"), "https://example.test/v1/chat/completions");
    }

    #[test]
    fn request_body_never_contains_api_key() {
        let request = ChatRequest {
            model: "model".into(),
            messages: vec![ChatMessage::user("hello")],
            temperature: None,
            max_tokens: None,
            response_format_json: None,
            seed: None,
            structured_mode: Some(StructuredMode::JsonObject),
            response_schema: None,
        };
        let body = request_body(&request, StructuredMode::JsonObject).to_string();
        assert!(!body.contains("sk-secret"));
        assert!(body.contains("json_object"));
    }

    #[test]
    fn retry_after_delta_is_parsed() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, "3".parse().unwrap());
        assert_eq!(retry_after_ms(&headers), Some(3000));
    }
}
