//! Fake LLM provider with fault-injection matrix.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_llm::{ChatRequest, ChatResponse, LlmProvider, ProviderCapabilities};

/// Fault modes matching the M3 gate matrix.
#[derive(Debug, Clone)]
pub enum FakeLlmMode {
    Success {
        content: String,
    },
    Auth401,
    Forbidden403,
    RateLimited429,
    Server500,
    Timeout,
    BadJson,
    MissingKeys,
    /// Return scripted responses in order, then last forever.
    Script(Vec<String>),
}

pub struct FakeLlmProvider {
    id: String,
    model: String,
    mode: Mutex<FakeLlmMode>,
    caps: ProviderCapabilities,
    calls: AtomicU32,
}

impl FakeLlmProvider {
    pub fn new(mode: FakeLlmMode) -> Self {
        Self {
            id: "fake".into(),
            model: "fake-model".into(),
            mode: Mutex::new(mode),
            caps: ProviderCapabilities {
                json_mode: true,
                json_schema: false,
                tools: false,
                seed: false,
                vision: false,
                max_context_tokens: Some(8192),
                manual_override: false,
            },
            calls: AtomicU32::new(0),
        }
    }

    pub fn with_capabilities(mut self, caps: ProviderCapabilities) -> Self {
        self.caps = caps;
        self
    }

    pub fn call_count(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    pub fn set_mode(&self, mode: FakeLlmMode) {
        *self.mode.lock().unwrap() = mode;
    }
}

#[async_trait]
impl LlmProvider for FakeLlmProvider {
    fn id(&self) -> &str {
        &self.id
    }

    fn model(&self) -> &str {
        &self.model
    }

    fn capabilities(&self) -> &ProviderCapabilities {
        &self.caps
    }

    async fn chat(&self, _request: &ChatRequest) -> VcResult<ChatResponse> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mode = self.mode.lock().unwrap().clone();
        match mode {
            FakeLlmMode::Success { content } => Ok(ChatResponse {
                content,
                finish_reason: Some("stop".into()),
                prompt_tokens: Some(10),
                completion_tokens: Some(20),
            }),
            FakeLlmMode::Auth401 => Err(VcError::new(ErrorCode::LlmAuthFailed, "401 unauthorized")),
            FakeLlmMode::Forbidden403 => {
                Err(VcError::new(ErrorCode::LlmAuthFailed, "403 forbidden"))
            }
            FakeLlmMode::RateLimited429 => {
                Err(VcError::new(ErrorCode::LlmRateLimited, "429 rate limited"))
            }
            FakeLlmMode::Server500 => Err(VcError::new(
                ErrorCode::LlmProviderUnavailable,
                "500 server error",
            )),
            FakeLlmMode::Timeout => Err(VcError::new(
                ErrorCode::LlmProviderUnavailable,
                "request timeout",
            )),
            FakeLlmMode::BadJson => Ok(ChatResponse {
                content: "not-json{{{".into(),
                finish_reason: Some("stop".into()),
                prompt_tokens: Some(1),
                completion_tokens: Some(1),
            }),
            FakeLlmMode::MissingKeys => Ok(ChatResponse {
                content: r#"{"unexpected":true}"#.into(),
                finish_reason: Some("stop".into()),
                prompt_tokens: Some(1),
                completion_tokens: Some(1),
            }),
            FakeLlmMode::Script(items) => {
                let n = self.calls.load(Ordering::SeqCst) as usize;
                let idx = n.saturating_sub(1).min(items.len().saturating_sub(1));
                let content = items.get(idx).cloned().unwrap_or_else(|| "{}".into());
                Ok(ChatResponse {
                    content,
                    finish_reason: Some("stop".into()),
                    prompt_tokens: Some(1),
                    completion_tokens: Some(1),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use videocaptionerr_llm::{ChatMessage, Role};

    #[tokio::test]
    async fn matrix_auth_and_rate_limit() {
        let p = FakeLlmProvider::new(FakeLlmMode::Auth401);
        let req = ChatRequest {
            model: "m".into(),
            messages: vec![ChatMessage {
                role: Role::User,
                content: "hi".into(),
            }],
            temperature: None,
            max_tokens: None,
            response_format_json: None,
        };
        assert_eq!(
            p.chat(&req).await.unwrap_err().code,
            ErrorCode::LlmAuthFailed
        );

        p.set_mode(FakeLlmMode::RateLimited429);
        assert_eq!(
            p.chat(&req).await.unwrap_err().code,
            ErrorCode::LlmRateLimited
        );
    }
}
