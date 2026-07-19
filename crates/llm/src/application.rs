//! Adapter from the provider-owned HTTP abstraction to application ports.

use std::sync::Arc;

use async_trait::async_trait;
use videocaptionerr_core::application_error::{AppResult, ApplicationError};
use videocaptionerr_core::ports::{
    LlmCapabilities as ApplicationCapabilities, LlmGateway, LlmMessage as ApplicationMessage,
    LlmRequest as ApplicationRequest, LlmResponse as ApplicationResponse, LlmRole,
    StructuredOutput,
};

use crate::provider::{ChatMessage, ChatRequest, LlmProvider, StructuredMode};

pub struct ProviderLlmGateway {
    provider: Arc<dyn LlmProvider>,
}

impl ProviderLlmGateway {
    pub fn new(provider: Arc<dyn LlmProvider>) -> Self {
        Self { provider }
    }

    pub fn provider(&self) -> &Arc<dyn LlmProvider> {
        &self.provider
    }
}

#[async_trait]
impl LlmGateway for ProviderLlmGateway {
    async fn chat(&self, request: ApplicationRequest) -> AppResult<ApplicationResponse> {
        let provider_request = ChatRequest {
            model: request.model,
            messages: request.messages.into_iter().map(map_message).collect(),
            temperature: request.temperature.map(f64::from),
            max_tokens: request.max_output_tokens,
            response_format_json: match request.structured_output {
                StructuredOutput::PromptOnly => None,
                StructuredOutput::JsonSchema | StructuredOutput::JsonObject => Some(true),
            },
            seed: request.seed,
            structured_mode: Some(map_structured_mode(request.structured_output)),
            response_schema: request.schema,
        };
        let response = self
            .provider
            .chat(&provider_request)
            .await
            .map_err(ApplicationError::Adapter)?;
        Ok(ApplicationResponse {
            content: response.content,
            prompt_tokens: response.prompt_tokens.map(u64::from),
            completion_tokens: response.completion_tokens.map(u64::from),
        })
    }

    async fn capabilities(&self) -> AppResult<ApplicationCapabilities> {
        let capabilities = self.provider.capabilities();
        Ok(ApplicationCapabilities {
            structured_output: map_structured_output(capabilities.effective_structured_mode()),
            returns_usage: capabilities.returns_usage,
            supports_seed: capabilities.seed,
            supports_model_list: capabilities.supports_model_list,
            max_context_tokens: capabilities.max_context_tokens,
            max_output_tokens: capabilities.max_output_tokens,
        })
    }
}

fn map_message(message: ApplicationMessage) -> ChatMessage {
    match message.role {
        LlmRole::System => ChatMessage::system(message.content),
        LlmRole::User => ChatMessage::user(message.content),
        LlmRole::Assistant => ChatMessage::assistant(message.content),
    }
}

fn map_structured_mode(output: StructuredOutput) -> StructuredMode {
    match output {
        StructuredOutput::JsonSchema => StructuredMode::JsonSchema,
        StructuredOutput::JsonObject => StructuredMode::JsonObject,
        StructuredOutput::PromptOnly => StructuredMode::PromptOnly,
    }
}

fn map_structured_output(mode: StructuredMode) -> StructuredOutput {
    match mode {
        StructuredMode::JsonSchema => StructuredOutput::JsonSchema,
        StructuredMode::JsonObject => StructuredOutput::JsonObject,
        StructuredMode::PromptOnly => StructuredOutput::PromptOnly,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use videocaptionerr_contracts::error::VcResult;
    use videocaptionerr_core::ports::LlmRole;

    use super::*;
    use crate::provider::{ChatResponse, ProviderCapabilities, Role};

    struct FakeProvider {
        request: Mutex<Option<ChatRequest>>,
        capabilities: ProviderCapabilities,
    }

    #[async_trait]
    impl LlmProvider for FakeProvider {
        fn id(&self) -> &str {
            "fake"
        }

        fn model(&self) -> &str {
            "fake-model"
        }

        fn capabilities(&self) -> &ProviderCapabilities {
            &self.capabilities
        }

        async fn chat(&self, request: &ChatRequest) -> VcResult<ChatResponse> {
            *self.request.lock().unwrap() = Some(request.clone());
            Ok(ChatResponse {
                content: "{\"ok\":true}".into(),
                finish_reason: Some("stop".into()),
                prompt_tokens: Some(3),
                completion_tokens: Some(2),
            })
        }
    }

    #[tokio::test]
    async fn maps_application_requests_without_owning_pipeline_policy() {
        let provider = Arc::new(FakeProvider {
            request: Mutex::new(None),
            capabilities: ProviderCapabilities::conservative_default(),
        });
        let gateway = ProviderLlmGateway::new(provider.clone());
        let response = gateway
            .chat(ApplicationRequest {
                model: "requested-model".into(),
                messages: vec![ApplicationMessage {
                    role: LlmRole::User,
                    content: "hello".into(),
                }],
                temperature: Some(0.2),
                max_output_tokens: Some(64),
                seed: Some(7),
                structured_output: StructuredOutput::JsonObject,
                schema: None,
            })
            .await
            .unwrap();
        assert_eq!(response.prompt_tokens, Some(3));
        assert_eq!(response.completion_tokens, Some(2));

        let request = provider.request.lock().unwrap().clone().unwrap();
        assert_eq!(request.model, "requested-model");
        assert_eq!(request.messages[0].content, "hello");
        assert_eq!(request.structured_mode, Some(StructuredMode::JsonObject));
        assert_eq!(request.seed, Some(7));
        assert_eq!(request.messages[0].role, Role::User);

        let capabilities = gateway.capabilities().await.unwrap();
        assert_eq!(capabilities.structured_output, StructuredOutput::PromptOnly);
    }
}
