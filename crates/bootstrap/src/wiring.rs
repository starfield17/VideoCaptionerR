use std::path::{Path, PathBuf};
use std::sync::Arc;

use ulid::Ulid;
use videocaptionerr_core::ports::{Clock, IdGenerator, StructuredOutput};
use videocaptionerr_core::use_cases::LlmPipeline;
use videocaptionerr_domain::UlidStr;
use videocaptionerr_llm::application::ProviderLlmGateway;
use videocaptionerr_llm::circuit::{CircuitBreaker, CircuitLlmProvider};
use videocaptionerr_llm::openai::{OpenAiConfig, OpenAiProvider};
use videocaptionerr_llm::prompt::{PromptBundle, PromptStage};
use videocaptionerr_llm::provider::{LlmProvider, ProviderCapabilities, StructuredMode};
use videocaptionerr_llm::templates::ProviderTemplate;
use videocaptionerr_platform::{AppConfig, FileLlmRequestRecorder};

use crate::capability::{apply_capability_override, prompt_snapshot};
use crate::config::LlmProcessDefaults;

pub(crate) use crate::capability::load_cached_capabilities;

pub(crate) struct UlidGenerator;

impl IdGenerator for UlidGenerator {
    fn next_id(&self) -> UlidStr {
        Ulid::new().into()
    }
}

pub(crate) struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0)
    }
}

pub(crate) fn default_prompt_dir() -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../prompts"))
}

pub(crate) fn build_llm_pipeline(
    config: &AppConfig,
    provider_id: Option<&str>,
    prompt_dir: &Path,
    logs_dir: &Path,
    ids: Arc<dyn IdGenerator>,
    cached_capabilities: Option<ProviderCapabilities>,
) -> videocaptionerr_contracts::error::VcResult<(
    Option<Arc<LlmPipeline>>,
    Option<LlmProcessDefaults>,
)> {
    let Some(provider_id) = provider_id.or(config.llm.default_provider.as_deref()) else {
        return Ok((None, None));
    };
    let provider_config = config.llm.providers.get(provider_id).ok_or_else(|| {
        videocaptionerr_contracts::error::VcError::new(
            videocaptionerr_contracts::error::ErrorCode::InvalidConfig,
            format!("default LLM provider profile '{provider_id}' is missing"),
        )
    })?;
    let template = provider_config
        .template
        .as_deref()
        .and_then(ProviderTemplate::parse)
        .unwrap_or(ProviderTemplate::Generic);
    let mut openai_config = OpenAiConfig::new(
        provider_id,
        &provider_config.base_url,
        &provider_config.api_key,
        &provider_config.model,
    );
    let capabilities = cached_capabilities.unwrap_or_else(|| template.default_capabilities());
    openai_config.capabilities =
        apply_capability_override(capabilities, provider_config.capability_override.as_ref());
    let provider = Arc::new(OpenAiProvider::new(openai_config)?);
    let provider: Arc<dyn LlmProvider> = Arc::new(CircuitLlmProvider::new(
        provider,
        Arc::new(CircuitBreaker::new(provider_id)),
    ));
    let capabilities = provider.capabilities().clone();
    let gateway = Arc::new(ProviderLlmGateway::new(provider));
    let recorder = Arc::new(FileLlmRequestRecorder::new(
        logs_dir.join("llm-requests.ndjson"),
    ));
    // WorkUnit/StageCommit wiring is attached in ApplicationRuntime::open so the
    // pipeline can create llm_batch units after the store handle exists.
    let pipeline = Arc::new(LlmPipeline::new(gateway, recorder, ids));

    let split_prompt = prompt_snapshot(PromptBundle::load(prompt_dir, PromptStage::Split)?);
    let correct_prompt = prompt_snapshot(PromptBundle::load(prompt_dir, PromptStage::Correct)?);
    let translate_prompt = prompt_snapshot(PromptBundle::load(prompt_dir, PromptStage::Translate)?);
    let structured_output = match capabilities.effective_structured_mode() {
        StructuredMode::JsonSchema => StructuredOutput::JsonSchema,
        StructuredMode::JsonObject => StructuredOutput::JsonObject,
        StructuredMode::PromptOnly => StructuredOutput::PromptOnly,
    };
    let defaults = LlmProcessDefaults {
        model: provider_config.model.clone(),
        provider_profile_revision: format!("{provider_id}:{}", provider_config.profile_revision),
        split_prompt,
        correct_prompt,
        translate_prompt,
        max_context_tokens: capabilities.max_context_tokens,
        max_output_tokens: capabilities.max_output_tokens,
        chars_per_token: videocaptionerr_core::DEFAULT_CHARS_PER_TOKEN,
        structured_output,
        seed: None,
    };
    Ok((Some(pipeline), Some(defaults)))
}
