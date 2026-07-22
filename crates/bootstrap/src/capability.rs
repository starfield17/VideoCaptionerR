use ulid::Ulid;
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::application_error::ApplicationError;
use videocaptionerr_core::ports::{CapabilityProbeRecord, LlmStage, PromptSnapshot};
use videocaptionerr_llm::probe::{CapabilityProbe, ProbeConfig, ProbeResult};
use videocaptionerr_llm::prompt::{PromptBundle, PromptStage};
use videocaptionerr_llm::provider::{
    ProviderCapabilities, StructuredMode, CAPABILITY_PROBE_VERSION,
};
use videocaptionerr_platform::{AppConfig, LlmCapabilityOverride, LlmProviderConfig};
use videocaptionerr_store::StoreHandle;

use crate::dto::{CapabilityProbeView, CapabilityView};
use crate::runtime::ApplicationRuntime;

impl ApplicationRuntime {
    /// Probe a provider only when an inbound adapter explicitly requests it.
    /// Cached results are used unless `force` is true; no API key enters the
    /// cache identity or the serialized result.
    pub async fn probe_llm_capabilities(
        &self,
        provider_id: Option<&str>,
        force: bool,
    ) -> VcResult<ProbeResult> {
        let config = AppConfig::load(&self.paths.config_file)?;
        let provider_id = provider_id
            .or(self.resolved_profile.llm_provider.as_deref())
            .or(config.llm.default_provider.as_deref())
            .ok_or_else(|| {
                VcError::new(
                    ErrorCode::LlmProviderUnavailable,
                    "no LLM provider profile is configured",
                )
            })?;
        let provider_config = config.llm.providers.get(provider_id).ok_or_else(|| {
            VcError::new(
                ErrorCode::InvalidConfig,
                format!("LLM provider profile '{provider_id}' is missing"),
            )
        })?;
        let probe_config = probe_config(provider_id, provider_config);
        let probe = CapabilityProbe::new(probe_config.clone());
        if !force {
            if let Some(result) = self
                .capability_probes
                .load(
                    &probe_config.provider_profile_id,
                    &probe_config.model,
                    &probe.cache_key(),
                )
                .await
                .map_err(ApplicationError::into_vc_error)?
            {
                return decode_probe_result(
                    &result,
                    &probe_config,
                    provider_config.capability_override.as_ref(),
                );
            }
        }

        let mut result = probe.run().await?;
        result.capabilities = apply_capability_override(
            result.capabilities,
            provider_config.capability_override.as_ref(),
        );
        let result_json = serde_json::to_string(&result).map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode LLM capability probe: {error}"),
            )
        })?;
        self.capability_probes
            .save(CapabilityProbeRecord {
                id: Ulid::new().to_string(),
                provider_profile_id: probe_config.provider_profile_id,
                model: probe_config.model,
                probe_hash: probe.cache_key(),
                result_json,
                created_at: chrono::Utc::now().to_rfc3339(),
                expires_at: None,
            })
            .await
            .map_err(ApplicationError::into_vc_error)?;
        Ok(result)
    }

    pub async fn probe_llm_capabilities_view(&self, force: bool) -> VcResult<CapabilityProbeView> {
        let result = self.probe_llm_capabilities(None, force).await?;
        Ok(CapabilityProbeView {
            provider_profile_id: result.provider_profile_id,
            profile_revision: result.profile_revision,
            model: result.model,
            probe_hash: result.probe_hash,
            capabilities: CapabilityView {
                structured_mode: structured_mode_label(
                    result.capabilities.effective_structured_mode(),
                )
                .into(),
                returns_usage: result.capabilities.returns_usage,
                seed: result.capabilities.seed,
                supports_model_list: result.capabilities.supports_model_list,
                max_context_tokens: result.capabilities.max_context_tokens,
                max_output_tokens: result.capabilities.max_output_tokens,
            },
            warnings: result.warnings,
        })
    }
}

pub(crate) fn probe_config(provider_id: &str, provider: &LlmProviderConfig) -> ProbeConfig {
    let mut config = ProbeConfig::new(
        provider_id,
        provider.profile_revision,
        &provider.base_url,
        &provider.api_key,
        &provider.model,
    );
    config.manual_override = Some(apply_capability_override(
        ProviderCapabilities::conservative_default(),
        provider.capability_override.as_ref(),
    ));
    if provider.capability_override.is_none() {
        config.manual_override = None;
    }
    config
}

pub(crate) fn load_cached_capabilities(
    store: &StoreHandle,
    config: &AppConfig,
    provider_id: Option<&str>,
) -> VcResult<Option<ProviderCapabilities>> {
    let Some(provider_id) = provider_id.or(config.llm.default_provider.as_deref()) else {
        return Ok(None);
    };
    let provider = config.llm.providers.get(provider_id).ok_or_else(|| {
        VcError::new(
            ErrorCode::InvalidConfig,
            format!("default LLM provider profile '{provider_id}' is missing"),
        )
    })?;
    let probe_config = probe_config(provider_id, provider);
    let probe = CapabilityProbe::new(probe_config.clone());
    let Some(result_json) = store.load_capability_probe_sync(
        &probe_config.provider_profile_id,
        &probe_config.model,
        &probe.cache_key(),
    )?
    else {
        return Ok(None);
    };
    let result = decode_probe_result(
        &result_json,
        &probe_config,
        provider.capability_override.as_ref(),
    )?;
    Ok(Some(result.capabilities))
}

pub(crate) fn decode_probe_result(
    result_json: &str,
    expected: &ProbeConfig,
    capability_override: Option<&LlmCapabilityOverride>,
) -> VcResult<ProbeResult> {
    let mut result: ProbeResult = serde_json::from_str(result_json).map_err(|error| {
        VcError::new(
            ErrorCode::CacheCorrupt,
            format!("decode cached LLM capability probe: {error}"),
        )
    })?;
    let expected_hash = CapabilityProbe::new(expected.clone()).cache_key();
    if result.probe_version != CAPABILITY_PROBE_VERSION
        || result.provider_profile_id != expected.provider_profile_id
        || result.profile_revision != expected.profile_revision
        || result.base_url.trim_end_matches('/') != expected.base_url.trim_end_matches('/')
        || result.model != expected.model
        || result.probe_hash != expected_hash
    {
        return Err(VcError::new(
            ErrorCode::CacheCorrupt,
            "cached LLM capability probe does not match its lookup identity",
        ));
    }
    result.capabilities = apply_capability_override(result.capabilities, capability_override);
    Ok(result)
}

pub(crate) fn apply_capability_override(
    mut capabilities: ProviderCapabilities,
    override_config: Option<&LlmCapabilityOverride>,
) -> ProviderCapabilities {
    let Some(override_config) = override_config else {
        return capabilities;
    };
    if let Some(mode) = override_config
        .structured_mode
        .as_deref()
        .and_then(parse_structured_mode)
    {
        capabilities.structured_mode = mode;
        capabilities.json_schema = mode == StructuredMode::JsonSchema;
        capabilities.json_mode = matches!(
            mode,
            StructuredMode::JsonSchema | StructuredMode::JsonObject
        );
    }
    if let Some(value) = override_config.returns_usage {
        capabilities.returns_usage = value;
    }
    if let Some(value) = override_config.supports_seed {
        capabilities.seed = value;
    }
    if let Some(value) = override_config.supports_model_list {
        capabilities.supports_model_list = value;
    }
    if override_config.max_context_tokens.is_some() {
        capabilities.max_context_tokens = override_config.max_context_tokens;
    }
    if override_config.max_output_tokens.is_some() {
        capabilities.max_output_tokens = override_config.max_output_tokens;
    }
    capabilities.manual_override = true;
    capabilities
}

fn parse_structured_mode(value: &str) -> Option<StructuredMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "json_schema" | "json-schema" => Some(StructuredMode::JsonSchema),
        "json_object" | "json-object" => Some(StructuredMode::JsonObject),
        "prompt_only" | "prompt-only" => Some(StructuredMode::PromptOnly),
        _ => None,
    }
}

pub(crate) fn structured_mode_label(mode: StructuredMode) -> &'static str {
    match mode {
        StructuredMode::JsonSchema => "json_schema",
        StructuredMode::JsonObject => "json_object",
        StructuredMode::PromptOnly => "prompt_only",
    }
}

pub(crate) fn prompt_snapshot(bundle: PromptBundle) -> PromptSnapshot {
    let stage = match bundle.stage {
        PromptStage::Split => LlmStage::Split,
        PromptStage::Correct => LlmStage::Correct,
        PromptStage::Translate => LlmStage::Translate,
    };
    PromptSnapshot {
        schema_version: bundle.schema_version,
        stage,
        files: bundle.files,
        content_hash: bundle.content_hash,
    }
}
