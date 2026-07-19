//! Capability probing and in-memory cache identity.
//!
//! Probes are intentionally small. They establish connectivity and test
//! optional request features; they never discover context limits by sending a
//! deliberately oversized request.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use videocaptionerr_contracts::error::VcResult;

use crate::openai::{endpoint, OpenAiConfig, OpenAiProvider};
use crate::provider::{
    ChatMessage, ChatRequest, ProviderCapabilities, StructuredMode, CAPABILITY_PROBE_VERSION,
};

#[derive(Debug, Clone)]
pub struct ProbeConfig {
    pub provider_profile_id: String,
    pub profile_revision: u64,
    pub base_url: String,
    pub api_key: String,
    pub model: String,
    pub timeout: Duration,
    /// Manual override wins over detected values but does not change cache identity.
    pub manual_override: Option<ProviderCapabilities>,
}

impl ProbeConfig {
    pub fn new(
        provider_profile_id: impl Into<String>,
        profile_revision: u64,
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            provider_profile_id: provider_profile_id.into(),
            profile_revision,
            base_url: base_url.into(),
            api_key: api_key.into(),
            model: model.into(),
            timeout: Duration::from_secs(30),
            manual_override: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeResult {
    pub probe_version: u32,
    pub provider_profile_id: String,
    pub profile_revision: u64,
    pub base_url: String,
    pub model: String,
    pub probe_hash: String,
    pub capabilities: ProviderCapabilities,
    #[serde(default)]
    pub warnings: Vec<String>,
}

impl ProbeResult {
    pub fn cache_key(&self) -> String {
        self.probe_hash.clone()
    }
}

/// Small process-local cache. Persistent callers can serialize `ProbeResult`
/// into the store's `llm_capability_probes` table using the same hash.
#[derive(Debug, Default)]
pub struct CapabilityCache {
    entries: BTreeMap<String, ProbeResult>,
}

impl CapabilityCache {
    pub fn get(&self, config: &ProbeConfig) -> Option<&ProbeResult> {
        self.entries.get(&probe_hash(config))
    }

    pub fn insert(&mut self, result: ProbeResult) {
        self.entries.insert(result.cache_key(), result);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub struct CapabilityProbe {
    config: ProbeConfig,
}

impl CapabilityProbe {
    pub fn new(config: ProbeConfig) -> Self {
        Self { config }
    }

    pub fn cache_key(&self) -> String {
        probe_hash(&self.config)
    }

    pub async fn run(&self) -> VcResult<ProbeResult> {
        let mut openai_config = OpenAiConfig::new(
            self.config.provider_profile_id.clone(),
            self.config.base_url.clone(),
            self.config.api_key.clone(),
            self.config.model.clone(),
        );
        openai_config.timeout = self.config.timeout;
        openai_config.capabilities = ProviderCapabilities::conservative_default();
        let provider = OpenAiProvider::new(openai_config)?;

        // Connectivity/auth/model acceptance is the only mandatory probe.
        let basic_request =
            probe_request(&self.config.model, StructuredMode::PromptOnly, None, None);
        let basic = provider.post_chat_request(&basic_request).await?;
        let mut capabilities = ProviderCapabilities::conservative_default();
        capabilities.returns_usage =
            basic.prompt_tokens.is_some() || basic.completion_tokens.is_some();
        let mut warnings = Vec::new();

        let schema = json!({
            "type": "object",
            "properties": {"ok": {"type": "boolean"}},
            "required": ["ok"],
            "additionalProperties": false
        });
        if provider
            .post_chat_request(&probe_request(
                &self.config.model,
                StructuredMode::JsonSchema,
                Some(schema),
                None,
            ))
            .await
            .is_ok()
        {
            capabilities.json_schema = true;
        } else if provider
            .post_chat_request(&probe_request(
                &self.config.model,
                StructuredMode::JsonObject,
                None,
                None,
            ))
            .await
            .is_ok()
        {
            capabilities.json_mode = true;
        } else {
            warnings.push("structured JSON output is unavailable; using prompt-only JSON".into());
        }

        if provider
            .post_chat_request(&probe_request(
                &self.config.model,
                StructuredMode::PromptOnly,
                None,
                Some(7),
            ))
            .await
            .is_ok()
        {
            capabilities.seed = true;
        } else {
            warnings.push("seed parameter is unavailable".into());
        }

        let model_response = provider
            .client()
            .get(endpoint(provider.base_url(), "models"))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .bearer_auth(provider.api_key())
            .send()
            .await;
        match model_response {
            Ok(response) if response.status().is_success() => {
                capabilities.supports_model_list = true;
            }
            Ok(_) => warnings.push("model-list endpoint is unavailable".into()),
            Err(_) => warnings.push("model-list endpoint could not be reached".into()),
        }

        capabilities.structured_mode = capabilities.effective_structured_mode();
        if let Some(mut override_caps) = self.config.manual_override.clone() {
            override_caps.manual_override = true;
            capabilities = override_caps;
        }

        Ok(ProbeResult {
            probe_version: CAPABILITY_PROBE_VERSION,
            provider_profile_id: self.config.provider_profile_id.clone(),
            profile_revision: self.config.profile_revision,
            base_url: self.config.base_url.trim_end_matches('/').to_owned(),
            model: self.config.model.clone(),
            probe_hash: probe_hash(&self.config),
            capabilities,
            warnings,
        })
    }
}

fn probe_request(
    model: &str,
    mode: StructuredMode,
    schema: Option<serde_json::Value>,
    seed: Option<i64>,
) -> ChatRequest {
    ChatRequest {
        model: model.to_owned(),
        messages: vec![ChatMessage::user(
            "Reply with a minimal JSON object: {\"ok\":true}.",
        )],
        temperature: Some(0.0),
        max_tokens: Some(16),
        response_format_json: None,
        seed,
        structured_mode: Some(mode),
        response_schema: schema,
    }
}

fn probe_hash(config: &ProbeConfig) -> String {
    // Deliberately exclude api_key. The profile revision is the secret-safe
    // invalidation boundary for authentication changes.
    let identity = format!(
        "{}\n{}\n{}\n{}\n{}",
        config.base_url.trim_end_matches('/'),
        config.model,
        config.provider_profile_id,
        config.profile_revision,
        CAPABILITY_PROBE_VERSION
    );
    blake3::hash(identity.as_bytes()).to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_identity_excludes_api_key() {
        let mut a = ProbeConfig::new("p", 3, "https://example.test/v1", "secret-a", "m");
        let mut b = a.clone();
        b.api_key = "secret-b".into();
        assert_eq!(
            CapabilityProbe::new(a.clone()).cache_key(),
            CapabilityProbe::new(b).cache_key()
        );
        a.profile_revision += 1;
        assert_ne!(
            CapabilityProbe::new(a).cache_key(),
            CapabilityProbe::new(ProbeConfig::new(
                "p",
                3,
                "https://example.test/v1",
                "secret-a",
                "m"
            ))
            .cache_key()
        );
    }

    #[test]
    fn manual_override_has_priority() {
        let mut caps = ProviderCapabilities::conservative_default();
        caps.json_schema = true;
        let mut config = ProbeConfig::new("p", 1, "https://example.test", "", "m");
        config.manual_override = Some(caps.clone());
        assert!(config.manual_override.unwrap().json_schema);
    }
}
