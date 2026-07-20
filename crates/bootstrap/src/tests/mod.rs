use crate::capability::{decode_probe_result, structured_mode_label};
use crate::{ApplicationRuntime, RuntimeConfig};
use videocaptionerr_contracts::error::ErrorCode;
use videocaptionerr_llm::probe::{CapabilityProbe, ProbeConfig, ProbeResult};
use videocaptionerr_llm::provider::{
    ProviderCapabilities, StructuredMode, CAPABILITY_PROBE_VERSION,
};
use videocaptionerr_platform::LlmCapabilityOverride;

fn probe_config() -> ProbeConfig {
    ProbeConfig::new(
        "primary",
        7,
        "https://example.test/v1/",
        "test-only-secret",
        "model-a",
    )
}

fn probe_fixture(config: &ProbeConfig) -> ProbeResult {
    ProbeResult {
        probe_version: CAPABILITY_PROBE_VERSION,
        provider_profile_id: config.provider_profile_id.clone(),
        profile_revision: config.profile_revision,
        base_url: config.base_url.trim_end_matches('/').into(),
        model: config.model.clone(),
        probe_hash: CapabilityProbe::new(config.clone()).cache_key(),
        capabilities: ProviderCapabilities::conservative_default(),
        warnings: vec![],
    }
}

#[test]
fn cached_probe_requires_full_identity_match() {
    let config = probe_config();
    let fixture = probe_fixture(&config);
    let encoded = serde_json::to_string(&fixture).unwrap();
    let decoded = decode_probe_result(&encoded, &config, None).unwrap();
    assert_eq!(decoded.probe_hash, fixture.probe_hash);

    let mut corrupt = fixture;
    corrupt.model = "different-model".into();
    let error =
        decode_probe_result(&serde_json::to_string(&corrupt).unwrap(), &config, None).unwrap_err();
    assert_eq!(error.code, ErrorCode::CacheCorrupt);
}

#[test]
fn manual_capability_override_wins_over_cached_auto_result() {
    let config = probe_config();
    let fixture = probe_fixture(&config);
    let override_config = LlmCapabilityOverride {
        structured_mode: Some("json_schema".into()),
        ..Default::default()
    };
    let decoded = decode_probe_result(
        &serde_json::to_string(&fixture).unwrap(),
        &config,
        Some(&override_config),
    )
    .unwrap();
    assert_eq!(
        decoded.capabilities.effective_structured_mode(),
        StructuredMode::JsonSchema
    );
    assert!(decoded.capabilities.manual_override);
}

#[test]
fn capability_view_uses_stable_structured_mode_labels() {
    assert_eq!(
        structured_mode_label(StructuredMode::JsonSchema),
        "json_schema"
    );
    assert_eq!(
        structured_mode_label(StructuredMode::JsonObject),
        "json_object"
    );
    assert_eq!(
        structured_mode_label(StructuredMode::PromptOnly),
        "prompt_only"
    );
}

#[tokio::test]
async fn probe_without_a_profile_fails_before_any_provider_request() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = ApplicationRuntime::open(RuntimeConfig {
        home: Some(dir.path().to_path_buf()),
        engine: "fake".into(),
        model_path: None,
        helper_path: None,
        prompt_dir: None,
    })
    .unwrap();
    let error = runtime
        .probe_llm_capabilities(None, false)
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::LlmProviderUnavailable);
}
