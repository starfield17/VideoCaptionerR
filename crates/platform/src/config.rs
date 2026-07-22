//! Global TOML configuration. API keys are plaintext by design (no warning).

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_contracts::version::SCHEMA_VERSION;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub schema_version: u32,
    #[serde(default)]
    pub llm: LlmSection,
    #[serde(default)]
    pub asr: AsrSection,
    #[serde(default)]
    pub export: ExportSection,
    #[serde(default)]
    pub cache: CacheSection,
    /// Named profiles override the global sections without changing their
    /// schema. Runtime creation resolves one profile into a frozen snapshot.
    #[serde(default)]
    pub profiles: BTreeMap<String, ProfileConfig>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            llm: LlmSection::default(),
            asr: AsrSection::default(),
            export: ExportSection::default(),
            cache: CacheSection::default(),
            profiles: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProfileConfig {
    #[serde(default)]
    pub preferred_engine: Option<String>,
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub device: Option<String>,
    #[serde(default)]
    pub compute_type: Option<String>,
    #[serde(default)]
    pub output_template: Option<String>,
    #[serde(default)]
    pub conflict_policy: Option<String>,
    #[serde(default)]
    pub cache_max_bytes: Option<u64>,
    #[serde(default)]
    pub llm_provider: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedProfile {
    pub name: Option<String>,
    pub preferred_engine: String,
    pub model_id: Option<String>,
    pub device: String,
    pub compute_type: String,
    pub output_template: String,
    pub conflict_policy: String,
    pub cache_max_bytes: u64,
    pub llm_provider: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LlmSection {
    #[serde(default)]
    pub providers: BTreeMap<String, LlmProviderConfig>,
    #[serde(default)]
    pub default_provider: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmProviderConfig {
    #[serde(default = "default_profile_revision")]
    pub profile_revision: u64,
    pub base_url: String,
    /// Plaintext by design. Never copy into Job snapshots, logs, or request hashes.
    pub api_key: String,
    pub model: String,
    #[serde(default)]
    pub template: Option<String>,
    #[serde(default)]
    pub capability_override: Option<LlmCapabilityOverride>,
}

/// Secret-free manual capability settings. The LLM adapter maps these values
/// to its provider capability type at the composition root.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LlmCapabilityOverride {
    #[serde(default)]
    pub structured_mode: Option<String>,
    #[serde(default)]
    pub returns_usage: Option<bool>,
    #[serde(default)]
    pub supports_seed: Option<bool>,
    #[serde(default)]
    pub supports_model_list: Option<bool>,
    #[serde(default)]
    pub max_context_tokens: Option<u32>,
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
}

fn default_profile_revision() -> u64 {
    1
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AsrSection {
    #[serde(default)]
    pub preferred_engine: Option<String>,
    #[serde(default)]
    pub model_id: Option<String>,
    #[serde(default)]
    pub device: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportSection {
    #[serde(default = "default_template")]
    pub template: String,
    #[serde(default = "default_conflict")]
    pub conflict_policy: String,
}

impl Default for ExportSection {
    fn default() -> Self {
        Self {
            template: default_template(),
            conflict_policy: default_conflict(),
        }
    }
}

fn default_template() -> String {
    "{stem}.{target_lang?}.{layout}.{format}".into()
}

fn default_conflict() -> String {
    "rename".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSection {
    #[serde(default = "default_cache_bytes")]
    pub max_bytes: u64,
}

impl Default for CacheSection {
    fn default() -> Self {
        Self {
            max_bytes: default_cache_bytes(),
        }
    }
}

fn default_cache_bytes() -> u64 {
    crate::constants::SHARED_CACHE_BYTES
}

impl AppConfig {
    pub fn resolve_profile(&self, name: Option<&str>) -> VcResult<ResolvedProfile> {
        let selected = match name {
            Some(name) => Some(self.profiles.get(name).ok_or_else(|| {
                VcError::new(
                    ErrorCode::InvalidConfig,
                    format!("profile '{name}' is not configured"),
                )
            })?),
            None => None,
        };
        let profile = selected.cloned().unwrap_or_default();
        Ok(ResolvedProfile {
            name: name.map(str::to_owned),
            preferred_engine: profile
                .preferred_engine
                .or_else(|| self.asr.preferred_engine.clone())
                .unwrap_or_else(|| "whisper-cpp".into()),
            model_id: profile.model_id.or_else(|| self.asr.model_id.clone()),
            device: profile
                .device
                .or_else(|| self.asr.device.clone())
                .unwrap_or_else(|| "cpu".into()),
            compute_type: profile.compute_type.unwrap_or_else(|| "default".into()),
            output_template: profile
                .output_template
                .unwrap_or_else(|| self.export.template.clone()),
            conflict_policy: profile
                .conflict_policy
                .unwrap_or_else(|| self.export.conflict_policy.clone()),
            cache_max_bytes: profile.cache_max_bytes.unwrap_or(self.cache.max_bytes),
            llm_provider: profile
                .llm_provider
                .or_else(|| self.llm.default_provider.clone()),
        })
    }

    pub fn load(path: &Path) -> VcResult<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let text = fs::read_to_string(path).map_err(|e| {
            VcError::new(
                ErrorCode::InvalidConfig,
                format!("read config {}: {e}", path.display()),
            )
        })?;
        let cfg: AppConfig = toml::from_str(&text).map_err(|e| {
            VcError::new(
                ErrorCode::InvalidConfig,
                format!("parse config {}: {e}", path.display()),
            )
        })?;
        if cfg.schema_version == 0 {
            return Err(VcError::new(
                ErrorCode::InvalidConfig,
                "config schema_version must be non-zero",
            ));
        }
        Ok(cfg)
    }

    pub fn save(&self, path: &Path) -> VcResult<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                VcError::new(ErrorCode::InvalidConfig, format!("create config dir: {e}"))
            })?;
        }
        let text = toml::to_string_pretty(self).map_err(|e| {
            VcError::new(ErrorCode::InvalidConfig, format!("serialize config: {e}"))
        })?;
        // Atomic-ish write via tmp + rename.
        let tmp = path.with_extension("toml.tmp");
        fs::write(&tmp, &text).map_err(|e| {
            VcError::new(ErrorCode::InvalidConfig, format!("write config tmp: {e}"))
        })?;
        fs::rename(&tmp, path)
            .map_err(|e| VcError::new(ErrorCode::InvalidConfig, format!("rename config: {e}")))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
            if let Some(parent) = path.parent() {
                let _ = fs::set_permissions(parent, fs::Permissions::from_mode(0o700));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn round_trip_toml() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let mut cfg = AppConfig::default();
        cfg.llm.providers.insert(
            "primary".into(),
            LlmProviderConfig {
                profile_revision: 1,
                base_url: "https://api.deepseek.com".into(),
                api_key: "sk-test".into(),
                model: "deepseek-v4-flash".into(),
                template: Some("generic".into()),
                capability_override: None,
            },
        );
        cfg.save(&path).unwrap();
        let loaded = AppConfig::load(&path).unwrap();
        assert_eq!(loaded.llm.providers["primary"].api_key, "sk-test");
    }

    #[test]
    fn named_profile_freezes_effective_runtime_settings() {
        let mut config = AppConfig::default();
        config.asr.preferred_engine = Some("whisper-cpp".into());
        config.export.conflict_policy = "rename".into();
        config.profiles.insert(
            "fast".into(),
            ProfileConfig {
                preferred_engine: Some("fake".into()),
                model_id: Some("model-a".into()),
                device: Some("cpu".into()),
                compute_type: Some("int8".into()),
                output_template: Some("{stem}.{format}".into()),
                conflict_policy: Some("fail".into()),
                cache_max_bytes: Some(123),
                llm_provider: None,
            },
        );
        let resolved = config.resolve_profile(Some("fast")).unwrap();
        assert_eq!(resolved.preferred_engine, "fake");
        assert_eq!(resolved.model_id.as_deref(), Some("model-a"));
        assert_eq!(resolved.compute_type, "int8");
        assert_eq!(resolved.output_template, "{stem}.{format}");
        assert_eq!(resolved.conflict_policy, "fail");
        assert_eq!(resolved.cache_max_bytes, 123);
    }
}
