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
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            llm: LlmSection::default(),
            asr: AsrSection::default(),
            export: ExportSection::default(),
            cache: CacheSection::default(),
        }
    }
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
    pub base_url: String,
    /// Plaintext by design. Never copy into Job snapshots, logs, or request hashes.
    pub api_key: String,
    pub model: String,
    #[serde(default)]
    pub template: Option<String>,
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
                base_url: "https://api.deepseek.com".into(),
                api_key: "sk-test".into(),
                model: "deepseek-v4-flash".into(),
                template: Some("generic".into()),
            },
        );
        cfg.save(&path).unwrap();
        let loaded = AppConfig::load(&path).unwrap();
        assert_eq!(loaded.llm.providers["primary"].api_key, "sk-test");
    }
}
