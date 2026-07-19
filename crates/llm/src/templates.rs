//! Built-in Provider templates (prefill only; not capability guarantees).

use serde::{Deserialize, Serialize};

use crate::provider::{ProviderCapabilities, StructuredMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderTemplate {
    /// Generic OpenAI-compatible endpoint.
    Generic,
    Ollama,
    LmStudio,
}

impl ProviderTemplate {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "generic" | "openai" | "openai-compatible" => Some(Self::Generic),
            "ollama" => Some(Self::Ollama),
            "lmstudio" | "lm-studio" | "lm_studio" => Some(Self::LmStudio),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Generic => "generic",
            Self::Ollama => "ollama",
            Self::LmStudio => "lm_studio",
        }
    }

    /// Prefill likely base URL for local templates.
    pub fn default_base_url(self) -> &'static str {
        match self {
            Self::Generic => "https://api.openai.com/v1",
            Self::Ollama => "http://127.0.0.1:11434/v1",
            Self::LmStudio => "http://127.0.0.1:1234/v1",
        }
    }

    /// Conservative capability guess before probing.
    pub fn default_capabilities(self) -> ProviderCapabilities {
        match self {
            Self::Generic => ProviderCapabilities {
                json_mode: true,
                json_schema: false,
                tools: false,
                seed: false,
                vision: false,
                returns_usage: true,
                supports_model_list: true,
                max_context_tokens: Some(8192),
                max_output_tokens: Some(4096),
                manual_override: false,
                structured_mode: StructuredMode::JsonObject,
            },
            Self::Ollama => ProviderCapabilities {
                json_mode: true,
                json_schema: false,
                tools: false,
                seed: true,
                vision: false,
                returns_usage: false,
                supports_model_list: true,
                max_context_tokens: Some(8192),
                max_output_tokens: Some(2048),
                manual_override: false,
                structured_mode: StructuredMode::JsonObject,
            },
            Self::LmStudio => ProviderCapabilities {
                json_mode: true,
                json_schema: false,
                tools: false,
                seed: false,
                vision: false,
                returns_usage: true,
                supports_model_list: true,
                max_context_tokens: Some(4096),
                max_output_tokens: Some(2048),
                manual_override: false,
                structured_mode: StructuredMode::JsonObject,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_aliases() {
        assert_eq!(
            ProviderTemplate::parse("openai-compatible"),
            Some(ProviderTemplate::Generic)
        );
        assert_eq!(
            ProviderTemplate::parse("lm-studio"),
            Some(ProviderTemplate::LmStudio)
        );
    }
}
