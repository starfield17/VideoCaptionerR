use std::path::PathBuf;

use videocaptionerr_core::ports::{PromptSnapshot, StructuredOutput};
use videocaptionerr_core::use_cases::LlmProcessOptions;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub home: Option<PathBuf>,
    /// Explicit CLI/test override. Normal callers select the named profile
    /// and inherit its effective engine/model settings.
    pub engine: Option<String>,
    pub model_path: Option<PathBuf>,
    pub helper_path: Option<PathBuf>,
    pub prompt_dir: Option<PathBuf>,
    pub profile: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct LlmProcessDefaults {
    pub(crate) model: String,
    pub(crate) provider_profile_revision: String,
    pub(crate) split_prompt: PromptSnapshot,
    pub(crate) correct_prompt: PromptSnapshot,
    pub(crate) translate_prompt: PromptSnapshot,
    pub(crate) max_context_tokens: Option<u32>,
    pub(crate) max_output_tokens: Option<u32>,
    pub(crate) chars_per_token: f64,
    pub(crate) structured_output: StructuredOutput,
    pub(crate) seed: Option<i64>,
}

impl LlmProcessDefaults {
    pub(crate) fn with_target_language(&self, target_language: String) -> LlmProcessOptions {
        LlmProcessOptions {
            model: self.model.clone(),
            provider_profile_revision: self.provider_profile_revision.clone(),
            split_prompt: self.split_prompt.clone(),
            correct_prompt: self.correct_prompt.clone(),
            translate_prompt: self.translate_prompt.clone(),
            max_context_tokens: self.max_context_tokens,
            max_output_tokens: self.max_output_tokens,
            chars_per_token: self.chars_per_token,
            structured_output: self.structured_output,
            seed: self.seed,
            target_language,
        }
    }
}
