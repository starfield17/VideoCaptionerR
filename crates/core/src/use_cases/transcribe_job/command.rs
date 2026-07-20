use super::*;

#[derive(Debug, Clone)]
pub struct LlmProcessOptions {
    pub model: String,
    pub provider_profile_revision: String,
    pub split_prompt: PromptSnapshot,
    pub correct_prompt: PromptSnapshot,
    pub translate_prompt: PromptSnapshot,
    pub max_context_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub chars_per_token: f64,
    pub structured_output: StructuredOutput,
    pub seed: Option<i64>,
    pub target_language: String,
}

impl LlmProcessOptions {
    pub(super) fn request(&self, stage: LlmStage) -> LlmPipelineRequest {
        let prompt = match stage {
            LlmStage::Split => self.split_prompt.clone(),
            LlmStage::Correct => self.correct_prompt.clone(),
            LlmStage::Translate => self.translate_prompt.clone(),
        };
        LlmPipelineRequest {
            stage,
            model: self.model.clone(),
            provider_profile_revision: self.provider_profile_revision.clone(),
            prompt,
            max_context_tokens: self.max_context_tokens,
            max_output_tokens: self.max_output_tokens,
            chars_per_token: self.chars_per_token,
            structured_output: self.structured_output,
            seed: self.seed,
            target_language: Some(self.target_language.clone()),
        }
    }
}

pub struct TranscribeJobCommand {
    pub job_id: JobId,
    pub batch_id: Option<BatchId>,
    pub execution_snapshot_id: UlidStr,
    pub profile_revision: UlidStr,
    pub input: PathBuf,
    pub job_dir: PathBuf,
    pub language: Option<String>,
    pub export: SubtitleExportRequest,
    pub llm: Option<LlmProcessOptions>,
}

#[derive(Debug)]
pub struct TranscribeJobResponse {
    pub job: Job,
    pub transcript: Transcript,
    pub export_path: PathBuf,
}
