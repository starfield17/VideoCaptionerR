use super::*;
use crate::execution_snapshot::JobExecutionSnapshot;
use crate::ports::{SubtitleFormat, SubtitleLayout};

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

    pub fn from_snapshot(snapshot: &crate::execution_snapshot::LlmExecutionSnapshot) -> Self {
        Self {
            model: snapshot.model.clone(),
            provider_profile_revision: snapshot.provider_profile_revision.clone(),
            split_prompt: snapshot.split_prompt.clone(),
            correct_prompt: snapshot.correct_prompt.clone(),
            translate_prompt: snapshot.translate_prompt.clone(),
            max_context_tokens: snapshot.max_context_tokens,
            max_output_tokens: snapshot.max_output_tokens,
            chars_per_token: snapshot.chars_per_token,
            structured_output: snapshot.structured_output,
            seed: snapshot.seed,
            target_language: snapshot.target_language.clone(),
        }
    }
}

#[derive(Debug, Clone)]
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

impl TranscribeJobCommand {
    /// Rebuild a Job command exclusively from its durable execution snapshot.
    /// Retry/resume paths must not re-read Prompt files, OutputPlanner state,
    /// or current Profile defaults.
    pub fn from_snapshot(snapshot: &JobExecutionSnapshot) -> AppResult<Self> {
        snapshot
            .validate()
            .map_err(ApplicationError::Invalid)?;
        let format = SubtitleFormat::parse(&snapshot.output.format).ok_or_else(|| {
            ApplicationError::Invalid(format!(
                "execution snapshot has unsupported subtitle format '{}'",
                snapshot.output.format
            ))
        })?;
        let layout = SubtitleLayout::parse(&snapshot.output.layout).ok_or_else(|| {
            ApplicationError::Invalid(format!(
                "execution snapshot has unsupported subtitle layout '{}'",
                snapshot.output.layout
            ))
        })?;
        Ok(Self {
            job_id: snapshot.job_id.clone(),
            batch_id: Some(snapshot.batch_id.clone()),
            execution_snapshot_id: snapshot.snapshot_id.clone(),
            profile_revision: snapshot.profile_revision.clone(),
            input: snapshot.source_path(),
            job_dir: snapshot.job_dir_path(),
            language: snapshot.source_language.clone(),
            export: SubtitleExportRequest {
                output_path: snapshot.output_path(),
                format,
                layout,
                fallback_to_source: snapshot.output.fallback_to_source,
            },
            llm: snapshot.llm.as_ref().map(LlmProcessOptions::from_snapshot),
        })
    }
}

#[derive(Debug)]
pub struct TranscribeJobResponse {
    pub job: Job,
    pub transcript: Transcript,
    pub export_path: PathBuf,
}
