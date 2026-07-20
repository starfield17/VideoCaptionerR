//! Plan construction.
use super::types::*;
use super::packing::{estimate_batch_tokens, pack_batches};
use crate::ports::{LlmStage, StructuredOutput};

pub(crate) fn empty_plan(request: &LlmPipelineRequest) -> LlmPlan {
    LlmPlan {
        schema_version: super::durable::LLM_PLAN_SCHEMA_VERSION,
        plan_id: String::new(),
        job_id: request
            .durable
            .as_ref()
            .map(|d| d.job_id.to_string()),
        stage: request.stage,
        input_artifact_id: request
            .durable
            .as_ref()
            .and_then(|d| d.input_artifact_id.clone()),
        transcript_revision: request
            .durable
            .as_ref()
            .map(|d| d.transcript_revision)
            .unwrap_or(0),
        model: request.model.clone(),
        provider_profile_revision: request.provider_profile_revision.clone(),
        prompt_bundle_hash: request.prompt.content_hash.clone(),
        prompt_artifact_hash: request.prompt.content_hash.clone(),
        effective_capability: format!("{:?}", request.structured_output),
        max_context_tokens: request.max_context_tokens,
        max_output_tokens: request.max_output_tokens,
        target_language: request.target_language.clone(),
        entries: Vec::new(),
        plan_hash: String::new(),
    }
}

pub(crate) fn effective_structured_output(
    requested: StructuredOutput,
    available: StructuredOutput,
) -> StructuredOutput {
    match (requested, available) {
        (StructuredOutput::JsonSchema, StructuredOutput::JsonSchema) => {
            StructuredOutput::JsonSchema
        }
        (StructuredOutput::JsonSchema, StructuredOutput::JsonObject)
        | (StructuredOutput::JsonObject, StructuredOutput::JsonSchema)
        | (StructuredOutput::JsonObject, StructuredOutput::JsonObject) => {
            StructuredOutput::JsonObject
        }
        (StructuredOutput::PromptOnly, _) | (_, StructuredOutput::PromptOnly) => {
            StructuredOutput::PromptOnly
        }
    }
}

pub(crate) fn make_plan(
    request: &LlmPipelineRequest,
    batches: &[BatchInput],
    context_limit: u32,
    output_limit: u32,
) -> LlmPlan {
    let entries = batches
        .iter()
        .map(|batch| {
            let input_tokens = estimate_batch_tokens(batch, request.chars_per_token);
            LlmPlanEntry {
                batch_index: batch.index,
                output_cue_ids: batch.output.iter().map(|item| item.id).collect(),
                context_cue_ids: batch.context.iter().map(|item| item.id).collect(),
                estimated_input_tokens: input_tokens.min(context_limit),
                reserved_output_tokens: output_limit,
                expected_text_revisions: Default::default(),
                expected_translation_revisions: Default::default(),
            }
        })
        .collect();
    let mut plan = LlmPlan {
        schema_version: super::durable::LLM_PLAN_SCHEMA_VERSION,
        plan_id: format!(
            "plan-{}",
            blake3::hash(
                format!(
                    "{}|{}|{}",
                    request.stage.as_str(),
                    request.prompt.content_hash,
                    request.model
                )
                .as_bytes()
            )
            .to_hex()
        ),
        job_id: request
            .durable
            .as_ref()
            .map(|d| d.job_id.to_string()),
        stage: request.stage,
        input_artifact_id: request
            .durable
            .as_ref()
            .and_then(|d| d.input_artifact_id.clone()),
        transcript_revision: request
            .durable
            .as_ref()
            .map(|d| d.transcript_revision)
            .unwrap_or(0),
        model: request.model.clone(),
        provider_profile_revision: request.provider_profile_revision.clone(),
        prompt_bundle_hash: request.prompt.content_hash.clone(),
        prompt_artifact_hash: request.prompt.content_hash.clone(),
        effective_capability: format!("{:?}", request.structured_output),
        max_context_tokens: Some(context_limit),
        max_output_tokens: Some(output_limit),
        target_language: request.target_language.clone(),
        entries,
        plan_hash: String::new(),
    };
    plan.plan_hash = super::durable::plan_hash(&plan);
    plan
}
