//! Token packing for LLM batches.
use super::types::*;
use videocaptionerr_contracts::error::{ErrorCode, VcError};

use crate::application_error::{AppResult, ApplicationError};
use crate::constants::{DEFAULT_CHARS_PER_TOKEN, LLM_MAX_ITEMS, TOKEN_SAFETY_MARGIN};

pub(crate) fn pack_batches(
    outputs: &[CueInput],
    context: &[CueInput],
    request: &LlmPipelineRequest,
    context_limit: u32,
    output_limit: u32,
) -> AppResult<Vec<BatchInput>> {
    let mut batches = Vec::new();
    let mut remaining = outputs;
    let mut index = 0;
    while !remaining.is_empty() {
        let batch = pack_one_batch(
            remaining,
            context,
            index,
            request,
            context_limit,
            output_limit,
        )?;
        let consumed = batch.output.len();
        batches.push(batch);
        remaining = &remaining[consumed..];
        index = index.saturating_add(1);
    }
    Ok(batches)
}

pub(crate) fn pack_one_batch(
    outputs: &[CueInput],
    context: &[CueInput],
    index: u32,
    request: &LlmPipelineRequest,
    context_limit: u32,
    output_limit: u32,
) -> AppResult<BatchInput> {
    let mut selected = Vec::new();
    for item in outputs.iter().take(LLM_MAX_ITEMS) {
        let candidate = {
            let mut current = selected.clone();
            current.push(item.clone());
            BatchInput {
                index,
                output: current,
                context: context.to_vec(),
            }
        };
        if fits_context(
            estimate_batch_tokens(&candidate, request.chars_per_token),
            output_limit,
            context_limit,
        ) {
            selected.push(item.clone());
        } else if selected.is_empty() {
            return Err(ApplicationError::Adapter(VcError::new(
                ErrorCode::LlmContextExceeded,
                format!("cue {} cannot fit the provider context limit", item.id),
            )));
        } else {
            break;
        }
    }
    Ok(BatchInput {
        index,
        output: selected,
        context: context.to_vec(),
    })
}

pub(crate) fn estimate_batch_tokens(batch: &BatchInput, chars_per_token: f64) -> u32 {
    let contents = batch
        .context
        .iter()
        .chain(batch.output.iter())
        .map(|item| item.text.as_str());
    contents
        .map(|text| estimate_tokens(text, chars_per_token))
        .sum::<u32>()
        .saturating_add(16)
}

pub(crate) fn estimate_tokens(text: &str, chars_per_token: f64) -> u32 {
    let cpt = if chars_per_token > 0.0 {
        chars_per_token
    } else {
        DEFAULT_CHARS_PER_TOKEN
    };
    ((text.chars().count() as f64 / cpt).ceil() as u32).max(1)
}

pub(crate) fn fits_context(input: u32, output: u32, limit: u32) -> bool {
    ((input as f64 + output as f64) * TOKEN_SAFETY_MARGIN).ceil() as u64 <= limit as u64
}
