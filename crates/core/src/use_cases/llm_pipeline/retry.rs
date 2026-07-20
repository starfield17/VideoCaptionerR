//! Transport/semantic retry classification helpers.
use videocaptionerr_contracts::error::ErrorCode;

use crate::application_error::ApplicationError;
use crate::ports::{LlmMessage, LlmStage};

use super::types::LlmPipelineRequest;

pub(crate) fn can_isolate(error: &ApplicationError) -> bool {
    matches!(
        error_code(error),
        ErrorCode::LlmValidationFailed
            | ErrorCode::LlmInvalidResponse
            | ErrorCode::LlmContextExceeded
    )
}

pub(crate) fn error_code(error: &ApplicationError) -> ErrorCode {
    match error {
        ApplicationError::Adapter(error) => error.code,
        ApplicationError::Domain(_) => ErrorCode::InvalidArgument,
        ApplicationError::Cancelled => ErrorCode::Cancelled,
        ApplicationError::StatePersistence { primary, .. } => primary.code,
        ApplicationError::Invalid(_) => ErrorCode::InvalidArgument,
    }
}

pub(crate) fn temperature(stage: LlmStage) -> f32 {
    match stage {
        LlmStage::Split => 0.1,
        LlmStage::Correct | LlmStage::Translate => 0.2,
    }
}

pub(crate) fn hash_request(request: &LlmPipelineRequest, messages: &[LlmMessage]) -> String {
    let body = serde_json::json!({
        "provider_profile_revision": request.provider_profile_revision,
        "model": request.model,
        "stage": request.stage,
        "prompt_bundle_hash": request.prompt.content_hash,
        "messages": messages,
        "structured_output": request.structured_output,
        "seed": request.seed,
    });
    blake3::hash(body.to_string().as_bytes())
        .to_hex()
        .to_string()
}
