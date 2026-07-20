use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::application_error::ApplicationError;
use videocaptionerr_core::use_cases::{EditTranscriptCommand, EditTranscriptResponse};
use videocaptionerr_domain::{JobId, LlmTextField};

use crate::dto::TranscriptEditView;
use crate::runtime::ApplicationRuntime;

impl ApplicationRuntime {
    pub async fn load_transcript(
        &self,
        job_id: &str,
    ) -> VcResult<videocaptionerr_domain::Transcript> {
        let job_id: JobId = job_id.parse().map_err(|error| {
            VcError::new(
                ErrorCode::InvalidArgument,
                format!("invalid Job id: {error}"),
            )
        })?;
        self.transcript_editor
            .load(&job_id)
            .await
            .map_err(ApplicationError::into_vc_error)
    }

    pub async fn edit_transcript(
        &self,
        job_id: &str,
        cue_id: u32,
        expected_revision: u64,
        field: &str,
        value: String,
    ) -> VcResult<EditTranscriptResponse> {
        let job_id: JobId = job_id.parse().map_err(|error| {
            VcError::new(
                ErrorCode::InvalidArgument,
                format!("invalid Job id: {error}"),
            )
        })?;
        let field = match field.trim().to_ascii_lowercase().as_str() {
            "source" | "text" => LlmTextField::Source,
            "translation" => LlmTextField::Translation,
            _ => {
                return Err(VcError::new(
                    ErrorCode::InvalidArgument,
                    "field must be source or translation",
                ))
            }
        };
        self.transcript_editor
            .edit(EditTranscriptCommand {
                job_id,
                cue_id,
                expected_transcript_revision: expected_revision,
                field,
                value,
            })
            .await
            .map_err(ApplicationError::into_vc_error)
    }

    pub async fn edit_transcript_view(
        &self,
        job_id: &str,
        cue_id: u32,
        expected_revision: u64,
        field: &str,
        value: String,
    ) -> VcResult<TranscriptEditView> {
        let result = self
            .edit_transcript(job_id, cue_id, expected_revision, field, value)
            .await?;
        Ok(TranscriptEditView {
            transcript: result.transcript,
            stage: result.stage.as_str().into(),
        })
    }
}
