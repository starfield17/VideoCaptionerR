//! Batch pause/resume and live event helpers for CLI/Desktop.

use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::application_error::ApplicationError;
use videocaptionerr_core::ports::ExpectedVersion;
use videocaptionerr_domain::BatchId;

use crate::runtime::ApplicationRuntime;

impl ApplicationRuntime {
    pub async fn pause_batch(&self, batch_id: &str) -> VcResult<()> {
        let id: BatchId = batch_id.parse().map_err(|e| {
            VcError::new(ErrorCode::InvalidArgument, format!("invalid Batch id: {e}"))
        })?;
        let mut batch = self
            .batches
            .load_batch(&id)
            .await
            .map_err(ApplicationError::into_vc_error)?
            .ok_or_else(|| {
                VcError::new(ErrorCode::InvalidArgument, format!("Batch {id} not found"))
            })?;
        batch.request_pause().map_err(VcError::from)?;
        let expected = ExpectedVersion::Exact(batch.version);
        self.batches
            .save_batch(&mut batch, expected)
            .await
            .map_err(ApplicationError::into_vc_error)
    }

    pub async fn resume_batch(&self, batch_id: &str) -> VcResult<()> {
        let id: BatchId = batch_id.parse().map_err(|e| {
            VcError::new(ErrorCode::InvalidArgument, format!("invalid Batch id: {e}"))
        })?;
        let mut batch = self
            .batches
            .load_batch(&id)
            .await
            .map_err(ApplicationError::into_vc_error)?
            .ok_or_else(|| {
                VcError::new(ErrorCode::InvalidArgument, format!("Batch {id} not found"))
            })?;
        batch.resume().map_err(VcError::from)?;
        let expected = ExpectedVersion::Exact(batch.version);
        self.batches
            .save_batch(&mut batch, expected)
            .await
            .map_err(ApplicationError::into_vc_error)
    }
}
