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
            .map_err(ApplicationError::into_vc_error)?;
        self.active_runs
            .signal_batch(&id)
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
            .map_err(ApplicationError::into_vc_error)?;
        self.active_runs
            .signal_batch(&id)
            .map_err(ApplicationError::into_vc_error)?;
        if batch.status().is_terminal() {
            return Ok(());
        }

        // A live owner will observe the durable resume through its polling
        // loop (and the local signal above). If no owner is alive, this
        // control command may become the next Processing Owner and rebuild
        // the execution exclusively from persisted Job snapshots.
        let _lease = match self.acquire_gui_processing_lock() {
            Ok(lease) => lease,
            Err(error) if error.code == ErrorCode::InstanceBusy => return Ok(()),
            Err(error) => return Err(error),
        };
        self.resume_batch_uc
            .execute(id)
            .await
            .map(|_| ())
            .map_err(ApplicationError::into_vc_error)
    }
}
