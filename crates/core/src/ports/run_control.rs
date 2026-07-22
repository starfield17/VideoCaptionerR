//! In-process controls for the currently running Processing Owner.
//!
//! The registry is deliberately a port. Core owns the lifecycle contract, but
//! Bootstrap/Platform owns the synchronization implementation. Persistent
//! Job/Batch state remains authoritative for commands issued by another
//! process.

use std::sync::Arc;

use tokio::sync::Notify;
use videocaptionerr_domain::{BatchId, JobId};

use crate::application_error::AppResult;
use crate::ports::AsrCancelToken;

/// The actual cooperative cancellation token and an in-process wake-up for a
/// long-lived Batch wait. A clone refers to the same cancellation state.
#[derive(Clone)]
pub struct RunControl {
    token: AsrCancelToken,
    wake: Arc<Notify>,
}

impl std::fmt::Debug for RunControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RunControl")
            .field("cancelled", &self.token.is_requested())
            .finish_non_exhaustive()
    }
}

impl RunControl {
    pub fn new() -> Self {
        Self {
            token: AsrCancelToken::new(),
            wake: Arc::new(Notify::new()),
        }
    }

    pub fn cancellation_token(&self) -> AsrCancelToken {
        self.token.clone()
    }

    pub fn request_cancel(&self) {
        self.token.request();
        self.wake.notify_waiters();
    }

    pub fn signal(&self) {
        self.wake.notify_waiters();
    }

    pub async fn wait(&self) {
        self.wake.notified().await;
    }
}

impl Default for RunControl {
    fn default() -> Self {
        Self::new()
    }
}

/// Registry of real controls owned by the active Processing Owner.
#[allow(clippy::missing_errors_doc)]
pub trait ActiveRunRegistry: Send + Sync {
    /// Register the token that will be passed to the actual ASR session.
    fn register(
        &self,
        job_id: JobId,
        batch_id: Option<BatchId>,
        control: RunControl,
    ) -> AppResult<()>;

    /// Remove a control after the Job has reached a terminal or cancelled
    /// state. Implementations must make this operation idempotent.
    fn unregister(&self, job_id: &JobId);

    /// Request cancellation of an active Job and return its real token when
    /// one is present in this process.
    fn cancel_job(&self, job_id: &JobId) -> AppResult<Option<AsrCancelToken>>;

    /// Request cancellation for all active Jobs in a Batch.
    fn cancel_batch(&self, batch_id: &BatchId) -> AppResult<Option<AsrCancelToken>>;

    /// Wake the live owner after a durable pause/resume command. Cross-process
    /// callers still rely on the owner polling persisted Batch state.
    fn signal_batch(&self, batch_id: &BatchId) -> AppResult<()>;
}
