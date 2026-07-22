//! Durable cross-process cancellation propagation for an owner-local run.

use std::sync::Arc;
use std::time::Duration;

use videocaptionerr_domain::{BatchId, BatchStatus, JobId, JobStatus};

use crate::ports::{BatchRepository, JobRepository, RunControl};

/// Polling interval for the durable cancellation watcher. SQLite remains the
/// cross-process fact source; the in-process registry is only a fast signal.
pub const CANCELLATION_WATCH_INTERVAL: Duration = Duration::from_millis(250);

/// One watcher belongs to exactly one active Job registration.
///
/// The owner must stop this handle when the Job leaves execution. `Drop` also
/// aborts it as an error-path safety net, so a failed stage cannot orphan a
/// Tokio task that continues querying the store.
pub struct ActiveCancellationWatcher {
    handle: tokio::task::JoinHandle<()>,
}

impl ActiveCancellationWatcher {
    pub fn spawn(
        jobs: Arc<dyn JobRepository>,
        batches: Arc<dyn BatchRepository>,
        job_id: JobId,
        batch_id: Option<BatchId>,
        control: RunControl,
    ) -> Self {
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(CANCELLATION_WATCH_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                interval.tick().await;
                if control.cancellation_token().is_requested() {
                    return;
                }

                let job = match jobs.load_job(&job_id).await {
                    Ok(job) => job,
                    Err(error) => {
                        tracing::warn!(
                            job_id = %job_id,
                            error = %error,
                            "cancellation watcher could not read Job state; retrying"
                        );
                        continue;
                    }
                };
                let job_cancelled = match job {
                    Some(job) => job.cancel_requested() || job.status() == JobStatus::Cancelled,
                    None => {
                        tracing::error!(
                            job_id = %job_id,
                            "cancellation watcher lost the persisted Job row; retrying"
                        );
                        false
                    }
                };

                let batch_cancelled = if let Some(batch_id) = batch_id.as_ref() {
                    match batches.load_batch(batch_id).await {
                        Ok(Some(batch)) => {
                            batch.cancel_requested() || batch.status() == BatchStatus::Cancelled
                        }
                        Ok(None) => {
                            tracing::error!(
                                job_id = %job_id,
                                batch_id = %batch_id,
                                "cancellation watcher lost the persisted Batch row; retrying"
                            );
                            false
                        }
                        Err(error) => {
                            tracing::warn!(
                                job_id = %job_id,
                                batch_id = %batch_id,
                                error = %error,
                                "cancellation watcher could not read Batch state; retrying"
                            );
                            false
                        }
                    }
                } else {
                    false
                };

                if job_cancelled || batch_cancelled {
                    control.request_cancel();
                    return;
                }
            }
        });
        Self { handle }
    }

    /// Abort and await the watcher before releasing its owner registration.
    pub async fn stop(mut self) {
        self.handle.abort();
        let _ = (&mut self.handle).await;
    }

    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}

impl Drop for ActiveCancellationWatcher {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
