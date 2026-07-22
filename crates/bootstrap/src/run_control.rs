//! Process-local implementation of Core's active-run control port.

use std::collections::HashMap;
use std::sync::Mutex;

use videocaptionerr_core::application_error::{AppResult, ApplicationError};
use videocaptionerr_core::ports::{ActiveRunRegistry, AsrCancelToken, RunControl};
use videocaptionerr_domain::{BatchId, JobId};

#[derive(Default)]
pub(crate) struct InMemoryActiveRunRegistry {
    runs: Mutex<HashMap<JobId, ActiveRun>>,
}

struct ActiveRun {
    batch_id: Option<BatchId>,
    control: RunControl,
}

impl InMemoryActiveRunRegistry {
    fn lock(&self) -> AppResult<std::sync::MutexGuard<'_, HashMap<JobId, ActiveRun>>> {
        self.runs
            .lock()
            .map_err(|_| ApplicationError::Invalid("active run registry was poisoned".into()))
    }
}

impl ActiveRunRegistry for InMemoryActiveRunRegistry {
    fn register(
        &self,
        job_id: JobId,
        batch_id: Option<BatchId>,
        control: RunControl,
    ) -> AppResult<()> {
        let mut runs = self.lock()?;
        if runs.contains_key(&job_id) {
            return Err(ApplicationError::Invalid(format!(
                "Job {job_id} already has an active run control"
            )));
        }
        runs.insert(job_id, ActiveRun { batch_id, control });
        Ok(())
    }

    fn unregister(&self, job_id: &JobId) {
        if let Ok(mut runs) = self.runs.lock() {
            runs.remove(job_id);
        }
    }

    fn cancel_job(&self, job_id: &JobId) -> AppResult<Option<AsrCancelToken>> {
        let control = self.lock()?.get(job_id).map(|run| run.control.clone());
        if let Some(control) = control {
            let token = control.cancellation_token();
            control.request_cancel();
            Ok(Some(token))
        } else {
            Ok(None)
        }
    }

    fn cancel_batch(&self, batch_id: &BatchId) -> AppResult<Option<AsrCancelToken>> {
        let controls: Vec<_> = self
            .lock()?
            .values()
            .filter(|run| run.batch_id.as_ref() == Some(batch_id))
            .map(|run| run.control.clone())
            .collect();
        for control in &controls {
            control.request_cancel();
        }
        Ok(controls.first().map(RunControl::cancellation_token))
    }

    fn signal_batch(&self, batch_id: &BatchId) -> AppResult<()> {
        let controls: Vec<_> = self
            .lock()?
            .values()
            .filter(|run| run.batch_id.as_ref() == Some(batch_id))
            .map(|run| run.control.clone())
            .collect();
        for control in controls {
            control.signal();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;
    use videocaptionerr_core::ports::ActiveRunRegistry;

    #[test]
    fn registry_cancels_the_registered_run_token_and_unregisters_idempotently() {
        let registry = InMemoryActiveRunRegistry::default();
        let job_id = JobId::from(Ulid::new());
        let batch_id = BatchId::from(Ulid::new());
        let control = RunControl::new();
        let token = control.cancellation_token();
        registry
            .register(job_id.clone(), Some(batch_id.clone()), control)
            .unwrap();
        let returned = registry.cancel_job(&job_id).unwrap().unwrap();
        assert!(returned.is_requested());
        assert!(token.is_requested());
        assert!(registry.cancel_batch(&batch_id).unwrap().is_some());
        registry.unregister(&job_id);
        registry.unregister(&job_id);
        assert!(registry.cancel_job(&job_id).unwrap().is_none());
    }
}
