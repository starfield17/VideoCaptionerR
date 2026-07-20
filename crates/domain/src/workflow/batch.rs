use super::{DomainEvent, JobTerminalStatus};
use crate::error::{DomainError, DomainResult};
use crate::identity::{BatchId, JobId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchExecutionProfile {
    pub asr_engine: String,
    pub asr_model: String,
    pub device: String,
    pub compute_type: String,
}

impl BatchExecutionProfile {
    pub fn same_asr_profile(&self, other: &Self) -> bool {
        self.asr_engine == other.asr_engine
            && self.asr_model == other.asr_model
            && self.device == other.device
            && self.compute_type == other.compute_type
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchStatus {
    Pending,
    Running,
    Done,
    Failed,
    Cancelled,
}

impl BatchStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed | Self::Cancelled)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Batch {
    id: BatchId,
    job_ids: Vec<JobId>,
    execution_profile: BatchExecutionProfile,
    status: BatchStatus,
    cancel_requested: bool,
    terminal_jobs: Vec<(JobId, JobTerminalStatusWire)>,
    terminal_event_emitted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum JobTerminalStatusWire {
    Done,
    DoneDegraded,
    Failed,
    Cancelled,
}

impl From<JobTerminalStatus> for JobTerminalStatusWire {
    fn from(value: JobTerminalStatus) -> Self {
        match value {
            JobTerminalStatus::Done => Self::Done,
            JobTerminalStatus::DoneDegraded => Self::DoneDegraded,
            JobTerminalStatus::Failed => Self::Failed,
            JobTerminalStatus::Cancelled => Self::Cancelled,
        }
    }
}

impl Batch {
    pub fn new(
        id: BatchId,
        job_ids: Vec<JobId>,
        execution_profile: BatchExecutionProfile,
    ) -> DomainResult<Self> {
        if job_ids.is_empty() {
            return Err(DomainError::InvalidArgument(
                "a batch must contain at least one job".into(),
            ));
        }
        if job_ids.windows(2).any(|ids| ids[0] == ids[1]) {
            return Err(DomainError::InvalidArgument(
                "a batch cannot contain duplicate jobs".into(),
            ));
        }
        Ok(Self {
            id,
            job_ids,
            execution_profile,
            status: BatchStatus::Pending,
            cancel_requested: false,
            terminal_jobs: Vec::new(),
            terminal_event_emitted: false,
        })
    }

    pub fn id(&self) -> &BatchId {
        &self.id
    }

    pub fn job_ids(&self) -> &[JobId] {
        &self.job_ids
    }

    pub fn execution_profile(&self) -> &BatchExecutionProfile {
        &self.execution_profile
    }

    pub fn status(&self) -> BatchStatus {
        self.status
    }

    pub fn cancel_requested(&self) -> bool {
        self.cancel_requested
    }

    pub fn start(&mut self) -> DomainResult<()> {
        self.transition(BatchStatus::Running)
    }

    pub fn require_profile(&self, profile: &BatchExecutionProfile) -> DomainResult<()> {
        if self.execution_profile.same_asr_profile(profile) {
            Ok(())
        } else {
            Err(DomainError::BatchProfileMismatch)
        }
    }

    pub fn record_job_terminal(
        &mut self,
        job_id: &JobId,
        status: JobTerminalStatus,
    ) -> DomainResult<()> {
        if self.status != BatchStatus::Running {
            return Err(DomainError::IllegalTransition {
                aggregate: "Batch",
                from: format!("{:?}", self.status),
                to: "record_job_terminal".into(),
            });
        }
        if !self.job_ids.iter().any(|candidate| candidate == job_id) {
            return Err(DomainError::MemberNotFound {
                aggregate: "Batch",
                id: job_id.to_string(),
            });
        }
        if let Some(existing) = self
            .terminal_jobs
            .iter_mut()
            .find(|(candidate, _)| candidate == job_id)
        {
            existing.1 = status.into();
        } else {
            self.terminal_jobs.push((job_id.clone(), status.into()));
        }
        Ok(())
    }

    pub fn finish(&mut self, status: BatchStatus) -> DomainResult<DomainEvent> {
        if !matches!(
            status,
            BatchStatus::Done | BatchStatus::Failed | BatchStatus::Cancelled
        ) {
            return Err(DomainError::InvalidArgument(
                "batch finish status must be terminal".into(),
            ));
        }
        if self.status != BatchStatus::Running {
            return Err(DomainError::AlreadyTerminal { aggregate: "Batch" });
        }
        if self.terminal_jobs.len() != self.job_ids.len() {
            return Err(DomainError::InvalidArgument(
                "all batch jobs must be terminal before the batch finishes".into(),
            ));
        }
        self.status = status;
        self.terminal_event_emitted = true;
        Ok(DomainEvent::BatchReachedTerminal {
            batch_id: self.id.clone(),
            status,
        })
    }

    pub fn cancel(&mut self) -> DomainResult<DomainEvent> {
        if self.status.is_terminal() {
            return Err(DomainError::AlreadyTerminal { aggregate: "Batch" });
        }
        self.cancel_requested = true;
        self.status = BatchStatus::Cancelled;
        self.terminal_event_emitted = true;
        Ok(DomainEvent::BatchReachedTerminal {
            batch_id: self.id.clone(),
            status: BatchStatus::Cancelled,
        })
    }

    /// A running Batch has no active owner after process restart. Returning it
    /// to Pending lets a later application command explicitly resume it.
    pub fn recover_after_restart(&mut self) -> DomainResult<()> {
        if self.status == BatchStatus::Running {
            self.status = BatchStatus::Pending;
        }
        Ok(())
    }

    pub fn terminal_event_emitted(&self) -> bool {
        self.terminal_event_emitted
    }

    fn transition(&mut self, to: BatchStatus) -> DomainResult<()> {
        if self.status != BatchStatus::Pending || to != BatchStatus::Running {
            return Err(DomainError::IllegalTransition {
                aggregate: "Batch",
                from: format!("{:?}", self.status),
                to: format!("{:?}", to),
            });
        }
        self.status = to;
        Ok(())
    }
}
