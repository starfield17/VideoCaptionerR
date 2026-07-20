use super::{ArtifactRef, StageKind};
use crate::error::{DomainError, DomainResult};
use crate::identity::{JobId, WorkUnitId};
use serde::{Deserialize, Serialize};

/// Default automatic retry budget for a WorkUnit (two retries after the first
/// attempt). Deterministic failures never consume this budget.
pub const WORK_UNIT_DEFAULT_AUTO_RETRIES: u32 = 2;

/// OOM may change strategy at most once (for example smaller batch or compute
/// mode). Silent GPU→CPU fallback is never implied by this budget.
pub const WORK_UNIT_OOM_STRATEGY_RETRIES: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkUnitStatus {
    Pending,
    Running,
    Done,
    Failed,
    Cancelled,
}

/// Errors that must not be automatically retried. Protocol pollution, corrupt
/// models, and unsupported parameters fall into this set.
pub fn is_deterministic_work_unit_error(error_code: &str) -> bool {
    matches!(
        error_code,
        "WORKER_PROTOCOL_ERROR"
            | "MODEL_DIGEST_MISMATCH"
            | "MODEL_NOT_FOUND"
            | "OPTION_UNSUPPORTED"
            | "ENGINE_CAPABILITY_INSUFFICIENT"
            | "ARTIFACT_CORRUPT"
            | "SOURCE_CHANGED"
            | "INPUT_NOT_FOUND"
            | "INPUT_UNSUPPORTED"
            | "AUDIO_STREAM_NOT_FOUND"
            | "INVALID_ARGUMENT"
            | "INVALID_CONFIG"
    )
}

pub fn is_oom_error(error_code: &str) -> bool {
    error_code == "ASR_OOM"
}

impl WorkUnitStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub owner: String,
    pub expires_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkUnit {
    id: WorkUnitId,
    job_id: JobId,
    stage: StageKind,
    unit_kind: String,
    unit_index: u32,
    input_hash: String,
    status: WorkUnitStatus,
    attempt: u32,
    #[serde(default)]
    oom_strategy_retries: u32,
    lease: Option<Lease>,
    error_code: Option<String>,
    artifact: Option<ArtifactRef>,
}

impl WorkUnit {
    pub fn new(
        id: WorkUnitId,
        job_id: JobId,
        stage: StageKind,
        unit_kind: impl Into<String>,
        unit_index: u32,
        input_hash: impl Into<String>,
    ) -> DomainResult<Self> {
        let input_hash = input_hash.into();
        if input_hash.is_empty() {
            return Err(DomainError::InvalidArgument(
                "work unit input hash cannot be empty".into(),
            ));
        }
        Ok(Self {
            id,
            job_id,
            stage,
            unit_kind: unit_kind.into(),
            unit_index,
            input_hash,
            status: WorkUnitStatus::Pending,
            attempt: 0,
            oom_strategy_retries: 0,
            lease: None,
            error_code: None,
            artifact: None,
        })
    }

    pub fn id(&self) -> &WorkUnitId {
        &self.id
    }

    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    pub fn stage(&self) -> StageKind {
        self.stage
    }

    pub fn unit_kind(&self) -> &str {
        &self.unit_kind
    }

    pub fn unit_index(&self) -> u32 {
        self.unit_index
    }

    pub fn input_hash(&self) -> &str {
        &self.input_hash
    }

    pub fn status(&self) -> WorkUnitStatus {
        self.status
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    pub fn oom_strategy_retries(&self) -> u32 {
        self.oom_strategy_retries
    }

    pub fn error_code(&self) -> Option<&str> {
        self.error_code.as_deref()
    }

    pub fn lease(&self) -> Option<&Lease> {
        self.lease.as_ref()
    }

    /// Whether a recoverable failure may still be automatically retried under
    /// the default two-retry budget. Deterministic errors always return false.
    pub fn may_auto_retry(&self, error_code: &str) -> bool {
        if is_deterministic_work_unit_error(error_code) || is_oom_error(error_code) {
            return false;
        }
        self.attempt < WORK_UNIT_DEFAULT_AUTO_RETRIES
    }

    /// OOM is allowed one strategy-changing retry. GPU→CPU is not implied.
    pub fn may_oom_strategy_retry(&self) -> bool {
        self.oom_strategy_retries < WORK_UNIT_OOM_STRATEGY_RETRIES
    }

    pub fn artifact(&self) -> Option<&ArtifactRef> {
        self.artifact.as_ref()
    }

    pub fn lease_for(
        &mut self,
        owner: impl Into<String>,
        now_ms: u64,
        expires_at_ms: u64,
    ) -> DomainResult<()> {
        if self.status != WorkUnitStatus::Pending {
            return Err(DomainError::IllegalTransition {
                aggregate: "WorkUnit",
                from: format!("{:?}", self.status),
                to: "Running".into(),
            });
        }
        if expires_at_ms <= now_ms {
            return Err(DomainError::InvalidArgument(
                "work unit lease must expire in the future".into(),
            ));
        }
        self.status = WorkUnitStatus::Running;
        self.lease = Some(Lease {
            owner: owner.into(),
            expires_at_ms,
        });
        Ok(())
    }

    pub fn complete(&mut self, artifact: ArtifactRef) -> DomainResult<()> {
        if self.status != WorkUnitStatus::Running {
            return Err(DomainError::IllegalTransition {
                aggregate: "WorkUnit",
                from: format!("{:?}", self.status),
                to: "Done".into(),
            });
        }
        if self.lease.is_none() {
            return Err(DomainError::LeaseRequired);
        }
        if artifact.stage != self.stage {
            return Err(DomainError::InvalidArgument(
                "artifact stage does not match work unit stage".into(),
            ));
        }
        self.status = WorkUnitStatus::Done;
        self.lease = None;
        self.artifact = Some(artifact);
        self.error_code = None;
        Ok(())
    }

    pub fn fail(&mut self, error_code: impl Into<String>) -> DomainResult<()> {
        if self.status != WorkUnitStatus::Running {
            return Err(DomainError::IllegalTransition {
                aggregate: "WorkUnit",
                from: format!("{:?}", self.status),
                to: "Failed".into(),
            });
        }
        self.status = WorkUnitStatus::Failed;
        self.lease = None;
        self.error_code = Some(error_code.into());
        Ok(())
    }

    /// Requeue a running unit after a recoverable failure. Returns `true` when
    /// the unit was returned to Pending for another attempt, or `false` when
    /// the automatic retry budget is exhausted and the unit is Failed.
    pub fn fail_with_auto_retry(&mut self, error_code: impl Into<String>) -> DomainResult<bool> {
        let error_code = error_code.into();
        if self.status != WorkUnitStatus::Running {
            return Err(DomainError::IllegalTransition {
                aggregate: "WorkUnit",
                from: format!("{:?}", self.status),
                to: "auto_retry".into(),
            });
        }
        if self.may_auto_retry(&error_code) {
            self.status = WorkUnitStatus::Pending;
            self.lease = None;
            self.attempt = self.attempt.saturating_add(1);
            self.error_code = Some(error_code);
            Ok(true)
        } else {
            self.fail(error_code)?;
            Ok(false)
        }
    }

    /// Record the single allowed OOM strategy-changing retry and requeue.
    /// Returns `true` when requeued, `false` when the unit is permanently Failed.
    pub fn requeue_after_oom_strategy_change(&mut self) -> DomainResult<bool> {
        if self.status != WorkUnitStatus::Running {
            return Err(DomainError::IllegalTransition {
                aggregate: "WorkUnit",
                from: format!("{:?}", self.status),
                to: "oom_strategy_retry".into(),
            });
        }
        if !self.may_oom_strategy_retry() {
            self.fail("ASR_OOM")?;
            return Ok(false);
        }
        self.oom_strategy_retries = self.oom_strategy_retries.saturating_add(1);
        self.status = WorkUnitStatus::Pending;
        self.lease = None;
        self.attempt = self.attempt.saturating_add(1);
        self.error_code = Some("ASR_OOM".into());
        Ok(true)
    }

    pub fn cancel(&mut self) -> DomainResult<()> {
        if self.status.is_terminal() {
            return Err(DomainError::AlreadyTerminal {
                aggregate: "WorkUnit",
            });
        }
        self.status = WorkUnitStatus::Cancelled;
        self.lease = None;
        Ok(())
    }

    pub fn recover_expired(&mut self, now_ms: u64) -> DomainResult<()> {
        let Some(lease) = &self.lease else {
            return Err(DomainError::LeaseRequired);
        };
        if self.status != WorkUnitStatus::Running || lease.expires_at_ms > now_ms {
            return Err(DomainError::LeaseConflict(
                "work unit lease has not expired".into(),
            ));
        }
        self.status = WorkUnitStatus::Pending;
        self.lease = None;
        self.attempt = self.attempt.saturating_add(1);
        Ok(())
    }

    /// Clear a completed artifact reference when startup verification finds
    /// that the backing file is missing or has a different hash.
    pub fn invalidate_artifact_for_recovery(
        &mut self,
        error_code: impl Into<String>,
    ) -> DomainResult<()> {
        self.status = WorkUnitStatus::Pending;
        self.lease = None;
        self.artifact = None;
        self.error_code = Some(error_code.into());
        Ok(())
    }

    pub fn retry(&mut self) -> DomainResult<()> {
        if !matches!(
            self.status,
            WorkUnitStatus::Failed | WorkUnitStatus::Cancelled
        ) {
            return Err(DomainError::IllegalTransition {
                aggregate: "WorkUnit",
                from: format!("{:?}", self.status),
                to: "Pending".into(),
            });
        }
        self.status = WorkUnitStatus::Pending;
        self.attempt = self.attempt.saturating_add(1);
        self.error_code = None;
        self.artifact = None;
        Ok(())
    }
}
