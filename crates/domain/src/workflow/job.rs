use super::*;
use crate::error::{DomainError, DomainResult};
use crate::identity::{BatchId, JobId, UlidStr};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    id: JobId,
    batch_id: Option<BatchId>,
    #[serde(default)]
    execution_snapshot_id: Option<UlidStr>,
    profile_revision: UlidStr,
    source_path: String,
    stages: Vec<StageState>,
    status: JobStatus,
}

impl Job {
    pub fn new(
        id: JobId,
        batch_id: Option<BatchId>,
        profile_revision: UlidStr,
        source_path: impl Into<String>,
    ) -> Self {
        let kinds = [
            StageKind::Probe,
            StageKind::ExtractAudio,
            StageKind::Asr,
            StageKind::Split,
            StageKind::Correct,
            StageKind::Translate,
            StageKind::Export,
        ];
        Self {
            id,
            batch_id,
            execution_snapshot_id: None,
            profile_revision,
            source_path: source_path.into(),
            stages: kinds
                .into_iter()
                .map(|kind| StageState {
                    kind,
                    status: StageStatus::Pending,
                    artifact: None,
                })
                .collect(),
            status: JobStatus::Pending,
        }
    }

    pub fn new_with_snapshot(
        id: JobId,
        batch_id: Option<BatchId>,
        execution_snapshot_id: UlidStr,
        profile_revision: UlidStr,
        source_path: impl Into<String>,
    ) -> Self {
        let mut job = Self::new(id, batch_id, profile_revision, source_path);
        job.execution_snapshot_id = Some(execution_snapshot_id);
        job
    }

    pub fn id(&self) -> &JobId {
        &self.id
    }

    pub fn batch_id(&self) -> Option<&BatchId> {
        self.batch_id.as_ref()
    }

    pub fn execution_snapshot_id(&self) -> Option<&UlidStr> {
        self.execution_snapshot_id.as_ref()
    }

    pub fn profile_revision(&self) -> &UlidStr {
        &self.profile_revision
    }

    pub fn source_path(&self) -> &str {
        &self.source_path
    }

    pub fn status(&self) -> JobStatus {
        self.status
    }

    pub fn stages(&self) -> &[StageState] {
        &self.stages
    }

    /// Point the stage at a newly committed transcript revision. The previous
    /// artifact remains immutable and addressable through the artifact store.
    pub fn record_transcript_revision(
        &mut self,
        kind: StageKind,
        artifact: ArtifactRef,
    ) -> DomainResult<()> {
        if artifact.stage != kind {
            return Err(DomainError::InvalidArgument(
                "transcript revision artifact stage does not match".into(),
            ));
        }
        let stage = self.stage_mut(kind)?;
        if !stage.status.is_terminal() {
            return Err(DomainError::IllegalTransition {
                aggregate: "Stage",
                from: format!("{:?}", stage.status),
                to: "record_transcript_revision".into(),
            });
        }
        stage.artifact = Some(artifact);
        Ok(())
    }

    pub fn start(&mut self) -> DomainResult<()> {
        if self.status != JobStatus::Pending {
            return Err(DomainError::IllegalTransition {
                aggregate: "Job",
                from: format!("{:?}", self.status),
                to: "Running".into(),
            });
        }
        self.status = JobStatus::Running;
        Ok(())
    }

    pub fn start_stage(&mut self, kind: StageKind) -> DomainResult<()> {
        if self.status != JobStatus::Running {
            return Err(DomainError::IllegalTransition {
                aggregate: "Job",
                from: format!("{:?}", self.status),
                to: format!("start_{kind:?}"),
            });
        }
        let index = self.stage_index(kind)?;
        if index > 0 && !self.stages[index - 1].status.prerequisite_satisfied() {
            return Err(DomainError::InvalidArgument(format!(
                "stage {kind:?} prerequisite is not complete"
            )));
        }
        let stage = &mut self.stages[index];
        if stage.status != StageStatus::Pending {
            return Err(DomainError::IllegalTransition {
                aggregate: "Stage",
                from: format!("{:?}", stage.status),
                to: "Running".into(),
            });
        }
        stage.status = StageStatus::Running;
        Ok(())
    }

    pub fn complete_stage(
        &mut self,
        kind: StageKind,
        artifact: ArtifactRef,
        degraded: bool,
    ) -> DomainResult<()> {
        let index = self.stage_index(kind)?;
        let stage = &mut self.stages[index];
        if stage.status != StageStatus::Running {
            return Err(DomainError::IllegalTransition {
                aggregate: "Stage",
                from: format!("{:?}", stage.status),
                to: "Done".into(),
            });
        }
        if artifact.stage != kind {
            return Err(DomainError::InvalidArgument(
                "artifact stage does not match the completed stage".into(),
            ));
        }
        stage.status = if degraded {
            StageStatus::DoneDegraded
        } else {
            StageStatus::Done
        };
        stage.artifact = Some(artifact);
        Ok(())
    }

    pub fn skip_stage(&mut self, kind: StageKind) -> DomainResult<()> {
        if self.status != JobStatus::Running {
            return Err(DomainError::IllegalTransition {
                aggregate: "Job",
                from: format!("{:?}", self.status),
                to: format!("skip_{kind:?}"),
            });
        }
        let index = self.stage_index(kind)?;
        if index > 0 && !self.stages[index - 1].status.prerequisite_satisfied() {
            return Err(DomainError::InvalidArgument(format!(
                "stage {kind:?} prerequisite is not complete"
            )));
        }
        let stage = &mut self.stages[index];
        if stage.status != StageStatus::Pending {
            return Err(DomainError::IllegalTransition {
                aggregate: "Stage",
                from: format!("{:?}", stage.status),
                to: "Skipped".into(),
            });
        }
        stage.status = StageStatus::Skipped;
        Ok(())
    }

    pub fn fail_stage(&mut self, kind: StageKind) -> DomainResult<()> {
        let stage = self.stage_mut(kind)?;
        if !matches!(stage.status, StageStatus::Running | StageStatus::Retrying) {
            return Err(DomainError::IllegalTransition {
                aggregate: "Stage",
                from: format!("{:?}", stage.status),
                to: "Failed".into(),
            });
        }
        stage.status = StageStatus::Failed;
        self.status = JobStatus::Failed;
        Ok(())
    }

    pub fn finish(&mut self) -> DomainResult<()> {
        if self.status != JobStatus::Running {
            return Err(DomainError::IllegalTransition {
                aggregate: "Job",
                from: format!("{:?}", self.status),
                to: "terminal".into(),
            });
        }
        if self.stages.iter().any(|stage| !stage.status.is_terminal()) {
            return Err(DomainError::InvalidArgument(
                "all job stages must be terminal before the job finishes".into(),
            ));
        }
        if self
            .stages
            .iter()
            .any(|stage| stage.status == StageStatus::Failed)
        {
            self.status = JobStatus::Failed;
        } else if self
            .stages
            .iter()
            .any(|stage| stage.status == StageStatus::DoneDegraded)
        {
            self.status = JobStatus::DoneDegraded;
        } else if self
            .stages
            .iter()
            .any(|stage| stage.status == StageStatus::Cancelled)
        {
            self.status = JobStatus::Cancelled;
        } else {
            self.status = JobStatus::Done;
        }
        Ok(())
    }

    /// Convert an in-flight Job into a restartable state after process death.
    /// Completed stages and their immutable artifact references are retained.
    pub fn recover_after_restart(&mut self) -> DomainResult<()> {
        if self.status != JobStatus::Running {
            return Ok(());
        }
        for stage in &mut self.stages {
            if matches!(stage.status, StageStatus::Running | StageStatus::Retrying) {
                stage.status = StageStatus::Pending;
            }
        }
        self.status = JobStatus::Pending;
        Ok(())
    }

    /// Invalidate this stage and every dependent stage when its committed
    /// artifact cannot be verified during startup recovery.
    pub fn invalidate_stage_for_recovery(&mut self, kind: StageKind) -> DomainResult<()> {
        let start_index = self.stage_index(kind)?;
        for stage in self.stages.iter_mut().skip(start_index) {
            stage.status = StageStatus::Pending;
            stage.artifact = None;
        }
        self.status = JobStatus::Pending;
        Ok(())
    }

    pub fn cancel(&mut self) -> DomainResult<()> {
        if self.status.is_terminal() {
            return Err(DomainError::AlreadyTerminal { aggregate: "Job" });
        }
        self.status = JobStatus::Cancelled;
        for stage in &mut self.stages {
            if !stage.status.is_terminal() {
                stage.status = StageStatus::Cancelled;
            }
        }
        Ok(())
    }

    /// Prepare an explicit retry without allowing a terminal job to silently
    /// return to the running state. Completed prerequisite stages remain
    /// reusable; the selected stage and all later stages are invalidated.
    pub fn prepare_retry(&mut self, from_stage: Option<StageKind>) -> DomainResult<()> {
        if !matches!(
            self.status,
            JobStatus::Failed | JobStatus::DoneDegraded | JobStatus::Cancelled
        ) {
            return Err(DomainError::IllegalTransition {
                aggregate: "Job",
                from: format!("{:?}", self.status),
                to: "retry".into(),
            });
        }
        let start_index = match from_stage {
            Some(kind) => self.stage_index(kind)?,
            None => self
                .stages
                .iter()
                .position(|stage| {
                    matches!(
                        stage.status,
                        StageStatus::Failed | StageStatus::Cancelled | StageStatus::WaitingProvider
                    )
                })
                .unwrap_or(0),
        };
        for stage in self.stages.iter_mut().skip(start_index) {
            stage.status = StageStatus::Pending;
            stage.artifact = None;
        }
        self.status = JobStatus::Pending;
        Ok(())
    }

    fn stage_index(&self, kind: StageKind) -> DomainResult<usize> {
        self.stages
            .iter()
            .position(|stage| stage.kind == kind)
            .ok_or_else(|| DomainError::MemberNotFound {
                aggregate: "Job",
                id: format!("stage::{kind:?}"),
            })
    }

    fn stage_mut(&mut self, kind: StageKind) -> DomainResult<&mut StageState> {
        let index = self.stage_index(kind)?;
        Ok(&mut self.stages[index])
    }
}
