//! Processing Workflow bounded context.
//!
//! These aggregates express lifecycle rules only. They do not start
//! processes, write files, send HTTP requests, or execute SQL.

use serde::{Deserialize, Serialize};

use crate::error::{DomainError, DomainResult};
use crate::identity::{BatchId, JobId, UlidStr, WorkUnitId};

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
pub enum DomainEvent {
    BatchReachedTerminal {
        batch_id: BatchId,
        status: BatchStatus,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobTerminalStatus {
    Done,
    DoneDegraded,
    Failed,
    Cancelled,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    Done,
    DoneDegraded,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Done | Self::DoneDegraded | Self::Failed | Self::Cancelled
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageKind {
    Probe,
    ExtractAudio,
    Asr,
    Split,
    Correct,
    Translate,
    Export,
}

impl StageKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Probe => "probe",
            Self::ExtractAudio => "extract_audio",
            Self::Asr => "asr",
            Self::Split => "split",
            Self::Correct => "correct",
            Self::Translate => "translate",
            Self::Export => "export",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value.trim().to_ascii_lowercase().as_str() {
            "probe" => Self::Probe,
            "extract_audio" | "extract-audio" => Self::ExtractAudio,
            "asr" => Self::Asr,
            "split" => Self::Split,
            "correct" => Self::Correct,
            "translate" => Self::Translate,
            "export" => Self::Export,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageStatus {
    Pending,
    WaitingResource,
    Running,
    Retrying,
    Done,
    DoneDegraded,
    Failed,
    Skipped,
    Cancelled,
    WaitingProvider,
}

impl StageStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Done | Self::DoneDegraded | Self::Failed | Self::Skipped | Self::Cancelled
        )
    }

    fn prerequisite_satisfied(self) -> bool {
        matches!(self, Self::Done | Self::DoneDegraded | Self::Skipped)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageState {
    pub kind: StageKind,
    pub status: StageStatus,
    pub artifact: Option<ArtifactRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    id: JobId,
    batch_id: Option<BatchId>,
    profile_revision: UlidStr,
    source_path: String,
    stages: Vec<StageState>,
    status: JobStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub id: UlidStr,
    pub stage: StageKind,
    pub path: String,
    pub content_hash: String,
    pub schema_version: u32,
    pub producer_fingerprint: String,
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

    pub fn id(&self) -> &JobId {
        &self.id
    }

    pub fn batch_id(&self) -> Option<&BatchId> {
        self.batch_id.as_ref()
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkUnitStatus {
    Pending,
    Running,
    Done,
    Failed,
    Cancelled,
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

    pub fn lease(&self) -> Option<&Lease> {
        self.lease.as_ref()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::UlidStr;
    use ulid::Ulid;

    fn id() -> UlidStr {
        UlidStr::from(Ulid::new())
    }

    fn profile() -> BatchExecutionProfile {
        BatchExecutionProfile {
            asr_engine: "fake".into(),
            asr_model: "tiny".into(),
            device: "cpu".into(),
            compute_type: "int8".into(),
        }
    }

    #[test]
    fn batch_rejects_model_profile_switch() {
        let job = id();
        let mut batch = Batch::new(id(), vec![job], profile()).unwrap();
        batch.start().unwrap();
        let mut other = profile();
        other.asr_model = "small".into();
        assert_eq!(
            batch.require_profile(&other),
            Err(DomainError::BatchProfileMismatch)
        );
    }

    #[test]
    fn batch_emits_one_terminal_event_and_cannot_restart() {
        let job = id();
        let mut batch = Batch::new(id(), vec![job.clone()], profile()).unwrap();
        batch.start().unwrap();
        batch
            .record_job_terminal(&job, JobTerminalStatus::Done)
            .unwrap();
        let event = batch.finish(BatchStatus::Done).unwrap();
        assert!(matches!(
            event,
            DomainEvent::BatchReachedTerminal {
                status: BatchStatus::Done,
                ..
            }
        ));
        assert!(batch.terminal_event_emitted());
        assert!(batch.start().is_err());
        assert!(batch.finish(BatchStatus::Done).is_err());
    }

    #[test]
    fn job_requires_stage_order_and_artifact_match() {
        let mut job = Job::new(id(), None, id(), "/media/a.wav");
        job.start().unwrap();
        assert!(job.start_stage(StageKind::Asr).is_err());
        job.start_stage(StageKind::Probe).unwrap();
        let artifact = ArtifactRef {
            id: id(),
            stage: StageKind::Probe,
            path: "probe.json".into(),
            content_hash: "h".into(),
            schema_version: 1,
            producer_fingerprint: "test".into(),
        };
        job.complete_stage(StageKind::Probe, artifact, false)
            .unwrap();
        job.start_stage(StageKind::ExtractAudio).unwrap();
    }

    #[test]
    fn retry_resets_failed_stage_and_preserves_prerequisite() {
        let mut job = Job::new(id(), None, id(), "/media/input.wav");
        job.start().unwrap();
        job.start_stage(StageKind::Probe).unwrap();
        job.complete_stage(
            StageKind::Probe,
            ArtifactRef {
                id: id(),
                stage: StageKind::Probe,
                path: "probe.json".into(),
                content_hash: "hash".into(),
                schema_version: 1,
                producer_fingerprint: "test".into(),
            },
            false,
        )
        .unwrap();
        job.start_stage(StageKind::ExtractAudio).unwrap();
        job.fail_stage(StageKind::ExtractAudio).unwrap();
        assert_eq!(job.status(), JobStatus::Failed);
        job.prepare_retry(None).unwrap();
        assert_eq!(job.status(), JobStatus::Pending);
        assert_eq!(job.stages()[0].status, StageStatus::Done);
        assert_eq!(job.stages()[1].status, StageStatus::Pending);
    }

    #[test]
    fn expired_work_unit_returns_to_pending_with_new_attempt() {
        let mut unit = WorkUnit::new(id(), id(), StageKind::Asr, "chunk", 0, "pcm-hash").unwrap();
        unit.lease_for("worker", 10, 20).unwrap();
        unit.recover_expired(20).unwrap();
        assert_eq!(unit.status(), WorkUnitStatus::Pending);
        assert_eq!(unit.attempt(), 1);
        assert!(unit.lease_for("worker-2", 20, 30).is_ok());
    }
}
