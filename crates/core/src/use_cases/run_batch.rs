//! FIFO Batch orchestration and Batch-scoped ASR session lifetime.

use std::sync::Arc;

use videocaptionerr_contracts::error::VcError;
use videocaptionerr_domain::{
    Batch, BatchExecutionProfile, BatchStatus, JobStatus, JobTerminalStatus,
};

use crate::application_error::{AppResult, ApplicationError};
use crate::ports::{
    ActiveRunRegistry, AsrRuntimeResolver, AsrRuntimeSpec, BatchRepository, EventPublisher,
    JobRepository, RunControl, Versioned,
};
use crate::use_cases::{TranscribeJob, TranscribeJobCommand, TranscribeJobResponse};

pub struct RunBatchCommand {
    pub batch: Batch,
    pub jobs: Vec<TranscribeJobCommand>,
    /// Durable runtime identity resolved once per Batch (load once / unload once).
    pub asr_spec: AsrRuntimeSpec,
}

pub struct RunBatchResponse {
    pub batch: Batch,
    pub jobs: Vec<TranscribeJobResponse>,
    pub failed_job_ids: Vec<String>,
    pub failures: Vec<RunBatchFailure>,
}

pub struct RunBatchFailure {
    pub job_id: String,
    pub error: VcError,
}

pub struct RunBatch {
    batches: Arc<dyn BatchRepository>,
    jobs: Arc<dyn JobRepository>,
    resolver: Arc<dyn AsrRuntimeResolver>,
    transcribe: Arc<TranscribeJob>,
    events: Arc<dyn EventPublisher>,
    active_runs: Option<Arc<dyn ActiveRunRegistry>>,
}

impl RunBatch {
    pub fn new(
        batches: Arc<dyn BatchRepository>,
        jobs: Arc<dyn JobRepository>,
        resolver: Arc<dyn AsrRuntimeResolver>,
        transcribe: Arc<TranscribeJob>,
        events: Arc<dyn EventPublisher>,
    ) -> Self {
        Self {
            batches,
            jobs,
            resolver,
            transcribe,
            events,
            active_runs: None,
        }
    }

    pub fn with_active_runs(mut self, active_runs: Arc<dyn ActiveRunRegistry>) -> Self {
        self.active_runs = Some(active_runs);
        self
    }

    pub async fn execute(&self, command: RunBatchCommand) -> AppResult<RunBatchResponse> {
        let requested_batch = command.batch;
        validate_commands(&requested_batch, &command.jobs)?;
        command
            .asr_spec
            .validate()
            .map_err(ApplicationError::Invalid)?;
        let requested_profile = BatchExecutionProfile {
            asr_engine: command.asr_spec.engine_family.clone(),
            asr_model: command.asr_spec.model_id.clone(),
            device: command.asr_spec.device.clone(),
            compute_type: command.asr_spec.compute_type.clone(),
        };
        let mut batch = self
            .batches
            .load_batch(requested_batch.id())
            .await?
            .ok_or_else(|| {
                ApplicationError::Invalid(format!(
                    "Batch {} must be persisted before execution",
                    requested_batch.id()
                ))
            })?;
        if batch.value != requested_batch {
            return Err(ApplicationError::Invalid(format!(
                "Batch {} does not match its persisted identity",
                requested_batch.id()
            )));
        }
        batch.value.require_profile(&requested_profile)?;
        for command in &command.jobs {
            self.jobs.load_job(&command.job_id).await?.ok_or_else(|| {
                ApplicationError::Invalid(format!(
                    "Job {} must be persisted before Batch execution",
                    command.job_id
                ))
            })?;
        }

        if batch.status() == BatchStatus::Pending {
            batch.start()?;
            self.save_batch(&mut batch).await?;
        } else if batch.status().is_terminal() {
            return Err(ApplicationError::Invalid(format!(
                "Batch {} is already {:?}",
                batch.id(),
                batch.status()
            )));
        }

        // Resolve + open once outside the per-Job loop. The worker/model stays
        // alive until the Batch reaches a terminal state.
        let runtime = match self.resolver.resolve(&command.asr_spec).await {
            Ok(runtime) => runtime,
            Err(error) => {
                self.fail_before_session(batch, &command.jobs).await?;
                return Err(error);
            }
        };
        let mut session = match runtime.open_session(batch.execution_profile()).await {
            Ok(session) => session,
            Err(error) => {
                self.fail_before_session(batch, &command.jobs).await?;
                return Err(error);
            }
        };

        let result = self
            .execute_with_session(batch, command.jobs, session.as_mut())
            .await;
        if let Err(error) = session.close().await {
            // The Batch/Job result is already the durable business outcome.
            // Cleanup diagnostics must never turn a successful Batch into a
            // business failure (nor hide the original execution error).
            tracing::warn!(error = %error, "ASR session close failed after Batch execution");
        }
        result
    }

    async fn execute_with_session(
        &self,
        mut batch: Versioned<Batch>,
        commands: Vec<TranscribeJobCommand>,
        session: &mut dyn crate::ports::AsrSession,
    ) -> AppResult<RunBatchResponse> {
        let mut responses = Vec::new();
        let mut failed_job_ids = Vec::new();
        let mut failures = Vec::new();

        let mut next_index = 0usize;
        let mut wait_control: Option<(videocaptionerr_domain::JobId, RunControl)> = None;
        while next_index < commands.len() {
            // Persistent state is reloaded at every safe boundary. The
            // in-process signal is only a latency optimization.
            if let Some(latest) = self.batches.load_batch(batch.id()).await? {
                batch = latest;
            }

            if batch.status().is_terminal() {
                // Another control path may have completed the aggregate while
                // this owner was inside an adapter. The durable terminal
                // result is authoritative; do not replay member transitions.
                break;
            }

            if batch.cancel_requested() {
                if let Some((job_id, _)) = wait_control.take() {
                    self.unregister_run(&job_id);
                }
                // A control process may have already terminalized every Job
                // and the Batch while this owner was waiting. That durable
                // terminal state is authoritative and must not be replayed.
                if !batch.status().is_terminal() {
                    self.cancel_remaining_jobs(&mut batch, &commands, next_index)
                        .await?;
                }
                break;
            }

            if batch.pause_requested() || batch.status() == BatchStatus::Paused {
                if batch.status() == BatchStatus::Running {
                    batch.mark_paused()?;
                    self.save_batch(&mut batch).await?;
                }
                let (job_id, control) = match wait_control.take() {
                    Some(existing) => existing,
                    None => {
                        let job_id = commands[next_index].job_id.clone();
                        let control = RunControl::new();
                        self.register_run(
                            job_id.clone(),
                            Some(batch.id().clone()),
                            control.clone(),
                        )?;
                        (job_id, control)
                    }
                };
                tokio::select! {
                    _ = control.wait() => {},
                    _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {},
                }
                wait_control = Some((job_id, control));
                continue;
            }

            if let Some((job_id, _)) = wait_control.take() {
                self.unregister_run(&job_id);
            }

            let job_command = commands[next_index].clone();
            let job_id = job_command.job_id.clone();
            // A resumed owner receives the original full command list. Already
            // terminal Jobs are acknowledged and never executed a second time.
            if let Some(existing) = self.jobs.load_job(&job_id).await? {
                if existing.status().is_terminal() {
                    if !batch.has_terminal_record(&job_id) {
                        batch.record_job_terminal(&job_id, terminal_status(existing.status()))?;
                        self.save_batch(&mut batch).await?;
                    }
                    next_index += 1;
                    continue;
                }
            }

            let control = RunControl::new();
            self.register_run(job_id.clone(), Some(batch.id().clone()), control.clone())?;
            let result = self
                .transcribe
                .execute_with_cancel(job_command, session, control.cancellation_token())
                .await;
            self.unregister_run(&job_id);

            // A control command may have changed Batch state while the Job was
            // inside an adapter. Refresh before recording the terminal member.
            if let Some(latest) = self.batches.load_batch(batch.id()).await? {
                batch = latest;
            }
            match result {
                Ok(response) => {
                    let terminal = terminal_status(response.job.status());
                    if !batch.status().is_terminal() {
                        batch.record_job_terminal(&job_id, terminal)?;
                        self.save_batch(&mut batch).await?;
                    }
                    responses.push(response);
                }
                Err(error) => {
                    let vc_error = error.into_vc_error();
                    let terminal = self
                        .jobs
                        .load_job(&job_id)
                        .await?
                        .map(|job| terminal_status(job.status()))
                        .unwrap_or(JobTerminalStatus::Failed);
                    if vc_error.code != videocaptionerr_contracts::error::ErrorCode::Cancelled {
                        let job_id_string = job_id.to_string();
                        failed_job_ids.push(job_id_string.clone());
                        failures.push(RunBatchFailure {
                            job_id: job_id_string,
                            error: vc_error,
                        });
                    }
                    if !batch.status().is_terminal() {
                        batch.record_job_terminal(&job_id, terminal)?;
                        self.save_batch(&mut batch).await?;
                    }
                }
            }
            next_index += 1;
        }

        let all_terminal = batch
            .job_ids()
            .iter()
            .all(|id| batch.has_terminal_record(id));
        if !all_terminal {
            return Ok(RunBatchResponse {
                batch: batch.value.clone(),
                jobs: responses,
                failed_job_ids,
                failures,
            });
        }
        // CancelBatch may have completed the aggregate while this owner was
        // inside a safe-point wait. Do not call finish twice; cleanup still
        // happens in the caller and the persisted terminal result wins.
        if batch.status().is_terminal() {
            return Ok(RunBatchResponse {
                batch: batch.value.clone(),
                jobs: responses,
                failed_job_ids,
                failures,
            });
        }
        if batch.status() == BatchStatus::Paused {
            batch.resume()?;
        }
        let any_job_failed = self
            .jobs
            .list_jobs()
            .await?
            .into_iter()
            .filter(|job| batch.job_ids().iter().any(|id| id == job.id()))
            .any(|job| job.status() == JobStatus::Failed);
        let final_status = if batch.cancel_requested() {
            BatchStatus::Cancelled
        } else if !any_job_failed {
            BatchStatus::Done
        } else {
            BatchStatus::Failed
        };
        let event = batch.finish(final_status)?;
        self.save_batch(&mut batch).await?;
        // Business state is committed above. Event delivery is deliberately
        // non-fatal so a live publisher outage cannot report a committed Batch
        // as failed. The production publisher writes the durable outbox.
        let _ = self.events.publish(event).await;

        Ok(RunBatchResponse {
            batch: batch.value,
            jobs: responses,
            failed_job_ids,
            failures,
        })
    }

    async fn save_batch(&self, batch: &mut Versioned<Batch>) -> AppResult<()> {
        let expected = batch.expected_version();
        self.batches.save_batch(batch, expected).await
    }

    fn register_run(
        &self,
        job_id: videocaptionerr_domain::JobId,
        batch_id: Option<videocaptionerr_domain::BatchId>,
        control: RunControl,
    ) -> AppResult<()> {
        if let Some(registry) = &self.active_runs {
            registry.register(job_id, batch_id, control)?;
        }
        Ok(())
    }

    fn unregister_run(&self, job_id: &videocaptionerr_domain::JobId) {
        if let Some(registry) = &self.active_runs {
            registry.unregister(job_id);
        }
    }

    async fn cancel_remaining_jobs(
        &self,
        batch: &mut Versioned<Batch>,
        commands: &[TranscribeJobCommand],
        start: usize,
    ) -> AppResult<()> {
        for command in commands.iter().skip(start) {
            let Some(mut job) = self.jobs.load_job(&command.job_id).await? else {
                continue;
            };
            if !job.status().is_terminal() {
                job.cancel()?;
                self.save_job(&mut job).await?;
            }
            if !batch.has_terminal_record(&command.job_id) {
                batch.record_job_terminal(&command.job_id, terminal_status(job.status()))?;
            }
        }
        self.save_batch(batch).await
    }

    async fn save_job(&self, job: &mut Versioned<videocaptionerr_domain::Job>) -> AppResult<()> {
        let expected = job.expected_version();
        self.jobs.save_job(job, expected).await
    }

    async fn fail_before_session(
        &self,
        mut batch: Versioned<Batch>,
        commands: &[TranscribeJobCommand],
    ) -> AppResult<()> {
        if batch.status() == BatchStatus::Pending {
            batch.start()?;
        } else if batch.status() == BatchStatus::Paused {
            batch.resume()?;
        }
        for command in commands {
            if let Some(mut job) = self.jobs.load_job(&command.job_id).await? {
                if !job.status().is_terminal() {
                    job.fail()?;
                    self.save_job(&mut job).await?;
                    batch.record_job_terminal(&command.job_id, JobTerminalStatus::Failed)?;
                }
            }
        }
        if batch
            .job_ids()
            .iter()
            .all(|id| batch.has_terminal_record(id))
        {
            batch.finish(BatchStatus::Failed)?;
        }
        self.save_batch(&mut batch).await
    }
}

fn validate_commands(batch: &Batch, commands: &[TranscribeJobCommand]) -> AppResult<()> {
    if commands.is_empty() {
        return Err(ApplicationError::Invalid(
            "Batch execution requires at least one Job command".into(),
        ));
    }
    if commands.len() == batch.job_ids().len() {
        // Fresh Batch: every member must be supplied in FIFO aggregate order.
        for (expected, command) in batch.job_ids().iter().zip(commands) {
            if expected != &command.job_id {
                return Err(ApplicationError::Invalid(
                    "Batch jobs must be supplied in FIFO aggregate order".into(),
                ));
            }
            if command.batch_id.as_ref() != Some(batch.id()) {
                return Err(ApplicationError::Invalid(
                    "Job command does not belong to the Batch".into(),
                ));
            }
        }
        return Ok(());
    }

    // Retry invocation: a legal subset may run while unselected members stay
    // terminal. Selected jobs must appear in Batch membership order.
    let mut membership = batch.job_ids().iter().peekable();
    for command in commands {
        if command.batch_id.as_ref() != Some(batch.id()) {
            return Err(ApplicationError::Invalid(
                "Job command does not belong to the Batch".into(),
            ));
        }
        loop {
            let Some(candidate) = membership.next() else {
                return Err(ApplicationError::Invalid(
                    "retry subset contains a Job outside Batch membership order".into(),
                ));
            };
            if candidate == &command.job_id {
                break;
            }
            if !batch.has_terminal_record(candidate) {
                return Err(ApplicationError::Invalid(format!(
                    "unselected Batch Job {candidate} must already be terminal before a subset retry"
                )));
            }
        }
    }
    for remaining in membership {
        if !batch.has_terminal_record(remaining) {
            return Err(ApplicationError::Invalid(format!(
                "unselected Batch Job {remaining} must already be terminal before a subset retry"
            )));
        }
    }
    Ok(())
}

fn terminal_status(status: JobStatus) -> JobTerminalStatus {
    match status {
        JobStatus::Done => JobTerminalStatus::Done,
        JobStatus::DoneDegraded => JobTerminalStatus::DoneDegraded,
        JobStatus::Failed => JobTerminalStatus::Failed,
        JobStatus::Cancelled => JobTerminalStatus::Cancelled,
        JobStatus::Pending | JobStatus::Running => JobTerminalStatus::Failed,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use tokio::sync::Notify;
    use ulid::Ulid;
    use videocaptionerr_contracts::error::{ErrorCode, VcError};
    use videocaptionerr_contracts::media::{AudioStream, MediaProbe};
    use videocaptionerr_domain::{
        ArtifactRef, BatchExecutionProfile, EngineFingerprint, Job, JobId, StageKind, Transcript,
        UlidStr, Word, PROB_UNAVAILABLE, SCHEMA_VERSION,
    };

    use super::*;
    use crate::application_error::AppResult;
    use crate::ports::{
        ArtifactCommit, ArtifactInput, ArtifactStore, AsrDescriptor, AsrRuntime,
        AsrRuntimeResolver, AsrRuntimeSpec, AsrSession, AsrTranscribeRequest, AudioExtraction,
        EventPublisher, ExpectedVersion, ExportedSubtitle, ExtractAudioRequest, IdGenerator,
        JobRepository, MediaGateway, ModelLocator, NormalizedAsrResult, ProbeMediaRequest,
        ProbedMedia, StageCommitRepository, StageCommitRequest, StageCommitResult,
        SubtitleExportRequest, SubtitleGateway, Versioned,
    };

    struct Ids;

    impl IdGenerator for Ids {
        fn next_id(&self) -> UlidStr {
            Ulid::new().into()
        }
    }

    struct Events;

    #[async_trait]
    impl EventPublisher for Events {
        async fn publish(&self, _event: videocaptionerr_domain::DomainEvent) -> AppResult<()> {
            Ok(())
        }
    }

    struct FailingEvents;

    #[async_trait]
    impl EventPublisher for FailingEvents {
        async fn publish(&self, _event: videocaptionerr_domain::DomainEvent) -> AppResult<()> {
            Err(ApplicationError::Invalid(
                "event publisher unavailable".into(),
            ))
        }
    }

    struct StageCommits;

    #[async_trait]
    impl StageCommitRepository for StageCommits {
        async fn commit_stage(&self, request: StageCommitRequest) -> AppResult<StageCommitResult> {
            Ok(StageCommitResult {
                job: request.job.map(|(job, expected)| {
                    Versioned::with_version(job.value, next_version(job.version, expected))
                }),
                work_unit: request.work_unit.map(|(unit, expected)| {
                    Versioned::with_version(unit.value, next_version(unit.version, expected))
                }),
            })
        }
    }

    fn next_version(current: u64, expected: ExpectedVersion) -> u64 {
        match expected {
            ExpectedVersion::New => 1,
            ExpectedVersion::Exact(version) => current.max(version).saturating_add(1),
        }
    }

    struct Jobs {
        values: Mutex<HashMap<String, Job>>,
    }

    #[async_trait]
    impl JobRepository for Jobs {
        async fn load_job(&self, id: &JobId) -> AppResult<Option<Versioned<Job>>> {
            Ok(self
                .values
                .lock()
                .unwrap()
                .get(id.as_str())
                .cloned()
                .map(|job| Versioned::with_version(job, 1)))
        }

        async fn save_job(
            &self,
            job: &mut Versioned<Job>,
            _expected: ExpectedVersion,
        ) -> AppResult<()> {
            self.values
                .lock()
                .unwrap()
                .insert(job.id().to_string(), job.value.clone());
            job.version = job.version.saturating_add(1).max(1);
            Ok(())
        }

        async fn delete_job(&self, _id: &JobId) -> AppResult<()> {
            Ok(())
        }

        async fn list_jobs(&self) -> AppResult<Vec<Versioned<Job>>> {
            Ok(self
                .values
                .lock()
                .unwrap()
                .values()
                .cloned()
                .map(|job| Versioned::with_version(job, 1))
                .collect())
        }
    }

    struct Batches {
        values: Mutex<Vec<Batch>>,
    }

    #[async_trait]
    impl BatchRepository for Batches {
        async fn load_batch(
            &self,
            _id: &videocaptionerr_domain::BatchId,
        ) -> AppResult<Option<Versioned<Batch>>> {
            Ok(self
                .values
                .lock()
                .unwrap()
                .last()
                .cloned()
                .map(|batch| Versioned::with_version(batch, 1)))
        }

        async fn list_batches(&self) -> AppResult<Vec<Versioned<Batch>>> {
            Ok(self
                .values
                .lock()
                .unwrap()
                .iter()
                .cloned()
                .map(|batch| Versioned::with_version(batch, 1))
                .collect())
        }

        async fn save_batch(
            &self,
            batch: &mut Versioned<Batch>,
            _expected: ExpectedVersion,
        ) -> AppResult<()> {
            self.values.lock().unwrap().push(batch.value.clone());
            batch.version = batch.version.saturating_add(1).max(1);
            Ok(())
        }
    }

    struct Media;

    fn input_artifact(stage: StageKind, name: &str) -> ArtifactInput {
        ArtifactInput {
            stage,
            path: PathBuf::from(name),
            content_hash: format!("hash-{name}"),
            schema_version: SCHEMA_VERSION,
            producer_fingerprint: "test".into(),
        }
    }

    #[async_trait]
    impl MediaGateway for Media {
        async fn probe(&self, _request: ProbeMediaRequest) -> AppResult<ProbedMedia> {
            Ok(ProbedMedia {
                probe: MediaProbe {
                    schema_version: SCHEMA_VERSION,
                    input_size: 1,
                    container: Some("wav".into()),
                    duration_ms: 500,
                    audio_streams: vec![AudioStream {
                        stream_index: 0,
                        codec: "pcm_s16le".into(),
                        language: Some("en".into()),
                        title: None,
                        channels: 1,
                        sample_rate: 16_000,
                        is_default: true,
                    }],
                },
                artifact: input_artifact(StageKind::Probe, "probe.json"),
            })
        }

        async fn media_hash(&self, _input: &Path) -> AppResult<String> {
            Ok("media-hash".into())
        }

        async fn extract_audio(&self, request: ExtractAudioRequest) -> AppResult<AudioExtraction> {
            Ok(AudioExtraction {
                wav_path: request.job_dir.join("audio.wav"),
                pcm_hash: "pcm-hash".into(),
                artifact: input_artifact(StageKind::ExtractAudio, "audio.wav"),
            })
        }
    }

    struct Artifacts;

    #[async_trait]
    impl ArtifactStore for Artifacts {
        async fn commit(&self, _commit: ArtifactCommit) -> AppResult<()> {
            Ok(())
        }

        async fn commit_transcript(
            &self,
            commit: crate::ports::TranscriptCommit,
        ) -> AppResult<ArtifactRef> {
            Ok(ArtifactRef {
                id: commit.artifact_id,
                stage: commit.stage,
                path: commit.path.to_string_lossy().into_owned(),
                content_hash: "transcript-hash".into(),
                schema_version: SCHEMA_VERSION,
                producer_fingerprint: commit.producer_fingerprint,
            })
        }

        async fn load_transcript(
            &self,
            _artifact: &ArtifactRef,
        ) -> AppResult<videocaptionerr_domain::Transcript> {
            Err(ApplicationError::Invalid(
                "fake store cannot load transcripts".into(),
            ))
        }

        async fn load_probe_manifest(
            &self,
            _artifact: &ArtifactRef,
        ) -> AppResult<crate::artifacts::ProbeManifest> {
            Err(ApplicationError::Invalid(
                "fake store cannot load probe manifests".into(),
            ))
        }

        async fn load_extract_manifest(
            &self,
            _artifact: &ArtifactRef,
        ) -> AppResult<crate::artifacts::ExtractManifest> {
            Err(ApplicationError::Invalid(
                "fake store cannot load extract manifests".into(),
            ))
        }

        async fn validate(&self, _artifact: &ArtifactRef) -> AppResult<()> {
            Ok(())
        }
    }

    struct Subtitles;

    #[async_trait]
    impl SubtitleGateway for Subtitles {
        async fn export(
            &self,
            _transcript: &Transcript,
            request: SubtitleExportRequest,
        ) -> AppResult<ExportedSubtitle> {
            Ok(ExportedSubtitle {
                path: request.output_path,
                content_hash: "export-hash".into(),
            })
        }
    }

    struct Session {
        descriptor: AsrDescriptor,
        close_count: Arc<AtomicUsize>,
        fail_first: bool,
        fail_close: bool,
        calls: usize,
    }

    #[async_trait]
    impl AsrSession for Session {
        fn descriptor(&self) -> &AsrDescriptor {
            &self.descriptor
        }

        async fn transcribe(
            &mut self,
            _request: AsrTranscribeRequest,
            _events: &dyn EventPublisher,
            _cancel: Option<crate::ports::AsrCancelToken>,
        ) -> AppResult<NormalizedAsrResult> {
            self.calls += 1;
            if self.fail_first && self.calls == 1 {
                return Err(ApplicationError::Adapter(VcError::new(
                    ErrorCode::AsrFailed,
                    "injected ASR failure",
                )));
            }
            Ok(NormalizedAsrResult {
                transcript: Transcript::new_asr(
                    "source",
                    EngineFingerprint::unknown(),
                    vec![
                        Word {
                            text: "hello".into(),
                            start_ms: 0,
                            end_ms: 200,
                            prob: 0.9,
                        },
                        Word {
                            text: "world".into(),
                            start_ms: 220,
                            end_ms: 500,
                            prob: PROB_UNAVAILABLE,
                        },
                    ],
                ),
            })
        }

        async fn close(self: Box<Self>) -> AppResult<()> {
            self.close_count.fetch_add(1, Ordering::SeqCst);
            if self.fail_close {
                Err(ApplicationError::Adapter(VcError::new(
                    ErrorCode::Internal,
                    "injected session cleanup failure",
                )))
            } else {
                Ok(())
            }
        }
    }

    struct Runtime {
        opens: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
        fail_first: bool,
    }

    struct GateState {
        started: Notify,
        release: Notify,
        calls: AtomicUsize,
        closes: AtomicUsize,
    }

    struct GateSession {
        descriptor: AsrDescriptor,
        state: Arc<GateState>,
    }

    #[async_trait]
    impl AsrSession for GateSession {
        fn descriptor(&self) -> &AsrDescriptor {
            &self.descriptor
        }

        async fn transcribe(
            &mut self,
            _request: AsrTranscribeRequest,
            _events: &dyn EventPublisher,
            _cancel: Option<crate::ports::AsrCancelToken>,
        ) -> AppResult<NormalizedAsrResult> {
            self.state.calls.fetch_add(1, Ordering::SeqCst);
            self.state.started.notify_one();
            self.state.release.notified().await;
            Ok(NormalizedAsrResult {
                transcript: Transcript::new_asr(
                    "source",
                    EngineFingerprint::unknown(),
                    vec![
                        Word {
                            text: "hello".into(),
                            start_ms: 0,
                            end_ms: 200,
                            prob: 0.9,
                        },
                        Word {
                            text: "world".into(),
                            start_ms: 220,
                            end_ms: 500,
                            prob: PROB_UNAVAILABLE,
                        },
                    ],
                ),
            })
        }

        async fn close(self: Box<Self>) -> AppResult<()> {
            self.state.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct GateRuntime {
        opens: Arc<AtomicUsize>,
        state: Arc<GateState>,
    }

    #[async_trait]
    impl AsrRuntime for GateRuntime {
        async fn open_session(
            &self,
            _profile: &BatchExecutionProfile,
        ) -> AppResult<Box<dyn AsrSession>> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(GateSession {
                descriptor: AsrDescriptor {
                    engine_id: "fake-gated".into(),
                    adapter_version: "test".into(),
                    runtime_version: "test".into(),
                    fingerprint: "fake-gated|test|test|fake|cpu".into(),
                    supports_word_timestamps: true,
                    supports_confidence: true,
                    cooperative_cancel: true,
                    max_audio_secs: Some(3600),
                },
                state: self.state.clone(),
            }))
        }
    }

    struct GateResolver {
        opens: Arc<AtomicUsize>,
        state: Arc<GateState>,
    }

    #[async_trait]
    impl AsrRuntimeResolver for GateResolver {
        async fn resolve(&self, _spec: &AsrRuntimeSpec) -> AppResult<Box<dyn AsrRuntime>> {
            Ok(Box::new(GateRuntime {
                opens: self.opens.clone(),
                state: self.state.clone(),
            }))
        }
    }

    #[async_trait]
    impl AsrRuntime for Runtime {
        async fn open_session(
            &self,
            _profile: &BatchExecutionProfile,
        ) -> AppResult<Box<dyn AsrSession>> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Session {
                descriptor: AsrDescriptor {
                    engine_id: "fake".into(),
                    adapter_version: "test".into(),
                    runtime_version: "test".into(),
                    fingerprint: "fake|test|test|fake|cpu".into(),
                    supports_word_timestamps: true,
                    supports_confidence: true,
                    cooperative_cancel: true,
                    max_audio_secs: Some(3600),
                },
                close_count: self.closes.clone(),
                fail_first: self.fail_first,
                fail_close: false,
                calls: 0,
            }))
        }
    }

    struct Resolver(Runtime);

    #[async_trait]
    impl AsrRuntimeResolver for Resolver {
        async fn resolve(&self, _spec: &AsrRuntimeSpec) -> AppResult<Box<dyn AsrRuntime>> {
            Ok(Box::new(Runtime {
                opens: self.0.opens.clone(),
                closes: self.0.closes.clone(),
                fail_first: self.0.fail_first,
            }))
        }
    }

    struct CleanupFailResolver {
        closes: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl AsrRuntimeResolver for CleanupFailResolver {
        async fn resolve(&self, _spec: &AsrRuntimeSpec) -> AppResult<Box<dyn AsrRuntime>> {
            Ok(Box::new(CleanupFailRuntime {
                closes: self.closes.clone(),
            }))
        }
    }

    struct CleanupFailRuntime {
        closes: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl AsrRuntime for CleanupFailRuntime {
        async fn open_session(
            &self,
            _profile: &BatchExecutionProfile,
        ) -> AppResult<Box<dyn AsrSession>> {
            Ok(Box::new(Session {
                descriptor: AsrDescriptor {
                    engine_id: "fake-cleanup".into(),
                    adapter_version: "test".into(),
                    runtime_version: "test".into(),
                    fingerprint: "fake-cleanup|test|test|fake|cpu".into(),
                    supports_word_timestamps: true,
                    supports_confidence: true,
                    cooperative_cancel: true,
                    max_audio_secs: Some(3600),
                },
                close_count: self.closes.clone(),
                fail_first: false,
                fail_close: true,
                calls: 0,
            }))
        }
    }

    fn profile() -> BatchExecutionProfile {
        BatchExecutionProfile {
            asr_engine: "fake".into(),
            asr_model: "fake".into(),
            device: "cpu".into(),
            compute_type: "default".into(),
        }
    }

    fn asr_spec() -> AsrRuntimeSpec {
        AsrRuntimeSpec {
            engine_family: "fake".into(),
            model_id: "fake".into(),
            verified_digest: None,
            locator: ModelLocator::file("fake:default"),
            device: "cpu".into(),
            compute_type: "default".into(),
        }
    }

    fn command(
        batch_id: &videocaptionerr_domain::BatchId,
        job_id: JobId,
        dir: &Path,
    ) -> TranscribeJobCommand {
        let input = dir.join("input.wav");
        if !input.exists() {
            std::fs::write(&input, b"RIFF....WAVEfmt ").unwrap();
        }
        let job_dir = dir.join(format!("job-{}", job_id.as_str()));
        std::fs::create_dir_all(&job_dir).unwrap();
        TranscribeJobCommand {
            job_id,
            batch_id: Some(batch_id.clone()),
            profile_revision: Ulid::new().into(),
            execution_snapshot_id: Ulid::new().into(),
            input,
            job_dir,
            language: Some("en".into()),
            export: SubtitleExportRequest {
                output_path: dir.join(format!("{}.srt", Ulid::new())),
                format: crate::ports::SubtitleFormat::Srt,
                layout: crate::ports::SubtitleLayout::SourceOnly,
                fallback_to_source: true,
            },
            llm: None,
        }
    }

    fn make_batch(dir: &Path, count: usize) -> (Batch, Vec<TranscribeJobCommand>) {
        let batch_id = Ulid::new().into();
        let mut ids = Vec::new();
        let mut commands = Vec::new();
        for _ in 0..count {
            let job_id: JobId = Ulid::new().into();
            ids.push(job_id.clone());
            commands.push(command(&batch_id, job_id, dir));
        }
        (Batch::new(batch_id, ids, profile()).unwrap(), commands)
    }

    fn use_case_with_events(
        jobs: Arc<Jobs>,
        events: Arc<dyn EventPublisher>,
    ) -> Arc<TranscribeJob> {
        Arc::new(TranscribeJob::new(
            jobs,
            Arc::new(Media),
            Arc::new(Artifacts),
            Arc::new(Subtitles),
            events,
            Arc::new(Ids),
            Arc::new(StageCommits),
        ))
    }

    fn use_case(jobs: Arc<Jobs>) -> Arc<TranscribeJob> {
        use_case_with_events(jobs, Arc::new(Events))
    }

    fn seed_persisted_state(
        jobs: &Arc<Jobs>,
        batches: &Arc<Batches>,
        batch: &Batch,
        commands: &[TranscribeJobCommand],
    ) {
        batches.values.lock().unwrap().push(batch.clone());
        let mut values = jobs.values.lock().unwrap();
        for command in commands {
            values.insert(
                command.job_id.to_string(),
                Job::new_with_snapshot(
                    command.job_id.clone(),
                    command.batch_id.clone(),
                    command.execution_snapshot_id.clone(),
                    command.profile_revision.clone(),
                    command.input.to_string_lossy(),
                ),
            );
        }
    }

    #[tokio::test]
    async fn opens_and_closes_one_session_for_the_whole_batch() {
        let dir = tempfile::tempdir().unwrap();
        let jobs = Arc::new(Jobs {
            values: Mutex::new(HashMap::new()),
        });
        let (batch, commands) = make_batch(dir.path(), 2);
        let batches = Arc::new(Batches {
            values: Mutex::new(Vec::new()),
        });
        seed_persisted_state(&jobs, &batches, &batch, &commands);
        let opens = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let runner = RunBatch::new(
            batches,
            jobs.clone(),
            Arc::new(Resolver(Runtime {
                opens: opens.clone(),
                closes: closes.clone(),
                fail_first: false,
            })),
            use_case(jobs),
            Arc::new(Events),
        );

        let result = runner
            .execute(RunBatchCommand {
                batch,
                jobs: commands,
                asr_spec: asr_spec(),
            })
            .await
            .unwrap();
        assert_eq!(result.jobs.len(), 2);
        assert!(result.failures.is_empty());
        assert_eq!(result.batch.status(), BatchStatus::Done);
        assert_eq!(opens.load(Ordering::SeqCst), 1);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn does_not_open_asr_before_the_batch_is_persisted() {
        let dir = tempfile::tempdir().unwrap();
        let jobs = Arc::new(Jobs {
            values: Mutex::new(HashMap::new()),
        });
        let (batch, commands) = make_batch(dir.path(), 1);
        let batches = Arc::new(Batches {
            values: Mutex::new(Vec::new()),
        });
        let opens = Arc::new(AtomicUsize::new(0));
        let runner = RunBatch::new(
            batches,
            jobs.clone(),
            Arc::new(Resolver(Runtime {
                opens: opens.clone(),
                closes: Arc::new(AtomicUsize::new(0)),
                fail_first: false,
            })),
            use_case(jobs),
            Arc::new(Events),
        );

        assert!(runner
            .execute(RunBatchCommand {
                batch,
                jobs: commands,
                asr_spec: asr_spec(),
            })
            .await
            .is_err());
        assert_eq!(opens.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn does_not_open_asr_when_a_batch_job_is_not_persisted() {
        let dir = tempfile::tempdir().unwrap();
        let jobs = Arc::new(Jobs {
            values: Mutex::new(HashMap::new()),
        });
        let (batch, commands) = make_batch(dir.path(), 1);
        let batches = Arc::new(Batches {
            values: Mutex::new(vec![batch.clone()]),
        });
        let opens = Arc::new(AtomicUsize::new(0));
        let runner = RunBatch::new(
            batches,
            jobs.clone(),
            Arc::new(Resolver(Runtime {
                opens: opens.clone(),
                closes: Arc::new(AtomicUsize::new(0)),
                fail_first: false,
            })),
            use_case(jobs),
            Arc::new(Events),
        );

        assert!(runner
            .execute(RunBatchCommand {
                batch,
                jobs: commands,
                asr_spec: asr_spec(),
            })
            .await
            .is_err());
        assert_eq!(opens.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn failed_job_does_not_skip_later_job_or_session_close() {
        let dir = tempfile::tempdir().unwrap();
        let jobs = Arc::new(Jobs {
            values: Mutex::new(HashMap::new()),
        });
        let (batch, commands) = make_batch(dir.path(), 2);
        let batches = Arc::new(Batches {
            values: Mutex::new(Vec::new()),
        });
        seed_persisted_state(&jobs, &batches, &batch, &commands);
        let opens = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let runner = RunBatch::new(
            batches,
            jobs.clone(),
            Arc::new(Resolver(Runtime {
                opens: opens.clone(),
                closes: closes.clone(),
                fail_first: true,
            })),
            use_case(jobs),
            Arc::new(Events),
        );

        let result = runner
            .execute(RunBatchCommand {
                batch,
                jobs: commands,
                asr_spec: asr_spec(),
            })
            .await
            .unwrap();
        assert_eq!(result.jobs.len(), 1);
        assert_eq!(result.failures.len(), 1);
        assert_eq!(result.batch.status(), BatchStatus::Failed);
        assert_eq!(opens.load(Ordering::SeqCst), 1);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_subset_runs_only_selected_job_and_keeps_other_terminal() {
        let dir = tempfile::tempdir().unwrap();
        let jobs = Arc::new(Jobs {
            values: Mutex::new(HashMap::new()),
        });
        let (batch, commands) = make_batch(dir.path(), 2);
        let batches = Arc::new(Batches {
            values: Mutex::new(Vec::new()),
        });
        seed_persisted_state(&jobs, &batches, &batch, &commands);

        // First full run: fail job A, succeed job B.
        let runner = RunBatch::new(
            batches.clone(),
            jobs.clone(),
            Arc::new(Resolver(Runtime {
                opens: Arc::new(AtomicUsize::new(0)),
                closes: Arc::new(AtomicUsize::new(0)),
                fail_first: true,
            })),
            use_case(jobs.clone()),
            Arc::new(Events),
        );
        let first = runner
            .execute(RunBatchCommand {
                batch: batch.clone(),
                jobs: commands.clone(),
                asr_spec: asr_spec(),
            })
            .await
            .unwrap();
        assert_eq!(first.batch.status(), BatchStatus::Failed);

        // Reopen only job A for retry.
        let mut batch = first.batch;
        batch.prepare_retry(&commands[0].job_id).unwrap();
        let mut job_a = jobs.load_job(&commands[0].job_id).await.unwrap().unwrap();
        // Full stage restart for this subset fixture: the in-memory Artifacts
        // adapter does not retain Probe/Extract manifests across jobs.
        job_a.prepare_retry(Some(StageKind::Probe)).unwrap();
        let expected = job_a.expected_version();
        jobs.save_job(&mut job_a, expected).await.unwrap();
        batches.values.lock().unwrap().push(batch.clone());

        let opens = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let runner = RunBatch::new(
            batches,
            jobs.clone(),
            Arc::new(Resolver(Runtime {
                opens: opens.clone(),
                closes: closes.clone(),
                fail_first: false,
            })),
            use_case(jobs),
            Arc::new(Events),
        );
        let result = runner
            .execute(RunBatchCommand {
                batch,
                jobs: vec![commands[0].clone()],
                asr_spec: asr_spec(),
            })
            .await
            .unwrap();
        assert_eq!(result.jobs.len(), 1);
        assert_eq!(result.jobs[0].job.id(), &commands[0].job_id);
        assert_eq!(result.batch.status(), BatchStatus::Done);
        assert_eq!(opens.load(Ordering::SeqCst), 1);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn committed_batch_does_not_fail_when_event_publisher_is_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        let jobs = Arc::new(Jobs {
            values: Mutex::new(HashMap::new()),
        });
        let (batch, commands) = make_batch(dir.path(), 1);
        let batches = Arc::new(Batches {
            values: Mutex::new(Vec::new()),
        });
        seed_persisted_state(&jobs, &batches, &batch, &commands);
        let runner = RunBatch::new(
            batches,
            jobs.clone(),
            Arc::new(Resolver(Runtime {
                opens: Arc::new(AtomicUsize::new(0)),
                closes: Arc::new(AtomicUsize::new(0)),
                fail_first: false,
            })),
            use_case_with_events(jobs, Arc::new(FailingEvents)),
            Arc::new(FailingEvents),
        );

        let result = runner
            .execute(RunBatchCommand {
                batch,
                jobs: commands,
                asr_spec: asr_spec(),
            })
            .await
            .unwrap();

        assert_eq!(result.batch.status(), BatchStatus::Done);
    }

    #[tokio::test]
    async fn pause_keeps_session_open_and_resume_continues_remaining_jobs() {
        let dir = tempfile::tempdir().unwrap();
        let jobs = Arc::new(Jobs {
            values: Mutex::new(HashMap::new()),
        });
        let (batch, commands) = make_batch(dir.path(), 2);
        let batch_id = batch.id().clone();
        let batches = Arc::new(Batches {
            values: Mutex::new(Vec::new()),
        });
        seed_persisted_state(&jobs, &batches, &batch, &commands);
        let state = Arc::new(GateState {
            started: Notify::new(),
            release: Notify::new(),
            calls: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
        });
        let opens = Arc::new(AtomicUsize::new(0));
        let runner = Arc::new(RunBatch::new(
            batches.clone(),
            jobs.clone(),
            Arc::new(GateResolver {
                opens: opens.clone(),
                state: state.clone(),
            }),
            use_case(jobs),
            Arc::new(Events),
        ));
        let task = tokio::spawn({
            let runner = runner.clone();
            async move {
                runner
                    .execute(RunBatchCommand {
                        batch,
                        jobs: commands,
                        asr_spec: asr_spec(),
                    })
                    .await
            }
        });

        state.started.notified().await;
        // The first Job is still inside ASR. Persisting pause now must be
        // observed only after that Job reaches its safe boundary.
        let mut paused = batches.load_batch(&batch_id).await.unwrap().unwrap();
        paused.request_pause().unwrap();
        let paused_expected = paused.expected_version();
        batches
            .save_batch(&mut paused, paused_expected)
            .await
            .unwrap();
        state.release.notify_one();

        for _ in 0..100 {
            if batches
                .load_batch(&batch_id)
                .await
                .unwrap()
                .is_some_and(|batch| batch.status() == BatchStatus::Paused)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(state.closes.load(Ordering::SeqCst), 0);
        let current = batches.load_batch(&batch_id).await.unwrap().unwrap();
        assert_eq!(current.status(), BatchStatus::Paused);
        assert_eq!(state.calls.load(Ordering::SeqCst), 1);

        let mut resumed = current;
        resumed.resume().unwrap();
        let resumed_expected = resumed.expected_version();
        batches
            .save_batch(&mut resumed, resumed_expected)
            .await
            .unwrap();
        for _ in 0..100 {
            if state.calls.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        state.release.notify_one();
        let result = task.await.unwrap().unwrap();
        assert_eq!(result.batch.status(), BatchStatus::Done);
        assert_eq!(result.jobs.len(), 2);
        assert_eq!(opens.load(Ordering::SeqCst), 1);
        assert_eq!(state.closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pause_then_cancel_finishes_remaining_jobs_without_closing_early() {
        let dir = tempfile::tempdir().unwrap();
        let jobs = Arc::new(Jobs {
            values: Mutex::new(HashMap::new()),
        });
        let (batch, commands) = make_batch(dir.path(), 2);
        let batch_id = batch.id().clone();
        let batches = Arc::new(Batches {
            values: Mutex::new(Vec::new()),
        });
        seed_persisted_state(&jobs, &batches, &batch, &commands);
        let state = Arc::new(GateState {
            started: Notify::new(),
            release: Notify::new(),
            calls: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
        });
        let runner = Arc::new(RunBatch::new(
            batches.clone(),
            jobs.clone(),
            Arc::new(GateResolver {
                opens: Arc::new(AtomicUsize::new(0)),
                state: state.clone(),
            }),
            use_case(jobs.clone()),
            Arc::new(Events),
        ));
        let task = tokio::spawn({
            let runner = runner.clone();
            async move {
                runner
                    .execute(RunBatchCommand {
                        batch,
                        jobs: commands,
                        asr_spec: asr_spec(),
                    })
                    .await
            }
        });

        state.started.notified().await;
        let mut paused = batches.load_batch(&batch_id).await.unwrap().unwrap();
        paused.request_pause().unwrap();
        let expected = paused.expected_version();
        batches.save_batch(&mut paused, expected).await.unwrap();
        state.release.notify_one();

        for _ in 0..100 {
            if batches
                .load_batch(&batch_id)
                .await
                .unwrap()
                .is_some_and(|batch| batch.status() == BatchStatus::Paused)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(
            batches
                .load_batch(&batch_id)
                .await
                .unwrap()
                .unwrap()
                .status(),
            BatchStatus::Paused
        );
        assert_eq!(state.closes.load(Ordering::SeqCst), 0);

        let request = crate::use_cases::CancelBatch::new(batches.clone(), jobs.clone())
            .execute(crate::use_cases::CancelBatchCommand {
                batch_id: batch_id.clone(),
            })
            .await
            .unwrap();
        assert!(request.cancel_requested);

        let result = task.await.unwrap().unwrap();
        assert_eq!(result.batch.status(), BatchStatus::Cancelled);
        assert_eq!(state.closes.load(Ordering::SeqCst), 1);
        let statuses: Vec<_> = jobs
            .list_jobs()
            .await
            .unwrap()
            .into_iter()
            .map(|job| job.status())
            .collect();
        assert!(statuses.contains(&JobStatus::Done));
        assert!(statuses.contains(&JobStatus::Cancelled));
    }

    #[tokio::test]
    async fn active_batch_cancel_converges_after_the_current_job_reaches_a_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let jobs = Arc::new(Jobs {
            values: Mutex::new(HashMap::new()),
        });
        let (batch, commands) = make_batch(dir.path(), 2);
        let batch_id = batch.id().clone();
        let batches = Arc::new(Batches {
            values: Mutex::new(Vec::new()),
        });
        seed_persisted_state(&jobs, &batches, &batch, &commands);
        let state = Arc::new(GateState {
            started: Notify::new(),
            release: Notify::new(),
            calls: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
        });
        let runner = Arc::new(RunBatch::new(
            batches.clone(),
            jobs.clone(),
            Arc::new(GateResolver {
                opens: Arc::new(AtomicUsize::new(0)),
                state: state.clone(),
            }),
            use_case(jobs.clone()),
            Arc::new(Events),
        ));
        let task = tokio::spawn({
            let runner = runner.clone();
            async move {
                runner
                    .execute(RunBatchCommand {
                        batch,
                        jobs: commands,
                        asr_spec: asr_spec(),
                    })
                    .await
            }
        });

        state.started.notified().await;
        let request = crate::use_cases::CancelBatch::new(batches.clone(), jobs.clone())
            .execute(crate::use_cases::CancelBatchCommand {
                batch_id: batch_id.clone(),
            })
            .await
            .unwrap();
        assert!(request.cancel_requested);
        state.release.notify_one();

        let result = task.await.unwrap().unwrap();
        assert_eq!(result.batch.status(), BatchStatus::Cancelled);
        assert_eq!(state.closes.load(Ordering::SeqCst), 1);
        assert!(jobs
            .list_jobs()
            .await
            .unwrap()
            .into_iter()
            .all(|job| job.status() == JobStatus::Cancelled));
        let repeated = crate::use_cases::CancelBatch::new(batches, jobs)
            .execute(crate::use_cases::CancelBatchCommand { batch_id })
            .await
            .unwrap();
        assert!(!repeated.cancel_requested);
    }

    #[tokio::test]
    async fn cleanup_failure_does_not_replace_a_committed_batch_result() {
        let dir = tempfile::tempdir().unwrap();
        let jobs = Arc::new(Jobs {
            values: Mutex::new(HashMap::new()),
        });
        let (batch, commands) = make_batch(dir.path(), 1);
        let batches = Arc::new(Batches {
            values: Mutex::new(Vec::new()),
        });
        seed_persisted_state(&jobs, &batches, &batch, &commands);
        let closes = Arc::new(AtomicUsize::new(0));
        let runner = RunBatch::new(
            batches,
            jobs.clone(),
            Arc::new(CleanupFailResolver {
                closes: closes.clone(),
            }),
            use_case(jobs),
            Arc::new(Events),
        );
        let result = runner
            .execute(RunBatchCommand {
                batch,
                jobs: commands,
                asr_spec: asr_spec(),
            })
            .await
            .unwrap();
        assert_eq!(result.batch.status(), BatchStatus::Done);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn batch_cancel_is_durable_and_repeated_cancel_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let jobs = Arc::new(Jobs {
            values: Mutex::new(HashMap::new()),
        });
        let (batch, commands) = make_batch(dir.path(), 2);
        let batch_id = batch.id().clone();
        let batches = Arc::new(Batches {
            values: Mutex::new(Vec::new()),
        });
        seed_persisted_state(&jobs, &batches, &batch, &commands);

        let first = crate::use_cases::CancelBatch::new(batches.clone(), jobs.clone())
            .execute(crate::use_cases::CancelBatchCommand {
                batch_id: batch_id.clone(),
            })
            .await
            .unwrap();
        assert!(first.cancel_requested);
        assert_eq!(
            batches
                .load_batch(&batch_id)
                .await
                .unwrap()
                .unwrap()
                .status(),
            BatchStatus::Cancelled
        );
        assert!(jobs
            .list_jobs()
            .await
            .unwrap()
            .into_iter()
            .all(|job| job.status() == JobStatus::Cancelled));

        let second = crate::use_cases::CancelBatch::new(batches.clone(), jobs.clone())
            .execute(crate::use_cases::CancelBatchCommand { batch_id })
            .await
            .unwrap();
        assert!(!second.cancel_requested);
        assert_eq!(
            batches
                .load_batch(batch.id())
                .await
                .unwrap()
                .unwrap()
                .status(),
            BatchStatus::Cancelled
        );
    }
}
