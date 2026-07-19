//! FIFO Batch orchestration and Batch-scoped ASR session lifetime.

use std::sync::Arc;

use videocaptionerr_contracts::error::VcError;
use videocaptionerr_domain::{Batch, BatchStatus, JobStatus, JobTerminalStatus};

use crate::application_error::{AppResult, ApplicationError};
use crate::ports::{AsrRuntime, BatchRepository, EventPublisher, JobRepository};
use crate::use_cases::{TranscribeJob, TranscribeJobCommand, TranscribeJobResponse};

pub struct RunBatchCommand {
    pub batch: Batch,
    pub jobs: Vec<TranscribeJobCommand>,
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
    asr: Arc<dyn AsrRuntime>,
    transcribe: Arc<TranscribeJob>,
    events: Arc<dyn EventPublisher>,
}

impl RunBatch {
    pub fn new(
        batches: Arc<dyn BatchRepository>,
        jobs: Arc<dyn JobRepository>,
        asr: Arc<dyn AsrRuntime>,
        transcribe: Arc<TranscribeJob>,
        events: Arc<dyn EventPublisher>,
    ) -> Self {
        Self {
            batches,
            jobs,
            asr,
            transcribe,
            events,
        }
    }

    pub async fn execute(&self, command: RunBatchCommand) -> AppResult<RunBatchResponse> {
        let mut batch = command.batch;
        validate_commands(&batch, &command.jobs)?;

        // Opening the session is deliberately outside the per-Job loop. The
        // worker/model stays alive until the Batch reaches a terminal state.
        let mut session = self.asr.open_session(batch.execution_profile()).await?;
        if let Err(error) = batch.start() {
            let _ = session.close().await;
            return Err(ApplicationError::Domain(error));
        }
        if let Err(error) = self.batches.save_batch(&batch).await {
            let _ = session.close().await;
            return Err(error);
        }

        let result = self
            .execute_with_session(batch, command.jobs, session.as_mut())
            .await;
        let close_result = session.close().await;
        match (result, close_result) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(response), Ok(())) => Ok(response),
        }
    }

    async fn execute_with_session(
        &self,
        mut batch: Batch,
        commands: Vec<TranscribeJobCommand>,
        session: &mut dyn crate::ports::AsrSession,
    ) -> AppResult<RunBatchResponse> {
        let mut responses = Vec::new();
        let mut failed_job_ids = Vec::new();
        let mut failures = Vec::new();

        for job_command in commands {
            let job_id = job_command.job_id.clone();
            match self.transcribe.execute(job_command, session).await {
                Ok(response) => {
                    let terminal = terminal_status(response.job.status());
                    batch.record_job_terminal(&job_id, terminal)?;
                    self.batches.save_batch(&batch).await?;
                    responses.push(response);
                }
                Err(error) => {
                    let vc_error = error.into_vc_error();
                    let job_id_string = job_id.to_string();
                    failed_job_ids.push(job_id_string.clone());
                    failures.push(RunBatchFailure {
                        job_id: job_id_string,
                        error: vc_error,
                    });
                    let terminal = self
                        .jobs
                        .load_job(&job_id)
                        .await?
                        .map(|job| terminal_status(job.status()))
                        .unwrap_or(JobTerminalStatus::Failed);
                    batch.record_job_terminal(&job_id, terminal)?;
                    self.batches.save_batch(&batch).await?;
                }
            }
        }

        let final_status = if failed_job_ids.is_empty() {
            BatchStatus::Done
        } else {
            BatchStatus::Failed
        };
        let event = batch.finish(final_status)?;
        self.batches.save_batch(&batch).await?;
        self.events.publish(event).await?;

        Ok(RunBatchResponse {
            batch,
            jobs: responses,
            failed_job_ids,
            failures,
        })
    }
}

fn validate_commands(batch: &Batch, commands: &[TranscribeJobCommand]) -> AppResult<()> {
    if commands.len() != batch.job_ids().len() {
        return Err(ApplicationError::Invalid(
            "Batch command count does not match Batch job membership".into(),
        ));
    }
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
        ArtifactCommit, ArtifactInput, ArtifactStore, AsrDescriptor, AsrSession,
        AsrTranscribeRequest, AudioExtraction, EventPublisher, ExportedSubtitle,
        ExtractAudioRequest, IdGenerator, JobRepository, MediaGateway, NormalizedAsrResult,
        ProbeMediaRequest, ProbedMedia, SubtitleExportRequest, SubtitleGateway,
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

    struct Jobs {
        values: Mutex<HashMap<String, Job>>,
    }

    #[async_trait]
    impl JobRepository for Jobs {
        async fn load_job(&self, id: &JobId) -> AppResult<Option<Job>> {
            Ok(self.values.lock().unwrap().get(id.as_str()).cloned())
        }

        async fn save_job(&self, job: &Job) -> AppResult<()> {
            self.values
                .lock()
                .unwrap()
                .insert(job.id().to_string(), job.clone());
            Ok(())
        }

        async fn delete_job(&self, _id: &JobId) -> AppResult<()> {
            Ok(())
        }

        async fn list_jobs(&self) -> AppResult<Vec<Job>> {
            Ok(self.values.lock().unwrap().values().cloned().collect())
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
        ) -> AppResult<Option<Batch>> {
            Ok(self.values.lock().unwrap().last().cloned())
        }

        async fn save_batch(&self, batch: &Batch) -> AppResult<()> {
            self.values.lock().unwrap().push(batch.clone());
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
            Ok(())
        }
    }

    struct Runtime {
        opens: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
        fail_first: bool,
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

    fn command(
        batch_id: &videocaptionerr_domain::BatchId,
        job_id: JobId,
        dir: &Path,
    ) -> TranscribeJobCommand {
        TranscribeJobCommand {
            job_id,
            batch_id: Some(batch_id.clone()),
            profile_revision: Ulid::new().into(),
            input: dir.join("input.wav"),
            job_dir: dir.join("job"),
            language: Some("en".into()),
            export: SubtitleExportRequest {
                output_path: dir.join("output.srt"),
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

    fn use_case(jobs: Arc<Jobs>) -> Arc<TranscribeJob> {
        Arc::new(TranscribeJob::new(
            jobs,
            Arc::new(Media),
            Arc::new(Artifacts),
            Arc::new(Subtitles),
            Arc::new(Events),
            Arc::new(Ids),
        ))
    }

    #[tokio::test]
    async fn opens_and_closes_one_session_for_the_whole_batch() {
        let dir = tempfile::tempdir().unwrap();
        let jobs = Arc::new(Jobs {
            values: Mutex::new(HashMap::new()),
        });
        let (batch, commands) = make_batch(dir.path(), 2);
        let opens = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let runner = RunBatch::new(
            Arc::new(Batches {
                values: Mutex::new(Vec::new()),
            }),
            jobs.clone(),
            Arc::new(Runtime {
                opens: opens.clone(),
                closes: closes.clone(),
                fail_first: false,
            }),
            use_case(jobs),
            Arc::new(Events),
        );

        let result = runner
            .execute(RunBatchCommand {
                batch,
                jobs: commands,
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
    async fn failed_job_does_not_skip_later_job_or_session_close() {
        let dir = tempfile::tempdir().unwrap();
        let jobs = Arc::new(Jobs {
            values: Mutex::new(HashMap::new()),
        });
        let (batch, commands) = make_batch(dir.path(), 2);
        let opens = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let runner = RunBatch::new(
            Arc::new(Batches {
                values: Mutex::new(Vec::new()),
            }),
            jobs.clone(),
            Arc::new(Runtime {
                opens: opens.clone(),
                closes: closes.clone(),
                fail_first: true,
            }),
            use_case(jobs),
            Arc::new(Events),
        );

        let result = runner
            .execute(RunBatchCommand {
                batch,
                jobs: commands,
            })
            .await
            .unwrap();
        assert_eq!(result.jobs.len(), 1);
        assert_eq!(result.failures.len(), 1);
        assert_eq!(result.batch.status(), BatchStatus::Failed);
        assert_eq!(opens.load(Ordering::SeqCst), 1);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }
}
