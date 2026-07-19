//! One-job transcription orchestration.
//!
//! The ASR session is supplied by RunBatch. This use case therefore never
//! loads, unloads, or switches an ASR model.

use std::path::PathBuf;
use std::sync::Arc;

use videocaptionerr_contracts::error::{ErrorCode, VcError};
use videocaptionerr_domain::{ArtifactRef, BatchId, Job, JobId, StageKind, Transcript, UlidStr};

use crate::application_error::{AppResult, ApplicationError};
use crate::ports::{
    ArtifactCommit, ArtifactInput, ArtifactStore, AsrSession, AsrTranscribeRequest, EventPublisher,
    IdGenerator, JobRepository, MediaGateway, ProbeMediaRequest, SubtitleExportRequest,
    SubtitleGateway, TranscriptCommit,
};

pub struct TranscribeJobCommand {
    pub job_id: JobId,
    pub batch_id: Option<BatchId>,
    pub profile_revision: UlidStr,
    pub input: PathBuf,
    pub job_dir: PathBuf,
    pub language: Option<String>,
    pub export: SubtitleExportRequest,
}

#[derive(Debug)]
pub struct TranscribeJobResponse {
    pub job: Job,
    pub transcript: Transcript,
    pub export_path: PathBuf,
}

pub struct TranscribeJob {
    jobs: Arc<dyn JobRepository>,
    media: Arc<dyn MediaGateway>,
    artifacts: Arc<dyn ArtifactStore>,
    subtitles: Arc<dyn SubtitleGateway>,
    events: Arc<dyn EventPublisher>,
    ids: Arc<dyn IdGenerator>,
}

impl TranscribeJob {
    pub fn new(
        jobs: Arc<dyn JobRepository>,
        media: Arc<dyn MediaGateway>,
        artifacts: Arc<dyn ArtifactStore>,
        subtitles: Arc<dyn SubtitleGateway>,
        events: Arc<dyn EventPublisher>,
        ids: Arc<dyn IdGenerator>,
    ) -> Self {
        Self {
            jobs,
            media,
            artifacts,
            subtitles,
            events,
            ids,
        }
    }

    pub async fn execute(
        &self,
        command: TranscribeJobCommand,
        session: &mut dyn AsrSession,
    ) -> AppResult<TranscribeJobResponse> {
        let mut job = Job::new(
            command.job_id.clone(),
            command.batch_id.clone(),
            command.profile_revision,
            command.input.to_string_lossy(),
        );
        self.jobs.save_job(&job).await?;
        job.start()?;
        self.jobs.save_job(&job).await?;

        let mut current_stage = None;
        let result: AppResult<TranscribeJobResponse> = async {
            let source_hash = self.media.media_hash(&command.input).await?;
            job.start_stage(StageKind::Probe)?;
            current_stage = Some(StageKind::Probe);
            let probed = self
                .media
                .probe(ProbeMediaRequest {
                    input: command.input.clone(),
                    job_dir: command.job_dir.clone(),
                })
                .await?;
            if !probed.probe.has_audio() {
                return Err(ApplicationError::Adapter(VcError::new(
                    ErrorCode::AudioStreamNotFound,
                    "no audio streams",
                )));
            }
            let stream_index = probed
                .probe
                .auto_select_stream()
                .or_else(|| probed.probe.default_stream())
                .ok_or_else(|| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::AudioStreamNotFound,
                        "no selectable audio stream",
                    ))
                })?
                .stream_index;
            let probe_artifact = self.commit_input(&command.job_id, probed.artifact).await?;
            job.complete_stage(StageKind::Probe, probe_artifact, false)?;
            self.jobs.save_job(&job).await?;

            job.start_stage(StageKind::ExtractAudio)?;
            current_stage = Some(StageKind::ExtractAudio);
            let extracted = self
                .media
                .extract_audio(crate::ports::ExtractAudioRequest {
                    input: command.input.clone(),
                    stream_index,
                    expected_duration_ms: Some(probed.probe.duration_ms),
                    job_dir: command.job_dir.clone(),
                })
                .await?;
            let extract_artifact = self
                .commit_input(&command.job_id, extracted.artifact)
                .await?;
            job.complete_stage(StageKind::ExtractAudio, extract_artifact, false)?;
            self.jobs.save_job(&job).await?;

            job.start_stage(StageKind::Asr)?;
            current_stage = Some(StageKind::Asr);
            if !session.descriptor().supports_word_timestamps {
                return Err(ApplicationError::Adapter(VcError::new(
                    ErrorCode::EngineCapabilityInsufficient,
                    "ASR session does not provide word timestamps",
                )));
            }
            let asr = session
                .transcribe(
                    AsrTranscribeRequest {
                        audio_path: extracted.wav_path,
                        language: command.language.clone(),
                        source_hash,
                        duration_ms: Some(probed.probe.duration_ms),
                    },
                    self.events.as_ref(),
                )
                .await?;
            asr.transcript.validate()?;
            let asr_artifact = self
                .artifacts
                .commit_transcript(TranscriptCommit {
                    job_id: command.job_id.clone(),
                    stage: StageKind::Asr,
                    artifact_id: self.ids.next_id(),
                    path: command.job_dir.join("01_asr.json"),
                    transcript: asr.transcript.clone(),
                    producer_fingerprint: session.descriptor().engine_id.clone(),
                })
                .await?;
            job.complete_stage(StageKind::Asr, asr_artifact, false)?;
            self.jobs.save_job(&job).await?;

            job.start_stage(StageKind::Split)?;
            current_stage = Some(StageKind::Split);
            let split = videocaptionerr_domain::rule_split(
                &asr.transcript,
                &videocaptionerr_domain::RuleSplitConfig::default(),
            )?;
            let split_artifact = self
                .artifacts
                .commit_transcript(TranscriptCommit {
                    job_id: command.job_id.clone(),
                    stage: StageKind::Split,
                    artifact_id: self.ids.next_id(),
                    path: command.job_dir.join("02_split.json"),
                    transcript: split.clone(),
                    producer_fingerprint: "domain-rule-split".into(),
                })
                .await?;
            job.complete_stage(StageKind::Split, split_artifact, false)?;
            self.jobs.save_job(&job).await?;

            current_stage = Some(StageKind::Correct);
            job.skip_stage(StageKind::Correct)?;
            current_stage = Some(StageKind::Translate);
            job.skip_stage(StageKind::Translate)?;
            self.jobs.save_job(&job).await?;

            job.start_stage(StageKind::Export)?;
            current_stage = Some(StageKind::Export);
            let exported = self.subtitles.export(&split, command.export).await?;
            let export_ref = ArtifactRef {
                id: self.ids.next_id(),
                stage: StageKind::Export,
                path: exported.path.to_string_lossy().into_owned(),
                content_hash: exported.content_hash,
                schema_version: videocaptionerr_domain::SCHEMA_VERSION,
                producer_fingerprint: "subtitle-writer".into(),
            };
            self.artifacts
                .commit(ArtifactCommit {
                    job_id: command.job_id.clone(),
                    artifact: export_ref.clone(),
                    work_unit_id: None,
                })
                .await?;
            job.complete_stage(StageKind::Export, export_ref, false)?;
            job.finish()?;
            self.jobs.save_job(&job).await?;

            Ok(TranscribeJobResponse {
                job: job.clone(),
                transcript: split,
                export_path: exported.path,
            })
        }
        .await;

        if let Err(error) = result {
            if let Some(stage) = current_stage {
                if job.fail_stage(stage).is_err() && !job.status().is_terminal() {
                    let _ = job.cancel();
                }
            } else if !job.status().is_terminal() {
                let _ = job.cancel();
            }
            let _ = self.jobs.save_job(&job).await;
            return Err(error);
        }
        result
    }

    async fn commit_input(&self, job_id: &JobId, input: ArtifactInput) -> AppResult<ArtifactRef> {
        let artifact = ArtifactRef {
            id: self.ids.next_id(),
            stage: input.stage,
            path: input.path.to_string_lossy().into_owned(),
            content_hash: input.content_hash,
            schema_version: input.schema_version,
            producer_fingerprint: input.producer_fingerprint,
        };
        self.artifacts
            .commit(ArtifactCommit {
                job_id: job_id.clone(),
                artifact: artifact.clone(),
                work_unit_id: None,
            })
            .await?;
        Ok(artifact)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use tempfile::tempdir;
    use ulid::Ulid;
    use videocaptionerr_contracts::media::{AudioStream, MediaProbe};
    use videocaptionerr_domain::{EngineFingerprint, Word, PROB_UNAVAILABLE, SCHEMA_VERSION};

    use super::*;
    use crate::application_error::AppResult;
    use crate::ports::{
        ArtifactStore, AsrDescriptor, AudioExtraction, ExportedSubtitle, ExtractAudioRequest,
        NormalizedAsrResult, ProbedMedia, SubtitleFormat, SubtitleLayout,
    };

    struct FakeIds;

    impl IdGenerator for FakeIds {
        fn next_id(&self) -> UlidStr {
            UlidStr::from(Ulid::new())
        }
    }

    struct FakeEvents;

    #[async_trait]
    impl EventPublisher for FakeEvents {
        async fn publish(&self, _event: videocaptionerr_domain::DomainEvent) -> AppResult<()> {
            Ok(())
        }
    }

    struct FakeJobs {
        saved: Mutex<Vec<Job>>,
    }

    #[async_trait]
    impl JobRepository for FakeJobs {
        async fn load_job(&self, _id: &JobId) -> AppResult<Option<Job>> {
            Ok(self.saved.lock().unwrap().last().cloned())
        }

        async fn save_job(&self, job: &Job) -> AppResult<()> {
            self.saved.lock().unwrap().push(job.clone());
            Ok(())
        }

        async fn delete_job(&self, _id: &JobId) -> AppResult<()> {
            Ok(())
        }

        async fn list_jobs(&self) -> AppResult<Vec<Job>> {
            Ok(self.saved.lock().unwrap().clone())
        }
    }

    struct FakeMedia;

    fn artifact(stage: StageKind, name: &str) -> ArtifactInput {
        ArtifactInput {
            stage,
            path: PathBuf::from(name),
            content_hash: format!("hash-{name}"),
            schema_version: SCHEMA_VERSION,
            producer_fingerprint: "fake-media".into(),
        }
    }

    #[async_trait]
    impl MediaGateway for FakeMedia {
        async fn probe(&self, _request: ProbeMediaRequest) -> AppResult<ProbedMedia> {
            Ok(ProbedMedia {
                probe: MediaProbe {
                    schema_version: SCHEMA_VERSION,
                    input_size: 10,
                    container: Some("wav".into()),
                    duration_ms: 1000,
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
                artifact: artifact(StageKind::Probe, "probe.json"),
            })
        }

        async fn media_hash(&self, _input: &std::path::Path) -> AppResult<String> {
            Ok("media-hash".into())
        }

        async fn extract_audio(&self, request: ExtractAudioRequest) -> AppResult<AudioExtraction> {
            Ok(AudioExtraction {
                wav_path: request.job_dir.join("audio.wav"),
                pcm_hash: "pcm-hash".into(),
                artifact: artifact(StageKind::ExtractAudio, "audio.wav"),
            })
        }
    }

    struct FakeArtifacts {
        committed: Mutex<Vec<ArtifactRef>>,
    }

    #[async_trait]
    impl ArtifactStore for FakeArtifacts {
        async fn commit(&self, commit: ArtifactCommit) -> AppResult<()> {
            self.committed.lock().unwrap().push(commit.artifact);
            Ok(())
        }

        async fn commit_transcript(&self, commit: TranscriptCommit) -> AppResult<ArtifactRef> {
            let artifact = ArtifactRef {
                id: commit.artifact_id,
                stage: commit.stage,
                path: commit.path.to_string_lossy().into_owned(),
                content_hash: format!("transcript-{}", commit.transcript.revision),
                schema_version: SCHEMA_VERSION,
                producer_fingerprint: commit.producer_fingerprint,
            };
            self.committed.lock().unwrap().push(artifact.clone());
            Ok(artifact)
        }

        async fn validate(&self, _artifact: &ArtifactRef) -> AppResult<()> {
            Ok(())
        }
    }

    struct FakeSubtitles;

    #[async_trait]
    impl SubtitleGateway for FakeSubtitles {
        async fn export(
            &self,
            _transcript: &Transcript,
            request: SubtitleExportRequest,
        ) -> AppResult<ExportedSubtitle> {
            Ok(ExportedSubtitle {
                path: request.output_path,
                content_hash: "srt-hash".into(),
            })
        }
    }

    struct FakeSession {
        descriptor: AsrDescriptor,
        transcript: Option<Transcript>,
        fail: bool,
    }

    #[async_trait]
    impl AsrSession for FakeSession {
        fn descriptor(&self) -> &AsrDescriptor {
            &self.descriptor
        }

        async fn transcribe(
            &mut self,
            _request: AsrTranscribeRequest,
            _events: &dyn EventPublisher,
        ) -> AppResult<NormalizedAsrResult> {
            if self.fail {
                return Err(ApplicationError::Adapter(VcError::new(
                    ErrorCode::AsrFailed,
                    "fake ASR failure",
                )));
            }
            Ok(NormalizedAsrResult {
                transcript: self.transcript.take().unwrap(),
            })
        }

        async fn close(self: Box<Self>) -> AppResult<()> {
            Ok(())
        }
    }

    fn transcript() -> Transcript {
        Transcript::new_asr(
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
        )
    }

    fn use_case() -> (TranscribeJob, Arc<FakeJobs>) {
        let jobs = Arc::new(FakeJobs {
            saved: Mutex::new(Vec::new()),
        });
        let artifacts = Arc::new(FakeArtifacts {
            committed: Mutex::new(Vec::new()),
        });
        let use_case = TranscribeJob::new(
            jobs.clone(),
            Arc::new(FakeMedia),
            artifacts,
            Arc::new(FakeSubtitles),
            Arc::new(FakeEvents),
            Arc::new(FakeIds),
        );
        (use_case, jobs)
    }

    fn command(dir: &std::path::Path) -> TranscribeJobCommand {
        TranscribeJobCommand {
            job_id: UlidStr::from(Ulid::new()),
            batch_id: None,
            profile_revision: UlidStr::from(Ulid::new()),
            input: dir.join("input.wav"),
            job_dir: dir.join("job"),
            language: Some("en".into()),
            export: SubtitleExportRequest {
                output_path: dir.join("output.srt"),
                format: SubtitleFormat::Srt,
                layout: SubtitleLayout::SourceOnly,
                fallback_to_source: true,
            },
        }
    }

    fn session(transcript: Option<Transcript>, fail: bool) -> FakeSession {
        FakeSession {
            descriptor: AsrDescriptor {
                engine_id: "fake".into(),
                adapter_version: "test".into(),
                runtime_version: "test".into(),
                supports_word_timestamps: true,
                supports_confidence: true,
                cooperative_cancel: true,
            },
            transcript,
            fail,
        }
    }

    #[tokio::test]
    async fn fake_vertical_slice_completes_without_unloading_session() {
        let dir = tempdir().unwrap();
        let (use_case, jobs) = use_case();
        let mut asr = session(Some(transcript()), false);
        let result = use_case
            .execute(command(dir.path()), &mut asr)
            .await
            .unwrap();
        assert_eq!(result.job.status(), videocaptionerr_domain::JobStatus::Done);
        assert!(!result.transcript.cues.is_empty());
        assert_eq!(
            jobs.saved.lock().unwrap().last().unwrap().status(),
            videocaptionerr_domain::JobStatus::Done
        );
    }

    #[tokio::test]
    async fn asr_failure_marks_job_failed() {
        let dir = tempdir().unwrap();
        let (use_case, jobs) = use_case();
        let mut asr = session(None, true);
        let error = use_case
            .execute(command(dir.path()), &mut asr)
            .await
            .unwrap_err();
        assert_eq!(error.into_vc_error().code, ErrorCode::AsrFailed);
        assert_eq!(
            jobs.saved.lock().unwrap().last().unwrap().status(),
            videocaptionerr_domain::JobStatus::Failed
        );
    }
}
