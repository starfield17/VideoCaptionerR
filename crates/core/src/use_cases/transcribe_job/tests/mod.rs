
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use tempfile::tempdir;
use ulid::Ulid;
use videocaptionerr_contracts::media::{AudioStream, MediaProbe};
use videocaptionerr_domain::{EngineFingerprint, Word, PROB_UNAVAILABLE, SCHEMA_VERSION};

use super::*;
use crate::application_error::AppResult;
use crate::ports::{
    ArtifactCommit, ArtifactStore, AsrDescriptor, AudioAnalysis, AudioExtraction,
    AudioRangeExtraction, CacheGcResult, CacheRepository, ChunkPlanCommit, ChunkPlanStore, Clock,
    ExpectedVersion, ExportedSubtitle, ExtractAudioRangeRequest, ExtractAudioRequest,
    NormalizedAsrResult, ProbedMedia, StageCommitRepository, StageCommitRequest, StageCommitResult,
    SubtitleFormat, SubtitleLayout, TranscriptCommit, Versioned, WorkUnitRepository,
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
    fail_terminal_save: Arc<AtomicBool>,
}

#[async_trait]
impl JobRepository for FakeJobs {
    async fn load_job(&self, _id: &JobId) -> AppResult<Option<Versioned<Job>>> {
        Ok(self
            .saved
            .lock()
            .unwrap()
            .last()
            .cloned()
            .map(|job| Versioned::with_version(job, 1)))
    }

    async fn save_job(
        &self,
        job: &mut Versioned<Job>,
        _expected: ExpectedVersion,
    ) -> AppResult<()> {
        if job.status().is_terminal() && self.fail_terminal_save.swap(false, Ordering::SeqCst) {
            return Err(ApplicationError::Adapter(VcError::new(
                ErrorCode::Internal,
                "injected terminal Job save failure",
            )));
        }
        self.saved.lock().unwrap().push(job.value.clone());
        job.version = job.version.saturating_add(1).max(1);
        Ok(())
    }

    async fn delete_job(&self, _id: &JobId) -> AppResult<()> {
        Ok(())
    }

    async fn list_jobs(&self) -> AppResult<Vec<Versioned<Job>>> {
        Ok(self
            .saved
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .map(|job| Versioned::with_version(job, 1))
            .collect())
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

#[derive(Default)]
struct LongMedia {
    ranges: Mutex<Vec<(u64, u64, PathBuf)>>,
    analyses: Mutex<u32>,
}

#[async_trait]
impl MediaGateway for LongMedia {
    async fn probe(&self, _request: ProbeMediaRequest) -> AppResult<ProbedMedia> {
        Ok(ProbedMedia {
            probe: MediaProbe {
                schema_version: SCHEMA_VERSION,
                input_size: 1,
                container: Some("wav".into()),
                duration_ms: 250_000,
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

    async fn media_hash(&self, _input: &Path) -> AppResult<String> {
        Ok("media-hash".into())
    }

    async fn extract_audio(&self, request: ExtractAudioRequest) -> AppResult<AudioExtraction> {
        Ok(AudioExtraction {
            wav_path: request.job_dir.join("audio.wav"),
            pcm_hash: "pcm-hash".into(),
            artifact: artifact(StageKind::ExtractAudio, "audio.wav"),
        })
    }

    async fn analyze_audio(&self, _request: AudioAnalysisRequest) -> AppResult<AudioAnalysis> {
        *self.analyses.lock().unwrap() += 1;
        Ok(AudioAnalysis {
            silences: vec![crate::chunking::SilenceRegion {
                start_ms: 119_000,
                end_ms: 121_000,
            }],
            energy: Vec::new(),
        })
    }

    async fn extract_audio_range(
        &self,
        request: ExtractAudioRangeRequest,
    ) -> AppResult<AudioRangeExtraction> {
        self.ranges.lock().unwrap().push((
            request.read_start_ms,
            request.read_end_ms,
            request.output_path.clone(),
        ));
        Ok(AudioRangeExtraction {
            wav_path: request.output_path,
            pcm_hash: "chunk-pcm-hash".into(),
        })
    }
}

struct FakeArtifacts {
    committed: Mutex<Vec<ArtifactRef>>,
    transcripts: Mutex<HashMap<String, Transcript>>,
    fail_export_once: Option<Arc<AtomicBool>>,
}

struct FakeStageCommits {
    artifacts: Arc<FakeArtifacts>,
}

fn next_version(current: u64, expected: ExpectedVersion) -> u64 {
    match expected {
        ExpectedVersion::New => 1,
        ExpectedVersion::Exact(version) => current.max(version).saturating_add(1),
    }
}

#[async_trait]
impl StageCommitRepository for FakeStageCommits {
    async fn commit_stage(&self, request: StageCommitRequest) -> AppResult<StageCommitResult> {
        if let Some(prepared) = request.artifact {
            if prepared.artifact.stage == StageKind::Export
                && self
                    .artifacts
                    .fail_export_once
                    .as_ref()
                    .is_some_and(|flag| flag.swap(false, Ordering::SeqCst))
            {
                return Err(ApplicationError::Adapter(VcError::new(
                    ErrorCode::ExportFailed,
                    "injected export artifact commit failure",
                )));
            }
            if let ArtifactSource::Bytes { bytes } = prepared.source {
                if let Ok(transcript) = serde_json::from_slice::<Transcript>(&bytes) {
                    self.artifacts
                        .transcripts
                        .lock()
                        .unwrap()
                        .insert(prepared.artifact.path.clone(), transcript);
                }
            }
            self.artifacts
                .committed
                .lock()
                .unwrap()
                .push(prepared.artifact);
        }

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

#[async_trait]
impl ArtifactStore for FakeArtifacts {
    async fn commit(&self, commit: ArtifactCommit) -> AppResult<()> {
        if commit.artifact.stage == StageKind::Export
            && self
                .fail_export_once
                .as_ref()
                .is_some_and(|flag| flag.swap(false, Ordering::SeqCst))
        {
            return Err(ApplicationError::Adapter(VcError::new(
                ErrorCode::ExportFailed,
                "injected export artifact commit failure",
            )));
        }
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
        self.transcripts
            .lock()
            .unwrap()
            .insert(artifact.path.clone(), commit.transcript);
        Ok(artifact)
    }

    async fn load_transcript(
        &self,
        artifact: &ArtifactRef,
    ) -> AppResult<videocaptionerr_domain::Transcript> {
        self.transcripts
            .lock()
            .unwrap()
            .get(&artifact.path)
            .cloned()
            .ok_or_else(|| ApplicationError::Invalid("fake transcript not found".into()))
    }

    async fn validate(&self, _artifact: &ArtifactRef) -> AppResult<()> {
        Ok(())
    }
}

#[derive(Default)]
struct LongPlans {
    values: Mutex<Vec<ChunkPlan>>,
}

#[async_trait]
impl ChunkPlanStore for LongPlans {
    async fn commit(&self, commit: ChunkPlanCommit) -> AppResult<ArtifactRef> {
        self.values.lock().unwrap().push(commit.plan.clone());
        Ok(ArtifactRef {
            id: commit.artifact_id,
            stage: StageKind::Asr,
            path: commit.path.to_string_lossy().into_owned(),
            content_hash: "chunk-plan-hash".into(),
            schema_version: SCHEMA_VERSION,
            producer_fingerprint: commit.producer_fingerprint,
        })
    }
}

#[derive(Default)]
struct LongCache {
    values: Mutex<HashMap<String, Vec<u8>>>,
}

impl LongCache {
    fn corrupt_first(&self) {
        let key = self.values.lock().unwrap().keys().next().cloned().unwrap();
        self.values
            .lock()
            .unwrap()
            .insert(key, b"not-json".to_vec());
    }
}

#[async_trait]
impl CacheRepository for LongCache {
    async fn gc(&self, _max_bytes: u64) -> AppResult<CacheGcResult> {
        Ok(CacheGcResult {
            before_bytes: 0,
            after_bytes: 0,
            deleted_entries: 0,
            skipped_leased: 0,
        })
    }

    async fn read(&self, key: &str) -> AppResult<Option<Vec<u8>>> {
        Ok(self.values.lock().unwrap().get(key).cloned())
    }

    async fn write(&self, key: &str, bytes: &[u8]) -> AppResult<()> {
        self.values
            .lock()
            .unwrap()
            .insert(key.to_owned(), bytes.to_vec());
        Ok(())
    }
}

#[derive(Default)]
struct LongUnits {
    values: Mutex<Vec<WorkUnit>>,
}

impl LongUnits {
    fn snapshot(&self) -> Vec<WorkUnit> {
        self.values.lock().unwrap().clone()
    }

    fn complete(&self, id: &videocaptionerr_domain::WorkUnitId, artifact: ArtifactRef) {
        let mut values = self.values.lock().unwrap();
        let unit = values.iter_mut().find(|unit| unit.id() == id).unwrap();
        unit.complete(artifact).unwrap();
    }
}

#[async_trait]
impl WorkUnitRepository for LongUnits {
    async fn load_work_unit(
        &self,
        id: &videocaptionerr_domain::WorkUnitId,
    ) -> AppResult<Option<Versioned<WorkUnit>>> {
        Ok(self
            .values
            .lock()
            .unwrap()
            .iter()
            .find(|unit| unit.id() == id)
            .cloned()
            .map(|unit| Versioned::with_version(unit, 1)))
    }

    async fn find_work_unit(
        &self,
        job_id: &JobId,
        stage: StageKind,
        unit_kind: &str,
        unit_index: u32,
        input_hash: &str,
    ) -> AppResult<Option<Versioned<WorkUnit>>> {
        Ok(self
            .values
            .lock()
            .unwrap()
            .iter()
            .find(|unit| {
                unit.job_id() == job_id
                    && unit.stage() == stage
                    && unit.unit_kind() == unit_kind
                    && unit.unit_index() == unit_index
                    && unit.input_hash() == input_hash
            })
            .cloned()
            .map(|unit| Versioned::with_version(unit, 1)))
    }

    async fn save_work_unit(
        &self,
        unit: &mut Versioned<WorkUnit>,
        _expected: ExpectedVersion,
    ) -> AppResult<()> {
        let mut values = self.values.lock().unwrap();
        if let Some(existing) = values
            .iter_mut()
            .find(|existing| existing.id() == unit.id())
        {
            *existing = unit.value.clone();
        } else {
            values.push(unit.value.clone());
        }
        unit.version = unit.version.saturating_add(1).max(1);
        Ok(())
    }

    async fn recover_expired(&self, _now_ms: u64) -> AppResult<u32> {
        Ok(0)
    }

    async fn count_retryable(
        &self,
        job_id: &JobId,
        from_stage: Option<StageKind>,
    ) -> AppResult<u32> {
        let count = self
            .values
            .lock()
            .unwrap()
            .iter()
            .filter(|unit| {
                unit.job_id() == job_id
                    && matches!(
                        unit.status(),
                        WorkUnitStatus::Failed | WorkUnitStatus::Cancelled
                    )
                    && from_stage.is_none_or(|stage| unit.stage() as u8 >= stage as u8)
            })
            .count();
        Ok(count as u32)
    }

    async fn lease_next_ready(
        &self,
        job_id: &JobId,
        stage: StageKind,
        owner: &str,
        now_ms: u64,
        lease_ms: u64,
    ) -> AppResult<Option<Versioned<WorkUnit>>> {
        let mut values = self.values.lock().unwrap();
        let Some(index) = values.iter().position(|unit| {
            unit.job_id() == job_id
                && unit.stage() == stage
                && unit.status() == WorkUnitStatus::Pending
        }) else {
            return Ok(None);
        };
        let unit = &mut values[index];
        unit.lease_for(owner, now_ms, now_ms + lease_ms)
            .map_err(ApplicationError::Domain)?;
        Ok(Some(Versioned::with_version(unit.clone(), 1)))
    }

    async fn retry_failed(&self, job_id: &JobId, from_stage: Option<StageKind>) -> AppResult<u32> {
        let mut values = self.values.lock().unwrap();
        let mut count = 0;
        for unit in values.iter_mut().filter(|unit| {
            unit.job_id() == job_id
                && matches!(
                    unit.status(),
                    WorkUnitStatus::Failed | WorkUnitStatus::Cancelled
                )
                && from_stage.is_none_or(|stage| unit.stage() as u8 >= stage as u8)
        }) {
            unit.retry().map_err(ApplicationError::Domain)?;
            count += 1;
        }
        Ok(count)
    }
}

struct LongArtifacts {
    units: Arc<LongUnits>,
}

struct LongStageCommits {
    units: Arc<LongUnits>,
}

#[async_trait]
impl StageCommitRepository for LongStageCommits {
    async fn commit_stage(&self, request: StageCommitRequest) -> AppResult<StageCommitResult> {
        if let Some((unit, expected)) = &request.work_unit {
            let mut values = self.units.values.lock().unwrap();
            let existing = values
                .iter_mut()
                .find(|candidate| candidate.id() == unit.id())
                .ok_or_else(|| ApplicationError::Invalid("long-audio WorkUnit missing".into()))?;
            *existing = unit.value.clone();
            return Ok(StageCommitResult {
                job: None,
                work_unit: Some(Versioned::with_version(
                    unit.value.clone(),
                    next_version(unit.version, *expected),
                )),
            });
        }
        Ok(StageCommitResult::default())
    }
}

#[async_trait]
impl ArtifactStore for LongArtifacts {
    async fn commit(&self, _commit: ArtifactCommit) -> AppResult<()> {
        Ok(())
    }

    async fn commit_transcript(&self, commit: TranscriptCommit) -> AppResult<ArtifactRef> {
        let artifact = ArtifactRef {
            id: commit.artifact_id,
            stage: commit.stage,
            path: commit.path.to_string_lossy().into_owned(),
            content_hash: format!("chunk-{}", commit.transcript.source_hash),
            schema_version: SCHEMA_VERSION,
            producer_fingerprint: commit.producer_fingerprint,
        };
        if let Some(unit_id) = commit.work_unit_id.as_ref() {
            self.units.complete(unit_id, artifact.clone());
        }
        Ok(artifact)
    }

    async fn load_transcript(&self, _artifact: &ArtifactRef) -> AppResult<Transcript> {
        Err(ApplicationError::Invalid(
            "long-audio fake does not load artifact transcripts".into(),
        ))
    }

    async fn validate(&self, _artifact: &ArtifactRef) -> AppResult<()> {
        Ok(())
    }
}

struct LongClock;

impl Clock for LongClock {
    fn now_ms(&self) -> u64 {
        1_000_000
    }
}

struct LongSession {
    descriptor: AsrDescriptor,
    calls: Arc<Mutex<Vec<PathBuf>>>,
    fail_chunk: Option<u32>,
    failed: bool,
}

impl LongSession {
    fn new(fail_chunk: Option<u32>) -> Self {
        Self {
            descriptor: AsrDescriptor {
                engine_id: "fake-long".into(),
                adapter_version: "test".into(),
                runtime_version: "test".into(),
                fingerprint: "fake-long|test|test|fake|cpu".into(),
                supports_word_timestamps: true,
                supports_confidence: true,
                cooperative_cancel: true,
                max_audio_secs: Some(120),
            },
            calls: Arc::new(Mutex::new(Vec::new())),
            fail_chunk,
            failed: false,
        }
    }

    fn calls(&self) -> Vec<PathBuf> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl AsrSession for LongSession {
    fn descriptor(&self) -> &AsrDescriptor {
        &self.descriptor
    }

    async fn transcribe(
        &mut self,
        request: AsrTranscribeRequest,
        _events: &dyn EventPublisher,
    ) -> AppResult<NormalizedAsrResult> {
        let path = request.audio_path;
        self.calls.lock().unwrap().push(path.clone());
        let index = path
            .file_stem()
            .and_then(|value| value.to_str())
            .and_then(|value| value.strip_prefix("chunk-"))
            .and_then(|value| value.parse::<u32>().ok())
            .ok_or_else(|| ApplicationError::Invalid("missing fake chunk index".into()))?;
        if self.fail_chunk == Some(index) && !self.failed {
            self.failed = true;
            return Err(ApplicationError::Adapter(VcError::new(
                ErrorCode::AsrFailed,
                "injected long-audio chunk failure",
            )));
        }
        let words = match index {
            0 => vec![
                Word {
                    text: "left".into(),
                    start_ms: 119_000,
                    end_ms: 119_500,
                    prob: 0.9,
                },
                Word {
                    text: "boundary".into(),
                    start_ms: 119_900,
                    end_ms: 120_100,
                    prob: 0.9,
                },
            ],
            1 => vec![
                Word {
                    text: "boundary".into(),
                    start_ms: 1_500,
                    end_ms: 1_700,
                    prob: 0.9,
                },
                Word {
                    text: "right".into(),
                    start_ms: 120_500,
                    end_ms: 121_000,
                    prob: 0.9,
                },
            ],
            2 => vec![Word {
                text: "tail".into(),
                start_ms: 6_500,
                end_ms: 7_000,
                prob: 0.9,
            }],
            _ => Vec::new(),
        };
        Ok(NormalizedAsrResult {
            transcript: Transcript::new_asr("chunk", EngineFingerprint::unknown(), words),
        })
    }

    async fn close(self: Box<Self>) -> AppResult<()> {
        Ok(())
    }
}

fn long_use_case(
    media: Arc<LongMedia>,
    plans: Arc<LongPlans>,
    cache: Arc<LongCache>,
    units: Arc<LongUnits>,
) -> TranscribeJob {
    TranscribeJob::new(
        Arc::new(FakeJobs {
            saved: Mutex::new(Vec::new()),
            fail_terminal_save: Arc::new(AtomicBool::new(false)),
        }),
        media,
        Arc::new(LongArtifacts {
            units: units.clone(),
        }),
        Arc::new(FakeSubtitles),
        Arc::new(FakeEvents),
        Arc::new(FakeIds),
        Arc::new(LongStageCommits {
            units: units.clone(),
        }),
    )
    .with_chunking(plans, cache, units, Arc::new(LongClock))
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

fn make_use_case(fail_export_once: Option<Arc<AtomicBool>>) -> (TranscribeJob, Arc<FakeJobs>) {
    let jobs = Arc::new(FakeJobs {
        saved: Mutex::new(Vec::new()),
        fail_terminal_save: Arc::new(AtomicBool::new(false)),
    });
    let artifacts = Arc::new(FakeArtifacts {
        committed: Mutex::new(Vec::new()),
        transcripts: Mutex::new(HashMap::new()),
        fail_export_once,
    });
    let use_case = TranscribeJob::new(
        jobs.clone(),
        Arc::new(FakeMedia),
        artifacts.clone(),
        Arc::new(FakeSubtitles),
        Arc::new(FakeEvents),
        Arc::new(FakeIds),
        Arc::new(FakeStageCommits { artifacts }),
    );
    (use_case, jobs)
}

fn use_case() -> (TranscribeJob, Arc<FakeJobs>) {
    make_use_case(None)
}

fn use_case_with_export_failure() -> (TranscribeJob, Arc<FakeJobs>, Arc<AtomicBool>) {
    let fail_export_once = Arc::new(AtomicBool::new(true));
    let (use_case, jobs) = make_use_case(Some(fail_export_once.clone()));
    (use_case, jobs, fail_export_once)
}

fn command(dir: &std::path::Path) -> TranscribeJobCommand {
    TranscribeJobCommand {
        job_id: UlidStr::from(Ulid::new()),
        batch_id: None,
        execution_snapshot_id: UlidStr::from(Ulid::new()),
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
        llm: None,
    }
}

fn session(transcript: Option<Transcript>, fail: bool) -> FakeSession {
    FakeSession {
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
async fn pending_job_rejects_a_different_execution_snapshot() {
    let dir = tempdir().unwrap();
    let (use_case, jobs) = use_case();
    let mut command = command(dir.path());
    jobs.saved.lock().unwrap().push(Job::new_with_snapshot(
        command.job_id.clone(),
        command.batch_id.clone(),
        command.execution_snapshot_id.clone(),
        command.profile_revision.clone(),
        command.input.to_string_lossy(),
    ));
    command.execution_snapshot_id = Ulid::new().into();

    let mut asr = session(Some(transcript()), false);
    let error = use_case.execute(command, &mut asr).await.unwrap_err();

    assert_eq!(error.into_vc_error().code, ErrorCode::InvalidArgument);
}

#[tokio::test]
async fn retry_after_export_failure_reuses_completed_transcript_stages() {
    let dir = tempdir().unwrap();
    let (use_case, jobs, export_failure) = use_case_with_export_failure();
    let first_command = command(dir.path());
    let mut first_session = session(Some(transcript()), false);
    let error = use_case
        .execute(first_command, &mut first_session)
        .await
        .unwrap_err();
    assert_eq!(error.into_vc_error().code, ErrorCode::ExportFailed);
    assert!(!export_failure.load(Ordering::SeqCst));

    let failed_job = jobs.saved.lock().unwrap().last().unwrap().clone();
    assert_eq!(
        failed_job.status(),
        videocaptionerr_domain::JobStatus::Failed
    );
    assert_eq!(
        failed_job
            .stages()
            .iter()
            .find(|stage| stage.kind == StageKind::Split)
            .unwrap()
            .status,
        StageStatus::Done
    );
    assert_eq!(
        failed_job
            .stages()
            .iter()
            .find(|stage| stage.kind == StageKind::Export)
            .unwrap()
            .status,
        StageStatus::Failed
    );

    let mut retry_job = failed_job.clone();
    retry_job.prepare_retry(None).unwrap();
    let mut retry_job = Versioned::with_version(retry_job, 1);
    jobs.save_job(&mut retry_job, ExpectedVersion::Exact(1))
        .await
        .unwrap();

    let mut retry_command = command(dir.path());
    retry_command.job_id = failed_job.id().clone();
    retry_command.execution_snapshot_id = failed_job
        .execution_snapshot_id()
        .cloned()
        .expect("retry fixture must have an execution snapshot");
    retry_command.profile_revision = failed_job.profile_revision().clone();
    let mut retry_session = session(None, true);
    let result = use_case
        .execute(retry_command, &mut retry_session)
        .await
        .unwrap();
    assert_eq!(result.job.status(), videocaptionerr_domain::JobStatus::Done);
    assert_eq!(
        result.transcript,
        videocaptionerr_domain::rule_split(
            &transcript(),
            &videocaptionerr_domain::RuleSplitConfig::default(),
        )
        .unwrap()
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

#[tokio::test]
async fn terminal_state_save_failure_is_returned_with_the_primary_error() {
    let dir = tempdir().unwrap();
    let (use_case, jobs) = use_case();
    jobs.fail_terminal_save.store(true, Ordering::SeqCst);
    let mut asr = session(None, true);

    let error = use_case
        .execute(command(dir.path()), &mut asr)
        .await
        .unwrap_err();

    assert!(matches!(error, ApplicationError::StatePersistence { .. }));
}

#[tokio::test]
async fn long_audio_uses_core_ownership_and_skips_cached_asr() {
    let dir = tempdir().unwrap();
    let media = Arc::new(LongMedia::default());
    let plans = Arc::new(LongPlans::default());
    let cache = Arc::new(LongCache::default());
    let units = Arc::new(LongUnits::default());
    let use_case = long_use_case(media.clone(), plans.clone(), cache.clone(), units.clone());
    let command = command(dir.path());
    let mut session = LongSession::new(None);

    let result = use_case
        .transcribe_asr(
            &command,
            &mut session,
            "media-hash",
            "pcm-hash",
            &command.job_dir.join("audio.wav"),
            250_000,
        )
        .await
        .unwrap();

    assert_eq!(
        result
            .words
            .iter()
            .map(|word| word.text.as_str())
            .collect::<Vec<_>>(),
        ["left", "boundary", "right", "tail"]
    );
    assert_eq!(*media.analyses.lock().unwrap(), 1);
    assert_eq!(plans.values.lock().unwrap().last().unwrap().chunks.len(), 3);
    let ranges = media.ranges.lock().unwrap().clone();
    assert_eq!(ranges.len(), 3);
    assert!(ranges.windows(2).all(|pair| pair[0].2 != pair[1].2));
    assert!(ranges.iter().all(|(start, end, _)| start < end));
    assert!(units
        .snapshot()
        .iter()
        .all(|unit| unit.status() == WorkUnitStatus::Done && unit.artifact().is_some()));

    let mut cached_session = LongSession::new(None);
    let cached = use_case
        .transcribe_asr(
            &command,
            &mut cached_session,
            "media-hash",
            "pcm-hash",
            &command.job_dir.join("audio.wav"),
            250_000,
        )
        .await
        .unwrap();
    assert_eq!(cached.words, result.words);
    assert!(cached_session.calls().is_empty());
    assert_eq!(media.ranges.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn failed_long_audio_chunk_is_retryable_without_rerunning_completed_chunks() {
    let dir = tempdir().unwrap();
    let media = Arc::new(LongMedia::default());
    let plans = Arc::new(LongPlans::default());
    let cache = Arc::new(LongCache::default());
    let units = Arc::new(LongUnits::default());
    let use_case = long_use_case(media, plans, cache.clone(), units.clone());
    let command = command(dir.path());
    let mut session = LongSession::new(Some(1));

    let error = use_case
        .transcribe_asr(
            &command,
            &mut session,
            "media-hash",
            "pcm-hash",
            &command.job_dir.join("audio.wav"),
            250_000,
        )
        .await
        .unwrap_err();
    assert_eq!(error.into_vc_error().code, ErrorCode::AsrFailed);
    let after_failure = units.snapshot();
    assert_eq!(after_failure.len(), 3);
    assert_eq!(
        after_failure
            .iter()
            .map(WorkUnit::status)
            .collect::<Vec<_>>(),
        [
            WorkUnitStatus::Done,
            WorkUnitStatus::Failed,
            WorkUnitStatus::Pending
        ]
    );
    assert_eq!(cache.values.lock().unwrap().len(), 1);

    assert_eq!(units.retry_failed(&command.job_id, None).await.unwrap(), 1);
    let result = use_case
        .transcribe_asr(
            &command,
            &mut session,
            "media-hash",
            "pcm-hash",
            &command.job_dir.join("audio.wav"),
            250_000,
        )
        .await
        .unwrap();
    let calls = session.calls();
    assert_eq!(calls.len(), 4);
    assert!(calls[0].ends_with("chunk-0000.wav"));
    assert!(calls[1].ends_with("chunk-0001.wav"));
    assert!(calls[2].ends_with("chunk-0001.wav"));
    assert!(calls[3].ends_with("chunk-0002.wav"));
    assert_eq!(
        result
            .words
            .iter()
            .map(|word| word.text.as_str())
            .collect::<Vec<_>>(),
        ["left", "boundary", "right", "tail"]
    );
    assert!(units
        .snapshot()
        .iter()
        .all(|unit| unit.status() == WorkUnitStatus::Done));
}

#[tokio::test]
async fn corrupt_completed_chunk_cache_fails_explicitly() {
    let dir = tempdir().unwrap();
    let media = Arc::new(LongMedia::default());
    let plans = Arc::new(LongPlans::default());
    let cache = Arc::new(LongCache::default());
    let units = Arc::new(LongUnits::default());
    let use_case = long_use_case(media, plans, cache.clone(), units);
    let command = command(dir.path());
    let mut initial = LongSession::new(None);
    use_case
        .transcribe_asr(
            &command,
            &mut initial,
            "media-hash",
            "pcm-hash",
            &command.job_dir.join("audio.wav"),
            250_000,
        )
        .await
        .unwrap();
    cache.corrupt_first();

    let mut retry = LongSession::new(None);
    let error = use_case
        .transcribe_asr(
            &command,
            &mut retry,
            "media-hash",
            "pcm-hash",
            &command.job_dir.join("audio.wav"),
            250_000,
        )
        .await
        .unwrap_err();
    assert_eq!(error.into_vc_error().code, ErrorCode::CacheCorrupt);
    assert!(retry.calls().is_empty());
}
