//! Application-port implementation backed by the existing worker client.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;
use videocaptionerr_contracts::error::{ErrorCode, VcError};
use videocaptionerr_core::application_error::{AppResult, ApplicationError};
use videocaptionerr_core::ports::{
    asr_fingerprint, cancel_grace, ApplicationEvent, AsrCancelToken,
    AsrDescriptor as ApplicationAsrDescriptor, AsrRuntime, AsrRuntimeSpec, AsrSession,
    AsrSessionControl, AsrTranscribeRequest, EventPublisher, ModelLocator, NormalizedAsrResult,
};
use videocaptionerr_domain::BatchExecutionProfile;

use crate::descriptor::{ConfidenceKind, EngineDescriptor, TimestampGranularity};
use crate::engine::AsrEvent;
use crate::normalize::{normalize_asr, NormalizeOptions};
use crate::options::AsrOptions;
use crate::worker::{WorkerClient, WorkerControl};

#[derive(Debug, Clone)]
pub struct WorkerAsrRuntime {
    launch: WorkerLaunch,
    engine: String,
    spec: AsrRuntimeSpec,
}

#[derive(Debug, Clone)]
enum WorkerLaunch {
    Helper { path: PathBuf },
    Python { python: PathBuf, script: PathBuf },
}

impl WorkerAsrRuntime {
    pub fn helper(
        helper_path: impl Into<PathBuf>,
        engine: impl Into<String>,
        spec: AsrRuntimeSpec,
    ) -> Self {
        Self {
            launch: WorkerLaunch::Helper {
                path: helper_path.into(),
            },
            engine: engine.into(),
            spec,
        }
    }

    pub fn python(
        python_path: impl Into<PathBuf>,
        worker_script: impl Into<PathBuf>,
        engine: impl Into<String>,
        spec: AsrRuntimeSpec,
    ) -> Self {
        Self {
            launch: WorkerLaunch::Python {
                python: python_path.into(),
                script: worker_script.into(),
            },
            engine: engine.into(),
            spec,
        }
    }

    /// Back-compat constructor used by older call sites / tests.
    pub fn new(
        helper_path: impl Into<PathBuf>,
        engine: impl Into<String>,
        model_path: Option<PathBuf>,
    ) -> Self {
        let engine = engine.into();
        let locator = match model_path {
            Some(p) if p.is_dir() => ModelLocator::directory(p.to_string_lossy()),
            Some(p) => ModelLocator::file(p.to_string_lossy()),
            None => ModelLocator::file(format!("{engine}:default")),
        };
        let model_id = locator.display();
        Self::helper(
            helper_path,
            engine.clone(),
            AsrRuntimeSpec {
                engine_family: engine,
                model_id,
                verified_digest: None,
                locator,
                device: "cpu".into(),
                compute_type: "default".into(),
            },
        )
    }

    fn model_load_path(&self) -> Option<PathBuf> {
        match &self.spec.locator {
            ModelLocator::File { path } | ModelLocator::Directory { path } => {
                Some(PathBuf::from(path))
            }
            ModelLocator::HuggingFaceSnapshot {
                repo_id,
                revision,
                path,
            } => {
                // Python engines accept HF repo ids; prefer explicit local path.
                if let Some(local) = path {
                    Some(PathBuf::from(local))
                } else {
                    Some(PathBuf::from(format!("{repo_id}@{revision}")))
                }
            }
        }
    }
}

#[async_trait]
impl AsrRuntime for WorkerAsrRuntime {
    async fn open_session(
        &self,
        profile: &BatchExecutionProfile,
    ) -> AppResult<Box<dyn AsrSession>> {
        if profile.asr_engine != self.engine && profile.asr_engine != self.spec.engine_family {
            return Err(ApplicationError::Invalid(format!(
                "ASR runtime engine {} does not match Batch engine {}",
                self.engine, profile.asr_engine
            )));
        }
        let mut client = match &self.launch {
            WorkerLaunch::Helper { path } => WorkerClient::spawn(path, &self.engine).await,
            WorkerLaunch::Python { python, script } => {
                WorkerClient::spawn_python(python, script, &self.engine).await
            }
        }
        .map_err(ApplicationError::Adapter)?;

        // Pass device/compute through load payload via model path + options side channel.
        client
            .load_model_with_options(
                self.model_load_path().as_deref(),
                &self.spec.device,
                &self.spec.compute_type,
            )
            .await
            .map_err(ApplicationError::Adapter)?;
        let descriptor = client.descriptor().cloned().ok_or_else(|| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::WorkerProtocolError,
                "worker did not provide an engine descriptor",
            ))
        })?;
        if !descriptor.supports_full_pipeline() {
            return Err(ApplicationError::Adapter(VcError::new(
                ErrorCode::EngineCapabilityInsufficient,
                "ASR worker does not provide A2 word/character timestamps",
            )));
        }
        let mut application_descriptor = map_descriptor(&descriptor);
        let options_hash = blake3::hash(
            format!(
                "word_timestamps=true|device={}|compute={}",
                self.spec.device, self.spec.compute_type
            )
            .as_bytes(),
        )
        .to_hex()
        .to_string();
        application_descriptor.fingerprint = asr_fingerprint(
            &application_descriptor.engine_id,
            &application_descriptor.adapter_version,
            &application_descriptor.runtime_version,
            &self.spec,
            &options_hash,
        );
        let control: Arc<dyn AsrSessionControl> = Arc::new(WorkerControlAdapter(client.control()));
        Ok(Box::new(WorkerAsrSession {
            client,
            descriptor: application_descriptor,
            device: self.spec.device.clone(),
            control,
        }))
    }
}

struct WorkerControlAdapter(WorkerControl);

#[async_trait]
impl AsrSessionControl for WorkerControlAdapter {
    async fn request_cancel(&self) -> AppResult<()> {
        self.0
            .cancel_current()
            .await
            .map_err(ApplicationError::Adapter)
    }

    async fn ping(&self) -> AppResult<()> {
        self.0.ping().await.map_err(ApplicationError::Adapter)
    }
}

struct WorkerAsrSession {
    client: WorkerClient,
    descriptor: ApplicationAsrDescriptor,
    device: String,
    control: Arc<dyn AsrSessionControl>,
}

#[async_trait]
impl AsrSession for WorkerAsrSession {
    fn descriptor(&self) -> &ApplicationAsrDescriptor {
        &self.descriptor
    }

    fn control(&self) -> Option<Arc<dyn AsrSessionControl>> {
        Some(self.control.clone())
    }

    async fn transcribe(
        &mut self,
        request: AsrTranscribeRequest,
        events: &dyn EventPublisher,
        cancel: Option<AsrCancelToken>,
    ) -> AppResult<NormalizedAsrResult> {
        let (sink, mut rx) = mpsc::channel(256);
        let buffered = Arc::new(Mutex::new(Vec::<AsrEvent>::new()));
        let buffer = buffered.clone();
        let collector = tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                buffer.lock().unwrap().push(event);
            }
        });

        let control = self.client.control();
        let cancel_watch = cancel.clone();
        let cancel_task = tokio::spawn(async move {
            let Some(token) = cancel_watch else {
                return;
            };
            loop {
                if token.is_requested() {
                    let _ = control.cancel_current().await;
                    tokio::time::sleep(cancel_grace()).await;
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });

        let raw = self
            .client
            .transcribe(
                &request.audio_path,
                &AsrOptions {
                    language: request.language,
                    model_path: None,
                    device: Some(self.device.clone()),
                    word_timestamps: true,
                    ..Default::default()
                },
                sink,
                None,
            )
            .await;
        cancel_task.abort();
        let _ = collector.await;
        let collected: Vec<AsrEvent> = std::mem::take(&mut *buffered.lock().unwrap());
        for event in collected {
            let _ = events.publish_live(map_asr_event(event)).await;
        }

        let raw = raw.map_err(ApplicationError::Adapter)?;
        if cancel.as_ref().is_some_and(AsrCancelToken::is_requested) {
            return Err(ApplicationError::Cancelled);
        }
        let transcript = normalize_asr(
            &raw,
            &NormalizeOptions {
                source_hash: request.source_hash,
                duration_ms: request.duration_ms,
                device: Some(self.device.clone()),
            },
        )
        .map_err(ApplicationError::Adapter)?;
        Ok(NormalizedAsrResult { transcript })
    }

    async fn close(mut self: Box<Self>) -> AppResult<()> {
        let unload = self
            .client
            .unload_model()
            .await
            .map_err(ApplicationError::Adapter);
        let shutdown = self
            .client
            .shutdown()
            .await
            .map_err(ApplicationError::Adapter);
        match (unload, shutdown) {
            (Err(error), _) => Err(error),
            (Ok(()), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }
}

fn map_asr_event(event: AsrEvent) -> ApplicationEvent {
    match event {
        AsrEvent::Progress {
            processed_ms,
            total_ms,
            message,
        } => ApplicationEvent::Progress {
            job_id: None,
            processed_ms,
            total_ms,
            message,
        },
        AsrEvent::Language { language } => ApplicationEvent::Language {
            job_id: None,
            language,
        },
        AsrEvent::Segment(segment) => ApplicationEvent::Segment {
            job_id: None,
            text: segment.text,
            start_ms: segment.start_ms,
            end_ms: segment.end_ms,
        },
        AsrEvent::Log { level, message } => ApplicationEvent::Log {
            job_id: None,
            level,
            message,
        },
    }
}

fn map_descriptor(descriptor: &EngineDescriptor) -> ApplicationAsrDescriptor {
    ApplicationAsrDescriptor {
        engine_id: descriptor.engine_id.clone(),
        adapter_version: descriptor.adapter_version.clone(),
        runtime_version: descriptor.runtime_version.clone(),
        fingerprint: format!(
            "{}|{}|{}",
            descriptor.engine_id, descriptor.adapter_version, descriptor.runtime_version
        ),
        supports_word_timestamps: matches!(
            descriptor.timestamp_granularity,
            TimestampGranularity::Word | TimestampGranularity::Character
        ),
        supports_confidence: !matches!(descriptor.confidence_kind, ConfidenceKind::None),
        cooperative_cancel: descriptor.cooperative_cancel,
        max_audio_secs: descriptor.max_audio_secs,
    }
}
