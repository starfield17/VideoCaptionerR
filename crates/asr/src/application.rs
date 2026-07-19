//! Application-port implementation backed by the existing worker client.

use std::path::PathBuf;

use async_trait::async_trait;
use tokio::sync::mpsc;
use videocaptionerr_contracts::error::{ErrorCode, VcError};
use videocaptionerr_core::application_error::{AppResult, ApplicationError};
use videocaptionerr_core::ports::{
    AsrDescriptor as ApplicationAsrDescriptor, AsrRuntime, AsrSession, AsrTranscribeRequest,
    EventPublisher, NormalizedAsrResult,
};
use videocaptionerr_domain::BatchExecutionProfile;

use crate::descriptor::{ConfidenceKind, EngineDescriptor, TimestampGranularity};
use crate::normalize::{normalize_asr, NormalizeOptions};
use crate::options::AsrOptions;
use crate::worker::WorkerClient;

#[derive(Debug, Clone)]
pub struct WorkerAsrRuntime {
    helper_path: PathBuf,
    engine: String,
    model_path: Option<PathBuf>,
}

impl WorkerAsrRuntime {
    pub fn new(
        helper_path: impl Into<PathBuf>,
        engine: impl Into<String>,
        model_path: Option<PathBuf>,
    ) -> Self {
        Self {
            helper_path: helper_path.into(),
            engine: engine.into(),
            model_path,
        }
    }
}

#[async_trait]
impl AsrRuntime for WorkerAsrRuntime {
    async fn open_session(
        &self,
        profile: &BatchExecutionProfile,
    ) -> AppResult<Box<dyn AsrSession>> {
        if profile.asr_engine != self.engine {
            return Err(ApplicationError::Invalid(format!(
                "ASR runtime engine {} does not match Batch engine {}",
                self.engine, profile.asr_engine
            )));
        }
        let mut client = WorkerClient::spawn(&self.helper_path, &self.engine)
            .await
            .map_err(ApplicationError::Adapter)?;
        client
            .load_model(self.model_path.as_deref())
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
        let application_descriptor = map_descriptor(&descriptor);
        Ok(Box::new(WorkerAsrSession {
            client,
            descriptor: application_descriptor,
            device: profile.device.clone(),
        }))
    }
}

struct WorkerAsrSession {
    client: WorkerClient,
    descriptor: ApplicationAsrDescriptor,
    device: String,
}

#[async_trait]
impl AsrSession for WorkerAsrSession {
    fn descriptor(&self) -> &ApplicationAsrDescriptor {
        &self.descriptor
    }

    async fn transcribe(
        &mut self,
        request: AsrTranscribeRequest,
        _events: &dyn EventPublisher,
    ) -> AppResult<NormalizedAsrResult> {
        let (sink, mut events) = mpsc::channel(256);
        let drain = tokio::spawn(async move { while events.recv().await.is_some() {} });
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
            .await
            .map_err(ApplicationError::Adapter)?;
        let _ = drain.await;
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

fn map_descriptor(descriptor: &EngineDescriptor) -> ApplicationAsrDescriptor {
    ApplicationAsrDescriptor {
        engine_id: descriptor.engine_id.clone(),
        adapter_version: descriptor.adapter_version.clone(),
        runtime_version: descriptor.runtime_version.clone(),
        supports_word_timestamps: matches!(
            descriptor.timestamp_granularity,
            TimestampGranularity::Word | TimestampGranularity::Character
        ),
        supports_confidence: !matches!(descriptor.confidence_kind, ConfidenceKind::None),
        cooperative_cancel: descriptor.cooperative_cancel,
    }
}
