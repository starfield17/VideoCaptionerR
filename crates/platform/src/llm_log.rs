//! Metadata-only LLM request log adapter.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use videocaptionerr_contracts::error::{ErrorCode, VcError};
use videocaptionerr_core::application_error::{AppResult, ApplicationError};
use videocaptionerr_core::ports::{LlmRequestMetadata, LlmRequestRecorder};

#[derive(Clone)]
pub struct FileLlmRequestRecorder {
    path: Arc<PathBuf>,
    write_lock: Arc<Mutex<()>>,
}

impl FileLlmRequestRecorder {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: Arc::new(path.into()),
            write_lock: Arc::new(Mutex::new(())),
        }
    }
}

#[async_trait]
impl LlmRequestRecorder for FileLlmRequestRecorder {
    async fn record(&self, metadata: LlmRequestMetadata) -> AppResult<()> {
        let _guard = self.write_lock.lock().await;
        let parent = self.path.parent().map(PathBuf::from);
        if let Some(parent) = parent {
            tokio::fs::create_dir_all(parent).await.map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("create LLM log directory: {error}"),
                ))
            })?;
        }
        let mut line = serde_json::to_vec(&metadata).map_err(|error| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode LLM request metadata: {error}"),
            ))
        })?;
        line.push(b'\n');
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.path.as_path())
            .await
            .map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("open LLM request log: {error}"),
                ))
            })?;
        tokio::io::AsyncWriteExt::write_all(&mut file, &line)
            .await
            .map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("write LLM request log: {error}"),
                ))
            })?;
        Ok(())
    }
}
