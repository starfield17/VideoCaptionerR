//! ASR engine trait and result types.

use std::path::Path;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use videocaptionerr_contracts::error::VcResult;
use videocaptionerr_contracts::protocol::{SegmentData, SegmentWord};

use crate::descriptor::EngineDescriptor;
use crate::options::AsrOptions;

/// Streaming events from an ASR engine during transcription.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AsrEvent {
    Progress {
        processed_ms: Option<u64>,
        total_ms: Option<u64>,
        message: Option<String>,
    },
    Segment(SegmentData),
    Language {
        language: String,
    },
    Log {
        level: String,
        message: String,
    },
}

/// Raw ASR result before normalization into Transcript IR.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsrRawResult {
    pub language: Option<String>,
    pub segments: Vec<SegmentData>,
    pub duration_ms: Option<u64>,
    pub words: Vec<SegmentWord>,
    pub engine_id: String,
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_digest: Option<String>,
}

#[async_trait]
pub trait AsrEngine: Send + Sync {
    fn descriptor(&self) -> &EngineDescriptor;

    async fn transcribe(
        &self,
        audio: &Path,
        opts: &AsrOptions,
        sink: mpsc::Sender<AsrEvent>,
    ) -> VcResult<AsrRawResult>;
}
