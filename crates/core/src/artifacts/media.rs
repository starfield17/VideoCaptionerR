//! Typed media-stage manifests used for crash-safe resume.
//!
//! Probe and Extract stages commit these documents as their stage artifacts.
//! Resume paths load and validate them instead of re-running ffprobe/ffmpeg.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use videocaptionerr_contracts::media::MediaProbe;
use videocaptionerr_domain::UlidStr;

use crate::execution_snapshot::SourceStatSnapshot;

pub const PROBE_MANIFEST_SCHEMA_VERSION: u32 = 1;
pub const EXTRACT_MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Durable Probe-stage result. Resume reuses this without calling ffprobe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeManifest {
    pub schema_version: u32,
    pub source_path: String,
    pub source_stat: SourceStatSnapshot,
    pub source_hash: String,
    pub probe: MediaProbe,
    pub selected_stream_index: u32,
    pub producer: String,
}

impl ProbeManifest {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != PROBE_MANIFEST_SCHEMA_VERSION {
            return Err(format!(
                "unsupported probe manifest schema version {}",
                self.schema_version
            ));
        }
        if self.source_path.is_empty() || self.source_hash.is_empty() {
            return Err("probe manifest source identity is incomplete".into());
        }
        if !self.probe.has_audio() {
            return Err("probe manifest has no audio streams".into());
        }
        if !self
            .probe
            .audio_streams
            .iter()
            .any(|stream| stream.stream_index == self.selected_stream_index)
        {
            return Err(format!(
                "probe manifest selected stream {} is not present",
                self.selected_stream_index
            ));
        }
        Ok(())
    }
}

/// Durable Extract-stage result. Resume reuses the WAV without calling ffmpeg.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtractManifest {
    pub schema_version: u32,
    pub probe_artifact_id: UlidStr,
    pub stream_index: u32,
    pub wav_path: String,
    pub wav_content_hash: String,
    pub pcm_hash: String,
    pub sample_rate: u32,
    pub channels: u16,
    pub duration_ms: u64,
    pub producer: String,
}

impl ExtractManifest {
    pub fn wav_path_buf(&self) -> PathBuf {
        PathBuf::from(&self.wav_path)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != EXTRACT_MANIFEST_SCHEMA_VERSION {
            return Err(format!(
                "unsupported extract manifest schema version {}",
                self.schema_version
            ));
        }
        if self.wav_path.is_empty()
            || self.wav_content_hash.is_empty()
            || self.pcm_hash.is_empty()
        {
            return Err("extract manifest audio identity is incomplete".into());
        }
        if self.sample_rate == 0 || self.channels == 0 {
            return Err("extract manifest audio parameters are invalid".into());
        }
        Ok(())
    }
}
