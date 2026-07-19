//! Media probe contracts. ffprobe is authoritative.

use serde::{Deserialize, Serialize};

/// One audio stream from ffprobe (global stream index).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioStream {
    pub stream_index: u32,
    pub codec: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub channels: u16,
    pub sample_rate: u32,
    pub is_default: bool,
}

/// Probe result for an input media file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaProbe {
    pub schema_version: u32,
    pub input_size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    pub duration_ms: u64,
    pub audio_streams: Vec<AudioStream>,
}

impl MediaProbe {
    pub fn has_audio(&self) -> bool {
        !self.audio_streams.is_empty()
    }

    /// Auto-select only when exactly one usable stream exists.
    pub fn auto_select_stream(&self) -> Option<&AudioStream> {
        if self.audio_streams.len() == 1 {
            self.audio_streams.first()
        } else {
            None
        }
    }

    pub fn default_stream(&self) -> Option<&AudioStream> {
        self.audio_streams
            .iter()
            .find(|s| s.is_default)
            .or_else(|| self.audio_streams.first())
    }
}
