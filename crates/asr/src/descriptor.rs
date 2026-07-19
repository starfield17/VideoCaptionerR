//! Engine capability descriptors from handshake/probing.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampGranularity {
    None,
    Segment,
    Word,
    Character,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfidenceKind {
    None,
    WordProb,
    LogProb,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceDescriptor {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub is_default: bool,
}

/// Capability level for pipeline eligibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CapabilityLevel {
    /// Full text only — not allowed in full subtitle pipeline.
    A0 = 0,
    /// Segment text + segment timestamps — degraded/experimental.
    A1 = 1,
    /// Word/character text + start/end — minimum for full v1 pipeline.
    A2 = 2,
    /// A2 + meaningful confidence.
    A3 = 3,
}

impl EngineDescriptor {
    pub fn capability_level(&self) -> CapabilityLevel {
        match self.timestamp_granularity {
            TimestampGranularity::None => CapabilityLevel::A0,
            TimestampGranularity::Segment => CapabilityLevel::A1,
            TimestampGranularity::Word | TimestampGranularity::Character => {
                if matches!(self.confidence_kind, ConfidenceKind::None) {
                    CapabilityLevel::A2
                } else {
                    CapabilityLevel::A3
                }
            }
        }
    }

    pub fn supports_full_pipeline(&self) -> bool {
        self.capability_level() >= CapabilityLevel::A2 && self.unavailable_reason.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EngineDescriptor {
    pub protocol_version: u32,
    pub engine_id: String,
    pub adapter_version: String,
    pub runtime_version: String,
    pub devices: Vec<DeviceDescriptor>,
    pub timestamp_granularity: TimestampGranularity,
    pub confidence_kind: ConfidenceKind,
    pub native_vad: bool,
    pub language_detection: bool,
    pub streaming_events: bool,
    pub cooperative_cancel: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_audio_secs: Option<u32>,
    pub supported_options: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<String>,
}

impl EngineDescriptor {
    pub fn fake_worker() -> Self {
        let mut supported = BTreeSet::new();
        supported.insert("language".into());
        supported.insert("beam_size".into());
        Self {
            protocol_version: videocaptionerr_contracts::protocol::PROTOCOL_VERSION,
            engine_id: "fake".into(),
            adapter_version: env!("CARGO_PKG_VERSION").into(),
            runtime_version: "test".into(),
            devices: vec![DeviceDescriptor {
                id: "cpu".into(),
                name: "CPU".into(),
                is_default: true,
            }],
            timestamp_granularity: TimestampGranularity::Word,
            confidence_kind: ConfidenceKind::WordProb,
            native_vad: false,
            language_detection: true,
            streaming_events: true,
            cooperative_cancel: true,
            max_audio_secs: Some(3600),
            supported_options: supported,
            unavailable_reason: None,
        }
    }
}
