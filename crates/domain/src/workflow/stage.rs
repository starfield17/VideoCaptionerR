use crate::identity::UlidStr;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    Done,
    DoneDegraded,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Done | Self::DoneDegraded | Self::Failed | Self::Cancelled
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageKind {
    Probe,
    ExtractAudio,
    Asr,
    Split,
    Correct,
    Translate,
    Export,
}

impl StageKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Probe => "probe",
            Self::ExtractAudio => "extract_audio",
            Self::Asr => "asr",
            Self::Split => "split",
            Self::Correct => "correct",
            Self::Translate => "translate",
            Self::Export => "export",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value.trim().to_ascii_lowercase().as_str() {
            "probe" => Self::Probe,
            "extract_audio" | "extract-audio" => Self::ExtractAudio,
            "asr" => Self::Asr,
            "split" => Self::Split,
            "correct" => Self::Correct,
            "translate" => Self::Translate,
            "export" => Self::Export,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageStatus {
    Pending,
    WaitingResource,
    Running,
    Retrying,
    Done,
    DoneDegraded,
    Failed,
    Skipped,
    Cancelled,
    WaitingProvider,
}

impl StageStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Done | Self::DoneDegraded | Self::Failed | Self::Skipped | Self::Cancelled
        )
    }

    pub(super) fn prerequisite_satisfied(self) -> bool {
        matches!(self, Self::Done | Self::DoneDegraded | Self::Skipped)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageState {
    pub kind: StageKind,
    pub status: StageStatus,
    pub artifact: Option<ArtifactRef>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub id: UlidStr,
    pub stage: StageKind,
    pub path: String,
    pub content_hash: String,
    pub schema_version: u32,
    pub producer_fingerprint: String,
}
