//! Immutable, secret-free execution inputs captured for one Job.
//!
//! A snapshot is the durable authority for resuming a Job. It deliberately
//! contains effective values rather than references to mutable configuration
//! files. Provider credentials are never represented by this type.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use videocaptionerr_domain::{BatchId, JobId, UlidStr};

use crate::ports::{LlmStage, PromptSnapshot, StructuredOutput};

pub const JOB_EXECUTION_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceStatSnapshot {
    pub size: u64,
    pub modified_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AudioStreamSelection {
    Auto,
    Explicit { index: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AsrExecutionSnapshot {
    pub engine: String,
    pub model_locator: String,
    pub model_id: Option<String>,
    pub model_digest: Option<String>,
    pub device: String,
    pub compute_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputPlanSnapshot {
    pub path: String,
    pub format: String,
    pub layout: String,
    pub conflict_policy: String,
    pub fallback_to_source: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmExecutionSnapshot {
    pub provider_profile_revision: String,
    pub model: String,
    pub max_context_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub chars_per_token: f64,
    pub structured_output: StructuredOutput,
    pub seed: Option<i64>,
    pub target_language: String,
    pub split_prompt: PromptSnapshot,
    pub correct_prompt: PromptSnapshot,
    pub translate_prompt: PromptSnapshot,
}

impl LlmExecutionSnapshot {
    pub fn prompt(&self, stage: LlmStage) -> &PromptSnapshot {
        match stage {
            LlmStage::Split => &self.split_prompt,
            LlmStage::Correct => &self.correct_prompt,
            LlmStage::Translate => &self.translate_prompt,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobExecutionSnapshot {
    pub snapshot_id: UlidStr,
    pub schema_version: u32,
    pub created_at: String,
    pub job_id: JobId,
    pub batch_id: BatchId,
    pub canonical_source_path: String,
    pub source_stat: SourceStatSnapshot,
    pub job_dir: String,
    pub profile_revision: UlidStr,
    pub asr: AsrExecutionSnapshot,
    pub audio_stream: AudioStreamSelection,
    pub source_language: Option<String>,
    pub target_language: Option<String>,
    pub output: OutputPlanSnapshot,
    pub llm: Option<LlmExecutionSnapshot>,
}

impl JobExecutionSnapshot {
    pub fn job_dir_path(&self) -> PathBuf {
        PathBuf::from(&self.job_dir)
    }

    pub fn source_path(&self) -> PathBuf {
        PathBuf::from(&self.canonical_source_path)
    }

    pub fn output_path(&self) -> PathBuf {
        PathBuf::from(&self.output.path)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != JOB_EXECUTION_SNAPSHOT_SCHEMA_VERSION {
            return Err(format!(
                "unsupported execution snapshot schema version {}",
                self.schema_version
            ));
        }
        if self.job_id.as_str().is_empty() || self.batch_id.as_str().is_empty() {
            return Err("execution snapshot Job id cannot be empty".into());
        }
        if self.canonical_source_path.is_empty() {
            return Err("execution snapshot source path cannot be empty".into());
        }
        if self.job_dir.is_empty() || self.output.path.is_empty() {
            return Err("execution snapshot paths cannot be empty".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use ulid::Ulid;

    use super::*;
    use crate::ports::{PromptSnapshot, StructuredOutput};

    fn prompt(stage: LlmStage, body: &str) -> PromptSnapshot {
        PromptSnapshot {
            schema_version: 1,
            stage,
            files: BTreeMap::from([(String::from("system.txt"), body.to_owned())]),
            content_hash: format!("hash-{body}"),
        }
    }

    fn snapshot() -> JobExecutionSnapshot {
        let job_id = Ulid::new().into();
        let batch_id = Ulid::new().into();
        JobExecutionSnapshot {
            snapshot_id: Ulid::new().into(),
            schema_version: JOB_EXECUTION_SNAPSHOT_SCHEMA_VERSION,
            created_at: "2026-07-20T00:00:00Z".into(),
            job_id,
            batch_id,
            canonical_source_path: "/media/input.mp4".into(),
            source_stat: SourceStatSnapshot {
                size: 42,
                modified_at_ms: Some(1_725_000_000_000),
            },
            job_dir: "/jobs/01JOB".into(),
            profile_revision: Ulid::new().into(),
            asr: AsrExecutionSnapshot {
                engine: "whisper-cpp".into(),
                model_locator: "/models/ggml-tiny.bin".into(),
                model_id: Some("tiny.en".into()),
                model_digest: Some("blake3:model".into()),
                device: "cpu".into(),
                compute_type: "default".into(),
            },
            audio_stream: AudioStreamSelection::Explicit { index: 1 },
            source_language: Some("en".into()),
            target_language: Some("zh".into()),
            output: OutputPlanSnapshot {
                path: "/exports/input.zh.source.srt".into(),
                format: "srt".into(),
                layout: "source".into(),
                conflict_policy: "rename".into(),
                fallback_to_source: true,
            },
            llm: Some(LlmExecutionSnapshot {
                provider_profile_revision: "provider-rev-1".into(),
                model: "gpt-test".into(),
                max_context_tokens: Some(8_192),
                max_output_tokens: Some(1_024),
                chars_per_token: 4.0,
                structured_output: StructuredOutput::JsonObject,
                seed: Some(7),
                target_language: "zh".into(),
                split_prompt: prompt(LlmStage::Split, "split prompt"),
                correct_prompt: prompt(LlmStage::Correct, "correct prompt"),
                translate_prompt: prompt(LlmStage::Translate, "translate prompt"),
            }),
        }
    }

    #[test]
    fn snapshot_round_trips_through_json_and_toml() {
        let original = snapshot();

        let json = serde_json::to_string(&original).unwrap();
        let from_json: JobExecutionSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(from_json, original);

        let toml = toml::to_string(&original).unwrap();
        let from_toml: JobExecutionSnapshot = toml::from_str(&toml).unwrap();
        assert_eq!(from_toml, original);
    }

    #[test]
    fn snapshot_serialization_has_no_api_key_field_or_value() {
        let snapshot = snapshot();
        let json = serde_json::to_string(&snapshot).unwrap();

        assert!(!json.contains("api_key"));
        assert!(!json.contains("sk-test"));
        assert!(!json.contains("API_KEY"));
    }

    #[test]
    fn snapshot_validation_rejects_an_unsupported_schema() {
        let mut snapshot = snapshot();
        snapshot.schema_version += 1;

        assert!(snapshot.validate().is_err());
    }
}
