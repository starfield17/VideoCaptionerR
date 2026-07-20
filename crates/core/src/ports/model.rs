//! Typed model location and ASR runtime specification.
//!
//! Domain keeps a logical [`BatchExecutionProfile`]. Application owns the
//! concrete locator, digest, and resolver used to open a Batch-scoped runtime.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::application_error::{AppResult, ApplicationError};
use crate::ports::AsrRuntime;

/// Where model weights live on disk or as a remote snapshot reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ModelLocator {
    File {
        path: String,
    },
    Directory {
        path: String,
    },
    HuggingFaceSnapshot {
        repo_id: String,
        revision: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
    },
}

impl ModelLocator {
    pub fn file(path: impl Into<String>) -> Self {
        Self::File { path: path.into() }
    }

    pub fn directory(path: impl Into<String>) -> Self {
        Self::Directory { path: path.into() }
    }

    pub fn hugging_face(
        repo_id: impl Into<String>,
        revision: impl Into<String>,
        path: Option<String>,
    ) -> Self {
        Self::HuggingFaceSnapshot {
            repo_id: repo_id.into(),
            revision: revision.into(),
            path,
        }
    }

    /// Parse a legacy v1 plain-string locator as a file path.
    pub fn from_v1_string(s: &str) -> Self {
        let path = Path::new(s);
        if path.is_dir() {
            Self::directory(s)
        } else {
            Self::file(s)
        }
    }

    pub fn display(&self) -> String {
        match self {
            Self::File { path } | Self::Directory { path } => path.clone(),
            Self::HuggingFaceSnapshot {
                repo_id,
                revision,
                path,
            } => match path {
                Some(p) => format!("hf:{repo_id}@{revision}:{p}"),
                None => format!("hf:{repo_id}@{revision}"),
            },
        }
    }

    /// Local filesystem path used to load/verify weights when applicable.
    pub fn local_path(&self) -> Option<PathBuf> {
        match self {
            Self::File { path } | Self::Directory { path } => Some(PathBuf::from(path)),
            Self::HuggingFaceSnapshot { path, .. } => path.as_ref().map(PathBuf::from),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::File { path } => {
                if path.is_empty() {
                    return Err("model file locator path cannot be empty".into());
                }
                let p = Path::new(path);
                if p.exists() && !p.is_file() {
                    return Err(format!("model locator is not a file: {path}"));
                }
                Ok(())
            }
            Self::Directory { path } => {
                if path.is_empty() {
                    return Err("model directory locator path cannot be empty".into());
                }
                let p = Path::new(path);
                if p.exists() && !p.is_dir() {
                    return Err(format!("model locator is not a directory: {path}"));
                }
                Ok(())
            }
            Self::HuggingFaceSnapshot {
                repo_id, revision, ..
            } => {
                if repo_id.is_empty() {
                    return Err("HuggingFace repo_id cannot be empty".into());
                }
                if revision.is_empty() {
                    return Err("HuggingFace revision cannot be empty".into());
                }
                Ok(())
            }
        }
    }
}

/// Complete, secret-free identity of one Batch-scoped ASR runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AsrRuntimeSpec {
    pub engine_family: String,
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_digest: Option<String>,
    pub locator: ModelLocator,
    pub device: String,
    pub compute_type: String,
}

impl AsrRuntimeSpec {
    pub fn validate(&self) -> Result<(), String> {
        if self.engine_family.is_empty() {
            return Err("ASR engine family cannot be empty".into());
        }
        if self.model_id.is_empty() {
            return Err("ASR model id cannot be empty".into());
        }
        if self.device.is_empty() {
            return Err("ASR device cannot be empty".into());
        }
        if self.compute_type.is_empty() {
            return Err("ASR compute type cannot be empty".into());
        }
        self.locator.validate()?;
        Ok(())
    }

    /// Cache-safe fingerprint fragment for engine + model + device + compute.
    /// Callers must still append adapter/runtime versions and output-affecting options.
    pub fn identity_key(&self) -> String {
        format!(
            "{}|{}|{}|{}|{}|{}",
            self.engine_family,
            self.model_id,
            self.verified_digest.as_deref().unwrap_or("digest:none"),
            self.locator.display(),
            self.device,
            self.compute_type
        )
    }
}

/// Resolves a durable [`AsrRuntimeSpec`] into a concrete Batch-scoped runtime.
///
/// Domain never sees Python paths or filesystem probes; only this port does.
#[async_trait]
pub trait AsrRuntimeResolver: Send + Sync {
    async fn resolve(&self, spec: &AsrRuntimeSpec) -> AppResult<Box<dyn AsrRuntime>>;
}

/// Build a complete fingerprint from runtime identity plus adapter/runtime versions
/// and normalized option material that affects ASR output.
pub fn asr_fingerprint(
    engine_id: &str,
    adapter_version: &str,
    runtime_version: &str,
    spec: &AsrRuntimeSpec,
    options_hash: &str,
) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}",
        engine_id,
        adapter_version,
        runtime_version,
        spec.identity_key(),
        options_hash,
        env!("CARGO_PKG_VERSION"),
    )
}

pub fn validate_spec(spec: &AsrRuntimeSpec) -> AppResult<()> {
    spec.validate().map_err(ApplicationError::Invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locator_round_trip_json() {
        let loc = ModelLocator::hugging_face("org/model", "main", Some("weights".into()));
        let json = serde_json::to_string(&loc).unwrap();
        let back: ModelLocator = serde_json::from_str(&json).unwrap();
        assert_eq!(back, loc);
    }

    #[test]
    fn v1_string_becomes_file_or_directory() {
        assert!(matches!(
            ModelLocator::from_v1_string("/models/a.bin"),
            ModelLocator::File { .. }
        ));
    }

    #[test]
    fn empty_file_locator_is_invalid() {
        assert!(ModelLocator::file("").validate().is_err());
    }

    #[test]
    fn identity_key_includes_digest() {
        let a = AsrRuntimeSpec {
            engine_family: "whisper-cpp".into(),
            model_id: "tiny".into(),
            verified_digest: Some("blake3:aaa".into()),
            locator: ModelLocator::file("/m.bin"),
            device: "cpu".into(),
            compute_type: "default".into(),
        };
        let mut b = a.clone();
        b.verified_digest = Some("blake3:bbb".into());
        assert_ne!(a.identity_key(), b.identity_key());
    }
}
