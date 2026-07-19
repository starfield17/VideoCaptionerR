//! Editable prompt loading and immutable stage snapshots.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptStage {
    Split,
    Correct,
    Translate,
}

impl PromptStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Split => "split",
            Self::Correct => "correct",
            Self::Translate => "translate",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptBundle {
    pub schema_version: u32,
    pub stage: PromptStage,
    pub files: BTreeMap<String, String>,
    pub content_hash: String,
}

impl PromptBundle {
    pub fn load(root: &Path, stage: PromptStage) -> VcResult<Self> {
        let directory = root.join(stage.as_str());
        let entries = fs::read_dir(&directory).map_err(|e| {
            VcError::new(
                ErrorCode::InvalidConfig,
                format!("read prompt directory {}: {e}", directory.display()),
            )
        })?;
        let mut files = BTreeMap::new();
        for entry in entries {
            let path = entry
                .map_err(|e| {
                    VcError::new(ErrorCode::InvalidConfig, format!("read prompt entry: {e}"))
                })?
                .path();
            if !path.is_file() {
                continue;
            }
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| {
                    VcError::new(ErrorCode::InvalidConfig, "prompt filename is not UTF-8")
                })?
                .to_owned();
            let raw = fs::read_to_string(&path).map_err(|e| {
                VcError::new(
                    ErrorCode::InvalidConfig,
                    format!("read prompt {}: {e}", path.display()),
                )
            })?;
            files.insert(name, normalize_prompt(&raw));
        }
        if files.is_empty() {
            return Err(VcError::new(
                ErrorCode::InvalidConfig,
                format!("prompt stage {} contains no files", directory.display()),
            ));
        }
        Ok(Self::from_files(stage, files))
    }

    pub fn from_files(stage: PromptStage, files: BTreeMap<String, String>) -> Self {
        let mut hasher = blake3::Hasher::new();
        for (name, content) in &files {
            hasher.update(name.as_bytes());
            hasher.update(&[0]);
            hasher.update(content.as_bytes());
            hasher.update(&[0]);
        }
        Self {
            schema_version: 1,
            stage,
            files,
            content_hash: hasher.finalize().to_hex().to_string(),
        }
    }

    pub fn system_prompt(&self) -> String {
        self.files
            .get("system.txt")
            .cloned()
            .unwrap_or_else(|| self.files.values().cloned().collect::<Vec<_>>().join("\n"))
    }

    /// Copy the normalized prompt files into an immutable Job stage snapshot.
    pub fn snapshot_to(&self, job_dir: &Path) -> VcResult<PathBuf> {
        let directory = job_dir
            .join("llm")
            .join("prompts")
            .join(self.stage.as_str());
        fs::create_dir_all(&directory).map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("create prompt snapshot directory: {e}"),
            )
        })?;
        for (name, content) in &self.files {
            let final_path = directory.join(name);
            let tmp_path = PathBuf::from(format!("{}.tmp", final_path.display()));
            fs::write(&tmp_path, content).map_err(|e| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("write prompt snapshot: {e}"),
                )
            })?;
            fs::rename(&tmp_path, &final_path).map_err(|e| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("commit prompt snapshot: {e}"),
                )
            })?;
        }
        let metadata = serde_json::json!({
            "schema_version": self.schema_version,
            "stage": self.stage,
            "content_hash": self.content_hash,
            "files": self.files.keys().collect::<Vec<_>>(),
        });
        let metadata_path = directory.join("bundle.json");
        let metadata_tmp = PathBuf::from(format!("{}.tmp", metadata_path.display()));
        fs::write(
            &metadata_tmp,
            serde_json::to_vec_pretty(&metadata).map_err(|e| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("serialize prompt metadata: {e}"),
                )
            })?,
        )
        .map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("write prompt metadata: {e}"),
            )
        })?;
        fs::rename(&metadata_tmp, &metadata_path).map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("commit prompt metadata: {e}"),
            )
        })?;
        Ok(directory)
    }
}

fn normalize_prompt(input: &str) -> String {
    let normalized = input.replace("\r\n", "\n").replace('\r', "\n");
    if normalized.ends_with('\n') {
        normalized
    } else {
        format!("{normalized}\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn loads_normalizes_hashes_and_snapshots() {
        let source = tempdir().unwrap();
        let stage_dir = source.path().join("translate");
        fs::create_dir_all(&stage_dir).unwrap();
        fs::write(stage_dir.join("system.txt"), "line one\r\nline two").unwrap();
        let bundle = PromptBundle::load(source.path(), PromptStage::Translate).unwrap();
        assert_eq!(bundle.system_prompt(), "line one\nline two\n");
        let job = tempdir().unwrap();
        let snapshot = bundle.snapshot_to(job.path()).unwrap();
        assert_eq!(
            fs::read_to_string(snapshot.join("system.txt")).unwrap(),
            "line one\nline two\n"
        );
        assert!(snapshot.join("bundle.json").is_file());
    }

    #[test]
    fn hashes_are_order_independent() {
        let mut a = BTreeMap::new();
        a.insert("b.txt".into(), "b\n".into());
        a.insert("a.txt".into(), "a\n".into());
        let mut b = BTreeMap::new();
        b.insert("a.txt".into(), "a\n".into());
        b.insert("b.txt".into(), "b\n".into());
        assert_eq!(
            PromptBundle::from_files(PromptStage::Split, a).content_hash,
            PromptBundle::from_files(PromptStage::Split, b).content_hash
        );
    }
}
