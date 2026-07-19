//! Model manifest and digest-validated downloader.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelManifest {
    pub schema_version: u32,
    pub models: Vec<ModelEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelEntry {
    pub model_id: String,
    pub engine_family: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_repo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
    #[serde(default)]
    pub platforms: Vec<String>,
}

impl ModelManifest {
    pub fn builtin() -> Self {
        Self {
            schema_version: 1,
            models: vec![
                ModelEntry {
                    model_id: "whisper-cpp/tiny-q5_1".into(),
                    engine_family: "whisper-cpp".into(),
                    source_repo: Some("ggerganov/whisper.cpp".into()),
                    source_file: Some("ggml-tiny-q5_1.bin".into()),
                    source_url: Some(
                        "https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-tiny-q5_1.bin"
                            .into(),
                    ),
                    expected_sha256: Some(
                        "818710568da3ca15689e31a743197b520007872ff9576237bda97bd1b469c3d7"
                            .into(),
                    ),
                    purpose: Some("smoke".into()),
                    platforms: vec![],
                },
                ModelEntry {
                    model_id: "fake/tiny".into(),
                    engine_family: "fake".into(),
                    source_repo: None,
                    source_file: None,
                    source_url: None,
                    expected_sha256: None,
                    purpose: Some("smoke".into()),
                    platforms: vec![],
                },
            ],
        }
    }

    pub fn find(&self, model_id: &str) -> Option<&ModelEntry> {
        self.models.iter().find(|m| m.model_id == model_id)
    }

    pub fn load_or_default(path: &Path) -> Self {
        if let Ok(text) = fs::read_to_string(path) {
            if let Ok(m) = toml::from_str(&text) {
                return m;
            }
        }
        Self::builtin()
    }
}

/// Download model to `dest_dir` using `.partial` then atomic rename after digest check.
/// Never called implicitly — user must select the model.
pub async fn download_model(entry: &ModelEntry, dest_dir: &Path) -> VcResult<PathBuf> {
    let Some(url) = &entry.source_url else {
        return Err(VcError::new(
            ErrorCode::ModelNotFound,
            format!("model {} has no source_url", entry.model_id),
        ));
    };
    let file_name = entry
        .source_file
        .clone()
        .unwrap_or_else(|| format!("{}.bin", entry.model_id.replace('/', "_")));
    fs::create_dir_all(dest_dir)
        .map_err(|e| VcError::new(ErrorCode::ModelNotFound, format!("create model dir: {e}")))?;
    let final_path = dest_dir.join(&file_name);
    if final_path.exists() {
        if let Some(expected) = &entry.expected_sha256 {
            let actual = sha256_file(&final_path)?;
            if actual.eq_ignore_ascii_case(expected) {
                return Ok(final_path);
            }
            // Corrupt existing file — remove and redownload.
            let _ = fs::remove_file(&final_path);
        } else {
            return Ok(final_path);
        }
    }

    let partial = dest_dir.join(format!("{file_name}.partial"));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(600))
        .build()
        .map_err(|e| VcError::new(ErrorCode::RuntimeUnavailable, format!("http client: {e}")))?;

    let resp = client.get(url).send().await.map_err(|e| {
        VcError::new(
            ErrorCode::RuntimeUnavailable,
            format!("download {url}: {e}"),
        )
    })?;
    if !resp.status().is_success() {
        return Err(VcError::new(
            ErrorCode::ModelNotFound,
            format!("download {url}: HTTP {}", resp.status()),
        ));
    }
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| VcError::new(ErrorCode::RuntimeUnavailable, format!("download body: {e}")))?;

    {
        let mut f = File::create(&partial)
            .map_err(|e| VcError::new(ErrorCode::ModelNotFound, format!("create partial: {e}")))?;
        f.write_all(&bytes)
            .map_err(|e| VcError::new(ErrorCode::ModelNotFound, format!("write partial: {e}")))?;
        f.sync_all().ok();
    }

    if let Some(expected) = &entry.expected_sha256 {
        let actual = sha256_file(&partial)?;
        if !actual.eq_ignore_ascii_case(expected) {
            let _ = fs::remove_file(&partial);
            return Err(VcError::new(
                ErrorCode::ModelDigestMismatch,
                format!("expected {expected}, got {actual}"),
            ));
        }
    }

    fs::rename(&partial, &final_path)
        .map_err(|e| VcError::new(ErrorCode::ModelNotFound, format!("rename model: {e}")))?;
    Ok(final_path)
}

pub fn sha256_file(path: &Path) -> VcResult<String> {
    let mut f = File::open(path)
        .map_err(|e| VcError::new(ErrorCode::ModelNotFound, format!("open for sha256: {e}")))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f
            .read(&mut buf)
            .map_err(|e| VcError::new(ErrorCode::ModelNotFound, format!("read for sha256: {e}")))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Verify an on-disk model against the manifest digest.
pub fn verify_model_file(path: &Path, expected_sha256: &str) -> VcResult<()> {
    let actual = sha256_file(path)?;
    if !actual.eq_ignore_ascii_case(expected_sha256) {
        return Err(VcError::new(
            ErrorCode::ModelDigestMismatch,
            format!("expected {expected_sha256}, got {actual}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_has_smoke_models() {
        let m = ModelManifest::builtin();
        assert!(m.find("whisper-cpp/tiny-q5_1").is_some());
        assert!(m.find("fake/tiny").is_some());
    }

    #[test]
    fn digest_mismatch_detected() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("m.bin");
        fs::write(&p, b"hello").unwrap();
        let err = verify_model_file(&p, "00").unwrap_err();
        assert_eq!(err.code, ErrorCode::ModelDigestMismatch);
    }
}
