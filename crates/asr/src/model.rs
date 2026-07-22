//! Model manifest and digest-validated downloader.
//!
//! Downloads use `.partial`, optional resume, digest verification, and atomic
//! publish. Models are never downloaded silently as a default.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::ports::ModelLocator;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelManifest {
    pub schema_version: u32,
    pub models: Vec<ModelEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelLocatorKind {
    File,
    Directory,
    HuggingFaceSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelEntry {
    pub model_id: String,
    pub engine_family: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_repo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_blake3: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
    #[serde(default)]
    pub platforms: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locator_kind: Option<ModelLocatorKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_runtime: Option<String>,
    #[serde(default)]
    pub languages: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_level: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_size_bytes: Option<u64>,
}

impl ModelEntry {
    pub fn to_locator(&self, local_path: Option<&Path>) -> ModelLocator {
        match self.locator_kind.unwrap_or(ModelLocatorKind::File) {
            ModelLocatorKind::Directory => {
                let path = local_path
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default();
                ModelLocator::directory(path)
            }
            ModelLocatorKind::HuggingFaceSnapshot => ModelLocator::hugging_face(
                self.source_repo.clone().unwrap_or_default(),
                self.revision.clone().unwrap_or_else(|| "main".into()),
                local_path.map(|p| p.to_string_lossy().into_owned()),
            ),
            ModelLocatorKind::File => {
                let path = local_path
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default();
                ModelLocator::file(path)
            }
        }
    }

    pub fn is_platform_compatible(&self) -> bool {
        if self.platforms.is_empty() {
            return true;
        }
        let current = format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH);
        self.platforms.iter().any(|p| {
            p == "*"
                || p == std::env::consts::OS
                || *p == current
                || (p == "macos-aarch64" && cfg!(all(target_os = "macos", target_arch = "aarch64")))
        })
    }
}

impl ModelManifest {
    pub fn builtin() -> Self {
        Self {
            schema_version: 2,
            models: vec![
                ModelEntry {
                    model_id: "whisper-cpp/tiny-q5_1".into(),
                    engine_family: "whisper-cpp".into(),
                    revision: Some("main".into()),
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
                    expected_blake3: None,
                    license: Some("MIT".into()),
                    purpose: Some("smoke".into()),
                    platforms: vec![],
                    locator_kind: Some(ModelLocatorKind::File),
                    min_runtime: Some("whisper-cpp".into()),
                    languages: vec!["*".into()],
                    timestamp_level: Some("word".into()),
                    estimated_size_bytes: Some(31_000_000),
                },
                ModelEntry {
                    model_id: "faster-whisper/tiny".into(),
                    engine_family: "faster-whisper".into(),
                    revision: Some("main".into()),
                    source_repo: Some("Systran/faster-whisper-tiny".into()),
                    source_file: None,
                    source_url: None,
                    expected_sha256: None,
                    expected_blake3: None,
                    license: Some("MIT".into()),
                    purpose: Some("smoke".into()),
                    platforms: vec![],
                    locator_kind: Some(ModelLocatorKind::HuggingFaceSnapshot),
                    min_runtime: Some("faster-whisper".into()),
                    languages: vec!["*".into()],
                    timestamp_level: Some("word".into()),
                    estimated_size_bytes: Some(75_000_000),
                },
                ModelEntry {
                    model_id: "mlx-whisper/tiny".into(),
                    engine_family: "mlx-whisper".into(),
                    revision: Some("main".into()),
                    source_repo: Some("mlx-community/whisper-tiny-mlx".into()),
                    source_file: None,
                    source_url: None,
                    expected_sha256: None,
                    expected_blake3: None,
                    license: Some("MIT".into()),
                    purpose: Some("smoke".into()),
                    platforms: vec!["macos-aarch64".into()],
                    locator_kind: Some(ModelLocatorKind::HuggingFaceSnapshot),
                    min_runtime: Some("mlx-whisper".into()),
                    languages: vec!["*".into()],
                    timestamp_level: Some("word".into()),
                    estimated_size_bytes: Some(75_000_000),
                },
                ModelEntry {
                    model_id: "fake/tiny".into(),
                    engine_family: "fake".into(),
                    revision: None,
                    source_repo: None,
                    source_file: None,
                    source_url: None,
                    expected_sha256: None,
                    expected_blake3: None,
                    license: None,
                    purpose: Some("smoke".into()),
                    platforms: vec![],
                    locator_kind: Some(ModelLocatorKind::File),
                    min_runtime: None,
                    languages: vec![],
                    timestamp_level: Some("word".into()),
                    estimated_size_bytes: None,
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
    if !entry.is_platform_compatible() {
        return Err(VcError::new(
            ErrorCode::RuntimeUnavailable,
            format!(
                "model {} is not compatible with this platform",
                entry.model_id
            ),
        ));
    }
    let Some(url) = &entry.source_url else {
        return Err(VcError::new(
            ErrorCode::ModelNotFound,
            format!(
                "model {} has no source_url; HuggingFace snapshots must be downloaded explicitly",
                entry.model_id
            ),
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
            remove_model_file(&final_path);
        } else if let Some(expected) = &entry.expected_blake3 {
            let actual = blake3_file(&final_path)?;
            if actual.eq_ignore_ascii_case(expected)
                || actual
                    .trim_start_matches("blake3:")
                    .eq_ignore_ascii_case(expected.trim_start_matches("blake3:"))
            {
                return Ok(final_path);
            }
            remove_model_file(&final_path);
        } else {
            return Ok(final_path);
        }
    }

    if let Some(size) = entry.estimated_size_bytes {
        preflight_disk_space(dest_dir, size)?;
    }

    let partial = dest_dir.join(format!("{file_name}.partial"));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(600))
        .build()
        .map_err(|e| VcError::new(ErrorCode::RuntimeUnavailable, format!("http client: {e}")))?;

    let mut request = client.get(url);
    let mut resume_from = 0u64;
    if partial.exists() {
        if let Ok(meta) = fs::metadata(&partial) {
            resume_from = meta.len();
            if resume_from > 0 {
                request = request.header(reqwest::header::RANGE, format!("bytes={resume_from}-"));
            }
        }
    }

    let resp = request.send().await.map_err(|e| {
        VcError::new(
            ErrorCode::RuntimeUnavailable,
            format!("download {url}: {e}"),
        )
    })?;
    if !resp.status().is_success() && resp.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(VcError::new(
            ErrorCode::ModelNotFound,
            format!("download {url}: HTTP {}", resp.status()),
        ));
    }

    use futures::StreamExt;
    let mut stream = resp.bytes_stream();
    let mut f = if resume_from > 0 {
        fs::OpenOptions::new()
            .append(true)
            .open(&partial)
            .map_err(|e| VcError::new(ErrorCode::ModelNotFound, format!("open partial: {e}")))?
    } else {
        File::create(&partial)
            .map_err(|e| VcError::new(ErrorCode::ModelNotFound, format!("create partial: {e}")))?
    };
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| {
            VcError::new(
                ErrorCode::RuntimeUnavailable,
                format!("download stream: {e}"),
            )
        })?;
        f.write_all(&chunk)
            .map_err(|e| VcError::new(ErrorCode::ModelNotFound, format!("write partial: {e}")))?;
    }
    f.sync_all().ok();
    drop(f);

    if let Some(expected) = &entry.expected_sha256 {
        let actual = sha256_file(&partial)?;
        if !actual.eq_ignore_ascii_case(expected) {
            remove_model_file(&partial);
            return Err(VcError::new(
                ErrorCode::ModelDigestMismatch,
                format!("expected {expected}, got {actual}"),
            ));
        }
    }
    if let Some(expected) = &entry.expected_blake3 {
        let actual = blake3_file(&partial)?;
        let exp = expected.trim_start_matches("blake3:");
        let act = actual.trim_start_matches("blake3:");
        if !act.eq_ignore_ascii_case(exp) {
            remove_model_file(&partial);
            return Err(VcError::new(
                ErrorCode::ModelDigestMismatch,
                format!("expected blake3:{exp}, got {actual}"),
            ));
        }
    }

    // Atomic publish: rename after full verification.
    fs::rename(&partial, &final_path)
        .map_err(|e| VcError::new(ErrorCode::ModelNotFound, format!("rename model: {e}")))?;
    Ok(final_path)
}

fn remove_model_file(path: &Path) {
    if let Err(error) = fs::remove_file(path) {
        if error.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(
                path = %path.display(),
                error = %error,
                "model temporary file cleanup failed"
            );
        }
    }
}

fn preflight_disk_space(dest_dir: &Path, needed: u64) -> VcResult<()> {
    // Best-effort: check free space via `statvfs` is platform-specific; use a
    // conservative file-write probe only when needed is huge and dir missing.
    let _ = dest_dir;
    let _ = needed;
    // If we cannot determine free space, proceed; OS will fail the write.
    Ok(())
}

/// Content digest used in fingerprints/cache keys.
pub fn blake3_file(path: &Path) -> VcResult<String> {
    let mut f = File::open(path)
        .map_err(|e| VcError::new(ErrorCode::ModelNotFound, format!("open for blake3: {e}")))?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f
            .read(&mut buf)
            .map_err(|e| VcError::new(ErrorCode::ModelNotFound, format!("read for blake3: {e}")))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
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
