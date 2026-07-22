//! Shared artifact cache with atomic writes and cooperative GC leases.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::application_error::{AppResult, ApplicationError};
use videocaptionerr_core::ports::{CacheGcResult, CacheRepository};

#[derive(Debug, Clone)]
pub struct CacheStore {
    root: Arc<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheGcReport {
    pub before_bytes: u64,
    pub after_bytes: u64,
    pub deleted_entries: u32,
    pub skipped_leased: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct CacheMetadata {
    schema_version: u32,
    key: String,
    content_hash: String,
    size: u64,
}

pub struct CacheLease {
    lock: File,
    #[allow(dead_code)]
    path: PathBuf,
}

impl Drop for CacheLease {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.lock);
    }
}

impl CacheStore {
    pub fn new(root: impl Into<PathBuf>) -> VcResult<Self> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|error| {
            VcError::new(
                ErrorCode::InvalidConfig,
                format!("create cache directory {}: {error}", root.display()),
            )
        })?;
        Ok(Self {
            root: Arc::new(root),
        })
    }

    pub fn root(&self) -> &Path {
        self.root.as_path()
    }

    pub fn put_bytes(&self, key: &str, bytes: &[u8]) -> VcResult<PathBuf> {
        validate_key(key)?;
        let _lease = self.acquire(key)?;
        let data_path = self.data_path(key);
        let temp_path = self.root.join(format!("{key}.partial"));
        let mut file = File::create(&temp_path).map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("create cache partial: {error}"),
            )
        })?;
        file.write_all(bytes).map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("write cache partial: {error}"),
            )
        })?;
        file.sync_all().map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("flush cache partial: {error}"),
            )
        })?;
        drop(file);
        fs::rename(&temp_path, &data_path).map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("commit cache entry: {error}"),
            )
        })?;

        let metadata = CacheMetadata {
            schema_version: 1,
            key: key.into(),
            content_hash: blake3::hash(bytes).to_hex().to_string(),
            size: bytes.len() as u64,
        };
        let metadata_bytes = serde_json::to_vec(&metadata).map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode cache metadata: {error}"),
            )
        })?;
        let metadata_path = self.metadata_path(key);
        let metadata_temp = self.root.join(format!("{key}.meta.partial"));
        fs::write(&metadata_temp, metadata_bytes).map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("write cache metadata: {error}"),
            )
        })?;
        fs::rename(metadata_temp, metadata_path).map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("commit cache metadata: {error}"),
            )
        })?;
        Ok(data_path)
    }

    pub fn read_bytes(&self, key: &str) -> VcResult<Vec<u8>> {
        validate_key(key)?;
        let _lease = self.acquire(key)?;
        let path = self.data_path(key);
        let metadata: CacheMetadata =
            serde_json::from_slice(&fs::read(self.metadata_path(key)).map_err(|error| {
                VcError::new(
                    ErrorCode::CacheCorrupt,
                    format!("read cache metadata: {error}"),
                )
            })?)
            .map_err(|error| {
                VcError::new(
                    ErrorCode::CacheCorrupt,
                    format!("decode cache metadata: {error}"),
                )
            })?;
        if metadata.key != key {
            return Err(VcError::new(
                ErrorCode::CacheCorrupt,
                "cache key metadata mismatch",
            ));
        }
        let bytes = fs::read(&path).map_err(|error| {
            VcError::new(
                ErrorCode::CacheCorrupt,
                format!("read cache entry: {error}"),
            )
        })?;
        let actual = blake3::hash(&bytes).to_hex().to_string();
        if actual != metadata.content_hash || bytes.len() as u64 != metadata.size {
            return Err(VcError::new(
                ErrorCode::CacheCorrupt,
                format!("cache entry hash mismatch: {}", path.display()),
            ));
        }
        Ok(bytes)
    }

    pub fn read_if_present(&self, key: &str) -> VcResult<Option<Vec<u8>>> {
        validate_key(key)?;
        if !self.data_path(key).is_file() || !self.metadata_path(key).is_file() {
            return Ok(None);
        }
        self.read_bytes(key).map(Some)
    }

    pub fn acquire(&self, key: &str) -> VcResult<CacheLease> {
        validate_key(key)?;
        let path = self.lock_path(key);
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| {
                VcError::new(
                    ErrorCode::CacheCorrupt,
                    format!("open cache lease: {error}"),
                )
            })?;
        lock.try_lock_exclusive().map_err(|error| {
            VcError::new(
                ErrorCode::CacheCorrupt,
                format!("cache entry is leased: {key}: {error}"),
            )
        })?;
        Ok(CacheLease { lock, path })
    }

    pub fn gc(&self, max_bytes: u64) -> VcResult<CacheGcReport> {
        let mut entries = Vec::new();
        for item in fs::read_dir(self.root.as_path()).map_err(|error| {
            VcError::new(
                ErrorCode::CacheCorrupt,
                format!("read cache directory: {error}"),
            )
        })? {
            let path = item
                .map_err(|error| {
                    VcError::new(
                        ErrorCode::CacheCorrupt,
                        format!("read cache entry: {error}"),
                    )
                })?
                .path();
            if path.extension().and_then(|value| value.to_str()) != Some("cache") {
                continue;
            }
            let metadata = fs::metadata(&path).map_err(|error| {
                VcError::new(
                    ErrorCode::CacheCorrupt,
                    format!("stat cache entry: {error}"),
                )
            })?;
            entries.push((path, metadata.len()));
        }
        entries.sort_by_key(|(path, _)| fs::metadata(path).and_then(|meta| meta.modified()).ok());
        let before_bytes = entries.iter().map(|(_, size)| *size).sum::<u64>();
        let mut after_bytes = before_bytes;
        let mut deleted_entries = 0;
        let mut skipped_leased = 0;
        for (path, size) in entries {
            if after_bytes <= max_bytes {
                break;
            }
            let key = path
                .file_stem()
                .and_then(|value| value.to_str())
                .ok_or_else(|| {
                    VcError::new(ErrorCode::CacheCorrupt, "cache filename is not UTF-8")
                })?;
            let Ok(_lease) = self.acquire(key) else {
                skipped_leased += 1;
                continue;
            };
            fs::remove_file(&path).map_err(|error| {
                VcError::new(
                    ErrorCode::CacheCorrupt,
                    format!("delete cache entry: {error}"),
                )
            })?;
            let metadata_path = self.metadata_path(key);
            if let Err(error) = fs::remove_file(&metadata_path) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        path = %metadata_path.display(),
                        error = %error,
                        "cache metadata cleanup failed"
                    );
                }
            }
            after_bytes = after_bytes.saturating_sub(size);
            deleted_entries += 1;
        }
        Ok(CacheGcReport {
            before_bytes,
            after_bytes,
            deleted_entries,
            skipped_leased,
        })
    }

    fn data_path(&self, key: &str) -> PathBuf {
        self.root.join(format!("{key}.cache"))
    }

    fn metadata_path(&self, key: &str) -> PathBuf {
        self.root.join(format!("{key}.meta"))
    }

    fn lock_path(&self, key: &str) -> PathBuf {
        self.root.join(format!("{key}.lock"))
    }
}

#[async_trait::async_trait]
impl CacheRepository for CacheStore {
    async fn gc(&self, max_bytes: u64) -> AppResult<CacheGcResult> {
        let cache = self.clone();
        let report = tokio::task::spawn_blocking(move || cache.gc(max_bytes))
            .await
            .map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::CacheCorrupt,
                    format!("cache GC task failed: {error}"),
                ))
            })?
            .map_err(ApplicationError::Adapter)?;
        Ok(CacheGcResult {
            before_bytes: report.before_bytes,
            after_bytes: report.after_bytes,
            deleted_entries: report.deleted_entries,
            skipped_leased: report.skipped_leased,
        })
    }

    async fn read(&self, key: &str) -> AppResult<Option<Vec<u8>>> {
        let cache = self.clone();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || cache.read_if_present(&key))
            .await
            .map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::CacheCorrupt,
                    format!("cache read task failed: {error}"),
                ))
            })?
            .map_err(ApplicationError::Adapter)
    }

    async fn write(&self, key: &str, bytes: &[u8]) -> AppResult<()> {
        let cache = self.clone();
        let key = key.to_owned();
        let bytes = bytes.to_vec();
        tokio::task::spawn_blocking(move || cache.put_bytes(&key, &bytes).map(|_| ()))
            .await
            .map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("cache write task failed: {error}"),
                ))
            })?
            .map_err(ApplicationError::Adapter)
    }
}

fn validate_key(key: &str) -> VcResult<()> {
    if key.is_empty()
        || key.len() > 128
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(VcError::new(
            ErrorCode::InvalidArgument,
            "invalid cache key",
        ));
    }
    Ok(())
}

pub fn cache_key(parts: &[&str]) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update(&[0]);
    }
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn cache_writes_atomically_and_detects_corruption() {
        let dir = tempdir().unwrap();
        let cache = CacheStore::new(dir.path()).unwrap();
        let key = cache_key(&["stage", "input"]);
        cache.put_bytes(&key, b"payload").unwrap();
        assert_eq!(cache.read_bytes(&key).unwrap(), b"payload");
        fs::write(cache.data_path(&key), b"corrupt").unwrap();
        assert_eq!(
            cache.read_bytes(&key).unwrap_err().code,
            ErrorCode::CacheCorrupt
        );
    }

    #[test]
    fn gc_skips_a_leased_entry() {
        let dir = tempdir().unwrap();
        let cache = CacheStore::new(dir.path()).unwrap();
        let first = cache_key(&["first"]);
        let second = cache_key(&["second"]);
        cache.put_bytes(&first, b"12345").unwrap();
        cache.put_bytes(&second, b"67890").unwrap();
        let lease = cache.acquire(&first).unwrap();
        let report = cache.gc(0).unwrap();
        assert_eq!(report.skipped_leased, 1);
        assert!(cache.data_path(&first).is_file());
        drop(lease);
        assert!(cache.gc(0).unwrap().deleted_entries >= 1);
    }
}
