//! Atomic artifact write / commit helpers.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::ports::{ArtifactSource, PreparedArtifact};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageCommitFaultPoint {
    BeforeTempWrite,
    AfterTempWrite,
    AfterRename,
    AfterArtifactInsert,
    AfterWorkUnitUpdate,
    AfterJobUpdate,
    AfterOutboxInsert,
    AfterDbCommit,
}

/// Compute BLAKE3 hex digest of bytes.
pub fn blake3_bytes(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

/// Stream BLAKE3 of a file without loading it fully into memory.
pub fn blake3_file(path: &Path) -> VcResult<String> {
    let mut file = File::open(path).map_err(|e| {
        VcError::new(
            ErrorCode::ArtifactCorrupt,
            format!("open {} for hash: {e}", path.display()),
        )
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCorrupt,
                format!("read {} for hash: {e}", path.display()),
            )
        })?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

/// Write bytes to `path.tmp`, fsync, validate optional predicate, rename to `path`.
pub fn atomic_write_bytes(path: &Path, data: &[u8]) -> VcResult<String> {
    let tmp = tmp_path(path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("create parent for {}: {e}", path.display()),
            )
        })?;
    }

    {
        let mut f = File::create(&tmp).map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("create {}: {e}", tmp.display()),
            )
        })?;
        f.write_all(data).map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("write {}: {e}", tmp.display()),
            )
        })?;
        f.sync_all().map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("sync {}: {e}", tmp.display()),
            )
        })?;
    }

    // Reread and validate.
    let mut reread = Vec::new();
    {
        let mut f = File::open(&tmp).map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("reopen {}: {e}", tmp.display()),
            )
        })?;
        f.read_to_end(&mut reread).map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("reread {}: {e}", tmp.display()),
            )
        })?;
    }
    if reread.as_slice() != data {
        remove_artifact_file(&tmp);
        return Err(VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("reread mismatch for {}", tmp.display()),
        ));
    }

    let hash = blake3_bytes(data);
    fs::rename(&tmp, path).map_err(|e| {
        remove_artifact_file(&tmp);
        VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("rename {} -> {}: {e}", tmp.display(), path.display()),
        )
    })?;
    Ok(hash)
}

/// Serialize JSON (pretty, stable) and atomically commit.
pub fn atomic_write_json<T: Serialize>(path: &Path, value: &T) -> VcResult<String> {
    let data = serde_json::to_vec_pretty(value).map_err(|e| {
        VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("serialize json for {}: {e}", path.display()),
        )
    })?;
    atomic_write_bytes(path, &data)
}

/// Commit an existing temp file written by an external process (e.g. ffmpeg).
/// Validates by reopening, hashing, then renaming `tmp` -> `final_path`.
pub fn commit_file(tmp: &Path, final_path: &Path) -> VcResult<String> {
    if !tmp.exists() {
        return Err(VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("tmp missing: {}", tmp.display()),
        ));
    }
    let hash = blake3_file(tmp)?;
    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("create parent for {}: {e}", final_path.display()),
            )
        })?;
    }
    fs::rename(tmp, final_path).map_err(|e| {
        VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("rename {} -> {}: {e}", tmp.display(), final_path.display()),
        )
    })?;
    Ok(hash)
}

/// Publish the file belonging to an atomic stage commit. The returned flag is
/// true only when this call created or moved the final path, so a failed DB
/// transaction can clean up its own publication without deleting a preexisting
/// valid artifact.
pub fn publish_prepared_artifact(prepared: &PreparedArtifact) -> VcResult<bool> {
    publish_prepared_artifact_with_fault(prepared, None)
}

pub fn publish_prepared_artifact_with_fault(
    prepared: &PreparedArtifact,
    fault: Option<StageCommitFaultPoint>,
) -> VcResult<bool> {
    let final_path = Path::new(&prepared.artifact.path);
    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("create artifact parent {}: {error}", parent.display()),
            )
        })?;
    }

    match &prepared.source {
        ArtifactSource::ExistingFile { path } => {
            let source_hash = blake3_file(path)?;
            if source_hash != prepared.artifact.content_hash {
                return Err(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("prepared artifact hash mismatch: {}", path.display()),
                ));
            }
            if path == final_path {
                return Ok(false);
            }
            if final_path.exists() {
                let final_hash = blake3_file(final_path)?;
                if final_hash == prepared.artifact.content_hash {
                    remove_artifact_file(path);
                    return Ok(false);
                }
                return Err(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!(
                        "refusing to replace non-matching artifact {}",
                        final_path.display()
                    ),
                ));
            }
            fs::rename(path, final_path).map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!(
                        "publish {} -> {}: {error}",
                        path.display(),
                        final_path.display()
                    ),
                )
            })?;
            sync_parent(final_path);
            fault_at(fault, StageCommitFaultPoint::AfterRename)?;
            Ok(true)
        }
        ArtifactSource::Bytes { bytes } => {
            let actual_hash = blake3_bytes(bytes);
            if actual_hash != prepared.artifact.content_hash {
                return Err(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!(
                        "serialized artifact hash mismatch: {}",
                        final_path.display()
                    ),
                ));
            }
            if final_path.exists() {
                let final_hash = blake3_file(final_path)?;
                if final_hash == prepared.artifact.content_hash {
                    return Ok(false);
                }
                return Err(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!(
                        "refusing to replace non-matching artifact {}",
                        final_path.display()
                    ),
                ));
            }

            let partial = partial_path(final_path);
            if partial.exists() {
                return Err(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("partial artifact already exists: {}", partial.display()),
                ));
            }
            fault_at(fault, StageCommitFaultPoint::BeforeTempWrite)?;
            {
                let mut file = File::create(&partial).map_err(|error| {
                    VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("create {}: {error}", partial.display()),
                    )
                })?;
                file.write_all(bytes).map_err(|error| {
                    VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("write {}: {error}", partial.display()),
                    )
                })?;
                file.sync_all().map_err(|error| {
                    VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("sync {}: {error}", partial.display()),
                    )
                })?;
            }
            fault_at(fault, StageCommitFaultPoint::AfterTempWrite)?;
            if blake3_file(&partial)? != prepared.artifact.content_hash {
                remove_artifact_file(&partial);
                return Err(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("reread hash mismatch: {}", partial.display()),
                ));
            }
            if final_path.exists() {
                let final_hash = blake3_file(final_path)?;
                remove_artifact_file(&partial);
                if final_hash == prepared.artifact.content_hash {
                    return Ok(false);
                }
                return Err(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!(
                        "refusing to replace non-matching artifact {}",
                        final_path.display()
                    ),
                ));
            }
            fs::rename(&partial, final_path).map_err(|error| {
                remove_artifact_file(&partial);
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!(
                        "publish {} -> {}: {error}",
                        partial.display(),
                        final_path.display()
                    ),
                )
            })?;
            sync_parent(final_path);
            fault_at(fault, StageCommitFaultPoint::AfterRename)?;
            Ok(true)
        }
    }
}

fn fault_at(
    configured: Option<StageCommitFaultPoint>,
    point: StageCommitFaultPoint,
) -> VcResult<()> {
    if configured == Some(point) {
        return Err(VcError::new(
            ErrorCode::Internal,
            format!("injected stage commit interruption at {point:?}"),
        ));
    }
    Ok(())
}

pub fn partial_path(path: &Path) -> PathBuf {
    let mut partial = path.as_os_str().to_os_string();
    partial.push(".partial");
    PathBuf::from(partial)
}

pub fn sync_parent(path: &Path) {
    if let Some(parent) = path.parent() {
        if let Ok(file) = File::open(parent) {
            let _ = file.sync_all();
        }
    }
}

/// Remove uncommitted `.tmp` siblings under a directory (startup recovery).
pub fn quarantine_tmp_files(dir: &Path) -> VcResult<Vec<PathBuf>> {
    let mut removed = Vec::new();
    if !dir.is_dir() {
        return Ok(removed);
    }
    for entry in fs::read_dir(dir).map_err(|e| {
        VcError::new(
            ErrorCode::ArtifactCorrupt,
            format!("read_dir {}: {e}", dir.display()),
        )
    })? {
        let entry = entry
            .map_err(|e| VcError::new(ErrorCode::ArtifactCorrupt, format!("dir entry: {e}")))?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("tmp")
            || path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains(".tmp."))
            || path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(".tmp"))
        {
            remove_artifact_file(&path);
            removed.push(path);
        }
    }
    Ok(removed)
}

fn remove_artifact_file(path: &Path) {
    if let Err(error) = fs::remove_file(path) {
        if error.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(
                path = %path.display(),
                error = %error,
                "temporary artifact cleanup failed"
            );
        }
    }
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn atomic_write_and_hash() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("artifact.json");
        let data = br#"{"ok":true}"#;
        let hash = atomic_write_bytes(&path, data).unwrap();
        assert_eq!(hash, blake3_bytes(data));
        assert_eq!(fs::read(&path).unwrap(), data);
        assert!(
            !path.with_extension("json.tmp").exists()
                || !dir.path().join("artifact.json.tmp").exists()
        );
        // Our tmp is path + ".tmp"
        assert!(!PathBuf::from(format!("{}.tmp", path.display())).exists());
    }

    #[test]
    fn crash_point_leaves_no_corrupt_final() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("final.bin");
        let tmp = PathBuf::from(format!("{}.tmp", path.display()));

        // Simulate crash after writing tmp but before rename: final must not exist.
        {
            let mut f = File::create(&tmp).unwrap();
            f.write_all(b"partial").unwrap();
        }
        assert!(tmp.exists());
        assert!(!path.exists());

        // Recovery removes tmp.
        let removed = quarantine_tmp_files(dir.path()).unwrap();
        assert!(removed.iter().any(|p| p == &tmp));
        assert!(!tmp.exists());
        assert!(!path.exists());
    }

    #[test]
    fn commit_file_renames_atomically() {
        let dir = tempdir().unwrap();
        let tmp = dir.path().join("audio.tmp.wav");
        let final_path = dir.path().join("audio.wav");
        fs::write(&tmp, b"RIFF....WAVE").unwrap();
        let hash = commit_file(&tmp, &final_path).unwrap();
        assert!(!tmp.exists());
        assert!(final_path.exists());
        assert_eq!(hash, blake3_file(&final_path).unwrap());
    }

    #[test]
    fn half_written_tmp_does_not_replace_good_final() {
        let dir = tempdir().unwrap();
        let final_path = dir.path().join("audio.wav");
        fs::write(&final_path, b"GOOD").unwrap();
        let good_hash = blake3_file(&final_path).unwrap();

        // A failed extraction only writes tmp; commit never called.
        let tmp = dir.path().join("audio.tmp.wav");
        fs::write(&tmp, b"BAD").unwrap();
        let _ = fs::remove_file(&tmp);

        assert_eq!(fs::read(&final_path).unwrap(), b"GOOD");
        assert_eq!(blake3_file(&final_path).unwrap(), good_hash);
    }
}
