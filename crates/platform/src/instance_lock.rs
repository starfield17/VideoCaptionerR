//! Exclusive processing instance lock (GUI vs CLI).

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use fs2::FileExt;
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};

/// Who holds the exclusive processing lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockOwner {
    Cli,
    Gui,
}

impl LockOwner {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Gui => "gui",
        }
    }
}

/// RAII exclusive processing lock. Dropping releases the lock.
#[derive(Debug)]
pub struct InstanceLock {
    path: PathBuf,
    file: File,
    owner: LockOwner,
}

impl InstanceLock {
    /// Try to acquire the exclusive processing lock.
    /// Returns `INSTANCE_BUSY` if another process holds it.
    pub fn try_acquire(lock_path: &Path, owner: LockOwner) -> VcResult<Self> {
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                VcError::new(
                    ErrorCode::InvalidConfig,
                    format!("create lock dir {}: {e}", parent.display()),
                )
            })?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(lock_path)
            .map_err(|e| {
                VcError::new(
                    ErrorCode::InstanceBusy,
                    format!("open lock {}: {e}", lock_path.display()),
                )
            })?;

        file.try_lock_exclusive().map_err(|e| {
            VcError::new(
                ErrorCode::InstanceBusy,
                format!(
                    "processing instance lock held (path {}): {e}",
                    lock_path.display()
                ),
            )
        })?;

        // Lock before touching the metadata. A second process must not be
        // able to truncate the live owner's diagnostic payload while its lock
        // attempt is being rejected.
        let payload = format!("owner={}\npid={}\n", owner.as_str(), std::process::id());
        file.set_len(0).map_err(|e| {
            let _ = FileExt::unlock(&file);
            VcError::new(
                ErrorCode::InstanceBusy,
                format!("truncate lock metadata {}: {e}", lock_path.display()),
            )
        })?;
        file.write_all(payload.as_bytes())
            .map_err(|e| VcError::new(ErrorCode::InstanceBusy, format!("write lock file: {e}")))?;
        file.sync_all().ok();

        Ok(Self {
            path: lock_path.to_path_buf(),
            file,
            owner,
        })
    }

    pub fn owner(&self) -> LockOwner {
        self.owner
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for InstanceLock {
    fn drop(&mut self) {
        // Use the fs2 trait method explicitly so we do not hit std::fs::File::unlock (1.89+).
        let _ = FileExt::unlock(&self.file);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn exclusive_lock_blocks_second_acquirer() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("processing.lock");

        let first = InstanceLock::try_acquire(&path, LockOwner::Cli).unwrap();
        assert_eq!(first.owner(), LockOwner::Cli);

        let second = InstanceLock::try_acquire(&path, LockOwner::Gui);
        assert!(second.is_err());
        let err = second.unwrap_err();
        assert_eq!(err.code, ErrorCode::InstanceBusy);

        drop(first);

        let third = InstanceLock::try_acquire(&path, LockOwner::Gui).unwrap();
        assert_eq!(third.owner(), LockOwner::Gui);
    }

    #[test]
    fn lock_file_records_owner() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("processing.lock");
        let lock = InstanceLock::try_acquire(&path, LockOwner::Cli).unwrap();
        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains("owner=cli"));
        assert!(content.contains("pid="));
        drop(lock);
    }
}
