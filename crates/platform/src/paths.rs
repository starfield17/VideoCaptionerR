//! Application directory layout.

use std::fs;
use std::path::{Path, PathBuf};

use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};

const ENV_HOME: &str = "VIDEOCAPTIONERR_HOME";

/// Resolved application paths under the platform app-data directory
/// or `VIDEOCAPTIONERR_HOME`.
#[derive(Debug, Clone)]
pub struct AppPaths {
    pub home: PathBuf,
    pub config_dir: PathBuf,
    pub config_file: PathBuf,
    pub state_dir: PathBuf,
    pub db_path: PathBuf,
    pub jobs_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub models_dir: PathBuf,
    pub envs_dir: PathBuf,
    pub logs_dir: PathBuf,
    pub locks_dir: PathBuf,
}

impl AppPaths {
    /// Resolve from environment / platform defaults without creating directories.
    pub fn resolve() -> VcResult<Self> {
        let home = if let Ok(override_home) = std::env::var(ENV_HOME) {
            PathBuf::from(override_home)
        } else {
            let base = dirs_next_data_dir().ok_or_else(|| {
                VcError::new(
                    ErrorCode::InvalidConfig,
                    "cannot determine application data directory",
                )
            })?;
            base.join("videocaptionerr")
        };
        Ok(Self::from_home(home))
    }

    pub fn from_home(home: impl Into<PathBuf>) -> Self {
        let home = home.into();
        let config_dir = home.join("config");
        let state_dir = home.join("state");
        Self {
            config_file: config_dir.join("config.toml"),
            config_dir,
            db_path: state_dir.join("videocaptionerr.db"),
            state_dir,
            jobs_dir: home.join("jobs"),
            cache_dir: home.join("cache"),
            models_dir: home.join("models"),
            envs_dir: home.join("envs"),
            logs_dir: home.join("logs"),
            locks_dir: home.join("locks"),
            home,
        }
    }

    /// Create the recommended directory layout.
    pub fn ensure_layout(&self) -> VcResult<()> {
        for dir in [
            &self.home,
            &self.config_dir,
            &self.state_dir,
            &self.jobs_dir,
            &self.cache_dir,
            &self.models_dir,
            &self.envs_dir,
            &self.logs_dir,
            &self.locks_dir,
        ] {
            fs::create_dir_all(dir).map_err(|e| {
                VcError::new(
                    ErrorCode::InvalidConfig,
                    format!("failed to create {}: {e}", dir.display()),
                )
            })?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&self.config_dir, fs::Permissions::from_mode(0o700));
            let _ = fs::set_permissions(&self.home, fs::Permissions::from_mode(0o700));
        }
        Ok(())
    }

    pub fn job_dir(&self, job_id: &str, sanitized_stem: &str) -> PathBuf {
        self.jobs_dir.join(format!("{job_id}_{sanitized_stem}"))
    }

    pub fn instance_lock_path(&self) -> PathBuf {
        self.locks_dir.join("processing.lock")
    }
}

/// Minimal dirs helper to avoid pulling `dirs` into store if workspace dep missing path.
fn dirs_next_data_dir() -> Option<PathBuf> {
    // Prefer XDG on Unix, then known fallbacks.
    if let Ok(xdg) = std::env::var("XDG_DATA_HOME") {
        if !xdg.is_empty() {
            return Some(PathBuf::from(xdg));
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        return Some(PathBuf::from(home).join(".local/share"));
    }
    #[cfg(windows)]
    {
        if let Ok(ad) = std::env::var("LOCALAPPDATA") {
            return Some(PathBuf::from(ad));
        }
    }
    None
}

/// Sanitize a media stem for use in Job directory names.
pub fn sanitize_stem(stem: &str) -> String {
    let mut out = String::with_capacity(stem.len());
    for ch in stem.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            // Whitespace and other non-safe characters become underscores.
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "media".into()
    } else {
        trimmed.chars().take(64).collect()
    }
}

pub fn is_absolute(path: &Path) -> bool {
    path.is_absolute()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn layout_under_override() {
        let dir = tempdir().unwrap();
        let paths = AppPaths::from_home(dir.path());
        paths.ensure_layout().unwrap();
        assert!(paths.config_dir.is_dir());
        assert!(paths.locks_dir.is_dir());
        assert_eq!(paths.db_path, dir.path().join("state/videocaptionerr.db"));
    }

    #[test]
    fn sanitize_stem_basic() {
        assert_eq!(sanitize_stem("hello world"), "hello_world");
        assert_eq!(sanitize_stem("中文"), "media");
        assert_eq!(sanitize_stem("a/b\\c"), "a_b_c");
    }
}
