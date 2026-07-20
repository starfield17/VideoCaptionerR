//! Managed, family-isolated Python environments.
//!
//! Layout (ADR 0016):
//!   envs/faster-whisper/<lock_hash>/
//!   envs/mlx-whisper/<lock_hash>/
//!
//! Never uses personal Conda paths. Never `pip install latest` at runtime.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use blake3::Hasher;
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineFamily {
    Fake,
    WhisperCpp,
    FasterWhisper,
    MlxWhisper,
}

impl EngineFamily {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fake => "fake",
            Self::WhisperCpp => "whisper-cpp",
            Self::FasterWhisper => "faster-whisper",
            Self::MlxWhisper => "mlx-whisper",
        }
    }

    pub fn requires_python(self) -> bool {
        matches!(self, Self::FasterWhisper | Self::MlxWhisper)
    }
}

#[derive(Debug, Clone)]
pub struct ManagedEnvConfig {
    pub family: EngineFamily,
    pub envs_root: PathBuf,
    pub runtimes_root: PathBuf,
    pub uv_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct ManagedPythonEnv {
    pub family: EngineFamily,
    pub lock_hash: String,
    pub root: PathBuf,
}

impl ManagedPythonEnv {
    pub fn python_bin(&self) -> PathBuf {
        #[cfg(windows)]
        {
            self.root.join("Scripts").join("python.exe")
        }
        #[cfg(not(windows))]
        {
            self.root.join("bin").join("python")
        }
    }

    pub fn is_ready(&self) -> bool {
        self.python_bin().is_file() && self.root.join(".smoke-ok").is_file()
    }
}

/// Compute a stable lock hash from the family's lock/requirements files.
pub fn lock_hash_for_family(runtimes_root: &Path, family: EngineFamily) -> VcResult<String> {
    let dir = runtimes_root.join(family.as_str());
    let mut hasher = Hasher::new();
    for name in [
        "uv.lock",
        "requirements.lock",
        "pyproject.toml",
        "requirements.txt",
    ] {
        let path = dir.join(name);
        if path.is_file() {
            hasher.update(name.as_bytes());
            hasher.update(b"\0");
            let bytes = fs::read(&path).map_err(|e| {
                VcError::new(
                    ErrorCode::RuntimeUnavailable,
                    format!("read {}: {e}", path.display()),
                )
            })?;
            hasher.update(&bytes);
            hasher.update(b"\0");
        }
    }
    // Platform/accelerator variant.
    hasher.update(std::env::consts::OS.as_bytes());
    hasher.update(b"-");
    hasher.update(std::env::consts::ARCH.as_bytes());
    Ok(hex::encode(&hasher.finalize().as_bytes()[..16]))
}

/// Ensure a managed env exists for the family. Creates via `uv` when missing.
pub fn ensure_managed_env(config: &ManagedEnvConfig) -> VcResult<ManagedPythonEnv> {
    if !config.family.requires_python() {
        return Err(VcError::new(
            ErrorCode::InvalidArgument,
            format!(
                "engine family {} does not use a managed Python env",
                config.family.as_str()
            ),
        ));
    }
    let lock_hash = lock_hash_for_family(&config.runtimes_root, config.family)?;
    let root = config
        .envs_root
        .join(config.family.as_str())
        .join(&lock_hash);
    let env = ManagedPythonEnv {
        family: config.family,
        lock_hash,
        root: root.clone(),
    };
    if env.is_ready() {
        return Ok(env);
    }
    create_env(config, &env)?;
    smoke_test(&env)?;
    fs::write(env.root.join(".smoke-ok"), b"ok").map_err(|e| {
        VcError::new(
            ErrorCode::RuntimeUnavailable,
            format!("write smoke marker: {e}"),
        )
    })?;
    Ok(env)
}

fn create_env(config: &ManagedEnvConfig, env: &ManagedPythonEnv) -> VcResult<()> {
    fs::create_dir_all(&env.root).map_err(|e| {
        VcError::new(
            ErrorCode::RuntimeUnavailable,
            format!("create env dir {}: {e}", env.root.display()),
        )
    })?;
    let uv = config.uv_path.clone().or_else(find_uv).ok_or_else(|| {
        VcError::new(
            ErrorCode::RuntimeUnavailable,
            "uv not found; install uv to provision managed ASR Python envs",
        )
    })?;
    let family_dir = config.runtimes_root.join(config.family.as_str());
    let requirements = family_dir.join("requirements.lock");
    let requirements = if requirements.is_file() {
        requirements
    } else {
        family_dir.join("requirements.txt")
    };
    if !requirements.is_file() {
        return Err(VcError::new(
            ErrorCode::RuntimeUnavailable,
            format!(
                "missing lock/requirements for {}: {}",
                config.family.as_str(),
                requirements.display()
            ),
        ));
    }

    // Create venv.
    let status = Command::new(&uv)
        .args(["venv", "--python", "3.11"])
        .arg(&env.root)
        .status()
        .map_err(|e| {
            VcError::new(
                ErrorCode::RuntimeUnavailable,
                format!("uv venv failed: {e}"),
            )
        })?;
    if !status.success() {
        // Retry without pinned python version.
        let status = Command::new(&uv)
            .arg("venv")
            .arg(&env.root)
            .status()
            .map_err(|e| {
                VcError::new(
                    ErrorCode::RuntimeUnavailable,
                    format!("uv venv failed: {e}"),
                )
            })?;
        if !status.success() {
            return Err(VcError::new(
                ErrorCode::RuntimeUnavailable,
                format!("uv venv exited with {status}"),
            ));
        }
    }

    let status = Command::new(&uv)
        .args(["pip", "install", "--python"])
        .arg(env.python_bin())
        .arg("-r")
        .arg(&requirements)
        .status()
        .map_err(|e| {
            VcError::new(
                ErrorCode::RuntimeUnavailable,
                format!("uv pip install failed: {e}"),
            )
        })?;
    if !status.success() {
        return Err(VcError::new(
            ErrorCode::RuntimeUnavailable,
            format!("uv pip install exited with {status}"),
        ));
    }
    Ok(())
}

fn smoke_test(env: &ManagedPythonEnv) -> VcResult<()> {
    let module = match env.family {
        EngineFamily::FasterWhisper => "faster_whisper",
        EngineFamily::MlxWhisper => "mlx_whisper",
        EngineFamily::Fake | EngineFamily::WhisperCpp => {
            return Ok(());
        }
    };
    let code = format!(
        "import {module}; import sys; print(getattr({module}, '__version__', 'ok'), file=sys.stderr)"
    );
    let output = Command::new(env.python_bin())
        .args(["-c", &code])
        .output()
        .map_err(|e| {
            VcError::new(
                ErrorCode::RuntimeSmokeTestFailed,
                format!("smoke import failed to start: {e}"),
            )
        })?;
    if !output.status.success() {
        return Err(VcError::new(
            ErrorCode::RuntimeSmokeTestFailed,
            format!(
                "smoke import of {module} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        ));
    }
    Ok(())
}

fn find_uv() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("VIDEOCAPTIONERR_UV") {
        let path = PathBuf::from(p);
        if path.is_file() {
            return Some(path);
        }
    }
    which("uv")
}

fn which(bin: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(bin);
        if candidate.is_file() {
            return Some(candidate);
        }
        #[cfg(windows)]
        {
            let candidate = dir.join(format!("{bin}.exe"));
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_hash_is_stable_for_same_inputs() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/runtimes");
        let a = lock_hash_for_family(&root, EngineFamily::FasterWhisper).unwrap();
        let b = lock_hash_for_family(&root, EngineFamily::FasterWhisper).unwrap();
        assert_eq!(a, b);
        assert!(!a.is_empty());
    }
}
