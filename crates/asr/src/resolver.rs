//! Application-owned ASR runtime resolver.
//!
//! Maps an [`AsrRuntimeSpec`] engine family to a concrete worker launch plan.
//! Domain never sees Python paths or filesystem probes.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use videocaptionerr_contracts::error::{ErrorCode, VcError};
use videocaptionerr_core::application_error::{AppResult, ApplicationError};
use videocaptionerr_core::ports::{AsrRuntime, AsrRuntimeResolver, AsrRuntimeSpec, ModelLocator};

use crate::application::WorkerAsrRuntime;
use crate::python_env::{ensure_managed_env, EngineFamily, ManagedEnvConfig};
use crate::worker::resolve_helper_binary;

/// Resolves engine families to helper / managed-Python workers.
#[derive(Debug, Clone)]
pub struct FamilyAsrRuntimeResolver {
    helper_path: PathBuf,
    python_runtimes_root: PathBuf,
    envs_root: PathBuf,
    /// Optional override for uv binary. Defaults to PATH lookup.
    uv_path: Option<PathBuf>,
}

impl FamilyAsrRuntimeResolver {
    pub fn new(
        helper_path: impl Into<PathBuf>,
        python_runtimes_root: impl Into<PathBuf>,
        envs_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            helper_path: helper_path.into(),
            python_runtimes_root: python_runtimes_root.into(),
            envs_root: envs_root.into(),
            uv_path: None,
        }
    }

    pub fn with_default_paths(home: &Path) -> Self {
        let runtimes = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/runtimes");
        Self::new(resolve_helper_binary(), runtimes, home.join("envs"))
    }

    pub fn with_uv_path(mut self, uv: impl Into<PathBuf>) -> Self {
        self.uv_path = Some(uv.into());
        self
    }

    pub fn helper_path(&self) -> &Path {
        &self.helper_path
    }

    fn map_family(engine: &str) -> AppResult<EngineFamily> {
        match engine {
            "fake" => Ok(EngineFamily::Fake),
            "whisper-cpp" => Ok(EngineFamily::WhisperCpp),
            "faster-whisper" => Ok(EngineFamily::FasterWhisper),
            "mlx-whisper" => Ok(EngineFamily::MlxWhisper),
            other => Err(ApplicationError::Adapter(VcError::new(
                ErrorCode::RuntimeUnavailable,
                format!("unsupported ASR engine family '{other}'"),
            ))),
        }
    }

    fn validate_locator_for_family(family: EngineFamily, locator: &ModelLocator) -> AppResult<()> {
        match (family, locator) {
            (EngineFamily::Fake, _) => Ok(()),
            (EngineFamily::WhisperCpp, ModelLocator::File { path }) => {
                if !Path::new(path).is_file() {
                    return Err(ApplicationError::Adapter(VcError::new(
                        ErrorCode::ModelNotFound,
                        format!("whisper-cpp model file not found: {path}"),
                    )));
                }
                Ok(())
            }
            (EngineFamily::WhisperCpp, other) => Err(ApplicationError::Invalid(format!(
                "whisper-cpp requires a File locator, got {}",
                other.display()
            ))),
            (
                EngineFamily::FasterWhisper,
                ModelLocator::Directory { path } | ModelLocator::File { path },
            ) => {
                if !Path::new(path).exists() {
                    return Err(ApplicationError::Adapter(VcError::new(
                        ErrorCode::ModelNotFound,
                        format!("faster-whisper model path not found: {path}"),
                    )));
                }
                Ok(())
            }
            (EngineFamily::FasterWhisper, ModelLocator::HuggingFaceSnapshot { .. }) => Ok(()),
            (
                EngineFamily::MlxWhisper,
                ModelLocator::Directory { path } | ModelLocator::File { path },
            ) => {
                if !Path::new(path).exists() {
                    return Err(ApplicationError::Adapter(VcError::new(
                        ErrorCode::ModelNotFound,
                        format!("mlx-whisper model path not found: {path}"),
                    )));
                }
                Ok(())
            }
            (EngineFamily::MlxWhisper, ModelLocator::HuggingFaceSnapshot { .. }) => Ok(()),
        }
    }

    fn verify_digest(spec: &AsrRuntimeSpec) -> AppResult<()> {
        let Some(expected) = spec.verified_digest.as_deref() else {
            return Ok(());
        };
        let Some(path) = spec.locator.local_path() else {
            return Ok(());
        };
        if !path.is_file() {
            // Directory / snapshot digests are tracked at publish time.
            return Ok(());
        }
        let actual = crate::model::blake3_file(&path).map_err(ApplicationError::Adapter)?;
        let expected_norm = expected
            .strip_prefix("blake3:")
            .unwrap_or(expected)
            .to_ascii_lowercase();
        let actual_norm = actual
            .strip_prefix("blake3:")
            .unwrap_or(&actual)
            .to_ascii_lowercase();
        if expected_norm != actual_norm {
            return Err(ApplicationError::Adapter(VcError::new(
                ErrorCode::ModelDigestMismatch,
                format!(
                    "model digest mismatch for {}: expected {expected}, got {actual}",
                    path.display()
                ),
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl AsrRuntimeResolver for FamilyAsrRuntimeResolver {
    async fn resolve(&self, spec: &AsrRuntimeSpec) -> AppResult<Box<dyn AsrRuntime>> {
        spec.validate().map_err(ApplicationError::Invalid)?;
        let family = Self::map_family(&spec.engine_family)?;
        Self::validate_locator_for_family(family, &spec.locator)?;
        Self::verify_digest(spec)?;

        match family {
            EngineFamily::Fake => Ok(Box::new(WorkerAsrRuntime::helper(
                self.helper_path.clone(),
                "fake",
                spec.clone(),
            ))),
            EngineFamily::WhisperCpp => Ok(Box::new(WorkerAsrRuntime::helper(
                self.helper_path.clone(),
                "whisper-cpp",
                spec.clone(),
            ))),
            EngineFamily::FasterWhisper => {
                let env = ensure_managed_env(&ManagedEnvConfig {
                    family: EngineFamily::FasterWhisper,
                    envs_root: self.envs_root.clone(),
                    runtimes_root: self.python_runtimes_root.clone(),
                    uv_path: self.uv_path.clone(),
                })
                .map_err(ApplicationError::Adapter)?;
                let script = self
                    .python_runtimes_root
                    .join("faster-whisper")
                    .join("worker.py");
                Ok(Box::new(WorkerAsrRuntime::python(
                    env.python_bin(),
                    script,
                    "faster-whisper",
                    spec.clone(),
                )))
            }
            EngineFamily::MlxWhisper => {
                if !cfg!(all(target_os = "macos", target_arch = "aarch64")) {
                    // Protocol-level fake tests still use the fake family on Linux.
                    // Real MLX is Apple Silicon only.
                    return Err(ApplicationError::Adapter(VcError::new(
                        ErrorCode::RuntimeUnavailable,
                        "mlx-whisper real runtime is only available on macOS Apple Silicon",
                    )));
                }
                let env = ensure_managed_env(&ManagedEnvConfig {
                    family: EngineFamily::MlxWhisper,
                    envs_root: self.envs_root.clone(),
                    runtimes_root: self.python_runtimes_root.clone(),
                    uv_path: self.uv_path.clone(),
                })
                .map_err(ApplicationError::Adapter)?;
                let script = self
                    .python_runtimes_root
                    .join("mlx-whisper")
                    .join("worker.py");
                Ok(Box::new(WorkerAsrRuntime::python(
                    env.python_bin(),
                    script,
                    "mlx-whisper",
                    spec.clone(),
                )))
            }
        }
    }
}

/// Test helper: always returns the same prebuilt runtime.
pub struct FixedAsrRuntimeResolver {
    runtime: Arc<dyn AsrRuntime>,
}

impl FixedAsrRuntimeResolver {
    pub fn new(runtime: Arc<dyn AsrRuntime>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl AsrRuntimeResolver for FixedAsrRuntimeResolver {
    async fn resolve(&self, _spec: &AsrRuntimeSpec) -> AppResult<Box<dyn AsrRuntime>> {
        Ok(Box::new(ClonedRuntime(self.runtime.clone())))
    }
}

struct ClonedRuntime(Arc<dyn AsrRuntime>);

#[async_trait]
impl AsrRuntime for ClonedRuntime {
    async fn open_session(
        &self,
        profile: &videocaptionerr_domain::BatchExecutionProfile,
    ) -> AppResult<Box<dyn videocaptionerr_core::ports::AsrSession>> {
        self.0.open_session(profile).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn maps_fake_family() {
        let resolver = FamilyAsrRuntimeResolver::new(
            resolve_helper_binary(),
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/runtimes"),
            tempfile::tempdir().unwrap().path().join("envs"),
        );
        let spec = AsrRuntimeSpec {
            engine_family: "fake".into(),
            model_id: "fake".into(),
            verified_digest: None,
            locator: ModelLocator::file("fake:default"),
            device: "cpu".into(),
            compute_type: "default".into(),
        };
        let runtime = resolver.resolve(&spec).await.unwrap();
        let _ = runtime;
    }

    #[tokio::test]
    async fn rejects_unknown_family() {
        let resolver = FamilyAsrRuntimeResolver::new(
            resolve_helper_binary(),
            PathBuf::from("."),
            PathBuf::from("."),
        );
        let spec = AsrRuntimeSpec {
            engine_family: "nemo".into(),
            model_id: "x".into(),
            verified_digest: None,
            locator: ModelLocator::file("/nope"),
            device: "cpu".into(),
            compute_type: "default".into(),
        };
        match resolver.resolve(&spec).await {
            Err(ApplicationError::Adapter(e)) => {
                assert_eq!(e.code, ErrorCode::RuntimeUnavailable);
            }
            Err(other) => panic!("expected adapter error, got {other:?}"),
            Ok(_) => panic!("expected error for unknown family"),
        }
    }

    #[tokio::test]
    async fn mlx_unavailable_on_non_apple() {
        if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
            return;
        }
        let resolver = FamilyAsrRuntimeResolver::new(
            resolve_helper_binary(),
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/runtimes"),
            tempfile::tempdir().unwrap().path().join("envs"),
        );
        let spec = AsrRuntimeSpec {
            engine_family: "mlx-whisper".into(),
            model_id: "tiny".into(),
            verified_digest: None,
            locator: ModelLocator::directory("/tmp"),
            device: "apple-silicon".into(),
            compute_type: "default".into(),
        };
        match resolver.resolve(&spec).await {
            Err(ApplicationError::Adapter(e)) => {
                assert_eq!(e.code, ErrorCode::RuntimeUnavailable);
            }
            Err(other) => panic!("expected adapter error, got {other:?}"),
            Ok(_) => panic!("expected mlx unavailable on this platform"),
        }
    }
}
