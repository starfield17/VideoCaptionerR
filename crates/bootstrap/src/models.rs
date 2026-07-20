//! Explicit model install / verify (no silent default download).

use std::path::{Path, PathBuf};

use videocaptionerr_asr::{download_model, verify_model_file, ModelManifest};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};

use crate::runtime::ApplicationRuntime;

pub struct ModelInstallResult {
    pub model_id: String,
    pub path: PathBuf,
    pub sha256: Option<String>,
}

impl ApplicationRuntime {
    /// Download and digest-verify a model from the builtin/user manifest.
    /// Must be called explicitly; never triggered by transcribe defaults.
    pub async fn install_model(
        &self,
        model_id: &str,
        dest: Option<PathBuf>,
    ) -> VcResult<ModelInstallResult> {
        let manifest_path = self.paths.home.join("models").join("manifest.toml");
        let manifest = ModelManifest::load_or_default(&manifest_path);
        let entry = manifest.find(model_id).ok_or_else(|| {
            VcError::new(
                ErrorCode::ModelNotFound,
                format!("model id '{model_id}' not in manifest"),
            )
        })?;
        let dest_dir = dest.unwrap_or_else(|| {
            self.paths
                .models_dir
                .join(entry.engine_family.replace('/', "_"))
        });
        let path = download_model(entry, &dest_dir).await?;
        let sha256 = if let Some(expected) = &entry.expected_sha256 {
            verify_model_file(&path, expected)?;
            Some(expected.clone())
        } else {
            None
        };
        Ok(ModelInstallResult {
            model_id: model_id.into(),
            path,
            sha256,
        })
    }

    /// Verify an on-disk model file against a hex digest (no download).
    pub fn verify_model(&self, path: &Path, expected_sha256: &str) -> VcResult<()> {
        if !path.is_file() {
            return Err(VcError::new(
                ErrorCode::ModelNotFound,
                format!("model file not found: {}", path.display()),
            ));
        }
        verify_model_file(path, expected_sha256)
    }
}
