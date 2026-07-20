use super::*;

#[async_trait]
impl ArtifactRecoveryStore for SqliteArtifactStore {
    async fn recover(
        &self,
        roots: &[std::path::PathBuf],
    ) -> AppResult<videocaptionerr_core::ports::ArtifactRecoveryReport> {
        self.store
            .recover_artifacts(roots.to_vec())
            .await
            .map_err(ApplicationError::Adapter)
    }
}
