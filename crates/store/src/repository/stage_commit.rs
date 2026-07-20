use super::*;

#[async_trait]
impl StageCommitRepository for StoreHandle {
    async fn commit_stage(&self, request: StageCommitRequest) -> AppResult<StageCommitResult> {
        StoreHandle::commit_stage(self, request)
            .await
            .map_err(ApplicationError::Adapter)
    }
}
