use super::*;
use videocaptionerr_core::ports::{
    RetryTransactionRepository, RetryTransactionRequest, RetryTransactionResult,
};

#[async_trait]
impl StageCommitRepository for StoreHandle {
    async fn commit_stage(&self, request: StageCommitRequest) -> AppResult<StageCommitResult> {
        StoreHandle::commit_stage(self, request)
            .await
            .map_err(ApplicationError::Adapter)
    }
}

#[async_trait]
impl RetryTransactionRepository for StoreHandle {
    async fn apply_retry(&self, request: RetryTransactionRequest) -> AppResult<RetryTransactionResult> {
        StoreHandle::apply_retry(self, request)
            .await
            .map_err(ApplicationError::Adapter)
    }
}
