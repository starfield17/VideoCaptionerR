use super::*;

#[async_trait]
impl BatchCreationRepository for StoreHandle {
    async fn create_batch_graph(
        &self,
        request: BatchCreationRequest,
    ) -> AppResult<CreatedBatchGraph> {
        self.create_batch_graph(request)
            .await
            .map_err(ApplicationError::Adapter)
    }
}
