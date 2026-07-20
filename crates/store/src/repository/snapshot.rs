use super::*;

#[async_trait]
impl SnapshotRepository for StoreHandle {
    async fn load_execution_snapshot(
        &self,
        id: &videocaptionerr_domain::UlidStr,
    ) -> AppResult<Option<JobExecutionSnapshot>> {
        let json = self
            .load_execution_snapshot(id.as_str())
            .await
            .map_err(ApplicationError::Adapter)?;
        json.map(|body| {
            serde_json::from_str(&body).map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCorrupt,
                    format!("decode execution snapshot: {error}"),
                ))
            })
        })
        .transpose()
    }

    async fn save_execution_snapshot(&self, snapshot: &JobExecutionSnapshot) -> AppResult<()> {
        self.save_execution_snapshot(snapshot.clone())
            .await
            .map_err(ApplicationError::Adapter)
    }

    async fn load_snapshots_for_batch(&self, id: &BatchId) -> AppResult<Vec<JobExecutionSnapshot>> {
        let rows = self
            .load_snapshots_for_batch(id.as_str())
            .await
            .map_err(ApplicationError::Adapter)?;
        rows.into_iter()
            .map(|body| {
                serde_json::from_str(&body).map_err(|error| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode batch execution snapshot: {error}"),
                    ))
                })
            })
            .collect()
    }
}
