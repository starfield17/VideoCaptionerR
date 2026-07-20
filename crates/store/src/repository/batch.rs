use super::*;

#[async_trait]
impl BatchRepository for StoreHandle {
    async fn load_batch(&self, id: &BatchId) -> AppResult<Option<Versioned<Batch>>> {
        let row = self
            .load_batch_aggregate(id.as_str())
            .await
            .map_err(ApplicationError::Adapter)?;
        row.map(|(body, version)| {
            serde_json::from_str(&body)
                .map_err(|error| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode Batch aggregate: {error}"),
                    ))
                })
                .map(|value| Versioned::with_version(value, version))
        })
        .transpose()
    }

    async fn list_batches(&self) -> AppResult<Vec<Versioned<Batch>>> {
        let rows = self
            .list_batch_aggregates()
            .await
            .map_err(ApplicationError::Adapter)?;
        rows.into_iter()
            .map(|(body, version)| {
                serde_json::from_str(&body)
                    .map_err(|error| {
                        ApplicationError::Adapter(VcError::new(
                            ErrorCode::ArtifactCorrupt,
                            format!("decode Batch aggregate: {error}"),
                        ))
                    })
                    .map(|value| Versioned::with_version(value, version))
            })
            .collect()
    }

    async fn save_batch(
        &self,
        batch: &mut Versioned<Batch>,
        expected: ExpectedVersion,
    ) -> AppResult<()> {
        let json = serde_json::to_string(&batch.value).map_err(|error| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode Batch aggregate: {error}"),
            ))
        })?;
        self.save_batch_aggregate(
            batch.id().as_str(),
            batch.status().as_str(),
            &batch.execution_profile().asr_model,
            &batch.execution_profile().device,
            &json,
            expected,
        )
        .await
        .map_err(ApplicationError::Adapter)
        .map(|version| {
            batch.version = version;
        })
    }
}
