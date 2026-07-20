use super::*;

#[async_trait]
impl JobRepository for StoreHandle {
    async fn load_job(&self, id: &JobId) -> AppResult<Option<Versioned<Job>>> {
        let row = self
            .load_job_aggregate(id.as_str())
            .await
            .map_err(ApplicationError::Adapter)?;
        row.map(|(body, version)| {
            serde_json::from_str(&body)
                .map_err(|error| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode Job aggregate: {error}"),
                    ))
                })
                .map(|value| Versioned::with_version(value, version))
        })
        .transpose()
    }

    async fn save_job(&self, job: &mut Versioned<Job>, expected: ExpectedVersion) -> AppResult<()> {
        let json = serde_json::to_string(&job.value).map_err(|error| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode Job aggregate: {error}"),
            ))
        })?;
        self.save_job_aggregate(
            job.id().as_str(),
            job.batch_id().map(|id| id.as_str()),
            job.status().as_str(),
            job.source_path(),
            job.profile_revision().as_str(),
            job.execution_snapshot_id().map(|id| id.as_str()),
            &json,
            expected,
        )
        .await
        .map_err(ApplicationError::Adapter)
        .map(|version| {
            job.version = version;
        })
    }

    async fn delete_job(&self, id: &JobId) -> AppResult<()> {
        self.delete_job_record(id.as_str())
            .await
            .map_err(ApplicationError::Adapter)
    }

    async fn list_jobs(&self) -> AppResult<Vec<Versioned<Job>>> {
        let rows = self
            .list_job_aggregates()
            .await
            .map_err(ApplicationError::Adapter)?;
        rows.into_iter()
            .map(|(body, version)| {
                serde_json::from_str(&body)
                    .map_err(|error| {
                        ApplicationError::Adapter(VcError::new(
                            ErrorCode::ArtifactCorrupt,
                            format!("decode Job aggregate: {error}"),
                        ))
                    })
                    .map(|value| Versioned::with_version(value, version))
            })
            .collect()
    }
}
