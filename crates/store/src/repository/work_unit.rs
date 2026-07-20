use super::*;

#[async_trait]
impl WorkUnitRepository for StoreHandle {
    async fn load_work_unit(
        &self,
        id: &videocaptionerr_domain::WorkUnitId,
    ) -> AppResult<Option<Versioned<WorkUnit>>> {
        let row = self
            .load_work_unit_aggregate(id.as_str())
            .await
            .map_err(ApplicationError::Adapter)?;
        row.map(|(body, version)| {
            serde_json::from_str(&body)
                .map_err(|error| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode WorkUnit aggregate: {error}"),
                    ))
                })
                .map(|value| Versioned::with_version(value, version))
        })
        .transpose()
    }

    async fn find_work_unit(
        &self,
        job_id: &videocaptionerr_domain::JobId,
        stage: StageKind,
        unit_kind: &str,
        unit_index: u32,
        input_hash: &str,
    ) -> AppResult<Option<Versioned<WorkUnit>>> {
        let row = self
            .find_work_unit_aggregate(
                job_id.as_str(),
                stage_name(stage),
                unit_kind,
                unit_index,
                input_hash,
            )
            .await
            .map_err(ApplicationError::Adapter)?;
        row.map(|(body, version)| {
            serde_json::from_str(&body)
                .map_err(|error| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode WorkUnit aggregate: {error}"),
                    ))
                })
                .map(|value| Versioned::with_version(value, version))
        })
        .transpose()
    }

    async fn save_work_unit(
        &self,
        unit: &mut Versioned<WorkUnit>,
        expected: ExpectedVersion,
    ) -> AppResult<()> {
        let json = serde_json::to_string(&unit.value).map_err(|error| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode WorkUnit aggregate: {error}"),
            ))
        })?;
        let (lease_owner, lease_expires_at) = unit
            .lease()
            .map(|lease| {
                (
                    Some(lease.owner.as_str()),
                    DateTime::<Utc>::from_timestamp_millis(lease.expires_at_ms as i64)
                        .map(|value| value.to_rfc3339()),
                )
            })
            .unwrap_or((None, None));
        self.save_work_unit_aggregate(
            WorkUnitRecord {
                id: unit.id().to_string(),
                job_id: unit.job_id().to_string(),
                stage: stage_name(unit.stage()).into(),
                unit_kind: unit.unit_kind().into(),
                unit_index: unit.unit_index(),
                input_hash: unit.input_hash().into(),
                status: unit.status().as_str().into(),
                attempt: unit.attempt(),
                lease_owner: lease_owner.map(str::to_owned),
                lease_expires_at,
                artifact_id: unit.artifact().map(|artifact| artifact.id.to_string()),
                aggregate_json: json,
            },
            expected,
        )
        .await
        .map_err(ApplicationError::Adapter)
        .map(|version| {
            unit.version = version;
        })
    }

    async fn recover_expired(&self, now_ms: u64) -> AppResult<u32> {
        let now = DateTime::<Utc>::from_timestamp_millis(now_ms as i64).ok_or_else(|| {
            ApplicationError::Invalid("recovery timestamp is outside chrono range".into())
        })?;
        let rows = self
            .list_expired_work_unit_aggregates(&now.to_rfc3339())
            .await
            .map_err(ApplicationError::Adapter)?;
        for (body, version) in &rows {
            let mut unit: WorkUnit = serde_json::from_str(body).map_err(|error| {
                ApplicationError::Adapter(VcError::new(
                    ErrorCode::ArtifactCorrupt,
                    format!("decode expired WorkUnit aggregate: {error}"),
                ))
            })?;
            unit.recover_expired(now_ms)
                .map_err(ApplicationError::Domain)?;
            let mut versioned = Versioned::with_version(unit, *version);
            let expected = versioned.expected_version();
            self.save_work_unit(&mut versioned, expected).await?;
        }
        u32::try_from(rows.len()).map_err(|_| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::Internal,
                "expired work-unit count exceeds u32",
            ))
        })
    }

    async fn count_retryable(
        &self,
        job_id: &videocaptionerr_domain::JobId,
        from_stage: Option<StageKind>,
    ) -> AppResult<u32> {
        self.count_retryable_aggregates(job_id.as_str(), from_stage.map(stage_name))
            .await
            .map_err(ApplicationError::Adapter)
    }

    async fn lease_next_ready(
        &self,
        job_id: &videocaptionerr_domain::JobId,
        stage: StageKind,
        owner: &str,
        now_ms: u64,
        lease_ms: u64,
    ) -> AppResult<Option<Versioned<WorkUnit>>> {
        let now = DateTime::<Utc>::from_timestamp_millis(now_ms as i64).ok_or_else(|| {
            ApplicationError::Invalid("lease timestamp is outside chrono range".into())
        })?;
        let expires_ms = now_ms
            .checked_add(lease_ms)
            .ok_or_else(|| ApplicationError::Invalid("lease expiry timestamp overflowed".into()))?;
        let expires =
            DateTime::<Utc>::from_timestamp_millis(expires_ms as i64).ok_or_else(|| {
                ApplicationError::Invalid("lease expiry is outside chrono range".into())
            })?;
        let row = self
            .lease_next_ready_aggregate(LeaseRequest {
                job_id: job_id.to_string(),
                stage: stage_name(stage).to_string(),
                owner: owner.to_string(),
                now_rfc3339: now.to_rfc3339(),
                now_ms,
                expires_rfc3339: expires.to_rfc3339(),
                expires_at_ms: expires_ms,
            })
            .await
            .map_err(ApplicationError::Adapter)?;
        row.map(|(body, version)| {
            serde_json::from_str(&body)
                .map_err(|error| {
                    ApplicationError::Adapter(VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode leased WorkUnit: {error}"),
                    ))
                })
                .map(|value| Versioned::with_version(value, version))
        })
        .transpose()
    }

    async fn retry_failed(
        &self,
        job_id: &videocaptionerr_domain::JobId,
        from_stage: Option<StageKind>,
    ) -> AppResult<u32> {
        self.retry_failed_aggregates(job_id.as_str(), from_stage.map(stage_name))
            .await
            .map_err(ApplicationError::Adapter)
    }
}
