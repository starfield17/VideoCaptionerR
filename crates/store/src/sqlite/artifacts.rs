use super::*;

impl SqliteStore {
    pub fn commit_artifact_and_unit(
        &mut self,
        meta: &ArtifactMeta,
        work_unit_id: Option<&str>,
    ) -> VcResult<()> {
        let tx = self.conn.unchecked_transaction().map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("begin commit tx: {e}"),
            )
        })?;

        tx.execute(
            "INSERT INTO artifacts (
                id, job_id, stage, kind, path, content_hash, schema_version,
                producer_fingerprint, created_at, committed
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1)",
            params![
                meta.id,
                meta.job_id,
                meta.stage,
                meta.kind.as_str(),
                meta.path,
                meta.content_hash,
                meta.schema_version as i64,
                meta.producer_fingerprint,
                meta.created_at,
            ],
        )
        .map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("insert artifact: {e}"),
            )
        })?;

        if let Some(unit_id) = work_unit_id {
            let now = chrono::Utc::now().to_rfc3339();
            let aggregate_json: String = tx
                .query_row(
                    "SELECT COALESCE(aggregate_json, '') FROM work_units WHERE id = ?1",
                    [unit_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("load work unit for artifact commit: {e}"),
                    )
                })?
                .ok_or_else(|| {
                    VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("work unit not found during artifact commit: {unit_id}"),
                    )
                })?;
            if aggregate_json.is_empty() {
                let changed = tx
                    .execute(
                        "UPDATE work_units SET status = ?1, artifact_id = ?2, finished_at = ?3,
                         lease_owner = NULL, lease_expires_at = NULL,
                         aggregate_version = aggregate_version + 1
                         WHERE id = ?4 AND status = 'running'",
                        params![
                            work_unit_status_name(WorkUnitStatus::Done),
                            meta.id,
                            now,
                            unit_id
                        ],
                    )
                    .map_err(|e| {
                        VcError::new(
                            ErrorCode::ArtifactCommitFailed,
                            format!("update legacy work unit: {e}"),
                        )
                    })?;
                if changed != 1 {
                    return Err(VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("work unit {unit_id} was not running during artifact commit"),
                    ));
                }
                return tx.commit().map_err(|e| {
                    VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("commit artifact tx: {e}"),
                    )
                });
            }
            let mut unit: videocaptionerr_domain::WorkUnit = serde_json::from_str(&aggregate_json)
                .map_err(|e| {
                    VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode work unit during artifact commit: {e}"),
                    )
                })?;
            if unit.job_id().as_str() != meta.job_id {
                return Err(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    "work unit and artifact belong to different Jobs",
                ));
            }
            let artifact_id: UlidStr = meta.id.parse().map_err(|e| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("invalid artifact id during work unit commit: {e}"),
                )
            })?;
            let artifact_stage =
                videocaptionerr_domain::StageKind::parse(&meta.stage).ok_or_else(|| {
                    VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!(
                            "invalid artifact stage during work unit commit: {}",
                            meta.stage
                        ),
                    )
                })?;
            unit.complete(videocaptionerr_domain::ArtifactRef {
                id: artifact_id,
                stage: artifact_stage,
                path: meta.path.clone(),
                content_hash: meta.content_hash.clone(),
                schema_version: meta.schema_version,
                producer_fingerprint: meta.producer_fingerprint.clone(),
            })
            .map_err(VcError::from)?;
            let updated_json = serde_json::to_string(&unit).map_err(|e| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("encode completed work unit: {e}"),
                )
            })?;
            let changed = tx
                .execute(
                    "UPDATE work_units SET status = ?1, artifact_id = ?2, finished_at = ?3,
                     lease_owner = NULL, lease_expires_at = NULL, aggregate_json = ?5,
                     aggregate_version = aggregate_version + 1
                     WHERE id = ?4 AND status = 'running'",
                    params![
                        work_unit_status_name(WorkUnitStatus::Done),
                        meta.id,
                        now,
                        unit_id,
                        updated_json,
                    ],
                )
                .map_err(|e| {
                    VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("update work unit: {e}"),
                    )
                })?;
            if changed != 1 {
                return Err(VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("work unit {unit_id} was not running during artifact commit"),
                ));
            }
        }

        tx.commit().map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("commit artifact tx: {e}"),
            )
        })?;
        Ok(())
    }

    /// Publish a prepared artifact and atomically persist the control-plane
    /// state that makes it reachable. A file published by this invocation is
    /// removed if the SQLite transaction fails; a process crash between those
    /// operations is handled by `recover_artifacts` on the next startup.
    #[cfg(test)]
    pub fn new_artifact_meta(
        job_id: &str,
        stage: &str,
        kind: ArtifactKind,
        path: &str,
        content_hash: &str,
        producer_fingerprint: &str,
    ) -> ArtifactMeta {
        ArtifactMeta::new(
            UlidStr::from(Ulid::new()).into_string(),
            job_id,
            stage,
            kind,
            path,
            content_hash,
            producer_fingerprint,
            chrono::Utc::now().to_rfc3339(),
        )
    }
}
