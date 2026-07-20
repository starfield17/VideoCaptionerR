use super::*;

impl SqliteStore {
    pub(crate) fn save_job_aggregate(
        &self,
        id: &str,
        batch_id: Option<&str>,
        status: &str,
        source_path: &str,
        profile_revision: &str,
        execution_snapshot_id: Option<&str>,
        aggregate_json: &str,
        expected: ExpectedVersion,
    ) -> VcResult<u64> {
        let now = chrono::Utc::now().to_rfc3339();
        let projection = execution_snapshot_id
            .map(|snapshot_id| {
                self.conn
                    .query_row(
                        "SELECT canonical_source_path, job_dir, profile_revision
                         FROM execution_snapshots WHERE snapshot_id = ?1",
                        [snapshot_id],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(|error| {
                        VcError::new(
                            ErrorCode::Internal,
                            format!("load execution snapshot projection: {error}"),
                        )
                    })?
                    .ok_or_else(|| {
                        VcError::new(
                            ErrorCode::InvalidArgument,
                            format!("execution snapshot {snapshot_id} not found"),
                        )
                    })
            })
            .transpose()?;
        let source_path = projection
            .as_ref()
            .map(|value| value.0.as_str())
            .unwrap_or(source_path);
        let job_dir = projection
            .as_ref()
            .map(|value| value.1.as_str())
            .unwrap_or("");
        let profile_revision = projection
            .as_ref()
            .map(|value| value.2.as_str())
            .unwrap_or(profile_revision);

        match expected {
            ExpectedVersion::New => {
                self.conn
                    .execute(
                        "INSERT INTO jobs (
                            id, batch_id, status, source_path, job_dir, profile_revision,
                            execution_snapshot_id, aggregate_json, aggregate_version,
                            created_at, updated_at
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1, ?9, ?9)",
                        params![
                            id,
                            batch_id,
                            status,
                            source_path,
                            job_dir,
                            profile_revision,
                            execution_snapshot_id,
                            aggregate_json,
                            now
                        ],
                    )
                    .map_err(|error| {
                        if is_constraint(&error) {
                            stale_result("Job", id, expected)
                        } else {
                            VcError::new(
                                ErrorCode::Internal,
                                format!("insert job aggregate: {error}"),
                            )
                        }
                    })?;
                Ok(1)
            }
            ExpectedVersion::Exact(version) => {
                let changed = self
                    .conn
                    .execute(
                        "UPDATE jobs SET
                            batch_id = ?1, status = ?2, source_path = ?3, job_dir = ?4,
                            profile_revision = ?5, execution_snapshot_id = ?6,
                            aggregate_json = ?7, aggregate_version = aggregate_version + 1,
                            updated_at = ?8
                         WHERE id = ?9 AND aggregate_version = ?10",
                        params![
                            batch_id,
                            status,
                            source_path,
                            job_dir,
                            profile_revision,
                            execution_snapshot_id,
                            aggregate_json,
                            now,
                            id,
                            version as i64
                        ],
                    )
                    .map_err(|error| {
                        VcError::new(
                            ErrorCode::Internal,
                            format!("update job aggregate: {error}"),
                        )
                    })?;
                if changed != 1 {
                    return Err(stale_result("Job", id, expected));
                }
                version.checked_add(1).ok_or_else(|| {
                    VcError::new(ErrorCode::Internal, "Job aggregate version overflow")
                })
            }
        }
    }

    pub(crate) fn load_job_aggregate(&self, id: &str) -> VcResult<Option<(String, u64)>> {
        self.conn
            .query_row(
                "SELECT aggregate_json, aggregate_version FROM jobs WHERE id = ?1",
                [id],
                |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64)),
            )
            .optional()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("load job aggregate: {e}")))
    }

    pub(crate) fn list_job_aggregates(&self) -> VcResult<Vec<(String, u64)>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT aggregate_json, aggregate_version FROM jobs
                 WHERE aggregate_json IS NOT NULL
                 ORDER BY created_at, id",
            )
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("list job aggregates: {e}")))?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("query job aggregates: {e}")))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("read job aggregates: {e}")))
    }

    pub(crate) fn delete_job_record(&self, id: &str) -> VcResult<()> {
        self.conn
            .execute("DELETE FROM jobs WHERE id = ?1", [id])
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("delete job: {e}")))?;
        Ok(())
    }

    #[cfg(test)]
    pub fn insert_job(
        &self,
        id: &str,
        batch_id: Option<&str>,
        source_path: &str,
        job_dir: &str,
        status: &str,
    ) -> VcResult<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn
            .execute(
                "INSERT INTO jobs (id, batch_id, status, source_path, job_dir, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
                params![id, batch_id, status, source_path, job_dir, now],
            )
            .map_err(|e| {
                VcError::new(ErrorCode::Internal, format!("insert job: {e}"))
            })?;
        Ok(())
    }

    #[cfg(test)]
    pub fn get_job_status(&self, id: &str) -> VcResult<Option<String>> {
        self.conn
            .query_row("SELECT status FROM jobs WHERE id = ?1", [id], |r| r.get(0))
            .optional()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("get job: {e}")))
    }
}
