use super::*;

impl SqliteStore {
    pub(crate) fn save_batch_aggregate(
        &self,
        id: &str,
        status: &str,
        asr_model: &str,
        device: &str,
        aggregate_json: &str,
        expected: ExpectedVersion,
    ) -> VcResult<u64> {
        Self::save_batch_aggregate_on(
            &self.conn,
            id,
            status,
            asr_model,
            device,
            aggregate_json,
            expected,
        )
    }

    /// Write a Batch row on the caller-provided connection, including a
    /// transaction connection used by atomic first creation.
    pub(crate) fn save_batch_aggregate_on(
        conn: &Connection,
        id: &str,
        status: &str,
        asr_model: &str,
        device: &str,
        aggregate_json: &str,
        expected: ExpectedVersion,
    ) -> VcResult<u64> {
        let now = chrono::Utc::now().to_rfc3339();
        match expected {
            ExpectedVersion::New => {
                conn.execute(
                    "INSERT INTO batches (
                            id, status, asr_model_id, asr_device, aggregate_json,
                            aggregate_version, created_at, updated_at
                         ) VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?6)",
                    params![id, status, asr_model, device, aggregate_json, now],
                )
                .map_err(|error| {
                    if is_constraint(&error) {
                        stale_result("Batch", id, expected)
                    } else {
                        VcError::new(
                            ErrorCode::Internal,
                            format!("insert batch aggregate: {error}"),
                        )
                    }
                })?;
                Ok(1)
            }
            ExpectedVersion::Exact(version) => {
                let changed = conn
                    .execute(
                        "UPDATE batches SET
                            status = ?1, asr_model_id = ?2, asr_device = ?3,
                            aggregate_json = ?4, aggregate_version = aggregate_version + 1,
                            updated_at = ?5
                         WHERE id = ?6 AND aggregate_version = ?7",
                        params![
                            status,
                            asr_model,
                            device,
                            aggregate_json,
                            now,
                            id,
                            version as i64
                        ],
                    )
                    .map_err(|error| {
                        VcError::new(
                            ErrorCode::Internal,
                            format!("update batch aggregate: {error}"),
                        )
                    })?;
                if changed != 1 {
                    return Err(stale_result("Batch", id, expected));
                }
                version.checked_add(1).ok_or_else(|| {
                    VcError::new(ErrorCode::Internal, "Batch aggregate version overflow")
                })
            }
        }
    }

    pub(crate) fn load_batch_aggregate(&self, id: &str) -> VcResult<Option<(String, u64)>> {
        self.conn
            .query_row(
                "SELECT aggregate_json, aggregate_version FROM batches WHERE id = ?1",
                [id],
                |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64)),
            )
            .optional()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("load batch aggregate: {e}")))
    }

    pub(crate) fn list_batch_aggregates(&self) -> VcResult<Vec<(String, u64)>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT aggregate_json, aggregate_version FROM batches
                 WHERE aggregate_json IS NOT NULL ORDER BY created_at, id",
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("list batch aggregates: {error}"),
                )
            })?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("query batch aggregates: {error}"),
                )
            })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("read batch aggregates: {error}"),
            )
        })
    }
}
