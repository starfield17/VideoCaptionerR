use super::*;

impl SqliteStore {
    pub(crate) fn save_work_unit_aggregate(
        &self,
        record: &WorkUnitRecord,
        expected: ExpectedVersion,
    ) -> VcResult<u64> {
        match expected {
            ExpectedVersion::New => {
                self.conn
                    .execute(
                        "INSERT INTO work_units (
                            id, job_id, stage, unit_kind, unit_index, input_hash, status, attempt,
                            artifact_id, lease_owner, lease_expires_at, aggregate_json,
                            aggregate_version
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 1)",
                        params![
                            record.id,
                            record.job_id,
                            record.stage,
                            record.unit_kind,
                            record.unit_index as i64,
                            record.input_hash,
                            record.status,
                            record.attempt as i64,
                            record.artifact_id,
                            record.lease_owner,
                            record.lease_expires_at,
                            record.aggregate_json,
                        ],
                    )
                    .map_err(|error| {
                        if is_constraint(&error) {
                            stale_result("WorkUnit", &record.id, expected)
                        } else {
                            VcError::new(ErrorCode::Internal, format!("insert work unit: {error}"))
                        }
                    })?;
                Ok(1)
            }
            ExpectedVersion::Exact(version) => {
                let changed = self
                    .conn
                    .execute(
                        "UPDATE work_units SET
                            job_id = ?1, stage = ?2, unit_kind = ?3, unit_index = ?4,
                            input_hash = ?5, status = ?6, attempt = ?7, artifact_id = ?8,
                            lease_owner = ?9, lease_expires_at = ?10, aggregate_json = ?11,
                            aggregate_version = aggregate_version + 1
                         WHERE id = ?12 AND aggregate_version = ?13",
                        params![
                            record.job_id,
                            record.stage,
                            record.unit_kind,
                            record.unit_index as i64,
                            record.input_hash,
                            record.status,
                            record.attempt as i64,
                            record.artifact_id,
                            record.lease_owner,
                            record.lease_expires_at,
                            record.aggregate_json,
                            record.id,
                            version as i64,
                        ],
                    )
                    .map_err(|error| {
                        VcError::new(ErrorCode::Internal, format!("update work unit: {error}"))
                    })?;
                if changed != 1 {
                    return Err(stale_result("WorkUnit", &record.id, expected));
                }
                version.checked_add(1).ok_or_else(|| {
                    VcError::new(ErrorCode::Internal, "WorkUnit aggregate version overflow")
                })
            }
        }
    }

    pub(crate) fn load_work_unit_aggregate(&self, id: &str) -> VcResult<Option<(String, u64)>> {
        self.conn
            .query_row(
                "SELECT aggregate_json, aggregate_version FROM work_units WHERE id = ?1",
                [id],
                |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64)),
            )
            .optional()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("load work unit: {e}")))
    }

    pub(crate) fn find_work_unit_aggregate(
        &self,
        job_id: &str,
        stage: &str,
        unit_kind: &str,
        unit_index: u32,
        input_hash: &str,
    ) -> VcResult<Option<(String, u64)>> {
        self.conn
            .query_row(
                "SELECT aggregate_json, aggregate_version FROM work_units
                 WHERE job_id = ?1 AND stage = ?2 AND unit_kind = ?3
                   AND unit_index = ?4 AND input_hash = ?5
                 ORDER BY id LIMIT 1",
                params![job_id, stage, unit_kind, unit_index as i64, input_hash],
                |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64)),
            )
            .optional()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("find work unit: {e}")))
    }

    pub(crate) fn list_expired_work_unit_aggregates(
        &self,
        now_rfc3339: &str,
    ) -> VcResult<Vec<(String, u64)>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT aggregate_json, aggregate_version FROM work_units
                 WHERE status = 'running'
                   AND lease_expires_at IS NOT NULL
                   AND lease_expires_at < ?1
                   AND aggregate_json IS NOT NULL
                 ORDER BY unit_index, id",
            )
            .map_err(|e| {
                VcError::new(ErrorCode::Internal, format!("prepare expired units: {e}"))
            })?;
        let rows = statement
            .query_map([now_rfc3339], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("query expired units: {e}")))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("read expired units: {e}")))
    }

    pub(crate) fn lease_next_ready(
        &mut self,
        request: &LeaseRequest,
    ) -> VcResult<Option<(String, u64)>> {
        let tx = self.conn.unchecked_transaction().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("begin lease transaction: {error}"),
            )
        })?;
        let selected: Option<(String, String, u64)> = tx
            .query_row(
                "SELECT id, aggregate_json, aggregate_version FROM work_units
                 WHERE job_id = ?1 AND stage = ?2 AND status = 'pending'
                   AND aggregate_json IS NOT NULL
                 ORDER BY unit_index, id LIMIT 1",
                params![request.job_id, request.stage],
                |row| Ok((row.get(0)?, row.get(1)?, row.get::<_, i64>(2)? as u64)),
            )
            .optional()
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("select ready work unit: {error}"),
                )
            })?;
        let Some((id, aggregate_json, version)) = selected else {
            tx.commit().map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("finish empty lease transaction: {error}"),
                )
            })?;
            return Ok(None);
        };

        let mut unit: videocaptionerr_domain::WorkUnit = serde_json::from_str(&aggregate_json)
            .map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCorrupt,
                    format!("decode ready work unit {id}: {error}"),
                )
            })?;
        unit.lease_for(&request.owner, request.now_ms, request.expires_at_ms)
            .map_err(VcError::from)?;
        let updated_json = serde_json::to_string(&unit).map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode leased work unit: {error}"),
            )
        })?;
        let changed = tx
            .execute(
                "UPDATE work_units SET status = 'running', attempt = ?1,
                 lease_owner = ?2, lease_expires_at = ?3, started_at = ?4,
                 aggregate_json = ?5, aggregate_version = aggregate_version + 1
                 WHERE id = ?6 AND status = 'pending' AND aggregate_version = ?7",
                params![
                    unit.attempt() as i64,
                    request.owner,
                    request.expires_rfc3339,
                    request.now_rfc3339,
                    updated_json,
                    id,
                    version as i64,
                ],
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("persist work unit lease: {error}"),
                )
            })?;
        if changed != 1 {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                "work unit was claimed by another scheduler",
            ));
        }
        tx.commit().map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("commit work unit lease: {error}"),
            )
        })?;
        Ok(Some((updated_json, version.saturating_add(1))))
    }

    /// Retry failed work units from the requested stage onward. Domain
    /// transitions are applied before the control row is updated.
    pub(crate) fn retry_failed(&mut self, job_id: &str, from_stage: Option<&str>) -> VcResult<u32> {
        let tx = self.conn.unchecked_transaction().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("begin retry transaction: {error}"),
            )
        })?;
        let mut statement = tx
            .prepare(
                "SELECT id, stage, aggregate_json FROM work_units
                 WHERE job_id = ?1 AND status IN ('failed', 'cancelled')
                   AND aggregate_json IS NOT NULL ORDER BY unit_index, id",
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("prepare failed units: {error}"),
                )
            })?;
        let rows = statement
            .query_map([job_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|error| {
                VcError::new(ErrorCode::Internal, format!("query failed units: {error}"))
            })?;
        let candidates = rows.collect::<Result<Vec<_>, _>>().map_err(|error| {
            VcError::new(ErrorCode::Internal, format!("read failed units: {error}"))
        })?;
        drop(statement);

        let mut retried = 0u32;
        for (id, stage, aggregate_json) in candidates {
            if from_stage.is_some_and(|start| stage_rank(&stage) < stage_rank(start)) {
                continue;
            }
            let mut unit: videocaptionerr_domain::WorkUnit = serde_json::from_str(&aggregate_json)
                .map_err(|error| {
                    VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode failed work unit {id}: {error}"),
                    )
                })?;
            unit.retry().map_err(VcError::from)?;
            let updated_json = serde_json::to_string(&unit).map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("encode retried work unit: {error}"),
                )
            })?;
            tx.execute(
                "UPDATE work_units SET status = 'pending', attempt = ?1,
                 error_code = NULL, error_json = NULL, artifact_id = NULL,
                 lease_owner = NULL, lease_expires_at = NULL, started_at = NULL,
                 finished_at = NULL, aggregate_json = ?2,
                 aggregate_version = aggregate_version + 1 WHERE id = ?3",
                params![unit.attempt() as i64, updated_json, id],
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("persist retried work unit: {error}"),
                )
            })?;
            retried = retried.saturating_add(1);
        }
        tx.commit().map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("commit retry transaction: {error}"),
            )
        })?;
        Ok(retried)
    }

    pub(crate) fn list_work_units_for_job(
        &self,
        job_id: &str,
    ) -> VcResult<Vec<(String, u64)>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT aggregate_json, aggregate_version FROM work_units
                 WHERE job_id = ?1 AND aggregate_json IS NOT NULL
                 ORDER BY unit_index, id",
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("prepare list work units: {error}"),
                )
            })?;
        let rows = statement
            .query_map([job_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("query list work units: {error}"),
                )
            })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("read list work units: {error}"),
            )
        })
    }

    pub(crate) fn count_retryable(&self, job_id: &str, from_stage: Option<&str>) -> VcResult<u32> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT stage FROM work_units
                 WHERE job_id = ?1 AND status IN ('failed', 'cancelled')
                   AND aggregate_json IS NOT NULL ORDER BY unit_index, id",
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("prepare retryable work units: {error}"),
                )
            })?;
        let rows = statement
            .query_map([job_id], |row| row.get::<_, String>(0))
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("query retryable work units: {error}"),
                )
            })?;
        let mut count = 0u32;
        for row in rows {
            let stage = row.map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("read retryable work unit: {error}"),
                )
            })?;
            if from_stage.is_none_or(|start| stage_rank(&stage) >= stage_rank(start)) {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub fn insert_work_unit(
        &self,
        id: &str,
        job_id: &str,
        stage: &str,
        unit_kind: &str,
        unit_index: i64,
        input_hash: &str,
        status: WorkUnitStatus,
    ) -> VcResult<()> {
        self.conn
            .execute(
                "INSERT INTO work_units (
                    id, job_id, stage, unit_kind, unit_index, input_hash, status, attempt
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0)",
                params![
                    id,
                    job_id,
                    stage,
                    unit_kind,
                    unit_index,
                    input_hash,
                    work_unit_status_name(status)
                ],
            )
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("insert work unit: {e}")))?;
        Ok(())
    }

    #[cfg(test)]
    pub fn get_work_unit_status(&self, id: &str) -> VcResult<Option<WorkUnitStatus>> {
        let s: Option<String> = self
            .conn
            .query_row("SELECT status FROM work_units WHERE id = ?1", [id], |r| {
                r.get(0)
            })
            .optional()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("get work unit: {e}")))?;
        Ok(s.and_then(|x| parse_work_unit_status(&x)))
    }

    /// Expire running leases: return to Pending and increment attempt.
    #[cfg(test)]
    pub fn recover_expired_leases(&self, now_rfc3339: &str) -> VcResult<usize> {
        let n = self
            .conn
            .execute(
                "UPDATE work_units
                 SET status = 'pending',
                     attempt = attempt + 1,
                     lease_owner = NULL,
                     lease_expires_at = NULL,
                     started_at = NULL
                 WHERE status = 'running'
                   AND lease_expires_at IS NOT NULL
                   AND lease_expires_at < ?1",
                [now_rfc3339],
            )
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("recover leases: {e}")))?;
        Ok(n)
    }
}
