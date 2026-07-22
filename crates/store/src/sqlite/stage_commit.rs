use super::*;

impl SqliteStore {
    pub(crate) fn commit_stage(
        &mut self,
        request: videocaptionerr_core::ports::StageCommitRequest,
    ) -> VcResult<videocaptionerr_core::ports::StageCommitResult> {
        let fault = self.fault.take();
        let published = request
            .artifact
            .as_ref()
            .map(|artifact| publish_prepared_artifact_with_fault(artifact, fault))
            .transpose()?;
        let result = self.commit_stage_transaction(&request, fault);
        if result.is_err() && published == Some(true) && fault.is_none() {
            if let Some(artifact) = &request.artifact {
                let path = Path::new(&artifact.artifact.path);
                if let Err(error) = fs::remove_file(path) {
                    if error.kind() != std::io::ErrorKind::NotFound {
                        tracing::warn!(
                            path = %path.display(),
                            error = %error,
                            "published artifact cleanup failed after transaction rollback"
                        );
                    }
                }
                sync_parent(path);
            }
        }
        result
    }

    fn commit_stage_transaction(
        &mut self,
        request: &videocaptionerr_core::ports::StageCommitRequest,
        fault: Option<StageCommitFaultPoint>,
    ) -> VcResult<videocaptionerr_core::ports::StageCommitResult> {
        let tx = self.conn.unchecked_transaction().map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("begin stage commit transaction: {error}"),
            )
        })?;

        if let Some((job, expected)) = &request.job {
            if matches!(expected, ExpectedVersion::New) {
                insert_job_tx(&tx, job)?;
            }
        }
        if let Some((unit, expected)) = &request.work_unit {
            if matches!(expected, ExpectedVersion::New) {
                insert_work_unit_tx(&tx, unit)?;
            }
        }

        if let Some(artifact) = &request.artifact {
            let meta = artifact_meta_for(artifact);
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
            .map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("insert stage artifact: {error}"),
                )
            })?;
            stage_fault_at(fault, StageCommitFaultPoint::AfterArtifactInsert)?;
        }

        if let Some((unit, expected)) = &request.work_unit {
            if let Some(artifact) = &request.artifact {
                if unit.value.job_id() != &artifact.job_id {
                    return Err(VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        "work unit and artifact belong to different Jobs",
                    ));
                }
            }
            if !matches!(expected, ExpectedVersion::New) {
                update_work_unit_tx(&tx, unit, *expected)?;
            }
            stage_fault_at(fault, StageCommitFaultPoint::AfterWorkUnitUpdate)?;
        }

        if let Some((job, expected)) = &request.job {
            if let Some(artifact) = &request.artifact {
                if job.value.id() != &artifact.job_id {
                    return Err(VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        "Job and artifact belong to different Jobs",
                    ));
                }
            }
            if !matches!(expected, ExpectedVersion::New) {
                update_job_tx(&tx, job, *expected)?;
            }
            sync_stage_projection(&tx, &job.value)?;
            stage_fault_at(fault, StageCommitFaultPoint::AfterJobUpdate)?;
        }

        if let Some(event) = &request.event {
            insert_outbox_tx(&tx, event)?;
            stage_fault_at(fault, StageCommitFaultPoint::AfterOutboxInsert)?;
        }

        tx.commit().map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("commit stage transaction: {error}"),
            )
        })?;
        stage_fault_at(fault, StageCommitFaultPoint::AfterDbCommit)?;

        Ok(videocaptionerr_core::ports::StageCommitResult {
            job: request.job.as_ref().map(|(job, expected)| {
                let version = next_version(job.version, *expected);
                videocaptionerr_core::ports::Versioned::with_version(job.value.clone(), version)
            }),
            work_unit: request.work_unit.as_ref().map(|(unit, expected)| {
                let version = next_version(unit.version, *expected);
                videocaptionerr_core::ports::Versioned::with_version(unit.value.clone(), version)
            }),
        })
    }
}

fn insert_job_tx(
    tx: &rusqlite::Transaction<'_>,
    job: &videocaptionerr_core::ports::Versioned<videocaptionerr_domain::Job>,
) -> VcResult<()> {
    let now = chrono::Utc::now().to_rfc3339();
    let projection = snapshot_projection(tx, job.value.execution_snapshot_id())?;
    let source_path = projection
        .as_ref()
        .map(|value| value.0.as_str())
        .unwrap_or(job.value.source_path());
    let job_dir = projection
        .as_ref()
        .map(|value| value.1.as_str())
        .unwrap_or("");
    let profile_revision = projection
        .as_ref()
        .map(|value| value.2.as_str())
        .unwrap_or(job.value.profile_revision().as_str());
    let aggregate_json = serde_json::to_string(&job.value).map_err(|error| {
        VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("encode Job aggregate: {error}"),
        )
    })?;
    tx.execute(
        "INSERT INTO jobs (
            id, batch_id, status, source_path, job_dir, profile_revision,
            execution_snapshot_id, aggregate_json, aggregate_version,
            created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1, ?9, ?9)",
        params![
            job.value.id().as_str(),
            job.value.batch_id().map(|id| id.as_str()),
            job_status_name(job.value.status()),
            source_path,
            job_dir,
            profile_revision,
            job.value.execution_snapshot_id().map(|id| id.as_str()),
            aggregate_json,
            now,
        ],
    )
    .map_err(|error| {
        if is_constraint(&error) {
            stale_result("Job", job.value.id().as_str(), ExpectedVersion::New)
        } else {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("insert Job aggregate: {error}"),
            )
        }
    })?;
    Ok(())
}

fn update_job_tx(
    tx: &rusqlite::Transaction<'_>,
    job: &videocaptionerr_core::ports::Versioned<videocaptionerr_domain::Job>,
    expected: ExpectedVersion,
) -> VcResult<()> {
    let ExpectedVersion::Exact(version) = expected else {
        return Ok(());
    };
    let projection = snapshot_projection(tx, job.value.execution_snapshot_id())?;
    let source_path = projection
        .as_ref()
        .map(|value| value.0.as_str())
        .unwrap_or(job.value.source_path());
    let job_dir = projection
        .as_ref()
        .map(|value| value.1.as_str())
        .unwrap_or("");
    let profile_revision = projection
        .as_ref()
        .map(|value| value.2.as_str())
        .unwrap_or(job.value.profile_revision().as_str());
    let aggregate_json = serde_json::to_string(&job.value).map_err(|error| {
        VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("encode Job aggregate: {error}"),
        )
    })?;
    let changed = tx
        .execute(
            "UPDATE jobs SET
                batch_id = ?1, status = ?2, source_path = ?3, job_dir = ?4,
                profile_revision = ?5, execution_snapshot_id = ?6,
                aggregate_json = ?7, aggregate_version = aggregate_version + 1,
                updated_at = ?8
             WHERE id = ?9 AND aggregate_version = ?10",
            params![
                job.value.batch_id().map(|id| id.as_str()),
                job_status_name(job.value.status()),
                source_path,
                job_dir,
                profile_revision,
                job.value.execution_snapshot_id().map(|id| id.as_str()),
                aggregate_json,
                chrono::Utc::now().to_rfc3339(),
                job.value.id().as_str(),
                version as i64,
            ],
        )
        .map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("update Job aggregate: {error}"),
            )
        })?;
    if changed != 1 {
        return Err(stale_result("Job", job.value.id().as_str(), expected));
    }
    Ok(())
}

fn insert_work_unit_tx(
    tx: &rusqlite::Transaction<'_>,
    unit: &videocaptionerr_core::ports::Versioned<videocaptionerr_domain::WorkUnit>,
) -> VcResult<()> {
    let json = serde_json::to_string(&unit.value).map_err(|error| {
        VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("encode WorkUnit: {error}"),
        )
    })?;
    tx.execute(
        "INSERT INTO work_units (
            id, job_id, stage, unit_kind, unit_index, input_hash, status, attempt,
            artifact_id, lease_owner, lease_expires_at, aggregate_json, aggregate_version
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 1)",
        params![
            unit.value.id().as_str(),
            unit.value.job_id().as_str(),
            unit.value.stage().as_str(),
            unit.value.unit_kind(),
            unit.value.unit_index() as i64,
            unit.value.input_hash(),
            work_unit_status_name(unit.value.status()),
            unit.value.attempt() as i64,
            unit.value.artifact().map(|artifact| artifact.id.as_str()),
            unit.value.lease().map(|lease| lease.owner.as_str()),
            unit.value.lease().and_then(|lease| {
                chrono::DateTime::<chrono::Utc>::from_timestamp_millis(lease.expires_at_ms as i64)
                    .map(|value| value.to_rfc3339())
            }),
            json,
        ],
    )
    .map_err(|error| {
        if is_constraint(&error) {
            stale_result("WorkUnit", unit.value.id().as_str(), ExpectedVersion::New)
        } else {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("insert WorkUnit: {error}"),
            )
        }
    })?;
    Ok(())
}

fn update_work_unit_tx(
    tx: &rusqlite::Transaction<'_>,
    unit: &videocaptionerr_core::ports::Versioned<videocaptionerr_domain::WorkUnit>,
    expected: ExpectedVersion,
) -> VcResult<()> {
    let ExpectedVersion::Exact(version) = expected else {
        return Ok(());
    };
    let json = serde_json::to_string(&unit.value).map_err(|error| {
        VcError::new(
            ErrorCode::ArtifactCommitFailed,
            format!("encode WorkUnit: {error}"),
        )
    })?;
    let changed = tx
        .execute(
            "UPDATE work_units SET
                job_id = ?1, stage = ?2, unit_kind = ?3, unit_index = ?4,
                input_hash = ?5, status = ?6, attempt = ?7, artifact_id = ?8,
                lease_owner = ?9, lease_expires_at = ?10, aggregate_json = ?11,
                finished_at = CASE WHEN ?6 IN ('done', 'failed', 'cancelled')
                                   THEN ?12 ELSE finished_at END,
                aggregate_version = aggregate_version + 1
             WHERE id = ?13 AND aggregate_version = ?14",
            params![
                unit.value.job_id().as_str(),
                unit.value.stage().as_str(),
                unit.value.unit_kind(),
                unit.value.unit_index() as i64,
                unit.value.input_hash(),
                work_unit_status_name(unit.value.status()),
                unit.value.attempt() as i64,
                unit.value.artifact().map(|artifact| artifact.id.as_str()),
                unit.value.lease().map(|lease| lease.owner.as_str()),
                unit.value.lease().and_then(|lease| {
                    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
                        lease.expires_at_ms as i64,
                    )
                    .map(|value| value.to_rfc3339())
                }),
                json,
                chrono::Utc::now().to_rfc3339(),
                unit.value.id().as_str(),
                version as i64,
            ],
        )
        .map_err(|error| VcError::new(ErrorCode::Internal, format!("update WorkUnit: {error}")))?;
    if changed != 1 {
        return Err(stale_result("WorkUnit", unit.value.id().as_str(), expected));
    }
    Ok(())
}

pub(crate) fn sync_stage_projection(
    tx: &rusqlite::Transaction<'_>,
    job: &videocaptionerr_domain::Job,
) -> VcResult<()> {
    for stage in job.stages() {
        tx.execute(
            "INSERT INTO stages (id, job_id, stage, status, attempt)
             VALUES (?1, ?2, ?3, ?4, 0)
             ON CONFLICT(job_id, stage) DO UPDATE SET status = excluded.status",
            params![
                format!("{}:{}", job.id(), stage.kind.as_str()),
                job.id().as_str(),
                stage.kind.as_str(),
                stage_status_name(stage.status),
            ],
        )
        .map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("update stage projection: {error}"),
            )
        })?;
    }
    Ok(())
}

pub(crate) fn insert_outbox_tx(
    tx: &rusqlite::Transaction<'_>,
    event: &videocaptionerr_core::ports::OutboxEvent,
) -> VcResult<()> {
    let sequence: i64 = tx
        .query_row(
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM outbox_events
             WHERE aggregate_type = ?1 AND aggregate_id = ?2",
            params![event.aggregate_type, event.aggregate_id],
            |row| row.get(0),
        )
        .map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("next outbox sequence: {error}"),
            )
        })?;
    let id = UlidStr::from(Ulid::new()).into_string();
    tx.execute(
        "INSERT INTO outbox_events (
            id, aggregate_type, aggregate_id, sequence, event_type,
            payload_json, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            id,
            event.aggregate_type,
            event.aggregate_id,
            sequence,
            event.event_type,
            event.payload_json,
            event.created_at,
        ],
    )
    .map_err(|error| VcError::new(ErrorCode::Internal, format!("insert outbox event: {error}")))?;
    Ok(())
}

impl SqliteStore {
    pub(crate) fn apply_retry_transaction(
        &mut self,
        request: videocaptionerr_core::ports::RetryTransactionRequest,
    ) -> VcResult<videocaptionerr_core::ports::RetryTransactionResult> {
        let tx = self.conn.unchecked_transaction().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("begin retry transaction: {error}"),
            )
        })?;

        let (job, job_expected) = &request.job;
        update_job_tx(&tx, job, *job_expected)?;
        sync_stage_projection(&tx, &job.value)?;

        let mut work_units = Vec::with_capacity(request.work_units.len());
        for (unit, expected) in &request.work_units {
            update_work_unit_tx(&tx, unit, *expected)?;
            work_units.push(videocaptionerr_core::ports::Versioned::with_version(
                unit.value.clone(),
                next_version(unit.version, *expected),
            ));
        }

        let batch = if let Some((batch, expected)) = &request.batch {
            update_batch_tx(&tx, batch, *expected)?;
            Some(videocaptionerr_core::ports::Versioned::with_version(
                batch.value.clone(),
                next_version(batch.version, *expected),
            ))
        } else {
            None
        };

        insert_outbox_tx(&tx, &request.event)?;

        tx.commit().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("commit retry transaction: {error}"),
            )
        })?;

        Ok(videocaptionerr_core::ports::RetryTransactionResult {
            batch,
            job: videocaptionerr_core::ports::Versioned::with_version(
                job.value.clone(),
                next_version(job.version, *job_expected),
            ),
            work_units,
        })
    }
}

fn update_batch_tx(
    tx: &rusqlite::Transaction<'_>,
    batch: &videocaptionerr_core::ports::Versioned<videocaptionerr_domain::Batch>,
    expected: ExpectedVersion,
) -> VcResult<()> {
    let ExpectedVersion::Exact(version) = expected else {
        return Err(VcError::new(
            ErrorCode::InvalidArgument,
            "retry Batch update requires an exact CAS version",
        ));
    };
    let aggregate_json = serde_json::to_string(&batch.value).map_err(|error| {
        VcError::new(
            ErrorCode::Internal,
            format!("encode Batch aggregate: {error}"),
        )
    })?;
    let profile = batch.value.execution_profile();
    let changed = tx
        .execute(
            "UPDATE batches SET
                status = ?1, asr_model_id = ?2, asr_device = ?3,
                aggregate_json = ?4, aggregate_version = aggregate_version + 1,
                updated_at = ?5
             WHERE id = ?6 AND aggregate_version = ?7",
            params![
                match batch.value.status() {
                    videocaptionerr_domain::BatchStatus::Pending => "pending",
                    videocaptionerr_domain::BatchStatus::Running => "running",
                    videocaptionerr_domain::BatchStatus::Paused => "paused",
                    videocaptionerr_domain::BatchStatus::Done => "done",
                    videocaptionerr_domain::BatchStatus::Failed => "failed",
                    videocaptionerr_domain::BatchStatus::Cancelled => "cancelled",
                },
                profile.asr_model.as_str(),
                profile.device.as_str(),
                aggregate_json,
                chrono::Utc::now().to_rfc3339(),
                batch.value.id().as_str(),
                version as i64,
            ],
        )
        .map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("update Batch aggregate: {error}"),
            )
        })?;
    if changed != 1 {
        return Err(stale_result("Batch", batch.value.id().as_str(), expected));
    }
    Ok(())
}
