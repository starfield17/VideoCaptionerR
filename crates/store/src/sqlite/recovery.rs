use super::*;

impl SqliteStore {
    pub(crate) fn recover_artifacts(
        &mut self,
        roots: &[PathBuf],
    ) -> VcResult<ArtifactRecoveryReport> {
        let mut report = ArtifactRecoveryReport::default();
        let mut referenced = HashSet::new();
        let mut statement = self
            .conn
            .prepare(
                "SELECT id, job_id, stage, path, content_hash, committed
                 FROM artifacts ORDER BY id",
            )
            .map_err(|error| {
                VcError::new(ErrorCode::Internal, format!("prepare artifacts: {error}"))
            })?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })
            .map_err(|error| {
                VcError::new(ErrorCode::Internal, format!("query artifacts: {error}"))
            })?;
        let artifact_rows = rows.collect::<Result<Vec<_>, _>>().map_err(|error| {
            VcError::new(ErrorCode::Internal, format!("read artifacts: {error}"))
        })?;
        drop(statement);

        let tx = self.conn.unchecked_transaction().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("begin artifact recovery: {error}"),
            )
        })?;
        for (id, job_id, stage, path, expected_hash, committed) in &artifact_rows {
            if *committed == 1 {
                referenced.insert(path.clone());
                let valid = Path::new(path).is_file()
                    && blake3_file(Path::new(path)).ok().as_deref() == Some(expected_hash);
                if !valid {
                    report.corrupt_artifacts.push(id.clone());
                    invalidate_artifact_references(&tx, id, job_id, stage)?;
                    tx.execute("UPDATE artifacts SET committed = 0 WHERE id = ?1", [id])
                        .map_err(|error| {
                            VcError::new(
                                ErrorCode::Internal,
                                format!("mark corrupt artifact {id}: {error}"),
                            )
                        })?;
                }
            }
        }
        tx.commit().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("commit artifact recovery: {error}"),
            )
        })?;

        for root in roots {
            let mut files = Vec::new();
            collect_files(root, &mut files)?;
            for path in files {
                if is_recovery_path(&path) {
                    continue;
                }
                let name = path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("");
                if name.ends_with(".partial") || name.ends_with(".tmp") || name.contains(".tmp.") {
                    quarantine_file(root, &path)?;
                    report.partial_files.push(path);
                } else if !referenced.contains(&path.to_string_lossy().into_owned()) {
                    quarantine_file(root, &path)?;
                    report.orphan_files.push(path);
                }
            }
        }
        Ok(report)
    }
}

fn invalidate_artifact_references(
    tx: &rusqlite::Transaction<'_>,
    artifact_id: &str,
    job_id: &str,
    stage_name: &str,
) -> VcResult<()> {
    if let Some(stage) = videocaptionerr_domain::StageKind::parse(stage_name) {
        let job_row: Option<(Option<String>, i64)> = tx
            .query_row(
                "SELECT aggregate_json, aggregate_version FROM jobs WHERE id = ?1",
                [job_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| {
                VcError::new(ErrorCode::Internal, format!("load corrupt Job: {error}"))
            })?;
        match job_row {
            Some((Some(body), _version)) => {
                let mut job: videocaptionerr_domain::Job =
                    serde_json::from_str(&body).map_err(|error| {
                        VcError::new(
                            ErrorCode::ArtifactCorrupt,
                            format!("decode corrupt Job: {error}"),
                        )
                    })?;
                job.invalidate_stage_for_recovery(stage)
                    .map_err(VcError::from)?;
                let body = serde_json::to_string(&job).map_err(|error| {
                    VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("encode recovered Job: {error}"),
                    )
                })?;
                tx.execute(
                    "UPDATE jobs SET status = ?1, aggregate_json = ?2,
                     aggregate_version = aggregate_version + 1, updated_at = ?3
                     WHERE id = ?4",
                    params![
                        job_status_name(job.status()),
                        body,
                        chrono::Utc::now().to_rfc3339(),
                        job_id,
                    ],
                )
                .map_err(|error| {
                    VcError::new(ErrorCode::Internal, format!("recover Job: {error}"))
                })?;
                sync_stage_projection(tx, &job)?;
            }
            Some((None, _version)) => {
                tx.execute(
                    "UPDATE jobs SET status = 'pending', aggregate_version = aggregate_version + 1,
                     updated_at = ?1 WHERE id = ?2",
                    params![chrono::Utc::now().to_rfc3339(), job_id],
                )
                .map_err(|error| {
                    VcError::new(ErrorCode::Internal, format!("recover legacy Job: {error}"))
                })?;
            }
            None => {}
        }
    }

    let mut statement = tx
        .prepare("SELECT id, aggregate_json FROM work_units WHERE artifact_id = ?1")
        .map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("prepare corrupt WorkUnits: {error}"),
            )
        })?;
    let rows = statement
        .query_map([artifact_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("query corrupt WorkUnits: {error}"),
            )
        })?;
    let units = rows.collect::<Result<Vec<_>, _>>().map_err(|error| {
        VcError::new(
            ErrorCode::Internal,
            format!("read corrupt WorkUnits: {error}"),
        )
    })?;
    drop(statement);
    for (unit_id, body) in units {
        if let Some(body) = body {
            let mut unit: videocaptionerr_domain::WorkUnit =
                serde_json::from_str(&body).map_err(|error| {
                    VcError::new(
                        ErrorCode::ArtifactCorrupt,
                        format!("decode corrupt WorkUnit: {error}"),
                    )
                })?;
            unit.invalidate_artifact_for_recovery("ARTIFACT_CORRUPT")
                .map_err(VcError::from)?;
            let body = serde_json::to_string(&unit).map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("encode recovered WorkUnit: {error}"),
                )
            })?;
            tx.execute(
                "UPDATE work_units SET status = 'pending', artifact_id = NULL,
                 error_code = 'ARTIFACT_CORRUPT', aggregate_json = ?1,
                 aggregate_version = aggregate_version + 1 WHERE id = ?2",
                params![body, unit_id],
            )
        } else {
            tx.execute(
                "UPDATE work_units SET status = 'pending', artifact_id = NULL,
                 error_code = 'ARTIFACT_CORRUPT', aggregate_version = aggregate_version + 1
                 WHERE id = ?1",
                [unit_id],
            )
        }
        .map_err(|error| VcError::new(ErrorCode::Internal, format!("recover WorkUnit: {error}")))?;
    }
    Ok(())
}

fn collect_files(root: &Path, files: &mut Vec<PathBuf>) -> VcResult<()> {
    if !root.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(root).map_err(|error| {
        VcError::new(
            ErrorCode::Internal,
            format!("scan recovery root {}: {error}", root.display()),
        )
    })? {
        let path = entry
            .map_err(|error| {
                VcError::new(ErrorCode::Internal, format!("read recovery entry: {error}"))
            })?
            .path();
        if path.file_name().and_then(|value| value.to_str()) == Some(".recovery-quarantine") {
            continue;
        }
        if path.is_dir() {
            collect_files(&path, files)?;
        } else if path.is_file() {
            files.push(path);
        }
    }
    Ok(())
}

fn is_recovery_path(path: &Path) -> bool {
    path.components()
        .any(|component| component.as_os_str() == ".recovery-quarantine")
}

fn quarantine_file(root: &Path, path: &Path) -> VcResult<()> {
    let quarantine = root.join(".recovery-quarantine");
    fs::create_dir_all(&quarantine).map_err(|error| {
        VcError::new(
            ErrorCode::Internal,
            format!("create recovery quarantine: {error}"),
        )
    })?;
    let name = path.file_name().ok_or_else(|| {
        VcError::new(
            ErrorCode::Internal,
            format!("recovery path has no filename: {}", path.display()),
        )
    })?;
    let mut destination = quarantine.join(name);
    if destination.exists() {
        destination = quarantine.join(format!("{}.{}", name.to_string_lossy(), Ulid::new()));
    }
    fs::rename(path, &destination).map_err(|error| {
        VcError::new(
            ErrorCode::Internal,
            format!(
                "quarantine {} -> {}: {error}",
                path.display(),
                destination.display()
            ),
        )
    })?;
    Ok(())
}
