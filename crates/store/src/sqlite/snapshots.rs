use super::*;

impl SqliteStore {
    pub(crate) fn save_execution_snapshot(&self, snapshot: &JobExecutionSnapshot) -> VcResult<()> {
        Self::save_execution_snapshot_on(&self.conn, snapshot)
    }

    /// Insert an immutable snapshot on the supplied connection. Keeping the
    /// SQL here lets Batch graph creation use the exact same projection rules
    /// while still committing through one SQLite transaction.
    pub(crate) fn save_execution_snapshot_on(
        conn: &Connection,
        snapshot: &JobExecutionSnapshot,
    ) -> VcResult<()> {
        snapshot
            .validate()
            .map_err(|error| VcError::new(ErrorCode::InvalidArgument, error))?;
        let snapshot_json = serde_json::to_string(snapshot).map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode execution snapshot: {error}"),
            )
        })?;
        let llm_json = snapshot
            .llm
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("encode LLM execution snapshot: {error}"),
                )
            })?;
        let stream_selection = serde_json::to_string(&snapshot.audio_stream).map_err(|error| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("encode audio stream selection: {error}"),
            )
        })?;
        let existing: Option<String> = conn
            .query_row(
                "SELECT snapshot_json FROM execution_snapshots WHERE snapshot_id = ?1",
                [&snapshot.snapshot_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("load existing execution snapshot: {error}"),
                )
            })?;
        if let Some(existing) = existing {
            if existing == snapshot_json {
                return Ok(());
            }
            return Err(VcError::new(
                ErrorCode::StaleResult,
                format!(
                    "execution snapshot {} is immutable and already contains different data",
                    snapshot.snapshot_id
                ),
            ));
        }

        conn.execute(
            "INSERT INTO execution_snapshots (
                    snapshot_id, schema_version, job_id, batch_id, created_at,
                    canonical_source_path, source_size, source_modified_at_ms, job_dir,
                    profile_revision, asr_engine, model_locator, model_id, model_digest,
                    device, compute_type, audio_stream_selection, source_language,
                    target_language, output_path, output_format, output_layout,
                    conflict_policy, fallback_to_source, llm_json, snapshot_json
                 ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                    ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26
                 )",
            params![
                snapshot.snapshot_id.to_string(),
                snapshot.schema_version as i64,
                snapshot.job_id.to_string(),
                snapshot.batch_id.to_string(),
                snapshot.created_at,
                snapshot.canonical_source_path,
                snapshot.source_stat.size as i64,
                snapshot
                    .source_stat
                    .modified_at_ms
                    .map(|value| value as i64),
                snapshot.job_dir,
                snapshot.profile_revision.to_string(),
                snapshot.asr.engine,
                serde_json::to_string(&snapshot.asr.model_locator).map_err(|error| {
                    VcError::new(
                        ErrorCode::ArtifactCommitFailed,
                        format!("encode model locator: {error}"),
                    )
                })?,
                snapshot.asr.model_id,
                snapshot.asr.model_digest,
                snapshot.asr.device,
                snapshot.asr.compute_type,
                stream_selection,
                snapshot.source_language,
                snapshot.target_language,
                snapshot.output.path,
                snapshot.output.format,
                snapshot.output.layout,
                snapshot.output.conflict_policy,
                snapshot.output.fallback_to_source,
                llm_json,
                snapshot_json,
            ],
        )
        .map_err(|error| {
            if is_constraint(&error) {
                VcError::new(
                    ErrorCode::StaleResult,
                    format!("execution snapshot {} already exists", snapshot.snapshot_id),
                )
            } else {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("save execution snapshot: {error}"),
                )
            }
        })?;
        Ok(())
    }

    pub(crate) fn load_execution_snapshot(&self, id: &str) -> VcResult<Option<String>> {
        self.conn
            .query_row(
                "SELECT snapshot_json FROM execution_snapshots WHERE snapshot_id = ?1",
                [id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("load execution snapshot: {error}"),
                )
            })
    }

    pub(crate) fn load_snapshots_for_batch(&self, batch_id: &str) -> VcResult<Vec<String>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT snapshot_json FROM execution_snapshots
                 WHERE batch_id = ?1 ORDER BY job_id",
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("prepare batch execution snapshots: {error}"),
                )
            })?;
        let rows = statement
            .query_map([batch_id], |row| row.get::<_, String>(0))
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("query batch execution snapshots: {error}"),
                )
            })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("read batch execution snapshots: {error}"),
            )
        })
    }
}
