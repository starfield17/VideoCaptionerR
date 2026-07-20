use super::*;

impl SqliteStore {
    pub(crate) fn load_capability_probe(
        &self,
        provider_profile_id: &str,
        model: &str,
        probe_hash: &str,
    ) -> VcResult<Option<String>> {
        self.conn
            .query_row(
                "SELECT result_json FROM llm_capability_probes
                 WHERE provider_profile_id = ?1 AND model = ?2 AND probe_hash = ?3",
                params![provider_profile_id, model, probe_hash],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("load LLM capability probe: {error}"),
                )
            })
    }

    pub(crate) fn save_capability_probe(&mut self, record: &CapabilityProbeRecord) -> VcResult<()> {
        self.conn
            .execute(
                "INSERT INTO llm_capability_probes (
                    id, provider_profile_id, model, probe_hash, result_json,
                    created_at, expires_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(provider_profile_id, model, probe_hash) DO UPDATE SET
                    result_json = excluded.result_json,
                    created_at = excluded.created_at,
                    expires_at = excluded.expires_at",
                params![
                    record.id,
                    record.provider_profile_id,
                    record.model,
                    record.probe_hash,
                    record.result_json,
                    record.created_at,
                    record.expires_at,
                ],
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("save LLM capability probe: {error}"),
                )
            })?;
        Ok(())
    }
}
