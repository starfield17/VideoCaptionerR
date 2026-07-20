use super::*;

#[async_trait]
impl CapabilityProbeStore for StoreHandle {
    async fn load(
        &self,
        provider_profile_id: &str,
        model: &str,
        probe_hash: &str,
    ) -> AppResult<Option<String>> {
        self.load_capability_probe(provider_profile_id, model, probe_hash)
            .await
            .map_err(ApplicationError::Adapter)
    }

    async fn save(&self, record: CapabilityProbeRecord) -> AppResult<()> {
        self.save_capability_probe(record)
            .await
            .map_err(ApplicationError::Adapter)
    }
}
