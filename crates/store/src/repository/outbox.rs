use super::*;

#[async_trait]
impl OutboxRepository for StoreHandle {
    async fn list_pending(
        &self,
        limit: u32,
    ) -> AppResult<Vec<videocaptionerr_core::ports::StoredOutboxEvent>> {
        self.list_pending_outbox(limit)
            .await
            .map_err(ApplicationError::Adapter)
    }

    async fn mark_delivered(
        &self,
        id: &videocaptionerr_domain::UlidStr,
        delivered_at: &str,
    ) -> AppResult<()> {
        self.mark_outbox_delivered(id.as_str(), delivered_at)
            .await
            .map_err(ApplicationError::Adapter)
    }
}

#[async_trait]
impl EventPublisher for StoreHandle {
    async fn publish(&self, event: DomainEvent) -> AppResult<()> {
        let (aggregate_id, event_type) = match &event {
            DomainEvent::BatchReachedTerminal { batch_id, .. } => {
                (batch_id.to_string(), "batch_reached_terminal")
            }
        };
        let payload_json = serde_json::to_string(&event).map_err(|error| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::Internal,
                format!("encode domain event: {error}"),
            ))
        })?;
        self.append_outbox(OutboxEvent {
            aggregate_type: "Batch".into(),
            aggregate_id,
            event_type: event_type.into(),
            payload_json,
            created_at: Utc::now().to_rfc3339(),
        })
        .await
        .map_err(ApplicationError::Adapter)
    }
}
