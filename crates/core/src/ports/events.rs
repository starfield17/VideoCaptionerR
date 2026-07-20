use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use videocaptionerr_domain::{DomainEvent, UlidStr};

use crate::application_error::AppResult;

#[async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(&self, event: DomainEvent) -> AppResult<()>;
}

/// Event data supplied to a repository commit. The store assigns the event
/// id and per-aggregate sequence inside the same SQLite transaction as the
/// state change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxEvent {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredOutboxEvent {
    pub id: UlidStr,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub sequence: u64,
    pub event_type: String,
    pub payload_json: String,
    pub created_at: String,
    pub delivered_at: Option<String>,
}

#[async_trait]
pub trait OutboxRepository: Send + Sync {
    async fn list_pending(&self, limit: u32) -> AppResult<Vec<StoredOutboxEvent>>;
    async fn mark_delivered(&self, id: &UlidStr, delivered_at: &str) -> AppResult<()>;
}
