use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use videocaptionerr_domain::{DomainEvent, JobId, UlidStr};

use crate::application_error::AppResult;

/// Live application events (progress, segments, logs). Delivery failure must
/// never rewrite already-committed business state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApplicationEvent {
    Progress {
        job_id: Option<JobId>,
        processed_ms: Option<u64>,
        total_ms: Option<u64>,
        message: Option<String>,
    },
    Language {
        job_id: Option<JobId>,
        language: String,
    },
    Segment {
        job_id: Option<JobId>,
        text: String,
        start_ms: u64,
        end_ms: u64,
    },
    Log {
        job_id: Option<JobId>,
        level: String,
        message: String,
    },
    Heartbeat {
        job_id: Option<JobId>,
    },
    Domain(DomainEvent),
}

#[async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(&self, event: DomainEvent) -> AppResult<()>;

    /// Best-effort live delivery. Default ignores live events so existing
    /// outbox-only publishers remain valid. Failures must not be treated as
    /// business failures by callers.
    async fn publish_live(&self, _event: ApplicationEvent) -> AppResult<()> {
        Ok(())
    }
}

/// Optional subscription surface for CLI JSON / Tauri. Implementations may
/// buffer or drop under backpressure without affecting the outbox authority.
#[async_trait]
pub trait LiveEventSink: Send + Sync {
    async fn emit(&self, event: ApplicationEvent) -> AppResult<()>;
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
