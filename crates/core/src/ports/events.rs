use async_trait::async_trait;
use videocaptionerr_domain::DomainEvent;

use crate::application_error::AppResult;

#[async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(&self, event: DomainEvent) -> AppResult<()>;
}
