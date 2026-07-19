use async_trait::async_trait;

use crate::application_error::AppResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheGcResult {
    pub before_bytes: u64,
    pub after_bytes: u64,
    pub deleted_entries: u32,
    pub skipped_leased: u32,
}

#[async_trait]
pub trait CacheRepository: Send + Sync {
    async fn gc(&self, max_bytes: u64) -> AppResult<CacheGcResult>;
    async fn read(&self, key: &str) -> AppResult<Option<Vec<u8>>>;
    async fn write(&self, key: &str, bytes: &[u8]) -> AppResult<()>;
}
