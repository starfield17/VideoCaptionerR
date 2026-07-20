use super::BatchStatus;
use crate::identity::BatchId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DomainEvent {
    BatchReachedTerminal {
        batch_id: BatchId,
        status: BatchStatus,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobTerminalStatus {
    Done,
    DoneDegraded,
    Failed,
    Cancelled,
}
