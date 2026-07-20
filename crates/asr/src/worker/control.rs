//! Cloneable cooperative-cancel / ping control path.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::process::ChildStdin;
use tokio::sync::Mutex as AsyncMutex;
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_contracts::protocol::ProtocolMessageType;

use super::process::send_to_worker;

/// Control path that can be cloned before a long transcription starts.
/// It shares only the worker stdin and sequence allocator; stdout remains
/// owned by the main client reader so terminal messages stay ordered.
#[derive(Clone)]
pub struct WorkerControl {
    pub(super) stdin: Arc<AsyncMutex<ChildStdin>>,
    pub(super) session_id: String,
    pub(super) next_seq: Arc<AtomicU64>,
    pub(super) current_request_id: Arc<AtomicU64>,
}

impl WorkerControl {
    async fn send(
        &self,
        request_id: Option<u64>,
        msg_type: ProtocolMessageType,
        data: Option<serde_json::Value>,
    ) -> VcResult<()> {
        send_to_worker(
            &self.stdin,
            &self.session_id,
            &self.next_seq,
            request_id,
            msg_type,
            data,
        )
        .await
    }

    pub async fn cancel_current(&self) -> VcResult<()> {
        let target = self.current_request_id.load(Ordering::SeqCst);
        if target == 0 {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                "worker has no active transcription",
            ));
        }
        self.cancel(target).await
    }

    pub async fn cancel(&self, target_request_id: u64) -> VcResult<()> {
        self.send(
            None,
            ProtocolMessageType::Cancel,
            Some(serde_json::json!({"target_request_id": target_request_id})),
        )
        .await
    }

    /// Send a heartbeat while inference is running. The response is consumed
    /// by the main reader and remains subject to the normal protocol checks.
    pub async fn ping(&self) -> VcResult<()> {
        self.send(None, ProtocolMessageType::Ping, None).await
    }
}
