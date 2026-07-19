use std::sync::Arc;

use videocaptionerr_domain::{ArtifactRef, JobId, StageKind};

use crate::application_error::AppResult;
use crate::chunking::ChunkPlan;
use crate::ports::{ChunkPlanCommit, ChunkPlanStore, IdGenerator};

pub struct PersistChunkPlan {
    store: Arc<dyn ChunkPlanStore>,
    ids: Arc<dyn IdGenerator>,
}

impl PersistChunkPlan {
    pub fn new(store: Arc<dyn ChunkPlanStore>, ids: Arc<dyn IdGenerator>) -> Self {
        Self { store, ids }
    }

    pub async fn execute(
        &self,
        job_id: JobId,
        path: std::path::PathBuf,
        plan: ChunkPlan,
    ) -> AppResult<ArtifactRef> {
        self.store
            .commit(ChunkPlanCommit {
                job_id,
                artifact_id: self.ids.next_id(),
                path,
                plan,
                producer_fingerprint: "rust-chunk-planner".into(),
            })
            .await
    }
}

pub fn chunk_plan_stage() -> StageKind {
    StageKind::Asr
}
