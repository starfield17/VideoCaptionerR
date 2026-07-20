//! Application use cases.

pub mod chunk_plan;
pub mod llm_pipeline;
pub mod maintenance;
pub mod run_batch;
pub mod startup_recovery;
pub mod transcribe_job;
pub mod transcript_editor;

pub use chunk_plan::PersistChunkPlan;
pub use llm_pipeline::{LlmPipeline, LlmPipelineRequest, LlmPipelineResult, LlmPlan, LlmPlanEntry};
pub use maintenance::{
    CacheGc, LeaseNextWorkUnitCommand, RetryFailedWorkUnits, RetryFailedWorkUnitsCommand,
    RetryFailedWorkUnitsResponse, WorkUnitScheduler,
};
pub use run_batch::{RunBatch, RunBatchCommand, RunBatchFailure, RunBatchResponse};
pub use startup_recovery::{RecoveryReport, StartupRecovery};
pub use transcribe_job::{
    LlmProcessOptions, TranscribeJob, TranscribeJobCommand, TranscribeJobResponse,
};
pub use transcript_editor::{EditTranscriptCommand, EditTranscriptResponse, TranscriptEditor};
