//! Application-owned LLM stages.
//!
//! This module owns packing, validation, retries, binary isolation and stale
//! result handling. Providers only transport an already-shaped request.

mod agent;
mod correct;
pub mod durable;
mod execute;
mod packing;
mod plan;
mod retry;
mod service;
mod split;
mod translate;
mod types;
mod validation;

#[cfg(test)]
mod tests;

pub use durable::LlmDurableContext;
pub use types::{LlmPipeline, LlmPipelineRequest, LlmPipelineResult, LlmPlan, LlmPlanEntry};

// Test-visible helpers (same as the former monolithic module scope).
#[cfg(test)]
pub(crate) use validation::{data_prompt, is_original_residue};
