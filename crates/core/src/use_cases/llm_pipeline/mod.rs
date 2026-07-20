//! Application-owned LLM stages.
//!
//! This module owns packing, validation, retries, binary isolation and stale
//! result handling. Providers only transport an already-shaped request.

mod types;
mod plan;
mod packing;
mod validation;
mod split;
mod retry;
mod service;
mod execute;
mod correct;
mod translate;
pub mod durable;

#[cfg(test)]
mod tests;

pub use durable::LlmDurableContext;
pub use types::{LlmPipeline, LlmPipelineRequest, LlmPipelineResult, LlmPlan, LlmPlanEntry};

// Test-visible helpers (same as the former monolithic module scope).
#[cfg(test)]
pub(crate) use validation::{data_prompt, is_original_residue};
