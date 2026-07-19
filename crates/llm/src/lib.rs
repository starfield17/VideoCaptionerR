//! LLM provider traits and shared types.
//!
//! This crate MUST NOT own Job scheduling or persistence policy.

pub mod circuit;
pub mod json_parse;
pub mod openai;
pub mod probe;
pub mod prompt;
pub mod provider;
pub mod templates;
pub mod token;

pub use provider::{
    ChatMessage, ChatRequest, ChatResponse, LlmProvider, ProviderCapabilities, Role, StructuredMode,
};
