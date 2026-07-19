//! LLM provider traits and shared types.
//!
//! This crate MUST NOT own Job scheduling or persistence policy.

pub mod provider;

pub use provider::{
    ChatMessage, ChatRequest, ChatResponse, LlmProvider, ProviderCapabilities, Role,
};
