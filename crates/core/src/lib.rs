//! Application and business services shared by CLI and GUI.
//!
//! This crate MUST NOT import Tauri, React, or terminal-rendering concerns.

pub mod config;
pub mod constants;

pub use config::{AppConfig, LlmProviderConfig};
pub use constants::*;
