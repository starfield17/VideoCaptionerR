//! Application and business services shared by CLI and GUI.
//!
//! This crate MUST NOT import Tauri, React, or terminal-rendering concerns.

pub mod application_error;
pub mod constants;
pub mod ports;
pub mod split;
pub mod text_joiner;
pub mod use_cases;

pub use application_error::{AppResult, ApplicationError};
pub use constants::*;
pub use split::{rule_split, RuleSplitConfig};
pub use text_joiner::{join_word_texts, join_words};
