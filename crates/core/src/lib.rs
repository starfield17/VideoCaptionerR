//! Application and business services shared by CLI and GUI.
//!
//! This crate MUST NOT import Tauri, React, or terminal-rendering concerns.

pub mod application_error;
pub mod config;
pub mod constants;
pub mod media;
pub mod pipeline;
pub mod ports;
pub mod split;
pub mod subtitle;
pub mod text_joiner;
pub mod use_cases;

pub use application_error::{AppResult, ApplicationError};
pub use config::{AppConfig, LlmProviderConfig};
pub use constants::*;
pub use media::{
    extract_audio_wav, media_hash_file, pcm_hash_file, probe_media, select_audio_stream,
    ExtractOptions,
};
pub use pipeline::{run_transcribe, TranscribeRequest, TranscribeResult};
pub use split::{rule_split, RuleSplitConfig};
pub use subtitle::{
    import_srt, import_vtt, preflight_export, write_ass, write_srt, write_vtt, ConflictPolicy,
    ExportFormat, ExportLayout, ExportOptions, ImportLayout, ImportOptions, OutputPlanner,
};
pub use text_joiner::{join_word_texts, join_words};
