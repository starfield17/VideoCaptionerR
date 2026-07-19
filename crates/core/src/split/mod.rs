//! Sentence splitting. Rule-based path produces cue word ranges directly.

pub mod rule;

pub use rule::{rule_split, RuleSplitConfig};
