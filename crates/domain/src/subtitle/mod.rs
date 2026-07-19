//! Subtitle Document bounded context.

pub mod constants;
pub mod split;
pub mod text_joiner;
pub mod transcript;

pub use split::{rule_split, RuleSplitConfig};
pub use text_joiner::{join_word_texts, join_words};
pub use transcript::{
    Cue, CueFlags, EngineFingerprint, FieldOrigin, RangeUsize, TimelineSource, Transcript, Word,
    PROB_UNAVAILABLE,
};
