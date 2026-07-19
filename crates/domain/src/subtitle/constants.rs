//! Subtitle-domain policy constants from the frozen v1 defaults.

pub const MAX_WORD_COUNT_CJK: usize = 25;
pub const MAX_WORD_COUNT_ENGLISH: usize = 18;
pub const MAX_GAP_MS: u64 = 1500;
pub const RULE_SPLIT_GAP_MS: u64 = 500;
pub const MERGE_SHORT_GAP_MS: u64 = 200;
pub const MERGE_VERY_SHORT_GAP_MS: u64 = 500;
pub const MERGE_MIN_WORDS: usize = 5;
pub const MERGE_VERY_SHORT_WORDS: usize = 3;
pub const TIME_GAP_WINDOW_SIZE: usize = 5;
pub const TIME_GAP_MULTIPLIER: u64 = 3;
pub const PREFIX_WORD_RATIO: f64 = 0.6;
pub const SUFFIX_WORD_RATIO: f64 = 0.4;
