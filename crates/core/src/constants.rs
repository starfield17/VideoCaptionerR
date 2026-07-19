//! Frozen default constants from the architecture manual (Appendix A).

pub use videocaptionerr_domain::subtitle::constants::{
    MAX_GAP_MS, MAX_WORD_COUNT_CJK, MAX_WORD_COUNT_ENGLISH, MERGE_MIN_WORDS, MERGE_SHORT_GAP_MS,
    MERGE_VERY_SHORT_GAP_MS, MERGE_VERY_SHORT_WORDS, PREFIX_WORD_RATIO, RULE_SPLIT_GAP_MS,
    SUFFIX_WORD_RATIO, TIME_GAP_MULTIPLIER, TIME_GAP_WINDOW_SIZE,
};

pub const LLM_TARGET_CJK_LENGTH: usize = 18;
pub const LLM_TARGET_ENGLISH_WORDS: usize = 12;
pub const SEGMENT_WORD_THRESHOLD: usize = 500;
pub const LLM_MAX_ITEMS: usize = 20;
pub const CORRECTION_SIMILARITY: f64 = 0.7;
pub const SPLIT_RETRIES: u32 = 2;
pub const CORRECTION_TRANSLATION_RETRIES: u32 = 3;
pub const SPLIT_TEMPERATURE: f64 = 0.1;
pub const CORRECTION_TEMPERATURE: f64 = 0.2;
pub const TRANSLATION_TEMPERATURE_MIN: f64 = 0.2;
pub const TRANSLATION_TEMPERATURE_MAX: f64 = 0.3;
pub const GLOBAL_MAX_CHUNK_SECS: u64 = 600;
pub const CHUNK_SEARCH_RADIUS_SECS: u64 = 30;
pub const CHUNK_CONTEXT_PADDING_SECS: f64 = 1.5;
pub const MINIMUM_CHUNK_SECS: u64 = 60;
pub const VAD_THRESHOLD: f64 = 0.4;
pub const VAD_MIN_SILENCE_MS: u64 = 500;
pub const VAD_SPEECH_PADDING_MS: u64 = 200;
pub const CANCEL_GRACE_MS: u64 = 3000;
pub const WORKER_SEGMENT_CHANNEL: usize = 256;
pub const WORK_UNIT_RETRIES: u32 = 2;
pub const OOM_STRATEGY_RETRY: u32 = 1;
pub const SHARED_CACHE_BYTES: u64 = 20 * 1024 * 1024 * 1024;
pub const PCM_SAMPLE_RATE: u32 = 16_000;
pub const PCM_CHANNELS: u16 = 1;
pub const PCM_BYTES_PER_HOUR: u64 = 115_200_000; // approx 16kHz mono s16le
