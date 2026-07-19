//! Compatibility exports for the canonical Transcript domain aggregate.
//!
//! New application/domain code should import these types from the domain
//! crate. This module remains the stable external schema path during the DDD
//! migration.

pub use videocaptionerr_domain::subtitle::transcript::{
    Cue, CueFlags, CueTextUpdate, EngineFingerprint, FieldOrigin, LlmResultBinding, LlmTextField,
    RangeUsize, TimelineSource, Transcript, Word, PROB_UNAVAILABLE,
};
