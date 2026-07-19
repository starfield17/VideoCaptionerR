//! Canonical Transcript IR.

use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::error::{ErrorCode, VcError, VcResult};
use crate::version::SCHEMA_VERSION;

/// Confidence: `0.0..=1.0` from adapter, `-1.0` when unavailable.
pub const PROB_UNAVAILABLE: f32 = -1.0;

/// Where cue times come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimelineSource {
    /// Word ranges from ASR; cue times derive from first/last word.
    AsrWords,
    /// Explicit imported cue times; words may be empty.
    ImportedCue,
}

/// Fingerprint of the ASR engine that produced the word timeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineFingerprint {
    pub engine_id: String,
    pub adapter_version: String,
    pub runtime_version: String,
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,
}

impl EngineFingerprint {
    pub fn unknown() -> Self {
        Self {
            engine_id: "unknown".into(),
            adapter_version: "0".into(),
            runtime_version: "0".into(),
            model_id: "unknown".into(),
            model_digest: None,
            device: None,
        }
    }
}

/// Field-level provenance for text/translation protection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FieldOrigin {
    Asr,
    RuleSplit,
    Llm { request_id: String },
    User,
    Imported,
}

impl FieldOrigin {
    pub fn is_user(&self) -> bool {
        matches!(self, Self::User)
    }
}

/// Per-word ASR token with immutable timestamps.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Word {
    pub text: String,
    pub start_ms: u64,
    pub end_ms: u64,
    /// `0.0..=1.0` or [`PROB_UNAVAILABLE`].
    pub prob: f32,
}

impl Word {
    pub fn has_confidence(&self) -> bool {
        self.prob >= 0.0 && self.prob <= 1.0
    }

    pub fn validate(&self) -> VcResult<()> {
        if self.end_ms < self.start_ms {
            return Err(VcError::new(
                ErrorCode::TimestampInvalid,
                format!("word end_ms {} < start_ms {}", self.end_ms, self.start_ms),
            ));
        }
        if !(self.prob == PROB_UNAVAILABLE || (0.0..=1.0).contains(&self.prob)) {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                format!("invalid word.prob {}", self.prob),
            ));
        }
        Ok(())
    }
}

/// Cue flags for diagnostics and filtering.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CueFlags {
    #[serde(default)]
    pub llm_failed: bool,
    #[serde(default)]
    pub hallucination_filtered: bool,
    #[serde(default)]
    pub restored_fragment: bool,
    #[serde(default)]
    pub user_edited_text: bool,
    #[serde(default)]
    pub user_edited_translation: bool,
}

/// One subtitle cue. Times for ASR-derived cues come from word ranges.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cue {
    pub id: u32,
    /// Inclusive start / exclusive end into `Transcript.words`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub word_range: Option<RangeUsize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub imported_start_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub imported_end_ms: Option<u64>,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub translation: Option<String>,
    #[serde(default)]
    pub flags: CueFlags,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_origin: Option<FieldOrigin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub translation_origin: Option<FieldOrigin>,
    /// Monotonic field revision for CAS / stale-result protection.
    #[serde(default)]
    pub text_revision: u64,
    #[serde(default)]
    pub translation_revision: u64,
}

/// Serde-friendly inclusive-start exclusive-end range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeUsize {
    pub start: usize,
    pub end: usize,
}

impl From<Range<usize>> for RangeUsize {
    fn from(r: Range<usize>) -> Self {
        Self {
            start: r.start,
            end: r.end,
        }
    }
}

impl From<RangeUsize> for Range<usize> {
    fn from(r: RangeUsize) -> Self {
        r.start..r.end
    }
}

impl RangeUsize {
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    pub fn len(&self) -> usize {
        self.end.saturating_sub(self.start)
    }

    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }
}

/// Canonical intermediate representation shared by all pipeline stages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Transcript {
    pub schema_version: u32,
    pub revision: u64,
    pub source_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    pub engine: EngineFingerprint,
    pub words: Vec<Word>,
    pub cues: Vec<Cue>,
    pub next_cue_id: u32,
    pub timeline_source: TimelineSource,
}

impl Transcript {
    pub fn new_asr(
        source_hash: impl Into<String>,
        engine: EngineFingerprint,
        words: Vec<Word>,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            revision: 1,
            source_hash: source_hash.into(),
            language: None,
            engine,
            words,
            cues: Vec::new(),
            next_cue_id: 1,
            timeline_source: TimelineSource::AsrWords,
        }
    }

    pub fn new_imported(source_hash: impl Into<String>, cues: Vec<Cue>) -> Self {
        let next_cue_id = cues
            .iter()
            .map(|c| c.id)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        Self {
            schema_version: SCHEMA_VERSION,
            revision: 1,
            source_hash: source_hash.into(),
            language: None,
            engine: EngineFingerprint::unknown(),
            words: Vec::new(),
            cues,
            next_cue_id: next_cue_id.max(1),
            timeline_source: TimelineSource::ImportedCue,
        }
    }

    /// Cue start/end for ASR-derived cues from word range; imported cues use explicit times.
    pub fn cue_times(&self, cue: &Cue) -> VcResult<(u64, u64)> {
        match self.timeline_source {
            TimelineSource::AsrWords => {
                let range = cue.word_range.ok_or_else(|| {
                    VcError::new(
                        ErrorCode::TimestampInvalid,
                        format!("cue {} missing word_range", cue.id),
                    )
                })?;
                if range.is_empty() || range.end > self.words.len() {
                    return Err(VcError::new(
                        ErrorCode::TimestampInvalid,
                        format!("cue {} has invalid word_range", cue.id),
                    ));
                }
                let start = self.words[range.start].start_ms;
                let end = self.words[range.end - 1].end_ms;
                Ok((start, end))
            }
            TimelineSource::ImportedCue => {
                let start = cue.imported_start_ms.ok_or_else(|| {
                    VcError::new(
                        ErrorCode::TimestampInvalid,
                        format!("cue {} missing imported_start_ms", cue.id),
                    )
                })?;
                let end = cue.imported_end_ms.ok_or_else(|| {
                    VcError::new(
                        ErrorCode::TimestampInvalid,
                        format!("cue {} missing imported_end_ms", cue.id),
                    )
                })?;
                if end < start {
                    return Err(VcError::new(
                        ErrorCode::TimestampInvalid,
                        format!("cue {} end < start", cue.id),
                    ));
                }
                Ok((start, end))
            }
        }
    }

    /// Validate timeline invariants. Does not mutate.
    pub fn validate(&self) -> VcResult<()> {
        if self.schema_version == 0 {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                "schema_version must be non-zero",
            ));
        }
        for w in &self.words {
            w.validate()?;
        }
        for i in 1..self.words.len() {
            if self.words[i].start_ms < self.words[i - 1].start_ms {
                return Err(VcError::new(
                    ErrorCode::TimestampInvalid,
                    format!("words not ordered at index {i}"),
                ));
            }
        }

        let mut last_end = 0usize;
        for cue in &self.cues {
            if let Some(range) = cue.word_range {
                if range.is_empty() || range.end > self.words.len() {
                    return Err(VcError::new(
                        ErrorCode::TimestampInvalid,
                        format!("cue {} invalid word_range", cue.id),
                    ));
                }
                if range.start < last_end {
                    return Err(VcError::new(
                        ErrorCode::TimestampInvalid,
                        format!("cue {} overlaps previous word range", cue.id),
                    ));
                }
                last_end = range.end;
            }
            let _ = self.cue_times(cue)?;
        }
        Ok(())
    }

    pub fn bump_revision(&mut self) {
        self.revision = self.revision.saturating_add(1);
    }

    pub fn allocate_cue_id(&mut self) -> u32 {
        let id = self.next_cue_id;
        self.next_cue_id = self.next_cue_id.saturating_add(1);
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn sample_words() -> Vec<Word> {
        vec![
            Word {
                text: "hello".into(),
                start_ms: 0,
                end_ms: 200,
                prob: 0.9,
            },
            Word {
                text: "world".into(),
                start_ms: 220,
                end_ms: 500,
                prob: PROB_UNAVAILABLE,
            },
        ]
    }

    #[test]
    fn asr_cue_times_from_words() {
        let mut t = Transcript::new_asr("hash", EngineFingerprint::unknown(), sample_words());
        t.cues.push(Cue {
            id: 1,
            word_range: Some(RangeUsize::new(0, 2)),
            imported_start_ms: None,
            imported_end_ms: None,
            text: "hello world".into(),
            translation: None,
            flags: CueFlags::default(),
            text_origin: Some(FieldOrigin::Asr),
            translation_origin: None,
            text_revision: 0,
            translation_revision: 0,
        });
        t.next_cue_id = 2;
        t.validate().unwrap();
        assert_eq!(t.cue_times(&t.cues[0]).unwrap(), (0, 500));
    }

    #[test]
    fn round_trip_json() {
        let mut t = Transcript::new_asr("abc", EngineFingerprint::unknown(), sample_words());
        t.language = Some("en".into());
        let json = serde_json::to_string_pretty(&t).unwrap();
        let back: Transcript = serde_json::from_str(&json).unwrap();
        assert_eq!(t, back);
    }

    #[test]
    fn rejects_overlapping_ranges() {
        let mut t = Transcript::new_asr("h", EngineFingerprint::unknown(), sample_words());
        t.cues = vec![
            Cue {
                id: 1,
                word_range: Some(RangeUsize::new(0, 2)),
                imported_start_ms: None,
                imported_end_ms: None,
                text: "a".into(),
                translation: None,
                flags: CueFlags::default(),
                text_origin: None,
                translation_origin: None,
                text_revision: 0,
                translation_revision: 0,
            },
            Cue {
                id: 2,
                word_range: Some(RangeUsize::new(1, 2)),
                imported_start_ms: None,
                imported_end_ms: None,
                text: "b".into(),
                translation: None,
                flags: CueFlags::default(),
                text_origin: None,
                translation_origin: None,
                text_revision: 0,
                translation_revision: 0,
            },
        ];
        assert!(t.validate().is_err());
    }
}
