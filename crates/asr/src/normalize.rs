//! ASR raw result → Transcript IR normalizer.

use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_contracts::protocol::SegmentWord;
use videocaptionerr_contracts::transcript::{
    EngineFingerprint, Transcript, Word, PROB_UNAVAILABLE,
};

use crate::engine::AsrRawResult;

/// Options for normalization.
#[derive(Debug, Clone)]
pub struct NormalizeOptions {
    pub source_hash: String,
    pub duration_ms: Option<u64>,
    pub device: Option<String>,
}

/// Normalize raw ASR into Transcript IR (words only; cues empty until split).
pub fn normalize_asr(raw: &AsrRawResult, opts: &NormalizeOptions) -> VcResult<Transcript> {
    let duration = opts.duration_ms.or(raw.duration_ms).unwrap_or_else(|| {
        raw.words
            .iter()
            .map(|w| w.end_ms)
            .chain(raw.segments.iter().map(|s| s.end_ms))
            .max()
            .unwrap_or(0)
    });

    let mut words_src: Vec<SegmentWord> = if !raw.words.is_empty() {
        raw.words.clone()
    } else {
        raw.segments
            .iter()
            .flat_map(|s| s.words.clone().unwrap_or_default())
            .collect()
    };

    // Fallback: segment-level words if still empty (A1 degraded — still materialize as words).
    if words_src.is_empty() {
        for s in &raw.segments {
            let text = s.text.trim();
            if text.is_empty() {
                continue;
            }
            words_src.push(SegmentWord {
                text: text.to_string(),
                start_ms: s.start_ms,
                end_ms: s.end_ms,
                prob: PROB_UNAVAILABLE,
            });
        }
    }

    let mut words: Vec<Word> = Vec::with_capacity(words_src.len());
    for mut w in words_src {
        // Unit conversion already in ms at protocol boundary.
        // Clip times.
        if w.start_ms > duration {
            w.start_ms = duration;
        }
        if w.end_ms > duration {
            w.end_ms = duration;
        }
        if w.end_ms < w.start_ms {
            // Only repair inverse order caused by at most 1 ms rounding.
            if w.start_ms - w.end_ms <= 1 {
                w.end_ms = w.start_ms;
            } else {
                return Err(VcError::new(
                    ErrorCode::TimestampInvalid,
                    format!(
                        "adapter inverse timestamps: {}..{} for {:?}",
                        w.start_ms, w.end_ms, w.text
                    ),
                ));
            }
        }

        // Whitespace / text normalization (NFC deferred to TextJoiner at join time).
        w.text = w.text.trim().to_string();
        if w.text.is_empty() {
            continue;
        }

        // Punctuation without independent time already has times from adapter;
        // keep as its own word token for range fidelity.

        let prob = if w.prob == PROB_UNAVAILABLE || (0.0..=1.0).contains(&w.prob) {
            w.prob
        } else {
            PROB_UNAVAILABLE
        };

        words.push(Word {
            text: w.text,
            start_ms: w.start_ms,
            end_ms: w.end_ms,
            prob,
        });
    }

    // Monotonicity validation (starts non-decreasing). Do not silently sort.
    for i in 1..words.len() {
        if words[i].start_ms < words[i - 1].start_ms {
            return Err(VcError::new(
                ErrorCode::TimestampInvalid,
                format!("words not ordered at index {i}"),
            ));
        }
    }

    let engine = EngineFingerprint {
        engine_id: raw.engine_id.clone(),
        adapter_version: env!("CARGO_PKG_VERSION").into(),
        runtime_version: "helper".into(),
        model_id: raw.model_id.clone(),
        model_digest: raw.model_digest.clone(),
        device: opts.device.clone(),
    };

    let mut t = Transcript::new_asr(opts.source_hash.clone(), engine, words);
    t.language = raw.language.clone();
    t.validate()?;
    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::AsrRawResult;
    use videocaptionerr_contracts::protocol::SegmentData;

    #[test]
    fn normalizes_and_clips() {
        let raw = AsrRawResult {
            language: Some("en".into()),
            segments: vec![SegmentData {
                text: "hi there".into(),
                start_ms: 0,
                end_ms: 500,
                words: Some(vec![
                    SegmentWord {
                        text: "hi".into(),
                        start_ms: 0,
                        end_ms: 200,
                        prob: 0.9,
                    },
                    SegmentWord {
                        text: "there".into(),
                        start_ms: 210,
                        end_ms: 5000, // beyond duration
                        prob: 0.8,
                    },
                ]),
            }],
            duration_ms: Some(1000),
            words: vec![],
            engine_id: "fake".into(),
            model_id: "tiny".into(),
            model_digest: None,
        };
        let t = normalize_asr(
            &raw,
            &NormalizeOptions {
                source_hash: "h".into(),
                duration_ms: Some(1000),
                device: Some("cpu".into()),
            },
        )
        .unwrap();
        assert_eq!(t.words.len(), 2);
        assert_eq!(t.words[1].end_ms, 1000);
        assert!(t.cues.is_empty());
    }

    #[test]
    fn rejects_large_inversion() {
        let raw = AsrRawResult {
            language: None,
            segments: vec![],
            duration_ms: Some(1000),
            words: vec![SegmentWord {
                text: "x".into(),
                start_ms: 500,
                end_ms: 100,
                prob: 0.5,
            }],
            engine_id: "fake".into(),
            model_id: "m".into(),
            model_digest: None,
        };
        let err = normalize_asr(
            &raw,
            &NormalizeOptions {
                source_hash: "h".into(),
                duration_ms: Some(1000),
                device: None,
            },
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::TimestampInvalid);
    }
}
