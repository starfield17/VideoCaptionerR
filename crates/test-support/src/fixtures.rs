//! Shared IR fixtures for tests.

use videocaptionerr_contracts::protocol::SegmentWord;
use videocaptionerr_contracts::transcript::{
    EngineFingerprint, Transcript, Word, PROB_UNAVAILABLE,
};

pub fn sample_words() -> Vec<Word> {
    vec![
        Word {
            text: "hello".into(),
            start_ms: 0,
            end_ms: 200,
            prob: 0.9,
        },
        Word {
            text: "world".into(),
            start_ms: 250,
            end_ms: 500,
            prob: PROB_UNAVAILABLE,
        },
        Word {
            text: "from".into(),
            start_ms: 520,
            end_ms: 700,
            prob: 0.8,
        },
        Word {
            text: "tests".into(),
            start_ms: 720,
            end_ms: 1000,
            prob: 0.85,
        },
    ]
}

pub fn sample_segment_words() -> Vec<SegmentWord> {
    sample_words()
        .into_iter()
        .map(|w| SegmentWord {
            text: w.text,
            start_ms: w.start_ms,
            end_ms: w.end_ms,
            prob: w.prob,
        })
        .collect()
}

pub fn sample_transcript() -> Transcript {
    Transcript::new_asr(
        "blake3:fixture",
        EngineFingerprint {
            engine_id: "fake".into(),
            adapter_version: "0.1.0".into(),
            runtime_version: "test".into(),
            model_id: "fake-tiny".into(),
            model_digest: Some("fake".into()),
            device: Some("cpu".into()),
        },
        sample_words(),
    )
}
