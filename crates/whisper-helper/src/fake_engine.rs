//! Deterministic fake engine for protocol / e2e tests.

use std::path::Path;

use videocaptionerr_contracts::protocol::SegmentWord;

use crate::audio::wav_duration_ms;

pub fn transcribe(path: &Path) -> anyhow::Result<(u64, Vec<SegmentWord>)> {
    let duration_ms = wav_duration_ms(path).unwrap_or(1000);
    let words = vec![
        SegmentWord {
            text: "hello".into(),
            start_ms: 0,
            end_ms: (duration_ms / 4).max(1),
            prob: 0.95,
        },
        SegmentWord {
            text: "from".into(),
            start_ms: duration_ms / 4,
            end_ms: duration_ms / 2,
            prob: 0.9,
        },
        SegmentWord {
            text: "whisper".into(),
            start_ms: duration_ms / 2,
            end_ms: (duration_ms * 3) / 4,
            prob: 0.92,
        },
        SegmentWord {
            text: "helper".into(),
            start_ms: (duration_ms * 3) / 4,
            end_ms: duration_ms.max(1),
            prob: 0.91,
        },
    ];
    Ok((duration_ms.max(1), words))
}
