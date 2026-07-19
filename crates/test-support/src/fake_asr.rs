//! In-process fake ASR engine for protocol and pipeline tests.

use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;
use videocaptionerr_asr::{AsrEngine, AsrEvent, AsrOptions, AsrRawResult, EngineDescriptor};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_contracts::protocol::{SegmentData, SegmentWord};

/// Behavior modes for fault injection.
#[derive(Debug, Clone)]
pub enum FakeAsrMode {
    /// Return a fixed successful transcription.
    Success,
    /// Fail with ASR_FAILED after optional delay.
    Fail { message: String },
    /// Hang until cancelled (cooperative cancel path).
    HangUntilCancel { grace: Duration },
    /// Crash-like: return WORKER_CRASHED.
    Crash,
    /// Emit protocol-pollution-style error.
    ProtocolPollution,
    /// Simulate OOM once then succeed (caller tracks attempts).
    OomThenSuccess,
    /// Timeout style error.
    Timeout,
}

/// Configurable fake ASR adapter.
pub struct FakeAsrEngine {
    descriptor: EngineDescriptor,
    mode: FakeAsrMode,
    calls: AtomicU32,
    words: Vec<SegmentWord>,
    language: Option<String>,
}

impl FakeAsrEngine {
    pub fn new(mode: FakeAsrMode) -> Self {
        Self {
            descriptor: EngineDescriptor::fake_worker(),
            mode,
            calls: AtomicU32::new(0),
            words: default_words(),
            language: Some("en".into()),
        }
    }

    pub fn with_words(mut self, words: Vec<SegmentWord>) -> Self {
        self.words = words;
        self
    }

    pub fn call_count(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }

    pub fn shared(mode: FakeAsrMode) -> Arc<Self> {
        Arc::new(Self::new(mode))
    }
}

fn default_words() -> Vec<SegmentWord> {
    vec![
        SegmentWord {
            text: "hello".into(),
            start_ms: 0,
            end_ms: 300,
            prob: 0.95,
        },
        SegmentWord {
            text: "world".into(),
            start_ms: 320,
            end_ms: 700,
            prob: 0.9,
        },
    ]
}

#[async_trait]
impl AsrEngine for FakeAsrEngine {
    fn descriptor(&self) -> &EngineDescriptor {
        &self.descriptor
    }

    async fn transcribe(
        &self,
        _audio: &Path,
        _opts: &AsrOptions,
        sink: mpsc::Sender<AsrEvent>,
    ) -> VcResult<AsrRawResult> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;

        match &self.mode {
            FakeAsrMode::Success => {}
            FakeAsrMode::Fail { message } => {
                return Err(VcError::new(ErrorCode::AsrFailed, message.clone()));
            }
            FakeAsrMode::HangUntilCancel { grace } => {
                tokio::time::sleep(*grace).await;
                return Err(VcError::new(ErrorCode::Cancelled, "cancelled during hang"));
            }
            FakeAsrMode::Crash => {
                return Err(VcError::new(ErrorCode::WorkerCrashed, "fake crash"));
            }
            FakeAsrMode::ProtocolPollution => {
                return Err(VcError::new(
                    ErrorCode::WorkerProtocolError,
                    "non-json on stdout",
                ));
            }
            FakeAsrMode::OomThenSuccess => {
                if n == 1 {
                    return Err(VcError::new(ErrorCode::AsrOom, "fake OOM"));
                }
            }
            FakeAsrMode::Timeout => {
                return Err(VcError::new(ErrorCode::WorkerTimeout, "fake timeout"));
            }
        }

        let _ = sink
            .send(AsrEvent::Language {
                language: self.language.clone().unwrap_or_else(|| "en".into()),
            })
            .await;

        let end_ms = self.words.last().map(|w| w.end_ms).unwrap_or(0);
        let text: String = self
            .words
            .iter()
            .map(|w| w.text.as_str())
            .collect::<Vec<_>>()
            .join(" ");

        let segment = SegmentData {
            text: text.clone(),
            start_ms: 0,
            end_ms,
            words: Some(self.words.clone()),
        };

        let _ = sink.send(AsrEvent::Segment(segment.clone())).await;
        let _ = sink
            .send(AsrEvent::Progress {
                processed_ms: Some(end_ms),
                total_ms: Some(end_ms),
                message: Some("done".into()),
            })
            .await;

        Ok(AsrRawResult {
            language: self.language.clone(),
            segments: vec![segment],
            duration_ms: Some(end_ms),
            words: self.words.clone(),
            engine_id: self.descriptor.engine_id.clone(),
            model_id: "fake-tiny".into(),
            model_digest: Some("fake-digest".into()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn success_emits_segments() {
        let eng = FakeAsrEngine::new(FakeAsrMode::Success);
        let (tx, mut rx) = mpsc::channel(16);
        let result = eng
            .transcribe(Path::new("/tmp/x.wav"), &AsrOptions::default(), tx)
            .await
            .unwrap();
        assert_eq!(result.words.len(), 2);
        let mut events = Vec::new();
        while let Some(e) = rx.recv().await {
            events.push(e);
        }
        assert!(!events.is_empty());
    }

    #[tokio::test]
    async fn oom_then_success() {
        let eng = FakeAsrEngine::new(FakeAsrMode::OomThenSuccess);
        let (tx, _rx) = mpsc::channel(8);
        let err = eng
            .transcribe(Path::new("a"), &AsrOptions::default(), tx)
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::AsrOom);

        let (tx2, _rx2) = mpsc::channel(8);
        let ok = eng
            .transcribe(Path::new("a"), &AsrOptions::default(), tx2)
            .await
            .unwrap();
        assert_eq!(ok.engine_id, "fake");
    }
}
