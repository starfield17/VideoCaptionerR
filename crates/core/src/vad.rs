//! Application-facing VAD policy and Silero ONNX fallback boundary.
//!
//! Full Silero weights are optional assets. When the model file is absent the
//! fallback reports `RUNTIME_UNAVAILABLE` rather than fabricating speech cuts.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use videocaptionerr_contracts::error::{ErrorCode, VcError};

use crate::application_error::{AppResult, ApplicationError};
use crate::chunking::SilenceRegion;

/// Manual defaults (implementation-decisions §15.1).
pub const VAD_DEFAULT_THRESHOLD: f32 = 0.4;
pub const VAD_DEFAULT_MIN_SILENCE_MS: u64 = 500;
pub const VAD_DEFAULT_SPEECH_PAD_MS: u64 = 200;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VadOptions {
    pub enabled: bool,
    pub threshold: f32,
    pub min_silence_ms: u64,
    pub speech_pad_ms: u64,
    /// Optional path to Silero ONNX model. When missing, fallback is unavailable.
    pub model_path: Option<PathBuf>,
}

impl Default for VadOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold: VAD_DEFAULT_THRESHOLD,
            min_silence_ms: VAD_DEFAULT_MIN_SILENCE_MS,
            speech_pad_ms: VAD_DEFAULT_SPEECH_PAD_MS,
            model_path: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VadCapability {
    EngineNative,
    RustSileroFallback,
    Unavailable,
}

/// Detect whether Rust-side Silero fallback can run.
pub fn silero_capability(model_path: Option<&Path>) -> VadCapability {
    match model_path {
        Some(p) if p.is_file() => VadCapability::RustSileroFallback,
        _ => VadCapability::Unavailable,
    }
}

/// Run Silero ONNX VAD when the model is present. Without weights, fail honestly.
pub fn detect_silence_regions(
    pcm_s16le_mono_16k: &[i16],
    options: &VadOptions,
) -> AppResult<Vec<SilenceRegion>> {
    if !options.enabled {
        return Ok(Vec::new());
    }
    let Some(model) = options.model_path.as_ref() else {
        return Err(ApplicationError::Adapter(VcError::new(
            ErrorCode::RuntimeUnavailable,
            "Silero VAD model path not configured",
        )));
    };
    if !model.is_file() {
        return Err(ApplicationError::Adapter(VcError::new(
            ErrorCode::RuntimeUnavailable,
            format!("Silero VAD model not found: {}", model.display()),
        )));
    }
    // Full ONNX inference is asset-gated. With a present model file we still
    // require a linked runtime; until then report unavailable rather than fake.
    let _ = pcm_s16le_mono_16k;
    Err(ApplicationError::Adapter(VcError::new(
        ErrorCode::RuntimeUnavailable,
        "Silero ONNX runtime not linked in this build; use engine-native VAD or energy cuts",
    )))
}

/// Energy-based silence regions used when VAD is disabled or unavailable.
/// Deterministic fallback for ChunkPlan (not claimed as neural VAD).
pub fn energy_silence_regions(
    samples: &[f32],
    sample_rate: u32,
    min_silence_ms: u64,
    threshold: f32,
) -> Vec<SilenceRegion> {
    if samples.is_empty() || sample_rate == 0 {
        return Vec::new();
    }
    let min_samples = (min_silence_ms * u64::from(sample_rate) / 1000) as usize;
    let min_samples = min_samples.max(1);
    let mut regions = Vec::new();
    let mut start: Option<usize> = None;
    for (i, &s) in samples.iter().enumerate() {
        let silent = s.abs() < threshold;
        match (silent, start) {
            (true, None) => start = Some(i),
            (false, Some(s0)) => {
                if i - s0 >= min_samples {
                    let start_ms = (s0 as u64 * 1000) / u64::from(sample_rate);
                    let end_ms = (i as u64 * 1000) / u64::from(sample_rate);
                    regions.push(SilenceRegion { start_ms, end_ms });
                }
                start = None;
            }
            _ => {}
        }
    }
    if let Some(s0) = start {
        if samples.len() - s0 >= min_samples {
            let start_ms = (s0 as u64 * 1000) / u64::from(sample_rate);
            let end_ms = (samples.len() as u64 * 1000) / u64::from(sample_rate);
            regions.push(SilenceRegion { start_ms, end_ms });
        }
    }
    regions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_manual() {
        let o = VadOptions::default();
        assert!(o.enabled);
        assert!((o.threshold - 0.4).abs() < f32::EPSILON);
        assert_eq!(o.min_silence_ms, 500);
        assert_eq!(o.speech_pad_ms, 200);
    }

    #[test]
    fn missing_model_is_unavailable_not_fake() {
        let cap = silero_capability(None);
        assert_eq!(cap, VadCapability::Unavailable);
        let err = detect_silence_regions(&[], &VadOptions::default()).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("RUNTIME_UNAVAILABLE") || msg.contains("RuntimeUnavailable"));
    }

    #[test]
    fn energy_fallback_finds_silence() {
        let sr = 16_000;
        let mut samples = vec![0.5f32; sr as usize]; // 1s speech
        samples.extend(std::iter::repeat(0.0).take(sr as usize)); // 1s silence
        samples.extend(std::iter::repeat(0.5).take(sr as usize / 2));
        let regions = energy_silence_regions(&samples, sr, 500, 0.1);
        assert!(!regions.is_empty());
        assert!(regions[0].end_ms - regions[0].start_ms >= 500);
    }
}
