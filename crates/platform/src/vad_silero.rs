//! Silero ONNX VAD (optional feature `silero-vad`).
//!
//! Links the ONNX Runtime only in this platform adapter. Core keeps policy
//! and energy fallback; this module performs real inference when the model
//! file is present and the feature is enabled.

use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::chunking::SilenceRegion;
use videocaptionerr_core::vad::VadOptions;

#[cfg(feature = "silero-vad")]
const SAMPLE_RATE: u32 = 16_000;
/// Silero v4 window size at 16 kHz (512 samples ≈ 32 ms).
#[cfg(feature = "silero-vad")]
const WINDOW: usize = 512;

/// Detect silence regions from mono s16le PCM at 16 kHz.
pub fn silero_silence_regions(
    pcm_s16le_mono_16k: &[i16],
    options: &VadOptions,
) -> VcResult<Vec<SilenceRegion>> {
    if !options.enabled {
        return Ok(Vec::new());
    }
    let model = options.model_path.as_ref().ok_or_else(|| {
        VcError::new(
            ErrorCode::RuntimeUnavailable,
            "Silero VAD model_path is not configured",
        )
    })?;
    if !model.is_file() {
        return Err(VcError::new(
            ErrorCode::RuntimeUnavailable,
            format!("Silero VAD model not found: {}", model.display()),
        ));
    }

    #[cfg(feature = "silero-vad")]
    {
        run_silero(model.as_path(), pcm_s16le_mono_16k, options)
    }
    #[cfg(not(feature = "silero-vad"))]
    {
        let _ = pcm_s16le_mono_16k;
        Err(VcError::new(
            ErrorCode::RuntimeUnavailable,
            "platform built without silero-vad feature; rebuild with --features silero-vad",
        ))
    }
}

#[cfg(feature = "silero-vad")]
fn run_silero(
    model: &std::path::Path,
    pcm: &[i16],
    options: &VadOptions,
) -> VcResult<Vec<SilenceRegion>> {
    use ndarray::{Array1, Array2, Array3};
    use ort::session::builder::GraphOptimizationLevel;
    use ort::session::Session;
    use ort::value::Tensor;

    let session = Session::builder()
        .map_err(|e| VcError::new(ErrorCode::RuntimeUnavailable, format!("ort builder: {e}")))?
        .with_optimization_level(GraphOptimizationLevel::Level1)
        .map_err(|e| VcError::new(ErrorCode::RuntimeUnavailable, format!("ort opt: {e}")))?
        .commit_from_file(model)
        .map_err(|e| {
            VcError::new(
                ErrorCode::RuntimeUnavailable,
                format!("load Silero ONNX {}: {e}", model.display()),
            )
        })?;

    let mut state = Array3::<f32>::zeros((2, 1, 128));
    let samples: Vec<f32> = pcm.iter().map(|&s| f32::from(s) / 32768.0).collect();
    let mut speech = Vec::with_capacity(samples.len() / WINDOW + 1);
    let mut pos = 0usize;

    while pos < samples.len() {
        let end = (pos + WINDOW).min(samples.len());
        let mut window = vec![0.0f32; WINDOW];
        window[..end - pos].copy_from_slice(&samples[pos..end]);
        let input = Array2::from_shape_vec((1, WINDOW), window)
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("vad window shape: {e}")))?;
        let sr = Array1::from_vec(vec![i64::from(SAMPLE_RATE)]);

        let input_t = Tensor::from_array(input)
            .map_err(|e| VcError::new(ErrorCode::RuntimeUnavailable, format!("vad input: {e}")))?;
        let sr_t = Tensor::from_array(sr)
            .map_err(|e| VcError::new(ErrorCode::RuntimeUnavailable, format!("vad sr: {e}")))?;
        let state_t = Tensor::from_array(state.clone())
            .map_err(|e| VcError::new(ErrorCode::RuntimeUnavailable, format!("vad state: {e}")))?;

        let inputs = ort::inputs![
            "input" => input_t,
            "sr" => sr_t,
            "state" => state_t,
        ]
        .map_err(|e| VcError::new(ErrorCode::RuntimeUnavailable, format!("Silero inputs: {e}")))?;
        let outputs = session.run(inputs).map_err(|e| {
            VcError::new(
                ErrorCode::RuntimeUnavailable,
                format!("Silero inference failed: {e}"),
            )
        })?;

        let mut prob = 0.0f32;
        if let Some(out) = outputs.get("output") {
            if let Ok(arr) = out.try_extract_tensor::<f32>() {
                if let Some(v) = arr.iter().next() {
                    prob = *v;
                }
            }
        }
        speech.push(prob >= options.threshold);

        if let Some(next_state) = outputs.get("stateN").or_else(|| outputs.get("state")) {
            if let Ok(arr) = next_state.try_extract_tensor::<f32>() {
                let flat: Vec<f32> = arr.iter().copied().collect();
                if flat.len() == state.len() {
                    state.as_slice_mut().unwrap().copy_from_slice(&flat);
                }
            }
        }
        pos += WINDOW;
    }

    let window_ms = (WINDOW as u64 * 1000) / u64::from(SAMPLE_RATE);
    let mut regions = Vec::new();
    let mut sil_start: Option<u64> = None;
    for (i, is_speech) in speech.iter().enumerate() {
        let t = i as u64 * window_ms;
        if !*is_speech {
            if sil_start.is_none() {
                sil_start = Some(t);
            }
        } else if let Some(s0) = sil_start.take() {
            let end = t;
            if end.saturating_sub(s0) >= options.min_silence_ms {
                let pad = options.speech_pad_ms;
                regions.push(SilenceRegion {
                    start_ms: s0.saturating_add(pad / 2),
                    end_ms: end.saturating_sub(pad / 2).max(s0 + 1),
                });
            }
        }
    }
    if let Some(s0) = sil_start {
        let end = speech.len() as u64 * window_ms;
        if end.saturating_sub(s0) >= options.min_silence_ms {
            regions.push(SilenceRegion {
                start_ms: s0,
                end_ms: end.max(s0 + 1),
            });
        }
    }
    Ok(regions)
}

/// Official Silero VAD ONNX asset URL (not auto-downloaded).
pub fn default_silero_model_url() -> &'static str {
    "https://github.com/snakers4/silero-vad/raw/master/src/silero_vad/data/silero_vad.onnx"
}

#[cfg(test)]
mod tests {
    use super::*;
    use videocaptionerr_core::vad::VadOptions;

    #[test]
    fn missing_model_errors_honestly() {
        let opts = VadOptions {
            model_path: Some(std::path::PathBuf::from("/no/such/silero.onnx")),
            ..Default::default()
        };
        let err = silero_silence_regions(&[], &opts).unwrap_err();
        assert_eq!(err.code, ErrorCode::RuntimeUnavailable);
    }
}
