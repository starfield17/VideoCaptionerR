//! WAV loading helpers for the isolated helper process.

use std::path::Path;

use hound::SampleFormat;

/// Load mono f32 samples at the native sample rate. Returns (samples, sample_rate).
#[cfg_attr(not(feature = "whisper-cpp"), allow(dead_code))]
pub fn load_wav_f32(path: &Path) -> anyhow::Result<(Vec<f32>, u32)> {
    let mut reader = hound::WavReader::open(path)?;
    let spec = reader.spec();
    let rate = spec.sample_rate;
    let channels = spec.channels.max(1) as usize;
    let samples: Vec<f32> = match spec.sample_format {
        SampleFormat::Float => reader
            .samples::<f32>()
            .collect::<Result<Vec<_>, _>>()?
            .chunks(channels)
            .map(|c| c.iter().sum::<f32>() / channels as f32)
            .collect(),
        SampleFormat::Int => {
            let max = match spec.bits_per_sample {
                8 => i8::MAX as f32,
                16 => i16::MAX as f32,
                24 => (1i32 << 23) as f32,
                32 => i32::MAX as f32,
                other => anyhow::bail!("unsupported bits_per_sample {other}"),
            };
            reader
                .samples::<i32>()
                .collect::<Result<Vec<_>, _>>()?
                .chunks(channels)
                .map(|c| {
                    let sum: i64 = c.iter().map(|s| i64::from(*s)).sum();
                    (sum as f32 / channels as f32) / max
                })
                .collect()
        }
    };
    Ok((samples, rate))
}

/// Resample mono f32 to 16 kHz using linear interpolation (good enough for smoke).
#[cfg_attr(not(feature = "whisper-cpp"), allow(dead_code))]
pub fn resample_mono(samples: &[f32], from_rate: u32, to_rate: u32) -> Vec<f32> {
    if from_rate == 0 || samples.is_empty() {
        return Vec::new();
    }
    if from_rate == to_rate {
        return samples.to_vec();
    }
    let ratio = f64::from(from_rate) / f64::from(to_rate);
    let out_len = ((samples.len() as f64) / ratio).ceil() as usize;
    let mut out = Vec::with_capacity(out_len);
    for i in 0..out_len {
        let src = i as f64 * ratio;
        let idx = src.floor() as usize;
        let frac = (src - idx as f64) as f32;
        let a = samples.get(idx).copied().unwrap_or(0.0);
        let b = samples.get(idx + 1).copied().unwrap_or(a);
        out.push(a + (b - a) * frac);
    }
    out
}

pub fn wav_duration_ms(path: &Path) -> Option<u64> {
    let reader = hound::WavReader::open(path).ok()?;
    let spec = reader.spec();
    let len = reader.duration() as u64;
    if spec.sample_rate == 0 {
        return None;
    }
    Some(len.saturating_mul(1000) / u64::from(spec.sample_rate))
}

#[cfg_attr(not(feature = "whisper-cpp"), allow(dead_code))]
pub fn load_pcm16k(path: &Path) -> anyhow::Result<(Vec<f32>, u64)> {
    let (samples, rate) = load_wav_f32(path)?;
    let pcm = resample_mono(&samples, rate, 16_000);
    let duration_ms = if pcm.is_empty() {
        0
    } else {
        (pcm.len() as u64 * 1000) / 16_000
    };
    Ok((pcm, duration_ms.max(1)))
}
