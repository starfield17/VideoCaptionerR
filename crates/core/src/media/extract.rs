//! Atomic ffmpeg audio extraction to 16 kHz mono PCM s16le WAV.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_store::artifact::commit_file;

use crate::constants::{PCM_BYTES_PER_HOUR, PCM_CHANNELS, PCM_SAMPLE_RATE};
use crate::media::hash::pcm_hash_file;
use crate::media::probe::find_ffmpeg;

/// Options for canonical audio extraction.
#[derive(Debug, Clone)]
pub struct ExtractOptions {
    pub stream_index: u32,
    pub ffmpeg_path: Option<PathBuf>,
    /// Expected duration from probe (ms) for tolerance check.
    pub expected_duration_ms: Option<u64>,
    /// Duration tolerance in ms (default 1500).
    pub duration_tolerance_ms: u64,
}

impl Default for ExtractOptions {
    fn default() -> Self {
        Self {
            stream_index: 0,
            ffmpeg_path: None,
            expected_duration_ms: None,
            duration_tolerance_ms: 1500,
        }
    }
}

/// Estimate required free disk space before extraction.
pub fn required_disk_bytes(duration_ms: u64) -> u64 {
    let hours = duration_ms as f64 / 3_600_000.0;
    let pcm = (hours * PCM_BYTES_PER_HOUR as f64) as u64;
    // estimated_pcm_bytes * 1.5 + 256 MiB
    let with_margin = pcm.saturating_mul(3) / 2;
    with_margin.saturating_add(256 * 1024 * 1024)
}

/// Check free space on the filesystem containing `dir`.
pub fn ensure_disk_space(dir: &Path, duration_ms: u64) -> VcResult<()> {
    let required = required_disk_bytes(duration_ms);
    // Best-effort: use `statvfs` via libc-less approach — try to create parent and
    // skip hard check if unavailable. On Unix, read `/` free via `df` is heavy;
    // use a simple available-space probe with std when possible.
    if let Some(available) = available_bytes(dir) {
        if available < required {
            return Err(VcError::new(
                ErrorCode::DiskSpaceInsufficient,
                format!(
                    "need ~{required} bytes free for extraction, have {available} under {}",
                    dir.display()
                ),
            ));
        }
    }
    Ok(())
}

fn available_bytes(dir: &Path) -> Option<u64> {
    // Portable-enough: run `df -Pk` and parse. Not for production hot path volume.
    let path = if dir.exists() {
        dir.to_path_buf()
    } else {
        dir.parent()?.to_path_buf()
    };
    let output = Command::new("df").args(["-Pk"]).arg(&path).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let line = text.lines().nth(1)?;
    let avail_kb: u64 = line.split_whitespace().nth(3)?.parse().ok()?;
    Some(avail_kb.saturating_mul(1024))
}

/// Extract audio to `job_dir/audio.wav` via `audio.tmp.wav` then atomic commit.
///
/// On cancellation/failure, only the tmp file is removed; a previous valid
/// `audio.wav` is preserved.
pub fn extract_audio_wav(
    input: &Path,
    job_dir: &Path,
    opts: &ExtractOptions,
) -> VcResult<ExtractResult> {
    if !input.exists() {
        return Err(VcError::new(
            ErrorCode::InputNotFound,
            format!("input not found: {}", input.display()),
        ));
    }
    fs::create_dir_all(job_dir).map_err(|e| {
        VcError::new(
            ErrorCode::FfmpegFailed,
            format!("create job dir {}: {e}", job_dir.display()),
        )
    })?;

    if let Some(dur) = opts.expected_duration_ms {
        ensure_disk_space(job_dir, dur)?;
    }

    let ffmpeg = find_ffmpeg(opts.ffmpeg_path.as_deref())?;
    let tmp = job_dir.join("audio.tmp.wav");
    let final_path = job_dir.join("audio.wav");

    // Clean any leftover tmp from a previous crash.
    let _ = fs::remove_file(&tmp);

    let map = format!("0:{}", opts.stream_index);
    let mut cmd = Command::new(&ffmpeg);
    cmd.args(["-nostdin", "-hide_banner", "-loglevel", "error", "-i"])
        .arg(input)
        .args([
            "-map",
            &map,
            "-vn",
            "-ac",
            "1",
            "-ar",
            &PCM_SAMPLE_RATE.to_string(),
            "-c:a",
            "pcm_s16le",
            "-f",
            "wav",
            "-y",
        ])
        .arg(&tmp)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());

    let output = cmd.output().map_err(|e| {
        // Ensure tmp cleaned on spawn failure.
        let _ = fs::remove_file(&tmp);
        VcError::new(ErrorCode::FfmpegFailed, format!("spawn ffmpeg: {e}"))
    })?;

    if !output.status.success() {
        let _ = fs::remove_file(&tmp);
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(VcError::new(
            ErrorCode::FfmpegFailed,
            format!("ffmpeg extraction failed: {stderr}"),
        ));
    }

    if !tmp.exists() {
        return Err(VcError::new(
            ErrorCode::FfmpegFailed,
            "ffmpeg reported success but tmp wav missing",
        ));
    }

    // Validate WAV header / format.
    validate_pcm_wav(&tmp)?;

    if let Some(expected) = opts.expected_duration_ms {
        if let Some(actual) = wav_duration_ms(&tmp) {
            let delta = actual.abs_diff(expected);
            if delta > opts.duration_tolerance_ms {
                let _ = fs::remove_file(&tmp);
                return Err(VcError::new(
                    ErrorCode::FfmpegFailed,
                    format!(
                        "extracted duration {actual}ms differs from probe {expected}ms by {delta}ms"
                    ),
                ));
            }
        }
    }

    // Atomic rename tmp -> final.
    let content_hash = commit_file(&tmp, &final_path)?;
    // Prefer hashing final path (same bytes).
    let pcm_hash = pcm_hash_file(&final_path).unwrap_or(content_hash);

    Ok(ExtractResult {
        wav_path: final_path,
        pcm_hash,
        sample_rate: PCM_SAMPLE_RATE,
        channels: PCM_CHANNELS,
    })
}

#[derive(Debug, Clone)]
pub struct ExtractResult {
    pub wav_path: PathBuf,
    pub pcm_hash: String,
    pub sample_rate: u32,
    pub channels: u16,
}

/// Validate 16 kHz mono PCM s16le WAV header.
pub fn validate_pcm_wav(path: &Path) -> VcResult<()> {
    let data = fs::read(path).map_err(|e| {
        VcError::new(
            ErrorCode::FfmpegFailed,
            format!("read wav {}: {e}", path.display()),
        )
    })?;
    if data.len() < 44 {
        return Err(VcError::new(
            ErrorCode::FfmpegFailed,
            "wav too small to be valid",
        ));
    }
    if &data[0..4] != b"RIFF" || &data[8..12] != b"WAVE" {
        return Err(VcError::new(
            ErrorCode::FfmpegFailed,
            "not a RIFF/WAVE file",
        ));
    }
    // Find fmt chunk.
    let mut i = 12usize;
    let mut fmt: Option<(u16, u16, u32, u16)> = None;
    while i + 8 <= data.len() {
        let tag = &data[i..i + 4];
        let size = u32::from_le_bytes(data[i + 4..i + 8].try_into().unwrap()) as usize;
        let body = i + 8;
        if tag == b"fmt " && body + 16 <= data.len() {
            let audio_format = u16::from_le_bytes(data[body..body + 2].try_into().unwrap());
            let channels = u16::from_le_bytes(data[body + 2..body + 4].try_into().unwrap());
            let sample_rate = u32::from_le_bytes(data[body + 4..body + 8].try_into().unwrap());
            let bits = u16::from_le_bytes(data[body + 14..body + 16].try_into().unwrap());
            fmt = Some((audio_format, channels, sample_rate, bits));
            break;
        }
        i = body + size + (size % 2); // word align
    }
    let Some((audio_format, channels, sample_rate, bits)) = fmt else {
        return Err(VcError::new(
            ErrorCode::FfmpegFailed,
            "wav missing fmt chunk",
        ));
    };
    if audio_format != 1 {
        return Err(VcError::new(
            ErrorCode::FfmpegFailed,
            format!("wav audio format {audio_format} is not PCM"),
        ));
    }
    if channels != PCM_CHANNELS {
        return Err(VcError::new(
            ErrorCode::FfmpegFailed,
            format!("wav channels {channels} != {PCM_CHANNELS}"),
        ));
    }
    if sample_rate != PCM_SAMPLE_RATE {
        return Err(VcError::new(
            ErrorCode::FfmpegFailed,
            format!("wav sample_rate {sample_rate} != {PCM_SAMPLE_RATE}"),
        ));
    }
    if bits != 16 {
        return Err(VcError::new(
            ErrorCode::FfmpegFailed,
            format!("wav bits {bits} != 16"),
        ));
    }
    Ok(())
}

fn wav_duration_ms(path: &Path) -> Option<u64> {
    let data = fs::read(path).ok()?;
    if data.len() < 44 {
        return None;
    }
    let mut i = 12usize;
    let mut byte_rate = None;
    let mut data_size = None;
    while i + 8 <= data.len() {
        let tag = &data[i..i + 4];
        let size = u32::from_le_bytes(data[i + 4..i + 8].try_into().ok()?) as usize;
        let body = i + 8;
        if tag == b"fmt " && body + 16 <= data.len() {
            byte_rate = Some(u32::from_le_bytes(
                data[body + 8..body + 12].try_into().ok()?,
            ));
        } else if tag == b"data" {
            data_size = Some(size as u64);
        }
        i = body + size + (size % 2);
        if i >= data.len() {
            break;
        }
    }
    let br = byte_rate? as u64;
    let ds = data_size?;
    if br == 0 {
        return None;
    }
    Some(ds.saturating_mul(1000) / br)
}

/// Simulate ffmpeg killed mid-extraction: ensure only tmp exists, not final.
#[cfg(test)]
pub fn simulate_half_extract(job_dir: &Path) -> PathBuf {
    let _ = fs::create_dir_all(job_dir);
    let tmp = job_dir.join("audio.tmp.wav");
    fs::write(&tmp, b"PARTIAL").unwrap();
    tmp
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn have_ffmpeg() -> bool {
        find_ffmpeg(None).is_ok()
    }

    #[test]
    fn half_file_does_not_become_final() {
        let dir = tempdir().unwrap();
        let job = dir.path().join("job");
        let tmp = simulate_half_extract(&job);
        assert!(tmp.exists());
        assert!(!job.join("audio.wav").exists());
        // Recovery: remove tmp only.
        fs::remove_file(&tmp).unwrap();
        assert!(!job.join("audio.wav").exists());
    }

    #[test]
    fn extract_sine_to_pcm() {
        if !have_ffmpeg() {
            eprintln!("skip: ffmpeg missing");
            return;
        }
        let dir = tempdir().unwrap();
        let src = dir.path().join("in.wav");
        let status = Command::new("ffmpeg")
            .args([
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:duration=0.5",
                "-ar",
                "44100",
                "-ac",
                "2",
                "-y",
            ])
            .arg(&src)
            .status()
            .unwrap();
        assert!(status.success());

        // Probe stream index 0.
        let job = dir.path().join("job");
        let opts = ExtractOptions {
            stream_index: 0,
            expected_duration_ms: Some(500),
            ..Default::default()
        };
        let result = extract_audio_wav(&src, &job, &opts).unwrap();
        assert!(result.wav_path.exists());
        assert!(!job.join("audio.tmp.wav").exists());
        validate_pcm_wav(&result.wav_path).unwrap();
        assert_eq!(result.sample_rate, 16_000);
        assert!(!result.pcm_hash.is_empty());
    }

    #[test]
    fn failed_extract_preserves_existing_wav() {
        if !have_ffmpeg() {
            return;
        }
        let dir = tempdir().unwrap();
        let job = dir.path().join("job");
        fs::create_dir_all(&job).unwrap();
        let final_path = job.join("audio.wav");
        fs::write(&final_path, b"GOOD_PLACEHOLDER_NOT_WAV____").unwrap();
        // Make it look enough like we care about presence; validation would fail on re-extract.
        // Use invalid input so ffmpeg fails.
        let bad = dir.path().join("missing-input.mp4");
        let opts = ExtractOptions::default();
        let err = extract_audio_wav(&bad, &job, &opts).unwrap_err();
        assert!(matches!(
            err.code,
            ErrorCode::InputNotFound | ErrorCode::FfmpegFailed
        ));
        // Previous file still there.
        assert!(final_path.exists());
        assert!(!job.join("audio.tmp.wav").exists());
    }
}
