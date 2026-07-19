//! ffprobe-based media probing. Extensions are not authoritative.

use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_contracts::media::{AudioStream, MediaProbe};
use videocaptionerr_contracts::version::SCHEMA_VERSION;

/// Locate ffprobe on PATH or via override.
pub fn find_ffprobe(override_path: Option<&Path>) -> VcResult<PathBuf> {
    if let Some(p) = override_path {
        if p.is_file() {
            return Ok(p.to_path_buf());
        }
        return Err(VcError::new(
            ErrorCode::FfmpegUnavailable,
            format!("ffprobe override not found: {}", p.display()),
        ));
    }
    which("ffprobe")
        .ok_or_else(|| VcError::new(ErrorCode::FfmpegUnavailable, "ffprobe not found on PATH"))
}

pub fn find_ffmpeg(override_path: Option<&Path>) -> VcResult<PathBuf> {
    if let Some(p) = override_path {
        if p.is_file() {
            return Ok(p.to_path_buf());
        }
        return Err(VcError::new(
            ErrorCode::FfmpegUnavailable,
            format!("ffmpeg override not found: {}", p.display()),
        ));
    }
    which("ffmpeg")
        .ok_or_else(|| VcError::new(ErrorCode::FfmpegUnavailable, "ffmpeg not found on PATH"))
}

fn which(cmd: &str) -> Option<PathBuf> {
    if let Ok(path) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path) {
            let candidate = dir.join(cmd);
            if candidate.is_file() {
                return Some(candidate);
            }
            #[cfg(windows)]
            {
                let candidate = dir.join(format!("{cmd}.exe"));
                if candidate.is_file() {
                    return Some(candidate);
                }
            }
        }
    }
    None
}

/// Probe media with ffprobe JSON. File extension is ignored for validity.
pub fn probe_media(input: &Path, ffprobe: Option<&Path>) -> VcResult<MediaProbe> {
    if !input.exists() {
        return Err(VcError::new(
            ErrorCode::InputNotFound,
            format!("input not found: {}", input.display()),
        ));
    }
    let meta = std::fs::metadata(input).map_err(|e| {
        VcError::new(
            ErrorCode::InputNotFound,
            format!("stat {}: {e}", input.display()),
        )
    })?;
    let input_size = meta.len();

    let bin = find_ffprobe(ffprobe)?;
    let output = Command::new(&bin)
        .args([
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
        ])
        .arg(input)
        .output()
        .map_err(|e| VcError::new(ErrorCode::ProbeFailed, format!("spawn ffprobe: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(VcError::new(
            ErrorCode::ProbeFailed,
            format!("ffprobe failed for {}: {stderr}", input.display()),
        ));
    }

    let json: Value = serde_json::from_slice(&output.stdout)
        .map_err(|e| VcError::new(ErrorCode::ProbeFailed, format!("parse ffprobe json: {e}")))?;

    let duration_ms = parse_duration_ms(&json);
    let container = json
        .pointer("/format/format_name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let mut audio_streams = Vec::new();
    if let Some(streams) = json.get("streams").and_then(|s| s.as_array()) {
        for stream in streams {
            if stream.get("codec_type").and_then(|v| v.as_str()) != Some("audio") {
                continue;
            }
            let stream_index = stream.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
            let codec = stream
                .get("codec_name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let channels = stream.get("channels").and_then(|v| v.as_u64()).unwrap_or(0) as u16;
            let sample_rate = stream
                .get("sample_rate")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .or_else(|| stream.get("sample_rate").and_then(|v| v.as_u64()))
                .unwrap_or(0) as u32;
            let language = stream
                .pointer("/tags/language")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let title = stream
                .pointer("/tags/title")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let disposition_default = stream
                .pointer("/disposition/default")
                .and_then(|v| v.as_i64())
                .unwrap_or(0)
                == 1;
            audio_streams.push(AudioStream {
                stream_index,
                codec,
                language,
                title,
                channels,
                sample_rate,
                is_default: disposition_default,
            });
        }
    }

    if audio_streams.is_empty() {
        // Still return probe so caller can surface AUDIO_STREAM_NOT_FOUND.
        return Ok(MediaProbe {
            schema_version: SCHEMA_VERSION,
            input_size,
            container,
            duration_ms,
            audio_streams,
        });
    }

    Ok(MediaProbe {
        schema_version: SCHEMA_VERSION,
        input_size,
        container,
        duration_ms,
        audio_streams,
    })
}

fn parse_duration_ms(json: &Value) -> u64 {
    if let Some(s) = json.pointer("/format/duration").and_then(|v| v.as_str()) {
        if let Ok(secs) = s.parse::<f64>() {
            return (secs * 1000.0).round() as u64;
        }
    }
    if let Some(n) = json.pointer("/format/duration").and_then(|v| v.as_f64()) {
        return (n * 1000.0).round() as u64;
    }
    // Fallback: max audio stream duration
    let mut max_ms = 0u64;
    if let Some(streams) = json.get("streams").and_then(|s| s.as_array()) {
        for stream in streams {
            if let Some(s) = stream.get("duration").and_then(|v| v.as_str()) {
                if let Ok(secs) = s.parse::<f64>() {
                    max_ms = max_ms.max((secs * 1000.0).round() as u64);
                }
            }
        }
    }
    max_ms
}

/// Auto-select only when exactly one usable stream; otherwise return default
/// candidate for confirmation (caller decides).
pub fn select_audio_stream(probe: &MediaProbe) -> VcResult<Option<&AudioStream>> {
    if probe.audio_streams.is_empty() {
        return Err(VcError::new(
            ErrorCode::AudioStreamNotFound,
            "no audio streams in media",
        ));
    }
    Ok(probe.auto_select_stream())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use tempfile::tempdir;

    fn have_ffprobe() -> bool {
        find_ffprobe(None).is_ok()
    }

    #[test]
    fn probe_generated_wav() {
        if !have_ffprobe() {
            eprintln!("skip: ffprobe missing");
            return;
        }
        let dir = tempdir().unwrap();
        let wav = dir.path().join("tone.wav");
        let status = Command::new("ffmpeg")
            .args([
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:duration=1",
                "-ar",
                "16000",
                "-ac",
                "1",
                "-y",
            ])
            .arg(&wav)
            .status()
            .expect("spawn ffmpeg");
        assert!(status.success());

        let probe = probe_media(&wav, None).unwrap();
        assert!(probe.has_audio());
        assert_eq!(probe.audio_streams.len(), 1);
        assert!(probe.duration_ms >= 900 && probe.duration_ms <= 1200);
        assert!(select_audio_stream(&probe).unwrap().is_some());
    }

    #[test]
    fn missing_file() {
        let err = probe_media(Path::new("/no/such/file.mp4"), None).unwrap_err();
        assert_eq!(err.code, ErrorCode::InputNotFound);
    }
}
