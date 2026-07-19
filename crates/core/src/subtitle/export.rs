//! Deterministic SRT/VTT/ASS writers. Same IR + options => byte-identical output.

use std::fs;
use std::io::Write;
use std::path::Path;

use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_contracts::transcript::Transcript;
use videocaptionerr_store::artifact::atomic_write_bytes;

use super::time::{format_srt_time, format_vtt_time};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    Srt,
    Vtt,
    Ass,
}

impl ExportFormat {
    pub fn extension(self) -> &'static str {
        match self {
            Self::Srt => "srt",
            Self::Vtt => "vtt",
            Self::Ass => "ass",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "srt" => Some(Self::Srt),
            "vtt" => Some(Self::Vtt),
            "ass" => Some(Self::Ass),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExportLayout {
    /// Source text only.
    #[default]
    SourceOnly,
    /// Translation only (see missing_translation policy).
    TranslationOnly,
    /// Source then translation (bilingual).
    BilingualSourceFirst,
    /// Translation then source.
    BilingualTranslationFirst,
}

impl ExportLayout {
    pub fn as_template_token(self) -> &'static str {
        match self {
            Self::SourceOnly => "source",
            Self::TranslationOnly => "translation",
            Self::BilingualSourceFirst | Self::BilingualTranslationFirst => "bilingual",
        }
    }
}

/// What to do when translation-only output has a missing translation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MissingTranslationPolicy {
    /// Fall back to source text.
    #[default]
    FallbackToSource,
    /// Fail export.
    Fail,
}

#[derive(Debug, Clone, Copy)]
pub struct ExportOptions {
    pub format: ExportFormat,
    pub layout: ExportLayout,
    pub missing_translation: MissingTranslationPolicy,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            format: ExportFormat::Srt,
            layout: ExportLayout::SourceOnly,
            missing_translation: MissingTranslationPolicy::FallbackToSource,
        }
    }
}

/// Render subtitle file contents (UTF-8, LF). Does not write to disk.
pub fn render(transcript: &Transcript, opts: &ExportOptions) -> VcResult<String> {
    match opts.format {
        ExportFormat::Srt => render_srt(transcript, opts),
        ExportFormat::Vtt => render_vtt(transcript, opts),
        ExportFormat::Ass => render_ass(transcript, opts),
    }
}

/// Atomically write rendered subtitles to `path` (via `.tmp` + rename).
pub fn write_export(
    path: &Path,
    transcript: &Transcript,
    opts: &ExportOptions,
) -> VcResult<String> {
    let body = render(transcript, opts)?;
    // Ensure trailing newline for determinism.
    let mut data = body.into_bytes();
    if !data.ends_with(b"\n") {
        data.push(b'\n');
    }
    // Normalize to LF only (already LF from writers).
    atomic_write_bytes(path, &data)
}

pub fn write_srt(path: &Path, transcript: &Transcript, opts: &ExportOptions) -> VcResult<String> {
    let mut o = *opts;
    o.format = ExportFormat::Srt;
    write_export(path, transcript, &o)
}

pub fn write_vtt(path: &Path, transcript: &Transcript, opts: &ExportOptions) -> VcResult<String> {
    let mut o = *opts;
    o.format = ExportFormat::Vtt;
    write_export(path, transcript, &o)
}

pub fn write_ass(path: &Path, transcript: &Transcript, opts: &ExportOptions) -> VcResult<String> {
    let mut o = *opts;
    o.format = ExportFormat::Ass;
    write_export(path, transcript, &o)
}

fn render_srt(transcript: &Transcript, opts: &ExportOptions) -> VcResult<String> {
    let mut out = String::new();
    let mut ordinal = 1u32;
    for cue in &transcript.cues {
        let (start, end) = transcript.cue_times(cue)?;
        let text = cue_text(cue, opts)?;
        if text.is_empty() {
            continue;
        }
        out.push_str(&ordinal.to_string());
        out.push('\n');
        out.push_str(&format_srt_time(start));
        out.push_str(" --> ");
        out.push_str(&format_srt_time(end));
        out.push('\n');
        out.push_str(&text);
        out.push('\n');
        out.push('\n');
        ordinal += 1;
    }
    Ok(out)
}

fn render_vtt(transcript: &Transcript, opts: &ExportOptions) -> VcResult<String> {
    let mut out = String::from("WEBVTT\n\n");
    for cue in &transcript.cues {
        let (start, end) = transcript.cue_times(cue)?;
        let text = cue_text(cue, opts)?;
        if text.is_empty() {
            continue;
        }
        out.push_str(&format_vtt_time(start));
        out.push_str(" --> ");
        out.push_str(&format_vtt_time(end));
        out.push('\n');
        out.push_str(&text);
        out.push('\n');
        out.push('\n');
    }
    Ok(out)
}

fn render_ass(transcript: &Transcript, opts: &ExportOptions) -> VcResult<String> {
    let mut out = String::new();
    out.push_str("[Script Info]\n");
    out.push_str("ScriptType: v4.00+\n");
    out.push_str("PlayResX: 1920\n");
    out.push_str("PlayResY: 1080\n");
    out.push_str("WrapStyle: 0\n");
    out.push_str("ScaledBorderAndShadow: yes\n\n");
    out.push_str("[V4+ Styles]\n");
    out.push_str(
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, \
Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, \
Alignment, MarginL, MarginR, MarginV, Encoding\n",
    );
    out.push_str(
        "Style: Default,Arial,48,&H00FFFFFF,&H000000FF,&H00000000,&H64000000,0,0,0,0,100,100,0,0,1,2,0,2,40,40,40,1\n\n",
    );
    out.push_str("[Events]\n");
    out.push_str(
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n",
    );

    for cue in &transcript.cues {
        let (start, end) = transcript.cue_times(cue)?;
        let text = cue_text(cue, opts)?;
        if text.is_empty() {
            continue;
        }
        let text = escape_ass(&text.replace('\n', "\\N"));
        out.push_str("Dialogue: 0,");
        out.push_str(&format_ass_time(start));
        out.push(',');
        out.push_str(&format_ass_time(end));
        out.push_str(",Default,,0,0,0,,");
        out.push_str(&text);
        out.push('\n');
    }
    Ok(out)
}

fn format_ass_time(ms: u64) -> String {
    let cs = ms / 10; // centiseconds
    let c = cs % 100;
    let total_s = cs / 100;
    let s = total_s % 60;
    let total_m = total_s / 60;
    let m = total_m % 60;
    let h = total_m / 60;
    format!("{h}:{m:02}:{s:02}.{c:02}")
}

fn escape_ass(s: &str) -> String {
    // Escape braces and backslashes to prevent override-tag injection.
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '{' => out.push_str("\\{"),
            '}' => out.push_str("\\}"),
            '\\' => out.push_str("\\\\"),
            _ => out.push(ch),
        }
    }
    out
}

fn cue_text(
    cue: &videocaptionerr_contracts::transcript::Cue,
    opts: &ExportOptions,
) -> VcResult<String> {
    match opts.layout {
        ExportLayout::SourceOnly => Ok(cue.text.clone()),
        ExportLayout::TranslationOnly => match &cue.translation {
            Some(t) if !t.is_empty() => Ok(t.clone()),
            _ => match opts.missing_translation {
                MissingTranslationPolicy::FallbackToSource => Ok(cue.text.clone()),
                MissingTranslationPolicy::Fail => Err(VcError::new(
                    ErrorCode::ExportValidationFailed,
                    format!("cue {} missing translation", cue.id),
                )),
            },
        },
        ExportLayout::BilingualSourceFirst => {
            let t = cue.translation.as_deref().unwrap_or("");
            if t.is_empty() {
                Ok(cue.text.clone())
            } else {
                Ok(format!("{}\n{}", cue.text, t))
            }
        }
        ExportLayout::BilingualTranslationFirst => {
            let t = cue.translation.as_deref().unwrap_or("");
            if t.is_empty() {
                Ok(cue.text.clone())
            } else {
                Ok(format!("{t}\n{}", cue.text))
            }
        }
    }
}

/// Convenience: write bytes with explicit content (tests / tooling).
pub fn write_bytes_atomic(path: &Path, data: &[u8]) -> VcResult<String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            VcError::new(ErrorCode::ExportFailed, format!("create export dir: {e}"))
        })?;
    }
    atomic_write_bytes(path, data)
}

/// Ensure parent exists then open for write (non-atomic helper for streaming).
pub fn ensure_parent(path: &Path) -> VcResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            VcError::new(
                ErrorCode::ExportFailed,
                format!("create export dir {}: {e}", parent.display()),
            )
        })?;
    }
    Ok(())
}

#[allow(dead_code)]
fn write_all(path: &Path, data: &[u8]) -> VcResult<()> {
    ensure_parent(path)?;
    let mut f = fs::File::create(path).map_err(|e| {
        VcError::new(
            ErrorCode::ExportFailed,
            format!("create {}: {e}", path.display()),
        )
    })?;
    f.write_all(data).map_err(|e| {
        VcError::new(
            ErrorCode::ExportFailed,
            format!("write {}: {e}", path.display()),
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use videocaptionerr_contracts::transcript::{
        Cue, CueFlags, EngineFingerprint, FieldOrigin, RangeUsize, Transcript, Word,
    };

    fn sample() -> Transcript {
        let mut t = Transcript::new_asr(
            "h",
            EngineFingerprint::unknown(),
            vec![
                Word {
                    text: "Hello".into(),
                    start_ms: 0,
                    end_ms: 500,
                    prob: 0.9,
                },
                Word {
                    text: "world".into(),
                    start_ms: 520,
                    end_ms: 1000,
                    prob: 0.9,
                },
            ],
        );
        t.cues.push(Cue {
            id: 1,
            word_range: Some(RangeUsize::new(0, 2)),
            imported_start_ms: None,
            imported_end_ms: None,
            text: "Hello world".into(),
            translation: Some("你好世界".into()),
            flags: CueFlags::default(),
            text_origin: Some(FieldOrigin::RuleSplit),
            translation_origin: Some(FieldOrigin::Llm {
                request_id: "r1".into(),
            }),
            text_revision: 0,
            translation_revision: 0,
        });
        t.next_cue_id = 2;
        t
    }

    #[test]
    fn srt_deterministic() {
        let t = sample();
        let opts = ExportOptions::default();
        let a = render(&t, &opts).unwrap();
        let b = render(&t, &opts).unwrap();
        assert_eq!(a, b);
        assert!(a.contains("00:00:00,000 --> 00:00:01,000"));
        assert!(a.starts_with("1\n"));
    }

    #[test]
    fn vtt_header_and_dot_ms() {
        let t = sample();
        let opts = ExportOptions {
            format: ExportFormat::Vtt,
            ..Default::default()
        };
        let body = render(&t, &opts).unwrap();
        assert!(body.starts_with("WEBVTT\n"));
        assert!(body.contains("00:00:00.000 --> 00:00:01.000"));
    }

    #[test]
    fn ass_escapes_braces() {
        let mut t = sample();
        t.cues[0].text = "inject {\\an8}x".into();
        let opts = ExportOptions {
            format: ExportFormat::Ass,
            ..Default::default()
        };
        let body = render(&t, &opts).unwrap();
        assert!(body.contains("\\{"));
        assert!(!body.contains("{\\an8}"));
    }

    #[test]
    fn bilingual_and_atomic_write() {
        let t = sample();
        let opts = ExportOptions {
            format: ExportFormat::Srt,
            layout: ExportLayout::BilingualSourceFirst,
            ..Default::default()
        };
        let body = render(&t, &opts).unwrap();
        assert!(body.contains("Hello world\n你好世界"));

        let dir = tempdir().unwrap();
        let path = dir.path().join("out.srt");
        let hash = write_export(&path, &t, &opts).unwrap();
        assert!(!hash.is_empty());
        assert_eq!(fs::read_to_string(&path).unwrap(), body);
    }

    #[test]
    fn missing_translation_fail() {
        let mut t = sample();
        t.cues[0].translation = None;
        let opts = ExportOptions {
            layout: ExportLayout::TranslationOnly,
            missing_translation: MissingTranslationPolicy::Fail,
            ..Default::default()
        };
        assert!(render(&t, &opts).is_err());
    }
}
