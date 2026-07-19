//! Tolerant SRT/VTT import into Transcript IR (ImportedCue timeline).

use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_contracts::transcript::{Cue, CueFlags, FieldOrigin, Transcript};

use super::time::{parse_srt_time, parse_vtt_time};

/// How multiline cue text should be interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ImportLayout {
    /// Single language; join lines with space (or keep break markers stripped).
    #[default]
    Mono,
    /// First line(s) source, last line translation.
    SourceAboveTranslation,
    /// First line translation, last line source.
    TranslationAboveSource,
}

#[derive(Debug, Clone, Default)]
pub struct ImportOptions {
    pub layout: ImportLayout,
    /// Optional hash/identity for the imported source file.
    pub source_hash: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ImportDiagnostics {
    pub warnings: Vec<String>,
}

/// Import an SRT document. Readers are tolerant and collect diagnostics.
pub fn import_srt(input: &str, opts: &ImportOptions) -> VcResult<(Transcript, ImportDiagnostics)> {
    let mut diags = ImportDiagnostics::default();
    let blocks = split_blocks(input);
    let mut cues = Vec::new();
    let mut next_id = 1u32;

    for block in blocks {
        let lines: Vec<&str> = block
            .lines()
            .map(|l| l.trim_end_matches('\r'))
            .filter(|l| !l.is_empty())
            .collect();
        if lines.is_empty() {
            continue;
        }

        // Optional numeric index line.
        let mut idx = 0usize;
        if lines[0].chars().all(|c| c.is_ascii_digit()) && lines.len() >= 2 {
            idx = 1;
        }
        if idx >= lines.len() {
            diags.warnings.push("empty cue block skipped".into());
            continue;
        }

        let timing = lines[idx];
        let Some((start_s, end_s)) = split_arrow(timing) else {
            diags
                .warnings
                .push(format!("malformed timing skipped: {timing}"));
            continue;
        };
        let start_ms = match parse_srt_time(start_s.trim()) {
            Ok(v) => v,
            Err(e) => {
                diags.warnings.push(format!("bad start time: {e}"));
                continue;
            }
        };
        let end_ms = match parse_srt_time(end_s.trim()) {
            Ok(v) => v,
            Err(e) => {
                diags.warnings.push(format!("bad end time: {e}"));
                continue;
            }
        };
        if end_ms < start_ms {
            diags.warnings.push(format!(
                "cue {next_id}: end < start ({end_ms} < {start_ms}); skipped"
            ));
            continue;
        }

        let text_lines: Vec<&str> = lines[idx + 1..].to_vec();
        if text_lines.is_empty() {
            diags
                .warnings
                .push(format!("cue {next_id}: empty text; skipped"));
            continue;
        }
        let (text, translation) = apply_layout(&text_lines, opts.layout);
        cues.push(Cue {
            id: next_id,
            word_range: None,
            imported_start_ms: Some(start_ms),
            imported_end_ms: Some(end_ms),
            text,
            translation,
            flags: CueFlags::default(),
            text_origin: Some(FieldOrigin::Imported),
            translation_origin: opts
                .layout
                .has_translation()
                .then_some(FieldOrigin::Imported),
            text_revision: 0,
            translation_revision: 0,
        });
        next_id += 1;
    }

    if cues.is_empty() {
        return Err(VcError::new(
            ErrorCode::InvalidArgument,
            "no cues imported from SRT",
        ));
    }

    let hash = opts
        .source_hash
        .clone()
        .unwrap_or_else(|| "imported:srt".into());
    let t = Transcript::new_imported(hash, cues);
    t.validate()?;
    Ok((t, diags))
}

/// Import a VTT document (header required; NOTE/STYLE blocks skipped lightly).
pub fn import_vtt(input: &str, opts: &ImportOptions) -> VcResult<(Transcript, ImportDiagnostics)> {
    let mut diags = ImportDiagnostics::default();
    let trimmed = input.trim_start_matches('\u{feff}');
    if !trimmed
        .lines()
        .next()
        .is_some_and(|l| l.trim().starts_with("WEBVTT"))
    {
        return Err(VcError::new(
            ErrorCode::InvalidArgument,
            "VTT must start with WEBVTT",
        ));
    }

    // Drop header line and optional header metadata until blank line.
    let mut body = String::new();
    let mut past_header = false;
    for line in trimmed.lines() {
        if !past_header {
            if line.trim().is_empty() {
                past_header = true;
            }
            continue;
        }
        // Skip NOTE / STYLE blocks (until blank).
        body.push_str(line);
        body.push('\n');
    }

    // Reuse block parser but with VTT times.
    let blocks = split_blocks(&body);
    let mut cues = Vec::new();
    let mut next_id = 1u32;
    let mut skip_until_blank = false;

    for block in blocks {
        let raw_lines: Vec<&str> = block.lines().map(|l| l.trim_end_matches('\r')).collect();
        if raw_lines.is_empty() {
            skip_until_blank = false;
            continue;
        }
        let first = raw_lines[0].trim();
        if first.starts_with("NOTE") || first.starts_with("STYLE") || first.starts_with("REGION") {
            skip_until_blank = true;
            continue;
        }
        if skip_until_blank {
            continue;
        }

        let lines: Vec<&str> = raw_lines
            .iter()
            .copied()
            .filter(|l| !l.is_empty())
            .collect();
        if lines.is_empty() {
            continue;
        }

        // Optional cue identifier line before timing.
        let mut idx = 0usize;
        if !lines[0].contains("-->") {
            if lines.len() < 2 || !lines[1].contains("-->") {
                diags
                    .warnings
                    .push(format!("unrecognized VTT block skipped: {}", lines[0]));
                continue;
            }
            idx = 1;
        }

        let timing = lines[idx];
        let Some((start_s, end_s)) = split_arrow(timing) else {
            diags
                .warnings
                .push(format!("malformed VTT timing: {timing}"));
            continue;
        };
        // Strip settings after end timestamp.
        let end_token = end_s.split_whitespace().next().unwrap_or(end_s);
        let start_ms = match parse_vtt_time(start_s.trim()) {
            Ok(v) => v,
            Err(e) => {
                diags.warnings.push(format!("bad VTT start: {e}"));
                continue;
            }
        };
        let end_ms = match parse_vtt_time(end_token.trim()) {
            Ok(v) => v,
            Err(e) => {
                diags.warnings.push(format!("bad VTT end: {e}"));
                continue;
            }
        };
        let text_lines: Vec<&str> = lines[idx + 1..].to_vec();
        if text_lines.is_empty() {
            diags.warnings.push(format!("cue {next_id}: empty text"));
            continue;
        }
        let (text, translation) = apply_layout(&text_lines, opts.layout);
        cues.push(Cue {
            id: next_id,
            word_range: None,
            imported_start_ms: Some(start_ms),
            imported_end_ms: Some(end_ms),
            text,
            translation,
            flags: CueFlags::default(),
            text_origin: Some(FieldOrigin::Imported),
            translation_origin: opts
                .layout
                .has_translation()
                .then_some(FieldOrigin::Imported),
            text_revision: 0,
            translation_revision: 0,
        });
        next_id += 1;
    }

    if cues.is_empty() {
        return Err(VcError::new(
            ErrorCode::InvalidArgument,
            "no cues imported from VTT",
        ));
    }
    let hash = opts
        .source_hash
        .clone()
        .unwrap_or_else(|| "imported:vtt".into());
    let t = Transcript::new_imported(hash, cues);
    t.validate()?;
    Ok((t, diags))
}

impl ImportLayout {
    fn has_translation(self) -> bool {
        !matches!(self, Self::Mono)
    }
}

fn split_blocks(input: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut cur = String::new();
    for line in input.lines() {
        if line.trim().is_empty() {
            if !cur.trim().is_empty() {
                blocks.push(std::mem::take(&mut cur));
            }
        } else {
            cur.push_str(line);
            cur.push('\n');
        }
    }
    if !cur.trim().is_empty() {
        blocks.push(cur);
    }
    blocks
}

fn split_arrow(timing: &str) -> Option<(&str, &str)> {
    let timing = timing.trim();
    let (a, b) = timing.split_once("-->")?;
    Some((a, b))
}

fn apply_layout(lines: &[&str], layout: ImportLayout) -> (String, Option<String>) {
    match layout {
        ImportLayout::Mono => (lines.join(" "), None),
        ImportLayout::SourceAboveTranslation => {
            if lines.len() == 1 {
                (lines[0].to_string(), None)
            } else {
                let mid = lines.len() / 2;
                // Prefer last line as translation when exactly 2 lines.
                if lines.len() == 2 {
                    (lines[0].to_string(), Some(lines[1].to_string()))
                } else {
                    (lines[..mid].join(" "), Some(lines[mid..].join(" ")))
                }
            }
        }
        ImportLayout::TranslationAboveSource => {
            if lines.len() == 1 {
                (lines[0].to_string(), None)
            } else if lines.len() == 2 {
                (lines[1].to_string(), Some(lines[0].to_string()))
            } else {
                let mid = lines.len() / 2;
                (lines[mid..].join(" "), Some(lines[..mid].join(" ")))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_simple_srt() {
        let srt = "1\n00:00:00,000 --> 00:00:01,500\nHello world\n\n2\n00:00:02,000 --> 00:00:03,000\nBye\n";
        let (t, d) = import_srt(srt, &ImportOptions::default()).unwrap();
        assert!(d.warnings.is_empty());
        assert_eq!(t.cues.len(), 2);
        assert_eq!(t.cues[0].text, "Hello world");
        assert_eq!(t.cue_times(&t.cues[0]).unwrap(), (0, 1500));
    }

    #[test]
    fn import_bilingual_layout() {
        let srt = "1\n00:00:00,000 --> 00:00:01,000\nHello\n你好\n";
        let opts = ImportOptions {
            layout: ImportLayout::SourceAboveTranslation,
            ..Default::default()
        };
        let (t, _) = import_srt(srt, &opts).unwrap();
        assert_eq!(t.cues[0].text, "Hello");
        assert_eq!(t.cues[0].translation.as_deref(), Some("你好"));
    }

    #[test]
    fn import_vtt_basic() {
        let vtt = "WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nHi\n";
        let (t, _) = import_vtt(vtt, &ImportOptions::default()).unwrap();
        assert_eq!(t.cues.len(), 1);
        assert_eq!(t.cues[0].text, "Hi");
    }

    #[test]
    fn malformed_srt_collects_warnings() {
        let srt = "1\nnot-a-time\nHello\n\n2\n00:00:01,000 --> 00:00:02,000\nOk\n";
        let (t, d) = import_srt(srt, &ImportOptions::default()).unwrap();
        assert_eq!(t.cues.len(), 1);
        assert!(!d.warnings.is_empty());
    }
}
