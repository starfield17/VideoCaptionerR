//! Export preflight diagnostics.

use serde::{Deserialize, Serialize};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_contracts::transcript::Transcript;

use super::export::{ExportLayout, ExportOptions, MissingTranslationPolicy};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportDiagnosticLevel {
    Error,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportDiagnostic {
    pub level: ExportDiagnosticLevel,
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cue_id: Option<u32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExportReport {
    pub schema_version: u32,
    pub errors: Vec<ExportDiagnostic>,
    pub warnings: Vec<ExportDiagnostic>,
}

impl ExportReport {
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    pub fn all(&self) -> impl Iterator<Item = &ExportDiagnostic> {
        self.errors.iter().chain(self.warnings.iter())
    }
}

const MAX_CUE_CHARS: usize = 200;
const MAX_CPS: f64 = 30.0;
const MIN_DURATION_MS: u64 = 400;
const MAX_DURATION_MS: u64 = 10_000;

/// Run export preflight. Errors block export; warnings allow export.
pub fn preflight_export(transcript: &Transcript, opts: &ExportOptions) -> VcResult<ExportReport> {
    let mut report = ExportReport {
        schema_version: videocaptionerr_contracts::version::SCHEMA_VERSION,
        ..Default::default()
    };

    // Structural validate first.
    if let Err(e) = transcript.validate() {
        report.errors.push(ExportDiagnostic {
            level: ExportDiagnosticLevel::Error,
            code: e.code.as_str().into(),
            message: e.message,
            cue_id: None,
        });
        return Ok(report);
    }

    let mut prev_end: Option<u64> = None;
    let mut prev_text: Option<&str> = None;

    for cue in &transcript.cues {
        let (start, end) = match transcript.cue_times(cue) {
            Ok(v) => v,
            Err(e) => {
                report.errors.push(ExportDiagnostic {
                    level: ExportDiagnosticLevel::Error,
                    code: ErrorCode::TimestampInvalid.as_str().into(),
                    message: e.message,
                    cue_id: Some(cue.id),
                });
                continue;
            }
        };

        if end < start {
            report.errors.push(ExportDiagnostic {
                level: ExportDiagnosticLevel::Error,
                code: "INVERSE_CUE".into(),
                message: format!("cue {} end < start", cue.id),
                cue_id: Some(cue.id),
            });
        }

        if let Some(pe) = prev_end {
            if start < pe {
                report.warnings.push(ExportDiagnostic {
                    level: ExportDiagnosticLevel::Warning,
                    code: "OVERLAP".into(),
                    message: format!("cue {} overlaps previous", cue.id),
                    cue_id: Some(cue.id),
                });
            }
        }
        prev_end = Some(end);

        let duration = end.saturating_sub(start);
        if duration > 0 && duration < MIN_DURATION_MS {
            report.warnings.push(ExportDiagnostic {
                level: ExportDiagnosticLevel::Warning,
                code: "DURATION_SHORT".into(),
                message: format!("cue {} duration {duration}ms < {MIN_DURATION_MS}ms", cue.id),
                cue_id: Some(cue.id),
            });
        }
        if duration > MAX_DURATION_MS {
            report.warnings.push(ExportDiagnostic {
                level: ExportDiagnosticLevel::Warning,
                code: "DURATION_LONG".into(),
                message: format!("cue {} duration {duration}ms > {MAX_DURATION_MS}ms", cue.id),
                cue_id: Some(cue.id),
            });
        }

        if cue.text.trim().is_empty() {
            report.errors.push(ExportDiagnostic {
                level: ExportDiagnosticLevel::Error,
                code: "EMPTY_SOURCE".into(),
                message: format!("cue {} empty source text", cue.id),
                cue_id: Some(cue.id),
            });
        }

        match opts.layout {
            ExportLayout::TranslationOnly => {
                let missing = cue
                    .translation
                    .as_ref()
                    .map(|t| t.trim().is_empty())
                    .unwrap_or(true);
                if missing {
                    match opts.missing_translation {
                        MissingTranslationPolicy::Fail => {
                            report.errors.push(ExportDiagnostic {
                                level: ExportDiagnosticLevel::Error,
                                code: "EMPTY_TRANSLATION".into(),
                                message: format!("cue {} missing translation", cue.id),
                                cue_id: Some(cue.id),
                            });
                        }
                        MissingTranslationPolicy::FallbackToSource => {
                            report.warnings.push(ExportDiagnostic {
                                level: ExportDiagnosticLevel::Warning,
                                code: "TRANSLATION_FALLBACK".into(),
                                message: format!(
                                    "cue {} missing translation; fallback to source",
                                    cue.id
                                ),
                                cue_id: Some(cue.id),
                            });
                        }
                    }
                }
            }
            ExportLayout::BilingualSourceFirst | ExportLayout::BilingualTranslationFirst => {
                if cue
                    .translation
                    .as_ref()
                    .map(|t| t.trim().is_empty())
                    .unwrap_or(true)
                {
                    report.warnings.push(ExportDiagnostic {
                        level: ExportDiagnosticLevel::Warning,
                        code: "EMPTY_TRANSLATION".into(),
                        message: format!("cue {} missing translation in bilingual layout", cue.id),
                        cue_id: Some(cue.id),
                    });
                }
            }
            ExportLayout::SourceOnly => {}
        }

        if cue.text.chars().count() > MAX_CUE_CHARS {
            report.warnings.push(ExportDiagnostic {
                level: ExportDiagnosticLevel::Warning,
                code: "CUE_TOO_LONG".into(),
                message: format!("cue {} exceeds {MAX_CUE_CHARS} characters", cue.id),
                cue_id: Some(cue.id),
            });
        }

        if duration > 0 {
            let cps = cue.text.chars().count() as f64 / (duration as f64 / 1000.0);
            if cps > MAX_CPS {
                report.warnings.push(ExportDiagnostic {
                    level: ExportDiagnosticLevel::Warning,
                    code: "CPS_HIGH".into(),
                    message: format!("cue {} cps {cps:.1} > {MAX_CPS}", cue.id),
                    cue_id: Some(cue.id),
                });
            }
        }

        if let Some(pt) = prev_text {
            if pt == cue.text {
                report.warnings.push(ExportDiagnostic {
                    level: ExportDiagnosticLevel::Warning,
                    code: "REPEATED_TEXT".into(),
                    message: format!("cue {} repeats previous text", cue.id),
                    cue_id: Some(cue.id),
                });
            }
        }
        prev_text = Some(cue.text.as_str());

        if cue.flags.llm_failed {
            report.warnings.push(ExportDiagnostic {
                level: ExportDiagnosticLevel::Warning,
                code: "LLM_FAILED".into(),
                message: format!("cue {} has llm_failed flag", cue.id),
                cue_id: Some(cue.id),
            });
        }
        if cue.flags.restored_fragment {
            report.warnings.push(ExportDiagnostic {
                level: ExportDiagnosticLevel::Warning,
                code: "RESTORED_FRAGMENT".into(),
                message: format!("cue {} restored hallucination fragment", cue.id),
                cue_id: Some(cue.id),
            });
        }
        if cue.flags.user_edited_text || cue.flags.user_edited_translation {
            report.warnings.push(ExportDiagnostic {
                level: ExportDiagnosticLevel::Warning,
                code: "USER_EDITED".into(),
                message: format!("cue {} has user edits", cue.id),
                cue_id: Some(cue.id),
            });
        }
    }

    Ok(report)
}

/// Block export when preflight has errors.
pub fn ensure_exportable(report: &ExportReport) -> VcResult<()> {
    if report.has_errors() {
        let msg = report
            .errors
            .first()
            .map(|e| e.message.clone())
            .unwrap_or_else(|| "export validation failed".into());
        Err(VcError::new(ErrorCode::ExportValidationFailed, msg))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use videocaptionerr_contracts::transcript::{
        Cue, CueFlags, EngineFingerprint, RangeUsize, Word,
    };

    fn cue(id: u32, s: usize, e: usize, text: &str) -> Cue {
        Cue {
            id,
            word_range: Some(RangeUsize::new(s, e)),
            imported_start_ms: None,
            imported_end_ms: None,
            text: text.into(),
            translation: None,
            flags: CueFlags::default(),
            text_origin: None,
            translation_origin: None,
            text_revision: 0,
            translation_revision: 0,
        }
    }

    #[test]
    fn flags_empty_source() {
        let mut t = Transcript::new_asr(
            "h",
            EngineFingerprint::unknown(),
            vec![Word {
                text: "x".into(),
                start_ms: 0,
                end_ms: 1000,
                prob: 0.9,
            }],
        );
        t.cues.push(cue(1, 0, 1, "   "));
        t.next_cue_id = 2;
        let report = preflight_export(&t, &ExportOptions::default()).unwrap();
        assert!(report.has_errors());
    }

    #[test]
    fn repeated_text_warning() {
        let mut t = Transcript::new_asr(
            "h",
            EngineFingerprint::unknown(),
            vec![
                Word {
                    text: "a".into(),
                    start_ms: 0,
                    end_ms: 500,
                    prob: 0.9,
                },
                Word {
                    text: "b".into(),
                    start_ms: 600,
                    end_ms: 1200,
                    prob: 0.9,
                },
            ],
        );
        t.cues = vec![cue(1, 0, 1, "same"), cue(2, 1, 2, "same")];
        t.next_cue_id = 3;
        let report = preflight_export(&t, &ExportOptions::default()).unwrap();
        assert!(!report.has_errors());
        assert!(report.warnings.iter().any(|w| w.code == "REPEATED_TEXT"));
    }
}
