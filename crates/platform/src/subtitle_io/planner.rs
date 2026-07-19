//! Shared OutputPlanner for CLI and GUI.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};

use super::export::{ExportFormat, ExportLayout};

/// Conflict policy for export paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConflictPolicy {
    /// Append .1, .2, ... and reserve paths at Job creation (default).
    #[default]
    Rename,
    /// Fail preflight if any output exists.
    Fail,
    /// Overwrite export files only (never input media / other Job targets).
    Overwrite,
}

impl ConflictPolicy {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "rename" => Some(Self::Rename),
            "fail" => Some(Self::Fail),
            "overwrite" => Some(Self::Overwrite),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rename => "rename",
            Self::Fail => "fail",
            Self::Overwrite => "overwrite",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlannedPath {
    pub path: PathBuf,
    pub format: ExportFormat,
    pub layout: ExportLayout,
    pub reserved_name: String,
}

#[derive(Debug, Clone)]
pub struct OutputPlan {
    pub directory: PathBuf,
    pub paths: Vec<PlannedPath>,
}

/// Plans export paths under `<source-dir>/subtitles/` by default.
#[derive(Debug, Clone)]
pub struct OutputPlanner {
    pub template: String,
    pub conflict: ConflictPolicy,
    /// Paths already reserved in this Batch/session (same-stem safety).
    reserved: HashSet<PathBuf>,
}

impl Default for OutputPlanner {
    fn default() -> Self {
        Self {
            template: "{stem}.{target_lang?}.{layout}.{format}".into(),
            conflict: ConflictPolicy::Rename,
            reserved: HashSet::new(),
        }
    }
}

impl OutputPlanner {
    pub fn new(template: impl Into<String>, conflict: ConflictPolicy) -> Self {
        Self {
            template: template.into(),
            conflict,
            reserved: HashSet::new(),
        }
    }

    /// Default export directory: `<source-file-directory>/subtitles/`.
    pub fn default_dir(source_path: &Path) -> PathBuf {
        let parent = source_path.parent().unwrap_or_else(|| Path::new("."));
        parent.join("subtitles")
    }

    /// Plan a single output path and reserve it.
    pub fn plan(
        &mut self,
        source_path: &Path,
        target_lang: Option<&str>,
        layout: ExportLayout,
        format: ExportFormat,
    ) -> VcResult<PlannedPath> {
        let dir = Self::default_dir(source_path);
        let stem = source_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("media");
        let base_name = render_template(&self.template, stem, target_lang, layout, format)?;
        let mut candidate = dir.join(&base_name);
        let mut reserved_name = base_name.clone();

        match self.conflict {
            ConflictPolicy::Overwrite => {
                // Still avoid colliding with another reservation in this planner.
                if self.reserved.contains(&candidate) {
                    return Err(VcError::new(
                        ErrorCode::OutputConflict,
                        format!("output already reserved: {}", candidate.display()),
                    ));
                }
            }
            ConflictPolicy::Fail => {
                if candidate.exists() || self.reserved.contains(&candidate) {
                    return Err(VcError::new(
                        ErrorCode::OutputConflict,
                        format!("output exists: {}", candidate.display()),
                    ));
                }
            }
            ConflictPolicy::Rename => {
                let mut n = 1u32;
                while candidate.exists() || self.reserved.contains(&candidate) {
                    let renamed = insert_suffix(&base_name, n);
                    candidate = dir.join(&renamed);
                    reserved_name = renamed;
                    n += 1;
                    if n > 10_000 {
                        return Err(VcError::new(
                            ErrorCode::OutputConflict,
                            "too many rename collisions",
                        ));
                    }
                }
            }
        }

        // Overwrite must not target the source media itself.
        if let (Ok(a), Ok(b)) = (candidate.canonicalize(), source_path.canonicalize()) {
            if a == b {
                return Err(VcError::new(
                    ErrorCode::OutputConflict,
                    "export path must not overwrite input media",
                ));
            }
        } else if candidate == source_path {
            return Err(VcError::new(
                ErrorCode::OutputConflict,
                "export path must not overwrite input media",
            ));
        }

        self.reserved.insert(candidate.clone());
        Ok(PlannedPath {
            path: candidate,
            format,
            layout,
            reserved_name,
        })
    }

    pub fn is_reserved(&self, path: &Path) -> bool {
        self.reserved.contains(path)
    }
}

fn render_template(
    template: &str,
    stem: &str,
    target_lang: Option<&str>,
    layout: ExportLayout,
    format: ExportFormat,
) -> VcResult<String> {
    // Restricted variables only — no scripts/expressions.
    // `{target_lang?}` omitted entirely (with surrounding dots cleaned) when absent.
    let mut out = template.to_string();

    if out.contains('{') {
        // Disallow unknown braces content by allowlist replacement.
        out = out.replace("{stem}", stem);
        out = out.replace("{layout}", layout.as_template_token());
        out = out.replace("{format}", format.extension());
        if let Some(lang) = target_lang {
            out = out.replace("{target_lang?}", lang);
            out = out.replace("{target_lang}", lang);
        } else {
            out = out.replace("{target_lang?}", "");
            if out.contains("{target_lang}") {
                return Err(VcError::new(
                    ErrorCode::InvalidArgument,
                    "template requires target_lang",
                ));
            }
        }
        if out.contains('{') || out.contains('}') {
            return Err(VcError::new(
                ErrorCode::InvalidArgument,
                format!("unsupported template expression: {template}"),
            ));
        }
    } else {
        return Err(VcError::new(
            ErrorCode::InvalidArgument,
            "template must include variables",
        ));
    }

    // Collapse duplicate / dangling dots from optional segments.
    while out.contains("..") {
        out = out.replace("..", ".");
    }
    let out = out.trim_matches('.').to_string();
    if out.is_empty() {
        return Err(VcError::new(
            ErrorCode::InvalidArgument,
            "template rendered empty filename",
        ));
    }
    // Basic path-injection guard.
    if out.contains('/') || out.contains('\\') || out.contains("..") {
        return Err(VcError::new(
            ErrorCode::InvalidArgument,
            "template produced invalid path characters",
        ));
    }
    Ok(out)
}

fn insert_suffix(filename: &str, n: u32) -> String {
    if let Some((stem, ext)) = filename.rsplit_once('.') {
        format!("{stem}.{n}.{ext}")
    } else {
        format!("{filename}.{n}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn same_stem_rename() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("lecture.mp4");
        fs::write(&src, b"x").unwrap();
        let sub = dir.path().join("subtitles");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("lecture.source.srt"), b"old").unwrap();

        let mut planner = OutputPlanner::default();
        let p1 = planner
            .plan(&src, None, ExportLayout::SourceOnly, ExportFormat::Srt)
            .unwrap();
        assert!(p1
            .path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .contains(".1."));

        let p2 = planner
            .plan(&src, None, ExportLayout::SourceOnly, ExportFormat::Srt)
            .unwrap();
        assert_ne!(p1.path, p2.path);
    }

    #[test]
    fn fail_policy() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("a.mp4");
        fs::write(&src, b"x").unwrap();
        let sub = dir.path().join("subtitles");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("a.source.srt"), b"old").unwrap();

        let mut planner = OutputPlanner::new("{stem}.{layout}.{format}", ConflictPolicy::Fail);
        // template without optional lang
        let err = planner
            .plan(&src, None, ExportLayout::SourceOnly, ExportFormat::Srt)
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::OutputConflict);
    }

    #[test]
    fn template_optional_lang() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("talk.mp4");
        fs::write(&src, b"x").unwrap();
        let mut planner = OutputPlanner::default();
        let p = planner
            .plan(
                &src,
                Some("zh-CN"),
                ExportLayout::BilingualSourceFirst,
                ExportFormat::Srt,
            )
            .unwrap();
        assert_eq!(
            p.path.file_name().unwrap().to_str().unwrap(),
            "talk.zh-CN.bilingual.srt"
        );
    }
}
