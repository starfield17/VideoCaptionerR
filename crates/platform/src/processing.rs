//! Host adapters used by Core's media Batch creation use case.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use videocaptionerr_contracts::error::{ErrorCode, VcError};
use videocaptionerr_core::application_error::{AppResult, ApplicationError};
use videocaptionerr_core::execution_snapshot::SourceStatSnapshot;
use videocaptionerr_core::ports::{
    ImportedSubtitle, JobWorkspace, MediaFileCatalog, OutputPlanRequest,
    OutputPlanner as OutputPlannerPort, PlannedOutput, PreparedMediaFile, SubtitleFormat,
    SubtitleImportLayout, SubtitleImporter, SubtitleLayout,
};
use videocaptionerr_domain::JobId;

use crate::subtitle_io::{import_srt, import_vtt, ImportLayout, ImportOptions};
use crate::subtitle_io::{ConflictPolicy, ExportFormat, ExportLayout, OutputPlanner};

#[derive(Debug, Clone, Copy, Default)]
pub struct LocalMediaFileCatalog;

impl MediaFileCatalog for LocalMediaFileCatalog {
    fn prepare(&self, input: &Path) -> AppResult<PreparedMediaFile> {
        let canonical_path = std::fs::canonicalize(input).map_err(|error| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::InputNotFound,
                format!("input not found {}: {error}", input.display()),
            ))
        })?;
        let metadata = std::fs::metadata(&canonical_path).map_err(|error| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::InputNotFound,
                format!("read input metadata {}: {error}", canonical_path.display()),
            ))
        })?;
        let modified_at_ms = metadata
            .modified()
            .ok()
            .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
            .and_then(|value| u64::try_from(value.as_millis()).ok());
        Ok(PreparedMediaFile {
            canonical_path,
            source_stat: SourceStatSnapshot {
                size: metadata.len(),
                modified_at_ms,
            },
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LocalSubtitleImporter;

impl SubtitleImporter for LocalSubtitleImporter {
    fn import(&self, input: &Path, layout: SubtitleImportLayout) -> AppResult<ImportedSubtitle> {
        let source_path = std::fs::canonicalize(input).map_err(|error| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::InputNotFound,
                format!("subtitle file not found {}: {error}", input.display()),
            ))
        })?;
        let text = std::fs::read_to_string(&source_path).map_err(|error| {
            ApplicationError::Adapter(VcError::new(
                ErrorCode::InputNotFound,
                format!("read subtitle {}: {error}", source_path.display()),
            ))
        })?;
        let options = ImportOptions {
            layout: import_layout(layout),
            source_hash: Some(format!("blake3:{}", blake3::hash(text.as_bytes()).to_hex())),
        };
        let extension = source_path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        let imported = match extension.as_str() {
            "srt" => import_srt(&text, &options),
            "vtt" => import_vtt(&text, &options),
            other => Err(VcError::new(
                ErrorCode::InputUnsupported,
                format!("unsupported subtitle extension '{other}' (expected srt|vtt)"),
            )),
        }
        .map_err(ApplicationError::Adapter)?;
        Ok(ImportedSubtitle {
            source_path,
            transcript: imported.0,
            warnings: imported.1.warnings,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AppJobWorkspace {
    root: PathBuf,
}

impl AppJobWorkspace {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl JobWorkspace for AppJobWorkspace {
    fn directory_for(&self, job_id: &JobId, source_path: &Path) -> PathBuf {
        let stem = source_path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("media");
        self.root
            .join(format!("{}_{}", job_id, crate::sanitize_stem(stem)))
    }
}

pub struct PlatformOutputPlanner {
    template: String,
    conflict: ConflictPolicy,
    planner: Mutex<OutputPlanner>,
}

impl PlatformOutputPlanner {
    pub fn new(template: impl Into<String>, conflict: ConflictPolicy) -> Self {
        let template = template.into();
        Self {
            template: template.clone(),
            conflict,
            planner: Mutex::new(OutputPlanner::new(template, conflict)),
        }
    }
}

impl OutputPlannerPort for PlatformOutputPlanner {
    fn begin_batch(&self) -> AppResult<()> {
        let mut planner = self
            .planner
            .lock()
            .map_err(|_| ApplicationError::Invalid("output planner was poisoned".into()))?;
        *planner = OutputPlanner::new(self.template.clone(), self.conflict);
        Ok(())
    }

    fn plan(&self, request: &OutputPlanRequest) -> AppResult<PlannedOutput> {
        let mut planner = self
            .planner
            .lock()
            .map_err(|_| ApplicationError::Invalid("output planner was poisoned".into()))?;
        let planned = planner
            .plan(
                &request.source_path,
                request.target_language.as_deref(),
                export_layout(request.layout),
                export_format(request.format),
            )
            .map_err(ApplicationError::Adapter)?;
        Ok(PlannedOutput {
            path: planned.path,
            conflict_policy: planner.conflict.as_str().into(),
        })
    }
}

fn export_format(format: SubtitleFormat) -> ExportFormat {
    match format {
        SubtitleFormat::Srt => ExportFormat::Srt,
        SubtitleFormat::Vtt => ExportFormat::Vtt,
        SubtitleFormat::Ass => ExportFormat::Ass,
    }
}

fn export_layout(layout: SubtitleLayout) -> ExportLayout {
    match layout {
        SubtitleLayout::SourceOnly => ExportLayout::SourceOnly,
        SubtitleLayout::TranslationOnly => ExportLayout::TranslationOnly,
        SubtitleLayout::BilingualSourceFirst => ExportLayout::BilingualSourceFirst,
        SubtitleLayout::BilingualTranslationFirst => ExportLayout::BilingualTranslationFirst,
    }
}

fn import_layout(layout: SubtitleImportLayout) -> ImportLayout {
    match layout {
        SubtitleImportLayout::Mono => ImportLayout::Mono,
        SubtitleImportLayout::SourceAboveTranslation => ImportLayout::SourceAboveTranslation,
        SubtitleImportLayout::TranslationAboveSource => ImportLayout::TranslationAboveSource,
    }
}
