use std::path::PathBuf;

use videocaptionerr_platform::AppPaths;

use crate::dto::DoctorView;
use crate::runtime::ApplicationRuntime;

#[derive(Debug, Clone)]
pub struct DoctorReport {
    pub version: &'static str,
    pub paths: AppPaths,
    pub ffmpeg: Option<PathBuf>,
    pub ffprobe: Option<PathBuf>,
    pub helper: PathBuf,
}

impl ApplicationRuntime {
    pub fn doctor(&self) -> DoctorReport {
        DoctorReport {
            version: env!("CARGO_PKG_VERSION"),
            paths: self.paths.clone(),
            ffmpeg: find_on_path("ffmpeg"),
            ffprobe: find_on_path("ffprobe"),
            helper: self.helper_path.clone(),
        }
    }

    pub fn doctor_view(&self) -> DoctorView {
        let report = self.doctor();
        DoctorView {
            version: report.version.into(),
            home: report.paths.home.display().to_string(),
            database: report.paths.db_path.display().to_string(),
            ffmpeg: report.ffmpeg.map(|path| path.display().to_string()),
            ffprobe: report.ffprobe.map(|path| path.display().to_string()),
            helper: report.helper.display().to_string(),
        }
    }
}

fn find_on_path(command: &str) -> Option<PathBuf> {
    std::env::var_os("PATH")?.to_str().and_then(|path| {
        std::env::split_paths(path).find_map(|dir| {
            let candidate = dir.join(command);
            candidate.is_file().then_some(candidate)
        })
    })
}
