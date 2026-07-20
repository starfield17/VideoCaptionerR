use std::path::PathBuf;
use std::process::Command;

use videocaptionerr_asr::{lock_hash_for_family, EngineFamily};
use videocaptionerr_platform::AppPaths;

use crate::dto::DoctorView;
use crate::runtime::ApplicationRuntime;

#[derive(Debug, Clone)]
pub struct RuntimeSmoke {
    pub family: String,
    pub ok: bool,
    pub detail: String,
    pub env_path: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DoctorReport {
    pub version: &'static str,
    pub paths: AppPaths,
    pub ffmpeg: Option<PathBuf>,
    pub ffprobe: Option<PathBuf>,
    pub helper: PathBuf,
    pub helper_exists: bool,
    pub uv: Option<PathBuf>,
    pub runtime_smokes: Vec<RuntimeSmoke>,
}

impl ApplicationRuntime {
    pub fn doctor(&self) -> DoctorReport {
        let uv = find_uv();
        let runtimes_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/runtimes");
        let mut runtime_smokes = Vec::new();

        // Fake always available via helper.
        runtime_smokes.push(RuntimeSmoke {
            family: "fake".into(),
            ok: self.helper_path.exists(),
            detail: if self.helper_path.exists() {
                "helper binary present".into()
            } else {
                "helper binary missing".into()
            },
            env_path: None,
        });

        // faster-whisper: real import smoke when managed env exists or can be resolved.
        runtime_smokes.push(smoke_python_family(
            EngineFamily::FasterWhisper,
            &self.paths.envs_dir,
            &runtimes_root,
            uv.as_ref(),
            "faster_whisper",
            true,
        ));

        // mlx-whisper: only real on Apple Silicon; elsewhere report RUNTIME_UNAVAILABLE.
        let mlx_supported = cfg!(all(target_os = "macos", target_arch = "aarch64"));
        if mlx_supported {
            runtime_smokes.push(smoke_python_family(
                EngineFamily::MlxWhisper,
                &self.paths.envs_dir,
                &runtimes_root,
                uv.as_ref(),
                "mlx_whisper",
                true,
            ));
        } else {
            runtime_smokes.push(RuntimeSmoke {
                family: "mlx-whisper".into(),
                ok: false,
                detail:
                    "RUNTIME_UNAVAILABLE: mlx-whisper real runtime requires macOS Apple Silicon"
                        .into(),
                env_path: None,
            });
        }

        DoctorReport {
            version: env!("CARGO_PKG_VERSION"),
            paths: self.paths.clone(),
            ffmpeg: find_on_path("ffmpeg"),
            ffprobe: find_on_path("ffprobe"),
            helper: self.helper_path.clone(),
            helper_exists: self.helper_path.exists(),
            uv,
            runtime_smokes,
        }
    }

    pub fn doctor_view(&self) -> DoctorView {
        let report = self.doctor();
        let runtime_lines: Vec<String> = report
            .runtime_smokes
            .iter()
            .map(|s| {
                format!(
                    "{}: {} ({})",
                    s.family,
                    if s.ok { "ok" } else { "fail" },
                    s.detail
                )
            })
            .collect();
        DoctorView {
            version: report.version.into(),
            home: report.paths.home.display().to_string(),
            database: report.paths.db_path.display().to_string(),
            ffmpeg: report.ffmpeg.map(|path| path.display().to_string()),
            ffprobe: report.ffprobe.map(|path| path.display().to_string()),
            helper: report.helper.display().to_string(),
            uv: report.uv.map(|p| p.display().to_string()),
            helper_exists: report.helper_exists,
            runtime_smokes: runtime_lines,
        }
    }
}

fn smoke_python_family(
    family: EngineFamily,
    envs_root: &std::path::Path,
    runtimes_root: &std::path::Path,
    uv: Option<&PathBuf>,
    module: &str,
    run_import: bool,
) -> RuntimeSmoke {
    let family_name = family.as_str().to_string();
    let lock_hash = match lock_hash_for_family(runtimes_root, family) {
        Ok(h) => h,
        Err(e) => {
            return RuntimeSmoke {
                family: family_name,
                ok: false,
                detail: format!("RUNTIME_UNAVAILABLE: lock hash: {e}"),
                env_path: None,
            };
        }
    };
    let env_root = envs_root.join(family.as_str()).join(&lock_hash);
    let python = {
        #[cfg(windows)]
        {
            env_root.join("Scripts").join("python.exe")
        }
        #[cfg(not(windows))]
        {
            env_root.join("bin").join("python")
        }
    };
    if !python.is_file() {
        let hint = match uv {
            Some(u) => format!(
                "managed env missing at {}; provision with uv ({})",
                env_root.display(),
                u.display()
            ),
            None => format!(
                "managed env missing at {} and uv not found (set VIDEOCAPTIONERR_UV)",
                env_root.display()
            ),
        };
        return RuntimeSmoke {
            family: family_name,
            ok: false,
            detail: format!("RUNTIME_SMOKE_TEST_FAILED: {hint}"),
            env_path: Some(env_root.display().to_string()),
        };
    }
    if !run_import {
        return RuntimeSmoke {
            family: family_name,
            ok: true,
            detail: "python present (import skipped)".into(),
            env_path: Some(env_root.display().to_string()),
        };
    }
    let code =
        format!("import {module}; import sys; print(getattr({module}, '__version__', 'ok'))");
    match Command::new(&python).args(["-c", &code]).output() {
        Ok(out) if out.status.success() => {
            let ver = String::from_utf8_lossy(&out.stdout).trim().to_string();
            RuntimeSmoke {
                family: family_name,
                ok: true,
                detail: format!("import ok version={ver}"),
                env_path: Some(env_root.display().to_string()),
            }
        }
        Ok(out) => RuntimeSmoke {
            family: family_name,
            ok: false,
            detail: format!(
                "RUNTIME_SMOKE_TEST_FAILED: import {module}: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ),
            env_path: Some(env_root.display().to_string()),
        },
        Err(e) => RuntimeSmoke {
            family: family_name,
            ok: false,
            detail: format!("RUNTIME_SMOKE_TEST_FAILED: spawn python: {e}"),
            env_path: Some(env_root.display().to_string()),
        },
    }
}

fn find_uv() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("VIDEOCAPTIONERR_UV") {
        let path = PathBuf::from(p);
        if path.is_file() {
            return Some(path);
        }
    }
    find_on_path("uv")
}

fn find_on_path(command: &str) -> Option<PathBuf> {
    std::env::var_os("PATH")?.to_str().and_then(|path| {
        std::env::split_paths(path).find_map(|dir| {
            let candidate = dir.join(command);
            candidate.is_file().then_some(candidate)
        })
    })
}
