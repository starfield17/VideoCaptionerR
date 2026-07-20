//! whisper.cpp engine — only linked into the helper process (never the main app).
//!
//! Uses a thin C shim (`whisper_shim.c`) over the vendored whisper.cpp C API when
//! built with `--features whisper-cpp`.

use std::path::Path;
use std::sync::Mutex;

use videocaptionerr_contracts::protocol::SegmentWord;

#[cfg(whisper_cpp_linked)]
use crate::audio::load_pcm16k;

pub struct LoadedModel {
    #[cfg(whisper_cpp_linked)]
    ctx: *mut ffi::vc_whisper_ctx,
    #[cfg(not(whisper_cpp_linked))]
    _path: std::path::PathBuf,
}

#[cfg(whisper_cpp_linked)]
unsafe impl Send for LoadedModel {}

static LOADED: Mutex<Option<LoadedModel>> = Mutex::new(None);

#[cfg(whisper_cpp_linked)]
mod ffi {
    use std::os::raw::{c_char, c_float, c_int};

    #[repr(C)]
    pub struct vc_whisper_ctx {
        _private: [u8; 0],
    }

    extern "C" {
        pub fn vc_whisper_load(path: *const c_char) -> *mut vc_whisper_ctx;
        pub fn vc_whisper_free(ctx: *mut vc_whisper_ctx);
        pub fn vc_whisper_full(
            ctx: *mut vc_whisper_ctx,
            samples: *const c_float,
            n_samples: c_int,
            n_threads: c_int,
            language: *const c_char,
            detect_language: c_int,
        ) -> c_int;
        pub fn vc_whisper_n_segments(ctx: *mut vc_whisper_ctx) -> c_int;
        pub fn vc_whisper_n_tokens(ctx: *mut vc_whisper_ctx, i_segment: c_int) -> c_int;
        pub fn vc_whisper_token_text(
            ctx: *mut vc_whisper_ctx,
            i_segment: c_int,
            i_token: c_int,
        ) -> *const c_char;
        pub fn vc_whisper_token_times(
            ctx: *mut vc_whisper_ctx,
            i_segment: c_int,
            i_token: c_int,
            t0: *mut i64,
            t1: *mut i64,
            prob: *mut c_float,
        );
        pub fn vc_whisper_segment_text(ctx: *mut vc_whisper_ctx, i_segment: c_int)
            -> *const c_char;
        pub fn vc_whisper_segment_t0(ctx: *mut vc_whisper_ctx, i_segment: c_int) -> i64;
        pub fn vc_whisper_segment_t1(ctx: *mut vc_whisper_ctx, i_segment: c_int) -> i64;
    }
}

pub fn load(path: &Path) -> anyhow::Result<()> {
    if !path.is_file() {
        anyhow::bail!("model not found: {}", path.display());
    }
    #[cfg(whisper_cpp_linked)]
    {
        use std::ffi::CString;
        let c_path = CString::new(path.to_string_lossy().as_bytes())?;
        unsafe {
            let ctx = ffi::vc_whisper_load(c_path.as_ptr());
            if ctx.is_null() {
                anyhow::bail!("whisper.cpp failed to load {}", path.display());
            }
            *LOADED.lock().unwrap() = Some(LoadedModel { ctx });
        }
        Ok(())
    }
    #[cfg(not(whisper_cpp_linked))]
    {
        *LOADED.lock().unwrap() = Some(LoadedModel {
            _path: path.to_path_buf(),
        });
        Ok(())
    }
}

pub fn unload() {
    #[cfg(whisper_cpp_linked)]
    {
        if let Some(loaded) = LOADED.lock().unwrap().take() {
            unsafe {
                ffi::vc_whisper_free(loaded.ctx);
            }
        }
    }
    #[cfg(not(whisper_cpp_linked))]
    {
        *LOADED.lock().unwrap() = None;
    }
}

#[allow(dead_code)]
pub fn is_loaded() -> bool {
    LOADED.lock().unwrap().is_some()
}

pub fn runtime_version() -> String {
    #[cfg(whisper_cpp_linked)]
    {
        format!("whisper-cpp-c-api+{}", env!("CARGO_PKG_VERSION"))
    }
    #[cfg(not(whisper_cpp_linked))]
    {
        "whisper-cpp-stub".into()
    }
}

pub fn transcribe(
    audio: &Path,
    language: Option<&str>,
    cancel: &dyn Fn() -> bool,
) -> anyhow::Result<(u64, Vec<SegmentWord>, String)> {
    if cancel() {
        anyhow::bail!("cancelled");
    }
    #[cfg(whisper_cpp_linked)]
    {
        transcribe_real(audio, language, cancel)
    }
    #[cfg(not(whisper_cpp_linked))]
    {
        let _ = (audio, language, cancel);
        anyhow::bail!(
            "whisper-cpp engine not linked into this helper; rebuild with --features whisper-cpp and vendor/whisper.cpp"
        );
    }
}

#[cfg(whisper_cpp_linked)]
fn transcribe_real(
    audio: &Path,
    language: Option<&str>,
    cancel: &dyn Fn() -> bool,
) -> anyhow::Result<(u64, Vec<SegmentWord>, String)> {
    use std::ffi::{CStr, CString};

    let guard = LOADED.lock().unwrap();
    let loaded = guard
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("model not loaded"))?;
    let (pcm, duration_ms) = load_pcm16k(audio)?;
    if cancel() {
        anyhow::bail!("cancelled");
    }

    let lang_c = CString::new(language.unwrap_or("en"))?;
    let threads = std::thread::available_parallelism()
        .map(|n| n.get() as i32)
        .unwrap_or(4);
    unsafe {
        let rc = ffi::vc_whisper_full(
            loaded.ctx,
            pcm.as_ptr(),
            pcm.len() as i32,
            threads,
            lang_c.as_ptr(),
            if language.is_none() { 1 } else { 0 },
        );
        if rc != 0 {
            anyhow::bail!("whisper_full failed with code {rc}");
        }
        if cancel() {
            anyhow::bail!("cancelled");
        }

        let mut words = Vec::new();
        let n_segments = ffi::vc_whisper_n_segments(loaded.ctx);
        for i in 0..n_segments {
            let n_tokens = ffi::vc_whisper_n_tokens(loaded.ctx, i);
            for t in 0..n_tokens {
                let text_ptr = ffi::vc_whisper_token_text(loaded.ctx, i, t);
                if text_ptr.is_null() {
                    continue;
                }
                let text = CStr::from_ptr(text_ptr).to_string_lossy();
                let text = text.trim();
                if text.is_empty() || text.starts_with('[') || text.starts_with('<') {
                    continue;
                }
                let mut t0 = 0i64;
                let mut t1 = 0i64;
                let mut prob = -1.0f32;
                ffi::vc_whisper_token_times(loaded.ctx, i, t, &mut t0, &mut t1, &mut prob);
                let start_ms = (t0.max(0) as u64) * 10;
                let end_ms = (t1.max(t0).max(0) as u64) * 10;
                words.push(SegmentWord {
                    text: text.to_string(),
                    start_ms,
                    end_ms: end_ms.max(start_ms + 1),
                    prob: if prob.is_finite() { prob } else { -1.0 },
                });
            }
        }

        if words.is_empty() {
            for i in 0..n_segments {
                let text_ptr = ffi::vc_whisper_segment_text(loaded.ctx, i);
                if text_ptr.is_null() {
                    continue;
                }
                let text = CStr::from_ptr(text_ptr).to_string_lossy();
                let text = text.trim();
                if text.is_empty() {
                    continue;
                }
                let t0 = ffi::vc_whisper_segment_t0(loaded.ctx, i).max(0) as u64 * 10;
                let t1 = ffi::vc_whisper_segment_t1(loaded.ctx, i).max(0) as u64 * 10;
                words.push(SegmentWord {
                    text: text.to_string(),
                    start_ms: t0,
                    end_ms: t1.max(t0 + 1),
                    prob: -1.0,
                });
            }
        }

        let lang = language.unwrap_or("en").to_string();
        let end = words.last().map(|w| w.end_ms).unwrap_or(duration_ms);
        Ok((duration_ms.max(end), words, lang))
    }
}
