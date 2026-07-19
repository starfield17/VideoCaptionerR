//! Streaming content hashes (BLAKE3).

use std::fs::File;
use std::io::Read;
use std::path::Path;

use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};

/// Stream BLAKE3 hex digest of any file without loading it fully into memory.
pub fn blake3_path(path: &Path) -> VcResult<String> {
    let mut file = File::open(path).map_err(|e| {
        VcError::new(
            ErrorCode::InputNotFound,
            format!("open {} for hash: {e}", path.display()),
        )
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).map_err(|e| {
            VcError::new(
                ErrorCode::InputNotFound,
                format!("read {} for hash: {e}", path.display()),
            )
        })?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

/// Full media hash of the original input (`media_hash`).
pub fn media_hash_file(path: &Path) -> VcResult<String> {
    blake3_path(path)
}

/// Hash of normalized PCM/WAV payload (`pcm_hash`).
pub fn pcm_hash_file(wav_path: &Path) -> VcResult<String> {
    blake3_path(wav_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn hashes_streamed_and_stable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("a.bin");
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(&[1u8; 100_000]).unwrap();
        }
        let h1 = media_hash_file(&path).unwrap();
        let h2 = media_hash_file(&path).unwrap();
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64);
    }
}
