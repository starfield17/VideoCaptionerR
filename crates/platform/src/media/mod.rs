//! Media probing, hashing, and audio extraction.

pub mod extract;
pub mod hash;
pub mod probe;

pub use extract::{extract_audio_wav, ExtractOptions};
pub use hash::{blake3_path, media_hash_file, pcm_hash_file};
pub use probe::{find_ffprobe, probe_media, select_audio_stream};
