//! Outbound adapters for operating-system and file-based services.

pub mod config;
pub mod constants;
pub mod instance_lock;
pub mod llm_log;
pub mod media;
pub mod media_gateway;
pub mod paths;
pub mod processing;
pub mod subtitle_gateway;
pub mod subtitle_io;
pub mod vad_silero;

pub use config::{
    AppConfig, LlmCapabilityOverride, LlmProviderConfig, ProfileConfig, ResolvedProfile,
};
pub use instance_lock::{InstanceLock, LockOwner};
pub use llm_log::FileLlmRequestRecorder;
pub use media::{
    extract_audio_wav, media_hash_file, pcm_hash_file, probe_media, select_audio_stream,
    ExtractOptions,
};
pub use media_gateway::FfmpegMediaGateway;
pub use paths::{sanitize_stem, AppPaths};
pub use processing::{
    AppJobWorkspace, LocalMediaFileCatalog, LocalSubtitleImporter, PlatformOutputPlanner,
};
pub use subtitle_gateway::FileSubtitleGateway;
pub use subtitle_io::{
    import_srt, import_vtt, preflight_export, write_ass, write_srt, write_vtt, ConflictPolicy,
    ExportFormat, ExportLayout, ExportOptions, ImportLayout, ImportOptions, OutputPlanner,
};
