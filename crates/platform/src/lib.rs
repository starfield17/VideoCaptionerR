//! Outbound adapters for operating-system and file-based services.
//!
//! The initial migration keeps the existing, tested core implementations
//! behind these adapters. Later DDD tasks move their concrete modules here
//! without changing application ports.

pub mod media_gateway;
pub mod subtitle_gateway;

pub use media_gateway::FfmpegMediaGateway;
pub use subtitle_gateway::FileSubtitleGateway;
