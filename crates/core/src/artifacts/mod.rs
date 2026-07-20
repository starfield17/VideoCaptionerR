//! Application-owned durable artifact document types.

pub mod media;

pub use media::{
    ExtractManifest, ProbeManifest, EXTRACT_MANIFEST_SCHEMA_VERSION, PROBE_MANIFEST_SCHEMA_VERSION,
};
