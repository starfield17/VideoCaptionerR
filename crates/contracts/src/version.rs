//! Schema and protocol version constants.

/// Major schema version for persisted documents and external contracts.
pub const SCHEMA_VERSION: u32 = 1;

/// Crate version string for producer fingerprints.
pub const CONTRACTS_VERSION: &str = env!("CARGO_PKG_VERSION");
