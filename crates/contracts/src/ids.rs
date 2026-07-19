//! Public identifiers (ULID strings).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::error::{ErrorCode, VcError};

/// Newtype wrapper around a ULID string for public identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UlidStr(String);

impl UlidStr {
    pub fn new() -> Self {
        Self(Ulid::new().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl Default for UlidStr {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for UlidStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for UlidStr {
    type Err = VcError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ulid::from_string(s)
            .map_err(|_| VcError::new(ErrorCode::InvalidArgument, format!("invalid ULID: {s}")))?;
        Ok(Self(s.to_string()))
    }
}

impl From<Ulid> for UlidStr {
    fn from(value: Ulid) -> Self {
        Self(value.to_string())
    }
}

impl AsRef<str> for UlidStr {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Job identifier.
pub type JobId = UlidStr;

/// Worker/helper session identifier.
pub type SessionId = UlidStr;

/// Monotonic-in-session request id (numeric in protocol, ULID optional externally).
pub type RequestId = u64;
