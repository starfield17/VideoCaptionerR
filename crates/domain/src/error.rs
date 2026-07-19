//! Errors raised when a domain invariant or state transition is invalid.

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DomainError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("invalid timestamp: {0}")]
    TimestampInvalid(String),
    #[error("illegal {aggregate} transition from {from} to {to}")]
    IllegalTransition {
        aggregate: &'static str,
        from: String,
        to: String,
    },
    #[error("stale result: expected revision {expected}, actual revision {actual}")]
    StaleRevision { expected: u64, actual: u64 },
    #[error("batch execution profile does not match the existing profile")]
    BatchProfileMismatch,
    #[error("{aggregate} is already terminal")]
    AlreadyTerminal { aggregate: &'static str },
    #[error("{aggregate} member was not found: {id}")]
    MemberNotFound { aggregate: &'static str, id: String },
    #[error("work unit lease conflict: {0}")]
    LeaseConflict(String),
    #[error("work unit has no active lease")]
    LeaseRequired,
}

pub type DomainResult<T> = Result<T, DomainError>;
