//! Application-layer failure categories and explicit boundary mapping.

use thiserror::Error;
use videocaptionerr_contracts::error::{ErrorCode, VcError};
use videocaptionerr_domain::error::DomainError;

#[derive(Debug, Error)]
pub enum ApplicationError {
    #[error("{0}")]
    Domain(#[from] DomainError),
    #[error("{0}")]
    Adapter(#[from] VcError),
    #[error("operation cancelled")]
    Cancelled,
    #[error("primary operation failed: {primary}; state persistence failed: {state}")]
    StatePersistence {
        primary: Box<VcError>,
        state: Box<VcError>,
    },
    #[error("{0}")]
    Invalid(String),
}

pub type AppResult<T> = Result<T, ApplicationError>;

impl ApplicationError {
    pub fn into_vc_error(self) -> VcError {
        match self {
            Self::Domain(error) => error.into(),
            Self::Adapter(error) => error,
            Self::Cancelled => VcError::new(ErrorCode::Cancelled, "operation cancelled"),
            Self::StatePersistence { primary, state } => (*primary).clone().with_detail(format!(
                "state persistence failed: {}: {}",
                state.code, state.message
            )),
            Self::Invalid(message) => VcError::new(ErrorCode::InvalidArgument, message),
        }
    }
}
