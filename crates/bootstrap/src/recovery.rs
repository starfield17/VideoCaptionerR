use std::path::PathBuf;
use std::sync::Arc;

use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_core::application_error::ApplicationError;
use videocaptionerr_core::use_cases::{RecoveryReport, StartupRecovery};

pub(crate) fn run_startup_recovery_sync(
    recovery: Arc<StartupRecovery>,
    roots: Vec<PathBuf>,
) -> VcResult<RecoveryReport> {
    let join = std::thread::Builder::new()
        .name("videocaptionerr-startup-recovery".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| {
                    VcError::new(
                        ErrorCode::Internal,
                        format!("create startup recovery runtime: {error}"),
                    )
                })?;
            runtime
                .block_on(recovery.execute(roots))
                .map_err(ApplicationError::into_vc_error)
        })
        .map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("spawn startup recovery: {error}"),
            )
        })?;
    join.join()
        .map_err(|_| VcError::new(ErrorCode::Internal, "startup recovery thread panicked"))?
}
