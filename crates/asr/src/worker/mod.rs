//! NDJSON worker/helper client over stdio.

mod client;
mod control;
mod process;
mod protocol_session;

#[cfg(test)]
mod tests;

pub use client::{
    WorkerClient, CANCEL_GRACE, FIRST_SEGMENT_TIMEOUT, INTER_SEGMENT_TIMEOUT, LOAD_TIMEOUT,
    SHUTDOWN_TIMEOUT, STARTUP_TIMEOUT,
};
pub use control::WorkerControl;
pub use process::{kill_process_tree, resolve_helper_binary};
pub use protocol_session::WorkerProtocolSession;
