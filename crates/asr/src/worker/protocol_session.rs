//! Strict inbound NDJSON session validation for worker/helper protocols.
//!
//! The session is the single authority for version, session id, sequence,
//! request ownership, and terminal-message rules. Callers must terminate the
//! process when this type returns a protocol error.

use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_contracts::protocol::{
    ProtocolEnvelope, ProtocolMessageType, PROTOCOL_VERSION, WORKER_MAX_LINE_BYTES,
};

/// Tracks one worker stdio session from hello through shutdown.
#[derive(Debug, Clone)]
pub struct WorkerProtocolSession {
    session_id: Option<String>,
    /// Next expected inbound `seq` (strictly increasing from 0).
    next_inbound_seq: u64,
    active_request_id: Option<u64>,
    active_request_terminal: bool,
    terminated: bool,
}

impl WorkerProtocolSession {
    pub fn new() -> Self {
        Self {
            session_id: None,
            next_inbound_seq: 0,
            active_request_id: None,
            active_request_terminal: false,
            terminated: false,
        }
    }

    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    pub fn active_request_id(&self) -> Option<u64> {
        self.active_request_id
    }

    pub fn is_terminated(&self) -> bool {
        self.terminated
    }

    /// Begin tracking a request that expects a single terminal response.
    pub fn begin_request(&mut self, request_id: u64) -> VcResult<()> {
        self.ensure_open()?;
        if self.active_request_id.is_some() && !self.active_request_terminal {
            return Err(protocol_error("worker already has an active request"));
        }
        self.active_request_id = Some(request_id);
        self.active_request_terminal = false;
        Ok(())
    }

    /// Clear active request tracking after a successful terminal or abandon.
    pub fn end_request(&mut self) {
        self.active_request_id = None;
        self.active_request_terminal = false;
    }

    /// Parse and validate one raw stdout line. Oversized lines, dirty JSON,
    /// unknown types, seq regressions, cross-request traffic, and post-terminal
    /// messages all terminate the session.
    pub fn accept_line(&mut self, line: &str) -> VcResult<ProtocolEnvelope> {
        self.ensure_open()?;
        if line.len() > WORKER_MAX_LINE_BYTES {
            return self.fail("helper line exceeds 4 MiB");
        }
        let env = ProtocolEnvelope::from_ndjson_line(line).map_err(|error| {
            self.terminated = true;
            VcError::new(
                ErrorCode::WorkerProtocolError,
                format!("protocol pollution / invalid json: {error}"),
            )
        })?;
        self.accept_envelope(env)
    }

    pub fn accept_envelope(&mut self, env: ProtocolEnvelope) -> VcResult<ProtocolEnvelope> {
        self.ensure_open()?;
        if env.protocol_version != PROTOCOL_VERSION {
            return self.fail(format!(
                "protocol_version mismatch: got {}, want {}",
                env.protocol_version, PROTOCOL_VERSION
            ));
        }

        if env.seq != self.next_inbound_seq {
            return self.fail(format!(
                "inbound seq must be strictly increasing: expected {}, got {}",
                self.next_inbound_seq, env.seq
            ));
        }
        self.next_inbound_seq = self.next_inbound_seq.saturating_add(1);

        let typed = match env.typed() {
            Some(value) => value,
            None => {
                return self.fail(format!("unknown message type: {}", env.msg_type));
            }
        };

        match typed {
            ProtocolMessageType::HelloOk => {
                if self.session_id.is_some() {
                    return self.fail("duplicate hello_ok for session");
                }
                if env.session_id.is_empty() {
                    return self.fail("hello_ok session_id is empty");
                }
                self.session_id = Some(env.session_id.clone());
                return Ok(env);
            }
            ProtocolMessageType::Pong => {
                // Control-path heartbeat. May omit request_id.
                self.require_stable_session(&env)?;
                return Ok(env);
            }
            _ => {}
        }

        self.require_stable_session(&env)?;

        if let Some(request_id) = env.request_id {
            if request_id == 0 {
                return self.fail("request_id must be positive");
            }
            match self.active_request_id {
                Some(active) if active == request_id => {
                    if self.active_request_terminal {
                        return self.fail(format!(
                            "message after terminal for request {request_id}"
                        ));
                    }
                }
                Some(active) => {
                    // Cross-request traffic is a hard protocol error. Silently
                    // ignoring other-request messages is forbidden.
                    return self.fail(format!(
                        "message for request {request_id} while request {active} is active"
                    ));
                }
                None => {
                    // Accept control responses that carry a request id when no
                    // transcription is active (load/unload/ping/shutdown).
                }
            }
        } else if matches!(
            typed,
            ProtocolMessageType::Progress
                | ProtocolMessageType::Segment
                | ProtocolMessageType::Language
                | ProtocolMessageType::Result
                | ProtocolMessageType::Error
                | ProtocolMessageType::Cancelled
        ) {
            return self.fail(format!(
                "{} message requires request_id",
                typed.as_str()
            ));
        }

        if typed.is_terminal() {
            if let Some(active) = self.active_request_id {
                if env.request_id != Some(active) {
                    return self.fail(format!(
                        "terminal for request {:?} while active request is {active}",
                        env.request_id
                    ));
                }
                if self.active_request_terminal {
                    return self.fail(format!("duplicate terminal for request {active}"));
                }
                self.active_request_terminal = true;
            }
        }

        Ok(env)
    }

    fn require_stable_session(&self, env: &ProtocolEnvelope) -> VcResult<()> {
        let Some(expected) = &self.session_id else {
            return Err(protocol_error(
                "session has no established session_id before non-hello message",
            ));
        };
        if &env.session_id != expected {
            return Err(protocol_error(format!(
                "session_id mismatch: got {}, want {expected}",
                env.session_id
            )));
        }
        Ok(())
    }

    fn ensure_open(&self) -> VcResult<()> {
        if self.terminated {
            Err(protocol_error("worker protocol session already terminated"))
        } else {
            Ok(())
        }
    }

    fn fail(&mut self, message: impl Into<String>) -> VcResult<ProtocolEnvelope> {
        self.terminated = true;
        Err(protocol_error(message))
    }
}

impl Default for WorkerProtocolSession {
    fn default() -> Self {
        Self::new()
    }
}

fn protocol_error(message: impl Into<String>) -> VcError {
    VcError::new(ErrorCode::WorkerProtocolError, message.into())
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use videocaptionerr_contracts::protocol::ProtocolEnvelope;

    fn env(
        session: &str,
        request_id: Option<u64>,
        seq: u64,
        msg_type: ProtocolMessageType,
    ) -> ProtocolEnvelope {
        ProtocolEnvelope::new(session, request_id, seq, msg_type, None)
    }

    #[test]
    fn rejects_wrong_protocol_version() {
        let mut session = WorkerProtocolSession::new();
        let mut message = env("s", None, 0, ProtocolMessageType::HelloOk);
        message.protocol_version = 99;
        assert_eq!(
            session.accept_envelope(message).unwrap_err().code,
            ErrorCode::WorkerProtocolError
        );
        assert!(session.is_terminated());
    }

    #[test]
    fn rejects_non_monotonic_seq() {
        let mut session = WorkerProtocolSession::new();
        session
            .accept_envelope(env("s1", None, 0, ProtocolMessageType::HelloOk))
            .unwrap();
        let error = session
            .accept_envelope(env("s1", None, 0, ProtocolMessageType::Pong))
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::WorkerProtocolError);
    }

    #[test]
    fn rejects_session_id_change() {
        let mut session = WorkerProtocolSession::new();
        session
            .accept_envelope(env("s1", None, 0, ProtocolMessageType::HelloOk))
            .unwrap();
        let error = session
            .accept_envelope(env("s2", None, 1, ProtocolMessageType::Pong))
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::WorkerProtocolError);
    }

    #[test]
    fn rejects_cross_request_and_duplicate_terminal() {
        let mut session = WorkerProtocolSession::new();
        session
            .accept_envelope(env("s1", None, 0, ProtocolMessageType::HelloOk))
            .unwrap();
        session.begin_request(7).unwrap();
        let error = session
            .accept_envelope(env(
                "s1",
                Some(8),
                1,
                ProtocolMessageType::Segment,
            ))
            .unwrap_err();
        assert!(error.message.contains("while request 7 is active"));

        let mut session = WorkerProtocolSession::new();
        session
            .accept_envelope(env("s1", None, 0, ProtocolMessageType::HelloOk))
            .unwrap();
        session.begin_request(7).unwrap();
        session
            .accept_envelope(env("s1", Some(7), 1, ProtocolMessageType::Result))
            .unwrap();
        let error = session
            .accept_envelope(env("s1", Some(7), 2, ProtocolMessageType::Result))
            .unwrap_err();
        assert!(error.message.contains("duplicate terminal") || error.message.contains("after terminal"));
    }

    #[test]
    fn rejects_unknown_type_and_dirty_line() {
        let mut session = WorkerProtocolSession::new();
        session
            .accept_envelope(env("s1", None, 0, ProtocolMessageType::HelloOk))
            .unwrap();
        let mut unknown = env("s1", None, 1, ProtocolMessageType::Pong);
        unknown.msg_type = "not_a_type".into();
        assert!(session.accept_envelope(unknown).is_err());

        let mut session = WorkerProtocolSession::new();
        assert!(session.accept_line("not-json\n").is_err());
        assert!(session.is_terminated());
    }

    #[test]
    fn rejects_oversized_line() {
        let mut session = WorkerProtocolSession::new();
        let line = "x".repeat(WORKER_MAX_LINE_BYTES + 1);
        assert!(session.accept_line(&line).is_err());
    }
}
