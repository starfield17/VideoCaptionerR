//! Provider reliability circuit breaker (no cost budgets).

use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};

use crate::provider::{ChatRequest, ChatResponse, LlmProvider, ProviderCapabilities};

/// Default open duration after repeated 429/5xx failures.
pub const DEFAULT_OPEN_SECS: u64 = 60;

/// Consecutive recoverable failures before opening the circuit.
pub const DEFAULT_FAILURE_THRESHOLD: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

struct Inner {
    state: CircuitState,
    consecutive_failures: u32,
    opened_at: Option<Instant>,
    open_for: Duration,
    failure_threshold: u32,
    /// When auth fails, stay open until manual reset (not just a timer).
    auth_locked: bool,
    retry_after: Option<Instant>,
}

/// Per-provider circuit breaker. Authentication errors lock immediately.
pub struct CircuitBreaker {
    provider_id: String,
    inner: Mutex<Inner>,
}

/// Decorates a provider with the per-provider circuit and Retry-After policy.
/// The wrapped provider remains responsible for HTTP/runtime behavior; this
/// type only controls whether a request may start and records its outcome.
pub struct CircuitLlmProvider {
    inner: Arc<dyn LlmProvider>,
    breaker: Arc<CircuitBreaker>,
}

impl CircuitLlmProvider {
    pub fn new(inner: Arc<dyn LlmProvider>, breaker: Arc<CircuitBreaker>) -> Self {
        Self { inner, breaker }
    }

    pub fn breaker(&self) -> &Arc<CircuitBreaker> {
        &self.breaker
    }

    pub fn provider(&self) -> &Arc<dyn LlmProvider> {
        &self.inner
    }
}

#[async_trait]
impl LlmProvider for CircuitLlmProvider {
    fn id(&self) -> &str {
        self.inner.id()
    }

    fn model(&self) -> &str {
        self.inner.model()
    }

    fn capabilities(&self) -> &ProviderCapabilities {
        self.inner.capabilities()
    }

    async fn chat(&self, request: &ChatRequest) -> VcResult<ChatResponse> {
        self.breaker.guard()?;
        match self.inner.chat(request).await {
            Ok(response) => {
                self.breaker.record_success();
                Ok(response)
            }
            Err(error) => {
                let retry_after = error.retry_after_ms.map(Duration::from_millis);
                self.breaker.record_failure(error.code, retry_after);
                Err(error)
            }
        }
    }
}

impl CircuitBreaker {
    pub fn new(provider_id: impl Into<String>) -> Self {
        Self {
            provider_id: provider_id.into(),
            inner: Mutex::new(Inner {
                state: CircuitState::Closed,
                consecutive_failures: 0,
                opened_at: None,
                open_for: Duration::from_secs(DEFAULT_OPEN_SECS),
                failure_threshold: DEFAULT_FAILURE_THRESHOLD,
                auth_locked: false,
                retry_after: None,
            }),
        }
    }

    pub fn with_open_duration(self, d: Duration) -> Self {
        self.inner.lock().unwrap().open_for = d;
        self
    }

    pub fn provider_id(&self) -> &str {
        &self.provider_id
    }

    pub fn state(&self) -> CircuitState {
        let mut g = self.inner.lock().unwrap();
        self.refresh_locked(&mut g);
        g.state
    }

    /// Check whether a new request may proceed.
    pub fn guard(&self) -> VcResult<()> {
        let mut g = self.inner.lock().unwrap();
        self.refresh_locked(&mut g);
        if g.auth_locked {
            return Err(VcError::new(
                ErrorCode::LlmAuthFailed,
                format!(
                    "provider {} circuit locked after authentication failure",
                    self.provider_id
                ),
            ));
        }
        if let Some(until) = g.retry_after {
            if Instant::now() < until {
                return Err(VcError::new(
                    ErrorCode::LlmRateLimited,
                    format!(
                        "provider {} honoring Retry-After until {:?}",
                        self.provider_id, until
                    ),
                ));
            }
            g.retry_after = None;
        }
        match g.state {
            CircuitState::Open => Err(VcError::new(
                ErrorCode::LlmProviderUnavailable,
                format!(
                    "provider {} circuit open after repeated failures",
                    self.provider_id
                ),
            )),
            CircuitState::Closed | CircuitState::HalfOpen => Ok(()),
        }
    }

    /// Record a successful request.
    pub fn record_success(&self) {
        let mut g = self.inner.lock().unwrap();
        g.consecutive_failures = 0;
        g.state = CircuitState::Closed;
        g.opened_at = None;
        g.retry_after = None;
        // auth_locked is only cleared by reset()
    }

    /// Record a failure; open circuit on threshold or auth.
    pub fn record_failure(&self, code: ErrorCode, retry_after: Option<Duration>) {
        let mut g = self.inner.lock().unwrap();
        if matches!(code, ErrorCode::LlmAuthFailed) {
            g.auth_locked = true;
            g.state = CircuitState::Open;
            g.opened_at = Some(Instant::now());
            return;
        }
        if let Some(d) = retry_after {
            g.retry_after = Some(Instant::now() + d);
        }
        if matches!(
            code,
            ErrorCode::LlmRateLimited | ErrorCode::LlmProviderUnavailable
        ) {
            g.consecutive_failures = g.consecutive_failures.saturating_add(1);
            if g.consecutive_failures >= g.failure_threshold {
                g.state = CircuitState::Open;
                g.opened_at = Some(Instant::now());
            }
        }
    }

    /// Clear auth lock and reopen for requests.
    pub fn reset(&self) {
        let mut g = self.inner.lock().unwrap();
        g.auth_locked = false;
        g.consecutive_failures = 0;
        g.state = CircuitState::Closed;
        g.opened_at = None;
        g.retry_after = None;
    }

    fn refresh_locked(&self, g: &mut Inner) {
        if g.auth_locked {
            return;
        }
        if g.state == CircuitState::Open {
            if let Some(opened) = g.opened_at {
                if opened.elapsed() >= g.open_for {
                    g.state = CircuitState::HalfOpen;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_locks_immediately() {
        let cb = CircuitBreaker::new("p");
        cb.record_failure(ErrorCode::LlmAuthFailed, None);
        assert_eq!(cb.state(), CircuitState::Open);
        assert_eq!(cb.guard().unwrap_err().code, ErrorCode::LlmAuthFailed);
        cb.reset();
        assert!(cb.guard().is_ok());
    }

    #[test]
    fn repeated_5xx_opens() {
        let cb = CircuitBreaker::new("p").with_open_duration(Duration::from_millis(50));
        for _ in 0..DEFAULT_FAILURE_THRESHOLD {
            cb.record_failure(ErrorCode::LlmProviderUnavailable, None);
        }
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(cb.guard().is_err());
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        assert!(cb.guard().is_ok());
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
    }
}
