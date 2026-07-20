//! Application-owned structured repair loop (M3 "generic agent loop").
//!
//! This is deliberately small: after JSON/semantic validation fails, append a
//! corrective user message and re-call the provider. It is not an autonomous
//! multi-tool agent. Business policy (budgets, isolation) stays here; HTTP
//! stays in the llm adapter.

use crate::ports::{LlmMessage, LlmRole};

/// Build the next repair turn after a semantic validation failure.
pub fn repair_user_message(violation: &str) -> LlmMessage {
    LlmMessage {
        role: LlmRole::User,
        content: format!(
            "The previous response was rejected. Fix this violation and return only the requested JSON: {violation}"
        ),
    }
}

/// Semantic validation retry budget: `retries` means that many repairs after
/// the first attempt (manual: max two semantic retries → 3 attempts total when
/// retries=2).
pub fn max_semantic_attempts(retries: u32) -> u32 {
    retries.saturating_add(1)
}

/// Transport auto-retry budget: two retries after first attempt → 3 total.
pub const MAX_TRANSPORT_ATTEMPTS: u32 = 3;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repair_message_marks_violation_without_provider_jargon() {
        let msg = repair_user_message("cue 1 empty");
        assert!(matches!(msg.role, LlmRole::User));
        assert!(msg.content.contains("cue 1 empty"));
        assert!(msg.content.contains("JSON"));
    }

    #[test]
    fn semantic_budget_matches_manual_two_retries() {
        assert_eq!(max_semantic_attempts(2), 3);
        assert_eq!(max_semantic_attempts(0), 1);
    }
}
