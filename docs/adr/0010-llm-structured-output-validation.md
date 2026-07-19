# ADR 0010: LLM structured-output fallback and business validation

## Status

Accepted (frozen for v1)

## Decision

Prefer JsonSchema → JsonObject → PromptOnly + tolerant JSON parse. Provider-declared schema support never replaces business validation (key sets, similarity, residue heuristics). Failed batches use binary isolation down to per-cue fallback with `llm_failed`.

## Consequences

- One reusable agent loop for split/correct/translate.
- No cost/request budgets in v1.
