# ADR 0011: Automatic Provider capability detection with manual override

## Status

Accepted (frozen for v1)

## Decision

Automatic capability probes run on Test Connection, profile create/change, and explicit re-detect. Manual override > cached probe > conservative default. Normal batches do not probe every request. API keys are not hashed into probe identity.

## Consequences

- Templates (Generic/Ollama/LM Studio) prefill fields but are not guarantees.
- Optional capability failure degrades that capability only.
