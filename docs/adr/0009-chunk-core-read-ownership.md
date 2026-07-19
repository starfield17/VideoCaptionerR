# ADR 0009: Silence/energy chunk cuts with core/read ownership

## Status

Accepted (frozen for v1)

## Decision

Long-audio chunking uses silence/energy cuts with contiguous non-overlapping `core` ownership and optional overlapping `read` context. After offset correction, keep words whose center falls in the chunk core. No text-based fuzzy overlap deduplication.

## Consequences

- Failed chunks do not invalidate completed chunk caches.
- Local ASR does not chunk by default unless adapter/user/memory requires it.
