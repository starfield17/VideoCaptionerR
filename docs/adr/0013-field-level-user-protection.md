# ADR 0013: Field-level user protection and revisions

## Status

Accepted (frozen for v1)

## Decision

Persist field origins for text/translation. User-edited translation blocks later automatic overwrite of that field. Async LLM results bind to Transcript revision, cue ID, and field revision; stale results are discarded as `STALE_RESULT`.

## Consequences

- Immutable revision history with latest pointer.
- No full revision per keystroke; edit transactions create revisions.
