# ADR 0015: Directly editable prompts with stage snapshots

## Status

Accepted (frozen for v1)

## Decision

Prompt files are editable on disk under `prompts/`. At LLM stage start, read/normalize/hash the bundle, copy into an immutable Job stage snapshot, and execute all units from that snapshot. Prompt-bundle hash participates in cache keys.

## Consequences

- Mid-stage prompt edits do not affect in-flight units.
- No DB/GUI requirement to edit prompts.
