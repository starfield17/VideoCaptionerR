# ADR 0002: Transcript word timeline is immutable

## Status

Accepted (frozen for v1)

## Decision

ASR-derived `words` are created once by normalization and never modified by LLM stages. Cue times for ASR timelines always derive from word ranges. Re-splitting only changes cue ranges and bumps Transcript revision.

## Consequences

- LLM stages receive no mutable timeline fields.
- Manual split/merge operate on ranges/IDs, not free-form times.
- Export is a pure function of IR + options.
