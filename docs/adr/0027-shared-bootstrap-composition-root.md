# ADR 0027: Shared bootstrap composition root

## Status

Accepted (frozen for v1)

## Decision

videocaptionerr-bootstrap is the only crate that wires concrete adapters to
application ports. CLI and Tauri desktop use the same bootstrap facade and
must not construct SQLite, worker, HTTP, ffmpeg, or subtitle adapters
independently.

## Consequences

- Runtime configuration and platform paths are resolved at the composition
  boundary.
- CLI and GUI behavior cannot drift through duplicate orchestration.
