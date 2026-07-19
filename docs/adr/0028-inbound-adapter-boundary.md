# ADR 0028: Inbound adapters cannot bypass application services

## Status

Accepted (frozen for v1)

## Decision

CLI and desktop code only parse commands, render results, and subscribe to
typed events. They MUST NOT execute SQL, call HTTP or ASR clients, spawn
ffmpeg/processes, write subtitle artifacts, or mutate aggregate fields
directly.

## Consequences

- Job queries and deletion become application use cases.
- Error exit codes come from typed application/contract errors, never message
  matching.
- Architecture checks can enforce the boundary mechanically.
