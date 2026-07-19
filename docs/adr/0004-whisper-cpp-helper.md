# ADR 0004: whisper.cpp isolated Rust helper

## Status

Accepted (frozen for v1)

## Decision

whisper.cpp runs in an isolated Rust helper process behind the same envelope concepts as Python workers, not in-process in the main application.

## Consequences

- Stuck inference cannot freeze the main process.
- Cancellation can kill the process tree.
- Crashes are isolated; artifact commit stays consistent with Python workers.
