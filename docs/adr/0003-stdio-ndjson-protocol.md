# ADR 0003: Versioned stdio NDJSON protocol

## Status

Accepted (frozen for v1)

## Decision

ASR workers and the whisper.cpp helper communicate with the main process via versioned NDJSON on stdio (not localhost HTTP). stdout is protocol-only; stderr is logs. One active transcription per worker.

## Consequences

- Protocol pollution is a deterministic error and restarts the worker.
- Heartbeats use the control path (`ping`) during inference.
- Cancellation escalates to process-tree kill after grace.
