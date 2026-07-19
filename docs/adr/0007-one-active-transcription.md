# ADR 0007: One active transcription per worker

## Status

Accepted (frozen for v1)

## Decision

Each ASR worker/helper accepts at most one concurrent transcription. Additional load/transcribe requests return `WORKER_BUSY`. Control messages (`hello`, `ping`, `cancel`, `shutdown`) remain accepted during inference.

## Consequences

- Simplifies cancellation and protocol state.
- Model reuse is Batch-scoped, not multi-request concurrent.
