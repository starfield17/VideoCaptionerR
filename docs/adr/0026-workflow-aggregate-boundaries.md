# ADR 0026: Workflow aggregate boundaries

## Status

Accepted (frozen for v1)

## Decision

Transcript, Batch, Job, and WorkUnit are separate aggregate roots. Aggregate
mutations occur through behavior methods that validate legal transitions. A
Batch owns the frozen ASR execution profile and the model session lifetime; a
Job does not unload the model.

## Consequences

- Work-unit retries and leases do not mutate Job state directly.
- Stage completion requires a committed artifact reference.
- Terminal Batch transitions emit the signal used by the application to close
  the model session exactly once.
