# ADR 0020: Batch-terminal model unload

## Status

Accepted (frozen for v1)

## Decision

Model session lifetime is the Batch: load once at Batch start, reuse for every Job, unload immediately when Batch reaches Done/Failed/Cancelled. Worker process lifetime is separate; a model-free Ready process MAY remain without major model memory.

## Consequences

- Single Job is a one-Job Batch.
- If one Job fails but Batch continues, keep model loaded until Batch terminal state.
