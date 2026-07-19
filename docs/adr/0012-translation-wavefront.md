# ADR 0012: Same-Job translation wavefront

## Status

Accepted (frozen for v1)

## Decision

Default translation context includes accepted translations from the previous batch. Batches within one Job execute in order (wavefront). Different Jobs may share the Provider semaphore concurrently. Context cues are read-only and not part of the expected output-key set.

## Consequences

- Same-Job batch N waits for N-1 acceptance.
- Cross-Job concurrency remains resource-lane limited.
