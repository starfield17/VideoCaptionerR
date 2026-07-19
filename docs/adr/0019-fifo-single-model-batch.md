# ADR 0019: FIFO single-model Batch scheduling

## Status

Accepted (frozen for v1)

## Decision

One Batch freezes one ASR engine/model/device/compute profile. Scheduling is Batch FIFO, Job FIFO within Batch, stage dependency order, resource-lane concurrency. No dynamic model affinity, fairness aging, or mid-Batch model switching.

## Consequences

- Per-Job ASR model overrides are forbidden.
- Pipeline overlap across stages remains allowed under resource lanes.
