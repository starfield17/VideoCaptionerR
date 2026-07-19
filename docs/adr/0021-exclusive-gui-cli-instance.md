# ADR 0021: Exclusive GUI/CLI processing instance

## Status

Accepted (frozen for v1)

## Decision

GUI and CLI processing are mutually exclusive via an application-level lock. No dual schedulers, concurrent work-unit leasing, or mutating IPC between GUI and CLI in v1. Shared application services still own all business logic.

## Consequences

- CLI returns `INSTANCE_BUSY` when GUI holds the lock and vice versa.
- Read-only commands MAY be allowed individually if proven safe.
