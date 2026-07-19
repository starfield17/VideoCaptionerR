# ADR 0001: Rust core is the only business-logic layer

## Status

Accepted (frozen for v1)

## Decision

All splitting, retries, cache policy, scheduling, export planning, and IR mutations live in Rust (`crates/core` and related crates). Python workers and the GUI MUST NOT reimplement business rules.

## Consequences

- Python ASR workers stay thin protocol adapters.
- CLI and GUI call the same application services.
- Frontend is a shell over shared commands/events.

## DDD amendment

The domain crate owns business invariants and domain behavior. The core crate
owns application commands, use cases, application-owned ports, orchestration,
and explicit commit boundaries. Concrete adapters and GUI code MUST NOT
reimplement business rules. CLI and GUI use the same bootstrap composition
root.
