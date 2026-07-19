# ADR 0005: JSON artifacts authority + SQLite control plane

## Status

Accepted (frozen for v1)

## Decision

Versioned JSON artifacts are the authoritative full Transcript/stage data. SQLite is authoritative for Job/Batch/work-unit status, leases, artifact indexes, events, and profiles. A single-writer store actor serializes writes.

## Consequences

- Stage commit is tmp → validate → hash → rename → SQLite transaction.
- Crash recovery is deterministic at each boundary.
- No uncontrolled DB-and-file dual truth.
