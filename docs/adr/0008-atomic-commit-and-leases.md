# ADR 0008: Stage/work-unit atomic commit and leases

## Status

Accepted (frozen for v1)

## Decision

Successful artifacts commit as: write `.tmp` → flush → reread/validate → BLAKE3 → atomic rename → single SQLite transaction (artifact + unit/stage + event). Running work units hold leases; expired leases return to Pending with incremented attempt.

## Consequences

- Crash at any boundary recovers deterministically.
- No half-committed official artifacts.
- Done units are not rerun.
