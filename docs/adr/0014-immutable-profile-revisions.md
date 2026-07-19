# ADR 0014: Immutable Profile revision snapshots

## Status

Accepted (frozen for v1)

## Decision

Jobs freeze effective settings via immutable Profile revisions. Configuration precedence: CLI one-shot > Job override > Profile revision > global TOML > compiled default. Secrets never enter Job snapshots or request hashes.

## Consequences

- Re-running uses the snapshotted revision unless explicitly changed.
- Provider profiles referenced by stable ID/revision.
