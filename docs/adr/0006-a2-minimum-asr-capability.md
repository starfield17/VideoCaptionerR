# ADR 0006: A2 is the minimum full ASR capability

## Status

Accepted (frozen for v1)

## Decision

Full subtitle pipeline requires A2 (word/character timestamps). A0/A1 engines need forced alignment before claiming full support. Do not fabricate word times by proportional estimation.

## Consequences

- Capability comes from handshake/descriptor, not engine labels alone.
- A3 enables confidence UI/rules; missing confidence uses `prob = -1.0`.
