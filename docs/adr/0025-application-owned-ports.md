# ADR 0025: Application-owned ports

## Status

Accepted (frozen for v1)

## Decision

Ports are defined by videocaptionerr-core according to application needs.
Adapter crates MUST NOT define the application-facing repository, media,
ASR-session, LLM-gateway, artifact, subtitle, event, clock, or ID contracts
that use cases depend on.

## Consequences

- Adapter-specific protocol types stay inside adapters or contracts.
- Fakes implement the same ports as production adapters.
- No generic repository or dependency-injection framework is introduced.
