# ADR 0022: No early CI / private corpus / no WER-CER in v1

## Status

Accepted (frozen for v1)

## Decision

Early development is local-first with no CI requirement. Quality corpus is private via `VIDEOCAPTIONERR_CORPUS`. v1 does not implement WER/CER scoring or model rankings. Adapter conformance is structural/operational only. Failure-injection tests are milestone gates.

## Consequences

- Missing private corpus skips quality tests explicitly; unit/protocol/fault tests still run.
- Multi-platform packaging smoke is a release gate (M8), not an early milestone.
