# ADR 0023: Pragmatic DDD bounded-context ownership

## Status

Accepted (frozen for v1)

## Decision

VideoCaptionerR uses two core bounded contexts: Subtitle Document and
Processing Workflow. Supporting areas such as media, ASR, LLM, persistence,
subtitle codecs, and platform paths are integration areas and MUST NOT
redefine domain invariants.

## Consequences

- Transcript and TextJoiner belong to the domain.
- Batch, Job, and WorkUnit own workflow transitions.
- Infrastructure remains replaceable and testable through ports.
