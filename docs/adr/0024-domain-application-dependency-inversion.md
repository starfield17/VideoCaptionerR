# ADR 0024: Domain and application dependency inversion

## Status

Accepted (frozen for v1)

## Decision

The domain crate has no dependency on another VideoCaptionerR crate. The
application crate depends inward on domain and contracts only. Store, ASR,
LLM, platform, and process adapters implement application-owned ports and
depend inward on the application contracts they satisfy.

## Consequences

- A use case can be tested without a database, process, network, or filesystem.
- Concrete adapters cannot determine pipeline order or aggregate transitions.
- The workspace must remain acyclic.
