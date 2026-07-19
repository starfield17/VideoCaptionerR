# ADR 0017: Model manifests and digest validation

## Status

Accepted (frozen for v1)

## Decision

Downloadable models require a versioned manifest (id, family, revision, files, digest, license, sources, runtime, languages, RAM/VRAM, timestamp level). Download to `.partial`, verify digest, atomically rename. No silent default model download. No GC of models in active use.

## Consequences

- Model name alone is insufficient for cache fingerprints.
- User must explicitly select/download before first transcription.
