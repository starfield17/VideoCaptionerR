# ADR 0016: Runtime-family-isolated Python environments

## Status

Accepted (frozen for v1)

## Decision

Managed Python envs are isolated per family: `envs/faster-whisper/<lock_hash>/`, `envs/mlx-whisper/<lock_hash>/`. Do not merge CTranslate2, MLX, Qwen, and NeMo into one environment. Use lockfiles via managed `uv`; never `pip install latest` at runtime.

## Consequences

- Doctor performs real import/device/model smoke tests.
- Qwen/NeMo manifests may exist but are not installed in v1.
