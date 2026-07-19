# ADR 0018: Plaintext TOML API keys with log redaction

## Status

Accepted (frozen for v1)

## Decision

API keys are stored in TOML plaintext by design with no user warning. Unix config dirs/files SHOULD be 0700/0600. Keys MUST NOT appear in Job snapshots, artifacts, logs, events, diagnostics, or request hashes. Authorization headers and tokens are redacted from logs.

## Consequences

- No OS credential-store integration in v1.
- Config files are gitignored.
