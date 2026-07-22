# VideoCaptionerR

Batch subtitle generation: media → ASR → split → LLM correct/translate → SRT/VTT/ASS.

**License:** GPL-3.0-only  
**Binary prefix:** `videocaptionerr`

## Status

Implementation follows the frozen baseline in
[`docs/implementation-decisions-all_DDD_FINAL.md`](docs/implementation-decisions-all_DDD_FINAL.md)
(milestones M0–M7).

## Workspace

```text
crates/
  contracts/   # IR, protocols, error codes
  domain/      # pure aggregates and state machines
  core/        # application / business services
  platform/    # filesystem, ffmpeg, subtitle, and config adapters
  bootstrap/   # shared CLI/Desktop composition root
  asr/         # ASR traits and adapters
  llm/         # LLM provider client
  store/       # SQLite store actor, artifacts, locks
  cli/         # clap CLI
  test-support/
apps/
  desktop/     # Tauri shell and frontend
tools/xtask/
```

## Build

```bash
cargo build --workspace
cargo test --workspace
cargo run -p videocaptionerr-cli -- doctor
cargo run -p xtask -- gen-schemas
```

Local check gate (M0+):

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

## Application home

Default: platform data dir (`~/.local/share/videocaptionerr` on Linux).  
Override: `VIDEOCAPTIONERR_HOME=/absolute/path`.

## Python ASR (later milestones)

Use the Lab conda env when running Python workers:

```text
/home/hazel/miniconda3/envs/Lab
```
