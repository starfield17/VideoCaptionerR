# M8 — Native multi-platform release checklist

Authority: `docs/implementation-decisions-all_DDD_FINAL.md` §25 M8.

v1 is **not** released until Linux, Windows, and macOS Apple Silicon each pass
native packaging smoke tests below.

## Sidecars (per platform)

| Binary | Role |
|--------|------|
| `videocaptionerr` / CLI | Inbound adapter |
| `videocaptionerr-whisper-helper` | Isolated ASR helper (never link whisper.cpp into main) |
| `ffmpeg` / `ffprobe` | Media probe/extract |
| `uv` | Managed Python env install for faster-whisper / mlx-whisper |

Override paths via env (`VIDEOCAPTIONERR_HELPER`, `VIDEOCAPTIONERR_UV`, etc.).

## Managed Python envs

```text
$APP_HOME/envs/faster-whisper/<lock_hash>/
$APP_HOME/envs/mlx-whisper/<lock_hash>/
```

- Install only from lockfiles under `python/runtimes/*/requirements.lock`
- Never `pip install latest` at runtime
- mlx-whisper real runtime: **macOS Apple Silicon only**

## Model download

- Explicit user selection only (no silent default model)
- Manifest digests; `.partial` + atomic publish
- Smoke models: whisper-cpp tiny-q5_1, faster-whisper tiny

## Per-platform smoke

### 1. Install package

- Install the platform package (deb/AppImage, MSI, dmg)
- Confirm GPL-3.0-only notices and third-party licenses are present

### 2. Doctor

```text
videocaptionerr doctor
```

Expect:

- home/db paths
- ffmpeg/ffprobe present (bundled or PATH)
- helper present
- uv present (or documented override)
- runtime smoke lines for fake / faster-whisper / mlx (mlx may report RUNTIME_UNAVAILABLE off AS)

### 3. First subtitle (fake)

```text
videocaptionerr transcribe --engine fake path/to/jfk.wav
```

Expect SRT/VTT export and Job Done.

### 4. Optional real engine

- whisper-cpp: helper built with `--features whisper-cpp` + `ggml-tiny-q5_1.bin`
- faster-whisper: managed env provisioned + model directory/snapshot
- mlx: Apple Silicon only

### 5. Exclusive instance

- CLI processing lock vs GUI: second instance reports InstanceBusy

## Signing / notarization decision

| Platform | Decision (v1 prep) |
|----------|--------------------|
| Linux | Unsigned packages acceptable for smoke; signing optional |
| Windows | Code signing deferred until smoke green |
| macOS | Notarization deferred until smoke green |

CI and automated releases remain **optional** after implementation is complete
(manual §3.1: no early CI requirement).

## Host notes

- Development host may only validate Linux packaging end-to-end.
- Windows/macOS smoke requires native machines or VM/CI runners.
- Document results in release notes before tagging v1.
