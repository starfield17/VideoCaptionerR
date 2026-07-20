# Silero VAD (optional)

## Build

```bash
cargo build -p videocaptionerr-platform --features silero-vad
```

Links ONNX Runtime only in the **platform** crate (not main ASR helper / not core).

## Model asset

Download the official Silero VAD ONNX weights (not committed to git):

```bash
mkdir -p .local-dev-assets/models/vad
curl -L -o .local-dev-assets/models/vad/silero_vad.onnx \
  https://github.com/snakers4/silero-vad/raw/master/src/silero_vad/data/silero_vad.onnx
```

Or under app home:

```text
$VIDEOCAPTIONERR_HOME/models/vad/silero_vad.onnx
```

## Runtime

```rust
use videocaptionerr_core::vad::VadOptions;
use videocaptionerr_platform::vad_silero::silero_silence_regions;

let opts = VadOptions {
    model_path: Some(path.into()),
    ..Default::default()
};
let regions = silero_silence_regions(&pcm_s16le_16k, &opts)?;
```

Without the feature or model file, the API returns `RUNTIME_UNAVAILABLE` (no fabricated VAD).
Energy-based silence fallback remains in `videocaptionerr_core::vad::energy_silence_regions`.
