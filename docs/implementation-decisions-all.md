# VideoCaptionerR — Implementation Decisions and Architecture Manual

> Final implementation baseline for coding and testing agents  
> Repository: `https://github.com/starfield17/VideoCaptionerR`  
> License: `GPL-3.0-only`  
> Status: **Frozen implementation baseline for v1**

---

## 0. Purpose and authority

This document combines and supersedes the implementation-relevant content of:

1. `subtitle-tool-architecture-manual(3).md`;
2. `subtitle-tool-architecture-manual-v1-addendum(1).md`;
3. the completed implementation questionnaire and all later corrections.

It is intended to be handed directly to Claude Code, Codex, or another coding agent. It defines product scope, architecture, contracts, defaults, persistence rules, failure behavior, milestones, and acceptance gates.

### 0.1 Normative language

The words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are normative.

### 0.2 Source-precedence rule

When two sources appear to disagree, use this order:

1. this document;
2. the v1 addendum;
3. the original architecture manual;
4. the reference `WEIFENG2333/VideoCaptioner` repository's behavior;
5. an agent's preferred implementation.

The reference repository is a source of pipeline ideas, prompts, tested constants, and edge cases. Its architecture and code organization are **not** authoritative and SHOULD NOT be copied wholesale.

### 0.3 Frozen decisions are not design prompts

A coding agent MUST NOT reopen a frozen decision merely because another approach is common or personally preferred. A decision may be challenged only when:

- it is technically impossible;
- it creates a direct contradiction inside this document;
- it would introduce a security or data-loss defect that cannot be contained;
- required third-party licensing makes it impossible.

In that case, the agent MUST state the exact contradiction and propose the smallest correction. It MUST NOT silently substitute another design.

---

## 1. Coding-agent operating rules

### 1.1 Think before coding

Before implementing a task, the agent MUST:

1. state material assumptions;
2. name ambiguous inputs or competing interpretations;
3. identify a simpler implementation when one exists;
4. define observable success criteria;
5. list the files or modules it expects to change;
6. state how each step will be verified.

A suitable task plan is:

```text
[Step] -> verify: [specific command or assertion]
[Step] -> verify: [specific command or assertion]
[Step] -> verify: [specific command or assertion]
```

The agent SHOULD ask a question before coding only when the missing answer changes a public contract or makes the implementation unsafe. It SHOULD NOT ask about details already frozen here.

### 1.2 Simplicity first

The implementation MUST be the minimum design that satisfies the current milestone.

- Do not add speculative features.
- Do not add abstractions for a single use unless they enforce a frozen boundary.
- Do not create configurability that is not requested.
- Do not build generic plugin frameworks beyond the contracts defined here.
- Do not add impossible-scenario handling merely to make code look defensive.
- If an implementation is much larger than necessary, simplify it before merging.

### 1.3 Surgical changes

For an existing codebase:

- touch only code required by the task;
- do not reformat or refactor adjacent modules without need;
- follow established style unless the task is explicitly a style migration;
- remove imports, variables, and functions made unused by the current change;
- report unrelated dead code, but do not delete it;
- every changed line SHOULD trace to the task's success criteria.

### 1.4 Tests are part of the task

A feature is not complete when only the normal path works. The task MUST include the milestone-appropriate tests for:

- success;
- invalid input;
- cancellation;
- timeout;
- crash or process death;
- recovery or retry;
- half-written artifacts where relevant.

---

## 2. Project identity, scope, and principles

### 2.1 Project identity

```text
Product name: VideoCaptionerR
Repository: starfield17/VideoCaptionerR
Rust package and binary prefix: videocaptionerr
License: GPL-3.0-only
```

Cargo packages and release metadata MUST use:

```toml
license = "GPL-3.0-only"
```

Do not use `GPL-3.0-or-later` unless the repository is explicitly relicensed.

### 2.2 Product goal

VideoCaptionerR is a batch subtitle-generation tool:

```text
media input
-> probe and audio extraction
-> ASR with word/character timestamps
-> sentence splitting
-> LLM correction
-> LLM translation
-> SRT/VTT/ASS export
```

The intended architecture is:

- Rust for application services, orchestration, business logic, persistence, CLI, desktop commands, and LLM calls;
- Python only for ASR runtimes that require Python;
- Tauri 2 plus a web frontend for the desktop application;
- a CLI implemented before the GUI and sharing the same application services;
- a single Transcript IR through all stages;
- persistent work units and atomic artifacts for crash-safe resume.

### 2.3 Explicit non-goals

v1 MUST NOT implement:

- subtitle burn-in or video synthesis;
- ASS visual rendering beyond deterministic subtitle-file export;
- TTS or dubbing;
- traditional translation APIs;
- online-video downloading;
- cost-budget enforcement for LLM requests;
- automatic glossary extraction by default;
- WER/CER scoring infrastructure;
- telemetry;
- automatic application updates;
- simultaneous GUI and CLI schedulers;
- automatic model choice on first use.

### 2.4 Design principles

1. **One IR:** all stages consume and produce typed IR rather than passing SRT text internally.
2. **Immutable ASR timeline:** LLM stages cannot modify timestamps.
3. **Rust owns business rules:** Python workers do not own splitting, retries, cache policy, or scheduling.
4. **Crash-safe work:** a successful artifact is validated and atomically committed.
5. **Fine-grained resume:** ASR chunks and LLM batches are independent work units.
6. **Capability over name:** actual runtime capabilities come from handshake/probing, not engine labels alone.
7. **Batch simplicity:** one Batch uses one ASR model/device configuration and executes FIFO.
8. **No hidden degradation:** every fallback is recorded and visible.
9. **Deterministic export:** same IR and options produce byte-identical files.
10. **No duplicated business logic:** CLI and GUI call the same application services.

---

## 3. Development platforms and release strategy

### 3.1 Current development environment

Current implementation and testing are performed locally on:

```text
Linux x86_64
```

Early development MUST NOT depend on GitHub Actions or another CI system.

### 3.2 Target platforms

The source architecture MUST remain compatible with:

```text
Linux x86_64
Windows x86_64
macOS Apple Silicon
```

Platform-specific code MUST be isolated behind small modules for:

- process-tree control;
- filesystem/application paths;
- instance locking;
- sidecar discovery;
- Python/runtime installation;
- desktop packaging.

### 3.3 Build policy

Early milestones are verified locally. Multi-platform target compilation and packaging may be run manually when useful.

A complete desktop release is not considered validated merely because Rust code cross-compiles. Before public v1 release, each target platform MUST pass native packaging and installation smoke tests, including sidecars and ASR runtime behavior.

CI and automated releases MAY be introduced only after the implementation is substantially complete. They are not an early milestone requirement.

---

## 4. Repository and workspace layout

The initial workspace SHOULD be:

```text
VideoCaptionerR/
├── Cargo.toml
├── crates/
│   ├── contracts/       # IR, protocol, events, error codes, schema source
│   ├── core/            # pipeline and application/business services
│   ├── asr/             # ASR traits, helpers, worker clients, adapters
│   ├── llm/             # provider client, capability probe, agent loop
│   ├── store/           # SQLite store actor, migrations, artifact metadata
│   ├── cli/             # clap adapter and terminal/NDJSON rendering
│   └── test-support/    # fake worker, fake provider, fault injection fixtures
├── apps/
│   └── desktop/         # Tauri 2 + React + TypeScript + Vite
├── python/
│   └── runtimes/
│       ├── faster-whisper/
│       └── mlx-whisper/
├── prompts/
│   ├── split/
│   ├── correct/
│   └── translate/
├── schemas/             # generated; do not hand-edit
├── migrations/
├── tools/
│   └── xtask/
└── docs/
    └── adr/
```

### 4.1 Dependency direction

The dependency direction MUST remain acyclic:

```text
CLI / Desktop
    -> application services in core
        -> contracts
        -> asr
        -> llm
        -> store
```

`contracts` MUST NOT depend on UI, database, scheduling, or runtime implementations.

`core` MUST NOT import Tauri, React, or terminal-rendering concerns.

`asr` and `llm` MUST NOT own Job scheduling or persistence policy.

### 4.2 Technology selections

```text
Rust async runtime: Tokio
Desktop: Tauri 2
Frontend: React + TypeScript + Vite
Frontend package manager: pnpm
Database access: rusqlite
Database write model: single-writer store actor
Public identifiers: ULID strings
User-editable configuration: TOML
Schema generation: Rust contracts -> xtask -> JSON Schema / TS / Python fixtures
```

Do not introduce an ORM unless a later decision explicitly replaces `rusqlite`.

---

## 5. Application directories and configuration

### 5.1 Default application home

Use the platform-native application data directory. Support an explicit environment override:

```text
VIDEOCAPTIONERR_HOME=/absolute/path
```

Recommended layout:

```text
<app-home>/
├── config/
│   └── config.toml
├── state/
│   └── videocaptionerr.db
├── jobs/
├── cache/
├── models/
├── envs/
├── logs/
└── locks/
```

Portable mode is not the default.

### 5.2 Configuration precedence

```text
CLI one-shot argument
> Job override
> immutable Profile revision
> global TOML configuration
> compiled default
```

Every persisted configuration document MUST have `schema_version`.

Migrations MUST:

1. back up the old file;
2. preserve unknown fields where practical;
3. validate the migrated result;
4. atomically replace the original file;
5. emit warnings for deprecated fields.

### 5.3 Plaintext API keys

API keys are intentionally stored directly in TOML. The application MUST NOT show a warning about this design.

Example:

```toml
schema_version = 1

[llm.providers.primary]
base_url = "https://example.invalid/v1"
api_key = "plaintext-key"
model = "model-name"
```

No OS credential-store integration is required.

Even though storage is plaintext:

- Unix config directories SHOULD be mode `0700`;
- Unix config files SHOULD be mode `0600`;
- Windows SHOULD retain current-user default ACLs;
- generated local config files MUST be ignored by Git;
- API keys MUST NOT be copied into Job snapshots, artifacts, logs, events, diagnostics, or request hashes;
- Authorization headers, proxy credentials, and tokens MUST be redacted from logs.

A Job snapshot references a provider profile by stable ID/revision and stores non-secret effective settings only.

---

## 6. Core contracts and schemas

### 6.1 Single source of truth

Rust types in `crates/contracts` are the source of truth for:

- Transcript IR;
- worker protocol;
- helper protocol;
- CLI event envelope;
- GUI command/event payloads;
- error codes;
- artifact metadata;
- profile and capability schemas.

`xtask` SHOULD generate:

- JSON Schema;
- TypeScript interfaces;
- Python schema fixtures/constants;
- stable error-code lists.

Python and TypeScript MUST NOT maintain divergent hand-written copies of business defaults.

### 6.2 Schema compatibility

Every persisted or external contract MUST carry a schema/protocol version.

Within one major version:

- new optional fields MAY be added;
- required fields MUST NOT be removed or reinterpreted;
- unknown optional fields SHOULD be ignored where specified;
- unknown message types are protocol errors unless explicitly declared extensible.

---

## 7. Transcript IR

### 7.1 Canonical model

```rust
#[derive(Serialize, Deserialize, Clone)]
pub struct Transcript {
    pub schema_version: u32,
    pub revision: u64,
    pub source_hash: String,
    pub language: Option<String>,
    pub engine: EngineFingerprint,
    pub words: Vec<Word>,
    pub cues: Vec<Cue>,
    pub next_cue_id: u32,
    pub timeline_source: TimelineSource,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Word {
    pub text: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub prob: f32,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Cue {
    pub id: u32,
    pub word_range: Option<Range<usize>>,
    pub imported_start_ms: Option<u64>,
    pub imported_end_ms: Option<u64>,
    pub text: String,
    pub translation: Option<String>,
    pub flags: CueFlags,
}
```

The exact Rust representation MAY vary to satisfy Serde and schema constraints, but the semantics MUST remain.

### 7.2 Timeline invariants

For ASR-derived Transcripts:

1. `words` are created by ASR normalization and are immutable afterward.
2. Words are ordered and non-negative.
3. Every word lies inside source duration.
4. Cue word ranges are legal, ordered, and non-overlapping.
5. Cue start/end derive from the first and last word in the range.
6. LLM stages receive no mutable timeline fields.
7. Re-splitting changes cue ranges and creates a new Transcript revision; it does not rerun ASR.

For imported subtitles:

- `timeline_source = ImportedCue`;
- `words` may be empty;
- each cue owns explicit imported start/end times;
- word-range-based exact splitting is unavailable until forced alignment is performed.

### 7.3 Confidence semantics

`Word.prob` is defined as:

```text
0.0..=1.0 : confidence provided by the adapter
-1.0      : no comparable confidence is available
other     : invalid
```

When `prob == -1.0`:

- the UI MUST NOT display a low-confidence marker;
- probability-based hallucination filters MUST skip that word;
- values MUST NOT be compared across different engine families.

### 7.4 Stable cue IDs

- initial split assigns IDs `1..N` in timeline order;
- automatic full re-split may rebuild IDs but MUST create a new Transcript revision;
- manual split keeps the old ID on the first half and allocates `next_cue_id` to the second;
- manual merge keeps the first cue ID and tombstones removed IDs;
- sorting and export MUST NOT mutate cue IDs;
- SRT ordinal numbers are an export view, not cue IDs.

### 7.5 Field provenance and user protection

Persist field-level origins for at least `text` and `translation`:

```rust
pub enum FieldOrigin {
    Asr,
    RuleSplit,
    Llm { request_id: String },
    User,
    Imported,
}
```

A user edit to translation MUST block later automatic translation from overwriting that field. A user edit to source text MUST not unnecessarily block correction/translation of unrelated fields.

Every asynchronous LLM result MUST be bound to:

- Transcript revision;
- cue ID;
- source field revision.

A stale result MUST be discarded and recorded. It MUST NOT overwrite newer edits.

### 7.6 TextJoiner

Text assembly MUST be centralized in a language-aware `TextJoiner`.

It MUST handle:

- no spaces between normal CJK characters;
- natural spaces between Latin words;
- punctuation spacing by Unicode category;
- apostrophes and hyphens;
- decimals, percentages, currency, and URLs;
- mixed CJK/Latin boundaries without blindly adding or removing every space;
- Unicode NFC normalization in normalized IR while preserving raw adapter output separately.

Do not use `join(" ")` as the universal word assembly strategy.

---

## 8. Media input, probing, and audio extraction

### 8.1 ffprobe is authoritative

File extensions are only a picker optimization. The Probe stage determines whether an input is processable.

```rust
pub struct MediaProbe {
    pub input_size: u64,
    pub container: Option<String>,
    pub duration_ms: u64,
    pub audio_streams: Vec<AudioStream>,
}

pub struct AudioStream {
    pub stream_index: u32,
    pub codec: String,
    pub language: Option<String>,
    pub title: Option<String>,
    pub channels: u16,
    pub sample_rate: u32,
    pub is_default: bool,
}
```

`stream_index` is the ffprobe global stream index.

For one usable audio stream, select it automatically. For multiple reasonable candidates, prefer the default flag but require user confirmation during preflight.

The selected stream index is frozen into the Job profile snapshot. If a resumed source probes to a different stream set, previous audio artifacts are invalid.

### 8.2 Source ownership

Adding media to a Job stores:

- normalized absolute path;
- file metadata;
- probe data;
- content hashes.

The source media file is not copied into the Job directory by default.

### 8.3 Hashes

Compute both:

- `media_hash`: full BLAKE3 of the original input;
- `pcm_hash`: BLAKE3 of normalized PCM data.

The full media hash is computed as part of initial preparation and MUST exist before ASR cache selection is finalized.

Hashes MUST be streamed rather than loading entire files into memory.

### 8.4 Canonical extraction

Use argument arrays, never a shell-concatenated command:

```text
ffmpeg -nostdin -hide_banner -loglevel error
  -i <input>
  -map 0:<stream_index>
  -vn -ac 1 -ar 16000 -c:a pcm_s16le -f wav
  -progress pipe:1 -y <job-dir>/audio.tmp.wav
```

After process success:

1. validate exit code;
2. validate WAV readability;
3. validate 16 kHz, mono, PCM s16le;
4. compare duration within a defined tolerance;
5. atomically rename to `audio.wav`.

Cancellation or failure deletes only `audio.tmp.wav` and does not damage a previous valid `audio.wav`.

### 8.5 Temporary-space preflight

16 kHz mono 16-bit PCM is approximately 115.2 MB/hour.

Required available space before extraction:

```text
estimated_pcm_bytes * 1.5 + 256 MiB
```

Fail early with `DISK_SPACE_INSUFFICIENT` when the requirement is not met.

---

## 9. Output planning and subtitle import/export

### 9.1 Output directory and template

Default export directory:

```text
<source-file-directory>/subtitles/
```

Default template:

```text
{stem}.{target_lang?}.{layout}.{format}
```

Example:

```text
lecture.zh-CN.bilingual.srt
```

Profiles MAY edit a restricted, validated template using known variables. Arbitrary scripts and executable expressions are forbidden.

### 9.2 Conflict policy

One shared `OutputPlanner` is used by CLI and GUI.

```text
rename    default; append .1, .2, ... and reserve paths at Job creation
fail      fail preflight if any output exists
overwrite overwrite export files only
```

Overwrite MUST NOT replace:

- input media;
- another Job's reserved target;
- Job snapshots;
- cache artifacts.

### 9.3 Job directory

Use:

```text
{ULID}_{sanitized_stem}
```

### 9.4 Import support

v1 supports importing:

- SRT;
- VTT.

ASS import is deferred. ASS export remains supported.

Readers SHOULD be tolerant and emit diagnostics. Writers MUST be strict and deterministic.

For imported multiline cues, the import workflow MUST ask whether content is:

- one language with line breaks;
- source above translation;
- translation above source.

Do not infer bilingual layout from language detection alone.

### 9.5 Deterministic writers

Same IR plus same options MUST produce byte-identical output.

- UTF-8;
- default LF line endings;
- SRT time `HH:MM:SS,mmm`;
- VTT header `WEBVTT` and dot milliseconds;
- ASS line breaks as `\N`;
- ASS escape braces, backslashes, and override-tag injection;
- write to `.tmp`, validate, then atomically replace;
- SRT ordinals are regenerated continuously at export.

For translation-only output with missing translations, the Profile MUST explicitly select fallback-to-source or fail. Do not silently vary behavior.

### 9.6 Export preflight

Before writing, produce diagnostics for:

- time out of range;
- inverse or overlapping cues;
- empty source or translation;
- excessive cue length;
- excessive characters per second;
- display duration too short/long;
- repeated consecutive text;
- LLM fallbacks;
- restored hallucination fragments;
- user-edited fields.

Errors block export. Warnings allow export and are written to `export-report.json`.

---

## 10. ASR abstraction and capability levels

### 10.1 Engine contract

```rust
#[async_trait]
pub trait AsrEngine: Send + Sync {
    fn descriptor(&self) -> &EngineDescriptor;

    async fn transcribe(
        &self,
        audio: &Path,
        opts: &AsrOptions,
        sink: mpsc::Sender<AsrEvent>,
    ) -> Result<AsrRawResult>;
}
```

Actual signatures MAY evolve, but adapters MUST return structured results and events.

### 10.2 EngineDescriptor

Capabilities MUST be obtained from actual runtime handshake/FFI/helper probing, not guessed only from engine ID.

```rust
pub struct EngineDescriptor {
    pub protocol_version: u32,
    pub engine_id: String,
    pub adapter_version: String,
    pub runtime_version: String,
    pub devices: Vec<DeviceDescriptor>,
    pub timestamp_granularity: TimestampGranularity,
    pub confidence_kind: ConfidenceKind,
    pub native_vad: bool,
    pub language_detection: bool,
    pub streaming_events: bool,
    pub cooperative_cancel: bool,
    pub max_audio_secs: Option<u32>,
    pub supported_options: BTreeSet<String>,
    pub unavailable_reason: Option<String>,
}
```

The GUI only enables options confirmed by the current descriptor. Unsupported options MUST NOT be silently ignored.

### 10.3 Capability levels

```text
A0 full text only                         not allowed in full subtitle pipeline
A1 segment text + segment timestamps      degraded/experimental path only
A2 word/character text + start/end        minimum for full v1 pipeline
A3 A2 + meaningful confidence             enables confidence UI/rules
```

An A0/A1 engine needs a forced aligner before it can claim full subtitle support. Do not fabricate word times by proportional estimation and label them native.

### 10.4 Official v1 adapters

Official v1 support:

1. whisper.cpp through a Rust helper process;
2. faster-whisper through a Python worker;
3. mlx-whisper through a Python worker on Apple Silicon.

Qwen ASR and NVIDIA NeMo retain future adapter boundaries but are not official v1 requirements.

### 10.5 Adapter order

```text
fake worker
-> whisper.cpp Rust helper
-> faster-whisper
-> mlx-whisper
-> later experimental Qwen/NeMo adapters
```

### 10.6 No default ASR model

VideoCaptionerR does not automatically select a default model.

Before the first transcription, the user must explicitly select and download:

- model family/revision;
- quantization where relevant;
- source/mirror;
- compatible device/runtime.

Preflight may offer a download action but MUST NOT silently download a model.

---

## 11. whisper.cpp isolation

Despite the original preference for in-process `whisper-rs`, v1 uses an isolated Rust helper process.

```text
main Rust application
<-> structured helper protocol
Rust whisper.cpp helper
<-> whisper.cpp FFI
```

Reasons:

- a stuck native inference does not freeze the main process;
- cancellation can escalate to process-tree termination;
- crashes are isolated;
- artifact commit semantics remain consistent with Python workers.

The helper remains Rust code. Python is not involved.

The helper protocol SHOULD reuse the same envelope concepts as the Python worker while keeping engine-specific messages explicit.

---

## 12. Python ASR worker protocol

### 12.1 Design constraints

- long-lived process for model reuse within a Batch;
- stdio NDJSON, not localhost HTTP;
- stdout contains protocol messages only;
- stderr contains runtime/library logs;
- one active transcription per worker;
- no splitting, retry policy, persistence, hallucination policy, or scheduling in Python;
- worker code remains intentionally thin.

### 12.2 Envelope

```json
{
  "protocol_version": 1,
  "session_id": "01J...",
  "request_id": 42,
  "seq": 7,
  "type": "segment",
  "data": {}
}
```

Rules:

- UTF-8;
- one JSON object per line;
- maximum line size 4 MiB;
- request `seq` starts at 0 and is monotonically increasing;
- every request has exactly one terminal message: `result`, `error`, or `cancelled`;
- messages after terminal state are protocol violations;
- unknown critical message types cause rejection and worker restart;
- same major protocol version is required;
- minor-compatible additions are optional fields.

### 12.3 Requests

At minimum:

```json
{"type":"hello"}
{"type":"ping"}
{"type":"load_model","data":{}}
{"type":"transcribe","data":{}}
{"type":"cancel","data":{"target_request_id":42}}
{"type":"unload_model"}
{"type":"shutdown"}
```

### 12.4 Events and results

At minimum:

```text
result
error
cancelled
pong
progress
segment
language
log metadata when explicitly routed
```

Segments are streamed while inference runs. The final artifact is not committed until the terminal result arrives and the complete raw result validates.

### 12.5 Internal concurrency

A worker needs:

1. a permanent stdin reader;
2. a single blocking inference executor;
3. a thread-safe cancellation token;
4. a bounded single-writer stdout queue.

While transcribing, `hello`, `ping`, `cancel`, and `shutdown` may be accepted. Another model load or transcription returns `WORKER_BUSY`.

### 12.6 Heartbeat and backpressure

- startup, load, first-segment, and inter-segment timeouts are separate;
- progress is not heartbeat;
- `ping` must be handled by the control path during inference;
- Rust's segment channel defaults to a bounded capacity of 256;
- GUI progress is throttled to approximately 5-10 Hz;
- SQLite progress persistence is at most 1 Hz;
- indeterminate progress is shown honestly when processed duration is unavailable.

### 12.7 Cancellation escalation

1. send cooperative cancel;
2. wait `cancel_grace_ms = 3000` by default;
3. terminate the worker/helper process tree if still active;
4. mark the current work unit Cancelled;
5. preserve previously committed work units.

Use a process group on Unix and a Job Object on Windows. Killing only the immediate parent PID is insufficient.

### 12.8 Retry policy

- default automatic retry budget: two retries per work unit;
- OOM: at most one strategy-changing retry, such as smaller batch or compute mode;
- after that, fail the unit;
- do not silently fall back from GPU to CPU unless a later explicit option is added;
- protocol pollution, corrupt models, and unsupported parameters are deterministic and not automatically retried.

---

## 13. Python runtimes and model distribution

### 13.1 Runtime-family isolation

Official v1 managed Python environments:

```text
envs/faster-whisper/<lock_hash>/
envs/mlx-whisper/<lock_hash>/
```

Do not force CTranslate2, MLX, Qwen/vLLM, and NeMo into one environment.

Qwen and NeMo environment manifests MAY be specified for future work but are not installed in v1.

### 13.2 uv

Use bundled or managed `uv` and lockfiles. Do not run `pip install latest` at runtime.

Every environment records:

- Python version;
- uv version;
- lock hash;
- platform/accelerator variant;
- installation timestamp;
- smoke-test result.

The doctor command MUST perform an actual import/device/model smoke test rather than only detecting a GPU name.

### 13.3 Sidecars

Bundle fixed ffmpeg, ffprobe, uv, and helper binaries as Tauri sidecars for each target platform. Advanced users MAY configure an override path.

Frontend code MUST NOT be allowed to execute arbitrary shell commands. All process launches go through Rust application services and a strict allowlist.

### 13.4 Model manifest

Every downloadable model artifact needs a manifest containing at least:

```text
model_id
engine_family
revision
files and sizes
cryptographic digest
license
source URL
mirrors
minimum runtime
supported languages
estimated RAM/VRAM
timestamp level
```

Downloads use `.partial`, support resume where practical, validate the digest, and atomically rename after success.

A model in active use must not be garbage-collected.

---

## 14. ASR raw artifacts and normalization

### 14.1 Preserve raw output

Each adapter writes/returns a structured raw artifact first:

```text
asr.raw.json
```

Rust then runs `AsrNormalizer` to produce:

```text
01_asr.json
```

This separates model/runtime behavior from normalization behavior and permits re-normalizing without rerunning inference.

### 14.2 Normalization order

```text
unit conversion
-> time clipping
-> token/punctuation merge
-> whitespace/text normalization
-> monotonicity validation
-> hallucination-rule evaluation
-> Transcript IR
```

### 14.3 Time conversion

- convert external values to integer microseconds internally;
- round to integer milliseconds at the IR boundary;
- clip starts to `[0, duration_ms]`;
- clip ends to `[start_ms, duration_ms]`;
- only repair inverse order caused by at most 1 ms rounding;
- larger inversions are adapter errors;
- punctuation without independent time joins a neighboring word without artificially extending the timeline;
- do not sort invalid timestamps silently to hide adapter defects.

### 14.4 Engine fingerprint

A cache-safe fingerprint includes:

- engine ID;
- adapter version;
- runtime version;
- model revision and weight digest;
- quantization;
- execution mode;
- device identity;
- compute type;
- backend/build flags;
- normalized ASR options;
- VAD model/options where relevant.

A model name alone is not sufficient.

---

## 15. VAD, long audio, and hallucination handling

### 15.1 VAD strategy

- use engine-native VAD when descriptor-confirmed and reliable;
- use Rust-side Silero ONNX fallback for engines without suitable native VAD and for chunk planning;
- implement fallback VAD before the first adapter that requires time-limited chunking or lacks adequate native VAD;
- do not force fallback VAD into the earliest IR-only milestone.

Unified defaults:

```text
enabled = true
threshold = 0.4
min_silence_ms = 500
speech_pad_ms = 200
```

### 15.2 Local chunking policy

Local ASR does not chunk by default. Chunking activates only when:

- an adapter declares a maximum duration;
- the user explicitly enables chunking;
- memory protection requires it;
- a forced aligner has a duration limit.

### 15.3 ChunkPlan

```rust
pub struct AudioChunk {
    pub index: u32,
    pub core_start_ms: u64,
    pub core_end_ms: u64,
    pub read_start_ms: u64,
    pub read_end_ms: u64,
    pub cut_reason: CutReason,
}
```

Defaults:

```text
search_radius = 30 s
context_padding = 1.5 s
min_chunk_secs = 60 s
max_chunk_secs = min(profile setting, adapter limit)
```

`core` ranges are contiguous, non-overlapping output ownership. `read` ranges may overlap as model context. After absolute offset correction, retain words whose center belongs to the chunk's core range.

This preserves the rule:

```text
no text-based fuzzy overlap deduplication
```

### 15.4 Cut algorithm

1. search for qualifying silence around target time;
2. rank candidates by longer silence then proximity;
3. otherwise use lowest short-term energy;
4. otherwise force a cut at target;
5. apply read padding and core ownership;
6. enforce minimum chunk length.

### 15.5 Chunk cache

```text
blake3(
  pcm_hash |
  chunk_plan_hash |
  chunk_index |
  engine_fingerprint |
  normalized_asr_options
)
```

A failed chunk does not invalidate completed chunks.

### 15.6 Hallucination filters

Default behavior:

- exclude detected hallucination/noise fragments from the formal Transcript;
- preserve them in a structured filtered artifact;
- display them in the GUI;
- allow one-click restoration.

Rules include:

1. bracketed music/environment markers;
2. low confidence plus high repetition;
3. a versioned built-in Chinese Whisper hallucination list;
4. Profile additions and per-rule disabling.

Do not permanently delete filtered evidence.

Confidence thresholds are adapter-specific. Unknown/unreliable confidence does not trigger low-confidence filtering.

---

## 16. Sentence splitting

### 16.1 Rule-based splitting

Rule splitting is implemented in Rust and follows the tested logic from the reference project while using word indices:

1. split on time gaps;
2. detect unusually large gaps in a moving window;
3. split oversized groups near language-specific function words;
4. force split remaining oversized groups near preferred ratios;
5. merge very short neighboring groups under gap rules.

It MUST produce cue word ranges directly. It MUST NOT convert to plain text and later fuzzily recover timestamps.

### 16.2 LLM splitting

The default full `process` Profile enables LLM splitting.

Preferred protocol:

1. join words while recording word-to-character offsets;
2. ask the LLM to insert `<br>` without changing any other character;
3. remove `<br>` and require exact content equality after defined normalization;
4. map break character offsets to word boundaries;
5. retry at most twice with structured violations;
6. fall back to rule splitting if still invalid.

An indexed-cut-array strategy MAY exist as an alternative, but should not be implemented before the default strategy works and tests pass.

### 16.3 Split defaults

Use the constants in Appendix A unless a language-specific Profile overrides them.

---

## 17. LLM correction and translation

### 17.1 Default full process pipeline

The default `process` Profile enables:

```text
ASR
LLM sentence splitting
LLM correction
reflect translation
export
```

The `transcribe` CLI command remains a simpler semantic command and does not implicitly run the full LLM pipeline unless its options explicitly request it.

### 17.2 Generic agent loop

Use one reusable validation loop for split/correct/translate rather than three duplicate implementations.

Typical flow:

```text
send request
-> parse structured result
-> run business validation
-> accept if valid
-> append concise violations and retry
-> isolate failing items when batch-local
-> final per-cue fallback
```

Correction validation includes:

- exact input/output cue-key set;
- similarity threshold 0.7 after normalization;
- special handling for extremely short cues;
- no translation during correction.

Translation validation includes:

- exact output key set;
- non-empty translations;
- original-text residue heuristics with allowlists for names, URLs, code, numbers, formulae, and glossary-locked terms;
- no timeline fields.

### 17.3 Batch packing

`20` remains the maximum cue count, not the sole limit.

```text
estimated input
+ reserved output
+ safety margin
<= effective context limit
```

When no official tokenizer is available, use a conservative configurable `chars_per_token` estimate. A single oversized cue gets a dedicated strategy; it must not be retried indefinitely with the same impossible request.

Persist `llm.plan.json` with:

- output cue IDs;
- context cue IDs;
- estimated tokens;
- prompt bundle hash;
- Provider profile revision;
- model and generation parameters.

### 17.4 Translation context and concurrency

Default translation context includes accepted translations from the previous batch.

Therefore:

- batches within one Job execute as a wavefront in order;
- batch `n` waits for accepted output from batch `n-1`;
- different Jobs may share the Provider semaphore and execute concurrently;
- context cues are read-only and not part of the expected output-key set.

### 17.5 Binary isolation of failed LLM batches

After batch retries fail for a content/shape error:

1. classify global errors such as authentication or missing model;
2. stop the stage for global errors;
3. otherwise split the batch in half;
4. commit valid children independently;
5. continue until one cue;
6. for a final failed cue, preserve source/existing translation and set `llm_failed`.

Do not discard nineteen valid cues because one cue caused an invalid batch.

### 17.6 Glossary

Automatic glossary extraction is disabled by default in v1.

The architecture MAY preserve an optional future translation substate, but the default pipeline does not run it and it MUST NOT block the current milestones.

### 17.7 No cost/request budgets

Do not implement:

- `max_cost`;
- `max_requests`;
- budget-approval states;
- model price tables;
- cost-based circuit breakers.

Token usage MAY be logged when returned by the Provider, but it does not stop execution.

### 17.8 Circuit breaker

Reliability controls remain:

- authentication errors immediately stop new requests for that Provider;
- repeated 429/5xx failures open a temporary Provider circuit, approximately one minute by default;
- honor `Retry-After`;
- do not let every Job independently retry an unavailable Provider at once.

---

## 18. LLM Provider profiles and capability detection

### 18.1 Built-in templates

v1 provides templates for:

- Generic OpenAI-compatible;
- Ollama;
- LM Studio.

Templates prefill likely fields but are not guarantees.

### 18.2 Capability model

```rust
pub struct LlmCapabilities {
    pub structured_output: StructuredMode,
    pub returns_usage: bool,
    pub supports_seed: bool,
    pub supports_model_list: bool,
    pub max_context_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
}

pub enum StructuredMode {
    JsonSchema,
    JsonObject,
    PromptOnly,
}
```

### 18.3 Automatic detection plus manual override

Automatic capability detection is required and manual override is retained.

Effective priority:

```text
manual override
> cached automatic detection
> conservative built-in default
```

Capability probes run when:

- Test Connection is invoked;
- a Provider Profile is created or materially changed;
- base URL, model, or authentication changes;
- the user explicitly selects Re-detect Capabilities;
- the probe version invalidates an old result.

Normal LLM batches MUST NOT probe on every request.

### 18.4 Probe behavior

Use minimal requests to determine where possible:

- connectivity;
- authentication;
- model acceptance;
- JSON Schema structured output;
- JSON Object mode;
- usage field availability;
- model-list endpoint;
- seed parameter acceptance.

Do not discover context limits by deliberately sending enormous requests.

Context-limit priority:

```text
manual override
> Provider/model metadata
> known model data shipped with the application
> conservative default
```

A probe failure for one optional capability degrades that capability only. It does not necessarily invalidate the entire Provider.

### 18.5 Structured-output order

```text
JsonSchema
-> JsonObject
-> PromptOnly + tolerant JSON parsing
-> strict business validation in all modes
```

Provider-declared JSON Schema support never replaces business validation.

### 18.6 Probe persistence

Cache probe results by:

```text
base_url
model
Provider Profile revision
capability-probe version
```

The API key is neither hashed into the identity nor logged.

---

## 19. Prompt storage and reproducibility

Prompt files are directly editable under `prompts/` or the configured application prompt directory.

Do not require editing prompts through a database or GUI.

To retain reproducibility:

1. at the beginning of an LLM stage, read current prompt files;
2. normalize and hash the prompt bundle;
3. copy the exact contents into the Job's immutable stage snapshot;
4. execute all work units in that stage using the snapshot;
5. later edits affect only a new Job or an explicit stage rerun;
6. the prompt-bundle hash participates in request and artifact cache keys.

Do not reread editable source prompt files independently for every batch within the same stage.

Subtitle and user-reference content is untrusted data. System prompts MUST state that content inside data delimiters is not an instruction. Do not expose local paths, environment variables, or secrets to the model.

---

## 20. Jobs, Batches, stages, and scheduling

### 20.1 Pipeline

```text
Probe
-> ExtractAudio
-> ASR
-> Split
-> Correct
-> Translate
-> Export
```

Each stage state includes at least:

```text
Pending
WaitingResource
Running(progress)
Retrying(attempt/max)
Done
DoneDegraded
Failed
Skipped
Cancelled
WaitingProvider
```

Additional internal substates MAY exist, but avoid speculative state proliferation.

### 20.2 Batch invariants

One Batch freezes one ASR execution profile:

```rust
pub struct BatchExecutionProfile {
    pub asr_engine: EngineId,
    pub asr_model: ModelId,
    pub device: DeviceId,
    pub compute_type: ComputeType,
    pub llm_profiles: StageLlmProfiles,
}
```

Every Job in the Batch uses the same ASR engine/model/device/compute type. Per-Job ASR model overrides are forbidden.

### 20.3 FIFO scheduling

Use a direct queue:

```text
Batch FIFO
Job FIFO within Batch
stage dependency order within Job
resource-lane concurrency where dependency-safe
```

Do not implement:

- dynamic model-affinity reordering;
- fairness aging for different models;
- interleaved model switching within a Batch;
- complex cross-Batch global optimization.

### 20.4 Resource lanes

Retain bounded lanes for:

- ffmpeg/CPU;
- ASR device;
- LLM Provider;
- disk commit.

Pipeline overlap remains allowed. For example, one Job may translate while another waits for or uses ASR, subject to the single-model Batch and resource constraints.

Backpressure MUST prevent extracting audio for hundreds of inputs when downstream queues and disk are saturated.

### 20.5 Model lifecycle

The model session lifetime is the Batch.

```text
Batch starts:
  acquire/start worker or helper
  load the Batch model once

Batch runs:
  reuse the model for every Job
  do not unload between Jobs
  do not switch model

Batch reaches Done, Failed, or Cancelled:
  unload model immediately
  release primary VRAM/unified-memory allocation
```

A single Job is treated as a one-Job Batch.

If one Job fails but Batch policy permits later Jobs to continue, keep the model loaded until the entire Batch reaches terminal state.

Worker process lifetime is separate from model lifetime. A model-free Ready process MAY remain, but it must not retain major model memory.

### 20.6 Pause and cancellation

Pause stops issuing new resource permits. Running units finish unless explicitly cancelled.

Cancellation:

- propagates to HTTP, ffmpeg, worker, or helper;
- preserves all committed artifacts/work units;
- marks uncommitted active units Cancelled;
- permits later resume/retry from remaining units.

### 20.7 Progress presentation

Do not calculate a weighted total Job percentage or maintain EMA stage-duration weights.

Expose only:

- current stage;
- current stage progress when measurable;
- active work unit;
- attempt count;
- completed Jobs / total Jobs in Batch.

Unknown progress is indeterminate, not a fabricated percentage.

---

## 21. State store, artifacts, and recovery

### 21.1 Authority split

Versioned JSON artifacts are the authoritative full Transcript/stage data.

SQLite is authoritative for:

- Job and Batch status;
- stage/work-unit status;
- revisions and leases;
- artifact indexes and hashes;
- events;
- Profiles and probe records.

Do not implement uncontrolled DB-and-file dual truth.

### 21.2 Core tables

At minimum:

```sql
batches(...);
jobs(...);
stages(...);
artifacts(
  id, job_id, stage, kind, path, content_hash, schema_version,
  producer_fingerprint, created_at, committed
);
work_units(
  id, job_id, stage, unit_kind, unit_index, input_hash,
  status, attempt, artifact_id, error_code, error_json,
  lease_owner, lease_expires_at, started_at, finished_at,
  UNIQUE(job_id, stage, unit_kind, unit_index, input_hash)
);
job_events(
  id, job_id, seq, event_type, payload_json, created_at,
  UNIQUE(job_id, seq)
);
profile_revisions(...);
llm_requests(...);
llm_capability_probes(...);
```

Migrations are numbered and checksummed.

### 21.3 Single writer

GUI, CLI, and scheduler MUST NOT independently write through arbitrary connections.

A Rust store actor serializes writes. Reads may use a pool.

Enable:

- WAL;
- foreign keys;
- busy timeout;
- application instance lock.

### 21.4 Stage commit protocol

A successful stage or work-unit artifact is committed as:

1. write `<artifact>.tmp`;
2. flush and close;
3. reread and validate schema/invariants;
4. calculate BLAKE3;
5. atomically rename to final path;
6. in one SQLite transaction, insert artifact/update unit or stage/add event;
7. publish in-process/UI event.

A process crash at any numbered boundary MUST recover deterministically.

### 21.5 Startup recovery

- remove or quarantine uncommitted `.tmp` files;
- if a committed DB artifact is missing or hash-invalid, return its stage/unit to Pending and record `ARTIFACT_CORRUPT`;
- expired Running leases return to Pending with incremented attempt;
- committed Done work units are not rerun;
- stale asynchronous results are discarded.

### 21.6 Revisions

Keep immutable revisions and a latest pointer. User edit transactions create new revisions; do not create a full revision for every keystroke.

Rollback selects an older revision; it does not destroy later history.

### 21.7 Cache keys

Canonicalize parameter JSON before hashing:

- sort keys;
- materialize defaults;
- convert paths to content identities where required;
- use fixed float representation;
- never hash Debug output.

General artifact key:

```text
blake3(input content identity | stage | normalized parameters | producer fingerprint)
```

LLM request key:

```text
blake3(
  Provider Profile revision |
  model |
  normalized messages |
  response schema hash |
  generation parameters |
  prompt bundle hash
)
```

Only validated final LLM results enter reusable cache.

### 21.8 Cache limits and deletion

Shared cache default: `20 GiB`, excluding model storage.

GC deletes only entries that are:

- unreferenced;
- not leased/running;
- outside retention policy.

Deleting a Job removes:

- Job DB records as defined by explicit delete flow;
- Job-private artifacts/snapshots.

It preserves:

- exported subtitle files;
- shared cache;
- shared models and runtimes.

Raw ASR and stage artifacts remain with the Job until the Job is deleted.

### 21.9 Database backups

Before database migrations:

- create an atomic backup;
- run integrity checks;
- preserve a recoverable previous copy on failure.

A manual diagnostic/export command SHOULD be provided.

---

## 22. GUI and CLI

### 22.1 Shared application services

Conceptually:

```text
                   CLI adapter
                  /
Application service
                  \
                   Tauri desktop adapter
```

The GUI is a shell over the same command/application layer as the CLI. It SHOULD NOT repeatedly spawn the CLI binary as a subprocess for normal operations.

Both use the same:

- command structs;
- validation;
- scheduler;
- store actor;
- event schemas;
- error codes;
- OutputPlanner;
- Doctor.

### 22.2 Exclusive processing instance

GUI and CLI processing are not allowed to run in parallel.

Use an application-level exclusive lock:

```text
GUI owns processing lock:
  CLI mutating/processing commands return INSTANCE_BUSY

CLI owns processing lock:
  GUI cannot start a scheduler and exits or opens only an explicitly safe read-only view
```

Do not implement:

- two schedulers;
- concurrent work-unit leasing by GUI and CLI;
- IPC forwarding of mutating commands between them in v1;
- GUI as a persistent daemon.

Read-only commands may be allowed individually if proven safe.

### 22.3 CLI first

Complete CLI/core through M5 before implementing the full GUI. An early Tauri shell may compile, but it must not drive architectural duplication.

### 22.4 CLI commands

Target shape:

```bash
videocaptionerr transcribe <files...> --profile <name>
videocaptionerr process <files...> --profile <name> --target-lang <lang>
videocaptionerr subtitle <input.srt|input.vtt> --target-lang <lang>
videocaptionerr export <job-id> --format srt --bilingual both
videocaptionerr jobs list
videocaptionerr jobs retry <id>
videocaptionerr jobs rm <id>
videocaptionerr worker doctor
videocaptionerr cache gc --max-size 20G
```

Final argument names may evolve, but semantic responsibilities should remain.

### 22.5 CLI machine events

`--json` writes NDJSON to stdout; human logs go to stderr.

Envelope includes:

```text
schema_version
event_id
job_id
timestamp
type
data
```

Within one major schema version, only backward-compatible optional additions are allowed.

Suggested exit codes:

```text
0 success
2 invalid arguments/config
3 dependency/runtime unavailable
4 input/probe failure
5 ASR failure
6 LLM failure
7 export failure
8 cancelled
9 partial Batch success
```

`jobs retry` retries failed work units by default. `--from-stage` explicitly invalidates that stage and later stages. `--dry-run` shows plan, downloads, outputs, and cache hits without computation.

### 22.6 GUI language

v1 GUI, CLI help, user-facing errors, and logs are English only. Do not introduce an i18n framework in v1.

Stable error codes remain uppercase English identifiers.

### 22.7 Desktop information architecture

One queue-centered interface:

```text
Batch/Job queue
-> selected Job stage timeline
-> subtitle editor
-> LLM request metadata/log view
-> runtime log view
-> model/runtime management
```

Single-file processing is a Batch of size one, not a separate pipeline.

### 22.8 Subtitle editor

Required behavior:

- virtualized table for 1000+ cues;
- columns for index, read-only time, source, translation;
- inline editing with revision CAS;
- low-confidence markers only for valid adapter confidence;
- view/restore filtered fragments;
- split at word boundary and merge adjacent cues for ASR-derived IR;
- no hidden timestamp mutation;
- stale automatic results never overwrite a user edit.

---

## 23. Preflight, errors, logs, and diagnostics

### 23.1 Job preflight

Before starting computation, verify:

1. source and selected audio track;
2. internal output-path conflicts;
3. ASR adapter/runtime/model availability;
4. device/runtime smoke test;
5. disk space;
6. valid language/export/stage combination;
7. Provider connectivity when immediately needed.

If an LLM Provider is unavailable, ASR and local splitting may still proceed. The Job enters `WaitingProvider` before the required LLM stage rather than being blocked at initial ASR.

Preflight failures SHOULD link to a concrete repair action.

### 23.2 Error categories

```text
Recoverable  automatic retry is appropriate
Degradable   fallback preserves usable output
Fatal        Job/work unit cannot continue without intervention
```

Every error contains:

- stable code;
- user-facing English message;
- structured diagnostic details;
- retry/degradation history;
- correlation ID;
- suggested repair where possible.

### 23.3 Structured logging

Use Rust `tracing` with spans/fields for:

```text
batch_id
job_id
stage
work_unit_id
request_id
worker_session_id
attempt
```

Python logs arrive via stderr or explicitly structured log events and are correlated by session/request.

### 23.4 LLM content logs

Default mode is `metadata-only`.

Record:

- Provider/model;
- request hash;
- token usage when available;
- latency;
- attempt;
- stage/work-unit IDs;
- error category/code.

Full messages/responses require explicit user opt-in per Profile or Job.

Content stays local and is never uploaded automatically.

Diagnostic bundles exclude source-media paths and LLM content by default unless the user explicitly includes them.

---

## 24. Testing strategy

### 24.1 No early CI requirement

Tests run locally during early development. Do not add GitHub Actions merely to satisfy this document.

A later release phase may add automated build/release workflows.

### 24.2 Suggested local entry points

```bash
cargo xtask check
cargo xtask test
cargo xtask test-faults
cargo xtask test-adapter --engine <engine>
cargo xtask package --target <target>
```

Exact commands may differ, but provide one documented aggregate local workflow.

### 24.3 Test layers

| Layer | Content | Network/GPU |
|---|---|---|
| pure unit | IR invariants, TextJoiner, split, export, keys, errors | no |
| property | random words/ranges/time boundaries/round trips | no |
| protocol | fake worker/helper, dirty stdout, seq, partial lines, crash | no |
| LLM contract | fake OpenAI-compatible server and failure matrix | local only |
| adapter conformance | real runtime against private corpus | optional marker |
| end-to-end | tiny media to deterministic subtitles plus resume | CPU default |
| packaging smoke | native install/doctor/first run on target OS | release gate |

### 24.4 Private corpus

The quality corpus is not stored in the public repository.

Use:

```text
VIDEOCAPTIONERR_CORPUS=/absolute/path/to/corpus
```

The repository may include:

- corpus manifest schema;
- loader;
- expected metadata format;
- tiny synthetic/protocol fixtures that are safe to distribute.

If the private corpus is absent, quality/adaptor corpus tests skip explicitly; unit, protocol, fake Provider, and fault tests still run.

### 24.5 No WER/CER infrastructure

v1 does not calculate or gate on:

- WER;
- CER;
- cross-engine quality scores;
- word-boundary mean error;
- model rankings.

Adapter conformance tests structural and operational behavior only:

- legal ordered timestamps;
- correct cancellation behavior or descriptor declaration;
- no stdout protocol pollution;
- normalized error mapping;
- no unbounded memory growth;
- complete fingerprints;
- stable handling of silence, mixed scripts, numbers, and long input.

### 24.6 Mandatory fault injection

The following are release-blocking at the milestone that introduces the component:

1. ffmpeg killed at 50% leaves no final `audio.wav`;
2. worker crashes after streamed segments; no partial final artifact is committed;
3. one ASR chunk fails; completed chunks remain cached;
4. one LLM batch misses a key; binary isolation preserves other output;
5. `Retry-After` is respected;
6. user edits a cue during an LLM request; stale result is discarded;
7. crashes before/after rename and before/after DB commit recover consistently;
8. same-stem outputs do not overwrite each other;
9. secrets are redacted from logs and diagnostics;
10. cache GC cannot delete leased or open model/artifact files;
11. GUI/CLI exclusive lock prevents dual schedulers;
12. Batch terminal transition unloads its model.

A milestone is not complete if its cancellation, timeout, crash, and recovery tests are deferred.

---

## 25. Implementation milestones

## M0 — Workspace and contracts

Implement:

- Rust workspace;
- `contracts` crate;
- IR and protocol schemas;
- stable error codes;
- `xtask` generation;
- SQLite migrations;
- store actor skeleton;
- atomic artifact helper;
- fake worker/helper;
- fake LLM Provider;
- exclusive instance lock;
- local test commands.

Gate:

```text
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
pure unit tests
protocol fixture round-trip
migration round-trip
artifact crash-point tests
instance-lock tests
```

## M1 — IR, media, and subtitle fundamentals

Implement:

- Transcript IR and revisions;
- TextJoiner;
- rule splitting;
- SRT/VTT import;
- SRT/VTT/ASS deterministic export;
- OutputPlanner;
- ffprobe;
- atomic ffmpeg extraction;
- media/PCM hashes;
- export preflight.

Gate:

- IR property tests;
- Unicode/mixed-script tests;
- malformed subtitle diagnostics;
- same-stem collision tests;
- ffmpeg cancellation/half-file tests;
- deterministic writer tests.

## M2 — whisper.cpp vertical slice

Implement:

- Rust helper process and protocol;
- whisper.cpp adapter;
- model manifest/downloader;
- model selection without default;
- cancel/kill process tree;
- raw ASR artifact;
- AsrNormalizer;
- `videocaptionerr transcribe` end-to-end.

Gate:

- helper crash/restart tests;
- protocol pollution tests;
- model digest failure tests;
- cancellation timeout/kill tests;
- CPU end-to-end subtitle export;
- adapter structural conformance.

## M3 — LLM pipeline

Implement:

- editable prompt files and stage snapshots;
- OpenAI-compatible client;
- automatic capability detection plus manual override;
- Generic/Ollama/LM Studio templates;
- token-aware packing;
- generic agent loop;
- LLM splitting;
- correction;
- reflect translation;
- wavefront context;
- batch binary isolation;
- metadata-only request logs;
- circuit breaker.

Do not implement automatic glossary or cost budgets.

Gate:

- fake Provider matrix: 401/403/429/500/timeout/bad JSON/missing keys;
- capability-probe cache/override tests;
- prompt-snapshot reproducibility;
- stale-revision test;
- binary-isolation test;
- prompt-injection fixture.

## M4 — Python runtimes

Implement:

- NDJSON worker protocol;
- faster-whisper runtime;
- mlx-whisper runtime;
- uv lock-based environments;
- runtime Doctor;
- worker heartbeat/backpressure;
- crash/OOM policy;
- capability descriptors.

Gate:

- dirty stdout;
- partial/oversized line;
- heartbeat during inference;
- bounded queue behavior;
- cooperative and forced cancellation;
- one active request enforcement;
- environment smoke test.

## M5 — Batch, work units, cache, and resume

Implement:

- FIFO Batch scheduler;
- one-model Batch invariant;
- Batch-scoped model session;
- model unload at Batch terminal state;
- resource lanes/backpressure;
- chunk/LLM work units;
- stage/work-unit recovery;
- cache and GC;
- pause/cancel/resume;
- CLI Job/Batch commands;
- machine NDJSON events.

Gate:

- resume only failed unit;
- lease expiration;
- cache corruption;
- GC race;
- Batch unload on Done/Failed/Cancelled;
- no model switch in Batch;
- CLI/GUI lock semantics at core level.

## M6 — Desktop shell

Implement:

- Tauri 2 application;
- React queue/Job views;
- stage timeline;
- subtitle editor;
- model/runtime management;
- Provider configuration and capability probe UI;
- filtered-fragment recovery;
- log/diagnostic views;
- exclusive instance behavior.

Gate:

- 1000+ cue virtualization;
- edit CAS conflict;
- stale result display;
- no duplicated business rule in frontend;
- no arbitrary shell execution;
- English-only user-facing consistency.

## M7 — Advanced long-audio and subtitle behavior

Implement:

- Rust-side VAD fallback;
- persisted ChunkPlan;
- core/read ownership clipping;
- long-audio chunk cache;
- enhanced export diagnostics;
- advanced split/edit behavior.

Automatic glossary remains optional and is not a milestone gate.

Gate:

- cut near continuous speech;
- no fuzzy text deduplication;
- no lost/duplicated core ownership;
- one failed chunk resumes independently;
- VAD-disabled/unsupported capability behavior.

## M8 — Native multi-platform release validation

Implement/verify:

- Linux package;
- Windows package;
- macOS Apple Silicon package;
- target sidecars;
- runtime installers;
- model downloads;
- notices and GPL compliance;
- native install/Doctor/first subtitle smoke tests;
- final decision on release automation and signing/notarization.

v1 is not released until all three target platforms pass native packaging smoke tests.

---

## 26. Definition of Done

A feature is complete only when all applicable statements are true:

- normal, cancellation, timeout, and crash paths have tests;
- no half-committed official artifact is possible;
- recovery behavior is deterministic;
- errors have stable codes and actionable messages;
- logs carry correlation IDs and no secrets;
- CLI and GUI use shared application services;
- no business rules are duplicated in Python or frontend;
- new configuration has schema version, validation, migration behavior, and cache impact;
- new ASR adapters pass structural conformance;
- user-visible behavior is reflected in this document or an approved ADR;
- `fmt`, `clippy -D warnings`, and relevant tests pass locally;
- every changed line is connected to the task.

---

## 27. Coding-task template

Every agent task SHOULD use:

```text
Goal:
  Implement one module or one state transition.

Inputs/outputs:
  Name exact Rust types, protocol messages, files, or schemas.

Allowed changes:
  List directories/files.

Forbidden changes:
  List frozen interfaces and unrelated areas.

Invariants:
  3-8 explicit rules.

Error codes:
  List stable errors introduced/used.

Tests first:
  List success, invalid input, cancel, timeout, crash, recovery cases.

Verification:
  Exact commands and expected outcomes.

Done:
  fmt + clippy -D warnings + relevant tests + no unrelated diff.
```

Do not assign “implement worker + scheduler + GUI” as one task. A suitable decomposition is:

1. protocol types/schema;
2. fake endpoint;
3. client routing;
4. process read/write loops;
5. lifecycle state machine;
6. cancel/kill;
7. real adapter;
8. conformance tests;
9. application event integration;
10. GUI adapter last.

---

## 28. Required ADRs

Create concise ADRs before or during M0/M1 for:

1. Rust core is the only business-logic layer;
2. Transcript word timeline is immutable;
3. A2 is the minimum full ASR capability;
4. versioned stdio NDJSON protocol;
5. one active transcription per worker;
6. whisper.cpp isolated Rust helper;
7. stage/work-unit atomic commit and leases;
8. silence/energy chunk cuts with core/read ownership;
9. LLM structured-output fallback and business validation;
10. automatic Provider capability detection with manual override;
11. same-Job translation wavefront;
12. field-level user protection and revisions;
13. immutable Profile revision snapshots;
14. directly editable prompts with stage snapshots;
15. runtime-family-isolated Python environments;
16. model manifests and digest validation;
17. plaintext TOML API keys with log redaction;
18. FIFO single-model Batch scheduling;
19. Batch-terminal model unload;
20. exclusive GUI/CLI processing instance;
21. JSON artifacts as full-data authority and SQLite as control authority;
22. no early CI/private corpus/no WER-CER v1 policy.

---

## Appendix A — Frozen default constants

| Constant | Default | Purpose |
|---|---:|---|
| `MAX_WORD_COUNT_CJK` | 25 | rule split hard upper bound |
| LLM target CJK length | 18 chars | LLM splitting target |
| `MAX_WORD_COUNT_ENGLISH` | 18 | rule split hard upper bound |
| LLM target English length | 12 words | LLM splitting target |
| `MAX_GAP` | 1500 ms | maximum tolerated inter-word gap |
| `RULE_SPLIT_GAP` | 500 ms | primary rule split gap |
| `MERGE_SHORT_GAP` | 200 ms | merge short group threshold |
| `MERGE_VERY_SHORT_GAP` | 500 ms | merge very short group threshold |
| `MERGE_MIN_WORDS` | 5 | short-group size |
| `MERGE_VERY_SHORT_WORDS` | 3 | very-short-group size |
| `TIME_GAP_WINDOW_SIZE` | 5 | moving gap window |
| `TIME_GAP_MULTIPLIER` | 3 | unusual-gap multiplier |
| `PREFIX_WORD_RATIO` | 0.6 | preferred forced split region |
| `SUFFIX_WORD_RATIO` | 0.4 | preferred forced split region |
| `SEGMENT_WORD_THRESHOLD` | 500 words | LLM split planning segment |
| LLM max items | 20 cues | item upper bound, also token limited |
| correction similarity | 0.7 | over-rewrite prevention |
| split retries | 2 | agent-loop retry maximum |
| correction/translation retries | 3 | before binary isolation/fallback |
| split temperature | 0.1 | default |
| correction temperature | 0.2 | default |
| translation temperature | 0.2-0.3 | default range |
| global max chunk | 600 s | reduced by adapter limit |
| chunk search radius | 30 s | silence/energy search |
| chunk context padding | 1.5 s | read-only context overlap |
| minimum chunk | 60 s | prevent tiny chunks |
| VAD threshold | 0.4 | unified default |
| VAD minimum silence | 500 ms | unified default |
| VAD speech padding | 200 ms | unified default |
| cancel grace | 3000 ms | before process-tree kill |
| worker segment channel | 256 | bounded backpressure |
| GUI progress rate | 5-10 Hz | event throttle |
| DB progress rate | <=1 Hz | write throttle |
| work-unit retries | 2 retries | general recoverable default |
| OOM strategy retry | 1 | then fail |
| shared cache | 20 GiB | excludes models |
| PCM format | 16 kHz mono s16le WAV | canonical audio |

---

## Appendix B — Stable error-code baseline

This list may grow, but meanings MUST remain stable within v1:

```text
INVALID_ARGUMENT
INVALID_CONFIG
CONFIG_MIGRATION_FAILED
INSTANCE_BUSY
INPUT_NOT_FOUND
INPUT_UNSUPPORTED
PROBE_FAILED
AUDIO_STREAM_NOT_FOUND
SOURCE_CHANGED
DISK_SPACE_INSUFFICIENT
FFMPEG_UNAVAILABLE
FFMPEG_FAILED
MODEL_NOT_FOUND
MODEL_DIGEST_MISMATCH
RUNTIME_UNAVAILABLE
RUNTIME_SMOKE_TEST_FAILED
DEVICE_UNAVAILABLE
ENGINE_CAPABILITY_INSUFFICIENT
OPTION_UNSUPPORTED
WORKER_BUSY
WORKER_START_FAILED
WORKER_PROTOCOL_ERROR
WORKER_TIMEOUT
WORKER_CRASHED
ASR_OOM
ASR_FAILED
TIMESTAMP_INVALID
ARTIFACT_CORRUPT
ARTIFACT_COMMIT_FAILED
CACHE_CORRUPT
LLM_AUTH_FAILED
LLM_MODEL_NOT_FOUND
LLM_RATE_LIMITED
LLM_PROVIDER_UNAVAILABLE
LLM_CONTEXT_EXCEEDED
LLM_INVALID_RESPONSE
LLM_VALIDATION_FAILED
STALE_RESULT
OUTPUT_CONFLICT
EXPORT_VALIDATION_FAILED
EXPORT_FAILED
CANCELLED
PARTIAL_BATCH_SUCCESS
```

---

## Appendix C — Final questionnaire decisions

| Q | Final decision |
|---:|---|
| 01 | Product/repository name `VideoCaptionerR`; binary/package prefix `videocaptionerr` |
| 02 | `GPL-3.0-only` |
| 03 | Three target platforms; current work local Linux x86_64; no early CI |
| 04 | React + TypeScript + Vite |
| 05 | pnpm |
| 06 | dedicated `contracts` crate |
| 07 | rusqlite + store actor |
| 08 | native app-data directory + `VIDEOCAPTIONERR_HOME` override |
| 09 | TOML configuration, including plaintext API keys |
| 10 | ULID IDs |
| 11 | Rust schema source + xtask generation |
| 12 | add M0 contracts/infrastructure milestone |
| 13 | reference source media by path/hash; do not copy by default |
| 14 | compute full media hash during preparation before ASR cache selection |
| 15 | Job directory `{ULID}_{sanitized_stem}` |
| 16 | versioned JSON artifacts are full Transcript authority |
| 17 | retain raw/stage artifacts with Job until deletion |
| 18 | 20 GiB shared cache, models excluded |
| 19 | immutable revisions and latest pointer |
| 20 | v1 imports SRT/VTT; ASS import later |
| 21 | default output `<source>/subtitles/` |
| 22 | restricted configurable output template |
| 23 | deleting Job preserves exports/shared cache/models |
| 24 | migration backup + integrity check |
| 25 | fake -> whisper helper -> faster-whisper -> mlx -> later Qwen/NeMo |
| 26 | official v1: whisper.cpp, faster-whisper, mlx-whisper |
| 27 | whisper.cpp in isolated Rust helper process |
| 28 | no default model |
| 29 | explicit user model download |
| 30 | bundled sidecars, advanced path override allowed |
| 31 | managed Python environments for faster-whisper and MLX only in v1 |
| 32 | at most one worker per device/runtime-family/model session; one active transcription |
| 33 | unload model when Batch reaches terminal state |
| 34 | auto-select only unambiguous single track; confirm multiple candidates |
| 35 | implement Rust VAD fallback before an adapter requires it |
| 36 | local ASR does not chunk by default |
| 37 | 30 s search, 1.5 s padding, 60 s minimum chunk |
| 38 | exclude filtered fragments but preserve/recover them |
| 39 | versioned built-in hallucination list plus Profile customization |
| 40 | one strategy-changing OOM retry, then fail |
| 41 | simple main ASR controls plus capability-aware advanced panel |
| 42 | language-aware CJK/Latin spacing |
| 43 | adapter-specific confidence thresholds |
| 44 | Generic OpenAI-compatible, Ollama, LM Studio templates |
| 45 | plaintext API key, no warning |
| 46 | metadata-only LLM logs by default |
| 47 | prompts directly editable; snapshot at stage start |
| 48 | automatic LLM capability detection retained; manual override has priority |
| 49 | conservative character/token estimate in v1 |
| 50 | default process enables LLM split, correction, reflect translation |
| 51 | previous accepted translation context; same-Job wavefront |
| 52 | automatic glossary disabled by default |
| 53 | no LLM cost/request budgets |
| 54 | per-cue fallback with `llm_failed` warning |
| 55 | auth stops immediately; repeated 429/5xx opens temporary circuit |
| 56 | CLI/core through M5 before full GUI |
| 57 | GUI and CLI processing are mutually exclusive; shared application services |
| 58 | English-only v1 interface/messages |
| 59 | FIFO, one ASR model per Batch, no runtime model switching |
| 60 | no weighted total progress; show current stage/unit and Job count |
| 61 | Provider outage does not block earlier ASR; wait at LLM stage |
| 62 | cancellation preserves committed work and supports resume |
| 63 | versioned stdout NDJSON machine interface |
| 64 | no updater and no telemetry in v1 |
| 65 | no early CI; local tests/manual platform builds; CI considered later |
| 66 | private local corpus via environment path |
| 67 | no WER/CER quality infrastructure in v1 |
| 68 | failure-injection paths are milestone completion gates |

---

## Appendix D — Explicit overrides of the earlier manuals

The following earlier recommendations are intentionally replaced:

| Earlier recommendation | Final rule |
|---|---|
| AGPL or permissive license discussion | repository is `GPL-3.0-only` |
| API key in OS credential store | plaintext TOML, no warning; still redact logs |
| manual-only LLM capabilities | automatic detection plus manual override |
| Prompt controlled only through immutable Profiles | editable files; immutable snapshot at stage start |
| automatic glossary workflow | disabled by default |
| LLM request/cost budgets | not implemented |
| global model affinity/fairness scheduler | FIFO single-model Batch |
| per-Job model unload | unload only after Batch terminal state |
| weighted/EMA total progress | current-stage progress only |
| GUI/CLI IPC cooperation | mutually exclusive processing instance; no mutating IPC in v1 |
| early three-platform CI | no early CI; local-first development |
| public repository quality corpus | private local corpus |
| WER/CER release gates | no WER/CER infrastructure in v1 |
| in-process whisper-rs | isolated Rust helper process |

All other compatible contracts from the two source manuals remain active.
