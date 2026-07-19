# VideoCaptionerR — Implementation Decisions and DDD Architecture Manual

> Final implementation baseline for coding and testing agents  
> Repository: `https://github.com/starfield17/VideoCaptionerR`  
> License: `GPL-3.0-only`  
> Status: **Frozen implementation baseline for v1, including the mandatory DDD architecture and existing-code migration plan**

---

## 0. Purpose and authority

This document combines and supersedes the implementation-relevant content of:

1. `subtitle-tool-architecture-manual(3).md`;
2. `subtitle-tool-architecture-manual-v1-addendum(1).md`;
3. the completed implementation questionnaire and all later corrections.

It is intended to be handed directly to Claude Code, Codex, or another coding agent. It defines product scope, architecture, contracts, defaults, persistence rules, failure behavior, milestones, and acceptance gates.

This DDD revision additionally defines:

- the domain model and aggregate boundaries;
- the application/use-case layer;
- owned ports and external adapters;
- bounded-context language;
- the required dependency inversion;
- a concrete migration plan for the partially implemented repository snapshot supplied with this document.

DDD requirements in this document are normative. They are not an optional refactoring style and MUST be completed before the Batch scheduler and desktop shell are allowed to expand the current dependency graph.

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


### 0.4 Existing-code baseline

The migration instructions in this document are based on static inspection of the supplied `VideoCaptionerR-main.zip` source snapshot on 2026-07-19.

The inspected snapshot contains approximately 10,000 lines of Rust across:

- `contracts`;
- `core`;
- `asr`;
- `llm`;
- `store`;
- `cli`;
- `test-support`;
- `whisper-helper`;
- `xtask`.

The repository has completed substantial M0-M2 work and contains an incomplete M3 LLM checkpoint. The coding agent MUST preserve already-correct behavior and tests while changing ownership and dependency direction.

The source snapshot has one known compile-blocking inconsistency:

```text
crates/llm/src/lib.rs declares `pub mod agent;`
but crates/llm/src/agent.rs does not exist.
```

Before measuring the migration baseline, the coding agent MUST either:

1. restore the intended `agent.rs` implementation from the active work branch; or
2. remove the module declaration temporarily if no code depends on it.

It MUST NOT invent a large agent-loop implementation merely to silence this missing module. Implement that functionality in its correct DDD layer during the relevant migration phase.

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
11. **Domain ownership:** business invariants live in domain aggregates or domain services, not in SQL, CLI branches, HTTP adapters, workers, or filesystem code.
12. **Dependency inversion:** application use cases own the ports they require; ASR, LLM, SQLite, ffmpeg, subtitle codecs, and desktop/CLI code implement or call those ports from the outside.
13. **Pragmatic DDD:** use aggregates, value objects, domain services, ports, and repositories only where they protect real invariants. Do not add ceremonial layers, generic repositories, or event sourcing.

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

## 4. Mandatory DDD architecture and workspace layout

VideoCaptionerR uses a pragmatic Domain-Driven Design plus ports-and-adapters architecture.

DDD here means:

- business terms have one precise meaning;
- aggregate roots own their invariants and legal state transitions;
- application use cases orchestrate work without owning infrastructure;
- ports are owned by the application side that needs them;
- external technology is implemented as replaceable adapters;
- CLI and GUI are inbound adapters;
- SQLite, ffmpeg, whisper runtimes, HTTP Providers, model downloads, and subtitle files are outbound adapters.

DDD does **not** mean:

- one crate per entity;
- a generic repository abstraction;
- getters/setters for every field;
- event sourcing;
- CQRS for simple reads;
- wrapping every standard type in a newtype;
- duplicating transport DTOs and domain types when they have identical stable semantics;
- adding empty `domain/application/infrastructure` folders inside every crate.

### 4.1 Ubiquitous language

Coding agents MUST use the following terms consistently.

| Term | Meaning and owner |
|---|---|
| `Batch` | Ordered execution request with one frozen ASR execution profile and one model-session lifetime |
| `Job` | Processing request for one source media or imported subtitle document |
| `Stage` | One named pipeline step such as Probe, ExtractAudio, ASR, Split, Correct, Translate, or Export |
| `WorkUnit` | Independently retryable and commit-able part of a Stage, such as an ASR chunk or LLM batch |
| `Artifact` | Immutable, validated, content-hashed output committed by a Stage or WorkUnit |
| `Transcript` | Aggregate root for normalized words, cues, timeline rules, revisions, and field provenance |
| `Cue` | Entity inside a Transcript identified by a stable cue ID |
| `Word` | Immutable timestamped ASR value owned by a Transcript |
| `ProfileRevision` | Immutable effective configuration snapshot referenced by a Batch/Job |
| `ModelSession` | Loaded ASR model bound to one Batch execution profile |
| `ProviderProfile` | LLM endpoint/model configuration with detected and overridden capabilities |
| `StageSnapshot` | Immutable prompt/config/input snapshot used by all work units in one Stage run |
| `Lease` | Time-bounded ownership of a running WorkUnit |
| `DomainEvent` | Fact produced by a legal domain transition; not an instruction to external technology |

Do not rename these concepts casually. In particular:

- `Job` is not a process/thread;
- `WorkUnit` is not the same as a Job;
- `Artifact` is not an arbitrary temporary file;
- `Transcript.revision` is not a database row version alone;
- `ModelSession` lifetime is the Batch, not an individual Job.

### 4.2 Bounded contexts and ownership

The implementation has two core domain areas and several supporting/integration areas.

#### 4.2.1 Subtitle Document domain

Owns:

- `Transcript`;
- `Word`;
- `Cue`;
- cue IDs and tombstones;
- timeline source;
- field provenance/revisions;
- manual split/merge/edit operations;
- rule-based sentence splitting;
- `TextJoiner`;
- subtitle-quality rules that do not depend on a specific file codec;
- stale-result protection inputs.

It MUST NOT know about:

- SQLite;
- ffmpeg;
- worker processes;
- HTTP Providers;
- output filesystem paths;
- Tauri;
- CLI formatting.

#### 4.2.2 Processing Workflow domain

Owns:

- `Batch`;
- `Job`;
- `Stage`;
- `WorkUnit`;
- legal lifecycle transitions;
- retry attempt counters and lease semantics;
- Batch single-model invariant;
- terminal-state semantics;
- cancellation semantics at the state-model level;
- immutable `ProfileRevision` identity;
- immutable `ArtifactRef` metadata after commit;
- domain events describing transitions.

It MUST NOT start processes, send HTTP, write files, or execute SQL.

#### 4.2.3 Supporting and integration areas

These are not allowed to redefine core business rules:

- Media integration: ffprobe, ffmpeg, media hashing, PCM extraction;
- ASR integration: helper/worker protocol, model/runtime adapters, normalization adapter details;
- LLM integration: OpenAI-compatible HTTP, capability probes, rate/circuit behavior;
- Persistence: SQLite repositories, migrations, artifact-file implementation;
- Subtitle I/O: SRT/VTT/ASS parsing and writing, output path allocation;
- Runtime/platform: application paths, configuration files, instance lock, sidecar discovery;
- Inbound interfaces: CLI and Tauri desktop.

### 4.3 Layer responsibilities

#### Domain layer

The domain layer contains:

- aggregate roots;
- entities;
- value objects;
- domain services;
- domain policies;
- domain errors;
- domain events.

The domain layer MUST:

- be deterministic;
- be testable without network, filesystem, database, process spawning, Tokio runtime, or environment variables;
- reject illegal state transitions;
- expose behavior-oriented methods rather than requiring callers to mutate fields;
- never depend on another VideoCaptionerR crate.

The domain layer MAY depend on small serialization/schema crates because the canonical Transcript artifact is itself a frozen product contract. This is an explicit pragmatic exception. Serialization attributes MUST NOT contain persistence-specific SQL or Provider behavior.

#### Application layer

`videocaptionerr-core` becomes the application layer.

It owns:

- commands and use-case request/response types;
- application services/use cases;
- ports required by use cases;
- orchestration across aggregates and adapters;
- transaction/commit boundaries;
- retry/degradation decisions defined by this manual;
- mapping domain events to outbound work;
- authorization of stale-result checks;
- resource-independent scheduling decisions.

It MUST NOT:

- depend on concrete `asr`, `llm`, `store`, `platform`, or subtitle-codec crates;
- call `std::fs`, `std::process`, `reqwest`, `rusqlite`, or Tauri;
- create SQL;
- discover binaries from PATH;
- parse CLI flags;
- format terminal output;
- own an HTTP client or worker client;
- directly create wall-clock timestamps or random IDs when deterministic injection is required.

#### Infrastructure/outbound adapter layer

Concrete outbound adapters include:

- `videocaptionerr-store`;
- `videocaptionerr-asr`;
- `videocaptionerr-llm`;
- `videocaptionerr-platform`;
- helper/worker executables.

They implement application ports and translate:

```text
application request
<-> technology-specific request/response/error
```

Adapters MUST NOT decide:

- pipeline order;
- whether a Stage is skipped;
- Batch FIFO semantics;
- when a model is unloaded relative to Batch terminal state;
- whether user edits may be overwritten;
- cache invalidation policy;
- retry budget beyond adapter-local transport attempts explicitly allowed here.

#### Inbound adapter layer

CLI and desktop:

- parse/render;
- acquire the exclusive processing instance through bootstrap/runtime services;
- translate user actions into application commands;
- subscribe to application events;
- never execute SQL or invoke ASR/LLM adapters directly;
- never duplicate validation already owned by domain/application.

#### Composition root

A dedicated bootstrap/runtime crate wires concrete adapters to application ports.

Only the composition root is allowed to know the full concrete graph.

### 4.4 Aggregate roots and invariants

#### 4.4.1 Transcript aggregate

`Transcript` remains the canonical subtitle-domain aggregate root.

It owns:

- immutable `words`;
- `cues`;
- `next_cue_id`;
- transcript revision;
- timeline source;
- cue field origins and revisions.

Legal mutations MUST occur through methods such as:

```rust
split_cue(...)
merge_cues(...)
replace_rule_split(...)
apply_corrected_text(...)
apply_translation(...)
edit_text(...)
edit_translation(...)
restore_filtered_fragment(...)
```

Those method names are illustrative; exact APIs MAY differ.

External crates MUST NOT directly:

- replace `words`;
- assign cue IDs;
- decrement revisions;
- alter ASR timestamps;
- overwrite user-origin fields;
- apply an LLM result without checking its bound revision.

During migration, public fields MAY remain temporarily for Serde compatibility, but new code MUST use behavior methods. The migration is complete only when illegal mutations are structurally difficult or tested by a narrow mutation API.

#### 4.4.2 Batch aggregate

`Batch` owns:

- ordered `JobId` membership;
- frozen `BatchExecutionProfile`;
- Batch lifecycle;
- cancellation intent;
- terminal result summary.

It enforces:

- all Jobs use the same ASR engine/model/device/compute type;
- Job order is stable FIFO;
- no Job-level ASR model override;
- terminal Batch cannot return to Running;
- model unload is requested exactly once after terminal transition.

The domain event should express `BatchReachedTerminal`; the application layer reacts by closing/unloading the Batch model session.

The domain does not call `unload_model()` itself.

#### 4.4.3 Job aggregate

`Job` owns:

- source identity and selected stream;
- frozen Profile revision reference;
- ordered Stage states;
- current Job status;
- committed Artifact references;
- cancellation/degradation/failure summary.

It enforces legal Stage progression. A Stage cannot become Done before its prerequisite state and committed artifact requirements are satisfied.

#### 4.4.4 WorkUnit aggregate

`WorkUnit` owns:

- unit identity and input hash;
- state;
- attempt;
- lease;
- error summary;
- committed Artifact reference.

It enforces:

- one active lease;
- Done units are not leased again;
- terminal result after cancellation does not become Done without an explicit new attempt;
- attempt increments on recovery/retry;
- artifact association occurs only through the commit operation;
- expired leases return through a legal recovery transition.

#### 4.4.5 ProfileRevision and ArtifactRef

`ProfileRevision` and committed `ArtifactRef` are immutable.

Do not expose update methods that modify an existing revision or committed artifact identity in place. Create a new revision/artifact instead.

### 4.5 Domain services and policies

Use a domain service only when behavior does not naturally belong to one aggregate/entity.

Approved domain services/policies include:

- rule sentence splitter;
- `TextJoiner`;
- subtitle-quality evaluation independent of output codec;
- translation/correction result validator;
- canonical cache-key input policy;
- legal Stage dependency policy.

Do not move technology-specific behavior into a “domain service.” Examples that remain adapters:

- ffmpeg command construction;
- OpenAI JSON payload construction;
- SQLite transactions;
- Hugging Face downloads;
- SRT byte encoding;
- process-tree kill.

### 4.6 Application ports

Ports are defined by `videocaptionerr-core`, because the application owns the need.

Do not define application-facing ports inside the concrete adapter crate merely because that adapter was implemented first.

Required port families:

```rust
#[async_trait]
pub trait BatchRepository { /* load/save Batch aggregate */ }

#[async_trait]
pub trait JobRepository { /* load/save Job aggregate and queries */ }

#[async_trait]
pub trait WorkUnitRepository { /* lease/recover/save WorkUnit */ }

#[async_trait]
pub trait ArtifactStore { /* write temp, validate, commit, read */ }

#[async_trait]
pub trait MediaGateway { /* probe, hash, extract */ }

#[async_trait]
pub trait AsrRuntime { /* open one Batch-scoped model session */ }

#[async_trait]
pub trait AsrSession { /* transcribe Jobs/chunks, unload/close */ }

#[async_trait]
pub trait LlmGateway { /* chat/capability operations used by use cases */ }

#[async_trait]
pub trait SubtitleGateway { /* import/export/output allocation */ }

#[async_trait]
pub trait EventPublisher { /* publish application/domain events */ }

pub trait Clock { /* deterministic current time */ }

pub trait IdGenerator { /* deterministic ULID-compatible IDs */ }
```

Exact signatures MAY be smaller. Avoid one method per SQL statement and avoid a single unbounded “god port.”

Repository interfaces are aggregate-oriented. For example:

```rust
save_job(job)
load_job(job_id)
save_work_unit(unit)
lease_next_ready_work_unit(...)
```

Do not expose:

```rust
execute_sql(...)
connection()
table_rows(...)
generic Repository<T>
```

### 4.7 Application use cases

At minimum, the application layer SHOULD converge on these use-case boundaries:

```text
CreateBatch
RunBatch
TranscribeJob
ProcessJob
ImportSubtitle
ExportJob
RetryFailedWorkUnits
CancelBatch
DeleteJob
UpdateCue
ProbeLlmCapabilities
InstallOrVerifyModel
Doctor
CacheGc
```

A use case:

1. accepts a command DTO;
2. loads or creates aggregates through repositories;
3. invokes aggregate behavior;
4. calls required ports;
5. commits artifacts through the required atomic protocol;
6. persists aggregate state;
7. publishes resulting events;
8. returns a response DTO.

A long-running Stage is not wrapped in one long SQLite transaction. Persist state transitions at explicit boundaries.

### 4.8 Batch-scoped ASR session

The application design MUST make the Batch model lifetime structurally clear.

Recommended shape:

```rust
pub trait AsrRuntime {
    async fn open_session(
        &self,
        profile: &BatchExecutionProfile,
    ) -> AppResult<Box<dyn AsrSession>>;
}

pub trait AsrSession {
    fn descriptor(&self) -> &EngineDescriptor;

    async fn transcribe(
        &mut self,
        request: AsrTranscribeRequest,
        events: &dyn EventPublisher,
    ) -> AppResult<AsrRawResult>;

    async fn close(self: Box<Self>) -> AppResult<()>;
}
```

`RunBatch`:

1. opens one session;
2. processes Jobs in FIFO order;
3. keeps the session across Job failures when Batch policy continues;
4. transitions the Batch to terminal;
5. closes/unloads the model once;
6. records cleanup failure without rewriting the already-determined Batch business result.

`TranscribeJob` MUST NOT unload or shut down the model.

### 4.9 Final workspace layout

The target v1 workspace is:

```text
VideoCaptionerR/
├── Cargo.toml
├── crates/
│   ├── domain/          # pure aggregates, value objects, domain services/policies/events
│   ├── contracts/       # external/persisted DTOs, protocol, event envelope, error codes, schemas
│   ├── core/            # application ports, commands, use cases, orchestration
│   ├── asr/             # ASR outbound adapters and worker/helper client
│   ├── llm/             # LLM outbound adapters, HTTP, capability probe, circuit
│   ├── store/           # SQLite repository/artifact adapter and migrations
│   ├── platform/        # config, paths, media/ffmpeg, subtitle I/O, instance lock, sidecars
│   ├── bootstrap/       # composition root shared by CLI and desktop
│   ├── cli/             # inbound CLI adapter and rendering only
│   ├── test-support/    # fake ports/adapters and deterministic clocks/IDs
│   └── whisper-helper/  # isolated whisper.cpp/fake protocol executable
├── apps/
│   └── desktop/         # Tauri inbound adapter
├── python/
│   └── runtimes/
│       ├── faster-whisper/
│       └── mlx-whisper/
├── prompts/
├── schemas/
├── migrations/
├── tools/
│   └── xtask/
└── docs/
    └── adr/
```

`platform` is one pragmatic supporting crate. Do not split it further unless compile/platform boundaries become materially painful.

### 4.10 Target dependency direction

The target internal dependency graph is:

```text
domain
  ↑
contracts
  ↑
core (application ports/use cases)
  ↑              ↑              ↑              ↑
store          asr            llm          platform
   \             |              |              /
    \            |              |             /
              bootstrap
                 ↑
           CLI / Desktop
```

More precisely:

```text
domain:
  depends on no VideoCaptionerR crate

contracts:
  may depend on domain
  MUST NOT depend on core or adapters

core:
  depends on domain + contracts
  MUST NOT depend on store/asr/llm/platform/bootstrap/cli/desktop

store/asr/llm/platform:
  may depend on domain + contracts + core
  MUST NOT depend on CLI/Desktop
  MUST NOT depend on bootstrap

bootstrap:
  depends on core and concrete adapters
  contains the composition root

CLI/Desktop:
  depend on bootstrap and stable command/event contracts
  MUST NOT depend directly on store/asr/llm internals
```

`whisper-helper` remains an independent executable depending on protocol/contracts and its runtime binding. It does not depend on application use cases.

### 4.11 Forbidden dependency and API rules

The following are mandatory architecture gates:

- no `videocaptionerr-store`, `videocaptionerr-asr`, `videocaptionerr-llm`, or `videocaptionerr-platform` dependency in `crates/core/Cargo.toml`;
- no `rusqlite`, `reqwest`, Tauri, ffmpeg process invocation, or filesystem writes in `domain` or application-use-case modules;
- no raw SQL outside `store`;
- no public SQLite connection escape hatch;
- no CLI/Tauri branch that calls an adapter directly;
- no adapter that imports CLI/Desktop;
- no domain type that uses `chrono::Utc::now()`, random ULID creation, environment variables, or global configuration;
- no error mapping by parsing human-readable error strings;
- no direct aggregate-state update from repository SQL without passing through an allowed transition or validated reconstruction path.

Add an `xtask verify-architecture` command that uses `cargo metadata` and source checks to enforce at least the Cargo dependency rules and raw-SQL/forbidden-import checks.

### 4.12 Contracts and domain serialization

Business ownership and schema ownership are distinct:

- `domain` owns business semantics and invariants;
- `contracts` owns persisted/external schema exposure, protocol envelopes, stable error codes, and cross-language generation.

For the canonical Transcript artifact, either of these is acceptable:

1. domain aggregate types derive Serde/Schemars directly and `contracts` re-exports them; or
2. `contracts` defines `TranscriptDto` with explicit lossless conversion to/from the domain aggregate.

For v1 migration, option 1 is recommended because it preserves the existing JSON schema and minimizes churn.

During migration:

```rust
// contracts compatibility module
pub mod transcript {
    pub use videocaptionerr_domain::subtitle::*;
}
```

This compatibility re-export MAY remain through v1. New domain/application code SHOULD import domain types from `videocaptionerr-domain` directly.

### 4.13 Composition root and shared shell

Add `videocaptionerr-bootstrap`.

It owns construction of:

- `Sqlite...Repository` adapters;
- artifact adapter;
- ffmpeg/media adapter;
- subtitle I/O adapter;
- ASR runtime adapter;
- LLM Provider adapters;
- event publisher;
- clock and ID generator;
- application use-case facade.

CLI and Tauri use the same bootstrap facade.

The bootstrap crate may resolve configuration and platform paths, but it MUST NOT contain subtitle business rules or pipeline state transitions.

### 4.14 Pragmatic limits

To keep the project “simple but work”:

- do not add a dependency-injection framework;
- use explicit constructors and `Arc<dyn Port>` where runtime polymorphism is required;
- use concrete types in pure domain tests;
- do not create a message bus for in-process calls;
- domain events may be simple enums returned from methods;
- do not implement event sourcing;
- do not add generic Unit of Work abstractions; create explicit repository commit operations needed by Stage/artifact atomicity;
- do not split the domain crate by bounded context into more crates in v1;
- do not rename `videocaptionerr-core` merely to make it say “application”; document its role and keep the stable package name.

### 4.15 Technology selections

```text
Rust async runtime: Tokio, outside the pure domain layer
Desktop: Tauri 2
Frontend: React + TypeScript + Vite
Frontend package manager: pnpm
Database access: rusqlite in the store adapter
Database write model: true single-writer store actor
Public identifiers: ULID-compatible domain IDs generated through IdGenerator
User-editable configuration: TOML in the platform adapter
Schema generation: domain/contracts -> xtask -> JSON Schema / TS / Python fixtures
Dependency wiring: explicit bootstrap constructors, no DI framework
```

Do not introduce an ORM unless a later decision explicitly replaces `rusqlite`.


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

### 6.1 Business and contract sources of truth

The sources of truth are split deliberately:

```text
crates/domain:
  business semantics, aggregates, invariants, legal transitions

crates/contracts:
  persisted/external schemas, protocol, events, stable error codes,
  artifact metadata, capability DTOs, and schema generation surface
```

The canonical Transcript business type is owned by `domain`. For v1, `contracts` SHOULD re-export the same type for schema compatibility rather than maintain a divergent copy.

`xtask` generates:

- JSON Schema;
- TypeScript interfaces;
- Python schema fixtures/constants;
- stable error-code lists;
- optional architecture dependency reports.

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


### 7.7 Aggregate mutation boundary

`Transcript` is not a passive transport bag even when it derives Serde.

New code MUST use domain methods for:

- cue allocation;
- split/merge;
- source or translation edits;
- LLM result application;
- filtered-fragment restoration;
- revision increments;
- user-origin protection.

Deserialization/reconstruction MUST call `validate()` or an equivalent checked constructor before the object is used.

Repository and adapter code may reconstruct from persisted data, but MUST NOT apply business mutations directly.

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


### 20.8 Workflow aggregate transitions

Persisted state strings are representations of typed domain states, not the state model itself.

At minimum define typed enums for:

```rust
BatchStatus
JobStatus
StageStatus
WorkUnitStatus
```

Transition methods MUST reject illegal transitions. Examples:

```text
Pending -> Running
Running -> Done / DoneDegraded / Failed / Cancelled / WaitingProvider
Running lease expired -> Pending with attempt + 1
Done -> no transition except explicit invalidation creating a new run/revision
Batch terminal -> no return to Running
```

SQL MUST NOT contain hidden transition logic that is absent from the domain/application layer.

### 20.9 Use-case orchestration

The current linear pipeline is implemented as application use cases and Stage runners, not one function that owns every adapter.

Recommended decomposition:

```text
RunBatch
  -> open Batch-scoped ASR session
  -> for each ordered Job:
       PrepareMedia
       RunAsrStage
       RunSplitStage
       RunCorrectStage
       RunTranslateStage
       RunExportStage
  -> finalize Batch
  -> unload model session
```

Stage runners MAY be internal application services. They must still depend only on ports.

Every Stage runner has explicit:

- input snapshot identity;
- ready/prerequisite check;
- work-unit plan;
- success artifact kind;
- degradation/failure mapping;
- cancellation boundary;
- commit boundary.

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


### 21.3.1 Repository ownership

Repository traits live in `core::ports`, while SQLite implementations live in `store`.

The store adapter MUST expose domain/application operations rather than a public connection.

Remove or make private any API equivalent to:

```rust
pub fn conn(&self) -> &rusqlite::Connection
```

CLI and desktop MUST use application queries/use cases such as:

```text
ListJobs
DeleteJob
RetryFailedWorkUnits
```

They MUST NOT prepare SQL.

### 21.3.2 True single-writer behavior

The inspected code uses `Arc<Mutex<Store>>`, which serializes callers but is not a dedicated store actor and may block async executor threads.

Before M5 scheduler concurrency, replace it with one of:

1. a dedicated blocking database thread receiving typed commands and replying through oneshot channels; or
2. a strictly controlled `spawn_blocking` executor with one owned connection and typed command queue.

Option 1 is recommended.

The application layer only sees async repository ports. It does not know whether rusqlite is blocking.


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

### 22.1 Shared application services and composition root

Conceptually:

```text
CLI adapter --------\
                     -> bootstrap -> application use cases -> owned ports
Tauri adapter ------/                                  <- concrete adapters
```

The GUI is a shell over the same command/application layer as the CLI. It MUST NOT repeatedly spawn the CLI binary for normal operations.

Both use the same:

- application command/response types;
- validation;
- Batch scheduler;
- repositories through ports;
- event schemas;
- error codes;
- output planning use case;
- Doctor use case.

The CLI/Desktop crates MUST NOT directly depend on concrete store/ASR/LLM internals once the DDD migration is complete.


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


### 24.7 Architecture tests

Add a local architecture gate:

```bash
cargo run -p xtask -- verify-architecture
```

It MUST fail when:

- `domain` depends on another VideoCaptionerR crate;
- `core` depends on `store`, `asr`, `llm`, `platform`, `bootstrap`, `cli`, or desktop;
- CLI/Desktop directly depend on store internals;
- raw SQL exists outside `store`;
- `domain` imports `reqwest`, `rusqlite`, Tauri, Tokio process, or filesystem/process APIs;
- application-use-case modules invoke filesystem/process/network/database APIs;
- a public `Store::conn()`-style escape hatch exists;
- known aggregate fields are directly mutated outside approved domain modules after the compatibility phase.

The check SHOULD use `cargo metadata` for dependency rules and targeted source scanning for forbidden imports. It is a local gate and does not imply early CI.

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


## M2.5 — Mandatory DDD extraction and dependency inversion

This migration is required before expanding the current M3 WIP or starting M5 scheduler work.

Implement:

1. add `videocaptionerr-domain`;
2. move/re-export Transcript and IDs without changing JSON shape;
3. move rule split and TextJoiner into the domain;
4. add Batch/Job/Stage/WorkUnit typed aggregates and transitions;
5. define application ports in `videocaptionerr-core`;
6. replace `run_transcribe` with an injected `TranscribeJob` use case;
7. add `platform` and `bootstrap` crates;
8. make store/ASR/LLM/platform implement application ports;
9. remove concrete adapter dependencies from `core`;
10. remove direct SQL and concrete adapter calls from CLI;
11. add architecture verification tests;
12. preserve M0-M2 observable behavior and artifact schemas.

Gate:

```text
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo run -p xtask -- verify-architecture
existing fake-helper transcribe vertical slice still passes
existing JSON schemas remain backward compatible
CLI jobs list/delete use application use cases, not SQL
core Cargo.toml has no store/asr/llm/platform dependency
```

Do not continue building a scheduler around the pre-DDD `run_transcribe` function.

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


### 26.1 DDD completion conditions

A task that changes business behavior is incomplete unless:

- the invariant has one domain owner;
- the application use case invokes behavior through that owner;
- infrastructure does not reproduce the rule;
- repository updates preserve legal transitions;
- adapter errors are translated to stable application errors;
- tests cover the aggregate transition and the use-case path;
- the dependency direction remains valid.

An infrastructure task is incomplete if it requires `core` to import the adapter.

A CLI/GUI task is incomplete if it performs SQL, filesystem orchestration, worker calls, or Provider calls directly.

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

Dependency rule:
  State which crate/layer may depend on which other crate/layer.

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

1. Rust domain owns business invariants; `core` is the application/use-case layer;
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
23. pragmatic DDD and bounded-context ownership;
24. domain/application/infrastructure dependency inversion;
25. application-owned ports and adapter implementations;
26. Transcript, Batch, Job, and WorkUnit aggregate boundaries;
27. shared bootstrap composition root for CLI/Desktop;
28. no raw SQL or concrete adapter access from inbound interfaces.

The existing repository ADR `docs/adr/0001-rust-core-business-logic.md` is partially superseded. Replace it with a new ADR or amend it so that:

```text
domain = owner of business invariants and domain behavior
core   = owner of application use cases, ports, and orchestration
Python/GUI/adapters = no duplicated business rules
```

Its original intent—Rust, not Python or GUI, owns business behavior—remains valid.

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

---

## Appendix E — Existing-code DDD migration plan

This appendix is specific to the inspected `VideoCaptionerR-main.zip` snapshot. It tells a coding agent what to change next.

### E.1 Static assessment summary

The current code has strong foundations:

- `Transcript` already validates timestamps/ranges and owns revision/cue-ID helpers;
- word timeline immutability is represented;
- worker/helper protocol and capability descriptor are separated;
- JSON artifacts and SQLite metadata are distinguished;
- fake ASR and fake LLM components exist;
- LLM capability probing, circuit breaker, prompt snapshot utilities, and token estimation have begun;
- ADRs already freeze most non-DDD decisions.

The current code is not yet DDD-compliant because:

1. `core` depends directly on `store`, `asr`, and `llm`;
2. `core::pipeline::run_transcribe` directly performs application orchestration, process/runtime control, filesystem writes, output planning, and SQLite state changes;
3. `core` contains concrete ffmpeg/ffprobe, filesystem subtitle I/O, configuration I/O, and path logic;
4. CLI directly opens SQLite, calls `Store::conn()`, prepares SQL, and deletes rows;
5. ports are currently owned by adapter crates (`AsrEngine`, `LlmProvider`) rather than the application-facing need;
6. persisted lifecycle states are mostly raw strings or infrastructure enums, not domain aggregates;
7. current model unload happens inside one Job transcription;
8. failure paths may return while Job/WorkUnit remain `running`;
9. the work-unit placeholder input identity is based on a path string;
10. `StoreHandle` is a mutex wrapper, not the documented dedicated actor;
11. human error text is parsed to determine CLI exit codes;
12. `contracts` mixes the domain aggregate with transport/protocol ownership;
13. aggregate fields can be directly mutated across crates;
14. `crates/llm/src/lib.rs` references a missing `agent.rs`.

This is a dependency/ownership migration, not a rewrite.

### E.2 Required target crates

Add to the workspace:

```text
crates/domain
crates/platform
crates/bootstrap
```

Add workspace dependencies:

```toml
videocaptionerr-domain = { path = "crates/domain" }
videocaptionerr-platform = { path = "crates/platform" }
videocaptionerr-bootstrap = { path = "crates/bootstrap" }
```

Do not delete existing crates.

### E.3 File ownership map

| Current file/module | Target owner | Required action |
|---|---|---|
| `contracts/src/transcript.rs` | `domain/src/subtitle/transcript.rs` | move business type/behavior; keep contracts compatibility re-export |
| `contracts/src/ids.rs` | `domain/src/identity.rs` | move domain IDs/generator-independent value objects; re-export from contracts |
| `core/src/constants.rs` | domain/application/platform by meaning | move subtitle policy constants to domain; leave non-domain defaults near owning adapter |
| `core/src/text_joiner.rs` | `domain/src/subtitle/text_joiner.rs` | move with tests unchanged |
| `core/src/split/rule.rs` | `domain/src/subtitle/split.rs` | move as domain service; map domain errors at boundary |
| `core/src/subtitle/preflight.rs` | domain quality policy + platform report adapter | split pure cue/timing diagnostics from codec/file concerns |
| `core/src/media/probe.rs` | `platform/src/media/probe.rs` | move concrete ffprobe adapter |
| `core/src/media/extract.rs` | `platform/src/media/extract.rs` | move concrete ffmpeg adapter |
| `core/src/media/hash.rs` | `platform/src/media/hash.rs` | move file/PCM hashing adapter |
| `core/src/subtitle/import.rs` | `platform/src/subtitle_io/import.rs` | move SRT/VTT parsing adapter |
| `core/src/subtitle/export.rs` | `platform/src/subtitle_io/export.rs` | move deterministic file writer adapter |
| `core/src/subtitle/time.rs` | `platform/src/subtitle_io/time.rs` | move codec time parsing/formatting |
| `core/src/subtitle/planner.rs` | `platform/src/subtitle_io/planner.rs` | move filesystem conflict allocator; expose through application port |
| `core/src/config.rs` | `platform/src/config.rs` | move TOML I/O; keep effective-config command DTO in application/contracts |
| `core/src/pipeline.rs` | `core/src/use_cases/transcribe_job.rs` | rewrite as injected use case; no direct concrete adapter imports |
| `asr/src/engine.rs` | adapter plus `core::ports::asr` | application-facing session port belongs in core; adapter implements it |
| `llm/src/provider.rs` | LLM adapter internal + `core::ports::llm` | retain HTTP/provider primitive; implement application gateway |
| `store/src/store.rs` | repository adapter | implement core repository ports; remove connection escape hatch |
| `store/src/paths.rs` | `platform/src/paths.rs` | move application/platform path ownership |
| `store/src/instance_lock.rs` | `platform/src/instance_lock.rs` | move process-instance concern |
| `store/src/artifact.rs` | store/artifact adapter | keep atomic file implementation if cohesive; implement `ArtifactStore` port |
| `cli/src/main.rs` | inbound adapter | parse commands/render responses only; call bootstrap application facade |
| `test-support/fake_asr.rs` | fake application port | implement `AsrRuntime/AsrSession`, not only adapter-owned trait |
| `test-support/fake_llm.rs` | fake application port | implement `LlmGateway`; lower-level provider fake may remain |
| `whisper-helper` | protocol helper | keep independent; no domain/application dependency needed |
| `xtask` | architecture/schema tooling | add `verify-architecture` |

### E.4 Migration sequence

The sequence is mandatory because it keeps the tree buildable and avoids a big-bang rewrite.

#### DDD-0 — Freeze and repair baseline

Tasks:

1. restore or remove the missing `llm::agent` module declaration;
2. run and record baseline:
   ```bash
   cargo fmt --check
   cargo clippy --workspace --all-targets --all-features -- -D warnings
   cargo test --workspace
   ```
3. run one fake-helper transcription;
4. save generated schema hashes;
5. do not change behavior yet.

Acceptance:

- baseline compiles;
- failures are recorded before migration;
- no speculative implementation is added.

#### DDD-1 — Extract domain without changing public JSON

Tasks:

1. create `videocaptionerr-domain`;
2. move Transcript/Word/Cue/timeline/provenance logic;
3. move typed IDs;
4. create `DomainError` and checked constructors;
5. move `TextJoiner`;
6. move rule splitting and its constants;
7. add workflow state enums and transition tests;
8. make `contracts` re-export migrated types;
9. preserve existing serialized field names and schema version.

Acceptance:

- old imports remain compiling through compatibility re-exports;
- transcript JSON schema has no unintended breaking change;
- domain has no internal VideoCaptionerR dependency;
- domain tests run without filesystem/process/network/database.

#### DDD-2 — Add application ports and use-case skeleton

Create:

```text
crates/core/src/
├── commands/
├── ports/
│   ├── repositories.rs
│   ├── media.rs
│   ├── asr.rs
│   ├── llm.rs
│   ├── subtitle.rs
│   ├── artifact.rs
│   ├── events.rs
│   └── system.rs
├── use_cases/
│   ├── transcribe_job.rs
│   ├── run_batch.rs
│   ├── list_jobs.rs
│   ├── delete_job.rs
│   └── probe_llm_capabilities.rs
└── lib.rs
```

Tasks:

1. define the minimum port signatures needed by current behavior;
2. implement `TranscribeJob` using injected ports;
3. do not yet delete the old `run_transcribe`;
4. create fakes for every new port;
5. reproduce current fake-helper vertical slice in a pure application test.

Acceptance:

- the new use case contains no concrete adapter imports;
- application tests use fakes only;
- no DI framework;
- no generic repository.

#### DDD-3 — Extract concrete platform adapters

Tasks:

1. add `videocaptionerr-platform`;
2. move media/config/path/instance-lock/subtitle-I/O code;
3. implement `MediaGateway` and `SubtitleGateway`;
4. preserve output names and artifact byte determinism;
5. keep compatibility re-exports temporarily if needed.

Acceptance:

- `core` no longer uses `std::fs`/`std::process` for pipeline work;
- existing media/subtitle tests pass in platform;
- deterministic output hashes remain unchanged.

#### DDD-4 — Adapt ASR/LLM/store

ASR:

1. define `AsrRuntime` and Batch-scoped `AsrSession` in core;
2. implement them around current `WorkerClient`;
3. keep helper protocol unchanged;
4. move normalization ownership deliberately:
   - adapter-specific unit/time normalization remains ASR adapter;
   - creation/acceptance of valid Transcript uses domain checked construction.

LLM:

1. retain OpenAI HTTP, probing, circuit, tolerant parse, and low-level Provider types in `llm`;
2. implement `LlmGateway`;
3. place subtitle-specific correction/translation validation in domain/application, not the HTTP adapter;
4. implement the missing generic agent-loop behavior in application only when M3 requires it.

Store:

1. implement typed aggregate repository ports;
2. map rows to checked domain reconstruction;
3. remove public `conn()`;
4. move ULID/time creation to injected application services where required;
5. replace raw status strings at boundaries with typed states;
6. retain migration SQL and atomic transaction behavior.

Acceptance:

- adapters depend inward on core ports;
- core has no adapter dependencies;
- CLI cannot access SQL;
- stable `VcError` mapping is structural.

#### DDD-5 — Add bootstrap and migrate CLI

Tasks:

1. add `videocaptionerr-bootstrap`;
2. wire repositories, ports, clock, IDs, and application facade;
3. migrate `doctor`, `transcribe`, `jobs list`, and `jobs rm`;
4. remove CLI dependencies on `store`, `asr`, and `llm` internals;
5. remove SQL from CLI;
6. replace string-matching exit mapping with `VcError.code`/typed application errors.

Acceptance:

```text
rg "SELECT |INSERT |UPDATE |DELETE " crates/cli -> no matches
rg "\.conn\(" crates/cli -> no matches
CLI Cargo.toml -> no store/asr/llm direct dependency
```

CLI output and exit semantics remain compatible.

#### DDD-6 — Replace old pipeline and correct lifecycle

Tasks:

1. switch production transcribe command to `TranscribeJob`;
2. delete or reduce the old `run_transcribe` compatibility wrapper;
3. ensure every failure path updates Job/Stage/WorkUnit state;
4. remove `media_hash_placeholder(path)`;
5. compute/commit correct input hashes;
6. commit Probe, Extract, ASR, Split, and Export artifacts with correct Stage labels;
7. stop ignoring directory/model-cleanup errors without structured recording;
8. move model unload from Job use case to `RunBatch`;
9. create a one-Job Batch for the existing transcribe command.

Acceptance:

- a Job failure never remains Running without a live lease;
- current single-file behavior still works;
- model is loaded once and unloaded after Batch terminal;
- Stage artifact metadata names the real producing Stage.

#### DDD-7 — Implement true store actor before M5 concurrency

Tasks:

1. create typed store commands;
2. own the rusqlite connection on one dedicated blocking thread;
3. return through oneshot responses;
4. keep reads either on that actor or a clearly safe read path;
5. eliminate blocking mutex use from async application paths;
6. add actor shutdown/recovery tests.

Acceptance:

- one writer connection;
- no `Arc<Mutex<Store>>` on async request paths;
- cancellation does not corrupt the command channel;
- migrations and artifact transactions remain deterministic.

#### DDD-8 — Continue M3-M8 on the new architecture

Only after DDD-0 through DDD-6 pass may coding agents add substantial new LLM Stage orchestration or Batch scheduler behavior.

M5 MUST also require DDD-7.

### E.5 Current `run_transcribe` changes

The existing function currently performs too many responsibilities:

```text
input validation
Job ID and directory creation
DB Job/WorkUnit insert
probe
audio selection
artifact write
media hash
ffmpeg extraction
PCM hash
worker spawn
capability validation
model load
event drain
ASR
model unload/shutdown
raw artifact write
normalization
split
output planning
export preflight
export write
artifact metadata creation
DB commit
Job finalization
```

Replace it with:

```text
RunBatchUseCase
  owns Batch/session lifecycle

TranscribeJobUseCase
  orchestrates one Job through ports

domain services
  own split/TextJoiner/Transcript invariants

platform adapter
  owns ffprobe/ffmpeg/files/subtitle codecs/output allocation

ASR adapter
  owns worker/helper communication

store adapter
  owns SQL and atomic metadata transaction
```

Do not merely divide the original function into many private functions while preserving concrete dependencies. The dependency direction must change.

### E.6 Required new domain APIs

The exact signatures may vary, but tests must demonstrate behavior equivalent to:

```rust
impl Transcript {
    pub fn apply_rule_split(&self, plan: SplitPlan) -> DomainResult<Self>;
    pub fn edit_text(&self, cue_id: CueId, expected_revision: u64, text: String)
        -> DomainResult<Self>;
    pub fn edit_translation(
        &self,
        cue_id: CueId,
        expected_revision: u64,
        translation: String,
    ) -> DomainResult<Self>;
    pub fn apply_llm_text(
        &self,
        binding: LlmResultBinding,
        updates: Vec<CueTextUpdate>,
    ) -> DomainResult<Self>;
}
```

```rust
impl WorkUnit {
    pub fn lease(...);
    pub fn complete(artifact: ArtifactRef, ...);
    pub fn fail(error: DomainFailure, ...);
    pub fn cancel(...);
    pub fn recover_expired(...);
}
```

```rust
impl Batch {
    pub fn start(...);
    pub fn record_job_terminal(...);
    pub fn cancel(...);
    pub fn finish(...);
}
```

Do not add setters such as `set_status(String)`.

### E.7 Error mapping

Use three error levels:

```text
DomainError
  illegal business state/invariant

ApplicationError
  use-case failure, adapter category, cancellation/degradation

VcError / protocol error DTO
  stable cross-boundary error code and diagnostics
```

`From`/mapping functions MUST be explicit.

Do not determine an exit code by searching human message text. The inspected CLI currently does this and it must be replaced.

### E.8 Transaction and artifact boundary

A repository `save(job)` call alone is not enough to satisfy artifact atomicity.

The application-owned commit port must represent the required operation:

```text
validated final file exists
+ artifact metadata insert
+ WorkUnit/Stage state transition
+ event append
```

The store adapter executes the metadata part in one SQLite transaction after atomic rename.

The domain/application layer decides that the transition is legal; the store adapter guarantees persistence atomicity.

### E.9 Compatibility policy

During migration:

- preserve CLI command names and output where feasible;
- preserve protocol envelopes;
- preserve schema versions unless a deliberate migration is created;
- preserve artifact filenames;
- preserve current tests;
- use re-exports/deprecated wrappers for one migration phase;
- remove compatibility APIs once all internal callers move.

Do not keep duplicate active implementations of split, export planning, or pipeline orchestration.

### E.10 Coding-agent task order

Recommended agent task queue:

```text
DDD-001 repair missing llm agent module / establish baseline
DDD-002 add domain crate and compatibility re-exports
DDD-003 move Transcript and IDs
DDD-004 move TextJoiner and rule split
DDD-005 add workflow aggregates and transition tests
DDD-006 add application ports
DDD-007 implement fake ports and TranscribeJob use case
DDD-008 add platform crate and move media
DDD-009 move subtitle I/O/config/paths/lock
DDD-010 implement store repository ports; remove conn escape
DDD-011 implement ASR Batch session port
DDD-012 implement LLM gateway port
DDD-013 add bootstrap composition root
DDD-014 migrate CLI jobs queries and transcribe
DDD-015 replace old pipeline and fix failure/artifact lifecycle
DDD-016 implement architecture xtask
DDD-017 replace StoreHandle mutex with true actor
DDD-018 resume M3 agent loop/correct/translate implementation
```

Each task MUST use the task template in §27 and include allowed/forbidden files.

---

## Appendix F — DDD dependency matrix

`ALLOW` means the dependency is permitted, not mandatory.

| From \ To | domain | contracts | core | store | asr | llm | platform | bootstrap | CLI/Desktop |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| domain | — | NO | NO | NO | NO | NO | NO | NO | NO |
| contracts | ALLOW | — | NO | NO | NO | NO | NO | NO | NO |
| core | ALLOW | ALLOW | — | NO | NO | NO | NO | NO | NO |
| store | ALLOW | ALLOW | ALLOW | — | NO | NO | NO | NO | NO |
| asr | ALLOW | ALLOW | ALLOW | NO | — | NO | platform only if strictly required | NO | NO |
| llm | ALLOW | ALLOW | ALLOW | NO | NO | — | NO | NO | NO |
| platform | ALLOW | ALLOW | ALLOW | optional narrow use | NO | NO | — | NO | NO |
| bootstrap | ALLOW | ALLOW | ALLOW | ALLOW | ALLOW | ALLOW | ALLOW | — | NO |
| CLI/Desktop | NO direct domain mutation | contracts/commands | via bootstrap facade | NO | NO | NO | NO | ALLOW | — |

Any exception requires an ADR and must not create a cycle.

---

## Appendix G — DDD review checklist

Before merging a change, answer:

### Domain

- Which aggregate or domain service owns the invariant?
- Can the rule be tested without I/O?
- Is an illegal state representable or directly mutable?
- Does a domain method return a meaningful error/event?

### Application

- What use case owns orchestration?
- Are all external actions behind owned ports?
- Are commit and cancellation boundaries explicit?
- Is model/session lifetime at Batch scope?

### Infrastructure

- Does the adapter only translate technology-specific behavior?
- Does it avoid pipeline/scheduling policy?
- Does it map errors structurally?
- Does it preserve atomic artifact and protocol rules?

### Inbound adapters

- Is CLI/GUI only parsing/rendering/calling use cases?
- Is there any direct SQL, HTTP, worker, ffmpeg, or filesystem orchestration?
- Are machine events and exit codes typed?

### Tests

- Is the aggregate transition unit-tested?
- Is the use case tested with fakes?
- Is the adapter contract tested independently?
- Are cancellation, failure, recovery, and stale-result paths covered?
- Does `xtask verify-architecture` pass?

