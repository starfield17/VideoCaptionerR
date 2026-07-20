# PR2 Crash-Point Matrix

The production stage commit path is `publish prepared artifact -> SQLite
transaction -> outbox`. A normal SQLite error removes a file published by the
failed call. The recovery path is still authoritative for a process that dies
between those operations.

| Boundary | Restart state | Recovery action | Automated coverage |
| --- | --- | --- | --- |
| before `.partial` creation | no official artifact | no-op; Job/WorkUnit remains retryable | `injected_stage_commit_faults_converge_after_recovery` (`BeforeTempWrite`) |
| after `.partial` fsync, before rename | partial file only | move `.partial` to `.recovery-quarantine` | same test (`AfterTempWrite`) |
| after rename, before DB transaction | final file with no DB reference | quarantine the orphan final file | same test (`AfterRename`) |
| after artifact row insert | transaction not committed | SQLite rollback; quarantine final file | same test (`AfterArtifactInsert`) |
| after WorkUnit CAS update | transaction not committed | SQLite rollback; WorkUnit cannot appear Done alone | same test (`AfterWorkUnitUpdate`) |
| after Job/stage CAS update | transaction not committed | SQLite rollback; stage cannot appear Done alone | same test (`AfterJobUpdate`) |
| after outbox insert | transaction not committed | SQLite rollback; no event without state | same test (`AfterOutboxInsert`) |
| after DB commit | complete control-plane state | retain artifact, metadata, aggregate, and outbox; retry observes committed state | same test (`AfterDbCommit`) |

The test hook is compiled only for store tests. It returns an interruption
error at the selected boundary; the resulting filesystem/SQLite state is then
passed through the same artifact reconciliation used by startup recovery.

Additional invariants are covered by:

- `atomic_stage_commit_persists_artifact_job_stage_and_outbox_together`;
- `atomic_work_unit_commit_persists_done_status_and_artifact_reference`;
- `stale_stage_commit_rolls_back_artifact_metadata_and_file`;
- `stale_work_unit_stage_commit_rolls_back_artifact_metadata`;
- `startup_recovery_quarantines_orphans_and_invalidates_corrupt_stage`;
- the core `StartupRecovery` test for expired leases and Running aggregate
  recovery.
