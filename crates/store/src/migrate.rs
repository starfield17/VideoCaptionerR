//! Numbered, checksummed SQLite migrations.

use rusqlite::{Connection, OptionalExtension};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};

/// A single migration: version, SQL body, and content checksum.
pub struct Migration {
    pub version: i64,
    pub name: &'static str,
    pub sql: &'static str,
}

impl Migration {
    pub fn checksum(&self) -> String {
        blake3::hash(self.sql.as_bytes()).to_hex().to_string()
    }
}

/// Ordered migrations. Append-only; never edit applied SQL in place.
pub static MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "init",
        sql: r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  checksum TEXT NOT NULL,
  applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
  id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  asr_model_id TEXT,
  asr_device TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  finished_at TEXT,
  error_code TEXT,
  error_json TEXT
);

CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  batch_id TEXT REFERENCES batches(id),
  status TEXT NOT NULL,
  source_path TEXT NOT NULL,
  source_hash TEXT,
  pcm_hash TEXT,
  job_dir TEXT NOT NULL,
  profile_id TEXT,
  profile_revision INTEGER,
  selected_stream_index INTEGER,
  language TEXT,
  target_lang TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  finished_at TEXT,
  error_code TEXT,
  error_json TEXT
);

CREATE TABLE IF NOT EXISTS stages (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  stage TEXT NOT NULL,
  status TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 0,
  started_at TEXT,
  finished_at TEXT,
  error_code TEXT,
  error_json TEXT,
  UNIQUE(job_id, stage)
);

CREATE TABLE IF NOT EXISTS artifacts (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  stage TEXT NOT NULL,
  kind TEXT NOT NULL,
  path TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  producer_fingerprint TEXT NOT NULL,
  created_at TEXT NOT NULL,
  committed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS work_units (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  stage TEXT NOT NULL,
  unit_kind TEXT NOT NULL,
  unit_index INTEGER NOT NULL,
  input_hash TEXT NOT NULL,
  status TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 0,
  artifact_id TEXT REFERENCES artifacts(id),
  error_code TEXT,
  error_json TEXT,
  lease_owner TEXT,
  lease_expires_at TEXT,
  started_at TEXT,
  finished_at TEXT,
  UNIQUE(job_id, stage, unit_kind, unit_index, input_hash)
);

CREATE TABLE IF NOT EXISTS job_events (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  seq INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(job_id, seq)
);

CREATE TABLE IF NOT EXISTS profile_revisions (
  id TEXT PRIMARY KEY,
  profile_id TEXT NOT NULL,
  revision INTEGER NOT NULL,
  body_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(profile_id, revision)
);

CREATE TABLE IF NOT EXISTS llm_requests (
  id TEXT PRIMARY KEY,
  job_id TEXT,
  stage TEXT,
  provider_profile_id TEXT,
  model TEXT,
  request_hash TEXT,
  status TEXT NOT NULL,
  error_code TEXT,
  created_at TEXT NOT NULL,
  finished_at TEXT,
  metadata_json TEXT
);

CREATE TABLE IF NOT EXISTS llm_capability_probes (
  id TEXT PRIMARY KEY,
  provider_profile_id TEXT NOT NULL,
  model TEXT NOT NULL,
  probe_hash TEXT NOT NULL,
  result_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  expires_at TEXT,
  UNIQUE(provider_profile_id, model, probe_hash)
);

CREATE TABLE IF NOT EXISTS transcript_revisions (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  revision INTEGER NOT NULL,
  artifact_id TEXT REFERENCES artifacts(id),
  is_latest INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  UNIQUE(job_id, revision)
);

CREATE INDEX IF NOT EXISTS idx_jobs_batch ON jobs(batch_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_work_units_job ON work_units(job_id, status);
CREATE INDEX IF NOT EXISTS idx_artifacts_job ON artifacts(job_id);
    "#,
    },
    Migration {
        version: 2,
        name: "job_aggregate_json",
        sql: r#"
ALTER TABLE jobs ADD COLUMN aggregate_json TEXT;
"#,
    },
    Migration {
        version: 3,
        name: "batch_aggregate_json",
        sql: r#"
ALTER TABLE batches ADD COLUMN aggregate_json TEXT;
"#,
    },
    Migration {
        version: 4,
        name: "work_unit_aggregate_json",
        sql: r#"
ALTER TABLE work_units ADD COLUMN aggregate_json TEXT;
"#,
    },
    Migration {
        version: 5,
        name: "execution_snapshots_and_aggregate_versions",
        sql: r#"
ALTER TABLE jobs ADD COLUMN aggregate_version INTEGER NOT NULL DEFAULT 0;
ALTER TABLE batches ADD COLUMN aggregate_version INTEGER NOT NULL DEFAULT 0;
ALTER TABLE work_units ADD COLUMN aggregate_version INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN execution_snapshot_id TEXT;

CREATE TABLE IF NOT EXISTS execution_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  schema_version INTEGER NOT NULL,
  job_id TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  canonical_source_path TEXT NOT NULL,
  source_size INTEGER NOT NULL,
  source_modified_at_ms INTEGER,
  job_dir TEXT NOT NULL,
  profile_revision TEXT NOT NULL,
  asr_engine TEXT NOT NULL,
  model_locator TEXT NOT NULL,
  model_id TEXT,
  model_digest TEXT,
  device TEXT NOT NULL,
  compute_type TEXT NOT NULL,
  audio_stream_selection TEXT NOT NULL,
  source_language TEXT,
  target_language TEXT,
  output_path TEXT NOT NULL,
  output_format TEXT NOT NULL,
  output_layout TEXT NOT NULL,
  conflict_policy TEXT NOT NULL,
  fallback_to_source INTEGER NOT NULL,
  llm_json TEXT,
  snapshot_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_execution_snapshot
  ON jobs(execution_snapshot_id);
CREATE INDEX IF NOT EXISTS idx_execution_snapshots_job
  ON execution_snapshots(job_id);
CREATE INDEX IF NOT EXISTS idx_execution_snapshots_batch
  ON execution_snapshots(batch_id);
"#,
    },
];

/// Apply pending migrations. Verifies checksums of already-applied versions.
pub fn migrate(conn: &Connection) -> VcResult<i64> {
    conn.execute_batch(
        "PRAGMA foreign_keys = ON;
         PRAGMA journal_mode = WAL;
         PRAGMA busy_timeout = 5000;",
    )
    .map_err(|e| {
        VcError::new(
            ErrorCode::ConfigMigrationFailed,
            format!("pragma setup: {e}"),
        )
    })?;

    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            checksum TEXT NOT NULL,
            applied_at TEXT NOT NULL
        );",
    )
    .map_err(|e| {
        VcError::new(
            ErrorCode::ConfigMigrationFailed,
            format!("create schema_migrations: {e}"),
        )
    })?;

    let mut current: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);

    // Verify applied migrations still match.
    for m in MIGRATIONS.iter().filter(|m| m.version <= current) {
        let stored: Option<String> = conn
            .query_row(
                "SELECT checksum FROM schema_migrations WHERE version = ?1",
                [m.version],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| {
                VcError::new(
                    ErrorCode::ConfigMigrationFailed,
                    format!("read migration checksum: {e}"),
                )
            })?;
        if let Some(stored) = stored {
            let expected = m.checksum();
            if stored != expected {
                return Err(VcError::new(
                    ErrorCode::ConfigMigrationFailed,
                    format!(
                        "migration {} checksum mismatch (db changed under us)",
                        m.version
                    ),
                ));
            }
        }
    }

    let pending: Vec<&Migration> = MIGRATIONS.iter().filter(|m| m.version > current).collect();
    for m in pending {
        let tx = conn.unchecked_transaction().map_err(|e| {
            VcError::new(
                ErrorCode::ConfigMigrationFailed,
                format!("begin migration tx: {e}"),
            )
        })?;
        tx.execute_batch(m.sql).map_err(|e| {
            VcError::new(
                ErrorCode::ConfigMigrationFailed,
                format!("apply migration {} ({}): {e}", m.version, m.name),
            )
        })?;
        let now = chrono::Utc::now().to_rfc3339();
        tx.execute(
            "INSERT INTO schema_migrations (version, name, checksum, applied_at)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![m.version, m.name, m.checksum(), now],
        )
        .map_err(|e| {
            VcError::new(
                ErrorCode::ConfigMigrationFailed,
                format!("record migration {}: {e}", m.version),
            )
        })?;
        tx.commit().map_err(|e| {
            VcError::new(
                ErrorCode::ConfigMigrationFailed,
                format!("commit migration {}: {e}", m.version),
            )
        })?;
        current = m.version;
    }

    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn migration_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let conn = Connection::open(&path).unwrap();
        let v = migrate(&conn).unwrap();
        assert_eq!(v, MIGRATIONS.last().unwrap().version);

        // Re-open and migrate again is idempotent.
        let conn2 = Connection::open(&path).unwrap();
        let v2 = migrate(&conn2).unwrap();
        assert_eq!(v2, MIGRATIONS.last().unwrap().version);

        // Tables exist.
        let n: i64 = conn2
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='jobs'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn checksum_stable() {
        let c1 = MIGRATIONS[0].checksum();
        let c2 = MIGRATIONS[0].checksum();
        assert_eq!(c1, c2);
        assert_eq!(c1.len(), 64);
    }
}
