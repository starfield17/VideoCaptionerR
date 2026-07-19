//! Single-writer store actor over SQLite.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection, OptionalExtension};
use ulid::Ulid;
use videocaptionerr_contracts::artifact::{ArtifactKind, ArtifactMeta};
use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};
use videocaptionerr_contracts::ids::UlidStr;

use crate::migrate::migrate;

/// Work unit lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkUnitStatus {
    Pending,
    Running,
    Done,
    Failed,
    Cancelled,
}

impl WorkUnitStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "pending" => Self::Pending,
            "running" => Self::Running,
            "done" => Self::Done,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => return None,
        })
    }
}

/// Blocking store handle. All writes go through this mutex (single writer).
#[derive(Clone)]
pub struct StoreHandle {
    inner: Arc<Mutex<Store>>,
}

impl StoreHandle {
    pub fn open(db_path: &Path) -> VcResult<Self> {
        Ok(Self {
            inner: Arc::new(Mutex::new(Store::open(db_path)?)),
        })
    }

    pub fn with<F, T>(&self, f: F) -> VcResult<T>
    where
        F: FnOnce(&mut Store) -> VcResult<T>,
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| VcError::new(ErrorCode::Internal, "store mutex poisoned"))?;
        f(&mut guard)
    }
}

/// SQLite-backed control plane.
pub struct Store {
    conn: Connection,
    path: PathBuf,
}

impl Store {
    pub fn open(db_path: &Path) -> VcResult<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                VcError::new(ErrorCode::InvalidConfig, format!("create db parent: {e}"))
            })?;
        }
        let conn = Connection::open(db_path).map_err(|e| {
            VcError::new(
                ErrorCode::InvalidConfig,
                format!("open db {}: {e}", db_path.display()),
            )
        })?;
        migrate(&conn)?;
        Ok(Self {
            conn,
            path: db_path.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    pub fn insert_job(
        &self,
        id: &str,
        batch_id: Option<&str>,
        source_path: &str,
        job_dir: &str,
        status: &str,
    ) -> VcResult<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn
            .execute(
                "INSERT INTO jobs (id, batch_id, status, source_path, job_dir, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
                params![id, batch_id, status, source_path, job_dir, now],
            )
            .map_err(|e| {
                VcError::new(ErrorCode::Internal, format!("insert job: {e}"))
            })?;
        Ok(())
    }

    pub fn get_job_status(&self, id: &str) -> VcResult<Option<String>> {
        self.conn
            .query_row("SELECT status FROM jobs WHERE id = ?1", [id], |r| r.get(0))
            .optional()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("get job: {e}")))
    }

    pub fn mark_job_done(
        &self,
        id: &str,
        source_hash: &str,
        pcm_hash: &str,
        selected_stream_index: i64,
        language: Option<&str>,
    ) -> VcResult<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn
            .execute(
                "UPDATE jobs SET status='done', source_hash=?1, pcm_hash=?2,
                 selected_stream_index=?3, language=?4, updated_at=?5, finished_at=?5
                 WHERE id=?6",
                params![
                    source_hash,
                    pcm_hash,
                    selected_stream_index,
                    language,
                    now,
                    id
                ],
            )
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("mark job done: {e}")))?;
        Ok(())
    }

    /// Insert artifact metadata and mark committed in one transaction with a work unit update.
    pub fn commit_artifact_and_unit(
        &mut self,
        meta: &ArtifactMeta,
        work_unit_id: Option<&str>,
    ) -> VcResult<()> {
        let tx = self.conn.unchecked_transaction().map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("begin commit tx: {e}"),
            )
        })?;

        tx.execute(
            "INSERT INTO artifacts (
                id, job_id, stage, kind, path, content_hash, schema_version,
                producer_fingerprint, created_at, committed
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1)",
            params![
                meta.id,
                meta.job_id,
                meta.stage,
                meta.kind.as_str(),
                meta.path,
                meta.content_hash,
                meta.schema_version as i64,
                meta.producer_fingerprint,
                meta.created_at,
            ],
        )
        .map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("insert artifact: {e}"),
            )
        })?;

        if let Some(unit_id) = work_unit_id {
            let now = chrono::Utc::now().to_rfc3339();
            tx.execute(
                "UPDATE work_units SET status = ?1, artifact_id = ?2, finished_at = ?3,
                 lease_owner = NULL, lease_expires_at = NULL
                 WHERE id = ?4",
                params![WorkUnitStatus::Done.as_str(), meta.id, now, unit_id],
            )
            .map_err(|e| {
                VcError::new(
                    ErrorCode::ArtifactCommitFailed,
                    format!("update work unit: {e}"),
                )
            })?;
        }

        tx.commit().map_err(|e| {
            VcError::new(
                ErrorCode::ArtifactCommitFailed,
                format!("commit artifact tx: {e}"),
            )
        })?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_work_unit(
        &self,
        id: &str,
        job_id: &str,
        stage: &str,
        unit_kind: &str,
        unit_index: i64,
        input_hash: &str,
        status: WorkUnitStatus,
    ) -> VcResult<()> {
        self.conn
            .execute(
                "INSERT INTO work_units (
                    id, job_id, stage, unit_kind, unit_index, input_hash, status, attempt
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0)",
                params![
                    id,
                    job_id,
                    stage,
                    unit_kind,
                    unit_index,
                    input_hash,
                    status.as_str()
                ],
            )
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("insert work unit: {e}")))?;
        Ok(())
    }

    pub fn get_work_unit_status(&self, id: &str) -> VcResult<Option<WorkUnitStatus>> {
        let s: Option<String> = self
            .conn
            .query_row("SELECT status FROM work_units WHERE id = ?1", [id], |r| {
                r.get(0)
            })
            .optional()
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("get work unit: {e}")))?;
        Ok(s.and_then(|x| WorkUnitStatus::parse(&x)))
    }

    /// Expire running leases: return to Pending and increment attempt.
    pub fn recover_expired_leases(&self, now_rfc3339: &str) -> VcResult<usize> {
        let n = self
            .conn
            .execute(
                "UPDATE work_units
                 SET status = 'pending',
                     attempt = attempt + 1,
                     lease_owner = NULL,
                     lease_expires_at = NULL,
                     started_at = NULL
                 WHERE status = 'running'
                   AND lease_expires_at IS NOT NULL
                   AND lease_expires_at < ?1",
                [now_rfc3339],
            )
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("recover leases: {e}")))?;
        Ok(n)
    }

    pub fn append_job_event(
        &self,
        job_id: &str,
        event_type: &str,
        payload_json: Option<&str>,
    ) -> VcResult<String> {
        let id = UlidStr::from(Ulid::new()).into_string();
        let next_seq: i64 = self
            .conn
            .query_row(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM job_events WHERE job_id = ?1",
                [job_id],
                |r| r.get(0),
            )
            .unwrap_or(1);
        let now = chrono::Utc::now().to_rfc3339();
        self.conn
            .execute(
                "INSERT INTO job_events (id, job_id, seq, event_type, payload_json, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![id, job_id, next_seq, event_type, payload_json, now],
            )
            .map_err(|e| VcError::new(ErrorCode::Internal, format!("append event: {e}")))?;
        Ok(id)
    }

    pub fn new_artifact_meta(
        job_id: &str,
        stage: &str,
        kind: ArtifactKind,
        path: &str,
        content_hash: &str,
        producer_fingerprint: &str,
    ) -> ArtifactMeta {
        ArtifactMeta::new(
            UlidStr::from(Ulid::new()).into_string(),
            job_id,
            stage,
            kind,
            path,
            content_hash,
            producer_fingerprint,
            chrono::Utc::now().to_rfc3339(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::atomic_write_bytes;
    use tempfile::tempdir;

    #[test]
    fn job_and_artifact_commit() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("t.db");
        let mut store = Store::open(&db).unwrap();

        store
            .insert_job("job1", None, "/media/a.mp4", "/jobs/job1", "pending")
            .unwrap();
        assert_eq!(
            store.get_job_status("job1").unwrap().as_deref(),
            Some("pending")
        );

        store
            .insert_work_unit(
                "wu1",
                "job1",
                "asr",
                "chunk",
                0,
                "hash0",
                WorkUnitStatus::Running,
            )
            .unwrap();

        let art_path = dir.path().join("transcript.json");
        let hash = atomic_write_bytes(&art_path, br#"{"ok":1}"#).unwrap();
        let meta = Store::new_artifact_meta(
            "job1",
            "asr",
            ArtifactKind::Transcript,
            art_path.to_str().unwrap(),
            &hash,
            "test@0.1.0",
        );
        store.commit_artifact_and_unit(&meta, Some("wu1")).unwrap();

        assert_eq!(
            store.get_work_unit_status("wu1").unwrap(),
            Some(WorkUnitStatus::Done)
        );
    }

    #[test]
    fn lease_recovery() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("t.db");
        let store = Store::open(&db).unwrap();
        store
            .insert_job("job1", None, "/a", "/j", "running")
            .unwrap();
        store
            .insert_work_unit(
                "wu1",
                "job1",
                "asr",
                "chunk",
                0,
                "h",
                WorkUnitStatus::Pending,
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE work_units SET status='running', lease_owner='cli',
                 lease_expires_at='2020-01-01T00:00:00Z', attempt=1 WHERE id='wu1'",
                [],
            )
            .unwrap();

        let n = store
            .recover_expired_leases("2026-01-01T00:00:00Z")
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(
            store.get_work_unit_status("wu1").unwrap(),
            Some(WorkUnitStatus::Pending)
        );
        let attempt: i64 = store
            .conn
            .query_row("SELECT attempt FROM work_units WHERE id='wu1'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(attempt, 2);
    }
}
