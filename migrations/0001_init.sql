-- Mirror of crates/store migration v1 (checksummed in-code).
-- Hand edits here do not change runtime; update migrate.rs.

CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  checksum TEXT NOT NULL,
  applied_at TEXT NOT NULL
);

-- See crates/store/src/migrate.rs for the full applied SQL body.
