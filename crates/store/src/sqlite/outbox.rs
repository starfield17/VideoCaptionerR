use super::*;

impl SqliteStore {
    pub(crate) fn list_pending_outbox(
        &self,
        limit: u32,
    ) -> VcResult<Vec<videocaptionerr_core::ports::StoredOutboxEvent>> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT id, aggregate_type, aggregate_id, sequence, event_type,
                        payload_json, created_at, delivered_at
                 FROM outbox_events WHERE delivered_at IS NULL
                 ORDER BY created_at, id LIMIT ?1",
            )
            .map_err(|error| {
                VcError::new(ErrorCode::Internal, format!("prepare outbox: {error}"))
            })?;
        let rows = statement
            .query_map([limit as i64], |row| {
                let id: String = row.get(0)?;
                Ok(videocaptionerr_core::ports::StoredOutboxEvent {
                    id: id.parse().map_err(|_| {
                        rusqlite::Error::InvalidColumnType(
                            0,
                            "id".into(),
                            rusqlite::types::Type::Text,
                        )
                    })?,
                    aggregate_type: row.get(1)?,
                    aggregate_id: row.get(2)?,
                    sequence: row.get::<_, i64>(3)? as u64,
                    event_type: row.get(4)?,
                    payload_json: row.get(5)?,
                    created_at: row.get(6)?,
                    delivered_at: row.get(7)?,
                })
            })
            .map_err(|error| VcError::new(ErrorCode::Internal, format!("query outbox: {error}")))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| VcError::new(ErrorCode::Internal, format!("read outbox: {error}")))
    }

    pub(crate) fn mark_outbox_delivered(&self, id: &str, delivered_at: &str) -> VcResult<()> {
        self.conn
            .execute(
                "UPDATE outbox_events SET delivered_at = ?1 WHERE id = ?2",
                params![delivered_at, id],
            )
            .map_err(|error| {
                VcError::new(
                    ErrorCode::Internal,
                    format!("mark outbox delivered: {error}"),
                )
            })?;
        Ok(())
    }

    pub(crate) fn append_outbox(
        &mut self,
        event: &videocaptionerr_core::ports::OutboxEvent,
    ) -> VcResult<()> {
        let tx = self.conn.unchecked_transaction().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("begin outbox transaction: {error}"),
            )
        })?;
        insert_outbox_tx(&tx, event)?;
        tx.commit().map_err(|error| {
            VcError::new(
                ErrorCode::Internal,
                format!("commit outbox transaction: {error}"),
            )
        })
    }
}
