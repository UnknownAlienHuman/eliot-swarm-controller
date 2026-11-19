use super::*;
use eliot_core::api::{SettingsHistoryEntry, SettingsHistoryStatus, SettingsSection};

pub const SETTINGS_HISTORY_LIMIT: usize = 100;

impl Store {
    /// Persist metadata before publishing a settings file revision. The pending
    /// row makes a crash between filesystem and SQLite effects observable.
    pub fn prepare_settings_history(
        &mut self,
        previous_revision: Option<&str>,
        current_revision: &str,
        changed_sections: &[SettingsSection],
        author: &str,
        created_at: i64,
    ) -> Result<i64> {
        self.ensure_writer_healthy()?;
        if !valid_revision(current_revision)
            || previous_revision.is_some_and(|revision| !valid_revision(revision))
            || changed_sections.is_empty()
            || changed_sections.len() > 16
            || author.is_empty()
            || author.len() > 256
            || created_at < 0
        {
            return Err(Error::new(
                "invalid",
                "settings history metadata is invalid",
            ));
        }
        let changed_sections_json = serde_json::to_string(changed_sections)
            .map_err(|_| Error::new("invalid", "settings history sections are invalid"))?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        tx.execute(
            "DELETE FROM settings_history
             WHERE status != 'pending' AND revision NOT IN (
                 SELECT revision FROM settings_history WHERE status != 'pending'
                 ORDER BY revision DESC LIMIT ?1
             )",
            [(SETTINGS_HISTORY_LIMIT - 1) as i64],
        )
        .map_err(sql_error)?;
        let total: i64 = tx
            .query_row("SELECT COUNT(*) FROM settings_history", [], |row| {
                row.get(0)
            })
            .map_err(sql_error)?;
        if total >= SETTINGS_HISTORY_LIMIT as i64 {
            return Err(Error::new(
                "unavailable",
                "settings history has unresolved pending entries; reconcile before saving",
            ));
        }
        tx.execute(
            "INSERT INTO settings_history(
                 previous_revision, current_revision, changed_sections, author, status, created_at
             ) VALUES (?1, ?2, ?3, ?4, 'pending', ?5)",
            params![
                previous_revision,
                current_revision,
                changed_sections_json,
                author,
                created_at
            ],
        )
        .map_err(sql_error)?;
        let revision = tx.last_insert_rowid();
        commit_transaction(tx, &self.write_faulted)?;
        Ok(revision)
    }

    /// Settle one pending file publication. Repeated exact settlement is safe.
    pub fn settle_settings_history(
        &mut self,
        revision: i64,
        status: SettingsHistoryStatus,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let value = history_status(status);
        if status == SettingsHistoryStatus::Pending || revision <= 0 {
            return Err(Error::new(
                "invalid",
                "settings history settlement is invalid",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let changed = tx
            .execute(
                "UPDATE settings_history SET status = ?1 WHERE revision = ?2 AND status = 'pending'",
                params![value, revision],
            )
            .map_err(sql_error)?;
        if changed == 0 {
            let existing: Option<String> = tx
                .query_row(
                    "SELECT status FROM settings_history WHERE revision = ?1",
                    [revision],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?;
            if existing.as_deref() != Some(value) {
                return Err(Error::new(
                    "source_integrity",
                    "settings history row is not pending",
                ));
            }
        }
        commit_transaction(tx, &self.write_faulted)
    }

    /// Reconcile crash-left pending rows against the exact current config hash.
    pub fn reconcile_settings_history(&mut self, current_revision: Option<&str>) -> Result<()> {
        self.ensure_writer_healthy()?;
        if current_revision.is_some_and(|revision| !valid_revision(revision)) {
            return Err(Error::new("invalid", "settings revision is invalid"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let mut statement = tx
            .prepare(
                "SELECT revision, previous_revision, current_revision
                 FROM settings_history WHERE status = 'pending' ORDER BY revision",
            )
            .map_err(sql_error)?;
        let pending = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(sql_error)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        drop(statement);
        for (id, previous, next) in pending {
            let status = if current_revision == Some(next.as_str()) {
                "applied"
            } else if current_revision == previous.as_deref() {
                "not_applied"
            } else {
                "unknown"
            };
            tx.execute(
                "UPDATE settings_history SET status = ?1 WHERE revision = ?2 AND status = 'pending'",
                params![status, id],
            )
            .map_err(sql_error)?;
        }
        commit_transaction(tx, &self.write_faulted)
    }

    /// Return the newest retained settings changes, newest first.
    pub fn list_settings_history(&self, limit: usize) -> Result<Vec<SettingsHistoryEntry>> {
        if limit == 0 || limit > SETTINGS_HISTORY_LIMIT {
            return Err(Error::new(
                "invalid",
                "settings history limit is out of range",
            ));
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT revision, previous_revision, current_revision,
                        changed_sections, author, created_at, status
                 FROM settings_history ORDER BY revision DESC LIMIT ?1",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([limit as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, String>(6)?,
                ))
            })
            .map_err(sql_error)?;
        rows.map(|row| {
            let (
                revision,
                previous_revision,
                current_revision,
                sections,
                author,
                created_at,
                status,
            ) = row.map_err(sql_error)?;
            let changed_sections = serde_json::from_str(&sections).map_err(|_| {
                Error::new(
                    "source_integrity",
                    "stored settings history sections are invalid",
                )
            })?;
            Ok(SettingsHistoryEntry {
                revision,
                previous_revision,
                current_revision,
                changed_sections,
                author,
                created_at,
                status: parse_history_status(&status)?,
            })
        })
        .collect()
    }
}

fn valid_revision(revision: &str) -> bool {
    revision.len() == 64 && revision.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn history_status(status: SettingsHistoryStatus) -> &'static str {
    match status {
        SettingsHistoryStatus::Pending => "pending",
        SettingsHistoryStatus::Applied => "applied",
        SettingsHistoryStatus::NotApplied => "not_applied",
        SettingsHistoryStatus::Unknown => "unknown",
    }
}

fn parse_history_status(status: &str) -> Result<SettingsHistoryStatus> {
    match status {
        "pending" => Ok(SettingsHistoryStatus::Pending),
        "applied" => Ok(SettingsHistoryStatus::Applied),
        "not_applied" => Ok(SettingsHistoryStatus::NotApplied),
        "unknown" => Ok(SettingsHistoryStatus::Unknown),
        _ => Err(Error::new(
            "source_integrity",
            "stored settings history status is invalid",
        )),
    }
}
