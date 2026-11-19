//! Bounded child-set readback for retained command expansions.
//!
//! Owning contract: `command.children` in docs/spec/api.md. Retention prunes
//! whole parent/child sets, children before parent (docs/spec/core.md#retention),
//! so a retained parent always pages its complete fixed set in ordinal order.
//! The caller loads the parent and authorizes it (same as `command.get`)
//! before paging, so cursor membership is never checked for an unauthorized
//! caller. An absent parent id whose ULID time is older than the retry window
//! plus the allowed clock skew may have been pruned (`history_expired`);
//! any other absent id never existed.

use super::*;

const LIST_COMMAND_CHILDREN_FIRST_SQL: &str =
    "SELECT id, principal, method, target, expected_generation,
            generation, expected_turn_id, turn_id, request_id, verb, args,
            state, outcome, created_at, updated_at, probe, selector, config_revision
     FROM commands WHERE parent_command = ?1 ORDER BY ordinal LIMIT ?2";
const LIST_COMMAND_CHILDREN_CURSOR_SQL: &str =
    "SELECT id, principal, method, target, expected_generation,
            generation, expected_turn_id, turn_id, request_id, verb, args,
            state, outcome, created_at, updated_at, probe, selector, config_revision
     FROM commands WHERE parent_command = ?1 AND ordinal > ?2
     ORDER BY ordinal LIMIT ?3";

/// One bounded page of a retained expansion: the parent for authorization,
/// its direct children in fixed ordinal order, and the continuation cursor.
#[derive(Clone, Debug, PartialEq)]
pub struct CommandChildrenPage {
    pub parent: Command,
    pub children: Vec<Command>,
    pub next: Option<String>,
}

impl Store {
    /// Page the fixed child set of a retained expansion in ordinal order.
    /// `after` names the last child of the previous page; `next` echoes the
    /// last returned child id, or is `None` on an empty page.
    ///
    /// The caller passes the loaded parent row after authorizing it (same as
    /// `command.get`); the parent is re-checked here so a set that vanished
    /// since reads `not_found` rather than an empty page.
    pub fn list_command_children(
        &self,
        parent: &Command,
        after: Option<&str>,
        limit: usize,
    ) -> Result<CommandChildrenPage> {
        validate_limit(limit)?;
        if after.is_some_and(|cursor| cursor.trim().is_empty()) {
            return Err(Error::new("invalid", "child cursor must be a non-empty id"));
        }
        if load_command_record(&self.connection, &parent.id)?.is_none() {
            return Err(Error::new(
                "not_found",
                format!("command {} was not found", parent.id),
            ));
        }
        let mut children = Vec::new();
        match after {
            None => {
                let mut statement = self
                    .connection
                    .prepare(LIST_COMMAND_CHILDREN_FIRST_SQL)
                    .map_err(sql_error)?;
                let rows = statement
                    .query_map(params![parent.id, limit as i64], command_record_from_row)
                    .map_err(sql_error)?;
                for row in rows {
                    children.push(command_from_raw(row.map_err(sql_error)?)?.command);
                }
            }
            Some(cursor) => {
                let floor = child_ordinal(&self.connection, &parent.id, cursor)?;
                let mut statement = self
                    .connection
                    .prepare(LIST_COMMAND_CHILDREN_CURSOR_SQL)
                    .map_err(sql_error)?;
                let rows = statement
                    .query_map(
                        params![parent.id, floor, limit as i64],
                        command_record_from_row,
                    )
                    .map_err(sql_error)?;
                for row in rows {
                    children.push(command_from_raw(row.map_err(sql_error)?)?.command);
                }
            }
        }
        enforce_page_bytes(&mut children)?;
        let next = children.last().map(|child| child.id.clone());
        Ok(CommandChildrenPage {
            parent: parent.clone(),
            children,
            next,
        })
    }
}

/// Ordinal of one member of the requested set. A cursor naming no child of
/// this parent is `invalid`; it reveals nothing about other sets.
fn child_ordinal(connection: &Connection, parent_id: &str, cursor: &str) -> Result<i64> {
    connection
        .query_row(
            "SELECT ordinal FROM commands WHERE id = ?1 AND parent_command = ?2",
            params![cursor, parent_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| {
            Error::new(
                "invalid",
                "child cursor does not match the requested parent",
            )
        })
}

/// Whether an absent id may have been pruned: exactly the admission
/// `id_expired` age rule, plus the allowed generator-clock skew. Retention
/// keeps every younger command and mail row, so only an absent ULID older
/// than the retry window plus that skew may be gone; any other absent id
/// never existed. Mail passes the delivery's write-id head. Pure: no table
/// scan, so no unindexed full-table read.
pub fn history_expired(id: &str, now: i64) -> bool {
    ulid_timestamp_ms(id).is_some_and(|created_at| {
        now.saturating_sub(created_at) > RETRY_WINDOW_MS + RETRY_CLOCK_SKEW_MS
    })
}
