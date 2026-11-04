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
            state, outcome, created_at, updated_at, probe
     FROM commands WHERE parent_command = ?1 ORDER BY ordinal LIMIT ?2";
const LIST_COMMAND_CHILDREN_CURSOR_SQL: &str =
    "SELECT id, principal, method, target, expected_generation,
            generation, expected_turn_id, turn_id, request_id, verb, args,
            state, outcome, created_at, updated_at, probe
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    const DAY_MS: i64 = 24 * 60 * 60 * 1_000;
    const NOW: i64 = 1_800_000_000_000;

    fn store() -> Store {
        Store::open(Path::new(":memory:")).unwrap()
    }

    fn ulid_at(ts_ms: i64, tag: u8) -> String {
        let mut bytes = [0_u8; 16];
        bytes[..6].copy_from_slice(&(ts_ms as u64).to_be_bytes()[2..8]);
        bytes[6..].copy_from_slice(&[tag; 10]);
        Ulid::from_bytes(bytes).to_string()
    }

    fn insert_command(
        store: &mut Store,
        id: &str,
        parent: Option<(&str, &str, i64)>,
        created_at: i64,
    ) {
        let digest = vec![0_u8; 32];
        let (parent, link_kind, ordinal) = match parent {
            Some((id, kind, ordinal)) => (Some(id), Some(kind), Some(ordinal)),
            None => (None, None, None),
        };
        store
            .connection
            .execute(
                "INSERT INTO commands(id, principal, method, payload_digest, parent_command,
                                      link_kind, ordinal, verb, args, state, created_at, updated_at)
                 VALUES (?1, 'owner', 'command.submit', ?2, ?3, ?4, ?5, 'send', '{}',
                         'applied', ?6, ?6)",
                params![id, digest, parent, link_kind, ordinal, created_at],
            )
            .unwrap();
    }

    fn seed_expansion(store: &mut Store, parent: &str, members: i64) -> Vec<String> {
        insert_command(store, parent, None, NOW);
        let mut children = Vec::new();
        for ordinal in 0..members {
            let child = format!("{parent}#{ordinal}");
            insert_command(store, &child, Some((parent, "rule_action", ordinal)), NOW);
            children.push(child);
        }
        children
    }

    fn retained(store: &Store, id: &str) -> Command {
        store.get_command(id).unwrap().unwrap()
    }

    #[test]
    fn pages_direct_children_in_ordinal_order_with_last_id_cursor() {
        let mut store = store();
        let children = seed_expansion(&mut store, "parent-1", 3);
        insert_command(
            &mut store,
            "grandchild",
            Some((children[0].as_str(), "send", 0)),
            NOW,
        );
        let parent = retained(&store, "parent-1");
        let first = store.list_command_children(&parent, None, 2).unwrap();
        assert_eq!(first.parent.id, "parent-1");
        assert_eq!(
            first
                .children
                .iter()
                .map(|child| &child.id)
                .collect::<Vec<_>>(),
            [&children[0], &children[1]]
        );
        assert_eq!(first.next.as_deref(), Some(children[1].as_str()));
        let second = store
            .list_command_children(&parent, Some(children[1].as_str()), 2)
            .unwrap();
        assert_eq!(second.children.len(), 1);
        assert_eq!(second.children[0].id, children[2]);
        assert_eq!(second.next.as_deref(), Some(children[2].as_str()));
        let tail = store
            .list_command_children(&parent, Some(children[2].as_str()), 2)
            .unwrap();
        assert!(tail.children.is_empty());
        assert_eq!(tail.next, None);
    }

    #[test]
    fn childless_parent_pages_empty_and_rejects_any_cursor() {
        let mut store = store();
        insert_command(&mut store, "parent-1", None, NOW);
        let parent = retained(&store, "parent-1");
        let page = store.list_command_children(&parent, None, 100).unwrap();
        assert!(page.children.is_empty());
        assert_eq!(page.next, None);
        let error = store
            .list_command_children(&parent, Some("parent-1"), 100)
            .unwrap_err();
        assert_eq!(error.code, "invalid");
    }

    #[test]
    fn cursors_outside_the_set_and_bad_bounds_are_invalid() {
        let mut store = store();
        let children = seed_expansion(&mut store, "parent-1", 1);
        seed_expansion(&mut store, "parent-2", 1);
        let first = retained(&store, "parent-1");
        let second = retained(&store, "parent-2");
        for after in [
            Some(children[0].as_str()),
            Some("missing"),
            Some(""),
            Some(" \t "),
        ] {
            let error = store
                .list_command_children(&second, after, 100)
                .unwrap_err();
            assert_eq!(error.code, "invalid", "cursor {after:?}");
        }
        let error = store.list_command_children(&first, None, 0).unwrap_err();
        assert_eq!(error.code, "invalid");
    }

    #[test]
    fn paging_a_vanished_parent_reads_not_found() {
        let mut store = store();
        seed_expansion(&mut store, "parent-1", 0);
        let parent = retained(&store, "parent-1");
        store
            .connection
            .execute("DELETE FROM commands WHERE id = 'parent-1'", ())
            .unwrap();
        let error = store.list_command_children(&parent, None, 100).unwrap_err();
        assert_eq!(error.code, "not_found");
    }

    #[test]
    fn pruned_expansion_id_classifies_as_history_gap() {
        let mut store = store();
        let old = NOW - 31 * DAY_MS;
        let parent = ulid_at(old, 1);
        let child = ulid_at(old, 2);
        insert_command(&mut store, &parent, None, old);
        insert_command(&mut store, &child, Some((&parent, "rule_action", 0)), old);
        let report = store.prune_retention(None, Some(30), None, NOW).unwrap();
        assert_eq!(report.commands_deleted, 2);
        assert!(store.get_command(&parent).unwrap().is_none());
        assert!(history_expired(&parent, NOW));
    }

    #[test]
    fn history_gap_uses_the_retry_window_plus_clock_skew() {
        // Only an absent ULID strictly older than the retry window plus the
        // allowed generator-clock skew may have been pruned.
        let expired = NOW - RETRY_WINDOW_MS - RETRY_CLOCK_SKEW_MS - 1;
        assert!(history_expired(&ulid_at(expired, 3), NOW));
        for (label, id) in [
            (
                "boundary",
                ulid_at(NOW - RETRY_WINDOW_MS - RETRY_CLOCK_SKEW_MS, 4),
            ),
            ("young", ulid_at(NOW, 5)),
            ("skew_ahead", ulid_at(NOW + RETRY_CLOCK_SKEW_MS, 6)),
            ("not_a_ulid", "missing".to_owned()),
        ] {
            assert!(
                !history_expired(&id, NOW),
                "{label} must never have existed"
            );
        }
    }

    fn plan_detail(row: &rusqlite::Row<'_>) -> rusqlite::Result<String> {
        row.get(3)
    }

    #[test]
    fn child_pages_seek_the_parent_ordinal_index_without_sorting() {
        let store = store();
        for (sql, floor) in [
            (LIST_COMMAND_CHILDREN_FIRST_SQL, None),
            (LIST_COMMAND_CHILDREN_CURSOR_SQL, Some(0)),
        ] {
            let mut statement = store
                .connection
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap();
            let mut plan = Vec::new();
            let rows = match floor {
                None => statement.query_map(params!["parent-1", 100], plan_detail),
                Some(floor) => statement.query_map(params!["parent-1", floor, 100], plan_detail),
            }
            .unwrap();
            for row in rows {
                plan.push(row.unwrap());
            }
            let plan = plan.join("\n");
            assert!(
                plan.contains("USING INDEX commands_children"),
                "plan should seek the child index, got:\n{plan}"
            );
            assert!(
                !plan.contains("SCAN commands"),
                "plan should not table-scan commands, got:\n{plan}"
            );
            assert!(
                !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
                "plan should not sort, got:\n{plan}"
            );
        }
    }
}
