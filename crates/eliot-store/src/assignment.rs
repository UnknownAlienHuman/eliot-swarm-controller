//! Folder assignment cursor and slots on ordinary launch commands and variables.
use super::*;
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentStatus {
    pub cursor: Option<String>,
    pub busy: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AssignmentLaunch {
    key: String,
    brief: String,
    previous: Option<String>,
    max_busy: u64,
}

fn validate_key(key: &str) -> Result<()> {
    if key.is_empty()
        || key.len() > 128
        || !key
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
    {
        return Err(Error::new("invalid", "invalid assignment key"));
    }
    Ok(())
}

fn status(connection: &Connection, key: &str) -> Result<AssignmentStatus> {
    validate_key(key)?;
    let value: Option<String> = connection
        .query_row(
            "SELECT value FROM vars WHERE scope='global' AND key=?1",
            [format!("assignment.{key}")],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    let cursor = value
        .map(|value| -> Result<String> {
            decode_json(&value)?
                .get("cursor")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| Error::new("source_integrity", "assignment cursor is malformed"))
        })
        .transpose()?;
    // Commands and native terminal turns own slots. Process silence or an idle
    // projection never settles an uncertain launch/input.
    let busy: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM commands c
         WHERE c.verb='launch' AND json_extract(c.args,'$.assignment.key')=?1
           AND (c.state IN ('accepted','queued','dispatched','unknown')
             OR EXISTS (SELECT 1 FROM commands s WHERE s.parent_command=c.id
                 AND s.link_kind='send' AND
                 (s.state IN ('accepted','queued','dispatched','unknown')
                   OR (s.state='applied' AND (s.turn_id IS NULL OR EXISTS (
                     SELECT 1 FROM turns t WHERE t.agent=s.target
                       AND t.generation=s.generation AND t.turn_id=s.turn_id
                       AND t.state IN ('running','unknown')))))))",
            [key],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    Ok(AssignmentStatus {
        cursor,
        busy: usize::try_from(busy)
            .map_err(|_| Error::new("source_integrity", "invalid assignment slot count"))?,
    })
}

impl Store {
    pub fn assignment_status(&self, key: &str) -> Result<AssignmentStatus> {
        self.ensure_committed_reads()?;
        status(&self.connection, key)
    }
}

/// Called inside ordinary launch admission. Its cursor update rolls back with
/// any later hold/capacity/command failure, and is visible only after commit.
pub(super) fn commit_cursor_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    now: i64,
) -> Result<()> {
    let Some(value) = request.args.get("assignment") else {
        return Ok(());
    };
    if principal != "owner" {
        return Err(Error::new(
            "not_allowed",
            "folder assignment requires Owner automation",
        ));
    }
    let launch: AssignmentLaunch = serde_json::from_value(value.clone())
        .map_err(|_| Error::new("invalid", "invalid assignment launch"))?;
    validate_key(&launch.key)?;
    if launch.max_busy == 0
        || launch.max_busy > 1_000
        || launch.brief.is_empty()
        || launch.brief.len() > 255
        || launch.brief.contains(['/', '\\'])
        || launch.brief.chars().any(char::is_control)
        || launch
            .previous
            .as_ref()
            .is_some_and(|old| old >= &launch.brief)
        || request
            .args
            .get("text")
            .and_then(Value::as_str)
            .is_none_or(|text| text.trim().is_empty())
    {
        return Err(Error::new(
            "invalid",
            "invalid assignment brief or concurrency",
        ));
    }
    let current = status(tx, &launch.key)?;
    if current.cursor != launch.previous {
        return Err(Error::new(
            "conflict",
            "assignment cursor changed before admission",
        ));
    }
    if current.busy as u64 >= launch.max_busy {
        return Err(Error::new(
            "capacity_unavailable",
            "assignment slots are occupied",
        ));
    }
    tx.execute(
        "INSERT INTO vars(scope,key,value,version,set_by,note,set_at)
         VALUES ('global',?1,?2,1,'owner','automation assignment cursor',?3)
         ON CONFLICT(scope,key) DO UPDATE SET value=excluded.value,
           version=vars.version+1,set_by=excluded.set_by,note=excluded.note,set_at=excluded.set_at",
        params![
            format!("assignment.{}", launch.key),
            encode_json(&json!({"cursor":launch.brief,"command_id":request.id}))?,
            now
        ],
    )
    .map_err(sql_error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn launch(previous: Option<&str>, brief: &str) -> SubmitCommand {
        let mut request = SubmitCommand::new(
            Ulid::from(
                (1_800_000_000_000_u128 << 80) | (u128::from(Ulid::new()) & ((1_u128 << 80) - 1)),
            )
            .to_string(),
            "launch",
        );
        request.args = json!({"project":"p","workspace":"w","harness":"codex",
            "profile":"worker","profile_snapshot":{},"text":"read brief",
            "assignment":{"key":"pool","brief":brief,"previous":previous,"max_busy":2}});
        request
    }

    #[test]
    fn cursor_commits_with_launch_and_unresolved_slots_include_unknown() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let first = launch(None, "01.md");
        let admitted = store
            .submit("owner", first.clone(), 1_800_000_000_000)
            .unwrap();
        let second = launch(Some("01.md"), "02.md");
        store.submit("owner", second, 1_800_000_000_000).unwrap();
        store
            .connection
            .execute(
                "UPDATE commands SET state='unknown' WHERE id=?1",
                [&first.id],
            )
            .unwrap();
        assert_eq!(
            store.assignment_status("pool").unwrap(),
            AssignmentStatus {
                cursor: Some("02.md".into()),
                busy: 2
            }
        );
        let third = launch(Some("02.md"), "03.md");
        assert_eq!(
            store
                .submit("owner", third.clone(), 1_800_000_000_000)
                .unwrap_err()
                .code,
            "capacity_unavailable"
        );
        assert_eq!(
            store.assignment_status("pool").unwrap().cursor.as_deref(),
            Some("02.md")
        );
        assert!(
            !store
                .submit("owner", first, 1_800_000_000_000)
                .unwrap()
                .is_new
        );
        store
            .connection
            .execute(
                "UPDATE commands SET state='failed' WHERE id=?1 OR parent_command=?1",
                [&admitted.command.id],
            )
            .unwrap();
        store.submit("owner", third, 1_800_000_000_000).unwrap();
        assert_eq!(
            store.assignment_status("pool").unwrap().cursor.as_deref(),
            Some("03.md")
        );
    }

    #[test]
    fn later_capacity_refusal_rolls_back_cursor_and_agent() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let mut request = launch(None, "01.md");
        request.args["profile_snapshot"] =
            json!({"capacity_scopes":[{"scope":"profile:worker","max_live":0}]});
        assert_eq!(
            store
                .submit("owner", request, 1_800_000_000_000)
                .unwrap_err()
                .code,
            "capacity_unavailable"
        );
        assert_eq!(
            store.assignment_status("pool").unwrap(),
            AssignmentStatus {
                cursor: None,
                busy: 0
            }
        );
        let count: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM agents", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn completed_native_turn_releases_slot_but_unknown_turn_holds_it() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let request = launch(None, "01.md");
        let admission = store.submit("owner", request, 1_800_000_000_000).unwrap();
        let id = admission.command.id;
        store.connection.execute("UPDATE commands SET state='applied',turn_id=CASE WHEN verb='send' THEN 't1' END WHERE id=?1 OR parent_command=?1",[&id]).unwrap();
        store
            .connection
            .execute(
                "INSERT INTO turns(agent,generation,turn_id,state) VALUES (1,1,'t1','unknown')",
                [],
            )
            .unwrap();
        assert_eq!(store.assignment_status("pool").unwrap().busy, 1);
        store
            .connection
            .execute("UPDATE turns SET state='completed'", [])
            .unwrap();
        assert_eq!(store.assignment_status("pool").unwrap().busy, 0);
    }
}
