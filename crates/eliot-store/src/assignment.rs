//! Folder assignment cursor and slots on ordinary launch commands and variables.
use super::*;
use serde::Deserialize;
use std::collections::HashSet;

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

fn status(connection: &Connection, key: &str, max_busy: u64) -> Result<AssignmentStatus> {
    validate_key(key)?;
    if max_busy == 0 || max_busy > 1_000 {
        return Err(Error::new("invalid", "invalid assignment capacity bound"));
    }
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
    // The partial index contains only durable unresolved membership. LIMIT
    // bounds each admission/status recount by the configured capacity.
    let busy: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM (
             SELECT id FROM commands INDEXED BY commands_assignment_busy
             WHERE verb='launch' AND assignment_busy=1
               AND json_extract(args,'$.assignment.key')=?1
             LIMIT ?2)",
            params![key, max_busy as i64],
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
    pub fn assignment_status(&self, key: &str, max_busy: u64) -> Result<AssignmentStatus> {
        self.ensure_committed_reads()?;
        status(&self.connection, key, max_busy)
    }

    /// Verifies that an event's retained Store cause descends from an Owner
    /// assignment launch for the exact Agent generation and assignment key.
    pub fn is_assignment_event(
        &self,
        seq: i64,
        key: &str,
        agent: &str,
        generation: i64,
    ) -> Result<bool> {
        self.ensure_committed_reads()?;
        validate_key(key)?;
        let agent_num = parse_agent_id(agent)?;
        if seq <= 0 || generation <= 0 {
            return Ok(false);
        }
        let event = self
            .connection
            .query_row(
                "SELECT kind, agent, generation, cause FROM events WHERE seq=?1",
                [seq],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?;
        let Some((kind, Some(event_agent), Some(event_generation), Some(cause))) = event else {
            return Ok(false);
        };
        if kind != "turn.completed" || event_agent != agent_num || event_generation != generation {
            return Ok(false);
        }
        let cause: core::EventCause = serde_json::from_str(&cause)
            .map_err(|_| Error::new("source_integrity", "event cause is malformed"))?;
        let assignment_rule = format!("assignment-{key}");
        if cause.rule.as_deref() != Some(assignment_rule.as_str()) {
            return Ok(false);
        }
        let mut command_id = cause.command_id;
        let mut visited = HashSet::new();
        for _ in 0..=16 {
            if !visited.insert(command_id.clone()) {
                return Ok(false);
            }
            let command = self
                .connection
                .query_row(
                    "SELECT principal, verb, args, target, generation, parent_command
                     FROM commands WHERE id=?1",
                    [&command_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, Option<i64>>(3)?,
                            row.get::<_, Option<i64>>(4)?,
                            row.get::<_, Option<String>>(5)?,
                        ))
                    },
                )
                .optional()
                .map_err(sql_error)?;
            let Some((principal, verb, args, target, command_generation, parent)) = command else {
                break;
            };
            if verb == "launch" {
                let launch_args: Value = decode_json(&args)?;
                let assignment_launch_matches = principal == "owner"
                    && target == Some(agent_num)
                    && command_generation == Some(generation)
                    && launch_args
                        .get("assignment")
                        .and_then(|assignment| assignment.get("key"))
                        .and_then(Value::as_str)
                        == Some(key);
                if !assignment_launch_matches {
                    return Ok(false);
                }
                let Some(parent) = parent else {
                    return Ok(false);
                };
                let parent_command = self
                    .connection
                    .query_row(
                        "SELECT principal, verb, args FROM commands WHERE id=?1",
                        [parent],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(sql_error)?;
                let Some((parent_principal, parent_verb, parent_args)) = parent_command else {
                    return Ok(false);
                };
                let parent_args: Value = decode_json(&parent_args)?;
                return Ok(parent_principal == "owner"
                    && matches!(parent_verb.as_str(), "rule" | "timer.run" | "rule.catch_up")
                    && parent_args.get("rule").and_then(Value::as_str)
                        == Some(assignment_rule.as_str()));
            }
            let Some(parent) = parent else {
                break;
            };
            command_id = parent;
        }
        Ok(false)
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
    let current = status(tx, &launch.key, launch.max_busy)?;
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
            store.assignment_status("pool", 2).unwrap(),
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
            store
                .assignment_status("pool", 2)
                .unwrap()
                .cursor
                .as_deref(),
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
            store
                .assignment_status("pool", 2)
                .unwrap()
                .cursor
                .as_deref(),
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
            store.assignment_status("pool", 2).unwrap(),
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
        assert_eq!(store.assignment_status("pool", 2).unwrap().busy, 1);
        store
            .connection
            .execute("UPDATE turns SET state='completed'", [])
            .unwrap();
        assert_eq!(store.assignment_status("pool", 2).unwrap().busy, 0);
    }

    #[test]
    fn assignment_count_uses_bounded_partial_membership_after_large_completed_history() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let request = launch(None, "01.md");
        let admission = store
            .submit("owner", request.clone(), 1_800_000_000_000)
            .unwrap();
        let agent = admission.command.target.clone().unwrap();
        store
            .connection
            .execute(
                "UPDATE commands SET state='unknown',generation=1 WHERE id=?1",
                [&request.id],
            )
            .unwrap();

        let args = json!({"assignment":{"key":"pool"}}).to_string();
        let agent_num = parse_agent_id(&agent).unwrap();
        {
            let tx = store.connection.transaction().unwrap();
            for index in 0..5_000 {
                let launch_id = Ulid::new().to_string();
                let send_id = Ulid::new().to_string();
                let turn_id = format!("completed-{index}");
                tx.execute(
                    "INSERT INTO commands(id,principal,method,payload_digest,target,
                         expected_generation,generation,verb,args,state,created_at,updated_at)
                     VALUES (?1,'owner','command.submit',?2,?3,0,1,'launch',?4,'applied',1,1)",
                    params![launch_id, [0_u8; 32].as_slice(), agent_num, args],
                )
                .unwrap();
                tx.execute(
                    "INSERT INTO turns(agent,generation,turn_id,state) VALUES (?1,1,?2,'completed')",
                    params![agent_num, turn_id],
                )
                .unwrap();
                tx.execute(
                    "INSERT INTO commands(id,principal,method,payload_digest,target,generation,
                         parent_command,link_kind,ordinal,turn_id,verb,args,state,created_at,updated_at)
                     VALUES (?1,'owner','command.submit',?2,?3,1,?4,'send',0,?5,'send','{}','applied',1,1)",
                    params![send_id, [0_u8; 32].as_slice(), agent_num, launch_id, turn_id],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        for _ in 0..2 {
            store
                .connection
                .execute(
                    "INSERT INTO commands(id,principal,method,payload_digest,target,
                     expected_generation,generation,verb,args,state,created_at,updated_at)
                 VALUES (?1,'owner','command.submit',?2,?3,0,1,'launch',?4,'unknown',1,1)",
                    params![
                        Ulid::new().to_string(),
                        [0_u8; 32].as_slice(),
                        agent_num,
                        args
                    ],
                )
                .unwrap();
        }

        assert_eq!(store.assignment_status("pool", 1).unwrap().busy, 1);
        assert_eq!(store.assignment_status("pool", 2).unwrap().busy, 2);
        assert_eq!(store.assignment_status("pool", 3).unwrap().busy, 3);
        let mut statement = store
            .connection
            .prepare(
                "SELECT COUNT(*) FROM (
                 SELECT id FROM commands INDEXED BY commands_assignment_busy
                 WHERE verb='launch' AND assignment_busy=1
                   AND json_extract(args,'$.assignment.key')=?1 LIMIT ?2)",
            )
            .unwrap();
        assert_eq!(
            statement
                .query_row(params!["pool", 1], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert!(statement.get_status(rusqlite::StatementStatus::VmStep) < 100);
        drop(statement);

        let mut statement = store
            .connection
            .prepare(
                "EXPLAIN QUERY PLAN
                 SELECT id FROM commands INDEXED BY commands_assignment_busy
                 WHERE verb='launch' AND assignment_busy=1
                   AND json_extract(args,'$.assignment.key')=?1 LIMIT ?2",
            )
            .unwrap();
        let plan = statement
            .query_map(params!["pool", 1], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .join(" ");
        assert!(
            plan.contains("SEARCH commands USING INDEX commands_assignment_busy"),
            "{plan}"
        );
        let indexed_members: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM commands INDEXED BY commands_assignment_busy
                 WHERE verb='launch' AND assignment_busy=1
                   AND json_extract(args,'$.assignment.key')='pool'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(indexed_members, 3);
        store
            .connection
            .execute(
                "UPDATE commands SET state='failed'
             WHERE link_kind='send' AND parent_command IN (
               SELECT id FROM commands WHERE verb='launch'
                 AND json_extract(args,'$.assignment.key')='pool')
               AND state IN ('accepted','queued','dispatched','unknown')",
                [],
            )
            .unwrap();
        store.connection.execute(
            "UPDATE commands SET state='failed'
             WHERE verb='launch' AND json_extract(args,'$.assignment.key')='pool' AND state='unknown'",
            [],
        ).unwrap();
        assert_eq!(store.assignment_status("pool", 1).unwrap().busy, 0);
    }

    #[test]
    fn assignment_event_requires_retained_owner_launch_and_matching_rule_parent() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let request = launch(None, "01.md");
        let admission = store
            .submit("owner", request.clone(), 1_800_000_000_000)
            .unwrap();
        let agent = admission.command.target.clone().unwrap();
        let linked_send = admission.linked_command.as_ref().unwrap().id.clone();
        let root_id = Ulid::new().to_string();
        store.connection.execute(
            "INSERT INTO commands(id,principal,method,payload_digest,verb,args,state,created_at,updated_at)
             VALUES (?1,'owner','timer.run',?2,'timer.run',?3,'applied',1,1)",
            params![root_id, [0_u8;32].as_slice(), json!({"rule":"assignment-pool"}).to_string()],
        ).unwrap();
        store
            .connection
            .execute(
                "UPDATE commands SET parent_command=?1,link_kind='rule_action',ordinal=0,
                 generation=1 WHERE id=?2",
                params![root_id, request.id],
            )
            .unwrap();
        let cause =
            json!({"command_id":linked_send,"rule":"assignment-pool","depth":0}).to_string();
        store
            .connection
            .execute(
                "INSERT INTO events(ts,kind,family,agent,generation,source,data,cause)
             VALUES (1,'turn.completed','turn',?1,1,'daemon','{}',?2)",
                params![parse_agent_id(&agent).unwrap(), cause],
            )
            .unwrap();
        let seq = store.connection.last_insert_rowid();
        assert!(!store.is_assignment_event(seq, "other", &agent, 1).unwrap());
        assert!(!store.is_assignment_event(seq, "pool", &agent, 2).unwrap());
        assert!(store.is_assignment_event(seq, "pool", &agent, 1).unwrap());

        store
            .connection
            .execute(
                "UPDATE commands SET principal='project:p' WHERE id=?1",
                [&request.id],
            )
            .unwrap();
        assert!(!store.is_assignment_event(seq, "pool", &agent, 1).unwrap());
        store
            .connection
            .execute(
                "UPDATE commands SET principal='owner' WHERE id=?1",
                [&request.id],
            )
            .unwrap();
        store
            .connection
            .execute(
                "UPDATE commands SET principal='project:p' WHERE id=?1",
                [&root_id],
            )
            .unwrap();
        assert!(!store.is_assignment_event(seq, "pool", &agent, 1).unwrap());
    }
}
