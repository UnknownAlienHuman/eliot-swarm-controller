//! Folder assignment admissions and slots on ordinary launch commands.
use super::*;
use serde::Deserialize;
use std::collections::HashSet;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AssignmentLaunch {
    key: String,
    brief: String,
    max_busy: u64,
    /// The brief's frozen Goal for Goal-owned work.
    #[serde(default)]
    goal: Option<core::api::GoalDefinition>,
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

fn require_slot(connection: &Connection, key: &str, max_busy: u64) -> Result<()> {
    validate_key(key)?;
    if max_busy == 0 || max_busy > 1_000 {
        return Err(Error::new("invalid", "invalid assignment capacity bound"));
    }
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
    if busy >= max_busy as i64 {
        return Err(Error::new(
            "capacity_unavailable",
            format!("configured capacity bound assignment:{key} max_busy={max_busy}"),
        ));
    }
    Ok(())
}

impl Store {
    /// Return the folder-assignment key whose Owner launch created this exact
    /// Agent generation, when exactly one launch claims it. Used by
    /// retirement cleanup after an applied stop; ambiguous or absent
    /// launches retire with no cleanup.
    pub fn assignment_key_for_agent(&self, agent: &str, generation: i64) -> Result<Option<String>> {
        let agent_num = parse_agent_id(agent)?;
        self.ensure_committed_reads()?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT DISTINCT json_extract(args,'$.assignment.key') FROM commands
                 WHERE verb='launch' AND principal='owner' AND target=?1 AND generation=?2
                   AND json_extract(args,'$.assignment.key') IS NOT NULL LIMIT 2",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![agent_num, generation], |row| {
                row.get::<_, String>(0)
            })
            .map_err(sql_error)?;
        let mut found: Option<String> = None;
        let mut count = 0;
        for key in rows {
            count += 1;
            found = Some(key.map_err(sql_error)?);
        }
        if count != 1 {
            return Ok(None);
        }
        let key = found.ok_or_else(|| Error::new("source_integrity", "assignment key vanished"))?;
        validate_key(&key)?;
        Ok(Some(key))
    }

    pub fn require_assignment_slot(&self, key: &str, max_busy: u64) -> Result<()> {
        self.ensure_committed_reads()?;
        require_slot(&self.connection, key, max_busy)
    }

    pub fn unadmitted_briefs(&self, key: &str, briefs: Vec<String>) -> Result<Vec<String>> {
        self.ensure_committed_reads()?;
        let mut present = self
            .connection
            .prepare_cached(
                "SELECT 1 FROM assignment_admissions WHERE assignment_key=?1 AND brief=?2",
            )
            .map_err(sql_error)?;
        let mut unadmitted = Vec::new();
        for name in briefs {
            if !present.exists(params![key, name]).map_err(sql_error)? {
                unadmitted.push(name);
            }
        }
        Ok(unadmitted)
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
        if event_agent != agent_num || event_generation != generation {
            return Ok(false);
        }
        match kind.as_str() {
            // One-turn work retires on its completed turn, unless a
            // Goal-owned assignment Goal still owns this generation's work.
            "turn.completed" => {
                if goals::assignment_goal_open(&self.connection, agent_num, generation)? {
                    return Ok(false);
                }
            }
            // Goal-owned work retires on its Goal's terminal fact.
            "goal.changed" => {}
            _ => return Ok(false),
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

/// Record the name inside ordinary launch admission. The insert rolls back with
/// any later admission failure and is visible only after commit. Returns the
/// brief's frozen Goal and its launch cause for Goal-owned work.
pub(super) fn record_admission_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
) -> Result<Option<(core::api::GoalDefinition, core::EventCause)>> {
    let Some(value) = request.args.get("assignment") else {
        return Ok(None);
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
    require_slot(tx, &launch.key, launch.max_busy)?;
    let inserted = tx
        .execute(
            "INSERT INTO assignment_admissions (assignment_key, brief) VALUES (?1,?2)
             ON CONFLICT (assignment_key, brief) DO NOTHING",
            params![launch.key, launch.brief],
        )
        .map_err(sql_error)?;
    if inserted == 0 {
        return Err(Error::new(
            "conflict",
            "assignment brief was already admitted",
        ));
    }
    Ok(launch.goal.map(|goal| {
        let cause = core::EventCause {
            command_id: request.id.clone(),
            rule: Some(format!("assignment-{}", launch.key)),
            depth: 1,
            remote_call: None,
        };
        (goal, cause)
    }))
}
