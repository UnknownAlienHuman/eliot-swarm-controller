use crate::usage::manager_at_sql;
use eliot_core::api::{
    AutonomyChecks, AutonomyExecutorProfile, AutonomyExecutorSwarm, AutonomyNotes, AutonomyRow,
    AutonomySteer, AutonomyWaits, SWARM_MAIL_SENDER,
};
use rusqlite::{Connection, params};

const MAX_INPUT_ROWS: usize = 10_000;
const INPUT_PROBE_ROWS: i64 = MAX_INPUT_ROWS as i64 + 1;
const MAX_PROFILES: usize = 20;
const PROFILE_PROBE_ROWS: i64 = MAX_PROFILES as i64 + 1;

// SQLite matches partial-index predicates by syntax. Share them with the reads.
macro_rules! shell_start {
    () => {
        "kind = 'tool.started' AND source <> 'daemon'
         AND lower(json_extract(data, '$.tool')) IN (
             'commandexecution', 'bash', 'powershell', 'exec_command', 'shell',
             'usershell', 'run_command', 'run command'
         )"
    };
}

macro_rules! swarm_start {
    () => {
        "kind = 'tool.started' AND source = 'daemon' AND agent IS NOT NULL
         AND json_extract(data, '$.origin') = 'swarm_mcp'
         AND json_extract(data, '$.tool') = 'swarm'"
    };
}

macro_rules! inject_applied {
    () => {
        "kind = 'command.applied' AND source = 'daemon' AND (
             json_extract(data, '$.verb') = 'inject'
             OR json_extract(data, '$.outcome.application_point') IN ('next_input', 'turn_safe_point')
             OR json_extract(data, '$.outcome.context_outcome.application_point') = 'active_turn'
         )"
    };
}

pub(super) const INDEXES: &str = concat!(
    "
CREATE INDEX autonomy_activity_events_window ON events(kind, ts, seq, agent)
    WHERE kind = 'turn.started' OR kind = 'goal.round'
       OR kind = 'project.check.finished' OR kind = 'mail.delivered';
CREATE INDEX autonomy_activity_shell_window ON events(ts, seq) WHERE ",
    shell_start!(),
    ";
CREATE INDEX autonomy_activity_mcp_window ON events(ts, seq) WHERE ",
    swarm_start!(),
    ";
CREATE INDEX autonomy_activity_inject_window ON events(ts, seq) WHERE ",
    inject_applied!(),
    ";
CREATE INDEX autonomy_activity_commands_window ON commands(created_at, id) WHERE verb = 'steer';
CREATE INDEX autonomy_activity_launch ON commands(target, created_at, id) WHERE verb = 'launch';
"
);

pub(super) struct Activity {
    pub(super) steer: AutonomyRow<AutonomySteer>,
    pub(super) checks: AutonomyRow<AutonomyChecks>,
    pub(super) waits: AutonomyRow<AutonomyWaits>,
    pub(super) notes: AutonomyRow<AutonomyNotes>,
    pub(super) executor_swarm: AutonomyRow<AutonomyExecutorSwarm>,
}

pub(super) fn read(tx: &Connection, since: i64, until: i64) -> eliot_core::Result<Activity> {
    let (checks, waits) = read_shell_activity(tx, since, until)?;
    Ok(Activity {
        steer: read_steer(tx, since, until)?,
        checks,
        waits,
        notes: read_notes(tx, since, until)?,
        executor_swarm: read_executor_swarm(tx, since, until)?,
    })
}

fn read_steer(
    tx: &Connection,
    since: i64,
    until: i64,
) -> eliot_core::Result<AutonomyRow<AutonomySteer>> {
    let mut statement = tx
        .prepare(
            "SELECT principal, json_type(cause, '$.rule') IS 'text'
                    OR COALESCE(link_kind = 'rule_action', 0)
             FROM commands INDEXED BY autonomy_activity_commands_window
             WHERE verb = 'steer' AND created_at BETWEEN ?1 AND ?2
             ORDER BY created_at, id LIMIT ?3",
        )
        .map_err(crate::sql_error)?;
    let rows = statement
        .query_map(params![since, until, INPUT_PROBE_ROWS], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?))
        })
        .map_err(crate::sql_error)?;
    let mut counts = AutonomySteer {
        manual: 0,
        swarm: 0,
    };
    let mut reasons = vec!["command retention can remove earlier steer commands".into()];
    let mut unclassified = false;
    for (index, row) in rows.enumerate() {
        if index == MAX_INPUT_ROWS {
            add_cap_reason(&mut reasons, "steer commands");
            break;
        }
        let (principal, rule) = row.map_err(crate::sql_error)?;
        if rule || principal.starts_with("project:") || principal == SWARM_MAIL_SENDER {
            counts.swarm += 1;
        } else if principal == "owner" || principal.starts_with("agent:") {
            counts.manual += 1;
        } else {
            unclassified = true;
        }
    }
    if unclassified {
        reasons.push("some retained steers have no recognized caller or rule attribution".into());
    }
    Ok(coverage_row(
        "commands principal and rule cause",
        reasons,
        counts,
    ))
}

fn read_shell_activity(
    tx: &Connection,
    since: i64,
    until: i64,
) -> eliot_core::Result<(AutonomyRow<AutonomyChecks>, AutonomyRow<AutonomyWaits>)> {
    let mut statement = tx
        .prepare(concat!(
            "SELECT json_extract(e.data, '$.tool'), json_extract(e.data, '$.command'),
                    COALESCE(json_extract(e.data, '$.command_truncated'), 0),
                    (SELECT json_extract(c.args, '$._project_check_goal.check')
                     FROM commands c INDEXED BY autonomy_activity_launch
                     WHERE c.verb = 'launch' AND c.target = e.agent
                     ORDER BY c.created_at, c.id LIMIT 1)
             FROM events e INDEXED BY autonomy_activity_shell_window
             WHERE ",
            shell_start!(),
            " AND e.ts BETWEEN ?1 AND ?2
             ORDER BY e.ts, e.seq LIMIT ?3",
        ))
        .map_err(crate::sql_error)?;
    let rows = statement
        .query_map(params![since, until, INPUT_PROBE_ROWS], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(crate::sql_error)?;
    let mut manual = 0;
    let mut waits = 0;
    let mut reasons =
        vec!["native command capture depends on the adapter and can be redacted".into()];
    let mut missing_command = false;
    for (index, row) in rows.enumerate() {
        if index == MAX_INPUT_ROWS {
            add_cap_reason(&mut reasons, "shell tool starts");
            break;
        }
        let (tool, command, truncated, project_check) = row.map_err(crate::sql_error)?;
        missing_command |= truncated || command.is_none();
        let Some(command) = command else { continue };
        let check: Option<Vec<String>> =
            project_check.and_then(|argv| serde_json::from_str(&argv).ok());
        let (check_runs, is_wait) = super::shell::matches(&command, check.as_deref(), &tool);
        manual += check_runs;
        waits += u64::from(is_wait);
    }
    if missing_command {
        reasons.push("one or more shell commands were missing or truncated".into());
    }
    let wait_row = coverage_row(
        "native tool.started",
        reasons.clone(),
        AutonomyWaits { calls: waits },
    );

    let (goals, goal_overflow) = bounded_count(
        tx,
        "SELECT COUNT(*), COALESCE(SUM(has_exit), 0) FROM (
             SELECT json_type(data, '$.exit_code') IS 'integer' AS has_exit
             FROM events INDEXED BY autonomy_activity_events_window
             WHERE kind = 'goal.round' AND source = 'daemon' AND ts BETWEEN ?1 AND ?2
             ORDER BY ts, seq LIMIT ?3
         )",
        since,
        until,
    )?;
    let (fast, fast_overflow) = bounded_count(
        tx,
        "SELECT COUNT(*), COUNT(*) FROM (
             SELECT 1 FROM events INDEXED BY autonomy_activity_events_window
             WHERE kind = 'project.check.finished' AND ts BETWEEN ?1 AND ?2
             ORDER BY ts, seq LIMIT ?3
         )",
        since,
        until,
    )?;
    if goal_overflow || fast_overflow {
        add_cap_reason(&mut reasons, "Swarm check facts");
    }
    reasons.push("command retention can remove the frozen launch check".into());
    reasons.push("Swarm counts recorded check exits. Older fast checks and checks without a recorded exit are absent".into());
    Ok((
        coverage_row(
            "native tool.started, goal.round, project.check.finished",
            reasons,
            AutonomyChecks {
                manual,
                swarm: goals + fast,
            },
        ),
        wait_row,
    ))
}

fn read_notes(
    tx: &Connection,
    since: i64,
    until: i64,
) -> eliot_core::Result<AutonomyRow<AutonomyNotes>> {
    let (by_kind, truncated) = crate::context::notes::read_counters(tx, since, until)?;
    let counts = AutonomyNotes {
        delivered: by_kind.iter().map(|kind| kind.delivered).sum(),
        moved: by_kind.iter().map(|kind| kind.moved).sum(),
        cancelled: by_kind.iter().map(|kind| kind.cancelled).sum(),
        expired: by_kind.iter().map(|kind| kind.expired).sum(),
        by_kind,
    };
    let mut row = AutonomyRow::partial(
        "retained fact-note settlements, context moves, and automatic background-process wakes",
        "older outcomes without fact-note provenance are outside this source",
        counts,
    );
    if truncated {
        row.limit_coverage("the fact-note outcome scan reached its limit");
    }
    Ok(row)
}

/// The probe row establishes truncation. Including it in the lower bound
/// keeps the source query a single capped indexed range.
fn bounded_count(
    tx: &Connection,
    sql: &str,
    since: i64,
    until: i64,
) -> eliot_core::Result<(u64, bool)> {
    let (examined, matching): (i64, i64) = tx
        .query_row(sql, params![since, until, INPUT_PROBE_ROWS], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .map_err(crate::sql_error)?;
    Ok((as_count(matching), examined == INPUT_PROBE_ROWS))
}

fn read_executor_swarm(
    tx: &Connection,
    since: i64,
    until: i64,
) -> eliot_core::Result<AutonomyRow<AutonomyExecutorSwarm>> {
    let manager = manager_at_sql(
        "observation.agent",
        "COALESCE(t.completed_at, observation.ts)",
    );
    let mut statement = tx
        .prepare(&format!(concat!(
            "WITH
             turn_probe AS (
                 SELECT agent, generation, json_extract(data, '$.turn_id') AS turn_id, ts
                 FROM events INDEXED BY autonomy_activity_events_window
                 WHERE kind = 'turn.started' AND ts BETWEEN ?1 AND ?2 AND agent IS NOT NULL
                 ORDER BY ts, seq LIMIT ?3
             ),
             turn_input AS (
                 SELECT * FROM turn_probe LIMIT ?4
             ),
             turn_meta AS (
                 SELECT COUNT(*) > ?4 AS truncated FROM turn_probe
             ),
             call_probe AS (
                 SELECT agent, generation, json_extract(data, '$.turn_id') AS turn_id, ts
                 FROM events INDEXED BY autonomy_activity_mcp_window
                 WHERE ", swarm_start!(), "
                   AND ts BETWEEN ?1 AND ?2
                 ORDER BY ts, seq LIMIT ?3
             ),
             call_input AS (
                 SELECT * FROM call_probe LIMIT ?4
             ),
             call_meta AS (
                 SELECT COUNT(*) > ?4 AS truncated FROM call_probe
             ),
             cohort AS (
                 SELECT *, 0 AS called FROM turn_input
                 UNION ALL
                 SELECT *, 1 AS called FROM call_input
             ),
             executors AS (
                 SELECT observation.agent, MAX(observation.called) AS called
                 FROM cohort observation
                 LEFT JOIN turns t ON t.agent = observation.agent AND t.generation = observation.generation
                                  AND t.turn_id = observation.turn_id
                 WHERE NOT {manager}
                 GROUP BY observation.agent
             ),
             profile_groups AS (
                 SELECT a.profile AS profile,
                        COUNT(*) AS executors,
                        SUM(executors.called) AS called_swarm
                 FROM executors JOIN agents AS a ON a.id = executors.agent
                 WHERE a.origin = 'eliot'
                 GROUP BY a.profile
             ),
             profile_probe AS (
                 SELECT profile, executors, called_swarm
                 FROM profile_groups ORDER BY profile LIMIT ?5
             ),
             profile_meta AS (
                 SELECT COUNT(*) > ?6 AS truncated FROM profile_groups
             )
             SELECT p.profile, p.executors, p.called_swarm,
                    (SELECT truncated FROM turn_meta),
                    (SELECT truncated FROM call_meta),
                    (SELECT truncated FROM profile_meta)
             FROM profile_meta
             LEFT JOIN profile_probe AS p ON 1 = 1
             ORDER BY p.profile",
        ), manager = manager))
        .map_err(crate::sql_error)?;
    let rows = statement
        .query_map(
            params![
                since,
                until,
                INPUT_PROBE_ROWS,
                MAX_INPUT_ROWS as i64,
                PROFILE_PROBE_ROWS,
                MAX_PROFILES as i64,
            ],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .map_err(crate::sql_error)?;

    let mut profiles = Vec::new();
    let mut turns_truncated = false;
    let mut calls_truncated = false;
    let mut profiles_truncated = false;
    for row in rows {
        let (profile, executors, called_swarm, turns_over, calls_over, profiles_over) =
            row.map_err(crate::sql_error)?;
        turns_truncated = turns_over != 0;
        calls_truncated = calls_over != 0;
        profiles_truncated = profiles_over != 0;
        let (Some(profile), Some(executors), Some(called_swarm)) =
            (profile, executors, called_swarm)
        else {
            continue;
        };
        if profiles.len() == MAX_PROFILES {
            break;
        }
        profiles.push(AutonomyExecutorProfile {
            profile,
            executors: as_count(executors),
            called_swarm: as_count(called_swarm),
        });
    }

    let mut reasons = Vec::new();
    if turns_truncated {
        reasons.push(format!(
            "turn.started input exceeded the {MAX_INPUT_ROWS}-row cap. Executor counts are a lower bound"
        ));
    }
    if calls_truncated {
        reasons.push(format!(
            "tool.started input exceeded the {MAX_INPUT_ROWS}-row cap. Executor call attribution is a lower bound"
        ));
    }
    if profiles_truncated {
        reasons.push(format!(
            "profile output exceeded the {MAX_PROFILES}-profile cap. Remaining profiles are omitted"
        ));
    }
    Ok(coverage_row(
        "agents, turn.started, and Swarm MCP tool.started",
        reasons,
        AutonomyExecutorSwarm {
            profiles,
            profiles_truncated,
        },
    ))
}

fn add_cap_reason(reasons: &mut Vec<String>, source: &str) {
    reasons.push(format!(
        "{source} exceeded the {MAX_INPUT_ROWS}-row input cap. Counts are a lower bound"
    ));
}

fn coverage_row<T>(source: &str, reasons: Vec<String>, counts: T) -> AutonomyRow<T> {
    if reasons.is_empty() {
        AutonomyRow::complete(source, counts)
    } else {
        AutonomyRow::partial(source, &reasons.join(". "), counts)
    }
}

fn as_count(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
