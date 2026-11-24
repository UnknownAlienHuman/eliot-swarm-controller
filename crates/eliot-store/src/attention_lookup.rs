use crate::{Store, event_from_record, event_record_from_row, sql_error};
use eliot_core::{Event, Result, parse_agent_id};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;

const MAX_ATTENTION_HISTORY_TURNS: usize = eliot_core::api::MAX_ATTENTION_HISTORY;
const MAX_ATTENTION_ACTIVITY_EVENTS_PER_TURN: usize = 1_024;
const MAX_ATTENTION_PROFILE_PAIRS: usize = eliot_core::api::MAX_ATTENTION_THRESHOLD_PAIRS;

/// The version is comparable only on this same connection. It holds no read
/// snapshot while the attention worker is idle, so it cannot pin the WAL.
pub struct AttentionVersionReader {
    connection: Connection,
}

impl AttentionVersionReader {
    pub fn open(path: &Path) -> Result<Self> {
        let store = Store::open_read_only(path)?;
        store
            .connection
            .execute_batch("ROLLBACK")
            .map_err(sql_error)?;
        Ok(Self {
            connection: store.connection,
        })
    }

    pub fn version(&self) -> Result<i64> {
        self.connection
            .query_row("PRAGMA data_version", [], |row| row.get(0))
            .map_err(sql_error)
    }
}

#[derive(Clone, Debug)]
struct SuccessfulAttentionTurn {
    agent: i64,
    generation: i64,
    turn_id: String,
    started_at: i64,
    completed_at: i64,
    started_seq: i64,
    completed_seq: i64,
    workspace: Option<String>,
}

/// Only direct Owner input to this exact turn can veto an automatic interrupt.
/// The accepted event orders input against the question even within one millisecond.
pub(super) fn owner_input_after_question(
    connection: &Connection,
    agent: i64,
    generation: i64,
    turn: &str,
    after: i64,
) -> Result<bool> {
    connection
        .query_row(
            "SELECT (SELECT pruned_through FROM rule_history_coverage WHERE singleton = 1) > ?4 OR EXISTS(
                 SELECT 1 FROM events AS e INDEXED BY events_agent
                 JOIN commands AS c ON c.id = json_extract(e.data, '$.command_id')
                 WHERE e.agent = ?1 AND e.generation = ?2 AND e.seq > ?4
                   AND e.kind = 'command.accepted'
                   AND c.target = ?1 AND c.generation = ?2
                   AND c.principal = 'owner' AND c.parent_command IS NULL
                   AND c.verb IN ('send', 'steer')
                   AND c.state NOT IN ('rejected', 'cancelled')
                   AND COALESCE(c.expected_turn_id, c.turn_id) = ?3
             )",
            params![agent, generation, turn, after],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

pub(super) fn guard_attention_interrupt(
    connection: &Connection,
    agent: i64,
    generation: i64,
    turn: &str,
    command_id: &str,
) -> Result<()> {
    let after: Option<i64> = connection
        .query_row(
            "SELECT after_question_seq FROM turn_attention_steps
             WHERE command_id = ?1 AND agent = ?2 AND generation = ?3 AND turn_id = ?4
               AND step = 'interrupt'",
            params![command_id, agent, generation, turn],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    if let Some(after) = after
        && owner_input_after_question(connection, agent, generation, turn, after)?
    {
        return Err(eliot_core::Error::new(
            "owner_input",
            "newer Owner input superseded the automatic interrupt",
        ));
    }
    Ok(())
}

impl Store {
    /// A timed step remains reserved for the lifetime of its exact turn,
    /// independent of diagnostic journal retention.
    pub fn attention_turn_step(
        &self,
        agent: &str,
        generation: i64,
        turn: &str,
        step: &str,
        reason: Option<&str>,
    ) -> Result<Option<(i64, i64)>> {
        self.connection
            .query_row(
                "SELECT event_seq, claimed_at FROM turn_attention_steps
             WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3
               AND step = ?4 AND (incident_kind = ?5 OR step = 'question')",
                params![
                    parse_agent_id(agent)?,
                    generation,
                    turn,
                    step,
                    reason.unwrap_or_default()
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)
    }

    pub fn attention_owner_input_after(
        &self,
        agent: &str,
        generation: i64,
        turn: &str,
        after: i64,
    ) -> Result<bool> {
        owner_input_after_question(
            &self.connection,
            parse_agent_id(agent)?,
            generation,
            turn,
            after,
        )
    }

    /// Read the retained attention report for one deterministic incident key.
    pub fn attention_incident_event(&self, incident_id: &str) -> Result<Option<Event>> {
        self.attention_report_event(incident_id, "incident")
    }

    pub fn attention_report_event(&self, incident_id: &str, phase: &str) -> Result<Option<Event>> {
        let record = self
            .connection
            .query_row(
                "SELECT seq, ts, kind, agent, generation, source, native_id, data, subject, cause
                 FROM events
                 WHERE kind = 'x.user.agent.attention'
                   AND json_extract(data, '$.data.incident_id') = ?1
                   AND json_extract(data, '$.data.phase') = ?2
                 ORDER BY seq
                 LIMIT 1",
                params![incident_id, phase],
                event_record_from_row,
            )
            .optional()
            .map_err(sql_error)?;
        record.map(event_from_record).transpose()
    }

    /// Read one persisted reaction step, including its commit timestamp and
    /// command binding, so a restarted worker resumes from the recorded step.
    pub fn attention_step_event(&self, incident_id: &str, step: &str) -> Result<Option<Event>> {
        let record = self
            .connection
            .query_row(
                "SELECT seq, ts, kind, agent, generation, source, native_id, data, subject, cause
                 FROM events
                 WHERE kind = 'x.user.agent.attention_step'
                   AND json_extract(data, '$.incident_id') = ?1
                   AND json_extract(data, '$.step') = ?2
                 ORDER BY seq DESC
                 LIMIT 1",
                params![incident_id, step],
                event_record_from_row,
            )
            .optional()
            .map_err(sql_error)?;
        record.map(event_from_record).transpose()
    }

    /// The immediately preceding turn-unknown fact is written in the same
    /// loss transaction and can preserve an exact turn after the Agent
    /// projection has already cleared its active-turn field.
    pub fn attention_loss_turn_id(
        &self,
        agent: &str,
        generation: i64,
        loss_event_seq: i64,
    ) -> Result<Option<String>> {
        let agent_num = parse_agent_id(agent)?;
        let exact: Option<Option<String>> = self
            .connection
            .query_row(
                "SELECT json_extract(data, '$.turn_id') FROM events
                 WHERE seq = ?1 AND agent = ?2 AND generation = ?3 AND kind = 'agent.lost'",
                params![loss_event_seq, agent_num, generation],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        if let Some(turn) = exact.flatten() {
            return Ok(Some(turn));
        }
        let Some(loss_ts) = self.daemon_event_timestamp(loss_event_seq)? else {
            return Ok(None);
        };
        self.connection
            .query_row(
                "SELECT json_extract(data, '$.turn_id')
                 FROM events
                 WHERE kind = 'turn.unknown'
                   AND agent = ?1
                   AND generation = ?2
                   AND seq < ?3
                   AND ts = ?4
                 ORDER BY seq DESC
                 LIMIT 1",
                params![agent_num, generation, loss_event_seq, loss_ts],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)
    }

    /// Current, non-archived profile/project pairs for the bounded attention
    /// status view, with the existing worktree/read-only guard for Git history.
    /// Returns at most `limit + 1` entries so the caller can report truncation.
    pub fn attention_profile_pairs(&self, limit: usize) -> Result<Vec<(String, String, bool)>> {
        let limit = limit.clamp(1, MAX_ATTENTION_PROFILE_PAIRS);
        let mut statement = self
            .connection
            .prepare(
                "SELECT DISTINCT profile, project FROM agents
                 WHERE archived = 0 AND generation > 0 AND project <> ''
                 ORDER BY project, profile LIMIT ?1",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![limit as i64 + 1], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(sql_error)?;
        let pairs = rows
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)?;
        let pairs = pairs
            .into_iter()
            .map(|(profile, project)| {
                let code_eligible = self.attention_pair_code_eligible(&profile, &project)?;
                Ok((profile, project, code_eligible))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(pairs)
    }

    pub fn attention_pair_code_eligible(&self, profile: &str, project: &str) -> Result<bool> {
        profile_code_eligible(&self.connection, profile, project)
    }

    /// Read at most twenty recent successful turns for one exact frozen
    /// profile/project pair and return one maximum native and Git quiet
    /// gap per usable turn. Each gap stays within one Agent generation and
    /// turn; missing lifecycle, source-gap, clock-regressed, and overlarge
    /// histories are excluded.
    pub fn attention_quiet_gaps(
        &self,
        profile: &str,
        project: &str,
        include_git: bool,
    ) -> Result<(Vec<i64>, Vec<i64>)> {
        let turns = successful_attention_turns(&self.connection, profile, project)?;
        let mut native_gaps = Vec::with_capacity(turns.len());
        let mut git_gaps = Vec::with_capacity(turns.len());
        for turn in turns {
            if let Some(gap) = attention_quiet_gap(&self.connection, &turn, false)? {
                native_gaps.push(gap);
            }
            if include_git && let Some(gap) = attention_quiet_gap(&self.connection, &turn, true)? {
                git_gaps.push(gap);
            }
        }
        Ok((native_gaps, git_gaps))
    }
}

fn profile_code_eligible(connection: &Connection, profile: &str, project: &str) -> Result<bool> {
    let tokens = profile
        .split(['-', '_', ' '])
        .filter(|part| !part.is_empty())
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    let named_read_only = tokens
        .iter()
        .any(|token| matches!(token.as_str(), "audit" | "readonly"))
        || tokens
            .windows(2)
            .any(|pair| pair[0] == "read" && pair[1] == "only");
    if named_read_only {
        return Ok(false);
    }
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM agents AS a
                 WHERE a.archived = 0 AND a.generation > 0
                   AND a.profile = ?1 AND a.project = ?2 AND a.workspace IS NOT NULL
                   AND json_extract(a.profile_snapshot, '$.permissions') IS NOT 'read-only'
                   AND json_extract(a.profile_snapshot, '$.permission_mode') IS NOT 'plan'
                   AND json_extract(a.profile_snapshot, '$.mode') IS NOT 'plan'
                   AND json_extract(a.profile_snapshot, '$.collaboration_mode') IS NOT 'plan'
                   AND json_extract(a.effective, '$.permissions') IS NOT 'read-only'
                   AND json_extract(a.effective, '$.permission_mode') IS NOT 'plan'
                   AND json_extract(a.effective, '$.mode') IS NOT 'plan'
                   AND json_extract(a.effective, '$.collaboration_mode') IS NOT 'plan'
                   AND lower(COALESCE(json_extract(a.profile_choice, '$.role'), '')) <> 'audit'
             )",
            params![profile, project],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

fn successful_attention_turns(
    connection: &Connection,
    profile: &str,
    project: &str,
) -> Result<Vec<SuccessfulAttentionTurn>> {
    // An Agent's newest successful turn is an upper bound on every eligible
    // result it can contribute. Visit Agents in that order and stop once an
    // unvisited Agent cannot displace the twentieth result, including ties.
    let mut frontiers = connection
        .prepare_cached(
            "SELECT a.id, (
                SELECT t.completed_at FROM turns AS t INDEXED BY attention_successful_turns
                WHERE t.agent = a.id AND t.state = 'completed' AND t.outcome = 'success'
                  AND t.started_at > 0 AND t.completed_at >= t.started_at
                ORDER BY t.completed_at DESC, t.turn_id DESC LIMIT 1
             ) AS newest
             FROM agents AS a INDEXED BY agents_list_project
             WHERE a.project = ?1 ORDER BY newest DESC, a.id DESC",
        )
        .map_err(sql_error)?;
    let mut agents = frontiers.query([project]).map_err(sql_error)?;
    let mut statement = connection
        .prepare_cached(
            "SELECT t.agent, t.generation, t.turn_id, t.started_at, t.completed_at,
                    started.seq, completed.seq,
                    json_extract(started.subject, '$.workspace')
             FROM turns AS t INDEXED BY attention_successful_turns
             CROSS JOIN events AS started INDEXED BY attention_turn_endpoints
               ON started.agent = t.agent AND started.generation = t.generation
              AND started.kind = 'turn.started'
              AND json_extract(started.data, '$.turn_id') = t.turn_id
             CROSS JOIN events AS completed INDEXED BY attention_turn_endpoints
               ON completed.agent = t.agent AND completed.generation = t.generation
              AND completed.kind = 'turn.completed'
              AND json_extract(completed.data, '$.turn_id') = t.turn_id
             WHERE t.agent = ?4 AND t.completed_at >= ?5
               AND t.state = 'completed' AND t.outcome = 'success'
               AND t.started_at > 0 AND t.completed_at >= t.started_at
               AND started.seq < completed.seq
               AND json_extract(started.subject, '$.profile') = ?1
               AND json_extract(completed.subject, '$.profile') = ?1
               AND json_extract(started.subject, '$.project') = json_extract(completed.subject, '$.project')
               AND (json_extract(started.subject, '$.project') = ?2
                    OR json_extract(started.subject, '$.project') = (
                        SELECT checkout FROM project_registrations WHERE id = ?2
                    ))
               AND NOT EXISTS (
                   SELECT 1 FROM events AS incident INDEXED BY attention_history_events
                   WHERE incident.agent = t.agent AND incident.generation = t.generation
                     AND incident.kind = 'x.user.agent.attention'
                     AND json_extract(incident.data, '$.data.phase') = 'incident'
                     AND json_extract(incident.data, '$.data.turn_id') = t.turn_id
               )
               AND NOT EXISTS (
                   SELECT 1 FROM turn_attention_steps AS step
                   WHERE step.agent = t.agent AND step.generation = t.generation
                     AND step.turn_id = t.turn_id
               )
               AND NOT EXISTS (
                   SELECT 1 FROM events AS dispatched INDEXED BY attention_history_events
                   JOIN commands AS c ON c.id = json_extract(dispatched.data, '$.command_id')
                   WHERE dispatched.agent = t.agent AND dispatched.generation = t.generation
                     AND dispatched.kind = 'command.dispatched' AND dispatched.seq > started.seq
                     AND dispatched.seq < completed.seq
                     AND c.target = t.agent AND c.generation = t.generation
                     AND COALESCE(c.expected_turn_id, c.turn_id) = t.turn_id
                     AND c.verb IN ('send', 'steer', 'inject', 'interrupt', 'approve', 'deny', 'answer')
                     AND c.state NOT IN ('rejected', 'cancelled')
               )
             ORDER BY t.completed_at DESC, t.turn_id DESC LIMIT ?3",
        )
        .map_err(sql_error)?;
    let mut newest: Vec<SuccessfulAttentionTurn> = Vec::new();
    while let Some(agent) = agents.next().map_err(sql_error)? {
        let id: i64 = agent.get(0).map_err(sql_error)?;
        let Some(at) = agent.get::<_, Option<i64>>(1).map_err(sql_error)? else {
            break;
        };
        let cutoff = newest.get(MAX_ATTENTION_HISTORY_TURNS - 1);
        if let Some(cutoff) = cutoff
            && (at, id) < (cutoff.completed_at, cutoff.agent)
        {
            break;
        }
        let rows = statement
            .query_map(
                params![
                    profile,
                    project,
                    MAX_ATTENTION_HISTORY_TURNS as i64,
                    id,
                    cutoff.map_or(0, |turn| turn.completed_at),
                ],
                |row| {
                    Ok(SuccessfulAttentionTurn {
                        agent: row.get(0)?,
                        generation: row.get(1)?,
                        turn_id: row.get(2)?,
                        started_at: row.get(3)?,
                        completed_at: row.get(4)?,
                        started_seq: row.get(5)?,
                        completed_seq: row.get(6)?,
                        workspace: row.get(7)?,
                    })
                },
            )
            .map_err(sql_error)?;
        for row in rows {
            newest.push(row.map_err(sql_error)?);
        }
        newest.sort_by(|left, right| {
            (right.completed_at, right.agent, &right.turn_id).cmp(&(
                left.completed_at,
                left.agent,
                &left.turn_id,
            ))
        });
        newest.truncate(MAX_ATTENTION_HISTORY_TURNS);
    }
    Ok(newest)
}

fn attention_quiet_gap(
    connection: &Connection,
    turn: &SuccessfulAttentionTurn,
    git: bool,
) -> Result<Option<i64>> {
    if git && !git_history_covered(connection, turn)? {
        return Ok(None);
    }
    if history_clock_gapped(connection, turn, git)? {
        return Ok(None);
    }
    let source_gap: bool = if git {
        connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM events INDEXED BY attention_history_events
                     WHERE agent = ?1 AND generation = ?2 AND kind = 'source.gap'
                       AND json_extract(data, '$.source') = 'git'
                       AND (ts BETWEEN ?3 AND ?4 OR (seq > ?5 AND seq < ?6))
                 )",
                params![
                    turn.agent,
                    turn.generation,
                    turn.started_at,
                    turn.completed_at,
                    turn.started_seq,
                    turn.completed_seq,
                ],
                |row| row.get(0),
            )
            .map_err(sql_error)?
    } else {
        connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM events AS gap INDEXED BY attention_history_events
                     WHERE gap.agent = ?1 AND gap.generation = ?2 AND gap.kind = 'source.gap'
                       AND (gap.ts BETWEEN ?3 AND ?4 OR (gap.seq > ?5 AND gap.seq < ?6))
                       AND EXISTS (
                           SELECT 1 FROM json_each(gap.data, '$.families') AS family
                           WHERE family.value IN ('turn', 'message', 'tool', 'request')
                       )
                 )",
                params![
                    turn.agent,
                    turn.generation,
                    turn.started_at,
                    turn.completed_at,
                    turn.started_seq,
                    turn.completed_seq,
                ],
                |row| row.get(0),
            )
            .map_err(sql_error)?
    };
    if source_gap {
        return Ok(None);
    }

    let sql = if git {
        "SELECT event.seq, event.ts, event.source FROM events AS event
         WHERE event.agent = ?1 AND event.generation = ?2 AND event.source = 'git'
           AND event.seq > ?3 AND event.seq < ?4
           AND event.ts BETWEEN ?5 AND ?6
           AND (
               (event.kind = 'git.committed' AND NOT EXISTS (
                   SELECT 1 FROM turns AS other
                   WHERE other.agent = event.agent AND other.generation = event.generation
                     AND other.turn_id <> ?7 AND other.started_at IS NOT NULL
                     AND other.completed_at IS NOT NULL
                     AND event.ts BETWEEN other.started_at AND other.completed_at
               ))
               OR (event.kind = 'file.changed' AND EXISTS (
                   SELECT 1 FROM events AS cause
                   WHERE cause.seq = json_extract(event.data, '$.cause_seq')
                     AND cause.agent = event.agent AND cause.generation = event.generation
                     AND json_extract(cause.data, '$.turn_id') = ?7
                     AND cause.seq > ?3 AND cause.seq < ?4
               ))
           )
         ORDER BY event.seq LIMIT ?8"
    } else {
        "SELECT event.seq, event.ts, event.source FROM events AS event
         WHERE event.agent = ?1 AND event.generation = ?2 AND event.native_id IS NOT NULL
           AND event.seq > ?3 AND event.seq < ?4
           AND event.ts BETWEEN ?5 AND ?6
           AND event.source NOT IN ('daemon', 'os', 'git', 'workspace_process')
           AND event.kind IN ('turn.started', 'turn.completed', 'message.completed',
                              'tool.started', 'tool.completed', 'request.opened',
                              'request.resolved', 'request.closed')
           AND json_extract(event.data, '$.turn_id') = ?7
         ORDER BY event.seq LIMIT ?8"
    };
    let mut statement = connection.prepare(sql).map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![
                turn.agent,
                turn.generation,
                turn.started_seq,
                turn.completed_seq,
                turn.started_at,
                turn.completed_at,
                turn.turn_id,
                MAX_ATTENTION_ACTIVITY_EVENTS_PER_TURN as i64 + 1,
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .map_err(sql_error)?;
    let events = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    if events.len() > MAX_ATTENTION_ACTIVITY_EVENTS_PER_TURN {
        return Ok(None);
    }

    let mut last_by_source = std::collections::HashMap::<String, i64>::new();
    let mut points = Vec::with_capacity(events.len() + 2);
    points.push(turn.started_at);
    for (_, at, source) in events {
        if !(turn.started_at..=turn.completed_at).contains(&at)
            || last_by_source.get(&source).is_some_and(|last| at < *last)
        {
            return Ok(None);
        }
        last_by_source.insert(source, at);
        points.push(at);
    }
    points.push(turn.completed_at);
    points.sort_unstable();
    Ok(points.windows(2).map(|pair| pair[1] - pair[0]).max())
}

fn git_history_covered(connection: &Connection, turn: &SuccessfulAttentionTurn) -> Result<bool> {
    let Some(workspace) = turn.workspace.as_deref() else {
        return Ok(false);
    };
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM source_state AS source
                 JOIN git_work_state AS work ON work.workspace = ?3
                 WHERE source.agent = ?1 AND source.generation = ?2 AND source.source = 'git'
                   AND source.state = 'open' AND source.health IN ('ok', 'slow')
                   AND source.last_store_seq IS NOT NULL
                   AND (work.work_available = 1 OR
                        (work.work_error = 'no_base_oid' AND work.dirty_digest IS NOT NULL))
             )",
            params![turn.agent, turn.generation, workspace],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

fn history_clock_gapped(
    connection: &Connection,
    turn: &SuccessfulAttentionTurn,
    git: bool,
) -> Result<bool> {
    let sql = if git {
        "SELECT EXISTS(
             SELECT 1 FROM events AS event
             WHERE event.agent = ?1 AND event.generation = ?2 AND event.source = 'git'
               AND event.seq > ?3 AND event.seq < ?4
               AND event.ts NOT BETWEEN ?5 AND ?6
               AND (
                   event.kind = 'git.committed'
                   OR (event.kind = 'file.changed' AND EXISTS (
                       SELECT 1 FROM events AS cause
                       WHERE cause.seq = json_extract(event.data, '$.cause_seq')
                         AND cause.agent = event.agent AND cause.generation = event.generation
                         AND cause.seq > ?3 AND cause.seq < ?4
                         AND json_extract(cause.data, '$.turn_id') = ?7
                   ))
               )
         )"
    } else {
        "SELECT EXISTS(
             SELECT 1 FROM events AS event
             WHERE event.agent = ?1 AND event.generation = ?2 AND event.native_id IS NOT NULL
               AND event.seq > ?3 AND event.seq < ?4
               AND event.ts NOT BETWEEN ?5 AND ?6
               AND event.source NOT IN ('daemon', 'os', 'git', 'workspace_process')
               AND event.kind IN ('turn.started', 'turn.completed', 'message.completed',
                                  'tool.started', 'tool.completed', 'request.opened',
                                  'request.resolved', 'request.closed')
               AND json_extract(event.data, '$.turn_id') = ?7
         )"
    };
    connection
        .query_row(
            sql,
            params![
                turn.agent,
                turn.generation,
                turn.started_seq,
                turn.completed_seq,
                turn.started_at,
                turn.completed_at,
                turn.turn_id,
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)
}
