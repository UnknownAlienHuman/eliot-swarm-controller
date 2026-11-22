//! Durable persistence for automatic background-process wakes (#879, #881, #883).

use super::{
    Change, Error, Result, Store, admission, commit_transaction, context, format_agent_id,
    load_agent_by_num, load_turn, parse_agent_id, process_record_from_row, sql_error,
    validate_limit, wakes::unique_wake_command_id,
};
use eliot_core::{
    BACKGROUND_SERVER_TOPIC, BACKGROUND_WAKE_PRINCIPAL, InjectPoint, ProcessIdentity,
    ProcessRecord, ProcessState, SubmitCommand, TurnOutcome, TurnState,
    api::AgentBackgroundWakeView,
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde_json::json;
use std::collections::{HashMap, HashSet};

const MAX_ACTIVE_BACKGROUND_WAKES: i64 = 1_024;
pub const MAX_BACKGROUND_WAKE_PAGE: usize = 128;
const BACKGROUND_WAKE_RETENTION_MS: i64 = 31 * 24 * 60 * 60 * 1_000;
const MAX_PRE_ADMISSION_ATTEMPTS: u32 = 5;
const MAX_DECLARATION_SCAN_ROWS: i64 = 128;
const MAX_DECLARED_COMMAND_BYTES: i64 = 4_096;
const MAX_TOOL_WINDOWS: i64 = 128;
const MAX_TAIL_SCAN_CHARS: i64 = 4_096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackgroundWakeArmInput {
    pub agent: String,
    pub generation: i64,
    pub turn_id: String,
    pub tool_item_id: Option<String>,
    pub process: ProcessIdentity,
    pub command_line: String,
    pub command_truncated: bool,
    /// Redacted, bounded tail captured at arm time, if any.
    pub output_tail: Option<String>,
    pub expires_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackgroundWakeRecord {
    pub id: String,
    pub agent: String,
    pub generation: i64,
    pub turn_id: String,
    pub process: ProcessIdentity,
    pub command_line: String,
    pub command_truncated: bool,
    pub state: String,
    pub outcome_kind: Option<String>,
    pub exit_code: Option<i32>,
    pub output_tail: Option<String>,
    pub note_text: Option<String>,
    pub delivery_command_id: String,
    pub delivery_verb: Option<String>,
    pub delivery_attempts: u32,
    pub delivery_next_at: i64,
    pub created_at: i64,
    pub expires_at: i64,
}

/// One native tool execution window (`tool.started` to `tool.completed`, or
/// turn completion if still open) committed for the completing turn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackgroundWakeToolWindow {
    pub turn_id: String,
    pub item_id: String,
    pub started_at: i64,
    pub completed_at: i64,
}

/// Everything the Router needs to pick a candidate, read in one short
/// Store call. Only bounded fields are selected; no event payload is decoded.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackgroundWakeArmContext {
    /// Earliest process start (Unix ms): this completing turn's start.
    pub window_start: i64,
    /// Latest process start (Unix ms): this completing turn's completion.
    pub window_end: i64,
    pub declared_pids: HashSet<u32>,
    pub declared_commands: Vec<String>,
    /// Committed native tool call windows belonging strictly to this completing turn.
    pub tool_windows: Vec<BackgroundWakeToolWindow>,
    /// Identities that already had an automatic wake in this generation.
    pub woken: HashSet<ProcessIdentity>,
}

impl Store {
    /// Arm context for a committed `turn.completed`, or `None` when the turn is
    /// ineligible (see `background_wake_arm_eligible_tx`).
    pub fn background_wake_arm_context(
        &self,
        agent: &str,
        generation: i64,
        turn_id: &str,
    ) -> Result<Option<BackgroundWakeArmContext>> {
        let agent_num = parse_agent_id(agent)?;
        if !background_wake_arm_eligible_tx(&self.connection, agent_num, generation, turn_id)? {
            return Ok(None);
        }
        let window: Option<(Option<i64>, Option<i64>)> = self
            .connection
            .query_row(
                "SELECT started_at, completed_at
                 FROM turns WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3",
                params![agent_num, generation, turn_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        let Some((Some(window_start), Some(window_end))) = window else {
            return Ok(None);
        };
        let mut context = BackgroundWakeArmContext {
            window_start,
            window_end,
            ..Default::default()
        };

        let mut declared = self
            .connection
            .prepare(
                "SELECT json_extract(e.data, '$.data.pid'),
                        substr(json_extract(e.data, '$.data.command'), 1, ?3)
                 FROM events AS e INDEXED BY events_kind
                 JOIN commands AS c ON c.id = json_extract(e.cause, '$.command_id')
                 WHERE e.kind = ?1 AND c.principal = ?2
                 ORDER BY e.seq DESC LIMIT ?4",
            )
            .map_err(sql_error)?;
        let rows = declared
            .query_map(
                params![
                    format!("x.user.{BACKGROUND_SERVER_TOPIC}"),
                    format!("agent:{agent}:{generation}"),
                    MAX_DECLARED_COMMAND_BYTES,
                    MAX_DECLARATION_SCAN_ROWS,
                ],
                |row| {
                    Ok((
                        row.get::<_, Option<i64>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                    ))
                },
            )
            .map_err(sql_error)?;
        for row in rows {
            let (pid, command) = row.map_err(sql_error)?;
            if let Some(pid) = pid
                .and_then(|pid| u32::try_from(pid).ok())
                .filter(|pid| *pid > 0)
            {
                context.declared_pids.insert(pid);
            }
            if let Some(command) = command.map(|command| command.trim().to_owned())
                && !command.is_empty()
            {
                context.declared_commands.push(command);
            }
        }

        // Collect native `tool.started` -> `tool.completed` windows belonging strictly
        // to this completing turn; any process started outside every tool window of
        // this turn (including post-turn notify hooks) is excluded.
        let mut completed_at_by_item: HashMap<String, i64> = HashMap::new();
        let mut completed_tools = self
            .connection
            .prepare(
                "SELECT json_extract(data, '$.item_id'), ts
                 FROM events INDEXED BY events_tool_turn
                 WHERE agent = ?1 AND generation = ?2
                   AND kind = 'tool.completed'
                   AND json_extract(data, '$.turn_id') = ?3
                   AND source = 'stream'
                   AND json_type(data, '$.item_id') = 'text'
                   AND length(trim(json_extract(data, '$.item_id'))) > 0
                 ORDER BY seq ASC LIMIT ?4",
            )
            .map_err(sql_error)?;
        let rows = completed_tools
            .query_map(
                params![agent_num, generation, turn_id, MAX_TOOL_WINDOWS],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .map_err(sql_error)?;
        for row in rows {
            let (item_id, ts) = row.map_err(sql_error)?;
            completed_at_by_item
                .entry(item_id)
                .and_modify(|prev| *prev = (*prev).max(ts))
                .or_insert(ts);
        }

        let mut started_tools = self
            .connection
            .prepare(
                "SELECT json_extract(data, '$.turn_id'), json_extract(data, '$.item_id'), ts
                 FROM events INDEXED BY events_tool_turn
                 WHERE agent = ?1 AND generation = ?2
                   AND kind = 'tool.started'
                   AND json_extract(data, '$.turn_id') = ?3
                   AND source = 'stream'
                   AND json_type(data, '$.item_id') = 'text'
                   AND length(trim(json_extract(data, '$.item_id'))) > 0
                 ORDER BY seq ASC LIMIT ?4",
            )
            .map_err(sql_error)?;
        let rows = started_tools
            .query_map(
                params![agent_num, generation, turn_id, MAX_TOOL_WINDOWS],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .map_err(sql_error)?;
        for row in rows {
            let (tool_turn, item_id, started_at) = row.map_err(sql_error)?;
            let completed_at = completed_at_by_item
                .get(&item_id)
                .copied()
                .unwrap_or(window_end)
                .max(started_at);
            context.tool_windows.push(BackgroundWakeToolWindow {
                turn_id: tool_turn,
                item_id,
                started_at,
                completed_at,
            });
        }
        if context.tool_windows.is_empty() {
            return Ok(None);
        }

        let mut woken = self
            .connection
            .prepare(
                "SELECT host_boot_id, pid, start_time FROM background_process_wakes
                 WHERE agent = ?1 AND generation = ?2 LIMIT ?3",
            )
            .map_err(sql_error)?;
        let rows = woken
            .query_map(
                params![agent_num, generation, MAX_DECLARATION_SCAN_ROWS],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .map_err(sql_error)?;
        for row in rows {
            let (host_boot_id, pid, start_time) = row.map_err(sql_error)?;
            if let (Ok(pid), Ok(start_time)) = (u32::try_from(pid), u64::try_from(start_time)) {
                context.woken.insert(ProcessIdentity {
                    host_boot_id,
                    pid,
                    start_time,
                });
            }
        }
        Ok(Some(context))
    }

    /// Bounded output tail that the native `tool.completed` for `item_id` in
    /// `turn_id` captured, if the adapter recorded one.
    pub fn background_wake_tool_output_tail(
        &self,
        agent: &str,
        generation: i64,
        turn_id: &str,
        item_id: &str,
    ) -> Result<Option<String>> {
        let agent_num = parse_agent_id(agent)?;
        let tail: Option<Option<String>> = self
            .connection
            .query_row(
                "SELECT substr(COALESCE(
                            json_extract(data, '$.output_tail'),
                            json_extract(data, '$.error_tail'),
                            json_extract(data, '$.stderr_tail'),
                            json_extract(data, '$.stdout_tail'),
                            json_extract(data, '$.native_error.message')), -?5)
                 FROM events INDEXED BY events_tool_turn
                 WHERE agent = ?1 AND generation = ?2
                   AND kind = 'tool.completed'
                   AND json_extract(data, '$.turn_id') = ?3
                   AND source = 'stream'
                   AND json_extract(data, '$.item_id') = ?4
                 LIMIT 1",
                params![agent_num, generation, turn_id, item_id, MAX_TAIL_SCAN_CHARS,],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        Ok(tail
            .flatten()
            .map(|tail| tail.trim().to_owned())
            .filter(|tail| !tail.is_empty()))
    }

    /// Arm at most one automatic wake for `(agent, generation)` if all eligibility
    /// gates still hold and the tracked process is running under agent custody.
    pub fn background_wake_arm(
        &mut self,
        input: &BackgroundWakeArmInput,
        now: i64,
    ) -> Result<Option<BackgroundWakeRecord>> {
        if input.generation <= 0
            || input.turn_id.trim().is_empty()
            || input
                .tool_item_id
                .as_deref()
                .is_some_and(|id| id.trim().is_empty())
            || input.process.host_boot_id.trim().is_empty()
            || input.process.pid == 0
            || input.process.start_time == 0
            || input.command_line.trim().is_empty()
            || input.expires_at < now
        {
            return Err(Error::new(
                "invalid",
                "background wake arm input is invalid",
            ));
        }
        self.ensure_writer_healthy()?;
        let outcome = self.background_wake_arm_inner(input, now);
        self.fault_on_persistence_error(&outcome);
        outcome
    }

    fn background_wake_arm_inner(
        &mut self,
        input: &BackgroundWakeArmInput,
        now: i64,
    ) -> Result<Option<BackgroundWakeRecord>> {
        let agent_num = parse_agent_id(&input.agent)?;
        let start_time = i64::try_from(input.process.start_time)
            .map_err(|_| Error::new("invalid", "process start time overflows Store integer"))?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if !background_wake_arm_eligible_tx(&tx, agent_num, input.generation, &input.turn_id)? {
            return Ok(None);
        }
        if let Some(tool_item_id) = input.tool_item_id.as_deref()
            && !has_turn_tool_tx(
                &tx,
                agent_num,
                input.generation,
                &input.turn_id,
                Some(tool_item_id),
            )?
        {
            return Ok(None);
        }
        let owner = format!("agent:{}#{}", input.agent, input.generation);
        let proc_row: Option<ProcessRecord> = tx
            .query_row(
                "SELECT host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at
                 FROM processes
                 WHERE host_boot_id = ?1 AND pid = ?2 AND start_time = ?3",
                params![input.process.host_boot_id, input.process.pid, start_time],
                process_record_from_row,
            )
            .optional()
            .map_err(sql_error)?;
        let Some(proc_row) = proc_row else {
            return Ok(None);
        };
        if proc_row.owner != owner
            || proc_row.role != "child"
            || proc_row.state != ProcessState::Running
        {
            return Ok(None);
        }
        let already_woken: bool = tx
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM background_process_wakes
                     WHERE agent = ?1 AND generation = ?2
                       AND host_boot_id = ?3 AND pid = ?4 AND start_time = ?5
                 )",
                params![
                    agent_num,
                    input.generation,
                    input.process.host_boot_id,
                    input.process.pid,
                    start_time
                ],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if already_woken {
            return Ok(None);
        }
        let active: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM background_process_wakes
                 WHERE state IN ('waiting', 'delivering', 'delivery_pending')",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if active >= MAX_ACTIVE_BACKGROUND_WAKES {
            return Err(Error::new(
                "capacity",
                "active automatic background wake limit has been reached",
            ));
        }
        let id = unique_wake_command_id(&tx, "")?;
        let delivery_command_id = unique_wake_command_id(&tx, &id)?;
        tx.execute(
            "INSERT INTO background_process_wakes (
                 id, agent, generation, turn_id, host_boot_id, pid, start_time,
                 command_line, command_truncated, output_tail, state, delivery_command_id,
                 created_at, expires_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'waiting', ?11, ?12, ?13)",
            params![
                id,
                agent_num,
                input.generation,
                input.turn_id,
                input.process.host_boot_id,
                input.process.pid,
                start_time,
                input.command_line,
                input.command_truncated,
                input.output_tail,
                delivery_command_id,
                now,
                input.expires_at,
            ],
        )
        .map_err(sql_error)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Some(BackgroundWakeRecord {
            id,
            agent: input.agent.clone(),
            generation: input.generation,
            turn_id: input.turn_id.clone(),
            process: input.process.clone(),
            command_line: input.command_line.clone(),
            command_truncated: input.command_truncated,
            state: "waiting".to_owned(),
            outcome_kind: None,
            exit_code: None,
            output_tail: input.output_tail.clone(),
            note_text: None,
            delivery_command_id,
            delivery_verb: None,
            delivery_attempts: 0,
            delivery_next_at: 0,
            created_at: now,
            expires_at: input.expires_at,
        }))
    }

    /// Pending automatic wake for `agent.get` (`swarm show`).
    pub fn pending_background_wake(
        &self,
        agent: &str,
        generation: i64,
    ) -> Result<Option<AgentBackgroundWakeView>> {
        if generation <= 0 {
            return Ok(None);
        }
        let agent_num = parse_agent_id(agent)?;
        let row = self
            .connection
            .query_row(
                "SELECT id, agent, generation, turn_id, host_boot_id, pid, start_time,
                        command_line, command_truncated, state, outcome_kind, exit_code,
                        output_tail, note_text, delivery_command_id, delivery_verb,
                        delivery_attempts, delivery_next_at, created_at, expires_at
                 FROM background_process_wakes
                 WHERE agent = ?1 AND generation = ?2
                   AND state IN ('waiting', 'delivering', 'delivery_pending')
                 LIMIT 1",
                params![agent_num, generation],
                decode_record_row,
            )
            .optional()
            .map_err(sql_error)?;
        row.map(|record| {
            let record = record?;
            let view = AgentBackgroundWakeView {
                id: record.id,
                turn_id: record.turn_id,
                process: record.process,
                command_line: record.command_line,
                command_truncated: record.command_truncated,
                state: record.state,
                armed_at: record.created_at,
                expires_at: record.expires_at,
            };
            view.validate()?;
            Ok(view)
        })
        .transpose()
    }

    pub fn background_wake_page(
        &self,
        after: Option<(i64, String)>,
        limit: usize,
    ) -> Result<Vec<BackgroundWakeRecord>> {
        if limit == 0 || limit > MAX_BACKGROUND_WAKE_PAGE {
            return Err(Error::new(
                "invalid",
                "background wake page limit is out of range",
            ));
        }
        let mut stmt = self
            .connection
            .prepare(match after {
                None => {
                    "SELECT id, agent, generation, turn_id, host_boot_id, pid, start_time,
                            command_line, command_truncated, state, outcome_kind, exit_code,
                            output_tail, note_text, delivery_command_id, delivery_verb,
                            delivery_attempts, delivery_next_at, created_at, expires_at
                     FROM background_process_wakes
                     WHERE state IN ('waiting', 'delivering', 'delivery_pending')
                     ORDER BY created_at, id LIMIT ?1"
                }
                Some(_) => {
                    "SELECT id, agent, generation, turn_id, host_boot_id, pid, start_time,
                            command_line, command_truncated, state, outcome_kind, exit_code,
                            output_tail, note_text, delivery_command_id, delivery_verb,
                            delivery_attempts, delivery_next_at, created_at, expires_at
                     FROM background_process_wakes
                     WHERE state IN ('waiting', 'delivering', 'delivery_pending')
                       AND (created_at, id) > (?1, ?2)
                     ORDER BY created_at, id LIMIT ?3"
                }
            })
            .map_err(sql_error)?;
        let rows = match after {
            None => stmt
                .query_map([limit as i64], decode_record_row)
                .map_err(sql_error)?,
            Some((created_at, id)) => stmt
                .query_map(params![created_at, id, limit as i64], decode_record_row)
                .map_err(sql_error)?,
        };
        let mut records = Vec::new();
        for row in rows {
            records.push(row.map_err(sql_error)??);
        }
        Ok(records)
    }

    pub fn background_wake_begin_delivery(
        &mut self,
        id: &str,
        generation: i64,
        outcome_kind: &str,
        exit_code: Option<i32>,
        output_tail: Option<&str>,
        note_text: &str,
    ) -> Result<bool> {
        if !matches!(outcome_kind, "exited" | "expired")
            || note_text.trim().is_empty()
            || note_text.contains('\0')
        {
            return Err(Error::new("invalid", "background wake note is invalid"));
        }
        self.ensure_writer_healthy()?;
        let result = self
            .connection
            .execute(
                "UPDATE background_process_wakes
                 SET state = 'delivering',
                     outcome_kind = ?1,
                     exit_code = ?2,
                     output_tail = ?3,
                     note_text = ?4
                 WHERE id = ?5 AND generation = ?6 AND state = 'waiting'",
                params![
                    outcome_kind,
                    exit_code,
                    output_tail,
                    note_text,
                    id,
                    generation
                ],
            )
            .map_err(sql_error);
        self.fault_on_persistence_error(&result);
        result.map(|changed| changed == 1)
    }

    pub fn background_wake_mark_delivery_pending(
        &mut self,
        id: &str,
        generation: i64,
        command_id: &str,
        verb: &str,
    ) -> Result<bool> {
        if !matches!(verb, "send" | "inject") {
            return Err(Error::new("invalid", "background wake verb is invalid"));
        }
        self.ensure_writer_healthy()?;
        let result = self
            .connection
            .execute(
                "UPDATE background_process_wakes
                 SET state = 'delivery_pending', delivery_verb = ?1
                 WHERE id = ?2 AND generation = ?3 AND delivery_command_id = ?4
                   AND state IN ('delivering', 'delivery_pending')",
                params![verb, id, generation, command_id],
            )
            .map_err(sql_error);
        self.fault_on_persistence_error(&result);
        result.map(|changed| changed == 1)
    }

    pub fn background_wake_delivery_pre_admission_failure(
        &mut self,
        id: &str,
        generation: i64,
        now: i64,
    ) -> Result<bool> {
        self.ensure_writer_healthy()?;
        let outcome = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let attempts: Option<i64> = tx
                .query_row(
                    "SELECT delivery_attempts FROM background_process_wakes
                     WHERE id = ?1 AND generation = ?2 AND state = 'delivering'",
                    params![id, generation],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?;
            let Some(attempts) = attempts else {
                return Ok(false);
            };
            let attempts = u32::try_from(attempts)
                .map_err(|_| Error::new("source_integrity", "wake attempt count is invalid"))?;
            let next_attempt = attempts.saturating_add(1);
            let changed = if next_attempt >= MAX_PRE_ADMISSION_ATTEMPTS {
                tx.execute(
                    "UPDATE background_process_wakes
                     SET delivery_attempts = ?1, state = 'failed', settled_at = ?2
                     WHERE id = ?3 AND generation = ?4 AND state = 'delivering'",
                    params![next_attempt, now, id, generation],
                )
                .map_err(sql_error)?
            } else {
                let delay_ms = 500_i64.saturating_mul(1_i64 << attempts.min(6)).min(30_000);
                tx.execute(
                    "UPDATE background_process_wakes
                     SET delivery_attempts = ?1, delivery_next_at = ?2
                     WHERE id = ?3 AND generation = ?4 AND state = 'delivering'",
                    params![next_attempt, now.saturating_add(delay_ms), id, generation],
                )
                .map_err(sql_error)?
            };
            commit_transaction(tx, &self.write_faulted)?;
            Ok(changed == 1)
        })();
        self.fault_on_persistence_error(&outcome);
        outcome
    }

    pub fn background_wake_finish(
        &mut self,
        id: &str,
        generation: i64,
        command_id: Option<&str>,
        final_state: &str,
        now: i64,
    ) -> Result<bool> {
        if !matches!(
            final_state,
            "delivered" | "expired" | "stale" | "cancelled" | "failed" | "unknown"
        ) {
            return Err(Error::new(
                "invalid",
                "background wake terminal state is invalid",
            ));
        }
        self.ensure_writer_healthy()?;
        let result = self
            .connection
            .execute(
                "UPDATE background_process_wakes
                 SET state = ?1, settled_at = ?2
                 WHERE id = ?3 AND generation = ?4
                   AND (?5 IS NULL OR delivery_command_id = ?5)
                   AND state IN ('waiting', 'delivering', 'delivery_pending')",
                params![final_state, now, id, generation, command_id],
            )
            .map_err(sql_error);
        self.fault_on_persistence_error(&result);
        result.map(|changed| changed == 1)
    }

    /// Admit an automatic-wake `inject` directly into an active turn (`TurnSafePoint`
    /// or `ActiveTurn`). Refuses `bad_state` if the Agent became idle or changed turns,
    /// so the caller falls back cleanly to `send`.
    pub fn background_wake_admit_inject(
        &mut self,
        wake: &BackgroundWakeRecord,
        active_turn: &str,
        text: &str,
        now: i64,
    ) -> Result<Change<Option<String>>> {
        self.ensure_writer_healthy()?;
        let agent_num = parse_agent_id(&wake.agent)?;
        let outcome = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let agent = load_agent_by_num(&tx, agent_num)?
                .ok_or_else(|| Error::new("bad_state", "wake target Agent is absent"))?;
            if agent.generation != wake.generation
                || agent.is_terminal()
                || agent.observe_only
                || !matches!(
                    agent.state,
                    eliot_core::AgentState::Busy | eliot_core::AgentState::Waiting
                )
                || agent.active_turn.as_deref() != Some(active_turn)
            {
                return Err(Error::new(
                    "bad_state",
                    "Agent is no longer in the active turn for mid-turn wake inject",
                ));
            }
            let points = context::context_points(&agent)?;
            if !points
                .iter()
                .any(|point| matches!(point, InjectPoint::TurnSafePoint | InjectPoint::ActiveTurn))
            {
                return Err(Error::new(
                    "bad_state",
                    "Agent does not support mid-turn context injection",
                ));
            }
            let mut request = SubmitCommand::new(wake.delivery_command_id.clone(), "inject");
            request.target = Some(wake.agent.clone());
            request.expected_generation = Some(wake.generation);
            request.expected_turn_id = Some(active_turn.to_owned());
            request.args = json!(eliot_core::InjectArgs {
                text: text.to_owned(),
                point: InjectPoint::Soonest,
                expires_at_ms: None,
            });
            let digest = admission::command_payload_digest(&request)?;
            let accepted =
                context::admit_tx(&tx, BACKGROUND_WAKE_PRINCIPAL, &request, &digest, now, None)?;
            if accepted.data["application_point"] == "next_input" {
                return Err(Error::new(
                    "bad_state",
                    "wake inject resolved to next_input instead of active turn",
                ));
            }
            let active_turn_cmd = (accepted.data["application_point"] == "active_turn")
                .then(|| wake.delivery_command_id.clone());
            tx.execute(
                "UPDATE background_process_wakes
                 SET state = 'delivery_pending', delivery_verb = 'inject'
                 WHERE id = ?1 AND generation = ?2 AND state IN ('delivering', 'delivery_pending')",
                params![wake.id, wake.generation],
            )
            .map_err(sql_error)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: active_turn_cmd,
                events: vec![accepted],
            })
        })();
        self.fault_on_persistence_error(&outcome);
        outcome
    }

    pub fn background_wake_prune(&mut self, now: i64, limit: usize) -> Result<usize> {
        validate_limit(limit)?;
        let cutoff = now.saturating_sub(BACKGROUND_WAKE_RETENTION_MS);
        self.ensure_writer_healthy()?;
        let result = self
            .connection
            .execute(
                "DELETE FROM background_process_wakes WHERE id IN (
                     SELECT id FROM background_process_wakes
                     WHERE state IN ('delivered', 'expired', 'stale', 'cancelled', 'failed', 'unknown')
                       AND settled_at < ?1
                     ORDER BY settled_at, id LIMIT ?2
                 )",
                params![cutoff, limit as i64],
            )
            .map_err(sql_error);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn agent_has_active_stop(&self, agent: &str, generation: i64) -> Result<bool> {
        let agent_num = parse_agent_id(agent)?;
        agent_has_active_stop_tx(&self.connection, agent_num, generation)
    }
}

fn has_turn_tool_tx(
    connection: &rusqlite::Connection,
    agent_num: i64,
    generation: i64,
    turn_id: &str,
    item_id: Option<&str>,
) -> Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM events INDEXED BY events_tool_turn
                 WHERE agent = ?1 AND generation = ?2
                   AND kind = 'tool.started'
                   AND json_extract(data, '$.turn_id') = ?3
                   AND source = 'stream'
                   AND json_type(data, '$.item_id') = 'text'
                   AND length(trim(json_extract(data, '$.item_id'))) > 0
                   AND (?4 IS NULL OR json_extract(data, '$.item_id') = ?4)
             )",
            params![agent_num, generation, turn_id, item_id],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

/// Whether the completing turn is eligible for automatic background-process wake arming.
fn background_wake_arm_eligible_tx(
    connection: &rusqlite::Connection,
    agent_num: i64,
    generation: i64,
    turn_id: &str,
) -> Result<bool> {
    if generation <= 0 || turn_id.trim().is_empty() {
        return Ok(false);
    }
    let Some(agent) = load_agent_by_num(connection, agent_num)? else {
        return Ok(false);
    };
    if agent.generation != generation
        || agent.is_terminal()
        || agent.observe_only
        || agent
            .active_turn
            .as_deref()
            .is_some_and(|active| active != turn_id)
    {
        return Ok(false);
    }
    let Some(turn) = load_turn(connection, agent_num, generation, turn_id)? else {
        return Ok(false);
    };
    let newer_turn: bool = connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM turns
                 WHERE agent = ?1 AND generation = ?2 AND turn_id <> ?3
                   AND started_at > ?4
             )",
            params![agent_num, generation, turn_id, turn.started_at.unwrap_or(0)],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if newer_turn {
        return Ok(false);
    }
    if turn.state != TurnState::Completed
        || !matches!(
            turn.outcome,
            Some(TurnOutcome::Success | TurnOutcome::Failure)
        )
    {
        return Ok(false);
    }
    if agent_has_active_stop_tx(connection, agent_num, generation)? {
        return Ok(false);
    }
    let interrupted: bool = connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM commands INDEXED BY commands_target
                 WHERE target = ?1 AND generation = ?2 AND verb = 'interrupt'
                   AND state IN ('accepted', 'queued', 'dispatched', 'applied', 'unknown')
                   AND (turn_id = ?3 OR expected_turn_id = ?3
                        OR (?4 IS NOT NULL AND created_at >= ?4))
             )",
            params![agent_num, generation, turn_id, turn.started_at],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if interrupted {
        return Ok(false);
    }
    let manual_wake_open: bool = connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM agent_wakes
                 WHERE agent = ?1 AND generation = ?2
                   AND (state IN ('waiting', 'satisfied', 'delivery_pending')
                        OR (turn_id = ?3 AND state <> 'stale'))
             )",
            params![agent_num, generation, turn_id],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if manual_wake_open {
        return Ok(false);
    }
    let auto_wake_open: bool = connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM background_process_wakes
                 WHERE agent = ?1 AND generation = ?2
                   AND (state IN ('waiting', 'delivering', 'delivery_pending') OR turn_id = ?3)
             )",
            params![agent_num, generation, turn_id],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if auto_wake_open {
        return Ok(false);
    }
    // Requiring at least one committed native `tool.started` item belonging to
    // this completing turn enforces both tool attribution (#903 item 1) and the
    // wake loop guard (#903 item 3: any turn started by a wake that ran no
    // native tool arms no new wake).
    has_turn_tool_tx(connection, agent_num, generation, turn_id, None)
}

fn agent_has_active_stop_tx(
    connection: &rusqlite::Connection,
    agent_num: i64,
    generation: i64,
) -> Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM commands INDEXED BY commands_target
                 WHERE target = ?1 AND generation = ?2 AND verb = 'stop'
                   AND state IN ('accepted', 'queued', 'dispatched', 'applied', 'unknown')
             )",
            params![agent_num, generation],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

fn decode_record_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Result<BackgroundWakeRecord>> {
    let id: String = row.get(0)?;
    let agent: i64 = row.get(1)?;
    let generation: i64 = row.get(2)?;
    let turn_id: String = row.get(3)?;
    let host_boot_id: String = row.get(4)?;
    let pid_raw: i64 = row.get(5)?;
    let start_raw: i64 = row.get(6)?;
    let command_line: String = row.get(7)?;
    let command_truncated: bool = row.get(8)?;
    let state: String = row.get(9)?;
    let outcome_kind: Option<String> = row.get(10)?;
    let exit_code: Option<i32> = row.get(11)?;
    let output_tail: Option<String> = row.get(12)?;
    let note_text: Option<String> = row.get(13)?;
    let delivery_command_id: String = row.get(14)?;
    let delivery_verb: Option<String> = row.get(15)?;
    let delivery_attempts_raw: i64 = row.get(16)?;
    let delivery_next_at: i64 = row.get(17)?;
    let created_at: i64 = row.get(18)?;
    let expires_at: i64 = row.get(19)?;
    Ok((|| {
        let pid = u32::try_from(pid_raw)
            .map_err(|_| Error::new("source_integrity", "stored wake pid is invalid"))?;
        let start_time = u64::try_from(start_raw)
            .map_err(|_| Error::new("source_integrity", "stored wake start_time is invalid"))?;
        let delivery_attempts = u32::try_from(delivery_attempts_raw).map_err(|_| {
            Error::new(
                "source_integrity",
                "stored wake delivery attempts are invalid",
            )
        })?;
        Ok(BackgroundWakeRecord {
            id,
            agent: format_agent_id(agent),
            generation,
            turn_id,
            process: ProcessIdentity {
                host_boot_id,
                pid,
                start_time,
            },
            command_line,
            command_truncated,
            state,
            outcome_kind,
            exit_code,
            output_tail,
            note_text,
            delivery_command_id,
            delivery_verb,
            delivery_attempts,
            delivery_next_at,
            created_at,
            expires_at,
        })
    })())
}
