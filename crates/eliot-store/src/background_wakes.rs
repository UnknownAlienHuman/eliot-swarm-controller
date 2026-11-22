//! Durable persistence for automatic background-process wakes (#879, #881, #883).

use super::{
    Change, Error, Result, Store, admission, commit_transaction, context, decode_json,
    format_agent_id, load_agent_by_num, load_turn, parse_agent_id, process_record_from_row,
    sql_error, validate_limit, wakes::unique_wake_command_id,
};
use eliot_core::{
    BACKGROUND_SERVER_TOPIC, BACKGROUND_WAKE_PRINCIPAL, InjectPoint, ProcessIdentity,
    ProcessRecord, ProcessState, SubmitCommand, TurnOutcome, TurnState,
    api::AgentBackgroundWakeView,
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};
use std::collections::HashSet;

const MAX_ACTIVE_BACKGROUND_WAKES: i64 = 1_024;
pub const MAX_BACKGROUND_WAKE_PAGE: usize = 128;
const BACKGROUND_WAKE_RETENTION_MS: i64 = 31 * 24 * 60 * 60 * 1_000;
const MAX_PRE_ADMISSION_ATTEMPTS: u32 = 5;
const MAX_DECLARATION_SCAN_ROWS: usize = 128;
const MAX_TOOL_SCAN_ROWS: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackgroundWakeArmInput {
    pub agent: String,
    pub generation: i64,
    pub turn_id: String,
    pub process: ProcessIdentity,
    pub command_line: String,
    pub command_truncated: bool,
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackgroundWakeServerDeclarations {
    pub declared_pids: HashSet<u32>,
    pub declared_commands: Vec<String>,
}

impl Store {
    /// Whether this Agent generation and completed turn are eligible to arm an
    /// automatic background-process wake (no terminal state, stop, interrupt,
    /// open/same-turn manual wake, open automatic wake, or earlier automatic
    /// wake for this turn).
    pub fn background_wake_arm_eligible(
        &self,
        agent: &str,
        generation: i64,
        turn_id: &str,
    ) -> Result<bool> {
        let agent_num = parse_agent_id(agent)?;
        background_wake_arm_eligible_tx(&self.connection, agent_num, generation, turn_id)
    }

    /// Read the Agent generation's committed long-lived server declarations
    /// (`emit` topic `background.server` with `pid`, `pids` or `command`).
    pub fn background_wake_server_declarations(
        &self,
        agent: &str,
        generation: i64,
    ) -> Result<BackgroundWakeServerDeclarations> {
        parse_agent_id(agent)?;
        let mut declarations = BackgroundWakeServerDeclarations::default();

        let server_event_kind = format!("x.user.{BACKGROUND_SERVER_TOPIC}");
        let agent_principal = format!("agent:{agent}:{generation}");
        let mut emit_stmt = self
            .connection
            .prepare(
                "SELECT e.data
                 FROM events AS e INDEXED BY events_kind
                 JOIN commands AS c ON c.id = json_extract(e.cause, '$.command_id')
                 WHERE e.kind = ?1 AND c.principal = ?2
                 ORDER BY e.seq DESC LIMIT ?3",
            )
            .map_err(sql_error)?;
        let emit_rows = emit_stmt
            .query_map(
                params![
                    server_event_kind,
                    agent_principal,
                    MAX_DECLARATION_SCAN_ROWS as i64
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(sql_error)?;
        for row in emit_rows {
            let raw = row.map_err(sql_error)?;
            if let Ok(value) = decode_json(&raw) {
                let payload = value.get("data").unwrap_or(&value);
                if let Some(pid) = payload
                    .get("pid")
                    .and_then(Value::as_u64)
                    .and_then(|pid| u32::try_from(pid).ok())
                    .filter(|pid| *pid > 0)
                {
                    declarations.declared_pids.insert(pid);
                }
                if let Some(pids) = payload.get("pids").and_then(Value::as_array) {
                    for pid in pids
                        .iter()
                        .filter_map(Value::as_u64)
                        .filter_map(|pid| u32::try_from(pid).ok())
                        .filter(|pid| *pid > 0)
                    {
                        declarations.declared_pids.insert(pid);
                    }
                }
                if let Some(cmd) = payload
                    .get("command")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|cmd| !cmd.is_empty())
                {
                    declarations.declared_commands.push(cmd.to_owned());
                }
            }
        }

        Ok(declarations)
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
                 command_line, command_truncated, state, delivery_command_id,
                 created_at, expires_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'waiting', ?10, ?11, ?12)",
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
            output_tail: None,
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

    /// Native shell commands (`tool.started.command`) the plugin reported in `turn_id`.
    pub fn background_wake_tool_commands(
        &self,
        agent: &str,
        generation: i64,
        turn_id: &str,
    ) -> Result<Vec<String>> {
        let agent_num = parse_agent_id(agent)?;
        let mut stmt = self
            .connection
            .prepare(
                "SELECT json_extract(data, '$.command') FROM events INDEXED BY events_agent
                 WHERE agent = ?1 AND generation = ?2 AND kind = 'tool.started'
                   AND json_extract(data, '$.turn_id') = ?3
                   AND json_type(data, '$.command') = 'text'
                 ORDER BY seq DESC LIMIT ?4",
            )
            .map_err(sql_error)?;
        let rows = stmt
            .query_map(
                params![agent_num, generation, turn_id, MAX_TOOL_SCAN_ROWS as i64],
                |row| row.get::<_, String>(0),
            )
            .map_err(sql_error)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(sql_error)
    }

    /// Return a bounded, raw output tail captured by a native `tool.completed`
    /// event in `turn_id` matching `command_line`, if the adapter recorded one.
    pub fn background_wake_captured_output_tail(
        &self,
        agent: &str,
        generation: i64,
        turn_id: &str,
        command_line: &str,
    ) -> Result<Option<String>> {
        let agent_num = parse_agent_id(agent)?;
        let turn = load_turn(&self.connection, agent_num, generation, turn_id)?;
        let started_at = turn.and_then(|turn| turn.started_at);
        let mut stmt = self
            .connection
            .prepare(
                "SELECT kind, data FROM events INDEXED BY events_agent
                 WHERE agent = ?1 AND generation = ?2
                   AND kind IN ('tool.started', 'tool.completed')
                   AND (?3 IS NULL OR ts >= ?3)
                 ORDER BY seq DESC LIMIT 128",
            )
            .map_err(sql_error)?;
        let rows = stmt
            .query_map(params![agent_num, generation, started_at], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(sql_error)?;

        let mut matching_items = HashSet::new();
        let mut completed_in_turn: Vec<Value> = Vec::new();
        let needle = command_line.split_whitespace().next().unwrap_or("");
        for row in rows {
            let (kind, raw) = row.map_err(sql_error)?;
            let Ok(data) = decode_json(&raw) else {
                continue;
            };
            if data.get("turn_id").and_then(Value::as_str) != Some(turn_id) {
                continue;
            }
            if kind == "tool.started" {
                if let (Some(item_id), Some(cmd)) = (
                    data.get("item_id").and_then(Value::as_str),
                    data.get("command").and_then(Value::as_str),
                ) && !needle.is_empty()
                    && (cmd.contains(needle) || command_line.contains(cmd.trim()))
                {
                    matching_items.insert(item_id.to_owned());
                }
            } else if kind == "tool.completed" {
                completed_in_turn.push(data);
            }
        }

        let matched = completed_in_turn.iter().find_map(|data| {
            let item_id = data.get("item_id").and_then(Value::as_str)?;
            matching_items
                .contains(item_id)
                .then(|| extract_captured_tail(data))
                .flatten()
        });
        Ok(matched)
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
    if agent.generation != generation || agent.is_terminal() || agent.observe_only {
        return Ok(false);
    }
    let Some(turn) = load_turn(connection, agent_num, generation, turn_id)? else {
        return Ok(false);
    };
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
    Ok(!auto_wake_open)
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

fn extract_captured_tail(data: &Value) -> Option<String> {
    for key in ["output_tail", "error_tail", "stderr_tail", "stdout_tail"] {
        if let Some(text) = data
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
        {
            return Some(text.to_owned());
        }
    }
    data.pointer("/native_error/message")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
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
