//! Durable self-scoped Agent wake registrations (#833 item 3).

use super::{
    Error, Result, Store, canonical, command_binding, commit_transaction, digest_id_bound,
    encode_json, format_agent_id, load_agent_by_num, load_command_record, load_turn,
    parse_agent_id, process_record_from_row, sql_error, validate_id_age, validate_limit,
    validate_ulid, workspace, write_binding, write_exists,
};
use eliot_core::{
    Agent, CommandState, ProcessIdentity, ProcessRecord, ScriptDirectExit, ScriptProcessResult,
    StderrSnapshot, TurnState, WorkspacePhase,
    api::{AgentWakeCondition, AgentWakeRequest, AgentWakeResponse, ScriptRunApplied},
};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::json;
use ulid::Ulid;

const AGENT_WAKE_METHOD: &str = "agent.wake";
const MAX_ACTIVE_AGENT_WAKES: i64 = 1_024;
pub const MAX_AGENT_WAKE_PAGE: usize = 128;
const AGENT_WAKE_RETENTION_MS: i64 = 25 * 60 * 60 * 1_000;
const MAX_AGENT_WAKE_PRE_ADMISSION_ATTEMPTS: u32 = 5;
const MAX_AGENT_WAKE_FAILURE_REASON_BYTES: usize = 384;
const MAX_AGENT_WAKE_FACT_BYTES: usize = 1_024;

#[derive(Clone, Debug, PartialEq)]
pub struct AgentWakeRecord {
    pub id: String,
    pub agent: String,
    pub generation: i64,
    pub turn_id: String,
    pub condition: AgentWakeCondition,
    pub state: String,
    pub created_at: i64,
    pub expires_at: i64,
    pub fact: Option<String>,
    pub delivery_command_id: String,
    pub delivery_attempts: u32,
    pub delivery_next_at: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentWakeTurnReadiness {
    Waiting,
    Ready,
    Stale,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentWakeRunReceipt {
    Pending,
    ExitZero,
    ExitNonZero {
        exit_code: i32,
        stderr: Option<StderrSnapshot>,
    },
    NoMatch,
    Unknown,
}

impl Store {
    /// Bind a wake to the authenticated Agent's current active turn and retain
    /// the acknowledgement before returning it to the caller.
    pub fn agent_wake_arm(
        &mut self,
        principal: &str,
        request: &AgentWakeRequest,
        now: i64,
    ) -> Result<AgentWakeResponse> {
        request.validate()?;
        self.ensure_committed_reads()?;
        let (agent_num, agent_id, generation) = parse_agent_principal(principal)?;
        let payload_digest = canonical::digest(&json!({
            "condition": &request.condition,
            "timeout_secs": request.timeout_secs,
        }))?;
        if let Some((old_principal, old_method, old_digest, old_result)) =
            write_binding(&self.connection, &request.id)?
        {
            return retained_wake(
                old_principal,
                old_method,
                old_digest,
                old_result,
                principal,
                request,
                &payload_digest,
            );
        }
        validate_id_age(&request.id, now)?;
        if command_binding(&self.connection, &request.id)?.is_some()
            || write_exists(&self.connection, &request.id)?
            || digest_id_bound(&self.connection, &request.id)?
        {
            return Err(Error::new(
                "id_conflict",
                "wake id is already used by another retained operation",
            ));
        }
        self.ensure_writer_healthy()?;
        let result = self.agent_wake_arm_inner(
            principal,
            request,
            &payload_digest,
            (agent_num, agent_id, generation),
            now,
        );
        self.fault_on_persistence_error(&result);
        result
    }

    fn agent_wake_arm_inner(
        &mut self,
        principal: &str,
        request: &AgentWakeRequest,
        payload_digest: &[u8],
        caller: (i64, String, i64),
        now: i64,
    ) -> Result<AgentWakeResponse> {
        let (agent_num, agent_id, generation) = caller;
        let condition = serde_json::to_string(&request.condition)
            .map_err(|_| Error::new("internal", "wake condition could not be encoded"))?;
        let expires_at = now
            .checked_add(
                i64::try_from(request.timeout_secs)
                    .unwrap_or(i64::MAX)
                    .saturating_mul(1_000),
            )
            .ok_or_else(|| Error::new("invalid", "wake expiry overflows the Store clock"))?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if let Some((old_principal, old_method, old_digest, old_result)) =
            write_binding(&tx, &request.id)?
        {
            return retained_wake(
                old_principal,
                old_method,
                old_digest,
                old_result,
                principal,
                request,
                payload_digest,
            );
        }
        if command_binding(&tx, &request.id)?.is_some()
            || write_exists(&tx, &request.id)?
            || digest_id_bound(&tx, &request.id)?
        {
            return Err(Error::new(
                "id_conflict",
                "wake id is already used by another retained operation",
            ));
        }
        let agent = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "calling Agent was not found"))?;
        if agent.id != agent_id || agent.generation != generation || agent.is_terminal() {
            return Err(Error::new(
                "stale_generation",
                "wake caller is not the current live Agent generation",
            ));
        }
        let turn_id = agent.active_turn.clone().ok_or_else(|| {
            Error::new("bad_state", "agent.wake requires the caller's active turn")
        })?;
        let turn = load_turn(&tx, agent_num, generation, &turn_id)?
            .ok_or_else(|| Error::new("bad_state", "Agent active turn is not retained"))?;
        if turn.state != TurnState::Running {
            return Err(Error::new(
                "bad_state",
                "agent.wake requires the caller's exact running turn",
            ));
        }
        validate_condition_scope(&tx, &agent, generation, &request.condition)?;
        let active: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM agent_wakes
                 WHERE state IN ('waiting','satisfied','delivery_pending')",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if active >= MAX_ACTIVE_AGENT_WAKES {
            return Err(Error::new(
                "capacity",
                "active Agent wake limit has been reached",
            ));
        }
        let open_for_agent: bool = tx
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM agent_wakes WHERE agent = ?1 AND generation = ?2
                       AND state IN ('waiting','satisfied','delivery_pending')
                 )",
                params![agent_num, generation],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if open_for_agent {
            return Err(Error::new(
                "conflict",
                "Agent already has an active wake for this generation",
            ));
        }
        let delivery_command_id = unique_wake_command_id(&tx, &request.id)?;
        let response = AgentWakeResponse {
            id: request.id.clone(),
            agent: agent.id,
            generation,
            turn_id,
            delivery_command_id: delivery_command_id.clone(),
            state: "armed".to_owned(),
        };
        response.validate()?;
        let result = encode_json(&response)?;
        tx.execute(
            "INSERT INTO agent_wakes
                 (id, agent, generation, turn_id, condition, state, delivery_command_id,
                  created_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 'waiting', ?6, ?7, ?8)",
            params![
                request.id,
                agent_num,
                generation,
                response.turn_id,
                condition,
                delivery_command_id,
                now,
                expires_at,
            ],
        )
        .map_err(sql_error)?;
        tx.execute(
            "INSERT INTO writes(id, principal, method, payload_digest, result, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                request.id,
                principal,
                AGENT_WAKE_METHOD,
                payload_digest,
                result,
                now,
            ],
        )
        .map_err(sql_error)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(response)
    }

    /// Read an ordered page of live waits. The caller advances a keyset cursor
    /// and keeps every pass bounded, including while older waits remain open.
    pub fn agent_wake_page(
        &self,
        after: Option<(i64, String)>,
        limit: usize,
    ) -> Result<Vec<AgentWakeRecord>> {
        if limit == 0 || limit > MAX_AGENT_WAKE_PAGE {
            return Err(Error::new(
                "invalid",
                "Agent wake page limit is out of range",
            ));
        }
        let mut statement = self
            .connection
            .prepare(match after {
                None => {
                    "SELECT id, agent, generation, turn_id, condition, state, created_at,
                            expires_at, fact, delivery_command_id, delivery_attempts,
                            delivery_next_at
                     FROM agent_wakes
                     WHERE state IN ('waiting','satisfied','delivery_pending')
                     ORDER BY created_at, id LIMIT ?1"
                }
                Some(_) => {
                    "SELECT id, agent, generation, turn_id, condition, state, created_at,
                            expires_at, fact, delivery_command_id, delivery_attempts,
                            delivery_next_at
                     FROM agent_wakes
                     WHERE state IN ('waiting','satisfied','delivery_pending')
                       AND (created_at, id) > (?1, ?2)
                     ORDER BY created_at, id LIMIT ?3"
                }
            })
            .map_err(sql_error)?;
        let raw = match after {
            None => statement
                .query_map([limit as i64], wake_row)
                .map_err(sql_error)?,
            Some((created_at, id)) => statement
                .query_map(params![created_at, id, limit as i64], wake_row)
                .map_err(sql_error)?,
        };
        let mut rows = Vec::new();
        for row in raw {
            rows.push(decode_wake_row(row.map_err(sql_error)?)?);
        }
        Ok(rows)
    }

    pub fn agent_wake_turn_readiness(
        &self,
        agent: &str,
        generation: i64,
        turn_id: &str,
    ) -> Result<AgentWakeTurnReadiness> {
        let Some(current) = self.get_agent(agent)? else {
            return Ok(AgentWakeTurnReadiness::Stale);
        };
        if current.generation != generation || current.is_terminal() {
            return Ok(AgentWakeTurnReadiness::Stale);
        }
        let Some(turn) = self.get_turn(agent, generation, turn_id)? else {
            return Ok(AgentWakeTurnReadiness::Stale);
        };
        Ok(match turn.state {
            TurnState::Running | TurnState::Unknown => AgentWakeTurnReadiness::Waiting,
            TurnState::Completed => AgentWakeTurnReadiness::Ready,
        })
    }

    pub fn agent_wake_process(
        &self,
        agent: &str,
        generation: i64,
        identity: &ProcessIdentity,
    ) -> Result<Option<ProcessRecord>> {
        if generation <= 0 {
            return Err(Error::new(
                "stale_generation",
                "process generation is invalid",
            ));
        }
        let start_time = i64::try_from(identity.start_time)
            .map_err(|_| Error::new("invalid", "process start time is out of range"))?;
        let direct_owner = format!("agent:{agent}#{generation}");
        let workspace_owner = format!("workspace:{agent}#{generation}");
        self.connection
            .query_row(
                "SELECT host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at
                 FROM processes
                 WHERE host_boot_id = ?1 AND pid = ?2 AND start_time = ?3
                   AND owner IN (?4, ?5)",
                params![
                    identity.host_boot_id,
                    identity.pid,
                    start_time,
                    direct_owner,
                    workspace_owner
                ],
                process_record_from_row,
            )
            .optional()
            .map_err(sql_error)
    }

    /// Read the retained result of one already-authorized Run command.
    pub fn agent_wake_run_receipt(&self, run_id: &str) -> Result<AgentWakeRunReceipt> {
        let Some(run) = load_command_record(&self.connection, run_id)? else {
            return Err(Error::new(
                "not_found",
                "authorized Run receipt was not found",
            ));
        };
        if run.command.verb != "run" {
            return Err(Error::new(
                "not_allowed",
                "argv exit-zero wake requires an existing Run command",
            ));
        }
        Ok(match run.command.state {
            CommandState::Accepted | CommandState::Queued | CommandState::Dispatched => {
                AgentWakeRunReceipt::Pending
            }
            CommandState::Applied => {
                let Some(outcome) = run.command.outcome else {
                    return Ok(AgentWakeRunReceipt::Unknown);
                };
                match serde_json::from_value::<ScriptRunApplied>(outcome)
                    .ok()
                    .and_then(|applied| applied.validate().ok().map(|()| applied))
                {
                    Some(_) => AgentWakeRunReceipt::ExitZero,
                    None => AgentWakeRunReceipt::Unknown,
                }
            }
            CommandState::Failed => failed_run_wake_receipt(run.command.outcome),
            CommandState::Rejected | CommandState::Cancelled => AgentWakeRunReceipt::NoMatch,
            CommandState::Unknown => AgentWakeRunReceipt::Unknown,
        })
    }

    /// Record the condition fact and reserve a stable command id before any
    /// notification is submitted. Retrying after a crash uses that same id.
    pub fn agent_wake_satisfy(
        &mut self,
        id: &str,
        generation: i64,
        fact: &str,
        now: i64,
        allow_equal_seconds_deadline: bool,
    ) -> Result<bool> {
        if fact.trim().is_empty() || fact.len() > 1_024 || fact.contains('\0') {
            return Err(Error::new("invalid", "wake fact exceeds its text bound"));
        }
        self.ensure_writer_healthy()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let Some(agent_num) = wake_agent_num(&tx, id, generation)? else {
            return Ok(false);
        };
        let agent = load_agent_by_num(&tx, agent_num)?;
        if agent.is_none_or(|agent| agent.generation != generation || agent.is_terminal()) {
            tx.execute(
                "UPDATE agent_wakes SET state = 'stale', settled_at = ?1
                 WHERE id = ?2 AND generation = ?3 AND state = 'waiting'",
                params![now, id, generation],
            )
            .map_err(sql_error)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(false);
        }
        let changed = tx
            .execute(
                "UPDATE agent_wakes SET state = 'satisfied', fact = ?1, settled_at = ?2
                 WHERE id = ?3 AND generation = ?4 AND state = 'waiting'
                   AND (expires_at > ?2 OR (
                       ?5 = 1
                       AND json_extract(condition, '$.kind') = 'seconds'
                       AND created_at + CAST(json_extract(condition, '$.seconds') AS INTEGER) * 1000 = expires_at
                       AND expires_at <= ?2
                       AND created_at + CAST(json_extract(condition, '$.seconds') AS INTEGER) * 1000 <= ?2
                   ))",
                params![fact, now, id, generation, allow_equal_seconds_deadline],
            )
            .map_err(sql_error)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(changed == 1)
    }

    pub fn agent_wake_mark_delivery_pending(
        &mut self,
        id: &str,
        generation: i64,
        command_id: &str,
    ) -> Result<bool> {
        self.agent_wake_mark_state(id, generation, command_id, "delivery_pending")
    }

    pub fn agent_wake_delivery_pre_admission_failure(
        &mut self,
        id: &str,
        generation: i64,
        now: i64,
        failure_reason: &str,
    ) -> Result<bool> {
        self.ensure_writer_healthy()?;
        let outcome = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let current: Option<(i64, Option<String>)> = tx
                .query_row(
                    "SELECT delivery_attempts, fact FROM agent_wakes
                     WHERE id = ?1 AND generation = ?2 AND state = 'satisfied'",
                    params![id, generation],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(sql_error)?;
            let Some((attempts, fact)) = current else {
                return Ok(false);
            };
            let attempts = u32::try_from(attempts).map_err(|_| {
                Error::new("source_integrity", "wake delivery attempt count is invalid")
            })?;
            let next_attempt = attempts.saturating_add(1);
            let changed = if next_attempt >= MAX_AGENT_WAKE_PRE_ADMISSION_ATTEMPTS {
                let failed_fact =
                    wake_delivery_failure_fact(fact.as_deref(), failure_reason, next_attempt);
                tx.execute(
                    "UPDATE agent_wakes
                     SET delivery_attempts = ?1, fact = ?2, state = 'failed', settled_at = ?3
                     WHERE id = ?4 AND generation = ?5 AND state = 'satisfied'",
                    params![next_attempt, failed_fact, now, id, generation],
                )
                .map_err(sql_error)?
            } else {
                let delay_ms = 500_i64.saturating_mul(1_i64 << attempts.min(6)).min(30_000);
                tx.execute(
                    "UPDATE agent_wakes
                     SET delivery_attempts = ?1, delivery_next_at = ?2
                     WHERE id = ?3 AND generation = ?4 AND state = 'satisfied'",
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

    pub fn agent_wake_finish(
        &mut self,
        id: &str,
        generation: i64,
        command_id: Option<&str>,
        state: &str,
        now: i64,
    ) -> Result<bool> {
        if !matches!(
            state,
            "delivered" | "not_matched" | "failed" | "unknown" | "stale" | "expired"
        ) {
            return Err(Error::new(
                "invalid",
                "Agent wake terminal state is invalid",
            ));
        }
        self.ensure_writer_healthy()?;
        let result = self.connection.execute(
            "UPDATE agent_wakes SET state = ?1, settled_at = ?2
             WHERE id = ?3 AND generation = ?4
               AND (?5 IS NULL OR delivery_command_id = ?5)
               AND state IN ('waiting','satisfied','delivery_pending')",
            params![state, now, id, generation, command_id],
        );
        let outcome = result.map_err(sql_error);
        self.fault_on_persistence_error(&outcome);
        outcome.map(|changed| changed == 1)
    }

    pub fn agent_wake_expire(&mut self, id: &str, generation: i64, now: i64) -> Result<bool> {
        self.ensure_writer_healthy()?;
        let result = self.connection.execute(
            "UPDATE agent_wakes SET state = 'expired', settled_at = ?1
             WHERE id = ?2 AND generation = ?3 AND expires_at <= ?1
               AND state IN ('waiting','satisfied')",
            params![now, id, generation],
        );
        let outcome = result.map_err(sql_error);
        self.fault_on_persistence_error(&outcome);
        outcome.map(|changed| changed == 1)
    }

    pub fn agent_wake_mark_stale(&mut self, id: &str, generation: i64, now: i64) -> Result<bool> {
        self.agent_wake_finish(id, generation, None, "stale", now)
    }

    pub fn agent_wake_mark_unknown(
        &mut self,
        id: &str,
        generation: i64,
        command_id: Option<&str>,
        now: i64,
    ) -> Result<bool> {
        self.agent_wake_finish(id, generation, command_id, "unknown", now)
    }

    pub fn agent_wake_prune(&mut self, now: i64, limit: usize) -> Result<usize> {
        validate_limit(limit)?;
        let cutoff = now.saturating_sub(AGENT_WAKE_RETENTION_MS);
        self.ensure_writer_healthy()?;
        let result = self.connection.execute(
            "DELETE FROM agent_wakes WHERE id IN (
                 SELECT id FROM agent_wakes
                 WHERE state IN ('delivered','not_matched','failed','unknown','stale','expired')
                   AND settled_at < ?1
                 ORDER BY settled_at, id LIMIT ?2
             )",
            params![cutoff, limit as i64],
        );
        let outcome = result.map_err(sql_error);
        self.fault_on_persistence_error(&outcome);
        outcome
    }

    fn agent_wake_mark_state(
        &mut self,
        id: &str,
        generation: i64,
        command_id: &str,
        state: &str,
    ) -> Result<bool> {
        if state != "delivery_pending" {
            return Err(Error::new(
                "invalid",
                "Agent wake state transition is invalid",
            ));
        }
        self.ensure_writer_healthy()?;
        let result = self.connection.execute(
            "UPDATE agent_wakes SET state = 'delivery_pending'
             WHERE id = ?1 AND generation = ?2 AND delivery_command_id = ?3
               AND state IN ('satisfied','delivery_pending')",
            params![id, generation, command_id],
        );
        let outcome = result.map_err(sql_error);
        self.fault_on_persistence_error(&outcome);
        outcome.map(|changed| changed == 1)
    }
}

fn retained_wake(
    old_principal: String,
    old_method: String,
    old_digest: Vec<u8>,
    old_result: String,
    principal: &str,
    request: &AgentWakeRequest,
    payload_digest: &[u8],
) -> Result<AgentWakeResponse> {
    if old_principal != principal || old_method != AGENT_WAKE_METHOD || old_digest != payload_digest
    {
        return Err(Error::new(
            "id_conflict",
            "wake id is bound to another principal, method, or payload",
        ));
    }
    let response: AgentWakeResponse = serde_json::from_str(&old_result)
        .map_err(|_| Error::new("internal", "retained Agent wake result is invalid"))?;
    response.validate()?;
    if response.id != request.id {
        return Err(Error::new("source_integrity", "retained wake id changed"));
    }
    Ok(response)
}

fn parse_agent_principal(principal: &str) -> Result<(i64, String, i64)> {
    let rest = principal
        .strip_prefix("agent:")
        .ok_or_else(|| Error::new("not_allowed", "agent.wake is Agent-only"))?;
    let (agent_id, generation) = rest
        .rsplit_once(':')
        .ok_or_else(|| Error::new("not_allowed", "Agent principal is malformed"))?;
    let agent_num = parse_agent_id(agent_id)?;
    let generation = generation
        .parse::<i64>()
        .ok()
        .filter(|generation| *generation > 0)
        .ok_or_else(|| Error::new("not_allowed", "Agent generation is malformed"))?;
    if principal != format!("agent:{agent_id}:{generation}") {
        return Err(Error::new(
            "not_allowed",
            "Agent principal is not canonical",
        ));
    }
    Ok((agent_num, format_agent_id(agent_num), generation))
}

fn validate_condition_scope(
    tx: &Transaction<'_>,
    agent: &Agent,
    generation: i64,
    condition: &AgentWakeCondition,
) -> Result<()> {
    match condition {
        AgentWakeCondition::FileAppears { .. } => {
            require_agent_wake_workspace(tx, agent, generation).map(|_| ())
        }
        AgentWakeCondition::ProcessExit { identity } => {
            let start_time = i64::try_from(identity.start_time).map_err(|_| {
                Error::invalid_field(
                    "params.condition.identity.start_time",
                    "a process start time in range",
                )
            })?;
            let agent_owner = format!("agent:{}#{generation}", agent.id);
            let workspace_owner = format!("workspace:{}#{generation}", agent.id);
            let found: bool = tx
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM processes
                         WHERE host_boot_id = ?1 AND pid = ?2 AND start_time = ?3
                           AND owner IN (?4, ?5)
                     )",
                    params![
                        identity.host_boot_id,
                        identity.pid,
                        start_time,
                        agent_owner,
                        workspace_owner,
                    ],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if !found {
                return Err(Error::new(
                    "not_allowed",
                    "process identity is not owned by this Agent generation",
                ));
            }
            Ok(())
        }
        AgentWakeCondition::ArgvExit0 { run_id } => {
            validate_ulid(run_id)?;
            let run = load_command_record(tx, run_id)?
                .ok_or_else(|| Error::new("not_found", "authorized Run command was not found"))?;
            if run.command.verb != "run" {
                return Err(Error::new(
                    "not_allowed",
                    "argv exit-zero wake requires an existing Run command",
                ));
            }
            let frozen = super::scripts::decode_frozen_run(&run.command.args)?;
            super::rules::validate_script_run_admitted_origin_tx(tx, &run.command, &frozen)?;
            let eliot_core::api::FrozenScriptContext::Event { event, subject, .. } =
                &frozen.context
            else {
                return Err(Error::new(
                    "not_allowed",
                    "argv exit-zero wake requires an Agent-bound event Run",
                ));
            };
            let workspace = agent.workspace.as_deref().ok_or_else(|| {
                Error::new(
                    "not_allowed",
                    "argv exit-zero Run has no exact Agent workspace binding",
                )
            })?;
            if event.agent.as_deref() != Some(agent.id.as_str())
                || event.generation != Some(generation)
                || event
                    .subject
                    .as_ref()
                    .and_then(|row| row.workspace.as_deref())
                    != Some(workspace)
                || subject.as_ref().and_then(|row| row.workspace.as_deref()) != Some(workspace)
                || run
                    .command
                    .target
                    .as_deref()
                    .is_some_and(|target| target != agent.id.as_str())
            {
                return Err(Error::new(
                    "not_allowed",
                    "Run receipt is not scoped to this exact Agent generation and workspace",
                ));
            }
            Ok(())
        }
        AgentWakeCondition::ChildCompletion { turn } => {
            let child_num = parse_agent_id(&turn.agent)?;
            let child = load_agent_by_num(tx, child_num)?
                .ok_or_else(|| Error::new("not_found", "child Agent was not found"))?;
            if child.parent.as_deref() != Some(agent.id.as_str()) {
                return Err(Error::new(
                    "not_allowed",
                    "child completion must name a direct child of the caller",
                ));
            }
            if child.generation != turn.generation {
                return Err(Error::new(
                    "stale_generation",
                    "child completion turn is not in the child's current generation",
                ));
            }
            if load_turn(tx, child_num, turn.generation, &turn.turn_id)?.is_none() {
                return Err(Error::new("not_found", "exact child turn was not retained"));
            }
            Ok(())
        }
        AgentWakeCondition::Seconds { .. } => Ok(()),
    }
}

fn require_agent_wake_workspace(
    tx: &Transaction<'_>,
    agent: &Agent,
    generation: i64,
) -> Result<eliot_core::WorkspacePreparation> {
    let preparation = workspace::load_workspace(tx, parse_agent_id(&agent.id)?)?
        .ok_or_else(|| Error::new("not_allowed", "wake requires a managed Agent worktree"))?;
    if preparation.agent != agent.id
        || preparation.target_generation != generation
        || preparation.phase != WorkspacePhase::Ready
        || agent.workspace.as_deref() != Some(preparation.worktree_path.as_str())
        || agent.workspace_phase != Some(WorkspacePhase::Ready)
    {
        return Err(Error::new(
            "not_allowed",
            "wake worktree does not match the current Agent generation",
        ));
    }
    Ok(preparation)
}

fn failed_run_wake_receipt(outcome: Option<serde_json::Value>) -> AgentWakeRunReceipt {
    let Some(outcome) = outcome else {
        return AgentWakeRunReceipt::Unknown;
    };
    if outcome.get("code").and_then(serde_json::Value::as_str) != Some("failed") {
        return AgentWakeRunReceipt::Unknown;
    }
    let cause = outcome.get("cause").and_then(serde_json::Value::as_str);
    let Some(process) = outcome.get("process") else {
        return AgentWakeRunReceipt::Unknown;
    };
    let Ok(process) = serde_json::from_value::<ScriptProcessResult>(process.clone()) else {
        return AgentWakeRunReceipt::Unknown;
    };
    if process.validate().is_err() {
        return AgentWakeRunReceipt::Unknown;
    }
    match process.direct_exit {
        ScriptDirectExit::Exited {
            success: false,
            exit_code: Some(exit_code),
        } if exit_code != 0 && cause == Some("script_exit_failed") => {
            AgentWakeRunReceipt::ExitNonZero {
                exit_code,
                stderr: process.stderr,
            }
        }
        ScriptDirectExit::Exited {
            success: true,
            exit_code: Some(0),
        } if cause != Some("script_exit_failed") => AgentWakeRunReceipt::NoMatch,
        ScriptDirectExit::Exited {
            success: false,
            exit_code: None,
        } if cause == Some("script_exit_failed") => AgentWakeRunReceipt::NoMatch,
        ScriptDirectExit::Unknown
        | ScriptDirectExit::Exited { success: true, .. }
        | ScriptDirectExit::Exited {
            success: false,
            exit_code: Some(_),
        }
        | ScriptDirectExit::Exited {
            success: false,
            exit_code: None,
        } => AgentWakeRunReceipt::Unknown,
    }
}

fn wake_delivery_failure_fact(fact: Option<&str>, reason: &str, attempts: u32) -> String {
    let reason = reason
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    let reason = truncate_utf8(&reason, MAX_AGENT_WAKE_FAILURE_REASON_BYTES);
    let suffix = format!("; delivery stopped after {attempts} pre-admission failures: {reason}");
    let fact = fact.unwrap_or("wake condition satisfied");
    let fact = truncate_utf8(fact, MAX_AGENT_WAKE_FACT_BYTES.saturating_sub(suffix.len()));
    format!("{fact}{suffix}")
}

fn truncate_utf8(text: &str, max_bytes: usize) -> &str {
    let mut end = text.len().min(max_bytes);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn wake_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawWake> {
    Ok(RawWake {
        id: row.get(0)?,
        agent: row.get(1)?,
        generation: row.get(2)?,
        turn_id: row.get(3)?,
        condition: row.get(4)?,
        state: row.get(5)?,
        created_at: row.get(6)?,
        expires_at: row.get(7)?,
        fact: row.get(8)?,
        delivery_command_id: row.get(9)?,
        delivery_attempts: row.get(10)?,
        delivery_next_at: row.get(11)?,
    })
}

struct RawWake {
    id: String,
    agent: i64,
    generation: i64,
    turn_id: String,
    condition: String,
    state: String,
    created_at: i64,
    expires_at: i64,
    fact: Option<String>,
    delivery_command_id: String,
    delivery_attempts: u32,
    delivery_next_at: i64,
}

fn decode_wake_row(row: RawWake) -> Result<AgentWakeRecord> {
    let condition = serde_json::from_str::<AgentWakeCondition>(&row.condition)
        .map_err(|_| Error::new("source_integrity", "retained wake condition is invalid"))?;
    Ok(AgentWakeRecord {
        id: row.id,
        agent: format_agent_id(row.agent),
        generation: row.generation,
        turn_id: row.turn_id,
        condition,
        state: row.state,
        created_at: row.created_at,
        expires_at: row.expires_at,
        fact: row.fact,
        delivery_command_id: row.delivery_command_id,
        delivery_attempts: row.delivery_attempts,
        delivery_next_at: row.delivery_next_at,
    })
}

fn wake_agent_num(tx: &Transaction<'_>, id: &str, generation: i64) -> Result<Option<i64>> {
    tx.query_row(
        "SELECT agent FROM agent_wakes WHERE id = ?1 AND generation = ?2 AND state = 'waiting'",
        params![id, generation],
        |row| row.get(0),
    )
    .optional()
    .map_err(sql_error)
}

fn unique_wake_command_id(tx: &Transaction<'_>, wake_id: &str) -> Result<String> {
    for _ in 0..8 {
        let id = Ulid::new().to_string();
        let reserved_wake_id: bool = tx
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM agent_wakes
                     WHERE id = ?1 OR delivery_command_id = ?1
                 )",
                [&id],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if id != wake_id
            && command_binding(tx, &id)?.is_none()
            && write_binding(tx, &id)?.is_none()
            && !digest_id_bound(tx, &id)?
            && !reserved_wake_id
        {
            return Ok(id);
        }
    }
    Err(Error::new(
        "capacity",
        "could not reserve a unique wake delivery command id",
    ))
}
