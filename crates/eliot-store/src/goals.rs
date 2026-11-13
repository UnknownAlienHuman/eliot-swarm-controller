//! Durable Eliot-mode Goals: revisioned definitions on one Agent
//! generation plus the stale-round-safe conditional round commit.

use super::*;
use eliot_core::api::{
    CommandSubmitResponse, GOAL_COST_UNSUPPORTED, GoalCounters, GoalDefinition, GoalMarkerState,
    GoalMode, GoalRoundMarker, GoalRoundOutcome, GoalRoundRequest, GoalRoundResponse, GoalStatus,
    GoalView, MAX_GOAL_OBSERVATION_BYTES,
};
use serde::{Deserialize, Serialize};

/// Pre-round facts. The Router evaluates gates from this snapshot; the commit
/// re-verifies generation, revision and round freshness atomically.
#[derive(Clone, Debug)]
pub struct GoalRoundProbe {
    pub view: GoalView,
    pub agent_state: AgentState,
    pub active_turn: Option<String>,
    pub pending_count: usize,
    pub unhealthy_sources: Vec<String>,
    pub open_commands: i64,
    pub unknown_commands: i64,
    pub max_seq: i64,
    pub turn_count: i64,
    /// True when the turn budget was spent solely by this Goal's own last
    /// admitted continuation (exactly one turn since the last round commit,
    /// and it is that continuation's completed turn). Only then may a
    /// spent-budget round still run its final check.
    pub spent_by_goal_continuation: bool,
    pub workspace: Option<String>,
}

/// Definite check result. Unknown check outcomes never reach the commit.
#[derive(Clone, Debug)]
pub struct GoalCheckResult {
    pub exit_code: Option<i32>,
    pub output_complete: bool,
    pub output_digest: Option<String>,
    pub observation: Option<String>,
}

/// One ordinary `send` continuation admitted atomically with the round commit.
#[derive(Clone, Debug)]
pub struct GoalContinuation {
    pub id: String,
    pub text: String,
}

/// One round-key form. Idle keys name an exact idle event seq; slot keys
/// name an anchored cadence slot index (first slot is 1). The form must
/// match the Goal: cadenced Goals round on slots, the rest on idle seqs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GoalRoundKey {
    Idle(i64),
    Slot(i64),
}

impl GoalRoundKey {
    pub fn value(self) -> i64 {
        match self {
            Self::Idle(value) | Self::Slot(value) => value,
        }
    }

    pub fn kind(self) -> &'static str {
        match self {
            Self::Idle(_) => "idle",
            Self::Slot(_) => "slot",
        }
    }

    pub fn text(self) -> String {
        format!("{}:{}", self.kind(), self.value())
    }
}

/// Binding check inside an open Immediate transaction. Returns Some(view)
/// when this exact write id is an exact replay; the caller commits and
/// returns it without touching Goal state. Returns None for a fresh id.
fn replay_goal_write<T: serde::de::DeserializeOwned>(
    tx: &Transaction<'_>,
    id: &str,
    principal: &str,
    method: &str,
    payload_digest: &[u8],
) -> Result<Option<T>> {
    if command_binding(tx, id)?.is_some() {
        return Err(Error::new(
            "id_conflict",
            format!("{method} id is already used by a command"),
        ));
    }
    let Some((old_principal, old_method, old_digest, old_result)) = write_binding(tx, id)? else {
        return Ok(None);
    };
    if old_principal != principal || old_method != method || old_digest != payload_digest {
        return Err(Error::new(
            "id_conflict",
            format!("{method} id is bound to another principal, method, or payload"),
        ));
    }
    let value: T = serde_json::from_value(decode_json(&old_result)?)
        .map_err(|error| Error::new("internal", error.to_string()))?;
    Ok(Some(value))
}

/// Persist the original result alongside the already-applied mutation,
/// still inside the caller's transaction.
fn retain_goal_result<T: Serialize>(
    tx: &Transaction<'_>,
    id: &str,
    principal: &str,
    method: &str,
    payload_digest: &[u8],
    view: &T,
    now: i64,
) -> Result<()> {
    let result_json = encode_json(view)?;
    // The retained write stores the full original GoalView, whose
    // definition alone may approach 64 KiB plus counters/marker
    // overhead. Bound it by the wire frame budget, not by the
    // smaller persisted-event budget used for ordinary events.
    if result_json.len() > MAX_FRAME_BYTES {
        return Err(Error::new(
            "frame_too_large",
            format!("{method} result exceeds the frame bound"),
        ));
    }
    db!(tx.execute(
        "INSERT INTO writes(id, principal, method, payload_digest, result, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![id, principal, method, payload_digest, result_json, now],
    ));
    Ok(())
}

#[derive(Clone, Debug)]
pub struct GoalRoundCommit {
    pub id: String,
    pub principal: String,
    pub agent: String,
    pub mode: String,
    pub expected_generation: i64,
    pub expected_revision: i64,
    pub round_key: GoalRoundKey,
    /// Agent high-water observed by the claim. Slot commits require it
    /// unchanged (no Agent event during the check); idle commits pin the
    /// key itself instead.
    pub claim_seq: i64,
    pub check: Option<GoalCheckResult>,
    pub blocked_reason: Option<String>,
    pub continuation: Option<GoalContinuation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GoalCommitOutcome {
    Completed,
    Blocked { reason: String },
    Continued { command_id: String },
}

#[derive(Clone, Debug)]
pub struct GoalRoundCommitResult {
    pub outcome: GoalCommitOutcome,
    pub view: GoalView,
    pub admission: Option<Admission>,
    pub response: GoalRoundResponse,
}

impl GoalRoundCommitResult {
    fn replay(response: GoalRoundResponse) -> Result<Self> {
        let outcome = match response.outcome {
            GoalRoundOutcome::Completed => GoalCommitOutcome::Completed,
            GoalRoundOutcome::Blocked => GoalCommitOutcome::Blocked {
                reason: response
                    .reason
                    .clone()
                    .ok_or_else(|| Error::new("internal", "stored blocked round has no reason"))?,
            },
            GoalRoundOutcome::Continued => GoalCommitOutcome::Continued {
                command_id: response.command_id.clone().ok_or_else(|| {
                    Error::new("internal", "stored continued round has no command id")
                })?,
            },
        };
        Ok(Self {
            outcome,
            view: response.view.clone(),
            admission: None,
            response,
        })
    }
}

/// One active Eliot-mode Goal that could round on its retained idle
/// event. `idle_seq` carries the latest event seq only when that row
/// still names an idle event of the Goal's generation; otherwise the
/// retained evidence is pruned, superseded or never existed and the
/// Router waits for a real event instead of inventing one.
#[derive(Clone, Debug)]
pub struct GoalReconcileCandidate {
    pub agent: String,
    pub generation: i64,
    pub revision: i64,
    pub agent_state: AgentState,
    pub idle_seq: Option<i64>,
}

/// A duplicate claim returns its original reply, without another check.
#[derive(Clone, Debug)]
pub enum GoalRoundClaim {
    Claimed { seq: i64, continuation_id: String },
    Replay(Box<GoalRoundResponse>),
}

/// Private reservation and final reply in the existing writes.result column.
/// Its captured marker binding survives Goal revisions and daemon restart.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct GoalRoundRecord {
    request: GoalRoundRequest,
    generation: i64,
    revision: i64,
    key: GoalRoundKey,
    holder: String,
    claim_seq: i64,
    continuation_id: String,
    outcome: StoredGoalRoundOutcome,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum StoredGoalRoundOutcome {
    Running,
    Finished(Box<GoalRoundResponse>),
    Failed(Error),
}

/// Retention uses the same serialized variant as the live reservation DTO.
pub(super) fn goal_round_running_outcome_json() -> Result<String> {
    encode_json(&StoredGoalRoundOutcome::Running)
}

fn goal_round_digest(request: &GoalRoundRequest) -> Result<Vec<u8>> {
    canonical::digest(&json!({
        "agent": request.agent, "mode": request.mode.as_str(),
        "idle_seq": request.idle_seq, "slot": request.slot,
    }))
}

fn goal_request_key(request: &GoalRoundRequest) -> Result<GoalRoundKey> {
    request.validate()?;
    match (request.idle_seq, request.slot) {
        (Some(seq), None) => Ok(GoalRoundKey::Idle(seq)),
        (None, Some(slot)) => Ok(GoalRoundKey::Slot(slot)),
        _ => Err(Error::new("invalid", "goal round needs exactly one key")),
    }
}

fn update_goal_round_tx(tx: &Transaction<'_>, record: &GoalRoundRecord) -> Result<()> {
    let result = encode_json(record)?;
    if result.len() > MAX_FRAME_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "goal.round result exceeds the frame bound",
        ));
    }
    let changed = db!(tx.execute(
        "UPDATE writes SET result = ?1 WHERE id = ?2 AND method = 'goal.round'",
        params![result, record.request.id],
    ));
    if changed != 1 {
        return Err(Error::new(
            "unknown",
            "goal round reservation is no longer retained",
        ));
    }
    Ok(())
}

fn abandon_goal_marker_tx(tx: &Transaction<'_>, record: &GoalRoundRecord, now: i64) -> Result<()> {
    db!(tx.execute(
        "UPDATE goal_markers SET state = 'unknown', updated_at = ?1
         WHERE agent = ?2 AND mode = ?3 AND generation = ?4 AND revision = ?5
           AND round_key = ?6 AND round_key_kind = ?7 AND state = 'running' AND holder = ?8",
        params![
            now,
            parse_agent_id(&record.request.agent)?,
            record.request.mode.as_str(),
            record.generation,
            record.revision,
            record.key.value(),
            record.key.kind(),
            record.holder
        ],
    ));
    Ok(())
}

fn unknown_goal_round_error() -> Error {
    Error::new("unknown", "goal round ended without a retained result")
}

fn retain_goal_round_error_tx(
    tx: &Transaction<'_>,
    record: &mut GoalRoundRecord,
    error: &Error,
) -> Result<Error> {
    record.outcome = StoredGoalRoundOutcome::Failed(error.clone());
    if let Err(failure) = update_goal_round_tx(tx, record) {
        if failure.code != "frame_too_large" {
            return Err(failure);
        }
        // If the original error cannot fit, retain and return unknown.
        // Claim preflights this recovery record before any external check.
        let unknown = unknown_goal_round_error();
        record.outcome = StoredGoalRoundOutcome::Failed(unknown.clone());
        update_goal_round_tx(tx, record)?;
        return Ok(unknown);
    }
    Ok(error.clone())
}

// Nested Result lets callers commit recovery of an abandoned reservation
// before returning its stored error to the client.
fn replay_goal_round_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &GoalRoundRequest,
    holder: &str,
    now: i64,
) -> Result<Option<Result<GoalRoundResponse>>> {
    let digest = goal_round_digest(request)?;
    let Some(mut record) =
        replay_goal_write::<GoalRoundRecord>(tx, &request.id, principal, "goal.round", &digest)?
    else {
        validate_id_age(&request.id, now)?;
        return Ok(None);
    };
    match &record.outcome {
        StoredGoalRoundOutcome::Finished(response) => {
            request.validate_response(response)?;
            return Ok(Some(Ok(*response.clone())));
        }
        StoredGoalRoundOutcome::Failed(error) => return Ok(Some(Err(error.clone()))),
        StoredGoalRoundOutcome::Running => {}
    }
    let marker = load_goal_marker_tx(
        tx,
        parse_agent_id(&request.agent)?,
        request.mode.as_str(),
        record.generation,
        record.revision,
        record.key,
    )?;
    if record.holder == holder && marker.is_some_and(|marker| marker.is_live(holder)) {
        return Ok(Some(Err(Error::new(
            "busy",
            "goal round already in flight",
        ))));
    }
    abandon_goal_marker_tx(tx, &record, now)?;
    let error = retain_goal_round_error_tx(tx, &mut record, &unknown_goal_round_error())?;
    Ok(Some(Err(error)))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GoalStatePersist {
    status: GoalStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    paused_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    blocked_reason: Option<String>,
    #[serde(default)]
    counters: GoalCounters,
    /// Slot-grid anchor captured when a cadence was set; stable across
    /// pause/clear/resume. Absent without a cadence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cadence_anchor_ms: Option<i64>,
    /// Last committed cadence slot. Slot keys are monotonic: any slot at
    /// or below it is refused at probe, claim and commit. Fresh goals
    /// start with none; idle rounds never touch it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_committed_slot: Option<i64>,
    /// Command id of the Goal continuation admitted by the most recent
    /// `Continued` round commit, until a later round consumes it. Private
    /// and never shown in a view: the probe resolves it to that command's
    /// completed turn so a budget spent by the Goal's own continuation
    /// does not skip the final check. Absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_continuation_command_id: Option<String>,
}

struct GoalRow {
    agent_num: i64,
    mode: String,
    generation: i64,
    revision: i64,
    definition: GoalDefinition,
    state: GoalStatePersist,
    last_round_key: Option<String>,
    updated_at: i64,
}

impl Store {
    pub fn goal_set(
        &mut self,
        agent: &str,
        mode: &str,
        id: &str,
        principal: &str,
        definition: &GoalDefinition,
        now: i64,
    ) -> Result<GoalView> {
        self.ensure_writer_healthy()?;
        definition.validate()?;
        let result = self.goal_set_inner(agent, mode, id, principal, definition, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn goal_get(&self, agent: &str, mode: &str) -> Result<Option<GoalView>> {
        require_eliot_mode(mode)?;
        let agent_num = parse_agent_id(agent)?;
        load_goal_row(&self.connection, agent_num, mode)?
            .map(|row| self.goal_view(row))
            .transpose()
    }

    pub fn goal_pause(
        &mut self,
        agent: &str,
        mode: &str,
        id: &str,
        principal: &str,
        reason: Option<&str>,
        now: i64,
    ) -> Result<GoalView> {
        self.ensure_writer_healthy()?;
        let result = self.goal_pause_inner(agent, mode, id, principal, reason, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn goal_clear(
        &mut self,
        agent: &str,
        mode: &str,
        id: &str,
        principal: &str,
        now: i64,
    ) -> Result<GoalView> {
        self.ensure_writer_healthy()?;
        let result = self.goal_clear_inner(agent, mode, id, principal, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn goal_resume(
        &mut self,
        agent: &str,
        mode: &str,
        id: &str,
        principal: &str,
        now: i64,
    ) -> Result<GoalView> {
        self.ensure_writer_healthy()?;
        let result = self.goal_resume_inner(agent, mode, id, principal, now);
        self.fault_on_persistence_error(&result);
        result
    }

    /// Whether an active native-mode Goal owns the Agent. The automatic
    /// Eliot driver waits while one drives. Native `set` is unsupported,
    /// so no such row exists today; this stays the wait point.
    pub fn goal_native_driving(&self, agent: &str) -> Result<bool> {
        let agent_num = parse_agent_id(agent)?;
        Ok(load_goal_row(&self.connection, agent_num, "native")?
            .is_some_and(|row| row.state.status == GoalStatus::Active))
    }

    /// Active non-cadenced Eliot Goals bound to their Agent's current
    /// generation. The Router applies the same enablement checks as the
    /// event path before reserving a round on the retained idle subject.
    pub fn goal_reconcile_candidates(&self) -> Result<Vec<GoalReconcileCandidate>> {
        let rows: Vec<(i64, i64, i64)> = {
            let mut statement = self
                .connection
                .prepare(
                    "SELECT g.agent, g.generation, g.revision
                     FROM goals g JOIN agents a ON a.id = g.agent
                     WHERE g.mode = 'eliot'
                       AND json_extract(g.state, '$.status') = 'active'
                       AND g.generation = a.generation
                       AND json_extract(g.definition, '$.cadence_ms') IS NULL
                       AND a.observe_only = 0
                     ORDER BY g.agent",
                )
                .map_err(sql_error)?;
            statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                .map_err(sql_error)?
                .collect::<rusqlite::Result<Vec<(i64, i64, i64)>>>()
                .map_err(sql_error)?
        };
        let mut candidates = Vec::with_capacity(rows.len());
        for (agent_num, generation, revision) in rows {
            let agent_row = load_agent_by_num(&self.connection, agent_num)?
                .ok_or_else(|| Error::new("internal", "reconcile goal lost its Agent"))?;
            let max_seq = max_agent_seq(&self.connection, agent_num, generation)?;
            let idle_seq = if max_seq > 0
                && round_key_names_generation_event(
                    &self.connection,
                    agent_num,
                    generation,
                    max_seq,
                )? {
                Some(max_seq)
            } else {
                None
            };
            candidates.push(GoalReconcileCandidate {
                agent: format_agent_id(agent_num),
                generation,
                revision,
                agent_state: agent_row.state,
                idle_seq,
            });
        }
        Ok(candidates)
    }

    /// Read round facts, pausing a generation-drifted Goal. Fails before any
    /// check runs when the Goal is missing, terminal, paused or drifted, when
    /// another round is in flight, or when the key is already consumed. Gate
    /// facts (idle state, turns, commands, sources) are only reported here;
    /// the Router evaluates them and `goal_claim_round` re-validates them
    /// atomically before anything runs.
    pub fn goal_round_probe(
        &mut self,
        agent: &str,
        mode: &str,
        key: GoalRoundKey,
        now: i64,
    ) -> Result<GoalRoundProbe> {
        self.ensure_writer_healthy()?;
        require_eliot_mode(mode)?;
        require_key_range(key)?;
        let agent_num = parse_agent_id(agent)?;
        let agent_row = load_agent_by_num(&self.connection, agent_num)?
            .ok_or_else(|| Error::new("not_found", "goal Agent was not found"))?;
        if agent_row.observe_only {
            return Err(Error::new(
                "not_allowed",
                "observe-only Agents take no Goal rounds",
            ));
        }
        let Some(row) = load_goal_row(&self.connection, agent_num, mode)? else {
            return Err(Error::new("not_found", "goal was not found"));
        };
        if row.generation != agent_row.generation {
            if row.state.status == GoalStatus::Active {
                self.pause_goal_tx(agent_num, &row, "Agent generation advanced", now)?;
            }
            return Err(Error::new(
                "stale_generation",
                format!(
                    "goal binds generation {} but Agent {} is at generation {}",
                    row.generation, agent, agent_row.generation
                ),
            ));
        }
        if row.state.status != GoalStatus::Active {
            return Err(Error::new(
                "bad_state",
                format!("goal is {} and takes no rounds", row.state.status.as_str()),
            ));
        }
        if row.last_round_key.as_deref() == Some(&key.text()) {
            return Err(Error::new(
                "stale_round",
                "goal round was already committed",
            ));
        }
        let markers = load_goal_markers(&self.connection, agent_num, mode)?;
        if markers
            .iter()
            .any(|marker| marker.is_live(&self.goal_holder))
        {
            return Err(Error::new("busy", "goal round already in flight"));
        }
        // The burn ignores revision: pause/clear/resume bump it but never
        // unburn an unknown round key of this generation. It still separates
        // key forms: a burned idle seq never blocks the same slot number.
        if markers.iter().any(|marker| {
            marker.round_key == key.value()
                && marker.kind == key.kind()
                && marker.generation == row.generation
                && !marker.is_live(&self.goal_holder)
        }) {
            return Err(Error::new("stale_round", "goal round key was already used"));
        }
        require_key_form(&row, key)?;
        match key {
            GoalRoundKey::Idle(idle_seq) => {
                if !round_key_names_generation_event(
                    &self.connection,
                    agent_num,
                    row.generation,
                    idle_seq,
                )? {
                    return Err(Error::new(
                        "stale_round",
                        "round key does not name an idle event of this Agent generation",
                    ));
                }
            }
            GoalRoundKey::Slot(slot) => {
                require_slot_due(&row, slot, now)?;
            }
        }
        let turn_count = count_turns(&self.connection, agent_num, agent_row.generation)?;
        let spent_by_goal_continuation = goal_continuation_spent_budget(
            &self.connection,
            agent_num,
            row.generation,
            row.state.counters.turns_at_set,
            row.state.counters.turns_used,
            row.state.last_continuation_command_id.as_deref(),
            turn_count,
        )?;
        let view = self.goal_view(row)?;
        let probe = GoalRoundProbe {
            agent_state: agent_row.state,
            active_turn: agent_row.active_turn.clone(),
            pending_count: agent_row.pending.len(),
            unhealthy_sources: unhealthy_open_sources(
                &self.connection,
                agent_num,
                agent_row.generation,
            )?,
            open_commands: count_agent_commands(
                &self.connection,
                agent_num,
                agent_row.generation,
                false,
            )?,
            unknown_commands: count_agent_commands(
                &self.connection,
                agent_num,
                agent_row.generation,
                true,
            )?,
            max_seq: max_agent_seq(&self.connection, agent_num, agent_row.generation)?,
            turn_count,
            spent_by_goal_continuation,
            workspace: agent_row.workspace.clone(),
            view,
        };
        Ok(probe)
    }

    /// Commit the durable in-flight marker for one round, re-validating the
    /// whole probe (Goal binding, single flight, idle gates, round key, and
    /// the spent-budget qualification) in the same Immediate transaction.
    /// `expected_spent_by_goal_continuation` is the probe's qualification;
    /// if the live one differs the claim is refused as stale without
    /// inserting a marker. A second claim while a marker is running is
    /// refused without running anything. Every claim resolves exactly once:
    /// the final commit consumes its marker, anything else (unknown check,
    /// interference, crash) leaves it `unknown` and burns the key.
    pub fn goal_claim_round(
        &mut self,
        principal: &str,
        request: &GoalRoundRequest,
        expected_revision: i64,
        expected_spent_by_goal_continuation: bool,
        now: i64,
    ) -> Result<GoalRoundClaim> {
        self.ensure_writer_healthy()?;
        let result = self.goal_claim_round_inner(
            principal,
            request,
            expected_revision,
            expected_spent_by_goal_continuation,
            now,
        );
        self.fault_on_persistence_error(&result);
        result
    }

    /// Resolve retained requests before probing any newer Goal state.
    pub fn goal_round_replay(
        &mut self,
        principal: &str,
        request: &GoalRoundRequest,
        now: i64,
    ) -> Result<Option<GoalRoundResponse>> {
        self.ensure_writer_healthy()?;
        require_nonempty(principal, "principal")?;
        request.validate()?;
        require_eliot_mode(request.mode.as_str())?;
        validate_ulid(&request.id)?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let replay = replay_goal_round_tx(&tx, principal, request, &self.goal_holder, now)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(replay)
        })();
        self.fault_on_persistence_error(&result);
        result?.transpose()
    }

    /// Burn the marker and retain the original sanitized error atomically.
    /// If persistence itself fails, the reservation stays unresolved; it
    /// never authorizes a rerun, and a new holder reads it as unknown.
    pub fn goal_abandon_round(
        &mut self,
        principal: &str,
        request: &GoalRoundRequest,
        error: &Error,
        now: i64,
    ) -> Result<Error> {
        self.goal_finish_round_error(principal, request, error, now, true)
    }

    /// Retain the request error without releasing process single-flight.
    /// A failed request and an unproven process-family closure are independent.
    pub fn goal_hold_round_error(
        &mut self,
        principal: &str,
        request: &GoalRoundRequest,
        error: &Error,
        now: i64,
    ) -> Result<Error> {
        self.goal_finish_round_error(principal, request, error, now, false)
    }

    fn goal_finish_round_error(
        &mut self,
        principal: &str,
        request: &GoalRoundRequest,
        error: &Error,
        now: i64,
        release_marker: bool,
    ) -> Result<Error> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let mut record = replay_goal_write::<GoalRoundRecord>(
                &tx,
                &request.id,
                principal,
                "goal.round",
                &goal_round_digest(request)?,
            )?
            .ok_or_else(|| Error::new("unknown", "goal round reservation is no longer retained"))?;
            if release_marker && record.holder == self.goal_holder {
                // A later proven closure may release the marker without
                // replacing an already retained original request error.
                abandon_goal_marker_tx(&tx, &record, now)?;
            }
            let retained = if matches!(record.outcome, StoredGoalRoundOutcome::Running)
                && record.holder == self.goal_holder
            {
                retain_goal_round_error_tx(&tx, &mut record, error)?
            } else if let StoredGoalRoundOutcome::Failed(retained) = record.outcome {
                retained
            } else {
                error.clone()
            };
            commit_transaction(tx, &self.write_faulted)?;
            Ok(retained)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Apply one round outcome only if generation, revision and round still
    /// match. An optional `send` continuation is admitted in the same
    /// transaction; any mismatch rolls everything back.
    pub fn goal_commit_round(
        &mut self,
        commit: &GoalRoundCommit,
        now: i64,
    ) -> Result<GoalRoundCommitResult> {
        self.ensure_writer_healthy()?;
        let result = self.goal_commit_round_inner(commit, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn goal_set_inner(
        &mut self,
        agent: &str,
        mode: &str,
        id: &str,
        principal: &str,
        definition: &GoalDefinition,
        now: i64,
    ) -> Result<GoalView> {
        require_eliot_mode(mode)?;
        definition.require_auto_bound()?;
        let payload_digest = canonical::digest(&json!({
            "agent": agent,
            "mode": mode,
            "definition": definition,
        }))?;
        let goal_holder = self.goal_holder.clone();
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if let Some(view) = replay_goal_write(&tx, id, principal, "goal.set", &payload_digest)? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(view);
        }
        validate_id_age(id, now)?;
        let agent_num = parse_agent_id(agent)?;
        let agent_row = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "goal Agent was not found"))?;
        if agent_row.observe_only {
            return Err(Error::new(
                "not_allowed",
                "observe-only Agents take no Goals",
            ));
        }
        let encoded = encode_json(definition)?;
        if encoded.len() > MAX_EVENT_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "goal definition exceeds 64 KiB",
            ));
        }
        let turn_count = count_turns(&tx, agent_num, agent_row.generation)?;
        if let Some(row) = load_goal_row_tx(&tx, agent_num, mode)? {
            if row.generation == agent_row.generation
                && row.state.status == GoalStatus::Active
                && encode_json(&row.definition)? == encoded
            {
                let view = goal_view_on(&tx, &goal_holder, row)?;
                retain_goal_result(&tx, id, principal, "goal.set", &payload_digest, &view, now)?;
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(view);
            }
            let revision = row.revision + 1;
            let state = GoalStatePersist {
                status: GoalStatus::Active,
                paused_reason: None,
                blocked_reason: None,
                counters: GoalCounters {
                    turns_at_set: turn_count,
                    ..GoalCounters::default()
                },
                cadence_anchor_ms: definition.cadence_ms.map(|_| now),
                last_committed_slot: None,
                last_continuation_command_id: None,
            };
            db!(tx.execute(
                "UPDATE goals SET generation = ?1, revision = ?2, definition = ?3,
                     state = ?4, last_round_key = NULL, updated_at = ?5
                 WHERE agent = ?6 AND mode = ?7",
                params![
                    agent_row.generation,
                    revision,
                    encoded,
                    encode_json(&state)?,
                    now,
                    agent_num,
                    mode,
                ],
            ));
            let updated = load_goal_row_tx(&tx, agent_num, mode)?
                .ok_or_else(|| Error::new("internal", "goal disappeared after set"))?;
            let view = goal_view_on(&tx, &goal_holder, updated)?;
            retain_goal_result(&tx, id, principal, "goal.set", &payload_digest, &view, now)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(view);
        }
        let state = GoalStatePersist {
            status: GoalStatus::Active,
            paused_reason: None,
            blocked_reason: None,
            counters: GoalCounters {
                turns_at_set: turn_count,
                ..GoalCounters::default()
            },
            cadence_anchor_ms: definition.cadence_ms.map(|_| now),
            last_committed_slot: None,
            last_continuation_command_id: None,
        };
        db!(tx.execute(
            "INSERT INTO goals(agent, mode, generation, revision, definition,
                 state, last_round_key, updated_at)
             VALUES (?1, ?2, ?3, 1, ?4, ?5, NULL, ?6)",
            params![
                agent_num,
                mode,
                agent_row.generation,
                encoded,
                encode_json(&state)?,
                now,
            ],
        ));
        let inserted = load_goal_row_tx(&tx, agent_num, mode)?
            .ok_or_else(|| Error::new("internal", "goal disappeared after set"))?;
        let view = goal_view_on(&tx, &goal_holder, inserted)?;
        retain_goal_result(&tx, id, principal, "goal.set", &payload_digest, &view, now)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(view)
    }

    fn goal_pause_inner(
        &mut self,
        agent: &str,
        mode: &str,
        id: &str,
        principal: &str,
        reason: Option<&str>,
        now: i64,
    ) -> Result<GoalView> {
        require_eliot_mode(mode)?;
        if let Some(reason) = reason
            && (reason.trim().is_empty() || reason.len() > MAX_GOAL_OBSERVATION_BYTES)
        {
            return Err(Error::new(
                "invalid",
                "goal pause reason must be nonempty and at most 4 KiB",
            ));
        }
        let payload_digest = canonical::digest(&json!({
            "agent": agent,
            "mode": mode,
            "reason": reason,
        }))?;
        let goal_holder = self.goal_holder.clone();
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if let Some(view) = replay_goal_write(&tx, id, principal, "goal.pause", &payload_digest)? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(view);
        }
        validate_id_age(id, now)?;
        let agent_num = parse_agent_id(agent)?;
        let Some(row) = load_goal_row_tx(&tx, agent_num, mode)? else {
            return Err(Error::new("not_found", "goal was not found"));
        };
        if row.state.status != GoalStatus::Paused && row.state.status != GoalStatus::Active {
            return Err(Error::new(
                "bad_state",
                format!("goal is {} and cannot pause", row.state.status.as_str()),
            ));
        }
        let view = if row.state.status == GoalStatus::Paused {
            goal_view_on(&tx, &goal_holder, row)?
        } else {
            let mut state = row.state;
            state.status = GoalStatus::Paused;
            state.paused_reason = reason.map(ToOwned::to_owned);
            db!(tx.execute(
                "UPDATE goals SET revision = ?1, state = ?2, updated_at = ?3
                 WHERE agent = ?4 AND mode = ?5 AND revision = ?6",
                params![
                    row.revision + 1,
                    encode_json(&state)?,
                    now,
                    agent_num,
                    mode,
                    row.revision,
                ],
            ));
            let updated = load_goal_row_tx(&tx, agent_num, mode)?
                .ok_or_else(|| Error::new("internal", "goal disappeared after pause"))?;
            goal_view_on(&tx, &goal_holder, updated)?
        };
        retain_goal_result(
            &tx,
            id,
            principal,
            "goal.pause",
            &payload_digest,
            &view,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(view)
    }

    fn goal_clear_inner(
        &mut self,
        agent: &str,
        mode: &str,
        id: &str,
        principal: &str,
        now: i64,
    ) -> Result<GoalView> {
        require_eliot_mode(mode)?;
        let payload_digest = canonical::digest(&json!({
            "agent": agent,
            "mode": mode,
        }))?;
        let goal_holder = self.goal_holder.clone();
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if let Some(view) = replay_goal_write(&tx, id, principal, "goal.clear", &payload_digest)? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(view);
        }
        validate_id_age(id, now)?;
        let agent_num = parse_agent_id(agent)?;
        let Some(row) = load_goal_row_tx(&tx, agent_num, mode)? else {
            return Err(Error::new("not_found", "goal was not found"));
        };
        let view = if row.state.status == GoalStatus::Cleared {
            goal_view_on(&tx, &goal_holder, row)?
        } else {
            let state = GoalStatePersist {
                status: GoalStatus::Cleared,
                paused_reason: None,
                blocked_reason: None,
                counters: row.state.counters,
                cadence_anchor_ms: row.state.cadence_anchor_ms,
                last_committed_slot: row.state.last_committed_slot,
                last_continuation_command_id: None,
            };
            db!(tx.execute(
                "UPDATE goals SET revision = ?1, state = ?2, updated_at = ?3
             WHERE agent = ?4 AND mode = ?5 AND revision = ?6",
                params![
                    row.revision + 1,
                    encode_json(&state)?,
                    now,
                    agent_num,
                    mode,
                    row.revision,
                ],
            ));
            let updated = load_goal_row_tx(&tx, agent_num, mode)?
                .ok_or_else(|| Error::new("internal", "goal disappeared after clear"))?;
            goal_view_on(&tx, &goal_holder, updated)?
        };
        retain_goal_result(
            &tx,
            id,
            principal,
            "goal.clear",
            &payload_digest,
            &view,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(view)
    }

    fn goal_resume_inner(
        &mut self,
        agent: &str,
        mode: &str,
        id: &str,
        principal: &str,
        now: i64,
    ) -> Result<GoalView> {
        require_eliot_mode(mode)?;
        let payload_digest = canonical::digest(&json!({
            "agent": agent,
            "mode": mode,
        }))?;
        let goal_holder = self.goal_holder.clone();
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if let Some(view) = replay_goal_write(&tx, id, principal, "goal.resume", &payload_digest)? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(view);
        }
        validate_id_age(id, now)?;
        let agent_num = parse_agent_id(agent)?;
        let Some(row) = load_goal_row_tx(&tx, agent_num, mode)? else {
            return Err(Error::new("not_found", "goal was not found"));
        };
        let view = if row.state.status == GoalStatus::Active {
            goal_view_on(&tx, &goal_holder, row)?
        } else {
            if row.state.status != GoalStatus::Paused {
                return Err(Error::new(
                    "bad_state",
                    format!(
                        "goal is {} and resumes only via set",
                        row.state.status.as_str()
                    ),
                ));
            }
            let agent_row = load_agent_by_num(&tx, agent_num)?
                .ok_or_else(|| Error::new("not_found", "goal Agent was not found"))?;
            if agent_row.observe_only {
                return Err(Error::new(
                    "not_allowed",
                    "observe-only Agents take no Goals",
                ));
            }
            if row.generation != agent_row.generation {
                return Err(Error::new(
                    "stale_generation",
                    "goal binds a past Agent generation; set a new goal",
                ));
            }
            let mut state = row.state;
            state.status = GoalStatus::Active;
            state.paused_reason = None;
            db!(tx.execute(
                "UPDATE goals SET revision = ?1, state = ?2, updated_at = ?3
             WHERE agent = ?4 AND mode = ?5 AND revision = ?6",
                params![
                    row.revision + 1,
                    encode_json(&state)?,
                    now,
                    agent_num,
                    mode,
                    row.revision,
                ],
            ));
            let updated = load_goal_row_tx(&tx, agent_num, mode)?
                .ok_or_else(|| Error::new("internal", "goal disappeared after resume"))?;
            goal_view_on(&tx, &goal_holder, updated)?
        };
        retain_goal_result(
            &tx,
            id,
            principal,
            "goal.resume",
            &payload_digest,
            &view,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(view)
    }

    fn pause_goal_tx(
        &mut self,
        agent_num: i64,
        row: &GoalRow,
        reason: &str,
        now: i64,
    ) -> Result<GoalView> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let state = GoalStatePersist {
            status: GoalStatus::Paused,
            paused_reason: Some(reason.to_owned()),
            blocked_reason: row.state.blocked_reason.clone(),
            counters: row.state.counters.clone(),
            cadence_anchor_ms: row.state.cadence_anchor_ms,
            last_committed_slot: row.state.last_committed_slot,
            last_continuation_command_id: row.state.last_continuation_command_id.clone(),
        };
        let state_text = encode_json(&state)?;
        db!(tx.execute(
            "UPDATE goals SET revision = ?1, state = ?2, updated_at = ?3
             WHERE agent = ?4 AND mode = ?5 AND revision = ?6",
            params![
                row.revision + 1,
                state_text,
                now,
                agent_num,
                row.mode,
                row.revision,
            ],
        ));
        commit_transaction(tx, &self.write_faulted)?;
        let row = load_goal_row(&self.connection, agent_num, &row.mode)?
            .ok_or_else(|| Error::new("internal", "goal disappeared after pause"))?;
        self.goal_view(row)
    }

    fn goal_claim_round_inner(
        &mut self,
        principal: &str,
        request: &GoalRoundRequest,
        expected_revision: i64,
        expected_spent: bool,
        now: i64,
    ) -> Result<GoalRoundClaim> {
        let agent = request.agent.as_str();
        let mode = request.mode.as_str();
        let key = goal_request_key(request)?;
        require_eliot_mode(mode)?;
        require_nonempty(principal, "principal")?;
        validate_ulid(&request.id)?;
        require_key_range(key)?;
        let agent_num = parse_agent_id(agent)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if let Some(replay) = replay_goal_round_tx(&tx, principal, request, &self.goal_holder, now)?
        {
            commit_transaction(tx, &self.write_faulted)?;
            return replay.map(|response| GoalRoundClaim::Replay(Box::new(response)));
        }
        let agent_row = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "goal Agent was not found"))?;
        if agent_row.observe_only {
            return Err(Error::new(
                "not_allowed",
                "observe-only Agents take no Goal rounds",
            ));
        }
        let Some(row) = load_goal_row_tx(&tx, agent_num, mode)? else {
            return Err(Error::new("not_found", "goal was not found"));
        };
        if row.generation != agent_row.generation {
            if row.state.status == GoalStatus::Active {
                let state = GoalStatePersist {
                    status: GoalStatus::Paused,
                    paused_reason: Some("Agent generation advanced".to_owned()),
                    blocked_reason: row.state.blocked_reason.clone(),
                    counters: row.state.counters.clone(),
                    cadence_anchor_ms: row.state.cadence_anchor_ms,
                    last_committed_slot: row.state.last_committed_slot,
                    last_continuation_command_id: row.state.last_continuation_command_id.clone(),
                };
                db!(tx.execute(
                    "UPDATE goals SET revision = ?1, state = ?2, updated_at = ?3
                     WHERE agent = ?4 AND mode = ?5 AND revision = ?6",
                    params![
                        row.revision + 1,
                        encode_json(&state)?,
                        now,
                        agent_num,
                        mode,
                        row.revision,
                    ],
                ));
                commit_transaction(tx, &self.write_faulted)?;
            }
            return Err(Error::new(
                "stale_generation",
                format!(
                    "goal binds generation {} but Agent {} is at generation {}",
                    row.generation, agent, agent_row.generation
                ),
            ));
        }
        if row.state.status != GoalStatus::Active {
            return Err(Error::new(
                "bad_state",
                format!("goal is {} and takes no rounds", row.state.status.as_str()),
            ));
        }
        if row.revision != expected_revision {
            return Err(Error::new(
                "stale_round",
                "goal revision changed since the probe",
            ));
        }
        // A crashed holder's `running` rows read as `unknown`; rewrite them
        // durably so later readers need no holder comparison.
        db!(tx.execute(
            "UPDATE goal_markers SET state = 'unknown', updated_at = ?1
             WHERE agent = ?2 AND mode = ?3 AND state = 'running' AND holder <> ?4",
            params![now, agent_num, mode, self.goal_holder],
        ));
        let markers = load_goal_markers_tx(&tx, agent_num, mode)?;
        if markers
            .iter()
            .any(|marker| marker.is_live(&self.goal_holder))
        {
            return Err(Error::new("busy", "goal round already in flight"));
        }
        // The burn ignores revision: pause/clear/resume bump it but never
        // unburn an unknown round key of this generation.
        if row.last_round_key.as_deref() == Some(&key.text())
            || markers.iter().any(|marker| {
                marker.round_key == key.value()
                    && marker.kind == key.kind()
                    && marker.generation == row.generation
            })
        {
            return Err(Error::new("stale_round", "goal round key was already used"));
        }
        require_key_form(&row, key)?;
        if agent_row.state != AgentState::Idle {
            return Err(Error::new("bad_state", "goal round needs an idle Agent"));
        }
        if agent_row.active_turn.is_some() {
            return Err(Error::new("bad_state", "goal round needs no active turn"));
        }
        if !agent_row.pending.is_empty() {
            return Err(Error::new(
                "bad_state",
                "goal round needs no pending requests",
            ));
        }
        if let Some(source) = first_unhealthy_open_source_tx(&tx, agent_num, row.generation)? {
            return Err(Error::new(
                "bad_state",
                format!("goal round needs healthy harness sources; {source} is not healthy"),
            ));
        }
        if count_agent_commands(&tx, agent_num, row.generation, true)? > 0 {
            return Err(Error::new(
                "bad_state",
                "resolve unknown commands before the goal round",
            ));
        }
        if count_agent_commands(&tx, agent_num, row.generation, false)? > 0 {
            return Err(Error::new("busy", "Agent has open commands"));
        }
        // Re-validate the exact spent-budget binding in this transaction:
        // a probe whose qualification went stale before the claim (a turn
        // ran after the probe) must not act. Refusing inserts no marker,
        // so the round key stays usable with a fresh probe.
        let live_spent = goal_continuation_spent_budget(
            &tx,
            agent_num,
            row.generation,
            row.state.counters.turns_at_set,
            row.state.counters.turns_used,
            row.state.last_continuation_command_id.as_deref(),
            count_turns_tx(&tx, agent_num, row.generation)?,
        )?;
        if live_spent != expected_spent {
            return Err(Error::new(
                "stale_round",
                "goal turn budget state changed since the probe",
            ));
        }
        let claim_seq = max_agent_seq_tx(&tx, agent_num, row.generation)?;
        match key {
            GoalRoundKey::Idle(idle_seq) => {
                if !round_key_names_generation_event(&tx, agent_num, row.generation, idle_seq)? {
                    return Err(Error::new(
                        "stale_round",
                        "round key does not name an idle event of this Agent generation",
                    ));
                }
                if claim_seq != idle_seq {
                    return Err(Error::new(
                        "stale_round",
                        "round key is not the latest Agent event",
                    ));
                }
            }
            GoalRoundKey::Slot(slot) => {
                require_slot_due(&row, slot, now)?;
            }
        }
        db!(tx.execute(
            "INSERT INTO goal_markers(agent, mode, generation, revision, round_key,
                 round_key_kind, holder, state, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'running', ?8)",
            params![
                agent_num,
                mode,
                row.generation,
                row.revision,
                key.value(),
                key.kind(),
                self.goal_holder,
                now,
            ],
        ));
        let continuation_id = Ulid::new().to_string();
        if command_binding(&tx, &continuation_id)?.is_some() || write_exists(&tx, &continuation_id)?
        {
            return Err(Error::new(
                "id_conflict",
                "goal continuation id is already used",
            ));
        }
        let record = GoalRoundRecord {
            request: request.clone(),
            generation: row.generation,
            revision: row.revision,
            key,
            holder: self.goal_holder.clone(),
            claim_seq,
            continuation_id: continuation_id.clone(),
            outcome: StoredGoalRoundOutcome::Running,
        };
        let mut recovery = record.clone();
        recovery.outcome = StoredGoalRoundOutcome::Failed(unknown_goal_round_error());
        if encode_json(&recovery)?.len() > MAX_FRAME_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "goal.round recovery exceeds the frame bound",
            ));
        }
        retain_goal_result(
            &tx,
            &request.id,
            principal,
            "goal.round",
            &goal_round_digest(request)?,
            &record,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(GoalRoundClaim::Claimed {
            seq: claim_seq,
            continuation_id,
        })
    }

    fn goal_commit_round_inner(
        &mut self,
        commit: &GoalRoundCommit,
        now: i64,
    ) -> Result<GoalRoundCommitResult> {
        let request = GoalRoundRequest {
            id: commit.id.clone(),
            agent: commit.agent.clone(),
            mode: GoalMode::parse(&commit.mode)?,
            idle_seq: match commit.round_key {
                GoalRoundKey::Idle(seq) => Some(seq),
                _ => None,
            },
            slot: match commit.round_key {
                GoalRoundKey::Slot(slot) => Some(slot),
                _ => None,
            },
        };
        require_eliot_mode(&commit.mode)?;
        let agent_num = parse_agent_id(&commit.agent)?;
        require_key_range(commit.round_key)?;
        if commit.claim_seq < 0 {
            return Err(Error::new(
                "invalid",
                "goal round claim seq must be nonnegative",
            ));
        }
        if let Some(continuation) = &commit.continuation {
            validate_ulid(&continuation.id)?;
            require_nonempty(&continuation.text, "continuation text")?;
            check_input(&continuation.text)?;
        }
        if commit.check.is_none() && commit.continuation.is_some() {
            return Err(Error::new(
                "internal",
                "goal continuation requires a check result",
            ));
        }
        if commit.check.is_some() && commit.blocked_reason.is_some() {
            return Err(Error::new(
                "internal",
                "checked goal rounds decide their own block",
            ));
        }
        if commit
            .check
            .as_ref()
            .is_some_and(|check| check.exit_code.is_none())
        {
            return Err(Error::new(
                "unknown",
                "goal check without an exit code has no committable outcome",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let mut record = replay_goal_write::<GoalRoundRecord>(
            &tx,
            &request.id,
            &commit.principal,
            "goal.round",
            &goal_round_digest(&request)?,
        )?
        .ok_or_else(|| Error::new("unknown", "goal round reservation is no longer retained"))?;
        if !matches!(record.outcome, StoredGoalRoundOutcome::Running)
            || record.holder != self.goal_holder
        {
            let replay =
                replay_goal_round_tx(&tx, &commit.principal, &request, &self.goal_holder, now)?
                    .ok_or_else(|| {
                        Error::new("unknown", "goal round reservation is no longer retained")
                    })?;
            commit_transaction(tx, &self.write_faulted)?;
            return replay.and_then(GoalRoundCommitResult::replay);
        }
        if record.generation != commit.expected_generation
            || record.revision != commit.expected_revision
            || record.claim_seq != commit.claim_seq
            || record.key != commit.round_key
            || commit
                .continuation
                .as_ref()
                .is_some_and(|c| c.id != record.continuation_id)
        {
            return Err(Error::new(
                "stale_round",
                "goal round commit does not match its reservation",
            ));
        }
        let Some(row) = load_goal_row_tx(&tx, agent_num, &commit.mode)? else {
            return Err(Error::new("not_found", "goal was not found"));
        };
        if row.state.status != GoalStatus::Active {
            return Err(Error::new(
                "bad_state",
                format!("goal is {} and takes no rounds", row.state.status.as_str()),
            ));
        }
        if row.generation != commit.expected_generation {
            return Err(Error::new(
                "stale_generation",
                "goal generation no longer matches",
            ));
        }
        if row.revision != commit.expected_revision {
            return Err(Error::new(
                "stale_round",
                "goal revision changed during the round",
            ));
        }
        if row.last_round_key.as_deref() == Some(&commit.round_key.text()) {
            return Err(Error::new(
                "stale_round",
                "goal round was already committed",
            ));
        }
        let marker = load_goal_marker_tx(
            &tx,
            agent_num,
            &commit.mode,
            commit.expected_generation,
            commit.expected_revision,
            commit.round_key,
        )?;
        if marker
            .as_ref()
            .is_none_or(|marker| !marker.is_live(&self.goal_holder))
        {
            return Err(Error::new("stale_round", "goal round is not in flight"));
        }
        match commit.round_key {
            GoalRoundKey::Idle(idle_seq) => {
                if !round_key_names_generation_event(&tx, agent_num, row.generation, idle_seq)? {
                    return Err(Error::new(
                        "stale_round",
                        "round key does not name an idle event of this Agent generation",
                    ));
                }
                if max_agent_seq_tx(&tx, agent_num, row.generation)? != idle_seq {
                    return Err(Error::new(
                        "stale_round",
                        "Agent events advanced past the round key",
                    ));
                }
            }
            GoalRoundKey::Slot(slot) => {
                require_slot_due(&row, slot, now)?;
                if max_agent_seq_tx(&tx, agent_num, row.generation)? != commit.claim_seq {
                    return Err(Error::new(
                        "stale_round",
                        "Agent events advanced during the round",
                    ));
                }
            }
        }
        if let Some(source) = first_unhealthy_open_source_tx(&tx, agent_num, row.generation)? {
            return Err(Error::new(
                "stale_round",
                format!("source {source} left healthy during the round"),
            ));
        }
        let agent_row = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "goal Agent was not found"))?;
        if count_agent_commands(&tx, agent_num, row.generation, true)? > 0 {
            return Err(Error::new(
                "stale_round",
                "an unknown command appeared during the round",
            ));
        }
        if count_agent_commands(&tx, agent_num, row.generation, false)? > 0 {
            return Err(Error::new(
                "stale_round",
                "a command opened during the round",
            ));
        }
        if agent_row.active_turn.is_some() {
            return Err(Error::new("stale_round", "a turn started during the round"));
        }
        if !agent_row.pending.is_empty() {
            return Err(Error::new(
                "stale_round",
                "a request opened during the round",
            ));
        }
        let turn_count = count_turns_tx(&tx, agent_num, row.generation)?;
        let mut counters = row.state.counters.clone();
        counters.turns_used = counters
            .turns_used
            .max(turn_count.saturating_sub(counters.turns_at_set));
        let mut admission: Option<Admission> = None;
        let outcome = if let Some(check) = &commit.check {
            counters.rounds += 1;
            counters.last_exit_code = check.exit_code;
            counters.last_check_at = Some(now);
            counters.last_observation = check.observation.clone();
            if check.output_complete
                && let Some(digest) = &check.output_digest
            {
                let same_output = counters.last_output_digest.as_deref() == Some(digest.as_str());
                match goal_fingerprint_tx(&tx, agent_num, row.generation)? {
                    GoalFingerprint::None => {
                        // No Git observation exists for this generation;
                        // check output alone decides the streak.
                        if same_output {
                            counters.consecutive_same_output += 1;
                        } else {
                            counters.consecutive_same_output = 0;
                        }
                    }
                    GoalFingerprint::Incomplete => {
                        // An incomplete fingerprint never proves no progress.
                        counters.consecutive_same_output = 0;
                        counters.last_fingerprint = None;
                    }
                    GoalFingerprint::Complete(fingerprint) => {
                        let same_fingerprint =
                            counters.last_fingerprint.as_deref() == Some(fingerprint.as_str());
                        if same_output && same_fingerprint {
                            counters.consecutive_same_output += 1;
                        } else {
                            counters.consecutive_same_output = 0;
                        }
                        counters.last_fingerprint = Some(fingerprint);
                    }
                }
                counters.last_output_digest = Some(digest.clone());
            }
            if check.exit_code == Some(0) {
                GoalCommitOutcome::Completed
            } else if let Some(reason) = goal_postcheck_block(&row.definition, &counters) {
                GoalCommitOutcome::Blocked { reason }
            } else if let Some(continuation) = &commit.continuation {
                let request = SubmitCommand {
                    id: continuation.id.clone(),
                    target: Some(commit.agent.clone()),
                    selector: None,
                    verb: "send".to_owned(),
                    args: json!({"text": continuation.text}),
                    expected_generation: Some(row.generation),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                };
                let limits = self.hierarchy_limits;
                if command_binding(&tx, &request.id)?.is_some() || write_exists(&tx, &request.id)? {
                    return Err(Error::new(
                        "id_conflict",
                        "goal continuation id is already used",
                    ));
                }
                let send = admission::submit_tx(
                    &tx,
                    &commit.principal,
                    request,
                    None,
                    now,
                    false,
                    limits,
                )?;
                let outcome = GoalCommitOutcome::Continued {
                    command_id: send.command.id.clone(),
                };
                admission = Some(send);
                outcome
            } else {
                return Err(Error::new(
                    "internal",
                    "failed goal check needs a block reason or a continuation",
                ));
            }
        } else if let Some(reason) = &commit.blocked_reason {
            GoalCommitOutcome::Blocked {
                reason: reason.clone(),
            }
        } else {
            return Err(Error::new(
                "internal",
                "goal round without a check can only block",
            ));
        };
        let mut state = row.state;
        match &outcome {
            GoalCommitOutcome::Completed => {
                state.status = GoalStatus::Completed;
                state.last_continuation_command_id = None;
            }
            GoalCommitOutcome::Blocked { reason } => {
                state.status = GoalStatus::Blocked;
                state.blocked_reason = Some(reason.clone());
                state.last_continuation_command_id = None;
            }
            // Only a newly admitted continuation binds: an idempotent
            // readback of an older command (same round id at a fresh key)
            // must never grant the owned-continuation proof.
            GoalCommitOutcome::Continued { command_id } => {
                state.last_continuation_command_id =
                    if admission.as_ref().is_some_and(|admission| admission.is_new) {
                        Some(command_id.clone())
                    } else {
                        None
                    };
            }
        }
        state.counters = counters;
        if let GoalRoundKey::Slot(slot) = commit.round_key {
            state.last_committed_slot = Some(slot);
        }
        let state_text = encode_json(&state)?;
        db!(tx.execute(
            "DELETE FROM goal_markers
             WHERE agent = ?1 AND mode = ?2 AND generation = ?3 AND revision = ?4
               AND round_key = ?5 AND round_key_kind = ?6",
            params![
                agent_num,
                commit.mode,
                commit.expected_generation,
                commit.expected_revision,
                commit.round_key.value(),
                commit.round_key.kind(),
            ],
        ));
        db!(tx.execute(
            "UPDATE goals SET revision = ?1, state = ?2, last_round_key = ?3,
                 updated_at = ?4
             WHERE agent = ?5 AND mode = ?6 AND revision = ?7",
            params![
                row.revision + 1,
                state_text,
                commit.round_key.text(),
                now,
                agent_num,
                commit.mode,
                row.revision,
            ],
        ));
        let row = load_goal_row_tx(&tx, agent_num, &commit.mode)?
            .ok_or_else(|| Error::new("internal", "goal disappeared after round"))?;
        let view = goal_view_on(&tx, &self.goal_holder, row)?;
        let (wire_outcome, reason, command_id) = match &outcome {
            GoalCommitOutcome::Completed => (GoalRoundOutcome::Completed, None, None),
            GoalCommitOutcome::Blocked { reason } => {
                (GoalRoundOutcome::Blocked, Some(reason.clone()), None)
            }
            GoalCommitOutcome::Continued { command_id } => {
                (GoalRoundOutcome::Continued, None, Some(command_id.clone()))
            }
        };
        let response = GoalRoundResponse {
            id: request.id.clone(),
            outcome: wire_outcome,
            reason,
            command_id,
            view: view.clone(),
            continuation: admission.as_ref().map(|a| CommandSubmitResponse {
                command_id: a.command.id.clone(),
                agent: a.command.target.clone(),
                generation: a.command.generation,
                state: a.command.state,
                outcome: a.command.outcome.clone(),
                linked_command: None,
            }),
        };
        request.validate_response(&response)?;
        record.outcome = StoredGoalRoundOutcome::Finished(Box::new(response.clone()));
        update_goal_round_tx(&tx, &record)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(GoalRoundCommitResult {
            outcome,
            view,
            admission,
            response,
        })
    }
}

/// One continuation command row as the probe reads it. Only the fields
/// that bind it to this Goal's Agent, generation and completed turn.
struct ContinuationCommand {
    target: Option<i64>,
    generation: Option<i64>,
    turn_id: Option<String>,
    state: String,
    verb: String,
}

/// Whether the goal's turn budget was spent solely by its own last
/// admitted continuation. Qualifies only when exactly one turn ran since
/// the last round commit (the stored `turns_used` is that commit's live
/// count) and that turn is the pending continuation's completed turn,
/// resolved through its durable command id — two bounded point lookups,
/// never an event scan. Anything else (no binding, another turn after
/// the continuation, an unfinished or unknown turn) does not qualify.
fn goal_continuation_spent_budget(
    connection: &Connection,
    agent_num: i64,
    generation: i64,
    turns_at_set: i64,
    stored_turns_used: i64,
    continuation_command_id: Option<&str>,
    turn_count: i64,
) -> Result<bool> {
    let Some(command_id) = continuation_command_id else {
        return Ok(false);
    };
    if turn_count.saturating_sub(turns_at_set) != stored_turns_used.saturating_add(1) {
        return Ok(false);
    }
    let command: Option<ContinuationCommand> = connection
        .query_row(
            "SELECT target, generation, turn_id, state, verb FROM commands WHERE id = ?1",
            params![command_id],
            |row| {
                Ok(ContinuationCommand {
                    target: row.get(0)?,
                    generation: row.get(1)?,
                    turn_id: row.get(2)?,
                    state: row.get(3)?,
                    verb: row.get(4)?,
                })
            },
        )
        .optional()
        .map_err(sql_error)?;
    let Some(command) = command else {
        return Ok(false);
    };
    if command.target != Some(agent_num) || command.state != "applied" || command.verb != "send" {
        return Ok(false);
    }
    // A missing command generation is not an exact binding.
    if command.generation != Some(generation) {
        return Ok(false);
    }
    let Some(turn_id) = command.turn_id else {
        return Ok(false);
    };
    let turn_state: Option<String> = connection
        .query_row(
            "SELECT state FROM turns WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3",
            params![agent_num, generation, turn_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    Ok(turn_state.as_deref() == Some("completed"))
}

/// Decide a failed check against the frozen limits. Success always
/// completes; otherwise the round budget, turn budget and no-progress streak
/// block in that order. Counters are already updated for this check.
fn goal_postcheck_block(definition: &GoalDefinition, counters: &GoalCounters) -> Option<String> {
    if definition
        .max_rounds
        .is_some_and(|max| counters.rounds >= max)
    {
        return Some("round_budget".to_owned());
    }
    if definition
        .max_turns
        .is_some_and(|max| counters.turns_used >= max)
    {
        return Some("turn_limit".to_owned());
    }
    if definition
        .max_without_progress
        .is_some_and(|max| counters.consecutive_same_output >= max)
    {
        return Some("no_progress".to_owned());
    }
    None
}

fn require_eliot_mode(mode: &str) -> Result<()> {
    if mode != "eliot" {
        return Err(Error::new(
            "unsupported",
            "native Goal mode is not supported by the installed harness plugins",
        ));
    }
    Ok(())
}

fn require_key_range(key: GoalRoundKey) -> Result<()> {
    match key {
        GoalRoundKey::Idle(seq) => {
            if seq < 0 {
                return Err(Error::new(
                    "invalid",
                    "goal round idle_seq must be nonnegative",
                ));
            }
        }
        GoalRoundKey::Slot(slot) => {
            if slot < 1 {
                return Err(Error::new("invalid", "goal round slot must be at least 1"));
            }
        }
    }
    Ok(())
}

/// The key form must match the Goal: cadenced Goals round on slots, the
/// rest on idle seqs. A mismatch is a discipline refusal, not a stale key.
fn require_key_form(row: &GoalRow, key: GoalRoundKey) -> Result<()> {
    match (row.definition.cadence_ms, key) {
        (Some(_), GoalRoundKey::Idle(_)) => Err(Error::new(
            "bad_state",
            "cadenced goal rounds name cadence slots, not idle events",
        )),
        (None, GoalRoundKey::Slot(_)) => Err(Error::new(
            "bad_state",
            "goal without a cadence rounds on idle events, not slots",
        )),
        _ => Ok(()),
    }
}

/// Start of slot `slot` (1-based) on the Goal's anchored grid. Like timer
/// intervals, the first slot starts one period after the anchor.
fn slot_start_ms(anchor_ms: i64, period_ms: i64, slot: i64) -> Result<i64> {
    let start = i128::from(anchor_ms) + i128::from(period_ms) * i128::from(slot);
    i64::try_from(start)
        .map_err(|_| Error::new("invalid", "goal cadence slot overflows epoch millis"))
}

/// A slot round names a due slot newer than the last committed one.
/// Due slots never un-due, so the commit recheck only guards clock
/// rollback. Called at probe, claim and commit alike.
fn require_slot_due(row: &GoalRow, slot: i64, now: i64) -> Result<()> {
    if row
        .state
        .last_committed_slot
        .is_some_and(|committed| slot <= committed)
    {
        return Err(Error::new(
            "stale_round",
            "goal cadence slot is not newer than the last committed slot",
        ));
    }
    let (Some(period_ms), Some(anchor_ms)) =
        (row.definition.cadence_ms, row.state.cadence_anchor_ms)
    else {
        return Err(Error::new("internal", "cadenced goal lost its anchor"));
    };
    if slot_start_ms(anchor_ms, period_ms, slot)? > now {
        return Err(Error::new(
            "stale_round",
            "goal cadence slot is not due yet",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct GoalMarkerRow {
    generation: i64,
    round_key: i64,
    kind: String,
    holder: String,
    state: String,
}

impl GoalMarkerRow {
    /// Live only for the holder that claimed it. A `running` row from a
    /// crashed holder reads as `unknown` everywhere.
    fn is_live(&self, holder: &str) -> bool {
        self.state == "running" && self.holder == holder
    }
}

impl Store {
    /// Render one Goal row with a live turn count and its unresolved round
    /// marker (the live in-flight round if any, else this generation's
    /// latest unknown marker).
    fn goal_view(&self, row: GoalRow) -> Result<GoalView> {
        goal_view_on(&self.connection, &self.goal_holder, row)
    }
}

fn goal_view_on(connection: &Connection, goal_holder: &str, row: GoalRow) -> Result<GoalView> {
    let turn_count = count_turns(connection, row.agent_num, row.generation)?;
    let mut counters = row.state.counters.clone();
    counters.turns_used = counters
        .turns_used
        .max(turn_count.saturating_sub(counters.turns_at_set));
    let round_marker = display_goal_marker(
        connection,
        goal_holder,
        row.agent_num,
        &row.mode,
        row.generation,
    )?;
    Ok(GoalView {
        agent: format!("a{}", row.agent_num),
        mode: if row.mode == "eliot" {
            GoalMode::Eliot
        } else {
            GoalMode::Native
        },
        generation: row.generation,
        revision: row.revision,
        cadence_anchor_ms: row.state.cadence_anchor_ms,
        status: row.state.status,
        paused_reason: row.state.paused_reason,
        blocked_reason: row.state.blocked_reason,
        definition: row.definition,
        counters,
        cost_status: GOAL_COST_UNSUPPORTED.to_owned(),
        last_round_key: row.last_round_key,
        round_marker,
        updated_at: row.updated_at,
    })
}

/// The live in-flight marker if one exists, else this generation's
/// latest unknown marker (crashed `running` rows read as `unknown`).
/// Revision bumps never hide it.
fn display_goal_marker(
    connection: &Connection,
    holder: &str,
    agent_num: i64,
    mode: &str,
    generation: i64,
) -> Result<Option<GoalRoundMarker>> {
    let live: Option<(i64, String, i64, i64, i64)> = connection
        .query_row(
            "SELECT round_key, round_key_kind, generation, revision, updated_at
             FROM goal_markers
             WHERE agent = ?1 AND mode = ?2 AND state = 'running' AND holder = ?3
             LIMIT 1",
            params![agent_num, mode, holder],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    if let Some((round_key, kind, generation, revision, updated_at)) = live {
        return Ok(Some(GoalRoundMarker {
            round_key: format!("{kind}:{round_key}"),
            generation,
            revision,
            state: GoalMarkerState::Running,
            updated_at,
        }));
    }
    let burned: Option<(i64, String, i64, i64)> = connection
        .query_row(
            "SELECT round_key, round_key_kind, revision, updated_at FROM goal_markers
             WHERE agent = ?1 AND mode = ?2 AND generation = ?3
               AND (state = 'unknown' OR holder <> ?4)
             ORDER BY updated_at DESC, round_key DESC LIMIT 1",
            params![agent_num, mode, generation, holder],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(sql_error)?;
    Ok(
        burned.map(|(round_key, kind, revision, updated_at)| GoalRoundMarker {
            round_key: format!("{kind}:{round_key}"),
            generation,
            revision,
            state: GoalMarkerState::Unknown,
            updated_at,
        }),
    )
}

fn load_goal_markers(
    connection: &Connection,
    agent_num: i64,
    mode: &str,
) -> Result<Vec<GoalMarkerRow>> {
    let mut statement = connection
        .prepare(
            "SELECT generation, round_key, round_key_kind, holder, state
             FROM goal_markers WHERE agent = ?1 AND mode = ?2",
        )
        .map_err(sql_error)?;
    statement
        .query_map(params![agent_num, mode], |row| {
            Ok(GoalMarkerRow {
                generation: row.get(0)?,
                round_key: row.get(1)?,
                kind: row.get(2)?,
                holder: row.get(3)?,
                state: row.get(4)?,
            })
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<GoalMarkerRow>>>()
        .map_err(sql_error)
}

fn load_goal_markers_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    mode: &str,
) -> Result<Vec<GoalMarkerRow>> {
    let mut statement = tx
        .prepare(
            "SELECT generation, round_key, round_key_kind, holder, state
             FROM goal_markers WHERE agent = ?1 AND mode = ?2",
        )
        .map_err(sql_error)?;
    statement
        .query_map(params![agent_num, mode], |row| {
            Ok(GoalMarkerRow {
                generation: row.get(0)?,
                round_key: row.get(1)?,
                kind: row.get(2)?,
                holder: row.get(3)?,
                state: row.get(4)?,
            })
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<GoalMarkerRow>>>()
        .map_err(sql_error)
}

fn load_goal_marker_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    mode: &str,
    generation: i64,
    revision: i64,
    key: GoalRoundKey,
) -> Result<Option<GoalMarkerRow>> {
    tx.query_row(
        "SELECT generation, round_key, round_key_kind, holder, state
         FROM goal_markers
         WHERE agent = ?1 AND mode = ?2 AND generation = ?3 AND revision = ?4
           AND round_key = ?5 AND round_key_kind = ?6",
        params![
            agent_num,
            mode,
            generation,
            revision,
            key.value(),
            key.kind()
        ],
        |row| {
            Ok(GoalMarkerRow {
                generation: row.get(0)?,
                round_key: row.get(1)?,
                kind: row.get(2)?,
                holder: row.get(3)?,
                state: row.get(4)?,
            })
        },
    )
    .optional()
    .map_err(sql_error)
}

/// The round key must name an idle event of this Agent generation: the
/// event-time subject snapshot shows the Agent idle. A missing row, a row
/// of another Agent/generation, or a non-idle subject is a stale key.
fn round_key_names_generation_event(
    connection: &Connection,
    agent_num: i64,
    generation: i64,
    seq: i64,
) -> Result<bool> {
    connection
        .query_row(
            "SELECT agent, generation, subject FROM events WHERE seq = ?1",
            params![seq],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)
        .map(|found| {
            found.is_some_and(|(agent, row_generation, subject)| {
                agent == Some(agent_num)
                    && row_generation == Some(generation)
                    && subject_is_idle(subject.as_deref())
            })
        })
}

/// An event-time subject snapshot proves the Agent was idle at that
/// event. A missing or unreadable subject proves nothing.
fn subject_is_idle(subject: Option<&str>) -> bool {
    subject.is_some_and(|text| {
        serde_json::from_str::<EventSubject>(text)
            .map(|subject| subject.state == AgentState::Idle)
            .unwrap_or(false)
    })
}

/// Read-only workspace fingerprint over committed Git observation: the
/// latest complete `git.changed` HEAD plus dirty set, and the attributed
/// edit paths. Pure read; it never touches the workspace.
enum GoalFingerprint {
    /// No Git observation exists for this generation: the streak falls
    /// back to check output alone.
    None,
    /// A Git binding exists but its observation is missing, partial,
    /// gapped, truncated or unhealthy. Never proves no progress.
    Incomplete,
    /// Canonical digest of HEAD, dirty set and attributed edit paths.
    Complete(String),
}

fn goal_fingerprint_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
) -> Result<GoalFingerprint> {
    let owners: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM source_owners WHERE agent = ?1 AND generation = ?2
             AND family IN ('git', 'file') AND source = 'git' AND mechanism = 'git'",
            params![agent_num, generation],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if owners == 0 {
        return Ok(GoalFingerprint::None);
    }
    if owners != 2 {
        return Ok(GoalFingerprint::Incomplete);
    }
    let healthy: bool = tx
        .query_row(
            "SELECT state = 'open' AND health = 'ok' FROM source_state
             WHERE agent = ?1 AND generation = ?2 AND source = 'git'",
            params![agent_num, generation],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?
        .unwrap_or(false);
    if !healthy {
        return Ok(GoalFingerprint::Incomplete);
    }
    let data: Option<String> = tx
        .query_row(
            "SELECT data FROM events WHERE agent = ?1 AND generation = ?2
             AND source = 'git' AND kind = 'git.changed' ORDER BY seq DESC LIMIT 1",
            params![agent_num, generation],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    let Some(data) = data else {
        return Ok(GoalFingerprint::Incomplete);
    };
    let data = decode_json(&data)?;
    if data.get("complete").and_then(Value::as_bool) != Some(true)
        || data.get("history_gap").and_then(Value::as_bool) == Some(true)
    {
        return Ok(GoalFingerprint::Incomplete);
    }
    let mut statement = tx
        .prepare(
            "SELECT path FROM agent_edits WHERE agent = ?1 AND generation = ?2
             ORDER BY path LIMIT ?3",
        )
        .map_err(sql_error)?;
    let paths = statement
        .query_map(
            params![agent_num, generation, MAX_PAGE_ENTRIES as i64 + 1],
            |row| row.get::<_, String>(0),
        )
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<String>>>()
        .map_err(sql_error)?;
    if paths.len() > MAX_PAGE_ENTRIES {
        return Ok(GoalFingerprint::Incomplete);
    }
    let digest = canonical::digest(&json!({
        "head": data.get("head").unwrap_or(&Value::Null),
        "dirty": data.get("dirty").unwrap_or(&Value::Null),
        "edits": paths,
    }))?;
    Ok(GoalFingerprint::Complete(
        digest.iter().map(|byte| format!("{byte:02x}")).collect(),
    ))
}

fn load_goal_row(connection: &Connection, agent_num: i64, mode: &str) -> Result<Option<GoalRow>> {
    connection
        .query_row(
            "SELECT agent, mode, generation, revision, definition, state,
                    last_round_key, updated_at
             FROM goals WHERE agent = ?1 AND mode = ?2",
            params![agent_num, mode],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?
        .map(
            |(agent, mode, generation, revision, definition, state, last_round_key, updated_at)| {
                let definition: GoalDefinition =
                    serde_json::from_str(&definition).map_err(|error| {
                        Error::new("internal", format!("invalid stored goal: {error}"))
                    })?;
                let state: GoalStatePersist = serde_json::from_str(&state).map_err(|error| {
                    Error::new("internal", format!("invalid stored goal state: {error}"))
                })?;
                Ok(GoalRow {
                    agent_num: agent,
                    mode,
                    generation,
                    revision,
                    definition,
                    state,
                    last_round_key,
                    updated_at,
                })
            },
        )
        .transpose()
}

fn load_goal_row_tx(tx: &Transaction<'_>, agent_num: i64, mode: &str) -> Result<Option<GoalRow>> {
    tx.query_row(
        "SELECT agent, mode, generation, revision, definition, state,
                last_round_key, updated_at
         FROM goals WHERE agent = ?1 AND mode = ?2",
        params![agent_num, mode],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, i64>(7)?,
            ))
        },
    )
    .optional()
    .map_err(sql_error)?
    .map(
        |(agent, mode, generation, revision, definition, state, last_round_key, updated_at)| {
            let definition: GoalDefinition = serde_json::from_str(&definition)
                .map_err(|error| Error::new("internal", format!("invalid stored goal: {error}")))?;
            let state: GoalStatePersist = serde_json::from_str(&state).map_err(|error| {
                Error::new("internal", format!("invalid stored goal state: {error}"))
            })?;
            Ok(GoalRow {
                agent_num: agent,
                mode,
                generation,
                revision,
                definition,
                state,
                last_round_key,
                updated_at,
            })
        },
    )
    .transpose()
}

fn count_turns(connection: &Connection, agent_num: i64, generation: i64) -> Result<i64> {
    connection
        .query_row(
            "SELECT COUNT(*) FROM turns WHERE agent = ?1 AND generation = ?2",
            params![agent_num, generation],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

fn count_turns_tx(tx: &Transaction<'_>, agent_num: i64, generation: i64) -> Result<i64> {
    tx.query_row(
        "SELECT COUNT(*) FROM turns WHERE agent = ?1 AND generation = ?2",
        params![agent_num, generation],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn max_agent_seq(connection: &Connection, agent_num: i64, generation: i64) -> Result<i64> {
    connection
        .query_row(
            "SELECT COALESCE(MAX(seq), 0) FROM events WHERE agent = ?1 AND generation = ?2",
            params![agent_num, generation],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

fn max_agent_seq_tx(tx: &Transaction<'_>, agent_num: i64, generation: i64) -> Result<i64> {
    tx.query_row(
        "SELECT COALESCE(MAX(seq), 0) FROM events WHERE agent = ?1 AND generation = ?2",
        params![agent_num, generation],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn unhealthy_open_sources(
    connection: &Connection,
    agent_num: i64,
    generation: i64,
) -> Result<Vec<String>> {
    let mut statement = connection
        .prepare(
            "SELECT source FROM source_state
             WHERE agent = ?1 AND generation = ?2 AND state = 'open' AND health <> 'ok'
               AND source NOT IN ('daemon', 'os', 'git', 'workspace_process')
             ORDER BY source",
        )
        .map_err(sql_error)?;
    statement
        .query_map(params![agent_num, generation], |row| row.get(0))
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<String>>>()
        .map_err(sql_error)
}

fn first_unhealthy_open_source_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
) -> Result<Option<String>> {
    tx.query_row(
        "SELECT source FROM source_state
         WHERE agent = ?1 AND generation = ?2 AND state = 'open' AND health <> 'ok'
           AND source NOT IN ('daemon', 'os', 'git', 'workspace_process')
         ORDER BY source LIMIT 1",
        params![agent_num, generation],
        |row| row.get(0),
    )
    .optional()
    .map_err(sql_error)
}

fn count_agent_commands(
    connection: &Connection,
    agent_num: i64,
    generation: i64,
    unknown_only: bool,
) -> Result<i64> {
    let states = if unknown_only {
        "state = 'unknown'"
    } else {
        "state IN ('accepted', 'queued', 'dispatched')
         AND NOT (verb = 'inject' AND state IN ('accepted', 'queued') AND outcome IS NULL
                  AND json_extract(args, '$.point') IN ('next_input', 'turn_safe_point'))"
    };
    let sql = format!(
        "SELECT COUNT(*) FROM commands WHERE target = ?1 AND {states}
         AND (generation IS NULL OR generation = ?2)"
    );
    connection
        .query_row(&sql, params![agent_num, generation], |row| row.get(0))
        .map_err(sql_error)
}
