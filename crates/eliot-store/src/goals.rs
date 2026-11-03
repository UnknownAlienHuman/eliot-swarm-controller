//! Durable Eliot-mode Goals: revisioned definitions on one Agent
//! generation plus the stale-round-safe conditional round commit.

use super::*;
use eliot_core::api::{
    GOAL_COST_UNSUPPORTED, GoalCounters, GoalDefinition, GoalMarkerState, GoalMode,
    GoalRoundMarker, GoalStatus, GoalView, MAX_GOAL_OBSERVATION_BYTES,
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

#[derive(Clone, Debug)]
pub struct GoalRoundCommit {
    pub agent: String,
    pub mode: String,
    pub expected_generation: i64,
    pub expected_revision: i64,
    pub round_key: i64,
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
        definition: &GoalDefinition,
        now: i64,
    ) -> Result<GoalView> {
        self.ensure_writer_healthy()?;
        definition.validate()?;
        let result = self.goal_set_inner(agent, mode, definition, now);
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
        reason: Option<&str>,
        now: i64,
    ) -> Result<GoalView> {
        self.ensure_writer_healthy()?;
        let result = self.goal_pause_inner(agent, mode, reason, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn goal_clear(&mut self, agent: &str, mode: &str, now: i64) -> Result<GoalView> {
        self.ensure_writer_healthy()?;
        let result = self.goal_clear_inner(agent, mode, now);
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn goal_resume(&mut self, agent: &str, mode: &str, now: i64) -> Result<GoalView> {
        self.ensure_writer_healthy()?;
        let result = self.goal_resume_inner(agent, mode, now);
        self.fault_on_persistence_error(&result);
        result
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
        idle_seq: i64,
        now: i64,
    ) -> Result<GoalRoundProbe> {
        self.ensure_writer_healthy()?;
        require_eliot_mode(mode)?;
        if idle_seq < 0 {
            return Err(Error::new(
                "invalid",
                "goal round idle_seq must be nonnegative",
            ));
        }
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
        if row.last_round_key.as_deref() == Some(&round_key_text(idle_seq)) {
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
        // unburn an unknown round key of this generation.
        if markers.iter().any(|marker| {
            marker.round_key == idle_seq
                && marker.generation == row.generation
                && !marker.is_live(&self.goal_holder)
        }) {
            return Err(Error::new("stale_round", "goal round key was already used"));
        }
        if !round_key_names_generation_event(&self.connection, agent_num, row.generation, idle_seq)?
        {
            return Err(Error::new(
                "stale_round",
                "round key does not name an idle event of this Agent generation",
            ));
        }
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
            turn_count: count_turns(&self.connection, agent_num, agent_row.generation)?,
            workspace: agent_row.workspace.clone(),
            view,
        };
        Ok(probe)
    }

    /// Commit the durable in-flight marker for one round, re-validating the
    /// whole probe (Goal binding, single flight, idle gates, round key) in
    /// the same Immediate transaction. A second claim while a marker is
    /// running is refused without running anything. Every claim resolves
    /// exactly once: the final commit consumes its marker, anything else
    /// (unknown check, interference, crash) leaves it `unknown` and burns
    /// the key.
    pub fn goal_claim_round(
        &mut self,
        agent: &str,
        mode: &str,
        idle_seq: i64,
        expected_revision: i64,
        now: i64,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = self.goal_claim_round_inner(agent, mode, idle_seq, expected_revision, now);
        self.fault_on_persistence_error(&result);
        result
    }

    /// Mark one claimed round `unknown` without committing any outcome. The
    /// key stays burned: it is never rerun. Idempotent.
    pub fn goal_abandon_round(
        &mut self,
        agent: &str,
        mode: &str,
        generation: i64,
        revision: i64,
        round_key: i64,
        now: i64,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        require_eliot_mode(mode)?;
        let agent_num = parse_agent_id(agent)?;
        if round_key < 0 {
            return Err(Error::new("invalid", "goal round key must be nonnegative"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        db!(tx.execute(
            "UPDATE goal_markers SET state = 'unknown', updated_at = ?1
             WHERE agent = ?2 AND mode = ?3 AND generation = ?4 AND revision = ?5
               AND round_key = ?6 AND state = 'running'",
            params![now, agent_num, mode, generation, revision, round_key],
        ));
        commit_transaction(tx, &self.write_faulted)?;
        Ok(())
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
        definition: &GoalDefinition,
        now: i64,
    ) -> Result<GoalView> {
        require_eliot_mode(mode)?;
        let agent_num = parse_agent_id(agent)?;
        let agent_row = load_agent_by_num(&self.connection, agent_num)?
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
        let turn_count = count_turns(&self.connection, agent_num, agent_row.generation)?;
        if let Some(row) = load_goal_row(&self.connection, agent_num, mode)? {
            if row.generation == agent_row.generation
                && row.state.status == GoalStatus::Active
                && encode_json(&row.definition)? == encoded
            {
                return self.goal_view(row);
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
            };
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
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
            commit_transaction(tx, &self.write_faulted)?;
            let row = load_goal_row(&self.connection, agent_num, mode)?
                .ok_or_else(|| Error::new("internal", "goal disappeared after set"))?;
            return self.goal_view(row);
        }
        let state = GoalStatePersist {
            status: GoalStatus::Active,
            paused_reason: None,
            blocked_reason: None,
            counters: GoalCounters {
                turns_at_set: turn_count,
                ..GoalCounters::default()
            },
        };
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
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
        commit_transaction(tx, &self.write_faulted)?;
        let row = load_goal_row(&self.connection, agent_num, mode)?
            .ok_or_else(|| Error::new("internal", "goal disappeared after set"))?;
        self.goal_view(row)
    }

    fn goal_pause_inner(
        &mut self,
        agent: &str,
        mode: &str,
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
        let agent_num = parse_agent_id(agent)?;
        let Some(row) = load_goal_row(&self.connection, agent_num, mode)? else {
            return Err(Error::new("not_found", "goal was not found"));
        };
        if row.state.status == GoalStatus::Paused {
            return self.goal_view(row);
        }
        if row.state.status != GoalStatus::Active {
            return Err(Error::new(
                "bad_state",
                format!("goal is {} and cannot pause", row.state.status.as_str()),
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
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
        commit_transaction(tx, &self.write_faulted)?;
        self.goal_get(agent, mode)?
            .ok_or_else(|| Error::new("internal", "goal disappeared after pause"))
    }

    fn goal_clear_inner(&mut self, agent: &str, mode: &str, now: i64) -> Result<GoalView> {
        require_eliot_mode(mode)?;
        let agent_num = parse_agent_id(agent)?;
        let Some(row) = load_goal_row(&self.connection, agent_num, mode)? else {
            return Err(Error::new("not_found", "goal was not found"));
        };
        if row.state.status == GoalStatus::Cleared {
            return self.goal_view(row);
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let state = GoalStatePersist {
            status: GoalStatus::Cleared,
            paused_reason: None,
            blocked_reason: None,
            counters: row.state.counters,
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
        self.goal_get(agent, mode)?
            .ok_or_else(|| Error::new("internal", "goal disappeared after clear"))
    }

    fn goal_resume_inner(&mut self, agent: &str, mode: &str, now: i64) -> Result<GoalView> {
        require_eliot_mode(mode)?;
        let agent_num = parse_agent_id(agent)?;
        let Some(row) = load_goal_row(&self.connection, agent_num, mode)? else {
            return Err(Error::new("not_found", "goal was not found"));
        };
        if row.state.status == GoalStatus::Active {
            return self.goal_view(row);
        }
        if row.state.status != GoalStatus::Paused {
            return Err(Error::new(
                "bad_state",
                format!(
                    "goal is {} and resumes only via set",
                    row.state.status.as_str()
                ),
            ));
        }
        let agent_row = load_agent_by_num(&self.connection, agent_num)?
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
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
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
        commit_transaction(tx, &self.write_faulted)?;
        self.goal_get(agent, mode)?
            .ok_or_else(|| Error::new("internal", "goal disappeared after resume"))
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
        agent: &str,
        mode: &str,
        idle_seq: i64,
        expected_revision: i64,
        now: i64,
    ) -> Result<()> {
        require_eliot_mode(mode)?;
        if idle_seq < 0 {
            return Err(Error::new(
                "invalid",
                "goal round idle_seq must be nonnegative",
            ));
        }
        let agent_num = parse_agent_id(agent)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
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
        if row.last_round_key.as_deref() == Some(&round_key_text(idle_seq))
            || markers
                .iter()
                .any(|marker| marker.round_key == idle_seq && marker.generation == row.generation)
        {
            return Err(Error::new("stale_round", "goal round key was already used"));
        }
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
        if !round_key_names_generation_event(&tx, agent_num, row.generation, idle_seq)? {
            return Err(Error::new(
                "stale_round",
                "round key does not name an idle event of this Agent generation",
            ));
        }
        if max_agent_seq_tx(&tx, agent_num, row.generation)? != idle_seq {
            return Err(Error::new(
                "stale_round",
                "round key is not the latest Agent event",
            ));
        }
        db!(tx.execute(
            "INSERT INTO goal_markers(agent, mode, generation, revision, round_key,
                 holder, state, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'running', ?7)",
            params![
                agent_num,
                mode,
                row.generation,
                row.revision,
                idle_seq,
                self.goal_holder,
                now,
            ],
        ));
        commit_transaction(tx, &self.write_faulted)?;
        Ok(())
    }

    fn goal_commit_round_inner(
        &mut self,
        commit: &GoalRoundCommit,
        now: i64,
    ) -> Result<GoalRoundCommitResult> {
        require_eliot_mode(&commit.mode)?;
        let agent_num = parse_agent_id(&commit.agent)?;
        if commit.round_key < 0 {
            return Err(Error::new("invalid", "goal round key must be nonnegative"));
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
        if row.last_round_key.as_deref() == Some(&round_key_text(commit.round_key)) {
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
        if !round_key_names_generation_event(&tx, agent_num, row.generation, commit.round_key)? {
            return Err(Error::new(
                "stale_round",
                "round key does not name an idle event of this Agent generation",
            ));
        }
        if max_agent_seq_tx(&tx, agent_num, row.generation)? != commit.round_key {
            return Err(Error::new(
                "stale_round",
                "Agent events advanced past the round key",
            ));
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
                if counters.last_output_digest.as_deref() == Some(digest.as_str()) {
                    counters.consecutive_same_output += 1;
                } else {
                    counters.consecutive_same_output = 0;
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
                    verb: "send".to_owned(),
                    args: json!({"text": continuation.text}),
                    expected_generation: Some(row.generation),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                };
                let send = admission::submit_tx(&tx, "owner", request, None, now, false)?;
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
            }
            GoalCommitOutcome::Blocked { reason } => {
                state.status = GoalStatus::Blocked;
                state.blocked_reason = Some(reason.clone());
            }
            GoalCommitOutcome::Continued { .. } => {}
        }
        state.counters = counters;
        let state_text = encode_json(&state)?;
        db!(tx.execute(
            "DELETE FROM goal_markers
             WHERE agent = ?1 AND mode = ?2 AND generation = ?3 AND revision = ?4
               AND round_key = ?5",
            params![
                agent_num,
                commit.mode,
                commit.expected_generation,
                commit.expected_revision,
                commit.round_key,
            ],
        ));
        db!(tx.execute(
            "UPDATE goals SET revision = ?1, state = ?2, last_round_key = ?3,
                 updated_at = ?4
             WHERE agent = ?5 AND mode = ?6 AND revision = ?7",
            params![
                row.revision + 1,
                state_text,
                round_key_text(commit.round_key),
                now,
                agent_num,
                commit.mode,
                row.revision,
            ],
        ));
        commit_transaction(tx, &self.write_faulted)?;
        let row = load_goal_row(&self.connection, agent_num, &commit.mode)?
            .ok_or_else(|| Error::new("internal", "goal disappeared after round"))?;
        let view = self.goal_view(row)?;
        Ok(GoalRoundCommitResult {
            outcome,
            view,
            admission,
        })
    }
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

fn round_key_text(seq: i64) -> String {
    format!("idle:{seq}")
}

#[derive(Clone, Debug)]
struct GoalMarkerRow {
    generation: i64,
    round_key: i64,
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
        let turn_count = count_turns(&self.connection, row.agent_num, row.generation)?;
        let mut counters = row.state.counters.clone();
        counters.turns_used = counters
            .turns_used
            .max(turn_count.saturating_sub(counters.turns_at_set));
        let round_marker = display_goal_marker(
            &self.connection,
            &self.goal_holder,
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
    let live: Option<(i64, i64, i64, i64)> = connection
        .query_row(
            "SELECT round_key, generation, revision, updated_at FROM goal_markers
             WHERE agent = ?1 AND mode = ?2 AND state = 'running' AND holder = ?3
             LIMIT 1",
            params![agent_num, mode, holder],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(sql_error)?;
    if let Some((round_key, generation, revision, updated_at)) = live {
        return Ok(Some(GoalRoundMarker {
            round_key: round_key_text(round_key),
            generation,
            revision,
            state: GoalMarkerState::Running,
            updated_at,
        }));
    }
    let burned: Option<(i64, i64, i64)> = connection
        .query_row(
            "SELECT round_key, revision, updated_at FROM goal_markers
             WHERE agent = ?1 AND mode = ?2 AND generation = ?3
               AND (state = 'unknown' OR holder <> ?4)
             ORDER BY updated_at DESC, round_key DESC LIMIT 1",
            params![agent_num, mode, generation, holder],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sql_error)?;
    Ok(
        burned.map(|(round_key, revision, updated_at)| GoalRoundMarker {
            round_key: round_key_text(round_key),
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
            "SELECT generation, round_key, holder, state
             FROM goal_markers WHERE agent = ?1 AND mode = ?2",
        )
        .map_err(sql_error)?;
    statement
        .query_map(params![agent_num, mode], |row| {
            Ok(GoalMarkerRow {
                generation: row.get(0)?,
                round_key: row.get(1)?,
                holder: row.get(2)?,
                state: row.get(3)?,
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
            "SELECT generation, round_key, holder, state
             FROM goal_markers WHERE agent = ?1 AND mode = ?2",
        )
        .map_err(sql_error)?;
    statement
        .query_map(params![agent_num, mode], |row| {
            Ok(GoalMarkerRow {
                generation: row.get(0)?,
                round_key: row.get(1)?,
                holder: row.get(2)?,
                state: row.get(3)?,
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
    round_key: i64,
) -> Result<Option<GoalMarkerRow>> {
    tx.query_row(
        "SELECT generation, round_key, holder, state
         FROM goal_markers
         WHERE agent = ?1 AND mode = ?2 AND generation = ?3 AND revision = ?4
           AND round_key = ?5",
        params![agent_num, mode, generation, revision, round_key],
        |row| {
            Ok(GoalMarkerRow {
                generation: row.get(0)?,
                round_key: row.get(1)?,
                holder: row.get(2)?,
                state: row.get(3)?,
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
        "state IN ('accepted', 'queued', 'dispatched')"
    };
    let sql = format!(
        "SELECT COUNT(*) FROM commands WHERE target = ?1 AND {states}
         AND (generation IS NULL OR generation = ?2)"
    );
    connection
        .query_row(&sql, params![agent_num, generation], |row| row.get(0))
        .map_err(sql_error)
}
