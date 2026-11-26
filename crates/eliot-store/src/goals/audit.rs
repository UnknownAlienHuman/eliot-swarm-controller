//! Durable, revision-bound Goal audit intent and bounded Owner history.

use super::*;
use serde::{Deserialize, Serialize};

pub const GOAL_AUDIT_HISTORY_LIMIT: usize = 8;
const GOAL_AUDIT_CANDIDATE_LIMIT: i64 = 64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GoalAuditHistoryEntry {
    pub cycle: u8,
    pub auditor: String,
    #[serde(default)]
    pub successor: Option<String>,
    pub text: String,
}

#[derive(Clone, Debug)]
pub struct GoalAuditEvidence {
    pub definition: GoalDefinition,
    pub observations: Vec<String>,
    pub attempt_digests: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GoalAuditCycle {
    pub generation: i64,
    pub revision: i64,
    pub cycle: u8,
    pub max_audits: u8,
    pub launch_command: String,
    pub phase: String,
    #[serde(default)]
    pub auditor: Option<String>,
    #[serde(default)]
    pub auditor_generation: Option<i64>,
    #[serde(default)]
    pub audit_text: Option<String>,
    #[serde(default)]
    pub successor: Option<String>,
    #[serde(default)]
    pub stop_command: Option<String>,
    #[serde(default)]
    pub switch_command: Option<String>,
    #[serde(default)]
    pub blocked_reason: Option<String>,
}

#[derive(Clone, Debug)]
pub enum GoalAuditStep {
    Ready {
        source: Agent,
        revision: i64,
        cycle: u8,
    },
    Running {
        source: Agent,
        revision: i64,
        launch_command: String,
        cycle: u8,
    },
    ReadyToContinue {
        source: Agent,
        revision: i64,
        auditor: String,
        text: String,
        stop_command: String,
        cycle: u8,
    },
    Held {
        source: Agent,
        revision: i64,
        reason: String,
        cycle: u8,
    },
}

fn step_for(store: &Store, row: GoalRow) -> Result<Option<GoalAuditStep>> {
    if let Some(cycle) = row.state.audit_cycle {
        let source = load_agent_by_num(&store.connection, row.agent_num)?;
        let Some(source) = source else {
            return Ok(None);
        };
        if source.generation != cycle.generation {
            return Ok(Some(GoalAuditStep::Held {
                source,
                revision: cycle.revision,
                reason: "audit_source_generation_changed".into(),
                cycle: cycle.cycle,
            }));
        }
        if cycle.switch_command.is_some() {
            return Ok(Some(GoalAuditStep::Held {
                source,
                revision: cycle.revision,
                reason: cycle
                    .blocked_reason
                    .unwrap_or_else(|| "audit_switch_pending".into()),
                cycle: cycle.cycle,
            }));
        }
        let step = match cycle.phase.as_str() {
            "launch_pending" | "audit_running" => GoalAuditStep::Running {
                source,
                revision: cycle.revision,
                launch_command: cycle.launch_command,
                cycle: cycle.cycle,
            },
            "audit_unknown" => GoalAuditStep::Held {
                source,
                revision: cycle.revision,
                reason: cycle
                    .blocked_reason
                    .unwrap_or_else(|| "audit_outcome_unknown".into()),
                cycle: cycle.cycle,
            },
            "audit_complete" => match (cycle.auditor, cycle.audit_text) {
                (Some(auditor), Some(text)) => GoalAuditStep::ReadyToContinue {
                    source,
                    revision: cycle.revision,
                    auditor,
                    text,
                    stop_command: cycle.stop_command.ok_or_else(|| {
                        Error::new("internal", "completed audit has no reserved STOP id")
                    })?,
                    cycle: cycle.cycle,
                },
                _ => GoalAuditStep::Held {
                    source,
                    revision: cycle.revision,
                    reason: "audit_readback_incomplete".into(),
                    cycle: cycle.cycle,
                },
            },
            _ => GoalAuditStep::Held {
                source,
                revision: cycle.revision,
                reason: cycle
                    .blocked_reason
                    .unwrap_or_else(|| "audit_outcome_unknown".into()),
                cycle: cycle.cycle,
            },
        };
        return Ok(Some(step));
    }
    if row.state.status != GoalStatus::Blocked
        || row.state.blocked_reason.as_deref().is_none_or(|reason| {
            !matches!(
                reason,
                "round_budget" | "turn_limit" | "no_progress" | "escalation_refused"
            )
        })
    {
        return Ok(None);
    }
    let source = load_agent_by_num(&store.connection, row.agent_num)?;
    let Some(source) = source else {
        return Ok(None);
    };
    if source.generation != row.generation {
        return Ok(Some(GoalAuditStep::Held {
            source,
            revision: row.revision,
            reason: "audit_source_generation_changed".into(),
            cycle: u8::try_from(row.state.audit_history.len()).unwrap_or(0),
        }));
    }
    let cycles = u8::try_from(row.state.audit_history.len()).unwrap_or(u8::MAX);
    Ok(Some(if cycles >= GOAL_AUDIT_HISTORY_LIMIT as u8 {
        GoalAuditStep::Held {
            source,
            revision: row.revision,
            reason: "audit_budget_exhausted".into(),
            cycle: cycles,
        }
    } else {
        GoalAuditStep::Ready {
            source,
            revision: row.revision,
            cycle: cycles + 1,
        }
    }))
}

impl Store {
    /// Report a known refusal once while keeping the blocked revision eligible.
    pub fn note_goal_audit_retry(
        &mut self,
        agent: &str,
        generation: i64,
        revision: i64,
        reason: &str,
    ) -> Result<bool> {
        self.ensure_writer_healthy()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let agent_num = parse_agent_id(agent)?;
        let Some(mut row) = load_goal_row(&tx, agent_num, "eliot")? else {
            return Ok(false);
        };
        if row.generation != generation
            || row.revision != revision
            || row.state.status != GoalStatus::Blocked
            || row.state.audit_retry_reason.as_deref() == Some(reason)
        {
            return Ok(false);
        }
        row.state.audit_retry_reason = Some(core::api::truncate_bytes(reason, 256).0);
        tx.execute(
            "UPDATE goals SET state=?1 WHERE agent=?2 AND mode='eliot'",
            params![encode_json(&row.state)?, agent_num],
        )
        .map_err(sql_error)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(true)
    }
    /// Bounded page of blocked Goals that may advance an audit cycle.
    pub fn goal_audit_candidates(&self, after: i64) -> Result<(Vec<GoalAuditStep>, Option<i64>)> {
        let mut statement = self.connection.prepare(
            "SELECT agent, mode, generation, revision, definition, state, last_round_key, updated_at
             FROM goals WHERE agent>?1 AND mode='eliot'
             AND json_extract(state,'$.status')='blocked'
             AND json_extract(state,'$.blocked_reason') IN ('round_budget','turn_limit','no_progress','escalation_refused')
             AND json_extract(state,'$.audit_cycle.switch_command') IS NULL
             AND COALESCE(json_extract(state,'$.audit_cycle.phase'),'launch_pending')
                 IN ('launch_pending','audit_running','audit_complete')
             AND EXISTS (SELECT 1 FROM agents a
                         WHERE a.id=goals.agent AND a.generation=goals.generation)
             AND (json_extract(state,'$.audit_cycle.generation') IS NULL
                  OR (json_extract(state,'$.audit_cycle.generation')=goals.generation
                      AND json_extract(state,'$.audit_cycle.revision')=goals.revision))
             ORDER BY agent LIMIT ?2"
        ).map_err(sql_error)?;
        let rows = statement
            .query_map([after, GOAL_AUDIT_CANDIDATE_LIMIT], |row| {
                let definition: String = row.get(4)?;
                let state: String = row.get(5)?;
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    definition,
                    state,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, i64>(7)?,
                ))
            })
            .map_err(sql_error)?;
        let mut out = Vec::new();
        let mut last = None;
        for result in rows {
            let (
                agent_num,
                mode,
                generation,
                revision,
                definition,
                state,
                last_round_key,
                updated_at,
            ) = result.map_err(sql_error)?;
            last = Some(agent_num);
            let row = GoalRow {
                agent_num,
                mode,
                generation,
                revision,
                definition: serde_json::from_str(&definition)
                    .map_err(|e| Error::new("internal", e.to_string()))?,
                state: serde_json::from_str(&state)
                    .map_err(|e| Error::new("internal", e.to_string()))?,
                last_round_key,
                updated_at,
            };
            if let Some(step) = step_for(self, row)? {
                out.push(step);
            }
        }
        Ok((out, last))
    }

    /// Release an audit launch reservation only when admission is proven absent.
    pub fn release_unadmitted_goal_audit(
        &mut self,
        agent: &str,
        generation: i64,
        revision: i64,
        launch: &str,
        now: i64,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let agent_num = parse_agent_id(agent)?;
        let Some(mut row) = load_goal_row(&tx, agent_num, "eliot")? else {
            return Err(Error::new("not_found", "audit source Goal missing"));
        };
        let Some(cycle) = row.state.audit_cycle.as_ref() else {
            return Err(Error::new(
                "stale_goal",
                "audit launch claim is no longer current",
            ));
        };
        if row.generation != generation
            || row.revision != revision
            || cycle.generation != generation
            || cycle.revision != revision
            || cycle.launch_command != launch
            || cycle.phase != "launch_pending"
        {
            return Err(Error::new(
                "stale_goal",
                "audit launch claim is no longer current",
            ));
        }
        if load_command(&tx, launch)?.is_some() {
            return Err(Error::new("bad_state", "audit launch command was admitted"));
        }
        row.state.audit_cycle = None;
        tx.execute(
            "UPDATE goals SET state=?1,updated_at=?2 WHERE agent=?3 AND mode='eliot' AND generation=?4 AND revision=?5",
            params![encode_json(&row.state)?, now, agent_num, generation, revision],
        )
        .map_err(sql_error)?;
        tx.commit().map_err(sql_error)
    }

    /// Reserve the one audit launch against the exact blocked Goal revision.
    pub fn begin_goal_audit(
        &mut self,
        agent: &str,
        generation: i64,
        revision: i64,
        max_audits: u8,
        launch_command: &str,
        now: i64,
    ) -> Result<Option<GoalAuditStep>> {
        if max_audits > GOAL_AUDIT_HISTORY_LIMIT as u8 || launch_command.is_empty() {
            return Err(Error::new(
                "invalid_argument",
                "invalid Goal audit bound or launch id",
            ));
        }
        validate_ulid(launch_command)?;
        self.ensure_writer_healthy()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let agent_num = parse_agent_id(agent)?;
        let Some(mut row) = load_goal_row(&tx, agent_num, "eliot")? else {
            tx.rollback().map_err(sql_error)?;
            return Ok(None);
        };
        if row.generation != generation || row.revision != revision {
            tx.rollback().map_err(sql_error)?;
            return Ok(None);
        }
        if let Some(cycle) = &row.state.audit_cycle {
            let replay = cycle.generation == generation
                && cycle.revision == revision
                && cycle.max_audits == max_audits
                && cycle.launch_command == launch_command;
            if replay {
                tx.commit().map_err(sql_error)?;
                return self.goal_audit_step_for(agent_num);
            }
            tx.rollback().map_err(sql_error)?;
            return Ok(None);
        }
        if row.state.status != GoalStatus::Blocked
            || row.state.blocked_reason.as_deref().is_none_or(|reason| {
                !matches!(
                    reason,
                    "round_budget" | "turn_limit" | "no_progress" | "escalation_refused"
                )
            })
        {
            tx.rollback().map_err(sql_error)?;
            return Ok(None);
        }
        let cycle = u8::try_from(row.state.audit_history.len())
            .unwrap_or(u8::MAX)
            .saturating_add(1);
        if cycle > max_audits || max_audits == 0 {
            tx.rollback().map_err(sql_error)?;
            return Ok(None);
        }
        row.state.audit_cycle = Some(GoalAuditCycle {
            generation,
            revision,
            cycle,
            max_audits,
            launch_command: launch_command.to_owned(),
            phase: "launch_pending".into(),
            auditor: None,
            auditor_generation: None,
            audit_text: None,
            successor: None,
            stop_command: None,
            switch_command: None,
            blocked_reason: None,
        });
        let changed = tx.execute("UPDATE goals SET state=?1,updated_at=?2 WHERE agent=?3 AND mode='eliot' AND generation=?4 AND revision=?5",
            params![encode_json(&row.state)?, now, agent_num, generation, revision]).map_err(sql_error)?;
        tx.commit().map_err(sql_error)?;
        if changed == 1 {
            Ok(Some(GoalAuditStep::Running {
                source: load_agent_by_num(&self.connection, agent_num)?
                    .ok_or_else(|| Error::new("not_found", "audit source Agent missing"))?,
                revision,
                launch_command: launch_command.to_owned(),
                cycle,
            }))
        } else {
            Ok(None)
        }
    }

    /// Read the retained, bounded auditor text for the Owner inbox.
    pub fn goal_audit_history(&self, agent: &str) -> Result<Vec<GoalAuditHistoryEntry>> {
        let Some(row) = load_goal_row(&self.connection, parse_agent_id(agent)?, "eliot")? else {
            return Ok(Vec::new());
        };
        Ok(row
            .state
            .audit_history
            .into_iter()
            .rev()
            .take(GOAL_AUDIT_HISTORY_LIMIT)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect())
    }

    /// Read bounded observations and attempt digests from retained Goal round results.
    pub fn goal_audit_evidence(
        &self,
        agent: &str,
        generation: i64,
        revision: i64,
    ) -> Result<Option<GoalAuditEvidence>> {
        let agent_num = parse_agent_id(agent)?;
        let Some(row) = load_goal_row(&self.connection, agent_num, "eliot")? else {
            return Ok(None);
        };
        if row.generation != generation
            || row.revision != revision
            || row.state.status != GoalStatus::Blocked
        {
            return Ok(None);
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT result FROM writes
             WHERE method='goal.round' AND json_extract(result,'$.request.agent')=?1
               AND json_extract(result,'$.generation')=?2
               AND json_extract(result,'$.revision')<=?3
             ORDER BY created_at DESC,id DESC LIMIT 64",
            )
            .map_err(sql_error)?;
        let records = statement
            .query_map(params![agent, generation, revision], |r| {
                r.get::<_, String>(0)
            })
            .map_err(sql_error)?;
        let mut observations = Vec::new();
        let mut digests = Vec::new();
        for raw in records {
            let raw = raw.map_err(sql_error)?;
            let Ok(record) = serde_json::from_str::<GoalRoundRecord>(&raw) else {
                continue;
            };
            if record.request.agent != agent
                || record.generation != generation
                || record.revision > revision
            {
                continue;
            }
            if let StoredGoalRoundOutcome::Finished(response) = record.outcome {
                if response.view.definition != row.definition {
                    continue;
                }
                if let Some(observation) = response.view.counters.last_observation
                    && !observations.contains(&observation)
                    && observations.len() < 8
                {
                    observations.push(observation);
                }
                if let Some(digest) = response.view.counters.last_output_digest
                    && !digests.contains(&digest)
                    && digests.len() < 8
                {
                    digests.push(digest);
                }
            }
        }
        observations.reverse();
        digests.reverse();
        Ok(Some(GoalAuditEvidence {
            definition: row.definition,
            observations,
            attempt_digests: digests,
        }))
    }

    /// Settle the exact audit launch readback; unknown effects are held in place.
    pub fn settle_goal_audit(
        &mut self,
        agent: &str,
        generation: i64,
        revision: i64,
        launch_command: &str,
        policy: &core::SecretPolicy,
        now: i64,
    ) -> Result<Option<GoalAuditStep>> {
        self.ensure_writer_healthy()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let agent_num = parse_agent_id(agent)?;
        let Some(mut row) = load_goal_row(&tx, agent_num, "eliot")? else {
            tx.rollback().map_err(sql_error)?;
            return Ok(None);
        };
        let Some(cycle) = row.state.audit_cycle.as_mut() else {
            tx.rollback().map_err(sql_error)?;
            return Ok(None);
        };
        if row.generation != generation
            || row.revision != revision
            || cycle.generation != generation
            || cycle.revision != revision
            || cycle.launch_command != launch_command
        {
            tx.rollback().map_err(sql_error)?;
            return Ok(None);
        }
        if matches!(
            cycle.phase.as_str(),
            "audit_complete" | "audit_unknown" | "audit_failed" | "stop_unknown" | "held"
        ) {
            tx.rollback().map_err(sql_error)?;
            return self.goal_audit_step_for(agent_num);
        }
        let before_phase = cycle.phase.clone();
        let before_reason = cycle.blocked_reason.clone();
        let launch = load_command(&tx, launch_command)?;
        let Some(launch) = launch else {
            cycle.phase = "audit_unknown".into();
            cycle.blocked_reason = Some("audit_launch_missing".into());
            let _ = tx.execute("UPDATE goals SET state=?1,updated_at=?2 WHERE agent=?3 AND mode='eliot' AND generation=?4 AND revision=?5",
                params![encode_json(&row.state)?, now, agent_num, generation, revision]).map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
            return self.goal_audit_step_for(agent_num);
        };
        let linked = load_linked_command(&tx, launch_command)?;
        let mut turn_id = launch
            .turn_id
            .clone()
            .or_else(|| linked.as_ref().and_then(|command| command.turn_id.clone()));
        let turn_agent = linked
            .as_ref()
            .and_then(|command| command.target.as_deref())
            .or(launch.target.as_deref())
            .and_then(|id| parse_agent_id(id).ok())
            .unwrap_or(agent_num);
        let turn_generation = linked
            .as_ref()
            .and_then(|command| command.generation)
            .or(launch.generation)
            .unwrap_or(generation);
        if turn_id.is_none() {
            turn_id = tx.query_row(
                "SELECT turn_id FROM turns WHERE agent=?1 AND generation=?2 AND start_command_id=?3 ORDER BY started_at DESC LIMIT 1",
                params![turn_agent, turn_generation, launch_command],
                |r| r.get(0),
            ).optional().map_err(sql_error)?;
        }
        let Some(turn_id) = turn_id else {
            let unknown = launch.state == CommandState::Unknown
                || linked
                    .as_ref()
                    .is_some_and(|command| command.state == CommandState::Unknown);
            let failed = matches!(
                launch.state,
                CommandState::Rejected | CommandState::Failed | CommandState::Cancelled
            ) || linked.as_ref().is_some_and(|command| {
                matches!(
                    command.state,
                    CommandState::Rejected | CommandState::Failed | CommandState::Cancelled
                )
            });
            cycle.phase = if unknown {
                "audit_unknown"
            } else if failed {
                "audit_failed"
            } else {
                "audit_running"
            }
            .into();
            cycle.blocked_reason = if unknown {
                Some("audit_launch_unknown".into())
            } else if failed {
                Some("audit_launch_failed".into())
            } else {
                None
            };
            if cycle.phase == before_phase && cycle.blocked_reason == before_reason {
                tx.rollback().map_err(sql_error)?;
                return self.goal_audit_step_for(agent_num);
            }
            tx.execute("UPDATE goals SET state=?1,updated_at=?2 WHERE agent=?3 AND mode='eliot' AND generation=?4 AND revision=?5",
                params![encode_json(&row.state)?, now, agent_num, generation, revision]).map_err(sql_error)?;
            tx.commit().map_err(sql_error)?;
            return self.goal_audit_step_for(agent_num);
        };
        let exact_command_applied = launch.state == CommandState::Applied
            && linked
                .as_ref()
                .is_none_or(|command| command.state == CommandState::Applied);
        let turn = load_turn(&tx, turn_agent, turn_generation, &turn_id)?;
        let Some(turn) = turn else {
            tx.rollback().map_err(sql_error)?;
            return Ok(None);
        };
        if turn.state == core::TurnState::Unknown
            || launch.state == CommandState::Unknown
            || linked
                .as_ref()
                .is_some_and(|command| command.state == CommandState::Unknown)
        {
            cycle.phase = "audit_unknown".into();
            cycle.blocked_reason = Some("audit_turn_unknown".into());
        } else if !exact_command_applied {
            cycle.phase = "audit_running".into();
            cycle.blocked_reason = Some("audit_launch_not_applied".into());
        } else if turn.state == core::TurnState::Running {
            cycle.phase = "audit_running".into();
            cycle.blocked_reason = None;
        } else if turn.state == core::TurnState::Completed
            && turn.outcome == Some(core::TurnOutcome::Success)
            && turn.final_text.is_some()
        {
            if let Some(auditor) = launch.target.clone() {
                cycle.auditor = Some(auditor.clone());
                cycle.auditor_generation = Some(turn_generation);
                let audit_text = turn
                    .final_text
                    .ok_or_else(|| Error::new("bad_state", "successful audit has no final text"))?;
                let audit_text = policy.redact_bounded(&audit_text, MAX_GOAL_OBSERVATION_BYTES);
                cycle.audit_text = Some(audit_text.clone());
                if !row
                    .state
                    .audit_history
                    .iter()
                    .any(|entry| entry.cycle == cycle.cycle && entry.auditor == auditor)
                {
                    row.state.audit_history.push(GoalAuditHistoryEntry {
                        cycle: cycle.cycle,
                        auditor,
                        successor: None,
                        text: audit_text,
                    });
                    if row.state.audit_history.len() > GOAL_AUDIT_HISTORY_LIMIT {
                        row.state.audit_history.remove(0);
                    }
                }
                cycle.stop_command = Some(ulid::Ulid::new().to_string());
                cycle.phase = "audit_complete".into();
                cycle.blocked_reason = None;
            } else {
                cycle.phase = "held".into();
                cycle.blocked_reason = Some("audit_success_missing_target".into());
                row.state.blocked_reason = cycle.blocked_reason.clone();
            }
        } else {
            cycle.phase = "audit_failed".into();
            cycle.blocked_reason = Some(match turn.outcome {
                Some(core::TurnOutcome::Interrupted) => "audit_turn_interrupted".into(),
                Some(core::TurnOutcome::Failure) => "audit_turn_failed".into(),
                _ => "audit_turn_terminal_without_success".into(),
            });
        }
        if cycle.phase == before_phase && cycle.blocked_reason == before_reason {
            tx.rollback().map_err(sql_error)?;
            return self.goal_audit_step_for(agent_num);
        }
        tx.execute("UPDATE goals SET state=?1,updated_at=?2 WHERE agent=?3 AND mode='eliot' AND generation=?4 AND revision=?5",
            params![encode_json(&row.state)?, now, agent_num, generation, revision]).map_err(sql_error)?;
        tx.commit().map_err(sql_error)?;
        self.goal_audit_step_for(agent_num)
    }

    fn goal_audit_step_for(&self, agent_num: i64) -> Result<Option<GoalAuditStep>> {
        let Some(row) = load_goal_row(&self.connection, agent_num, "eliot")? else {
            return Ok(None);
        };
        step_for(self, row)
    }

    /// Settle an admitted failure or retain unknown custody on the exact cycle.
    pub fn end_goal_audit(
        &mut self,
        agent: &str,
        generation: i64,
        revision: i64,
        reason: &str,
        outcome_unknown: bool,
        now: i64,
    ) -> Result<()> {
        if reason.trim().is_empty() || reason.len() > 256 || reason.chars().any(char::is_control) {
            return Err(Error::new(
                "invalid",
                "audit hold reason must be a short single line",
            ));
        }
        self.ensure_writer_healthy()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let agent_num = parse_agent_id(agent)?;
        let Some(mut row) = load_goal_row(&tx, agent_num, "eliot")? else {
            return Err(Error::new("not_found", "audit source Goal missing"));
        };
        let Some(cycle) = row.state.audit_cycle.as_mut() else {
            return Err(Error::new("bad_state", "audit cycle missing"));
        };
        if row.generation != generation
            || row.revision != revision
            || cycle.generation != generation
            || cycle.revision != revision
        {
            return Err(Error::new("stale_goal", "audit cycle changed before hold"));
        }
        cycle.phase = if outcome_unknown {
            "held"
        } else {
            "audit_failed"
        }
        .into();
        cycle.blocked_reason = Some(reason.to_owned());
        row.state.blocked_reason = Some(reason.to_owned());
        tx.execute("UPDATE goals SET state=?1,updated_at=?2 WHERE agent=?3 AND mode='eliot' AND generation=?4 AND revision=?5",
            params![encode_json(&row.state)?, now, agent_num, generation, revision]).map_err(sql_error)?;
        tx.commit().map_err(sql_error)
    }
}

/// Consume an exact reserved audit cycle as part of the existing transfer transaction.
pub(super) fn advance_goal_audit_tx(
    tx: &Transaction<'_>,
    source: &Agent,
    to: &Agent,
    revision: i64,
    switch_id: &str,
    state: &mut GoalStatePersist,
) -> Result<bool> {
    let Some(cycle) = state.audit_cycle.clone() else {
        return Ok(false);
    };
    if cycle.phase != "audit_complete"
        || cycle.generation != source.generation
        || cycle.revision != revision
        || cycle.switch_command.as_deref() != Some(switch_id)
    {
        return Err(Error::new(
            "stale_goal",
            "audit switch binding changed before transfer",
        ));
    }
    require_audit_stop_applied(tx, &cycle)?;
    let auditor = cycle
        .auditor
        .ok_or_else(|| Error::new("bad_state", "audit cycle has no auditor"))?;
    if let Some(entry) = state
        .audit_history
        .iter_mut()
        .rev()
        .find(|entry| entry.cycle == cycle.cycle && entry.auditor == auditor)
    {
        entry.successor = Some(to.id.clone());
    } else {
        state.audit_history.push(GoalAuditHistoryEntry {
            cycle: cycle.cycle,
            auditor,
            successor: Some(to.id.clone()),
            text: cycle
                .audit_text
                .ok_or_else(|| Error::new("bad_state", "audit cycle has no final text"))?,
        });
        if state.audit_history.len() > GOAL_AUDIT_HISTORY_LIMIT {
            state.audit_history.remove(0);
        }
    }
    state.audit_cycle = None;
    state.audit_retry_reason = None;
    state.counters.rounds = 0;
    state.counters.turns_used = 0;
    state.counters.turns_at_set = count_turns(tx, parse_agent_id(&to.id)?, to.generation)?;
    state.counters.consecutive_same_output = 0;
    state.counters.consecutive_same_failure = 0;
    state.counters.last_exit_code = None;
    state.counters.last_output_digest = None;
    state.counters.last_fingerprint = None;
    state.counters.last_observation = None;
    state.counters.last_retry_reason = None;
    state.counters.last_check_at = None;
    state.counters.diagnosis_round = None;
    state.counters.escalation_round = None;
    Ok(true)
}

fn require_audit_stop_applied(tx: &Transaction<'_>, cycle: &GoalAuditCycle) -> Result<()> {
    let Some(stop_id) = cycle.stop_command.as_deref() else {
        return Ok(());
    };
    let stop = load_command(tx, stop_id)?
        .ok_or_else(|| Error::new("bad_state", "bound audit STOP is missing"))?;
    if stop.state != CommandState::Applied {
        return Err(Error::new("busy", "audit STOP is not Applied"));
    }
    if stop.verb != "stop"
        || stop.target != cycle.auditor
        || cycle.auditor_generation.is_none_or(|generation| {
            stop.expected_generation != Some(generation) || stop.generation != Some(generation)
        })
    {
        return Err(Error::new(
            "stale_generation",
            "audit STOP is not bound to the exact auditor generation",
        ));
    }
    Ok(())
}

/// Bind the exact switch reservation to this completed audit cycle.
pub(crate) fn bind_goal_audit_switch_tx(
    tx: &Transaction<'_>,
    source: &Agent,
    revision: i64,
    switch_id: &str,
    now: i64,
) -> Result<()> {
    validate_ulid(switch_id)?;
    let source_num = parse_agent_id(&source.id)?;
    let mut row = load_goal_row(tx, source_num, "eliot")?
        .ok_or_else(|| Error::new("bad_state", "audit source Goal missing"))?;
    let cycle = row
        .state
        .audit_cycle
        .as_mut()
        .ok_or_else(|| Error::new("bad_state", "audit cycle missing"))?;
    if row.generation != source.generation
        || row.revision != revision
        || cycle.generation != source.generation
        || cycle.revision != revision
        || cycle.phase != "audit_complete"
    {
        return Err(Error::new(
            "stale_goal",
            "audit cycle changed before switch reservation",
        ));
    }
    require_audit_stop_applied(tx, cycle)?;
    if cycle
        .switch_command
        .as_deref()
        .is_some_and(|old| old != switch_id)
    {
        return Err(Error::new(
            "id_conflict",
            "audit cycle is already bound to another switch",
        ));
    }
    cycle.switch_command = Some(switch_id.to_owned());
    db!(tx.execute("UPDATE goals SET state=?1,updated_at=?2 WHERE agent=?3 AND mode='eliot' AND generation=?4 AND revision=?5",
        params![encode_json(&row.state)?, now, source_num, source.generation, revision]));
    Ok(())
}

/// Read exact auditor text only for the switch bound to the source cycle.
pub(crate) fn goal_audit_switch_text(
    tx: &Transaction<'_>,
    source: &Agent,
    switch_id: &str,
) -> Result<Option<String>> {
    let source_num = parse_agent_id(&source.id)?;
    let Some(row) = load_goal_row(tx, source_num, "eliot")? else {
        return Ok(None);
    };
    if row.generation != source.generation {
        return Ok(None);
    }
    let Some(cycle) = row.state.audit_cycle else {
        return Ok(None);
    };
    if cycle.phase != "audit_complete" || cycle.switch_command.as_deref() != Some(switch_id) {
        return Ok(None);
    }
    Ok(cycle.audit_text)
}

/// Persist the exact terminal switch refusal on the source Goal for Owner review.
pub(crate) fn settle_goal_audit_switch_tx(
    tx: &Transaction<'_>,
    source: &Agent,
    switch_id: &str,
    now: i64,
) -> Result<()> {
    let source_num = parse_agent_id(&source.id)?;
    let Some(mut row) = load_goal_row(tx, source_num, "eliot")? else {
        return Ok(());
    };
    let Some(cycle) = row.state.audit_cycle.as_mut() else {
        return Ok(());
    };
    if row.generation != source.generation
        || cycle.generation != source.generation
        || cycle.revision != row.revision
        || cycle.switch_command.as_deref() != Some(switch_id)
    {
        return Ok(());
    }
    let Some(command) = load_command(tx, switch_id)? else {
        cycle.phase = "held".into();
        cycle.blocked_reason = Some("audit_switch_missing".into());
        row.state.blocked_reason = cycle.blocked_reason.clone();
        tx.execute("UPDATE goals SET state=?1,updated_at=?2 WHERE agent=?3 AND mode='eliot' AND generation=?4 AND revision=?5",
            params![encode_json(&row.state)?, now, source_num, source.generation, row.revision]).map_err(sql_error)?;
        return Ok(());
    };
    let reason = match command.state {
        CommandState::Rejected => Some("audit_switch_rejected"),
        CommandState::Failed => Some("audit_switch_failed"),
        CommandState::Unknown => Some("audit_switch_unknown"),
        CommandState::Cancelled => Some("audit_switch_cancelled"),
        _ => None,
    };
    if let Some(reason) = reason {
        cycle.phase = "held".into();
        cycle.blocked_reason = Some(reason.into());
        row.state.blocked_reason = Some(reason.into());
        tx.execute("UPDATE goals SET state=?1,updated_at=?2 WHERE agent=?3 AND mode='eliot' AND generation=?4 AND revision=?5",
            params![encode_json(&row.state)?, now, source_num, source.generation, row.revision]).map_err(sql_error)?;
    }
    Ok(())
}
