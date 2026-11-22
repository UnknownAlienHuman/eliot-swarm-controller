use super::*;
use eliot_core::api::{AttentionStepReadback, MAX_DIAGNOSE_ATTENTION_STEPS};
use eliot_core::{ATTENTION_STEP_KIND, AttentionStep};

pub(super) fn insert_attention_step(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    step: &AttentionStep,
    now: i64,
) -> Result<Event> {
    insert_daemon_event(
        tx,
        now,
        ATTENTION_STEP_KIND,
        "observe",
        Some(agent),
        Some(generation),
        json!(step),
    )
}

impl Store {
    pub fn claim_timed_attention_step(
        &mut self,
        agent: &str,
        generation: i64,
        reason: Option<&str>,
        reaction: &AttentionStep,
        after_question: Option<i64>,
        now: i64,
    ) -> Result<Option<Event>> {
        let turn = reaction
            .turn_id
            .as_deref()
            .ok_or_else(|| Error::new("invalid", "timed attention step requires an exact turn"))?;
        if generation < 1 || !reaction.is_bounded() {
            return Err(Error::new("invalid", "invalid timed attention step"));
        }
        let agent = parse_agent_id(agent)?;
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            if !load_agent_by_num(&tx, agent)?.is_some_and(|current| {
                current.generation == generation
                    && (reaction.step == "owner_notice"
                        || current.active_turn.as_deref() == Some(turn))
            }) {
                return Ok(None);
            }
            let event = insert_attention_step(&tx, agent, generation, reaction, now)?;
            let claimed = tx
                .execute(
                    "INSERT INTO turn_attention_steps(
                    agent, generation, turn_id, incident_kind, step,
                    event_seq, claimed_at, command_id, after_question_seq
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT DO NOTHING",
                    params![
                        agent,
                        generation,
                        turn,
                        reason.unwrap_or_default(),
                        reaction.step,
                        event.seq,
                        event.ts,
                        reaction.command_id,
                        after_question
                    ],
                )
                .map_err(sql_error)?;
            if claimed == 0 {
                return Ok(None);
            }
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Some(event))
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn latest_session_growth_incident(
        &self,
        agent: &str,
        generation: i64,
    ) -> Result<Option<(i64, Option<String>)>> {
        let agent = parse_agent_id(agent)?;
        let latest = self
            .connection
            .query_row(
                "SELECT seq, json_extract(data, '$.turn_id'),
                        COALESCE(json_extract(data, '$.warn'), 1),
                        json_extract(data, '$.ratio')
                 FROM events WHERE agent = ?1 AND generation = ?2 AND kind = ?3
                   AND kind IN ('session.growth', 'x.user.agent.attention_step')
                 ORDER BY seq DESC LIMIT 1",
                params![agent, generation, core::SESSION_GROWTH_KIND],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, bool>(2)?,
                        row.get::<_, Option<f64>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?;
        Ok(latest.and_then(|(seq, turn, active, ratio)| {
            // A reported occupancy above capacity is not a verified signal
            // for mutating the session. File size is independently measured.
            (active && ratio.is_none_or(|ratio| ratio <= 1.0)).then_some((seq, turn))
        }))
    }

    /// Reserve one reaction before any command may be sent. A lost response
    /// leaves the retained claim in place, including across daemon restart.
    pub fn claim_attention_step(
        &mut self,
        agent: &str,
        generation: i64,
        reaction: &AttentionStep,
        now: i64,
    ) -> Result<Option<Event>> {
        if generation < 1 || !reaction.is_bounded() {
            return Err(Error::new("invalid", "invalid monitoring reaction"));
        }
        self.ensure_writer_healthy()?;
        let result = (|| {
            let agent = parse_agent_id(agent)?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let current = load_agent_by_num(&tx, agent)?;
            if !current.is_some_and(|current| current.generation == generation) {
                return Ok(None);
            }
            let exists: bool = tx
                .query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM events
                        WHERE agent = ?1 AND generation = ?2 AND kind = ?3
                          AND kind IN ('session.growth', 'x.user.agent.attention_step')
                          AND json_extract(data, '$.incident_id') = ?4
                          AND json_extract(data, '$.step') = ?5
                    )",
                    params![
                        agent,
                        generation,
                        ATTENTION_STEP_KIND,
                        reaction.incident_id,
                        reaction.step
                    ],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if exists {
                return Ok(None);
            }
            let event = insert_attention_step(&tx, agent, generation, reaction, now)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Some(event))
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn recent_attention_steps(
        &self,
        agent: &str,
        turn: Option<&str>,
    ) -> Result<Vec<AttentionStepReadback>> {
        let agent_num = parse_agent_id(agent)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT generation, ts, data FROM events
                 WHERE agent = ?1 AND generation IS NOT NULL AND kind = ?2
                   AND kind IN ('session.growth', 'x.user.agent.attention_step')
                   AND (?3 IS NULL OR json_extract(data, '$.turn_id') = ?3)
                 ORDER BY seq DESC LIMIT ?4",
            )
            .map_err(sql_error)?;
        let mut rows = statement
            .query(params![
                agent_num,
                ATTENTION_STEP_KIND,
                turn,
                MAX_DIAGNOSE_ATTENTION_STEPS as i64
            ])
            .map_err(sql_error)?;
        let mut result = Vec::new();
        while let Some(row) = rows.next().map_err(sql_error)? {
            let generation = row.get(0).map_err(sql_error)?;
            let at = row.get(1).map_err(sql_error)?;
            let data: String = row.get(2).map_err(sql_error)?;
            let Ok(mut reaction) = serde_json::from_str::<AttentionStep>(&data) else {
                continue;
            };
            if !reaction.is_bounded() {
                continue;
            }
            if let Some(id) = reaction.command_id.as_deref() {
                if reaction.step == "switch_for_session_growth" {
                    if let Ok(result) = self.switch_result(id)
                        && result.from == agent
                        && result.from_generation == generation
                    {
                        reaction.outcome = command_state_name(result.state).to_owned();
                    }
                } else if let Some(command) = self.get_command(id)?
                    && ((command.target.as_deref() == Some(agent)
                        && (command.generation == Some(generation)
                            || (command.verb == "resume"
                                && command.expected_generation == Some(generation))))
                        || (reaction.step == "owner_notice"
                            && command.principal == "owner"
                            && command.verb == "notify"
                            && command.target.is_none()))
                {
                    reaction.outcome = if command.verb == "inject"
                        && command.args["point"] == "turn_safe_point"
                        && command.state == CommandState::Accepted
                    {
                        "queued"
                    } else {
                        command_state_name(command.state)
                    }
                    .to_owned();
                } else if reaction.step == "owner_notice"
                    && write_binding(&self.connection, id)?.is_some_and(
                        |(principal, method, _, _)| principal == "owner" && method == "mail.send",
                    )
                {
                    reaction.outcome = "applied".to_owned();
                }
            }
            result.push(AttentionStepReadback {
                generation,
                at,
                reaction,
            });
        }
        result.reverse();
        Ok(result)
    }
}
