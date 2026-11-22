//! Granted normalized observer facts. This path never applies controlled Agent
//! reduction, command effects, identity changes or speculative input association.
use super::*;
use eliot_core::SourceHealthUpdate;

pub(super) fn require_transcript_families(
    connection: &Connection,
    agent: &Agent,
    source: &str,
    families: &[String],
) -> Result<()> {
    if source != "transcript" || families.is_empty() {
        return Err(Error::new(
            "not_allowed",
            "observer facts require the granted transcript source",
        ));
    }
    let supported = observer::live_families(&agent.harness)?;
    let id = parse_agent_id(&agent.id)?;
    let state = load_source_state(connection, id, agent.generation, source)?.ok_or_else(|| {
        Error::new(
            "source_integrity",
            "observer successor has no committed source state",
        )
    })?;
    if state.state != "open" {
        return Err(Error::new(
            "source_integrity",
            "observer successor is closed",
        ));
    }
    for family in families {
        if !supported.contains(&family.as_str()) {
            return Err(Error::new(
                "not_allowed",
                "family is outside the observed live-fact grant",
            ));
        }
        let owner: Option<(String,String)> = connection.query_row(
            "SELECT source, mechanism FROM source_owners WHERE agent = ?1 AND generation = ?2 AND family = ?3",
            params![id,agent.generation,family], |row| Ok((row.get(0)?,row.get(1)?)),
        ).optional().map_err(sql_error)?;
        if owner
            .as_ref()
            .is_none_or(|(owner, mechanism)| owner != source || mechanism != "rollout_tail")
        {
            return Err(Error::new(
                "source_integrity",
                "observer family has not been handed to the live transcript",
            ));
        }
    }
    Ok(())
}

fn optional_text<'a>(data: &'a Value, field: &str) -> Result<Option<&'a str>> {
    match data.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        _ => Err(Error::new(
            "invalid",
            "observed text field must be a string or null",
        )),
    }
}

fn optional_time(data: &Value, field: &str) -> Result<Option<i64>> {
    match data.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_i64().map(Some).ok_or_else(|| {
            Error::new(
                "invalid",
                "observed turn timing must be integer epoch milliseconds",
            )
        }),
    }
}

pub(super) fn persist_turn_fact(
    tx: &Transaction<'_>,
    agent: &Agent,
    event: &NativeEvent,
) -> Result<()> {
    if !matches!(
        event.kind.as_str(),
        "turn.started" | "turn.completed" | "message.completed"
    ) {
        return Ok(());
    }
    let turn_id = required_string(&event.data, "turn_id")?;
    let id = parse_agent_id(&agent.id)?;
    let existing = load_turn(tx, id, agent.generation, &turn_id)?;
    let mut turn = existing.clone().unwrap_or(Turn {
        agent: agent.id.clone(),
        generation: agent.generation,
        turn_id: turn_id.clone(),
        state: TurnState::Unknown,
        status: None,
        outcome: None,
        native_status: None,
        final_text: None,
        started_at: None,
        completed_at: None,
        usage: None,
        config_revision: current_config_revision_tx(tx, id, agent.generation)?,
        config: None,
        preceded_by: None,
        resolution_source: None,
        effective_model: None,
        native_error_message: None,
    });
    match event.kind.as_str() {
        "turn.started" => {
            if turn.state == TurnState::Completed {
                return Err(Error::new(
                    "source_integrity",
                    "observed start would regress a completed turn",
                ));
            }
            turn.state = TurnState::Running;
            if let Some(at) = optional_time(&event.data, "started_at")? {
                if turn.started_at.is_some_and(|known| known != at) {
                    return Err(Error::new(
                        "source_integrity",
                        "observed start timing changed",
                    ));
                }
                turn.started_at = Some(at);
            }
        }
        "turn.completed" => {
            let status = required_string(&event.data, "status")?;
            let outcome = match event.data.get("outcome") {
                None | Some(Value::Null) => None,
                Some(Value::String(value)) => Some(TurnOutcome::parse(value).ok_or_else(|| {
                    Error::new("invalid", "observed turn outcome is not canonical")
                })?),
                Some(_) => {
                    return Err(Error::new(
                        "invalid",
                        "observed turn outcome must be a string or null",
                    ));
                }
            };
            let native_status = optional_text(&event.data, "native_status")?
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
            if turn.state == TurnState::Completed {
                return Err(Error::new(
                    "source_integrity",
                    "observed turn terminal fact changed after completion",
                ));
            }
            turn.state = TurnState::Completed;
            turn.status = Some(status);
            turn.outcome = outcome;
            turn.native_status = native_status;
            if let Some(at) = optional_time(&event.data, "started_at")? {
                if turn.started_at.is_some_and(|known| known != at) {
                    return Err(Error::new(
                        "source_integrity",
                        "observed start timing changed",
                    ));
                }
                turn.started_at = Some(at);
            }
            turn.completed_at = optional_time(&event.data, "completed_at")?;
            if let Some(text) = optional_text(&event.data, "final_text")? {
                turn.final_text = Some(text.to_owned());
            }
        }
        "message.completed" => {
            if turn.state != TurnState::Completed
                && event.data.get("phase").and_then(Value::as_str) == Some("final_answer")
                && let Some(text) = optional_text(&event.data, "text")?
            {
                turn.final_text = Some(text.to_owned());
            }
        }
        _ => return Ok(()),
    }
    if existing.is_none() {
        insert_turn(
            tx,
            TurnInsert {
                agent: id,
                generation: agent.generation,
                turn_id: &turn_id,
                start_command_id: None,
                state: turn.state,
                status: turn.status.as_deref(),
                outcome: turn.outcome.as_ref().map(TurnOutcome::as_str),
                native_status: turn.native_status.as_deref(),
                final_text: turn.final_text.as_deref(),
                started_at: turn.started_at,
                completed_at: turn.completed_at,
                usage: None,
                config_revision: turn.config_revision,
                preceded_by: turn.preceded_by.as_deref(),
                effective_model: None,
            },
        )?;
    } else {
        db!(tx.execute(
            "UPDATE turns SET state = ?1, status = ?2, outcome = COALESCE(?3, outcome),
                              native_status = COALESCE(?4, native_status),
                              final_text = ?5, started_at = ?6, completed_at = ?7,
                              messages = messages + ?8
             WHERE agent = ?9 AND generation = ?10 AND turn_id = ?11",
            params![
                turn_state_name(turn.state),
                turn.status,
                turn.outcome.as_ref().map(TurnOutcome::as_str),
                turn.native_status,
                turn.final_text,
                turn.started_at,
                turn.completed_at,
                i64::from(event.kind == "message.completed"),
                id,
                agent.generation,
                turn_id
            ],
        ));
    }
    Ok(())
}

impl Store {
    pub fn set_native_source_health(
        &mut self,
        update: SourceHealthUpdate,
        expected_subject: (&str, &str),
        now: i64,
    ) -> Result<Change<SourceHealthUpdate>> {
        self.ensure_writer_healthy()?;
        let result = self.set_source_health_inner(update, Some(expected_subject), now);
        self.fault_on_persistence_error(&result);
        result
    }

    /// Controlled health for the credential-bound Agent/generation. It rejects
    /// monitor sources, stale generations, closed rows, and observer
    /// escalation, with the same commit semantics as the observed path:
    /// unchanged health is a no-op with no event.
    pub fn set_source_health(
        &mut self,
        update: SourceHealthUpdate,
        now: i64,
    ) -> Result<Change<SourceHealthUpdate>> {
        self.ensure_writer_healthy()?;
        let result = self.set_source_health_inner(update, None, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn set_source_health_inner(
        &mut self,
        update: SourceHealthUpdate,
        expected_subject: Option<(&str, &str)>,
        now: i64,
    ) -> Result<Change<SourceHealthUpdate>> {
        update.validate()?;
        if matches!(
            update.source.as_str(),
            "daemon" | "os" | "git" | "workspace_process"
        ) {
            return Err(match expected_subject {
                Some(_) => Error::new(
                    "not_allowed",
                    "observer health cannot change monitor sources",
                ),
                None => Error::new(
                    "not_allowed",
                    "controlled health cannot change monitor sources",
                ),
            });
        }
        let id = parse_agent_id(&update.agent_id)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let agent = load_agent_by_num(&tx, id)?
            .ok_or_else(|| Error::new("not_found", "health Agent was not found"))?;
        match expected_subject {
            Some(subject) => {
                observer::require_live_subject(&tx, &agent, update.generation, subject)?;
                if update.source != "observe" {
                    let families = source_handoff::owner_bindings(&tx, id, update.generation)?
                        .into_iter()
                        .filter(|owner| owner.source == update.source)
                        .map(|owner| owner.family)
                        .collect::<Vec<_>>();
                    require_transcript_families(&tx, &agent, &update.source, &families)?;
                }
            }
            None => {
                if update.generation <= 0 || update.generation != agent.generation {
                    return Err(Error::new(
                        "stale_generation",
                        "health requires the current admitted generation",
                    ));
                }
                observer::require_controlled(&agent)?;
            }
        }
        let state =
            load_source_state(&tx, id, update.generation, &update.source)?.ok_or_else(|| {
                Error::new("source_integrity", "health source has no committed state")
            })?;
        if state.state != "open" {
            return Err(Error::new(
                "source_integrity",
                "health cannot reopen a closed source",
            ));
        }
        let mut events = Vec::new();
        if state.health != update.health {
            events.push(update_open_source_health(
                &tx,
                id,
                update.generation,
                &update.source,
                &update.health,
                now,
            )?);
        }
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value: update,
            events,
        })
    }
}

pub(super) fn update_open_source_health(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    source: &str,
    health: &str,
    now: i64,
) -> Result<Event> {
    let changed = tx
        .execute(
            "UPDATE source_state SET health = ?1, updated_at = ?2
             WHERE agent = ?3 AND generation = ?4 AND source = ?5 AND state = 'open'",
            params![health, now, agent, generation, source],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new(
            "source_integrity",
            "health source changed before commit",
        ));
    }
    insert_daemon_event(
        tx,
        now,
        "source.health",
        "source",
        Some(agent),
        Some(generation),
        json!({"source":source,"state":"open","health":health}),
    )
}
