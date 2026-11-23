use super::native_events::{persist_effects, verified_native_event_cause_tx};
use super::*;

#[derive(Debug, PartialEq, Eq)]
pub struct ActiveTurnRecovery {
    /// The bound send; `None` for a self-started native turn with no send,
    /// which reads back through the session's start command.
    pub command_id: Option<String>,
    pub turn_id: String,
    source_event_seq: i64,
    view_projection_gap: bool,
}

/// The daemon monitor's readback result for one active turn.
#[derive(Debug)]
pub enum RecoveredTurn {
    /// A proven native terminal in `turn.completed` data shape.
    Terminal(Value),
    /// The owned native process exited and its drained stream held no
    /// terminal for the exact turn.
    NativeExited,
    /// The bounded reads proved nothing.
    Inconclusive,
}

impl Store {
    pub fn active_turn_recovery(&self, agent: &Agent) -> Result<Option<ActiveTurnRecovery>> {
        active_turn_recovery(&self.connection, agent)
    }

    /// Recheck the turn, native subject, and source episode in the same
    /// transaction that applies the normal reducer and terminal effects.
    pub fn settle_recovered_turn(
        &mut self,
        captured: &Agent,
        binding: &ActiveTurnRecovery,
        outcome: RecoveredTurn,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = self.settle_recovered_turn_inner(captured, binding, outcome, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn settle_recovered_turn_inner(
        &mut self,
        captured: &Agent,
        binding: &ActiveTurnRecovery,
        outcome: RecoveredTurn,
        now: i64,
    ) -> Result<Vec<Event>> {
        let agent_num = parse_agent_id(&captured.id)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let Some(agent) = load_agent_by_num(&tx, agent_num)? else {
            return Ok(Vec::new());
        };
        if agent.generation != captured.generation
            || agent.native_scope != captured.native_scope
            || agent.native_ref != captured.native_ref
            || active_turn_recovery(&tx, &agent)?.as_ref() != Some(binding)
        {
            return Ok(Vec::new());
        }

        let native_ended = matches!(
            &outcome,
            RecoveredTurn::Terminal(_) | RecoveredTurn::NativeExited
        );
        let (kind, mut data) = match outcome {
            RecoveredTurn::Terminal(mut terminal) => {
                if binding.view_projection_gap {
                    terminal[core::VIEW_RECOVERY_MARKER_FIELD] =
                        json!(core::VIEW_RECOVERY_READBACK);
                }
                ("turn.completed", terminal)
            }
            unknown => {
                let reason = if binding.view_projection_gap {
                    core::MUSE_VIEW_UNKNOWN_REASON_PREFIX
                } else if matches!(unknown, RecoveredTurn::NativeExited) {
                    "native_exit_without_session_terminal"
                } else {
                    "Native snapshot did not prove the turn outcome within the recovery budget"
                };
                ("turn.unknown", json!({"reason": reason}))
            }
        };
        if let Some(command_id) = &binding.command_id {
            data["command_id"] = json!(command_id);
        }
        data["turn_id"] = json!(binding.turn_id);
        data["source_event_seq"] = json!(binding.source_event_seq);
        // Size the final reply after adding the exact recovery bindings.
        core::limits::bound_terminal_text(&mut data, MAX_EVENT_BYTES);
        if encode_json(&data)?.len() > MAX_EVENT_BYTES
            && let Some(fields) = data.as_object_mut()
        {
            fields.remove("native_error");
            core::limits::bound_terminal_text(&mut data, MAX_EVENT_BYTES);
        }
        let native = NativeEvent {
            kind: kind.to_owned(),
            native_id: format!("snapshot:{}", binding.source_event_seq),
            data,
            ts: None,
        };
        let (next, effects) = core::apply(&agent, &native, now)?;
        let cause = verified_native_event_cause_tx(
            &tx,
            &agent,
            agent_num,
            agent.generation,
            &native,
            &effects,
        )?;
        let mut event = DaemonEventInput::new(
            now,
            kind,
            "turn",
            Some(agent_num),
            Some(agent.generation),
            native.data.clone(),
        );
        event.cause = cause.clone();
        let mut events = Vec::new();
        if native_ended {
            super::native_events::settle_unfinished_tools_tx(
                &tx,
                &agent,
                agent_num,
                &binding.turn_id,
                now,
                &mut events,
            )?;
        }
        events.push(insert_daemon_event_input(&tx, event)?);
        let (commands, provider_events) = persist_effects(
            &tx,
            &effects,
            agent_num,
            agent.generation,
            now,
            &native,
            &next,
        )?;
        for command in commands {
            events.push(insert_daemon_command_event(
                &tx,
                now,
                &command.kind(),
                Some(agent_num),
                Some(agent.generation),
                &command.command_id,
                command.data,
            )?);
        }
        for mut event in provider_events {
            if let Some(cause) = cause.as_ref() {
                annotate_new_event_cause_tx(&tx, &mut event, cause.clone())?;
            }
            events.push(event);
        }
        update_agent_projection(&tx, &next)?;
        if kind == "turn.completed" {
            super::goals::continue_stop_paused_goal_tx(&tx, agent_num, now, &mut events)?;
        }
        events.sort_by_key(|event| event.seq);
        commit_transaction(tx, &self.write_faulted)?;
        Ok(events)
    }
}

/// Whether this native event is the adapter's own `turn.completed` for the
/// exact turn whose latest settlement is an inconclusive monitor recovery or
/// the adapter's bound observation deadline in this live generation. The
/// terminal must name the same send. Loss, stop and Owner resolution retain
/// their ordinary refusal.
pub(super) fn resolves_recovered_turn_tx(
    tx: &Transaction<'_>,
    agent: &Agent,
    agent_num: i64,
    generation: i64,
    event: &NativeEvent,
) -> Result<bool> {
    let Some(turn_id) = event.data.get("turn_id").and_then(Value::as_str) else {
        return Ok(false);
    };
    let command_id = event.data.get("command_id").and_then(Value::as_str);
    if event.kind != "turn.completed"
        || agent.observe_only
        || agent.generation != generation
        || load_turn(tx, agent_num, generation, turn_id)?
            .is_none_or(|turn| turn.state != TurnState::Unknown)
    {
        return Ok(false);
    }
    let recovered: Option<bool> = tx
        .query_row(
            "SELECT json_extract(data, '$.command_id') IS ?4 AND (
                    (NOT ?6 AND source = 'daemon' AND json_type(data, '$.source_event_seq') IS 'integer')
                    OR (source = 'stream' AND json_extract(data, '$.reason') IS ?5)
                    OR (?7 AND ?4 IS NULL AND source = 'stream'
                        AND json_extract(data, '$.reason') IS ?8))
             FROM events INDEXED BY events_agent
             WHERE agent = ?1 AND generation = ?2 AND kind = 'turn.unknown'
               AND json_extract(data, '$.turn_id') = ?3
             ORDER BY seq DESC LIMIT 1",
            params![
                agent_num,
                generation,
                turn_id,
                command_id,
                core::limits::NATIVE_TERMINAL_DEADLINE_REASON,
                agent.active_turn.as_deref() == Some(turn_id),
                agent.state == AgentState::Idle && agent.active_turn.is_none(),
                core::limits::NATIVE_TERMINAL_UNCONFIRMED_REASON,
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    Ok(recovered == Some(true))
}

/// The `turn.resolved` event for a recovered turn resolved by its native
/// terminal, linked to the committed native event and its send.
pub(super) fn insert_native_resolution_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    terminal: &NativeEvent,
    native_event_seq: i64,
    cause: Option<core::EventCause>,
    now: i64,
) -> Result<Event> {
    let mut data = json!({
        "turn_id": terminal.data.get("turn_id"),
        "command_id": terminal.data.get("command_id"),
        "status": terminal.data.get("status"),
        "resolution_source": "native",
        "native_event_seq": native_event_seq,
    });
    if let Some(outcome) = terminal.data.get("outcome") {
        data["outcome"] = outcome.clone();
    }
    let mut event = DaemonEventInput::new(
        now,
        "turn.resolved",
        "turn",
        Some(agent_num),
        Some(generation),
        data,
    );
    event.cause = cause;
    insert_daemon_event_input(tx, event)
}

fn active_turn_recovery(
    connection: &Connection,
    agent: &Agent,
) -> Result<Option<ActiveTurnRecovery>> {
    let Some(turn_id) = agent.active_turn.as_deref().filter(|_| {
        !agent.observe_only && matches!(agent.state, AgentState::Busy | AgentState::Waiting)
    }) else {
        return Ok(None);
    };
    let agent_num = parse_agent_id(&agent.id)?;
    if load_turn(connection, agent_num, agent.generation, turn_id)?
        .is_some_and(|turn| turn.state == TurnState::Unknown)
    {
        return Ok(None);
    }
    // A source can fail between send dispatch and the committed turn.started.
    // Bind the window to that same send, never to an unrelated historical gap.
    let command: Option<(Option<String>, i64, bool)> = connection
        .query_row(
            "SELECT COALESCE(json_extract(start.data, '$.command_id'), (
                 SELECT turn.start_command_id FROM turns turn
                 WHERE turn.agent = start.agent AND turn.generation = start.generation
                   AND turn.turn_id = json_extract(start.data, '$.turn_id')
             )), COALESCE((
                 SELECT dispatch.seq FROM events dispatch
                 WHERE dispatch.agent = start.agent AND dispatch.generation = start.generation
                 AND dispatch.seq < start.seq AND dispatch.kind = 'command.dispatched'
                 AND json_extract(dispatch.data, '$.command_id') = json_extract(start.data, '$.command_id')
                 ORDER BY dispatch.seq DESC LIMIT 1
             ), start.seq), json_extract(start.data, '$.native_trigger') IS NOT NULL
             FROM events start
             WHERE start.agent = ?1 AND start.generation = ?2 AND start.kind = 'turn.started'
             AND json_extract(start.data, '$.turn_id') = ?3
             ORDER BY start.seq DESC LIMIT 1",
            params![agent_num, agent.generation, turn_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let Some((command_id, start_seq, self_started)) = command else {
        return Ok(None);
    };
    if command_id.is_none() && !self_started {
        return Ok(None);
    }
    // Turn or source loss starts recovery. A gap naming only `message` is
    // coverage-only (one unpersisted message): the turn's outcome still
    // arrives on its healthy source, so it never starts outcome settlement.
    let trigger: Option<(i64, Option<String>)> = connection
        .query_row(
            "SELECT e.seq, json_extract(e.data, '$.reason')
             FROM events e INDEXED BY events_agent
             JOIN source_state s ON s.agent = e.agent AND s.generation = e.generation
                 AND s.source = json_extract(e.data, '$.source')
             WHERE e.agent = ?1 AND e.generation = ?2 AND e.seq > ?3
             AND s.source NOT IN ('daemon', 'os', 'git', 'workspace_process')
             AND (e.kind = 'source.gap' OR (e.kind = 'source.closed' AND s.state = 'closed')
                 OR (e.kind = 'source.health'
                 AND s.health IN ('stale', 'error')
                 AND json_extract(e.data, '$.health') IN ('stale', 'error')))
             AND EXISTS (
                 SELECT 1 FROM source_owners o
                 WHERE o.agent = s.agent AND o.generation = s.generation AND o.source = s.source
                 AND o.family IN ('session', 'turn', 'message', 'tool', 'request')
                 AND (e.kind != 'source.gap' OR (o.family != 'message'
                     AND o.family IN (SELECT value FROM json_each(e.data, '$.families'))))
             )
             AND NOT EXISTS (
                 SELECT 1 FROM events ok INDEXED BY events_agent
                 WHERE ok.agent = e.agent AND ok.seq > e.seq AND ok.generation = e.generation
                 AND ok.kind = 'source.health' AND json_extract(ok.data, '$.source') = s.source
                 AND json_extract(ok.data, '$.health') = 'ok'
             )
             ORDER BY (e.kind = 'source.gap') DESC, e.seq DESC LIMIT 1",
            params![agent_num, agent.generation, start_seq],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    Ok(
        trigger.map(|(source_event_seq, reason)| ActiveTurnRecovery {
            command_id,
            turn_id: turn_id.to_owned(),
            source_event_seq,
            view_projection_gap: eliot_core::api::HarnessCapabilities::of(&agent.harness)
                .view_projection_gap_recovery
                && reason.is_some_and(|value| {
                    value.starts_with(core::MUSE_VIEW_UNAVAILABLE_REASON_PREFIX)
                }),
        }),
    )
}
