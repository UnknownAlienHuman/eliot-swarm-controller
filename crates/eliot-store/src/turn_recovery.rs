use super::native_events::{persist_effects, verified_native_event_cause_tx};
use super::*;

#[derive(Debug, PartialEq, Eq)]
pub struct ActiveTurnRecovery {
    pub command_id: String,
    pub turn_id: String,
    source_event_seq: i64,
    muse_projection_gap: bool,
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

        let (kind, mut data) = match outcome {
            RecoveredTurn::Terminal(mut terminal) => {
                if binding.muse_projection_gap {
                    terminal[core::VIEW_RECOVERY_MARKER_FIELD] =
                        json!(core::VIEW_RECOVERY_READBACK);
                }
                ("turn.completed", terminal)
            }
            unknown => {
                let reason = if binding.muse_projection_gap {
                    core::MUSE_VIEW_UNKNOWN_REASON_PREFIX
                } else if matches!(unknown, RecoveredTurn::NativeExited) {
                    "native_exit_without_session_terminal"
                } else {
                    "Native snapshot did not prove the turn outcome within the recovery budget"
                };
                ("turn.unknown", json!({"reason": reason}))
            }
        };
        data["command_id"] = json!(binding.command_id);
        data["turn_id"] = json!(binding.turn_id);
        data["source_event_seq"] = json!(binding.source_event_seq);
        // Optional recovered output must not prevent retaining terminal proof;
        // dropped reply text keeps the reason it is missing.
        if encode_json(&data)?.len() > MAX_EVENT_BYTES
            && let Some(fields) = data.as_object_mut()
        {
            if fields
                .remove("final_text")
                .is_some_and(|text| text.is_string())
            {
                fields.insert("final_text_unavailable".to_owned(), json!("event_budget"));
            }
            fields.remove("native_error");
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
        let mut events = vec![insert_daemon_event_input(&tx, event)?];
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
                &format!("command.{}", command_state_name(command.state)),
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
        events.sort_by_key(|event| event.seq);
        update_agent_projection(&tx, &next)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(events)
    }
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
    // A source can fail between send dispatch and the committed turn.started.
    // Bind the window to that same send, never to an unrelated historical gap.
    let command: Option<(String, i64)> = connection
        .query_row(
            "SELECT json_extract(start.data, '$.command_id'), COALESCE((
                 SELECT dispatch.seq FROM events dispatch
                 WHERE dispatch.agent = start.agent AND dispatch.generation = start.generation
                 AND dispatch.seq < start.seq AND dispatch.kind = 'command.dispatched'
                 AND json_extract(dispatch.data, '$.command_id') = json_extract(start.data, '$.command_id')
                 ORDER BY dispatch.seq DESC LIMIT 1
             ), start.seq)
             FROM events start
             WHERE start.agent = ?1 AND start.generation = ?2 AND start.kind = 'turn.started'
             AND json_extract(start.data, '$.turn_id') = ?3
             ORDER BY start.seq DESC LIMIT 1",
            params![agent_num, agent.generation, turn_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let Some((command_id, start_seq)) = command else {
        return Ok(None);
    };
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
            muse_projection_gap: agent.harness == "muse"
                && reason.is_some_and(|value| {
                    value.starts_with(core::MUSE_VIEW_UNAVAILABLE_REASON_PREFIX)
                }),
        }),
    )
}
