use super::*;

pub(super) struct PendingCommandEvent {
    pub(super) command_id: String,
    pub(super) state: CommandState,
    pub(super) data: Value,
}

/// Inputs shared by the native-effect persistence helpers, in the caller's transaction.
#[derive(Clone, Copy)]
struct EffectScope<'a, 'b> {
    tx: &'a Transaction<'b>,
    agent_num: i64,
    generation: i64,
    now: i64,
    native_event: &'a NativeEvent,
    next_agent: &'a Agent,
}

pub(super) fn persist_effects(
    tx: &Transaction<'_>,
    effects: &[Effect],
    agent_num: i64,
    generation: i64,
    now: i64,
    native_event: &NativeEvent,
    next_agent: &Agent,
) -> Result<(Vec<PendingCommandEvent>, Vec<Event>)> {
    let scope = EffectScope {
        tx,
        agent_num,
        generation,
        now,
        native_event,
        next_agent,
    };
    let mut command_events = Vec::new();
    let mut provider_events = Vec::new();
    if matches!(
        native_event.kind.as_str(),
        "hook.before_tool" | "hook.before_model"
    ) {
        command_events.extend(context::settle_turn_safe_point_claims_tx(
            tx,
            agent_num,
            generation,
            native_event,
            now,
        )?);
    }
    for effect in effects {
        match effect {
            Effect::CommandTurnAssociated {
                command_id,
                turn_id,
                effort,
            } => {
                command_events.extend(persist_turn_associated(scope, command_id, turn_id, effort)?);
            }
            Effect::RequestPending {
                request_id,
                turn_id,
                pending,
                kind,
                details,
            } => {
                persist_request_effect(
                    scope,
                    request_id,
                    turn_id,
                    *pending,
                    kind.as_deref(),
                    details.as_ref(),
                )?;
            }
            Effect::RequestClosed {
                request_id,
                turn_id,
            } => {
                persist_request_closed(scope, request_id, turn_id)?;
            }
            Effect::MessageCompleted {
                command_id,
                turn_id,
                text,
            } => {
                persist_message_completed(scope, command_id, turn_id, text)?;
            }
            Effect::TurnTerminal {
                command_id,
                turn_id,
                state,
                status,
                outcome,
                native_status,
                final_text,
                usage,
                completed_at,
            } => {
                command_events.extend(persist_turn_terminal(
                    scope,
                    command_id.as_deref(),
                    turn_id,
                    *state,
                    status.as_deref(),
                    *outcome,
                    native_status.as_deref(),
                    final_text.as_deref(),
                    usage.as_ref(),
                    *completed_at,
                )?);
                if command_id.is_some() {
                    provider_events.extend(provider_holds::terminal_tx(
                        scope.tx,
                        scope.next_agent,
                        effect,
                        scope.native_event,
                        scope.now,
                    )?);
                }
            }
            Effect::SessionUsageUpdated { usage } => {
                persist_session_usage(scope, usage)?;
            }
        }
    }
    Ok((command_events, provider_events))
}

fn persist_turn_associated(
    scope: EffectScope<'_, '_>,
    command_id: &str,
    turn_id: &str,
    resolved_effort: &Option<String>,
) -> Result<Vec<PendingCommandEvent>> {
    let EffectScope {
        tx,
        agent_num,
        generation,
        now,
        native_event,
        ..
    } = scope;
    let command = load_command_record(tx, command_id)?.ok_or_else(|| {
        Error::new(
            "source_integrity",
            "turn.started references an unknown command",
        )
    })?;
    require_effect_command(&command, agent_num, generation, "send", command_id)?;
    if !matches!(
        command.command.state,
        CommandState::Dispatched | CommandState::Unknown
    ) || !core::command_transition_allowed(command.state, CommandState::Applied)
        || command.command.turn_id.is_some()
        || command
            .expected_turn_id
            .as_deref()
            .is_some_and(|expected| expected != turn_id)
    {
        return Err(Error::new(
            "source_integrity",
            "turn.started does not match an unresolved send command and expected turn",
        ));
    }
    if load_turn(tx, agent_num, generation, turn_id)?.is_some() {
        return Err(Error::new(
            "source_integrity",
            format!("turn {turn_id} already exists in this Agent generation"),
        ));
    }
    insert_turn(
        tx,
        TurnInsert {
            agent: agent_num,
            generation,
            turn_id,
            state: TurnState::Running,
            status: None,
            outcome: None,
            native_status: None,
            final_text: None,
            started_at: Some(now),
            completed_at: None,
            usage: None,
            config_revision: command.config_revision,
        },
    )?;
    // The applied send outcome repeats what the native turn request
    // carried, so the first turn under an overlay shows its effective
    // model/effort in its readback. Unchanged without an overlay.
    let mut outcome = json!({"turn_id": turn_id});
    if command.config_revision > 0 {
        let mut config = get_agent_config_tx(tx, agent_num, generation, command.config_revision)?
            .ok_or_else(|| {
            Error::new(
                "source_integrity",
                "send command revision has no retained agent_config",
            )
        })?;
        // A plugin-reported effort is the native request's resolved value
        // (for example the native readback a model-only configure left out
        // of the overlay). Recording it keeps `requested` (what the owner
        // asked) separate from `effective` (what the turn carried), so the
        // turn and the generation readback show the effort actually sent.
        // Absent reports invent nothing: the overlay stands as stored.
        if let Some(resolved) = resolved_effort.as_deref()
            && config.effort.as_deref() != Some(resolved)
        {
            db!(tx.execute(
                "UPDATE agent_config SET effective = json_set(effective, '$.effort', ?3)
                 WHERE agent = ?1 AND generation = ?2 AND revision = ?4",
                params![agent_num, generation, resolved, config.revision],
            ));
            config.effort = Some(resolved.to_owned());
        }
        outcome["config_revision"] = json!(config.revision);
        outcome["model"] = config
            .model
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null);
        outcome["effort"] = config
            .effort
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null);
    }
    db!(tx.execute(
        "UPDATE commands SET state = 'applied', turn_id = ?1, outcome = ?2,
                             updated_at = ?3 WHERE id = ?4",
        params![turn_id, encode_json(&outcome)?, now, command_id],
    ));
    // Presentation stamps the notice cooldown at native acceptance, so a long
    // queue cannot spend it while waiting. Ordinary sends match no notice row.
    db!(tx.execute(
        "UPDATE mail_notices SET last_notice_at = ?1
         WHERE recipient = ?2 AND open_notice_command = ?3",
        params![now, agent_num, command_id],
    ));
    let mut command_events = vec![PendingCommandEvent {
        command_id: command_id.to_owned(),
        state: CommandState::Applied,
        data: json!({
            "command_id": command_id, "state": "applied", "outcome": outcome,
        }),
    }];
    command_events.extend(context::apply_next_input_claims_tx(
        tx,
        agent_num,
        generation,
        command_id,
        turn_id,
        native_event,
        now,
    )?);
    Ok(command_events)
}

fn persist_request_effect(
    scope: EffectScope<'_, '_>,
    request_id: &str,
    turn_id: &str,
    pending: bool,
    kind: Option<&str>,
    details: Option<&Value>,
) -> Result<()> {
    let EffectScope {
        tx,
        agent_num,
        generation,
        native_event,
        ..
    } = scope;
    let expected_kind = if pending {
        "request.opened"
    } else {
        "request.resolved"
    };
    if native_event.kind != expected_kind
        || required_string(&native_event.data, "request_id")?.as_str() != request_id
        || required_string(&native_event.data, "turn_id")?.as_str() != turn_id
    {
        return Err(Error::new(
            "source_integrity",
            "request effect is not bound to its exact native request and turn",
        ));
    }
    let command_id = required_string(&native_event.data, "command_id")?;
    let command = load_command_record(tx, &command_id)?.ok_or_else(|| {
        Error::new(
            "source_integrity",
            "request event references an unknown command",
        )
    })?;
    if command.target != Some(agent_num) || command.command.generation != Some(generation) {
        return Err(Error::new(
            "source_integrity",
            "request event command is bound to another Agent or generation",
        ));
    }
    if pending {
        persist_request_opened(
            scope,
            &command,
            &command_id,
            request_id,
            turn_id,
            kind,
            details,
        )
    } else {
        persist_request_resolved(scope, &command, request_id, turn_id, kind, details)
    }
}

fn persist_request_opened(
    scope: EffectScope<'_, '_>,
    command: &CommandRecord,
    command_id: &str,
    request_id: &str,
    turn_id: &str,
    kind: Option<&str>,
    details: Option<&Value>,
) -> Result<()> {
    let EffectScope {
        tx,
        agent_num,
        generation,
        next_agent,
        ..
    } = scope;
    require_effect_command(command, agent_num, generation, "send", command_id)?;
    let kind =
        kind.ok_or_else(|| Error::new("source_integrity", "request.opened is missing its kind"))?;
    let details = details
        .ok_or_else(|| Error::new("source_integrity", "request.opened is missing its details"))?;
    require_nonempty(kind, "request kind")?;
    if encode_json(details)?.len() > MAX_EVENT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "pending request details exceed 64 KiB",
        ));
    }
    if command.command.state != CommandState::Applied
        || command.command.turn_id.as_deref() != Some(turn_id)
    {
        return Err(Error::new(
            "source_integrity",
            "request.opened is not bound to its exact applied send turn",
        ));
    }
    let projected = PendingRequest {
        id: request_id.to_owned(),
        turn_id: turn_id.to_owned(),
        kind: kind.to_owned(),
        details: details.clone(),
    };
    if !next_agent.pending.contains(&projected) {
        return Err(Error::new(
            "source_integrity",
            "request.opened is missing from the next Agent projection",
        ));
    }
    match load_pending_request(tx, agent_num, generation, request_id)? {
        Some((old, state)) if state == "pending" && old == projected => {}
        Some((old, state))
            if state == "pending"
                && old.turn_id == projected.turn_id
                && old.kind == projected.kind =>
        {
            let details = encode_json(details)?;
            db!(tx.execute(
                "UPDATE pending_requests SET details = ?1
                 WHERE agent = ?2 AND generation = ?3 AND request_id = ?4
                   AND turn_id = ?5 AND state = 'pending'",
                params![details, agent_num, generation, request_id, turn_id],
            ));
        }
        Some(_) => {
            return Err(Error::new(
                "source_integrity",
                "request id was reused with another pending binding",
            ));
        }
        None => {
            let details = encode_json(details)?;
            db!(tx.execute(
                "INSERT INTO pending_requests(
                     agent, generation, request_id, turn_id, kind, details, state
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending')",
                params![agent_num, generation, request_id, turn_id, kind, details],
            ));
        }
    }
    Ok(())
}

fn persist_request_resolved(
    scope: EffectScope<'_, '_>,
    command: &CommandRecord,
    request_id: &str,
    turn_id: &str,
    kind: Option<&str>,
    details: Option<&Value>,
) -> Result<()> {
    let EffectScope {
        tx,
        agent_num,
        generation,
        next_agent,
        ..
    } = scope;
    if kind.is_some() || details.is_some() {
        return Err(Error::new(
            "source_integrity",
            "request.resolved unexpectedly carries open-request details",
        ));
    }
    if !matches!(command.command.verb.as_str(), "approve" | "deny" | "answer")
        || command.command.request_id.as_deref() != Some(request_id)
        || command.command.turn_id.as_deref() != Some(turn_id)
        || !matches!(
            command.state,
            CommandState::Dispatched
                | CommandState::Unknown
                | CommandState::Applied
                | CommandState::Failed
        )
    {
        return Err(Error::new(
            "source_integrity",
            "request.resolved is not bound to its exact dispatched reply",
        ));
    }
    if next_agent
        .pending
        .iter()
        .any(|request| request.id.as_str() == request_id && request.turn_id.as_str() == turn_id)
    {
        return Err(Error::new(
            "source_integrity",
            "request.resolved remains in the next Agent projection",
        ));
    }
    let Some((existing, state)) = load_pending_request(tx, agent_num, generation, request_id)?
    else {
        return Err(Error::new(
            "source_integrity",
            "request.resolved references no durable pending request",
        ));
    };
    if state != "pending" || existing.turn_id.as_str() != turn_id {
        return Err(Error::new(
            "source_integrity",
            "request.resolved does not match its durable pending binding",
        ));
    }
    let changed = tx
        .execute(
            "UPDATE pending_requests SET state = 'resolved'
             WHERE agent = ?1 AND generation = ?2 AND request_id = ?3
               AND turn_id = ?4 AND state = 'pending'",
            params![agent_num, generation, request_id, turn_id],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new(
            "source_integrity",
            "pending request changed before resolution was stored",
        ));
    }
    Ok(())
}

fn persist_request_closed(
    scope: EffectScope<'_, '_>,
    request_id: &str,
    turn_id: &str,
) -> Result<()> {
    let EffectScope {
        tx,
        agent_num,
        generation,
        native_event,
        next_agent,
        ..
    } = scope;
    if native_event.kind != "request.closed"
        || native_event.data.get("command_id").is_some()
        || required_string(&native_event.data, "request_id")?.as_str() != request_id
        || required_string(&native_event.data, "turn_id")?.as_str() != turn_id
        || next_agent
            .pending
            .iter()
            .any(|request| request.id.as_str() == request_id)
    {
        return Err(Error::new(
            "source_integrity",
            "request.closed is not an exact pending-request cleanup",
        ));
    }
    let Some((existing, state)) = load_pending_request(tx, agent_num, generation, request_id)?
    else {
        return Err(Error::new(
            "source_integrity",
            "request.closed references no durable pending request",
        ));
    };
    if state != "pending" || existing.turn_id.as_str() != turn_id {
        return Err(Error::new(
            "source_integrity",
            "request.closed does not match its durable pending binding",
        ));
    }
    let changed = tx
        .execute(
            "UPDATE pending_requests SET state = 'resolved'
             WHERE agent = ?1 AND generation = ?2 AND request_id = ?3
               AND turn_id = ?4 AND state = 'pending'",
            params![agent_num, generation, request_id, turn_id],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new(
            "source_integrity",
            "pending request changed before native cleanup was stored",
        ));
    }
    Ok(())
}

fn persist_message_completed(
    scope: EffectScope<'_, '_>,
    command_id: &str,
    turn_id: &str,
    text: &str,
) -> Result<()> {
    let EffectScope {
        tx,
        agent_num,
        generation,
        ..
    } = scope;
    if text.len() > MAX_EVENT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "final message exceeds 64 KiB",
        ));
    }
    let command = load_command_record(tx, command_id)?.ok_or_else(|| {
        Error::new(
            "source_integrity",
            "message.completed references an unknown command",
        )
    })?;
    require_effect_command(&command, agent_num, generation, "send", command_id)?;
    if command.command.turn_id.as_deref() != Some(turn_id)
        || command.command.state != CommandState::Applied
    {
        return Err(Error::new(
            "source_integrity",
            "message.completed is not bound to the command's exact active turn",
        ));
    }
    let changed = tx
        .execute(
            "UPDATE turns SET final_text = ?1
             WHERE agent = ?2 AND generation = ?3 AND turn_id = ?4
               AND state = 'running'",
            params![text, agent_num, generation, turn_id],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new(
            "source_integrity",
            "message.completed does not target the exact running turn",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn persist_turn_terminal(
    scope: EffectScope<'_, '_>,
    command_id: Option<&str>,
    turn_id: &str,
    state: TurnState,
    status: Option<&str>,
    outcome: Option<TurnOutcome>,
    native_status: Option<&str>,
    final_text: Option<&str>,
    usage: Option<&TurnUsage>,
    completed_at: i64,
) -> Result<Vec<PendingCommandEvent>> {
    let EffectScope {
        tx,
        agent_num,
        generation,
        now,
        next_agent,
        ..
    } = scope;
    if let Some(text) = final_text
        && text.len() > MAX_EVENT_BYTES
    {
        return Err(Error::new(
            "frame_too_large",
            "turn final text exceeds 64 KiB",
        ));
    }
    if let Some(command_id) = command_id {
        let command = load_command_record(tx, command_id)?.ok_or_else(|| {
            Error::new(
                "source_integrity",
                "turn terminal event references an unknown command",
            )
        })?;
        require_effect_command(&command, agent_num, generation, "send", command_id)?;
        if command.command.turn_id.as_deref() != Some(turn_id) {
            return Err(Error::new(
                "source_integrity",
                "turn terminal command is not associated with this exact turn",
            ));
        }
    }
    let usage = usage.map(encode_json).transpose()?;
    let outcome = outcome.map(|outcome| outcome.as_str());
    let changed = tx
        .execute(
            "UPDATE turns SET state = ?1, status = COALESCE(?2, status),
                              outcome = COALESCE(?3, outcome),
                              native_status = COALESCE(?4, native_status),
                              final_text = COALESCE(?5, final_text), completed_at = ?6,
                              usage = COALESCE(?7, usage)
             WHERE agent = ?8 AND generation = ?9 AND turn_id = ?10",
            params![
                turn_state_name(state),
                status,
                outcome,
                native_status,
                final_text,
                completed_at,
                usage,
                agent_num,
                generation,
                turn_id,
            ],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new(
            "source_integrity",
            format!("terminal event references missing turn {turn_id}"),
        ));
    }
    db!(tx.execute(
        "UPDATE pending_requests SET state = 'stale'
         WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3
           AND state = 'pending'",
        params![agent_num, generation, turn_id],
    ));
    if next_agent
        .pending
        .iter()
        .any(|request| request.turn_id.as_str() == turn_id)
    {
        return Err(Error::new(
            "source_integrity",
            "terminal turn retains pending requests in the Agent projection",
        ));
    }
    context::cancel_turn_safe_point_tx(tx, agent_num, generation, turn_id, now)
}

/// Persist one `session.usage` snapshot as the latest for its Agent
/// generation, in the caller's transaction. A strictly newer snapshot
/// replaces the retained one; an older or equal-timestamp snapshot or a
/// byte-identical duplicate is ignored, so replays and out-of-order
/// deliveries can never inflate or regress the readback. No history rows and
/// no rollups: only the latest snapshot reads back.
fn persist_session_usage(scope: EffectScope<'_, '_>, usage: &SessionUsage) -> Result<()> {
    let EffectScope {
        tx,
        agent_num,
        generation,
        now,
        ..
    } = scope;
    let snapshot = encode_json(usage)?;
    if snapshot.len() > MAX_EVENT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "session usage snapshot exceeds 64 KiB",
        ));
    }
    let observed_at = usage.observed_at.unwrap_or(now);
    if observed_at < 0 {
        return Err(Error::new(
            "invalid",
            "session usage observed_at must be nonnegative",
        ));
    }
    let stored: Option<(String, i64)> = tx
        .query_row(
            "SELECT snapshot, observed_at FROM session_usage WHERE agent = ?1 AND generation = ?2",
            params![agent_num, generation],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    match stored {
        None => {
            db!(tx.execute(
                "INSERT INTO session_usage(agent, generation, snapshot, observed_at)
                 VALUES (?1, ?2, ?3, ?4)",
                params![agent_num, generation, snapshot, observed_at],
            ));
        }
        Some((stored_snapshot, stored_at)) => {
            if stored_snapshot == snapshot || observed_at <= stored_at {
                return Ok(());
            }
            db!(tx.execute(
                "UPDATE session_usage SET snapshot = ?1, observed_at = ?2
                 WHERE agent = ?3 AND generation = ?4",
                params![snapshot, observed_at, agent_num, generation],
            ));
        }
    }
    Ok(())
}

pub(super) fn verified_native_event_cause_tx(
    tx: &Transaction<'_>,
    previous_agent: &Agent,
    agent_num: i64,
    generation: i64,
    native_event: &NativeEvent,
    effects: &[Effect],
) -> Result<Option<core::EventCause>> {
    if native_event.kind == "session.ready"
        && let Some(command_id) = native_event.data.get("command_id").and_then(Value::as_str)
    {
        let Some(command) = load_command_record(tx, command_id)? else {
            return Ok(None);
        };
        let bound = command.target == Some(agent_num)
            && command.command.generation == Some(generation)
            && matches!(command.command.verb.as_str(), "launch" | "resume")
            && matches!(
                command.state,
                CommandState::Dispatched | CommandState::Unknown
            );
        return if bound {
            command_cause_tx(tx, command_id)
        } else {
            Ok(None)
        };
    }
    if native_event.kind == "session.ended"
        && let Some(command_id) = native_event.data.get("command_id").and_then(Value::as_str)
        && let Some(command) = load_command_record(tx, command_id)?
    {
        let bound = command.target == Some(agent_num)
            && command.command.generation == Some(generation)
            && command.command.verb == "stop"
            && matches!(
                command.state,
                CommandState::Dispatched | CommandState::Unknown | CommandState::Applied
            );
        if bound {
            return command_cause_tx(tx, command_id);
        }
    }

    // core::apply currently emits at most one effect for a native event. Refuse
    // a future multi-effect fact until its causal command selection is explicit.
    if effects.len() > 1 {
        return Err(Error::new(
            "source_integrity",
            "native event has multiple reducer effects for causal attribution",
        ));
    }
    if let Some(effect) = effects.first() {
        return effect_cause_tx(tx, agent_num, generation, native_event, effect);
    }

    let family = family_for_kind(&native_event.kind)?;
    if matches!(family, "turn" | "tool" | "message" | "request")
        && let Some(turn_id) = previous_agent.active_turn.as_deref()
        && native_event.data.get("turn_id").and_then(Value::as_str) == Some(turn_id)
    {
        return command_cause_for_turn_tx(tx, agent_num, generation, turn_id);
    }
    Ok(None)
}

fn effect_cause_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    native_event: &NativeEvent,
    effect: &Effect,
) -> Result<Option<core::EventCause>> {
    match effect {
        Effect::CommandTurnAssociated {
            command_id,
            turn_id,
            ..
        } => {
            let Some(command) = load_command_record(tx, command_id)? else {
                return Ok(None);
            };
            let bound = command.target == Some(agent_num)
                && command.command.generation == Some(generation)
                && command.command.verb == "send"
                && matches!(
                    command.command.state,
                    CommandState::Dispatched | CommandState::Unknown
                )
                && command.command.turn_id.is_none()
                && command
                    .expected_turn_id
                    .as_deref()
                    .is_none_or(|expected| expected == turn_id);
            if bound {
                command_cause_tx(tx, command_id)
            } else {
                Ok(None)
            }
        }
        Effect::MessageCompleted {
            command_id,
            turn_id,
            ..
        }
        | Effect::TurnTerminal {
            command_id: Some(command_id),
            turn_id,
            ..
        } => {
            let Some(command) = load_command_record(tx, command_id)? else {
                return Ok(None);
            };
            let bound = command.target == Some(agent_num)
                && command.command.generation == Some(generation)
                && command.command.verb == "send"
                && command.command.turn_id.as_deref() == Some(turn_id.as_str())
                && command.command.state == CommandState::Applied;
            if bound {
                command_cause_tx(tx, command_id)
            } else {
                Ok(None)
            }
        }
        Effect::RequestPending {
            request_id,
            turn_id,
            pending: false,
            ..
        } => verified_request_reply_cause_tx(
            tx,
            agent_num,
            generation,
            native_event,
            request_id,
            turn_id,
        ),
        Effect::RequestPending { turn_id, .. }
        | Effect::RequestClosed { turn_id, .. }
        | Effect::TurnTerminal { turn_id, .. } => {
            command_cause_for_turn_tx(tx, agent_num, generation, turn_id)
        }
        Effect::SessionUsageUpdated { .. } => Ok(None),
    }
}

fn verified_request_reply_cause_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    native_event: &NativeEvent,
    request_id: &str,
    turn_id: &str,
) -> Result<Option<core::EventCause>> {
    if native_event.kind != "request.resolved"
        || native_event.data.get("request_id").and_then(Value::as_str) != Some(request_id)
        || native_event.data.get("turn_id").and_then(Value::as_str) != Some(turn_id)
    {
        return Ok(None);
    }
    let Some(command_id) = native_event.data.get("command_id").and_then(Value::as_str) else {
        return Ok(None);
    };
    let Some(command) = load_command_record(tx, command_id)? else {
        return Ok(None);
    };
    let bound = command.target == Some(agent_num)
        && command.command.generation == Some(generation)
        && matches!(command.command.verb.as_str(), "approve" | "deny" | "answer")
        && command.command.request_id.as_deref() == Some(request_id)
        && command.command.turn_id.as_deref() == Some(turn_id)
        && matches!(
            command.state,
            CommandState::Dispatched
                | CommandState::Unknown
                | CommandState::Applied
                | CommandState::Failed
        );
    if bound {
        command_cause_tx(tx, command_id)
    } else {
        Ok(None)
    }
}

fn require_effect_command(
    command: &CommandRecord,
    agent: i64,
    generation: i64,
    verb: &str,
    command_id: &str,
) -> Result<()> {
    if command.target != Some(agent)
        || command.command.generation != Some(generation)
        || command.command.verb != verb
    {
        return Err(Error::new(
            "source_integrity",
            format!("native event command {command_id} is bound to another Agent or generation"),
        ));
    }
    Ok(())
}
