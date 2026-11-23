use super::status_views::AgentStatusLookup;
use super::*;
use eliot_core::{is_request_reply_verb, is_start_verb};
use std::collections::HashMap;

const AGENT_NOTICE_BATCH_SQL: &str = "WITH request AS (
        SELECT CAST(json_extract(value, '$[0]') AS INTEGER) AS agent,
               CAST(json_extract(value, '$[1]') AS INTEGER) AS generation,
               json_extract(value, '$[2]') AS active_turn,
               CAST(json_extract(value, '$[3]') AS INTEGER) AS last_progress_at
        FROM json_each(?1)
    ), targets AS MATERIALIZED (
        SELECT r.agent, r.generation, r.active_turn, r.last_progress_at,
               COALESCE(r.active_turn,
                        CASE WHEN json_type(t.data, '$.turn_id') = 'text'
                             THEN json_extract(t.data, '$.turn_id') END) AS turn_id,
               t.kind AS latest_kind, t.ts AS latest_ts, t.data AS latest_data
        FROM request AS r
        LEFT JOIN events AS t ON t.seq = (
            SELECT e.seq FROM events AS e INDEXED BY events_turn_lifecycle
            WHERE r.active_turn IS NULL
              AND e.agent = r.agent AND e.generation = r.generation
              AND e.kind IN ('turn.started', 'turn.completed', 'turn.unknown')
            ORDER BY e.seq DESC LIMIT 1
        )
    )
    SELECT t.agent, t.generation, t.active_turn, t.last_progress_at,
           t.turn_id, t.latest_kind, t.latest_ts, t.latest_data,
           n.ts, n.data
    FROM targets AS t
    LEFT JOIN events AS n ON n.seq = (
        SELECT e.seq FROM events AS e INDEXED BY events_turn_notice
        WHERE t.turn_id IS NOT NULL AND e.agent = t.agent AND e.generation = t.generation
          AND e.kind = 'turn.notice'
          AND json_extract(e.data, '$.turn_id') = t.turn_id
        ORDER BY e.seq DESC LIMIT 1
    )";

pub(super) struct NativeNoticeRead {
    pub(super) notice: core::api::AgentNativeNotice,
    pub(super) provider_failure: bool,
}

/// Resolve each requested Agent's latest relevant notice in one page-scoped
/// query over the existing `(agent, seq)` event index.
pub(super) fn agent_native_notices(
    connection: &Connection,
    lookups: &[AgentStatusLookup],
) -> Result<Vec<Option<NativeNoticeRead>>> {
    if lookups.is_empty() {
        return Ok(Vec::new());
    }
    let request_rows: Vec<_> = lookups
        .iter()
        .map(|lookup| {
            (
                lookup.agent_num,
                lookup.generation,
                lookup.active_turn.as_deref(),
                lookup.last_progress_at,
            )
        })
        .collect();
    let encoded = encode_json(&request_rows)?;
    let mut statement = connection
        .prepare(AGENT_NOTICE_BATCH_SQL)
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![encoded], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<i64>>(8)?,
                row.get::<_, Option<String>>(9)?,
            ))
        })
        .map_err(sql_error)?;

    let mut notices = HashMap::with_capacity(lookups.len());
    for row in rows {
        let (
            agent_num,
            generation,
            active_turn,
            last_progress_at,
            turn_id,
            latest_kind,
            latest_ts,
            latest_data,
            notice_ts,
            notice_data,
        ) = row.map_err(sql_error)?;
        let latest_turn_data = if active_turn.is_none() {
            latest_data.as_deref().map(decode_json).transpose()?
        } else {
            None
        };
        let completed = active_turn.is_none() && latest_kind.as_deref() == Some("turn.completed");
        let terminal_outcome = latest_turn_data
            .as_ref()
            .and_then(|data| data.get("outcome"))
            .and_then(Value::as_str);
        if completed && terminal_outcome != Some("failure") {
            notices.insert((agent_num, generation), None);
            continue;
        }
        let mut notice = None;
        if let Some(turn_id) = turn_id.as_deref() {
            if completed
                && let (Some(ts), Some(data)) = (latest_ts, latest_turn_data.as_ref())
                && let Some(terminal) = terminal_native_notice(generation, ts, turn_id, data)
            {
                terminal.validate()?;
                notice = Some(terminal);
            }
            if notice.is_none() {
                notice = native_notice_from_event(generation, turn_id, notice_ts.zip(notice_data))?;
            }
        }
        if active_turn.is_some()
            && notice.as_ref().is_some_and(|notice| {
                last_progress_at.is_some_and(|progress| progress > notice.observed_at)
            })
        {
            notice = None;
        }
        let provider_origin = latest_turn_data
            .as_ref()
            .and_then(|data| data.get("origin"))
            .and_then(Value::as_str)
            == Some(core::FailureOrigin::Provider.as_str());
        notices.insert(
            (agent_num, generation),
            notice.map(|notice| NativeNoticeRead {
                provider_failure: provider_origin || notice.provider_kind.is_some(),
                notice,
            }),
        );
    }

    lookups
        .iter()
        .map(|lookup| {
            notices
                .remove(&(lookup.agent_num, lookup.generation))
                .ok_or_else(|| Error::new("internal", "Agent notice page is incomplete"))
        })
        .collect()
}

fn native_notice_from_event(
    generation: i64,
    turn_id: &str,
    event: Option<(i64, String)>,
) -> Result<Option<eliot_core::api::AgentNativeNotice>> {
    let Some((ts, raw)) = event else {
        return Ok(None);
    };
    let data: Value = decode_json(&raw)?;
    let notice: eliot_core::api::NativeNotice = serde_json::from_value(data)
        .map_err(|_| Error::new("source_integrity", "retained native notice is invalid"))?;
    notice.validate()?;
    if notice.turn_id != turn_id {
        return Err(Error::new(
            "source_integrity",
            "retained native notice turn binding changed",
        ));
    }
    let (message, truncated) = bounded_notice_message(notice.message.as_deref(), notice.truncated);
    let view = eliot_core::api::AgentNativeNotice {
        generation,
        turn_id: notice.turn_id,
        kind: notice.kind,
        provider_kind: notice.provider_kind,
        message,
        truncated,
        observed_at: ts,
    };
    view.validate()?;
    Ok(Some(view))
}

fn terminal_native_notice(
    generation: i64,
    ts: i64,
    turn_id: &str,
    data: &Value,
) -> Option<eliot_core::api::AgentNativeNotice> {
    let error = data.get("native_error")?;
    let message = error.get("message").and_then(Value::as_str);
    let truncated = error
        .get("truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let (message, truncated) = bounded_notice_message(message, truncated);
    Some(eliot_core::api::AgentNativeNotice {
        generation,
        turn_id: turn_id.to_owned(),
        kind: eliot_core::api::NativeNoticeKind::Error,
        provider_kind: data
            .get("provider_restriction")
            .and_then(|value| {
                serde_json::from_value::<eliot_core::ProviderRestriction>(value.clone()).ok()
            })
            .map(|restriction| restriction.kind),
        message,
        truncated,
        observed_at: ts,
    })
}

fn bounded_notice_message(
    message: Option<&str>,
    already_truncated: bool,
) -> (Option<String>, bool) {
    let Some(message) = message else {
        return (None, already_truncated);
    };
    let normalized: String = message
        .chars()
        .filter(|character| !character.is_control() || character.is_whitespace())
        .collect();
    let normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return (None, already_truncated);
    }
    let mut chars = normalized.chars();
    let bounded = chars
        .by_ref()
        .take(eliot_core::api::AGENT_NOTICE_MESSAGE_CHARS)
        .collect::<String>();
    let truncated = already_truncated || chars.next().is_some();
    (Some(bounded), truncated)
}

pub(super) struct PendingCommandEvent {
    pub(super) command_id: String,
    pub(super) state: CommandState,
    pub(super) data: Value,
}

impl PendingCommandEvent {
    pub(super) fn kind(&self) -> String {
        if self.data["outcome"]["delivery"] == "moved" {
            "context.moved".to_owned()
        } else {
            format!("command.{}", command_state_name(self.state))
        }
    }
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
    if native_event.kind == "turn.model_observed"
        && let Some(event) = provider_holds::model_mismatch_tx(tx, next_agent, native_event, now)?
    {
        provider_events.push(event);
    }
    if matches!(
        native_event.kind.as_str(),
        "hook.before_tool" | "hook.after_tool" | "hook.before_model"
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
            Effect::NativeTurnStarted { turn_id, trigger } => {
                persist_native_turn_started(scope, turn_id, trigger)?;
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
                persist_message_completed(scope, command_id.as_deref(), turn_id, text)?;
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
                if native_event.kind == "session.ended" {
                    settle_unfinished_tools_tx(
                        tx,
                        next_agent,
                        agent_num,
                        turn_id,
                        now,
                        &mut provider_events,
                    )?;
                }
                if command_id.is_some() || native_event.kind == "turn.completed" {
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
    if native_event.kind == "tool.started"
        && let Some(turn_id) = native_event
            .data
            .get("turn_id")
            .and_then(Value::as_str)
            .or(next_agent.active_turn.as_deref())
    {
        db!(tx.execute(
            "UPDATE turns SET tool_calls = tool_calls + 1
             WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3
               AND state IN ('running', 'unknown')",
            params![agent_num, generation, turn_id],
        ));
    }
    if matches!(
        native_event.kind.as_str(),
        "turn.model_observed" | "tool.started" | "tool.completed"
    ) && let Some(turn_id) = native_event.data["turn_id"].as_str()
    {
        confirm_inline_mail_tx(tx, agent_num, generation, turn_id, now)?;
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
        next_agent,
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
    if native_event.kind == "turn.input_bound" {
        // The harness merged this send into its running self-started turn:
        // the existing turn gains its command instead of a second turn.
        let changed = tx
            .execute(
                "UPDATE turns SET start_command_id = ?1
                 WHERE agent = ?2 AND generation = ?3 AND turn_id = ?4 AND state = 'running'
                   AND native_trigger IS NOT NULL AND start_command_id IS NULL",
                params![command_id, agent_num, generation, turn_id],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(Error::new(
                "source_integrity",
                "turn.input_bound does not target a running unbound native turn",
            ));
        }
    } else {
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
                start_command_id: Some(command_id),
                state: TurnState::Running,
                status: None,
                outcome: None,
                native_status: None,
                final_text: None,
                started_at: Some(now),
                completed_at: None,
                usage: None,
                config_revision: command.config_revision,
                preceded_by: None,
                effective_model: (command.config_revision == 0)
                    .then_some(next_agent.effective.as_ref())
                    .flatten()
                    .and_then(|effective| effective.get("model"))
                    .and_then(Value::as_str),
                native_trigger: None,
            },
        )?;
    }
    // A SEND dispatched before Stop may acquire its exact turn after Stop
    // dispatch. Capture the association before any drained session terminal
    // clears the Agent's active-turn pointer.
    db!(tx.execute(
        "UPDATE commands SET expected_turn_id = ?1
         WHERE target = ?2 AND generation = ?3 AND verb = 'stop' AND principal = 'owner'
           AND state IN ('dispatched','unknown') AND expected_turn_id IS NULL
           AND EXISTS (
             SELECT 1 FROM events AS stop_dispatch
             WHERE stop_dispatch.agent=?2 AND stop_dispatch.generation=?3
               AND stop_dispatch.kind='command.dispatched'
               AND json_extract(stop_dispatch.data,'$.command_id')=commands.id
               AND stop_dispatch.seq > (
                 SELECT MAX(send_dispatch.seq) FROM events AS send_dispatch
                 WHERE send_dispatch.agent=?2 AND send_dispatch.generation=?3
                   AND send_dispatch.kind='command.dispatched'
                   AND json_extract(send_dispatch.data,'$.command_id')=?4
               )
           )",
        params![turn_id, agent_num, generation, command_id],
    ));
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
    update_owner_rules_input_status(
        tx,
        agent_num,
        command_id,
        OwnerRulesDeliveryStatus::Delivered,
    )?;
    handoff::refresh_parent_tx(tx, command_id, now)?;
    // Native acceptance starts the wake cooldown and acknowledges only
    // the inline deliveries bound to this input.
    db!(tx.execute(
        "UPDATE mail_notices SET last_notice_at = ?1
         WHERE recipient = ?2 AND open_notice_command = ?3",
        params![now, agent_num, command_id],
    ));
    ack_inline_mail_tx(tx, agent_num, generation, command_id, now)?;
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

/// Record a turn the harness started by itself. It runs under the current
/// configuration revision and has no command until a send binds to it.
fn persist_native_turn_started(
    scope: EffectScope<'_, '_>,
    turn_id: &str,
    trigger: &str,
) -> Result<()> {
    let EffectScope {
        tx,
        agent_num,
        generation,
        now,
        next_agent,
        ..
    } = scope;
    if load_turn(tx, agent_num, generation, turn_id)?.is_some() {
        return Err(Error::new(
            "source_integrity",
            format!("turn {turn_id} already exists in this Agent generation"),
        ));
    }
    let config_revision = current_config_revision_tx(tx, agent_num, generation)?;
    insert_turn(
        tx,
        TurnInsert {
            agent: agent_num,
            generation,
            turn_id,
            start_command_id: None,
            state: TurnState::Running,
            status: None,
            outcome: None,
            native_status: None,
            final_text: None,
            started_at: Some(now),
            completed_at: None,
            usage: None,
            config_revision,
            preceded_by: None,
            effective_model: (config_revision == 0)
                .then_some(next_agent.effective.as_ref())
                .flatten()
                .and_then(|effective| effective.get("model"))
                .and_then(Value::as_str),
            native_trigger: Some(trigger),
        },
    )
}

/// A turn fact without a command is valid only for the exact running
/// self-started turn no send has bound to.
fn require_unbound_native_turn(scope: EffectScope<'_, '_>, turn_id: &str) -> Result<()> {
    if scope.next_agent.active_turn.as_deref() == Some(turn_id)
        && native_terminal_for_unbound_turn(scope, turn_id)?
    {
        return Ok(());
    }
    Err(Error::new(
        "source_integrity",
        format!(
            "{} has no command for a send-bound turn",
            scope.native_event.kind
        ),
    ))
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
    if pending && native_event.data.get("command_id").is_none() {
        require_unbound_native_turn(scope, turn_id)?;
        return persist_request_opened(scope, None, request_id, turn_id, kind, details);
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
            Some((&command, &command_id)),
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
    command: Option<(&CommandRecord, &str)>,
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
    if let Some((command, command_id)) = command {
        require_effect_command(command, agent_num, generation, "send", command_id)?;
    }
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
    if command.is_some_and(|(command, _)| {
        command.command.state != CommandState::Applied
            || command.command.turn_id.as_deref() != Some(turn_id)
    }) {
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
    if !is_request_reply_verb(command.command.verb.as_str())
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
    settle_pending_request_tx(
        tx,
        agent_num,
        generation,
        request_id,
        turn_id,
        "request.resolved",
        "resolution",
    )?;
    Ok(())
}

/// Settle one exact pending request as resolved: the durable row must still
/// be `pending` under this turn binding, and exactly one row must flip.
fn settle_pending_request_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    request_id: &str,
    turn_id: &str,
    event: &str,
    stored_detail: &str,
) -> Result<()> {
    let Some((existing, state)) = load_pending_request(tx, agent_num, generation, request_id)?
    else {
        return Err(Error::new(
            "source_integrity",
            format!("{event} references no durable pending request"),
        ));
    };
    if state != "pending" || existing.turn_id.as_str() != turn_id {
        return Err(Error::new(
            "source_integrity",
            format!("{event} does not match its durable pending binding"),
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
            format!("pending request changed before {stored_detail} was stored"),
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
    settle_pending_request_tx(
        tx,
        agent_num,
        generation,
        request_id,
        turn_id,
        "request.closed",
        "native cleanup",
    )?;
    Ok(())
}

fn persist_message_completed(
    scope: EffectScope<'_, '_>,
    command_id: Option<&str>,
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
    if let Some(command_id) = command_id {
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
    } else {
        require_unbound_native_turn(scope, turn_id)?;
    }
    let changed = tx
        .execute(
            "UPDATE turns SET final_text = ?1, messages = messages + 1
             WHERE agent = ?2 AND generation = ?3 AND turn_id = ?4
               AND state IN ('running', 'unknown')",
            params![text, agent_num, generation, turn_id],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new(
            "source_integrity",
            "message.completed does not target the exact running turn",
        ));
    }
    if scope.native_event.data["model_output"].as_bool() != Some(false) {
        confirm_inline_mail_tx(
            scope.tx,
            scope.agent_num,
            scope.generation,
            turn_id,
            scope.now,
        )?;
    }
    Ok(())
}

fn ack_inline_mail_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    command_id: &str,
    now: i64,
) -> Result<()> {
    db!(tx.execute(
        "UPDATE messages SET presented_at = ?1, read_at = ?1, inline_ack_pending = 1
         WHERE recipient = ?2 AND recipient_generation = ?3
           AND presentation_command = ?4 AND presented_at IS NULL AND read_at IS NULL",
        params![now, agent_num, generation, command_id],
    ));
    Ok(())
}

fn confirm_inline_mail_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    turn: &str,
    now: i64,
) -> Result<()> {
    let Some(command) = command_id_for_turn_tx(tx, agent, generation, turn)? else {
        return Ok(());
    };
    // A late exact terminal can establish delivery after an unknown was
    // recorded, as long as a later input has not claimed these same rows.
    ack_inline_mail_tx(tx, agent, generation, &command, now)?;
    db!(tx.execute(
        "INSERT OR IGNORE INTO attention_wakes(recipient, thread, woken_at)
         SELECT DISTINCT recipient, thread, ?1 FROM messages
         WHERE recipient = ?2 AND recipient_generation = ?3
           AND presentation_command = ?4 AND presented_at IS NOT NULL AND wake = 1
           AND thread LIKE 'attention-%' AND thread NOT LIKE 'attention-%-advisory'",
        params![now, agent, generation, command],
    ));
    db!(tx.execute(
        "UPDATE messages SET inline_ack_pending = 0
         WHERE recipient = ?1 AND recipient_generation = ?2 AND presentation_command = ?3
           AND inline_ack_pending = 1",
        params![agent, generation, command],
    ));
    Ok(())
}

/// Roll back just the acknowledgements made at input association that still
/// lack model output or an explicit mailbox acknowledgement.
pub(super) fn rollback_inline_mail_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    turn: &str,
) -> Result<()> {
    let Some(command) = command_id_for_turn_tx(tx, agent, generation, turn)? else {
        return Ok(());
    };
    db!(tx.execute(
        "UPDATE messages SET presented_at = NULL, read_at = NULL, inline_ack_pending = 0,
             inline_wake_retries = MIN(2, inline_wake_retries + wake)
         WHERE recipient = ?1 AND recipient_generation = ?2 AND presentation_command = ?3
           AND inline_ack_pending = 1",
        params![agent, generation, command],
    ));
    Ok(())
}

/// Retain the unresolved end of each committed tool start for an exact turn
/// whose completion or custody loss has already been established.
pub(super) fn settle_unfinished_tools_tx(
    tx: &Transaction<'_>,
    agent: &Agent,
    agent_num: i64,
    turn: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    if !core::api::HarnessCapabilities::of(&agent.harness).reconcile_unfinished_tools {
        return Ok(());
    }
    let mut statement = tx
        .prepare(
            "SELECT json_extract(s.data, '$.item_id'), MIN(json_extract(s.data, '$.tool'))
         FROM events s
         WHERE s.agent = ?1 AND s.generation = ?2 AND s.source = 'stream'
           AND s.kind = 'tool.started' AND s.native_id IS NOT NULL
           AND json_extract(s.data, '$.turn_id') = ?3
           AND json_type(s.data, '$.item_id') = 'text'
           AND NOT EXISTS (
               SELECT 1 FROM events terminal
               WHERE terminal.agent = s.agent AND terminal.generation = s.generation
                 AND terminal.kind = 'tool.completed' AND terminal.seq > s.seq
                 AND json_extract(terminal.data, '$.turn_id') = ?3
                 AND json_extract(terminal.data, '$.item_id') = json_extract(s.data, '$.item_id')
           )
         GROUP BY json_extract(s.data, '$.item_id')",
        )
        .map_err(sql_error)?;
    let tools = statement
        .query_map(params![agent_num, agent.generation, turn], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    for (item_id, tool) in tools {
        events.push(insert_daemon_event(
            tx,
            now,
            "tool.completed",
            "tool",
            Some(agent_num),
            Some(agent.generation),
            json!({"turn_id": turn, "item_id": item_id, "tool": tool,
                "status": "unknown", "reason": "turn_settled_without_native_tool_terminal"}),
        )?);
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "The operation's distinct inputs remain explicit at its established call boundary."
)]
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
        native_event,
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
    } else if native_event.kind == "turn.completed"
        && !native_terminal_for_unbound_turn(scope, turn_id)?
    {
        return Err(Error::new(
            "source_integrity",
            "turn.completed has no command for a send-bound turn",
        ));
    }
    // A recovered turn resolved by its native terminal (only the Store's
    // recovered-turn check admits it): the settlement's unknown outcome and
    // predecessor give way to the terminal's own.
    if load_turn(tx, agent_num, generation, turn_id)?
        .is_some_and(|turn| turn.state == TurnState::Unknown)
    {
        if native_event.kind == "turn.completed" && state == TurnState::Completed {
            db!(tx.execute(
                "UPDATE turns SET outcome = NULL, preceded_by = NULL
                 WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3 AND state = 'unknown'",
                params![agent_num, generation, turn_id],
            ));
        } else if native_event.kind != "session.ended" || state != TurnState::Unknown {
            return Err(Error::new(
                "source_integrity",
                "an unknown turn resolves only from its native turn.completed",
            ));
        }
    }
    let usage = usage.map(encode_json).transpose()?;
    let outcome = outcome.map(|outcome| outcome.as_str());
    // A non-success terminal outcome reports on the bound command: record
    // it as the causal predecessor. Success needs no explanation, and
    // without a bound command there is nothing to link.
    let preceded_by = match outcome {
        Some("failure") | Some("interrupted") | Some("unknown") => command_id,
        _ => None,
    };
    let native_error_message = native_event
        .data
        .pointer("/native_error/message")
        .and_then(Value::as_str)
        .filter(|message| !message.is_empty());
    let effective_model = eliot_core::api::HarnessCapabilities::of(&next_agent.harness)
        .turn_completed_reports_effective_model
        .then(|| {
            native_event
                .data
                .get("effective_model")
                .and_then(Value::as_str)
        })
        .flatten();
    let native_followup_pending = native_event.kind == "turn.completed"
        && native_event.data["native_followup_pending"].as_bool() == Some(true);
    let changed = tx
        .execute(
            "UPDATE turns SET state = ?1, status = COALESCE(?2, status),
                              outcome = COALESCE(?3, outcome),
                              native_status = COALESCE(?4, native_status),
                              final_text = COALESCE(?5, final_text), completed_at = ?6,
                              usage = COALESCE(?7, usage),
                              preceded_by = COALESCE(?8, preceded_by),
                              native_error_message = COALESCE(?9, native_error_message),
                              effective_model = COALESCE(?10, effective_model),
                              native_followup_pending = ?11
             WHERE agent = ?12 AND generation = ?13 AND turn_id = ?14",
            params![
                turn_state_name(state),
                status,
                outcome,
                native_status,
                final_text,
                completed_at,
                usage,
                preceded_by,
                native_error_message,
                effective_model,
                native_followup_pending,
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
    let delivered = outcome == Some("success")
        || (final_text.is_some_and(|text| !text.trim().is_empty())
            && native_event.data["model_output"].as_bool() != Some(false));
    if delivered {
        confirm_inline_mail_tx(tx, agent_num, generation, turn_id, now)?;
    } else {
        rollback_inline_mail_tx(tx, agent_num, generation, turn_id)?;
    }
    if native_event.kind == "turn.unknown" && next_agent.active_turn.as_deref() == Some(turn_id) {
        return Ok(Vec::new());
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
    context::settle_ended_turn_context_tx(tx, agent_num, generation, turn_id, now)
}

/// A commandless native terminal: the running unbound turn, or that turn
/// after the monitor's recovery settled it `unknown`.
fn native_terminal_for_unbound_turn(scope: EffectScope<'_, '_>, turn_id: &str) -> Result<bool> {
    scope
        .tx
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM turns
                 WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3
                   AND state IN ('running', 'unknown')
                   AND native_trigger IS NOT NULL AND start_command_id IS NULL
             )",
            params![scope.agent_num, scope.generation, turn_id],
            |row| row.get(0),
        )
        .map_err(sql_error)
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
    persist_session_usage_snapshot(tx, agent_num, generation, now, usage)
}

/// Replace the generation's current session snapshot in the caller's
/// transaction. Native journal events and live non-journal samples share this
/// monotonic projection update.
pub(super) fn persist_session_usage_snapshot(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    now: i64,
    usage: &SessionUsage,
) -> Result<()> {
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
            && is_start_verb(command.command.verb.as_str())
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
            command_id: Some(command_id),
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
        Effect::SessionUsageUpdated { .. }
        | Effect::NativeTurnStarted { .. }
        | Effect::MessageCompleted {
            command_id: None, ..
        } => Ok(None),
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
        && is_request_reply_verb(command.command.verb.as_str())
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
