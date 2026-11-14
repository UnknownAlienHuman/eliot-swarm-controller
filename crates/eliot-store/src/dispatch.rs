use super::*;

mod targetless;

/// Open a dispatch: load the command record and reject states that can no
/// longer be dispatched.
fn load_dispatchable_tx(tx: &Transaction<'_>, id: &str) -> Result<CommandRecord> {
    let command = load_command_record(tx, id)?
        .ok_or_else(|| Error::new("not_found", format!("command {id} was not found")))?;
    if !matches!(command.state, CommandState::Accepted | CommandState::Queued) {
        return Err(Error::new(
            "bad_state",
            format!(
                "command {} in state {:?} cannot be dispatched",
                command.command.id, command.state
            ),
        ));
    }
    Ok(command)
}

/// Target gates shared by every Agent-bound verb: Agent load, project
/// scope, generation binding, observer control, and provider holds.
fn dispatch_target_tx(tx: &Transaction<'_>, command: &CommandRecord) -> Result<(i64, Agent, i64)> {
    let agent_num = command
        .target
        .ok_or_else(|| Error::new("bad_state", "L1 dispatch requires a concrete Agent target"))?;
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", "command target Agent was not found"))?;
    projects::validate_project_command_dispatch_tx(tx, command)?;
    let command_generation = command.command.generation.unwrap_or(agent.generation);
    if !matches!(command.command.verb.as_str(), "attach" | "detach") {
        observer::require_controlled(&agent)?;
    }
    if matches!(command.command.verb.as_str(), "launch" | "send" | "steer") {
        provider_holds::require_dispatch_tx(tx, &agent, &command.command)?;
    }
    Ok((agent_num, agent, command_generation))
}

/// Rule-action hold gate: a held or cancelled command is reloaded and
/// returned for the caller to commit; Ready continues dispatch.
fn dispatch_rule_hold_tx(
    tx: &Transaction<'_>,
    id: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<Option<Command>> {
    match rules::gate_rule_action_dispatch_tx(tx, id, now, events)? {
        rules::RuleActionDispatchGate::Ready => Ok(None),
        rules::RuleActionDispatchGate::Held | rules::RuleActionDispatchGate::Cancelled => {
            let command = load_command(tx, id)?
                .ok_or_else(|| Error::new("internal", "gated command disappeared"))?;
            Ok(Some(command))
        }
    }
}

/// Dispatch one `launch`: first generation, native snapshot, and starting
/// state.
#[allow(clippy::too_many_arguments)]
fn dispatch_launch_tx(
    tx: &Transaction<'_>,
    id: &str,
    command: &CommandRecord,
    agent: &Agent,
    agent_num: i64,
    command_generation: i64,
    native_store: Option<(&Agent, &NativeStoreBinding)>,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    if command.state != CommandState::Accepted
        || agent.generation != 0
        || command_generation != 0
        || agent.state != AgentState::Created
    {
        return Err(Error::new(
            "stale_generation",
            "launch no longer targets the initial created Agent generation",
        ));
    }
    workspace::gate_workspace_start(tx, agent, &command.command, 1)?;
    capacity::require_reservation_tx(tx, agent_num, 1)?;
    let snapshot = native_store
        .map(|(expected, binding)| {
            workspace::launch_snapshot_with_native_store(agent, expected, binding, id)
        })
        .transpose()?;
    let token = format!("{}{}", Ulid::new(), Ulid::new());
    let changed = db!(tx.execute(
        "UPDATE agents SET generation = 1, state = 'starting', token = ?1,
                                 updated_at = ?2,
                                 profile_snapshot = COALESCE(?3, profile_snapshot)
             WHERE id = ?4 AND generation = 0 AND state = 'created'",
        params![token, now, snapshot, agent_num],
    ));
    if changed != 1 {
        return Err(Error::new(
            "stale_generation",
            "launch target changed before its generation token was committed",
        ));
    }
    install_initial_source_plan(tx, agent_num, 1, now)?;
    update_command_state(tx, id, CommandState::Dispatched, None, Some(1), now)?;
    events.push(insert_daemon_event_for_command(
        tx,
        id,
        DaemonEventInput::new(
            now,
            "agent.starting",
            "agent",
            Some(agent_num),
            Some(1),
            json!({"agent_id": agent.id, "command_id": id}),
        ),
    )?);
    events.push(insert_daemon_command_event(
        tx,
        now,
        "command.dispatched",
        Some(agent_num),
        Some(1),
        id,
        json!({"command_id": id, "verb": "launch"}),
    )?);
    Ok(())
}

/// Dispatch one `resume`: close the old generation, carry the native
/// subject forward, and cancel superseded commands.
#[allow(clippy::too_many_arguments)]
fn dispatch_resume_tx(
    tx: &Transaction<'_>,
    id: &str,
    command: &CommandRecord,
    agent: &Agent,
    agent_num: i64,
    command_generation: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let old_generation = command_generation;
    if !matches!(command.state, CommandState::Accepted | CommandState::Queued)
        || command_generation != agent.generation
        || command
            .command
            .expected_generation
            .is_some_and(|expected| expected != agent.generation)
        || !matches!(
            agent.state,
            AgentState::Stopped | AgentState::Failed | AgentState::Lost
        )
        || agent.active_turn.is_some()
    {
        return Err(Error::new(
            "stale_generation",
            "resume no longer targets the captured terminal Agent generation",
        ));
    }
    // Control-verb support is owned by the PluginHost manifest
    // check; only generation, state and pending work are rechecked.
    if has_pending_resume_tx(tx, agent_num, old_generation, id)? {
        return Err(Error::new(
            "bad_state",
            "another pending resume targets this Agent generation",
        ));
    }
    if has_in_flight_stop_tx(tx, agent_num, old_generation)? {
        return Err(Error::new(
            "bad_state",
            "a dispatched or unknown Stop still targets this Agent generation",
        ));
    }
    let native_scope = agent
        .native_scope
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::new("unsupported", "resume has no retained native subject"))?;
    let native_ref = agent
        .native_ref
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::new("unsupported", "resume has no retained native subject"))?;
    let next_generation = agent
        .generation
        .checked_add(1)
        .ok_or_else(|| Error::new("bad_state", "Agent generation cannot be incremented"))?;
    workspace::gate_workspace_start(tx, agent, &command.command, next_generation)?;
    capacity::require_reservation_tx(tx, agent_num, next_generation)?;
    for old_id in pending_command_ids(tx, agent_num, agent.generation)? {
        if old_id == id {
            continue;
        }
        let outcome = json!({"reason": "generation_resumed"});
        update_command_state(
            tx,
            &old_id,
            CommandState::Cancelled,
            Some(&outcome),
            None,
            now,
        )?;
        events.push(insert_daemon_command_event(
            tx,
            now,
            "command.cancelled",
            Some(agent_num),
            Some(agent.generation),
            &old_id,
            json!({"command_id": old_id, "reason": "generation_resumed"}),
        )?);
    }
    db!(tx.execute(
        "UPDATE source_state SET state = 'closed', updated_at = ?1
         WHERE agent = ?2 AND generation = ?3 AND state = 'open' AND source NOT IN ('os', 'git', 'workspace_process')",
        params![now, agent_num, agent.generation],
    ));
    db!(tx.execute(
        "UPDATE pending_requests SET state = 'stale'
         WHERE agent = ?1 AND generation = ?2 AND state = 'pending'",
        params![agent_num, agent.generation],
    ));
    let token = format!("{}{}", Ulid::new(), Ulid::new());
    let changed = db!(tx.execute(
        "UPDATE agents
         SET generation = ?1, state = 'starting', active_turn = NULL,
             pending = '[]', token = ?2, updated_at = ?3
         WHERE id = ?4 AND generation = ?5
           AND state IN ('stopped', 'failed', 'lost')
           AND active_turn IS NULL AND native_scope = ?6 AND native_ref = ?7",
        params![
            next_generation,
            token,
            now,
            agent_num,
            agent.generation,
            native_scope,
            native_ref
        ],
    ));
    if changed != 1 {
        return Err(Error::new(
            "stale_generation",
            "resume target changed before its new generation was committed",
        ));
    }
    install_initial_source_plan(tx, agent_num, next_generation, now)?;
    update_command_state(
        tx,
        id,
        CommandState::Dispatched,
        None,
        Some(next_generation),
        now,
    )?;
    events.push(insert_daemon_event_for_command(
        tx,
        id,
        DaemonEventInput::new(
            now,
            "agent.starting",
            "agent",
            Some(agent_num),
            Some(next_generation),
            json!({
                "agent_id": agent.id,
                "command_id": id,
                "resume_from": {
                    "native_scope": native_scope,
                    "native_ref": native_ref
                }
            }),
        ),
    )?);
    events.push(insert_daemon_command_event(
        tx,
        now,
        "command.dispatched",
        Some(agent_num),
        Some(next_generation),
        id,
        json!({"command_id": id, "verb": "resume"}),
    )?);
    Ok(())
}

/// Dispatch one `worktree.remove`: same-repository writes fence it, and a
/// still-accepted command queues instead of dispatching. Returns the queued
/// command when the caller must commit early.
#[allow(clippy::too_many_arguments)]
fn dispatch_worktree_remove_tx(
    tx: &Transaction<'_>,
    id: &str,
    command: &CommandRecord,
    agent: &Agent,
    agent_num: i64,
    command_generation: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<Option<Command>> {
    let mut workspace = workspace::load_workspace(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", "managed worktree was not found"))?;
    workspace::require_workspace_removable(tx, agent, &workspace)?;
    if command_generation != agent.generation
        || command
            .command
            .args
            .get("target_generation")
            .and_then(Value::as_i64)
            != Some(workspace.target_generation)
    {
        return Err(Error::new(
            "stale_generation",
            "worktree removal binding changed",
        ));
    }
    if repo_git_write_blocked_tx(tx, id, &workspace.repo_common_dir)? {
        if command.state == CommandState::Accepted {
            update_command_state(tx, id, CommandState::Queued, None, None, now)?;
            events.push(insert_daemon_command_event(
                tx,
                now,
                "command.queued",
                Some(agent_num),
                Some(agent.generation),
                id,
                json!({
                    "command_id": id,
                    "verb": "worktree.remove",
                    "reason": "same_repository_write_pending",
                }),
            )?);
        }
        let queued = load_command(tx, id)?
            .ok_or_else(|| Error::new("internal", "queued removal disappeared"))?;
        return Ok(Some(queued));
    }
    let old = workspace.clone();
    workspace.phase = WorkspacePhase::Removing;
    workspace.outcome = Some(
        json!({"stage": "removal", "command_id": id, "previous_phase": old.phase,
            "native_git_write_dispatched": false}),
    );
    workspace.updated_at = now;
    workspace::persist_workspace(tx, &old, &workspace)?;
    update_command_state(tx, id, CommandState::Dispatched, None, None, now)?;
    events.push(workspace::workspace_event(tx, &workspace, now)?);
    events.push(insert_daemon_command_event(
        tx,
        now,
        "command.dispatched",
        Some(agent_num),
        Some(agent.generation),
        id,
        json!({"command_id": id, "verb": "worktree.remove"}),
    )?);
    Ok(None)
}

/// Dispatch one `send`: generation, turn, settings-lane, and FIFO gates,
/// then input claiming and the frozen configuration revision. Returns the
/// queued command when a gate is not yet open.
#[allow(clippy::too_many_arguments)]
fn dispatch_send_tx(
    tx: &Transaction<'_>,
    id: &str,
    command: &CommandRecord,
    agent: &Agent,
    agent_num: i64,
    command_generation: i64,
    now: i64,
    context_items: Option<&mut Vec<core::ContextItem>>,
    events: &mut Vec<Event>,
) -> Result<Option<Command>> {
    if command_generation != agent.generation {
        return Err(Error::new(
            "stale_generation",
            format!(
                "command targets generation {command_generation}, current is {}",
                agent.generation
            ),
        ));
    }
    if command
        .expected_turn_id
        .as_deref()
        .is_some_and(|expected| agent.active_turn.as_deref() != Some(expected))
    {
        return Err(Error::new(
            "stale_generation",
            "send command's captured turn is no longer current",
        ));
    }
    if agent.is_terminal() {
        return Err(Error::new(
            "bad_state",
            "cannot dispatch input to a terminal Agent",
        ));
    }
    let ready = send_gate_open(tx, agent_num, agent.generation)?;
    let earliest = earliest_pending_send(tx, agent_num, agent.generation)?;
    // Shared admission order: only a configure admitted no later
    // than this send holds the settings lane. A later configure
    // never blocks an earlier send, so a send dispatched now
    // always carries the revision current at its own dispatch.
    let own_key = command_admission_key(tx, id)?;
    let settings_held = earliest_pending_configure_key(tx, agent_num, agent.generation)?
        .is_some_and(|(_, created_at, rowid)| (created_at, rowid) <= own_key);
    if !ready || settings_held || earliest.as_deref().is_some_and(|first| first != id) {
        if command.state == CommandState::Accepted {
            update_command_state(tx, id, CommandState::Queued, None, None, now)?;
            events.push(insert_daemon_command_event(
                tx,
                now,
                "command.queued",
                Some(agent_num),
                Some(agent.generation),
                id,
                json!({"command_id": id, "verb": "send"}),
            )?);
        }
        let queued = load_command_record(tx, id)?
            .ok_or_else(|| Error::new("internal", "queued command disappeared"))?;
        return Ok(Some(queued.command));
    }
    let claimed_items = if context_items.is_some() {
        context::claim_next_input_tx(tx, agent_num, agent, &command.command, now, events)?
    } else {
        Vec::new()
    };
    // Freeze the revision this send runs under: the latest
    // retained revision at its own dispatch. A retry or
    // reassociation keeps this stamp, so a send committed
    // before an overlay never runs under newer configuration.
    let revision = current_config_revision_tx(tx, agent_num, agent.generation)?;
    db!(tx.execute(
        "UPDATE commands SET config_revision = ?1 WHERE id = ?2",
        params![revision, id],
    ));
    let send_args = context::send_args(&command.command.args, &claimed_items)?;
    check_rpc_frame(
        "agent.command",
        &json!({
            "agent_id": agent.id,
            "generation": agent.generation,
            "command_id": id,
            "verb": "send",
            "args": send_args,
        }),
    )?;
    update_command_state(tx, id, CommandState::Dispatched, None, None, now)?;
    events.push(insert_daemon_command_event(
        tx,
        now,
        "command.dispatched",
        Some(agent_num),
        Some(agent.generation),
        id,
        json!({"command_id": id, "verb": "send"}),
    )?);
    if let Some(output) = context_items {
        *output = claimed_items;
    }
    Ok(None)
}

/// Dispatch one `stop`: generation and turn binding, then state change.
#[allow(clippy::too_many_arguments)]
fn dispatch_stop_tx(
    tx: &Transaction<'_>,
    id: &str,
    command: &CommandRecord,
    agent: &Agent,
    agent_num: i64,
    command_generation: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    if command_generation != agent.generation {
        return Err(Error::new(
            "stale_generation",
            format!(
                "command targets generation {command_generation}, current is {}",
                agent.generation
            ),
        ));
    }
    if command
        .expected_turn_id
        .as_deref()
        .is_some_and(|expected| agent.active_turn.as_deref() != Some(expected))
    {
        return Err(Error::new(
            "stale_generation",
            "stop command's captured turn is no longer current",
        ));
    }
    check_rpc_frame(
        "agent.stop",
        &json!({
            "agent_id": agent.id,
            "generation": agent.generation,
            "command_id": id,
            "force": command.command.args.get("force").and_then(Value::as_bool).unwrap_or(false),
        }),
    )?;
    update_command_state(tx, id, CommandState::Dispatched, None, None, now)?;
    events.push(insert_daemon_command_event(
        tx,
        now,
        "command.dispatched",
        Some(agent_num),
        Some(agent.generation),
        id,
        json!({"command_id": id, "verb": "stop"}),
    )?);
    Ok(())
}

/// Dispatch one `configure`: the settings lane holds behind earlier
/// configures and unresolved sends. Returns the queued command when the
/// lane is held.
#[allow(clippy::too_many_arguments)]
fn dispatch_configure_tx(
    tx: &Transaction<'_>,
    id: &str,
    command: &CommandRecord,
    agent: &Agent,
    agent_num: i64,
    command_generation: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<Option<Command>> {
    if command_generation != agent.generation
        || command
            .command
            .expected_generation
            .is_some_and(|expected| expected != agent.generation)
    {
        return Err(Error::new(
            "stale_generation",
            format!(
                "configure targets generation {command_generation}, current is {}",
                agent.generation
            ),
        ));
    }
    if agent.is_terminal()
        || !matches!(
            agent.state,
            AgentState::Idle | AgentState::Busy | AgentState::Waiting
        )
    {
        return Err(Error::new(
            "bad_state",
            "configure requires a live Agent generation with a native session",
        ));
    }
    // Shared admission order: an earlier configure still pending
    // holds the lane, and earlier send input that has not frozen
    // its revision or association holds it too. The Router waits
    // for the lane release and redispatches; a queued configure
    // keeps its admission and assigns its revision only when it
    // actually applies.
    let own_key = command_admission_key(tx, id)?;
    let earliest = earliest_pending_configure_key(tx, agent_num, agent.generation)?;
    let earlier_configure = earliest.as_ref().is_some_and(|(first, _, _)| first != id);
    let earlier_send = earliest_unresolved_send_key(tx, agent_num, agent.generation)?
        .is_some_and(|(_, created_at, rowid)| (created_at, rowid) < own_key);
    if earlier_configure || earlier_send {
        if command.state == CommandState::Accepted {
            update_command_state(tx, id, CommandState::Queued, None, None, now)?;
            events.push(insert_daemon_command_event(
                tx,
                now,
                "command.queued",
                Some(agent_num),
                Some(agent.generation),
                id,
                json!({"command_id": id, "verb": "configure"}),
            )?);
        }
        let queued = load_command_record(tx, id)?
            .ok_or_else(|| Error::new("internal", "queued command disappeared"))?;
        return Ok(Some(queued.command));
    }
    check_rpc_frame(
        "agent.command",
        &json!({
            "agent_id": agent.id,
            "generation": agent.generation,
            "command_id": id,
            "verb": "configure",
            "args": command.command.args,
        }),
    )?;
    update_command_state(tx, id, CommandState::Dispatched, None, None, now)?;
    events.push(insert_daemon_command_event(
        tx,
        now,
        "command.dispatched",
        Some(agent_num),
        Some(agent.generation),
        id,
        json!({"command_id": id, "verb": "configure"}),
    )?);
    Ok(None)
}

/// Dispatch one active-turn `inject`: expiry settles rejected, otherwise the
/// context is claimed. Queued injects are never dispatched directly.
#[allow(clippy::too_many_arguments)]
fn dispatch_inject_tx(
    tx: &Transaction<'_>,
    id: &str,
    command: &CommandRecord,
    agent: &Agent,
    agent_num: i64,
    command_generation: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let args = context::decode_inject_args(&command.command.args)?;
    if args.point != InjectPoint::ActiveTurn {
        return Err(Error::new(
            "bad_state",
            "queued inject commands are claimed, not dispatched directly",
        ));
    }
    if command_generation != agent.generation
        || command
            .command
            .expected_generation
            .is_some_and(|expected| expected != agent.generation)
    {
        return Err(Error::new(
            "stale_generation",
            "active_turn inject targets a stale Agent generation",
        ));
    }
    if !matches!(agent.state, AgentState::Busy | AgentState::Waiting) {
        return Err(Error::new(
            "bad_state",
            "active_turn inject requires a busy or waiting Agent",
        ));
    }
    let turn_id = command.command.turn_id.as_deref().ok_or_else(|| {
        Error::new(
            "source_integrity",
            "active_turn inject has no captured turn",
        )
    })?;
    if command
        .expected_turn_id
        .as_deref()
        .is_some_and(|expected| expected != turn_id)
        || agent.active_turn.as_deref() != Some(turn_id)
    {
        return Err(Error::new(
            "stale_generation",
            "active_turn inject no longer targets the exact active turn",
        ));
    }
    if command.command.request_id.is_some() {
        return Err(Error::new(
            "source_integrity",
            "active_turn inject unexpectedly has a native request id",
        ));
    }
    provider_holds::require_dispatch_tx(tx, agent, &command.command)?;
    if args
        .expires_at_ms
        .is_some_and(|expires_at| expires_at <= now)
    {
        let outcome = json!({"code": "expired"});
        update_command_state(tx, id, CommandState::Rejected, Some(&outcome), None, now)?;
        events.push(insert_daemon_command_event(
            tx,
            now,
            "command.rejected",
            Some(agent_num),
            Some(agent.generation),
            id,
            json!({"command_id": id, "state": "rejected", "outcome": outcome}),
        )?);
    } else {
        check_rpc_frame(
            "agent.command",
            &json!({
                "agent_id": agent.id,
                "generation": agent.generation,
                "command_id": id,
                "verb": "inject",
                "turn_id": turn_id,
                "args": command.command.args,
            }),
        )?;
        context::claim_context_tx(
            tx,
            id,
            context::ContextConsumer {
                id,
                point: InjectPoint::ActiveTurn,
            },
            agent_num,
            agent.generation,
            now,
            events,
        )?;
    }
    Ok(())
}

/// Dispatch one turn-control command (`steer`, `interrupt`, `approve`,
/// `deny`, `answer`): recheck the captured turn and pending request.
#[allow(clippy::too_many_arguments)]
fn dispatch_control_tx(
    tx: &Transaction<'_>,
    id: &str,
    command: &CommandRecord,
    agent: &Agent,
    agent_num: i64,
    command_generation: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    if command_generation != agent.generation
        || command
            .command
            .expected_generation
            .is_some_and(|expected| expected != agent.generation)
    {
        return Err(Error::new(
            "stale_generation",
            "control command targets a stale Agent generation",
        ));
    }
    // Control-verb support is owned by the PluginHost manifest
    // check; only generation, state, turn and request are rechecked.
    if !matches!(agent.state, AgentState::Busy | AgentState::Waiting) {
        return Err(Error::new(
            "bad_state",
            "control command requires a busy or waiting Agent",
        ));
    }
    let turn_id =
        command.command.turn_id.as_deref().ok_or_else(|| {
            Error::new("source_integrity", "control command has no captured turn")
        })?;
    if command
        .expected_turn_id
        .as_deref()
        .is_some_and(|expected| expected != turn_id)
        || agent.active_turn.as_deref() != Some(turn_id)
    {
        return Err(Error::new(
            "stale_generation",
            "control command no longer targets the exact active turn",
        ));
    }
    if matches!(command.command.verb.as_str(), "approve" | "deny" | "answer") {
        let request_id = command.command.request_id.as_deref().ok_or_else(|| {
            Error::new(
                "source_integrity",
                "request reply has no captured request id",
            )
        })?;
        let Some((pending, state)) =
            load_pending_request(tx, agent_num, agent.generation, request_id)?
        else {
            return Err(Error::new(
                "stale_generation",
                "captured native request is no longer present",
            ));
        };
        if state != "pending"
            || pending.turn_id.as_str() != turn_id
            || !agent.pending.contains(&pending)
        {
            return Err(Error::new(
                "stale_generation",
                "request reply no longer matches its exact pending request",
            ));
        }
        if has_open_request_reply(tx, agent_num, agent.generation, request_id, Some(id))? {
            return Err(Error::new(
                "bad_state",
                "another reply command is unresolved for this native request",
            ));
        }
    } else if command.command.request_id.is_some() {
        return Err(Error::new(
            "source_integrity",
            "turn control unexpectedly has a native request id",
        ));
    }
    let mut params = json!({
        "agent_id": agent.id,
        "generation": agent.generation,
        "command_id": id,
        "verb": command.command.verb,
        "turn_id": turn_id,
        "args": command.command.args,
    });
    if let Some(request_id) = command.command.request_id.as_deref() {
        params["request_id"] = Value::String(request_id.to_owned());
    }
    check_rpc_frame("agent.command", &params)?;
    update_command_state(tx, id, CommandState::Dispatched, None, None, now)?;
    events.push(insert_daemon_command_event(
        tx,
        now,
        "command.dispatched",
        Some(agent_num),
        Some(agent.generation),
        id,
        json!({
            "command_id": id,
            "verb": command.command.verb,
            "turn_id": turn_id,
            "request_id": command.command.request_id
        }),
    )?);
    Ok(())
}

impl Store {
    pub(super) fn dispatch_inner(
        &mut self,
        id: &str,
        now: i64,
        native_store: Option<(&Agent, &NativeStoreBinding)>,
        context_items: Option<&mut Vec<core::ContextItem>>,
    ) -> Result<Change<Command>> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let command = load_dispatchable_tx(&tx, id)?;
        let mut events = Vec::new();
        if native_store.is_some() && command.command.verb != "launch" {
            return Err(Error::new(
                "bad_state",
                "native store binding can be appended only at launch dispatch",
            ));
        }
        if let Some(command) = targetless::dispatch_tx(&tx, id, &command, now, &mut events)? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value: command,
                events,
            });
        }
        let (agent_num, agent, command_generation) = dispatch_target_tx(&tx, &command)?;
        if let Some(value) = dispatch_rule_hold_tx(&tx, id, now, &mut events)? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change { value, events });
        }

        let held: Option<Command> = match command.command.verb.as_str() {
            "attach" | "detach" => {
                observer::dispatch(&tx, &command, &agent, now, &mut events)?;
                None
            }
            "launch" => {
                dispatch_launch_tx(
                    &tx,
                    id,
                    &command,
                    &agent,
                    agent_num,
                    command_generation,
                    native_store,
                    now,
                    &mut events,
                )?;
                None
            }
            "resume" => {
                dispatch_resume_tx(
                    &tx,
                    id,
                    &command,
                    &agent,
                    agent_num,
                    command_generation,
                    now,
                    &mut events,
                )?;
                None
            }
            "worktree.remove" => dispatch_worktree_remove_tx(
                &tx,
                id,
                &command,
                &agent,
                agent_num,
                command_generation,
                now,
                &mut events,
            )?,
            "send" => dispatch_send_tx(
                &tx,
                id,
                &command,
                &agent,
                agent_num,
                command_generation,
                now,
                context_items,
                &mut events,
            )?,
            "stop" => {
                dispatch_stop_tx(
                    &tx,
                    id,
                    &command,
                    &agent,
                    agent_num,
                    command_generation,
                    now,
                    &mut events,
                )?;
                None
            }
            "configure" => dispatch_configure_tx(
                &tx,
                id,
                &command,
                &agent,
                agent_num,
                command_generation,
                now,
                &mut events,
            )?,
            "inject" => {
                dispatch_inject_tx(
                    &tx,
                    id,
                    &command,
                    &agent,
                    agent_num,
                    command_generation,
                    now,
                    &mut events,
                )?;
                None
            }
            "steer" | "interrupt" | "approve" | "deny" | "answer" => {
                dispatch_control_tx(
                    &tx,
                    id,
                    &command,
                    &agent,
                    agent_num,
                    command_generation,
                    now,
                    &mut events,
                )?;
                None
            }
            verb => {
                return Err(Error::new(
                    "unsupported",
                    format!("Store does not dispatch the {verb} verb"),
                ));
            }
        };

        if let Some(value) = held {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change { value, events });
        }
        let command = load_command_record(&tx, id)?
            .ok_or_else(|| Error::new("internal", "dispatched command disappeared"))?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value: command.command,
            events,
        })
    }
}
