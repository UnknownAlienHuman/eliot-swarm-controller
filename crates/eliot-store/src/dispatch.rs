use super::*;

impl Store {
    pub(super) fn dispatch_inner(
        &mut self,
        id: &str,
        now: i64,
        native_store: Option<(&Agent, &NativeStoreBinding)>,
    ) -> Result<Change<Command>> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let mut command = load_command_record(&tx, id)?
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
        let mut events = Vec::new();
        if native_store.is_some() && command.command.verb != "launch" {
            return Err(Error::new(
                "bad_state",
                "native store binding can be appended only at launch dispatch",
            ));
        }
        if command.command.verb == "run" {
            if command.target.is_some()
                || command.command.generation.is_some()
                || command.command.expected_generation.is_some()
                || command.command.turn_id.is_some()
                || command.command.request_id.is_some()
            {
                return Err(Error::new(
                    "source_integrity",
                    "stored run command has an Agent binding",
                ));
            }
            projects::validate_project_command_dispatch_tx(&tx, &command)?;
            scripts::validate_run_dispatch_tx(&tx, &command)?;
            match rules::gate_rule_action_dispatch_tx(&tx, id, now, &mut events)? {
                rules::RuleActionDispatchGate::Ready => {}
                rules::RuleActionDispatchGate::Held | rules::RuleActionDispatchGate::Cancelled => {
                    let command = load_command(&tx, id)?
                        .ok_or_else(|| Error::new("internal", "gated command disappeared"))?;
                    commit_transaction(tx, &self.write_faulted)?;
                    return Ok(Change {
                        value: command,
                        events,
                    });
                }
            }
            update_command_state(&tx, id, CommandState::Dispatched, None, None, now)?;
            events.push(insert_daemon_command_event(
                &tx,
                now,
                "command.dispatched",
                None,
                None,
                id,
                json!({"command_id": id, "verb": "run"}),
            )?);
            let command = load_command(&tx, id)?
                .ok_or_else(|| Error::new("internal", "dispatched run command disappeared"))?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value: command,
                events,
            });
        }
        if command.command.verb == "git.fetch" {
            if command.target.is_some()
                || command.command.generation.is_some()
                || command.command.expected_generation.is_some()
                || command.command.turn_id.is_some()
                || command.expected_turn_id.is_some()
                || command.command.request_id.is_some()
                || command.command.probe
            {
                return Err(Error::new(
                    "source_integrity",
                    "stored git.fetch command has an Agent binding",
                ));
            }
            let frozen = projects::decode_frozen_git_fetch(&command.command.args)?;
            projects::validate_git_fetch_intent_tx(&tx, &command.command.principal, &frozen)?;
            projects::validate_project_command_dispatch_tx(&tx, &command)?;
            match rules::gate_rule_action_dispatch_tx(&tx, id, now, &mut events)? {
                rules::RuleActionDispatchGate::Ready => {}
                rules::RuleActionDispatchGate::Held | rules::RuleActionDispatchGate::Cancelled => {
                    let command = load_command(&tx, id)?
                        .ok_or_else(|| Error::new("internal", "gated command disappeared"))?;
                    commit_transaction(tx, &self.write_faulted)?;
                    return Ok(Change {
                        value: command,
                        events,
                    });
                }
            }
            if repo_git_write_blocked_tx(&tx, id, &frozen.common_dir)? {
                if command.state == CommandState::Accepted {
                    update_command_state(&tx, id, CommandState::Queued, None, None, now)?;
                    events.push(insert_daemon_command_event(
                        &tx,
                        now,
                        "command.queued",
                        None,
                        None,
                        id,
                        json!({
                            "command_id": id,
                            "verb": "git.fetch",
                            "reason": "same_repository_write_pending",
                        }),
                    )?);
                }
                let command = load_command(&tx, id)?
                    .ok_or_else(|| Error::new("internal", "queued git.fetch disappeared"))?;
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value: command,
                    events,
                });
            }
            update_command_state(&tx, id, CommandState::Dispatched, None, None, now)?;
            events.push(insert_daemon_command_event(
                &tx,
                now,
                "command.dispatched",
                None,
                None,
                id,
                json!({"command_id": id, "verb": "git.fetch"}),
            )?);
            let command = load_command(&tx, id)?
                .ok_or_else(|| Error::new("internal", "dispatched git.fetch disappeared"))?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value: command,
                events,
            });
        }
        let agent_num = command.target.ok_or_else(|| {
            Error::new("bad_state", "L1 dispatch requires a concrete Agent target")
        })?;
        let agent = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "command target Agent was not found"))?;
        projects::validate_project_command_dispatch_tx(&tx, &command)?;
        let command_generation = command.command.generation.unwrap_or(agent.generation);
        if !matches!(command.command.verb.as_str(), "attach" | "detach") {
            observer::require_controlled(&agent)?;
        }
        if matches!(command.command.verb.as_str(), "launch" | "send" | "steer") {
            provider_holds::require_dispatch_tx(&tx, &agent, &command.command)?;
        }
        match rules::gate_rule_action_dispatch_tx(&tx, id, now, &mut events)? {
            rules::RuleActionDispatchGate::Ready => {}
            rules::RuleActionDispatchGate::Held | rules::RuleActionDispatchGate::Cancelled => {
                let command = load_command(&tx, id)?
                    .ok_or_else(|| Error::new("internal", "gated command disappeared"))?;
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value: command,
                    events,
                });
            }
        }

        match command.command.verb.as_str() {
            "attach" | "detach" => observer::dispatch(&tx, &command, &agent, now, &mut events)?,
            "launch" => {
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
                workspace::gate_workspace_start(&tx, &agent, &command.command, 1)?;
                capacity::require_reservation_tx(&tx, agent_num, 1)?;
                let snapshot = native_store
                    .map(|(expected, binding)| {
                        workspace::launch_snapshot_with_native_store(&agent, expected, binding, id)
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
                install_initial_source_plan(&tx, agent_num, 1, &agent.harness, now)?;
                update_command_state(&tx, id, CommandState::Dispatched, None, Some(1), now)?;
                events.push(insert_daemon_event_for_command(
                    &tx,
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
                    &tx,
                    now,
                    "command.dispatched",
                    Some(agent_num),
                    Some(1),
                    id,
                    json!({"command_id": id, "verb": "launch"}),
                )?);
            }
            "resume" => {
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
                if !matches!(agent.harness.as_str(), "codex" | "fake") {
                    return Err(Error::new(
                        "unsupported",
                        "resume is only supported for Codex or Fake Agents",
                    ));
                }
                if has_pending_resume_tx(&tx, agent_num, old_generation, id)? {
                    return Err(Error::new(
                        "bad_state",
                        "another pending resume targets this Agent generation",
                    ));
                }
                if has_in_flight_stop_tx(&tx, agent_num, old_generation)? {
                    return Err(Error::new(
                        "bad_state",
                        "a dispatched or unknown Stop still targets this Agent generation",
                    ));
                }
                let native_scope = agent
                    .native_scope
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        Error::new("unsupported", "resume has no retained native subject")
                    })?;
                let native_ref = agent
                    .native_ref
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        Error::new("unsupported", "resume has no retained native subject")
                    })?;
                let next_generation = agent.generation.checked_add(1).ok_or_else(|| {
                    Error::new("bad_state", "Agent generation cannot be incremented")
                })?;
                workspace::gate_workspace_start(&tx, &agent, &command.command, next_generation)?;
                capacity::require_reservation_tx(&tx, agent_num, next_generation)?;
                for old_id in pending_command_ids(&tx, agent_num, agent.generation)? {
                    if old_id == id {
                        continue;
                    }
                    let outcome = json!({"reason": "generation_resumed"});
                    update_command_state(
                        &tx,
                        &old_id,
                        CommandState::Cancelled,
                        Some(&outcome),
                        None,
                        now,
                    )?;
                    events.push(insert_daemon_command_event(
                        &tx,
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
                install_initial_source_plan(&tx, agent_num, next_generation, &agent.harness, now)?;
                update_command_state(
                    &tx,
                    id,
                    CommandState::Dispatched,
                    None,
                    Some(next_generation),
                    now,
                )?;
                events.push(insert_daemon_event_for_command(
                    &tx,
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
                    &tx,
                    now,
                    "command.dispatched",
                    Some(agent_num),
                    Some(next_generation),
                    id,
                    json!({"command_id": id, "verb": "resume"}),
                )?);
            }
            "worktree.remove" => {
                let mut workspace = workspace::load_workspace(&tx, agent_num)?
                    .ok_or_else(|| Error::new("not_found", "managed worktree was not found"))?;
                workspace::require_workspace_removable(&tx, &agent, &workspace)?;
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
                if repo_git_write_blocked_tx(&tx, id, &workspace.repo_common_dir)? {
                    if command.state == CommandState::Accepted {
                        update_command_state(&tx, id, CommandState::Queued, None, None, now)?;
                        events.push(insert_daemon_command_event(
                            &tx,
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
                    let queued = load_command(&tx, id)?
                        .ok_or_else(|| Error::new("internal", "queued removal disappeared"))?;
                    commit_transaction(tx, &self.write_faulted)?;
                    return Ok(Change {
                        value: queued,
                        events,
                    });
                }
                let old = workspace.clone();
                workspace.phase = WorkspacePhase::Removing;
                workspace.outcome = Some(
                    json!({"stage": "removal", "command_id": id, "previous_phase": old.phase,
                        "native_git_write_dispatched": false}),
                );
                workspace.updated_at = now;
                workspace::persist_workspace(&tx, &old, &workspace)?;
                update_command_state(&tx, id, CommandState::Dispatched, None, None, now)?;
                events.push(workspace::workspace_event(&tx, &workspace, now)?);
                events.push(insert_daemon_command_event(
                    &tx,
                    now,
                    "command.dispatched",
                    Some(agent_num),
                    Some(agent.generation),
                    id,
                    json!({"command_id": id, "verb": "worktree.remove"}),
                )?);
            }
            "send" => {
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
                let ready = send_gate_open(&tx, agent_num, agent.generation)?;
                let earliest = earliest_pending_send(&tx, agent_num, agent.generation)?;
                if !ready || earliest.as_deref().is_some_and(|first| first != id) {
                    if command.state == CommandState::Accepted {
                        update_command_state(&tx, id, CommandState::Queued, None, None, now)?;
                        events.push(insert_daemon_command_event(
                            &tx,
                            now,
                            "command.queued",
                            Some(agent_num),
                            Some(agent.generation),
                            id,
                            json!({"command_id": id, "verb": "send"}),
                        )?);
                    }
                    command = load_command_record(&tx, id)?
                        .ok_or_else(|| Error::new("internal", "queued command disappeared"))?;
                    commit_transaction(tx, &self.write_faulted)?;
                    return Ok(Change {
                        value: command.command,
                        events,
                    });
                }
                check_rpc_frame(
                    "agent.command",
                    &json!({
                        "agent_id": agent.id,
                        "generation": agent.generation,
                        "command_id": id,
                        "verb": "send",
                        "args": command.command.args,
                    }),
                )?;
                update_command_state(&tx, id, CommandState::Dispatched, None, None, now)?;
                events.push(insert_daemon_command_event(
                    &tx,
                    now,
                    "command.dispatched",
                    Some(agent_num),
                    Some(agent.generation),
                    id,
                    json!({"command_id": id, "verb": "send"}),
                )?);
            }
            "stop" => {
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
                update_command_state(&tx, id, CommandState::Dispatched, None, None, now)?;
                events.push(insert_daemon_command_event(
                    &tx,
                    now,
                    "command.dispatched",
                    Some(agent_num),
                    Some(agent.generation),
                    id,
                    json!({"command_id": id, "verb": "stop"}),
                )?);
            }
            "steer" | "interrupt" | "approve" | "deny" | "answer" => {
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
                if !supports_control_verb(&agent.harness, &command.command.verb) {
                    return Err(Error::new(
                        "unsupported",
                        "control command is not supported by this harness",
                    ));
                }
                if !matches!(agent.state, AgentState::Busy | AgentState::Waiting) {
                    return Err(Error::new(
                        "bad_state",
                        "control command requires a busy or waiting Agent",
                    ));
                }
                let turn_id = command.command.turn_id.as_deref().ok_or_else(|| {
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
                        load_pending_request(&tx, agent_num, agent.generation, request_id)?
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
                    if has_open_request_reply(
                        &tx,
                        agent_num,
                        agent.generation,
                        request_id,
                        Some(id),
                    )? {
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
                update_command_state(&tx, id, CommandState::Dispatched, None, None, now)?;
                events.push(insert_daemon_command_event(
                    &tx,
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
            }
            verb => {
                return Err(Error::new(
                    "unsupported",
                    format!("Store does not dispatch the {verb} verb"),
                ));
            }
        }

        command = load_command_record(&tx, id)?
            .ok_or_else(|| Error::new("internal", "dispatched command disappeared"))?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value: command.command,
            events,
        })
    }
}
