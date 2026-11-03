use super::*;

pub(super) fn command_semantic_payload(request: &SubmitCommand, args: Value) -> Result<Value> {
    let mut payload = json!({
        "target": request.target,
        "verb": request.verb,
        "args": args,
        "expected_generation": request.expected_generation,
        "expected_turn_id": request.expected_turn_id,
    });
    if request.probe {
        payload["probe"] = Value::Bool(true);
    }
    if let Some(request_id) = request.request_id.as_deref() {
        payload["request_id"] = Value::String(request_id.to_owned());
    }
    if encode_json(&payload)?.len() > MAX_FRAME_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "command semantic request exceeds its bound",
        ));
    }
    Ok(payload)
}

pub(super) fn submit_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: SubmitCommand,
    preparation: Option<WorkspacePreparationInput>,
    now: i64,
    derived_rule: bool,
) -> Result<Admission> {
    require_nonempty(principal, "principal")?;
    if request.verb == "run" && !derived_rule {
        return Err(Error::new(
            "not_allowed",
            "run commands must be admitted by an event-rule firing",
        ));
    }
    if !request.args.is_object() {
        return Err(Error::new("invalid", "command args must be a JSON object"));
    }
    if request.probe && request.verb != "send" {
        return Err(Error::new(
            "bad_state",
            "provider probes are allowed only for SEND commands",
        ));
    }
    // Bound durable admission data here. Host checks the completed start
    // frame, including runtime environment and source state, before writing.
    if request.verb != "attach" && encode_json(&request)?.len() > MAX_FRAME_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "command request exceeds 1 MiB",
        ));
    }
    validate_ulid(&request.id)?;
    let mut semantic_args = if request.verb == "git.fetch" {
        serde_json::from_value::<core::api::FrozenGitFetch>(request.args.clone())
            .map_err(|_| Error::new("invalid", "git.fetch args must be a frozen intent"))?
            .binding_args()?
    } else {
        request.args.clone()
    };
    if request.verb == "attach" {
        semantic_args
            .as_object_mut()
            .ok_or_else(|| Error::new("invalid", "attach args must be an object"))?
            .remove("profile_snapshot");
    }
    let semantic_payload = command_semantic_payload(&request, semantic_args)?;
    let payload_digest = canonical::digest(&semantic_payload)?;
    if let Some((old_principal, old_method, old_digest)) = command_binding(tx, &request.id)? {
        if old_principal != principal
            || old_method != COMMAND_METHOD
            || old_digest != payload_digest
        {
            return Err(Error::new(
                "id_conflict",
                "command id is already bound to another principal, method, or payload",
            ));
        }
        let command = load_command(tx, &request.id)?.ok_or_else(|| {
            Error::new("internal", "command binding exists without its command row")
        })?;
        let linked_command = load_linked_command(tx, &request.id)?;
        return Ok(Admission {
            command,
            linked_command,
            events: Vec::new(),
            is_new: false,
        });
    }
    if write_exists(tx, &request.id)? {
        return Err(Error::new(
            "id_conflict",
            "mutation id is already used by another write",
        ));
    }
    if matches!(request.verb.as_str(), "launch" | "attach")
        && request
            .args
            .get("profile_snapshot")
            .is_some_and(|snapshot| snapshot.get("native_store_binding").is_some())
    {
        return Err(Error::new(
            "invalid",
            "native store binding is derived at launch dispatch",
        ));
    }
    if !derived_rule {
        validate_id_age(&request.id, now)?;
    }
    if !matches!(request.verb.as_str(), "attach" | "detach")
        && let Some(target) = request.target.as_deref()
        && let Some(agent) = load_agent_by_num(tx, parse_agent_id(target)?)?
    {
        observer::require_controlled(&agent)?;
    }
    if let Some(input) = &preparation {
        if !matches!(request.verb.as_str(), "launch" | "resume") {
            return Err(Error::new(
                "invalid",
                "workspace preparation requires launch or resume",
            ));
        }
        workspace::validate_workspace_input(input)?;
    } else if request.verb == "launch"
        && request
            .args
            .get("worktree")
            .is_some_and(|value| !value.is_null())
    {
        return Err(Error::new(
            "invalid",
            "worktree launch requires frozen preparation",
        ));
    }

    let mut events = Vec::new();
    let linked = match request.verb.as_str() {
        "attach" | "detach" => {
            observer::admit(tx, principal, &request, &payload_digest, now, &mut events)?;
            None
        }
        "run" => {
            if request.target.is_some()
                || request.expected_generation.is_some()
                || request.expected_turn_id.is_some()
                || request.request_id.is_some()
                || request.probe
                || preparation.is_some()
            {
                return Err(Error::new(
                    "invalid",
                    "run is a targetless command without Agent bindings",
                ));
            }
            scripts::decode_frozen_run(&request.args)?;
            events.push(admit_command(
                tx,
                &CommandInsert::targetless_admitted(principal, &request, &payload_digest, now),
                json!({"command_id": request.id, "verb": "run"}),
            )?);
            None
        }
        "git.fetch" => {
            if request.target.is_some()
                || request.expected_generation.is_some()
                || request.expected_turn_id.is_some()
                || request.request_id.is_some()
                || request.probe
                || preparation.is_some()
            {
                return Err(Error::new(
                    "invalid",
                    "git.fetch is a targetless command without Agent bindings",
                ));
            }
            let frozen = projects::decode_frozen_git_fetch(&request.args)?;
            projects::validate_git_fetch_intent_tx(tx, principal, &frozen)?;
            events.push(admit_command(
                tx,
                &CommandInsert::targetless_admitted(principal, &request, &payload_digest, now),
                json!({"command_id": request.id, "verb": "git.fetch"}),
            )?);
            None
        }
        "launch" => {
            if request.target.is_some() {
                return Err(Error::new("invalid", "launch does not take a target"));
            }
            if request.request_id.is_some() {
                return Err(Error::new(
                    "invalid",
                    "launch does not take a native request id",
                ));
            }
            if request.expected_turn_id.is_some()
                || request
                    .expected_generation
                    .is_some_and(|expected| expected != 0)
            {
                return Err(Error::new(
                    "stale_generation",
                    "a new launch starts from generation 0",
                ));
            }
            let project = required_string(&request.args, "project")?;
            let harness = if request.args.get("harness").is_none() {
                "fake".to_owned()
            } else {
                required_string(&request.args, "harness")?
            };
            if !matches!(harness.as_str(), "fake" | "codex") {
                return Err(Error::new(
                    "unsupported",
                    format!("unsupported launch harness {harness}"),
                ));
            }
            let profile = if request.args.get("profile").is_none() && harness == "fake" {
                "fake".to_owned()
            } else {
                required_string(&request.args, "profile")?
            };
            let profile_snapshot = match request.args.get("profile_snapshot") {
                Some(Value::Object(_)) => request.args["profile_snapshot"].clone(),
                Some(_) => {
                    return Err(Error::new(
                        "invalid",
                        "args.profile_snapshot must be a JSON object",
                    ));
                }
                None if harness == "fake" => json!({}),
                None => {
                    return Err(Error::new(
                        "invalid",
                        "the codex harness requires a frozen profile_snapshot",
                    ));
                }
            };
            let encoded_profile_snapshot = encode_json(&profile_snapshot)?;
            let workspace = match &preparation {
                Some(input) => {
                    if input.project != project {
                        return Err(Error::new(
                            "invalid",
                            "workspace project differs from launch",
                        ));
                    }
                    input.worktree_path.clone()
                }
                None => required_string(&request.args, "workspace")?,
            };
            if preparation.is_some() && workspace::load_workspace_by_path(tx, &workspace)?.is_some()
            {
                return Err(Error::new(
                    "id_conflict",
                    "worktree path is already managed",
                ));
            }
            workspace::require_managed_path_ready(tx, &workspace)?;
            if request.args.get("prompt").is_some() {
                return Err(Error::new(
                    "invalid",
                    "use args.text for the linked launch input",
                ));
            }
            let text = optional_string(&request.args, "text")?;
            if let Some(text) = &text {
                require_nonempty(text, "args.text")?;
                check_input(text)?;
            }
            db!(tx.execute(
                "INSERT INTO agents(generation, project, harness, profile,
                                        profile_snapshot, workspace, state,
                                        created_at, updated_at)
                     VALUES (0, ?1, ?2, ?3, ?4, ?5, 'created', ?6, ?6)",
                params![
                    project,
                    harness,
                    profile,
                    encoded_profile_snapshot,
                    workspace,
                    now
                ],
            ));
            let agent_num = tx.last_insert_rowid();
            let agent_id = format_agent_id(agent_num);
            let agent = load_agent_by_num(tx, agent_num)?
                .ok_or_else(|| Error::new("internal", "new launch Agent disappeared"))?;
            provider_holds::require_unheld_tx(tx, &agent)?;
            let command =
                CommandInsert::admitted(principal, &request, &payload_digest, (agent_num, 0), now);
            let cause = ordinary_command_cause(command.id);
            insert_command(tx, &command, &cause)?;
            events.push(insert_daemon_event_for_command(
                    tx,
                    command.id,
                    DaemonEventInput::new(
                        now,
                        "agent.created",
                        "agent",
                        Some(agent_num),
                        Some(0),
                        json!({"agent_id": agent_id, "project": project, "harness": harness, "profile": profile}),
                    ),
                )?);
            events.push(insert_daemon_event_input(
                tx,
                DaemonEventInput::new(
                    now,
                    "command.accepted",
                    "command",
                    Some(agent_num),
                    Some(0),
                    json!({"command_id": request.id, "verb": "launch"}),
                )
                .with_cause(cause),
            )?);

            if let Some(text) = text {
                let (linked_id, event) =
                    admit_linked_send(tx, principal, &request.id, agent_num, 1, &text, now)?;
                events.push(event);
                Some(linked_id)
            } else {
                None
            }
        }
        "send" | "stop" => {
            if request.request_id.is_some() {
                return Err(Error::new(
                    "invalid",
                    "send and stop do not take a native request id",
                ));
            }
            let target = request
                .target
                .as_deref()
                .ok_or_else(|| Error::new("invalid", "send and stop require an Agent target"))?;
            let agent_num = parse_agent_id(target)?;
            let agent = load_agent_by_num(tx, agent_num)?
                .ok_or_else(|| Error::new("not_found", format!("Agent {target} was not found")))?;
            if let Some(expected) = request.expected_generation
                && expected != agent.generation
            {
                return Err(Error::new(
                    "stale_generation",
                    format!(
                        "expected generation {expected}, current is {}",
                        agent.generation
                    ),
                ));
            }
            if let Some(expected_turn) = request.expected_turn_id.as_deref()
                && agent.active_turn.as_deref() != Some(expected_turn)
            {
                return Err(Error::new(
                    "stale_generation",
                    "expected turn does not match the current Agent turn",
                ));
            }
            if request.verb == "send" {
                if agent.generation == 0 || agent.state == AgentState::Created {
                    return Err(Error::new(
                        "bad_state",
                        "dispatch launch before submitting Agent input",
                    ));
                }
                if agent.is_terminal() {
                    return Err(Error::new("bad_state", "cannot send to a terminal Agent"));
                }
                let text = required_string(&request.args, "text")?;
                check_input(&text)?;
                check_rpc_frame(
                    "agent.command",
                    &json!({
                        "agent_id": agent.id,
                        "generation": agent.generation,
                        "command_id": request.id,
                        "verb": "send",
                        "args": request.args,
                    }),
                )?;
                let ready = send_gate_open(tx, agent_num, agent.generation)?
                    && earliest_pending_send(tx, agent_num, agent.generation)?.is_none();
                let state = if ready {
                    CommandState::Accepted
                } else {
                    CommandState::Queued
                };
                events.push(admit_command(
                    tx,
                    &CommandInsert {
                        state,
                        ..CommandInsert::admitted(
                            principal,
                            &request,
                            &payload_digest,
                            (agent_num, agent.generation),
                            now,
                        )
                    },
                    json!({"command_id": request.id, "verb": "send"}),
                )?);
                if let Some(event) =
                    provider_holds::admit_send_tx(tx, &agent, &request.id, request.probe, now)?
                {
                    events.push(event);
                }
                None
            } else {
                if request
                    .args
                    .get("force")
                    .is_some_and(|value| !value.is_boolean())
                {
                    return Err(Error::new("invalid", "stop force must be a boolean"));
                }
                check_rpc_frame(
                    "agent.stop",
                    &json!({
                        "agent_id": agent.id,
                        "generation": agent.generation,
                        "command_id": request.id,
                        "force": request.args.get("force").and_then(Value::as_bool).unwrap_or(false),
                    }),
                )?;
                events.push(admit_command(
                    tx,
                    &CommandInsert {
                        turn_id: request.expected_turn_id.as_deref(),
                        ..CommandInsert::admitted(
                            principal,
                            &request,
                            &payload_digest,
                            (agent_num, agent.generation),
                            now,
                        )
                    },
                    json!({"command_id": request.id, "verb": "stop"}),
                )?);
                None
            }
        }
        "worktree.remove" => {
            let target = request
                .target
                .as_deref()
                .ok_or_else(|| Error::new("invalid", "worktree.remove requires an Agent"))?;
            let agent_num = parse_agent_id(target)?;
            let agent = load_agent_by_num(tx, agent_num)?
                .ok_or_else(|| Error::new("not_found", "workspace owner was not found"))?;
            if request.request_id.is_some()
                || request.expected_turn_id.is_some()
                || request
                    .expected_generation
                    .is_some_and(|expected| expected != agent.generation)
                || request.args.as_object().is_none_or(|args| !args.is_empty())
            {
                return Err(Error::new(
                    "invalid",
                    "invalid worktree.remove binding or arguments",
                ));
            }
            let workspace = workspace::load_workspace(tx, agent_num)?
                .ok_or_else(|| Error::new("not_found", "Agent has no managed worktree"))?;
            workspace::require_workspace_removable(tx, &agent, &workspace)?;
            let args = json!({"target_generation": workspace.target_generation});
            events.push(admit_command(
                tx,
                &CommandInsert {
                    args: &args,
                    ..CommandInsert::admitted(
                        principal,
                        &request,
                        &payload_digest,
                        (agent_num, agent.generation),
                        now,
                    )
                },
                json!({"command_id": request.id, "verb": "worktree.remove"}),
            )?);
            None
        }
        "resume" => {
            let target = request
                .target
                .as_deref()
                .ok_or_else(|| Error::new("invalid", "resume requires an Agent target"))?;
            let agent_num = parse_agent_id(target)?;
            let agent = load_agent_by_num(tx, agent_num)?
                .ok_or_else(|| Error::new("not_found", format!("Agent {target} was not found")))?;
            if let Some(path) = &agent.workspace {
                workspace::require_managed_path_ready(tx, path)?;
            }
            if !matches!(agent.harness.as_str(), "codex" | "fake") {
                return Err(Error::new(
                    "unsupported",
                    "resume is only supported for Codex or Fake Agents with a retained native subject",
                ));
            }
            if let Some(expected) = request.expected_generation
                && expected != agent.generation
            {
                return Err(Error::new(
                    "stale_generation",
                    format!(
                        "expected generation {expected}, current is {}",
                        agent.generation
                    ),
                ));
            }
            if request.expected_turn_id.is_some() {
                return Err(Error::new(
                    "stale_generation",
                    "resume cannot target an active turn",
                ));
            }
            if request.request_id.is_some() {
                return Err(Error::new(
                    "invalid",
                    "resume does not take a native request id",
                ));
            }
            if !matches!(
                agent.state,
                AgentState::Stopped | AgentState::Failed | AgentState::Lost
            ) || agent.active_turn.is_some()
            {
                return Err(Error::new(
                    "bad_state",
                    "resume requires a stopped, failed, or lost Agent with no active turn",
                ));
            }
            if has_pending_resume_tx(tx, agent_num, agent.generation, &request.id)? {
                return Err(Error::new(
                    "bad_state",
                    "a pending resume already targets this Agent generation",
                ));
            }
            if has_in_flight_stop_tx(tx, agent_num, agent.generation)? {
                return Err(Error::new(
                    "bad_state",
                    "a dispatched or unknown Stop still targets this Agent generation",
                ));
            }
            if [&agent.native_scope, &agent.native_ref]
                .iter()
                .any(|value| value.as_deref().is_none_or(|value| value.trim().is_empty()))
            {
                return Err(Error::new(
                    "unsupported",
                    "resume requires a retained native_scope and native_ref",
                ));
            }
            if request
                .args
                .as_object()
                .is_some_and(|args| args.keys().any(|key| key != "text"))
            {
                return Err(Error::new(
                    "unsupported",
                    "resume accepts only new linked input text; profile and subject are retained",
                ));
            }
            let text = optional_string(&request.args, "text")?;
            if let Some(text) = &text {
                require_nonempty(text, "args.text")?;
                check_input(text)?;
                provider_holds::require_unheld_tx(tx, &agent)?;
            }
            projects::validate_project_resume_action_tx(
                tx,
                principal,
                &agent.id,
                agent.generation,
                request.expected_generation,
                text.is_some(),
            )?;
            let next_generation = agent
                .generation
                .checked_add(1)
                .ok_or_else(|| Error::new("bad_state", "Agent generation cannot be incremented"))?;
            let resume_args = json!({});
            events.push(admit_command(
                tx,
                &CommandInsert {
                    args: &resume_args,
                    ..CommandInsert::admitted(
                        principal,
                        &request,
                        &payload_digest,
                        (agent_num, agent.generation),
                        now,
                    )
                },
                json!({"command_id": request.id, "verb": "resume"}),
            )?);

            if let Some(text) = text {
                let (linked_id, event) = admit_linked_send(
                    tx,
                    principal,
                    &request.id,
                    agent_num,
                    next_generation,
                    &text,
                    now,
                )?;
                events.push(event);
                Some(linked_id)
            } else {
                None
            }
        }
        "steer" | "interrupt" | "approve" | "deny" | "answer" => {
            let target = request
                .target
                .as_deref()
                .ok_or_else(|| Error::new("invalid", "control command requires an Agent target"))?;
            let agent_num = parse_agent_id(target)?;
            let agent = load_agent_by_num(tx, agent_num)?
                .ok_or_else(|| Error::new("not_found", format!("Agent {target} was not found")))?;
            if !supports_control_verb(&agent.harness, &request.verb) {
                return Err(Error::new(
                    "unsupported",
                    format!(
                        "{} is not supported by the {} harness",
                        request.verb, agent.harness
                    ),
                ));
            }
            if let Some(expected) = request.expected_generation
                && expected != agent.generation
            {
                return Err(Error::new(
                    "stale_generation",
                    format!(
                        "expected generation {expected}, current is {}",
                        agent.generation
                    ),
                ));
            }
            if !matches!(agent.state, AgentState::Busy | AgentState::Waiting) {
                return Err(Error::new(
                    "bad_state",
                    "turn controls require a busy or waiting Agent",
                ));
            }
            let turn_id = agent.active_turn.clone().ok_or_else(|| {
                Error::new("bad_state", "turn control requires an exact active turn")
            })?;
            if request
                .expected_turn_id
                .as_deref()
                .is_some_and(|expected| expected != turn_id.as_str())
            {
                return Err(Error::new(
                    "stale_generation",
                    "expected turn does not match the current Agent turn",
                ));
            }

            let request_id = match request.verb.as_str() {
                "steer" | "interrupt" => {
                    if request.request_id.is_some() {
                        return Err(Error::new(
                            "invalid",
                            "steer and interrupt do not take a native request id",
                        ));
                    }
                    None
                }
                _ => {
                    let pending = if let Some(request_id) = request.request_id.as_deref() {
                        require_nonempty(request_id, "request_id")?;
                        let (pending, state) =
                            load_pending_request(tx, agent_num, agent.generation, request_id)?
                                .ok_or_else(|| {
                                    Error::new(
                                        "not_found",
                                        "pending request was not found in this Agent generation",
                                    )
                                })?;
                        if state != "pending" {
                            return Err(Error::new(
                                "source_integrity",
                                "native request is no longer pending",
                            ));
                        }
                        pending
                    } else {
                        capture_single_pending_request(tx, agent_num, agent.generation)?
                    };
                    if pending.turn_id.as_str() != turn_id.as_str()
                        || request
                            .expected_turn_id
                            .as_deref()
                            .is_some_and(|expected| expected != pending.turn_id.as_str())
                    {
                        return Err(Error::new(
                            "stale_generation",
                            "pending request does not belong to the captured active turn",
                        ));
                    }
                    if !agent.pending.contains(&pending) {
                        return Err(Error::new(
                            "source_integrity",
                            "pending request row does not match the Agent projection",
                        ));
                    }
                    if request.verb == "answer" && contains_secret_marker(&pending.details) {
                        return Err(Error::new(
                            "secret_in_payload",
                            "answer to a secret question is not permitted",
                        ));
                    }
                    if has_open_request_reply(tx, agent_num, agent.generation, &pending.id, None)? {
                        return Err(Error::new(
                            "bad_state",
                            "a reply command is already unresolved for this native request",
                        ));
                    }
                    Some(pending.id)
                }
            };
            if request.verb == "steer" {
                let text = required_string(&request.args, "text")?;
                check_input(&text)?;
                provider_holds::require_unheld_tx(tx, &agent)?;
            }
            let mut params = json!({
                "agent_id": agent.id,
                "generation": agent.generation,
                "command_id": request.id,
                "verb": request.verb,
                "turn_id": turn_id,
                "args": request.args,
            });
            if let Some(request_id) = request_id.as_deref() {
                params["request_id"] = Value::String(request_id.to_owned());
            }
            check_rpc_frame("agent.command", &params)?;
            events.push(admit_command(
                tx,
                &CommandInsert {
                    turn_id: Some(&turn_id),
                    request_id: request_id.as_deref(),
                    ..CommandInsert::admitted(
                        principal,
                        &request,
                        &payload_digest,
                        (agent_num, agent.generation),
                        now,
                    )
                },
                json!({
                    "command_id": request.id,
                    "verb": request.verb,
                    "turn_id": turn_id,
                    "request_id": request_id
                }),
            )?);
            None
        }
        verb => {
            return Err(Error::new(
                "unsupported",
                format!("Store does not admit the {verb} verb"),
            ));
        }
    };

    if matches!(request.verb.as_str(), "launch" | "resume") {
        workspace::admit_workspace_preparation(
            tx,
            &request,
            preparation.as_ref(),
            now,
            &mut events,
        )?;
    }
    let command = load_command(tx, &request.id)?
        .ok_or_else(|| Error::new("internal", "new command was not visible in its transaction"))?;
    let linked_command = match linked.as_deref() {
        Some(linked_id) => load_command(tx, linked_id)?,
        None => None,
    };
    if matches!(request.verb.as_str(), "launch" | "resume") {
        let agent_num = command
            .target
            .as_deref()
            .map(parse_agent_id)
            .transpose()?
            .ok_or_else(|| Error::new("internal", "start admission has no Agent"))?;
        let agent = load_agent_by_num(tx, agent_num)?
            .ok_or_else(|| Error::new("internal", "start admission Agent disappeared"))?;
        let generation = if request.verb == "launch" {
            1
        } else {
            agent
                .generation
                .checked_add(1)
                .ok_or_else(|| Error::new("bad_state", "generation overflow"))?
        };
        capacity::reserve_tx(tx, &agent, generation)?;
    }
    Ok(Admission {
        command,
        linked_command,
        events,
        is_new: true,
    })
}
