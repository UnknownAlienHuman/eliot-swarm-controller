use super::*;

/// Gate, dispatch and append `command.dispatched` for a targetless command
/// whose verb-specific validation has already passed, then reload it. `what`
/// names the command in the reload-failure message.
fn dispatch_targetless_tx(
    tx: &Transaction<'_>,
    id: &str,
    command: &CommandRecord,
    now: i64,
    events: &mut Vec<Event>,
    what: &str,
) -> Result<Option<Command>> {
    match rules::gate_rule_action_dispatch_tx(tx, id, now, events)? {
        rules::RuleActionDispatchGate::Ready => {}
        rules::RuleActionDispatchGate::Held | rules::RuleActionDispatchGate::Cancelled => {
            let command = load_command(tx, id)?
                .ok_or_else(|| Error::new("internal", "gated command disappeared"))?;
            return Ok(Some(command));
        }
    }
    update_command_state(tx, id, CommandState::Dispatched, None, None, now)?;
    events.push(insert_daemon_command_event(
        tx,
        now,
        "command.dispatched",
        None,
        None,
        id,
        json!({"command_id": id, "verb": command.command.verb}),
    )?);
    let command = load_command(tx, id)?
        .ok_or_else(|| Error::new("internal", format!("dispatched {what} disappeared")))?;
    Ok(Some(command))
}

// Share dispatch_inner's transaction and accumulator; the caller commits before returning.
pub(super) fn dispatch_tx(
    tx: &Transaction<'_>,
    id: &str,
    command: &CommandRecord,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<Option<Command>> {
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
        projects::validate_project_command_dispatch_tx(tx, command)?;
        scripts::validate_run_dispatch_tx(tx, command)?;
        return dispatch_targetless_tx(tx, id, command, now, events, "run command");
    }
    if matches!(
        command.command.verb.as_str(),
        "notify" | "emit" | "attention.emit" | "mail.send" | "var.set"
    ) {
        if command.target.is_some()
            || command.command.generation.is_some()
            || command.command.expected_generation.is_some()
            || command.command.turn_id.is_some()
            || command.command.request_id.is_some()
        {
            return Err(Error::new(
                "source_integrity",
                "stored targetless action has an Agent binding",
            ));
        }
        projects::validate_project_command_dispatch_tx(tx, command)?;
        return dispatch_targetless_tx(tx, id, command, now, events, "targetless action");
    }
    let verb = command.command.verb.as_str();
    if matches!(verb, "git.fetch" | "git.publish" | "git.integrate") {
        if command.command.selector.is_some() {
            return Err(Error::new(
                "source_integrity",
                "stored Git command has a selector",
            ));
        }
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
                "stored Git command has an Agent binding",
            ));
        }
        let common_dir = match verb {
            "git.fetch" => {
                let frozen = projects::decode_frozen_git_fetch(&command.command.args)?;
                projects::validate_git_fetch_intent_tx(tx, &command.command.principal, &frozen)?;
                frozen.common_dir
            }
            "git.publish" => {
                let frozen = projects::decode_frozen_git_publish(&command.command.args)?;
                projects::validate_git_publish_intent_tx(tx, &command.command.principal, &frozen)?;
                frozen.common_dir
            }
            _ => {
                let frozen = projects::decode_frozen_git_integrate(&command.command.args)?;
                projects::validate_git_integrate_intent_tx(
                    tx,
                    &command.command.principal,
                    &frozen,
                )?;
                frozen.common_dir
            }
        };
        projects::validate_project_command_dispatch_tx(tx, command)?;
        match rules::gate_rule_action_dispatch_tx(tx, id, now, events)? {
            rules::RuleActionDispatchGate::Ready => {}
            rules::RuleActionDispatchGate::Held | rules::RuleActionDispatchGate::Cancelled => {
                let command = load_command(tx, id)?
                    .ok_or_else(|| Error::new("internal", "gated command disappeared"))?;
                return Ok(Some(command));
            }
        }
        if repo_git_write_blocked_tx(tx, id, &common_dir)? {
            if command.state == CommandState::Accepted {
                update_command_state(tx, id, CommandState::Queued, None, None, now)?;
                events.push(insert_daemon_command_event(
                    tx,
                    now,
                    "command.queued",
                    None,
                    None,
                    id,
                    json!({
                        "command_id": id,
                        "verb": verb,
                        "reason": "same_repository_write_pending",
                    }),
                )?);
            }
            let command = load_command(tx, id)?
                .ok_or_else(|| Error::new("internal", "queued Git command disappeared"))?;
            return Ok(Some(command));
        }
        update_command_state(tx, id, CommandState::Dispatched, None, None, now)?;
        events.push(insert_daemon_command_event(
            tx,
            now,
            "command.dispatched",
            None,
            None,
            id,
            json!({"command_id": id, "verb": verb}),
        )?);
        let command = load_command(tx, id)?
            .ok_or_else(|| Error::new("internal", "dispatched Git command disappeared"))?;
        return Ok(Some(command));
    }
    Ok(None)
}
