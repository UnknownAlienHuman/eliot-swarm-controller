use super::*;

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
            json!({"command_id": id, "verb": "run"}),
        )?);
        let command = load_command(tx, id)?
            .ok_or_else(|| Error::new("internal", "dispatched run command disappeared"))?;
        return Ok(Some(command));
    }
    if matches!(command.command.verb.as_str(), "notify" | "emit") {
        if command.target.is_some()
            || command.command.generation.is_some()
            || command.command.expected_generation.is_some()
            || command.command.turn_id.is_some()
            || command.command.request_id.is_some()
        {
            return Err(Error::new(
                "source_integrity",
                "stored notify/emit command has an Agent binding",
            ));
        }
        projects::validate_project_command_dispatch_tx(tx, command)?;
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
            .ok_or_else(|| Error::new("internal", "dispatched notify/emit disappeared"))?;
        return Ok(Some(command));
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
        projects::validate_git_fetch_intent_tx(tx, &command.command.principal, &frozen)?;
        projects::validate_project_command_dispatch_tx(tx, command)?;
        match rules::gate_rule_action_dispatch_tx(tx, id, now, events)? {
            rules::RuleActionDispatchGate::Ready => {}
            rules::RuleActionDispatchGate::Held | rules::RuleActionDispatchGate::Cancelled => {
                let command = load_command(tx, id)?
                    .ok_or_else(|| Error::new("internal", "gated command disappeared"))?;
                return Ok(Some(command));
            }
        }
        if repo_git_write_blocked_tx(tx, id, &frozen.common_dir)? {
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
                        "verb": "git.fetch",
                        "reason": "same_repository_write_pending",
                    }),
                )?);
            }
            let command = load_command(tx, id)?
                .ok_or_else(|| Error::new("internal", "queued git.fetch disappeared"))?;
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
            json!({"command_id": id, "verb": "git.fetch"}),
        )?);
        let command = load_command(tx, id)?
            .ok_or_else(|| Error::new("internal", "dispatched git.fetch disappeared"))?;
        return Ok(Some(command));
    }
    Ok(None)
}
