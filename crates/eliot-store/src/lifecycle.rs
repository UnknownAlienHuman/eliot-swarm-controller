use super::*;

impl Store {
    pub(super) fn settle_inner(
        &mut self,
        id: &str,
        state: CommandState,
        outcome: Option<Value>,
        now: i64,
    ) -> Result<Change<Command>> {
        validate_settle_outcome(state, &outcome)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let mut record = load_command_record(&tx, id)?
            .ok_or_else(|| Error::new("not_found", format!("command {id} was not found")))?;
        check_settle_verb_guards(&tx, &record, state, &outcome, id)?;
        if let Some(command) = check_settle_idempotent(&record, state, outcome.as_ref())? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value: command,
                events: Vec::new(),
            });
        }
        if !core::command_transition_allowed(record.state, state) {
            return Err(Error::new(
                "bad_state",
                "command state transition is not allowed for ordinary settlement",
            ));
        }
        if state == CommandState::Applied
            && record.command.verb == "send"
            && record.command.turn_id.is_none()
        {
            return Err(Error::new(
                "bad_state",
                "send is applied only after exact turn association is committed",
            ));
        }
        if state == CommandState::Applied && is_turn_control(&record.command.verb) {
            validate_control_applied_outcome(&record.command, id, outcome.as_ref())?;
        }

        let target = record.target;
        let generation = record.command.generation;
        let initial_launch_never_dispatched = if record.command.verb == "launch"
            && generation == Some(0)
            && matches!(record.state, CommandState::Accepted | CommandState::Queued)
            && let Some(agent_num) = target
        {
            load_agent_by_num(&tx, agent_num)?
                .is_some_and(|agent| agent.generation == 0 && agent.state == AgentState::Created)
        } else {
            false
        };
        update_command_state(&tx, id, state, outcome.as_ref(), None, now)?;
        let mut events = Vec::new();
        let state_name = command_state_name(state);
        events.push(insert_daemon_command_event(
            &tx,
            now,
            &format!("command.{state_name}"),
            target,
            generation,
            id,
            json!({"command_id": id, "state": state_name, "outcome": outcome}),
        )?);

        if matches!(record.command.verb.as_str(), "attach" | "detach") {
            observer::settle_failure(&tx, &record.command, now, &mut events)?;
        }

        release_settle_capacity(
            &tx,
            &record,
            state,
            id,
            initial_launch_never_dispatched,
            now,
            &mut events,
        )?;

        apply_settle_agent_transition(
            &tx,
            &record,
            state,
            id,
            initial_launch_never_dispatched,
            now,
            &mut events,
        )?;

        record = load_command_record(&tx, id)?
            .ok_or_else(|| Error::new("internal", "settled command disappeared"))?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value: record.command,
            events,
        })
    }
}

fn validate_settle_outcome(state: CommandState, outcome: &Option<Value>) -> Result<()> {
    if !matches!(
        state,
        CommandState::Applied
            | CommandState::Rejected
            | CommandState::Failed
            | CommandState::Unknown
    ) {
        return Err(Error::new(
            "invalid",
            "settlement state is not a result state",
        ));
    }
    if let Some(outcome) = outcome
        && encode_json(outcome)?.len() > MAX_EVENT_BYTES
    {
        return Err(Error::new(
            "frame_too_large",
            "command outcome exceeds 64 KiB",
        ));
    }
    Ok(())
}

fn check_settle_verb_guards(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    state: CommandState,
    outcome: &Option<Value>,
    id: &str,
) -> Result<()> {
    if record.command.verb == "git.fetch" && state == CommandState::Applied {
        let frozen = projects::decode_frozen_git_fetch(&record.command.args)?;
        let value = outcome.as_ref().ok_or_else(|| {
            Error::new(
                "invalid",
                "applied git.fetch requires verified ref readback",
            )
        })?;
        let outcome = serde_json::from_value::<core::api::GitFetchOutcome>(value.clone())
            .map_err(|_| Error::new("invalid", "applied git.fetch readback is malformed"))?;
        outcome.validate_for(&frozen)?;
    }
    if matches!(record.command.verb.as_str(), "attach" | "detach") && state == CommandState::Applied
    {
        return Err(Error::new(
            "not_allowed",
            "observer commands require exact observer settlement",
        ));
    }
    if record.command.verb == "worktree.remove" {
        return Err(Error::new(
            "bad_state",
            "use typed workspace removal settlement",
        ));
    }
    if let Some(agent_num) = record.target
        && let Some(workspace) = workspace::load_workspace(tx, agent_num)?
        && ((workspace.start_command_id == id
            && record
                .command
                .generation
                .is_some_and(|generation| generation < workspace.target_generation)
            && matches!(
                record.state,
                CommandState::Accepted | CommandState::Queued | CommandState::Unknown
            ))
            || (record.command.verb == "stop"
                && workspace
                    .outcome
                    .as_ref()
                    .and_then(|outcome| outcome.get("stop_command_id"))
                    .and_then(Value::as_str)
                    == Some(id)))
    {
        return Err(Error::new(
            "bad_state",
            "use workspace-bound preparation/cancellation settlement",
        ));
    }
    Ok(())
}

fn check_settle_idempotent(
    record: &CommandRecord,
    state: CommandState,
    outcome: Option<&Value>,
) -> Result<Option<Command>> {
    if matches!(
        record.state,
        CommandState::Applied
            | CommandState::Rejected
            | CommandState::Failed
            | CommandState::Cancelled
    ) {
        if record.state == state {
            if state == CommandState::Applied && record.command.verb == "send" {
                if outcome
                    .and_then(|value| value.get("turn_id"))
                    .and_then(Value::as_str)
                    .is_some_and(|turn_id| record.command.turn_id.as_deref() != Some(turn_id))
                {
                    return Err(Error::new(
                        "source_integrity",
                        "settlement turn id conflicts with the committed send association",
                    ));
                }
                return Ok(Some(record.command.clone()));
            }
            if outcome.is_none() || record.command.outcome.as_ref() == outcome {
                return Ok(Some(record.command.clone()));
            }
        }
        if matches!(
            record.state,
            CommandState::Applied
                | CommandState::Rejected
                | CommandState::Failed
                | CommandState::Cancelled
        ) {
            return Err(Error::new(
                "bad_state",
                format!(
                    "command {} already has terminal state {:?}",
                    record.command.id, record.state
                ),
            ));
        }
    }
    if record.state == CommandState::Unknown
        && state == CommandState::Unknown
        && (outcome.is_none() || record.command.outcome.as_ref() == outcome)
    {
        return Ok(Some(record.command.clone()));
    }
    Ok(None)
}

fn release_settle_capacity(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    state: CommandState,
    id: &str,
    initial_launch_never_dispatched: bool,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let target = record.target;
    let generation = record.command.generation;
    let unstarted_resume_failure = record.command.verb == "resume"
        && matches!(record.state, CommandState::Accepted | CommandState::Queued)
        && matches!(state, CommandState::Rejected | CommandState::Failed);
    if initial_launch_never_dispatched
        && matches!(state, CommandState::Rejected | CommandState::Failed)
    {
        capacity::release_unstarted_tx(
            tx,
            target.ok_or_else(|| Error::new("internal", "launch has no Agent"))?,
            1,
            id,
        )?;
    } else if unstarted_resume_failure {
        cancel_unstarted_resume_input_tx(
            tx,
            target.ok_or_else(|| Error::new("internal", "resume has no Agent"))?,
            id,
            "resume_not_dispatched",
            now,
            events,
            false,
        )?;
    } else if state == CommandState::Rejected
        && matches!(record.command.verb.as_str(), "launch" | "resume")
    {
        let agent = target.ok_or_else(|| Error::new("internal", "rejected start has no Agent"))?;
        let command_generation =
            generation.ok_or_else(|| Error::new("internal", "rejected start has no generation"))?;
        let capacity_generation = if record.command.verb == "launch" {
            1
        } else if matches!(record.state, CommandState::Accepted | CommandState::Queued) {
            command_generation
                .checked_add(1)
                .ok_or_else(|| Error::new("bad_state", "capacity generation overflow"))?
        } else {
            command_generation
        };
        capacity::release_unstarted_tx(tx, agent, capacity_generation, id)?;
    }
    Ok(())
}

fn apply_settle_agent_transition(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    state: CommandState,
    id: &str,
    initial_launch_never_dispatched: bool,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let target = record.target;
    let generation = record.command.generation;
    if initial_launch_never_dispatched {
        let agent_num = target.ok_or_else(|| Error::new("internal", "launch has no Agent"))?;
        fail_unstarted_launch_tx(tx, agent_num, id, now, events)?;
    } else if matches!(record.command.verb.as_str(), "launch" | "resume")
        && matches!(state, CommandState::Rejected | CommandState::Failed)
    {
        if let (Some(agent_num), Some(generation)) = (target, generation) {
            let reason = if record.command.verb == "resume" {
                "resume_failed"
            } else {
                "launch_failed"
            };
            fail_start_tx(tx, agent_num, generation, id, reason, now, events)?;
        }
    } else if record.command.verb == "stop"
        && state == CommandState::Applied
        && let (Some(agent_num), Some(generation)) = (target, generation)
    {
        stop_agent_tx(tx, agent_num, generation, id, now, events)?;
    }
    Ok(())
}

fn restore_resume_workspace_marker_tx(
    tx: &Transaction<'_>,
    current: &WorkspacePreparation,
    command_id: &str,
    agent_num: i64,
    old_generation: i64,
    now: i64,
    allow_workspace_cancel: bool,
) -> Result<WorkspacePreparation> {
    let next_generation = old_generation
        .checked_add(1)
        .ok_or_else(|| Error::new("source_integrity", "resume workspace generation overflow"))?;
    let current_outcome = current.outcome.as_ref().ok_or_else(|| {
        Error::new(
            "source_integrity",
            "resume workspace marker has no prior Ready state",
        )
    })?;
    let current_previous = current_outcome
        .get("resume_previous")
        .ok_or_else(|| Error::new("source_integrity", "resume workspace lost its prior marker"))?;
    let current_reuse = current_outcome.get("reuse").and_then(Value::as_bool) == Some(true);
    if current.start_command_id != command_id
        || current.target_generation != next_generation
        || !matches!(
            current.phase,
            WorkspacePhase::Creating | WorkspacePhase::Ready
        )
        || current.setup_index != current.frozen_setup.len()
        || !current_reuse
        || workspace::workspace_cancel_requested(current) && !allow_workspace_cancel
    {
        return Err(Error::new(
            "source_integrity",
            "resume withdrawal is not bound to its exact retained workspace marker",
        ));
    }
    let previous_start_id = current_previous
        .get("start_command_id")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("source_integrity", "resume prior marker has no start id"))?;
    if current_previous
        .get("target_generation")
        .and_then(Value::as_i64)
        != Some(old_generation)
    {
        return Err(Error::new(
            "source_integrity",
            "resume prior marker belongs to another generation",
        ));
    }
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", "resume workspace Agent was not found"))?;
    if parse_agent_id(&current.agent)? != agent_num
        || agent.generation != old_generation
        || !matches!(
            agent.state,
            AgentState::Stopped | AgentState::Failed | AgentState::Lost
        )
        || agent.active_turn.is_some()
        || agent.workspace.as_deref() != Some(current.worktree_path.as_str())
        || agent
            .native_scope
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
        || agent
            .native_ref
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
    {
        return Err(Error::new(
            "source_integrity",
            "resume withdrawal cannot restore a changed old Agent or workspace",
        ));
    }
    let previous_start = load_command_record(tx, previous_start_id)?
        .ok_or_else(|| Error::new("source_integrity", "resume prior start command is missing"))?;
    if previous_start.target != Some(agent_num)
        || !matches!(previous_start.command.verb.as_str(), "launch" | "resume")
        || previous_start.command.generation != Some(old_generation)
        || !matches!(
            previous_start.state,
            CommandState::Applied | CommandState::Dispatched | CommandState::Unknown
        )
        || previous_start
            .command
            .expected_generation
            .is_some_and(|expected| expected.checked_add(1) != Some(old_generation))
    {
        return Err(Error::new(
            "source_integrity",
            "resume prior start command does not own the retained workspace generation",
        ));
    }
    let previous_outcome = current_previous
        .get("outcome")
        .cloned()
        .filter(|outcome| !outcome.is_null());
    let mut restored = current.clone();
    restored.start_command_id = previous_start_id.to_owned();
    restored.target_generation = old_generation;
    restored.phase = WorkspacePhase::Ready;
    restored.outcome = previous_outcome;
    restored.updated_at = now;
    Ok(restored)
}

pub(super) fn cancel_unstarted_resume_input_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    command_id: &str,
    reason: &str,
    now: i64,
    events: &mut Vec<Event>,
    allow_workspace_cancel: bool,
) -> Result<Option<WorkspacePreparation>> {
    let resume = load_command_record(tx, command_id)?
        .ok_or_else(|| Error::new("source_integrity", "resume command row disappeared"))?;
    let old_generation = resume
        .command
        .expected_generation
        .or(resume.command.generation)
        .ok_or_else(|| Error::new("source_integrity", "resume has no retained old generation"))?;
    if resume.target != Some(agent_num)
        || resume.command.verb != "resume"
        || resume.command.generation != Some(old_generation)
        || resume
            .command
            .expected_generation
            .is_some_and(|expected| expected != old_generation)
        || !matches!(
            resume.state,
            CommandState::Rejected | CommandState::Failed | CommandState::Cancelled
        )
    {
        return Err(Error::new(
            "source_integrity",
            "resume cleanup is not bound to its exact undispatched old generation",
        ));
    }
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", "resume target Agent was not found"))?;
    if agent.generation != old_generation
        || !matches!(
            agent.state,
            AgentState::Stopped | AgentState::Failed | AgentState::Lost
        )
        || agent.active_turn.is_some()
        || agent
            .native_scope
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
        || agent
            .native_ref
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
    {
        return Err(Error::new(
            "source_integrity",
            "resume cleanup cannot alter a changed or nonterminal old Agent",
        ));
    }

    let next_generation = old_generation
        .checked_add(1)
        .ok_or_else(|| Error::new("source_integrity", "resume capacity generation overflow"))?;
    let workspace_restore = match workspace::load_workspace(tx, agent_num)? {
        Some(current) => {
            let restored = restore_resume_workspace_marker_tx(
                tx,
                &current,
                command_id,
                agent_num,
                old_generation,
                now,
                allow_workspace_cancel,
            )?;
            Some((current, restored))
        }
        None => None,
    };
    cancel_resume_linked_input_tx(
        tx,
        resume.command,
        agent_num,
        next_generation,
        reason,
        now,
        events,
    )?;
    capacity::release_unstarted_tx(tx, agent_num, next_generation, command_id).and_then(|()| {
        if let Some((current, restored)) = workspace_restore {
            workspace::persist_workspace(tx, &current, &restored)?;
            events.push(workspace::workspace_event(tx, &restored, now)?);
            Ok(Some(restored))
        } else {
            Ok(None)
        }
    })
}

fn cancel_resume_linked_input_tx(
    tx: &Transaction<'_>,
    command: Command,
    agent_num: i64,
    next_generation: i64,
    reason: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let retained = rules::retained_action_admission(tx, command)?;
    if let Some(input) = retained.linked_command {
        let input_target = input
            .target
            .as_deref()
            .map(parse_agent_id)
            .transpose()?
            .ok_or_else(|| Error::new("source_integrity", "resume input has no Agent target"))?;
        let input_generation = input
            .generation
            .or(input.expected_generation)
            .ok_or_else(|| Error::new("source_integrity", "resume input has no next generation"))?;
        if input_target != agent_num || input_generation != next_generation {
            return Err(Error::new(
                "source_integrity",
                "resume input no longer targets its exact next generation",
            ));
        }
        match input.state {
            CommandState::Accepted | CommandState::Queued => cancel_pending_command_tx(
                tx,
                input_target,
                input_generation,
                &input.id,
                reason,
                now,
                events,
            )?,
            CommandState::Rejected | CommandState::Cancelled => {}
            CommandState::Dispatched
            | CommandState::Unknown
            | CommandState::Applied
            | CommandState::Failed => {
                return Err(Error::new(
                    "bad_state",
                    "resume input has an external outcome; its reservation remains held",
                ));
            }
        }
    }
    Ok(())
}

fn is_turn_control(verb: &str) -> bool {
    matches!(verb, "steer" | "interrupt" | "approve" | "deny" | "answer")
}

fn validate_control_applied_outcome(
    command: &Command,
    command_id: &str,
    outcome: Option<&Value>,
) -> Result<()> {
    let Some(outcome) = outcome else {
        return Err(Error::new(
            "source_integrity",
            "control command cannot be applied without its exact native result",
        ));
    };
    let reported_command = outcome.get("command_id").and_then(Value::as_str);
    let reported_turn = outcome.get("turn_id").and_then(Value::as_str);
    let reported_request = outcome.get("request_id").and_then(Value::as_str);
    if reported_command != Some(command_id)
        || command
            .turn_id
            .as_deref()
            .is_none_or(|turn_id| reported_turn != Some(turn_id))
        || reported_request != command.request_id.as_deref()
    {
        return Err(Error::new(
            "source_integrity",
            "control result does not match its exact command, turn, and request binding",
        ));
    }
    Ok(())
}

pub(super) fn transition_lost_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    now: i64,
    reason: &str,
    events: &mut Vec<Event>,
) -> Result<()> {
    let Some(agent) = load_agent_by_num(tx, agent_num)? else {
        return Err(Error::new("not_found", "Agent was not found"));
    };
    observer::require_controlled(&agent)?;
    if agent.generation != generation {
        return Err(Error::new(
            "stale_generation",
            "loss transition targets a stale generation",
        ));
    }
    if agent.is_terminal() {
        return Ok(());
    }

    let dispatched = command_ids_in_state(tx, agent_num, generation, "dispatched")?;
    for command_id in dispatched {
        let outcome = json!({"reason": reason});
        update_command_state(
            tx,
            &command_id,
            CommandState::Unknown,
            Some(&outcome),
            None,
            now,
        )?;
        events.push(insert_daemon_command_event(
            tx,
            now,
            "command.unknown",
            Some(agent_num),
            Some(generation),
            &command_id,
            json!({"command_id": command_id, "reason": reason}),
        )?);
    }
    for command_id in pending_command_ids(tx, agent_num, generation)? {
        cancel_pending_command_tx(
            tx,
            agent_num,
            generation,
            &command_id,
            "generation_lost",
            now,
            events,
        )?;
    }

    mark_lost_turn_unknown_tx(tx, &agent, agent_num, generation, now, reason, events)?;

    db!(tx.execute(
        "UPDATE agents SET state = 'lost', active_turn = NULL, pending = '[]',
                           token = NULL, updated_at = ?1
         WHERE id = ?2 AND generation = ?3",
        params![now, agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE pending_requests SET state = 'stale'
         WHERE agent = ?1 AND generation = ?2 AND state = 'pending'",
        params![agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE source_state SET state = 'closed', health = 'stale', updated_at = ?1
         WHERE agent = ?2 AND generation = ?3 AND source NOT IN ('os', 'git', 'workspace_process')",
        params![now, agent_num, generation],
    ));
    events.push(insert_daemon_event(
        tx,
        now,
        "agent.lost",
        "agent",
        Some(agent_num),
        Some(generation),
        json!({"agent_id": agent.id, "reason": reason}),
    )?);
    Ok(())
}

fn mark_lost_turn_unknown_tx(
    tx: &Transaction<'_>,
    agent: &Agent,
    agent_num: i64,
    generation: i64,
    now: i64,
    reason: &str,
    events: &mut Vec<Event>,
) -> Result<()> {
    if let Some(turn_id) = agent.active_turn.as_deref() {
        let exists = load_turn(tx, agent_num, generation, turn_id)?.is_some();
        if exists {
            db!(tx.execute(
                "UPDATE turns SET state = 'unknown', completed_at = ?1
                 WHERE agent = ?2 AND generation = ?3 AND turn_id = ?4",
                params![now, agent_num, generation, turn_id],
            ));
        } else {
            insert_turn(
                tx,
                TurnInsert {
                    agent: agent_num,
                    generation,
                    turn_id,
                    state: TurnState::Unknown,
                    status: None,
                    final_text: None,
                    started_at: None,
                    completed_at: Some(now),
                    usage: None,
                },
            )?;
        }
        let cause = command_cause_for_turn_tx(tx, agent_num, generation, turn_id)?;
        let event = DaemonEventInput::new(
            now,
            "turn.unknown",
            "turn",
            Some(agent_num),
            Some(generation),
            json!({"turn_id": turn_id, "reason": reason}),
        );
        let event = match cause {
            Some(cause) => event.with_cause(cause),
            None => event,
        };
        events.push(insert_daemon_event_input(tx, event)?);
    }
    Ok(())
}

fn fail_start_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    command_id: &str,
    reason: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let Some(agent) = load_agent_by_num(tx, agent_num)? else {
        return Err(Error::new("not_found", "launch target Agent was not found"));
    };
    observer::require_controlled(&agent)?;
    if agent.generation != generation || agent.state != AgentState::Starting {
        return Ok(());
    }
    for pending_id in pending_command_ids(tx, agent_num, generation)? {
        cancel_pending_command_tx(tx, agent_num, generation, &pending_id, reason, now, events)?;
    }
    db!(tx.execute(
        "UPDATE agents SET state = 'failed', active_turn = NULL, pending = '[]',
                           token = NULL, updated_at = ?1
         WHERE id = ?2 AND generation = ?3",
        params![now, agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE pending_requests SET state = 'stale'
         WHERE agent = ?1 AND generation = ?2 AND state = 'pending'",
        params![agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE source_state SET state = 'closed', health = 'error', updated_at = ?1
         WHERE agent = ?2 AND generation = ?3 AND source NOT IN ('os', 'git', 'workspace_process')",
        params![now, agent_num, generation],
    ));
    events.push(insert_daemon_event_for_command(
        tx,
        command_id,
        DaemonEventInput::new(
            now,
            "agent.failed",
            "agent",
            Some(agent_num),
            Some(generation),
            json!({"agent_id": agent.id, "command_id": command_id, "reason": reason}),
        ),
    )?);
    Ok(())
}

fn stop_agent_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    command_id: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let Some(agent) = load_agent_by_num(tx, agent_num)? else {
        return Err(Error::new("not_found", "stop target Agent was not found"));
    };
    observer::require_controlled(&agent)?;
    if agent.generation != generation || agent.is_terminal() {
        return Ok(());
    }
    for dispatched_id in command_ids_in_state(tx, agent_num, generation, "dispatched")? {
        if dispatched_id == command_id {
            continue;
        }
        let outcome = json!({"reason": "generation_stopped"});
        update_command_state(
            tx,
            &dispatched_id,
            CommandState::Unknown,
            Some(&outcome),
            None,
            now,
        )?;
        events.push(insert_daemon_command_event(
            tx,
            now,
            "command.unknown",
            Some(agent_num),
            Some(generation),
            &dispatched_id,
            json!({"command_id": dispatched_id, "reason": "generation_stopped"}),
        )?);
    }
    for pending_id in pending_command_ids(tx, agent_num, generation)? {
        if pending_id == command_id {
            continue;
        }
        cancel_pending_command_tx(
            tx,
            agent_num,
            generation,
            &pending_id,
            "generation_stopped",
            now,
            events,
        )?;
    }
    if let Some(turn_id) = agent.active_turn.as_deref() {
        db!(tx.execute(
            "UPDATE turns SET state = 'unknown', completed_at = ?1
             WHERE agent = ?2 AND generation = ?3 AND turn_id = ?4",
            params![now, agent_num, generation, turn_id],
        ));
        events.push(insert_daemon_event_for_command(
            tx,
            command_id,
            DaemonEventInput::new(
                now,
                "turn.unknown",
                "turn",
                Some(agent_num),
                Some(generation),
                json!({"turn_id": turn_id, "reason": "stopped_without_terminal_result"}),
            ),
        )?);
    }
    db!(tx.execute(
        "UPDATE agents SET state = 'stopped', active_turn = NULL, pending = '[]',
                           token = NULL, updated_at = ?1
         WHERE id = ?2 AND generation = ?3",
        params![now, agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE pending_requests SET state = 'stale'
         WHERE agent = ?1 AND generation = ?2 AND state = 'pending'",
        params![agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE source_state SET state = 'closed', updated_at = ?1
         WHERE agent = ?2 AND generation = ?3 AND source NOT IN ('os', 'git', 'workspace_process')",
        params![now, agent_num, generation],
    ));
    events.push(insert_daemon_event_for_command(
        tx,
        command_id,
        DaemonEventInput::new(
            now,
            "agent.stopped",
            "agent",
            Some(agent_num),
            Some(generation),
            json!({"agent_id": agent.id, "command_id": command_id}),
        ),
    )?);
    Ok(())
}
