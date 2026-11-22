//! Task transfer is one Store transaction and ordinary generation-bound SENDs.
use super::*;
use eliot_core::api::{
    CommandCancelRequest, CommandCancelResponse, HandoffRequest, HandoffResponse, ReparentRequest,
    SendTransfer,
};

mod brief;
const METHOD: &str = "swarm.handoff";

#[derive(Debug)]
pub struct HandoffAdmission {
    pub value: HandoffResponse,
    pub events: Vec<Event>,
    pub is_new: bool,
}

impl Store {
    pub fn is_handoff_brief(&self, command_id: &str) -> Result<bool> {
        is_brief_tx(&self.connection, command_id)
    }

    /// Router preflight permits retained retries during drain, before new admission.
    pub fn replay_handoff(
        &self,
        principal: &str,
        request: &HandoffRequest,
    ) -> Result<Option<HandoffResponse>> {
        self.ensure_committed_reads()?;
        request.validate()?;
        let digest = canonical::digest(
            &serde_json::to_value(request)
                .map_err(|_| Error::new("internal", "cannot encode handoff"))?,
        )?;
        let Some((p, method, binding)) = command_binding(&self.connection, &request.id)? else {
            if write_exists(&self.connection, &request.id)?
                || digest_id_bound(&self.connection, &request.id)?
            {
                return Err(Error::new(
                    "id_conflict",
                    "handoff id used by another write",
                ));
            }
            if principal == "owner" || principal.starts_with("agent:") {
                return Ok(None);
            }
            return Err(Error::new("not_allowed", "only Owner may hand off tasks"));
        };
        if p != principal || method != METHOD || binding != digest {
            return Err(Error::new("id_conflict", "handoff id has another binding"));
        }
        let parent = load_command(&self.connection, &request.id)?
            .ok_or_else(|| Error::new("source_integrity", "handoff parent missing"))?;
        let value = serde_json::from_value(
            parent
                .outcome
                .ok_or_else(|| Error::new("source_integrity", "handoff result missing"))?,
        )
        .map_err(|_| Error::new("source_integrity", "handoff result malformed"))?;
        request.validate_response(&value)?;
        Ok(Some(value))
    }

    pub fn handoff(
        &mut self,
        principal: &str,
        request: &HandoffRequest,
        policy: &core::SecretPolicy,
        owner_process: Option<&OwnerProcess>,
        now: i64,
    ) -> Result<HandoffAdmission> {
        self.ensure_writer_healthy()?;
        let result = self.handoff_inner(principal, request, policy, owner_process, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn handoff_inner(
        &mut self,
        principal: &str,
        request: &HandoffRequest,
        policy: &core::SecretPolicy,
        owner_process: Option<&OwnerProcess>,
        now: i64,
    ) -> Result<HandoffAdmission> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let mut change = handoff_tx(
            &tx,
            principal,
            request,
            policy,
            now,
            self.hierarchy_limits,
            HandoffInput::Brief,
        )?;
        annotate_owner_events(&tx, &mut change.events, owner_process)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(change)
    }

    /// Owner withdrawal of one undispatched handoff input, never a native cancel.
    pub fn cancel_handoff_input(
        &mut self,
        principal: &str,
        request: &CommandCancelRequest,
        now: i64,
    ) -> Result<Change<CommandCancelResponse>> {
        self.ensure_writer_healthy()?;
        let result = self.cancel_handoff_input_inner(principal, request, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn cancel_handoff_input_inner(
        &mut self,
        principal: &str,
        request: &CommandCancelRequest,
        now: i64,
    ) -> Result<Change<CommandCancelResponse>> {
        request.validate()?;
        let digest = canonical::digest(&json!({"command_id":request.command_id}))?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if command_binding(&tx, &request.id)?.is_some() || digest_id_bound(&tx, &request.id)? {
            return Err(Error::new(
                "id_conflict",
                "cancel id used by another mutation",
            ));
        }
        if let Some((p, m, d, result)) = write_binding(&tx, &request.id)? {
            if p != principal || m != "command.cancel" || d != digest {
                return Err(Error::new("id_conflict", "cancel id has another binding"));
            }
            let value = serde_json::from_value(decode_json(&result)?)
                .map_err(|_| Error::new("source_integrity", "invalid cancel result"))?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        validate_id_age(&request.id, now)?;
        let record = load_command_record(&tx, &request.command_id)?
            .ok_or_else(|| Error::new("not_found", "input missing"))?;
        let eligible:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM commands c JOIN commands p ON p.id=c.parent_command WHERE c.id=?1 AND c.link_kind='handoff' AND p.method='swarm.handoff')",[&request.command_id],|row|row.get(0)).map_err(sql_error)?;
        let selector_eligible: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM commands c JOIN commands p ON p.id=c.parent_command WHERE c.id=?1 AND c.link_kind='selector' AND p.selector IS NOT NULL)",[&request.command_id],|row|row.get(0)).map_err(sql_error)?;
        let reason = if principal == "owner" {
            if !eligible {
                return Err(Error::new(
                    "bad_state",
                    "command.cancel supports only handoff SEND children; launch/resume inputs and ordinary commands cannot be withdrawn",
                ));
            }
            if record.command.verb != "send"
                || !matches!(record.state, CommandState::Accepted | CommandState::Queued)
            {
                return Err(Error::new(
                    "bad_state",
                    "cancel requires an undispatched handoff SEND",
                ));
            }
            "owner_handoff_no_effect"
        } else {
            // A manager may cancel a send it issued itself while still
            // accepted/queued, before dispatch. The target must be in the
            // caller's controlled scope; dispatched work is never withdrawn.
            let caller = hierarchy::parse_caller(&tx, principal)?;
            let caller_num = match &caller {
                hierarchy::Caller::Agent { num, .. } => *num,
                _ => {
                    return Err(Error::new(
                        "not_allowed",
                        "only Owner may withdraw a handoff input",
                    ));
                }
            };
            let caller_agent = load_agent_by_num(&tx, caller_num)?.ok_or_else(|| {
                Error::new("stale_generation", "caller generation is no longer current")
            })?;
            observer::require_controlled(&caller_agent)?;
            if record.command.principal != principal {
                return Err(Error::new(
                    "not_allowed",
                    "only the send issuer may cancel its queued input",
                ));
            }
            let target_num = record
                .target
                .ok_or_else(|| Error::new("source_integrity", "input target missing"))?;
            let target_agent = load_agent_by_num(&tx, target_num)?
                .ok_or_else(|| Error::new("not_found", "cancel target is no longer retained"))?;
            observer::require_controlled(&target_agent)?;
            hierarchy::caller_may_command(&tx, &caller, &target_agent)?;
            let ordinary: bool = tx
                .query_row(
                    "SELECT parent_command IS NULL AND link_kind IS NULL FROM commands WHERE id=?1",
                    [&request.command_id],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if selector_eligible {
                let parent_principal: String = tx
                    .query_row(
                        "SELECT p.principal FROM commands c JOIN commands p ON p.id=c.parent_command WHERE c.id=?1",
                        [&request.command_id],
                        |row| row.get(0),
                    )
                    .map_err(sql_error)?;
                if parent_principal != principal {
                    return Err(Error::new(
                        "not_allowed",
                        "only the send issuer may cancel its queued input",
                    ));
                }
            }
            if !eligible && !ordinary && !selector_eligible {
                return Err(Error::new(
                    "bad_state",
                    "command.cancel supports only own handoff SEND children, own ordinary SENDs and own selector SEND children; launch/resume inputs cannot be withdrawn",
                ));
            }
            if record.command.verb != "send"
                || !matches!(record.state, CommandState::Accepted | CommandState::Queued)
                || record.command.turn_id.is_some()
            {
                return Err(Error::new(
                    "bad_state",
                    "cancel requires an undispatched SEND",
                ));
            }
            "manager_cancel_no_effect"
        };
        let mut events = Vec::new();
        cancel_pending_command_tx(
            &tx,
            record
                .target
                .ok_or_else(|| Error::new("source_integrity", "input target missing"))?,
            record
                .command
                .generation
                .ok_or_else(|| Error::new("source_integrity", "input generation missing"))?,
            &request.command_id,
            reason,
            now,
            &mut events,
        )?;
        let value = CommandCancelResponse {
            command_id: request.command_id.clone(),
            state: CommandState::Cancelled,
        };
        coordination::retain_write_tx(
            &tx,
            &request.id,
            principal,
            "command.cancel",
            &digest,
            &serde_json::to_value(&value)
                .map_err(|_| Error::new("internal", "cannot encode cancel"))?,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change { value, events })
    }
}

/// Validate the retained source work before allocation and again in the transfer.
pub(super) fn source_inputs(
    connection: &Connection,
    from: &Agent,
    task_command: &str,
    commands: &[String],
    principal: Option<&str>,
    policy: &core::SecretPolicy,
) -> Result<(Command, Vec<Command>)> {
    let task = source_task(connection, from, task_command, principal)?;
    let originals = source_sends(connection, from, commands, principal, policy, false)?;
    Ok((task, originals))
}

fn source_task(
    connection: &Connection,
    from: &Agent,
    task_command: &str,
    principal: Option<&str>,
) -> Result<Command> {
    let task = load_command(connection, task_command)?.ok_or_else(|| {
        Error::new(
            "not_found",
            format!("original task SEND {task_command} is not retained"),
        )
    })?;
    if task.verb != "send"
        || task.target.as_deref() != Some(&from.id)
        || task.generation != Some(from.generation)
    {
        return Err(Error::new(
            "invalid",
            format!(
                "original task {task_command} must be a SEND on {} generation {}",
                from.id, from.generation
            ),
        ));
    }
    if principal.is_some_and(|principal| task.principal != principal) {
        return Err(Error::new(
            "not_allowed",
            "handoff transfers only SENDs issued by the caller",
        ));
    }
    Ok(task)
}

fn source_sends(
    connection: &Connection,
    from: &Agent,
    commands: &[String],
    principal: Option<&str>,
    policy: &core::SecretPolicy,
    project_transfer: bool,
) -> Result<Vec<Command>> {
    let mut originals = Vec::new();
    for id in commands {
        let record = load_command_record(connection, id)?.ok_or_else(|| {
            Error::new("not_found", format!("selected SEND {id} is not retained"))
        })?;
        let c = &record.command;
        let stopped_before_dispatch = project_transfer
            && c.state == CommandState::Cancelled
            && c.outcome.as_ref().and_then(|data| data["reason"].as_str())
                == Some("generation_stopped");
        if c.verb != "send"
            || c.target.as_deref() != Some(&from.id)
            || c.generation != Some(from.generation)
            || !(matches!(c.state, CommandState::Accepted | CommandState::Queued)
                || stopped_before_dispatch)
            || c.turn_id.is_some()
            || record.expected_turn_id.is_some()
            || c.request_id.is_some()
            || c.probe
            || c.selector.is_some()
            || c.args
                .as_object()
                .is_none_or(|args| args.len() != 1 || !args.contains_key("text"))
        {
            return Err(Error::new(
                "bad_state",
                format!(
                    "selected command {id} is not an undispatched plain SEND on {} generation {}",
                    from.id, from.generation
                ),
            ));
        }
        if principal.is_some_and(|principal| c.principal != principal) {
            return Err(Error::new(
                "not_allowed",
                "handoff transfers only SENDs issued by the caller",
            ));
        }
        if policy.contains_value(&c.args) {
            return Err(Error::new(
                "secret_in_payload",
                "successor input contains a known secret",
            ));
        }
        originals.push(record.command);
    }
    Ok(originals)
}

#[derive(Clone, Copy)]
pub(super) enum HandoffInput<'a> {
    Brief,
    /// The switch has already proven an exact provider refusal before work.
    ProviderRetry,
    /// The project transfer has proved source custody and performed the team edits.
    ProjectTransfer {
        saved_context: Option<&'a str>,
    },
}

/// Shared transfer transaction. Caller commits, or rolls back the entire transfer.
pub(super) fn handoff_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &HandoffRequest,
    policy: &core::SecretPolicy,
    now: i64,
    limits: core::api::HierarchyLimits,
    input: HandoffInput<'_>,
) -> Result<HandoffAdmission> {
    let project_transfer = matches!(input, HandoffInput::ProjectTransfer { .. });
    if !project_transfer || !request.task_command.is_empty() {
        request.validate()?;
    }
    // Owner hands off anywhere with claims, Goal and children. A manager may
    // hand off only between two workers in its own controlled scope, never
    // itself, transferring only task inputs it issued itself. Agent handoffs
    // never move children, claims or Goals; moving children stays Owner-only.
    let manager_caller = if principal == "owner" {
        None
    } else {
        let caller = hierarchy::parse_caller(tx, principal)?;
        match &caller {
            hierarchy::Caller::Owner => None,
            hierarchy::Caller::Agent { num, .. } => {
                let caller_agent = load_agent_by_num(tx, *num)?.ok_or_else(|| {
                    Error::new("stale_generation", "caller generation is no longer current")
                })?;
                observer::require_controlled(&caller_agent)?;
                Some(caller)
            }
        }
    };
    let mut payload = serde_json::to_value(request)
        .map_err(|_| Error::new("internal", "cannot encode handoff"))?;
    if project_transfer
        && request.task_command.is_empty()
        && let Some(args) = payload.as_object_mut()
    {
        args.remove("task_command");
    }
    let digest = canonical::digest(&payload)?;
    // Resolve the immutable operation binding before all mutable Agent/history gates.
    if let Some((old_principal, method, old_digest)) = command_binding(tx, &request.id)? {
        if old_principal != principal || method != METHOD || old_digest != digest {
            return Err(Error::new("id_conflict", "handoff id has another binding"));
        }
        let parent = load_command(tx, &request.id)?
            .ok_or_else(|| Error::new("source_integrity", "handoff parent missing"))?;
        let value: HandoffResponse = serde_json::from_value(
            parent
                .outcome
                .ok_or_else(|| Error::new("source_integrity", "handoff result missing"))?,
        )
        .map_err(|_| Error::new("source_integrity", "handoff result malformed"))?;
        request.validate_response(&value)?;
        return Ok(HandoffAdmission {
            value,
            events: Vec::new(),
            is_new: false,
        });
    }
    if write_exists(tx, &request.id)? || digest_id_bound(tx, &request.id)? {
        return Err(Error::new(
            "id_conflict",
            "handoff id is used by another write",
        ));
    }
    validate_id_age(&request.id, now)?;
    if request.commands.len() as i64 > limits.max_selector_targets {
        return Err(Error::new(
            "fanout_limit",
            "handoff selection exceeds configured bound",
        ));
    }
    let from = load_agent_by_num(tx, parse_agent_id(&request.from)?)?
        .ok_or_else(|| Error::new("not_found", "source Agent missing"))?;
    let to = load_agent_by_num(tx, parse_agent_id(&request.to)?)?
        .ok_or_else(|| Error::new("not_found", "destination Agent missing"))?;
    if from.generation != request.from_generation || to.generation != request.to_generation {
        return Err(Error::new("stale_generation", "handoff generation moved"));
    }
    if from.project_scope() != to.project_scope() {
        return Err(Error::new(
            "not_allowed",
            "handoff must stay in one project",
        ));
    }
    if let Some(hierarchy::Caller::Agent { id: caller_id, .. }) = &manager_caller
        && (request.from == *caller_id || request.to == *caller_id)
    {
        return Err(Error::new(
            "not_allowed",
            "handoff endpoints must be workers, not the caller",
        ));
    }
    if let Some(caller) = &manager_caller {
        hierarchy::caller_may_command(tx, caller, &from)?;
        hierarchy::caller_may_command(tx, caller, &to)?;
    }
    project_transfer::require_no_intent(tx, &from.project)?;
    project_transfer::require_no_intent(tx, &to.project)?;
    if !project_transfer {
        quiescent(tx, &from, false)?;
    }
    quiescent(tx, &to, true)?;
    let input_principal = manager_caller.as_ref().map(|_| principal);
    let task = if project_transfer && request.task_command.is_empty() {
        None
    } else {
        Some(source_task(
            tx,
            &from,
            &request.task_command,
            input_principal,
        )?)
    };
    let originals = source_sends(
        tx,
        &from,
        &request.commands,
        input_principal,
        policy,
        project_transfer,
    )?;
    let required_task = || {
        task.as_ref()
            .ok_or_else(|| Error::new("source_integrity", "handoff task is missing"))
    };
    let (brief_text, brief_truncated) = match input {
        HandoffInput::Brief => brief::build(tx, request, Some(required_task()?), policy, None)?,
        HandoffInput::ProjectTransfer { saved_context } => {
            brief::build(tx, request, task.as_ref(), policy, saved_context)?
        }
        HandoffInput::ProviderRetry => {
            let task = required_task()?;
            if policy.contains_value(&task.args) {
                return Err(Error::new(
                    "secret_in_payload",
                    "retry input contains a known secret",
                ));
            }
            let text = task
                .args
                .get("text")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::new("source_integrity", "retry task text missing"))?;
            (text.to_owned(), false)
        }
    };
    let mut events = Vec::new();
    // Agent authority never widens: an Agent handoff transfers only task
    // inputs the caller issued itself. Claims, Goals and children stay put;
    // moving children stays Owner-only.
    let (claims, claim_conflicts, conflicts_truncated, goal_revision, reparented) =
        if project_transfer || manager_caller.is_some() {
            (Vec::new(), Vec::new(), false, None, Vec::new())
        } else {
            let (claims, claim_conflicts, conflicts_truncated) =
                attribution::transfer_claims_tx(tx, &from, &to, policy, now, &mut events)?;
            let goal_revision = goals::transfer_goal_tx(tx, &from, &to, policy, now, &mut events)?;
            let moved = reparent::reparent_tx(
                tx,
                &ReparentRequest {
                    id: request.id.clone(),
                    agent: None,
                    children_of: Some(from.id.clone()),
                    expected_generation: from.generation,
                    parent: to.id.clone(),
                    parent_generation: to.generation,
                },
                limits,
                now,
                true,
            )?;
            events.extend(moved.events);
            (
                claims,
                claim_conflicts,
                conflicts_truncated,
                goal_revision,
                moved.value.moved,
            )
        };
    let brief_command = Ulid::new().to_string();
    let transfers: Vec<SendTransfer> = originals
        .iter()
        .map(|c| SendTransfer {
            original: c.id.clone(),
            successor: Ulid::new().to_string(),
        })
        .collect();
    let children = std::iter::once(brief_command.clone())
        .chain(transfers.iter().map(|t| t.successor.clone()))
        .collect::<Vec<_>>();
    let value = HandoffResponse {
        id: request.id.clone(),
        from: from.id.clone(),
        from_generation: from.generation,
        to: to.id.clone(),
        to_generation: to.generation,
        brief_command: brief_command.clone(),
        children: children.clone(),
        transfers: transfers.clone(),
        claims,
        claim_conflicts,
        conflicts_truncated,
        goal_revision,
        reparented,
        brief_truncated,
        brief_state: CommandState::Accepted,
        successor_gate: "queued_until_brief_applied".into(),
    };
    let outcome = serde_json::to_value(&value)
        .map_err(|_| Error::new("internal", "cannot encode handoff result"))?;
    if encode_json(&outcome)?.len() > MAX_EVENT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "handoff result exceeds retained command budget",
        ));
    }
    let mut parent = SubmitCommand::new(request.id.clone(), "handoff");
    parent.args = payload;
    events.push(admit_command(
        tx,
        &CommandInsert {
            method: METHOD,
            state: CommandState::Applied,
            outcome: Some(&outcome),
            ..CommandInsert::targetless_admitted(principal, &parent, &digest, now)
        },
        json!({"command_id":request.id,"verb":"handoff"}),
    )?);
    for (original, transfer) in originals.iter().zip(&transfers) {
        if original.state != CommandState::Cancelled {
            cancel_pending_command_tx(
                tx,
                parse_agent_id(&from.id)?,
                from.generation,
                &original.id,
                "handoff_no_effect",
                now,
                &mut events,
            )?;
        }
        let mut outcome = json!({"reason":"handoff_no_effect","no_effect":true,"handoff":request.id,"successor":transfer.successor});
        if original.state == CommandState::Cancelled {
            outcome["previous_reason"] = json!("generation_stopped");
        }
        tx.execute(
            "UPDATE commands SET outcome=?1 WHERE id=?2",
            params![encode_json(&outcome)?, original.id],
        )
        .map_err(sql_error)?;
    }
    for (ordinal, id) in children.iter().enumerate() {
        let mut child = SubmitCommand::new(id.clone(), "send");
        child.target = Some(to.id.clone());
        child.expected_generation = Some(to.generation);
        let (child_principal, args) = if ordinal == 0 {
            (principal, json!({"text":brief_text}))
        } else {
            let original = &originals[ordinal - 1];
            (original.principal.as_str(), original.args.clone())
        };
        child.args = args;
        let child_digest = admission::command_payload_digest(&child)?;
        admission::admit_send_member(
            tx,
            child_principal,
            &child,
            &child_digest,
            &to,
            now,
            &mut events,
        )?;
        tx.execute(
            "UPDATE commands SET parent_command=?1,link_kind='handoff',ordinal=?2 WHERE id=?3",
            params![request.id, ordinal as i64, id],
        )
        .map_err(sql_error)?;
    }
    super::result_page::record_handoff_origin_tx(tx, parse_agent_id(&to.id)?, to.generation)?;
    refresh_parent_tx(tx, &brief_command, now)?;
    let parent = load_command(tx, &request.id)?
        .ok_or_else(|| Error::new("internal", "handoff parent disappeared"))?;
    let value = serde_json::from_value(parent.outcome.unwrap())
        .map_err(|_| Error::new("internal", "handoff result malformed"))?;
    request.validate_response(&value)?;
    Ok(HandoffAdmission {
        value,
        events,
        is_new: true,
    })
}

pub(super) fn quiescent(tx: &Transaction<'_>, agent: &Agent, destination: bool) -> Result<()> {
    observer::require_controlled(agent)?;
    let num = parse_agent_id(&agent.id)?;
    if agent.state != AgentState::Idle || agent.active_turn.is_some() || !agent.pending.is_empty() {
        return Err(Error::new(
            "busy",
            format!(
                "handoff requires idle Agent {} generation {} with no active turn or pending request; its state is {}",
                agent.id,
                agent.generation,
                agent_state_name(agent.state),
            ),
        ));
    }
    if has_unknown_gate(tx, num)? || !send_gate_open(tx, num, agent.generation)? {
        return Err(Error::new(
            "busy",
            format!(
                "Agent {} generation {} has an unresolved outcome blocking handoff",
                agent.id, agent.generation
            ),
        ));
    }
    let open: Option<String> = tx
        .query_row(
            "SELECT id FROM commands WHERE target=?1 AND
              (state IN ('dispatched','unknown') OR (?2 AND state IN ('accepted','queued')))
             ORDER BY created_at,id LIMIT 1",
            params![num, destination],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    if let Some(command) = open {
        return Err(Error::new(
            "busy",
            format!("Agent {} has unresolved command {command}", agent.id),
        ));
    }
    let pending: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pending_requests WHERE agent=?1 AND state='pending')",
            [num],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if pending {
        return Err(Error::new(
            "busy",
            format!(
                "Agent {} has an unresolved request blocking handoff",
                agent.id
            ),
        ));
    }
    Ok(())
}

/// Only ordinal-zero SEND is the brief; links are the existing command representation.
pub(super) fn is_brief_tx(tx: &Connection, id: &str) -> Result<bool> {
    tx.query_row("SELECT EXISTS(SELECT 1 FROM commands c JOIN commands p ON p.id=c.parent_command
        WHERE c.id=?1 AND c.link_kind='handoff' AND c.ordinal=0 AND c.verb='send' AND p.method='swarm.handoff' AND p.verb='handoff')",[id],|row|row.get(0)).map_err(sql_error)
}

/// Automatic native loss cannot discard withheld work. Explicit Owner cancellation
/// and generation/lifecycle actions keep their normal no-effect cancellation rules.
pub(super) fn held_successor_tx(tx: &Transaction<'_>, id: &str) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM commands c JOIN commands brief
        ON brief.parent_command=c.parent_command AND brief.link_kind='handoff' AND brief.ordinal=0
        WHERE c.id=?1 AND c.link_kind='handoff' AND c.ordinal>0 AND brief.state<>'applied')",
        [id],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

/// Keep only delivery readback mutable in the parent result. No new receipt table.
pub(super) fn refresh_parent_tx(tx: &Transaction<'_>, id: &str, now: i64) -> Result<()> {
    if !is_brief_tx(tx, id)? {
        return Ok(());
    }
    let (parent, brief_state): (String, String) = tx
        .query_row(
            "SELECT parent_command,state FROM commands WHERE id=?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(sql_error)?;
    let state: CommandState = parse_enum(&brief_state, "command state")?;
    let gate = match state {
        CommandState::Applied => "released",
        CommandState::Unknown => "queued_brief_unknown",
        CommandState::Failed | CommandState::Rejected | CommandState::Cancelled => {
            "queued_brief_failed"
        }
        _ => "queued_until_brief_applied",
    };
    tx.execute("UPDATE commands SET outcome=json_set(outcome,'$.brief_state',?1,'$.successor_gate',?2),updated_at=?3 WHERE id=?4",params![brief_state,gate,now,parent]).map_err(sql_error)?;
    Ok(())
}

/// A failed or unknown brief holds successors, never cancels or replays them.
pub(super) fn gate_tx(
    tx: &Transaction<'_>,
    command: &CommandRecord,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<bool> {
    let link: Option<(String, i64)> = tx
        .query_row(
            "SELECT parent_command,ordinal FROM commands WHERE id=?1 AND link_kind='handoff'",
            [&command.command.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let Some((parent, ordinal)) = link else {
        return Ok(false);
    };
    let brief_id: String = tx
        .query_row(
            "SELECT id FROM commands WHERE parent_command=?1 AND link_kind='handoff' AND ordinal=0",
            [&parent],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if !is_brief_tx(tx, &brief_id)? {
        return Err(Error::new("source_integrity", "invalid handoff child set"));
    }
    if ordinal == 0 {
        return Ok(false);
    }
    let ready: bool = tx
        .query_row(
            "SELECT state='applied' FROM commands WHERE id=?1",
            [&brief_id],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if ready {
        return Ok(false);
    }
    if command.state == CommandState::Accepted {
        update_command_state(
            tx,
            &command.command.id,
            CommandState::Queued,
            None,
            None,
            now,
        )?;
        events.push(insert_daemon_command_event(
            tx,
            now,
            "command.queued",
            command.target,
            command.command.generation,
            &command.command.id,
            json!({"reason":"handoff_brief_not_applied","brief":brief_id}),
        )?);
    }
    Ok(true)
}
