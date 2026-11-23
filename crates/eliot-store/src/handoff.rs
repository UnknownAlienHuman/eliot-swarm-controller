//! Task transfer is one Store transaction and ordinary generation-bound SENDs.
use super::*;
use eliot_core::api::{
    CommandCancelRequest, CommandCancelResponse, HandoffRequest, HandoffResponse, ReparentRequest,
    SendTransfer,
};

mod brief;
pub(super) use brief::{last_turn_uncertain, last_unfinished_turn};

/// Committed when a generation's task inputs, claims, Goal and children move.
pub const AGENT_HANDED_OFF: &str = "agent.handed_off";

/// What the Router read from Git and the file system for one brief.
#[derive(Clone, Debug, Default)]
pub struct BriefFacts {
    pub git: Option<String>,
    /// Exact original SEND bound to the task record read for this brief.
    pub task_command: Option<String>,
    /// Where the task's own saved files can be read, shown with the task.
    pub task_note: Option<String>,
    /// The task text from the Git record, used when the Store retains none.
    pub task_text: Option<String>,
}

/// Bound on following a chain of same-worktree handoffs.
const MAX_SUCCESSOR_CHAIN: usize = 64;
const METHOD: &str = "swarm.handoff";

#[derive(Debug)]
pub struct HandoffAdmission {
    pub value: HandoffResponse,
    pub events: Vec<Event>,
    pub is_new: bool,
}

impl Store {
    /// Find the task turn before any commandless native continuations.
    /// An intervening failed or unbound ordinary turn still blocks handoff.
    pub fn latest_handoff_task_turn(&self, agent: &str, generation: i64) -> Result<Option<Turn>> {
        let agent_num = parse_agent_id(agent)?;
        let turn_id: Option<String> = self
            .connection
            .query_row(
                "SELECT turn_id FROM turns WHERE agent = ?1 AND generation = ?2
                 AND (native_trigger IS NULL OR start_command_id IS NOT NULL)
                 ORDER BY COALESCE(started_at, completed_at, 0) DESC, turn_id DESC LIMIT 1",
                params![agent_num, generation],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        turn_id
            .as_deref()
            .map(|turn_id| self.get_turn(agent, generation, turn_id))
            .transpose()
            .map(Option::flatten)
    }

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

    /// The caller's authority over this source, before any worktree read
    /// or record that the transfer would need.
    pub fn check_handoff_caller(&self, principal: &str, request: &HandoffRequest) -> Result<()> {
        self.ensure_committed_reads()?;
        let from = load_agent_by_num(&self.connection, parse_agent_id(&request.from)?)?
            .ok_or_else(|| Error::new("not_found", "source Agent missing"))?;
        if from.generation != request.from_generation {
            return Err(Error::new("stale_generation", "handoff generation moved"));
        }
        let input_principal = if principal == "owner" {
            None
        } else {
            let caller = hierarchy::parse_caller(&self.connection, principal)?;
            hierarchy::caller_may_command(&self.connection, &caller, &from)?;
            Some(principal)
        };
        source_task(
            &self.connection,
            &from,
            &request.task_command,
            input_principal,
        )
        .map(|_| ())
    }

    pub fn handoff(
        &mut self,
        principal: &str,
        request: &HandoffRequest,
        policy: &core::SecretPolicy,
        owner_process: Option<&OwnerProcess>,
        facts: &BriefFacts,
        now: i64,
    ) -> Result<HandoffAdmission> {
        self.ensure_writer_healthy()?;
        let result = self.handoff_inner(principal, request, policy, owner_process, facts, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn handoff_inner(
        &mut self,
        principal: &str,
        request: &HandoffRequest,
        policy: &core::SecretPolicy,
        owner_process: Option<&OwnerProcess>,
        facts: &BriefFacts,
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
            HandoffInput::Brief {
                facts,
                discover: false,
            },
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
) -> Result<(Option<Command>, Vec<Command>)> {
    let task = continuation_task(connection, from, task_command, principal)?;
    let originals = source_sends(
        connection,
        from,
        commands,
        principal,
        policy,
        from.is_terminal(),
    )?;
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

/// The command stating this Agent's task, from any of its generations. A
/// named command that retention removed is not an error: the first retained
/// instruction serves instead, and the Git task record when there is none.
fn continuation_task(
    connection: &Connection,
    from: &Agent,
    task_command: &str,
    principal: Option<&str>,
) -> Result<Option<Command>> {
    if !task_command.is_empty()
        && let Some(task) = load_command(connection, task_command)?
    {
        if task.target.as_deref() != Some(&from.id)
            || !matches!(task.verb.as_str(), "send" | "launch")
            || task.args.get("text").and_then(Value::as_str).is_none()
        {
            return Err(Error::new(
                "invalid",
                format!("task {task_command} is not an instruction to {}", from.id),
            ));
        }
        if principal.is_some_and(|principal| task.principal != principal) {
            return Err(Error::new(
                "not_allowed",
                "handoff transfers only SENDs issued by the caller",
            ));
        }
        return Ok(Some(task));
    }
    let id: Option<String> = connection
        .query_row(
            "SELECT id FROM commands INDEXED BY commands_target
             WHERE target = ?1 AND verb IN ('send', 'launch')
               AND principal NOT LIKE 'swarm_note:%'
               AND (?2 IS NULL OR principal = ?2)
               AND json_type(args, '$.text') = 'text'
             ORDER BY (link_kind IS 'handoff' AND ordinal = 0), state = 'rejected',
                      created_at, rowid
             LIMIT 1",
            params![parse_agent_id(&from.id)?, principal],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    id.map(|id| load_command(connection, &id))
        .transpose()
        .map(Option::flatten)
}

fn source_sends(
    connection: &Connection,
    from: &Agent,
    commands: &[String],
    principal: Option<&str>,
    policy: &core::SecretPolicy,
    stopped_source: bool,
) -> Result<Vec<Command>> {
    let mut originals = Vec::new();
    for id in commands {
        let record = load_command_record(connection, id)?.ok_or_else(|| {
            Error::new("not_found", format!("selected SEND {id} is not retained"))
        })?;
        let c = &record.command;
        if c.target.as_deref() != Some(&from.id)
            || c.generation != Some(from.generation)
            || !transferable_send(&record, stopped_source)
        {
            let required = if stopped_source {
                "a plain SEND stopped or fenced before dispatch"
            } else {
                "an undispatched plain SEND"
            };
            return Err(Error::new(
                "bad_state",
                format!(
                    "selected command {id} must be {required} on {} generation {}",
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

/// A plain SEND that never reached the native session. From a stopped or lost
/// source this also covers input the generation's end cancelled without effect.
pub(super) fn transferable_send(record: &CommandRecord, stopped_source: bool) -> bool {
    let c = &record.command;
    let pending = matches!(c.state, CommandState::Accepted | CommandState::Queued);
    // A provider hold refused this input at dispatch, before any native effect.
    let held = c.state == CommandState::Rejected
        && c.outcome.as_ref().is_some_and(|outcome| {
            outcome.get("successor").is_none()
                && outcome["reason"] == "provider_held"
                && outcome["where"] == "router.dispatch.preflight"
        });
    let undelivered = pending
        || held
        || (stopped_source
            && c.outcome.as_ref().is_some_and(|outcome| {
                outcome.get("successor").is_none()
                    && match c.state {
                        CommandState::Cancelled => {
                            outcome["reason"] == "generation_stopped"
                                || outcome["reason"] == "generation_lost"
                        }
                        CommandState::Rejected => {
                            outcome["reason"] == "transfer_in_progress"
                                && outcome["where"] == "router.dispatch.preflight"
                        }
                        _ => false,
                    }
            }));
    c.verb == "send"
        && undelivered
        && c.turn_id.is_none()
        && record.expected_turn_id.is_none()
        && c.request_id.is_none()
        && !c.probe
        && c.selector.is_none()
        && c.args
            .as_object()
            .is_some_and(|args| args.len() == 1 && args.contains_key("text"))
}

#[derive(Clone, Copy)]
pub(super) enum HandoffInput<'a> {
    /// Context for an existing destination, which waits for its own input.
    Brief {
        facts: &'a BriefFacts,
        /// A switch finds the task itself and accepts an ended source.
        discover: bool,
    },
    /// B continues the task from its branch.
    Continuation {
        directive: &'a str,
        facts: &'a BriefFacts,
    },
    /// The switch has already proven an exact provider refusal before work.
    ProviderRetry,
    /// The project transfer has proved source custody and performed the team edits.
    ProjectTransfer { saved_context: Option<&'a str> },
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
    let from_switch = matches!(
        input,
        HandoffInput::Continuation { .. } | HandoffInput::Brief { discover: true, .. }
    );
    if (!project_transfer && !from_switch) || !request.task_command.is_empty() {
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
    let continuation = matches!(input, HandoffInput::Continuation { .. });
    if project_transfer && from.is_terminal() {
        require_settled_input(tx, &from, parse_agent_id(&from.id)?)?;
    } else if from_switch {
        transfer_source_ready(tx, &from)?;
    } else {
        quiescent(tx, &from, false)?;
    }
    quiescent(tx, &to, true)?;
    let input_principal = manager_caller.as_ref().map(|_| principal);
    let task = if from_switch {
        continuation_task(tx, &from, &request.task_command, input_principal)?
    } else if project_transfer && request.task_command.is_empty() {
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
        project_transfer || (from_switch && from.is_terminal()),
    )?;
    let required_task = || {
        task.as_ref()
            .ok_or_else(|| Error::new("source_integrity", "handoff task is missing"))
    };
    let (brief_text, brief_truncated) = match input {
        HandoffInput::Brief { facts, .. } => brief::build(
            tx,
            request,
            task.as_ref(),
            policy,
            brief::BriefInput {
                saved_context: None,
                transferred: &originals,
                facts: Some(facts),
                directive: None,
                now,
            },
        )?,
        HandoffInput::Continuation { directive, facts } => brief::build(
            tx,
            request,
            task.as_ref(),
            policy,
            brief::BriefInput {
                saved_context: None,
                transferred: &originals,
                facts: Some(facts),
                directive: Some(directive),
                now,
            },
        )?,
        HandoffInput::ProjectTransfer { saved_context } => brief::build(
            tx,
            request,
            task.as_ref(),
            policy,
            brief::BriefInput {
                saved_context,
                transferred: &originals,
                facts: None,
                directive: None,
                now,
            },
        )?,
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
            let goal_revision =
                goals::transfer_goal_tx(tx, &from, &to, policy, continuation, now, &mut events)?;
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
        let mut outcome = if original.state.is_terminal() {
            original.outcome.clone().ok_or_else(|| {
                Error::new("source_integrity", "no-effect SEND outcome disappeared")
            })?
        } else {
            cancel_pending_command_tx(
                tx,
                parse_agent_id(&from.id)?,
                from.generation,
                &original.id,
                "handoff_no_effect",
                now,
                &mut events,
            )?;
            json!({"reason": "handoff_no_effect"})
        };
        outcome["no_effect"] = json!(true);
        outcome["handoff"] = json!(request.id);
        outcome["successor"] = json!(transfer.successor);
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
    // The durable fact that this generation's work has moved. Automatic
    // handoff reads it so the same work is never moved twice.
    events.push(insert_daemon_event(
        tx,
        now,
        AGENT_HANDED_OFF,
        "handoff",
        Some(parse_agent_id(&from.id)?),
        Some(from.generation),
        json!({"handoff": request.id, "to": to.id, "to_generation": to.generation}),
    )?);
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

/// A continuation source is idle, or an ended session whose processes have
/// fully drained. Unknown outcomes of an ended generation cannot still land:
/// they stay recorded, are named in the brief and are never replayed.
pub(super) fn transfer_source_ready(tx: &Transaction<'_>, agent: &Agent) -> Result<()> {
    observer::require_controlled(agent)?;
    if !agent.is_terminal() {
        return live_source_ready(tx, agent);
    }
    if let Some(blocker) = session_exit_blocker(tx, agent)? {
        return Err(Error::new(
            "busy",
            format!(
                "Agent {} may still be writing to its worktree: {blocker}",
                agent.id
            ),
        ));
    }
    Ok(())
}

/// Only the running generation can still act on an uncertain command or
/// turn, so only that blocks a live source, and stopping it clears the block.
fn live_source_ready(tx: &Transaction<'_>, agent: &Agent) -> Result<()> {
    require_idle(agent, false)?;
    let num = parse_agent_id(&agent.id)?;
    let open: Option<String> = tx
        .query_row(
            "SELECT verb || ' ' || id || ' (' || state || ')' FROM commands
             WHERE target = ?1 AND generation = ?2 AND state IN ('dispatched', 'unknown')
             UNION ALL
             SELECT 'turn ' || turn_id || ' (' || state || ')' FROM turns
             WHERE agent = ?1 AND generation = ?2 AND state IN ('running', 'unknown')
             LIMIT 1",
            params![num, agent.generation],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    if let Some(open) = open {
        return Err(Error::new(
            "busy",
            format!(
                "the running session of Agent {} may still apply {open}. Run `swarm stop {}`, then retry; the brief will name it",
                agent.id, agent.id
            ),
        ));
    }
    require_no_pending_request(tx, agent, num)
}

/// The Agent now holding this Agent's worktree, following each
/// same-worktree handoff to the end of the chain.
pub(crate) fn current_successor(connection: &Connection, agent: i64) -> Result<Option<String>> {
    let mut statement = connection
        .prepare_cached("SELECT successor FROM agent_successors WHERE agent = ?1")
        .map_err(sql_error)?;
    let (mut current, mut cursor) = (None, agent);
    for _ in 0..MAX_SUCCESSOR_CHAIN {
        let Some(next) = statement
            .query_row([cursor], |row| row.get::<_, i64>(0))
            .optional()
            .map_err(sql_error)?
        else {
            break;
        };
        current = Some(next);
        cursor = next;
    }
    Ok(current.map(format_agent_id))
}

/// New input to a handed-off Agent would write to a worktree its successor
/// now owns, so it is refused with the Agent to use instead.
pub(crate) fn require_not_handed_off(connection: &Connection, agent: i64) -> Result<()> {
    match current_successor(connection, agent)? {
        Some(successor) => Err(Error::new(
            "handed_off",
            format!(
                "Agent {} handed its work to {successor}; send to {successor} instead",
                format_agent_id(agent)
            ),
        )),
        None => Ok(()),
    }
}

/// What still prevents treating an ended generation's process tree as gone.
/// A lost state alone proves nothing: orphaned harness children can keep
/// writing until custody records the whole family's exit.
pub(super) fn session_exit_blocker(tx: &Connection, agent: &Agent) -> Result<Option<String>> {
    let num = parse_agent_id(&agent.id)?;
    if let Some(blocker) = agent_lifecycle::custody_blocker_tx(tx, num, false, None)? {
        return Ok(Some(blocker));
    }
    let proof = capacity::owned_exit_proof_tx(tx, num, agent.generation)?;
    if proof.is_complete() {
        return Ok(None);
    }
    // A generation refused before any process started has nothing to drain.
    let started: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM processes WHERE owner = ?1)",
            [format!("agent:{}#{}", agent.id, agent.generation)],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if !started {
        return Ok(None);
    }
    Ok(Some(
        proof
            .missing_reason()
            .unwrap_or("process exit is unproven")
            .to_owned(),
    ))
}

pub(super) fn quiescent(tx: &Transaction<'_>, agent: &Agent, destination: bool) -> Result<()> {
    observer::require_controlled(agent)?;
    let num = parse_agent_id(&agent.id)?;
    require_idle(agent, destination)?;
    require_settled_input(tx, agent, num)?;
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
    require_no_pending_request(tx, agent, num)
}

fn require_idle(agent: &Agent, destination: bool) -> Result<()> {
    if agent.state == AgentState::Idle && agent.active_turn.is_none() && agent.pending.is_empty() {
        return Ok(());
    }
    let remedy = if destination {
        String::new()
    } else {
        format!(". Run `swarm interrupt {}`, then retry", agent.id)
    };
    Err(Error::new(
        "busy",
        format!(
            "handoff requires idle Agent {} generation {} with no active turn or pending request; its state is {}{remedy}",
            agent.id,
            agent.generation,
            agent_state_name(agent.state),
        ),
    ))
}

fn require_no_pending_request(tx: &Transaction<'_>, agent: &Agent, num: i64) -> Result<()> {
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

fn require_settled_input(tx: &Transaction<'_>, agent: &Agent, num: i64) -> Result<()> {
    if has_unknown_gate(tx, num)? || !send_outcome_gate_open(tx, num, agent.generation)? {
        return Err(Error::new(
            "busy",
            format!(
                "Agent {} generation {} has an unresolved outcome blocking handoff",
                agent.id, agent.generation
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
