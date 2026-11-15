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
    /// Router preflight permits retained retries during drain, before new admission.
    pub fn replay_handoff(
        &self,
        principal: &str,
        request: &HandoffRequest,
    ) -> Result<Option<HandoffResponse>> {
        self.ensure_committed_reads()?;
        request.validate()?;
        if principal != "owner" {
            return Err(Error::new("not_allowed", "only Owner may hand off tasks"));
        }
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
            return Ok(None);
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
        now: i64,
    ) -> Result<HandoffAdmission> {
        self.ensure_writer_healthy()?;
        let result = self.handoff_inner(principal, request, policy, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn handoff_inner(
        &mut self,
        principal: &str,
        request: &HandoffRequest,
        policy: &core::SecretPolicy,
        now: i64,
    ) -> Result<HandoffAdmission> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let change = handoff_tx(&tx, principal, request, policy, now, self.hierarchy_limits)?;
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
        if principal != "owner" {
            return Err(Error::new(
                "not_allowed",
                "only Owner may withdraw a handoff input",
            ));
        }
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
        if !eligible
            || record.command.verb != "send"
            || !matches!(record.state, CommandState::Accepted | CommandState::Queued)
        {
            return Err(Error::new(
                "bad_state",
                "cancel requires an undispatched handoff SEND",
            ));
        }
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
            "owner_handoff_no_effect",
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

/// Shared transfer transaction. Caller commits, or rolls back the entire transfer.
pub(super) fn handoff_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &HandoffRequest,
    policy: &core::SecretPolicy,
    now: i64,
    limits: core::api::HierarchyLimits,
) -> Result<HandoffAdmission> {
    request.validate()?;
    if principal != "owner" {
        return Err(Error::new("not_allowed", "only Owner may hand off tasks"));
    }
    let payload = serde_json::to_value(request)
        .map_err(|_| Error::new("internal", "cannot encode handoff"))?;
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
    if from.project != to.project {
        return Err(Error::new(
            "not_allowed",
            "handoff must stay in one project",
        ));
    }
    quiescent(tx, &from, false)?;
    quiescent(tx, &to, true)?;
    let task = load_command(tx, &request.task_command)?
        .ok_or_else(|| Error::new("not_found", "original task is not retained"))?;
    if task.verb != "send"
        || task.target.as_deref() != Some(&from.id)
        || task.generation != Some(from.generation)
    {
        return Err(Error::new(
            "invalid",
            "original task must be a SEND on A's exact generation",
        ));
    }
    let mut originals = Vec::new();
    for id in &request.commands {
        let record = load_command_record(tx, id)?
            .ok_or_else(|| Error::new("not_found", "selected SEND is not retained"))?;
        let c = &record.command;
        if c.verb != "send"
            || c.target.as_deref() != Some(&from.id)
            || c.generation != Some(from.generation)
            || !matches!(c.state, CommandState::Accepted | CommandState::Queued)
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
                "selected work is not an undispatched plain SEND on A",
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
    let (brief_text, brief_truncated) = brief::build(tx, request, &task, policy)?;
    let mut events = Vec::new();
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
    )?;
    events.extend(moved.events);
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
        reparented: moved.value.moved,
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
        cancel_pending_command_tx(
            tx,
            parse_agent_id(&from.id)?,
            from.generation,
            &original.id,
            "handoff_no_effect",
            now,
            &mut events,
        )?;
        tx.execute("UPDATE commands SET outcome=?1 WHERE id=?2",params![encode_json(&json!({"reason":"handoff_no_effect","no_effect":true,"handoff":request.id,"successor":transfer.successor}))?,original.id]).map_err(sql_error)?;
    }
    for (ordinal, id) in children.iter().enumerate() {
        let mut child = SubmitCommand::new(id.clone(), "send");
        child.target = Some(to.id.clone());
        child.expected_generation = Some(to.generation);
        child.args = if ordinal == 0 {
            json!({"text":brief_text})
        } else {
            originals[ordinal - 1].args.clone()
        };
        let child_digest = admission::command_payload_digest(&child)?;
        admission::admit_send_member(tx, principal, &child, &child_digest, &to, now, &mut events)?;
        tx.execute(
            "UPDATE commands SET parent_command=?1,link_kind='handoff',ordinal=?2 WHERE id=?3",
            params![request.id, ordinal as i64, id],
        )
        .map_err(sql_error)?;
    }
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
    if agent.state != AgentState::Idle
        || agent.active_turn.is_some()
        || !agent.pending.is_empty()
        || has_unknown_gate(tx, num)?
        || !send_gate_open(tx, num, agent.generation)?
    {
        return Err(Error::new(
            "busy",
            "handoff requires a quiescent Agent with known outcomes",
        ));
    }
    let open: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM commands WHERE target=?1 AND
        (state IN ('dispatched','unknown') OR (?2 AND state IN ('accepted','queued'))))",
            params![num, destination],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let pending: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pending_requests WHERE agent=?1 AND state='pending')",
            [num],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if open || pending {
        return Err(Error::new(
            "busy",
            "handoff has unresolved commands or requests",
        ));
    }
    Ok(())
}

/// Only ordinal-zero SEND is the brief; links are the existing command representation.
pub(super) fn is_brief_tx(tx: &Transaction<'_>, id: &str) -> Result<bool> {
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

#[cfg(test)]
mod tests;
