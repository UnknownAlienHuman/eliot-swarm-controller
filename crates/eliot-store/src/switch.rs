//! Switch orchestration uses only existing command rows and normal launch/handoff admission.
use super::*;
use eliot_core::api::{SwitchAgent, SwitchRequest, SwitchResponse};

const METHOD: &str = "swarm.switch";

impl Store {
    pub fn replay_switch(
        &self,
        principal: &str,
        request: &SwitchRequest,
    ) -> Result<Option<SwitchResponse>> {
        self.ensure_committed_reads()?;
        request.validate()?;
        owner(principal)?;
        let digest = canonical::digest(
            &serde_json::to_value(request)
                .map_err(|_| Error::new("internal", "cannot encode switch"))?,
        )?;
        replay_on(&self.connection, principal, request, &digest)
    }

    /// Retain both child identities before any profile/auth/Git/native preparation.
    pub fn reserve_switch(
        &mut self,
        principal: &str,
        request: &SwitchRequest,
        policy: &core::SecretPolicy,
        now: i64,
    ) -> Result<Change<SwitchResponse>> {
        if let Some(value) = self.replay_switch(principal, request)? {
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        self.ensure_writer_healthy()?;
        let result = (|| {
            let args = serde_json::to_value(request)
                .map_err(|_| Error::new("internal", "cannot encode switch"))?;
            let digest = canonical::digest(&args)?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            if let Some(value) = replay_on(&tx, principal, request, &digest)? {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value,
                    events: Vec::new(),
                });
            }
            validate_id_age(&request.id, now)?;
            if policy.contains_value(&args) {
                return Err(Error::new(
                    "secret_in_payload",
                    "switch contains a known secret",
                ));
            }
            if encode_json(&args)?.len() > MAX_EVENT_BYTES {
                return Err(Error::new(
                    "frame_too_large",
                    "switch exceeds command budget",
                ));
            }
            source_tx(&tx, request, self.hierarchy_limits)?;
            let value = SwitchResponse {
                id: request.id.clone(),
                from: request.from.clone(),
                from_generation: request.from_generation,
                state: CommandState::Accepted,
                launch_command: Ulid::new().to_string(),
                handoff_command: Ulid::new().to_string(),
                launch_state: None,
                created: None,
                handoff: None,
                error: None,
                next_action: "preparing_launch".into(),
            };
            let mut parent = SubmitCommand::new(&request.id, "switch");
            parent.args = args;
            let outcome = serde_json::to_value(&value)
                .map_err(|_| Error::new("internal", "cannot encode switch result"))?;
            let event = admit_command(
                &tx,
                &CommandInsert {
                    method: METHOD,
                    outcome: Some(&outcome),
                    ..CommandInsert::targetless_admitted(principal, &parent, &digest, now)
                },
                json!({"command_id":request.id,"verb":"switch"}),
            )?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value,
                events: vec![event],
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Claim one worker once. Retries are readback, not continuation or another launch.
    pub fn start_switch(
        &mut self,
        id: &str,
        now: i64,
    ) -> Result<Change<Option<(SwitchRequest, SwitchResponse)>>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let command = switch_parent(&tx, id)?;
            if command.state != CommandState::Accepted {
                return Ok(Change {
                    value: None,
                    events: Vec::new(),
                });
            }
            let request: SwitchRequest = serde_json::from_value(command.args)
                .map_err(|_| Error::new("source_integrity", "switch request malformed"))?;
            let value = stored_result(&command.outcome)?;
            update_command_state(
                &tx,
                id,
                CommandState::Dispatched,
                command.outcome.as_ref(),
                None,
                now,
            )?;
            let event = insert_daemon_command_event(
                &tx,
                now,
                "command.dispatched",
                None,
                None,
                id,
                json!({"component":"switch_orchestration"}),
            )?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: Some((request, value)),
                events: vec![event],
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn switch_launch_ready(&self, id: &str) -> Result<bool> {
        self.ensure_committed_reads()?;
        let value = result_on(&self.connection, id)?;
        let Some(launch) = load_command(&self.connection, &value.launch_command)? else {
            return Ok(false);
        };
        if launch.state != CommandState::Applied {
            return Ok(false);
        }
        match ready_destination_tx(&self.connection, &launch) {
            Ok(_) => Ok(true),
            Err(error) if error.code == "internal" => Err(error),
            Err(_) => Ok(false),
        }
    }

    pub fn switch_result(&self, id: &str) -> Result<SwitchResponse> {
        self.ensure_committed_reads()?;
        result_on(&self.connection, id)
    }

    /// Same transaction as normal launch allocation, capacity/workspace reservation and child link.
    pub fn admit_switch_launch(
        &mut self,
        id: &str,
        request: SubmitCommand,
        preparation: Option<WorkspacePreparationInput>,
        now: i64,
    ) -> Result<Admission> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let parent = switch_parent(&tx, id)?;
            let plan = stored_result(&parent.outcome)?;
            if parent.state != CommandState::Dispatched
                || request.id != plan.launch_command
                || request.verb != "launch"
                || request.args.get("text").is_some()
                || request.args.get("prompt").is_some()
                || request.args.get("parent").is_some()
            {
                return Err(Error::new(
                    "bad_state",
                    "switch launch binding or phase differs",
                ));
            }
            let source: SwitchRequest = serde_json::from_value(parent.args)
                .map_err(|_| Error::new("source_integrity", "switch request malformed"))?;
            let agent = source_tx(&tx, &source, self.hierarchy_limits)?;
            let requested = serde_json::to_value(&source.launch)
                .map_err(|_| Error::new("internal", "cannot encode switch selectors"))?;
            for key in [
                "profile", "harness", "model", "effort", "auth", "cmd", "worktree", "base",
            ] {
                if let Some(value) = requested.get(key).filter(|value| !value.is_null())
                    && request.args.get(key) != Some(value)
                {
                    return Err(Error::new(
                        "id_conflict",
                        "prepared launch differs from reserved selectors",
                    ));
                }
            }

            if request.args.get("project").and_then(Value::as_str) != Some(agent.project_scope()) {
                return Err(Error::new(
                    "not_allowed",
                    "switch launch must keep the source project",
                ));
            }
            if load_command(&tx, &request.id)?.is_some()
                && component(&tx, id, &request.id, 0)?.is_none()
            {
                return Err(Error::new(
                    "id_conflict",
                    "reserved launch id occupied by another command",
                ));
            }
            let admission = admission::submit_tx(
                &tx,
                "owner",
                request,
                preparation,
                now,
                false,
                self.hierarchy_limits,
                self.capacity_policy.as_deref(),
            )?;
            tx.execute(
                "UPDATE commands SET parent_command=?1,link_kind='switch',ordinal=0 WHERE id=?2",
                params![id, plan.launch_command],
            )
            .map_err(sql_error)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(admission)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Shared H2 transfer and switch completion commit together. On refusal the savepoint
    /// rolls back every H2 mutation, then records the created B for ordinary Owner recovery.
    pub fn finish_switch(
        &mut self,
        id: &str,
        policy: &core::SecretPolicy,
        now: i64,
    ) -> Result<Change<SwitchResponse>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let parent = switch_parent(&tx, id)?;
            if parent.state != CommandState::Dispatched {
                let value = result_on(&tx, id)?;
                return Ok(Change {
                    value,
                    events: Vec::new(),
                });
            }
            let request: SwitchRequest = serde_json::from_value(parent.args.clone())
                .map_err(|_| Error::new("source_integrity", "switch request malformed"))?;
            let plan = stored_result(&parent.outcome)?;
            let launch = component(&tx, id, &plan.launch_command, 0)?
                .ok_or_else(|| Error::new("bad_state", "switch launch not admitted"))?;
            let mut events = Vec::new();
            let mut state = CommandState::Failed;
            let mut error = None;
            if launch.state == CommandState::Applied {
                tx.execute_batch("SAVEPOINT switch_transfer")
                    .map_err(sql_error)?;
                let transfer: Result<handoff::HandoffAdmission> = (|| {
                    let destination = ready_destination_tx(&tx, &launch)?;
                    let handoff = request.handoff(
                        &plan.handoff_command,
                        &destination.id,
                        destination.generation,
                    );
                    let change = handoff::handoff_tx(
                        &tx,
                        "owner",
                        &handoff,
                        policy,
                        now,
                        self.hierarchy_limits,
                    )?;
                    tx.execute("UPDATE commands SET parent_command=?1,link_kind='switch',ordinal=1 WHERE id=?2", params![id,plan.handoff_command]).map_err(sql_error)?;
                    Ok(change)
                })();
                match transfer {
                    Ok(change) => {
                        tx.execute_batch("RELEASE switch_transfer")
                            .map_err(sql_error)?;
                        events.extend(change.events);
                        state = CommandState::Applied;
                    }
                    Err(e) => {
                        tx.execute_batch("ROLLBACK TO switch_transfer; RELEASE switch_transfer")
                            .map_err(sql_error)?;
                        error = Some(e);
                    }
                }
            } else {
                state = if matches!(
                    launch.state,
                    CommandState::Accepted
                        | CommandState::Queued
                        | CommandState::Dispatched
                        | CommandState::Unknown
                ) {
                    CommandState::Unknown
                } else {
                    CommandState::Failed
                };
                error = Some(Error::new(
                    "launch_not_ready",
                    "launch has no proven readiness",
                ));
            }
            // Persistence faults must roll back the whole transaction, never be converted to a refusal.
            if error.as_ref().is_some_and(|error| error.code == "internal") {
                return Err(error.unwrap());
            }
            let mut value = result_on(&tx, id)?;
            value.error = error.map(|error| error.code);
            value.state = state;
            // H2 owns the full transfer result; keep only its fixed reference here.
            value.handoff = None;
            let outcome = serde_json::to_value(&value)
                .map_err(|_| Error::new("internal", "cannot encode switch result"))?;
            update_command_state(&tx, id, state, Some(&outcome), None, now)?;
            events.push(insert_daemon_command_event(&tx, now, &format!("command.{}",command_state_name(state)), None, None, id, json!({"launch_command":plan.launch_command,"handoff_command":plan.handoff_command}))?);
            let value = result_on(&tx, id)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change { value, events })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn fail_switch_preparation(
        &mut self,
        id: &str,
        code: &str,
        now: i64,
    ) -> Result<Change<Command>> {
        let mut value = self.switch_result(id)?;
        value.error = Some(code.to_owned());
        self.settle(
            id,
            CommandState::Rejected,
            Some(
                serde_json::to_value(value)
                    .map_err(|_| Error::new("internal", "cannot encode switch result"))?,
            ),
            now,
        )
    }
}

fn replay_on(
    connection: &Connection,
    principal: &str,
    request: &SwitchRequest,
    digest: &[u8],
) -> Result<Option<SwitchResponse>> {
    if let Some((p, m, d)) = command_binding(connection, &request.id)? {
        if p != principal || m != METHOD || d != digest {
            return Err(Error::new("id_conflict", "switch id has another binding"));
        }
        let result = result_on(connection, &request.id)?;
        request.validate_response(&result)?;
        return Ok(Some(result));
    }
    if write_exists(connection, &request.id)? || digest_id_bound(connection, &request.id)? {
        return Err(Error::new("id_conflict", "switch id used by another write"));
    }
    Ok(None)
}

fn owner(principal: &str) -> Result<()> {
    if principal == "owner" {
        Ok(())
    } else {
        Err(Error::new("not_allowed", "only Owner may switch Agents"))
    }
}

fn switch_parent(connection: &Connection, id: &str) -> Result<Command> {
    let parent =
        load_command(connection, id)?.ok_or_else(|| Error::new("not_found", "switch missing"))?;
    if parent.method != METHOD || parent.verb != "switch" || parent.principal != "owner" {
        return Err(Error::new("bad_state", "not an Owner switch"));
    }
    Ok(parent)
}

fn stored_result(outcome: &Option<Value>) -> Result<SwitchResponse> {
    serde_json::from_value(
        outcome
            .clone()
            .ok_or_else(|| Error::new("source_integrity", "switch plan missing"))?,
    )
    .map_err(|_| Error::new("source_integrity", "switch plan malformed"))
}

fn component(
    connection: &Connection,
    parent: &str,
    id: &str,
    ordinal: i64,
) -> Result<Option<Command>> {
    let linked: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM commands WHERE id=?1 AND parent_command=?2 AND link_kind='switch' AND ordinal=?3)",params![id,parent,ordinal],|row|row.get(0)).map_err(sql_error)?;
    if linked {
        load_command(connection, id)
    } else {
        Ok(None)
    }
}

fn result_on(connection: &Connection, id: &str) -> Result<SwitchResponse> {
    let parent = switch_parent(connection, id)?;
    let mut result = stored_result(&parent.outcome)?;
    result.state = parent.state;
    let mut ready = false;
    if let Some(launch) = component(connection, id, &result.launch_command, 0)? {
        ready = match ready_destination_tx(connection, &launch) {
            Ok(_) => launch.state == CommandState::Applied,
            Err(error) if error.code == "internal" => return Err(error),
            Err(_) => false,
        };
        result.launch_state = Some(launch.state);
        result.created = launch.target.map(|agent| SwitchAgent {
            agent,
            generation: launch.generation.unwrap_or(0),
        });
    }
    if let Some(handoff) = component(connection, id, &result.handoff_command, 1)? {
        result.handoff = Some(
            serde_json::from_value(
                handoff
                    .outcome
                    .ok_or_else(|| Error::new("source_integrity", "handoff result missing"))?,
            )
            .map_err(|_| Error::new("source_integrity", "handoff result malformed"))?,
        );
    }
    result.next_action = if result.handoff.is_some() {
        "read_handoff_children"
    } else if result.launch_state == Some(CommandState::Unknown) {
        "resolve_launch_unknown_never_relaunch"
    } else if result.launch_state == Some(CommandState::Applied)
        && parent.state != CommandState::Dispatched
    {
        if ready {
            "finish_with_ordinary_swarm.handoff"
        } else {
            "inspect_readiness_then_ordinary_swarm.handoff"
        }
    } else if parent.state == CommandState::Unknown {
        "inspect_retained_children_never_retry_launch"
    } else if matches!(
        parent.state,
        CommandState::Failed | CommandState::Rejected | CommandState::Cancelled
    ) {
        "inspect_created_agent_stop_separately"
    } else {
        "wait_for_switch"
    }
    .into();
    Ok(result)
}

fn source_tx(
    tx: &Transaction<'_>,
    request: &SwitchRequest,
    limits: core::api::HierarchyLimits,
) -> Result<Agent> {
    if request.commands.len() as i64 > limits.max_selector_targets {
        return Err(Error::new(
            "fanout_limit",
            "switch selection exceeds configured bound",
        ));
    }
    let source = load_agent_by_num(tx, parse_agent_id(&request.from)?)?
        .ok_or_else(|| Error::new("not_found", "source Agent missing"))?;
    if source.generation != request.from_generation {
        return Err(Error::new("stale_generation", "source generation moved"));
    }
    handoff::quiescent(tx, &source, false)?;
    Ok(source)
}

fn ready_destination_tx(tx: &Connection, launch: &Command) -> Result<Agent> {
    let agent = launch
        .target
        .as_deref()
        .ok_or_else(|| Error::new("source_integrity", "switch launch target missing"))?;
    let destination = load_agent_by_num(tx, parse_agent_id(agent)?)?
        .ok_or_else(|| Error::new("not_found", "created Agent missing"))?;
    // Some native adapters omit command_id on session.ready. The first generation
    // of this newly allocated B has exactly one start command: the reserved launch.
    // Use that exact persistent binding; a contradictory supplied ID still refuses.
    let ready: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM events WHERE agent=?1 AND generation=1 AND kind='session.ready' AND source='stream' AND json_extract(subject,'$.state')='idle'
        AND (json_extract(data,'$.command_id') IS NULL OR json_extract(data,'$.command_id')=?2)
        AND (json_extract(cause,'$.command_id') IS NULL OR json_extract(cause,'$.command_id')=?2))
        AND NOT EXISTS(SELECT 1 FROM commands WHERE target=?1 AND generation=1 AND verb IN ('launch','resume') AND id<>?2)", params![parse_agent_id(agent)?,launch.id], |row|row.get(0)).map_err(sql_error)?;
    if launch.verb != "launch"
        || destination.generation != 1
        || launch.generation != Some(destination.generation)
        || !ready
        || destination.state != AgentState::Idle
        || launch
            .outcome
            .as_ref()
            .and_then(|value| value.get("resolution_source"))
            .is_some()
    {
        return Err(Error::new(
            "bad_state",
            "switch requires exact committed session readiness",
        ));
    }
    let input: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM commands WHERE target=?1 AND generation=?2 AND verb='send') OR EXISTS(SELECT 1 FROM turns WHERE agent=?1 AND generation=?2)",params![parse_agent_id(agent)?,destination.generation],|row|row.get(0)).map_err(sql_error)?;
    if input {
        return Err(Error::new(
            "bad_state",
            "switch destination already has input",
        ));
    }
    Ok(destination)
}

#[cfg(test)]
mod tests;
