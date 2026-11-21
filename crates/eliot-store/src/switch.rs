//! Switch orchestration uses only existing command rows and normal launch/handoff admission.
use super::*;
use eliot_core::api::{
    ORGANIZATION_CHANGED_KIND, OrgRowChange, OrganizationChangedEvent, SwitchAgent, SwitchRequest,
    SwitchResponse,
};

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
            source_tx(&tx, request, self.hierarchy_limits, policy)?;
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
                error_reason: None,
                disposal_command: None,
                disposal_state: None,
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
        policy: &core::SecretPolicy,
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
            let (agent, target) = source_tx(&tx, &source, self.hierarchy_limits, policy)?;
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
            // Continue-in-project keeps the source authorization scope above
            // and additionally binds the resolved execution registration: the
            // prepared launch must carry the target the worker verified, at
            // the revision it verified, or a mismatched workspace or a
            // revoked registration refuses inside this transaction.
            if let Some(registration) = target {
                if request
                    .args
                    .get("registered_project")
                    .and_then(Value::as_str)
                    != Some(registration.id.as_str())
                {
                    return Err(Error::new(
                        "not_allowed",
                        "switch target project differs from prepared registration",
                    ));
                }
                let revision = request
                    .args
                    .get(core::api::SWITCH_PROJECT_REVISION_ARG)
                    .and_then(Value::as_i64);
                if revision != Some(registration.revision) {
                    return Err(Error::new(
                        "stale_assignment",
                        "switch target registration changed",
                    ));
                }
                // The frozen checkout admitted here must already be the
                // target registration's checkout: a changed registration or
                // a preparation from another repository refuses before any
                // workspace row or Agent is created.
                if request.args.get("workspace").and_then(Value::as_str)
                    != Some(registration.checkout.as_str())
                {
                    return Err(Error::new(
                        "not_allowed",
                        "switch workspace differs from its target registration",
                    ));
                }
                if let Some(input) = preparation.as_ref()
                    && (input.repo_path != registration.checkout
                        || input.repo_common_dir != registration.repo_common_dir
                        || input.project != registration.checkout)
                {
                    return Err(Error::new(
                        "not_allowed",
                        "switch workspace preparation differs from its target registration",
                    ));
                }
                if let Some(preparation) = request.args.get("workspace_preparation") {
                    let repo_path = preparation.get("repo_path").and_then(Value::as_str);
                    let repo_common_dir =
                        preparation.get("repo_common_dir").and_then(Value::as_str);
                    let preparation_project = preparation.get("project").and_then(Value::as_str);
                    if repo_path != Some(registration.checkout.as_str())
                        || repo_common_dir != Some(registration.repo_common_dir.as_str())
                        || preparation_project != Some(registration.checkout.as_str())
                    {
                        return Err(Error::new(
                            "not_allowed",
                            "switch workspace preparation differs from its target registration",
                        ));
                    }
                }
            } else if request
                .args
                .get("registered_project")
                .and_then(Value::as_str)
                .is_some_and(|project| project != agent.project)
                || (agent.project != agent.project_scope()
                    && request
                        .args
                        .get("registered_project")
                        .and_then(Value::as_str)
                        != Some(agent.project.as_str()))
            {
                // An ordinary switch preserves both the execution project and
                // the authorization scope.
                return Err(Error::new(
                    "not_allowed",
                    "switch must keep the source execution project",
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

    /// Commit the H2 transfer, or roll back H2 and admit ordinary disposal of
    /// the exact unused successor after a definite refusal.
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
            let mut disposal = None;
            // The destination binding is captured before the transfer creates
            // B's first input; re-deriving it afterwards would see that input
            // and refuse.
            let mut continued_to: Option<String> = None;
            if launch.state == CommandState::Applied {
                tx.execute_batch("SAVEPOINT switch_transfer")
                    .map_err(sql_error)?;
                let transfer: Result<handoff::HandoffAdmission> = (|| {
                    let destination = ready_destination_tx(&tx, &launch)?;
                    continued_to = Some(destination.id.clone());
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
                        // The continuation rides the existing overlay: B's
                        // link records the source it continues. Plain text,
                        // never a foreign key, so deleting the source
                        // afterwards cannot fault the Store writer.
                        let created = continued_to
                            .as_deref()
                            .ok_or_else(|| {
                                Error::new("source_integrity", "switch destination binding missing")
                            })
                            .and_then(|destination| {
                                parse_agent_id(destination).map_err(|_| {
                                    Error::new(
                                        "source_integrity",
                                        "switch destination id malformed",
                                    )
                                })
                            })?;
                        parse_agent_id(&request.from).map_err(|_| {
                            Error::new("source_integrity", "switch source id malformed")
                        })?;
                        // An existing row (a menu write raced the handoff)
                        // moves to a new revision like any overlay write, so
                        // a menu that read it before the switch is refused.
                        db!(tx.execute(
                            "INSERT INTO chat_organization(agent, project_link, tags, continued_from)
                             VALUES (?1, NULL, '[]', ?2)
                             ON CONFLICT(agent) DO UPDATE SET continued_from = excluded.continued_from,
                                 revision = chat_organization.revision + 1",
                            params![created, request.from],
                        ));
                        // The event names B's row as this write left it, so a
                        // loaded sidebar refreshes B by id at its revision.
                        let (organization, tags, revision) =
                            organization::agent_organization(&tx, created)?;
                        let change = OrganizationChangedEvent {
                            id: id.to_owned(),
                            method: METHOD.to_owned(),
                            rows: vec![OrgRowChange {
                                agent: format_agent_id(created),
                                organization,
                                tags,
                                revision: revision.ok_or_else(|| {
                                    Error::new("internal", "switch overlay row is missing")
                                })?,
                            }],
                        };
                        change.validate()?;
                        events.push(insert_daemon_event(
                            &tx,
                            now,
                            ORGANIZATION_CHANGED_KIND,
                            "organization",
                            None,
                            None,
                            serde_json::to_value(&change).map_err(|_| {
                                Error::new("internal", "cannot encode organization event")
                            })?,
                        )?);
                        state = CommandState::Applied;
                    }
                    Err(e) => {
                        tx.execute_batch("ROLLBACK TO switch_transfer; RELEASE switch_transfer")
                            .map_err(sql_error)?;
                        if e.code == "unknown" {
                            state = CommandState::Unknown;
                        } else if e.code != "internal" && continued_to.is_some() {
                            let destination = ready_destination_tx(&tx, &launch)?;
                            let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
                            stop.target = Some(destination.id);
                            stop.expected_generation = Some(destination.generation);
                            stop.args = json!({"force": true});
                            let admitted = admission::submit_tx(
                                &tx,
                                "owner",
                                stop,
                                None,
                                now,
                                false,
                                self.hierarchy_limits,
                                self.capacity_policy.as_deref(),
                            )?;
                            tx.execute(
                                "UPDATE commands SET parent_command=?1,link_kind='switch',ordinal=2 WHERE id=?2",
                                params![id, admitted.command.id],
                            ).map_err(sql_error)?;
                            events.extend(admitted.events);
                            disposal = Some(admitted.command);
                        }
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
            if let Some(error) = &error {
                set_switch_error(&mut value, error, policy);
            }
            value.state = state;
            if let Some(disposal) = disposal {
                value.disposal_command = Some(disposal.id);
                value.disposal_state = Some(disposal.state);
            }
            value.next_action = switch_next_action(&value).into();
            // H2 owns the full transfer result; keep only its fixed reference here.
            value.handoff = None;
            let outcome = serde_json::to_value(&value)
                .map_err(|_| Error::new("internal", "cannot encode switch result"))?;
            update_command_state(&tx, id, state, Some(&outcome), None, now)?;
            events.push(insert_daemon_command_event(
                &tx, now, &format!("command.{}",command_state_name(state)), None, None, id,
                json!({"launch_command":plan.launch_command,"handoff_command":plan.handoff_command,
                    "error":value.error,"reason":value.error_reason,"disposal_command":value.disposal_command}),
            )?);
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
        error: &Error,
        policy: &core::SecretPolicy,
        now: i64,
    ) -> Result<Change<Command>> {
        let mut value = self.switch_result(id)?;
        set_switch_error(&mut value, error, policy);
        value.state = CommandState::Rejected;
        value.next_action.clear();
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

fn set_switch_error(value: &mut SwitchResponse, error: &Error, policy: &core::SecretPolicy) {
    value.error = Some(core::bound_chars(&policy.redact_text(&error.code), 128));
    value.error_reason = Some(core::bound_chars(&policy.redact_text(&error.message), 512));
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

/// Check the exact unused successor again when its reserved stop dispatches.
/// An Owner may have admitted input to B after the refusal was committed.
pub(super) fn require_unused_switch_successor(
    tx: &Transaction<'_>,
    stop_id: &str,
    agent: &Agent,
) -> Result<()> {
    let switch_id: Option<String> = tx
        .query_row(
            "SELECT parent_command FROM commands WHERE id=?1 AND link_kind='switch' AND ordinal=2",
            [stop_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    let Some(switch_id) = switch_id else {
        return Ok(());
    };
    let parent = switch_parent(tx, &switch_id)?;
    let receipt = stored_result(&parent.outcome)?;
    if parent.state != CommandState::Failed || receipt.disposal_command.as_deref() != Some(stop_id)
    {
        return Err(Error::new(
            "source_integrity",
            "switch successor stop does not match the refusal",
        ));
    }
    let launch = component(tx, &switch_id, &receipt.launch_command, 0)?
        .ok_or_else(|| Error::new("source_integrity", "switch launch is not retained"))?;
    let destination = ready_destination_tx(tx, &launch)?;
    if destination.id != agent.id || destination.generation != agent.generation {
        return Err(Error::new(
            "stale_generation",
            "switch successor generation moved",
        ));
    }
    Ok(())
}

fn result_on(connection: &Connection, id: &str) -> Result<SwitchResponse> {
    let parent = switch_parent(connection, id)?;
    let mut result = stored_result(&parent.outcome)?;
    result.state = parent.state;
    if let Some(launch) = component(connection, id, &result.launch_command, 0)? {
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
    if let Some(disposal_id) = &result.disposal_command {
        let disposal = component(connection, id, disposal_id, 2)?.ok_or_else(|| {
            Error::new("source_integrity", "switch successor stop is not retained")
        })?;
        if disposal.verb != "stop"
            || result.created.as_ref().is_none_or(|agent| {
                disposal.target.as_deref() != Some(&agent.agent)
                    || disposal.generation != Some(agent.generation)
            })
        {
            return Err(Error::new(
                "source_integrity",
                "switch successor stop binding changed",
            ));
        }
        result.disposal_state = Some(disposal.state);
    }
    result.next_action = switch_next_action(&result).into();
    Ok(result)
}

fn switch_next_action(result: &SwitchResponse) -> &'static str {
    match result.state {
        CommandState::Applied => "read_handoff_children",
        CommandState::Failed | CommandState::Rejected | CommandState::Cancelled => "",
        CommandState::Unknown if result.launch_state == Some(CommandState::Unknown) => {
            "resolve_launch_unknown_never_relaunch"
        }
        CommandState::Unknown => "inspect_retained_children_never_retry_launch",
        CommandState::Accepted => "preparing_launch",
        _ => "wait_for_switch",
    }
}

fn source_tx(
    tx: &Transaction<'_>,
    request: &SwitchRequest,
    limits: core::api::HierarchyLimits,
    policy: &core::SecretPolicy,
) -> Result<(Agent, Option<core::api::ProjectRegistrationView>)> {
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
    project_transfer::require_no_intent(tx, &source.project)?;
    handoff::source_inputs(
        tx,
        &source,
        &request.task_command,
        &request.commands,
        None,
        policy,
    )?;
    goals::require_handoff_source_goal(tx, &source, policy)?;
    require_switch_children(tx, &source, limits)?;
    // Continue-in-project binds the target registration at reserve time, so
    // preparation resolves the target checkout and never the source repo.
    // A projectless source has no execution project for B: it must name one.
    // An Agent continued into a project keeps that execution project, so an
    // ordinary switch afterwards needs no target.
    let target = match request.launch.project.as_deref() {
        Some(project) => Some(projects::project_registration_tx(tx, project)?.ok_or_else(
            || {
                Error::new(
                    "not_found",
                    format!("switch target project {project} was not found"),
                )
            },
        )?),
        None if source.project.is_empty() => {
            return Err(Error::new(
                "invalid",
                "switch from a projectless chat needs a target project",
            ));
        }
        None => None,
    };
    if let Some(registration) = &target {
        project_transfer::require_no_intent(tx, &registration.id)?;
    }
    Ok((source, target))
}

fn require_switch_children(
    tx: &Transaction<'_>,
    source: &Agent,
    limits: core::api::HierarchyLimits,
) -> Result<()> {
    let source_num = parse_agent_id(&source.id)?;
    // B is a new root. Its controlled tree has the same size as this
    // source's controlled subtree, with B taking the place of A.
    let count_limit = limits
        .max_tree_agents
        .min(limits.max_selector_targets.saturating_add(1));
    let members = hierarchy::controlled_subtree_member_nums(tx, source_num, count_limit)?;
    if members.len() as i64 > count_limit {
        return Err(Error::new(
            "fanout_limit",
            format!(
                "Agent {} has too many controlled descendants to transfer",
                source.id
            ),
        ));
    }
    let mut direct = 0_i64;
    for id in members.into_iter().filter(|id| *id != source_num) {
        let child = load_agent_by_num(tx, id)?
            .ok_or_else(|| Error::new("source_integrity", "switch child disappeared"))?;
        if child.project_scope() != source.project_scope() {
            return Err(Error::new(
                "not_allowed",
                format!(
                    "controlled child {} is outside the source project scope",
                    child.id
                ),
            ));
        }
        if !(1..=limits.max_depth).contains(&child.depth.saturating_sub(source.depth)) {
            return Err(Error::new(
                "fanout_limit",
                format!(
                    "controlled child {} would exceed the successor depth limit",
                    child.id
                ),
            ));
        }
        direct += i64::from(child.parent.as_deref() == Some(&source.id));
    }
    if direct > limits.max_children_per_agent {
        return Err(Error::new(
            "fanout_limit",
            format!(
                "Agent {} has too many direct controlled children to transfer",
                source.id
            ),
        ));
    }
    Ok(())
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
