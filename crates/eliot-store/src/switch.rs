//! Switch orchestration uses only existing command rows and normal launch/handoff admission.
use super::*;
use eliot_core::api::{
    ORGANIZATION_CHANGED_KIND, OrgRowChange, OrganizationChangedEvent, SwitchAgent, SwitchLaunch,
    SwitchRequest, SwitchResponse,
};

const METHOD: &str = "swarm.switch";
const PROVIDER_FAILURE: &str = "provider_failure";
const PROVIDER_AUTHORITY: &str = "provider_authority";

#[derive(Clone, Copy)]
pub struct ProviderSwitchFailure {
    pub kind: core::ProviderRestrictionKind,
    pub authority: Option<[u8; 32]>,
}

impl ProviderSwitchFailure {
    pub fn require_authority(self, snapshot: &Value) -> Result<[u8; 32]> {
        let candidate = core::provider_switch_authority(snapshot)?;
        if Some(candidate) == self.authority {
            return Ok(candidate);
        }
        Err(Error::new(
            "not_allowed",
            "automatic switch authority differs from the original launch",
        ))
    }
}

pub const PROVIDER_SWITCH_QUEUED: &str = "provider.failover_queued";
pub const PROVIDER_SWITCH_SETTLED: &str = "provider.failover_settled";

pub struct SwitchWork {
    pub request: SwitchRequest,
    pub plan: SwitchResponse,
    pub provider_failure: Option<ProviderSwitchFailure>,
}

fn decode_switch(mut args: Value) -> Result<(SwitchRequest, Option<ProviderSwitchFailure>)> {
    let malformed = || Error::new("source_integrity", "switch request malformed");
    let failure = args
        .as_object_mut()
        .ok_or_else(malformed)?
        .remove(PROVIDER_FAILURE)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| malformed())?;
    let authority = args
        .as_object_mut()
        .ok_or_else(malformed)?
        .remove(PROVIDER_AUTHORITY)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| malformed())?;
    let request = serde_json::from_value(args).map_err(|_| malformed())?;
    Ok((
        request,
        failure.map(|kind| ProviderSwitchFailure { kind, authority }),
    ))
}

fn reserve_switch_tx(
    tx: &Transaction<'_>,
    request: &SwitchRequest,
    args: Value,
    digest: &[u8],
    now: i64,
) -> Result<Change<SwitchResponse>> {
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
        disposal_reason: None,
        routing: None,
        next_action: "preparing_launch".into(),
    };
    let mut parent = SubmitCommand::new(&request.id, "switch");
    parent.args = args;
    let outcome = serde_json::to_value(&value)
        .map_err(|_| Error::new("internal", "cannot encode switch result"))?;
    let event = admit_command(
        tx,
        &CommandInsert {
            method: METHOD,
            outcome: Some(&outcome),
            ..CommandInsert::targetless_admitted("owner", &parent, digest, now)
        },
        json!({"command_id":request.id,"verb":"switch"}),
    )?;
    Ok(Change {
        value,
        events: vec![event],
    })
}

/// Reserve the ordinary switch with the terminal fact, before publication.
/// The only replayable task is the original input of this role attempt.
pub(super) fn reserve_provider_switch_tx(
    tx: &Transaction<'_>,
    agent: &Agent,
    terminal: &core::NativeEvent,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    if terminal.kind != "turn.completed"
        || terminal.data.get("provider_request_refused_before_work") != Some(&Value::Bool(true))
        || terminal.data.get("outcome").and_then(Value::as_str) != Some("failure")
        || agent.generation != 1
        || agent.chat
        || agent.project.is_empty()
        || agent.observe_only
        || agent.parent.is_some()
        || agent.state != AgentState::Idle
    {
        return Ok(());
    }
    let Some(remaining) = agent
        .profile_choice
        .as_ref()
        .and_then(|choice| choice.provider_failover.as_ref())
    else {
        return Ok(());
    };
    let Some(task) = terminal.data.get("command_id").and_then(Value::as_str) else {
        return Ok(());
    };
    let Ok(restriction) = serde_json::from_value::<core::ProviderRestriction>(
        terminal.data["provider_restriction"].clone(),
    ) else {
        return Ok(());
    };
    if restriction.kind == core::ProviderRestrictionKind::ModelRemoved
        || !provider_retry_allowed(tx, agent, task)?
    {
        return Ok(());
    }
    match handoff::quiescent(tx, agent, false) {
        Ok(()) => {}
        Err(error) if error.code == "busy" => return Ok(()),
        Err(error) => return Err(error),
    }
    let agent_num = parse_agent_id(&agent.id)?;
    if remaining.is_empty() {
        events.push(insert_daemon_event(
            tx,
            now,
            PROVIDER_SWITCH_SETTLED,
            "provider",
            Some(agent_num),
            Some(agent.generation),
            json!({"task_command":task, "kind":restriction.kind, "state":"exhausted"}),
        )?);
        return Ok(());
    }
    let request = SwitchRequest {
        id: Ulid::new().to_string(),
        from: agent.id.clone(),
        from_generation: agent.generation,
        task_command: task.to_owned(),
        commands: Vec::new(),
        launch: SwitchLaunch::default(),
    };
    let mut args = serde_json::to_value(&request)
        .map_err(|_| Error::new("internal", "cannot encode provider switch"))?;
    args[PROVIDER_FAILURE] = json!(restriction.kind);
    args[PROVIDER_AUTHORITY] = json!(core::provider_switch_authority(&agent.profile_snapshot)?);
    let digest = canonical::digest(&args)?;
    events.extend(reserve_switch_tx(tx, &request, args, &digest, now)?.events);
    events.push(insert_daemon_event(
        tx,
        now,
        PROVIDER_SWITCH_QUEUED,
        "provider",
        Some(agent_num),
        Some(agent.generation),
        json!({"switch_command":request.id, "task_command":task, "kind":restriction.kind}),
    )?);
    Ok(())
}

fn require_provider_retry(tx: &Transaction<'_>, agent: &Agent, task: &str) -> Result<()> {
    if !provider_retry_allowed(tx, agent, task)? {
        return Err(Error::new("bad_state", "provider retry source changed"));
    }
    Ok(())
}

fn provider_retry_allowed(tx: &Transaction<'_>, agent: &Agent, task: &str) -> Result<bool> {
    let num = parse_agent_id(&agent.id)?;
    tx.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM commands c
            JOIN commands p ON p.id=c.parent_command
            JOIN turns t ON t.agent=c.target AND t.generation=c.generation AND t.turn_id=c.turn_id
            WHERE c.id=?1 AND c.target=?2 AND c.generation=?3 AND c.principal='owner'
              AND c.verb='send' AND c.state='applied' AND c.ordinal=0
              AND ((c.link_kind='send' AND p.verb='launch' AND p.target=c.target
                  AND NOT EXISTS(SELECT 1 FROM json_each(p.args) field
                    JOIN json_each(?4) explicit ON explicit.value=field.key))
                OR (c.link_kind='handoff' AND p.method='swarm.handoff' AND EXISTS(
                    SELECT 1 FROM commands s
                    JOIN commands l ON l.parent_command=s.id AND l.ordinal=0 AND l.target=c.target
                    WHERE s.id=p.parent_command AND s.method='swarm.switch'
                      AND json_extract(s.args,'$.provider_failure') IS NOT NULL)))
              AND t.state='completed' AND t.outcome='failure' AND t.tool_calls=0
              AND t.final_text IS NULL
              AND EXISTS(SELECT 1 FROM source_state ss WHERE ss.agent=?2 AND ss.generation=?3
                AND ss.source='stream' AND ss.state='open' AND ss.health='ok' AND ss.gap_id IS NULL)
              AND NOT EXISTS(SELECT 1 FROM commands other WHERE other.target=?2 AND other.generation=?3
                AND other.id<>c.id AND other.verb<>'launch')
        )",
        params![task, num, agent.generation, encode_json(&core::PROVIDER_LAUNCH_OVERRIDE_FIELDS)?],
        |row| row.get(0),
    ).map_err(sql_error)
}

impl Store {
    pub fn accepted_provider_switches(&self, after: &str) -> Result<Vec<String>> {
        self.ensure_committed_reads()?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT id FROM commands WHERE state='accepted' AND verb='switch' AND method=?1
                 AND json_extract(args,'$.provider_failure') IS NOT NULL AND id > ?2
                 ORDER BY id LIMIT 64",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![METHOD, after], |row| row.get(0))
            .map_err(sql_error)?;
        rows.collect::<rusqlite::Result<_>>().map_err(sql_error)
    }

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
            let change = reserve_switch_tx(&tx, request, args, &digest, now)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(change)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Claim one worker once. Retries are readback, not continuation or another launch.
    pub fn start_switch(&mut self, id: &str, now: i64) -> Result<Change<Option<SwitchWork>>> {
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
            let value = stored_result(&command.outcome)?;
            let (request, provider_failure) = decode_switch(command.args)?;
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
                value: Some(SwitchWork {
                    request,
                    plan: value,
                    provider_failure,
                }),
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
            let (source, provider_failure) = decode_switch(parent.args)?;
            let (agent, target) = source_tx(&tx, &source, self.hierarchy_limits, policy)?;
            if let Some(failure) = provider_failure {
                require_provider_retry(&tx, &agent, &source.task_command)?;
                failure.require_authority(&agent.profile_snapshot)?;
                failure.require_authority(&request.args["profile_snapshot"])?;
                let selected = request.args.get("profile").and_then(Value::as_str);
                if !agent
                    .profile_choice
                    .as_ref()
                    .and_then(|choice| choice.provider_failover.as_ref())
                    .is_some_and(|remaining| {
                        remaining.iter().any(|name| Some(name.as_str()) == selected)
                    })
                {
                    return Err(Error::new(
                        "bad_state",
                        "provider switch selection is outside the frozen role",
                    ));
                }
            }
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
            let (request, provider_failure) = decode_switch(parent.args)?;
            let plan = stored_result(&parent.outcome)?;
            let launch = component(&tx, id, &plan.launch_command, 0)?
                .ok_or_else(|| Error::new("bad_state", "switch launch not admitted"))?;
            let mut events = Vec::new();
            let mut state = CommandState::Failed;
            let error;
            let mut disposal = None;
            let transfer: Result<handoff::HandoffAdmission> = if launch.state
                == CommandState::Applied
            {
                tx.execute_batch("SAVEPOINT switch_transfer")
                    .map_err(sql_error)?;
                (|| {
                    let destination = ready_destination_tx(&tx, &launch)?;
                    let handoff = request.handoff(
                        &plan.handoff_command,
                        &destination.id,
                        destination.generation,
                    );
                    if let Some(failure) = provider_failure {
                        let (agent, _) = source_tx(&tx, &request, self.hierarchy_limits, policy)?;
                        require_provider_retry(&tx, &agent, &request.task_command)?;
                        failure.require_authority(&agent.profile_snapshot)?;
                        failure.require_authority(&destination.profile_snapshot)?;
                    }
                    let transfer = if provider_failure.is_some() {
                        handoff::HandoffInput::ProviderRetry
                    } else {
                        handoff::HandoffInput::Brief
                    };
                    let change = handoff::handoff_tx(
                        &tx,
                        "owner",
                        &handoff,
                        policy,
                        now,
                        self.hierarchy_limits,
                        transfer,
                    )?;
                    tx.execute("UPDATE commands SET parent_command=?1,link_kind='switch',ordinal=1 WHERE id=?2", params![id,plan.handoff_command]).map_err(sql_error)?;
                    // The continuation rides the existing overlay: B's
                    // link records the source it continues. Plain text,
                    // never a foreign key, so deleting the source
                    // afterwards cannot fault the Store writer.
                    let created = parse_agent_id(&destination.id)?;
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
                    let organization_change = OrganizationChangedEvent {
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
                    organization_change.validate()?;
                    let mut change = change;
                    change.events.push(insert_daemon_event(
                        &tx,
                        now,
                        ORGANIZATION_CHANGED_KIND,
                        "organization",
                        None,
                        None,
                        serde_json::to_value(&organization_change).map_err(|_| {
                            Error::new("internal", "cannot encode organization event")
                        })?,
                    )?);
                    Ok(change)
                })()
            } else {
                Err(Error::new(
                    if matches!(
                        launch.state,
                        CommandState::Accepted
                            | CommandState::Queued
                            | CommandState::Dispatched
                            | CommandState::Unknown
                    ) {
                        "unknown"
                    } else {
                        "launch_not_ready"
                    },
                    "launch has no proven readiness",
                ))
            };
            let mut disposal_reason = None;
            match transfer {
                Ok(change) => {
                    tx.execute_batch("RELEASE switch_transfer")
                        .map_err(sql_error)?;
                    events.extend(change.events);
                    state = CommandState::Applied;
                    error = None;
                }
                Err(refusal) => {
                    if launch.state == CommandState::Applied {
                        // A successful rollback proves even an internal H2 failure had no transfer effect.
                        tx.execute_batch("ROLLBACK TO switch_transfer; RELEASE switch_transfer")
                            .map_err(sql_error)?;
                    }
                    if refusal.code == "unknown" {
                        state = CommandState::Unknown;
                    } else if let Some(destination) = launch_destination_tx(&tx, &launch)?
                        && switch_successor_needs_stop(&tx, &destination)?
                    {
                        if switch_successor_has_input(&tx, &destination)? {
                            disposal_reason = Some(
                                "Successor has a SEND or turn. Its session was left running."
                                    .to_owned(),
                            );
                        } else {
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
                    }
                    error = Some(refusal);
                }
            }
            let mut value = result_on(&tx, id)?;
            if let Some(error) = &error {
                set_switch_error(&mut value, error, policy);
            }
            value.state = state;
            value.disposal_reason = disposal_reason;
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
            if let Some(failure) = provider_failure {
                let kind = failure.kind;
                events.push(insert_daemon_event(
                    &tx, now, PROVIDER_SWITCH_SETTLED, "provider",
                    Some(parse_agent_id(&request.from)?), Some(request.from_generation),
                    json!({
                        "switch_command": id,
                        "task_command": request.task_command,
                        "kind": kind,
                        "continued_to": value.created.as_ref().filter(|_| state == CommandState::Applied).map(|agent| &agent.agent),
                        "profile": launch.args.get("profile"),
                        "retry_command": value.handoff.as_ref().map(|handoff| &handoff.brief_command),
                        "error": value.error,
                        "state": state,
                    }),
                )?);
            }
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
        value.routing = error.routing.clone();
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

    /// Latest exact provider switch fact for this attempt, including queueing.
    pub fn provider_switch_event(
        &self,
        agent: &str,
        generation: i64,
        task: Option<&str>,
    ) -> Result<Option<Event>> {
        self.ensure_committed_reads()?;
        let row = self
            .connection
            .query_row(
                "SELECT seq, ts, kind, agent, generation, source, native_id, data, subject, cause
             FROM events WHERE agent=?1 AND generation=?2 AND kind IN (?3,?4)
             AND (?5 IS NULL OR json_extract(data,'$.task_command')=?5)
             ORDER BY seq DESC LIMIT 1",
                params![
                    parse_agent_id(agent)?,
                    generation,
                    PROVIDER_SWITCH_QUEUED,
                    PROVIDER_SWITCH_SETTLED,
                    task
                ],
                event_record_from_row,
            )
            .optional()
            .map_err(sql_error)?;
        row.map(event_from_record).transpose()
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
    let destination = launch_destination_tx(tx, &launch)?;
    if destination.as_ref().is_none_or(|destination| {
        destination.id != agent.id || destination.generation != agent.generation
    }) {
        return Err(Error::new(
            "stale_generation",
            "switch successor generation moved",
        ));
    }
    if switch_successor_has_input(tx, agent)? {
        return Err(Error::new(
            "bad_state",
            "Successor has a SEND or turn. Its session was left running.",
        ));
    }
    Ok(())
}

pub(super) fn refresh_disposal_parent_tx(
    tx: &Transaction<'_>,
    command_id: &str,
    state: CommandState,
    now: i64,
) -> Result<()> {
    tx.execute(
        "UPDATE commands SET outcome=json_set(outcome,'$.disposal_state',?2,'$.disposal_reason',
             (SELECT json_extract(outcome,'$.message') FROM commands WHERE id=?1)), updated_at=?3
         WHERE id=(SELECT parent_command FROM commands WHERE id=?1 AND link_kind='switch' AND ordinal=2 AND verb='stop')
           AND method='swarm.switch' AND state='failed' AND json_extract(outcome,'$.disposal_command')=?1",
        params![command_id, command_state_name(state), now],
    ).map_err(sql_error)?;
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

fn launch_destination_tx(tx: &Connection, launch: &Command) -> Result<Option<Agent>> {
    let (Some(target), Some(1)) = (launch.target.as_deref(), launch.generation) else {
        return Ok(None);
    };
    Ok(load_agent_by_num(tx, parse_agent_id(target)?)?
        .filter(|agent| launch.verb == "launch" && agent.generation == 1 && !agent.observe_only))
}

fn switch_successor_needs_stop(tx: &Connection, agent: &Agent) -> Result<bool> {
    if !agent.is_terminal() {
        return Ok(true);
    }
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM processes WHERE owner=?1 AND state IN ('running','unknown'))",
        [format!("agent:{}#{}", agent.id, agent.generation)],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn switch_successor_has_input(tx: &Connection, agent: &Agent) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM commands WHERE target=?1 AND generation=?2 AND verb='send')
          OR EXISTS(SELECT 1 FROM turns WHERE agent=?1 AND generation=?2)",
        params![parse_agent_id(&agent.id)?, agent.generation],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

pub(super) fn require_switch_successor_accepting_input(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
) -> Result<()> {
    let disposing: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM commands WHERE target=?1 AND generation=?2
           AND link_kind='switch' AND ordinal=2 AND verb='stop'
           AND state IN ('dispatched','unknown'))",
            params![agent, generation],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if disposing {
        return Err(Error::new(
            "busy",
            "Switch successor stop is already in flight.",
        ));
    }
    Ok(())
}

fn ready_destination_tx(tx: &Connection, launch: &Command) -> Result<Agent> {
    let destination = launch_destination_tx(tx, launch)?.ok_or_else(|| {
        Error::new(
            "stale_generation",
            "switch launch generation is unavailable",
        )
    })?;
    let agent = &destination.id;
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
    if switch_successor_has_input(tx, &destination)? {
        return Err(Error::new(
            "bad_state",
            "switch destination already has input",
        ));
    }
    Ok(destination)
}
