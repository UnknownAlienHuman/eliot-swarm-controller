//! Bounded, explicit replay over retained rule history.

use super::*;
use std::collections::{HashMap, HashSet};

struct PlannedAction {
    event_seq: i64,
    child_id: String,
    request: SubmitCommand,
    cause: core::EventCause,
}

struct CatchUpParentInput<'a> {
    principal: &'a str,
    request: &'a core::api::RuleCatchUpRequest,
    state: &'a core::api::RuleStateView,
    through_seq: i64,
    children: Vec<String>,
    suppressed: Vec<i64>,
    payload_digest: &'a [u8],
    now: i64,
}

impl Store {
    /// Look up a retained catch-up request before the Router evaluates current
    /// project, rule revision, range-capture, or rendered-action gates.
    pub fn replay_rule_catch_up(
        &self,
        principal: &str,
        request: &core::api::RuleCatchUpRequest,
    ) -> Result<Option<RuleCatchUpAdmission>> {
        require_nonempty(principal, "principal")?;
        request.validate()?;
        validate_ulid(&request.id)?;
        replay_rule_catch_up_tx(&self.connection, principal, request)
    }

    /// Admit one owner-requested, finite catch-up range. `captured_through_seq`
    /// is the boundary the daemon used while reading and rendering the range;
    /// the original optional request bound remains unchanged in the mutation ID
    /// digest. The live event cursor is never rewound or advanced here.
    pub fn admit_rule_catch_up(
        &mut self,
        principal: &str,
        request: &core::api::RuleCatchUpRequest,
        state: &core::api::RuleStateView,
        captured_through_seq: i64,
        actions: &[RuleCatchUpAction],
        now: i64,
    ) -> Result<RuleCatchUpAdmission> {
        self.ensure_writer_healthy()?;
        let result = self.admit_rule_catch_up_inner(
            principal,
            request,
            state,
            captured_through_seq,
            actions,
            now,
        );
        self.fault_on_persistence_error(&result);
        result
    }

    fn admit_rule_catch_up_inner(
        &mut self,
        principal: &str,
        request: &core::api::RuleCatchUpRequest,
        state: &core::api::RuleStateView,
        captured_through_seq: i64,
        actions: &[RuleCatchUpAction],
        now: i64,
    ) -> Result<RuleCatchUpAdmission> {
        require_nonempty(principal, "principal")?;
        request.validate()?;
        validate_ulid(&request.id)?;
        let payload_digest = rule_catch_up_digest(request)?;

        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if let Some(admission) = replay_rule_catch_up_tx(&tx, principal, request)? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(admission);
        }
        if request
            .through_seq
            .is_some_and(|requested| requested != captured_through_seq)
        {
            return Err(Error::new(
                "invalid",
                "captured rule history boundary differs from the request",
            ));
        }
        let encoded_actions = serde_json::to_vec(actions)
            .map_err(|_| Error::new("invalid", "rule catch-up actions cannot be encoded"))?;
        if encoded_actions.len() > MAX_PAGE_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "rule catch-up actions exceed the bounded request size",
            ));
        }

        let project = validate_rule_principal_tx(&tx, principal)?;
        if request.project.as_deref() != project.as_ref().map(|view| view.id.as_str())
            || state.project.as_deref() != request.project.as_deref()
        {
            return Err(Error::new(
                "not_allowed",
                "rule catch-up project does not match its principal",
            ));
        }
        state.validate()?;
        let stored = load_rule_state(&tx, principal, &request.rule)?
            .ok_or_else(|| Error::new("not_found", "rule state was not found"))?;
        if state.id != request.rule
            || state.project != stored.view.project
            || state.project.as_deref() != request.project.as_deref()
            || state.revision != stored.view.revision
        {
            return Err(Error::new(
                "bad_state",
                "rule catch-up principal, project, or revision is stale",
            ));
        }
        if !stored.view.enabled || stored.view.paused_reason.is_some() {
            return Err(Error::new(
                "bad_state",
                "rule catch-up rule is disabled or paused",
            ));
        }
        if stored.view.retiring_revision.is_some() {
            return Err(Error::new(
                "bad_state",
                "rule catch-up is unavailable while a revision is retiring",
            ));
        }
        if captured_through_seq < request.after_seq || captured_through_seq > high_water_tx(&tx)? {
            return Err(Error::new("invalid", "rule catch-up boundary is invalid"));
        }
        // Catch-up is explicit work over history the current lane already passed.
        // It never races the live scanner for the same event or rewinds its cursor.
        if captured_through_seq > stored.view.current_cursor {
            return Err(Error::new(
                "bad_state",
                "rule catch-up cannot pass the live event cursor",
            ));
        }
        let retained_through: i64 = tx
            .query_row(
                "SELECT pruned_through FROM rule_history_coverage WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?
            .ok_or_else(|| Error::new("source_integrity", "rule history coverage is absent"))?;
        if request.after_seq < retained_through {
            return Err(Error::new(
                "history_gap",
                "requested rule history is no longer fully retained",
            ));
        }
        if write_exists(&tx, &request.id)? {
            return Err(Error::new(
                "id_conflict",
                "rule catch-up ID is already used",
            ));
        }
        validate_id_age(&request.id, now)?;
        if !actions.is_empty() && has_open_rule_actions_tx(&tx, principal, &request.rule)? {
            return Err(Error::new(
                "bad_state",
                "an open or unknown rule child blocks catch-up actions",
            ));
        }

        let limit = rule_action_limit(&stored)?;
        let mut planned = Vec::new();
        let mut rejected = Vec::<(i64, String)>::new();
        let mut suppressed = Vec::new();
        let mut seen = HashSet::with_capacity(actions.len());
        let mut scoped_added = HashMap::<String, i64>::new();
        let mut global_added = 0_i64;
        let mut previous_seq = request.after_seq;

        for action in actions {
            if action.event_seq <= request.after_seq
                || action.event_seq > captured_through_seq
                || action.event_seq <= previous_seq
                || !seen.insert(action.event_seq)
            {
                return Err(Error::new(
                    "invalid",
                    "rule catch-up actions must be unique and ordered within the requested range",
                ));
            }
            previous_seq = action.event_seq;
            let trigger = load_rule_trigger(&tx, action.event_seq).map_err(|error| {
                if error.code == "not_found" {
                    Error::new("history_gap", "a matched rule event is no longer retained")
                } else {
                    error
                }
            })?;
            let depth = next_rule_cause_depth(trigger.cause.as_ref());
            if depth > 5 {
                suppressed.push(action.event_seq);
                continue;
            }
            if let Some(code) = rule_action_binding_error(&trigger, &action.request) {
                rejected.push((action.event_seq, code));
                continue;
            }
            if should_suppress_own_event_send_tx(
                &tx,
                principal,
                &request.rule,
                &stored.canonical_definition,
                &action.request,
                trigger.cause.as_ref(),
            )? {
                suppressed.push(action.event_seq);
                continue;
            }
            if let Err(error) = validate_project_rule_action_tx(&tx, principal, &action.request) {
                if deterministic_rule_rejection(&error) {
                    rejected.push((action.event_seq, error.code));
                    continue;
                }
                return Err(error);
            }
            let target = action.request.target.as_deref().ok_or_else(|| {
                Error::new("invalid", "rule catch-up action has no frozen Agent target")
            })?;
            let (scoped_count, global_count) =
                rule_action_counts(&tx, principal, &request.rule, Some(target), &limit, now)?;
            let bucket = match limit.per {
                core::api::RuleLimitScope::Rule => String::new(),
                core::api::RuleLimitScope::Agent => target.to_owned(),
            };
            let scoped_total =
                scoped_count.saturating_add(*scoped_added.get(&bucket).unwrap_or(&0));
            if rule_limit_reached(scoped_total, limit.max)
                || global_count.saturating_add(global_added) >= GLOBAL_RULE_ACTION_LIMIT
            {
                rejected.push((action.event_seq, "rate_limited".to_owned()));
                continue;
            }

            let child_id = derived_rule_id(
                "action-0",
                principal,
                &request.rule,
                state.revision,
                action.event_seq,
            )?;
            if load_command(&tx, &child_id)?.is_some() {
                return Err(Error::new(
                    "bad_state",
                    "a deterministic rule action for this event already exists",
                ));
            }
            let mut child_request = action.request.clone();
            child_request.id = child_id.clone();
            let cause = core::EventCause {
                command_id: child_id.clone(),
                rule: Some(request.rule.clone()),
                depth,
            };
            planned.push(PlannedAction {
                event_seq: action.event_seq,
                child_id,
                request: child_request,
                cause,
            });
            *scoped_added.entry(bucket).or_default() += 1;
            global_added += 1;
        }

        let child_ids = planned
            .iter()
            .map(|action| action.child_id.clone())
            .collect::<Vec<_>>();
        let (parent, mut parent_event) = insert_catch_up_parent(
            &tx,
            CatchUpParentInput {
                principal,
                request,
                state,
                through_seq: captured_through_seq,
                children: child_ids,
                suppressed: suppressed.clone(),
                payload_digest: &payload_digest,
                now,
            },
        )?;
        let parent_cause = core::EventCause {
            command_id: parent.id.clone(),
            rule: Some(request.rule.clone()),
            depth: 1,
        };
        annotate_new_command_cause_tx(&tx, &parent.id, &mut parent_event, parent_cause.clone())?;

        let mut children = Vec::with_capacity(planned.len());
        let mut events = Vec::new();
        let mut fired = Vec::with_capacity(planned.len());
        for action in planned {
            let mut admission = submit_tx(&tx, principal, action.request, None, now, true)?;
            if !admission.is_new {
                return Err(Error::new(
                    "source_integrity",
                    "new rule catch-up child already exists",
                ));
            }
            let child = admission.command;
            annotate_rule_command_event(
                &tx,
                &child.id,
                &mut admission.events,
                action.cause.clone(),
            )?;
            let linked = tx
                .execute(
                    "UPDATE commands SET parent_command = ?1, link_kind = 'rule_action', ordinal = ?2
                     WHERE id = ?3 AND parent_command IS NULL
                       AND link_kind IS NULL AND ordinal IS NULL",
                    params![parent.id, children.len() as i64, child.id],
                )
                .map_err(sql_error)?;
            if linked != 1 {
                return Err(Error::new(
                    "source_integrity",
                    "catch-up child could not be bound to its request parent",
                ));
            }
            events.extend(admission.events);
            fired.push((action.event_seq, child.id.clone(), action.cause));
            children.push(child);
        }
        events.push(parent_event);
        let mut markers = Vec::with_capacity(fired.len() + rejected.len());
        for (event_seq, child_id, cause) in fired {
            markers.push((
                event_seq,
                "rule.fired",
                json!({
                    "rule": request.rule,
                    "project": request.project,
                    "revision": state.revision,
                    "event_seq": event_seq,
                    "parent_command": parent.id,
                    "child_command": child_id,
                }),
                cause,
            ));
        }
        for (event_seq, code) in rejected {
            markers.push((
                event_seq,
                "rule.rejected",
                json!({
                    "rule": request.rule,
                    "project": request.project,
                    "revision": state.revision,
                    "event_seq": event_seq,
                    "parent_command": parent.id,
                    "code": code,
                }),
                parent_cause.clone(),
            ));
        }
        markers.sort_by_key(|(event_seq, _, _, _)| *event_seq);
        for (_, kind, data, cause) in markers {
            events.push(insert_rule_event_with_cause(
                &tx,
                now,
                kind,
                data,
                Some(cause),
            )?);
        }
        ensure_full_catch_up_result(&children)?;
        ensure_full_catch_up_result(&events)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(RuleCatchUpAdmission {
            parent,
            children,
            events,
            is_new: true,
        })
    }
}

fn rule_catch_up_digest(request: &core::api::RuleCatchUpRequest) -> Result<Vec<u8>> {
    let mut args = json!({
        "rule": request.rule,
        "after_seq": request.after_seq,
    });
    if let Some(project) = &request.project {
        args["project"] = json!(project);
    }
    if let Some(through_seq) = request.through_seq {
        args["through_seq"] = json!(through_seq);
    }
    canonical::digest(&json!({
        "target": null,
        "verb": "rule.catch_up",
        "args": args,
        "expected_generation": null,
        "expected_turn_id": null,
    }))
}

fn insert_catch_up_parent(
    tx: &Transaction<'_>,
    input: CatchUpParentInput<'_>,
) -> Result<(Command, Event)> {
    let CatchUpParentInput {
        principal,
        request,
        state,
        through_seq,
        children,
        suppressed,
        payload_digest,
        now,
    } = input;
    if write_exists(tx, &request.id)? {
        return Err(Error::new(
            "id_conflict",
            "rule catch-up ID is already used",
        ));
    }
    let mut args = json!({
        "rule": request.rule,
        "after_seq": request.after_seq,
        "through_seq": through_seq,
        "revision": state.revision,
    });
    if let Some(project) = &request.project {
        args["project"] = json!(project);
    }
    let outcome = json!({
        "children": children,
        "through_seq": through_seq,
        "suppressed_event_seqs": suppressed,
    });
    let command = CommandInsert {
        id: &request.id,
        principal,
        method: COMMAND_METHOD,
        payload_digest,
        target: None,
        expected_generation: None,
        generation: None,
        expected_turn_id: None,
        parent_command: None,
        link_kind: None,
        ordinal: None,
        turn_id: None,
        request_id: None,
        probe: false,
        verb: "rule.catch_up",
        args: &args,
        state: CommandState::Applied,
        outcome: Some(&outcome),
        created_at: now,
        updated_at: now,
    };
    let mut event_data = args.clone();
    event_data["command_id"] = json!(request.id);
    event_data["children"] = json!(outcome["children"]);
    let event = admit_command(tx, &command, event_data)?;
    Ok((
        Command {
            id: request.id.clone(),
            principal: principal.to_owned(),
            method: COMMAND_METHOD.to_owned(),
            verb: "rule.catch_up".to_owned(),
            target: None,
            expected_generation: None,
            generation: None,
            args,
            state: CommandState::Applied,
            turn_id: None,
            request_id: None,
            outcome: Some(outcome),
            created_at: now,
            updated_at: now,
            probe: false,
        },
        event,
    ))
}

fn replay_rule_catch_up_tx(
    connection: &Connection,
    principal: &str,
    request: &core::api::RuleCatchUpRequest,
) -> Result<Option<RuleCatchUpAdmission>> {
    let digest = rule_catch_up_digest(request)?;
    let Some((old_principal, old_method, old_digest)) = command_binding(connection, &request.id)?
    else {
        return Ok(None);
    };
    if old_principal != principal || old_method != COMMAND_METHOD || old_digest != digest {
        return Err(Error::new(
            "id_conflict",
            "rule catch-up ID is already bound to another request",
        ));
    }
    let parent = load_command(connection, &request.id)?
        .ok_or_else(|| Error::new("internal", "rule catch-up binding has no command row"))?;
    request.validate_response(&parent)?;
    let children = load_catch_up_children(connection, &parent.id)?;
    let expected = parent
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.get("children"))
        .and_then(Value::as_array)
        .ok_or_else(|| Error::new("source_integrity", "catch-up parent has no fixed child set"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::new("source_integrity", "catch-up child ID is invalid"))
        })
        .collect::<Result<Vec<_>>>()?;
    if children
        .iter()
        .map(|child| child.id.clone())
        .collect::<Vec<_>>()
        != expected
    {
        return Err(Error::new(
            "source_integrity",
            "catch-up parent child rows do not match its fixed child set",
        ));
    }
    Ok(Some(RuleCatchUpAdmission {
        parent,
        children,
        events: Vec::new(),
        is_new: false,
    }))
}

fn load_catch_up_children(connection: &Connection, parent_id: &str) -> Result<Vec<Command>> {
    let mut statement = connection
        .prepare(
            "SELECT id, principal, method, target, expected_generation, generation,
                    expected_turn_id, turn_id, request_id, verb, args, state, outcome,
                    created_at, updated_at, probe
             FROM commands
             WHERE parent_command = ?1 AND link_kind = 'rule_action' AND ordinal IS NOT NULL
             ORDER BY ordinal",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([parent_id], command_record_from_row)
        .map_err(sql_error)?;
    let mut children = Vec::new();
    for row in rows {
        children.push(command_from_raw(row.map_err(sql_error)?)?.command);
    }
    ensure_full_catch_up_result(&children)?;
    Ok(children)
}

fn ensure_full_catch_up_result<T: serde::Serialize>(items: &[T]) -> Result<()> {
    if items.len() > MAX_PAGE_ENTRIES {
        return Err(Error::new(
            "frame_too_large",
            "rule catch-up result exceeds the bounded entry count",
        ));
    }
    let encoded = serde_json::to_vec(items)
        .map_err(|_| Error::new("internal", "rule catch-up result cannot be encoded"))?;
    if encoded.len() > MAX_PAGE_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "rule catch-up result exceeds the bounded response size",
        ));
    }
    Ok(())
}
