//! Bounded, explicit replay over retained rule history.

use super::*;
use eliot_core::is_start_verb;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct RuleCatchUpAction {
    pub event_seq: i64,
    pub outcome: RuleCatchUpActionOutcome,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub enum RuleCatchUpActionOutcome {
    Command(SubmitCommand),
    Rejected {
        code: String,
        message: String,
    },
    SkippedCauseDepth,
    RejectedWithCapacity {
        code: String,
        message: String,
        failure_where: String,
    },
}

impl RuleCatchUpAction {
    pub fn command(event_seq: i64, request: SubmitCommand) -> Self {
        Self {
            event_seq,
            outcome: RuleCatchUpActionOutcome::Command(request),
        }
    }

    pub fn rejected(event_seq: i64, code: impl Into<String>) -> Self {
        let code = code.into();
        Self {
            event_seq,
            outcome: RuleCatchUpActionOutcome::Rejected {
                message: code.clone(),
                code,
            },
        }
    }

    pub fn rejected_with_message(
        event_seq: i64,
        code: impl Into<String>,
        message: impl AsRef<str>,
    ) -> Self {
        let code = code.into();
        Self {
            event_seq,
            outcome: RuleCatchUpActionOutcome::Rejected {
                message: super::bounded_rule_rejection_message(message.as_ref(), &code),
                code,
            },
        }
    }

    pub fn skipped_cause_depth(event_seq: i64) -> Self {
        Self {
            event_seq,
            outcome: RuleCatchUpActionOutcome::SkippedCauseDepth,
        }
    }

    pub fn rejected_with_capacity(event_seq: i64, error: &Error) -> Self {
        Self {
            event_seq,
            outcome: RuleCatchUpActionOutcome::RejectedWithCapacity {
                code: error.code.clone(),
                message: super::bounded_rule_rejection_message(error.message.as_ref(), &error.code),
                failure_where: error
                    .failure_where
                    .as_deref()
                    .unwrap_or("store.admission.capacity")
                    .to_owned(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuleCatchUpAdmission {
    pub parent: Command,
    pub children: Vec<Admission>,
    pub events: Vec<Event>,
    pub is_new: bool,
}

struct PlannedAction {
    event_seq: i64,
    child_id: String,
    admission: Admission,
    events: Vec<Event>,
    cause: core::EventCause,
}

struct SkippedRuleAction {
    event_seq: i64,
    reason: &'static str,
    cause_depth: u8,
    self_cause: bool,
}

struct CatchUpParentInput<'a> {
    principal: &'a str,
    request: &'a core::api::RuleCatchUpRequest,
    state: &'a core::api::RuleStateView,
    through_seq: i64,
    children: Vec<String>,
    suppressed: Vec<i64>,
    rate_limit: core::api::RuleLimit,
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
        if !actions.is_empty() && has_open_rule_actions(&tx, principal, &request.rule)? {
            return Err(Error::new(
                "bad_state",
                "an open or unknown rule child blocks catch-up actions",
            ));
        }

        let limit = rule_action_limit(&stored)?;
        let mut planned = Vec::new();
        let mut rejected = Vec::<(i64, Error)>::new();
        let mut suppressed = Vec::new();
        let mut skipped_markers = Vec::<SkippedRuleAction>::new();
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
                if !trigger.kind.starts_with("rule.") {
                    skipped_markers.push(SkippedRuleAction {
                        event_seq: action.event_seq,
                        reason: "cause_depth",
                        cause_depth: depth,
                        self_cause: rule_event_originates_from_rule_tx(
                            &tx,
                            principal,
                            &request.rule,
                            trigger.cause.as_ref(),
                        )?,
                    });
                }
                continue;
            }
            let action_request = match &action.outcome {
                RuleCatchUpActionOutcome::Command(request) => request,
                RuleCatchUpActionOutcome::SkippedCauseDepth => {
                    return Err(Error::new(
                        "invalid",
                        "catch-up action is not over the cause-depth limit",
                    ));
                }
                RuleCatchUpActionOutcome::Rejected { code, message } => {
                    validate_rule_rejection_code(code)?;
                    if !core::is_deterministic_pre_admission_rejection(code) {
                        return Err(Error::new(
                            "invalid",
                            "rule catch-up rejection code is not deterministic",
                        ));
                    }
                    rejected.push((
                        action.event_seq,
                        Error::new(code.clone(), bounded_rule_rejection_message(message, code)),
                    ));
                    continue;
                }
                RuleCatchUpActionOutcome::RejectedWithCapacity {
                    code,
                    message,
                    failure_where,
                } => {
                    validate_rule_rejection_code(code)?;
                    if code != "capacity_unavailable" || failure_where != "store.admission.capacity"
                    {
                        return Err(Error::new(
                            "invalid",
                            "rule catch-up capacity rejection metadata is invalid",
                        ));
                    }
                    rejected.push((
                        action.event_seq,
                        Error::new(code.clone(), bounded_rule_rejection_message(message, code))
                            .with_where(failure_where.clone()),
                    ));
                    continue;
                }
            };
            let binding_error = if action_request.verb == "run" {
                rule_run_action_binding_error(
                    &tx,
                    principal,
                    &request.rule,
                    state.revision,
                    action.event_seq,
                    action_request,
                )?
            } else {
                rule_action_binding_error(&trigger, action.event_seq, action_request)
            };
            if let Some(code) = binding_error {
                rejected.push((action.event_seq, Error::new(code.clone(), code)));
                continue;
            }
            if should_suppress_own_event_send_or_run_tx(
                &tx,
                principal,
                &request.rule,
                &stored.canonical_definition,
                action_request,
                trigger.cause.as_ref(),
            )? {
                suppressed.push(action.event_seq);
                if !trigger.kind.starts_with("rule.") {
                    skipped_markers.push(SkippedRuleAction {
                        event_seq: action.event_seq,
                        reason: "self_cause",
                        cause_depth: depth,
                        self_cause: true,
                    });
                }
                continue;
            }
            if let Err(error) = validate_project_rule_action_tx(&tx, principal, action_request) {
                if core::is_deterministic_pre_admission_rejection(&error.code) {
                    rejected.push((action.event_seq, error));
                    continue;
                }
                return Err(error);
            }
            let target = action_request.target.as_deref();
            let (scoped_count, global_count) =
                rule_action_counts(&tx, principal, &request.rule, target, &limit, now)?;
            let bucket = match limit.per {
                core::api::RuleLimitScope::Rule => Some(String::new()),
                core::api::RuleLimitScope::Agent => target.map(str::to_owned),
            };
            let scoped_total = bucket.as_ref().map_or(0, |bucket| {
                scoped_count.saturating_add(*scoped_added.get(bucket).unwrap_or(&0))
            });
            if rule_limit_reached(scoped_total, limit.max)
                || global_count.saturating_add(global_added) >= GLOBAL_RULE_ACTION_LIMIT
            {
                rejected.push((
                    action.event_seq,
                    Error::new("rate_limited", "rule action rate limit reached"),
                ));
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
            let mut child_request = action_request.clone();
            child_request.id = child_id.clone();
            let cause = core::EventCause {
                command_id: child_id.clone(),
                rule: Some(request.rule.clone()),
                remote_call: trigger
                    .cause
                    .as_ref()
                    .and_then(|cause| cause.remote_call.clone()),
                depth,
            };
            let preparation = super::rules::rule_launch_preparation(&child_request)?;
            tx.execute_batch("SAVEPOINT rule_catch_up_action")
                .map_err(sql_error)?;
            let limits = self.hierarchy_limits;
            let policy = self.capacity_policy.as_deref();
            let mut admission = match admission::submit_tx(
                &tx,
                principal,
                child_request,
                preparation,
                now,
                true,
                limits,
                policy,
            ) {
                Ok(admission) if admission.is_new => admission,
                Ok(_) => {
                    return Err(Error::new(
                        "source_integrity",
                        "new rule catch-up child already exists",
                    ));
                }
                Err(error) if core::is_deterministic_pre_admission_rejection(&error.code) => {
                    tx.execute_batch(
                        "ROLLBACK TO rule_catch_up_action; RELEASE rule_catch_up_action",
                    )
                    .map_err(sql_error)?;
                    rejected.push((action.event_seq, error));
                    continue;
                }
                Err(error) => return Err(error),
            };
            annotate_rule_command_event(&tx, &child_id, &mut admission.events, cause.clone())?;
            if let Some(linked_command) = &admission.linked_command {
                annotate_rule_command_event(
                    &tx,
                    &linked_command.id,
                    &mut admission.events,
                    core::EventCause {
                        command_id: linked_command.id.clone(),
                        rule: cause.rule.clone(),
                        remote_call: cause.remote_call.clone(),
                        depth: cause.depth,
                    },
                )?;
            }
            tx.execute_batch("RELEASE rule_catch_up_action")
                .map_err(sql_error)?;
            let action_events = std::mem::take(&mut admission.events);
            planned.push(PlannedAction {
                event_seq: action.event_seq,
                child_id,
                admission,
                events: action_events,
                cause,
            });
            if let Some(bucket) = bucket {
                *scoped_added.entry(bucket).or_default() += 1;
            }
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
                rate_limit: limit,
                payload_digest: &payload_digest,
                now,
            },
        )?;
        let parent_cause = core::EventCause {
            command_id: parent.id.clone(),
            rule: Some(request.rule.clone()),
            remote_call: None,
            depth: 1,
        };
        annotate_new_command_cause_tx(&tx, &parent.id, &mut parent_event, parent_cause.clone())?;

        let mut children = Vec::with_capacity(planned.len());
        let mut events = Vec::new();
        let mut fired = Vec::with_capacity(planned.len());
        for action in planned {
            let PlannedAction {
                event_seq,
                admission,
                events: mut action_events,
                cause,
                ..
            } = action;
            let child_id = admission.command.id.clone();
            let linked = tx
                .execute(
                    "UPDATE commands SET parent_command = ?1, link_kind = 'rule_action', ordinal = ?2
                     WHERE id = ?3 AND parent_command IS NULL
                       AND link_kind IS NULL AND ordinal IS NULL",
                    params![parent.id, children.len() as i64, child_id],
                )
                .map_err(sql_error)?;
            if linked != 1 {
                return Err(Error::new(
                    "source_integrity",
                    "catch-up child could not be bound to its request parent",
                ));
            }
            events.append(&mut action_events);
            fired.push((event_seq, child_id, cause));
            children.push(admission);
        }
        events.push(parent_event);
        let mut markers = Vec::with_capacity(fired.len() + rejected.len() + skipped_markers.len());
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
        for (event_seq, error) in rejected {
            let is_capacity = error.code == "capacity_unavailable"
                && error.failure_where.as_deref() == Some("store.admission.capacity");
            let mut data = json!({
                "rule": request.rule,
                "project": request.project,
                "revision": state.revision,
                "event_seq": event_seq,
                "parent_command": parent.id,
                "code": error.code,
                "message": bounded_rule_rejection_message(&error.message, &error.code),
            });
            if is_capacity {
                data["where"] = json!("store.admission.capacity");
            }
            markers.push((event_seq, "rule.rejected", data, parent_cause.clone()));
        }
        for skipped in skipped_markers {
            markers.push((
                skipped.event_seq,
                "rule.skipped",
                json!({
                    "rule": request.rule,
                    "project": request.project,
                    "revision": state.revision,
                    "event_seq": skipped.event_seq,
                    "parent_command": parent.id,
                    "reason": skipped.reason,
                    "self_cause": skipped.self_cause,
                    "cause_depth": skipped.cause_depth,
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
        super::ensure_admission_page(&children)?;
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
        rate_limit,
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
    // Freeze the admission rate policy in the committed catch-up parent:
    // catch-up Run output validates against this saved policy plus current
    // authority, without requiring the definition revision to remain retained.
    let rate_limit = serde_json::to_value(rate_limit)
        .map_err(|_| Error::new("internal", "rule rate policy cannot be encoded"))?;
    let outcome = json!({
        "children": children,
        "through_seq": through_seq,
        "suppressed_event_seqs": suppressed,
        "rate_limit": rate_limit,
    });
    let command = CommandInsert {
        id: &request.id,
        principal,
        method: COMMAND_METHOD,
        payload_digest,
        target: None,
        selector: None,
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
            selector: None,
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
            config_revision: 0,
            config: None,
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
        .map(|child| child.command.id.clone())
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

fn load_catch_up_children(connection: &Connection, parent_id: &str) -> Result<Vec<Admission>> {
    let mut statement = connection
        .prepare(
            "SELECT id, principal, method, target, expected_generation, generation,
                    expected_turn_id, turn_id, request_id, verb, args, state, outcome,
                    created_at, updated_at, probe, selector, config_revision
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
        let command = command_from_raw(row.map_err(sql_error)?)?.command;
        children.push(super::retained_action_admission(connection, command)?);
    }
    super::ensure_admission_page(&children)?;
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

fn resume_workspace_stop_pending_tx(
    tx: &Transaction<'_>,
    resume_id: &str,
    target: i64,
    old_generation: i64,
) -> Result<bool> {
    let Some(workspace) = super::super::workspace::load_workspace(tx, target)? else {
        return Ok(false);
    };
    if workspace.start_command_id != resume_id
        || !super::super::workspace::workspace_cancel_requested(&workspace)
    {
        return Ok(false);
    }
    let next_generation = old_generation
        .checked_add(1)
        .ok_or_else(|| Error::new("source_integrity", "resume generation overflow"))?;
    let stop_id = workspace
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.get("stop_command_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("source_integrity", "workspace cancel marker has no Stop"))?;
    let stop = load_command_record(tx, stop_id)?
        .ok_or_else(|| Error::new("source_integrity", "workspace cancel Stop disappeared"))?;
    if workspace.target_generation != next_generation
        || !matches!(
            workspace.phase,
            WorkspacePhase::Creating | WorkspacePhase::Ready
        )
        || stop.command.verb != "stop"
        || stop.target != Some(target)
        || stop.command.generation != Some(old_generation)
        || stop.expected_turn_id.is_some()
        || !matches!(stop.state, CommandState::Dispatched | CommandState::Unknown)
    {
        return Err(Error::new(
            "source_integrity",
            "workspace cancel marker does not retain its exact pending Stop",
        ));
    }
    Ok(true)
}

/// Gate a rule-action child at the Store dispatch boundary. A held successor is
/// durably queued; a failed predecessor cancels every later undispatched sibling.
pub(crate) fn gate_rule_action_dispatch_tx(
    tx: &Transaction<'_>,
    command_id: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<RuleActionDispatchGate> {
    let link: Option<(Option<String>, Option<String>, Option<i64>)> = tx
        .query_row(
            "SELECT parent_command, link_kind, ordinal FROM commands WHERE id = ?1",
            [command_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let Some((parent_id, link_kind, ordinal)) = link else {
        return Err(Error::new(
            "not_found",
            "rule-action dispatch command was not found",
        ));
    };
    if link_kind.as_deref() != Some("rule_action") {
        return Ok(RuleActionDispatchGate::Ready);
    }
    let (Some(parent_id), Some(ordinal)) = (parent_id, ordinal) else {
        return Err(Error::new(
            "source_integrity",
            "rule-action child has incomplete parent ordering metadata",
        ));
    };
    if ordinal < 0 {
        return Err(Error::new(
            "source_integrity",
            "rule-action ordinal is negative",
        ));
    }
    let parent = load_command_record(tx, &parent_id)?
        .ok_or_else(|| Error::new("source_integrity", "rule-action parent is missing"))?;
    if !matches!(
        parent.command.verb.as_str(),
        "rule" | "timer.run" | "rule.catch_up" | "run"
    ) {
        return Err(Error::new(
            "source_integrity",
            "rule-action parent has an unsupported verb",
        ));
    }
    if parent.command.verb == "run" && parent.command.state != CommandState::Applied {
        return Err(Error::new(
            "source_integrity",
            "script output children cannot dispatch before the Run child set is committed",
        ));
    }
    let expected_children = parent
        .command
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.get("children"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            Error::new(
                "source_integrity",
                "rule-action parent has no fixed child set",
            )
        })?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::new("source_integrity", "rule-action child ID is invalid"))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut statement = tx
        .prepare(
            "SELECT id, principal, ordinal, state, target, generation, expected_generation
             FROM commands
             WHERE parent_command = ?1 AND link_kind = 'rule_action' AND ordinal IS NOT NULL
             ORDER BY ordinal",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([&parent_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<i64>>(6)?,
            ))
        })
        .map_err(sql_error)?;
    let sibling_rows = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    drop(statement);
    let child_ids = sibling_rows
        .iter()
        .map(|(id, _, _, _, _, _, _)| id.clone())
        .collect::<Vec<_>>();
    if child_ids != expected_children {
        return Err(Error::new(
            "source_integrity",
            "rule-action child rows differ from the parent's fixed child set",
        ));
    }
    let current_index = usize::try_from(ordinal)
        .map_err(|_| Error::new("source_integrity", "rule-action ordinal is invalid"))?;
    if current_index >= sibling_rows.len()
        || sibling_rows[current_index].0 != command_id
        || sibling_rows.iter().enumerate().any(|(index, row)| {
            i64::try_from(index).ok() != Some(row.2) || row.1 != parent.command.principal
        })
    {
        return Err(Error::new(
            "source_integrity",
            "rule-action sibling ordering is inconsistent",
        ));
    }
    if current_index == 0 {
        return Ok(RuleActionDispatchGate::Ready);
    }

    let mut failed_predecessor = None;
    for (index, row) in sibling_rows.iter().take(current_index).enumerate() {
        let predecessor_state = parse_enum::<CommandState>(&row.3, "command state")?;
        let mut predecessor_pending = matches!(
            predecessor_state,
            CommandState::Accepted
                | CommandState::Queued
                | CommandState::Dispatched
                | CommandState::Unknown
        );
        let mut predecessor_failed = matches!(
            predecessor_state,
            CommandState::Rejected | CommandState::Failed | CommandState::Cancelled
        );
        if predecessor_state == CommandState::Applied {
            let predecessor = load_command_record(tx, &row.0)?.ok_or_else(|| {
                Error::new("source_integrity", "rule-action predecessor disappeared")
            })?;
            if is_start_verb(predecessor.command.verb.as_str()) {
                let retained = retained_action_admission(tx, predecessor.command)?;
                if let Some(linked_send) = retained.linked_command {
                    match linked_send.state {
                        CommandState::Applied => {}
                        CommandState::Rejected | CommandState::Failed | CommandState::Cancelled => {
                            predecessor_failed = true
                        }
                        CommandState::Accepted
                        | CommandState::Queued
                        | CommandState::Dispatched
                        | CommandState::Unknown => predecessor_pending = true,
                    }
                }
            }
        }
        if predecessor_failed {
            failed_predecessor = Some(index);
            break;
        }
        if predecessor_pending {
            let current = load_command_record(tx, command_id)?
                .ok_or_else(|| Error::new("source_integrity", "rule-action child disappeared"))?;
            if current.state == CommandState::Accepted {
                let target = current.target;
                let generation = current
                    .command
                    .generation
                    .or(current.command.expected_generation);
                if target.is_some() != generation.is_some() {
                    return Err(Error::new(
                        "source_integrity",
                        "rule-action child has inconsistent target binding",
                    ));
                }
                update_command_state(tx, command_id, CommandState::Queued, None, None, now)?;
                events.push(insert_daemon_command_event(
                    tx,
                    now,
                    "command.queued",
                    target,
                    generation,
                    command_id,
                    json!({
                        "command_id": command_id,
                        "reason": "rule_action_predecessor_pending",
                    }),
                )?);
            }
            return Ok(RuleActionDispatchGate::Held);
        }
    }
    let Some(failed_index) = failed_predecessor else {
        return Ok(RuleActionDispatchGate::Ready);
    };
    for row in sibling_rows.iter().skip(failed_index + 1) {
        let sibling_state: CommandState = parse_enum(&row.3, "command state")?;
        if !matches!(sibling_state, CommandState::Accepted | CommandState::Queued) {
            continue;
        }
        let sibling = load_command_record(tx, &row.0)?
            .ok_or_else(|| Error::new("source_integrity", "rule-action sibling disappeared"))?;
        if sibling.command.verb != "resume" {
            continue;
        }
        let target = row.4.ok_or_else(|| {
            Error::new(
                "source_integrity",
                "undispatched rule-action child has no target",
            )
        })?;
        let generation = row.5.or(row.6).ok_or_else(|| {
            Error::new(
                "source_integrity",
                "undispatched rule-action child has no generation",
            )
        })?;
        if resume_workspace_stop_pending_tx(tx, &row.0, target, generation)? {
            let current = load_command_record(tx, command_id)?
                .ok_or_else(|| Error::new("source_integrity", "rule-action child disappeared"))?;
            if current.state == CommandState::Accepted {
                let current_target = current.target;
                let current_generation = current
                    .command
                    .generation
                    .or(current.command.expected_generation);
                if current_target.is_some() != current_generation.is_some() {
                    return Err(Error::new(
                        "source_integrity",
                        "rule-action child has inconsistent target binding",
                    ));
                }
                update_command_state(tx, command_id, CommandState::Queued, None, None, now)?;
                events.push(insert_daemon_command_event(
                    tx,
                    now,
                    "command.queued",
                    current_target,
                    current_generation,
                    command_id,
                    json!({
                        "command_id": command_id,
                        "reason": "workspace_stop_pending",
                    }),
                )?);
            }
            return Ok(RuleActionDispatchGate::Held);
        }
    }
    for row in sibling_rows.iter().skip(failed_index + 1) {
        let sibling_state: CommandState = parse_enum(&row.3, "command state")?;
        if !matches!(sibling_state, CommandState::Accepted | CommandState::Queued) {
            continue;
        }
        let sibling = load_command_record(tx, &row.0)?
            .ok_or_else(|| Error::new("source_integrity", "rule-action sibling disappeared"))?;
        if sibling.target.is_some() {
            let target = row.4.ok_or_else(|| {
                Error::new(
                    "source_integrity",
                    "undispatched rule-action child has no target",
                )
            })?;
            let generation = row.5.or(row.6).ok_or_else(|| {
                Error::new(
                    "source_integrity",
                    "undispatched rule-action child has no generation",
                )
            })?;
            cancel_pending_command_tx(
                tx,
                target,
                generation,
                &row.0,
                "rule_action_predecessor_terminal",
                now,
                events,
            )?;
            if sibling.command.verb == "launch" {
                super::super::capacity::release_unstarted_tx(tx, target, 1, &row.0)?;
                super::super::fail_unstarted_launch_tx(tx, target, &row.0, now, events)?;
            } else if sibling.command.verb == "resume" {
                super::super::lifecycle::cancel_unstarted_resume_input_tx(
                    tx,
                    target,
                    &row.0,
                    "rule_action_predecessor_terminal",
                    now,
                    events,
                    false,
                )?;
            }
            continue;
        }
        if row.4.is_some() || row.5.is_some() || row.6.is_some() {
            return Err(Error::new(
                "source_integrity",
                "targetless rule-action child has an Agent binding",
            ));
        }
        let outcome = json!({"reason": "rule_action_predecessor_terminal"});
        update_command_state(
            tx,
            &row.0,
            CommandState::Cancelled,
            Some(&outcome),
            None,
            now,
        )?;
        events.push(insert_daemon_command_event(
            tx,
            now,
            "command.cancelled",
            None,
            None,
            &row.0,
            json!({
                "command_id": row.0,
                "reason": "rule_action_predecessor_terminal",
            }),
        )?);
    }
    Ok(RuleActionDispatchGate::Cancelled)
}
