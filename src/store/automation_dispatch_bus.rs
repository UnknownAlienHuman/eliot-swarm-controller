//! Narrow Store bridge implementing the shared `swarm-bus` metadata contract
//! over the existing ScriptRun event cursor and pending-intent journal.
//!
//! This module is included as a child of `store::automation_dispatch` so it can
//! reuse that module's validated source projections, semantic dedupe, and
//! pending-intent writer. It owns no database, queue, or second cursor.

use super::*;

const MAX_BUS_SCAN: usize = crate::automation::intake::MAX_INTAKE_PAGE;
const MAX_BUS_PAGE_ITEMS: usize = 32;

type ConsumerSubmissionOperationRow = (String, String, Option<String>, Option<String>, String);
type ConsumerTaskAttemptScopeRow = (
    String,
    String,
    i64,
    Option<String>,
    String,
    i64,
    Option<i64>,
);
type ConsumerEventOperationRow = (Option<String>, Option<String>, String, String);

enum ScriptConsumerAuthority<'a> {
    /// Compatibility/direct Store route. This remains a real authenticated
    /// Manager and retains the pre-existing current-GM check.
    Manager(&'a Principal),
    /// A real authenticated Module client with a strict persisted consumer
    /// scope. This carries the Manager ID as data, never as a Principal.
    ScopedModule(crate::store::bus_kernel::ScriptRunConsumerBinding),
}

impl ScriptConsumerAuthority<'_> {
    fn owner_manager_id(&self) -> &str {
        match self {
            Self::Manager(principal) => &principal.client_id,
            Self::ScopedModule(binding) => binding.owner_manager_id(),
        }
    }
}

/// Private scope formed only after reloading either a registered direct
/// Manager or a Module registration with an exact durable consumer grant.
/// It is never accepted from request JSON.
struct AuthenticatedScriptConsumer<'a> {
    authority: ScriptConsumerAuthority<'a>,
    entry: AutomationEntry,
}

/// Run the shared pure selector router against a Store-normalized event and
/// one already-validated enabled ScriptRun entry. Store still performs the
/// source-specific authorization before admitting an action; this only
/// centralizes source/kind/status matching with the sibling bus contract.
pub(crate) fn route_script_run_event(
    entry: &AutomationEntry,
    event: &crate::automation::intake::ObservedEvent,
    status: Option<crate::automation::event_rules::EventStatus>,
) -> Result<bool> {
    let projected_status = status
        .map(|value| {
            swarm_bus::EventStatus::parse(value.as_str())
                .map_err(|error| Error::new("BUS_SELECTOR_INVALID", error.to_string()))
        })
        .transpose()?;
    let metadata = swarm_bus::EventMetadata::new(
        event.observation_id,
        &event.source_id,
        &event.event_kind,
        projected_status,
    )
    .map_err(|error| Error::new("BUS_EVENT_METADATA_INVALID", error.to_string()))?;
    let consumer_id = format!(
        "script-{}",
        model::digest(
            model::canonical(&json!({
                "owner_manager_id":entry.owner_manager_id,
                "project_id":entry.project_id,
                "automation_id":entry.automation_id
            }))?
            .as_bytes()
        )
    );
    let mut subscriptions = Vec::new();
    for rule in entry
        .event_rules
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|rule| rule.action == crate::automation::event_rules::EventRuleAction::ScriptRun)
    {
        let Some((source_id, event_kind)) = rule.selected_source_kind() else {
            return Err(Error::new(
                "AUTOMATION_RECORD_CORRUPT",
                "ScriptRun event selector has no exact source and kind",
            ));
        };
        let selector_status = if rule.is_task_submission_applied() {
            Some(swarm_bus::EventStatus::Applied)
        } else {
            rule.status
                .map(|value| swarm_bus::EventStatus::parse(value.as_str()))
                .transpose()
                .map_err(|error| Error::new("BUS_SELECTOR_INVALID", error.to_string()))?
        };
        let selector = swarm_bus::EventSelector::new(source_id, event_kind, selector_status)
            .map_err(|error| Error::new("BUS_SELECTOR_INVALID", error.to_string()))?;
        let subscription =
            swarm_bus::Subscription::new(&consumer_id, selector, swarm_bus::Action::ScriptRun)
                .map_err(|error| Error::new("BUS_SUBSCRIPTION_INVALID", error.to_string()))?;
        subscriptions.push(subscription);
    }
    let plan = swarm_bus::route_event(&metadata, &subscriptions)
        .map_err(|error| Error::new("BUS_ROUTE_INVALID", error.to_string()))?;
    Ok(plan.items().iter().any(|item| {
        item.consumer_id() == consumer_id.as_str() && item.action() == swarm_bus::Action::ScriptRun
    }))
}

/// Digest only the ScriptRun action target and its exact selected source
/// selectors. Cosmetic entry revisions do not rotate consumer scope; changing
/// the action or source set does, requiring explicit re-enrollment.
pub(crate) fn script_run_consumer_scope_digest(entry: &AutomationEntry) -> Result<String> {
    let script_id = selected_script_id(entry)?;
    let mut selectors = entry
        .event_rules
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|rule| rule.action == crate::automation::event_rules::EventRuleAction::ScriptRun)
        .map(|rule| {
            let (source_id, event_kind) = rule.selected_source_kind().ok_or_else(|| {
                Error::new(
                    "AUTOMATION_RECORD_CORRUPT",
                    "ScriptRun event selector has no exact source and kind",
                )
            })?;
            let status = if rule.is_task_submission_applied() {
                Some(crate::automation::event_rules::EventStatus::Applied)
            } else {
                rule.status
            };
            Ok((
                source_id.to_owned(),
                event_kind.to_owned(),
                status.map(crate::automation::event_rules::EventStatus::as_str),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    selectors.sort();
    selectors.dedup();
    Ok(model::digest(
        model::canonical(&json!({
            "owner_manager_id":entry.owner_manager_id,
            "project_id":entry.project_id,
            "automation_id":entry.automation_id,
            "script_id":script_id,
            "selectors":selectors,
        }))?
        .as_bytes(),
    ))
}

/// Read safe event headers selected by one current, authenticated Manager.
/// `after_observation_id` is only a read position: this function never writes
/// either the reader position or the durable ScriptRun cursor.
pub(crate) fn events_page(
    db: &Connection,
    principal: &Principal,
    project_id: &str,
    consumer_id: &str,
    requested_after_observation_id: Option<i64>,
    limit: usize,
    app_config: &Config,
) -> Result<Value> {
    let consumer = current_script_consumer(db, principal, project_id, consumer_id)?;
    let entry = &consumer.entry;
    let state = load_script_trigger_state(db, entry)?.ok_or_else(|| {
        Error::new(
            "BUS_CONSUMER_CURSOR_MISSING",
            "selected ScriptRun consumer has no durable cursor",
        )
    })?;
    let after_observation_id = requested_after_observation_id.unwrap_or(state.cursor);
    if after_observation_id < 0 || !(1..=MAX_BUS_PAGE_ITEMS).contains(&limit) {
        return Err(Error::invalid(
            "bus page requires a nonnegative read position and a 1..=32 limit",
        ));
    }

    let high_water = automation_intake::observed_event_high_water(db)?;
    let scanned =
        automation_intake::observed_event_page(db, after_observation_id, high_water, MAX_BUS_SCAN)?;
    let mut items = Vec::with_capacity(limit);
    let mut scanned_through = after_observation_id;
    let mut stopped_before_scan_end = false;
    for (index, event) in scanned.iter().enumerate() {
        scanned_through = event.observation_id;
        if let Some(projection) =
            authorized_script_event_projection(db, &consumer, app_config, event)?
        {
            items.push(json!({
                "observation_id":event.observation_id,
                "source_id":event.source_id,
                "event_kind":event.event_kind,
                "status":projection.status.map(crate::automation::event_rules::EventStatus::as_str),
                "action":{"kind":"script_run","script_id":selected_script_id(entry)?},
            }));
            if items.len() == limit {
                stopped_before_scan_end = index + 1 < scanned.len();
                break;
            }
        }
    }
    if !stopped_before_scan_end
        && scanned.len() < MAX_BUS_SCAN
        && high_water >= after_observation_id
    {
        // The bounded query returned every observation through the current
        // high-water mark. Advancing over filtered events is necessary for a
        // global observation cursor, but the marker contains no event data.
        scanned_through = high_water;
    }
    Ok(json!({
        "consumer_id":consumer_id,
        "automation_revision":entry.revision,
        "consumer_cursor":state.cursor,
        "expected_cursor":state.cursor,
        "after_observation_id":after_observation_id,
        "items":items,
        "scanned_through":scanned_through,
        "has_more":scanned_through < high_water,
        "acknowledged":false,
    }))
}

/// Compare-and-advance the existing per-entry O1 cursor through one exact
/// occurrence. The pending ScriptRun intent and cursor update are committed
/// by the caller's ordinary Store mutation transaction. Execution/admission of
/// that intent remains the existing later ScriptRun continuation.
#[allow(clippy::too_many_arguments)]
pub(crate) fn admit_script_run_page(
    tx: &Transaction<'_>,
    principal: &Principal,
    project_id: &str,
    consumer_id: &str,
    expected_automation_revision: i64,
    expected_cursor: i64,
    through_observation_id: i64,
    requested_occurrences: &[Value],
    app_config: &Config,
    now_ms: i64,
) -> Result<Value> {
    let consumer = current_script_consumer(tx, principal, project_id, consumer_id)?;
    let entry = &consumer.entry;
    if entry.revision != expected_automation_revision {
        return Err(Error::new(
            "BUS_AUTOMATION_REVISION_CONFLICT",
            "current automation revision differs from the page snapshot",
        ));
    }
    let script_id = selected_script_id(entry)?;
    if expected_cursor < 0 || through_observation_id <= expected_cursor {
        return Err(Error::invalid(
            "bus page must advance strictly beyond its expected cursor",
        ));
    }

    let key = config::script_dispatch_state_key(
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?;
    let mut state = load_script_trigger_state(tx, entry)?.ok_or_else(|| {
        Error::new(
            "BUS_CONSUMER_CURSOR_MISSING",
            "selected ScriptRun consumer has no durable cursor",
        )
    })?;
    if state.cursor != expected_cursor {
        return Err(Error::new(
            "BUS_CURSOR_CONFLICT",
            "durable consumer cursor changed; read a fresh event page",
        ));
    }
    revalidate_script_trigger_intents(tx, entry, &mut state, app_config, now_ms)?;
    if state.pending.len() >= MAX_PENDING_SUBJECTS {
        return Err(Error::new(
            "BUS_PENDING_CAPACITY",
            "ScriptRun pending-intent journal is full",
        ));
    }

    let high_water = automation_intake::observed_event_high_water(tx)?;
    if through_observation_id > high_water {
        return Err(Error::new(
            "BUS_OCCURRENCE_NOT_FOUND",
            "selected page cut is beyond committed observations",
        ));
    }
    let page = automation_intake::observed_event_page(
        tx,
        expected_cursor,
        through_observation_id,
        MAX_BUS_SCAN,
    )?;
    if page.last().map(|event| event.observation_id) != Some(through_observation_id) {
        return Err(Error::new(
            "BUS_SCAN_BOUND_REACHED",
            "page cut is not the last occurrence in the bounded scan",
        ));
    }

    let mut expected_occurrences = Vec::new();
    for event in &page {
        if let Some(projection) =
            authorized_script_event_projection(tx, &consumer, app_config, event)?
        {
            expected_occurrences.push(json!({
                "observation_id":event.observation_id,
                "source_id":event.source_id,
                "event_kind":event.event_kind,
                "status":projection.status.map(crate::automation::event_rules::EventStatus::as_str),
                "action":{"kind":"script_run","script_id":script_id},
            }));
        }
    }
    if crate::model::canonical(&json!(requested_occurrences))?
        != crate::model::canonical(&json!(expected_occurrences))?
    {
        return Err(Error::new(
            "BUS_ACTION_SET_CHANGED",
            "requested event/action set differs from the current authorized page projection",
        ));
    }

    let pending_before = state.pending.len();
    for event in &page {
        if state.pending.len() >= MAX_PENDING_SUBJECTS {
            return Err(Error::new(
                "BUS_PENDING_CAPACITY",
                "ScriptRun pending-intent journal filled before the selected occurrence",
            ));
        }
        match &consumer.authority {
            ScriptConsumerAuthority::Manager(_) => {
                if event.source_id == LocalProducer::TaskSubmission.stream_id()
                    && event.event_kind == LocalProducer::TaskSubmission.event_kind()
                {
                    process_task_submission_script_trigger(
                        tx, app_config, entry, &mut state, event,
                    )?;
                } else {
                    process_system_event_script_trigger(tx, app_config, entry, &mut state, event)?;
                }
            }
            ScriptConsumerAuthority::ScopedModule(binding) => {
                process_script_trigger_for_scoped_module(
                    tx,
                    app_config,
                    entry,
                    &mut state,
                    event,
                    binding.owner_manager_id(),
                )?;
            }
        }
        state.cursor = event.observation_id;
    }
    let processed = page.len();
    if state
        .catch_up_until
        .is_some_and(|cut| state.cursor >= cut && high_water >= cut)
    {
        state.catch_up_until = None;
    }
    state.updated_at_ms = now_ms;
    save_script_trigger_state(tx, &key, &state)?;
    let pending_added = state.pending.len().saturating_sub(pending_before);
    Ok(json!({
        "consumer_id":consumer_id,
        "automation_revision":entry.revision,
        "expected_cursor":expected_cursor,
        "cursor":state.cursor,
        "processed_observations":processed,
        "occurrences":expected_occurrences,
        "pending_actions_added":pending_added,
        "disposition":if pending_added > 0 {"durably_pending"} else {"scanned_no_new_action"},
        "continuation":"existing_script_trigger_reconciler",
    }))
}

fn current_script_consumer<'a>(
    db: &Connection,
    principal: &'a Principal,
    project_id: &str,
    consumer_id: &str,
) -> Result<AuthenticatedScriptConsumer<'a>> {
    let authority = match principal.role {
        Role::Manager if !principal.client_id.is_empty() => {
            authorization::require_registered_manager(db, &principal.client_id)?;
            ScriptConsumerAuthority::Manager(principal)
        }
        Role::Module => {
            let binding = crate::store::bus_kernel::module_consumer_binding(db, principal)?;
            if binding.project_id() != project_id || binding.automation_id() != consumer_id {
                return Err(Error::new(
                    "FORBIDDEN",
                    "request does not match the authenticated consumer's retained scope",
                ));
            }
            authorization::require_registered_manager(db, binding.owner_manager_id())?;
            ScriptConsumerAuthority::ScopedModule(binding)
        }
        _ => {
            return Err(Error::new(
                "FORBIDDEN",
                "bus ScriptRun calls require an authenticated Manager or scoped Module consumer",
            ));
        }
    };
    let owner_manager_id = authority.owner_manager_id();
    let entry = config::load_entry(db, owner_manager_id, project_id, consumer_id)?
        .ok_or_else(|| Error::new("NOT_FOUND", "selected automation consumer was not found"))?;
    config::validate_entry(&entry)?;
    if entry.owner_manager_id != owner_manager_id || !entry.script_run_ready() {
        return Err(Error::new(
            "FORBIDDEN",
            "consumer must be an enabled ScriptRun action owned by its retained Manager",
        ));
    }
    if let ScriptConsumerAuthority::ScopedModule(binding) = &authority
        && script_run_consumer_scope_digest(&entry)? != binding.scope_digest()
    {
        return Err(Error::new(
            "BUS_CONSUMER_SCOPE_CHANGED",
            "current ScriptRun action or source selectors differ from the enrolled scope",
        ));
    }
    Ok(AuthenticatedScriptConsumer { authority, entry })
}

fn authorized_script_event_projection(
    db: &Connection,
    consumer: &AuthenticatedScriptConsumer<'_>,
    app_config: &Config,
    event: &crate::automation::intake::ObservedEvent,
) -> Result<Option<crate::automation::intake::SafeEventProjection>> {
    match &consumer.authority {
        ScriptConsumerAuthority::Manager(_) => {
            authorized_script_event_projection_for_manager(db, &consumer.entry, app_config, event)
        }
        ScriptConsumerAuthority::ScopedModule(binding) => {
            authorized_script_event_projection_for_module(
                db,
                binding.owner_manager_id(),
                &consumer.entry,
                app_config,
                event,
            )
        }
    }
}

fn authorized_script_event_projection_for_manager(
    db: &Connection,
    entry: &AutomationEntry,
    app_config: &Config,
    event: &crate::automation::intake::ObservedEvent,
) -> Result<Option<crate::automation::intake::SafeEventProjection>> {
    if !entry.selects_script_run_source_kind(&event.source_id, &event.event_kind) {
        return Ok(None);
    }
    // This command is only the consumer control plane. Routing its own
    // admission receipt would let an any-event ScriptRun recursively submit
    // new bus admissions forever.
    if event.source_id == "controller" && event.event_kind == "bus.consumer.admit" {
        return Ok(None);
    }
    let script_id = selected_script_id(entry)?;
    for projection in script_event_projections_with_alias(db, event)? {
        let status = projection.status;
        let generic_match = route_script_run_event(entry, event, status)?;
        let typed_submission_match = if event.source_id == LocalProducer::TaskSubmission.stream_id()
            && event.event_kind == LocalProducer::TaskSubmission.event_kind()
        {
            let receipt = automation_intake::receipt_by_observation_id(
                db,
                LocalProducer::TaskSubmission.source_id(),
                event.observation_id,
            )?;
            if let Some(receipt) = receipt {
                if let Some(cause) = cause_from_fact(event.observation_id, &receipt.payload)? {
                    entry.accepts_task_submission_script_run_event(&receipt, &cause)
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };
        if !generic_match && !typed_submission_match {
            continue;
        }

        let cause = system_event_cause(event, &projection, &script_id)?;
        let authorized = if typed_submission_match {
            current_submission_scope_matches_for_owner(db, &entry.owner_manager_id, entry, event)?
        } else {
            let context =
                super::script_trigger_authority::ScriptRunConsumerContext::from_entry(entry)?;
            match context.require_current_source(db, app_config, entry, &cause) {
                Ok(_) => true,
                Err(error) if error.code == "SCRIPT_EVENT_SELF_CAUSED" => false,
                Err(error) if script_event_revalidation_error(&error) => false,
                Err(error) => return Err(error),
            }
        };
        if authorized {
            return Ok(Some(projection));
        }
    }
    Ok(None)
}

fn authorized_script_event_projection_for_module(
    db: &Connection,
    owner_manager_id: &str,
    entry: &AutomationEntry,
    app_config: &Config,
    event: &crate::automation::intake::ObservedEvent,
) -> Result<Option<crate::automation::intake::SafeEventProjection>> {
    if !entry.selects_script_run_source_kind(&event.source_id, &event.event_kind)
        || (event.source_id == "controller" && event.event_kind == "bus.consumer.admit")
    {
        return Ok(None);
    }
    let script_id = selected_script_id(entry)?;
    for projection in script_event_projections_with_alias(db, event)? {
        if !route_script_run_event(entry, event, projection.status)? {
            continue;
        }
        let typed_submission = event.source_id == LocalProducer::TaskSubmission.stream_id()
            && event.event_kind == LocalProducer::TaskSubmission.event_kind();
        if typed_submission {
            let receipt = automation_intake::receipt_by_observation_id(
                db,
                LocalProducer::TaskSubmission.source_id(),
                event.observation_id,
            )?;
            let Some(receipt) = receipt else {
                continue;
            };
            let Some(cause) = cause_from_fact(event.observation_id, &receipt.payload)? else {
                continue;
            };
            if entry.accepts_task_submission_script_run_event(&receipt, &cause)
                && current_submission_scope_matches_for_owner(db, owner_manager_id, entry, event)?
            {
                return Ok(Some(projection));
            }
            continue;
        }
        let cause = system_event_cause(event, &projection, &script_id)?;
        match script_event_invocation_context_for_consumer(
            db,
            app_config,
            entry,
            &cause,
            owner_manager_id,
        ) {
            Ok(_) => return Ok(Some(projection)),
            Err(error) if error.code == "SCRIPT_EVENT_SELF_CAUSED" => continue,
            Err(error) if script_event_revalidation_error(&error) => continue,
            Err(error) if error.code == "SCRIPT_EVENT_SOURCE_UNAUTHORIZED" => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

fn process_script_trigger_for_scoped_module(
    tx: &Transaction<'_>,
    app_config: &Config,
    entry: &AutomationEntry,
    state: &mut ScriptTriggerState,
    event: &crate::automation::intake::ObservedEvent,
    owner_manager_id: &str,
) -> Result<()> {
    if event.source_id == LocalProducer::TaskSubmission.stream_id()
        && event.event_kind == LocalProducer::TaskSubmission.event_kind()
    {
        let Some(receipt) = automation_intake::receipt_by_observation_id(
            tx,
            LocalProducer::TaskSubmission.source_id(),
            event.observation_id,
        )?
        else {
            remember_script_trigger_recent(
                state,
                json!({"observation_id":event.observation_id,"disposition":"skipped","reason":"submission_receipt_unavailable"}),
            );
            return Ok(());
        };
        let cause = match cause_from_fact(event.observation_id, &receipt.payload) {
            Ok(cause) => cause,
            Err(error) => {
                remember_script_trigger_recent(
                    state,
                    json!({"observation_id":event.observation_id,"disposition":"gap","reason":error.code.to_ascii_lowercase()}),
                );
                return Ok(());
            }
        };
        let projection = script_event_projection_with_alias(tx, event, None)?;
        let route_matches = route_script_run_event(entry, event, projection.status)?;
        if let Some(cause) = cause.as_ref()
            && route_matches
            && entry.accepts_task_submission_script_run_event(&receipt, cause)
        {
            if !current_submission_scope_matches_for_owner(tx, owner_manager_id, entry, event)? {
                remember_script_trigger_recent(
                    state,
                    json!({"observation_id":event.observation_id,"disposition":"source_not_authorized"}),
                );
                return Ok(());
            }
            mark_observed_script_selectors(state, entry, event, projection.status)?;
            queue_applied_submission_script_trigger(tx, entry, state, cause.clone())?;
            return Ok(());
        }
        if route_matches {
            process_scoped_system_event(tx, app_config, owner_manager_id, entry, state, event)?;
        } else {
            remember_script_trigger_recent(
                state,
                json!({"observation_id":event.observation_id,"disposition":if cause.is_none() {"submission_not_applied"} else {"rule_unmatched"}}),
            );
        }
        return Ok(());
    }
    process_scoped_system_event(tx, app_config, owner_manager_id, entry, state, event)
}

fn process_scoped_system_event(
    tx: &Transaction<'_>,
    app_config: &Config,
    owner_manager_id: &str,
    entry: &AutomationEntry,
    state: &mut ScriptTriggerState,
    event: &crate::automation::intake::ObservedEvent,
) -> Result<()> {
    let projections = script_event_projections_with_alias(tx, event)?;
    if projections.is_empty() {
        remember_script_trigger_recent(
            state,
            json!({"observation_id":event.observation_id,"disposition":"occurrence_projection_unavailable"}),
        );
        return Ok(());
    }
    let script_id = selected_script_id(entry)?;
    for projection in projections {
        if super::event_requires_occurrence_projection(event)
            && (projection.occurrence_phase.is_none() || projection.occurrence_id.is_none())
        {
            remember_script_trigger_recent(
                state,
                json!({"observation_id":event.observation_id,"disposition":"occurrence_projection_unavailable"}),
            );
            continue;
        }
        if super::script_event_status_required(entry, &event.source_id, &event.event_kind)
            && projection.status.is_none()
        {
            remember_script_trigger_recent(
                state,
                json!({"observation_id":event.observation_id,"disposition":"status_projection_unavailable"}),
            );
            continue;
        }
        if !route_script_run_event(entry, event, projection.status)? {
            remember_script_trigger_recent(
                state,
                json!({"observation_id":event.observation_id,"disposition":"rule_unmatched"}),
            );
            continue;
        }
        let mut cause = system_event_cause(event, &projection, &script_id)?;
        match script_event_invocation_context_for_consumer(
            tx,
            app_config,
            entry,
            &cause,
            owner_manager_id,
        ) {
            Ok(context) => {
                mark_observed_script_selectors(state, entry, event, projection.status)?;
                attach_event_task_scope(&mut cause, &context)?;
                if let Some(operation_id) =
                    script_event_trigger_operation_exists(tx, entry, &cause, &script_id)?
                {
                    remember_script_trigger_recent(
                        state,
                        json!({"observation_id":event.observation_id,"script_id":script_id,"disposition":"already_admitted_semantically","operation_id":operation_id}),
                    );
                } else {
                    queue_system_event_script_trigger(
                        tx,
                        entry,
                        state,
                        cause,
                        script_id.clone(),
                        None,
                    )?;
                }
            }
            Err(error) if error.code == "SCRIPT_EVENT_SELF_CAUSED" => {
                remember_script_trigger_recent(
                    state,
                    json!({"observation_id":event.observation_id,"disposition":"same_automation_feedback_suppressed"}),
                );
            }
            Err(error)
                if error.code == "SCRIPT_EVENT_SOURCE_UNAUTHORIZED"
                    || error.code == "SCRIPT_EVENT_SOURCE_REVOKED"
                    || super::script_event_revalidation_error(&error) =>
            {
                remember_script_trigger_recent(
                    state,
                    json!({"observation_id":event.observation_id,"disposition":"source_not_authorized"}),
                );
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

pub(super) fn current_submission_scope_matches_for_owner(
    db: &Connection,
    owner_manager_id: &str,
    entry: &AutomationEntry,
    event: &crate::automation::intake::ObservedEvent,
) -> Result<bool> {
    let Some(receipt) = automation_intake::receipt_by_observation_id(
        db,
        LocalProducer::TaskSubmission.source_id(),
        event.observation_id,
    )?
    else {
        return Ok(false);
    };
    if receipt.observation_id != event.observation_id
        || receipt.source_id != LocalProducer::TaskSubmission.source_id()
        || receipt.event_kind != LocalProducer::TaskSubmission.event_kind()
        || receipt.operation_id.as_deref() != event.operation_id.as_deref()
        || receipt.payload["operation_id"].as_str() != receipt.operation_id.as_deref()
    {
        return Ok(false);
    }
    let Some(cause) = cause_from_fact(event.observation_id, &receipt.payload)? else {
        return Ok(false);
    };
    let Some(operation_id) = receipt.operation_id.as_deref() else {
        return Ok(false);
    };
    let operation_scope: Option<ConsumerSubmissionOperationRow> = db
        .query_row(
            "SELECT method,state,task_id,attempt_id,caller_id FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .optional()?;
    let Some((method, state, operation_task, operation_attempt, _caller)) = operation_scope else {
        return Ok(false);
    };
    if !matches!(method.as_str(), "task.submit" | "task.submission")
        || !matches!(state.as_str(), "settled" | "completed")
    {
        return Ok(false);
    }
    let document = match submissions::document(db, cause.id()) {
        Ok(document) => document,
        Err(error) if script_trigger_revalidation_error(&error) => return Ok(false),
        Err(error) => return Err(error),
    };
    if document["operation_id"] != operation_id
        || document["outcome"] != "applied"
        || document["task_id"].as_str().is_none_or(str::is_empty)
        || document["attempt_id"].as_str().is_none_or(str::is_empty)
        || document["candidate_ref"].as_str().is_none_or(str::is_empty)
    {
        return Ok(false);
    }
    let task_id = model::text(&document, "task_id")?;
    let task_revision = model::positive(&document, "task_revision")?;
    let attempt_id = model::text(&document, "attempt_id")?;
    if operation_task.as_deref() != Some(task_id)
        || operation_attempt.as_deref() != Some(attempt_id)
    {
        return Ok(false);
    }
    let task = super::tasks::get_task(db, task_id)?;
    let attempt = super::tasks::get_attempt(db, attempt_id)?;
    if task["project_id"] != entry.project_id
        || task["state"] != "open"
        || task["revision"] != task_revision
        || task["current_attempt_id"] != attempt_id
        || attempt["task_id"] != task_id
        || attempt["task_revision"] != task_revision
        || !attempt["released_at_ms"].is_null()
        || attempt["submission_ref"] != cause.id()
        || attempt["candidate_ref"] != document["candidate_ref"]
    {
        return Ok(false);
    }
    consumer_owner_has_current_attempt(
        db,
        owner_manager_id,
        task_id,
        task_revision,
        attempt_id,
        &entry.project_id,
    )
}

fn consumer_owner_has_current_attempt(
    db: &Connection,
    owner_manager_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    project_id: &str,
) -> Result<bool> {
    let scope: Option<ConsumerTaskAttemptScopeRow> = db
        .query_row(
            "SELECT t.project_id,t.state,t.revision,t.current_attempt_id,a.owner_id,a.task_revision,a.released_at_ms \
             FROM tasks AS t JOIN attempts AS a ON a.task_id=t.task_id \
             WHERE t.task_id=?1 AND a.attempt_id=?2",
            params![task_id, attempt_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    Ok(scope.is_some_and(
        |(project, state, current_revision, current_attempt, owner, attempt_revision, released)| {
            project == project_id
                && state == "open"
                && current_revision == task_revision
                && current_attempt.as_deref() == Some(attempt_id)
                && owner == owner_manager_id
                && attempt_revision == task_revision
                && released.is_none()
        },
    ))
}

/// Module-consumer source check. It uses the persisted Manager identity only
/// as data and revalidates that identity's current Task scope; it never creates
/// a Manager Principal or depends on a Manager IPC/GM session.
pub(super) fn script_event_invocation_context_for_consumer(
    db: &Connection,
    app_config: &Config,
    entry: &AutomationEntry,
    cause: &Value,
    owner_manager_id: &str,
) -> Result<super::ScriptEventInvocationContext> {
    if cause["kind"] != "system_event" {
        return Err(Error::new(
            "SCRIPT_EVENT_CAUSE_INVALID",
            "event trigger cause has the wrong kind",
        ));
    }
    let observation_id = model::positive(cause, "observation_id")?;
    let event = automation_intake::observed_event_by_id(db, observation_id)?.ok_or_else(|| {
        Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "event source is unavailable",
        )
    })?;
    if cause["source_id"] != event.source_id
        || cause["event_kind"] != event.event_kind
        || cause["recorded_at_ms"] != event.recorded_at_ms
        || cause["operation_id"].as_str() != event.operation_id.as_deref()
        || !entry.selects_script_run_source_kind(&event.source_id, &event.event_kind)
    {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "event no longer matches the exact selected source and kind",
        ));
    }
    let projection =
        super::script_event_projection_with_alias(db, &event, cause["occurrence_phase"].as_str())?;
    if super::event_requires_occurrence_projection(&event)
        && (projection.occurrence_phase.is_none() || projection.occurrence_id.is_none())
    {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "selected event has no exact typed occurrence projection",
        ));
    }
    let expected_id = super::system_event_semantic_id(observation_id, &projection)?;
    if cause["id"] != expected_id
        || cause["occurrence_phase"].as_str() != projection.occurrence_phase.as_deref()
        || cause["occurrence_id"].as_str() != projection.occurrence_id.as_deref()
        || cause["status"].as_str()
            != projection
                .status
                .map(crate::automation::event_rules::EventStatus::as_str)
        || cause["error_code"].as_str() != projection.error_code.as_deref()
        || cause["failure_category"].as_str() != projection.failure_category.as_deref()
        || cause["failed_supervisor"].as_str() != projection.failed_supervisor.as_deref()
        || !entry.accepts_script_run_event(&event.source_id, &event.event_kind, projection.status)
    {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "selected event projection or configured status no longer matches its cause",
        ));
    }
    authorization::require_registered_manager(db, owner_manager_id)?;
    let module_lifecycle_fact =
        if crate::store::module_supervisor_observation::is_lifecycle_event_source_kind(
            &event.source_id,
            &event.event_kind,
        ) {
            Some(
                crate::store::module_supervisor_observation::verified_lifecycle_event(db, &event)?
                    .ok_or_else(|| {
                        Error::new(
                            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                            "module lifecycle event has no exact Store-certified binding scope",
                        )
                    })?,
            )
        } else {
            None
        };
    if let Some(fact) = module_lifecycle_fact.as_ref() {
        require_module_lifecycle_binding_source(db, entry, owner_manager_id, fact)?;
    }

    let mut task_id = None;
    let mut task_revision = None;
    let mut attempt_id = None;
    let mut project_id = entry.project_id.clone();
    let mut operation_id = None;
    if let Some(event_operation_id) = event.operation_id.as_deref() {
        let operation_scope: Option<ConsumerEventOperationRow> = db
            .query_row(
                "SELECT task_id,attempt_id,effective_request_json,caller_id FROM operations WHERE operation_id=?1",
                [event_operation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let Some((operation_task_id, operation_attempt_id, effective_json, caller_id)) =
            operation_scope
        else {
            return Err(Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "linked Operation no longer exists",
            ));
        };
        if super::event_is_script_feedback_from_same_automation(
            db,
            event_operation_id,
            &effective_json,
            entry,
        )? {
            return Err(Error::new(
                "SCRIPT_EVENT_SELF_CAUSED",
                "same automation cannot recursively trigger from its own invocation effect",
            ));
        }
        match (operation_task_id, operation_attempt_id) {
            (Some(operation_task), Some(operation_attempt)) => {
                let attempt = super::tasks::get_attempt(db, &operation_attempt)?;
                let derived_task = model::text(&attempt, "task_id")?;
                let task = super::tasks::get_task(db, derived_task)?;
                let revision = model::positive(&attempt, "task_revision")?;
                if operation_task != derived_task
                    || task["project_id"] != entry.project_id
                    || task["state"] != "open"
                    || task["current_attempt_id"] != operation_attempt
                    || task["revision"] != revision
                    || !attempt["released_at_ms"].is_null()
                    || !consumer_owner_has_current_attempt(
                        db,
                        owner_manager_id,
                        &operation_task,
                        revision,
                        &operation_attempt,
                        &entry.project_id,
                    )?
                {
                    return Err(Error::new(
                        "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                        "linked Operation is outside the Manager's exact current Task scope",
                    ));
                }
                task_id = Some(operation_task);
                task_revision = Some(revision);
                attempt_id = Some(operation_attempt);
            }
            (None, Some(_)) => {
                return Err(Error::new(
                    "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                    "linked Operation has an Attempt without its Task",
                ));
            }
            (Some(operation_task), None) if caller_id == owner_manager_id => {
                let task = super::tasks::get_task(db, &operation_task)?;
                if task["project_id"] != entry.project_id || task["state"] != "open" {
                    return Err(Error::new(
                        "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                        "linked Operation Task is outside the Manager's current project scope",
                    ));
                }
                task_id = Some(operation_task);
            }
            (Some(_), None) => {
                return Err(Error::new(
                    "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                    "task-only Operation is not owned by the retained Manager",
                ));
            }
            (None, None) if caller_id == owner_manager_id => {}
            (None, None) => {
                return Err(Error::new(
                    "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                    "taskless Operation is outside the retained Manager's own source scope",
                ));
            }
        }
        operation_id = Some(event_operation_id.to_owned());
    } else if event.source_id == "controller:hooks" && event.event_kind == "git.post_commit" {
        let fact = automation_intake::hook_commit_fact_by_observation(db, observation_id)?
            .ok_or_else(|| {
                Error::new(
                    "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                    "selected HookCommit has no verified O1 receipt",
                )
            })?;
        if fact.project_id != entry.project_id {
            return Err(Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "selected HookCommit belongs to another project",
            ));
        }
        match crate::store::hooks::source_status(db, app_config, &fact.source_id)? {
            crate::store::hooks::HookSourceStatus::Current(source)
                if source.source_id == fact.source_id && source.project_id == entry.project_id =>
            {
                project_id = fact.project_id;
            }
            _ => {
                return Err(Error::new(
                    "SCRIPT_EVENT_SOURCE_REVOKED",
                    "selected HookSource is no longer current for this project",
                ));
            }
        }
    } else if event.source_id == "controller:hook-source"
        && matches!(
            event.event_kind.as_str(),
            "hook.source.setup" | "hook.source.revoke"
        )
    {
        project_id =
            super::hook_source_admin_project_scope(db, app_config, &entry.project_id, &event)?;
    } else if module_lifecycle_fact.is_some() {
        // The exact binding and Manager-owned agent.open source were checked
        // above, independently of any optional Operation correlation.
    } else if event.source_id == "controller:host-lifecycle"
        && ((matches!(event.event_kind.as_str(), "host.exit" | "host.interrupted")
            && projection.occurrence_phase.as_deref() == Some("host_interruption_observed"))
            || (matches!(event.event_kind.as_str(), "host.exit" | "host.failed")
                && projection.occurrence_phase.as_deref() == Some("host_terminal_exit_observed")))
    {
        // Safe host lifecycle metadata is global and carries no Task data.
    } else {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "event has no scoped Operation, verified HookSource, or typed host ACL",
        ));
    }

    for (field, observed) in [
        ("task_id", task_id.as_deref()),
        ("attempt_id", attempt_id.as_deref()),
    ] {
        if cause.get(field).is_some_and(|value| !value.is_null())
            && cause[field].as_str() != observed
        {
            return Err(Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "retained event Task/Attempt references differ from current source scope",
            ));
        }
    }
    if cause
        .get("task_revision")
        .is_some_and(|value| !value.is_null())
        && cause["task_revision"].as_i64() != task_revision
    {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "retained event Task revision differs from current source scope",
        ));
    }
    let mut input = json!({
        "kind":"system.event",
        "id":cause["id"],
        "observation_id":observation_id,
        "source_id":event.source_id,
        "event_kind":event.event_kind,
        "recorded_at_ms":event.recorded_at_ms,
        "project_id":project_id
    });
    if let Some(operation_id) = operation_id {
        input["operation_id"] = json!(operation_id);
    }
    if let Some(status) = projection.status {
        input["status"] = json!(status.as_str());
    }
    if let Some(error_code) = projection.error_code {
        input["error_code"] = json!(error_code);
    }
    if let Some(failure_category) = projection.failure_category {
        input["failure_category"] = json!(failure_category);
    }
    if let Some(failed_supervisor) = projection.failed_supervisor {
        input["failed_supervisor"] = json!(failed_supervisor);
    }
    if let Some(task_id) = task_id.as_deref() {
        input["task_id"] = json!(task_id);
    }
    if let Some(task_revision) = task_revision {
        input["task_revision"] = json!(task_revision);
    }
    if let Some(attempt_id) = attempt_id.as_deref() {
        input["attempt_id"] = json!(attempt_id);
    }
    Ok(super::ScriptEventInvocationContext {
        input,
        task_id,
        task_revision,
        attempt_id,
    })
}

fn require_module_lifecycle_binding_source(
    db: &Connection,
    entry: &AutomationEntry,
    owner_manager_id: &str,
    fact: &crate::store::module_supervisor_observation::VerifiedModuleLifecycleEvent,
) -> Result<()> {
    let mut statement = db.prepare(
        "SELECT caller_id,task_id,attempt_id FROM operations \
         WHERE method='agent.open' AND binding_id=?1 AND binding_generation=?2 \
         ORDER BY created_at_ms,operation_id LIMIT 2",
    )?;
    let opens = statement
        .query_map(params![fact.binding_id, fact.generation], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if opens.len() != 1 {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "module lifecycle binding has no unique retained agent.open source",
        ));
    }
    let (caller_id, task_id, attempt_id) = &opens[0];
    if caller_id != owner_manager_id {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "module lifecycle binding was opened by another Manager",
        ));
    }
    match (task_id.as_deref(), attempt_id.as_deref()) {
        (Some(task_id), Some(attempt_id)) => {
            let task = super::tasks::get_task(db, task_id)?;
            let attempt = super::tasks::get_attempt(db, attempt_id)?;
            let revision = task["revision"].as_i64();
            if task["project_id"] != entry.project_id
                || task["state"] != "open"
                || revision.is_none_or(|value| value <= 0)
                || task["current_attempt_id"].as_str() != Some(attempt_id)
                || attempt["task_id"].as_str() != Some(task_id)
                || attempt["task_revision"].as_i64() != revision
                || attempt["owner_id"].as_str() != Some(owner_manager_id)
                || !attempt["released_at_ms"].is_null()
            {
                return Err(Error::new(
                    "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                    "module lifecycle binding Task is outside the Manager's current project scope",
                ));
            }
        }
        (Some(task_id), None) => {
            let task = super::tasks::get_task(db, task_id)?;
            if task["project_id"] != entry.project_id || task["state"] != "open" {
                return Err(Error::new(
                    "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                    "module lifecycle binding Task is outside the Manager's current project scope",
                ));
            }
        }
        (None, Some(_)) => {
            return Err(Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "module lifecycle binding has an Attempt without its Task",
            ));
        }
        (None, None) => {}
    }
    Ok(())
}
