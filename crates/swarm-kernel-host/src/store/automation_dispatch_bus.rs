//! Narrow Store bridge implementing the shared `swarm-bus` metadata contract
//! over the existing ScriptRun event cursor and pending-intent journal.
//!
//! This module is included as a child of `store::automation_dispatch` so it can
//! reuse that module's validated source projections, semantic dedupe, and
//! pending-intent writer. It owns no database, queue, or second cursor.

use super::*;
use std::collections::BTreeSet;

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
type ConsumerEventOperationRow = (
    Option<String>,
    Option<String>,
    String,
    String,
    Option<String>,
    Option<i64>,
);
type ModuleEventWorkDispatchIdentity = (String, i64, String);
type ModuleEventOperationLinkOwner = (
    String,
    String,
    String,
    String,
    Option<ModuleEventWorkDispatchIdentity>,
);
type ModuleEventOperationAncestryRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
);
type ModuleEventLaunchParentRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    String,
);
type ModuleEventObservationRow = (
    String,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
    String,
    i64,
);
type ModuleEventOperationScopeRow = (
    String,
    String,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
);
type ModuleEventOpenRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DescriptorModuleEventScope {
    pub(super) binding_id: String,
    pub(super) binding_generation: i64,
    pub(super) source_event_key: String,
    pub(super) module_client_id: String,
    pub(super) module_artifact_id: String,
    pub(super) descriptor_selector_digest: String,
    pub(super) descriptor_event_schema_digest: String,
    pub(super) agent_open_operation_id: String,
    pub(super) agent_open_task_id: Option<String>,
    pub(super) agent_open_task_revision: Option<i64>,
    pub(super) agent_open_attempt_id: Option<String>,
    pub(super) source_task_id: Option<String>,
    pub(super) source_task_revision: Option<i64>,
    pub(super) source_attempt_id: Option<String>,
    pub(super) task_id: Option<String>,
    pub(super) task_revision: Option<i64>,
    pub(super) attempt_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct ModuleEventTaskScope {
    task_id: Option<String>,
    task_revision: Option<i64>,
    attempt_id: Option<String>,
}

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
                seal_retained_module_event_source(tx, entry, event, &mut cause)?;
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
            Err(error) if error.code == "SCRIPT_EVENT_SOURCE_UNAUTHORIZED" => {
                if super::event_can_wait_for_source_proof(tx, event)? {
                    // The descriptor-admitted metadata row is a valid source
                    // fact, but its retained Module proof can be completed by
                    // a later source admission. Keep the exact cause under
                    // the existing pending bound.
                    queue_system_event_script_trigger(
                        tx,
                        entry,
                        state,
                        cause,
                        script_id.clone(),
                        Some(super::SYSTEM_EVENT_SOURCE_PROOF_PENDING),
                    )?;
                } else {
                    // Foreign or malformed operationless rows never become
                    // pending from an arbitrary source/kind selector.
                    remember_script_trigger_recent(
                        state,
                        json!({"observation_id":event.observation_id,"disposition":"source_not_authorized"}),
                    );
                }
            }
            Err(error)
                if error.code == "SCRIPT_EVENT_SOURCE_REVOKED"
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

/// Revalidate one retained Manager-owned `event.emit` source without creating
/// a Manager Principal or depending on Manager IPC/GM state. The retained
/// event body remains private; this checks only its immutable Store identity.
fn require_manager_event_scope(
    db: &Connection,
    event: &crate::automation::intake::ObservedEvent,
    operation_id: &str,
    entry: &AutomationEntry,
    caller_id: &str,
) -> Result<()> {
    require_manager_event_scope_for_owner(
        db,
        event,
        operation_id,
        &entry.project_id,
        &entry.owner_manager_id,
        caller_id,
    )
}

/// The retained ScriptRun link validator has the same Manager owner and
/// project identity as an AutomationEntry, but does not hold the entry. Keep
/// its `event.emit` checks on this shared source proof so operation-linked
/// Manager events cannot fall through the generic Operation ACL path.
pub(super) fn require_manager_event_scope_for_retained(
    db: &Connection,
    event: &crate::automation::intake::ObservedEvent,
    operation_id: &str,
    project_id: &str,
    owner_manager_id: &str,
    caller_id: &str,
) -> Result<()> {
    require_manager_event_scope_for_owner(
        db,
        event,
        operation_id,
        project_id,
        owner_manager_id,
        caller_id,
    )
}

fn require_manager_event_scope_for_owner(
    db: &Connection,
    event: &crate::automation::intake::ObservedEvent,
    operation_id: &str,
    project_id: &str,
    owner_manager_id: &str,
    caller_id: &str,
) -> Result<()> {
    if event.source_id != crate::store::MANAGER_EVENT_SOURCE_STREAM {
        return Ok(());
    }
    if caller_id != owner_manager_id {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Manager event Operation is outside the configured owner scope",
        ));
    }
    let (method, effective_json): (String, String) = db.query_row(
        "SELECT method,effective_request_json FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if method != "event.emit" {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Manager event observation is linked to the wrong Operation method",
        ));
    }
    let effective: Value = serde_json::from_str(&effective_json).map_err(|_| {
        Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Manager event Operation authority record is malformed",
        )
    })?;
    let scope = &effective["event_emit"];
    if scope["schema_version"] != 1
        || scope["source_stream_id"] != crate::store::MANAGER_EVENT_SOURCE_STREAM
        || scope["owner_manager_id"] != owner_manager_id
        || scope["project_id"] != project_id
        || scope["name"] != event.event_kind
        || scope["source_event_key"].as_str().is_none()
        || scope["payload_digest"].as_str().is_none()
    {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Manager event Operation scope does not match the selected automation",
        ));
    }
    let row: Option<(String, Option<String>, String, String)> = db
        .query_row(
            "SELECT source_event_key,operation_id,kind,payload_json FROM observations \
             WHERE observation_id=?1",
            [event.observation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((source_event_key, observation_operation_id, kind, payload_json)) = row else {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Manager event observation is unavailable",
        ));
    };
    if observation_operation_id.as_deref() != Some(operation_id)
        || kind != event.event_kind
        || source_event_key != scope["source_event_key"].as_str().unwrap_or_default()
    {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Manager event observation identity no longer matches its Operation",
        ));
    }
    let payload: Value = serde_json::from_str(&payload_json).map_err(|_| {
        Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Manager event observation payload is malformed",
        )
    })?;
    if payload["schema_version"] != 1
        || payload["source_stream_id"] != crate::store::MANAGER_EVENT_SOURCE_STREAM
        || payload["source_event_key"] != scope["source_event_key"]
        || payload["owner_manager_id"] != owner_manager_id
        || payload["project_id"] != project_id
        || payload["name"] != event.event_kind
        || payload["dedupe_key"] != scope["dedupe_key"]
        || payload["payload"].is_null()
        || model::digest(model::canonical(&payload["payload"])?.as_bytes())
            != scope["payload_digest"].as_str().unwrap_or_default()
    {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Manager event observation payload does not match its retained scope",
        ));
    }
    Ok(())
}

fn module_event_contract_digest(
    db: &Connection,
    selector: &Value,
    artifact_id: &str,
) -> Result<String> {
    let registry =
        crate::store::meta(db, "module_catalog:trusted_descriptors:v1")?.ok_or_else(|| {
            Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "Module event has no retained trusted descriptor registry",
            )
        })?;
    let registered_revision = selector["registered_revision"]
        .as_u64()
        .filter(|value| *value > 0);
    let selected_revision = selector["selected_revision"].as_u64();
    let registry_revision = registry["revision"].as_u64();
    let module_id = selector["module_id"].as_str();
    if registry["schema_version"] != 1
        || registered_revision.is_none()
        || selected_revision
            .is_none_or(|revision| revision < registered_revision.unwrap_or_default())
        || registry_revision.is_none_or(|revision| revision < selected_revision.unwrap_or_default())
        || module_id.is_none_or(str::is_empty)
    {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Module event descriptor selector is inconsistent with its retained registry",
        ));
    }
    let descriptor = registry["descriptors"]
        .as_array()
        .and_then(|entries| {
            entries.iter().find_map(|entry| {
                let descriptor = &entry["descriptor"];
                (entry["registered_revision"].as_u64() == registered_revision
                    && descriptor["module_id"].as_str() == module_id
                    && descriptor["artifact"] == selector["artifact"]
                    && descriptor["artifact"]["artifact_id"].as_str() == Some(artifact_id))
                .then_some(descriptor)
            })
        })
        .ok_or_else(|| {
            Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "Module event descriptor contract is unavailable",
            )
        })?;
    let event_schemas = descriptor["event_schemas"].as_array().ok_or_else(|| {
        Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Module event descriptor has no retained event contract",
        )
    })?;
    Ok(model::digest(
        model::canonical(&Value::Array(event_schemas.to_owned()))?.as_bytes(),
    ))
}

/// Read immutable Task/Attempt origin facts and derive a separate current
/// action scope. Task state, the current Attempt pointer, release time and the
/// present Task revision can remove action rights, but never erase the source
/// of an already committed Module observation.
fn module_event_task_scopes(
    db: &Connection,
    project_id: &str,
    owner_manager_id: &str,
    binding_id: &str,
    binding_generation: i64,
    task_id: Option<&str>,
    attempt_id: Option<&str>,
) -> Result<(ModuleEventTaskScope, ModuleEventTaskScope)> {
    match (task_id, attempt_id) {
        (Some(task_id), Some(attempt_id)) => {
            let task = super::tasks::get_task(db, task_id)?;
            let attempt = super::tasks::get_attempt(db, attempt_id)?;
            let source_revision = attempt["task_revision"].as_i64().filter(|value| *value > 0);
            if task["project_id"] != project_id
                || attempt["task_id"].as_str() != Some(task_id)
                || source_revision.is_none()
                || attempt["owner_id"].as_str() != Some(owner_manager_id)
                || attempt["binding_id"].as_str() != Some(binding_id)
                || attempt["binding_generation"].as_i64() != Some(binding_generation)
                || attempt["task_snapshot"]["revision"]
                    .as_i64()
                    .is_some_and(|revision| Some(revision) != source_revision)
            {
                return Err(Error::new(
                    "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                    "Module event Task/Attempt provenance is inconsistent",
                ));
            }
            let origin = ModuleEventTaskScope {
                task_id: Some(task_id.to_owned()),
                task_revision: source_revision,
                attempt_id: Some(attempt_id.to_owned()),
            };
            let current_task_revision = task["revision"].as_i64();
            let action = if task["state"] == "open"
                && current_task_revision == source_revision
                && task["current_attempt_id"].as_str() == Some(attempt_id)
                && attempt["released_at_ms"].is_null()
            {
                origin.clone()
            } else {
                ModuleEventTaskScope::default()
            };
            Ok((origin, action))
        }
        (Some(task_id), None) => {
            let task = super::tasks::get_task(db, task_id)?;
            if task["project_id"] != project_id {
                return Err(Error::new(
                    "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                    "Module event Task provenance is outside its retained project",
                ));
            }
            Ok((
                ModuleEventTaskScope {
                    task_id: Some(task_id.to_owned()),
                    task_revision: None,
                    attempt_id: None,
                },
                ModuleEventTaskScope::default(),
            ))
        }
        (None, Some(_)) => Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Module event source has an Attempt without its Task",
        )),
        (None, None) => Ok((
            ModuleEventTaskScope::default(),
            ModuleEventTaskScope::default(),
        )),
    }
}

fn module_event_operation_link_owner(
    link: crate::automation::authorization::AnyOnBehalfOperationLink,
) -> ModuleEventOperationLinkOwner {
    use crate::automation::authorization::AnyOnBehalfOperationLink as Link;
    match link {
        Link::Review(link)
        | Link::Acceptance(link)
        | Link::Publication(link)
        | Link::CronCheckRun(link)
        | Link::GoalProgression(link)
        | Link::ScriptRun(link)
        | Link::ScriptEffect(link) => (
            link.operation_id,
            link.technical_requester_id,
            link.effective_manager_id,
            link.project_id,
            None,
        ),
        Link::WorkDispatch(link) => (
            link.operation_id,
            link.technical_requester_id,
            link.effective_manager_id,
            link.project_id,
            Some((
                link.automation_id,
                link.automation_revision,
                link.semantic_slot_id,
            )),
        ),
        Link::Repair(link) => (
            link.operation_id.clone(),
            link.technical_requester_id.clone(),
            link.effective_manager_id.clone(),
            link.project_id.clone(),
            None,
        ),
    }
}

const NATIVE_MCP_PHASE_OPERATION_CALLER: &str = "swarm.internal.c8.native_mcp";

/// Read the retained parent of a C8 native MCP phase Operation. The child
/// Operation stores this link in its immutable native_mcp envelope; no live
/// AssignmentContext, Task, participant lease, or supervisor record is read.
#[expect(
    clippy::too_many_arguments,
    reason = "native MCP phase validation keeps each retained identity field explicit"
)]
fn module_event_native_mcp_phase_parent(
    db: &Connection,
    operation_id: &str,
    method: &str,
    caller_id: &str,
    task_id: Option<&str>,
    attempt_id: Option<&str>,
    binding_id: Option<&str>,
    binding_generation: Option<i64>,
) -> Result<Option<String>> {
    let expected_original_phase = match method {
        "native.mcp.install" => "install",
        "native.mcp.observe" => "observe",
        "native.mcp.arm" => "arm",
        "native.mcp.read" => "read",
        _ => return Ok(None),
    };
    let unauthorized = || {
        Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "native MCP phase Operation has no exact retained launch parent",
        )
    };
    if caller_id != NATIVE_MCP_PHASE_OPERATION_CALLER {
        return Err(unauthorized());
    }

    type PhaseOperationRow = (
        String,
        String,
        String,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let phase_operation: Option<PhaseOperationRow> = db
        .query_row(
            "SELECT method,caller_id,client_request_id,\
                    json_extract(original_request_json,'$.schema_version'),\
                    json_extract(original_request_json,'$.operation_id'),\
                    json_extract(original_request_json,'$.phase'),\
                    json_extract(original_request_json,'$.binding_generation'),\
                    json_extract(original_request_json,'$.binding_id'),\
                    json_extract(effective_request_json,'$.native_mcp.schema_version'),\
                    json_extract(effective_request_json,'$.native_mcp.binding_generation'),\
                    json_extract(effective_request_json,'$.native_mcp.parent_launch_operation_id'),\
                    json_extract(effective_request_json,'$.native_mcp.launch_identity_digest'),\
                    json_extract(effective_request_json,'$.native_mcp.phase'),\
                    json_extract(effective_request_json,'$.native_mcp.method'),\
                    json_extract(effective_request_json,'$.native_mcp.task_revision'),\
                    json_extract(effective_request_json,'$.native_mcp.task_id'),\
                    json_extract(effective_request_json,'$.native_mcp.attempt_id'),\
                    json_extract(effective_request_json,'$.native_mcp.binding_id'),\
                    json_extract(effective_request_json,'$.native_mcp.child_operation_id') \
             FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get(12)?,
                    row.get(13)?,
                    row.get(14)?,
                    row.get(15)?,
                    row.get(16)?,
                    row.get(17)?,
                    row.get(18)?,
                ))
            },
        )
        .optional()?;
    let Some((
        retained_method,
        retained_caller_id,
        client_request_id,
        original_schema_version,
        original_operation_id,
        original_phase,
        original_binding_generation,
        original_binding_id,
        native_schema_version,
        native_binding_generation,
        parent_launch_operation_id,
        launch_identity_digest,
        native_phase,
        native_method,
        native_task_revision,
        native_task_id,
        native_attempt_id,
        native_binding_id,
        native_child_operation_id,
    )) = phase_operation
    else {
        return Err(unauthorized());
    };

    let native_phase_matches = matches!(
        (method, native_phase.as_deref()),
        ("native.mcp.install", Some("install"))
            | (
                "native.mcp.observe",
                Some("observe_unknown" | "observe_refresh")
            )
            | ("native.mcp.arm", Some("arm"))
            | ("native.mcp.read", Some("read"))
    );
    let valid_launch_digest = launch_identity_digest
        .as_deref()
        .and_then(|value| value.strip_prefix("sha256:"))
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        });
    let (Some(task_id), Some(attempt_id), Some(binding_id), Some(binding_generation)) =
        (task_id, attempt_id, binding_id, binding_generation)
    else {
        return Err(unauthorized());
    };
    let Some(parent_launch_operation_id) =
        parent_launch_operation_id.filter(|value| !value.is_empty() && value != operation_id)
    else {
        return Err(unauthorized());
    };
    if retained_method != method
        || retained_caller_id != caller_id
        || client_request_id != format!("native-mcp:{operation_id}")
        || original_schema_version != Some(1)
        || original_operation_id.as_deref() != Some(operation_id)
        || original_phase.as_deref() != Some(expected_original_phase)
        || original_binding_id.as_deref() != Some(binding_id)
        || original_binding_generation != Some(binding_generation)
        || native_schema_version != Some(1)
        || !valid_launch_digest
        || !native_phase_matches
        || native_method.as_deref() != Some(method)
        || native_task_id.as_deref() != Some(task_id)
        || native_task_revision.is_none_or(|revision| revision <= 0)
        || native_attempt_id.as_deref() != Some(attempt_id)
        || native_binding_id.as_deref() != Some(binding_id)
        || native_child_operation_id.as_deref() != Some(operation_id)
        || native_binding_generation != Some(binding_generation)
    {
        return Err(unauthorized());
    }
    let task_revision = native_task_revision.ok_or_else(unauthorized)?;

    type ParentLaunchRow = (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<i64>,
        String,
    );
    let parent: Option<ParentLaunchRow> = db
        .query_row(
            "SELECT method,task_id,attempt_id,binding_id,binding_generation,\
                    json_extract(effective_request_json,'$.launch_manifest.task.task_id'),\
                    json_extract(effective_request_json,'$.launch_manifest.task.observed_revision'),\
                    json_extract(effective_request_json,'$.launch_manifest.task.attempt_id'),\
                    json_extract(effective_request_json,'$.launch_manifest.binding.binding_id'),\
                    json_extract(effective_request_json,'$.launch_manifest.binding.generation'),\
                    operation_id \
             FROM operations WHERE operation_id=?1",
            [&parent_launch_operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                ))
            },
        )
        .optional()?;
    let Some((
        parent_method,
        parent_task_id,
        parent_attempt_id,
        parent_binding_id,
        parent_binding_generation,
        manifest_task_id,
        manifest_task_revision,
        manifest_attempt_id,
        manifest_binding_id,
        manifest_binding_generation,
        retained_parent_id,
    )) = parent
    else {
        return Err(unauthorized());
    };
    if parent_method != "swarm.launch"
        || retained_parent_id != parent_launch_operation_id
        || parent_task_id.as_deref() != Some(task_id)
        || parent_attempt_id.as_deref() != Some(attempt_id)
        || parent_binding_id.as_deref() != Some(binding_id)
        || parent_binding_generation != Some(binding_generation)
        || manifest_task_id.as_deref() != Some(task_id)
        || manifest_task_revision != Some(task_revision)
        || manifest_attempt_id.as_deref() != Some(attempt_id)
        || manifest_binding_id.as_deref() != Some(binding_id)
        || manifest_binding_generation != Some(binding_generation)
    {
        return Err(unauthorized());
    }
    Ok(Some(parent_launch_operation_id))
}

/// Resolve only an Operation's retained Manager owner. Direct callers remain
/// direct; technical callers require a validated retained automation link or
/// the exact immutable WorkDispatch launch ancestry. This carries an owner ID
/// as provenance data and never constructs a Manager Principal.
fn module_event_operation_owner(
    db: &Connection,
    operation_id: &str,
    project_id: &str,
    expected_owner_manager_id: &str,
) -> Result<String> {
    fn resolve(
        db: &Connection,
        operation_id: &str,
        project_id: &str,
        expected_owner_manager_id: &str,
        seen: &mut BTreeSet<String>,
        depth: u8,
    ) -> Result<String> {
        let unauthorized = |message: &str| Error::new("SCRIPT_EVENT_SOURCE_UNAUTHORIZED", message);
        if depth > 3 || !seen.insert(operation_id.to_owned()) {
            return Err(unauthorized(
                "Module event Operation ancestry is cyclic, ambiguous, or too deep",
            ));
        }
        let operation: Option<ModuleEventOperationAncestryRow> = db
            .query_row(
                "SELECT method,caller_id,task_id,attempt_id,binding_id,binding_generation,\
                        prerequisite_operation_id,\
                        json_type(effective_request_json,'$.automation_on_behalf'),\
                        json_type(effective_request_json,'$.on_behalf'),\
                        json_extract(effective_request_json,'$.launch_manifest.actor.kind'),\
                        json_extract(effective_request_json,'$.launch_manifest.actor.client_id'),\
                        json_extract(effective_request_json,'$.launch_manifest.actor.effective_manager_id'),\
                        json_extract(effective_request_json,'$.launch_manifest.actor.automation_id'),\
                        json_extract(effective_request_json,'$.launch_manifest.actor.automation_revision'),\
                        json_extract(effective_request_json,'$.launch_manifest.actor.semantic_slot_id'),\
                        json_extract(effective_request_json,'$.launch_manifest.task.task_id'),\
                        json_extract(effective_request_json,'$.launch_manifest.task.attempt_id'),\
                        json_extract(effective_request_json,'$.launch_manifest.binding.binding_id'),\
                        json_extract(effective_request_json,'$.launch_manifest.binding.generation') \
                 FROM operations WHERE operation_id=?1",
                [operation_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                        row.get(10)?,
                        row.get(11)?,
                        row.get(12)?,
                        row.get(13)?,
                        row.get(14)?,
                        row.get(15)?,
                        row.get(16)?,
                        row.get(17)?,
                        row.get(18)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            method,
            caller_id,
            task_id,
            attempt_id,
            binding_id,
            binding_generation,
            prerequisite_id,
            automation_on_behalf_type,
            on_behalf_type,
            launch_actor_kind,
            launch_actor_client_id,
            launch_actor_manager_id,
            launch_actor_automation_id,
            launch_actor_automation_revision,
            launch_actor_semantic_slot_id,
            launch_manifest_task_id,
            launch_manifest_attempt_id,
            launch_manifest_binding_id,
            launch_manifest_binding_generation,
        )) = operation
        else {
            return Err(unauthorized("Module event Operation is unavailable"));
        };

        if let Some(parent_id) = module_event_native_mcp_phase_parent(
            db,
            operation_id,
            &method,
            &caller_id,
            task_id.as_deref(),
            attempt_id.as_deref(),
            binding_id.as_deref(),
            binding_generation,
        )? {
            return resolve(
                db,
                &parent_id,
                project_id,
                expected_owner_manager_id,
                seen,
                depth + 1,
            );
        }
        if caller_id == NATIVE_MCP_PHASE_OPERATION_CALLER {
            return Err(unauthorized(
                "internal native MCP Operation is outside its retained phase contract",
            ));
        }

        if let Some(link) = authorization::any_on_behalf_operation_link(db, operation_id)? {
            let (
                linked_operation_id,
                technical_requester_id,
                owner_id,
                linked_project_id,
                work_dispatch_identity,
            ) = module_event_operation_link_owner(link);
            if linked_operation_id != operation_id
                || technical_requester_id != caller_id
                || linked_project_id != project_id
                || owner_id != expected_owner_manager_id
            {
                return Err(unauthorized(
                    "Module event Operation retained on-behalf identity does not match its project or caller",
                ));
            }
            if let Some((automation_id, automation_revision, semantic_slot_id)) =
                work_dispatch_identity
                && (method != "swarm.launch"
                    || launch_actor_kind.as_deref() != Some("work_dispatch")
                    || launch_actor_client_id.as_deref() != Some(caller_id.as_str())
                    || launch_actor_manager_id.as_deref() != Some(owner_id.as_str())
                    || launch_actor_automation_id.as_deref() != Some(automation_id.as_str())
                    || launch_actor_automation_revision != Some(automation_revision)
                    || launch_actor_semantic_slot_id.as_deref() != Some(semantic_slot_id.as_str())
                    || task_id
                        .as_deref()
                        .is_some_and(|task| launch_manifest_task_id.as_deref() != Some(task))
                    || attempt_id.as_deref().is_some_and(|attempt| {
                        launch_manifest_attempt_id.as_deref() != Some(attempt)
                    })
                    || binding_id.as_deref().is_some_and(|binding| {
                        launch_manifest_binding_id.as_deref() != Some(binding)
                    })
                    || binding_generation.is_some_and(|generation| {
                        launch_manifest_binding_generation != Some(generation)
                    }))
            {
                return Err(unauthorized(
                    "WorkDispatch link and retained launch manifest disagree about the Manager owner",
                ));
            }
            return Ok(owner_id);
        }

        if automation_on_behalf_type.as_deref() == Some("object")
            || on_behalf_type.as_deref() == Some("object")
            || launch_actor_kind.as_deref() == Some("work_dispatch")
        {
            return Err(unauthorized(
                "Module event Operation has on-behalf request data without a validated retained owner link",
            ));
        }

        if method == "agent.open"
            && let Some(parent_id) = prerequisite_id.as_deref()
        {
            validate_module_event_launch_open_parent(
                db,
                operation_id,
                parent_id,
                &caller_id,
                task_id.as_deref(),
                attempt_id.as_deref(),
                binding_id.as_deref(),
                binding_generation,
            )?;
            return resolve(
                db,
                parent_id,
                project_id,
                expected_owner_manager_id,
                seen,
                depth + 1,
            );
        }

        if method == "task.dispatch"
            && let Some(parent_id) =
                super::super::launcher_dispatch::historical_parent_for_operation(db, operation_id)?
        {
            return resolve(
                db,
                &parent_id,
                project_id,
                expected_owner_manager_id,
                seen,
                depth + 1,
            );
        }

        if caller_id == expected_owner_manager_id {
            return Ok(caller_id);
        }
        Err(unauthorized(
            "Module event Operation caller has no immutable retained Manager owner proof",
        ))
    }

    resolve(
        db,
        operation_id,
        project_id,
        expected_owner_manager_id,
        &mut BTreeSet::new(),
        0,
    )
}

fn require_module_event_operation_owner(
    db: &Connection,
    operation_id: &str,
    project_id: &str,
    expected_owner_manager_id: &str,
) -> Result<()> {
    module_event_operation_owner(db, operation_id, project_id, expected_owner_manager_id)?;
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "launch/open ancestry validation keeps each retained identity field explicit"
)]
fn validate_module_event_launch_open_parent(
    db: &Connection,
    open_operation_id: &str,
    parent_operation_id: &str,
    open_caller_id: &str,
    task_id: Option<&str>,
    attempt_id: Option<&str>,
    binding_id: Option<&str>,
    binding_generation: Option<i64>,
) -> Result<()> {
    let unauthorized = || {
        Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Module agent.open does not match its retained launch parent",
        )
    };
    let parent: Option<ModuleEventLaunchParentRow> = db
        .query_row(
            "SELECT caller_id,method,task_id,attempt_id,binding_id,binding_generation,effective_request_json \
             FROM operations WHERE operation_id=?1",
            [parent_operation_id],
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
    let Some((
        parent_caller_id,
        method,
        parent_task_id,
        parent_attempt_id,
        parent_binding_id,
        parent_generation,
        effective_raw,
    )) = parent
    else {
        return Err(unauthorized());
    };
    let effective: Value = serde_json::from_str(&effective_raw).map_err(|_| unauthorized())?;
    let manifest = &effective["launch_manifest"];
    if method != "swarm.launch"
        || parent_caller_id != open_caller_id
        || parent_task_id.as_deref() != task_id
        || parent_attempt_id.as_deref() != attempt_id
        || parent_binding_id.as_deref() != binding_id
        || parent_generation != binding_generation
        || task_id.is_none_or(str::is_empty)
        || attempt_id.is_none_or(str::is_empty)
        || binding_id.is_none_or(str::is_empty)
        || binding_generation.is_none_or(|value| value <= 0)
        || manifest["task"]["task_id"].as_str() != task_id
        || manifest["task"]["attempt_id"].as_str() != attempt_id
        || manifest["binding"]["binding_id"].as_str() != binding_id
        || manifest["binding"]["generation"].as_i64() != binding_generation
        || manifest["binding"]["operation_id"].as_str() != Some(open_operation_id)
    {
        return Err(unauthorized());
    }
    let open_matches_parent: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM operations WHERE operation_id=?1 AND method='agent.open' \
         AND caller_id=?2 AND prerequisite_operation_id=?3 AND task_id=?4 AND attempt_id=?5 \
         AND binding_id=?6 AND binding_generation=?7)",
        params![
            open_operation_id,
            open_caller_id,
            parent_operation_id,
            task_id,
            attempt_id,
            binding_id,
            binding_generation,
        ],
        |row| row.get(0),
    )?;
    if !open_matches_parent {
        return Err(unauthorized());
    }
    Ok(())
}

/// Prove a Module event from its already committed authenticated observation,
/// immutable binding and descriptor selector, and the unique Manager-owned
/// `agent.open`. A linked event Operation supplies its own immutable
/// Task/Attempt origin; current Task state is used only to derive action scope.
/// No current registration, descriptor enablement, binding release or Task
/// liveness check can revoke the stored source fact.
pub(super) fn require_module_event_source_provenance(
    db: &Connection,
    project_id: &str,
    owner_manager_id: &str,
    event: &crate::automation::intake::ObservedEvent,
) -> Result<DescriptorModuleEventScope> {
    let module_client_id = event
        .source_id
        .strip_prefix("module:")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "Module event source has no registered module identity",
            )
        })?;
    let observation: Option<ModuleEventObservationRow> = db
        .query_row(
            "SELECT source_stream_id,source_event_key,binding_id,binding_generation,\
                    operation_id,kind,recorded_at_ms \
             FROM observations WHERE observation_id=?1",
            [event.observation_id],
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
    let Some((
        source_stream_id,
        source_event_key,
        binding_id,
        binding_generation,
        observation_operation_id,
        kind,
        recorded_at_ms,
    )) = observation
    else {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Module event observation is unavailable",
        ));
    };
    let source_event_key = source_event_key
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "Module event observation has no retained source key",
            )
        })?;
    if source_stream_id != event.source_id
        || observation_operation_id.as_deref() != event.operation_id.as_deref()
        || kind != event.event_kind
        || recorded_at_ms != event.recorded_at_ms
    {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Module event observation identity no longer matches its source",
        ));
    }
    let binding_id = binding_id
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "Module event observation has no retained binding scope",
            )
        })?;
    let binding_generation = binding_generation
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "Module event observation has no retained binding generation",
            )
        })?;
    let binding = crate::store::operations::get_binding(db, &binding_id, binding_generation)?;
    if binding["route"]["runtime"].as_str() != Some("module")
        || binding["observation"]["module_client_id"].as_str() != Some(module_client_id)
    {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Module event binding is not owned by the recorded module",
        ));
    }
    let artifact_id = binding["module_artifact_id"].as_str().ok_or_else(|| {
        Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Module event binding has no immutable artifact identity",
        )
    })?;
    let selector = binding["observation"]
        .get("module_contract_selector")
        .ok_or_else(|| {
            Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "Module event binding has no retained descriptor selector",
            )
        })?;
    let descriptor_selector_digest = model::digest(model::canonical(selector)?.as_bytes());
    let registered_revision = selector["registered_revision"].as_u64();
    let selected_revision = selector["selected_revision"].as_u64();
    if selector["schema_version"] != 1
        || registered_revision.is_none_or(|revision| revision == 0)
        || selected_revision
            .is_none_or(|revision| revision < registered_revision.unwrap_or_default())
        || selector["artifact"]["artifact_id"].as_str() != Some(artifact_id)
    {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Module event binding has no valid retained descriptor identity",
        ));
    }
    let descriptor_event_schema_digest = module_event_contract_digest(db, selector, artifact_id)?;

    let mut statement = db.prepare(
        "SELECT operation_id,caller_id,task_id,attempt_id FROM operations \
         WHERE method='agent.open' AND binding_id=?1 AND binding_generation=?2 \
         ORDER BY created_at_ms,operation_id LIMIT 2",
    )?;
    let opens = statement
        .query_map(params![binding_id, binding_generation], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if opens.len() != 1 {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "Module event binding has no unique retained agent.open source",
        ));
    }
    let (agent_open_operation_id, _caller_id, binding_task_id, binding_attempt_id) = &opens[0];
    require_module_event_operation_owner(
        db,
        agent_open_operation_id,
        project_id,
        owner_manager_id,
    )?;
    let (agent_open_origin, _) = module_event_task_scopes(
        db,
        project_id,
        owner_manager_id,
        &binding_id,
        binding_generation,
        binding_task_id.as_deref(),
        binding_attempt_id.as_deref(),
    )?;

    let (source_scope, action_scope) = if let Some(operation_id) = event.operation_id.as_deref() {
        let operation: Option<ModuleEventOperationScopeRow> = db
            .query_row(
                "SELECT operation_id,caller_id,binding_id,binding_generation,task_id,attempt_id \
                 FROM operations WHERE operation_id=?1",
                [operation_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            retained_operation_id,
            _operation_caller_id,
            operation_binding_id,
            operation_binding_generation,
            operation_task_id,
            operation_attempt_id,
        )) = operation
        else {
            return Err(Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "Module event Operation is unavailable",
            ));
        };
        require_module_event_operation_owner(db, operation_id, project_id, owner_manager_id)?;
        if retained_operation_id != operation_id
            || operation_binding_id.as_deref() != Some(binding_id.as_str())
            || operation_binding_generation != Some(binding_generation)
        {
            return Err(Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "Module event Operation is outside its retained Manager binding",
            ));
        }
        module_event_task_scopes(
            db,
            project_id,
            owner_manager_id,
            &binding_id,
            binding_generation,
            operation_task_id.as_deref(),
            operation_attempt_id.as_deref(),
        )?
    } else {
        (agent_open_origin.clone(), ModuleEventTaskScope::default())
    };

    Ok(DescriptorModuleEventScope {
        binding_id,
        binding_generation,
        source_event_key,
        module_client_id: module_client_id.to_owned(),
        module_artifact_id: artifact_id.to_owned(),
        descriptor_selector_digest,
        descriptor_event_schema_digest,
        agent_open_operation_id: agent_open_operation_id.to_owned(),
        agent_open_task_id: binding_task_id.clone(),
        agent_open_task_revision: agent_open_origin.task_revision,
        agent_open_attempt_id: binding_attempt_id.clone(),
        source_task_id: source_scope.task_id,
        source_task_revision: source_scope.task_revision,
        source_attempt_id: source_scope.attempt_id,
        task_id: action_scope.task_id,
        task_revision: action_scope.task_revision,
        attempt_id: action_scope.attempt_id,
    })
}

/// Seal the Module source facts that made the current admission valid into
/// the retained cause. This runs only after the normal current source gate;
/// the proof is later used without reopening Manager, registration, Task or
/// descriptor liveness state.
pub(super) fn seal_retained_module_event_source(
    db: &Connection,
    entry: &AutomationEntry,
    event: &crate::automation::intake::ObservedEvent,
    cause: &mut Value,
) -> Result<()> {
    if !event.source_id.starts_with("module:") {
        return Ok(());
    }
    let scope = require_module_event_source_provenance(
        db,
        &entry.project_id,
        &entry.owner_manager_id,
        event,
    )?;
    cause["module_source"] = json!({
        "schema_version": 1,
        "source_stream_id": event.source_id,
        "source_event_key": scope.source_event_key,
        "observation_id": event.observation_id,
        "event_kind": event.event_kind,
        "recorded_at_ms": event.recorded_at_ms,
        "operation_id": event.operation_id,
        "binding_id": scope.binding_id,
        "binding_generation": scope.binding_generation,
        "module_client_id": scope.module_client_id,
        "module_artifact_id": scope.module_artifact_id,
        "descriptor_selector_digest": scope.descriptor_selector_digest,
        "descriptor_event_schema_digest": scope.descriptor_event_schema_digest,
        "agent_open_operation_id": scope.agent_open_operation_id,
        "agent_open_task_id": scope.agent_open_task_id,
        "agent_open_task_revision": scope.agent_open_task_revision,
        "agent_open_attempt_id": scope.agent_open_attempt_id,
        "source_task_id": scope.source_task_id,
        "source_task_revision": scope.source_task_revision,
        "source_attempt_id": scope.source_attempt_id,
        "owner_manager_id": entry.owner_manager_id,
        "project_id": entry.project_id,
        "task_id": scope.task_id,
        "task_revision": scope.task_revision,
        "attempt_id": scope.attempt_id,
    });
    Ok(())
}

fn retained_module_source_corrupt(message: &str) -> Error {
    Error::new("AUTOMATION_LINK_CORRUPT", message)
}

/// Re-read only immutable source identity for a retained Module event. Current
/// binding release, registration, descriptor enablement, Task state and
/// Manager session are deliberately absent: an admitted ScriptRun may finish
/// after any of those current facts change.
pub(super) fn validate_retained_module_event_source(
    db: &Connection,
    project_id: &str,
    owner_manager_id: &str,
    event: &crate::automation::intake::ObservedEvent,
    cause: &Value,
) -> Result<DescriptorModuleEventScope> {
    let module_client_id = event
        .source_id
        .strip_prefix("module:")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            retained_module_source_corrupt("retained Module event has no module identity")
        })?;
    let proof = cause.get("module_source").ok_or_else(|| {
        retained_module_source_corrupt("retained Module event has no sealed source proof")
    })?;
    let proof_text = |field: &str, description: &str| {
        proof[field]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| retained_module_source_corrupt(description))
    };
    let source_stream_id = proof_text(
        "source_stream_id",
        "retained Module proof has no source stream",
    )?;
    let source_event_key = proof_text(
        "source_event_key",
        "retained Module proof has no source key",
    )?;
    let binding_id = proof_text("binding_id", "retained Module proof has no binding")?;
    let proof_module_client_id = proof_text(
        "module_client_id",
        "retained Module proof has no module client",
    )?;
    let module_artifact_id = proof_text(
        "module_artifact_id",
        "retained Module proof has no artifact",
    )?;
    let descriptor_selector_digest = proof_text(
        "descriptor_selector_digest",
        "retained Module proof has no descriptor selector identity",
    )?;
    let descriptor_event_schema_digest = proof_text(
        "descriptor_event_schema_digest",
        "retained Module proof has no descriptor event contract identity",
    )?;
    let agent_open_operation_id = proof_text(
        "agent_open_operation_id",
        "retained Module proof has no agent.open identity",
    )?;
    let binding_generation = proof["binding_generation"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            retained_module_source_corrupt("retained Module proof has no binding generation")
        })?;
    let proof_agent_open_task_id = proof["agent_open_task_id"].as_str().map(str::to_owned);
    let proof_agent_open_task_revision = proof["agent_open_task_revision"].as_i64();
    let proof_agent_open_attempt_id = proof["agent_open_attempt_id"].as_str().map(str::to_owned);
    let proof_source_task_id = proof["source_task_id"].as_str().map(str::to_owned);
    let proof_source_task_revision = proof["source_task_revision"].as_i64();
    let proof_source_attempt_id = proof["source_attempt_id"].as_str().map(str::to_owned);
    let proof_task_id = proof["task_id"].as_str().map(str::to_owned);
    let proof_task_revision = proof["task_revision"].as_i64();
    let proof_attempt_id = proof["attempt_id"].as_str().map(str::to_owned);
    let proof_owner_manager_id =
        proof_text("owner_manager_id", "retained Module proof has no owner")?;
    let proof_project_id = proof_text("project_id", "retained Module proof has no project")?;
    let proof_observation_id = proof["observation_id"].as_i64().filter(|value| *value > 0);
    let proof_recorded_at_ms = proof["recorded_at_ms"].as_i64();
    let proof_agent_open_scope_valid = match (
        &proof_agent_open_task_id,
        proof_agent_open_task_revision,
        &proof_agent_open_attempt_id,
    ) {
        (None, None, None) | (Some(_), None, None) => true,
        (Some(_), Some(revision), Some(_)) => revision > 0,
        _ => false,
    };
    let proof_source_scope_valid = match (
        &proof_source_task_id,
        proof_source_task_revision,
        &proof_source_attempt_id,
    ) {
        (None, None, None) | (Some(_), None, None) => true,
        (Some(_), Some(revision), Some(_)) => revision > 0,
        _ => false,
    };
    let proof_action_scope_valid = match (&proof_task_id, proof_task_revision, &proof_attempt_id) {
        (None, None, None) => true,
        (Some(_), Some(revision), Some(_)) => revision > 0,
        _ => false,
    };
    if proof["schema_version"] != 1
        || source_stream_id != event.source_id
        || source_event_key.is_empty()
        || proof_observation_id != Some(event.observation_id)
        || proof["event_kind"] != event.event_kind
        || proof_recorded_at_ms != Some(event.recorded_at_ms)
        || proof["operation_id"].as_str() != event.operation_id.as_deref()
        || proof_module_client_id != module_client_id
        || proof_owner_manager_id != owner_manager_id
        || proof_project_id != project_id
        || !proof_agent_open_scope_valid
        || !proof_source_scope_valid
        || !proof_action_scope_valid
        || cause["task_id"] != proof["task_id"]
        || cause["task_revision"] != proof["task_revision"]
        || cause["attempt_id"] != proof["attempt_id"]
    {
        return Err(retained_module_source_corrupt(
            "retained Module source proof does not match its event scope",
        ));
    }

    let observation: Option<ModuleEventObservationRow> = db
        .query_row(
            "SELECT source_stream_id,source_event_key,binding_id,binding_generation,\
                    operation_id,kind,recorded_at_ms \
             FROM observations WHERE observation_id=?1",
            [event.observation_id],
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
    let Some((
        observed_source_stream_id,
        observed_source_event_key,
        observed_binding_id,
        observed_binding_generation,
        observed_operation_id,
        observed_kind,
        observed_recorded_at_ms,
    )) = observation
    else {
        return Err(retained_module_source_corrupt(
            "retained Module event observation is missing",
        ));
    };
    if observed_source_stream_id != source_stream_id
        || observed_source_event_key.as_deref() != Some(source_event_key)
        || observed_binding_id.as_deref() != Some(binding_id)
        || observed_binding_generation != Some(binding_generation)
        || observed_operation_id.as_deref() != event.operation_id.as_deref()
        || observed_kind != event.event_kind
        || observed_recorded_at_ms != event.recorded_at_ms
    {
        return Err(retained_module_source_corrupt(
            "retained Module event observation identity changed",
        ));
    }

    let binding = crate::store::operations::get_binding(db, binding_id, binding_generation)
        .map_err(|_| retained_module_source_corrupt("retained Module binding is missing"))?;
    let selector = binding["observation"]
        .get("module_contract_selector")
        .ok_or_else(|| {
            retained_module_source_corrupt("retained Module binding has no descriptor selector")
        })?;
    let selector_digest = model::digest(
        model::canonical(selector)
            .map_err(|_| {
                retained_module_source_corrupt("retained Module descriptor selector is malformed")
            })?
            .as_bytes(),
    );
    if binding["route"]["runtime"] != "module"
        || binding["observation"]["module_client_id"].as_str() != Some(module_client_id)
        || binding["module_artifact_id"].as_str() != Some(module_artifact_id)
        || binding["route"]["module_artifact_id"].as_str() != Some(module_artifact_id)
        || selector["artifact"]["artifact_id"].as_str() != Some(module_artifact_id)
        || selector_digest != descriptor_selector_digest
    {
        return Err(retained_module_source_corrupt(
            "retained Module binding identity changed",
        ));
    }
    let retained_event_schema_digest =
        module_event_contract_digest(db, selector, module_artifact_id).map_err(|_| {
            retained_module_source_corrupt("retained Module event contract is unavailable")
        })?;
    if retained_event_schema_digest != descriptor_event_schema_digest {
        return Err(retained_module_source_corrupt(
            "retained Module event contract identity changed",
        ));
    }

    let open: Option<ModuleEventOpenRow> = db
        .query_row(
            "SELECT operation_id,caller_id,task_id,attempt_id,binding_id,binding_generation \
             FROM operations WHERE operation_id=?1 AND method='agent.open'",
            [agent_open_operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()?;
    let Some((
        retained_open_operation_id,
        _caller_id,
        open_task_id,
        open_attempt_id,
        open_binding_id,
        open_binding_generation,
    )) = open
    else {
        return Err(retained_module_source_corrupt(
            "retained Module agent.open source is missing",
        ));
    };
    require_module_event_operation_owner(db, agent_open_operation_id, project_id, owner_manager_id)
        .map_err(|_| retained_module_source_corrupt("retained Module agent.open owner changed"))?;
    if retained_open_operation_id != agent_open_operation_id
        || open_binding_id.as_deref() != Some(binding_id)
        || open_binding_generation != Some(binding_generation)
        || open_task_id.as_ref() != proof_agent_open_task_id.as_ref()
        || open_attempt_id.as_ref() != proof_agent_open_attempt_id.as_ref()
    {
        return Err(retained_module_source_corrupt(
            "retained Module agent.open source identity changed",
        ));
    }
    let (open_origin, _) = module_event_task_scopes(
        db,
        project_id,
        owner_manager_id,
        binding_id,
        binding_generation,
        open_task_id.as_deref(),
        open_attempt_id.as_deref(),
    )
    .map_err(|_| retained_module_source_corrupt("retained Module agent.open Task scope changed"))?;
    if open_origin.task_revision != proof_agent_open_task_revision {
        return Err(retained_module_source_corrupt(
            "retained Module agent.open Attempt revision changed",
        ));
    }

    let source_origin = if let Some(operation_id) = event.operation_id.as_deref() {
        let operation: Option<ModuleEventOperationScopeRow> = db
            .query_row(
                "SELECT operation_id,caller_id,binding_id,binding_generation,task_id,attempt_id \
                 FROM operations WHERE operation_id=?1",
                [operation_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            retained_operation_id,
            _operation_caller_id,
            operation_binding_id,
            operation_binding_generation,
            operation_task_id,
            operation_attempt_id,
        )) = operation
        else {
            return Err(retained_module_source_corrupt(
                "retained Module event Operation is missing",
            ));
        };
        require_module_event_operation_owner(db, operation_id, project_id, owner_manager_id)
            .map_err(|_| {
                retained_module_source_corrupt("retained Module event Operation owner changed")
            })?;
        if retained_operation_id != operation_id
            || operation_binding_id.as_deref() != Some(binding_id)
            || operation_binding_generation != Some(binding_generation)
        {
            return Err(retained_module_source_corrupt(
                "retained Module event Operation binding identity changed",
            ));
        }
        module_event_task_scopes(
            db,
            project_id,
            owner_manager_id,
            binding_id,
            binding_generation,
            operation_task_id.as_deref(),
            operation_attempt_id.as_deref(),
        )
        .map_err(|_| retained_module_source_corrupt("retained Module event Task scope changed"))?
        .0
    } else {
        open_origin.clone()
    };
    if source_origin.task_id != proof_source_task_id
        || source_origin.task_revision != proof_source_task_revision
        || source_origin.attempt_id != proof_source_attempt_id
    {
        return Err(retained_module_source_corrupt(
            "retained Module event source Task/Attempt identity changed",
        ));
    }
    if proof_task_id.is_some()
        && (proof_task_id != source_origin.task_id
            || proof_task_revision != source_origin.task_revision
            || proof_attempt_id != source_origin.attempt_id)
    {
        return Err(retained_module_source_corrupt(
            "retained Module action scope differs from its immutable source Attempt",
        ));
    }

    Ok(DescriptorModuleEventScope {
        binding_id: binding_id.to_owned(),
        binding_generation,
        source_event_key: source_event_key.to_owned(),
        module_client_id: module_client_id.to_owned(),
        module_artifact_id: module_artifact_id.to_owned(),
        descriptor_selector_digest: descriptor_selector_digest.to_owned(),
        descriptor_event_schema_digest: descriptor_event_schema_digest.to_owned(),
        agent_open_operation_id: agent_open_operation_id.to_owned(),
        agent_open_task_id: proof_agent_open_task_id,
        agent_open_task_revision: proof_agent_open_task_revision,
        agent_open_attempt_id: proof_agent_open_attempt_id,
        source_task_id: proof_source_task_id,
        source_task_revision: proof_source_task_revision,
        source_attempt_id: proof_source_attempt_id,
        task_id: proof_task_id,
        task_revision: proof_task_revision,
        attempt_id: proof_attempt_id,
    })
}

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

    let module_event_scope = if event.source_id.starts_with("module:") {
        Some(require_module_event_source_provenance(
            db,
            &entry.project_id,
            owner_manager_id,
            &event,
        )?)
    } else {
        None
    };
    let mut task_id = None;
    let mut task_revision = None;
    let mut attempt_id = None;
    let mut project_id = entry.project_id.clone();
    let mut operation_id = None;
    if let Some(event_operation_id) = event.operation_id.as_deref() {
        let operation_scope: Option<ConsumerEventOperationRow> = db
            .query_row(
                "SELECT task_id,attempt_id,effective_request_json,caller_id,binding_id,binding_generation \
                 FROM operations WHERE operation_id=?1",
                [event_operation_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            operation_task_id,
            operation_attempt_id,
            effective_json,
            caller_id,
            operation_binding_id,
            operation_binding_generation,
        )) = operation_scope
        else {
            return Err(Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "linked Operation no longer exists",
            ));
        };
        require_manager_event_scope(db, &event, event_operation_id, entry, &caller_id)?;
        let module_operation_has_taskless_action_scope =
            if let Some(module_scope) = module_event_scope.as_ref() {
                let source_scope_matches = operation_task_id.as_deref()
                    == module_scope.source_task_id.as_deref()
                    && operation_attempt_id.as_deref() == module_scope.source_attempt_id.as_deref();
                require_module_event_operation_owner(
                    db,
                    event_operation_id,
                    &entry.project_id,
                    owner_manager_id,
                )?;
                if operation_binding_id.as_deref() != Some(module_scope.binding_id.as_str())
                    || operation_binding_generation != Some(module_scope.binding_generation)
                    || !source_scope_matches
                {
                    return Err(Error::new(
                        "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                        "Module event Operation is outside its retained binding scope",
                    ));
                }
                module_scope.task_id.is_none()
            } else {
                false
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
        if !module_operation_has_taskless_action_scope {
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
    } else if let Some(module_scope) = module_event_scope.as_ref() {
        task_id = module_scope.task_id.clone();
        task_revision = module_scope.task_revision;
        attempt_id = module_scope.attempt_id.clone();
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
            "event has no scoped Operation, verified Module/HookSource, or typed host ACL",
        ));
    }

    if module_event_scope.is_some() && cause.get("module_source").is_some() {
        if cause["task_id"] != cause["module_source"]["task_id"]
            || cause["task_revision"] != cause["module_source"]["task_revision"]
            || cause["attempt_id"] != cause["module_source"]["attempt_id"]
        {
            return Err(Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "retained Module action snapshot differs from its sealed source proof",
            ));
        }
    } else {
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
        "SELECT operation_id,caller_id,task_id,attempt_id FROM operations \
         WHERE method='agent.open' AND binding_id=?1 AND binding_generation=?2 \
         ORDER BY created_at_ms,operation_id LIMIT 2",
    )?;
    let opens = statement
        .query_map(params![fact.binding_id, fact.generation], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if opens.len() != 1 {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "module lifecycle binding has no unique retained agent.open source",
        ));
    }
    let (operation_id, _caller_id, task_id, attempt_id) = &opens[0];
    require_module_event_operation_owner(db, operation_id, &entry.project_id, owner_manager_id)?;
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
