//! Bounded durable routing from committed submission facts into the existing
//! review-assignment Operation and semantic slot.

use super::{automation_intake, capacity, reviews::ReviewActor, submissions, tasks};
use crate::{
    automation::{
        actions::{AutomationCause, AutomationStep},
        authorization::{self, ManagerExecutionContext},
        config::{self, AutomationEntry},
        intake::{IntakeItem, IntakeStatus, LocalProducer},
    },
    config::Config,
    error::{Error, Result},
    model::{self, Principal, Role},
    review::ReviewAssignRequest,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeSet;

#[path = "automation_dispatch_bus.rs"]
pub(crate) mod bus_kernel;
#[path = "script_trigger_authority.rs"]
pub(crate) mod script_trigger_authority;

const DISPATCH_SCHEMA_VERSION: u32 = 1;
const MAX_PENDING_SUBJECTS: usize = 128;
const SCRIPT_TRIGGER_STATE_VERSION: u32 = 1;
const MAX_RECENT_DISPOSITIONS: usize = 20;
const MAX_RECONCILE_FACTS: usize = 16;
const MAX_PENDING_RECHECKS: usize = 8;
const MAX_SUBMISSION_PAGE: usize = 32;
const MAX_INTAKE_SOURCE_PAGE: usize = 64;
const GLOBAL_CURSOR_KEY: &str = "automation:v1:dispatch_global_cursor";
pub(super) const SYSTEM_EVENT_SOURCE_PROOF_PENDING: &str = "system_event_source_proof_pending";
type RetainedOperationScope = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    String,
);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GlobalDispatchCursor {
    schema_version: u32,
    last_entry_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DispatchState {
    schema_version: u32,
    owner_manager_id: String,
    project_id: String,
    automation_id: String,
    step: String,
    cursor: i64,
    #[serde(default)]
    hook_cursor: i64,
    #[serde(default)]
    hook_activation_cut: i64,
    #[serde(default)]
    hook_include_existing: bool,
    activation_cut: i64,
    catch_up_until: Option<i64>,
    pending_after_observation_id: i64,
    pending: Vec<PendingSubject>,
    recent: Vec<Value>,
    updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingSubject {
    cause: Value,
    reason: String,
    wake_when: Vec<String>,
    first_seen_at_ms: i64,
    last_checked_at_ms: i64,
    held: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    awaiting_hook: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    hook_fact: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    hook_event_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    hook_observation_id: Option<i64>,
}

/// Independent O1 journal consumer for the single closed ScriptRun action.
/// It shares the registered TaskSubmission source but has its own activation
/// cut so enabling scripts cannot rewind or widen ReviewDispatch coverage.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScriptTriggerState {
    schema_version: u32,
    owner_manager_id: String,
    project_id: String,
    automation_id: String,
    cursor: i64,
    activation_cut: i64,
    catch_up_until: Option<i64>,
    pending: Vec<PendingScriptTrigger>,
    recent: Vec<Value>,
    #[serde(default)]
    observed_selector_ids: Vec<String>,
    updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingScriptTrigger {
    cause: Value,
    script_id: String,
    automation_revision: i64,
    observation_id: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    consumer_context: Option<script_trigger_authority::ScriptRunConsumerContext>,
    held: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    held_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ScriptTriggerIntent {
    pub(crate) owner_manager_id: String,
    pub(crate) project_id: String,
    pub(crate) automation_id: String,
    pub(crate) automation_revision: i64,
    pub(crate) script_id: String,
    pub(crate) cause: Value,
    pub(crate) consumer_context: Option<script_trigger_authority::ScriptRunConsumerContext>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// One shared intake source snapshot, produced once per Store transaction and
/// reused by every enabled entry in that reconciliation pass.
#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) struct IntakeSnapshot {
    pub(crate) cursor: i64,
    pub(crate) high_water: i64,
    pub(crate) observation_high_water: i64,
    pub(crate) processed: usize,
    pub(crate) status: IntakeStatus,
    pub(crate) hook_commit: Option<SourceIntakeSnapshot>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) struct SourceIntakeSnapshot {
    pub(crate) cursor: i64,
    pub(crate) high_water: i64,
    pub(crate) processed: usize,
    pub(crate) status: IntakeStatus,
}

#[derive(Clone, Copy)]
struct DispatchPassContext<'a> {
    entry: &'a AutomationEntry,
    app_config: &'a Config,
    now_ms: i64,
    hook: Option<HookCommitPassContext<'a>>,
}

#[derive(Clone, Copy)]
struct HookCommitPassContext<'a> {
    settings: &'a config::HookCommitSettings,
    source_wait_reason: Option<&'a str>,
}

struct MatchedHookCommit {
    receipt: crate::automation::intake::EventReceipt,
    fact: crate::hooks::contract::HookCommitFact,
}

#[derive(Debug)]
enum SubjectResult {
    Assigned {
        operation_id: String,
        value: Value,
    },
    Pending {
        reason: String,
        wake_when: Vec<String>,
    },
    Skipped {
        reason: String,
    },
}

/// Advance the shared, durable submission intake exactly once for a caller's
/// transaction. Per-entry readers consume the retained pending journal below;
/// they never rescan the observations source directly.
pub(crate) fn reconcile_source_intake(
    tx: &Transaction<'_>,
    limit: usize,
    include_hook_commit: bool,
    now_ms: i64,
) -> Result<IntakeSnapshot> {
    let submission =
        reconcile_local_source_intake(tx, LocalProducer::TaskSubmission, limit, now_ms)?;
    let hook_commit = include_hook_commit
        .then(|| reconcile_local_source_intake(tx, LocalProducer::HookCommit, limit, now_ms))
        .transpose()?;
    Ok(IntakeSnapshot {
        cursor: submission.cursor,
        high_water: submission.high_water,
        observation_high_water: automation_intake::observed_event_high_water(tx)?,
        processed: submission.processed,
        status: submission.status,
        hook_commit,
    })
}

fn reconcile_local_source_intake(
    tx: &Transaction<'_>,
    producer: LocalProducer,
    limit: usize,
    now_ms: i64,
) -> Result<SourceIntakeSnapshot> {
    let (registration, cursor) =
        automation_intake::register_local_source(tx, producer, true, now_ms)?;
    if !registration.include_existing && registration.initial_cursor > 0 {
        return Err(Error::new(
            "AUTOMATION_INTAKE_HISTORY_UNAVAILABLE",
            format!(
                "the registered {:?} source skipped history needed by per-entry activation cuts",
                producer
            ),
        ));
    }
    let page = automation_intake::reconcile_source_page(
        tx,
        &registration.source_id,
        cursor.observation_id,
        limit.min(MAX_INTAKE_SOURCE_PAGE),
        now_ms,
    )?;
    if matches!(
        page.status,
        IntakeStatus::UnknownSource | IntakeStatus::StaleCursor
    ) {
        return Err(Error::new(
            "AUTOMATION_INTAKE_RECONCILIATION_FAILED",
            format!("shared {:?} intake returned {:?}", producer, page.status),
        ));
    }
    Ok(SourceIntakeSnapshot {
        cursor: page.cursor.ok_or_else(|| {
            Error::new(
                "AUTOMATION_INTAKE_CURSOR_MISSING",
                format!("registered {:?} source has no durable cursor", producer),
            )
        })?,
        high_water: page.high_water.ok_or_else(|| {
            Error::new(
                "AUTOMATION_INTAKE_HIGH_WATER_MISSING",
                format!("registered {:?} source has no high-water cut", producer),
            )
        })?,
        processed: page.processed,
        status: page.status,
    })
}

pub(super) fn configure_activation(
    tx: &Transaction<'_>,
    before: Option<&AutomationEntry>,
    after: &AutomationEntry,
    include_existing: bool,
    cut: i64,
    now_ms: i64,
) -> Result<()> {
    configure_script_trigger_activation(tx, before, after, include_existing, cut, now_ms)?;
    let had_review_coverage = before.is_some_and(|entry| {
        entry.enabled
            && entry.steps.contains(&AutomationStep::ReviewDispatch)
            && entry.task_submission_review_rule_selected()
    });
    let has_review_coverage = after.enabled
        && after.steps.contains(&AutomationStep::ReviewDispatch)
        && after.task_submission_review_rule_selected();
    let before_hook_source = before
        .filter(|_| had_review_coverage)
        .and_then(|entry| entry.hook_commit.as_ref())
        .map(|hook| hook.source_id.as_str());
    let after_hook_source = after
        .hook_commit
        .as_ref()
        .filter(|_| has_review_coverage)
        .map(|hook| hook.source_id.as_str());
    let hook_selection_changed = before_hook_source != after_hook_source;
    let key = config::dispatch_state_key(
        &after.owner_manager_id,
        &after.project_id,
        &after.automation_id,
    )?;
    let mut state = match load_state(tx, after)? {
        Some(state) => state,
        None if before.is_none() => empty_state(after, cut, now_ms),
        None => {
            return Err(Error::new(
                "AUTOMATION_CURSOR_MISSING",
                "existing automation entry has no durable dispatch cursor",
            ));
        }
    };

    if !has_review_coverage {
        if before.is_some_and(|entry| entry.enabled) {
            for pending in &mut state.pending {
                pending.held = true;
                pending.reason = "automation_disabled_or_step_removed".to_owned();
                pending.last_checked_at_ms = now_ms;
            }
        }
    } else if !had_review_coverage {
        state.activation_cut = cut;
        state.cursor = if include_existing { 0 } else { cut };
        state.catch_up_until = include_existing.then_some(cut);
        for pending in &mut state.pending {
            if pending_observation_id(pending) <= cut {
                pending.held = !include_existing;
                if include_existing {
                    pending.reason = "awaiting_review_assignment".to_owned();
                }
            }
        }
    } else if before.is_some_and(|entry| entry.enabled) && !after.enabled {
        for pending in &mut state.pending {
            pending.held = true;
            pending.reason = "automation_disabled".to_owned();
            pending.last_checked_at_ms = now_ms;
        }
    }

    if let Some(source_id) = after_hook_source {
        if hook_selection_changed || !had_review_coverage {
            // HookCommit and TaskSubmission have separate retained journals.
            // Rewind only this opted-in entry's submission cursor so exact
            // applied submissions can join commits under the requested cut.
            state.cursor = if include_existing { 0 } else { cut };
            state.activation_cut = cut;
            state.catch_up_until = include_existing.then_some(cut);
            state.hook_cursor = if include_existing { 0 } else { cut };
            state.hook_activation_cut = cut;
            state.hook_include_existing = include_existing;
            for pending in &mut state.pending {
                let same_selected_source = pending
                    .hook_fact
                    .as_ref()
                    .and_then(|fact| fact.get("source_id"))
                    .and_then(Value::as_str)
                    == Some(source_id);
                if pending.hook_fact.is_some() && !same_selected_source {
                    pending.held = true;
                    pending.reason = "hook_source_selection_changed".to_owned();
                    pending.last_checked_at_ms = now_ms;
                } else if pending.awaiting_hook || same_selected_source {
                    pending.held = false;
                    if pending.awaiting_hook {
                        pending.reason = "awaiting_verified_hook_commit".to_owned();
                    }
                }
            }
        }
    } else if before_hook_source.is_some() {
        for pending in &mut state.pending {
            if pending.awaiting_hook || pending.hook_fact.is_some() {
                pending.held = true;
                pending.reason = "hook_trigger_removed".to_owned();
                pending.last_checked_at_ms = now_ms;
            }
        }
    }
    state.updated_at_ms = now_ms;
    save_state(tx, &key, &state)
}

fn configure_script_trigger_activation(
    tx: &Transaction<'_>,
    before: Option<&AutomationEntry>,
    after: &AutomationEntry,
    include_existing: bool,
    cut: i64,
    now_ms: i64,
) -> Result<()> {
    let had = before.is_some_and(AutomationEntry::script_run_ready);
    let has = after.script_run_ready();
    let target_changed =
        before.and_then(|entry| entry.script_run.as_ref()) != after.script_run.as_ref();
    let route_changed = before.is_some_and(|entry| entry.event_rules != after.event_rules);
    let revision_changed = before.is_some_and(|entry| entry.revision != after.revision);
    let key = config::script_dispatch_state_key(
        &after.owner_manager_id,
        &after.project_id,
        &after.automation_id,
    )?;
    let existing_state = load_script_trigger_state(tx, after)?;
    // Do not create a new ScriptRun ledger for entries that have never selected
    // ScriptRun. Existing disabled or removed-route ledgers still go through
    // the lifecycle below and remain available for transfer/history.
    if !had && !has && existing_state.is_none() {
        return Ok(());
    }
    let mut state =
        existing_state.unwrap_or_else(|| empty_script_trigger_state(after, cut, now_ms));
    if !has {
        for pending in &mut state.pending {
            pending.held = true;
            pending.held_reason = Some("automation_disabled_or_script_route_removed".to_owned());
        }
    } else if !had || target_changed || route_changed {
        state.cursor = if include_existing { 0 } else { cut };
        state.activation_cut = cut;
        state.catch_up_until = include_existing.then_some(cut);
        state.observed_selector_ids.clear();
        for pending in &mut state.pending {
            pending.held = true;
            pending.held_reason = Some(if route_changed {
                "script_trigger_rule_changed".to_owned()
            } else {
                "script_trigger_selection_changed".to_owned()
            });
        }
    } else if revision_changed {
        // Unrelated entry edits change the signed automation revision. Keep
        // the exact event and require current entry/Task/script rights before
        // rebinding it to that revision.
        for pending in &mut state.pending {
            if !pending.held {
                pending.held = true;
                pending.held_reason = Some("automation_revision_revalidation_required".to_owned());
            }
        }
    }
    state.updated_at_ms = now_ms;
    save_script_trigger_state(tx, &key, &state)
}

/// Process one enabled entry. The caller owns the surrounding Store
/// transaction; fact cursor movement, pending reasons, Operation admission and
/// review-slot reservation therefore commit together.
pub(super) fn reconcile_entry(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    budget: usize,
    intake: IntakeSnapshot,
    config: &Config,
    now_ms: i64,
) -> Result<Value> {
    if !entry.enabled
        || !entry.steps.contains(&AutomationStep::ReviewDispatch)
        || !entry.task_submission_review_rule_selected()
    {
        return Ok(
            json!({"automation_id":entry.automation_id,"processed":0,"waiting_for":"disabled_or_unselected"}),
        );
    }
    let key = config::dispatch_state_key(
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?;
    let mut state = load_state(tx, entry)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_CURSOR_MISSING",
            "enabled automation entry has no durable dispatch cursor",
        )
    })?;
    let budget = budget.min(MAX_RECONCILE_FACTS);
    let hook_source_wait_reason = if let Some(settings) = entry.hook_commit.as_ref() {
        let reason = selected_hook_source_wait_reason(tx, config, entry, settings)?;
        let mut rewind_hook_cursor = false;
        for pending in &mut state.pending {
            if !pending.awaiting_hook
                || !(pending.reason == "awaiting_verified_hook_commit"
                    || pending.reason.starts_with("hook_source_"))
            {
                continue;
            }
            if reason.is_none() && pending.reason.starts_with("hook_source_") {
                // A fact already indexed while the source was unavailable may
                // have passed this entry's HookCommit cursor. Revisit retained
                // facts when the selected source becomes usable again.
                rewind_hook_cursor = true;
            }
            let next_reason = reason
                .clone()
                .unwrap_or_else(|| "awaiting_verified_hook_commit".to_owned());
            let unavailable = reason.is_some();
            let next_wake_when = if unavailable {
                vec!["selected_hook_source_available".to_owned()]
            } else {
                vec!["matching_hook_commit_observed".to_owned()]
            };
            if pending.reason != next_reason
                || pending.held != unavailable
                || pending.wake_when != next_wake_when
            {
                pending.reason = next_reason;
                pending.held = unavailable;
                pending.wake_when = next_wake_when;
                pending.last_checked_at_ms = now_ms;
            }
        }
        if rewind_hook_cursor {
            state.hook_cursor = if state.hook_include_existing {
                0
            } else {
                state.hook_activation_cut
            };
        }
        reason
    } else {
        None
    };
    let pass = DispatchPassContext {
        entry,
        app_config: config,
        now_ms,
        hook: entry
            .hook_commit
            .as_ref()
            .map(|settings| HookCommitPassContext {
                settings,
                source_wait_reason: hook_source_wait_reason.as_deref(),
            }),
    };
    if budget == 0 {
        state.updated_at_ms = now_ms;
        save_state(tx, &key, &state)?;
        return Ok(state_projection(&state));
    }

    let mut processed = 0usize;
    let pending_budget = budget.min(MAX_PENDING_RECHECKS);
    processed += recheck_pending(tx, entry, &mut state, pending_budget, config, now_ms)?;
    let remaining_budget = budget.saturating_sub(processed);
    if remaining_budget > 0 {
        if entry.hook_commit.is_some() {
            let hook_intake = intake.hook_commit.ok_or_else(|| {
                Error::new(
                    "AUTOMATION_HOOK_INTAKE_MISSING",
                    "selected HookCommit source was not reconciled",
                )
            })?;
            let hook_processed =
                consume_hook_commit_page(tx, &mut state, remaining_budget, hook_intake, &pass)?;
            processed += hook_processed;
            let remaining_budget = budget.saturating_sub(processed);
            if remaining_budget > 0 && state.pending.len() < MAX_PENDING_SUBJECTS {
                processed +=
                    consume_submission_page(tx, &mut state, remaining_budget, intake, &pass)?;
            }
            let remaining_budget = budget.saturating_sub(processed);
            if remaining_budget > 0 {
                processed += recheck_pending(
                    tx,
                    entry,
                    &mut state,
                    remaining_budget.min(MAX_PENDING_RECHECKS),
                    config,
                    now_ms,
                )?;
            }
        } else if state.pending.len() < MAX_PENDING_SUBJECTS {
            processed += consume_submission_page(tx, &mut state, remaining_budget, intake, &pass)?;
        }
    }
    state.updated_at_ms = now_ms;
    save_state(tx, &key, &state)?;
    let mut projection = state_projection(&state);
    projection["processed"] = json!(processed);
    projection["high_water"] = json!(intake.high_water);
    projection["intake_cursor"] = json!(intake.cursor);
    projection["intake_status"] = serde_json::to_value(intake.status)?;
    projection["hook_intake"] = serde_json::to_value(intake.hook_commit)?;
    Ok(projection)
}

pub(super) fn dispatch_state(db: &rusqlite::Connection, entry: &AutomationEntry) -> Result<Value> {
    let state = load_state(db, entry)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_CURSOR_MISSING",
            "automation entry has no durable dispatch cursor",
        )
    })?;
    Ok(state_projection(&state))
}

/// Read the existing bounded journal without creating or advancing a cursor.
pub(super) fn script_trigger_state(
    db: &rusqlite::Connection,
    entry: &AutomationEntry,
) -> Result<Value> {
    Ok(load_script_trigger_state(db, entry)?
        .map(serde_json::to_value)
        .transpose()?
        .unwrap_or(Value::Null))
}

/// Shared-reconciler entry point. A bounded global keyset cursor prevents one
/// owner/entry from monopolizing each wake. The Store must call this in its
/// writer transaction after committed changes and at startup; watch messages
/// remain lossy hints only.
pub(crate) fn reconcile(
    tx: &Transaction<'_>,
    config: &Config,
    entry_budget: usize,
    fact_budget: usize,
    now_ms: i64,
) -> Result<Value> {
    let entry_budget = entry_budget.clamp(1, 32);
    let fact_budget = fact_budget.clamp(1, MAX_RECONCILE_FACTS);
    let (entries, last_entry_key) = enabled_entry_page(tx, entry_budget)?;
    let include_hook_commit = entries.iter().any(|entry| {
        entry.hook_commit.is_some()
            || entry.selects_script_run_source_kind("controller:hooks", "git.post_commit")
    });
    let intake = reconcile_source_intake(tx, MAX_INTAKE_SOURCE_PAGE, include_hook_commit, now_ms)?;
    let mut results = Vec::with_capacity(entries.len());
    for entry in entries {
        let review = if entry.enabled && entry.steps.contains(&AutomationStep::ReviewDispatch) {
            reconcile_entry(tx, &entry, fact_budget, intake, config, now_ms)?
        } else {
            json!({"automation_id":entry.automation_id,"processed":0,"waiting_for":"review_action_not_selected"})
        };
        let script_run =
            reconcile_script_trigger_entry(tx, &entry, fact_budget, intake, config, now_ms)?;
        results.push(json!({"review_dispatch":review,"script_run":script_run}));
    }
    if let Some(last_entry_key) = last_entry_key {
        config::write_record(
            tx,
            GLOBAL_CURSOR_KEY,
            &json!({"schema_version":1,"last_entry_key":last_entry_key}),
        )?;
    }
    Ok(json!({
        "intake":intake,
        "entries":results,
        "entry_budget":entry_budget,
        "fact_budget_per_entry":fact_budget
    }))
}

fn reconcile_script_trigger_entry(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    budget: usize,
    intake: IntakeSnapshot,
    app_config: &Config,
    now_ms: i64,
) -> Result<Value> {
    if !entry.script_run_ready() {
        return Ok(json!({
            "automation_id":entry.automation_id,
            "processed":0,
            "waiting_for":"script_run_trigger_not_selected"
        }));
    }
    let key = config::script_dispatch_state_key(
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?;
    let mut state = load_script_trigger_state(tx, entry)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_CURSOR_MISSING",
            "enabled ScriptRun action has no durable O1 trigger cursor",
        )
    })?;
    revalidate_script_trigger_intents(tx, entry, &mut state, app_config, now_ms)?;
    let page_limit = budget
        .clamp(1, MAX_RECONCILE_FACTS)
        .min(crate::automation::intake::MAX_INTAKE_PAGE);
    let page = automation_intake::observed_event_page(
        tx,
        state.cursor,
        intake.observation_high_water,
        page_limit,
    )?;
    let target = state
        .catch_up_until
        .map_or(intake.observation_high_water, |cut| {
            intake.observation_high_water.min(cut)
        });
    if state.cursor >= target || state.pending.len() >= MAX_PENDING_SUBJECTS {
        finish_script_trigger_catch_up(&mut state, intake);
        state.updated_at_ms = now_ms;
        save_script_trigger_state(tx, &key, &state)?;
        return Ok(script_trigger_state_projection(&state, entry, 0, intake));
    }

    let mut processed = 0usize;
    let mut stopped_at_cut = false;
    let mut stopped_for_capacity = false;
    for event in &page {
        let observation_id = event.observation_id;
        if observation_id <= state.cursor {
            return Err(Error::new(
                "AUTOMATION_OBSERVATION_ORDER_INVALID",
                "ScriptRun observation page returned an ID at or before its cursor",
            ));
        }
        if observation_id > target {
            state.cursor = target;
            stopped_at_cut = true;
            break;
        }
        if state.pending.len() >= MAX_PENDING_SUBJECTS {
            stopped_for_capacity = true;
            break;
        }
        processed += 1;
        if !entry.selects_script_run_source_kind(&event.source_id, &event.event_kind) {
            state.cursor = observation_id;
            continue;
        }
        if event.source_id == LocalProducer::TaskSubmission.stream_id()
            && event.event_kind == LocalProducer::TaskSubmission.event_kind()
        {
            process_task_submission_script_trigger(tx, app_config, entry, &mut state, event)?;
        } else {
            process_system_event_script_trigger(tx, app_config, entry, &mut state, event)?;
        }
        state.cursor = observation_id;
    }
    if !stopped_at_cut
        && !stopped_for_capacity
        && processed == page.len()
        && page.len() < page_limit
    {
        state.cursor = target;
    }
    finish_script_trigger_catch_up(&mut state, intake);
    state.updated_at_ms = now_ms;
    save_script_trigger_state(tx, &key, &state)?;
    Ok(script_trigger_state_projection(
        &state, entry, processed, intake,
    ))
}

fn process_task_submission_script_trigger(
    tx: &Transaction<'_>,
    app_config: &Config,
    entry: &AutomationEntry,
    state: &mut ScriptTriggerState,
    event: &crate::automation::intake::ObservedEvent,
) -> Result<()> {
    let observation_id = event.observation_id;
    let Some(receipt) = automation_intake::receipt_by_observation_id(
        tx,
        LocalProducer::TaskSubmission.source_id(),
        observation_id,
    )?
    else {
        remember_script_trigger_recent(
            state,
            json!({
                "observation_id":observation_id,
                "disposition":"skipped",
                "reason":"submission_receipt_unavailable"
            }),
        );
        return Ok(());
    };
    if receipt.source_id != LocalProducer::TaskSubmission.source_id()
        || receipt.event_kind != LocalProducer::TaskSubmission.event_kind()
        || receipt.observation_id != observation_id
        || receipt.operation_id.as_deref().is_none_or(|operation_id| {
            receipt.payload["operation_id"].as_str() != Some(operation_id)
        })
    {
        remember_script_trigger_recent(
            state,
            json!({
                "observation_id":observation_id,
                "disposition":"gap",
                "reason":"submission_operation_identity_mismatch"
            }),
        );
        return Ok(());
    }
    let applied_cause = match cause_from_fact(observation_id, &receipt.payload) {
        Err(error) => {
            remember_script_trigger_recent(
                state,
                json!({
                    "observation_id":observation_id,
                    "disposition":"gap",
                    "reason":error.code.to_ascii_lowercase()
                }),
            );
            return Ok(());
        }
        Ok(cause) => cause,
    };
    let projection = script_event_projection_with_alias(tx, event, None)?;
    let bus_route_matches = bus_kernel::route_script_run_event(entry, event, projection.status)?;
    if let Some(cause) = applied_cause.as_ref()
        && bus_route_matches
        && entry.accepts_task_submission_script_run_event(&receipt, cause)
    {
        mark_observed_script_selectors(state, entry, event, projection.status)?;
        queue_applied_submission_script_trigger(tx, entry, state, cause.clone())?;
        return Ok(());
    }
    if bus_route_matches {
        process_system_event_script_trigger(tx, app_config, entry, state, event)?;
    } else {
        remember_script_trigger_recent(
            state,
            json!({
                "observation_id":observation_id,
                "disposition":if applied_cause.is_none() {"submission_not_applied"} else {"rule_unmatched"}
            }),
        );
    }
    Ok(())
}

fn queue_applied_submission_script_trigger(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    state: &mut ScriptTriggerState,
    cause: AutomationCause,
) -> Result<()> {
    let observation_id = match &cause {
        AutomationCause::AppliedSubmission { observation_id, .. } => *observation_id,
        _ => {
            return Err(Error::new(
                "AUTOMATION_CAUSE_INVALID",
                "expected an applied TaskSubmission cause",
            ));
        }
    };
    let document = submissions::document(tx, cause.id())?;
    let task_id = model::text(&document, "task_id")?;
    let task_revision = model::positive(&document, "task_revision")?;
    let attempt_id = model::text(&document, "attempt_id")?;
    let script_id = selected_script_id(entry)?;
    if state
        .pending
        .iter()
        .any(|pending| pending.script_id == script_id && pending.cause["id"] == cause.id())
    {
        remember_script_trigger_recent(
            state,
            json!({
                "observation_id":observation_id,
                "submission_ref":cause.id(),
                "disposition":"coalesced"
            }),
        );
        return Ok(());
    }
    if let Some(operation_id) = script_trigger_operation_exists(
        tx,
        entry,
        &cause,
        task_id,
        task_revision,
        attempt_id,
        &document,
        &script_id,
    )? {
        remember_script_trigger_recent(
            state,
            json!({
                "observation_id":observation_id,
                "submission_ref":cause.id(),
                "script_id":script_id,
                "disposition":"already_admitted_semantically",
                "operation_id":operation_id
            }),
        );
    } else {
        let consumer_context =
            script_trigger_authority::ScriptRunConsumerContext::from_entry(entry)?;
        state.pending.push(PendingScriptTrigger {
            cause: cause.as_json(),
            script_id,
            automation_revision: entry.revision,
            observation_id,
            consumer_context: Some(consumer_context),
            held: false,
            held_reason: None,
        });
        remember_script_trigger_recent(
            state,
            json!({
                "observation_id":observation_id,
                "submission_ref":cause.id(),
                "disposition":"queued"
            }),
        );
    }
    Ok(())
}

fn process_system_event_script_trigger(
    tx: &Transaction<'_>,
    app_config: &Config,
    entry: &AutomationEntry,
    state: &mut ScriptTriggerState,
    event: &crate::automation::intake::ObservedEvent,
) -> Result<()> {
    let projections = script_event_projections_with_alias(tx, event)?;
    if projections.is_empty() {
        remember_script_trigger_recent(
            state,
            json!({
                "observation_id":event.observation_id,
                "disposition":"occurrence_projection_unavailable"
            }),
        );
        return Ok(());
    }
    for projection in projections {
        process_system_event_script_projection(tx, app_config, entry, state, event, projection)?;
    }
    Ok(())
}

fn process_system_event_script_projection(
    tx: &Transaction<'_>,
    app_config: &Config,
    entry: &AutomationEntry,
    state: &mut ScriptTriggerState,
    event: &crate::automation::intake::ObservedEvent,
    projection: crate::automation::intake::SafeEventProjection,
) -> Result<()> {
    let script_id = selected_script_id(entry)?;
    if event_requires_occurrence_projection(event)
        && (projection.occurrence_phase.is_none() || projection.occurrence_id.is_none())
    {
        remember_script_trigger_recent(
            state,
            json!({
                "observation_id":event.observation_id,
                "disposition":"occurrence_projection_unavailable"
            }),
        );
        return Ok(());
    }
    let status_required = script_event_status_required(entry, &event.source_id, &event.event_kind);
    if status_required && projection.status.is_none() {
        remember_script_trigger_recent(
            state,
            json!({
                "observation_id":event.observation_id,
                "disposition":"status_projection_unavailable"
            }),
        );
        return Ok(());
    }
    let route_matches = bus_kernel::route_script_run_event(entry, event, projection.status)?;
    if !route_matches {
        remember_script_trigger_recent(
            state,
            json!({
                "observation_id":event.observation_id,
                "disposition":"rule_unmatched"
            }),
        );
        return Ok(());
    }
    let mut cause = system_event_cause(event, &projection, &script_id)?;
    let duplicate_pending = state
        .pending
        .iter()
        .any(|pending| pending.script_id == script_id && pending.cause["id"] == cause["id"]);
    if duplicate_pending {
        if script_cancel_event_alias_seen(state, event, &cause, &script_id) {
            // The generic Operation cancellation fact and its exact
            // ScriptRun-specific view share one phase/occurrence identity.
            // Preserve both explicitly selected routes while admitting only
            // one semantic trigger.
            mark_observed_script_selectors(state, entry, event, projection.status)?;
        }
        if event.source_id == "controller:host-lifecycle"
            && matches!(event.event_kind.as_str(), "host.exit" | "host.failed")
            && state.pending.iter().any(|pending| {
                !pending.held
                    && pending.script_id == script_id
                    && pending.cause["id"] == cause["id"]
            })
        {
            // The first view already passed the current-Manager source gate.
            // Record the matching second selector before coalescing the shared
            // host occurrence so Manager readback does not leave it pending.
            mark_observed_script_selectors(state, entry, event, projection.status)?;
        }
        remember_script_trigger_recent(
            state,
            json!({
                "observation_id":event.observation_id,
                "disposition":"coalesced"
            }),
        );
        return Ok(());
    }

    let consumer_context = script_trigger_authority::ScriptRunConsumerContext::from_entry(entry)?;
    match consumer_context.require_current_source(tx, app_config, entry, &cause) {
        Ok(context) => {
            bus_kernel::seal_retained_module_event_source(tx, entry, event, &mut cause)?;
            mark_observed_script_selectors(state, entry, event, projection.status)?;
            attach_event_task_scope(&mut cause, &context)?;
            if let Some(operation_id) =
                script_event_trigger_operation_exists(tx, entry, &cause, &script_id)?
            {
                remember_script_trigger_recent(
                    state,
                    json!({
                        "observation_id":event.observation_id,
                        "script_id":script_id,
                        "disposition":"already_admitted_semantically",
                        "operation_id":operation_id
                    }),
                );
            } else {
                queue_system_event_script_trigger(tx, entry, state, cause, script_id, None)?;
            }
        }
        Err(error) if error.code == "SCRIPT_EVENT_SELF_CAUSED" => {
            remember_script_trigger_recent(
                state,
                json!({
                    "observation_id":event.observation_id,
                    "disposition":"same_automation_feedback_suppressed"
                }),
            );
        }
        Err(error) if error.code == "SCRIPT_EVENT_SOURCE_UNAUTHORIZED" => {
            if event_can_wait_for_source_proof(tx, event)? {
                // The authenticated descriptor envelope is durable, but its
                // retained Module source proof may become available only
                // after the observation (for example, after agent.open).
                // Keep that exact cause behind the existing pending bound.
                queue_system_event_script_trigger(
                    tx,
                    entry,
                    state,
                    cause,
                    script_id,
                    Some(SYSTEM_EVENT_SOURCE_PROOF_PENDING),
                )?;
            } else {
                // Unknown, foreign, or malformed operationless rows never
                // acquire pending status from selector spelling alone.
                remember_script_trigger_recent(
                    state,
                    json!({"disposition":"source_not_authorized"}),
                );
            }
        }
        Err(error) if script_event_revalidation_error(&error) => {
            queue_system_event_script_trigger(
                tx,
                entry,
                state,
                cause,
                script_id,
                Some(&format!("event_{}", error.code.to_ascii_lowercase())),
            )?;
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

fn script_cancel_event_alias_seen(
    state: &ScriptTriggerState,
    event: &crate::automation::intake::ObservedEvent,
    cause: &Value,
    script_id: &str,
) -> bool {
    let Some(operation_id) = cause["operation_id"].as_str().filter(|id| !id.is_empty()) else {
        return false;
    };
    if cause["occurrence_phase"] != "operation_cancelled"
        || cause["occurrence_id"] != format!("operation:{operation_id}:operation_cancelled")
        || cause["status"] != "cancelled"
        || cause["script_run_id"].as_str().is_none_or(str::is_empty)
    {
        return false;
    }
    let counterpart = match (event.source_id.as_str(), event.event_kind.as_str()) {
        ("controller:operations", "operation.cancelled") => {
            ("controller:scripts", "script.cancelled")
        }
        ("controller:scripts", "script.cancelled") => {
            ("controller:operations", "operation.cancelled")
        }
        _ => return false,
    };
    state.pending.iter().any(|pending| {
        pending.script_id == script_id
            && pending.cause["id"] == cause["id"]
            && pending.cause["operation_id"].as_str() == Some(operation_id)
            && pending.cause["occurrence_phase"] == "operation_cancelled"
            && pending.cause["occurrence_id"] == cause["occurrence_id"]
            && pending.cause["status"] == "cancelled"
            && pending.cause["script_run_id"] == cause["script_run_id"]
            && pending.cause["source_id"] == counterpart.0
            && pending.cause["event_kind"] == counterpart.1
    })
}

/// Known typed event families and legacy views of normalized occurrences must
/// never degrade to observation-ID-only causes. Doing so would make a broken
/// safe projection either match a statusless selector or lose raw/normalized
/// semantic deduplication. Future source/kind pairs remain selectable through
/// the generic metadata-only Operation ACL path.
fn event_requires_occurrence_projection(event: &crate::automation::intake::ObservedEvent) -> bool {
    !raw_safe_event_aliases(event).is_empty()
        || super::module_supervisor_observation::is_lifecycle_event_source_kind(
            &event.source_id,
            &event.event_kind,
        )
        || (event.source_id == "controller" && event.event_kind == "task.submission")
        || matches!(
            (event.source_id.as_str(), event.event_kind.as_str()),
            ("controller:messages", "message.sent" | "message.reply_sent")
                | ("controller:coordination", "coordination.answer")
                | (
                    "controller:runtime",
                    "native.operation.completed" | "native.result.available"
                )
                | (
                    "controller:host-lifecycle",
                    "host.exit" | "host.failed" | "host.interrupted"
                )
                | (
                    "controller:scripts",
                    "script.run"
                        | "script.started"
                        | "script.cancelled"
                        | "script.completed"
                        | "script.failed"
                        | "script.incomplete"
                )
                | (
                    "controller:operations",
                    "operation.rejected" | "operation.outcome_unknown" | "operation.cancelled"
                )
                | ("controller:native-mcp", "native.mcp.failure")
                | (
                    "controller:hook-source",
                    "hook.source.setup" | "hook.source.revoke"
                )
        )
}

/// Only a descriptor-admitted Module metadata envelope may wait for a source
/// proof that is committed after the observation. Source/kind spelling alone
/// never makes an operationless row eligible for the hold.
pub(super) fn event_can_wait_for_source_proof(
    db: &Connection,
    event: &crate::automation::intake::ObservedEvent,
) -> Result<bool> {
    Ok(event.operation_id.is_none()
        && event.source_id.starts_with("module:")
        && automation_intake::module_event_metadata_projection(db, event)?.is_some())
}

fn hook_source_admin_project_scope(
    db: &Connection,
    app_config: &Config,
    project_id: &str,
    event: &crate::automation::intake::ObservedEvent,
) -> Result<String> {
    let unauthorized = || {
        Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "selected HookSource administration fact is outside the current project source scope",
        )
    };
    let occurrence = automation_intake::hook_source_admin_occurrence_by_observation(db, event)?
        .ok_or_else(unauthorized)?;
    if occurrence.project_id != project_id {
        return Err(unauthorized());
    }
    let source_status = super::hooks::source_status(db, app_config, &occurrence.source_id)?;
    match (event.event_kind.as_str(), source_status) {
        (
            "hook.source.setup",
            super::hooks::HookSourceStatus::Current(source)
            | super::hooks::HookSourceStatus::Revoked(source)
            | super::hooks::HookSourceStatus::Stale(source),
        ) if source.source_id == occurrence.source_id
            && source.project_id == project_id
            && source.event == "git.post_commit" =>
        {
            // Setup is a historical committed fact. Preserve it if the writer
            // drains after a later revoke or workspace-registration change.
            Ok(occurrence.project_id)
        }
        ("hook.source.revoke", super::hooks::HookSourceStatus::Revoked(source))
            if source.source_id == occurrence.source_id
                && source.project_id == project_id
                && source.event == "git.post_commit"
                && source.revoked_at_ms.is_some() =>
        {
            // The typed reader also requires the retained HookSource client to
            // be disabled, so this historical fact cannot revive its credential.
            Ok(occurrence.project_id)
        }
        _ => Err(unauthorized()),
    }
}

fn queue_system_event_script_trigger(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    state: &mut ScriptTriggerState,
    cause: Value,
    script_id: String,
    held_reason: Option<&str>,
) -> Result<()> {
    let observation_id = model::positive(&cause, "observation_id")?;
    if let Some(operation_id) =
        script_event_trigger_operation_exists(tx, entry, &cause, &script_id)?
    {
        remember_script_trigger_recent(
            state,
            json!({
                "observation_id":observation_id,
                "script_id":script_id,
                "disposition":"already_admitted_semantically",
                "operation_id":operation_id
            }),
        );
        return Ok(());
    }
    state.pending.push(PendingScriptTrigger {
        cause,
        script_id,
        automation_revision: entry.revision,
        observation_id,
        consumer_context: Some(
            script_trigger_authority::ScriptRunConsumerContext::from_entry(entry)?,
        ),
        held: held_reason.is_some(),
        held_reason: held_reason.map(str::to_owned),
    });
    remember_script_trigger_recent(
        state,
        json!({
            "observation_id":observation_id,
            "disposition":if held_reason.is_some() {"held_pending_current_authorization"} else {"queued"},
            "reason":held_reason
        }),
    );
    Ok(())
}

fn selected_script_id(entry: &AutomationEntry) -> Result<String> {
    entry
        .script_run
        .as_ref()
        .map(|settings| settings.script_id.clone())
        .ok_or_else(|| {
            Error::new(
                "AUTOMATION_RECORD_INVALID",
                "selected ScriptRun action has no script target",
            )
        })
}

fn script_event_status_required(
    entry: &AutomationEntry,
    source_id: &str,
    event_kind: &str,
) -> bool {
    entry.event_rules.as_ref().is_some_and(|rules| {
        rules.iter().any(|rule| {
            rule.action == crate::automation::event_rules::EventRuleAction::ScriptRun
                && rule.selected_source_kind() == Some((source_id, event_kind))
                && rule.status.is_some()
        })
    })
}

fn script_event_selector_id(rule: &crate::automation::event_rules::EventRule) -> Result<String> {
    let (source_id, event_kind) = rule
        .selected_source_kind()
        .ok_or_else(|| Error::new("AUTOMATION_RECORD_INVALID", "ScriptRun selector is invalid"))?;
    let status = if rule.is_task_submission_applied() {
        Some(crate::automation::event_rules::EventStatus::Applied)
    } else {
        rule.status
    };
    Ok(model::digest(
        model::canonical(&json!({
            "source_id":source_id,
            "event_kind":event_kind,
            "status":status.map(crate::automation::event_rules::EventStatus::as_str)
        }))?
        .as_bytes(),
    ))
}

fn mark_observed_script_selectors(
    state: &mut ScriptTriggerState,
    entry: &AutomationEntry,
    event: &crate::automation::intake::ObservedEvent,
    status: Option<crate::automation::event_rules::EventStatus>,
) -> Result<()> {
    for rule in entry
        .event_rules
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|rule| rule.action == crate::automation::event_rules::EventRuleAction::ScriptRun)
    {
        let legacy_applied_match = rule.is_task_submission_applied()
            && event.source_id == "controller"
            && event.event_kind == "task.submission"
            && status == Some(crate::automation::event_rules::EventStatus::Applied);
        let generic_match = rule.matches_safe_event(&event.source_id, &event.event_kind, status);
        if !legacy_applied_match && !generic_match {
            continue;
        }
        let selector_id = script_event_selector_id(rule)?;
        if !state.observed_selector_ids.contains(&selector_id) {
            if state.observed_selector_ids.len() >= crate::automation::event_rules::MAX_EVENT_RULES
            {
                return Err(Error::new(
                    "AUTOMATION_CURSOR_CORRUPT",
                    "observed ScriptRun selector ledger exceeds its configured bound",
                ));
            }
            state.observed_selector_ids.push(selector_id);
        }
    }
    Ok(())
}

fn valid_event_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_occurrence_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-/@".contains(&byte))
}

fn system_event_cause(
    event: &crate::automation::intake::ObservedEvent,
    projection: &crate::automation::intake::SafeEventProjection,
    script_id: &str,
) -> Result<Value> {
    let semantic_id = system_event_semantic_id(event.observation_id, projection)?;
    let mut cause = json!({
        "kind":"system_event",
        "id":semantic_id,
        "observation_id":event.observation_id,
        "source_id":event.source_id,
        "event_kind":event.event_kind,
        "recorded_at_ms":event.recorded_at_ms,
        "operation_id":event.operation_id,
        "status":projection.status.map(crate::automation::event_rules::EventStatus::as_str),
        "error_code":projection.error_code,
        "occurrence_phase":projection.occurrence_phase,
        "occurrence_id":projection.occurrence_id,
        "script_id":script_id
    });
    if let Some(failure_category) = projection.failure_category.as_deref() {
        cause["failure_category"] = json!(failure_category);
    }
    if let Some(failed_supervisor) = projection.failed_supervisor.as_deref() {
        cause["failed_supervisor"] = json!(failed_supervisor);
    }
    if let Some(script_run_id) = projection.script_run_id.as_deref() {
        cause["script_run_id"] = json!(script_run_id);
    }
    Ok(cause)
}

fn system_event_semantic_id(
    observation_id: i64,
    projection: &crate::automation::intake::SafeEventProjection,
) -> Result<String> {
    match (
        projection.occurrence_phase.as_deref(),
        projection.occurrence_id.as_deref(),
    ) {
        (Some(phase), Some(occurrence_id)) => Ok(model::digest(
            model::canonical(&json!({"phase":phase,"occurrence_id":occurrence_id}))?.as_bytes(),
        )),
        (None, None) => Ok(format!("observation:{observation_id}")),
        _ => Err(Error::new(
            "SCRIPT_EVENT_PROJECTION_INVALID",
            "safe event projection has an incomplete occurrence identity",
        )),
    }
}

fn script_event_projections_with_alias(
    db: &Connection,
    event: &crate::automation::intake::ObservedEvent,
) -> Result<Vec<crate::automation::intake::SafeEventProjection>> {
    if event.source_id == LocalProducer::TaskSubmission.stream_id()
        && event.event_kind == LocalProducer::TaskSubmission.event_kind()
    {
        return Ok(vec![
            automation_intake::task_submission_projection_by_observation(db, event)?,
        ]);
    }
    let direct = automation_intake::safe_event_projection(db, event)?;
    if direct.occurrence_phase.is_some() && direct.occurrence_id.is_some() {
        return Ok(vec![direct]);
    }
    if event.source_id.starts_with("module:") {
        // An arbitrary Module kind is routable only through the common
        // descriptor-admitted metadata envelope. Never fall back to an empty
        // observation-only projection for an unadmitted or malformed Module.
        return Ok(
            automation_intake::module_event_metadata_projection(db, event)?
                .into_iter()
                .collect(),
        );
    }
    let aliases = match raw_runtime_outcome_kind(db, event)? {
        Some(RawRuntimeOutcomeKind::Applied | RawRuntimeOutcomeKind::Rejected) => {
            raw_safe_event_aliases(event)
        }
        Some(RawRuntimeOutcomeKind::Unknown) => vec![(
            "controller:operations",
            "operation.outcome_unknown",
            "operation_outcome_unknown",
        )],
        Some(RawRuntimeOutcomeKind::Accepted | RawRuntimeOutcomeKind::Invalid) => Vec::new(),
        None => raw_safe_event_aliases(event),
    };
    let expected_occurrence =
        if event.source_id == "controller:host-lifecycle" && event.event_kind == "host.exit" {
            automation_intake::host_exit_occurrence_projection(db, event)?.occurrence_id
        } else {
            None
        };
    if aliases.is_empty() {
        // Future generic event kinds have no typed payload projection; they
        // may still be selected as metadata-only Operation events. Known
        // aliases and normalized event families fail closed instead of
        // degrading to an observation-ID cause.
        return if event_requires_occurrence_projection(event) {
            Ok(Vec::new())
        } else {
            Ok(vec![direct])
        };
    }
    let mut projections = Vec::new();
    for (source_id, event_kind, phase) in aliases {
        let mut statement = if event.operation_id.is_some() {
            db.prepare(
                "SELECT observation_id,source_stream_id,kind,operation_id,recorded_at_ms \
                 FROM observations WHERE operation_id=?1 AND source_stream_id=?2 AND kind=?3 \
                 ORDER BY observation_id LIMIT 32",
            )?
        } else if expected_occurrence.is_some() {
            db.prepare(
                "SELECT observation_id,source_stream_id,kind,operation_id,recorded_at_ms \
                 FROM observations WHERE source_stream_id=?1 AND kind=?2 \
                 AND json_extract(payload_json,'$.occurrence_id')=?3 \
                 ORDER BY observation_id LIMIT 32",
            )?
        } else {
            continue;
        };
        let alias_events = if let Some(operation_id) = event.operation_id.as_deref() {
            statement
                .query_map(params![operation_id, source_id, event_kind], |row| {
                    Ok(crate::automation::intake::ObservedEvent {
                        observation_id: row.get(0)?,
                        source_id: row.get(1)?,
                        event_kind: row.get(2)?,
                        operation_id: row.get(3)?,
                        recorded_at_ms: row.get(4)?,
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            statement
                .query_map(params![source_id, event_kind, expected_occurrence], |row| {
                    Ok(crate::automation::intake::ObservedEvent {
                        observation_id: row.get(0)?,
                        source_id: row.get(1)?,
                        event_kind: row.get(2)?,
                        operation_id: row.get(3)?,
                        recorded_at_ms: row.get(4)?,
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        for alias_event in alias_events {
            if let Some(operation_id) = event.operation_id.as_deref()
                && alias_event.operation_id.as_deref() != Some(operation_id)
            {
                continue;
            }
            let projection = automation_intake::safe_event_projection(db, &alias_event)?;
            if projection.occurrence_phase.as_deref() != Some(phase)
                || projection.occurrence_id.is_none()
                || expected_occurrence
                    .as_deref()
                    .is_some_and(|expected| projection.occurrence_id.as_deref() != Some(expected))
            {
                continue;
            }
            if let Some(operation_id) = event.operation_id.as_deref()
                && projection.occurrence_id.as_deref()
                    != Some(format!("operation:{operation_id}:{phase}").as_str())
            {
                continue;
            }
            if !projections
                .iter()
                .any(|prior: &crate::automation::intake::SafeEventProjection| {
                    prior.occurrence_phase == projection.occurrence_phase
                        && prior.occurrence_id == projection.occurrence_id
                })
            {
                projections.push(projection);
            }
        }
    }
    Ok(projections)
}

fn script_event_projection_with_alias(
    db: &Connection,
    event: &crate::automation::intake::ObservedEvent,
    expected_phase: Option<&str>,
) -> Result<crate::automation::intake::SafeEventProjection> {
    let projections = script_event_projections_with_alias(db, event)?;
    match expected_phase {
        Some(phase) => Ok(projections
            .into_iter()
            .find(|projection| projection.occurrence_phase.as_deref() == Some(phase))
            .unwrap_or_default()),
        None if projections.len() == 1 => Ok(projections.into_iter().next().unwrap_or_default()),
        None if projections.is_empty() => Ok(Default::default()),
        None => Err(Error::new(
            "SCRIPT_EVENT_PROJECTION_AMBIGUOUS",
            "event has multiple typed occurrence phases; retain the exact selected phase",
        )),
    }
}

fn raw_safe_event_aliases(
    event: &crate::automation::intake::ObservedEvent,
) -> Vec<(&'static str, &'static str, &'static str)> {
    match (event.source_id.as_str(), event.event_kind.as_str()) {
        ("controller", "message.send" | "coordination.send") => vec![(
            "controller:messages",
            "message.sent",
            "message_send_committed",
        )],
        ("controller", "coordination.consult") => vec![
            (
                "controller:messages",
                "message.sent",
                "message_send_committed",
            ),
            (
                "controller:coordination",
                "coordination.answer",
                "coordination_answered",
            ),
        ],
        (source_id, "runtime.outcome") if source_id.starts_with("module:") => vec![(
            "controller:runtime",
            "native.operation.completed",
            "native_outcome_terminal",
        )],
        (source_id, "runtime.result") if source_id.starts_with("module:") => vec![(
            "controller:runtime",
            "native.result.available",
            "native_result_page_recorded",
        )],
        ("controller:host-lifecycle", "host.exit") => vec![(
            "controller:host-lifecycle",
            "host.interrupted",
            "host_interruption_observed",
        )],
        _ => Vec::new(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawRuntimeOutcomeKind {
    Accepted,
    Applied,
    Rejected,
    Unknown,
    Invalid,
}

/// Classify only the persisted RuntimeOutcome identity and enum fields.
/// Arbitrary receipt details are neither fetched into Rust nor included in a
/// resulting cause.
fn raw_runtime_outcome_kind(
    db: &Connection,
    event: &crate::automation::intake::ObservedEvent,
) -> Result<Option<RawRuntimeOutcomeKind>> {
    if !event.source_id.starts_with("module:") || event.event_kind != "runtime.outcome" {
        return Ok(None);
    }
    let Some(operation_id) = event.operation_id.as_deref() else {
        return Ok(Some(RawRuntimeOutcomeKind::Invalid));
    };
    let outcome: Option<Option<String>> = db
        .query_row(
            "SELECT CASE WHEN json_type(payload_json)='object' \
               AND json_type(payload_json,'$.operation_id')='text' \
               AND json_extract(payload_json,'$.operation_id')=?4 \
               AND json_type(payload_json,'$.outcome')='text' \
             THEN json_extract(payload_json,'$.outcome') ELSE NULL END \
             FROM observations \
             WHERE observation_id=?1 AND source_stream_id=?2 AND kind=?3 AND operation_id=?4",
            params![
                event.observation_id,
                event.source_id,
                event.event_kind,
                operation_id,
            ],
            |row| row.get(0),
        )
        .optional()?;
    let Some(Some(outcome)) = outcome else {
        return Ok(Some(RawRuntimeOutcomeKind::Invalid));
    };
    let outcome = match outcome.as_str() {
        "accepted" => RawRuntimeOutcomeKind::Accepted,
        "applied" => RawRuntimeOutcomeKind::Applied,
        "rejected" => RawRuntimeOutcomeKind::Rejected,
        "unknown" => RawRuntimeOutcomeKind::Unknown,
        _ => RawRuntimeOutcomeKind::Invalid,
    };
    Ok(Some(outcome))
}

pub(crate) struct ScriptEventInvocationContext {
    pub(crate) input: Value,
    pub(crate) task_id: Option<String>,
    pub(crate) task_revision: Option<i64>,
    pub(crate) attempt_id: Option<String>,
}

/// Validate an immutable event cause during operation-link readback. Current
/// Manager/source rights are checked at admission and the start gate; this
/// historical check re-reads only the sealed immutable source proof for a
/// Module event and the existing immutable Manager-owned `event.emit` scope
/// before reconstructing safe input. Current source liveness is checked only
/// by the admission/start gates.
pub(crate) fn validate_retained_script_event_cause(
    db: &Connection,
    project_id: &str,
    cause: &Value,
) -> Result<Value> {
    if cause["kind"] != "system_event"
        || cause["script_id"].as_str().is_none_or(str::is_empty)
        || cause["script_revision"]
            .as_i64()
            .is_none_or(|revision| revision <= 0)
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "retained ScriptRun event cause has an invalid identity",
        ));
    }
    let observation_id = model::positive(cause, "observation_id")?;
    let event = automation_intake::observed_event_by_id(db, observation_id)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "retained ScriptRun event observation is missing",
        )
    })?;
    if cause["source_id"] != event.source_id
        || cause["event_kind"] != event.event_kind
        || cause["recorded_at_ms"] != event.recorded_at_ms
        || cause["operation_id"].as_str() != event.operation_id.as_deref()
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "retained ScriptRun event identity no longer matches its observation",
        ));
    }
    let projection =
        script_event_projection_with_alias(db, &event, cause["occurrence_phase"].as_str())?;
    if event_requires_occurrence_projection(&event)
        && (projection.occurrence_phase.is_none() || projection.occurrence_id.is_none())
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "retained ScriptRun event is missing its exact typed occurrence projection",
        ));
    }
    let expected_id = system_event_semantic_id(observation_id, &projection)?;
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
        || cause
            .get("script_run_id")
            .is_some_and(|value| value.as_str() != projection.script_run_id.as_deref())
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "retained ScriptRun event projection no longer matches its observation",
        ));
    }

    let task_id = cause["task_id"].as_str().map(str::to_owned);
    let task_revision = cause["task_revision"].as_i64();
    let attempt_id = cause["attempt_id"].as_str().map(str::to_owned);
    if !matches!(
        (&task_id, task_revision, &attempt_id),
        (None, None, None) | (Some(_), Some(_), Some(_))
    ) {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "retained ScriptRun event Task/Attempt scope is incomplete",
        ));
    }
    let owner_manager_id = cause["automation_consumer"]["owner_manager_id"]
        .as_str()
        .filter(|value| !value.is_empty());
    let module_event_scope = if event.source_id.starts_with("module:") {
        let owner_manager_id = owner_manager_id.ok_or_else(|| {
            Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "retained Module ScriptRun event has no owner attribution",
            )
        })?;
        Some(bus_kernel::validate_retained_module_event_source(
            db,
            project_id,
            owner_manager_id,
            &event,
            cause,
        )?)
    } else {
        None
    };
    if let Some(operation_id) = event.operation_id.as_deref() {
        let operation_scope: Option<RetainedOperationScope> = db
            .query_row(
                "SELECT task_id,attempt_id,binding_id,binding_generation,caller_id \
                 FROM operations WHERE operation_id=?1",
                [operation_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            operation_task,
            operation_attempt,
            operation_binding_id,
            operation_binding_generation,
            caller_id,
        )) = operation_scope
        else {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "retained ScriptRun event Operation is missing",
            ));
        };
        if event.source_id == crate::store::MANAGER_EVENT_SOURCE_STREAM {
            let owner_manager_id = owner_manager_id.ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "retained Manager event ScriptRun has no owner attribution",
                )
            })?;
            bus_kernel::require_manager_event_scope_for_retained(
                db,
                &event,
                operation_id,
                project_id,
                owner_manager_id,
                &caller_id,
            )
            .map_err(|_| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "retained Manager event no longer matches its immutable scope",
                )
            })?;
        }
        let module_operation_has_retained_source_scope =
            if let Some(module_scope) = module_event_scope.as_ref() {
                let source_scope_matches = operation_task.as_deref()
                    == module_scope.source_task_id.as_deref()
                    && operation_attempt.as_deref() == module_scope.source_attempt_id.as_deref();
                let action_scope_matches = task_id.as_deref() == module_scope.task_id.as_deref()
                    && task_revision == module_scope.task_revision
                    && attempt_id.as_deref() == module_scope.attempt_id.as_deref();
                if operation_binding_id.as_deref() != Some(module_scope.binding_id.as_str())
                    || operation_binding_generation != Some(module_scope.binding_generation)
                    || !source_scope_matches
                    || !action_scope_matches
                    || owner_manager_id != Some(caller_id.as_str())
                {
                    return Err(Error::new(
                        "AUTOMATION_LINK_CORRUPT",
                        "retained Module event Operation is outside its binding scope",
                    ));
                }
                true
            } else {
                false
            };
        if !module_operation_has_retained_source_scope {
            match (
                operation_task.as_deref(),
                operation_attempt.as_deref(),
                task_id.as_deref(),
                task_revision,
                attempt_id.as_deref(),
            ) {
                (
                    Some(operation_task),
                    Some(operation_attempt),
                    Some(task_id),
                    Some(task_revision),
                    Some(attempt_id),
                ) if operation_task == task_id && operation_attempt == attempt_id => {
                    let scope: Option<(String, i64, String)> = db
                        .query_row(
                            "SELECT a.task_id,a.task_revision,t.project_id FROM attempts a \
                         JOIN tasks t ON t.task_id=a.task_id WHERE a.attempt_id=?1",
                            [attempt_id],
                            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                        )
                        .optional()?;
                    if !scope.is_some_and(|(attempt_task, attempt_revision, task_project)| {
                        attempt_task == operation_task
                            && attempt_revision == task_revision
                            && task_project == project_id
                    }) {
                        return Err(Error::new(
                            "AUTOMATION_LINK_CORRUPT",
                            "retained ScriptRun event Task/Attempt no longer matches its project scope",
                        ));
                    }
                }
                (None, None, None, None, None) => {}
                (Some(operation_task), None, None, None, None) => {
                    let task_project: Option<String> = db
                        .query_row(
                            "SELECT project_id FROM tasks WHERE task_id=?1",
                            [operation_task],
                            |row| row.get(0),
                        )
                        .optional()?;
                    if task_project.as_deref() != Some(project_id) {
                        return Err(Error::new(
                            "AUTOMATION_LINK_CORRUPT",
                            "retained event-only ScriptRun Task reference is outside its project",
                        ));
                    }
                }
                _ => {
                    return Err(Error::new(
                        "AUTOMATION_LINK_CORRUPT",
                        "retained ScriptRun event Task/Attempt differs from its Operation",
                    ));
                }
            }
        }
    } else if let Some(module_scope) = module_event_scope.as_ref() {
        if task_id.as_deref() != module_scope.task_id.as_deref()
            || task_revision != module_scope.task_revision
            || attempt_id.as_deref() != module_scope.attempt_id.as_deref()
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "retained Module event Task/Attempt differs from its binding scope",
            ));
        }
    } else if event.source_id == "controller:hooks" && event.event_kind == "git.post_commit" {
        let fact = automation_intake::hook_commit_fact_by_observation(db, observation_id)?
            .ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "retained ScriptRun HookCommit no longer has its verified O1 receipt",
                )
            })?;
        if fact.project_id != project_id {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "retained ScriptRun HookCommit belongs to another project",
            ));
        }
    } else if event.source_id == "controller:hook-source"
        && matches!(
            event.event_kind.as_str(),
            "hook.source.setup" | "hook.source.revoke"
        )
    {
        let occurrence = automation_intake::hook_source_admin_occurrence_by_observation(
            db, &event,
        )?
        .ok_or_else(|| {
            Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "retained HookSource administration event no longer matches its source record",
            )
        })?;
        if occurrence.project_id != project_id {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "retained HookSource administration event belongs to another project",
            ));
        }
    } else if event.source_id != "controller:host-lifecycle"
        || !((matches!(event.event_kind.as_str(), "host.exit" | "host.interrupted")
            && projection.occurrence_phase.as_deref() == Some("host_interruption_observed"))
            || (matches!(event.event_kind.as_str(), "host.exit" | "host.failed")
                && projection.occurrence_phase.as_deref() == Some("host_terminal_exit_observed")))
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "retained ScriptRun event has no recognized immutable source scope",
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
    if let Some(operation_id) = event.operation_id {
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
    if let Some(task_id) = task_id {
        input["task_id"] = json!(task_id);
    }
    if let Some(task_revision) = task_revision {
        input["task_revision"] = json!(task_revision);
    }
    if let Some(attempt_id) = attempt_id {
        input["attempt_id"] = json!(attempt_id);
    }
    Ok(input)
}

/// Legacy GM-scoped fixture helper. Production ScriptRun dispatch uses the
/// persisted owner-scoped consumer context above instead.
#[cfg(test)]
pub(crate) fn script_event_invocation_context(
    db: &Connection,
    app_config: &Config,
    entry: &AutomationEntry,
    cause: &Value,
) -> Result<ScriptEventInvocationContext> {
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
        script_event_projection_with_alias(db, &event, cause["occurrence_phase"].as_str())?;
    if event_requires_occurrence_projection(&event)
        && (projection.occurrence_phase.is_none() || projection.occurrence_id.is_none())
    {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "selected event has no exact typed occurrence projection",
        ));
    }
    let expected_event_id = system_event_semantic_id(observation_id, &projection)?;
    if cause["id"] != expected_event_id
        || cause["occurrence_phase"].as_str() != projection.occurrence_phase.as_deref()
        || cause["occurrence_id"].as_str() != projection.occurrence_id.as_deref()
        || cause["status"].as_str()
            != projection
                .status
                .map(crate::automation::event_rules::EventStatus::as_str)
        || cause["error_code"].as_str() != projection.error_code.as_deref()
        || cause["failure_category"].as_str() != projection.failure_category.as_deref()
        || cause["failed_supervisor"].as_str() != projection.failed_supervisor.as_deref()
        || cause
            .get("script_run_id")
            .is_some_and(|value| value.as_str() != projection.script_run_id.as_deref())
        || !entry.accepts_script_run_event(&event.source_id, &event.event_kind, projection.status)
    {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "selected event projection or configured status no longer matches its cause",
        ));
    }
    let expected_id = match (
        projection.occurrence_phase.as_deref(),
        projection.occurrence_id.as_deref(),
    ) {
        (Some(phase), Some(occurrence_id)) => model::digest(
            model::canonical(&json!({"phase":phase,"occurrence_id":occurrence_id}))?.as_bytes(),
        ),
        _ => format!("observation:{observation_id}"),
    };
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
        || cause
            .get("script_run_id")
            .is_some_and(|value| value.as_str() != projection.script_run_id.as_deref())
    {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "event projection no longer matches the exact retained cause",
        ));
    }
    let status = projection.status;
    if !entry.accepts_script_run_event(&event.source_id, &event.event_kind, status) {
        return Err(Error::new(
            "SCRIPT_EVENT_RULE_CHANGED",
            "current ScriptRun event selector no longer matches this event",
        ));
    }
    authorization::require_registered_manager(db, &entry.owner_manager_id)?;
    let manager = Principal {
        link_id: "internal-script-event-authorization".to_owned(),
        client_id: entry.owner_manager_id.clone(),
        role: Role::Manager,
    };
    super::gm::require_authority(db, &manager)?;

    let mut task_id = None;
    let mut task_revision = None;
    let mut attempt_id = None;
    let mut project_id = entry.project_id.clone();
    let mut operation_id = None;
    if let Some(event_operation_id) = event.operation_id.as_deref() {
        if !super::operation_visible_to(db, &manager, event_operation_id)? {
            return Err(Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "linked Operation is outside the current Manager's visibility",
            ));
        }
        let operation_scope: Option<(Option<String>, Option<String>, String)> = db
            .query_row(
                "SELECT task_id,attempt_id,effective_request_json FROM operations WHERE operation_id=?1",
                [event_operation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((operation_task_id, operation_attempt_id, effective_json)) = operation_scope
        else {
            return Err(Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "linked Operation no longer exists",
            ));
        };
        if event_is_script_feedback_from_same_automation(
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
                let attempt = tasks::get_attempt(db, &operation_attempt)?;
                let derived_task = model::text(&attempt, "task_id")?;
                let task = tasks::get_task(db, derived_task)?;
                if operation_task != derived_task || task["project_id"] != entry.project_id {
                    return Err(Error::new(
                        "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                        "linked Operation Task is outside the configured project",
                    ));
                }
                let revision = model::positive(&attempt, "task_revision")?;
                let (current_task, current_attempt) =
                    super::scripts::require_run_scope(db, &manager, &operation_attempt, revision)?;
                if model::text(&current_task, "task_id")? != operation_task
                    || current_attempt["attempt_id"] != operation_attempt
                {
                    return Err(Error::new(
                        "STALE_ATTEMPT",
                        "event Task/Attempt is no longer the exact current Manager scope",
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
            (Some(operation_task), None) => {
                // This event has an ordinary project-visible Task link but no
                // real Attempt authority. Keep the script invocation
                // event-only; in particular, do not fabricate the Task's
                // current revision or grant TaskOwnerMessage.
                let task = tasks::get_task(db, &operation_task)?;
                if task["project_id"] != entry.project_id {
                    return Err(Error::new(
                        "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                        "linked Operation Task is outside the configured project",
                    ));
                }
            }
            (None, None) => {}
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
        match super::hooks::source_status(db, app_config, &fact.source_id)? {
            super::hooks::HookSourceStatus::Current(source)
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
        project_id = hook_source_admin_project_scope(db, app_config, &entry.project_id, &event)?;
    } else if event.source_id == "controller:host-lifecycle"
        && ((matches!(event.event_kind.as_str(), "host.exit" | "host.interrupted")
            && projection.occurrence_phase.as_deref() == Some("host_interruption_observed"))
            || (matches!(event.event_kind.as_str(), "host.exit" | "host.failed")
                && projection.occurrence_phase.as_deref() == Some("host_terminal_exit_observed")))
    {
        // The typed host lifecycle projection is global metadata visible to an
        // active Manager. It carries no Task, binding, or private payload.
    } else {
        return Err(Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "event has no operation, verified HookSource, or typed host ACL",
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
    if let Some(status) = status {
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
    Ok(ScriptEventInvocationContext {
        input,
        task_id,
        task_revision,
        attempt_id,
    })
}

fn attach_event_task_scope(
    cause: &mut Value,
    context: &ScriptEventInvocationContext,
) -> Result<()> {
    match (
        context.task_id.as_deref(),
        context.task_revision,
        context.attempt_id.as_deref(),
    ) {
        (Some(task_id), Some(task_revision), Some(attempt_id)) => {
            if cause.get("task_id").is_some_and(|value| !value.is_null())
                && (cause["task_id"] != task_id
                    || cause["task_revision"] != task_revision
                    || cause["attempt_id"] != attempt_id)
            {
                return Err(Error::new(
                    "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                    "event cause Task/Attempt scope changed",
                ));
            }
            cause["task_id"] = json!(task_id);
            cause["task_revision"] = json!(task_revision);
            cause["attempt_id"] = json!(attempt_id);
        }
        (None, None, None) => {
            if ["task_id", "task_revision", "attempt_id"]
                .iter()
                .any(|field| cause.get(*field).is_some_and(|value| !value.is_null()))
            {
                return Err(Error::new(
                    "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                    "event cause retained a Task/Attempt scope no longer supplied by its source",
                ));
            }
        }
        _ => {
            return Err(Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "event scope is not an all-present Task/Attempt triple",
            ));
        }
    }
    Ok(())
}

fn event_is_script_feedback_from_same_automation(
    db: &Connection,
    operation_id: &str,
    effective_json: &str,
    entry: &AutomationEntry,
) -> Result<bool> {
    let effective: Value = serde_json::from_str(effective_json).map_err(|_| {
        Error::new(
            "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
            "linked Operation authority record is malformed",
        )
    })?;
    let parent_operation_id = if effective["script_invocation"].is_object() {
        let link = &effective["script_invocation"];
        if link["schema_version"] != 1
            || link["operation_id"] != operation_id
            || link["action"] != "message.send"
            || link["grant"] != "task_owner_message"
        {
            return Err(Error::new(
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED",
                "linked script effect does not match its exact message Operation",
            ));
        }
        link["cause"]["script_run_operation_id"]
            .as_str()
            .map(str::to_owned)
    } else {
        Some(operation_id.to_owned())
    };
    let Some(parent_operation_id) = parent_operation_id else {
        return Ok(false);
    };
    let Some(parent) = authorization::operation_link(db, &parent_operation_id)? else {
        return Ok(false);
    };
    Ok(parent.action == "script.run"
        && parent.automation_id == entry.automation_id
        && parent.project_id == entry.project_id)
}

fn script_event_trigger_operation_exists(
    tx: &Connection,
    entry: &AutomationEntry,
    cause: &Value,
    script_id: &str,
) -> Result<Option<String>> {
    let request_id = authorization::script_run_event_request_id(
        &entry.automation_id,
        model::text(cause, "id")?,
        script_id,
    )?;
    let existing: Option<String> = tx
        .query_row(
            "SELECT operation_id FROM operations WHERE caller_id=?1 AND method='script.run' AND client_request_id=?2",
            params![authorization::AUTOMATION_TECHNICAL_REQUESTER_ID, request_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(operation_id) = existing else {
        return Ok(None);
    };
    let link = authorization::operation_link(tx, &operation_id)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "semantic event ScriptRun is retained without its validated on-behalf link",
        )
    })?;
    if link.action != "script.run"
        || link.automation_id != entry.automation_id
        || link.project_id != entry.project_id
        || !authorization::script_run_causes_semantically_match(&link.cause, cause)
        || link.cause["script_id"] != script_id
    {
        return Err(Error::new(
            "REQUEST_ID_CONFLICT",
            "semantic ScriptRun request is retained under another event cause",
        ));
    }
    Ok(Some(operation_id))
}

fn script_event_revalidation_error(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "FORBIDDEN"
            | "UNAUTHORIZED"
            | "NOT_FOUND"
            | "STALE_ATTEMPT"
            | "ATTEMPT_SCOPE_STALE"
            | "SCRIPT_EVENT_RULE_CHANGED"
            | "SCRIPT_EVENT_SOURCE_REVOKED"
            | "SCRIPT_EVENT_SOURCE_UNAUTHORIZED"
    )
}

#[allow(clippy::too_many_arguments)]
fn script_trigger_operation_exists(
    tx: &Connection,
    entry: &AutomationEntry,
    cause: &AutomationCause,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    submission: &Value,
    script_id: &str,
) -> Result<Option<String>> {
    let request_id = authorization::script_run_request_id(
        &entry.automation_id,
        task_id,
        task_revision,
        attempt_id,
        cause.id(),
        script_id,
    )?;
    let existing: Option<String> = tx
        .query_row(
            "SELECT operation_id FROM operations WHERE caller_id=?1 AND method='script.run' AND client_request_id=?2",
            params![authorization::AUTOMATION_TECHNICAL_REQUESTER_ID, request_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(operation_id) = existing else {
        return Ok(None);
    };
    let link = authorization::operation_link(tx, &operation_id)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "semantic ScriptRun request is retained without its validated on-behalf link",
        )
    })?;
    let semantic_cause = cause.as_json();
    if link.action != "script.run"
        || link.automation_id != entry.automation_id
        || link.project_id != entry.project_id
        || link.cause["kind"] != "applied_submission"
        || link.cause["operation_id"] != semantic_cause["operation_id"]
        || link.cause["id"] != cause.id()
        || link.cause["script_id"] != script_id
        || link.cause["task_id"] != task_id
        || link.cause["task_revision"] != task_revision
        || link.cause["attempt_id"] != attempt_id
        || link.cause["candidate_ref"] != submission["candidate_ref"]
    {
        return Err(Error::new(
            "REQUEST_ID_CONFLICT",
            "semantic ScriptRun request is retained under a different applied submission",
        ));
    }
    Ok(Some(operation_id))
}

fn script_trigger_admission_retained(
    db: &Connection,
    intent: &ScriptTriggerIntent,
) -> Result<bool> {
    let Some(entry) = config::load_entry(
        db,
        &intent.owner_manager_id,
        &intent.project_id,
        &intent.automation_id,
    )?
    else {
        return Ok(false);
    };
    match intent.cause["kind"].as_str() {
        Some("applied_submission") => {
            let cause = cause_from_json(&intent.cause)?;
            let submission = submissions::document(db, cause.id())?;
            Ok(script_trigger_operation_exists(
                db,
                &entry,
                &cause,
                model::text(&submission, "task_id")?,
                model::positive(&submission, "task_revision")?,
                model::text(&submission, "attempt_id")?,
                &submission,
                &intent.script_id,
            )?
            .is_some())
        }
        Some("system_event") => Ok(script_event_trigger_operation_exists(
            db,
            &entry,
            &intent.cause,
            &intent.script_id,
        )?
        .is_some()),
        _ => Err(Error::new(
            "AUTOMATION_CURSOR_CORRUPT",
            "pending ScriptRun cause has an unsupported kind",
        )),
    }
}

/// Return a bounded batch of durable, unheld ScriptRun intents. Selection is
/// from the existing O1 metadata ledger; no independent timer or event source
/// is introduced.
pub(crate) fn pending_script_triggers(
    db: &rusqlite::Connection,
    limit: usize,
) -> Result<Vec<ScriptTriggerIntent>> {
    let limit = limit.clamp(1, 32);
    let mut statement = db.prepare(
        "SELECT key FROM meta WHERE key LIKE 'automation:v1:script_dispatch:%' \
         AND EXISTS(SELECT 1 FROM json_each(value_json,'$.record.pending') AS pending \
                    WHERE json_extract(pending.value,'$.held')=0) \
         ORDER BY key LIMIT ?1",
    )?;
    let keys = statement
        .query_map([limit as i64], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut intents = Vec::new();
    for key in keys {
        let value =
            config::read_record(db, &key, "ScriptRun trigger cursor")?.ok_or_else(|| {
                Error::new(
                    "AUTOMATION_CURSOR_MISSING",
                    "ScriptRun trigger cursor disappeared",
                )
            })?;
        let state: ScriptTriggerState = serde_json::from_value(value).map_err(|_| {
            Error::new(
                "AUTOMATION_CURSOR_CORRUPT",
                "ScriptRun trigger cursor fields are invalid",
            )
        })?;
        validate_script_trigger_state(&state)?;
        for pending in state.pending.iter().filter(|pending| !pending.held) {
            intents.push(ScriptTriggerIntent {
                owner_manager_id: state.owner_manager_id.clone(),
                project_id: state.project_id.clone(),
                automation_id: state.automation_id.clone(),
                automation_revision: pending.automation_revision,
                script_id: pending.script_id.clone(),
                cause: pending.cause.clone(),
                consumer_context: pending.consumer_context.clone(),
            });
            if intents.len() >= limit {
                return Ok(intents);
            }
        }
    }
    Ok(intents)
}

pub(crate) fn finish_script_trigger(
    db: &rusqlite::Connection,
    intent: &ScriptTriggerIntent,
    disposition: &str,
    details: Value,
    now_ms: i64,
) -> Result<()> {
    let key = config::script_dispatch_state_key(
        &intent.owner_manager_id,
        &intent.project_id,
        &intent.automation_id,
    )?;
    let state = load_script_trigger_state(
        db,
        &AutomationEntry::new(
            &intent.owner_manager_id,
            &intent.project_id,
            &intent.automation_id,
            now_ms,
        ),
    )?;
    let Some(mut state) = state else {
        if script_trigger_admission_retained(db, intent)? {
            return Ok(());
        }
        return Err(Error::new(
            "AUTOMATION_CURSOR_MISSING",
            "ScriptRun trigger cursor disappeared before pending intent readback",
        ));
    };
    let prior_len = state.pending.len();
    state.pending.retain(|pending| {
        pending.held
            || pending.automation_revision != intent.automation_revision
            || pending.script_id != intent.script_id
            || pending.cause != intent.cause
    });
    if state.pending.len() == prior_len {
        return Ok(());
    }
    remember_script_trigger_recent(
        &mut state,
        json!({
            "observation_id":intent.cause["observation_id"],
            "submission_ref":intent.cause["id"],
            "script_id":intent.script_id,
            "cause":intent.cause,
            "disposition":disposition,
            "details":details
        }),
    );
    state.updated_at_ms = now_ms;
    save_script_trigger_state(db, &key, &state)
}

pub(crate) fn hold_script_trigger(
    db: &rusqlite::Connection,
    intent: &ScriptTriggerIntent,
    reason: &str,
    failed_script_revision: Option<i64>,
    details: Value,
    now_ms: i64,
) -> Result<()> {
    let key = config::script_dispatch_state_key(
        &intent.owner_manager_id,
        &intent.project_id,
        &intent.automation_id,
    )?;
    let entry = AutomationEntry::new(
        &intent.owner_manager_id,
        &intent.project_id,
        &intent.automation_id,
        now_ms,
    );
    let mut state = load_script_trigger_state(db, &entry)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_CURSOR_MISSING",
            "ScriptRun trigger cursor disappeared",
        )
    })?;
    let Some(pending) = state.pending.iter_mut().find(|pending| {
        pending.automation_revision == intent.automation_revision
            && pending.script_id == intent.script_id
            && pending.cause == intent.cause
            && !pending.held
    }) else {
        return Ok(());
    };
    pending.held = true;
    pending.held_reason = Some(if reason == "SCRIPT_REGISTRY_DAMAGED" {
        script_registry_hold_reason(failed_script_revision.ok_or_else(|| {
            Error::new(
                "SCRIPT_TRIGGER_REVISION_UNKNOWN",
                "damaged script trigger has no captured revision identity",
            )
        })?)
    } else {
        format!("admission_revalidation:{}", reason.to_ascii_lowercase())
    });
    remember_script_trigger_recent(
        &mut state,
        json!({
            "observation_id":intent.cause["observation_id"],
            "submission_ref":intent.cause["id"],
            "script_id":intent.script_id,
            "cause":intent.cause,
            "disposition":"blocked_pending_current_authorization",
            "details":details
        }),
    );
    state.updated_at_ms = now_ms;
    save_script_trigger_state(db, &key, &state)
}

fn enabled_entry_page(
    db: &rusqlite::Connection,
    limit: usize,
) -> Result<(Vec<AutomationEntry>, Option<String>)> {
    let prefix = "automation:v1:entry:";
    let pattern = format!("{prefix}%");
    let cursor = config::read_record(db, GLOBAL_CURSOR_KEY, "automation dispatcher cursor")?
        .map(|value| {
            serde_json::from_value::<GlobalDispatchCursor>(value).map_err(|_| {
                Error::new(
                    "AUTOMATION_CURSOR_CORRUPT",
                    "global automation cursor fields are invalid",
                )
            })
        })
        .transpose()?;
    if cursor.as_ref().is_some_and(|cursor| {
        cursor.schema_version != 1 || !cursor.last_entry_key.starts_with(prefix)
    }) {
        return Err(Error::new(
            "AUTOMATION_CURSOR_CORRUPT",
            "global automation cursor version or key is invalid",
        ));
    }
    let after = cursor.map_or_else(|| prefix.to_owned(), |cursor| cursor.last_entry_key);
    let mut keys = select_enabled_entry_keys(db, &pattern, &after, limit)?;
    if keys.len() < limit {
        let wrapped =
            select_enabled_entry_keys_before(db, &pattern, prefix, &after, limit - keys.len())?;
        keys.extend(wrapped);
    }
    if keys.is_empty() {
        keys = select_enabled_entry_keys(db, &pattern, prefix, limit)?;
    }
    let last_key = keys.last().cloned();
    let mut entries = Vec::with_capacity(keys.len());
    for key in keys {
        let raw: String =
            db.query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
                row.get(0)
            })?;
        let sealed: Value = serde_json::from_str(&raw).map_err(|_| {
            Error::new(
                "AUTOMATION_RECORD_CORRUPT",
                "automation entry JSON is invalid",
            )
        })?;
        let value = config::open_record(sealed, "automation entry")?;
        let entry: AutomationEntry = serde_json::from_value(value).map_err(|_| {
            Error::new(
                "AUTOMATION_RECORD_CORRUPT",
                "automation entry fields are invalid",
            )
        })?;
        config::validate_entry(&entry)?;
        if entry.enabled
            && (entry.steps.contains(&AutomationStep::ReviewDispatch) || entry.script_run_ready())
        {
            entries.push(entry);
        }
    }
    Ok((entries, last_key))
}

fn select_enabled_entry_keys(
    db: &rusqlite::Connection,
    pattern: &str,
    after: &str,
    limit: usize,
) -> Result<Vec<String>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut statement = db.prepare(
        "SELECT key FROM meta WHERE key LIKE ?1 AND key>?2 \
         AND json_extract(value_json,'$.record.enabled')=1 \
         AND EXISTS(SELECT 1 FROM json_each(value_json,'$.record.steps') AS step \
                    WHERE step.value IN ('review_dispatch','script_run')) \
         ORDER BY key LIMIT ?3",
    )?;
    Ok(statement
        .query_map(params![pattern, after, limit as i64], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn select_enabled_entry_keys_before(
    db: &rusqlite::Connection,
    pattern: &str,
    prefix: &str,
    before: &str,
    limit: usize,
) -> Result<Vec<String>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut statement = db.prepare(
        "SELECT key FROM meta WHERE key LIKE ?1 AND key>?2 AND key<=?3 \
         AND json_extract(value_json,'$.record.enabled')=1 \
         AND EXISTS(SELECT 1 FROM json_each(value_json,'$.record.steps') AS step \
                    WHERE step.value IN ('review_dispatch','script_run')) \
         ORDER BY key LIMIT ?4",
    )?;
    Ok(statement
        .query_map(params![pattern, prefix, before, limit as i64], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn consume_submission_page(
    tx: &Transaction<'_>,
    state: &mut DispatchState,
    budget: usize,
    intake: IntakeSnapshot,
    pass: &DispatchPassContext<'_>,
) -> Result<usize> {
    let page_limit = budget.min(MAX_SUBMISSION_PAGE);
    let page = automation_intake::pending_page(
        tx,
        LocalProducer::TaskSubmission.source_id(),
        state.cursor,
        page_limit,
    )?;
    if page.status == IntakeStatus::UnknownSource {
        return Err(Error::new(
            "AUTOMATION_INTAKE_SOURCE_MISSING",
            "shared submission intake has not been registered",
        ));
    }
    if page.status == IntakeStatus::StaleCursor {
        // A future-only entry may intentionally start at a global observation
        // cut ahead of the source-specific intake cursor. Wait until intake
        // reaches that cut; never rewind the per-entry cursor.
        return Ok(0);
    }
    let intake_cursor = page.cursor.ok_or_else(|| {
        Error::new(
            "AUTOMATION_INTAKE_CURSOR_MISSING",
            "registered submission source has no durable cursor",
        )
    })?;
    if intake_cursor != intake.cursor {
        return Err(Error::new(
            "AUTOMATION_INTAKE_CURSOR_CHANGED",
            "per-entry journal read differs from the shared intake snapshot",
        ));
    }
    let target = state
        .catch_up_until
        .map_or(intake_cursor, |cut| intake_cursor.min(cut));
    if state.cursor >= target {
        finish_activation_catch_up(state, intake);
        return Ok(0);
    }

    let mut processed = 0usize;
    let mut stopped_at_cut = false;
    let mut stopped_for_capacity = false;
    for item in &page.items {
        let observation_id = intake_item_observation_id(item);
        if observation_id <= state.cursor {
            return Err(Error::new(
                "AUTOMATION_INTAKE_JOURNAL_ORDER_INVALID",
                "pending journal returned an observation at or before the consumer cursor",
            ));
        }
        if observation_id > target {
            // The first returned observation after the activation cut proves
            // that all source rows through the cut are already journaled.
            state.cursor = target;
            stopped_at_cut = true;
            break;
        }
        if state.pending.len() >= MAX_PENDING_SUBJECTS {
            stopped_for_capacity = true;
            break;
        }
        processed += 1;
        match item {
            IntakeItem::Gap(gap) => {
                if gap.source_id != LocalProducer::TaskSubmission.source_id()
                    || gap.observation_id != observation_id
                {
                    return Err(Error::new(
                        "AUTOMATION_INTAKE_JOURNAL_IDENTITY_INVALID",
                        "pending gap does not match the registered submission source",
                    ));
                }
                remember_recent(
                    state,
                    json!({
                        "observation_id":observation_id,
                        "source_id":gap.source_id,
                        "source_event_key":gap.source_event_key,
                        "disposition":"gap",
                        "reason":gap.reason
                    }),
                );
            }
            IntakeItem::Receipt(receipt) => {
                if receipt.source_id != LocalProducer::TaskSubmission.source_id()
                    || receipt.event_kind != LocalProducer::TaskSubmission.event_kind()
                {
                    return Err(Error::new(
                        "AUTOMATION_INTAKE_JOURNAL_IDENTITY_INVALID",
                        "pending receipt is outside the registered submission source",
                    ));
                }
                let fact_operation_id = receipt.operation_id.as_deref();
                if fact_operation_id.is_none()
                    || receipt.payload.get("operation_id").and_then(Value::as_str)
                        != fact_operation_id
                {
                    remember_recent(
                        state,
                        json!({
                            "observation_id":observation_id,
                            "source_event_key":receipt.source_event_key,
                            "disposition":"gap",
                            "reason":"submission_operation_identity_mismatch"
                        }),
                    );
                    state.cursor = observation_id;
                    continue;
                }
                match cause_from_fact(observation_id, &receipt.payload) {
                    Err(error) => remember_recent(
                        state,
                        json!({
                            "observation_id":observation_id,
                            "source_event_key":receipt.source_event_key,
                            "disposition":"gap",
                            "reason":error.code.to_ascii_lowercase()
                        }),
                    ),
                    Ok(None) => remember_recent(
                        state,
                        json!({
                            "observation_id":observation_id,
                            "source_event_key":receipt.source_event_key,
                            "disposition":"skipped",
                            "reason":"submission_not_applied"
                        }),
                    ),
                    Ok(Some(cause))
                        if !pass
                            .entry
                            .accepts_task_submission_review_event(receipt, &cause) =>
                    {
                        remember_recent(
                            state,
                            json!({
                                "observation_id":observation_id,
                                "submission_ref":cause.id(),
                                "source_event_key":receipt.source_event_key,
                                "disposition":"rule_unmatched"
                            }),
                        );
                    }
                    Ok(Some(cause))
                        if state
                            .pending
                            .iter()
                            .any(|pending| pending_cause_id(pending) == cause.id()) =>
                    {
                        // Duplicate producer observations for the same
                        // immutable submission share one retained pending
                        // subject and therefore one later review attempt.
                        remember_recent(
                            state,
                            json!({
                                "observation_id":observation_id,
                                "submission_ref":cause.id(),
                                "source_event_key":receipt.source_event_key,
                                "disposition":"coalesced"
                            }),
                        );
                    }
                    Ok(Some(cause)) => {
                        if let Some(hook) = pass.hook {
                            match matching_hook_commit(
                                tx,
                                pass.app_config,
                                pass.entry,
                                hook.settings,
                                state,
                                &cause,
                                hook.source_wait_reason,
                            )? {
                                HookMatch::Matched(matched) => {
                                    let MatchedHookCommit {
                                        receipt: hook,
                                        fact,
                                    } = *matched;
                                    state.pending.push(PendingSubject {
                                        cause: cause.as_json(),
                                        reason: "awaiting_review_assignment".to_owned(),
                                        wake_when: vec![
                                            "eligible_reviewer_or_review_slot_change".to_owned(),
                                        ],
                                        first_seen_at_ms: pass.now_ms,
                                        last_checked_at_ms: pass.now_ms,
                                        held: false,
                                        awaiting_hook: false,
                                        hook_fact: Some(serde_json::to_value(&fact)?),
                                        hook_event_key: Some(hook.source_event_key.clone()),
                                        hook_observation_id: Some(hook.observation_id),
                                    });
                                    remember_recent(
                                        state,
                                        json!({
                                            "observation_id":observation_id,
                                            "submission_ref":cause.id(),
                                            "source_event_key":receipt.source_event_key,
                                            "hook_observation_id":hook.observation_id,
                                            "hook_event_key":hook.source_event_key,
                                            "disposition":"hook_commit_matched"
                                        }),
                                    );
                                }
                                HookMatch::Waiting { source_reason } => {
                                    let unavailable = source_reason.is_some();
                                    state.pending.push(PendingSubject {
                                        cause: cause.as_json(),
                                        reason: source_reason.unwrap_or_else(|| {
                                            "awaiting_verified_hook_commit".to_owned()
                                        }),
                                        wake_when: if unavailable {
                                            vec!["selected_hook_source_available".to_owned()]
                                        } else {
                                            vec!["matching_hook_commit_observed".to_owned()]
                                        },
                                        first_seen_at_ms: pass.now_ms,
                                        last_checked_at_ms: pass.now_ms,
                                        held: unavailable,
                                        awaiting_hook: true,
                                        hook_fact: None,
                                        hook_event_key: None,
                                        hook_observation_id: None,
                                    });
                                }
                                HookMatch::ReadbackOnly { reason } => remember_recent(
                                    state,
                                    json!({
                                        "observation_id":observation_id,
                                        "submission_ref":cause.id(),
                                        "source_event_key":receipt.source_event_key,
                                        "disposition":"readback_only",
                                        "capability":"hook_commit_receipt",
                                        "reason":reason
                                    }),
                                ),
                            }
                        } else {
                            match attempt_review_assignment(tx, pass.entry, &cause, pass.now_ms)? {
                                SubjectResult::Assigned {
                                    operation_id,
                                    value,
                                } => remember_recent(
                                    state,
                                    json!({
                                        "observation_id":observation_id,
                                        "submission_ref":cause.id(),
                                        "source_event_key":receipt.source_event_key,
                                        "disposition":"assigned",
                                        "operation_id":operation_id,
                                        "review_assignment_id":value["review_assignment_id"]
                                    }),
                                ),
                                SubjectResult::Pending { reason, wake_when } => {
                                    state.pending.push(PendingSubject {
                                        cause: cause.as_json(),
                                        reason,
                                        wake_when,
                                        first_seen_at_ms: pass.now_ms,
                                        last_checked_at_ms: pass.now_ms,
                                        held: false,
                                        awaiting_hook: false,
                                        hook_fact: None,
                                        hook_event_key: None,
                                        hook_observation_id: None,
                                    });
                                }
                                SubjectResult::Skipped { reason } => remember_recent(
                                    state,
                                    json!({
                                        "observation_id":observation_id,
                                        "submission_ref":cause.id(),
                                        "source_event_key":receipt.source_event_key,
                                        "disposition":"skipped",
                                        "reason":reason
                                    }),
                                ),
                            }
                        }
                    }
                }
            }
        }
        state.cursor = observation_id;
    }
    if !stopped_at_cut
        && !stopped_for_capacity
        && processed == page.items.len()
        && page.items.len() < page_limit
    {
        state.cursor = target;
    }
    finish_activation_catch_up(state, intake);
    Ok(processed)
}

enum HookMatch {
    Matched(Box<MatchedHookCommit>),
    Waiting { source_reason: Option<String> },
    ReadbackOnly { reason: String },
}

struct GitSnapshotIdentity {
    project_id: String,
    commit_oid: String,
}

fn matching_hook_commit(
    tx: &Transaction<'_>,
    app_config: &Config,
    entry: &AutomationEntry,
    settings: &config::HookCommitSettings,
    state: &DispatchState,
    cause: &AutomationCause,
    source_wait_reason: Option<&str>,
) -> Result<HookMatch> {
    let Some(candidate) = submission_git_identity(tx, entry, cause)? else {
        return Ok(HookMatch::ReadbackOnly {
            reason: "submission_candidate_has_no_verified_source_snapshot_identity".to_owned(),
        });
    };
    let indexed = match automation_intake::hook_commit_by_identity(
        tx,
        &settings.source_id,
        &candidate.project_id,
        &candidate.commit_oid,
    ) {
        Ok(indexed) => indexed,
        Err(error) if safe_hook_index_readback_error(&error.code) => {
            return Ok(HookMatch::ReadbackOnly {
                reason: format!(
                    "hook_receipt_readback_unavailable:{}",
                    error.code.to_ascii_lowercase()
                ),
            });
        }
        Err(error) => return Err(error),
    };
    let Some((receipt, fact)) = indexed else {
        return Ok(HookMatch::Waiting {
            source_reason: source_wait_reason.map(str::to_owned),
        });
    };
    if !state.hook_include_existing && receipt.observation_id <= state.hook_activation_cut {
        return Ok(HookMatch::ReadbackOnly {
            reason: "matching_hook_commit_precedes_activation_cut".to_owned(),
        });
    }
    if fact.commit_oid != candidate.commit_oid
        || fact.project_id != candidate.project_id
        || fact.project_id != entry.project_id
    {
        return Ok(HookMatch::ReadbackOnly {
            reason: "hook_commit_project_or_candidate_identity_mismatch".to_owned(),
        });
    }
    if let Some(reason) =
        selected_hook_fact_readback_reason(tx, app_config, entry, settings, &fact)?
    {
        return Ok(HookMatch::ReadbackOnly { reason });
    }
    Ok(HookMatch::Matched(Box::new(MatchedHookCommit {
        receipt,
        fact,
    })))
}

fn submission_git_identity(
    db: &rusqlite::Connection,
    entry: &AutomationEntry,
    cause: &AutomationCause,
) -> Result<Option<GitSnapshotIdentity>> {
    let AutomationCause::AppliedSubmission {
        operation_id,
        submission_ref,
        ..
    } = cause
    else {
        return Ok(None);
    };
    let document = match submissions::document(db, submission_ref) {
        Ok(document) => document,
        Err(error) if safe_submission_candidate_readback_error(&error.code) => return Ok(None),
        Err(error) => return Err(error),
    };
    if document["operation_id"] != json!(operation_id) {
        return Ok(None);
    }
    let Some(task_id) = document["task_id"].as_str() else {
        return Ok(None);
    };
    let task = match tasks::get_task(db, task_id) {
        Ok(task) => task,
        Err(error) if safe_submission_candidate_readback_error(&error.code) => return Ok(None),
        Err(error) => return Err(error),
    };
    let Some(candidate_ref) = document["candidate_ref"].as_str() else {
        return Ok(None);
    };
    let candidate = match super::results::get(db, candidate_ref) {
        Ok(candidate) => candidate,
        Err(error) if safe_submission_candidate_readback_error(&error.code) => return Ok(None),
        Err(error) => return Err(error),
    };
    let attempt_id = document["attempt_id"].as_str().unwrap_or_default();
    let task_revision = document["task_revision"].as_i64();
    let commit_oid = candidate.metadata["commit"].as_str().unwrap_or_default();
    let tree_oid = candidate.metadata["tree"].as_str().unwrap_or_default();
    if task["project_id"] != entry.project_id
        || candidate.kind != "source_snapshot"
        || candidate.metadata["coverage"] != "complete"
        || candidate.metadata["task_id"] != task_id
        || candidate.metadata["attempt_id"] != attempt_id
        || task_revision.is_none()
        || candidate.metadata["task_revision"] != json!(task_revision)
        || !crate::forge::valid_object_id(commit_oid)
        || !crate::forge::valid_object_id(tree_oid)
    {
        return Ok(None);
    }
    Ok(Some(GitSnapshotIdentity {
        project_id: task["project_id"].as_str().unwrap_or_default().to_owned(),
        commit_oid: commit_oid.to_ascii_lowercase(),
    }))
}

fn safe_submission_candidate_readback_error(code: &str) -> bool {
    matches!(
        code,
        "ARTIFACT_DAMAGED"
            | "AUTOMATION_RECORD_CORRUPT"
            | "AUTOMATION_RECORD_VERSION"
            | "INVALID_PARAMS"
            | "NOT_FOUND"
            | "SUBMISSION_DAMAGED"
    )
}

fn selected_hook_source_wait_reason(
    db: &rusqlite::Connection,
    app_config: &Config,
    entry: &AutomationEntry,
    settings: &config::HookCommitSettings,
) -> Result<Option<String>> {
    match super::hooks::source_status(db, app_config, &settings.source_id) {
        Ok(super::hooks::HookSourceStatus::Current(source))
            if source.project_id == entry.project_id =>
        {
            Ok(None)
        }
        Ok(super::hooks::HookSourceStatus::Current(_)) => {
            Ok(Some("hook_source_project_mismatch".to_owned()))
        }
        Ok(super::hooks::HookSourceStatus::Revoked(_)) => {
            Ok(Some("hook_source_revoked".to_owned()))
        }
        Ok(super::hooks::HookSourceStatus::Missing) => Ok(Some("hook_source_missing".to_owned())),
        Ok(super::hooks::HookSourceStatus::Stale(_)) => Ok(Some("hook_source_stale".to_owned())),
        Err(error) if optional_hook_source_record_error(&error.code) => Ok(Some(format!(
            "hook_source_record_corrupt:{}",
            error.code.to_ascii_lowercase()
        ))),
        Err(error) => Err(error),
    }
}

fn optional_hook_source_record_error(code: &str) -> bool {
    matches!(
        code,
        "AUTOMATION_RECORD_CORRUPT"
            | "AUTOMATION_RECORD_VERSION"
            | "HOOK_SETUP_REQUEST_RECORD_INVALID"
            | "HOOK_SOURCE_RECORD_INVALID"
    )
}

fn safe_hook_index_readback_error(code: &str) -> bool {
    matches!(
        code,
        "AUTOMATION_HOOK_FACT_INVALID"
            | "AUTOMATION_HOOK_INDEX_CORRUPT"
            | "AUTOMATION_INTAKE_RECORD_INVALID"
            | "AUTOMATION_RECORD_CORRUPT"
            | "AUTOMATION_RECORD_VERSION"
            | "HOOK_EVENT_RECORD_INVALID"
    )
}

fn selected_hook_fact_readback_reason(
    db: &rusqlite::Connection,
    app_config: &Config,
    entry: &AutomationEntry,
    settings: &config::HookCommitSettings,
    fact: &crate::hooks::contract::HookCommitFact,
) -> Result<Option<String>> {
    if fact.source_id != settings.source_id {
        return Ok(Some("hook_source_does_not_match_selected_entry".to_owned()));
    }
    if fact.project_id != entry.project_id {
        return Ok(Some("hook_project_outside_selected_entry".to_owned()));
    }
    match super::hooks::source_status(db, app_config, &settings.source_id) {
        Ok(super::hooks::HookSourceStatus::Current(source)) => {
            if source.source_id != fact.source_id
                || source.project_id != fact.project_id
                || source.project_id != entry.project_id
                || source.canonical_repository != fact.canonical_repository
                || source.registration_id != fact.registration_id
                || source.registration_generation != fact.registration_generation
                || source.event != fact.event
            {
                return Ok(Some("hook_source_readback_mismatch".to_owned()));
            }
            Ok(None)
        }
        Ok(super::hooks::HookSourceStatus::Revoked(_)) => {
            Ok(Some("hook_source_revoked".to_owned()))
        }
        Ok(super::hooks::HookSourceStatus::Missing) => Ok(Some("hook_source_missing".to_owned())),
        Ok(super::hooks::HookSourceStatus::Stale(_)) => Ok(Some("hook_source_stale".to_owned())),
        Err(error) if optional_hook_source_record_error(&error.code) => Ok(Some(format!(
            "hook_source_record_corrupt:{}",
            error.code.to_ascii_lowercase()
        ))),
        Err(error) => Err(error),
    }
}

fn consume_hook_commit_page(
    tx: &Transaction<'_>,
    state: &mut DispatchState,
    budget: usize,
    intake: SourceIntakeSnapshot,
    pass: &DispatchPassContext<'_>,
) -> Result<usize> {
    let Some(hook) = pass.hook else {
        return Err(Error::new(
            "AUTOMATION_HOOK_CONTEXT_MISSING",
            "HookCommit page consumption requires a selected hook trigger",
        ));
    };
    let entry = pass.entry;
    let settings = hook.settings;
    let app_config = pass.app_config;
    let now_ms = pass.now_ms;
    let page_limit = budget.min(MAX_SUBMISSION_PAGE);
    let page = automation_intake::pending_page(
        tx,
        LocalProducer::HookCommit.source_id(),
        state.hook_cursor,
        page_limit,
    )?;
    if page.status == IntakeStatus::UnknownSource {
        return Err(Error::new(
            "AUTOMATION_INTAKE_SOURCE_MISSING",
            "selected HookCommit intake has not been registered",
        ));
    }
    if page.status == IntakeStatus::StaleCursor {
        return Ok(0);
    }
    let intake_cursor = page.cursor.ok_or_else(|| {
        Error::new(
            "AUTOMATION_INTAKE_CURSOR_MISSING",
            "registered HookCommit source has no durable cursor",
        )
    })?;
    if intake_cursor != intake.cursor {
        return Err(Error::new(
            "AUTOMATION_INTAKE_CURSOR_CHANGED",
            "per-entry HookCommit journal differs from the shared intake snapshot",
        ));
    }
    if state.hook_cursor >= intake_cursor {
        return Ok(0);
    }

    let mut processed = 0usize;
    for item in &page.items {
        let observation_id = intake_item_observation_id(item);
        if observation_id <= state.hook_cursor {
            return Err(Error::new(
                "AUTOMATION_INTAKE_JOURNAL_ORDER_INVALID",
                "HookCommit journal returned an observation at or before the consumer cursor",
            ));
        }
        processed += 1;
        match item {
            IntakeItem::Gap(gap) => {
                if gap.source_id != LocalProducer::HookCommit.source_id()
                    || gap.observation_id != observation_id
                {
                    return Err(Error::new(
                        "AUTOMATION_INTAKE_JOURNAL_IDENTITY_INVALID",
                        "pending gap does not match the registered HookCommit source",
                    ));
                }
                remember_recent(
                    state,
                    json!({
                        "hook_observation_id":observation_id,
                        "hook_source_event_key":gap.source_event_key,
                        "disposition":"readback_only",
                        "capability":"hook_commit_receipt",
                        "reason":gap.reason
                    }),
                );
            }
            IntakeItem::Receipt(receipt) => {
                if receipt.source_id != LocalProducer::HookCommit.source_id()
                    || receipt.event_kind != LocalProducer::HookCommit.event_kind()
                {
                    return Err(Error::new(
                        "AUTOMATION_INTAKE_JOURNAL_IDENTITY_INVALID",
                        "pending receipt is outside the registered HookCommit source",
                    ));
                }
                let fact = match automation_intake::parse_hook_commit_fact(receipt) {
                    Ok(fact) => fact,
                    Err(error) => {
                        remember_recent(
                            state,
                            json!({
                                "hook_observation_id":observation_id,
                                "hook_source_event_key":receipt.source_event_key,
                                "disposition":"readback_only",
                                "capability":"hook_commit_receipt",
                                "reason":error.code.to_ascii_lowercase()
                            }),
                        );
                        state.hook_cursor = observation_id;
                        continue;
                    }
                };
                if fact.source_id != settings.source_id {
                    remember_recent(
                        state,
                        json!({
                            "hook_observation_id":observation_id,
                            "hook_source_event_key":receipt.source_event_key,
                            "disposition":"unselected_source"
                        }),
                    );
                    state.hook_cursor = observation_id;
                    continue;
                }
                if !state.hook_include_existing && observation_id <= state.hook_activation_cut {
                    remember_recent(
                        state,
                        json!({
                            "hook_observation_id":observation_id,
                            "hook_source_event_key":receipt.source_event_key,
                            "disposition":"readback_only",
                            "capability":"hook_commit_receipt",
                            "reason":"hook_commit_precedes_activation_cut"
                        }),
                    );
                    state.hook_cursor = observation_id;
                    continue;
                }
                if let Some(reason) =
                    selected_hook_fact_readback_reason(tx, app_config, entry, settings, &fact)?
                {
                    remember_recent(
                        state,
                        json!({
                            "hook_observation_id":observation_id,
                            "hook_source_event_key":receipt.source_event_key,
                            "disposition":"readback_only",
                            "capability":"hook_commit_receipt",
                            "reason":reason
                        }),
                    );
                    state.hook_cursor = observation_id;
                    continue;
                }

                let mut matched = Vec::new();
                let mut unmatchable = BTreeSet::new();
                for pending in &mut state.pending {
                    if pending.held || !pending.awaiting_hook {
                        continue;
                    }
                    let cause = match cause_from_json(&pending.cause) {
                        Ok(cause) => cause,
                        Err(_) => {
                            pending.reason = "retained_cause_corrupt".to_owned();
                            pending.wake_when = vec!["operator_inspection".to_owned()];
                            pending.last_checked_at_ms = now_ms;
                            continue;
                        }
                    };
                    match submission_git_identity(tx, entry, &cause)? {
                        Some(candidate)
                            if candidate.project_id == fact.project_id
                                && candidate.commit_oid == fact.commit_oid.to_ascii_lowercase() =>
                        {
                            pending.awaiting_hook = false;
                            pending.hook_fact = Some(serde_json::to_value(&fact)?);
                            pending.hook_event_key = Some(receipt.source_event_key.clone());
                            pending.hook_observation_id = Some(observation_id);
                            pending.reason = "awaiting_review_assignment".to_owned();
                            pending.wake_when =
                                vec!["eligible_reviewer_or_review_slot_change".to_owned()];
                            pending.last_checked_at_ms = now_ms;
                            matched.push(cause.id().to_owned());
                        }
                        None => {
                            unmatchable.insert(pending_cause_id(pending).to_owned());
                        }
                        _ => {}
                    }
                }
                if !unmatchable.is_empty() {
                    state.pending.retain(|pending| {
                        !pending.awaiting_hook || !unmatchable.contains(pending_cause_id(pending))
                    });
                    for submission_ref in unmatchable {
                        remember_recent(
                            state,
                            json!({
                                "submission_ref":submission_ref,
                                "hook_observation_id":observation_id,
                                "disposition":"readback_only",
                                "capability":"hook_commit_receipt",
                                "reason":"submission_candidate_has_no_verified_source_snapshot_identity"
                            }),
                        );
                    }
                }
                if matched.is_empty() {
                    remember_recent(
                        state,
                        json!({
                            "hook_observation_id":observation_id,
                            "hook_source_event_key":receipt.source_event_key,
                            "disposition":"retained_readback_only",
                            "capability":"hook_commit_receipt",
                            "reason":"no_exact_applied_submission_yet"
                        }),
                    );
                } else {
                    remember_recent(
                        state,
                        json!({
                            "hook_observation_id":observation_id,
                            "hook_source_event_key":receipt.source_event_key,
                            "submission_refs":matched,
                            "disposition":"hook_commit_matched"
                        }),
                    );
                }
            }
        }
        state.hook_cursor = observation_id;
    }
    if processed == page.items.len() && page.items.len() < page_limit {
        state.hook_cursor = intake_cursor;
    }
    Ok(processed)
}

fn intake_item_observation_id(item: &IntakeItem) -> i64 {
    match item {
        IntakeItem::Receipt(receipt) => receipt.observation_id,
        IntakeItem::Gap(gap) => gap.observation_id,
    }
}

fn finish_activation_catch_up(state: &mut DispatchState, intake: IntakeSnapshot) {
    let Some(cut) = state.catch_up_until else {
        return;
    };
    let reached_cut = state.cursor >= cut;
    let source_caught_up_before_cut = intake.cursor >= intake.high_water
        && intake.high_water < cut
        && state.cursor >= intake.high_water;
    if reached_cut || source_caught_up_before_cut {
        state.catch_up_until = None;
    }
}

fn recheck_pending(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    state: &mut DispatchState,
    budget: usize,
    app_config: &Config,
    now_ms: i64,
) -> Result<usize> {
    if budget == 0 || state.pending.is_empty() || !entry.enabled {
        return Ok(0);
    }
    state.pending.sort_by_key(pending_observation_id);
    let eligible = state
        .pending
        .iter()
        .enumerate()
        .filter(|(_, pending)| !pending.held && !pending.awaiting_hook)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if eligible.is_empty() {
        return Ok(0);
    }
    let start = eligible
        .iter()
        .position(|index| {
            pending_observation_id(&state.pending[*index]) > state.pending_after_observation_id
        })
        .unwrap_or(0);
    let candidates = eligible.len().min(budget);
    let indices = (0..candidates)
        .map(|offset| eligible[(start + offset) % eligible.len()])
        .collect::<Vec<_>>();
    let mut remove = BTreeSet::new();
    for index in indices {
        state.pending_after_observation_id = pending_observation_id(&state.pending[index]);
        let cause = match cause_from_json(&state.pending[index].cause) {
            Ok(cause) => cause,
            Err(error) => {
                state.pending[index].reason = "retained_cause_corrupt".to_owned();
                state.pending[index].wake_when = vec!["operator_inspection".to_owned()];
                state.pending[index].last_checked_at_ms = now_ms;
                remember_recent(state, json!({"disposition":"blocked","error":error}));
                continue;
            }
        };
        let hook_fact = state.pending[index].hook_fact.clone();
        if let Some(hook_fact_value) = hook_fact.as_ref()
            && let Some(reason) = pending_hook_readback_reason(
                tx,
                app_config,
                entry,
                &state.pending[index],
                &cause,
                hook_fact_value,
            )?
        {
            remove.insert(cause.id().to_owned());
            remember_recent(
                state,
                json!({
                    "submission_ref":cause.id(),
                    "hook_observation_id":state.pending[index].hook_observation_id,
                    "hook_event_key":state.pending[index].hook_event_key,
                    "disposition":"readback_only",
                    "capability":"hook_commit_receipt",
                    "reason":reason
                }),
            );
            continue;
        }
        let hook_event_key = state.pending[index].hook_event_key.clone();
        let hook_observation_id = state.pending[index].hook_observation_id;
        match attempt_review_assignment(tx, entry, &cause, now_ms)? {
            SubjectResult::Assigned {
                operation_id,
                value,
            } => {
                remove.insert(cause.id().to_owned());
                remember_recent(
                    state,
                    json!({
                        "submission_ref":cause.id(),
                        "disposition":"assigned",
                        "operation_id":operation_id,
                        "review_assignment_id":value["review_assignment_id"],
                        "hook_event_key":hook_event_key,
                        "hook_observation_id":hook_observation_id
                    }),
                );
            }
            SubjectResult::Pending { reason, wake_when } => {
                state.pending[index].reason = reason;
                state.pending[index].wake_when = wake_when;
                state.pending[index].last_checked_at_ms = now_ms;
            }
            SubjectResult::Skipped { reason } => {
                remove.insert(cause.id().to_owned());
                remember_recent(
                    state,
                    json!({
                        "submission_ref":cause.id(),
                        "disposition":"skipped",
                        "reason":reason,
                        "hook_event_key":hook_event_key,
                        "hook_observation_id":hook_observation_id
                    }),
                );
            }
        }
    }
    state
        .pending
        .retain(|pending| !remove.contains(pending_cause_id(pending)));
    Ok(candidates)
}

fn pending_hook_readback_reason(
    db: &rusqlite::Connection,
    app_config: &Config,
    entry: &AutomationEntry,
    pending: &PendingSubject,
    cause: &AutomationCause,
    value: &Value,
) -> Result<Option<String>> {
    let Some(settings) = entry.hook_commit.as_ref() else {
        return Ok(Some("hook_trigger_removed".to_owned()));
    };
    let fact = match crate::hooks::contract::HookCommitFact::parse(value) {
        Ok(fact) => fact,
        Err(error) => return Ok(Some(error.code.to_ascii_lowercase())),
    };
    let event_key = pending.hook_event_key.as_deref().unwrap_or_default();
    if pending.hook_observation_id.is_none_or(|id| id <= 0)
        || event_key != format!("{}:{}", fact.source_id, fact.commit_oid)
        || fact.source_id != settings.source_id
        || fact.project_id != entry.project_id
    {
        return Ok(Some("hook_pending_identity_mismatch".to_owned()));
    }
    let Some(candidate) = submission_git_identity(db, entry, cause)? else {
        return Ok(Some(
            "submission_candidate_has_no_verified_source_snapshot_identity".to_owned(),
        ));
    };
    if candidate.project_id != fact.project_id
        || candidate.commit_oid != fact.commit_oid.to_ascii_lowercase()
    {
        return Ok(Some("hook_commit_candidate_identity_changed".to_owned()));
    }
    let retained = match automation_intake::hook_commit_by_identity(
        db,
        &fact.source_id,
        &fact.project_id,
        &fact.commit_oid,
    ) {
        Ok(Some(retained)) => retained,
        Ok(None) => return Ok(Some("hook_commit_receipt_missing".to_owned())),
        Err(error) if safe_hook_index_readback_error(&error.code) => {
            return Ok(Some(format!(
                "hook_commit_receipt_unavailable:{}",
                error.code.to_ascii_lowercase()
            )));
        }
        Err(error) => return Err(error),
    };
    let (receipt, retained_fact) = retained;
    if receipt.observation_id != pending.hook_observation_id.unwrap_or_default()
        || receipt.source_event_key != event_key
        || retained_fact != fact
    {
        return Ok(Some("hook_pending_receipt_mismatch".to_owned()));
    }
    selected_hook_fact_readback_reason(db, app_config, entry, settings, &fact)
}

fn attempt_review_assignment(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    cause: &AutomationCause,
    now_ms: i64,
) -> Result<SubjectResult> {
    if !entry.enabled || !entry.steps.contains(&AutomationStep::ReviewDispatch) {
        return Ok(SubjectResult::Pending {
            reason: "automation_disabled_or_step_removed".to_owned(),
            wake_when: vec!["automation_config_changed".to_owned()],
        });
    }
    if entry.scope.work_pool_id.is_some() {
        return Ok(SubjectResult::Pending {
            reason: "work_pool_scope_unavailable".to_owned(),
            wake_when: vec!["work_pool_membership_reader_available".to_owned()],
        });
    }
    if entry.review.required_reviewers != 1 {
        return Ok(SubjectResult::Pending {
            reason: "reviewer_count_unsupported".to_owned(),
            wake_when: vec!["supported_review_slot_count_selected".to_owned()],
        });
    }
    let Some(profile) = entry.review.profile.as_deref() else {
        return Ok(SubjectResult::Pending {
            reason: "review_profile_required".to_owned(),
            wake_when: vec!["review_profile_configured".to_owned()],
        });
    };
    let AutomationCause::AppliedSubmission { operation_id, .. } = cause else {
        return Err(Error::new(
            "AUTOMATION_CAUSE_INVALID",
            "review assignment requires an applied submission cause",
        ));
    };
    let submission_ref = cause.id();
    let document = match submissions::document(tx, submission_ref) {
        Ok(document) => document,
        Err(error) => {
            return Ok(SubjectResult::Pending {
                reason: format!("submission_record_unavailable:{}", error.code),
                wake_when: vec!["submission_record_readable".to_owned()],
            });
        }
    };
    if document["operation_id"] != json!(operation_id)
        || document["task_revision"].as_i64().is_none()
    {
        return Ok(SubjectResult::Pending {
            reason: "submission_identity_mismatch".to_owned(),
            wake_when: vec!["operator_inspection".to_owned()],
        });
    }
    let attempt_id = match document["attempt_id"].as_str() {
        Some(value) => value,
        None => {
            return Ok(SubjectResult::Pending {
                reason: "submission_missing_attempt".to_owned(),
                wake_when: vec!["operator_inspection".to_owned()],
            });
        }
    };
    let task_id = match document["task_id"].as_str() {
        Some(value) => value,
        None => {
            return Ok(SubjectResult::Pending {
                reason: "submission_missing_task".to_owned(),
                wake_when: vec!["operator_inspection".to_owned()],
            });
        }
    };
    let task_revision = document["task_revision"].as_i64().unwrap_or_default();
    let candidate_ref = match document["candidate_ref"].as_str() {
        Some(value) => value,
        None => {
            return Ok(SubjectResult::Pending {
                reason: "submission_missing_candidate".to_owned(),
                wake_when: vec!["operator_inspection".to_owned()],
            });
        }
    };
    let attempt = match tasks::get_attempt(tx, attempt_id) {
        Ok(value) => value,
        Err(error) if error.code == "NOT_FOUND" => {
            return Ok(SubjectResult::Skipped {
                reason: "attempt_no_longer_available".to_owned(),
            });
        }
        Err(error) => return Err(error),
    };
    let task = match tasks::get_task(tx, task_id) {
        Ok(value) => value,
        Err(error) if error.code == "NOT_FOUND" => {
            return Ok(SubjectResult::Skipped {
                reason: "task_no_longer_available".to_owned(),
            });
        }
        Err(error) => return Err(error),
    };
    if task["project_id"] != entry.project_id {
        return Ok(SubjectResult::Skipped {
            reason: "outside_project_scope".to_owned(),
        });
    }
    if task["state"] != "open"
        || task["revision"] != json!(task_revision)
        || task["current_attempt_id"] != json!(attempt_id)
        || attempt["state"] != "submitted"
        || attempt["task_revision"] != json!(task_revision)
        || attempt["task_id"] != json!(task_id)
        || !attempt["released_at_ms"].is_null()
    {
        return Ok(SubjectResult::Skipped {
            reason: "submission_is_not_current_attempt_work".to_owned(),
        });
    }
    if attempt["submission_ref"] != json!(submission_ref)
        || attempt["candidate_ref"] != json!(candidate_ref)
    {
        return Ok(SubjectResult::Skipped {
            reason: "submission_is_not_current_attempt_candidate".to_owned(),
        });
    }
    let transferred_authority = if attempt["owner_id"] == json!(entry.owner_manager_id) {
        None
    } else {
        match authorization::current_transferred_attempt_authority(
            tx,
            entry,
            AutomationStep::ReviewDispatch,
            task_id,
            task_revision,
            attempt_id,
            submission_ref,
            candidate_ref,
        ) {
            Ok(Some(authority)) => Some(authority),
            Ok(None) => {
                return Ok(SubjectResult::Skipped {
                    reason: "attempt_owner_is_not_in_current_transfer_lineage".to_owned(),
                });
            }
            Err(error) if error.code == "FORBIDDEN" => {
                return Ok(SubjectResult::Pending {
                    reason: "successor_manager_authority_unavailable".to_owned(),
                    wake_when: vec!["current_gm_or_task_scope_changed".to_owned()],
                });
            }
            Err(error) if error.code == "AUTOMATION_ACTION_CHANGED" => {
                return Ok(SubjectResult::Pending {
                    reason: "automation_configuration_changed".to_owned(),
                    wake_when: vec!["automation_config_changed".to_owned()],
                });
            }
            Err(error) if error.code == "AUTOMATION_ATTEMPT_STALE" => {
                return Ok(SubjectResult::Skipped {
                    reason: "submission_is_not_current_attempt_work".to_owned(),
                });
            }
            Err(error) => return Err(error),
        }
    };
    let context = match ManagerExecutionContext::from_committed_entry(tx, entry, cause.clone()) {
        Ok(context) => context,
        Err(error) if error.code == "FORBIDDEN" || error.code == "UNAUTHORIZED" => {
            return Ok(SubjectResult::Pending {
                reason: "manager_authority_unavailable".to_owned(),
                wake_when: vec!["manager_registration_or_rights_changed".to_owned()],
            });
        }
        Err(error) if error.code.starts_with("AUTOMATION_ACTION") => {
            return Ok(SubjectResult::Pending {
                reason: error.code.to_ascii_lowercase(),
                wake_when: vec!["automation_configuration_changed".to_owned()],
            });
        }
        Err(error) => return Err(error),
    };
    let client_request_id = automatic_request_id(
        &context,
        task_id,
        attempt_id,
        task_revision,
        submission_ref,
        candidate_ref,
        transferred_authority.as_ref(),
    )?;
    let request = ReviewAssignRequest::for_automation(
        client_request_id.clone(),
        attempt_id.to_owned(),
        task_revision,
        submission_ref.to_owned(),
        candidate_ref.to_owned(),
        profile.to_owned(),
    );
    let request_value = json!({
        "client_request_id":client_request_id,
        "attempt_id":attempt_id,
        "expected_revision":task_revision,
        "submission_ref":submission_ref,
        "candidate_ref":candidate_ref,
        "review_profile":profile
    });
    match reserve_automatic_operation(tx, &context, &request, &request_value, now_ms) {
        Ok((operation_id, value)) => Ok(SubjectResult::Assigned {
            operation_id,
            value,
        }),
        Err(error) if is_pending_review_error(&error) => Ok(SubjectResult::Pending {
            reason: error.code.to_ascii_lowercase(),
            wake_when: vec!["eligible_reviewer_or_review_slot_change".to_owned()],
        }),
        Err(error) if is_stale_review_error(&error) => Ok(SubjectResult::Skipped {
            reason: error.code.to_ascii_lowercase(),
        }),
        Err(error) => Err(error),
    }
}

fn reserve_automatic_operation(
    tx: &Transaction<'_>,
    context: &ManagerExecutionContext,
    request: &ReviewAssignRequest,
    request_value: &Value,
    now_ms: i64,
) -> Result<(String, Value)> {
    let caller = context.technical_requester_id();
    let request_id = model::text(request_value, "client_request_id")?;
    let original_json = model::canonical(request_value)?;
    let old: Option<(String, String, String, Option<String>)> = tx
        .query_row(
            "SELECT method,original_request_json,state,result_json FROM operations \
             WHERE caller_id=?1 AND client_request_id=?2",
            params![caller, request_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    if let Some((method, original, state, result)) = old {
        if method != "review.assign" || original != original_json {
            return Err(Error::new(
                "REVIEW_SLOT_CONFLICT",
                "this semantic review request already retained different effective inputs",
            ));
        }
        if state == "settled" {
            let operation_id: String = tx.query_row(
                "SELECT operation_id FROM operations WHERE caller_id=?1 AND client_request_id=?2",
                params![caller, request_id],
                |row| row.get(0),
            )?;
            let link = authorization::operation_link(tx, &operation_id)?.ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "settled automatic review request has no retained manager link",
                )
            })?;
            if link.effective_manager_id != context.effective_manager_id()
                || link.project_id != context.project_id()
                || link.automation_id != context.automation_id()
                || link.cause["id"] != context.semantic_cause_id()
            {
                return Err(Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "settled automatic review request is linked to another manager, entry, or subject",
                ));
            }
            let result: Value = serde_json::from_str(&result.ok_or_else(|| {
                Error::new(
                    "AUTOMATION_OPERATION_CORRUPT",
                    "settled review request has no result",
                )
            })?)?;
            return Ok((operation_id, result));
        }
        return Err(Error::new(
            "REVIEW_ASSIGNMENT_RECONCILING",
            "automatic review assignment has an unresolved retained Operation",
        ));
    }

    let operation_id = model::new_id();
    let effective = json!({
        "request":request_value,
        "automation_on_behalf":context.linkage_value()
    });
    tx.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,?3,'review.assign',?4,?5,'queued',?6,?6,?6)",
        params![operation_id, caller, request_id, original_json, model::canonical(&effective)?, now_ms],
    )?;
    tx.execute_batch("SAVEPOINT automation_review_assignment")?;
    let outcome = super::reviews::reserve_assign(
        tx,
        ReviewActor::OnBehalf(context),
        request,
        &operation_id,
        now_ms,
    );
    match outcome {
        Ok(value) => {
            let receipt = json!({"ok":true,"value":value});
            let effective: Value = {
                let raw: String = tx.query_row(
                    "SELECT effective_request_json FROM operations WHERE operation_id=?1",
                    [&operation_id],
                    |row| row.get(0),
                )?;
                serde_json::from_str(&raw)?
            };
            let mut effective = effective;
            if !effective.is_object() {
                effective = json!({"request":request_value});
            }
            effective["automation_on_behalf"] = context.linkage_value();
            effective["receipt"] = receipt;
            tx.execute(
                "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3,effective_request_json=?4 WHERE operation_id=?1",
                params![operation_id, model::canonical(&value)?, now_ms, model::canonical(&effective)?],
            )?;
            capacity::sync_operation(tx, &operation_id, now_ms)?;
            tx.execute(
                "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
                 VALUES('controller',?1,?1,'review.assign',?2,?3)",
                params![operation_id, model::canonical(&value)?, now_ms],
            )?;
            authorization::save_operation_link(tx, &operation_id, context, now_ms)?;
            tx.execute_batch("RELEASE automation_review_assignment")?;
            Ok((operation_id, value))
        }
        Err(error) => {
            tx.execute_batch(
                "ROLLBACK TO automation_review_assignment; RELEASE automation_review_assignment",
            )?;
            tx.execute(
                "DELETE FROM operations WHERE operation_id=?1",
                [&operation_id],
            )?;
            Err(error)
        }
    }
}

fn automatic_request_id(
    context: &ManagerExecutionContext,
    task_id: &str,
    attempt_id: &str,
    task_revision: i64,
    submission_ref: &str,
    candidate_ref: &str,
    transferred_authority: Option<&authorization::TransferredAttemptAuthority>,
) -> Result<String> {
    let mut identity = json!({
        "automation_id":context.automation_id(),
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "submission_ref":submission_ref,
        "candidate_ref":candidate_ref,
        "action":"review.assign",
        "slot":"primary"
    });
    if let Some(authority) = transferred_authority {
        identity["transferred_attempt_authority"] = json!({
            "source_attempt_owner_id":authority.source_attempt_owner_id(),
            "successor_manager_id":authority.successor_manager_id(),
            "transfer_operation_ids":authority.transfer_operation_ids(),
        });
    }
    Ok(model::digest(model::canonical(&identity)?.as_bytes()))
}

fn cause_from_fact(observation_id: i64, payload: &Value) -> Result<Option<AutomationCause>> {
    if payload["outcome"] != "applied" {
        return Ok(None);
    }
    let operation_id = model::text(payload, "operation_id").map_err(|_| {
        Error::new(
            "AUTOMATION_FACT_CORRUPT",
            "applied submission fact has no Operation identity",
        )
    })?;
    let submission_ref = model::text(payload, "submission_ref").map_err(|_| {
        Error::new(
            "AUTOMATION_FACT_CORRUPT",
            "applied submission fact has no submission identity",
        )
    })?;
    Ok(Some(AutomationCause::AppliedSubmission {
        observation_id,
        operation_id: operation_id.to_owned(),
        submission_ref: submission_ref.to_owned(),
    }))
}

fn cause_from_json(value: &Value) -> Result<AutomationCause> {
    if value["kind"] != "applied_submission" {
        return Err(Error::new(
            "AUTOMATION_CAUSE_INVALID",
            "pending cause kind is unsupported",
        ));
    }
    Ok(AutomationCause::AppliedSubmission {
        observation_id: value["observation_id"].as_i64().ok_or_else(|| {
            Error::new(
                "AUTOMATION_CAUSE_INVALID",
                "pending cause has no observation",
            )
        })?,
        operation_id: model::text(value, "operation_id")?.to_owned(),
        submission_ref: model::text(value, "id")?.to_owned(),
    })
}

fn is_pending_review_error(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "REVIEW_AUDITOR_UNAVAILABLE"
            | "REVIEW_AUDITOR_AMBIGUOUS"
            | "REVIEWER_PROFILE_UNAVAILABLE"
            | "REVIEWER_UNAVAILABLE"
            | "REVIEW_SLOT_CONFLICT"
            | "REVIEW_ASSIGNMENT_RECONCILING"
            | "REVIEW_CAPACITY"
            | "CAPACITY_UNAVAILABLE"
            | "FORBIDDEN"
            | "UNAUTHORIZED"
    )
}

fn is_stale_review_error(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "STALE_REVISION" | "STALE_SUBMISSION" | "NOT_FOUND" | "SUBMISSION_DAMAGED"
    )
}

fn load_state(db: &rusqlite::Connection, entry: &AutomationEntry) -> Result<Option<DispatchState>> {
    let key = config::dispatch_state_key(
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?;
    let Some(record) = config::read_record(db, &key, "automation dispatch cursor")? else {
        return Ok(None);
    };
    let state: DispatchState = serde_json::from_value(record).map_err(|_| {
        Error::new(
            "AUTOMATION_CURSOR_CORRUPT",
            "automation dispatch state fields are invalid",
        )
    })?;
    validate_state(&state, entry)?;
    Ok(Some(state))
}

pub(super) fn relocate_state(
    tx: &Transaction<'_>,
    former: &AutomationEntry,
    new: &AutomationEntry,
) -> Result<bool> {
    config::validate_entry(former)?;
    config::validate_entry(new)?;
    if former.owner_manager_id == new.owner_manager_id
        || former.project_id != new.project_id
        || former.automation_id != new.automation_id
    {
        return Err(Error::invalid(
            "automation dispatch relocation must preserve project and automation identity while changing owner",
        ));
    }

    let source_key = config::dispatch_state_key(
        &former.owner_manager_id,
        &former.project_id,
        &former.automation_id,
    )?;
    let target_key =
        config::dispatch_state_key(&new.owner_manager_id, &new.project_id, &new.automation_id)?;
    let target_exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM meta WHERE key=?1)",
        [&target_key],
        |row| row.get(0),
    )?;
    if target_exists {
        return Err(Error::conflict(
            "automation dispatch target state already exists",
        ));
    }

    if let Some(mut state) = load_state(tx, former)? {
        state.owner_manager_id = new.owner_manager_id.clone();
        validate_state(&state, new)?;
        config::write_record(tx, &target_key, &serde_json::to_value(&state)?)?;
        let deleted = tx.execute("DELETE FROM meta WHERE key=?1", [&source_key])?;
        if deleted != 1 {
            return Err(Error::new(
                "AUTOMATION_CURSOR_MISSING",
                "automation dispatch source state changed during relocation",
            ));
        }
    }

    // ScriptRun has an independent activation cursor inside O1's dispatch
    // family. Move its exact pending causes with the cursor, but hold unstarted
    // work until the successor's current Manager, script ownership and
    // Task/Attempt rights have all been revalidated. No former-owner principal
    // or credential is retained in the intent.
    let script_source_key = config::script_dispatch_state_key(
        &former.owner_manager_id,
        &former.project_id,
        &former.automation_id,
    )?;
    let script_target_key = config::script_dispatch_state_key(
        &new.owner_manager_id,
        &new.project_id,
        &new.automation_id,
    )?;
    let script_record = config::read_record(tx, &script_source_key, "ScriptRun trigger cursor")?;
    let script_journal_relocated = script_record.is_some();
    if let Some(record) = script_record {
        let mut script_state: ScriptTriggerState =
            serde_json::from_value(record).map_err(|_| {
                Error::new(
                    "AUTOMATION_CURSOR_CORRUPT",
                    "ScriptRun trigger cursor fields are invalid during transfer",
                )
            })?;
        validate_script_trigger_state(&script_state)?;
        if script_state.owner_manager_id != former.owner_manager_id
            || script_state.project_id != former.project_id
            || script_state.automation_id != former.automation_id
        {
            return Err(Error::new(
                "AUTOMATION_CURSOR_CORRUPT",
                "ScriptRun trigger cursor source identity changed during transfer",
            ));
        }
        let script_target_exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM meta WHERE key=?1)",
            [&script_target_key],
            |row| row.get(0),
        )?;
        if script_target_exists {
            return Err(Error::conflict(
                "ScriptRun trigger transfer target state already exists",
            ));
        }
        let mut retained_pending = Vec::with_capacity(script_state.pending.len());
        let mut transfer_history = Vec::new();
        for mut pending in script_state.pending.drain(..) {
            let admitted = match pending.cause["kind"].as_str() {
                Some("applied_submission") => {
                    let cause = cause_from_json(&pending.cause)?;
                    let submission = submissions::document(tx, cause.id())?;
                    script_trigger_operation_exists(
                        tx,
                        new,
                        &cause,
                        model::text(&submission, "task_id")?,
                        model::positive(&submission, "task_revision")?,
                        model::text(&submission, "attempt_id")?,
                        &submission,
                        &pending.script_id,
                    )?
                }
                Some("system_event") => script_event_trigger_operation_exists(
                    tx,
                    new,
                    &pending.cause,
                    &pending.script_id,
                )?,
                _ => {
                    return Err(Error::new(
                        "AUTOMATION_CURSOR_CORRUPT",
                        "pending ScriptRun cause has an unsupported kind during transfer",
                    ));
                }
            };
            if let Some(operation_id) = admitted {
                transfer_history.push(json!({
                    "observation_id":pending.observation_id,
                    "submission_ref":pending.cause["id"],
                    "script_id":pending.script_id,
                    "cause":pending.cause,
                    "disposition":"already_admitted_before_automation_transfer",
                    "operation_id":operation_id
                }));
                continue;
            }
            if !pending.held {
                pending.held = true;
                pending.held_reason =
                    Some("held_on_transfer_current_actor_revalidation_required".to_owned());
            }
            pending.automation_revision = new.revision;
            // A handover changes the current consumer authority, not the
            // immutable producer cause. Re-seal the successor's action
            // context only when it still selects this pending ScriptRun;
            // otherwise leave the old cause unbound and held for ordinary
            // revalidation if that target is selected again.
            pending.consumer_context =
                match script_trigger_authority::ScriptRunConsumerContext::from_entry(new) {
                    Ok(context)
                        if context.matches_pending_identity(
                            &new.owner_manager_id,
                            &new.project_id,
                            &new.automation_id,
                            new.revision,
                            &pending.script_id,
                        ) =>
                    {
                        Some(context)
                    }
                    Ok(_) => None,
                    Err(error) if error.code == "AUTOMATION_ACTION_CHANGED" => None,
                    Err(error) => return Err(error),
                };
            retained_pending.push(pending);
        }
        script_state.pending = retained_pending;
        for event in transfer_history {
            remember_script_trigger_recent(&mut script_state, event);
        }
        script_state.owner_manager_id = new.owner_manager_id.clone();
        validate_script_trigger_state(&script_state)?;
        save_script_trigger_state(tx, &script_target_key, &script_state)?;
        let deleted = tx.execute("DELETE FROM meta WHERE key=?1", [&script_source_key])?;
        if deleted != 1 {
            return Err(Error::new(
                "AUTOMATION_CURSOR_MISSING",
                "ScriptRun trigger source state changed during transfer",
            ));
        }
    }
    Ok(script_journal_relocated)
}

fn revalidate_script_trigger_intents(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    state: &mut ScriptTriggerState,
    app_config: &Config,
    now_ms: i64,
) -> Result<()> {
    let mut recent = Vec::new();
    for pending in &mut state.pending {
        if !pending.held || !script_trigger_hold_is_revalidatable(pending.held_reason.as_deref()) {
            continue;
        }
        match script_trigger_block_reason(tx, entry, pending, app_config)? {
            None => {
                seal_revalidated_module_event_source(tx, entry, pending, app_config)?;
                pending.held = false;
                pending.held_reason = None;
                pending.automation_revision = entry.revision;
                pending.consumer_context =
                    Some(script_trigger_authority::ScriptRunConsumerContext::from_entry(entry)?);
                recent.push(json!({
                    "observation_id":pending.observation_id,
                    "submission_ref":pending.cause["id"],
                    "script_id":pending.script_id,
                    "cause":pending.cause,
                    "disposition":"pending_intent_revalidated_for_current_manager",
                    "automation_revision":entry.revision
                }));
            }
            Some(reason) => {
                if pending.held_reason.as_deref() != Some(reason.as_str()) {
                    pending.held_reason = Some(reason.clone());
                    recent.push(json!({
                        "observation_id":pending.observation_id,
                        "submission_ref":pending.cause["id"],
                        "script_id":pending.script_id,
                        "cause":pending.cause,
                        "disposition":"pending_intent_requires_current_authorization",
                        "reason":reason
                    }));
                }
            }
        }
    }
    for event in recent {
        remember_script_trigger_recent(state, event);
    }
    if !state.pending.is_empty() {
        state.updated_at_ms = now_ms;
    }
    Ok(())
}

/// Complete the retained source proof for the one bounded generic Module hold
/// before making its pending cause runnable. The source observation and its
/// closed projection were already admitted; this step only seals the exact
/// descriptor/binding/Task facts that the ordinary current-source gate has
/// now revalidated. Legacy routes and transfer handling stay on their own
/// paths.
fn seal_revalidated_module_event_source(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    pending: &mut PendingScriptTrigger,
    app_config: &Config,
) -> Result<()> {
    if pending.held_reason.as_deref() != Some(SYSTEM_EVENT_SOURCE_PROOF_PENDING)
        || pending
            .cause
            .get("operation_id")
            .is_some_and(|value| !value.is_null())
        || pending
            .cause
            .get("module_source")
            .is_some_and(Value::is_object)
    {
        return Ok(());
    }
    let Some(source_id) = pending.cause["source_id"].as_str() else {
        return Err(Error::new(
            "AUTOMATION_CURSOR_CORRUPT",
            "generic Module source hold has no retained source identity",
        ));
    };
    if !source_id.starts_with("module:") {
        return Err(Error::new(
            "AUTOMATION_CURSOR_CORRUPT",
            "generic Module source hold names a non-Module source",
        ));
    }
    let observation_id = model::positive(&pending.cause, "observation_id")?;
    let event = automation_intake::observed_event_by_id(tx, observation_id)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_CURSOR_CORRUPT",
            "generic Module source hold points to a missing observation",
        )
    })?;
    let context = bus_kernel::script_event_invocation_context_for_consumer(
        tx,
        app_config,
        entry,
        &pending.cause,
        &entry.owner_manager_id,
    )?;
    bus_kernel::seal_retained_module_event_source(tx, entry, &event, &mut pending.cause)?;
    attach_event_task_scope(&mut pending.cause, &context)?;
    Ok(())
}

fn script_trigger_hold_is_revalidatable(reason: Option<&str>) -> bool {
    reason.is_some_and(|reason| {
        reason == "held_on_transfer_current_actor_revalidation_required"
            || reason == "automation_revision_revalidation_required"
            || reason == "script_action_or_route_not_selected"
            || reason.starts_with("admission_revalidation:")
            || reason.starts_with("current_manager_")
            || reason.starts_with("script_owner_")
            || reason.starts_with("script_revision_")
            || reason.starts_with("task_attempt_")
            || reason.starts_with("submission_")
            || reason.starts_with("system_event_")
    })
}

const SCRIPT_REGISTRY_HOLD_PREFIX: &str =
    "admission_revalidation:script_registry_damaged:revision:";

fn script_registry_hold_reason(revision: i64) -> String {
    format!("{SCRIPT_REGISTRY_HOLD_PREFIX}{revision}")
}

fn script_registry_hold_revision(reason: Option<&str>) -> Result<Option<i64>> {
    let Some(revision) = reason.and_then(|reason| reason.strip_prefix(SCRIPT_REGISTRY_HOLD_PREFIX))
    else {
        return Ok(None);
    };
    let revision = revision
        .parse::<i64>()
        .ok()
        .filter(|revision| *revision > 0)
        .ok_or_else(|| {
            Error::new(
                "AUTOMATION_CURSOR_CORRUPT",
                "held ScriptRun registry revision identity is invalid",
            )
        })?;
    Ok(Some(revision))
}

fn script_trigger_block_reason(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    pending: &PendingScriptTrigger,
    app_config: &Config,
) -> Result<Option<String>> {
    let active_revision: Option<i64> = tx
        .query_row(
            "SELECT active_revision FROM scripts WHERE script_id=?1",
            [&pending.script_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    if let Some(failed_revision) = script_registry_hold_revision(pending.held_reason.as_deref())? {
        let Some(active_revision) = active_revision else {
            return Ok(Some(script_registry_hold_reason(failed_revision)));
        };
        if active_revision == failed_revision {
            match super::scripts::script_trigger_revision_snapshot(
                tx,
                &pending.script_id,
                active_revision,
            ) {
                Ok(()) => return Ok(Some(script_registry_hold_reason(failed_revision))),
                Err(error) if error.code == "SCRIPT_REGISTRY_DAMAGED" => {
                    return Ok(Some(script_registry_hold_reason(failed_revision)));
                }
                Err(error) => return Err(error),
            }
        }
        match super::scripts::script_trigger_revision_snapshot(
            tx,
            &pending.script_id,
            active_revision,
        ) {
            Ok(()) => {}
            Err(error) if error.code == "SCRIPT_REGISTRY_DAMAGED" => {
                return Ok(Some(script_registry_hold_reason(active_revision)));
            }
            Err(error) => return Err(error),
        }
    }
    if !entry.script_run_ready() {
        return Ok(Some("script_action_or_route_not_selected".to_owned()));
    }
    if entry
        .script_run
        .as_ref()
        .is_none_or(|settings| settings.script_id != pending.script_id)
    {
        return Ok(Some("script_owner_target_changed".to_owned()));
    }
    let context = match pending.consumer_context.as_ref() {
        Some(context) => context.clone(),
        None => script_trigger_authority::ScriptRunConsumerContext::from_entry(entry)?,
    };
    if context.require_entry_match(entry).is_err() {
        return Ok(Some("script_action_or_route_not_selected".to_owned()));
    }
    if let Err(error) = authorization::require_registered_manager(tx, &entry.owner_manager_id) {
        if error.code == "FORBIDDEN" {
            return Ok(Some(
                "current_manager_not_registered_or_disabled".to_owned(),
            ));
        }
        return Err(error);
    }
    let script_owner: Option<String> = tx
        .query_row(
            "SELECT owner_id FROM scripts WHERE script_id=?1",
            [&pending.script_id],
            |row| row.get(0),
        )
        .optional()?;
    if script_owner.as_deref() != Some(entry.owner_manager_id.as_str()) {
        return Ok(Some(
            "script_owner_does_not_match_current_manager".to_owned(),
        ));
    }
    if active_revision.is_none() {
        return Ok(Some("script_revision_not_active".to_owned()));
    }
    match context.require_current_source(tx, app_config, entry, &pending.cause) {
        Ok(_) => Ok(None),
        Err(error) if script_event_revalidation_error(&error) => {
            if pending.held_reason.as_deref() == Some(SYSTEM_EVENT_SOURCE_PROOF_PENDING)
                && matches!(
                    error.code.as_str(),
                    "SCRIPT_EVENT_SOURCE_UNAUTHORIZED" | "SCRIPT_EVENT_SOURCE_REVOKED"
                )
            {
                Ok(Some(SYSTEM_EVENT_SOURCE_PROOF_PENDING.to_owned()))
            } else {
                Ok(Some(format!(
                    "system_event_{}",
                    error.code.to_ascii_lowercase()
                )))
            }
        }
        Err(error) if script_trigger_revalidation_error(&error) => Ok(Some(format!(
            "submission_{}",
            error.code.to_ascii_lowercase()
        ))),
        Err(error) => Err(error),
    }
}

fn script_trigger_revalidation_error(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "FORBIDDEN"
            | "UNAUTHORIZED"
            | "NOT_FOUND"
            | "STALE_ATTEMPT"
            | "ATTEMPT_SCOPE_STALE"
            | "SUBMISSION_DAMAGED"
            | "AUTOMATION_ACTION_CHANGED"
            | "SCRIPT_REVISION_NOT_ACTIVE"
            | "SCRIPT_SCOPE_CHANGED"
    )
}

fn validate_state(state: &DispatchState, entry: &AutomationEntry) -> Result<()> {
    if state.schema_version != DISPATCH_SCHEMA_VERSION
        || state.owner_manager_id != entry.owner_manager_id
        || state.project_id != entry.project_id
        || state.automation_id != entry.automation_id
        || state.step != AutomationStep::ReviewDispatch.as_str()
        || state.cursor < 0
        || state.hook_cursor < 0
        || state.hook_activation_cut < 0
        || state.pending.len() > MAX_PENDING_SUBJECTS
        || state.recent.len() > MAX_RECENT_DISPOSITIONS
    {
        return Err(Error::new(
            "AUTOMATION_CURSOR_CORRUPT",
            "automation dispatch cursor identity or bounds are invalid",
        ));
    }
    Ok(())
}

fn save_state(db: &rusqlite::Connection, key: &str, state: &DispatchState) -> Result<()> {
    let value = serde_json::to_value(state)?;
    config::write_record(db, key, &value)
}

fn empty_script_trigger_state(
    entry: &AutomationEntry,
    cut: i64,
    now_ms: i64,
) -> ScriptTriggerState {
    ScriptTriggerState {
        schema_version: SCRIPT_TRIGGER_STATE_VERSION,
        owner_manager_id: entry.owner_manager_id.clone(),
        project_id: entry.project_id.clone(),
        automation_id: entry.automation_id.clone(),
        cursor: cut,
        activation_cut: cut,
        catch_up_until: None,
        pending: Vec::new(),
        recent: Vec::new(),
        observed_selector_ids: Vec::new(),
        updated_at_ms: now_ms,
    }
}

fn load_script_trigger_state(
    db: &rusqlite::Connection,
    entry: &AutomationEntry,
) -> Result<Option<ScriptTriggerState>> {
    let key = config::script_dispatch_state_key(
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?;
    let Some(record) = config::read_record(db, &key, "ScriptRun trigger cursor")? else {
        return Ok(None);
    };
    let state: ScriptTriggerState = serde_json::from_value(record).map_err(|_| {
        Error::new(
            "AUTOMATION_CURSOR_CORRUPT",
            "ScriptRun trigger cursor fields are invalid",
        )
    })?;
    validate_script_trigger_state(&state)?;
    if state.owner_manager_id != entry.owner_manager_id
        || state.project_id != entry.project_id
        || state.automation_id != entry.automation_id
    {
        return Err(Error::new(
            "AUTOMATION_CURSOR_CORRUPT",
            "ScriptRun trigger cursor belongs to another automation scope",
        ));
    }
    Ok(Some(state))
}

fn validate_script_trigger_state(state: &ScriptTriggerState) -> Result<()> {
    if state.schema_version != SCRIPT_TRIGGER_STATE_VERSION
        || state.owner_manager_id.is_empty()
        || state.project_id.is_empty()
        || state.automation_id.is_empty()
        || state.cursor < 0
        || state.activation_cut < 0
        || state
            .catch_up_until
            .is_some_and(|cut| cut < state.activation_cut)
        || state.pending.len() > MAX_PENDING_SUBJECTS
        || state.recent.len() > MAX_RECENT_DISPOSITIONS
        || state.observed_selector_ids.len() > crate::automation::event_rules::MAX_EVENT_RULES
        || state
            .observed_selector_ids
            .iter()
            .any(|selector| !valid_event_digest(selector))
        || state.pending.iter().any(|pending| {
            pending.automation_revision <= 0
                || pending.observation_id <= 0
                || pending.script_id.is_empty()
                || pending.consumer_context.as_ref().is_some_and(|context| {
                    !context.matches_pending_identity(
                        &state.owner_manager_id,
                        &state.project_id,
                        &state.automation_id,
                        pending.automation_revision,
                        &pending.script_id,
                    )
                })
                || !valid_pending_script_trigger_cause(pending)
        })
    {
        return Err(Error::new(
            "AUTOMATION_CURSOR_CORRUPT",
            "ScriptRun trigger cursor identity or bounds are invalid",
        ));
    }
    Ok(())
}

fn valid_pending_script_trigger_cause(pending: &PendingScriptTrigger) -> bool {
    let cause = &pending.cause;
    match cause["kind"].as_str() {
        Some("applied_submission") => cause["id"].as_str().is_some_and(|id| !id.is_empty()),
        Some("system_event") => {
            let all_task_scope_absent = ["task_id", "task_revision", "attempt_id"]
                .iter()
                .all(|key| cause.get(*key).is_none_or(Value::is_null));
            let all_task_scope_present = cause["task_id"].as_str().is_some_and(|id| !id.is_empty())
                && cause["task_revision"]
                    .as_i64()
                    .is_some_and(|revision| revision > 0)
                && cause["attempt_id"]
                    .as_str()
                    .is_some_and(|id| !id.is_empty());
            let occurrence_pair_valid = match (
                cause["occurrence_phase"].as_str(),
                cause["occurrence_id"].as_str(),
            ) {
                (None, None) => true,
                (Some(phase), Some(identity)) => {
                    !phase.is_empty() && valid_occurrence_identity(identity)
                }
                _ => false,
            };
            cause["id"].as_str().is_some_and(|id| !id.is_empty())
                && cause["script_id"].as_str() == Some(pending.script_id.as_str())
                && cause["observation_id"].as_i64() == Some(pending.observation_id)
                && cause["source_id"]
                    .as_str()
                    .is_some_and(|value| !value.is_empty())
                && cause["event_kind"]
                    .as_str()
                    .is_some_and(|value| !value.is_empty())
                && cause["recorded_at_ms"]
                    .as_i64()
                    .is_some_and(|value| value >= 0)
                && crate::automation::event_rules::EventStatus::parse_optional(
                    cause["status"].as_str(),
                )
                .is_ok()
                && occurrence_pair_valid
                && (all_task_scope_absent || all_task_scope_present)
        }
        _ => false,
    }
}

fn save_script_trigger_state(
    db: &rusqlite::Connection,
    key: &str,
    state: &ScriptTriggerState,
) -> Result<()> {
    validate_script_trigger_state(state)?;
    config::write_record(db, key, &serde_json::to_value(state)?)
}

fn finish_script_trigger_catch_up(state: &mut ScriptTriggerState, intake: IntakeSnapshot) {
    if state
        .catch_up_until
        .is_some_and(|cut| state.cursor >= cut && intake.observation_high_water >= cut)
    {
        state.catch_up_until = None;
    }
}

fn script_trigger_state_projection(
    state: &ScriptTriggerState,
    entry: &AutomationEntry,
    processed: usize,
    intake: IntakeSnapshot,
) -> Value {
    let waiting_for = entry
        .event_rules
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|rule| rule.action == crate::automation::event_rules::EventRuleAction::ScriptRun)
        .filter_map(|rule| {
            let selector_id = script_event_selector_id(rule).ok()?;
            if state.observed_selector_ids.contains(&selector_id) {
                return None;
            }
            let (source_id, event_kind) = rule.selected_source_kind()?;
            Some(json!({
                "source_id":source_id,
                "event_kind":event_kind,
                "status":rule.status.map(crate::automation::event_rules::EventStatus::as_str),
                "reason":"awaiting_matching_authorized_event"
            }))
        })
        .collect::<Vec<_>>();
    json!({
        "automation_id":state.automation_id,
        "step":"script_run",
        "cursor":state.cursor,
        "activation_cut":state.activation_cut,
        "catch_up_until":state.catch_up_until,
        "pending":state.pending,
        "recent":state.recent,
        "waiting_for":waiting_for,
        "processed":processed,
        "high_water":intake.high_water,
        "intake_cursor":intake.cursor,
        "observation_high_water":intake.observation_high_water,
        "intake_status":intake.status,
        "updated_at_ms":state.updated_at_ms
    })
}

fn remember_script_trigger_recent(state: &mut ScriptTriggerState, event: Value) {
    state.recent.push(event);
    if state.recent.len() > MAX_RECENT_DISPOSITIONS {
        let excess = state.recent.len() - MAX_RECENT_DISPOSITIONS;
        state.recent.drain(0..excess);
    }
}

fn empty_state(entry: &AutomationEntry, cut: i64, now_ms: i64) -> DispatchState {
    DispatchState {
        schema_version: DISPATCH_SCHEMA_VERSION,
        owner_manager_id: entry.owner_manager_id.clone(),
        project_id: entry.project_id.clone(),
        automation_id: entry.automation_id.clone(),
        step: AutomationStep::ReviewDispatch.as_str().to_owned(),
        cursor: cut,
        hook_cursor: cut,
        hook_activation_cut: cut,
        hook_include_existing: false,
        activation_cut: cut,
        catch_up_until: None,
        pending_after_observation_id: 0,
        pending: Vec::new(),
        recent: Vec::new(),
        updated_at_ms: now_ms,
    }
}

fn state_projection(state: &DispatchState) -> Value {
    json!({
        "schema_version":state.schema_version,
        "step":state.step,
        "cursor":state.cursor,
        "hook_cursor":state.hook_cursor,
        "hook_activation_cut":state.hook_activation_cut,
        "hook_include_existing":state.hook_include_existing,
        "activation_cut":state.activation_cut,
        "catch_up_until":state.catch_up_until,
        "pending":state.pending,
        "recent":state.recent,
        "updated_at_ms":state.updated_at_ms
    })
}

fn pending_observation_id(pending: &PendingSubject) -> i64 {
    pending.cause["observation_id"].as_i64().unwrap_or(i64::MAX)
}

fn pending_cause_id(pending: &PendingSubject) -> &str {
    pending.cause["id"].as_str().unwrap_or_default()
}

fn remember_recent(state: &mut DispatchState, event: Value) {
    state.recent.push(event);
    if state.recent.len() > MAX_RECENT_DISPOSITIONS {
        let excess = state.recent.len() - MAX_RECENT_DISPOSITIONS;
        state.recent.drain(0..excess);
    }
}

#[cfg(test)]
mod script_event_trigger_tests {
    use super::*;
    use crate::automation::{
        actions::AutomationStep,
        config::{AutomationEntry, ScriptRunSettings},
        event_rules::{EventRule, EventRuleAction, EventStatus},
    };
    use rusqlite::{Connection, Transaction, params};
    use serde_json::{Value, json};
    use std::collections::BTreeSet;

    const MANAGER_ID: &str = "script-event-manager";
    const PROJECT_ID: &str = "script-event-project";
    const AUTOMATION_ID: &str = "script-event-route";
    const SCRIPT_ID: &str = "script_event_fixture";
    const OPERATION_ID: &str = "coordination-consult-fixture";

    fn fixture() -> (Connection, AutomationEntry, i64, i64, i64) {
        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        db.execute_batch(super::super::SCHEMA).unwrap();
        db.execute_batch(super::super::WORKSPACE_SCHEMA).unwrap();
        db.execute_batch(super::super::SCRIPT_SCHEMA).unwrap();
        super::super::set_meta(
            &db,
            &format!("client:{MANAGER_ID}"),
            &json!({"role":"manager","disabled":false}),
        )
        .unwrap();
        super::super::set_meta(
            &db,
            "gm",
            &json!({"client_id":MANAGER_ID,"binding_id":null,"binding_generation":null,"epoch":1}),
        )
        .unwrap();

        let tx = db.transaction().unwrap();
        tx.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,'event-source-fixture','coordination.consult','{}','{}','settled','{}',1,1,1,1)",
            params![OPERATION_ID, MANAGER_ID],
        )
        .unwrap();
        let raw_id = insert_event(
            &tx,
            "controller",
            "raw-coordination-consult",
            "coordination.consult",
            json!({"private_answer":"PRIVATE CARD TEXT","recipient":"PRIVATE RECIPIENT"}),
        );
        let sent_id = insert_event(
            &tx,
            "controller:messages",
            &format!("sent:{OPERATION_ID}"),
            "message.sent",
            json!({
                "schema_version":1,
                "status":"sent",
                "phase":"message_send_committed",
                "occurrence_id":format!("operation:{OPERATION_ID}:message_send_committed")
            }),
        );
        let answer_id = insert_event(
            &tx,
            "controller:coordination",
            &format!("answer:{OPERATION_ID}"),
            "coordination.answer",
            json!({
                "schema_version":1,
                "status":"answered",
                "phase":"coordination_answered",
                "occurrence_id":format!("operation:{OPERATION_ID}:coordination_answered")
            }),
        );
        tx.commit().unwrap();

        let mut entry = AutomationEntry::new(MANAGER_ID, PROJECT_ID, AUTOMATION_ID, 1);
        entry.enabled = true;
        entry.steps = vec![AutomationStep::ScriptRun];
        entry.script_run = Some(ScriptRunSettings {
            script_id: SCRIPT_ID.to_owned(),
        });
        entry.event_rules = Some(vec![
            selector("controller", "coordination.consult", None),
            selector(
                "controller:messages",
                "message.sent",
                Some(EventStatus::Sent),
            ),
            selector(
                "controller:coordination",
                "coordination.answer",
                Some(EventStatus::Answered),
            ),
        ]);
        (db, entry, raw_id, sent_id, answer_id)
    }

    fn selector(source_id: &str, event_kind: &str, status: Option<EventStatus>) -> EventRule {
        EventRule {
            source: None,
            predicate: None,
            source_id: Some(source_id.to_owned()),
            event_kind: Some(event_kind.to_owned()),
            status,
            action: EventRuleAction::ScriptRun,
        }
    }

    fn insert_event(
        tx: &Transaction<'_>,
        source_id: &str,
        source_event_key: &str,
        event_kind: &str,
        payload: Value,
    ) -> i64 {
        tx.execute(
            "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,10)",
            params![
                source_id,
                source_event_key,
                OPERATION_ID,
                event_kind,
                crate::model::canonical(&payload).unwrap(),
            ],
        )
        .unwrap();
        tx.last_insert_rowid()
    }

    #[test]
    fn coordination_consult_preserves_two_exact_safe_phases_and_never_exposes_payload() {
        let (mut db, entry, raw_id, sent_id, answer_id) = fixture();
        let tx = db.transaction().unwrap();
        let raw = automation_intake::observed_event_by_id(&tx, raw_id)
            .unwrap()
            .unwrap();
        let sent = automation_intake::observed_event_by_id(&tx, sent_id)
            .unwrap()
            .unwrap();
        let answer = automation_intake::observed_event_by_id(&tx, answer_id)
            .unwrap()
            .unwrap();
        let mut state = empty_script_trigger_state(&entry, 0, 10);

        process_system_event_script_trigger(&tx, &Config::default(), &entry, &mut state, &raw)
            .unwrap();
        assert_eq!(state.pending.len(), 2);
        assert_ne!(state.pending[0].cause["id"], state.pending[1].cause["id"]);
        assert_eq!(
            state
                .pending
                .iter()
                .map(|pending| pending.cause["occurrence_phase"].as_str().unwrap())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["message_send_committed", "coordination_answered"]),
        );

        // The same two durable normalized views must coalesce with their raw
        // Operation event, while the two real phases above remain distinct.
        process_system_event_script_trigger(&tx, &Config::default(), &entry, &mut state, &sent)
            .unwrap();
        process_system_event_script_trigger(&tx, &Config::default(), &entry, &mut state, &answer)
            .unwrap();
        assert_eq!(state.pending.len(), 2);

        for pending in &state.pending {
            let cause = &pending.cause;
            assert_eq!(cause["source_id"], "controller");
            assert_eq!(cause["event_kind"], "coordination.consult");
            let mut retained_cause = cause.clone();
            retained_cause["script_revision"] = json!(1);
            let retained =
                validate_retained_script_event_cause(&tx, PROJECT_ID, &retained_cause).unwrap();
            let context =
                script_event_invocation_context(&tx, &Config::default(), &entry, cause).unwrap();
            assert_eq!(context.task_id, None);
            assert_eq!(context.task_revision, None);
            assert_eq!(context.attempt_id, None);
            assert_eq!(context.input["operation_id"], OPERATION_ID);
            assert_eq!(context.input["event_kind"], "coordination.consult");
            assert!(!context.input.to_string().contains("PRIVATE CARD TEXT"));
            assert!(!context.input.to_string().contains("PRIVATE RECIPIENT"));
            assert!(context.input.get("payload").is_none());
            assert_eq!(retained, context.input);
        }

        let manager_disabled = crate::model::canonical(&json!({
            "role":"manager",
            "disabled":true
        }))
        .unwrap();
        tx.execute(
            "UPDATE meta SET value_json=?2 WHERE key=?1",
            params![format!("client:{MANAGER_ID}"), manager_disabled],
        )
        .unwrap();
        let revoked = script_event_invocation_context(
            &tx,
            &Config::default(),
            &entry,
            &state.pending[0].cause,
        );
        let error = match revoked {
            Ok(_) => panic!("disabled current Manager must not admit a pending event run"),
            Err(error) => error,
        };
        assert_eq!(error.code, "FORBIDDEN");
        let script_runs: i64 = tx
            .query_row(
                "SELECT count(*) FROM operations WHERE method='script.run'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            script_runs, 0,
            "event projection must not itself admit a run"
        );
    }

    #[test]
    fn raw_runtime_unknown_aliases_only_the_exact_operation_unknown_occurrence() {
        let (mut db, _, _, _, _) = fixture();
        let tx = db.transaction().unwrap();
        let oversized_unknown_detail = format!("PRIVATE_UNKNOWN_DETAIL:{}", "x".repeat(64 * 1024));
        let raw_unknown = insert_event(
            &tx,
            "module:runtime-fixture",
            "outcome-unknown-raw",
            "runtime.outcome",
            json!({
                "operation_id": OPERATION_ID,
                "outcome": "unknown",
                "details": {
                    "private_native_detail":"must not escape",
                    "oversized_private_detail":oversized_unknown_detail.as_str()
                }
            }),
        );
        let raw_applied = insert_event(
            &tx,
            "module:runtime-fixture",
            "outcome-applied-raw",
            "runtime.outcome",
            json!({
                "operation_id": OPERATION_ID,
                "outcome": "applied",
                "details": {"private_native_detail":"must not escape"}
            }),
        );
        let raw_accepted = insert_event(
            &tx,
            "module:runtime-fixture",
            "outcome-accepted-raw",
            "runtime.outcome",
            json!({
                "operation_id": OPERATION_ID,
                "outcome": "accepted",
                "details": {"private_native_detail":"must not escape"}
            }),
        );
        let operation_unknown = insert_event(
            &tx,
            "controller:operations",
            &format!("operation:{OPERATION_ID}:operation_outcome_unknown"),
            "operation.outcome_unknown",
            json!({
                "schema_version":1,
                "phase":"operation_outcome_unknown",
                "status":"unknown",
                "occurrence_id":format!("operation:{OPERATION_ID}:operation_outcome_unknown"),
                "error_code":"OUTCOME_UNKNOWN"
            }),
        );
        let native_applied = insert_event(
            &tx,
            "controller:runtime",
            &format!("terminal:{OPERATION_ID}"),
            "native.operation.completed",
            json!({
                "schema_version":1,
                "phase":"native_outcome_terminal",
                "status":"applied",
                "occurrence_id":format!("operation:{OPERATION_ID}:native_outcome_terminal")
            }),
        );
        tx.commit().unwrap();

        let observed = |observation_id| {
            automation_intake::observed_event_by_id(&db, observation_id)
                .unwrap()
                .unwrap()
        };
        let unknown_event = observed(raw_unknown);
        let oversized_unknown_receipts: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM observations WHERE observation_id=?1 \
                 AND length(CAST(payload_json AS BLOB))>48*1024",
                [raw_unknown],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(oversized_unknown_receipts, 1);
        let unknown_projections = script_event_projections_with_alias(&db, &unknown_event).unwrap();
        assert_eq!(unknown_projections.len(), 1);
        assert_eq!(unknown_projections[0].status, Some(EventStatus::Unknown));
        assert_eq!(
            unknown_projections[0].error_code.as_deref(),
            Some("OUTCOME_UNKNOWN")
        );
        assert_eq!(
            unknown_projections[0].occurrence_phase.as_deref(),
            Some("operation_outcome_unknown")
        );
        assert!(!format!("{:?}", unknown_projections[0]).contains("must not escape"));
        assert!(!format!("{:?}", unknown_projections[0]).contains(&oversized_unknown_detail));
        assert_eq!(
            unknown_projections[0].occurrence_id.as_deref(),
            Some(format!("operation:{OPERATION_ID}:operation_outcome_unknown").as_str())
        );
        assert_eq!(
            system_event_semantic_id(raw_unknown, &unknown_projections[0]).unwrap(),
            system_event_semantic_id(
                operation_unknown,
                &automation_intake::safe_event_projection(&db, &observed(operation_unknown))
                    .unwrap()
            )
            .unwrap()
        );

        let applied_event = observed(raw_applied);
        let applied_projections = script_event_projections_with_alias(&db, &applied_event).unwrap();
        assert_eq!(applied_projections.len(), 1);
        assert_eq!(
            applied_projections[0].occurrence_phase.as_deref(),
            Some("native_outcome_terminal")
        );
        assert_eq!(
            applied_projections[0].occurrence_id.as_deref(),
            Some(format!("operation:{OPERATION_ID}:native_outcome_terminal").as_str())
        );

        let accepted_event = observed(raw_accepted);
        assert!(
            script_event_projections_with_alias(&db, &accepted_event)
                .unwrap()
                .is_empty(),
            "an unbound synthetic receipt cannot impersonate the accepted-writer projection"
        );
        assert_ne!(
            unknown_projections[0].occurrence_phase.as_deref(),
            automation_intake::safe_event_projection(&db, &observed(native_applied))
                .unwrap()
                .occurrence_phase
                .as_deref(),
            "unknown native outcome must never acquire the completion phase"
        );
    }

    #[test]
    fn accepted_runtime_outcome_is_statusless_safe_and_semantically_deduplicated() {
        const MODULE_ID: &str = "accepted-event-module";
        const MODULE_LINK_ID: &str = "accepted-event-link";
        const BINDING_ID: &str = "accepted-event-binding";
        const OTHER_BINDING_ID: &str = "accepted-event-other-binding";
        const ACCEPTED_OPERATION_ID: &str = "accepted-native-operation";
        const PRIVATE_DETAIL: &str = "PRIVATE_NATIVE_RECEIPT_DETAIL";
        let oversized_private_detail =
            format!("PRIVATE_LARGE_NATIVE_DETAIL:{}", "x".repeat(64 * 1024));

        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        db.execute_batch(include_str!("../../migrations/001_core.sql"))
            .unwrap();
        db.execute_batch(super::super::WORKSPACE_SCHEMA).unwrap();
        db.execute_batch(super::super::SCRIPT_SCHEMA).unwrap();
        super::super::set_meta(
            &db,
            &format!("client:{MANAGER_ID}"),
            &json!({"role":"manager","disabled":false}),
        )
        .unwrap();
        super::super::set_meta(
            &db,
            "gm",
            &json!({"client_id":MANAGER_ID,"binding_id":null,"binding_generation":null,"epoch":1}),
        )
        .unwrap();
        super::super::set_meta(
            &db,
            &format!("client:{MODULE_ID}"),
            &json!({
                "role":"module",
                "disabled":false,
                "binding_id":BINDING_ID,
                "binding_generation":1
            }),
        )
        .unwrap();
        let binding_state = json!({
            "module_client_id":MODULE_ID,
            "module_link_id":MODULE_LINK_ID
        });
        db.execute(
            "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,state_json,created_at_ms) \
             VALUES(?1,1,'accepted-event-lane','accepted-event-instance','accepted-event-artifact','ready',NULL,NULL,'{}',?2,1)",
            params![BINDING_ID, model::canonical(&binding_state).unwrap()],
        )
        .unwrap();
        db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,binding_id,binding_generation,state,native_refs_json,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) \
             VALUES(?1,?2,'accepted-event-send','agent.send',?4,'{}',?3,1,'sending','{}',NULL,1,NULL,1,1)",
            params![
                ACCEPTED_OPERATION_ID,
                MANAGER_ID,
                BINDING_ID,
                model::canonical(&json!({"text":"PRIVATE_INPUT_TEXT"})).unwrap(),
            ],
        )
        .unwrap();

        let principal = Principal {
            link_id: MODULE_LINK_ID.to_owned(),
            client_id: MODULE_ID.to_owned(),
            role: Role::Module,
        };
        for (index, receipt_detail) in ["first receipt", "second receipt"].iter().enumerate() {
            super::super::runtime::outcome(
                &mut db,
                &principal,
                &json!({
                    "operation_id":ACCEPTED_OPERATION_ID,
                    "outcome":"accepted",
                    "native_scope_key":null,
                    "native_root_id":null,
                    "turn_id":null,
                    "native_input_id":"PRIVATE_NATIVE_INPUT_ID",
                    "details":{
                        "private_native_detail":PRIVATE_DETAIL,
                        "oversized_private_detail":if index == 0 { PRIVATE_DETAIL } else { oversized_private_detail.as_str() },
                        "private_link_id":MODULE_LINK_ID,
                        "receipt_note":receipt_detail
                    }
                }),
            )
            .unwrap();
        }

        let actual_ids = {
            let mut statement = db
                .prepare(
                    "SELECT observation_id FROM observations \
                     WHERE source_stream_id=?1 AND operation_id=?2 AND kind='runtime.outcome' \
                     ORDER BY observation_id",
                )
                .unwrap();
            statement
                .query_map(
                    params![format!("module:{MODULE_ID}"), ACCEPTED_OPERATION_ID],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert_eq!(actual_ids.len(), 2, "distinct native receipts are retained");
        let oversized_receipts: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM observations WHERE observation_id IN (?1,?2) \
                 AND length(CAST(payload_json AS BLOB))>48*1024",
                params![actual_ids[0], actual_ids[1]],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            oversized_receipts, 1,
            "one real writer receipt exceeds 48 KiB"
        );

        fn insert_raw_outcome(
            tx: &Transaction<'_>,
            source_id: &str,
            binding_id: &str,
            operation_id: &str,
            embedded_operation_id: &str,
            detail: &str,
        ) -> i64 {
            let payload = json!({
                "operation_id":embedded_operation_id,
                "outcome":"accepted",
                "native_scope_key":null,
                "native_root_id":null,
                "turn_id":null,
                "details":{"private_native_detail":detail}
            });
            let encoded = model::canonical(&payload).unwrap();
            let event_key = format!(
                "outcome:{operation_id}:{}",
                model::digest(encoded.as_bytes())
            );
            tx.execute(
                "INSERT INTO observations(source_stream_id,source_event_key,binding_id,binding_generation,operation_id,kind,payload_json,recorded_at_ms) \
                 VALUES(?1,?2,?3,1,?4,'runtime.outcome',?5,2)",
                params![source_id, event_key, binding_id, operation_id, encoded],
            )
            .unwrap();
            tx.last_insert_rowid()
        }

        let (forged_source_id, forged_binding_id, forged_payload_id) = {
            let tx = db.transaction().unwrap();
            let other_binding_state = json!({
                "module_client_id":MODULE_ID,
                "module_link_id":"forged-binding-link"
            });
            tx.execute(
                "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,state_json,created_at_ms) \
                 VALUES(?1,1,'forged-event-lane','forged-event-instance','forged-event-artifact','ready',NULL,NULL,'{}',?2,1)",
                params![OTHER_BINDING_ID, model::canonical(&other_binding_state).unwrap()],
            )
            .unwrap();
            let source = insert_raw_outcome(
                &tx,
                "module:forged-source",
                BINDING_ID,
                ACCEPTED_OPERATION_ID,
                ACCEPTED_OPERATION_ID,
                "forged source",
            );
            let binding = insert_raw_outcome(
                &tx,
                &format!("module:{MODULE_ID}"),
                OTHER_BINDING_ID,
                ACCEPTED_OPERATION_ID,
                ACCEPTED_OPERATION_ID,
                "forged operation binding tuple",
            );
            let embedded = insert_raw_outcome(
                &tx,
                &format!("module:{MODULE_ID}"),
                BINDING_ID,
                ACCEPTED_OPERATION_ID,
                "forged-embedded-operation",
                "forged embedded operation identity",
            );
            tx.commit().unwrap();
            (source, binding, embedded)
        };

        let actual_events = actual_ids
            .iter()
            .map(|id| {
                automation_intake::observed_event_by_id(&db, *id)
                    .unwrap()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let actual_projections = actual_events
            .iter()
            .map(|event| script_event_projections_with_alias(&db, event).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual_projections.len(), 2);
        for projections in &actual_projections {
            assert_eq!(projections.len(), 1);
            assert_eq!(projections[0].status, None);
            assert_eq!(projections[0].error_code, None);
            assert_eq!(
                projections[0].occurrence_phase.as_deref(),
                Some("native_input_accepted")
            );
            assert_eq!(
                projections[0].occurrence_id.as_deref(),
                Some(format!("operation:{ACCEPTED_OPERATION_ID}:native_input_accepted").as_str())
            );
            assert!(!format!("{:?}", projections[0]).contains(PRIVATE_DETAIL));
            assert!(!format!("{:?}", projections[0]).contains(&oversized_private_detail));
        }
        assert_eq!(
            system_event_semantic_id(actual_ids[0], &actual_projections[0][0]).unwrap(),
            system_event_semantic_id(actual_ids[1], &actual_projections[1][0]).unwrap(),
            "accepted receipts share one stable Operation phase identity"
        );
        let statusless_rule = selector("module:accepted-event-module", "runtime.outcome", None);
        let completed_rule = selector(
            "module:accepted-event-module",
            "runtime.outcome",
            Some(EventStatus::Completed),
        );
        assert!(statusless_rule.matches_safe_event(
            &actual_events[0].source_id,
            &actual_events[0].event_kind,
            actual_projections[0][0].status
        ));
        assert!(!completed_rule.matches_safe_event(
            &actual_events[0].source_id,
            &actual_events[0].event_kind,
            actual_projections[0][0].status
        ));
        let terminal_aliases: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM observations WHERE source_stream_id='controller:runtime' \
                 AND operation_id=?1 AND kind='native.operation.completed'",
                [ACCEPTED_OPERATION_ID],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(terminal_aliases, 0, "accepted is not a terminal completion");

        for id in [forged_source_id, forged_binding_id, forged_payload_id] {
            let event = automation_intake::observed_event_by_id(&db, id)
                .unwrap()
                .unwrap();
            assert!(
                script_event_projections_with_alias(&db, &event)
                    .unwrap()
                    .is_empty(),
                "forged source, Operation/binding tuple, or embedded identity stays suppressed"
            );
        }

        let mut entry = AutomationEntry::new(MANAGER_ID, PROJECT_ID, AUTOMATION_ID, 1);
        entry.enabled = true;
        entry.steps = vec![AutomationStep::ScriptRun];
        entry.script_run = Some(ScriptRunSettings {
            script_id: SCRIPT_ID.to_owned(),
        });
        entry.event_rules = Some(vec![statusless_rule]);
        let mut state = empty_script_trigger_state(&entry, 0, 10);
        let app_config = Config::default();
        let tx = db.transaction().unwrap();
        process_system_event_script_trigger(
            &tx,
            &app_config,
            &entry,
            &mut state,
            &actual_events[0],
        )
        .unwrap();
        assert_eq!(state.pending.len(), 1);
        process_system_event_script_trigger(
            &tx,
            &app_config,
            &entry,
            &mut state,
            &actual_events[1],
        )
        .unwrap();
        assert_eq!(
            state.pending.len(),
            1,
            "the second receipt coalesces by phase"
        );
        let cause = &state.pending[0].cause;
        assert!(
            cause["status"].is_null(),
            "the common retained cause encodes an absent normalized status as null"
        );
        assert_eq!(cause["occurrence_phase"], "native_input_accepted");
        assert_eq!(
            cause["occurrence_id"],
            format!("operation:{ACCEPTED_OPERATION_ID}:native_input_accepted")
        );
        assert!(cause.get("task_id").is_none());
        assert!(cause.get("attempt_id").is_none());
        let reread = script_event_invocation_context(&tx, &app_config, &entry, cause).unwrap();
        assert_eq!(reread.input["operation_id"], ACCEPTED_OPERATION_ID);
        assert!(reread.input.get("status").is_none());
        assert!(reread.input.get("error_code").is_none());
        assert!(reread.input.get("task_id").is_none());
        let rendered = reread.input.to_string();
        assert!(!rendered.contains(PRIVATE_DETAIL));
        assert!(!rendered.contains("PRIVATE_NATIVE_INPUT_ID"));
        assert!(!rendered.contains(MODULE_LINK_ID));
        assert!(!rendered.contains("PRIVATE_INPUT_TEXT"));
    }

    #[test]
    fn store_operation_cancel_commits_one_safe_taskless_script_run_event() {
        const HISTORICAL_OPERATION_ID: &str = "historical-cancel-before-event-schema";
        const TARGET_OPERATION_ID: &str = "queued-cancel-event-target";
        const PRIVATE_CANCEL_REASON: &str = "PRIVATE_CANCEL_REASON_FIXTURE";
        const PRIVATE_TARGET_REQUEST: &str = "PRIVATE_TARGET_REQUEST_FIXTURE";
        const PRIVATE_OLD_REASON: &str = "PRIVATE_OLD_CANCEL_REASON_FIXTURE";

        let (mut db, _, _, _, _) = fixture();
        // The real cancellation writer checks owned-service reservations even
        // for a taskless target; use the same installed table as the Store.
        db.execute_batch(super::super::OWNED_SERVICE_SCHEMA)
            .unwrap();
        {
            let tx = db.transaction().unwrap();
            tx.execute(
                "INSERT INTO operations(
                    operation_id,caller_id,client_request_id,method,original_request_json,
                    effective_request_json,state,result_json,due_at_ms,settled_at_ms,
                    created_at_ms,updated_at_ms
                 ) VALUES(?1,?2,'historical-cancel-request','coordination.consult',?3,'{}',
                    'cancelled',?4,1,1,1,1)",
                params![
                    HISTORICAL_OPERATION_ID,
                    MANAGER_ID,
                    format!(r#"{{"request":"{PRIVATE_TARGET_REQUEST}"}}"#),
                    format!(r#"{{"reason":"{PRIVATE_OLD_REASON}"}}"#),
                ],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO operations(
                    operation_id,caller_id,client_request_id,method,original_request_json,
                    effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms
                 ) VALUES(?1,?2,'queued-cancel-target','coordination.consult',?3,'{}',
                    'queued',1,1,1)",
                params![
                    TARGET_OPERATION_ID,
                    MANAGER_ID,
                    format!(r#"{{"request":"{PRIVATE_TARGET_REQUEST}"}}"#),
                ],
            )
            .unwrap();
            tx.commit().unwrap();
        }

        // Installing the forward-only adapter after an existing cancellation
        // must leave that historical Operation without a new occurrence.
        {
            let tx = db.transaction().unwrap();
            super::super::operation_cancel_event_schema::install(&tx).unwrap();
            let historical_events: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM observations
                     WHERE source_stream_id='controller:operations'
                       AND source_event_key=?1 AND kind='operation.cancelled'",
                    [format!(
                        "operation:{HISTORICAL_OPERATION_ID}:operation_cancelled"
                    )],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(historical_events, 0);
            tx.commit().unwrap();
        }

        let mut entry = AutomationEntry::new(MANAGER_ID, PROJECT_ID, AUTOMATION_ID, 1);
        entry.enabled = true;
        entry.steps = vec![AutomationStep::ScriptRun];
        entry.script_run = Some(ScriptRunSettings {
            script_id: SCRIPT_ID.to_owned(),
        });
        entry.event_rules = Some(vec![selector(
            "controller:operations",
            "operation.cancelled",
            Some(EventStatus::Cancelled),
        )]);
        {
            let tx = db.transaction().unwrap();
            let cut = super::automation_intake::observed_event_high_water(&tx).unwrap();
            super::configure_script_trigger_activation(&tx, None, &entry, false, cut, 20).unwrap();
            tx.commit().unwrap();
        }

        let manager = crate::model::Principal {
            link_id: "script-event-cancel-manager-link".to_owned(),
            client_id: MANAGER_ID.to_owned(),
            role: crate::model::Role::Manager,
        };
        let cancel_request = json!({
            "client_request_id":"cancel-target-once",
            "operation_id":TARGET_OPERATION_ID,
            "reason":PRIVATE_CANCEL_REASON,
        });
        let first_receipt = super::super::mutate(
            &mut db,
            &manager,
            "operation.cancel",
            &cancel_request,
            &Config::default(),
        )
        .unwrap();
        assert_ne!(first_receipt["operation_id"], TARGET_OPERATION_ID);
        assert_eq!(first_receipt["cancelled_operation_id"], TARGET_OPERATION_ID);

        let target: (String, Option<String>, Option<i64>) = db
            .query_row(
                "SELECT state,result_json,settled_at_ms FROM operations WHERE operation_id=?1",
                [TARGET_OPERATION_ID],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(target.0, "cancelled");
        assert!(target.1.as_deref().unwrap().contains(PRIVATE_CANCEL_REASON));
        assert!(target.2.is_some());

        let event_key = format!("operation:{TARGET_OPERATION_ID}:operation_cancelled");
        let (event_id, event_operation_id, event_kind, event_payload): (
            i64,
            String,
            String,
            String,
        ) = db
            .query_row(
                "SELECT observation_id,operation_id,kind,payload_json FROM observations
                 WHERE source_stream_id='controller:operations' AND source_event_key=?1",
                [&event_key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(event_operation_id, TARGET_OPERATION_ID);
        assert_eq!(event_kind, "operation.cancelled");
        let payload: Value = serde_json::from_str(&event_payload).unwrap();
        assert_eq!(
            payload,
            json!({
                "schema_version":1,
                "phase":"operation_cancelled",
                "status":"cancelled",
                "occurrence_id":event_key.clone(),
                "error_code":"OPERATION_CANCELLED"
            })
        );

        let app_config = Config::default();
        let first_state = {
            let tx = db.transaction().unwrap();
            let intake = super::reconcile_source_intake(&tx, 64, false, 30).unwrap();
            super::reconcile_script_trigger_entry(&tx, &entry, 16, intake, &app_config, 30)
                .unwrap();
            let state = super::load_script_trigger_state(&tx, &entry)
                .unwrap()
                .unwrap();
            tx.commit().unwrap();
            state
        };
        assert_eq!(first_state.pending.len(), 1);
        assert!(first_state.cursor >= event_id);
        let first_cause = first_state.pending[0].cause.clone();
        assert_eq!(first_cause["observation_id"], event_id);
        assert_eq!(first_cause["operation_id"], TARGET_OPERATION_ID);
        assert_eq!(first_cause["source_id"], "controller:operations");
        assert_eq!(first_cause["event_kind"], "operation.cancelled");
        assert_eq!(first_cause["occurrence_phase"], "operation_cancelled");
        assert_eq!(first_cause["occurrence_id"], event_key);
        assert_eq!(first_cause["status"], "cancelled");
        assert_eq!(first_cause["error_code"], "OPERATION_CANCELLED");
        assert!(first_cause.get("task_id").is_none());
        assert!(first_cause.get("attempt_id").is_none());
        assert!(first_cause.get("payload").is_none());
        assert!(first_cause.get("request").is_none());
        assert!(first_cause.get("result").is_none());

        let mut retained_cause = first_cause.clone();
        retained_cause["script_revision"] = json!(1);
        let retained_input =
            super::validate_retained_script_event_cause(&db, PROJECT_ID, &retained_cause).unwrap();
        let context =
            super::script_event_invocation_context(&db, &app_config, &entry, &first_cause).unwrap();
        assert_eq!(retained_input, context.input);
        assert_eq!(context.task_id, None);
        assert_eq!(context.task_revision, None);
        assert_eq!(context.attempt_id, None);
        assert_eq!(context.input["operation_id"], TARGET_OPERATION_ID);
        assert_eq!(context.input["status"], "cancelled");
        assert_eq!(context.input["error_code"], "OPERATION_CANCELLED");
        assert!(context.input.get("task_id").is_none());
        assert!(context.input.get("attempt_id").is_none());
        assert!(context.input.get("payload").is_none());
        assert!(context.input.get("request").is_none());
        assert!(context.input.get("result").is_none());
        let safe_views = format!("{payload} {first_cause} {}", context.input);
        for private in [
            PRIVATE_CANCEL_REASON,
            PRIVATE_TARGET_REQUEST,
            PRIVATE_OLD_REASON,
        ] {
            assert!(!safe_views.contains(private));
        }
        let task_count: i64 = db
            .query_row("SELECT COUNT(*) FROM tasks", [], |row| row.get(0))
            .unwrap();
        assert_eq!(task_count, 0);

        // Replaying the exact Store request and reconciling the ordinary
        // cursor again must preserve one source fact and one pending ScriptRun.
        let retry_receipt = super::super::mutate(
            &mut db,
            &manager,
            "operation.cancel",
            &cancel_request,
            &app_config,
        )
        .unwrap();
        assert_eq!(retry_receipt, first_receipt);
        let event_count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM observations
                 WHERE source_stream_id='controller:operations'
                   AND source_event_key=?1 AND kind='operation.cancelled'",
                [&event_key],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event_count, 1);

        let second_state = {
            let tx = db.transaction().unwrap();
            let intake = super::reconcile_source_intake(&tx, 64, false, 40).unwrap();
            super::reconcile_script_trigger_entry(&tx, &entry, 16, intake, &app_config, 40)
                .unwrap();
            let state = super::load_script_trigger_state(&tx, &entry)
                .unwrap()
                .unwrap();
            tx.commit().unwrap();
            state
        };
        assert_eq!(second_state.pending.len(), 1);
        assert_eq!(second_state.pending[0].cause["id"], first_cause["id"]);
        assert_eq!(second_state.cursor, first_state.cursor);
    }
}

#[cfg(test)]
mod hook_source_admin_event_tests {
    use super::*;
    use crate::automation::{
        actions::AutomationStep,
        config::{AutomationEntry, ScriptRunSettings},
        event_rules::{EventRule, EventRuleAction},
    };
    use crate::{
        config::Config,
        hooks::contract::HookSetupRequest,
        model::{self, Credential, Principal, Role},
    };
    use rusqlite::Connection;
    use serde_json::{Value, json};

    const MANAGER_ID: &str = "hook-admin-event-manager";
    const OPERATOR_ID: &str = "hook-admin-event-operator";
    const PROJECT_ID: &str = "hook-admin-event-project";
    const AUTOMATION_ID: &str = "hook_admin_event_route";
    const SCRIPT_ID: &str = "hook_admin_event_fixture";

    fn event_rule(event_kind: &str) -> EventRule {
        EventRule {
            source: None,
            predicate: None,
            source_id: Some("controller:hook-source".to_owned()),
            event_kind: Some(event_kind.to_owned()),
            status: None,
            action: EventRuleAction::ScriptRun,
        }
    }

    fn app_config() -> Config {
        let mut config = Config::default();
        config.forge.enabled = true;
        config.forge.projects.insert(
            PROJECT_ID.to_owned(),
            crate::forge::ForgeProject {
                canonical_repository: "github.com/owner/hook-admin-fixture".to_owned(),
                repository_path: std::env::temp_dir().join("hook-admin-fixture-repository"),
                remote_name: "origin".to_owned(),
                policy_revision: crate::policy::OWNER_POLICY_V2_ID.to_owned(),
                target_refs: vec!["refs/heads/main".to_owned()],
            },
        );
        config.workspace.projects.insert(
            PROJECT_ID.to_owned(),
            crate::workspace::WorkspaceProjectConfig {
                allowed_roots: vec![std::env::temp_dir().join("hook-admin-fixture-workspaces")],
            },
        );
        config
    }

    fn fixture() -> (Connection, Config, AutomationEntry, Principal) {
        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        db.execute_batch(super::super::SCHEMA).unwrap();
        db.execute_batch(super::super::WORKSPACE_SCHEMA).unwrap();
        db.execute_batch(super::super::SCRIPT_SCHEMA).unwrap();
        super::super::set_meta(
            &db,
            &format!("client:{MANAGER_ID}"),
            &json!({"role":"manager","disabled":false}),
        )
        .unwrap();
        super::super::set_meta(
            &db,
            "gm",
            &json!({"client_id":MANAGER_ID,"binding_id":null,"binding_generation":null,"epoch":1}),
        )
        .unwrap();
        super::super::set_meta(&db, "local_operator_client_id", &json!(OPERATOR_ID)).unwrap();
        super::super::set_meta(
            &db,
            &format!("client:{OPERATOR_ID}"),
            &json!({"role":"operator","disabled":false}),
        )
        .unwrap();

        let app_config = app_config();
        let mut entry = AutomationEntry::new(MANAGER_ID, PROJECT_ID, AUTOMATION_ID, 1);
        entry.enabled = true;
        entry.steps = vec![AutomationStep::ScriptRun];
        entry.script_run = Some(ScriptRunSettings {
            script_id: SCRIPT_ID.to_owned(),
        });
        entry.event_rules = Some(vec![
            event_rule("hook.source.setup"),
            event_rule("hook.source.revoke"),
        ]);
        crate::automation::config::validate_entry(&entry).unwrap();

        let tx = db.transaction().unwrap();
        super::super::workspace::sync_configured_registrations(&tx, OPERATOR_ID, &app_config, 1)
            .unwrap();
        let cut = super::automation_intake::observed_event_high_water(&tx).unwrap();
        // This fixture selects only events committed after the route is activated.
        super::configure_script_trigger_activation(&tx, None, &entry, false, cut, 1).unwrap();
        tx.commit().unwrap();
        let manager = Principal {
            link_id: "hook-admin-event-manager-link".to_owned(),
            client_id: MANAGER_ID.to_owned(),
            role: Role::Manager,
        };
        (db, app_config, entry, manager)
    }

    fn drain_events(
        db: &mut Connection,
        app_config: &Config,
        entry: &AutomationEntry,
        now_ms: i64,
    ) -> ScriptTriggerState {
        let tx = db.transaction().unwrap();
        let intake = super::reconcile_source_intake(&tx, 64, false, now_ms).unwrap();
        super::reconcile_script_trigger_entry(&tx, entry, 16, intake, app_config, now_ms).unwrap();
        let state = super::load_script_trigger_state(&tx, entry)
            .unwrap()
            .expect("enabled ScriptRun event route retains a cursor");
        tx.commit().unwrap();
        state
    }

    fn checked_input(
        db: &Connection,
        app_config: &Config,
        entry: &AutomationEntry,
        cause: &Value,
    ) -> Value {
        let mut retained_cause = cause.clone();
        retained_cause["script_revision"] = json!(1);
        let retained =
            super::validate_retained_script_event_cause(db, PROJECT_ID, &retained_cause).unwrap();
        let context = super::script_event_invocation_context(db, app_config, entry, cause).unwrap();
        assert_eq!(retained, context.input);
        assert!(context.input.get("operation_id").is_none());
        assert!(context.input.get("task_id").is_none());
        assert!(context.input.get("attempt_id").is_none());
        context.input
    }

    #[test]
    fn real_hook_setup_and_revoke_writers_drain_through_statusless_script_rules() {
        let (mut db, app_config, entry, manager) = fixture();
        let source_id = model::new_id();
        let credential = Credential {
            client_id: format!("hook-source:{source_id}"),
            token: format!("{}{}", model::new_id(), model::new_id()),
        };
        let credential_hash = model::digest(credential.token.as_bytes());
        let request = HookSetupRequest {
            client_request_id: model::new_id(),
            project_id: PROJECT_ID.to_owned(),
            source_id: source_id.clone(),
            credential: credential.clone(),
        };
        let setup_id = {
            let tx = db.transaction().unwrap();
            let source =
                super::super::hooks::setup_source(&tx, &manager, &app_config, &request, 10)
                    .unwrap()
                    .source;
            assert_eq!(source.project_id, PROJECT_ID);
            let id = tx.last_insert_rowid();
            tx.commit().unwrap();
            id
        };

        let hook_principal = Principal {
            link_id: "revoked-hook-source-link".to_owned(),
            client_id: credential.client_id.clone(),
            role: Role::HookSource,
        };
        let revoke_id = {
            let tx = db.transaction().unwrap();
            super::super::hooks::revoke(&tx, &manager, &source_id, 1, 20).unwrap();
            let id = tx.last_insert_rowid();
            tx.commit().unwrap();
            id
        };
        let revoked_emit =
            super::super::hooks::emit_scope(&db, &hook_principal, &app_config, &source_id);
        let error = match revoked_emit {
            Err(error) => error,
            Ok(_) => panic!("revoked HookSource credentials cannot emit another fact"),
        };
        assert_eq!(error.code, "UNAUTHORIZED");
        let client = super::super::meta(&db, &format!("client:{}", credential.client_id))
            .unwrap()
            .unwrap();
        assert_eq!(client["disabled"], true);

        let after_revoke = drain_events(&mut db, &app_config, &entry, 25);
        assert_eq!(after_revoke.cursor, revoke_id);

        let setup_event = super::automation_intake::observed_event_by_id(&db, setup_id)
            .unwrap()
            .expect("the committed setup observation remains readable");
        let setup_projection =
            super::automation_intake::safe_event_projection(&db, &setup_event).unwrap();
        assert_eq!(
            setup_projection.occurrence_phase.as_deref(),
            Some("hook_source_setup_committed"),
            "the real setup writer must yield its closed typed projection"
        );
        assert!(entry.accepts_script_run_event(
            &setup_event.source_id,
            &setup_event.event_kind,
            setup_projection.status
        ));
        assert_eq!(
            super::hook_source_admin_project_scope(&db, &app_config, PROJECT_ID, &setup_event)
                .unwrap(),
            PROJECT_ID
        );
        let setup_probe_cause =
            super::system_event_cause(&setup_event, &setup_projection, SCRIPT_ID).unwrap();
        super::script_event_invocation_context(&db, &app_config, &entry, &setup_probe_cause)
            .unwrap_or_else(|error| {
                panic!("setup Manager/source invocation probe: {}", error.code)
            });

        let revoke_event = super::automation_intake::observed_event_by_id(&db, revoke_id)
            .unwrap()
            .expect("the committed revoke observation remains readable");
        let revoke_projection =
            super::automation_intake::safe_event_projection(&db, &revoke_event).unwrap();
        assert_eq!(
            revoke_projection.occurrence_phase.as_deref(),
            Some("hook_source_revoked"),
            "the real revoke writer must yield its closed typed projection"
        );
        assert!(entry.accepts_script_run_event(
            &revoke_event.source_id,
            &revoke_event.event_kind,
            revoke_projection.status
        ));
        assert_eq!(
            super::hook_source_admin_project_scope(&db, &app_config, PROJECT_ID, &revoke_event)
                .unwrap(),
            PROJECT_ID
        );
        let revoke_probe_cause =
            super::system_event_cause(&revoke_event, &revoke_projection, SCRIPT_ID).unwrap();
        super::script_event_invocation_context(&db, &app_config, &entry, &revoke_probe_cause)
            .unwrap_or_else(|error| {
                panic!("revoke Manager/source invocation probe: {}", error.code)
            });

        let recent_dispositions: Vec<_> = after_revoke
            .recent
            .iter()
            .filter_map(|item| item["disposition"].as_str())
            .collect();
        assert_eq!(
            after_revoke.pending.len(),
            2,
            "safe recent dispositions: {recent_dispositions:?}"
        );
        let setup_cause = after_revoke
            .pending
            .iter()
            .find(|pending| pending.observation_id == setup_id)
            .expect("the earlier setup fact remains selectable after a later revoke");
        assert_eq!(setup_cause.cause["source_id"], "controller:hook-source");
        assert_eq!(setup_cause.cause["event_kind"], "hook.source.setup");
        assert_eq!(setup_cause.cause["status"], "applied");
        assert_eq!(
            setup_cause.cause["occurrence_phase"],
            "hook_source_setup_committed"
        );
        let setup_input = checked_input(&db, &app_config, &entry, &setup_cause.cause);
        let setup_rendered = setup_input.to_string();
        assert!(!setup_rendered.contains(&credential.token));
        assert!(!setup_rendered.contains(&credential_hash));
        assert!(!setup_rendered.contains("created_by"));
        assert!(!setup_rendered.contains("github.com/owner/hook-admin-fixture"));
        assert!(!setup_rendered.contains(MANAGER_ID));
        assert!(setup_input.get("payload").is_none());

        let revoke_cause = after_revoke
            .pending
            .iter()
            .find(|pending| pending.observation_id == revoke_id)
            .expect("the revocation fact remains selectable after credential disablement");
        assert_eq!(revoke_cause.cause["event_kind"], "hook.source.revoke");
        assert_eq!(revoke_cause.cause["status"], "invalidated");
        assert_eq!(
            revoke_cause.cause["occurrence_phase"],
            "hook_source_revoked"
        );
        let revoke_input = checked_input(&db, &app_config, &entry, &revoke_cause.cause);
        let revoke_rendered = revoke_input.to_string();
        assert!(!revoke_rendered.contains(&credential.token));
        assert!(!revoke_rendered.contains(&credential_hash));
        assert!(!revoke_rendered.contains("revoked_by"));
        assert!(!revoke_rendered.contains("github.com/owner/hook-admin-fixture"));
        assert!(revoke_input.get("payload").is_none());
    }
}
