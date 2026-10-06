//! Durable consumer for committed Task creation, revision, and claim facts.
//!
//! The consumer retains its source cursor and pending readiness reasons in the
//! same transaction as the launch-admission callback. That callback must use
//! the shared Launcher OnBehalf actor; this module never manufactures a
//! Principal or performs a native/model effect.

use crate::{
    automation::{
        actions::AutomationStep,
        config::{self, AutomationEntry},
        work_dispatch::{
            AUTOMATION_TECHNICAL_REQUESTER_ID, WorkDispatchContext, WorkDispatchLaunchSettings,
            WorkDispatchSource, launch_request, preview_request,
        },
    },
    error::{Error, Result},
    launcher::{LaunchPreviewRequest, LaunchRequest},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const STATE_SCHEMA_VERSION: u32 = 1;
const SLOT_SCHEMA_VERSION: u32 = 1;
const MAX_SOURCE_PAGE: usize = 32;
const MAX_PENDING_SUBJECTS: usize = 128;
const MAX_PENDING_RECHECKS: usize = 8;
const MAX_RECENT: usize = 20;
const MAX_REASON_BYTES: usize = 128;
const MAX_RECONCILE_ENTRIES: usize = 32;
const STATE_PREFIX: &str = "automation:v1:work-dispatch:state:";
const SLOT_PREFIX: &str = "launch:v1:semantic-slot:";
const OPERATION_LINK_PREFIX: &str = "work-dispatch:v1:operation-link:";
const ENTRY_LINK_PREFIX: &str = "work-dispatch:v1:entry-operation:";
const GLOBAL_CURSOR_KEY: &str = "automation:v1:work-dispatch_global_cursor";

type TaskFactOperationRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
);
type LaunchOperationIdentityRow = (String, String, Option<String>, Option<String>, String);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GlobalWorkDispatchCursor {
    schema_version: u32,
    last_entry_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkDispatchState {
    schema_version: u32,
    owner_manager_id: String,
    project_id: String,
    automation_id: String,
    cursor: i64,
    activation_cut: i64,
    catch_up_until: Option<i64>,
    pending: Vec<PendingSubject>,
    recent: Vec<Value>,
    updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingSubject {
    observation_id: i64,
    source_kind: String,
    source_operation_id: String,
    task_id: String,
    task_revision: i64,
    attempt_id: Option<String>,
    reason: String,
    wake_when: Vec<String>,
    first_seen_at_ms: i64,
    last_checked_at_ms: i64,
    held: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LaunchSlotReceipt {
    schema_version: u32,
    semantic_slot_id: String,
    manager_id: String,
    task_id: String,
    task_revision: i64,
    attempt_id: Option<String>,
    action: String,
    parameters_digest: String,
    operation_id: String,
    reserved_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkDispatchOperationLink {
    schema_version: u32,
    pub(crate) operation_id: String,
    pub(crate) technical_requester_id: String,
    pub(crate) effective_manager_id: String,
    pub(crate) automation_id: String,
    pub(crate) automation_revision: i64,
    pub(crate) project_id: String,
    pub(crate) action: String,
    pub(crate) semantic_cause_kind: String,
    pub(crate) semantic_cause_id: String,
    pub(crate) semantic_slot_id: String,
    pub(crate) task_id: String,
    pub(crate) task_revision: i64,
    pub(crate) attempt_id: Option<String>,
    pub(crate) source: Value,
    pub(crate) linked_at_ms: i64,
}

impl WorkDispatchOperationLink {
    pub(crate) fn belongs_to(&self, principal: &Principal) -> bool {
        principal.role == Role::Manager && principal.client_id == self.effective_manager_id
    }

    pub(crate) fn value(&self) -> Result<Value> {
        serde_json::to_value(self).map_err(Into::into)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedWorkDispatch {
    context: WorkDispatchContext,
    preview: LaunchPreviewRequest,
}

impl PreparedWorkDispatch {
    pub(crate) fn context(&self) -> &WorkDispatchContext {
        &self.context
    }

    pub(crate) fn preview(&self) -> &LaunchPreviewRequest {
        &self.preview
    }

    /// Bind a real launcher preview digest and return the canonical existing
    /// `swarm.launch` request. This method creates no Operation.
    pub(crate) fn launch_request(&self, plan_digest: &str) -> Result<(LaunchRequest, Value)> {
        launch_request(&self.context, &self.preview, plan_digest)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum WorkDispatchOutcome {
    Admitted {
        operation_id: String,
    },
    Pending {
        reason: String,
        wake_when: Vec<String>,
    },
    Skipped {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LaunchSlotResolution {
    Vacant,
    Reuse {
        operation_id: String,
        operation_state: String,
    },
    Conflict {
        operation_id: String,
        operation_state: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LaunchSlotRetention {
    Retained,
    Reuse {
        operation_id: String,
        operation_state: String,
    },
    Conflict {
        operation_id: String,
        operation_state: String,
    },
}

struct WorkFact {
    observation_id: i64,
    kind: String,
    source_event_key: Option<String>,
    operation_id: Option<String>,
    payload_json: String,
}

#[derive(Debug)]
enum WorkFactSubject {
    Candidate {
        source: WorkDispatchSource,
        task_id: String,
        task_revision: i64,
        attempt_id: Option<String>,
    },
    Ignored {
        reason: &'static str,
    },
}

/// Initialize or update the per-entry activation cut alongside config.apply.
/// `include_existing=false` begins after the current Task-fact watermark; true
/// replays only through the captured cut before following new facts.
pub(crate) fn configure_activation(
    tx: &Transaction<'_>,
    before: Option<&AutomationEntry>,
    after: &AutomationEntry,
    include_existing: bool,
    now_ms: i64,
) -> Result<()> {
    let had_work = before.is_some_and(entry_has_work_dispatch);
    let has_work = entry_has_work_dispatch(after);
    let state_key = state_key(after)?;
    let mut state = match load_state(tx, after)? {
        Some(state) => state,
        None if before.is_none_or(|entry| !entry_has_work_dispatch(entry)) => {
            empty_state(after, work_high_water(tx)?, now_ms)
        }
        None => {
            return Err(Error::new(
                "AUTOMATION_WORK_CURSOR_MISSING",
                "existing automation entry has no durable work-dispatch cursor",
            ));
        }
    };
    if !has_work {
        if had_work {
            for pending in &mut state.pending {
                pending.held = true;
                pending.reason = "automation_disabled_or_step_removed".to_owned();
                pending.last_checked_at_ms = now_ms;
            }
        }
    } else if !had_work {
        let cut = work_high_water(tx)?;
        state.activation_cut = cut;
        state.cursor = if include_existing { 0 } else { cut };
        state.catch_up_until = include_existing.then_some(cut);
        if include_existing {
            for pending in &mut state.pending {
                pending.held = false;
                pending.reason = "awaiting_work_dispatch".to_owned();
                pending.last_checked_at_ms = now_ms;
            }
        }
    }
    state.updated_at_ms = now_ms;
    save_state(tx, &state_key, &state)
}

/// Prepare a validated on-behalf launch plan for one exact current Task
/// revision, either unclaimed or on its manager-owned reserved Attempt.
/// Readiness/policy/capacity are still determined by the existing launcher
/// preview through the caller's real `LaunchActor::OnBehalf` integration.
pub(crate) fn prepare(
    db: &Connection,
    entry: &AutomationEntry,
    settings: &WorkDispatchLaunchSettings,
    task_id: &str,
    task_revision: i64,
    attempt_id: Option<&str>,
    source: WorkDispatchSource,
) -> Result<PreparedWorkDispatch> {
    let context = WorkDispatchContext::from_current_assignment(
        db,
        entry,
        task_id,
        task_revision,
        attempt_id,
        source,
    )?;
    let preview = preview_request(&context, settings)?;
    Ok(PreparedWorkDispatch { context, preview })
}

/// One transactionally bounded pass. Root supplies the shared launcher
/// admission callback; each cursor/pending change commits with the callback's
/// Operation and semantic-slot reservation. No callback may start native work.
pub(crate) fn reconcile_entry<F>(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    settings: Option<&WorkDispatchLaunchSettings>,
    budget: usize,
    now_ms: i64,
    mut admit: F,
) -> Result<Value>
where
    F: FnMut(&Transaction<'_>, &PreparedWorkDispatch) -> Result<WorkDispatchOutcome>,
{
    if !entry.enabled || !entry.steps.contains(&AutomationStep::WorkDispatch) {
        return Ok(json!({
            "automation_id":entry.automation_id,
            "processed":0,
            "waiting_for":"disabled_or_unselected"
        }));
    }
    let Some(settings) = settings else {
        return Ok(json!({
            "automation_id":entry.automation_id,
            "processed":0,
            "status":"capability_gap",
            "code":"work_dispatch_settings_missing",
            "reason":"configure an explicit launch route, profiles, workspace policy and bounds"
        }));
    };
    if entry.scope.work_pool_id.is_some() {
        return Ok(json!({
            "automation_id":entry.automation_id,
            "processed":0,
            "status":"capability_gap",
            "code":"work_pool_scope_unavailable"
        }));
    }
    let key = state_key(entry)?;
    let mut state = load_state(tx, entry)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_WORK_CURSOR_MISSING",
            "enabled work_dispatch entry has no activation cursor",
        )
    })?;
    validate_state(&state, entry)?;
    let budget = budget.min(MAX_SOURCE_PAGE);
    if budget == 0 {
        return Ok(state_projection(&state));
    }

    let mut processed = 0usize;
    let pending_limit = budget.min(MAX_PENDING_RECHECKS);
    processed += recheck_pending(
        tx,
        entry,
        settings,
        &mut state,
        pending_limit,
        now_ms,
        &mut admit,
    )?;
    let remaining = budget.saturating_sub(processed);
    if remaining > 0 && state.pending.len() < MAX_PENDING_SUBJECTS {
        processed += consume_work_fact_page(
            tx, entry, settings, &mut state, remaining, now_ms, &mut admit,
        )?;
    }
    state.updated_at_ms = now_ms;
    save_state(tx, &key, &state)?;
    let mut result = state_projection(&state);
    result["processed"] = json!(processed);
    result["high_water"] = json!(work_high_water(tx)?);
    Ok(result)
}

/// Shared bounded Store pass over enabled WorkDispatch entries. The launcher
/// callback only admits retained Operations; cursor, pending reasons, semantic
/// slot and Operation admission all commit in the caller's transaction.
pub(crate) fn reconcile(
    tx: &Transaction<'_>,
    launcher_config: &crate::config::Config,
    entry_budget: usize,
    fact_budget: usize,
    now_ms: i64,
) -> Result<Value> {
    let entry_budget = entry_budget.clamp(1, MAX_RECONCILE_ENTRIES);
    let fact_budget = fact_budget.clamp(1, MAX_SOURCE_PAGE);
    let (entries, last_entry_key) = enabled_entry_page(tx, entry_budget)?;
    let mut results = Vec::with_capacity(entries.len());
    for entry in entries {
        results.push(reconcile_entry(
            tx,
            &entry,
            entry.work_dispatch.as_ref(),
            fact_budget,
            now_ms,
            |tx, prepared| {
                super::launcher::admit_work_dispatch(tx, prepared, launcher_config, now_ms)
            },
        )?);
    }
    if let Some(last_entry_key) = last_entry_key {
        config::write_record(
            tx,
            GLOBAL_CURSOR_KEY,
            &json!({"schema_version":1,"last_entry_key":last_entry_key}),
        )?;
    }
    Ok(json!({
        "entries":results,
        "entry_budget":entry_budget,
        "fact_budget_per_entry":fact_budget
    }))
}

pub(super) fn dispatch_state(db: &Connection, entry: &AutomationEntry) -> Result<Value> {
    let Some(state) = load_state(db, entry)? else {
        return Ok(json!({
            "status":if entry.steps.contains(&AutomationStep::WorkDispatch) {"not_initialized"} else {"not_configured"},
            "automation_id":entry.automation_id
        }));
    };
    Ok(state_projection(&state))
}

fn enabled_entry_page(
    db: &Connection,
    limit: usize,
) -> Result<(Vec<AutomationEntry>, Option<String>)> {
    let prefix = "automation:v1:entry:";
    let pattern = format!("{prefix}%");
    let cursor = config::read_record(db, GLOBAL_CURSOR_KEY, "WorkDispatch global cursor")?
        .map(|value| {
            serde_json::from_value::<GlobalWorkDispatchCursor>(value).map_err(|_| {
                Error::new(
                    "AUTOMATION_WORK_CURSOR_CORRUPT",
                    "global WorkDispatch cursor fields are invalid",
                )
            })
        })
        .transpose()?;
    if cursor.as_ref().is_some_and(|cursor| {
        cursor.schema_version != 1 || !cursor.last_entry_key.starts_with(prefix)
    }) {
        return Err(Error::new(
            "AUTOMATION_WORK_CURSOR_CORRUPT",
            "global WorkDispatch cursor version or key is invalid",
        ));
    }
    let after = cursor.map_or_else(|| prefix.to_owned(), |cursor| cursor.last_entry_key);
    let mut keys = select_enabled_entry_keys(db, &pattern, &after, limit)?;
    if keys.len() < limit {
        keys.extend(select_enabled_entry_keys_before(
            db,
            &pattern,
            prefix,
            &after,
            limit - keys.len(),
        )?);
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
        if entry.enabled && entry.steps.contains(&AutomationStep::WorkDispatch) {
            entries.push(entry);
        }
    }
    Ok((entries, last_key))
}

fn select_enabled_entry_keys(
    db: &Connection,
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
                    WHERE step.value='work_dispatch') \
         ORDER BY key LIMIT ?3",
    )?;
    Ok(statement
        .query_map(params![pattern, after, limit as i64], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn select_enabled_entry_keys_before(
    db: &Connection,
    pattern: &str,
    prefix: &str,
    before: &str,
    limit: usize,
) -> Result<Vec<String>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut statement = db.prepare(
        "SELECT key FROM meta WHERE key LIKE ?1 AND key>?2 AND key<?3 \
         AND json_extract(value_json,'$.record.enabled')=1 \
         AND EXISTS(SELECT 1 FROM json_each(value_json,'$.record.steps') AS step \
                    WHERE step.value='work_dispatch') \
         ORDER BY key LIMIT ?4",
    )?;
    Ok(statement
        .query_map(params![pattern, prefix, before, limit as i64], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

/// Consume committed Task facts without advancing past unhandled records.
/// Source identity and the immutable Operation result are checked before their
/// typed subject can influence admission.
fn consume_work_fact_page<F>(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    settings: &WorkDispatchLaunchSettings,
    state: &mut WorkDispatchState,
    budget: usize,
    now_ms: i64,
    admit: &mut F,
) -> Result<usize>
where
    F: FnMut(&Transaction<'_>, &PreparedWorkDispatch) -> Result<WorkDispatchOutcome>,
{
    let high_water = work_high_water(tx)?;
    let target = state
        .catch_up_until
        .map_or(high_water, |cut| high_water.min(cut));
    if state.cursor >= target {
        if state.catch_up_until.is_some_and(|cut| state.cursor >= cut) {
            state.catch_up_until = None;
        }
        return Ok(0);
    }
    let mut statement = tx.prepare(
        "SELECT observation_id,kind,source_event_key,operation_id,payload_json FROM observations \
         WHERE source_stream_id='controller' AND kind IN ('task.create','task.revise','task.claim') \
           AND observation_id>?1 \
           AND observation_id<=?2 ORDER BY observation_id LIMIT ?3",
    )?;
    let facts = statement
        .query_map(params![state.cursor, target, budget as i64], |row| {
            Ok(WorkFact {
                observation_id: row.get(0)?,
                kind: row.get(1)?,
                source_event_key: row.get(2)?,
                operation_id: row.get(3)?,
                payload_json: row.get(4)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);

    if facts.is_empty() {
        state.cursor = target;
        if state.catch_up_until.is_some_and(|cut| state.cursor >= cut) {
            state.catch_up_until = None;
        }
        return Ok(0);
    }
    let mut processed = 0usize;
    for fact in facts {
        if state.pending.len() >= MAX_PENDING_SUBJECTS {
            break;
        }
        processed += 1;
        let observation_id = fact.observation_id;
        let identity = read_work_fact_subject(tx, &fact);
        match identity {
            Err(error) if error.code == "AUTOMATION_WORK_SOURCE_GAP" => {
                remember_recent(
                    state,
                    json!({
                        "observation_id":observation_id,
                        "disposition":"gap",
                        "reason":error.code.to_ascii_lowercase()
                    }),
                );
                state.cursor = observation_id;
            }
            Err(error) => return Err(error),
            Ok(WorkFactSubject::Ignored { reason }) => {
                remember_recent(
                    state,
                    json!({
                        "observation_id":observation_id,
                        "disposition":"skipped",
                        "reason":reason
                    }),
                );
                state.cursor = observation_id;
            }
            Ok(WorkFactSubject::Candidate {
                source,
                task_id,
                task_revision,
                attempt_id,
            }) => {
                match prepare(
                    tx,
                    entry,
                    settings,
                    &task_id,
                    task_revision,
                    attempt_id.as_deref(),
                    source.clone(),
                ) {
                    Err(error) if is_stale_subject(&error) => {
                        remember_recent(
                            state,
                            json!({
                                "observation_id":observation_id,
                                "task_id":task_id,
                                "task_revision":task_revision,
                                "attempt_id":attempt_id,
                                "disposition":"skipped",
                                "reason":error.code.to_ascii_lowercase()
                            }),
                        );
                    }
                    Err(error) if error.code == "FORBIDDEN" => {
                        retain_pending(
                            state,
                            PendingSubject {
                                observation_id,
                                source_kind: source.event_kind().to_owned(),
                                source_operation_id: source.operation_id().to_owned(),
                                task_id,
                                task_revision,
                                attempt_id,
                                reason: "manager_authority_unavailable".to_owned(),
                                wake_when: vec![
                                    "manager_registration_or_automation_configuration_changed"
                                        .to_owned(),
                                ],
                                first_seen_at_ms: now_ms,
                                last_checked_at_ms: now_ms,
                                held: false,
                            },
                        )?;
                    }
                    Err(error) => return Err(error),
                    Ok(plan) => match admit(tx, &plan)? {
                        WorkDispatchOutcome::Admitted { operation_id } => remember_recent(
                            state,
                            json!({
                                "observation_id":observation_id,
                                "task_id":task_id,
                                "task_revision":task_revision,
                                "attempt_id":attempt_id,
                                "disposition":"admitted",
                                "operation_id":operation_id,
                                "semantic_slot_id":plan.context.semantic_slot_id()
                            }),
                        ),
                        WorkDispatchOutcome::Pending { reason, wake_when } => {
                            retain_pending(
                                state,
                                PendingSubject {
                                    observation_id,
                                    source_kind: source.event_kind().to_owned(),
                                    source_operation_id: source.operation_id().to_owned(),
                                    task_id,
                                    task_revision,
                                    attempt_id,
                                    reason,
                                    wake_when,
                                    first_seen_at_ms: now_ms,
                                    last_checked_at_ms: now_ms,
                                    held: false,
                                },
                            )?;
                        }
                        WorkDispatchOutcome::Skipped { reason } => remember_recent(
                            state,
                            json!({
                                "observation_id":observation_id,
                                "task_id":task_id,
                                "task_revision":task_revision,
                                "attempt_id":attempt_id,
                                "disposition":"skipped",
                                "reason":reason
                            }),
                        ),
                    },
                }
                state.cursor = observation_id;
            }
        }
    }
    if state.cursor >= target && state.catch_up_until.is_some_and(|cut| state.cursor >= cut) {
        state.catch_up_until = None;
    }
    Ok(processed)
}

fn read_work_fact_subject(db: &Connection, fact: &WorkFact) -> Result<WorkFactSubject> {
    let gap = |message| Error::new("AUTOMATION_WORK_SOURCE_GAP", message);
    let operation_id = fact
        .operation_id
        .as_deref()
        .ok_or_else(|| gap("Task fact has no Operation identity"))?;
    if fact.source_event_key.as_deref() != Some(operation_id) {
        return Err(gap(
            "Task fact source key differs from its Operation identity",
        ));
    }
    let payload: Value = serde_json::from_str(&fact.payload_json)
        .map_err(|_| gap("Task fact payload is invalid"))?;
    if payload["operation_id"].as_str() != Some(operation_id) {
        return Err(gap("Task fact payload does not bind its source Operation"));
    }
    let operation: Option<TaskFactOperationRow> = db
        .query_row(
            "SELECT method,state,task_id,attempt_id,result_json,client_request_id FROM operations WHERE operation_id=?1",
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
    let Some((method, state, operation_task, operation_attempt, result_json, client_request_id)) =
        operation
    else {
        return Err(gap("Task fact Operation is missing"));
    };
    if method != fact.kind || state != "settled" {
        return Err(gap(
            "Task fact does not reference a settled Operation of the same method",
        ));
    }
    let result_json =
        result_json.ok_or_else(|| gap("settled Task fact has no Operation result"))?;
    let operation_result: Value = serde_json::from_str(&result_json)
        .map_err(|_| gap("Task fact Operation result is invalid"))?;
    if model::canonical(&operation_result)? != model::canonical(&payload)? {
        return Err(gap(
            "Task fact payload differs from its immutable Operation result",
        ));
    }
    let source =
        WorkDispatchSource::from_committed_fact(fact.observation_id, &fact.kind, operation_id)?;
    let task_id = payload["task_id"]
        .as_str()
        .filter(|value| !value.is_empty() && value.len() <= 512)
        .ok_or_else(|| gap("Task fact has no bounded Task ID"))?
        .to_owned();
    if operation_task
        .as_deref()
        .is_some_and(|operation_task| operation_task != task_id)
    {
        return Err(gap("Task fact Operation is linked to a different Task"));
    }

    let (task_revision, attempt_id) = match fact.kind.as_str() {
        "task.create" => {
            if payload["created"] != true {
                return Ok(WorkFactSubject::Ignored {
                    reason: "task_create_idempotent_reuse",
                });
            }
            let revision = payload["revision"]
                .as_i64()
                .filter(|revision| *revision > 0)
                .ok_or_else(|| gap("created Task revision is missing"))?;
            if payload.get("attempt_id").is_some() || operation_attempt.is_some() {
                return Err(gap("new Task fact unexpectedly names an Attempt"));
            }
            (revision, None)
        }
        "task.revise" => {
            let revision = payload["revision"]
                .as_i64()
                .filter(|revision| *revision > 0)
                .ok_or_else(|| gap("revised Task revision is missing"))?;
            if payload.get("attempt_id").is_some() || operation_attempt.is_some() {
                return Err(gap("Task revision fact unexpectedly names an Attempt"));
            }
            (revision, None)
        }
        "task.claim" => {
            let attempt_id = payload["attempt_id"]
                .as_str()
                .filter(|value| !value.is_empty() && value.len() <= 512)
                .ok_or_else(|| gap("Task claim has no bounded Attempt ID"))?
                .to_owned();
            if operation_attempt
                .as_deref()
                .is_some_and(|operation_attempt| operation_attempt != attempt_id)
            {
                return Err(gap("Task claim Operation is linked to a different Attempt"));
            }
            let revision: Option<i64> = db
                .query_row(
                    "SELECT task_revision FROM attempts WHERE attempt_id=?1 AND task_id=?2",
                    params![attempt_id, task_id],
                    |row| row.get(0),
                )
                .optional()?;
            let revision = revision
                .filter(|revision| *revision > 0)
                .ok_or_else(|| gap("Task claim fact references no exact retained Attempt"))?;
            if is_launch_child_claim(db, &client_request_id, &task_id, revision, &attempt_id)? {
                return Ok(WorkFactSubject::Ignored {
                    reason: "task_claim_already_admitted_by_launch",
                });
            }
            (revision, Some(attempt_id))
        }
        _ => return Err(gap("unsupported Task fact kind")),
    };
    Ok(WorkFactSubject::Candidate {
        source,
        task_id,
        task_revision,
        attempt_id,
    })
}

/// The launch flow records its internal Task claim as a normal committed
/// `task.claim` Operation with the deterministic child request ID
/// `launch:<parent-operation-id>:claim`. Do not treat that claim as a second
/// WorkDispatch trigger: its parent launch already owns the semantic slot and
/// will advance this same Attempt after the held workspace lease.
fn is_launch_child_claim(
    db: &Connection,
    client_request_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
) -> Result<bool> {
    let Some(parent_operation_id) = client_request_id
        .strip_prefix("launch:")
        .and_then(|value| value.strip_suffix(":claim"))
    else {
        return Ok(false);
    };
    if parent_operation_id.is_empty()
        || parent_operation_id.len() > 256
        || parent_operation_id.chars().any(char::is_control)
        || client_request_id != format!("launch:{parent_operation_id}:claim")
    {
        return Ok(false);
    }
    let parent: Option<(String, Option<String>, Option<String>, String)> = db
        .query_row(
            "SELECT method,task_id,attempt_id,original_request_json FROM operations WHERE operation_id=?1",
            [parent_operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((method, parent_task_id, parent_attempt_id, original_request_json)) = parent else {
        return Ok(false);
    };
    if method != "swarm.launch"
        || parent_task_id.as_deref() != Some(task_id)
        || parent_attempt_id.as_deref() != Some(attempt_id)
    {
        return Ok(false);
    }
    let original_request: Value = serde_json::from_str(&original_request_json).map_err(|_| {
        Error::new(
            "AUTOMATION_WORK_SOURCE_GAP",
            "parent launch request for a Task claim is invalid",
        )
    })?;
    let request = LaunchRequest::parse(&original_request).map_err(|_| {
        Error::new(
            "AUTOMATION_WORK_SOURCE_GAP",
            "parent Task claim Operation is not a canonical launch request",
        )
    })?;
    Ok(request.preview.task_id == task_id
        && request.preview.expected_task_revision == task_revision)
}

fn recheck_pending<F>(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    settings: &WorkDispatchLaunchSettings,
    state: &mut WorkDispatchState,
    limit: usize,
    now_ms: i64,
    admit: &mut F,
) -> Result<usize>
where
    F: FnMut(&Transaction<'_>, &PreparedWorkDispatch) -> Result<WorkDispatchOutcome>,
{
    let pending_items = std::mem::take(&mut state.pending);
    let mut checked = 0usize;
    let mut keep = Vec::with_capacity(pending_items.len());
    for mut pending in pending_items {
        if pending.held || checked >= limit {
            keep.push(pending);
            continue;
        }
        checked += 1;
        match prepare(
            tx,
            entry,
            settings,
            &pending.task_id,
            pending.task_revision,
            pending.attempt_id.as_deref(),
            WorkDispatchSource::from_committed_fact(
                pending.observation_id,
                &pending.source_kind,
                &pending.source_operation_id,
            )?,
        ) {
            Err(error) if is_stale_subject(&error) => remember_recent(
                state,
                json!({
                    "observation_id":pending.observation_id,
                    "task_id":pending.task_id,
                    "task_revision":pending.task_revision,
                    "attempt_id":pending.attempt_id,
                    "disposition":"skipped",
                    "reason":error.code.to_ascii_lowercase()
                }),
            ),
            Err(error) if error.code == "FORBIDDEN" => {
                pending.reason = "manager_authority_unavailable".to_owned();
                pending.wake_when =
                    vec!["manager_registration_or_automation_configuration_changed".to_owned()];
                pending.last_checked_at_ms = now_ms;
                keep.push(pending);
            }
            Err(error) => return Err(error),
            Ok(plan) => match admit(tx, &plan)? {
                WorkDispatchOutcome::Admitted { operation_id } => remember_recent(
                    state,
                    json!({
                        "observation_id":pending.observation_id,
                        "task_id":pending.task_id,
                        "task_revision":pending.task_revision,
                        "attempt_id":pending.attempt_id,
                        "disposition":"admitted",
                        "operation_id":operation_id,
                        "semantic_slot_id":plan.context.semantic_slot_id()
                    }),
                ),
                WorkDispatchOutcome::Pending { reason, wake_when } => {
                    pending.reason = bounded_reason(&reason)?;
                    pending.wake_when = bounded_wake_when(wake_when)?;
                    pending.last_checked_at_ms = now_ms;
                    keep.push(pending);
                }
                WorkDispatchOutcome::Skipped { reason } => remember_recent(
                    state,
                    json!({
                        "observation_id":pending.observation_id,
                        "task_id":pending.task_id,
                        "task_revision":pending.task_revision,
                        "attempt_id":pending.attempt_id,
                        "disposition":"skipped",
                        "reason":reason
                    }),
                ),
            },
        }
    }
    state.pending = keep;
    Ok(checked)
}

fn entry_has_work_dispatch(entry: &AutomationEntry) -> bool {
    entry.work_dispatch_ready()
}

fn empty_state(entry: &AutomationEntry, cut: i64, now_ms: i64) -> WorkDispatchState {
    WorkDispatchState {
        schema_version: STATE_SCHEMA_VERSION,
        owner_manager_id: entry.owner_manager_id.clone(),
        project_id: entry.project_id.clone(),
        automation_id: entry.automation_id.clone(),
        cursor: cut,
        activation_cut: cut,
        catch_up_until: None,
        pending: Vec::new(),
        recent: Vec::new(),
        updated_at_ms: now_ms,
    }
}

fn load_state(db: &Connection, entry: &AutomationEntry) -> Result<Option<WorkDispatchState>> {
    let Some(value) = config::read_record(db, &state_key(entry)?, "work-dispatch state")? else {
        return Ok(None);
    };
    let state: WorkDispatchState = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "AUTOMATION_WORK_STATE_CORRUPT",
            "work-dispatch state fields are invalid",
        )
    })?;
    validate_state(&state, entry)?;
    Ok(Some(state))
}

/// Move only the typed per-entry journal during an explicit ownership
/// transfer. Global intake cursors, semantic slots, and operation history are
/// independent records and remain untouched.
pub(super) fn relocate_state(
    tx: &Transaction<'_>,
    former: &AutomationEntry,
    successor: &AutomationEntry,
) -> Result<()> {
    if former.owner_manager_id == successor.owner_manager_id
        || former.project_id != successor.project_id
        || former.automation_id != successor.automation_id
    {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_SCOPE",
            "WorkDispatch state relocation requires the exact former and successor entry pair",
        ));
    }
    let old_key = state_key(former)?;
    let new_key = state_key(successor)?;
    let target_exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM meta WHERE key=?1)",
        [&new_key],
        |row| row.get(0),
    )?;
    if target_exists {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_CONFLICT",
            "successor already has WorkDispatch state for this automation",
        ));
    }
    let Some(mut state) = load_state(tx, former)? else {
        return Ok(());
    };
    state.owner_manager_id = successor.owner_manager_id.clone();
    validate_state(&state, successor)?;
    save_state(tx, &new_key, &state)?;
    let deleted = tx.execute("DELETE FROM meta WHERE key=?1", [&old_key])?;
    if deleted != 1 {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_STATE_CORRUPT",
            "former WorkDispatch state changed while transfer was being applied",
        ));
    }
    Ok(())
}

fn validate_state(state: &WorkDispatchState, entry: &AutomationEntry) -> Result<()> {
    if state.schema_version != STATE_SCHEMA_VERSION
        || state.owner_manager_id != entry.owner_manager_id
        || state.project_id != entry.project_id
        || state.automation_id != entry.automation_id
        || state.cursor < 0
        || state.activation_cut < 0
        || state.catch_up_until.is_some_and(|cut| cut < state.cursor)
        || state.pending.len() > MAX_PENDING_SUBJECTS
        || state.updated_at_ms < 0
        || state.pending.iter().any(|item| {
            item.observation_id <= 0
                || item.task_id.is_empty()
                || item.task_id.len() > 512
                || item
                    .attempt_id
                    .as_ref()
                    .is_some_and(|attempt_id| attempt_id.is_empty() || attempt_id.len() > 512)
                || item.task_revision <= 0
                || !matches!(
                    item.source_kind.as_str(),
                    "task.create" | "task.revise" | "task.claim"
                )
                || item.source_operation_id.is_empty()
                || item.source_operation_id.len() > 256
                || item.source_operation_id.chars().any(char::is_control)
                || item.reason.is_empty()
                || item.reason.len() > MAX_REASON_BYTES
                || item.wake_when.len() > 8
                || item.first_seen_at_ms < 0
                || item.last_checked_at_ms < item.first_seen_at_ms
        })
    {
        return Err(Error::new(
            "AUTOMATION_WORK_STATE_CORRUPT",
            "work-dispatch state identity, cursor, or bounds are invalid",
        ));
    }
    Ok(())
}

fn save_state(tx: &Transaction<'_>, key: &str, state: &WorkDispatchState) -> Result<()> {
    config::write_record(tx, key, &serde_json::to_value(state)?)
}

fn state_key(entry: &AutomationEntry) -> Result<String> {
    let identity = json!({
        "manager_id":entry.owner_manager_id,
        "project_id":entry.project_id,
        "automation_id":entry.automation_id
    });
    Ok(format!(
        "{STATE_PREFIX}{}",
        model::digest(model::canonical(&identity)?.as_bytes())
    ))
}

fn work_high_water(db: &Connection) -> Result<i64> {
    Ok(db.query_row(
        "SELECT COALESCE(MAX(observation_id),0) FROM observations \
         WHERE source_stream_id='controller' AND kind IN ('task.create','task.revise','task.claim')",
        [],
        |row| row.get(0),
    )?)
}

fn retain_pending(state: &mut WorkDispatchState, mut pending: PendingSubject) -> Result<()> {
    pending.reason = bounded_reason(&pending.reason)?;
    pending.wake_when = bounded_wake_when(pending.wake_when)?;
    if let Some(existing) = state.pending.iter_mut().find(|existing| {
        existing.task_id == pending.task_id
            && existing.task_revision == pending.task_revision
            && existing.attempt_id == pending.attempt_id
    }) {
        existing.reason = pending.reason;
        existing.wake_when = pending.wake_when;
        existing.last_checked_at_ms = pending.last_checked_at_ms;
        existing.held = pending.held;
        return Ok(());
    }
    if state.pending.len() >= MAX_PENDING_SUBJECTS {
        return Err(Error::new(
            "AUTOMATION_WORK_PENDING_CAPACITY",
            "work-dispatch pending subject bound is full",
        ));
    }
    state.pending.push(pending);
    Ok(())
}

fn bounded_reason(reason: &str) -> Result<String> {
    if reason.is_empty() || reason.len() > MAX_REASON_BYTES || reason.chars().any(char::is_control)
    {
        return Err(Error::invalid("work-dispatch pending reason is invalid"));
    }
    Ok(reason.to_owned())
}

fn bounded_wake_when(mut wake_when: Vec<String>) -> Result<Vec<String>> {
    if wake_when.len() > 8
        || wake_when.iter().any(|value| {
            value.is_empty() || value.len() > 128 || value.chars().any(char::is_control)
        })
    {
        return Err(Error::invalid("work-dispatch wake condition is invalid"));
    }
    wake_when.sort();
    wake_when.dedup();
    Ok(wake_when)
}

fn remember_recent(state: &mut WorkDispatchState, value: Value) {
    state.recent.push(value);
    if state.recent.len() > MAX_RECENT {
        state.recent.remove(0);
    }
}

fn state_projection(state: &WorkDispatchState) -> Value {
    json!({
        "automation_id":state.automation_id,
        "cursor":state.cursor,
        "activation_cut":state.activation_cut,
        "catch_up_until":state.catch_up_until,
        "pending":state.pending,
        "recent":state.recent,
        "updated_at_ms":state.updated_at_ms
    })
}

fn is_stale_subject(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "NOT_FOUND"
            | "AUTOMATION_WORK_SUBJECT_STALE"
            | "AUTOMATION_WORK_ASSIGNMENT_STALE"
            | "AUTOMATION_WORK_NOT_READY"
    )
}

pub(crate) fn slot_identity_for_assignment(
    manager_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: Option<&str>,
    preview: &LaunchPreviewRequest,
) -> Result<(String, String)> {
    if preview.task_id != task_id || preview.expected_task_revision != task_revision {
        return Err(Error::new(
            "LAUNCH_SLOT_SUBJECT_MISMATCH",
            "preview does not match the exact semantic slot Task revision",
        ));
    }
    validate_slot_identity_fields(manager_id, task_id, task_revision, attempt_id)?;
    let identity = json!({
        "manager_id":manager_id,
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "action":"swarm.launch",
        "slot_id":"primary"
    });
    let slot_id = model::digest(model::canonical(&identity)?.as_bytes());
    let parameters_digest = model::digest(model::canonical(&preview_params(preview))?.as_bytes());
    Ok((slot_id, parameters_digest))
}

pub(crate) fn resolve_assignment_slot(
    db: &Connection,
    manager_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: Option<&str>,
    preview: &LaunchPreviewRequest,
) -> Result<LaunchSlotResolution> {
    let (slot_id, parameters_digest) =
        slot_identity_for_assignment(manager_id, task_id, task_revision, attempt_id, preview)?;
    let Some(receipt) = load_slot(db, &slot_id)? else {
        return Ok(LaunchSlotResolution::Vacant);
    };
    validate_slot_identity(
        &receipt,
        manager_id,
        task_id,
        task_revision,
        attempt_id,
        &slot_id,
    )?;
    let operation_state = launch_operation_state(db, &receipt.operation_id)?;
    if receipt.parameters_digest == parameters_digest {
        Ok(LaunchSlotResolution::Reuse {
            operation_id: receipt.operation_id,
            operation_state,
        })
    } else {
        Ok(LaunchSlotResolution::Conflict {
            operation_id: receipt.operation_id,
            operation_state,
        })
    }
}

// The explicit fields are the canonical semantic-slot key and its immutable
// launch request at this transaction boundary.
#[allow(clippy::too_many_arguments)]
pub(crate) fn retain_assignment_slot(
    tx: &Transaction<'_>,
    manager_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: Option<&str>,
    preview: &LaunchPreviewRequest,
    operation_id: &str,
    now_ms: i64,
) -> Result<LaunchSlotRetention> {
    let (slot_id, parameters_digest) =
        slot_identity_for_assignment(manager_id, task_id, task_revision, attempt_id, preview)?;
    let operation: Option<(String, Option<String>, Option<String>)> = tx
        .query_row(
            "SELECT method,task_id,attempt_id FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    if !operation.is_some_and(|(method, task, attempt)| {
        method == "swarm.launch"
            && task.as_deref() == Some(task_id)
            && attempt.as_deref() == attempt_id
    }) {
        return Err(Error::new(
            "LAUNCH_SLOT_OPERATION_MISMATCH",
            "launch Operation is not bound to the exact semantic slot subject",
        ));
    }
    if let Some(existing) = load_slot(tx, &slot_id)? {
        validate_slot_identity(
            &existing,
            manager_id,
            task_id,
            task_revision,
            attempt_id,
            &slot_id,
        )?;
        let operation_state = launch_operation_state(tx, &existing.operation_id)?;
        if existing.parameters_digest == parameters_digest {
            return Ok(if existing.operation_id == operation_id {
                LaunchSlotRetention::Retained
            } else {
                LaunchSlotRetention::Reuse {
                    operation_id: existing.operation_id,
                    operation_state,
                }
            });
        }
        return Ok(LaunchSlotRetention::Conflict {
            operation_id: existing.operation_id,
            operation_state,
        });
    }
    let receipt = LaunchSlotReceipt {
        schema_version: SLOT_SCHEMA_VERSION,
        semantic_slot_id: slot_id.clone(),
        manager_id: manager_id.to_owned(),
        task_id: task_id.to_owned(),
        task_revision,
        attempt_id: attempt_id.map(str::to_owned),
        action: "swarm.launch".to_owned(),
        parameters_digest,
        operation_id: operation_id.to_owned(),
        reserved_at_ms: now_ms,
    };
    let sealed = config::seal_record(&serde_json::to_value(&receipt)?)?;
    tx.execute(
        "INSERT OR IGNORE INTO meta(key,value_json) VALUES(?1,?2)",
        params![slot_key(&slot_id), model::canonical(&sealed)?],
    )?;
    let stored = load_slot(tx, &slot_id)?.ok_or_else(|| {
        Error::new(
            "LAUNCH_SLOT_WRITE_MISSING",
            "launch semantic slot was not retained",
        )
    })?;
    validate_slot_identity(
        &stored,
        manager_id,
        task_id,
        task_revision,
        attempt_id,
        &slot_id,
    )?;
    let operation_state = launch_operation_state(tx, &stored.operation_id)?;
    if stored.operation_id == operation_id && stored.parameters_digest == receipt.parameters_digest
    {
        Ok(LaunchSlotRetention::Retained)
    } else if stored.parameters_digest == receipt.parameters_digest {
        Ok(LaunchSlotRetention::Reuse {
            operation_id: stored.operation_id,
            operation_state,
        })
    } else {
        Ok(LaunchSlotRetention::Conflict {
            operation_id: stored.operation_id,
            operation_state,
        })
    }
}

fn launch_operation_state(db: &Connection, operation_id: &str) -> Result<String> {
    db.query_row(
        "SELECT state FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
        [operation_id],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| {
        Error::new(
            "LAUNCH_SLOT_CORRUPT",
            "launch semantic slot references no current launch Operation",
        )
    })
}

fn validate_slot_identity_fields(
    manager_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: Option<&str>,
) -> Result<()> {
    if manager_id.is_empty()
        || manager_id.len() > 256
        || manager_id.chars().any(char::is_control)
        || task_id.is_empty()
        || task_id.len() > 512
        || task_id.chars().any(char::is_control)
        || task_revision <= 0
        || attempt_id.is_some_and(|attempt_id| {
            attempt_id.is_empty()
                || attempt_id.len() > 512
                || attempt_id.chars().any(char::is_control)
        })
    {
        return Err(Error::invalid("launch semantic slot identity is invalid"));
    }
    Ok(())
}

fn validate_slot_identity(
    receipt: &LaunchSlotReceipt,
    manager_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: Option<&str>,
    slot_id: &str,
) -> Result<()> {
    if receipt.schema_version != SLOT_SCHEMA_VERSION
        || receipt.semantic_slot_id != slot_id
        || receipt.manager_id != manager_id
        || receipt.task_id != task_id
        || receipt.task_revision != task_revision
        || receipt.attempt_id.as_deref() != attempt_id
        || receipt.action != "swarm.launch"
        || receipt.operation_id.is_empty()
        || receipt.reserved_at_ms < 0
    {
        return Err(Error::new(
            "LAUNCH_SLOT_CORRUPT",
            "launch semantic slot identity is inconsistent",
        ));
    }
    Ok(())
}

fn validate_slot_operation_identity(db: &Connection, receipt: &LaunchSlotReceipt) -> Result<()> {
    let operation: Option<LaunchOperationIdentityRow> = db
        .query_row(
            "SELECT caller_id,method,task_id,attempt_id,original_request_json \
             FROM operations WHERE operation_id=?1",
            [&receipt.operation_id],
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
    let Some((caller, method, task_id, operation_attempt_id, original_request_json)) = operation
    else {
        return Err(Error::new(
            "LAUNCH_SLOT_CORRUPT",
            "launch semantic slot references a missing Operation",
        ));
    };
    if method != "swarm.launch"
        || task_id.as_deref() != Some(receipt.task_id.as_str())
        || receipt
            .attempt_id
            .as_deref()
            .is_some_and(|attempt_id| operation_attempt_id.as_deref() != Some(attempt_id))
    {
        return Err(Error::new(
            "LAUNCH_SLOT_CORRUPT",
            "launch semantic slot Operation identity does not match its retained subject",
        ));
    }
    if caller == AUTOMATION_TECHNICAL_REQUESTER_ID {
        let link =
            read_work_dispatch_operation_link(db, &receipt.operation_id)?.ok_or_else(|| {
                Error::new(
                    "LAUNCH_SLOT_CORRUPT",
                    "automated launch slot has no retained manager attribution",
                )
            })?;
        if link.effective_manager_id != receipt.manager_id
            || link.semantic_slot_id != receipt.semantic_slot_id
        {
            return Err(Error::new(
                "LAUNCH_SLOT_CORRUPT",
                "automated launch slot references another manager or semantic subject",
            ));
        }
    } else if caller != receipt.manager_id {
        return Err(Error::new(
            "LAUNCH_SLOT_CORRUPT",
            "launch semantic slot Operation caller differs from its effective manager",
        ));
    }
    let original_request: Value = serde_json::from_str(&original_request_json).map_err(|_| {
        Error::new(
            "LAUNCH_SLOT_CORRUPT",
            "launch semantic slot Operation request is invalid",
        )
    })?;
    let request = LaunchRequest::parse(&original_request).map_err(|_| {
        Error::new(
            "LAUNCH_SLOT_CORRUPT",
            "launch semantic slot Operation request is not canonical",
        )
    })?;
    if request.preview.task_id != receipt.task_id
        || request.preview.expected_task_revision != receipt.task_revision
        || model::digest(model::canonical(&preview_params(&request.preview))?.as_bytes())
            != receipt.parameters_digest
    {
        return Err(Error::new(
            "LAUNCH_SLOT_CORRUPT",
            "launch semantic slot does not match its immutable Operation request",
        ));
    }
    if let Some(attempt_id) = operation_attempt_id.as_deref() {
        let exact_attempt: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts WHERE attempt_id=?1 AND task_id=?2 \
             AND task_revision=?3 AND owner_id=?4)",
            params![
                attempt_id,
                receipt.task_id,
                receipt.task_revision,
                receipt.manager_id
            ],
            |row| row.get(0),
        )?;
        if !exact_attempt {
            return Err(Error::new(
                "LAUNCH_SLOT_CORRUPT",
                "launch semantic slot Operation Attempt differs from its Task revision or manager",
            ));
        }
    }
    if receipt.parameters_digest.len() != 64
        || !receipt
            .parameters_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::new(
            "LAUNCH_SLOT_CORRUPT",
            "launch semantic slot parameter digest is invalid",
        ));
    }
    Ok(())
}

fn validate_slot_operation_structure(receipt: &LaunchSlotReceipt) -> Result<()> {
    if receipt.parameters_digest.len() != 64
        || !receipt
            .parameters_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || receipt.operation_id.is_empty()
    {
        return Err(Error::new(
            "LAUNCH_SLOT_CORRUPT",
            "launch semantic slot digest or Attempt reference is invalid",
        ));
    }
    Ok(())
}

fn load_slot(db: &Connection, slot_id: &str) -> Result<Option<LaunchSlotReceipt>> {
    let value = config::read_record(db, &slot_key(slot_id), "launch semantic slot")?;
    let receipt = value
        .map(|value| {
            serde_json::from_value(value).map_err(|_| {
                Error::new(
                    "LAUNCH_SLOT_CORRUPT",
                    "launch semantic slot fields are invalid",
                )
            })
        })
        .transpose()?;
    if let Some(receipt) = receipt.as_ref() {
        validate_slot_operation_structure(receipt)?;
        validate_slot_operation_identity(db, receipt)?;
    }
    Ok(receipt)
}

fn slot_key(slot_id: &str) -> String {
    format!("{SLOT_PREFIX}{slot_id}")
}

fn preview_params(preview: &LaunchPreviewRequest) -> Value {
    json!({
        "task_id":preview.task_id,
        "expected_task_revision":preview.expected_task_revision,
        "route":preview.route,
        "agent_profile":preview.agent_profile,
        "mcp_profile":preview.mcp_profile,
        "mcp_surface":preview.mcp_surface,
        "workspace_policy":preview.workspace_policy,
        "requested_model":preview.requested_model,
        "requested_effort":preview.requested_effort,
        "budget":preview.budget,
        "stop_conditions":preview.stop_conditions,
        "purpose":preview.purpose
    })
}

/// Persist immutable effective-manager attribution beside the admitted launch
/// Operation. The row and slot are written in the caller's admission
/// transaction; this helper never creates an Operation or starts work.
pub(crate) fn save_operation_link(
    tx: &Transaction<'_>,
    operation_id: &str,
    context: &WorkDispatchContext,
    now_ms: i64,
) -> Result<WorkDispatchOperationLink> {
    if operation_id.is_empty() || operation_id.len() > 256 || now_ms < 0 {
        return Err(Error::invalid(
            "WorkDispatch Operation link identity is invalid",
        ));
    }
    let operation: Option<LaunchOperationIdentityRow> = tx
        .query_row(
            "SELECT caller_id,method,task_id,attempt_id,original_request_json \
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
    let Some((caller, method, task_id, operation_attempt_id, original_request_json)) = operation
    else {
        return Err(Error::new(
            "AUTOMATION_LINK_OPERATION_MISMATCH",
            "WorkDispatch link has no admitted launch Operation",
        ));
    };
    let subject = context.subject();
    if caller != AUTOMATION_TECHNICAL_REQUESTER_ID
        || method != "swarm.launch"
        || task_id.as_deref() != Some(subject.task_id())
        || operation_attempt_id.as_deref() != subject.attempt_id()
    {
        return Err(Error::new(
            "AUTOMATION_LINK_OPERATION_MISMATCH",
            "launch Operation caller or exact Task/Attempt does not match WorkDispatch context",
        ));
    }
    let original_request: Value = serde_json::from_str(&original_request_json).map_err(|_| {
        Error::new(
            "AUTOMATION_LINK_OPERATION_MISMATCH",
            "launch Operation original request is invalid",
        )
    })?;
    let request = LaunchRequest::parse(&original_request).map_err(|_| {
        Error::new(
            "AUTOMATION_LINK_OPERATION_MISMATCH",
            "launch Operation original request is not a canonical launch request",
        )
    })?;
    if request.preview.task_id != subject.task_id()
        || request.preview.expected_task_revision != subject.task_revision()
    {
        return Err(Error::new(
            "AUTOMATION_LINK_OPERATION_MISMATCH",
            "launch Operation original request differs from WorkDispatch subject",
        ));
    }
    let semantic_cause_kind = if subject.attempt_id().is_some() {
        "manager_owned_assignment"
    } else {
        "initial_task"
    };
    let link = WorkDispatchOperationLink {
        schema_version: 1,
        operation_id: operation_id.to_owned(),
        technical_requester_id: context.technical_requester_id().to_owned(),
        effective_manager_id: context.effective_manager_id().to_owned(),
        automation_id: context.automation_id().to_owned(),
        automation_revision: context.automation_revision(),
        project_id: context.project_id().to_owned(),
        action: "swarm.launch".to_owned(),
        semantic_cause_kind: semantic_cause_kind.to_owned(),
        semantic_cause_id: context.semantic_slot_id().to_owned(),
        semantic_slot_id: context.semantic_slot_id().to_owned(),
        task_id: subject.task_id().to_owned(),
        task_revision: subject.task_revision(),
        attempt_id: subject.attempt_id().map(str::to_owned),
        source: context.linkage_value()["source"].clone(),
        linked_at_ms: now_ms,
    };
    validate_work_dispatch_link(tx, &link)?;
    let key = operation_link_key(operation_id);
    if let Some(existing) = config::read_record(tx, &key, "WorkDispatch operation link")? {
        let existing: WorkDispatchOperationLink =
            serde_json::from_value(existing).map_err(|_| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "retained WorkDispatch operation link fields are invalid",
                )
            })?;
        validate_work_dispatch_link(tx, &existing)?;
        if !same_work_dispatch_link_identity(&existing, &link) {
            return Err(Error::conflict(
                "launch Operation already has a different WorkDispatch attribution",
            ));
        }
        save_entry_link_index(tx, &existing)?;
        return Ok(existing);
    }
    config::write_record(tx, &key, &link.value()?)?;
    save_entry_link_index(tx, &link)?;
    Ok(link)
}

/// Load and integrity-check the retained WorkDispatch link. Caller visibility
/// must still be filtered by the existing current manager/task authorization.
pub(crate) fn operation_link(
    db: &Connection,
    operation_id: &str,
) -> Result<Option<WorkDispatchOperationLink>> {
    let Some(link) = read_work_dispatch_operation_link(db, operation_id)? else {
        return Ok(None);
    };
    require_work_dispatch_slot(db, &link)?;
    Ok(Some(link))
}

fn read_work_dispatch_operation_link(
    db: &Connection,
    operation_id: &str,
) -> Result<Option<WorkDispatchOperationLink>> {
    let Some(value) = config::read_record(
        db,
        &operation_link_key(operation_id),
        "WorkDispatch operation link",
    )?
    else {
        return Ok(None);
    };
    let link: WorkDispatchOperationLink = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "WorkDispatch operation link fields are invalid",
        )
    })?;
    validate_work_dispatch_link(db, &link)?;
    Ok(Some(link))
}

/// List only this manager/entry's durable operation links. The caller must
/// perform current manager/project authorization before invoking this page.
pub(crate) fn entry_operation_links(
    db: &Connection,
    owner: &str,
    project: &str,
    automation_id: &str,
    after: &str,
    limit: usize,
) -> Result<Vec<WorkDispatchOperationLink>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let prefix = entry_link_prefix(owner, project, automation_id)?;
    let after_key = if after.is_empty() {
        prefix.clone()
    } else {
        format!("{prefix}{after}")
    };
    let mut statement =
        db.prepare("SELECT key FROM meta WHERE key LIKE ?1 AND key>?2 ORDER BY key LIMIT ?3")?;
    let keys = statement
        .query_map(
            params![format!("{prefix}%"), after_key, limit.min(100) as i64],
            |row| row.get::<_, String>(0),
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);
    let mut links = Vec::with_capacity(keys.len());
    for key in keys {
        let value = config::read_record(db, &key, "WorkDispatch entry operation link")?
            .ok_or_else(|| Error::new("AUTOMATION_LINK_CORRUPT", "entry link index is dangling"))?;
        let link: WorkDispatchOperationLink = serde_json::from_value(value).map_err(|_| {
            Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "WorkDispatch entry link fields are invalid",
            )
        })?;
        validate_work_dispatch_link(db, &link)?;
        require_work_dispatch_slot(db, &link)?;
        if link.effective_manager_id != owner
            || link.project_id != project
            || link.automation_id != automation_id
            || entry_link_key(owner, project, automation_id, &link.operation_id)? != key
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "WorkDispatch entry link index crossed its manager or project scope",
            ));
        }
        links.push(link);
    }
    Ok(links)
}

/// Rebuild the private context only from a validated retained Store link.
/// This is for the shared launcher dispatcher, never request parsing.
pub(crate) fn context_for_operation(
    db: &Connection,
    operation_id: &str,
) -> Result<Option<WorkDispatchContext>> {
    let Some(link) = operation_link(db, operation_id)? else {
        return Ok(None);
    };
    let source = source_from_link(&link)?;
    let launch_settings = retained_launch_settings(db, operation_id)?;
    let context = WorkDispatchContext::from_validated_operation_link(
        db,
        &link.operation_id,
        &link.effective_manager_id,
        &link.automation_id,
        link.automation_revision,
        &link.project_id,
        &link.task_id,
        link.task_revision,
        link.attempt_id.as_deref(),
        launch_settings,
        source,
        &link.semantic_slot_id,
    )?;
    Ok(Some(context))
}

fn retained_launch_settings(
    db: &Connection,
    operation_id: &str,
) -> Result<WorkDispatchLaunchSettings> {
    let raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
        [operation_id],
        |row| row.get(0),
    )?;
    let value: Value = serde_json::from_str(&raw).map_err(|_| {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "linked launch request is invalid",
        )
    })?;
    let request = LaunchRequest::parse(&value).map_err(|_| {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "linked launch request does not match the canonical contract",
        )
    })?;
    Ok(WorkDispatchLaunchSettings::from_preview(&request.preview))
}

fn validate_work_dispatch_link(db: &Connection, link: &WorkDispatchOperationLink) -> Result<()> {
    let corrupt = || {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "WorkDispatch link does not match its retained Operation, source, or semantic slot",
        )
    };
    if link.schema_version != 1
        || link.operation_id.is_empty()
        || link.operation_id.len() > 256
        || link.technical_requester_id != AUTOMATION_TECHNICAL_REQUESTER_ID
        || link.effective_manager_id.is_empty()
        || link.effective_manager_id.len() > 256
        || link.automation_id.is_empty()
        || link.automation_revision <= 0
        || link.project_id.is_empty()
        || link.task_revision <= 0
        || link.action != "swarm.launch"
        || link.semantic_cause_id != link.semantic_slot_id
        || link.linked_at_ms < 0
        || link.semantic_cause_kind
            != if link.attempt_id.is_some() {
                "manager_owned_assignment"
            } else {
                "initial_task"
            }
    {
        return Err(corrupt());
    }
    validate_slot_identity_fields(
        &link.effective_manager_id,
        &link.task_id,
        link.task_revision,
        link.attempt_id.as_deref(),
    )
    .map_err(|_| corrupt())?;
    let operation: Option<LaunchOperationIdentityRow> = db
        .query_row(
            "SELECT caller_id,method,task_id,attempt_id,original_request_json \
             FROM operations WHERE operation_id=?1",
            [&link.operation_id],
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
    let Some((caller, method, operation_task, operation_attempt, original_json)) = operation else {
        return Err(corrupt());
    };
    if caller != link.technical_requester_id
        || method != "swarm.launch"
        || operation_task.as_deref() != Some(link.task_id.as_str())
        || link
            .attempt_id
            .as_deref()
            .is_some_and(|attempt_id| operation_attempt.as_deref() != Some(attempt_id))
    {
        return Err(corrupt());
    }
    let request_value: Value = serde_json::from_str(&original_json).map_err(|_| corrupt())?;
    let request = LaunchRequest::parse(&request_value).map_err(|_| corrupt())?;
    if request.preview.task_id != link.task_id
        || request.preview.expected_task_revision != link.task_revision
    {
        return Err(corrupt());
    }
    let expected_slot = WorkDispatchContext::from_validated_operation_link(
        db,
        &link.operation_id,
        &link.effective_manager_id,
        &link.automation_id,
        link.automation_revision,
        &link.project_id,
        &link.task_id,
        link.task_revision,
        link.attempt_id.as_deref(),
        WorkDispatchLaunchSettings::from_preview(&request.preview),
        source_from_link(link)?,
        &link.semantic_slot_id,
    )
    .map_err(|_| corrupt())?;
    let (slot_id, parameters_digest) = slot_identity_for_assignment(
        &link.effective_manager_id,
        &link.task_id,
        link.task_revision,
        link.attempt_id.as_deref(),
        &request.preview,
    )
    .map_err(|_| corrupt())?;
    if slot_id != link.semantic_slot_id
        || expected_slot.semantic_slot_id() != link.semantic_slot_id
        || parameters_digest.len() != 64
    {
        return Err(corrupt());
    }
    if let Some(attempt_id) = operation_attempt.as_deref() {
        let exact_attempt: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts WHERE attempt_id=?1 AND task_id=?2 \
             AND task_revision=?3 AND owner_id=?4)",
            params![
                attempt_id,
                link.task_id,
                link.task_revision,
                link.effective_manager_id
            ],
            |row| row.get(0),
        )?;
        if !exact_attempt {
            return Err(corrupt());
        }
    }
    let source_observation_id = link.source["observation_id"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(corrupt)?;
    let source_kind = link.source["event_kind"].as_str().ok_or_else(corrupt)?;
    let source_operation_id = link.source["operation_id"].as_str().ok_or_else(corrupt)?;
    let source_row: Option<(String, Option<String>, Option<String>, String)> = db
        .query_row(
            "SELECT kind,source_event_key,operation_id,payload_json FROM observations \
             WHERE observation_id=?1 AND source_stream_id='controller'",
            [source_observation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((kind, event_key, operation_id, payload_json)) = source_row else {
        return Err(corrupt());
    };
    if kind != source_kind
        || event_key.as_deref() != Some(source_operation_id)
        || operation_id.as_deref() != Some(source_operation_id)
    {
        return Err(corrupt());
    }
    let source_fact = WorkFact {
        observation_id: source_observation_id,
        kind,
        source_event_key: event_key,
        operation_id,
        payload_json,
    };
    if !matches!(
        read_work_fact_subject(db, &source_fact).map_err(|_| corrupt())?,
        WorkFactSubject::Candidate {
            source: _,
            task_id,
            task_revision,
            attempt_id
        } if task_id == link.task_id
            && task_revision == link.task_revision
            && attempt_id == link.attempt_id
    ) {
        return Err(corrupt());
    }
    Ok(())
}

fn require_work_dispatch_slot(db: &Connection, link: &WorkDispatchOperationLink) -> Result<()> {
    let slot = load_slot(db, &link.semantic_slot_id)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "WorkDispatch Operation has no retained semantic launch slot",
        )
    })?;
    if slot.operation_id != link.operation_id
        || slot.manager_id != link.effective_manager_id
        || slot.task_id != link.task_id
        || slot.task_revision != link.task_revision
        || slot.attempt_id != link.attempt_id
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "WorkDispatch Operation link differs from its semantic launch slot",
        ));
    }
    Ok(())
}

fn source_from_link(link: &WorkDispatchOperationLink) -> Result<WorkDispatchSource> {
    model::fields(
        &link.source,
        &["observation_id", "event_kind", "operation_id"],
    )
    .map_err(|_| {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "WorkDispatch source fields are invalid",
        )
    })?;
    WorkDispatchSource::from_committed_fact(
        link.source["observation_id"].as_i64().unwrap_or_default(),
        link.source["event_kind"].as_str().unwrap_or_default(),
        link.source["operation_id"].as_str().unwrap_or_default(),
    )
    .map_err(|_| {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "WorkDispatch source identity is invalid",
        )
    })
}

fn same_work_dispatch_link_identity(
    left: &WorkDispatchOperationLink,
    right: &WorkDispatchOperationLink,
) -> bool {
    left.operation_id == right.operation_id
        && left.technical_requester_id == right.technical_requester_id
        && left.effective_manager_id == right.effective_manager_id
        && left.automation_id == right.automation_id
        && left.automation_revision == right.automation_revision
        && left.project_id == right.project_id
        && left.action == right.action
        && left.semantic_cause_kind == right.semantic_cause_kind
        && left.semantic_cause_id == right.semantic_cause_id
        && left.semantic_slot_id == right.semantic_slot_id
        && left.task_id == right.task_id
        && left.task_revision == right.task_revision
        && left.attempt_id == right.attempt_id
        && left.source == right.source
}

fn save_entry_link_index(tx: &Transaction<'_>, link: &WorkDispatchOperationLink) -> Result<()> {
    let key = entry_link_key(
        &link.effective_manager_id,
        &link.project_id,
        &link.automation_id,
        &link.operation_id,
    )?;
    if let Some(existing) = config::read_record(tx, &key, "WorkDispatch entry operation link")? {
        let existing: WorkDispatchOperationLink =
            serde_json::from_value(existing).map_err(|_| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "WorkDispatch entry operation index is invalid",
                )
            })?;
        if !same_work_dispatch_link_identity(&existing, link) {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "WorkDispatch entry operation index points to another link",
            ));
        }
        return Ok(());
    }
    config::write_record(tx, &key, &link.value()?)
}

fn operation_link_key(operation_id: &str) -> String {
    format!("{OPERATION_LINK_PREFIX}{operation_id}")
}

fn entry_link_prefix(owner: &str, project: &str, automation_id: &str) -> Result<String> {
    let identity = json!({
        "manager_id":owner,
        "project_id":project,
        "automation_id":automation_id
    });
    Ok(format!(
        "{ENTRY_LINK_PREFIX}{}:",
        model::digest(model::canonical(&identity)?.as_bytes())
    ))
}

fn entry_link_key(
    owner: &str,
    project: &str,
    automation_id: &str,
    operation_id: &str,
) -> Result<String> {
    Ok(format!(
        "{}{operation_id}",
        entry_link_prefix(owner, project, automation_id)?
    ))
}
