//! Bounded routing from applied acceptance facts to the existing durable
//! managed-Issue-label Operation. Remote reads/writes remain in
//! `store::github_effects` after this transaction commits.

use super::automation_reconcile::{
    self, DomainErrorDisposition, MalformedAutomationEntry, QuarantineEvidence, SubjectDisposition,
    SubjectErrorDisposition,
};
use super::{Store, capacity, github_effects};
use crate::{
    automation::{
        actions::AutomationStep,
        authorization::{self, GithubProjectionContext},
        config::{self, AutomationEntry},
    },
    error::{Error, Result},
    model,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const STATE_SCHEMA_VERSION: u32 = 1;
const GLOBAL_CURSOR_KEY: &str = "automation:v1:github-projection:global-cursor";
const EFFECT_DRAIN_CURSOR_KEY: &str = "automation:v1:github-projection:effect-drain-cursor";
const STATE_PREFIX: &str = "automation:v1:github-projection:state:";
const QUARANTINE_PREFIX: &str = "automation:v1:github-projection:quarantine:";
const ACCEPTANCE_STREAM: &str = "controller:acceptance";
const METHOD: &str = "github.effect.managed_label";
const MAX_ENTRIES_PER_PASS: usize = 16;
const MAX_FACTS_PER_ENTRY: usize = 16;
const MAX_EFFECTS_PER_PASS: usize = 16;
const MAX_RECENT: usize = 32;

/// Root may continue after these exact projection-state failures only after
/// rolling back this domain transaction. All other errors stop.
pub(super) fn classify_domain_error(error: Error) -> DomainErrorDisposition {
    if matches!(
        error.code.as_str(),
        "AUTOMATION_GITHUB_PROJECTION_CURSOR_CORRUPT"
            | "AUTOMATION_GITHUB_PROJECTION_CURSOR_MISMATCH"
            | "AUTOMATION_GITHUB_PROJECTION_STATE_CORRUPT"
    ) {
        DomainErrorDisposition::Degraded { code: error.code }
    } else {
        DomainErrorDisposition::Fatal(error)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GlobalCursor {
    schema_version: u32,
    last_entry_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EffectDrainCursor {
    schema_version: u32,
    last_created_at_ms: i64,
    last_operation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectionState {
    schema_version: u32,
    owner_manager_id: String,
    project_id: String,
    automation_id: String,
    configured_revision: i64,
    cursor: i64,
    activation_cut: i64,
    catch_up_until: Option<i64>,
    activation_history_unavailable: bool,
    recent: Vec<Value>,
    updated_at_ms: i64,
}

#[derive(Debug, Clone)]
struct AcceptanceEvent {
    observation_id: i64,
    source_event_key: Option<String>,
    operation_id: Option<String>,
    kind: String,
    payload_json: String,
}

/// Commit a new activation watermark with the revisioned configuration.
/// Existing acceptance facts are covered only by explicit include-existing.
pub(crate) fn configure_activation(
    tx: &Transaction<'_>,
    before: Option<&AutomationEntry>,
    after: &AutomationEntry,
    include_existing: bool,
    cut: i64,
    now_ms: i64,
) -> Result<()> {
    if cut < 0 || now_ms < 0 {
        return Err(Error::invalid(
            "GitHub projection activation cut and time must be non-negative",
        ));
    }
    let key = state_key(after)?;
    let before_active = before.is_some_and(entry_projection_active);
    let after_active = entry_projection_active(after);
    let target_changed =
        before.is_some_and(|entry| entry.github_projection != after.github_projection);
    let new_coverage = after_active && (!before_active || target_changed);
    let loaded = load_state(tx, after)?;
    let state_missing = loaded.is_none();
    let mut state = loaded.unwrap_or_else(|| empty_state(after, cut, now_ms, before_active));

    if new_coverage {
        state.activation_cut = cut;
        state.cursor = if include_existing { 0 } else { cut };
        state.catch_up_until = include_existing.then_some(cut);
        state.activation_history_unavailable = false;
        state.recent.clear();
        remember_recent(
            &mut state,
            json!({
                "status":"activation_updated",
                "include_existing":include_existing,
                "activation_cut":cut,
                "source_id":after.github_projection.as_ref().map(|settings| settings.source_id.as_str()),
                "label":after.github_projection.as_ref().map(|settings| settings.label.as_str()),
                "present":after.github_projection.as_ref().map(|settings| settings.present),
                "recorded_at_ms":now_ms
            }),
        );
    } else if state_missing && before_active {
        state.cursor = cut;
        state.activation_cut = cut;
        state.catch_up_until = None;
        state.activation_history_unavailable = true;
        remember_recent(
            &mut state,
            json!({
                "status":"capability_gap",
                "code":"activation_history_unavailable",
                "reason":"no activation watermark was retained; earlier acceptance facts were not replayed",
                "recorded_at_ms":now_ms
            }),
        );
    }
    state.configured_revision = after.revision;
    state.updated_at_ms = now_ms;
    save_state(tx, &key, &state)
}

/// Run one fair, bounded page of enabled, configured GitHub label projections.
pub(crate) fn reconcile(
    tx: &Transaction<'_>,
    entry_budget: usize,
    fact_budget: usize,
    now_ms: i64,
) -> Result<Value> {
    if now_ms < 0 {
        return Err(Error::invalid(
            "GitHub projection reconciliation time is invalid",
        ));
    }
    let entry_budget = entry_budget.min(MAX_ENTRIES_PER_PASS);
    let fact_budget = fact_budget.min(MAX_FACTS_PER_ENTRY);
    if entry_budget == 0 || fact_budget == 0 {
        return Ok(json!({
            "entries":[],
            "processed":0,
            "entry_budget":entry_budget,
            "fact_budget_per_entry":fact_budget,
            "status":"idle"
        }));
    }
    let (entries, last_entry_key, malformed_entries) = enabled_entry_page(tx, entry_budget)?;
    for malformed in &malformed_entries {
        persist_malformed_entry(tx, malformed, now_ms)?;
    }
    let mut results = Vec::with_capacity(entries.len());
    let mut total_processed = 0usize;
    let mut total_quarantined = malformed_entries.len();
    for entry in &entries {
        let result = reconcile_entry(tx, entry, fact_budget, now_ms)?;
        total_processed = total_processed.saturating_add(
            result["processed"]
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or_default(),
        );
        total_quarantined = total_quarantined.saturating_add(
            result["quarantined"]
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or_default(),
        );
        results.push(result);
    }
    if let Some(last_entry_key) = &last_entry_key {
        config::write_record(
            tx,
            GLOBAL_CURSOR_KEY,
            &json!({"schema_version":1,"last_entry_key":last_entry_key}),
        )?;
    }
    Ok(json!({
        "entries":results,
        "processed":total_processed,
        "quarantined":total_quarantined,
        "status":if total_quarantined > 0 {
            "degraded"
        } else if total_processed == 0 {
            "idle"
        } else {
            "progressed"
        },
        "entry_budget":entry_budget,
        "fact_budget_per_entry":fact_budget,
        "cursor":last_entry_key
    }))
}

/// Reconcile previously admitted queued Operations as well as fresh ones.
/// Only queued operations cross the write boundary; `outcome_unknown` remains
/// on the established exact readback-only manager path.
pub(crate) async fn reconcile_effects_once(store: &Store, budget: usize) -> Result<Value> {
    let limit = budget.min(MAX_EFFECTS_PER_PASS);
    if limit == 0 {
        return Ok(json!({"operations":[],"processed":0,"status":"idle"}));
    }
    let operation_ids = store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let cursor = config::read_record(
                &tx,
                EFFECT_DRAIN_CURSOR_KEY,
                "GitHub projection effect drain cursor",
            )?
            .map(|value| {
                let cursor: EffectDrainCursor = serde_json::from_value(value).map_err(|_| {
                    Error::new(
                        "AUTOMATION_GITHUB_PROJECTION_CURSOR_CORRUPT",
                        "GitHub projection effect drain cursor fields are invalid",
                    )
                })?;
                if cursor.schema_version != 1
                    || cursor.last_created_at_ms < 0
                    || cursor.last_operation_id.is_empty()
                {
                    return Err(Error::new(
                        "AUTOMATION_GITHUB_PROJECTION_CURSOR_CORRUPT",
                        "GitHub projection effect drain cursor is invalid",
                    ));
                }
                Ok(cursor)
            })
            .transpose()?;

            let mut rows = select_queued_effect_page(&tx, cursor.as_ref(), limit)?;
            // Wrap to the oldest retained queued Operation after reaching the
            // end. Advancing this durable cursor before dispatch means a
            // transiently failing Operation cannot monopolize every pass; if
            // the process stops, wrapped paging will still rediscover it.
            if rows.is_empty() && cursor.is_some() {
                rows = select_queued_effect_page(&tx, None, limit)?;
            }
            if let Some((last_created_at_ms, last_operation_id)) = rows.last() {
                config::write_record(
                    &tx,
                    EFFECT_DRAIN_CURSOR_KEY,
                    &json!({
                        "schema_version":1,
                        "last_created_at_ms":last_created_at_ms,
                        "last_operation_id":last_operation_id
                    }),
                )?;
            }
            tx.commit()?;
            Ok(rows
                .into_iter()
                .map(|(_, operation_id)| operation_id)
                .collect::<Vec<_>>())
        })
        .await?;
    let mut results = Vec::with_capacity(operation_ids.len());
    for operation_id in operation_ids {
        match github_effects::call_automatic(store, &operation_id).await {
            Ok(result) => results.push(result),
            Err(error) => results.push(json!({
                "operation_id":operation_id,
                "status":"dispatch_error",
                "error":{"code":error.code,"message":error.message}
            })),
        }
    }
    let processed = results.len();
    Ok(json!({
        "operations":results,
        "processed":processed,
        "budget":limit
    }))
}

fn select_queued_effect_page(
    db: &Connection,
    after: Option<&EffectDrainCursor>,
    limit: usize,
) -> Result<Vec<(i64, String)>> {
    let mut statement = db.prepare(
        "SELECT created_at_ms,operation_id FROM operations WHERE method=?1 AND caller_id=?2 AND state='queued' \
         AND json_extract(effective_request_json,'$.automation_on_behalf.action')=?1 \
         AND (?3 IS NULL OR created_at_ms>?3 OR (created_at_ms=?3 AND operation_id>?4)) \
         ORDER BY created_at_ms,operation_id LIMIT ?5",
    )?;
    Ok(statement
        .query_map(
            params![
                METHOD,
                authorization::AUTOMATION_TECHNICAL_REQUESTER_ID,
                after.map(|cursor| cursor.last_created_at_ms),
                after.map(|cursor| cursor.last_operation_id.as_str()),
                limit as i64
            ],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

/// Bounded manager-facing state for automation explain/readback.
pub(crate) fn state(db: &Connection, entry: &AutomationEntry) -> Result<Value> {
    match load_state(db, entry)? {
        Some(state) => Ok(state_projection(&state)),
        None => Ok(json!({
            "automation_id":entry.automation_id,
            "status":"uninitialized",
            "coverage":"partial",
            "gaps":[{"code":"activation_history_unavailable"}]
        })),
    }
}

/// Preserve the exact cursor and retained cause summary across Manager transfer.
pub(crate) fn relocate_state(
    tx: &Transaction<'_>,
    former: &AutomationEntry,
    new: &AutomationEntry,
) -> Result<()> {
    config::validate_entry(former)?;
    config::validate_entry(new)?;
    if former.owner_manager_id == new.owner_manager_id
        || former.project_id != new.project_id
        || former.automation_id != new.automation_id
    {
        return Err(Error::invalid(
            "GitHub projection relocation must preserve project and automation identity while changing owner",
        ));
    }
    let source_key = state_key(former)?;
    let target_key = state_key(new)?;
    let target_exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM meta WHERE key=?1)",
        [&target_key],
        |row| row.get(0),
    )?;
    if target_exists {
        return Err(Error::conflict(
            "GitHub projection target state already exists",
        ));
    }
    let Some(mut state) = load_state(tx, former)? else {
        return Ok(());
    };
    state.owner_manager_id = new.owner_manager_id.clone();
    state.configured_revision = new.revision;
    validate_state(&state, new)?;
    save_state(tx, &target_key, &state)?;
    let deleted = tx.execute("DELETE FROM meta WHERE key=?1", [&source_key])?;
    if deleted != 1 {
        return Err(Error::new(
            "AUTOMATION_GITHUB_PROJECTION_STATE_MISSING",
            "GitHub projection source state changed during relocation",
        ));
    }
    Ok(())
}

fn reconcile_entry(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    budget: usize,
    now_ms: i64,
) -> Result<Value> {
    if !entry.enabled || !entry.steps.contains(&AutomationStep::GithubProjection) {
        return Ok(json!({
            "automation_id":entry.automation_id,
            "processed":0,
            "status":"disabled_or_unselected"
        }));
    }
    if !entry.github_projection_ready() {
        return Ok(json!({
            "automation_id":entry.automation_id,
            "processed":0,
            "status":"capability_gap",
            "code":"github_projection_settings_required"
        }));
    }
    let key = state_key(entry)?;
    let high_water = acceptance_high_water(tx)?;
    let mut state = match load_state(tx, entry)? {
        Some(state) => state,
        None => {
            let state = empty_state(entry, high_water, now_ms, true);
            save_state(tx, &key, &state)?;
            return Ok(state_projection_with_processed(&state, 0, 0, high_water));
        }
    };
    if state.configured_revision != entry.revision {
        return Err(Error::new(
            "AUTOMATION_GITHUB_PROJECTION_CURSOR_MISMATCH",
            "GitHub projection activation cursor does not match the current automation revision",
        ));
    }
    let mut quarantined = 0usize;
    if budget == 0 {
        return Ok(state_projection_with_processed(
            &state,
            0,
            quarantined,
            high_water,
        ));
    }
    let target = state
        .catch_up_until
        .map_or(high_water, |cut| cut.min(high_water));
    if state.cursor >= target {
        if state.catch_up_until.is_some_and(|cut| state.cursor >= cut) {
            state.catch_up_until = None;
        }
        state.updated_at_ms = now_ms;
        save_state(tx, &key, &state)?;
        return Ok(state_projection_with_processed(
            &state,
            0,
            quarantined,
            high_water,
        ));
    }
    let limit = i64::try_from(budget.saturating_add(1))
        .map_err(|_| Error::invalid("GitHub projection page limit exceeds platform range"))?;
    let mut statement = tx.prepare(
        "SELECT observation_id,source_event_key,operation_id,kind,payload_json FROM observations \
         WHERE source_stream_id=?1 AND kind='task.acceptance' AND observation_id>?2 \
           AND observation_id<=?3 ORDER BY observation_id LIMIT ?4",
    )?;
    let events = statement
        .query_map(
            params![ACCEPTANCE_STREAM, state.cursor, target, limit],
            |row| {
                Ok(AcceptanceEvent {
                    observation_id: row.get(0)?,
                    source_event_key: row.get(1)?,
                    operation_id: row.get(2)?,
                    kind: row.get(3)?,
                    payload_json: row.get(4)?,
                })
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);
    let has_more = events.len() > budget;
    let mut processed = 0usize;
    let mut waiting = false;
    for event in events.iter().take(budget) {
        let replay = event.observation_id <= state.activation_cut
            && state.catch_up_until == Some(state.activation_cut);
        let evidence = projection_event_evidence(event);
        let disposition = automation_reconcile::with_subject_savepoint(
            tx,
            || {
                let Some(accepted_operation_id) = event_identity(event)? else {
                    return Ok(SubjectDisposition::Skipped {
                        code: "acceptance_fact_not_applied".to_owned(),
                        reason: "acceptance Observation is not an applied candidate".to_owned(),
                    });
                };
                let context = GithubProjectionContext::from_acceptance_observation(
                    tx,
                    entry,
                    event.observation_id,
                    &accepted_operation_id,
                    state.activation_cut,
                    replay,
                )?;
                let request = context.request_value()?;
                let value = reserve_projection(tx, entry, &context, &request, now_ms)?;
                Ok(SubjectDisposition::Applied((accepted_operation_id, value)))
            },
            |error| classify_projection_subject_error(error, evidence.clone()),
        )?;
        match disposition {
            SubjectDisposition::Applied((accepted_operation_id, value)) => {
                remember_recent(
                    &mut state,
                    json!({
                        "observation_id":event.observation_id,
                        "accepted_operation_id":accepted_operation_id,
                        "status":value["status"],
                        "code":value.get("code").cloned().unwrap_or(Value::Null),
                        "effect_operation_id":value.get("operation_id").cloned().unwrap_or(Value::Null),
                        "recorded_at_ms":now_ms
                    }),
                );
                state.cursor = event.observation_id;
                processed += 1;
            }
            SubjectDisposition::Pending { code, reason } => {
                remember_recent(
                    &mut state,
                    json!({
                        "observation_id":event.observation_id,
                        "status":"pending",
                        "code":code,
                        "reason":reason,
                        "effect_operation_id":null
                    }),
                );
                waiting = true;
                break;
            }
            SubjectDisposition::Skipped { code, reason } => {
                remember_recent(
                    &mut state,
                    json!({
                        "observation_id":event.observation_id,
                        "status":"skipped",
                        "code":code,
                        "reason":reason,
                        "effect_operation_id":null
                    }),
                );
                state.cursor = event.observation_id;
                processed += 1;
            }
            SubjectDisposition::Quarantined { code, evidence } => {
                persist_subject_quarantine(tx, &code, evidence, now_ms)?;
                quarantined = quarantined.saturating_add(1);
                remember_recent(
                    &mut state,
                    json!({
                        "observation_id":event.observation_id,
                        "status":"quarantined",
                        "code":code,
                        "effect_operation_id":null,
                        "recorded_at_ms":now_ms
                    }),
                );
                state.cursor = event.observation_id;
                processed += 1;
            }
        }
    }
    if !has_more && !waiting {
        state.cursor = target;
        if state.catch_up_until.is_some_and(|cut| state.cursor >= cut) {
            state.catch_up_until = None;
        }
    }
    state.updated_at_ms = now_ms;
    save_state(tx, &key, &state)?;
    Ok(state_projection_with_processed(
        &state,
        processed,
        quarantined,
        high_water,
    ))
}

fn reserve_projection(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    context: &GithubProjectionContext,
    request_value: &Value,
    now_ms: i64,
) -> Result<Value> {
    let caller = context.technical_requester_id();
    let request_id = model::text(request_value, "client_request_id")?;
    let original_json = model::canonical(request_value)?;
    let existing: Option<(String, String, String, Option<String>, String)> = tx
        .query_row(
            "SELECT operation_id,method,original_request_json,result_json,state FROM operations WHERE caller_id=?1 AND client_request_id=?2",
            params![caller, request_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .optional()?;
    if let Some((operation_id, method, original, result_json, state)) = existing {
        if method != METHOD || original != original_json {
            return Err(Error::new(
                "AUTOMATION_GITHUB_PROJECTION_SLOT_CONFLICT",
                "this deterministic managed-label request ID already retains different inputs",
            ));
        }
        let retained = GithubProjectionContext::from_committed_operation(tx, &operation_id)?;
        if retained.request_value()? != *request_value {
            return Err(Error::new(
                "AUTOMATION_GITHUB_PROJECTION_SLOT_CONFLICT",
                "existing managed-label Operation does not retain this exact acceptance cause",
            ));
        }
        if !matches!(
            state.as_str(),
            "queued" | "sending" | "outcome_unknown" | "settled" | "rejected" | "cancelled"
        ) {
            return Err(Error::new(
                "AUTOMATION_OPERATION_CORRUPT",
                "existing managed-label Operation has an unsupported state",
            ));
        }
        let result = result_json
            .map(|raw| serde_json::from_str::<Value>(&raw))
            .transpose()?
            .unwrap_or_else(|| json!({"operation_id":operation_id,"state":state}));
        return Ok(json!({
            "status":"coalesced",
            "operation_id":operation_id,
            "operation_state":state,
            "coalesced":true,
            "effect_started":matches!(state.as_str(),"queued"|"sending"|"outcome_unknown"),
            "receipt":result
        }));
    }

    let operation_id = model::new_id();
    let attribution = context.linkage_value();
    tx.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6,'queued',?7,?7,?7)",
        params![
            operation_id,
            caller,
            request_id,
            METHOD,
            original_json,
            model::canonical(&json!({"automation_on_behalf":attribution}))?,
            now_ms
        ],
    )?;
    let receipt = github_effects::reserve_on_behalf(
        tx,
        entry,
        context,
        request_value,
        &operation_id,
        now_ms,
    )?;
    let mut effective: Value = json!({"automation_on_behalf":context.linkage_value()});
    effective["receipt"] = json!({"ok":true,"value":receipt});
    tx.execute(
        "UPDATE operations SET result_json=?2,effective_request_json=?3,updated_at_ms=?4 WHERE operation_id=?1 AND method=?5 AND state='queued'",
        params![operation_id, model::canonical(&receipt)?, model::canonical(&effective)?, now_ms, METHOD],
    )?;
    capacity::sync_operation(tx, &operation_id, now_ms)?;
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller',?1,?1,?2,?3,?4)",
        params![operation_id, METHOD, model::canonical(&receipt)?, now_ms],
    )?;
    authorization::save_github_projection_operation_link(tx, &operation_id, context, now_ms)?;
    Ok(json!({
        "status":"projection_reserved",
        "operation_id":operation_id,
        "operation_state":"queued",
        "coalesced":false,
        "effect_started":false,
        "receipt":receipt
    }))
}

fn event_identity(event: &AcceptanceEvent) -> Result<Option<String>> {
    let Some(operation_id) = event.operation_id.as_deref() else {
        return Ok(None);
    };
    let expected_key = format!("accept:{operation_id}");
    if event.kind != "task.acceptance"
        || event.source_event_key.as_deref() != Some(expected_key.as_str())
    {
        return Ok(None);
    }
    let payload: Value = serde_json::from_str(&event.payload_json).map_err(|_| {
        Error::new(
            "AUTOMATION_FACT_CORRUPT",
            "acceptance Observation payload is invalid",
        )
    })?;
    if payload["operation_id"] != operation_id
        || payload["acceptance_operation_id"] != operation_id
        || payload["outcome"] != "applied"
        || payload["task_accepted"] != true
    {
        return Ok(None);
    }
    Ok(Some(operation_id.to_owned()))
}

fn classify_projection_subject_error(
    error: &Error,
    evidence: QuarantineEvidence,
) -> Option<SubjectErrorDisposition> {
    match error.code.as_str() {
        "AUTOMATION_CURRENT_GM_REQUIRED" | "GITHUB_EFFECT_TARGET_NOT_FOUND"
        | "GITHUB_EFFECT_SLOT_BUSY" => Some(SubjectErrorDisposition::Pending {
            code: error.code.clone(),
            reason: "current Manager authority, source mapping, or exact effect slot may become available later".to_owned(),
        }),
        "AUTOMATION_ACTION_CHANGED"
        | "AUTOMATION_GITHUB_PROJECTION_SETTINGS_REQUIRED"
        | "AUTOMATION_GITHUB_PROJECTION_SOURCE_STALE"
        | "FORBIDDEN" => Some(SubjectErrorDisposition::Skipped {
            code: error.code.clone(),
            reason: "the accepted candidate is no longer selected for this exact current projection".to_owned(),
        }),
        "AUTOMATION_FACT_CORRUPT"
        | "AUTOMATION_FACT_MISSING"
        | "AUTOMATION_LINK_CORRUPT"
        | "AUTOMATION_OPERATION_CORRUPT"
        | "AUTOMATION_RECORD_CORRUPT" => Some(SubjectErrorDisposition::Quarantined {
            code: error.code.clone(),
            evidence,
        }),
        _ => None,
    }
}

fn projection_event_evidence(event: &AcceptanceEvent) -> QuarantineEvidence {
    let operation_digest = event
        .operation_id
        .as_deref()
        .map(|value| model::digest(value.as_bytes()))
        .unwrap_or_else(|| "missing".to_owned());
    QuarantineEvidence {
        subject_identity: format!(
            "acceptance-observation:{}:operation:{}",
            event.observation_id, operation_digest
        ),
        source_pointer: Some(format!("observations/{}", event.observation_id)),
        source_digest: Some(model::digest(event.payload_json.as_bytes())),
    }
}

fn persist_subject_quarantine(
    tx: &Transaction<'_>,
    code: &str,
    evidence: QuarantineEvidence,
    now_ms: i64,
) -> Result<()> {
    let record_key = automation_reconcile::quarantine_record_key(QUARANTINE_PREFIX, &evidence)?;
    match automation_reconcile::with_subject_savepoint(
        tx,
        || {
            automation_reconcile::persist_quarantine(tx, &record_key, code, evidence, now_ms)?;
            Ok(SubjectDisposition::Applied(()))
        },
        |_| None,
    )? {
        SubjectDisposition::Applied(()) => Ok(()),
        _ => Err(Error::new(
            "AUTOMATION_GITHUB_PROJECTION_QUARANTINE_INVALID",
            "projection subject did not produce durable quarantine evidence",
        )),
    }
}

fn enabled_entry_page(
    db: &Connection,
    limit: usize,
) -> Result<(
    Vec<AutomationEntry>,
    Option<String>,
    Vec<MalformedAutomationEntry>,
)> {
    if limit == 0 {
        return Ok((Vec::new(), None, Vec::new()));
    }
    let prefix = "automation:v1:entry:";
    let pattern = "automation:v1:entry:%";
    let cursor_value =
        config::read_record(db, GLOBAL_CURSOR_KEY, "GitHub projection global cursor").map_err(
            |error| {
                if error.code == "AUTOMATION_RECORD_CORRUPT" {
                    Error::new(
                        "AUTOMATION_GITHUB_PROJECTION_CURSOR_CORRUPT",
                        "GitHub projection global cursor record is corrupt",
                    )
                } else {
                    error
                }
            },
        )?;
    let cursor = cursor_value
        .map(|value| {
            serde_json::from_value::<GlobalCursor>(value).map_err(|_| {
                Error::new(
                    "AUTOMATION_GITHUB_PROJECTION_CURSOR_CORRUPT",
                    "GitHub projection global cursor fields are invalid",
                )
            })
        })
        .transpose()?;
    if cursor.as_ref().is_some_and(|cursor| {
        cursor.schema_version != 1
            || !cursor.last_entry_key.starts_with(prefix)
            || cursor.last_entry_key.len() > 512
            || cursor.last_entry_key.chars().any(char::is_control)
    }) {
        return Err(Error::new(
            "AUTOMATION_GITHUB_PROJECTION_CURSOR_CORRUPT",
            "GitHub projection global cursor identity is invalid",
        ));
    }
    let after = cursor.map_or_else(|| prefix.to_owned(), |cursor| cursor.last_entry_key);
    let mut keys = select_entry_keys(db, pattern, &after, limit)?;
    if keys.len() < limit {
        keys.extend(select_entry_keys_before(
            db,
            pattern,
            prefix,
            &after,
            limit - keys.len(),
        )?);
    }
    if keys.is_empty() {
        return Ok((Vec::new(), None, Vec::new()));
    }
    let last_key = keys.last().cloned();
    let mut entries = Vec::with_capacity(keys.len());
    let mut malformed_entries = Vec::new();
    for key in keys {
        let raw: String =
            db.query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
                row.get(0)
            })?;
        let entry = match automation_reconcile::parse_automation_entry(&raw, "automation entry") {
            Ok(entry) => entry,
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "AUTOMATION_RECORD_CORRUPT" | "AUTOMATION_RECORD_INVALID"
                ) =>
            {
                malformed_entries.push(MalformedAutomationEntry {
                    code: error.code,
                    evidence: automation_reconcile::automation_entry_evidence(&key, &raw),
                });
                continue;
            }
            Err(error) => return Err(error),
        };
        if config::entry_key(
            &entry.owner_manager_id,
            &entry.project_id,
            &entry.automation_id,
        )? != key
        {
            malformed_entries.push(MalformedAutomationEntry {
                code: "AUTOMATION_RECORD_INVALID".to_owned(),
                evidence: automation_reconcile::automation_entry_evidence(&key, &raw),
            });
            continue;
        }
        if entry.enabled && entry.steps.contains(&AutomationStep::GithubProjection) {
            entries.push(entry);
        }
    }
    Ok((entries, last_key, malformed_entries))
}

fn persist_malformed_entry(
    tx: &Transaction<'_>,
    malformed: &MalformedAutomationEntry,
    now_ms: i64,
) -> Result<()> {
    let record_key =
        automation_reconcile::quarantine_record_key(QUARANTINE_PREFIX, &malformed.evidence)?;
    match automation_reconcile::with_subject_savepoint(
        tx,
        || {
            automation_reconcile::persist_quarantine(
                tx,
                &record_key,
                &malformed.code,
                malformed.evidence.clone(),
                now_ms,
            )?;
            Ok(SubjectDisposition::Applied(()))
        },
        |_| None,
    )? {
        SubjectDisposition::Applied(()) => Ok(()),
        _ => Err(Error::new(
            "AUTOMATION_GITHUB_PROJECTION_QUARANTINE_INVALID",
            "malformed GitHub projection entry did not produce a durable quarantine",
        )),
    }
}

fn select_entry_keys(
    db: &Connection,
    pattern: &str,
    after: &str,
    limit: usize,
) -> Result<Vec<String>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut statement =
        db.prepare("SELECT key FROM meta WHERE key LIKE ?1 AND key>?2 ORDER BY key LIMIT ?3")?;
    Ok(statement
        .query_map(params![pattern, after, limit as i64], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn select_entry_keys_before(
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
        "SELECT key FROM meta WHERE key LIKE ?1 AND key>?2 AND key<?3 ORDER BY key LIMIT ?4",
    )?;
    Ok(statement
        .query_map(params![pattern, prefix, before, limit as i64], |row| {
            row.get(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn load_state(db: &Connection, entry: &AutomationEntry) -> Result<Option<ProjectionState>> {
    let Some(value) = config::read_record(db, &state_key(entry)?, "GitHub projection state")
        .map_err(|error| {
            if error.code == "AUTOMATION_RECORD_CORRUPT" {
                Error::new(
                    "AUTOMATION_GITHUB_PROJECTION_STATE_CORRUPT",
                    "GitHub projection state record is corrupt",
                )
            } else {
                error
            }
        })?
    else {
        return Ok(None);
    };
    let state: ProjectionState = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "AUTOMATION_GITHUB_PROJECTION_STATE_CORRUPT",
            "GitHub projection state fields are invalid",
        )
    })?;
    validate_state(&state, entry)?;
    Ok(Some(state))
}

fn validate_state(state: &ProjectionState, entry: &AutomationEntry) -> Result<()> {
    if state.schema_version != STATE_SCHEMA_VERSION
        || state.owner_manager_id != entry.owner_manager_id
        || state.project_id != entry.project_id
        || state.automation_id != entry.automation_id
        || state.configured_revision <= 0
        || state.configured_revision > entry.revision
        || state.cursor < 0
        || state.activation_cut < 0
        || state
            .catch_up_until
            .is_some_and(|cut| cut < state.cursor || cut != state.activation_cut)
        || state.recent.len() > MAX_RECENT
        || state.updated_at_ms < 0
    {
        return Err(Error::new(
            "AUTOMATION_GITHUB_PROJECTION_STATE_CORRUPT",
            "GitHub projection state identity or bounds are invalid",
        ));
    }
    Ok(())
}

fn state_key(entry: &AutomationEntry) -> Result<String> {
    Ok(format!(
        "{STATE_PREFIX}{}:{}",
        config::scope_digest(&entry.owner_manager_id, &entry.project_id)?,
        entry.automation_id
    ))
}

fn empty_state(
    entry: &AutomationEntry,
    cut: i64,
    now_ms: i64,
    history_unavailable: bool,
) -> ProjectionState {
    ProjectionState {
        schema_version: STATE_SCHEMA_VERSION,
        owner_manager_id: entry.owner_manager_id.clone(),
        project_id: entry.project_id.clone(),
        automation_id: entry.automation_id.clone(),
        configured_revision: entry.revision,
        cursor: cut,
        activation_cut: cut,
        catch_up_until: None,
        activation_history_unavailable: history_unavailable,
        recent: Vec::new(),
        updated_at_ms: now_ms,
    }
}

fn entry_projection_active(entry: &AutomationEntry) -> bool {
    entry.github_projection_ready()
}

fn acceptance_high_water(db: &Connection) -> Result<i64> {
    Ok(db.query_row(
        "SELECT COALESCE(MAX(observation_id),0) FROM observations WHERE source_stream_id=?1 AND kind='task.acceptance'",
        [ACCEPTANCE_STREAM],
        |row| row.get(0),
    )?)
}

fn save_state(db: &Connection, key: &str, state: &ProjectionState) -> Result<()> {
    config::write_record(db, key, &serde_json::to_value(state)?)
}

fn remember_recent(state: &mut ProjectionState, item: Value) {
    if state.recent.len() == MAX_RECENT {
        state.recent.remove(0);
    }
    state.recent.push(item);
}

fn state_projection(state: &ProjectionState) -> Value {
    json!({
        "automation_id":state.automation_id,
        "status":if state.activation_history_unavailable {"partial"} else {"active"},
        "coverage":if state.activation_history_unavailable {"partial"} else {"complete"},
        "cursor":state.cursor,
        "activation_cut":state.activation_cut,
        "catch_up_until":state.catch_up_until,
        "activation_history_unavailable":state.activation_history_unavailable,
        "recent":state.recent
    })
}

fn state_projection_with_processed(
    state: &ProjectionState,
    processed: usize,
    quarantined: usize,
    high_water: i64,
) -> Value {
    let mut result = state_projection(state);
    result["processed"] = json!(processed);
    result["quarantined"] = json!(quarantined);
    result["high_water"] = json!(high_water);
    result
}
