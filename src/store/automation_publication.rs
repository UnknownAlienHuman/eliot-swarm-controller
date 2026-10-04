//! Bounded, durable routing from applied acceptance facts to the existing
//! exact-candidate Forge publication Operation.
//!
//! This module only reserves the Operation. Native Git execution and every
//! uncertain-effect readback remain in `store::forge`.

use super::capacity;
use crate::{
    automation::{
        actions::AutomationStep,
        config::{self, AutomationEntry},
        publication::PublicationContext,
    },
    config::Config,
    error::{Error, Result},
    model,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const STATE_SCHEMA_VERSION: u32 = 1;
const GLOBAL_CURSOR_KEY: &str = "automation:v1:publication:global-cursor";
const STATE_PREFIX: &str = "automation:v1:publication:state:";
const ACCEPTANCE_STREAM: &str = "controller:acceptance";
const MAX_ENTRIES_PER_PASS: usize = 16;
const MAX_FACTS_PER_ENTRY: usize = 16;
const MAX_PENDING: usize = 64;
const MAX_PENDING_RECHECKS: usize = 4;
const MAX_RECENT: usize = 32;
const BASE_RETRY_DELAY_MS: i64 = 1_000;
const MAX_RETRY_DELAY_MS: i64 = 60_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GlobalCursor {
    schema_version: u32,
    last_entry_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicationState {
    schema_version: u32,
    owner_manager_id: String,
    project_id: String,
    automation_id: String,
    configured_revision: i64,
    cursor: i64,
    activation_cut: i64,
    catch_up_until: Option<i64>,
    activation_history_unavailable: bool,
    pending: Vec<PendingAcceptance>,
    recent: Vec<Value>,
    updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingAcceptance {
    observation_id: i64,
    accepted_operation_id: String,
    historical_replay_authorized: bool,
    retries: u32,
    next_retry_at_ms: i64,
}

#[derive(Debug)]
struct AcceptanceEvent {
    observation_id: i64,
    source_event_key: Option<String>,
    operation_id: Option<String>,
    payload_json: String,
}

struct AcceptanceReservation<'a> {
    event: &'a AcceptanceEvent,
    accepted_operation_id: &'a str,
    activation_cut: i64,
    historical_replay_authorized: bool,
}

#[derive(Debug)]
enum ReserveDisposition {
    Reserved(Value),
    Pending { code: String, reason: &'static str },
    Skipped { code: String, reason: &'static str },
}

/// Atomically updates the per-entry activation watermark with the revisioned
/// automation configuration. A historical replay is admitted only through
/// the explicit `include_existing` catch-up window.
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
            "publication activation cut and time must be non-negative",
        ));
    }
    let key = state_key(after)?;
    let before_active = before.is_some_and(entry_publication_active);
    let after_active = entry_publication_active(after);
    let target_changed = before.is_some_and(|entry| entry.publication != after.publication);
    let new_coverage = after_active && (!before_active || target_changed);
    let loaded = load_state(tx, after)?;
    let state_missing = loaded.is_none();
    let mut state = loaded.unwrap_or_else(|| empty_state(after, cut, now_ms, before_active));

    if new_coverage {
        state.activation_cut = cut;
        state.cursor = if include_existing { 0 } else { cut };
        state.catch_up_until = include_existing.then_some(cut);
        state.activation_history_unavailable = false;
        state.pending.clear();
        remember_recent(
            &mut state,
            json!({
                "status":"activation_updated",
                "include_existing":include_existing,
                "activation_cut":cut,
                "target_ref":after.publication.as_ref().map(|settings| settings.target_ref.as_str()),
                "recorded_at_ms":now_ms
            }),
        );
    } else if state_missing && before_active {
        // An existing active entry without a retained cursor has no proof that
        // prior acceptance facts were in scope. Start at this exact update.
        state.cursor = cut;
        state.activation_cut = cut;
        state.catch_up_until = None;
        state.activation_history_unavailable = true;
        state.pending.clear();
        remember_recent(
            &mut state,
            json!({
                "status":"capability_gap",
                "code":"activation_history_unavailable",
                "reason":"no publication activation watermark was retained; earlier acceptance facts were not replayed",
                "recorded_at_ms":now_ms
            }),
        );
    }
    state.configured_revision = after.revision;
    state.updated_at_ms = now_ms;
    save_state(tx, &key, &state)
}

/// Run one fair, globally bounded page of enabled Publication entries.
/// `fact_budget` applies independently to each selected entry.
pub(crate) fn reconcile(
    tx: &Transaction<'_>,
    launcher_config: &Config,
    entry_budget: usize,
    fact_budget: usize,
    now_ms: i64,
) -> Result<Value> {
    if now_ms < 0 {
        return Err(Error::invalid("publication reconciliation time is invalid"));
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

    let (entries, last_entry_key) = enabled_entry_page(tx, entry_budget)?;
    let mut results = Vec::with_capacity(entries.len());
    let mut total_processed = 0usize;
    for entry in &entries {
        let result = reconcile_entry(tx, launcher_config, entry, fact_budget, now_ms)?;
        total_processed = total_processed.saturating_add(
            result["processed"]
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
        "entry_budget":entry_budget,
        "fact_budget_per_entry":fact_budget,
        "cursor":last_entry_key
    }))
}

/// Bounded manager-facing state projection for automation explain/readback.
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

fn reconcile_entry(
    tx: &Transaction<'_>,
    launcher_config: &Config,
    entry: &AutomationEntry,
    budget: usize,
    now_ms: i64,
) -> Result<Value> {
    if !entry.enabled || !entry.steps.contains(&AutomationStep::Publication) {
        return Ok(json!({
            "automation_id":entry.automation_id,
            "processed":0,
            "status":"disabled_or_unselected"
        }));
    }
    if !entry.publication_ready() {
        return Ok(json!({
            "automation_id":entry.automation_id,
            "processed":0,
            "status":"capability_gap",
            "code":"publication_settings_required"
        }));
    }

    let key = state_key(entry)?;
    let high_water = acceptance_high_water(tx)?;
    let mut state = match load_state(tx, entry)? {
        Some(state) => state,
        None => {
            let mut state = empty_state(entry, high_water, now_ms, true);
            state.activation_history_unavailable = true;
            remember_recent(
                &mut state,
                json!({
                    "status":"capability_gap",
                    "code":"activation_history_unavailable",
                    "reason":"no activation cut was retained; prior acceptance facts were not replayed",
                    "recorded_at_ms":now_ms
                }),
            );
            save_state(tx, &key, &state)?;
            return Ok(state_projection_with_processed(&state, 0, high_water));
        }
    };
    if state.configured_revision != entry.revision {
        return Err(Error::new(
            "AUTOMATION_PUBLICATION_CURSOR_MISMATCH",
            "publication activation cursor does not match the current automation revision",
        ));
    }
    if budget == 0 {
        return Ok(state_projection_with_processed(&state, 0, high_water));
    }

    let mut processed = recheck_pending(
        tx,
        launcher_config,
        entry,
        &mut state,
        budget.min(MAX_PENDING_RECHECKS),
        now_ms,
    )?;
    let remaining_budget = budget.saturating_sub(processed);
    if remaining_budget == 0 {
        state.updated_at_ms = now_ms;
        save_state(tx, &key, &state)?;
        return Ok(state_projection_with_processed(
            &state, processed, high_water,
        ));
    }
    if state.pending.len() >= MAX_PENDING {
        remember_capacity_gap(&mut state, None, now_ms);
        state.updated_at_ms = now_ms;
        save_state(tx, &key, &state)?;
        return Ok(state_projection_with_processed(
            &state, processed, high_water,
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
            &state, processed, high_water,
        ));
    }

    let limit = i64::try_from(remaining_budget.saturating_add(1))
        .map_err(|_| Error::invalid("publication page limit exceeds platform range"))?;
    let mut statement = tx.prepare(
        "SELECT observation_id,source_event_key,operation_id,payload_json FROM observations \
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
                    payload_json: row.get(3)?,
                })
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);

    let has_more = events.len() > remaining_budget;
    let mut blocked_on_pending_capacity = false;
    for event in events.iter().take(remaining_budget) {
        let Some(accepted_operation_id) = event_identity(event)? else {
            remember_recent(
                &mut state,
                json!({
                    "observation_id":event.observation_id,
                    "status":"skipped",
                    "code":"acceptance_fact_not_applied",
                    "publication_started":false
                }),
            );
            state.cursor = event.observation_id;
            processed += 1;
            continue;
        };
        if state.pending.iter().any(|pending| {
            pending.observation_id == event.observation_id
                && pending.accepted_operation_id == accepted_operation_id
        }) {
            remember_recent(
                &mut state,
                event_projection(
                    event.observation_id,
                    &accepted_operation_id,
                    "pending",
                    "acceptance_already_waiting",
                    None,
                ),
            );
            state.cursor = event.observation_id;
            processed += 1;
            continue;
        }
        if state.pending.len() >= MAX_PENDING {
            remember_capacity_gap(&mut state, Some(event.observation_id), now_ms);
            blocked_on_pending_capacity = true;
            break;
        }
        let replay = event.observation_id <= state.activation_cut
            && state.catch_up_until == Some(state.activation_cut);
        let disposition = consume_event_isolated(
            tx,
            launcher_config,
            entry,
            AcceptanceReservation {
                event,
                accepted_operation_id: &accepted_operation_id,
                activation_cut: state.activation_cut,
                historical_replay_authorized: replay,
            },
            now_ms,
        )?;
        if let ReserveDisposition::Pending { .. } = disposition {
            state.pending.push(PendingAcceptance {
                observation_id: event.observation_id,
                accepted_operation_id: accepted_operation_id.clone(),
                historical_replay_authorized: replay,
                retries: 0,
                next_retry_at_ms: now_ms.saturating_add(BASE_RETRY_DELAY_MS),
            });
        }
        remember_recent(
            &mut state,
            disposition_projection(event.observation_id, &accepted_operation_id, disposition),
        );
        state.cursor = event.observation_id;
        processed += 1;
    }
    if !has_more && !blocked_on_pending_capacity {
        state.cursor = target;
        if state.catch_up_until.is_some_and(|cut| state.cursor >= cut) {
            state.catch_up_until = None;
        }
    }
    state.updated_at_ms = now_ms;
    save_state(tx, &key, &state)?;
    Ok(state_projection_with_processed(
        &state, processed, high_water,
    ))
}

fn consume_event_isolated(
    tx: &Transaction<'_>,
    launcher_config: &Config,
    entry: &AutomationEntry,
    reservation: AcceptanceReservation<'_>,
    now_ms: i64,
) -> Result<ReserveDisposition> {
    let AcceptanceReservation {
        event,
        accepted_operation_id,
        activation_cut,
        historical_replay_authorized,
    } = reservation;
    let context = match PublicationContext::from_acceptance_observation(
        tx,
        entry,
        event.observation_id,
        accepted_operation_id,
        activation_cut,
        historical_replay_authorized,
        launcher_config,
    ) {
        Ok(context) => context,
        Err(error) if pending_context_error(&error) => {
            return Ok(ReserveDisposition::Pending {
                code: "publication_prerequisite_waiting".to_owned(),
                reason: "current manager or Forge reservation prerequisite is temporarily unavailable",
            });
        }
        Err(error) if skip_context_error(&error) => {
            return Ok(ReserveDisposition::Skipped {
                code: error.code,
                reason: "acceptance is no longer eligible for this exact selected publication action",
            });
        }
        Err(error) => return Err(error),
    };
    let request_value = context.request_value()?;
    tx.execute_batch("SAVEPOINT automation_publication_reserve")?;
    match reserve_automatic_publication(tx, launcher_config, &context, &request_value, now_ms) {
        Ok(result) => {
            tx.execute_batch("RELEASE automation_publication_reserve")?;
            Ok(ReserveDisposition::Reserved(result))
        }
        Err(error) if pending_reservation_error(&error) => {
            tx.execute_batch(
                "ROLLBACK TO automation_publication_reserve; RELEASE automation_publication_reserve",
            )?;
            Ok(ReserveDisposition::Pending {
                code: "forge_reservation_waiting".to_owned(),
                reason: "the existing Forge worker reports a temporary repository or slot hold",
            })
        }
        Err(error) if skip_context_error(&error) => {
            tx.execute_batch(
                "ROLLBACK TO automation_publication_reserve; RELEASE automation_publication_reserve",
            )?;
            Ok(ReserveDisposition::Skipped {
                code: error.code,
                reason: "Forge no longer admits this exact accepted candidate and target",
            })
        }
        Err(error) => {
            tx.execute_batch(
                "ROLLBACK TO automation_publication_reserve; RELEASE automation_publication_reserve",
            )?;
            Err(error)
        }
    }
}

fn reserve_automatic_publication(
    tx: &Transaction<'_>,
    launcher_config: &Config,
    context: &PublicationContext,
    request_value: &Value,
    now_ms: i64,
) -> Result<Value> {
    let caller = context.technical_requester_id();
    let request_id = model::text(request_value, "client_request_id")?;
    let original_json = model::canonical(request_value)?;
    let existing: Option<(String, String, String, Option<String>, String)> = tx
        .query_row(
            "SELECT operation_id,method,original_request_json,result_json,state FROM operations \
             WHERE caller_id=?1 AND client_request_id=?2",
            params![caller, request_id],
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
    if let Some((operation_id, method, original, result_json, state)) = existing {
        if method != "forge.publish_ref" || original != original_json {
            return Err(Error::new(
                "PUBLICATION_SLOT_CONFLICT",
                "this automatic publication request ID already retains different inputs",
            ));
        }
        let retained = PublicationContext::from_committed_operation(tx, &operation_id)?;
        if retained.request_value()? != *request_value {
            return Err(Error::new(
                "PUBLICATION_SLOT_CONFLICT",
                "existing automatic request does not retain this exact accepted-candidate slot",
            ));
        }
        if !matches!(
            state.as_str(),
            "queued" | "sending" | "outcome_unknown" | "settled" | "rejected" | "cancelled"
        ) {
            return Err(Error::new(
                "AUTOMATION_OPERATION_CORRUPT",
                "existing automatic publication Operation has an unsupported state",
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
            "publication_started":matches!(state.as_str(),"queued"|"sending"|"outcome_unknown"),
            "receipt":result
        }));
    }

    let operation_id = model::new_id();
    let attribution = context.linkage_value();
    let effective = json!({"automation_on_behalf":attribution});
    tx.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,?3,'forge.publish_ref',?4,?5,'queued',?6,?6,?6)",
        params![
            operation_id,
            caller,
            request_id,
            original_json,
            model::canonical(&effective)?,
            now_ms
        ],
    )?;

    let reserved = super::forge::reserve_on_behalf(
        tx,
        context,
        request_value,
        &operation_id,
        launcher_config,
    )?;
    let coalesced = reserved["coalesced"] == true
        || reserved["status"] == "coalesced"
        || reserved["publication"] == "coalesced";
    let mut result = reserved;
    result["operation_id"] = json!(operation_id);
    result["coalesced"] = json!(coalesced);
    result["publication_started"] = json!(!coalesced);

    let raw_effective: String = tx.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1",
        [&operation_id],
        |row| row.get(0),
    )?;
    let mut effective: Value = serde_json::from_str(&raw_effective)?;
    effective["automation_on_behalf"] = context.linkage_value();
    effective["receipt"] = json!({"ok":true,"value":result});
    let state = if coalesced { "settled" } else { "queued" };
    tx.execute(
        "UPDATE operations SET state=?2,result_json=?3,settled_at_ms=?4,updated_at_ms=?5,effective_request_json=?6 \
         WHERE operation_id=?1 AND method='forge.publish_ref' AND state='queued'",
        params![
            operation_id,
            state,
            model::canonical(&result)?,
            coalesced.then_some(now_ms),
            now_ms,
            model::canonical(&effective)?
        ],
    )?;
    capacity::sync_operation(tx, &operation_id, now_ms)?;
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
         VALUES('controller',?1,?1,'forge.publish_ref',?2,?3)",
        params![operation_id, model::canonical(&result)?, now_ms],
    )?;
    save_operation_link(tx, context, &operation_id, now_ms)?;
    Ok(json!({
        "status":if coalesced {"coalesced"} else {"publication_reserved"},
        "operation_id":operation_id,
        "operation_state":state,
        "publication_started":!coalesced,
        "coalesced":coalesced,
        "receipt":result
    }))
}

fn save_operation_link(
    tx: &Transaction<'_>,
    context: &PublicationContext,
    operation_id: &str,
    now_ms: i64,
) -> Result<()> {
    let record = context.operation_link_value(operation_id, now_ms);
    let operation_key = config::operation_link_key(operation_id)?;
    if config::read_record(tx, &operation_key, "publication Operation link")?.is_some() {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "publication Operation link cannot replace a retained record",
        ));
    }
    config::write_record(tx, &operation_key, &record)?;
    let entry_key = config::entry_operation_key(
        context.effective_manager_id(),
        context.project_id(),
        context.automation_id(),
        operation_id,
    )?;
    if config::read_record(tx, &entry_key, "publication entry Operation link")?.is_some() {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "publication entry Operation link cannot replace a retained record",
        ));
    }
    config::write_record(tx, &entry_key, &record)
}

fn recheck_pending(
    tx: &Transaction<'_>,
    launcher_config: &Config,
    entry: &AutomationEntry,
    state: &mut PublicationState,
    budget: usize,
    now_ms: i64,
) -> Result<usize> {
    let mut processed = 0usize;
    while processed < budget {
        let Some(index) = state
            .pending
            .iter()
            .position(|pending| pending.next_retry_at_ms <= now_ms)
        else {
            break;
        };
        let mut pending = state.pending.remove(index);
        let event = load_event(tx, pending.observation_id, &pending.accepted_operation_id)?;
        let replay = pending.historical_replay_authorized;
        if (event.observation_id <= state.activation_cut) != replay {
            return Err(Error::new(
                "AUTOMATION_PUBLICATION_STATE_CORRUPT",
                "pending acceptance replay authority no longer matches its activation cut",
            ));
        }
        let disposition = consume_event_isolated(
            tx,
            launcher_config,
            entry,
            AcceptanceReservation {
                event: &event,
                accepted_operation_id: &pending.accepted_operation_id,
                activation_cut: state.activation_cut,
                historical_replay_authorized: replay,
            },
            now_ms,
        )?;
        processed += 1;
        if let ReserveDisposition::Pending { .. } = disposition {
            pending.retries = pending.retries.saturating_add(1);
            pending.next_retry_at_ms = now_ms.saturating_add(retry_delay(pending.retries));
            state.pending.push(pending.clone());
        }
        remember_recent(
            state,
            disposition_projection(
                pending.observation_id,
                &pending.accepted_operation_id,
                disposition,
            ),
        );
    }
    Ok(processed)
}

fn load_event(
    db: &Connection,
    observation_id: i64,
    accepted_operation_id: &str,
) -> Result<AcceptanceEvent> {
    db.query_row(
        "SELECT observation_id,source_event_key,operation_id,payload_json FROM observations \
         WHERE observation_id=?1 AND source_stream_id=?2 AND kind='task.acceptance' \
           AND source_event_key=?3 AND operation_id=?3",
        params![
            observation_id,
            ACCEPTANCE_STREAM,
            format!("accept:{accepted_operation_id}")
        ],
        |row| {
            Ok(AcceptanceEvent {
                observation_id: row.get(0)?,
                source_event_key: row.get(1)?,
                operation_id: row.get(2)?,
                payload_json: row.get(3)?,
            })
        },
    )
    .optional()?
    .ok_or_else(|| {
        Error::new(
            "AUTOMATION_FACT_MISSING",
            "pending acceptance Observation no longer exists at its exact identity",
        )
    })
}

fn event_identity(event: &AcceptanceEvent) -> Result<Option<String>> {
    let payload: Value = serde_json::from_str(&event.payload_json).map_err(|_| {
        Error::new(
            "AUTOMATION_FACT_CORRUPT",
            "acceptance Observation payload is invalid",
        )
    })?;
    if payload["outcome"] != "applied" || payload["task_accepted"] != true {
        return Ok(None);
    }
    let operation_id = event.operation_id.as_deref().ok_or_else(|| {
        Error::new(
            "AUTOMATION_FACT_CORRUPT",
            "applied acceptance Observation has no Operation identity",
        )
    })?;
    if event.source_event_key.as_deref() != Some(format!("accept:{operation_id}").as_str())
        || payload["operation_id"] != operation_id
        || payload["acceptance_operation_id"] != operation_id
    {
        return Err(Error::new(
            "AUTOMATION_FACT_CORRUPT",
            "acceptance Observation key and payload identities differ",
        ));
    }
    Ok(Some(operation_id.to_owned()))
}

fn disposition_projection(
    observation_id: i64,
    accepted_operation_id: &str,
    disposition: ReserveDisposition,
) -> Value {
    match disposition {
        ReserveDisposition::Reserved(value) => json!({
            "observation_id":observation_id,
            "accepted_operation_id":accepted_operation_id,
            "status":value["status"],
            "code":value["code"],
            "operation_id":value["operation_id"],
            "operation_state":value["operation_state"],
            "publication_started":value["publication_started"] == true,
            "coalesced":value["coalesced"] == true
        }),
        ReserveDisposition::Pending { code, reason } => event_projection(
            observation_id,
            accepted_operation_id,
            "pending",
            &code,
            Some(reason),
        ),
        ReserveDisposition::Skipped { code, reason } => event_projection(
            observation_id,
            accepted_operation_id,
            "skipped",
            &code,
            Some(reason),
        ),
    }
}

fn event_projection(
    observation_id: i64,
    accepted_operation_id: &str,
    status: &str,
    code: &str,
    reason: Option<&str>,
) -> Value {
    json!({
        "observation_id":observation_id,
        "accepted_operation_id":accepted_operation_id,
        "status":status,
        "code":code,
        "reason":reason,
        "publication_started":false
    })
}

fn pending_context_error(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "AUTOMATION_CURRENT_GM_REQUIRED" | "FORGE_DISABLED"
    )
}

fn skip_context_error(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "AUTOMATION_ACTION_CHANGED"
            | "AUTOMATION_ACTION_UNAVAILABLE"
            | "AUTOMATION_FACT_MISSING"
            | "AUTOMATION_FACT_NOT_APPLIED"
            | "FORGE_ACCEPTANCE_STALE"
            | "FORGE_SUBMISSION_MISMATCH"
            | "FORGE_CANDIDATE_MISMATCH"
            | "FORGE_POLICY_MISMATCH"
            | "AUTOMATION_PUBLICATION_SETTINGS_REQUIRED"
            | "AUTOMATION_PUBLICATION_ACTIVATION_MISMATCH"
            | "FORGE_PROJECT_UNCONFIGURED"
            | "FORBIDDEN"
    )
}

fn pending_reservation_error(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "FORGE_PROCESS_TREE_UNCONFIRMED" | "FORGE_PUBLICATION_SLOT_BUSY"
    )
}

fn retry_delay(retries: u32) -> i64 {
    let shift = retries.saturating_sub(1).min(16);
    BASE_RETRY_DELAY_MS
        .saturating_mul(1_i64 << shift)
        .min(MAX_RETRY_DELAY_MS)
}

fn remember_capacity_gap(state: &mut PublicationState, observation_id: Option<i64>, now_ms: i64) {
    let already_recorded = state.recent.iter().any(|item| {
        item["code"] == "pending_publication_capacity"
            && item["observation_id"] == observation_id.map_or(Value::Null, |id| json!(id))
    });
    if !already_recorded {
        remember_recent(
            state,
            json!({
                "observation_id":observation_id,
                "status":"pending",
                "code":"pending_publication_capacity",
                "reason":"the bounded unresolved acceptance queue is full; the source cursor remains before the next unqueued fact",
                "publication_started":false,
                "recorded_at_ms":now_ms
            }),
        );
    }
}

fn enabled_entry_page(
    db: &Connection,
    limit: usize,
) -> Result<(Vec<AutomationEntry>, Option<String>)> {
    if limit == 0 {
        return Ok((Vec::new(), None));
    }
    let prefix = "automation:v1:entry:";
    let pattern = "automation:v1:entry:%";
    let cursor_value = config::read_record(db, GLOBAL_CURSOR_KEY, "publication global cursor")?;
    let cursor = cursor_value
        .map(|value| {
            serde_json::from_value::<GlobalCursor>(value).map_err(|_| {
                Error::new(
                    "AUTOMATION_PUBLICATION_CURSOR_CORRUPT",
                    "publication global cursor fields are invalid",
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
            "AUTOMATION_PUBLICATION_CURSOR_CORRUPT",
            "publication global cursor identity is invalid",
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
        return Ok((Vec::new(), None));
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
        if config::entry_key(
            &entry.owner_manager_id,
            &entry.project_id,
            &entry.automation_id,
        )? != key
        {
            return Err(Error::new(
                "AUTOMATION_RECORD_CORRUPT",
                "automation entry identity does not match its metadata key",
            ));
        }
        if entry.enabled && entry.steps.contains(&AutomationStep::Publication) {
            entries.push(entry);
        }
    }
    Ok((entries, last_key))
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
    let mut statement = db.prepare(
        "SELECT key FROM meta WHERE key LIKE ?1 AND key>?2 \
         AND json_extract(value_json,'$.record.enabled')=1 \
         AND EXISTS(SELECT 1 FROM json_each(value_json,'$.record.steps') AS step WHERE step.value='publication') \
         ORDER BY key LIMIT ?3",
    )?;
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
        "SELECT key FROM meta WHERE key LIKE ?1 AND key>?2 AND key<?3 \
         AND json_extract(value_json,'$.record.enabled')=1 \
         AND EXISTS(SELECT 1 FROM json_each(value_json,'$.record.steps') AS step WHERE step.value='publication') \
         ORDER BY key LIMIT ?4",
    )?;
    Ok(statement
        .query_map(params![pattern, prefix, before, limit as i64], |row| {
            row.get(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn load_state(db: &Connection, entry: &AutomationEntry) -> Result<Option<PublicationState>> {
    let Some(value) = config::read_record(db, &state_key(entry)?, "publication state")? else {
        return Ok(None);
    };
    let state: PublicationState = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "AUTOMATION_PUBLICATION_STATE_CORRUPT",
            "publication state fields are invalid",
        )
    })?;
    validate_state(&state, entry)?;
    Ok(Some(state))
}

fn validate_state(state: &PublicationState, entry: &AutomationEntry) -> Result<()> {
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
        || state.pending.len() > MAX_PENDING
        || state.pending.iter().any(|pending| {
            pending.observation_id <= 0
                || pending.accepted_operation_id.is_empty()
                || pending.accepted_operation_id.len() > 128
                || pending.historical_replay_authorized
                    != (pending.observation_id <= state.activation_cut)
                || pending.retries > 1_000_000
                || pending.next_retry_at_ms < 0
        })
        || state.recent.len() > MAX_RECENT
        || state.updated_at_ms < 0
    {
        return Err(Error::new(
            "AUTOMATION_PUBLICATION_STATE_CORRUPT",
            "publication state identity or bounds are invalid",
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
) -> PublicationState {
    PublicationState {
        schema_version: STATE_SCHEMA_VERSION,
        owner_manager_id: entry.owner_manager_id.clone(),
        project_id: entry.project_id.clone(),
        automation_id: entry.automation_id.clone(),
        configured_revision: entry.revision,
        cursor: cut,
        activation_cut: cut,
        catch_up_until: None,
        activation_history_unavailable: history_unavailable,
        pending: Vec::new(),
        recent: Vec::new(),
        updated_at_ms: now_ms,
    }
}

fn entry_publication_active(entry: &AutomationEntry) -> bool {
    entry.publication_ready()
}

fn acceptance_high_water(db: &Connection) -> Result<i64> {
    Ok(db.query_row(
        "SELECT COALESCE(MAX(observation_id),0) FROM observations WHERE source_stream_id=?1 AND kind='task.acceptance'",
        [ACCEPTANCE_STREAM],
        |row| row.get(0),
    )?)
}

fn save_state(db: &Connection, key: &str, state: &PublicationState) -> Result<()> {
    config::write_record(db, key, &serde_json::to_value(state)?)
}

fn remember_recent(state: &mut PublicationState, value: Value) {
    state.recent.push(value);
    if state.recent.len() > MAX_RECENT {
        let excess = state.recent.len() - MAX_RECENT;
        state.recent.drain(0..excess);
    }
}

fn state_projection(state: &PublicationState) -> Value {
    let incomplete = state.activation_history_unavailable
        || state.catch_up_until.is_some()
        || !state.pending.is_empty();
    json!({
        "automation_id":state.automation_id,
        "configured_revision":state.configured_revision,
        "cursor":state.cursor,
        "activation_cut":state.activation_cut,
        "catch_up_until":state.catch_up_until,
        "coverage":if incomplete {"partial"} else {"complete"},
        "status":if state.catch_up_until.is_some() {"catching_up"} else if !state.pending.is_empty() {"waiting_on_outcome"} else if state.activation_history_unavailable {"partial"} else {"ready"},
        "activation_history_unavailable":state.activation_history_unavailable,
        "pending":state.pending,
        "recent":state.recent,
        "updated_at_ms":state.updated_at_ms
    })
}

fn state_projection_with_processed(
    state: &PublicationState,
    processed: usize,
    high_water: i64,
) -> Value {
    let mut value = state_projection(state);
    value["processed"] = json!(processed);
    value["high_water"] = json!(high_water);
    value
}
