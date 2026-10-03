//! Bounded, per-entry recovery for canonical committed review results.
//!
//! The review submission transaction is independent of this consumer. A
//! failure here leaves the review result committed and leaves the cursor at
//! its last successful observation, so the entry can retry on a later pass.

use super::automation_disposition;
use crate::{
    automation::{
        actions::AutomationStep,
        config::{self, AutomationEntry},
    },
    error::{Error, Result},
    model,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const STATE_SCHEMA_VERSION: u32 = 1;
const GLOBAL_CURSOR_KEY: &str = "automation:v1:review-disposition:global-cursor";
const STATE_PREFIX: &str = "automation:v1:review-disposition:state:";
const REVIEW_STREAM: &str = "controller:review";
const MAX_ENTRIES_PER_PASS: usize = 16;
const MAX_FACTS_PER_ENTRY: usize = 16;
const MAX_RECENT: usize = 32;
const MAX_PENDING_RESULTS: usize = 64;
const MAX_PENDING_RECHECKS: usize = 4;
const MAX_PENDING_RETRIES: u32 = 32;
const BASE_RETRY_DELAY_MS: i64 = 1_000;
const MAX_RETRY_DELAY_MS: i64 = 60_000;
const MAX_CURSOR_KEY_BYTES: usize = 512;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GlobalCursor {
    schema_version: u32,
    last_entry_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DispositionState {
    schema_version: u32,
    owner_manager_id: String,
    project_id: String,
    automation_id: String,
    configured_revision: i64,
    cursor: i64,
    activation_cut: i64,
    catch_up_until: Option<i64>,
    activation_history_unavailable: bool,
    pending: Vec<PendingResult>,
    recent: Vec<Value>,
    updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingResult {
    observation_id: i64,
    review_assignment_id: String,
    review_result_operation_id: String,
    retries: u32,
    next_retry_at_ms: i64,
}

struct ReviewResultEvent {
    observation_id: i64,
    source_event_key: Option<String>,
    operation_id: Option<String>,
    payload_json: String,
}

/// Update the durable result-consumer activation cut atomically with a
/// revisioned automation entry update.
pub(super) fn configure_activation(
    tx: &Transaction<'_>,
    before: Option<&AutomationEntry>,
    after: &AutomationEntry,
    include_existing: bool,
    cut: i64,
    now_ms: i64,
) -> Result<()> {
    if cut < 0 || now_ms < 0 {
        return Err(Error::invalid(
            "review disposition activation cut and time must be non-negative",
        ));
    }
    let key = state_key(after)?;
    let before_active = before.is_some_and(entry_has_result_action);
    let after_active = entry_has_result_action(after);
    let added_result_step =
        result_steps(after).any(|step| before.is_none_or(|prior| !prior.steps.contains(&step)));
    let new_coverage = after_active && (!before_active || added_result_step);

    let loaded = load_state(tx, after)?;
    let state_missing = loaded.is_none();
    let mut state = loaded.unwrap_or_else(|| empty_state(after, cut, now_ms, before_active));

    if new_coverage {
        state.activation_cut = cut;
        state.cursor = if include_existing { 0 } else { cut };
        state.catch_up_until = include_existing.then_some(cut);
        state.activation_history_unavailable = false;
        remember_recent(
            &mut state,
            json!({
                "status":"activation_updated",
                "include_existing":include_existing,
                "activation_cut":cut,
                "selected_steps":result_steps(after).map(AutomationStep::as_str).collect::<Vec<_>>(),
                "recorded_at_ms":now_ms
            }),
        );
    } else if state_missing && before_active {
        // A first cursor for a previously active entry has no historical
        // activation receipt. Start at this exact update; never infer
        // permission to replay earlier review results.
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
                "reason":"the result action has no retained activation cut; prior review results were not replayed",
                "recorded_at_ms":now_ms
            }),
        );
    }

    state.configured_revision = after.revision;
    state.updated_at_ms = now_ms;
    save_state(tx, &key, &state)
}

/// Run one fair, globally bounded page of enabled result-disposition entries.
/// `fact_budget` applies independently to each selected entry.
pub(super) fn reconcile(
    tx: &Transaction<'_>,
    entry_budget: usize,
    fact_budget: usize,
    now_ms: i64,
) -> Result<Value> {
    if now_ms < 0 {
        return Err(Error::invalid(
            "review disposition time must be non-negative",
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

    let (entries, last_entry_key) = enabled_entry_page(tx, entry_budget)?;
    let mut results = Vec::with_capacity(entries.len());
    let mut total_processed = 0usize;
    for entry in &entries {
        let result = reconcile_entry(tx, entry, fact_budget, now_ms)?;
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
            &json!({
                "schema_version":1,
                "last_entry_key":last_entry_key
            }),
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

/// Return the bounded manager-facing cursor projection for `automation.explain`.
pub(super) fn disposition_state(db: &Connection, entry: &AutomationEntry) -> Result<Value> {
    match load_state(db, entry)? {
        Some(state) => Ok(state_projection(&state)),
        None => Ok(json!({
            "automation_id":entry.automation_id,
            "status":"uninitialized",
            "coverage":"partial",
            "gaps":[{
                "code":"activation_history_unavailable",
                "reason":"no per-entry result activation cursor has been retained"
            }]
        })),
    }
}

fn reconcile_entry(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    budget: usize,
    now_ms: i64,
) -> Result<Value> {
    if !entry_has_result_action(entry) {
        return Ok(json!({
            "automation_id":entry.automation_id,
            "processed":0,
            "status":"disabled_or_unselected"
        }));
    }

    let key = state_key(entry)?;
    let high_water = observation_high_water(tx)?;
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
                    "reason":"no activation cut was retained; prior review results were not replayed",
                    "recorded_at_ms":now_ms
                }),
            );
            save_state(tx, &key, &state)?;
            return Ok(state_projection_with_processed(&state, 0, high_water));
        }
    };
    if state.configured_revision != entry.revision {
        return Err(Error::new(
            "AUTOMATION_REVIEW_DISPOSITION_CURSOR_MISMATCH",
            "result activation cursor does not match the current automation revision",
        ));
    }
    if budget == 0 {
        return Ok(state_projection_with_processed(&state, 0, high_water));
    }

    let mut processed = recheck_pending(
        tx,
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
    if state.pending.len() >= MAX_PENDING_RESULTS {
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
            state.cursor = target;
            state.catch_up_until = None;
            state.updated_at_ms = now_ms;
            save_state(tx, &key, &state)?;
        }
        return Ok(state_projection_with_processed(
            &state, processed, high_water,
        ));
    }

    let limit = i64::try_from(remaining_budget.saturating_add(1))
        .map_err(|_| Error::invalid("review result page limit exceeds platform range"))?;
    let mut statement = tx.prepare(
        "SELECT observation_id,source_event_key,operation_id,payload_json FROM observations \
         WHERE source_stream_id=?1 AND kind='review.result' AND observation_id>?2 \
           AND observation_id<=?3 ORDER BY observation_id LIMIT ?4",
    )?;
    let events = statement
        .query_map(params![REVIEW_STREAM, state.cursor, target, limit], |row| {
            Ok(ReviewResultEvent {
                observation_id: row.get(0)?,
                source_event_key: row.get(1)?,
                operation_id: row.get(2)?,
                payload_json: row.get(3)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);

    let has_more = events.len() > remaining_budget;
    let mut blocked_on_pending_capacity = false;
    for event in events.iter().take(remaining_budget) {
        let (assignment_id, result_operation_id) = event_identity(event)?;
        if state.pending.iter().any(|pending| {
            pending.observation_id == event.observation_id
                && pending.review_assignment_id == assignment_id
                && pending.review_result_operation_id == result_operation_id
        }) {
            remember_recent(
                &mut state,
                json!({
                    "observation_id":event.observation_id,
                    "review_assignment_id":assignment_id,
                    "review_result_operation_id":result_operation_id,
                    "status":"pending",
                    "code":"pending_result_already_queued",
                    "disposition_applied":false
                }),
            );
            state.cursor = event.observation_id;
            processed += 1;
            continue;
        }
        if state.pending.len() >= MAX_PENDING_RESULTS {
            remember_capacity_gap(&mut state, Some(event.observation_id), now_ms);
            blocked_on_pending_capacity = true;
            break;
        }
        let result = consume_isolated(tx, entry, &assignment_id, &result_operation_id, now_ms)?;
        if is_unresolved(&result) {
            state.pending.push(PendingResult {
                observation_id: event.observation_id,
                review_assignment_id: assignment_id.clone(),
                review_result_operation_id: result_operation_id.clone(),
                retries: 0,
                next_retry_at_ms: now_ms.saturating_add(BASE_RETRY_DELAY_MS),
            });
        }
        remember_recent(
            &mut state,
            summarize_event(event, &assignment_id, &result_operation_id, result),
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

fn remember_capacity_gap(state: &mut DispositionState, observation_id: Option<i64>, now_ms: i64) {
    let already_recorded = state.recent.iter().any(|item| {
        item["code"] == "pending_result_capacity"
            && item["observation_id"] == observation_id.map_or(Value::Null, |id| json!(id))
    });
    if !already_recorded {
        remember_recent(
            state,
            json!({
                "observation_id":observation_id,
                "status":"pending",
                "code":"pending_result_capacity",
                "reason":"the bounded unresolved-result queue is full; source cursor remains before the next unqueued event",
                "disposition_applied":false,
                "recorded_at_ms":now_ms
            }),
        );
    }
}

fn consume_isolated(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    assignment_id: &str,
    result_operation_id: &str,
    now_ms: i64,
) -> Result<Value> {
    tx.execute_batch("SAVEPOINT review_disposition_consumer")?;
    match automation_disposition::consume_review_result_for_entry(
        tx,
        entry,
        assignment_id,
        result_operation_id,
        now_ms,
    ) {
        Ok(result) => {
            tx.execute_batch("RELEASE SAVEPOINT review_disposition_consumer")?;
            Ok(result)
        }
        Err(error) => {
            tx.execute_batch(
                "ROLLBACK TO SAVEPOINT review_disposition_consumer; \
                 RELEASE SAVEPOINT review_disposition_consumer",
            )?;
            Err(error)
        }
    }
}

fn recheck_pending(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    state: &mut DispositionState,
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
        let event = ReviewResultEvent {
            observation_id: pending.observation_id,
            source_event_key: Some(format!("result:{}", pending.review_assignment_id)),
            operation_id: Some(pending.review_result_operation_id.clone()),
            payload_json: load_result_payload(
                tx,
                pending.observation_id,
                &pending.review_assignment_id,
                &pending.review_result_operation_id,
            )?,
        };
        event_identity(&event)?;
        let result = consume_isolated(
            tx,
            entry,
            &pending.review_assignment_id,
            &pending.review_result_operation_id,
            now_ms,
        )?;
        processed += 1;
        if is_unresolved(&result) {
            pending.retries = pending.retries.saturating_add(1).min(MAX_PENDING_RETRIES);
            pending.next_retry_at_ms = now_ms.saturating_add(retry_delay(pending.retries));
            state.pending.push(pending.clone());
        }
        remember_recent(
            state,
            summarize_event(
                &event,
                &pending.review_assignment_id,
                &pending.review_result_operation_id,
                result,
            ),
        );
    }
    Ok(processed)
}

fn load_result_payload(
    db: &Connection,
    observation_id: i64,
    assignment_id: &str,
    result_operation_id: &str,
) -> Result<String> {
    let source_event_key = format!("result:{assignment_id}");
    db.query_row(
        "SELECT payload_json FROM observations WHERE source_stream_id=?1 \
         AND source_event_key=?2 AND operation_id=?3 AND observation_id=?4 \
         AND kind='review.result'",
        params![
            REVIEW_STREAM,
            source_event_key,
            result_operation_id,
            observation_id
        ],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| {
        Error::new(
            "AUTOMATION_REVIEW_DISPOSITION_PENDING_EVENT_MISSING",
            "an unresolved review-disposition cause no longer has its exact canonical result event",
        )
    })
}

fn is_unresolved(result: &Value) -> bool {
    matches!(
        result["feedback"]["status"].as_str(),
        Some("pending" | "queued" | "sending" | "native_accepted" | "outcome_unknown")
    )
}

fn retry_delay(retries: u32) -> i64 {
    let exponent = retries.min(6);
    BASE_RETRY_DELAY_MS
        .saturating_mul(1_i64 << exponent)
        .min(MAX_RETRY_DELAY_MS)
}

fn event_identity(event: &ReviewResultEvent) -> Result<(String, String)> {
    let key = event.source_event_key.as_deref().ok_or_else(|| {
        Error::new(
            "REVIEW_DISPOSITION_EVENT_DAMAGED",
            "canonical review result has no event key",
        )
    })?;
    let assignment_id = key
        .strip_prefix("result:")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Error::new(
                "REVIEW_DISPOSITION_EVENT_DAMAGED",
                "canonical review result has an invalid event key",
            )
        })?;
    validate_event_id(assignment_id)?;
    let result_operation_id = event.operation_id.as_deref().ok_or_else(|| {
        Error::new(
            "REVIEW_DISPOSITION_EVENT_DAMAGED",
            "canonical review result has no Operation identity",
        )
    })?;
    validate_event_id(result_operation_id)?;
    let record: Value = serde_json::from_str(&event.payload_json).map_err(|_| {
        Error::new(
            "REVIEW_DISPOSITION_EVENT_DAMAGED",
            "canonical review result payload is invalid JSON",
        )
    })?;
    if record["schema_version"] != 1
        || record["review_assignment_id"] != assignment_id
        || record["operation_id"] != result_operation_id
        || record["result"]["review_assignment_id"] != assignment_id
        || record["result"]["operation_id"] != result_operation_id
    {
        return Err(Error::new(
            "REVIEW_DISPOSITION_EVENT_DAMAGED",
            "canonical review result event identity differs from its retained payload",
        ));
    }
    Ok((assignment_id.to_owned(), result_operation_id.to_owned()))
}

fn summarize_event(
    event: &ReviewResultEvent,
    assignment_id: &str,
    result_operation_id: &str,
    result: Value,
) -> Value {
    json!({
        "observation_id":event.observation_id,
        "review_assignment_id":assignment_id,
        "review_result_operation_id":result_operation_id,
        "status":result.get("status").cloned().unwrap_or(Value::Null),
        "code":result.get("code").cloned().unwrap_or(Value::Null),
        "reason":result.get("reason").cloned().unwrap_or(Value::Null),
        "operation_id":result.get("operation_id").cloned().unwrap_or(Value::Null),
        "disposition_applied":result.get("disposition_applied").cloned().unwrap_or(json!(false)),
        "coalesced":result.get("status").is_some_and(|status| status == "coalesced")
    })
}

fn enabled_entry_page(
    db: &Connection,
    limit: usize,
) -> Result<(Vec<AutomationEntry>, Option<String>)> {
    let prefix = "automation:v1:entry:";
    let pattern = format!("{prefix}%");
    let cursor = config::read_record(db, GLOBAL_CURSOR_KEY, "review disposition global cursor")?
        .map(|value| {
            serde_json::from_value::<GlobalCursor>(value).map_err(|_| {
                Error::new(
                    "AUTOMATION_REVIEW_DISPOSITION_CURSOR_CORRUPT",
                    "global review-disposition cursor fields are invalid",
                )
            })
        })
        .transpose()?;
    if cursor.as_ref().is_some_and(|cursor| {
        cursor.schema_version != 1
            || cursor.last_entry_key.len() > MAX_CURSOR_KEY_BYTES
            || !cursor.last_entry_key.starts_with(prefix)
            || cursor.last_entry_key.chars().any(char::is_control)
    }) {
        return Err(Error::new(
            "AUTOMATION_REVIEW_DISPOSITION_CURSOR_CORRUPT",
            "global review-disposition cursor identity or bounds are invalid",
        ));
    }
    let after = cursor.map_or_else(|| prefix.to_owned(), |cursor| cursor.last_entry_key);
    let mut keys = select_entry_keys(db, &pattern, &after, limit)?;
    if keys.len() < limit {
        let wrapped = select_entry_keys_before(db, &pattern, prefix, &after, limit - keys.len())?;
        keys.extend(wrapped);
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
        if entry_has_result_action(&entry) {
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
         AND EXISTS(SELECT 1 FROM json_each(value_json,'$.record.steps') AS step \
                    WHERE step.value IN ('review_disposition','acceptance')) \
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
         AND EXISTS(SELECT 1 FROM json_each(value_json,'$.record.steps') AS step \
                    WHERE step.value IN ('review_disposition','acceptance')) \
         ORDER BY key LIMIT ?4",
    )?;
    Ok(statement
        .query_map(params![pattern, prefix, before, limit as i64], |row| {
            row.get(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn load_state(db: &Connection, entry: &AutomationEntry) -> Result<Option<DispositionState>> {
    let Some(value) = config::read_record(db, &state_key(entry)?, "review-disposition state")?
    else {
        return Ok(None);
    };
    let state: DispositionState = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "AUTOMATION_REVIEW_DISPOSITION_STATE_CORRUPT",
            "review-disposition state fields are invalid",
        )
    })?;
    validate_state(&state, entry)?;
    Ok(Some(state))
}

fn validate_state(state: &DispositionState, entry: &AutomationEntry) -> Result<()> {
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
        || state.pending.len() > MAX_PENDING_RESULTS
        || state.pending.iter().any(|pending| {
            pending.observation_id <= 0
                || pending.review_assignment_id.is_empty()
                || pending.review_result_operation_id.is_empty()
                || pending.review_assignment_id.len() > 128
                || pending.review_result_operation_id.len() > 128
                || pending.retries > MAX_PENDING_RETRIES
                || pending.next_retry_at_ms < 0
        })
        || state.recent.len() > MAX_RECENT
        || state.updated_at_ms < 0
    {
        return Err(Error::new(
            "AUTOMATION_REVIEW_DISPOSITION_STATE_CORRUPT",
            "review-disposition state identity, cursor, or bounds are invalid",
        ));
    }
    Ok(())
}

fn save_state(tx: &Transaction<'_>, key: &str, state: &DispositionState) -> Result<()> {
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

fn empty_state(
    entry: &AutomationEntry,
    cut: i64,
    now_ms: i64,
    history_unavailable: bool,
) -> DispositionState {
    DispositionState {
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

fn entry_has_result_action(entry: &AutomationEntry) -> bool {
    entry.enabled && result_steps(entry).next().is_some()
}

fn result_steps(entry: &AutomationEntry) -> impl Iterator<Item = AutomationStep> + '_ {
    [
        AutomationStep::ReviewDisposition,
        AutomationStep::Acceptance,
    ]
    .into_iter()
    .filter(|step| entry.steps.contains(step))
}

fn observation_high_water(db: &Connection) -> Result<i64> {
    Ok(db.query_row(
        "SELECT COALESCE(MAX(observation_id),0) FROM observations",
        [],
        |row| row.get(0),
    )?)
}

fn remember_recent(state: &mut DispositionState, value: Value) {
    state.recent.push(value);
    if state.recent.len() > MAX_RECENT {
        let excess = state.recent.len() - MAX_RECENT;
        state.recent.drain(0..excess);
    }
}

fn state_projection(state: &DispositionState) -> Value {
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
    state: &DispositionState,
    processed: usize,
    high_water: i64,
) -> Value {
    let mut projection = state_projection(state);
    projection["processed"] = json!(processed);
    projection["high_water"] = json!(high_water);
    projection
}

fn validate_event_id(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(Error::new(
            "REVIEW_DISPOSITION_EVENT_DAMAGED",
            "canonical review result identity is invalid",
        ));
    }
    Ok(())
}
