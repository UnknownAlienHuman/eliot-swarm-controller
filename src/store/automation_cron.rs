//! Durable manager-owned calendar CheckRun admission.
//!
//! The existing scheduler owns the timer. This module contributes a bounded
//! Store reconciliation pass and transaction-coupled occurrence ledger; it
//! does not create another worker, timer, or authority principal.

use super::{Store, meta, set_meta};
use crate::{
    automation::{
        actions::AutomationStep,
        authorization::CronExecutionContext,
        config::{self, AutomationEntry},
    },
    config::Config,
    error::{Error, Result},
    model,
    scheduler::{self, CronSettings},
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const SCHEMA_VERSION: u32 = 1;
const STATE_PREFIX: &str = "automation:v1:cron:state:";
const ACTIVE_PREFIX: &str = "automation:v1:cron:active:";
const DUE_PREFIX: &str = "automation:v1:cron:due:";
const HELD_PREFIX: &str = "automation:v1:cron:held:";
const HELD_CURSOR_KEY: &str = "automation:v1:cron:held_cursor";
const OCCURRENCE_PREFIX: &str = "automation:v1:cron:occurrence:";
const RETRY_DELAY_MS: i64 = 60_000;
const MAX_RECONCILE_BATCH: usize = 32;
const ACTIVE_OPERATION_STATES: &[&str] =
    &["queued", "sending", "native_accepted", "outcome_unknown"];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActiveGeneration {
    schema_version: u32,
    generation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GenerationState {
    schema_version: u32,
    logical_id: String,
    origin_manager_id: String,
    project_id: String,
    automation_id: String,
    generation: String,
    activation_cut_ms: i64,
    include_existing: bool,
    #[serde(default)]
    last_considered_due_ms: Option<i64>,
    #[serde(default)]
    last_occurrence_id: Option<String>,
    #[serde(default)]
    last_operation_id: Option<String>,
    #[serde(default)]
    last_operation_state: Option<String>,
    #[serde(default)]
    last_receipt: Option<Value>,
    #[serde(default)]
    indexed_due_at_ms: Option<i64>,
    updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DueIndex {
    schema_version: u32,
    logical_id: String,
    origin_manager_id: String,
    current_owner_manager_id: String,
    project_id: String,
    automation_id: String,
    generation: String,
    wake_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HeldIndex {
    schema_version: u32,
    logical_id: String,
    origin_manager_id: String,
    current_owner_manager_id: String,
    project_id: String,
    automation_id: String,
    generation: String,
    first_due_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OccurrenceRecord {
    schema_version: u32,
    logical_id: String,
    origin_manager_id: String,
    project_id: String,
    automation_id: String,
    generation: String,
    occurrence_id: String,
    due_at_ms: i64,
    operation_id: String,
    operation_state: String,
    disposition: String,
    receipt: Value,
    recorded_at_ms: i64,
}

#[derive(Debug, Clone)]
struct Candidate {
    index_key: String,
    index: DueIndex,
    entry: AutomationEntry,
    context: CronExecutionContext,
    due_at_ms: i64,
    occurrence_id: String,
}

#[derive(Debug, Default)]
struct PreparedBatch {
    candidates: Vec<Candidate>,
    next_due_at_ms: Option<i64>,
    due_remaining: bool,
}

fn logical_id(db: &Connection, entry: &AutomationEntry) -> Result<(String, String)> {
    let lineage = config::transfer_lineage(
        db,
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?;
    let origin = lineage
        .last()
        .map(|edge| edge.former_owner_manager_id.as_str())
        .unwrap_or(&entry.owner_manager_id);
    let identity = json!({
        "cron_logical_entry_schema_version": 1,
        "origin_manager_id": origin,
        "project_id": entry.project_id,
        "automation_id": entry.automation_id,
    });
    Ok((
        origin.to_owned(),
        model::digest(model::canonical(&identity)?.as_bytes()),
    ))
}

fn state_key(logical_id: &str, generation: &str) -> String {
    format!("{STATE_PREFIX}{logical_id}:{generation}")
}

fn active_key(logical_id: &str) -> String {
    format!("{ACTIVE_PREFIX}{logical_id}")
}

fn occurrence_key(logical_id: &str, generation: &str, occurrence_id: &str) -> String {
    format!("{OCCURRENCE_PREFIX}{logical_id}:{generation}:{occurrence_id}")
}

fn held_key(logical_id: &str) -> String {
    format!("{HELD_PREFIX}{logical_id}")
}

fn due_key(wake_at_ms: i64, logical_id: &str) -> Result<String> {
    if wake_at_ms < 0 {
        return Err(Error::invalid("cron wake time must be nonnegative"));
    }
    Ok(format!("{DUE_PREFIX}{wake_at_ms:020}:{logical_id}"))
}

fn read_json<T: for<'de> Deserialize<'de>>(
    db: &Connection,
    key: &str,
    label: &str,
) -> Result<Option<T>> {
    meta(db, key)?
        .map(|value| {
            serde_json::from_value(value)
                .map_err(|_| Error::new("AUTOMATION_CRON_STATE_INVALID", label))
        })
        .transpose()
}

fn write_state(db: &Connection, state: &GenerationState) -> Result<()> {
    set_meta(
        db,
        &state_key(&state.logical_id, &state.generation),
        &json!(state),
    )
}

fn clear_held(db: &Connection, logical_id: &str) -> Result<()> {
    db.execute("DELETE FROM meta WHERE key=?1", [held_key(logical_id)])?;
    Ok(())
}

fn set_held(
    db: &Connection,
    state: &GenerationState,
    entry: &AutomationEntry,
    first_due_at_ms: i64,
) -> Result<()> {
    let held = HeldIndex {
        schema_version: SCHEMA_VERSION,
        logical_id: state.logical_id.clone(),
        origin_manager_id: state.origin_manager_id.clone(),
        current_owner_manager_id: entry.owner_manager_id.clone(),
        project_id: state.project_id.clone(),
        automation_id: state.automation_id.clone(),
        generation: state.generation.clone(),
        first_due_at_ms,
    };
    set_meta(db, &held_key(&state.logical_id), &json!(held))
}

fn load_state(
    db: &Connection,
    logical_id: &str,
    generation: &str,
) -> Result<Option<GenerationState>> {
    let Some(state) = read_json::<GenerationState>(
        db,
        &state_key(logical_id, generation),
        "cron generation state is malformed",
    )?
    else {
        return Ok(None);
    };
    if state.schema_version != SCHEMA_VERSION
        || state.logical_id != logical_id
        || state.generation != generation
        || state.activation_cut_ms < 0
    {
        return Err(Error::new(
            "AUTOMATION_CRON_STATE_INVALID",
            "cron generation state identity is inconsistent",
        ));
    }
    Ok(Some(state))
}

fn is_enabled(entry: &AutomationEntry) -> bool {
    entry.enabled && entry.steps.contains(&AutomationStep::CheckRun) && entry.cron.is_some()
}

fn active_generation(db: &Connection, logical_id: &str) -> Result<Option<String>> {
    let Some(active) = read_json::<ActiveGeneration>(
        db,
        &active_key(logical_id),
        "active cron generation is malformed",
    )?
    else {
        return Ok(None);
    };
    if active.schema_version != SCHEMA_VERSION || active.generation.is_empty() {
        return Err(Error::new(
            "AUTOMATION_CRON_STATE_INVALID",
            "active cron generation identity is inconsistent",
        ));
    }
    Ok(Some(active.generation))
}

fn current_wake(
    settings: &CronSettings,
    state: &GenerationState,
    now_ms: i64,
) -> Result<Option<i64>> {
    let latest =
        scheduler::calendar::latest_due(&settings.calendar, now_ms, state.last_considered_due_ms)?;
    if let Some(due) = latest
        && (state.include_existing || due.due_at_ms > state.activation_cut_ms)
    {
        return Ok(Some(now_ms.saturating_add(1)));
    }
    scheduler::calendar::next_due_at_ms(&settings.calendar, now_ms)
}

fn set_due_index(
    db: &Connection,
    state: &mut GenerationState,
    owner_manager_id: &str,
    wake_at_ms: Option<i64>,
) -> Result<()> {
    if let Some(previous) = state.indexed_due_at_ms.take() {
        db.execute(
            "DELETE FROM meta WHERE key=?1",
            [due_key(previous, &state.logical_id)?],
        )?;
    }
    if let Some(wake_at_ms) = wake_at_ms {
        let index = DueIndex {
            schema_version: SCHEMA_VERSION,
            logical_id: state.logical_id.clone(),
            origin_manager_id: state.origin_manager_id.clone(),
            current_owner_manager_id: owner_manager_id.to_owned(),
            project_id: state.project_id.clone(),
            automation_id: state.automation_id.clone(),
            generation: state.generation.clone(),
            wake_at_ms,
        };
        set_meta(db, &due_key(wake_at_ms, &state.logical_id)?, &json!(index))?;
        state.indexed_due_at_ms = Some(wake_at_ms);
    }
    Ok(())
}

fn retry_wake(now_ms: i64) -> i64 {
    now_ms.saturating_add(RETRY_DELAY_MS).max(0)
}

/// Configure the cursor and due index in the same transaction as an
/// automation.config apply. A false include-existing flag cuts activation at
/// `now_ms`; true permits the one newest occurrence already due at that cut.
pub(crate) fn configure_activation(
    tx: &Transaction<'_>,
    before: Option<&AutomationEntry>,
    after: &AutomationEntry,
    include_existing: bool,
    now_ms: i64,
) -> Result<()> {
    if now_ms < 0 {
        return Err(Error::invalid(
            "cron activation timestamp must be nonnegative",
        ));
    }
    let (origin, logical) = logical_id(tx, after)?;
    clear_held(tx, &logical)?;
    let was_enabled = before.is_some_and(is_enabled);
    let enabled = is_enabled(after);
    let Some(settings) = after.cron.as_ref() else {
        if let Some(generation) = active_generation(tx, &logical)?
            && let Some(mut state) = load_state(tx, &logical, &generation)?
        {
            set_due_index(tx, &mut state, &after.owner_manager_id, None)?;
            state.updated_at_ms = now_ms;
            write_state(tx, &state)?;
        }
        return Ok(());
    };

    let generation = scheduler::calendar::generation_digest(&settings.calendar)?;
    let previous_generation = active_generation(tx, &logical)?;
    let previous_calendar = before
        .and_then(|entry| entry.cron.as_ref())
        .map(|cron| &cron.calendar);
    let previous_action = before
        .and_then(|entry| entry.cron.as_ref())
        .map(|cron| &cron.action);
    let calendar_changed = previous_calendar != Some(&settings.calendar);
    let action_changed = previous_action != Some(&settings.action);
    let activation_changed = enabled && (!was_enabled || calendar_changed || action_changed);

    let mut state = load_state(tx, &logical, &generation)?.unwrap_or_else(|| GenerationState {
        schema_version: SCHEMA_VERSION,
        logical_id: logical.clone(),
        origin_manager_id: origin.clone(),
        project_id: after.project_id.clone(),
        automation_id: after.automation_id.clone(),
        generation: generation.clone(),
        activation_cut_ms: after.cron.as_ref().unwrap().calendar.anchor_ms,
        include_existing: true,
        last_considered_due_ms: None,
        last_occurrence_id: None,
        last_operation_id: None,
        last_operation_state: None,
        last_receipt: None,
        indexed_due_at_ms: None,
        updated_at_ms: now_ms,
    });
    if state.origin_manager_id != origin
        || state.project_id != after.project_id
        || state.automation_id != after.automation_id
    {
        return Err(Error::new(
            "AUTOMATION_CRON_STATE_INVALID",
            "cron logical identity does not match its retained cursor",
        ));
    }
    if activation_changed {
        state.activation_cut_ms = now_ms;
        state.include_existing = include_existing;
    }
    state.updated_at_ms = now_ms;
    set_meta(
        tx,
        &active_key(&logical),
        &json!(ActiveGeneration {
            schema_version: SCHEMA_VERSION,
            generation: generation.clone(),
        }),
    )?;
    if enabled {
        let wake = current_wake(settings, &state, now_ms)?;
        set_due_index(tx, &mut state, &after.owner_manager_id, wake)?;
    } else {
        set_due_index(tx, &mut state, &after.owner_manager_id, None)?;
    }
    write_state(tx, &state)?;

    // A calendar edit retires the old due index but keeps its per-generation
    // cursor and occurrence records so switching back cannot replay work.
    if let Some(old_generation) = previous_generation
        && old_generation != generation
        && let Some(mut old_state) = load_state(tx, &logical, &old_generation)?
    {
        set_due_index(tx, &mut old_state, &after.owner_manager_id, None)?;
        old_state.updated_at_ms = now_ms;
        write_state(tx, &old_state)?;
    }
    Ok(())
}

/// Ownership relocation changes only the due-index executor pointer. The
/// stable origin key, occurrence cursor, and Operation/on-behalf records stay
/// fixed across A→B→C transfer chains.
pub(crate) fn relocate_state(
    tx: &Transaction<'_>,
    before: &AutomationEntry,
    after: &AutomationEntry,
) -> Result<()> {
    if before.project_id != after.project_id || before.automation_id != after.automation_id {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_CORRUPT",
            "cron transfer changed the project or logical automation identity",
        ));
    }
    let (old_origin, logical) = logical_id(tx, before)?;
    let (new_origin, new_logical) = logical_id(tx, after)?;
    if old_origin != new_origin || logical != new_logical {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_CORRUPT",
            "cron transfer changed the sealed origin identity",
        ));
    }
    let Some(generation) = active_generation(tx, &logical)? else {
        return Ok(());
    };
    let Some(mut state) = load_state(tx, &logical, &generation)? else {
        return Err(Error::new(
            "AUTOMATION_CRON_STATE_INVALID",
            "active cron generation has no retained cursor",
        ));
    };
    if let Some(wake_at_ms) = state.indexed_due_at_ms {
        let key = due_key(wake_at_ms, &logical)?;
        let mut due: DueIndex =
            read_json(tx, &key, "cron due index is malformed")?.ok_or_else(|| {
                Error::new(
                    "AUTOMATION_CRON_STATE_INVALID",
                    "active cron cursor references a missing due index",
                )
            })?;
        if due.logical_id != logical
            || due.generation != generation
            || due.origin_manager_id != old_origin
            || due.project_id != after.project_id
            || due.automation_id != after.automation_id
        {
            return Err(Error::new(
                "AUTOMATION_CRON_STATE_INVALID",
                "cron due index does not match the transferred logical entry",
            ));
        }
        due.current_owner_manager_id
            .clone_from(&after.owner_manager_id);
        set_meta(tx, &key, &json!(due))?;
    }
    if let Some(mut held) = read_json::<HeldIndex>(
        tx,
        &held_key(&logical),
        "held cron occurrence index is malformed",
    )? {
        if held.origin_manager_id != old_origin
            || held.project_id != after.project_id
            || held.automation_id != after.automation_id
            || held.generation != generation
        {
            return Err(Error::new(
                "AUTOMATION_CRON_STATE_INVALID",
                "held cron occurrence does not match the transferred logical entry",
            ));
        }
        held.current_owner_manager_id
            .clone_from(&after.owner_manager_id);
        set_meta(tx, &held_key(&logical), &json!(held))?;
    }
    state.updated_at_ms = after.updated_at_ms;
    write_state(tx, &state)
}

/// Read-only manager config projection for one entry.
pub(crate) fn state(db: &Connection, entry: &AutomationEntry) -> Result<Value> {
    let enabled = is_enabled(entry);
    let Some(settings) = entry.cron.as_ref() else {
        return Ok(json!({"configured":false,"enabled":false}));
    };
    let (origin, logical) = logical_id(db, entry)?;
    let generation = scheduler::calendar::generation_digest(&settings.calendar)?;
    let now_ms = model::now_ms()?;
    let upcoming_occurrences =
        scheduler::calendar::preview_next_occurrences(&settings.calendar, now_ms, 3)?
            .into_iter()
            .map(|occurrence| occurrence.due_at_ms)
            .collect::<Vec<_>>();
    let Some(state) = load_state(db, &logical, &generation)? else {
        return Ok(json!({
            "configured":true,
            "enabled":enabled,
            "origin_manager_id":origin,
            "calendar_generation":generation,
            "next_due_at_ms":if enabled { scheduler::calendar::next_due_at_ms(&settings.calendar, now_ms)? } else { None },
            "upcoming_occurrences":upcoming_occurrences,
            "last_considered_due_at_ms":Value::Null,
            "last_occurrence_id":Value::Null,
            "last_operation":Value::Null
        }));
    };
    let next_due = if enabled {
        match latest_due_for_state(settings, &state, now_ms)? {
            Some(occurrence) => Some(occurrence.due_at_ms),
            None => scheduler::calendar::next_due_at_ms(&settings.calendar, now_ms)?,
        }
    } else {
        None
    };
    let last_operation = state.last_operation_id.as_ref().map(|operation_id| {
        json!({
            "operation_id":operation_id,
            "state":state.last_operation_state.as_deref(),
            "receipt":state.last_receipt.as_ref(),
        })
    });
    Ok(json!({
        "configured":true,
        "enabled":enabled,
        "origin_manager_id":origin,
        "calendar_generation":generation,
        "activation_cut_ms":state.activation_cut_ms,
        "include_existing":state.include_existing,
        "next_due_at_ms":next_due,
        "upcoming_occurrences":upcoming_occurrences,
        "last_considered_due_at_ms":state.last_considered_due_ms,
        "last_occurrence_id":state.last_occurrence_id,
        "last_operation":last_operation,
        "held_occurrence":read_json::<HeldIndex>(db,&held_key(&logical),"held cron occurrence index is malformed")?.is_some(),
        "updated_at_ms":state.updated_at_ms
    }))
}

fn operation_state(db: &Connection, operation_id: &str) -> Result<Option<String>> {
    db.query_row(
        "SELECT state FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

fn latest_due_for_state(
    settings: &CronSettings,
    state: &GenerationState,
    now_ms: i64,
) -> Result<Option<scheduler::CalendarOccurrence>> {
    Ok(
        scheduler::calendar::latest_due(&settings.calendar, now_ms, state.last_considered_due_ms)?
            .filter(|due| state.include_existing || due.due_at_ms > state.activation_cut_ms),
    )
}

fn candidate_for_index(
    db: &Transaction<'_>,
    index_key: String,
    index: DueIndex,
    now_ms: i64,
) -> Result<Option<Candidate>> {
    if index.schema_version != SCHEMA_VERSION
        || index.wake_at_ms < 0
        || index_key != due_key(index.wake_at_ms, &index.logical_id)?
    {
        return Err(Error::new(
            "AUTOMATION_CRON_STATE_INVALID",
            "cron due index identity is inconsistent",
        ));
    }
    let Some(state) = load_state(db, &index.logical_id, &index.generation)? else {
        return Ok(None);
    };
    if state.indexed_due_at_ms != Some(index.wake_at_ms)
        || state.origin_manager_id != index.origin_manager_id
        || state.project_id != index.project_id
        || state.automation_id != index.automation_id
    {
        return Err(Error::new(
            "AUTOMATION_CRON_STATE_INVALID",
            "cron due index and cursor disagree",
        ));
    }
    let Some(entry) = config::load_entry(
        db,
        &index.current_owner_manager_id,
        &index.project_id,
        &index.automation_id,
    )?
    else {
        return Ok(None);
    };
    let (origin, logical) = logical_id(db, &entry)?;
    let active = active_generation(db, &logical)?;
    let valid_current = is_enabled(&entry)
        && logical == index.logical_id
        && origin == index.origin_manager_id
        && active.as_deref() == Some(index.generation.as_str())
        && entry.cron.as_ref().is_some_and(|settings| {
            scheduler::calendar::generation_digest(&settings.calendar)
                .is_ok_and(|generation| generation == index.generation)
        });
    if !valid_current {
        return Ok(None);
    }
    let settings = entry
        .cron
        .as_ref()
        .expect("enabled cron entry has settings");
    let Some(due) = latest_due_for_state(settings, &state, now_ms)? else {
        return Ok(None);
    };
    let active_operation = state
        .last_operation_id
        .as_deref()
        .map(|operation_id| operation_state(db, operation_id))
        .transpose()?
        .flatten()
        .is_some_and(|operation| ACTIVE_OPERATION_STATES.contains(&operation.as_str()));
    if meta(db, "execution_mode")?.unwrap_or(Value::Null)["new_work"] != "enabled"
        || active_operation
    {
        return Ok(None);
    }
    let occurrence_id = scheduler::calendar::occurrence_id(
        &origin,
        &entry.project_id,
        &entry.automation_id,
        &index.generation,
        due.due_at_ms,
    )?;
    let context = match CronExecutionContext::from_committed_entry(
        db,
        &entry,
        &index.generation,
        &occurrence_id,
        due.due_at_ms,
    ) {
        Ok(context) => context,
        Err(error)
            if error.code == "FORBIDDEN"
                || error.code.starts_with("AUTOMATION_")
                || error.code == "NOT_FOUND"
                || error.code == "CHECK_SOURCE_REQUIRED" =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    Ok(Some(Candidate {
        index_key,
        index,
        entry,
        context,
        due_at_ms: due.due_at_ms,
        occurrence_id,
    }))
}

fn read_rows(
    db: &Connection,
    lower: &str,
    upper: &str,
    after: Option<&str>,
    limit: usize,
) -> Result<Vec<(String, String)>> {
    let mut statement = db.prepare(
        "SELECT key,value_json FROM meta \
         WHERE key>=?1 AND key<?2 AND (?3 IS NULL OR key>?3) \
         ORDER BY key LIMIT ?4",
    )?;
    statement
        .query_map(params![lower, upper, after, limit as i64], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn read_held_rows(db: &Connection, limit: usize) -> Result<Vec<(String, String)>> {
    let upper = format!("{HELD_PREFIX}~");
    let cursor = meta(db, HELD_CURSOR_KEY)?.and_then(|value| value.as_str().map(str::to_owned));
    let after_key = cursor.as_deref().map(held_key);
    let mut rows = read_rows(db, HELD_PREFIX, &upper, after_key.as_deref(), limit)?;
    if rows.len() < limit
        && let Some(after_key) = after_key.as_deref()
    {
        let mut wrapped = read_rows(db, HELD_PREFIX, after_key, None, limit - rows.len())?;
        rows.append(&mut wrapped);
    }
    Ok(rows)
}

fn prepare_batch(db: &Transaction<'_>, limit: usize, now_ms: i64) -> Result<PreparedBatch> {
    let mut batch = PreparedBatch::default();
    let limit = limit.clamp(1, MAX_RECONCILE_BATCH);
    let held_rows = read_held_rows(db, limit)?;
    let mut last_held_logical = None;
    for (held_key_value, raw) in held_rows {
        let held: HeldIndex = serde_json::from_str(&raw).map_err(|_| {
            Error::new(
                "AUTOMATION_CRON_STATE_INVALID",
                "held cron index JSON is invalid",
            )
        })?;
        if held.schema_version != SCHEMA_VERSION
            || held.first_due_at_ms < 0
            || held_key_value != held_key(&held.logical_id)
        {
            return Err(Error::new(
                "AUTOMATION_CRON_STATE_INVALID",
                "held cron index identity is inconsistent",
            ));
        }
        last_held_logical = Some(held.logical_id.clone());
        let Some(state) = load_state(db, &held.logical_id, &held.generation)? else {
            clear_held(db, &held.logical_id)?;
            continue;
        };
        let Some(wake_at_ms) = state.indexed_due_at_ms else {
            clear_held(db, &held.logical_id)?;
            continue;
        };
        let index_key = due_key(wake_at_ms, &held.logical_id)?;
        let Some(index) = read_json::<DueIndex>(db, &index_key, "cron due index is malformed")?
        else {
            clear_held(db, &held.logical_id)?;
            continue;
        };
        if held.origin_manager_id != index.origin_manager_id
            || held.current_owner_manager_id != index.current_owner_manager_id
            || held.project_id != index.project_id
            || held.automation_id != index.automation_id
            || held.generation != index.generation
        {
            return Err(Error::new(
                "AUTOMATION_CRON_STATE_INVALID",
                "held and due indexes disagree",
            ));
        }
        if let Some(candidate) = candidate_for_index(db, index_key.clone(), index.clone(), now_ms)?
        {
            if !batch
                .candidates
                .iter()
                .any(|candidate| candidate.index.logical_id == held.logical_id)
            {
                batch.next_due_at_ms = min_due(
                    batch.next_due_at_ms,
                    candidate
                        .entry
                        .cron
                        .as_ref()
                        .map(|settings| {
                            scheduler::calendar::next_due_at_ms(&settings.calendar, now_ms)
                        })
                        .transpose()?
                        .flatten(),
                );
                batch.candidates.push(candidate);
            }
        } else {
            refresh_candidate_index(db, &index_key, &index, now_ms, true)?;
        }
    }
    if let Some(logical) = last_held_logical {
        set_meta(db, HELD_CURSOR_KEY, &json!(logical))?;
    }

    let upper = format!("{DUE_PREFIX}{:020}:~", now_ms.max(0));
    let remaining = limit.saturating_sub(batch.candidates.len());
    let due_rows = read_rows(db, DUE_PREFIX, &upper, None, remaining + 1)?;
    batch.due_remaining |= due_rows.len() > remaining;
    for (key, raw) in due_rows.into_iter().take(remaining) {
        let index: DueIndex = serde_json::from_str(&raw).map_err(|_| {
            Error::new(
                "AUTOMATION_CRON_STATE_INVALID",
                "cron due index JSON is invalid",
            )
        })?;
        let already_prepared = batch
            .candidates
            .iter()
            .any(|candidate| candidate.index.logical_id == index.logical_id);
        if !already_prepared
            && let Some(candidate) = candidate_for_index(db, key.clone(), index.clone(), now_ms)?
        {
            batch.next_due_at_ms = min_due(
                batch.next_due_at_ms,
                candidate
                    .entry
                    .cron
                    .as_ref()
                    .map(|settings| scheduler::calendar::next_due_at_ms(&settings.calendar, now_ms))
                    .transpose()?
                    .flatten(),
            );
            batch.candidates.push(candidate);
        } else if !already_prepared {
            batch.next_due_at_ms = min_due(
                batch.next_due_at_ms,
                refresh_candidate_index(db, &key, &index, now_ms, true)?,
            );
        }
    }
    if batch.next_due_at_ms.is_none() {
        // A held occurrence remains indexed for event-driven re-evaluation;
        // this bounded clock wake supplies recovery after process/readback
        // interruptions even when its retry index is in the future.
        let has_held: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM meta WHERE key>=?1 AND key<?2)",
            params![HELD_PREFIX, format!("{HELD_PREFIX}~")],
            |row| row.get(0),
        )?;
        if has_held {
            batch.next_due_at_ms = Some(retry_wake(now_ms));
        }
    }
    Ok(batch)
}

fn min_due(current: Option<i64>, candidate: Option<i64>) -> Option<i64> {
    match (current, candidate) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn refresh_candidate_index(
    tx: &Transaction<'_>,
    index_key: &str,
    _snapshot: &DueIndex,
    now_ms: i64,
    retry: bool,
) -> Result<Option<i64>> {
    let Some(raw_index) = meta(tx, index_key)? else {
        return Ok(None);
    };
    let index: DueIndex = serde_json::from_value(raw_index).map_err(|_| {
        Error::new(
            "AUTOMATION_CRON_STATE_INVALID",
            "cron due index is malformed",
        )
    })?;
    let Some(active_generation) = active_generation(tx, &index.logical_id)? else {
        tx.execute("DELETE FROM meta WHERE key=?1", [index_key])?;
        clear_held(tx, &index.logical_id)?;
        return Ok(None);
    };
    if active_generation != index.generation {
        tx.execute("DELETE FROM meta WHERE key=?1", [index_key])?;
        clear_held(tx, &index.logical_id)?;
        return Ok(None);
    }
    let Some(mut state) = load_state(tx, &index.logical_id, &index.generation)? else {
        tx.execute("DELETE FROM meta WHERE key=?1", [index_key])?;
        clear_held(tx, &index.logical_id)?;
        return Ok(None);
    };
    let Some(entry) = config::load_entry(
        tx,
        &index.current_owner_manager_id,
        &index.project_id,
        &index.automation_id,
    )?
    else {
        set_due_index(tx, &mut state, &index.current_owner_manager_id, None)?;
        state.updated_at_ms = now_ms;
        write_state(tx, &state)?;
        clear_held(tx, &index.logical_id)?;
        return Ok(None);
    };
    let Some(settings) = entry.cron.as_ref().filter(|_| is_enabled(&entry)) else {
        set_due_index(tx, &mut state, &entry.owner_manager_id, None)?;
        state.updated_at_ms = now_ms;
        write_state(tx, &state)?;
        clear_held(tx, &index.logical_id)?;
        return Ok(None);
    };
    let pending = latest_due_for_state(settings, &state, now_ms)?;
    let wake = if retry && pending.is_some() {
        Some(retry_wake(now_ms))
    } else if retry {
        scheduler::calendar::next_due_at_ms(&settings.calendar, now_ms)?
    } else {
        current_wake(settings, &state, now_ms)?
    };
    set_due_index(tx, &mut state, &entry.owner_manager_id, wake)?;
    if retry {
        if let Some(due) = pending {
            set_held(tx, &state, &entry, due.due_at_ms)?;
        } else {
            clear_held(tx, &index.logical_id)?;
        }
    } else {
        clear_held(tx, &index.logical_id)?;
    }
    state.updated_at_ms = now_ms;
    write_state(tx, &state)?;
    Ok(wake)
}

fn operation_for_request(db: &Connection, request_id: &str) -> Result<Option<(String, String)>> {
    db.query_row(
        "SELECT operation_id,state FROM operations \
         WHERE caller_id=?1 AND method='check.run' AND client_request_id=?2 \
         ORDER BY created_at_ms DESC,operation_id DESC LIMIT 1",
        params![
            crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID,
            request_id
        ],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
    .map_err(Into::into)
}

fn occurrence_record(
    db: &Connection,
    candidate: &Candidate,
    request_id: &str,
    receipt: Result<Value>,
    now_ms: i64,
) -> Result<Option<OccurrenceRecord>> {
    let Some((operation_id, operation_state)) = operation_for_request(db, request_id)? else {
        return Ok(None);
    };
    let (disposition, receipt_value) = match receipt {
        Ok(value) => ("retained".to_owned(), json!({"ok":true,"receipt":value})),
        Err(error) => (
            "rejected".to_owned(),
            json!({"ok":false,"error":{"code":error.code,"message":error.message}}),
        ),
    };
    let receipt_value = json!({
        "operation_id":operation_id,
        "operation_state":operation_state,
        "disposition":disposition,
        "admission":receipt_value
    });
    Ok(Some(OccurrenceRecord {
        schema_version: SCHEMA_VERSION,
        logical_id: candidate.index.logical_id.clone(),
        origin_manager_id: candidate.index.origin_manager_id.clone(),
        project_id: candidate.index.project_id.clone(),
        automation_id: candidate.index.automation_id.clone(),
        generation: candidate.index.generation.clone(),
        occurrence_id: candidate.occurrence_id.clone(),
        due_at_ms: candidate.due_at_ms,
        operation_id,
        operation_state,
        disposition,
        receipt: receipt_value,
        recorded_at_ms: now_ms,
    }))
}

fn admit_candidate(
    tx: &Transaction<'_>,
    candidate: &Candidate,
    config: &Config,
    resolution: &super::checks::CheckPlanResolution,
    now_ms: i64,
) -> Result<(Option<i64>, bool)> {
    let Some(raw_index) = meta(tx, &candidate.index_key)? else {
        return Ok((None, false));
    };
    let index: DueIndex = serde_json::from_value(raw_index).map_err(|_| {
        Error::new(
            "AUTOMATION_CRON_STATE_INVALID",
            "cron due index is malformed",
        )
    })?;
    if index.logical_id != candidate.index.logical_id
        || index.generation != candidate.index.generation
        || index.wake_at_ms != candidate.index.wake_at_ms
    {
        return Ok((None, false));
    }
    let Some(active) = active_generation(tx, &index.logical_id)? else {
        tx.execute("DELETE FROM meta WHERE key=?1", [&candidate.index_key])?;
        clear_held(tx, &index.logical_id)?;
        return Ok((None, false));
    };
    if active != index.generation {
        tx.execute("DELETE FROM meta WHERE key=?1", [&candidate.index_key])?;
        clear_held(tx, &index.logical_id)?;
        return Ok((None, false));
    }
    let Some(mut state) = load_state(tx, &index.logical_id, &index.generation)? else {
        return Err(Error::new(
            "AUTOMATION_CRON_STATE_INVALID",
            "due cron generation has no cursor",
        ));
    };
    let Some(entry) = config::load_entry(
        tx,
        &index.current_owner_manager_id,
        &index.project_id,
        &index.automation_id,
    )?
    else {
        set_due_index(tx, &mut state, &index.current_owner_manager_id, None)?;
        write_state(tx, &state)?;
        clear_held(tx, &index.logical_id)?;
        return Ok((None, false));
    };
    let (origin, logical) = logical_id(tx, &entry)?;
    let Some(settings) = entry.cron.as_ref().filter(|_| is_enabled(&entry)) else {
        set_due_index(tx, &mut state, &entry.owner_manager_id, None)?;
        write_state(tx, &state)?;
        clear_held(tx, &index.logical_id)?;
        return Ok((None, false));
    };
    let generation = scheduler::calendar::generation_digest(&settings.calendar)?;
    let Some(due) = latest_due_for_state(settings, &state, now_ms)? else {
        let next = refresh_candidate_index(tx, &candidate.index_key, &index, now_ms, false)?;
        return Ok((next, false));
    };
    let current_occurrence = scheduler::calendar::occurrence_id(
        &origin,
        &entry.project_id,
        &entry.automation_id,
        &generation,
        due.due_at_ms,
    )?;
    if logical != index.logical_id
        || origin != index.origin_manager_id
        || generation != index.generation
        || current_occurrence != candidate.occurrence_id
        || due.due_at_ms != candidate.due_at_ms
    {
        let next = refresh_candidate_index(tx, &candidate.index_key, &index, now_ms, true)?;
        return Ok((next, false));
    }
    if meta(tx, "execution_mode")?.unwrap_or(Value::Null)["new_work"] != "enabled" {
        let next = refresh_candidate_index(tx, &candidate.index_key, &index, now_ms, true)?;
        return Ok((next, false));
    }
    if let Some(operation_id) = state.last_operation_id.as_deref()
        && operation_state(tx, operation_id)?
            .is_some_and(|operation| ACTIVE_OPERATION_STATES.contains(&operation.as_str()))
    {
        let next = refresh_candidate_index(tx, &candidate.index_key, &index, now_ms, true)?;
        return Ok((next, false));
    }
    let occurrence_key = occurrence_key(&logical, &generation, &current_occurrence);
    if let Some(existing) =
        read_json::<OccurrenceRecord>(tx, &occurrence_key, "cron occurrence ledger is malformed")?
    {
        if existing.due_at_ms != due.due_at_ms
            || existing.logical_id != logical
            || existing.origin_manager_id != origin
            || existing.project_id != entry.project_id
            || existing.automation_id != entry.automation_id
            || existing.generation != generation
            || existing.occurrence_id != current_occurrence
            || existing.recorded_at_ms < 0
            || !matches!(existing.disposition.as_str(), "retained" | "rejected")
        {
            return Err(Error::new(
                "AUTOMATION_CRON_STATE_INVALID",
                "retained cron occurrence identity conflicts with its key",
            ));
        }
        state.last_considered_due_ms = Some(due.due_at_ms);
        state.last_occurrence_id = Some(current_occurrence);
        state.last_operation_id = Some(existing.operation_id);
        state.last_operation_state = Some(existing.operation_state);
        state.last_receipt = Some(existing.receipt);
        state.updated_at_ms = now_ms;
        let next = current_wake(settings, &state, now_ms)?;
        set_due_index(tx, &mut state, &entry.owner_manager_id, next)?;
        write_state(tx, &state)?;
        clear_held(tx, &logical)?;
        return Ok((next, false));
    }

    let admission = super::mutate_cron_check_in_transaction(
        tx,
        &candidate.context,
        config,
        now_ms,
        resolution,
    )?;
    let params = candidate.context.request_params()?;
    let request_id = model::text(&params, "client_request_id")?.to_owned();
    let Some(occurrence) =
        occurrence_record(tx, candidate, &request_id, admission.receipt, now_ms)?
    else {
        return Err(Error::new(
            "AUTOMATION_CRON_RECEIPT_MISSING",
            "typed cron admission returned without a durable Operation receipt",
        ));
    };
    let operation_id = occurrence.operation_id.clone();
    let operation_state_value = occurrence.operation_state.clone();
    let receipt = occurrence.receipt.clone();
    set_meta(tx, &occurrence_key, &json!(occurrence))?;
    state.last_considered_due_ms = Some(due.due_at_ms);
    state.last_occurrence_id = Some(current_occurrence);
    state.last_operation_id = Some(operation_id);
    state.last_operation_state = Some(operation_state_value);
    state.last_receipt = Some(receipt);
    state.updated_at_ms = now_ms;
    let next = current_wake(settings, &state, now_ms)?;
    set_due_index(tx, &mut state, &entry.owner_manager_id, next)?;
    write_state(tx, &state)?;
    clear_held(tx, &logical)?;
    Ok((next, admission.wake_check_worker))
}

fn postpone_candidate(
    db: &mut Connection,
    candidate: &Candidate,
    now_ms: i64,
) -> Result<Option<i64>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let next = refresh_candidate_index(&tx, &candidate.index_key, &candidate.index, now_ms, true)?;
    tx.commit()?;
    Ok(next)
}

impl Store {
    /// Process a bounded batch of manager-owned due occurrences. Calendar
    /// evaluation and source resolution happen before the final IMMEDIATE
    /// transaction; current entry, enablement, rights, operation receipt, and
    /// occurrence cursor are revalidated/committed together in that boundary.
    pub(crate) async fn reconcile_automation_cron_once(
        &self,
        limit: usize,
        now_ms: i64,
    ) -> Result<Option<i64>> {
        let prepared = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let batch = prepare_batch(&tx, limit, now_ms)?;
                tx.commit()?;
                Ok(batch)
            })
            .await?;
        let config = self.config.clone();
        let mut next_due = prepared.next_due_at_ms;
        let mut wake_check_worker = false;
        for candidate in prepared.candidates {
            let context = candidate.context.clone();
            let resolution = match self.resolve_cron_check_plan(context).await {
                Ok(resolution) => resolution,
                Err(_) => {
                    let for_retry = candidate.clone();
                    let retry = self
                        .run(move |db| postpone_candidate(db, &for_retry, now_ms))
                        .await?;
                    next_due = min_due(next_due, retry);
                    continue;
                }
            };
            let for_tx = candidate.clone();
            let config = config.clone();
            let result = self
                .run(move |db| {
                    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                    let result = admit_candidate(&tx, &for_tx, &config, &resolution, now_ms);
                    match result {
                        Ok((next, wake)) => {
                            tx.commit()?;
                            Ok((next, wake))
                        }
                        Err(error) => Err(error),
                    }
                })
                .await;
            match result {
                Ok((next, wake)) => {
                    next_due = min_due(next_due, next);
                    wake_check_worker |= wake;
                }
                Err(error)
                    if matches!(
                        error.code.as_str(),
                        "AUTOMATION_ACTION_CHANGED" | "AUTOMATION_NOT_FOUND" | "FORBIDDEN"
                    ) =>
                {
                    let retry = self
                        .run(move |db| postpone_candidate(db, &candidate, now_ms))
                        .await?;
                    next_due = min_due(next_due, retry);
                }
                Err(error) => return Err(error),
            }
        }
        if prepared.due_remaining {
            next_due = min_due(next_due, Some(now_ms.saturating_add(1)));
        }
        if wake_check_worker {
            self.changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        Ok(next_due)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        artifacts::ArtifactFiles,
        automation::authorization,
        checks::{
            model::{CheckProfile, Parser},
            source::{SourceFile, SourceManifest},
        },
        model::{Credential, Principal},
        platform::{DataRoot, bootstrap_credential},
        store::StoreOwner,
    };
    use rusqlite::params;
    use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

    const PROJECT: &str = "manager-cron-regression";
    const AUTOMATION_ID: &str = "calendar-check";

    async fn call(store: &Store, principal: &Principal, method: &str, mut request: Value) -> Value {
        request["client_request_id"] = json!(model::new_id());
        store
            .call(principal.clone(), method.to_owned(), request)
            .await
            .unwrap()
    }

    async fn register_manager(
        store: &Store,
        operator: &Principal,
        client_id: &str,
    ) -> (Principal, String) {
        let token = format!("cron-test-{client_id}-{}", model::new_id());
        call(
            store,
            operator,
            "client.register",
            json!({
                "client_id":client_id,
                "role":"manager",
                "token_hash":model::digest(token.as_bytes()),
            }),
        )
        .await;
        let principal = store
            .authenticate(Credential {
                client_id: client_id.to_owned(),
                token: token.clone(),
            })
            .await
            .unwrap();
        (principal, token)
    }

    async fn create_source_snapshot(
        store: &Store,
        directory: &PathBuf,
        task_id: &str,
        attempt_id: &str,
    ) -> String {
        let candidate_ref = format!("source-{}", model::digest(task_id.as_bytes()));
        let content = b"manager cron source fixture";
        let commit = "a".repeat(40);
        let tree = "b".repeat(40);
        let manifest = SourceManifest {
            version: 1,
            commit: commit.clone(),
            tree: tree.clone(),
            files: vec![SourceFile {
                path: "fixture.txt".into(),
                mode: "100644".into(),
                object_id: "c".repeat(40),
                byte_length: content.len() as u64,
                sha256: model::digest(content),
            }],
        };
        let metadata = json!({
            "task_id":task_id,
            "attempt_id":attempt_id,
            "task_revision":1,
            "commit":commit,
            "tree":tree,
            "file_count":1,
            "coverage":"complete",
        });
        let (record, bytes) = ArtifactFiles::document(
            "source_snapshot",
            &candidate_ref,
            &json!(manifest),
            metadata.clone(),
        )
        .unwrap();
        let source_dir = directory.join("sources").join(&candidate_ref);
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::write(source_dir.join("fixture.txt"), content).unwrap();
        ArtifactFiles::new(directory)
            .unwrap()
            .publish(&record, &bytes)
            .unwrap();
        store
            .run(move |db| {
                db.execute(
                    "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                    params![
                        record.artifact_id,
                        record.relative_path,
                        record.kind,
                        record.byte_length as i64,
                        record.content_digest,
                        model::now_ms()?,
                        model::canonical(&metadata)?,
                    ],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        candidate_ref
    }

    fn cron_entry_state(db: &Connection, owner: &str) -> Result<Value> {
        let entry = config::load_entry(db, owner, PROJECT, AUTOMATION_ID)?
            .ok_or_else(|| Error::new("TEST_CRON_ENTRY_MISSING", "cron entry is missing"))?;
        state(db, &entry)
    }

    #[tokio::test]
    async fn manager_cron_admits_once_and_preserves_origin_across_transfer_and_restart() {
        let directory =
            std::env::temp_dir().join(format!("swarm-manager-cron-{}", model::new_id()));
        std::fs::create_dir_all(&directory).unwrap();
        let root = DataRoot::acquire(&directory).unwrap();
        let operator_credential = bootstrap_credential(&root.path).unwrap();
        let mut config = Config::default();
        config.storage.data_dir = root.path.clone();
        config.checks.enabled = true;
        config.checks.profiles = vec![CheckProfile {
            profile_id: "strict".into(),
            profile_revision: "v1".into(),
            executable: std::env::current_exe().unwrap(),
            args: Vec::new(),
            parser: Parser::ExitCode,
            resource: "cron-checks".into(),
            environment: BTreeMap::new(),
            inherit_env: Vec::new(),
            expected_targets: Vec::new(),
            reproducible: false,
            fingerprint_env: Vec::new(),
            versioned_inputs: BTreeMap::new(),
        }];
        let config = Arc::new(config);
        let owner = StoreOwner::start(root, config.clone(), operator_credential.clone())
            .await
            .unwrap();
        let operator = owner
            .store
            .authenticate(operator_credential.clone())
            .await
            .unwrap();
        let (origin, _) = register_manager(&owner.store, &operator, "cron-origin").await;
        let (successor, successor_token) =
            register_manager(&owner.store, &operator, "cron-successor").await;
        let (unrelated, _) = register_manager(&owner.store, &operator, "cron-unrelated").await;

        call(
            &owner.store,
            &operator,
            "gm.handover",
            json!({"client_id":origin.client_id}),
        )
        .await;
        let task = call(
            &owner.store,
            &operator,
            "task.create",
            json!({
                "project_id":PROJECT,
                "spec":{
                    "objective":"Retain a real manager-owned cron CheckRun",
                    "phase":"verification",
                    "owner_policy_id":"owner-policy-v1",
                    "requirements":[{"id":"R1","statement":"Preserve the exact Attempt owner and candidate"}],
                }
            }),
        )
        .await;
        let task_id = task["task_id"].as_str().unwrap().to_owned();
        let claimed = call(
            &owner.store,
            &origin,
            "task.claim",
            json!({"task_id":task_id,"expected_revision":1}),
        )
        .await;
        let attempt_id = claimed["attempt_id"].as_str().unwrap().to_owned();
        let candidate_ref =
            create_source_snapshot(&owner.store, &owner.store.data_dir, &task_id, &attempt_id)
                .await;
        let cron_settings = json!({
            "calendar":{"expression":"0 0 0 1 1 *","timezone":"UTC","anchor_ms":0},
            "action":{
                "kind":"check_run",
                "attempt_id":attempt_id,
                "expected_task_revision":1,
                "candidate_ref":candidate_ref,
                "profile_id":"strict",
                "profile_revision":"v1",
            }
        });
        let enable_entry = json!({
            "project_id":PROJECT,
            "changes":[{
                "automation_id":AUTOMATION_ID,
                "expected_revision":0,
                "include_existing":true,
                "patch":{"enabled":true,"steps":["check_run"],"cron":cron_settings},
            }]
        });
        let applied = call(
            &owner.store,
            &origin,
            "automation.config.apply",
            enable_entry,
        )
        .await;
        assert_eq!(applied["applied"], true);

        // An independently configured manager cannot use their own entry to
        // act on an Attempt outside their current task scope.
        let foreign_applied = call(
            &owner.store,
            &unrelated,
            "automation.config.apply",
            json!({
                "project_id":PROJECT,
                "changes":[{
                    "automation_id":"foreign-calendar-check",
                    "expected_revision":0,
                    "include_existing":true,
                    "patch":{"enabled":true,"steps":["check_run"],"cron":cron_settings},
                }]
            }),
        )
        .await;
        assert_eq!(foreign_applied["applied"], true);

        let reconcile_ms = model::now_ms().unwrap();
        let (first, concurrent) = tokio::join!(
            owner.store.reconcile_automation_cron_once(16, reconcile_ms),
            owner.store.reconcile_automation_cron_once(16, reconcile_ms),
        );
        first.unwrap();
        concurrent.unwrap();

        // A distinct direct request that resolves to the queued CheckRun gets
        // its own settled receipt without changing the original worker row or
        // leaving a second queued Operation without a CheckRun.
        let coalesced_request_id = format!("manager-cron-coalesced-{}", model::new_id());
        let coalesced_receipt = owner
            .store
            .call(
                origin.clone(),
                "check.run".into(),
                json!({
                    "client_request_id":coalesced_request_id.clone(),
                    "attempt_id":attempt_id.clone(),
                    "candidate_ref":candidate_ref.clone(),
                    "profile_id":"strict",
                    "profile_revision":"v1",
                }),
            )
            .await
            .unwrap();
        assert_eq!(coalesced_receipt["coalesced"], true);
        let (
            coalesced_operation_id,
            coalesced_state,
            coalesced_result,
            queued_operation_count,
            check_count,
            original_check,
        ) =
            owner
                .store
                .run({
                    let request_id = coalesced_request_id.clone();
                    let origin_id = origin.client_id.clone();
                    let original_operation = coalesced_receipt["operation_id"]
                        .as_str()
                        .unwrap()
                        .to_owned();
                    move |db| {
                        let (operation_id, state, result): (String, String, String) = db.query_row(
                            "SELECT operation_id,state,result_json FROM operations WHERE caller_id=?1 AND method='check.run' AND client_request_id=?2",
                            params![origin_id, request_id],
                            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                        )?;
                        let queued_operation_count: i64 = db.query_row(
                            "SELECT count(*) FROM operations WHERE method='check.run' AND state='queued'",
                            [],
                            |row| row.get(0),
                        )?;
                        let (check_count, original_check_id, original_state): (i64, String, String) =
                            db.query_row(
                                "SELECT count(*),check_id,state FROM check_runs WHERE operation_id=?1",
                                [&original_operation],
                                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                            )?;
                        Ok((
                            operation_id,
                            state,
                            serde_json::from_str::<Value>(&result)?,
                            queued_operation_count,
                            check_count,
                            (original_check_id, original_state),
                        ))
                    }
                })
                .await
                .unwrap();
        assert_ne!(
            coalesced_operation_id,
            coalesced_receipt["operation_id"].as_str().unwrap()
        );
        assert_eq!(coalesced_state, "settled");
        assert_eq!(coalesced_result["coalesced"], true);
        assert_eq!(queued_operation_count, 1);
        assert_eq!(check_count, 1);
        assert_eq!(original_check.1, "queued");
        assert_eq!(
            original_check.0,
            coalesced_receipt["check_id"].as_str().unwrap()
        );

        let (operation_id, occurrence_id, original_link, owner_state, attempt_owner, op_count) =
            owner
                .store
                .run({
                    let origin_id = origin.client_id.clone();
                    let attempt_id = attempt_id.clone();
                    move |db| {
                        let (operation_id,): (String,) = db.query_row(
                            "SELECT operation_id FROM operations WHERE caller_id=?1 AND method='check.run' ORDER BY created_at_ms,operation_id LIMIT 1",
                            [authorization::AUTOMATION_TECHNICAL_REQUESTER_ID],
                            |row| Ok((row.get(0)?,)),
                        )?;
                        let link = authorization::operation_link(db, &operation_id)?
                            .ok_or_else(|| Error::new("TEST_CRON_LINK_MISSING", "cron link is missing"))?;
                        let attempt_owner = db.query_row(
                            "SELECT owner_id FROM attempts WHERE attempt_id=?1",
                            [&attempt_id],
                            |row| row.get::<_, String>(0),
                        )?;
                        let check_count: i64 = db.query_row(
                            "SELECT count(*) FROM check_runs WHERE operation_id=?1",
                            [&operation_id],
                            |row| row.get(0),
                        )?;
                        let op_count: i64 = db.query_row(
                            "SELECT count(*) FROM operations WHERE caller_id=?1 AND method='check.run'",
                            [authorization::AUTOMATION_TECHNICAL_REQUESTER_ID],
                            |row| row.get(0),
                        )?;
                        if check_count != 1 {
                            return Err(Error::new("TEST_CRON_CHECK_MISSING", "typed cron admission did not retain one standard CheckRun"));
                        }
                        Ok((
                            operation_id,
                            link.cause["id"].as_str().unwrap().to_owned(),
                            link.effective_manager_id,
                            cron_entry_state(db, &origin_id)?,
                            attempt_owner,
                            op_count,
                        ))
                    }
                })
                .await
                .unwrap();
        assert_eq!(op_count, 1, "the foreign manager must not admit a CheckRun");
        assert_eq!(original_link, origin.client_id);
        assert_eq!(attempt_owner, origin.client_id);
        assert_eq!(
            owner_state["last_occurrence_id"].as_str().unwrap(),
            occurrence_id.as_str()
        );
        assert_eq!(
            owner_state["last_operation"]["operation_id"]
                .as_str()
                .unwrap(),
            operation_id.as_str()
        );

        call(
            &owner.store,
            &operator,
            "gm.handover",
            json!({"client_id":successor.client_id}),
        )
        .await;
        let transferred = call(
            &owner.store,
            &successor,
            "automation.config.transfer",
            json!({
                "project_id":PROJECT,
                "former_owner_manager_id":origin.client_id,
                "automation_id":AUTOMATION_ID,
                "expected_revision":1,
            }),
        )
        .await;
        assert_eq!(transferred["status"], "transferred");

        let foreign_transfer = owner
            .store
            .call(
                unrelated.clone(),
                "automation.config.transfer".into(),
                json!({
                    "client_request_id":model::new_id(),
                    "project_id":PROJECT,
                    "former_owner_manager_id":origin.client_id,
                    "automation_id":AUTOMATION_ID,
                    "expected_revision":1,
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(foreign_transfer.code, "FORBIDDEN");

        let (transferred_state, transferred_owner, historical_owner, count_after_transfer) = owner
            .store
            .run({
                let successor_id = successor.client_id.clone();
                let operation_id = operation_id.clone();
                let attempt_id = attempt_id.clone();
                move |db| {
                    let attempt_owner = db.query_row(
                        "SELECT owner_id FROM attempts WHERE attempt_id=?1",
                        [&attempt_id],
                        |row| row.get::<_, String>(0),
                    )?;
                    let link =
                        authorization::operation_link(db, &operation_id)?.ok_or_else(|| {
                            Error::new("TEST_CRON_LINK_MISSING", "cron link disappeared")
                        })?;
                    let count = db.query_row(
                        "SELECT count(*) FROM operations WHERE caller_id=?1 AND method='check.run'",
                        [authorization::AUTOMATION_TECHNICAL_REQUESTER_ID],
                        |row| row.get::<_, i64>(0),
                    )?;
                    Ok((
                        cron_entry_state(db, &successor_id)?,
                        attempt_owner,
                        link.effective_manager_id,
                        count,
                    ))
                }
            })
            .await
            .unwrap();
        assert_eq!(
            transferred_state["last_occurrence_id"].as_str().unwrap(),
            occurrence_id.as_str()
        );
        assert_eq!(
            transferred_state["calendar_generation"],
            owner_state["calendar_generation"]
        );
        assert_eq!(transferred_owner, origin.client_id);
        assert_eq!(historical_owner, origin.client_id);
        assert_eq!(count_after_transfer, 1);

        owner.close().await.unwrap();
        let reopened_root = DataRoot::acquire(&directory).unwrap();
        let reopened = StoreOwner::start(reopened_root, config, operator_credential.clone())
            .await
            .unwrap();
        let reopened_successor = reopened
            .store
            .authenticate(Credential {
                client_id: successor.client_id,
                token: successor_token,
            })
            .await
            .unwrap();
        reopened
            .store
            .reconcile_automation_cron_once(16, reconcile_ms)
            .await
            .unwrap();
        let (reopened_state, reopened_count, reopened_attempt_owner, reopened_link_owner) =
            reopened
                .store
                .run({
                    let operation_id = operation_id.clone();
                    let attempt_id = attempt_id.clone();
                    let current_owner = reopened_successor.client_id.clone();
                    move |db| {
                        let link = authorization::operation_link(db, &operation_id)?
                            .ok_or_else(|| Error::new("TEST_CRON_LINK_MISSING", "cron link did not survive restart"))?;
                        let count = db.query_row(
                            "SELECT count(*) FROM operations WHERE caller_id=?1 AND method='check.run'",
                            [authorization::AUTOMATION_TECHNICAL_REQUESTER_ID],
                            |row| row.get::<_, i64>(0),
                        )?;
                        let attempt_owner = db.query_row(
                            "SELECT owner_id FROM attempts WHERE attempt_id=?1",
                            [&attempt_id],
                            |row| row.get::<_, String>(0),
                        )?;
                        Ok((
                            cron_entry_state(db, &current_owner)?,
                            count,
                            attempt_owner,
                            link.effective_manager_id,
                        ))
                    }
                })
                .await
                .unwrap();
        assert_eq!(
            reopened_state["last_occurrence_id"].as_str().unwrap(),
            occurrence_id.as_str()
        );
        assert_eq!(
            reopened_state["last_operation"]["operation_id"]
                .as_str()
                .unwrap(),
            operation_id.as_str()
        );
        assert_eq!(
            reopened_count, 1,
            "restart must not replay the same due occurrence"
        );
        assert_eq!(reopened_attempt_owner, origin.client_id);
        assert_eq!(reopened_link_owner, origin.client_id);
        reopened.close().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }
}
