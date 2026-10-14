//! Bounded, durable routing from applied acceptance facts to the existing
//! exact-candidate Forge publication Operation.
//!
//! This module only reserves the Operation. Native Git execution and every
//! uncertain-effect readback remain in `store::forge`.

use super::automation_reconcile::{
    self, DomainErrorDisposition, MalformedAutomationEntry, QuarantineEvidence, QuarantinedSubject,
    SubjectDisposition, SubjectErrorDisposition,
};
use super::capacity;
use super::operation_lifecycle::{
    ExistingAutomaticOperation, ExistingAutomaticOperationLookup, OperationLifecycleState,
};
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
const QUARANTINE_PREFIX: &str = "automation:v1:publication:quarantine:";
const ACCEPTANCE_STREAM: &str = "controller:acceptance";
const MAX_ENTRIES_PER_PASS: usize = 16;
const MAX_FACTS_PER_ENTRY: usize = 16;
const MAX_PENDING: usize = 64;
const MAX_PENDING_RECHECKS: usize = 4;
const MAX_RECENT: usize = 32;
const BASE_RETRY_DELAY_MS: i64 = 1_000;
const MAX_RETRY_DELAY_MS: i64 = 60_000;

/// Root may continue after these exact publication-state failures only after
/// rolling back the publication domain transaction. All other errors stop.
pub(super) fn classify_domain_error(error: Error) -> DomainErrorDisposition {
    if matches!(
        error.code.as_str(),
        "AUTOMATION_PUBLICATION_CURSOR_CORRUPT"
            | "AUTOMATION_PUBLICATION_CURSOR_MISMATCH"
            | "AUTOMATION_PUBLICATION_STATE_CORRUPT"
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

#[derive(Debug, Clone)]
struct AcceptanceEvent {
    observation_id: i64,
    source_event_key: Option<String>,
    operation_id: Option<String>,
    payload_json: String,
}

struct PublicationInvocation<'a, 'db> {
    tx: &'a Transaction<'db>,
    launcher_config: &'a Config,
    entry: &'a AutomationEntry,
    now_ms: i64,
    forge_preparation: &'a super::forge::ForgeExecutionPreparation,
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
    forge_preparation: &super::forge::ForgeExecutionPreparation,
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

    let (entries, last_entry_key, malformed_entries) = enabled_entry_page(tx, entry_budget)?;
    for malformed in &malformed_entries {
        persist_malformed_entry(tx, malformed, now_ms)?;
    }
    let mut results = Vec::with_capacity(entries.len());
    let mut total_processed = 0usize;
    let mut total_quarantined = malformed_entries.len();
    for entry in &entries {
        let result = reconcile_entry(
            tx,
            launcher_config,
            entry,
            fact_budget,
            now_ms,
            forge_preparation,
        )?;
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

/// Read-only bounded preflight for the Forge executable preparation. It uses
/// the same selected publication-entry page and cursor/state rules as the
/// mutating reconciliation pass, so check/review-only automation passes do not
/// hash executable images when no publication reservation is due.
pub(crate) fn forge_preparation_demand(
    db: &Connection,
    entry_budget: usize,
    fact_budget: usize,
    now_ms: i64,
) -> Result<bool> {
    if now_ms < 0 {
        return Err(Error::invalid("publication preparation time is invalid"));
    }
    let entry_budget = entry_budget.min(MAX_ENTRIES_PER_PASS);
    let fact_budget = fact_budget.min(MAX_FACTS_PER_ENTRY);
    if entry_budget == 0 || fact_budget == 0 {
        return Ok(false);
    }
    // A damaged immutable entry cannot demand executable hashing. Reconciliation
    // records its exact key and payload digest in the publication transaction.
    let (entries, _, _) = enabled_entry_page(db, entry_budget)?;
    let high_water = super::automation::observation_cut(db)?;
    entries.iter().try_fold(false, |demand, entry| {
        if demand {
            return Ok(true);
        }
        publication_entry_demand(db, entry, fact_budget, now_ms, high_water)
    })
}

fn publication_entry_demand(
    db: &Connection,
    entry: &AutomationEntry,
    budget: usize,
    now_ms: i64,
    high_water: i64,
) -> Result<bool> {
    if !entry.publication_ready() || budget == 0 {
        return Ok(false);
    }
    let Some(state) = load_state(db, entry)? else {
        return Ok(false);
    };
    if state.configured_revision != entry.revision {
        return Err(Error::new(
            "AUTOMATION_PUBLICATION_CURSOR_MISMATCH",
            "publication activation cursor does not match the current automation revision",
        ));
    }
    if state
        .pending
        .iter()
        .any(|pending| pending.next_retry_at_ms <= now_ms)
    {
        return Ok(true);
    }
    if state.pending.len() >= MAX_PENDING {
        return Ok(false);
    }
    let target = state
        .catch_up_until
        .map_or(high_water, |cut| cut.min(high_water));
    if state.cursor >= target {
        return Ok(false);
    }
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM observations \
         WHERE source_stream_id=?1 AND kind='task.acceptance' \
           AND observation_id>?2 AND observation_id<=?3)",
        params![ACCEPTANCE_STREAM, state.cursor, target],
        |row| row.get(0),
    )
    .map_err(Into::into)
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
    forge_preparation: &super::forge::ForgeExecutionPreparation,
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
    let high_water = super::automation::observation_cut(tx)?;
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
            return Ok(state_projection_with_processed(&state, 0, 0, high_water));
        }
    };
    if state.configured_revision != entry.revision {
        return Err(Error::new(
            "AUTOMATION_PUBLICATION_CURSOR_MISMATCH",
            "publication activation cursor does not match the current automation revision",
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

    let invocation = PublicationInvocation {
        tx,
        launcher_config,
        entry,
        now_ms,
        forge_preparation,
    };
    let mut processed = recheck_pending(
        &invocation,
        &mut state,
        budget.min(MAX_PENDING_RECHECKS),
        &mut quarantined,
    )?;
    let remaining_budget = budget.saturating_sub(processed);
    if remaining_budget == 0 {
        state.updated_at_ms = now_ms;
        save_state(tx, &key, &state)?;
        return Ok(state_projection_with_processed(
            &state,
            processed,
            quarantined,
            high_water,
        ));
    }
    if state.pending.len() >= MAX_PENDING {
        remember_capacity_gap(&mut state, None, now_ms);
        state.updated_at_ms = now_ms;
        save_state(tx, &key, &state)?;
        return Ok(state_projection_with_processed(
            &state,
            processed,
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
            processed,
            quarantined,
            high_water,
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
        let replay = event.observation_id <= state.activation_cut
            && state.catch_up_until == Some(state.activation_cut);
        let disposition =
            consume_event_isolated(&invocation, Some(event), None, state.activation_cut, replay)?;
        match disposition {
            SubjectDisposition::Applied((subject, accepted_operation_id, result)) => {
                if publication_unresolved(&result) {
                    if state.pending.len() >= MAX_PENDING {
                        remember_capacity_gap(&mut state, Some(subject.observation_id), now_ms);
                        blocked_on_pending_capacity = true;
                        break;
                    }
                    state.pending.push(PendingAcceptance {
                        observation_id: subject.observation_id,
                        accepted_operation_id: accepted_operation_id.clone(),
                        historical_replay_authorized: replay,
                        retries: 0,
                        next_retry_at_ms: now_ms.saturating_add(BASE_RETRY_DELAY_MS),
                    });
                }
                remember_recent(
                    &mut state,
                    json!({
                        "observation_id":subject.observation_id,
                        "accepted_operation_id":accepted_operation_id,
                        "status":if result["terminal_not_applied"] == true { json!("skipped") } else { result["status"].clone() },
                        "publication_status":result["status"],
                        "automation_revision":entry.revision,
                        "code":result.get("code").cloned().unwrap_or(Value::Null),
                        "reason":result.get("reason").cloned().unwrap_or(Value::Null),
                        "operation_id":result.get("operation_id").cloned().unwrap_or(Value::Null),
                        "operation_state":result.get("operation_state").cloned().unwrap_or(Value::Null),
                        "effect_operation_id":result.get("effect_operation_id").unwrap_or(&result["operation_id"]),
                        "effect_operation_state":result.get("effect_operation_state").unwrap_or(&result["operation_state"]),
                        "publication_started":result["publication_started"] == true,
                        "coalesced":result["coalesced"] == true
                    }),
                );
                state.cursor = subject.observation_id;
                processed += 1;
            }
            SubjectDisposition::Pending { code, reason } => {
                if state.pending.len() >= MAX_PENDING {
                    remember_capacity_gap(&mut state, Some(event.observation_id), now_ms);
                    blocked_on_pending_capacity = true;
                    break;
                }
                let accepted_operation_id = event.operation_id.as_deref().ok_or_else(|| {
                    Error::new(
                        "AUTOMATION_PUBLICATION_DISPOSITION_INVALID",
                        "pending publication subject has no validated Operation identity",
                    )
                })?;
                state.pending.push(PendingAcceptance {
                    observation_id: event.observation_id,
                    accepted_operation_id: accepted_operation_id.to_owned(),
                    historical_replay_authorized: replay,
                    retries: 0,
                    next_retry_at_ms: now_ms.saturating_add(BASE_RETRY_DELAY_MS),
                });
                remember_recent(
                    &mut state,
                    event_projection(
                        event.observation_id,
                        accepted_operation_id,
                        "pending",
                        &code,
                        Some(&reason),
                    ),
                );
                state.cursor = event.observation_id;
                processed += 1;
            }
            SubjectDisposition::Skipped { code, reason } => {
                let accepted_operation_id = event.operation_id.as_deref().unwrap_or_default();
                remember_recent(
                    &mut state,
                    json!({
                        "observation_id":event.observation_id,
                        "accepted_operation_id":accepted_operation_id,
                        "status":"skipped",
                        "code":code,
                        "reason":reason,
                        "publication_started":false
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
                        "recorded_at_ms":now_ms
                    }),
                );
                state.cursor = event.observation_id;
                processed += 1;
            }
        }
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
        &state,
        processed,
        quarantined,
        high_water,
    ))
}

fn consume_event_isolated(
    invocation: &PublicationInvocation<'_, '_>,
    event: Option<&AcceptanceEvent>,
    pending: Option<&PendingAcceptance>,
    activation_cut: i64,
    historical_replay_authorized: bool,
) -> Result<SubjectDisposition<(AcceptanceEvent, String, Value)>> {
    let evidence = match (event, pending) {
        (Some(event), None) => acceptance_event_evidence(event),
        (None, Some(pending)) => pending_acceptance_evidence(pending),
        _ => {
            return Err(Error::new(
                "AUTOMATION_PUBLICATION_SUBJECT_INVALID",
                "publication subject must be exactly one new or pending acceptance",
            ));
        }
    };
    automation_reconcile::with_subject_savepoint(
        invocation.tx,
        || {
            let event = match (event, pending) {
                (Some(event), None) => event.clone(),
                (None, Some(pending)) => load_event(
                    invocation.tx,
                    pending.observation_id,
                    &pending.accepted_operation_id,
                )?,
                _ => {
                    return Err(Error::new(
                        "AUTOMATION_PUBLICATION_SUBJECT_INVALID",
                        "publication subject source changed before savepoint execution",
                    ));
                }
            };
            let Some(accepted_operation_id) = event_identity(&event)? else {
                return Ok(SubjectDisposition::Skipped {
                    code: "acceptance_fact_not_applied".to_owned(),
                    reason: "acceptance Observation is not an applied candidate".to_owned(),
                });
            };
            if let Some(pending) = pending {
                if accepted_operation_id != pending.accepted_operation_id {
                    return Err(Error::new(
                        "AUTOMATION_PUBLICATION_STATE_CORRUPT",
                        "pending acceptance Operation differs from its exact Observation",
                    ));
                }
                if (event.observation_id <= activation_cut) != historical_replay_authorized {
                    return Err(Error::new(
                        "AUTOMATION_PUBLICATION_STATE_CORRUPT",
                        "pending acceptance replay authority no longer matches its activation cut",
                    ));
                }
                if let Some(result) =
                    retained_publication_for_pending(invocation, &event, &accepted_operation_id)?
                {
                    return Ok(SubjectDisposition::Applied((
                        event,
                        accepted_operation_id,
                        result,
                    )));
                }
            }
            let context = PublicationContext::from_acceptance_observation(
                invocation.tx,
                invocation.entry,
                event.observation_id,
                &accepted_operation_id,
                activation_cut,
                historical_replay_authorized,
                invocation.launcher_config,
            )?;
            let request_value = context.request_value()?;
            let result = reserve_automatic_publication(
                invocation.tx,
                invocation.launcher_config,
                &context,
                &request_value,
                invocation.now_ms,
                invocation.forge_preparation,
            )?;
            Ok(SubjectDisposition::Applied((
                event,
                accepted_operation_id,
                result,
            )))
        },
        |error| classify_publication_subject_error(error, evidence.clone()),
    )
}

/// Resume only an exact retained publication for this pending acceptance.
/// Fresh admission remains below this check and still requires current GM
/// authority. The entry index bounds lookup to one owner/project/automation;
/// the committed loader verifies the immutable Operation link and candidate.
fn retained_publication_for_pending(
    invocation: &PublicationInvocation<'_, '_>,
    event: &AcceptanceEvent,
    accepted_operation_id: &str,
) -> Result<Option<Value>> {
    let entry = invocation.entry;
    let prefix = config::entry_operation_prefix(
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?;
    let upper_bound = format!("{};", prefix.trim_end_matches(':'));
    let mut statement = invocation.tx.prepare(
        "SELECT substr(link.key,length(?1)+1) \
         FROM meta AS link \
         JOIN operations AS op ON op.operation_id=substr(link.key,length(?1)+1) \
         WHERE link.key >= ?1 AND link.key < ?2 \
           AND op.caller_id=?3 AND op.method='forge.publish_ref' \
           AND CASE WHEN json_valid(op.effective_request_json) \
                    THEN json_extract(op.effective_request_json,'$.automation_on_behalf.effective_manager_id') END=?4 \
           AND CASE WHEN json_valid(op.effective_request_json) \
                    THEN json_extract(op.effective_request_json,'$.automation_on_behalf.project_id') END=?5 \
           AND CASE WHEN json_valid(op.effective_request_json) \
                    THEN json_extract(op.effective_request_json,'$.automation_on_behalf.automation_id') END=?6 \
           AND CASE WHEN json_valid(op.effective_request_json) \
                    THEN json_extract(op.effective_request_json,'$.automation_on_behalf.automation_revision') END=?7 \
           AND CASE WHEN json_valid(op.effective_request_json) \
                    THEN json_extract(op.effective_request_json,'$.automation_on_behalf.cause.observation_id') END=?8 \
           AND CASE WHEN json_valid(op.effective_request_json) \
                    THEN json_extract(op.effective_request_json,'$.automation_on_behalf.cause.operation_id') END=?9 \
           AND CASE WHEN json_valid(op.effective_request_json) \
                    THEN json_extract(op.effective_request_json,'$.automation_on_behalf.cause.id') END=?9 \
         ORDER BY op.operation_id LIMIT 2",
    )?;
    let operation_ids = statement
        .query_map(
            params![
                prefix,
                upper_bound,
                crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID,
                entry.owner_manager_id,
                entry.project_id,
                entry.automation_id,
                entry.revision,
                event.observation_id,
                accepted_operation_id,
            ],
            |row| row.get::<_, String>(0),
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);
    if operation_ids.len() > 1 {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "pending acceptance has multiple exact retained publication Operations",
        ));
    }
    let Some(operation_id) = operation_ids.into_iter().next() else {
        return Ok(None);
    };

    let retained = ExistingAutomaticOperation::load(
        invocation.tx,
        ExistingAutomaticOperationLookup::OperationId(&operation_id),
    )?
    .ok_or_else(|| {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "indexed publication Operation disappeared during pending recovery",
        )
    })?;
    let context = PublicationContext::from_existing_operation(invocation.tx, &retained)?;
    let linkage = context.linkage_value();
    let cause = &linkage["cause"];
    if linkage["effective_manager_id"] != entry.owner_manager_id
        || linkage["project_id"] != entry.project_id
        || linkage["automation_id"] != entry.automation_id
        || linkage["automation_revision"] != entry.revision
        || cause["observation_id"] != event.observation_id
        || cause["operation_id"] != accepted_operation_id
        || cause["id"] != accepted_operation_id
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "retained publication does not match this exact pending entry and acceptance",
        ));
    }

    let request_value = context.request_value()?;
    let original_request_json = model::canonical(&request_value)?;
    if retained.operation_id != operation_id
        || retained.caller_id != crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
        || retained.method != "forge.publish_ref"
        || retained.client_request_id != model::text(&request_value, "client_request_id")?
        || retained.original_request_json != original_request_json
        || retained.original_request != request_value
    {
        return Err(publication_state_corrupt(
            "indexed publication Operation differs from its immutable request",
        ));
    }
    classify_retained_publication(invocation.tx, retained, &context, &request_value).map(Some)
}

/// Revalidate the committed context and classify one retained publication.
/// The retained Forge intent binds the immutable request, target and candidate;
/// a result can prove application only with Forge's exact remote-readback shape.
fn classify_retained_publication(
    db: &Connection,
    retained: ExistingAutomaticOperation,
    context: &PublicationContext,
    request_value: &Value,
) -> Result<Value> {
    let intent = retained_publication_intent(&retained, context, request_value)?;
    validate_publication_cancellation(db, &retained, &intent)?;
    let coalesced_owner = match retained
        .result
        .as_ref()
        .and_then(|result| result.get("outcome"))
        .and_then(Value::as_str)
    {
        Some("coalesced") => {
            let owner_id = retained
                .result
                .as_ref()
                .and_then(|result| result.get("coalesced_to"))
                .and_then(Value::as_str)
                .ok_or_else(|| publication_state_corrupt("coalesced receipt has no owner ID"))?;
            Some(owner_id.to_owned())
        }
        _ => None,
    };
    let operation_id = retained.operation_id.clone();
    let mut classified =
        classify_publication_lifecycle(retained, &intent, coalesced_owner.as_deref())?;
    if let Some(owner_id) = coalesced_owner {
        let (owner, owner_intent) =
            validate_coalesced_owner(db, &operation_id, &owner_id, &intent)?;
        if owner
            .result
            .as_ref()
            .is_some_and(|result| result["outcome"] == "coalesced")
        {
            return Err(publication_state_corrupt(
                "coalesced Forge owner is another alias",
            ));
        }
        validate_publication_cancellation(db, &owner, &owner_intent)?;
        let owner_value = classify_publication_lifecycle(owner, &owner_intent, None)?;
        for key in [
            "status",
            "publication_started",
            "terminal_not_applied",
            "readback_required",
        ] {
            if let Some(value) = owner_value.get(key) {
                classified[key] = value.clone();
            }
        }
        classified["effect_operation_id"] = json!(owner_id);
        classified["effect_operation_state"] = owner_value["operation_state"].clone();
        classified["effect_receipt"] = owner_value["receipt"].clone();
    }
    if classified["terminal_not_applied"] == true {
        classified["code"] = classified["status"].clone();
        classified["reason"] = json!(
            "retained publication effect is terminal without an applied remote readback; this automation revision will not replay it"
        );
    }
    Ok(classified)
}

fn publication_unresolved(value: &Value) -> bool {
    matches!(
        value["status"].as_str(),
        Some("publication_reserved" | "publication_in_flight" | "publication_readback_required")
    )
}

fn validate_publication_cancellation(
    db: &Connection,
    retained: &ExistingAutomaticOperation,
    intent: &crate::forge::PublicationIntent,
) -> Result<()> {
    if retained.state != OperationLifecycleState::Cancelled {
        return Ok(());
    }
    let receipt = retained.result.as_ref().ok_or_else(|| {
        publication_state_corrupt("cancelled Forge Operation has no cancellation receipt")
    })?;
    validate_cancelled_receipt(
        receipt,
        &retained.operation_id,
        intent,
        retained.sent_at_ms.is_some(),
    )?;
    let cancellation_id = model::text(receipt, "cancelled_by")?;
    let cancellation = ExistingAutomaticOperation::load(
        db,
        ExistingAutomaticOperationLookup::OperationId(cancellation_id),
    )?
    .ok_or_else(|| {
        publication_state_corrupt("Forge cancellation references a missing Operation")
    })?;
    if cancellation.method != "operation.cancel"
        || cancellation.state != OperationLifecycleState::Settled
        || cancellation.result.as_ref().is_none_or(|result| {
            result["operation_id"] != cancellation_id
                || result["cancelled_operation_id"] != retained.operation_id
                || result["native_cancel_sent"] != false
        })
        || cancellation.original_request["operation_id"] != retained.operation_id
        || cancellation.original_request["reason"] != receipt["reason"]
        || cancellation.original_request["client_request_id"] != cancellation.client_request_id
    {
        return Err(publication_state_corrupt(
            "Forge cancellation does not prove the exact unsent target was cancelled",
        ));
    }
    Ok(())
}

fn retained_publication_intent(
    retained: &ExistingAutomaticOperation,
    context: &PublicationContext,
    request_value: &Value,
) -> Result<crate::forge::PublicationIntent> {
    if retained.method != "forge.publish_ref" {
        return Err(publication_state_corrupt(
            "retained publication intent belongs to a different Operation method",
        ));
    }
    if retained.client_request_id != model::text(request_value, "client_request_id")?
        || retained.original_request_json != model::canonical(request_value)?
        || retained.original_request != *request_value
    {
        return Err(publication_state_corrupt(
            "retained Forge Operation differs from its exact semantic request",
        ));
    }
    let effective = &retained.effective_request;
    let intent: crate::forge::PublicationIntent =
        serde_json::from_value(effective["publication_intent"].clone()).map_err(|_| {
            publication_state_corrupt("retained Forge publication intent is invalid")
        })?;
    validate_publication_intent(&intent, &retained.operation_id, context, request_value)?;
    if effective["automation_on_behalf"] != context.linkage_value() {
        return Err(publication_state_corrupt(
            "retained Forge attribution differs from its accepted publication context",
        ));
    }
    Ok(intent)
}

fn validate_publication_intent(
    intent: &crate::forge::PublicationIntent,
    operation_id: &str,
    context: &PublicationContext,
    request_value: &Value,
) -> Result<()> {
    intent
        .validate()
        .map_err(|_| publication_state_corrupt("retained Forge intent failed validation"))?;
    if context.request_value()? != *request_value
        || intent.operation_id != operation_id
        || intent.project_id != context.project_id()
        || intent.canonical_repository != context.canonical_repository()
        || intent.attempt_id != context.attempt_id()
        || intent.task_revision != context.task_revision()
        || intent.admitted_gm_epoch != context.gm_epoch()
        || intent.submission_ref != context.submission_ref()
        || intent.accepted_operation_id != context.accepted_operation_id()
        || intent.candidate_ref != context.candidate_ref()
        || intent.policy_revision != context.policy_revision()
        || intent.target_ref != context.target_ref()
        || intent.expected_old_ref.as_deref() != context.expected_old_ref()
        || intent.expected_create != context.expected_create()
        || intent.force
    {
        return Err(publication_state_corrupt(
            "retained Forge intent differs from its exact accepted request or target",
        ));
    }
    Ok(())
}

fn validate_coalesced_owner(
    db: &Connection,
    operation_id: &str,
    owner_id: &str,
    intent: &crate::forge::PublicationIntent,
) -> Result<(ExistingAutomaticOperation, crate::forge::PublicationIntent)> {
    if owner_id.is_empty() || owner_id == operation_id {
        return Err(publication_state_corrupt(
            "coalesced receipt does not identify a distinct retained owner",
        ));
    }
    let owner = ExistingAutomaticOperation::load(
        db,
        ExistingAutomaticOperationLookup::OperationId(owner_id),
    )?
    .ok_or_else(|| {
        publication_state_corrupt("coalesced receipt references a missing Forge owner")
    })?;
    if owner.method != "forge.publish_ref" {
        return Err(publication_state_corrupt(
            "coalesced receipt references a non-Forge Operation",
        ));
    }
    if owner.caller_id.trim().is_empty()
        || owner.client_request_id.trim().is_empty()
        || !owner.has_unbound_execution_scope()
        || owner.original_request["client_request_id"] != owner.client_request_id
        || owner.original_request_json != model::canonical(&owner.original_request)?
        || owner.effective_request_json != model::canonical(&owner.effective_request)?
    {
        return Err(publication_state_corrupt(
            "coalesced Forge owner has an invalid caller or immutable request identity",
        ));
    }
    let effective = &owner.effective_request;
    let owner_intent: crate::forge::PublicationIntent =
        serde_json::from_value(effective["publication_intent"].clone()).map_err(|_| {
            publication_state_corrupt("coalesced Forge owner has no valid publication intent")
        })?;
    owner_intent
        .validate()
        .map_err(|_| publication_state_corrupt("coalesced Forge owner intent is invalid"))?;
    if owner_intent.operation_id != owner_id || !same_publication_slot(intent, &owner_intent) {
        return Err(publication_state_corrupt(
            "coalesced Forge owner does not retain the same exact effect slot",
        ));
    }
    let request = crate::forge::PublishRefRequest::parse(&owner.original_request)
        .map_err(|_| publication_state_corrupt("coalesced Forge owner request is invalid"))?;
    if owner_intent.attempt_id != request.attempt_id
        || owner_intent.task_revision != request.expected_revision
        || owner_intent.submission_ref != request.submission_ref
        || owner_intent.accepted_operation_id != request.accepted_operation_id
        || owner_intent.candidate_ref != request.candidate_ref
        || owner_intent.policy_revision != request.expected_policy_revision
        || owner_intent.target_ref != request.target_ref
        || owner_intent.expected_old_ref != request.expected_old_ref
        || owner_intent.expected_create != request.expected_create
    {
        return Err(publication_state_corrupt(
            "coalesced Forge owner intent differs from its immutable request",
        ));
    }
    Ok((owner, owner_intent))
}

fn same_publication_slot(
    left: &crate::forge::PublicationIntent,
    right: &crate::forge::PublicationIntent,
) -> bool {
    left.project_id == right.project_id
        && left.canonical_repository == right.canonical_repository
        && left.candidate_ref == right.candidate_ref
        && left.candidate_sha256 == right.candidate_sha256
        && left.commit == right.commit
        && left.tree == right.tree
        && left.target_ref == right.target_ref
        && left.expected_old_ref == right.expected_old_ref
        && left.expected_create == right.expected_create
}

fn classify_publication_lifecycle(
    retained: ExistingAutomaticOperation,
    intent: &crate::forge::PublicationIntent,
    verified_coalesced_owner: Option<&str>,
) -> Result<Value> {
    let operation_id = retained.operation_id;
    let sent = retained.sent_at_ms.is_some();
    let result = retained.result;
    let publication_started = sent
        && result
            .as_ref()
            .and_then(|receipt| receipt.get("publication_may_have_started"))
            .and_then(Value::as_bool)
            == Some(true);
    let mut value = json!({
        "operation_id":operation_id,
        "operation_state":retained.state.as_str(),
        "publication_started":false,
    });
    let object = value
        .as_object_mut()
        .expect("publication classification is an object");
    match retained.state {
        OperationLifecycleState::Queued => {
            if let Some(result) = result.as_ref() {
                validate_queued_forge_receipt(result, &operation_id, intent)?;
            }
            object.insert("status".to_owned(), json!("publication_in_flight"));
            object.insert("coalesced".to_owned(), json!(false));
        }
        OperationLifecycleState::Sending | OperationLifecycleState::NativeAccepted => {
            let result = result.as_ref().ok_or_else(|| {
                publication_state_corrupt("dispatched Forge Operation has no retained receipt")
            })?;
            validate_dispatch_receipt(result, &operation_id)?;
            object.insert("status".to_owned(), json!("publication_in_flight"));
            object.insert("publication_started".to_owned(), json!(publication_started));
            object.insert("coalesced".to_owned(), json!(false));
        }
        OperationLifecycleState::OutcomeUnknown => {
            let result = result.as_ref().ok_or_else(|| {
                publication_state_corrupt(
                    "unknown Forge Operation has no retained readback evidence",
                )
            })?;
            validate_unknown_receipt(result, &operation_id)?;
            object.insert("status".to_owned(), json!("publication_readback_required"));
            object.insert("publication_started".to_owned(), json!(publication_started));
            object.insert("coalesced".to_owned(), json!(false));
            object.insert("readback_required".to_owned(), json!(true));
        }
        OperationLifecycleState::Cancelled => {
            let result = result.as_ref().ok_or_else(|| {
                publication_state_corrupt("cancelled Forge Operation has no cancellation receipt")
            })?;
            validate_cancelled_receipt(result, &operation_id, intent, sent)?;
            object.insert("status".to_owned(), json!("publication_cancelled"));
            object.insert("coalesced".to_owned(), json!(false));
            object.insert("terminal_not_applied".to_owned(), json!(true));
        }
        OperationLifecycleState::Rejected => {
            let result = result.as_ref().ok_or_else(|| {
                publication_state_corrupt("rejected Forge Operation has no structured reason")
            })?;
            validate_rejected_receipt(result, &operation_id, sent)?;
            object.insert("status".to_owned(), json!("publication_rejected"));
            object.insert("coalesced".to_owned(), json!(false));
            object.insert("terminal_not_applied".to_owned(), json!(true));
        }
        OperationLifecycleState::Settled => {
            if retained.settled_at_ms.is_none() {
                return Err(publication_state_corrupt(
                    "settled Forge Operation has no settlement timestamp",
                ));
            }
            let result = result.as_ref().ok_or_else(|| {
                publication_state_corrupt("settled Forge Operation has no domain result")
            })?;
            let classification = validate_settled_receipt(
                result,
                &operation_id,
                intent,
                sent,
                verified_coalesced_owner,
            )?;
            object.insert("status".to_owned(), json!(classification.status));
            object.insert(
                "publication_started".to_owned(),
                json!(classification.publication_started),
            );
            object.insert("coalesced".to_owned(), json!(classification.coalesced));
            object.insert(
                "terminal_not_applied".to_owned(),
                json!(classification.terminal_not_applied),
            );
            object.insert("outcome".to_owned(), json!(classification.outcome));
        }
    }
    if let Some(result) = result {
        object.insert("receipt".to_owned(), result);
    }
    Ok(value)
}

struct SettledPublicationClassification {
    status: &'static str,
    outcome: &'static str,
    publication_started: bool,
    terminal_not_applied: bool,
    coalesced: bool,
}

fn validate_settled_receipt(
    result: &Value,
    operation_id: &str,
    intent: &crate::forge::PublicationIntent,
    sent: bool,
    verified_coalesced_owner: Option<&str>,
) -> Result<SettledPublicationClassification> {
    require_operation_identity(result, operation_id)?;
    if result["force"] != false {
        return Err(publication_state_corrupt(
            "settled Forge result is missing the non-force proof",
        ));
    }
    match result["outcome"].as_str() {
        Some("applied") => {
            if result["publication"] != "confirmed_by_remote_readback"
                || result["remote_ref"]["present"] != true
                || result["remote_ref"]["commit"]
                    .as_str()
                    .is_none_or(|commit| !commit.eq_ignore_ascii_case(&intent.commit))
                || result["publication_may_have_started"] != true
                || result["acceptance_current_at_finish"].as_bool().is_none()
                || !sent
            {
                return Err(publication_state_corrupt(
                    "applied Forge result lacks the exact accepted target remote readback",
                ));
            }
            Ok(SettledPublicationClassification {
                status: "publication_applied",
                outcome: "applied",
                publication_started: true,
                terminal_not_applied: false,
                coalesced: false,
            })
        }
        Some("coalesced") => {
            validate_coalesced_forge_receipt(result, operation_id, verified_coalesced_owner)?;
            if sent {
                return Err(publication_state_corrupt(
                    "coalesced Forge Operation has a dispatch timestamp",
                ));
            }
            Ok(SettledPublicationClassification {
                status: "publication_not_applied",
                outcome: "coalesced",
                publication_started: false,
                terminal_not_applied: true,
                coalesced: true,
            })
        }
        Some("stale_gm_epoch") => {
            validate_stale_gm_epoch_receipt(result, operation_id, intent)?;
            if sent {
                return Err(publication_state_corrupt(
                    "stale-GM Forge result is inconsistent with a sent publication",
                ));
            }
            Ok(SettledPublicationClassification {
                status: "publication_not_applied",
                outcome: "stale_gm_epoch",
                publication_started: false,
                terminal_not_applied: true,
                coalesced: false,
            })
        }
        Some("failed") => {
            let publication = result["publication"].as_str();
            let attempted = result["publication_may_have_started"].as_bool();
            validate_structured_error(&result["error"])?;
            match (publication, attempted, sent) {
                (Some("not_started"), Some(false), false) => {}
                (Some("not_confirmed"), Some(_), true)
                    if result["acceptance_current_at_finish"].as_bool().is_some() => {}
                _ => {
                    return Err(publication_state_corrupt(
                        "failed Forge result has incoherent attempt evidence",
                    ));
                }
            }
            Ok(SettledPublicationClassification {
                status: "publication_not_applied",
                outcome: "failed",
                publication_started: sent && attempted == Some(true),
                terminal_not_applied: true,
                coalesced: false,
            })
        }
        _ => Err(publication_state_corrupt(
            "settled Forge result has an unknown domain outcome",
        )),
    }
}

fn validate_queued_forge_receipt(
    result: &Value,
    operation_id: &str,
    intent: &crate::forge::PublicationIntent,
) -> Result<()> {
    if result["operation_id"] != operation_id
        || result["attempt_id"] != intent.attempt_id
        || result["state"] != "queued"
        || result["publication"] != "not_started"
        || result["force"] != false
    {
        return Err(publication_state_corrupt(
            "queued Forge receipt differs from its immutable admission",
        ));
    }
    Ok(())
}

fn validate_dispatch_receipt(result: &Value, operation_id: &str) -> Result<()> {
    require_operation_identity(result, operation_id)?;
    if result["force"] != false || result["publication_may_have_started"].as_bool().is_none() {
        return Err(publication_state_corrupt(
            "dispatched Forge receipt has invalid attempted-effect evidence",
        ));
    }
    Ok(())
}

fn validate_unknown_receipt(result: &Value, operation_id: &str) -> Result<()> {
    require_operation_identity(result, operation_id)?;
    if result["force"] != false
        || result["publication_may_have_started"].as_bool().is_none()
        || !(result["outcome"] == "unknown" || result["process_tree_unconfirmed"] == true)
    {
        return Err(publication_state_corrupt(
            "unknown Forge receipt has no exact readback or interrupted-dispatch evidence",
        ));
    }
    if result["outcome"] == "unknown"
        && (result["reason"].as_str().is_none_or(str::is_empty)
            || !matches!(
                result["publication"].as_str(),
                Some("requires_readback" | "operator_intervention_required")
            ))
    {
        return Err(publication_state_corrupt(
            "unknown Forge outcome has an invalid readback disposition",
        ));
    }
    Ok(())
}

fn validate_cancelled_receipt(
    result: &Value,
    operation_id: &str,
    intent: &crate::forge::PublicationIntent,
    sent: bool,
) -> Result<()> {
    model::fields(result, &["reason", "cancelled_by", "previous_result"])
        .map_err(|_| publication_state_corrupt("cancelled Forge receipt has unknown fields"))?;
    if result["reason"].as_str().is_none_or(str::is_empty)
        || result["cancelled_by"].as_str().is_none_or(str::is_empty)
    {
        return Err(publication_state_corrupt(
            "cancelled Forge receipt lacks its exact reason or cancelling Operation",
        ));
    }
    if let Some(previous) = result.get("previous_result") {
        validate_stale_gm_epoch_receipt(previous, operation_id, intent)?;
    }
    if sent {
        return Err(publication_state_corrupt(
            "cancelled Forge Operation has contradictory dispatch evidence",
        ));
    }
    Ok(())
}

fn validate_rejected_receipt(result: &Value, operation_id: &str, sent: bool) -> Result<()> {
    if sent {
        return Err(publication_state_corrupt(
            "rejected Forge Operation has dispatch evidence without a domain outcome",
        ));
    }
    if result
        .get("operation_id")
        .is_some_and(|value| value != operation_id)
    {
        return Err(publication_state_corrupt(
            "rejected Forge result identifies a different Operation",
        ));
    }
    let error = result.get("error").unwrap_or(result);
    validate_structured_error(error)
}

fn validate_stale_gm_epoch_receipt(
    result: &Value,
    operation_id: &str,
    intent: &crate::forge::PublicationIntent,
) -> Result<()> {
    require_operation_identity(result, operation_id)?;
    if result["outcome"] != "stale_gm_epoch"
        || result["publication"] != "not_started"
        || result["force"] != false
        || result["admitted_gm_epoch"] != intent.admitted_gm_epoch
        || result["current_gm_epoch"]
            .as_i64()
            .is_none_or(|epoch| epoch < 0 || epoch == intent.admitted_gm_epoch)
        || result
            .get("publication_may_have_started")
            .is_some_and(|value| value != false)
    {
        return Err(publication_state_corrupt(
            "stale-GM Forge result lacks its structured no-write proof",
        ));
    }
    Ok(())
}

fn validate_coalesced_forge_receipt(
    result: &Value,
    operation_id: &str,
    verified_owner: Option<&str>,
) -> Result<()> {
    let owner = result["coalesced_to"].as_str();
    if result["operation_id"] != operation_id
        || result["outcome"] != "coalesced"
        || result["state"] != "settled"
        || result["publication"] != "coalesced"
        || result["coalesced"] != true
        || result["publication_may_have_started"] != false
        || result["force"] != false
        || owner.is_none_or(str::is_empty)
        || owner != verified_owner
    {
        return Err(publication_state_corrupt(
            "coalesced Forge result has no matching retained same-slot owner",
        ));
    }
    Ok(())
}

fn require_operation_identity(result: &Value, operation_id: &str) -> Result<()> {
    if !result.is_object() || result["operation_id"] != operation_id {
        return Err(publication_state_corrupt(
            "retained Forge result does not identify its exact Operation",
        ));
    }
    Ok(())
}

fn validate_structured_error(error: &Value) -> Result<()> {
    if error["code"].as_str().is_none_or(str::is_empty)
        || error["message"].as_str().is_none_or(str::is_empty)
    {
        return Err(publication_state_corrupt(
            "Forge failure has no structured error code and message",
        ));
    }
    Ok(())
}

fn publication_state_corrupt(message: &str) -> Error {
    Error::new("AUTOMATION_PUBLICATION_STATE_CORRUPT", message)
}

fn reserve_automatic_publication(
    tx: &Transaction<'_>,
    launcher_config: &Config,
    context: &PublicationContext,
    request_value: &Value,
    now_ms: i64,
    forge_preparation: &super::forge::ForgeExecutionPreparation,
) -> Result<Value> {
    let caller = context.technical_requester_id();
    let request_id = model::text(request_value, "client_request_id")?;
    let original_json = model::canonical(request_value)?;
    let existing = ExistingAutomaticOperation::load(
        tx,
        ExistingAutomaticOperationLookup::ClientRequest {
            caller_id: caller,
            client_request_id: request_id,
        },
    )?;
    if let Some(existing) = existing {
        if existing.caller_id != caller
            || existing.client_request_id != request_id
            || existing.method != "forge.publish_ref"
            || existing.original_request_json != original_json
            || existing.original_request != *request_value
        {
            return Err(Error::new(
                "PUBLICATION_SLOT_CONFLICT",
                "this automatic publication request ID already retains different inputs",
            ));
        }
        let retained_context = PublicationContext::from_existing_operation(tx, &existing)?;
        if retained_context.request_value()? != *request_value {
            return Err(Error::new(
                "PUBLICATION_SLOT_CONFLICT",
                "existing automatic request does not retain this exact accepted-candidate slot",
            ));
        }
        return classify_retained_publication(tx, existing, &retained_context, request_value);
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
        Some(forge_preparation),
    )?;
    let initial = ExistingAutomaticOperation::load(
        tx,
        ExistingAutomaticOperationLookup::OperationId(&operation_id),
    )?
    .ok_or_else(|| publication_state_corrupt("new Forge Operation disappeared during admission"))?;
    if initial.caller_id != caller
        || initial.client_request_id != request_id
        || initial.method != "forge.publish_ref"
        || initial.original_request_json != original_json
        || initial.original_request != *request_value
    {
        return Err(publication_state_corrupt(
            "new Forge Operation differs from its immutable admission",
        ));
    }
    let intent = retained_publication_intent(&initial, context, request_value)?;
    let coalesced_to = if reserved["outcome"] == "coalesced" {
        let owner_id = reserved["coalesced_to"]
            .as_str()
            .ok_or_else(|| publication_state_corrupt("coalesced receipt has no owner ID"))?;
        validate_coalesced_owner(tx, &operation_id, owner_id, &intent)?;
        validate_coalesced_forge_receipt(&reserved, &operation_id, Some(owner_id))?;
        Some(owner_id.to_owned())
    } else {
        validate_queued_forge_receipt(&reserved, &operation_id, &intent)?;
        None
    };
    let coalesced = coalesced_to.is_some();
    let mut effective = initial.effective_request;
    effective["automation_on_behalf"] = context.linkage_value();
    effective["receipt"] = json!({"ok":true,"value":reserved});
    let effective_json = model::canonical(&effective)?;
    let changed = if coalesced {
        tx.execute(
            "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3,effective_request_json=?4 \
             WHERE operation_id=?1 AND method='forge.publish_ref' AND state='queued' AND sent_at_ms IS NULL AND settled_at_ms IS NULL",
            params![operation_id, model::canonical(&reserved)?, now_ms, effective_json],
        )?
    } else {
        tx.execute(
            "UPDATE operations SET result_json=?2,updated_at_ms=?3,effective_request_json=?4 \
             WHERE operation_id=?1 AND method='forge.publish_ref' AND state='queued' AND sent_at_ms IS NULL AND settled_at_ms IS NULL",
            params![operation_id, model::canonical(&reserved)?, now_ms, effective_json],
        )?
    };
    if changed != 1 {
        return Err(Error::conflict(
            "fresh publication reservation changed before its receipt was retained",
        ));
    }
    capacity::sync_operation(tx, &operation_id, now_ms)?;
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
         VALUES('controller',?1,?1,'forge.publish_ref',?2,?3)",
        params![operation_id, model::canonical(&reserved)?, now_ms],
    )?;
    save_operation_link(tx, context, &operation_id, now_ms)?;
    let retained = ExistingAutomaticOperation::load(
        tx,
        ExistingAutomaticOperationLookup::OperationId(&operation_id),
    )?
    .ok_or_else(|| publication_state_corrupt("reserved Forge Operation disappeared"))?;
    let mut classified = classify_retained_publication(tx, retained, context, request_value)?;
    if !coalesced {
        classified["status"] = json!("publication_reserved");
    }
    Ok(classified)
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
    invocation: &PublicationInvocation<'_, '_>,
    state: &mut PublicationState,
    budget: usize,
    quarantined: &mut usize,
) -> Result<usize> {
    let now_ms = invocation.now_ms;
    let mut processed = 0usize;
    while processed < budget {
        let Some(index) = state
            .pending
            .iter()
            .position(|pending| pending.next_retry_at_ms <= now_ms)
        else {
            break;
        };
        let pending = state.pending[index].clone();
        let replay = pending.historical_replay_authorized;
        let disposition = consume_event_isolated(
            invocation,
            None,
            Some(&pending),
            state.activation_cut,
            replay,
        )?;
        match disposition {
            SubjectDisposition::Applied((event, accepted_operation_id, result)) => {
                if publication_unresolved(&result) {
                    let pending = &mut state.pending[index];
                    pending.retries = pending.retries.saturating_add(1);
                    pending.next_retry_at_ms = now_ms.saturating_add(retry_delay(pending.retries));
                } else {
                    state.pending.remove(index);
                }
                processed += 1;
                remember_recent(
                    state,
                    json!({
                        "observation_id":event.observation_id,
                        "accepted_operation_id":accepted_operation_id,
                        "status":if result["terminal_not_applied"] == true { json!("skipped") } else { result["status"].clone() },
                        "publication_status":result["status"],
                        "automation_revision":invocation.entry.revision,
                        "code":result.get("code").cloned().unwrap_or(Value::Null),
                        "reason":result.get("reason").cloned().unwrap_or(Value::Null),
                        "operation_id":result.get("operation_id").cloned().unwrap_or(Value::Null),
                        "operation_state":result.get("operation_state").cloned().unwrap_or(Value::Null),
                        "effect_operation_id":result.get("effect_operation_id").unwrap_or(&result["operation_id"]),
                        "effect_operation_state":result.get("effect_operation_state").unwrap_or(&result["operation_state"]),
                        "publication_started":result["publication_started"] == true,
                        "coalesced":result["coalesced"] == true
                    }),
                );
            }
            SubjectDisposition::Pending { code, reason } => {
                let projection = {
                    let pending = &mut state.pending[index];
                    pending.retries = pending.retries.saturating_add(1);
                    pending.next_retry_at_ms = now_ms.saturating_add(retry_delay(pending.retries));
                    event_projection(
                        pending.observation_id,
                        &pending.accepted_operation_id,
                        "pending",
                        &code,
                        Some(&reason),
                    )
                };
                processed += 1;
                remember_recent(state, projection);
            }
            SubjectDisposition::Skipped { code, reason } => {
                state.pending.remove(index);
                processed += 1;
                remember_recent(
                    state,
                    event_projection(
                        pending.observation_id,
                        &pending.accepted_operation_id,
                        "skipped",
                        &code,
                        Some(&reason),
                    ),
                );
            }
            SubjectDisposition::Quarantined { code, evidence } => {
                persist_subject_quarantine(invocation.tx, &code, evidence, now_ms)?;
                *quarantined = (*quarantined).saturating_add(1);
                state.pending.remove(index);
                processed += 1;
                remember_recent(
                    state,
                    json!({
                        "observation_id":pending.observation_id,
                        "status":"quarantined",
                        "code":code,
                        "recorded_at_ms":now_ms
                    }),
                );
            }
        }
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
           AND source_event_key=?3 AND operation_id=?4",
        params![
            observation_id,
            ACCEPTANCE_STREAM,
            format!("accept:{accepted_operation_id}"),
            accepted_operation_id
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
        "FORGE_PROCESS_TREE_UNCONFIRMED"
            | "FORGE_PUBLICATION_SLOT_BUSY"
            | "FORGE_EXECUTION_PREPARATION_MISSING"
            | "FORGE_WORKER_IMAGE_INVALID"
    )
}

fn classify_publication_subject_error(
    error: &Error,
    evidence: QuarantineEvidence,
) -> Option<SubjectErrorDisposition> {
    if pending_context_error(error) || pending_reservation_error(error) {
        return Some(SubjectErrorDisposition::Pending {
            code: error.code.clone(),
            reason: "an exact current authority or Forge prerequisite may become available later"
                .to_owned(),
        });
    }
    if skip_context_error(error) {
        return Some(SubjectErrorDisposition::Skipped {
            code: error.code.clone(),
            reason: "the exact accepted candidate is no longer selected or applicable".to_owned(),
        });
    }
    match error.code.as_str() {
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

fn acceptance_event_evidence(event: &AcceptanceEvent) -> QuarantineEvidence {
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

fn pending_acceptance_evidence(pending: &PendingAcceptance) -> QuarantineEvidence {
    QuarantineEvidence {
        subject_identity: format!(
            "acceptance-observation:{}:operation:{}",
            pending.observation_id,
            model::digest(pending.accepted_operation_id.as_bytes())
        ),
        source_pointer: Some(format!("observations/{}", pending.observation_id)),
        source_digest: None,
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
            "AUTOMATION_PUBLICATION_QUARANTINE_INVALID",
            "publication subject did not produce durable quarantine evidence",
        )),
    }
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
    let cursor_value = config::read_record(db, GLOBAL_CURSOR_KEY, "publication global cursor")
        .map_err(|error| {
            if error.code == "AUTOMATION_RECORD_CORRUPT" {
                Error::new(
                    "AUTOMATION_PUBLICATION_CURSOR_CORRUPT",
                    "publication global cursor record is corrupt",
                )
            } else {
                error
            }
        })?;
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
        if entry_quarantine_matches(db, &key, &raw)? {
            continue;
        }
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
        if entry.enabled && entry.steps.contains(&AutomationStep::Publication) {
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
    if quarantine_marker_matches(tx, &record_key, &malformed.code, &malformed.evidence)? {
        return Ok(());
    }
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
            "AUTOMATION_PUBLICATION_QUARANTINE_INVALID",
            "malformed publication entry did not produce a durable quarantine",
        )),
    }
}

/// Read the exact typed quarantine marker without refreshing its occurrence
/// count or timestamps. A malformed or conflicting marker remains fatal.
pub(super) fn quarantine_marker_matches(
    db: &Connection,
    record_key: &str,
    code: &str,
    evidence: &QuarantineEvidence,
) -> Result<bool> {
    let Some(value) = config::read_record(db, record_key, "automation quarantine")? else {
        return Ok(false);
    };
    let record: QuarantinedSubject = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "AUTOMATION_QUARANTINE_RECORD_CORRUPT",
            "retained automation quarantine evidence is invalid",
        )
    })?;
    if record.code != code || record.evidence != *evidence {
        return Err(Error::new(
            "AUTOMATION_QUARANTINE_IDENTITY_CONFLICT",
            "quarantine metadata key resolves to different subject evidence",
        ));
    }
    if record.first_seen_at_ms < 0
        || record.last_seen_at_ms < record.first_seen_at_ms
        || record.occurrences == 0
    {
        return Err(Error::new(
            "AUTOMATION_QUARANTINE_RECORD_CORRUPT",
            "retained automation quarantine bounds are invalid",
        ));
    }
    Ok(true)
}

/// Return whether this exact malformed entry has already been isolated by the
/// publication owner. The key and digest derive from the retained raw bytes.
pub(super) fn entry_quarantine_matches(db: &Connection, key: &str, raw: &str) -> Result<bool> {
    let evidence = automation_reconcile::automation_entry_evidence(key, raw);
    let code = match automation_reconcile::parse_automation_entry(raw, "automation entry") {
        Ok(entry) => {
            if config::entry_key(
                &entry.owner_manager_id,
                &entry.project_id,
                &entry.automation_id,
            )? == key
            {
                return Ok(false);
            }
            "AUTOMATION_RECORD_INVALID".to_owned()
        }
        Err(error)
            if matches!(
                error.code.as_str(),
                "AUTOMATION_RECORD_CORRUPT" | "AUTOMATION_RECORD_INVALID"
            ) =>
        {
            error.code
        }
        Err(error) => return Err(error),
    };
    let record_key = automation_reconcile::quarantine_record_key(QUARANTINE_PREFIX, &evidence)?;
    quarantine_marker_matches(db, &record_key, &code, &evidence)
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
        "SELECT key FROM meta WHERE key LIKE ?1 AND key>?2 AND key<=?3 ORDER BY key LIMIT ?4",
    )?;
    Ok(statement
        .query_map(params![pattern, prefix, before, limit as i64], |row| {
            row.get(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn load_state(db: &Connection, entry: &AutomationEntry) -> Result<Option<PublicationState>> {
    let Some(value) =
        config::read_record(db, &state_key(entry)?, "publication state").map_err(|error| {
            if error.code == "AUTOMATION_RECORD_CORRUPT" {
                Error::new(
                    "AUTOMATION_PUBLICATION_STATE_CORRUPT",
                    "publication state record is corrupt",
                )
            } else {
                error
            }
        })?
    else {
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

pub(super) fn relocate_state(
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
            "publication relocation must preserve project and automation identity while changing owner",
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
        return Err(Error::conflict("publication target state already exists"));
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
            "AUTOMATION_PUBLICATION_STATE_MISSING",
            "publication source state changed during relocation",
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
    quarantined: usize,
    high_water: i64,
) -> Value {
    let mut value = state_projection(state);
    value["processed"] = json!(processed);
    value["quarantined"] = json!(quarantined);
    value["high_water"] = json!(high_water);
    value
}

#[cfg(test)]
mod tests {
    use super::{ExistingAutomaticOperation, classify_publication_lifecycle};
    use crate::forge::PublicationIntent;
    use serde_json::{Value, json};

    fn intent() -> PublicationIntent {
        PublicationIntent {
            operation_id: "op-publication".to_owned(),
            project_id: "project-1".to_owned(),
            canonical_repository: "github.com/owner/repo".to_owned(),
            attempt_id: "attempt-1".to_owned(),
            task_revision: 1,
            admitted_gm_epoch: 0,
            submission_ref: "submission-1".to_owned(),
            accepted_operation_id: "acceptance-1".to_owned(),
            candidate_ref: "candidate-1".to_owned(),
            candidate_sha256: "a".repeat(64),
            commit: "b".repeat(40),
            tree: "c".repeat(40),
            remote_name: "origin".to_owned(),
            target_ref: "refs/heads/main".to_owned(),
            expected_old_ref: Some("d".repeat(40)),
            expected_create: false,
            force: false,
            policy_revision: "owner-policy-v1".to_owned(),
        }
    }

    /// Classify one typed retained row, including its actual dispatch and
    /// terminal timestamps.
    fn classify(
        state: &str,
        result: Option<Value>,
        sent_at_ms: Option<i64>,
        settled_at_ms: Option<i64>,
        verified_coalesced_owner: Option<&str>,
    ) -> super::Result<Value> {
        let retained = ExistingAutomaticOperation::decode(
            "op-publication".to_owned(),
            state.to_owned(),
            result.map(|value| value.to_string()),
            sent_at_ms,
            settled_at_ms,
        )?;
        classify_publication_lifecycle(retained, &intent(), verified_coalesced_owner)
    }

    /// The exact applied receipt `store::forge` retains after a confirmed remote
    /// readback.
    fn applied_publication_receipt() -> Value {
        let commit = intent().commit;
        json!({
            "operation_id":"op-publication",
            "outcome":"applied",
            "publication":"confirmed_by_remote_readback",
            "publication_may_have_started":true,
            "acceptance_current_at_finish":true,
            "remote_ref":{"present":true,"commit":commit},
            "force":false
        })
    }

    #[test]
    fn cancelled_publication_is_not_coalesced_success() {
        let classified = classify(
            "cancelled",
            Some(json!({"reason":"cancelled before dispatch","cancelled_by":"op-cancel"})),
            None,
            Some(1),
            None,
        )
        .unwrap();
        assert_eq!(classified["status"], "publication_cancelled");
        assert_eq!(classified["terminal_not_applied"], true);
        assert_eq!(classified["coalesced"], false);
        assert_eq!(classified["publication_started"], false);
    }

    #[test]
    fn cancelled_stale_epoch_publication_preserves_its_prior_result() {
        let classified = classify(
            "cancelled",
            Some(json!({
                "reason":"accepted publication became stale",
                "cancelled_by":"op-cancel",
                "previous_result":{
                    "operation_id":"op-publication",
                    "outcome":"stale_gm_epoch",
                    "publication":"not_started",
                    "admitted_gm_epoch":0,
                    "current_gm_epoch":1,
                    "force":false
                }
            })),
            None,
            Some(1),
            None,
        )
        .unwrap();

        assert_eq!(classified["status"], "publication_cancelled");
        assert_eq!(
            classified["receipt"]["previous_result"]["outcome"],
            "stale_gm_epoch"
        );
    }

    #[test]
    fn rejected_publication_is_not_replayed() {
        let classified = classify(
            "rejected",
            Some(json!({"code":"FORBIDDEN","message":"scope denied"})),
            None,
            Some(1),
            None,
        )
        .unwrap();
        assert_eq!(classified["status"], "publication_rejected");
        assert_eq!(classified["terminal_not_applied"], true);
        assert_eq!(classified["coalesced"], false);
    }

    #[test]
    fn settled_stale_epoch_publication_is_not_applied() {
        // A settled state carrying a no-write outcome is a terminal result that
        // did not apply, so it must never be reported as a satisfied publication.
        let classified = classify(
            "settled",
            Some(json!({
                "operation_id":"op-publication",
                "outcome":"stale_gm_epoch",
                "publication":"not_started",
                "admitted_gm_epoch":0,
                "current_gm_epoch":1,
                "force":false
            })),
            None,
            Some(1),
            None,
        )
        .unwrap();
        assert_eq!(classified["status"], "publication_not_applied");
        assert_eq!(classified["terminal_not_applied"], true);
        assert_eq!(classified["coalesced"], false);
    }

    #[test]
    fn settled_confirmed_readback_is_publication_applied() {
        let classified = classify(
            "settled",
            Some(applied_publication_receipt()),
            Some(1),
            Some(2),
            None,
        )
        .unwrap();
        assert_eq!(classified["status"], "publication_applied");
        assert_eq!(classified["terminal_not_applied"], false);
        assert_eq!(classified["publication_started"], true);
    }

    #[test]
    fn publication_unknown_requires_exact_remote_readback() {
        let classified = classify(
            "outcome_unknown",
            Some(json!({
                "operation_id":"op-publication",
                "outcome":"unknown",
                "publication":"requires_readback",
                "reason":"push result was lost",
                "publication_may_have_started":true,
                "force":false
            })),
            Some(1),
            None,
            None,
        )
        .unwrap();
        assert_eq!(classified["status"], "publication_readback_required");
        assert_eq!(classified["readback_required"], true);
        assert_eq!(classified["coalesced"], false);
    }

    #[test]
    fn coalesced_publication_requires_an_exact_owner_reference() {
        let receipt = json!({
            "operation_id":"op-publication",
            "outcome":"coalesced",
            "state":"settled",
            "publication":"coalesced",
            "coalesced":true,
            "coalesced_to":"op-owner",
            "publication_may_have_started":false,
            "force":false
        });
        let classified =
            classify("settled", Some(receipt), None, Some(1), Some("op-owner")).unwrap();
        assert_eq!(classified["status"], "publication_not_applied");
        assert_eq!(classified["outcome"], "coalesced");
        assert_eq!(classified["coalesced"], true);
        assert_eq!(classified["publication_started"], false);
    }

    #[test]
    fn applied_receipt_for_another_remote_ref_is_corrupt() {
        let mut receipt = applied_publication_receipt();
        receipt["remote_ref"]["commit"] = json!("e".repeat(40));
        let error = classify("settled", Some(receipt), Some(1), Some(2), None)
            .expect_err("the remote readback must match the retained Forge intent");
        assert_eq!(error.code, "AUTOMATION_PUBLICATION_STATE_CORRUPT");
    }

    #[test]
    fn unknown_publication_operation_state_is_corruption_not_a_new_state() {
        let error = ExistingAutomaticOperation::decode(
            "op-publication".to_owned(),
            "maybe_applied".to_owned(),
            None,
            None,
            None,
        )
        .expect_err("an unknown lifecycle state is retained-data corruption");
        assert_eq!(error.code, "AUTOMATION_OPERATION_CORRUPT");
    }

    #[test]
    fn queued_operation_cannot_retain_dispatch_timestamp() {
        let error = ExistingAutomaticOperation::decode(
            "op-publication".to_owned(),
            "queued".to_owned(),
            None,
            Some(1),
            None,
        )
        .expect_err("a queued Operation cannot have crossed dispatch");
        assert_eq!(error.code, "AUTOMATION_OPERATION_CORRUPT");
    }
}
