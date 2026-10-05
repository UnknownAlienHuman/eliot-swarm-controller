//! Bounded durable intake over explicitly admitted local observation streams.
//!
//! Callers pass their existing Store transaction so registration/cursor,
//! immutable receipt, and pending journal writes share one commit boundary.

use crate::{
    automation::{config, intake::*},
    error::{Error, Result},
    model,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

const REGISTRATION_SCHEMA_VERSION: u32 = 1;
const RECEIPT_SCHEMA_VERSION: u32 = 1;
const JOURNAL_SCHEMA_VERSION: u32 = 1;
const MAX_SOURCE_EVENT_KEY_BYTES: usize = 512;
const MAX_INTAKE_PAYLOAD_BYTES: i64 = 48 * 1024;
const RECEIPT_STORAGE_PREFIX: &str = "automation:v1:intake:receipt:";
const HOOK_COMMIT_INDEX_PREFIX: &str = "automation:v1:intake:hook_commit:";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalRecord {
    schema_version: u32,
    source_id: String,
    observation_id: i64,
    outcome: JournalOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum JournalOutcome {
    Receipt {
        receipt_key: String,
        event_digest: String,
    },
    Gap {
        gap: EventGap,
    },
}

#[derive(Debug)]
struct ObservationRow {
    observation_id: i64,
    source_event_key: Option<String>,
    source_key_oversized: bool,
    operation_id: Option<String>,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
    event_kind: String,
    payload_json: String,
    payload_oversized: bool,
    recorded_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HookCommitIndex {
    schema_version: u32,
    source_id: String,
    project_id: String,
    commit_oid: String,
    canonical_repository: String,
    receipt_key: String,
    event_digest: String,
}

/// Find the immutable, readback-verified HookCommit receipt for one selected
/// setup source and exact project/commit identity. The caller must still
/// validate the current HookSource/workspace registration before admission.
pub(crate) fn hook_commit_by_identity(
    db: &Connection,
    hook_source_id: &str,
    project_id: &str,
    commit_oid: &str,
) -> Result<Option<(EventReceipt, crate::hooks::contract::HookCommitFact)>> {
    let normalized_commit = commit_oid.to_ascii_lowercase();
    let key = hook_commit_index_key(hook_source_id, project_id, &normalized_commit)?;
    let Some(value) = config::read_record(db, &key, "HookCommit identity index")? else {
        return Ok(None);
    };
    let index: HookCommitIndex = decode_record(value, "HookCommit identity index")?;
    if index.schema_version != 1
        || index.source_id != hook_source_id
        || index.project_id != project_id
        || !index.commit_oid.eq_ignore_ascii_case(&normalized_commit)
        || !valid_receipt_storage_key(&index.receipt_key)
        || !valid_event_digest(&index.event_digest)
    {
        return Err(Error::new(
            "AUTOMATION_HOOK_INDEX_CORRUPT",
            "HookCommit identity index is inconsistent",
        ));
    }
    let Some(value) = readback_value(db, &index.receipt_key, "HookCommit event receipt")? else {
        return Err(Error::new(
            "AUTOMATION_HOOK_INDEX_CORRUPT",
            "HookCommit identity index points to a missing receipt",
        ));
    };
    let value = value.map_err(|reason| {
        Error::new(
            "AUTOMATION_HOOK_INDEX_CORRUPT",
            format!("HookCommit receipt readback failed: {reason}"),
        )
    })?;
    let receipt: EventReceipt = decode_record(value, "HookCommit event receipt")?;
    let registration =
        load_registration(db, LocalProducer::HookCommit.source_id())?.ok_or_else(|| {
            Error::new(
                "AUTOMATION_HOOK_INDEX_CORRUPT",
                "HookCommit receipt has no registered intake source",
            )
        })?;
    let fact = parse_hook_commit_fact(&receipt)?;
    if !valid_receipt(
        &receipt,
        LocalProducer::HookCommit.source_id(),
        &registration,
        receipt.observation_id,
    ) || receipt.event_digest != index.event_digest
        || receipt_key(&receipt.source_id, &receipt.source_event_key)? != index.receipt_key
        || fact.source_id != index.source_id
        || fact.project_id != index.project_id
        || !fact.commit_oid.eq_ignore_ascii_case(&index.commit_oid)
        || fact.canonical_repository != index.canonical_repository
        || receipt_digest(&receipt).as_deref() != Some(receipt.event_digest.as_str())
    {
        return Err(Error::new(
            "AUTOMATION_HOOK_INDEX_CORRUPT",
            "HookCommit indexed receipt does not match its retained identity",
        ));
    }
    Ok(Some((receipt, fact)))
}

pub(crate) fn parse_hook_commit_fact(
    receipt: &EventReceipt,
) -> Result<crate::hooks::contract::HookCommitFact> {
    validate_hook_commit_payload(
        &receipt.source_id,
        &receipt.event_kind,
        &receipt.source_event_key,
        &receipt.payload,
    )
}

/// Read one bounded page from the durable global observations sequence. This
/// lane is deliberately metadata-only: the caller receives no source event
/// key, binding identity, or payload. ScriptRun asks a typed projection only
/// after an exact manager selector matches and current event visibility is
/// established.
pub(crate) fn observed_event_page(
    db: &Connection,
    after_observation_id: i64,
    through_observation_id: i64,
    limit: usize,
) -> Result<Vec<ObservedEvent>> {
    if after_observation_id < 0 || through_observation_id < after_observation_id {
        return Err(Error::invalid("observed-event cursor range is invalid"));
    }
    let mut statement = db.prepare(
        "SELECT observation_id,source_stream_id,kind,operation_id,recorded_at_ms \
         FROM observations WHERE observation_id>?1 AND observation_id<=?2 \
         ORDER BY observation_id LIMIT ?3",
    )?;
    statement
        .query_map(
            params![
                after_observation_id,
                through_observation_id,
                limit.clamp(1, MAX_INTAKE_PAGE) as i64,
            ],
            |row| {
                Ok(ObservedEvent {
                    observation_id: row.get(0)?,
                    source_id: row.get(1)?,
                    event_kind: row.get(2)?,
                    operation_id: row.get(3)?,
                    recorded_at_ms: row.get(4)?,
                })
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

/// Read one exact observation by its durable O1 sequence identity. This
/// remains metadata-only; callers must use an authorized adapter before
/// projecting any event-specific fields.
pub(crate) fn observed_event_by_id(
    db: &Connection,
    observation_id: i64,
) -> Result<Option<ObservedEvent>> {
    if observation_id <= 0 {
        return Ok(None);
    }
    db.query_row(
        "SELECT observation_id,source_stream_id,kind,operation_id,recorded_at_ms \
         FROM observations WHERE observation_id=?1",
        [observation_id],
        |row| {
            Ok(ObservedEvent {
                observation_id: row.get(0)?,
                source_id: row.get(1)?,
                event_kind: row.get(2)?,
                operation_id: row.get(3)?,
                recorded_at_ms: row.get(4)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

pub(crate) fn observed_event_high_water(db: &Connection) -> Result<i64> {
    Ok(db.query_row(
        "SELECT COALESCE(MAX(observation_id),0) FROM observations",
        [],
        |row| row.get(0),
    )?)
}

/// Read one receipt only after its exact source has been registered and the
/// observation has been committed to that source's immutable O1 journal.
/// This is used to retain the existing typed TaskSubmission contract while
/// ScriptRun advances its separate global observation cursor.
pub(crate) fn receipt_by_observation_id(
    db: &Connection,
    source_id: &str,
    observation_id: i64,
) -> Result<Option<EventReceipt>> {
    if observation_id <= 0 {
        return Ok(None);
    }
    let Some(registration) = load_registration(db, source_id)? else {
        return Ok(None);
    };
    let Some(cursor) = load_cursor(db, source_id)? else {
        return Ok(None);
    };
    if observation_id > cursor.observation_id {
        return Ok(None);
    }
    let key = journal_key(source_id, observation_id);
    let item = read_pending_journal(db, source_id, &registration, observation_id, &key)?;
    Ok(match item {
        IntakeItem::Receipt(receipt) => Some(receipt),
        IntakeItem::Gap(_) => None,
    })
}

/// Project the closed TaskSubmission outcome only after exact O1 journal
/// readback. Submission bodies, candidate metadata, and event keys are never
/// returned to the ScriptRun consumer.
pub(crate) fn task_submission_projection_by_observation(
    db: &Connection,
    event: &ObservedEvent,
) -> Result<crate::automation::intake::SafeEventProjection> {
    if event.source_id != LocalProducer::TaskSubmission.stream_id()
        || event.event_kind != LocalProducer::TaskSubmission.event_kind()
    {
        return Ok(Default::default());
    }
    let Some(receipt) = receipt_by_observation_id(
        db,
        LocalProducer::TaskSubmission.source_id(),
        event.observation_id,
    )?
    else {
        return Ok(Default::default());
    };
    if receipt.event_kind != event.event_kind
        || receipt.operation_id.as_deref() != event.operation_id.as_deref()
    {
        return Ok(Default::default());
    }
    use crate::automation::event_rules::EventStatus;
    let (status, status_name) = match receipt.payload["outcome"].as_str() {
        Some("applied") => (EventStatus::Applied, "applied"),
        Some("failed") => (EventStatus::Failed, "failed"),
        Some("stale_submission_scope") => (EventStatus::Invalidated, "invalidated"),
        _ => return Ok(Default::default()),
    };
    let operation_id = receipt.operation_id.as_deref().unwrap_or_default();
    if operation_id.is_empty() {
        return Ok(Default::default());
    }
    let occurrence_phase = format!("task_submission_{status_name}");
    let occurrence_id = format!("operation:{operation_id}:{occurrence_phase}");
    if !valid_occurrence_identity(&occurrence_id) {
        return Ok(Default::default());
    }
    Ok(crate::automation::intake::SafeEventProjection {
        status: Some(status),
        occurrence_phase: Some(occurrence_phase),
        occurrence_id: Some(occurrence_id),
        ..Default::default()
    })
}

/// Read the verified HookCommit identity associated with one exact durable
/// observation. The caller must additionally check that the HookSource is
/// still current and belongs to its configured project.
pub(crate) fn hook_commit_fact_by_observation(
    db: &Connection,
    observation_id: i64,
) -> Result<Option<crate::hooks::contract::HookCommitFact>> {
    let row: Option<(Option<String>, String)> = db
        .query_row(
            "SELECT source_event_key,payload_json FROM observations \
             WHERE observation_id=?1 AND source_stream_id=?2 AND kind=?3",
            params![
                observation_id,
                LocalProducer::HookCommit.stream_id(),
                LocalProducer::HookCommit.event_kind()
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((Some(source_event_key), payload_json)) = row else {
        return Ok(None);
    };
    if !valid_event_key(&source_event_key) || payload_json.len() as i64 > MAX_INTAKE_PAYLOAD_BYTES {
        return Ok(None);
    }
    let payload: Value = match serde_json::from_str(&payload_json) {
        Ok(payload) => payload,
        Err(_) => return Ok(None),
    };
    let Ok(fact) = validate_hook_commit_payload(
        LocalProducer::HookCommit.source_id(),
        LocalProducer::HookCommit.event_kind(),
        &source_event_key,
        &payload,
    ) else {
        return Ok(None);
    };
    let Some((receipt, indexed_fact)) =
        hook_commit_by_identity(db, &fact.source_id, &fact.project_id, &fact.commit_oid)?
    else {
        return Ok(None);
    };
    if receipt.observation_id != observation_id || indexed_fact != fact {
        return Ok(None);
    }
    Ok(Some(fact))
}

/// Read only producer-normalized status metadata for event kinds whose
/// payload contract is explicitly safe. Unlisted event payloads are never
/// parsed for selector matching.
pub(crate) fn safe_event_projection(
    db: &Connection,
    event: &ObservedEvent,
) -> Result<crate::automation::intake::SafeEventProjection> {
    use crate::automation::event_rules::EventStatus;

    let expected = match (event.source_id.as_str(), event.event_kind.as_str()) {
        ("controller:messages", "message.sent" | "message.reply_sent") => {
            Some((EventStatus::Sent, "message_send_committed"))
        }
        ("controller:coordination", "coordination.answer") => {
            Some((EventStatus::Answered, "coordination_answered"))
        }
        ("controller:runtime", "native.operation.completed") => {
            Some((EventStatus::Applied, "native_outcome_terminal"))
        }
        ("controller:runtime", "native.result.available") => {
            Some((EventStatus::Completed, "native_result_page_recorded"))
        }
        ("controller:host-lifecycle", "host.interrupted") => {
            Some((EventStatus::Unknown, "host_interruption_observed"))
        }
        ("controller:operations", "operation.rejected") => {
            Some((EventStatus::Rejected, "operation_rejected"))
        }
        ("controller:operations", "operation.outcome_unknown") => {
            Some((EventStatus::Unknown, "operation_outcome_unknown"))
        }
        _ => None,
    };
    let Some((expected_status, expected_phase)) = expected else {
        return Ok(Default::default());
    };
    let raw: Option<String> = db
        .query_row(
            "SELECT payload_json FROM observations WHERE observation_id=?1 AND source_stream_id=?2 AND kind=?3",
            params![event.observation_id, event.source_id, event.event_kind],
            |row| row.get(0),
        )
        .optional()?;
    let Some(raw) = raw else {
        return Ok(Default::default());
    };
    let value: Value = match serde_json::from_str(&raw) {
        Ok(value) => value,
        Err(_) => return Ok(Default::default()),
    };
    let phase = value["phase"].as_str();
    let occurrence_id = value["occurrence_id"].as_str();
    if value["schema_version"] != 1
        || phase != Some(expected_phase)
        || occurrence_id.is_none_or(|identity| !valid_occurrence_identity(identity))
    {
        return Ok(Default::default());
    }
    let status = match value["status"].as_str() {
        Some("applied") => EventStatus::Applied,
        Some("completed") => EventStatus::Completed,
        Some("failed") => EventStatus::Failed,
        Some("incomplete") => EventStatus::Incomplete,
        Some("cancelled") => EventStatus::Cancelled,
        Some("rejected") => EventStatus::Rejected,
        Some("sent") => EventStatus::Sent,
        Some("answered") => EventStatus::Answered,
        Some("invalidated") => EventStatus::Invalidated,
        Some("unknown") => EventStatus::Unknown,
        _ => return Ok(Default::default()),
    };
    let expected_status = match (event.event_kind.as_str(), status) {
        ("native.operation.completed", EventStatus::Applied | EventStatus::Rejected) => status,
        ("native.result.available", EventStatus::Completed | EventStatus::Incomplete) => status,
        (_, observed) if observed == expected_status => expected_status,
        _ => return Ok(Default::default()),
    };
    // This status is written only by the result producer after validating the
    // exact ResultPage, including eof/range/length/digest. Its closed event DTO
    // intentionally omits eof, artifact identity and digest; do not require or
    // expose those fields here.
    if event.event_kind == "native.result.available"
        && !matches!(status, EventStatus::Completed | EventStatus::Incomplete)
    {
        return Ok(Default::default());
    }
    let expected_error_code = match (event.source_id.as_str(), event.event_kind.as_str()) {
        ("controller:host-lifecycle", "host.interrupted") => Some("HOST_INTERRUPTED"),
        ("controller:operations", "operation.rejected") => Some("OPERATION_REJECTED"),
        ("controller:operations", "operation.outcome_unknown") => Some("OUTCOME_UNKNOWN"),
        _ => None,
    };
    let error_code = match expected_error_code {
        Some(code) if value["error_code"] == code => Some(code.to_owned()),
        Some(_) => return Ok(Default::default()),
        None => None,
    };
    if event.source_id == "controller:operations" && event.operation_id.is_none() {
        return Ok(Default::default());
    }
    if event.event_kind == "host.interrupted" && error_code.is_none() {
        return Ok(Default::default());
    }
    if event.event_kind == "host.interrupted"
        && !host_occurrence_matches(&value, occurrence_id.unwrap_or_default())
    {
        return Ok(Default::default());
    }
    if let Some(operation_id) = event.operation_id.as_deref()
        && occurrence_id != Some(format!("operation:{operation_id}:{expected_phase}").as_str())
    {
        return Ok(Default::default());
    }
    Ok(crate::automation::intake::SafeEventProjection {
        status: Some(expected_status),
        error_code,
        occurrence_phase: Some(expected_phase.to_owned()),
        occurrence_id: occurrence_id.map(ToOwned::to_owned),
    })
}

/// Internal correlation for the legacy host-exit record. Its payload is read
/// only to validate the epoch-pair identity and is never copied into a script
/// input or trigger cause.
pub(crate) fn host_exit_occurrence_projection(
    db: &Connection,
    event: &ObservedEvent,
) -> Result<crate::automation::intake::SafeEventProjection> {
    if event.source_id != "controller:host-lifecycle" || event.event_kind != "host.exit" {
        return Ok(Default::default());
    }
    let raw: Option<String> = db
        .query_row(
            "SELECT payload_json FROM observations WHERE observation_id=?1 \
             AND source_stream_id='controller:host-lifecycle' AND kind='host.exit'",
            [event.observation_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(raw) = raw else {
        return Ok(Default::default());
    };
    if raw.len() as i64 > MAX_INTAKE_PAYLOAD_BYTES {
        return Ok(Default::default());
    }
    let value: Value = match serde_json::from_str(&raw) {
        Ok(value) => value,
        Err(_) => return Ok(Default::default()),
    };
    let phase = value["phase"].as_str();
    let occurrence_id = value["occurrence_id"].as_str();
    if phase != Some("host_interruption_observed")
        || occurrence_id.is_none_or(|identity| !valid_occurrence_identity(identity))
        || !host_occurrence_matches(&value, occurrence_id.unwrap_or_default())
    {
        return Ok(Default::default());
    }
    Ok(crate::automation::intake::SafeEventProjection {
        occurrence_phase: phase.map(ToOwned::to_owned),
        occurrence_id: occurrence_id.map(ToOwned::to_owned),
        ..Default::default()
    })
}

fn host_occurrence_matches(value: &Value, occurrence_id: &str) -> bool {
    let Some(previous_epoch) = value["previous_host_epoch"].as_i64() else {
        return false;
    };
    let Some(current_epoch) = value["current_host_epoch"].as_i64() else {
        return false;
    };
    previous_epoch > 0
        && current_epoch > 0
        && previous_epoch != current_epoch
        && occurrence_id == format!("host-interruption:{previous_epoch}:{current_epoch}")
}

fn valid_occurrence_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-/@".contains(&byte))
}

/// Admit a closed, existing local Store producer and initialize its cursor.
/// Registration and cursor initialization are written in the caller's Store
/// transaction. `include_existing=false` records a high-water baseline and
/// does not replay earlier observations.
pub(crate) fn register_local_source(
    tx: &Transaction<'_>,
    producer: LocalProducer,
    include_existing: bool,
    now_ms: i64,
) -> Result<(SourceRegistration, IntakeCursor)> {
    if now_ms < 0 {
        return Err(Error::invalid(
            "intake registration time cannot be negative",
        ));
    }
    let source_id = producer.source_id();
    let registration_key = registration_key(source_id);
    if let Some(existing) =
        config::read_record(tx, &registration_key, "intake source registration")?
    {
        let registration: SourceRegistration =
            decode_record(existing, "intake source registration")?;
        validate_registration(&registration, source_id)?;
        let cursor = load_cursor(tx, source_id)?.ok_or_else(|| {
            Error::new(
                "AUTOMATION_INTAKE_CURSOR_MISSING",
                "registered intake source has no durable cursor",
            )
        })?;
        return Ok((registration, cursor));
    }
    if load_cursor(tx, source_id)?.is_some() {
        return Err(Error::new(
            "AUTOMATION_INTAKE_RECORD_INVALID",
            "unregistered intake source already has a cursor",
        ));
    }

    let high_water = observation_high_water(tx, producer.stream_id(), producer.event_kind())?;
    let initial_cursor = if include_existing { 0 } else { high_water };
    let registration = SourceRegistration {
        schema_version: REGISTRATION_SCHEMA_VERSION,
        source_id: source_id.to_owned(),
        producer,
        stream_id: producer.stream_id().to_owned(),
        event_kind: producer.event_kind().to_owned(),
        initial_cursor,
        include_existing,
        registered_at_ms: now_ms,
    };
    write_immutable_record(
        tx,
        &registration_key,
        &registration,
        "intake source registration",
    )?;
    let cursor = IntakeCursor {
        schema_version: 1,
        source_id: source_id.to_owned(),
        observation_id: initial_cursor,
        updated_at_ms: now_ms,
    };
    config::write_record(tx, &cursor_key(source_id), &json!(cursor))?;
    Ok((registration, cursor))
}

/// Reconcile at most one bounded observation page. The caller supplies the
/// cursor it last observed; a concurrent/replayed caller receives an explicit
/// stale result and must read back before attempting another page.
pub(crate) fn reconcile_source_page(
    tx: &Transaction<'_>,
    source_id: &str,
    expected_cursor: i64,
    limit: usize,
    now_ms: i64,
) -> Result<ReconcilePage> {
    validate_source_id(source_id)?;
    if expected_cursor < 0 || now_ms < 0 {
        return Err(Error::invalid(
            "expected intake cursor and reconciliation time cannot be negative",
        ));
    }
    let Some(registration) = load_registration(tx, source_id)? else {
        return Ok(ReconcilePage {
            source_id: source_id.to_owned(),
            status: IntakeStatus::UnknownSource,
            cursor: None,
            high_water: None,
            processed: 0,
            items: Vec::new(),
        });
    };
    let mut cursor = load_cursor(tx, source_id)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_INTAKE_CURSOR_MISSING",
            "registered intake source has no durable cursor",
        )
    })?;
    let high_water = observation_high_water(tx, &registration.stream_id, &registration.event_kind)?;
    if cursor.observation_id != expected_cursor || cursor.observation_id > high_water {
        return Ok(ReconcilePage {
            source_id: source_id.to_owned(),
            status: IntakeStatus::StaleCursor,
            cursor: Some(cursor.observation_id),
            high_water: Some(high_water),
            processed: 0,
            items: Vec::new(),
        });
    }

    let page_limit = limit.clamp(1, MAX_INTAKE_PAGE);
    let rows = observation_page(
        tx,
        &registration,
        cursor.observation_id,
        high_water,
        page_limit,
    )?;
    let mut items = Vec::with_capacity(rows.len());
    let mut last_scanned = cursor.observation_id;
    for row in rows {
        let mut item = process_observation(tx, source_id, &row)?;
        if let IntakeItem::Receipt(receipt) = &item
            && registration.producer == LocalProducer::HookCommit
        {
            let event_key = receipt.source_event_key.clone();
            let event_digest = receipt.event_digest.clone();
            let index_error = match parse_hook_commit_fact(receipt)
                .and_then(|fact| write_hook_commit_index(tx, receipt, &fact))
            {
                Ok(_) => None,
                Err(error) if error.code == "STORE_ERROR" => return Err(error),
                Err(error) => Some(error.code.to_ascii_lowercase()),
            };
            if let Some(reason_code) = index_error {
                item = IntakeItem::Gap(gap(
                    &row,
                    source_id,
                    Some(&event_key),
                    Some(&event_digest),
                    &format!("hook_commit_index_readback:{reason_code}"),
                ));
            }
        }
        write_journal(tx, source_id, row.observation_id, &item)?;
        last_scanned = row.observation_id;
        items.push(item);
    }

    // With the same stream/kind predicate, a high-water row must be visible in
    // a subsequent page. Treat a violated assumption as an explicit gap,
    // never as an empty-success acknowledgement.
    if items.is_empty() && high_water > cursor.observation_id {
        let gap = EventGap {
            source_id: source_id.to_owned(),
            observation_id: high_water,
            source_event_key: None,
            event_digest: None,
            reason: "source_page_empty_below_high_water".to_owned(),
        };
        let item = IntakeItem::Gap(gap);
        write_journal(tx, source_id, high_water, &item)?;
        last_scanned = high_water;
        items.push(item);
    }

    if last_scanned != cursor.observation_id {
        cursor.observation_id = last_scanned;
        cursor.updated_at_ms = now_ms;
        config::write_record(tx, &cursor_key(source_id), &json!(cursor))?;
    }
    let status = status_for_items(&items, cursor.observation_id == expected_cursor);
    Ok(ReconcilePage {
        source_id: source_id.to_owned(),
        status,
        cursor: Some(cursor.observation_id),
        high_water: Some(high_water),
        processed: items.len(),
        items,
    })
}

/// Read durable subjects by observation cursor. The query is bounded and
/// repeatable; consumers advance their own read cursor only after handling
/// the returned page. No task, Operation, or model call is created here.
pub(crate) fn pending_page(
    db: &Connection,
    source_id: &str,
    after_observation_id: i64,
    limit: usize,
) -> Result<PendingPage> {
    validate_source_id(source_id)?;
    if after_observation_id < 0 {
        return Err(Error::invalid("pending-page cursor cannot be negative"));
    }
    let Some(registration) = load_registration(db, source_id)? else {
        return Ok(PendingPage {
            source_id: source_id.to_owned(),
            status: IntakeStatus::UnknownSource,
            cursor: None,
            next_after_observation_id: None,
            items: Vec::new(),
        });
    };
    let cursor = load_cursor(db, source_id)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_INTAKE_CURSOR_MISSING",
            "registered intake source has no durable cursor",
        )
    })?;
    if after_observation_id > cursor.observation_id {
        return Ok(PendingPage {
            source_id: source_id.to_owned(),
            status: IntakeStatus::StaleCursor,
            cursor: Some(cursor.observation_id),
            next_after_observation_id: Some(after_observation_id),
            items: Vec::new(),
        });
    }

    let prefix = journal_prefix(source_id);
    let after_key = journal_key(source_id, after_observation_id);
    let mut statement =
        db.prepare("SELECT key FROM meta WHERE key LIKE ?1 AND key>?2 ORDER BY key LIMIT ?3")?;
    let keys = statement
        .query_map(
            params![
                format!("{prefix}%"),
                after_key,
                limit.clamp(1, MAX_INTAKE_PAGE) as i64
            ],
            |row| row.get::<_, String>(0),
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);

    let mut items = Vec::with_capacity(keys.len());
    let mut next_after = after_observation_id;
    for key in keys {
        let observation_id = journal_observation_id(source_id, &key)?;
        if observation_id > cursor.observation_id {
            return Err(Error::new(
                "AUTOMATION_INTAKE_JOURNAL_CURSOR_INVALID",
                "pending journal key is beyond the durable source cursor",
            ));
        }
        next_after = observation_id;
        items.push(read_pending_journal(
            db,
            source_id,
            &registration,
            observation_id,
            &key,
        )?);
    }
    let status = status_for_items(&items, next_after == after_observation_id);
    Ok(PendingPage {
        source_id: source_id.to_owned(),
        status,
        cursor: Some(cursor.observation_id),
        next_after_observation_id: Some(next_after),
        items,
    })
}

fn journal_observation_id(source_id: &str, key: &str) -> Result<i64> {
    let prefix = journal_prefix(source_id);
    let suffix = key.strip_prefix(&prefix).ok_or_else(|| {
        Error::new(
            "AUTOMATION_INTAKE_JOURNAL_KEY_INVALID",
            "pending journal key is outside its source prefix",
        )
    })?;
    let observation_id = suffix.parse::<i64>().map_err(|_| {
        Error::new(
            "AUTOMATION_INTAKE_JOURNAL_KEY_INVALID",
            "pending journal key has no numeric observation identity",
        )
    })?;
    if observation_id <= 0 || journal_key(source_id, observation_id) != key {
        return Err(Error::new(
            "AUTOMATION_INTAKE_JOURNAL_KEY_INVALID",
            "pending journal key is not canonically encoded",
        ));
    }
    Ok(observation_id)
}

fn read_pending_journal(
    db: &Connection,
    source_id: &str,
    registration: &SourceRegistration,
    observation_id: i64,
    key: &str,
) -> Result<IntakeItem> {
    let Some(value) = readback_value(db, key, "automation intake pending journal")? else {
        return Ok(IntakeItem::Gap(readback_gap(
            source_id,
            observation_id,
            "pending_journal_missing",
        )));
    };
    let value = match value {
        Ok(value) => value,
        Err(reason) => {
            return Ok(IntakeItem::Gap(readback_gap(
                source_id,
                observation_id,
                reason,
            )));
        }
    };
    let journal: JournalRecord = match decode_record(value, "pending journal") {
        Ok(journal) => journal,
        Err(_) => {
            return Ok(IntakeItem::Gap(readback_gap(
                source_id,
                observation_id,
                "pending_journal_record_invalid",
            )));
        }
    };
    if validate_journal(&journal, source_id).is_err() || journal.observation_id != observation_id {
        return Ok(IntakeItem::Gap(readback_gap(
            source_id,
            observation_id,
            "pending_journal_identity_mismatch",
        )));
    }
    read_journal_item(db, source_id, registration, observation_id, journal)
}

/// Open a sealed meta record while converting only record-local corruption to
/// a caller-visible gap. SQLite errors and unexpected canonicalization failures
/// remain Store errors and must not be treated as an empty page.
fn readback_value(
    db: &Connection,
    key: &str,
    label: &str,
) -> Result<Option<std::result::Result<Value, &'static str>>> {
    let raw: Option<String> = db
        .query_row("SELECT value_json FROM meta WHERE key=?1", [key], |row| {
            row.get(0)
        })
        .optional()?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    if raw.len() > config::MAX_META_RECORD_BYTES {
        return Ok(Some(Err("sealed_record_exceeds_metadata_bound")));
    }
    let sealed: Value = match serde_json::from_str(&raw) {
        Ok(value) => value,
        Err(_) => return Ok(Some(Err("sealed_record_json_invalid"))),
    };
    match config::open_record(sealed, label) {
        Ok(value) => Ok(Some(Ok(value))),
        Err(error) if error.code == "AUTOMATION_RECORD_VERSION" => {
            Ok(Some(Err("sealed_record_version_unsupported")))
        }
        Err(error) if error.code == "AUTOMATION_RECORD_CORRUPT" => {
            Ok(Some(Err("sealed_record_integrity_invalid")))
        }
        Err(error) => Err(error),
    }
}

fn readback_gap(source_id: &str, observation_id: i64, reason: &str) -> EventGap {
    EventGap {
        source_id: source_id.to_owned(),
        observation_id,
        source_event_key: None,
        event_digest: None,
        reason: reason.to_owned(),
    }
}

fn process_observation(
    tx: &Transaction<'_>,
    source_id: &str,
    row: &ObservationRow,
) -> Result<IntakeItem> {
    if row.source_key_oversized {
        return Ok(IntakeItem::Gap(gap(
            row,
            source_id,
            None,
            None,
            "source_event_key_invalid",
        )));
    }
    let Some(event_key) = row.source_event_key.as_deref() else {
        return Ok(IntakeItem::Gap(gap(
            row,
            source_id,
            None,
            None,
            "source_event_key_missing",
        )));
    };
    if !valid_event_key(event_key) {
        let retained_key = (event_key.len() <= MAX_SOURCE_EVENT_KEY_BYTES).then_some(event_key);
        return Ok(IntakeItem::Gap(gap(
            row,
            source_id,
            retained_key,
            None,
            "source_event_key_invalid",
        )));
    }
    if row.payload_oversized {
        return Ok(IntakeItem::Gap(gap(
            row,
            source_id,
            Some(event_key),
            None,
            "payload_exceeds_intake_bound",
        )));
    }
    let payload: Value = match serde_json::from_str(&row.payload_json) {
        Ok(payload) => payload,
        Err(_) => {
            return Ok(IntakeItem::Gap(gap(
                row,
                source_id,
                Some(event_key),
                None,
                "payload_invalid",
            )));
        }
    };
    if source_id == LocalProducer::HookCommit.source_id()
        && validate_hook_commit_payload(source_id, &row.event_kind, event_key, &payload).is_err()
    {
        return Ok(IntakeItem::Gap(gap(
            row,
            source_id,
            Some(event_key),
            None,
            "hook_commit_contract_invalid",
        )));
    }
    let digest_input = json!({
        "schema_version":1,
        "source_id":source_id,
        "source_event_key":event_key,
        "event_kind":row.event_kind.clone(),
        "operation_id":row.operation_id.clone(),
        "binding_id":row.binding_id.clone(),
        "binding_generation":row.binding_generation,
        "payload":payload.clone()
    });
    let event_digest = match model::canonical(&digest_input) {
        Ok(canonical) => model::digest(canonical.as_bytes()),
        Err(_) => {
            return Ok(IntakeItem::Gap(gap(
                row,
                source_id,
                Some(event_key),
                None,
                "event_digest_unavailable",
            )));
        }
    };
    let key = receipt_key(source_id, event_key)?;
    if let Some(existing) = config::read_record(tx, &key, "automation intake event receipt")? {
        let receipt: EventReceipt = match decode_record(existing, "event receipt") {
            Ok(receipt) => receipt,
            Err(_) => {
                return Ok(IntakeItem::Gap(gap(
                    row,
                    source_id,
                    Some(event_key),
                    Some(&event_digest),
                    "receipt_record_invalid",
                )));
            }
        };
        if receipt.source_id != source_id
            || receipt.source_event_key != event_key
            || receipt.event_digest != event_digest
        {
            return Ok(IntakeItem::Gap(gap(
                row,
                source_id,
                Some(event_key),
                Some(&event_digest),
                "receipt_digest_conflict",
            )));
        }
    } else {
        let receipt = EventReceipt {
            schema_version: RECEIPT_SCHEMA_VERSION,
            source_id: source_id.to_owned(),
            observation_id: row.observation_id,
            source_event_key: event_key.to_owned(),
            event_digest: event_digest.clone(),
            event_kind: row.event_kind.clone(),
            operation_id: row.operation_id.clone(),
            binding_id: row.binding_id.clone(),
            binding_generation: row.binding_generation,
            payload: payload.clone(),
            recorded_at_ms: row.recorded_at_ms,
        };
        let receipt_value = serde_json::to_value(&receipt)?;
        if model::canonical(&receipt_value)?.len()
            > config::MAX_META_RECORD_BYTES.saturating_sub(128)
        {
            return Ok(IntakeItem::Gap(gap(
                row,
                source_id,
                Some(event_key),
                Some(&event_digest),
                "receipt_exceeds_metadata_bound",
            )));
        }
        write_immutable_record(tx, &key, &receipt, "automation intake event receipt")?;
    }
    Ok(IntakeItem::Receipt(EventReceipt {
        schema_version: RECEIPT_SCHEMA_VERSION,
        source_id: source_id.to_owned(),
        observation_id: row.observation_id,
        source_event_key: event_key.to_owned(),
        event_digest,
        event_kind: row.event_kind.clone(),
        operation_id: row.operation_id.clone(),
        binding_id: row.binding_id.clone(),
        binding_generation: row.binding_generation,
        payload,
        recorded_at_ms: row.recorded_at_ms,
    }))
}

fn read_journal_item(
    db: &Connection,
    source_id: &str,
    registration: &SourceRegistration,
    observation_id: i64,
    journal: JournalRecord,
) -> Result<IntakeItem> {
    match journal.outcome {
        JournalOutcome::Gap { gap } => {
            if gap.source_id != source_id || gap.observation_id != observation_id {
                return Ok(IntakeItem::Gap(readback_gap(
                    source_id,
                    observation_id,
                    "pending_journal_identity_mismatch",
                )));
            }
            if gap.reason.is_empty()
                || gap.reason.len() > 128
                || gap.reason.chars().any(char::is_control)
                || gap
                    .source_event_key
                    .as_deref()
                    .is_some_and(|key| !valid_event_key(key))
                || gap
                    .event_digest
                    .as_deref()
                    .is_some_and(|digest| !valid_event_digest(digest))
            {
                return Ok(IntakeItem::Gap(readback_gap(
                    source_id,
                    observation_id,
                    "pending_journal_gap_invalid",
                )));
            }
            Ok(IntakeItem::Gap(gap))
        }
        JournalOutcome::Receipt {
            receipt_key: receipt_storage_key,
            event_digest,
        } => {
            if !valid_receipt_storage_key(&receipt_storage_key)
                || !valid_event_digest(&event_digest)
            {
                return Ok(IntakeItem::Gap(readback_gap(
                    source_id,
                    observation_id,
                    "pending_journal_receipt_pointer_invalid",
                )));
            }
            let Some(value) =
                readback_value(db, &receipt_storage_key, "automation intake event receipt")?
            else {
                return Ok(IntakeItem::Gap(readback_gap(
                    source_id,
                    observation_id,
                    "receipt_missing_on_readback",
                )));
            };
            let value = match value {
                Ok(value) => value,
                Err(reason) => {
                    return Ok(IntakeItem::Gap(readback_gap(
                        source_id,
                        observation_id,
                        reason,
                    )));
                }
            };
            let receipt: EventReceipt = match decode_record(value, "event receipt") {
                Ok(receipt) => receipt,
                Err(_) => {
                    return Ok(IntakeItem::Gap(readback_gap(
                        source_id,
                        observation_id,
                        "receipt_record_invalid",
                    )));
                }
            };
            let expected_receipt_key = receipt_key(source_id, &receipt.source_event_key)?;
            if !valid_receipt(&receipt, source_id, registration, observation_id)
                || receipt.event_digest != event_digest
                || expected_receipt_key != receipt_storage_key
                || receipt_digest(&receipt).as_deref() != Some(receipt.event_digest.as_str())
            {
                return Ok(IntakeItem::Gap(readback_gap(
                    source_id,
                    observation_id,
                    "receipt_readback_mismatch",
                )));
            }
            Ok(IntakeItem::Receipt(EventReceipt {
                observation_id,
                ..receipt
            }))
        }
    }
}

fn valid_event_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_receipt_storage_key(value: &str) -> bool {
    value
        .strip_prefix(RECEIPT_STORAGE_PREFIX)
        .is_some_and(valid_event_digest)
}

fn valid_receipt(
    receipt: &EventReceipt,
    source_id: &str,
    registration: &SourceRegistration,
    journal_observation_id: i64,
) -> bool {
    receipt.schema_version == RECEIPT_SCHEMA_VERSION
        && receipt.source_id == source_id
        && receipt.event_kind == registration.event_kind
        && valid_event_key(&receipt.source_event_key)
        && valid_event_digest(&receipt.event_digest)
        && receipt.observation_id > 0
        && receipt.observation_id <= journal_observation_id
        && receipt.recorded_at_ms >= 0
        && receipt
            .binding_generation
            .is_none_or(|generation| generation >= 0)
        && model::canonical(&receipt.payload)
            .is_ok_and(|payload| payload.len() as i64 <= MAX_INTAKE_PAYLOAD_BYTES)
}

fn receipt_digest(receipt: &EventReceipt) -> Option<String> {
    let digest_input = json!({
        "schema_version":1,
        "source_id":receipt.source_id,
        "source_event_key":receipt.source_event_key,
        "event_kind":receipt.event_kind,
        "operation_id":receipt.operation_id,
        "binding_id":receipt.binding_id,
        "binding_generation":receipt.binding_generation,
        "payload":receipt.payload
    });
    model::canonical(&digest_input)
        .ok()
        .map(|canonical| model::digest(canonical.as_bytes()))
}

fn observation_page(
    db: &Connection,
    registration: &SourceRegistration,
    cursor: i64,
    high_water: i64,
    limit: usize,
) -> Result<Vec<ObservationRow>> {
    let mut statement = db.prepare(
        "SELECT observation_id,\
         CASE WHEN length(CAST(source_event_key AS BLOB))>?6 THEN NULL ELSE source_event_key END,\
         COALESCE(length(CAST(source_event_key AS BLOB))>?6,0),\
         operation_id,binding_id,binding_generation,kind,\
         CASE WHEN length(CAST(payload_json AS BLOB))>?7 THEN '' ELSE payload_json END,\
         length(CAST(payload_json AS BLOB))>?7,recorded_at_ms \
         FROM observations WHERE source_stream_id=?1 AND kind=?2 AND observation_id>?3 AND observation_id<=?4 \
         ORDER BY observation_id LIMIT ?5",
    )?;
    let rows = statement
        .query_map(
            params![
                registration.stream_id,
                registration.event_kind,
                cursor,
                high_water,
                limit as i64,
                MAX_SOURCE_EVENT_KEY_BYTES as i64,
                MAX_INTAKE_PAYLOAD_BYTES
            ],
            |row| {
                Ok(ObservationRow {
                    observation_id: row.get(0)?,
                    source_event_key: row.get(1)?,
                    source_key_oversized: row.get(2)?,
                    operation_id: row.get(3)?,
                    binding_id: row.get(4)?,
                    binding_generation: row.get(5)?,
                    event_kind: row.get(6)?,
                    payload_json: row.get(7)?,
                    payload_oversized: row.get(8)?,
                    recorded_at_ms: row.get(9)?,
                })
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn observation_high_water(db: &Connection, stream_id: &str, event_kind: &str) -> Result<i64> {
    Ok(db.query_row(
        "SELECT COALESCE(MAX(observation_id),0) FROM observations WHERE source_stream_id=?1 AND kind=?2",
        params![stream_id, event_kind],
        |row| row.get(0),
    )?)
}

fn load_registration(db: &Connection, source_id: &str) -> Result<Option<SourceRegistration>> {
    let Some(value) = config::read_record(
        db,
        &registration_key(source_id),
        "intake source registration",
    )?
    else {
        return Ok(None);
    };
    let registration: SourceRegistration = decode_record(value, "intake source registration")?;
    validate_registration(&registration, source_id)?;
    Ok(Some(registration))
}

fn load_cursor(db: &Connection, source_id: &str) -> Result<Option<IntakeCursor>> {
    let Some(value) = config::read_record(db, &cursor_key(source_id), "intake source cursor")?
    else {
        return Ok(None);
    };
    let cursor: IntakeCursor = decode_record(value, "intake source cursor")?;
    if cursor.schema_version != 1
        || cursor.source_id != source_id
        || cursor.observation_id < 0
        || cursor.updated_at_ms < 0
    {
        return Err(Error::new(
            "AUTOMATION_INTAKE_RECORD_INVALID",
            "stored intake cursor identity or value is invalid",
        ));
    }
    Ok(Some(cursor))
}

fn validate_registration(registration: &SourceRegistration, source_id: &str) -> Result<()> {
    if registration.schema_version != REGISTRATION_SCHEMA_VERSION
        || registration.source_id != source_id
        || registration.source_id != registration.producer.source_id()
        || registration.stream_id != registration.producer.stream_id()
        || registration.event_kind != registration.producer.event_kind()
        || registration.initial_cursor < 0
        || registration.registered_at_ms < 0
    {
        return Err(Error::new(
            "AUTOMATION_INTAKE_RECORD_INVALID",
            "stored intake registration is unsupported or inconsistent",
        ));
    }
    Ok(())
}

fn validate_journal(journal: &JournalRecord, source_id: &str) -> Result<()> {
    if journal.schema_version != JOURNAL_SCHEMA_VERSION
        || journal.source_id != source_id
        || journal.observation_id <= 0
    {
        return Err(Error::new(
            "AUTOMATION_INTAKE_RECORD_INVALID",
            "stored intake journal identity is invalid",
        ));
    }
    Ok(())
}

fn write_journal(
    tx: &Transaction<'_>,
    source_id: &str,
    observation_id: i64,
    item: &IntakeItem,
) -> Result<()> {
    let outcome = match item {
        IntakeItem::Gap(gap) => JournalOutcome::Gap { gap: gap.clone() },
        IntakeItem::Receipt(receipt) => JournalOutcome::Receipt {
            receipt_key: receipt_key(source_id, &receipt.source_event_key)?,
            event_digest: receipt.event_digest.clone(),
        },
    };
    let record = JournalRecord {
        schema_version: JOURNAL_SCHEMA_VERSION,
        source_id: source_id.to_owned(),
        observation_id,
        outcome,
    };
    write_immutable_record(
        tx,
        &journal_key(source_id, observation_id),
        &record,
        "automation intake pending journal",
    )?;
    Ok(())
}

fn write_immutable_record<T: Serialize>(
    tx: &Transaction<'_>,
    key: &str,
    record: &T,
    label: &str,
) -> Result<bool> {
    let value = serde_json::to_value(record)?;
    if let Some(existing) = config::read_record(tx, key, label)? {
        if existing == value {
            return Ok(false);
        }
        return Err(Error::new(
            "AUTOMATION_INTAKE_IMMUTABLE_CONFLICT",
            format!("{label} key already contains different immutable content"),
        ));
    }
    let sealed = config::seal_record(&value)?;
    let canonical = model::canonical(&sealed)?;
    tx.execute(
        "INSERT OR IGNORE INTO meta(key,value_json) VALUES(?1,?2)",
        params![key, canonical],
    )?;
    let stored = config::read_record(tx, key, label)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_INTAKE_WRITE_MISSING",
            format!("{label} was not retained"),
        )
    })?;
    if stored != value {
        return Err(Error::new(
            "AUTOMATION_INTAKE_IMMUTABLE_CONFLICT",
            format!("{label} key was concurrently assigned different content"),
        ));
    }
    Ok(true)
}

fn decode_record<T: DeserializeOwned>(value: Value, label: &str) -> Result<T> {
    serde_json::from_value(value).map_err(|_| {
        Error::new(
            "AUTOMATION_INTAKE_RECORD_INVALID",
            format!("stored {label} fields are invalid"),
        )
    })
}

fn status_for_items(items: &[IntakeItem], unchanged: bool) -> IntakeStatus {
    if items.iter().any(|item| matches!(item, IntakeItem::Gap(_))) {
        IntakeStatus::Gap
    } else if unchanged || items.is_empty() {
        IntakeStatus::CaughtUp
    } else {
        IntakeStatus::Advanced
    }
}

fn gap(
    row: &ObservationRow,
    source_id: &str,
    source_event_key: Option<&str>,
    event_digest: Option<&str>,
    reason: &str,
) -> EventGap {
    EventGap {
        source_id: source_id.to_owned(),
        observation_id: row.observation_id,
        source_event_key: source_event_key.map(ToOwned::to_owned),
        event_digest: event_digest.map(ToOwned::to_owned),
        reason: reason.to_owned(),
    }
}

fn valid_event_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SOURCE_EVENT_KEY_BYTES
        && !value.chars().any(char::is_control)
}

fn validate_source_id(source_id: &str) -> Result<()> {
    if source_id.is_empty() || source_id.len() > 128 || source_id.chars().any(char::is_control) {
        return Err(Error::invalid("source_id is invalid"));
    }
    Ok(())
}

fn registration_key(source_id: &str) -> String {
    format!(
        "automation:v1:intake:source:{}",
        model::digest(source_id.as_bytes())
    )
}

fn cursor_key(source_id: &str) -> String {
    format!(
        "automation:v1:intake:cursor:{}",
        model::digest(source_id.as_bytes())
    )
}

fn receipt_key(source_id: &str, source_event_key: &str) -> Result<String> {
    let identity = json!([source_id, source_event_key]);
    Ok(format!(
        "automation:v1:intake:receipt:{}",
        model::digest(model::canonical(&identity)?.as_bytes())
    ))
}

fn validate_hook_commit_payload(
    receipt_source_id: &str,
    event_kind: &str,
    source_event_key: &str,
    payload: &Value,
) -> Result<crate::hooks::contract::HookCommitFact> {
    if receipt_source_id != LocalProducer::HookCommit.source_id()
        || event_kind != LocalProducer::HookCommit.event_kind()
    {
        return Err(Error::new(
            "AUTOMATION_HOOK_FACT_INVALID",
            "HookCommit receipt is outside its registered source contract",
        ));
    }
    let fact = crate::hooks::contract::HookCommitFact::parse(payload)?;
    if source_event_key != format!("{}:{}", fact.source_id, fact.commit_oid) {
        return Err(Error::new(
            "AUTOMATION_HOOK_FACT_INVALID",
            "HookCommit event key does not identify its source and exact commit",
        ));
    }
    Ok(fact)
}

fn write_hook_commit_index(
    tx: &Transaction<'_>,
    receipt: &EventReceipt,
    fact: &crate::hooks::contract::HookCommitFact,
) -> Result<()> {
    let record = HookCommitIndex {
        schema_version: 1,
        source_id: fact.source_id.clone(),
        project_id: fact.project_id.clone(),
        commit_oid: fact.commit_oid.to_ascii_lowercase(),
        canonical_repository: fact.canonical_repository.clone(),
        receipt_key: receipt_key(&receipt.source_id, &receipt.source_event_key)?,
        event_digest: receipt.event_digest.clone(),
    };
    let key = hook_commit_index_key(&record.source_id, &record.project_id, &record.commit_oid)?;
    write_immutable_record(tx, &key, &record, "HookCommit identity index")?;
    Ok(())
}

fn hook_commit_index_key(source_id: &str, project_id: &str, commit_oid: &str) -> Result<String> {
    let identity = json!([source_id, project_id, commit_oid.to_ascii_lowercase()]);
    Ok(format!(
        "{HOOK_COMMIT_INDEX_PREFIX}{}",
        model::digest(model::canonical(&identity)?.as_bytes())
    ))
}

fn journal_prefix(source_id: &str) -> String {
    format!(
        "automation:v1:intake:pending:{}:",
        model::digest(source_id.as_bytes())
    )
}

fn journal_key(source_id: &str, observation_id: i64) -> String {
    format!("{}{observation_id:020}", journal_prefix(source_id))
}
