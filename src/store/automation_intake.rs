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
        let item = process_observation(tx, source_id, &row)?;
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

fn journal_prefix(source_id: &str) -> String {
    format!(
        "automation:v1:intake:pending:{}:",
        model::digest(source_id.as_bytes())
    )
}

fn journal_key(source_id: &str, observation_id: i64) -> String {
    format!("{}{observation_id:020}", journal_prefix(source_id))
}
