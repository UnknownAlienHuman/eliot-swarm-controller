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
const MAX_SCRIPT_TERMINAL_EVENT_BYTES: i64 =
    (crate::scripts::manifest::MAX_RESULT_BYTES + 128 * 1024) as i64;
const RECEIPT_STORAGE_PREFIX: &str = "automation:v1:intake:receipt:";
const HOOK_COMMIT_INDEX_PREFIX: &str = "automation:v1:intake:hook_commit:";

type ScriptTerminalProjectionRow = (String, String, String, Option<String>);

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HookSourceSetupFact {
    source_id: String,
    project_id: String,
    canonical_repository: String,
    registration_id: String,
    registration_generation: i64,
    event: String,
    created_by: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HookSourceRevokeFact {
    source_id: String,
    project_id: String,
    event: String,
    revision: i64,
    revoked_at_ms: i64,
    revoked_by: String,
}

#[derive(Debug, Clone)]
pub(crate) struct HookSourceAdminOccurrence {
    pub(crate) source_id: String,
    pub(crate) project_id: String,
    pub(crate) status: crate::automation::event_rules::EventStatus,
    pub(crate) occurrence_phase: String,
    pub(crate) occurrence_id: String,
}

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

/// Project the closed ScriptRun outcome only after exact source, Operation,
/// result and run readback. Raw script content never enters the projection.
fn script_terminal_event_projection(
    db: &Connection,
    event: &ObservedEvent,
) -> Result<crate::automation::intake::SafeEventProjection> {
    use crate::automation::event_rules::EventStatus;

    if event.source_id != "controller:scripts"
        || !matches!(
            event.event_kind.as_str(),
            "script.completed" | "script.failed" | "script.incomplete"
        )
    {
        return Ok(Default::default());
    }
    let Some(operation_id) = event.operation_id.as_deref().filter(|id| !id.is_empty()) else {
        return Ok(Default::default());
    };
    let row: Option<ScriptTerminalProjectionRow> = db
        .query_row(
            "SELECT o.source_event_key,r.run_id,r.state,\
             CASE WHEN json_valid(o.payload_json) THEN \
               CASE WHEN json_type(o.payload_json,'$.outcome')='text' \
                 THEN json_extract(o.payload_json,'$.outcome') ELSE NULL END \
             ELSE NULL END \
             FROM observations AS o \
             JOIN operations AS op ON op.operation_id=o.operation_id \
             JOIN script_runs AS r ON r.operation_id=op.operation_id \
             WHERE o.observation_id=?1 AND o.source_stream_id='controller:scripts' \
               AND o.kind=?2 AND o.operation_id=?3 \
               AND o.source_event_key='terminal:' || op.operation_id \
               AND op.method='script.run' AND op.state='settled' \
               AND op.result_json=o.payload_json \
               AND length(o.payload_json)<=?4 AND length(op.result_json)<=?4 \
               AND op.settled_at_ms=o.recorded_at_ms \
               AND r.finished_at_ms=o.recorded_at_ms \
               AND json_valid(o.payload_json) \
               AND json_extract(o.payload_json,'$.operation_id')=op.operation_id \
               AND json_extract(o.payload_json,'$.run_id')=r.run_id \
               AND json_extract(o.payload_json,'$.state')=r.state \
               AND op.task_id IS r.task_id AND op.attempt_id IS r.attempt_id \
               AND ((r.task_id IS NULL AND r.task_revision IS NULL AND r.attempt_id IS NULL) \
                 OR (r.task_id IS NOT NULL AND r.task_revision>0 AND r.attempt_id IS NOT NULL)) \
               AND (?2!='script.failed' OR \
                 json_type(o.payload_json,'$.execution_started')='false') \
               AND (?2!='script.incomplete' OR \
                 json_type(o.payload_json,'$.execution_may_have_started') IN ('true','false')) \
               AND (?2!='script.completed' OR (\
                 json_extract(o.payload_json,'$.result_ref') IS r.result_ref \
                 AND json_extract(o.payload_json,'$.stdout_ref') IS r.stdout_ref \
                 AND json_extract(o.payload_json,'$.stderr_ref') IS r.stderr_ref \
                 AND json_extract(o.payload_json,'$.exit_code') IS r.exit_code))",
            params![
                event.observation_id,
                event.event_kind,
                operation_id,
                MAX_SCRIPT_TERMINAL_EVENT_BYTES
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((source_event_key, _run_id, run_state, outcome)) = row else {
        return Ok(Default::default());
    };
    if source_event_key != format!("terminal:{operation_id}") {
        return Ok(Default::default());
    }
    // `script.completed` is the worker's terminal callback kind for all
    // validated terminal states. Status describes only the retained script
    // run state; it never implies Task completion.
    let (status, phase) = match (event.event_kind.as_str(), run_state.as_str()) {
        ("script.failed", "failed") | ("script.completed", "failed") => {
            (EventStatus::Failed, "script_run_failed")
        }
        ("script.incomplete", "incomplete") | ("script.completed", "incomplete") => {
            (EventStatus::Incomplete, "script_run_incomplete")
        }
        ("script.completed", "completed") => (EventStatus::Completed, "script_run_completed"),
        _ => return Ok(Default::default()),
    };
    let outcome_matches = match status {
        EventStatus::Completed => {
            matches!(outcome.as_deref(), Some("applied" | "effects_incomplete"))
        }
        EventStatus::Failed => outcome.as_deref() == Some("failed"),
        EventStatus::Incomplete => outcome.as_deref() == Some("incomplete"),
        _ => false,
    };
    if !outcome_matches {
        return Ok(Default::default());
    }
    let occurrence_phase = phase.to_owned();
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

/// Admit only the closed RuntimeOutcome shape written by `runtime::outcome`.
/// SQLite checks the exact linked observation and closed DTO fields in place;
/// arbitrary `details` never leave SQLite or enter the returned projection.
fn accepted_runtime_outcome_projection(
    db: &Connection,
    event: &ObservedEvent,
) -> Result<Option<crate::automation::intake::SafeEventProjection>> {
    let Some(operation_id) = event.operation_id.as_deref().filter(|id| !id.is_empty()) else {
        return Ok(None);
    };
    if event.observation_id <= 0
        || !event.source_id.starts_with("module:")
        || event.event_kind != "runtime.outcome"
    {
        return Ok(None);
    }

    let accepted: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM observations AS o \
             JOIN operations AS op ON op.operation_id=o.operation_id \
             JOIN bindings AS b ON b.binding_id=o.binding_id AND b.generation=o.binding_generation \
             WHERE o.observation_id=?1 AND o.source_stream_id=?2 AND o.kind=?3 \
               AND o.operation_id=?4 AND o.source_event_key IS NOT NULL \
               AND o.binding_id IS NOT NULL AND o.binding_generation>0 \
               AND op.binding_id=o.binding_id AND op.binding_generation=o.binding_generation \
               AND length(o.source_event_key)=length('outcome:' || op.operation_id || ':')+64 \
               AND substr(o.source_event_key,1,length('outcome:' || op.operation_id || ':'))=\
                   'outcome:' || op.operation_id || ':' \
               AND substr(o.source_event_key,length('outcome:' || op.operation_id || ':')+1) \
                   NOT GLOB '*[^0-9a-f]*' \
               AND json_type(b.state_json,'$.module_client_id')='text' \
               AND json_extract(b.state_json,'$.module_client_id')<>'' \
               AND o.source_stream_id='module:' || json_extract(b.state_json,'$.module_client_id') \
               AND json_type(o.payload_json)='object' \
               AND json_type(o.payload_json,'$.operation_id')='text' \
               AND json_extract(o.payload_json,'$.operation_id')=op.operation_id \
               AND json_type(o.payload_json,'$.outcome')='text' \
               AND json_extract(o.payload_json,'$.outcome')='accepted' \
               AND json_type(o.payload_json,'$.details') IS NOT NULL \
               AND COALESCE(json_type(o.payload_json,'$.native_scope_key') IN ('text','null'),1) \
               AND COALESCE(json_type(o.payload_json,'$.native_root_id') IN ('text','null'),1) \
               AND COALESCE(json_type(o.payload_json,'$.turn_id') IN ('text','null'),1) \
               AND COALESCE(json_type(o.payload_json,'$.native_input_id') IN ('text','null'),1) \
               AND NOT EXISTS (\
                 SELECT 1 FROM json_each(o.payload_json) AS field \
                 WHERE field.key NOT IN (\
                   'operation_id','outcome','native_scope_key','native_root_id',\
                   'turn_id','native_input_id','details')) \
               AND (SELECT COUNT(*) FROM json_each(o.payload_json))=\
                   (SELECT COUNT(DISTINCT field.key) FROM json_each(o.payload_json) AS field)\
             )",
        params![
            event.observation_id,
            event.source_id,
            event.event_kind,
            operation_id,
        ],
        |row| row.get(0),
    )?;
    if !accepted {
        return Ok(None);
    }

    let occurrence_phase = "native_input_accepted";
    let occurrence_id = format!("operation:{operation_id}:{occurrence_phase}");
    if !valid_occurrence_identity(&occurrence_id) {
        return Ok(None);
    }
    Ok(Some(crate::automation::intake::SafeEventProjection {
        status: None,
        error_code: None,
        occurrence_phase: Some(occurrence_phase.to_owned()),
        occurrence_id: Some(occurrence_id),
        ..Default::default()
    }))
}

/// Revalidate the exact operationless HookSource administration fact against
/// its retained source and client records. The DTOs are closed and bounded;
/// actor, repository, and credential metadata never enter ScriptRun input.
pub(crate) fn hook_source_admin_occurrence_by_observation(
    db: &Connection,
    event: &ObservedEvent,
) -> Result<Option<HookSourceAdminOccurrence>> {
    use crate::automation::event_rules::EventStatus;

    const SOURCE_ID: &str = "controller:hook-source";
    const SETUP_KIND: &str = "hook.source.setup";
    const REVOKE_KIND: &str = "hook.source.revoke";
    const EVENT_NAME: &str = "git.post_commit";
    const MAX_PAYLOAD_BYTES: i64 = 2048;

    if event.source_id != SOURCE_ID
        || !matches!(event.event_kind.as_str(), SETUP_KIND | REVOKE_KIND)
        || event.operation_id.is_some()
    {
        return Ok(None);
    }
    type HookAdminObservationRow = (Option<String>, Option<String>, Option<String>, i64, i64);
    let row: Option<HookAdminObservationRow> = db
        .query_row(
            "SELECT CASE WHEN source_event_key IS NOT NULL \
                         AND length(CAST(source_event_key AS BLOB))<=256 \
                         THEN source_event_key END, \
                    operation_id, \
                    CASE WHEN length(CAST(payload_json AS BLOB))<=2048 \
                         THEN payload_json END, \
                    recorded_at_ms, length(CAST(payload_json AS BLOB)) \
             FROM observations WHERE observation_id=?1 \
               AND source_stream_id='controller:hook-source' AND kind=?2",
            params![event.observation_id, event.event_kind],
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
    let Some((source_event_key, operation_id, raw, recorded_at_ms, payload_bytes)) = row else {
        return Ok(None);
    };
    if source_event_key
        .as_deref()
        .is_none_or(|key| key.is_empty() || key.len() > 256)
        || operation_id.is_some()
        || event.recorded_at_ms != recorded_at_ms
        || recorded_at_ms < 0
        || !(0..=MAX_PAYLOAD_BYTES).contains(&payload_bytes)
    {
        return Ok(None);
    }
    let Some(raw) = raw else {
        return Ok(None);
    };

    let (source_id, project_id, status, occurrence_phase, occurrence_id) =
        match event.event_kind.as_str() {
            SETUP_KIND => {
                let Ok(fact) = serde_json::from_str::<HookSourceSetupFact>(&raw) else {
                    return Ok(None);
                };
                let expected_key = format!("setup:{}", fact.source_id);
                if !crate::hooks::contract::is_canonical_v4_uuid(&fact.source_id)
                    || fact.project_id.trim().is_empty()
                    || fact.project_id.len() > 128
                    || fact.project_id.chars().any(char::is_control)
                    || fact.canonical_repository.trim().is_empty()
                    || fact.canonical_repository.len() > 512
                    || fact.canonical_repository.chars().any(char::is_control)
                    || !crate::forge::canonical_repository(&fact.canonical_repository)
                        .is_ok_and(|repository| repository == fact.canonical_repository)
                    || fact.registration_id.trim().is_empty()
                    || fact.registration_id.len() > 128
                    || fact.registration_id.chars().any(char::is_control)
                    || fact.registration_generation <= 0
                    || fact.event != EVENT_NAME
                    || fact.created_by.trim().is_empty()
                    || fact.created_by.len() > 128
                    || fact.created_by.chars().any(char::is_control)
                    || source_event_key.as_deref() != Some(expected_key.as_str())
                    || recorded_at_ms < 0
                {
                    return Ok(None);
                }
                let source_id = fact.source_id;
                (
                    source_id.clone(),
                    fact.project_id,
                    EventStatus::Applied,
                    "hook_source_setup_committed".to_owned(),
                    format!("hook_source:{source_id}:setup:{recorded_at_ms}"),
                )
            }
            REVOKE_KIND => {
                let Ok(fact) = serde_json::from_str::<HookSourceRevokeFact>(&raw) else {
                    return Ok(None);
                };
                let expected_key = format!("revoke:{}:{}", fact.source_id, fact.revision);
                if !crate::hooks::contract::is_canonical_v4_uuid(&fact.source_id)
                    || fact.project_id.trim().is_empty()
                    || fact.project_id.len() > 128
                    || fact.project_id.chars().any(char::is_control)
                    || fact.event != EVENT_NAME
                    || fact.revision <= 0
                    || fact.revoked_at_ms != recorded_at_ms
                    || fact.revoked_by.trim().is_empty()
                    || fact.revoked_by.len() > 128
                    || fact.revoked_by.chars().any(char::is_control)
                    || source_event_key.as_deref() != Some(expected_key.as_str())
                {
                    return Ok(None);
                }
                let source_id = fact.source_id;
                (
                    source_id.clone(),
                    fact.project_id,
                    EventStatus::Invalidated,
                    "hook_source_revoked".to_owned(),
                    format!("hook_source:{source_id}:revoked:{}", fact.revision),
                )
            }
            _ => return Ok(None),
        };

    let Some(source_value) =
        config::read_record(db, &format!("hook:v1:source:{source_id}"), "hook source")?
    else {
        return Ok(None);
    };
    let Ok(source) =
        serde_json::from_value::<crate::hooks::contract::HookSourceRecord>(source_value)
    else {
        return Ok(None);
    };
    let valid_digest =
        |value: &str| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit());
    if source.schema_version != crate::hooks::contract::SOURCE_SCHEMA_VERSION
        || source.source_id != source_id
        || source.client_id != format!("hook-source:{source_id}")
        || source.project_id != project_id
        || source.event != EVENT_NAME
        || source.registration_id.trim().is_empty()
        || source.registration_generation <= 0
        || !valid_digest(&source.registration_digest)
        || !valid_digest(&source.setup_request_digest)
        || source.canonical_repository.trim().is_empty()
        || source.canonical_repository.len() > 512
        || source.canonical_repository.chars().any(char::is_control)
        || !crate::forge::canonical_repository(&source.canonical_repository)
            .is_ok_and(|repository| repository == source.canonical_repository)
        || source.created_by.trim().is_empty()
        || source.created_by.len() > 128
        || source.created_by.chars().any(char::is_control)
        || source.created_at_ms < 0
        || source.revision <= 0
        || source
            .revoked_at_ms
            .is_some_and(|revoked_at| revoked_at < source.created_at_ms)
        || source.last_observation_id.is_some_and(|id| id <= 0)
    {
        return Ok(None);
    }
    let Some(client) = super::meta(db, &format!("client:{}", source.client_id))? else {
        return Ok(None);
    };
    let Some(token_hash) = client["token_hash"].as_str() else {
        return Ok(None);
    };
    if client["role"] != "hook_source"
        || client["hook_source_id"] != source.source_id
        || !valid_digest(token_hash)
        || client["disabled"].as_bool() != Some(source.revoked_at_ms.is_some())
    {
        return Ok(None);
    }

    let producer_matches = match event.event_kind.as_str() {
        SETUP_KIND => {
            let Ok(fact) = serde_json::from_str::<HookSourceSetupFact>(&raw) else {
                return Ok(None);
            };
            fact.project_id == source.project_id
                && fact.canonical_repository == source.canonical_repository
                && fact.registration_id == source.registration_id
                && fact.registration_generation == source.registration_generation
                && fact.created_by == source.created_by
                && source.created_at_ms == recorded_at_ms
        }
        REVOKE_KIND => {
            let Ok(fact) = serde_json::from_str::<HookSourceRevokeFact>(&raw) else {
                return Ok(None);
            };
            fact.project_id == source.project_id
                && fact.revision == source.revision
                && source.revoked_at_ms == Some(fact.revoked_at_ms)
                && client["disabled"] == true
        }
        _ => false,
    };
    if !producer_matches {
        return Ok(None);
    }
    let occurrence = HookSourceAdminOccurrence {
        source_id,
        project_id,
        status,
        occurrence_phase,
        occurrence_id,
    };
    if !valid_occurrence_identity(&occurrence.occurrence_id) {
        return Ok(None);
    }
    Ok(Some(occurrence))
}

/// Read only producer-normalized status metadata for event kinds whose
/// payload contract is explicitly safe. Unlisted event payloads are never
/// parsed for selector matching.
pub(crate) fn safe_event_projection(
    db: &Connection,
    event: &ObservedEvent,
) -> Result<crate::automation::intake::SafeEventProjection> {
    use crate::automation::event_rules::EventStatus;

    if event.source_id == "controller:scripts" {
        return script_terminal_event_projection(db, event);
    }
    if event.source_id == "controller:host-lifecycle"
        && matches!(event.event_kind.as_str(), "host.exit" | "host.failed")
    {
        return host_terminal_exit_projection(db, event);
    }
    if event.source_id.starts_with("module:") && event.event_kind == "runtime.outcome" {
        return Ok(accepted_runtime_outcome_projection(db, event)?.unwrap_or_default());
    }
    if event.source_id == "controller:native-mcp" && event.event_kind == "native.mcp.failure" {
        return native_mcp_failure_projection(db, event);
    }
    if event.source_id == "controller:hook-source"
        && matches!(
            event.event_kind.as_str(),
            "hook.source.setup" | "hook.source.revoke"
        )
    {
        return Ok(hook_source_admin_occurrence_by_observation(db, event)?
            .map(
                |occurrence| crate::automation::intake::SafeEventProjection {
                    status: Some(occurrence.status),
                    occurrence_phase: Some(occurrence.occurrence_phase),
                    occurrence_id: Some(occurrence.occurrence_id),
                    ..Default::default()
                },
            )
            .unwrap_or_default());
    }

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
        ("controller:operations", "operation.cancelled") => {
            Some((EventStatus::Cancelled, "operation_cancelled"))
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
        ("controller:operations", "operation.cancelled") => Some("OPERATION_CANCELLED"),
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
        ..Default::default()
    })
}

/// Project the closed failure occurrence written in the same transaction as
/// a native MCP retry/stale marker. Payload reads are byte-bounded in SQL, and
/// the exact observation, source key, retained launch Operation, and event link
/// are all revalidated before exposing selector metadata.
fn native_mcp_failure_projection(
    db: &Connection,
    event: &ObservedEvent,
) -> Result<crate::automation::intake::SafeEventProjection> {
    use crate::automation::event_rules::EventStatus;

    let Some(operation_id) = event.operation_id.as_deref() else {
        return Ok(Default::default());
    };
    type NativeMcpFailureRow = (
        Option<String>,
        String,
        Option<String>,
        i64,
        i64,
        Option<String>,
    );
    let row: Option<NativeMcpFailureRow> = db
        .query_row(
            "SELECT CASE WHEN o.source_event_key IS NOT NULL \
                         AND length(CAST(o.source_event_key AS BLOB))<=256 \
                         THEN o.source_event_key END, \
                    o.operation_id, \
                    CASE WHEN length(CAST(o.payload_json AS BLOB))<=2048 \
                         THEN o.payload_json END, \
                    o.recorded_at_ms, length(CAST(o.payload_json AS BLOB)), op.method \
             FROM observations AS o LEFT JOIN operations AS op \
               ON op.operation_id=o.operation_id \
             WHERE o.observation_id=?1 AND o.source_stream_id='controller:native-mcp' \
               AND o.kind='native.mcp.failure' AND o.operation_id=?2",
            params![event.observation_id, operation_id],
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
    let Some((source_event_key, linked_operation_id, raw, recorded_at_ms, payload_bytes, method)) =
        row
    else {
        return Ok(Default::default());
    };
    if linked_operation_id != operation_id
        || method.as_deref() != Some("swarm.launch")
        || event.recorded_at_ms != recorded_at_ms
        || recorded_at_ms < 0
        || !(0..=2048).contains(&payload_bytes)
    {
        return Ok(Default::default());
    }
    let Some(raw) = raw else {
        return Ok(Default::default());
    };
    let value: Value = match serde_json::from_str(&raw) {
        Ok(value) => value,
        Err(_) => return Ok(Default::default()),
    };
    let Some(object) = value.as_object() else {
        return Ok(Default::default());
    };
    const FIELDS: &[&str] = &[
        "schema_version",
        "phase",
        "status",
        "occurrence_id",
        "error_code",
        "failure_category",
        "failed_supervisor",
        "failure_kind",
        "attempt",
    ];
    if object.len() != FIELDS.len() || FIELDS.iter().any(|field| !object.contains_key(*field)) {
        return Ok(Default::default());
    }

    let Some(attempt) = value["attempt"].as_i64().filter(|attempt| *attempt > 0) else {
        return Ok(Default::default());
    };
    let Some(supervisor) = value["failed_supervisor"]
        .as_str()
        .filter(|supervisor| matches!(*supervisor, "native_mcp_readback" | "native_mcp_tools"))
    else {
        return Ok(Default::default());
    };
    let Some(failure_kind) = value["failure_kind"].as_str().filter(|failure_kind| {
        matches!(
            (supervisor, *failure_kind),
            ("native_mcp_readback", "readback_retry")
                | ("native_mcp_tools", "tools_retry" | "stale_launch_hold")
        )
    }) else {
        return Ok(Default::default());
    };
    let Some(code) = value["error_code"].as_str().filter(|code| {
        !code.is_empty()
            && code.len() <= 64
            && code.as_bytes().first().is_some_and(u8::is_ascii_uppercase)
            && code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    }) else {
        return Ok(Default::default());
    };
    let category = if code.starts_with("STALE")
        || matches!(
            code,
            "FORBIDDEN" | "NATIVE_MCP_SCOPE_MISMATCH" | "NATIVE_MCP_WORKSPACE_LEASE_UNAVAILABLE"
        ) {
        "assignment_scope_unavailable"
    } else {
        match code {
            "NATIVE_TRANSPORT" | "NATIVE_READ_FAILED" | "NATIVE_MCP_READBACK_TIMEOUT" => {
                "native_service_unavailable"
            }
            "PRIVATE_ARTIFACT_REFERENCE" | "AUTH_ERROR" | "UNAUTHORIZED" => {
                "scoped_artifact_or_credential_unavailable"
            }
            _ => "native_readback_incomplete",
        }
    };
    let status = if code == "NATIVE_OUTCOME_UNKNOWN" {
        EventStatus::Unknown
    } else {
        EventStatus::Failed
    };
    let status_name = status.as_str();
    let operation_digest = model::digest(operation_id.as_bytes());
    let expected_occurrence_id = format!(
        "operation:{operation_digest}:native_mcp_failure:{supervisor}:{failure_kind}:{attempt}"
    );
    let occurrence_id = value["occurrence_id"].as_str();
    if value["schema_version"].as_i64() != Some(1)
        || value["phase"].as_str() != Some("native_mcp_failure")
        || value["status"].as_str() != Some(status_name)
        || value["failure_category"].as_str() != Some(category)
        || occurrence_id != Some(expected_occurrence_id.as_str())
        || !valid_occurrence_identity(occurrence_id.unwrap_or_default())
        || source_event_key.as_deref() != occurrence_id
    {
        return Ok(Default::default());
    }

    Ok(crate::automation::intake::SafeEventProjection {
        status: Some(status),
        error_code: Some(code.to_owned()),
        failure_category: Some(category.to_owned()),
        failed_supervisor: Some(supervisor.to_owned()),
        occurrence_phase: Some("native_mcp_failure".to_owned()),
        occurrence_id: occurrence_id.map(ToOwned::to_owned),
    })
}

/// Project only the closed terminal lifecycle DTO written by `retain_exit`.
/// Persisted error codes and receipt diagnostics never enter a ScriptRun
/// cause or input.
fn host_terminal_exit_projection(
    db: &Connection,
    event: &ObservedEvent,
) -> Result<crate::automation::intake::SafeEventProjection> {
    use crate::automation::event_rules::EventStatus;

    if event.operation_id.is_some() {
        return Ok(Default::default());
    }
    let row: Option<(String, String, i64)> = db
        .query_row(
            "SELECT source_event_key,payload_json,recorded_at_ms FROM observations \
             WHERE observation_id=?1 AND source_stream_id='controller:host-lifecycle' \
             AND kind=?2 AND operation_id IS NULL",
            params![event.observation_id, event.event_kind],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((source_event_key, raw, recorded_at_ms)) = row else {
        return Ok(Default::default());
    };
    if raw.len() as i64 > MAX_INTAKE_PAYLOAD_BYTES
        || recorded_at_ms < 0
        || event.recorded_at_ms != recorded_at_ms
    {
        return Ok(Default::default());
    }
    let value: Value = match serde_json::from_str(&raw) {
        Ok(value) => value,
        Err(_) => return Ok(Default::default()),
    };
    let Some(fields) = value.as_object() else {
        return Ok(Default::default());
    };
    let status = match value["status"].as_str() {
        Some("completed") if event.event_kind == "host.exit" => EventStatus::Completed,
        Some("failed") => EventStatus::Failed,
        _ => return Ok(Default::default()),
    };
    let host_epoch = value["host_epoch"].as_i64();
    let occurrence_id = value["occurrence_id"].as_str();
    if value["schema_version"] != 1
        || value["phase"] != "host_terminal_exit_observed"
        || host_epoch.is_none_or(|epoch| epoch <= 0)
        || host_epoch.is_some_and(|epoch| {
            source_event_key
                != format!(
                    "{}:{epoch}",
                    if event.event_kind == "host.exit" {
                        "terminal"
                    } else {
                        "failed"
                    }
                )
        })
        || occurrence_id
            != host_epoch
                .map(|epoch| format!("host-terminal-exit:{epoch}"))
                .as_deref()
        || !valid_occurrence_identity(occurrence_id.unwrap_or_default())
    {
        return Ok(Default::default());
    }
    let failure_category = value["failure_category"].as_str();
    let failed_supervisor = value["failed_supervisor"].as_str();
    let category_valid = match failure_category {
        Some("startup_failure" | "runtime_failure") => failed_supervisor.is_none(),
        Some("supervisor_stopped" | "supervisor_failed") => {
            failed_supervisor.is_none_or(super::host_lifecycle::is_known_supervisor)
        }
        _ => false,
    };
    if (status == EventStatus::Completed
        && (failure_category.is_some() || failed_supervisor.is_some()))
        || (status == EventStatus::Failed && !category_valid)
        || (event.event_kind == "host.failed" && status != EventStatus::Failed)
    {
        return Ok(Default::default());
    }
    let base_fields = [
        "host_epoch",
        "occurrence_id",
        "phase",
        "schema_version",
        "status",
    ];
    let expected_field_count = if status == EventStatus::Completed {
        base_fields.len()
    } else if failed_supervisor.is_some() {
        base_fields.len() + 2
    } else {
        base_fields.len() + 1
    };
    if fields.len() != expected_field_count
        || fields.keys().any(|key| {
            !base_fields.contains(&key.as_str())
                && key != "failure_category"
                && key != "failed_supervisor"
        })
    {
        return Ok(Default::default());
    }
    let host_epoch = host_epoch.unwrap_or_default();
    if status == EventStatus::Failed {
        let (sibling_kind, sibling_key) = if event.event_kind == "host.exit" {
            ("host.failed", format!("failed:{host_epoch}"))
        } else {
            ("host.exit", format!("terminal:{host_epoch}"))
        };
        let sibling: Option<(String, String, i64)> = db
            .query_row(
                "SELECT source_event_key,payload_json,recorded_at_ms FROM observations \
                 WHERE source_stream_id='controller:host-lifecycle' AND kind=?1 \
                 AND source_event_key=?2 AND operation_id IS NULL LIMIT 1",
                params![sibling_kind, sibling_key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        if !sibling.is_some_and(|(key, payload, time)| {
            key == sibling_key && payload == raw && time == recorded_at_ms
        }) {
            return Ok(Default::default());
        }
    } else {
        let normalized_failure_exists: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM observations \
             WHERE source_stream_id='controller:host-lifecycle' AND kind='host.failed' \
             AND source_event_key=?1 AND operation_id IS NULL)",
            [format!("failed:{host_epoch}")],
            |row| row.get(0),
        )?;
        if normalized_failure_exists {
            return Ok(Default::default());
        }
    }
    Ok(crate::automation::intake::SafeEventProjection {
        status: Some(status),
        failure_category: failure_category.map(ToOwned::to_owned),
        failed_supervisor: failed_supervisor.map(ToOwned::to_owned),
        occurrence_phase: Some("host_terminal_exit_observed".to_owned()),
        occurrence_id: occurrence_id.map(ToOwned::to_owned),
        ..Default::default()
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
