//! Manager-enabled, task-scoped Goal progression from durable adapter turns.
//!
//! This consumer admits one ordinary continuation Operation per exact terminal
//! EventRef. OpenCode uses its existing controller `agent.goal continue` path;
//! Codex uses the existing `agent.send` next-turn path only after its journal
//! evidence and source tuple are revalidated. It never dispatches native input
//! itself. Store supplies the normal authority/admission callback and commits
//! it with the cursor and event receipt in the same SQLite transaction.

use super::automation_reconcile::{
    DomainErrorDisposition, MalformedAutomationEntry, QuarantineEvidence, SubjectDisposition,
    SubjectErrorDisposition,
};
use super::{automation_reconcile, operations, tasks};
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
use std::collections::HashSet;
use swarm_contracts::runtime::{
    GoalContinuationAdmissionReceipt, GoalContinuationLink, GoalTerminalEvidence,
    ModuleReceiptIdentity,
};

const STATE_PREFIX: &str = "automation:v1:goal-progression:state:";
const QUARANTINE_PREFIX: &str = "automation:v1:goal-progression:quarantine:";
const SLOT_PREFIX: &str = "goal-progression:v1:terminal-slot:";
const ENTRY_PREFIX: &str = "automation:v1:entry:";
const GLOBAL_CURSOR_KEY: &str = "automation:v1:goal-progression_global_cursor";
const STATE_SCHEMA: u32 = 1;
const SLOT_SCHEMA: u32 = 1;
const MAX_SOURCE_PAGE: usize = 32;
const MAX_ENTRY_PAGE: usize = 16;
const MAX_RECENT: usize = 20;
const MAX_PENDING_SOURCE_GAPS: usize = 64;
const MAX_SOURCE_GAP_RECHECKS: usize = 8;
const GOAL_TERMINAL_EVIDENCE_KIND: &str = "goal.terminal.evidence";

/// Root may continue after these exact Goal cursor failures only after rolling
/// back the Goal domain transaction. All other errors stop.
pub(super) fn classify_domain_error(error: Error) -> DomainErrorDisposition {
    if matches!(
        error.code.as_str(),
        "AUTOMATION_GOAL_CURSOR_CORRUPT" | "AUTOMATION_GOAL_CURSOR_MISSING"
    ) {
        DomainErrorDisposition::Degraded { code: error.code }
    } else {
        DomainErrorDisposition::Fatal(error)
    }
}

type ObservationRow = (i64, String, String);
type LinkedGoalOperationRow = (
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
);

struct GoalSourceRow {
    observation_id: i64,
    operation_id: String,
    raw: String,
    from_pending_source_gap: bool,
    verified_fact: Option<Value>,
}

struct GoalAttemptSubjectRow {
    task_id: Option<String>,
    task_revision: Option<i64>,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    schema_version: u32,
    owner_manager_id: String,
    project_id: String,
    automation_id: String,
    automation_revision: i64,
    cursor: i64,
    activation_cut: i64,
    catch_up_until: Option<i64>,
    #[serde(default)]
    pending_source_gaps: Vec<PendingSourceGap>,
    #[serde(default)]
    prefer_pending_source_retry: bool,
    recent: Vec<Value>,
    updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingSourceGap {
    // The observation row is the immutable source cause. Keep its exact
    // identity and payload digest in the existing per-entry ledger so a later
    // pass can retry readback without replaying the native input.
    source_observation_id: i64,
    source_operation_id: String,
    source_payload_sha256: String,
    reason: String,
    first_seen_at_ms: i64,
    last_checked_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GlobalCursor {
    schema_version: u32,
    last_entry_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TerminalSlot {
    schema_version: u32,
    event_slot_id: String,
    terminal_event: Value,
    native_session_id: String,
    native_input_id: String,
    source_observation_id: i64,
    source_operation_id: String,
    manager_id: String,
    automation_id: String,
    automation_revision: i64,
    goal_id: String,
    goal_revision: i64,
    goal_active: bool,
    completion_status: String,
    goal_owner_manager_id: String,
    goal_last_reviser_manager_id: String,
    objective_sha256: String,
    task_id: String,
    task_revision: i64,
    attempt_id: String,
    binding_id: String,
    binding_generation: i64,
    disposition: String,
    operation_id: Option<String>,
    recorded_at_ms: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct GoalProgressionAdmission {
    request: Value,
    linkage: Value,
    continuation_method: String,
    semantic_slot_id: String,
    effective_manager_id: String,
    task_id: String,
    task_revision: i64,
    attempt_id: String,
    binding_id: String,
    binding_generation: i64,
}

impl GoalProgressionAdmission {
    pub(crate) fn request(&self) -> &Value {
        &self.request
    }
    pub(crate) fn linkage(&self) -> &Value {
        &self.linkage
    }
    pub(crate) fn continuation_method(&self) -> &str {
        &self.continuation_method
    }
    pub(crate) fn semantic_slot_id(&self) -> &str {
        &self.semantic_slot_id
    }
    pub(crate) fn effective_manager_id(&self) -> &str {
        &self.effective_manager_id
    }
    pub(crate) fn task_id(&self) -> &str {
        &self.task_id
    }
    pub(crate) fn task_revision(&self) -> i64 {
        self.task_revision
    }
    pub(crate) fn attempt_id(&self) -> &str {
        &self.attempt_id
    }
    pub(crate) fn binding_id(&self) -> &str {
        &self.binding_id
    }
    pub(crate) fn binding_generation(&self) -> i64 {
        self.binding_generation
    }

    /// Recheck the manager-owned config, exact Goal revision, current
    /// Task/Attempt, manager scope, binding and terminal source immediately
    /// before the normal Operation is admitted.
    pub(crate) fn require_current_for_admission(&self, db: &Connection, now_ms: i64) -> Result<()> {
        require_current_admission(db, self, now_ms, None)
    }

    /// Recheck the same immutable request after its Operation row has been
    /// inserted. The current Operation is excluded when resolving the prior
    /// native Goal receipt.
    pub(crate) fn require_current_for_operation(
        &self,
        db: &Connection,
        operation_id: &str,
        now_ms: i64,
    ) -> Result<()> {
        require_current_admission(db, self, now_ms, Some(operation_id))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AdmissionResult {
    Admitted { operation_id: String },
    Reused { operation_id: String },
    Conflict { code: String, reason: String },
}

fn state_key(entry: &AutomationEntry) -> Result<String> {
    Ok(format!(
        "{STATE_PREFIX}{}",
        model::digest(
            model::canonical(&json!([
                entry.owner_manager_id,
                entry.project_id,
                entry.automation_id
            ]))?
            .as_bytes()
        )
    ))
}

fn event_slot_id(fact: &Value) -> Result<String> {
    let identity = json!([
        fact["binding_id"],
        fact["binding_generation"],
        fact["native_session_id"],
        fact["native_input_id"],
        fact["terminal_event"]["id"],
        fact["terminal_event"]["seq"],
        fact["terminal_event"]["sha256"]
    ]);
    Ok(model::digest(model::canonical(&identity)?.as_bytes()))
}

fn load_state(db: &Connection, entry: &AutomationEntry) -> Result<Option<State>> {
    let Some(value) = config::read_record(db, &state_key(entry)?, "Goal progression state")
        .map_err(|error| {
            if error.code == "AUTOMATION_RECORD_CORRUPT" {
                Error::new(
                    "AUTOMATION_GOAL_CURSOR_CORRUPT",
                    "Goal progression cursor state record is corrupt",
                )
            } else {
                error
            }
        })?
    else {
        return Ok(None);
    };
    let state: State = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "AUTOMATION_GOAL_CURSOR_CORRUPT",
            "Goal progression cursor fields are invalid",
        )
    })?;
    if state.schema_version != STATE_SCHEMA
        || state.owner_manager_id != entry.owner_manager_id
        || state.project_id != entry.project_id
        || state.automation_id != entry.automation_id
        || state.cursor < 0
        || state.activation_cut < 0
        || state.recent.len() > MAX_RECENT
        || state.pending_source_gaps.len() > MAX_PENDING_SOURCE_GAPS
        || state.pending_source_gaps.iter().any(|pending| {
            pending.source_observation_id <= 0
                || pending.source_operation_id.is_empty()
                || !is_digest(&pending.source_payload_sha256)
                || pending.reason.is_empty()
                || pending.first_seen_at_ms < 0
                || pending.last_checked_at_ms < pending.first_seen_at_ms
        })
    {
        return Err(Error::new(
            "AUTOMATION_GOAL_CURSOR_CORRUPT",
            "Goal progression cursor scope is invalid",
        ));
    }
    Ok(Some(state))
}

fn save_state(tx: &Transaction<'_>, entry: &AutomationEntry, state: &State) -> Result<()> {
    config::write_record(tx, &state_key(entry)?, &json!(state))
}

/// Transfer only the per-entry cursor. Terminal slots and admitted Operations
/// keep their original immutable authority and cause linkage.
pub(super) fn relocate_state(
    tx: &Transaction<'_>,
    former: &AutomationEntry,
    successor: &AutomationEntry,
) -> Result<()> {
    config::validate_entry(former)?;
    config::validate_entry(successor)?;
    if former.owner_manager_id == successor.owner_manager_id
        || former.project_id != successor.project_id
        || former.automation_id != successor.automation_id
    {
        return Err(Error::invalid(
            "Goal cursor relocation must preserve project and automation identity while changing owner",
        ));
    }
    let source_key = state_key(former)?;
    let target_key = state_key(successor)?;
    let target_exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM meta WHERE key=?1)",
        [&target_key],
        |row| row.get(0),
    )?;
    if target_exists {
        return Err(Error::conflict("Goal cursor target state already exists"));
    }
    let Some(mut state) = load_state(tx, former)? else {
        if former.goal_progression_ready() {
            return Err(Error::new(
                "AUTOMATION_GOAL_CURSOR_MISSING",
                "enabled Goal progression has no cursor to transfer",
            ));
        }
        return Ok(());
    };
    state
        .owner_manager_id
        .clone_from(&successor.owner_manager_id);
    state.automation_revision = successor.revision;
    state.updated_at_ms = successor.updated_at_ms;
    save_state(tx, successor, &state)?;
    if tx.execute("DELETE FROM meta WHERE key=?1", [&source_key])? != 1 {
        return Err(Error::new(
            "AUTOMATION_GOAL_CURSOR_MISSING",
            "Goal cursor source changed during transfer",
        ));
    }
    Ok(())
}

fn observation_high_water(db: &Connection) -> Result<i64> {
    Ok(db.query_row(
        "SELECT COALESCE(MAX(observation_id),0) FROM observations WHERE kind IN ('opencode.input_execution','goal.terminal.evidence')",
        [], |row| row.get(0),
    )?)
}

/// Capture the same activation cut semantics used by the existing automation
/// consumers. New entries start after existing terminal events by default.
pub(crate) fn configure_activation(
    tx: &Transaction<'_>,
    before: Option<&AutomationEntry>,
    after: &AutomationEntry,
    include_existing: bool,
    now_ms: i64,
) -> Result<()> {
    let had = before.is_some_and(AutomationEntry::goal_progression_ready);
    let has = after.goal_progression_ready();
    if !had && !has && load_state(tx, after)?.is_none() {
        return Ok(());
    }
    let key = state_key(after)?;
    let mut state = match load_state(tx, after)? {
        Some(state) => state,
        None if !had => {
            let cut = observation_high_water(tx)?;
            State {
                schema_version: STATE_SCHEMA,
                owner_manager_id: after.owner_manager_id.clone(),
                project_id: after.project_id.clone(),
                automation_id: after.automation_id.clone(),
                automation_revision: after.revision,
                cursor: if has && include_existing { 0 } else { cut },
                activation_cut: cut,
                catch_up_until: (has && include_existing).then_some(cut),
                pending_source_gaps: Vec::new(),
                prefer_pending_source_retry: false,
                recent: Vec::new(),
                updated_at_ms: now_ms,
            }
        }
        None => {
            return Err(Error::new(
                "AUTOMATION_GOAL_CURSOR_MISSING",
                "Goal progression entry has no durable cursor",
            ));
        }
    };
    if !has && had {
        state.catch_up_until = None;
        state.recent.push(json!({"disposition":"held","reason":"automation_disabled_or_step_removed","automation_revision":after.revision}));
        trim_recent(&mut state.recent);
    } else if has
        && (!had || before.is_some_and(|old| old.goal_progression != after.goal_progression))
    {
        let cut = observation_high_water(tx)?;
        state.activation_cut = cut;
        state.cursor = if include_existing { 0 } else { cut };
        state.catch_up_until = include_existing.then_some(cut);
    }
    state.automation_revision = after.revision;
    state.updated_at_ms = now_ms;
    config::write_record(tx, &key, &json!(state))
}

fn trim_recent(recent: &mut Vec<Value>) {
    if recent.len() > MAX_RECENT {
        recent.drain(..recent.len() - MAX_RECENT);
    }
}

fn event_page(
    db: &Connection,
    after: i64,
    limit: usize,
    through: Option<i64>,
) -> Result<Vec<ObservationRow>> {
    let rows = if let Some(through) = through {
        let mut statement = db.prepare(
            "SELECT observation_id,operation_id,payload_json FROM observations \
             WHERE kind IN ('opencode.input_execution','goal.terminal.evidence') AND observation_id>?1 AND observation_id<=?2 \
             ORDER BY observation_id LIMIT ?3",
        )?;
        statement
            .query_map(params![after, through, limit as i64], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
    } else {
        let mut statement = db.prepare(
            "SELECT observation_id,operation_id,payload_json FROM observations \
             WHERE kind IN ('opencode.input_execution','goal.terminal.evidence') AND observation_id>?1 \
             ORDER BY observation_id LIMIT ?2",
        )?;
        statement
            .query_map(params![after, limit as i64], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    Ok(rows)
}

fn pending_source_gap_payload(db: &Connection, pending: &PendingSourceGap) -> Result<String> {
    let row: Option<(Option<String>, Option<String>, Option<String>)> = db
        .query_row(
            "SELECT kind,operation_id,payload_json FROM observations \
             WHERE observation_id=?1",
            [pending.source_observation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((kind, operation_id, raw)) = row else {
        return Err(invalid_source(
            "retained terminal source observation was deleted before revalidation",
        ));
    };
    let (Some(kind), Some(operation_id), Some(raw)) = (kind, operation_id, raw) else {
        return Err(invalid_source(
            "retained terminal source observation is missing immutable identity or payload",
        ));
    };
    if kind != "opencode.input_execution" && kind != GOAL_TERMINAL_EVIDENCE_KIND {
        return Err(invalid_source(
            "retained terminal source observation has an immutable unexpected kind",
        ));
    }
    if operation_id != pending.source_operation_id
        || model::digest(raw.as_bytes()) != pending.source_payload_sha256
    {
        return Err(invalid_source(
            "retained terminal source observation identity or payload changed",
        ));
    }
    Ok(raw)
}

fn retain_source_gap(
    state: &mut State,
    observation_id: i64,
    operation_id: &str,
    raw: &str,
    error: &Error,
    now_ms: i64,
) -> Result<bool> {
    if observation_id <= 0 || operation_id.is_empty() {
        return Err(Error::new(
            "AUTOMATION_GOAL_CURSOR_CORRUPT",
            "terminal source gap has an invalid observation or Operation identity",
        ));
    }
    let payload_sha256 = model::digest(raw.as_bytes());
    let reason = error.code.to_ascii_lowercase();
    if let Some(pending) = state
        .pending_source_gaps
        .iter_mut()
        .find(|pending| pending.source_observation_id == observation_id)
    {
        if pending.source_operation_id != operation_id
            || pending.source_payload_sha256 != payload_sha256
        {
            return Err(Error::new(
                "AUTOMATION_GOAL_CURSOR_CORRUPT",
                "retained terminal source payload identity differs",
            ));
        }
        pending.reason = reason;
        pending.last_checked_at_ms = next_source_gap_check_at(pending.last_checked_at_ms, now_ms);
        return Ok(true);
    }
    if state.pending_source_gaps.len() >= MAX_PENDING_SOURCE_GAPS {
        return Ok(false);
    }
    state.pending_source_gaps.push(PendingSourceGap {
        source_observation_id: observation_id,
        source_operation_id: operation_id.to_owned(),
        source_payload_sha256: payload_sha256,
        reason,
        first_seen_at_ms: now_ms,
        last_checked_at_ms: now_ms,
    });
    Ok(true)
}

fn next_source_gap_check_at(previous: i64, now_ms: i64) -> i64 {
    now_ms.max(previous.saturating_add(1))
}

fn recheck_pending_source_gaps(
    tx: &Transaction<'_>,
    state: &mut State,
    budget: usize,
    now_ms: i64,
) -> Result<(Vec<GoalSourceRow>, usize, usize)> {
    let limit = budget
        .min(MAX_SOURCE_GAP_RECHECKS)
        .min(state.pending_source_gaps.len());
    let mut rows = Vec::new();
    let mut checked = 0usize;
    let mut quarantined = 0usize;
    let mut visited = HashSet::new();
    while checked < limit && !state.pending_source_gaps.is_empty() {
        let Some(index) = state
            .pending_source_gaps
            .iter()
            .enumerate()
            .filter(|(_, pending)| !visited.contains(&pending.source_observation_id))
            .min_by_key(|(_, pending)| {
                (
                    pending.last_checked_at_ms,
                    pending.first_seen_at_ms,
                    pending.source_observation_id,
                )
            })
            .map(|(index, _)| index)
        else {
            break;
        };
        let pending = state.pending_source_gaps[index].clone();
        visited.insert(pending.source_observation_id);
        checked += 1;
        let evidence = pending_source_gap_evidence(&pending);
        let disposition = automation_reconcile::with_subject_savepoint(
            tx,
            || {
                let raw = pending_source_gap_payload(tx, &pending)?;
                let fact = verified_terminal(
                    tx,
                    pending.source_observation_id,
                    &pending.source_operation_id,
                    &raw,
                )?;
                Ok(SubjectDisposition::Applied((raw, fact)))
            },
            |error| classify_goal_source_error(error, evidence.clone()),
        )?;
        match disposition {
            SubjectDisposition::Applied((raw, fact)) => {
                state.pending_source_gaps.remove(index);
                rows.push(GoalSourceRow {
                    observation_id: pending.source_observation_id,
                    operation_id: pending.source_operation_id,
                    raw,
                    from_pending_source_gap: true,
                    verified_fact: Some(fact),
                });
            }
            SubjectDisposition::Pending { code, reason } => {
                {
                    let pending = &mut state.pending_source_gaps[index];
                    pending.reason = code.to_ascii_lowercase();
                    pending.last_checked_at_ms =
                        next_source_gap_check_at(pending.last_checked_at_ms, now_ms);
                }
                append_recent(
                    state,
                    json!({
                        "observation_id":pending.source_observation_id,
                        "source_operation_id":pending.source_operation_id,
                        "disposition":"pending",
                        "reason":reason,
                        "code":code,
                        "retryable":true
                    }),
                );
            }
            SubjectDisposition::Quarantined { code, evidence } => {
                persist_goal_quarantine(tx, &code, evidence, now_ms)?;
                quarantined = quarantined.saturating_add(1);
                state.pending_source_gaps.remove(index);
                append_recent(
                    state,
                    json!({
                        "observation_id":pending.source_observation_id,
                        "source_operation_id":pending.source_operation_id,
                        "disposition":"quarantined",
                        "code":code,
                        "retryable":false
                    }),
                );
            }
            SubjectDisposition::Skipped { code, reason } => {
                return Err(Error::new(
                    "AUTOMATION_GOAL_SOURCE_CLASSIFIER_INVALID",
                    format!(
                        "Goal source classifier returned unsupported skipped code {code}: {reason}"
                    ),
                ));
            }
        }
    }
    Ok((rows, checked, quarantined))
}

fn advance_cursor_for_source_row(
    state: &mut State,
    observation_id: i64,
    from_pending_source_gap: bool,
) {
    if !from_pending_source_gap {
        state.cursor = state.cursor.max(observation_id);
    }
}

fn source_task_revision(db: &Connection, operation: &Value) -> Result<i64> {
    let _operation_id = operation["operation_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_source("source Operation ID is missing"))?;
    let operation_task_id = operation["task_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_source("source Operation Task ID is missing"))?;
    let operation_attempt_id = operation["attempt_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_source("source Operation Attempt ID is missing"))?;
    let operation_binding_id = operation["binding_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_source("source Operation binding ID is missing"))?;
    let operation_binding_generation = operation["binding_generation"]
        .as_i64()
        .filter(|generation| *generation > 0)
        .ok_or_else(|| invalid_source("source Operation binding generation is missing"))?;
    // Read the durable Attempt by its retained identity first. Compare the
    // Operation tuple in Rust so a present mismatching Attempt is invalid,
    // while an absent row remains a retryable evidence gap.
    let subject: Option<GoalAttemptSubjectRow> = db
        .query_row(
            "SELECT task_id,task_revision,binding_id,binding_generation \
             FROM attempts WHERE attempt_id=?1",
            [operation_attempt_id],
            |row| {
                Ok(GoalAttemptSubjectRow {
                    task_id: row.get(0)?,
                    task_revision: row.get(1)?,
                    binding_id: row.get(2)?,
                    binding_generation: row.get(3)?,
                })
            },
        )
        .optional()?;
    let Some(GoalAttemptSubjectRow {
        task_id,
        task_revision: revision,
        binding_id,
        binding_generation: generation,
    }) = subject
    else {
        return Err(Error::new(
            "AUTOMATION_GOAL_SOURCE_GAP",
            "terminal source is not linked to one exact Task Attempt and binding",
        ));
    };
    let (Some(task_id), Some(revision), Some(binding_id), Some(generation)) =
        (task_id, revision, binding_id, generation)
    else {
        return Err(invalid_source(
            "terminal source Task Attempt has missing immutable task or binding identity",
        ));
    };
    if operation_task_id != task_id.as_str()
        || operation_binding_id != binding_id.as_str()
        || operation_binding_generation != generation
        || revision <= 0
        || generation <= 0
    {
        return Err(invalid_source(
            "terminal source Task Attempt identity does not match its Operation",
        ));
    }
    Ok(revision)
}

fn invalid_source(message: &str) -> Error {
    Error::new("AUTOMATION_GOAL_SOURCE_INVALID", message)
}

fn goal_source_evidence(
    observation_id: i64,
    operation_id: &str,
    raw: Option<&str>,
) -> QuarantineEvidence {
    QuarantineEvidence {
        subject_identity: format!(
            "goal-observation:{}:operation:{}",
            observation_id,
            model::digest(operation_id.as_bytes())
        ),
        source_pointer: Some(format!("observations/{observation_id}")),
        source_digest: raw.map(|payload| model::digest(payload.as_bytes())),
    }
}

fn pending_source_gap_evidence(pending: &PendingSourceGap) -> QuarantineEvidence {
    QuarantineEvidence {
        subject_identity: format!(
            "goal-observation:{}:operation:{}",
            pending.source_observation_id,
            model::digest(pending.source_operation_id.as_bytes())
        ),
        source_pointer: Some(format!("observations/{}", pending.source_observation_id)),
        source_digest: Some(pending.source_payload_sha256.clone()),
    }
}

fn classify_goal_source_error(
    error: &Error,
    evidence: QuarantineEvidence,
) -> Option<SubjectErrorDisposition> {
    match error.code.as_str() {
        "AUTOMATION_GOAL_SOURCE_GAP" => Some(SubjectErrorDisposition::Pending {
            code: error.code.clone(),
            reason: error.message.clone(),
        }),
        "AUTOMATION_GOAL_SOURCE_INVALID" => Some(SubjectErrorDisposition::Quarantined {
            code: error.code.clone(),
            evidence,
        }),
        _ => None,
    }
}

fn classify_goal_admission_error(
    error: &Error,
    evidence: QuarantineEvidence,
) -> Option<SubjectErrorDisposition> {
    match error.code.as_str() {
        "AUTOMATION_GOAL_SOURCE_GAP" | "GOAL_NATIVE_STATE_UNRESOLVED" | "BINDING_NOT_READY" => {
            Some(SubjectErrorDisposition::Pending {
                code: error.code.clone(),
                reason: error.message.clone(),
            })
        }
        "AUTOMATION_GOAL_SOURCE_INVALID" => Some(SubjectErrorDisposition::Quarantined {
            code: error.code.clone(),
            evidence,
        }),
        "GOAL_NOT_ACTIVE"
        | "AUTOMATION_ACTION_CHANGED"
        | "GOAL_OWNER_CONFLICT"
        | "GOAL_NATIVE_OWNERSHIP_CONFLICT"
        | "GOAL_MANAGER_SCOPE_CONFLICT"
        | "FORBIDDEN" => Some(SubjectErrorDisposition::Skipped {
            code: error.code.clone(),
            reason: error.message.clone(),
        }),
        _ => None,
    }
}

fn verify_terminal_isolated(
    tx: &Transaction<'_>,
    observation_id: i64,
    operation_id: &str,
    raw: &str,
) -> Result<SubjectDisposition<Value>> {
    let evidence = goal_source_evidence(observation_id, operation_id, Some(raw));
    automation_reconcile::with_subject_savepoint(
        tx,
        || {
            Ok(SubjectDisposition::Applied(verified_terminal(
                tx,
                observation_id,
                operation_id,
                raw,
            )?))
        },
        |error| classify_goal_source_error(error, evidence.clone()),
    )
}

fn persist_goal_quarantine(
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
            "AUTOMATION_GOAL_QUARANTINE_INVALID",
            "Goal subject did not produce durable quarantine evidence",
        )),
    }
}

fn persist_malformed_entry(
    tx: &Transaction<'_>,
    malformed: &MalformedAutomationEntry,
    now_ms: i64,
) -> Result<()> {
    persist_goal_quarantine(tx, &malformed.code, malformed.evidence.clone(), now_ms)
}

fn original_request(db: &Connection, operation_id: &str) -> Result<Value> {
    let row: Option<Option<String>> = db
        .query_row(
            "SELECT original_request_json FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(raw) = row else {
        return Err(Error::new(
            "AUTOMATION_GOAL_SOURCE_GAP",
            "source Operation request is not retained yet",
        ));
    };
    let Some(raw) = raw else {
        return Err(invalid_source(
            "source Operation request is missing its immutable JSON",
        ));
    };
    serde_json::from_str(&raw).map_err(|_| invalid_source("source Operation request is invalid"))
}

fn verified_goal_terminal_evidence(
    db: &Connection,
    observation_id: i64,
    operation_id: &str,
    evidence_value: &Value,
    operation: &Value,
    input_execution: Option<&Value>,
) -> Result<Value> {
    let evidence: GoalTerminalEvidence = serde_json::from_value(evidence_value.clone())
        .map_err(|_| invalid_source("Goal terminal evidence payload is malformed"))?;
    evidence
        .validate()
        .map_err(|_| invalid_source("Goal terminal evidence contract is invalid"))?;
    if evidence.operation_id != operation_id
        || operation["binding_id"].as_str() != Some(evidence.binding_id.as_str())
        || Some(evidence.binding_generation) != operation["binding_generation"].as_i64()
        || operation["task_id"].as_str() != Some(evidence.task_id.as_str())
        || operation["attempt_id"].as_str() != Some(evidence.attempt_id.as_str())
        || evidence.task_revision != source_task_revision(db, operation)?
    {
        return Err(invalid_source(
            "Goal terminal evidence does not match the source Operation tuple",
        ));
    }
    let attempt_id = operation["attempt_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_source("source Operation Attempt ID is missing"))?;
    let attempt = tasks::get_attempt(db, attempt_id).map_err(|error| {
        if error.code == "NOT_FOUND" {
            Error::new(
                "AUTOMATION_GOAL_SOURCE_GAP",
                "source Attempt is not retained yet",
            )
        } else {
            error
        }
    })?;
    if attempt["task_id"] != operation["task_id"]
        || attempt["binding_id"] != operation["binding_id"]
        || attempt["binding_generation"] != operation["binding_generation"]
        || evidence.task_snapshot_sha256
            != model::digest(model::canonical(&attempt["task_snapshot"])?.as_bytes())
    {
        return Err(invalid_source(
            "Goal terminal evidence Attempt snapshot or binding differs",
        ));
    }
    let retained = if evidence.source == "codex" {
        operation["native_refs"].get("goal_terminal_evidence")
    } else {
        input_execution.and_then(|proof| proof.get("goal_terminal_evidence"))
    };
    if evidence.source == "opencode"
        && input_execution
            .is_none_or(|proof| operation["native_refs"]["input_execution"] != proof.clone())
    {
        return Err(invalid_source(
            "OpenCode Goal terminal evidence is not the retained execution proof",
        ));
    }
    if retained != Some(evidence_value) {
        return Err(invalid_source(
            "Goal terminal evidence is not the exact retained producer receipt",
        ));
    }
    if evidence.source == "codex" {
        let marker = &operation["result"]["details"]["goal_terminal_event"];
        let retained_event = marker.get("event").ok_or_else(|| {
            invalid_source("Codex terminal journal event is not retained with its Operation")
        })?;
        let retained_record = marker.get("event_record").ok_or_else(|| {
            invalid_source("Codex terminal journal record is not retained with its Operation")
        })?;
        if retained_event != &serde_json::to_value(&evidence.terminal_event)?
            || retained_record["id"] != retained_event["id"]
            || retained_record["seq"] != retained_event["seq"]
            || retained_record["kind"] != "turn.completed"
            || retained_record["status"] != "completed"
            || retained_event["sha256"].as_str()
                != Some(model::digest(model::canonical(retained_record)?.as_bytes()).as_str())
        {
            return Err(invalid_source(
                "Codex terminal journal EventRef does not match its retained record",
            ));
        }
    }
    let (native_session, native_input) = if evidence.source == "codex" {
        (
            operation["native_refs"]["session_id"].as_str(),
            operation["native_refs"]["input_id"].as_str(),
        )
    } else {
        (
            operation["native_refs"]["input_execution"]["native_session_id"].as_str(),
            operation["native_refs"]["input_execution"]["native_input_id"].as_str(),
        )
    };
    if native_session != Some(evidence.native_session_id.as_str())
        || native_input != Some(evidence.native_input_id.as_str())
    {
        return Err(invalid_source(
            "Goal terminal evidence native identity differs from the retained Operation",
        ));
    }
    let retained_run = if evidence.source == "codex" {
        operation["native_refs"]["turn_id"]
            .as_str()
            .filter(|value| !value.is_empty())
    } else {
        operation["native_refs"]["input_execution"]["native_run_id"]
            .as_str()
            .filter(|value| !value.is_empty())
    };
    if retained_run != evidence.native_run_id.as_deref() {
        return Err(invalid_source(
            "Goal terminal evidence native run identity differs from the retained Operation",
        ));
    }
    if operation["method"] == "agent.send" {
        validate_retained_goal_continuation_admission(db, operation_id, operation, &evidence)?;
    }
    Ok(json!({
        "observation_id":observation_id,
        "source_operation_id":operation_id,
        "source_method":operation["method"],
        "task_id":evidence.task_id,
        "task_revision":evidence.task_revision,
        "attempt_id":evidence.attempt_id,
        "binding_id":evidence.binding_id,
        "binding_generation":evidence.binding_generation,
        "native_session_id":evidence.native_session_id,
        "native_input_id":evidence.native_input_id,
        "terminal_event":serde_json::to_value(&evidence.terminal_event)?,
        "native_run_id":evidence.native_run_id,
        "disposition":evidence.disposition,
    }))
}

/// A Codex Goal continuation is an ordinary `agent.send`, so its terminal
/// marker is only meaningful when the Store-authenticated closed continuation
/// receipt is retained with the same Operation.  Revalidation is entirely
/// historical: it follows the immutable effective cause and Operation tuple and
/// never asks the current binding boot, Task/Attempt owner, or manager for
/// permission again.
fn validate_retained_goal_continuation_admission(
    db: &Connection,
    operation_id: &str,
    operation: &Value,
    evidence: &GoalTerminalEvidence,
) -> Result<()> {
    let details = operation["result"]["details"]
        .as_object()
        .ok_or_else(|| invalid_source("Codex continuation result details are missing"))?;
    let receipt: GoalContinuationAdmissionReceipt = serde_json::from_value(
        details
            .get("goal_continuation_admission")
            .cloned()
            .ok_or_else(|| {
                invalid_source("Codex continuation terminal result has no admission receipt")
            })?,
    )
    .map_err(|_| invalid_source("Codex continuation admission receipt is malformed"))?;
    receipt
        .validate()
        .map_err(|_| invalid_source("Codex continuation admission receipt is invalid"))?;
    let module_receipt: ModuleReceiptIdentity = serde_json::from_value(
        details
            .get("module_receipt")
            .cloned()
            .ok_or_else(|| invalid_source("Codex continuation module receipt is missing"))?,
    )
    .map_err(|_| invalid_source("Codex continuation module receipt is malformed"))?;
    module_receipt
        .validate()
        .map_err(|_| invalid_source("Codex continuation module receipt is invalid"))?;
    if receipt.module_receipt != module_receipt
        || receipt.context.operation_id != operation_id
        || receipt.context.binding_id != operation["binding_id"].as_str().unwrap_or_default()
        || receipt.context.binding_generation
            != operation["binding_generation"].as_i64().unwrap_or_default()
        || receipt.native_input_id.as_deref() != Some(evidence.native_input_id.as_str())
        || operation["native_refs"]["input_id"].as_str() != Some(evidence.native_input_id.as_str())
    {
        return Err(invalid_source(
            "Codex continuation receipt does not match the terminal Operation and native input",
        ));
    }
    let effective_raw: String = db.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| row.get(0),
    )?;
    let effective: Value = serde_json::from_str(&effective_raw)
        .map_err(|_| invalid_source("Codex continuation effective request is malformed"))?;
    if effective["automation_on_behalf"]["action"] != "agent.send"
        || effective["automation_on_behalf"]["cause"]["kind"] != "goal_progression"
    {
        return Err(invalid_source(
            "Codex continuation admission is outside the retained Goal cause",
        ));
    }
    let expected_continuation: GoalContinuationLink =
        serde_json::from_value(effective["automation_on_behalf"]["cause"]["continuation"].clone())
            .map_err(|_| invalid_source("Codex continuation cause linkage is malformed"))?;
    if receipt.context.continuation != expected_continuation {
        return Err(invalid_source(
            "Codex continuation receipt differs from the retained prior EventRef linkage",
        ));
    }
    Ok(())
}

/// The common Goal DTO is an additional typed terminal receipt.  OpenCode
/// still has to satisfy the original execution-proof reader before that DTO
/// can be consumed; the DTO must never turn a partial input receipt into a
/// terminal source by itself.
fn validate_opencode_execution_proof(operation: &Value, proof: &Value) -> Result<()> {
    if operation["native_refs"]["input_execution"] != *proof
        || proof["reader_revision"] != "opencode-execution-log-v1"
        || proof["correlation"] != "durable_serialized_execution"
        || proof["disposition"] != "completed"
        || proof["uncertainty"].is_string()
        || !proof["admission"].is_object()
        || !proof["delivery"].is_object()
        || !proof["execution_started"].is_object()
        || proof["terminal"]["outcome"] != "completed"
        || proof["terminal"]["event"]["id"].as_str().is_none()
        || proof["terminal"]["event"]["seq"]
            .as_i64()
            .is_none_or(|seq| seq < 1)
        || proof["terminal"]["event"]["sha256"]
            .as_str()
            .is_none_or(|hash| !is_digest(hash))
        || proof["native_input_id"].as_str().is_none()
        || proof["native_session_id"].as_str().is_none()
    {
        return Err(invalid_source(
            "exact synchronized OpenCode terminal proof is incomplete",
        ));
    }
    if let Some(retained_run) = operation["native_refs"]["input_execution"]["native_run_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        && proof["native_run_id"].as_str() != Some(retained_run)
    {
        return Err(invalid_source(
            "OpenCode terminal proof native run differs from the retained execution",
        ));
    }
    Ok(())
}

fn verified_terminal(
    db: &Connection,
    observation_id: i64,
    operation_id: &str,
    raw: &str,
) -> Result<Value> {
    if operation_id.is_empty() {
        return Err(invalid_source(
            "terminal observation has no immutable Operation ID",
        ));
    }
    let proof: Value = serde_json::from_str(raw)
        .map_err(|_| invalid_source("terminal observation payload is invalid"))?;
    let operation = operations::get_operation(db, operation_id).map_err(|error| {
        if error.code == "NOT_FOUND" {
            Error::new(
                "AUTOMATION_GOAL_SOURCE_GAP",
                "source Operation is not retained yet",
            )
        } else {
            error
        }
    })?;
    if operation["method"] != "task.dispatch"
        && operation["method"] != "agent.send"
        && operation["method"] != "agent.goal"
    {
        return Err(invalid_source(
            "terminal observation refers to an unsupported Operation",
        ));
    }
    // Only an unsettled lifecycle can still acquire the synchronized native
    // receipt. Rejected/cancelled operations are terminal non-continuable
    // facts; an unknown state is malformed evidence, not a retry condition.
    match operation["state"].as_str() {
        Some("settled") => {}
        Some("queued" | "sending" | "native_accepted" | "outcome_unknown") => {
            return Err(Error::new(
                "AUTOMATION_GOAL_SOURCE_GAP",
                "source Operation is not settled and may still acquire synchronized execution evidence",
            ));
        }
        Some("rejected" | "cancelled") => {
            return Err(invalid_source(
                "source Operation has a terminal non-continuable disposition",
            ));
        }
        Some(_) | None => {
            return Err(invalid_source(
                "source Operation has an unknown or malformed state",
            ));
        }
    }
    if operation["method"] == "agent.goal"
        && original_request(db, operation_id)?["action"] != "continue"
    {
        return Err(invalid_source(
            "only explicit Goal continuation inputs are progression sources",
        ));
    }
    if proof["schema_id"].as_str()
        == Some(swarm_contracts::module_contract::GOAL_TERMINAL_EVIDENCE_SCHEMA_ID)
    {
        if operation["method"] == "agent.goal" {
            return Err(invalid_source(
                "Goal continuation Operations cannot be terminal source evidence",
            ));
        }
        return verified_goal_terminal_evidence(
            db,
            observation_id,
            operation_id,
            &proof,
            &operation,
            None,
        );
    }
    if proof["goal_terminal_evidence"].is_object() {
        if proof["goal_terminal_evidence"]["source"] == "opencode" {
            validate_opencode_execution_proof(&operation, &proof)?;
        }
        return verified_goal_terminal_evidence(
            db,
            observation_id,
            operation_id,
            &proof["goal_terminal_evidence"],
            &operation,
            Some(&proof),
        );
    }
    if operation["native_refs"]["input_execution"].is_null() {
        return Err(Error::new(
            "AUTOMATION_GOAL_SOURCE_GAP",
            "settled source Operation has no synchronized execution receipt yet",
        ));
    }
    validate_opencode_execution_proof(&operation, &proof)?;
    if operation["task_id"].as_str().is_none()
        || operation["attempt_id"].as_str().is_none()
        || operation["binding_id"].as_str().is_none()
        || operation["binding_generation"]
            .as_i64()
            .is_none_or(|generation| generation < 1)
    {
        return Err(invalid_source(
            "exact source Operation Task/Attempt linkage is missing",
        ));
    }
    Ok(json!({
        "observation_id":observation_id,
        "source_operation_id":operation_id,
        "source_method":operation["method"],
        "task_id":operation["task_id"],
        "task_revision":source_task_revision(db, &operation)?,
        "attempt_id":operation["attempt_id"],
        "binding_id":operation["binding_id"],
        "binding_generation":operation["binding_generation"],
        "native_session_id":proof["native_session_id"],
        "native_input_id":proof["native_input_id"],
        "terminal_event":proof["terminal"]["event"],
        "native_run_id":proof["native_run_id"],
        "disposition":proof["disposition"],
    }))
}

fn is_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Resolve only controller-recorded Goal state for this exact binding
/// generation. A missing history means the adapter must still prove that the
/// native entry is absent (`expected_revision = 0`) before it creates one.
/// A non-applied or unresolved prior Goal Operation leaves ownership unclear.
fn native_goal_predecessor(
    db: &Connection,
    binding_id: &str,
    generation: i64,
    before_operation_id: Option<&str>,
    objective: &str,
) -> Result<(Option<String>, i64)> {
    let row: Option<(String, String, Option<String>)> = if let Some(before) = before_operation_id {
        let boundary: Option<String> = db
            .query_row(
                "SELECT method FROM operations WHERE operation_id=?1",
                [before],
                |row| row.get(0),
            )
            .optional()?;
        if boundary.as_deref() != Some("agent.goal") {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "Goal continuation Operation is outside its exact native binding",
            ));
        }
        db.query_row(
            "SELECT operation_id,state,result_json FROM operations \
             WHERE binding_id=?1 AND binding_generation=?2 AND method='agent.goal' \
               AND rowid < (SELECT rowid FROM operations WHERE operation_id=?3) \
             ORDER BY rowid DESC LIMIT 1",
            params![binding_id, generation, before],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?
    } else {
        db.query_row(
            "SELECT operation_id,state,result_json FROM operations \
             WHERE binding_id=?1 AND binding_generation=?2 AND method='agent.goal' \
             ORDER BY rowid DESC LIMIT 1",
            params![binding_id, generation],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?
    };
    let Some((operation_id, state, raw_result)) = row else {
        return Ok((None, 0));
    };
    if state != "settled" {
        return Err(Error::new(
            "GOAL_NATIVE_STATE_UNRESOLVED",
            format!("native Goal predecessor Operation {operation_id} is {state}"),
        ));
    }
    let result: Value = serde_json::from_str(&raw_result.ok_or_else(|| {
        Error::new(
            "GOAL_NATIVE_STATE_UNRESOLVED",
            "settled Goal predecessor has no result",
        )
    })?)
    .map_err(|_| {
        Error::new(
            "GOAL_NATIVE_STATE_UNRESOLVED",
            "Goal predecessor result is invalid",
        )
    })?;
    if result["outcome"] != "applied" || !result["details"]["goal"].is_object() {
        return Err(Error::new(
            "GOAL_NATIVE_STATE_UNRESOLVED",
            format!(
                "native Goal predecessor Operation {operation_id} has no exact applied record receipt"
            ),
        ));
    }
    let goal = &result["details"]["goal"];
    if goal["present"] == false {
        if goal["revision"].is_null() && goal["status"].is_null() {
            return Ok((Some(operation_id), 0));
        }
        return Err(Error::new(
            "GOAL_NATIVE_STATE_UNRESOLVED",
            "absent Goal predecessor contains inconsistent revision or status",
        ));
    }
    let revision = goal["revision"]
        .as_i64()
        .filter(|revision| *revision > 0)
        .ok_or_else(|| {
            Error::new(
                "GOAL_NATIVE_STATE_UNRESOLVED",
                "applied native Goal has no positive revision",
            )
        })?;
    let expected_digest = format!(
        "sha256:{}",
        model::digest(model::canonical(&json!(objective))?.as_bytes()),
    );
    if goal["present"] != true
        || goal["status"] != "active"
        || goal["objective_digest"] != expected_digest
    {
        return Err(Error::new(
            "GOAL_NATIVE_OWNERSHIP_CONFLICT",
            format!(
                "native Goal predecessor Operation {operation_id} does not retain the selected active objective"
            ),
        ));
    }
    Ok((Some(operation_id), revision))
}

fn target_for(
    db: &Connection,
    entry: &AutomationEntry,
    fact: &Value,
    now_ms: i64,
) -> Result<Option<Value>> {
    let Some(settings) = entry.goal_progression.as_ref() else {
        return Ok(None);
    };
    let task_id = fact["task_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_source("source Task ID is missing"))?;
    let Some(target) = super::goals::progression_target(
        db,
        &entry.project_id,
        task_id,
        &settings.goal_id,
        now_ms,
    )?
    else {
        return Ok(None);
    };
    if target["scope"]["task_revision"] != fact["task_revision"]
        || target["scope"]["attempt_id"] != fact["attempt_id"]
        || target["binding_id"] != fact["binding_id"]
        || target["binding_generation"] != fact["binding_generation"]
    {
        return Ok(None);
    }
    Ok(Some(target))
}

fn prepare(
    db: &Connection,
    entry: &AutomationEntry,
    fact: &Value,
    target: &Value,
    slot_id: &str,
) -> Result<GoalProgressionAdmission> {
    if target["active"] != true || target["completion"]["status"] != "pending" {
        return Err(Error::new(
            "GOAL_NOT_ACTIVE",
            "exact Task/Attempt is no longer active and incomplete",
        ));
    }
    if !crate::automation::authorization::current_manager_id_has_task_scope(
        db,
        &entry.owner_manager_id,
        target["scope"]["task_id"].as_str().unwrap_or_default(),
        &entry.project_id,
    )? {
        return Err(Error::new(
            "GOAL_MANAGER_SCOPE_CONFLICT",
            "automation owner has no current Manager scope for the exact Attempt",
        ));
    }
    let binding_id = target["binding_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_source("Goal Attempt has no immutable binding"))?;
    let binding_generation = target["binding_generation"]
        .as_i64()
        .filter(|generation| *generation > 0)
        .ok_or_else(|| invalid_source("Goal Attempt has no immutable binding generation"))?;
    let binding = operations::get_binding(db, binding_id, binding_generation)?;
    let continuation_method = if crate::runtime::codex::is_controller_route(&binding["route"]) {
        "agent.send"
    } else if binding["route"]["runtime"] == crate::runtime::opencode_v2::RUNTIME {
        "agent.goal"
    } else {
        ""
    };
    if continuation_method.is_empty()
        || !binding["released_at_ms"].is_null()
        || binding["state"] != "ready"
        || binding["native_root_id"].as_str().is_none()
    {
        return Err(Error::new(
            "GOAL_UNSUPPORTED_RUNTIME",
            "Goal progression requires a ready OpenCode controller or Codex controller binding",
        ));
    }
    let objective = target["objective"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            invalid_source("shared Goal objective is missing from the retained Goal record")
        })?;
    let (native_goal_predecessor_operation_id, expected_revision) =
        if continuation_method == "agent.goal" {
            let (predecessor, revision) =
                native_goal_predecessor(db, binding_id, binding_generation, None, objective)?;
            (predecessor, Some(revision))
        } else {
            (None, None)
        };
    let request_id = if continuation_method == "agent.send" {
        format!("o9gc:{}", slot_id)
    } else {
        format!("o9gp:{}", slot_id)
    };
    let request = if continuation_method == "agent.send" {
        json!({
            "client_request_id":request_id,
            "binding_id":binding_id,
            "generation":binding_generation,
            "text":objective,
            "delivery":"next_turn",
        })
    } else {
        json!({
            "client_request_id":request_id,
            "binding_id":binding_id,
            "generation":binding_generation,
            "action":"continue",
            "objective":objective,
            "expected_revision":expected_revision.unwrap_or_default(),
        })
    };
    let continuation = GoalContinuationLink {
        schema_id: swarm_contracts::module_contract::GOAL_CONTINUATION_SCHEMA_ID.to_owned(),
        schema_version: GoalContinuationLink::VERSION,
        method: continuation_method.to_owned(),
        owner: GoalContinuationLink::OWNER.to_owned(),
        source_operation_id: fact["source_operation_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        source_observation_id: fact["observation_id"].as_i64().unwrap_or_default(),
        terminal_event: serde_json::from_value(fact["terminal_event"].clone())
            .map_err(|_| invalid_source("terminal EventRef cannot form continuation linkage"))?,
        native_session_id: fact["native_session_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        native_input_id: fact["native_input_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        native_run_id: fact["native_run_id"].as_str().map(str::to_owned),
        task_id: target["scope"]["task_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        task_revision: target["scope"]["task_revision"]
            .as_i64()
            .unwrap_or_default(),
        attempt_id: target["scope"]["attempt_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        binding_id: binding_id.to_owned(),
        binding_generation,
        goal_id: target["goal_id"].as_str().unwrap_or_default().to_owned(),
        goal_revision: target["revision"].as_i64().unwrap_or_default(),
        objective_sha256: model::digest(objective.as_bytes()),
    };
    continuation
        .validate()
        .map_err(|_| invalid_source("continuation linkage is invalid"))?;
    let continuation_value = serde_json::to_value(&continuation)?;
    let cause = json!({
        "kind":"goal_progression",
        "id":slot_id,
        "continuation":continuation_value.clone(),
        "continuation_owner":GoalContinuationLink::OWNER,
        "source_operation_id":fact["source_operation_id"],
        "source_observation_id":fact["observation_id"],
        "terminal_event":fact["terminal_event"],
        "native_session_id":fact["native_session_id"],
        "native_input_id":fact["native_input_id"],
        "native_run_id":fact["native_run_id"],
        "goal_id":target["goal_id"],
        "goal_revision":target["revision"],
        "goal_active":target["active"],
        "completion_status":target["completion"]["status"],
        "goal_owner_manager_id":target["created_by_manager_id"],
        "goal_last_reviser_manager_id":target["updated_by_manager_id"],
        "objective_sha256":model::digest(target["objective"].as_str().unwrap_or_default().as_bytes()),
        "task_id":target["scope"]["task_id"],
        "task_revision":target["scope"]["task_revision"],
        "attempt_id":target["scope"]["attempt_id"],
        "binding_id":binding_id,
        "binding_generation":binding_generation,
        "expected_native_goal_revision":expected_revision,
        "native_goal_predecessor_operation_id":native_goal_predecessor_operation_id.clone(),
    });
    let linkage = json!({
        "schema_version":1,
        "technical_requester_id":crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID,
        "effective_manager_id":entry.owner_manager_id,
        "automation_id":entry.automation_id,
        "automation_revision":entry.revision,
        "project_id":entry.project_id,
        "action":continuation_method,
        "semantic_cause_kind":"goal_progression",
        "semantic_cause_id":slot_id,
        "cause":cause,
        "kind":"goal_progression",
        "id":slot_id,
        "goal_id":target["goal_id"],
        "goal_revision":target["revision"],
        "goal_active":target["active"],
        "completion_status":target["completion"]["status"],
        "goal_owner_manager_id":target["created_by_manager_id"],
        "goal_last_reviser_manager_id":target["updated_by_manager_id"],
        "task_id":target["scope"]["task_id"],
        "task_revision":target["scope"]["task_revision"],
        "attempt_id":target["scope"]["attempt_id"],
        "binding_id":binding_id,
        "binding_generation":binding_generation,
        "source_operation_id":fact["source_operation_id"],
        "source_observation_id":fact["observation_id"],
        "terminal_event":fact["terminal_event"],
        "native_session_id":fact["native_session_id"],
        "native_input_id":fact["native_input_id"],
        "native_run_id":fact["native_run_id"],
        "semantic_slot_id":slot_id,
        "expected_native_goal_revision":expected_revision,
        "native_goal_predecessor_operation_id":native_goal_predecessor_operation_id,
        "continuation_owner":GoalContinuationLink::OWNER,
        "continuation":continuation_value,
    });
    Ok(GoalProgressionAdmission {
        request,
        linkage,
        continuation_method: continuation_method.to_owned(),
        semantic_slot_id: slot_id.to_owned(),
        effective_manager_id: entry.owner_manager_id.clone(),
        task_id: target["scope"]["task_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        task_revision: target["scope"]["task_revision"]
            .as_i64()
            .unwrap_or_default(),
        attempt_id: target["scope"]["attempt_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        binding_id: binding_id.to_owned(),
        binding_generation,
    })
}

fn require_current_admission(
    db: &Connection,
    admission: &GoalProgressionAdmission,
    now_ms: i64,
    before_operation_id: Option<&str>,
) -> Result<()> {
    let linkage = &admission.linkage;
    let stale = || {
        Error::new(
            "AUTOMATION_ACTION_CHANGED",
            "Goal progression authority or exact target changed before admission",
        )
    };
    let owner = linkage["effective_manager_id"].as_str().ok_or_else(stale)?;
    let project = linkage["project_id"].as_str().ok_or_else(stale)?;
    let automation_id = linkage["automation_id"].as_str().ok_or_else(stale)?;
    let entry = config::load_entry(db, owner, project, automation_id)?.ok_or_else(stale)?;
    if !entry.goal_progression_ready()
        || entry.revision != linkage["automation_revision"].as_i64().ok_or_else(stale)?
        || entry
            .goal_progression
            .as_ref()
            .map(|settings| settings.goal_id.as_str())
            != linkage["goal_id"].as_str()
        || entry.scope.work_pool_id.is_some()
    {
        return Err(stale());
    }
    let task_id = linkage["task_id"].as_str().ok_or_else(stale)?;
    let goal_id = linkage["goal_id"].as_str().ok_or_else(stale)?;
    let target = super::goals::progression_target(db, project, task_id, goal_id, now_ms)?
        .ok_or_else(stale)?;
    let requested_objective = if admission.continuation_method() == "agent.send" {
        admission.request.get("text")
    } else {
        admission.request.get("objective")
    };
    if target["active"] != linkage["goal_active"]
        || linkage["goal_active"] != true
        || target["completion"]["status"] != linkage["completion_status"]
        || linkage["completion_status"] != "pending"
        || target["revision"] != linkage["goal_revision"]
        || target["created_by_manager_id"] != linkage["goal_owner_manager_id"]
        || target["updated_by_manager_id"] != linkage["goal_last_reviser_manager_id"]
        || target["scope"]["task_revision"] != linkage["task_revision"]
        || target["scope"]["attempt_id"] != linkage["attempt_id"]
        || target["binding_id"] != linkage["binding_id"]
        || target["binding_generation"] != linkage["binding_generation"]
        || target.get("objective") != requested_objective
    {
        return Err(stale());
    }
    if !crate::automation::authorization::current_manager_id_has_task_scope(
        db, owner, task_id, project,
    )? {
        return Err(Error::new(
            "GOAL_MANAGER_SCOPE_CONFLICT",
            "automation owner no longer has current Manager scope for the exact Attempt",
        ));
    }
    let binding =
        operations::get_binding(db, admission.binding_id(), admission.binding_generation())?;
    let supported_binding = match admission.continuation_method() {
        "agent.goal" => binding["route"]["runtime"] == crate::runtime::opencode_v2::RUNTIME,
        "agent.send" => crate::runtime::codex::is_controller_route(&binding["route"]),
        _ => false,
    };
    if !supported_binding
        || !binding["released_at_ms"].is_null()
        || binding["state"] != "ready"
        || binding["native_root_id"].as_str().is_none()
    {
        return Err(Error::new(
            "GOAL_UNSUPPORTED_RUNTIME",
            "Goal continuation requires its exact ready OpenCode or Codex controller binding",
        ));
    }
    let request_valid = if admission.continuation_method() == "agent.send" {
        admission.request["binding_id"] == admission.binding_id()
            && admission.request["generation"] == admission.binding_generation()
            && admission.request["client_request_id"]
                == format!("o9gc:{}", admission.semantic_slot_id())
            && admission.request["delivery"] == "next_turn"
            && admission.request["text"]
                .as_str()
                .is_some_and(|text| !text.is_empty())
            && admission.request.get("action").is_none()
            && admission.request.get("expected_revision").is_none()
    } else {
        admission.request["action"] == "continue"
            && admission.request["binding_id"] == admission.binding_id()
            && admission.request["generation"] == admission.binding_generation()
            && admission.request["client_request_id"]
                == format!("o9gp:{}", admission.semantic_slot_id())
            && admission.request["expected_revision"] == linkage["expected_native_goal_revision"]
    };
    let continuation: GoalContinuationLink =
        serde_json::from_value(linkage["continuation"].clone()).map_err(|_| {
            Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "Goal continuation linkage is invalid",
            )
        })?;
    if !request_valid
        || continuation.method != admission.continuation_method()
        || continuation.owner != GoalContinuationLink::OWNER
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "Goal progression request differs from its immutable admission linkage",
        ));
    }
    if admission.continuation_method() == "agent.goal" {
        let (native_predecessor, native_revision) = native_goal_predecessor(
            db,
            admission.binding_id(),
            admission.binding_generation(),
            before_operation_id,
            admission.request["objective"].as_str().ok_or_else(stale)?,
        )?;
        if linkage["native_goal_predecessor_operation_id"]
            != native_predecessor
                .as_deref()
                .map_or(Value::Null, |id| json!(id))
            || linkage["expected_native_goal_revision"] != json!(native_revision)
        {
            return Err(Error::new(
                "GOAL_NATIVE_OWNERSHIP_CONFLICT",
                "the controller-recorded native Goal changed before continuation admission",
            ));
        }
    }
    let observation_id = linkage["source_observation_id"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(stale)?;
    let source_operation_id = linkage["source_operation_id"].as_str().ok_or_else(stale)?;
    let raw: Option<String> = db.query_row(
        "SELECT payload_json FROM observations WHERE observation_id=?1 AND operation_id=?2 AND kind IN ('opencode.input_execution','goal.terminal.evidence')",
        params![observation_id, source_operation_id],
        |row| row.get(0),
    ).optional()?;
    let raw = raw.ok_or_else(stale)?;
    let fact = verified_terminal(db, observation_id, source_operation_id, &raw)?;
    if fact["terminal_event"] != linkage["terminal_event"]
        || fact["native_session_id"] != linkage["native_session_id"]
        || fact["native_input_id"] != linkage["native_input_id"]
        || fact["task_id"] != task_id
        || fact["task_revision"] != linkage["task_revision"]
        || fact["attempt_id"] != linkage["attempt_id"]
        || fact["binding_id"] != linkage["binding_id"]
        || fact["binding_generation"] != linkage["binding_generation"]
        || fact["native_run_id"] != linkage["native_run_id"]
    {
        return Err(stale());
    }
    Ok(())
}

/// Persist the validated on-behalf attribution after the ordinary runtime
/// Operation has been reserved, within the same mutation transaction.
pub(crate) fn retain_operation_link(
    tx: &Transaction<'_>,
    operation_id: &str,
    admission: &GoalProgressionAdmission,
    now_ms: i64,
) -> Result<()> {
    admission.require_current_for_operation(tx, operation_id, now_ms)?;
    crate::automation::authorization::save_goal_progression_operation_link(
        tx,
        operation_id,
        &admission.linkage,
        now_ms,
    )?;
    Ok(())
}

/// Validate the permanent Operation attribution against the exact ordinary
/// request, terminal proof, and terminal-slot receipt. This deliberately uses
/// the saved admission receipt rather than requiring the Goal to remain
/// current forever after admission.
pub(crate) fn validate_operation_link(
    db: &Connection,
    link: &crate::automation::authorization::OnBehalfOperationLink,
) -> Result<()> {
    let corrupt = || {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "Goal progression Operation attribution does not match its durable terminal receipt",
        )
    };
    let cause = &link.cause;
    if !matches!(link.action.as_str(), "agent.goal" | "agent.send")
        || cause["kind"] != "goal_progression"
        || cause["id"].as_str().is_none_or(str::is_empty)
        || cause["goal_id"].as_str().is_none_or(str::is_empty)
        || cause["goal_revision"]
            .as_i64()
            .is_none_or(|value| value <= 0)
        || cause["goal_active"] != true
        || cause["completion_status"] != "pending"
        || cause["goal_owner_manager_id"]
            .as_str()
            .is_none_or(str::is_empty)
        || cause["goal_last_reviser_manager_id"]
            .as_str()
            .is_none_or(str::is_empty)
        || cause["objective_sha256"]
            .as_str()
            .is_none_or(|value| !is_digest(value))
        || cause["task_id"].as_str().is_none_or(str::is_empty)
        || cause["task_revision"]
            .as_i64()
            .is_none_or(|value| value <= 0)
        || cause["attempt_id"].as_str().is_none_or(str::is_empty)
        || cause["binding_id"].as_str().is_none_or(str::is_empty)
        || cause["binding_generation"]
            .as_i64()
            .is_none_or(|value| value <= 0)
        || cause["source_operation_id"]
            .as_str()
            .is_none_or(str::is_empty)
        || cause["source_observation_id"]
            .as_i64()
            .is_none_or(|value| value <= 0)
        || cause["native_session_id"]
            .as_str()
            .is_none_or(str::is_empty)
        || cause["native_input_id"].as_str().is_none_or(str::is_empty)
        || (link.action == "agent.goal"
            && cause["expected_native_goal_revision"]
                .as_i64()
                .is_none_or(|value| value < 0))
        || (link.action == "agent.goal"
            && !cause["native_goal_predecessor_operation_id"].is_null()
            && cause["native_goal_predecessor_operation_id"]
                .as_str()
                .is_none_or(str::is_empty))
    {
        return Err(corrupt());
    }
    let slot_id = cause["id"].as_str().ok_or_else(corrupt)?;
    let row: Option<LinkedGoalOperationRow> = db.query_row(
        "SELECT caller_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation FROM operations WHERE operation_id=?1",
        [&link.operation_id],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?)),
    ).optional()?;
    let Some((
        caller,
        method,
        original_raw,
        effective_raw,
        task_id,
        attempt_id,
        binding_id,
        generation,
    )) = row
    else {
        return Err(corrupt());
    };
    if caller != link.technical_requester_id
        || caller != crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
        || method != link.action
        || task_id.as_deref() != cause["task_id"].as_str()
        || attempt_id.as_deref() != cause["attempt_id"].as_str()
        || binding_id.as_deref() != cause["binding_id"].as_str()
        || generation != cause["binding_generation"].as_i64()
    {
        return Err(corrupt());
    }
    let request: Value = serde_json::from_str(&original_raw).map_err(|_| corrupt())?;
    let effective: Value = serde_json::from_str(&effective_raw).map_err(|_| corrupt())?;
    let request_valid = if link.action == "agent.send" {
        request["binding_id"] == cause["binding_id"]
            && request["generation"] == cause["binding_generation"]
            && request["client_request_id"] == format!("o9gc:{slot_id}")
            && request["delivery"] == "next_turn"
            && request["text"].as_str().is_some_and(|text| {
                model::digest(text.as_bytes())
                    == cause["objective_sha256"].as_str().unwrap_or_default()
            })
            && request.get("action").is_none()
            && request.get("expected_revision").is_none()
    } else {
        request["action"] == "continue"
            && request["binding_id"] == cause["binding_id"]
            && request["generation"] == cause["binding_generation"]
            && request["expected_revision"] == cause["expected_native_goal_revision"]
            && request["client_request_id"] == format!("o9gp:{slot_id}")
            && request["objective"].as_str().is_some_and(|objective| {
                model::digest(objective.as_bytes())
                    == cause["objective_sha256"].as_str().unwrap_or_default()
            })
    };
    if link.action == "agent.send" || cause.get("continuation").is_some() {
        let continuation: GoalContinuationLink =
            serde_json::from_value(cause["continuation"].clone()).map_err(|_| corrupt())?;
        if continuation.validate().is_err()
            || continuation.method != link.action
            || cause["source_operation_id"].as_str()
                != Some(continuation.source_operation_id.as_str())
            || cause["source_observation_id"].as_i64() != Some(continuation.source_observation_id)
            || serde_json::to_value(&continuation.terminal_event).map_err(|_| corrupt())?
                != cause["terminal_event"]
            || cause["native_session_id"].as_str() != Some(continuation.native_session_id.as_str())
            || cause["native_input_id"].as_str() != Some(continuation.native_input_id.as_str())
            || continuation.native_run_id != cause["native_run_id"].as_str().map(str::to_owned)
            || cause["task_id"].as_str() != Some(continuation.task_id.as_str())
            || cause["task_revision"].as_i64() != Some(continuation.task_revision)
            || cause["attempt_id"].as_str() != Some(continuation.attempt_id.as_str())
            || cause["binding_id"].as_str() != Some(continuation.binding_id.as_str())
            || cause["binding_generation"].as_i64() != Some(continuation.binding_generation)
            || cause["goal_id"].as_str() != Some(continuation.goal_id.as_str())
            || cause["goal_revision"].as_i64() != Some(continuation.goal_revision)
            || cause["objective_sha256"].as_str() != Some(continuation.objective_sha256.as_str())
            || !request_valid
            || effective["automation_on_behalf"]["technical_requester_id"]
                != link.technical_requester_id
            || effective["automation_on_behalf"]["effective_manager_id"]
                != link.effective_manager_id
            || effective["automation_on_behalf"]["automation_id"] != link.automation_id
            || effective["automation_on_behalf"]["automation_revision"] != link.automation_revision
            || effective["automation_on_behalf"]["project_id"] != link.project_id
            || effective["automation_on_behalf"]["action"] != link.action
            || effective["automation_on_behalf"]["cause"] != *cause
        {
            return Err(corrupt());
        }
    } else if !request_valid
        || effective["automation_on_behalf"]["technical_requester_id"]
            != link.technical_requester_id
        || effective["automation_on_behalf"]["effective_manager_id"] != link.effective_manager_id
        || effective["automation_on_behalf"]["automation_id"] != link.automation_id
        || effective["automation_on_behalf"]["automation_revision"] != link.automation_revision
        || effective["automation_on_behalf"]["project_id"] != link.project_id
        || effective["automation_on_behalf"]["action"] != link.action
        || effective["automation_on_behalf"]["cause"] != *cause
    {
        return Err(corrupt());
    }
    if link.action == "agent.goal" {
        let (native_predecessor, native_revision) = native_goal_predecessor(
            db,
            cause["binding_id"].as_str().ok_or_else(corrupt)?,
            cause["binding_generation"].as_i64().ok_or_else(corrupt)?,
            Some(&link.operation_id),
            request["objective"].as_str().ok_or_else(corrupt)?,
        )
        .map_err(|_| corrupt())?;
        if cause["native_goal_predecessor_operation_id"]
            != native_predecessor
                .as_deref()
                .map_or(Value::Null, |id| json!(id))
            || cause["expected_native_goal_revision"] != json!(native_revision)
        {
            return Err(corrupt());
        }
    }
    let source_operation_id = cause["source_operation_id"].as_str().ok_or_else(corrupt)?;
    let source_observation_id = cause["source_observation_id"]
        .as_i64()
        .ok_or_else(corrupt)?;
    let raw: Option<String> = db.query_row(
        "SELECT payload_json FROM observations WHERE observation_id=?1 AND operation_id=?2 AND kind IN ('opencode.input_execution','goal.terminal.evidence')",
        params![source_observation_id, source_operation_id],
        |row| row.get(0),
    ).optional()?;
    let raw = raw.ok_or_else(corrupt)?;
    let fact = verified_terminal(db, source_observation_id, source_operation_id, &raw)?;
    if event_slot_id(&fact)? != slot_id
        || fact["terminal_event"] != cause["terminal_event"]
        || fact["native_session_id"] != cause["native_session_id"]
        || fact["native_input_id"] != cause["native_input_id"]
        || fact["task_id"] != cause["task_id"]
        || fact["task_revision"] != cause["task_revision"]
        || fact["attempt_id"] != cause["attempt_id"]
        || fact["binding_id"] != cause["binding_id"]
        || fact["binding_generation"] != cause["binding_generation"]
        || fact["native_run_id"] != cause["native_run_id"]
    {
        return Err(corrupt());
    }
    let slot_key = format!("{SLOT_PREFIX}{slot_id}");
    let value = config::read_record(db, &slot_key, "Goal progression terminal slot")?
        .ok_or_else(corrupt)?;
    let slot: TerminalSlot = serde_json::from_value(value).map_err(|_| corrupt())?;
    if slot.schema_version != SLOT_SCHEMA
        || slot.event_slot_id != slot_id
        || slot.terminal_event != cause["terminal_event"]
        || slot.native_session_id != cause["native_session_id"]
        || slot.native_input_id != cause["native_input_id"]
        || slot.source_observation_id != source_observation_id
        || slot.source_operation_id != source_operation_id
        || slot.manager_id != link.effective_manager_id
        || slot.automation_id != link.automation_id
        || slot.automation_revision != link.automation_revision
        || slot.goal_id != cause["goal_id"]
        || slot.goal_revision != cause["goal_revision"]
        || slot.goal_active != cause["goal_active"]
        || slot.completion_status != cause["completion_status"]
        || slot.goal_owner_manager_id != cause["goal_owner_manager_id"]
        || slot.goal_last_reviser_manager_id != cause["goal_last_reviser_manager_id"]
        || slot.objective_sha256 != cause["objective_sha256"]
        || slot.task_id != cause["task_id"]
        || slot.task_revision != cause["task_revision"]
        || slot.attempt_id != cause["attempt_id"]
        || slot.binding_id != cause["binding_id"]
        || slot.binding_generation != cause["binding_generation"]
        || !matches!(slot.disposition.as_str(), "admitted" | "reused")
        || slot.operation_id.as_deref() != Some(link.operation_id.as_str())
    {
        return Err(corrupt());
    }
    Ok(())
}

fn append_recent(state: &mut State, value: Value) {
    state.recent.push(value);
    trim_recent(&mut state.recent);
}

fn state_projection(
    db: &Connection,
    state: &State,
    processed: usize,
    quarantined: usize,
) -> Result<Value> {
    let mut projection = serde_json::to_value(state)?;
    projection["processed"] = json!(processed);
    projection["quarantined"] = json!(quarantined);
    projection["source_high_water"] = json!(observation_high_water(db)?);
    Ok(projection)
}

/// Transactional bounded pass. The supplied callback must route through the
/// ordinary manager on-behalf authority, mutate/Operation receipt path, and
/// return only after the exact controller-owned continuation Operation has
/// been admitted (`agent.goal` for OpenCode or `agent.send` for Codex).
pub(crate) fn reconcile_entry<F>(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    budget: usize,
    now_ms: i64,
    mut admit: F,
) -> Result<Value>
where
    F: FnMut(&Transaction<'_>, &GoalProgressionAdmission) -> Result<AdmissionResult>,
{
    if !entry.goal_progression_ready() {
        return Ok(
            json!({"automation_id":entry.automation_id,"processed":0,"waiting_for":"disabled_or_unselected"}),
        );
    }
    let mut state = load_state(tx, entry)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_GOAL_CURSOR_MISSING",
            "enabled Goal progression entry has no durable cursor",
        )
    })?;
    let current = config::load_entry(
        tx,
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?
    .ok_or_else(|| {
        Error::new(
            "AUTOMATION_ACTION_CHANGED",
            "current Goal progression entry is missing",
        )
    })?;
    if current != *entry || !current.goal_progression_ready() {
        return Err(Error::new(
            "AUTOMATION_ACTION_CHANGED",
            "Goal progression entry changed before terminal admission",
        ));
    }
    let source_high_water = observation_high_water(tx)?;
    let target = state
        .catch_up_until
        .map_or(source_high_water, |cut| source_high_water.min(cut));
    let pass_budget = budget.clamp(1, MAX_SOURCE_PAGE);
    let initial_pending_source_gaps = state.pending_source_gaps.len();
    let fresh_work_available = state.cursor < target;
    let pending_capacity_full = initial_pending_source_gaps >= MAX_PENDING_SOURCE_GAPS;
    let pending_recheck_budget = if initial_pending_source_gaps == 0 {
        0
    } else if pass_budget == 1 {
        if pending_capacity_full || !fresh_work_available || state.prefer_pending_source_retry {
            1
        } else {
            0
        }
    } else {
        let pending_budget = if fresh_work_available && !pending_capacity_full {
            pass_budget.saturating_sub(1)
        } else {
            pass_budget
        };
        pending_budget
            .min(initial_pending_source_gaps)
            .min(MAX_SOURCE_GAP_RECHECKS)
    };
    let mut processed = 0usize;
    let (mut rows, source_gap_checks, source_gap_quarantines) = if pending_recheck_budget == 0 {
        (Vec::new(), 0, 0)
    } else {
        recheck_pending_source_gaps(tx, &mut state, pending_recheck_budget, now_ms)?
    };
    let mut quarantined = source_gap_quarantines;
    processed += source_gap_checks;
    if pass_budget == 1 && initial_pending_source_gaps > 0 {
        state.prefer_pending_source_retry = pending_recheck_budget == 0;
    } else if initial_pending_source_gaps == 0 {
        state.prefer_pending_source_retry = false;
    }
    let remaining_budget = pass_budget.saturating_sub(processed);
    let page_limit = remaining_budget.min(MAX_SOURCE_PAGE);
    let can_read_page = page_limit > 0 && state.pending_source_gaps.len() < MAX_PENDING_SOURCE_GAPS;
    let page = if can_read_page {
        event_page(tx, state.cursor, page_limit, state.catch_up_until)?
    } else {
        Vec::new()
    };
    let drained_catch_up =
        can_read_page && state.catch_up_until.is_some() && page.len() < page_limit;
    rows.extend(
        page.into_iter()
            .map(|(observation_id, operation_id, raw)| GoalSourceRow {
                observation_id,
                operation_id,
                raw,
                from_pending_source_gap: false,
                verified_fact: None,
            }),
    );
    let mut stopped_for_source_gap_capacity = false;
    for GoalSourceRow {
        observation_id,
        operation_id,
        raw,
        from_pending_source_gap,
        verified_fact,
    } in rows
    {
        if !from_pending_source_gap {
            processed += 1;
        }
        if !from_pending_source_gap && observation_id <= state.cursor {
            return Err(Error::new(
                "AUTOMATION_OBSERVATION_ORDER_INVALID",
                "Goal progression observation page returned an ID at or before its cursor",
            ));
        }
        if !from_pending_source_gap && observation_id > target {
            state.cursor = target;
            break;
        }
        let fact_disposition = match verified_fact {
            Some(fact) => SubjectDisposition::Applied(fact),
            None => verify_terminal_isolated(tx, observation_id, &operation_id, &raw)?,
        };
        let fact = match fact_disposition {
            SubjectDisposition::Applied(fact) => fact,
            SubjectDisposition::Pending { code, reason } => {
                let error = Error::new(code.clone(), reason.clone());
                if !retain_source_gap(
                    &mut state,
                    observation_id,
                    &operation_id,
                    &raw,
                    &error,
                    now_ms,
                )? {
                    stopped_for_source_gap_capacity = true;
                    break;
                }
                append_recent(
                    &mut state,
                    json!({"observation_id":observation_id,"source_operation_id":operation_id,"disposition":"pending","reason":reason,"code":code,"retryable":true}),
                );
                advance_cursor_for_source_row(&mut state, observation_id, from_pending_source_gap);
                continue;
            }
            SubjectDisposition::Quarantined { code, evidence } => {
                persist_goal_quarantine(tx, &code, evidence, now_ms)?;
                quarantined = quarantined.saturating_add(1);
                append_recent(
                    &mut state,
                    json!({"observation_id":observation_id,"source_operation_id":operation_id,"disposition":"quarantined","code":code,"retryable":false}),
                );
                advance_cursor_for_source_row(&mut state, observation_id, from_pending_source_gap);
                continue;
            }
            SubjectDisposition::Skipped { code, reason } => {
                return Err(Error::new(
                    "AUTOMATION_GOAL_SOURCE_CLASSIFIER_INVALID",
                    format!(
                        "Goal source classifier returned unsupported skipped code {code}: {reason}"
                    ),
                ));
            }
        };
        let target = match target_for(tx, entry, &fact, now_ms) {
            Ok(target) => target,
            Err(error) if error.code == "AUTOMATION_GOAL_SOURCE_GAP" => {
                if !retain_source_gap(
                    &mut state,
                    observation_id,
                    &operation_id,
                    &raw,
                    &error,
                    now_ms,
                )? {
                    stopped_for_source_gap_capacity = true;
                    break;
                }
                append_recent(
                    &mut state,
                    json!({"observation_id":observation_id,"source_operation_id":operation_id,"disposition":"held","reason":"goal_target_source_unverified","code":error.code,"retryable":true}),
                );
                advance_cursor_for_source_row(&mut state, observation_id, from_pending_source_gap);
                continue;
            }
            Err(error) if error.code == "AUTOMATION_GOAL_SOURCE_INVALID" => {
                persist_goal_quarantine(
                    tx,
                    &error.code,
                    goal_source_evidence(observation_id, &operation_id, Some(&raw)),
                    now_ms,
                )?;
                quarantined = quarantined.saturating_add(1);
                append_recent(
                    &mut state,
                    json!({"observation_id":observation_id,"source_operation_id":operation_id,"disposition":"quarantined","code":error.code,"retryable":false}),
                );
                advance_cursor_for_source_row(&mut state, observation_id, from_pending_source_gap);
                continue;
            }
            Err(error) => return Err(error),
        };
        let Some(target) = target else {
            append_recent(
                &mut state,
                json!({"observation_id":observation_id,"source_operation_id":operation_id,"disposition":"skipped","reason":"not_the_selected_goal_attempt"}),
            );
            advance_cursor_for_source_row(&mut state, observation_id, from_pending_source_gap);
            continue;
        };
        let slot_id = event_slot_id(&fact)?;
        let slot_key = format!("{SLOT_PREFIX}{slot_id}");
        if let Some(existing) =
            config::read_record(tx, &slot_key, "Goal progression terminal slot")?
        {
            let slot: TerminalSlot = serde_json::from_value(existing).map_err(|_| {
                Error::new(
                    "AUTOMATION_GOAL_SLOT_CORRUPT",
                    "Goal progression terminal receipt is invalid",
                )
            })?;
            if slot.schema_version != SLOT_SCHEMA
                || slot.event_slot_id != slot_id
                || slot.terminal_event != fact["terminal_event"]
                || slot.native_session_id != fact["native_session_id"]
                || slot.native_input_id != fact["native_input_id"]
            {
                return Err(Error::new(
                    "AUTOMATION_GOAL_SLOT_CORRUPT",
                    "Goal progression terminal receipt identity differs",
                ));
            }
            append_recent(
                &mut state,
                json!({"observation_id":observation_id,"source_operation_id":operation_id,"disposition":"duplicate_event","claimed_disposition":slot.disposition,"operation_id":slot.operation_id,"claimed_by_manager_id":slot.manager_id,"goal_id":slot.goal_id,"goal_revision":slot.goal_revision}),
            );
            advance_cursor_for_source_row(&mut state, observation_id, from_pending_source_gap);
            continue;
        }
        let prepared = match prepare(tx, entry, &fact, &target, &slot_id) {
            Ok(prepared) => prepared,
            Err(error) if error.code == "AUTOMATION_GOAL_SOURCE_GAP" => {
                if !retain_source_gap(
                    &mut state,
                    observation_id,
                    &operation_id,
                    &raw,
                    &error,
                    now_ms,
                )? {
                    stopped_for_source_gap_capacity = true;
                    break;
                }
                append_recent(
                    &mut state,
                    json!({"observation_id":observation_id,"source_operation_id":operation_id,"disposition":"held","reason":"goal_preparation_source_unverified","code":error.code,"retryable":true}),
                );
                advance_cursor_for_source_row(&mut state, observation_id, from_pending_source_gap);
                continue;
            }
            Err(error) if error.code == "GOAL_NATIVE_STATE_UNRESOLVED" => {
                if !retain_source_gap(
                    &mut state,
                    observation_id,
                    &operation_id,
                    &raw,
                    &error,
                    now_ms,
                )? {
                    stopped_for_source_gap_capacity = true;
                    break;
                }
                append_recent(
                    &mut state,
                    json!({"observation_id":observation_id,"source_operation_id":operation_id,"disposition":"pending","reason":error.message,"code":error.code,"retryable":true}),
                );
                advance_cursor_for_source_row(&mut state, observation_id, from_pending_source_gap);
                continue;
            }
            Err(error) if error.code == "AUTOMATION_GOAL_SOURCE_INVALID" => {
                persist_goal_quarantine(
                    tx,
                    &error.code,
                    goal_source_evidence(observation_id, &operation_id, Some(&raw)),
                    now_ms,
                )?;
                quarantined = quarantined.saturating_add(1);
                append_recent(
                    &mut state,
                    json!({"observation_id":observation_id,"source_operation_id":operation_id,"disposition":"quarantined","code":error.code,"retryable":false}),
                );
                advance_cursor_for_source_row(&mut state, observation_id, from_pending_source_gap);
                continue;
            }
            Err(error) if error.code == "GOAL_NOT_ACTIVE" => {
                append_recent(
                    &mut state,
                    json!({"observation_id":observation_id,"source_operation_id":operation_id,"disposition":"skipped","code":error.code,"reason":error.message}),
                );
                advance_cursor_for_source_row(&mut state, observation_id, from_pending_source_gap);
                continue;
            }
            Err(error) => {
                let disposition = match error.code.as_str() {
                    "GOAL_OWNER_CONFLICT"
                    | "GOAL_NATIVE_OWNERSHIP_CONFLICT"
                    | "GOAL_MANAGER_SCOPE_CONFLICT"
                    | "FORBIDDEN" => "ownership_conflict",
                    "GOAL_UNSUPPORTED_RUNTIME" => "unsupported_runtime",
                    _ => return Err(error),
                };
                let slot = TerminalSlot {
                    schema_version: SLOT_SCHEMA,
                    event_slot_id: slot_id.clone(),
                    terminal_event: fact["terminal_event"].clone(),
                    native_session_id: fact["native_session_id"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    native_input_id: fact["native_input_id"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    source_observation_id: observation_id,
                    source_operation_id: operation_id.clone(),
                    manager_id: entry.owner_manager_id.clone(),
                    automation_id: entry.automation_id.clone(),
                    automation_revision: entry.revision,
                    goal_id: target["goal_id"].as_str().unwrap_or_default().to_owned(),
                    goal_revision: target["revision"].as_i64().unwrap_or_default(),
                    goal_active: target["active"].as_bool().unwrap_or_default(),
                    completion_status: target["completion"]["status"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    goal_owner_manager_id: target["created_by_manager_id"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    goal_last_reviser_manager_id: target["updated_by_manager_id"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    objective_sha256: model::digest(
                        target["objective"].as_str().unwrap_or_default().as_bytes(),
                    ),
                    task_id: target["scope"]["task_id"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    task_revision: target["scope"]["task_revision"]
                        .as_i64()
                        .unwrap_or_default(),
                    attempt_id: target["scope"]["attempt_id"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    binding_id: fact["binding_id"].as_str().unwrap_or_default().to_owned(),
                    binding_generation: fact["binding_generation"].as_i64().unwrap_or_default(),
                    disposition: disposition.to_owned(),
                    operation_id: None,
                    recorded_at_ms: now_ms,
                };
                // An ownership/scope conflict belongs to this entry's durable
                // status, not the global event claim. A correctly authorized
                // entry may still admit this EventRef; successful admission
                // remains globally single-use.
                if disposition != "ownership_conflict" {
                    config::write_record(tx, &slot_key, &json!(slot))?;
                }
                append_recent(
                    &mut state,
                    json!({"observation_id":observation_id,"source_operation_id":operation_id,"goal_id":target["goal_id"],"goal_revision":target["revision"],"goal_owner_manager_id":target["created_by_manager_id"],"goal_last_reviser_manager_id":target["updated_by_manager_id"],"disposition":disposition,"code":error.code,"reason":error.message}),
                );
                advance_cursor_for_source_row(&mut state, observation_id, from_pending_source_gap);
                continue;
            }
        };
        let evidence = goal_source_evidence(observation_id, &operation_id, Some(&raw));
        let subject_outcome = automation_reconcile::with_subject_savepoint(
            tx,
            || match admit(tx, &prepared)? {
                outcome @ (AdmissionResult::Admitted { .. } | AdmissionResult::Reused { .. }) => {
                    Ok(SubjectDisposition::Applied(outcome))
                }
                AdmissionResult::Conflict { code, reason } => match code.as_str() {
                    "AUTOMATION_GOAL_SOURCE_GAP"
                    | "GOAL_NATIVE_STATE_UNRESOLVED"
                    | "BINDING_NOT_READY" => Ok(SubjectDisposition::Pending { code, reason }),
                    "AUTOMATION_GOAL_SOURCE_INVALID" => Ok(SubjectDisposition::Quarantined {
                        code,
                        evidence: evidence.clone(),
                    }),
                    "GOAL_NOT_ACTIVE"
                    | "AUTOMATION_ACTION_CHANGED"
                    | "GOAL_OWNER_CONFLICT"
                    | "GOAL_NATIVE_OWNERSHIP_CONFLICT"
                    | "GOAL_MANAGER_SCOPE_CONFLICT"
                    | "FORBIDDEN"
                    | "GOAL_REVISION_CONFLICT"
                    | "NATIVE_GOAL_CONFLICT" => Ok(SubjectDisposition::Skipped { code, reason }),
                    "REQUEST_ID_CONFLICT" => Err(Error::new(
                        "AUTOMATION_GOAL_ADMISSION_IDENTITY_CONFLICT",
                        "Goal continuation request identity collides with retained inputs",
                    )),
                    _ => Err(Error::new(
                        "AUTOMATION_GOAL_ADMISSION_CONFLICT_UNKNOWN",
                        format!("Goal admission returned an unclassified conflict code {code}"),
                    )),
                },
            },
            |error| classify_goal_admission_error(error, evidence.clone()),
        )?;
        let outcome = match subject_outcome {
            SubjectDisposition::Applied(outcome) => outcome,
            SubjectDisposition::Pending { code, reason } => {
                let error = Error::new(code.clone(), reason.clone());
                if !retain_source_gap(
                    &mut state,
                    observation_id,
                    &operation_id,
                    &raw,
                    &error,
                    now_ms,
                )? {
                    stopped_for_source_gap_capacity = true;
                    break;
                }
                append_recent(
                    &mut state,
                    json!({"observation_id":observation_id,"source_operation_id":operation_id,"disposition":"pending","code":code,"reason":reason,"retryable":true}),
                );
                advance_cursor_for_source_row(&mut state, observation_id, from_pending_source_gap);
                continue;
            }
            SubjectDisposition::Quarantined { code, evidence } => {
                persist_goal_quarantine(tx, &code, evidence, now_ms)?;
                quarantined = quarantined.saturating_add(1);
                append_recent(
                    &mut state,
                    json!({"observation_id":observation_id,"source_operation_id":operation_id,"disposition":"quarantined","code":code,"retryable":false}),
                );
                advance_cursor_for_source_row(&mut state, observation_id, from_pending_source_gap);
                continue;
            }
            SubjectDisposition::Skipped { code, reason }
                if matches!(
                    code.as_str(),
                    "GOAL_NOT_ACTIVE" | "AUTOMATION_ACTION_CHANGED"
                ) =>
            {
                append_recent(
                    &mut state,
                    json!({"observation_id":observation_id,"source_operation_id":operation_id,"disposition":"skipped","code":code,"reason":reason}),
                );
                advance_cursor_for_source_row(&mut state, observation_id, from_pending_source_gap);
                continue;
            }
            SubjectDisposition::Skipped { code, reason } => {
                AdmissionResult::Conflict { code, reason }
            }
        };
        let (disposition, admitted_operation_id) = match outcome {
            AdmissionResult::Admitted { operation_id } => ("admitted", Some(operation_id)),
            AdmissionResult::Reused { operation_id } => ("reused", Some(operation_id)),
            AdmissionResult::Conflict { code, reason } => {
                let disposition = match code.as_str() {
                    "FORBIDDEN"
                    | "GOAL_OWNER_CONFLICT"
                    | "GOAL_NATIVE_OWNERSHIP_CONFLICT"
                    | "GOAL_MANAGER_SCOPE_CONFLICT" => "ownership_conflict",
                    "GOAL_REVISION_CONFLICT" | "NATIVE_GOAL_CONFLICT" => "admission_conflict",
                    _ => {
                        return Err(Error::new(
                            "AUTOMATION_GOAL_ADMISSION_CONFLICT_UNKNOWN",
                            format!(
                                "Goal admission conflict code changed after classification: {code}"
                            ),
                        ));
                    }
                };
                let slot = TerminalSlot {
                    schema_version: SLOT_SCHEMA,
                    event_slot_id: slot_id.clone(),
                    terminal_event: fact["terminal_event"].clone(),
                    native_session_id: fact["native_session_id"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    native_input_id: fact["native_input_id"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    source_observation_id: observation_id,
                    source_operation_id: operation_id.clone(),
                    manager_id: entry.owner_manager_id.clone(),
                    automation_id: entry.automation_id.clone(),
                    automation_revision: entry.revision,
                    goal_id: target["goal_id"].as_str().unwrap_or_default().to_owned(),
                    goal_revision: target["revision"].as_i64().unwrap_or_default(),
                    goal_active: target["active"].as_bool().unwrap_or_default(),
                    completion_status: target["completion"]["status"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    goal_owner_manager_id: target["created_by_manager_id"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    goal_last_reviser_manager_id: target["updated_by_manager_id"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    objective_sha256: model::digest(
                        target["objective"].as_str().unwrap_or_default().as_bytes(),
                    ),
                    task_id: prepared.task_id.clone(),
                    task_revision: prepared.task_revision,
                    attempt_id: prepared.attempt_id.clone(),
                    binding_id: prepared.binding_id.clone(),
                    binding_generation: prepared.binding_generation,
                    disposition: disposition.to_owned(),
                    operation_id: None,
                    recorded_at_ms: now_ms,
                };
                if disposition != "ownership_conflict" {
                    config::write_record(tx, &slot_key, &json!(slot))?;
                }
                append_recent(
                    &mut state,
                    json!({"observation_id":observation_id,"source_operation_id":operation_id,"terminal_event":fact["terminal_event"],"goal_id":target["goal_id"],"goal_revision":target["revision"],"goal_owner_manager_id":target["created_by_manager_id"],"goal_last_reviser_manager_id":target["updated_by_manager_id"],"disposition":disposition,"code":code,"reason":reason}),
                );
                advance_cursor_for_source_row(&mut state, observation_id, from_pending_source_gap);
                continue;
            }
        };
        let slot = TerminalSlot {
            schema_version: SLOT_SCHEMA,
            event_slot_id: slot_id.clone(),
            terminal_event: fact["terminal_event"].clone(),
            native_session_id: fact["native_session_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            native_input_id: fact["native_input_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            source_observation_id: observation_id,
            source_operation_id: operation_id.clone(),
            manager_id: entry.owner_manager_id.clone(),
            automation_id: entry.automation_id.clone(),
            automation_revision: entry.revision,
            goal_id: target["goal_id"].as_str().unwrap_or_default().to_owned(),
            goal_revision: target["revision"].as_i64().unwrap_or_default(),
            goal_active: target["active"].as_bool().unwrap_or_default(),
            completion_status: target["completion"]["status"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            goal_owner_manager_id: target["created_by_manager_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            goal_last_reviser_manager_id: target["updated_by_manager_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            objective_sha256: model::digest(
                target["objective"].as_str().unwrap_or_default().as_bytes(),
            ),
            task_id: prepared.task_id.clone(),
            task_revision: prepared.task_revision,
            attempt_id: prepared.attempt_id.clone(),
            binding_id: prepared.binding_id.clone(),
            binding_generation: prepared.binding_generation,
            disposition: disposition.to_owned(),
            operation_id: admitted_operation_id.clone(),
            recorded_at_ms: now_ms,
        };
        config::write_record(tx, &slot_key, &json!(slot))?;
        append_recent(
            &mut state,
            json!({"observation_id":observation_id,"source_operation_id":operation_id,"terminal_event":fact["terminal_event"],"goal_id":target["goal_id"],"goal_revision":target["revision"],"goal_owner_manager_id":target["created_by_manager_id"],"goal_last_reviser_manager_id":target["updated_by_manager_id"],"disposition":disposition,"operation_id":admitted_operation_id,"semantic_slot_id":slot_id}),
        );
        advance_cursor_for_source_row(&mut state, observation_id, from_pending_source_gap);
    }
    if drained_catch_up
        && !stopped_for_source_gap_capacity
        && let Some(through) = state.catch_up_until.take()
    {
        state.cursor = state.cursor.max(through);
    }
    state.updated_at_ms = now_ms;
    save_state(tx, entry, &state)?;
    state_projection(tx, &state, processed, quarantined)
}

/// Shared pump for enabled Goal progression entries, with one global entry
/// cursor so a large project set does not starve later managers.
pub(crate) fn reconcile<F>(
    tx: &Transaction<'_>,
    entry_budget: usize,
    event_budget: usize,
    now_ms: i64,
    mut admit: F,
) -> Result<Value>
where
    F: FnMut(&Transaction<'_>, &GoalProgressionAdmission) -> Result<AdmissionResult>,
{
    let limit = entry_budget.clamp(1, MAX_ENTRY_PAGE);
    let prefix = ENTRY_PREFIX;
    let pattern = format!("{prefix}%");
    let cursor = config::read_record(tx, GLOBAL_CURSOR_KEY, "Goal progression global cursor")
        .map_err(|error| {
            if error.code == "AUTOMATION_RECORD_CORRUPT" {
                Error::new(
                    "AUTOMATION_GOAL_CURSOR_CORRUPT",
                    "global Goal progression cursor record is corrupt",
                )
            } else {
                error
            }
        })?
        .map(|value| {
            serde_json::from_value::<GlobalCursor>(value).map_err(|_| {
                Error::new(
                    "AUTOMATION_GOAL_CURSOR_CORRUPT",
                    "global Goal progression cursor is invalid",
                )
            })
        })
        .transpose()?;
    let after = cursor.map_or_else(|| prefix.to_owned(), |cursor| cursor.last_entry_key);
    let select = |after: &str, limit: usize| -> Result<Vec<(String, String)>> {
        let mut statement = tx.prepare(
            "SELECT key,value_json FROM meta WHERE key LIKE ?1 AND key>?2 ORDER BY key LIMIT ?3",
        )?;
        Ok(statement
            .query_map(params![pattern, after, limit as i64], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    };
    let mut rows = select(&after, limit)?;
    if rows.is_empty() && !after.eq(prefix) {
        rows = select(prefix, limit)?;
    }
    let mut entries = Vec::new();
    let mut malformed_entries = Vec::new();
    for (key, raw) in &rows {
        let entry = match automation_reconcile::parse_automation_entry(
            raw,
            "Goal progression automation entry",
        ) {
            Ok(entry) => entry,
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "AUTOMATION_RECORD_CORRUPT" | "AUTOMATION_RECORD_INVALID"
                ) =>
            {
                malformed_entries.push(MalformedAutomationEntry {
                    code: error.code,
                    evidence: automation_reconcile::automation_entry_evidence(key, raw),
                });
                continue;
            }
            Err(error) => return Err(error),
        };
        if config::entry_key(
            &entry.owner_manager_id,
            &entry.project_id,
            &entry.automation_id,
        )? != *key
        {
            malformed_entries.push(MalformedAutomationEntry {
                code: "AUTOMATION_RECORD_INVALID".to_owned(),
                evidence: automation_reconcile::automation_entry_evidence(key, raw),
            });
            continue;
        }
        if entry.goal_progression_ready() {
            entries.push(entry);
        }
    }
    for malformed in &malformed_entries {
        persist_malformed_entry(tx, malformed, now_ms)?;
    }
    let mut results = Vec::new();
    for entry in entries {
        results.push(reconcile_entry(
            tx,
            &entry,
            event_budget,
            now_ms,
            &mut admit,
        )?);
    }
    let total_processed = results
        .iter()
        .map(|result| result["processed"].as_u64().unwrap_or_default())
        .fold(0u64, u64::saturating_add);
    let total_quarantined = results
        .iter()
        .map(|result| result["quarantined"].as_u64().unwrap_or_default())
        .fold(malformed_entries.len() as u64, u64::saturating_add);
    let status = if total_quarantined > 0 {
        "degraded"
    } else if rows.is_empty() && total_processed == 0 {
        "idle"
    } else {
        "progressed"
    };
    let last_entry_key = rows.last().map(|(key, _)| key.as_str());
    if let Some((last_entry_key, _)) = rows.last() {
        config::write_record(
            tx,
            GLOBAL_CURSOR_KEY,
            &json!({"schema_version":1,"last_entry_key":last_entry_key}),
        )?;
    }
    let processed = usize::try_from(total_processed).unwrap_or(usize::MAX);
    Ok(json!({
        "entries":results,
        "processed":processed,
        "quarantined":total_quarantined,
        "status":status,
        "entry_budget":limit,
        "event_budget_per_entry":event_budget,
        "cursor":last_entry_key
    }))
}

pub(crate) fn state(db: &Connection, entry: &AutomationEntry) -> Result<Value> {
    if let Some(state) = load_state(db, entry)? {
        return Ok(serde_json::to_value(state)?);
    }
    Ok(
        json!({"status":if entry.steps.contains(&AutomationStep::GoalProgression) {"not_initialized"} else {"not_configured"},"automation_id":entry.automation_id}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn goal_result(objective: &str, status: &str, revision: i64) -> Value {
        let objective_digest = format!(
            "sha256:{}",
            model::digest(model::canonical(&json!(objective)).unwrap().as_bytes()),
        );
        json!({
            "outcome":"applied",
            "details":{"goal":{
                "present":true,
                "status":status,
                "revision":revision,
                "objective_digest":objective_digest
            }}
        })
    }

    fn insert_operation(
        db: &Connection,
        operation_id: &str,
        binding_id: &str,
        generation: i64,
        method: &str,
        state: &str,
        result: Option<Value>,
    ) {
        db.execute(
            "INSERT INTO operations(operation_id,binding_id,binding_generation,method,state,result_json) \
             VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                operation_id,
                binding_id,
                generation,
                method,
                state,
                result.map(|value| model::canonical(&value).unwrap()),
            ],
        )
        .unwrap();
    }

    #[test]
    fn continuation_uses_latest_exact_binding_goal_receipt_not_source_method_guess() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE operations(
                operation_id TEXT PRIMARY KEY,
                binding_id TEXT,
                binding_generation INTEGER,
                method TEXT NOT NULL,
                state TEXT NOT NULL,
                result_json TEXT
            );",
        )
        .unwrap();

        insert_operation(
            &db,
            "other-generation-goal",
            "binding-a",
            2,
            "agent.goal",
            "settled",
            Some(goal_result("shared objective", "active", 4)),
        );
        insert_operation(
            &db,
            "goal-revision-7",
            "binding-a",
            3,
            "agent.goal",
            "settled",
            Some(goal_result("shared objective", "active", 7)),
        );
        insert_operation(
            &db,
            "terminal-turn",
            "binding-a",
            3,
            "agent.send",
            "settled",
            None,
        );
        insert_operation(
            &db,
            "new-followup",
            "binding-a",
            3,
            "agent.goal",
            "queued",
            None,
        );

        let (predecessor, revision) = native_goal_predecessor(
            &db,
            "binding-a",
            3,
            Some("new-followup"),
            "shared objective",
        )
        .unwrap();

        assert_eq!(predecessor.as_deref(), Some("goal-revision-7"));
        assert_eq!(revision, 7);
    }
}
