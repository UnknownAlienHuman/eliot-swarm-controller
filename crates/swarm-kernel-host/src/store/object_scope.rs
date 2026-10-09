//! Shared object read grants for Operation, Task graph and Artifact readers.
//!
//! Single Store-side object-authorization boundary for the A2 slice
//! (#49 operation reads, #51 Task graph reads, #52 artifact reads), following the
//! R23/R25/R26 implementation handoffs:
//!
//! * every grant derives from a verified retained relation plus an independently
//!   authenticated principal, never from handler `result_json`, a content digest,
//!   or a method-membership list;
//! * identity is loaded from retained rows, never from request JSON;
//! * a projected shape is a ceiling: a Receipt never carries Diagnostic fields and
//!   an unprojected result stays absent instead of raw;
//! * a distinct `Role::Observer` without a retained relation receives nothing, while
//!   the same frontend profile backed by a verified local Operator credential keeps
//!   its global bounded diagnostic view;
//! * unknown method or unresolvable relation fails closed;
//! * GM designation damage removes the current-GM shortcut and yields no grant,
//!   while genuine Store failures keep propagating as errors.
//!
//! `crate::artifacts` stays a Principal-free integrity and file-IO layer; this module
//! decides disclosure and hands back one authorized record instead of reopening
//! identity in every caller.

use super::gm;
use crate::artifacts::ArtifactRecord;
use crate::error::{Error, Result};
use crate::model::{self, Principal, Role};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

/// Exact Task graph identity, loaded from a retained object.
///
/// `project_id`/`task_id` are always present so an unclaimed Task keeps a resolvable
/// identity; revision, Attempt and binding pair refine it only when the retained
/// object actually carries them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskGraphIdentity {
    pub(crate) project_id: String,
    pub(crate) task_id: String,
    pub(crate) task_revision: Option<i64>,
    pub(crate) attempt_id: Option<String>,
    pub(crate) binding_id: Option<String>,
    pub(crate) binding_generation: Option<i64>,
}

/// A retained evidence object together with the exact Operation that produced
/// its receipt. Public handlers use both values for the evidence grant; neither
/// is derived from caller parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskEvidenceIdentity {
    pub(crate) identity: TaskGraphIdentity,
    pub(crate) operation_id: String,
}

/// Projection ceilings for Task graph reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TaskReadLevel {
    /// Identity plus revision/state/phase only.
    Summary,
    /// Frozen spec/brief and permitted pointers.
    Detail,
    /// Exact submission/check/acceptance/family receipts, still bounded.
    Evidence,
}

/// Positive relations that can produce a Task graph grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskReadBasis {
    LocalOperator,
    CurrentTaskManager,
    CurrentGmProjectScope,
    CurrentParticipant,
    RetainedAssignedReviewer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TaskReadGrant {
    pub(crate) level: TaskReadLevel,
    pub(crate) basis: TaskReadBasis,
}

/// Projection ceilings for Operation reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum OperationReadLevel {
    Summary,
    Receipt,
    Diagnostic,
}

impl OperationReadLevel {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Summary => "summary",
            Self::Receipt => "receipt",
            Self::Diagnostic => "diagnostic",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OperationReadBasis {
    LocalOperator,
    ExactCaller,
    RetainedAttemptOwner,
    CurrentTaskManager,
    CurrentGmTaskScope,
    OnBehalfLink,
    DirectedOrRetainedRelation,
}

impl OperationReadBasis {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::LocalOperator => "local_operator",
            Self::ExactCaller => "exact_caller",
            Self::RetainedAttemptOwner => "retained_attempt_owner",
            Self::CurrentTaskManager => "current_task_manager",
            Self::CurrentGmTaskScope => "current_gm_task_scope",
            Self::OnBehalfLink => "validated_on_behalf_link",
            Self::DirectedOrRetainedRelation => "directed_or_retained_relation",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OperationReadGrant {
    pub(crate) level: OperationReadLevel,
    pub(crate) basis: OperationReadBasis,
}

/// Projection ceilings for artifact reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ArtifactReadLevel {
    Metadata,
    Bytes,
    Assemble,
}

impl ArtifactReadLevel {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::Bytes => "bytes",
            Self::Assemble => "assemble",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArtifactReadBasis {
    LocalOperator,
    CurrentTaskManager,
    CurrentGmTaskScope,
    CurrentParticipantCandidate,
    AssignedReviewerCandidate,
    ExactTaskEvidenceCaller,
    ExactCheckCaller,
    ExactResultOperationCaller,
    ValidatedOnBehalfTaskScope,
    AllSourcePages,
    ScriptScope,
}

impl ArtifactReadBasis {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::LocalOperator => "local_operator",
            Self::CurrentTaskManager => "current_task_manager",
            Self::CurrentGmTaskScope => "current_gm_task_scope",
            Self::CurrentParticipantCandidate => "current_participant_candidate",
            Self::AssignedReviewerCandidate => "assigned_reviewer_candidate",
            Self::ExactTaskEvidenceCaller => "exact_task_evidence_caller",
            Self::ExactCheckCaller => "exact_check_caller",
            Self::ExactResultOperationCaller => "exact_result_operation_caller",
            Self::ValidatedOnBehalfTaskScope => "validated_on_behalf_task_scope",
            Self::AllSourcePages => "all_source_pages",
            Self::ScriptScope => "script_scope",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ArtifactReadGrant {
    pub(crate) level: ArtifactReadLevel,
    pub(crate) basis: ArtifactReadBasis,
}

/// One authorized artifact plus the grant that authorized it, handed downstream
/// instead of reopening identity from an artifact id.
#[derive(Debug, Clone)]
pub(crate) struct AuthorizedArtifact {
    pub(crate) record: ArtifactRecord,
    pub(crate) grant: ArtifactReadGrant,
}

/// One Operation row loaded once and consumed by the grant resolver. Callers must
/// not re-query operations and must not accept a different row shape.
#[derive(Debug, Clone)]
pub(crate) struct OperationRow {
    pub(crate) operation_id: String,
    pub(crate) caller_id: String,
    pub(crate) method: String,
    pub(crate) state: String,
    pub(crate) task_id: Option<String>,
    pub(crate) attempt_id: Option<String>,
    pub(crate) binding_id: Option<String>,
    pub(crate) binding_generation: Option<i64>,
    pub(crate) original_request_json: String,
    pub(crate) effective_request_json: String,
    pub(crate) result_json: Option<String>,
}

/// One native page and the exact retained operation whose result it represents.
/// `semantic_identity` is the same closed identity used by the assembler, with
/// page-local byte ranges removed.
#[derive(Debug, Clone)]
pub(crate) struct NativeResultSource {
    pub(crate) producer_operation_id: String,
    pub(crate) target_operation_id: String,
    pub(crate) task_identity: Option<TaskGraphIdentity>,
}

/// Closed set of artifact domains. Kind selects one decoder; unknown or malformed
/// kinds are damage rather than a generic fallback.
#[derive(Debug, Clone)]
pub(crate) enum ArtifactDomainIdentity {
    Task {
        identity: TaskGraphIdentity,
        producer_operation_id: String,
        artifact_kind: String,
    },
    Script,
    /// Every retained source page for a native result. An assembled artifact
    /// inherits all of these exact page identities; its assembler is not owner.
    NativeResult(Vec<NativeResultSource>),
}

fn identity_damaged(message: &str) -> Error {
    Error::new("OBJECT_SCOPE_DAMAGED", message)
}

fn bounded_text(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)?
        .as_str()
        .filter(|text| !text.trim().is_empty() && text.len() <= 256)
        .map(str::to_owned)
}

fn retained_json(raw: &str, message: &str) -> Result<Value> {
    serde_json::from_str(raw).map_err(|_| identity_damaged(message))
}

// ---------------------------------------------------------------------------
// Task graph identity loaders
// ---------------------------------------------------------------------------

/// Identity for an exact retained Task. `None` when the Task is absent, so a
/// caller can answer NOT_FOUND instead of inventing an identity.
pub(crate) fn identity_for_task(
    db: &Connection,
    task_id: &str,
) -> Result<Option<TaskGraphIdentity>> {
    let row: Option<(String, i64)> = db
        .query_row(
            "SELECT project_id,revision FROM tasks WHERE task_id=?1",
            [task_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(row.map(|(project_id, task_revision)| TaskGraphIdentity {
        project_id,
        task_id: task_id.to_string(),
        task_revision: Some(task_revision),
        attempt_id: None,
        binding_id: None,
        binding_generation: None,
    }))
}

/// Identity for an exact retained Attempt, including its binding pair when set.
pub(crate) fn identity_for_attempt(
    db: &Connection,
    attempt_id: &str,
) -> Result<Option<TaskGraphIdentity>> {
    let row: Option<(
        String,
        String,
        i64,
        Option<String>,
        Option<String>,
        Option<i64>,
        String,
    )> = db
        .query_row(
            "SELECT t.project_id,a.task_id,a.task_revision,a.attempt_id,a.binding_id, \
                    a.binding_generation,a.task_snapshot_json
             FROM attempts AS a JOIN tasks AS t ON t.task_id=a.task_id
             WHERE a.attempt_id=?1",
            [attempt_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    let Some((
        project_id,
        task_id,
        task_revision,
        attempt_id,
        binding_id,
        binding_generation,
        snapshot_json,
    )) = row
    else {
        return Ok(None);
    };
    let snapshot = retained_json(
        &snapshot_json,
        "Attempt snapshot is not valid retained JSON",
    )?;
    if snapshot.get("revision").and_then(Value::as_i64) != Some(task_revision)
        || (binding_id.is_some() != binding_generation.is_some())
    {
        return Err(identity_damaged(
            "Attempt revision or binding identity differs from its frozen snapshot",
        ));
    }
    Ok(Some(TaskGraphIdentity {
        project_id,
        task_id,
        task_revision: Some(task_revision),
        attempt_id,
        binding_id,
        binding_generation,
    }))
}

/// The current unreleased Attempt of a retained Task, derived from the actual
/// `attempts` producer rows. There is no `tasks.current_attempt_id` column.
pub(crate) fn identity_for_current_task_attempt(
    db: &Connection,
    task_id: &str,
) -> Result<Option<String>> {
    let attempt_id: Option<String> = db
        .query_row(
            "SELECT attempt_id FROM attempts WHERE task_id=?1 AND released_at_ms IS NULL \
             ORDER BY created_at_ms DESC,attempt_id DESC LIMIT 1",
            [task_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(attempt_id)
}

/// Internal coherence check. Every identity is revalidated against retained rows so
/// a partially loaded identity cannot infer authority.
fn verify_identity_coherence(db: &Connection, identity: &TaskGraphIdentity) -> Result<()> {
    if let Some(attempt_id) = identity.attempt_id.as_deref() {
        let Some(attempt) = identity_for_attempt(db, attempt_id)? else {
            return Err(identity_damaged(
                "identity names an Attempt that is no longer retained",
            ));
        };
        if attempt.task_id != identity.task_id {
            return Err(identity_damaged("Attempt points at a different Task"));
        }
        if identity.project_id != attempt.project_id
            || identity.task_revision.is_none()
            || attempt.task_revision != identity.task_revision
        {
            return Err(identity_damaged(
                "Attempt identity differs from its frozen Task revision or project",
            ));
        }
        // A binding pair is all-or-nothing and must match the retained Attempt.
        if identity.binding_id.is_some() != attempt.binding_id.is_some()
            || identity.binding_generation.is_some() != attempt.binding_generation.is_some()
        {
            return Err(identity_damaged("identity binding pair is incomplete"));
        }
        if identity.binding_id != attempt.binding_id
            || identity.binding_generation != attempt.binding_generation
        {
            return Err(identity_damaged(
                "identity binding pair does not match the retained Attempt",
            ));
        }
    } else if identity.binding_id.is_some() || identity.binding_generation.is_some() {
        return Err(identity_damaged(
            "binding pair cannot exist without an Attempt in one identity",
        ));
    } else {
        let Some(task) = identity_for_task(db, &identity.task_id)? else {
            return Err(identity_damaged(
                "identity names a Task that is no longer retained",
            ));
        };
        if task.project_id != identity.project_id || task.task_revision != identity.task_revision {
            return Err(identity_damaged(
                "Task identity differs from the retained current Task revision",
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Task graph read resolver
// ---------------------------------------------------------------------------

/// True when the principal is the verified local Operator credential. Role
/// `Operator` alone is deliberately insufficient.
fn verified_local_operator(db: &Connection, principal: &Principal) -> Result<bool> {
    if principal.role != Role::Operator {
        return Ok(false);
    }
    match super::require_local_operator(db, &principal.client_id) {
        Ok(()) => Ok(true),
        Err(error) if error.code == "LOCAL_OPERATOR_MISMATCH" => Ok(false),
        Err(error) => Err(error),
    }
}

/// Current GM designation, or `None` while designation is damaged. Genuine Store
/// failures propagate instead of silently removing a grant.
fn current_gm_for_read(db: &Connection) -> Result<Option<gm::CurrentGm>> {
    Ok(gm::read_current(db)?)
}

fn known_scope_denial(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "FORBIDDEN"
            | "UNAUTHORIZED"
            | "NOT_FOUND"
            | "STALE_PARTICIPANT"
            | "PARTICIPANT_NOT_ASSIGNED"
            | "STALE_REVIEW_ASSIGNMENT"
    )
}

/// Current exact Manager/GM Task scope, validated by the existing Store policy.
/// This preserves current GM fleet scope while keeping a Manager role alone from
/// becoming authority. Errors other than explicit scope denial propagate.
fn current_manager_task_basis(
    db: &Connection,
    principal: &Principal,
    identity: &TaskGraphIdentity,
) -> Result<Option<TaskReadBasis>> {
    if principal.role != Role::Manager {
        return Ok(None);
    }
    let gm = current_gm_for_read(db)?;
    if !crate::automation::authorization::current_manager_id_has_task_scope(
        db,
        &principal.client_id,
        &identity.task_id,
        &identity.project_id,
    )? {
        return Ok(None);
    }
    Ok(Some(
        if gm.is_some_and(|current| current.client_id == principal.client_id) {
            TaskReadBasis::CurrentGmProjectScope
        } else {
            TaskReadBasis::CurrentTaskManager
        },
    ))
}

/// Current Participant scope over this exact Task graph. A review-only basis is
/// handled separately so it never acquires Task work authority.
fn current_participant_scope(
    db: &Connection,
    principal: &Principal,
    identity: &TaskGraphIdentity,
) -> Result<bool> {
    if principal.role != Role::Participant {
        return Ok(false);
    }
    let Some(registration) = super::meta(db, &format!("client:{}", principal.client_id))? else {
        return Ok(false);
    };
    if registration["participation_basis"]["kind"] == "sponsored_reviewer" {
        return Ok(false);
    }
    let scope = match super::coordination::current_scope(db, principal) {
        Ok(scope) => scope,
        Err(error) if known_scope_denial(&error) => return Ok(false),
        Err(error) => return Err(error),
    };
    if bounded_text(&scope, "task_id").as_deref() != Some(identity.task_id.as_str()) {
        return Ok(false);
    }
    if let Some(expected) = identity.task_revision
        && scope.get("task_revision").and_then(Value::as_i64) != Some(expected)
    {
        return Ok(false);
    }
    if let Some(attempt_id) = identity.attempt_id.as_deref()
        && bounded_text(&scope, "attempt_id").as_deref() != Some(attempt_id)
    {
        return Ok(false);
    }
    Ok(true)
}

/// Exact retained assigned-reviewer scope, checked by the review assignment
/// validators. Historical Attempt reads use only the validator that preserves an
/// assigned late-result slot; raw registration JSON is never itself a grant.
fn retained_assigned_reviewer_scope(
    db: &Connection,
    principal: &Principal,
    identity: &TaskGraphIdentity,
) -> Result<bool> {
    if principal.role != Role::Participant {
        return Ok(false);
    }
    let Some(registration) = super::meta(db, &format!("client:{}", principal.client_id))? else {
        return Ok(false);
    };
    if registration["role"] != "participant"
        || registration["disabled"] == true
        || registration["participation_basis"]["kind"] != "sponsored_reviewer"
    {
        return Ok(false);
    }
    let scope = registration["participation_basis"]["review_scope"].clone();
    if bounded_text(&scope, "task_id").as_deref() != Some(identity.task_id.as_str()) {
        return Ok(false);
    }
    if let Some(expected) = identity.task_revision
        && scope.get("task_revision").and_then(Value::as_i64) != Some(expected)
    {
        return Ok(false);
    }
    if let Some(attempt_id) = identity.attempt_id.as_deref()
        && bounded_text(&scope, "attempt_id").as_deref() != Some(attempt_id)
    {
        return Ok(false);
    }
    let Some(assignment_id) = bounded_text(&scope, "review_assignment_id") else {
        return Ok(false);
    };
    match super::coordination::require_review_scope(db, principal, &assignment_id, &scope) {
        Ok(()) => Ok(true),
        Err(error) if error.code == "STALE_PARTICIPANT" => {
            match super::coordination::require_historical_review_result_scope(
                db,
                principal,
                &assignment_id,
                &scope,
            ) {
                Ok(()) => Ok(true),
                Err(error) if known_scope_denial(&error) => Ok(false),
                Err(error) => Err(error),
            }
        }
        Err(error) if known_scope_denial(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Resolve one Task graph read. `Ok(None)` means no positive relation exists, so
/// the caller must answer NOT_FOUND or filter the row; it never means denied.
pub(crate) fn resolve_task_read(
    db: &Connection,
    principal: &Principal,
    identity: &TaskGraphIdentity,
    requested: TaskReadLevel,
) -> Result<Option<TaskReadGrant>> {
    verify_identity_coherence(db, identity)?;
    if verified_local_operator(db, principal)? {
        return Ok(Some(TaskReadGrant {
            level: TaskReadLevel::Evidence,
            basis: TaskReadBasis::LocalOperator,
        }));
    }
    if let Some(basis) = current_manager_task_basis(db, principal, identity)? {
        return Ok(Some(TaskReadGrant {
            level: requested.max(TaskReadLevel::Detail),
            basis,
        }));
    }
    if current_participant_scope(db, principal, identity)? {
        return Ok(Some(TaskReadGrant {
            level: requested.min(TaskReadLevel::Evidence),
            basis: TaskReadBasis::CurrentParticipant,
        }));
    }
    if retained_assigned_reviewer_scope(db, principal, identity)? {
        return Ok(Some(TaskReadGrant {
            level: requested.min(TaskReadLevel::Evidence),
            basis: TaskReadBasis::RetainedAssignedReviewer,
        }));
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// Operation read resolver
// ---------------------------------------------------------------------------

/// Load one Operation row once. Callers must not re-query operations.
pub(crate) fn load_operation(db: &Connection, operation_id: &str) -> Result<Option<OperationRow>> {
    let row: Option<(
        String,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        String,
        String,
        Option<String>,
    )> = db
        .query_row(
            "SELECT operation_id,caller_id,method,state,task_id,attempt_id, \
                    binding_id,binding_generation,original_request_json,effective_request_json,result_json
             FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                ))
            },
        )
        .optional()?;
    Ok(row.map(
        |(
            operation_id,
            caller_id,
            method,
            state,
            task_id,
            attempt_id,
            binding_id,
            binding_generation,
            original_request_json,
            effective_request_json,
            result_json,
        )| OperationRow {
            operation_id,
            caller_id,
            method,
            state,
            task_id,
            attempt_id,
            binding_id,
            binding_generation,
            original_request_json,
            effective_request_json,
            result_json,
        },
    ))
}

/// Positive Task relation for one Operation row, when retained columns form a
/// coherent Task/Attempt tuple. A bare Task id never infers Attempt authority and a
/// taskless Operation yields no relation at all.
fn operation_task_identity(
    db: &Connection,
    operation: &OperationRow,
) -> Result<Option<TaskGraphIdentity>> {
    match (
        operation.task_id.as_deref(),
        operation.attempt_id.as_deref(),
    ) {
        (None, None) => Ok(None),
        (Some(task_id), None) => {
            if operation.binding_id.is_some() || operation.binding_generation.is_some() {
                return Err(identity_damaged(
                    "Operation binding scope cannot be inferred without an Attempt",
                ));
            }
            identity_for_task(db, task_id)?
                .map(Some)
                .ok_or_else(|| identity_damaged("Operation Task is not retained"))
        }
        (None, Some(_)) => Err(identity_damaged("Operation Attempt scope has no Task id")),
        (Some(task_id), Some(attempt_id)) => {
            let Some(identity) = identity_for_attempt(db, attempt_id)? else {
                return Err(identity_damaged(
                    "Operation names an Attempt that is not retained",
                ));
            };
            if identity.task_id != task_id {
                return Err(identity_damaged(
                    "Operation Task differs from its exact retained Attempt",
                ));
            }
            if operation.binding_id.is_some() != operation.binding_generation.is_some()
                || (operation.binding_id.is_some()
                    && (operation.binding_id != identity.binding_id
                        || operation.binding_generation != identity.binding_generation))
            {
                return Err(identity_damaged(
                    "Operation binding pair differs from its exact retained Attempt",
                ));
            }
            Ok(Some(identity))
        }
    }
}

fn granted_receipt(basis: OperationReadBasis) -> OperationReadGrant {
    OperationReadGrant {
        level: OperationReadLevel::Receipt,
        basis,
    }
}

fn registered_manager(db: &Connection, principal: &Principal) -> Result<bool> {
    if principal.role != Role::Manager {
        return Ok(false);
    }
    let Some(registration) = super::meta(db, &format!("client:{}", principal.client_id))? else {
        return Ok(false);
    };
    Ok(registration["role"] == "manager" && registration["disabled"] != true)
}

fn exact_launch_attempt<'a>(
    operation: &OperationRow,
    identity: Option<&'a TaskGraphIdentity>,
) -> Result<Option<&'a TaskGraphIdentity>> {
    if operation.method != "swarm.launch" {
        return Ok(None);
    }
    let (Some(task_id), Some(attempt_id)) = (
        operation.task_id.as_deref(),
        operation.attempt_id.as_deref(),
    ) else {
        return Ok(None);
    };
    let Some(identity) = identity.filter(|identity| identity.attempt_id.is_some()) else {
        return Ok(None);
    };
    if identity.task_id != task_id
        || identity.attempt_id.as_deref() != Some(attempt_id)
        || identity.task_revision.is_none()
    {
        return Err(identity_damaged(
            "launch Operation does not name its exact frozen Task Attempt",
        ));
    }
    Ok(Some(identity))
}

/// A registered Manager who owns the exact frozen Attempt may read this
/// `swarm.launch` receipt, including historical Attempts. This is a Receipt-only
/// relation; it never exposes diagnostics by itself.
fn retained_launch_attempt_owner(
    db: &Connection,
    principal: &Principal,
    operation: &OperationRow,
    identity: Option<&TaskGraphIdentity>,
) -> Result<bool> {
    if operation.method != "swarm.launch" || !registered_manager(db, principal)? {
        return Ok(false);
    }
    let Some(identity) = exact_launch_attempt(operation, identity)? else {
        return Ok(false);
    };
    let attempt_id = identity
        .attempt_id
        .as_deref()
        .ok_or_else(|| identity_damaged("launch Attempt identity is missing"))?;
    // Retained ownership is intentionally independent of the Task's current
    // Attempt pointer.
    let attempt_owner: Option<String> = db
        .query_row(
            "SELECT owner_id FROM attempts WHERE attempt_id=?1 AND task_id=?2 AND task_revision=?3",
            params![attempt_id, identity.task_id, identity.task_revision],
            |row| row.get(0),
        )
        .optional()?;
    Ok(attempt_owner.as_deref() == Some(principal.client_id.as_str()))
}

fn current_gm_launch_scope(
    db: &Connection,
    principal: &Principal,
    operation: &OperationRow,
    identity: Option<&TaskGraphIdentity>,
) -> Result<bool> {
    if operation.method != "swarm.launch" || !registered_manager(db, principal)? {
        return Ok(false);
    }
    let Some(identity) = exact_launch_attempt(operation, identity)? else {
        return Ok(false);
    };
    let is_current_gm =
        current_gm_for_read(db)?.is_some_and(|current| current.client_id == principal.client_id);
    if !is_current_gm {
        return Ok(false);
    }
    Ok(
        resolve_task_read(db, principal, identity, TaskReadLevel::Evidence)?.is_some_and(|grant| {
            grant.basis == TaskReadBasis::CurrentGmProjectScope
                && grant.level >= TaskReadLevel::Evidence
        }),
    )
}

/// Manager diagnostics are operation-local decorators, not an Operation grant.
/// A Manager must own the exact frozen Attempt, or be the current GM within
/// exact Task/Attempt scope, and an operation-specific producer must be retained.
fn explicit_launch_diagnostic_relation(
    db: &Connection,
    principal: &Principal,
    operation: &OperationRow,
    identity: Option<&TaskGraphIdentity>,
) -> Result<bool> {
    if operation.method != "swarm.launch" || !registered_manager(db, principal)? {
        return Ok(false);
    }
    if !retained_launch_attempt_owner(db, principal, operation, identity)?
        && !current_gm_launch_scope(db, principal, operation, identity)?
    {
        return Ok(false);
    }

    let effective = retained_json(
        &operation.effective_request_json,
        "launch Operation effective request is not valid retained JSON",
    )?;
    let manifest = effective
        .get("launch_manifest")
        .filter(|manifest| manifest.is_object());
    let native_mcp = manifest.is_some_and(|manifest| {
        ["native_mcp_readback", "native_mcp_latest_failure"]
            .iter()
            .any(|field| manifest.get(*field).is_some_and(|value| !value.is_null()))
    });
    let issuance = manifest.is_some_and(|manifest| {
        manifest
            .get("participant_issuance_latest_failure")
            .is_some_and(|value| !value.is_null())
    });
    let workspace =
        super::meta(db, &format!("launcher:failure:{}", operation.operation_id))?.is_some();
    let native_mcp_tools =
        super::launcher_mcp_tools::diagnostic_for_operation(db, &operation.operation_id)?.is_some();
    Ok(native_mcp || issuance || workspace || native_mcp_tools)
}

/// Resolve the narrow permission for retained `swarm.launch` diagnostic
/// attachments. This never raises the Operation's generic projection above
/// Receipt; callers may use `true` only for bounded domain-specific decorators.
pub(crate) fn resolve_launch_diagnostic_read(
    db: &Connection,
    principal: &Principal,
    operation_id: &str,
) -> Result<bool> {
    if principal.role != Role::Manager {
        return Ok(false);
    }
    let Some(operation) = load_operation(db, operation_id)? else {
        return Ok(false);
    };
    if operation.method != "swarm.launch" {
        return Ok(false);
    }
    let identity = operation_task_identity(db, &operation)?;
    explicit_launch_diagnostic_relation(db, principal, &operation, identity.as_ref())
}

/// Resolve one Operation read. `Ok(None)` means no positive relation exists.
///
/// The exact caller receives a bounded Receipt, never a global Diagnostic and never
/// access to unrelated Task objects.
pub(crate) fn resolve_operation_read(
    db: &Connection,
    principal: &Principal,
    operation_id: &str,
) -> Result<Option<OperationReadGrant>> {
    let Some(operation) = load_operation(db, operation_id)? else {
        return Ok(None);
    };
    if verified_local_operator(db, principal)? {
        return Ok(Some(OperationReadGrant {
            level: OperationReadLevel::Diagnostic,
            basis: OperationReadBasis::LocalOperator,
        }));
    }
    // Exact callers retain their bounded receipt even if optional Task linkage is
    // absent or damaged. This relation never grants diagnostic attachments.
    if operation.caller_id == principal.client_id {
        return Ok(Some(granted_receipt(OperationReadBasis::ExactCaller)));
    }
    let identity = operation_task_identity(db, &operation)?;
    if retained_launch_attempt_owner(db, principal, &operation, identity.as_ref())? {
        return Ok(Some(granted_receipt(
            OperationReadBasis::RetainedAttemptOwner,
        )));
    }
    if let Some(identity) = identity
        && let Some(basis) = current_manager_task_basis(db, principal, &identity)?
    {
        return Ok(Some(granted_receipt(match basis {
            TaskReadBasis::CurrentGmProjectScope => OperationReadBasis::CurrentGmTaskScope,
            _ => OperationReadBasis::CurrentTaskManager,
        })));
    }
    // Validated on-behalf links keep their own exact identity checks. A generic
    // `is_manager` fallback is deliberately absent, and a damaged link is an error
    // rather than a public fallback.
    if crate::automation::authorization::any_on_behalf_operation_link(db, &operation.operation_id)?
        .is_some()
        && match crate::automation::authorization::on_behalf_visible_to(
            db,
            principal,
            &operation.operation_id,
        ) {
            Ok(visible) => visible,
            Err(error) if known_scope_denial(&error) => false,
            Err(error) => return Err(error),
        }
    {
        return Ok(Some(granted_receipt(OperationReadBasis::OnBehalfLink)));
    }
    // The legacy SQL helper has an `:operator` candidate flag based on role.
    // A failed local-Operator check must never reach that branch. Verified local
    // Operators already returned Diagnostic above; a non-local Operator may
    // still read its exact caller receipt, but gains no role-only relation.
    if principal.role != Role::Operator {
        match super::operation_relation_visible_to(db, principal, operation_id) {
            Ok(true) => {
                return Ok(Some(granted_receipt(
                    OperationReadBasis::DirectedOrRetainedRelation,
                )));
            }
            Ok(false) => {}
            Err(error) if known_scope_denial(&error) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// Task evidence identities
// ---------------------------------------------------------------------------

fn artifact_if_present(db: &Connection, artifact_id: &str) -> Result<Option<ArtifactRecord>> {
    match super::results::get(db, artifact_id) {
        Ok(record) => Ok(Some(record)),
        Err(error) if error.code == "NOT_FOUND" => Ok(None),
        Err(error) => Err(error),
    }
}

fn operation_result(operation: &OperationRow) -> Result<Value> {
    operation
        .result_json
        .as_deref()
        .map(|raw| retained_json(raw, "Operation result is not valid retained JSON"))
        .transpose()
        .map(|result| result.unwrap_or(Value::Null))
}

fn operation_effective(operation: &OperationRow) -> Result<Value> {
    retained_json(
        &operation.effective_request_json,
        "Operation effective request is not valid retained JSON",
    )
}

fn operation_original(operation: &OperationRow) -> Result<Value> {
    retained_json(
        &operation.original_request_json,
        "Operation original request is not valid retained JSON",
    )
}

fn attempt_identity_or_damaged(db: &Connection, attempt_id: &str) -> Result<TaskGraphIdentity> {
    identity_for_attempt(db, attempt_id)?
        .ok_or_else(|| identity_damaged("retained Attempt is absent"))
}

/// Load a submission's TaskGraph from its immutable document and exact producer.
/// Both committed and retained stale submission documents remain tied to their
/// own frozen Attempt; neither is compared with the Task's current revision.
fn submission_artifact_identity(
    db: &Connection,
    record: &ArtifactRecord,
) -> Result<TaskEvidenceIdentity> {
    if record.kind != "task_submission" {
        return Err(identity_damaged("artifact is not a Task submission"));
    }
    let operation_id = bounded_text(&record.metadata, "operation_id")
        .ok_or_else(|| identity_damaged("submission metadata has no producing Operation"))?;
    let operation = load_operation(db, &operation_id)?
        .ok_or_else(|| identity_damaged("submission Operation is not retained"))?;
    if operation.method != "task.submit" || operation.state != "settled" {
        return Err(identity_damaged(
            "submission artifact is not linked to a settled task.submit Operation",
        ));
    }
    let result = operation_result(&operation)?;
    let effective = operation_effective(&operation)?;
    let document = effective
        .get("submission_document")
        .cloned()
        .unwrap_or(Value::Null);
    let mut expected_metadata = document.clone();
    if let Some(fields) = expected_metadata.as_object_mut() {
        fields.remove("claims");
        fields.remove("summary");
    }
    let raw = model::canonical(&document)?;
    let artifact_id = format!("submission-{}", model::digest(operation_id.as_bytes()));
    let result_names_artifact = (result["outcome"] == "applied"
        && result["submission_ref"].as_str() == Some(record.artifact_id.as_str()))
        || (result["outcome"] == "stale_submission_scope"
            && result["submission_artifact_ref"].as_str() == Some(record.artifact_id.as_str()));
    if !result_names_artifact
        || record.artifact_id != artifact_id
        || expected_metadata != record.metadata
        || record.byte_length != raw.len() as u64
        || record.content_digest != model::digest(raw.as_bytes())
        || operation.task_id.as_deref() != document["task_id"].as_str()
        || operation.attempt_id.as_deref() != document["attempt_id"].as_str()
        || result["attempt_id"] != document["attempt_id"]
        || result["candidate_ref"] != document["candidate_ref"]
    {
        return Err(identity_damaged(
            "submission document, artifact and settled Operation disagree",
        ));
    }
    let attempt_id = model::text(&document, "attempt_id")?;
    let identity = attempt_identity_or_damaged(db, attempt_id)?;
    if identity.task_id != document["task_id"].as_str().unwrap_or_default()
        || identity.task_revision != document["task_revision"].as_i64()
    {
        return Err(identity_damaged(
            "submission does not match its exact frozen Attempt revision",
        ));
    }
    let candidate = super::results::get(db, model::text(&document, "candidate_ref")?)?;
    if candidate.content_digest != document["candidate_sha256"].as_str().unwrap_or_default()
        || candidate.byte_length
            != document["candidate_byte_length"]
                .as_u64()
                .unwrap_or(u64::MAX)
        || candidate.kind != document["candidate_kind"].as_str().unwrap_or_default()
    {
        return Err(identity_damaged(
            "submission candidate identity differs from its retained artifact",
        ));
    }
    Ok(TaskEvidenceIdentity {
        identity,
        operation_id,
    })
}

/// Public exact submission identity for the Store evidence wrapper.
pub(crate) fn identity_for_submission(
    db: &Connection,
    submission_ref: &str,
) -> Result<Option<TaskEvidenceIdentity>> {
    let Some(record) = artifact_if_present(db, submission_ref)? else {
        return Ok(None);
    };
    if record.kind != "task_submission" {
        return Ok(None);
    }
    submission_artifact_identity(db, &record).map(Some)
}

/// `source_snapshot` metadata intentionally has no Operation id. The producer
/// uses a deterministic artifact id and settles `source.capture` with
/// `candidate_ref`; resolve exactly one retained match and verify its Attempt,
/// expected revision, commit, tree and coverage.
fn source_snapshot_identity(
    db: &Connection,
    record: &ArtifactRecord,
) -> Result<TaskEvidenceIdentity> {
    let mut statement = db.prepare(
        "SELECT operation_id FROM operations WHERE method='source.capture' AND state='settled' \
         AND json_extract(result_json,'$.outcome')='applied' \
         AND json_extract(result_json,'$.candidate_ref')=?1 ORDER BY operation_id LIMIT 2",
    )?;
    let matches = statement
        .query_map([&record.artifact_id], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let [operation_id] = matches.as_slice() else {
        return Err(identity_damaged(
            "source snapshot does not have one exact settled source.capture producer",
        ));
    };
    let operation = load_operation(db, operation_id)?
        .ok_or_else(|| identity_damaged("source.capture Operation is not retained"))?;
    let result = operation_result(&operation)?;
    let effective = operation_effective(&operation)?;
    let capture = effective.get("capture").unwrap_or(&Value::Null);
    let task_id = bounded_text(&record.metadata, "task_id")
        .ok_or_else(|| identity_damaged("source snapshot has no Task identity"))?;
    let attempt_id = bounded_text(&record.metadata, "attempt_id")
        .ok_or_else(|| identity_damaged("source snapshot has no Attempt identity"))?;
    let revision = record.metadata["task_revision"]
        .as_i64()
        .ok_or_else(|| identity_damaged("source snapshot has no frozen revision"))?;
    let commit = bounded_text(&record.metadata, "commit")
        .ok_or_else(|| identity_damaged("source snapshot has no commit identity"))?;
    let tree = bounded_text(&record.metadata, "tree")
        .ok_or_else(|| identity_damaged("source snapshot has no tree identity"))?;
    let valid_object_id = |value: &str| {
        matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
    };
    if record.artifact_id != format!("source-{}", model::digest(operation_id.as_bytes()))
        || operation.task_id.as_deref() != Some(task_id.as_str())
        || operation.attempt_id.as_deref() != Some(attempt_id.as_str())
        || capture["attempt_id"] != attempt_id
        || capture["expected_revision"].as_i64() != Some(revision)
        || capture["commit"]
            .as_str()
            .is_none_or(|value| !value.eq_ignore_ascii_case(&commit))
        || identity_for_attempt(db, &attempt_id)?
            .as_ref()
            .is_none_or(|identity| {
                identity.task_id != task_id || identity.task_revision != Some(revision)
            })
        || !valid_object_id(&commit)
        || !valid_object_id(&tree)
        || record.metadata["coverage"] != "complete"
        || record.metadata["file_count"].as_u64().is_none()
        || result["candidate_ref"] != record.artifact_id
        || result["commit"] != record.metadata["commit"]
        || result["tree"] != record.metadata["tree"]
        || result["file_count"] != record.metadata["file_count"]
    {
        return Err(identity_damaged(
            "source snapshot differs from its exact retained source.capture result",
        ));
    }
    Ok(TaskEvidenceIdentity {
        identity: attempt_identity_or_damaged(db, &attempt_id)?,
        operation_id: operation.operation_id,
    })
}

fn acceptance_identity(db: &Connection, operation_id: &str) -> Result<TaskEvidenceIdentity> {
    let operation = load_operation(db, operation_id)?
        .ok_or_else(|| identity_damaged("acceptance Operation is not retained"))?;
    let result = operation_result(&operation)?;
    if operation.method != "task.accept"
        || operation.state != "settled"
        || result["outcome"] != "applied"
        || result["acceptance_operation_id"] != operation_id
        || result["task_id"].as_str() != operation.task_id.as_deref()
        || result["attempt_id"].as_str() != operation.attempt_id.as_deref()
    {
        return Err(identity_damaged(
            "acceptance is not the exact settled task.accept decision",
        ));
    }
    let attempt_id = model::text(&result, "attempt_id")?;
    let identity = attempt_identity_or_damaged(db, attempt_id)?;
    if identity.task_id != result["task_id"].as_str().unwrap_or_default()
        || identity.task_revision != result["task_revision"].as_i64()
    {
        return Err(identity_damaged(
            "acceptance decision differs from its frozen Attempt revision",
        ));
    }
    let submission = super::results::get(db, model::text(&result, "submission_ref")?)?;
    let submission_identity = submission_artifact_identity(db, &submission)?;
    if submission_identity.identity != identity
        || submission.metadata["candidate_ref"] != result["candidate_ref"]
    {
        return Err(identity_damaged(
            "acceptance does not retain its exact submission and candidate",
        ));
    }
    let current: Option<(Option<String>, Option<String>, Option<i64>, Option<String>)> = db
        .query_row(
            "SELECT accepted_operation_id,accepted_attempt_id,accepted_revision,accepted_candidate_ref \
             FROM tasks WHERE task_id=?1",
            [&identity.task_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    if let Some((Some(current_operation), attempt, revision, candidate)) = current
        && current_operation == operation_id
        && (attempt.as_deref() != Some(attempt_id)
            || revision != identity.task_revision
            || candidate.as_deref() != result["candidate_ref"].as_str())
    {
        return Err(identity_damaged(
            "current accepted Task pointers differ from their retained decision",
        ));
    }
    Ok(TaskEvidenceIdentity {
        identity,
        operation_id: operation.operation_id,
    })
}

/// Public exact acceptance identity for the Store evidence wrapper.
pub(crate) fn identity_for_acceptance(
    db: &Connection,
    acceptance_operation_id: &str,
) -> Result<Option<TaskEvidenceIdentity>> {
    let Some(operation) = load_operation(db, acceptance_operation_id)? else {
        return Ok(None);
    };
    if operation.method != "task.accept" {
        return Ok(None);
    }
    acceptance_identity(db, acceptance_operation_id).map(Some)
}

type CheckRow = (String, String, String, String, Option<String>);

fn check_row(db: &Connection, check_id: &str) -> Result<Option<CheckRow>> {
    Ok(db
        .query_row(
            "SELECT operation_id,attempt_id,candidate_ref,state,result_ref \
             FROM check_runs WHERE check_id=?1",
            [check_id],
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
        .optional()?)
}

fn check_run_identity(db: &Connection, check_id: &str) -> Result<Option<TaskEvidenceIdentity>> {
    let Some((operation_id, attempt_id, candidate_ref, _state, _result_ref)) =
        check_row(db, check_id)?
    else {
        return Ok(None);
    };
    let operation = load_operation(db, &operation_id)?
        .ok_or_else(|| identity_damaged("CheckRun Operation is not retained"))?;
    let original = operation_original(&operation)?;
    let effective = operation_effective(&operation)?;
    if operation.method != "check.run"
        || operation.attempt_id.as_deref() != Some(attempt_id.as_str())
        || original["attempt_id"] != attempt_id
        || original["candidate_ref"] != candidate_ref
        || effective["check_id"] != check_id
    {
        return Err(identity_damaged(
            "CheckRun differs from its exact retained check.run request",
        ));
    }
    let identity = attempt_identity_or_damaged(db, &attempt_id)?;
    if operation.task_id.as_deref() != Some(identity.task_id.as_str()) {
        return Err(identity_damaged(
            "CheckRun Operation names a different Task than its Attempt",
        ));
    }
    Ok(Some(TaskEvidenceIdentity {
        identity,
        operation_id,
    }))
}

/// Public CheckRun identity for the Store evidence wrapper.
pub(crate) fn identity_for_check(
    db: &Connection,
    check_id: &str,
) -> Result<Option<TaskEvidenceIdentity>> {
    check_run_identity(db, check_id)
}

/// Binding/generation is usable for family scope only when it names exactly one
/// retained Attempt. Ambiguous bindings never fall back to a generic Task.
pub(crate) fn identity_for_family(
    db: &Connection,
    binding_id: &str,
    generation: i64,
) -> Result<Option<TaskGraphIdentity>> {
    let mut statement = db.prepare(
        "SELECT attempt_id FROM attempts WHERE binding_id=?1 AND binding_generation=?2 \
         ORDER BY created_at_ms,attempt_id LIMIT 2",
    )?;
    let ids = statement
        .query_map(params![binding_id, generation], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    match ids.as_slice() {
        [] => Ok(None),
        [attempt_id] => {
            let identity = attempt_identity_or_damaged(db, attempt_id)?;
            if identity.binding_id.as_deref() != Some(binding_id)
                || identity.binding_generation != Some(generation)
            {
                return Err(identity_damaged(
                    "family binding differs from its exact retained Attempt",
                ));
            }
            Ok(Some(identity))
        }
        _ => Err(Error::new(
            "OBJECT_SCOPE_AMBIGUOUS",
            "binding generation maps to more than one retained Attempt",
        )),
    }
}

// ---------------------------------------------------------------------------
// Artifact domain identity decoders
// ---------------------------------------------------------------------------

/// Decode one artifact into a closed retained producer relation. Artifact
/// metadata helps select a decoder; it never supplies authority by itself.
pub(crate) fn artifact_domain_identity(
    db: &Connection,
    record: &ArtifactRecord,
) -> Result<ArtifactDomainIdentity> {
    match record.kind.as_str() {
        "task_submission" => {
            let evidence = submission_artifact_identity(db, record)?;
            Ok(ArtifactDomainIdentity::Task {
                identity: evidence.identity,
                producer_operation_id: evidence.operation_id,
                artifact_kind: record.kind.clone(),
            })
        }
        "source_snapshot" => {
            let evidence = source_snapshot_identity(db, record)?;
            Ok(ArtifactDomainIdentity::Task {
                identity: evidence.identity,
                producer_operation_id: evidence.operation_id,
                artifact_kind: record.kind.clone(),
            })
        }
        "check_result" | "check_output" => {
            let evidence = check_artifact_identity(db, record)?;
            Ok(ArtifactDomainIdentity::Task {
                identity: evidence.identity,
                producer_operation_id: evidence.operation_id,
                artifact_kind: record.kind.clone(),
            })
        }
        "native_result_page" => Ok(ArtifactDomainIdentity::NativeResult(vec![
            native_page_source(db, record)?,
        ])),
        "native_result" => Ok(ArtifactDomainIdentity::NativeResult(
            assembled_result_sources(db, record)?,
        )),
        "script_bundle" | "script_result" | "script_output" => {
            // Script identity and historical scope are owned by the existing
            // scripts::authorize_artifact_read validator.
            Ok(ArtifactDomainIdentity::Script)
        }
        other => Err(identity_damaged(&format!(
            "artifact kind {other} has no closed provenance decoder"
        ))),
    }
}

fn check_artifact_identity(
    db: &Connection,
    record: &ArtifactRecord,
) -> Result<TaskEvidenceIdentity> {
    let check_id = bounded_text(&record.metadata, "check_id")
        .ok_or_else(|| identity_damaged("CheckRun artifact has no check_id"))?;
    let evidence = check_run_identity(db, &check_id)?
        .ok_or_else(|| identity_damaged("CheckRun artifact has no retained CheckRun"))?;
    let (operation_id, _attempt_id, candidate_ref, state, result_ref) =
        check_row(db, &check_id)?.ok_or_else(|| identity_damaged("CheckRun disappeared"))?;
    let operation = load_operation(db, &operation_id)?
        .ok_or_else(|| identity_damaged("CheckRun producer Operation disappeared"))?;
    let result = operation_result(&operation)?;
    match record.kind.as_str() {
        "check_result" => {
            let retained_cleanup =
                retained_check_cleanup_result(db, &check_id, &operation_id, record)?;
            let current_matches = !(result_ref.as_deref() != Some(record.artifact_id.as_str())
                || result["result_ref"] != record.artifact_id
                || record.metadata["candidate_ref"] != candidate_ref
                || record.metadata["state"] != state
                || result["check_id"] != check_id
                || result["state"] != state);
            if !current_matches && !retained_cleanup {
                return Err(identity_damaged(
                    "check_result is not the exact retained CheckRun result artifact",
                ));
            }
        }
        "check_output" => {
            let stream = record.metadata["stream"].as_str();
            if !matches!(stream, Some("stdout" | "stderr"))
                || !result["output_refs"].as_array().is_some_and(|refs| {
                    refs.iter()
                        .filter(|item| *item == &record.artifact_id)
                        .count()
                        == 1
                })
                || record.artifact_id
                    != format!(
                        "checklog-{}",
                        model::digest(
                            format!("{}:{}", operation_id, stream.unwrap_or_default()).as_bytes()
                        )
                    )
            {
                return Err(identity_damaged(
                    "check_output is not an exact retained CheckRun output reference",
                ));
            }
        }
        _ => return Err(identity_damaged("unknown CheckRun artifact kind")),
    }
    Ok(evidence)
}

/// The Store records a validated worker Completion before exposing partial
/// capture. Its exact artifact identity survives the later release readback;
/// artifact metadata alone never establishes this historical relationship.
fn retained_check_cleanup_result(
    db: &Connection,
    check_id: &str,
    operation_id: &str,
    record: &ArtifactRecord,
) -> Result<bool> {
    let (pending, history, token): (Option<String>, Option<String>, Option<String>) = db.query_row(
        "SELECT json_extract(spec_json,'$.cleanup_pending'),json_extract(spec_json,'$.cleanup_history'),json_extract(spec_json,'$.token') FROM check_runs WHERE check_id=?1 AND operation_id=?2",
        params![check_id, operation_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    for raw in [pending, history].into_iter().flatten() {
        let completion: crate::checks::worker::Completion = serde_json::from_str(&raw)
            .map_err(|_| identity_damaged("retained CheckRun cleanup completion is malformed"))?;
        if completion.check_id != check_id
            || completion.operation_id != operation_id
            || Some(completion.token.as_str()) != token.as_deref()
            || completion.resource_released
            || completion.result.kind != "check_result"
            || completion.result.metadata["check_id"] != check_id
            || completion.result.metadata["state"] != completion.state
        {
            return Err(identity_damaged(
                "retained CheckRun cleanup identity differs",
            ));
        }
        if serde_json::to_value(&completion.result)? == serde_json::to_value(record)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn binding_generation(metadata: &Value) -> Result<i64> {
    let generation = metadata
        .get("generation")
        .and_then(Value::as_i64)
        .or_else(|| metadata.get("binding_generation").and_then(Value::as_i64))
        .filter(|value| *value > 0)
        .ok_or_else(|| identity_damaged("native result has no positive binding generation"))?;
    if metadata
        .get("generation")
        .and_then(Value::as_i64)
        .is_some_and(|other| other != generation)
        || metadata
            .get("binding_generation")
            .and_then(Value::as_i64)
            .is_some_and(|other| other != generation)
    {
        return Err(identity_damaged(
            "native result binding generation fields disagree",
        ));
    }
    Ok(generation)
}

fn candidate_target_id(metadata: &Value) -> Result<Option<String>> {
    let source = &metadata["source"];
    let mut candidates = Vec::new();
    for value in [
        metadata.get("target_operation_id"),
        metadata
            .get("selector")
            .and_then(|selector| selector.get("input_operation_id")),
        source.get("input_operation_id"),
        source
            .get("origin")
            .and_then(|origin| origin.get("target_operation_id")),
        metadata
            .get("normalized_result_origin")
            .and_then(|origin| origin.get("target_operation_id")),
    ]
    .into_iter()
    .flatten()
    {
        if value.is_null() {
            continue;
        }
        let id = value
            .as_str()
            .filter(|id| !id.trim().is_empty() && id.len() <= 256)
            .ok_or_else(|| identity_damaged("native result target Operation id is malformed"))?;
        candidates.push(id.to_owned());
    }
    if candidates.windows(2).any(|pair| pair[0] != pair[1]) {
        return Err(identity_damaged(
            "native result target Operation references disagree",
        ));
    }
    Ok(candidates.into_iter().next())
}

fn operation_fact(operation: &OperationRow) -> Result<Value> {
    Ok(json!({
        "operation_id":operation.operation_id,
        "method":operation.method,
        "state":operation.state,
        "task_id":operation.task_id,
        "attempt_id":operation.attempt_id,
        "binding_id":operation.binding_id,
        "binding_generation":operation.binding_generation,
        "result":operation_result(operation)?,
    }))
}

fn assembly_identity(record: &ArtifactRecord) -> Result<Value> {
    let metadata = &record.metadata;
    let source_value = metadata
        .get("source")
        .filter(|value| value.is_object())
        .cloned()
        .ok_or_else(|| identity_damaged("assembly source page has no source object"))?;
    let selector = metadata
        .get("selector")
        .filter(|value| value.is_object())
        .cloned()
        .ok_or_else(|| identity_damaged("assembly source page has no selector object"))?;
    let mut source = source_value;
    let normalized =
        source["schema_id"] == swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID;
    let special = matches!(
        source["kind"].as_str(),
        Some("command_output" | "claude_assistant_result")
    ) || normalized;
    if source["kind"] == "command_output" && selector["kind"] != "command_output" {
        return Err(identity_damaged(
            "Command output selector differs from its retained page provenance",
        ));
    }
    if source["kind"] == "claude_assistant_result" && selector["kind"] != "claude_assistant_result"
    {
        return Err(identity_damaged(
            "Claude result selector differs from its retained page provenance",
        ));
    }
    let source_map = source
        .as_object_mut()
        .ok_or_else(|| identity_damaged("assembly source identity is malformed"))?;
    source_map.remove("whole_digest_verified");
    if special {
        for field in [
            "result_operation_id",
            "result_input_sha256",
            "result_module_receipt",
        ] {
            source_map.remove(field);
        }
    } else if bounded_text(metadata, "native_scope_key").is_none()
        || bounded_text(metadata, "native_root_id").is_none()
    {
        return Err(identity_damaged(
            "native assembly page has no retained scope and root identity",
        ));
    }
    let mut identity = json!({
        "binding_id":metadata["binding_id"],
        "generation":binding_generation(metadata)?,
        "binding_generation":binding_generation(metadata)?,
        "selector":selector,
        "source":source,
        "media_type":metadata["media_type"],
        "total_bytes":metadata["total_bytes"],
    });
    if !special {
        identity["native_scope_key"] = metadata["native_scope_key"].clone();
        identity["native_root_id"] = metadata["native_root_id"].clone();
    }
    Ok(identity)
}

fn identity_comparison_key(identity: &Value) -> Value {
    let mut key = identity.clone();
    if key["source"]["schema_id"]
        == swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID
        && let Some(producer) = key
            .get_mut("source")
            .and_then(|source| source.get_mut("origin"))
            .and_then(|origin| origin.get_mut("producer"))
            .and_then(Value::as_object_mut)
    {
        producer.insert(
            "completion_condition".to_owned(),
            json!("native_input_admitted"),
        );
        producer.insert("execution_complete".to_owned(), json!(false));
        producer.insert("task_completion".to_owned(), json!("unknown"));
        producer.insert("disposition".to_owned(), json!("admitted"));
    }
    key
}

fn validate_native_page_producer(
    db: &Connection,
    record: &ArtifactRecord,
    producer: &OperationRow,
    binding_id: &str,
    generation: i64,
    target_id: &str,
    target: &OperationRow,
) -> Result<Option<TaskGraphIdentity>> {
    let result = operation_result(producer)?;
    let source = &record.metadata["source"];
    let selector = &record.metadata["selector"];
    match producer.method.as_str() {
        "task.dispatch" => {
            let refs = result["details"]["artifact_refs"]
                .as_array()
                .ok_or_else(|| {
                    identity_damaged("dispatch result has no artifact reference list")
                })?;
            if target_id != producer.operation_id
                || !refs
                    .iter()
                    .filter(|item| *item == &record.artifact_id)
                    .count()
                    .eq(&1)
                || !matches!(result["outcome"].as_str(), Some("applied" | "rejected"))
            {
                return Err(identity_damaged(
                    "native page is not named by its exact retained dispatch result",
                ));
            }
        }
        "agent.result" => {
            let original = operation_original(producer)?;
            if result["outcome"] != "applied"
                || result["details"]["artifact_ref"] != record.artifact_id
                || result["details"]["source"] != *source
                || original["selector"] != *selector
            {
                return Err(identity_damaged(
                    "native page is not the exact retained agent.result artifact",
                ));
            }
            if source["schema_id"]
                == swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID
            {
                let (_, origin) =
                    super::normalized_result::validate_source(db, &producer.operation_id, source)?;
                if record.metadata["normalized_result_origin"] != origin
                    || origin["target_operation_id"] != target_id
                {
                    return Err(identity_damaged(
                        "normalized page differs from its exact retained dispatch origin",
                    ));
                }
            } else if source["kind"] == "claude_assistant_result" {
                let producer_fact = operation_fact(producer)?;
                let target_fact = operation_fact(target)?;
                let origin = super::results::load_claude_result_origin(
                    db,
                    &producer.operation_id,
                    &producer_fact,
                    binding_id,
                    generation,
                    target_id,
                    &target_fact,
                )?;
                super::results::validate_claude_assistant_result_source(
                    &original,
                    source,
                    &record.metadata,
                    &origin["producer"],
                )?;
            } else if selector["kind"] == "command_status" {
                let snapshot = super::command_results::admitted_target_snapshot(
                    db,
                    &producer.operation_id,
                    binding_id,
                    generation,
                    target_id,
                )?;
                if record.metadata["target_operation_status"] != snapshot
                    || record.metadata["target_input_sha256"] != snapshot["input_sha256"]
                {
                    return Err(identity_damaged(
                        "Command status page differs from its sealed dispatch snapshot",
                    ));
                }
            } else if selector["kind"] == "command_output" {
                let output = model::text(selector, "native_output")?;
                let snapshot = super::command_results::admitted_output_snapshot(
                    db,
                    &producer.operation_id,
                    binding_id,
                    generation,
                    target_id,
                    output,
                )?;
                if record.metadata["target_command_output"] != snapshot
                    || record.metadata["target_input_sha256"] != snapshot["input_sha256"]
                {
                    return Err(identity_damaged(
                        "Command output page differs from its sealed dispatch snapshot",
                    ));
                }
            } else if selector["kind"] == "antigravity_status" {
                let binding = super::operations::get_binding(db, binding_id, generation)?;
                let session_id = model::text(selector, "session_id")?;
                let snapshot = super::results::antigravity_status_snapshot(
                    db, binding_id, generation, &binding, target_id, session_id,
                )?;
                if record.metadata["target_operation_status"] != snapshot {
                    return Err(identity_damaged(
                        "Antigravity status page differs from its exact target receipt",
                    ));
                }
            } else if selector["kind"] == "input_status" {
                let target_raw: String = db.query_row(
                    "SELECT original_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3",
                    params![target_id, binding_id, generation],
                    |row| row.get(0),
                )?;
                let target_request = retained_json(
                    &target_raw,
                    "native input status target request is not valid retained JSON",
                )?;
                let target_digest = model::digest(model::canonical(&target_request)?.as_bytes());
                if record.metadata["target_input_sha256"] != target_digest {
                    return Err(identity_damaged(
                        "native input status page differs from its exact target request",
                    ));
                }
            }
        }
        _ => {
            return Err(identity_damaged(
                "native page producer method is outside the retained result vocabulary",
            ));
        }
    }
    if producer.binding_id.as_deref() != Some(binding_id)
        || producer.binding_generation != Some(generation)
        || target.binding_id.as_deref() != Some(binding_id)
        || target.binding_generation != Some(generation)
    {
        return Err(identity_damaged(
            "native page producer and target differ from its retained binding generation",
        ));
    }
    let target_identity = operation_task_identity(db, target)?;
    if target.method == "task.dispatch"
        && target_identity
            .as_ref()
            .is_none_or(|identity| identity.attempt_id.is_none())
    {
        return Err(identity_damaged(
            "task.dispatch result has no exact retained frozen Attempt",
        ));
    }
    let producer_identity = operation_task_identity(db, producer)?;
    if producer_identity.as_ref().is_some_and(|identity| {
        target_identity.as_ref().is_none_or(|target| {
            identity.task_id != target.task_id
                || identity.attempt_id != target.attempt_id
                || identity.task_revision != target.task_revision
        })
    }) {
        return Err(identity_damaged(
            "result Operation Task scope differs from its exact target dispatch",
        ));
    }
    Ok(target_identity)
}

fn native_page_source(db: &Connection, record: &ArtifactRecord) -> Result<NativeResultSource> {
    let metadata = &record.metadata;
    let producer_id = bounded_text(metadata, "operation_id")
        .ok_or_else(|| identity_damaged("native result page has no producer Operation"))?;
    let binding_id = bounded_text(metadata, "binding_id")
        .ok_or_else(|| identity_damaged("native result page has no binding identity"))?;
    let generation = binding_generation(metadata)?;
    let producer = load_operation(db, &producer_id)?
        .ok_or_else(|| identity_damaged("native result page producer Operation is absent"))?;
    if producer.state != "settled"
        || producer.binding_id.as_deref() != Some(binding_id.as_str())
        || producer.binding_generation != Some(generation)
        || metadata["page_sha256"] != record.content_digest
        || metadata["byte_length"].as_u64() != Some(record.byte_length)
        || metadata["total_bytes"].as_u64().is_none()
        || metadata["offset_bytes"].as_u64().is_none()
        || metadata["eof"].as_bool().is_none()
    {
        return Err(identity_damaged(
            "native result page range or producer identity is inconsistent",
        ));
    }
    let target_id = if producer.method == "task.dispatch" {
        producer.operation_id.clone()
    } else {
        candidate_target_id(metadata)?
            .ok_or_else(|| identity_damaged("agent.result page has no exact target dispatch"))?
    };
    let target = load_operation(db, &target_id)?
        .ok_or_else(|| identity_damaged("native result target Operation is absent"))?;
    if !matches!(target.method.as_str(), "task.dispatch" | "agent.send") {
        return Err(identity_damaged(
            "native result target is not an exact dispatch or send Operation",
        ));
    }
    let task_identity = validate_native_page_producer(
        db,
        record,
        &producer,
        &binding_id,
        generation,
        &target_id,
        &target,
    )?;
    Ok(NativeResultSource {
        producer_operation_id: producer_id,
        target_operation_id: target_id,
        task_identity,
    })
}

fn assembled_result_sources(
    db: &Connection,
    record: &ArtifactRecord,
) -> Result<Vec<NativeResultSource>> {
    let operation_id = bounded_text(&record.metadata, "assembly_operation_id")
        .ok_or_else(|| identity_damaged("assembled result has no exact assembly Operation"))?;
    let operation = load_operation(db, &operation_id)?
        .ok_or_else(|| identity_damaged("assembly Operation is not retained"))?;
    let result = operation_result(&operation)?;
    let original = operation_original(&operation)?;
    let refs = original["page_refs"]
        .as_array()
        .ok_or_else(|| identity_damaged("assembly Operation has no ordered page_refs"))?;
    let parts = record.metadata["parts"]
        .as_array()
        .ok_or_else(|| identity_damaged("assembled result has no retained part manifest"))?;
    let total = record.byte_length;
    if operation.method != "artifact.assemble"
        || operation.state != "settled"
        || result["outcome"] != "applied"
        || record.artifact_id != format!("assembled-{}", model::digest(operation_id.as_bytes()))
        || result["details"]["artifact_ref"] != record.artifact_id
        || result["details"]["byte_length"].as_u64() != Some(total)
        || result["details"]["sha256"] != record.content_digest
        || result["details"]["metadata"] != record.public_metadata()
        || record.metadata["coverage"] != "complete"
        || record.metadata["byte_length"].as_u64() != Some(total)
        || record.metadata["sha256"] != record.content_digest
        || record.metadata["part_count"].as_u64() != Some(parts.len() as u64)
        || refs.len() != parts.len()
        || refs.is_empty()
    {
        return Err(identity_damaged(
            "assembled result differs from its exact settled artifact.assemble Operation",
        ));
    }
    if original["expected_sha256"] != record.metadata["expected_sha256"] {
        return Err(identity_damaged(
            "assembly expected digest differs from its immutable request",
        ));
    }
    let mut sources = Vec::with_capacity(parts.len());
    let mut offset = 0u64;
    let mut expected_identity: Option<Value> = None;
    for (index, part) in parts.iter().enumerate() {
        let page_id = refs[index]
            .as_str()
            .ok_or_else(|| identity_damaged("assembly page reference is malformed"))?;
        if part["artifact_ref"] != page_id {
            return Err(identity_damaged(
                "assembly part order differs from its retained request",
            ));
        }
        let page = match super::results::get(db, page_id) {
            Ok(page) => page,
            Err(error) if error.code == "NOT_FOUND" => {
                return Err(identity_damaged("assembled source page is not retained"));
            }
            Err(error) => return Err(error),
        };
        if page.kind != "native_result_page" {
            return Err(identity_damaged(
                "assembly source is not a native result page",
            ));
        }
        let source = native_page_source(db, &page)?;
        let identity = assembly_identity(&page)?;
        let comparison = identity_comparison_key(&identity);
        if expected_identity
            .as_ref()
            .is_some_and(|expected| *expected != comparison)
        {
            return Err(identity_damaged(
                "assembled pages do not share one full semantic source identity",
            ));
        }
        if expected_identity.is_none() {
            expected_identity = Some(comparison);
            if record.metadata["identity"] != identity {
                return Err(identity_damaged(
                    "assembled identity differs from its first exact source page",
                ));
            }
        }
        let length = page.byte_length;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| identity_damaged("assembly page range overflows"))?;
        if part["offset_bytes"].as_u64() != Some(offset)
            || part["byte_length"].as_u64() != Some(length)
            || part["sha256"] != page.content_digest
            || page.metadata["offset_bytes"].as_u64() != Some(offset)
            || page.metadata["total_bytes"].as_u64() != record.metadata["byte_length"].as_u64()
            || page.metadata["eof"].as_bool() != Some(end == total)
            || (length == 0 && !(parts.len() == 1 && total == 0))
            || end > total
            || (end == total && index + 1 != parts.len())
        {
            return Err(identity_damaged(
                "assembled part coverage, order, digest or EOF metadata is inconsistent",
            ));
        }
        offset = end;
        sources.push(source);
    }
    if offset != total {
        return Err(identity_damaged(
            "assembled page manifest does not cover the retained result length",
        ));
    }
    Ok(sources)
}

// ---------------------------------------------------------------------------
// Artifact read resolver
// ---------------------------------------------------------------------------

fn evidence_artifact_basis(kind: &str) -> ArtifactReadBasis {
    match kind {
        "check_result" | "check_output" => ArtifactReadBasis::ExactCheckCaller,
        _ => ArtifactReadBasis::ExactTaskEvidenceCaller,
    }
}

fn artifact_level_for_relation(
    principal: &Principal,
    requested: ArtifactReadLevel,
    basis: ArtifactReadBasis,
) -> ArtifactReadLevel {
    match basis {
        ArtifactReadBasis::CurrentParticipantCandidate
        | ArtifactReadBasis::AssignedReviewerCandidate
        | ArtifactReadBasis::ExactTaskEvidenceCaller
        | ArtifactReadBasis::ExactCheckCaller
        | ArtifactReadBasis::ExactResultOperationCaller
        | ArtifactReadBasis::ScriptScope => requested.min(ArtifactReadLevel::Bytes),
        ArtifactReadBasis::ValidatedOnBehalfTaskScope
            if !matches!(principal.role, Role::Manager | Role::Operator) =>
        {
            requested.min(ArtifactReadLevel::Bytes)
        }
        _ => requested,
    }
}

fn basis_for_task_grant(basis: TaskReadBasis) -> ArtifactReadBasis {
    match basis {
        TaskReadBasis::LocalOperator => ArtifactReadBasis::LocalOperator,
        TaskReadBasis::CurrentTaskManager => ArtifactReadBasis::CurrentTaskManager,
        TaskReadBasis::CurrentGmProjectScope => ArtifactReadBasis::CurrentGmTaskScope,
        TaskReadBasis::CurrentParticipant => ArtifactReadBasis::CurrentParticipantCandidate,
        TaskReadBasis::RetainedAssignedReviewer => ArtifactReadBasis::AssignedReviewerCandidate,
    }
}

fn exact_operation_artifact_grant(
    db: &Connection,
    principal: &Principal,
    operation_id: &str,
    identity: Option<&TaskGraphIdentity>,
    exact_basis: ArtifactReadBasis,
    requested: ArtifactReadLevel,
) -> Result<Option<ArtifactReadGrant>> {
    let operation = load_operation(db, operation_id)?
        .ok_or_else(|| identity_damaged("artifact producer Operation is absent"))?;
    if principal.role != Role::Module && operation.caller_id == principal.client_id {
        return Ok(Some(ArtifactReadGrant {
            level: artifact_level_for_relation(principal, requested, exact_basis),
            basis: exact_basis,
        }));
    }
    if identity.is_some()
        && let Some(grant) = resolve_operation_read(db, principal, operation_id)?
        && grant.basis == OperationReadBasis::OnBehalfLink
    {
        let basis = ArtifactReadBasis::ValidatedOnBehalfTaskScope;
        return Ok(Some(ArtifactReadGrant {
            level: artifact_level_for_relation(principal, requested, basis),
            basis,
        }));
    }
    Ok(None)
}

fn task_artifact_grant(
    db: &Connection,
    principal: &Principal,
    identity: &TaskGraphIdentity,
    producer_operation_id: &str,
    artifact_kind: &str,
    requested: ArtifactReadLevel,
) -> Result<Option<ArtifactReadGrant>> {
    if let Some(grant) = resolve_task_read(db, principal, identity, TaskReadLevel::Evidence)? {
        let basis = basis_for_task_grant(grant.basis);
        return Ok(Some(ArtifactReadGrant {
            level: artifact_level_for_relation(principal, requested, basis),
            basis,
        }));
    }
    exact_operation_artifact_grant(
        db,
        principal,
        producer_operation_id,
        Some(identity),
        evidence_artifact_basis(artifact_kind),
        requested,
    )
}

fn native_source_grant(
    db: &Connection,
    principal: &Principal,
    source: &NativeResultSource,
    requested: ArtifactReadLevel,
) -> Result<Option<ArtifactReadGrant>> {
    if let Some(identity) = source.task_identity.as_ref()
        && let Some(grant) = resolve_task_read(db, principal, identity, TaskReadLevel::Evidence)?
    {
        let basis = basis_for_task_grant(grant.basis);
        return Ok(Some(ArtifactReadGrant {
            level: artifact_level_for_relation(principal, requested, basis),
            basis,
        }));
    }
    if let Some(grant) = exact_operation_artifact_grant(
        db,
        principal,
        &source.producer_operation_id,
        source.task_identity.as_ref(),
        ArtifactReadBasis::ExactResultOperationCaller,
        requested,
    )? {
        return Ok(Some(grant));
    }
    exact_operation_artifact_grant(
        db,
        principal,
        &source.target_operation_id,
        source.task_identity.as_ref(),
        ArtifactReadBasis::ExactResultOperationCaller,
        requested,
    )
}

/// Resolve one artifact read. `Ok(None)` means no positive relation exists.
/// Script artifact scope remains delegated to its existing retained validator.
pub(crate) fn resolve_artifact_read(
    db: &Connection,
    principal: &Principal,
    record: &ArtifactRecord,
    requested: ArtifactReadLevel,
) -> Result<Option<ArtifactReadGrant>> {
    if principal.role == Role::Module {
        return Ok(None);
    }
    match artifact_domain_identity(db, record)? {
        ArtifactDomainIdentity::Task {
            identity,
            producer_operation_id,
            artifact_kind,
        } => task_artifact_grant(
            db,
            principal,
            &identity,
            &producer_operation_id,
            &artifact_kind,
            requested,
        ),
        ArtifactDomainIdentity::Script => {
            match super::scripts::authorize_artifact_read(db, principal, record) {
                Ok(()) => Ok(Some(ArtifactReadGrant {
                    level: requested.min(ArtifactReadLevel::Bytes),
                    basis: ArtifactReadBasis::ScriptScope,
                })),
                Err(error) if known_scope_denial(&error) => Ok(None),
                Err(error) => Err(error),
            }
        }
        ArtifactDomainIdentity::NativeResult(sources) => {
            if sources.is_empty() {
                return Err(identity_damaged("native result has no exact source pages"));
            }
            if verified_local_operator(db, principal)? {
                return Ok(Some(ArtifactReadGrant {
                    level: requested,
                    basis: ArtifactReadBasis::LocalOperator,
                }));
            }
            let mut minimum = requested;
            let mut page_basis = None;
            for source in &sources {
                let Some(grant) = native_source_grant(db, principal, source, requested)? else {
                    return Ok(None);
                };
                minimum = minimum.min(grant.level);
                if page_basis.is_none() {
                    page_basis = Some(grant.basis);
                }
            }
            if minimum < requested {
                return Ok(None);
            }
            Ok(Some(ArtifactReadGrant {
                level: requested,
                basis: if record.kind == "native_result" {
                    ArtifactReadBasis::AllSourcePages
                } else {
                    page_basis.ok_or_else(|| {
                        identity_damaged("native result page has no resolved read basis")
                    })?
                },
            }))
        }
    }
}

/// Resolve an ordered page set for assembly. This validates source identity,
/// exact page references, contiguous byte coverage and each Bytes/Assemble grant;
/// the resulting aggregate never widens any page grant.
pub(crate) fn resolve_assembly_sources(
    db: &Connection,
    principal: &Principal,
    records: &[ArtifactRecord],
) -> Result<Option<ArtifactReadGrant>> {
    if records.is_empty() {
        return Ok(None);
    }
    let mut offset = 0u64;
    let mut total = None;
    let mut identity_key: Option<Value> = None;
    for record in records {
        if record.kind != "native_result_page" {
            return Err(identity_damaged("assembly source is not a result page"));
        }
        let Some(grant) =
            resolve_artifact_read(db, principal, record, ArtifactReadLevel::Assemble)?
        else {
            return Ok(None);
        };
        if grant.level < ArtifactReadLevel::Assemble {
            return Ok(None);
        }
        let page_identity = assembly_identity(record)?;
        let comparison = identity_comparison_key(&page_identity);
        if identity_key
            .as_ref()
            .is_some_and(|expected| *expected != comparison)
        {
            return Err(Error::new(
                "ARTIFACT_SCOPE",
                "assembly pages do not share one semantic source identity",
            ));
        }
        identity_key.get_or_insert(comparison);
        let page_total = record.metadata["total_bytes"]
            .as_u64()
            .ok_or_else(|| identity_damaged("assembly page has no total byte length"))?;
        if total.is_some_and(|expected| expected != page_total) {
            return Err(identity_damaged("assembly page total lengths disagree"));
        }
        total = Some(page_total);
        let end = offset
            .checked_add(record.byte_length)
            .ok_or_else(|| identity_damaged("assembly page coverage overflows"))?;
        if record.metadata["offset_bytes"].as_u64() != Some(offset)
            || record.metadata["byte_length"].as_u64() != Some(record.byte_length)
            || record.metadata["page_sha256"] != record.content_digest
            || record.metadata["eof"].as_bool() != Some(end == page_total)
            || (record.byte_length == 0 && !(records.len() == 1 && page_total == 0))
            || end > page_total
        {
            return Err(identity_damaged(
                "assembly source pages are reordered, overlapping or incomplete",
            ));
        }
        offset = end;
    }
    if total != Some(offset) {
        return Err(identity_damaged("assembly pages do not cover their source"));
    }
    Ok(Some(ArtifactReadGrant {
        level: ArtifactReadLevel::Assemble,
        basis: ArtifactReadBasis::AllSourcePages,
    }))
}

/// Denial helper so callers answer one unambiguous error instead of a raw `None`.
pub(crate) fn unauthorized_artifact() -> Error {
    Error::new(
        "FORBIDDEN",
        "artifact is outside every retained relation held by this principal",
    )
}
