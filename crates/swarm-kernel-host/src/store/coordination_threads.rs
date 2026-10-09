//! Store-owned current projections for addressed coordination Threads.
//!
//! Operations and their automatic Observations remain the durable history.
//! This module adds only compact `meta` projections and task-scoped indexes;
//! message bodies live once in the typed mailbox Operation result.

use super::{
    coordination as coordination_store, gm, mailbox, meta, operations, results, set_meta, tasks,
};
use crate::{
    artifacts::MAX_PAGE_BYTES,
    coordination::{self as coordination_keys, thread as wire},
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use swarm_contracts::coordination_limits as limits;

const THREAD_PREFIX: &str = "coordination:thread:";
const THREAD_TASK_INDEX_PREFIX: &str = "coordination:thread-task-index:";
const THREAD_ACTOR_INDEX_PREFIX: &str = "coordination:thread-actor-index:";
const THREAD_ACTIVE_INDEX_PREFIX: &str = "coordination:thread-active-index:";
const THREAD_FINGERPRINT_PREFIX: &str = "coordination:thread-fingerprint:";
const THREAD_STATES: &[&str] = &["open", "resolved", "unresolved", "withdrawn", "superseded"];
const ACTIVE_THREAD_REVIEW_THRESHOLD: i64 = 3;

#[derive(Clone)]
enum ThreadListIndex {
    Task,
    Actor(String),
}

#[derive(Debug, Clone)]
pub(crate) struct ThreadParticipant {
    pub client_id: String,
    pub role: String,
    pub generation: Option<i64>,
    pub participation_basis: Value,
    pub registration_fingerprint: String,
    pub actor: Value,
    pub scope: Value,
}

#[derive(Debug, Clone)]
pub(crate) struct ThreadContext {
    pub thread_id: String,
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
    /// Attempt owner at Thread creation; immutable provenance only, never
    /// durable read or mutation authority after Task ownership changes.
    pub sponsor_owner_id: String,
    pub topic_kind: String,
    pub state: String,
    pub state_revision: i64,
    pub next_message_seq: i64,
    pub participants: Vec<ThreadParticipant>,
    /// Safe persisted header projection; it contains no credential material.
    pub projection: Value,
}

/// Store authorization for retained Thread reads. Exact roster reads survive
/// Task release while that exact registration remains enabled. Manager reads
/// use the current Task owner or current GM, independent of the Thread's old
/// pinned Attempt; creator/sponsor identity is provenance, not a lasting grant.
pub(crate) fn authorize_retained_thread_read(
    db: &Connection,
    principal: &Principal,
    thread_id: &str,
) -> Result<ThreadContext> {
    let context = load_context(db, thread_id)?;
    if retained_identity_matches(db, principal, &context)? {
        return Ok(context);
    }
    match principal.role {
        Role::Operator => {
            super::require_local_operator(db, &principal.client_id)?;
            Ok(context)
        }
        Role::Manager => {
            match manager_can_read_task_history(
                db,
                principal,
                &context.task_id,
                Some((&context.attempt_id, Some(context.task_revision))),
            ) {
                Ok(true) => Ok(context),
                Ok(false) => Err(Error::new(
                    "FORBIDDEN",
                    "Thread read requires an exact retained registration or current Task Manager/GM authority",
                )),
                Err(error) => Err(error),
            }
        }
        _ => Err(Error::new(
            "FORBIDDEN",
            "Thread read requires a retained exact registration or current Manager/Operator authority",
        )),
    }
}

/// New Thread-bound effects always require a current exact Task/Attempt grant.
/// A saved sponsor ID alone never survives as mutation authority.
pub(crate) fn authorize_current_thread_mutation(
    db: &Connection,
    principal: &Principal,
    thread_id: &str,
) -> Result<ThreadContext> {
    let context = load_context(db, thread_id)?;
    if context.state != "open" {
        return Err(Error::new("THREAD_CLOSED", "Thread is not open"));
    }
    current_scope_matches(db, principal, &context)?;
    require_current_registration(db, principal)?;
    match principal.role {
        Role::Participant => {
            let participant = require_thread_participant(&context, principal)?;
            validate_participant_current(db, participant, &context)?;
        }
        Role::Manager | Role::Operator => {}
        _ => {
            return Err(Error::new(
                "FORBIDDEN",
                "Thread mutation requires a current Participant, Manager/GM, or local Operator",
            ));
        }
    }
    Ok(context)
}

/// Resolve an already admitted Task-bound Thread Operation to its trusted
/// Task/Attempt scope before the mutation savepoint. The caller identity is
/// authenticated by the Store; a request-supplied Thread ID never grants
/// scope on its own. `None` means the request cannot receive a scoped error
/// receipt and should remain caller-owned.
pub(crate) fn admitted_thread_operation_scope(
    db: &Connection,
    principal: &Principal,
    method: &str,
    params: &Value,
) -> Result<Option<(String, i64, String)>> {
    if method == "coordination.thread.open" {
        let value = params
            .get("params")
            .filter(|value| value.is_object())
            .unwrap_or(params);
        let request = match wire::parse_mutation(method, value) {
            Ok(wire::ThreadMutation::Open(request)) => request,
            Ok(_) => return Ok(None),
            Err(error) if is_expected_scope_rejection(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        if require_manager_or_operator(principal).is_err() {
            return Ok(None);
        }
        let task = match tasks::get_task(db, &request.task_id) {
            Ok(task) => task,
            Err(error) if is_expected_scope_rejection(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        let Some(task_revision) = task["revision"].as_i64().filter(|revision| *revision > 0) else {
            return Ok(None);
        };
        match coordination_store::watch_scope(
            db,
            principal,
            Some(&request.task_id),
            Some(task_revision),
            Some(&request.attempt_id),
        ) {
            Ok(scope)
                if scope["task"]["task_id"] == request.task_id
                    && scope["task"]["revision"] == task_revision
                    && scope["attempt"]["attempt_id"] == request.attempt_id =>
            {
                if let Err(error) = require_current_registration(db, principal) {
                    if is_expected_scope_rejection(&error) {
                        return Ok(None);
                    }
                    return Err(error);
                }
                Ok(Some((request.task_id, task_revision, request.attempt_id)))
            }
            Ok(_) => Ok(None),
            Err(error) if is_expected_scope_rejection(&error) => Ok(None),
            Err(error) => Err(error),
        }
    } else if thread_operation_method(method) {
        let value = params
            .get("params")
            .filter(|value| value.is_object())
            .unwrap_or(params);
        let Some(thread_id) = value.get("thread_id").and_then(Value::as_str) else {
            return Ok(None);
        };
        match authorize_retained_thread_read(db, principal, thread_id) {
            Ok(context) => Ok(Some((
                context.task_id,
                context.task_revision,
                context.attempt_id,
            ))),
            Err(error) if is_expected_scope_rejection(&error) => Ok(None),
            Err(error) => Err(error),
        }
    } else {
        Ok(None)
    }
}

fn is_expected_scope_rejection(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "INVALID_PARAMS"
            | "FORBIDDEN"
            | "UNAUTHORIZED"
            | "STALE_REVISION"
            | "STALE_PARTICIPANT"
            | "NOT_FOUND"
            | "THREAD_CLOSED"
    )
}

/// Require exact immutable Thread roster membership. The current-scope helper
/// is intentionally separate so callers must opt into both checks for writes.
pub(crate) fn require_thread_participant<'a>(
    context: &'a ThreadContext,
    principal: &Principal,
) -> Result<&'a ThreadParticipant> {
    let role = role_name(&principal.role);
    context
        .participants
        .iter()
        .find(|participant| {
            participant.client_id == principal.client_id && participant.role == role
        })
        .ok_or_else(|| Error::new("FORBIDDEN", "caller is not an exact Thread participant"))
}

/// Validate a participant effect grant when a contract mutation needs the
/// caller to act as a roster member in addition to holding current scope.
/// Current Manager/Operator control authority is checked separately and is
/// never derived from immutable Thread membership.
pub(crate) fn validate_thread_participant_current(
    db: &Connection,
    context: &ThreadContext,
    principal: &Principal,
) -> Result<()> {
    let participant = require_thread_participant(context, principal)?;
    validate_participant_current(db, participant, context)
}

/// Retained Operation read gate for Thread-backed APIs. A caller can always
/// inspect its own failed admission receipt; another caller's Operation is
/// visible only through the exact retained Thread read scope.
pub(crate) fn authorize_operation_read(
    db: &Connection,
    principal: &Principal,
    operation_id: &str,
) -> Result<()> {
    struct ThreadOperationReadRow {
        caller_id: String,
        method: String,
        task_id: Option<String>,
        attempt_id: Option<String>,
        original_request_json: String,
        result_json: String,
    }
    let row: Option<ThreadOperationReadRow> = db
        .query_row(
            "SELECT caller_id,method,task_id,attempt_id,original_request_json,COALESCE(result_json,'null') \
             FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| {
                Ok(ThreadOperationReadRow {
                    caller_id: row.get(0)?,
                    method: row.get(1)?,
                    task_id: row.get(2)?,
                    attempt_id: row.get(3)?,
                    original_request_json: row.get(4)?,
                    result_json: row.get(5)?,
                })
            },
        )
        .optional()?;
    let Some(ThreadOperationReadRow {
        caller_id,
        method,
        task_id,
        attempt_id,
        original_request_json: request_raw,
        result_json: result_raw,
    }) = row
    else {
        return Err(Error::new("NOT_FOUND", format!("Operation {operation_id}")));
    };
    if !thread_operation_method(&method) {
        return Err(Error::new(
            "FORBIDDEN",
            "Operation is outside the Thread read surface",
        ));
    }
    if caller_id == principal.client_id {
        return Ok(());
    }
    let request: Value = serde_json::from_str(&request_raw)?;
    let result: Value = serde_json::from_str(&result_raw)?;
    let thread_id = result
        .get("thread_id")
        .and_then(Value::as_str)
        .or_else(|| {
            result
                .get("payload")
                .and_then(|value| value.get("thread_id"))
                .and_then(Value::as_str)
        })
        .or_else(|| request.get("thread_id").and_then(Value::as_str))
        .or_else(|| {
            request
                .get("params")
                .and_then(|value| value.get("thread_id"))
                .and_then(Value::as_str)
        })
        .ok_or_else(|| Error::new("FORBIDDEN", "Operation has no retained Thread scope"))?;
    let context = authorize_retained_thread_read(db, principal, thread_id)?;
    if task_id.as_deref() != Some(context.task_id.as_str())
        || attempt_id.as_deref() != Some(context.attempt_id.as_str())
    {
        return Err(Error::new(
            "FORBIDDEN",
            "Operation scope differs from its retained Thread",
        ));
    }
    Ok(())
}

pub(super) fn read(
    db: &Connection,
    principal: &Principal,
    method: &str,
    value: &Value,
) -> Result<Value> {
    wire::validate_read(method, value)?;
    match method {
        "coordination.thread.get" => read_thread(db, principal, value),
        "coordination.thread.list" => list_threads(db, principal, value),
        _ => Err(Error::new("METHOD_NOT_FOUND", method)),
    }
}

pub(super) fn apply(
    tx: &Transaction<'_>,
    principal: &Principal,
    method: &str,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    match wire::parse_mutation(method, value)? {
        wire::ThreadMutation::Open(request) => {
            apply_open(tx, principal, request, method, operation_id, now)
        }
        wire::ThreadMutation::Send(request) => {
            apply_send(tx, principal, request, method, operation_id, now)
        }
        wire::ThreadMutation::Resolve(request) => {
            apply_resolve(tx, principal, request, method, operation_id, now)
        }
        wire::ThreadMutation::Withdraw(request) => {
            apply_withdraw(tx, principal, request, method, operation_id, now)
        }
        wire::ThreadMutation::Supersede(request) => {
            apply_supersede(tx, principal, request, method, operation_id, now)
        }
    }
}

pub(crate) fn apply_contract_decision(
    tx: &Transaction<'_>,
    principal: &Principal,
    method: &str,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    let request = crate::coordination::contract::parse_decision_request(method, value)?;
    if !matches!(principal.role, Role::Manager | Role::Operator) {
        return Err(Error::new(
            "FORBIDDEN",
            "contract decisions require the exact Attempt owner, current GM, or local Operator",
        ));
    }
    let context = authorize_current_thread_mutation(tx, principal, &request.thread_id)?;
    if context.topic_kind != "contract" {
        return Err(Error::new(
            "NOT_FOUND",
            "contract decision requires a contract Thread",
        ));
    }
    require_expected_revision(&context, request.expected_state_revision)?;
    if request.task_id != context.task_id
        || request.task_revision != context.task_revision
        || request.attempt_id != context.attempt_id
    {
        return Err(Error::new(
            "STALE_REVISION",
            "contract decision must name the exact Thread Task revision and Attempt",
        ));
    }

    let scope = coordination_store::watch_scope(
        tx,
        principal,
        Some(&context.task_id),
        Some(context.task_revision),
        Some(&context.attempt_id),
    )?;
    if scope["task"]["task_id"] != context.task_id
        || scope["task"]["revision"] != context.task_revision
        || scope["attempt"]["attempt_id"] != context.attempt_id
        || scope["task"]["state"] != "open"
        || !scope["attempt"]["released_at_ms"].is_null()
    {
        return Err(Error::new(
            "STALE_REVISION",
            "contract decision requires the current open Task and unreleased Attempt",
        ));
    }
    let attempt_owner_id = model::text(&scope["attempt"], "owner_id")?.to_owned();
    let (authority_basis, gm_epoch) = match principal.role {
        Role::Operator => ("local_operator", None),
        Role::Manager if attempt_owner_id == principal.client_id => ("attempt_owner", None),
        Role::Manager => (
            "current_gm",
            Some(gm::require_current_manager(tx, &principal.client_id)?),
        ),
        _ => unreachable!("decision role checked above"),
    };

    let proposal = coordination_store::load_current_contract_proposal_revision(
        tx,
        &context.thread_id,
        &context.task_id,
        context.task_revision,
        &context.attempt_id,
        &request.proposal_revision_id,
    )?;
    if proposal["proposal_id"] != request.proposal_id
        || proposal["proposal_digest"] != request.proposal_digest
    {
        return Err(Error::new(
            "STALE_CONTRACT_REVISION",
            "contract decision must name the exact current proposal identity and digest",
        ));
    }

    let current_scopes = super::code_scopes::affected_scope_revisions(
        tx,
        &context.task_id,
        context.task_revision,
        &context.attempt_id,
        &proposal["proposal"]["affected"],
    )?;
    if current_scopes["coverage"] != "complete" {
        return Err(Error::new(
            "SCOPE_COVERAGE_INCOMPLETE",
            "contract decision requires complete coverage of proposal-relevant current scopes",
        ));
    }
    let mut current_scope_refs = current_scopes["items"].as_array().cloned().ok_or_else(|| {
        Error::new(
            "SCOPE_COVERAGE_INCOMPLETE",
            "current affected-scope snapshot has no item list",
        )
    })?;
    current_scope_refs.sort_by(|left, right| {
        left["scope_intent_id"]
            .as_str()
            .cmp(&right["scope_intent_id"].as_str())
    });
    if current_scope_refs != request.affected_scope_revisions {
        return Err(Error::new(
            "STALE_SCOPE_REVISION",
            "contract decision scope references differ from the exact current affected-scope snapshot",
        ));
    }

    if coordination_store::load_contract_decision(
        tx,
        coordination_store::ContractDecisionIdentity {
            thread_id: &context.thread_id,
            task_id: &context.task_id,
            task_revision: context.task_revision,
            attempt_id: &context.attempt_id,
            proposal_id: &request.proposal_id,
            proposal_revision_id: &request.proposal_revision_id,
            proposal_digest: &request.proposal_digest,
        },
    )?
    .is_some()
    {
        return Err(Error::new(
            "CONTRACT_DECISION_ALREADY_RECORDED",
            "this immutable proposal revision already has a terminal contract decision",
        ));
    }

    let observation_payload = json!({
        "thread_id":context.thread_id,
        "proposal_id":request.proposal_id,
        "proposal_revision_id":request.proposal_revision_id,
        "decision_operation_id":operation_id,
    });
    let source_stream_id = format!("coordination:proposal:{}", request.proposal_id);
    let source_event_key = format!("decision:{}", request.proposal_revision_id);
    let observation_kind = request.kind.observation_kind();
    let observation_id = tx.query_row(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6) RETURNING observation_id",
        params![
            source_stream_id,
            source_event_key,
            operation_id,
            observation_kind,
            model::canonical(&observation_payload)?,
            now,
        ],
        |row| row.get::<_, i64>(0),
    )?;
    if observation_id <= 0 {
        return Err(Error::new(
            "STORE_INVARIANT",
            "contract decision Observation has no positive retained identity",
        ));
    }
    let decision = json!({
        "schema_version":1,
        "record_type":"contract_decision",
        "decision":request.kind.as_str(),
        "decision_operation_id":operation_id,
        "observation_id":observation_id,
        "thread_id":context.thread_id,
        "thread_state_revision":context.state_revision,
        "proposal_id":request.proposal_id,
        "proposal_revision_id":request.proposal_revision_id,
        "proposal_revision":proposal["revision"],
        "proposal_digest":request.proposal_digest,
        "task_id":context.task_id,
        "task_revision":context.task_revision,
        "attempt_id":context.attempt_id,
        "attempt_owner_id":attempt_owner_id,
        "actor":{"client_id":principal.client_id,"role":role_name(&principal.role)},
        "authority_basis":{"kind":authority_basis,"gm_epoch":gm_epoch},
        "affected_scope_revisions":current_scope_refs,
        "scope_coverage":"complete",
        "reason":request.reason,
        "conditions":request.conditions,
        "caveats":request.caveats,
        "created_at_ms":now,
        "model_work_started":false,
        "native_execution":false,
    });
    let decision_key = coordination_store::contract_decision_key(
        &request.proposal_id,
        &request.proposal_revision_id,
    );
    if meta(tx, &decision_key)?.is_some() {
        return Err(Error::new(
            "CONTRACT_DECISION_ALREADY_RECORDED",
            "this immutable proposal revision already has a terminal contract decision",
        ));
    }
    set_meta(tx, &decision_key, &decision)?;
    Ok(json!({
        "operation_id":operation_id,
        "decision":request.kind.as_str(),
        "thread_id":context.thread_id,
        "task_id":context.task_id,
        "task_revision":context.task_revision,
        "attempt_id":context.attempt_id,
        "proposal_id":request.proposal_id,
        "proposal_revision_id":request.proposal_revision_id,
        "proposal_digest":request.proposal_digest,
        "decision_observation_id":observation_id,
        "changed":true,
        "model_work_started":false,
        "native_execution":false,
    }))
}

fn apply_open(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: wire::OpenRequest,
    method: &str,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    require_manager_or_operator(principal)?;
    let task = tasks::get_task(tx, &request.task_id)?;
    let task_revision = task["revision"]
        .as_i64()
        .filter(|revision| *revision > 0)
        .ok_or_else(|| Error::new("TASK_DAMAGED", "Task revision is invalid"))?;
    let scope = coordination_store::watch_scope(
        tx,
        principal,
        Some(&request.task_id),
        Some(task_revision),
        Some(&request.attempt_id),
    )?;
    let attempt = tasks::get_attempt(tx, &request.attempt_id)?;
    if scope["task"]["task_id"] != request.task_id
        || scope["task"]["revision"] != task_revision
        || scope["attempt"]["attempt_id"] != request.attempt_id
        || attempt["task_id"] != request.task_id
        || attempt["task_revision"] != task_revision
    {
        return Err(Error::new(
            "STALE_REVISION",
            "Thread open requires the exact current Task revision and Attempt",
        ));
    }
    if let Some(assignment_id) = request.assignment_id.as_deref() {
        require_current_assignment(&attempt, assignment_id)?;
    }
    validate_artifact_ref(tx, principal, request.body_ref.as_deref())?;

    let sponsor_owner_id = model::text(&attempt, "owner_id")?.to_owned();
    let scope_id =
        coordination_keys::scope_id(&request.task_id, task_revision, &request.attempt_id)?;
    let fingerprint_basis = json!({
        "task_id":request.task_id,
        "task_revision":task_revision,
        "attempt_id":request.attempt_id,
        "topic_kind":request.topic_kind,
        "subject":request.subject,
        "participants":request.participants.iter().map(|participant| json!({
            "client_id":participant.client_id,
            "generation":participant.generation,
            "reason":participant.reason,
        })).collect::<Vec<_>>(),
        "reasonability":request.reasonability,
        "related_scopes":request.related_scopes,
        "body_ref":request.body_ref,
        "assignment_id":request.assignment_id,
    });
    let loop_fingerprint = format!(
        "sha256:{}",
        model::digest(model::canonical(&fingerprint_basis)?.as_bytes())
    );
    let duplicate_key = fingerprint_key(&scope_id, &loop_fingerprint);
    let mut duplicate_thread_id = None;
    if let Some(prior_id) =
        meta(tx, &duplicate_key)?.and_then(|value| value["thread_id"].as_str().map(str::to_owned))
    {
        match load_context(tx, &prior_id) {
            Ok(prior) if prior.state == "open" => {
                if request.supersedes_thread_id.as_deref() != Some(prior_id.as_str()) {
                    return Err(Error::new(
                        "THREAD_DUPLICATE",
                        format!("an active exact Thread already exists: {prior_id}"),
                    ));
                }
                duplicate_thread_id = Some(prior_id);
            }
            Ok(_) | Err(_) => {
                tx.execute("DELETE FROM meta WHERE key=?1", [&duplicate_key])?;
            }
        }
    }
    let supersedes = if let Some(prior_id) = request.supersedes_thread_id.as_deref() {
        let prior = load_context(tx, prior_id)?;
        if prior.state != "open"
            || prior.task_id != request.task_id
            || prior.task_revision != task_revision
            || prior.attempt_id != request.attempt_id
            || prior
                .projection
                .get("superseded_by_thread_id")
                .is_some_and(|item| !item.is_null())
        {
            return Err(Error::new(
                "STALE_REVISION",
                "successor Thread must link one open Thread in this exact Task/Attempt",
            ));
        }
        Some(prior_id.to_owned())
    } else {
        None
    };

    let creator_registration = require_current_registration(tx, principal)?;
    let creator_actor = model::message_actor(&creator_registration, &principal.client_id);
    let creator_scope = json!({
        "client_id":principal.client_id,
        "role":role_name(&principal.role),
        "task_id":request.task_id,
        "task_revision":task_revision,
        "attempt_id":request.attempt_id,
        "scope_id":scope_id,
    });
    let mut participants = Vec::with_capacity(request.participants.len());
    for spec in &request.participants {
        participants.push(resolve_participant(
            tx,
            &spec.client_id,
            spec.generation,
            &spec.reason,
            &request.task_id,
            task_revision,
            &request.attempt_id,
        )?);
    }

    let mut reasons = Vec::new();
    if participants.len() == 1 {
        reasons.push("single_participant".to_owned());
    }
    if participants.len() > 4 {
        reasons.push("participant_count_above_normal".to_owned());
    }
    if request.related_scopes.is_empty() {
        reasons.push("broad_or_pathless_topic".to_owned());
    }
    let active_count = active_thread_count(tx, &scope_id)?;
    if active_count + 1 >= ACTIVE_THREAD_REVIEW_THRESHOLD {
        reasons.push("multiple_open_threads_for_scope".to_owned());
    }

    let thread_id = format!("coord-{}", model::new_id());
    let header_participants: Vec<Value> = participants.iter().map(participant_projection).collect();
    let registration_fingerprint =
        registration_fingerprint(&principal.client_id, &creator_registration)?;
    let mut projection = json!({
        "schema_version":1,
        "thread_id":thread_id,
        "task_id":request.task_id,
        "task_revision":task_revision,
        "attempt_id":request.attempt_id,
        "sponsor_owner_id":sponsor_owner_id,
        "creator_actor":creator_actor,
        "creator_scope":creator_scope,
        "creator_registration_fingerprint":registration_fingerprint,
        "topic_kind":request.topic_kind,
        "subject":request.subject,
        "participants":header_participants,
        "state":"open",
        "state_revision":1,
        "next_message_seq":1,
        "last_message_id":Value::Null,
        "last_message_seq":Value::Null,
        "supersedes_thread_id":supersedes,
        "superseded_by_thread_id":Value::Null,
        "assignment_id":request.assignment_id,
        "reasonability":request.reasonability,
        "related_scopes":request.related_scopes,
        "body_ref":request.body_ref,
        "reasonability_result":{
            "classification":if reasons.is_empty() {"reasonable"} else {"accepted_with_warning"},
            "reasons":reasons,
            "duplicate_thread_id":duplicate_thread_id,
            "loop_fingerprint":loop_fingerprint,
        },
        "created_by":creator_actor,
        "created_at_ms":now,
        "updated_at_ms":now,
        "source_operation_id":operation_id,
    });
    if model::canonical(&projection)?.len() > MAX_PAGE_BYTES {
        return Err(Error::new(
            "PAYLOAD_TOO_LARGE",
            "Thread header exceeds the existing 64 KiB projection page limit",
        ));
    }
    stamp_operation_scope(
        tx,
        principal,
        method,
        operation_id,
        &request.client_request_id,
        &request.task_id,
        &request.attempt_id,
    )?;
    set_meta(tx, &thread_key(&thread_id), &projection)?;
    set_meta(
        tx,
        &task_index_key(&request.task_id, &request.attempt_id, &thread_id),
        &json!({"thread_id":thread_id,"task_id":request.task_id,"attempt_id":request.attempt_id}),
    )?;
    let mut indexed_actors = BTreeSet::new();
    indexed_actors.insert(principal.client_id.clone());
    indexed_actors.insert(sponsor_owner_id.clone());
    indexed_actors.extend(
        participants
            .iter()
            .filter_map(|participant| participant["client_id"].as_str().map(str::to_owned)),
    );
    for actor_id in indexed_actors {
        set_meta(
            tx,
            &actor_index_key(&actor_id, &request.task_id, &request.attempt_id, &thread_id),
            &json!({"thread_id":thread_id,"task_id":request.task_id,"attempt_id":request.attempt_id}),
        )?;
    }
    set_meta(
        tx,
        &active_index_key(&scope_id, &thread_id),
        &json!({"thread_id":thread_id}),
    )?;
    set_meta(
        tx,
        &duplicate_key,
        &json!({"thread_id":thread_id,"loop_fingerprint":loop_fingerprint}),
    )?;

    projection["operation_id"] = json!(operation_id);
    Ok(json!({
        "operation_id":operation_id,
        "thread_id":thread_id,
        "task_id":request.task_id,
        "task_revision":task_revision,
        "attempt_id":request.attempt_id,
        "state":"open",
        "revision":1,
        "state_revision":1,
        "reasonability":projection["reasonability_result"],
        "created_at_ms":now,
        "model_work_started":false,
        "source_operation_id":operation_id,
    }))
}

fn apply_send(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: wire::SendRequest,
    method: &str,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    let context = authorize_current_thread_mutation(tx, principal, &request.thread_id)?;
    let sender = require_thread_participant(&context, principal)?;
    validate_participant_current(tx, sender, &context)?;
    if context.state != "open" {
        return Err(Error::new("THREAD_CLOSED", "Thread is not open"));
    }
    let recipient = context
        .participants
        .iter()
        .find(|participant| participant.client_id == request.recipient)
        .ok_or_else(|| Error::new("FORBIDDEN", "recipient is not an exact Thread participant"))?;
    validate_participant_current(tx, recipient, &context)?;
    if let Some(revision_id) = request.proposal_revision_id.as_deref() {
        validate_proposal_revision(tx, &context, revision_id)?;
    }
    if recipient.client_id == sender.client_id {
        return Err(Error::invalid(
            "coordination messages require a distinct recipient",
        ));
    }

    validate_artifact_ref(tx, principal, request.body_ref.as_deref())?;
    for reference in &request.evidence_refs {
        validate_evidence_ref(tx, principal, &context, reference)?;
    }
    let sequence = context.next_message_seq;
    if sequence <= 0 || sequence == i64::MAX {
        return Err(Error::new(
            "THREAD_DAMAGED",
            "Thread message sequence is invalid",
        ));
    }
    let sender_registration = require_current_registration(tx, principal)?;
    let recipient_registration = meta(tx, &format!("client:{}", recipient.client_id))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "recipient registration is absent"))?;
    let message_id = format!("cmsg-{operation_id}");
    let (in_reply_to, reply_to) = resolve_reply(
        tx,
        &context,
        &sender.client_id,
        &recipient.client_id,
        request.reply_to_message_id.as_deref(),
        request.in_reply_to_digest.as_deref(),
    )?;
    let progress = message_progress(
        &context.thread_id,
        &sender.client_id,
        &recipient.client_id,
        &request,
    )?;
    let payload = json!({
        "contract":"eliot-coordination-message-v1",
        "thread_id":context.thread_id,
        "message_id":message_id,
        "message_seq":sequence,
        "sender_actor":model::message_actor(&sender_registration,&sender.client_id),
        "sender_scope":model::message_scope(&sender_registration,&sender.client_id),
        "recipient_actor":model::message_actor(&recipient_registration,&recipient.client_id),
        "recipient_scope":model::message_scope(&recipient_registration,&recipient.client_id),
        "speech_act":request.speech_act,
        "subject":request.subject,
        "summary":request.summary,
        "inline_body":request.inline_body,
        "body_ref":request.body_ref,
        "evidence_refs":request.evidence_refs,
        "proposal_revision_id":request.proposal_revision_id,
        "in_reply_to":in_reply_to,
    });
    let delivery = mailbox::admit_delivery(
        tx,
        operation_id,
        mailbox::DeliveryRequest {
            sender: principal,
            recipient_id: &recipient.client_id,
            payload_kind: "eliot-coordination-message-v1",
            payload_version: 1,
            payload,
            message_id: message_id.clone(),
            reply_to,
            admission_deadline_ms: Value::Null,
            delivery_deadline_ms: Value::Null,
            reply_deadline_ms: request.reply_deadline_ms,
        },
    )?;
    let mut result = delivery;
    result["thread_id"] = json!(context.thread_id);
    result["message_seq"] = json!(sequence);
    result["requires_reply"] = json!(request.requires_reply);
    result["thread_state_revision"] = json!(context.state_revision);
    result["progress"] = progress;
    result["model_work_started"] = json!(false);

    let mut projection = context.projection.clone();
    projection["next_message_seq"] = json!(sequence + 1);
    projection["last_message_id"] = json!(message_id);
    projection["last_message_seq"] = json!(sequence);
    projection["updated_at_ms"] = json!(now);
    projection["source_operation_id"] = json!(operation_id);
    stamp_operation_scope(
        tx,
        principal,
        method,
        operation_id,
        &request.client_request_id,
        &context.task_id,
        &context.attempt_id,
    )?;
    set_meta(tx, &thread_key(&context.thread_id), &projection)?;
    Ok(result)
}

fn apply_resolve(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: wire::ResolveRequest,
    method: &str,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    let context = authorize_current_thread_mutation(tx, principal, &request.thread_id)?;
    require_expected_revision(&context, request.expected_state_revision)?;
    match principal.role {
        Role::Manager | Role::Operator => require_manager_or_operator(principal)?,
        Role::Participant => {
            let participant = require_thread_participant(&context, principal)?;
            validate_participant_current(tx, participant, &context)?;
            if !matches!(request.outcome.as_str(), "unresolved" | "withdrawn")
                || !participant_has_own_question(tx, &context, &principal.client_id)?
            {
                return Err(Error::new(
                    "FORBIDDEN",
                    "a Participant may only close its own open question as unresolved or withdrawn",
                ));
            }
        }
        _ => return Err(Error::new("FORBIDDEN", "Thread closure authority required")),
    }
    if request.outcome == "resolved" && context.topic_kind == "contract" {
        let proposal_revision_id = request
            .selected_proposal_revision_id
            .as_deref()
            .ok_or_else(|| {
                Error::new(
                    "RATIFICATION_REQUIRED",
                    "contract Thread resolution requires the exact ratified proposal revision",
                )
            })?;
        let proposal = coordination_store::load_current_contract_proposal_revision(
            tx,
            &context.thread_id,
            &context.task_id,
            context.task_revision,
            &context.attempt_id,
            proposal_revision_id,
        )?;
        let ratification = request
            .manager_ratification_operation_id
            .as_deref()
            .ok_or_else(|| {
                Error::new(
                    "RATIFICATION_REQUIRED",
                    "contract Thread resolution requires an exact manager ratification Operation",
                )
            })?;
        validate_contract_ratification(
            tx,
            &context,
            ratification,
            model::text(&proposal, "proposal_id")?,
            proposal_revision_id,
            model::text(&proposal, "proposal_digest")?,
        )?;
    }
    if request.manager_ratification_operation_id.is_some()
        && (request.outcome != "resolved" || context.topic_kind != "contract")
    {
        return Err(Error::invalid(
            "manager_ratification_operation_id is only valid for a resolved contract Thread",
        ));
    }
    if let Some(proposal_revision_id) = request.selected_proposal_revision_id.as_deref() {
        validate_proposal_revision(tx, &context, proposal_revision_id)?;
    }
    for follow_up_operation_id in &request.follow_up_operation_ids {
        let follow_up = operations::get_operation(tx, follow_up_operation_id)?;
        if follow_up["task_id"] != context.task_id || follow_up["attempt_id"] != context.attempt_id
        {
            return Err(Error::new(
                "FORBIDDEN",
                "follow_up_operation_ids must name Operations in this exact Task/Attempt",
            ));
        }
    }
    stamp_operation_scope(
        tx,
        principal,
        method,
        operation_id,
        &request.client_request_id,
        &context.task_id,
        &context.attempt_id,
    )?;
    let state = request.outcome.clone();
    let resolution = json!({
        "outcome":state,
        "summary":request.resolution_summary,
        "selected_proposal_revision_id":request.selected_proposal_revision_id,
        "remaining_objections":request.remaining_objections,
        "follow_up_operation_ids":request.follow_up_operation_ids,
        "manager_ratification_operation_id":request.manager_ratification_operation_id,
        "recorded_by":principal.client_id,
        "operation_id":operation_id,
        "recorded_at_ms":now,
    });
    let updated = close_projection(tx, &context, &state, resolution, operation_id, now)?;
    Ok(json!({
        "operation_id":operation_id,
        "thread_id":context.thread_id,
        "state":state,
        "state_revision":updated["state_revision"],
        "resolution":updated["resolution"],
        "model_work_started":false,
        "source_operation_id":operation_id,
    }))
}

fn apply_withdraw(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: wire::ChangeRequest,
    method: &str,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    if principal.role != Role::Participant {
        return Err(Error::new(
            "FORBIDDEN",
            "thread.withdraw is limited to a Participant withdrawing its own question",
        ));
    }
    let context = authorize_current_thread_mutation(tx, principal, &request.thread_id)?;
    let participant = require_thread_participant(&context, principal)?;
    validate_participant_current(tx, participant, &context)?;
    require_expected_revision(&context, request.expected_state_revision)?;
    if !participant_has_own_question(tx, &context, &principal.client_id)? {
        return Err(Error::new(
            "FORBIDDEN",
            "Participant may withdraw only its own open question",
        ));
    }
    stamp_operation_scope(
        tx,
        principal,
        method,
        operation_id,
        &request.client_request_id,
        &context.task_id,
        &context.attempt_id,
    )?;
    let withdrawal = json!({
        "reason":request.reason,
        "recorded_by":principal.client_id,
        "operation_id":operation_id,
        "recorded_at_ms":now,
    });
    let updated = close_projection(tx, &context, "withdrawn", withdrawal, operation_id, now)?;
    Ok(json!({
        "operation_id":operation_id,
        "thread_id":context.thread_id,
        "state":"withdrawn",
        "state_revision":updated["state_revision"],
        "model_work_started":false,
        "source_operation_id":operation_id,
    }))
}

fn apply_supersede(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: wire::SupersedeRequest,
    method: &str,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    require_manager_or_operator(principal)?;
    let context = authorize_current_thread_mutation(tx, principal, &request.thread_id)?;
    require_expected_revision(&context, request.expected_state_revision)?;
    let successor = load_context(tx, &request.superseding_thread_id)?;
    if successor.state != "open"
        || successor.task_id != context.task_id
        || successor.task_revision != context.task_revision
        || successor.attempt_id != context.attempt_id
        || successor.projection["supersedes_thread_id"] != context.thread_id
    {
        return Err(Error::new(
            "STALE_REVISION",
            "superseding Thread must be an open linked successor in this exact Task/Attempt",
        ));
    }
    if context
        .projection
        .get("superseded_by_thread_id")
        .is_some_and(|value| !value.is_null())
    {
        return Err(Error::conflict("Thread already has a successor"));
    }
    stamp_operation_scope(
        tx,
        principal,
        method,
        operation_id,
        &request.client_request_id,
        &context.task_id,
        &context.attempt_id,
    )?;
    let mut updated = close_projection(
        tx,
        &context,
        "superseded",
        json!({
            "reason":request.reason,
            "superseding_thread_id":successor.thread_id,
            "recorded_by":principal.client_id,
            "operation_id":operation_id,
            "recorded_at_ms":now,
        }),
        operation_id,
        now,
    )?;
    updated["superseded_by_thread_id"] = json!(successor.thread_id);
    set_meta(tx, &thread_key(&context.thread_id), &updated)?;
    Ok(json!({
        "operation_id":operation_id,
        "thread_id":context.thread_id,
        "superseding_thread_id":successor.thread_id,
        "state":"superseded",
        "state_revision":updated["state_revision"],
        "model_work_started":false,
        "source_operation_id":operation_id,
    }))
}

fn close_projection(
    tx: &Transaction<'_>,
    context: &ThreadContext,
    state: &str,
    resolution: Value,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    if !THREAD_STATES.contains(&state) || state == "open" {
        return Err(Error::invalid("invalid Thread terminal state"));
    }
    let next_revision = context
        .state_revision
        .checked_add(1)
        .filter(|revision| *revision > 0)
        .ok_or_else(|| Error::new("THREAD_DAMAGED", "Thread revision overflow"))?;
    let mut projection = context.projection.clone();
    projection["state"] = json!(state);
    projection["state_revision"] = json!(next_revision);
    projection["resolution"] = resolution;
    projection["updated_at_ms"] = json!(now);
    projection["source_operation_id"] = json!(operation_id);
    set_meta(tx, &thread_key(&context.thread_id), &projection)?;
    let scope_id =
        coordination_keys::scope_id(&context.task_id, context.task_revision, &context.attempt_id)?;
    tx.execute(
        "DELETE FROM meta WHERE key=?1",
        [active_index_key(&scope_id, &context.thread_id)],
    )?;
    let fingerprint = context.projection["reasonability_result"]["loop_fingerprint"]
        .as_str()
        .unwrap_or_default();
    if !fingerprint.is_empty() {
        let key = fingerprint_key(&scope_id, fingerprint);
        let indexed = meta(tx, &key)?;
        if indexed
            .as_ref()
            .and_then(|value| value["thread_id"].as_str())
            == Some(context.thread_id.as_str())
        {
            let predecessor = match context.projection["supersedes_thread_id"].as_str() {
                Some(thread_id) => load_context_optional(tx, thread_id)?,
                None => None,
            };
            match predecessor {
                Some(prior)
                    if prior.state == "open"
                        && prior.projection["reasonability_result"]["loop_fingerprint"]
                            == fingerprint =>
                {
                    set_meta(
                        tx,
                        &key,
                        &json!({"thread_id":prior.thread_id,"loop_fingerprint":fingerprint}),
                    )?;
                }
                _ => {
                    tx.execute("DELETE FROM meta WHERE key=?1", [&key])?;
                }
            };
        }
    }
    Ok(projection)
}

fn read_thread(db: &Connection, principal: &Principal, value: &Value) -> Result<Value> {
    let thread_id = model::text(value, "thread_id")?;
    let context = authorize_retained_thread_read(db, principal, thread_id)?;
    let after = value
        .get("after_message_seq")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let limit = value
        .get("limit")
        .and_then(Value::as_i64)
        .unwrap_or(limits::DEFAULT_READ_PAGE_SIZE);
    let mut statement = db.prepare(
        "SELECT operation_id,result_json FROM operations \
         WHERE method='coordination.message.send' AND state='settled' AND task_id=?1 AND attempt_id=?2 \
           AND json_extract(result_json,'$.payload.thread_id')=?3 \
           AND CAST(json_extract(result_json,'$.payload.message_seq') AS INTEGER)>?4 \
         ORDER BY CAST(json_extract(result_json,'$.payload.message_seq') AS INTEGER),operation_id LIMIT ?5",
    )?;
    let rows: Vec<(String, String)> = statement
        .query_map(
            params![
                context.task_id,
                context.attempt_id,
                context.thread_id,
                after,
                limit + 1
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?
        .collect::<std::result::Result<_, _>>()?;
    let header = public_projection(&context.projection);
    let mut messages = Vec::new();
    let mut next_cursor = after;
    let mut has_more = rows.len() as i64 > limit;
    for (index, (operation_id, raw)) in rows.iter().enumerate() {
        if index as i64 >= limit {
            has_more = true;
            break;
        }
        let result: Value = serde_json::from_str(raw)?;
        let payload = result
            .get("payload")
            .filter(|item| item.is_object())
            .cloned()
            .ok_or_else(|| Error::new("THREAD_DAMAGED", "typed message Operation lacks payload"))?;
        let sequence = payload["message_seq"]
            .as_i64()
            .ok_or_else(|| Error::new("THREAD_DAMAGED", "typed message sequence is absent"))?;
        let cancellation = cancellation_for_delivery(db, result["delivery_id"].as_str())?;
        let message = json!({
            "operation_id":operation_id,
            "message_id":result["message_id"],
            "delivery_id":result["delivery_id"],
            "payload_digest":result["payload_digest"],
            "payload_kind":result["payload_kind"],
            "payload_version":result["payload_version"],
            "payload":payload,
            "requires_reply":result["requires_reply"],
            "admission_deadline_ms":result["admission_deadline_ms"],
            "delivery_deadline_ms":result["delivery_deadline_ms"],
            "reply_deadline_ms":result["reply_deadline_ms"],
            "reply_to":result["reply_to"],
            "cancellation":cancellation,
        });
        let mut candidate = messages.clone();
        candidate.push(message.clone());
        let page = json!({
            "thread":header,
            "messages":candidate,
            "after_message_seq":after,
            "next_cursor":sequence,
            "has_more":has_more || index + 1 < rows.len(),
        });
        if model::canonical(&page)?.len() > MAX_PAGE_BYTES {
            has_more = true;
            if messages.is_empty() {
                return Err(Error::new(
                    "PAYLOAD_TOO_LARGE",
                    "one typed message exceeds the existing Thread page byte limit",
                ));
            }
            break;
        }
        next_cursor = sequence;
        messages.push(message);
    }
    let page = json!({
        "thread":header,
        "messages":messages,
        "after_message_seq":after,
        "next_cursor":next_cursor,
        "has_more":has_more,
        "coverage":if has_more {"bounded_page"} else {"complete_for_page"},
    });
    if model::canonical(&page)?.len() > MAX_PAGE_BYTES {
        return Err(Error::new(
            "PAYLOAD_TOO_LARGE",
            "Thread read page exceeds 64 KiB",
        ));
    }
    Ok(page)
}

fn list_threads(db: &Connection, principal: &Principal, value: &Value) -> Result<Value> {
    let task_id = model::text(value, "task_id")?;
    let requested_attempt_filter = value.get("attempt_id").and_then(Value::as_str);
    let (index_kind, attempt_filter) =
        thread_list_index(db, principal, task_id, requested_attempt_filter)?;
    let state_filter = value.get("state").and_then(Value::as_str);
    let topic_filter = value.get("topic_kind").and_then(Value::as_str);
    let limit = value
        .get("limit")
        .and_then(Value::as_i64)
        .unwrap_or(limits::DEFAULT_READ_PAGE_SIZE);
    let after_thread_id = value.get("after_thread_id").and_then(Value::as_str);
    let prefix = match &index_kind {
        ThreadListIndex::Task => task_index_prefix(task_id, attempt_filter.as_deref()),
        ThreadListIndex::Actor(client_id) => {
            actor_index_prefix(client_id, task_id, attempt_filter.as_deref())
        }
    };
    let after_key = match after_thread_id {
        Some(id) => {
            let cursor = load_context(db, id)?;
            if cursor.task_id != task_id
                || attempt_filter
                    .as_deref()
                    .is_some_and(|attempt| cursor.attempt_id != attempt)
            {
                return Err(Error::invalid(
                    "after_thread_id is outside this Task/Attempt filter",
                ));
            }
            authorize_retained_thread_read(db, principal, id)?;
            Some(match &index_kind {
                ThreadListIndex::Task => {
                    task_index_key(&cursor.task_id, &cursor.attempt_id, &cursor.thread_id)
                }
                ThreadListIndex::Actor(client_id) => actor_index_key(
                    client_id,
                    &cursor.task_id,
                    &cursor.attempt_id,
                    &cursor.thread_id,
                ),
            })
        }
        None => None,
    };
    let upper = format!("{prefix}g");
    let scan_limit = (limit * 8 + 1).min(coordination_keys::MAX_INBOX_SCAN);
    let sql = if after_key.is_some() {
        "SELECT key,value_json FROM meta WHERE key>?1 AND key<?2 ORDER BY key LIMIT ?3"
    } else {
        "SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key LIMIT ?3"
    };
    let mut statement = db.prepare(sql)?;
    let rows: Vec<(String, String)> = statement
        .query_map(
            params![after_key.unwrap_or(prefix), upper, scan_limit],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?
        .collect::<std::result::Result<_, _>>()?;
    let mut items = Vec::new();
    let mut next_cursor = None;
    let mut has_more = rows.len() as i64 >= scan_limit;
    let mut scanned = 0i64;
    for (key, raw) in rows {
        scanned += 1;
        let index: Value = serde_json::from_str(&raw)?;
        let thread_id = model::text(&index, "thread_id")?;
        let Some(context) = load_context_optional(db, thread_id)? else {
            continue;
        };
        let expected_index_key = match &index_kind {
            ThreadListIndex::Task => {
                task_index_key(&context.task_id, &context.attempt_id, thread_id)
            }
            ThreadListIndex::Actor(client_id) => {
                actor_index_key(client_id, &context.task_id, &context.attempt_id, thread_id)
            }
        };
        if context.task_id != task_id
            || attempt_filter
                .as_deref()
                .is_some_and(|attempt| context.attempt_id != attempt)
            || expected_index_key != key
        {
            return Err(Error::new(
                "THREAD_INDEX_DAMAGED",
                "Thread list index differs from its record",
            ));
        }
        let authorized = match authorize_retained_thread_read(db, principal, thread_id) {
            Ok(_) => true,
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "FORBIDDEN" | "UNAUTHORIZED" | "NOT_FOUND"
                ) =>
            {
                false
            }
            Err(error) => return Err(error),
        };
        if !authorized {
            continue;
        }
        next_cursor = Some(thread_id.to_owned());
        if state_filter.is_some_and(|state| context.state != state)
            || topic_filter.is_some_and(|topic| context.topic_kind != topic)
        {
            continue;
        }
        items.push(public_projection(&context.projection));
        if items.len() as i64 >= limit {
            has_more = scanned < scan_limit || rows_have_more(db, &key, &upper)?;
            break;
        }
    }
    let mut page = json!({
        "task_id":task_id,
        "attempt_id":attempt_filter,
        "items":items,
        "next_cursor":next_cursor,
        "has_more":has_more,
        "coverage":if has_more {"bounded_page"} else {"complete_for_filter"},
    });
    while model::canonical(&page)?.len() > MAX_PAGE_BYTES {
        if page["items"]
            .as_array()
            .is_some_and(|items| items.len() > 1)
        {
            page["items"].as_array_mut().unwrap().pop();
            page["has_more"] = json!(true);
            page["coverage"] = json!("bounded_page");
            let next_cursor = page["items"]
                .as_array()
                .and_then(|items| items.last())
                .and_then(|item| item.get("thread_id"))
                .cloned()
                .unwrap_or(Value::Null);
            page["next_cursor"] = next_cursor;
        } else {
            return Err(Error::new(
                "PAYLOAD_TOO_LARGE",
                "Thread list header exceeds 64 KiB",
            ));
        }
    }
    Ok(page)
}

fn rows_have_more(db: &Connection, after_key: &str, upper: &str) -> Result<bool> {
    let exists: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM meta WHERE key>?1 AND key<?2)",
        params![after_key, upper],
        |row| row.get(0),
    )?;
    Ok(exists)
}

fn thread_list_index(
    db: &Connection,
    principal: &Principal,
    task_id: &str,
    requested_attempt: Option<&str>,
) -> Result<(ThreadListIndex, Option<String>)> {
    match principal.role {
        Role::Operator => {
            super::require_local_operator(db, &principal.client_id)?;
            Ok((ThreadListIndex::Task, requested_attempt.map(str::to_owned)))
        }
        Role::Participant => Ok((
            ThreadListIndex::Actor(principal.client_id.clone()),
            requested_attempt.map(str::to_owned),
        )),
        Role::Manager => {
            if let Some(attempt_id) = requested_attempt {
                if manager_can_read_task_history(db, principal, task_id, Some((attempt_id, None)))?
                {
                    Ok((ThreadListIndex::Task, Some(attempt_id.to_owned())))
                } else {
                    Ok((
                        ThreadListIndex::Actor(principal.client_id.clone()),
                        Some(attempt_id.to_owned()),
                    ))
                }
            } else {
                if manager_can_read_task_history(db, principal, task_id, None)? {
                    return Ok((ThreadListIndex::Task, None));
                }
                Ok((ThreadListIndex::Actor(principal.client_id.clone()), None))
            }
        }
        _ => Err(Error::new(
            "FORBIDDEN",
            "Thread list requires a current scoped Manager/Operator or exact retained participant",
        )),
    }
}

/// Read authority for retained Task history is derived from the actual Task
/// project and the current registered Manager/GM. It intentionally does not
/// require the historical Thread's pinned revision or Attempt to remain live.
fn manager_can_read_task_history(
    db: &Connection,
    principal: &Principal,
    task_id: &str,
    retained_attempt: Option<(&str, Option<i64>)>,
) -> Result<bool> {
    if principal.role != Role::Manager {
        return Ok(false);
    }
    let task = match tasks::get_task(db, task_id) {
        Ok(task) => task,
        Err(error) if error.code == "NOT_FOUND" => return Ok(false),
        Err(error) => return Err(error),
    };
    let project_id = model::text(&task, "project_id")?;
    let retained_attempt = if let Some((attempt_id, expected_revision)) = retained_attempt {
        let attempt = match tasks::get_attempt(db, attempt_id) {
            Ok(attempt) => attempt,
            Err(error) if error.code == "NOT_FOUND" => return Ok(false),
            Err(error) => return Err(error),
        };
        let retained_revision = attempt["task_revision"]
            .as_i64()
            .filter(|revision| *revision > 0)
            .ok_or_else(|| {
                Error::new("STORE_INVARIANT", "retained Attempt has no Task revision")
            })?;
        if attempt["task_id"] != task_id
            || expected_revision.is_some_and(|revision| revision != retained_revision)
        {
            return Ok(false);
        }
        Some(attempt)
    } else {
        None
    };
    if let Err(error) =
        crate::automation::authorization::require_registered_manager(db, &principal.client_id)
    {
        if error.code == "FORBIDDEN" {
            return Ok(false);
        }
        return Err(error);
    }
    match super::gm::require_authority(db, principal) {
        Ok(()) => return Ok(true),
        Err(error) if error.code == "FORBIDDEN" => {}
        Err(error) => return Err(error),
    }
    if retained_attempt
        .as_ref()
        .is_some_and(|attempt| attempt["owner_id"] == principal.client_id)
    {
        return Ok(true);
    }
    if task["state"] != "open" {
        return Ok(false);
    }
    let Some(task_revision) = task["revision"].as_i64().filter(|revision| *revision > 0) else {
        return Ok(false);
    };
    let Some(attempt_id) = task["current_attempt_id"].as_str() else {
        return Ok(false);
    };
    let current = match coordination_store::concilium_manager_scope(
        db,
        principal,
        task_id,
        task_revision,
        attempt_id,
    ) {
        Ok(scope) => scope,
        Err(error) if is_expected_scope_rejection(&error) => return Ok(false),
        Err(error) => return Err(error),
    };
    if current["task"]["task_id"] != task_id
        || current["task"]["revision"] != task_revision
        || current["task"]["project_id"] != project_id
        || current["attempt"]["attempt_id"] != attempt_id
        || current["attempt"]["owner_id"] != principal.client_id
    {
        return Ok(false);
    }
    crate::automation::authorization::current_manager_has_task_scope(
        db, principal, task_id, project_id,
    )
}

fn cancellation_for_delivery(db: &Connection, delivery_id: Option<&str>) -> Result<Value> {
    let Some(delivery_id) = delivery_id else {
        return Ok(Value::Null);
    };
    let operation: Option<(String, String)> = db
        .query_row(
            "SELECT operation_id,result_json FROM operations \
             WHERE method='message.cancel' AND state='settled' \
               AND json_extract(result_json,'$.cancellation.delivery_id')=?1 ORDER BY operation_id LIMIT 1",
            [delivery_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    match operation {
        Some((operation_id, raw)) => {
            let result: Value = serde_json::from_str(&raw)?;
            Ok(json!({"operation_id":operation_id,"result":result}))
        }
        None => Ok(Value::Null),
    }
}

fn resolve_reply(
    tx: &Transaction<'_>,
    context: &ThreadContext,
    sender_id: &str,
    recipient_id: &str,
    message_id: Option<&str>,
    digest_claim: Option<&str>,
) -> Result<(Value, Value)> {
    let Some(message_id) = message_id else {
        if digest_claim.is_some() {
            return Err(Error::invalid("reply digest requires a prior message"));
        }
        return Ok((Value::Null, Value::Null));
    };
    let operation_id: Option<String> = tx
        .query_row(
            "SELECT operation_id FROM operations WHERE method='coordination.message.send' AND state='settled' \
             AND json_extract(result_json,'$.message_id')=?1 ORDER BY operation_id LIMIT 1",
            [message_id],
            |row| row.get(0),
        )
        .optional()?;
    let operation_id = operation_id
        .ok_or_else(|| Error::new("NOT_FOUND", format!("Thread message {message_id}")))?;
    let prior = operations::get_operation(tx, &operation_id)?;
    let prior_result = &prior["result"];
    if prior["method"] != "coordination.message.send"
        || prior["task_id"] != context.task_id
        || prior["attempt_id"] != context.attempt_id
        || prior_result["message_id"] != message_id
        || prior_result["payload"]["thread_id"] != context.thread_id
        || prior_result["sender"] != recipient_id
        || prior_result["recipient"] != sender_id
    {
        return Err(Error::invalid(
            "reply must reverse the exact parties of a message in this Thread",
        ));
    }
    model::verify_payload_digest_claim(prior_result["payload_digest"].as_str(), digest_claim)?;
    let delivery_id = model::text(prior_result, "delivery_id")?;
    let exact_delivery = mailbox::find_delivery(tx, delivery_id)?
        .ok_or_else(|| Error::new("NOT_FOUND", format!("Delivery {delivery_id}")))?;
    if exact_delivery["operation_id"] != operation_id
        || exact_delivery["result"]["message_id"] != message_id
        || exact_delivery["result"]["payload"]["thread_id"] != context.thread_id
    {
        return Err(Error::new(
            "MAILBOX_DAMAGED",
            "reply delivery identity differs from its immutable Thread Operation",
        ));
    }
    let reply_to = json!({
        "delivery_id":delivery_id,
        "payload_digest":prior_result["payload_digest"],
    });
    Ok((json!(message_id), reply_to))
}

/// Deterministic progress classification is a hint for existing attention
/// consumers, not an authorization or execution decision. Free text is never
/// sent to a model to decide whether coordination should continue.
fn message_progress(
    thread_id: &str,
    sender_id: &str,
    recipient_id: &str,
    request: &wire::SendRequest,
) -> Result<Value> {
    let progress_kind = if request.speech_act == "resolution_summary" {
        "resolution"
    } else if matches!(request.speech_act.as_str(), "propose" | "counterproposal") {
        "new_proposal"
    } else if !request.evidence_refs.is_empty() {
        "new_evidence"
    } else if request.speech_act == "object" {
        "new_counterexample"
    } else {
        "none"
    };
    let content = json!({
        "speech_act":request.speech_act,
        "subject":request.subject,
        "summary":request.summary,
        "inline_body":request.inline_body,
        "body_ref":request.body_ref,
        "evidence_refs":request.evidence_refs,
        "proposal_revision_id":request.proposal_revision_id,
    });
    let progress_digest = format!(
        "sha256:{}",
        model::digest(model::canonical(&content)?.as_bytes())
    );
    let loop_basis = json!({
        "thread_id":thread_id,
        "sender_id":sender_id,
        "recipient_id":recipient_id,
        "content":content,
    });
    let loop_fingerprint = format!(
        "sha256:{}",
        model::digest(model::canonical(&loop_basis)?.as_bytes())
    );
    Ok(json!({
        "progress_kind":progress_kind,
        "progress_digest":progress_digest,
        "loop_fingerprint":loop_fingerprint,
        "schedules_model_work":false,
    }))
}

fn validate_contract_ratification(
    db: &Connection,
    context: &ThreadContext,
    operation_id: &str,
    proposal_id: &str,
    proposal_revision_id: &str,
    proposal_digest: &str,
) -> Result<()> {
    let decision = coordination_store::load_contract_decision(
        db,
        coordination_store::ContractDecisionIdentity {
            thread_id: &context.thread_id,
            task_id: &context.task_id,
            task_revision: context.task_revision,
            attempt_id: &context.attempt_id,
            proposal_id,
            proposal_revision_id,
            proposal_digest,
        },
    )?
    .ok_or_else(|| {
        Error::new(
            "RATIFICATION_REQUIRED",
            "contract resolution requires the exact retained ratification decision",
        )
    })?;
    if decision["decision"] != "ratified" || decision["decision_operation_id"] != operation_id {
        return Err(Error::new(
            "RATIFICATION_INVALID",
            "contract resolution requires the ratification Operation for this exact current proposal revision",
        ));
    }
    Ok(())
}

fn validate_proposal_revision(
    db: &Connection,
    context: &ThreadContext,
    proposal_revision_id: &str,
) -> Result<()> {
    coordination_store::load_contract_proposal_revision(
        db,
        &context.thread_id,
        &context.task_id,
        context.task_revision,
        &context.attempt_id,
        proposal_revision_id,
    )
    .map(|_| ())
}

fn participant_has_own_question(
    db: &Connection,
    context: &ThreadContext,
    client_id: &str,
) -> Result<bool> {
    let mut statement = db.prepare(
        "SELECT result_json FROM operations \
         WHERE method='coordination.message.send' AND state='settled' \
           AND task_id=?1 AND attempt_id=?2 \
           AND json_extract(result_json,'$.payload.thread_id')=?3 \
         ORDER BY CAST(json_extract(result_json,'$.payload.message_seq') AS INTEGER),operation_id",
    )?;
    let rows = statement.query_map(
        params![context.task_id, context.attempt_id, context.thread_id],
        |row| row.get::<_, String>(0),
    )?;
    let mut open_questions = BTreeSet::new();
    for row in rows {
        let raw = row?;
        let result: Value = serde_json::from_str(&raw)?;
        let payload = result
            .get("payload")
            .filter(|payload| payload.is_object())
            .ok_or_else(|| {
                Error::new(
                    "THREAD_DAMAGED",
                    "message Operation lacks its typed payload",
                )
            })?;
        let message_id = payload
            .get("message_id")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("THREAD_DAMAGED", "typed message lacks its ID"))?;
        let sender_id = payload["sender_actor"]
            .get("client_id")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("THREAD_DAMAGED", "typed message lacks its sender"))?;
        match payload.get("speech_act").and_then(Value::as_str) {
            Some("query") if sender_id == client_id => {
                open_questions.insert(message_id.to_owned());
            }
            Some("answer" | "withdraw") => {
                if let Some(replied_to) = payload.get("in_reply_to").and_then(Value::as_str) {
                    open_questions.remove(replied_to);
                }
            }
            _ => {}
        }
    }
    Ok(!open_questions.is_empty())
}

fn require_expected_revision(context: &ThreadContext, expected: i64) -> Result<()> {
    if context.state != "open" || context.state_revision != expected {
        return Err(Error::new(
            "STALE_REVISION",
            "Thread state changed; reload before structural mutation",
        ));
    }
    Ok(())
}

fn require_current_assignment(attempt: &Value, assignment_id: &str) -> Result<()> {
    let producers = attempt["producers"]
        .as_array()
        .ok_or_else(|| Error::new("STALE_REVISION", "Attempt ProducerRefs are absent"))?;
    let matches: Vec<&Value> = producers
        .iter()
        .filter(|producer| producer["assignment_id"] == assignment_id)
        .collect();
    if matches.len() != 1
        || matches[0]["attempt_id"]
            .as_str()
            .is_some_and(|id| id != attempt["attempt_id"].as_str().unwrap_or_default())
        || matches!(
            matches[0]["disposition"].as_str(),
            Some("completed" | "failed" | "cancelled")
        )
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "assignment_id must identify one active ProducerRef on this Attempt",
        ));
    }
    Ok(())
}

fn resolve_participant(
    db: &Connection,
    client_id: &str,
    generation: Option<i64>,
    reason: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
) -> Result<Value> {
    let registration = meta(db, &format!("client:{client_id}"))?.ok_or_else(|| {
        Error::new(
            "NOT_FOUND",
            format!("participant {client_id} is not registered"),
        )
    })?;
    if registration["disabled"] == true {
        return Err(Error::new(
            "UNAUTHORIZED",
            "Thread participant registration is disabled",
        ));
    }
    let role = model::text(&registration, "role")?;
    if !matches!(role, "participant" | "manager") {
        return Err(Error::new(
            "FORBIDDEN",
            "Thread participants must be exact registered Participant or Manager clients",
        ));
    }
    let registered_generation = registration
        .get("binding_generation")
        .and_then(Value::as_i64);
    if generation != registered_generation {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "participant generation must exactly match the current registration",
        ));
    }
    let scope = coordination_store::watch_scope_for_creator(
        db,
        role,
        client_id,
        task_id,
        task_revision,
        attempt_id,
    )?
    .ok_or_else(|| {
        Error::new(
            "STALE_PARTICIPANT",
            "participant lacks the exact current Task/Attempt scope",
        )
    })?;
    let basis = registration
        .get("participation_basis")
        .cloned()
        .unwrap_or(Value::Null);
    let registration_fingerprint = registration_fingerprint(client_id, &registration)?;
    let actor = model::message_actor(&registration, client_id);
    let mut participant_scope = model::message_scope(&registration, client_id);
    participant_scope["task_id"] = json!(task_id);
    participant_scope["task_revision"] = json!(task_revision);
    participant_scope["attempt_id"] = json!(attempt_id);
    participant_scope["participation_basis"] = basis.clone();
    if scope["participant"]["client_id"]
        .as_str()
        .is_some_and(|scope_client| scope_client != client_id)
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "participant scope identity differs",
        ));
    }
    Ok(json!({
        "client_id":client_id,
        "role":role,
        "generation":registered_generation,
        "reason":reason,
        "actor":actor,
        "scope":participant_scope,
        "participation_basis":basis,
        "registration_fingerprint":registration_fingerprint,
        "grant_revision":registration.get("grant_revision").cloned().unwrap_or(Value::Null),
    }))
}

fn validate_participant_current(
    db: &Connection,
    participant: &ThreadParticipant,
    context: &ThreadContext,
) -> Result<()> {
    let registration = meta(db, &format!("client:{}", participant.client_id))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "Thread participant is no longer registered"))?;
    if registration["disabled"] == true
        || registration["role"] != participant.role
        || registration
            .get("binding_generation")
            .and_then(Value::as_i64)
            != participant.generation
        || registration
            .get("participation_basis")
            .cloned()
            .unwrap_or(Value::Null)
            != participant.participation_basis
        || registration_fingerprint(&participant.client_id, &registration)?
            != participant.registration_fingerprint
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "Thread participant registration or generation changed",
        ));
    }
    let scope = coordination_store::watch_scope_for_creator(
        db,
        &participant.role,
        &participant.client_id,
        &context.task_id,
        context.task_revision,
        &context.attempt_id,
    )?
    .ok_or_else(|| {
        Error::new(
            "STALE_PARTICIPANT",
            "Thread participant no longer has current Task/Attempt scope",
        )
    })?;
    if scope["task"]["task_id"] != context.task_id
        || scope["task"]["revision"] != context.task_revision
        || scope["attempt"]["attempt_id"] != context.attempt_id
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "Thread participant scope changed",
        ));
    }
    Ok(())
}

fn validate_artifact_ref(
    db: &Connection,
    principal: &Principal,
    artifact_id: Option<&str>,
) -> Result<()> {
    let Some(artifact_id) = artifact_id else {
        return Ok(());
    };
    let artifact = results::get(db, artifact_id)?;
    super::reviews::authorize_artifact_read(db, principal, artifact_id)?;
    if matches!(
        artifact.kind.as_str(),
        "script_bundle" | "script_result" | "script_output"
    ) {
        super::scripts::authorize_artifact_read(db, principal, &artifact)?;
    }
    Ok(())
}

fn validate_evidence_ref(
    db: &Connection,
    principal: &Principal,
    context: &ThreadContext,
    reference: &str,
) -> Result<()> {
    if let Some(operation_id) = reference.strip_prefix("operation:") {
        let operation = operations::get_operation(db, operation_id)?;
        if !matches!(
            operation["state"].as_str(),
            Some("settled" | "rejected" | "cancelled")
        ) || operation["task_id"] != context.task_id
            || operation["attempt_id"] != context.attempt_id
        {
            return Err(Error::new(
                "FORBIDDEN",
                "evidence Operation is outside this Task/Attempt",
            ));
        }
        return Ok(());
    }
    if let Some(artifact_id) = reference.strip_prefix("artifact:") {
        return validate_artifact_ref(db, principal, Some(artifact_id));
    }
    if let Some(raw_observation_id) = reference.strip_prefix("observation:") {
        let observation_id = raw_observation_id
            .parse::<i64>()
            .map_err(|_| Error::invalid("observation evidence ID must be a positive integer"))?;
        let exact: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM observations o JOIN operations op ON op.operation_id=o.operation_id \
             WHERE o.observation_id=?1 AND op.task_id=?2 AND op.attempt_id=?3)",
            params![observation_id, context.task_id, context.attempt_id],
            |row| row.get(0),
        )?;
        if !exact {
            return Err(Error::new(
                "NOT_FOUND",
                "observation evidence is outside this Task/Attempt",
            ));
        }
        return Ok(());
    }
    if let Some(submission_ref) = reference.strip_prefix("submission:") {
        let submission = super::submissions::document(db, submission_ref)?;
        if submission["task_id"] != context.task_id
            || submission["attempt_id"] != context.attempt_id
        {
            return Err(Error::new(
                "FORBIDDEN",
                "submission evidence is outside this Task/Attempt",
            ));
        }
        return Ok(());
    }
    if let Some(source_ref) = reference.strip_prefix("source:") {
        let artifact = results::get(db, source_ref)?;
        if artifact.kind != "source_snapshot" {
            return Err(Error::new(
                "INVALID_EVIDENCE",
                "source evidence must name a registered source snapshot",
            ));
        }
        super::reviews::authorize_artifact_read(db, principal, source_ref)?;
        return Ok(());
    }
    Err(Error::invalid(
        "evidence refs must use operation:, observation:, artifact:, submission:, or source: identities",
    ))
}

fn current_scope_matches(
    db: &Connection,
    principal: &Principal,
    context: &ThreadContext,
) -> Result<()> {
    let scope = match principal.role {
        Role::Participant => coordination_store::watch_scope(db, principal, None, None, None)?,
        Role::Manager | Role::Operator => coordination_store::watch_scope(
            db,
            principal,
            Some(&context.task_id),
            Some(context.task_revision),
            Some(&context.attempt_id),
        )?,
        _ => {
            return Err(Error::new(
                "FORBIDDEN",
                "Thread mutation requires exact Participant, Manager/GM, or local Operator authority",
            ));
        }
    };
    if scope["task"]["task_id"] != context.task_id
        || scope["task"]["revision"] != context.task_revision
        || scope["attempt"]["attempt_id"] != context.attempt_id
    {
        return Err(Error::new(
            "STALE_REVISION",
            "Thread mutation requires its exact current Task revision and Attempt",
        ));
    }
    Ok(())
}

fn retained_identity_matches(
    db: &Connection,
    principal: &Principal,
    context: &ThreadContext,
) -> Result<bool> {
    let role = role_name(&principal.role);
    if let Some(participant) = context.participants.iter().find(|participant| {
        participant.client_id == principal.client_id && participant.role == role
    }) {
        let registration = match meta(db, &format!("client:{}", principal.client_id))? {
            Some(registration) => registration,
            None => return Ok(false),
        };
        return Ok(registration["disabled"] != true
            && registration["role"] == participant.role
            && registration
                .get("binding_generation")
                .and_then(Value::as_i64)
                == participant.generation
            && registration
                .get("participation_basis")
                .cloned()
                .unwrap_or(Value::Null)
                == participant.participation_basis
            && registration_fingerprint(&participant.client_id, &registration)?
                == participant.registration_fingerprint);
    }
    Ok(false)
}

fn require_current_registration(db: &Connection, principal: &Principal) -> Result<Value> {
    let registration = meta(db, &format!("client:{}", principal.client_id))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "caller is not registered"))?;
    if registration["disabled"] == true || registration["role"] != role_name(&principal.role) {
        return Err(Error::new(
            "UNAUTHORIZED",
            "caller registration is disabled or changed",
        ));
    }
    if principal.role == Role::Operator {
        super::require_local_operator(db, &principal.client_id)?;
    }
    Ok(registration)
}

fn registration_fingerprint(client_id: &str, registration: &Value) -> Result<String> {
    let authority = json!({
        "client_id":client_id,
        "role":registration.get("role").cloned().unwrap_or(Value::Null),
        "task_id":registration.get("task_id").cloned().unwrap_or(Value::Null),
        "task_revision":registration.get("task_revision").cloned().unwrap_or(Value::Null),
        "attempt_id":registration.get("attempt_id").cloned().unwrap_or(Value::Null),
        "participation_basis":registration.get("participation_basis").cloned().unwrap_or(Value::Null),
        "binding_id":registration.get("binding_id").cloned().unwrap_or(Value::Null),
        "binding_generation":registration.get("binding_generation").cloned().unwrap_or(Value::Null),
        "grant_revision":registration.get("grant_revision").cloned().unwrap_or(Value::Null),
    });
    Ok(format!(
        "sha256:{}",
        model::digest(model::canonical(&authority)?.as_bytes())
    ))
}

fn participant_projection(participant: &Value) -> Value {
    json!({
        "client_id":participant["client_id"],
        "role":participant["role"],
        "generation":participant["generation"],
        "reason":participant["reason"],
        "actor":participant["actor"],
        "scope":participant["scope"],
        "participation_basis":participant["participation_basis"],
        "registration_fingerprint":participant["registration_fingerprint"],
        "grant_revision":participant["grant_revision"],
    })
}

fn public_projection(value: &Value) -> Value {
    let mut projection = value.clone();
    if let Some(participants) = projection
        .get_mut("participants")
        .and_then(Value::as_array_mut)
    {
        for participant in participants {
            if let Some(object) = participant.as_object_mut() {
                object.remove("registration_fingerprint");
                object.remove("grant_revision");
            }
        }
    }
    if let Some(object) = projection.as_object_mut() {
        object.remove("creator_registration_fingerprint");
    }
    projection
}

fn load_context(db: &Connection, thread_id: &str) -> Result<ThreadContext> {
    let value = meta(db, &thread_key(thread_id))?
        .ok_or_else(|| Error::new("NOT_FOUND", format!("Thread {thread_id}")))?;
    context_from_projection(value, thread_id)
}

fn load_context_optional(db: &Connection, thread_id: &str) -> Result<Option<ThreadContext>> {
    match meta(db, &thread_key(thread_id))? {
        Some(value) => context_from_projection(value, thread_id).map(Some),
        None => Ok(None),
    }
}

fn context_from_projection(value: Value, requested_id: &str) -> Result<ThreadContext> {
    if value["schema_version"] != 1 || value["thread_id"] != requested_id {
        return Err(Error::new(
            "THREAD_DAMAGED",
            "Thread projection identity is invalid",
        ));
    }
    let raw_participants = value["participants"]
        .as_array()
        .ok_or_else(|| Error::new("THREAD_DAMAGED", "Thread participant roster is invalid"))?;
    let mut participants = Vec::with_capacity(raw_participants.len());
    let mut seen = BTreeSet::new();
    for raw in raw_participants {
        let client_id = model::text(raw, "client_id")?.to_owned();
        let role = model::text(raw, "role")?.to_owned();
        if !seen.insert((client_id.clone(), role.clone())) {
            return Err(Error::new(
                "THREAD_DAMAGED",
                "Thread roster contains duplicate identities",
            ));
        }
        model::text(raw, "reason")?;
        participants.push(ThreadParticipant {
            client_id,
            role,
            generation: raw.get("generation").and_then(Value::as_i64),
            participation_basis: raw
                .get("participation_basis")
                .cloned()
                .unwrap_or(Value::Null),
            registration_fingerprint: model::text(raw, "registration_fingerprint")?.to_owned(),
            actor: raw.get("actor").cloned().unwrap_or(Value::Null),
            scope: raw.get("scope").cloned().unwrap_or(Value::Null),
        });
    }
    let state = model::text(&value, "state")?.to_owned();
    if !THREAD_STATES.contains(&state.as_str()) {
        return Err(Error::new("THREAD_DAMAGED", "Thread state is invalid"));
    }
    let topic_kind = model::text(&value, "topic_kind")?.to_owned();
    model::text(&value, "subject")?;
    Ok(ThreadContext {
        thread_id: model::text(&value, "thread_id")?.to_owned(),
        task_id: model::text(&value, "task_id")?.to_owned(),
        task_revision: value["task_revision"]
            .as_i64()
            .filter(|number| *number > 0)
            .ok_or_else(|| Error::new("THREAD_DAMAGED", "Thread Task revision is invalid"))?,
        attempt_id: model::text(&value, "attempt_id")?.to_owned(),
        sponsor_owner_id: model::text(&value, "sponsor_owner_id")?.to_owned(),
        topic_kind,
        state,
        state_revision: value["state_revision"]
            .as_i64()
            .filter(|number| *number > 0)
            .ok_or_else(|| Error::new("THREAD_DAMAGED", "Thread state revision is invalid"))?,
        next_message_seq: value["next_message_seq"]
            .as_i64()
            .filter(|number| *number > 0)
            .ok_or_else(|| Error::new("THREAD_DAMAGED", "Thread message sequence is invalid"))?,
        participants,
        projection: value,
    })
}

fn stamp_operation_scope(
    tx: &Transaction<'_>,
    principal: &Principal,
    method: &str,
    operation_id: &str,
    client_request_id: &str,
    task_id: &str,
    attempt_id: &str,
) -> Result<()> {
    let changed = tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1 AND caller_id=?4 AND method=?5 AND client_request_id=?6 \
         AND (task_id IS NULL OR task_id=?2) AND (attempt_id IS NULL OR attempt_id=?3)",
        params![operation_id, task_id, attempt_id, principal.client_id, method, client_request_id],
    )?;
    if changed != 1 {
        return Err(Error::new(
            "OPERATION_SCOPE_INVALID",
            "admitted Operation is absent, belongs to another caller/method, or has a different Task scope",
        ));
    }
    Ok(())
}

fn active_thread_count(db: &Connection, scope_id: &str) -> Result<i64> {
    let prefix = format!("{THREAD_ACTIVE_INDEX_PREFIX}{scope_id}:");
    let upper = format!("{prefix}g");
    db.query_row(
        "SELECT COUNT(*) FROM meta WHERE key>=?1 AND key<?2",
        params![prefix, upper],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn task_index_prefix(task_id: &str, attempt_id: Option<&str>) -> String {
    let task = coordination_keys::key_component(task_id);
    match attempt_id {
        Some(attempt) => format!(
            "{THREAD_TASK_INDEX_PREFIX}{task}:{}:",
            coordination_keys::key_component(attempt)
        ),
        None => format!("{THREAD_TASK_INDEX_PREFIX}{task}:"),
    }
}

fn thread_key(thread_id: &str) -> String {
    format!("{THREAD_PREFIX}{thread_id}")
}

fn task_index_key(task_id: &str, attempt_id: &str, thread_id: &str) -> String {
    format!(
        "{}{}",
        task_index_prefix(task_id, Some(attempt_id)),
        coordination_keys::key_component(thread_id)
    )
}

fn actor_index_prefix(client_id: &str, task_id: &str, attempt_id: Option<&str>) -> String {
    let actor = coordination_keys::key_component(client_id);
    let task = coordination_keys::key_component(task_id);
    match attempt_id {
        Some(attempt) => format!(
            "{THREAD_ACTOR_INDEX_PREFIX}{actor}:{task}:{}:",
            coordination_keys::key_component(attempt)
        ),
        None => format!("{THREAD_ACTOR_INDEX_PREFIX}{actor}:{task}:"),
    }
}

fn actor_index_key(client_id: &str, task_id: &str, attempt_id: &str, thread_id: &str) -> String {
    format!(
        "{}{}",
        actor_index_prefix(client_id, task_id, Some(attempt_id)),
        coordination_keys::key_component(thread_id)
    )
}

fn active_index_key(scope_id: &str, thread_id: &str) -> String {
    format!(
        "{THREAD_ACTIVE_INDEX_PREFIX}{scope_id}:{}",
        coordination_keys::key_component(thread_id)
    )
}

fn fingerprint_key(scope_id: &str, fingerprint: &str) -> String {
    format!("{THREAD_FINGERPRINT_PREFIX}{scope_id}:{fingerprint}")
}

fn role_name(role: &Role) -> &'static str {
    match role {
        Role::Operator => "operator",
        Role::Manager => "manager",
        Role::Participant => "participant",
        Role::Observer => "observer",
        Role::Module => "module",
        Role::ModuleSupervisor => "module_supervisor",
        Role::Scheduler => "scheduler",
        Role::HookSource => "hook_source",
    }
}

fn require_manager_or_operator(principal: &Principal) -> Result<()> {
    if matches!(principal.role, Role::Manager | Role::Operator) {
        Ok(())
    } else {
        Err(Error::new(
            "FORBIDDEN",
            "current Manager/GM or local Operator authority required",
        ))
    }
}

fn thread_operation_method(method: &str) -> bool {
    matches!(
        method,
        "coordination.thread.open"
            | "coordination.thread.resolve"
            | "coordination.thread.withdraw"
            | "coordination.thread.supersede"
            | "coordination.message.send"
            | "coordination.contract.propose"
            | "coordination.contract.respond"
            | "coordination.contract.ratify"
            | "coordination.contract.reject"
    )
}
