//! Participant-scoped coordination projections over the existing Store.
//!
//! The `meta` rows here are small, namespaced current records and exact
//! relevance indexes. Operations and Observations remain the audit/mailbox
//! authority; this module adds no schema or Task graph.

use super::{
    ParticipantCapabilityScope, gm, meta, operations, participant_capability_projection, results,
    set_meta, tasks,
};
use crate::{
    config::Config,
    coordination as keys,
    error::{Error, Result},
    model::{self, Principal, Role},
    store::launcher::LaunchActor,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

const LIVE_ATTEMPT_STATES: &[&str] = &[
    "reserved",
    "running",
    "submitted",
    "needs_correction",
    "recovery_pending",
];
const INBOUND_POLICIES: &[&str] = &["pull_only", "safe_boundary", "hold", "refuse"];
const RELEVANCE_SCAN_FACTOR: i64 = 4;

#[derive(Clone)]
struct ScopeData {
    client_id: Option<String>,
    registration: Value,
    task: Value,
    attempt: Value,
    scope_id: String,
}

#[derive(Clone)]
struct IndexedCard {
    client_id: String,
    card_kind: String,
    identity: String,
}

struct ConsultCardMatch {
    indexed: IndexedCard,
    participant: ScopeData,
    card: Value,
}

struct ParticipantOperationRecord {
    caller_id: String,
    method: String,
    state: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
    result: Value,
}

/// The returned value is safe for a participant-facing context projection.
/// The credential hash and native runtime identity never leave the Store.
pub(crate) fn current_scope(db: &Connection, principal: &Principal) -> Result<Value> {
    principal.require_participant()?;
    let scope = load_current_scope_for_client(db, &principal.client_id)?;
    Ok(scope_projection(&scope))
}

/// Admit submission only from a live, ordinary Participant grant for this
/// exact Task revision and Attempt. Sponsored reviewer grants remain limited
/// to their retained review slot and never acquire Task submission authority.
pub(crate) fn authorize_task_submission(
    db: &Connection,
    principal: &Principal,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
) -> Result<()> {
    principal.require_participant()?;
    let scope = load_current_scope(db, principal)?;
    if !matches!(
        scope.registration["participation_basis"]["kind"].as_str(),
        Some("attempt_owner" | "producer_ref")
    ) {
        return Err(Error::new(
            "FORBIDDEN",
            "review-only Participant grants cannot submit a Task",
        ));
    }
    if scope.task["task_id"] != task_id
        || scope.task["revision"] != task_revision
        || scope.attempt["attempt_id"] != attempt_id
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "submission must name this Participant's exact current Task revision and Attempt",
        ));
    }
    Ok(())
}

/// Enforce current Participant scope before the Store reaches general writer
/// fallthrough. Apply functions repeat this check in the write transaction.
pub(crate) fn authorize_participant_mutation(
    db: &Connection,
    principal: &Principal,
    method: &str,
) -> Result<()> {
    principal.require_participant()?;
    if !matches!(
        method,
        "coordination.work_card.publish"
            | "coordination.work_card.withdraw"
            | "coordination.contract_card.publish"
            | "coordination.contract_card.withdraw"
            | "coordination.send"
            | "coordination.consult"
            | "coordination.sync_integration"
            | "coordination.watch.create"
            | "coordination.watch.cancel"
    ) {
        return Err(Error::new(
            "FORBIDDEN",
            "participant credentials cannot perform this mutation",
        ));
    }
    load_current_scope(db, principal)?;
    Ok(())
}

/// Manager launcher projection helper. It validates exact current assignment
/// ownership before returning a bounded, redacted local participant page.
pub(crate) fn list_scope_participants(
    db: &Connection,
    principal: &Principal,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    limit: i64,
    after_client_id: Option<&str>,
) -> Result<Value> {
    let scope = manager_scope(db, principal, task_id, task_revision, attempt_id)?;
    list_participant_page(db, &scope, limit, after_client_id)
}

/// Exact contract relevance, with every indexed grant and card revalidated.
/// This is deliberately bounded; incomplete coverage is never an agreement.
pub(super) fn list_current_contract_participants(
    db: &Connection,
    principal: &Principal,
    contract_key: &str,
    limit: i64,
) -> Result<Value> {
    validate_identifier(contract_key, "contract_key", 1024)?;
    if !(1..=keys::MAX_PAGE_SIZE).contains(&limit) {
        return Err(Error::invalid("contract participant limit must be 1..=50"));
    }
    let caller = load_current_scope(db, principal)?;
    let scan_limit = (limit * RELEVANCE_SCAN_FACTOR + 32).min(keys::MAX_INBOX_SCAN);
    let (indexed, more) = indexed_cards(
        db,
        &caller.scope_id,
        keys::TermKind::Contract,
        contract_key,
        Some("contract"),
        None,
        scan_limit,
    )?;
    let mut items = Vec::new();
    let mut stale = 0usize;
    let mut overflow = false;
    for index in indexed {
        let member = match load_current_scope_for_client(db, &index.client_id) {
            Ok(member) if member.scope_id == caller.scope_id => member,
            Ok(_) => {
                stale += 1;
                continue;
            }
            Err(error) if is_stale_watch_authority(&error) => {
                stale += 1;
                continue;
            }
            Err(error) => return Err(error),
        };
        let Some(card) = current_card(
            db,
            &caller.scope_id,
            "contract",
            &index.identity,
            &index.client_id,
        )?
        else {
            stale += 1;
            continue;
        };
        if card["identity"] != contract_key
            || index.identity != contract_key
            || card["client_id"] != index.client_id
            || card["task_id"] != caller.task["task_id"]
            || card["task_revision"] != caller.task["revision"]
            || card["attempt_id"] != caller.attempt["attempt_id"]
            || card["card_kind"] != "contract"
        {
            stale += 1;
            continue;
        }
        if items.len() == limit as usize {
            overflow = true;
            break;
        }
        items.push(json!({"client_id":index.client_id,"participation_basis":member.registration["participation_basis"],"card":card}));
    }
    let incomplete = more || overflow || stale > 0;
    Ok(json!({
        "scope_id":caller.scope_id,
        "task_id":caller.task["task_id"],"task_revision":caller.task["revision"],"attempt_id":caller.attempt["attempt_id"],
        "items":items,"coverage":if incomplete { "partial" } else { "complete" },
        "gaps":if incomplete { json!([{"kind":"bounded_or_stale_contract_relevance","stale_entries":stale,"more":more || overflow}]) } else { json!([]) },
    }))
}

/// Authorization only for a retained watch subject. Unlike context access,
/// the subject may have transitioned; this returns no historical payload.
pub(crate) fn watch_creator_authorized_for_subject(
    db: &Connection,
    creator_role: &str,
    creator_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
) -> Result<bool> {
    let Some(registration) = meta(db, &format!("client:{creator_id}"))? else {
        return Ok(false);
    };
    if registration["disabled"] == true || registration["role"] != creator_role {
        return Ok(false);
    }
    let attempt = match tasks::get_attempt(db, attempt_id) {
        Ok(attempt) => attempt,
        Err(error) if error.code == "NOT_FOUND" => return Ok(false),
        Err(error) => return Err(error),
    };
    if attempt["task_id"] != task_id || attempt["task_revision"] != task_revision {
        return Ok(false);
    }
    match creator_role {
        "operator" => match super::require_local_operator(db, creator_id) {
            Ok(()) => Ok(true),
            Err(error) if error.code == "LOCAL_OPERATOR_MISMATCH" => Ok(false),
            Err(error) => Err(error),
        },
        "manager" => Ok(attempt["owner_id"] == creator_id
            || gm::record(db)?.is_some_and(|record| record["client_id"] == creator_id)),
        "participant" => {
            if registration["task_id"] != task_id
                || registration["task_revision"] != task_revision
                || registration["attempt_id"] != attempt_id
                || registration.get("binding_id").unwrap_or(&Value::Null)
                    != attempt.get("binding_id").unwrap_or(&Value::Null)
                || registration
                    .get("binding_generation")
                    .unwrap_or(&Value::Null)
                    != attempt.get("binding_generation").unwrap_or(&Value::Null)
            {
                return Ok(false);
            }
            let basis = &registration["participation_basis"];
            match basis["kind"].as_str() {
                Some("attempt_owner") => Ok(registration["created_by"] == attempt["owner_id"]),
                Some("producer_ref") => {
                    let Some(assignment) = basis["assignment_id"].as_str() else {
                        return Ok(false);
                    };
                    let Some(producer) = matching_producer(&attempt, assignment) else {
                        return Ok(false);
                    };
                    if matches!(
                        producer["disposition"].as_str(),
                        Some("completed" | "failed" | "cancelled")
                    ) {
                        return Ok(false);
                    }
                    Ok(registration["native_session_id"].is_null()
                        || registration["native_session_id"] == producer["native_session_id"])
                }
                Some("sponsored_reviewer") => {
                    let scope = &basis["review_scope"];
                    if scope["task_id"] != task_id
                        || scope["task_revision"] != task_revision
                        || scope["attempt_id"] != attempt_id
                        || scope["submission_ref"] != attempt["submission_ref"]
                        || scope["candidate_ref"] != attempt["candidate_ref"]
                    {
                        return Ok(false);
                    }
                    match verify_review_assignment(db, creator_id, &registration, scope) {
                        Ok(()) => Ok(true),
                        Err(error) if is_stale_watch_authority(&error) => Ok(false),
                        Err(error) => Err(error),
                    }
                }
                _ => Ok(false),
            }
        }
        _ => Ok(false),
    }
}

/// Resolve the exact scope used by participant coordination and watch methods.
/// Participants may never override their Task/Attempt identity; Managers and
/// Operators must name one exact current assignment they are authorized to
/// inspect.
pub(crate) fn watch_scope(
    db: &Connection,
    principal: &Principal,
    task_id: Option<&str>,
    task_revision: Option<i64>,
    attempt_id: Option<&str>,
) -> Result<Value> {
    let scope = match principal.role {
        Role::Participant => {
            if task_id.is_some() || task_revision.is_some() || attempt_id.is_some() {
                return Err(Error::invalid(
                    "participants use their authenticated Task/Attempt scope",
                ));
            }
            load_current_scope(db, principal)?
        }
        Role::Manager | Role::Operator => {
            let (Some(task_id), Some(task_revision), Some(attempt_id)) =
                (task_id, task_revision, attempt_id)
            else {
                return Err(Error::invalid(
                    "manager watch calls require task_id, task_revision, and attempt_id",
                ));
            };
            manager_scope(db, principal, task_id, task_revision, attempt_id)?
        }
        _ => {
            return Err(Error::new(
                "FORBIDDEN",
                "watch scope requires a Participant or authorized manager",
            ));
        }
    };
    Ok(scope_projection(&scope))
}

/// Revalidate the persisted watch creator without constructing an authenticated
/// Principal. Expected revocation or stale-scope outcomes return `None` so the
/// shared reconciler can settle quietly; storage and invariant errors propagate.
pub(crate) fn watch_scope_for_creator(
    db: &Connection,
    creator_role: &str,
    creator_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
) -> Result<Option<Value>> {
    let Some(registration) = meta(db, &format!("client:{creator_id}"))? else {
        return Ok(None);
    };
    if registration["disabled"] == true || registration["role"] != creator_role {
        return Ok(None);
    }
    match creator_role {
        "participant" => {
            let scope = match load_current_scope_for_client(db, creator_id) {
                Ok(scope) => scope,
                Err(error) if is_stale_watch_authority(&error) => return Ok(None),
                Err(error) => return Err(error),
            };
            if scope.task["task_id"] != task_id
                || scope.task["revision"] != task_revision
                || scope.attempt["attempt_id"] != attempt_id
            {
                return Ok(None);
            }
            Ok(Some(scope_projection(&scope)))
        }
        "manager" | "operator" => {
            let task = match tasks::get_task(db, task_id) {
                Ok(task) => task,
                Err(error) if error.code == "NOT_FOUND" => return Ok(None),
                Err(error) => return Err(error),
            };
            if task["state"] != "open"
                || task["revision"] != task_revision
                || task["current_attempt_id"] != attempt_id
            {
                return Ok(None);
            }
            let attempt = match tasks::get_attempt(db, attempt_id) {
                Ok(attempt) => attempt,
                Err(error) if error.code == "NOT_FOUND" => return Ok(None),
                Err(error) => return Err(error),
            };
            if let Err(error) =
                validate_current_attempt(&task, &attempt, task_id, task_revision, attempt_id)
            {
                if error.code == "STALE_PARTICIPANT" {
                    return Ok(None);
                }
                return Err(error);
            }
            let authorized = if creator_role == "operator" {
                match super::require_local_operator(db, creator_id) {
                    Ok(()) => true,
                    Err(error) if error.code == "LOCAL_OPERATOR_MISMATCH" => false,
                    Err(error) => return Err(error),
                }
            } else {
                let is_attempt_owner = attempt["owner_id"] == creator_id;
                let is_current_gm = gm::record(db)?
                    .is_some_and(|designation| designation["client_id"] == creator_id);
                is_attempt_owner || is_current_gm
            };
            if !authorized {
                return Ok(None);
            }
            Ok(Some(scope_projection(&ScopeData {
                client_id: None,
                registration: Value::Null,
                task,
                attempt,
                scope_id: keys::scope_id(task_id, task_revision, attempt_id)?,
            })))
        }
        _ => Ok(None),
    }
}

fn is_stale_watch_authority(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "NOT_FOUND"
            | "FORBIDDEN"
            | "UNAUTHORIZED"
            | "STALE_PARTICIPANT"
            | "STALE_REVISION"
            | "PARTICIPANT_NOT_ASSIGNED"
            | "STALE_REVIEW_ASSIGNMENT"
    )
}

/// Resolve configured auditor profiles through the exact pending-slot index.
/// This keeps review assignment admission proportional to the matching slot,
/// rather than walking every credential record in the installation.
pub(crate) fn find_review_profile_participants(
    tx: &Transaction<'_>,
    sponsor_client_id: &str,
    profile: &str,
    pending_scope: &Value,
) -> Result<Vec<String>> {
    let pending_scope = normalized_review_scope(pending_scope, true)?;
    if !pending_scope["review_assignment_id"].is_null() {
        return Err(Error::invalid(
            "profile lookup requires a pending review scope with null review_assignment_id",
        ));
    }
    let prefix = keys::pending_review_profile_prefix(&pending_scope, sponsor_client_id, profile)?;
    let upper = format!("{prefix}g");
    let mut statement = tx
        .prepare("SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key LIMIT 3")?;
    let rows: Vec<(String, String)> = statement
        .query_map(params![prefix, upper], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<std::result::Result<_, _>>()?;
    let mut clients = Vec::with_capacity(rows.len());
    for (key, raw) in rows {
        let index: Value = serde_json::from_str(&raw)?;
        let client_id = model::text(&index, "client_id")?;
        let registration = participant_registration(tx, client_id)?;
        if key
            != keys::pending_review_profile_key(
                &pending_scope,
                sponsor_client_id,
                profile,
                client_id,
            )?
            || index["sponsor_client_id"] != sponsor_client_id
            || index["review_profile"] != profile
            || index["review_scope"] != pending_scope
            || registration["disabled"] == true
            || registration["review_sponsor_client_id"] != sponsor_client_id
            || registration["review_profile"] != profile
            || registration["participation_basis"]["kind"] != "sponsored_reviewer"
            || registration["participation_basis"]["review_scope"] != pending_scope
        {
            return Err(Error::new(
                "REVIEW_PROFILE_INDEX_DAMAGED",
                "pending reviewer profile index differs from its exact registration",
            ));
        }
        clients.push(client_id.to_owned());
    }
    Ok(clients)
}

/// Bind a pre-registered sponsored reviewer to the server-generated slot ID.
/// Review assignment creation and this update must share one SQLite
/// transaction. The pending credential cannot perform any participant call.
pub(crate) fn bind_review_assignment(
    tx: &Transaction<'_>,
    reviewer_client_id: &str,
    review_assignment_id: &str,
    sponsor_client_id: &str,
    exact_scope: &Value,
) -> Result<()> {
    let mut scope = normalized_review_scope(exact_scope, true)?;
    if review_assignment_id.trim().is_empty()
        || review_assignment_id.len() > 128
        || review_assignment_id
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(Error::invalid(
            "review_assignment_id must be 1..=128 bytes without whitespace",
        ));
    }
    if scope["review_assignment_id"] != Value::Null {
        return Err(Error::conflict(
            "review scope is already bound to a review assignment",
        ));
    }
    let key = format!("client:{reviewer_client_id}");
    let mut registration = meta(tx, &key)?
        .ok_or_else(|| Error::new("NOT_FOUND", "sponsored reviewer is not registered"))?;
    if registration["role"] != "participant"
        || registration["disabled"] == true
        || registration["participation_basis"]["kind"] != "sponsored_reviewer"
        || registration["review_sponsor_client_id"] != sponsor_client_id
        || registration["participation_basis"]["review_scope"] != scope
    {
        return Err(Error::new(
            "FORBIDDEN",
            "participant registration does not match this review sponsor and slot",
        ));
    }
    validate_pending_review_tuple(tx, &scope)?;
    if let Some(profile) = registration["review_profile"].as_str() {
        tx.execute(
            "DELETE FROM meta WHERE key=?1",
            [keys::pending_review_profile_key(
                &scope,
                sponsor_client_id,
                profile,
                reviewer_client_id,
            )?],
        )?;
    }
    scope["review_assignment_id"] = json!(review_assignment_id);
    registration["participation_basis"]["review_scope"] = scope;
    set_meta(tx, &key, &registration)
}

/// Live assigned-review authority, suitable for packet/context reads and
/// ordinary review actions. It requires the Task revision and Attempt to
/// remain current.
pub(crate) fn require_review_scope(
    db: &Connection,
    principal: &Principal,
    review_assignment_id: &str,
    exact_scope: &Value,
) -> Result<()> {
    let scope = normalized_review_scope(exact_scope, false)?;
    if scope["review_assignment_id"] != review_assignment_id {
        return Err(Error::new("FORBIDDEN", "review assignment scope mismatch"));
    }
    let current = load_current_scope(db, principal)?;
    require_sponsored_scope(&current, &scope)?;
    verify_review_assignment(
        db,
        principal.client_id.as_str(),
        &current.registration,
        &scope,
    )
}

/// Narrow late-result authority for an already assigned reviewer. This helper
/// deliberately skips only current Task/Attempt lifecycle equality; the exact
/// retained tuple, enabled credential, sponsor and assignment evidence remain
/// mandatory. Call it only from the exact assigned-slot result path.
pub(crate) fn require_historical_review_result_scope(
    db: &Connection,
    principal: &Principal,
    review_assignment_id: &str,
    exact_scope: &Value,
) -> Result<()> {
    principal.require_participant()?;
    let scope = normalized_review_scope(exact_scope, false)?;
    if scope["review_assignment_id"] != review_assignment_id {
        return Err(Error::new("FORBIDDEN", "review assignment scope mismatch"));
    }
    let registration = participant_registration(db, &principal.client_id)?;
    if registration["disabled"] == true {
        return Err(Error::new(
            "UNAUTHORIZED",
            "participant credential disabled",
        ));
    }
    if registration["participation_basis"]["kind"] != "sponsored_reviewer"
        || registration["participation_basis"]["review_scope"] != scope
    {
        return Err(Error::new(
            "FORBIDDEN",
            "participant is not assigned to this exact review slot",
        ));
    }
    validate_retained_review_tuple(db, &scope)?;
    verify_review_assignment(db, &principal.client_id, &registration, &scope)
}

/// Canonical typed message request -> existing raw mailbox request. The caller
/// must invoke this inside the message batch's transaction, then pass the
/// result to the existing `message.send` apply branch. That preserves the
/// single Operation, receipt, digest and durable mailbox primitive.
pub(crate) fn normalize_send(
    db: &Connection,
    principal: &Principal,
    value: &Value,
) -> Result<Value> {
    principal.require_participant()?;
    model::fields(value, &["client_request_id", "recipient", "body"])?;
    model::text(value, "client_request_id")?;
    let sender = load_current_scope(db, principal)?;
    let recipient = model::text(value, "recipient")?;
    if recipient == principal.client_id {
        return Err(Error::invalid("coordination.send cannot target the sender"));
    }
    let recipient_registration = participant_registration(db, recipient)?;
    if recipient_registration["disabled"] == true {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "recipient is no longer active",
        ));
    }
    if recipient_registration["inbound_policy"] == "refuse" {
        return Err(Error::new(
            "DELIVERY_REFUSED",
            "recipient inbound policy refuses coordination delivery",
        ));
    }
    let target = load_current_scope_for_client(db, recipient)?;
    if target.scope_id != sender.scope_id {
        return Err(Error::new(
            "FORBIDDEN",
            "coordination delivery must target a participant in the exact Task/Attempt scope",
        ));
    }
    let body = value
        .get("body")
        .filter(|body| !body.is_null())
        .ok_or_else(|| Error::invalid("body must be a non-null JSON value"))?;
    let envelope = json!({
        "schema": "eliot.coordination.message.v1",
        "task_id": sender.task["task_id"],
        "task_revision": sender.task["revision"],
        "attempt_id": sender.attempt["attempt_id"],
        "sender": principal.client_id,
        "recipient": recipient,
        "body": body,
    });
    let text = model::canonical(&envelope)?;
    if text.len() > keys::MAX_MESSAGE_BYTES {
        return Err(Error::invalid(format!(
            "coordination message exceeds the {}-byte limit",
            keys::MAX_MESSAGE_BYTES
        )));
    }
    Ok(json!({
        "client_request_id": value["client_request_id"],
        "recipient": recipient,
        "text": text,
    }))
}

/// Dedicated participant read dispatch. Store-level routing must call this
/// only after the current authenticated principal has been reloaded.
pub(super) fn read(
    db: &Connection,
    principal: &Principal,
    method: &str,
    value: &Value,
) -> Result<Value> {
    match method {
        "swarm.context.get" => context_get(db, principal, value),
        "coordination.participant.get" => participant_get(db, principal, value),
        "coordination.participant.list" => participant_list(db, principal, value),
        "coordination.peer.find" => peer_find(db, principal, value),
        "coordination.work_card.get" => card_get(db, principal, "work", value),
        "coordination.work_card.list" => card_list(db, principal, "work", value),
        "coordination.contract_card.get" => card_get(db, principal, "contract", value),
        "coordination.contract_card.list" => card_list(db, principal, "contract", value),
        "coordination.contract.get" => contract_get(db, principal, value),
        "coordination.contract.list" => contract_list(db, principal, value),
        "coordination.inbox" => inbox(db, principal, value),
        "operation.get" => participant_operation_get(db, principal, value),
        _ => Err(Error::new(
            "METHOD_NOT_FOUND",
            format!("{method} is not an implemented coordination read"),
        )),
    }
}

/// Dedicated coordination mutations. Normal Store receipt handling owns the
/// outer Operation/Observation; every helper below writes only inside that
/// transaction and reports no native/queued effect.
pub(super) fn apply(
    tx: &Transaction<'_>,
    principal: &Principal,
    method: &str,
    value: &Value,
    config: &Config,
    operation_id: &str,
    now: i64,
) -> Result<(Value, bool)> {
    match method {
        "coordination.participant.register" => {
            register_participant(tx, principal, value, operation_id, now)
                .map(|value| (value, false))
        }
        "coordination.participant.disable" => {
            disable_participant(tx, principal, value, operation_id, now).map(|value| (value, false))
        }
        "coordination.work_card.publish" => {
            publish_card(tx, principal, "work", value, operation_id, now)
                .map(|value| (value, false))
        }
        "coordination.work_card.withdraw" => {
            withdraw_card(tx, principal, "work", value, operation_id, now)
                .map(|value| (value, false))
        }
        "coordination.contract_card.publish" => {
            publish_card(tx, principal, "contract", value, operation_id, now)
                .map(|value| (value, false))
        }
        "coordination.contract_card.withdraw" => {
            withdraw_card(tx, principal, "contract", value, operation_id, now)
                .map(|value| (value, false))
        }
        "coordination.contract.propose" => {
            propose_contract(tx, principal, value, operation_id, now).map(|value| (value, false))
        }
        "coordination.contract.respond" => {
            respond_contract(tx, principal, value, operation_id, now).map(|value| (value, false))
        }
        "coordination.send" => {
            send(tx, principal, value, config, operation_id, now).map(|value| (value, false))
        }
        "coordination.consult" => {
            consult(tx, principal, value, config, operation_id, now).map(|value| (value, false))
        }
        _ => Err(Error::new(
            "METHOD_NOT_FOUND",
            format!("{method} is not an implemented coordination mutation"),
        )),
    }
}

fn participant_registration(db: &Connection, client_id: &str) -> Result<Value> {
    let registration = meta(db, &format!("client:{client_id}"))?
        .ok_or_else(|| Error::new("NOT_FOUND", "participant is not registered"))?;
    if registration["role"] != "participant" {
        return Err(Error::new(
            "FORBIDDEN",
            "target identity is not a coordination participant",
        ));
    }
    Ok(registration)
}

fn load_current_scope(db: &Connection, principal: &Principal) -> Result<ScopeData> {
    principal.require_participant()?;
    load_current_scope_for_client(db, &principal.client_id)
}

fn participant_scope_lookup_error(error: Error, missing_message: &'static str) -> Error {
    if error.code == "NOT_FOUND" {
        Error::new("STALE_PARTICIPANT", missing_message)
    } else {
        // Database, decoding and invariant failures are not evidence that the
        // participant merely became stale. Preserve the original failure.
        error
    }
}

fn load_current_scope_for_client(db: &Connection, client_id: &str) -> Result<ScopeData> {
    let registration = participant_registration(db, client_id)?;
    if registration["disabled"] == true {
        return Err(Error::new(
            "UNAUTHORIZED",
            "participant credential disabled",
        ));
    }
    let task_id = required_registration_text(&registration, "task_id")?;
    let attempt_id = required_registration_text(&registration, "attempt_id")?;
    let task_revision = registration["task_revision"]
        .as_i64()
        .filter(|revision| *revision > 0)
        .ok_or_else(|| {
            Error::new(
                "STALE_PARTICIPANT",
                "participant has no valid Task revision",
            )
        })?;
    let task = tasks::get_task(db, task_id).map_err(|error| {
        participant_scope_lookup_error(error, "participant Task no longer exists")
    })?;
    if task["revision"] != task_revision
        || task["current_attempt_id"] != attempt_id
        || task["state"] != "open"
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "participant Task revision or current Attempt changed",
        ));
    }
    let attempt = tasks::get_attempt(db, attempt_id).map_err(|error| {
        participant_scope_lookup_error(error, "participant Attempt no longer exists")
    })?;
    validate_current_attempt(&task, &attempt, task_id, task_revision, attempt_id)?;
    validate_registration_binding(&registration, &attempt)?;
    validate_participation_basis(db, client_id, &registration, &task, &attempt, false)?;
    let scope_id = keys::scope_id(task_id, task_revision, attempt_id)?;
    Ok(ScopeData {
        client_id: Some(client_id.to_owned()),
        registration,
        task,
        attempt,
        scope_id,
    })
}

fn validate_current_attempt(
    task: &Value,
    attempt: &Value,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
) -> Result<()> {
    if attempt["attempt_id"] != attempt_id
        || attempt["task_id"] != task_id
        || attempt["task_revision"] != task_revision
        || !attempt["released_at_ms"].is_null()
        || !LIVE_ATTEMPT_STATES.contains(&attempt["state"].as_str().unwrap_or(""))
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "participant Attempt is no longer current and writable",
        ));
    }
    if task["current_attempt_id"] != attempt_id || task["revision"] != task_revision {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "participant Attempt no longer matches the current Task revision",
        ));
    }
    Ok(())
}

fn validate_registration_binding(registration: &Value, attempt: &Value) -> Result<()> {
    let binding_id = registration.get("binding_id").unwrap_or(&Value::Null);
    let generation = registration
        .get("binding_generation")
        .unwrap_or(&Value::Null);
    let attempt_binding_id = attempt.get("binding_id").unwrap_or(&Value::Null);
    let attempt_generation = attempt.get("binding_generation").unwrap_or(&Value::Null);
    if binding_id.is_null() != generation.is_null() {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "participant binding identity is incomplete",
        ));
    }
    if attempt_binding_id.is_null() != attempt_generation.is_null()
        || binding_id != attempt_binding_id
        || generation != attempt_generation
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "participant binding generation no longer matches the Attempt",
        ));
    }
    if let Some(session_id) = registration
        .get("native_session_id")
        .and_then(Value::as_str)
    {
        let present = attempt["producers"].as_array().is_some_and(|producers| {
            producers.iter().any(|producer| {
                producer["native_session_id"] == session_id
                    && !matches!(
                        producer["disposition"].as_str(),
                        Some("completed" | "failed" | "cancelled")
                    )
            })
        });
        if !present {
            return Err(Error::new(
                "STALE_PARTICIPANT",
                "participant native session is no longer a current Attempt producer",
            ));
        }
    }
    Ok(())
}

fn validate_participation_basis(
    db: &Connection,
    client_id: &str,
    registration: &Value,
    task: &Value,
    attempt: &Value,
    historical_review: bool,
) -> Result<()> {
    let basis = &registration["participation_basis"];
    match basis["kind"].as_str() {
        Some("attempt_owner") => {
            if basis
                .get("assignment_id")
                .is_some_and(|value| !value.is_null())
                || basis
                    .get("review_scope")
                    .is_some_and(|value| !value.is_null())
                || registration["created_by"] != attempt["owner_id"]
            {
                return Err(Error::new(
                    "STALE_PARTICIPANT",
                    "attempt-owner participation basis no longer matches the Attempt owner",
                ));
            }
        }
        Some("producer_ref") => {
            if basis
                .get("review_scope")
                .is_some_and(|value| !value.is_null())
            {
                return Err(Error::new(
                    "STALE_PARTICIPANT",
                    "producer participation basis contains review scope",
                ));
            }
            let assignment_id = basis
                .get("assignment_id")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| Error::new("STALE_PARTICIPANT", "producer assignment is missing"))?;
            let producer = matching_producer(attempt, assignment_id).ok_or_else(|| {
                Error::new(
                    "STALE_PARTICIPANT",
                    "producer assignment is no longer present on the Attempt",
                )
            })?;
            if matches!(
                producer["disposition"].as_str(),
                Some("completed" | "failed" | "cancelled")
            ) {
                return Err(Error::new(
                    "STALE_PARTICIPANT",
                    "producer assignment is already terminal",
                ));
            }
            if let Some(session_id) = registration["native_session_id"].as_str()
                && producer["native_session_id"] != session_id
            {
                return Err(Error::new(
                    "STALE_PARTICIPANT",
                    "producer reference changed its native session identity",
                ));
            }
        }
        Some("sponsored_reviewer") => {
            if basis
                .get("assignment_id")
                .is_some_and(|value| !value.is_null())
            {
                return Err(Error::new(
                    "STALE_PARTICIPANT",
                    "sponsored review uses review_scope, not a producer assignment",
                ));
            }
            let scope = normalized_review_scope(
                basis
                    .get("review_scope")
                    .ok_or_else(|| Error::new("STALE_PARTICIPANT", "review scope is missing"))?,
                historical_review,
            )?;
            if scope["task_id"] != task["task_id"]
                || scope["attempt_id"] != attempt["attempt_id"]
                || scope["task_revision"] != task["revision"]
                || scope["submission_ref"] != attempt["submission_ref"]
                || scope["candidate_ref"] != attempt["candidate_ref"]
            {
                return Err(Error::new(
                    "STALE_PARTICIPANT",
                    "sponsored review tuple no longer matches the Attempt candidate",
                ));
            }
            if scope["review_assignment_id"].is_null() {
                return Err(Error::new(
                    "PARTICIPANT_NOT_ASSIGNED",
                    "sponsored reviewer is waiting for an exact review assignment",
                ));
            }
            if historical_review {
                validate_retained_review_tuple(db, &scope)?;
            } else {
                verify_review_assignment(db, client_id, registration, &scope)?;
            }
        }
        _ => {
            return Err(Error::new(
                "STALE_PARTICIPANT",
                "participant has an unsupported participation basis",
            ));
        }
    }
    Ok(())
}

fn matching_producer<'a>(attempt: &'a Value, assignment_id: &str) -> Option<&'a Value> {
    let matches: Vec<&Value> = attempt["producers"]
        .as_array()?
        .iter()
        .filter(|producer| producer["assignment_id"] == assignment_id)
        .collect();
    if matches.len() == 1 {
        matches.first().copied()
    } else {
        None
    }
}

fn scope_projection(scope: &ScopeData) -> Value {
    let producers: Vec<Value> = scope.attempt["producers"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|producer| {
            json!({
                "assignment_id": producer.get("assignment_id").cloned().unwrap_or(Value::Null),
                "disposition": producer.get("disposition").cloned().unwrap_or(Value::Null),
            })
        })
        .take(100)
        .collect();
    let mut task = json!({
        "task_id": scope.task["task_id"],
        "project_id": scope.task["project_id"],
        "revision": scope.task["revision"],
        "state": scope.task["state"],
        "brief": scope.task["task_brief"],
    });
    if let Some(sources) = scope.task["task_brief"].get("source_index") {
        task["canonical_sources"] = sources.clone();
    }
    let attempt = json!({
        "attempt_id": scope.attempt["attempt_id"],
        "task_revision": scope.attempt["task_revision"],
        "owner_id": scope.attempt["owner_id"],
        "state": scope.attempt["state"],
        "binding_id": scope.attempt["binding_id"],
        "binding_generation": scope.attempt["binding_generation"],
        "submission_ref": scope.attempt["submission_ref"],
        "candidate_ref": scope.attempt["candidate_ref"],
        "producer_refs": producers,
        "producer_refs_truncated": scope.attempt["producers"].as_array().is_some_and(|items| items.len() > 100),
    });
    json!({
        "scope_id": scope.scope_id,
        "participant": public_registration(&scope.registration),
        "task": task,
        "attempt": attempt,
    })
}

fn public_registration(registration: &Value) -> Value {
    let mut public = registration.clone();
    if let Some(object) = public.as_object_mut() {
        for name in [
            "token_hash",
            "native_session_id",
            "created_by",
            "review_sponsor_client_id",
        ] {
            object.remove(name);
        }
    }
    public
}

fn authorize_attempt_manager(
    db: &Connection,
    principal: &Principal,
    attempt: &Value,
) -> Result<()> {
    if principal.role == Role::Operator {
        return super::require_local_operator(db, &principal.client_id);
    }
    if principal.role == Role::Manager && attempt["owner_id"] == principal.client_id {
        return Ok(());
    }
    if principal.role == Role::Manager {
        return gm::require_authority(db, principal);
    }
    Err(Error::new(
        "FORBIDDEN",
        "current Attempt owner, local operator, or current GM authority required",
    ))
}

#[derive(Clone, Copy)]
enum ParticipantRegistrationActor<'a> {
    Direct(&'a Principal),
    Launch(&'a LaunchActor),
}

impl ParticipantRegistrationActor<'_> {
    fn effective_owner_id(self) -> String {
        match self {
            Self::Direct(principal) => principal.client_id.clone(),
            Self::Launch(actor) => actor.effective_manager_id().to_owned(),
        }
    }

    fn authorize_attempt(self, db: &Connection, attempt: &Value) -> Result<()> {
        match self {
            Self::Direct(principal) => authorize_attempt_manager(db, principal, attempt),
            Self::Launch(actor) => {
                if let Some(principal) = actor.direct_principal() {
                    authorize_attempt_manager(db, principal, attempt)
                } else {
                    let manager_id = actor.effective_manager_id();
                    let registration =
                        meta(db, &format!("client:{manager_id}"))?.ok_or_else(|| {
                            Error::new("FORBIDDEN", "effective Manager is no longer registered")
                        })?;
                    if actor.role() != Role::Manager
                        || registration["disabled"] == true
                        || registration["role"] != "manager"
                        || attempt["owner_id"].as_str() != Some(manager_id)
                    {
                        return Err(Error::new(
                            "FORBIDDEN",
                            "on-behalf Participant must be issued by the current owning Manager",
                        ));
                    }
                    Ok(())
                }
            }
        }
    }

    fn normalize_basis(
        self,
        db: &Connection,
        value: Option<&Value>,
        task: &Value,
        attempt: &Value,
    ) -> Result<Value> {
        match self {
            Self::Direct(principal) => normalize_basis(db, value, principal, task, attempt),
            Self::Launch(actor) => {
                let basis =
                    value.ok_or_else(|| Error::invalid("participation_basis is required"))?;
                model::fields(basis, &["kind", "assignment_id", "review_scope"])?;
                if model::text(basis, "kind")? != "attempt_owner"
                    || basis
                        .get("assignment_id")
                        .is_some_and(|value| !value.is_null())
                    || basis
                        .get("review_scope")
                        .is_some_and(|value| !value.is_null())
                    || actor.effective_manager_id()
                        != attempt["owner_id"].as_str().unwrap_or_default()
                {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "launch issuance requires exact owning-Manager Attempt participation",
                    ));
                }
                Ok(json!({
                    "kind":"attempt_owner",
                    "assignment_id":null,
                    "review_scope":null
                }))
            }
        }
    }
}

fn manager_scope(
    db: &Connection,
    principal: &Principal,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
) -> Result<ScopeData> {
    let task = tasks::get_task(db, task_id)?;
    if task["revision"] != task_revision
        || task["state"] != "open"
        || task["current_attempt_id"] != attempt_id
    {
        return Err(Error::new(
            "STALE_REVISION",
            "requested Task/Attempt scope is not current",
        ));
    }
    let attempt = tasks::get_attempt(db, attempt_id)?;
    validate_current_attempt(&task, &attempt, task_id, task_revision, attempt_id)?;
    authorize_attempt_manager(db, principal, &attempt)?;
    Ok(ScopeData {
        client_id: None,
        registration: Value::Null,
        task,
        attempt,
        scope_id: keys::scope_id(task_id, task_revision, attempt_id)?,
    })
}

/// Resolve and validate the current assignment grant used by a Concilium
/// participant. The fingerprint commits only to non-secret authority fields.
pub(super) fn concilium_participant_scope_for_client(
    db: &Connection,
    client_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
) -> Result<Value> {
    let scope = load_current_scope_for_client(db, client_id)?;
    if scope.task["task_id"] != task_id
        || scope.task["revision"] != task_revision
        || scope.attempt["attempt_id"] != attempt_id
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "Concilium participant must hold the exact current Task revision and Attempt",
        ));
    }
    participant_authority_projection(&scope)
}

/// Private exact Participant authority used by code-scope and Concilium.
/// Public context projections remain redacted and retain their existing wire shape.
pub(super) fn participant_authority_scope(db: &Connection, principal: &Principal) -> Result<Value> {
    principal.require_participant()?;
    let scope = load_current_scope(db, principal)?;
    participant_authority_projection(&scope)
}

/// Resolve a caller's current Participant grant for a Concilium proposal or
/// position write. This helper never returns credential material.
pub(super) fn concilium_current_participant_scope(
    db: &Connection,
    principal: &Principal,
) -> Result<Value> {
    participant_authority_scope(db, principal)
}

/// Concilium identity commits to the verified key/Principal client ID. The
/// stored registration intentionally need not duplicate that ID. Thread uses
/// a different versioned preimage and is not routed through this helper.
pub(super) fn concilium_registration_fingerprint(
    client_id: &str,
    registration: &Value,
) -> Result<String> {
    if client_id.is_empty() {
        return Err(Error::new(
            "STORE_INVARIANT",
            "Participant authority has no verified client identity",
        ));
    }
    if registration
        .get("client_id")
        .is_some_and(|retained| !retained.is_null() && retained.as_str() != Some(client_id))
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "Participant registration client identity differs from its verified key",
        ));
    }
    let authority = json!({
        "client_id":client_id,
        "role":registration.get("role").cloned().unwrap_or(Value::Null),
        "disabled":registration.get("disabled").cloned().unwrap_or(Value::Null),
        "task_id":registration.get("task_id").cloned().unwrap_or(Value::Null),
        "task_revision":registration.get("task_revision").cloned().unwrap_or(Value::Null),
        "attempt_id":registration.get("attempt_id").cloned().unwrap_or(Value::Null),
        "participation_basis":registration.get("participation_basis").cloned().unwrap_or(Value::Null),
        "binding_id":registration.get("binding_id").cloned().unwrap_or(Value::Null),
        "binding_generation":registration.get("binding_generation").cloned().unwrap_or(Value::Null),
    });
    Ok(format!(
        "sha256:{}",
        model::digest(model::canonical(&authority)?.as_bytes())
    ))
}

fn participant_authority_projection(scope: &ScopeData) -> Result<Value> {
    let registration = &scope.registration;
    let client_id = scope.client_id.as_deref().ok_or_else(|| {
        Error::new(
            "STORE_INVARIANT",
            "Participant authority projection requires a verified client identity",
        )
    })?;
    let generation = registration
        .get("binding_generation")
        .cloned()
        .unwrap_or(Value::Null);
    let fingerprint = concilium_registration_fingerprint(client_id, registration)?;
    Ok(json!({
        "actor":{"client_id":client_id,"role":"participant","generation":generation},
        "scope":{
            "scope_id":scope.scope_id,
            "task_id":scope.task["task_id"],
            "task_revision":scope.task["revision"],
            "attempt_id":scope.attempt["attempt_id"],
            "binding_id":registration.get("binding_id").cloned().unwrap_or(Value::Null),
            "binding_generation":generation,
        },
        "participation_basis":registration.get("participation_basis").cloned().unwrap_or(Value::Null),
        "registration_fingerprint":fingerprint,
    }))
}

/// Validate a manager's current authority over an exact Task and Attempt and
/// return only the context needed to build a deterministic Concilium preview.
pub(super) fn concilium_manager_scope(
    db: &Connection,
    principal: &Principal,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
) -> Result<Value> {
    let scope = manager_scope(db, principal, task_id, task_revision, attempt_id)?;
    let attempt_digest = format!(
        "sha256:{}",
        model::digest(model::canonical(&scope.attempt)?.as_bytes())
    );
    Ok(json!({
        "task":{
            "task_id":scope.task["task_id"],
            "project_id":scope.task["project_id"],
            "revision":scope.task["revision"],
            "state":scope.task["state"],
            "brief":scope.task["task_brief"],
        },
        "attempt":{
            "attempt_id":scope.attempt["attempt_id"],
            "task_revision":scope.attempt["task_revision"],
            "owner_id":scope.attempt["owner_id"],
            "state":scope.attempt["state"],
            "binding_id":scope.attempt["binding_id"],
            "binding_generation":scope.attempt["binding_generation"],
            "digest":attempt_digest,
        },
        "scope_id":scope.scope_id,
    }))
}

fn register_participant(
    tx: &Transaction<'_>,
    principal: &Principal,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    register_participant_with_actor(
        tx,
        ParticipantRegistrationActor::Direct(principal),
        value,
        operation_id,
        now,
    )
}

/// Launch-only enrollment adapter. The outer Store launch-child path validates
/// the typed actor and exact held launch scope in this same transaction; this
/// shared core preserves technical caller identity while recording the
/// effective manager as the Participant creator.
pub(crate) fn register_participant_for_launch(
    tx: &Transaction<'_>,
    actor: &LaunchActor,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    register_participant_with_actor(
        tx,
        ParticipantRegistrationActor::Launch(actor),
        value,
        operation_id,
        now,
    )
}

fn register_participant_with_actor(
    tx: &Transaction<'_>,
    authority: ParticipantRegistrationActor<'_>,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    model::fields(
        value,
        &[
            "client_request_id",
            "client_id",
            "token_hash",
            "task_id",
            "task_revision",
            "attempt_id",
            "participation_basis",
            "binding_id",
            "binding_generation",
            "native_session_id",
            "display_alias",
            "inbound_policy",
            "review_profile",
        ],
    )?;
    let client_id = model::text(value, "client_id")?;
    validate_identifier(client_id, "client_id", 128)?;
    let token_hash = model::text(value, "token_hash")?;
    if token_hash.len() != 64 || !token_hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::invalid("token_hash must be SHA-256 hex"));
    }
    if meta(tx, &format!("client:{client_id}"))?.is_some() {
        return Err(Error::conflict(
            "client already registered; no implicit credential rotation",
        ));
    }
    let task_id = model::text(value, "task_id")?;
    let task_revision = model::positive(value, "task_revision")?;
    let attempt_id = model::text(value, "attempt_id")?;
    let task = tasks::get_task(tx, task_id)?;
    if task["revision"] != task_revision
        || task["state"] != "open"
        || task["current_attempt_id"] != attempt_id
    {
        return Err(Error::new(
            "STALE_REVISION",
            "participant registration requires the exact current Task revision and Attempt",
        ));
    }
    let attempt = tasks::get_attempt(tx, attempt_id)?;
    validate_current_attempt(&task, &attempt, task_id, task_revision, attempt_id)?;
    authority.authorize_attempt(tx, &attempt)?;

    let basis = authority.normalize_basis(tx, value.get("participation_basis"), &task, &attempt)?;
    let requested_binding_id = optional_nonempty_text(value.get("binding_id"), "binding_id")?;
    let requested_binding_generation =
        optional_positive_value(value.get("binding_generation"), "binding_generation")?;
    if requested_binding_id.is_some() != requested_binding_generation.is_some() {
        return Err(Error::invalid(
            "binding_id and binding_generation must be supplied together",
        ));
    }
    let binding_id = optional_nonempty_text(attempt.get("binding_id"), "Attempt binding_id")?;
    let binding_generation = optional_positive_value(
        attempt.get("binding_generation"),
        "Attempt binding_generation",
    )?;
    if binding_id.is_some() != binding_generation.is_some() {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "current Attempt binding identity is incomplete",
        ));
    }
    if let (Some(requested_id), Some(requested_generation)) = (
        requested_binding_id.as_deref(),
        requested_binding_generation,
    ) && (binding_id.as_deref() != Some(requested_id)
        || binding_generation != Some(requested_generation))
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "participant binding must match the current Attempt binding generation",
        ));
    }
    let native_session_id =
        optional_nonempty_text(value.get("native_session_id"), "native_session_id")?;
    if let Some(session_id) = native_session_id.as_deref() {
        let producer_ok = match basis["kind"].as_str() {
            Some("producer_ref") => matching_producer(
                &attempt,
                basis["assignment_id"].as_str().unwrap_or_default(),
            )
            .is_some_and(|producer| producer["native_session_id"] == session_id),
            _ => attempt["producers"].as_array().is_some_and(|producers| {
                producers.iter().any(|producer| {
                    producer["native_session_id"] == session_id
                        && !matches!(
                            producer["disposition"].as_str(),
                            Some("completed" | "failed" | "cancelled")
                        )
                })
            }),
        };
        if !producer_ok {
            return Err(Error::new(
                "STALE_PARTICIPANT",
                "native_session_id does not identify a current producer on this Attempt",
            ));
        }
    }
    let alias = value
        .get("display_alias")
        .and_then(Value::as_str)
        .unwrap_or(client_id);
    validate_identifier(alias, "display_alias", 128)?;
    let policy = value
        .get("inbound_policy")
        .and_then(Value::as_str)
        .unwrap_or("pull_only");
    if !INBOUND_POLICIES.contains(&policy) {
        return Err(Error::invalid(
            "inbound_policy must be pull_only, safe_boundary, hold, or refuse",
        ));
    }
    let review_profile = optional_nonempty_text(value.get("review_profile"), "review_profile")?;
    if review_profile
        .as_deref()
        .is_some_and(|profile| profile.len() > 128)
    {
        return Err(Error::invalid(
            "review_profile may contain at most 128 bytes",
        ));
    }
    if review_profile.is_some() && basis["kind"] != "sponsored_reviewer" {
        return Err(Error::invalid(
            "review_profile is only valid for sponsored reviewers",
        ));
    }
    let scope_id = keys::scope_id(task_id, task_revision, attempt_id)?;
    let effective_owner_id = authority.effective_owner_id();
    let registration = json!({
        "role": "participant",
        "token_hash": token_hash.to_lowercase(),
        "disabled": false,
        "task_id": task_id,
        "task_revision": task_revision,
        "attempt_id": attempt_id,
        "participation_basis": basis,
        "binding_id": binding_id,
        "binding_generation": binding_generation,
        "native_session_id": native_session_id,
        "display_alias": alias,
        "inbound_policy": policy,
        "grant_revision": 1,
        "created_by": effective_owner_id,
        "created_operation_id": operation_id,
        "created_at_ms": now,
        "review_sponsor_client_id": if review_profile.is_some() || basis["kind"] == "sponsored_reviewer" { json!(effective_owner_id) } else { Value::Null },
        "review_profile": review_profile,
    });
    set_meta(tx, &format!("client:{client_id}"), &registration)?;
    set_meta(
        tx,
        &keys::participant_key(&scope_id, client_id),
        &json!({"client_id":client_id,"grant_revision":1}),
    )?;
    if basis["kind"] == "sponsored_reviewer"
        && let Some(profile) = review_profile.as_deref()
    {
        let review_scope = &basis["review_scope"];
        let sponsor_client_id = effective_owner_id.as_str();
        set_meta(
            tx,
            &keys::pending_review_profile_key(review_scope, sponsor_client_id, profile, client_id)?,
            &json!({
                "client_id":client_id,
                "sponsor_client_id":sponsor_client_id,
                "review_profile":profile,
                "review_scope":review_scope,
            }),
        )?;
    }
    attach_operation_scope(tx, operation_id, task_id, attempt_id, &attempt)?;
    Ok(json!({
        "operation_id": operation_id,
        "client_id": client_id,
        "role": "participant",
        "task_id": task_id,
        "task_revision": task_revision,
        "attempt_id": attempt_id,
        "participation_basis": basis,
        "grant_revision": 1,
        "inbound_policy": policy,
        "review_profile": review_profile,
        "usable": basis["kind"] != "sponsored_reviewer",
    }))
}

fn normalize_basis(
    db: &Connection,
    value: Option<&Value>,
    principal: &Principal,
    task: &Value,
    attempt: &Value,
) -> Result<Value> {
    let basis = value.ok_or_else(|| Error::invalid("participation_basis is required"))?;
    model::fields(basis, &["kind", "assignment_id", "review_scope"])?;
    match model::text(basis, "kind")? {
        "attempt_owner" => {
            if basis
                .get("assignment_id")
                .is_some_and(|value| !value.is_null())
                || basis
                    .get("review_scope")
                    .is_some_and(|value| !value.is_null())
                || principal.client_id != attempt["owner_id"].as_str().unwrap_or_default()
            {
                return Err(Error::new(
                    "FORBIDDEN",
                    "attempt_owner participation must be sponsored by the exact Attempt owner",
                ));
            }
            Ok(json!({"kind":"attempt_owner","assignment_id":null,"review_scope":null}))
        }
        "producer_ref" => {
            if basis
                .get("review_scope")
                .is_some_and(|value| !value.is_null())
            {
                return Err(Error::invalid(
                    "producer_ref participation cannot carry review_scope",
                ));
            }
            let assignment_id = model::text(basis, "assignment_id")?;
            let producer = matching_producer(attempt, assignment_id).ok_or_else(|| {
                Error::new(
                    "NOT_FOUND",
                    "assignment_id is not one exact ProducerRef on the current Attempt",
                )
            })?;
            if matches!(
                producer["disposition"].as_str(),
                Some("completed" | "failed" | "cancelled")
            ) {
                return Err(Error::new(
                    "STALE_PARTICIPANT",
                    "terminal ProducerRef cannot sponsor a current participant",
                ));
            }
            Ok(json!({
                "kind":"producer_ref",
                "assignment_id":assignment_id,
                "review_scope":null,
            }))
        }
        "sponsored_reviewer" => {
            if basis
                .get("assignment_id")
                .is_some_and(|value| !value.is_null())
            {
                return Err(Error::invalid(
                    "sponsored_reviewer uses review_scope and cannot name a ProducerRef",
                ));
            }
            let scope = normalized_review_scope(
                basis
                    .get("review_scope")
                    .ok_or_else(|| Error::invalid("sponsored_reviewer requires review_scope"))?,
                true,
            )?;
            if scope["task_id"] != task["task_id"]
                || scope["attempt_id"] != attempt["attempt_id"]
                || scope["task_revision"] != task["revision"]
                || scope["submission_ref"] != attempt["submission_ref"]
                || scope["candidate_ref"] != attempt["candidate_ref"]
            {
                return Err(Error::new(
                    "STALE_REVISION",
                    "review scope must identify the exact current submitted candidate",
                ));
            }
            if attempt["state"] != "submitted"
                || scope["submission_ref"].is_null()
                || scope["candidate_ref"].is_null()
            {
                return Err(Error::new(
                    "STALE_REVISION",
                    "sponsored review requires a submitted Attempt with retained candidate refs",
                ));
            }
            validate_pending_review_tuple(db, &scope)?;
            Ok(json!({
                "kind":"sponsored_reviewer",
                "assignment_id":null,
                "review_scope":scope,
            }))
        }
        _ => Err(Error::invalid(
            "participation_basis.kind must be attempt_owner, producer_ref, or sponsored_reviewer",
        )),
    }
}

fn disable_participant(
    tx: &Transaction<'_>,
    principal: &Principal,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    model::fields(
        value,
        &["client_request_id", "client_id", "expected_grant_revision"],
    )?;
    let client_id = model::text(value, "client_id")?;
    let mut registration = participant_registration(tx, client_id)?;
    let task_id = required_registration_text(&registration, "task_id")?.to_owned();
    let attempt_id = required_registration_text(&registration, "attempt_id")?.to_owned();
    let task_revision = registration["task_revision"].as_i64().unwrap_or_default();
    let attempt = tasks::get_attempt(tx, &attempt_id)?;
    if attempt["task_id"] != task_id || attempt["task_revision"] != task_revision {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "participant registration no longer points at its original Attempt",
        ));
    }
    authorize_attempt_manager(tx, principal, &attempt)?;
    if let Some(expected) = value.get("expected_grant_revision") {
        let expected = expected
            .as_i64()
            .filter(|revision| *revision > 0)
            .ok_or_else(|| Error::invalid("expected_grant_revision must be positive"))?;
        if registration["grant_revision"] != expected {
            return Err(Error::new(
                "STALE_REVISION",
                "participant grant revision changed",
            ));
        }
    }
    let changed = registration["disabled"] != true;
    if changed {
        registration["disabled"] = json!(true);
        registration["grant_revision"] = json!(
            registration["grant_revision"]
                .as_i64()
                .unwrap_or(1)
                .saturating_add(1)
        );
        registration["revoked_by"] = json!(principal.client_id);
        registration["revoked_at_ms"] = json!(now);
        registration["revocation_operation_id"] = json!(operation_id);
        set_meta(tx, &format!("client:{client_id}"), &registration)?;
        let scope_id = keys::scope_id(&task_id, task_revision, &attempt_id)?;
        tx.execute(
            "DELETE FROM meta WHERE key=?1",
            [keys::participant_key(&scope_id, client_id)],
        )?;
        if registration["participation_basis"]["kind"] == "sponsored_reviewer"
            && registration["participation_basis"]["review_scope"]["review_assignment_id"].is_null()
            && let Some(profile) = registration["review_profile"].as_str()
        {
            tx.execute(
                "DELETE FROM meta WHERE key=?1",
                [keys::pending_review_profile_key(
                    &registration["participation_basis"]["review_scope"],
                    registration["review_sponsor_client_id"]
                        .as_str()
                        .unwrap_or_default(),
                    profile,
                    client_id,
                )?],
            )?;
        }
    }
    attach_operation_scope(tx, operation_id, &task_id, &attempt_id, &attempt)?;
    Ok(json!({
        "operation_id": operation_id,
        "client_id": client_id,
        "disabled": true,
        "changed": changed,
        "grant_revision": registration["grant_revision"],
        "coordination_history_erased": false,
    }))
}

fn validate_identifier(value: &str, name: &str, max: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(Error::invalid(format!(
            "{name} must be 1..={max} bytes without whitespace"
        )));
    }
    Ok(())
}

fn optional_nonempty_text(value: Option<&Value>, name: &str) -> Result<Option<String>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(_) => Err(Error::invalid(format!(
            "{name} must be nonempty text or null"
        ))),
    }
}

fn optional_positive_value(value: Option<&Value>, name: &str) -> Result<Option<i64>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => number
            .as_i64()
            .filter(|value| *value > 0)
            .map(Some)
            .ok_or_else(|| Error::invalid(format!("{name} must be a positive integer or null"))),
        Some(_) => Err(Error::invalid(format!(
            "{name} must be a positive integer or null"
        ))),
    }
}

fn required_registration_text<'a>(registration: &'a Value, field: &str) -> Result<&'a str> {
    registration
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::new("STALE_PARTICIPANT", format!("registration lacks {field}")))
}

pub(super) fn attach_operation_scope(
    tx: &Transaction<'_>,
    operation_id: &str,
    task_id: &str,
    attempt_id: &str,
    attempt: &Value,
) -> Result<()> {
    tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3,binding_id=?4,binding_generation=?5 WHERE operation_id=?1",
        params![
            operation_id,
            task_id,
            attempt_id,
            attempt.get("binding_id").and_then(Value::as_str),
            attempt.get("binding_generation").and_then(Value::as_i64),
        ],
    )?;
    Ok(())
}

fn normalized_review_scope(value: &Value, allow_pending: bool) -> Result<Value> {
    model::fields(
        value,
        &[
            "review_assignment_id",
            "task_id",
            "attempt_id",
            "task_revision",
            "submission_ref",
            "candidate_ref",
        ],
    )?;
    let assignment = value
        .get("review_assignment_id")
        .cloned()
        .ok_or_else(|| Error::invalid("review_assignment_id must be present as text or null"))?;
    if assignment.is_null() {
        if !allow_pending {
            return Err(Error::new(
                "PARTICIPANT_NOT_ASSIGNED",
                "review scope has not been bound to a review assignment",
            ));
        }
    } else if !assignment
        .as_str()
        .is_some_and(|value| !value.trim().is_empty())
    {
        return Err(Error::invalid(
            "review_assignment_id must be nonempty text or null",
        ));
    }
    let task_id = model::text(value, "task_id")?;
    let attempt_id = model::text(value, "attempt_id")?;
    let task_revision = model::positive(value, "task_revision")?;
    let submission_ref = model::text(value, "submission_ref")?;
    let candidate_ref = model::text(value, "candidate_ref")?;
    Ok(json!({
        "review_assignment_id": assignment,
        "task_id": task_id,
        "attempt_id": attempt_id,
        "task_revision": task_revision,
        "submission_ref": submission_ref,
        "candidate_ref": candidate_ref,
    }))
}

fn require_sponsored_scope(scope: &ScopeData, exact_scope: &Value) -> Result<()> {
    let registration = &scope.registration;
    if registration["participation_basis"]["kind"] != "sponsored_reviewer"
        || registration["participation_basis"]["review_scope"] != *exact_scope
    {
        return Err(Error::new(
            "FORBIDDEN",
            "participant is not assigned to this exact review slot",
        ));
    }
    Ok(())
}

fn validate_pending_review_tuple(db: &Connection, scope: &Value) -> Result<()> {
    if !scope["review_assignment_id"].is_null() {
        return Err(Error::invalid(
            "pre-registration review scope must have a null review_assignment_id",
        ));
    }
    let task_id = model::text(scope, "task_id")?;
    let attempt_id = model::text(scope, "attempt_id")?;
    let task_revision = model::positive(scope, "task_revision")?;
    let task = tasks::get_task(db, task_id)?;
    if task["revision"] != task_revision
        || task["state"] != "open"
        || task["current_attempt_id"] != attempt_id
    {
        return Err(Error::new(
            "STALE_REVISION",
            "sponsored review registration must identify the current Task revision and Attempt",
        ));
    }
    let attempt = tasks::get_attempt(db, attempt_id)?;
    validate_current_attempt(&task, &attempt, task_id, task_revision, attempt_id)?;
    if attempt["state"] != "submitted"
        || attempt["submission_ref"] != scope["submission_ref"]
        || attempt["candidate_ref"] != scope["candidate_ref"]
    {
        return Err(Error::new(
            "STALE_REVISION",
            "sponsored review registration must match the exact submitted candidate",
        ));
    }
    Ok(())
}

fn validate_retained_review_tuple(db: &Connection, scope: &Value) -> Result<()> {
    let task_id = model::text(scope, "task_id")?;
    let attempt_id = model::text(scope, "attempt_id")?;
    let task_revision = model::positive(scope, "task_revision")?;
    // Confirm that the Task still exists, while using the immutable Attempt
    // snapshot/refs rather than its newer current revision.
    tasks::get_task(db, task_id).map_err(|_| {
        Error::new(
            "STALE_REVIEW_ASSIGNMENT",
            "assigned review Task is no longer retained",
        )
    })?;
    let attempt = tasks::get_attempt(db, attempt_id).map_err(|_| {
        Error::new(
            "STALE_REVIEW_ASSIGNMENT",
            "assigned review Attempt is no longer retained",
        )
    })?;
    if attempt["task_id"] != task_id
        || attempt["task_revision"] != task_revision
        || attempt["submission_ref"] != scope["submission_ref"]
        || attempt["candidate_ref"] != scope["candidate_ref"]
    {
        return Err(Error::new(
            "STALE_REVIEW_ASSIGNMENT",
            "retained Attempt no longer identifies the exact reviewed candidate",
        ));
    }
    Ok(())
}

fn verify_review_assignment(
    db: &Connection,
    reviewer_client_id: &str,
    registration: &Value,
    scope: &Value,
) -> Result<()> {
    let assignment_id = model::text(scope, "review_assignment_id")?;
    let event_key = format!("assignment:{assignment_id}");
    let row: Option<(Option<String>, String)> = db
        .query_row(
            "SELECT operation_id,payload_json FROM observations \
             WHERE source_stream_id='controller:review' AND source_event_key=?1 \
               AND kind='review.assignment'",
            [event_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (operation_id, payload_json) = row.ok_or_else(|| {
        Error::new(
            "STALE_REVIEW_ASSIGNMENT",
            "review assignment observation is not retained",
        )
    })?;
    let operation_id = operation_id.ok_or_else(|| {
        Error::new(
            "STALE_REVIEW_ASSIGNMENT",
            "review assignment observation has no Operation link",
        )
    })?;
    let observation: Value = serde_json::from_str(&payload_json)?;
    if !review_record_matches(&observation, reviewer_client_id, registration, scope) {
        return Err(Error::new(
            "STALE_REVIEW_ASSIGNMENT",
            "review assignment observation does not match the participant and exact slot",
        ));
    }
    let identity: crate::review::ReviewSlotIdentity =
        serde_json::from_value(observation["identity"].clone())?;
    let current_slot = meta(db, &format!("review:slot:{}", identity.digest()?))?;
    if !current_slot.is_some_and(|slot| slot["review_assignment_id"] == assignment_id) {
        return Err(Error::new(
            "STALE_REVIEW_ASSIGNMENT",
            "reviewer assignment has been replaced for this exact slot",
        ));
    }
    let row: Option<(String, String, String)> = db
        .query_row(
            "SELECT method,state,result_json FROM operations WHERE operation_id=?1",
            [&operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let (method, state, result_json) = row.ok_or_else(|| {
        Error::new(
            "STALE_REVIEW_ASSIGNMENT",
            "review assignment Operation is not retained",
        )
    })?;
    if method != "review.assign" || state != "settled" {
        return Err(Error::new(
            "STALE_REVIEW_ASSIGNMENT",
            "review assignment Operation is not a settled review.assign",
        ));
    }
    let result: Value = serde_json::from_str(&result_json)?;
    let value = result
        .get("value")
        .filter(|_| result["ok"] == true)
        .unwrap_or(&result);
    if !review_record_matches(value, reviewer_client_id, registration, scope) {
        return Err(Error::new(
            "STALE_REVIEW_ASSIGNMENT",
            "settled review.assign result does not match the participant and exact slot",
        ));
    }
    Ok(())
}

fn review_record_matches(
    record: &Value,
    reviewer_client_id: &str,
    registration: &Value,
    scope: &Value,
) -> bool {
    // Retained assignment observations and review.assign results carry the
    // Task/Attempt tuple under `identity`; the participant grant carries the
    // six-field `review_scope`. Compare the same exact facts across those
    // different public record envelopes.
    let identity = record.get("identity").unwrap_or(record);
    record["review_assignment_id"] == scope["review_assignment_id"]
        && record["reviewer_client_id"] == reviewer_client_id
        && record["sponsor_client_id"] == registration["review_sponsor_client_id"]
        && [
            "task_id",
            "attempt_id",
            "task_revision",
            "submission_ref",
            "candidate_ref",
        ]
        .into_iter()
        .all(|field| identity[field] == scope[field])
}

fn participant_get(db: &Connection, principal: &Principal, value: &Value) -> Result<Value> {
    model::fields(
        value,
        &["client_id", "task_id", "task_revision", "attempt_id"],
    )?;
    let target_id = model::text(value, "client_id")?;
    let expected_scope = if principal.role == Role::Participant {
        let caller = load_current_scope(db, principal)?;
        if value.get("task_id").is_some()
            || value.get("task_revision").is_some()
            || value.get("attempt_id").is_some()
        {
            return Err(Error::invalid(
                "participants query only within their authenticated scope",
            ));
        }
        caller.scope_id
    } else {
        let task_id = model::text(value, "task_id")?;
        let task_revision = model::positive(value, "task_revision")?;
        let attempt_id = model::text(value, "attempt_id")?;
        manager_scope(db, principal, task_id, task_revision, attempt_id)?.scope_id
    };
    let target = load_current_scope_for_client(db, target_id)?;
    if target.scope_id != expected_scope {
        return Err(Error::new(
            "NOT_FOUND",
            "participant is not in the requested current scope",
        ));
    }
    Ok(json!({
        "participant": public_registration(&target.registration),
        "task_id": target.task["task_id"],
        "task_revision": target.task["revision"],
        "attempt_id": target.attempt["attempt_id"],
    }))
}

fn participant_list(db: &Connection, principal: &Principal, value: &Value) -> Result<Value> {
    model::fields(
        value,
        &[
            "task_id",
            "task_revision",
            "attempt_id",
            "limit",
            "after_client_id",
        ],
    )?;
    if principal.role == Role::Participant {
        return Err(Error::new(
            "FORBIDDEN",
            "participants use exact relation discovery instead of a roster",
        ));
    }
    let task_id = model::text(value, "task_id")?;
    let task_revision = model::positive(value, "task_revision")?;
    let attempt_id = model::text(value, "attempt_id")?;
    let limit = keys::parse_page(value.get("limit"), 20)?;
    let after = keys::optional_cursor(value, "after_client_id")?;
    let scope = manager_scope(db, principal, task_id, task_revision, attempt_id)?;
    list_participant_page(db, &scope, limit, after.as_deref())
}

fn lower_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn participant_client_id_from_index_key(prefix: &str, key: &str) -> Result<String> {
    let encoded = key
        .strip_prefix(prefix)
        .filter(|encoded| !encoded.is_empty() && encoded.len().is_multiple_of(2))
        .ok_or_else(|| {
            Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "participant index key is outside its exact scope prefix",
            )
        })?;
    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().as_chunks::<2>().0 {
        let high = lower_hex_nibble(pair[0]).ok_or_else(|| {
            Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "participant index key has a non-canonical client component",
            )
        })?;
        let low = lower_hex_nibble(pair[1]).ok_or_else(|| {
            Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "participant index key has a non-canonical client component",
            )
        })?;
        bytes.push((high << 4) | low);
    }
    let client_id = String::from_utf8(bytes).map_err(|_| {
        Error::new(
            "COORDINATION_INDEX_CORRUPT",
            "participant index key does not encode a UTF-8 client identity",
        )
    })?;
    if client_id.len() > 256 {
        return Err(Error::new(
            "COORDINATION_INDEX_CORRUPT",
            "participant index identity cannot be represented by after_client_id",
        ));
    }
    if keys::key_component(&client_id) != encoded {
        return Err(Error::new(
            "COORDINATION_INDEX_CORRUPT",
            "participant index key is not canonical",
        ));
    }
    Ok(client_id)
}
fn list_participant_page(
    db: &Connection,
    scope: &ScopeData,
    limit: i64,
    after_client_id: Option<&str>,
) -> Result<Value> {
    if !(1..=keys::MAX_PAGE_SIZE).contains(&limit) {
        return Err(Error::invalid(format!(
            "limit must be in 1..={}",
            keys::MAX_PAGE_SIZE
        )));
    }
    let prefix = keys::participant_prefix(&scope.scope_id);
    let upper = format!("{prefix}g");
    let (lower, exclusive) = match after_client_id {
        Some(client_id) => (format!("{prefix}{}", keys::key_component(client_id)), true),
        None => (prefix.clone(), false),
    };
    let comparison = if exclusive { ">" } else { ">=" };
    let sql = format!(
        "SELECT key,value_json FROM meta WHERE key {comparison} ?1 AND key < ?2 ORDER BY key LIMIT ?3"
    );
    let scan_limit = (limit * RELEVANCE_SCAN_FACTOR + 32).min(keys::MAX_INBOX_SCAN);
    let mut statement = db.prepare(&sql)?;
    let rows: Vec<(String, String)> = statement
        .query_map(params![lower, upper, scan_limit + 1], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<std::result::Result<_, _>>()?;
    let has_unscanned = rows.len() as i64 > scan_limit;
    let mut items = Vec::new();
    let mut stale = 0usize;
    let mut last_scanned: Option<String> = None;
    let mut cursor_before_extra: Option<String> = None;
    let mut more_active = false;
    for (key, raw) in rows.iter().take(scan_limit as usize) {
        let indexed_client_id = participant_client_id_from_index_key(&prefix, key)?;
        if let Some(before) = last_scanned.as_ref() {
            cursor_before_extra = Some(before.clone());
        }
        last_scanned = Some(indexed_client_id.clone());
        let record: Value = serde_json::from_str(raw)?;
        let Some(client_id) = record.get("client_id").and_then(Value::as_str) else {
            stale = stale.saturating_add(1);
            continue;
        };
        if client_id != indexed_client_id {
            stale = stale.saturating_add(1);
            continue;
        }
        match load_current_scope_for_client(db, &indexed_client_id) {
            Ok(candidate) if candidate.scope_id == scope.scope_id => {
                let item = json!({
                    "client_id": indexed_client_id,
                    "participant": public_registration(&candidate.registration),
                });
                if items.len() < limit as usize {
                    items.push(item);
                } else {
                    more_active = true;
                    // Resume before the first unreturned active participant.
                    last_scanned = cursor_before_extra;
                    break;
                }
            }
            Ok(_) => stale = stale.saturating_add(1),
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "NOT_FOUND"
                        | "FORBIDDEN"
                        | "UNAUTHORIZED"
                        | "STALE_PARTICIPANT"
                        | "STALE_REVISION"
                        | "PARTICIPANT_NOT_ASSIGNED"
                ) =>
            {
                stale = stale.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
    let partial = more_active || has_unscanned || stale > 0;
    let gaps = [
        (stale > 0).then(|| json!({"kind":"stale_participant_index_entries","count":stale})),
        has_unscanned.then(|| json!({"kind":"participant_page_scan_bound","count":null})),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    Ok(json!({
        "items": items,
        "task_id": scope.task["task_id"],
        "task_revision": scope.task["revision"],
        "attempt_id": scope.attempt["attempt_id"],
        "next_after": if partial { last_scanned } else { None },
        "coverage": if partial { "partial" } else { "complete" },
        "gaps": gaps,
    }))
}

fn publish_card(
    tx: &Transaction<'_>,
    principal: &Principal,
    card_kind: &str,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    let allowed = if card_kind == "work" {
        &["client_request_id", "fields"][..]
    } else {
        &["client_request_id", "contract_key", "fields"][..]
    };
    model::fields(value, allowed)?;
    let scope = load_current_scope(tx, principal)?;
    let identity = if card_kind == "work" {
        "work"
    } else {
        let key = model::text(value, "contract_key")?;
        validate_identifier(key, "contract_key", 256)?;
        key
    };
    let fields = value
        .get("fields")
        .ok_or_else(|| Error::invalid("fields is required"))?;
    keys::validate_card_fields(card_kind, fields)?;
    let digest = model::digest(model::canonical(fields)?.as_bytes());
    let current_key = keys::card_key(&scope.scope_id, card_kind, identity, &principal.client_id);
    let prior = meta(tx, &current_key)?;
    if let Some(prior) = prior.as_ref()
        && prior["state"] == "current"
        && prior["material_digest"] == digest
    {
        attach_operation_scope(
            tx,
            operation_id,
            scope.task["task_id"].as_str().unwrap_or_default(),
            scope.attempt["attempt_id"].as_str().unwrap_or_default(),
            &scope.attempt,
        )?;
        return Ok(json!({
            "operation_id": operation_id,
            "card_kind": card_kind,
            "identity": identity,
            "card_revision": prior["card_revision"],
            "material_digest": digest,
            "changed": false,
            "coalesced": true,
        }));
    }
    if let Some(prior) = prior.as_ref()
        && prior["state"] == "current"
    {
        remove_relevance_indexes(
            tx,
            &scope.scope_id,
            &principal.client_id,
            card_kind,
            identity,
            &prior["fields"],
        )?;
    }
    let revision = prior
        .as_ref()
        .and_then(|prior| prior["card_revision"].as_i64())
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| Error::new("REVISION_OVERFLOW", "card revision exhausted"))?;
    let card = json!({
        "card_kind": card_kind,
        "identity": identity,
        "task_id": scope.task["task_id"],
        "task_revision": scope.task["revision"],
        "attempt_id": scope.attempt["attempt_id"],
        "client_id": principal.client_id,
        "card_revision": revision,
        "state": "current",
        "material_digest": digest,
        "fields": fields,
        "updated_at_ms": now,
    });
    set_meta(tx, &current_key, &card)?;
    set_meta(
        tx,
        &card_revision_key(
            &scope.scope_id,
            card_kind,
            identity,
            &principal.client_id,
            revision,
        ),
        &card,
    )?;
    set_meta(
        tx,
        &card_owner_key(&scope.scope_id, &principal.client_id, card_kind, identity),
        &json!({"card_key":current_key,"card_revision":revision}),
    )?;
    write_relevance_indexes(
        tx,
        &scope.scope_id,
        &principal.client_id,
        card_kind,
        identity,
        revision,
        fields,
    )?;
    attach_operation_scope(
        tx,
        operation_id,
        scope.task["task_id"].as_str().unwrap_or_default(),
        scope.attempt["attempt_id"].as_str().unwrap_or_default(),
        &scope.attempt,
    )?;
    Ok(json!({
        "operation_id": operation_id,
        "card_kind": card_kind,
        "identity": identity,
        "card_revision": revision,
        "material_digest": digest,
        "changed": true,
        "coalesced": false,
    }))
}

fn withdraw_card(
    tx: &Transaction<'_>,
    principal: &Principal,
    card_kind: &str,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    let allowed = if card_kind == "work" {
        &["client_request_id"][..]
    } else {
        &["client_request_id", "contract_key"][..]
    };
    model::fields(value, allowed)?;
    let scope = load_current_scope(tx, principal)?;
    let identity = if card_kind == "work" {
        "work"
    } else {
        let key = model::text(value, "contract_key")?;
        validate_identifier(key, "contract_key", 256)?;
        key
    };
    let current_key = keys::card_key(&scope.scope_id, card_kind, identity, &principal.client_id);
    let prior = meta(tx, &current_key)?;
    let Some(prior) = prior.filter(|prior| prior["state"] == "current") else {
        attach_operation_scope(
            tx,
            operation_id,
            scope.task["task_id"].as_str().unwrap_or_default(),
            scope.attempt["attempt_id"].as_str().unwrap_or_default(),
            &scope.attempt,
        )?;
        return Ok(json!({
            "operation_id": operation_id,
            "card_kind": card_kind,
            "identity": identity,
            "changed": false,
            "state": "unavailable",
        }));
    };
    remove_relevance_indexes(
        tx,
        &scope.scope_id,
        &principal.client_id,
        card_kind,
        identity,
        &prior["fields"],
    )?;
    tx.execute(
        "DELETE FROM meta WHERE key=?1",
        [card_owner_key(
            &scope.scope_id,
            &principal.client_id,
            card_kind,
            identity,
        )],
    )?;
    let revision = prior["card_revision"]
        .as_i64()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| Error::new("REVISION_OVERFLOW", "card revision exhausted"))?;
    let tombstone = json!({
        "card_kind": card_kind,
        "identity": identity,
        "task_id": scope.task["task_id"],
        "task_revision": scope.task["revision"],
        "attempt_id": scope.attempt["attempt_id"],
        "client_id": principal.client_id,
        "card_revision": revision,
        "state": "withdrawn",
        "material_digest": prior["material_digest"],
        "fields": {},
        "updated_at_ms": now,
    });
    set_meta(tx, &current_key, &tombstone)?;
    set_meta(
        tx,
        &card_revision_key(
            &scope.scope_id,
            card_kind,
            identity,
            &principal.client_id,
            revision,
        ),
        &tombstone,
    )?;
    attach_operation_scope(
        tx,
        operation_id,
        scope.task["task_id"].as_str().unwrap_or_default(),
        scope.attempt["attempt_id"].as_str().unwrap_or_default(),
        &scope.attempt,
    )?;
    Ok(json!({
        "operation_id": operation_id,
        "card_kind": card_kind,
        "identity": identity,
        "card_revision": revision,
        "changed": true,
        "state": "withdrawn",
    }))
}

fn card_revision_key(
    scope: &str,
    card_kind: &str,
    identity: &str,
    client_id: &str,
    revision: i64,
) -> String {
    format!(
        "coordination:card-revision:{scope}:{card_kind}:{}:{}:{revision:020}",
        keys::key_component(identity),
        keys::key_component(client_id),
    )
}

fn card_owner_key(scope: &str, client_id: &str, card_kind: &str, identity: &str) -> String {
    format!(
        "coordination:card-owner:{scope}:{}:{card_kind}:{}",
        keys::key_component(client_id),
        keys::key_component(identity),
    )
}

fn indexable_card_fields(fields: &Value, card_kind: &str, identity: &str) -> Value {
    if card_kind != "contract" {
        return fields.clone();
    }
    let mut indexed = fields.clone();
    if let Some(object) = indexed.as_object_mut() {
        object.insert("contract_key".into(), json!(identity));
    }
    indexed
}

fn write_relevance_indexes(
    tx: &Transaction<'_>,
    scope: &str,
    client_id: &str,
    card_kind: &str,
    identity: &str,
    revision: i64,
    fields: &Value,
) -> Result<()> {
    let indexed_fields = indexable_card_fields(fields, card_kind, identity);
    for (term_kind, terms) in keys::indexed_terms(&indexed_fields, card_kind) {
        for term in terms {
            let key = keys::relevance_key(scope, term_kind, &term, client_id, card_kind, identity);
            set_meta(
                tx,
                &key,
                &json!({
                    "client_id":client_id,
                    "card_kind":card_kind,
                    "identity":identity,
                    "term_kind":term_kind.as_str(),
                    "term":term,
                    "card_revision":revision,
                }),
            )?;
        }
    }
    Ok(())
}

fn remove_relevance_indexes(
    tx: &Transaction<'_>,
    scope: &str,
    client_id: &str,
    card_kind: &str,
    identity: &str,
    fields: &Value,
) -> Result<()> {
    let indexed_fields = indexable_card_fields(fields, card_kind, identity);
    for (term_kind, terms) in keys::indexed_terms(&indexed_fields, card_kind) {
        for term in terms {
            tx.execute(
                "DELETE FROM meta WHERE key=?1",
                [keys::relevance_key(
                    scope, term_kind, &term, client_id, card_kind, identity,
                )],
            )?;
        }
    }
    Ok(())
}

fn read_scope(db: &Connection, principal: &Principal, value: &Value) -> Result<ScopeData> {
    if principal.role == Role::Participant {
        if value.get("task_id").is_some()
            || value.get("task_revision").is_some()
            || value.get("attempt_id").is_some()
        {
            return Err(Error::invalid(
                "participants query only within their authenticated Task/Attempt",
            ));
        }
        return load_current_scope(db, principal);
    }
    let task_id = model::text(value, "task_id")?;
    let task_revision = model::positive(value, "task_revision")?;
    let attempt_id = model::text(value, "attempt_id")?;
    manager_scope(db, principal, task_id, task_revision, attempt_id)
}

fn indexed_cards(
    db: &Connection,
    scope_id: &str,
    term_kind: keys::TermKind,
    term: &str,
    card_kind: Option<&str>,
    after_client_id: Option<&str>,
    raw_limit: i64,
) -> Result<(Vec<IndexedCard>, bool)> {
    let prefix = keys::relevance_prefix(scope_id, term_kind, term);
    let upper = format!("{prefix}g");
    let lower = match after_client_id {
        Some(client_id) => format!(
            "{}{client}:~",
            prefix,
            client = keys::key_component(client_id)
        ),
        None => prefix,
    };
    let comparison = if after_client_id.is_some() { ">" } else { ">=" };
    let raw: Vec<String> = if let Some(card_kind) = card_kind {
        let sql = format!(
            "SELECT value_json FROM meta WHERE key {comparison} ?1 AND key < ?2 \
             AND json_extract(value_json,'$.card_kind')=?3 ORDER BY key LIMIT ?4"
        );
        let mut statement = db.prepare(&sql)?;
        statement
            .query_map(params![lower, upper, card_kind, raw_limit + 1], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<std::result::Result<_, _>>()?
    } else {
        let sql = format!(
            "SELECT value_json FROM meta WHERE key {comparison} ?1 AND key < ?2 ORDER BY key LIMIT ?3"
        );
        let mut statement = db.prepare(&sql)?;
        statement
            .query_map(params![lower, upper, raw_limit + 1], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<std::result::Result<_, _>>()?
    };
    let has_more = raw.len() as i64 > raw_limit;
    let cards = raw
        .iter()
        .take(raw_limit as usize)
        .filter_map(|raw| serde_json::from_str::<Value>(raw).ok())
        .filter_map(|value| {
            Some(IndexedCard {
                client_id: value.get("client_id")?.as_str()?.to_owned(),
                card_kind: value.get("card_kind")?.as_str()?.to_owned(),
                identity: value.get("identity")?.as_str()?.to_owned(),
            })
        })
        .collect();
    Ok((cards, has_more))
}

fn current_card(
    db: &Connection,
    scope_id: &str,
    card_kind: &str,
    identity: &str,
    client_id: &str,
) -> Result<Option<Value>> {
    Ok(meta(
        db,
        &keys::card_key(scope_id, card_kind, identity, client_id),
    )?
    .filter(|card| card["state"] == "current"))
}

fn project_card(card: &Value, selected_fields: Option<&Value>) -> Result<Value> {
    if card["state"] != "current" {
        return Ok(json!({"available":false,"state":card["state"]}));
    }
    validate_projection_fields(selected_fields)?;
    let mut fields = card["fields"].clone();
    if let Some(selected) = selected_fields {
        let selected = selected
            .as_array()
            .ok_or_else(|| Error::invalid("fields must be an array of field names"))?;
        let mut projected = serde_json::Map::new();
        for item in selected {
            let name = item
                .as_str()
                .filter(|name| !name.trim().is_empty())
                .ok_or_else(|| Error::invalid("fields entries must be nonempty strings"))?;
            if let Some(value) = fields.get(name) {
                projected.insert(name.to_owned(), value.clone());
            }
        }
        fields = Value::Object(projected);
    }
    Ok(json!({
        "available": true,
        "card_kind": card["card_kind"],
        "identity": card["identity"],
        "task_id": card["task_id"],
        "task_revision": card["task_revision"],
        "attempt_id": card["attempt_id"],
        "client_id": card["client_id"],
        "card_revision": card["card_revision"],
        "material_digest": card["material_digest"],
        "updated_at_ms": card["updated_at_ms"],
        "fields": fields,
    }))
}

fn validate_projection_fields(selected_fields: Option<&Value>) -> Result<()> {
    let Some(selected_fields) = selected_fields else {
        return Ok(());
    };
    let selected = selected_fields
        .as_array()
        .ok_or_else(|| Error::invalid("fields must be an array of field names"))?;
    if selected.len() > 64 {
        return Err(Error::invalid("fields may select at most 64 names"));
    }
    let mut seen = BTreeSet::new();
    for item in selected {
        let name = item
            .as_str()
            .filter(|name| !name.trim().is_empty())
            .ok_or_else(|| Error::invalid("fields entries must be nonempty strings"))?;
        if !seen.insert(name) {
            return Err(Error::invalid("fields entries must be unique"));
        }
    }
    Ok(())
}

fn card_get(
    db: &Connection,
    principal: &Principal,
    card_kind: &str,
    value: &Value,
) -> Result<Value> {
    let allowed: &[&str] = if card_kind == "work" {
        &[
            "participant_id",
            "fields",
            "task_id",
            "task_revision",
            "attempt_id",
            "limit",
            "after_client_id",
        ]
    } else {
        &[
            "contract_key",
            "participant_id",
            "fields",
            "task_id",
            "task_revision",
            "attempt_id",
            "limit",
            "after_client_id",
        ]
    };
    model::fields(value, allowed)?;
    validate_projection_fields(value.get("fields"))?;
    let scope = read_scope(db, principal, value)?;
    let fields = value.get("fields");
    if card_kind == "work" {
        let owner = match value.get("participant_id") {
            Some(_) => model::text(value, "participant_id")?,
            None if principal.role == Role::Participant => principal.client_id.as_str(),
            None => {
                return Err(Error::invalid(
                    "participant_id is required for manager work-card lookup",
                ));
            }
        };
        if principal.role == Role::Participant && owner != principal.client_id {
            return Err(Error::new(
                "FORBIDDEN",
                "participants discover another owner's card through exact peer.find matches",
            ));
        }
        let participant = load_current_scope_for_client(db, owner)?;
        if participant.scope_id != scope.scope_id {
            return Err(Error::new("NOT_FOUND", "participant is outside this scope"));
        }
        return match current_card(db, &scope.scope_id, "work", "work", owner)? {
            Some(card) => project_card(&card, fields),
            None => Ok(json!({"available":false,"card_kind":"work","participant_id":owner})),
        };
    }
    let contract_key = model::text(value, "contract_key")?;
    validate_identifier(contract_key, "contract_key", 256)?;
    if value.get("participant_id").is_some() {
        let owner = model::text(value, "participant_id")?;
        if principal.role == Role::Participant && owner != principal.client_id {
            return Err(Error::new(
                "FORBIDDEN",
                "participants discover another owner's card through exact peer.find matches",
            ));
        }
        let participant = load_current_scope_for_client(db, owner)?;
        if participant.scope_id != scope.scope_id {
            return Err(Error::new("NOT_FOUND", "participant is outside this scope"));
        }
        return match current_card(db, &scope.scope_id, "contract", contract_key, owner)? {
            Some(card) => project_card(&card, fields),
            None => Ok(json!({
                "available":false,
                "card_kind":"contract",
                "contract_key":contract_key,
                "participant_id":owner,
            })),
        };
    }
    let (items, next_after, coverage, gaps) = contract_cards_for_term(
        db,
        &scope,
        contract_key,
        value.get("limit"),
        value.get("after_client_id"),
        fields,
    )?;
    Ok(json!({
        "contract_key":contract_key,
        "items":items,
        "next_after":next_after,
        "coverage":coverage,
        "gaps":gaps,
    }))
}

fn card_list(
    db: &Connection,
    principal: &Principal,
    card_kind: &str,
    value: &Value,
) -> Result<Value> {
    let allowed: &[&str] = if card_kind == "work" {
        &[
            "contract_key",
            "path",
            "symbol",
            "interface",
            "fields",
            "task_id",
            "task_revision",
            "attempt_id",
            "limit",
            "after_client_id",
        ]
    } else {
        &[
            "contract_key",
            "fields",
            "task_id",
            "task_revision",
            "attempt_id",
            "limit",
            "after_client_id",
        ]
    };
    model::fields(value, allowed)?;
    validate_projection_fields(value.get("fields"))?;
    let scope = read_scope(db, principal, value)?;
    let (term_kind, term) = if card_kind == "contract" {
        let contract_key = model::text(value, "contract_key")?;
        validate_identifier(contract_key, "contract_key", 256)?;
        (keys::TermKind::Contract, contract_key.to_owned())
    } else {
        selector(value)?.ok_or_else(|| {
            Error::invalid(
                "work_card.list requires one exact contract/path/symbol/interface selector",
            )
        })?
    };
    let limit = keys::parse_page(value.get("limit"), 20)?;
    let after = keys::optional_cursor(value, "after_client_id")?;
    let (cards, index_more) = indexed_cards(
        db,
        &scope.scope_id,
        term_kind,
        &term,
        Some(card_kind),
        after.as_deref(),
        limit,
    )?;
    let mut items = Vec::new();
    let mut stale = 0usize;
    let mut last = None;
    for indexed in cards {
        last = Some(indexed.client_id.clone());
        let Ok(participant) = load_current_scope_for_client(db, &indexed.client_id) else {
            stale = stale.saturating_add(1);
            continue;
        };
        if participant.scope_id != scope.scope_id {
            stale = stale.saturating_add(1);
            continue;
        }
        if let Some(card) = current_card(
            db,
            &scope.scope_id,
            &indexed.card_kind,
            &indexed.identity,
            &indexed.client_id,
        )? {
            items.push(project_card(&card, value.get("fields"))?);
        } else {
            stale = stale.saturating_add(1);
        }
    }
    let partial = index_more || stale > 0;
    Ok(json!({
        "items":items,
        "selector":{"kind":term_kind.as_str(),"value":term},
        "next_after":if index_more { last } else { None },
        "coverage":if partial { "partial" } else { "complete" },
        "gaps":if stale > 0 { json!([{"kind":"stale_card_index_entries","count":stale}]) } else if index_more { json!([{"kind":"more_indexed_cards","count":null}]) } else { json!([]) },
    }))
}

fn selector(value: &Value) -> Result<Option<(keys::TermKind, String)>> {
    let mut found = None;
    for (field, kind) in [
        ("contract_key", keys::TermKind::Contract),
        ("path", keys::TermKind::Path),
        ("symbol", keys::TermKind::Symbol),
        ("interface", keys::TermKind::Interface),
    ] {
        if let Some(raw) = value.get(field) {
            let term = raw
                .as_str()
                .filter(|term| !term.trim().is_empty() && term.len() <= 1024)
                .ok_or_else(|| {
                    Error::invalid(format!("{field} must be nonempty text up to 1024 bytes"))
                })?;
            if term.bytes().any(|byte| byte.is_ascii_control()) {
                return Err(Error::invalid(format!(
                    "{field} contains a control character"
                )));
            }
            if kind == keys::TermKind::Contract {
                validate_identifier(term, field, 256)?;
            }
            if found.is_some() {
                return Err(Error::invalid(
                    "supply exactly one contract_key, path, symbol, or interface selector",
                ));
            }
            found = Some((kind, term.to_owned()));
        }
    }
    Ok(found)
}

fn contract_cards_for_term(
    db: &Connection,
    scope: &ScopeData,
    contract_key: &str,
    raw_limit: Option<&Value>,
    raw_after: Option<&Value>,
    fields: Option<&Value>,
) -> Result<(Vec<Value>, Option<String>, &'static str, Value)> {
    validate_identifier(contract_key, "contract_key", 256)?;
    let limit = keys::parse_page(raw_limit, 20)?;
    let after_value = json!({"after_client_id":raw_after.cloned().unwrap_or(Value::Null)});
    let after = keys::optional_cursor(&after_value, "after_client_id")?;
    let (cards, index_more) = indexed_cards(
        db,
        &scope.scope_id,
        keys::TermKind::Contract,
        contract_key,
        Some("contract"),
        after.as_deref(),
        limit,
    )?;
    let mut items = Vec::new();
    let mut stale = 0usize;
    let mut last = None;
    for indexed in cards {
        last = Some(indexed.client_id.clone());
        let Ok(participant) = load_current_scope_for_client(db, &indexed.client_id) else {
            stale = stale.saturating_add(1);
            continue;
        };
        if participant.scope_id != scope.scope_id {
            stale = stale.saturating_add(1);
            continue;
        }
        match current_card(
            db,
            &scope.scope_id,
            "contract",
            contract_key,
            &indexed.client_id,
        )? {
            Some(card) => items.push(project_card(&card, fields)?),
            None => stale = stale.saturating_add(1),
        }
    }
    let partial = index_more || stale > 0;
    Ok((
        items,
        if index_more { last } else { None },
        if partial { "partial" } else { "complete" },
        if stale > 0 {
            json!([{"kind":"stale_card_index_entries","count":stale}])
        } else if index_more {
            json!([{"kind":"more_indexed_cards","count":null}])
        } else {
            json!([])
        },
    ))
}

fn context_get(db: &Connection, principal: &Principal, value: &Value) -> Result<Value> {
    model::fields(
        value,
        &[
            "task_id",
            "task_revision",
            "attempt_id",
            "contract_key",
            "path",
            "symbol",
            "interface",
            "limit",
            "after_client_id",
        ],
    )?;
    let scope = read_scope(db, principal, value)?;
    let runtime_capability = if principal.role == Role::Participant {
        participant_capability_projection(
            db,
            ParticipantCapabilityScope {
                participant_id: &principal.client_id,
                task_id: model::text(&scope.task, "task_id")?,
                task_revision: model::positive(&scope.task, "revision")?,
                attempt_id: model::text(&scope.attempt, "attempt_id")?,
                binding_id: scope.attempt["binding_id"].as_str(),
                binding_generation: scope.attempt["binding_generation"].as_i64(),
                grant_revision: scope.registration["grant_revision"].as_i64(),
                native_session_id: scope.registration["native_session_id"].as_str(),
                basis_kind: scope.registration["participation_basis"]["kind"].as_str(),
            },
        )?
    } else {
        Value::Null
    };
    let participant_id =
        (principal.role == Role::Participant).then_some(principal.client_id.as_str());
    let work_card = match participant_id {
        Some(client_id) => current_card(db, &scope.scope_id, "work", "work", client_id)?,
        None => None,
    };
    let mut contract_cards = Vec::new();
    let mut contract_gaps = Vec::new();
    if let Some(client_id) = participant_id {
        let prefix = format!(
            "coordination:card-owner:{}:{}:contract:",
            scope.scope_id,
            keys::key_component(client_id)
        );
        let upper = format!("{prefix}g");
        let mut statement = db.prepare(
            "SELECT value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key LIMIT 21",
        )?;
        let rows: Vec<String> = statement
            .query_map(params![prefix, upper], |row| row.get(0))?
            .collect::<std::result::Result<_, _>>()?;
        for raw in rows.iter().take(20) {
            let owner_index: Value = serde_json::from_str(raw)?;
            let Some(card_key) = owner_index.get("card_key").and_then(Value::as_str) else {
                contract_gaps.push(json!({"kind":"malformed_card_owner_index"}));
                continue;
            };
            if let Some(card) = meta(db, card_key)?.filter(|card| card["state"] == "current") {
                contract_cards.push(project_card(&card, None)?);
            }
        }
        if rows.len() > 20 {
            contract_gaps.push(json!({"kind":"contract_card_context_bound","limit":20}));
        }
    }
    let peer_discovery = match selector(value)? {
        Some(_) => peer_find(db, principal, value)?,
        None => json!({
            "items":[],
            "coverage":"partial",
            "gaps":[{"kind":"exact_relationship_selector_not_supplied"}],
        }),
    };
    let gaps = if contract_gaps.is_empty() {
        json!([])
    } else {
        Value::Array(contract_gaps)
    };
    Ok(json!({
        "scope":scope_projection(&scope),
        "work_card":work_card.map(|card| project_card(&card, None)).transpose()?,
        "contract_cards":contract_cards,
        "contract_card_gaps":gaps,
        "peer_discovery":peer_discovery,
        "runtime_capability":runtime_capability,
    }))
}

fn peer_find(db: &Connection, principal: &Principal, value: &Value) -> Result<Value> {
    model::fields(
        value,
        &[
            "task_id",
            "task_revision",
            "attempt_id",
            "contract_key",
            "path",
            "symbol",
            "interface",
            "fields",
            "limit",
            "after_client_id",
        ],
    )?;
    validate_projection_fields(value.get("fields"))?;
    let scope = read_scope(db, principal, value)?;
    let (term_kind, term) = selector(value)?
        .ok_or_else(|| Error::invalid("peer.find requires one exact relationship selector"))?;
    let limit = keys::parse_page(value.get("limit"), 20)?;
    let after = keys::optional_cursor(value, "after_client_id")?;
    // Scan a bounded selector index, then collapse multiple matching cards to
    // one peer. This is a sparse directory query, never a roster walk.
    let scan_limit = keys::MAX_INBOX_SCAN;
    let (cards, index_more) = indexed_cards(
        db,
        &scope.scope_id,
        term_kind,
        &term,
        None,
        after.as_deref(),
        scan_limit,
    )?;
    let mut items = Vec::new();
    let mut seen = BTreeSet::new();
    let mut stale = 0usize;
    let mut last = None;
    let mut has_more_peers = false;
    for indexed in cards {
        last = Some(indexed.client_id.clone());
        if principal.role == Role::Participant && indexed.client_id == principal.client_id {
            continue;
        }
        if !seen.insert(indexed.client_id.clone()) {
            continue;
        }
        let Ok(candidate) = load_current_scope_for_client(db, &indexed.client_id) else {
            stale = stale.saturating_add(1);
            continue;
        };
        if candidate.scope_id != scope.scope_id {
            stale = stale.saturating_add(1);
            continue;
        }
        let Some(card) = current_card(
            db,
            &scope.scope_id,
            &indexed.card_kind,
            &indexed.identity,
            &indexed.client_id,
        )?
        else {
            stale = stale.saturating_add(1);
            continue;
        };
        if items.len() >= limit as usize {
            has_more_peers = true;
            // Continue from the last returned peer; this candidate remains
            // discoverable because its client ID sorts later in the index.
            last = items
                .last()
                .and_then(|item: &Value| item["client_id"].as_str())
                .map(str::to_owned);
            break;
        }
        items.push(json!({
            "client_id":indexed.client_id,
            "participant":{
                "display_alias":candidate.registration["display_alias"],
                "inbound_policy":candidate.registration["inbound_policy"],
                "participation_basis_kind":candidate.registration["participation_basis"]["kind"],
            },
            "match":{"kind":term_kind.as_str(),"value":term,"reason":"exact_card_index"},
            "card":project_card(&card, value.get("fields"))?,
        }));
    }
    let partial = index_more || stale > 0 || has_more_peers;
    Ok(json!({
        "items":items,
        "selector":{"kind":term_kind.as_str(),"value":term},
        "task_id":scope.task["task_id"],
        "task_revision":scope.task["revision"],
        "attempt_id":scope.attempt["attempt_id"],
        "next_after":if partial { last } else { None },
        "coverage":if partial { "partial" } else { "complete" },
        "gaps":if stale > 0 { json!([{"kind":"stale_card_index_entries","count":stale}]) } else if index_more { json!([{"kind":"relevance_scan_bound","count":null}]) } else if has_more_peers { json!([{"kind":"more_relevant_peers","count":null}]) } else { json!([]) },
    }))
}

fn inbox(db: &Connection, principal: &Principal, value: &Value) -> Result<Value> {
    principal.require_participant()?;
    model::fields(value, &["limit", "after_operation_id"])?;
    let limit = keys::parse_page(value.get("limit"), 20)?;
    let after = keys::optional_cursor(value, "after_operation_id")?;
    let scope = match load_current_scope(db, principal) {
        Ok(scope) => scope,
        Err(error) if is_stale_watch_authority(&error) => {
            // Only enabled authenticated creators can retrieve their own
            // compact watch notices after a subject transition. No retained
            // mail or context is read by this fallback.
            let current = super::current_principal(db, principal.clone())?;
            current.require_participant()?;
            let watch_notifications =
                super::coordination_watch::notifications(db, &current, limit)?;
            return Ok(
                json!({"items":[],"task_id":null,"task_revision":null,"attempt_id":null,
                "inbound_policy":null,"availability":"current_scope_unavailable","next_after":null,
                "watch_notifications":watch_notifications,"coverage":"partial",
                "gaps":[{"kind":"current_scope_unavailable","code":error.code}]}),
            );
        }
        Err(error) => return Err(error),
    };
    let watch_notifications = super::coordination_watch::notifications(db, principal, 20)?;
    if scope.registration["inbound_policy"] == "hold" {
        return Ok(json!({
            "items":[],
            "task_id":scope.task["task_id"],
            "task_revision":scope.task["revision"],
            "attempt_id":scope.attempt["attempt_id"],
            "inbound_policy":"hold",
            "availability":"held_by_inbound_policy",
            "next_after":null,
            "watch_notifications":watch_notifications,
            "coverage":"complete",
            "gaps":[],
        }));
    }
    let after_key = if let Some(operation_id) = after.as_deref() {
        let row: Option<(i64, String)> = db
            .query_row(
                "SELECT created_at_ms,result_json FROM operations \
                 WHERE operation_id=?1 AND caller_id<>?2 \
                   AND method IN ('coordination.send','coordination.consult') \
                   AND state='settled' AND json_extract(result_json,'$.recipient')=?2",
                params![operation_id, principal.client_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let (created_at_ms, result_json) = row.ok_or_else(|| {
            Error::invalid(
                "after_operation_id must identify a retained delivery to this participant",
            )
        })?;
        let result: Value = serde_json::from_str(&result_json)?;
        if !delivery_matches_scope(&result, &scope, &principal.client_id)? {
            return Err(Error::invalid(
                "after_operation_id is not a delivery in the authenticated exact scope",
            ));
        }
        let key = keys::mailbox_key(
            &scope.scope_id,
            &principal.client_id,
            created_at_ms,
            operation_id,
        );
        if meta(db, &key)?.is_none() {
            return Err(Error::invalid(
                "after_operation_id has no retained scoped inbox index",
            ));
        }
        Some(key)
    } else {
        None
    };
    let scan_limit = (limit * RELEVANCE_SCAN_FACTOR + 32).min(keys::MAX_INBOX_SCAN);
    let prefix = keys::mailbox_prefix(&scope.scope_id, &principal.client_id);
    let upper = format!("{prefix}g");
    let (lower, comparison) = match after_key {
        Some(key) => (key, ">"),
        None => (prefix, ">="),
    };
    let sql = format!(
        "SELECT key,value_json FROM meta WHERE key {comparison} ?1 AND key < ?2 ORDER BY key LIMIT ?3"
    );
    let mut statement = db.prepare(&sql)?;
    let rows: Vec<(String, String)> = statement
        .query_map(params![lower, upper, scan_limit + 1], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<std::result::Result<_, _>>()?;
    let scan_more = rows.len() as i64 > scan_limit;
    let mut messages = Vec::new();
    let mut stale = 0usize;
    let mut last_operation = None;
    let mut more = false;
    for (_, raw_index) in rows.iter().take(scan_limit as usize) {
        let index: Value = serde_json::from_str(raw_index)?;
        let Some(operation_id) = index.get("operation_id").and_then(Value::as_str) else {
            stale = stale.saturating_add(1);
            continue;
        };
        let operation: Option<(String, String, i64, String, String)> = db
            .query_row(
                "SELECT method,caller_id,created_at_ms,original_request_json,result_json \
                 FROM operations WHERE operation_id=?1 \
                   AND method IN ('coordination.send','coordination.consult') \
                   AND state='settled'",
                [operation_id],
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
        let Some((method, sender, created_at, request_json, result_json)) = operation else {
            stale = stale.saturating_add(1);
            continue;
        };
        let result: Value = serde_json::from_str(&result_json)?;
        let request: Value = serde_json::from_str(&request_json)?;
        if !delivery_matches_scope(&result, &scope, &principal.client_id)?
            || result["sender"] != sender
            || index["sender"] != sender
            || index["recipient"] != principal.client_id
            || index["task_id"] != scope.task["task_id"]
            || index["task_revision"] != scope.task["revision"]
            || index["attempt_id"] != scope.attempt["attempt_id"]
            || index["created_at_ms"] != created_at
            || (method == "coordination.send" && request["recipient"] != principal.client_id)
        {
            stale = stale.saturating_add(1);
            continue;
        }
        if messages.len() == limit as usize {
            more = true;
            break;
        }
        let envelope: Value = serde_json::from_str(model::text(&result, "text")?)?;
        let body_matches = if method == "coordination.send" {
            envelope["body"] == request["body"]
        } else {
            consult_delivery_matches(
                &result,
                &request,
                &envelope["body"],
                &scope,
                &sender,
                &principal.client_id,
                operation_id,
            )?
        };
        if !body_matches {
            stale = stale.saturating_add(1);
            continue;
        }
        messages.push(json!({
            "operation_id":operation_id,
            "delivery_id":result["delivery_id"],
                "sender":sender,
            "recipient":principal.client_id,
            "sent_at_ms":created_at,
            "payload_digest":result["payload_digest"],
            "kind":if method == "coordination.consult" { "consult" } else { "message" },
            "body":envelope["body"],
        }));
        last_operation = Some(operation_id.to_owned());
    }
    let partial = scan_more || stale > 0 || more;
    let next_after = if more || (scan_more && !messages.is_empty()) {
        last_operation
    } else {
        None
    };
    Ok(json!({
        "items":messages,
        "task_id":scope.task["task_id"],
        "task_revision":scope.task["revision"],
        "attempt_id":scope.attempt["attempt_id"],
        "inbound_policy":scope.registration["inbound_policy"],
        "next_after":next_after,
        "watch_notifications":watch_notifications,
        "coverage":if partial { "partial" } else { "complete" },
        "gaps":if stale > 0 { json!([{"kind":"stale_mailbox_records","count":stale}]) } else if scan_more { json!([{"kind":"inbox_scan_bound","count":null}]) } else { json!([]) },
    }))
}

fn delivery_matches_scope(result: &Value, scope: &ScopeData, recipient: &str) -> Result<bool> {
    let Some(text) = result.get("text").and_then(Value::as_str) else {
        return Ok(false);
    };
    let envelope: Value = match serde_json::from_str(text) {
        Ok(envelope) => envelope,
        Err(_) => return Ok(false),
    };
    Ok(envelope["schema"] == "eliot.coordination.message.v1"
        && envelope["sender"] == result["sender"]
        && envelope["recipient"] == recipient
        && envelope["task_id"] == scope.task["task_id"]
        && envelope["task_revision"] == scope.task["revision"]
        && envelope["attempt_id"] == scope.attempt["attempt_id"]
        && result["recipient"] == recipient
        && result["task_id"] == scope.task["task_id"]
        && result["task_revision"] == scope.task["revision"]
        && result["attempt_id"] == scope.attempt["attempt_id"])
}

fn consult_delivery_matches(
    result: &Value,
    request: &Value,
    body: &Value,
    scope: &ScopeData,
    sender: &str,
    recipient: &str,
    operation_id: &str,
) -> Result<bool> {
    let consult = match keys::parse_consult_request(request) {
        Ok(consult) => consult,
        Err(_) => return Ok(false),
    };
    let evidence_digest = evidence_set_digest(&consult.evidence_refs)?;
    Ok(result["delivery_created"] == true
        && result["delivery_operation_id"] == operation_id
        && result["ask_id"] == body["ask_id"]
        && result["ask"] == *body
        && body["schema"] == "eliot.coordination.consult.v1"
        && body["task_id"] == scope.task["task_id"]
        && body["task_revision"] == scope.task["revision"]
        && body["attempt_id"] == scope.attempt["attempt_id"]
        && body["sender"] == sender
        && body["recipient"] == recipient
        && body["target"] == consult_target_json(&consult)
        && body["field"] == consult.field
        && body["question_kind"] == consult.question_kind
        && body["question"] == consult.question
        && body["why_needed"] == consult.why_needed
        && body["expected_answer"] == consult.expected_answer
        && body["blocking"] == consult.blocking
        && body["reply_deadline_ms"] == consult.reply_deadline_ms
        && body["evidence_refs"] == json!(consult.evidence_refs)
        && body["evidence_set_digest"] == evidence_digest)
}

fn participant_operation_get(
    db: &Connection,
    principal: &Principal,
    value: &Value,
) -> Result<Value> {
    principal.require_participant()?;
    model::fields(value, &["operation_id"])?;
    let operation_id = model::text(value, "operation_id")?;
    if let Some(operation) = authorize_contract_operation_read(db, principal, operation_id)? {
        return Ok(operation);
    }
    let scope = load_current_scope(db, principal)?;
    let raw: Option<ParticipantOperationRecord> = db
        .query_row(
            "SELECT caller_id,method,state,task_id,attempt_id,binding_id,binding_generation,result_json \
             FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| {
                let result_raw: Option<String> = row.get(7)?;
                let result = result_raw
                    .map(|raw| {
                        serde_json::from_str(&raw).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                7,
                                rusqlite::types::Type::Text,
                                Box::new(error),
                            )
                        })
                    })
                    .transpose()?
                    .unwrap_or(Value::Null);
                Ok(ParticipantOperationRecord {
                    caller_id: row.get(0)?,
                    method: row.get(1)?,
                    state: row.get(2)?,
                    task_id: row.get(3)?,
                    attempt_id: row.get(4)?,
                    binding_id: row.get(5)?,
                    binding_generation: row.get(6)?,
                    result,
                })
            },
        )
        .optional()?;
    let record = raw.ok_or_else(|| Error::new("NOT_FOUND", format!("Operation {operation_id}")))?;
    if matches!(
        scope.registration["participation_basis"]["kind"].as_str(),
        Some("attempt_owner" | "producer_ref")
    ) && matches!(record.method.as_str(), "source.capture" | "agent.result")
    {
        return participant_native_operation_projection(db, &scope, operation_id, &record);
    }
    if record.caller_id != principal.client_id
        || !record.method.starts_with("coordination.")
        || record.task_id.as_deref() != scope.task["task_id"].as_str()
        || record.attempt_id.as_deref() != scope.attempt["attempt_id"].as_str()
        || record.binding_id.as_deref() != scope.attempt["binding_id"].as_str()
        || record.binding_generation != scope.attempt["binding_generation"].as_i64()
    {
        return Err(Error::new(
            "NOT_FOUND",
            "Operation is outside the authenticated participant scope",
        ));
    }
    operations::get_operation(db, operation_id)
}

/// Return only the retained candidate-origin facts an ordinary Participant
/// needs to continue its exact current Attempt. Manager/native operation
/// contracts, caller identity and runtime references stay private here.
fn participant_native_operation_projection(
    db: &Connection,
    scope: &ScopeData,
    operation_id: &str,
    record: &ParticipantOperationRecord,
) -> Result<Value> {
    let task_id = scope.task["task_id"]
        .as_str()
        .ok_or_else(|| Error::new("NOT_FOUND", "Participant Task scope is incomplete"))?;
    let attempt_id = scope.attempt["attempt_id"]
        .as_str()
        .ok_or_else(|| Error::new("NOT_FOUND", "Participant Attempt scope is incomplete"))?;
    if record.task_id.as_deref() != Some(task_id)
        || record.attempt_id.as_deref() != Some(attempt_id)
        || record.state != "settled"
        || record.result["outcome"] != "applied"
    {
        return Err(Error::new(
            "NOT_FOUND",
            "Operation is outside the authenticated participant scope",
        ));
    }
    let task_revision = model::positive(&scope.task, "revision")?;
    let base = json!({
        "operation_id":operation_id,
        "method":record.method,
        "state":record.state,
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
    });
    match record.method.as_str() {
        "source.capture" => {
            let candidate_ref = record.result["candidate_ref"].as_str().ok_or_else(|| {
                Error::new(
                    "NOT_FOUND",
                    "source capture has no applied candidate origin",
                )
            })?;
            let candidate = results::get(db, candidate_ref)?;
            if candidate.kind != "source_snapshot"
                || candidate.metadata["task_id"] != task_id
                || candidate.metadata["attempt_id"] != attempt_id
                || candidate.metadata["task_revision"] != task_revision
            {
                return Err(Error::new(
                    "NOT_FOUND",
                    "source capture candidate is outside the current Participant Attempt",
                ));
            }
            Ok(json!({
                "operation_id":base["operation_id"],
                "method":base["method"],
                "state":base["state"],
                "task_id":base["task_id"],
                "task_revision":base["task_revision"],
                "attempt_id":base["attempt_id"],
                "candidate_ref":candidate_ref,
                "candidate_kind":candidate.kind,
                "candidate_sha256":candidate.content_digest,
                "candidate_byte_length":candidate.byte_length,
                "result":{"outcome":"applied","candidate_ref":candidate_ref},
            }))
        }
        "agent.result" => {
            let binding_id = scope.attempt["binding_id"].as_str().ok_or_else(|| {
                Error::new(
                    "NOT_FOUND",
                    "native result is not linked to the current Attempt binding",
                )
            })?;
            let binding_generation =
                scope.attempt["binding_generation"]
                    .as_i64()
                    .ok_or_else(|| {
                        Error::new(
                            "NOT_FOUND",
                            "native result is not linked to the current Attempt generation",
                        )
                    })?;
            if record.binding_id.as_deref() != Some(binding_id)
                || record.binding_generation != Some(binding_generation)
            {
                return Err(Error::new(
                    "NOT_FOUND",
                    "Operation is outside the authenticated participant binding scope",
                ));
            }
            let details = &record.result["details"];
            if details["completion_condition"] == "result_page_persisted"
                && details["source"]["schema_id"]
                    == swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID
            {
                let page_id = details["artifact_ref"].as_str().ok_or_else(|| {
                    Error::new(
                        "NOT_FOUND",
                        "normalized result has no retained page artifact",
                    )
                })?;
                let page = results::get(db, page_id)?;
                if !super::normalized_result::validate_page_artifact(db, &scope.attempt, &page)?
                    || page.metadata["operation_id"] != operation_id
                {
                    return Err(Error::new(
                        "NOT_FOUND",
                        "normalized result page is outside the retained Participant Attempt",
                    ));
                }
                let whole_body = page.metadata["offset_bytes"] == 0
                    && page.metadata["total_bytes"] == page.byte_length
                    && page.metadata["eof"] == true;
                return Ok(json!({
                    "operation_id":base["operation_id"],
                    "method":base["method"],
                    "state":base["state"],
                    "task_id":base["task_id"],
                    "task_revision":base["task_revision"],
                    "attempt_id":base["attempt_id"],
                    "binding_id":binding_id,
                    "binding_generation":binding_generation,
                    "candidate_ref":if whole_body { json!(page_id) } else { Value::Null },
                    "candidate_refs":[page_id],
                    "candidate_kind":page.kind,
                    "candidate_sha256":page.content_digest,
                    "candidate_byte_length":page.byte_length,
                    "result":{
                        "outcome":"applied",
                        "completion_condition":"result_page_persisted",
                        "artifact_ref":page_id,
                        "offset_bytes":page.metadata["offset_bytes"],
                        "total_bytes":page.metadata["total_bytes"],
                        "eof":page.metadata["eof"]
                    }
                }));
            }
            if details["completion_condition"] != "batch_output_artifacts_selected"
                || details["dispatch_operation_id"].as_str().is_none()
            {
                return Err(Error::new(
                    "NOT_FOUND",
                    "native result has no complete retained candidate origin",
                ));
            }
            let dispatch_id = details["dispatch_operation_id"]
                .as_str()
                .unwrap_or_default();
            let dispatch = operations::get_operation(db, dispatch_id)?;
            if dispatch["method"] != "task.dispatch"
                || !matches!(dispatch["state"].as_str(), Some("settled" | "rejected"))
                || dispatch["task_id"] != task_id
                || dispatch["attempt_id"] != attempt_id
                || dispatch["binding_id"] != binding_id
                || dispatch["binding_generation"] != binding_generation
            {
                return Err(Error::new(
                    "NOT_FOUND",
                    "native result dispatch is outside the current Participant Attempt",
                ));
            }
            let refs = details["artifact_refs"].as_array().ok_or_else(|| {
                Error::new("NOT_FOUND", "native result has no retained artifact pages")
            })?;
            if refs.is_empty() {
                return Err(Error::new(
                    "NOT_FOUND",
                    "native result has no retained artifact pages",
                ));
            }
            let mut candidate_refs = Vec::with_capacity(refs.len());
            for reference in refs {
                let page_id = reference.as_str().ok_or_else(|| {
                    Error::new("NOT_FOUND", "native result artifact reference is invalid")
                })?;
                let page = results::get(db, page_id)?;
                if page.kind != "native_result_page"
                    || page.metadata["operation_id"] != dispatch_id
                    || page.metadata["binding_id"] != binding_id
                    || page.metadata["binding_generation"] != binding_generation
                    || !crate::runtime::batch::BATCH_OUTPUTS
                        .contains(&page.metadata["native_output"].as_str().unwrap_or(""))
                {
                    return Err(Error::new(
                        "NOT_FOUND",
                        "native result page is outside the retained dispatch origin",
                    ));
                }
                candidate_refs.push(page_id.to_owned());
            }
            Ok(json!({
                "operation_id":base["operation_id"],
                "method":base["method"],
                "state":base["state"],
                "task_id":base["task_id"],
                "task_revision":base["task_revision"],
                "attempt_id":base["attempt_id"],
                "binding_id":binding_id,
                "binding_generation":binding_generation,
                "candidate_ref":if candidate_refs.len() == 1 { json!(candidate_refs[0]) } else { Value::Null },
                "candidate_refs":candidate_refs.clone(),
                "result":{"outcome":"applied","dispatch_operation_id":dispatch_id,"artifact_refs":candidate_refs},
            }))
        }
        _ => Err(Error::new(
            "NOT_FOUND",
            "Operation is outside the authenticated participant scope",
        )),
    }
}

fn send(
    tx: &Transaction<'_>,
    principal: &Principal,
    value: &Value,
    config: &Config,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    let sender = load_current_scope(tx, principal)?;
    let mailbox_params = normalize_send(tx, principal, value)?;
    let (mailbox_result, queued) = super::apply(
        tx,
        principal,
        "message.send",
        &mailbox_params,
        config,
        super::ApplyContext {
            operation_id,
            now,
            plan: super::MutationPlan {
                check_plan: None,
                forge_execution: None,
                launch_operation_id: None,
            },
        },
    )?;
    if queued {
        return Err(Error::new(
            "COORDINATION_SEND_QUEUED",
            "coordination messages must remain durable mailbox records without native effects",
        ));
    }
    let recipient = model::text(&mailbox_result, "recipient")?;
    let index_key = keys::mailbox_key(&sender.scope_id, recipient, now, operation_id);
    set_meta(
        tx,
        &index_key,
        &json!({
            "operation_id":operation_id,
            "sender":principal.client_id,
            "recipient":recipient,
            "task_id":sender.task["task_id"],
            "task_revision":sender.task["revision"],
            "attempt_id":sender.attempt["attempt_id"],
            "created_at_ms":now,
        }),
    )?;
    attach_operation_scope(
        tx,
        operation_id,
        sender.task["task_id"].as_str().unwrap_or_default(),
        sender.attempt["attempt_id"].as_str().unwrap_or_default(),
        &sender.attempt,
    )?;
    Ok(json!({
        "operation_id":operation_id,
        "delivery_id":mailbox_result["delivery_id"],
        "sender":principal.client_id,
        "recipient":mailbox_result["recipient"],
        "task_id":sender.task["task_id"],
        "task_revision":sender.task["revision"],
        "attempt_id":sender.attempt["attempt_id"],
        "payload_digest":mailbox_result["payload_digest"],
        "text":mailbox_result["text"],
        "delivery":"durable_mailbox_only",
    }))
}

fn consult(
    tx: &Transaction<'_>,
    principal: &Principal,
    value: &Value,
    config: &Config,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    principal.require_participant()?;
    let request = keys::parse_consult_request(value)?;
    let sender = load_current_scope(tx, principal)?;
    let (cards, index_more, stale) = consult_matching_cards(tx, &sender, &request)?;
    if index_more || stale > 0 {
        return scoped_consult_result(
            tx,
            operation_id,
            &sender,
            json!({
                "status":"unknown_coverage",
                "owner_state":"unknown_coverage",
                "target":consult_target_json(&request),
                "field":request.field,
                "delivery_created":false,
                "coverage":"partial",
                "gaps":[{
                    "kind":if index_more { "consult_relevance_scan_bound" } else { "stale_consult_card_index_entries" },
                    "count":if index_more { Value::Null } else { json!(stale) },
                }],
            }),
        );
    }

    let field_matches: Vec<&ConsultCardMatch> = cards
        .iter()
        .filter(|candidate| {
            candidate.card["fields"]
                .get(&request.field)
                .is_some_and(|answer| !answer.is_null())
        })
        .collect();
    if field_matches.len() == 1 {
        let candidate = field_matches[0];
        let selected_fields = json!([request.field]);
        let card = project_card(&candidate.card, Some(&selected_fields))?;
        let mut answer = serde_json::Map::new();
        answer.insert(
            request.field.clone(),
            card["fields"]
                .get(request.field.as_str())
                .cloned()
                .unwrap_or(Value::Null),
        );
        return scoped_consult_result(
            tx,
            operation_id,
            &sender,
            json!({
                "status":"answered_from_card",
                "owner_state":"exact_owner",
                "target":consult_target_json(&request),
                "field":request.field,
                "card_revision":card["card_revision"],
                "card_digest":card["material_digest"],
                "answer":Value::Object(answer),
                "source":{
                    "client_id":candidate.indexed.client_id,
                    "card_kind":candidate.indexed.card_kind,
                    "identity":candidate.indexed.identity,
                },
                "delivery_created":false,
                "coverage":"complete",
                "gaps":[],
            }),
        );
    }
    if field_matches.len() > 1 {
        return scoped_consult_result(
            tx,
            operation_id,
            &sender,
            json!({
                "status":"multiple_candidates",
                "owner_state":"multiple_candidates",
                "target":consult_target_json(&request),
                "field":request.field,
                "card_fact_count":field_matches.len(),
                "candidates":consult_candidates(&field_matches),
                "delivery_created":false,
                "coverage":"complete",
                "gaps":[],
            }),
        );
    }

    let mut owner_matches = Vec::new();
    let mut seen_owners = BTreeSet::new();
    for candidate in cards
        .iter()
        .filter(|candidate| candidate.indexed.client_id != principal.client_id)
    {
        if seen_owners.insert(candidate.indexed.client_id.clone()) {
            owner_matches.push(candidate);
        }
    }
    if owner_matches.is_empty() {
        return scoped_consult_result(
            tx,
            operation_id,
            &sender,
            json!({
                "status":"unowned",
                "owner_state":"unowned",
                "target":consult_target_json(&request),
                "field":request.field,
                "delivery_created":false,
                "coverage":"complete",
                "manager_attention":if request.blocking {
                    json!({"created":false,"gap":"blocking_unowned_attention_not_implemented"})
                } else {
                    Value::Null
                },
                "gaps":if request.blocking {
                    json!([{"kind":"manager_attention_not_recorded"}])
                } else {
                    json!([])
                },
            }),
        );
    }
    if owner_matches.len() > 1 {
        return scoped_consult_result(
            tx,
            operation_id,
            &sender,
            json!({
                "status":"multiple_candidates",
                "owner_state":"multiple_candidates",
                "target":consult_target_json(&request),
                "field":request.field,
                "candidates":consult_candidates(&owner_matches),
                "delivery_created":false,
                "coverage":"complete",
                "gaps":[],
            }),
        );
    }

    let recipient = &owner_matches[0].indexed.client_id;
    let evidence_set_digest = evidence_set_digest(&request.evidence_refs)?;
    let fingerprint = consult_fingerprint(
        &sender,
        &principal.client_id,
        recipient,
        &request,
        &evidence_set_digest,
    )?;
    let ask_id = fingerprint.clone();
    let consult_key = keys::consult_key(&sender.scope_id, &fingerprint);
    if let Some(existing) = meta(tx, &consult_key)? {
        let Some(existing_operation_id) = existing["operation_id"].as_str() else {
            return scoped_consult_result(
                tx,
                operation_id,
                &sender,
                stale_consult_result(&request, "stale_consult_fingerprint_record"),
            );
        };
        let existing_operation = match operations::get_operation(tx, existing_operation_id) {
            Ok(operation) => operation,
            Err(error) if error.code == "NOT_FOUND" => {
                return scoped_consult_result(
                    tx,
                    operation_id,
                    &sender,
                    stale_consult_result(&request, "stale_consult_operation"),
                );
            }
            Err(error) => return Err(error),
        };
        let existing_result = &existing_operation["result"];
        if existing["fingerprint"] != fingerprint
            || existing["ask_id"] != ask_id
            || existing["recipient"] != recipient.as_str()
            || existing_operation["caller_id"] != principal.client_id
            || existing_operation["method"] != "coordination.consult"
            || existing_operation["state"] != "settled"
            || existing_operation["task_id"] != sender.task["task_id"]
            || existing_operation["attempt_id"] != sender.attempt["attempt_id"]
            || existing_result["ask_id"] != ask_id
            || existing_result["recipient"] != recipient.as_str()
        {
            return scoped_consult_result(
                tx,
                operation_id,
                &sender,
                stale_consult_result(&request, "consult_coalescing_record_mismatch"),
            );
        }
        return scoped_consult_result(
            tx,
            operation_id,
            &sender,
            json!({
                "operation_id":operation_id,
                "status":"coalesced",
                "owner_state":"exact_owner",
                "ask_id":ask_id,
                "delivery_operation_id":existing_operation_id,
                "delivery_id":existing_result["delivery_id"],
                "sender":principal.client_id,
                "recipient":recipient,
                "task_id":sender.task["task_id"],
                "task_revision":sender.task["revision"],
                "attempt_id":sender.attempt["attempt_id"],
                "ask":existing_result["ask"],
                "delivery_created":false,
                "delivery":"durable_mailbox_only",
            }),
        );
    }

    let ask = json!({
        "schema":"eliot.coordination.consult.v1",
        "ask_id":ask_id,
        "task_id":sender.task["task_id"],
        "task_revision":sender.task["revision"],
        "attempt_id":sender.attempt["attempt_id"],
        "sender":principal.client_id,
        "recipient":recipient,
        "target":consult_target_json(&request),
        "field":request.field,
        "question_kind":request.question_kind,
        "question":request.question,
        "why_needed":request.why_needed,
        "expected_answer":request.expected_answer,
        "blocking":request.blocking,
        "reply_deadline_ms":request.reply_deadline_ms,
        "evidence_refs":request.evidence_refs,
        "evidence_set_digest":evidence_set_digest,
        "created_at_ms":now,
    });
    let sent = send(
        tx,
        principal,
        &json!({
            "client_request_id":request.client_request_id,
            "recipient":recipient,
            "body":ask,
        }),
        config,
        operation_id,
        now,
    )?;
    set_meta(
        tx,
        &consult_key,
        &json!({
            "ask_id":ask_id,
            "fingerprint":fingerprint,
            "operation_id":operation_id,
            "recipient":recipient,
            "task_id":sender.task["task_id"],
            "task_revision":sender.task["revision"],
            "attempt_id":sender.attempt["attempt_id"],
        }),
    )?;
    let mut result = sent;
    result["status"] = json!("asked");
    result["owner_state"] = json!("exact_owner");
    result["ask_id"] = json!(ask_id);
    result["delivery_operation_id"] = json!(operation_id);
    result["ask"] = ask;
    result["delivery_created"] = json!(true);
    Ok(result)
}

fn consult_matching_cards(
    db: &Connection,
    sender: &ScopeData,
    request: &keys::ConsultRequest,
) -> Result<(Vec<ConsultCardMatch>, bool, usize)> {
    let prefix =
        keys::relevance_prefix(&sender.scope_id, request.target_kind, &request.target_value);
    let upper = format!("{prefix}g");
    let raw_limit = keys::MAX_INBOX_SCAN;
    let mut statement = db.prepare(
        "SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key LIMIT ?3",
    )?;
    let raw: Vec<(String, String)> = statement
        .query_map(params![prefix, upper, raw_limit + 1], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<std::result::Result<_, _>>()?;
    let index_more = raw.len() as i64 > raw_limit;
    let mut cards = Vec::new();
    let mut stale = 0usize;
    for (index_key, raw_index) in raw.iter().take(raw_limit as usize) {
        let index: Value = match serde_json::from_str(raw_index) {
            Ok(index) => index,
            Err(_) => {
                stale = stale.saturating_add(1);
                continue;
            }
        };
        let Some(client_id) = index.get("client_id").and_then(Value::as_str) else {
            stale = stale.saturating_add(1);
            continue;
        };
        let Some(card_kind) = index.get("card_kind").and_then(Value::as_str) else {
            stale = stale.saturating_add(1);
            continue;
        };
        let Some(identity) = index.get("identity").and_then(Value::as_str) else {
            stale = stale.saturating_add(1);
            continue;
        };
        if !matches!(card_kind, "work" | "contract")
            || keys::relevance_key(
                &sender.scope_id,
                request.target_kind,
                &request.target_value,
                client_id,
                card_kind,
                identity,
            ) != index_key.as_str()
        {
            stale = stale.saturating_add(1);
            continue;
        }
        let indexed = IndexedCard {
            client_id: client_id.to_owned(),
            card_kind: card_kind.to_owned(),
            identity: identity.to_owned(),
        };
        let participant = match load_current_scope_for_client(db, client_id) {
            Ok(participant) => participant,
            Err(_) => {
                stale = stale.saturating_add(1);
                continue;
            }
        };
        if participant.scope_id != sender.scope_id {
            stale = stale.saturating_add(1);
            continue;
        }
        let Some(card) = current_card(db, &sender.scope_id, card_kind, identity, client_id)? else {
            stale = stale.saturating_add(1);
            continue;
        };
        if card["client_id"] != client_id
            || card["task_id"] != sender.task["task_id"]
            || card["task_revision"] != sender.task["revision"]
            || card["attempt_id"] != sender.attempt["attempt_id"]
            || card["card_kind"] != card_kind
            || card["identity"] != identity
        {
            stale = stale.saturating_add(1);
            continue;
        }
        let indexed_fields = indexable_card_fields(&card["fields"], card_kind, identity);
        let exact_term = keys::indexed_terms(&indexed_fields, card_kind)
            .get(&request.target_kind)
            .is_some_and(|terms| terms.contains(&request.target_value));
        if !exact_term {
            stale = stale.saturating_add(1);
            continue;
        }
        cards.push(ConsultCardMatch {
            indexed,
            participant,
            card,
        });
    }
    Ok((cards, index_more, stale))
}

fn consult_candidates(matches: &[&ConsultCardMatch]) -> Value {
    let mut candidates = BTreeMap::<String, Value>::new();
    for candidate in matches {
        candidates
            .entry(candidate.indexed.client_id.clone())
            .or_insert_with(|| {
                json!({
                    "client_id":candidate.indexed.client_id,
                    "display_alias":candidate.participant.registration["display_alias"],
                    "card_kind":candidate.indexed.card_kind,
                    "identity":candidate.indexed.identity,
                })
            });
    }
    let mut items: Vec<Value> = candidates.into_values().collect();
    let truncated = items.len() > 20;
    items.truncate(20);
    json!({"items":items,"truncated":truncated})
}

fn consult_target_json(request: &keys::ConsultRequest) -> Value {
    let field = match request.target_kind {
        keys::TermKind::Contract => "contract_key",
        keys::TermKind::Path => "path",
        keys::TermKind::Symbol => "symbol",
        keys::TermKind::Interface => "interface",
    };
    let mut target = serde_json::Map::new();
    target.insert(field.to_owned(), json!(request.target_value));
    Value::Object(target)
}

fn consult_fingerprint(
    sender: &ScopeData,
    sender_id: &str,
    recipient: &str,
    request: &keys::ConsultRequest,
    evidence_set_digest: &str,
) -> Result<String> {
    let fingerprint = json!({
        "task_id":sender.task["task_id"],
        "task_revision":sender.task["revision"],
        "attempt_id":sender.attempt["attempt_id"],
        "sender":sender_id,
        "participation_basis":sender.registration["participation_basis"],
        "recipient":recipient,
        "target":{"kind":request.target_kind.as_str(),"value":request.target_value},
        "field":request.field,
        "question_kind":request.question_kind,
        "question":request.question,
        "why_needed":request.why_needed,
        "expected_answer":request.expected_answer,
        "blocking":request.blocking,
        "reply_deadline_ms":request.reply_deadline_ms,
        "evidence_set_digest":evidence_set_digest,
    });
    Ok(model::digest(model::canonical(&fingerprint)?.as_bytes()))
}

fn evidence_set_digest(evidence_refs: &[String]) -> Result<String> {
    let mut sorted = evidence_refs.to_vec();
    sorted.sort();
    Ok(model::digest(model::canonical(&json!(sorted))?.as_bytes()))
}

fn stale_consult_result(request: &keys::ConsultRequest, gap: &str) -> Value {
    json!({
        "status":"unknown_coverage",
        "owner_state":"unknown_coverage",
        "target":consult_target_json(request),
        "field":request.field,
        "delivery_created":false,
        "coverage":"partial",
        "gaps":[{"kind":gap}],
    })
}

fn scoped_consult_result(
    tx: &Transaction<'_>,
    operation_id: &str,
    sender: &ScopeData,
    result: Value,
) -> Result<Value> {
    attach_operation_scope(
        tx,
        operation_id,
        sender.task["task_id"].as_str().unwrap_or_default(),
        sender.attempt["attempt_id"].as_str().unwrap_or_default(),
        &sender.attempt,
    )?;
    Ok(result)
}

fn contract_thread_context(
    db: &Connection,
    principal: &Principal,
    thread_id: &str,
) -> Result<(super::coordination_threads::ThreadContext, ScopeData, Value)> {
    let context =
        super::coordination_threads::authorize_current_thread_mutation(db, principal, thread_id)?;
    if context.topic_kind != "contract" || context.state != "open" {
        return Err(Error::new(
            "THREAD_STATE_CONFLICT",
            "contract proposals require an open contract Thread",
        ));
    }
    let participant = super::coordination_threads::require_thread_participant(&context, principal)?;
    super::coordination_threads::validate_thread_participant_current(db, &context, principal)?;
    let sender = load_current_scope(db, principal)?;
    if sender.task["task_id"].as_str() != Some(context.task_id.as_str())
        || sender.task["revision"].as_i64() != Some(context.task_revision)
        || sender.attempt["attempt_id"].as_str() != Some(context.attempt_id.as_str())
    {
        return Err(Error::new(
            "STALE_THREAD_SCOPE",
            "current Participant scope no longer matches the contract Thread",
        ));
    }
    let author = json!({
        "client_id": participant.client_id,
        "role": participant.role,
        "generation": participant.generation,
        "participation_basis": participant.participation_basis,
        "registration_fingerprint": participant.registration_fingerprint,
        "actor": participant.actor,
        "scope": participant.scope,
    });
    Ok((context, sender, author))
}

fn propose_contract(
    tx: &Transaction<'_>,
    principal: &Principal,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    let request = crate::coordination::contract::parse_proposal_request(value)?;
    let (context, sender, author) = contract_thread_context(tx, principal, &request.thread_id)?;
    let (proposal_id, revision_number, proposal_sequence, created_at_ms) =
        if let Some(prior_revision_id) = request.supersedes_revision_id.as_deref() {
            let pointer_key = proposal_revision_pointer_key(prior_revision_id);
            let pointer = meta(tx, &pointer_key)?.ok_or_else(|| {
                Error::new(
                    "STALE_CONTRACT_REVISION",
                    "supersedes_revision_id is not retained",
                )
            })?;
            if pointer["thread_id"] != context.thread_id {
                return Err(Error::new(
                    "STALE_CONTRACT_REVISION",
                    "superseded revision belongs to another Thread",
                ));
            }
            let proposal_id = model::text(&pointer, "proposal_id")?.to_owned();
            let head = meta(tx, &proposal_head_key(&proposal_id))?.ok_or_else(|| {
                Error::new(
                    "COORDINATION_INDEX_CORRUPT",
                    "proposal revision pointer has no proposal header",
                )
            })?;
            if head["thread_id"] != context.thread_id
                || head["latest_revision_id"] != prior_revision_id
            {
                return Err(Error::new(
                    "STALE_CONTRACT_REVISION",
                    "supersedes_revision_id must name the current proposal head in this Thread",
                ));
            }
            let prior = meta(tx, &proposal_revision_key(&proposal_id, prior_revision_id))?
                .ok_or_else(|| {
                    Error::new(
                        "COORDINATION_INDEX_CORRUPT",
                        "proposal header points to a missing revision",
                    )
                })?;
            if prior["proposal_digest"] != head["latest_digest"] {
                return Err(Error::new(
                    "COORDINATION_INDEX_CORRUPT",
                    "proposal header digest does not match its latest revision",
                ));
            }
            let revision_number = head["revision_count"]
                .as_i64()
                .and_then(|count| count.checked_add(1))
                .ok_or_else(|| Error::new("REVISION_OVERFLOW", "proposal revision exhausted"))?;
            let proposal_sequence = head["proposal_sequence"]
                .as_i64()
                .filter(|sequence| *sequence > 0)
                .ok_or_else(|| {
                    Error::new("COORDINATION_INDEX_CORRUPT", "proposal sequence is missing")
                })?;
            let created_at_ms = head["created_at_ms"]
                .as_i64()
                .filter(|timestamp| *timestamp > 0)
                .ok_or_else(|| {
                    Error::new(
                        "COORDINATION_INDEX_CORRUPT",
                        "proposal creation time is missing",
                    )
                })?;
            (
                proposal_id,
                revision_number,
                proposal_sequence,
                created_at_ms,
            )
        } else {
            let proposal_id = format!("cprop-{}", uuid::Uuid::new_v4());
            if meta(tx, &proposal_head_key(&proposal_id))?.is_some() {
                return Err(Error::new(
                    "ID_COLLISION",
                    "proposal identifier already exists",
                ));
            }
            let sequence_key = proposal_thread_sequence_key(&context.thread_id);
            let prior_sequence = match meta(tx, &sequence_key)? {
                None => 0,
                Some(record) => record["last_sequence"]
                    .as_i64()
                    .filter(|sequence| *sequence >= 0)
                    .ok_or_else(|| {
                        Error::new(
                            "COORDINATION_INDEX_CORRUPT",
                            "proposal sequence record is invalid",
                        )
                    })?,
            };
            let proposal_sequence = prior_sequence.checked_add(1).ok_or_else(|| {
                Error::new("REVISION_OVERFLOW", "Thread proposal sequence exhausted")
            })?;
            set_meta(
                tx,
                &sequence_key,
                &json!({"last_sequence":proposal_sequence}),
            )?;
            (proposal_id, 1, proposal_sequence, now)
        };
    let proposal_revision_id = format!("cprev-{}", uuid::Uuid::new_v4());
    if meta(tx, &proposal_revision_pointer_key(&proposal_revision_id))?.is_some() {
        return Err(Error::new(
            "ID_COLLISION",
            "proposal revision identifier already exists",
        ));
    }
    let revision = json!({
        "schema_version": 1,
        "record_type": "contract_proposal_revision",
        "proposal_id": proposal_id,
        "proposal_revision_id": proposal_revision_id,
        "revision": revision_number,
        "thread_id": context.thread_id,
        "task_id": context.task_id,
        "task_revision": context.task_revision,
        "attempt_id": context.attempt_id,
        "sponsor_owner_id": context.sponsor_owner_id,
        "author": author,
        "supersedes_revision_id": request.supersedes_revision_id,
        "proposal_digest": request.digest,
        "proposal": request.canonical_body,
        "operation_id": operation_id,
        "created_at_ms": now,
    });
    let head_key = proposal_head_key(&proposal_id);
    let prior_head = meta(tx, &head_key)?;
    let header = json!({
        "schema_version": 1,
        "record_type": "contract_proposal_head",
        "proposal_id": proposal_id,
        "thread_id": context.thread_id,
        "topic": request.topic,
        "latest_revision_id": proposal_revision_id,
        "latest_digest": request.digest,
        "revision_count": revision_number,
        "proposal_sequence": proposal_sequence,
        "sponsor_owner_id": context.sponsor_owner_id,
        "created_at_ms": prior_head.as_ref().and_then(|head| head["created_at_ms"].as_i64()).unwrap_or(created_at_ms),
        "updated_at_ms": now,
    });
    let index_key = proposal_thread_index_key(&context.thread_id, &proposal_id);
    let index = json!({
        "proposal_id": proposal_id,
        "thread_id": context.thread_id,
        "topic": request.topic,
        "latest_revision_id": proposal_revision_id,
        "latest_digest": request.digest,
        "revision_count": revision_number,
        "proposal_sequence": proposal_sequence,
        "sponsor_owner_id": context.sponsor_owner_id,
        "created_at_ms": header["created_at_ms"],
        "updated_at_ms": now,
    });
    let revision_key = proposal_revision_key(&proposal_id, &proposal_revision_id);
    let pointer_key = proposal_revision_pointer_key(&proposal_revision_id);
    set_meta(tx, &revision_key, &revision)?;
    set_meta(
        tx,
        &pointer_key,
        &json!({
            "proposal_id": proposal_id,
            "thread_id": context.thread_id,
            "revision": revision_number,
        }),
    )?;
    set_meta(tx, &head_key, &header)?;
    set_meta(tx, &index_key, &index)?;
    if revision_number == 1 {
        let page_key = proposal_thread_page_key(&context.thread_id, proposal_sequence);
        if meta(tx, &page_key)?.is_some() {
            return Err(Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "Thread proposal sequence is already indexed",
            ));
        }
        set_meta(
            tx,
            &page_key,
            &json!({"proposal_id":proposal_id,"proposal_sequence":proposal_sequence}),
        )?;
    }
    let stream_id = proposal_stream_id(&proposal_id);
    let source_event_key = format!("revision:{proposal_revision_id}");
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,'coordination.contract_proposed',?4,?5)",
        params![stream_id, source_event_key, operation_id, model::canonical(&revision)?, now],
    )?;
    attach_operation_scope(
        tx,
        operation_id,
        sender.task["task_id"].as_str().unwrap_or_default(),
        sender.attempt["attempt_id"].as_str().unwrap_or_default(),
        &sender.attempt,
    )?;
    Ok(json!({
        "operation_id": operation_id,
        "thread_id": context.thread_id,
        "proposal_id": proposal_id,
        "proposal_revision_id": proposal_revision_id,
        "proposal_digest": request.digest,
        "proposal_sequence": proposal_sequence,
        "revision": revision_number,
        "changed": true,
        "model_work_started": false,
    }))
}

fn respond_contract(
    tx: &Transaction<'_>,
    principal: &Principal,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    let request = crate::coordination::contract::parse_response_request(value)?;
    let (context, sender, author) = contract_thread_context(tx, principal, &request.thread_id)?;
    let header = meta(tx, &proposal_head_key(&request.proposal_id))?
        .ok_or_else(|| Error::new("CONTRACT_PROPOSAL_NOT_FOUND", "proposal is not retained"))?;
    if header["thread_id"] != context.thread_id {
        return Err(Error::new("NOT_FOUND", "proposal is outside this Thread"));
    }
    let revision = meta(
        tx,
        &proposal_revision_key(&request.proposal_id, &request.proposal_revision_id),
    )?
    .ok_or_else(|| {
        Error::new(
            "CONTRACT_PROPOSAL_NOT_FOUND",
            "proposal revision is not retained",
        )
    })?;
    if revision["thread_id"] != context.thread_id
        || revision["proposal_digest"] != request.proposal_digest
    {
        return Err(Error::new(
            "DIGEST_MISMATCH",
            "proposal revision digest or Thread does not match",
        ));
    }
    if revision["task_id"].as_str() != Some(context.task_id.as_str())
        || revision["task_revision"].as_i64() != Some(context.task_revision)
        || revision["attempt_id"].as_str() != Some(context.attempt_id.as_str())
    {
        return Err(Error::new(
            "STALE_THREAD_SCOPE",
            "proposal revision is outside the pinned Thread Task/Attempt",
        ));
    }
    let response_key = proposal_response_key(&request.proposal_id, operation_id);
    if meta(tx, &response_key)?.is_some() {
        return Err(Error::new(
            "COORDINATION_INDEX_CORRUPT",
            "response Operation already has a response record",
        ));
    }
    let mut response = json!({
        "schema_version": 1,
        "record_type": "contract_proposal_response",
        "thread_id": context.thread_id,
        "proposal_id": request.proposal_id,
        "proposal_revision_id": request.proposal_revision_id,
        "proposal_digest": request.proposal_digest,
        "act": request.act.as_str(),
        "objection_basis": request.objection_basis,
        "normalized_objection_basis": request.normalized_objection_basis,
        "material_basis": request.material_basis,
        "classification": if request.material_basis { "material_objection" } else { "no_progress" },
        "reason": request.reason,
        "evidence_refs": request.evidence_refs,
        "task_id": context.task_id,
        "task_revision": context.task_revision,
        "attempt_id": context.attempt_id,
        "sponsor_owner_id": context.sponsor_owner_id,
        "author": author,
        "operation_id": operation_id,
        "recorded_at_ms": now,
    });
    let stream_id = proposal_stream_id(&request.proposal_id);
    let source_event_key = format!("response:{operation_id}");
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,'coordination.contract_response',?4,?5)",
        params![stream_id, source_event_key, operation_id, model::canonical(&response)?, now],
    )?;
    let observation_id = tx.last_insert_rowid();
    response["observation_id"] = json!(observation_id);
    set_meta(tx, &response_key, &response)?;
    let page_key = proposal_response_page_key(
        &request.proposal_id,
        &request.proposal_revision_id,
        observation_id,
    );
    set_meta(
        tx,
        &page_key,
        &json!({"response_key":response_key,"operation_id":operation_id,"observation_id":observation_id}),
    )?;
    attach_operation_scope(
        tx,
        operation_id,
        sender.task["task_id"].as_str().unwrap_or_default(),
        sender.attempt["attempt_id"].as_str().unwrap_or_default(),
        &sender.attempt,
    )?;
    Ok(json!({
        "operation_id": operation_id,
        "thread_id": context.thread_id,
        "proposal_id": request.proposal_id,
        "proposal_revision_id": request.proposal_revision_id,
        "proposal_digest": request.proposal_digest,
        "act": request.act.as_str(),
        "objection_basis": request.objection_basis,
        "material_basis": request.material_basis,
        "response_observation_id": observation_id,
        "changed": true,
        "model_work_started": false,
    }))
}

fn contract_get(db: &Connection, principal: &Principal, value: &Value) -> Result<Value> {
    let request = crate::coordination::contract::parse_get_request(value)?;
    let context = super::coordination_threads::authorize_retained_thread_read(
        db,
        principal,
        &request.thread_id,
    )?;
    if context.topic_kind != "contract" {
        return Err(Error::new(
            "NOT_FOUND",
            "contract proposal is outside this Thread",
        ));
    }
    let header = meta(db, &proposal_head_key(&request.proposal_id))?
        .ok_or_else(|| Error::new("CONTRACT_PROPOSAL_NOT_FOUND", "proposal is not retained"))?;
    if header["thread_id"] != context.thread_id {
        return Err(Error::new("NOT_FOUND", "proposal is outside this Thread"));
    }
    let pointer = meta(
        db,
        &proposal_revision_pointer_key(&request.proposal_revision_id),
    )?
    .ok_or_else(|| {
        Error::new(
            "CONTRACT_PROPOSAL_NOT_FOUND",
            "proposal revision is not retained",
        )
    })?;
    if pointer["proposal_id"] != request.proposal_id || pointer["thread_id"] != context.thread_id {
        return Err(Error::new(
            "NOT_FOUND",
            "proposal revision is outside this Thread",
        ));
    }
    let revision = meta(
        db,
        &proposal_revision_key(&request.proposal_id, &request.proposal_revision_id),
    )?
    .ok_or_else(|| {
        Error::new(
            "COORDINATION_INDEX_CORRUPT",
            "proposal revision pointer has no record",
        )
    })?;
    let prefix = proposal_response_page_prefix(&request.proposal_id, &request.proposal_revision_id);
    let upper = format!("{prefix}g");
    let (lower, comparison) = match request.after_observation_id {
        Some(cursor) => (
            proposal_response_page_key(&request.proposal_id, &request.proposal_revision_id, cursor),
            ">",
        ),
        None => (prefix, ">="),
    };
    let mut statement = db.prepare(&format!(
        "SELECT value_json FROM meta WHERE key {comparison} ?1 AND key < ?2 ORDER BY key LIMIT ?3"
    ))?;
    let rows: Vec<String> = statement
        .query_map(params![lower, upper, request.limit + 1], |row| row.get(0))?
        .collect::<std::result::Result<_, _>>()?;
    let has_more = rows.len() as i64 > request.limit;
    let mut responses = Vec::with_capacity(rows.len().min(request.limit as usize));
    let mut last_observation_id = None;
    for raw in rows.into_iter().take(request.limit as usize) {
        let page: Value = serde_json::from_str(&raw)?;
        let response_key = model::text(&page, "response_key")?;
        let response = meta(db, response_key)?.ok_or_else(|| {
            Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "response page points to a missing record",
            )
        })?;
        if response["thread_id"] != context.thread_id
            || response["proposal_id"] != request.proposal_id
            || response["proposal_revision_id"] != request.proposal_revision_id
            || response["proposal_digest"] != revision["proposal_digest"]
        {
            return Err(Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "response record does not match its proposal revision",
            ));
        }
        last_observation_id = page["observation_id"].as_i64();
        responses.push(response);
    }
    let next_after_observation_id = if has_more { last_observation_id } else { None };
    Ok(json!({
        "thread_id": context.thread_id,
        "proposal_id": request.proposal_id,
        "proposal_revision_id": request.proposal_revision_id,
        "proposal_digest": revision["proposal_digest"],
        "proposal": revision["proposal"],
        "revision_metadata": revision,
        "responses": responses,
        "next_after_observation_id": next_after_observation_id,
        "coverage": if has_more { "partial" } else { "complete" },
    }))
}

fn contract_list(db: &Connection, principal: &Principal, value: &Value) -> Result<Value> {
    let request = crate::coordination::contract::parse_list_request(value)?;
    let context = super::coordination_threads::authorize_retained_thread_read(
        db,
        principal,
        &request.thread_id,
    )?;
    if context.topic_kind != "contract" {
        return Err(Error::new(
            "NOT_FOUND",
            "contract proposals are outside this Thread",
        ));
    }
    let prefix = proposal_thread_page_prefix(&context.thread_id);
    let upper = format!("{prefix}g");
    let (lower, comparison) = match request.after_sequence {
        Some(sequence) => (format!("{prefix}{:020}", sequence), ">"),
        None => (prefix, ">="),
    };
    let mut statement = db.prepare(&format!(
        "SELECT value_json FROM meta WHERE key {comparison} ?1 AND key < ?2 ORDER BY key LIMIT ?3"
    ))?;
    let rows: Vec<String> = statement
        .query_map(params![lower, upper, request.limit + 1], |row| row.get(0))?
        .collect::<std::result::Result<_, _>>()?;
    let has_more = rows.len() as i64 > request.limit;
    let mut items = Vec::with_capacity(rows.len().min(request.limit as usize));
    let mut last_sequence = None;
    for raw in rows.into_iter().take(request.limit as usize) {
        let page: Value = serde_json::from_str(&raw)?;
        let proposal_id = model::text(&page, "proposal_id")?;
        let header = meta(db, &proposal_head_key(proposal_id))?.ok_or_else(|| {
            Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "Thread proposal page points to a missing header",
            )
        })?;
        if header["thread_id"] != context.thread_id
            || header["proposal_sequence"] != page["proposal_sequence"]
        {
            return Err(Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "Thread proposal page does not match its current header",
            ));
        }
        last_sequence = page["proposal_sequence"].as_i64();
        items.push(json!({
            "proposal_id": header["proposal_id"],
            "thread_id": header["thread_id"],
            "topic": header["topic"],
            "latest_revision_id": header["latest_revision_id"],
            "latest_digest": header["latest_digest"],
            "revision_count": header["revision_count"],
            "proposal_sequence": header["proposal_sequence"],
            "sponsor_owner_id": header["sponsor_owner_id"],
            "updated_at_ms": header["updated_at_ms"],
        }));
    }
    Ok(json!({
        "thread_id": context.thread_id,
        "items": items,
        "next_after_sequence": if has_more { last_sequence } else { None },
        "coverage": if has_more { "partial" } else { "complete" },
    }))
}

fn authorize_contract_operation_read(
    db: &Connection,
    principal: &Principal,
    operation_id: &str,
) -> Result<Option<Value>> {
    let raw: Option<(String, String, String, Option<String>, Option<String>, Option<String>, Option<i64>, String, Option<String>)> = db.query_row(
        "SELECT caller_id,method,state,task_id,attempt_id,binding_id,binding_generation,original_request_json,result_json FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?)),
    ).optional()?;
    let Some((
        caller_id,
        method,
        state,
        task_id,
        attempt_id,
        binding_id,
        binding_generation,
        original_request,
        result_json,
    )) = raw
    else {
        return Ok(None);
    };
    if !matches!(
        method.as_str(),
        "coordination.contract.propose" | "coordination.contract.respond"
    ) {
        return Ok(None);
    }
    // A caller may read its own generic rejection receipt even when the
    // supplied Thread was invalid, inaccessible, or later became unreadable.
    // In that case no Thread-derived Task scope is exposed by this API.
    if caller_id == principal.client_id {
        return Ok(Some(operations::get_operation(db, operation_id)?));
    }
    if state != "settled" {
        return Err(Error::new(
            "NOT_FOUND",
            "rejected contract Operation is visible only to its caller",
        ));
    }
    super::coordination_threads::authorize_operation_read(db, principal, operation_id).map_err(
        |_| {
            Error::new(
                "NOT_FOUND",
                "Operation is outside the retained Thread scope",
            )
        },
    )?;
    let request: Value = serde_json::from_str(&original_request)?;
    let thread_id = request
        .get("thread_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            Error::new(
                "NOT_FOUND",
                "contract Operation has no retained Thread scope",
            )
        })?;
    let context =
        super::coordination_threads::authorize_retained_thread_read(db, principal, thread_id)
            .map_err(|_| {
                Error::new(
                    "NOT_FOUND",
                    "Operation is outside the retained Thread scope",
                )
            })?;
    if context.topic_kind != "contract"
        || task_id.as_deref().is_some_and(|id| id != context.task_id)
        || attempt_id
            .as_deref()
            .is_some_and(|id| id != context.attempt_id)
    {
        return Err(Error::new(
            "NOT_FOUND",
            "Operation is outside the retained Thread scope",
        ));
    }
    super::coordination_threads::require_thread_participant(&context, principal).map_err(|_| {
        Error::new(
            "NOT_FOUND",
            "Operation is outside the retained Thread roster",
        )
    })?;
    let author = context
        .participants
        .iter()
        .find(|participant| {
            participant.client_id == caller_id
                && participant.scope.get("binding_id").and_then(Value::as_str)
                    == binding_id.as_deref()
                && participant
                    .scope
                    .get("binding_generation")
                    .and_then(Value::as_i64)
                    == binding_generation
        })
        .ok_or_else(|| {
            Error::new(
                "NOT_FOUND",
                "Operation author is outside the retained Thread roster",
            )
        })?;
    let result: Value = serde_json::from_str(result_json.as_deref().ok_or_else(|| {
        Error::new(
            "COORDINATION_INDEX_CORRUPT",
            "settled contract Operation has no result",
        )
    })?)?;
    match method.as_str() {
        "coordination.contract.propose" => {
            let proposal_id = model::text(&result, "proposal_id")?;
            let revision_id = model::text(&result, "proposal_revision_id")?;
            let revision =
                meta(db, &proposal_revision_key(proposal_id, revision_id))?.ok_or_else(|| {
                    Error::new(
                        "COORDINATION_INDEX_CORRUPT",
                        "settled proposal Operation has no revision",
                    )
                })?;
            let pointer =
                meta(db, &proposal_revision_pointer_key(revision_id))?.ok_or_else(|| {
                    Error::new(
                        "COORDINATION_INDEX_CORRUPT",
                        "settled proposal Operation has no revision pointer",
                    )
                })?;
            if pointer["proposal_id"] != proposal_id
                || pointer["thread_id"] != context.thread_id
                || pointer["revision"] != result["revision"]
                || revision["operation_id"] != operation_id
                || revision["proposal_id"] != proposal_id
                || revision["proposal_revision_id"] != revision_id
                || revision["revision"] != result["revision"]
                || revision["proposal_digest"] != result["proposal_digest"]
                || revision["thread_id"] != context.thread_id
                || revision["task_id"] != context.task_id
                || revision["task_revision"] != context.task_revision
                || revision["attempt_id"] != context.attempt_id
                || revision["author"]["client_id"] != caller_id
                || revision["author"]["role"] != author.role
                || revision["author"]["generation"].as_i64() != author.generation
                || revision["author"]["registration_fingerprint"] != author.registration_fingerprint
                || revision["author"]["actor"] != author.actor
                || revision["author"]["scope"] != author.scope
            {
                return Err(Error::new(
                    "NOT_FOUND",
                    "Operation is outside the retained Thread scope",
                ));
            }
        }
        "coordination.contract.respond" => {
            let proposal_id = model::text(&result, "proposal_id")?;
            let response = meta(db, &proposal_response_key(proposal_id, operation_id))?
                .ok_or_else(|| {
                    Error::new(
                        "COORDINATION_INDEX_CORRUPT",
                        "settled response Operation has no response record",
                    )
                })?;
            if response["operation_id"] != operation_id
                || response["thread_id"] != context.thread_id
                || response["proposal_id"] != proposal_id
                || response["proposal_revision_id"] != result["proposal_revision_id"]
                || response["proposal_digest"] != result["proposal_digest"]
                || response["act"] != result["act"]
                || response["objection_basis"] != result["objection_basis"]
                || response["material_basis"] != result["material_basis"]
                || response["observation_id"] != result["response_observation_id"]
                || response["task_id"] != context.task_id
                || response["task_revision"] != context.task_revision
                || response["attempt_id"] != context.attempt_id
                || response["author"]["client_id"] != caller_id
                || response["author"]["role"] != author.role
                || response["author"]["generation"].as_i64() != author.generation
                || response["author"]["registration_fingerprint"] != author.registration_fingerprint
                || response["author"]["actor"] != author.actor
                || response["author"]["scope"] != author.scope
            {
                return Err(Error::new(
                    "NOT_FOUND",
                    "Operation is outside the retained Thread scope",
                ));
            }
        }
        _ => unreachable!(),
    }
    Ok(Some(operations::get_operation(db, operation_id)?))
}

fn proposal_head_key(proposal_id: &str) -> String {
    format!("coordination:proposal:{proposal_id}")
}

fn proposal_revision_key(proposal_id: &str, revision_id: &str) -> String {
    format!("coordination:proposal:{proposal_id}:revision:{revision_id}")
}

fn proposal_revision_pointer_key(revision_id: &str) -> String {
    format!("coordination:proposal-revision:{revision_id}")
}

fn proposal_response_key(proposal_id: &str, operation_id: &str) -> String {
    format!("coordination:proposal:{proposal_id}:response:{operation_id}")
}

fn proposal_stream_id(proposal_id: &str) -> String {
    format!("coordination:proposal:{proposal_id}")
}

fn proposal_thread_sequence_key(thread_id: &str) -> String {
    format!("coordination:proposal-thread-sequence:{thread_id}")
}

fn proposal_thread_index_key(thread_id: &str, proposal_id: &str) -> String {
    format!("coordination:proposal-thread:{thread_id}:{proposal_id}")
}

fn proposal_thread_page_prefix(thread_id: &str) -> String {
    format!("coordination:proposal-thread-page:{thread_id}:")
}

fn proposal_thread_page_key(thread_id: &str, sequence: i64) -> String {
    format!("{}{:020}", proposal_thread_page_prefix(thread_id), sequence)
}

fn proposal_response_page_prefix(proposal_id: &str, revision_id: &str) -> String {
    format!("coordination:proposal-response:{proposal_id}:{revision_id}:")
}

fn proposal_response_page_key(proposal_id: &str, revision_id: &str, observation_id: i64) -> String {
    format!(
        "{}{:020}",
        proposal_response_page_prefix(proposal_id, revision_id),
        observation_id
    )
}
