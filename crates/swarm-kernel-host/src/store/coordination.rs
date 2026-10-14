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
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
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
const LEGACY_INDEX_GAP_LIMIT: usize = 256;
/// Bound on legacy mailbox locators examined per resumable normalization pass.
/// Inbox items stay withheld until the entire legacy range has been examined.
const INBOX_NORMALIZE_BOUND: i64 = 64;

#[derive(Clone)]
pub(super) struct ScopeData {
    pub(super) client_id: Option<String>,
    pub(super) registration: Value,
    pub(super) task: Value,
    pub(super) attempt: Value,
    pub(super) scope_id: String,
}

#[derive(Clone)]
struct IndexedCard {
    client_id: String,
    card_kind: String,
    identity: String,
    index_key: String,
    scope_id: Option<String>,
    card_revision: Option<i64>,
    valid_index: bool,
}

struct IndexedCardPage {
    cards: Vec<IndexedCard>,
    has_more: bool,
    stale_entries: usize,
    last_scanned_key: Option<String>,
}

struct IndexedParticipantPage {
    client_ids: Vec<String>,
    has_more: bool,
    stale_entries: usize,
    last_scanned_key: Option<String>,
}

#[derive(Clone)]
enum PeerSelector {
    Term(keys::TermKind, String),
    Task(String),
    Participant(String),
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

/// Canonical current endpoint identity for internal membership and ACK checks.
/// Public registration projections intentionally omit the metadata-key client
/// identity, so callers must not reconstruct this context from that projection.
pub(super) fn current_scope_context(db: &Connection, principal: &Principal) -> Result<Value> {
    principal.require_participant()?;
    let scope = load_current_scope_for_client(db, &principal.client_id)?;
    Ok(scope_context(&scope))
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
    let indexed = indexed_global_cards(
        db,
        keys::TermKind::Contract,
        contract_key,
        Some("contract"),
        None,
        scan_limit,
    )?;
    let mut items = Vec::new();
    let mut stale = indexed.stale_entries;
    let mut overflow = false;
    for index in indexed.cards {
        if !index.valid_index {
            stale += 1;
            continue;
        }
        let member = match load_current_scope_for_client(db, &index.client_id) {
            Ok(member) => member,
            Err(error) if is_stale_watch_authority(&error) => {
                stale += 1;
                continue;
            }
            Err(error) => return Err(error),
        };
        let Some(card) = current_card(
            db,
            &member.scope_id,
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
            || card["task_id"] != member.task["task_id"]
            || card["task_revision"] != member.task["revision"]
            || card["attempt_id"] != member.attempt["attempt_id"]
            || card["card_kind"] != "contract"
            || index.scope_id.as_deref() != Some(member.scope_id.as_str())
            || index.card_revision != card["card_revision"].as_i64()
        {
            stale += 1;
            continue;
        }
        let relation = if index.client_id == principal.client_id {
            Some(json!({"kind":"self","context":scope_context(&member)}))
        } else {
            current_peer_relation(
                db,
                &caller,
                &member,
                Some(&PeerSelector::Term(
                    keys::TermKind::Contract,
                    contract_key.to_owned(),
                )),
            )?
        };
        let Some(relation) = relation else {
            continue;
        };
        if items.len() == limit as usize {
            overflow = true;
            break;
        }
        items.push(json!({
            "client_id":index.client_id,
            "participation_basis":member.registration["participation_basis"],
            "context":scope_context(&member),
            "participant":{
                "display_alias":member.registration["display_alias"],
                "inbound_policy":member.registration["inbound_policy"],
            },
            "relation":relation,
            "card":card,
        }));
    }
    let migration_gap = legacy_index_backfill_gap_for(
        db,
        &[],
        Some((keys::TermKind::Contract, contract_key)),
        &[],
    )?;
    let incomplete = indexed.has_more || overflow || stale > 0 || migration_gap.is_some();
    let mut gaps = Vec::new();
    if stale > 0 {
        gaps.push(json!({"kind":"stale_or_corrupt_contract_relevance","count":stale}));
    }
    if indexed.has_more || overflow {
        gaps.push(json!({"kind":"bounded_contract_relevance","more":indexed.has_more || overflow}));
    }
    if let Some(gap) = migration_gap {
        gaps.push(gap);
    }
    Ok(json!({
        "scope_id":caller.scope_id,
        "task_id":caller.task["task_id"],"task_revision":caller.task["revision"],"attempt_id":caller.attempt["attempt_id"],
        "items":items,"coverage":if incomplete { "partial" } else { "complete" },
        "gaps":gaps,
    }))
}

/// Manager-side freshness uses the same sparse exact-contract directory and
/// pair validator while retaining each participant's own current context.
/// The manager's Task scope authorizes the read; it does not become a cell
/// member or an acknowledgement obligation.
pub(super) fn list_current_contract_participants_for_manager(
    db: &Connection,
    principal: &Principal,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    contract_key: &str,
    limit: i64,
) -> Result<Value> {
    validate_identifier(contract_key, "contract_key", 1024)?;
    if !(1..=keys::MAX_PAGE_SIZE).contains(&limit) {
        return Err(Error::invalid("contract participant limit must be 1..=50"));
    }
    let caller = manager_scope(db, principal, task_id, task_revision, attempt_id)?;
    let scan_limit = (limit * RELEVANCE_SCAN_FACTOR + 32).min(keys::MAX_INBOX_SCAN);
    let indexed = indexed_global_cards(
        db,
        keys::TermKind::Contract,
        contract_key,
        Some("contract"),
        None,
        scan_limit,
    )?;
    let mut candidates = Vec::<(ScopeData, Value)>::new();
    let mut seen = BTreeSet::new();
    let mut stale = indexed.stale_entries;
    let mut overflow = false;
    for index in indexed.cards {
        if !index.valid_index {
            stale = stale.saturating_add(1);
            continue;
        }
        let member = match load_current_scope_for_client(db, &index.client_id) {
            Ok(member) => member,
            Err(error) if is_stale_watch_authority(&error) => {
                stale = stale.saturating_add(1);
                continue;
            }
            Err(error) => return Err(error),
        };
        let Some(card) = current_card(
            db,
            &member.scope_id,
            "contract",
            contract_key,
            &index.client_id,
        )?
        else {
            stale = stale.saturating_add(1);
            continue;
        };
        if card["identity"] != contract_key
            || card["client_id"] != index.client_id
            || card["task_id"] != member.task["task_id"]
            || card["task_revision"] != member.task["revision"]
            || card["attempt_id"] != member.attempt["attempt_id"]
            || card["card_kind"] != "contract"
            || index.scope_id.as_deref() != Some(member.scope_id.as_str())
            || index.card_revision != card["card_revision"].as_i64()
            || !matches!(
                card.pointer("/fields/role").and_then(Value::as_str),
                Some("producer" | "consumer" | "carrier")
            )
        {
            stale = stale.saturating_add(1);
            continue;
        }
        if !seen.insert(index.client_id.clone()) {
            continue;
        }
        if let Some((anchor, _)) = candidates.first()
            && current_peer_relation(
                db,
                anchor,
                &member,
                Some(&PeerSelector::Term(
                    keys::TermKind::Contract,
                    contract_key.to_owned(),
                )),
            )?
            .is_none()
        {
            continue;
        }
        if candidates.len() == limit as usize {
            overflow = true;
            break;
        }
        candidates.push((member, card));
    }
    let items = candidates
        .iter()
        .map(|(member, card)| {
            Ok(json!({
                "client_id":member.client_id,
                "participation_basis":member.registration["participation_basis"],
                "context":scope_context(member),
                "card":project_card(card, None)?,
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let migration_gap = legacy_index_backfill_gap_for(
        db,
        &[],
        Some((keys::TermKind::Contract, contract_key)),
        &[],
    )?;
    let incomplete = indexed.has_more || overflow || stale > 0 || migration_gap.is_some();
    let mut gaps = Vec::new();
    if stale > 0 {
        gaps.push(json!({"kind":"stale_or_corrupt_contract_relevance","count":stale}));
    }
    if indexed.has_more || overflow {
        gaps.push(json!({"kind":"bounded_contract_relevance","more":indexed.has_more || overflow}));
    }
    if let Some(gap) = migration_gap {
        gaps.push(gap);
    }
    Ok(json!({
        "scope_id":caller.scope_id,
        "task_id":caller.task["task_id"],
        "task_revision":caller.task["revision"],
        "attempt_id":caller.attempt["attempt_id"],
        "items":items,
        "coverage":if incomplete { "partial" } else { "complete" },
        "gaps":gaps,
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
            || gm::read_current(db)?.is_some_and(|current| current.client_id == creator_id)),
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
                    match verify_review_assignment(
                        db,
                        creator_id,
                        &registration,
                        scope,
                        ReviewAssignmentVerification::Current,
                    ) {
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
                let is_current_gm =
                    gm::read_current(db)?.is_some_and(|current| current.client_id == creator_id);
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
pub(super) fn bind_review_assignment(
    tx: &Transaction<'_>,
    reviewer_client_id: &str,
    review_assignment_id: &str,
    sponsor_client_id: &str,
    exact_scope: &Value,
    replacement_class: Option<super::reviews::ReviewReplacementClass>,
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
    validate_pending_review_tuple_with_replacement(tx, &scope, replacement_class)?;
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
        ReviewAssignmentVerification::Current,
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
    verify_review_assignment(
        db,
        &principal.client_id,
        &registration,
        &scope,
        ReviewAssignmentVerification::HistoricalFirstResult,
    )
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
    let Some(relation) = current_peer_relation(db, &sender, &target, None)? else {
        return Err(Error::new(
            "FORBIDDEN",
            "coordination delivery requires an exact current peer relation",
        ));
    };
    let body = value
        .get("body")
        .filter(|body| !body.is_null())
        .ok_or_else(|| Error::invalid("body must be a non-null JSON value"))?;
    let envelope = json!({
        "schema": "eliot.coordination.message.v2",
        "source_context": scope_context(&sender),
        "target_context": scope_context(&target),
        "relation": relation,
        // Kept as the source tuple for retained Operation projections. V2
        // consumers use the separate source and target contexts above.
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
        "coordination.contract.ratify" | "coordination.contract.reject" => {
            super::coordination_threads::apply_contract_decision(
                tx,
                principal,
                method,
                value,
                operation_id,
                now,
            )
            .map(|value| (value, false))
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

pub(super) fn load_current_scope_for_client(db: &Connection, client_id: &str) -> Result<ScopeData> {
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
                verify_review_assignment(
                    db,
                    client_id,
                    registration,
                    &scope,
                    ReviewAssignmentVerification::Current,
                )?;
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
    set_meta(
        tx,
        &keys::participant_task_key(task_id, client_id),
        &json!({
            "client_id":client_id,
            "task_id":task_id,
            "task_revision":task_revision,
            "attempt_id":attempt_id,
            "grant_revision":1,
        }),
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
        tx.execute(
            "DELETE FROM meta WHERE key=?1",
            [keys::participant_task_key(&task_id, client_id)],
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
    validate_pending_review_tuple_with_replacement(db, scope, None)
}

fn validate_pending_review_tuple_with_replacement(
    db: &Connection,
    scope: &Value,
    replacement_class: Option<super::reviews::ReviewReplacementClass>,
) -> Result<()> {
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
    let current_returned_candidate = matches!(
        replacement_class,
        Some(super::reviews::ReviewReplacementClass::ReturnedForCorrection)
    ) && attempt["state"] == "needs_correction";
    if (attempt["state"] != "submitted" && !current_returned_candidate)
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

/// Exact purpose of one review-assignment verification.
///
/// `Current` is the strict live authority used by packet/context, ordinary
/// review actions, watch authority and participation-basis checks: the live
/// review:slot pointer must name this exact assignment.
/// `HistoricalFirstResult` is reserved for the retained-assignment result
/// path, where an exact immutable assignment may already have been retired
/// by a replacement before its first result was written.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
enum ReviewAssignmentVerification {
    Current,
    HistoricalFirstResult,
}

fn verify_review_assignment(
    db: &Connection,
    reviewer_client_id: &str,
    registration: &Value,
    scope: &Value,
    purpose: ReviewAssignmentVerification,
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
    let lives_current_slot =
        current_slot.is_some_and(|slot| slot["review_assignment_id"] == assignment_id);
    // Purpose-gated retained-assignment exception. Only the exact
    // HistoricalFirstResult caller may let an already-retired assignment pass,
    // and only while that assignment has no retained result yet. Every Current
    // caller keeps the strict live-slot requirement.
    let historical_first_result_allowed = !lives_current_slot
        && !retained_review_result_exists(db, assignment_id)?
        && purpose == ReviewAssignmentVerification::HistoricalFirstResult;
    if !lives_current_slot && !historical_first_result_allowed {
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

/// Whether this exact assignment already retained a review result observation.
///
/// This is the one-result-per-assignment guard for the purpose-specific
/// retained-assignment relaxation in `verify_review_assignment`. It reads only
/// retained `review.result` existence, so the semantic-input comparison and
/// `REVIEW_RESULT_CONFLICT` refusal in `reviews::reserve_submit` remain the sole
/// authority for what a second result may contain.
fn retained_review_result_exists(db: &Connection, assignment_id: &str) -> Result<bool> {
    let event_key = format!("result:{assignment_id}");
    let row: Option<i64> = db
        .query_row(
            "SELECT 1 FROM observations WHERE source_stream_id=?1 AND source_event_key=?2 \
             AND kind='review.result'",
            params!["controller:review", event_key],
            |row| row.get(0),
        )
        .optional()?;
    Ok(row.is_some())
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

fn decode_index_component(encoded: &str) -> Option<String> {
    if encoded.is_empty() || !encoded.len().is_multiple_of(2) {
        return None;
    }
    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().as_chunks::<2>().0 {
        let high = lower_hex_nibble(pair[0])?;
        let low = lower_hex_nibble(pair[1])?;
        bytes.push((high << 4) | low);
    }
    let value = String::from_utf8(bytes).ok()?;
    (keys::key_component(&value) == encoded).then_some(value)
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
    let client_id = decode_index_component(encoded).ok_or_else(|| {
        Error::new(
            "COORDINATION_INDEX_CORRUPT",
            "participant index key has a non-canonical client component",
        )
    })?;
    if client_id.len() > 256 {
        return Err(Error::new(
            "COORDINATION_INDEX_CORRUPT",
            "participant index identity cannot be represented by after_client_id",
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
        // Heal the derived exact-term indexes when a retained card predates
        // the cross-Task directory projection.
        write_relevance_indexes(
            tx,
            &scope.scope_id,
            &principal.client_id,
            card_kind,
            identity,
            prior["card_revision"].as_i64().unwrap_or(1),
            fields,
        )?;
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

/// Advance one bounded page of the namespace-level legacy directory repair.
/// The cursor walks canonical registrations and current-card records only;
/// every candidate is revalidated against its own current Task, Attempt,
/// participant actor and grant before a derived index is written.
pub(super) fn reconcile_legacy_index_backfill(tx: &Transaction<'_>) -> Result<()> {
    let mut marker = match meta(tx, keys::LEGACY_INDEX_BACKFILL_KEY)? {
        Some(marker) => marker,
        None => json!({
            "version":1,
            "phase":"participants",
            "after_key":null,
            "scanned":0,
            "anomaly_count":0,
            "gaps":[],
            "gaps_overflow":false,
            "scan_complete":false,
            "complete":false,
            "coverage":"partial",
        }),
    };
    if marker["version"] != 1 {
        return Err(Error::new(
            "COORDINATION_INDEX_MARKER_DAMAGED",
            "legacy coordination index marker has an unsupported version",
        ));
    }
    if marker["scanned"].as_u64().is_none() || marker["anomaly_count"].as_u64().is_none() {
        return Err(Error::new(
            "COORDINATION_INDEX_MARKER_DAMAGED",
            "legacy coordination index marker has invalid counters",
        ));
    }
    if marker["scan_complete"] == true {
        if marker["complete"] == true {
            return Ok(());
        }
        // A partial pass is a current snapshot, not a permanent verdict.
        // Revisit it in bounded batches so repaired source rows can recover.
        marker["phase"] = json!("participants");
        marker["after_key"] = Value::Null;
        marker["scanned"] = json!(0);
        marker["anomaly_count"] = json!(0);
        marker["gaps"] = json!([]);
        marker["gaps_overflow"] = json!(false);
        marker["scan_complete"] = json!(false);
        marker["complete"] = json!(false);
        marker["coverage"] = json!("partial");
    }
    let phase = marker["phase"]
        .as_str()
        .ok_or_else(|| {
            Error::new(
                "COORDINATION_INDEX_MARKER_DAMAGED",
                "legacy coordination index marker has no phase",
            )
        })?
        .to_owned();
    let (prefix, upper) = match phase.as_str() {
        "participants" => ("client:", "client;"),
        "cards" => ("coordination:card:", "coordination:card;"),
        _ => {
            return Err(Error::new(
                "COORDINATION_INDEX_MARKER_DAMAGED",
                "legacy coordination index marker has an unsupported phase",
            ));
        }
    };
    let after = marker
        .get("after_key")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if after.as_deref().is_some_and(|key| !key.starts_with(prefix)) {
        return Err(Error::new(
            "COORDINATION_INDEX_MARKER_DAMAGED",
            "legacy coordination index cursor is outside its namespace",
        ));
    }
    let rows: Vec<(String, String)> = {
        let mut statement = tx.prepare(
            "SELECT key,value_json FROM meta \
             WHERE key>=?1 AND key<?2 AND (?3 IS NULL OR key>?3) \
             ORDER BY key LIMIT ?4",
        )?;
        statement
            .query_map(
                params![prefix, upper, after, keys::LEGACY_INDEX_BACKFILL_BATCH + 1],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?
            .collect::<std::result::Result<_, _>>()?
    };
    let has_more = rows.len() as i64 > keys::LEGACY_INDEX_BACKFILL_BATCH;
    let page = rows
        .iter()
        .take(keys::LEGACY_INDEX_BACKFILL_BATCH as usize)
        .collect::<Vec<_>>();
    for (key, raw) in &page {
        marker["scanned"] = json!(
            marker["scanned"]
                .as_u64()
                .unwrap_or_default()
                .saturating_add(1)
        );
        match phase.as_str() {
            "participants" => backfill_participant_task_hint(tx, key, raw, &mut marker)?,
            "cards" => backfill_current_card_hints(tx, key, raw, &mut marker)?,
            _ => unreachable!("the phase was validated above"),
        }
    }
    if has_more {
        marker["after_key"] = page
            .last()
            .map(|(key, _)| json!(key))
            .unwrap_or(Value::Null);
    } else if phase == "participants" {
        marker["phase"] = json!("cards");
        marker["after_key"] = Value::Null;
    } else {
        marker["phase"] = json!("complete");
        marker["after_key"] = Value::Null;
        marker["scan_complete"] = json!(true);
        let complete = marker["anomaly_count"].as_u64() == Some(0);
        marker["complete"] = json!(complete);
        marker["coverage"] = json!(if complete { "complete" } else { "partial" });
    }
    set_meta(tx, keys::LEGACY_INDEX_BACKFILL_KEY, &marker)
}

pub(super) fn legacy_index_backfill_status(db: &Connection) -> Result<Value> {
    Ok(
        meta(db, keys::LEGACY_INDEX_BACKFILL_KEY)?.unwrap_or_else(|| {
            json!({
                "version":1,
                "phase":"participants",
                "scanned":0,
                "anomaly_count":0,
                "gaps":[],
                "gaps_overflow":false,
                "scan_complete":false,
                "complete":false,
                "coverage":"partial",
            })
        }),
    )
}

pub(super) fn legacy_index_backfill_gap_for(
    db: &Connection,
    task_ids: &[&str],
    term: Option<(keys::TermKind, &str)>,
    client_ids: &[&str],
) -> Result<Option<Value>> {
    let status = legacy_index_backfill_status(db)?;
    if status["version"] != 1
        || status["phase"] != "complete"
        || status["scan_complete"] != true
        || status["scanned"].as_u64().is_none()
        || !status["after_key"].is_null()
    {
        return Ok(Some(json!({
            "kind":"legacy_coordination_index_backfill",
            "phase":status["phase"],
            "anomaly_count":status["anomaly_count"],
        })));
    }
    if status["complete"] == true
        && status["coverage"] == "complete"
        && status["anomaly_count"].as_u64() == Some(0)
    {
        return Ok(None);
    }
    let Some(gaps) = status["gaps"].as_array() else {
        // Older partial markers only retained a count. Keep them fail-closed
        // until a fresh bounded pass reconstructs current scoped gaps.
        return Ok(Some(json!({
            "kind":"legacy_coordination_index_backfill",
            "phase":status["phase"],
            "anomaly_count":status["anomaly_count"],
        })));
    };
    if status["gaps_overflow"] == true
        || status["anomaly_count"].as_u64() != Some(gaps.len() as u64)
    {
        return Ok(Some(json!({
            "kind":"legacy_coordination_index_backfill",
            "phase":status["phase"],
            "anomaly_count":status["anomaly_count"],
        })));
    }
    let relevant = gaps
        .iter()
        .filter(|gap| legacy_index_gap_matches(gap, task_ids, term, client_ids))
        .count();
    if relevant == 0 {
        return Ok(None);
    }
    Ok(Some(json!({
        "kind":"legacy_coordination_index_backfill",
        "phase":"complete",
        "anomaly_count":relevant,
    })))
}

fn legacy_index_gap_matches(
    gap: &Value,
    task_ids: &[&str],
    term: Option<(keys::TermKind, &str)>,
    client_ids: &[&str],
) -> bool {
    if gap["global"] == true {
        return true;
    }
    if let Some((kind, value)) = term {
        let exact_term = gap["terms"].as_array().is_some_and(|terms| {
            terms
                .iter()
                .any(|entry| entry["term_kind"] == kind.as_str() && entry["term"] == value)
        });
        let unknown_term_kind = gap["all_term_kinds"]
            .as_array()
            .is_some_and(|kinds| kinds.iter().any(|entry| entry == kind.as_str()));
        return exact_term || unknown_term_kind;
    }
    if task_ids.is_empty() && client_ids.is_empty() {
        return true;
    }
    let matches_task = gap["task_id"]
        .as_str()
        .is_some_and(|task_id| task_ids.contains(&task_id));
    let matches_client = gap["client_id"]
        .as_str()
        .is_some_and(|client_id| client_ids.contains(&client_id));
    let unknown_participant_task = gap["source_kind"] == "participant"
        && gap["task_id"].as_str().is_none()
        && !task_ids.is_empty();
    matches_task || matches_client || unknown_participant_task
}

fn backfill_participant_task_hint(
    tx: &Transaction<'_>,
    key: &str,
    raw: &str,
    marker: &mut Value,
) -> Result<()> {
    let client_id = key.strip_prefix("client:").unwrap_or_default();
    let registration: Value = match serde_json::from_str(raw) {
        Ok(registration) => registration,
        Err(_) => {
            mark_legacy_participant_gap(marker, key, None, Some(client_id));
            return Ok(());
        }
    };
    if client_id.is_empty() {
        mark_legacy_participant_gap(marker, key, Some(&registration), None);
        return Ok(());
    }
    let role: Role = match serde_json::from_value(registration["role"].clone()) {
        Ok(role) => role,
        Err(_) => {
            mark_legacy_participant_gap(marker, key, Some(&registration), Some(client_id));
            return Ok(());
        }
    };
    if role != Role::Participant {
        return Ok(());
    }
    let Some(disabled) = registration["disabled"].as_bool() else {
        mark_legacy_participant_gap(marker, key, Some(&registration), Some(client_id));
        return Ok(());
    };
    if disabled {
        return Ok(());
    }
    match load_current_scope_for_client(tx, client_id) {
        Ok(scope) => {
            if scope.client_id.as_deref() != Some(client_id) {
                mark_legacy_participant_gap(marker, key, Some(&registration), Some(client_id));
                return Ok(());
            }
            let task_id = scope.task["task_id"].as_str().unwrap_or_default();
            let task_revision = scope.task["revision"].as_i64().unwrap_or_default();
            let attempt_id = scope.attempt["attempt_id"].as_str().unwrap_or_default();
            let Some(grant_revision) = scope.registration["grant_revision"]
                .as_i64()
                .filter(|revision| *revision > 0)
            else {
                mark_legacy_participant_gap(marker, key, Some(&registration), Some(client_id));
                return Ok(());
            };
            let index_key = keys::participant_task_key(task_id, client_id);
            // Canonical namespace helper is the single source of truth for
            // derived-index scans; keep the backfill write inside it.
            debug_assert!(index_key.starts_with(keys::participant_task_namespace_prefix()));
            let expected = json!({
                "client_id":client_id,
                "task_id":task_id,
                "task_revision":task_revision,
                "attempt_id":attempt_id,
                "grant_revision":grant_revision,
            });
            mark_if_derived_hint_differs(tx, marker, &index_key, &expected)?;
            set_meta(tx, &index_key, &expected)
        }
        Err(error) if is_stale_watch_authority(&error) => Ok(()),
        Err(error) if error.code == "INVALID_PARAMS" => {
            mark_legacy_participant_gap(marker, key, Some(&registration), Some(client_id));
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn mark_if_derived_hint_differs(
    tx: &Transaction<'_>,
    marker: &mut Value,
    key: &str,
    expected: &Value,
) -> Result<()> {
    let raw: Option<String> = tx
        .query_row("SELECT value_json FROM meta WHERE key=?1", [key], |row| {
            row.get(0)
        })
        .optional()?;
    if let Some(raw) = raw {
        match serde_json::from_str::<Value>(&raw) {
            Ok(value) if value == *expected => {}
            _ => mark_legacy_participant_gap(
                marker,
                key,
                Some(expected),
                expected.get("client_id").and_then(Value::as_str),
            ),
        }
    }
    Ok(())
}

fn backfill_current_card_hints(
    tx: &Transaction<'_>,
    key: &str,
    raw: &str,
    marker: &mut Value,
) -> Result<()> {
    let card: Value = match serde_json::from_str(raw) {
        Ok(card) => card,
        Err(_) => {
            mark_legacy_card_gap(marker, key, None);
            return Ok(());
        }
    };
    match card["state"].as_str() {
        Some("current") => {}
        Some("withdrawn") => return Ok(()),
        _ => {
            mark_legacy_card_gap(marker, key, Some(&card));
            return Ok(());
        }
    }
    let Some(client_id) = card["client_id"].as_str() else {
        mark_legacy_card_gap(marker, key, Some(&card));
        return Ok(());
    };
    let Some(card_kind) = card["card_kind"].as_str() else {
        mark_legacy_card_gap(marker, key, Some(&card));
        return Ok(());
    };
    let Some(identity) = card["identity"].as_str() else {
        mark_legacy_card_gap(marker, key, Some(&card));
        return Ok(());
    };
    let Some(task_id) = card["task_id"].as_str() else {
        mark_legacy_card_gap(marker, key, Some(&card));
        return Ok(());
    };
    let Some(task_revision) = card["task_revision"].as_i64().filter(|value| *value > 0) else {
        mark_legacy_card_gap(marker, key, Some(&card));
        return Ok(());
    };
    let Some(attempt_id) = card["attempt_id"].as_str() else {
        mark_legacy_card_gap(marker, key, Some(&card));
        return Ok(());
    };
    let Some(card_revision) = card["card_revision"].as_i64().filter(|value| *value > 0) else {
        mark_legacy_card_gap(marker, key, Some(&card));
        return Ok(());
    };
    let Some(material_digest) = card["material_digest"].as_str() else {
        mark_legacy_card_gap(marker, key, Some(&card));
        return Ok(());
    };
    let Some(fields) = card.get("fields").filter(|fields| fields.is_object()) else {
        mark_legacy_card_gap(marker, key, Some(&card));
        return Ok(());
    };
    if !matches!(card_kind, "work" | "contract")
        || (card_kind == "work" && identity != "work")
        || (card_kind == "contract" && validate_identifier(identity, "contract_key", 256).is_err())
        || keys::validate_card_fields(card_kind, fields).is_err()
        || model::digest(model::canonical(fields)?.as_bytes()) != material_digest
    {
        mark_legacy_card_gap(marker, key, Some(&card));
        return Ok(());
    }
    let scope = match load_current_scope_for_client(tx, client_id) {
        Ok(scope) => scope,
        Err(error) if is_stale_watch_authority(&error) => return Ok(()),
        Err(error) if error.code == "INVALID_PARAMS" => {
            mark_legacy_card_gap(marker, key, Some(&card));
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    if scope.client_id.as_deref() != Some(client_id)
        || scope.task["task_id"] != task_id
        || scope.task["revision"] != task_revision
        || scope.attempt["attempt_id"] != attempt_id
    {
        // The record belongs to a prior Task/Attempt projection. It is not a
        // current card and therefore does not make the current index incomplete.
        return Ok(());
    }
    let scope_id = scope.scope_id;
    if key != keys::card_key(&scope_id, card_kind, identity, client_id) {
        mark_legacy_card_gap(marker, key, Some(&card));
        return Ok(());
    }
    write_relevance_indexes(
        tx,
        &scope_id,
        client_id,
        card_kind,
        identity,
        card_revision,
        fields,
    )
}

fn mark_legacy_participant_gap(
    marker: &mut Value,
    key: &str,
    registration: Option<&Value>,
    client_id: Option<&str>,
) {
    let client_id = client_id
        .filter(|client_id| !client_id.is_empty())
        .or_else(|| {
            key.strip_prefix("client:")
                .filter(|client_id| !client_id.is_empty())
        });
    let task_id = registration
        .and_then(|registration| registration.get("task_id"))
        .and_then(Value::as_str);
    mark_legacy_index_anomaly(
        marker,
        json!({
            "source_key":key,
            "source_kind":"participant",
            "global":false,
            "client_id":client_id,
            "task_id":task_id,
            "terms":[],
            "all_term_kinds":[],
        }),
    );
}

fn mark_legacy_card_gap(marker: &mut Value, key: &str, card: Option<&Value>) {
    let key_parts = key.strip_prefix("coordination:card:").and_then(|suffix| {
        let mut parts = suffix.split(':');
        let scope = parts.next()?;
        let card_kind = parts.next()?;
        let identity = parts.next()?;
        let client_id = parts.next()?;
        parts
            .next()
            .is_none()
            .then_some((scope, card_kind, identity, client_id))
    });
    let key_kind = key_parts.map(|(_, card_kind, _, _)| card_kind);
    let key_identity = key_parts.and_then(|(_, _, identity, _)| decode_index_component(identity));
    let key_client_id =
        key_parts.and_then(|(_, _, _, client_id)| decode_index_component(client_id));
    let card_kind = card
        .and_then(|card| card.get("card_kind"))
        .and_then(Value::as_str)
        .or(key_kind);
    let identity = card
        .and_then(|card| card.get("identity"))
        .and_then(Value::as_str)
        .or(key_identity.as_deref());
    let client_id = card
        .and_then(|card| card.get("client_id"))
        .and_then(Value::as_str)
        .or(key_client_id.as_deref());
    let task_id = card
        .and_then(|card| card.get("task_id"))
        .and_then(Value::as_str);
    let mut terms = Vec::new();
    let mut all_term_kinds = Vec::new();
    let mut global = key_parts.is_none() || !matches!(card_kind, Some("work" | "contract"));
    if key_kind.is_some_and(|key_kind| Some(key_kind) != card_kind) {
        global = true;
    }
    match card_kind {
        Some("contract") => {
            for contract_key in [identity, key_identity.as_deref()].into_iter().flatten() {
                let term = json!({"term_kind":"contract","term":contract_key});
                if !terms.contains(&term) {
                    terms.push(term);
                }
            }
            if terms.is_empty() {
                all_term_kinds.push(json!("contract"));
            }
        }
        Some("work") => {
            let fields = card
                .and_then(|card| card.get("fields"))
                .filter(|fields| fields.is_object());
            if let Some(fields) =
                fields.filter(|fields| keys::validate_card_fields("work", fields).is_ok())
            {
                for (kind, values) in keys::indexed_terms(fields, "work") {
                    for value in values {
                        terms.push(json!({"term_kind":kind.as_str(),"term":value}));
                    }
                }
            } else {
                all_term_kinds.extend([json!("path"), json!("symbol"), json!("interface")]);
            }
        }
        _ => global = true,
    }
    mark_legacy_index_anomaly(
        marker,
        json!({
            "source_key":key,
            "source_kind":"card",
            "global":global,
            "client_id":client_id,
            "task_id":task_id,
            "card_kind":card_kind,
            "terms":terms,
            "all_term_kinds":all_term_kinds,
        }),
    );
}

fn mark_legacy_index_anomaly(marker: &mut Value, gap: Value) {
    if marker["gaps"].as_array().is_none() {
        marker["gaps"] = json!([]);
    }
    let source_key = gap["source_key"].clone();
    if marker["gaps"].as_array().is_some_and(|gaps| {
        gaps.iter()
            .any(|existing| existing["source_key"] == source_key)
    }) {
        return;
    }
    let count = marker["anomaly_count"].as_u64().unwrap_or_default();
    marker["anomaly_count"] = json!(count.saturating_add(1));
    if marker["gaps"]
        .as_array()
        .is_some_and(|gaps| gaps.len() < LEGACY_INDEX_GAP_LIMIT)
    {
        marker["gaps"].as_array_mut().unwrap().push(gap);
    } else {
        marker["gaps_overflow"] = json!(true);
    }
}

fn is_legacy_projection_damage(error: &Error) -> bool {
    is_stale_watch_authority(error) || error.code == "INVALID_PARAMS"
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
            let global_key =
                keys::global_relevance_key(term_kind, &term, client_id, card_kind, identity);
            set_meta(
                tx,
                &global_key,
                &json!({
                    "client_id":client_id,
                    "scope_id":scope,
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
            tx.execute(
                "DELETE FROM meta WHERE key=?1",
                [keys::global_relevance_key(
                    term_kind, &term, client_id, card_kind, identity,
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
) -> Result<IndexedCardPage> {
    indexed_relevance_cards(
        db,
        Some(scope_id),
        term_kind,
        term,
        card_kind,
        after_client_id,
        None,
        raw_limit,
    )
}

fn indexed_cards_after_scan(
    db: &Connection,
    scope_id: &str,
    term_kind: keys::TermKind,
    term: &str,
    card_kind: Option<&str>,
    after_client_id: Option<&str>,
    after_scan_key: Option<&str>,
    raw_limit: i64,
) -> Result<IndexedCardPage> {
    indexed_relevance_cards(
        db,
        Some(scope_id),
        term_kind,
        term,
        card_kind,
        after_client_id,
        after_scan_key,
        raw_limit,
    )
}

/// Query only the exact-term cross-Task index. Every entry is checked against
/// the candidate's separately revalidated current scope before projection.
fn indexed_global_cards(
    db: &Connection,
    term_kind: keys::TermKind,
    term: &str,
    card_kind: Option<&str>,
    after_client_id: Option<&str>,
    raw_limit: i64,
) -> Result<IndexedCardPage> {
    indexed_relevance_cards(
        db,
        None,
        term_kind,
        term,
        card_kind,
        after_client_id,
        None,
        raw_limit,
    )
}

fn indexed_global_cards_after_scan(
    db: &Connection,
    term_kind: keys::TermKind,
    term: &str,
    card_kind: Option<&str>,
    after_client_id: Option<&str>,
    after_scan_key: Option<&str>,
    raw_limit: i64,
) -> Result<IndexedCardPage> {
    indexed_relevance_cards(
        db,
        None,
        term_kind,
        term,
        card_kind,
        after_client_id,
        after_scan_key,
        raw_limit,
    )
}

fn indexed_relevance_cards(
    db: &Connection,
    scope_id: Option<&str>,
    term_kind: keys::TermKind,
    term: &str,
    card_kind: Option<&str>,
    after_client_id: Option<&str>,
    after_scan_key: Option<&str>,
    raw_limit: i64,
) -> Result<IndexedCardPage> {
    let prefix = match scope_id {
        Some(scope_id) => keys::relevance_prefix(scope_id, term_kind, term),
        None => keys::global_relevance_prefix(term_kind, term),
    };
    let upper = format!("{prefix}g");
    if after_client_id.is_some() && after_scan_key.is_some() {
        return Err(Error::invalid(
            "choose after_client_id or after_scan_cursor",
        ));
    }
    let (comparison, lower) = match after_scan_key {
        Some(key) if key.starts_with(&prefix) && key.as_str() > prefix && key < &upper => {
            (">", key.to_owned())
        }
        Some(_) => return Err(Error::invalid("after_scan_cursor is outside this index")),
        None => match after_client_id {
            Some(client_id) => (
                ">",
                format!(
                    "{}{client}:~",
                    prefix,
                    client = keys::key_component(client_id)
                ),
            ),
            None => (">=", prefix.clone()),
        },
    };
    let sql = format!(
        "SELECT key,value_json FROM meta WHERE key {comparison} ?1 AND key < ?2 ORDER BY key LIMIT ?3"
    );
    let mut statement = db.prepare(&sql)?;
    let raw: Vec<(String, String)> = statement
        .query_map(params![lower, upper, raw_limit + 1], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<std::result::Result<_, _>>()?;
    let has_more = raw.len() as i64 > raw_limit;
    let last_scanned_key = raw
        .iter()
        .take(raw_limit as usize)
        .last()
        .map(|(key, _)| key.clone());
    let mut cards = Vec::new();
    let mut stale_entries = 0usize;
    for (key, raw) in raw.iter().take(raw_limit as usize) {
        let (key_client_id, key_card_kind, key_identity) =
            relevance_index_key_parts(&prefix, key, scope_id, term_kind, term)?;
        let value: Value = match serde_json::from_str(raw) {
            Ok(value) => value,
            Err(_) => {
                stale_entries = stale_entries.saturating_add(1);
                continue;
            }
        };
        let (Some(client_id), Some(found_kind), Some(identity), Some(card_revision)) = (
            value.get("client_id").and_then(Value::as_str),
            value.get("card_kind").and_then(Value::as_str),
            value.get("identity").and_then(Value::as_str),
            value
                .get("card_revision")
                .and_then(Value::as_i64)
                .filter(|value| *value > 0),
        ) else {
            stale_entries = stale_entries.saturating_add(1);
            continue;
        };
        if card_kind.is_some_and(|expected| expected != key_card_kind) {
            continue;
        }
        let entry_scope_id = value.get("scope_id").and_then(Value::as_str);
        if scope_id.is_none() && entry_scope_id.is_none() {
            stale_entries = stale_entries.saturating_add(1);
            continue;
        }
        let valid_index = client_id == key_client_id
            && found_kind == key_card_kind
            && identity == key_identity
            && value["term_kind"] == term_kind.as_str()
            && value["term"] == term;
        cards.push(IndexedCard {
            client_id: key_client_id,
            card_kind: key_card_kind,
            identity: key_identity,
            index_key: key.clone(),
            scope_id: entry_scope_id.map(str::to_owned),
            card_revision: Some(card_revision),
            valid_index,
        });
    }
    Ok(IndexedCardPage {
        cards,
        has_more,
        stale_entries,
        last_scanned_key,
    })
}

fn relevance_index_key_parts(
    prefix: &str,
    key: &str,
    scope_id: Option<&str>,
    term_kind: keys::TermKind,
    term: &str,
) -> Result<(String, String, String)> {
    let invalid = || {
        Error::new(
            "COORDINATION_INDEX_CORRUPT",
            "relevance index key is malformed or outside its exact namespace",
        )
    };
    let suffix = key.strip_prefix(prefix).ok_or_else(invalid)?;
    let mut parts = suffix.split(':');
    let client_id = parts
        .next()
        .and_then(decode_index_component)
        .ok_or_else(invalid)?;
    let card_kind = parts
        .next()
        .filter(|kind| matches!(*kind, "work" | "contract"));
    let identity = parts
        .next()
        .and_then(decode_index_component)
        .ok_or_else(invalid)?;
    if parts.next().is_some() {
        return Err(invalid());
    }
    let Some(card_kind) = card_kind else {
        return Err(invalid());
    };
    let expected = match scope_id {
        Some(scope_id) => {
            keys::relevance_key(scope_id, term_kind, term, &client_id, card_kind, &identity)
        }
        None => keys::global_relevance_key(term_kind, term, &client_id, card_kind, &identity),
    };
    if expected != key {
        return Err(invalid());
    }
    Ok((client_id, card_kind.to_owned(), identity))
}

fn indexed_task_participants(
    db: &Connection,
    task_id: &str,
    after_client_id: Option<&str>,
    after_scan_key: Option<&str>,
    raw_limit: i64,
) -> Result<IndexedParticipantPage> {
    let prefix = keys::participant_task_prefix(task_id);
    let upper = format!("{prefix}g");
    if after_client_id.is_some() && after_scan_key.is_some() {
        return Err(Error::invalid(
            "choose after_client_id or after_scan_cursor",
        ));
    }
    let (comparison, lower) = match after_scan_key {
        Some(key) if key.starts_with(&prefix) && key.as_str() > prefix && key < &upper => {
            (">", key.to_owned())
        }
        Some(_) => return Err(Error::invalid("after_scan_cursor is outside this index")),
        None => match after_client_id {
            Some(client_id) => (
                ">",
                format!(
                    "{}{client}",
                    prefix,
                    client = keys::key_component(client_id)
                ),
            ),
            None => (">=", prefix.clone()),
        },
    };
    let sql = format!(
        "SELECT key,value_json FROM meta WHERE key {comparison} ?1 AND key < ?2 ORDER BY key LIMIT ?3"
    );
    let mut statement = db.prepare(&sql)?;
    let rows: Vec<(String, String)> = statement
        .query_map(params![lower, upper, raw_limit + 1], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<std::result::Result<_, _>>()?;
    let has_more = rows.len() as i64 > raw_limit;
    let mut items = Vec::new();
    let mut stale_entries = 0usize;
    let last_scanned_key = rows
        .iter()
        .take(raw_limit as usize)
        .last()
        .map(|(key, _)| key.clone());
    for (key, raw) in rows.iter().take(raw_limit as usize) {
        let indexed_client_id = participant_client_id_from_index_key(&prefix, key)?;
        let entry: Value = match serde_json::from_str(raw) {
            Ok(entry) => entry,
            Err(_) => {
                stale_entries = stale_entries.saturating_add(1);
                continue;
            }
        };
        let (
            Some(client_id),
            Some(index_task_id),
            Some(task_revision),
            Some(attempt_id),
            Some(grant_revision),
        ) = (
            entry.get("client_id").and_then(Value::as_str),
            entry.get("task_id").and_then(Value::as_str),
            entry
                .get("task_revision")
                .and_then(Value::as_i64)
                .filter(|value| *value > 0),
            entry.get("attempt_id").and_then(Value::as_str),
            entry
                .get("grant_revision")
                .and_then(Value::as_i64)
                .filter(|value| *value > 0),
        )
        else {
            stale_entries = stale_entries.saturating_add(1);
            continue;
        };
        if client_id != indexed_client_id
            || index_task_id != task_id
            || key != &keys::participant_task_key(task_id, &indexed_client_id)
        {
            stale_entries = stale_entries.saturating_add(1);
            continue;
        }
        let scope = match load_current_scope_for_client(db, &indexed_client_id) {
            Ok(scope) => scope,
            Err(error) if is_legacy_projection_damage(&error) => {
                stale_entries = stale_entries.saturating_add(1);
                continue;
            }
            Err(error) => return Err(error),
        };
        if scope.task["task_id"] != task_id
            || scope.task["revision"] != task_revision
            || scope.attempt["attempt_id"] != attempt_id
            || scope.registration["grant_revision"] != grant_revision
        {
            stale_entries = stale_entries.saturating_add(1);
            continue;
        }
        items.push(indexed_client_id);
    }
    Ok(IndexedParticipantPage {
        client_ids: items,
        has_more,
        stale_entries,
        last_scanned_key,
    })
}

pub(super) fn scope_context(scope: &ScopeData) -> Value {
    json!({
        "task_id":scope.task["task_id"],
        "task_revision":scope.task["revision"],
        "attempt_id":scope.attempt["attempt_id"],
        "actor":{
            "client_id":scope.client_id,
            "participation_basis":scope.registration["participation_basis"],
        },
        "grant_revision":scope.registration["grant_revision"],
        "binding_id":scope.registration.get("binding_id").cloned().unwrap_or(Value::Null),
        "binding_generation":scope.registration.get("binding_generation").cloned().unwrap_or(Value::Null),
    })
}

fn relation_term(selector: Option<&PeerSelector>) -> Option<(keys::TermKind, &str)> {
    match selector {
        Some(PeerSelector::Term(kind, term)) => Some((*kind, term.as_str())),
        _ => None,
    }
}

fn relation_selector_matches(relation: &Value, selector: Option<&PeerSelector>) -> bool {
    match selector {
        None => true,
        Some(PeerSelector::Participant(client_id)) => {
            relation["source_context"]["actor"]["client_id"] == client_id.as_str()
                || relation["target_context"]["actor"]["client_id"] == client_id.as_str()
                || relation["kind"] != "unresolved_coordination"
        }
        Some(PeerSelector::Task(task_id)) => {
            relation["target_task_id"] == task_id.as_str()
                || relation["task_id"] == task_id.as_str()
                || relation["kind"] == "same_task_assignment"
                || relation["kind"] == "task_dependency"
        }
        Some(PeerSelector::Term(kind, term)) => {
            relation["term_kind"] == kind.as_str() && relation["term"] == term.as_str()
                || (relation["kind"] == "active_integration_cell"
                    && *kind == keys::TermKind::Contract
                    && relation["contract_key"] == term.as_str())
                || relation["original_relation"].is_object()
                    && relation_selector_matches(&relation["original_relation"], selector)
        }
    }
}

fn task_dependency_relation(source: &ScopeData, target: &ScopeData) -> Option<Value> {
    let source_dependencies = source
        .task
        .pointer("/spec/dependencies")
        .and_then(Value::as_array);
    if let Some(dependency) = source_dependencies
        .into_iter()
        .flatten()
        .find(|dependency| {
            dependency.get("task_id") == target.task.get("task_id")
                && dependency.get("required_revision") == target.task.get("revision")
                && dependency.get("required_phase").and_then(Value::as_str)
                    == target.task.pointer("/spec/phase").and_then(Value::as_str)
        })
    {
        return Some(json!({
            "kind":"task_dependency",
            "direction":"source_depends_on_target",
            "source_task_id":source.task["task_id"],
            "source_task_revision":source.task["revision"],
            "source_attempt_id":source.attempt["attempt_id"],
            "target_task_id":target.task["task_id"],
            "target_task_revision":target.task["revision"],
            "target_attempt_id":target.attempt["attempt_id"],
            "required_phase":dependency["required_phase"],
        }));
    }
    let target_dependencies = target
        .task
        .pointer("/spec/dependencies")
        .and_then(Value::as_array);
    target_dependencies
        .into_iter()
        .flatten()
        .find(|dependency| {
            dependency.get("task_id") == source.task.get("task_id")
                && dependency.get("required_revision") == source.task.get("revision")
                && dependency.get("required_phase").and_then(Value::as_str)
                    == source.task.pointer("/spec/phase").and_then(Value::as_str)
        })
        .map(|dependency| {
            json!({
                "kind":"task_dependency",
                "direction":"target_depends_on_source",
                "source_task_id":source.task["task_id"],
                "source_task_revision":source.task["revision"],
                "source_attempt_id":source.attempt["attempt_id"],
                "target_task_id":target.task["task_id"],
                "target_task_revision":target.task["revision"],
                "target_attempt_id":target.attempt["attempt_id"],
                "required_phase":dependency["required_phase"],
            })
        })
}

fn scope_cards(db: &Connection, scope: &ScopeData, card_kind: &str) -> Result<Vec<Value>> {
    let Some(client_id) = scope.client_id.as_deref() else {
        return Ok(Vec::new());
    };
    let prefix = format!(
        "coordination:card-owner:{}:{}:{card_kind}:",
        scope.scope_id,
        keys::key_component(client_id)
    );
    let upper = format!("{prefix}g");
    let mut statement =
        db.prepare("SELECT value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key LIMIT 101")?;
    let owners: Vec<String> = statement
        .query_map(params![prefix, upper], |row| row.get(0))?
        .collect::<std::result::Result<_, _>>()?;
    let mut cards = Vec::new();
    for raw in owners {
        let owner: Value = match serde_json::from_str(&raw) {
            Ok(owner) => owner,
            Err(_) => continue,
        };
        let Some(card_key) = owner.get("card_key").and_then(Value::as_str) else {
            continue;
        };
        let Some(card) = meta(db, card_key)?.filter(|card| card["state"] == "current") else {
            continue;
        };
        if card["client_id"] == client_id
            && card["task_id"] == scope.task["task_id"]
            && card["task_revision"] == scope.task["revision"]
            && card["attempt_id"] == scope.attempt["attempt_id"]
            && card["card_kind"] == card_kind
        {
            cards.push(card);
        }
    }
    Ok(cards)
}

fn card_relation_ref(card: &Value) -> Value {
    json!({
        "client_id":card["client_id"],
        "card_kind":card["card_kind"],
        "identity":card["identity"],
        "task_id":card["task_id"],
        "task_revision":card["task_revision"],
        "attempt_id":card["attempt_id"],
        "card_revision":card["card_revision"],
        "material_digest":card["material_digest"],
        "role":card.pointer("/fields/role").cloned().unwrap_or(Value::Null),
    })
}

fn shared_card_relation(
    db: &Connection,
    source: &ScopeData,
    target: &ScopeData,
    selector: Option<&PeerSelector>,
) -> Result<Option<Value>> {
    let required = relation_term(selector);
    let (source_cards, target_cards) = match required {
        Some((keys::TermKind::Contract, contract_key)) => (
            current_scope_contract_card(db, source, contract_key)?
                .into_iter()
                .collect(),
            current_scope_contract_card(db, target, contract_key)?
                .into_iter()
                .collect(),
        ),
        _ => (
            scope_cards(db, source, "contract")?,
            scope_cards(db, target, "contract")?,
        ),
    };
    for source_card in &source_cards {
        let source_fields = indexable_card_fields(
            &source_card["fields"],
            source_card["card_kind"].as_str().unwrap_or_default(),
            source_card["identity"].as_str().unwrap_or_default(),
        );
        let source_terms = keys::indexed_terms(
            &source_fields,
            source_card["card_kind"].as_str().unwrap_or_default(),
        );
        for target_card in &target_cards {
            let target_fields = indexable_card_fields(
                &target_card["fields"],
                target_card["card_kind"].as_str().unwrap_or_default(),
                target_card["identity"].as_str().unwrap_or_default(),
            );
            let target_terms = keys::indexed_terms(
                &target_fields,
                target_card["card_kind"].as_str().unwrap_or_default(),
            );
            for (kind, terms) in &source_terms {
                // Published path/symbol/interface fields are discovery hints,
                // not accepted scope authority. Cross-Task scope relations
                // are derived below from the current accepted code scopes.
                if *kind != keys::TermKind::Contract {
                    continue;
                }
                let Some(other_terms) = target_terms.get(kind) else {
                    continue;
                };
                for term in terms.intersection(other_terms) {
                    if required.is_some_and(|(required_kind, required_term)| {
                        *kind != required_kind || term != required_term
                    }) {
                        continue;
                    }
                    if *kind == keys::TermKind::Contract {
                        let role_is_current = |card: &Value| {
                            card["card_kind"] == "contract"
                                && card["identity"] == term.as_str()
                                && matches!(
                                    card.pointer("/fields/role").and_then(Value::as_str),
                                    Some("producer" | "consumer" | "carrier")
                                )
                        };
                        if !role_is_current(source_card) || !role_is_current(target_card) {
                            continue;
                        }
                    }
                    return Ok(Some(json!({
                        "kind":if *kind == keys::TermKind::Contract { "contract_key" } else { kind.as_str() },
                        "term_kind":kind.as_str(),
                        "term":term,
                        "source_card":card_relation_ref(source_card),
                        "target_card":card_relation_ref(target_card),
                    })));
                }
            }
        }
    }
    Ok(None)
}

fn current_scope_contract_card(
    db: &Connection,
    scope: &ScopeData,
    contract_key: &str,
) -> Result<Option<Value>> {
    let Some(client_id) = scope.client_id.as_deref() else {
        return Ok(None);
    };
    let Some(card) = current_card(db, &scope.scope_id, "contract", contract_key, client_id)? else {
        return Ok(None);
    };
    let Some(fields) = card.get("fields").filter(|fields| fields.is_object()) else {
        return Ok(None);
    };
    let Some(card_revision) = card["card_revision"].as_i64().filter(|value| *value > 0) else {
        return Ok(None);
    };
    let Some(material_digest) = card["material_digest"].as_str() else {
        return Ok(None);
    };
    if card["client_id"] != client_id
        || card["task_id"] != scope.task["task_id"]
        || card["task_revision"] != scope.task["revision"]
        || card["attempt_id"] != scope.attempt["attempt_id"]
        || card["card_kind"] != "contract"
        || card["identity"] != contract_key
        || keys::validate_card_fields("contract", fields).is_err()
        || model::digest(model::canonical(fields)?.as_bytes()) != material_digest
    {
        return Ok(None);
    }
    let _ = card_revision;
    Ok(Some(card))
}

fn accepted_scope_relation(
    db: &Connection,
    source: &ScopeData,
    target: &ScopeData,
    selector: Option<&PeerSelector>,
) -> Result<Option<Value>> {
    let Some((required_kind, required_term)) = relation_term(selector) else {
        if selector.is_some() {
            return Ok(None);
        }
        return accepted_scope_relation_for_terms(db, source, target, None);
    };
    if required_kind == keys::TermKind::Contract {
        return Ok(None);
    }
    accepted_scope_relation_for_terms(db, source, target, Some((required_kind, required_term)))
}

fn accepted_scope_relation_for_terms(
    db: &Connection,
    source: &ScopeData,
    target: &ScopeData,
    required: Option<(keys::TermKind, &str)>,
) -> Result<Option<Value>> {
    let source_scopes = super::code_scopes::current_scope_revisions(
        db,
        source.task["task_id"].as_str().unwrap_or_default(),
        source.task["revision"].as_i64().unwrap_or_default(),
        source.attempt["attempt_id"].as_str().unwrap_or_default(),
    )?;
    let target_scopes = super::code_scopes::current_scope_revisions(
        db,
        target.task["task_id"].as_str().unwrap_or_default(),
        target.task["revision"].as_i64().unwrap_or_default(),
        target.attempt["attempt_id"].as_str().unwrap_or_default(),
    )?;
    if source_scopes["coverage"] != "complete"
        || source_scopes["gaps"]
            .as_array()
            .is_none_or(|gaps| !gaps.is_empty())
        || target_scopes["coverage"] != "complete"
        || target_scopes["gaps"]
            .as_array()
            .is_none_or(|gaps| !gaps.is_empty())
    {
        return Ok(None);
    }
    for left in source_scopes["items"].as_array().into_iter().flatten() {
        if left["state"] != "active"
            || left["owner_client_id"].as_str() != source.client_id.as_deref()
        {
            continue;
        }
        for right in target_scopes["items"].as_array().into_iter().flatten() {
            if right["state"] != "active"
                || right["owner_client_id"].as_str() != target.client_id.as_deref()
            {
                continue;
            }
            for (kind, field) in [
                (keys::TermKind::Path, "paths"),
                (keys::TermKind::Symbol, "symbols"),
                (keys::TermKind::Interface, "interfaces"),
            ] {
                if required.is_some_and(|(required_kind, _)| kind != required_kind) {
                    continue;
                }
                let left_terms = left[field]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .filter(|term| !scope_term_has_glob(term))
                    .collect::<Vec<_>>();
                let right_terms = right[field]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .filter(|term| !scope_term_has_glob(term))
                    .collect::<Vec<_>>();
                let matched = left_terms.iter().find_map(|left_term| {
                    right_terms.iter().find_map(|right_term| {
                        let overlap = if kind == keys::TermKind::Path {
                            path_scope_covers(left_term, right_term)
                                || path_scope_covers(right_term, left_term)
                        } else {
                            left_term == right_term
                        };
                        let selected = required.is_none_or(|(required_kind, required_term)| {
                            kind == required_kind
                                && if kind == keys::TermKind::Path {
                                    path_scope_covers(left_term, required_term)
                                        || path_scope_covers(right_term, required_term)
                                        || path_scope_covers(required_term, left_term)
                                        || path_scope_covers(required_term, right_term)
                                } else {
                                    *left_term == required_term && *right_term == required_term
                                }
                        });
                        (overlap && selected).then_some(if required.is_some() {
                            required.map(|(_, term)| term).unwrap_or(*left_term)
                        } else if kind == keys::TermKind::Path
                            && path_scope_covers(left_term, right_term)
                        {
                            *right_term
                        } else {
                            *left_term
                        })
                    })
                });
                if let Some(term) = matched {
                    return Ok(Some(json!({
                        "kind":"accepted_scope_overlap",
                        "term_kind":kind.as_str(),
                        "term":term,
                        "source_scope":{
                            "scope_intent_id":left["scope_intent_id"],
                            "state_revision":left["state_revision"],
                            "digest":left["digest"],
                            "owner_client_id":left["owner_client_id"],
                        },
                        "target_scope":{
                            "scope_intent_id":right["scope_intent_id"],
                            "state_revision":right["state_revision"],
                            "digest":right["digest"],
                            "owner_client_id":right["owner_client_id"],
                        },
                    })));
                }
            }
        }
    }
    Ok(None)
}

fn scope_term_has_glob(term: &str) -> bool {
    term.contains('*') || term.contains('?') || term.contains('[') || term.contains('{')
}

fn path_scope_covers(declared: &str, target: &str) -> bool {
    !scope_term_has_glob(declared)
        && (target == declared
            || target.starts_with(&format!("{}/", declared.trim_end_matches('/'))))
}

fn pending_peer_relation_key(left_client_id: &str, right_client_id: &str) -> String {
    let (left, right) = if left_client_id <= right_client_id {
        (left_client_id, right_client_id)
    } else {
        (right_client_id, left_client_id)
    };
    format!(
        "coordination:peer-pending:{}:{}",
        keys::key_component(left),
        keys::key_component(right)
    )
}

fn pending_coordination_relation(
    db: &Connection,
    source: &ScopeData,
    target: &ScopeData,
    selector: Option<&PeerSelector>,
) -> Result<Option<Value>> {
    let (Some(source_id), Some(target_id)) =
        (source.client_id.as_deref(), target.client_id.as_deref())
    else {
        return Ok(None);
    };
    let Some(pending) = meta(db, &pending_peer_relation_key(source_id, target_id))? else {
        return Ok(None);
    };
    let source_context = scope_context(source);
    let target_context = scope_context(target);
    for ask in pending["asks"].as_array().into_iter().flatten() {
        let exact_direction =
            ask["source_context"] == source_context && ask["target_context"] == target_context;
        let reverse_direction =
            ask["source_context"] == target_context && ask["target_context"] == source_context;
        if !(exact_direction || reverse_direction) {
            continue;
        }
        if !relation_selector_matches(&ask["original_relation"], selector) {
            continue;
        }
        return Ok(Some(json!({
            "kind":"unresolved_coordination",
            "ask_id":ask["ask_id"],
            "source_context":ask["source_context"],
            "target_context":ask["target_context"],
            "original_relation":ask["original_relation"],
        })));
    }
    Ok(None)
}

fn current_peer_relation(
    db: &Connection,
    source: &ScopeData,
    target: &ScopeData,
    selector: Option<&PeerSelector>,
) -> Result<Option<Value>> {
    if source.client_id == target.client_id {
        return Ok(Some(json!({"kind":"self","context":scope_context(source)})));
    }
    let target_task_id = target.task["task_id"].as_str().unwrap_or_default();
    let task_selector_matches = match selector {
        Some(PeerSelector::Task(task_id)) => task_id == target_task_id,
        _ => true,
    };
    if task_selector_matches
        && let Some(review) = sponsored_review_relation(source, target)
        && !matches!(selector, Some(PeerSelector::Term(_, _)))
    {
        return Ok(Some(review));
    }
    if task_selector_matches
        && source.task["task_id"] == target.task["task_id"]
        && source.task["revision"] == target.task["revision"]
        && source.attempt["attempt_id"] == target.attempt["attempt_id"]
        && !matches!(selector, Some(PeerSelector::Term(_, _)))
    {
        return Ok(Some(json!({
            "kind":"same_task_assignment",
            "source_task_id":source.task["task_id"],
            "source_task_revision":source.task["revision"],
            "source_attempt_id":source.attempt["attempt_id"],
            "target_task_id":target.task["task_id"],
            "target_task_revision":target.task["revision"],
            "target_attempt_id":target.attempt["attempt_id"],
        })));
    }
    if task_selector_matches
        && let Some(dependency) = task_dependency_relation(source, target)
        && !matches!(selector, Some(PeerSelector::Term(_, _)))
    {
        return Ok(Some(dependency));
    }
    if let Some(card) = shared_card_relation(db, source, target, selector)? {
        return Ok(Some(card));
    }
    if let Some(scope) = accepted_scope_relation(db, source, target, selector)? {
        return Ok(Some(scope));
    }
    let cell_selector = matches!(selector, None | Some(PeerSelector::Participant(_)))
        || matches!(
            selector,
            Some(PeerSelector::Term(keys::TermKind::Contract, _))
        );
    if cell_selector
        && let Some(cell) = super::integration::current_peer_cell_relation(
            db,
            &scope_context(source),
            &scope_context(target),
            relation_term(selector).map(|(kind, term)| (kind, term.to_owned())),
        )?
    {
        return Ok(Some(cell));
    }
    let pending_selector = matches!(selector, None | Some(PeerSelector::Participant(_)))
        || matches!(selector, Some(PeerSelector::Term(_, _)));
    if pending_selector
        && let Some(pending) = pending_coordination_relation(db, source, target, selector)?
    {
        return Ok(Some(pending));
    }
    Ok(None)
}

fn sponsored_review_relation(source: &ScopeData, target: &ScopeData) -> Option<Value> {
    if source.task["task_id"] != target.task["task_id"]
        || source.task["revision"] != target.task["revision"]
        || source.attempt["attempt_id"] != target.attempt["attempt_id"]
    {
        return None;
    }
    let review = if source.registration["participation_basis"]["kind"] == "sponsored_reviewer" {
        (
            &source.registration["participation_basis"]["review_scope"],
            source,
        )
    } else if target.registration["participation_basis"]["kind"] == "sponsored_reviewer" {
        (
            &target.registration["participation_basis"]["review_scope"],
            target,
        )
    } else {
        return None;
    };
    let review_scope = review.0;
    if review_scope["task_id"] != source.task["task_id"]
        || review_scope["task_revision"] != source.task["revision"]
        || review_scope["attempt_id"] != source.attempt["attempt_id"]
        || review_scope["submission_ref"] != source.attempt["submission_ref"]
        || review_scope["candidate_ref"] != source.attempt["candidate_ref"]
    {
        return None;
    }
    Some(json!({
        "kind":"sponsored_review",
        "task_id":source.task["task_id"],
        "task_revision":source.task["revision"],
        "attempt_id":source.attempt["attempt_id"],
        "reviewer_client_id":review.1.client_id,
        "review_assignment_id":review_scope["review_assignment_id"],
        "submission_ref":review_scope["submission_ref"],
        "candidate_ref":review_scope["candidate_ref"],
    }))
}

fn relation_snapshot_current(
    db: &Connection,
    source: &ScopeData,
    target: &ScopeData,
    expected_relation: &Value,
    ask_id: Option<&str>,
) -> Result<bool> {
    if current_peer_relation(db, source, target, None)?.as_ref() == Some(expected_relation) {
        return Ok(true);
    }
    let (Some(source_id), Some(target_id), Some(ask_id)) = (
        source.client_id.as_deref(),
        target.client_id.as_deref(),
        ask_id,
    ) else {
        return Ok(false);
    };
    let Some(pending) = meta(db, &pending_peer_relation_key(source_id, target_id))? else {
        return Ok(false);
    };
    let source_context = scope_context(source);
    let target_context = scope_context(target);
    Ok(pending["asks"].as_array().is_some_and(|asks| {
        asks.iter().any(|ask| {
            ask["ask_id"] == ask_id
                && ask["source_context"] == source_context
                && ask["target_context"] == target_context
                && ask["original_relation"] == *expected_relation
        })
    }))
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
        let participant = load_current_scope_for_client(db, owner)?;
        if principal.role == Role::Participant
            && owner != principal.client_id
            && current_peer_relation(db, &scope, &participant, None)?.is_none()
        {
            return Err(Error::new(
                "NOT_FOUND",
                "no exact current relation authorizes this work-card read",
            ));
        }
        if principal.role != Role::Participant && participant.scope_id != scope.scope_id {
            return Err(Error::new("NOT_FOUND", "participant is outside this scope"));
        }
        return match current_card(db, &participant.scope_id, "work", "work", owner)? {
            Some(card) => project_card(&card, fields),
            None => Ok(json!({"available":false,"card_kind":"work","participant_id":owner})),
        };
    }
    let contract_key = model::text(value, "contract_key")?;
    validate_identifier(contract_key, "contract_key", 256)?;
    if value.get("participant_id").is_some() {
        let owner = model::text(value, "participant_id")?;
        let participant = load_current_scope_for_client(db, owner)?;
        if principal.role == Role::Participant
            && owner != principal.client_id
            && current_peer_relation(
                db,
                &scope,
                &participant,
                Some(&PeerSelector::Term(
                    keys::TermKind::Contract,
                    contract_key.to_owned(),
                )),
            )?
            .is_none()
        {
            return Err(Error::new(
                "NOT_FOUND",
                "no exact current contract relation authorizes this card read",
            ));
        }
        if principal.role != Role::Participant && participant.scope_id != scope.scope_id {
            return Err(Error::new("NOT_FOUND", "participant is outside this scope"));
        }
        return match current_card(db, &participant.scope_id, "contract", contract_key, owner)? {
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
    let scan_limit = (limit * RELEVANCE_SCAN_FACTOR + 32).min(keys::MAX_INBOX_SCAN);
    let (cards, index_more, index_stale) = if principal.role == Role::Participant {
        let page = indexed_global_cards(
            db,
            term_kind,
            &term,
            Some(card_kind),
            after.as_deref(),
            scan_limit,
        )?;
        (page.cards, page.has_more, page.stale_entries)
    } else {
        let page = indexed_cards(
            db,
            &scope.scope_id,
            term_kind,
            &term,
            Some(card_kind),
            after.as_deref(),
            limit,
        )?;
        (page.cards, page.has_more, page.stale_entries)
    };
    let migration_gap =
        legacy_index_backfill_gap_for(db, &[], Some((term_kind, term.as_str())), &[])?;
    let mut items = Vec::new();
    let mut stale = index_stale;
    let mut last = None;
    let mut has_more_peers = false;
    let mut last_scanned_index_key = None;
    for indexed in cards {
        let cursor_before_current = last_scanned_index_key.clone();
        last_scanned_index_key = Some(indexed.index_key.clone());
        last = Some(indexed.client_id.clone());
        if !indexed.valid_index {
            stale = stale.saturating_add(1);
            continue;
        }
        let participant = match load_current_scope_for_client(db, &indexed.client_id) {
            Ok(participant) => participant,
            Err(error) if is_stale_watch_authority(&error) => {
                stale = stale.saturating_add(1);
                continue;
            }
            Err(error) => return Err(error),
        };
        if principal.role != Role::Participant && participant.scope_id != scope.scope_id {
            stale = stale.saturating_add(1);
            continue;
        }
        let Some(card) = current_card(
            db,
            &participant.scope_id,
            &indexed.card_kind,
            &indexed.identity,
            &indexed.client_id,
        )?
        else {
            stale = stale.saturating_add(1);
            continue;
        };
        let indexed_fields = indexable_card_fields(&card["fields"], card_kind, &indexed.identity);
        let exact_term = keys::indexed_terms(&indexed_fields, card_kind)
            .get(&term_kind)
            .is_some_and(|terms| terms.contains(&term));
        if !exact_term
            || card["task_id"] != participant.task["task_id"]
            || card["task_revision"] != participant.task["revision"]
            || card["attempt_id"] != participant.attempt["attempt_id"]
            || card["client_id"] != indexed.client_id
            || indexed
                .scope_id
                .as_deref()
                .is_some_and(|scope_id| scope_id != participant.scope_id)
            || indexed.card_revision != card["card_revision"].as_i64()
        {
            stale = stale.saturating_add(1);
            continue;
        }
        if principal.role == Role::Participant
            && indexed.client_id != principal.client_id
            && current_peer_relation(
                db,
                &scope,
                &participant,
                Some(&PeerSelector::Term(term_kind, term.clone())),
            )?
            .is_none()
        {
            continue;
        }
        if items.len() == limit as usize {
            has_more_peers = true;
            last = items
                .last()
                .and_then(|item: &Value| item["client_id"].as_str())
                .map(str::to_owned);
            break;
        }
        items.push(project_card(&card, value.get("fields"))?);
    }
    let partial = index_more || stale > 0 || has_more_peers || migration_gap.is_some();
    let mut gaps = Vec::new();
    if stale > 0 {
        gaps.push(json!({"kind":"stale_or_corrupt_card_index_entries","count":stale}));
    }
    if index_more {
        gaps.push(json!({"kind":"more_indexed_cards","count":null}));
    }
    if has_more_peers {
        gaps.push(json!({"kind":"more_relevant_peers","count":null}));
    }
    if let Some(gap) = migration_gap {
        gaps.push(gap);
    }
    Ok(json!({
        "items":items,
        "selector":{"kind":term_kind.as_str(),"value":term},
        "next_after":if partial { last } else { None },
        "coverage":if partial { "partial" } else { "complete" },
        "gaps":gaps,
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
    let is_participant = scope.client_id.is_some();
    let scan_limit = (limit * RELEVANCE_SCAN_FACTOR + 32).min(keys::MAX_INBOX_SCAN);
    let (cards, index_more, index_stale) = if is_participant {
        let page = indexed_global_cards(
            db,
            keys::TermKind::Contract,
            contract_key,
            Some("contract"),
            after.as_deref(),
            scan_limit,
        )?;
        (page.cards, page.has_more, page.stale_entries)
    } else {
        let page = indexed_cards(
            db,
            &scope.scope_id,
            keys::TermKind::Contract,
            contract_key,
            Some("contract"),
            after.as_deref(),
            limit,
        )?;
        (page.cards, page.has_more, page.stale_entries)
    };
    let mut items = Vec::new();
    let mut stale = index_stale;
    let mut last = None;
    let mut has_more = false;
    for indexed in cards {
        last = Some(indexed.client_id.clone());
        if !indexed.valid_index {
            stale = stale.saturating_add(1);
            continue;
        }
        let participant = match load_current_scope_for_client(db, &indexed.client_id) {
            Ok(participant) => participant,
            Err(error) if is_stale_watch_authority(&error) => {
                stale = stale.saturating_add(1);
                continue;
            }
            Err(error) => return Err(error),
        };
        if !is_participant && participant.scope_id != scope.scope_id {
            stale = stale.saturating_add(1);
            continue;
        }
        let Some(card) = current_card(
            db,
            &participant.scope_id,
            "contract",
            contract_key,
            &indexed.client_id,
        )?
        else {
            stale = stale.saturating_add(1);
            continue;
        };
        if card["task_id"] != participant.task["task_id"]
            || card["task_revision"] != participant.task["revision"]
            || card["attempt_id"] != participant.attempt["attempt_id"]
            || card["identity"] != contract_key
            || indexed
                .scope_id
                .as_deref()
                .is_some_and(|scope_id| scope_id != participant.scope_id)
            || indexed.card_revision != card["card_revision"].as_i64()
        {
            stale = stale.saturating_add(1);
            continue;
        }
        if let Some(actor) = scope.client_id.as_deref()
            && indexed.client_id != actor
            && current_peer_relation(
                db,
                scope,
                &participant,
                Some(&PeerSelector::Term(
                    keys::TermKind::Contract,
                    contract_key.to_owned(),
                )),
            )?
            .is_none()
        {
            continue;
        }
        if items.len() == limit as usize {
            has_more = true;
            last = items
                .last()
                .and_then(|item: &Value| item["client_id"].as_str())
                .map(str::to_owned);
            break;
        }
        items.push(project_card(&card, fields)?);
    }
    let migration_gap = legacy_index_backfill_gap_for(
        db,
        &[],
        Some((keys::TermKind::Contract, contract_key)),
        &[],
    )?;
    let partial = index_more || stale > 0 || has_more || migration_gap.is_some();
    let mut gaps = Vec::new();
    if stale > 0 {
        gaps.push(json!({"kind":"stale_or_corrupt_card_index_entries","count":stale}));
    }
    if index_more {
        gaps.push(json!({"kind":"more_indexed_cards","count":null}));
    }
    if has_more {
        gaps.push(json!({"kind":"more_relevant_peers","count":null}));
    }
    if let Some(gap) = migration_gap {
        gaps.push(gap);
    }
    Ok((
        items,
        if partial { last } else { None },
        if partial { "partial" } else { "complete" },
        Value::Array(gaps),
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
            "related_task_id",
            "participant_id",
            "contract_key",
            "path",
            "symbol",
            "interface",
            "fields",
            "limit",
            "after_client_id",
            "after_scan_cursor",
        ],
    )?;
    validate_projection_fields(value.get("fields"))?;
    let scope = read_scope(db, principal, value)?;
    let term = selector(value)?;
    let related_task = value
        .get("related_task_id")
        .filter(|value| !value.is_null())
        .map(|_| model::text(value, "related_task_id").map(str::to_owned))
        .transpose()?;
    if let Some(task_id) = related_task.as_deref() {
        validate_identifier(task_id, "related_task_id", 128)?;
    }
    let participant_id = value
        .get("participant_id")
        .filter(|value| !value.is_null())
        .map(|_| model::text(value, "participant_id").map(str::to_owned))
        .transpose()?;
    if let Some(client_id) = participant_id.as_deref() {
        validate_identifier(client_id, "participant_id", 128)?;
    }
    if (term.is_some() as usize)
        + (related_task.is_some() as usize)
        + (participant_id.is_some() as usize)
        != 1
    {
        return Err(Error::invalid(
            "peer.find requires exactly one contract/path/symbol/interface, related_task_id, or participant_id selector",
        ));
    }
    let limit = keys::parse_page(value.get("limit"), 20)?;
    let after = keys::optional_cursor(value, "after_client_id")?;
    let after_scan_cursor = keys::parse_list_scan_cursor(value)?;
    if after.is_some() && after_scan_cursor.is_some() {
        return Err(Error::invalid(
            "choose after_client_id or after_scan_cursor",
        ));
    }
    if participant_id.is_some() && (after.is_some() || after_scan_cursor.is_some()) {
        return Err(Error::invalid("participant_id lookup cannot be paged"));
    }
    // Exact term and Task indexes keep the query proportional to this local
    // neighborhood. Candidate grants and both endpoint contexts are always
    // revalidated before any card is projected.
    let scan_limit = keys::MAX_INBOX_SCAN;
    let (cards, index_more, index_stale, selector, scan_prefix, page_last_scanned_key) =
        if let Some((kind, term)) = term {
            let prefix = if principal.role == Role::Participant {
                keys::global_relevance_prefix(kind, &term)
            } else {
                keys::relevance_prefix(&scope.scope_id, kind, &term)
            };
            let upper = format!("{prefix}g");
            let after_scan_key = after_scan_cursor
                .as_deref()
                .map(|cursor| {
                    keys::decode_list_scan_cursor(&prefix, &principal.client_id, &upper, cursor)
                })
                .transpose()?;
            let page = if principal.role == Role::Participant {
                indexed_global_cards_after_scan(
                    db,
                    kind,
                    &term,
                    None,
                    after.as_deref(),
                    after_scan_key.as_deref(),
                    scan_limit,
                )?
            } else {
                indexed_cards_after_scan(
                    db,
                    &scope.scope_id,
                    kind,
                    &term,
                    None,
                    after.as_deref(),
                    after_scan_key.as_deref(),
                    scan_limit,
                )?
            };
            (
                page.cards,
                page.has_more,
                page.stale_entries,
                PeerSelector::Term(kind, term),
                Some(prefix),
                page.last_scanned_key,
            )
        } else if let Some(task_id) = related_task {
            let prefix = keys::participant_task_prefix(&task_id);
            let upper = format!("{prefix}g");
            let after_scan_key = after_scan_cursor
                .as_deref()
                .map(|cursor| {
                    keys::decode_list_scan_cursor(&prefix, &principal.client_id, &upper, cursor)
                })
                .transpose()?;
            let page = indexed_task_participants(
                db,
                &task_id,
                after.as_deref(),
                after_scan_key.as_deref(),
                scan_limit,
            )?;
            let IndexedParticipantPage {
                client_ids,
                has_more,
                stale_entries,
                last_scanned_key,
            } = page;
            (
                client_ids
                    .into_iter()
                    .map(|client_id| IndexedCard {
                        index_key: keys::participant_task_key(&task_id, &client_id),
                        client_id,
                        card_kind: "work".to_owned(),
                        identity: "work".to_owned(),
                        scope_id: None,
                        card_revision: None,
                        valid_index: true,
                    })
                    .collect(),
                has_more,
                stale_entries,
                PeerSelector::Task(task_id),
                Some(prefix),
                last_scanned_key,
            )
        } else {
            let client_id = participant_id.unwrap_or_default();
            (
                vec![IndexedCard {
                    client_id: client_id.clone(),
                    card_kind: "work".to_owned(),
                    identity: "work".to_owned(),
                    index_key: String::new(),
                    scope_id: None,
                    card_revision: None,
                    valid_index: true,
                }],
                false,
                0,
                PeerSelector::Participant(client_id),
                None,
                None,
            )
        };
    let source_task_id = scope.task["task_id"].as_str().unwrap_or_default();
    let migration_gap = match &selector {
        PeerSelector::Term(kind, term) => {
            legacy_index_backfill_gap_for(db, &[], Some((*kind, term)), &[])?
        }
        PeerSelector::Task(task_id) => {
            legacy_index_backfill_gap_for(db, &[source_task_id, task_id], None, &[])?
        }
        PeerSelector::Participant(client_id) => {
            legacy_index_backfill_gap_for(db, &[source_task_id], None, &[client_id])?
        }
    };
    let mut items = Vec::new();
    let mut seen = BTreeSet::new();
    let mut stale = index_stale;
    let mut last = None;
    let mut has_more_peers = false;
    let mut last_scanned_index_key = None;
    for indexed in cards {
        let cursor_before_current = last_scanned_index_key.clone();
        last_scanned_index_key = Some(indexed.index_key.clone());
        last = Some(indexed.client_id.clone());
        if !indexed.valid_index {
            stale = stale.saturating_add(1);
            continue;
        }
        if principal.role == Role::Participant && indexed.client_id == principal.client_id {
            continue;
        }
        if !seen.insert(indexed.client_id.clone()) {
            continue;
        }
        let candidate = match load_current_scope_for_client(db, &indexed.client_id) {
            Ok(candidate) => candidate,
            Err(error) if is_stale_watch_authority(&error) => {
                stale = stale.saturating_add(1);
                continue;
            }
            Err(error) => return Err(error),
        };
        let manager_local_term =
            principal.role != Role::Participant && matches!(&selector, PeerSelector::Term(_, _));
        if manager_local_term && candidate.scope_id != scope.scope_id {
            continue;
        }
        let relation = if manager_local_term {
            Some(Value::Null)
        } else {
            current_peer_relation(db, &scope, &candidate, Some(&selector))?
        };
        let Some(relation) = relation else {
            continue;
        };
        if matches!(&selector, PeerSelector::Task(task_id)
            if candidate.task["task_id"] != task_id.as_str()
                || !matches!(relation["kind"].as_str(), Some("same_task_assignment" | "task_dependency" | "sponsored_review")))
        {
            continue;
        }
        let card = current_card(
            db,
            &candidate.scope_id,
            &indexed.card_kind,
            &indexed.identity,
            &indexed.client_id,
        )?;
        if matches!(&selector, PeerSelector::Term(_, _)) && card.is_none() {
            stale = stale.saturating_add(1);
            continue;
        }
        let card = match card {
            Some(card) if matches!(&selector, PeerSelector::Term(_, _)) => {
                if indexed.card_revision != card["card_revision"].as_i64()
                    || indexed
                        .scope_id
                        .as_deref()
                        .is_some_and(|scope_id| scope_id != candidate.scope_id.as_str())
                {
                    stale = stale.saturating_add(1);
                    continue;
                }
                let Some((kind, term)) = relation_term(Some(&selector)) else {
                    unreachable!()
                };
                let fields =
                    indexable_card_fields(&card["fields"], &indexed.card_kind, &indexed.identity);
                if !keys::indexed_terms(&fields, &indexed.card_kind)
                    .get(&kind)
                    .is_some_and(|terms| terms.contains(term))
                {
                    stale = stale.saturating_add(1);
                    continue;
                }
                Some(card)
            }
            Some(card) => Some(card),
            None => None,
        };
        if let Some(card) = card.as_ref()
            && (card["client_id"] != indexed.client_id
                || card["task_id"] != candidate.task["task_id"]
                || card["task_revision"] != candidate.task["revision"]
                || card["attempt_id"] != candidate.attempt["attempt_id"])
        {
            stale = stale.saturating_add(1);
            continue;
        }
        if items.len() >= limit as usize {
            has_more_peers = true;
            // Leave the first unreturned candidate on the next page.
            last_scanned_index_key = cursor_before_current;
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
            "context":scope_context(&candidate),
            "match":{"kind":selector_kind(&selector),"value":selector_value(&selector),"reason":if relation.is_null() { "exact_card_index" } else { relation["kind"].as_str().unwrap_or("exact_relation") },"relation":relation},
            "card":card.as_ref().map(|card| project_card(card, value.get("fields"))).transpose()?,
        }));
    }
    if !has_more_peers {
        last_scanned_index_key = page_last_scanned_key;
    }
    let next_scan_cursor = if index_more || has_more_peers {
        let prefix = scan_prefix.as_deref().ok_or_else(|| {
            Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "bounded peer page has no index scan namespace",
            )
        })?;
        let key = last_scanned_index_key.as_deref().ok_or_else(|| {
            Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "bounded peer page has no representable scan position",
            )
        })?;
        Some(keys::encode_list_scan_cursor(
            prefix,
            &principal.client_id,
            key,
        )?)
    } else {
        None
    };
    let partial = index_more || stale > 0 || has_more_peers || migration_gap.is_some();
    let mut gaps = Vec::new();
    if stale > 0 {
        gaps.push(json!({"kind":"stale_or_corrupt_relevance_entries","count":stale}));
    }
    if index_more {
        gaps.push(json!({"kind":"relevance_scan_bound","count":null}));
    }
    if has_more_peers {
        gaps.push(json!({"kind":"more_relevant_peers","count":null}));
    }
    if let Some(gap) = migration_gap {
        gaps.push(gap);
    }
    Ok(json!({
        "items":items,
        "selector":{"kind":selector_kind(&selector),"value":selector_value(&selector)},
        "task_id":scope.task["task_id"],
        "task_revision":scope.task["revision"],
        "attempt_id":scope.attempt["attempt_id"],
        "next_after":if partial { last } else { None },
        "next_scan_cursor":next_scan_cursor,
        "coverage":if partial { "partial" } else { "complete" },
        "gaps":gaps,
    }))
}

fn selector_kind(selector: &PeerSelector) -> &'static str {
    match selector {
        PeerSelector::Term(kind, _) => kind.as_str(),
        PeerSelector::Task(_) => "task",
        PeerSelector::Participant(_) => "participant",
    }
}

fn selector_value(selector: &PeerSelector) -> &str {
    match selector {
        PeerSelector::Term(_, value)
        | PeerSelector::Task(value)
        | PeerSelector::Participant(value) => value,
    }
}

#[derive(Clone)]
enum InboxCursor {
    Operation(String),
    ScopedKey(String),
}

fn inbox(db: &Connection, principal: &Principal, value: &Value) -> Result<Value> {
    principal.require_participant()?;
    model::fields(value, &["limit", "after_operation_id", "after_key"])?;
    let limit = keys::parse_page(value.get("limit"), 20)?;
    let after = keys::optional_cursor(value, "after_operation_id")?;
    let after_key = match value.get("after_key") {
        None | Some(Value::Null) => None,
        Some(Value::String(key)) => Some(key.as_str()),
        Some(_) => return Err(Error::invalid("after_key must be a string or null")),
    };
    if after.is_some() && after_key.is_some() {
        return Err(Error::invalid(
            "after_operation_id and after_key cannot be supplied together",
        ));
    }
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
                 "next_after_key":null,
                "watch_notifications":watch_notifications,"coverage":"partial",
                "gaps":[{"kind":"current_scope_unavailable","code":error.code}]}),
            );
        }
        Err(error) => return Err(error),
    };
    let watch_notifications = super::coordination_watch::notifications(db, principal, limit)?;
    if scope.registration["inbound_policy"] == "hold" {
        return Ok(json!({
            "items":[],
            "task_id":scope.task["task_id"],
            "task_revision":scope.task["revision"],
            "attempt_id":scope.attempt["attempt_id"],
            "inbound_policy":"hold",
            "availability":"held_by_inbound_policy",
            "next_after":null,
            "next_after_key":null,
            "watch_notifications":watch_notifications,
            "coverage":"complete",
            "gaps":[],
        }));
    }
    let prefix = keys::mailbox_prefix(&scope.scope_id, &principal.client_id);
    let upper = format!("{prefix}z");
    let operation_cut = after
        .as_deref()
        .map(|id| inbox_cursor_key(db, &scope, &principal.client_id, id))
        .transpose()?;
    // Validate a recovery cut before normalization can retire any locator.
    if let Some(key) = after_key
        && (!key.starts_with(&prefix) || key >= upper.as_str() || !inbox_key_exists(db, key)?)
    {
        // Exact retained scope membership also permits the overlong damaged
        // positions issued by the server; canonical ID caps do not apply.
        return Err(Error::invalid(
            "after_key must identify a retained inbox index in the authenticated exact scope",
        ));
    }
    // A bounded pass alone does not establish order: unexamined legacy rows
    // can belong before any converted v2 delivery. Keep the caller's delivery
    // cut unchanged while the independent normalization checkpoint advances.
    let normalization =
        normalize_legacy_mailbox_indexes(db, &scope, &principal.client_id, INBOX_NORMALIZE_BOUND)?;
    // A legacy recovery locator precedes all valid v2 deliveries and may be
    // retired in this pass. Restart its unread delivery phase at the canonical
    // beginning; converting that raw cut into an Operation cut could skip mail.
    let recovery_cut = match after_key {
        Some(key) if inbox_key_exists(db, key)? => Some(key.to_owned()),
        _ => None,
    };
    if let Some(checkpoint) = normalization.next {
        return Ok(json!({
            "items":[],
            "task_id":scope.task["task_id"],
            "task_revision":scope.task["revision"],
            "attempt_id":scope.attempt["attempt_id"],
            "inbound_policy":scope.registration["inbound_policy"],
            "availability":"legacy_order_normalization_pending",
            "next_after":after,
            "next_after_key":recovery_cut,
            "watch_notifications":watch_notifications,
            "coverage":"partial",
            "gaps":[{"kind":"inbox_normalization_pending","count":null,
                     "after_index_key":checkpoint}],
        }));
    }
    // Operation IDs remain the normal cursor. A scoped key is the bounded
    // recovery cursor when damaged indexes cannot be represented by one.
    let scan_limit = (limit * RELEVANCE_SCAN_FACTOR + 32).min(keys::MAX_INBOX_SCAN);
    let after_key = operation_cut.or(recovery_cut);
    // Unorderable legacy damage lies outside a canonical v2 cut. The full
    // normalization sweep still reports it so that a later key cannot claim
    // complete coverage merely by skipping the numeric namespace.
    let legacy_damage_outside_cut = after_key
        .as_deref()
        .is_some_and(|key| key >= format!("{prefix}v").as_str())
        && normalization.damaged > 0;
    let (lower, comparison) = match after_key.as_deref() {
        Some(key) => (key, ">"),
        None => (prefix.as_str(), ">="),
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
    let mut damaged = 0usize;
    // Advance only through rows actually examined. In particular, a valid
    // item excluded by either response budget remains the first row on the
    // next page. Damaged rows use their exact retained key when an Operation
    // ID cannot identify one unambiguous ordering position.
    let mut last_examined: Option<InboxCursor> = None;
    let mut serialized_items_bytes = 2usize; // canonical "[]"
    let mut more = false;
    let mut item_limited = false;
    let mut byte_limited = false;
    for (key, raw_index) in rows.iter().take(scan_limit as usize) {
        let index = serde_json::from_str::<Value>(raw_index).ok();
        let Some(operation_id) = inbox_operation_id_from_key(&prefix, key) else {
            damaged = damaged.saturating_add(1);
            last_examined = Some(InboxCursor::ScopedKey(key.clone()));
            continue;
        };
        let read_position = operation_read_position(db, &operation_id)?;
        let mut cursor = InboxCursor::ScopedKey(key.clone());
        let mut position_key: Option<String> = None;
        let mut duplicate_indexes = false;
        if let Some((position, indexed_at_ms)) = read_position
            && position > 0
        {
            let expected_position_key = mailbox_position_key(
                &scope.scope_id,
                &principal.client_id,
                position,
                &operation_id,
            );
            let expected_legacy_key = keys::mailbox_key(
                &scope.scope_id,
                &principal.client_id,
                indexed_at_ms,
                &operation_id,
            );
            if key == &expected_position_key || key == &expected_legacy_key {
                let alternate_key = if key == &expected_position_key {
                    &expected_legacy_key
                } else {
                    &expected_position_key
                };
                duplicate_indexes = inbox_key_exists(db, alternate_key)?;
                if key == &expected_position_key && !duplicate_indexes {
                    cursor = InboxCursor::Operation(operation_id.clone());
                }
            }
            position_key = Some(expected_position_key);
        }

        // The retained Operation is authoritative. Index damage must not turn
        // missing/invalid Operation receipts or body data into an empty page.
        let operation: Option<(String, String, i64, String, String)> = db
            .query_row(
                "SELECT method,caller_id,created_at_ms,original_request_json,result_json \
                 FROM operations WHERE operation_id=?1 \
                   AND method IN ('coordination.send','coordination.consult') \
                   AND state='settled'",
                [&operation_id],
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
            return Err(Error::new(
                "INBOX_OPERATION_MISSING",
                "scoped inbox index does not identify a settled coordination Operation",
            ));
        };
        let result: Value = serde_json::from_str(&result_json)?;
        let request: Value = serde_json::from_str(&request_json)?;
        if method == "coordination.consult" && result["delivery_created"] == false {
            // Coalesced/answered consultations have no mailbox body. A stale
            // copied index to one is derived-index damage.
            damaged = damaged.saturating_add(1);
            last_examined = Some(cursor);
            continue;
        }
        let raw_envelope = model::text(&result, "text")?;
        if raw_envelope.len() > keys::MAX_MESSAGE_BYTES {
            return Err(Error::new(
                "INBOX_OPERATION_BODY_DAMAGED",
                "settled coordination Operation exceeds the retained message byte limit",
            ));
        }
        let envelope: Value = serde_json::from_str(raw_envelope)?;
        let legacy_envelope = envelope["schema"] == "eliot.coordination.message.v1"
            && envelope["task_id"] == scope.task["task_id"]
            && envelope["task_revision"] == scope.task["revision"]
            && envelope["attempt_id"] == scope.attempt["attempt_id"];
        let versioned_envelope = envelope["schema"] == "eliot.coordination.message.v2"
            && envelope
                .pointer("/source_context/actor/client_id")
                .and_then(Value::as_str)
                == Some(sender.as_str())
            && envelope
                .pointer("/target_context/actor/client_id")
                .and_then(Value::as_str)
                == Some(principal.client_id.as_str())
            && envelope["source_context"]["task_id"] == result["task_id"]
            && envelope["source_context"]["task_revision"] == result["task_revision"]
            && envelope["source_context"]["attempt_id"] == result["attempt_id"]
            && envelope["target_context"]["task_id"] == scope.task["task_id"]
            && envelope["target_context"]["task_revision"] == scope.task["revision"]
            && envelope["target_context"]["attempt_id"] == scope.attempt["attempt_id"]
            && envelope["relation"] == result["relation"]
            && envelope["source_context"] == result["source_context"]
            && envelope["target_context"] == result["target_context"];
        if envelope["body"].is_null()
            || result["sender"] != sender
            || envelope["sender"] != sender
            || envelope["recipient"] != result["recipient"]
            || !(legacy_envelope || versioned_envelope)
        {
            return Err(Error::new(
                "INBOX_OPERATION_BODY_DAMAGED",
                "settled coordination Operation body differs from its authoritative receipt",
            ));
        }
        if method == "coordination.send"
            && (request["recipient"] != envelope["recipient"]
                || envelope["body"] != request["body"])
        {
            return Err(Error::new(
                "INBOX_OPERATION_BODY_DAMAGED",
                "settled coordination.send body differs from its original request",
            ));
        }
        if method == "coordination.consult"
            && !consult_delivery_matches(
                &result,
                &request,
                &envelope["body"],
                &sender,
                &operation_id,
            )?
        {
            return Err(Error::new(
                "INBOX_OPERATION_BODY_DAMAGED",
                "settled coordination Operation body does not match its retained delivery",
            ));
        }

        let index_matches = index.as_ref().is_some_and(|index| {
            let position_matches = read_position.is_some_and(|(position, indexed_at_ms)| {
                position > 0
                    // Unretired legacy locators are damage, never items.
                    // Only a proved v2 locator supplies the delivery order.
                    && position_key.as_deref() == Some(key.as_str())
                    && indexed_at_ms == created_at
                    && index["read_position"].as_i64() == Some(position)
            });
            index["operation_id"].as_str() == Some(operation_id.as_str())
                && index["created_at_ms"].as_i64() == Some(created_at)
                && index["sender"].as_str() == Some(sender.as_str())
                && index["recipient"].as_str() == Some(principal.client_id.as_str())
                && index["task_id"] == scope.task["task_id"]
                && index["task_revision"] == scope.task["revision"]
                && index["attempt_id"] == scope.attempt["attempt_id"]
                && position_matches
                && !duplicate_indexes
        });
        if !index_matches {
            damaged = damaged.saturating_add(1);
            last_examined = Some(cursor);
            continue;
        }

        if !delivery_matches_scope(&result, &scope, &principal.client_id)?
            || !retained_relation_matches_delivery(
                db,
                &envelope,
                &scope,
                &principal.client_id,
                &sender,
            )?
            || (method == "coordination.send" && request["recipient"] != principal.client_id)
        {
            stale = stale.saturating_add(1);
            last_examined = Some(cursor);
            continue;
        }
        if messages.len() == limit as usize {
            more = true;
            item_limited = true;
            break;
        }
        let message = json!({
            "operation_id":operation_id,
            "delivery_id":result["delivery_id"],
            "sender":sender,
            "recipient":principal.client_id,
            "sent_at_ms":created_at,
            "payload_digest":result["payload_digest"],
            "kind":if method == "coordination.consult" { "consult" } else { "message" },
            "body":envelope["body"],
        });
        let item_bytes = model::canonical(&message)?.len();
        let candidate_bytes = serialized_items_bytes
            .saturating_add(item_bytes)
            .saturating_add(usize::from(!messages.is_empty()));
        if candidate_bytes > super::projection::MAX_SERIALIZED_BYTES {
            more = true;
            byte_limited = true;
            break;
        }
        serialized_items_bytes = candidate_bytes;
        messages.push(message);
        last_examined = Some(cursor);
    }
    let partial = scan_more || stale > 0 || damaged > 0 || more || legacy_damage_outside_cut;
    let mut gaps = Vec::new();
    if damaged > 0 {
        gaps.push(json!({"kind":"damaged_mailbox_indexes","count":damaged}));
    }
    if legacy_damage_outside_cut {
        gaps.push(
            json!({"kind":"legacy_mailbox_normalization_damage","count":normalization.damaged}),
        );
    }
    if stale > 0 {
        gaps.push(json!({"kind":"stale_mailbox_records","count":stale}));
    }
    if scan_more {
        gaps.push(json!({"kind":"inbox_scan_bound","count":null}));
    }
    if item_limited {
        gaps.push(json!({"kind":"inbox_item_budget","count":null}));
    }
    if byte_limited {
        gaps.push(json!({"kind":"inbox_byte_budget","count":null}));
    }
    let (next_after, next_after_key) = match last_examined {
        Some(InboxCursor::Operation(operation_id)) => (json!(operation_id), Value::Null),
        Some(InboxCursor::ScopedKey(key)) => (Value::Null, json!(key)),
        None => (Value::Null, Value::Null),
    };
    Ok(json!({
        "items":messages,
        "task_id":scope.task["task_id"],
        "task_revision":scope.task["revision"],
        "attempt_id":scope.attempt["attempt_id"],
        "inbound_policy":scope.registration["inbound_policy"],
        "next_after":next_after,
        "next_after_key":next_after_key,
        "watch_notifications":watch_notifications,
        "coverage":if partial { "partial" } else { "complete" },
        "gaps":gaps,
    }))
}

fn inbox_cursor_key(
    db: &Connection,
    scope: &ScopeData,
    client_id: &str,
    operation_id: &str,
) -> Result<String> {
    let Some((position, created_at_ms)) = operation_read_position(db, operation_id)? else {
        return Err(Error::invalid(
            "after_operation_id must identify a retained scoped inbox index",
        ));
    };
    if position <= 0 {
        return Err(Error::new(
            "INBOX_INDEX_DAMAGED",
            "Operation read-position marker is not positive",
        ));
    }
    let position_key = mailbox_position_key(&scope.scope_id, client_id, position, operation_id);
    let legacy_key = keys::mailbox_key(&scope.scope_id, client_id, created_at_ms, operation_id);
    let position_index = inbox_key_exists(db, &position_key)?;
    let legacy_index = inbox_key_exists(db, &legacy_key)?;
    match (position_index, legacy_index) {
        (true, true) => Err(Error::invalid(
            "after_operation_id has duplicate scoped inbox indexes; resume with after_key",
        )),
        (true, false) => Ok(position_key),
        // The timestamp locator only proves retained scope membership; the
        // normal cursor always cuts at the exact canonical Observation key.
        (false, true) => Ok(position_key),
        (false, false) => Err(Error::invalid(
            "after_operation_id has no retained scoped inbox index",
        )),
    }
}

fn inbox_key_exists(db: &Connection, key: &str) -> Result<bool> {
    Ok(db.query_row(
        "SELECT EXISTS(SELECT 1 FROM meta WHERE key=?1)",
        [key],
        |row| row.get(0),
    )?)
}

fn inbox_operation_id_from_key(prefix: &str, key: &str) -> Option<String> {
    let suffix = key.strip_prefix(prefix)?;
    let (order, encoded_operation_id) = suffix.rsplit_once(':')?;
    if let Some(position) = order.strip_prefix("v2:") {
        let value = position.parse::<i64>().ok()?;
        if value <= 0 || position.len() != 20 || format!("{value:020}") != position {
            return None;
        }
    } else {
        let timestamp = order.parse::<i64>().ok()?;
        if timestamp <= 0 || order.len() != 20 || format!("{timestamp:020}") != order {
            return None;
        }
    }
    if encoded_operation_id.is_empty() || encoded_operation_id.len() % 2 != 0 {
        return None;
    }
    let mut decoded = Vec::with_capacity(encoded_operation_id.len() / 2);
    for pair in encoded_operation_id.as_bytes().as_chunks::<2>().0 {
        let byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
        decoded.push(byte);
    }
    let operation_id = String::from_utf8(decoded).ok()?;
    if operation_id.is_empty()
        || operation_id.len() > 256
        || keys::key_component(&operation_id) != encoded_operation_id
    {
        return None;
    }
    Some(operation_id)
}

fn operation_read_position(db: &Connection, operation_id: &str) -> Result<Option<(i64, i64)>> {
    Ok(db
        .query_row(
            "SELECT observation_id,recorded_at_ms FROM observations \
             WHERE source_stream_id='controller:read-position:operation' AND source_event_key=?1",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?)
}

fn mailbox_position_key(scope: &str, recipient: &str, position: i64, operation_id: &str) -> String {
    format!(
        "{}v2:{position:020}:{}",
        keys::mailbox_prefix(scope, recipient),
        keys::key_component(operation_id),
    )
}

/// One bounded sweep over existing legacy locators. The scoped hint advances
/// over every examined row and retains completion plus unresolved damage.
/// Legacy locator changes invalidate the hint; evidence-only repairs explicitly
/// discard it. Inserts, retires and hint changes commit together.
/// The returned checkpoint means order is still unknown: no inbox items may
/// be exposed. Remaining legacy rows are damage until a later sweep can
/// normalize them; only validated v2 locators ever deliver messages.
struct InboxNormalization {
    next: Option<String>,
    damaged: usize,
}

// Invalidate only a scoped legacy sweep affected by changed locator bytes.
// These triggers retain no delivery index and cannot authorize an item. Ordinary
// v2 writes and same-byte updates do not rearm known legacy poison. A repair to
// Observation/Operation evidence or a v2 twin must also delete its scoped hint.
fn install_mailbox_normalization_invalidation(tx: &Transaction<'_>) -> Result<()> {
    // Mailbox keys are fixed namespace + scope + hex recipient + locator.
    // Extract through the recipient colon, then use SQLite hex to address the
    // same hint key as keys::key_component. Each event does point lookups only.
    let hint = |row: &str| {
        format!(
            "'coordination:mailbox-normalization:'||lower(hex(substr({row}.key,1,\
             21+instr(substr({row}.key,22),':')+\
             instr(substr({row}.key,22+instr(substr({row}.key,22),':')),':'))))"
        )
    };
    let old_hint = hint("OLD");
    let new_hint = hint("NEW");
    tx.execute_batch(&format!(
        "CREATE TRIGGER IF NOT EXISTS mailbox_normalization_legacy_insert
         AFTER INSERT ON meta WHEN NEW.key LIKE 'coordination:mailbox:%'
         BEGIN
           DELETE FROM meta
           WHERE key={new_hint}
             AND NEW.key>=json_extract(value_json,'$.prefix')
             AND NEW.key<json_extract(value_json,'$.prefix')||'v';
         END;
         CREATE TRIGGER IF NOT EXISTS mailbox_normalization_legacy_delete
         AFTER DELETE ON meta WHEN OLD.key LIKE 'coordination:mailbox:%'
         BEGIN
           DELETE FROM meta
           WHERE key={old_hint}
             AND OLD.key>=json_extract(value_json,'$.prefix')
             AND OLD.key<json_extract(value_json,'$.prefix')||'v';
         END;
         CREATE TRIGGER IF NOT EXISTS mailbox_normalization_legacy_update
         AFTER UPDATE OF key,value_json ON meta
         WHEN (OLD.key LIKE 'coordination:mailbox:%' OR NEW.key LIKE 'coordination:mailbox:%')
           AND (OLD.key<>NEW.key OR OLD.value_json<>NEW.value_json)
         BEGIN
           DELETE FROM meta
           WHERE key IN ({old_hint},{new_hint})
             AND ((OLD.key>=json_extract(value_json,'$.prefix')
                   AND OLD.key<json_extract(value_json,'$.prefix')||'v')
               OR (NEW.key>=json_extract(value_json,'$.prefix')
                   AND NEW.key<json_extract(value_json,'$.prefix')||'v'));
         END;"
    ))?;
    Ok(())
}

fn normalize_legacy_mailbox_indexes(
    db: &Connection,
    scope: &ScopeData,
    recipient: &str,
    bound: i64,
) -> Result<InboxNormalization> {
    let bound = bound.clamp(1, keys::MAX_INBOX_SCAN);
    let prefix = keys::mailbox_prefix(&scope.scope_id, recipient);
    let legacy_upper = format!("{prefix}v");
    let checkpoint_key = format!(
        "coordination:mailbox-normalization:{}",
        keys::key_component(&prefix),
    );
    // Completion is a scheduling hint, not an ordering position. The reader
    // still validates each v2 item against its exact retained Observation.
    // Keep known unresolved damage visible without rewriting the hint on reads.
    if let Some(record) = meta(db, &checkpoint_key)?
        && record["state"] == "complete"
    {
        if record["prefix"].as_str() != Some(prefix.as_str()) {
            return Err(Error::new(
                "INBOX_INDEX_DAMAGED",
                "completed mailbox sweep scope is invalid",
            ));
        }
        let damaged = record["damaged_count"]
            .as_u64()
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| {
                Error::new(
                    "INBOX_INDEX_DAMAGED",
                    "completed mailbox sweep damage count is invalid",
                )
            })?;
        return Ok(InboxNormalization {
            next: None,
            damaged,
        });
    }
    // Pure v2 mailboxes do not need a write transaction on every read.
    if !db.query_row(
        "SELECT EXISTS(SELECT 1 FROM meta WHERE key>=?1 AND key<?2) \
         OR EXISTS(SELECT 1 FROM meta WHERE key=?3)",
        params![prefix, legacy_upper, checkpoint_key],
        |row| row.get::<_, bool>(0),
    )? {
        return Ok(InboxNormalization {
            next: None,
            damaged: 0,
        });
    }
    let tx = Transaction::new_unchecked(db, TransactionBehavior::Immediate)?;
    install_mailbox_normalization_invalidation(&tx)?;
    let checkpoint = meta(&tx, &checkpoint_key)?;
    let mut damaged = match checkpoint.as_ref() {
        None => 0,
        Some(record) => record["damaged_count"]
            .as_u64()
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| {
                Error::new(
                    "INBOX_INDEX_DAMAGED",
                    "mailbox normalization damage count is invalid",
                )
            })?,
    };
    let after = match checkpoint.as_ref() {
        None => None,
        Some(record) => Some(
            record["after_key"]
                .as_str()
                .filter(|key| key.starts_with(&prefix) && *key < legacy_upper.as_str())
                .ok_or_else(|| {
                    Error::new(
                        "INBOX_INDEX_DAMAGED",
                        "mailbox normalization checkpoint is invalid",
                    )
                })?,
        ),
    };
    let (lower, comparison) = match after {
        Some(key) => (key, ">"),
        None => (prefix.as_str(), ">="),
    };
    let rows: Vec<(String, String)> = {
        let sql = format!(
            "SELECT key,value_json FROM meta WHERE key {comparison} ?1 AND key<?2 \
             ORDER BY key LIMIT ?3"
        );
        let mut statement = tx.prepare(&sql)?;
        statement
            .query_map(params![lower, legacy_upper, bound + 1], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<std::result::Result<_, _>>()?
    };
    for (key, raw_index) in rows.iter().take(bound as usize) {
        // Count every unretired locator once across this bounded sweep.
        damaged = damaged.saturating_add(1);
        let Some(operation_id) = inbox_operation_id_from_key(&prefix, key) else {
            continue;
        };
        let Some((position, indexed_at_ms)) = operation_read_position(&tx, &operation_id)? else {
            continue;
        };
        if position <= 0 {
            continue;
        }
        let expected_position_key =
            mailbox_position_key(&scope.scope_id, recipient, position, &operation_id);
        let expected_legacy_key =
            keys::mailbox_key(&scope.scope_id, recipient, indexed_at_ms, &operation_id);
        if key.as_str() != expected_legacy_key.as_str() {
            continue;
        }
        let operation: Option<(String, i64)> = tx
            .query_row(
                "SELECT caller_id,created_at_ms FROM operations WHERE operation_id=?1 \
                 AND method IN ('coordination.send','coordination.consult') \
                 AND state='settled'",
                [&operation_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((sender, created_at)) = operation else {
            continue;
        };
        if indexed_at_ms != created_at {
            continue;
        }
        let index: Value = match serde_json::from_str(raw_index) {
            Ok(index) => index,
            Err(_) => continue,
        };
        // The legacy locator must already agree with the authoritative
        // receipt on every field the inbox checks; anything else stays
        // exactly as the inbox would report it (damage or stale).
        if index["operation_id"].as_str() != Some(operation_id.as_str())
            || index["created_at_ms"].as_i64() != Some(created_at)
            || index["sender"].as_str() != Some(sender.as_str())
            || index["recipient"].as_str() != Some(recipient)
            || index["task_id"] != scope.task["task_id"]
            || index["task_revision"] != scope.task["revision"]
            || index["attempt_id"] != scope.attempt["attempt_id"]
        {
            continue;
        }
        if inbox_key_exists(&tx, &expected_position_key)? {
            let twin: Option<String> = tx
                .query_row(
                    "SELECT value_json FROM meta WHERE key=?1",
                    [&expected_position_key],
                    |row| row.get(0),
                )
                .optional()?;
            let twin_valid = twin.as_deref().is_some_and(|raw| {
                serde_json::from_str::<Value>(raw).is_ok_and(|twin_index| {
                    twin_index["operation_id"].as_str() == Some(operation_id.as_str())
                        && twin_index["read_position"].as_i64() == Some(position)
                        && twin_index["created_at_ms"].as_i64() == Some(created_at)
                        && twin_index["sender"].as_str() == Some(sender.as_str())
                        && twin_index["recipient"].as_str() == Some(recipient)
                        && twin_index["task_id"] == scope.task["task_id"]
                        && twin_index["task_revision"] == scope.task["revision"]
                        && twin_index["attempt_id"] == scope.attempt["attempt_id"]
                })
            });
            // Retire onto a valid canonical twin only; otherwise the
            // conflict stays explicit damage and still resumes by key.
            if twin_valid {
                tx.execute("DELETE FROM meta WHERE key=?1", [key])?;
                damaged = damaged.saturating_sub(1);
            }
            continue;
        }
        let canonical = json!({
            "operation_id": operation_id,
            "read_position": position,
            "sender": sender,
            "recipient": recipient,
            "task_id": scope.task["task_id"],
            "task_revision": scope.task["revision"],
            "attempt_id": scope.attempt["attempt_id"],
            "created_at_ms": created_at,
        });
        set_meta(&tx, &expected_position_key, &canonical)?;
        tx.execute("DELETE FROM meta WHERE key=?1", [key])?;
        damaged = damaged.saturating_sub(1);
    }
    let next = if rows.len() > bound as usize {
        let key = rows[bound as usize - 1].0.clone();
        set_meta(
            &tx,
            &checkpoint_key,
            &json!({"prefix":prefix,"state":"pending", "after_key":key,"damaged_count":damaged}),
        )?;
        Some(key)
    } else {
        set_meta(
            &tx,
            &checkpoint_key,
            &json!({"prefix":prefix,"state":"complete","after_key":null,"damaged_count":damaged}),
        )?;
        None
    };
    tx.commit()?;
    Ok(InboxNormalization { next, damaged })
}

fn delivery_matches_scope(result: &Value, scope: &ScopeData, recipient: &str) -> Result<bool> {
    let Some(text) = result.get("text").and_then(Value::as_str) else {
        return Ok(false);
    };
    let envelope: Value = match serde_json::from_str(text) {
        Ok(envelope) => envelope,
        Err(_) => return Ok(false),
    };
    let legacy = envelope["schema"] == "eliot.coordination.message.v1"
        && envelope["task_id"] == scope.task["task_id"]
        && envelope["task_revision"] == scope.task["revision"]
        && envelope["attempt_id"] == scope.attempt["attempt_id"]
        && result["task_id"] == scope.task["task_id"]
        && result["task_revision"] == scope.task["revision"]
        && result["attempt_id"] == scope.attempt["attempt_id"];
    let versioned = envelope["schema"] == "eliot.coordination.message.v2"
        && envelope["target_context"]["actor"]["client_id"] == recipient
        && envelope["target_context"]["task_id"] == scope.task["task_id"]
        && envelope["target_context"]["task_revision"] == scope.task["revision"]
        && envelope["target_context"]["attempt_id"] == scope.attempt["attempt_id"]
        && envelope["target_context"]["grant_revision"] == scope.registration["grant_revision"]
        && envelope["target_context"]["actor"]["participation_basis"]
            == scope.registration["participation_basis"]
        && envelope["target_context"]["binding_id"]
            == scope
                .registration
                .get("binding_id")
                .cloned()
                .unwrap_or(Value::Null)
        && envelope["target_context"]["binding_generation"]
            == scope
                .registration
                .get("binding_generation")
                .cloned()
                .unwrap_or(Value::Null)
        && envelope["source_context"]["actor"]["client_id"] == result["sender"]
        && envelope["source_context"]["task_id"] == result["task_id"]
        && envelope["source_context"]["task_revision"] == result["task_revision"]
        && envelope["source_context"]["attempt_id"] == result["attempt_id"]
        && envelope["source_context"] == result["source_context"]
        && envelope["target_context"] == result["target_context"]
        && envelope["relation"] == result["relation"];
    Ok((legacy || versioned)
        && envelope["sender"] == result["sender"]
        && envelope["recipient"] == recipient
        && result["recipient"] == recipient)
}

fn retained_relation_matches_delivery(
    db: &Connection,
    envelope: &Value,
    target: &ScopeData,
    recipient: &str,
    sender: &str,
) -> Result<bool> {
    if envelope["schema"] == "eliot.coordination.message.v1" {
        return Ok(true);
    }
    let source_context = &envelope["source_context"];
    let target_context = &envelope["target_context"];
    if envelope["schema"] != "eliot.coordination.message.v2"
        || target_context != &scope_context(target)
        || target_context
            .pointer("/actor/client_id")
            .and_then(Value::as_str)
            != Some(recipient)
        || source_context
            .pointer("/actor/client_id")
            .and_then(Value::as_str)
            != Some(sender)
        || !retained_relation_contexts_match(&envelope["relation"], source_context, target_context)
    {
        return Ok(false);
    }
    let ask_id = envelope["body"]["ask_id"].as_str();
    match load_current_scope_for_client(db, sender) {
        Ok(source) if scope_context(&source) == *source_context => {
            relation_snapshot_current(db, &source, target, &envelope["relation"], ask_id)
        }
        // The receipt remains pinned to the source context that was validated
        // at send time. A later assignment or grant change cannot redirect it.
        Ok(_) => Ok(true),
        Err(error) if is_stale_watch_authority(&error) => Ok(true),
        Err(error) => Err(error),
    }
}

fn retained_relation_contexts_match(
    relation: &Value,
    source_context: &Value,
    target_context: &Value,
) -> bool {
    let context_matches_tuple = |context: &Value, prefix: &str, tuple: &Value| {
        let task_id_key = format!("{prefix}_task_id");
        let task_revision_key = format!("{prefix}_task_revision");
        let attempt_id_key = format!("{prefix}_attempt_id");
        context["task_id"] == tuple[task_id_key.as_str()]
            && context["task_revision"] == tuple[task_revision_key.as_str()]
            && tuple
                .get(attempt_id_key.as_str())
                .is_none_or(|attempt| context["attempt_id"] == *attempt)
    };
    match relation["kind"].as_str() {
        Some("same_task_assignment") => {
            context_matches_tuple(source_context, "source", relation)
                && context_matches_tuple(target_context, "target", relation)
                && source_context["task_id"] == target_context["task_id"]
                && source_context["task_revision"] == target_context["task_revision"]
                && source_context["attempt_id"] == target_context["attempt_id"]
        }
        Some("task_dependency") => {
            context_matches_tuple(source_context, "source", relation)
                && context_matches_tuple(target_context, "target", relation)
                && matches!(
                    relation["direction"].as_str(),
                    Some("source_depends_on_target" | "target_depends_on_source")
                )
                && relation["required_phase"].as_str().is_some()
        }
        Some("sponsored_review") => {
            let review_context = |context: &Value| {
                context["task_id"] == relation["task_id"]
                    && context["task_revision"] == relation["task_revision"]
                    && context["attempt_id"] == relation["attempt_id"]
                    && context
                        .pointer("/actor/participation_basis/kind")
                        .and_then(Value::as_str)
                        == Some("sponsored_reviewer")
                    && context
                        .pointer("/actor/participation_basis/review_scope/task_id")
                        .and_then(Value::as_str)
                        == relation["task_id"].as_str()
                    && context
                        .pointer("/actor/participation_basis/review_scope/task_revision")
                        .and_then(Value::as_i64)
                        == relation["task_revision"].as_i64()
                    && context
                        .pointer("/actor/participation_basis/review_scope/attempt_id")
                        .and_then(Value::as_str)
                        == relation["attempt_id"].as_str()
                    && context
                        .pointer("/actor/participation_basis/review_scope/submission_ref")
                        .and_then(Value::as_str)
                        == relation["submission_ref"].as_str()
                    && context
                        .pointer("/actor/participation_basis/review_scope/candidate_ref")
                        .and_then(Value::as_str)
                        == relation["candidate_ref"].as_str()
            };
            relation["review_assignment_id"].as_str().is_some()
                && ((relation["reviewer_client_id"]
                    == source_context
                        .pointer("/actor/client_id")
                        .cloned()
                        .unwrap_or(Value::Null)
                    && review_context(source_context))
                    || (relation["reviewer_client_id"]
                        == target_context
                            .pointer("/actor/client_id")
                            .cloned()
                            .unwrap_or(Value::Null)
                        && review_context(target_context)))
                && source_context["task_id"] == target_context["task_id"]
                && source_context["task_revision"] == target_context["task_revision"]
                && source_context["attempt_id"] == target_context["attempt_id"]
        }
        Some("contract_key") => {
            let card_matches = |card: &Value, context: &Value| {
                card["client_id"]
                    == context
                        .pointer("/actor/client_id")
                        .cloned()
                        .unwrap_or(Value::Null)
                    && card["task_id"] == context["task_id"]
                    && card["task_revision"] == context["task_revision"]
                    && card["attempt_id"] == context["attempt_id"]
            };
            let contract_role = |card: &Value| {
                matches!(
                    card["role"].as_str(),
                    Some("producer" | "consumer" | "carrier")
                )
            };
            relation["term"].as_str().is_some()
                && card_matches(&relation["source_card"], source_context)
                && card_matches(&relation["target_card"], target_context)
                && contract_role(&relation["source_card"])
                && contract_role(&relation["target_card"])
                && relation["source_card"]["identity"] == relation["term"]
                && relation["target_card"]["identity"] == relation["term"]
        }
        Some("accepted_scope_overlap") => {
            let scope_ref_valid = |scope: &Value, context: &Value| {
                scope["scope_intent_id"].as_str().is_some()
                    && scope["state_revision"]
                        .as_i64()
                        .is_some_and(|revision| revision > 0)
                    && scope["digest"].as_str().is_some_and(|digest| {
                        digest.len() == 64
                            && digest
                                .bytes()
                                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    })
                    && scope["owner_client_id"]
                        == context
                            .pointer("/actor/client_id")
                            .cloned()
                            .unwrap_or(Value::Null)
            };
            relation["term"].as_str().is_some()
                && matches!(
                    relation["term_kind"].as_str(),
                    Some("path" | "symbol" | "interface")
                )
                && scope_ref_valid(&relation["source_scope"], source_context)
                && scope_ref_valid(&relation["target_scope"], target_context)
        }
        Some("active_integration_cell") => {
            let tuple_matches = |context: &Value| {
                relation["task_revision_set"]
                    .as_array()
                    .is_some_and(|tuples| {
                        tuples.iter().any(|tuple| {
                            tuple["task_id"] == context["task_id"]
                                && tuple["task_revision"] == context["task_revision"]
                                && tuple["attempt_id"] == context["attempt_id"]
                        })
                    })
            };
            relation["cell_id"].as_str().is_some()
                && relation["contract_key"].as_str().is_some()
                && tuple_matches(source_context)
                && tuple_matches(target_context)
        }
        Some("unresolved_coordination") => {
            let direction_matches = (relation["source_context"] == *source_context
                && relation["target_context"] == *target_context)
                || (relation["source_context"] == *target_context
                    && relation["target_context"] == *source_context);
            relation["ask_id"].as_str().is_some()
                && direction_matches
                && retained_relation_contexts_match(
                    &relation["original_relation"],
                    source_context,
                    target_context,
                )
        }
        _ => false,
    }
}

fn consult_delivery_matches(
    result: &Value,
    request: &Value,
    body: &Value,
    sender: &str,
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
        && body["task_id"] == result["task_id"]
        && body["task_revision"] == result["task_revision"]
        && body["attempt_id"] == result["attempt_id"]
        && body["sender"] == sender
        && body["recipient"] == result["recipient"]
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
    let envelope: Value = serde_json::from_str(model::text(&mailbox_result, "text")?)?;
    let target_context = envelope["target_context"].clone();
    let target_scope_id = keys::scope_id(
        model::text(&target_context, "task_id")?,
        model::positive(&target_context, "task_revision")?,
        model::text(&target_context, "attempt_id")?,
    )?;
    let Some((read_position, _)) = operation_read_position(tx, operation_id)? else {
        return Err(Error::new(
            "STORE_INVARIANT",
            "admitted coordination Operation has no read-position marker",
        ));
    };
    if read_position <= 0 {
        return Err(Error::new(
            "STORE_INVARIANT",
            "admitted coordination Operation has an invalid read-position marker",
        ));
    }
    let index_key = mailbox_position_key(&target_scope_id, recipient, read_position, operation_id);
    set_meta(
        tx,
        &index_key,
        &json!({
            "operation_id":operation_id,
            "read_position":read_position,
            "sender":principal.client_id,
            "recipient":recipient,
            "task_id":target_context["task_id"],
            "task_revision":target_context["task_revision"],
            "attempt_id":target_context["attempt_id"],
            "created_at_ms":now,
        }),
    )?;
    remember_or_resolve_coordination(tx, &envelope, operation_id)?;
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
        "source_context":envelope["source_context"],
        "target_context":target_context,
        "relation":envelope["relation"],
        "payload_digest":mailbox_result["payload_digest"],
        "text":mailbox_result["text"],
        "delivery":"durable_mailbox_only",
    }))
}

/// Consult asks keep a narrow pair edge until an exact typed reply settles the
/// ask. The edge records the original relation and both immutable grant
/// contexts, so it cannot be reused after reassignment or revocation.
fn remember_or_resolve_coordination(
    tx: &Transaction<'_>,
    envelope: &Value,
    operation_id: &str,
) -> Result<()> {
    let source_context = &envelope["source_context"];
    let target_context = &envelope["target_context"];
    let source_id = source_context
        .pointer("/actor/client_id")
        .and_then(Value::as_str);
    let target_id = target_context
        .pointer("/actor/client_id")
        .and_then(Value::as_str);
    let (Some(source_id), Some(target_id)) = (source_id, target_id) else {
        return Err(Error::new(
            "STORE_INVARIANT",
            "versioned coordination message omitted an exact actor context",
        ));
    };
    let key = pending_peer_relation_key(source_id, target_id);
    let body = &envelope["body"];
    if body["schema"] == "eliot.coordination.consult.v1" {
        let ask_id = model::text(body, "ask_id")?.to_owned();
        let mut record = meta(tx, &key)?.unwrap_or_else(|| json!({"asks":[]}));
        let asks = record
            .get_mut("asks")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| {
                Error::new("STORE_INVARIANT", "pending coordination index is malformed")
            })?;
        let candidate = json!({
            "ask_id":ask_id,
            "source_context":source_context,
            "target_context":target_context,
            "original_relation":envelope["relation"],
            "operation_id":operation_id,
        });
        if let Some(existing) = asks.iter().find(|item| item["ask_id"] == ask_id) {
            if existing != &candidate {
                return Err(Error::new(
                    "STORE_INVARIANT",
                    "consult ask identity was reused for a different coordination delivery",
                ));
            }
        } else {
            asks.push(candidate);
            asks.sort_by(|left, right| left["ask_id"].as_str().cmp(&right["ask_id"].as_str()));
        }
        set_meta(tx, &key, &record)?;
    } else if body["schema"] == "eliot.coordination.reply.v1"
        && let Some(ask_id) = body.get("ask_id").and_then(Value::as_str)
        && let Some(mut record) = meta(tx, &key)?
    {
        let asks = record
            .get_mut("asks")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| {
                Error::new("STORE_INVARIANT", "pending coordination index is malformed")
            })?;
        asks.retain(|ask| {
            !(ask["ask_id"] == ask_id
                && ask["source_context"] == *target_context
                && ask["target_context"] == *source_context)
        });
        if asks.is_empty() {
            tx.execute("DELETE FROM meta WHERE key=?1", [&key])?;
        } else {
            set_meta(tx, &key, &record)?;
        }
    }
    Ok(())
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
    let (cards, index_more, stale, migration_gap) = consult_matching_cards(tx, &sender, &request)?;
    if index_more || stale > 0 || migration_gap.is_some() {
        let mut gaps = Vec::new();
        if index_more {
            gaps.push(json!({"kind":"consult_relevance_scan_bound","count":null}));
        }
        if stale > 0 {
            gaps.push(json!({"kind":"stale_or_corrupt_consult_card_index_entries","count":stale}));
        }
        if let Some(gap) = migration_gap {
            gaps.push(gap);
        }
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
                "gaps":gaps,
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
    let mut field_matches_by_owner = BTreeMap::<String, BTreeMap<String, &ConsultCardMatch>>::new();
    for candidate in &field_matches {
        let answer = &candidate.card["fields"][request.field.as_str()];
        let canonical = model::canonical(answer)?;
        field_matches_by_owner
            .entry(candidate.indexed.client_id.clone())
            .or_default()
            .entry(canonical)
            .or_insert(*candidate);
    }
    if field_matches_by_owner.len() == 1
        && field_matches_by_owner
            .values()
            .next()
            .is_some_and(|facts| facts.len() == 1)
    {
        let candidate = *field_matches_by_owner
            .values()
            .next()
            .and_then(|facts| facts.values().next())
            .expect("one owner and one card fact were checked");
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
    if field_matches_by_owner.len() > 1
        || field_matches_by_owner.values().any(|facts| facts.len() > 1)
    {
        let candidates = field_matches_by_owner
            .values()
            .flat_map(|facts| facts.values().copied())
            .collect::<Vec<_>>();
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
                "candidate_count":field_matches_by_owner.len(),
                "candidates":consult_candidates(&candidates),
                "gap":"conflicting_or_ambiguous_current_card_field",
                "delivery_created":false,
                "coverage":"complete",
                "gaps":[],
            }),
        );
    }

    let mut owner_matches = Vec::new();
    let mut seen_owners = BTreeSet::new();
    let owner_candidates: Vec<&ConsultCardMatch> = cards.iter().collect();
    for candidate in owner_candidates {
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
) -> Result<(Vec<ConsultCardMatch>, bool, usize, Option<Value>)> {
    let raw_limit = keys::MAX_INBOX_SCAN;
    let page = indexed_global_cards(
        db,
        request.target_kind,
        &request.target_value,
        None,
        None,
        raw_limit,
    )?;
    let mut cards = Vec::new();
    let mut stale = page.stale_entries;
    for indexed in page.cards {
        if !indexed.valid_index {
            stale = stale.saturating_add(1);
            continue;
        }
        if indexed.client_id == sender.client_id.as_deref().unwrap_or_default() {
            continue;
        }
        let participant = match load_current_scope_for_client(db, &indexed.client_id) {
            Ok(participant) => participant,
            Err(error) if is_stale_watch_authority(&error) => {
                stale = stale.saturating_add(1);
                continue;
            }
            Err(error) => return Err(error),
        };
        let Some(card) = current_card(
            db,
            &participant.scope_id,
            &indexed.card_kind,
            &indexed.identity,
            &indexed.client_id,
        )?
        else {
            stale = stale.saturating_add(1);
            continue;
        };
        if card["client_id"] != indexed.client_id
            || card["task_id"] != participant.task["task_id"]
            || card["task_revision"] != participant.task["revision"]
            || card["attempt_id"] != participant.attempt["attempt_id"]
            || card["card_kind"] != indexed.card_kind
            || card["identity"] != indexed.identity
            || indexed.scope_id.as_deref() != Some(participant.scope_id.as_str())
            || indexed.card_revision != card["card_revision"].as_i64()
        {
            stale = stale.saturating_add(1);
            continue;
        }
        let indexed_fields =
            indexable_card_fields(&card["fields"], &indexed.card_kind, &indexed.identity);
        let exact_term = keys::indexed_terms(&indexed_fields, &indexed.card_kind)
            .get(&request.target_kind)
            .is_some_and(|terms| terms.contains(&request.target_value));
        if !exact_term {
            stale = stale.saturating_add(1);
            continue;
        }
        if current_peer_relation(
            db,
            sender,
            &participant,
            Some(&PeerSelector::Term(
                request.target_kind,
                request.target_value.clone(),
            )),
        )?
        .is_none()
        {
            continue;
        }
        cards.push(ConsultCardMatch {
            indexed,
            participant,
            card,
        });
    }
    let migration_gap = legacy_index_backfill_gap_for(
        db,
        &[],
        Some((request.target_kind, &request.target_value)),
        &[],
    )?;
    Ok((cards, page.has_more, stale, migration_gap))
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

fn is_lower_sha256(value: &str) -> bool {
    crate::coordination::contract::is_canonical_sha256(value)
}

pub(super) fn contract_decision_key(proposal_id: &str, revision_id: &str) -> String {
    format!("coordination:contract-decision:{proposal_id}:{revision_id}")
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ContractDecisionIdentity<'a> {
    pub(super) thread_id: &'a str,
    pub(super) task_id: &'a str,
    pub(super) task_revision: i64,
    pub(super) attempt_id: &'a str,
    pub(super) proposal_id: &'a str,
    pub(super) proposal_revision_id: &'a str,
    pub(super) proposal_digest: &'a str,
}

pub(super) fn load_contract_decision(
    db: &Connection,
    identity: ContractDecisionIdentity<'_>,
) -> Result<Option<Value>> {
    let ContractDecisionIdentity {
        thread_id,
        task_id,
        task_revision,
        attempt_id,
        proposal_id,
        proposal_revision_id,
        proposal_digest,
    } = identity;
    let Some(decision) = meta(
        db,
        &contract_decision_key(proposal_id, proposal_revision_id),
    )?
    else {
        return Ok(None);
    };
    let decision_kind = match decision.get("decision").and_then(Value::as_str) {
        Some("ratified") => "ratified",
        Some("rejected") => "rejected",
        _ => {
            return Err(Error::new(
                "CONTRACT_DECISION_DAMAGED",
                "retained contract decision has an invalid decision kind",
            ));
        }
    };
    let expected_method = match decision_kind {
        "ratified" => "coordination.contract.ratify",
        "rejected" => "coordination.contract.reject",
        _ => unreachable!(),
    };
    let observation_id = decision["observation_id"]
        .as_i64()
        .filter(|id| *id > 0)
        .ok_or_else(|| {
            Error::new(
                "CONTRACT_DECISION_DAMAGED",
                "retained contract decision has no exact Observation",
            )
        })?;
    let operation_id = model::text(&decision, "decision_operation_id")?;
    struct ContractDecisionObservationRow {
        source_stream_id: String,
        source_event_key: Option<String>,
        operation_id: Option<String>,
        kind: String,
        payload_json: String,
        recorded_at_ms: i64,
    }
    let observation: Option<ContractDecisionObservationRow> = db
        .query_row(
            "SELECT source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms FROM observations WHERE observation_id=?1",
            [observation_id],
            |row| {
                Ok(ContractDecisionObservationRow {
                    source_stream_id: row.get(0)?,
                    source_event_key: row.get(1)?,
                    operation_id: row.get(2)?,
                    kind: row.get(3)?,
                    payload_json: row.get(4)?,
                    recorded_at_ms: row.get(5)?,
                })
            },
        )
        .optional()?;
    let Some(observation) = observation else {
        return Err(Error::new(
            "CONTRACT_DECISION_DAMAGED",
            "retained contract decision Observation is missing",
        ));
    };
    let payload: Value = serde_json::from_str(&observation.payload_json)?;
    let expected_observation_kind = match decision_kind {
        "ratified" => "coordination.contract_ratified",
        "rejected" => "coordination.contract_rejected",
        _ => unreachable!(),
    };
    let expected_payload = json!({
        "thread_id":thread_id,
        "proposal_id":proposal_id,
        "proposal_revision_id":proposal_revision_id,
        "decision_operation_id":operation_id,
    });
    if observation.source_stream_id != format!("coordination:proposal:{proposal_id}")
        || observation.source_event_key.as_deref()
            != Some(&format!("decision:{proposal_revision_id}"))
        || observation.operation_id.as_deref() != Some(operation_id)
        || observation.kind != expected_observation_kind
        || payload != expected_payload
        || decision["created_at_ms"].as_i64() != Some(observation.recorded_at_ms)
    {
        return Err(Error::new(
            "CONTRACT_DECISION_DAMAGED",
            "retained contract decision Observation identity is inconsistent",
        ));
    }
    let authority_kind = decision["authority_basis"]["kind"].as_str();
    let authority_valid = match authority_kind {
        Some("local_operator") => {
            decision["actor"]["role"] == "operator"
                && decision["authority_basis"]["gm_epoch"].is_null()
        }
        Some("attempt_owner") => {
            decision["actor"]["role"] == "manager"
                && decision["actor"]["client_id"] == decision["attempt_owner_id"]
                && decision["authority_basis"]["gm_epoch"].is_null()
        }
        Some("current_gm") => {
            decision["actor"]["role"] == "manager"
                && decision["authority_basis"]["gm_epoch"]
                    .as_i64()
                    .is_some_and(|epoch| epoch > 0)
        }
        _ => false,
    };
    if decision["schema_version"] != 1
        || decision["record_type"] != "contract_decision"
        || decision["thread_id"] != thread_id
        || decision["task_id"] != task_id
        || decision["task_revision"] != task_revision
        || decision["attempt_id"] != attempt_id
        || decision["proposal_id"] != proposal_id
        || decision["proposal_revision_id"] != proposal_revision_id
        || decision["proposal_revision"]
            .as_i64()
            .is_none_or(|revision| revision <= 0)
        || decision["proposal_digest"] != proposal_digest
        || decision["thread_state_revision"]
            .as_i64()
            .is_none_or(|revision| revision <= 0)
        || decision["scope_coverage"] != "complete"
        || !decision["affected_scope_revisions"].is_array()
        || decision["created_at_ms"]
            .as_i64()
            .is_none_or(|created| created <= 0)
        || !authority_valid
        || decision["model_work_started"] != false
        || decision["native_execution"] != false
    {
        return Err(Error::new(
            "CONTRACT_DECISION_DAMAGED",
            "retained contract decision identity or scope is inconsistent",
        ));
    }
    let operation = operations::get_operation(db, operation_id)?;
    let result = &operation["result"];
    if operation["method"] != expected_method
        || operation["state"] != "settled"
        || operation["task_id"] != task_id
        || operation["attempt_id"] != attempt_id
        || result["operation_id"] != operation_id
        || result["decision"] != decision_kind
        || result["thread_id"] != thread_id
        || result["task_id"] != task_id
        || result["task_revision"] != task_revision
        || result["attempt_id"] != attempt_id
        || result["proposal_id"] != proposal_id
        || result["proposal_revision_id"] != proposal_revision_id
        || result["proposal_digest"] != proposal_digest
        || result["decision_observation_id"] != observation_id
    {
        return Err(Error::new(
            "CONTRACT_DECISION_DAMAGED",
            "contract decision does not match its settled producer Operation",
        ));
    }
    Ok(Some(decision))
}

fn load_contract_proposal_revision_with_head(
    db: &Connection,
    thread_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    proposal_revision_id: &str,
) -> Result<(Value, Value)> {
    let pointer =
        meta(db, &proposal_revision_pointer_key(proposal_revision_id))?.ok_or_else(|| {
            Error::new(
                "CONTRACT_PROPOSAL_NOT_FOUND",
                "proposal revision is not retained",
            )
        })?;
    let proposal_id = model::text(&pointer, "proposal_id")?;
    let pointer_revision = pointer["revision"]
        .as_i64()
        .filter(|revision| *revision > 0)
        .ok_or_else(|| {
            Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "proposal revision pointer has no valid revision number",
            )
        })?;
    if pointer["thread_id"] != thread_id {
        return Err(Error::new(
            "FORBIDDEN",
            "proposal revision is outside this exact Thread",
        ));
    }

    let revision = meta(
        db,
        &proposal_revision_key(proposal_id, proposal_revision_id),
    )?
    .ok_or_else(|| {
        Error::new(
            "COORDINATION_INDEX_CORRUPT",
            "proposal revision pointer has no revision record",
        )
    })?;
    let revision_number = revision["revision"]
        .as_i64()
        .filter(|revision| *revision > 0)
        .ok_or_else(|| Error::new("PROPOSAL_DAMAGED", "proposal revision number is invalid"))?;
    if revision["schema_version"] != 1
        || revision["record_type"] != "contract_proposal_revision"
        || revision["proposal_id"] != proposal_id
        || revision["proposal_revision_id"] != proposal_revision_id
        || revision_number != pointer_revision
        || revision["thread_id"] != thread_id
        || revision["task_id"] != task_id
        || revision["task_revision"] != task_revision
        || revision["attempt_id"] != attempt_id
    {
        return Err(Error::new(
            "PROPOSAL_DAMAGED",
            "proposal revision identity or Task scope is inconsistent",
        ));
    }
    let proposal = revision
        .get("proposal")
        .filter(|proposal| proposal.is_object())
        .ok_or_else(|| Error::new("PROPOSAL_DAMAGED", "proposal body is not an object"))?;
    let proposal_digest = model::text(&revision, "proposal_digest")?;
    if !is_lower_sha256(proposal_digest)
        || model::digest(model::canonical(proposal)?.as_bytes()) != proposal_digest
    {
        return Err(Error::new(
            "PROPOSAL_DAMAGED",
            "proposal digest does not match the canonical proposal body",
        ));
    }

    let head = meta(db, &proposal_head_key(proposal_id))?.ok_or_else(|| {
        Error::new(
            "COORDINATION_INDEX_CORRUPT",
            "proposal revision has no proposal header",
        )
    })?;
    let revision_count = head["revision_count"]
        .as_i64()
        .filter(|count| *count > 0)
        .ok_or_else(|| {
            Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "proposal header revision count is invalid",
            )
        })?;
    let latest_revision_id = model::text(&head, "latest_revision_id")?;
    let latest_digest = model::text(&head, "latest_digest")?;
    if head["schema_version"] != 1
        || head["record_type"] != "contract_proposal_head"
        || head["proposal_id"] != proposal_id
        || head["thread_id"] != thread_id
        || head["proposal_sequence"]
            .as_i64()
            .is_none_or(|sequence| sequence <= 0)
        || !is_lower_sha256(latest_digest)
        || revision_number > revision_count
        || (latest_revision_id == proposal_revision_id
            && (latest_digest != proposal_digest || revision_count != revision_number))
        || (latest_revision_id != proposal_revision_id && revision_number >= revision_count)
    {
        return Err(Error::new(
            "COORDINATION_INDEX_CORRUPT",
            "proposal header is inconsistent with the retained revision",
        ));
    }

    let operation_id = model::text(&revision, "operation_id")?;
    let operation = operations::get_operation(db, operation_id)?;
    if operation["method"] != "coordination.contract.propose"
        || operation["state"] != "settled"
        || operation["task_id"] != task_id
        || operation["attempt_id"] != attempt_id
        || operation["result"]["proposal_id"] != proposal_id
        || operation["result"]["proposal_revision_id"] != proposal_revision_id
        || operation["result"]["proposal_digest"] != proposal_digest
        || operation["result"]["revision"] != revision_number
    {
        return Err(Error::new(
            "PROPOSAL_DAMAGED",
            "proposal revision does not match its settled producer Operation",
        ));
    }
    Ok((revision, head))
}

pub(super) fn load_contract_proposal_revision(
    db: &Connection,
    thread_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    proposal_revision_id: &str,
) -> Result<Value> {
    load_contract_proposal_revision_with_head(
        db,
        thread_id,
        task_id,
        task_revision,
        attempt_id,
        proposal_revision_id,
    )
    .map(|(revision, _)| revision)
}

pub(super) fn load_current_contract_proposal_revision(
    db: &Connection,
    thread_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    proposal_revision_id: &str,
) -> Result<Value> {
    let (revision, head) = load_contract_proposal_revision_with_head(
        db,
        thread_id,
        task_id,
        task_revision,
        attempt_id,
        proposal_revision_id,
    )?;
    if head["latest_revision_id"] != proposal_revision_id
        || head["latest_digest"] != revision["proposal_digest"]
    {
        return Err(Error::new(
            "STALE_CONTRACT_REVISION",
            "proposal revision is not the current exact proposal head",
        ));
    }
    Ok(revision)
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
            let prior = load_current_contract_proposal_revision(
                tx,
                &context.thread_id,
                &context.task_id,
                context.task_revision,
                &context.attempt_id,
                prior_revision_id,
            )?;
            let proposal_id = model::text(&prior, "proposal_id")?.to_owned();
            let head = meta(tx, &proposal_head_key(&proposal_id))?.ok_or_else(|| {
                Error::new(
                    "COORDINATION_INDEX_CORRUPT",
                    "current proposal revision has no proposal header",
                )
            })?;
            let revision_number = prior["revision"]
                .as_i64()
                .and_then(|revision| revision.checked_add(1))
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
    let revision = load_contract_proposal_revision(
        tx,
        &context.thread_id,
        &context.task_id,
        context.task_revision,
        &context.attempt_id,
        &request.proposal_revision_id,
    )?;
    if revision["proposal_id"] != request.proposal_id
        || revision["proposal_digest"] != request.proposal_digest
    {
        return Err(Error::new(
            "DIGEST_MISMATCH",
            "proposal revision digest or proposal identity does not match",
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
    let revision = load_contract_proposal_revision(
        db,
        &context.thread_id,
        &context.task_id,
        context.task_revision,
        &context.attempt_id,
        &request.proposal_revision_id,
    )?;
    if revision["proposal_id"] != request.proposal_id {
        return Err(Error::new(
            "NOT_FOUND",
            "proposal revision is outside this proposal identity",
        ));
    }
    let decision = load_contract_decision(
        db,
        ContractDecisionIdentity {
            thread_id: &context.thread_id,
            task_id: &context.task_id,
            task_revision: context.task_revision,
            attempt_id: &context.attempt_id,
            proposal_id: &request.proposal_id,
            proposal_revision_id: &request.proposal_revision_id,
            proposal_digest: model::text(&revision, "proposal_digest")?,
        },
    )?;
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
        "decision": decision,
        "decision_coverage": "complete_for_revision",
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
        let latest_revision_id = model::text(&header, "latest_revision_id")?;
        let latest = load_current_contract_proposal_revision(
            db,
            &context.thread_id,
            &context.task_id,
            context.task_revision,
            &context.attempt_id,
            latest_revision_id,
        )?;
        if latest["proposal_id"] != proposal_id
            || latest["proposal_digest"] != header["latest_digest"]
        {
            return Err(Error::new(
                "COORDINATION_INDEX_CORRUPT",
                "proposal header differs from its current canonical revision",
            ));
        }
        let decision = load_contract_decision(
            db,
            ContractDecisionIdentity {
                thread_id: &context.thread_id,
                task_id: &context.task_id,
                task_revision: context.task_revision,
                attempt_id: &context.attempt_id,
                proposal_id,
                proposal_revision_id: latest_revision_id,
                proposal_digest: model::text(&latest, "proposal_digest")?,
            },
        )?;
        let latest_decision = match decision {
            Some(decision) => json!({
                "kind":decision["decision"],
                "proposal_revision_id":decision["proposal_revision_id"],
                "decision_operation_id":decision["decision_operation_id"],
                "created_at_ms":decision["created_at_ms"],
            }),
            None => Value::Null,
        };
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
            "latest_decision": latest_decision,
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
    struct ContractOperationReadRow {
        caller_id: String,
        method: String,
        state: String,
        task_id: Option<String>,
        attempt_id: Option<String>,
        binding_id: Option<String>,
        binding_generation: Option<i64>,
        original_request_json: String,
        result_json: Option<String>,
        effective_request_json: String,
    }
    let raw: Option<ContractOperationReadRow> = db
        .query_row(
            "SELECT caller_id,method,state,task_id,attempt_id,binding_id,binding_generation,original_request_json,result_json,effective_request_json FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| {
                Ok(ContractOperationReadRow {
                    caller_id: row.get(0)?,
                    method: row.get(1)?,
                    state: row.get(2)?,
                    task_id: row.get(3)?,
                    attempt_id: row.get(4)?,
                    binding_id: row.get(5)?,
                    binding_generation: row.get(6)?,
                    original_request_json: row.get(7)?,
                    result_json: row.get(8)?,
                    effective_request_json: row.get(9)?,
                })
            },
        )
        .optional()?;
    let Some(ContractOperationReadRow {
        caller_id,
        method,
        state,
        task_id,
        attempt_id,
        binding_id,
        binding_generation,
        original_request_json: original_request,
        result_json,
        effective_request_json: effective_request,
    }) = raw
    else {
        return Ok(None);
    };
    if !matches!(
        method.as_str(),
        "coordination.contract.propose"
            | "coordination.contract.respond"
            | "coordination.contract.ratify"
            | "coordination.contract.reject"
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
    let result: Value = serde_json::from_str(result_json.as_deref().ok_or_else(|| {
        Error::new(
            "COORDINATION_INDEX_CORRUPT",
            "settled contract Operation has no result",
        )
    })?)?;
    if matches!(
        method.as_str(),
        "coordination.contract.ratify" | "coordination.contract.reject"
    ) {
        let decision_request =
            crate::coordination::contract::parse_decision_request(&method, &request)?;
        if decision_request.thread_id != context.thread_id
            || decision_request.task_id != context.task_id
            || decision_request.task_revision != context.task_revision
            || decision_request.attempt_id != context.attempt_id
        {
            return Err(Error::new(
                "NOT_FOUND",
                "contract decision Operation is outside the retained Thread scope",
            ));
        }
        let effective: Value = serde_json::from_str(&effective_request)?;
        let expected_scope = json!({
            "kind":"contract_thread",
            "thread_id":context.thread_id,
            "task_id":context.task_id,
            "task_revision":context.task_revision,
            "attempt_id":context.attempt_id,
        });
        if effective.get("coordination_scope") != Some(&expected_scope) {
            return Err(Error::new(
                "NOT_FOUND",
                "contract decision Operation lacks its exact retained contract Thread scope",
            ));
        }
        let revision = load_contract_proposal_revision(
            db,
            &context.thread_id,
            &context.task_id,
            context.task_revision,
            &context.attempt_id,
            &decision_request.proposal_revision_id,
        )?;
        let decision = load_contract_decision(
            db,
            ContractDecisionIdentity {
                thread_id: &context.thread_id,
                task_id: &context.task_id,
                task_revision: context.task_revision,
                attempt_id: &context.attempt_id,
                proposal_id: &decision_request.proposal_id,
                proposal_revision_id: &decision_request.proposal_revision_id,
                proposal_digest: &decision_request.proposal_digest,
            },
        )?
        .ok_or_else(|| {
            Error::new(
                "NOT_FOUND",
                "contract decision Operation has no retained decision record",
            )
        })?;
        if revision["proposal_id"] != decision_request.proposal_id
            || revision["proposal_digest"] != decision_request.proposal_digest
            || decision["decision_operation_id"] != operation_id
            || decision["decision"] != decision_request.kind.as_str()
            || decision["thread_state_revision"] != decision_request.expected_state_revision
            || decision["actor"]["client_id"] != caller_id
            || decision["reason"] != decision_request.reason
            || decision["conditions"] != json!(decision_request.conditions)
            || decision["caveats"] != json!(decision_request.caveats)
            || decision["affected_scope_revisions"]
                != json!(decision_request.affected_scope_revisions)
            || result["decision"] != decision_request.kind.as_str()
        {
            return Err(Error::new(
                "NOT_FOUND",
                "contract decision Operation differs from its retained decision record",
            ));
        }
        return Ok(Some(operations::get_operation(db, operation_id)?));
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
    match method.as_str() {
        "coordination.contract.propose" => {
            let proposal_id = model::text(&result, "proposal_id")?;
            let revision_id = model::text(&result, "proposal_revision_id")?;
            let revision = load_contract_proposal_revision(
                db,
                &context.thread_id,
                &context.task_id,
                context.task_revision,
                &context.attempt_id,
                revision_id,
            )?;
            if revision["operation_id"] != operation_id
                || revision["proposal_id"] != proposal_id
                || revision["proposal_revision_id"] != revision_id
                || revision["revision"] != result["revision"]
                || revision["proposal_digest"] != result["proposal_digest"]
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
            let revision_id = model::text(&result, "proposal_revision_id")?;
            let revision = load_contract_proposal_revision(
                db,
                &context.thread_id,
                &context.task_id,
                context.task_revision,
                &context.attempt_id,
                revision_id,
            )?;
            if revision["proposal_id"] != proposal_id
                || revision["proposal_digest"] != result["proposal_digest"]
            {
                return Err(Error::new(
                    "NOT_FOUND",
                    "Operation proposal revision is outside the retained Thread scope",
                ));
            }
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
