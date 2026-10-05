//! One manual, durable desired-state GitHub effect: a managed Issue label.
//!
//! The Issue and Task identities come only from the registered source map and
//! selected work pool. Writes use the existing `gh` account route. Once an
//! Operation enters `sending`, every recovery path is readback-only.

use super::{Store, capacity, current_principal, gm, mutate, operations};
use crate::{
    error::{Error, Result},
    github::{
        client::{GhCli, GitHubLabelApi, IssueLabelSnapshot, RepositoryRef},
        protocol::{self, ManagedLabelReconcileRequest, ManagedLabelRequest},
    },
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};

const METHOD: &str = "github.effect.managed_label";
const RECONCILE_METHOD: &str = "github.effect.reconcile_managed_label";

type EffectTargetRow = (
    String,
    String,
    String,
    String,
    i64,
    i64,
    i64,
    String,
    i64,
    String,
    Option<bool>,
);

#[derive(Debug, Clone)]
struct EffectTarget {
    source_id: String,
    project_id: String,
    host: String,
    owner: String,
    repo: String,
    repository_id: i64,
    issue_id: i64,
    issue_number: i64,
    task_id: String,
    task_revision: i64,
}

#[derive(Debug, Clone)]
struct RetainedEffect {
    request: ManagedLabelRequest,
    target: EffectTarget,
    result: Value,
    caller_id: String,
    attempt_id: Option<String>,
}

enum FencedSettlement {
    Settled,
    Existing,
    PreservedUnknown,
    Rejected(Error),
}

pub(super) async fn call(store: &Store, principal: Principal, value: Value) -> Result<Value> {
    call_with_api(store, principal, value, &GhCli).await
}

pub(super) async fn reconcile_call(
    store: &Store,
    principal: Principal,
    value: Value,
) -> Result<Value> {
    reconcile_call_with_api_inner(store, principal, value, &GhCli).await
}

#[cfg(test)]
pub(super) async fn reconcile_call_with_api<A: GitHubLabelApi + ?Sized>(
    store: &Store,
    principal: Principal,
    value: Value,
    api: &A,
) -> Result<Value> {
    reconcile_call_with_api_inner(store, principal, value, api).await
}

async fn reconcile_call_with_api_inner<A: GitHubLabelApi + ?Sized>(
    store: &Store,
    principal: Principal,
    value: Value,
    api: &A,
) -> Result<Value> {
    let value = protocol::validate_mutation(RECONCILE_METHOD, &value)?;
    let request = ManagedLabelReconcileRequest::parse(&value)?;
    let config = store.config.clone();
    let receipt = store
        .run({
            let principal = principal.clone();
            let value = value.clone();
            move |db| {
                let current = current_principal(db, principal)?;
                mutate(db, &current, RECONCILE_METHOD, &value, &config)
            }
        })
        .await?;
    let reconciliation_id = model::text(&receipt, "operation_id")?.to_owned();
    let operation = store
        .run({
            let reconciliation_id = reconciliation_id.clone();
            move |db| operations::get_operation(db, &reconciliation_id)
        })
        .await?;

    match operation["state"].as_str() {
        Some("settled") => return operation_result(store, &reconciliation_id).await,
        Some("rejected") => return Err(operation_error(&operation["result"])),
        Some("queued") => {}
        Some("cancelled") => {
            return Err(Error::new(
                "GITHUB_EFFECT_RECONCILE_CANCELLED",
                "the retained managed-label readback Operation was cancelled before dispatch",
            ));
        }
        _ => {
            return Err(Error::new(
                "GITHUB_EFFECT_RECONCILE_NOT_RESUMABLE",
                "the retained managed-label readback Operation is not resumable",
            ));
        }
    }

    let retained = store
        .run({
            let principal = principal.clone();
            let original_id = request.operation_id.clone();
            move |db| {
                let current = current_principal(db, principal)?;
                load_retained_effect(db, &current, &original_id)
            }
        })
        .await;
    let retained = match retained {
        Ok(retained) => retained,
        Err(error) => {
            return finish_reconcile_readback(
                store,
                &principal,
                &reconciliation_id,
                &request,
                None,
                None,
                "readback_not_started",
                Some(&error),
                false,
            )
            .await;
        }
    };
    let repository = match RepositoryRef::new(
        &retained.target.host,
        &retained.target.owner,
        &retained.target.repo,
    ) {
        Ok(repository) => repository,
        Err(error) => {
            return finish_reconcile_readback(
                store,
                &principal,
                &reconciliation_id,
                &request,
                Some(&retained),
                None,
                "readback_not_started",
                Some(&error),
                false,
            )
            .await;
        }
    };
    let readback = match read_remote(api, &repository, &retained.target).await {
        Ok(readback) => readback,
        Err(error) => {
            return finish_reconcile_readback(
                store,
                &principal,
                &reconciliation_id,
                &request,
                Some(&retained),
                None,
                "readback_unavailable",
                Some(&error),
                false,
            )
            .await;
        }
    };
    let observed_present = readback
        .labels
        .iter()
        .any(|label| label == &retained.request.label);
    if observed_present != retained.request.present {
        return finish_reconcile_readback(
            store,
            &principal,
            &reconciliation_id,
            &request,
            Some(&retained),
            Some((readback, observed_present)),
            "desired_state_not_observed",
            None,
            false,
        )
        .await;
    }

    finish_reconcile_readback(
        store,
        &principal,
        &reconciliation_id,
        &request,
        Some(&retained),
        Some((readback, observed_present)),
        "reconciled_from_readback",
        None,
        true,
    )
    .await
}

#[cfg(test)]
pub(super) async fn call_with_api<A: GitHubLabelApi + ?Sized>(
    store: &Store,
    principal: Principal,
    value: Value,
    api: &A,
) -> Result<Value> {
    call_with_api_inner(store, principal, value, api).await
}

#[cfg(not(test))]
async fn call_with_api<A: GitHubLabelApi + ?Sized>(
    store: &Store,
    principal: Principal,
    value: Value,
    api: &A,
) -> Result<Value> {
    call_with_api_inner(store, principal, value, api).await
}

async fn call_with_api_inner<A: GitHubLabelApi + ?Sized>(
    store: &Store,
    principal: Principal,
    value: Value,
    api: &A,
) -> Result<Value> {
    let value = protocol::validate_mutation(METHOD, &value)?;
    let request = ManagedLabelRequest::parse(&value)?;
    let config = store.config.clone();
    let receipt = store
        .run({
            let principal = principal.clone();
            let value = value.clone();
            move |db| {
                let current = current_principal(db, principal)?;
                mutate(db, &current, METHOD, &value, &config)
            }
        })
        .await?;
    let operation_id = model::text(&receipt, "operation_id")?.to_owned();
    let operation = store
        .run({
            let operation_id = operation_id.clone();
            move |db| operations::get_operation(db, &operation_id)
        })
        .await?;

    match operation["state"].as_str() {
        Some("settled") => return Ok(operation["result"].clone()),
        Some("rejected") => return Err(operation_error(&operation["result"])),
        Some("cancelled") => {
            return Err(Error::new(
                "GITHUB_EFFECT_CANCELLED",
                "the retained managed-label Operation was cancelled before dispatch",
            ));
        }
        Some("sending") => {
            return Err(Error::new(
                "GITHUB_EFFECT_IN_PROGRESS",
                "the managed-label effect may be in flight; inspect its Operation before retrying",
            ));
        }
        Some("queued" | "outcome_unknown") => {}
        _ => {
            return Err(Error::new(
                "GITHUB_EFFECT_NOT_RESUMABLE",
                "the retained managed-label Operation is not in a resumable state",
            ));
        }
    }

    let readback_only = operation["state"] == "outcome_unknown";
    let target = match load_target(store, &principal, &request, &operation_id, !readback_only).await
    {
        Ok(target) => target,
        Err(error) if !readback_only => {
            reject_before_target(store, &principal, &operation_id, &request, &error).await?;
            return Err(error);
        }
        Err(error) => return Err(error),
    };
    let repository = RepositoryRef::new(&target.host, &target.owner, &target.repo)?;
    let initial_readback = match read_remote(api, &repository, &target).await {
        Ok(readback) => readback,
        Err(error) if !readback_only => {
            finish_before_write(store, &principal, &operation_id, &target, &request, &error)
                .await?;
            return Err(error);
        }
        Err(error) => {
            return record_unknown(
                store,
                &principal,
                &operation_id,
                &target,
                &request,
                "readback_unavailable",
                None,
                Some(&error),
            )
            .await;
        }
    };
    let currently_present = initial_readback
        .labels
        .iter()
        .any(|label| label == &request.label);

    if readback_only {
        if currently_present == request.present {
            return settle(
                store,
                &principal,
                &operation_id,
                &target,
                &request,
                "reconciled_from_readback",
                false,
                &initial_readback,
            )
            .await;
        }
        return record_unknown(
            store,
            &principal,
            &operation_id,
            &target,
            &request,
            "desired_state_not_observed",
            Some(currently_present),
            None,
        )
        .await;
    }

    if currently_present == request.present {
        return settle(
            store,
            &principal,
            &operation_id,
            &target,
            &request,
            "already_in_desired_state",
            false,
            &initial_readback,
        )
        .await;
    }

    begin_write(store, &principal, &operation_id, &request).await?;
    // The write call is issued at most once. Even a CLI/API error after the
    // sending marker is ambiguous; only the exact Issue readback can settle it.
    let write_error = api
        .set_label(
            &repository,
            target.issue_number,
            &request.label,
            request.present,
        )
        .await
        .err();
    match read_remote(api, &repository, &target).await {
        Ok(readback)
            if readback.labels.iter().any(|label| label == &request.label) == request.present =>
        {
            settle(
                store,
                &principal,
                &operation_id,
                &target,
                &request,
                if write_error.is_some() {
                    "confirmed_after_transport_error"
                } else {
                    "applied"
                },
                true,
                &readback,
            )
            .await
        }
        Ok(readback) => {
            record_unknown(
                store,
                &principal,
                &operation_id,
                &target,
                &request,
                "desired_state_not_observed_after_write",
                Some(readback.labels.iter().any(|label| label == &request.label)),
                write_error.as_ref(),
            )
            .await
        }
        Err(error) => {
            record_unknown(
                store,
                &principal,
                &operation_id,
                &target,
                &request,
                "readback_unavailable_after_write",
                None,
                Some(&error),
            )
            .await
        }
    }
}

pub(super) fn apply(
    tx: &Transaction<'_>,
    principal: &Principal,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<(Value, bool)> {
    let request = ManagedLabelRequest::parse(value)?;
    let target = resolve_target(tx, principal, &request, true)?;
    let previous: Option<(String, String)> = tx
        .query_row(
            "SELECT s.operation_id,o.state FROM github_label_effect_slots s JOIN operations o ON o.operation_id=s.operation_id WHERE s.source_id=?1 AND s.issue_id=?2 AND s.label=?3",
            params![request.source_id, target.issue_id, request.label],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if previous.is_some_and(|(_, state)| {
        matches!(
            state.as_str(),
            "queued" | "sending" | "native_accepted" | "outcome_unknown"
        )
    }) {
        return Err(Error::conflict(
            "the exact Issue/label slot has an unresolved Operation; reconcile it before admitting another effect",
        ));
    }
    tx.execute(
        "INSERT INTO github_label_effect_slots(source_id,issue_id,label,desired_present,operation_id,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(source_id,issue_id,label) DO UPDATE SET desired_present=excluded.desired_present,operation_id=excluded.operation_id,updated_at_ms=excluded.updated_at_ms",
        params![request.source_id, target.issue_id, request.label, request.present, operation_id, now],
    )?;
    let changed = tx.execute(
        "UPDATE operations SET task_id=?2 WHERE operation_id=?1 AND method=?3 AND state='queued'",
        params![operation_id, request.task_id, METHOD],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "the managed-label Operation changed during admission",
        ));
    }
    Ok((
        json!({
            "operation_id":operation_id,
            "source_id":request.source_id,
            "task_id":request.task_id,
            "task_revision":request.expected_task_revision,
            "issue_id":target.issue_id,
            "issue_number":target.issue_number,
            "label":request.label,
            "present":request.present,
            "outcome":"managed_label_queued",
            "current_state_read_method":"operation.get"
        }),
        true,
    ))
}

/// Admit a current-GM/operator readback as its own ordinary Operation. It is
/// linked to the original Task and never takes ownership of the label slot.
pub(super) fn apply_reconcile(
    tx: &Transaction<'_>,
    principal: &Principal,
    value: &Value,
    reconciliation_id: &str,
    _now: i64,
) -> Result<(Value, bool)> {
    let request = ManagedLabelReconcileRequest::parse(value)?;
    let retained = load_retained_effect(tx, principal, &request.operation_id)?;
    let linked = tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1 AND method=?4 AND caller_id=?5 AND state='queued'",
        params![
            reconciliation_id,
            retained.target.task_id,
            retained.attempt_id,
            RECONCILE_METHOD,
            principal.client_id
        ],
    )?;
    if linked != 1 {
        return Err(Error::conflict(
            "the managed-label readback Operation changed during admission",
        ));
    }
    Ok((
        json!({
            "operation_id":reconciliation_id,
            "original_operation_id":request.operation_id,
            "source_id":retained.request.source_id,
            "project_id":retained.target.project_id,
            "task_id":retained.target.task_id,
            "repository_id":retained.target.repository_id,
            "issue_id":retained.target.issue_id,
            "issue_number":retained.target.issue_number,
            "label":retained.request.label,
            "desired_present":retained.request.present,
            "outcome":"readback_queued",
            "write_attempted":false,
            "current_state_read_method":"operation.get"
        }),
        true,
    ))
}

fn load_retained_effect(
    db: &Connection,
    principal: &Principal,
    operation_id: &str,
) -> Result<RetainedEffect> {
    gm::require_authority(db, principal)?;
    if !super::operation_visible_to(db, principal, operation_id)? {
        return Err(Error::new(
            "FORBIDDEN",
            "the unknown managed-label Operation is not visible to the current GM or Operator",
        ));
    }
    load_retained_effect_record(db, operation_id, Some(principal))
}

/// Revalidate a readback result that was already authorized and observed.
/// Permission is checked before the provider GET; this final transaction
/// protects the immutable retained identity and slot from remaps or races.
fn load_retained_effect_for_settlement(
    db: &Connection,
    operation_id: &str,
) -> Result<RetainedEffect> {
    load_retained_effect_record(db, operation_id, None)
}

fn load_retained_effect_record(
    db: &Connection,
    operation_id: &str,
    principal: Option<&Principal>,
) -> Result<RetainedEffect> {
    let raw: Option<String> = db
        .query_row(
            "SELECT json_object(\
                'operation_id',operation_id,\
                'caller_id',caller_id,\
                'client_request_id',client_request_id,\
                'method',method,\
                'state',state,\
                'task_id',task_id,\
                'attempt_id',attempt_id,\
                'original_request',json(original_request_json),\
                'effective_request',json(effective_request_json),\
                'result',json(result_json)) \
             FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| row.get(0),
        )
        .optional()?;
    let record: Value = serde_json::from_str(
        &raw.ok_or_else(|| Error::new("NOT_FOUND", "managed-label Operation was not found"))?,
    )?;
    if record["method"] != METHOD || record["state"] != "outcome_unknown" {
        return Err(Error::new(
            "GITHUB_EFFECT_NOT_UNKNOWN",
            "only the exact retained unknown managed-label Operation can be reconciled",
        ));
    }

    let original_request_value = record["original_request"].clone();
    let request = ManagedLabelRequest::parse(&original_request_value)?;
    let caller_id = model::text(&record, "caller_id")?.to_owned();
    let client_request_id = model::text(&record, "client_request_id")?;
    if request.client_request_id != client_request_id || record["task_id"] != request.task_id {
        return Err(Error::new(
            "GITHUB_EFFECT_OPERATION_MISMATCH",
            "the retained Operation request and Task identity are inconsistent",
        ));
    }
    let mut effective_request = record["effective_request"].clone();
    let receipt = effective_request
        .as_object_mut()
        .and_then(|object| object.remove("receipt"))
        .ok_or_else(|| {
            Error::new(
                "GITHUB_EFFECT_OPERATION_MISMATCH",
                "the retained Operation has no durable admission receipt",
            )
        })?;
    if receipt["ok"] != true || receipt["value"]["operation_id"] != operation_id {
        return Err(Error::new(
            "GITHUB_EFFECT_OPERATION_MISMATCH",
            "the retained Operation receipt does not identify this exact Operation",
        ));
    }
    let result = record["result"].clone();
    validate_retained_effect_result(&result, &receipt["value"], operation_id, &request)?;

    let target = match principal {
        Some(principal) => resolve_target(db, principal, &request, false)?,
        None => resolve_target_identity(db, &request, false)?,
    };
    if result["issue_id"].as_i64() != Some(target.issue_id)
        || result["issue_number"].as_i64() != Some(target.issue_number)
        || result
            .get("repository_id")
            .and_then(Value::as_i64)
            .is_some_and(|repository_id| repository_id != target.repository_id)
        || result
            .get("project_id")
            .and_then(Value::as_str)
            .is_some_and(|project_id| project_id != target.project_id)
    {
        return Err(Error::new(
            "GITHUB_EFFECT_TARGET_CHANGED",
            "the registered source no longer resolves to the exact retained repository and Issue",
        ));
    }

    let slot: Option<(i64, String)> = db
        .query_row(
            "SELECT desired_present,operation_id FROM github_label_effect_slots WHERE source_id=?1 AND issue_id=?2 AND label=?3",
            params![request.source_id, target.issue_id, request.label],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((desired_present, slot_operation_id)) = slot else {
        return Err(Error::new(
            "GITHUB_EFFECT_SLOT_MISMATCH",
            "the exact Issue/label slot is missing",
        ));
    };
    if slot_operation_id != operation_id || desired_present != i64::from(request.present) {
        return Err(Error::new(
            "GITHUB_EFFECT_SLOT_MISMATCH",
            "the exact Issue/label slot no longer records this Operation and desired state",
        ));
    }

    Ok(RetainedEffect {
        request,
        target,
        result,
        caller_id,
        attempt_id: record["attempt_id"].as_str().map(str::to_owned),
    })
}

fn validate_retained_effect_result(
    result: &Value,
    admission: &Value,
    operation_id: &str,
    request: &ManagedLabelRequest,
) -> Result<()> {
    validate_effect_tuple(admission, operation_id, request)?;
    validate_effect_tuple(result, operation_id, request)?;
    if result["issue_id"] != admission["issue_id"]
        || result["issue_number"] != admission["issue_number"]
    {
        return Err(Error::new(
            "GITHUB_EFFECT_OPERATION_MISMATCH",
            "the retained Operation result changed its admitted Issue identity",
        ));
    }
    Ok(())
}

fn validate_effect_tuple(
    value: &Value,
    operation_id: &str,
    request: &ManagedLabelRequest,
) -> Result<()> {
    let mut saved_present = None;
    for key in ["desired_present", "present"] {
        if let Some(saved_value) = value.get(key) {
            let saved_value = saved_value.as_bool().ok_or_else(|| {
                Error::new(
                    "GITHUB_EFFECT_OPERATION_MISMATCH",
                    "the retained Operation desired label state is invalid",
                )
            })?;
            if saved_present.is_some_and(|saved| saved != saved_value) {
                return Err(Error::new(
                    "GITHUB_EFFECT_OPERATION_MISMATCH",
                    "the retained Operation contains conflicting desired label states",
                ));
            }
            saved_present = Some(saved_value);
        }
    }
    let saved_revision = value["expected_task_revision"]
        .as_i64()
        .or_else(|| value["task_revision"].as_i64());
    if value["operation_id"] != operation_id
        || value["source_id"] != request.source_id
        || value["task_id"] != request.task_id
        || value["label"] != request.label
        || saved_revision != Some(request.expected_task_revision)
        || saved_present != Some(request.present)
        || value["issue_id"].as_i64().is_none_or(|value| value <= 0)
        || value["issue_number"]
            .as_i64()
            .is_none_or(|value| value <= 0)
    {
        return Err(Error::new(
            "GITHUB_EFFECT_OPERATION_MISMATCH",
            "the retained Operation result does not match its original managed-label request",
        ));
    }
    Ok(())
}

async fn load_target(
    store: &Store,
    principal: &Principal,
    request: &ManagedLabelRequest,
    operation_id: &str,
    require_selected_current_revision: bool,
) -> Result<EffectTarget> {
    let principal = principal.clone();
    let request = request.clone();
    let operation_id = operation_id.to_owned();
    store
        .run(move |db| {
            let current = current_principal(db, principal)?;
            let operation = operations::get_operation(db, &operation_id)?;
            verify_operation(&operation, &current, &request)?;
            let target = resolve_target(db, &current, &request, require_selected_current_revision)?;
            let slot_operation: Option<String> = db
                .query_row(
                    "SELECT operation_id FROM github_label_effect_slots WHERE source_id=?1 AND issue_id=?2 AND label=?3",
                    params![request.source_id, target.issue_id, request.label],
                    |row| row.get(0),
                )
                .optional()?;
            if slot_operation.as_deref() != Some(operation_id.as_str()) {
                return Err(Error::new(
                    "GITHUB_EFFECT_SLOT_MISMATCH",
                    "the exact Issue/label slot no longer points to this Operation",
                ));
            }
            Ok(target)
        })
        .await
}

fn resolve_target(
    db: &Connection,
    principal: &Principal,
    request: &ManagedLabelRequest,
    require_selected_current_revision: bool,
) -> Result<EffectTarget> {
    let target = resolve_target_identity(db, request, require_selected_current_revision)?;
    authorize_task_scope(db, principal, &target.task_id, &target.project_id)?;
    Ok(target)
}

fn resolve_target_identity(
    db: &Connection,
    request: &ManagedLabelRequest,
    require_selected_current_revision: bool,
) -> Result<EffectTarget> {
    let row: Option<EffectTargetRow> = db
        .query_row(
            "SELECT s.project_id,s.host,s.owner,s.repository_name,s.repository_id,i.issue_id,i.issue_number,i.mapping_status,t.revision,t.project_id,m.selected FROM github_sources s JOIN github_issue_items i ON i.source_id=s.source_id JOIN tasks t ON t.task_id=i.task_id LEFT JOIN github_work_pool_members m ON m.source_id=i.source_id AND m.issue_id=i.issue_id AND m.task_id=t.task_id WHERE s.source_id=?1 AND i.task_id=?2",
            params![request.source_id, request.task_id],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?,row.get(9)?,row.get(10)?)),
        )
        .optional()?;
    let Some((
        project_id,
        host,
        owner,
        repo,
        repository_id,
        issue_id,
        issue_number,
        mapping,
        revision,
        task_project,
        selected,
    )) = row
    else {
        return Err(Error::new(
            "GITHUB_EFFECT_TARGET_NOT_FOUND",
            "the Task is not mapped to an Issue in this registered source work pool",
        ));
    };
    if mapping != "mapped" || task_project != project_id || issue_id <= 0 || issue_number <= 0 {
        return Err(Error::new(
            "GITHUB_EFFECT_TARGET_INVALID",
            "the source Issue/Task mapping is unresolved or inconsistent",
        ));
    }
    if require_selected_current_revision
        && (selected != Some(true) || revision != request.expected_task_revision)
    {
        return Err(Error::new(
            "GITHUB_EFFECT_STALE_TASK",
            "the Task must remain selected at the exact requested revision before dispatch",
        ));
    }
    Ok(EffectTarget {
        source_id: request.source_id.clone(),
        project_id,
        host,
        owner,
        repo,
        repository_id,
        issue_id,
        issue_number,
        task_id: request.task_id.clone(),
        task_revision: revision,
    })
}

fn authorize_task_scope(
    db: &Connection,
    principal: &Principal,
    task_id: &str,
    project_id: &str,
) -> Result<()> {
    match &principal.role {
        Role::Operator => Ok(()),
        Role::Manager
            if crate::automation::authorization::current_manager_has_task_scope(
                db, principal, task_id, project_id,
            )? =>
        {
            Ok(())
        }
        _ => Err(Error::new(
            "FORBIDDEN",
            "the caller lacks current authority for this source-mapped Task",
        )),
    }
}

fn verify_operation(
    operation: &Value,
    principal: &Principal,
    request: &ManagedLabelRequest,
) -> Result<()> {
    if operation["method"] != METHOD
        || operation["caller_id"] != principal.client_id
        || operation["task_id"] != request.task_id
    {
        return Err(Error::new(
            "GITHUB_EFFECT_OPERATION_MISMATCH",
            "the retained Operation does not identify this exact caller and Task",
        ));
    }
    Ok(())
}

async fn read_remote<A: GitHubLabelApi + ?Sized>(
    api: &A,
    repository: &RepositoryRef,
    target: &EffectTarget,
) -> Result<IssueLabelSnapshot> {
    let repo = api.repository(repository).await?;
    if repo.id != target.repository_id {
        return Err(Error::new(
            "GITHUB_REPOSITORY_IDENTITY_CHANGED",
            "the configured path no longer identifies the registered repository",
        ));
    }
    let issue = api.issue_labels(repository, target.issue_number).await?;
    if issue.id != target.issue_id || issue.number != target.issue_number {
        return Err(Error::new(
            "GITHUB_ISSUE_IDENTITY_CHANGED",
            "the selected Issue number no longer resolves to the registered immutable Issue ID",
        ));
    }
    if issue.labels.len() > 256
        || issue
            .labels
            .iter()
            .any(|name| name.is_empty() || name.len() > 100 || name.chars().any(char::is_control))
    {
        return Err(Error::new(
            "GITHUB_RESPONSE_INVALID",
            "Issue label readback exceeded the bounded public projection",
        ));
    }
    Ok(issue)
}

async fn begin_write(
    store: &Store,
    principal: &Principal,
    operation_id: &str,
    request: &ManagedLabelRequest,
) -> Result<()> {
    let principal = principal.clone();
    let operation_id = operation_id.to_owned();
    let request = request.clone();
    store
        .run(move |db| {
            let now = model::now_ms()?;
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let current = current_principal(&tx, principal)?;
            let operation = operations::get_operation(&tx, &operation_id)?;
            verify_operation(&operation, &current, &request)?;
            resolve_target(&tx, &current, &request, true)?;
            let slot: Option<String> = tx
                .query_row(
                    "SELECT operation_id FROM github_label_effect_slots WHERE source_id=?1 AND issue_id=(SELECT issue_id FROM github_work_pool_members WHERE source_id=?1 AND task_id=?2) AND label=?3",
                    params![request.source_id, request.task_id, request.label],
                    |row| row.get(0),
                )
                .optional()?;
            if slot.as_deref() != Some(operation_id.as_str()) {
                return Err(Error::conflict("the managed-label semantic slot changed before write"));
            }
            if operation["state"] != "queued" {
                return Err(Error::conflict("the managed-label Operation is no longer queued"));
            }
            let changed = tx.execute(
                "UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1 AND state='queued'",
                params![operation_id, now],
            )?;
            if changed != 1 {
                return Err(Error::conflict("the managed-label Operation changed before write"));
            }
            capacity::sync_operation(&tx, &operation_id, now)?;
            tx.commit()?;
            Ok(())
        })
        .await
}

async fn finish_before_write(
    store: &Store,
    principal: &Principal,
    operation_id: &str,
    target: &EffectTarget,
    request: &ManagedLabelRequest,
    error: &Error,
) -> Result<()> {
    let result = effect_result(
        operation_id,
        target,
        request,
        "rejected_before_write",
        "not_confirmed",
        false,
        None,
        Some(error),
    );
    persist(store, principal, operation_id, result, "rejected").await
}

async fn reject_before_target(
    store: &Store,
    principal: &Principal,
    operation_id: &str,
    request: &ManagedLabelRequest,
    error: &Error,
) -> Result<()> {
    let result = json!({
        "operation_id":operation_id,
        "source_id":request.source_id,
        "task_id":request.task_id,
        "expected_task_revision":request.expected_task_revision,
        "label":request.label,
        "desired_present":request.present,
        "outcome":"rejected_before_write",
        "readback":"not_started",
        "write_attempted":false,
        "error":{"code":error.code,"message":error.message},
        "current_state_read_method":"operation.get"
    });
    persist(store, principal, operation_id, result, "rejected").await
}

#[allow(clippy::too_many_arguments)]
async fn settle(
    store: &Store,
    principal: &Principal,
    operation_id: &str,
    target: &EffectTarget,
    request: &ManagedLabelRequest,
    outcome: &str,
    write_attempted: bool,
    readback: &IssueLabelSnapshot,
) -> Result<Value> {
    let observed_present = readback.labels.iter().any(|label| label == &request.label);
    let result = effect_result(
        operation_id,
        target,
        request,
        outcome,
        "confirmed",
        write_attempted,
        Some(observed_present),
        None,
    );
    match persist_fenced_settlement(
        store,
        principal,
        operation_id,
        target,
        request,
        result,
        write_attempted,
        observed_present,
    )
    .await?
    {
        FencedSettlement::Settled
        | FencedSettlement::Existing
        | FencedSettlement::PreservedUnknown => {}
        FencedSettlement::Rejected(error) => return Err(error),
    }
    operation_result(store, operation_id).await
}

#[allow(clippy::too_many_arguments)]
async fn persist_fenced_settlement(
    store: &Store,
    principal: &Principal,
    operation_id: &str,
    target: &EffectTarget,
    request: &ManagedLabelRequest,
    settled_result: Value,
    write_attempted: bool,
    observed_present: bool,
) -> Result<FencedSettlement> {
    let principal = principal.clone();
    let operation_id = operation_id.to_owned();
    let target = target.clone();
    let request = request.clone();
    store
        .run(move |db| {
            let now = model::now_ms()?;
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let operation = operations::get_operation(&tx, &operation_id)?;
            if operation["method"] != METHOD
                || operation["caller_id"] != principal.client_id
                || operation["task_id"] != request.task_id
            {
                return Err(Error::new(
                    "GITHUB_EFFECT_OPERATION_MISMATCH",
                    "the retained Operation no longer identifies this caller and Task",
                ));
            }
            let current_state = operation["state"].as_str().unwrap_or_default().to_owned();
            if current_state == "settled" {
                tx.commit()?;
                return Ok(FencedSettlement::Existing);
            }
            if !matches!(current_state.as_str(), "queued" | "sending" | "outcome_unknown") {
                return Err(Error::conflict(
                    "the managed-label Operation changed before its settlement fence",
                ));
            }

            let fence = validate_settlement_target(
                &tx,
                &principal,
                &operation,
                &request,
                &target,
                &operation_id,
            );
            let (state, result, outcome) = match fence {
                Ok(()) => ("settled", settled_result, FencedSettlement::Settled),
                Err(error) if is_settlement_fence_error(&error) => {
                    if current_state == "queued" && !write_attempted {
                        let result = effect_result(
                            &operation_id,
                            &target,
                            &request,
                            "settlement_rejected_before_write",
                            "confirmed_but_not_settled",
                            false,
                            Some(observed_present),
                            Some(&error),
                        );
                        ("rejected", result, FencedSettlement::Rejected(error))
                    } else {
                        let result = effect_result(
                            &operation_id,
                            &target,
                            &request,
                            "settlement_blocked_by_caller_or_target_fence",
                            "observed_but_unsettled",
                            true,
                            Some(observed_present),
                            Some(&error),
                        );
                        (
                            "outcome_unknown",
                            result,
                            FencedSettlement::PreservedUnknown,
                        )
                    }
                }
                Err(error) => return Err(error),
            };

            let allowed_from = match state {
                "settled" => "'queued','sending','outcome_unknown'",
                "rejected" => "'queued'",
                "outcome_unknown" => "'sending','outcome_unknown'",
                _ => return Err(Error::new("INTERNAL", "invalid fenced effect state")),
            };
            let sql = format!(
                "UPDATE operations SET state=?2,result_json=?3,settled_at_ms=?4,updated_at_ms=?5 WHERE operation_id=?1 AND state IN ({allowed_from})"
            );
            let changed = tx.execute(
                &sql,
                params![
                    operation_id,
                    state,
                    model::canonical(&result)?,
                    if matches!(state, "settled" | "rejected") {
                        Some(now)
                    } else {
                        None
                    },
                    now
                ],
            )?;
            if changed != 1 {
                let latest_state = operations::get_operation(&tx, &operation_id)?["state"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                if latest_state != state {
                    return Err(Error::conflict(
                        "the managed-label Operation changed before its fenced result was recorded",
                    ));
                }
            }
            capacity::sync_operation(&tx, &operation_id, now)?;
            if state != "outcome_unknown" {
                let event_key = if state == "settled" {
                    format!("finished:{operation_id}")
                } else {
                    format!("rejected:{operation_id}")
                };
                tx.execute(
                    "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('github:managed-label',?1,?2,'github.effect.managed_label',?3,?4)",
                    params![event_key, operation_id, model::canonical(&result)?, now],
                )?;
            }
            tx.commit()?;
            Ok(outcome)
        })
        .await
}

fn validate_settlement_target(
    tx: &Transaction<'_>,
    principal: &Principal,
    operation: &Value,
    request: &ManagedLabelRequest,
    expected_target: &EffectTarget,
    operation_id: &str,
) -> Result<()> {
    let current = current_principal(tx, principal.clone())?;
    verify_operation(operation, &current, request)?;
    // The action was authorized before its external write began. Do not
    // discard a trusted readback merely because the GM designation changed;
    // fence only the retained source/Issue target and semantic slot here.
    let target = resolve_target_identity(tx, request, false)?;
    if !same_effect_target(expected_target, &target) {
        return Err(Error::new(
            "GITHUB_EFFECT_TARGET_CHANGED",
            "the registered source target changed before the managed-label result was settled",
        ));
    }
    let slot: Option<(i64, String)> = tx
        .query_row(
            "SELECT desired_present,operation_id FROM github_label_effect_slots WHERE source_id=?1 AND issue_id=?2 AND label=?3",
            params![request.source_id, target.issue_id, request.label],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if slot
        .as_ref()
        .is_none_or(|(desired_present, slot_operation_id)| {
            *desired_present != i64::from(request.present) || slot_operation_id != operation_id
        })
    {
        return Err(Error::new(
            "GITHUB_EFFECT_SLOT_MISMATCH",
            "the exact Issue/label slot changed before the managed-label result was settled",
        ));
    }
    Ok(())
}

fn is_settlement_fence_error(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "UNAUTHORIZED"
            | "LOCAL_OPERATOR_MISMATCH"
            | "FORBIDDEN"
            | "GITHUB_EFFECT_OPERATION_MISMATCH"
            | "GITHUB_EFFECT_TARGET_NOT_FOUND"
            | "GITHUB_EFFECT_TARGET_INVALID"
            | "GITHUB_EFFECT_TARGET_CHANGED"
            | "GITHUB_EFFECT_SLOT_MISMATCH"
    )
}

#[allow(clippy::too_many_arguments)]
async fn record_unknown(
    store: &Store,
    principal: &Principal,
    operation_id: &str,
    target: &EffectTarget,
    request: &ManagedLabelRequest,
    reason: &str,
    observed_present: Option<bool>,
    error: Option<&Error>,
) -> Result<Value> {
    let result = effect_result(
        operation_id,
        target,
        request,
        reason,
        "unknown",
        true,
        observed_present,
        error,
    );
    persist(store, principal, operation_id, result, "outcome_unknown").await?;
    operation_result(store, operation_id).await
}

#[allow(clippy::too_many_arguments)]
fn effect_result(
    operation_id: &str,
    target: &EffectTarget,
    request: &ManagedLabelRequest,
    outcome: &str,
    readback: &str,
    write_attempted: bool,
    observed_present: Option<bool>,
    error: Option<&Error>,
) -> Value {
    json!({
        "operation_id":operation_id,
        "source_id":target.source_id,
        "project_id":target.project_id,
        "task_id":target.task_id,
        "task_revision_at_observation":target.task_revision,
        "expected_task_revision":request.expected_task_revision,
        "repository_id":target.repository_id,
        "issue_id":target.issue_id,
        "issue_number":target.issue_number,
        "label":request.label,
        "desired_present":request.present,
        "observed_present":observed_present,
        "write_attempted":write_attempted,
        "readback":readback,
        "outcome":outcome,
        "error":error.map(|error| json!({"code":error.code,"message":error.message})),
        "current_state_read_method":"operation.get"
    })
}

async fn persist(
    store: &Store,
    principal: &Principal,
    operation_id: &str,
    result: Value,
    state: &'static str,
) -> Result<()> {
    let principal = principal.clone();
    let operation_id = operation_id.to_owned();
    store
        .run(move |db| {
            let now = model::now_ms()?;
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let current = current_principal(&tx, principal)?;
            let operation = operations::get_operation(&tx, &operation_id)?;
            if operation["method"] != METHOD || operation["caller_id"] != current.client_id {
                return Err(Error::new(
                    "GITHUB_EFFECT_OPERATION_MISMATCH",
                    "the retained Operation does not belong to the current caller",
                ));
            }
            let allowed_from = match state {
                "settled" => "'queued','sending','outcome_unknown'",
                "rejected" => "'queued'",
                "outcome_unknown" => "'sending','outcome_unknown'",
                _ => return Err(Error::new("INTERNAL", "invalid GitHub effect state")),
            };
            let sql = format!(
                "UPDATE operations SET state=?2,result_json=?3,settled_at_ms=?4,updated_at_ms=?5 WHERE operation_id=?1 AND state IN ({allowed_from})"
            );
            let changed = tx.execute(
                &sql,
                params![
                    operation_id,
                    state,
                    model::canonical(&result)?,
                    if matches!(state, "settled" | "rejected") { Some(now) } else { None },
                    now
                ],
            )?;
            if changed != 1 {
                let current_state = operations::get_operation(&tx, &operation_id)?["state"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                if current_state != state {
                    return Err(Error::conflict(
                        "the managed-label Operation changed before its outcome was recorded",
                    ));
                }
            }
            capacity::sync_operation(&tx, &operation_id, now)?;
            if state != "outcome_unknown" {
                let event_key = if state == "settled" {
                    format!("finished:{operation_id}")
                } else {
                    format!("rejected:{operation_id}")
                };
                tx.execute(
                    "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('github:managed-label',?1,?2,'github.effect.managed_label',?3,?4)",
                    params![event_key, operation_id, model::canonical(&result)?, now],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
}

async fn operation_result(store: &Store, operation_id: &str) -> Result<Value> {
    let operation_id = operation_id.to_owned();
    store
        .run(move |db| {
            let operation = operations::get_operation(db, &operation_id)?;
            let mut result = operation["result"].clone();
            result["operation_state"] = operation["state"].clone();
            Ok(result)
        })
        .await
}

#[allow(clippy::too_many_arguments)]
async fn finish_reconcile_readback(
    store: &Store,
    principal: &Principal,
    reconciliation_id: &str,
    request: &ManagedLabelReconcileRequest,
    retained: Option<&RetainedEffect>,
    observed: Option<(IssueLabelSnapshot, bool)>,
    outcome: &str,
    error: Option<&Error>,
    settle_original: bool,
) -> Result<Value> {
    let principal = principal.clone();
    let reconciliation_id_for_store = reconciliation_id.to_owned();
    let request = request.clone();
    let retained = retained.cloned();
    let observed = observed.clone();
    let outcome = outcome.to_owned();
    let error = error.cloned();
    store
        .run(move |db| {
            let reconciliation_id = reconciliation_id_for_store;
            let now = model::now_ms()?;
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let (method, caller_id, state): (String, String, String) = tx.query_row(
                "SELECT method,caller_id,state FROM operations WHERE operation_id=?1",
                [&reconciliation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            if method != RECONCILE_METHOD
                || caller_id != principal.client_id
                || state != "queued"
            {
                return Err(Error::conflict(
                    "the managed-label readback Operation changed before its result was recorded",
                ));
            }

            let mut settled_original = false;
            let mut final_outcome = outcome;
            let mut final_error = error;
            let mut final_retained = retained.clone();
            if settle_original
                && let (Some(initial), Some((snapshot, observed_present))) =
                    (retained.as_ref(), observed.as_ref())
                && *observed_present == initial.request.present
            {
                match load_retained_effect_for_settlement(&tx, &request.operation_id) {
                        Ok(current_effect)
                            if same_effect_target(&initial.target, &current_effect.target)
                                && same_managed_label_request(
                                    &current_effect.request,
                                    &initial.request,
                                ) =>
                        {
                            let mut result = current_effect.result.clone();
                            let previous_unknown_result = result.clone();
                            result["outcome"] = json!("reconciled_from_readback");
                            result["readback"] = json!("confirmed");
                            result["observed_present"] = json!(*observed_present);
                            result["error"] = Value::Null;
                            result["reconciliation"] = json!({
                                "operation_id":reconciliation_id,
                                "reconciler_client_id":principal.client_id,
                                "original_caller_id":current_effect.caller_id,
                                "prior_unknown_result":previous_unknown_result,
                                "observed_at_ms":now,
                                "repository_id":current_effect.target.repository_id,
                                "issue_id":snapshot.id,
                                "issue_number":snapshot.number,
                                "label":current_effect.request.label,
                                "desired_present":current_effect.request.present,
                                "observed_present":observed_present,
                                "readback":"confirmed"
                            });
                            let changed = tx.execute(
                                "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND method=?4 AND state='outcome_unknown'",
                                params![
                                    request.operation_id,
                                    model::canonical(&result)?,
                                    now,
                                    METHOD
                                ],
                            )?;
                            if changed == 1 {
                                capacity::sync_operation(&tx, &request.operation_id, now)?;
                                tx.execute(
                                    "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('github:managed-label',?1,?2,'github.effect.managed_label',?3,?4)",
                                    params![
                                        format!("finished:{}", request.operation_id),
                                        request.operation_id,
                                        model::canonical(&result)?,
                                        now
                                    ],
                                )?;
                                settled_original = true;
                                final_retained = Some(current_effect);
                            } else {
                                final_outcome = "original_operation_no_longer_unknown".to_owned();
                            }
                        }
                        Ok(_) => {
                            final_outcome = "retained_target_changed_before_settlement".to_owned();
                            final_error = Some(Error::new(
                                "GITHUB_EFFECT_TARGET_CHANGED",
                                "the retained Task, repository, Issue, label slot, or desired state changed after readback",
                            ));
                        }
                        Err(error) => {
                            final_outcome = "retained_operation_changed_before_settlement".to_owned();
                            final_error = Some(error);
                        }
                }
            }

            let result = reconcile_result(
                &reconciliation_id,
                &request,
                final_retained.as_ref(),
                observed.as_ref(),
                &final_outcome,
                if settled_original {
                    "settled"
                } else {
                    "outcome_unknown"
                },
                final_error.as_ref(),
            );
            let changed = tx.execute(
                "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND method=?4 AND caller_id=?5 AND state='queued'",
                params![
                    reconciliation_id,
                    model::canonical(&result)?,
                    now,
                    RECONCILE_METHOD,
                    principal.client_id
                ],
            )?;
            if changed != 1 {
                return Err(Error::conflict(
                    "the managed-label readback Operation changed before settlement",
                ));
            }
            capacity::sync_operation(&tx, &reconciliation_id, now)?;
            tx.execute(
                "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('github:managed-label',?1,?2,?3,?4,?5)",
                params![
                    format!("reconcile:{reconciliation_id}"),
                    reconciliation_id,
                    RECONCILE_METHOD,
                    model::canonical(&result)?,
                    now
                ],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await?;
    operation_result(store, reconciliation_id).await
}

fn reconcile_result(
    reconciliation_id: &str,
    request: &ManagedLabelReconcileRequest,
    retained: Option<&RetainedEffect>,
    observed: Option<&(IssueLabelSnapshot, bool)>,
    outcome: &str,
    original_state: &str,
    error: Option<&Error>,
) -> Value {
    let mut result = json!({
        "operation_id":reconciliation_id,
        "original_operation_id":request.operation_id,
        "outcome":outcome,
        "original_operation_state":original_state,
        "write_attempted":false,
        "readback":if observed.is_some() { "repository_and_issue_confirmed" } else if error.is_some() { "unavailable_or_not_started" } else { "not_started" },
        "current_state_read_method":"operation.get",
        "error":error.map(|error| json!({"code":error.code,"message":error.message}))
    });
    if let Some(retained) = retained {
        result["source_id"] = json!(retained.request.source_id);
        result["project_id"] = json!(retained.target.project_id);
        result["task_id"] = json!(retained.target.task_id);
        result["repository_id"] = json!(retained.target.repository_id);
        result["issue_id"] = json!(retained.target.issue_id);
        result["issue_number"] = json!(retained.target.issue_number);
        result["label"] = json!(retained.request.label);
        result["desired_present"] = json!(retained.request.present);
    }
    if let Some((snapshot, observed_present)) = observed {
        result["observed_issue_id"] = json!(snapshot.id);
        result["observed_issue_number"] = json!(snapshot.number);
        result["observed_labels"] = json!(snapshot.labels);
        result["observed_present"] = json!(observed_present);
    }
    result
}

fn same_effect_target(left: &EffectTarget, right: &EffectTarget) -> bool {
    left.source_id == right.source_id
        && left.project_id == right.project_id
        && left.host == right.host
        && left.owner == right.owner
        && left.repo == right.repo
        && left.repository_id == right.repository_id
        && left.issue_id == right.issue_id
        && left.issue_number == right.issue_number
        && left.task_id == right.task_id
}

fn same_managed_label_request(left: &ManagedLabelRequest, right: &ManagedLabelRequest) -> bool {
    left.client_request_id == right.client_request_id
        && left.source_id == right.source_id
        && left.task_id == right.task_id
        && left.expected_task_revision == right.expected_task_revision
        && left.label == right.label
        && left.present == right.present
}

fn operation_error(result: &Value) -> Error {
    let error = result.get("error").unwrap_or(result);
    Error::new(
        error["code"].as_str().unwrap_or("GITHUB_EFFECT_FAILED"),
        error["message"]
            .as_str()
            .unwrap_or("the retained managed-label Operation was rejected before write"),
    )
}
