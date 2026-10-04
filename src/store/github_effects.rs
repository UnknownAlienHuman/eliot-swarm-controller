//! One manual, durable desired-state GitHub effect: a managed Issue label.
//!
//! The Issue and Task identities come only from the registered source map and
//! selected work pool. Writes use the existing `gh` account route. Once an
//! Operation enters `sending`, every recovery path is readback-only.

use super::{Store, capacity, current_principal, mutate, operations};
use crate::{
    error::{Error, Result},
    github::{
        client::{GhCli, GitHubLabelApi, IssueLabelSnapshot, RepositoryRef},
        protocol::{self, ManagedLabelRequest},
    },
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};

const METHOD: &str = "github.effect.managed_label";

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

pub(super) async fn call(store: &Store, principal: Principal, value: Value) -> Result<Value> {
    call_with_api(store, principal, value, &GhCli).await
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
    authorize_task_scope(db, principal, &request.task_id, &project_id)?;
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
    let result = effect_result(
        operation_id,
        target,
        request,
        outcome,
        "confirmed",
        write_attempted,
        Some(readback.labels.iter().any(|label| label == &request.label)),
        None,
    );
    persist(store, principal, operation_id, result, "settled").await?;
    operation_result(store, operation_id).await
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

fn operation_error(result: &Value) -> Error {
    let error = result.get("error").unwrap_or(result);
    Error::new(
        error["code"].as_str().unwrap_or("GITHUB_EFFECT_FAILED"),
        error["message"]
            .as_str()
            .unwrap_or("the retained managed-label Operation was rejected before write"),
    )
}
