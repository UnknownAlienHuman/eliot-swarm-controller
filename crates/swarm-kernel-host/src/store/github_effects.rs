//! One manual, durable desired-state GitHub effect: a managed Issue label.
//!
//! The Issue and Task identities come only from the registered source map and
//! selected work pool. Writes use the existing `gh` account route. Once an
//! Operation enters `sending`, every recovery path is readback-only.

use super::{Store, capacity, current_principal, gm, mutate, operations};
use crate::{
    automation::{authorization::GithubProjectionContext, config::AutomationEntry},
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
    source_revision: String,
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

/// Dispatch one retained automated projection without manufacturing a
/// Principal for its technical caller. The typed on-behalf context is
/// revalidated at load and immediately before the single remote write.
pub(super) async fn call_automatic(store: &Store, operation_id: &str) -> Result<Value> {
    call_automatic_with_api(store, operation_id, &GhCli).await
}

async fn call_automatic_with_api<A: GitHubLabelApi + ?Sized>(
    store: &Store,
    operation_id: &str,
    api: &A,
) -> Result<Value> {
    let operation_id = operation_id.to_owned();
    let (operation, request) = store
        .run(move |db| {
            let operation = operations::get_operation(db, &operation_id)?;
            let context = GithubProjectionContext::from_committed_operation(db, &operation_id)?;
            let original_json: String = db.query_row(
                "SELECT original_request_json FROM operations WHERE operation_id=?1",
                [&operation_id],
                |row| row.get(0),
            )?;
            let original: Value = serde_json::from_str(&original_json)?;
            let request = ManagedLabelRequest::parse(&original)?;
            context.require_request_matches(&request)?;
            if operation["caller_id"].as_str() != Some(context.technical_requester_id())
                || operation["method"].as_str() != Some(METHOD)
                || operation["task_id"].as_str() != Some(context.task_id())
                || operation["attempt_id"].as_str() != Some(context.attempt_id())
            {
                return Err(Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "managed-label Operation identity differs from its retained on-behalf cause",
                ));
            }
            Ok((operation, request))
        })
        .await?;
    let state = operation["state"].as_str().unwrap_or_default();
    match state {
        "settled" => {
            let id = operation["operation_id"].as_str().ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "managed-label Operation has no ID",
                )
            })?;
            return operation_result(store, id).await;
        }
        "rejected" => return Err(operation_error(&operation["result"])),
        "cancelled" => {
            return Err(Error::new(
                "GITHUB_EFFECT_CANCELLED",
                "the retained automated managed-label Operation was cancelled before dispatch",
            ));
        }
        "queued" => {}
        "sending" => {
            return Err(Error::new(
                "GITHUB_EFFECT_IN_PROGRESS",
                "the automated managed-label effect may be in flight; it cannot be resent",
            ));
        }
        "outcome_unknown" => {
            return Err(Error::new(
                "GITHUB_EFFECT_READBACK_REQUIRED",
                "the automated managed-label effect is uncertain and requires the retained Manager readback path",
            ));
        }
        _ => {
            return Err(Error::new(
                "GITHUB_EFFECT_NOT_RESUMABLE",
                "the retained automated managed-label Operation is not resumable",
            ));
        }
    }

    let operation_id = operation["operation_id"]
        .as_str()
        .ok_or_else(|| {
            Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "managed-label Operation has no ID",
            )
        })?
        .to_owned();
    let (context, target) = match load_automatic_target(store, &operation_id, &request).await {
        Ok(loaded) => loaded,
        Err(error) if is_automatic_prewrite_stale_error(&error) => {
            if reject_stale_queued_automatic(store, &operation_id, &request).await? {
                return operation_result(store, &operation_id).await;
            }
            return Err(error);
        }
        Err(error) => return Err(error),
    };
    let repository = RepositoryRef::new(&target.host, &target.owner, &target.repo)?;
    let initial = match read_remote(api, &repository, &target).await {
        Ok(initial) => initial,
        Err(error) => {
            return finish_automatic_effect(
                store,
                &operation_id,
                &target,
                &request,
                "rejected",
                "rejected_before_write",
                "not_confirmed",
                false,
                None,
                Some(&error),
            )
            .await;
        }
    };
    let currently_present = initial.labels.iter().any(|label| label == &request.label);
    if currently_present == request.present {
        return finish_automatic_effect(
            store,
            &operation_id,
            &target,
            &request,
            "settled",
            "already_in_desired_state",
            "confirmed",
            false,
            Some(&initial),
            None,
        )
        .await;
    }

    if let Err(error) =
        begin_automatic_write(store, &operation_id, &context, &target, &request).await
    {
        if is_automatic_prewrite_stale_error(&error)
            && reject_stale_queued_automatic(store, &operation_id, &request).await?
        {
            return operation_result(store, &operation_id).await;
        }
        return Err(error);
    }
    // The write call is issued once. From the durable `sending` transition
    // onward, all recovery is readback-only.
    let write_error = api
        .set_label(
            &repository,
            target.issue_number,
            &request.label,
            request.present,
        )
        .await
        .err();
    if let Err(error) = validate_automatic_readback(store, &operation_id, &target, &request).await {
        return finish_automatic_effect(
            store,
            &operation_id,
            &target,
            &request,
            "outcome_unknown",
            "readback_blocked_by_current_authority_or_target",
            "unknown",
            true,
            None,
            Some(&error),
        )
        .await;
    }
    match read_remote(api, &repository, &target).await {
        Ok(readback)
            if readback.labels.iter().any(|label| label == &request.label) == request.present =>
        {
            finish_automatic_effect(
                store,
                &operation_id,
                &target,
                &request,
                "settled",
                if write_error.is_some() {
                    "confirmed_after_transport_error"
                } else {
                    "applied"
                },
                "confirmed",
                true,
                Some(&readback),
                None,
            )
            .await
        }
        Ok(readback) => {
            finish_automatic_effect(
                store,
                &operation_id,
                &target,
                &request,
                "outcome_unknown",
                "desired_state_not_observed_after_write",
                "unknown",
                true,
                Some(&readback),
                write_error.as_ref(),
            )
            .await
        }
        Err(error) => {
            finish_automatic_effect(
                store,
                &operation_id,
                &target,
                &request,
                "outcome_unknown",
                "readback_unavailable_after_write",
                "unknown",
                true,
                None,
                Some(write_error.as_ref().unwrap_or(&error)),
            )
            .await
        }
    }
}

async fn load_automatic_target(
    store: &Store,
    operation_id: &str,
    request: &ManagedLabelRequest,
) -> Result<(GithubProjectionContext, EffectTarget)> {
    let operation_id = operation_id.to_owned();
    let request = request.clone();
    store
        .run(move |db| {
            let operation = operations::get_operation(db, &operation_id)?;
            if operation["state"] != "queued" {
                return Err(Error::conflict(
                    "the automated managed-label Operation changed before dispatch",
                ));
            }
            let context = GithubProjectionContext::from_committed_operation(db, &operation_id)?;
            context.require_current(db)?;
            context.require_request_matches(&request)?;
            let target = resolve_target_identity(db, &request, true)?;
            validate_automatic_target(&context, &request, &target)?;
            RepositoryRef::new(&target.host, &target.owner, &target.repo)?;
            require_automatic_slot(db, &request, &target, &operation_id)?;
            Ok((context, target))
        })
        .await
}

/// Terminalize only an exact internal Operation that is still queued, has no
/// sent timestamp, retains its typed cause/admission receipt, and still owns
/// the exact durable label slot. This releases a provably unstarted stale
/// effect without creating a Principal or touching `sending`/unknown writes.
async fn reject_stale_queued_automatic(
    store: &Store,
    operation_id: &str,
    request: &ManagedLabelRequest,
) -> Result<bool> {
    let operation_id = operation_id.to_owned();
    let request = request.clone();
    store
        .run(move |db| {
            let now = model::now_ms()?;
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let operation = operations::get_operation(&tx, &operation_id)?;
            if operation["state"].as_str() != Some("queued")
                || operation["caller_id"].as_str()
                    != Some(authorization::AUTOMATION_TECHNICAL_REQUESTER_ID)
                || operation["method"].as_str() != Some(METHOD)
            {
                tx.commit()?;
                return Ok(false);
            }
            let context = GithubProjectionContext::from_committed_operation(&tx, &operation_id)?;
            context.require_request_matches(&request)?;
            if operation["task_id"].as_str() != Some(context.task_id())
                || operation["attempt_id"].as_str() != Some(context.attempt_id())
            {
                tx.commit()?;
                return Ok(false);
            }

            let sent_at_ms: Option<i64> = tx.query_row(
                "SELECT sent_at_ms FROM operations WHERE operation_id=?1 AND caller_id=?2 AND method=?3",
                params![operation_id, authorization::AUTOMATION_TECHNICAL_REQUESTER_ID, METHOD],
                |row| row.get(0),
            )?;
            if sent_at_ms.is_some() {
                tx.commit()?;
                return Ok(false);
            }

            let stale = (|| {
                context.require_current(&tx)?;
                let target = resolve_target_identity(&tx, &request, true)?;
                validate_automatic_target(&context, &request, &target)?;
                RepositoryRef::new(&target.host, &target.owner, &target.repo)?;
                require_automatic_slot(&tx, &request, &target, &operation_id)
            })();
            let Err(stale_error) = stale else {
                tx.commit()?;
                return Ok(false);
            };
            if !is_automatic_prewrite_stale_error(&stale_error) {
                tx.commit()?;
                return Ok(false);
            }

            let effective_json: String = tx.query_row(
                "SELECT effective_request_json FROM operations WHERE operation_id=?1",
                [&operation_id],
                |row| row.get(0),
            )?;
            let effective: Value = serde_json::from_str(&effective_json).map_err(|_| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "managed-label effective request is invalid",
                )
            })?;
            let receipt = effective.get("receipt").ok_or_else(|| {
                Error::new(
                    "GITHUB_EFFECT_OPERATION_MISMATCH",
                    "the retained automated Operation has no admission receipt",
                )
            })?;
            let linkage = context.linkage_value();
            if receipt["ok"] != true
                || receipt["value"]["operation_id"] != operation_id
                || effective.get("automation_on_behalf") != Some(&linkage)
            {
                return Err(Error::new(
                    "GITHUB_EFFECT_OPERATION_MISMATCH",
                    "the retained automated admission receipt does not match its typed cause",
                ));
            }
            let admission = &receipt["value"];
            validate_retained_effect_result(&operation["result"], admission, &operation_id, &request)?;
            if admission["source_id"] != context.source_id()
                || admission["project_id"] != context.project_id()
                || admission["task_id"] != context.task_id()
                || admission["task_revision"] != context.task_revision()
                || admission["source_revision"] != context.source_revision()
                || admission["label"] != context.label()
                || admission["present"] != context.present()
                || admission["desired_present"] != context.present()
                || admission["outcome"] != "managed_label_queued"
            {
                return Err(Error::new(
                    "GITHUB_EFFECT_OPERATION_MISMATCH",
                    "the retained admission receipt differs from its exact projection context",
                ));
            }

            let mut slot_statement = tx.prepare(
                "SELECT source_id,issue_id,label,desired_present FROM github_label_effect_slots WHERE operation_id=?1",
            )?;
            let slots = slot_statement
                .query_map([&operation_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            drop(slot_statement);
            let (slot_source_id, slot_issue_id, slot_label, slot_present) = match slots.as_slice() {
                [slot] => slot,
                _ => {
                    tx.commit()?;
                    return Ok(false);
                }
            };
            if slot_source_id != context.source_id()
                || Some(*slot_issue_id) != admission["issue_id"].as_i64()
                || slot_label != context.label()
                || *slot_present != i64::from(context.present())
            {
                tx.commit()?;
                return Ok(false);
            }

            let result = json!({
                "operation_id":operation_id,
                "source_id":context.source_id(),
                "project_id":context.project_id(),
                "task_id":context.task_id(),
                "task_revision_at_observation":context.task_revision(),
                "expected_task_revision":request.expected_task_revision,
                "issue_id":admission["issue_id"],
                "issue_number":admission["issue_number"],
                "source_revision":context.source_revision(),
                "label":request.label,
                "desired_present":request.present,
                "observed_present":null,
                "write_attempted":false,
                "readback":"not_confirmed",
                "outcome":"rejected_before_write",
                "error":{"code":stale_error.code,"message":stale_error.message},
                "current_state_read_method":"operation.get"
            });
            let changed = tx.execute(
                "UPDATE operations SET state='rejected',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND caller_id=?4 AND method=?5 AND state='queued' AND sent_at_ms IS NULL",
                params![
                    operation_id,
                    model::canonical(&result)?,
                    now,
                    authorization::AUTOMATION_TECHNICAL_REQUESTER_ID,
                    METHOD
                ],
            )?;
            if changed != 1 {
                tx.commit()?;
                return Ok(false);
            }
            capacity::sync_operation(&tx, &operation_id, now)?;
            let event_key = format!("rejected:{operation_id}");
            tx.execute(
                "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('github:managed-label',?1,?2,'github.effect.managed_label',?3,?4)",
                params![event_key, operation_id, model::canonical(&result)?, now],
            )?;
            tx.commit()?;
            Ok(true)
        })
        .await
}

fn is_automatic_prewrite_stale_error(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "AUTOMATION_ACTION_CHANGED"
            | "AUTOMATION_CURRENT_GM_REQUIRED"
            | "AUTOMATION_FACT_CORRUPT"
            | "AUTOMATION_FACT_MISSING"
            | "AUTOMATION_FACT_NOT_APPLIED"
            | "AUTOMATION_GITHUB_PROJECTION_SETTINGS_REQUIRED"
            | "AUTOMATION_GITHUB_PROJECTION_SOURCE_STALE"
            | "AUTOMATION_TRANSFER_SCOPE"
            | "FORBIDDEN"
            | "FORGE_ACCEPTANCE_STALE"
            | "GITHUB_EFFECT_SLOT_MISMATCH"
            | "GITHUB_EFFECT_STALE_TASK"
            | "GITHUB_EFFECT_TARGET_CHANGED"
            | "GITHUB_EFFECT_TARGET_INVALID"
            | "GITHUB_EFFECT_TARGET_NOT_FOUND"
            | "INVALID_PARAMS"
    )
}

async fn validate_automatic_readback(
    store: &Store,
    operation_id: &str,
    expected_target: &EffectTarget,
    request: &ManagedLabelRequest,
) -> Result<()> {
    let operation_id = operation_id.to_owned();
    let expected_target = expected_target.clone();
    let request = request.clone();
    store
        .run(move |db| {
            let operation = operations::get_operation(db, &operation_id)?;
            if operation["state"].as_str() != Some("sending") {
                return Err(Error::conflict(
                    "the automated managed-label Operation changed before readback",
                ));
            }
            let context = GithubProjectionContext::from_committed_operation(db, &operation_id)?;
            context.require_current(db)?;
            context.require_request_matches(&request)?;
            let target = resolve_target_identity(db, &request, true)?;
            validate_automatic_target(&context, &request, &target)?;
            if !same_automatic_effect_target(&target, &expected_target) {
                return Err(Error::new(
                    "GITHUB_EFFECT_TARGET_CHANGED",
                    "registered source target changed before automated readback",
                ));
            }
            require_automatic_slot(db, &request, &target, &operation_id)
        })
        .await
}

async fn begin_automatic_write(
    store: &Store,
    operation_id: &str,
    expected_context: &GithubProjectionContext,
    expected_target: &EffectTarget,
    request: &ManagedLabelRequest,
) -> Result<()> {
    let operation_id = operation_id.to_owned();
    let expected_context = expected_context.clone();
    let expected_target = expected_target.clone();
    let request = request.clone();
    store
        .run(move |db| {
            let now = model::now_ms()?;
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let operation = operations::get_operation(&tx, &operation_id)?;
            if operation["caller_id"].as_str()
                != Some(expected_context.technical_requester_id())
                || operation["method"].as_str() != Some(METHOD)
                || operation["state"].as_str() != Some("queued")
            {
                return Err(Error::conflict(
                    "the automated managed-label Operation is no longer queued",
                ));
            }
            let context = GithubProjectionContext::from_committed_operation(&tx, &operation_id)?;
            context.require_current(&tx)?;
            context.require_request_matches(&request)?;
            let target = resolve_target_identity(&tx, &request, true)?;
            validate_automatic_target(&context, &request, &target)?;
            if context.source_revision() != expected_context.source_revision()
                || !same_automatic_effect_target(&target, &expected_target)
            {
                return Err(Error::new(
                    "GITHUB_EFFECT_TARGET_CHANGED",
                    "registered source target changed before the automated label write",
                ));
            }
            require_automatic_slot(&tx, &request, &target, &operation_id)?;
            let changed = tx.execute(
                "UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1 AND caller_id=?3 AND method=?4 AND state='queued'",
                params![operation_id, now, context.technical_requester_id(), METHOD],
            )?;
            if changed != 1 {
                return Err(Error::conflict(
                    "the automated managed-label Operation changed before write",
                ));
            }
            capacity::sync_operation(&tx, &operation_id, now)?;
            tx.commit()?;
            Ok(())
        })
        .await
}

#[allow(clippy::too_many_arguments)]
async fn finish_automatic_effect(
    store: &Store,
    operation_id: &str,
    target: &EffectTarget,
    request: &ManagedLabelRequest,
    requested_state: &'static str,
    outcome: &str,
    readback: &str,
    write_attempted: bool,
    snapshot: Option<&IssueLabelSnapshot>,
    error: Option<&Error>,
) -> Result<Value> {
    let observed_present =
        snapshot.map(|snapshot| snapshot.labels.iter().any(|label| label == &request.label));
    let result = effect_result(
        operation_id,
        target,
        request,
        outcome,
        readback,
        write_attempted,
        observed_present,
        error,
    );
    persist_automatic_effect(
        store,
        operation_id,
        target,
        request,
        result,
        requested_state,
        write_attempted,
    )
    .await?;
    operation_result(store, operation_id).await
}

async fn persist_automatic_effect(
    store: &Store,
    operation_id: &str,
    expected_target: &EffectTarget,
    request: &ManagedLabelRequest,
    mut result: Value,
    requested_state: &'static str,
    write_attempted: bool,
) -> Result<()> {
    let operation_id = operation_id.to_owned();
    let expected_target = expected_target.clone();
    let request = request.clone();
    store
        .run(move |db| {
            let now = model::now_ms()?;
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let operation = operations::get_operation(&tx, &operation_id)?;
            let context = GithubProjectionContext::from_committed_operation(&tx, &operation_id)?;
            context.require_request_matches(&request)?;
            if operation["caller_id"].as_str() != Some(context.technical_requester_id())
                || operation["method"].as_str() != Some(METHOD)
                || operation["task_id"].as_str() != Some(context.task_id())
                || operation["attempt_id"].as_str() != Some(context.attempt_id())
            {
                return Err(Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "managed-label Operation identity differs from its retained on-behalf cause",
                ));
            }
            let current_state = operation["state"].as_str().unwrap_or_default();
            if current_state == "settled" {
                tx.commit()?;
                return Ok(());
            }
            let mut state = requested_state;
            let allowed_state = match requested_state {
                "settled" if write_attempted => "sending",
                "settled" | "rejected" => "queued",
                "outcome_unknown" => "sending",
                _ => return Err(Error::new("INTERNAL", "invalid automated GitHub effect state")),
            };
            if current_state != allowed_state {
                return Err(Error::conflict(
                    "the automated managed-label Operation changed before result recording",
                ));
            }

            let fence = (|| {
                context.require_current(&tx)?;
                let target = resolve_target_identity(&tx, &request, true)?;
                validate_automatic_target(&context, &request, &target)?;
                if !same_automatic_effect_target(&target, &expected_target) {
                    return Err(Error::new(
                        "GITHUB_EFFECT_TARGET_CHANGED",
                        "the registered source Issue, revision, or Task changed before managed-label settlement",
                    ));
                }
                require_automatic_slot(&tx, &request, &target, &operation_id)
            })();
            if let Err(fence_error) = fence {
                if write_attempted {
                    state = "outcome_unknown";
                    result["outcome"] = json!("settlement_blocked_by_current_authority_or_target");
                    result["readback"] = json!("observed_but_unsettled");
                    result["error"] = json!({"code":fence_error.code,"message":fence_error.message});
                } else {
                    return Err(fence_error);
                }
            }

            let allowed_from = match state {
                "settled" => "'queued','sending'",
                "rejected" => "'queued'",
                "outcome_unknown" => "'sending'",
                _ => return Err(Error::new("INTERNAL", "invalid automated GitHub effect state")),
            };
            let sql = format!(
                "UPDATE operations SET state=?2,result_json=?3,settled_at_ms=?4,updated_at_ms=?5 WHERE operation_id=?1 AND caller_id=?6 AND method=?7 AND state IN ({allowed_from})"
            );
            let changed = tx.execute(
                &sql,
                params![
                    operation_id,
                    state,
                    model::canonical(&result)?,
                    if state == "outcome_unknown" { None } else { Some(now) },
                    now,
                    context.technical_requester_id(),
                    METHOD
                ],
            )?;
            if changed != 1 {
                return Err(Error::conflict(
                    "the automated managed-label Operation changed before its result was recorded",
                ));
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

fn validate_automatic_target(
    context: &GithubProjectionContext,
    request: &ManagedLabelRequest,
    target: &EffectTarget,
) -> Result<()> {
    context.require_request_matches(request)?;
    if target.source_id != context.source_id()
        || target.source_revision != context.source_revision()
        || target.project_id != context.project_id()
        || target.task_id != context.task_id()
        || target.task_revision != context.task_revision()
    {
        return Err(Error::new(
            "AUTOMATION_GITHUB_PROJECTION_SOURCE_STALE",
            "registered GitHub target differs from the retained accepted-candidate cause",
        ));
    }
    Ok(())
}

fn require_automatic_slot(
    db: &Connection,
    request: &ManagedLabelRequest,
    target: &EffectTarget,
    operation_id: &str,
) -> Result<()> {
    let slot: Option<(i64, String)> = db
        .query_row(
            "SELECT desired_present,operation_id FROM github_label_effect_slots WHERE source_id=?1 AND issue_id=?2 AND label=?3",
            params![request.source_id, target.issue_id, request.label],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if slot.as_ref().is_none_or(|(present, operation)| {
        *present != i64::from(request.present) || operation != operation_id
    }) {
        return Err(Error::new(
            "GITHUB_EFFECT_SLOT_MISMATCH",
            "the retained Issue/label slot no longer points to this exact Operation",
        ));
    }
    Ok(())
}

fn same_automatic_effect_target(left: &EffectTarget, right: &EffectTarget) -> bool {
    left.source_id == right.source_id
        && left.source_revision == right.source_revision
        && left.project_id == right.project_id
        && left.host == right.host
        && left.owner == right.owner
        && left.repo == right.repo
        && left.repository_id == right.repository_id
        && left.issue_id == right.issue_id
        && left.issue_number == right.issue_number
        && left.task_id == right.task_id
        && left.task_revision == right.task_revision
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
            "source_revision":target.source_revision,
            "label":request.label,
            "present":request.present,
            "outcome":"managed_label_queued",
            "current_state_read_method":"operation.get"
        }),
        true,
    ))
}

/// Reserve the same durable `(source, Issue, label)` slot used by the direct
/// managed-label writer, with a closed accepted-candidate cause and current
/// Manager authority. This function performs no provider I/O.
pub(super) fn reserve_on_behalf(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    context: &GithubProjectionContext,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    if entry.owner_manager_id != context.effective_manager_id()
        || entry.project_id != context.project_id()
        || entry.automation_id != context.automation_id()
        || !entry.github_projection_ready()
    {
        return Err(Error::new(
            "AUTOMATION_ACTION_CHANGED",
            "GitHub projection settings changed before the managed-label slot was reserved",
        ));
    }
    context.require_current(tx)?;
    let request = ManagedLabelRequest::parse(value)?;
    context.require_request_matches(&request)?;
    let target = resolve_target_identity(tx, &request, true)?;
    if target.project_id != context.project_id()
        || target.task_id != context.task_id()
        || target.task_revision != context.task_revision()
        || target.source_id != context.source_id()
        || target.source_revision != context.source_revision()
    {
        return Err(Error::new(
            "AUTOMATION_GITHUB_PROJECTION_SOURCE_STALE",
            "registered source target differs from the exact accepted-candidate projection",
        ));
    }
    let previous: Option<(String, String)> = tx
        .query_row(
            "SELECT s.operation_id,o.state FROM github_label_effect_slots s JOIN operations o ON o.operation_id=s.operation_id WHERE s.source_id=?1 AND s.issue_id=?2 AND s.label=?3",
            params![request.source_id, target.issue_id, request.label],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if previous.is_some_and(|(previous_operation, state)| {
        previous_operation != operation_id
            && matches!(
                state.as_str(),
                "queued" | "sending" | "native_accepted" | "outcome_unknown"
            )
    }) {
        return Err(Error::new(
            "GITHUB_EFFECT_SLOT_BUSY",
            "the exact Issue/label slot has an unresolved Operation; reconcile it before admission",
        ));
    }
    tx.execute(
        "INSERT INTO github_label_effect_slots(source_id,issue_id,label,desired_present,operation_id,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(source_id,issue_id,label) DO UPDATE SET desired_present=excluded.desired_present,operation_id=excluded.operation_id,updated_at_ms=excluded.updated_at_ms",
        params![request.source_id, target.issue_id, request.label, request.present, operation_id, now],
    )?;
    let changed = tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1 AND caller_id=?4 AND method=?5 AND state='queued'",
        params![
            operation_id,
            request.task_id,
            context.attempt_id(),
            context.technical_requester_id(),
            METHOD
        ],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "the automated managed-label Operation changed during admission",
        ));
    }
    Ok(json!({
        "operation_id":operation_id,
        "source_id":request.source_id,
        "project_id":target.project_id,
        "task_id":request.task_id,
        "task_revision":request.expected_task_revision,
        "source_revision":target.source_revision,
        "issue_id":target.issue_id,
        "issue_number":target.issue_number,
        "label":request.label,
        "present":request.present,
        "desired_present":request.present,
        "outcome":"managed_label_queued",
        "current_state_read_method":"operation.get"
    }))
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
    let projection_context =
        if effective_request["automation_on_behalf"]["action"] == "github.effect.managed_label" {
            Some(GithubProjectionContext::from_committed_operation(
                db,
                operation_id,
            )?)
        } else {
            None
        };
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
    if let Some(context) = projection_context
        && (target.source_revision != context.source_revision()
            || target.task_revision != context.task_revision()
            || receipt["value"]["source_revision"] != context.source_revision()
            || result["source_revision"] != context.source_revision())
    {
        return Err(Error::new(
            "AUTOMATION_GITHUB_PROJECTION_SOURCE_STALE",
            "current registered source revision differs from the exact retained projection cause",
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
            "SELECT s.project_id,s.host,s.owner,s.repository_name,s.repository_id,i.issue_id,i.issue_number,i.mapping_status,i.source_revision,t.revision,t.project_id,m.selected FROM github_sources s JOIN github_issue_items i ON i.source_id=s.source_id JOIN tasks t ON t.task_id=i.task_id LEFT JOIN github_work_pool_members m ON m.source_id=i.source_id AND m.issue_id=i.issue_id AND m.task_id=t.task_id WHERE s.source_id=?1 AND i.task_id=?2",
            params![request.source_id, request.task_id],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?,row.get(9)?,row.get(10)?,row.get(11)?)),
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
        source_revision,
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
    if mapping != "mapped"
        || source_revision.trim().is_empty()
        || source_revision.len() > 512
        || source_revision.chars().any(char::is_control)
        || task_project != project_id
        || issue_id <= 0
        || issue_number <= 0
    {
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
        source_revision,
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
        "source_revision":target.source_revision,
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
        result["source_revision"] = json!(retained.target.source_revision);
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
