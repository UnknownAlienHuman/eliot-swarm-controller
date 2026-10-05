//! One manual PR title/body update bound to a successful accepted-candidate
//! publication. Ambiguous writes are retained as unknown and reconciled only
//! through exact PR readback; this module never creates or merges a PR.

use super::{Store, capacity, current_principal, forge, mutate, operations};
use crate::{
    config::Config,
    error::{Error, Result},
    forge::PublicationIntent,
    github::{
        client::{GhCli, GitHubPullRequestApi, PullRequestReadback, RepositoryRef},
        protocol::{
            self, PullRequestDescriptionReconcileRequest, PullRequestDescriptionUpdateRequest,
        },
    },
    model::{self, Principal},
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};

const METHOD: &str = "github.pull_request.update_description";
const RECONCILE_METHOD: &str = "github.pull_request.reconcile_description";

#[cfg(test)]
#[path = "github_pr_effect_tests.rs"]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Target {
    publication_operation_id: String,
    task_id: String,
    attempt_id: String,
    project_id: String,
    source_id: String,
    repository_id: i64,
    host: String,
    owner: String,
    repo: String,
    pull_request_id: i64,
    pull_request_number: i64,
    head_sha: String,
    target_ref: String,
    base_ref: String,
}

pub(super) async fn call(store: &Store, principal: Principal, value: Value) -> Result<Value> {
    call_with_api_inner(store, principal, value, &GhCli).await
}

#[cfg(test)]
pub(super) async fn call_with_api<A: GitHubPullRequestApi + ?Sized>(
    store: &Store,
    principal: Principal,
    value: Value,
    api: &A,
) -> Result<Value> {
    call_with_api_inner(store, principal, value, api).await
}

async fn call_with_api_inner<A: GitHubPullRequestApi + ?Sized>(
    store: &Store,
    principal: Principal,
    value: Value,
    api: &A,
) -> Result<Value> {
    let value = protocol::validate_mutation(METHOD, &value)?;
    let request = PullRequestDescriptionUpdateRequest::parse(&value)?;
    let config = store.config.clone();
    let receipt = store
        .run({
            let principal = principal.clone();
            let value = value.clone();
            let config = config.clone();
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
                "GITHUB_PR_UPDATE_CANCELLED",
                "the retained PR description Operation was cancelled before dispatch",
            ));
        }
        Some("sending") => {
            return Err(Error::new(
                "GITHUB_PR_UPDATE_IN_PROGRESS",
                "the PR update may be in flight; inspect its Operation before retrying",
            ));
        }
        Some("queued" | "outcome_unknown") => {}
        _ => {
            return Err(Error::new(
                "GITHUB_PR_UPDATE_NOT_RESUMABLE",
                "the retained PR description Operation is not in a resumable state",
            ));
        }
    }

    let readback_only = operation["state"] == "outcome_unknown";
    let target = match load_target(store, &principal, &request, &operation_id, &config).await {
        Ok(target) => target,
        Err(error) if !readback_only => {
            reject_before_write(store, &principal, &operation_id, &request, &error).await?;
            return Err(error);
        }
        Err(error) => return Err(error),
    };
    let repository = RepositoryRef::new(&target.host, &target.owner, &target.repo)?;
    let initial = match read_remote(api, &repository, &target).await {
        Ok(readback) => readback,
        Err(error) if !readback_only => {
            reject_before_write(store, &principal, &operation_id, &request, &error).await?;
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
                Some(&error),
            )
            .await;
        }
    };

    if readback_only {
        if desired_is_observed(&initial, &request) {
            return settle(
                store,
                &principal,
                &operation_id,
                &target,
                &request,
                "reconciled_from_readback",
                true,
            )
            .await;
        }
        return record_unknown(
            store,
            &principal,
            &operation_id,
            &target,
            &request,
            "desired_description_not_observed",
            None,
        )
        .await;
    }

    if desired_is_observed(&initial, &request) {
        return settle(
            store,
            &principal,
            &operation_id,
            &target,
            &request,
            "already_in_desired_state",
            false,
        )
        .await;
    }

    if let Err(error) = begin_write(store, &principal, &operation_id, &request, &config).await {
        // begin_write is the durable effect boundary. No PATCH is issued unless
        // it commits `sending`; reject only the exact Operation if it is still
        // queued. A concurrent writer, cancellation, or unknown outcome wins
        // its CAS and is never overwritten here.
        if let Err(record_error) =
            reject_before_write(store, &principal, &operation_id, &request, &error).await
        {
            return Err(Error::new(
                "GITHUB_PR_PRE_WRITE_RESULT_NOT_RECORDED",
                format!(
                    "no PATCH was sent by this path; the exact queued Operation could not be terminalized ({}) and must be inspected with operation.get",
                    record_error.code
                ),
            ));
        }
        return Err(error);
    }
    // One PATCH at most. Even a transport error is ambiguous after the durable
    // sending transition, so every path below performs only exact readback.
    let write_error = api
        .update_description(
            &repository,
            target.pull_request_number,
            &request.title,
            &request.body,
        )
        .await
        .err();
    match read_remote(api, &repository, &target).await {
        Ok(readback) if desired_is_observed(&readback, &request) => {
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
            )
            .await
        }
        Ok(_) => {
            record_unknown(
                store,
                &principal,
                &operation_id,
                &target,
                &request,
                "desired_description_not_observed_after_write",
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
                Some(write_error.as_ref().unwrap_or(&error)),
            )
            .await
        }
    }
}

pub(super) async fn reconcile_call(
    store: &Store,
    principal: Principal,
    value: Value,
) -> Result<Value> {
    reconcile_call_with_api_inner(store, principal, value, &GhCli).await
}

#[cfg(test)]
pub(super) async fn reconcile_call_with_api<A: GitHubPullRequestApi + ?Sized>(
    store: &Store,
    principal: Principal,
    value: Value,
    api: &A,
) -> Result<Value> {
    reconcile_call_with_api_inner(store, principal, value, api).await
}

async fn reconcile_call_with_api_inner<A: GitHubPullRequestApi + ?Sized>(
    store: &Store,
    principal: Principal,
    value: Value,
    api: &A,
) -> Result<Value> {
    let value = protocol::validate_mutation(RECONCILE_METHOD, &value)?;
    let request = PullRequestDescriptionReconcileRequest::parse(&value)?;
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
    let reconciliation_operation_id = model::text(&receipt, "operation_id")?.to_owned();
    let operation = store
        .run({
            let operation_id = reconciliation_operation_id.clone();
            move |db| operations::get_operation(db, &operation_id)
        })
        .await?;
    match operation["state"].as_str() {
        Some("settled") => return operation_result(store, &reconciliation_operation_id).await,
        Some("rejected") => return Err(reconciliation_error(&operation["result"])),
        Some("cancelled") => {
            return Err(Error::new(
                "GITHUB_PR_RECONCILE_CANCELLED",
                "the PR readback Operation was cancelled before dispatch",
            ));
        }
        Some("sending") => {
            return Err(Error::new(
                "GITHUB_PR_RECONCILE_IN_PROGRESS",
                "the exact PR readback may be in flight; inspect the reconciliation Operation",
            ));
        }
        Some("queued" | "outcome_unknown") => {}
        _ => {
            return Err(Error::new(
                "GITHUB_PR_RECONCILE_NOT_RESUMABLE",
                "the retained PR reconciliation Operation is not resumable",
            ));
        }
    }

    let (target, desired) =
        begin_reconcile_readback(store, &principal, &reconciliation_operation_id, &request).await?;
    let repository = RepositoryRef::new(&target.host, &target.owner, &target.repo)?;
    match read_remote(api, &repository, &target).await {
        Ok(readback) => {
            let observed = Some((
                readback.title.clone(),
                model::digest(readback.body.as_deref().unwrap_or("").as_bytes()),
            ));
            let (state, outcome) = if desired_is_observed(&readback, &desired) {
                ("settled", "confirmed_by_exact_readback")
            } else {
                ("settled", "desired_description_not_observed")
            };
            persist_reconcile_readback(
                store,
                &principal,
                &reconciliation_operation_id,
                &request,
                &target,
                &desired,
                state,
                outcome,
                observed,
                None,
            )
            .await
        }
        Err(error) => {
            persist_reconcile_readback(
                store,
                &principal,
                &reconciliation_operation_id,
                &request,
                &target,
                &desired,
                "outcome_unknown",
                "readback_unavailable",
                None,
                Some(&error),
            )
            .await
        }
    }
}

pub(super) fn apply_reconcile(
    tx: &Transaction<'_>,
    principal: &Principal,
    value: &Value,
    operation_id: &str,
    _now: i64,
) -> Result<(Value, bool)> {
    let request = PullRequestDescriptionReconcileRequest::parse(value)?;
    let (target, _) = resolve_reconcile_target(tx, principal, &request.operation_id)?;
    let operation = operations::get_operation(tx, operation_id)?;
    if operation["method"] != RECONCILE_METHOD
        || operation["caller_id"] != principal.client_id
        || operation["state"] != "queued"
    {
        return Err(Error::conflict(
            "the PR reconciliation Operation changed during admission",
        ));
    }
    let changed = tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1 AND method=?4 AND state='queued'",
        params![operation_id, target.task_id, target.attempt_id, RECONCILE_METHOD],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "the PR reconciliation Operation changed during admission",
        ));
    }
    Ok((
        json!({
            "operation_id":operation_id,
            "target_operation_id":request.operation_id,
            "publication_operation_id":target.publication_operation_id,
            "project_id":target.project_id,
            "source_id":target.source_id,
            "task_id":target.task_id,
            "attempt_id":target.attempt_id,
            "repository_id":target.repository_id,
            "pull_request_id":target.pull_request_id,
            "pull_request_number":target.pull_request_number,
            "head_sha":target.head_sha,
            "target_ref":target.target_ref,
            "base_ref":target.base_ref,
            "outcome":"pull_request_readback_queued",
            "readback_only":true,
            "write_attempted":false,
            "current_state_read_method":"operation.get"
        }),
        true,
    ))
}

pub(super) fn apply(
    tx: &Transaction<'_>,
    principal: &Principal,
    value: &Value,
    operation_id: &str,
    now: i64,
    config: &Config,
) -> Result<(Value, bool)> {
    let request = PullRequestDescriptionUpdateRequest::parse(value)?;
    let target = resolve_target(tx, principal, &request, operation_id, config)?;
    let previous: Option<String> = tx
        .query_row(
            "SELECT operation_id FROM github_pr_effect_slots WHERE repository_id=?1 AND pull_request_id=?2",
            params![target.repository_id, target.pull_request_id],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(previous_id) = previous {
        let previous_operation = operations::get_operation(tx, &previous_id)?;
        if matches!(
            previous_operation["state"].as_str(),
            Some("queued" | "sending" | "native_accepted" | "outcome_unknown")
        ) {
            return Err(Error::conflict(
                "the PR description resource has an unresolved Operation across head changes; reconcile it before admitting another update",
            ));
        }
    }
    tx.execute(
        "INSERT INTO github_pr_effect_slots(repository_id,pull_request_id,head_sha,operation_id,updated_at_ms) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(repository_id,pull_request_id) DO UPDATE SET head_sha=excluded.head_sha,operation_id=excluded.operation_id,updated_at_ms=excluded.updated_at_ms",
        params![target.repository_id, target.pull_request_id, target.head_sha, operation_id, now],
    )?;
    let changed = tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1 AND method=?4 AND state='queued'",
        params![operation_id, target.task_id, target.attempt_id, METHOD],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "the PR description Operation changed during admission",
        ));
    }
    Ok((
        json!({
            "operation_id":operation_id,
            "publication_operation_id":target.publication_operation_id,
            "project_id":target.project_id,
            "source_id":target.source_id,
            "task_id":target.task_id,
            "attempt_id":target.attempt_id,
            "repository_id":target.repository_id,
            "pull_request_id":target.pull_request_id,
            "pull_request_number":target.pull_request_number,
            "head_sha":target.head_sha,
            "target_ref":target.target_ref,
            "base_ref":target.base_ref,
            "outcome":"pull_request_description_queued",
            "current_state_read_method":"operation.get"
        }),
        true,
    ))
}

async fn begin_reconcile_readback(
    store: &Store,
    principal: &Principal,
    reconciliation_operation_id: &str,
    request: &PullRequestDescriptionReconcileRequest,
) -> Result<(Target, PullRequestDescriptionUpdateRequest)> {
    let principal = principal.clone();
    let reconciliation_operation_id = reconciliation_operation_id.to_owned();
    let request = request.clone();
    store
        .run(move |db| {
            let current = current_principal(db, principal)?;
            let now = model::now_ms()?;
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let reconciliation = operations::get_operation(&tx, &reconciliation_operation_id)?;
            if reconciliation["method"] != RECONCILE_METHOD
                || reconciliation["caller_id"] != current.client_id
                || !matches!(
                    reconciliation["state"].as_str(),
                    Some("queued" | "outcome_unknown")
                )
            {
                return Err(Error::conflict(
                    "the retained PR reconciliation Operation is not readback-ready",
                ));
            }
            let stored_request = stored_original_request(&tx, &reconciliation_operation_id)?;
            if stored_client_request_id(&tx, &reconciliation_operation_id)?
                != request.client_request_id
                || model::canonical(&stored_request)? != model::canonical(&json!({
                "client_request_id":request.client_request_id,
                "operation_id":request.operation_id
            }))? {
                return Err(Error::new(
                    "GITHUB_PR_RECONCILE_REQUEST_MISMATCH",
                    "the retained reconciliation request differs from this readback",
                ));
            }
            let (target, desired) =
                resolve_reconcile_target(&tx, &current, &request.operation_id)?;
            if reconciliation["task_id"] != target.task_id
                || reconciliation["attempt_id"] != target.attempt_id
            {
                return Err(Error::new(
                    "GITHUB_PR_RECONCILE_SCOPE_MISMATCH",
                    "the reconciliation Operation is not bound to the exact target Task and Attempt",
                ));
            }
            let changed = tx.execute(
                "UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1 AND state IN ('queued','outcome_unknown')",
                params![reconciliation_operation_id, now],
            )?;
            if changed != 1 {
                return Err(Error::conflict(
                    "the PR reconciliation Operation changed before readback",
                ));
            }
            capacity::sync_operation(&tx, &reconciliation_operation_id, now)?;
            tx.commit()?;
            Ok((target, desired))
        })
        .await
}

fn resolve_reconcile_target(
    db: &Connection,
    principal: &Principal,
    operation_id: &str,
) -> Result<(Target, PullRequestDescriptionUpdateRequest)> {
    let current = current_principal(db, principal.clone())?;
    if !super::operation_visible_to(db, &current, operation_id)? {
        return Err(Error::new("NOT_FOUND", format!("Operation {operation_id}")));
    }
    let operation = operations::get_operation(db, operation_id)?;
    if operation["method"] != METHOD || operation["state"] != "outcome_unknown" {
        return Err(Error::conflict(
            "readback reconciliation requires the exact retained unknown PR description Operation",
        ));
    }
    let request =
        PullRequestDescriptionUpdateRequest::parse(&stored_original_request(db, operation_id)?)?;
    if stored_client_request_id(db, operation_id)? != request.client_request_id {
        return Err(Error::new(
            "GITHUB_PR_OPERATION_MISMATCH",
            "the retained PR Operation request ID differs from its original request",
        ));
    }
    let target = resolve_historical_target(db, &current, &request)?;
    validate_unknown_operation_identity(&operation, &request, &target)?;
    let slot: Option<(String, String)> = db
        .query_row(
            "SELECT operation_id,head_sha FROM github_pr_effect_slots WHERE repository_id=?1 AND pull_request_id=?2",
            params![target.repository_id, target.pull_request_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if !matches!(slot.as_ref(), Some((slot_operation, slot_head)) if slot_operation == operation_id && slot_head == &target.head_sha)
    {
        return Err(Error::new(
            "GITHUB_PR_SLOT_MISMATCH",
            "the exact repository/PR resource slot and retained head no longer point to this unknown Operation",
        ));
    }
    Ok((target, request))
}

/// Revalidate immutable target and slot facts after the authorized GET without
/// rechecking current GM rights. The reconciliation Operation was authorized
/// before network I/O; this path can only record that already-observed result.
fn resolve_reconcile_target_after_readback(
    db: &Connection,
    operation_id: &str,
) -> Result<(Target, PullRequestDescriptionUpdateRequest)> {
    let operation = operations::get_operation(db, operation_id)?;
    if operation["method"] != METHOD || operation["state"] != "outcome_unknown" {
        return Err(Error::conflict(
            "the original PR Operation is no longer the retained unknown target",
        ));
    }
    let request =
        PullRequestDescriptionUpdateRequest::parse(&stored_original_request(db, operation_id)?)?;
    if stored_client_request_id(db, operation_id)? != request.client_request_id {
        return Err(Error::new(
            "GITHUB_PR_OPERATION_MISMATCH",
            "the retained PR Operation request ID differs from its original request",
        ));
    }
    let target = resolve_retained_target(db, &request)?;
    validate_unknown_operation_identity(&operation, &request, &target)?;
    let slot: Option<(String, String)> = db
        .query_row(
            "SELECT operation_id,head_sha FROM github_pr_effect_slots WHERE repository_id=?1 AND pull_request_id=?2",
            params![target.repository_id, target.pull_request_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if !matches!(slot.as_ref(), Some((slot_operation, slot_head)) if slot_operation == operation_id && slot_head == &target.head_sha)
    {
        return Err(Error::new(
            "GITHUB_PR_SLOT_MISMATCH",
            "the exact repository/PR resource slot and retained head no longer point to this unknown Operation",
        ));
    }
    Ok((target, request))
}

fn validate_unknown_operation_identity(
    operation: &Value,
    request: &PullRequestDescriptionUpdateRequest,
    target: &Target,
) -> Result<()> {
    let result = &operation["result"];
    let operation_id = model::text(operation, "operation_id")?;
    if operation["task_id"] != target.task_id
        || operation["attempt_id"] != target.attempt_id
        || result["operation_id"] != operation_id
        || result["publication_operation_id"] != target.publication_operation_id
        || result["project_id"] != target.project_id
        || result["source_id"] != target.source_id
        || result["task_id"] != target.task_id
        || result["attempt_id"] != target.attempt_id
        || result["repository_id"] != target.repository_id
        || result["pull_request_id"] != target.pull_request_id
        || result["pull_request_number"] != target.pull_request_number
        || result["head_sha"] != target.head_sha
        || result["target_ref"] != target.target_ref
        || result["base_ref"] != target.base_ref
        || result["write_attempted"] != true
        || (!result["requested_title"].is_null() && result["requested_title"] != request.title)
        || (!result["requested_body_digest"].is_null()
            && result["requested_body_digest"] != model::digest(request.body.as_bytes()))
    {
        return Err(Error::new(
            "GITHUB_PR_OPERATION_MISMATCH",
            "the retained unknown Operation does not prove this exact publication, Task, Attempt, repository, PR, head, base, and desired description",
        ));
    }
    Ok(())
}

fn resolve_historical_target(
    db: &Connection,
    principal: &Principal,
    request: &PullRequestDescriptionUpdateRequest,
) -> Result<Target> {
    let (intent, task_id) = forge::historical_applied_publication_context(
        db,
        principal,
        &request.publication_operation_id,
    )?;
    target_from_publication(db, request, intent, task_id)
}

fn resolve_retained_target(
    db: &Connection,
    request: &PullRequestDescriptionUpdateRequest,
) -> Result<Target> {
    let (intent, task_id) =
        forge::retained_applied_publication_record(db, &request.publication_operation_id)?;
    target_from_publication(db, request, intent, task_id)
}

fn target_from_publication(
    db: &Connection,
    request: &PullRequestDescriptionUpdateRequest,
    intent: PublicationIntent,
    task_id: String,
) -> Result<Target> {
    let (host, owner, repo) = repository_parts(&intent)?;
    let row: Option<(String, i64)> = db
        .query_row(
            "SELECT source_id,repository_id FROM github_sources WHERE project_id=?1 AND host=?2 AND owner=?3 AND repository_name=?4",
            params![intent.project_id, host, owner, repo],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (source_id, repository_id) = row.ok_or_else(|| {
        Error::new(
            "GITHUB_PR_SOURCE_NOT_FOUND",
            "the retained publication repository has no exact registered GitHub source identity",
        )
    })?;
    if repository_id <= 0 {
        return Err(Error::new(
            "GITHUB_PR_SOURCE_INVALID",
            "the registered repository identity is invalid",
        ));
    }
    Ok(Target {
        publication_operation_id: request.publication_operation_id.clone(),
        task_id,
        attempt_id: intent.attempt_id.clone(),
        project_id: intent.project_id.clone(),
        source_id,
        repository_id,
        host,
        owner,
        repo,
        pull_request_id: request.pull_request_id,
        pull_request_number: request.pull_request_number,
        head_sha: intent.commit,
        target_ref: intent.target_ref,
        base_ref: request.base_ref.clone(),
    })
}

fn stored_original_request(db: &Connection, operation_id: &str) -> Result<Value> {
    let raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| row.get(0),
    )?;
    serde_json::from_str(&raw).map_err(Into::into)
}

fn stored_client_request_id(db: &Connection, operation_id: &str) -> Result<String> {
    db.query_row(
        "SELECT client_request_id FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

async fn load_target(
    store: &Store,
    principal: &Principal,
    request: &PullRequestDescriptionUpdateRequest,
    operation_id: &str,
    config: &Config,
) -> Result<Target> {
    let principal = principal.clone();
    let request = request.clone();
    let operation_id = operation_id.to_owned();
    let config = config.clone();
    store
        .run(move |db| {
            let current = current_principal(db, principal)?;
            let operation = operations::get_operation(db, &operation_id)?;
            verify_operation(&operation, &current)?;
            let target = resolve_target(db, &current, &request, &operation_id, &config)?;
            if operation["task_id"] != target.task_id
                || operation["attempt_id"] != target.attempt_id
            {
                return Err(Error::new(
                    "GITHUB_PR_OPERATION_MISMATCH",
                    "the retained Operation no longer identifies the exact published Task and Attempt",
                ));
            }
            let slot: Option<(String, String)> = db
                .query_row(
                    "SELECT operation_id,head_sha FROM github_pr_effect_slots WHERE repository_id=?1 AND pull_request_id=?2",
                    params![target.repository_id, target.pull_request_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            if !matches!(slot.as_ref(), Some((slot_operation, slot_head)) if slot_operation == &operation_id && slot_head == &target.head_sha) {
                return Err(Error::new(
                    "GITHUB_PR_SLOT_MISMATCH",
                    "the exact repository/PR resource slot and retained head no longer point to this Operation",
                ));
            }
            Ok(target)
        })
        .await
}

fn resolve_target(
    db: &Connection,
    principal: &Principal,
    request: &PullRequestDescriptionUpdateRequest,
    operation_id: &str,
    config: &Config,
) -> Result<Target> {
    let (intent, task_id) = forge::applied_publication_context(
        db,
        principal,
        &request.publication_operation_id,
        config,
    )?;
    let (host, owner, repo) = repository_parts(&intent)?;
    let row: Option<(String, i64)> = db
        .query_row(
            "SELECT source_id,repository_id FROM github_sources WHERE project_id=?1 AND host=?2 AND owner=?3 AND repository_name=?4",
            params![intent.project_id, host, owner, repo],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (source_id, repository_id) = row.ok_or_else(|| {
        Error::new(
            "GITHUB_PR_SOURCE_NOT_FOUND",
            "the published repository has no exact registered GitHub source identity",
        )
    })?;
    if repository_id <= 0 {
        return Err(Error::new(
            "GITHUB_PR_SOURCE_INVALID",
            "the registered repository identity is invalid",
        ));
    }
    if !operation_id.is_empty() {
        let operation = operations::get_operation(db, operation_id)?;
        if operation["method"] == METHOD
            && !operation["task_id"].is_null()
            && operation["task_id"] != task_id
        {
            return Err(Error::new(
                "GITHUB_PR_OPERATION_MISMATCH",
                "the PR Operation no longer identifies the published Task",
            ));
        }
    }
    Ok(Target {
        publication_operation_id: request.publication_operation_id.clone(),
        task_id,
        attempt_id: intent.attempt_id.clone(),
        project_id: intent.project_id.clone(),
        source_id,
        repository_id,
        host,
        owner,
        repo,
        pull_request_id: request.pull_request_id,
        pull_request_number: request.pull_request_number,
        head_sha: intent.commit,
        target_ref: intent.target_ref,
        base_ref: request.base_ref.clone(),
    })
}

fn repository_parts(intent: &PublicationIntent) -> Result<(String, String, String)> {
    let parts: Vec<_> = intent.canonical_repository.split('/').collect();
    if parts.len() != 3 {
        return Err(Error::new(
            "GITHUB_PR_REPOSITORY_UNSUPPORTED",
            "PR effects require one canonical host/owner/repository path",
        ));
    }
    let repository = RepositoryRef::new(parts[0], parts[1], parts[2])?;
    if repository.host != parts[0].to_ascii_lowercase()
        || repository.owner != parts[1]
        || repository.name != parts[2]
    {
        return Err(Error::new(
            "GITHUB_PR_REPOSITORY_UNSUPPORTED",
            "the canonical forge repository cannot be represented as one GitHub REST repository path",
        ));
    }
    Ok((repository.host, repository.owner, repository.name))
}

fn verify_operation(operation: &Value, principal: &Principal) -> Result<()> {
    if operation["method"] != METHOD || operation["caller_id"] != principal.client_id {
        return Err(Error::new(
            "GITHUB_PR_OPERATION_MISMATCH",
            "the retained Operation does not belong to this exact caller and method",
        ));
    }
    Ok(())
}

async fn read_remote<A: GitHubPullRequestApi + ?Sized>(
    api: &A,
    repository: &RepositoryRef,
    target: &Target,
) -> Result<PullRequestReadback> {
    let repo = api.repository(repository).await?;
    if repo.id != target.repository_id {
        return Err(Error::new(
            "GITHUB_REPOSITORY_IDENTITY_CHANGED",
            "the configured repository path no longer identifies the registered repository",
        ));
    }
    let pull = api
        .pull_request(repository, target.pull_request_number)
        .await?;
    let target_branch = target
        .target_ref
        .strip_prefix("refs/heads/")
        .ok_or_else(|| Error::new("GITHUB_PR_TARGET_INVALID", "target ref is not a branch"))?;
    let expected_base = target
        .base_ref
        .strip_prefix("refs/heads/")
        .ok_or_else(|| Error::new("GITHUB_PR_TARGET_INVALID", "base ref is not a branch"))?;
    if pull.id != target.pull_request_id
        || pull.number != target.pull_request_number
        || pull.state != "open"
        || pull.merged
        || pull.head.repo.as_ref().map(|repo| repo.id) != Some(target.repository_id)
        || pull.base.repo.as_ref().map(|repo| repo.id) != Some(target.repository_id)
        || pull.head.ref_name.as_deref() != Some(target_branch)
        || pull.base.ref_name.as_deref() != Some(expected_base)
        || !pull.head.sha.eq_ignore_ascii_case(&target.head_sha)
    {
        return Err(Error::new(
            "GITHUB_PR_IDENTITY_CHANGED",
            "PR ID, open state, same-repository head/base, target branch, or exact published head SHA did not match",
        ));
    }
    Ok(pull)
}

fn desired_is_observed(
    pull: &PullRequestReadback,
    request: &PullRequestDescriptionUpdateRequest,
) -> bool {
    pull.title == request.title && pull.body.as_deref().unwrap_or("") == request.body
}

async fn begin_write(
    store: &Store,
    principal: &Principal,
    operation_id: &str,
    request: &PullRequestDescriptionUpdateRequest,
    config: &Config,
) -> Result<()> {
    let principal = principal.clone();
    let operation_id = operation_id.to_owned();
    let request = request.clone();
    let config = config.clone();
    store
        .run(move |db| {
            let now = model::now_ms()?;
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let current = current_principal(&tx, principal)?;
            let operation = operations::get_operation(&tx, &operation_id)?;
            verify_operation(&operation, &current)?;
            let target = resolve_target(&tx, &current, &request, &operation_id, &config)?;
            if operation["task_id"] != target.task_id
                || operation["attempt_id"] != target.attempt_id
            {
                return Err(Error::new(
                    "GITHUB_PR_OPERATION_MISMATCH",
                    "the PR Operation no longer identifies the published Task and Attempt",
                ));
            }
            let slot: Option<(String, String)> = tx
                .query_row(
                    "SELECT operation_id,head_sha FROM github_pr_effect_slots WHERE repository_id=?1 AND pull_request_id=?2",
                    params![target.repository_id, target.pull_request_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            if !matches!(slot.as_ref(), Some((slot_operation, slot_head)) if slot_operation == &operation_id && slot_head == &target.head_sha) || operation["state"] != "queued" {
                return Err(Error::conflict(
                    "the exact PR resource slot, retained head, or queued Operation changed before write",
                ));
            }
            let changed = tx.execute(
                "UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1 AND state='queued'",
                params![operation_id, now],
            )?;
            if changed != 1 {
                return Err(Error::conflict(
                    "the PR description Operation changed before write",
                ));
            }
            capacity::sync_operation(&tx, &operation_id, now)?;
            tx.commit()?;
            Ok(())
        })
        .await
}

async fn reject_before_write(
    store: &Store,
    principal: &Principal,
    operation_id: &str,
    request: &PullRequestDescriptionUpdateRequest,
    error: &Error,
) -> Result<()> {
    persist(
        store,
        principal,
        operation_id,
        json!({
            "operation_id":operation_id,
            "publication_operation_id":request.publication_operation_id,
            "pull_request_id":request.pull_request_id,
            "pull_request_number":request.pull_request_number,
            "outcome":"rejected_before_write",
            "write_attempted":false,
            "error":{"code":error.code,"message":error.message},
            "current_state_read_method":"operation.get"
        }),
        "rejected",
    )
    .await
}

// Keep the authenticated observation tied to its retained Operation, resource
// and desired state through the short final transaction.
#[allow(clippy::too_many_arguments)]
async fn persist_reconcile_readback(
    store: &Store,
    principal: &Principal,
    reconciliation_operation_id: &str,
    request: &PullRequestDescriptionReconcileRequest,
    target: &Target,
    desired: &PullRequestDescriptionUpdateRequest,
    state: &'static str,
    outcome: &str,
    observed: Option<(String, String)>,
    error: Option<&Error>,
) -> Result<Value> {
    let principal = principal.clone();
    let reconciliation_operation_id = reconciliation_operation_id.to_owned();
    let request = request.clone();
    let target = target.clone();
    let desired = desired.clone();
    let outcome = outcome.to_owned();
    let error = error.map(|error| json!({"code":error.code,"message":error.message}));
    store
        .run(move |db| {
            let current = current_principal(db, principal)?;
            let now = model::now_ms()?;
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let reconciliation =
                operations::get_operation(&tx, &reconciliation_operation_id)?;
            if reconciliation["method"] != RECONCILE_METHOD
                || reconciliation["caller_id"] != current.client_id
                || !matches!(
                    reconciliation["state"].as_str(),
                    Some("sending" | "outcome_unknown")
                )
            {
                return Err(Error::conflict(
                    "the PR reconciliation Operation changed before its readback was recorded",
                ));
            }
            let stored_request = stored_original_request(&tx, &reconciliation_operation_id)?;
            if stored_client_request_id(&tx, &reconciliation_operation_id)?
                != request.client_request_id
                || model::canonical(&stored_request)? != model::canonical(&json!({
                "client_request_id":request.client_request_id,
                "operation_id":request.operation_id
            }))? {
                return Err(Error::new(
                    "GITHUB_PR_RECONCILE_REQUEST_MISMATCH",
                    "the retained reconciliation request differs from this readback",
                ));
            }
            let (retained_target, retained_desired) =
                resolve_reconcile_target_after_readback(&tx, &request.operation_id)?;
            if retained_target != target
                || retained_desired != desired
                || reconciliation["task_id"] != target.task_id
                || reconciliation["attempt_id"] != target.attempt_id
            {
                return Err(Error::conflict(
                    "the exact PR, publication, Task, Attempt, slot, or desired state changed during readback",
                ));
            }

            let mut target_operation_id = None;
            if outcome == "confirmed_by_exact_readback" {
                let Some((observed_title, observed_body_digest)) = observed.as_ref() else {
                    return Err(Error::new(
                        "GITHUB_PR_RECONCILE_INVALID",
                        "confirmed readback omitted its observed description",
                    ));
                };
                if observed_title != &desired.title
                    || observed_body_digest != &model::digest(desired.body.as_bytes())
                {
                    return Err(Error::new(
                        "GITHUB_PR_RECONCILE_INVALID",
                        "readback confirmation differs from the exact retained desired description",
                    ));
                }
                let original = operations::get_operation(&tx, &request.operation_id)?;
                let mut original_result = original["result"].clone();
                original_result["requested_title"] = json!(desired.title);
                original_result["requested_body_digest"] =
                    json!(model::digest(desired.body.as_bytes()));
                original_result["outcome"] = json!("reconciled_from_readback");
                original_result["readback"] = json!("confirmed");
                original_result["write_attempted"] = json!(true);
                original_result["readback_reconciliation"] = json!({
                    "operation_id":reconciliation_operation_id,
                    "reconciler_client_id":current.client_id,
                    "confirmed_at_ms":now,
                    "observed_title":observed_title,
                    "observed_body_digest":observed_body_digest,
                    "readback_only":true
                });
                let changed = tx.execute(
                    "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND method=?4 AND state='outcome_unknown'",
                    params![
                        request.operation_id,
                        model::canonical(&original_result)?,
                        now,
                        METHOD
                    ],
                )?;
                if changed != 1 {
                    return Err(Error::conflict(
                        "the original PR Operation no longer has its retained unknown outcome",
                    ));
                }
                capacity::sync_operation(&tx, &request.operation_id, now)?;
                target_operation_id = Some(request.operation_id.clone());
                let event_key = format!("settled:{}", request.operation_id);
                tx.execute(
                    "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('github:pull-request',?1,?2,?3,?4,?5)",
                    params![
                        event_key,
                        request.operation_id,
                        METHOD,
                        model::canonical(&original_result)?,
                        now
                    ],
                )?;
            }

            let readback = match outcome.as_str() {
                "confirmed_by_exact_readback" => "confirmed",
                "desired_description_not_observed" => "not_confirmed",
                _ => "unavailable",
            };
            let result = json!({
                "operation_id":reconciliation_operation_id,
                "target_operation_id":request.operation_id,
                "publication_operation_id":target.publication_operation_id,
                "project_id":target.project_id,
                "source_id":target.source_id,
                "task_id":target.task_id,
                "attempt_id":target.attempt_id,
                "repository_id":target.repository_id,
                "pull_request_id":target.pull_request_id,
                "pull_request_number":target.pull_request_number,
                "head_sha":target.head_sha,
                "target_ref":target.target_ref,
                "base_ref":target.base_ref,
                "requested_title":desired.title,
                "requested_body_digest":model::digest(desired.body.as_bytes()),
                "observed_title":observed.as_ref().map(|value| value.0.as_str()),
                "observed_body_digest":observed.as_ref().map(|value| value.1.as_str()),
                "target_operation_state":if target_operation_id.is_some() { "settled" } else { "outcome_unknown" },
                "readback":readback,
                "readback_only":true,
                "write_attempted":false,
                "outcome":outcome,
                "error":error,
                "operation_state":state,
                "current_state_read_method":"operation.get"
            });
            let changed = tx.execute(
                "UPDATE operations SET state=?2,result_json=?3,settled_at_ms=?4,updated_at_ms=?5 WHERE operation_id=?1 AND method=?6 AND state IN ('sending','outcome_unknown')",
                params![
                    reconciliation_operation_id,
                    state,
                    model::canonical(&result)?,
                    if state == "settled" { Some(now) } else { None },
                    now,
                    RECONCILE_METHOD
                ],
            )?;
            if changed != 1 {
                return Err(Error::conflict(
                    "the PR reconciliation Operation changed before its readback was recorded",
                ));
            }
            capacity::sync_operation(&tx, &reconciliation_operation_id, now)?;
            if state == "settled" {
                let event_key = format!("reconcile:{}", reconciliation_operation_id);
                tx.execute(
                    "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('github:pull-request',?1,?2,?3,?4,?5)",
                    params![
                        event_key,
                        reconciliation_operation_id,
                        RECONCILE_METHOD,
                        model::canonical(&result)?,
                        now
                    ],
                )?;
            }
            tx.commit()?;
            Ok(result)
        })
        .await
}

async fn settle(
    store: &Store,
    principal: &Principal,
    operation_id: &str,
    target: &Target,
    request: &PullRequestDescriptionUpdateRequest,
    outcome: &str,
    write_attempted: bool,
) -> Result<Value> {
    persist(
        store,
        principal,
        operation_id,
        result_value(
            operation_id,
            target,
            request,
            outcome,
            "confirmed",
            write_attempted,
            None,
        ),
        "settled",
    )
    .await?;
    operation_result(store, operation_id).await
}

async fn record_unknown(
    store: &Store,
    principal: &Principal,
    operation_id: &str,
    target: &Target,
    request: &PullRequestDescriptionUpdateRequest,
    reason: &str,
    error: Option<&Error>,
) -> Result<Value> {
    persist(
        store,
        principal,
        operation_id,
        result_value(
            operation_id,
            target,
            request,
            reason,
            "unknown",
            true,
            error,
        ),
        "outcome_unknown",
    )
    .await?;
    operation_result(store, operation_id).await
}

fn result_value(
    operation_id: &str,
    target: &Target,
    request: &PullRequestDescriptionUpdateRequest,
    outcome: &str,
    readback: &str,
    write_attempted: bool,
    error: Option<&Error>,
) -> Value {
    json!({
        "operation_id":operation_id,
        "publication_operation_id":target.publication_operation_id,
        "project_id":target.project_id,
        "source_id":target.source_id,
        "task_id":target.task_id,
        "attempt_id":target.attempt_id,
        "repository_id":target.repository_id,
        "pull_request_id":target.pull_request_id,
        "pull_request_number":target.pull_request_number,
        "head_sha":target.head_sha,
        "target_ref":target.target_ref,
        "base_ref":target.base_ref,
        "requested_title":request.title,
        "requested_body_digest":model::digest(request.body.as_bytes()),
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
                    "GITHUB_PR_OPERATION_MISMATCH",
                    "the retained Operation does not belong to the current caller",
                ));
            }
            let allowed_from = match state {
                "settled" => "'queued','sending','outcome_unknown'",
                "rejected" => "'queued'",
                "outcome_unknown" => "'sending','outcome_unknown'",
                _ => return Err(Error::new("INTERNAL", "invalid PR effect state")),
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
                        "the PR description Operation changed before its outcome was recorded",
                    ));
                }
            }
            capacity::sync_operation(&tx, &operation_id, now)?;
            if state == "outcome_unknown" {
                super::record_operation_failure_event(&tx, &operation_id, state, now)?;
            }
            if state != "outcome_unknown" {
                let event_key = format!("{state}:{operation_id}");
                tx.execute(
                    "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('github:pull-request',?1,?2,'github.pull_request.update_description',?3,?4)",
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
        error["code"].as_str().unwrap_or("GITHUB_PR_UPDATE_FAILED"),
        error["message"]
            .as_str()
            .unwrap_or("the retained PR description Operation was rejected before write"),
    )
}

fn reconciliation_error(result: &Value) -> Error {
    let error = result.get("error").unwrap_or(result);
    Error::new(
        error["code"]
            .as_str()
            .unwrap_or("GITHUB_PR_RECONCILE_FAILED"),
        error["message"]
            .as_str()
            .unwrap_or("the retained PR readback Operation was rejected"),
    )
}
