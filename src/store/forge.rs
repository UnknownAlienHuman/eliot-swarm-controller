//! Exact accepted-candidate publication through the configured native Git client.
//! Admission is durable before a worker can run. Reconciliation only reads the
//! exact remote ref; it never replays a push whose outcome is unknown.
use super::{current_principal, gm, meta, operations, results, submissions, tasks};
use crate::{
    artifacts::ArtifactRecord,
    checks::source,
    config::Config,
    error::{Error, Result},
    forge::{
        ForgeConfig, ForgeProject, PublicationIntent, PublishRefRequest, RefReadback, run_git,
    },
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{Arc, OnceLock},
};

static FORGE_WRITE_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
static FORGE_PROCESS_SLOT: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();

fn process_slot() -> Arc<tokio::sync::Semaphore> {
    FORGE_PROCESS_SLOT
        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
        .clone()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkMode {
    PushOnce,
    ReadbackOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DispatchAuthorization {
    Authorized,
    StaleGmEpoch {
        admitted_gm_epoch: i64,
        current_gm_epoch: i64,
    },
}

#[derive(Debug, Clone)]
struct ForgeWork {
    intent: PublicationIntent,
    project: ForgeProject,
    candidate: Option<ArtifactRecord>,
    mode: WorkMode,
}

#[derive(Debug, Clone)]
enum ForgeOutcome {
    Applied {
        readback: RefReadback,
        push_exit_code: Option<i32>,
        timed_out: bool,
        stderr_digest: Option<String>,
        stderr_bytes: Option<u64>,
    },
    Failed(Error),
    StaleGmEpoch {
        admitted_gm_epoch: i64,
        current_gm_epoch: i64,
    },
    Unknown {
        reason: &'static str,
        readback: Option<RefReadback>,
        timed_out: bool,
        stderr_digest: Option<String>,
        stderr_bytes: Option<u64>,
        process_tree_unconfirmed: bool,
    },
}

fn current_gm_epoch(db: &Connection) -> Result<i64> {
    match gm::record(db)? {
        None => Ok(0),
        Some(record) => model::positive(&record, "epoch"),
    }
}

fn accepted_candidate(
    db: &Connection,
    p: &Principal,
    input: &PublishRefRequest,
    config: &ForgeConfig,
) -> Result<(PublicationIntent, ForgeProject, ArtifactRecord)> {
    gm::require_authority(db, p)?;
    let admitted_gm_epoch = current_gm_epoch(db)?;
    let attempt = tasks::get_attempt(db, &input.attempt_id)?;
    let task_id = model::text(&attempt, "task_id")?;
    let task = tasks::get_task(db, task_id)?;
    if task["state"] != "accepted"
        || task["revision"] != input.expected_revision
        || attempt["task_revision"] != input.expected_revision
        || attempt["state"] != "accepted"
        || !attempt["released_at_ms"].is_null()
        || attempt["submission_ref"] != input.submission_ref
        || attempt["candidate_ref"] != input.candidate_ref
        || task["accepted_attempt_id"] != input.attempt_id
        || task["accepted_operation_id"] != input.accepted_operation_id
        || task["accepted_revision"] != input.expected_revision
        || task["accepted_candidate_ref"] != input.candidate_ref
    {
        return Err(Error::new(
            "FORGE_ACCEPTANCE_STALE",
            "publication does not target the current accepted Task candidate",
        ));
    }

    let acceptance = operations::get_operation(db, &input.accepted_operation_id)?;
    if acceptance["method"] != "task.accept"
        || acceptance["state"] != "settled"
        || acceptance["result"]["outcome"] != "applied"
        || acceptance["result"]["acceptance_operation_id"] != input.accepted_operation_id
        || acceptance["result"]["task_id"] != task_id
        || acceptance["result"]["attempt_id"] != input.attempt_id
        || acceptance["result"]["task_revision"] != input.expected_revision
        || acceptance["result"]["submission_ref"] != input.submission_ref
        || acceptance["result"]["candidate_ref"] != input.candidate_ref
    {
        return Err(Error::new(
            "FORGE_ACCEPTANCE_STALE",
            "publication acceptance receipt is not the exact applied decision",
        ));
    }

    let project_id = model::text(&task, "project_id")?;
    let project = config.project(project_id)?.clone();
    if !project
        .target_refs
        .iter()
        .any(|target| target == &input.target_ref)
        || input.expected_policy_revision != project.policy_revision
        || attempt["task_snapshot"]["spec"]["owner_policy_id"] != project.policy_revision
    {
        return Err(Error::new(
            "FORGE_POLICY_MISMATCH",
            "target ref or accepted policy is not in the trusted project mapping",
        ));
    }

    let submission = submissions::document(db, &input.submission_ref)?;
    if submission["attempt_id"] != input.attempt_id
        || submission["task_revision"] != input.expected_revision
        || submission["candidate_ref"] != input.candidate_ref
    {
        return Err(Error::new(
            "FORGE_SUBMISSION_MISMATCH",
            "submission does not identify the accepted candidate",
        ));
    }
    let candidate = results::get(db, &input.candidate_ref)?;
    if candidate.kind != "source_snapshot"
        || candidate.metadata["attempt_id"] != input.attempt_id
        || candidate.metadata["task_revision"] != input.expected_revision
        || submission["candidate_sha256"] != candidate.content_digest
    {
        return Err(Error::new(
            "FORGE_CANDIDATE_MISMATCH",
            "publication requires the complete accepted source snapshot",
        ));
    }
    let commit = model::text(&candidate.metadata, "commit")?.to_owned();
    let tree = model::text(&candidate.metadata, "tree")?.to_owned();
    let intent = PublicationIntent::from_request(
        "",
        project_id,
        &project,
        input,
        candidate.content_digest.clone(),
        (commit, tree),
        admitted_gm_epoch,
    );
    intent.validate()?;
    Ok((intent, project, candidate))
}

pub(super) fn reserve(
    tx: &Transaction<'_>,
    p: &Principal,
    value: &Value,
    id: &str,
    config: &Config,
) -> Result<Value> {
    gm::require_authority(tx, p)?;
    let input = PublishRefRequest::parse(value)?;
    let (mut intent, _project, _candidate) = accepted_candidate(tx, p, &input, &config.forge)?;
    require_no_process_tree_hold(tx, &intent.canonical_repository)?;
    intent.operation_id = id.to_owned();
    let task_id = model::text(&tasks::get_attempt(tx, &input.attempt_id)?, "task_id")?.to_owned();
    tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=?4 WHERE operation_id=?1",
        params![
            id,
            task_id,
            input.attempt_id,
            model::canonical(&json!({"publication_intent":intent}))?
        ],
    )?;
    Ok(json!({
        "operation_id":id,
        "attempt_id":input.attempt_id,
        "state":"queued",
        "publication":"not_started",
        "force":false
    }))
}

fn unresolved_process_tree_hold(
    db: &Connection,
    canonical_repository: &str,
) -> Result<Option<String>> {
    db.query_row(
        "SELECT operation_id FROM operations \
         WHERE method='forge.publish_ref' AND state='outcome_unknown' \
           AND json_extract(effective_request_json,'$.publication_intent.canonical_repository')=?1 \
           AND json_extract(result_json,'$.process_tree_unconfirmed')=1 \
         ORDER BY created_at_ms,operation_id LIMIT 1",
        [canonical_repository],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

fn require_no_process_tree_hold(db: &Connection, canonical_repository: &str) -> Result<()> {
    if let Some(operation_id) = unresolved_process_tree_hold(db, canonical_repository)? {
        return Err(Error::new(
            "FORGE_PROCESS_TREE_UNCONFIRMED",
            format!(
                "publication {operation_id} has an unconfirmed native process tree; no further Forge push to this repository is permitted until manual operator intervention"
            ),
        ));
    }
    Ok(())
}

fn saved_intent(db: &Connection, id: &str) -> Result<PublicationIntent> {
    let raw: String = db.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1",
        [id],
        |row| row.get(0),
    )?;
    let value: Value = serde_json::from_str(&raw)?;
    let intent: PublicationIntent = serde_json::from_value(value["publication_intent"].clone())?;
    intent.validate()?;
    let operation = operations::get_operation(db, id)?;
    if intent.operation_id != id
        || operation["method"] != "forge.publish_ref"
        || operation["attempt_id"] != intent.attempt_id
    {
        return Err(Error::new(
            "FORGE_INTENT_INVALID",
            "saved publication intent does not match its Operation",
        ));
    }
    Ok(intent)
}

fn request(db: &Connection, id: &str) -> Result<PublishRefRequest> {
    let raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [id],
        |row| row.get(0),
    )?;
    PublishRefRequest::parse(&serde_json::from_str(&raw)?)
}

fn settle_before_write(tx: &Transaction<'_>, id: &str, error: Error) -> Result<()> {
    let now = model::now_ms()?;
    let result = json!({
        "operation_id":id,
        "outcome":"failed",
        "publication":"not_started",
        "force":false,
        "error":error
    });
    tx.execute(
        "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1",
        params![id, model::canonical(&result)?, now],
    )?;
    super::capacity::sync_operation(tx, id, now)?;
    tx.execute(
        "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:forge',?1,?1,'forge.publication',?2,?3)",
        params![format!("failed:{id}"), model::canonical(&result)?, now],
    )?;
    Ok(())
}

fn stale_gm_epoch_value(id: &str, admitted_gm_epoch: i64, current_gm_epoch: i64) -> Value {
    json!({
        "operation_id":id,
        "outcome":"stale_gm_epoch",
        "publication":"not_started",
        "admitted_gm_epoch":admitted_gm_epoch,
        "current_gm_epoch":current_gm_epoch,
        "force":false
    })
}

fn settle_stale_gm_epoch(
    tx: &Transaction<'_>,
    id: &str,
    admitted_gm_epoch: i64,
    current_gm_epoch: i64,
) -> Result<()> {
    let now = model::now_ms()?;
    let result = stale_gm_epoch_value(id, admitted_gm_epoch, current_gm_epoch);
    tx.execute(
        "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND state='queued'",
        params![id, model::canonical(&result)?, now],
    )?;
    super::capacity::sync_operation(tx, id, now)?;
    tx.execute(
        "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:forge',?1,?2,'forge.publication',?3,?4)",
        params![format!("stale-gm-epoch:{id}:{admitted_gm_epoch}:{current_gm_epoch}"), id, model::canonical(&result)?, now],
    )?;
    Ok(())
}

fn work_from_saved(
    db: &Connection,
    intent: PublicationIntent,
    config: &ForgeConfig,
    mode: WorkMode,
) -> Result<ForgeWork> {
    let project = config.project(&intent.project_id)?.clone();
    if crate::forge::canonical_repository(&project.canonical_repository)?
        != intent.canonical_repository
        || project.remote_name != intent.remote_name
        || project.policy_revision != intent.policy_revision
        || !project
            .target_refs
            .iter()
            .any(|target| target == &intent.target_ref)
    {
        return Err(Error::new(
            "FORGE_CONFIG_CHANGED",
            "saved publication intent differs from the trusted local project mapping",
        ));
    }
    let candidate = if mode == WorkMode::PushOnce {
        Some(results::get(db, &intent.candidate_ref)?)
    } else {
        None
    };
    Ok(ForgeWork {
        intent,
        project,
        candidate,
        mode,
    })
}

fn begin(
    db: &mut Connection,
    p: Principal,
    id: &str,
    config: &ForgeConfig,
) -> Result<Option<ForgeWork>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let operation = operations::get_operation(&tx, id)?;
    if operation["method"] != "forge.publish_ref" || operation["caller_id"] != p.client_id {
        return Err(Error::new(
            "FORBIDDEN",
            "publication belongs to another caller or method",
        ));
    }
    match operation["state"].as_str() {
        Some("queued") => {
            let saved = saved_intent(&tx, id)?;
            let current_epoch = current_gm_epoch(&tx)?;
            if saved.admitted_gm_epoch != current_epoch {
                settle_stale_gm_epoch(&tx, id, saved.admitted_gm_epoch, current_epoch)?;
                tx.commit()?;
                return Ok(None);
            }
            if unresolved_process_tree_hold(&tx, &saved.canonical_repository)?.is_some() {
                // Preserve this never-sent request as queued. The hold is
                // surfaced by the unresolved Operation and new admissions are
                // rejected with its identity; no external effect is started.
                return Ok(None);
            }
            if meta(&tx, "execution_mode")?.unwrap_or(Value::Null)["new_work"] != "enabled" {
                // Keep durable, never-sent work queued while the host drains.
                return Ok(None);
            }
            let start = (|| -> Result<ForgeWork> {
                let current = current_principal(&tx, p.clone())?;
                if !matches!(current.role, Role::Operator | Role::Manager) {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "publication requires the operator or current GM",
                    ));
                }
                gm::require_authority(&tx, &current)?;
                let input = request(&tx, id)?;
                let (mut expected, _, _) = accepted_candidate(&tx, &current, &input, config)?;
                expected.operation_id = id.to_owned();
                if expected != saved {
                    return Err(Error::new(
                        "FORGE_INTENT_CHANGED",
                        "current accepted candidate differs from saved publication intent",
                    ));
                }
                work_from_saved(&tx, saved, config, WorkMode::PushOnce)
            })();
            match start {
                Ok(work) => {
                    let now = model::now_ms()?;
                    tx.execute(
                        "UPDATE operations SET state='sending',result_json=json_set(COALESCE(result_json,'{}'),'$.process_tree_unconfirmed',json('false'),'$.process_tree_status','in_flight','$.publication_may_have_started',json('false')),sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1 AND state='queued'",
                        params![id, now],
                    )?;
                    tx.commit()?;
                    Ok(Some(work))
                }
                Err(error) => {
                    settle_before_write(&tx, id, error)?;
                    tx.commit()?;
                    Ok(None)
                }
            }
        }
        Some("outcome_unknown") => {
            let work =
                work_from_saved(&tx, saved_intent(&tx, id)?, config, WorkMode::ReadbackOnly)?;
            tx.commit()?;
            Ok(Some(work))
        }
        _ => Ok(None),
    }
}

/// Convert an abandoned in-process send to unknown only after the caller has
/// acquired the Git process slot, proving its owned command closure has ended.
/// This path is strictly readback-only.
fn begin_reconciliation(
    db: &mut Connection,
    p: Principal,
    id: &str,
    config: &ForgeConfig,
) -> Result<Option<ForgeWork>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let operation = operations::get_operation(&tx, id)?;
    if operation["method"] != "forge.publish_ref" || operation["caller_id"] != p.client_id {
        return Err(Error::new(
            "FORBIDDEN",
            "publication belongs to another caller or method",
        ));
    }
    match operation["state"].as_str() {
        Some("sending") => {
            let now = model::now_ms()?;
            tx.execute(
                "UPDATE operations SET state='outcome_unknown',result_json=json_set(COALESCE(result_json,'{}'),'$.process_tree_unconfirmed',json('true'),'$.process_tree_status','unconfirmed_after_restart','$.process_tree_cleanup','host_lifecycle_interrupted_before_confirmation','$.resolution','manual_operator_intervention_required','$.reason','process_tree_unconfirmed'),updated_at_ms=?2 WHERE operation_id=?1 AND state='sending'",
                params![id, now],
            )?;
        }
        Some("outcome_unknown") => {}
        _ => return Ok(None),
    }
    let work = work_from_saved(&tx, saved_intent(&tx, id)?, config, WorkMode::ReadbackOnly)?;
    tx.commit()?;
    Ok(Some(work))
}

fn dispatch_authorized(
    db: &Connection,
    p: Principal,
    work: &ForgeWork,
    config: &ForgeConfig,
) -> Result<DispatchAuthorization> {
    let operation = operations::get_operation(db, &work.intent.operation_id)?;
    if operation["method"] != "forge.publish_ref"
        || operation["state"] != "sending"
        || operation["caller_id"] != p.client_id
    {
        return Err(Error::conflict(
            "publication is no longer in its pre-write phase",
        ));
    }
    let saved = saved_intent(db, &work.intent.operation_id)?;
    if saved != work.intent {
        return Err(Error::new(
            "FORGE_INTENT_CHANGED",
            "saved publication intent differs from its authorized work item",
        ));
    }
    require_no_process_tree_hold(db, &saved.canonical_repository)?;
    let admitted_gm_epoch = saved.admitted_gm_epoch;
    let current_epoch = current_gm_epoch(db)?;
    if admitted_gm_epoch != current_epoch {
        return Ok(DispatchAuthorization::StaleGmEpoch {
            admitted_gm_epoch,
            current_gm_epoch: current_epoch,
        });
    }
    let current = current_principal(db, p)?;
    if !matches!(current.role, Role::Operator | Role::Manager) {
        return Err(Error::new(
            "FORBIDDEN",
            "publication requires the operator or current GM",
        ));
    }
    gm::require_authority(db, &current)?;
    let input = request(db, &work.intent.operation_id)?;
    let (expected, project, candidate) = accepted_candidate(db, &current, &input, config)?;
    let mut expected = expected;
    expected.operation_id = work.intent.operation_id.clone();
    if expected != work.intent
        || project != work.project
        || candidate.content_digest != work.intent.candidate_sha256
    {
        return Err(Error::new(
            "FORGE_ACCEPTANCE_STALE",
            "accepted candidate or project mapping changed before publication",
        ));
    }
    let now = model::now_ms()?;
    let changed = db.execute(
        "UPDATE operations SET result_json=json_set(COALESCE(result_json,'{}'),'$.publication_may_have_started',json('true')),updated_at_ms=?2 WHERE operation_id=?1 AND method='forge.publish_ref' AND state='sending'",
        params![&work.intent.operation_id, now],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "publication left its pre-write phase before the write boundary",
        ));
    }
    Ok(DispatchAuthorization::Authorized)
}

fn runner_error_outcome(error: Error) -> ForgeOutcome {
    if error.code == "FORGE_GIT_TREE_TERMINATION" {
        ForgeOutcome::Unknown {
            reason: "git_process_tree_unconfirmed",
            readback: None,
            timed_out: false,
            stderr_digest: None,
            stderr_bytes: None,
            process_tree_unconfirmed: true,
        }
    } else {
        ForgeOutcome::Failed(error)
    }
}

fn successful(output: &crate::forge::GitOutput) -> bool {
    !output.timed_out && output.status.success() && !output.stdout_truncated
}

fn stdout_text(output: &crate::forge::GitOutput) -> Result<String> {
    if output.timed_out || output.stdout_truncated {
        return Err(Error::new(
            "FORGE_GIT_OUTPUT",
            "Git command timed out or exceeded the bounded output size",
        ));
    }
    String::from_utf8(output.stdout.clone())
        .map_err(|_| Error::new("FORGE_GIT_OUTPUT", "Git returned non-UTF-8 identity output"))
}

fn require_success(output: &crate::forge::GitOutput, message: &'static str) -> Result<()> {
    if successful(output) {
        Ok(())
    } else {
        Err(Error::new("FORGE_GIT_REJECTED", message))
    }
}

fn command_text(config: &ForgeConfig, project: &ForgeProject, args: &[&str]) -> Result<String> {
    let owned: Vec<String> = args.iter().map(|value| (*value).to_owned()).collect();
    let output = run_git(config, project, &owned)?;
    require_success(&output, "configured Git identity check failed")?;
    stdout_text(&output)
}

fn validate_local_repository(
    config: &ForgeConfig,
    project: &ForgeProject,
    intent: &PublicationIntent,
) -> Result<()> {
    let top = command_text(config, project, &["rev-parse", "--show-toplevel"])?;
    let top = Path::new(top.trim()).canonicalize().map_err(|_| {
        Error::new(
            "FORGE_REPOSITORY",
            "configured Git root could not be resolved",
        )
    })?;
    if !same_path(&top, &project.repository_path) {
        return Err(Error::new(
            "FORGE_REPOSITORY",
            "configured path is not the trusted repository root",
        ));
    }
    let commit_expr = format!("{}^{{commit}}", intent.commit);
    let commit = command_text(
        config,
        project,
        &["rev-parse", "--verify", "--end-of-options", &commit_expr],
    )?;
    if !commit.trim().eq_ignore_ascii_case(&intent.commit) {
        return Err(Error::new(
            "FORGE_COMMIT_MISMATCH",
            "trusted repository does not contain the accepted exact commit",
        ));
    }
    let tree_expr = format!("{}^{{tree}}", intent.commit);
    let tree = command_text(
        config,
        project,
        &["rev-parse", "--verify", "--end-of-options", &tree_expr],
    )?;
    if !tree.trim().eq_ignore_ascii_case(&intent.tree) {
        return Err(Error::new(
            "FORGE_TREE_MISMATCH",
            "accepted commit tree differs from captured source identity",
        ));
    }
    let output = run_git(
        config,
        project,
        &["check-ref-format".into(), intent.target_ref.clone()],
    )?;
    require_success(&output, "target ref failed native Git validation")?;
    if let Some(old) = intent.expected_old_ref.as_deref() {
        let ancestor = run_git(
            config,
            project,
            &[
                "merge-base".into(),
                "--is-ancestor".into(),
                old.to_owned(),
                intent.commit.clone(),
            ],
        )?;
        require_success(
            &ancestor,
            "accepted commit is not a fast-forward from the expected old ref",
        )?;
    }
    Ok(())
}

fn same_path(left: &Path, right: &Path) -> bool {
    #[cfg(windows)]
    {
        left.to_string_lossy()
            .eq_ignore_ascii_case(&right.to_string_lossy())
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

fn validate_remote(
    config: &ForgeConfig,
    project: &ForgeProject,
    intent: &PublicationIntent,
) -> Result<String> {
    let urls = command_text(
        config,
        project,
        &["remote", "get-url", "--push", "--all", &intent.remote_name],
    )?;
    let urls: Vec<_> = urls
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    if urls.len() != 1
        || crate::forge::repository_from_remote_url(urls[0])? != intent.canonical_repository
    {
        return Err(Error::new(
            "FORGE_REMOTE_MISMATCH",
            "configured push remote does not identify the trusted canonical repository",
        ));
    }
    let push_url = urls[0].to_owned();
    let mirror_key = format!("remote.{}.mirror", intent.remote_name);
    if config_lines(config, project, &mirror_key)?
        .iter()
        .any(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "true" | "yes" | "on" | "1"
            )
        })
    {
        return Err(Error::new(
            "FORGE_REMOTE_UNSAFE",
            "mirror remotes cannot be used for exact ref publication",
        ));
    }
    for key in ["push.pushOption", "core.sshCommand"] {
        if !config_lines(config, project, key)?.is_empty() {
            return Err(Error::new(
                "FORGE_REMOTE_UNSAFE",
                "local push options or custom SSH command are not supported",
            ));
        }
    }
    for key in [
        format!("remote.{}.vcs", intent.remote_name),
        format!("remote.{}.uploadpack", intent.remote_name),
        format!("remote.{}.receivepack", intent.remote_name),
    ] {
        if !config_lines(config, project, &key)?.is_empty() {
            return Err(Error::new(
                "FORGE_REMOTE_UNSAFE",
                "configured remote helpers or custom server commands are not supported",
            ));
        }
    }
    Ok(push_url)
}

fn config_lines(config: &ForgeConfig, project: &ForgeProject, key: &str) -> Result<Vec<String>> {
    let args = ["config", "--get-all", key];
    let output = run_git(config, project, &args.map(str::to_owned))?;
    if output.status.success() && !output.timed_out && !output.stdout_truncated {
        let text = stdout_text(&output)?;
        Ok(text.lines().map(str::to_owned).collect())
    } else if !output.status.success() && !output.timed_out && output.stdout.is_empty() {
        // `git config --get-all` exits 1 when a key is unset.
        Ok(Vec::new())
    } else {
        Err(Error::new(
            "FORGE_GIT_CONFIG",
            "trusted Git push configuration could not be inspected",
        ))
    }
}

fn remote_ref(
    config: &ForgeConfig,
    project: &ForgeProject,
    intent: &PublicationIntent,
    push_url: &str,
) -> Result<RefReadback> {
    if crate::forge::repository_from_remote_url(push_url)? != intent.canonical_repository {
        return Err(Error::new(
            "FORGE_REMOTE_MISMATCH",
            "readback endpoint does not identify the trusted canonical repository",
        ));
    }
    remote_ref_at(config, project, intent, push_url)
}

/// Query an already selected endpoint. Production callers pass only the
/// credential-free push URL returned by `validate_remote`; keeping argv
/// construction separate also lets the local bare-repository test verify that
/// a remote alias's fetch URL is never substituted for its push URL.
fn remote_ref_at(
    config: &ForgeConfig,
    project: &ForgeProject,
    intent: &PublicationIntent,
    endpoint: &str,
) -> Result<RefReadback> {
    let args = [
        "ls-remote".to_owned(),
        "--refs".to_owned(),
        "--upload-pack=git-upload-pack".to_owned(),
        endpoint.to_owned(),
        intent.target_ref.clone(),
    ];
    let output = run_git(config, project, &args)?;
    require_success(&output, "exact remote ref readback failed")?;
    let text = stdout_text(&output)?;
    let lines: Vec<_> = text.lines().filter(|line| !line.is_empty()).collect();
    if lines.is_empty() {
        return Ok(RefReadback::Missing);
    }
    if lines.len() != 1 {
        return Err(Error::new(
            "FORGE_READBACK_INVALID",
            "exact remote query returned multiple refs",
        ));
    }
    let (oid, name) = lines[0].split_once('\t').ok_or_else(|| {
        Error::new(
            "FORGE_READBACK_INVALID",
            "remote ref response was malformed",
        )
    })?;
    if name != intent.target_ref || !crate::forge::valid_object_id(oid) {
        return Err(Error::new(
            "FORGE_READBACK_INVALID",
            "remote ref response did not match the requested full ref",
        ));
    }
    Ok(RefReadback::At(oid.to_ascii_lowercase()))
}

fn prepare_candidate(
    files: &crate::artifacts::ArtifactFiles,
    config: &ForgeConfig,
    work: &ForgeWork,
) -> Result<()> {
    let candidate = work
        .candidate
        .as_ref()
        .ok_or_else(|| Error::new("FORGE_CANDIDATE_MISSING", "candidate record is absent"))?;
    files.verify(candidate)?;
    let manifest = source::manifest(files, candidate)?;
    if manifest.commit != work.intent.commit
        || manifest.tree != work.intent.tree
        || candidate.content_digest != work.intent.candidate_sha256
    {
        return Err(Error::new(
            "FORGE_CANDIDATE_MISMATCH",
            "retained source manifest differs from publication intent",
        ));
    }
    validate_local_repository(config, &work.project, &work.intent)
}

fn prepare_push(config: &ForgeConfig, work: &ForgeWork) -> Result<String> {
    let push_url = validate_remote(config, &work.project, &work.intent)?;
    let before = remote_ref(config, &work.project, &work.intent, &push_url)?;
    if !before.matches_expected(&work.intent) {
        return Err(Error::new(
            "FORGE_EXPECTED_REF_MISMATCH",
            "remote ref no longer matches the expected old value or explicit create",
        ));
    }
    // Fail closed if the configured endpoint changed while preflight was in
    // flight. The exact credential-free URL validated above is also carried
    // in memory to the push worker; it is never persisted in the Operation.
    let current_push_url = validate_remote(config, &work.project, &work.intent)?;
    if current_push_url != push_url {
        return Err(Error::new(
            "FORGE_REMOTE_CHANGED",
            "configured push endpoint changed during remote preflight",
        ));
    }
    Ok(push_url)
}

fn execute_push(config: &ForgeConfig, work: &ForgeWork, push_url: &str) -> ForgeOutcome {
    let refspec = format!("{}:{}", work.intent.commit, work.intent.target_ref);
    let args = [
        "push".to_owned(),
        "--porcelain".to_owned(),
        "--no-verify".to_owned(),
        "--no-follow-tags".to_owned(),
        "--recurse-submodules=no".to_owned(),
        "--receive-pack=git-receive-pack".to_owned(),
        push_url.to_owned(),
        refspec,
    ];
    let push = run_git(config, &work.project, &args);
    let process_tree_unconfirmed =
        matches!(&push, Err(error) if error.code == "FORGE_GIT_TREE_TERMINATION");
    let (exit_code, timed_out, stderr_digest, stderr_bytes) = match push.as_ref() {
        Ok(output) => (
            output.status.code(),
            output.timed_out,
            Some(output.stderr_digest.clone()),
            Some(output.stderr_bytes),
        ),
        // A runner error does not prove that the native command itself timed
        // out. Preserve the distinct process-tree diagnosis below instead.
        Err(_) => (None, false, None, None),
    };
    if process_tree_unconfirmed {
        // Do not read back and call this Applied: the child tree may still be
        // capable of finishing the push after the bounded cleanup deadline.
        return ForgeOutcome::Unknown {
            reason: "git_process_tree_unconfirmed",
            readback: None,
            timed_out,
            stderr_digest,
            stderr_bytes,
            process_tree_unconfirmed: true,
        };
    }
    let readback = match remote_ref(config, &work.project, &work.intent, push_url) {
        Ok(value) => value,
        Err(error) => {
            let process_tree_unconfirmed = error.code == "FORGE_GIT_TREE_TERMINATION";
            return ForgeOutcome::Unknown {
                reason: if process_tree_unconfirmed {
                    "git_process_tree_unconfirmed"
                } else {
                    "readback_failed_after_push"
                },
                readback: None,
                timed_out,
                stderr_digest,
                stderr_bytes,
                process_tree_unconfirmed,
            };
        }
    };
    if readback.matches_intent(&work.intent) {
        return ForgeOutcome::Applied {
            readback,
            push_exit_code: exit_code,
            timed_out,
            stderr_digest,
            stderr_bytes,
        };
    }
    let definite_rejection = push
        .as_ref()
        .is_ok_and(|output| !output.timed_out && !output.status.success());
    if definite_rejection && readback.matches_expected(&work.intent) {
        return ForgeOutcome::Failed(Error::new(
            "FORGE_PUSH_REJECTED",
            "native Git rejected publication and exact remote ref remains unchanged",
        ));
    }
    ForgeOutcome::Unknown {
        reason: "push_outcome_not_confirmed_by_exact_readback",
        readback: Some(readback),
        timed_out,
        stderr_digest,
        stderr_bytes,
        process_tree_unconfirmed: false,
    }
}

fn execute_readback(config: &ForgeConfig, work: &ForgeWork) -> ForgeOutcome {
    let push_url = match validate_remote(config, &work.project, &work.intent) {
        Ok(push_url) => push_url,
        Err(error) => {
            let process_tree_unconfirmed = error.code == "FORGE_GIT_TREE_TERMINATION";
            return ForgeOutcome::Unknown {
                reason: if process_tree_unconfirmed {
                    "git_process_tree_unconfirmed"
                } else {
                    "trusted_remote_unavailable_for_reconciliation"
                },
                readback: None,
                timed_out: false,
                stderr_digest: None,
                stderr_bytes: None,
                process_tree_unconfirmed,
            };
        }
    };
    match remote_ref(config, &work.project, &work.intent, &push_url) {
        Ok(readback) => restart_readback_outcome(&work.intent, readback),
        Err(error) => ForgeOutcome::Unknown {
            reason: if error.code == "FORGE_GIT_TREE_TERMINATION" {
                "git_process_tree_unconfirmed"
            } else {
                "restart_readback_failed"
            },
            readback: None,
            timed_out: false,
            stderr_digest: None,
            stderr_bytes: None,
            process_tree_unconfirmed: error.code == "FORGE_GIT_TREE_TERMINATION",
        },
    }
}

fn restart_readback_outcome(intent: &PublicationIntent, readback: RefReadback) -> ForgeOutcome {
    if readback.matches_intent(intent) {
        ForgeOutcome::Applied {
            readback,
            push_exit_code: None,
            timed_out: false,
            stderr_digest: None,
            stderr_bytes: None,
        }
    } else {
        ForgeOutcome::Unknown {
            reason: "restart_readback_did_not_prove_publication",
            readback: Some(readback),
            timed_out: false,
            stderr_digest: None,
            stderr_bytes: None,
            process_tree_unconfirmed: false,
        }
    }
}

fn outcome_value(id: &str, outcome: &ForgeOutcome) -> (Value, &'static str) {
    match outcome {
        ForgeOutcome::Applied {
            readback,
            push_exit_code,
            timed_out,
            stderr_digest,
            stderr_bytes,
        } => (
            json!({
                "operation_id":id,
                "outcome":"applied",
                "publication":"confirmed_by_remote_readback",
                "remote_ref":readback.value(),
                "push_exit_code":push_exit_code,
                "timed_out":timed_out,
                "stderr_prefix_sha256":stderr_digest,
                "stderr_bytes":stderr_bytes,
                "force":false
            }),
            "settled",
        ),
        ForgeOutcome::Failed(error) => (
            json!({
                "operation_id":id,
                "outcome":"failed",
                "publication":"not_confirmed",
                "error":error,
                "force":false
            }),
            "settled",
        ),
        ForgeOutcome::StaleGmEpoch {
            admitted_gm_epoch,
            current_gm_epoch,
        } => (
            stale_gm_epoch_value(id, *admitted_gm_epoch, *current_gm_epoch),
            "settled",
        ),
        ForgeOutcome::Unknown {
            reason,
            readback,
            timed_out,
            stderr_digest,
            stderr_bytes,
            process_tree_unconfirmed,
        } => {
            let mut result = json!({
                "operation_id":id,
                "outcome":"unknown",
                "publication":if *process_tree_unconfirmed { "operator_intervention_required" } else { "requires_readback" },
                "reason":reason,
                "remote_ref":readback.as_ref().map(RefReadback::value),
                "timed_out":timed_out,
                "stderr_prefix_sha256":stderr_digest,
                "stderr_bytes":stderr_bytes,
                "process_tree_unconfirmed":process_tree_unconfirmed,
                "force":false
            });
            if *process_tree_unconfirmed {
                result["process_tree_status"] = json!("unconfirmed");
                result["process_tree_cleanup"] = json!(reason);
                result["resolution"] = json!("manual_operator_intervention_required");
            }
            (result, "outcome_unknown")
        }
    }
}

fn preserve_process_tree_hold(
    previous_result: &Value,
    result: &mut Value,
    state: &mut &'static str,
) {
    if previous_result["process_tree_unconfirmed"] != true {
        return;
    }
    if *state == "settled" {
        *state = "outcome_unknown";
        result["outcome"] = json!("unknown");
    }
    if result["reason"] != "process_tree_unconfirmed" && !result["reason"].is_null() {
        result["last_readback_reason"] = result["reason"].clone();
    }
    result["reason"] = json!("process_tree_unconfirmed");
    result["publication"] = json!("operator_intervention_required");
    result["process_tree_unconfirmed"] = json!(true);
    result["process_tree_status"] = previous_result
        .get("process_tree_status")
        .cloned()
        .unwrap_or_else(|| json!("unconfirmed"));
    result["process_tree_cleanup"] = previous_result
        .get("process_tree_cleanup")
        .cloned()
        .unwrap_or_else(|| json!("process_tree_unconfirmed"));
    result["resolution"] = json!("manual_operator_intervention_required");
}

fn publication_may_have_started(result: &Value) -> bool {
    result
        .get("publication_may_have_started")
        .and_then(Value::as_bool)
        .unwrap_or(true)
}

fn finish(db: &mut Connection, id: &str, outcome: ForgeOutcome) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let operation = operations::get_operation(&tx, id)?;
    if operation["method"] != "forge.publish_ref"
        || !matches!(
            operation["state"].as_str(),
            Some("sending" | "outcome_unknown")
        )
    {
        return Err(Error::conflict("publication is no longer reconcilable"));
    }
    let (mut result, mut state) = outcome_value(id, &outcome);
    result["publication_may_have_started"] =
        json!(publication_may_have_started(&operation["result"]));
    preserve_process_tree_hold(&operation["result"], &mut result, &mut state);
    if state == "settled" {
        let intent = saved_intent(&tx, id)?;
        let task = tasks::get_task(&tx, &intent_attempt_task(&tx, &intent)?)?;
        result["acceptance_current_at_finish"] = json!(
            task["accepted_operation_id"] == intent.accepted_operation_id
                && task["accepted_candidate_ref"] == intent.candidate_ref
        );
    }
    let now = model::now_ms()?;
    tx.execute(
        "UPDATE operations SET state=?2,result_json=?3,settled_at_ms=?4,updated_at_ms=?5 WHERE operation_id=?1",
        params![
            id,
            state,
            model::canonical(&result)?,
            if state == "settled" { Some(now) } else { None },
            now
        ],
    )?;
    super::capacity::sync_operation(&tx, id, now)?;
    let event_key = if state == "outcome_unknown" {
        format!(
            "unknown:{id}:{}",
            result["reason"].as_str().unwrap_or("unknown")
        )
    } else {
        format!("finished:{id}")
    };
    tx.execute(
        "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:forge',?1,?2,'forge.publication',?3,?4)",
        params![event_key,id,model::canonical(&result)?,now],
    )?;
    tx.commit()?;
    Ok(())
}

fn intent_attempt_task(db: &Connection, intent: &PublicationIntent) -> Result<String> {
    let attempt = tasks::get_attempt(db, &intent.attempt_id)?;
    Ok(model::text(&attempt, "task_id")?.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn reconciliation_config() -> ForgeConfig {
        let mut config = ForgeConfig::default();
        config.enabled = true;
        config.git_executable = std::env::current_exe().unwrap();
        config.projects.insert(
            "project-1".into(),
            ForgeProject {
                canonical_repository: "github.com/owner/repo".into(),
                repository_path: std::env::temp_dir(),
                remote_name: "origin".into(),
                policy_revision: "owner-policy-v1".into(),
                target_refs: vec!["refs/heads/main".into()],
            },
        );
        config
    }

    fn checked_fixture_git(command: &mut Command) -> Vec<u8> {
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "fixture Git command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }

    fn fixture_git_executable() -> std::path::PathBuf {
        let name = if cfg!(windows) { "git.exe" } else { "git" };
        std::env::split_paths(&std::env::var_os("PATH").expect("PATH is set"))
            .map(|directory| directory.join(name))
            .find(|candidate| candidate.is_file())
            .and_then(|candidate| candidate.canonicalize().ok())
            .expect("native Git is available for the bare repository fixture")
    }

    fn fixture_git_in(git: &Path, directory: &Path, args: &[&str]) -> Vec<u8> {
        let mut command = Command::new(git);
        command.arg("-C").arg(directory).args(args);
        checked_fixture_git(&mut command)
    }

    fn fixture_fetch_ref(git: &Path, bare: &Path, source: &Path, source_ref: &str) {
        let refspec = format!("{source_ref}:refs/heads/main");
        let mut command = Command::new(git);
        command
            .arg("--git-dir")
            .arg(bare)
            .args(["fetch", "--quiet"])
            .arg(source)
            .arg(refspec);
        checked_fixture_git(&mut command);
    }

    #[test]
    fn exact_push_endpoint_controls_expected_old_and_readback_with_split_remote_urls() {
        let directory = std::env::temp_dir().join(format!("swarm-forge-split-{}", model::new_id()));
        std::fs::create_dir_all(&directory).unwrap();
        let git = fixture_git_executable();
        let checkout = directory.join("checkout");
        let fetch_repository = directory.join("fetch.git");
        let push_repository = directory.join("push.git");

        let mut command = Command::new(&git);
        command.arg("init").arg("--quiet").arg(&checkout);
        checked_fixture_git(&mut command);
        for (message, branch_name) in [
            ("fetch-base", "fetch-base"),
            ("push-base", "push-base"),
            ("candidate", "candidate"),
        ] {
            fixture_git_in(
                &git,
                &checkout,
                &[
                    "-c",
                    "user.name=Forge Fixture",
                    "-c",
                    "user.email=forge-fixture@example.invalid",
                    "-c",
                    "commit.gpgsign=false",
                    "commit",
                    "--quiet",
                    "--allow-empty",
                    "-m",
                    message,
                ],
            );
            fixture_git_in(&git, &checkout, &["branch", branch_name]);
        }
        let oid = |reference: &str| {
            String::from_utf8(fixture_git_in(
                &git,
                &checkout,
                &["rev-parse", "--verify", reference],
            ))
            .unwrap()
            .trim()
            .to_owned()
        };
        let fetch_base = oid("refs/heads/fetch-base");
        let push_base = oid("refs/heads/push-base");
        let candidate = oid("refs/heads/candidate");

        for bare in [&fetch_repository, &push_repository] {
            let mut command = Command::new(&git);
            command.arg("init").arg("--bare").arg("--quiet").arg(bare);
            checked_fixture_git(&mut command);
        }
        fixture_fetch_ref(&git, &fetch_repository, &checkout, "refs/heads/fetch-base");
        fixture_fetch_ref(&git, &push_repository, &checkout, "refs/heads/push-base");

        let mut command = Command::new(&git);
        command
            .arg("-C")
            .arg(&checkout)
            .arg("remote")
            .arg("add")
            .arg("origin")
            .arg(&fetch_repository);
        checked_fixture_git(&mut command);
        let mut command = Command::new(&git);
        command
            .arg("-C")
            .arg(&checkout)
            .args(["remote", "set-url", "--push", "origin"])
            .arg(&push_repository);
        checked_fixture_git(&mut command);
        let fetch_url = String::from_utf8(fixture_git_in(
            &git,
            &checkout,
            &["remote", "get-url", "--all", "origin"],
        ))
        .unwrap()
        .trim()
        .to_owned();
        let push_url = String::from_utf8(fixture_git_in(
            &git,
            &checkout,
            &["remote", "get-url", "--push", "--all", "origin"],
        ))
        .unwrap()
        .trim()
        .to_owned();

        let project = ForgeProject {
            canonical_repository: "github.com/owner/repo".into(),
            repository_path: std::fs::canonicalize(&checkout).unwrap(),
            remote_name: "origin".into(),
            policy_revision: "owner-policy-v1".into(),
            target_refs: vec!["refs/heads/main".into()],
        };
        let mut config = ForgeConfig::default();
        config.enabled = true;
        config.git_executable = git.clone();
        config.projects.insert("project-1".into(), project.clone());
        let intent = PublicationIntent {
            operation_id: "forge-split-url-test".into(),
            project_id: "project-1".into(),
            canonical_repository: "github.com/owner/repo".into(),
            attempt_id: "attempt-1".into(),
            task_revision: 1,
            admitted_gm_epoch: 0,
            submission_ref: "submission-1".into(),
            accepted_operation_id: "accept-1".into(),
            candidate_ref: "candidate-1".into(),
            candidate_sha256: "a".repeat(64),
            commit: candidate.clone(),
            tree: "b".repeat(40),
            remote_name: "origin".into(),
            target_ref: "refs/heads/main".into(),
            expected_old_ref: Some(fetch_base.clone()),
            expected_create: false,
            force: false,
            policy_revision: "owner-policy-v1".into(),
        };

        assert_ne!(fetch_url, push_url);
        let fetch_preflight = remote_ref_at(&config, &project, &intent, &fetch_url).unwrap();
        let push_preflight = remote_ref_at(&config, &project, &intent, &push_url).unwrap();
        assert_eq!(fetch_preflight, RefReadback::At(fetch_base.clone()));
        assert!(fetch_preflight.matches_expected(&intent));
        assert_eq!(push_preflight, RefReadback::At(push_base.clone()));
        assert!(!push_preflight.matches_expected(&intent));
        // The wrong endpoint would permit a normal fast-forward push despite
        // the configured expected-old value not matching its destination.
        fixture_git_in(
            &git,
            &checkout,
            &["merge-base", "--is-ancestor", &push_base, &candidate],
        );

        // Model a misleading fetch mirror that already contains the candidate.
        // Restart readback must still query the push endpoint and remain
        // unknown, rather than confirming from the fetch-only mirror.
        fixture_fetch_ref(&git, &fetch_repository, &checkout, "refs/heads/candidate");
        let fetch_readback = remote_ref_at(&config, &project, &intent, &fetch_url).unwrap();
        let push_readback = remote_ref_at(&config, &project, &intent, &push_url).unwrap();
        assert!(matches!(
            restart_readback_outcome(&intent, fetch_readback),
            ForgeOutcome::Applied { .. }
        ));
        assert!(matches!(
            restart_readback_outcome(&intent, push_readback),
            ForgeOutcome::Unknown { .. }
        ));

        std::fs::remove_dir_all(directory).unwrap();
    }

    fn seed_reconciliation_operation(
        db: &Connection,
        operation_id: &str,
        state: &str,
        result: Value,
    ) {
        let intent = PublicationIntent {
            operation_id: operation_id.to_owned(),
            ..intent()
        };
        let now = 1_i64;
        for (artifact_id, relative_path, kind) in [
            ("submission-1", "test/submission.json", "submission"),
            ("candidate-1", "test/candidate.json", "candidate"),
        ] {
            db.execute(
                "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,?3,0,NULL,?4,'{}')",
                params![artifact_id, relative_path, kind, now],
            )
            .unwrap();
        }
        db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES('acceptance-1','operator','acceptance-request','task.accept','{}','{}','settled','{}',0,1,1,1)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO tasks(task_id,project_id,origin_key,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES('task-1','project-1',NULL,1,'open','{}',1,1)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,state,submission_ref,candidate_ref,released_at_ms,created_at_ms,updated_at_ms) VALUES('attempt-1','task-1',1,'{}','operator','controller','accepted','submission-1','candidate-1',1,1,1)",
            [],
        )
        .unwrap();
        db.execute(
            "UPDATE tasks SET state='accepted',accepted_attempt_id='attempt-1',accepted_operation_id='acceptance-1',accepted_revision=1,accepted_phase='complete',accepted_candidate_ref='candidate-1' WHERE task_id='task-1'",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,result_json,due_at_ms,sent_at_ms,created_at_ms,updated_at_ms) VALUES(?1,'operator',?2,'forge.publish_ref','{}',?3,'task-1','attempt-1',?4,?5,0,2,1,1)",
            params![
                operation_id,
                format!("request-{operation_id}"),
                model::canonical(&json!({"publication_intent":intent})).unwrap(),
                state,
                model::canonical(&result).unwrap(),
            ],
        )
        .unwrap();
    }

    fn intent() -> PublicationIntent {
        PublicationIntent {
            operation_id: "op-1".into(),
            project_id: "project-1".into(),
            canonical_repository: "github.com/owner/repo".into(),
            attempt_id: "attempt-1".into(),
            task_revision: 1,
            admitted_gm_epoch: 0,
            submission_ref: "submission-1".into(),
            accepted_operation_id: "acceptance-1".into(),
            candidate_ref: "candidate-1".into(),
            candidate_sha256: "a".repeat(64),
            commit: "b".repeat(40),
            tree: "c".repeat(40),
            remote_name: "origin".into(),
            target_ref: "refs/heads/main".into(),
            expected_old_ref: Some("d".repeat(40)),
            expected_create: false,
            force: false,
            policy_revision: "owner-policy-v1".into(),
        }
    }

    #[test]
    fn re_promoted_gm_does_not_make_an_old_queued_epoch_current_again() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(super::super::SCHEMA).unwrap();
        assert_eq!(current_gm_epoch(&db).unwrap(), 0);
        super::super::set_meta(&db, "gm", &json!({"client_id":"manager-a","epoch":4})).unwrap();
        let admitted_epoch = current_gm_epoch(&db).unwrap();

        super::super::set_meta(&db, "gm", &json!({"client_id":"manager-b","epoch":5})).unwrap();
        super::super::set_meta(&db, "gm", &json!({"client_id":"manager-a","epoch":6})).unwrap();
        let current_epoch = current_gm_epoch(&db).unwrap();

        assert_eq!(admitted_epoch, 4);
        assert_eq!(current_epoch, 6);
        assert_ne!(admitted_epoch, current_epoch);
        let (result, state) = outcome_value(
            "op-stale",
            &ForgeOutcome::StaleGmEpoch {
                admitted_gm_epoch: admitted_epoch,
                current_gm_epoch: current_epoch,
            },
        );
        assert_eq!(state, "settled");
        assert_eq!(result["outcome"], "stale_gm_epoch");
        assert_eq!(result["publication"], "not_started");
    }

    #[test]
    fn restart_readback_only_proves_the_exact_candidate_commit() {
        let intent = intent();
        let candidate = restart_readback_outcome(&intent, RefReadback::At(intent.commit.clone()));
        let (candidate_result, candidate_state) = outcome_value("op-1", &candidate);
        assert_eq!(candidate_state, "settled");
        assert_eq!(candidate_result["outcome"], "applied");

        for readback in [
            RefReadback::At(intent.expected_old_ref.clone().unwrap()),
            RefReadback::Missing,
            RefReadback::At("e".repeat(40)),
        ] {
            let outcome = restart_readback_outcome(&intent, readback);
            let (result, state) = outcome_value("op-1", &outcome);
            assert_eq!(state, "outcome_unknown");
            assert_eq!(result["outcome"], "unknown");
            assert_eq!(result["publication"], "requires_readback");
        }
    }

    #[test]
    fn unconfirmed_process_tree_holds_all_publications_to_its_repository() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(super::super::SCHEMA).unwrap();
        let effective = model::canonical(&json!({
            "publication_intent": {
                "canonical_repository": "github.com/owner/repo",
                "target_ref": "refs/heads/main"
            }
        }))
        .unwrap();
        db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,result_json,due_at_ms,created_at_ms,updated_at_ms) \
             VALUES('op-tree-hold','operator','req-tree-hold','forge.publish_ref','{}',?1,'outcome_unknown',?2,0,1,1)",
            params![effective, model::canonical(&json!({"process_tree_unconfirmed":true})).unwrap()],
        )
        .unwrap();

        assert_eq!(
            unresolved_process_tree_hold(&db, "github.com/owner/repo").unwrap(),
            Some("op-tree-hold".into())
        );
        assert_eq!(
            unresolved_process_tree_hold(&db, "github.com/owner/another-repo").unwrap(),
            None
        );
        let error = require_no_process_tree_hold(&db, "github.com/owner/repo").unwrap_err();
        assert_eq!(error.code, "FORGE_PROCESS_TREE_UNCONFIRMED");
        assert!(error.message.contains("op-tree-hold"));
    }

    #[test]
    fn matching_ref_readback_does_not_clear_an_unconfirmed_process_tree_hold() {
        let intent = intent();
        let previous_result = json!({"process_tree_unconfirmed":true});
        let applied = ForgeOutcome::Applied {
            readback: RefReadback::At(intent.commit.clone()),
            push_exit_code: None,
            timed_out: false,
            stderr_digest: None,
            stderr_bytes: None,
        };
        let (mut result, mut state) = outcome_value("op-1", &applied);
        preserve_process_tree_hold(&previous_result, &mut result, &mut state);

        assert_eq!(state, "outcome_unknown");
        assert_eq!(result["outcome"], "unknown");
        assert_eq!(result["reason"], "process_tree_unconfirmed");
        assert_eq!(result["publication"], "operator_intervention_required");
        assert_eq!(result["process_tree_unconfirmed"], true);
        assert_eq!(result["remote_ref"]["commit"], intent.commit);
    }

    #[test]
    fn clean_readback_recovery_remains_available_without_a_process_tree_hold() {
        let intent = intent();
        let previous_result = json!({
            "process_tree_unconfirmed":false,
            "publication_may_have_started":true
        });
        let applied = restart_readback_outcome(&intent, RefReadback::At(intent.commit.clone()));
        let (mut result, mut state) = outcome_value("op-1", &applied);
        preserve_process_tree_hold(&previous_result, &mut result, &mut state);

        assert_eq!(state, "settled");
        assert_eq!(result["outcome"], "applied");
        assert_ne!(result["publication"], "operator_intervention_required");
    }

    #[test]
    fn only_unconfirmed_runner_cleanup_creates_a_sticky_process_tree_outcome() {
        let held = runner_error_outcome(Error::new(
            "FORGE_GIT_TREE_TERMINATION",
            "process-tree cleanup was not confirmed",
        ));
        let (held_result, held_state) = outcome_value("op-held", &held);
        assert_eq!(held_state, "outcome_unknown");
        assert_eq!(held_result["process_tree_unconfirmed"], true);
        assert_eq!(held_result["reason"], "git_process_tree_unconfirmed");

        let ordinary = runner_error_outcome(Error::new(
            "FORGE_GIT_REJECTED",
            "configured validation command failed after confirmed cleanup",
        ));
        assert!(matches!(ordinary, ForgeOutcome::Failed(_)));
    }

    #[test]
    fn finished_clean_unknown_can_reconcile_exact_commit_through_store_state() {
        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch(super::super::SCHEMA).unwrap();
        seed_reconciliation_operation(
            &db,
            "op-clean-unknown",
            "outcome_unknown",
            json!({
                "outcome":"unknown",
                "process_tree_unconfirmed":false,
                "publication_may_have_started":true
            }),
        );
        let principal = Principal {
            link_id: "forge-test".into(),
            client_id: "operator".into(),
            role: Role::Operator,
        };
        let config = reconciliation_config();
        let work = begin_reconciliation(&mut db, principal, "op-clean-unknown", &config)
            .unwrap()
            .expect("clean unknown should be readback eligible");
        assert_eq!(work.mode, WorkMode::ReadbackOnly);
        let before = operations::get_operation(&db, "op-clean-unknown").unwrap();
        assert_eq!(before["state"], "outcome_unknown");
        assert_eq!(before["result"]["process_tree_unconfirmed"], false);

        let outcome =
            restart_readback_outcome(&work.intent, RefReadback::At(work.intent.commit.clone()));
        finish(&mut db, "op-clean-unknown", outcome).unwrap();
        let after = operations::get_operation(&db, "op-clean-unknown").unwrap();
        assert_eq!(after["state"], "settled");
        assert_eq!(after["result"]["outcome"], "applied");
        assert_eq!(after["result"]["publication_may_have_started"], true);
        assert_ne!(after["result"]["process_tree_unconfirmed"], true);
    }

    #[test]
    fn sending_after_restart_stays_held_even_if_readback_matches() {
        let directory =
            std::env::temp_dir().join(format!("swarm-forge-reopen-{}", model::new_id()));
        std::fs::create_dir_all(&directory).unwrap();
        let credential = crate::model::Credential {
            client_id: "operator".into(),
            token: "forge-restart-fixture-token".into(),
        };
        let db = super::super::open_database(&directory, &credential).unwrap();
        seed_reconciliation_operation(
            &db,
            "op-interrupted-send",
            "sending",
            json!({
                "process_tree_unconfirmed":false,
                "process_tree_status":"in_flight",
                "publication_may_have_started":false
            }),
        );
        drop(db);

        // Reopening exercises the real host-startup transaction before any
        // Forge reconciliation can observe this interrupted send.
        let mut db = super::super::open_database(&directory, &credential).unwrap();
        let restarted = operations::get_operation(&db, "op-interrupted-send").unwrap();
        assert_eq!(restarted["state"], "outcome_unknown");
        assert_eq!(restarted["result"]["outcome"], "unknown");
        assert_eq!(restarted["result"]["process_tree_unconfirmed"], true);
        assert_eq!(
            restarted["result"]["process_tree_status"],
            "unconfirmed_after_restart"
        );
        assert_eq!(
            restarted["result"]["process_tree_cleanup"],
            "host_lifecycle_interrupted_before_confirmation"
        );

        let principal = Principal {
            link_id: "forge-test".into(),
            client_id: "operator".into(),
            role: Role::Operator,
        };
        let config = reconciliation_config();
        let work = begin_reconciliation(&mut db, principal, "op-interrupted-send", &config)
            .unwrap()
            .expect("interrupted send should get readback only");
        let held = operations::get_operation(&db, "op-interrupted-send").unwrap();
        assert_eq!(held["state"], "outcome_unknown");
        assert_eq!(held["result"]["process_tree_unconfirmed"], true);
        assert_eq!(held["result"]["publication_may_have_started"], false);

        let outcome =
            restart_readback_outcome(&work.intent, RefReadback::At(work.intent.commit.clone()));
        finish(&mut db, "op-interrupted-send", outcome).unwrap();
        let after = operations::get_operation(&db, "op-interrupted-send").unwrap();
        assert_eq!(after["state"], "outcome_unknown");
        assert_eq!(after["result"]["process_tree_unconfirmed"], true);
        assert_eq!(
            after["result"]["publication"],
            "operator_intervention_required"
        );
        assert_eq!(
            unresolved_process_tree_hold(&db, "github.com/owner/repo").unwrap(),
            Some("op-interrupted-send".into())
        );
        drop(db);
        std::fs::remove_dir_all(directory).unwrap();
    }
}

fn pending(db: &Connection) -> Result<Vec<(String, String)>> {
    let mut statement = db.prepare(
        "SELECT operation_id,caller_id FROM operations WHERE method='forge.publish_ref' AND state IN ('sending','outcome_unknown') ORDER BY created_at_ms,operation_id",
    )?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn queued_pending(db: &Connection) -> Result<Vec<(String, String)>> {
    let mut statement = db.prepare(
        "SELECT operation_id,caller_id FROM operations WHERE method='forge.publish_ref' AND state='queued' ORDER BY created_at_ms,operation_id",
    )?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

impl super::Store {
    /// Durably admit one request and return its receipt. The host-owned forge
    /// supervisor performs all Git work, so cancellation of the IPC future
    /// cannot release or detach the worker that owns a publication effect.
    pub(crate) async fn publish_ref(&self, principal: Principal, params: Value) -> Result<Value> {
        PublishRefRequest::parse(&params)?;
        let config = self.config.clone();
        let p = principal.clone();
        let receipt = self
            .run(move |db| {
                let current = current_principal(db, p)?;
                if !matches!(current.role, Role::Operator | Role::Manager) {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "publication requires the operator or current GM",
                    ));
                }
                gm::require_authority(db, &current)?;
                super::mutate(db, &current, "forge.publish_ref", &params, &config)
            })
            .await?;
        self.changed
            .send_modify(|value| *value = value.wrapping_add(1));
        Ok(receipt)
    }

    /// A single host-owned lifecycle pass: reconcile every prior uncertain
    /// write by exact-ref readback, then dispatch only durable `queued` work.
    /// Host shutdown must await this future rather than aborting it.
    pub(crate) async fn supervise_forge_once(&self) -> Result<()> {
        let lock = FORGE_WRITE_LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
        let _guard = lock.lock().await;
        self.reconcile_forge_locked().await?;
        let queued = self.run(|db| queued_pending(db)).await?;
        let config = self.config.forge.clone();
        for (id, caller_id) in queued {
            let caller = Principal {
                link_id: "forge-queue-supervisor".into(),
                client_id: caller_id,
                role: Role::Operator,
            };
            let local_config = config.clone();
            let local_caller = caller.clone();
            let local_id = id.clone();
            let work = self
                .run(move |db| begin(db, local_caller, &local_id, &local_config))
                .await?;
            if let Some(work) = work {
                self.drive_forge(work, caller).await;
            }
        }
        Ok(())
    }

    async fn reconcile_forge_locked(&self) -> Result<()> {
        // A cancelled supervisor future may leave its owned blocking Git
        // closure draining. Its semaphore permit remains inside that closure.
        // Wait for it before changing `sending` to unknown or reading the ref.
        let slot = process_slot();
        let process_guard = slot
            .acquire_owned()
            .await
            .map_err(|_| Error::new("FORGE_PROCESS_CLOSED", "forge process slot stopped"))?;
        drop(process_guard);
        let pending = self.run(|db| pending(db)).await?;
        let config = self.config.forge.clone();
        for (id, caller_id) in pending {
            let caller = Principal {
                link_id: "forge-reconciler".into(),
                client_id: caller_id,
                role: Role::Operator,
            };
            let local_config = config.clone();
            let local_caller = caller.clone();
            let local_id = id.clone();
            let work = self
                .run(move |db| begin_reconciliation(db, local_caller, &local_id, &local_config))
                .await?;
            if let Some(work) = work {
                self.drive_forge(work, caller).await;
            }
        }
        Ok(())
    }

    async fn drive_forge(&self, work: ForgeWork, principal: Principal) {
        let id = work.intent.operation_id.clone();
        let outcome = if work.mode == WorkMode::ReadbackOnly {
            let config = self.config.forge.clone();
            let scan = work.clone();
            self.forge_file_io(move |_| Ok(execute_readback(&config, &scan)))
                .await
                .unwrap_or_else(|error| ForgeOutcome::Unknown {
                    reason: if error.code == "FORGE_GIT_TREE_TERMINATION" {
                        "git_process_tree_unconfirmed"
                    } else {
                        "readback_worker_lifecycle_unconfirmed"
                    },
                    readback: None,
                    timed_out: false,
                    stderr_digest: None,
                    stderr_bytes: None,
                    process_tree_unconfirmed: true,
                })
        } else {
            let config = self.config.forge.clone();
            let prep = work.clone();
            match self
                .forge_file_io(move |files| prepare_candidate(&files, &config, &prep))
                .await
            {
                Err(error) => runner_error_outcome(error),
                Ok(()) => {
                    let config = self.config.forge.clone();
                    let preflight_work = work.clone();
                    match self
                        .forge_file_io(move |_| prepare_push(&config, &preflight_work))
                        .await
                    {
                        Err(error) => runner_error_outcome(error),
                        Ok(push_url) => {
                            let auth_work = work.clone();
                            let auth_principal = principal.clone();
                            let config = self.config.forge.clone();
                            match self
                                .run(move |db| {
                                    dispatch_authorized(db, auth_principal, &auth_work, &config)
                                })
                                .await
                            {
                                Err(error) => runner_error_outcome(error),
                                Ok(DispatchAuthorization::StaleGmEpoch {
                                    admitted_gm_epoch,
                                    current_gm_epoch,
                                }) => ForgeOutcome::StaleGmEpoch {
                                    admitted_gm_epoch,
                                    current_gm_epoch,
                                },
                                Ok(DispatchAuthorization::Authorized) => {
                                    let config = self.config.forge.clone();
                                    let push_work = work.clone();
                                    let push_url = push_url.clone();
                                    self.forge_file_io(move |_| {
                                        Ok(execute_push(&config, &push_work, &push_url))
                                    })
                                    .await
                                    .unwrap_or_else(|error| ForgeOutcome::Unknown {
                                        reason: "push_worker_failed",
                                        readback: None,
                                        timed_out: false,
                                        stderr_digest: Some(model::digest(error.code.as_bytes())),
                                        stderr_bytes: None,
                                        process_tree_unconfirmed: true,
                                    })
                                }
                            }
                        }
                    }
                }
            }
        };
        let _ = self.run(move |db| finish(db, &id, outcome)).await;
        self.changed
            .send_modify(|value| *value = value.wrapping_add(1));
    }

    async fn forge_file_io<T: Send + 'static>(
        &self,
        f: impl FnOnce(crate::artifacts::ArtifactFiles) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let permit = process_slot()
            .acquire_owned()
            .await
            .map_err(|_| Error::new("FORGE_PROCESS_CLOSED", "forge process slot stopped"))?;
        self.file_io(move |files| {
            let _permit = permit;
            f(files)
        })
        .await
    }
}
