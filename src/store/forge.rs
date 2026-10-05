//! Exact accepted-candidate publication through the configured native Git client.
//! Admission is durable before a worker can run. Reconciliation only reads the
//! exact remote ref; it never replays a push whose outcome is unknown.
use super::{current_principal, gm, meta, operations, results, set_meta, submissions, tasks};
use crate::{
    artifacts::ArtifactRecord,
    automation::{authorization::TransferContinuation, publication::PublicationContext},
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
    collections::HashMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
    time::Duration,
};
use tokio::process::{Child as TokioChild, Command as TokioCommand};

/// Physical ownership follows the durable publication resource, rather than
/// the project or local remote alias.  `canonical_repository` is normalized
/// when the intent is admitted and `target_ref` is retained verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ForgeTargetKey {
    canonical_repository: String,
    target_ref: String,
}

impl ForgeTargetKey {
    fn from_intent(intent: &PublicationIntent) -> Result<Self> {
        Ok(Self {
            canonical_repository: crate::forge::canonical_repository(&intent.canonical_repository)?,
            target_ref: intent.target_ref.clone(),
        })
    }
}

#[derive(Debug)]
struct ForgeTargetLane {
    /// Held for the full durable begin -> native work -> finish sequence.
    /// `OwnedMutexGuard` keeps same-target operations serialized even while
    /// their short Store transactions yield to the runtime.
    serial: Arc<tokio::sync::Mutex<()>>,
    /// Captured by each blocking closure.  If its supervisor future is
    /// dropped while the closure is draining, this permit remains held until
    /// the closure exits, so the next same-target operation cannot read back
    /// or start a second effect prematurely.
    process: Arc<tokio::sync::Semaphore>,
}

struct ForgeTargetCoordinator {
    lanes: Mutex<HashMap<ForgeTargetKey, Weak<ForgeTargetLane>>>,
}

static FORGE_TARGET_COORDINATOR: OnceLock<ForgeTargetCoordinator> = OnceLock::new();

fn target_lane(key: &ForgeTargetKey) -> Arc<ForgeTargetLane> {
    let coordinator = FORGE_TARGET_COORDINATOR.get_or_init(|| ForgeTargetCoordinator {
        lanes: Mutex::new(HashMap::new()),
    });
    let mut lanes = coordinator
        .lanes
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    lanes.retain(|_, lane| lane.strong_count() != 0);
    if let Some(lane) = lanes.get(key).and_then(Weak::upgrade) {
        return lane;
    }
    let lane = Arc::new(ForgeTargetLane {
        serial: Arc::new(tokio::sync::Mutex::new(())),
        process: Arc::new(tokio::sync::Semaphore::new(1)),
    });
    lanes.insert(key.clone(), Arc::downgrade(&lane));
    lane
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkMode {
    PushOnce,
    ReadbackOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForgePass {
    Reconcile,
    Dispatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DispatchAuthorization {
    Authorized,
    Coalesced {
        owner_operation_id: String,
    },
    StaleGmEpoch {
        admitted_gm_epoch: i64,
        current_gm_epoch: i64,
    },
}

#[derive(Debug, Clone)]
struct NativeWorkerJob {
    kind: &'static str,
    phase_name: &'static str,
    job_id: String,
    operation_id: String,
    owner_token: String,
    plan_sha256: String,
    directory: PathBuf,
    plan_path: PathBuf,
    owner_path: PathBuf,
    authorization_path: PathBuf,
    result_path: PathBuf,
    executable: PathBuf,
    executable_sha256: String,
    timeout_seconds: u64,
}

struct NativeWorkerRun {
    job: NativeWorkerJob,
    child: TokioChild,
    owner_record: Value,
    _process_permit: tokio::sync::OwnedSemaphorePermit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerResultStatus {
    Present,
    Missing,
    Malformed,
}

impl WorkerResultStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::Missing => "missing",
            Self::Malformed => "malformed",
        }
    }
}

enum RetainedWorkerFile {
    Missing,
    OverLimit,
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone)]
struct ForgeWork {
    intent: PublicationIntent,
    lane: Arc<ForgeTargetLane>,
    project: ForgeProject,
    candidate: Option<ArtifactRecord>,
    mode: WorkMode,
    transfer_continuation: Option<TransferContinuation>,
}

#[derive(Debug, Clone)]
enum ForgeOutcome {
    Coalesced {
        owner_operation_id: String,
    },
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

#[derive(Debug, Clone)]
enum ForgeActor {
    Direct { client_id: String },
    OnBehalf(Box<PublicationContext>),
}

impl ForgeActor {
    fn caller_id(&self) -> &str {
        match self {
            Self::Direct { client_id } => client_id,
            Self::OnBehalf(context) => context.technical_requester_id(),
        }
    }

    fn require_current_write_authority(&self, db: &Connection) -> Result<i64> {
        match self {
            Self::Direct { client_id } => require_direct_write_authority(db, client_id),
            Self::OnBehalf(context) => {
                context.require_current(db)?;
                Ok(context.gm_epoch())
            }
        }
    }

    fn transfer_continuation(&self, db: &Connection) -> Result<Option<TransferContinuation>> {
        match self {
            Self::Direct { .. } => Ok(None),
            Self::OnBehalf(context) => context.transfer_continuation(db),
        }
    }

    fn require_prepared_transfer_write_authority(
        &self,
        db: &Connection,
        continuation: &TransferContinuation,
    ) -> Result<i64> {
        match self {
            Self::Direct { .. } => Err(Error::new(
                "AUTOMATION_TRANSFER_SCOPE",
                "direct Forge actors cannot use an automation transfer grant",
            )),
            Self::OnBehalf(context) => {
                context.require_prepared_transfer_continuation(db, continuation)?;
                Ok(context.gm_epoch())
            }
        }
    }

    fn require_readback_authority(&self, db: &Connection, config: &Config) -> Result<()> {
        match self {
            // Direct manual publications keep their existing recovery path:
            // the durable caller/Operation link is enough to read its exact ref.
            Self::Direct { .. } => Ok(()),
            Self::OnBehalf(context) => context.require_readback_authority(db, config),
        }
    }

    fn require_request_matches(&self, input: &PublishRefRequest) -> Result<()> {
        match self {
            Self::Direct { .. } => Ok(()),
            Self::OnBehalf(context) => {
                let expected = PublishRefRequest::parse(&context.request_value()?)?;
                if &expected != input {
                    return Err(Error::new(
                        "AUTOMATION_LINK_CORRUPT",
                        "publication request differs from its retained automation context",
                    ));
                }
                Ok(())
            }
        }
    }

    fn require_intent_matches(&self, intent: &PublicationIntent) -> Result<()> {
        let Self::OnBehalf(context) = self else {
            return Ok(());
        };
        if intent.project_id != context.project_id()
            || intent.canonical_repository != context.canonical_repository()
            || intent.attempt_id != context.attempt_id()
            || intent.task_revision != context.task_revision()
            || intent.admitted_gm_epoch != context.gm_epoch()
            || intent.submission_ref != context.submission_ref()
            || intent.accepted_operation_id != context.accepted_operation_id()
            || intent.candidate_ref != context.candidate_ref()
            || intent.policy_revision != context.policy_revision()
            || intent.target_ref != context.target_ref()
            || intent.expected_old_ref.as_deref() != context.expected_old_ref()
            || intent.expected_create != context.expected_create()
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication intent differs from its retained automation context",
            ));
        }
        Ok(())
    }
}

fn require_direct_write_authority(db: &Connection, client_id: &str) -> Result<i64> {
    let registration = meta(db, &format!("client:{client_id}"))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "client no longer registered"))?;
    if registration["disabled"] == true {
        return Err(Error::new("UNAUTHORIZED", "client disabled"));
    }
    let role: Role = serde_json::from_value(registration["role"].clone())?;
    match role {
        Role::Operator => super::require_local_operator(db, client_id)?,
        Role::Manager
            if gm::record(db)?
                .as_ref()
                .is_some_and(|record| record["client_id"] == client_id) => {}
        Role::Manager => {
            return Err(Error::new("FORBIDDEN", "current GM authority required"));
        }
        _ => return Err(Error::new("FORBIDDEN", "operator or current GM required")),
    }
    current_gm_epoch(db)
}

/// Authorize the current actor to read back an already confirmed publication
/// without requiring its accepted candidate to remain the Task's current
/// revision. This is only for a separately retained unknown PR effect; it does
/// not authorize a new publication or metadata write.
pub(super) fn historical_applied_publication_context(
    db: &Connection,
    principal: &Principal,
    operation_id: &str,
) -> Result<(PublicationIntent, String)> {
    let current = current_principal(db, principal.clone())?;
    ForgeActor::Direct {
        client_id: current.client_id.clone(),
    }
    .require_current_write_authority(db)?;

    retained_applied_publication_record(db, operation_id)
}

/// Validate the immutable facts of a confirmed publication without performing
/// a current-rights check. This is only for finalizing an already-authorized
/// exact PR readback after its GET crossed a manager handover. It does not
/// authorize a GET, retry, or new effect.
pub(super) fn retained_applied_publication_record(
    db: &Connection,
    operation_id: &str,
) -> Result<(PublicationIntent, String)> {
    let operation = operations::get_operation(db, operation_id)?;
    if operation["method"] != "forge.publish_ref"
        || operation["state"] != "settled"
        || operation["result"]["outcome"] != "applied"
        || operation["result"]["publication"] != "confirmed_by_remote_readback"
        || operation["result"]["acceptance_current_at_finish"] != true
        || operation["result"]["remote_ref"]["present"] != true
    {
        return Err(Error::new(
            "GITHUB_PR_PUBLICATION_REQUIRED",
            "readback requires the exact retained, remotely confirmed publication",
        ));
    }

    let saved = saved_intent(db, operation_id)?;
    let request = request(db, operation_id)?;
    let attempt = tasks::get_attempt(db, &saved.attempt_id)?;
    let task_id = model::text(&attempt, "task_id")?.to_owned();
    let task = tasks::get_task(db, &task_id)?;
    if operation["task_id"] != task_id
        || operation["attempt_id"] != saved.attempt_id
        || task["project_id"] != saved.project_id
        || saved.operation_id != operation_id
        || saved.attempt_id != request.attempt_id
        || saved.task_revision != request.expected_revision
        || saved.submission_ref != request.submission_ref
        || saved.accepted_operation_id != request.accepted_operation_id
        || saved.candidate_ref != request.candidate_ref
        || saved.policy_revision != request.expected_policy_revision
        || saved.target_ref != request.target_ref
        || saved.expected_old_ref != request.expected_old_ref
        || saved.expected_create != request.expected_create
        || operation["result"]["remote_ref"]["commit"] != saved.commit
    {
        return Err(Error::new(
            "GITHUB_PR_PUBLICATION_STALE",
            "the retained publication request, Task, Attempt, project, or confirmed commit is inconsistent",
        ));
    }
    Ok((saved, task_id))
}

fn actor_for_operation(db: &Connection, id: &str, operation: &Value) -> Result<ForgeActor> {
    let caller_id = model::text(operation, "caller_id")?;
    let effective_raw: String = db.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1",
        [id],
        |row| row.get(0),
    )?;
    let effective: Value = serde_json::from_str(&effective_raw).map_err(|_| {
        Error::new(
            "FORGE_INTENT_INVALID",
            "publication effective request is invalid",
        )
    })?;
    if !effective["automation_on_behalf"].is_null() {
        let context = PublicationContext::from_committed_operation(db, id)?;
        if caller_id != context.technical_requester_id() {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication technical requester differs from its retained Operation",
            ));
        }
        Ok(ForgeActor::OnBehalf(Box::new(context)))
    } else if caller_id == crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID {
        Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "internal publication Operation has no on-behalf attribution",
        ))
    } else {
        Ok(ForgeActor::Direct {
            client_id: caller_id.to_owned(),
        })
    }
}

fn current_gm_epoch(db: &Connection) -> Result<i64> {
    match gm::record(db)? {
        None => Ok(0),
        Some(record) => model::positive(&record, "epoch"),
    }
}

fn accepted_candidate(
    db: &Connection,
    actor: &ForgeActor,
    input: &PublishRefRequest,
    config: &ForgeConfig,
) -> Result<(PublicationIntent, ForgeProject, ArtifactRecord)> {
    accepted_candidate_with_transfer(db, actor, input, config, None)
}

fn accepted_candidate_with_transfer(
    db: &Connection,
    actor: &ForgeActor,
    input: &PublishRefRequest,
    config: &ForgeConfig,
    transfer_continuation: Option<&TransferContinuation>,
) -> Result<(PublicationIntent, ForgeProject, ArtifactRecord)> {
    let admitted_gm_epoch = match transfer_continuation {
        Some(continuation) => actor.require_prepared_transfer_write_authority(db, continuation)?,
        None => actor.require_current_write_authority(db)?,
    };
    actor.require_request_matches(input)?;
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
    if let ForgeActor::OnBehalf(context) = actor
        && (context.project_id() != project_id
            || context.policy_revision() != project.policy_revision
            || crate::forge::canonical_repository(&project.canonical_repository)?
                != context.canonical_repository())
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "publication project mapping differs from its retained automation context",
        ));
    }
    Ok((intent, project, candidate))
}

pub(super) fn reserve(
    tx: &Transaction<'_>,
    p: &Principal,
    value: &Value,
    id: &str,
    config: &Config,
) -> Result<Value> {
    let current = current_principal(tx, p.clone())?;
    if !matches!(current.role, Role::Operator | Role::Manager) {
        return Err(Error::new(
            "FORBIDDEN",
            "publication requires the operator or current GM",
        ));
    }
    gm::require_authority(tx, &current)?;
    let actor = ForgeActor::Direct {
        client_id: current.client_id.clone(),
    };
    let input = PublishRefRequest::parse(value)?;
    let (mut intent, _project, _candidate) = accepted_candidate(tx, &actor, &input, &config.forge)?;
    intent.operation_id = id.to_owned();
    persist_intent(tx, id, &input, &intent, None)?;
    if let Some(owner) = exact_slot_owner(tx, id, &intent)? {
        return Ok(coalesced_receipt(id, &owner));
    }
    require_no_process_tree_hold(tx, &intent.canonical_repository)?;
    Ok(queued_receipt(id, &input))
}

pub(super) fn reserve_on_behalf(
    tx: &Transaction<'_>,
    context: &PublicationContext,
    value: &Value,
    id: &str,
    config: &Config,
) -> Result<Value> {
    let operation = operations::get_operation(tx, id)?;
    if operation["method"] != "forge.publish_ref"
        || operation["caller_id"] != context.technical_requester_id()
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "automatic publication Operation has the wrong technical requester",
        ));
    }
    context.require_current(tx)?;
    let actor = ForgeActor::OnBehalf(Box::new(context.clone()));
    let input = PublishRefRequest::parse(value)?;
    let (mut intent, _project, _candidate) = accepted_candidate(tx, &actor, &input, &config.forge)?;
    intent.operation_id = id.to_owned();
    persist_intent(tx, id, &input, &intent, Some(&context.linkage_value()))?;
    if let Some(owner) = exact_slot_owner(tx, id, &intent)? {
        return Ok(coalesced_receipt(id, &owner));
    }
    require_no_process_tree_hold(tx, &intent.canonical_repository)?;
    Ok(queued_receipt(id, &input))
}

fn persist_intent(
    tx: &Transaction<'_>,
    id: &str,
    input: &PublishRefRequest,
    intent: &PublicationIntent,
    attribution: Option<&Value>,
) -> Result<()> {
    let task_id = model::text(&tasks::get_attempt(tx, &input.attempt_id)?, "task_id")?.to_owned();
    let mut effective = json!({"publication_intent":intent});
    if let Some(attribution) = attribution {
        if attribution.is_null() {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "automatic publication attribution could not be serialized",
            ));
        }
        effective["automation_on_behalf"] = attribution.clone();
    }
    tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=?4 WHERE operation_id=?1 AND method='forge.publish_ref'",
        params![id, task_id, input.attempt_id, model::canonical(&effective)?],
    )?;
    Ok(())
}

fn queued_receipt(id: &str, input: &PublishRefRequest) -> Value {
    json!({
        "operation_id":id,
        "attempt_id":input.attempt_id,
        "state":"queued",
        "publication":"not_started",
        "force":false
    })
}

fn coalesced_receipt(id: &str, owner_operation_id: &str) -> Value {
    json!({
        "operation_id":id,
        "outcome":"coalesced",
        "state":"settled",
        "publication":"coalesced",
        "coalesced":true,
        "coalesced_to":owner_operation_id,
        "publication_may_have_started":false,
        "force":false
    })
}

fn exact_slot_owner(
    db: &Connection,
    operation_id: &str,
    intent: &PublicationIntent,
) -> Result<Option<String>> {
    let current_epoch = current_gm_epoch(db)?;
    let mut statement = db.prepare(
        "SELECT operation_id,effective_request_json,state,result_json FROM operations \
         WHERE method='forge.publish_ref' AND operation_id<>?1 \
           AND json_extract(effective_request_json,'$.publication_intent.project_id')=?2 \
           AND json_extract(effective_request_json,'$.publication_intent.canonical_repository')=?3 \
           AND json_extract(effective_request_json,'$.publication_intent.candidate_ref')=?4 \
         ORDER BY created_at_ms,operation_id",
    )?;
    let mut rows = statement.query(params![
        operation_id,
        intent.project_id,
        intent.canonical_repository,
        intent.candidate_ref
    ])?;
    while let Some(row) = rows.next()? {
        let candidate_operation_id: String = row.get(0)?;
        let effective_raw: String = row.get(1)?;
        let state: String = row.get(2)?;
        let result_raw: Option<String> = row.get(3)?;
        if matches!(state.as_str(), "cancelled" | "rejected") {
            continue;
        }
        let effective: Value = serde_json::from_str(&effective_raw).map_err(|_| {
            Error::new(
                "FORGE_SLOT_CORRUPT",
                "retained publication intent is invalid",
            )
        })?;
        let candidate: PublicationIntent =
            serde_json::from_value(effective["publication_intent"].clone()).map_err(|_| {
                Error::new(
                    "FORGE_SLOT_CORRUPT",
                    "retained publication intent is invalid",
                )
            })?;
        candidate.validate().map_err(|_| {
            Error::new(
                "FORGE_SLOT_CORRUPT",
                "retained publication intent is invalid",
            )
        })?;
        if candidate.operation_id != candidate_operation_id {
            return Err(Error::new(
                "FORGE_SLOT_CORRUPT",
                "retained publication intent differs from its Operation identity",
            ));
        }
        let result = result_raw
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|_| {
                Error::new(
                    "FORGE_SLOT_CORRUPT",
                    "retained publication result is invalid",
                )
            })?
            .unwrap_or(Value::Null);
        if same_effect_slot(&candidate, intent)
            && operation_owns_slot(&state, &result, &candidate, current_epoch)
        {
            return Ok(Some(candidate_operation_id));
        }
    }
    Ok(None)
}

/// A retained slot blocks an identical request only while it is current work,
/// may have crossed the write boundary, or confirms the remote effect. Known
/// no-effect records (cancelled, stale-epoch, pre-write failure, definite Git
/// rejection, and coalesced duplicates) remain in history but release the slot.
fn operation_owns_slot(
    state: &str,
    result: &Value,
    intent: &PublicationIntent,
    current_epoch: i64,
) -> bool {
    match state {
        "queued" => intent.admitted_gm_epoch == current_epoch,
        "sending" | "outcome_unknown" => true,
        "cancelled" | "rejected" => false,
        "settled" => match result["outcome"].as_str() {
            Some("applied" | "unknown") => true,
            Some("coalesced" | "stale_gm_epoch") => false,
            Some("failed")
                if result["publication"] == "not_started"
                    || result["error"]["code"] == "FORGE_PUSH_REJECTED"
                    || result["publication_may_have_started"] == false =>
            {
                false
            }
            _ => result["publication_may_have_started"] != false,
        },
        // Unexpected retained states fail closed rather than releasing a
        // possibly effectful slot.
        _ => true,
    }
}

fn same_effect_slot(left: &PublicationIntent, right: &PublicationIntent) -> bool {
    left.project_id == right.project_id
        && left.canonical_repository == right.canonical_repository
        && left.candidate_ref == right.candidate_ref
        && left.candidate_sha256 == right.candidate_sha256
        && left.commit == right.commit
        && left.tree == right.tree
        && left.target_ref == right.target_ref
        && left.expected_old_ref == right.expected_old_ref
        && left.expected_create == right.expected_create
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

fn settle_queued_coalesced(
    tx: &Transaction<'_>,
    operation_id: &str,
    owner_operation_id: &str,
) -> Result<()> {
    let now = model::now_ms()?;
    let result = coalesced_receipt(operation_id, owner_operation_id);
    tx.execute(
        "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND method='forge.publish_ref' AND state='queued'",
        params![operation_id, model::canonical(&result)?, now],
    )?;
    super::capacity::sync_operation(tx, operation_id, now)?;
    tx.execute(
        "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:forge',?1,?1,'forge.publication',?2,?3)",
        params![
            format!("coalesced:{operation_id}:{owner_operation_id}"),
            model::canonical(&result)?,
            now
        ],
    )?;
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

/// Revalidate the exact current accepted candidate and current direct-write
/// authority behind a retained successful publication before allowing a
/// dependent PR effect. The publication's caller and GM epoch remain historical
/// provenance; they do not grant or deny this new manual Operation.
pub(super) fn applied_publication_context(
    db: &Connection,
    principal: &Principal,
    operation_id: &str,
    config: &Config,
) -> Result<(PublicationIntent, String)> {
    let current = current_principal(db, principal.clone())?;
    let actor = ForgeActor::Direct {
        client_id: current.client_id.clone(),
    };
    let operation = operations::get_operation(db, operation_id)?;
    if operation["method"] != "forge.publish_ref"
        || operation["state"] != "settled"
        || operation["result"]["outcome"] != "applied"
        || operation["result"]["publication"] != "confirmed_by_remote_readback"
        || operation["result"]["acceptance_current_at_finish"] != true
        || operation["result"]["remote_ref"]["present"] != true
    {
        return Err(Error::new(
            "GITHUB_PR_PUBLICATION_REQUIRED",
            "PR metadata updates require an exactly applied accepted-candidate publication",
        ));
    }

    let saved = saved_intent(db, operation_id)?;
    let request = request(db, operation_id)?;
    let (mut current_intent, _, _) = accepted_candidate(db, &actor, &request, &config.forge)?;
    current_intent.operation_id = operation_id.to_owned();
    // accepted_candidate validates current operator/GM authority and the live
    // Task, Attempt, acceptance, submission, candidate, project policy and
    // repository. The retained epoch records who authorized the historical
    // publication; normalize only this comparison field so a successor's
    // current epoch does not invalidate the historical proof.
    let mut comparable_intent = current_intent;
    comparable_intent.admitted_gm_epoch = saved.admitted_gm_epoch;
    let attempt = tasks::get_attempt(db, &saved.attempt_id)?;
    let task_id = model::text(&attempt, "task_id")?.to_owned();
    if comparable_intent != saved
        || operation["task_id"] != task_id
        || operation["attempt_id"] != saved.attempt_id
        || operation["result"]["remote_ref"]["commit"] != saved.commit
    {
        return Err(Error::new(
            "GITHUB_PR_PUBLICATION_STALE",
            "the current accepted Task, policy, repository or published commit differs from the retained publication",
        ));
    }
    Ok((saved, task_id))
}

fn settle_before_write(tx: &Transaction<'_>, id: &str, error: Error) -> Result<()> {
    let now = model::now_ms()?;
    let result = json!({
        "operation_id":id,
        "outcome":"failed",
        "publication":"not_started",
        "publication_may_have_started":false,
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
    let target = ForgeTargetKey::from_intent(&intent)?;
    let lane = target_lane(&target);
    Ok(ForgeWork {
        intent,
        lane,
        project,
        candidate,
        mode,
        transfer_continuation: None,
    })
}

fn begin(db: &mut Connection, id: &str, config: &Config) -> Result<Option<ForgeWork>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let operation = operations::get_operation(&tx, id)?;
    if operation["method"] != "forge.publish_ref" {
        return Err(Error::new(
            "FORBIDDEN",
            "Operation is not a Forge publication",
        ));
    }
    let actor = actor_for_operation(&tx, id, &operation)?;
    if operation["caller_id"] != actor.caller_id() {
        return Err(Error::new(
            "FORGE_INTENT_INVALID",
            "publication Operation caller differs from its retained actor",
        ));
    }
    match operation["state"].as_str() {
        Some("queued") => {
            let saved = saved_intent(&tx, id)?;
            actor.require_intent_matches(&saved)?;
            let transfer_continuation = actor.transfer_continuation(&tx)?;
            let current_epoch = current_gm_epoch(&tx)?;
            if saved.admitted_gm_epoch != current_epoch && transfer_continuation.is_none() {
                settle_stale_gm_epoch(&tx, id, saved.admitted_gm_epoch, current_epoch)?;
                tx.commit()?;
                return Ok(None);
            }
            if let Some(owner_operation_id) = exact_slot_owner(&tx, id, &saved)? {
                settle_queued_coalesced(&tx, id, &owner_operation_id)?;
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
                let input = request(&tx, id)?;
                let (mut expected, _, _) = accepted_candidate(&tx, &actor, &input, &config.forge)?;
                expected.operation_id = id.to_owned();
                if expected != saved {
                    return Err(Error::new(
                        "FORGE_INTENT_CHANGED",
                        "current accepted candidate differs from saved publication intent",
                    ));
                }
                work_from_saved(&tx, saved, &config.forge, WorkMode::PushOnce)
            })();
            match start {
                Ok(mut work) => {
                    work.transfer_continuation = transfer_continuation;
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
            let saved = saved_intent(&tx, id)?;
            actor.require_intent_matches(&saved)?;
            if matches!(actor, ForgeActor::OnBehalf(_))
                && actor.require_readback_authority(&tx, config).is_err()
            {
                tx.commit()?;
                return Ok(None);
            }
            let work = match work_from_saved(&tx, saved, &config.forge, WorkMode::ReadbackOnly) {
                Ok(work) => work,
                Err(_) if matches!(actor, ForgeActor::OnBehalf(_)) => {
                    tx.commit()?;
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            tx.commit()?;
            Ok(Some(work))
        }
        _ => Ok(None),
    }
}

/// Convert an abandoned in-process send to unknown only after the caller has
/// acquired the Git process slot, proving its owned command closure has ended.
/// This path is strictly readback-only.
fn reconcile_in_transaction(
    tx: &Transaction<'_>,
    actor: &ForgeActor,
    id: &str,
    forge_config: &ForgeConfig,
    full_config: Option<&Config>,
) -> Result<Option<ForgeWork>> {
    let operation = operations::get_operation(tx, id)?;
    if operation["method"] != "forge.publish_ref" || operation["caller_id"] != actor.caller_id() {
        return Err(Error::new(
            "FORBIDDEN",
            "publication actor differs from its retained Operation",
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
    let saved = saved_intent(tx, id)?;
    actor.require_intent_matches(&saved)?;
    if matches!(actor, ForgeActor::OnBehalf(_)) {
        let Some(config) = full_config else {
            return Ok(None);
        };
        if actor.require_readback_authority(tx, config).is_err() {
            return Ok(None);
        }
        let work = match work_from_saved(tx, saved, forge_config, WorkMode::ReadbackOnly) {
            Ok(work) => work,
            Err(_) => return Ok(None),
        };
        Ok(Some(work))
    } else {
        Ok(Some(work_from_saved(
            tx,
            saved,
            forge_config,
            WorkMode::ReadbackOnly,
        )?))
    }
}

fn begin_reconciliation_operation(
    db: &mut Connection,
    id: &str,
    config: &Config,
) -> Result<Option<ForgeWork>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let operation = operations::get_operation(&tx, id)?;
    let actor = actor_for_operation(&tx, id, &operation)?;
    let work = reconcile_in_transaction(&tx, &actor, id, &config.forge, Some(config))?;
    tx.commit()?;
    Ok(work)
}

// Kept for the existing direct-caller fixture and manual recovery behavior.
#[cfg(test)]
fn begin_reconciliation(
    db: &mut Connection,
    principal: Principal,
    id: &str,
    config: &ForgeConfig,
) -> Result<Option<ForgeWork>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let actor = ForgeActor::Direct {
        client_id: principal.client_id,
    };
    let work = reconcile_in_transaction(&tx, &actor, id, config, None)?;
    tx.commit()?;
    Ok(work)
}

fn dispatch_authorized(
    db: &Connection,
    work: &ForgeWork,
    config: &Config,
    worker_job: &NativeWorkerJob,
    owner_record: &Value,
) -> Result<DispatchAuthorization> {
    let operation = operations::get_operation(db, &work.intent.operation_id)?;
    let actor = actor_for_operation(db, &work.intent.operation_id, &operation)?;
    if operation["method"] != "forge.publish_ref"
        || operation["state"] != "sending"
        || operation["caller_id"] != actor.caller_id()
    {
        return Err(Error::conflict(
            "publication is no longer in its pre-write phase",
        ));
    }
    let saved = saved_intent(db, &work.intent.operation_id)?;
    actor.require_intent_matches(&saved)?;
    if saved != work.intent {
        return Err(Error::new(
            "FORGE_INTENT_CHANGED",
            "saved publication intent differs from its authorized work item",
        ));
    }
    require_no_process_tree_hold(db, &saved.canonical_repository)?;
    if let Some(owner_operation_id) = exact_slot_owner(db, &work.intent.operation_id, &saved)? {
        return Ok(DispatchAuthorization::Coalesced { owner_operation_id });
    }
    let admitted_gm_epoch = saved.admitted_gm_epoch;
    let current_epoch = current_gm_epoch(db)?;
    if admitted_gm_epoch != current_epoch && work.transfer_continuation.is_none() {
        return Ok(DispatchAuthorization::StaleGmEpoch {
            admitted_gm_epoch,
            current_gm_epoch: current_epoch,
        });
    }
    let input = request(db, &work.intent.operation_id)?;
    let (expected, project, candidate) = accepted_candidate_with_transfer(
        db,
        &actor,
        &input,
        &config.forge,
        work.transfer_continuation.as_ref(),
    )?;
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
    if let Some(continuation) = work.transfer_continuation.as_ref() {
        actor.require_prepared_transfer_write_authority(db, continuation)?;
    } else {
        let final_epoch = current_gm_epoch(db)?;
        if admitted_gm_epoch != final_epoch {
            return Ok(DispatchAuthorization::StaleGmEpoch {
                admitted_gm_epoch,
                current_gm_epoch: final_epoch,
            });
        }
    }
    let now = model::now_ms()?;
    let worker_receipt = json!({
        "version":1,
        "kind":worker_job.kind,
        "job_id":worker_job.job_id,
        "operation_id":worker_job.operation_id,
        "phase":worker_job.phase_name,
        "owner_token":worker_job.owner_token,
        "plan_sha256":worker_job.plan_sha256,
        "owner":owner_record
    });
    let changed = db.execute(
        "UPDATE operations SET result_json=json_remove(json_set(COALESCE(result_json,'{}'),'$.publication_may_have_started',json('true'),'$.native_worker',json(?2)),'$.native_worker_result','$.native_worker_recovery'),updated_at_ms=?3 WHERE operation_id=?1 AND method='forge.publish_ref' AND state='sending'",
        params![
            &work.intent.operation_id,
            model::canonical(&worker_receipt)?,
            now
        ],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "publication left its pre-write phase before the write boundary",
        ));
    }
    Ok(DispatchAuthorization::Authorized)
}

fn forge_worker_executable() -> Result<PathBuf> {
    let host = std::env::current_exe()?.canonicalize()?;
    let name = if cfg!(windows) {
        "swarm-forge-worker.exe"
    } else {
        "swarm-forge-worker"
    };
    let candidate = host.with_file_name(name);
    reject_forge_link(&candidate)?;
    let executable = candidate.canonicalize()?;
    if !executable.is_file() {
        return Err(Error::new(
            "FORGE_WORKER_UNAVAILABLE",
            "the installed Forge worker is not a regular executable",
        ));
    }
    Ok(executable)
}

fn reject_forge_link(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(Error::new(
            "FORGE_WORKER_PATH_INVALID",
            "Forge worker files and directories cannot be symbolic links",
        ));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x400 != 0 {
            return Err(Error::new(
                "FORGE_WORKER_PATH_INVALID",
                "Forge worker files and directories cannot be reparse points",
            ));
        }
    }
    Ok(())
}

fn ensure_private_forge_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            reject_forge_link(path)?;
            if !metadata.is_dir() {
                return Err(Error::new(
                    "FORGE_WORKER_PATH_INVALID",
                    "Forge worker state path is not a directory",
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => match fs::create_dir(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(path)?;
                reject_forge_link(path)?;
                if !metadata.is_dir() {
                    return Err(Error::new(
                        "FORGE_WORKER_PATH_INVALID",
                        "Forge worker state path is not a directory",
                    ));
                }
            }
            Err(error) => return Err(error.into()),
        },
        Err(error) => return Err(error.into()),
    }
    crate::platform::private_permissions(path, true)
}

fn hash_bounded_file(path: &Path, max_bytes: u64) -> Result<String> {
    let file = fs::File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > max_bytes {
        return Err(Error::new(
            "FORGE_WORKER_IMAGE_INVALID",
            "Forge worker image exceeds its bounded file size",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(max_bytes + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(Error::new(
            "FORGE_WORKER_IMAGE_INVALID",
            "Forge worker image exceeds its bounded file size",
        ));
    }
    Ok(model::digest(&bytes))
}

fn create_forge_worker_job(
    data_dir: &Path,
    work: &ForgeWork,
    config: &ForgeConfig,
    push_endpoint: Option<&str>,
) -> Result<NativeWorkerJob> {
    if work.mode == WorkMode::PushOnce && push_endpoint.is_none()
        || work.mode == WorkMode::ReadbackOnly && push_endpoint.is_some()
    {
        return Err(Error::invalid(
            "Forge worker phase and push endpoint do not match",
        ));
    }
    let data_dir = data_dir.canonicalize()?;
    let jobs_root = data_dir.join("forge-worker-runs");
    ensure_private_forge_directory(&jobs_root)?;
    let jobs_root = jobs_root.canonicalize()?;
    if !jobs_root.starts_with(&data_dir) {
        return Err(Error::new(
            "FORGE_WORKER_PATH_INVALID",
            "Forge worker state directory escaped the DataRoot",
        ));
    }
    let job_id = model::new_id();
    let directory = jobs_root.join(&job_id);
    fs::create_dir(&directory)?;
    crate::platform::private_permissions(&directory, true)?;
    let directory = directory.canonicalize()?;
    if !directory.starts_with(&jobs_root) {
        return Err(Error::new(
            "FORGE_WORKER_PATH_INVALID",
            "Forge worker job directory escaped its private root",
        ));
    }
    let owner_token = model::new_id();
    let executable = forge_worker_executable()?;
    let executable_sha256 = hash_bounded_file(&executable, 128 * 1024 * 1024)?;
    let phase = match work.mode {
        WorkMode::PushOnce => "push_once",
        WorkMode::ReadbackOnly => "readback_only",
    };
    let plan = json!({
        "schema_version":1,
        "kind":"forge_publish",
        "job_id":job_id,
        "operation_id":work.intent.operation_id,
        "owner_token":owner_token,
        "phase":phase,
        "intent":work.intent,
        "git_executable":config.git_executable,
        "timeout_seconds":config.timeout_seconds,
        "max_output_bytes":config.max_output_bytes,
        "project":work.project,
        "push_endpoint":push_endpoint
    });
    let plan_bytes = serde_json::to_vec(&plan)?;
    if plan_bytes.len() > 1_048_576 {
        return Err(Error::invalid("Forge worker plan exceeds its envelope"));
    }
    let plan_sha256 = model::digest(&plan_bytes);
    let plan_path = directory.join("plan.json");
    let owner_path = directory.join("owner.json");
    let authorization_path = directory.join("authorization.json");
    let result_path = directory.join("result.json");
    crate::platform::write_private_new(&plan_path, &plan_bytes)?;
    Ok(NativeWorkerJob {
        kind: "forge_publish",
        phase_name: phase,
        job_id,
        operation_id: work.intent.operation_id.clone(),
        owner_token,
        plan_sha256,
        directory,
        plan_path,
        owner_path,
        authorization_path,
        result_path,
        executable,
        executable_sha256,
        timeout_seconds: config.timeout_seconds,
    })
}

fn github_cli_executable() -> Result<PathBuf> {
    let path = std::env::var_os("PATH").ok_or_else(|| {
        Error::new(
            "GITHUB_CLI_UNAVAILABLE",
            "the configured GitHub CLI could not be resolved",
        )
    })?;
    let name = if cfg!(windows) { "gh.exe" } else { "gh" };
    for directory in std::env::split_paths(&path) {
        let candidate = directory.join(name);
        let Ok(metadata) = fs::symlink_metadata(&candidate) else {
            continue;
        };
        if metadata.is_file() {
            reject_forge_link(&candidate)?;
            return candidate.canonicalize().map_err(Into::into);
        }
    }
    Err(Error::new(
        "GITHUB_CLI_UNAVAILABLE",
        "the configured GitHub CLI could not be resolved",
    ))
}

pub(super) struct GitHubDescriptionWorkerRequest {
    pub(super) operation_id: String,
    pub(super) host: String,
    pub(super) owner: String,
    pub(super) repository: String,
    pub(super) pull_request_number: i64,
    pub(super) title: String,
    pub(super) body: String,
}

fn create_github_description_worker_job(
    data_dir: &Path,
    request: &GitHubDescriptionWorkerRequest,
) -> Result<NativeWorkerJob> {
    let data_dir = data_dir.canonicalize()?;
    let jobs_root = data_dir.join("forge-worker-runs");
    ensure_private_forge_directory(&jobs_root)?;
    let jobs_root = jobs_root.canonicalize()?;
    if !jobs_root.starts_with(&data_dir) {
        return Err(Error::new(
            "FORGE_WORKER_PATH_INVALID",
            "native worker state directory escaped the DataRoot",
        ));
    }
    let job_id = model::new_id();
    let directory = jobs_root.join(&job_id);
    fs::create_dir(&directory)?;
    crate::platform::private_permissions(&directory, true)?;
    let directory = directory.canonicalize()?;
    if !directory.starts_with(&jobs_root) {
        return Err(Error::new(
            "FORGE_WORKER_PATH_INVALID",
            "native worker job directory escaped its private root",
        ));
    }
    let owner_token = model::new_id();
    let executable = forge_worker_executable()?;
    let executable_sha256 = hash_bounded_file(&executable, 128 * 1024 * 1024)?;
    let gh_executable = github_cli_executable()?;
    let gh_executable_sha256 = hash_bounded_file(&gh_executable, 128 * 1024 * 1024)?;
    let timeout_seconds = 45;
    let max_output_bytes = 4 * 1024 * 1024;
    let plan = json!({
        "schema_version":1,
        "kind":"github_pr_description",
        "job_id":job_id,
        "operation_id":request.operation_id.as_str(),
        "owner_token":owner_token,
        "phase":"patch_once",
        "gh_executable":gh_executable,
        "gh_executable_sha256":gh_executable_sha256,
        "timeout_seconds":timeout_seconds,
        "max_output_bytes":max_output_bytes,
        "host":request.host.as_str(),
        "owner":request.owner.as_str(),
        "repository":request.repository.as_str(),
        "pull_request_number":request.pull_request_number,
        "title":request.title.as_str(),
        "body":request.body.as_str()
    });
    let plan_bytes = serde_json::to_vec(&plan)?;
    if plan_bytes.len() > 1_048_576 {
        return Err(Error::invalid("GitHub worker plan exceeds its envelope"));
    }
    let plan_sha256 = model::digest(&plan_bytes);
    let plan_path = directory.join("plan.json");
    let owner_path = directory.join("owner.json");
    let authorization_path = directory.join("authorization.json");
    let result_path = directory.join("result.json");
    crate::platform::write_private_new(&plan_path, &plan_bytes)?;
    Ok(NativeWorkerJob {
        kind: "github_pr_description",
        phase_name: "patch_once",
        job_id,
        operation_id: request.operation_id.clone(),
        owner_token,
        plan_sha256,
        directory,
        plan_path,
        owner_path,
        authorization_path,
        result_path,
        executable,
        executable_sha256,
        timeout_seconds,
    })
}

fn read_worker_bytes(path: &Path, max_bytes: u64) -> Result<Vec<u8>> {
    reject_forge_link(path)?;
    let file = fs::File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > max_bytes {
        return Err(Error::new(
            "FORGE_WORKER_EVIDENCE_INVALID",
            "retained Forge worker evidence exceeds its bounded envelope",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(max_bytes + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(Error::new(
            "FORGE_WORKER_EVIDENCE_INVALID",
            "retained Forge worker evidence exceeds its bounded envelope",
        ));
    }
    Ok(bytes)
}

fn read_worker_bytes_optional(path: &Path, max_bytes: u64) -> Result<RetainedWorkerFile> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RetainedWorkerFile::Missing);
        }
        Err(error) => return Err(error.into()),
    };
    reject_forge_link(path)?;
    if !metadata.is_file() {
        return Err(Error::new(
            "FORGE_WORKER_EVIDENCE_INVALID",
            "retained Forge worker result is not a regular private file",
        ));
    }
    if metadata.len() > max_bytes {
        return Ok(RetainedWorkerFile::OverLimit);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    match fs::File::open(path) {
        Ok(file) => file.take(max_bytes + 1).read_to_end(&mut bytes)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RetainedWorkerFile::Missing);
        }
        Err(error) => return Err(error.into()),
    };
    if bytes.len() as u64 > max_bytes {
        return Ok(RetainedWorkerFile::OverLimit);
    }
    Ok(RetainedWorkerFile::Bytes(bytes))
}

fn read_worker_json(path: &Path, max_bytes: u64) -> Result<Value> {
    reject_forge_link(path)?;
    let file = fs::File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > max_bytes {
        return Err(Error::invalid("Forge worker receipt exceeds its envelope"));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(max_bytes + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(Error::invalid("Forge worker receipt exceeds its envelope"));
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn validate_worker_owner(job: &NativeWorkerJob, pid: u32, owner: &Value) -> Result<()> {
    let fields = owner
        .as_object()
        .ok_or_else(|| Error::invalid("Forge worker owner record is invalid"))?;
    if fields.len() != 3
        || !fields.contains_key("version")
        || !fields.contains_key("token")
        || !fields.contains_key("process")
        || owner["version"] != 1
        || owner["token"] != job.owner_token
        || owner["process"]["purpose"] != "module"
        || owner["process"]["pid"].as_u64() != Some(u64::from(pid))
    {
        return Err(Error::new(
            "FORGE_WORKER_OWNER_INVALID",
            "Forge worker owner receipt did not match its direct process",
        ));
    }
    let image = swarm_process::process_image_identity(pid)?;
    let image_path = image
        .get("image_path")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .ok_or_else(|| Error::new("FORGE_WORKER_OWNER_INVALID", "worker image is absent"))?
        .canonicalize()?;
    if image_path != job.executable
        || image.get("image_sha256").and_then(Value::as_str) != Some(job.executable_sha256.as_str())
    {
        return Err(Error::new(
            "FORGE_WORKER_OWNER_INVALID",
            "Forge worker image differs from the installed executable",
        ));
    }
    #[cfg(windows)]
    {
        let recorded_birth = owner["process"]["creation_filetime"]
            .as_u64()
            .ok_or_else(|| Error::invalid("worker owner birth identity is invalid"))?;
        let observed_birth = image["creation_filetime"]
            .as_str()
            .and_then(|value| value.parse::<u64>().ok());
        if observed_birth != Some(recorded_birth) {
            return Err(Error::new(
                "FORGE_WORKER_OWNER_INVALID",
                "Forge worker process birth differs from its owner receipt",
            ));
        }
    }
    #[cfg(target_os = "linux")]
    {
        if owner["process"]["start_ticks"] != image["start_ticks"]
            || owner["process"]["boot_id"] != image["boot_id"]
            || owner["process"]["pgid"].as_i64() != Some(i64::from(pid))
        {
            return Err(Error::new(
                "FORGE_WORKER_OWNER_INVALID",
                "Forge worker process birth differs from its owner receipt",
            ));
        }
    }
    Ok(())
}

async fn wait_worker_owner(child: &mut TokioChild, job: &NativeWorkerJob) -> Result<Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if job.owner_path.try_exists()? {
            let owner = read_worker_json(&job.owner_path, 65_536)?;
            let pid = child
                .id()
                .ok_or_else(|| Error::new("FORGE_WORKER_OWNER_INVALID", "worker PID is absent"))?;
            let expected = job.clone();
            let owner = tokio::task::spawn_blocking(move || -> Result<Value> {
                validate_worker_owner(&expected, pid, &owner)?;
                Ok(owner)
            })
            .await
            .map_err(|_| {
                Error::new(
                    "FORGE_WORKER_OWNER_INVALID",
                    "worker image identity check did not complete",
                )
            })??;
            return Ok(owner);
        }
        if child.try_wait()?.is_some() {
            return Err(Error::new(
                "FORGE_WORKER_START_FAILED",
                "Forge worker exited before publishing its owner receipt",
            ));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::new(
                "FORGE_WORKER_START_UNCONFIRMED",
                "Forge worker owner receipt did not arrive within its startup bound",
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn native_worker_receipt(job: &NativeWorkerJob, owner: &Value) -> Value {
    json!({
        "version":1,
        "kind":job.kind,
        "job_id":job.job_id,
        "operation_id":job.operation_id,
        "phase":job.phase_name,
        "owner_token":job.owner_token,
        "plan_sha256":job.plan_sha256,
        "owner":owner
    })
}

fn write_worker_authorization(job: &NativeWorkerJob, authorized: bool) -> Result<()> {
    let authorization = json!({
        "schema_version":1,
        "kind":job.kind,
        "job_id":job.job_id,
        "operation_id":job.operation_id,
        "owner_token":job.owner_token,
        "plan_sha256":job.plan_sha256,
        "phase":job.phase_name,
        "authorized":authorized
    });
    crate::platform::write_private_new(
        &job.authorization_path,
        &serde_json::to_vec(&authorization)?,
    )
}

async fn wait_worker_exit(
    child: &mut TokioChild,
    timeout: Duration,
) -> Result<std::process::ExitStatus> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(None) => {
                return Err(Error::new(
                    "FORGE_GIT_TREE_TERMINATION",
                    "native worker did not exit within its bounded wait",
                ));
            }
            Err(_) => {
                return Err(Error::new(
                    "FORGE_GIT_TREE_TERMINATION",
                    "native worker exit status could not be confirmed",
                ));
            }
        }
    }
}

async fn wait_forge_worker(run: NativeWorkerRun, expected_authorized: bool) -> Result<Value> {
    let NativeWorkerRun {
        job,
        mut child,
        owner_record,
        _process_permit,
    } = run;
    let status = wait_worker_exit(
        &mut child,
        Duration::from_secs(job.timeout_seconds.saturating_add(60)),
    )
    .await?;
    let owner_process = owner_record["process"].clone();
    let token = job.owner_token.clone();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let process = owner_process.clone();
        let token = token.clone();
        let departed = match tokio::task::spawn_blocking(move || {
            swarm_process::departed_empty(&process, &token)
        })
        .await
        {
            Ok(Ok(departed)) => departed,
            _ => {
                return Err(Error::new(
                    "FORGE_GIT_TREE_TERMINATION",
                    "Forge worker family departure check could not be confirmed",
                ));
            }
        };
        if departed {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::new(
                "FORGE_GIT_TREE_TERMINATION",
                "Forge worker process family did not depart within its bounded wait",
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if !status.success() {
        return Err(Error::new(
            "FORGE_WORKER_EXITED",
            "Forge worker exited without a successful bounded result",
        ));
    }
    let result = read_worker_json(&job.result_path, 262_144)?;
    let fields = result
        .as_object()
        .ok_or_else(|| Error::new("FORGE_WORKER_RESULT_INVALID", "worker result is invalid"))?;
    const RESULT_FIELDS: [&str; 17] = [
        "schema_version",
        "kind",
        "job_id",
        "operation_id",
        "owner_token",
        "plan_sha256",
        "phase",
        "authorized",
        "outcome",
        "reason",
        "remote_ref",
        "push_exit_code",
        "timed_out",
        "stderr_sha256",
        "stderr_bytes",
        "process_tree_empty",
        "process_tree_unconfirmed",
    ];
    if fields.len() != RESULT_FIELDS.len()
        || RESULT_FIELDS
            .iter()
            .any(|field| !fields.contains_key(*field))
        || result["schema_version"] != 1
        || result["kind"] != job.kind
        || result["job_id"] != job.job_id
        || result["operation_id"] != job.operation_id
        || result["owner_token"] != job.owner_token
        || result["plan_sha256"] != job.plan_sha256
        || result["phase"] != job.phase_name
        || result["authorized"].as_bool() != Some(expected_authorized)
    {
        return Err(Error::new(
            "FORGE_WORKER_RESULT_INVALID",
            "Forge worker result did not match its exact authorized job",
        ));
    }
    if result["process_tree_empty"] != true
        || result["process_tree_unconfirmed"].as_bool() != Some(false)
    {
        return Err(Error::new(
            "FORGE_GIT_TREE_TERMINATION",
            "Forge worker reported unconfirmed native process departure",
        ));
    }
    Ok(result)
}

fn valid_worker_uuid(value: &str) -> bool {
    value.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| value.as_bytes()[index] == b'-')
        && value
            .bytes()
            .enumerate()
            .all(|(index, byte)| [8, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit())
}

fn validate_worker_result_scope(operation: &Value, receipt: &Value, result: &Value) -> Result<()> {
    for (field, expected) in [
        ("kind", &receipt["kind"]),
        ("job_id", &receipt["job_id"]),
        ("operation_id", &operation["operation_id"]),
        ("owner_token", &receipt["owner_token"]),
        ("plan_sha256", &receipt["plan_sha256"]),
        ("phase", &receipt["phase"]),
    ] {
        if let Some(actual) = result.get(field)
            && actual != expected
        {
            return Err(Error::new(
                "FORGE_WORKER_RESULT_SCOPE_MISMATCH",
                "retained worker result identifies a different operation or process owner",
            ));
        }
    }
    Ok(())
}

fn classify_worker_result(
    operation: &Value,
    receipt: &Value,
    retained: RetainedWorkerFile,
) -> Result<WorkerResultStatus> {
    const RESULT_FIELDS: [&str; 17] = [
        "schema_version",
        "kind",
        "job_id",
        "operation_id",
        "owner_token",
        "plan_sha256",
        "phase",
        "authorized",
        "outcome",
        "reason",
        "remote_ref",
        "push_exit_code",
        "timed_out",
        "stderr_sha256",
        "stderr_bytes",
        "process_tree_empty",
        "process_tree_unconfirmed",
    ];
    let stored = operation["result"].get("native_worker_result");
    if let Some(stored_result) = stored
        && stored_result.is_object()
    {
        validate_worker_result_scope(operation, receipt, stored_result)?;
    } else if stored.is_some() {
        return Ok(WorkerResultStatus::Malformed);
    }
    let bytes = match retained {
        RetainedWorkerFile::Missing => return Ok(WorkerResultStatus::Missing),
        RetainedWorkerFile::OverLimit => return Ok(WorkerResultStatus::Malformed),
        RetainedWorkerFile::Bytes(bytes) => bytes,
    };
    let result: Value = match serde_json::from_slice(&bytes) {
        Ok(result) => result,
        Err(_) => return Ok(WorkerResultStatus::Malformed),
    };
    let Some(fields) = result.as_object() else {
        return Ok(WorkerResultStatus::Malformed);
    };
    validate_worker_result_scope(operation, receipt, &result)?;
    if let Some(stored_result) = stored
        && stored_result.is_object()
        && model::canonical(stored_result)? != model::canonical(&result)?
    {
        return Ok(WorkerResultStatus::Malformed);
    }
    let kind = receipt["kind"].as_str().unwrap_or_default();
    let reason = result.get("reason").and_then(Value::as_str).unwrap_or("");
    let authorized = result.get("authorized").and_then(Value::as_bool);
    let outcome_valid = match (kind, authorized) {
        ("github_pr_description", Some(true)) => {
            result["outcome"] == "unknown"
                && matches!(
                    reason,
                    "github_write_completed_readback_required"
                        | "github_cli_failed_readback_required"
                        | "github_cli_timed_out"
                        | "github_request_body_delivery_failed"
                        | "github_cli_unavailable"
                        | "github_cli_image_changed"
                )
        }
        ("github_pr_description", Some(false)) => {
            (result["outcome"] == "not_authorized" && reason == "authorization_denied")
                || (result["outcome"] == "unknown" && reason == "authorization_not_received")
        }
        ("forge_publish", Some(true)) => {
            matches!(
                result["outcome"].as_str(),
                Some("applied" | "failed" | "unknown")
            )
        }
        ("forge_publish", Some(false)) => {
            (result["outcome"] == "not_authorized" && reason == "authorization_denied")
                || (result["outcome"] == "unknown" && reason == "authorization_not_received")
        }
        _ => false,
    };
    let ref_valid = match kind {
        "github_pr_description" => result["remote_ref"].is_null(),
        "forge_publish" => {
            result["remote_ref"].is_null() || worker_ref_readback(&result["remote_ref"]).is_some()
        }
        _ => false,
    };
    let phase_valid = match kind {
        "github_pr_description" => receipt["phase"] == "patch_once",
        "forge_publish" => matches!(
            receipt["phase"].as_str(),
            Some("push_once" | "readback_only")
        ),
        _ => false,
    };
    let result_matches = fields.len() == RESULT_FIELDS.len()
        && RESULT_FIELDS
            .iter()
            .all(|field| fields.contains_key(*field))
        && result["schema_version"] == 1
        && result["kind"] == receipt["kind"]
        && result["job_id"] == receipt["job_id"]
        && result["operation_id"] == operation["operation_id"]
        && result["owner_token"] == receipt["owner_token"]
        && result["plan_sha256"] == receipt["plan_sha256"]
        && result["phase"] == receipt["phase"]
        && phase_valid
        && outcome_valid
        && ref_valid
        && result["process_tree_empty"].is_boolean()
        && result["process_tree_unconfirmed"].is_boolean()
        && result["timed_out"].is_boolean()
        && (result["stderr_sha256"].is_null()
            || result["stderr_sha256"]
                .as_str()
                .is_some_and(valid_worker_digest))
        && (result["stderr_bytes"].is_null() || result["stderr_bytes"].is_u64())
        && (result["push_exit_code"].is_null() || result["push_exit_code"].is_i64());
    if result_matches {
        Ok(WorkerResultStatus::Present)
    } else {
        Ok(WorkerResultStatus::Malformed)
    }
}

fn worker_plan_scope_error() -> Error {
    Error::new(
        "FORGE_WORKER_PLAN_SCOPE_MISMATCH",
        "retained worker plan does not match its durable operation and worker receipt",
    )
}

fn validate_forge_worker_plan(
    db: &Connection,
    operation: &Value,
    receipt: &Value,
    plan: &Value,
) -> Result<()> {
    const PLAN_FIELDS: [&str; 12] = [
        "schema_version",
        "kind",
        "job_id",
        "operation_id",
        "owner_token",
        "phase",
        "intent",
        "git_executable",
        "timeout_seconds",
        "max_output_bytes",
        "project",
        "push_endpoint",
    ];
    const PROJECT_FIELDS: [&str; 5] = [
        "canonical_repository",
        "repository_path",
        "remote_name",
        "policy_revision",
        "target_refs",
    ];
    let fields = plan.as_object().ok_or_else(worker_plan_scope_error)?;
    let project_fields = plan["project"]
        .as_object()
        .ok_or_else(worker_plan_scope_error)?;
    let operation_id = operation["operation_id"]
        .as_str()
        .ok_or_else(worker_plan_scope_error)?;
    let intent = saved_intent(db, operation_id).map_err(|_| worker_plan_scope_error())?;
    let expected_intent = serde_json::to_value(&intent).map_err(|_| worker_plan_scope_error())?;
    let intent_matches = match (
        model::canonical(&plan["intent"]),
        model::canonical(&expected_intent),
    ) {
        (Ok(actual), Ok(expected)) => actual == expected,
        _ => false,
    };
    let expected_phase = receipt["phase"]
        .as_str()
        .ok_or_else(worker_plan_scope_error)?;
    let target_refs = plan["project"]["target_refs"]
        .as_array()
        .ok_or_else(worker_plan_scope_error)?;
    let phase_endpoint_valid = match expected_phase {
        "push_once" => plan["push_endpoint"].as_str().is_some(),
        "readback_only" => plan["push_endpoint"].is_null(),
        _ => false,
    };
    let plan_matches = operation["method"] == "forge.publish_ref"
        && matches!(
            operation["state"].as_str(),
            Some("sending" | "outcome_unknown")
        )
        && fields.len() == PLAN_FIELDS.len()
        && PLAN_FIELDS.iter().all(|field| fields.contains_key(*field))
        && project_fields.len() == PROJECT_FIELDS.len()
        && PROJECT_FIELDS
            .iter()
            .all(|field| project_fields.contains_key(*field))
        && plan["schema_version"] == 1
        && plan["kind"] == "forge_publish"
        && plan["job_id"] == receipt["job_id"]
        && plan["operation_id"] == operation_id
        && plan["owner_token"] == receipt["owner_token"]
        && plan["phase"] == receipt["phase"]
        && intent_matches
        && plan["git_executable"]
            .as_str()
            .is_some_and(|path| Path::new(path).is_absolute())
        && (1..=900).contains(&plan["timeout_seconds"].as_u64().unwrap_or_default())
        && (1..=256 * 1024).contains(&plan["max_output_bytes"].as_u64().unwrap_or_default())
        && plan["project"]["canonical_repository"] == intent.canonical_repository
        && plan["project"]["remote_name"] == intent.remote_name
        && plan["project"]["policy_revision"] == intent.policy_revision
        && plan["project"]["repository_path"]
            .as_str()
            .is_some_and(|path| Path::new(path).is_absolute())
        && target_refs
            .iter()
            .any(|target_ref| target_ref == &Value::String(intent.target_ref.clone()))
        && phase_endpoint_valid;
    if !plan_matches {
        return Err(worker_plan_scope_error());
    }
    Ok(())
}

fn worker_ref_readback(value: &Value) -> Option<RefReadback> {
    let fields = value.as_object()?;
    match value.get("present").and_then(Value::as_bool)? {
        false if fields.len() == 1 && fields.contains_key("present") => Some(RefReadback::Missing),
        true if fields.len() == 2 && fields.contains_key("present") => {
            let commit = value.get("commit").and_then(Value::as_str)?;
            if crate::forge::valid_object_id(commit) {
                Some(RefReadback::At(commit.to_ascii_lowercase()))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn map_forge_worker_result(
    job: &NativeWorkerJob,
    work: &ForgeWork,
    result: &Value,
) -> ForgeOutcome {
    let readback = worker_ref_readback(&result["remote_ref"]);
    let reason = result["reason"].as_str().unwrap_or_default();
    let timed_out = result["timed_out"].as_bool().unwrap_or(true);
    let push_exit_code = result["push_exit_code"]
        .as_i64()
        .and_then(|value| i32::try_from(value).ok());
    let stderr_digest = result["stderr_sha256"]
        .as_str()
        .filter(|value| valid_worker_digest(value))
        .map(str::to_owned);
    let stderr_bytes = result["stderr_bytes"].as_u64();
    if result["authorized"] != true
        || result["job_id"] != job.job_id
        || result["operation_id"] != work.intent.operation_id
        || result["plan_sha256"] != job.plan_sha256
    {
        return worker_unknown(
            "worker_result_invalid",
            readback,
            timed_out,
            stderr_digest,
            stderr_bytes,
            false,
        );
    }
    if result["process_tree_unconfirmed"] == true {
        return worker_unknown(
            "git_process_tree_unconfirmed",
            readback,
            timed_out,
            stderr_digest,
            stderr_bytes,
            true,
        );
    }
    match result["outcome"].as_str() {
        Some("applied")
            if readback
                .as_ref()
                .is_some_and(|value| value.matches_intent(&work.intent)) =>
        {
            ForgeOutcome::Applied {
                readback: readback.expect("validated exact candidate readback"),
                push_exit_code,
                timed_out,
                stderr_digest,
                stderr_bytes,
            }
        }
        Some("failed")
            if reason == "git_rejected_and_expected_ref_remains"
                && !timed_out
                && push_exit_code.is_some_and(|code| code != 0)
                && readback
                    .as_ref()
                    .is_some_and(|value| value.matches_expected(&work.intent)) =>
        {
            ForgeOutcome::Failed(Error::new(
                "FORGE_PUSH_REJECTED",
                "native Git rejected publication and exact remote ref remains unchanged",
            ))
        }
        _ => worker_unknown(
            worker_reason(reason),
            readback,
            timed_out,
            stderr_digest,
            stderr_bytes,
            false,
        ),
    }
}

fn worker_unknown(
    reason: &'static str,
    readback: Option<RefReadback>,
    timed_out: bool,
    stderr_digest: Option<String>,
    stderr_bytes: Option<u64>,
    process_tree_unconfirmed: bool,
) -> ForgeOutcome {
    ForgeOutcome::Unknown {
        reason,
        readback,
        timed_out,
        stderr_digest,
        stderr_bytes,
        process_tree_unconfirmed,
    }
}

fn worker_reason(reason: &str) -> &'static str {
    match reason {
        "authorization_not_received" | "authorization_denied" => "worker_authorization_missing",
        "trusted_remote_unavailable"
        | "trusted_remote_identity_mismatch"
        | "trusted_remote_configuration_unsafe_or_unavailable" => {
            "trusted_remote_unavailable_for_reconciliation"
        }
        "remote_ref_readback_invalid" => "remote_ref_readback_invalid",
        "expected_ref_changed_before_push" => "expected_ref_changed_before_push",
        "push_process_unobservable" => "push_worker_failed",
        "push_endpoint_changed_after_authorization" => "push_endpoint_changed_after_authorization",
        "readback_failed_after_push" => "readback_failed_after_push",
        "restart_readback_did_not_prove_publication" => {
            "restart_readback_did_not_prove_publication"
        }
        "push_outcome_not_confirmed_by_exact_readback" => {
            "push_outcome_not_confirmed_by_exact_readback"
        }
        _ => "worker_result_invalid",
    }
}

fn valid_worker_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
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

#[cfg(test)]
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
        ForgeOutcome::Coalesced { owner_operation_id } => {
            (coalesced_receipt(id, owner_operation_id), "settled")
        }
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
    if let Some(worker_receipt) = operation["result"].get("native_worker") {
        result["native_worker"] = worker_receipt.clone();
    }
    if let Some(recovery) = operation["result"].get("native_worker_recovery") {
        result["native_worker_recovery"] = recovery.clone();
    }
    preserve_process_tree_hold(&operation["result"], &mut result, &mut state);
    if state == "settled" && !matches!(outcome, ForgeOutcome::Coalesced { .. }) {
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

    fn seed_fairness_operation(
        db: &Connection,
        operation_id: &str,
        state: &str,
        created_at_ms: i64,
        target: Option<(&str, &str)>,
    ) {
        let effective = if let Some((repository, target_ref)) = target {
            let saved = PublicationIntent {
                operation_id: operation_id.to_owned(),
                canonical_repository: repository.to_owned(),
                target_ref: target_ref.to_owned(),
                ..intent()
            };
            json!({"publication_intent":saved})
        } else {
            // Valid JSON with an invalid retained intent exercises the real
            // selector's unroutable-row path without corrupting the Store.
            json!({"publication_intent":{"operation_id":operation_id}})
        };
        db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) VALUES(?1,'operator',?2,'forge.publish_ref','{}',?3,?4,0,?5,?5)",
            params![
                operation_id,
                format!("request-{operation_id}"),
                model::canonical(&effective).unwrap(),
                state,
                created_at_ms,
            ],
        )
        .unwrap();
    }

    #[test]
    fn real_pending_selection_rotates_held_targets_and_persists_each_phase_cursor() {
        let directory =
            std::env::temp_dir().join(format!("swarm-forge-fairness-{}", model::new_id()));
        std::fs::create_dir_all(&directory).unwrap();
        let database_path = directory.join("store.sqlite");
        let mut db = Connection::open(&database_path).unwrap();
        db.execute_batch(super::super::SCHEMA).unwrap();

        seed_fairness_operation(
            &db,
            "held-a-oldest",
            "outcome_unknown",
            1,
            Some(("github.com/owner/a", "refs/heads/main")),
        );
        seed_fairness_operation(
            &db,
            "held-a-next",
            "outcome_unknown",
            2,
            Some(("github.com/owner/a", "refs/heads/main")),
        );
        seed_fairness_operation(
            &db,
            "later-b",
            "outcome_unknown",
            3,
            Some(("github.com/owner/b", "refs/heads/main")),
        );
        seed_fairness_operation(
            &db,
            "queued-a",
            "queued",
            1,
            Some(("github.com/owner/a", "refs/heads/main")),
        );
        seed_fairness_operation(
            &db,
            "queued-b",
            "queued",
            2,
            Some(("github.com/owner/b", "refs/heads/main")),
        );

        let first_reconciliation = pending(&mut db, 1).unwrap();
        assert_eq!(first_reconciliation.len(), 1);
        assert_eq!(first_reconciliation[0].id, "held-a-oldest");
        assert_eq!(
            first_reconciliation[0].target.as_ref().unwrap().target_ref,
            "refs/heads/main"
        );
        assert_eq!(
            db.query_row::<String, _, _>(
                "SELECT state FROM operations WHERE operation_id='held-a-oldest'",
                [],
                |row| row.get(0),
            )
            .unwrap(),
            "outcome_unknown"
        );

        // The queue/reconcile cursors are independent. A first queue page
        // starts at its own head even though reconciliation has advanced.
        let first_dispatch = queued_pending(&mut db, 1).unwrap();
        assert_eq!(first_dispatch[0].id, "queued-a");
        drop(db);

        // Reopening the actual Store database proves the cursor is durable.
        let mut db = Connection::open(&database_path).unwrap();
        let second_reconciliation = pending(&mut db, 1).unwrap();
        assert_eq!(second_reconciliation[0].id, "later-b");
        let second_dispatch = queued_pending(&mut db, 1).unwrap();
        assert_eq!(second_dispatch[0].id, "queued-b");

        // Once the rotation reaches the end it wraps. The held row remains
        // unknown and retains the same target lane; the newer same-target row
        // is not selected ahead of it.
        let wrapped_reconciliation = pending(&mut db, 1).unwrap();
        assert_eq!(wrapped_reconciliation[0].id, "held-a-oldest");
        assert_eq!(
            db.query_row::<String, _, _>(
                "SELECT state FROM operations WHERE operation_id='held-a-oldest'",
                [],
                |row| row.get(0),
            )
            .unwrap(),
            "outcome_unknown"
        );
        drop(db);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn malformed_intent_keeps_its_error_and_does_not_poison_the_next_page() {
        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch(super::super::SCHEMA).unwrap();
        seed_fairness_operation(&db, "malformed-held", "outcome_unknown", 1, None);
        seed_fairness_operation(
            &db,
            "valid-later",
            "outcome_unknown",
            2,
            Some(("github.com/owner/z", "refs/heads/main")),
        );

        let malformed = pending(&mut db, 1).unwrap();
        assert_eq!(malformed[0].id, "malformed-held");
        assert!(malformed[0].target.is_none());
        assert!(saved_intent(&db, "malformed-held").is_err());
        assert_eq!(
            db.query_row::<String, _, _>(
                "SELECT state FROM operations WHERE operation_id='malformed-held'",
                [],
                |row| row.get(0),
            )
            .unwrap(),
            "outcome_unknown"
        );

        let valid = pending(&mut db, 1).unwrap();
        assert_eq!(valid[0].id, "valid-later");
        assert!(valid[0].target.is_some());
    }
}

#[derive(Debug, Clone)]
struct ForgePendingOperation {
    id: String,
    target: Option<ForgeTargetKey>,
}

#[derive(Debug, Clone)]
struct ForgePendingCursor {
    route_valid: i64,
    repository_key: String,
    target_ref: String,
    operation_id: String,
}

#[derive(Debug)]
struct ForgePendingRow {
    id: String,
    route_valid: bool,
    canonical_repository: String,
    repository_key: String,
    target_ref: String,
}

impl ForgePendingRow {
    fn cursor(&self) -> ForgePendingCursor {
        ForgePendingCursor {
            route_valid: i64::from(self.route_valid),
            repository_key: self.repository_key.clone(),
            target_ref: self.target_ref.clone(),
            operation_id: if self.route_valid {
                String::new()
            } else {
                self.id.clone()
            },
        }
    }
}

fn pending_cursor_key(reconciliation: bool) -> &'static str {
    if reconciliation {
        "forge:pending-cursor:v2:reconcile"
    } else {
        "forge:pending-cursor:v2:dispatch"
    }
}

fn read_pending_cursor(db: &Connection, key: &str) -> Result<Option<ForgePendingCursor>> {
    let raw = db
        .query_row("SELECT value_json FROM meta WHERE key=?1", [key], |row| {
            row.get::<_, String>(0)
        })
        .optional()?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let Ok(value) = serde_json::from_str::<Value>(&raw) else {
        // This is rebuildable routing metadata. Corruption resets only this
        // cursor; it does not alter or release any retained Operation.
        return Ok(None);
    };
    let Some(route_valid) = value["route_valid"].as_i64() else {
        return Ok(None);
    };
    let (Some(repository_key), Some(target_ref), Some(operation_id)) = (
        value["repository_key"].as_str(),
        value["target_ref"].as_str(),
        value["operation_id"].as_str(),
    ) else {
        return Ok(None);
    };
    if !matches!(route_valid, 0 | 1)
        || (route_valid == 0 && operation_id.is_empty())
        || (route_valid == 1 && (repository_key.is_empty() || target_ref.is_empty()))
    {
        return Ok(None);
    }
    Ok(Some(ForgePendingCursor {
        route_valid,
        repository_key: repository_key.to_owned(),
        target_ref: target_ref.to_owned(),
        operation_id: operation_id.to_owned(),
    }))
}

fn store_pending_cursor(db: &Connection, key: &str, cursor: &ForgePendingCursor) -> Result<()> {
    set_meta(
        db,
        key,
        &json!({
            "route_valid":cursor.route_valid,
            "repository_key":cursor.repository_key,
            "target_ref":cursor.target_ref,
            "operation_id":cursor.operation_id,
        }),
    )
}

#[derive(Clone, Copy)]
enum CursorSlice {
    After,
    Through,
}

fn collect_pending_rows(mut rows: rusqlite::Rows<'_>) -> Result<Vec<ForgePendingRow>> {
    let mut selected = Vec::new();
    while let Some(row) = rows.next()? {
        selected.push(ForgePendingRow {
            id: row.get(0)?,
            route_valid: row.get::<_, i64>(1)? == 1,
            canonical_repository: row.get(2)?,
            repository_key: row.get(3)?,
            target_ref: row.get(4)?,
        });
    }
    Ok(selected)
}

fn select_pending_rows(
    db: &Connection,
    reconciliation: bool,
    cursor: Option<&ForgePendingCursor>,
    slice: CursorSlice,
    limit: usize,
) -> Result<Vec<ForgePendingRow>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let state_filter = if reconciliation {
        "state IN ('sending','outcome_unknown')"
    } else {
        "state='queued'"
    };
    let mut query = format!(
        r#"
WITH stored AS (
    SELECT operation_id,created_at_ms,
           CASE WHEN json_valid(effective_request_json)
                THEN effective_request_json ELSE '{{}}' END AS effective_json
    FROM operations
    WHERE method='forge.publish_ref' AND {state_filter}
), extracted AS (
    SELECT operation_id,created_at_ms,
           CASE WHEN json_type(effective_json,'$.publication_intent.canonical_repository')='text'
                THEN json_extract(effective_json,'$.publication_intent.canonical_repository')
                ELSE '' END AS canonical_repository,
           CASE WHEN json_type(effective_json,'$.publication_intent.target_ref')='text'
                THEN json_extract(effective_json,'$.publication_intent.target_ref')
                ELSE '' END AS target_ref
    FROM stored
), routed AS (
    SELECT operation_id,created_at_ms,canonical_repository,target_ref,
           CASE WHEN canonical_repository<>'' AND target_ref<>'' THEN 1 ELSE 0 END AS route_valid,
           CASE WHEN canonical_repository<>'' AND target_ref<>''
                THEN lower(canonical_repository) ELSE '' END AS repository_key
    FROM extracted
), ranked AS (
    SELECT operation_id,created_at_ms,canonical_repository,target_ref,route_valid,repository_key,
           row_number() OVER (
               PARTITION BY route_valid,
                            CASE WHEN route_valid=1 THEN repository_key ELSE operation_id END,
                            CASE WHEN route_valid=1 THEN target_ref ELSE operation_id END
               ORDER BY created_at_ms,operation_id
           ) AS target_rank
    FROM routed
)
SELECT operation_id,route_valid,canonical_repository,repository_key,target_ref
FROM ranked WHERE target_rank=1
"#
    );
    if cursor.is_some() {
        query.push_str(match slice {
            CursorSlice::After => {
                " AND (route_valid > ?1 OR (route_valid = ?1 AND ((route_valid = 1 AND (repository_key > ?2 OR (repository_key = ?2 AND target_ref > ?3))) OR (route_valid = 0 AND operation_id > ?4))))"
            }
            CursorSlice::Through => {
                " AND (route_valid < ?1 OR (route_valid = ?1 AND ((route_valid = 1 AND (repository_key < ?2 OR (repository_key = ?2 AND target_ref <= ?3))) OR (route_valid = 0 AND operation_id <= ?4))))"
            }
        });
        query.push_str(" ORDER BY route_valid,repository_key,target_ref,operation_id LIMIT ?5");
    } else {
        query.push_str(" ORDER BY route_valid,repository_key,target_ref,operation_id LIMIT ?1");
    }
    let mut statement = db.prepare(&query)?;
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    if let Some(cursor) = cursor {
        collect_pending_rows(statement.query(params![
            cursor.route_valid,
            &cursor.repository_key,
            &cursor.target_ref,
            &cursor.operation_id,
            limit,
        ])?)
    } else {
        collect_pending_rows(statement.query([limit])?)
    }
}

/// Select one oldest retained Operation per publication target in a bounded
/// rotating keyset page. The queue-capacity bound remains routing
/// backpressure, not a new Forge/model quota. Each phase owns its cursor in
/// Store `meta`; cursor read, page selection, and cursor advance commit
/// together before any worker starts. Unfinished Operations remain authoritative
/// and return after wrap, including across host restart.
fn pending_for_state(
    db: &mut Connection,
    reconciliation: bool,
    limit: usize,
) -> Result<Vec<ForgePendingOperation>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let key = pending_cursor_key(reconciliation);
    let cursor = read_pending_cursor(&tx, key)?;
    let mut rows = select_pending_rows(
        &tx,
        reconciliation,
        cursor.as_ref(),
        CursorSlice::After,
        limit,
    )?;
    if let Some(cursor) = cursor.as_ref().filter(|_| rows.len() < limit) {
        let remaining = limit - rows.len();
        rows.extend(select_pending_rows(
            &tx,
            reconciliation,
            Some(cursor),
            CursorSlice::Through,
            remaining,
        )?);
    }
    if let Some(last) = rows.last() {
        store_pending_cursor(&tx, key, &last.cursor())?;
    }
    let pending = rows
        .into_iter()
        .map(|row| {
            let target = if row.route_valid {
                crate::forge::canonical_repository(&row.canonical_repository)
                    .ok()
                    .map(|canonical_repository| ForgeTargetKey {
                        canonical_repository,
                        target_ref: row.target_ref,
                    })
            } else {
                None
            };
            ForgePendingOperation { id: row.id, target }
        })
        .collect();
    tx.commit()?;
    Ok(pending)
}

fn pending(db: &mut Connection, limit: usize) -> Result<Vec<ForgePendingOperation>> {
    pending_for_state(db, true, limit)
}

fn queued_pending(db: &mut Connection, limit: usize) -> Result<Vec<ForgePendingOperation>> {
    pending_for_state(db, false, limit)
}

pub(super) struct GitHubDescriptionWorkerRun {
    run: NativeWorkerRun,
}

impl GitHubDescriptionWorkerRun {
    pub(super) fn receipt(&self) -> Value {
        native_worker_receipt(&self.run.job, &self.run.owner_record)
    }
}

async fn spawn_native_worker(
    job: NativeWorkerJob,
    process_permit: tokio::sync::OwnedSemaphorePermit,
) -> Result<NativeWorkerRun> {
    let mut command = TokioCommand::new(&job.executable);
    command
        .arg("--plan")
        .arg(&job.plan_path)
        .arg("--owner")
        .arg(&job.owner_path)
        .arg("--authorization")
        .arg(&job.authorization_path)
        .arg("--result")
        .arg(&job.result_path)
        .current_dir(&job.directory)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(false);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.as_std_mut().creation_flags(0x08000000);
    }
    let mut child = command.spawn().map_err(|_| {
        Error::new(
            "FORGE_WORKER_START_FAILED",
            "the installed native worker could not be started",
        )
    })?;
    let owner_record = match wait_worker_owner(&mut child, &job).await {
        Ok(owner) => owner,
        Err(error) => {
            let _ = wait_worker_exit(
                &mut child,
                Duration::from_secs(job.timeout_seconds.saturating_add(60)),
            )
            .await;
            return Err(error);
        }
    };
    Ok(NativeWorkerRun {
        job,
        child,
        owner_record,
        _process_permit: process_permit,
    })
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

    /// A single host-owned lifecycle pass: reconcile a bounded fair page of
    /// prior uncertain writes by exact-ref readback, then dispatch a bounded
    /// fair page of durable `queued` work.  Unrelated target lanes run on the
    /// existing Tokio executor concurrently; each target remains serialized.
    /// Host shutdown must await this future rather than aborting it.
    pub(crate) async fn supervise_forge_once(&self) -> Result<()> {
        let page_size = self.config.storage.queue_capacity;
        let pending = self.run(move |db| pending(db, page_size)).await;
        let reconcile_result = match pending {
            Ok(pending) => self.run_forge_page(pending, ForgePass::Reconcile).await,
            Err(error) => Err(error),
        };
        let queued = self.run(move |db| queued_pending(db, page_size)).await;
        let dispatch_result = match queued {
            Ok(queued) => self.run_forge_page(queued, ForgePass::Dispatch).await,
            Err(error) => Err(error),
        };
        reconcile_result?;
        dispatch_result?;
        Ok(())
    }

    pub(super) async fn prepare_github_description_worker(
        &self,
        request: GitHubDescriptionWorkerRequest,
    ) -> Result<GitHubDescriptionWorkerRun> {
        let data_dir = self.data_dir.clone();
        let job = self
            .file_io(move |_| create_github_description_worker_job(&data_dir, &request))
            .await?;
        let process_permit = self
            .artifact_io
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::new("STORE_CLOSED", "native worker capacity is closed"))?;
        Ok(GitHubDescriptionWorkerRun {
            run: spawn_native_worker(job, process_permit).await?,
        })
    }

    pub(super) async fn finish_github_description_worker(
        &self,
        worker: GitHubDescriptionWorkerRun,
        authorized: bool,
    ) -> Result<Value> {
        tokio::spawn(async move {
            let job = worker.run.job.clone();
            let authorization_result = match tokio::task::spawn_blocking(move || {
                write_worker_authorization(&job, authorized)
            })
            .await
            {
                Ok(result) => result,
                Err(_) => Err(Error::new(
                    "FORGE_WORKER_AUTHORIZATION_FAILED",
                    "worker authorization write did not complete",
                )),
            };
            let result = wait_forge_worker(worker.run, authorized).await;
            if let Err(error) = authorization_result {
                return match result {
                    Err(tree_error) if tree_error.code == "FORGE_GIT_TREE_TERMINATION" => {
                        Err(tree_error)
                    }
                    _ => Err(error),
                };
            }
            result
        })
        .await
        .map_err(|_| {
            Error::new(
                "FORGE_WORKER_TASK_FAILED",
                "native worker completion task did not return a result",
            )
        })?
    }

    async fn run_forge_page(
        &self,
        operations: Vec<ForgePendingOperation>,
        pass: ForgePass,
    ) -> Result<()> {
        // A dropped JoinHandle detaches its task, allowing an in-flight lane
        // task to keep its serial guard and closure permit until native work
        // drains.  The host normally awaits this page; detachment is only the
        // cancellation-safe fallback.
        let handles = operations
            .into_iter()
            .map(|operation| {
                let store = self.clone();
                tokio::spawn(async move { store.run_forge_operation(operation, pass).await })
            })
            .collect::<Vec<_>>();
        for result in futures_util::future::join_all(handles).await {
            let result = result.map_err(|error| {
                Error::new(
                    "FORGE_SUPERVISOR_TASK",
                    format!("Forge target task failed: {error}"),
                )
            })?;
            result?;
        }
        Ok(())
    }

    async fn run_forge_operation(
        &self,
        operation: ForgePendingOperation,
        pass: ForgePass,
    ) -> Result<()> {
        let Some(target) = operation.target else {
            // Do not guess a lane for malformed retained intent. Read and
            // return its established validation error without changing a
            // `sending`/`outcome_unknown` Operation or releasing its hold.
            let id = operation.id;
            return match self
                .run(move |db| {
                    let saved = saved_intent(db, &id)?;
                    ForgeTargetKey::from_intent(&saved).map(|_| ())
                })
                .await
            {
                Err(error) => Err(error),
                Ok(()) => Err(Error::new(
                    "FORGE_TARGET_UNROUTABLE",
                    "retained Forge intent has no usable target lane",
                )),
            };
        };
        let lane = target_lane(&target);
        let _serial = lane.serial.clone().lock_owned().await;
        if pass == ForgePass::Reconcile {
            // Prove that a prior same-target blocking closure has drained
            // before changing `sending` to unknown or reading the ref.
            let process_guard =
                lane.process.clone().acquire_owned().await.map_err(|_| {
                    Error::new("FORGE_PROCESS_CLOSED", "forge process slot stopped")
                })?;
            drop(process_guard);
        }
        let id = operation.id;
        let config = self.config.clone();
        let work = match pass {
            ForgePass::Reconcile => {
                self.run(move |db| begin_reconciliation_operation(db, &id, &config))
                    .await?
            }
            ForgePass::Dispatch => self.run(move |db| begin(db, &id, &config)).await?,
        };
        if let Some(work) = work {
            self.drive_forge(work).await;
        }
        Ok(())
    }

    async fn drive_forge(&self, work: ForgeWork) {
        let id = work.intent.operation_id.clone();
        let outcome = if work.mode == WorkMode::ReadbackOnly {
            match self.prior_native_worker_departed(&id).await {
                Ok(()) => self.run_forge_worker(&work, None).await,
                Err(error) => ForgeOutcome::Unknown {
                    reason: if error.code == "FORGE_GIT_TREE_TERMINATION" {
                        "git_process_tree_unconfirmed"
                    } else {
                        "worker_recovery_evidence_invalid"
                    },
                    readback: None,
                    timed_out: false,
                    stderr_digest: Some(model::digest(error.code.as_bytes())),
                    stderr_bytes: None,
                    process_tree_unconfirmed: error.code == "FORGE_GIT_TREE_TERMINATION",
                },
            }
        } else {
            let config = self.config.forge.clone();
            let prep = work.clone();
            match self
                .forge_file_io(&work.lane, move |files| {
                    prepare_candidate(&files, &config, &prep)
                })
                .await
            {
                Err(error) => runner_error_outcome(error),
                Ok(()) => {
                    let config = self.config.forge.clone();
                    let preflight_work = work.clone();
                    match self
                        .forge_file_io(&work.lane, move |_| prepare_push(&config, &preflight_work))
                        .await
                    {
                        Err(error) => runner_error_outcome(error),
                        Ok(push_url) => self.run_forge_worker(&work, Some(&push_url)).await,
                    }
                }
            }
        };
        let _ = self.run(move |db| finish(db, &id, outcome)).await;
        self.changed
            .send_modify(|value| *value = value.wrapping_add(1));
    }

    async fn run_forge_worker(
        &self,
        work: &ForgeWork,
        push_endpoint: Option<&str>,
    ) -> ForgeOutcome {
        let process_permit = match work.lane.process.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                return runner_error_outcome(Error::new(
                    "FORGE_PROCESS_CLOSED",
                    "forge process slot stopped",
                ));
            }
        };
        let data_dir = self.data_dir.clone();
        let job_work = work.clone();
        let forge_config = self.config.forge.clone();
        let endpoint = push_endpoint.map(str::to_owned);
        let job = match self
            .file_io(move |_| {
                create_forge_worker_job(&data_dir, &job_work, &forge_config, endpoint.as_deref())
            })
            .await
        {
            Ok(job) => job,
            Err(error) => return runner_error_outcome(error),
        };
        let run = match spawn_native_worker(job.clone(), process_permit).await {
            Ok(run) => run,
            Err(error) => {
                return runner_error_outcome(error);
            }
        };
        let owner_record = run.owner_record.clone();

        let mut deferred_outcome = None;
        let authorized = if work.mode == WorkMode::ReadbackOnly {
            true
        } else {
            let auth_work = work.clone();
            let config = self.config.clone();
            let auth_job = job.clone();
            let auth_owner = owner_record.clone();
            match self
                .run(move |db| dispatch_authorized(db, &auth_work, &config, &auth_job, &auth_owner))
                .await
            {
                Ok(DispatchAuthorization::Authorized) => true,
                Ok(DispatchAuthorization::StaleGmEpoch {
                    admitted_gm_epoch,
                    current_gm_epoch,
                }) => {
                    deferred_outcome = Some(ForgeOutcome::StaleGmEpoch {
                        admitted_gm_epoch,
                        current_gm_epoch,
                    });
                    false
                }
                Ok(DispatchAuthorization::Coalesced { owner_operation_id }) => {
                    deferred_outcome = Some(ForgeOutcome::Coalesced { owner_operation_id });
                    false
                }
                Err(error) => {
                    deferred_outcome = Some(runner_error_outcome(error));
                    false
                }
            }
        };
        let job_for_auth = job.clone();
        let authorization_result = self
            .file_io(move |_| write_worker_authorization(&job_for_auth, authorized))
            .await;
        let result = wait_forge_worker(run, authorized).await;
        if let Some(outcome) = deferred_outcome {
            return match result {
                Ok(_) => outcome,
                Err(error) if error.code == "FORGE_GIT_TREE_TERMINATION" => ForgeOutcome::Unknown {
                    reason: "git_process_tree_unconfirmed",
                    readback: None,
                    timed_out: false,
                    stderr_digest: None,
                    stderr_bytes: None,
                    process_tree_unconfirmed: true,
                },
                Err(_) => outcome,
            };
        }
        if authorization_result.is_err() {
            return match result {
                Err(error) if error.code == "FORGE_GIT_TREE_TERMINATION" => ForgeOutcome::Unknown {
                    reason: "git_process_tree_unconfirmed",
                    readback: None,
                    timed_out: false,
                    stderr_digest: None,
                    stderr_bytes: None,
                    process_tree_unconfirmed: true,
                },
                _ => ForgeOutcome::Unknown {
                    reason: "worker_authorization_delivery_failed",
                    readback: None,
                    timed_out: false,
                    stderr_digest: None,
                    stderr_bytes: None,
                    process_tree_unconfirmed: false,
                },
            };
        }
        match result {
            Ok(result) => map_forge_worker_result(&job, work, &result),
            Err(error) => ForgeOutcome::Unknown {
                reason: if error.code == "FORGE_GIT_TREE_TERMINATION" {
                    "git_process_tree_unconfirmed"
                } else {
                    "push_worker_failed"
                },
                readback: None,
                timed_out: false,
                stderr_digest: Some(model::digest(error.code.as_bytes())),
                stderr_bytes: None,
                process_tree_unconfirmed: error.code == "FORGE_GIT_TREE_TERMINATION",
            },
        }
    }

    pub(super) async fn prior_native_worker_departed(&self, operation_id: &str) -> Result<()> {
        match self.prior_native_worker_departed_inner(operation_id).await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.record_native_worker_recovery_block(operation_id, &error)
                    .await?;
                Err(error)
            }
        }
    }

    async fn prior_native_worker_departed_inner(&self, operation_id: &str) -> Result<()> {
        let id = operation_id.to_owned();
        let operation = self
            .run(move |db| operations::get_operation(db, &id))
            .await?;
        let Some(receipt) = operation["result"].get("native_worker") else {
            return Ok(());
        };
        let receipt_fields = receipt
            .as_object()
            .ok_or_else(|| Error::new("FORGE_GIT_TREE_TERMINATION", "worker receipt is invalid"))?;
        let owner_token = receipt
            .get("owner_token")
            .and_then(Value::as_str)
            .filter(|value| valid_worker_uuid(value))
            .ok_or_else(|| Error::new("FORGE_GIT_TREE_TERMINATION", "owner token is invalid"))?;
        let owner = receipt
            .get("owner")
            .filter(|owner| {
                receipt_fields.len() == 8
                    && [
                        "version",
                        "kind",
                        "job_id",
                        "operation_id",
                        "phase",
                        "owner_token",
                        "plan_sha256",
                        "owner",
                    ]
                    .iter()
                    .all(|field| receipt_fields.contains_key(*field))
                    && receipt["version"] == 1
                    && matches!(
                        receipt.get("kind").and_then(Value::as_str),
                        Some("forge_publish" | "github_pr_description")
                    )
                    && receipt
                        .get("job_id")
                        .and_then(Value::as_str)
                        .is_some_and(valid_worker_uuid)
                    && receipt.get("operation_id").and_then(Value::as_str) == Some(operation_id)
                    && matches!(
                        (receipt["kind"].as_str(), receipt["phase"].as_str()),
                        (Some("forge_publish"), Some("push_once" | "readback_only"))
                            | (Some("github_pr_description"), Some("patch_once"))
                    )
                    && receipt
                        .get("plan_sha256")
                        .and_then(Value::as_str)
                        .is_some_and(valid_worker_digest)
                    && owner.as_object().is_some_and(|fields| {
                        fields.len() == 3
                            && fields.contains_key("version")
                            && fields.contains_key("token")
                            && fields.contains_key("process")
                    })
                    && owner["version"] == 1
                    && owner["token"] == owner_token
                    && owner["process"]["purpose"] == "module"
            })
            .ok_or_else(|| Error::new("FORGE_GIT_TREE_TERMINATION", "owner receipt is invalid"))?;
        if !swarm_process::departed_empty(&owner["process"], owner_token)? {
            return Err(Error::new(
                "FORGE_GIT_TREE_TERMINATION",
                "previous Forge worker process family remains active",
            ));
        }
        let job_id = receipt["job_id"]
            .as_str()
            .ok_or_else(|| Error::new("FORGE_GIT_TREE_TERMINATION", "worker job ID is absent"))?
            .to_owned();
        let data_dir = self.data_dir.clone();
        let owner_directory = job_id;
        let (plan_bytes, result_file) = self
            .file_io(move |_| {
                let data_dir = data_dir.canonicalize().map_err(|_| {
                    Error::new(
                        "FORGE_WORKER_EVIDENCE_UNAVAILABLE",
                        "retained worker evidence is unavailable",
                    )
                })?;
                let jobs_root = data_dir.join("forge-worker-runs");
                reject_forge_link(&jobs_root).map_err(|_| {
                    Error::new(
                        "FORGE_WORKER_EVIDENCE_INVALID",
                        "retained worker evidence path is invalid",
                    )
                })?;
                let jobs_root = jobs_root.canonicalize().map_err(|_| {
                    Error::new(
                        "FORGE_WORKER_EVIDENCE_UNAVAILABLE",
                        "retained worker evidence is unavailable",
                    )
                })?;
                let job_directory = jobs_root.join(owner_directory);
                reject_forge_link(&job_directory).map_err(|_| {
                    Error::new(
                        "FORGE_WORKER_EVIDENCE_INVALID",
                        "retained worker evidence path is invalid",
                    )
                })?;
                let job_directory = job_directory.canonicalize().map_err(|_| {
                    Error::new(
                        "FORGE_WORKER_EVIDENCE_UNAVAILABLE",
                        "retained worker evidence is unavailable",
                    )
                })?;
                if !job_directory.starts_with(&jobs_root) {
                    return Err(Error::new(
                        "FORGE_WORKER_EVIDENCE_INVALID",
                        "retained worker evidence escaped its private state root",
                    ));
                }
                let plan_bytes = read_worker_bytes(&job_directory.join("plan.json"), 1_048_576)
                    .map_err(|_| {
                        Error::new(
                            "FORGE_WORKER_EVIDENCE_UNAVAILABLE",
                            "retained worker plan is unavailable",
                        )
                    })?;
                let result_file =
                    read_worker_bytes_optional(&job_directory.join("result.json"), 262_144)
                        .map_err(|_| {
                            Error::new(
                                "FORGE_WORKER_EVIDENCE_INVALID",
                                "retained worker result file is invalid",
                            )
                        })?;
                Ok((plan_bytes, result_file))
            })
            .await?;
        let retained_plan_digest = model::digest(&plan_bytes);
        if receipt["plan_sha256"].as_str() != Some(retained_plan_digest.as_str()) {
            return Err(worker_plan_scope_error());
        }
        let plan: Value = serde_json::from_slice(&plan_bytes).map_err(|_| {
            Error::new(
                "FORGE_WORKER_PLAN_SCOPE_MISMATCH",
                "retained worker plan is not valid JSON",
            )
        })?;
        match receipt["kind"].as_str() {
            Some("forge_publish") => {
                let operation = operation.clone();
                let receipt = receipt.clone();
                self.run(move |db| validate_forge_worker_plan(db, &operation, &receipt, &plan))
                    .await?;
            }
            Some("github_pr_description") => {
                let operation = operation.clone();
                let receipt = receipt.clone();
                self.run(move |db| {
                    super::github_pr_effects::validate_retained_worker_plan(
                        db, &operation, &receipt, &plan,
                    )
                })
                .await?;
            }
            _ => return Err(worker_plan_scope_error()),
        }
        let result_status = classify_worker_result(&operation, receipt, result_file)?;
        self.record_native_worker_recovery_ready(operation_id, receipt, result_status)
            .await?;
        Ok(())
    }

    async fn record_native_worker_recovery_ready(
        &self,
        operation_id: &str,
        receipt: &Value,
        result_status: WorkerResultStatus,
    ) -> Result<()> {
        let operation_id = operation_id.to_owned();
        let receipt = receipt.clone();
        self.run(move |db| {
            let now = model::now_ms()?;
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let operation = operations::get_operation(&tx, &operation_id)?;
            if !matches!(
                operation["state"].as_str(),
                Some("sending" | "outcome_unknown")
            ) || model::canonical(&operation["result"]["native_worker"])?
                != model::canonical(&receipt)?
            {
                return Err(Error::new(
                    "FORGE_WORKER_RECEIPT_CHANGED",
                    "the retained worker receipt changed during recovery",
                ));
            }
            let mut result = operation["result"].clone();
            result["native_worker_recovery"] = json!({
                "schema_version":1,
                "status":"readback_permitted",
                "job_id":receipt["job_id"],
                "plan_sha256":receipt["plan_sha256"],
                "process_family":"departed_empty",
                "worker_result":result_status.as_str(),
                "recorded_at_ms":now
            });
            result["process_tree_unconfirmed"] = json!(false);
            result["process_tree_status"] = json!("departed_empty");
            if result["reason"] == "process_tree_unconfirmed" {
                result["reason"] = json!("exact_remote_readback_required");
            }
            if result["publication"] == "operator_intervention_required" {
                result["publication"] = json!("requires_readback");
            }
            if let Some(fields) = result.as_object_mut() {
                fields.remove("process_tree_cleanup");
                fields.remove("resolution");
            }
            match receipt["kind"].as_str() {
                Some("forge_publish") => {
                    result["publication_may_have_started"] = json!(true);
                }
                Some("github_pr_description") => {
                    result["write_attempted"] = json!(true);
                    result["readback"] = json!("unknown");
                }
                _ => return Err(worker_plan_scope_error()),
            }
            let changed = tx.execute(
                "UPDATE operations SET result_json=?2,updated_at_ms=?3 WHERE operation_id=?1 AND state IN ('sending','outcome_unknown')",
                params![operation_id, model::canonical(&result)?, now],
            )?;
            if changed != 1 {
                return Err(Error::conflict(
                    "the operation changed while retaining native worker recovery",
                ));
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    async fn record_native_worker_recovery_block(
        &self,
        operation_id: &str,
        error: &Error,
    ) -> Result<()> {
        let operation_id = operation_id.to_owned();
        let process_family_departed = matches!(
            error.code.as_str(),
            "FORGE_WORKER_PLAN_SCOPE_MISMATCH"
                | "FORGE_WORKER_RESULT_SCOPE_MISMATCH"
                | "FORGE_WORKER_EVIDENCE_UNAVAILABLE"
                | "FORGE_WORKER_EVIDENCE_INVALID"
        );
        let tree_unconfirmed = !process_family_departed;
        let reason_code = match error.code.as_str() {
            "FORGE_WORKER_PLAN_SCOPE_MISMATCH" => "worker_plan_scope_mismatch",
            "FORGE_WORKER_RESULT_SCOPE_MISMATCH" => "worker_result_scope_mismatch",
            "FORGE_WORKER_RECEIPT_CHANGED" => "worker_receipt_changed",
            "FORGE_WORKER_EVIDENCE_UNAVAILABLE" => "worker_evidence_unavailable",
            "FORGE_WORKER_EVIDENCE_INVALID" => "worker_evidence_invalid",
            "FORGE_GIT_TREE_TERMINATION" => "owner_or_process_family_unconfirmed",
            _ => "recovery_evidence_unavailable",
        };
        self.run(move |db| {
            let now = model::now_ms()?;
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let operation = operations::get_operation(&tx, &operation_id)?;
            if !matches!(
                operation["method"].as_str(),
                Some("forge.publish_ref" | "github.pull_request.update_description")
            ) || !matches!(
                operation["state"].as_str(),
                Some("sending" | "outcome_unknown")
            ) {
                return Err(Error::conflict(
                    "the retained native worker Operation is no longer recovery-blocked",
                ));
            }
            let mut result = operation["result"].clone();
            result["native_worker_recovery"] = json!({
                "schema_version":1,
                "status":"blocked",
                "reason_code":reason_code,
                "process_family":if tree_unconfirmed { "unconfirmed" } else { "departed_empty" },
                "retryable":tree_unconfirmed || reason_code == "worker_evidence_unavailable",
                "recorded_at_ms":now
            });
            result["outcome"] = json!("unknown");
            result["reason"] = json!("native_worker_recovery_blocked");
            result["current_state_read_method"] = json!("operation.get");
            result["process_tree_unconfirmed"] = json!(tree_unconfirmed);
            result["process_tree_status"] = json!(if tree_unconfirmed {
                "unconfirmed"
            } else {
                "departed_empty"
            });
            if tree_unconfirmed {
                result["process_tree_cleanup"] = json!("native_worker_recovery_blocked");
                result["resolution"] = json!("manual_operator_intervention_required");
            } else if let Some(fields) = result.as_object_mut() {
                fields.remove("process_tree_cleanup");
                fields.remove("resolution");
            }
            if operation["method"] == "forge.publish_ref" {
                result["publication_may_have_started"] = json!(true);
                result["publication"] = json!("operator_intervention_required");
            } else {
                result["write_attempted"] = json!(true);
                result["readback"] = json!("unknown");
            }
            let changed = tx.execute(
                "UPDATE operations SET state=?2,result_json=?3,settled_at_ms=NULL,updated_at_ms=?4 WHERE operation_id=?1 AND state IN ('sending','outcome_unknown')",
                params![operation_id, "outcome_unknown", model::canonical(&result)?, now],
            )?;
            if changed != 1 {
                return Err(Error::conflict(
                    "the operation changed while retaining its recovery block",
                ));
            }
            super::capacity::sync_operation(&tx, &operation_id, now)?;
            super::record_operation_failure_event(&tx, &operation_id, "outcome_unknown", now)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    async fn forge_file_io<T: Send + 'static>(
        &self,
        lane: &Arc<ForgeTargetLane>,
        f: impl FnOnce(crate::artifacts::ArtifactFiles) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let lane = Arc::clone(lane);
        let permit = lane
            .process
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::new("FORGE_PROCESS_CLOSED", "forge process slot stopped"))?;
        self.file_io(move |files| {
            // Keep the lane itself alive with the closure as well as the
            // keyed process permit.  A cancelled supervisor cannot cause a
            // later lookup to create a fresh semaphore while this closure is
            // still draining.
            let _lane = lane;
            let _permit = permit;
            f(files)
        })
        .await
    }
}
