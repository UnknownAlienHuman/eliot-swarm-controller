#[path = "task_sources.rs"]
pub(super) mod task_sources;

use super::{acceptance, meta, operations};
use crate::{
    error::{Error, Result},
    model::{self, Principal, Role, StartOwner, TaskSpec},
    policy,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};

pub(super) fn get_task(db: &Connection, id: &str) -> Result<Value> {
    let raw:Option<String>=db.query_row("SELECT json_object('task_id',task_id,'project_id',project_id,'revision',revision,'state',state,'origin_key',origin_key,'spec',json(spec_json),'accepted_attempt_id',accepted_attempt_id,'accepted_operation_id',accepted_operation_id,'accepted_revision',accepted_revision,'accepted_phase',accepted_phase,'accepted_candidate_ref',accepted_candidate_ref) FROM tasks WHERE task_id=?1",[id],|r|r.get(0)).optional()?;
    let mut task: Value =
        serde_json::from_str(&raw.ok_or_else(|| Error::new("NOT_FOUND", format!("Task {id}")))?)?;
    let task_brief = task_sources::project_brief(&task["spec"]);
    task["task_brief"] = task_brief;
    let owner: Option<String> = db
        .query_row(
            "SELECT attempt_id FROM attempts WHERE task_id=?1 AND released_at_ms IS NULL",
            [id],
            |r| r.get(0),
        )
        .optional()?;
    task["current_attempt_id"] = json!(owner);
    Ok(task)
}
pub(super) fn get_attempt(db: &Connection, id: &str) -> Result<Value> {
    let raw:Option<String>=db.query_row("SELECT json_object('attempt_id',attempt_id,'task_id',task_id,'task_revision',task_revision,'owner_id',owner_id,'start_owner',start_owner,'start_operation_id',start_operation_id,'binding_id',binding_id,'binding_generation',binding_generation,'state',state,'released_at_ms',released_at_ms,'task_snapshot',json(task_snapshot_json),'producers',json(producers_json),'submission_ref',submission_ref,'candidate_ref',candidate_ref) FROM attempts WHERE attempt_id=?1",[id],|r|r.get(0)).optional()?;
    let attempt: Value = serde_json::from_str(
        &raw.ok_or_else(|| Error::new("NOT_FOUND", format!("Attempt {id}")))?,
    )?;
    Ok(project_attempt(attempt))
}

fn project_attempt(mut attempt: Value) -> Value {
    let snapshot = attempt.get("task_snapshot").cloned().unwrap_or(Value::Null);
    attempt["owner_policy"] = policy::attempt_projection(&snapshot);
    attempt["task_brief"] = snapshot.get("brief").cloned().unwrap_or(Value::Null);
    attempt
}

fn task_snapshot(
    spec: &TaskSpec,
    revision: i64,
    dependency_acceptances: Vec<Value>,
    owner_policy: policy::OwnerPolicyEdition,
    baseline_candidate: Value,
) -> Value {
    json!({
        "spec": spec,
        "revision": revision,
        "dependency_acceptances": dependency_acceptances,
        "baseline_candidate": baseline_candidate,
        "owner_policy": owner_policy,
        "brief": task_sources::brief(spec),
    })
}
fn spec(v: &Value) -> Result<TaskSpec> {
    let s: TaskSpec = serde_json::from_value(
        v.get("spec")
            .cloned()
            .ok_or_else(|| Error::invalid("spec required"))?,
    )?;
    s.validate()?;
    Ok(s)
}

fn task_validation_error(error: swarm_kernel::tasks::ValidationError) -> Error {
    Error::new(error.code(), error.message())
}

/// Task creation and revision are local planning rights shared by Managers
/// and the pinned local Operator. Do not use `require_writer`: it is a broad
/// role filter, not this positive method policy.
fn require_task_planner(principal: &Principal) -> Result<()> {
    match &principal.role {
        Role::Operator => principal.require_operator(),
        Role::Manager => Ok(()),
        _ => Err(Error::new(
            "FORBIDDEN",
            "task planning requires Manager or local Operator authority",
        )),
    }
}

/// A Manager may revise its own live assignment. Reassigning the specification
/// beneath another Manager's unreleased Attempt is reserved to the current GM
/// or local Operator; the Attempt snapshot itself remains immutable.
fn authorize_foreign_live_attempt_revision(
    tx: &Transaction<'_>,
    principal: &Principal,
    task: &Value,
) -> Result<()> {
    let Some(attempt_id) = task.get("current_attempt_id").and_then(Value::as_str) else {
        return Ok(());
    };
    let attempt = get_attempt(tx, attempt_id)?;
    if attempt["task_id"] != task["task_id"] || !attempt["released_at_ms"].is_null() {
        return Err(Error::new(
            "STORE_INVARIANT",
            "current Task Attempt pointer is not an unreleased Attempt for this Task",
        ));
    }
    if principal.role == Role::Operator {
        return Ok(());
    }
    let owner_id = model::text(&attempt, "owner_id")?;
    if principal.client_id != owner_id {
        super::gm::require_authority(tx, principal)?;
    }
    Ok(())
}

pub(super) fn create(
    tx: &Transaction<'_>,
    p: &Principal,
    v: &Value,
    id: &str,
    now: i64,
) -> Result<Value> {
    require_task_planner(p)?;
    model::fields(
        v,
        &["client_request_id", "project_id", "origin_key", "spec"],
    )?;
    let project = model::text(v, "project_id")?;
    let s = spec(v)?;
    let origin = if v.get("origin_key").is_some() {
        Some(model::text(v, "origin_key")?)
    } else {
        None
    };
    create_validated(tx, project, &s, origin, id, now)
}

/// Taskless automation can create a Task only in the project retained by its
/// current Manager-owned event entry. The typed ScriptEffect authority checks
/// the Manager, source, active entry and ScriptRun before and after this
/// shared Task.create body.
pub(super) fn create_for_script_effect(
    tx: &Transaction<'_>,
    expected_project_id: &str,
    v: &Value,
    id: &str,
    now: i64,
) -> Result<Value> {
    model::fields(v, &["client_request_id", "project_id", "spec"])?;
    let project = model::text(v, "project_id")?;
    if project != expected_project_id {
        return Err(Error::new(
            "FORBIDDEN",
            "script task_create is limited to its retained automation project",
        ));
    }
    let s = spec(v)?;
    create_validated(tx, project, &s, None, id, now)
}

fn create_validated(
    tx: &Transaction<'_>,
    project: &str,
    s: &TaskSpec,
    origin: Option<&str>,
    id: &str,
    now: i64,
) -> Result<Value> {
    if let Some(origin) = origin {
        let prior: Option<String> = tx
            .query_row(
                "SELECT task_id FROM tasks WHERE origin_key=?1",
                [origin],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(task_id) = prior {
            return Ok(
                json!({"operation_id":id,"task_id":task_id,"created":false,"reason":"origin_already_exists"}),
            );
        }
    }
    let task_id = model::new_id();
    tx.execute("INSERT INTO tasks(task_id,project_id,origin_key,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,1,'open',?4,?5,?5)",params![task_id,project,origin,model::canonical(&json!(s))?,now])?;
    tx.execute(
        "UPDATE operations SET task_id=?2 WHERE operation_id=?1",
        params![id, task_id],
    )?;
    Ok(json!({"operation_id":id,"task_id":task_id,"revision":1,"created":true}))
}
pub(super) fn revise(
    tx: &Transaction<'_>,
    p: &Principal,
    v: &Value,
    id: &str,
    now: i64,
) -> Result<Value> {
    require_task_planner(p)?;
    model::fields(
        v,
        &["client_request_id", "task_id", "expected_revision", "spec"],
    )?;
    let task_id = model::text(v, "task_id")?;
    let expected = model::positive(v, "expected_revision")?;
    let s = spec(v)?;
    let previous = get_task(tx, task_id)?;
    swarm_kernel::tasks::validate_revision_state(
        expected,
        previous["revision"].as_i64().unwrap_or_default(),
        previous["state"].as_str().unwrap_or_default(),
    )
    .map_err(task_validation_error)?;
    authorize_foreign_live_attempt_revision(tx, p, &previous)?;
    let next = swarm_kernel::tasks::validate_revision_update(
        expected,
        s.dependencies.iter().any(|d| d.task_id == task_id),
    )
    .map_err(task_validation_error)?;
    tx.execute("UPDATE tasks SET revision=?2,spec_json=?3,state='open',accepted_attempt_id=NULL,accepted_operation_id=NULL,accepted_revision=NULL,accepted_phase=NULL,accepted_candidate_ref=NULL,updated_at_ms=?4 WHERE task_id=?1",params![task_id,next,model::canonical(&json!(s))?,now])?;
    tx.execute(
        "UPDATE operations SET task_id=?2,effective_request_json=?3 WHERE operation_id=?1",
        params![
            id,
            task_id,
            model::canonical(&json!({"previous":previous}))?
        ],
    )?;
    Ok(
        json!({"operation_id":id,"task_id":task_id,"revision":next,"existing_attempt_preserved":true}),
    )
}
enum ClaimAuthority<'a> {
    Direct(&'a Principal),
    Launch(&'a super::launcher::LaunchActor),
}

pub(super) fn claim(
    tx: &Transaction<'_>,
    p: &Principal,
    v: &Value,
    id: &str,
    now: i64,
) -> Result<Value> {
    claim_with_authority(tx, ClaimAuthority::Direct(p), v, id, now)
}

/// Launch-only Task claim. A WorkDispatch actor retains its technical caller
/// separately from the effective manager who owns the Task Attempt.
pub(super) fn claim_for_launch(
    tx: &Transaction<'_>,
    actor: &super::launcher::LaunchActor,
    v: &Value,
    id: &str,
    now: i64,
) -> Result<Value> {
    claim_with_authority(tx, ClaimAuthority::Launch(actor), v, id, now)
}

fn claim_with_authority(
    tx: &Transaction<'_>,
    authority: ClaimAuthority<'_>,
    v: &Value,
    id: &str,
    now: i64,
) -> Result<Value> {
    let launch_claim = matches!(&authority, ClaimAuthority::Launch(_));
    model::fields(
        v,
        &[
            "client_request_id",
            "task_id",
            "expected_revision",
            "owner_id",
            "start_owner",
            "binding_id",
            "binding_generation",
        ],
    )?;
    let task_id = model::text(v, "task_id")?;
    let revision = model::positive(v, "expected_revision")?;
    let owner = match &authority {
        ClaimAuthority::Direct(principal) => {
            let owner = v
                .get("owner_id")
                .and_then(Value::as_str)
                .unwrap_or(&principal.client_id);
            principal.owns(owner)?;
            owner
        }
        ClaimAuthority::Launch(actor) => {
            let owner = actor.effective_manager_id();
            if v.get("owner_id").and_then(Value::as_str) != Some(owner) {
                return Err(Error::new(
                    "FORBIDDEN",
                    "launch claim owner must be the effective manager",
                ));
            }
            let caller: Option<String> = tx
                .query_row(
                    "SELECT caller_id FROM operations WHERE operation_id=?1",
                    [id],
                    |row| row.get(0),
                )
                .optional()?;
            if caller.as_deref() != Some(actor.technical_requester_id()) {
                return Err(Error::new(
                    "FORBIDDEN",
                    "launch claim Operation caller differs from its technical requester",
                ));
            }
            actor.require_action_object(tx, "swarm.launch", task_id, revision, None)?;
            owner
        }
    };
    let profile = meta(tx, &format!("client:{owner}"))?
        .ok_or_else(|| Error::new("NOT_FOUND", "owner is not registered"))?;
    if profile["disabled"] == true
        || !matches!(profile["role"].as_str(), Some("operator" | "manager"))
    {
        return Err(Error::new("FORBIDDEN", "owner cannot execute work"));
    }
    let start: StartOwner = if let Some(value) = v.get("start_owner") {
        serde_json::from_value(value.clone())?
    } else {
        StartOwner::NativeManager
    };
    swarm_kernel::tasks::validate_claim_start(start.as_str(), launch_claim)
        .map_err(task_validation_error)?;
    let task = get_task(tx, task_id)?;
    swarm_kernel::tasks::validate_claim_task_state(
        task["revision"].as_i64().unwrap_or_default(),
        revision,
        task["state"].as_str().unwrap_or_default(),
    )
    .map_err(task_validation_error)?;
    if let Some(existing) = task["current_attempt_id"].as_str() {
        let a = get_attempt(tx, existing)?;
        if a["owner_id"] == owner
            && a["task_revision"] == revision
            && a["start_owner"] == start.as_str()
            && a.get("binding_id") == Some(v.get("binding_id").unwrap_or(&Value::Null))
            && a.get("binding_generation")
                == Some(v.get("binding_generation").unwrap_or(&Value::Null))
        {
            return Ok(
                json!({"operation_id":id,"attempt_id":existing,"task_id":task_id,"created":false}),
            );
        }
        return Err(Error::new(
            "TASK_ALREADY_OWNED",
            format!("unreleased Attempt {existing}"),
        ));
    }
    if meta(tx, "execution_mode")?.unwrap_or(Value::Null)["new_work"] != "enabled" {
        return Err(Error::new("ADMISSION_DISABLED", "new work is disabled"));
    }
    let spec: TaskSpec = serde_json::from_value(task["spec"].clone())?;
    spec.validate()?;
    let owner_policy = policy::accepted_edition(spec.owner_policy_id.as_deref())?;
    let baseline_candidate = acceptance::freeze_baseline_candidate(
        tx,
        model::text(&task, "project_id")?,
        spec.baseline_candidate_ref.as_deref(),
    )?;
    let mut dependency_receipts = Vec::new();
    for d in &spec.dependencies {
        let accepted = acceptance::resolve_dependency(tx, d)?;
        dependency_receipts.push(json!({"task_id":d.task_id,"acceptance_operation_id":accepted}));
    }
    swarm_kernel::tasks::validate_claim_binding_pair(
        v.get("binding_id"),
        v.get("binding_generation"),
    )
    .map_err(task_validation_error)?;
    let (binding, generation) = match (v.get("binding_id"), v.get("binding_generation")) {
        (None, None) | (Some(Value::Null), Some(Value::Null)) => (None, None),
        (Some(_), Some(_)) => {
            let binding = model::text(v, "binding_id")?;
            let generation = model::positive(v, "binding_generation")?;
            let b = operations::get_binding(tx, binding, generation)?;
            if b["state"] != "ready" {
                return Err(Error::new(
                    "BINDING_NOT_READY",
                    "native binding is not ready",
                ));
            }
            (Some(binding), Some(generation))
        }
        _ => unreachable!("claim binding pair was validated by swarm-kernel"),
    };
    let attempt = model::new_id();
    let snapshot = task_snapshot(
        &spec,
        revision,
        dependency_receipts,
        owner_policy,
        baseline_candidate,
    );
    tx.execute("INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'reserved',?9,?9)",params![attempt,task_id,revision,model::canonical(&snapshot)?,owner,start.as_str(),binding,generation,now])?;
    tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1",
        params![id, task_id, attempt],
    )?;
    Ok(
        json!({"operation_id":id,"attempt_id":attempt,"task_id":task_id,"start_owner":start,"state":"reserved","created":true,"native_admission":"not_observed"}),
    )
}
pub(super) fn release(
    tx: &Transaction<'_>,
    p: &Principal,
    v: &Value,
    id: &str,
    now: i64,
) -> Result<Value> {
    model::fields(
        v,
        &[
            "client_request_id",
            "attempt_id",
            "outcome",
            "reason",
            "assignment_closed",
        ],
    )?;
    let attempt_id = model::text(v, "attempt_id")?;
    let outcome = model::text(v, "outcome")?;
    let reason = model::text(v, "reason")?;
    swarm_kernel::tasks::validate_release_request(outcome, v["assignment_closed"] == true)
        .map_err(task_validation_error)?;
    let a = get_attempt(tx, attempt_id)?;
    super::gm::require_attempt_control(tx, p, &a)?;
    if !a["released_at_ms"].is_null() {
        operations::prepare_owned_service_attempt_release(tx, &a, id, now)?;
        return Ok(
            json!({"operation_id":id,"attempt_id":attempt_id,"released":true,"changed":false}),
        );
    }
    let accepted = a["state"] == "accepted" && acceptance::accepted_attempt(tx, &a)?;
    let held: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM check_runs WHERE attempt_id=?1 AND resource_claimed_at_ms IS NOT NULL AND resource_released_at_ms IS NULL)", [attempt_id], |r| r.get(0))?;
    let unresolved:i64=tx.query_row("SELECT count(*) FROM operations WHERE attempt_id=?1 AND state IN ('sending','native_accepted','outcome_unknown')",[attempt_id],|r|r.get(0))?;
    let unresolved_producers = a["producers"].as_array().is_some_and(|items| {
        items.iter().any(|item| {
            !matches!(
                item["disposition"].as_str(),
                Some("completed" | "failed" | "cancelled")
            )
        })
    });
    swarm_kernel::tasks::validate_release_transition(
        outcome,
        accepted,
        held,
        unresolved,
        unresolved_producers,
    )
    .map_err(task_validation_error)?;
    operations::prepare_owned_service_attempt_release(tx, &a, id, now)?;
    // Caller explicitly seals cooperative native work. This is not process evidence.
    // A queued check needs a retained cancellation result, not an orphaned row.
    tx.execute("UPDATE check_runs SET spec_json=json_set(spec_json,'$.cancel_requested','attempt released before execution') WHERE attempt_id=?1 AND state='queued'",[attempt_id])?;
    tx.execute("UPDATE operations SET state='cancelled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE attempt_id=?1 AND state='queued' AND method<>'check.run'",params![attempt_id,model::canonical(&json!({"reason":"attempt released before delivery"}))?,now])?;
    tx.execute(
        "UPDATE attempts SET state=?2,released_at_ms=?3,updated_at_ms=?3 WHERE attempt_id=?1",
        params![attempt_id, outcome, now],
    )?;
    tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1",
        params![id, a["task_id"].as_str(), attempt_id],
    )?;
    super::capacity::sync_attempt(tx, attempt_id, now)?;
    Ok(
        json!({"operation_id":id,"attempt_id":attempt_id,"released":true,"outcome":outcome,"reason":reason,"evidence_kind":"caller_attested_assignment_closed","native_processes_stopped":false}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Config,
        model::{Requirement, SourceIndexStatus, TaskSourceIndexEntry},
        platform::{DataRoot, bootstrap_credential},
        policy::OWNER_POLICY_V1_ID,
        store::StoreOwner,
    };
    use std::sync::Arc;

    #[test]
    fn attempt_snapshot_freezes_policy_and_ordered_source_index() {
        let text = "Malformed issue comment: acceptance details are unclear.";
        let selected_text = "Issue body selected for the Task revision.";
        let spec = TaskSpec {
            acceptance: None,
            objective: "Implement issue 14".to_owned(),
            phase: "implementation".to_owned(),
            requirements: vec![Requirement {
                id: "R1".to_owned(),
                statement: "Keep unknown source comments visible".to_owned(),
            }],
            dependencies: Vec::new(),
            scope: None,
            baseline_candidate_ref: None,
            source_refs: vec![
                "issue:14/comment:7".to_owned(),
                "docs/owner-decisions.md".to_owned(),
            ],
            owner_policy_id: Some(OWNER_POLICY_V1_ID.to_owned()),
            source_index: vec![
                TaskSourceIndexEntry {
                    source_ref: "issue:14/body".to_owned(),
                    revision: Some("issue-revision-3".to_owned()),
                    content_sha256: Some(model::digest(selected_text.as_bytes())),
                    text: Some(selected_text.to_owned()),
                    status: SourceIndexStatus::Selected,
                    gap_reason: None,
                },
                TaskSourceIndexEntry {
                    source_ref: "issue:14/comment:7".to_owned(),
                    revision: Some("issue-revision-3".to_owned()),
                    content_sha256: None,
                    text: Some(text.to_owned()),
                    status: SourceIndexStatus::Gap,
                    gap_reason: Some("comment_parse_incomplete".to_owned()),
                },
            ],
        };
        spec.validate().unwrap();
        let snapshot = task_snapshot(
            &spec,
            3,
            Vec::new(),
            policy::accepted_edition(spec.owner_policy_id.as_deref()).unwrap(),
            json!({"status":"wide","reason":"baseline_not_configured"}),
        );

        assert_eq!(snapshot["owner_policy"]["status"], "accepted");
        assert_eq!(snapshot["owner_policy"]["edition"], 1);
        assert_eq!(
            snapshot["brief"]["source_index"][0]["content_sha256"],
            model::digest(selected_text.as_bytes())
        );
        assert_eq!(snapshot["brief"]["source_index"][0]["status"], "selected");
        assert_eq!(snapshot["brief"]["source_index"][0]["text"], selected_text);
        assert_eq!(snapshot["brief"]["source_index"][1]["status"], "gap");
        assert_eq!(snapshot["brief"]["source_index"][1]["text"], text);
        assert_eq!(
            snapshot["brief"]["source_index"][1]["content_sha256"],
            model::digest(text.as_bytes())
        );
        assert_eq!(
            snapshot["brief"]["source_index"][1]["gap_reason"],
            "comment_parse_incomplete"
        );
        assert_eq!(snapshot["brief"]["source_index"][2]["status"], "gap");
        assert_eq!(
            snapshot["brief"]["source_index"][2]["gap_reason"],
            "legacy_source_ref_without_pinned_revision_or_content"
        );
        assert_eq!(snapshot["brief"]["objective"], spec.objective);
    }

    #[test]
    fn legacy_unreadable_task_remains_readable_without_rewriting_its_spec() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(crate::store::SCHEMA).unwrap();
        db.execute(
            "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES('legacy-task','legacy-project',1,'open','{}',1,1)",
            [],
        )
        .unwrap();

        let task = get_task(&db, "legacy-task").unwrap();
        assert_eq!(task["task_id"], "legacy-task");
        assert_eq!(task["spec"], json!({}));
        assert_eq!(
            task["task_brief"],
            json!({
                "status": "unavailable",
                "reason": "stored_task_spec_unreadable",
            })
        );
        assert_eq!(task["current_attempt_id"], Value::Null);

        let persisted_spec: String = db
            .query_row(
                "SELECT spec_json FROM tasks WHERE task_id='legacy-task'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(persisted_spec, "{}");
    }

    #[tokio::test]
    async fn task_read_projection_exposes_selected_gap_and_legacy_source_order() {
        let directory =
            std::env::temp_dir().join(format!("swarm-task-source-projection-{}", model::new_id()));
        std::fs::create_dir_all(&directory).unwrap();
        let root = DataRoot::acquire(&directory).unwrap();
        let credential = bootstrap_credential(&root.path).unwrap();
        let mut config = Config::default();
        config.storage.data_dir = directory.clone();
        let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
            .await
            .unwrap();
        let principal = owner.store.authenticate(credential).await.unwrap();
        let selected_text = "Canonical Issue body.";
        let gap_text = "Unparsed source comment.";
        let created = owner
            .store
            .call(
                principal.clone(),
                "task.create".into(),
                json!({
                    "client_request_id":model::new_id(),
                    "project_id":"fixture",
                    "spec":{
                        "objective":"Project source inputs",
                        "phase":"implementation",
                        "requirements":[{"id":"R1","statement":"Retain source order and gaps"}],
                        "source_refs":["issue:14/comment:2","canonical:design-doc"],
                        "source_index":[
                            {
                                "source_ref":"issue:14/body",
                                "revision":"issue-revision-3",
                                "content_sha256":model::digest(selected_text.as_bytes()),
                                "text":selected_text,
                                "status":"selected"
                            },
                            {
                                "source_ref":"issue:14/comment:2",
                                "revision":"issue-revision-3",
                                "text":gap_text,
                                "status":"gap",
                                "gap_reason":"comment_parse_incomplete"
                            }
                        ]
                    }
                }),
            )
            .await
            .unwrap();
        let task_id = created["task_id"].as_str().unwrap().to_owned();
        let task = owner
            .store
            .call(
                principal.clone(),
                "task.get".into(),
                json!({"task_id":task_id}),
            )
            .await
            .unwrap();
        {
            let indexed = task["task_brief"]["source_index"].as_array().unwrap();
            assert_eq!(indexed.len(), 3);
            assert_eq!(indexed[0]["source_ref"], "issue:14/body");
            assert_eq!(indexed[0]["status"], "selected");
            assert_eq!(
                indexed[0]["content_sha256"],
                model::digest(selected_text.as_bytes())
            );
            assert_eq!(indexed[1]["source_ref"], "issue:14/comment:2");
            assert_eq!(indexed[1]["status"], "gap");
            assert_eq!(indexed[1]["text"], gap_text);
            assert_eq!(
                indexed[1]["content_sha256"],
                model::digest(gap_text.as_bytes())
            );
            assert_eq!(indexed[1]["gap_reason"], "comment_parse_incomplete");
            assert_eq!(indexed[2]["source_ref"], "canonical:design-doc");
            assert_eq!(indexed[2]["status"], "gap");
            assert_eq!(
                indexed[2]["gap_reason"],
                "legacy_source_ref_without_pinned_revision_or_content"
            );
        }

        let listed = owner
            .store
            .call(principal, "task.list".into(), json!({"limit":10,"after":0}))
            .await
            .unwrap();
        assert_eq!(listed["items"][0]["task_brief"], task["task_brief"]);
        owner.close().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn legacy_attempt_projection_does_not_assign_current_policy() {
        let attempt = project_attempt(json!({
            "task_snapshot": {"revision": 1, "spec": {"objective": "historical"}}
        }));

        assert_eq!(attempt["owner_policy"], json!({"status":"legacy_unknown"}));
        assert!(attempt["task_snapshot"].get("owner_policy").is_none());
        assert_eq!(attempt["task_brief"], Value::Null);
    }

    #[tokio::test]
    async fn claim_rejects_missing_or_unknown_policy_before_creating_an_attempt() {
        let directory =
            std::env::temp_dir().join(format!("swarm-policy-claim-{}", model::new_id()));
        std::fs::create_dir_all(&directory).unwrap();
        let root = DataRoot::acquire(&directory).unwrap();
        let credential = bootstrap_credential(&root.path).unwrap();
        let mut config = Config::default();
        config.storage.data_dir = directory.clone();
        let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
            .await
            .unwrap();
        let operator = owner.store.authenticate(credential).await.unwrap();

        for (owner_policy_id, expected_error) in [
            (None, "OWNER_POLICY_REQUIRED"),
            (Some("owner-policy-v999"), "OWNER_POLICY_UNKNOWN"),
        ] {
            let mut spec = json!({
                "objective":"Policy claim test",
                "phase":"implementation",
                "requirements":[{"id":"R1","statement":"Reject unknown policy before reservation"}],
            });
            if let Some(policy_id) = owner_policy_id {
                spec["owner_policy_id"] = json!(policy_id);
            }
            let task = owner
                .store
                .call(
                    operator.clone(),
                    "task.create".into(),
                    json!({
                        "client_request_id":model::new_id(),
                        "project_id":"fixture",
                        "spec":spec,
                    }),
                )
                .await
                .unwrap();
            let task_id = task["task_id"].as_str().unwrap();
            let error = owner
                .store
                .call(
                    operator.clone(),
                    "task.claim".into(),
                    json!({
                        "client_request_id":model::new_id(),
                        "task_id":task_id,
                        "expected_revision":1,
                    }),
                )
                .await
                .unwrap_err();
            assert_eq!(error.code, expected_error);

            let stored_task = owner
                .store
                .call(
                    operator.clone(),
                    "task.get".into(),
                    json!({"task_id":task_id}),
                )
                .await
                .unwrap();
            assert_eq!(stored_task["current_attempt_id"], Value::Null);
        }

        owner.close().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }
}
