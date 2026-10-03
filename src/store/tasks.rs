use super::{acceptance, meta, operations};
use crate::{
    error::{Error, Result},
    model::{self, Principal, StartOwner, TaskSpec},
    policy,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};

pub(super) fn get_task(db: &Connection, id: &str) -> Result<Value> {
    let raw:Option<String>=db.query_row("SELECT json_object('task_id',task_id,'project_id',project_id,'revision',revision,'state',state,'origin_key',origin_key,'spec',json(spec_json),'accepted_attempt_id',accepted_attempt_id,'accepted_operation_id',accepted_operation_id,'accepted_revision',accepted_revision,'accepted_phase',accepted_phase,'accepted_candidate_ref',accepted_candidate_ref) FROM tasks WHERE task_id=?1",[id],|r|r.get(0)).optional()?;
    let mut task: Value =
        serde_json::from_str(&raw.ok_or_else(|| Error::new("NOT_FOUND", format!("Task {id}")))?)?;
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
) -> Value {
    json!({
        "spec": spec,
        "revision": revision,
        "dependency_acceptances": dependency_acceptances,
        "owner_policy": owner_policy,
        "brief": spec.brief(),
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
pub(super) fn create(
    tx: &Transaction<'_>,
    p: &Principal,
    v: &Value,
    id: &str,
    now: i64,
) -> Result<Value> {
    p.require_operator()?;
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
    p.require_operator()?;
    model::fields(
        v,
        &["client_request_id", "task_id", "expected_revision", "spec"],
    )?;
    let task_id = model::text(v, "task_id")?;
    let expected = model::positive(v, "expected_revision")?;
    let s = spec(v)?;
    let previous = get_task(tx, task_id)?;
    if previous["revision"] != expected || previous["state"] == "archived" {
        return Err(Error::new("STALE_REVISION", "Task changed or is archived"));
    }
    if s.dependencies.iter().any(|d| d.task_id == task_id) {
        return Err(Error::invalid("Task cannot depend on itself"));
    }
    let next = expected
        .checked_add(1)
        .ok_or_else(|| Error::invalid("revision overflow"))?;
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
pub(super) fn claim(
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
    let owner = v
        .get("owner_id")
        .and_then(Value::as_str)
        .unwrap_or(&p.client_id);
    p.owns(owner)?;
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
    let task = get_task(tx, task_id)?;
    if task["revision"] != revision || task["state"] != "open" {
        return Err(Error::new(
            "STALE_REVISION",
            "Task is not open at the expected revision",
        ));
    }
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
    let mut dependency_receipts = Vec::new();
    for d in &spec.dependencies {
        let accepted = acceptance::resolve_dependency(tx, d)?;
        dependency_receipts.push(json!({"task_id":d.task_id,"acceptance_operation_id":accepted}));
    }
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
        _ => {
            return Err(Error::invalid(
                "binding_id and binding_generation must be supplied together",
            ));
        }
    };
    let attempt = model::new_id();
    let snapshot = task_snapshot(&spec, revision, dependency_receipts, owner_policy);
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
    if !["accepted", "failed", "cancelled", "superseded"].contains(&outcome) {
        return Err(Error::invalid(
            "release outcome must be accepted/failed/cancelled/superseded; accepted requires an existing decision",
        ));
    }
    if v["assignment_closed"] != true {
        return Err(Error::invalid(
            "explicit assignment_closed=true attestation required; no process is stopped by release",
        ));
    }
    let a = get_attempt(tx, attempt_id)?;
    p.owns(model::text(&a, "owner_id")?)?;
    if !a["released_at_ms"].is_null() {
        return Ok(
            json!({"operation_id":id,"attempt_id":attempt_id,"released":true,"changed":false}),
        );
    }
    let accepted = a["state"] == "accepted" && acceptance::accepted_attempt(tx, &a)?;
    if (outcome == "accepted") != accepted {
        return Err(Error::new(
            "ACCEPTANCE_STATE_MISMATCH",
            "accepted ownership must be released as accepted; revoke the decision before changing its outcome",
        ));
    }
    let held: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM check_runs WHERE attempt_id=?1 AND resource_claimed_at_ms IS NOT NULL AND resource_released_at_ms IS NULL)", [attempt_id], |r| r.get(0))?;
    if held {
        return Err(Error::new(
            "CHECK_RESOURCE_HELD",
            "check process disposition is unresolved",
        ));
    }
    let unresolved:i64=tx.query_row("SELECT count(*) FROM operations WHERE attempt_id=?1 AND state IN ('sending','native_accepted','outcome_unknown')",[attempt_id],|r|r.get(0))?;
    if unresolved > 0 {
        return Err(Error::new(
            "OUTCOME_UNKNOWN",
            "resolve already-sent effects before releasing ownership",
        ));
    }
    if a["producers"].as_array().is_some_and(|items| {
        items.iter().any(|item| {
            !matches!(
                item["disposition"].as_str(),
                Some("completed" | "failed" | "cancelled")
            )
        })
    }) {
        return Err(Error::new(
            "NATIVE_WORK_UNRESOLVED",
            "the exact assigned native runs have not ended; attestation cannot override a known producer",
        ));
    }
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
    fn attempt_snapshot_freezes_policy_and_source_gap_without_inventing_text() {
        let text = "Malformed issue comment: acceptance details are unclear.";
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
            source_refs: vec!["docs/owner-decisions.md".to_owned()],
            owner_policy_id: Some(OWNER_POLICY_V1_ID.to_owned()),
            source_index: vec![TaskSourceIndexEntry {
                source_ref: "issue:14/comment:7".to_owned(),
                revision: Some("issue-revision-3".to_owned()),
                content_sha256: None,
                text: Some(text.to_owned()),
                status: SourceIndexStatus::Gap,
                gap_reason: Some("comment_parse_incomplete".to_owned()),
            }],
        };
        spec.validate().unwrap();
        let snapshot = task_snapshot(
            &spec,
            3,
            Vec::new(),
            policy::accepted_edition(spec.owner_policy_id.as_deref()).unwrap(),
        );

        assert_eq!(snapshot["owner_policy"]["status"], "accepted");
        assert_eq!(snapshot["owner_policy"]["edition"], 1);
        assert_eq!(snapshot["brief"]["source_index"][0]["status"], "gap");
        assert_eq!(snapshot["brief"]["source_index"][0]["text"], text);
        assert_eq!(
            snapshot["brief"]["source_index"][0]["content_sha256"],
            model::digest(text.as_bytes())
        );
        assert_eq!(
            snapshot["brief"]["source_index"][0]["gap_reason"],
            "comment_parse_incomplete"
        );
        assert_eq!(snapshot["brief"]["source_index"][1]["status"], "gap");
        assert_eq!(
            snapshot["brief"]["source_index"][1]["gap_reason"],
            "legacy_source_ref_without_pinned_revision_or_content"
        );
        assert_eq!(snapshot["brief"]["objective"], spec.objective);
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
