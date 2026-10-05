//! Decision ownership, exact anchors and revocation history. File verification is
//! performed by the async Store facade; no filesystem work runs on the DB thread.
use super::{current_principal, operations, results, submissions, tasks};
use crate::{
    acceptance::{AcceptRequest, InvalidateRequest},
    artifacts::ArtifactRecord,
    automation::acceptance::AcceptanceContext,
    checks::{model::CheckProfile, worker},
    error::{Error, Result},
    model::{self, Dependency, Principal, TaskSpec},
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use std::collections::BTreeSet;

fn acceptance_validation_error(error: swarm_kernel::acceptance::ValidationError) -> Error {
    Error::new(error.code(), error.message())
}

pub(super) fn feedback_cursor(db: &Connection, submission: &str) -> Result<i64> {
    Ok(db.query_row(
        "SELECT coalesce(max(observation_id),0) FROM observations WHERE kind='task.feedback' AND (json_extract(payload_json,'$.finding.submission_ref')=?1 OR json_extract(payload_json,'$.submission_ref')=?1)",
        [submission], |r| r.get(0),
    )?)
}

fn revoked(db: &Connection, decision_id: &str) -> Result<bool> {
    Ok(db.query_row(
        "SELECT EXISTS(SELECT 1 FROM observations WHERE source_stream_id='controller:acceptance' AND source_event_key=?1)",
        [format!("invalidate:{decision_id}")], |r| r.get(0),
    )?)
}

/// Only original successful decisions qualify, not a coalesced request receipt.
fn decision(db: &Connection, id: &str) -> Result<Value> {
    let op = operations::get_operation(db, id)?;
    let value = &op["result"];
    if op["method"] != "task.accept"
        || op["state"] != "settled"
        || value["outcome"] != "applied"
        || value["acceptance_operation_id"] != id
        || value["task_id"] != op["task_id"]
        || value["attempt_id"] != op["attempt_id"]
    {
        return Err(Error::new(
            "NOT_ACCEPTANCE",
            "operation is not a committed acceptance decision",
        ));
    }
    Ok(value.clone())
}

pub(super) fn describe(db: &Connection, v: &Value) -> Result<Value> {
    model::fields(v, &["acceptance_operation_id"])?;
    let id = model::text(v, "acceptance_operation_id")?;
    let mut value = decision(db, id)?;
    let t = tasks::get_task(db, model::text(&value, "task_id")?)?;
    value["invalidated"] = json!(revoked(db, id)?);
    value["current"] = json!(t["accepted_operation_id"] == id);
    Ok(value)
}

/// A newer revision does not erase a valid pinned decision for an older revision.
pub(super) fn resolve_dependency(db: &Connection, dependency: &Dependency) -> Result<String> {
    let id: Option<String> = db.query_row(
        "SELECT operation_id FROM operations o WHERE method='task.accept' AND state='settled' AND task_id=?1 AND json_extract(result_json,'$.outcome')='applied' AND json_extract(result_json,'$.acceptance_operation_id')=operation_id AND json_extract(result_json,'$.task_revision')=?2 AND json_extract(result_json,'$.phase')=?3 AND NOT EXISTS(SELECT 1 FROM observations WHERE source_stream_id='controller:acceptance' AND source_event_key='invalidate:'||o.operation_id) ORDER BY created_at_ms DESC,operation_id DESC LIMIT 1",
        params![dependency.task_id, dependency.required_revision, dependency.required_phase], |r| r.get(0),
    ).optional()?;
    id.ok_or_else(|| {
        Error::new(
            "DEPENDENCY_NOT_READY",
            format!(
                "{} revision {} phase {}",
                dependency.task_id, dependency.required_revision, dependency.required_phase
            ),
        )
    })
}

pub(super) fn accepted_attempt(db: &Connection, attempt: &Value) -> Result<bool> {
    Ok(db.query_row(
        "SELECT EXISTS(SELECT 1 FROM operations o WHERE method='task.accept' AND state='settled' AND attempt_id=?1 AND json_extract(result_json,'$.outcome')='applied' AND json_extract(result_json,'$.acceptance_operation_id')=operation_id AND json_extract(result_json,'$.submission_ref')=?2 AND json_extract(result_json,'$.candidate_ref')=?3 AND NOT EXISTS(SELECT 1 FROM observations WHERE source_stream_id='controller:acceptance' AND source_event_key='invalidate:'||o.operation_id))",
        params![attempt["attempt_id"].as_str(), attempt["submission_ref"].as_str(), attempt["candidate_ref"].as_str()], |row| row.get(0),
    )?)
}

/// Freeze only a current accepted source candidate from the same project. A
/// missing, stale, cross-project or otherwise unproven reference is preserved
/// as an explicit wide-scope reason; it does not prevent claiming the Task.
pub(super) fn freeze_baseline_candidate(
    db: &Connection,
    project_id: &str,
    candidate_ref: Option<&str>,
) -> Result<Value> {
    let Some(candidate_ref) = candidate_ref else {
        return Ok(json!({"status":"wide","reason":"baseline_not_configured"}));
    };
    let candidate = match results::get(db, candidate_ref) {
        Ok(candidate) => candidate,
        Err(error) if error.code == "NOT_FOUND" => {
            return Ok(json!({
                "status":"wide",
                "candidate_ref":candidate_ref,
                "reason":"baseline_artifact_unregistered"
            }));
        }
        Err(error) => return Err(error),
    };
    if candidate.kind != "source_snapshot" || candidate.metadata["coverage"] != "complete" {
        return Ok(json!({
            "status":"wide",
            "candidate_ref":candidate_ref,
            "reason":"baseline_not_complete_source_snapshot"
        }));
    }

    let task_ids = {
        let mut statement = db.prepare(
            "SELECT task_id FROM tasks WHERE project_id=?1 AND state='accepted' AND accepted_candidate_ref=?2 ORDER BY task_id",
        )?;
        statement
            .query_map(params![project_id, candidate_ref], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let mut matches = Vec::new();
    for task_id in task_ids {
        let task = tasks::get_task(db, &task_id)?;
        let attempt_id = model::text(&task, "accepted_attempt_id")?;
        let attempt = tasks::get_attempt(db, attempt_id)?;
        let operation_id = model::text(&task, "accepted_operation_id")?;
        let accepted = match decision(db, operation_id) {
            Ok(accepted) => accepted,
            Err(error) if matches!(error.code.as_str(), "NOT_ACCEPTANCE" | "NOT_FOUND") => {
                continue;
            }
            Err(error) => return Err(error),
        };
        if revoked(db, operation_id)?
            || task["project_id"] != project_id
            || task["accepted_candidate_ref"] != candidate_ref
            || attempt["task_id"] != task_id
            || attempt["task_revision"] != task["accepted_revision"]
            || attempt["state"] != "accepted"
            || attempt["candidate_ref"] != candidate_ref
            || accepted["task_id"] != task_id
            || accepted["attempt_id"] != attempt_id
            || accepted["task_revision"] != task["accepted_revision"]
            || accepted["candidate_ref"] != candidate_ref
            || candidate.metadata["task_id"] != task_id
            || candidate.metadata["attempt_id"] != attempt_id
            || candidate.metadata["task_revision"] != task["accepted_revision"]
            || !accepted_attempt(db, &attempt)?
        {
            continue;
        }
        matches.push(json!({
            "status":"verified",
            "project_id":project_id,
            "task_id":task_id,
            "task_revision":task["accepted_revision"],
            "attempt_id":attempt_id,
            "acceptance_operation_id":operation_id,
            "candidate_ref":candidate_ref,
            "content_sha256":candidate.content_digest,
            "byte_length":candidate.byte_length
        }));
    }
    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Ok(json!({
            "status":"wide",
            "candidate_ref":candidate_ref,
            "reason":"baseline_lacks_current_same_project_acceptance"
        })),
        _ => Ok(json!({
            "status":"wide",
            "candidate_ref":candidate_ref,
            "reason":"baseline_acceptance_provenance_ambiguous"
        })),
    }
}

fn validate_dependencies(db: &Connection, attempt: &Value, spec: &TaskSpec) -> Result<()> {
    let dependency_task_ids: Vec<&str> = spec
        .dependencies
        .iter()
        .map(|dependency| dependency.task_id.as_str())
        .collect();
    let acceptance_ids =
        swarm_kernel::acceptance::dependency_receipt_ids(attempt, &dependency_task_ids)
            .map_err(acceptance_validation_error)?;
    for (dependency, id) in spec.dependencies.iter().zip(acceptance_ids) {
        let d = decision(db, &id)?;
        if revoked(db, &id)?
            || d["task_id"] != dependency.task_id
            || d["task_revision"] != dependency.required_revision
            || d["phase"] != dependency.required_phase
        {
            return Err(Error::new(
                "DEPENDENCY_REVALIDATION_REQUIRED",
                format!("pinned decision {id} is no longer valid; candidate is retained"),
            ));
        }
    }
    Ok(())
}

/// Materialize the precise evidence to be byte-verified off-thread. Every call
/// rechecks anchors/policy; finish additionally compares this manifest to begin.
enum AcceptActor<'a> {
    Direct(&'a Principal),
    OnBehalf(&'a AcceptanceContext),
}

impl AcceptActor<'_> {
    fn require_current(&self, db: &Connection) -> Result<()> {
        match self {
            Self::Direct(principal) => super::gm::require_authority(db, principal),
            Self::OnBehalf(context) => context.require_current_action(db),
        }
    }

    fn require_exact_scope(
        &self,
        db: &Connection,
        task_id: &str,
        input: &AcceptRequest,
    ) -> Result<()> {
        match self {
            Self::Direct(_) => Ok(()),
            Self::OnBehalf(context) => context.require_action_object(
                db,
                "task.accept",
                task_id,
                input.expected_revision,
                &input.attempt_id,
                &input.submission_ref,
                &input.candidate_ref,
                input.expected_feedback_observation_id,
                &input.check_ids,
            ),
        }
    }

    fn reviewer_id(&self) -> &str {
        match self {
            Self::Direct(principal) => &principal.client_id,
            Self::OnBehalf(context) => context.effective_manager_id(),
        }
    }

    fn automation_linkage(&self) -> Option<Value> {
        match self {
            Self::Direct(_) => None,
            Self::OnBehalf(context) => Some(context.linkage_value()),
        }
    }

    fn is_on_behalf(&self) -> bool {
        match self {
            Self::Direct(_) => false,
            Self::OnBehalf(_) => true,
        }
    }
}

fn evidence(
    db: &Connection,
    actor: &AcceptActor<'_>,
    input: &AcceptRequest,
) -> Result<(Value, Vec<ArtifactRecord>)> {
    actor.require_current(db)?;
    let doc = submissions::document(db, &input.submission_ref)?;
    let a = tasks::get_attempt(db, &input.attempt_id)?;
    let task_id = model::text(&a, "task_id")?;
    let t = tasks::get_task(db, task_id)?;
    actor.require_exact_scope(db, task_id, input)?;
    swarm_kernel::acceptance::validate_submission_scope(
        &doc,
        &a,
        &t,
        &input.attempt_id,
        input.expected_revision,
        &input.submission_ref,
        &input.candidate_ref,
    )
    .map_err(acceptance_validation_error)?;
    swarm_kernel::acceptance::validate_reviewer_independence(&doc, actor.reviewer_id())
        .map_err(acceptance_validation_error)?;
    if feedback_cursor(db, &input.submission_ref)? != input.expected_feedback_observation_id {
        return Err(Error::new(
            "REVIEW_CHANGED",
            "feedback changed; inspect it before accepting this same candidate",
        ));
    }
    let spec: TaskSpec = serde_json::from_value(a["task_snapshot"]["spec"].clone())?;
    let spec_value = serde_json::to_value(&spec)?;
    swarm_kernel::acceptance::validate_acceptance_required(&spec_value)
        .map_err(acceptance_validation_error)?;
    let policy = spec.acceptance.as_ref().ok_or_else(|| {
        Error::new(
            "ACCEPTANCE_POLICY_REQUIRED",
            "Task has no explicit acceptance policy; assignment and submission remain available",
        )
    })?;
    input.validate_coverage(&spec)?;
    validate_dependencies(db, &a, &spec)?;
    let candidate = results::get(db, &input.candidate_ref)?;
    swarm_kernel::acceptance::validate_candidate_identity(
        &candidate.content_digest,
        candidate.byte_length,
        doc["candidate_sha256"].as_str(),
        doc["candidate_byte_length"].as_u64(),
    )
    .map_err(acceptance_validation_error)?;
    let mut files = vec![results::get(db, &input.submission_ref)?, candidate];
    let policy_value = serde_json::to_value(policy)?;
    let mut profiles = BTreeSet::new();
    let mut checks = Vec::new();
    for id in &input.check_ids {
        let raw: Option<String> = db.query_row(
            "SELECT json_object('check_id',check_id,'operation_id',operation_id,'attempt_id',attempt_id,'candidate_ref',candidate_ref,'state',state,'exit_code',exit_code,'released_at_ms',resource_released_at_ms,'cached_from',cached_from_check_id,'result_ref',result_ref,'spec',json(spec_json),'coverage',json(coverage_json)) FROM check_runs WHERE check_id=?1",
            [id], |r| r.get(0),
        ).optional()?;
        let c: Value =
            serde_json::from_str(&raw.ok_or_else(|| Error::new("CHECK_NOT_READY", id))?)?;
        let profile = swarm_kernel::acceptance::validate_check(
            &policy_value,
            &c,
            &input.attempt_id,
            &input.candidate_ref,
            &profiles,
        )
        .map_err(acceptance_validation_error)?;
        profiles.insert(profile);
        let check_profile: CheckProfile = serde_json::from_value(c["spec"]["profile"].clone())
            .map_err(|_| Error::new("CHECK_EVIDENCE_MISSING", "check profile is malformed"))?;
        let expected_targets: Vec<String> =
            if c["spec"]["resolved_inputs"]["expected_targets"].is_array() {
                serde_json::from_value(c["spec"]["resolved_inputs"]["expected_targets"].clone())
                    .map_err(|_| {
                        Error::new("CHECK_EVIDENCE_MISSING", "resolved targets are malformed")
                    })?
            } else {
                check_profile.expected_targets.clone()
            };
        worker::validate_passed_coverage(
            &check_profile.parser,
            &expected_targets,
            &c["spec"]["scope_plan"],
            &c["coverage"],
        )
        .map_err(|_| Error::new("CHECK_INCOMPLETE", "check parser coverage is incomplete"))?;
        swarm_kernel::acceptance::validate_check_profile_identity(
            &c["spec"]["resolved_inputs"],
            &json!(check_profile.parser),
        )
        .map_err(acceptance_validation_error)?;
        let op = operations::get_operation(db, model::text(&c, "operation_id")?)?;
        swarm_kernel::acceptance::validate_check_operation(&op, id, &c["result_ref"])
            .map_err(acceptance_validation_error)?;
        if !c["cached_from"].is_null() {
            // Cache rows point directly at one original, completed process row.
            // Input identity is stable across Tasks/Attempts, so the full spec
            // JSON is intentionally not required to match.
            let source = model::text(&c, "cached_from")?;
            let source_row: Option<(String, String, String, Option<String>)> = db
                .query_row(
                    "SELECT s.candidate_ref,s.spec_json,s.coverage_json,s.process_identity_json FROM check_runs s JOIN check_runs c ON c.check_id=?1 WHERE s.check_id=?2 AND s.cached_from_check_id IS NULL AND s.state='passed' AND s.exit_code=0 AND s.resource_released_at_ms IS NOT NULL AND s.cache_key=c.cache_key AND s.result_ref=c.result_ref AND s.coverage_json=c.coverage_json AND json_extract(s.spec_json,'$.cache_policy')='reusable' AND json_extract(s.spec_json,'$.reproducible')=1 AND json_extract(s.spec_json,'$.input_fingerprint')=json_extract(c.spec_json,'$.input_fingerprint') AND json_extract(s.spec_json,'$.resolved_inputs')=json_extract(c.spec_json,'$.resolved_inputs') AND json_extract(s.spec_json,'$.scope_plan')=json_extract(c.spec_json,'$.scope_plan') AND EXISTS(SELECT 1 FROM operations op WHERE op.operation_id=s.operation_id AND op.method='check.run' AND op.state='settled' AND json_extract(op.result_json,'$.outcome')='applied' AND json_extract(op.result_json,'$.state')='passed' AND json_extract(op.result_json,'$.exit_code')=0 AND json_extract(op.result_json,'$.source_checkout_verified')=1 AND json_extract(op.result_json,'$.cached') IS NULL AND json_extract(op.result_json,'$.cached_from_check_id') IS NULL AND json_extract(op.result_json,'$.check_id')=s.check_id AND json_extract(op.result_json,'$.result_ref')=s.result_ref))",
                    params![id, source],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            let source_candidate_ref = source_row.as_ref().map(|row| row.0.clone());
            if let Some((_, spec_raw, coverage_raw, process_raw)) = source_row.as_ref() {
                let source_spec: Value = serde_json::from_str(spec_raw)?;
                let source_profile: CheckProfile =
                    serde_json::from_value(source_spec["profile"].clone()).map_err(|_| {
                        Error::new("CHECK_NOT_READY", "cached source profile is malformed")
                    })?;
                let source_targets: Vec<String> =
                    if source_spec["resolved_inputs"]["expected_targets"].is_array() {
                        serde_json::from_value(
                            source_spec["resolved_inputs"]["expected_targets"].clone(),
                        )
                        .map_err(|_| {
                            Error::new("CHECK_NOT_READY", "cached source targets are malformed")
                        })?
                    } else {
                        source_profile.expected_targets.clone()
                    };
                let source_coverage: Value = serde_json::from_str(coverage_raw)?;
                let source_process: Value = process_raw
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?
                    .ok_or_else(|| {
                        Error::new("CHECK_NOT_READY", "cached source process is missing")
                    })?;
                let source_process_valid =
                    super::checks::valid_process_receipt(&source_process, &source_spec);
                let source_coverage_valid = worker::validate_passed_coverage(
                    &source_profile.parser,
                    &source_targets,
                    &source_spec["scope_plan"],
                    &source_coverage,
                )
                .is_ok();
                swarm_kernel::acceptance::validate_cached_source(
                    &source_spec,
                    &json!(source_profile.parser),
                    source_process_valid,
                    source_coverage_valid,
                )
                .map_err(acceptance_validation_error)?;
            }
            let original = operations::get_operation(db, source)?;
            swarm_kernel::acceptance::validate_cached_operation(
                source_candidate_ref.is_some(),
                &original,
                &op,
                source,
            )
            .map_err(acceptance_validation_error)?;
            let source_candidate_ref = source_candidate_ref
                .ok_or_else(|| Error::new("CHECK_NOT_READY", "cached process row disappeared"))?;
            let source_candidate = results::get(db, &source_candidate_ref)?;
            let current_acceptance = freeze_baseline_candidate(
                db,
                model::text(&t, "project_id")?,
                Some(&source_candidate_ref),
            )?;
            if current_acceptance["status"] != "verified"
                || model::canonical(&current_acceptance)?
                    != model::canonical(&c["spec"]["cache_source_acceptance"])?
            {
                return Err(Error::new(
                    "CHECK_NOT_READY",
                    "cached source is no longer the same accepted project candidate",
                ));
            }
            files.push(source_candidate);
        } else if c["exit_code"] != 0 || c["released_at_ms"].is_null() {
            return Err(Error::new(
                "CHECK_NOT_READY",
                "check process/resource has not completed",
            ));
        }
        files.push(results::get(db, model::text(&c, "result_ref")?)?);
        let output_refs = op["result"]["output_refs"]
            .as_array()
            .ok_or_else(|| Error::new("CHECK_EVIDENCE_MISSING", "check output list is absent"))?;
        let output_owner = c["cached_from"].as_str().unwrap_or(id.as_str());
        for reference in output_refs {
            let reference = reference.as_str().ok_or_else(|| {
                Error::new(
                    "CHECK_EVIDENCE_MISSING",
                    "check output reference is malformed",
                )
            })?;
            let output = results::get(db, reference)?;
            if output.kind != "check_output" || output.metadata["check_id"] != output_owner {
                return Err(Error::new(
                    "CHECK_EVIDENCE_MISSING",
                    "check output is not owned by its original process",
                ));
            }
            files.push(output);
        }
        checks.push(c);
    }
    swarm_kernel::acceptance::validate_checks_complete(
        profiles.len(),
        policy.required_check_profiles.len(),
    )
    .map_err(acceptance_validation_error)?;
    let manifest: Vec<_> = files.iter().map(|f| json!({"artifact_id":f.artifact_id,"sha256":f.content_digest,"length":f.byte_length,"path":f.relative_path})).collect();
    Ok((
        json!({"task_id":a["task_id"],"phase":spec.phase,"policy":policy,"artifacts":manifest,"checks":checks,
        "dependency_acceptances":a["task_snapshot"]["dependency_acceptances"]}),
        files,
    ))
}

fn reserve_with_actor(
    tx: &Transaction<'_>,
    actor: &AcceptActor<'_>,
    v: &Value,
    id: &str,
) -> Result<Value> {
    let input = AcceptRequest::parse(v)?;
    let a = tasks::get_attempt(tx, &input.attempt_id)?;
    let task_id = model::text(&a, "task_id")?;
    let t = tasks::get_task(tx, task_id)?;
    actor.require_current(tx)?;
    actor.require_exact_scope(tx, task_id, &input)?;
    if let Some(prior) = t["accepted_operation_id"].as_str() {
        let d = decision(tx, prior)?;
        if d["attempt_id"] == input.attempt_id
            && d["task_revision"] == input.expected_revision
            && d["submission_ref"] == input.submission_ref
            && d["candidate_ref"] == input.candidate_ref
            && !revoked(tx, prior)?
        {
            if actor.is_on_behalf() {
                swarm_kernel::acceptance::validate_coalesced_scope(
                    &t,
                    input.expected_revision,
                    &input.attempt_id,
                )
                .map_err(acceptance_validation_error)?;
                let doc = submissions::document(tx, &input.submission_ref)?;
                swarm_kernel::acceptance::validate_reviewer_independence(&doc, actor.reviewer_id())
                    .map_err(acceptance_validation_error)?;
            }
            if let Some(linkage) = actor.automation_linkage() {
                let mut effective = json!({"request":v});
                effective["automation_on_behalf"] = linkage;
                tx.execute(
                    "UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=?4 WHERE operation_id=?1",
                    params![
                        id,
                        task_id,
                        input.attempt_id,
                        model::canonical(&effective)?,
                    ],
                )?;
            }
            return Ok(
                json!({"operation_id":prior,"acceptance_operation_id":prior,"coalesced":true}),
            );
        }
        return Err(Error::new(
            "ALREADY_ACCEPTED",
            "Task has a different current acceptance",
        ));
    }
    let (manifest, _) = evidence(tx, actor, &input)?;
    let mut effective = json!({"evidence":manifest});
    if actor.is_on_behalf() {
        effective["request"] = v.clone();
    }
    if let Some(linkage) = actor.automation_linkage() {
        effective["automation_on_behalf"] = linkage;
    }
    tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=?4 WHERE operation_id=?1",
        params![id,task_id,input.attempt_id,model::canonical(&effective)?],
    )?;
    Ok(
        json!({"operation_id":id,"attempt_id":input.attempt_id,"state":"queued","task_accepted":false}),
    )
}

pub(super) fn reserve(tx: &Transaction<'_>, p: &Principal, v: &Value, id: &str) -> Result<Value> {
    super::gm::require_authority(tx, p)?;
    reserve_with_actor(tx, &AcceptActor::Direct(p), v, id)
}

pub(super) fn reserve_on_behalf(
    tx: &Transaction<'_>,
    context: &AcceptanceContext,
    v: &Value,
    id: &str,
) -> Result<Value> {
    let operation = operations::get_operation(tx, id)?;
    if operation["method"] != "task.accept"
        || operation["caller_id"] != context.technical_requester_id()
        || model::canonical(&original_request(tx, id)?)? != model::canonical(v)?
    {
        return Err(Error::new(
            "FORBIDDEN",
            "acceptance Operation is not owned by the retained technical requester",
        ));
    }
    reserve_with_actor(tx, &AcceptActor::OnBehalf(context), v, id)
}

fn request(db: &Connection, id: &str) -> Result<AcceptRequest> {
    AcceptRequest::parse(&original_request(db, id)?)
}

fn original_request(db: &Connection, id: &str) -> Result<Value> {
    let raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [id],
        |r| r.get(0),
    )?;
    Ok(serde_json::from_str(&raw)?)
}
fn manifest(db: &Connection, id: &str) -> Result<Value> {
    let raw: String = db.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1",
        [id],
        |r| r.get(0),
    )?;
    Ok(serde_json::from_str::<Value>(&raw)?["evidence"].clone())
}

fn on_behalf_context(db: &Connection, id: &str, caller_id: &str) -> Result<AcceptanceContext> {
    if caller_id != crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID {
        return Err(Error::new(
            "FORBIDDEN",
            "acceptance Operation is not owned by the automation technical requester",
        ));
    }
    let context = AcceptanceContext::from_committed_operation(db, id)?;
    if context.technical_requester_id() != caller_id {
        return Err(Error::new(
            "FORBIDDEN",
            "acceptance linkage does not retain this technical requester",
        ));
    }
    Ok(context)
}

pub(super) fn begin(
    db: &mut Connection,
    p: Principal,
    id: &str,
) -> Result<Option<Result<Vec<ArtifactRecord>>>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let p = current_principal(&tx, p)?;
    super::gm::require_authority(&tx, &p)?;
    let op = operations::get_operation(&tx, id)?;
    if op["method"] != "task.accept" {
        return Err(Error::invalid("not an acceptance operation"));
    }
    if op["state"] == "settled" {
        return Ok(None);
    }
    if op["caller_id"] != p.client_id {
        return Err(Error::new(
            "FORBIDDEN",
            "decision belongs to another reviewer",
        ));
    }
    if !matches!(op["state"].as_str(), Some("queued" | "outcome_unknown")) {
        return Ok(None);
    }
    let work = (|| -> Result<Vec<ArtifactRecord>> {
        let input = request(&tx, id)?;
        let (current, files) = evidence(&tx, &AcceptActor::Direct(&p), &input)?;
        if current != manifest(&tx, id)? {
            return Err(Error::new(
                "ACCEPTANCE_EVIDENCE_CHANGED",
                "saved evidence no longer matches; candidate retained",
            ));
        }
        Ok(files)
    })();
    tx.execute("UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1", params![id,model::now_ms()?])?;
    tx.commit()?;
    Ok(Some(work))
}

pub(super) fn begin_on_behalf(
    db: &mut Connection,
    id: &str,
) -> Result<Option<Result<Vec<ArtifactRecord>>>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let op = operations::get_operation(&tx, id)?;
    if op["method"] != "task.accept" {
        return Err(Error::invalid("not an acceptance operation"));
    }
    if op["state"] == "settled" {
        return Ok(None);
    }
    let caller_id = model::text(&op, "caller_id")?;
    if caller_id != crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID {
        return Err(Error::new(
            "FORBIDDEN",
            "acceptance Operation is not owned by the automation technical requester",
        ));
    }
    if !matches!(op["state"].as_str(), Some("queued" | "outcome_unknown")) {
        return Ok(None);
    }
    let work = (|| -> Result<Vec<ArtifactRecord>> {
        let context = on_behalf_context(&tx, id, caller_id)?;
        let input = request(&tx, id)?;
        let (current, files) = evidence(&tx, &AcceptActor::OnBehalf(&context), &input)?;
        if current != manifest(&tx, id)? {
            return Err(Error::new(
                "ACCEPTANCE_EVIDENCE_CHANGED",
                "saved evidence no longer matches; candidate retained",
            ));
        }
        Ok(files)
    })();
    tx.execute(
        "UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1",
        params![id, model::now_ms()?],
    )?;
    tx.commit()?;
    Ok(Some(work))
}

pub(super) fn finish(
    db: &mut Connection,
    p: Principal,
    id: &str,
    verified: Result<()>,
) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let op = operations::get_operation(&tx, id)?;
    if op["method"] != "task.accept" || op["caller_id"] != p.client_id || op["state"] != "sending" {
        return Err(Error::conflict(
            "acceptance operation is no longer in its verification phase",
        ));
    }
    let input = request(&tx, id)?;
    let now = model::now_ms()?;
    let reviewer_id = p.client_id.clone();
    let result = verified.and_then(|()| {
        let p = current_principal(&tx, p)?;
        let (current, _) = evidence(&tx, &AcceptActor::Direct(&p), &input)?;
        if current != manifest(&tx, id)? {
            return Err(Error::new(
                "ACCEPTANCE_EVIDENCE_CHANGED",
                "evidence changed during byte verification",
            ));
        }
        Ok(current)
    });
    settle_acceptance(
        &tx,
        id,
        &input,
        now,
        result,
        Some(&reviewer_id),
        swarm_kernel::acceptance::evidence_level(false, !input.check_ids.is_empty()),
    )?;
    tx.commit()?;
    Ok(())
}

pub(super) fn finish_on_behalf(db: &mut Connection, id: &str, verified: Result<()>) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let op = operations::get_operation(&tx, id)?;
    if op["method"] != "task.accept"
        || op["caller_id"] != crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
        || op["state"] != "sending"
    {
        return Err(Error::conflict(
            "acceptance operation is no longer in its verification phase",
        ));
    }
    let input = request(&tx, id)?;
    let caller_id = model::text(&op, "caller_id")?.to_owned();
    let now = model::now_ms()?;
    let mut reviewer_id = None;
    let context = on_behalf_context(&tx, id, &caller_id);
    let result = verified.and_then(|()| {
        let context = context?;
        let (current, _) = evidence(&tx, &AcceptActor::OnBehalf(&context), &input)?;
        if current != manifest(&tx, id)? {
            return Err(Error::new(
                "ACCEPTANCE_EVIDENCE_CHANGED",
                "evidence changed during byte verification",
            ));
        }
        reviewer_id = Some(context.effective_manager_id().to_owned());
        Ok(current)
    });
    settle_acceptance(
        &tx,
        id,
        &input,
        now,
        result,
        reviewer_id.as_deref(),
        swarm_kernel::acceptance::evidence_level(true, !input.check_ids.is_empty()),
    )?;
    tx.commit()?;
    Ok(())
}

fn settle_acceptance(
    tx: &Transaction<'_>,
    id: &str,
    input: &AcceptRequest,
    now: i64,
    decision: Result<Value>,
    reviewer_id: Option<&str>,
    evidence_level: &str,
) -> Result<()> {
    let result = match decision {
        Ok(e) => {
            let reviewer_id = reviewer_id.ok_or_else(|| {
                Error::new(
                    "ACCEPTANCE_ACTOR_MISSING",
                    "verified acceptance has no retained reviewer identity",
                )
            })?;
            tx.execute("UPDATE tasks SET state='accepted',accepted_attempt_id=?2,accepted_operation_id=?3,accepted_revision=?4,accepted_phase=?5,accepted_candidate_ref=?6,updated_at_ms=?7 WHERE task_id=?1",
                params![e["task_id"].as_str(),input.attempt_id,id,input.expected_revision,e["phase"].as_str(),input.candidate_ref,now])?;
            tx.execute(
                "UPDATE attempts SET state='accepted',updated_at_ms=?2 WHERE attempt_id=?1",
                params![input.attempt_id, now],
            )?;
            json!({"operation_id":id,"acceptance_operation_id":id,"outcome":"applied","task_id":e["task_id"],
                "attempt_id":input.attempt_id,"task_revision":input.expected_revision,"phase":e["phase"],
                "submission_ref":input.submission_ref,"candidate_ref":input.candidate_ref,"reviewer_id":reviewer_id,
                "reason":input.reason,"reviews":input.reviews,"check_ids":input.check_ids,
                "feedback_observation_id":input.expected_feedback_observation_id,"evidence_level":evidence_level,
                "source_checkout_verified":!input.check_ids.is_empty(),"task_accepted":true,"ownership_released":false})
        }
        Err(error) => {
            json!({"operation_id":id,"outcome":"failed","error":error,"task_accepted":false})
        }
    };
    tx.execute("UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1",
        params![id,model::canonical(&result)?,now])?;
    super::capacity::sync_attempt(tx, &input.attempt_id, now)?;
    tx.execute("INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:acceptance',?1,?2,'task.acceptance',?3,?4)",
        params![format!("accept:{id}"),id,model::canonical(&result)?,now])?;
    Ok(())
}

pub(super) fn invalidate(
    tx: &Transaction<'_>,
    p: &Principal,
    v: &Value,
    id: &str,
    now: i64,
) -> Result<Value> {
    super::gm::require_authority(tx, p)?;
    let input = InvalidateRequest::parse(v)?;
    let prior = decision(tx, &input.acceptance_operation_id)?;
    let key = format!("invalidate:{}", input.acceptance_operation_id);
    let old: Option<String> = tx.query_row("SELECT payload_json FROM observations WHERE source_stream_id='controller:acceptance' AND source_event_key=?1", [&key], |r|r.get(0)).optional()?;
    if let Some(raw) = old {
        let mut value: Value = serde_json::from_str(&raw)?;
        value["coalesced"] = json!(true);
        return Ok(value);
    }
    let task_id = model::text(&prior, "task_id")?;
    let attempt_id = model::text(&prior, "attempt_id")?;
    let task = tasks::get_task(tx, task_id)?;
    let attempt = tasks::get_attempt(tx, attempt_id)?;
    let current = task["accepted_operation_id"] == input.acceptance_operation_id;
    let notify = current && attempt["released_at_ms"].is_null();
    if current {
        tx.execute("UPDATE tasks SET state='open',accepted_attempt_id=NULL,accepted_operation_id=NULL,accepted_revision=NULL,accepted_phase=NULL,accepted_candidate_ref=NULL,updated_at_ms=?2 WHERE task_id=?1",params![task_id,now])?;
        if notify {
            tx.execute("UPDATE attempts SET state='needs_correction',updated_at_ms=?2 WHERE attempt_id=?1 AND state='accepted'",params![attempt_id,now])?;
        }
        super::capacity::sync_attempt(tx, attempt_id, now)?;
    }
    let value = json!({"operation_id":id,"acceptance_operation_id":input.acceptance_operation_id,"task_id":task_id,"attempt_id":attempt_id,
        "submission_ref":prior["submission_ref"],"candidate_ref":prior["candidate_ref"],"reason":input.reason,"evidence":input.evidence,
        "invalidated":true,"applied_to_current":current,"native_input_sent":false,"writer_started":false,
        "message_id":if notify {Some(id)} else {None},"sender":p.client_id,"recipient":attempt["owner_id"],"text":input.reason});
    tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1",
        params![id, task_id, attempt_id],
    )?;
    tx.execute("INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:acceptance',?1,?2,'task.acceptance_invalidated',?3,?4)",params![key,id,model::canonical(&value)?,now])?;
    if notify {
        tx.execute("INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:review',?1,?2,'task.feedback',?3,?4)",params![format!("revoke:{id}"),id,model::canonical(&value)?,now])?;
    }
    Ok(value)
}
