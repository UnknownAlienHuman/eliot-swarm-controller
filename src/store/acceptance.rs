//! Decision ownership, exact anchors and revocation history. File verification is
//! performed by the async Store facade; no filesystem work runs on the DB thread.
use super::{current_principal, operations, results, submissions, tasks};
use crate::{
    acceptance::{AcceptRequest, InvalidateRequest},
    artifacts::ArtifactRecord,
    checks::{model::CheckProfile, worker},
    error::{Error, Result},
    model::{self, Dependency, Principal, TaskSpec},
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use std::collections::BTreeSet;

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
    let pins = attempt["task_snapshot"]["dependency_acceptances"]
        .as_array()
        .ok_or_else(|| {
            Error::new(
                "DEPENDENCY_EVIDENCE_MISSING",
                "Attempt has no dependency receipt list",
            )
        })?;
    if pins.len() != spec.dependencies.len() {
        return Err(Error::new(
            "DEPENDENCY_EVIDENCE_MISSING",
            "dependency receipts do not cover the frozen Task",
        ));
    }
    for dependency in &spec.dependencies {
        let pin = pins
            .iter()
            .find(|p| p["task_id"] == dependency.task_id)
            .ok_or_else(|| Error::new("DEPENDENCY_EVIDENCE_MISSING", &dependency.task_id))?;
        let id = model::text(pin, "acceptance_operation_id")?;
        let d = decision(db, id)?;
        if revoked(db, id)?
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
fn evidence(
    db: &Connection,
    p: &Principal,
    input: &AcceptRequest,
) -> Result<(Value, Vec<ArtifactRecord>)> {
    super::gm::require_authority(db, p)?;
    let doc = submissions::document(db, &input.submission_ref)?;
    let a = tasks::get_attempt(db, &input.attempt_id)?;
    let t = tasks::get_task(db, model::text(&a, "task_id")?)?;
    if doc["attempt_id"] != input.attempt_id
        || doc["task_revision"] != input.expected_revision
        || doc["candidate_ref"] != input.candidate_ref
        || a["submission_ref"] != input.submission_ref
        || a["candidate_ref"] != input.candidate_ref
        || a["task_revision"] != input.expected_revision
        || t["revision"] != input.expected_revision
        || t["state"] != "open"
        || !a["released_at_ms"].is_null()
        || !matches!(a["state"].as_str(), Some("submitted" | "needs_correction"))
    {
        return Err(Error::new(
            "STALE_SUBMISSION",
            "acceptance no longer targets the current unreleased submission",
        ));
    }
    if doc["owner_id"] == p.client_id || doc["submitted_by"] == p.client_id {
        return Err(Error::new(
            "INDEPENDENT_REVIEW_REQUIRED",
            "the writer/submitter cannot accept its own proposal",
        ));
    }
    if feedback_cursor(db, &input.submission_ref)? != input.expected_feedback_observation_id {
        return Err(Error::new(
            "REVIEW_CHANGED",
            "feedback changed; inspect it before accepting this same candidate",
        ));
    }
    let spec: TaskSpec = serde_json::from_value(a["task_snapshot"]["spec"].clone())?;
    let policy = spec.acceptance.as_ref().ok_or_else(|| {
        Error::new(
            "ACCEPTANCE_POLICY_REQUIRED",
            "Task has no explicit acceptance policy; assignment and submission remain available",
        )
    })?;
    input.validate_coverage(&spec)?;
    validate_dependencies(db, &a, &spec)?;
    let candidate = results::get(db, &input.candidate_ref)?;
    if candidate.content_digest != doc["candidate_sha256"].as_str().unwrap_or("")
        || Some(candidate.byte_length) != doc["candidate_byte_length"].as_u64()
    {
        return Err(Error::new(
            "CANDIDATE_DAMAGED",
            "candidate identity no longer matches the sealed submission",
        ));
    }
    let mut files = vec![results::get(db, &input.submission_ref)?, candidate];
    let mut profiles = BTreeSet::new();
    let mut checks = Vec::new();
    for id in &input.check_ids {
        let raw: Option<String> = db.query_row(
            "SELECT json_object('check_id',check_id,'operation_id',operation_id,'attempt_id',attempt_id,'candidate_ref',candidate_ref,'state',state,'exit_code',exit_code,'released_at_ms',resource_released_at_ms,'cached_from',cached_from_check_id,'result_ref',result_ref,'spec',json(spec_json),'coverage',json(coverage_json)) FROM check_runs WHERE check_id=?1",
            [id], |r| r.get(0),
        ).optional()?;
        let c: Value =
            serde_json::from_str(&raw.ok_or_else(|| Error::new("CHECK_NOT_READY", id))?)?;
        let profile = model::text(&c["spec"], "profile_id")?;
        let revision = model::text(&c["spec"], "profile_revision")?;
        if !policy
            .required_check_profiles
            .iter()
            .any(|r| r.profile_id == profile && r.profile_revision == revision)
            || !profiles.insert(profile.to_owned())
        {
            return Err(Error::new(
                "CHECK_PROFILE_MISMATCH",
                "check does not match a unique required profile revision",
            ));
        }
        if c["attempt_id"] != input.attempt_id
            || c["candidate_ref"] != input.candidate_ref
            || c["state"] != "passed"
            || c["coverage"]["gaps"]
                .as_array()
                .is_none_or(|g| !g.is_empty())
        {
            return Err(Error::new(
                "CHECK_NOT_READY",
                "required check is not a complete pass for this Attempt/candidate",
            ));
        }
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
        if c["spec"]["resolved_inputs"]["profile_identity"].is_object()
            && c["spec"]["resolved_inputs"]["profile_identity"]["parser"]
                != json!(check_profile.parser)
        {
            return Err(Error::new(
                "CHECK_EVIDENCE_MISSING",
                "check profile differs from its resolved identity",
            ));
        }
        let op = operations::get_operation(db, model::text(&c, "operation_id")?)?;
        if op["method"] != "check.run"
            || op["state"] != "settled"
            || op["result"]["outcome"] != "applied"
            || op["result"]["source_checkout_verified"] != true
            || op["result"]["check_id"] != id.as_str()
            || op["result"]["result_ref"] != c["result_ref"]
        {
            return Err(Error::new(
                "CHECK_EVIDENCE_MISSING",
                "check has no matching controller execution receipt",
            ));
        }
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
                if !source_spec["cache_source_acceptance"].is_null()
                    || !source_spec["cached_from_check_id"].is_null()
                    || source_spec["resolved_inputs"]["profile_identity"]["parser"]
                        != json!(source_profile.parser)
                    || !super::checks::valid_process_receipt(&source_process, &source_spec)
                    || worker::validate_passed_coverage(
                        &source_profile.parser,
                        &source_targets,
                        &source_spec["scope_plan"],
                        &source_coverage,
                    )
                    .is_err()
                {
                    return Err(Error::new(
                        "CHECK_NOT_READY",
                        "cached source is not an original parser-complete process result",
                    ));
                }
            }
            let original = operations::get_operation(db, source)?;
            let valid = source_candidate_ref.is_some()
                && original["method"] == "check.run"
                && original["state"] == "settled"
                && original["result"]["cached"].is_null()
                && original["result"]["cached_from_check_id"].is_null()
                && original["result"]["output_refs"] == op["result"]["output_refs"]
                && op["result"]["cached_from_check_id"] == source;
            if !valid {
                return Err(Error::new(
                    "CHECK_NOT_READY",
                    "cached process evidence is not valid",
                ));
            }
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
    if profiles.len() != policy.required_check_profiles.len() {
        return Err(Error::new(
            "CHECKS_REQUIRED",
            "required machine checks are missing; a review cannot substitute for them",
        ));
    }
    let manifest: Vec<_> = files.iter().map(|f| json!({"artifact_id":f.artifact_id,"sha256":f.content_digest,"length":f.byte_length,"path":f.relative_path})).collect();
    Ok((
        json!({"task_id":a["task_id"],"phase":spec.phase,"policy":policy,"artifacts":manifest,"checks":checks,
        "dependency_acceptances":a["task_snapshot"]["dependency_acceptances"]}),
        files,
    ))
}

pub(super) fn reserve(tx: &Transaction<'_>, p: &Principal, v: &Value, id: &str) -> Result<Value> {
    super::gm::require_authority(tx, p)?;
    let input = AcceptRequest::parse(v)?;
    let a = tasks::get_attempt(tx, &input.attempt_id)?;
    let t = tasks::get_task(tx, model::text(&a, "task_id")?)?;
    if let Some(prior) = t["accepted_operation_id"].as_str() {
        let d = decision(tx, prior)?;
        if d["attempt_id"] == input.attempt_id
            && d["task_revision"] == input.expected_revision
            && d["submission_ref"] == input.submission_ref
            && d["candidate_ref"] == input.candidate_ref
            && !revoked(tx, prior)?
        {
            return Ok(
                json!({"operation_id":prior,"acceptance_operation_id":prior,"coalesced":true}),
            );
        }
        return Err(Error::new(
            "ALREADY_ACCEPTED",
            "Task has a different current acceptance",
        ));
    }
    let (manifest, _) = evidence(tx, p, &input)?;
    tx.execute("UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=?4 WHERE operation_id=?1",
        params![id,a["task_id"].as_str(),input.attempt_id,model::canonical(&json!({"evidence":manifest}))?])?;
    Ok(
        json!({"operation_id":id,"attempt_id":input.attempt_id,"state":"queued","task_accepted":false}),
    )
}

fn request(db: &Connection, id: &str) -> Result<AcceptRequest> {
    let raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [id],
        |r| r.get(0),
    )?;
    AcceptRequest::parse(&serde_json::from_str(&raw)?)
}
fn manifest(db: &Connection, id: &str) -> Result<Value> {
    let raw: String = db.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1",
        [id],
        |r| r.get(0),
    )?;
    Ok(serde_json::from_str::<Value>(&raw)?["evidence"].clone())
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
        let (current, files) = evidence(&tx, &p, &input)?;
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
    let result = verified.and_then(|()| {
        let p = current_principal(&tx, p)?;
        let (current, _) = evidence(&tx, &p, &input)?;
        if current != manifest(&tx, id)? {
            return Err(Error::new(
                "ACCEPTANCE_EVIDENCE_CHANGED",
                "evidence changed during byte verification",
            ));
        }
        Ok(current)
    });
    let result = match result {
        Ok(e) => {
            tx.execute("UPDATE tasks SET state='accepted',accepted_attempt_id=?2,accepted_operation_id=?3,accepted_revision=?4,accepted_phase=?5,accepted_candidate_ref=?6,updated_at_ms=?7 WHERE task_id=?1",
                params![e["task_id"].as_str(),input.attempt_id,id,input.expected_revision,e["phase"].as_str(),input.candidate_ref,now])?;
            tx.execute(
                "UPDATE attempts SET state='accepted',updated_at_ms=?2 WHERE attempt_id=?1",
                params![input.attempt_id, now],
            )?;
            json!({"operation_id":id,"acceptance_operation_id":id,"outcome":"applied","task_id":e["task_id"],
                "attempt_id":input.attempt_id,"task_revision":input.expected_revision,"phase":e["phase"],
                "submission_ref":input.submission_ref,"candidate_ref":input.candidate_ref,"reviewer_id":op["caller_id"],
                "reason":input.reason,"reviews":input.reviews,"check_ids":input.check_ids,
                "feedback_observation_id":input.expected_feedback_observation_id,"evidence_level":if input.check_ids.is_empty(){"operator_review"}else{"operator_review_with_checks"},
                "source_checkout_verified":!input.check_ids.is_empty(),"task_accepted":true,"ownership_released":false})
        }
        Err(error) => {
            json!({"operation_id":id,"outcome":"failed","error":error,"task_accepted":false})
        }
    };
    tx.execute("UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1",
        params![id,model::canonical(&result)?,now])?;
    super::capacity::sync_attempt(&tx, &input.attempt_id, now)?;
    tx.execute("INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:acceptance',?1,?2,'task.acceptance',?3,?4)",
        params![format!("accept:{id}"),id,model::canonical(&result)?,now])?;
    tx.commit()?;
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
