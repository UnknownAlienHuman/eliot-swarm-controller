//! Anchored submission and review transitions. No native input or file I/O in transactions.
use super::{current_principal, operations, results, tasks};
use crate::{
    artifacts::{ArtifactFiles, ArtifactRecord},
    automation::disposition::ReviewDispositionContext,
    error::{Error, Result},
    model::{self, Principal, Role, TaskSpec},
    submission::{ChangeRequest, SubmitRequest, claim_counts},
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use swarm_kernel::reviews as review_contract;

fn current(db: &Connection, p: &Principal, input: &SubmitRequest) -> Result<Value> {
    let a = tasks::get_attempt(db, &input.attempt_id)?;
    if p.role == Role::Participant {
        super::coordination::authorize_task_submission(
            db,
            p,
            model::text(&a, "task_id")?,
            input.expected_revision,
            &input.attempt_id,
        )?;
    } else if matches!(p.role, Role::Operator | Role::Manager) {
        super::gm::require_attempt_control(db, p, &a)?;
    } else {
        return Err(Error::new(
            "FORBIDDEN",
            "submission requires an assigned Participant, Attempt owner, current GM, or operator",
        ));
    }
    let t = tasks::get_task(db, model::text(&a, "task_id")?)?;
    if t["state"] != "open"
        || t["revision"] != input.expected_revision
        || a["task_revision"] != input.expected_revision
    {
        return Err(Error::new(
            "STALE_REVISION",
            "submission does not target the current open Task revision",
        ));
    }
    if !a["released_at_ms"].is_null()
        || !matches!(
            a["state"].as_str(),
            Some("reserved" | "running" | "submitted" | "needs_correction" | "recovery_pending")
        )
    {
        return Err(Error::conflict(
            "Attempt cannot submit in its current state",
        ));
    }
    if a["submission_ref"] != json!(input.expected_submission_ref) {
        return Err(Error::new(
            "STALE_SUBMISSION",
            "current submission differs from expected_submission_ref",
        ));
    }
    Ok(a)
}
fn candidate(db: &Connection, a: &Value, input: &SubmitRequest) -> Result<ArtifactRecord> {
    candidate_for_attempt(
        db,
        a,
        &input.attempt_id,
        input.expected_revision,
        &input.candidate_ref,
    )
}

fn candidate_for_attempt(
    db: &Connection,
    a: &Value,
    attempt_id: &str,
    expected_revision: i64,
    candidate_ref: &str,
) -> Result<ArtifactRecord> {
    let record = results::get(db, candidate_ref)?;
    if record.kind == "source_snapshot" {
        if record.metadata["task_id"] != a["task_id"]
            || record.metadata["attempt_id"] != attempt_id
            || record.metadata["task_revision"] != expected_revision
        {
            return Err(Error::new(
                "CANDIDATE_SCOPE",
                "source snapshot belongs to another Attempt/revision",
            ));
        }
        return Ok(record);
    }
    if super::normalized_result::validate_candidate_origin(db, a, &record)? {
        return Ok(record);
    }
    let identity = match record.kind.as_str() {
        "native_result" if record.metadata["coverage"] == "complete" => {
            &record.metadata["identity"]
        }
        "native_result_page"
            if record.metadata["offset_bytes"] == 0
                && record.metadata["total_bytes"].as_u64() == Some(record.byte_length)
                && record.metadata["eof"] == true =>
        {
            &record.metadata
        }
        _ => {
            return Err(Error::new(
                "CANDIDATE_INCOMPLETE",
                "candidate must be a complete retained result, not one page of a larger body",
            ));
        }
    };
    validate_complete_command_output(&record)?;
    if command_output_snapshot(&record).is_some() {
        validate_command_output_attempt(db, a, attempt_id, &record)?;
    }
    let candidate_generation = if record.kind == "native_result_page" {
        identity["binding_generation"].clone()
    } else {
        identity["generation"].clone()
    };
    if !a["binding_id"].is_null()
        && (a["binding_id"] != identity["binding_id"]
            || a["binding_generation"] != candidate_generation)
    {
        return Err(Error::new(
            "CANDIDATE_SCOPE",
            "candidate belongs to another native binding",
        ));
    }
    // Selection is the caller's assertion. This does not attest a Git source tree,
    // assign a child by timestamp, or promote the native report to an independent check.
    Ok(record)
}

fn command_output_snapshot(record: &ArtifactRecord) -> Option<&Value> {
    match record.kind.as_str() {
        "native_result_page" if record.metadata["source"]["kind"] == "command_output" => {
            Some(&record.metadata["source"]["target_command_output"])
        }
        "native_result" if record.metadata["identity"]["source"]["kind"] == "command_output" => {
            Some(&record.metadata["identity"]["source"]["target_command_output"])
        }
        _ => None,
    }
}

fn validate_complete_command_output(record: &ArtifactRecord) -> Result<()> {
    let Some(snapshot) = command_output_snapshot(record) else {
        return Ok(());
    };
    if snapshot["method"] != "task.dispatch"
        || snapshot["operation_state"] != "settled"
        || snapshot["operation_outcome"] != "applied"
        || snapshot["native_response_identity"] != "unavailable"
        || snapshot["execution_complete"] != false
        || snapshot["task_completion"] != "unknown"
        || snapshot["native_replay"] != false
        || snapshot["truncated"] != false
        || snapshot["read_error"] != false
        || snapshot["stream_bytes"] != snapshot["stored_bytes"]
        || snapshot["stream_sha256"] != snapshot["stored_sha256"]
    {
        return Err(Error::new(
            "CANDIDATE_INCOMPLETE",
            "Command output is a Task candidate only when the retained stream is complete",
        ));
    }
    Ok(())
}

fn validate_command_output_attempt(
    db: &Connection,
    attempt: &Value,
    attempt_id: &str,
    candidate: &ArtifactRecord,
) -> Result<()> {
    let page_ids: Vec<String> = if candidate.kind == "native_result_page" {
        vec![candidate.artifact_id.clone()]
    } else {
        candidate.metadata["parts"]
            .as_array()
            .filter(|parts| !parts.is_empty())
            .ok_or_else(|| {
                Error::new(
                    "CANDIDATE_SCOPE",
                    "assembled Command output has no retained result pages",
                )
            })?
            .iter()
            .map(|part| model::text(part, "artifact_ref").map(str::to_owned))
            .collect::<Result<Vec<_>>>()?
    };
    for page_id in page_ids {
        let page = results::get(db, &page_id)?;
        let source = &page.metadata["source"];
        let dispatch_id = model::text(source, "input_operation_id")?;
        let result_operation_id = model::text(source, "result_operation_id")?;
        let dispatch = operations::get_operation(db, dispatch_id)?;
        let result_operation = operations::get_operation(db, result_operation_id)?;
        if page.kind != "native_result_page"
            || source["kind"] != "command_output"
            || source["target_command_output"]["operation_id"] != dispatch_id
            || page.metadata["operation_id"] != result_operation_id
            || dispatch["method"] != "task.dispatch"
            || dispatch["state"] != "settled"
            || dispatch["result"]["outcome"] != "applied"
            || dispatch["task_id"] != attempt["task_id"]
            || dispatch["attempt_id"] != attempt_id
            || dispatch["binding_id"] != attempt["binding_id"]
            || dispatch["binding_generation"] != attempt["binding_generation"]
            || result_operation["method"] != "agent.result"
            || result_operation["state"] != "settled"
            || result_operation["task_id"] != attempt["task_id"]
            || result_operation["attempt_id"] != attempt_id
            || result_operation["binding_id"] != attempt["binding_id"]
            || result_operation["binding_generation"] != attempt["binding_generation"]
            || result_operation["result"]["outcome"] != "applied"
            || result_operation["result"]["details"]["artifact_ref"] != page_id
            || result_operation["result"]["details"]["source"] != *source
        {
            return Err(Error::new(
                "CANDIDATE_SCOPE",
                "Command output is not the exact result retained for this Task Attempt",
            ));
        }
    }
    Ok(())
}

/// Participant candidates must be tied to this exact Attempt by the source
/// snapshot metadata or by the retained agent.result Operation that produced
/// every page. Binding equality alone is insufficient because one native
/// binding may have served more than one Task/Attempt.
fn authorize_participant_candidate(
    db: &Connection,
    attempt: &Value,
    candidate: &ArtifactRecord,
) -> Result<()> {
    if candidate.kind == "source_snapshot" {
        if candidate.metadata["task_id"] == attempt["task_id"]
            && candidate.metadata["attempt_id"] == attempt["attempt_id"]
            && candidate.metadata["task_revision"] == attempt["task_revision"]
        {
            return Ok(());
        }
        return Err(Error::new(
            "CANDIDATE_SCOPE",
            "source snapshot belongs to another Task or Attempt",
        ));
    }
    if super::normalized_result::validate_candidate_origin(db, attempt, candidate)? {
        return Ok(());
    }
    let claude_candidate = match candidate.kind.as_str() {
        "native_result_page" => candidate.metadata["source"]["kind"] == "claude_assistant_result",
        "native_result" => {
            candidate.metadata["identity"]["source"]["kind"] == "claude_assistant_result"
        }
        _ => false,
    };
    if !claude_candidate
        && (attempt["binding_id"].is_null() || attempt["binding_generation"].is_null())
    {
        return Err(Error::new(
            "CANDIDATE_SCOPE",
            "native result candidate is not linked to this bound Attempt",
        ));
    }
    let page_ids: Vec<String> = match candidate.kind.as_str() {
        "native_result_page" => vec![candidate.artifact_id.clone()],
        "native_result" => candidate.metadata["parts"]
            .as_array()
            .filter(|parts| !parts.is_empty())
            .ok_or_else(|| {
                Error::new(
                    "CANDIDATE_SCOPE",
                    "assembled candidate has no retained result pages",
                )
            })?
            .iter()
            .map(|part| model::text(part, "artifact_ref").map(str::to_owned))
            .collect::<Result<Vec<_>>>()?,
        _ => {
            return Err(Error::new(
                "CANDIDATE_SCOPE",
                "candidate is not an assigned source or native result",
            ));
        }
    };
    let mut seen = std::collections::BTreeSet::new();
    for page_id in page_ids {
        if !seen.insert(page_id.clone()) {
            return Err(Error::new(
                "CANDIDATE_SCOPE",
                "candidate repeats a retained result page",
            ));
        }
        let page = results::get(db, &page_id)?;
        if page.kind != "native_result_page" {
            return Err(Error::new(
                "CANDIDATE_SCOPE",
                "assembled candidate references a non-result page",
            ));
        }
        validate_complete_command_output(&page)?;
        let command_output = page.metadata["source"]["kind"] == "command_output";
        let claude_result = page.metadata["source"]["kind"] == "claude_assistant_result";
        let result_operation_id = page.metadata["operation_id"].as_str().ok_or_else(|| {
            Error::new(
                "CANDIDATE_SCOPE",
                "result page has no retained result operation",
            )
        })?;
        let operation_id = if command_output || claude_result {
            page.metadata["source"]["input_operation_id"].as_str()
        } else {
            Some(result_operation_id)
        }
        .ok_or_else(|| {
            Error::new(
                "CANDIDATE_SCOPE",
                "result page has no retained dispatch operation",
            )
        })?;
        let dispatch = operations::get_operation(db, operation_id)?;
        let native_output = if command_output {
            page.metadata["selector"]["native_output"].as_str()
        } else {
            page.metadata["native_output"].as_str()
        };
        if claude_result {
            authorize_claude_result_candidate(db, attempt, &page, &dispatch)?;
            continue;
        }
        if dispatch["method"] != "task.dispatch"
            || (command_output
                && (dispatch["state"] != "settled" || dispatch["result"]["outcome"] != "applied"))
            || (!command_output
                && !matches!(dispatch["state"].as_str(), Some("settled" | "rejected")))
            || dispatch["task_id"] != attempt["task_id"]
            || dispatch["attempt_id"] != attempt["attempt_id"]
            || dispatch["binding_id"] != attempt["binding_id"]
            || dispatch["binding_generation"] != attempt["binding_generation"]
            || page.metadata["binding_id"] != attempt["binding_id"]
            || page.metadata["binding_generation"] != attempt["binding_generation"]
            || (command_output
                && (page.metadata["source"]["result_operation_id"]
                    != page.metadata["operation_id"]
                    || page.metadata["source"]["target_command_output"]["operation_id"]
                        != operation_id
                    || !matches!(native_output, Some("stdout.ndjson" | "stderr.txt"))))
            || (!command_output
                && !claude_result
                && !crate::runtime::batch::BATCH_OUTPUTS.contains(&native_output.unwrap_or("")))
        {
            return Err(Error::new(
                "CANDIDATE_SCOPE",
                "result page is not linked to this exact Task, Attempt, and binding generation",
            ));
        }
        let linked = if command_output {
            let result_operation_id = model::text(&page.metadata, "operation_id")?;
            let result_operation = operations::get_operation(db, result_operation_id)?;
            result_operation["method"] == "agent.result"
                && result_operation["state"] == "settled"
                && result_operation["task_id"] == attempt["task_id"]
                && result_operation["attempt_id"] == attempt["attempt_id"]
                && result_operation["binding_id"] == attempt["binding_id"]
                && result_operation["binding_generation"] == attempt["binding_generation"]
                && result_operation["result"]["outcome"] == "applied"
                && result_operation["result"]["details"]["artifact_ref"] == page_id
                && result_operation["result"]["details"]["source"]["kind"] == "command_output"
                && result_operation["result"]["details"]["source"]["input_operation_id"]
                    == operation_id
        } else {
            let mut linked = false;
            let mut operation_stmt = db.prepare(
                "SELECT result_json FROM operations WHERE method='agent.result' AND state='settled' AND task_id=?1 AND attempt_id=?2 AND binding_id=?3 AND binding_generation=?4",
            )?;
            let operation_rows = operation_stmt.query_map(
                rusqlite::params![
                    attempt["task_id"].as_str(),
                    attempt["attempt_id"].as_str(),
                    attempt["binding_id"].as_str(),
                    attempt["binding_generation"].as_i64(),
                ],
                |row| row.get::<_, String>(0),
            )?;
            for raw in operation_rows {
                let result: Value = serde_json::from_str(&raw?)?;
                if result["outcome"] == "applied"
                    && result["details"]["dispatch_operation_id"] == operation_id
                    && result["details"]["artifact_refs"]
                        .as_array()
                        .is_some_and(|refs| {
                            refs.iter()
                                .any(|item| item.as_str() == Some(page_id.as_str()))
                        })
                {
                    linked = true;
                    break;
                }
            }
            linked
        };
        if !linked {
            return Err(Error::new(
                "CANDIDATE_SCOPE",
                "result page is not linked to an applied agent.result Operation",
            ));
        }
    }
    Ok(())
}

fn authorize_claude_result_candidate(
    db: &Connection,
    expected_attempt: &Value,
    page: &ArtifactRecord,
    dispatch: &Value,
) -> Result<()> {
    let source = &page.metadata["source"];
    let result_operation_id = model::text(&page.metadata, "operation_id")?;
    let dispatch_operation_id = model::text(source, "input_operation_id")?;
    let page_id = page.artifact_id.as_str();
    model::fields(
        source,
        &[
            "kind",
            "result_operation_id",
            "result_input_sha256",
            "result_module_receipt",
            "input_operation_id",
            "target_method",
            "target_input_sha256",
            "target_module_receipt",
            "native_session_id",
            "native_input_id",
            "native_payload_sha256",
            "native_payload_bytes",
            "result_frame_uuid",
            "result_subtype",
            "result_status",
            "result_sha256",
            "result_bytes",
            "content_digest",
            "native_output",
            "evidence",
            "native_response_identity",
            "execution_complete",
            "task_completion",
            "native_replay",
        ],
    )?;
    let result_operation = operations::get_operation(db, result_operation_id)?;
    let binding_id = model::text(&result_operation, "binding_id")?;
    let binding_generation = result_operation["binding_generation"]
        .as_i64()
        .ok_or_else(|| Error::new("CANDIDATE_SCOPE", "Claude result has no binding generation"))?;
    let origin = results::load_claude_result_origin(
        db,
        result_operation_id,
        &result_operation,
        binding_id,
        binding_generation,
        dispatch_operation_id,
        dispatch,
    )?;
    let expected_task_id = model::text(expected_attempt, "task_id")?;
    let expected_attempt_id = model::text(expected_attempt, "attempt_id")?;
    let expected_task_revision = model::positive(expected_attempt, "task_revision")?;
    if dispatch["task_id"].as_str() != Some(expected_task_id)
        || dispatch["attempt_id"].as_str() != Some(expected_attempt_id)
        || origin["target_task_id"].as_str() != Some(expected_task_id)
        || origin["target_attempt_id"].as_str() != Some(expected_attempt_id)
        || origin["target_task_revision"].as_i64() != Some(expected_task_revision)
    {
        return Err(Error::new(
            "CANDIDATE_SCOPE",
            "Claude result candidate belongs to another Task Attempt or revision",
        ));
    }
    let context = json!({
        "operation_id":result_operation_id,
        "result_input_sha256":origin["result_input_sha256"],
        "result_module_receipt":source["result_module_receipt"],
        "target_operation_id":origin["target_operation_id"],
        "target_method":origin["target_method"],
        "target_input_sha256":origin["target_input_sha256"],
        "target_module_receipt":origin["target_module_receipt"],
        "native_session_id":origin["native_session_id"],
        "native_input_id":origin["native_input_id"],
        "native_payload_sha256":origin["native_payload_sha256"],
        "native_payload_bytes":origin["native_payload_bytes"]
    });
    let request = json!({"selector":origin["result_selector"]});
    results::validate_sealed_module_receipt(
        &origin["descriptor"],
        &source["result_module_receipt"],
        result_operation_id,
        binding_id,
        binding_generation,
        origin["result_input_sha256"].as_str().unwrap_or_default(),
    )?;
    results::validate_claude_assistant_result_source(
        &request,
        source,
        &context,
        &origin["producer"],
    )?;
    if result_operation["state"] != "settled"
        || result_operation["result"]["outcome"] != "applied"
        || result_operation["result"]["details"]["artifact_ref"] != page_id
        || result_operation["result"]["details"]["source"] != source.clone()
        || page.metadata["operation_id"] != result_operation_id
        || page.metadata["binding_id"] != binding_id
        || page.metadata["binding_generation"].as_i64() != Some(binding_generation)
        || page.metadata["native_root_id"] != origin["native_session_id"]
        || page.metadata["native_scope_key"] != origin["native_scope_key"]
        || page.metadata["total_bytes"] != source["result_bytes"]
        || page.metadata["source"]["content_digest"] != source["content_digest"]
        || (page.metadata["total_bytes"] == json!(page.byte_length)
            && page.content_digest != source["result_sha256"].as_str().unwrap_or(""))
        || source["result_operation_id"] != result_operation_id
        || source["input_operation_id"] != dispatch_operation_id
    {
        return Err(Error::new(
            "CANDIDATE_SCOPE",
            "Claude result candidate is not the exact retained SDK frame",
        ));
    }
    Ok(())
}

/// The normal artifact reader uses this same exact-assignment rule, so an
/// ordinary Participant can inspect only its current candidate/work result.
pub(crate) fn authorize_participant_artifact_read(
    db: &Connection,
    principal: &Principal,
    artifact_id: &str,
) -> Result<()> {
    principal.require_participant()?;
    let scope = super::coordination::current_scope(db, principal)?;
    if !matches!(
        scope["participant"]["participation_basis"]["kind"].as_str(),
        Some("attempt_owner" | "producer_ref")
    ) {
        return Err(Error::new(
            "FORBIDDEN",
            "review-only Participant grants cannot read Task work artifacts",
        ));
    }
    let attempt_id = model::text(&scope["attempt"], "attempt_id")?;
    let attempt = tasks::get_attempt(db, attempt_id)?;
    if scope["attempt"]["submission_ref"] == artifact_id {
        let submission = results::get(db, artifact_id)?;
        let operation_id = model::text(&submission.metadata, "operation_id").ok();
        let operation = operation_id
            .map(|id| operations::get_operation(db, id))
            .transpose()?;
        if submission.kind == "task_submission"
            && submission.metadata["task_id"] == attempt["task_id"]
            && submission.metadata["attempt_id"] == attempt_id
            && submission.metadata["task_revision"] == attempt["task_revision"]
            && operation.as_ref().is_some_and(|operation| {
                operation["method"] == "task.submit"
                    && operation["state"] == "settled"
                    && operation["result"]["outcome"] == "applied"
                    && operation["task_id"] == attempt["task_id"]
                    && operation["attempt_id"] == attempt_id
                    && operation["result"]["submission_ref"] == artifact_id
            })
        {
            return Ok(());
        }
    }
    let candidate = candidate_for_attempt(
        db,
        &attempt,
        attempt_id,
        model::positive(&scope["task"], "revision")?,
        artifact_id,
    )?;
    authorize_participant_candidate(db, &attempt, &candidate)
}

struct RetainedSubmission {
    input: SubmitRequest,
    attempt: Value,
    task: Value,
    artifact: ArtifactRecord,
}

struct ValidatedSubmission {
    artifact: ArtifactRecord,
    still_current: bool,
}

/// The recovery actor must be the currently designated GM or local Operator.
/// Unlike submission admission, historical recovery deliberately does not
/// require control of the current Attempt: a released or superseded Attempt's
/// already-published artifact still needs an exact readback decision.
fn require_recovery_authority(db: &Connection, p: &Principal, task_id: &str) -> Result<()> {
    let p = current_principal(db, p.clone())?;
    if !matches!(p.role, Role::Operator | Role::Manager) {
        return Err(Error::new(
            "FORBIDDEN",
            "submission recovery requires the current GM or operator",
        ));
    }
    super::gm::require_authority(db, &p)?;
    if p.role == Role::Manager {
        let task = tasks::get_task(db, task_id)?;
        let project_id = model::text(&task, "project_id")?;
        if !crate::automation::authorization::current_manager_has_task_scope(
            db, &p, task_id, project_id,
        )? {
            return Err(Error::new(
                "FORBIDDEN",
                "current GM lacks scope for this Task and project",
            ));
        }
    }
    Ok(())
}

/// Rebuild the immutable expected document from the original request and the
/// retained Attempt/candidate. This keeps recovery anchored to the original
/// submitter and never lets the recovering GM replace any admission fact.
fn retained_submission(db: &Connection, id: &str, op: &Value) -> Result<RetainedSubmission> {
    if op["operation_id"] != id || op["method"] != "task.submit" {
        return Err(Error::new(
            "SUBMISSION_RECOVERY_TARGET",
            "recovery target is not the exact task.submit Operation",
        ));
    }
    let (original_raw, effective_raw, client_request_id): (String, String, String) = db.query_row(
        "SELECT original_request_json,effective_request_json,client_request_id FROM operations WHERE operation_id=?1",
        [id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let original: Value = serde_json::from_str(&original_raw)?;
    let input = SubmitRequest::parse(&original)?;
    let attempt = tasks::get_attempt(db, &input.attempt_id)?;
    let task = tasks::get_task(db, model::text(&attempt, "task_id")?)?;
    let candidate = candidate(db, &attempt, &input)?;
    let spec: TaskSpec = serde_json::from_value(attempt["task_snapshot"]["spec"].clone())?;
    let claims = input.normalized_claims(&spec)?;
    let counts = claim_counts(&claims);
    let expected_document = json!({
        "schema_version":1,
        "operation_id":id,
        "task_id":attempt["task_id"],
        "attempt_id":input.attempt_id,
        "task_revision":input.expected_revision,
        "phase":spec.phase,
        "owner_id":attempt["owner_id"],
        "submitted_by":op["caller_id"],
        "previous_submission_ref":input.expected_submission_ref,
        "candidate_ref":candidate.artifact_id,
        "candidate_sha256":candidate.content_digest,
        "candidate_kind":candidate.kind,
        "candidate_byte_length":candidate.byte_length,
        "summary":input.summary,
        "claims":claims,
        "claim_counts":counts,
        "evidence_level":"submitter_report",
        "source_checkout_verified":false
    });
    let effective: Value = serde_json::from_str(&effective_raw)?;
    let document = effective["submission_document"].clone();
    if op["task_id"] != expected_document["task_id"]
        || op["attempt_id"] != expected_document["attempt_id"]
        || original["client_request_id"] != client_request_id
        || expected_document["submitted_by"] != op["caller_id"]
        || model::canonical(&document)? != model::canonical(&expected_document)?
    {
        return Err(Error::new(
            "SUBMISSION_RECOVERY_TARGET",
            "retained submission differs from its original request, Attempt, or candidate",
        ));
    }
    let (artifact, _) = ArtifactFiles::submission(id, &document)?;
    Ok(RetainedSubmission {
        input,
        attempt,
        task,
        artifact,
    })
}

fn still_current(retained: &RetainedSubmission) -> bool {
    let input = &retained.input;
    let attempt = &retained.attempt;
    let task = &retained.task;
    task["state"] == "open"
        && task["revision"] == input.expected_revision
        && task["current_attempt_id"] == input.attempt_id
        && attempt["task_revision"] == input.expected_revision
        && attempt["released_at_ms"].is_null()
        && attempt["submission_ref"] == json!(input.expected_submission_ref)
        && matches!(
            attempt["state"].as_str(),
            Some("reserved" | "running" | "submitted" | "needs_correction" | "recovery_pending")
        )
}

fn validate_published_submission(
    db: &Connection,
    id: &str,
    op: &Value,
    published: ArtifactRecord,
) -> Result<ValidatedSubmission> {
    let retained = retained_submission(db, id, op)?;
    let expected = &retained.artifact;
    if published.kind != expected.kind
        || published.artifact_id != expected.artifact_id
        || published.relative_path != expected.relative_path
        || published.byte_length != expected.byte_length
        || published.content_digest != expected.content_digest
        || published.metadata != expected.metadata
    {
        return Err(Error::conflict(
            "published submission differs from its retained operation, Attempt, or candidate",
        ));
    }
    Ok(ValidatedSubmission {
        artifact: published,
        still_current: still_current(&retained),
    })
}

pub(super) fn reserve(tx: &Transaction<'_>, p: &Principal, v: &Value, id: &str) -> Result<Value> {
    let input = SubmitRequest::parse(v)?;
    let a = current(tx, p, &input)?;
    let candidate = candidate(tx, &a, &input)?;
    if p.role == Role::Participant {
        authorize_participant_candidate(tx, &a, &candidate)?;
    }
    let spec: TaskSpec = serde_json::from_value(a["task_snapshot"]["spec"].clone())?;
    let claims = input.normalized_claims(&spec)?;
    let counts = claim_counts(&claims);
    let document = json!({"schema_version":1,"operation_id":id,"task_id":a["task_id"],
        "attempt_id":input.attempt_id,"task_revision":input.expected_revision,"phase":spec.phase,
        "owner_id":a["owner_id"],"submitted_by":p.client_id,"previous_submission_ref":input.expected_submission_ref,
        "candidate_ref":candidate.artifact_id,"candidate_sha256":candidate.content_digest,
        "candidate_kind":candidate.kind,"candidate_byte_length":candidate.byte_length,
        "summary":input.summary,"claims":claims,"claim_counts":counts,
        "evidence_level":"submitter_report","source_checkout_verified":false});
    tx.execute("UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=?4 WHERE operation_id=?1",
        params![id,a["task_id"].as_str(),input.attempt_id,model::canonical(&json!({"submission_document":document}))?])?;
    Ok(
        json!({"operation_id":id,"attempt_id":input.attempt_id,"state":"queued",
        "admission":"durable_local","task_accepted":false}),
    )
}

pub(super) fn reserve_recovery(
    tx: &Transaction<'_>,
    p: &Principal,
    v: &Value,
    recovery_id: &str,
    now: i64,
) -> Result<(Value, bool)> {
    let p = current_principal(tx, p.clone())?;
    let target_id = model::text(v, "operation_id")?;
    let target = operations::get_operation(tx, target_id)?;
    if target["method"] != "task.submit"
        || !matches!(
            target["state"].as_str(),
            Some("outcome_unknown" | "settled")
        )
    {
        return Err(Error::new(
            "SUBMISSION_NOT_RECOVERABLE",
            "recovery requires the exact task.submit Operation in outcome_unknown or settled",
        ));
    }
    require_recovery_authority(tx, &p, model::text(&target, "task_id")?)?;
    // Validate the complete immutable target now; begin/finalize repeat this
    // against the same Operation so no client-supplied identity is trusted.
    retained_submission(tx, target_id, &target)?;
    if target["state"] == "settled" {
        settled_target_result(target_id, &target)?;
    }
    let linkage = json!({
        "target_operation_id":target_id,
        "original_caller_id":target["caller_id"],
        "task_id":target["task_id"],
        "attempt_id":target["attempt_id"]
    });
    let changed = tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=?4,updated_at_ms=?5 \
         WHERE operation_id=?1 AND method='task.submit.recover' AND caller_id=?6 AND state='queued'",
        params![
            recovery_id,
            target["task_id"].as_str(),
            target["attempt_id"].as_str(),
            model::canonical(&linkage)?,
            now,
            p.client_id
        ],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "submission recovery Operation is no longer queued",
        ));
    }
    Ok((
        json!({
            "operation_id":recovery_id,
            "target_operation_id":target_id,
            "state":"queued",
            "outcome":"recovery_pending",
            "artifact_readback":"required"
        }),
        true,
    ))
}

pub(super) enum SubmissionRecoveryStart {
    Verify {
        target_operation_id: String,
        expected_artifact: ArtifactRecord,
    },
    Complete(Value),
}

fn settle_recovery(tx: &Transaction<'_>, recovery_id: &str, value: &Value, now: i64) -> Result<()> {
    let changed = tx.execute(
        "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 \
         WHERE operation_id=?1 AND method='task.submit.recover' AND state='queued'",
        params![recovery_id, model::canonical(value)?, now],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "submission recovery Operation is no longer queued",
        ));
    }
    super::capacity::sync_operation(tx, recovery_id, now)?;
    tx.execute(
        "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
         VALUES('controller',?1,?2,'task.submit.recover',?3,?4)",
        params![
            format!("submission-recovery:{recovery_id}"),
            recovery_id,
            model::canonical(value)?,
            now
        ],
    )?;
    Ok(())
}

fn recovery_linkage(db: &Connection, recovery_id: &str, op: &Value) -> Result<(String, Value)> {
    if op["operation_id"] != recovery_id || op["method"] != "task.submit.recover" {
        return Err(Error::new(
            "SUBMISSION_RECOVERY_OPERATION",
            "Operation is not the exact task.submit.recover request",
        ));
    }
    let (original_raw, effective_raw, client_request_id): (String, String, String) = db.query_row(
        "SELECT original_request_json,effective_request_json,client_request_id FROM operations WHERE operation_id=?1",
        [recovery_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let original: Value = serde_json::from_str(&original_raw)?;
    let effective: Value = serde_json::from_str(&effective_raw)?;
    let target_id = model::text(&original, "operation_id")?.to_owned();
    if effective["target_operation_id"] != target_id
        || op["task_id"] != effective["task_id"]
        || op["attempt_id"] != effective["attempt_id"]
        || original["client_request_id"] != client_request_id
        || effective["receipt"]["ok"] != true
        || effective["receipt"]["value"]["operation_id"] != recovery_id
        || effective["receipt"]["value"]["target_operation_id"] != target_id
    {
        return Err(Error::new(
            "SUBMISSION_RECOVERY_OPERATION",
            "recovery request differs from its retained target linkage",
        ));
    }
    Ok((target_id, effective))
}

fn settled_target_result(target_id: &str, target: &Value) -> Result<Value> {
    let result = &target["result"];
    if target["method"] != "task.submit"
        || target["operation_id"] != target_id
        || target["state"] != "settled"
        || result["operation_id"] != target_id
        || !matches!(
            result["outcome"].as_str(),
            Some("applied" | "stale_submission_scope" | "failed")
        )
    {
        return Err(Error::new(
            "SUBMISSION_RECOVERY_TARGET",
            "settled target does not have a valid retained submission result",
        ));
    }
    Ok(result.clone())
}

pub(super) fn begin_recovery(
    db: &mut Connection,
    p: Principal,
    recovery_id: &str,
) -> Result<SubmissionRecoveryStart> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let p = current_principal(&tx, p)?;
    let recovery = operations::get_operation(&tx, recovery_id)?;
    if recovery["caller_id"] != p.client_id || recovery["method"] != "task.submit.recover" {
        return Err(Error::new(
            "FORBIDDEN",
            "recovery Operation belongs to another caller or method",
        ));
    }
    if recovery["state"] == "settled" {
        let value = recovery["result"].clone();
        tx.commit()?;
        return Ok(SubmissionRecoveryStart::Complete(value));
    }
    if recovery["state"] != "queued" {
        return Err(Error::conflict(
            "submission recovery Operation is not queued",
        ));
    }
    let (target_id, linkage) = recovery_linkage(&tx, recovery_id, &recovery)?;
    let target = operations::get_operation(&tx, &target_id)?;
    if target["method"] != "task.submit"
        || target["task_id"] != linkage["task_id"]
        || target["attempt_id"] != linkage["attempt_id"]
        || target["caller_id"] != linkage["original_caller_id"]
    {
        return Err(Error::new(
            "SUBMISSION_RECOVERY_TARGET",
            "target Operation no longer matches the retained recovery linkage",
        ));
    }
    require_recovery_authority(&tx, &p, model::text(&target, "task_id")?)?;
    let retained = retained_submission(&tx, &target_id, &target)?;
    if target["state"] == "settled" {
        let target_result = settled_target_result(&target_id, &target)?;
        let value = json!({
            "operation_id":recovery_id,
            "target_operation_id":target_id,
            "outcome":"already_settled",
            "target_result":target_result
        });
        settle_recovery(&tx, recovery_id, &value, model::now_ms()?)?;
        tx.commit()?;
        return Ok(SubmissionRecoveryStart::Complete(value));
    }
    if target["state"] != "outcome_unknown" {
        return Err(Error::new(
            "SUBMISSION_NOT_RECOVERABLE",
            "target is no longer an unknown submission",
        ));
    }
    let expected_artifact = retained.artifact;
    tx.commit()?;
    Ok(SubmissionRecoveryStart::Verify {
        target_operation_id: target_id,
        expected_artifact,
    })
}

fn insert_artifact_once(tx: &Transaction<'_>, artifact: &ArtifactRecord, now: i64) -> Result<()> {
    let existing: Option<(String, String, i64, String, String)> = tx
        .query_row(
            "SELECT kind,relative_path,byte_length,content_digest,metadata_json FROM artifacts WHERE artifact_id=?1",
            [&artifact.artifact_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .optional()?;
    if let Some((kind, path, byte_length, digest, metadata_raw)) = existing {
        let metadata: Value = serde_json::from_str(&metadata_raw)?;
        if kind != artifact.kind
            || path != artifact.relative_path
            || u64::try_from(byte_length).ok() != Some(artifact.byte_length)
            || digest != artifact.content_digest
            || metadata != artifact.metadata
        {
            return Err(Error::new(
                "ARTIFACT_COLLISION",
                "submission artifact ID is already registered with different content",
            ));
        }
        return Ok(());
    }
    tx.execute(
        "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) \
         VALUES(?1,?2,'task_submission',?3,?4,?5,?6)",
        params![
            artifact.artifact_id,
            artifact.relative_path,
            i64::try_from(artifact.byte_length)
                .map_err(|_| Error::invalid("submission too large"))?,
            artifact.content_digest,
            now,
            model::canonical(&artifact.metadata)?
        ],
    )?;
    Ok(())
}

pub(super) fn finish_recovery(
    db: &mut Connection,
    p: Principal,
    recovery_id: &str,
    expected_target_id: &str,
    verified: bool,
) -> Result<Value> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let p = current_principal(&tx, p)?;
    let recovery = operations::get_operation(&tx, recovery_id)?;
    if recovery["caller_id"] != p.client_id || recovery["method"] != "task.submit.recover" {
        return Err(Error::new(
            "FORBIDDEN",
            "recovery Operation belongs to another caller or method",
        ));
    }
    if recovery["state"] == "settled" {
        let result = recovery["result"].clone();
        tx.commit()?;
        return Ok(result);
    }
    if recovery["state"] != "queued" {
        return Err(Error::conflict(
            "submission recovery Operation is not queued",
        ));
    }
    let (target_id, linkage) = recovery_linkage(&tx, recovery_id, &recovery)?;
    if target_id != expected_target_id {
        return Err(Error::new(
            "SUBMISSION_RECOVERY_TARGET",
            "verified artifact target differs from the retained recovery request",
        ));
    }
    let target = operations::get_operation(&tx, &target_id)?;
    if target["method"] != "task.submit"
        || target["task_id"] != linkage["task_id"]
        || target["attempt_id"] != linkage["attempt_id"]
        || target["caller_id"] != linkage["original_caller_id"]
    {
        return Err(Error::new(
            "SUBMISSION_RECOVERY_TARGET",
            "target Operation no longer matches the retained recovery linkage",
        ));
    }
    require_recovery_authority(&tx, &p, model::text(&target, "task_id")?)?;
    let retained = retained_submission(&tx, &target_id, &target)?;
    let now = model::now_ms()?;
    if target["state"] == "settled" {
        let target_result = settled_target_result(&target_id, &target)?;
        let value = json!({
            "operation_id":recovery_id,
            "target_operation_id":target_id,
            "outcome":"already_settled",
            "target_result":target_result
        });
        settle_recovery(&tx, recovery_id, &value, now)?;
        tx.commit()?;
        return Ok(value);
    }
    if target["state"] != "outcome_unknown" {
        return Err(Error::new(
            "SUBMISSION_NOT_RECOVERABLE",
            "target is no longer an unknown submission",
        ));
    }
    if !verified {
        let value = json!({
            "operation_id":recovery_id,
            "target_operation_id":target_id,
            "outcome":"held_unknown",
            "reason":"submission_artifact_missing",
            "target_state":"outcome_unknown"
        });
        settle_recovery(&tx, recovery_id, &value, now)?;
        tx.commit()?;
        return Ok(value);
    }

    insert_artifact_once(&tx, &retained.artifact, now)?;
    let target_value = if still_current(&retained) {
        let changed = tx.execute(
            "UPDATE attempts SET state='submitted',submission_ref=?2,candidate_ref=?3,updated_at_ms=?4 \
             WHERE attempt_id=?1 AND released_at_ms IS NULL AND task_revision=?5 \
             AND submission_ref IS ?6 AND state IN ('reserved','running','submitted','needs_correction','recovery_pending')",
            params![
                retained.input.attempt_id,
                retained.artifact.artifact_id,
                retained.input.candidate_ref,
                now,
                retained.input.expected_revision,
                retained.input.expected_submission_ref
            ],
        )?;
        if changed != 1 {
            return Err(Error::conflict(
                "Attempt changed before the recovered submission was recorded",
            ));
        }
        json!({
            "operation_id":target_id,
            "outcome":"applied",
            "attempt_id":retained.input.attempt_id,
            "submission_ref":retained.artifact.artifact_id,
            "candidate_ref":retained.input.candidate_ref,
            "claim_counts":retained.artifact.metadata["claim_counts"],
            "state":"submitted",
            "task_accepted":false
        })
    } else {
        json!({
            "operation_id":target_id,
            "outcome":"stale_submission_scope",
            "attempt_id":retained.input.attempt_id,
            "submission_artifact_ref":retained.artifact.artifact_id,
            "candidate_ref":retained.input.candidate_ref,
            "task_accepted":false,
            "applied_to_attempt":false
        })
    };
    let target_changed = tx.execute(
        "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 \
         WHERE operation_id=?1 AND method='task.submit' AND state='outcome_unknown'",
        params![target_id, model::canonical(&target_value)?, now],
    )?;
    if target_changed != 1 {
        return Err(Error::conflict(
            "submission target changed before recovery finalization",
        ));
    }
    tx.execute(
        "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
         VALUES('controller',?1,?2,'task.submission',?3,?4)",
        params![
            format!("submission:{target_id}"),
            target_id,
            model::canonical(&target_value)?,
            now
        ],
    )?;
    super::capacity::sync_attempt(&tx, &retained.input.attempt_id, now)?;
    super::capacity::sync_operation(&tx, &target_id, now)?;
    let value = json!({
        "operation_id":recovery_id,
        "target_operation_id":target_id,
        "outcome":"recovered",
        "target_outcome":target_value["outcome"],
        "submission_ref":retained.artifact.artifact_id
    });
    settle_recovery(&tx, recovery_id, &value, now)?;
    tx.commit()?;
    Ok(value)
}

pub(super) fn begin(
    db: &mut Connection,
    p: Principal,
    id: &str,
) -> Result<Option<(ArtifactRecord, Value)>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let p = current_principal(&tx, p)?;
    if !matches!(p.role, Role::Operator | Role::Manager | Role::Participant) {
        return Err(Error::new(
            "FORBIDDEN",
            "submission requires an assigned Participant, Attempt owner, current GM, or operator",
        ));
    }
    let op = operations::get_operation(&tx, id)?;
    if op["method"] != "task.submit" || op["caller_id"] != p.client_id {
        return Err(Error::new(
            "FORBIDDEN",
            "submission belongs to another caller or method",
        ));
    }
    if op["state"] != "queued" {
        return Ok(None);
    }
    let original_raw: String = tx.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [id],
        |r| r.get(0),
    )?;
    let original: Value = serde_json::from_str(&original_raw)?;
    let input = SubmitRequest::parse(&original)?;
    current(&tx, &p, &input)?;
    let raw: String = tx.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1",
        [id],
        |r| r.get(0),
    )?;
    let effective: Value = serde_json::from_str(&raw)?;
    let document = effective["submission_document"].clone();
    let record = results::get(&tx, model::text(&document, "candidate_ref")?)?;
    if p.role == Role::Participant {
        authorize_participant_candidate(
            &tx,
            &tasks::get_attempt(&tx, &input.attempt_id)?,
            &record,
        )?;
    }
    let now = model::now_ms()?;
    tx.execute("UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1", params![id,now])?;
    tx.commit()?;
    // Only a first, admitted queued submit reaches publication here. Unknown
    // outcomes use the explicit GM recovery readback path below; they never
    // replay publish from this begin helper.
    Ok(Some((record, document)))
}

pub(super) fn finish(
    db: &mut Connection,
    p: Principal,
    id: &str,
    outcome: Result<ArtifactRecord>,
) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let op = operations::get_operation(&tx, id)?;
    if op["method"] != "task.submit" || op["caller_id"] != p.client_id || op["state"] != "sending" {
        return Err(Error::conflict(
            "submission operation is no longer executing",
        ));
    }
    let raw: String = tx.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [id],
        |r| r.get(0),
    )?;
    let input = SubmitRequest::parse(&serde_json::from_str(&raw)?)?;
    let checked = outcome.and_then(|record| {
        // Validate only immutable retained provenance after the file boundary;
        // GM handover cannot orphan bytes already published by the submitter.
        validate_published_submission(&tx, id, &op, record)
            .map(|checked| (checked.artifact, checked.still_current))
    });
    let now = model::now_ms()?;
    let value = match checked {
        Ok((a, true)) => {
            tx.execute("INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,'task_submission',?3,?4,?5,?6)",
                params![a.artifact_id,a.relative_path,i64::try_from(a.byte_length).map_err(|_| Error::invalid("submission too large"))?,a.content_digest,now,model::canonical(&a.metadata)?])?;
            let changed = tx.execute("UPDATE attempts SET state='submitted',submission_ref=?2,candidate_ref=?3,updated_at_ms=?4 WHERE attempt_id=?1 AND released_at_ms IS NULL AND task_revision=?5",
                params![input.attempt_id,a.artifact_id,input.candidate_ref,now,input.expected_revision])?;
            if changed != 1 {
                return Err(Error::conflict(
                    "Attempt changed before the published submission was recorded",
                ));
            }
            json!({"operation_id":id,"outcome":"applied","attempt_id":input.attempt_id,
                "submission_ref":a.artifact_id,"candidate_ref":input.candidate_ref,
                "claim_counts":a.metadata["claim_counts"],"state":"submitted","task_accepted":false})
        }
        Ok((a, false)) => {
            tx.execute("INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,'task_submission',?3,?4,?5,?6)",
                params![a.artifact_id,a.relative_path,i64::try_from(a.byte_length).map_err(|_| Error::invalid("submission too large"))?,a.content_digest,now,model::canonical(&a.metadata)?])?;
            json!({"operation_id":id,"outcome":"stale_submission_scope","attempt_id":input.attempt_id,
                "submission_artifact_ref":a.artifact_id,"candidate_ref":input.candidate_ref,
                "task_accepted":false,"applied_to_attempt":false})
        }
        Err(e) => json!({"operation_id":id,"outcome":"failed","error":e,"task_accepted":false}),
    };
    let observation = submission_diagnostic_observation(&value);
    tx.execute("UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1", params![id,model::canonical(&value)?,now])?;
    super::capacity::sync_attempt(&tx, &input.attempt_id, now)?;
    tx.execute("INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller',?1,?2,'task.submission',?3,?4)",
        params![format!("submission:{id}"),id,model::canonical(&observation)?,now])?;
    tx.commit()?;
    Ok(())
}

fn submission_diagnostic_observation(value: &Value) -> Value {
    if value["outcome"] != "failed" {
        return value.clone();
    }
    let code = value["error"]["code"]
        .as_str()
        .filter(|code| safe_submission_error_code(code))
        .unwrap_or("SUBMISSION_FAILED");
    json!({
        "operation_id":value["operation_id"],
        "outcome":"failed",
        "error":{"code":code},
        "task_accepted":false,
    })
}

fn safe_submission_error_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes().first().is_some_and(u8::is_ascii_uppercase)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

pub(super) fn document(db: &Connection, submission_ref: &str) -> Result<Value> {
    let a = results::get(db, submission_ref)?;
    if a.kind != "task_submission" {
        return Err(Error::invalid("reference is not a Task submission"));
    }
    let id = model::text(&a.metadata, "operation_id")?;
    let op = operations::get_operation(db, id)?;
    if op["method"] != "task.submit"
        || op["state"] != "settled"
        || op["result"]["outcome"] != "applied"
        || op["result"]["submission_ref"] != submission_ref
    {
        return Err(Error::new(
            "SUBMISSION_DAMAGED",
            "submission has no committed result",
        ));
    }
    let raw: String = db.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1",
        [id],
        |r| r.get(0),
    )?;
    let value: Value = serde_json::from_str(&raw)?;
    let document = value["submission_document"].clone();
    if model::digest(model::canonical(&document)?.as_bytes()) != a.content_digest {
        return Err(Error::new(
            "SUBMISSION_DAMAGED",
            "saved document differs from its artifact identity",
        ));
    }
    Ok(document)
}

pub(super) fn describe(db: &Connection, v: &Value) -> Result<Value> {
    model::fields(v, &["submission_ref", "after", "limit"])?;
    let reference = model::text(v, "submission_ref")?;
    let mut doc = document(db, reference)?;
    let (limit, after) = super::page(v)?;
    let after = usize::try_from(after).map_err(|_| Error::invalid("claim offset too large"))?;
    let claims = doc["claims"]
        .as_array()
        .ok_or_else(|| Error::new("SUBMISSION_DAMAGED", "missing claims"))?;
    if after > claims.len() {
        return Err(Error::invalid("claim offset exceeds submission"));
    }
    let end = claims.len().min(after.saturating_add(limit as usize));
    let next = if end < claims.len() { Some(end) } else { None };
    doc["claims"] = json!(&claims[after..end]);
    let a = tasks::get_attempt(db, model::text(&doc, "attempt_id")?)?;
    let t = tasks::get_task(db, model::text(&doc, "task_id")?)?;
    doc["submission_ref"] = json!(reference);
    doc["latest_feedback_observation_id"] =
        json!(super::acceptance::feedback_cursor(db, reference)?);
    doc["current"] = json!(
        a["submission_ref"] == reference
            && t["revision"] == doc["task_revision"]
            && (t["current_attempt_id"] == a["attempt_id"]
                || (t["accepted_attempt_id"] == a["attempt_id"]
                    && !t["accepted_operation_id"].is_null()))
    );
    doc["next_after"] = json!(next);
    doc["content_availability"] = json!("use artifact.read/export to verify the backing bytes");
    Ok(doc)
}

pub(super) fn request_changes(
    tx: &Transaction<'_>,
    p: &Principal,
    v: &Value,
    id: &str,
    now: i64,
) -> Result<Value> {
    if p.role != Role::Manager {
        super::gm::require_authority(tx, p)?;
    }
    let input = ChangeRequest::parse(v)?;
    request_changes_core(tx, FeedbackActor::Direct(p), &input, id, now)
}

/// Apply the existing Task feedback transition for an authenticated manager's
/// selected automation. This is a typed internal seam; it never creates a
/// Principal or reaches the Store dispatcher recursively.
pub(super) fn request_changes_on_behalf(
    tx: &Transaction<'_>,
    context: &ReviewDispositionContext,
    input: &ChangeRequest,
    id: &str,
    now: i64,
) -> Result<Value> {
    let value = serde_json::to_value(input)?;
    let validated = ChangeRequest::parse(&value)?;
    let (caller_id, method, client_request_id, original_request, state): (
        String,
        String,
        String,
        String,
        String,
    ) = tx.query_row(
        "SELECT caller_id,method,client_request_id,original_request_json,state \
         FROM operations WHERE operation_id=?1",
        [id],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?;
    if caller_id != crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
        || method != "task.request_changes"
        || client_request_id != validated.client_request_id
        || model::canonical(&serde_json::from_str::<Value>(&original_request)?)?
            != model::canonical(&value)?
        || !matches!(state.as_str(), "queued" | "settled")
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "on-behalf feedback Operation does not match its typed request",
        ));
    }
    let link_key = crate::automation::config::operation_link_key(id)?;
    let link = crate::automation::config::read_record(tx, &link_key, "on-behalf operation link")?
        .ok_or_else(|| {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "on-behalf feedback Operation has no retained manager link",
        )
    })?;
    let mut expected_cause = context.cause_value();
    expected_cause
        .as_object_mut()
        .ok_or_else(|| {
            Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "typed disposition cause is not an object",
            )
        })?
        .remove("review_assignment_sponsor_id");
    let mut retained_cause = link["cause"].clone();
    let retained_cause_object = retained_cause.as_object_mut();
    let cause_sponsor_matches = match retained_cause_object
        .and_then(|cause| cause.remove("review_assignment_sponsor_id"))
    {
        None => true,
        Some(Value::String(sponsor)) => sponsor == context.review_assignment_sponsor_id(),
        Some(_) => false,
    };
    let top_level_sponsor_matches = match link.get("review_assignment_sponsor_id") {
        None => true,
        Some(Value::String(sponsor)) => sponsor == context.review_assignment_sponsor_id(),
        Some(_) => false,
    };
    if link["schema_version"] != 1
        || link["operation_id"] != id
        || link["technical_requester_id"]
            != crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
        || link["effective_manager_id"] != context.effective_manager_id()
        || link["automation_id"] != context.automation_id()
        || link["automation_revision"] != context.automation_revision()
        || link["project_id"] != context.project_id()
        || link["action"] != "task.request_changes"
        || !cause_sponsor_matches
        || !top_level_sponsor_matches
        || retained_cause != expected_cause
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "on-behalf feedback Operation link does not match its review cause",
        ));
    }
    request_changes_core(tx, FeedbackActor::Automation(context), &validated, id, now)
}

#[derive(Clone, Copy)]
enum FeedbackActor<'a> {
    Direct(&'a Principal),
    Automation(&'a ReviewDispositionContext),
}

impl<'a> FeedbackActor<'a> {
    fn decision_actor_id(self) -> &'a str {
        match self {
            Self::Direct(principal) => &principal.client_id,
            Self::Automation(context) => context.effective_manager_id(),
        }
    }

    fn automation_context(self) -> Option<&'a ReviewDispositionContext> {
        match self {
            Self::Direct(_) => None,
            Self::Automation(context) => Some(context),
        }
    }
}

fn request_changes_core(
    tx: &Transaction<'_>,
    actor: FeedbackActor<'_>,
    input: &ChangeRequest,
    id: &str,
    now: i64,
) -> Result<Value> {
    if let Some(context) = actor.automation_context() {
        context.require_current_action(tx)?;
    }
    let a = tasks::get_attempt(tx, &input.attempt_id)?;
    let scoped_manager = match actor {
        FeedbackActor::Direct(p) => {
            if p.role == Role::Manager && a["owner_id"] != p.client_id {
                super::gm::require_attempt_control(tx, p, &a)?;
            }
            let legacy_authority = p.role == Role::Operator
                || (p.role == Role::Manager
                    && super::gm::record(tx)?
                        .is_some_and(|record| record["client_id"] == p.client_id));
            let scoped_manager = !legacy_authority
                && p.role == Role::Manager
                && a["owner_id"] == p.client_id
                && crate::policy::allows_scoped_manager_feedback(&a["task_snapshot"]);
            if !legacy_authority && !scoped_manager {
                // Frozen v1, legacy, and unrecognized Attempts retain the historical
                // local-Operator/current-GM guard. V2 adds one exact owner-scoped path.
                super::gm::require_authority(tx, p)?;
            }
            scoped_manager
        }
        FeedbackActor::Automation(context) => {
            if a["owner_id"] != context.attempt_owner_id()
                || !crate::policy::allows_scoped_manager_feedback(&a["task_snapshot"])
            {
                return Err(Error::new(
                    "FORBIDDEN",
                    "manager-owned automation requires the exact transferred owner-policy-v2 Attempt",
                ));
            }
            true
        }
    };
    let doc = document(tx, &input.submission_ref)?;
    if doc["attempt_id"] != input.attempt_id
        || doc["task_revision"] != input.expected_revision
        || doc["candidate_ref"] != input.candidate_ref
    {
        return Err(Error::invalid(
            "review anchors do not match the named submission",
        ));
    }
    let spec: TaskSpec = serde_json::from_value(a["task_snapshot"]["spec"].clone())?;
    if input
        .requirement_ids
        .iter()
        .any(|id| !spec.requirements.iter().any(|r| &r.id == id))
    {
        return Err(Error::invalid(
            "review names a requirement outside this revision",
        ));
    }
    let decision_actor_id = actor.decision_actor_id();
    let automation_task = if let Some(context) = actor.automation_context() {
        let identity = context.identity();
        if input.attempt_id != identity.attempt_id
            || input.expected_revision != identity.task_revision
            || input.submission_ref != identity.submission_ref
            || input.candidate_ref != identity.candidate_ref
            || a["task_id"] != identity.task_id
            || a["task_revision"] != identity.task_revision
            || !a["released_at_ms"].is_null()
            || !matches!(a["state"].as_str(), Some("submitted" | "needs_correction"))
            || a["submission_ref"] != identity.submission_ref
            || a["candidate_ref"] != identity.candidate_ref
        {
            return Err(Error::new(
                "STALE_REVIEW_SUBJECT",
                "automation feedback must target its exact current assigned candidate",
            ));
        }
        let task = tasks::get_task(tx, &identity.task_id)?;
        if task["state"] != "open"
            || task["revision"] != identity.task_revision
            || task["current_attempt_id"] != identity.attempt_id
            || task["project_id"] != context.project_id()
        {
            return Err(Error::new(
                "STALE_REVIEW_SUBJECT",
                "automation feedback requires the exact current open Task and Attempt",
            ));
        }
        Some(task)
    } else {
        None
    };
    let automation_provenance = if let Some(context) = actor.automation_context() {
        let identity = context.identity();
        let provenance = super::reviews::actionable_finding(
            tx,
            &identity.task_id,
            &identity.attempt_id,
            identity.task_revision,
            &identity.submission_ref,
            &identity.candidate_ref,
            &input.finding_id,
        )?;
        require_automation_review_provenance(tx, context, input, &provenance)?;
        Some(provenance)
    } else {
        None
    };
    let finding = input.finding();
    let key = format!(
        "finding:{}",
        model::digest(
            model::canonical(&json!([
                decision_actor_id,
                input.submission_ref,
                input.finding_id
            ]))?
            .as_bytes()
        )
    );
    let old: Option<String> = tx.query_row("SELECT payload_json FROM observations WHERE source_stream_id='controller:review' AND source_event_key=?1", [&key], |r| r.get(0)).optional()?;
    if let Some(raw) = old {
        let mut prior: Value = serde_json::from_str(&raw)?;
        if prior["finding"] != finding {
            return Err(Error::new(
                "FINDING_ID_CONFLICT",
                "finding ID already names different feedback",
            ));
        }
        if actor.automation_context().is_some()
            && let Some(provenance) = automation_provenance.as_ref()
        {
            let feedback_operation_id = model::text(&prior, "operation_id")?;
            retain_review_disposition(
                tx,
                id,
                feedback_operation_id,
                decision_actor_id,
                input,
                provenance,
                now,
            )?;
        }
        prior["coalesced"] = json!(true);
        return Ok(prior);
    }
    let t = match automation_task {
        Some(task) => task,
        None => tasks::get_task(tx, model::text(&a, "task_id")?)?,
    };
    let applies = t["state"] == "open"
        && t["revision"] == input.expected_revision
        && t["current_attempt_id"] == input.attempt_id
        && a["task_revision"] == input.expected_revision
        && a["released_at_ms"].is_null()
        && a["submission_ref"] == input.submission_ref
        && a["candidate_ref"] == input.candidate_ref
        && matches!(a["state"].as_str(), Some("submitted" | "needs_correction"));
    if scoped_manager && !applies {
        return Err(Error::new(
            "STALE_REVIEW_SUBJECT",
            "owner-scoped feedback requires the exact current open Task submission",
        ));
    }
    let review_provenance = if let Some(provenance) = automation_provenance {
        Some(provenance)
    } else if applies {
        match super::reviews::actionable_finding(
            tx,
            model::text(&a, "task_id")?,
            &input.attempt_id,
            input.expected_revision,
            &input.submission_ref,
            &input.candidate_ref,
            &input.finding_id,
        ) {
            Ok(provenance) => Some(provenance),
            Err(error)
                if !scoped_manager
                    && matches!(
                        error.code.as_str(),
                        "REVIEW_FINDING_NOT_FOUND" | "REVIEW_FINDING_NOT_ACTIONABLE"
                    ) =>
            {
                None
            }
            Err(error) => return Err(error),
        }
    } else {
        None
    };
    if scoped_manager {
        let provenance = review_provenance.as_ref().ok_or_else(|| {
            Error::new(
                "REVIEW_FINDING_NOT_FOUND",
                "owner-scoped feedback requires a current assigned-auditor finding",
            )
        })?;
        if provenance["finding"]["requirement_ids"] != json!(input.requirement_ids) {
            return Err(Error::new(
                "REVIEW_FINDING_MISMATCH",
                "manager feedback must preserve the assigned finding's exact requirement scope",
            ));
        }
        let review_evidence = provenance["finding"]["evidence_refs"]
            .as_array()
            .ok_or_else(|| Error::new("REVIEW_RESULT_DAMAGED", "finding evidence is missing"))?;
        if review_evidence.iter().any(|reference| {
            !input
                .evidence
                .iter()
                .any(|provided| reference.as_str() == Some(provided.as_str()))
        }) {
            return Err(Error::new(
                "REVIEW_EVIDENCE_MISMATCH",
                "manager feedback must retain every evidence reference from the assigned finding",
            ));
        }
    }
    if applies && let Some(provenance) = &review_provenance {
        retain_review_disposition(tx, id, id, decision_actor_id, input, provenance, now)?;
    }
    let value = json!({"operation_id":id,"message_id":if applies {Some(id)} else {None},
        "sender":decision_actor_id,"recipient":a["owner_id"],"task_id":a["task_id"],
        "finding":finding,"text":input.reason,"applied":applies,
        "status":if applies {"needs_correction"} else {"stale_review"},
        "delivery":if applies {"durable_mailbox_only"} else {"historical_evidence_only"},
        "review_provenance":review_provenance,
        "native_input_sent":false,"acceptance_changed":false,"repair_started":false,"publication_started":false});
    if applies {
        tx.execute(
            "UPDATE attempts SET state='needs_correction',updated_at_ms=?2 WHERE attempt_id=?1",
            params![input.attempt_id, now],
        )?;
        super::capacity::sync_attempt(tx, &input.attempt_id, now)?;
    }
    tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1",
        params![id, a["task_id"].as_str(), input.attempt_id],
    )?;
    tx.execute("INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:review',?1,?2,?3,?4,?5)",
        params![key,id,if applies {"task.feedback"} else {"task.review_stale"},model::canonical(&value)?,now])?;
    Ok(value)
}

fn retain_review_disposition(
    tx: &Transaction<'_>,
    operation_id: &str,
    feedback_operation_id: &str,
    decision_actor_id: &str,
    input: &ChangeRequest,
    provenance: &Value,
    now: i64,
) -> Result<()> {
    let assignment_id = model::text(provenance, "review_assignment_id")?;
    let disposition_key = format!("disposition:{assignment_id}");
    let old_disposition: Option<String> = tx
        .query_row(
            "SELECT payload_json FROM observations WHERE source_stream_id='controller:review' AND source_event_key=?1 AND kind='review.disposition'",
            [&disposition_key],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(raw) = old_disposition {
        let prior: Value = serde_json::from_str(&raw)?;
        review_contract::validate_disposition(&prior).map_err(|_| {
            Error::new(
                "REVIEW_DISPOSITION_CONFLICT",
                "the exact review slot already has a different manager disposition",
            )
        })?;
        if prior["review_assignment_id"] != assignment_id
            || prior["identity"] != provenance["identity"]
            || prior["review_result_operation_id"] != provenance["review_operation_id"]
            || prior["disposition"]
                != review_contract::ReviewDisposition::ReturnForCorrection.as_str()
        {
            return Err(Error::new(
                "REVIEW_DISPOSITION_CONFLICT",
                "the exact review slot already has a different manager disposition",
            ));
        }
        return Ok(());
    }
    let disposition = json!({
        "schema_version":1,
        "kind":"review.disposition",
        "review_assignment_id":assignment_id,
        "operation_id":operation_id,
        "disposition":review_contract::ReviewDisposition::ReturnForCorrection.as_str(),
        "review_result_operation_id":provenance["review_operation_id"],
        "reason":input.reason,
        "evidence_refs":input.evidence,
        "finding_ids":[input.finding_id],
        "decided_by":decision_actor_id,
        "identity":provenance["identity"],
        "task_feedback_operation_id":feedback_operation_id,
    });
    review_contract::validate_disposition(&disposition).map_err(|error| {
        Error::new(
            "REVIEW_DISPOSITION_DAMAGED",
            format!("generated review disposition is invalid: {error}"),
        )
    })?;
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:review',?1,?2,'review.disposition',?3,?4)",
        params![disposition_key, operation_id, model::canonical(&disposition)?, now],
    )?;
    Ok(())
}

fn require_automation_review_provenance(
    tx: &Transaction<'_>,
    context: &ReviewDispositionContext,
    input: &ChangeRequest,
    provenance: &Value,
) -> Result<()> {
    let damaged = || {
        Error::new(
            "REVIEW_RESULT_DAMAGED",
            "automation feedback does not match its exact retained review result",
        )
    };
    let identity = context.identity();
    let finding = &provenance["finding"];
    if provenance["review_assignment_id"] != context.review_assignment_id()
        || provenance["review_operation_id"] != context.review_result_operation_id()
        || provenance["identity"] != json!(identity)
        || provenance["finding"]["finding_id"] != input.finding_id
        || input.attempt_id != identity.attempt_id
        || input.expected_revision != identity.task_revision
        || input.submission_ref != identity.submission_ref
        || input.candidate_ref != identity.candidate_ref
        || json!(input.reason) != finding["reason"]
        || json!(input.requirement_ids) != finding["requirement_ids"]
        || json!(input.evidence) != finding["evidence_refs"]
    {
        return Err(damaged());
    }

    let assignment_key = format!("assignment:{}", context.review_assignment_id());
    let assignment_row: Option<(String, String)> = tx
        .query_row(
            "SELECT payload_json,operation_id FROM observations \
             WHERE source_stream_id='controller:review' AND source_event_key=?1 \
               AND kind='review.assignment'",
            [&assignment_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((assignment_json, assignment_operation_id)) = assignment_row else {
        return Err(damaged());
    };
    let assignment: Value = serde_json::from_str(&assignment_json).map_err(|_| damaged())?;
    if assignment["review_assignment_id"] != context.review_assignment_id()
        || assignment["operation_id"] != assignment_operation_id
        || assignment["identity"] != json!(identity)
        || assignment["sponsor_client_id"] != context.review_assignment_sponsor_id()
        || assignment["reviewer_client_id"] != provenance["reviewer_client_id"]
    {
        return Err(damaged());
    }
    let assignment_result: Option<(String, String, Option<String>)> = tx
        .query_row(
            "SELECT method,state,result_json FROM operations WHERE operation_id=?1",
            [&assignment_operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((method, state, result_json)) = assignment_result else {
        return Err(damaged());
    };
    let assignment_operation_result: Value =
        serde_json::from_str(&result_json.ok_or_else(damaged)?).map_err(|_| damaged())?;
    if method != "review.assign"
        || state != "settled"
        || assignment_operation_result["review_assignment_id"] != context.review_assignment_id()
        || assignment_operation_result["identity"] != json!(identity)
        || assignment_operation_result["sponsor_client_id"]
            != context.review_assignment_sponsor_id()
    {
        return Err(damaged());
    }

    let result_key = format!("result:{}", context.review_assignment_id());
    let result_row: Option<(String, String)> = tx
        .query_row(
            "SELECT payload_json,operation_id FROM observations \
             WHERE source_stream_id='controller:review' AND source_event_key=?1 \
               AND kind='review.result'",
            [&result_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((result_json, result_operation_id)) = result_row else {
        return Err(damaged());
    };
    let record: Value = serde_json::from_str(&result_json).map_err(|_| damaged())?;
    let result_operation: Option<(String, String, Option<String>)> = tx
        .query_row(
            "SELECT caller_id,method,result_json FROM operations WHERE operation_id=?1 AND state='settled'",
            [&result_operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((reviewer_id, method, result_operation_json)) = result_operation else {
        return Err(damaged());
    };
    let result: Value =
        serde_json::from_str(&result_operation_json.ok_or_else(damaged)?).map_err(|_| damaged())?;
    if result_operation_id != context.review_result_operation_id()
        || record["review_assignment_id"] != context.review_assignment_id()
        || record["operation_id"] != context.review_result_operation_id()
        || record["identity"] != json!(identity)
        || record["result"] != result
        || reviewer_id != assignment["reviewer_client_id"]
        || method != "review.submit"
        || result["review_assignment_id"] != context.review_assignment_id()
        || result["reviewer_client_id"] != assignment["reviewer_client_id"]
        || result["sponsor_client_id"] != context.review_assignment_sponsor_id()
        || result["task_id"] != identity.task_id
        || result["attempt_id"] != identity.attempt_id
        || result["task_revision"] != identity.task_revision
        || result["submission_ref"] != identity.submission_ref
        || result["candidate_ref"] != identity.candidate_ref
        || result["verdict"] != "changes_requested"
        || result["applicability"] != "current_candidate"
    {
        return Err(damaged());
    }
    Ok(())
}
