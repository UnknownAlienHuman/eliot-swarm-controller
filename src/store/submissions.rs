//! Anchored submission and review transitions. No native input or file I/O in transactions.
use super::{current_principal, operations, results, tasks};
use crate::{
    artifacts::ArtifactRecord,
    automation::disposition::ReviewDispositionContext,
    error::{Error, Result},
    model::{self, Principal, Role, TaskSpec},
    submission::{ChangeRequest, SubmitRequest, claim_counts},
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};

fn current(db: &Connection, p: &Principal, input: &SubmitRequest) -> Result<Value> {
    if !matches!(p.role, Role::Operator | Role::Manager) {
        return Err(Error::new(
            "FORBIDDEN",
            "submission requires a manager or operator",
        ));
    }
    let a = tasks::get_attempt(db, &input.attempt_id)?;
    super::gm::require_attempt_control(db, p, &a)?;
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
    let record = results::get(db, &input.candidate_ref)?;
    if record.kind == "source_snapshot" {
        if record.metadata["attempt_id"] != input.attempt_id
            || record.metadata["task_revision"] != input.expected_revision
        {
            return Err(Error::new(
                "CANDIDATE_SCOPE",
                "source snapshot belongs to another Attempt/revision",
            ));
        }
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
    if !a["binding_id"].is_null()
        && (a["binding_id"] != identity["binding_id"]
            || a["binding_generation"] != identity["generation"])
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

pub(super) fn reserve(tx: &Transaction<'_>, p: &Principal, v: &Value, id: &str) -> Result<Value> {
    let input = SubmitRequest::parse(v)?;
    let a = current(tx, p, &input)?;
    let candidate = candidate(tx, &a, &input)?;
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

pub(super) fn begin(
    db: &mut Connection,
    p: Principal,
    id: &str,
) -> Result<Option<(ArtifactRecord, Value)>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let p = current_principal(&tx, p)?;
    if !matches!(p.role, Role::Operator | Role::Manager) {
        return Err(Error::new(
            "FORBIDDEN",
            "submission requires a manager or operator",
        ));
    }
    let op = operations::get_operation(&tx, id)?;
    if op["method"] != "task.submit" || op["caller_id"] != p.client_id {
        return Err(Error::new(
            "FORBIDDEN",
            "submission belongs to another caller or method",
        ));
    }
    if !matches!(op["state"].as_str(), Some("queued" | "outcome_unknown")) {
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
    let now = model::now_ms()?;
    tx.execute("UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1", params![id,now])?;
    tx.commit()?;
    // This local immutable publication may resume after a host restart. It never
    // resumes/replays native work. A final CAS still guards every Task transition.
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
        // Publication is an already-crossed filesystem boundary. Do not let
        // a GM handover turn a verified immutable artifact into an orphan;
        // validate the retained submitter/candidate/Attempt provenance here,
        // while begin() remains the current-actor authorization boundary.
        let a = tasks::get_attempt(&tx, &input.attempt_id)?;
        let task = tasks::get_task(&tx, model::text(&a, "task_id")?)?;
        let candidate = candidate(&tx, &a, &input)?;
        let effective_raw: String = tx.query_row(
            "SELECT effective_request_json FROM operations WHERE operation_id=?1",
            [id],
            |row| row.get(0),
        )?;
        let effective: Value = serde_json::from_str(&effective_raw)?;
        let document = &effective["submission_document"];
        let mut expected_metadata = document.clone();
        if let Some(fields) = expected_metadata.as_object_mut() {
            fields.remove("claims");
            fields.remove("summary");
        }
        let bytes = model::canonical(document)?.into_bytes();
        let expected_artifact_id = format!("submission-{}", model::digest(id.as_bytes()));
        if document["schema_version"] != 1
            || document["operation_id"] != id
            || document["task_id"] != a["task_id"]
            || op["task_id"] != document["task_id"]
            || document["attempt_id"] != input.attempt_id
            || op["attempt_id"] != document["attempt_id"]
            || document["task_revision"] != input.expected_revision
            || document["owner_id"] != a["owner_id"]
            || document["submitted_by"] != op["caller_id"]
            || document["candidate_ref"] != candidate.artifact_id
            || document["candidate_sha256"] != candidate.content_digest
            || document["candidate_kind"] != candidate.kind
            || document["candidate_byte_length"] != candidate.byte_length
            || document["previous_submission_ref"] != json!(input.expected_submission_ref)
            || record.kind != "task_submission"
            || record.artifact_id != expected_artifact_id
            || record.relative_path != format!("artifacts/{expected_artifact_id}.bin")
            || record.byte_length != bytes.len() as u64
            || record.content_digest != model::digest(&bytes)
            || record.metadata != expected_metadata
        {
            return Err(Error::conflict(
                "published submission differs from its retained operation, Attempt, or candidate",
            ));
        }
        let still_current = task["state"] == "open"
            && task["revision"] == input.expected_revision
            && task["current_attempt_id"] == input.attempt_id
            && a["task_revision"] == input.expected_revision
            && a["released_at_ms"].is_null()
            && a["submission_ref"] == json!(input.expected_submission_ref)
            && matches!(
                a["state"].as_str(),
                Some(
                    "reserved" | "running" | "submitted" | "needs_correction" | "recovery_pending"
                )
            );
        Ok((record, still_current))
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
    tx.execute("UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1", params![id,model::canonical(&value)?,now])?;
    super::capacity::sync_attempt(&tx, &input.attempt_id, now)?;
    tx.execute("INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller',?1,?2,'task.submission',?3,?4)",
        params![format!("submission:{id}"),id,model::canonical(&value)?,now])?;
    tx.commit()?;
    Ok(())
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
    let cause = json!({
        "kind":"review_result",
        "id":context.review_assignment_id(),
        "review_assignment_id":context.review_assignment_id(),
        "operation_id":context.review_result_operation_id(),
        "identity":context.identity(),
    });
    if link["schema_version"] != 1
        || link["operation_id"] != id
        || link["technical_requester_id"]
            != crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
        || link["effective_manager_id"] != context.effective_manager_id()
        || link["automation_id"] != context.automation_id()
        || link["automation_revision"] != context.automation_revision()
        || link["project_id"] != context.project_id()
        || link["action"] != "task.request_changes"
        || link["cause"] != cause
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
            if a["owner_id"] != context.effective_manager_id()
                || !crate::policy::allows_scoped_manager_feedback(&a["task_snapshot"])
            {
                return Err(Error::new(
                    "FORBIDDEN",
                    "manager-owned automation requires the current owner-policy-v2 Attempt owner",
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
        if prior["review_assignment_id"] != assignment_id
            || prior["identity"] != provenance["identity"]
            || prior["review_result_operation_id"] != provenance["review_operation_id"]
            || prior["disposition"] != "return_for_correction"
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
        "disposition":"return_for_correction",
        "review_result_operation_id":provenance["review_operation_id"],
        "reason":input.reason,
        "evidence_refs":input.evidence,
        "finding_ids":[input.finding_id],
        "decided_by":decision_actor_id,
        "identity":provenance["identity"],
        "task_feedback_operation_id":feedback_operation_id,
    });
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
        || assignment["sponsor_client_id"] != context.effective_manager_id()
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
        || assignment_operation_result["sponsor_client_id"] != context.effective_manager_id()
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
        || result["sponsor_client_id"] != context.effective_manager_id()
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
