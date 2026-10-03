//! Anchored submission and review transitions. No native input or file I/O in transactions.
use super::{current_principal, operations, results, tasks};
use crate::{
    artifacts::ArtifactRecord,
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
    p.owns(model::text(&a, "owner_id")?)?;
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
        let p = current_principal(&tx, p)?;
        let a = current(&tx, &p, &input)?;
        let candidate = candidate(&tx, &a, &input)?;
        if record.metadata["candidate_ref"] != candidate.artifact_id
            || record.metadata["candidate_sha256"] != candidate.content_digest
        {
            return Err(Error::conflict(
                "candidate changed during submission publication",
            ));
        }
        Ok(record)
    });
    let now = model::now_ms()?;
    let value = match checked {
        Ok(a) => {
            tx.execute("INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,'task_submission',?3,?4,?5,?6)",
                params![a.artifact_id,a.relative_path,i64::try_from(a.byte_length).map_err(|_| Error::invalid("submission too large"))?,a.content_digest,now,model::canonical(&a.metadata)?])?;
            tx.execute("UPDATE attempts SET state='submitted',submission_ref=?2,candidate_ref=?3,updated_at_ms=?4 WHERE attempt_id=?1",
                params![input.attempt_id,a.artifact_id,input.candidate_ref,now])?;
            json!({"operation_id":id,"outcome":"applied","attempt_id":input.attempt_id,
                "submission_ref":a.artifact_id,"candidate_ref":input.candidate_ref,
                "claim_counts":a.metadata["claim_counts"],"state":"submitted","task_accepted":false})
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
    let a = tasks::get_attempt(tx, &input.attempt_id)?;
    let legacy_authority = p.role == Role::Operator
        || (p.role == Role::Manager
            && super::gm::record(tx)?.is_some_and(|record| record["client_id"] == p.client_id));
    let scoped_manager = !legacy_authority
        && p.role == Role::Manager
        && a["owner_id"] == p.client_id
        && crate::policy::allows_scoped_manager_feedback(&a["task_snapshot"]);
    if !legacy_authority && !scoped_manager {
        // Frozen v1, legacy, and unrecognized Attempts retain the historical
        // local-Operator/current-GM guard. V2 adds one exact owner-scoped path.
        super::gm::require_authority(tx, p)?;
    }
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
    let finding = input.finding();
    let key = format!(
        "finding:{}",
        model::digest(
            model::canonical(&json!([
                p.client_id,
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
        prior["coalesced"] = json!(true);
        return Ok(prior);
    }
    let t = tasks::get_task(tx, model::text(&a, "task_id")?)?;
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
    let review_provenance = if applies {
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
        } else {
            let disposition = json!({
                "schema_version":1,
                "kind":"review.disposition",
                "review_assignment_id":assignment_id,
                "operation_id":id,
                "disposition":"return_for_correction",
                "review_result_operation_id":provenance["review_operation_id"],
                "reason":input.reason,
                "evidence_refs":input.evidence,
                "finding_ids":[input.finding_id],
                "decided_by":p.client_id,
                "identity":provenance["identity"],
                "task_feedback_operation_id":id,
            });
            tx.execute(
                "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:review',?1,?2,'review.disposition',?3,?4)",
                params![disposition_key, id, model::canonical(&disposition)?, now],
            )?;
        }
    }
    let value = json!({"operation_id":id,"message_id":if applies {Some(id)} else {None},
        "sender":p.client_id,"recipient":a["owner_id"],"task_id":a["task_id"],
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
