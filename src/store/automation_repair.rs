//! Durable selected RepairDispatch consumer.
//!
//! This consumer reads only committed manager disposition/feedback facts. It
//! prepares a typed owner correction request and delegates admission to the
//! shared Store/runtime path; this module never creates a Principal or starts
//! native work. The canonical result cursor owns cause ordering; the delivery
//! Operation, link, and semantic slot are retained in the caller's transaction.

use super::{capacity, reviews};
use crate::{
    automation::{
        actions::AutomationStep,
        config::{self, AutomationEntry},
        repair::{RepairDeliveryRequest, RepairDispatchContext},
    },
    error::{Error, Result},
    model,
    model::{Principal, Role},
    review::{ReviewFinding, ReviewSlotIdentity},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const SLOT_SCHEMA_VERSION: u32 = 1;
const LINK_SCHEMA_VERSION: u32 = 1;
const REVIEW_STREAM: &str = "controller:review";
const SLOT_PREFIX: &str = "repair:v1:semantic-slot:";
const OPERATION_LINK_PREFIX: &str = "repair:v1:operation-link:";
const ENTRY_LINK_PREFIX: &str = "repair:v1:operation_by_entry:";

type RepairOperationRow = (
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RepairSlotReceipt {
    schema_version: u32,
    semantic_slot_id: String,
    effective_manager_id: String,
    task_id: String,
    task_revision: i64,
    attempt_id: String,
    submission_ref: String,
    candidate_ref: String,
    finding_id: String,
    parameters_digest: String,
    operation_id: String,
    reserved_at_ms: i64,
}

/// Validated on-behalf provenance for one exact correction delivery
/// Operation. This is evidence and owner-linkage, not credentials.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RepairDispatchOperationLink {
    pub(crate) schema_version: u32,
    pub(crate) operation_id: String,
    pub(crate) technical_requester_id: String,
    pub(crate) effective_manager_id: String,
    pub(crate) automation_id: String,
    pub(crate) automation_revision: i64,
    pub(crate) project_id: String,
    pub(crate) action: String,
    pub(crate) semantic_cause_kind: String,
    pub(crate) semantic_cause_id: String,
    pub(crate) semantic_slot_id: String,
    pub(crate) task_id: String,
    pub(crate) task_revision: i64,
    pub(crate) attempt_id: String,
    pub(crate) submission_ref: String,
    pub(crate) candidate_ref: String,
    pub(crate) finding_id: String,
    pub(crate) review_assignment_id: String,
    pub(crate) review_result_operation_id: String,
    pub(crate) disposition_operation_id: String,
    pub(crate) feedback_operation_id: String,
    pub(crate) feedback_observation_id: i64,
    pub(crate) binding_id: String,
    pub(crate) binding_generation: i64,
    pub(crate) request_digest: String,
    pub(crate) identity: ReviewSlotIdentity,
    pub(crate) finding: ReviewFinding,
    pub(crate) cause: Value,
    pub(crate) linked_at_ms: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedRepairDispatch {
    context: RepairDispatchContext,
    request: RepairDeliveryRequest,
}

/// Exact correction request recognized from a direct authenticated Manager
/// request. It carries only verified source identity and is not an authority
/// object; the direct Store handler still performs ordinary agent.send auth.
#[derive(Debug, Clone)]
pub(crate) struct DirectRepairSlot {
    manager_id: String,
    identity: ReviewSlotIdentity,
    finding: ReviewFinding,
    assignment_id: String,
    result_operation_id: String,
    disposition_operation_id: String,
    feedback_operation_id: String,
    feedback_observation_id: i64,
    binding_id: String,
    binding_generation: i64,
    semantic_slot_id: String,
    request: Value,
    request_digest: String,
}

struct CommittedRepairReview {
    result: Value,
    identity: ReviewSlotIdentity,
}

impl PreparedRepairDispatch {
    pub(crate) fn context(&self) -> &RepairDispatchContext {
        &self.context
    }

    pub(crate) fn request_value(&self) -> Value {
        self.request.value()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RepairSlotResolution {
    Vacant,
    Existing {
        operation_id: String,
        operation_state: String,
    },
    Conflict {
        operation_id: String,
        operation_state: String,
    },
}

/// Consume one exact review result from the shared automation result cursor.
/// The caller owns cause ordering and retry state; this function never scans
/// observations or advances an independent repair cursor.
pub(crate) fn consume_review_result_for_entry(
    tx: &Transaction<'_>,
    runtime_config: &crate::config::Config,
    entry: &AutomationEntry,
    assignment_id: &str,
    result_operation_id: &str,
    now_ms: i64,
) -> Result<Value> {
    if !entry.enabled || !entry.steps.contains(&AutomationStep::RepairDispatch) {
        return Ok(json!({
            "status":"skipped",
            "reason":"repair_dispatch_not_selected",
            "review_assignment_id":assignment_id,
            "review_result_operation_id":result_operation_id
        }));
    }
    config::validate_entry(entry)?;
    if entry.scope.work_pool_id.is_some() {
        return Ok(json!({
            "status":"capability_gap",
            "code":"work_pool_scope_unavailable",
            "review_assignment_id":assignment_id,
            "review_result_operation_id":result_operation_id
        }));
    }
    if !repair_entry_is_current(tx, entry)? {
        return Ok(repair_skipped(
            assignment_id,
            result_operation_id,
            "repair_automation_action_changed",
        ));
    }
    let Some(review) = committed_review_result(tx, assignment_id, result_operation_id)? else {
        return Ok(json!({
            "status":"skipped",
            "reason":"not_canonical_review_result_event",
            "review_assignment_id":assignment_id,
            "review_result_operation_id":result_operation_id
        }));
    };
    let result = &review.result;
    let Some(review_sponsor) = result["sponsor_client_id"].as_str() else {
        return Err(source_gap(
            "committed review result has no sponsor identity",
        ));
    };
    if result["applicability"] != "current_candidate" {
        return Ok(repair_skipped(
            assignment_id,
            result_operation_id,
            "historical_review_result",
        ));
    }
    match result["verdict"].as_str() {
        Some("changes_requested") => {}
        Some("pass") => {
            return Ok(repair_skipped(
                assignment_id,
                result_operation_id,
                "review_pass_is_advisory",
            ));
        }
        Some("inconclusive") => {
            return Ok(repair_skipped(
                assignment_id,
                result_operation_id,
                "inconclusive_review_result",
            ));
        }
        _ => {
            return Err(source_gap(
                "committed review result has an unsupported verdict",
            ));
        }
    }

    let disposition_key = format!("disposition:{assignment_id}");
    let disposition_row: Option<(Option<String>, String)> = tx
        .query_row(
            "SELECT operation_id,payload_json FROM observations \
             WHERE source_stream_id=?1 AND source_event_key=?2 AND kind='review.disposition'",
            params![REVIEW_STREAM, disposition_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((disposition_operation_id, disposition_json)) = disposition_row else {
        return Ok(repair_pending(
            assignment_id,
            result_operation_id,
            "awaiting_explicit_applied_feedback",
            "manager_applies_exact_finding_feedback",
        ));
    };
    let disposition_operation_id = disposition_operation_id
        .filter(|value| !value.is_empty())
        .ok_or_else(|| source_gap("review disposition observation has no Operation identity"))?;
    let disposition: Value = serde_json::from_str(&disposition_json)
        .map_err(|_| source_gap("review disposition payload is invalid"))?;
    if disposition["schema_version"] != 1
        || disposition["kind"] != "review.disposition"
        || disposition["review_assignment_id"] != assignment_id
        || disposition["operation_id"] != disposition_operation_id
        || disposition["review_result_operation_id"] != result_operation_id
        || disposition["identity"] != json!(review.identity)
    {
        return Err(source_gap(
            "review disposition does not match the exact result cause",
        ));
    }
    if disposition["disposition"] != "return_for_correction" {
        return Ok(repair_skipped(
            assignment_id,
            result_operation_id,
            "manager_disposition_does_not_request_correction",
        ));
    }
    if disposition["decided_by"] != review_sponsor {
        return Ok(repair_skipped(
            assignment_id,
            result_operation_id,
            "feedback_was_not_applied_by_review_sponsor",
        ));
    }
    let finding_ids = disposition["finding_ids"]
        .as_array()
        .ok_or_else(|| source_gap("review disposition has no selected finding list"))?;
    if finding_ids.len() != 1 {
        return Ok(json!({
            "status":"capability_gap",
            "code":"repair_finding_selection_unsupported",
            "review_assignment_id":assignment_id,
            "review_result_operation_id":result_operation_id,
            "selected_finding_count":finding_ids.len()
        }));
    }
    let finding_id = finding_ids[0]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| source_gap("review disposition finding identity is invalid"))?;
    let feedback_operation_id = disposition["task_feedback_operation_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| source_gap("review disposition has no feedback Operation identity"))?;
    let feedback_observation_id = observation_id_for_feedback(tx, feedback_operation_id)?;
    let provenance = match reviews::actionable_finding(
        tx,
        &review.identity.task_id,
        &review.identity.attempt_id,
        review.identity.task_revision,
        &review.identity.submission_ref,
        &review.identity.candidate_ref,
        finding_id,
    ) {
        Ok(provenance) => provenance,
        Err(error)
            if matches!(
                error.code.as_str(),
                "REVIEW_FINDING_NOT_FOUND"
                    | "REVIEW_FINDING_NOT_ACTIONABLE"
                    | "REVIEW_ANCHOR_MISMATCH"
            ) =>
        {
            return Ok(repair_skipped(
                assignment_id,
                result_operation_id,
                "selected_finding_is_no_longer_actionable",
            ));
        }
        Err(error) => return Err(error),
    };
    if provenance["review_assignment_id"] != assignment_id
        || provenance["review_operation_id"] != result_operation_id
        || provenance["identity"] != json!(review.identity)
        || provenance["finding"]["finding_id"] != finding_id
    {
        return Err(source_gap(
            "actionable finding differs from the exact review result",
        ));
    }
    let finding: ReviewFinding = serde_json::from_value(provenance["finding"].clone())
        .map_err(|_| source_gap("actionable review finding fields are invalid"))?;
    if review_sponsor != entry.owner_manager_id {
        return consume_transferred_repair_slot(
            tx,
            entry,
            review_sponsor,
            assignment_id,
            result_operation_id,
            &disposition_operation_id,
            feedback_operation_id,
            feedback_observation_id,
            review.identity,
            finding,
        );
    }
    let context = match RepairDispatchContext::from_committed_disposition(
        tx,
        entry,
        review.identity.clone(),
        assignment_id,
        result_operation_id,
        &disposition_operation_id,
        feedback_operation_id,
        feedback_observation_id,
        finding,
    ) {
        Ok(context) => context,
        Err(error) if error.code == "REPAIR_OWNER_UNAVAILABLE" => {
            return Ok(repair_pending(
                assignment_id,
                result_operation_id,
                "current_owner_binding_not_ready",
                "owner_binding_ready",
            ));
        }
        Err(error) if error.code == "REPAIR_REQUEST_TOO_LARGE" => {
            return Ok(json!({
                "status":"capability_gap",
                "code":"repair_request_exceeds_native_input_bound",
                "review_assignment_id":assignment_id,
                "review_result_operation_id":result_operation_id
            }));
        }
        Err(error) if matches!(error.code.as_str(), "FORBIDDEN" | "UNAUTHORIZED") => {
            return Ok(repair_pending(
                assignment_id,
                result_operation_id,
                "manager_authority_unavailable",
                "manager_registration_or_rights_restored",
            ));
        }
        Err(error) if error.code == "STALE_REPAIR_SUBJECT" => {
            return Ok(repair_skipped(
                assignment_id,
                result_operation_id,
                "task_attempt_candidate_or_policy_is_no_longer_current",
            ));
        }
        Err(error)
            if matches!(
                error.code.as_str(),
                "AUTOMATION_ACTION_CHANGED"
                    | "AUTOMATION_ACTION_UNAVAILABLE"
                    | "AUTOMATION_NOT_FOUND"
            ) =>
        {
            return Ok(repair_skipped(
                assignment_id,
                result_operation_id,
                "repair_automation_action_changed",
            ));
        }
        Err(error) => return Err(error),
    };
    let request = match context.delivery_request() {
        Ok(request) => request,
        Err(error) if error.code == "REPAIR_REQUEST_TOO_LARGE" => {
            return Ok(json!({
                "status":"capability_gap",
                "code":"repair_request_exceeds_native_input_bound",
                "review_assignment_id":assignment_id,
                "review_result_operation_id":result_operation_id
            }));
        }
        Err(error) => return Err(error),
    };
    let prepared = PreparedRepairDispatch { context, request };
    match resolve_semantic_slot(tx, &prepared)? {
        RepairSlotResolution::Existing {
            operation_id,
            operation_state: _,
        } => match delivery_state(tx, &operation_id, &prepared)? {
            DeliveryState::Verified => Ok(repair_delivered(
                assignment_id,
                result_operation_id,
                &operation_id,
                prepared.context.semantic_slot_id(),
            )),
            DeliveryState::NoEffect => Ok(repair_skipped(
                assignment_id,
                result_operation_id,
                "prior_repair_operation_ended_without_delivery",
            )),
            DeliveryState::Pending(reason) => Ok(repair_pending(
                assignment_id,
                result_operation_id,
                &reason,
                "exact_delivery_operation_readback",
            )),
        },
        RepairSlotResolution::Conflict { .. } => Ok(repair_skipped(
            assignment_id,
            result_operation_id,
            "repair_semantic_slot_conflict",
        )),
        RepairSlotResolution::Vacant => admit_repair_operation(
            tx,
            runtime_config,
            &prepared,
            assignment_id,
            result_operation_id,
            now_ms,
        ),
    }
}

fn repair_entry_is_current(db: &Connection, entry: &AutomationEntry) -> Result<bool> {
    let Some(current) = config::load_entry(
        db,
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?
    else {
        return Ok(false);
    };
    Ok(current.enabled
        && current.steps.contains(&AutomationStep::RepairDispatch)
        && current.scope.work_pool_id.is_none())
}

#[allow(clippy::too_many_arguments)] // The bridge matches one exact transferred review source and its durable historical slot.
fn consume_transferred_repair_slot(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    historical_owner_id: &str,
    assignment_id: &str,
    result_operation_id: &str,
    disposition_operation_id: &str,
    feedback_operation_id: &str,
    feedback_observation_id: i64,
    identity: ReviewSlotIdentity,
    finding: ReviewFinding,
) -> Result<Value> {
    let lineage = config::transfer_lineage(
        tx,
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?;
    if !lineage
        .iter()
        .any(|edge| edge.former_owner_manager_id == historical_owner_id)
    {
        return Ok(repair_skipped(
            assignment_id,
            result_operation_id,
            "review_sponsor_is_not_in_current_automation_transfer_lineage",
        ));
    }

    let semantic_slot_id = crate::automation::repair::semantic_slot_id(
        historical_owner_id,
        &identity,
        &finding.finding_id,
    )?;
    let Some(value) = config::read_record(
        tx,
        &slot_key(&semantic_slot_id),
        "historical RepairDispatch semantic slot",
    )?
    else {
        // Transfer history alone is not authority to create a new slot or a
        // new native send. Continuation is limited to the exact old queued op.
        return Ok(repair_skipped(
            assignment_id,
            result_operation_id,
            "transferred_repair_operation_not_previously_admitted",
        ));
    };
    let receipt: RepairSlotReceipt = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "REPAIR_SLOT_CORRUPT",
            "historical RepairDispatch slot fields are invalid",
        )
    })?;
    if receipt.schema_version != SLOT_SCHEMA_VERSION
        || receipt.semantic_slot_id != semantic_slot_id
        || receipt.effective_manager_id != historical_owner_id
        || receipt.task_id != identity.task_id
        || receipt.task_revision != identity.task_revision
        || receipt.attempt_id != identity.attempt_id
        || receipt.submission_ref != identity.submission_ref
        || receipt.candidate_ref != identity.candidate_ref
        || receipt.finding_id != finding.finding_id
    {
        return Err(Error::new(
            "REPAIR_SLOT_CORRUPT",
            "historical RepairDispatch slot belongs to another semantic subject",
        ));
    }

    let context = context_for_delivery_operation(tx, &receipt.operation_id)?;
    if context.effective_manager_id() != historical_owner_id
        || context.automation_id() != entry.automation_id
        || context.project_id() != entry.project_id
        || context.semantic_slot_id() != semantic_slot_id
        || context.review_assignment_id() != assignment_id
        || context.review_result_operation_id() != result_operation_id
        || context.disposition_operation_id() != disposition_operation_id
        || context.feedback_operation_id() != feedback_operation_id
        || context.feedback_observation_id() != feedback_observation_id
        || context.identity() != &identity
        || serde_json::to_value(context.finding())? != serde_json::to_value(&finding)?
    {
        return Err(Error::new(
            "REPAIR_LINK_CORRUPT",
            "historical RepairDispatch Operation is not the exact current review and feedback cause",
        ));
    }
    let request = context.delivery_request()?;
    let prepared = PreparedRepairDispatch { context, request };
    match resolve_semantic_slot(tx, &prepared)? {
        RepairSlotResolution::Existing { operation_id, .. }
            if operation_id == receipt.operation_id => {}
        RepairSlotResolution::Conflict { .. } => {
            return Ok(repair_skipped(
                assignment_id,
                result_operation_id,
                "historical_repair_semantic_slot_conflict",
            ));
        }
        RepairSlotResolution::Existing { .. } | RepairSlotResolution::Vacant => {
            return Err(Error::new(
                "REPAIR_SLOT_CORRUPT",
                "historical RepairDispatch slot resolved to a different Operation",
            ));
        }
    }

    let (operation_state, sent_at_ms): (String, Option<i64>) = tx.query_row(
        "SELECT state,sent_at_ms FROM operations WHERE operation_id=?1",
        [&receipt.operation_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if operation_state == "queued" && sent_at_ms.is_none() {
        let continuation = crate::automation::authorization::current_transfer_continuation(
            tx,
            &receipt.operation_id,
            "agent.send",
            AutomationStep::RepairDispatch,
            &identity.task_id,
        )?
        .ok_or_else(|| {
            Error::new(
                "FORBIDDEN",
                "historical queued RepairDispatch has no current-GM transfer continuation",
            )
        })?;
        prepared
            .context
            .require_transfer_continuation_matches(&continuation)?;
    }

    match delivery_state(tx, &receipt.operation_id, &prepared)? {
        DeliveryState::Verified => Ok(repair_delivered(
            assignment_id,
            result_operation_id,
            &receipt.operation_id,
            &semantic_slot_id,
        )),
        DeliveryState::NoEffect => Ok(repair_skipped(
            assignment_id,
            result_operation_id,
            "prior_repair_operation_ended_without_delivery",
        )),
        DeliveryState::Pending(reason) => Ok(repair_pending(
            assignment_id,
            result_operation_id,
            &reason,
            "exact_historical_delivery_operation_readback",
        )),
    }
}

fn committed_review_result(
    db: &Connection,
    assignment_id: &str,
    result_operation_id: &str,
) -> Result<Option<CommittedRepairReview>> {
    validate_text(assignment_id, "review_assignment_id")?;
    validate_text(result_operation_id, "review_result_operation_id")?;
    let assignment_key = format!("assignment:{assignment_id}");
    let assignment_row: Option<(i64, String, String)> = db
        .query_row(
            "SELECT observation_id,operation_id,payload_json FROM observations \
             WHERE source_stream_id=?1 AND source_event_key=?2 AND kind='review.assignment'",
            params![REVIEW_STREAM, assignment_key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((_, assignment_operation_id, assignment_json)) = assignment_row else {
        return Err(source_gap(
            "review result cause has no retained exact assignment",
        ));
    };
    let assignment: Value = serde_json::from_str(&assignment_json)
        .map_err(|_| source_gap("review assignment payload is invalid"))?;
    if assignment["review_assignment_id"] != assignment_id
        || assignment["operation_id"] != assignment_operation_id
    {
        return Err(source_gap(
            "review assignment identity differs from its event key",
        ));
    }
    let assignment_operation: Option<(String, String, Option<String>)> = db
        .query_row(
            "SELECT method,state,result_json FROM operations WHERE operation_id=?1",
            [&assignment_operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((assignment_method, assignment_state, assignment_result_json)) = assignment_operation
    else {
        return Err(source_gap("review assignment Operation is missing"));
    };
    let assignment_result: Value = serde_json::from_str(
        &assignment_result_json.ok_or_else(|| source_gap("review assignment has no result"))?,
    )
    .map_err(|_| source_gap("review assignment result is invalid"))?;
    if assignment_method != "review.assign"
        || assignment_state != "settled"
        || assignment_result["review_assignment_id"] != assignment_id
        || assignment_result["identity"] != assignment["identity"]
        || assignment_result["sponsor_client_id"] != assignment["sponsor_client_id"]
    {
        return Err(source_gap(
            "review assignment lacks its exact settled Operation",
        ));
    }

    let result_key = format!("result:{assignment_id}");
    let result_row: Option<(String, String)> = db
        .query_row(
            "SELECT operation_id,payload_json FROM observations \
             WHERE source_stream_id=?1 AND source_event_key=?2 AND kind='review.result'",
            params![REVIEW_STREAM, result_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((observed_result_operation_id, result_json)) = result_row else {
        return Ok(None);
    };
    if observed_result_operation_id != result_operation_id {
        return Ok(None);
    }
    let record: Value = serde_json::from_str(&result_json)
        .map_err(|_| source_gap("review result payload is invalid"))?;
    let identity: ReviewSlotIdentity = serde_json::from_value(assignment["identity"].clone())
        .map_err(|_| source_gap("review assignment identity is invalid"))?;
    if record["schema_version"] != 1
        || record["review_assignment_id"] != assignment_id
        || record["operation_id"] != result_operation_id
        || record["identity"] != json!(identity)
    {
        return Err(source_gap(
            "review result differs from the exact assignment",
        ));
    }
    let result_operation: Option<(String, String, String, Option<String>)> = db
        .query_row(
            "SELECT caller_id,method,state,result_json FROM operations WHERE operation_id=?1",
            [result_operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((reviewer_id, method, state, result_operation_json)) = result_operation else {
        return Err(source_gap("review result Operation is missing"));
    };
    let result: Value = serde_json::from_str(
        &result_operation_json
            .ok_or_else(|| source_gap("review result Operation has no result"))?,
    )
    .map_err(|_| source_gap("review result Operation result is invalid"))?;
    if method != "review.submit"
        || state != "settled"
        || reviewer_id
            != assignment["reviewer_client_id"]
                .as_str()
                .unwrap_or_default()
        || result != record["result"]
        || result["review_assignment_id"] != assignment_id
        || result["reviewer_client_id"] != assignment["reviewer_client_id"]
        || result["sponsor_client_id"] != assignment["sponsor_client_id"]
        || result["task_id"] != identity.task_id
        || result["task_revision"] != identity.task_revision
        || result["attempt_id"] != identity.attempt_id
        || result["submission_ref"] != identity.submission_ref
        || result["candidate_ref"] != identity.candidate_ref
    {
        return Err(source_gap(
            "review result lacks its exact settled assigned-review Operation",
        ));
    }
    Ok(Some(CommittedRepairReview { result, identity }))
}

fn admit_repair_operation(
    tx: &Transaction<'_>,
    runtime_config: &crate::config::Config,
    prepared: &PreparedRepairDispatch,
    assignment_id: &str,
    result_operation_id: &str,
    now_ms: i64,
) -> Result<Value> {
    if let Err(error) = prepared.context.require_current_for_admission(tx) {
        return Ok(match error.code.as_str() {
            "REPAIR_OWNER_UNAVAILABLE" => repair_pending(
                assignment_id,
                result_operation_id,
                "current_owner_binding_not_ready",
                "owner_binding_ready",
            ),
            "REPAIR_NATIVE_EFFECT_UNRESOLVED" => repair_pending(
                assignment_id,
                result_operation_id,
                "prior_native_effect_requires_reconciliation",
                "owner_binding_effect_reconciled",
            ),
            "UNAUTHORIZED" | "FORBIDDEN" => repair_pending(
                assignment_id,
                result_operation_id,
                "manager_authority_unavailable",
                "manager_registration_or_rights_restored",
            ),
            "STALE_REPAIR_SUBJECT" => repair_skipped(
                assignment_id,
                result_operation_id,
                "task_attempt_candidate_or_policy_is_no_longer_current",
            ),
            "REPAIR_REQUEST_TOO_LARGE" => json!({
                "status":"capability_gap",
                "code":"repair_request_exceeds_native_input_bound",
                "review_assignment_id":assignment_id,
                "review_result_operation_id":result_operation_id
            }),
            _ => return Err(error),
        });
    }

    let request = prepared.request_value();
    let original_json = model::canonical(&request)?;
    let caller_id = prepared.context.technical_requester_id();
    if tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM operations WHERE caller_id=?1 AND client_request_id=?2)",
        params![caller_id, prepared.context.semantic_slot_id()],
        |row| row.get::<_, bool>(0),
    )? {
        return Err(Error::new(
            "REPAIR_SLOT_CORRUPT",
            "repair request identity exists without its retained semantic slot",
        ));
    }
    let operation_id = model::new_id();
    let effective = json!({
        "request":request,
        "automation_on_behalf":prepared.context.linkage_value()
    });
    tx.execute_batch("SAVEPOINT automation_repair_delivery")?;
    let admission = (|| -> Result<Value> {
        tx.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) \
             VALUES(?1,?2,?3,'agent.send',?4,?5,'queued',?6,?6,?6)",
            params![
                operation_id,
                caller_id,
                prepared.context.semantic_slot_id(),
                original_json,
                model::canonical(&effective)?,
                now_ms
            ],
        )?;
        let value = super::runtime::user_command_for_repair(
            tx,
            &prepared.context,
            &operation_id,
            runtime_config,
        )?;
        let effective_json: String = tx.query_row(
            "SELECT effective_request_json FROM operations WHERE operation_id=?1",
            [&operation_id],
            |row| row.get(0),
        )?;
        let mut effective: Value = serde_json::from_str(&effective_json)?;
        effective["receipt"] = json!({"ok":true,"value":value});
        tx.execute(
            "UPDATE operations SET result_json=?2,effective_request_json=?3,updated_at_ms=?4 WHERE operation_id=?1 AND state='queued'",
            params![operation_id, model::canonical(&value)?, model::canonical(&effective)?, now_ms],
        )?;
        capacity::sync_operation(tx, &operation_id, now_ms)?;
        tx.execute(
            "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
             VALUES('controller',?1,?1,'agent.send',?2,?3)",
            params![operation_id, model::canonical(&value)?, now_ms],
        )?;
        retain_admission(tx, prepared, &operation_id, now_ms)?;
        Ok(value)
    })();
    match admission {
        Ok(_) => {
            tx.execute_batch("RELEASE SAVEPOINT automation_repair_delivery")?;
            let mut pending = repair_pending(
                assignment_id,
                result_operation_id,
                "correction_operation_queued_readback_pending",
                "exact_delivery_operation_readback",
            );
            pending["operation_id"] = json!(operation_id);
            pending["semantic_slot_id"] = json!(prepared.context.semantic_slot_id());
            Ok(pending)
        }
        Err(error) => {
            tx.execute_batch(
                "ROLLBACK TO SAVEPOINT automation_repair_delivery; \
                 RELEASE SAVEPOINT automation_repair_delivery",
            )?;
            match error.code.as_str() {
                "REPAIR_OWNER_UNAVAILABLE" | "BINDING_NOT_READY" | "ADMISSION_DISABLED" => {
                    Ok(repair_pending(
                        assignment_id,
                        result_operation_id,
                        "current_owner_binding_not_ready",
                        "owner_binding_ready_or_admission_enabled",
                    ))
                }
                "REPAIR_NATIVE_EFFECT_UNRESOLVED" => Ok(repair_pending(
                    assignment_id,
                    result_operation_id,
                    "prior_native_effect_requires_reconciliation",
                    "owner_binding_effect_reconciled",
                )),
                "STALE_REPAIR_SUBJECT" => Ok(repair_skipped(
                    assignment_id,
                    result_operation_id,
                    "task_attempt_candidate_or_policy_is_no_longer_current",
                )),
                "FORBIDDEN" | "UNAUTHORIZED" => Ok(repair_pending(
                    assignment_id,
                    result_operation_id,
                    "manager_authority_unavailable",
                    "manager_registration_or_rights_restored",
                )),
                _ => Err(error),
            }
        }
    }
}

fn repair_skipped(assignment_id: &str, result_operation_id: &str, reason: &str) -> Value {
    json!({
        "status":"skipped",
        "reason":reason,
        "review_assignment_id":assignment_id,
        "review_result_operation_id":result_operation_id,
        "delivery_verified":false
    })
}

fn repair_pending(
    assignment_id: &str,
    result_operation_id: &str,
    reason: &str,
    wake_when: &str,
) -> Value {
    json!({
        "status":"pending",
        "reason":reason,
        "wake_when":[wake_when],
        "review_assignment_id":assignment_id,
        "review_result_operation_id":result_operation_id,
        "delivery_verified":false
    })
}

fn repair_delivered(
    assignment_id: &str,
    result_operation_id: &str,
    operation_id: &str,
    semantic_slot_id: &str,
) -> Value {
    json!({
        "status":"delivered",
        "review_assignment_id":assignment_id,
        "review_result_operation_id":result_operation_id,
        "operation_id":operation_id,
        "semantic_slot_id":semantic_slot_id,
        "delivery_verified":true
    })
}

/// Recognize a direct request only when its delivery payload exactly matches
/// one current manager disposition. The caller request ID remains caller-owned
/// and is excluded from semantic slot identity. Generic sends and requests
/// with extra/different parameters remain ordinary sends.
pub(crate) fn recognize_direct_correction_request(
    db: &Connection,
    principal: &Principal,
    request: &Value,
) -> Result<Option<DirectRepairSlot>> {
    if principal.role != Role::Manager
        || request["delivery"] != "next_turn"
        || request["binding_id"].as_str().is_none_or(str::is_empty)
        || request["generation"]
            .as_i64()
            .is_none_or(|value| value <= 0)
    {
        return Ok(None);
    }
    crate::automation::authorization::require_registered_manager(db, &principal.client_id)?;
    let binding_id = request["binding_id"].as_str().unwrap_or_default();
    let binding_generation = request["generation"].as_i64().unwrap_or_default();
    let mut statement = db.prepare(
        "SELECT o.source_event_key,o.operation_id,o.payload_json \
         FROM observations AS o \
         JOIN attempts AS a ON a.attempt_id=json_extract(o.payload_json,'$.identity.attempt_id') \
         JOIN tasks AS t ON t.task_id=a.task_id \
         WHERE o.source_stream_id=?1 AND o.kind='review.disposition' \
           AND json_extract(o.payload_json,'$.decided_by')=?2 \
           AND json_extract(o.payload_json,'$.disposition')='return_for_correction' \
           AND a.owner_id=?2 AND a.binding_id=?3 AND a.binding_generation=?4 \
           AND a.state='needs_correction' AND a.released_at_ms IS NULL \
           AND t.state='open' AND t.revision=a.task_revision \
           AND t.current_attempt_id=a.attempt_id \
         ORDER BY o.observation_id DESC LIMIT 64",
    )?;
    let rows = statement
        .query_map(
            params![
                REVIEW_STREAM,
                principal.client_id,
                binding_id,
                binding_generation
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);

    let mut matched = None;
    for (event_key, disposition_operation_id, payload_json) in rows {
        let Some(disposition_operation_id) = disposition_operation_id else {
            return Err(source_gap(
                "manager disposition observation has no Operation identity",
            ));
        };
        let disposition: Value = serde_json::from_str(&payload_json)
            .map_err(|_| source_gap("manager disposition payload is invalid"))?;
        let assignment_id = disposition["review_assignment_id"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| source_gap("manager disposition has no assignment identity"))?;
        if event_key != format!("disposition:{assignment_id}")
            || disposition["schema_version"] != 1
            || disposition["kind"] != "review.disposition"
            || disposition["operation_id"] != disposition_operation_id
            || disposition["decided_by"] != principal.client_id
            || disposition["disposition"] != "return_for_correction"
        {
            return Err(source_gap("manager disposition identity is inconsistent"));
        }
        let Some(finding_id) = disposition["finding_ids"]
            .as_array()
            .filter(|findings| findings.len() == 1)
            .and_then(|findings| findings[0].as_str())
        else {
            continue;
        };
        let identity: ReviewSlotIdentity = serde_json::from_value(disposition["identity"].clone())
            .map_err(|_| source_gap("manager disposition review identity is invalid"))?;
        let feedback_operation_id = disposition["task_feedback_operation_id"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| source_gap("manager disposition has no feedback Operation identity"))?;
        let feedback_observation_id = observation_id_for_feedback(db, feedback_operation_id)?;
        let provenance = match reviews::actionable_finding(
            db,
            &identity.task_id,
            &identity.attempt_id,
            identity.task_revision,
            &identity.submission_ref,
            &identity.candidate_ref,
            finding_id,
        ) {
            Ok(provenance) => provenance,
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "REVIEW_FINDING_NOT_FOUND"
                        | "REVIEW_FINDING_NOT_ACTIONABLE"
                        | "REVIEW_ANCHOR_MISMATCH"
                ) =>
            {
                continue;
            }
            Err(error) => return Err(error),
        };
        if provenance["review_assignment_id"] != assignment_id
            || provenance["review_operation_id"] != disposition["review_result_operation_id"]
            || provenance["identity"] != json!(identity)
            || provenance["finding"]["finding_id"] != finding_id
        {
            return Err(source_gap(
                "current actionable finding differs from retained disposition",
            ));
        }
        let finding: ReviewFinding = serde_json::from_value(provenance["finding"].clone())
            .map_err(|_| source_gap("actionable finding fields are invalid"))?;
        crate::automation::repair::validate_committed_review_and_feedback(
            db,
            &principal.client_id,
            &identity,
            assignment_id,
            model::text(&disposition, "review_result_operation_id")?,
            &disposition_operation_id,
            feedback_operation_id,
            feedback_observation_id,
            &finding,
        )?;
        crate::automation::repair::require_current_manual_owner(
            db,
            &principal.client_id,
            &identity,
            binding_id,
            binding_generation,
        )?;
        let semantic_slot_id = crate::automation::repair::semantic_slot_id(
            &principal.client_id,
            &identity,
            finding_id,
        )?;
        let request_id = model::text(request, "client_request_id")?;
        let expected = json!({
            "client_request_id":request_id,
            "binding_id":binding_id,
            "generation":binding_generation,
            "delivery":"next_turn",
            "text":crate::automation::repair::render_correction_text(&identity, &finding)
        });
        if model::canonical(request)? != model::canonical(&expected)? {
            continue;
        }
        let request_digest = repair_request_digest(request)?;
        let candidate = DirectRepairSlot {
            manager_id: principal.client_id.clone(),
            identity,
            finding,
            assignment_id: assignment_id.to_owned(),
            result_operation_id: model::text(&disposition, "review_result_operation_id")?
                .to_owned(),
            disposition_operation_id,
            feedback_operation_id: feedback_operation_id.to_owned(),
            feedback_observation_id,
            binding_id: binding_id.to_owned(),
            binding_generation,
            semantic_slot_id,
            request: request.clone(),
            request_digest,
        };
        if matched.is_some() {
            return Err(Error::new(
                "REPAIR_SLOT_AMBIGUOUS",
                "direct correction request matches more than one retained disposition",
            ));
        }
        matched = Some(candidate);
    }
    Ok(matched)
}

pub(crate) fn resolve_direct_slot(
    db: &Connection,
    slot: &DirectRepairSlot,
) -> Result<RepairSlotResolution> {
    resolve_slot_parts(
        db,
        &slot.manager_id,
        &slot.identity,
        &slot.finding.finding_id,
        &slot.binding_id,
        slot.binding_generation,
        &slot.semantic_slot_id,
        &slot.request,
        &slot.request_digest,
    )
}

/// Resolve one durable manager/Task/finding slot before Operation creation.
/// Direct owner `agent.send` and selected automation must both call this
/// helper when the send is the exact retained correction request.
pub(crate) fn resolve_semantic_slot(
    db: &Connection,
    prepared: &PreparedRepairDispatch,
) -> Result<RepairSlotResolution> {
    resolve_slot_parts(
        db,
        prepared.context.effective_manager_id(),
        prepared.context.identity(),
        &prepared.context.finding().finding_id,
        prepared.context.binding_id(),
        prepared.context.binding_generation(),
        prepared.context.semantic_slot_id(),
        &prepared.request_value(),
        &prepared.request.parameters_digest()?,
    )
}

#[allow(clippy::too_many_arguments)] // The resolver checks one exact slot and its immutable Task, finding, binding, and request evidence.
fn resolve_slot_parts(
    db: &Connection,
    manager_id: &str,
    identity: &ReviewSlotIdentity,
    finding_id: &str,
    expected_binding_id: &str,
    binding_generation: i64,
    semantic_slot_id: &str,
    request: &Value,
    parameters_digest: &str,
) -> Result<RepairSlotResolution> {
    let key = slot_key(semantic_slot_id);
    let Some(value) = config::read_record(db, &key, "RepairDispatch semantic slot")? else {
        return Ok(RepairSlotResolution::Vacant);
    };
    let receipt: RepairSlotReceipt = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "REPAIR_SLOT_CORRUPT",
            "retained RepairDispatch semantic slot fields are invalid",
        )
    })?;
    if receipt.schema_version != SLOT_SCHEMA_VERSION
        || receipt.semantic_slot_id != semantic_slot_id
        || receipt.effective_manager_id != manager_id
        || receipt.task_id != identity.task_id
        || receipt.task_revision != identity.task_revision
        || receipt.attempt_id != identity.attempt_id
        || receipt.submission_ref != identity.submission_ref
        || receipt.candidate_ref != identity.candidate_ref
        || receipt.finding_id != finding_id
    {
        return Err(Error::new(
            "REPAIR_SLOT_CORRUPT",
            "retained RepairDispatch slot belongs to another semantic subject",
        ));
    }
    let (caller, method, state, original, task_id, attempt_id, actual_binding_id, generation) =
        read_repair_operation(db, &receipt.operation_id)?;
    let client_request_id: String = db.query_row(
        "SELECT client_request_id FROM operations WHERE operation_id=?1",
        [&receipt.operation_id],
        |row| row.get(0),
    )?;
    let original_value: Value = serde_json::from_str(&original).map_err(|_| {
        Error::new(
            "REPAIR_OPERATION_CORRUPT",
            "repair Operation request is invalid",
        )
    })?;
    let caller_is_automation =
        caller == crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID;
    let caller_is_manager = caller == manager_id;
    let original_request_id = original_value
        .get("client_request_id")
        .and_then(Value::as_str);
    let request_id_matches = if caller_is_automation {
        client_request_id == semantic_slot_id && original_request_id == Some(semantic_slot_id)
    } else if caller_is_manager {
        !client_request_id.is_empty() && original_request_id == Some(client_request_id.as_str())
    } else {
        false
    };
    let normalized_original = normalized_repair_request(&original_value, semantic_slot_id)
        .ok_or_else(|| {
            Error::new(
                "REPAIR_OPERATION_CORRUPT",
                "repair request shape is invalid",
            )
        })?;
    let normalized_request =
        normalized_repair_request(request, semantic_slot_id).ok_or_else(|| {
            Error::new(
                "REPAIR_OPERATION_CORRUPT",
                "repair slot request shape is invalid",
            )
        })?;
    if method != "agent.send"
        || !request_id_matches
        || task_id.as_deref() != Some(identity.task_id.as_str())
        || attempt_id.as_deref() != Some(identity.attempt_id.as_str())
        || actual_binding_id.as_deref() != Some(expected_binding_id)
        || generation != Some(binding_generation)
        || model::canonical(&normalized_original)? != model::canonical(&normalized_request)?
        || repair_request_digest(&original_value)? != receipt.parameters_digest
    {
        return Err(Error::new(
            "REPAIR_SLOT_CORRUPT",
            "retained repair slot does not match its immutable delivery Operation",
        ));
    }
    if caller_is_automation {
        let link = operation_link(db, &receipt.operation_id)?.ok_or_else(|| {
            Error::new(
                "REPAIR_LINK_CORRUPT",
                "automated correction Operation has no validated on-behalf link",
            )
        })?;
        if link.effective_manager_id != manager_id || link.semantic_slot_id != semantic_slot_id {
            return Err(Error::new(
                "REPAIR_LINK_CORRUPT",
                "automated correction link belongs to another Manager or semantic slot",
            ));
        }
    }
    if receipt.parameters_digest == parameters_digest {
        Ok(RepairSlotResolution::Existing {
            operation_id: receipt.operation_id,
            operation_state: state,
        })
    } else {
        Ok(RepairSlotResolution::Conflict {
            operation_id: receipt.operation_id,
            operation_state: state,
        })
    }
}

fn repair_request_digest(request: &Value) -> Result<String> {
    let delivery = RepairDeliveryRequest {
        client_request_id: model::text(request, "client_request_id")?.to_owned(),
        binding_id: model::text(request, "binding_id")?.to_owned(),
        generation: model::positive(request, "generation")?,
        text: model::text(request, "text")?.to_owned(),
    };
    if model::text(request, "delivery")? != "next_turn" {
        return Err(Error::invalid(
            "repair slot accepts only next_turn delivery",
        ));
    }
    delivery.parameters_digest()
}

fn normalized_repair_request(request: &Value, semantic_slot_id: &str) -> Option<Value> {
    let mut object = request.as_object()?.clone();
    if !object
        .get("client_request_id")
        .is_some_and(Value::is_string)
    {
        return None;
    }
    object.insert("client_request_id".to_owned(), json!(semantic_slot_id));
    Some(Value::Object(object))
}

/// Retain the automated on-behalf Operation link and semantic slot after the
/// shared runtime admission has created its ordinary queued `agent.send` row.
pub(crate) fn retain_admission(
    tx: &Transaction<'_>,
    prepared: &PreparedRepairDispatch,
    operation_id: &str,
    now_ms: i64,
) -> Result<RepairDispatchOperationLink> {
    validate_text(operation_id, "operation_id")?;
    prepared.context.require_current_for_admission(tx)?;
    let request = prepared.request_value();
    let parameters_digest = prepared.request.parameters_digest()?;
    let (caller, method, state, original, task_id, attempt_id, binding_id, generation) =
        read_repair_operation(tx, operation_id)?;
    if caller != prepared.context.technical_requester_id()
        || method != "agent.send"
        || state != "queued"
        || original != model::canonical(&request)?
        || task_id.as_deref() != Some(prepared.context.identity().task_id.as_str())
        || attempt_id.as_deref() != Some(prepared.context.identity().attempt_id.as_str())
        || binding_id.as_deref() != Some(prepared.context.binding_id())
        || generation != Some(prepared.context.binding_generation())
    {
        return Err(Error::new(
            "REPAIR_OPERATION_MISMATCH",
            "runtime admission did not retain the exact queued correction Operation",
        ));
    }
    let link = link_from_context(prepared, operation_id, &parameters_digest, now_ms);
    write_operation_link(tx, &link)?;
    retain_slot(
        tx,
        prepared.context(),
        operation_id,
        &parameters_digest,
        now_ms,
    )?;
    Ok(link)
}

/// Retain the same semantic slot for a direct owner correction Operation.
/// The caller must already have passed the normal authenticated `agent.send`
/// owner checks; this helper only ties it to the retained feedback identity.
pub(crate) fn retain_direct_admission(
    tx: &Transaction<'_>,
    principal: &Principal,
    slot: &DirectRepairSlot,
    operation_id: &str,
    now_ms: i64,
) -> Result<()> {
    if principal.role != Role::Manager || principal.client_id != slot.manager_id {
        return Err(Error::new(
            "FORBIDDEN",
            "direct repair slot can only be retained for its authenticated Manager owner",
        ));
    }
    crate::automation::repair::validate_committed_review_and_feedback(
        tx,
        &slot.manager_id,
        &slot.identity,
        &slot.assignment_id,
        &slot.result_operation_id,
        &slot.disposition_operation_id,
        &slot.feedback_operation_id,
        slot.feedback_observation_id,
        &slot.finding,
    )?;
    crate::automation::repair::require_current_manual_owner(
        tx,
        &slot.manager_id,
        &slot.identity,
        &slot.binding_id,
        slot.binding_generation,
    )?;
    let (caller, method, state, original, task_id, attempt_id, binding_id, generation) =
        read_repair_operation(tx, operation_id)?;
    let client_request_id: String = tx.query_row(
        "SELECT client_request_id FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| row.get(0),
    )?;
    let expected_request_id = model::text(&slot.request, "client_request_id")?;
    if caller != slot.manager_id
        || method != "agent.send"
        || state != "queued"
        || client_request_id != expected_request_id
        || original != model::canonical(&slot.request)?
        || task_id
            .as_deref()
            .is_some_and(|value| value != slot.identity.task_id.as_str())
        || attempt_id
            .as_deref()
            .is_some_and(|value| value != slot.identity.attempt_id.as_str())
        || binding_id.as_deref() != Some(slot.binding_id.as_str())
        || generation != Some(slot.binding_generation)
    {
        return Err(Error::new(
            "REPAIR_OPERATION_MISMATCH",
            "direct correction Operation differs from the exact authenticated repair request",
        ));
    }
    match resolve_direct_slot(tx, slot)? {
        RepairSlotResolution::Vacant => {}
        RepairSlotResolution::Existing {
            operation_id: existing,
            ..
        } if existing == operation_id => {}
        RepairSlotResolution::Existing { .. } | RepairSlotResolution::Conflict { .. } => {
            return Err(Error::new(
                "REPAIR_SLOT_CONFLICT",
                "another exact correction Operation already occupies this semantic slot",
            ));
        }
    }
    let changed = tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1 \
         AND (task_id IS NULL OR task_id=?2) AND (attempt_id IS NULL OR attempt_id=?3)",
        params![
            operation_id,
            slot.identity.task_id,
            slot.identity.attempt_id
        ],
    )?;
    if changed != 1 {
        return Err(Error::new(
            "REPAIR_OPERATION_MISMATCH",
            "direct correction Operation already names another Task or Attempt",
        ));
    }
    retain_slot_parts(
        tx,
        &slot.manager_id,
        &slot.identity,
        &slot.finding.finding_id,
        &slot.semantic_slot_id,
        operation_id,
        &slot.request_digest,
        now_ms,
    )
}

fn retain_slot(
    tx: &Transaction<'_>,
    context: &RepairDispatchContext,
    operation_id: &str,
    parameters_digest: &str,
    now_ms: i64,
) -> Result<()> {
    retain_slot_parts(
        tx,
        context.effective_manager_id(),
        context.identity(),
        &context.finding().finding_id,
        context.semantic_slot_id(),
        operation_id,
        parameters_digest,
        now_ms,
    )
}

#[allow(clippy::too_many_arguments)] // Persisting the slot keeps each validated semantic subject field explicit.
fn retain_slot_parts(
    tx: &Transaction<'_>,
    manager_id: &str,
    identity: &ReviewSlotIdentity,
    finding_id: &str,
    semantic_slot_id: &str,
    operation_id: &str,
    parameters_digest: &str,
    now_ms: i64,
) -> Result<()> {
    let key = slot_key(semantic_slot_id);
    if let Some(value) = config::read_record(tx, &key, "RepairDispatch semantic slot")? {
        let existing: RepairSlotReceipt = serde_json::from_value(value).map_err(|_| {
            Error::new(
                "REPAIR_SLOT_CORRUPT",
                "retained repair slot fields are invalid",
            )
        })?;
        if existing.operation_id == operation_id
            && existing.parameters_digest == parameters_digest
            && existing.semantic_slot_id == semantic_slot_id
        {
            return Ok(());
        }
        return Err(Error::new(
            "REPAIR_SLOT_CONFLICT",
            "another correction Operation already occupies this semantic slot",
        ));
    }
    let receipt = RepairSlotReceipt {
        schema_version: SLOT_SCHEMA_VERSION,
        semantic_slot_id: semantic_slot_id.to_owned(),
        effective_manager_id: manager_id.to_owned(),
        task_id: identity.task_id.clone(),
        task_revision: identity.task_revision,
        attempt_id: identity.attempt_id.clone(),
        submission_ref: identity.submission_ref.clone(),
        candidate_ref: identity.candidate_ref.clone(),
        finding_id: finding_id.to_owned(),
        parameters_digest: parameters_digest.to_owned(),
        operation_id: operation_id.to_owned(),
        reserved_at_ms: now_ms,
    };
    config::write_record(
        tx,
        &slot_key(semantic_slot_id),
        &serde_json::to_value(receipt)?,
    )
}

fn link_from_context(
    prepared: &PreparedRepairDispatch,
    operation_id: &str,
    request_digest: &str,
    linked_at_ms: i64,
) -> RepairDispatchOperationLink {
    let context = prepared.context();
    RepairDispatchOperationLink {
        schema_version: LINK_SCHEMA_VERSION,
        operation_id: operation_id.to_owned(),
        technical_requester_id: context.technical_requester_id().to_owned(),
        effective_manager_id: context.effective_manager_id().to_owned(),
        automation_id: context.automation_id().to_owned(),
        automation_revision: context.automation_revision(),
        project_id: context.project_id().to_owned(),
        action: "agent.send".to_owned(),
        semantic_cause_kind: "review_disposition".to_owned(),
        semantic_cause_id: context.review_assignment_id().to_owned(),
        semantic_slot_id: context.semantic_slot_id().to_owned(),
        task_id: context.identity().task_id.clone(),
        task_revision: context.identity().task_revision,
        attempt_id: context.identity().attempt_id.clone(),
        submission_ref: context.identity().submission_ref.clone(),
        candidate_ref: context.identity().candidate_ref.clone(),
        finding_id: context.finding().finding_id.clone(),
        review_assignment_id: context.review_assignment_id().to_owned(),
        review_result_operation_id: context.review_result_operation_id().to_owned(),
        disposition_operation_id: context.disposition_operation_id().to_owned(),
        feedback_operation_id: context.feedback_operation_id().to_owned(),
        feedback_observation_id: context.feedback_observation_id(),
        binding_id: context.binding_id().to_owned(),
        binding_generation: context.binding_generation(),
        request_digest: request_digest.to_owned(),
        identity: context.identity().clone(),
        finding: context.finding().clone(),
        cause: context.cause_value(),
        linked_at_ms,
    }
}

fn write_operation_link(tx: &Transaction<'_>, link: &RepairDispatchOperationLink) -> Result<()> {
    let operation_key = operation_link_key(&link.operation_id)?;
    if config::read_record(tx, &operation_key, "RepairDispatch operation link")?.is_some() {
        return Err(Error::new(
            "REPAIR_LINK_CORRUPT",
            "delivery Operation already has RepairDispatch attribution",
        ));
    }
    let value = serde_json::to_value(link)?;
    config::write_record(tx, &operation_key, &value)?;
    let index_key = entry_operation_key(
        &link.effective_manager_id,
        &link.project_id,
        &link.automation_id,
        &link.operation_id,
    )?;
    config::write_record(tx, &index_key, &value)
}

/// Load and fully validate one repair Operation link. This is used by
/// operation visibility and the stage-aware runtime actor reconstruction.
pub(crate) fn operation_link(
    db: &Connection,
    operation_id: &str,
) -> Result<Option<RepairDispatchOperationLink>> {
    let key = operation_link_key(operation_id)?;
    let Some(value) = config::read_record(db, &key, "RepairDispatch operation link")? else {
        return Ok(None);
    };
    let link: RepairDispatchOperationLink = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "REPAIR_LINK_CORRUPT",
            "RepairDispatch link fields are invalid",
        )
    })?;
    validate_operation_link(db, &link, operation_id)?;
    Ok(Some(link))
}

/// Rehydrate the internal context from the exact retained delivery Operation.
/// It validates immutable provenance only; callers must invoke the appropriate
/// current-action stage guard before new effects.
pub(crate) fn context_for_delivery_operation(
    db: &Connection,
    operation_id: &str,
) -> Result<RepairDispatchContext> {
    let link = operation_link(db, operation_id)?.ok_or_else(|| {
        Error::new(
            "REPAIR_LINK_NOT_FOUND",
            "delivery Operation has no retained RepairDispatch attribution",
        )
    })?;
    RepairDispatchContext::from_retained_link(
        db,
        &link.effective_manager_id,
        &link.automation_id,
        link.automation_revision,
        &link.project_id,
        &link.review_assignment_id,
        &link.review_result_operation_id,
        &link.disposition_operation_id,
        &link.feedback_operation_id,
        link.feedback_observation_id,
        link.identity,
        link.finding,
        &link.binding_id,
        link.binding_generation,
        &link.semantic_slot_id,
    )
}

fn validate_operation_link(
    db: &Connection,
    link: &RepairDispatchOperationLink,
    operation_id: &str,
) -> Result<()> {
    let corrupt = || {
        Error::new(
            "REPAIR_LINK_CORRUPT",
            "RepairDispatch link does not match its Operation and source facts",
        )
    };
    if link.schema_version != LINK_SCHEMA_VERSION
        || link.operation_id != operation_id
        || link.technical_requester_id
            != crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
        || link.action != "agent.send"
        || link.semantic_cause_kind != "review_disposition"
        || link.automation_revision <= 0
        || link.binding_generation <= 0
        || link.identity.task_id != link.task_id
        || link.identity.task_revision != link.task_revision
        || link.identity.attempt_id != link.attempt_id
        || link.identity.submission_ref != link.submission_ref
        || link.identity.candidate_ref != link.candidate_ref
        || link.finding.finding_id != link.finding_id
    {
        return Err(corrupt());
    }
    let context = RepairDispatchContext::from_retained_link(
        db,
        &link.effective_manager_id,
        &link.automation_id,
        link.automation_revision,
        &link.project_id,
        &link.review_assignment_id,
        &link.review_result_operation_id,
        &link.disposition_operation_id,
        &link.feedback_operation_id,
        link.feedback_observation_id,
        link.identity.clone(),
        link.finding.clone(),
        &link.binding_id,
        link.binding_generation,
        &link.semantic_slot_id,
    )
    .map_err(|_| corrupt())?;
    if link.semantic_cause_id != context.review_assignment_id()
        || link.cause != context.cause_value()
    {
        return Err(corrupt());
    }
    let (caller, method, _state, original, task, attempt, binding, generation) =
        read_repair_operation(db, operation_id).map_err(|_| corrupt())?;
    let client_request_id: String = db
        .query_row(
            "SELECT client_request_id FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| row.get(0),
        )
        .map_err(|_| corrupt())?;
    let request = context.delivery_request().map_err(|_| corrupt())?;
    if caller != link.technical_requester_id
        || method != "agent.send"
        || client_request_id != link.semantic_slot_id
        || original != model::canonical(&request.value()).map_err(|_| corrupt())?
        || task.as_deref() != Some(link.task_id.as_str())
        || attempt.as_deref() != Some(link.attempt_id.as_str())
        || binding.as_deref() != Some(link.binding_id.as_str())
        || generation != Some(link.binding_generation)
        || request.parameters_digest().map_err(|_| corrupt())? != link.request_digest
    {
        return Err(corrupt());
    }
    validate_slot_for_link(db, link).map_err(|_| corrupt())
}

fn validate_slot_for_link(db: &Connection, link: &RepairDispatchOperationLink) -> Result<()> {
    let value = config::read_record(
        db,
        &slot_key(&link.semantic_slot_id),
        "RepairDispatch semantic slot",
    )?
    .ok_or_else(|| {
        Error::new(
            "REPAIR_SLOT_CORRUPT",
            "RepairDispatch link has no semantic slot",
        )
    })?;
    let slot: RepairSlotReceipt = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "REPAIR_SLOT_CORRUPT",
            "RepairDispatch slot fields are invalid",
        )
    })?;
    if slot.schema_version != SLOT_SCHEMA_VERSION
        || slot.semantic_slot_id != link.semantic_slot_id
        || slot.effective_manager_id != link.effective_manager_id
        || slot.task_id != link.task_id
        || slot.task_revision != link.task_revision
        || slot.attempt_id != link.attempt_id
        || slot.submission_ref != link.submission_ref
        || slot.candidate_ref != link.candidate_ref
        || slot.finding_id != link.finding_id
        || slot.operation_id != link.operation_id
        || slot.parameters_digest != link.request_digest
    {
        return Err(Error::new(
            "REPAIR_SLOT_CORRUPT",
            "RepairDispatch slot differs from its retained Operation link",
        ));
    }
    Ok(())
}

pub(crate) fn entry_operation_links(
    db: &Connection,
    manager_id: &str,
    project_id: &str,
    automation_id: &str,
    after_operation_id: &str,
    limit: usize,
) -> Result<Vec<RepairDispatchOperationLink>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let prefix = entry_operation_prefix(manager_id, project_id, automation_id)?;
    let pattern = format!("{prefix}%");
    let after = format!("{prefix}{after_operation_id}");
    let mut statement =
        db.prepare("SELECT key FROM meta WHERE key LIKE ?1 AND key>?2 ORDER BY key LIMIT ?3")?;
    let keys = statement
        .query_map(params![pattern, after, limit.min(128) as i64], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);
    let mut links = Vec::with_capacity(keys.len());
    for key in keys {
        let value = config::read_record(db, &key, "RepairDispatch entry Operation link")?
            .ok_or_else(|| Error::new("REPAIR_LINK_CORRUPT", "entry link index is missing"))?;
        let link: RepairDispatchOperationLink = serde_json::from_value(value).map_err(|_| {
            Error::new("REPAIR_LINK_CORRUPT", "entry link index fields are invalid")
        })?;
        if link.effective_manager_id != manager_id
            || link.project_id != project_id
            || link.automation_id != automation_id
        {
            return Err(Error::new(
                "REPAIR_LINK_CORRUPT",
                "entry link index crossed its manager/project/automation scope",
            ));
        }
        validate_operation_link(db, &link, &link.operation_id)?;
        links.push(link);
    }
    Ok(links)
}

fn read_repair_operation(db: &Connection, operation_id: &str) -> Result<RepairOperationRow> {
    db.query_row(
        "SELECT caller_id,method,state,original_request_json,task_id,attempt_id,binding_id,binding_generation \
         FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| {
            Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?))
        },
    )
    .map_err(Into::into)
}

fn delivery_state(
    db: &Connection,
    operation_id: &str,
    prepared: &PreparedRepairDispatch,
) -> Result<DeliveryState> {
    let (caller, method, state, original, task_id, attempt_id, binding_id, generation) =
        read_repair_operation(db, operation_id)?;
    let client_request_id: String = db.query_row(
        "SELECT client_request_id FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| row.get(0),
    )?;
    if method != "agent.send"
        || task_id.as_deref() != Some(prepared.context.identity().task_id.as_str())
        || attempt_id.as_deref() != Some(prepared.context.identity().attempt_id.as_str())
        || binding_id.as_deref() != Some(prepared.context.binding_id())
        || generation != Some(prepared.context.binding_generation())
    {
        return Err(Error::new(
            "REPAIR_OPERATION_CORRUPT",
            "retained delivery Operation is outside its exact Attempt and binding",
        ));
    }
    let request: Value = serde_json::from_str(&original).map_err(|_| {
        Error::new(
            "REPAIR_OPERATION_CORRUPT",
            "retained delivery request is invalid",
        )
    })?;
    let expected = prepared.request_value();
    let semantic_slot_id = prepared.context.semantic_slot_id();
    let caller_is_automation = caller == prepared.context.technical_requester_id();
    let caller_is_manager = caller == prepared.context.effective_manager_id();
    let original_request_id = request.get("client_request_id").and_then(Value::as_str);
    let request_id_matches = if caller_is_automation {
        client_request_id == semantic_slot_id && original_request_id == Some(semantic_slot_id)
    } else if caller_is_manager {
        !client_request_id.is_empty() && original_request_id == Some(client_request_id.as_str())
    } else {
        false
    };
    let normalized_request = normalized_repair_request(&request, semantic_slot_id);
    let normalized_expected = normalized_repair_request(&expected, semantic_slot_id);
    let exact_request = matches!(
        (normalized_request, normalized_expected),
        (Some(request), Some(expected)) if request == expected
    );
    if !request_id_matches || !exact_request {
        return Err(Error::new(
            "REPAIR_OPERATION_CORRUPT",
            "retained delivery Operation has a different owner or exact correction request",
        ));
    }
    let result_json: Option<String> = db
        .query_row(
            "SELECT result_json FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| row.get(0),
        )
        .optional()?;
    match state.as_str() {
        "queued" | "sending" | "native_accepted" | "outcome_unknown" => Ok(DeliveryState::Pending(
            "correction_operation_requires_readback".to_owned(),
        )),
        "settled" => {
            let result: Value = serde_json::from_str(&result_json.ok_or_else(|| {
                Error::new(
                    "REPAIR_OPERATION_CORRUPT",
                    "settled correction Operation has no result",
                )
            })?)?;
            if result["outcome"] == "applied"
                && result["native_input_id"]
                    .as_str()
                    .is_some_and(|value| !value.is_empty())
                && result["details"]["completion_condition"] == "native_input_admitted"
            {
                Ok(DeliveryState::Verified)
            } else {
                Ok(DeliveryState::Pending(
                    "native_delivery_receipt_is_not_sufficiently_proved".to_owned(),
                ))
            }
        }
        "rejected" | "cancelled" => Ok(DeliveryState::NoEffect),
        _ => Err(Error::new(
            "REPAIR_OPERATION_CORRUPT",
            "correction Operation has an unsupported lifecycle state",
        )),
    }
}

enum DeliveryState {
    Verified,
    Pending(String),
    NoEffect,
}

fn observation_id_for_feedback(db: &Connection, feedback_operation_id: &str) -> Result<i64> {
    db.query_row(
        "SELECT observation_id FROM observations WHERE source_stream_id=?1 AND operation_id=?2 AND kind='task.feedback' ORDER BY observation_id DESC LIMIT 1",
        params![REVIEW_STREAM, feedback_operation_id],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| source_gap("applied feedback has no retained feedback observation"))
}

fn slot_key(slot_id: &str) -> String {
    format!("{SLOT_PREFIX}{slot_id}")
}

fn operation_link_key(operation_id: &str) -> Result<String> {
    validate_text(operation_id, "operation_id")?;
    Ok(format!("{OPERATION_LINK_PREFIX}{operation_id}"))
}

fn entry_operation_prefix(
    manager_id: &str,
    project_id: &str,
    automation_id: &str,
) -> Result<String> {
    config::validate_automation_id(automation_id)?;
    Ok(format!(
        "{ENTRY_LINK_PREFIX}{}:{automation_id}:",
        config::scope_digest(manager_id, project_id)?
    ))
}

fn entry_operation_key(
    manager_id: &str,
    project_id: &str,
    automation_id: &str,
    operation_id: &str,
) -> Result<String> {
    validate_text(operation_id, "operation_id")?;
    Ok(format!(
        "{}{operation_id}",
        entry_operation_prefix(manager_id, project_id, automation_id)?
    ))
}

fn source_gap(message: &str) -> Error {
    Error::new("AUTOMATION_REPAIR_SOURCE_GAP", message)
}

fn validate_text(value: &str, field: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(Error::invalid(format!("{field} is invalid")));
    }
    Ok(())
}
