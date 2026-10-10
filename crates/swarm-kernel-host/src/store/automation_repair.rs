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
        repair::{RepairDeliveryRequest, RepairDispatchContext, historical_singleton_text},
    },
    error::{Error, Result},
    model,
    model::{Principal, Role},
    review::{ReviewFinding, ReviewFindingsPackage, ReviewSlotIdentity},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const LEGACY_REPAIR_SCHEMA_VERSION: u32 = 1;
const SLOT_SCHEMA_VERSION: u32 = 2;
const LINK_SCHEMA_VERSION: u32 = 2;
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    finding_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    findings_digest: Option<String>,
    parameters_digest: String,
    operation_id: String,
    reserved_at_ms: i64,
    #[serde(default)]
    source_attempt_owner_id: Option<String>,
    #[serde(default)]
    review_assignment_sponsor_id: Option<String>,
    #[serde(default)]
    decision_manager_id: Option<String>,
    #[serde(default)]
    transfer_operation_ids: Option<Vec<String>>,
    #[serde(default)]
    captured_transfer_gm_epoch: Option<i64>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) finding_id: Option<String>,
    pub(crate) review_assignment_id: String,
    pub(crate) review_result_operation_id: String,
    pub(crate) disposition_operation_id: String,
    pub(crate) feedback_operation_id: String,
    pub(crate) feedback_observation_id: i64,
    pub(crate) binding_id: String,
    pub(crate) binding_generation: i64,
    pub(crate) request_digest: String,
    /// Legacy v1 links serialized a duplicate top-level identity. Decode it
    /// only to validate against `cause.identity`; new links keep the existing
    /// cause/package identity sources and do not serialize a second copy.
    #[serde(default, skip_serializing, rename = "identity")]
    legacy_identity: Option<ReviewSlotIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) findings_package: Option<ReviewFindingsPackage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) finding: Option<ReviewFinding>,
    pub(crate) cause: Value,
    pub(crate) linked_at_ms: i64,
    #[serde(default)]
    pub(crate) source_attempt_owner_id: Option<String>,
    #[serde(default)]
    pub(crate) review_assignment_sponsor_id: Option<String>,
    #[serde(default)]
    pub(crate) decision_manager_id: Option<String>,
    #[serde(default)]
    pub(crate) transfer_operation_ids: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) captured_transfer_gm_epoch: Option<i64>,
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
    findings_package: ReviewFindingsPackage,
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

#[derive(Debug, Clone)]
struct RepairSourceFacts {
    source_attempt_owner_id: String,
    review_assignment_sponsor_id: String,
    decision_manager_id: String,
    transfer_operation_ids: Vec<String>,
    captured_transfer_gm_epoch: Option<i64>,
}

impl RepairSourceFacts {
    fn from_context(context: &RepairDispatchContext) -> Self {
        Self {
            source_attempt_owner_id: context.source_attempt_owner_id().to_owned(),
            review_assignment_sponsor_id: context.review_assignment_sponsor_id().to_owned(),
            decision_manager_id: context.decision_manager_id().to_owned(),
            transfer_operation_ids: context.transfer_operation_ids().to_vec(),
            captured_transfer_gm_epoch: context.captured_transfer_gm_epoch(),
        }
    }
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
    let decision_manager_id = disposition["decided_by"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| source_gap("review disposition has no decision manager identity"))?;
    let finding_ids = disposition["finding_ids"]
        .as_array()
        .ok_or_else(|| source_gap("review disposition has no selected finding list"))?;
    if finding_ids.is_empty() {
        return Err(source_gap(
            "review disposition has an empty findings package",
        ));
    }
    let ordered_finding_ids = finding_ids
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| source_gap("review disposition finding identity is invalid"))
        })
        .collect::<Result<Vec<_>>>()?;
    let feedback_operation_id = disposition["task_feedback_operation_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| source_gap("review disposition has no feedback Operation identity"))?;
    let feedback_observation_id = observation_id_for_feedback(tx, feedback_operation_id)?;
    let mut ordered_findings = Vec::with_capacity(ordered_finding_ids.len());
    for finding_id in &ordered_finding_ids {
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
            || provenance["finding"]["finding_id"].as_str() != Some(finding_id.as_str())
        {
            return Err(source_gap(
                "actionable finding differs from the exact review result",
            ));
        }
        ordered_findings.push(
            serde_json::from_value::<ReviewFinding>(provenance["finding"].clone())
                .map_err(|_| source_gap("actionable review finding fields are invalid"))?,
        );
    }
    let findings_package = ReviewFindingsPackage::new(
        review.identity.clone(),
        assignment_id.to_owned(),
        result_operation_id.to_owned(),
        ordered_findings,
    )
    .map_err(|_| source_gap("ordered committed findings package is invalid"))?;
    if findings_package.finding_ids() != ordered_finding_ids {
        return Err(source_gap(
            "review disposition changed the immutable finding order",
        ));
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
        findings_package,
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
    if prepared.context.review_assignment_sponsor_id() != review_sponsor
        || prepared.context.decision_manager_id() != decision_manager_id
    {
        return Err(source_gap(
            "repair context differs from the exact saved sponsor or disposition actor",
        ));
    }
    if prepared
        .context
        .prior_effect_manager_ids()
        .iter()
        .any(|owner| owner.as_str() != prepared.context.effective_manager_id())
        && let Some(result) = consume_transferred_repair_slot(tx, entry, &prepared)?
    {
        return Ok(result);
    }
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

fn consume_transferred_repair_slot(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    current: &PreparedRepairDispatch,
) -> Result<Option<Value>> {
    let current_context = current.context();
    let assignment_id = current_context.review_assignment_id();
    let result_operation_id = current_context.review_result_operation_id();
    let prior_owners = current_context
        .prior_effect_manager_ids()
        .iter()
        .filter(|owner| owner.as_str() != current_context.effective_manager_id());
    let mut prior_slot: Option<(String, String, RepairSlotReceipt)> = None;
    for owner_id in prior_owners {
        let candidates = semantic_slot_id_candidates(
            owner_id,
            current_context.identity(),
            current_context.findings_package(),
        )?;
        let mut owner_slot: Option<(String, RepairSlotReceipt, RepairSlotResolution)> = None;
        for semantic_slot_id in candidates {
            let Some(value) = config::read_record(
                tx,
                &slot_key(&semantic_slot_id),
                "historical RepairDispatch semantic slot",
            )?
            else {
                continue;
            };
            let receipt: RepairSlotReceipt = serde_json::from_value(value).map_err(|_| {
                Error::new(
                    "REPAIR_SLOT_CORRUPT",
                    "historical RepairDispatch slot fields are invalid",
                )
            })?;
            if receipt.effective_manager_id != owner_id.as_str()
                || receipt.task_id != current_context.identity().task_id
                || receipt.task_revision != current_context.identity().task_revision
                || receipt.attempt_id != current_context.identity().attempt_id
                || receipt.submission_ref != current_context.identity().submission_ref
                || receipt.candidate_ref != current_context.identity().candidate_ref
            {
                return Err(Error::new(
                    "REPAIR_SLOT_CORRUPT",
                    "historical RepairDispatch slot belongs to another semantic subject",
                ));
            }

            let request = normalized_repair_request(&current.request_value(), &semantic_slot_id)
                .ok_or_else(|| {
                    Error::new(
                        "REPAIR_OPERATION_CORRUPT",
                        "current correction request has an invalid shape",
                    )
                })?;
            let request_digest = repair_request_digest(&request)?;
            let resolution = resolve_slot_parts(
                tx,
                owner_id,
                current_context.identity(),
                current_context.findings_package(),
                current_context.binding_id(),
                current_context.binding_generation(),
                &semantic_slot_id,
                &request,
                &request_digest,
            )?;
            if resolution == RepairSlotResolution::Vacant {
                return Err(Error::new(
                    "REPAIR_SLOT_CORRUPT",
                    "historical RepairDispatch slot disappeared during readback",
                ));
            }
            if owner_slot.is_some() {
                return Err(Error::new(
                    "REPAIR_SLOT_AMBIGUOUS",
                    "both current and historical RepairDispatch slots are retained for a prior owner",
                ));
            }
            owner_slot = Some((semantic_slot_id, receipt, resolution));
        }
        let Some((semantic_slot_id, receipt, resolution)) = owner_slot else {
            continue;
        };
        match resolution {
            RepairSlotResolution::Conflict { operation_id, .. }
                if operation_id == receipt.operation_id =>
            {
                return Ok(Some(repair_skipped(
                    assignment_id,
                    result_operation_id,
                    "historical_repair_semantic_slot_conflict",
                )));
            }
            RepairSlotResolution::Existing { operation_id, .. }
                if operation_id == receipt.operation_id =>
            {
                if receipt
                    .source_attempt_owner_id
                    .as_deref()
                    .is_some_and(|source| source != current_context.source_attempt_owner_id())
                    || receipt
                        .review_assignment_sponsor_id
                        .as_deref()
                        .is_some_and(|sponsor| {
                            sponsor != current_context.review_assignment_sponsor_id()
                        })
                    || receipt
                        .decision_manager_id
                        .as_deref()
                        .is_some_and(|manager| manager != current_context.decision_manager_id())
                {
                    return Err(Error::new(
                        "REPAIR_SLOT_CORRUPT",
                        "historical RepairDispatch slot has different retained source facts",
                    ));
                }
            }
            RepairSlotResolution::Conflict { .. }
            | RepairSlotResolution::Existing { .. }
            | RepairSlotResolution::Vacant => {
                return Err(Error::new(
                    "REPAIR_SLOT_CORRUPT",
                    "historical RepairDispatch slot resolved to a different Operation",
                ));
            }
        }
        if prior_slot.is_some() {
            return Ok(Some(repair_skipped(
                assignment_id,
                result_operation_id,
                "multiple_historical_repair_operations_require_readback",
            )));
        }
        prior_slot = Some((owner_id.clone(), semantic_slot_id, receipt));
    }

    let Some((historical_owner_id, semantic_slot_id, receipt)) = prior_slot else {
        return Ok(None);
    };
    let (caller, method, operation_state, _, _, _, _, _) =
        read_repair_operation(tx, &receipt.operation_id)?;
    if method != "agent.send" {
        return Err(Error::new(
            "REPAIR_OPERATION_CORRUPT",
            "historical repair slot is linked to a non-send Operation",
        ));
    }
    if caller != crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID {
        if caller != historical_owner_id
            || current_context.decision_manager_id() != historical_owner_id
            || receipt.source_attempt_owner_id.is_some()
            || receipt.review_assignment_sponsor_id.is_some()
            || receipt.decision_manager_id.is_some()
            || receipt.transfer_operation_ids.is_some()
            || receipt.captured_transfer_gm_epoch.is_some()
        {
            return Err(Error::new(
                "REPAIR_OPERATION_CORRUPT",
                "historical direct repair Operation has the wrong decision manager",
            ));
        }
        return Ok(Some(historical_direct_repair_readback(
            tx,
            &receipt.operation_id,
            assignment_id,
            result_operation_id,
            &semantic_slot_id,
        )?));
    }

    let context = context_for_delivery_operation(tx, &receipt.operation_id)?;
    if context.effective_manager_id() != historical_owner_id
        || context.automation_id() != entry.automation_id
        || context.project_id() != entry.project_id
        || context.semantic_slot_id() != semantic_slot_id
        || context.review_assignment_id() != assignment_id
        || context.review_result_operation_id() != result_operation_id
        || context.disposition_operation_id() != current_context.disposition_operation_id()
        || context.feedback_operation_id() != current_context.feedback_operation_id()
        || context.feedback_observation_id() != current_context.feedback_observation_id()
        || context.identity() != current_context.identity()
        || context.binding_id() != current_context.binding_id()
        || context.binding_generation() != current_context.binding_generation()
        || context.source_attempt_owner_id() != current_context.source_attempt_owner_id()
        || context.review_assignment_sponsor_id() != current_context.review_assignment_sponsor_id()
        || context.decision_manager_id() != current_context.decision_manager_id()
        || receipt
            .transfer_operation_ids
            .as_ref()
            .is_some_and(|operations| operations.as_slice() != context.transfer_operation_ids())
        || receipt.captured_transfer_gm_epoch != context.captured_transfer_gm_epoch()
        || serde_json::to_value(context.findings_package())?
            != serde_json::to_value(current_context.findings_package())?
    {
        return Err(Error::new(
            "REPAIR_LINK_CORRUPT",
            "historical RepairDispatch Operation is not the exact current review and feedback cause",
        ));
    }
    let link = operation_link(tx, &receipt.operation_id)?.ok_or_else(|| {
        Error::new(
            "REPAIR_LINK_CORRUPT",
            "historical repair has no validated link",
        )
    })?;
    let request = context.delivery_request()?;
    let expected_request = normalized_repair_request(&current.request_value(), &semantic_slot_id)
        .ok_or_else(|| {
        Error::new(
            "REPAIR_LINK_CORRUPT",
            "current correction request has an invalid shape",
        )
    })?;
    if !package_request_matches(
        &request.value(),
        &expected_request,
        &semantic_slot_id,
        context.findings_package(),
        link.schema_version == LEGACY_REPAIR_SCHEMA_VERSION && link.findings_package.is_none(),
    ) {
        return Err(Error::new(
            "REPAIR_LINK_CORRUPT",
            "historical RepairDispatch request differs from the exact current correction",
        ));
    }
    let prepared = PreparedRepairDispatch { context, request };
    match resolve_semantic_slot(tx, &prepared)? {
        RepairSlotResolution::Existing { operation_id, .. }
            if operation_id == receipt.operation_id => {}
        RepairSlotResolution::Conflict { .. } => {
            return Ok(Some(repair_skipped(
                assignment_id,
                result_operation_id,
                "historical_repair_semantic_slot_conflict",
            )));
        }
        RepairSlotResolution::Existing { .. } | RepairSlotResolution::Vacant => {
            return Err(Error::new(
                "REPAIR_SLOT_CORRUPT",
                "historical RepairDispatch slot resolved to a different Operation",
            ));
        }
    }

    let sent_at_ms: Option<i64> = tx.query_row(
        "SELECT sent_at_ms FROM operations WHERE operation_id=?1",
        [&receipt.operation_id],
        |row| row.get(0),
    )?;
    let mut continued_queued_operation = false;
    if operation_state == "queued" && sent_at_ms.is_none() {
        let continuation = crate::automation::authorization::current_transfer_continuation(
            tx,
            &receipt.operation_id,
            "agent.send",
            AutomationStep::RepairDispatch,
            &current_context.identity().task_id,
        )?
        .ok_or_else(|| {
            Error::new(
                "FORBIDDEN",
                "historical queued RepairDispatch has no current-GM transfer continuation",
            )
        })?;
        if continuation.current_owner_id() != current_context.effective_manager_id() {
            return Err(Error::new(
                "AUTOMATION_TRANSFER_SCOPE",
                "historical RepairDispatch continuation targets a different current GM",
            ));
        }
        prepared
            .context
            .require_transfer_continuation_matches(&continuation)?;
        continued_queued_operation = true;
    }

    let result = match delivery_state(tx, &receipt.operation_id, &prepared)? {
        DeliveryState::Verified => repair_delivered(
            assignment_id,
            result_operation_id,
            &receipt.operation_id,
            &semantic_slot_id,
        ),
        DeliveryState::NoEffect => repair_skipped(
            assignment_id,
            result_operation_id,
            "prior_repair_operation_ended_without_delivery",
        ),
        DeliveryState::Pending(reason) => repair_pending(
            assignment_id,
            result_operation_id,
            &reason,
            if continued_queued_operation {
                "current_transfer_continuation"
            } else {
                "exact_historical_delivery_operation_readback"
            },
        ),
    };
    Ok(Some(result))
}

fn historical_direct_repair_readback(
    db: &Connection,
    operation_id: &str,
    assignment_id: &str,
    result_operation_id: &str,
    semantic_slot_id: &str,
) -> Result<Value> {
    let (state, result_json): (String, Option<String>) = db.query_row(
        "SELECT state,result_json FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    match state.as_str() {
        "settled" => {
            let result: Value = serde_json::from_str(&result_json.ok_or_else(|| {
                Error::new(
                    "REPAIR_OPERATION_CORRUPT",
                    "settled direct repair Operation has no result",
                )
            })?)?;
            if result["outcome"] == "applied"
                && result["native_input_id"]
                    .as_str()
                    .is_some_and(|value| !value.is_empty())
                && result["details"]["completion_condition"] == "native_input_admitted"
            {
                Ok(repair_delivered(
                    assignment_id,
                    result_operation_id,
                    operation_id,
                    semantic_slot_id,
                ))
            } else {
                Ok(repair_pending(
                    assignment_id,
                    result_operation_id,
                    "native_delivery_receipt_is_not_sufficiently_proved",
                    "exact_historical_direct_operation_readback",
                ))
            }
        }
        "rejected" | "cancelled" => Ok(repair_skipped(
            assignment_id,
            result_operation_id,
            "prior_repair_operation_ended_without_delivery",
        )),
        "queued" | "sending" | "native_accepted" | "outcome_unknown" => Ok(repair_pending(
            assignment_id,
            result_operation_id,
            "direct_historical_correction_requires_readback",
            "exact_historical_direct_operation_readback",
        )),
        _ => Err(Error::new(
            "REPAIR_OPERATION_CORRUPT",
            "direct correction Operation has an unsupported lifecycle state",
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
           AND a.attempt_id=(SELECT active_attempt.attempt_id FROM attempts AS active_attempt \
                             WHERE active_attempt.task_id=t.task_id \
                               AND active_attempt.released_at_ms IS NULL \
                             ORDER BY active_attempt.created_at_ms DESC,active_attempt.attempt_id DESC LIMIT 1) \
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
        let finding_ids = disposition["finding_ids"]
            .as_array()
            .ok_or_else(|| source_gap("manager disposition has no findings package"))?;
        if finding_ids.is_empty() {
            return Err(source_gap(
                "manager disposition has an empty findings package",
            ));
        }
        let ordered_finding_ids = finding_ids
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
                    .ok_or_else(|| source_gap("manager disposition finding identity is invalid"))
            })
            .collect::<Result<Vec<_>>>()?;
        let identity: ReviewSlotIdentity = serde_json::from_value(disposition["identity"].clone())
            .map_err(|_| source_gap("manager disposition review identity is invalid"))?;
        let feedback_operation_id = disposition["task_feedback_operation_id"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| source_gap("manager disposition has no feedback Operation identity"))?;
        let feedback_observation_id = observation_id_for_feedback(db, feedback_operation_id)?;
        let result_operation_id = model::text(&disposition, "review_result_operation_id")?;
        let mut ordered_findings = Vec::with_capacity(ordered_finding_ids.len());
        let mut no_longer_actionable = false;
        for finding_id in &ordered_finding_ids {
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
                    no_longer_actionable = true;
                    break;
                }
                Err(error) => return Err(error),
            };
            if provenance["review_assignment_id"] != assignment_id
                || provenance["review_operation_id"] != result_operation_id
                || provenance["identity"] != json!(identity)
                || provenance["finding"]["finding_id"].as_str() != Some(finding_id.as_str())
            {
                return Err(source_gap(
                    "current actionable finding differs from retained disposition",
                ));
            }
            ordered_findings.push(
                serde_json::from_value::<ReviewFinding>(provenance["finding"].clone())
                    .map_err(|_| source_gap("actionable finding fields are invalid"))?,
            );
        }
        if no_longer_actionable {
            continue;
        }
        let findings_package = ReviewFindingsPackage::new(
            identity.clone(),
            assignment_id.to_owned(),
            result_operation_id.to_owned(),
            ordered_findings,
        )
        .map_err(|_| source_gap("retained ordered findings package is invalid"))?;
        if findings_package.finding_ids() != ordered_finding_ids {
            return Err(source_gap(
                "retained disposition changed the findings order",
            ));
        }
        let lineage = crate::automation::repair::validate_committed_review_and_feedback(
            db,
            &identity,
            assignment_id,
            result_operation_id,
            &disposition_operation_id,
            feedback_operation_id,
            feedback_observation_id,
            &findings_package,
        )?;
        if lineage.decision_manager_id.as_str() != principal.client_id.as_str()
            || lineage.binding_id.as_str() != binding_id
            || lineage.binding_generation != binding_generation
        {
            return Err(source_gap(
                "direct correction actor or binding differs from the retained disposition lineage",
            ));
        }
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
            findings_package.semantic_subject_key(),
        )?;
        let request_id = model::text(request, "client_request_id")?;
        let expected = json!({
            "client_request_id":request_id,
            "binding_id":binding_id,
            "generation":binding_generation,
            "delivery":"next_turn",
            "text":crate::automation::repair::render_correction_text(&identity, &findings_package)
        });
        let current_text = model::canonical(request)? == model::canonical(&expected)?;
        if !package_request_matches(
            request,
            &expected,
            &semantic_slot_id,
            &findings_package,
            true,
        ) {
            continue;
        }
        let request_digest = repair_request_digest(request)?;
        let candidate = DirectRepairSlot {
            manager_id: principal.client_id.clone(),
            identity,
            findings_package,
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
        if !current_text && resolve_direct_slot(db, &candidate)? == RepairSlotResolution::Vacant {
            return Err(Error::new(
                "REPAIR_REQUEST_RETIRED",
                "historical singleton correction text can only read back a retained delivery",
            ));
        }
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
    // A retained singleton caller may still carry historical text. Lookup
    // compares the current package contract and lets only the sealed v1
    // receipt opt into its old immutable renderer.
    let mut expected_request = slot.request.clone();
    expected_request["text"] = json!(crate::automation::repair::render_correction_text(
        &slot.identity,
        &slot.findings_package,
    ));
    resolve_slot_candidates(
        db,
        &slot.manager_id,
        &slot.identity,
        &slot.findings_package,
        &slot.binding_id,
        slot.binding_generation,
        &slot.semantic_slot_id,
        &expected_request,
        &repair_request_digest(&expected_request)?,
    )
}

/// Resolve one durable manager/Task/package slot before Operation creation.
/// Direct owner `agent.send` and selected automation must both call this
/// helper when the send is the exact retained correction request.
pub(crate) fn resolve_semantic_slot(
    db: &Connection,
    prepared: &PreparedRepairDispatch,
) -> Result<RepairSlotResolution> {
    resolve_slot_candidates(
        db,
        prepared.context.effective_manager_id(),
        prepared.context.identity(),
        prepared.context.findings_package(),
        prepared.context.binding_id(),
        prepared.context.binding_generation(),
        prepared.context.semantic_slot_id(),
        &prepared.request_value(),
        &prepared.request.parameters_digest()?,
    )
}

#[allow(clippy::too_many_arguments)] // Every value is part of the exact direct/automatic slot proof.
fn resolve_slot_candidates(
    db: &Connection,
    manager_id: &str,
    identity: &ReviewSlotIdentity,
    findings_package: &ReviewFindingsPackage,
    expected_binding_id: &str,
    binding_generation: i64,
    current_semantic_slot_id: &str,
    request: &Value,
    parameters_digest: &str,
) -> Result<RepairSlotResolution> {
    let candidates = semantic_slot_id_candidates(manager_id, identity, findings_package)?;
    if !candidates
        .iter()
        .any(|candidate| candidate == current_semantic_slot_id)
    {
        return Err(Error::new(
            "REPAIR_SLOT_CORRUPT",
            "RepairDispatch slot is not a recognized versioned package identity",
        ));
    }

    let mut retained = None;
    for semantic_slot_id in candidates {
        let resolution = resolve_slot_parts(
            db,
            manager_id,
            identity,
            findings_package,
            expected_binding_id,
            binding_generation,
            &semantic_slot_id,
            request,
            parameters_digest,
        )?;
        if resolution == RepairSlotResolution::Vacant {
            continue;
        }
        if retained.is_some() {
            return Err(Error::new(
                "REPAIR_SLOT_AMBIGUOUS",
                "both current and historical RepairDispatch slots are retained",
            ));
        }
        retained = Some(resolution);
    }
    Ok(retained.unwrap_or(RepairSlotResolution::Vacant))
}

fn semantic_slot_id_candidates(
    manager_id: &str,
    identity: &ReviewSlotIdentity,
    findings_package: &ReviewFindingsPackage,
) -> Result<Vec<String>> {
    let current =
        semantic_slot_id_for_schema(manager_id, identity, findings_package, SLOT_SCHEMA_VERSION)?;
    let historical = semantic_slot_id_for_schema(
        manager_id,
        identity,
        findings_package,
        LEGACY_REPAIR_SCHEMA_VERSION,
    )?;
    let mut candidates = vec![current];
    if candidates[0] != historical {
        candidates.push(historical);
    }
    Ok(candidates)
}

fn semantic_slot_id_for_schema(
    manager_id: &str,
    identity: &ReviewSlotIdentity,
    findings_package: &ReviewFindingsPackage,
    schema_version: u32,
) -> Result<String> {
    let subject_key = findings_package
        .semantic_subject_key_for_schema(schema_version)
        .ok_or_else(|| {
            Error::new(
                "REPAIR_SLOT_CORRUPT",
                "RepairDispatch slot schema version is unsupported",
            )
        })?;
    crate::automation::repair::semantic_slot_id(manager_id, identity, subject_key)
}

fn slot_subject_matches(
    manager_id: &str,
    identity: &ReviewSlotIdentity,
    findings_package: &ReviewFindingsPackage,
    semantic_slot_id: &str,
    receipt: &RepairSlotReceipt,
) -> Result<bool> {
    Ok(slot_identity_matches(
        manager_id,
        identity,
        findings_package,
        semantic_slot_id,
        receipt,
    )? && receipt_matches_package(receipt, findings_package)
        && receipt_schema_shape_matches(receipt, findings_package))
}

fn receipt_schema_shape_matches(
    receipt: &RepairSlotReceipt,
    findings_package: &ReviewFindingsPackage,
) -> bool {
    match (
        receipt.schema_version,
        receipt.finding_id.as_deref(),
        receipt.findings_digest.as_deref(),
    ) {
        (LEGACY_REPAIR_SCHEMA_VERSION, None, Some(_)) => true,
        (LEGACY_REPAIR_SCHEMA_VERSION, Some(finding_id), None) => {
            findings_package.findings.len() == 1
                && findings_package.findings[0].finding_id == finding_id
        }
        (SLOT_SCHEMA_VERSION, None, Some(findings_digest)) => {
            findings_digest == findings_package.findings_digest
        }
        _ => false,
    }
}

fn slot_identity_matches(
    manager_id: &str,
    identity: &ReviewSlotIdentity,
    findings_package: &ReviewFindingsPackage,
    semantic_slot_id: &str,
    receipt: &RepairSlotReceipt,
) -> Result<bool> {
    if !supported_repair_schema_version(receipt.schema_version) {
        return Ok(false);
    }
    let expected_id = semantic_slot_id_for_schema(
        manager_id,
        identity,
        findings_package,
        receipt.schema_version,
    )?;
    Ok(expected_id == semantic_slot_id && receipt.semantic_slot_id == semantic_slot_id)
}

fn supported_repair_schema_version(schema_version: u32) -> bool {
    matches!(
        schema_version,
        LEGACY_REPAIR_SCHEMA_VERSION | SLOT_SCHEMA_VERSION
    )
}

fn slot_schema_matches_link(link_schema_version: u32, slot_schema_version: u32) -> bool {
    supported_repair_schema_version(link_schema_version)
        && link_schema_version == slot_schema_version
}

#[allow(clippy::too_many_arguments)] // The resolver checks one exact slot and its immutable Task, finding, binding, and request evidence.
fn resolve_slot_parts(
    db: &Connection,
    manager_id: &str,
    identity: &ReviewSlotIdentity,
    findings_package: &ReviewFindingsPackage,
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
    if receipt.effective_manager_id != manager_id
        || receipt.task_id != identity.task_id
        || receipt.task_revision != identity.task_revision
        || receipt.attempt_id != identity.attempt_id
        || receipt.submission_ref != identity.submission_ref
        || receipt.candidate_ref != identity.candidate_ref
        || !slot_identity_matches(
            manager_id,
            identity,
            findings_package,
            semantic_slot_id,
            &receipt,
        )?
        || !receipt_schema_shape_matches(&receipt, findings_package)
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
    if repair_request_digest(request)? != parameters_digest {
        return Err(Error::new(
            "REPAIR_OPERATION_CORRUPT",
            "repair request digest differs from its retained parameters",
        ));
    }
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
    let allow_singleton_text = receipt.schema_version == LEGACY_REPAIR_SCHEMA_VERSION
        && receipt.findings_digest.is_none()
        && receipt.finding_id.is_some();
    let normalized_request_matches = package_request_matches(
        &normalized_original,
        &normalized_request,
        semantic_slot_id,
        findings_package,
        allow_singleton_text,
    );
    let legacy_digest = if receipt.schema_version == LEGACY_REPAIR_SCHEMA_VERSION
        && findings_package.findings.len() == 1
    {
        let finding = &findings_package.findings[0];
        let retained_digest =
            rendered_singleton_package_digest(&original_value, identity, &finding.finding_id)
                .or_else(|| {
                    (allow_singleton_text && normalized_request_matches)
                        .then(|| findings_package.findings_digest.clone())
                })
                .ok_or_else(|| {
                    Error::new(
                        "REPAIR_SLOT_CORRUPT",
                        "legacy singleton Operation has no exact retained package digest text",
                    )
                })?;
        let receipt_matches_legacy_shape = match (
            receipt.findings_digest.as_deref(),
            receipt.finding_id.as_deref(),
        ) {
            (Some(receipt_digest), None) => retained_digest == receipt_digest,
            (None, Some(receipt_finding_id)) => receipt_finding_id == finding.finding_id,
            _ => false,
        };
        if !receipt_matches_legacy_shape {
            return Err(Error::new(
                "REPAIR_SLOT_CORRUPT",
                "legacy singleton slot digest or finding identity differs from its immutable Operation",
            ));
        }
        Some(retained_digest)
    } else {
        None
    };
    let distinct_legacy_package = receipt.schema_version == LEGACY_REPAIR_SCHEMA_VERSION
        && findings_package.findings.len() == 1
        && legacy_digest
            .as_deref()
            .is_some_and(|digest| digest != findings_package.findings_digest);
    if method != "agent.send"
        || !request_id_matches
        || task_id.as_deref() != Some(identity.task_id.as_str())
        || attempt_id.as_deref() != Some(identity.attempt_id.as_str())
        || actual_binding_id.as_deref() != Some(expected_binding_id)
        || generation != Some(binding_generation)
        || repair_request_digest(&original_value)? != receipt.parameters_digest
    {
        return Err(Error::new(
            "REPAIR_SLOT_CORRUPT",
            "retained repair slot does not match its immutable delivery Operation",
        ));
    }
    if !distinct_legacy_package
        && (!receipt_matches_package(&receipt, findings_package) || !normalized_request_matches)
    {
        return Err(Error::new(
            "REPAIR_SLOT_CORRUPT",
            "retained repair slot does not match its immutable package and delivery Operation",
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
        let linked_package = findings_package_from_link(&link).map_err(|_| {
            Error::new(
                "REPAIR_LINK_CORRUPT",
                "automated correction link has no validated retained package",
            )
        })?;
        if distinct_legacy_package {
            let finding_id = &findings_package.findings[0].finding_id;
            if linked_package.identity != *identity
                || linked_package.findings.len() != 1
                || linked_package.findings[0].finding_id != *finding_id
                || legacy_digest.as_deref() != Some(linked_package.findings_digest.as_str())
                || linked_package.findings_digest == findings_package.findings_digest
            {
                return Err(Error::new(
                    "REPAIR_LINK_CORRUPT",
                    "historical automated slot does not prove its distinct retained package",
                ));
            }
        } else if linked_package != *findings_package {
            return Err(Error::new(
                "REPAIR_LINK_CORRUPT",
                "automated correction link package differs from the exact retained package",
            ));
        }
    } else if caller_is_manager {
        let has_automation_provenance = receipt.source_attempt_owner_id.is_some()
            || receipt.review_assignment_sponsor_id.is_some()
            || receipt.decision_manager_id.is_some()
            || receipt.transfer_operation_ids.is_some()
            || receipt.captured_transfer_gm_epoch.is_some();
        if has_automation_provenance {
            return Err(Error::new(
                "REPAIR_SLOT_CORRUPT",
                "direct correction slot unexpectedly carries automation provenance",
            ));
        }
    }
    if distinct_legacy_package {
        return Ok(RepairSlotResolution::Conflict {
            operation_id: receipt.operation_id,
            operation_state: state,
        });
    }
    Ok(RepairSlotResolution::Existing {
        operation_id: receipt.operation_id,
        operation_state: state,
    })
}

fn rendered_singleton_package_digest(
    request: &Value,
    identity: &ReviewSlotIdentity,
    finding_id: &str,
) -> Option<String> {
    let text = request.get("text")?.as_str()?;
    let prefix = format!(
        "A manager applied this ordered correction package to your current Task Attempt. Keep the same unreleased Attempt, address every finding in reviewer order, and submit a new candidate linked to the prior submission.\n\nTask: {}\nTask revision: {}\nAttempt: {}\nPrior submission: {}\nPrior candidate: {}\nFindings package digest: ",
        identity.task_id,
        identity.task_revision,
        identity.attempt_id,
        identity.submission_ref,
        identity.candidate_ref,
    );
    let digest_and_findings = text.strip_prefix(&prefix)?;
    let (digest, findings) = digest_and_findings.split_once("\n\n")?;
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || !findings.starts_with(&format!("Finding 1: {finding_id}\nReason:\n"))
    {
        return None;
    }
    Some(digest.to_owned())
}

fn receipt_matches_package(receipt: &RepairSlotReceipt, package: &ReviewFindingsPackage) -> bool {
    receipt.findings_digest.as_deref() == Some(package.findings_digest.as_str())
        || (receipt.findings_digest.is_none()
            && receipt.schema_version == 1
            && package.findings.len() == 1
            && receipt.finding_id.as_deref()
                == package
                    .findings
                    .first()
                    .map(|finding| finding.finding_id.as_str()))
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

fn package_request_matches(
    original: &Value,
    expected: &Value,
    semantic_slot_id: &str,
    package: &ReviewFindingsPackage,
    allow_singleton_text: bool,
) -> bool {
    let Some(original) = normalized_repair_request(original, semantic_slot_id) else {
        return false;
    };
    let Some(mut expected) = normalized_repair_request(expected, semantic_slot_id) else {
        return false;
    };
    if original == expected {
        return true;
    }
    if allow_singleton_text && let Some(text) = historical_singleton_text(package) {
        expected["text"] = json!(text);
        return original == expected;
    }
    false
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
    let lineage = crate::automation::repair::validate_committed_review_and_feedback(
        tx,
        &slot.identity,
        &slot.assignment_id,
        &slot.result_operation_id,
        &slot.disposition_operation_id,
        &slot.feedback_operation_id,
        slot.feedback_observation_id,
        &slot.findings_package,
    )?;
    if lineage.decision_manager_id.as_str() != slot.manager_id.as_str()
        || lineage.binding_id.as_str() != slot.binding_id.as_str()
        || lineage.binding_generation != slot.binding_generation
    {
        return Err(source_gap(
            "direct correction slot differs from the retained disposition lineage",
        ));
    }
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
        &slot.findings_package.findings_digest,
        &slot.semantic_slot_id,
        operation_id,
        &slot.request_digest,
        now_ms,
        None,
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
        &context.findings_package().findings_digest,
        context.semantic_slot_id(),
        operation_id,
        parameters_digest,
        now_ms,
        Some(RepairSourceFacts::from_context(context)),
    )
}

#[allow(clippy::too_many_arguments)] // Persisting the slot keeps each validated semantic subject field explicit.
fn retain_slot_parts(
    tx: &Transaction<'_>,
    manager_id: &str,
    identity: &ReviewSlotIdentity,
    findings_digest: &str,
    semantic_slot_id: &str,
    operation_id: &str,
    parameters_digest: &str,
    now_ms: i64,
    provenance: Option<RepairSourceFacts>,
) -> Result<()> {
    let expected_semantic_slot_id =
        crate::automation::repair::semantic_slot_id(manager_id, identity, findings_digest)?;
    if expected_semantic_slot_id != semantic_slot_id {
        return Err(Error::new(
            "REPAIR_SLOT_CORRUPT",
            "new RepairDispatch slot key does not match its full findings digest",
        ));
    }
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
            && existing.schema_version == SLOT_SCHEMA_VERSION
            && existing.semantic_slot_id == semantic_slot_id
            && existing.finding_id.is_none()
            && existing.findings_digest.as_deref() == Some(findings_digest)
        {
            return Ok(());
        }
        return Err(Error::new(
            "REPAIR_SLOT_CONFLICT",
            "another correction Operation already occupies this semantic slot",
        ));
    }
    let (
        source_attempt_owner_id,
        review_assignment_sponsor_id,
        decision_manager_id,
        transfer_operation_ids,
        captured_transfer_gm_epoch,
    ) = provenance.map_or((None, None, None, None, None), |facts| {
        (
            Some(facts.source_attempt_owner_id),
            Some(facts.review_assignment_sponsor_id),
            Some(facts.decision_manager_id),
            Some(facts.transfer_operation_ids),
            facts.captured_transfer_gm_epoch,
        )
    });
    let receipt = RepairSlotReceipt {
        schema_version: SLOT_SCHEMA_VERSION,
        semantic_slot_id: semantic_slot_id.to_owned(),
        effective_manager_id: manager_id.to_owned(),
        task_id: identity.task_id.clone(),
        task_revision: identity.task_revision,
        attempt_id: identity.attempt_id.clone(),
        submission_ref: identity.submission_ref.clone(),
        candidate_ref: identity.candidate_ref.clone(),
        finding_id: None,
        findings_digest: Some(findings_digest.to_owned()),
        parameters_digest: parameters_digest.to_owned(),
        operation_id: operation_id.to_owned(),
        reserved_at_ms: now_ms,
        source_attempt_owner_id,
        review_assignment_sponsor_id,
        decision_manager_id,
        transfer_operation_ids,
        captured_transfer_gm_epoch,
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
        finding_id: None,
        review_assignment_id: context.review_assignment_id().to_owned(),
        review_result_operation_id: context.review_result_operation_id().to_owned(),
        disposition_operation_id: context.disposition_operation_id().to_owned(),
        feedback_operation_id: context.feedback_operation_id().to_owned(),
        feedback_observation_id: context.feedback_observation_id(),
        binding_id: context.binding_id().to_owned(),
        binding_generation: context.binding_generation(),
        request_digest: request_digest.to_owned(),
        legacy_identity: None,
        findings_package: Some(context.findings_package().clone()),
        finding: None,
        cause: context.cause_value(),
        linked_at_ms,
        source_attempt_owner_id: Some(context.source_attempt_owner_id().to_owned()),
        review_assignment_sponsor_id: Some(context.review_assignment_sponsor_id().to_owned()),
        decision_manager_id: Some(context.decision_manager_id().to_owned()),
        transfer_operation_ids: Some(context.transfer_operation_ids().to_vec()),
        captured_transfer_gm_epoch: context.captured_transfer_gm_epoch(),
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
    let findings_package = findings_package_from_link(&link)?;
    let identity = findings_package.identity.clone();
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
        identity,
        findings_package,
        &link.binding_id,
        link.binding_generation,
        &link.semantic_slot_id,
        link.schema_version,
        link.schema_version == LEGACY_REPAIR_SCHEMA_VERSION && link.findings_package.is_none(),
        link.captured_transfer_gm_epoch,
    )
}

fn findings_package_from_link(link: &RepairDispatchOperationLink) -> Result<ReviewFindingsPackage> {
    let identity = identity_from_link(link)?;
    if let Some(package) = link.findings_package.as_ref() {
        package
            .validate()
            .map_err(|_| source_gap("retained RepairDispatch findings package is invalid"))?;
        if package.identity != identity
            || package.review_assignment_id != link.review_assignment_id
            || package.review_result_operation_id != link.review_result_operation_id
        {
            return Err(source_gap(
                "retained RepairDispatch package differs from its review cause",
            ));
        }
        return Ok(package.clone());
    }
    let finding = link
        .finding
        .as_ref()
        .filter(|finding| link.finding_id.as_deref() == Some(finding.finding_id.as_str()))
        .ok_or_else(|| source_gap("legacy RepairDispatch link has no exact finding"))?;
    ReviewFindingsPackage::new(
        identity,
        link.review_assignment_id.clone(),
        link.review_result_operation_id.clone(),
        vec![finding.clone()],
    )
    .map_err(|_| source_gap("legacy RepairDispatch finding cannot form a package"))
}

/// Recover the complete review identity from the retained v1 cause and verify
/// its flat Operation-link projection. The nested identity preserves review
/// policy generation and slot for the historical single-finding decoder;
/// plural findings are additionally bound by the validated package digest.
fn identity_from_link(link: &RepairDispatchOperationLink) -> Result<ReviewSlotIdentity> {
    let identity: ReviewSlotIdentity = serde_json::from_value(link.cause["identity"].clone())
        .map_err(|_| source_gap("retained RepairDispatch cause has no valid review identity"))?;
    if identity.task_id != link.task_id
        || identity.task_revision != link.task_revision
        || identity.attempt_id != link.attempt_id
        || identity.submission_ref != link.submission_ref
        || identity.candidate_ref != link.candidate_ref
        || link
            .legacy_identity
            .as_ref()
            .is_some_and(|legacy_identity| legacy_identity != &identity)
        || link.findings_package.as_ref().is_some_and(|package| {
            package.identity != identity
                || package.review_assignment_id != link.review_assignment_id
                || package.review_result_operation_id != link.review_result_operation_id
        })
    {
        return Err(source_gap(
            "retained RepairDispatch identity differs from its flat link or package",
        ));
    }
    Ok(identity)
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
    let findings_package = findings_package_from_link(link).map_err(|_| corrupt())?;
    let identity = findings_package.identity.clone();
    if !supported_repair_schema_version(link.schema_version)
        || link.operation_id != operation_id
        || link.technical_requester_id
            != crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
        || link.action != "agent.send"
        || link.semantic_cause_kind != "review_disposition"
        || link.automation_revision <= 0
        || link.binding_generation <= 0
        || link.finding_id.as_ref().is_some_and(|finding_id| {
            findings_package.findings.len() != 1
                || findings_package.findings[0].finding_id.as_str() != finding_id.as_str()
        })
        || (link.schema_version == LINK_SCHEMA_VERSION
            && (link.findings_package.is_none()
                || link.finding_id.is_some()
                || link.finding.is_some()))
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
        identity,
        findings_package,
        &link.binding_id,
        link.binding_generation,
        &link.semantic_slot_id,
        link.schema_version,
        link.schema_version == LEGACY_REPAIR_SCHEMA_VERSION && link.findings_package.is_none(),
        link.captured_transfer_gm_epoch,
    )
    .map_err(|_| corrupt())?;
    if link.semantic_cause_id != context.review_assignment_id()
        || !link_provenance_matches_context(link, &context)
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

fn link_provenance_matches_context(
    link: &RepairDispatchOperationLink,
    context: &RepairDispatchContext,
) -> bool {
    let fields = [
        link.source_attempt_owner_id.is_some(),
        link.review_assignment_sponsor_id.is_some(),
        link.decision_manager_id.is_some(),
        link.transfer_operation_ids.is_some(),
    ];
    let package_matches = link.findings_package.as_ref().map_or_else(
        || {
            context.findings_package().findings.len() == 1
                && link.finding.as_ref() == context.findings_package().findings.first()
        },
        |package| package == context.findings_package(),
    );
    if !package_matches {
        return false;
    }
    if fields.iter().all(|present| !present) {
        return link.captured_transfer_gm_epoch.is_none()
            && link.cause == context.legacy_cause_value();
    }
    if fields.iter().any(|present| !present) {
        return false;
    }
    link.source_attempt_owner_id.as_deref() == Some(context.source_attempt_owner_id())
        && link.review_assignment_sponsor_id.as_deref()
            == Some(context.review_assignment_sponsor_id())
        && link.decision_manager_id.as_deref() == Some(context.decision_manager_id())
        && link.transfer_operation_ids.as_deref() == Some(context.transfer_operation_ids())
        && link.captured_transfer_gm_epoch == context.captured_transfer_gm_epoch()
        && link.cause == context.cause_value()
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
    let findings_package = findings_package_from_link(link)?;
    if !slot_schema_matches_link(link.schema_version, slot.schema_version)
        || slot.semantic_slot_id != link.semantic_slot_id
        || slot.effective_manager_id != link.effective_manager_id
        || slot.task_id != link.task_id
        || slot.task_revision != link.task_revision
        || slot.attempt_id != link.attempt_id
        || slot.submission_ref != link.submission_ref
        || slot.candidate_ref != link.candidate_ref
        || !slot_subject_matches(
            &link.effective_manager_id,
            &findings_package.identity,
            &findings_package,
            &link.semantic_slot_id,
            &slot,
        )?
        || slot.operation_id != link.operation_id
        || slot.parameters_digest != link.request_digest
        || slot.source_attempt_owner_id != link.source_attempt_owner_id
        || slot.review_assignment_sponsor_id != link.review_assignment_sponsor_id
        || slot.decision_manager_id != link.decision_manager_id
        || slot.transfer_operation_ids != link.transfer_operation_ids
        || slot.captured_transfer_gm_epoch != link.captured_transfer_gm_epoch
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
    if !matches!(
        resolve_semantic_slot(db, prepared)?,
        RepairSlotResolution::Existing { operation_id: retained, .. } if retained == operation_id
    ) {
        return Err(Error::new(
            "REPAIR_OPERATION_CORRUPT",
            "delivery readback has no exact retained semantic slot",
        ));
    }
    let caller_is_automation = caller == prepared.context.technical_requester_id();
    let caller_is_manager = caller == prepared.context.effective_manager_id();
    let original_request_id = request.get("client_request_id").and_then(Value::as_str);
    let request_id_matches = if caller_is_automation {
        original_request_id == Some(client_request_id.as_str())
            && semantic_slot_id_candidates(
                prepared.context.effective_manager_id(),
                prepared.context.identity(),
                prepared.context.findings_package(),
            )?
            .contains(&client_request_id)
    } else if caller_is_manager {
        !client_request_id.is_empty() && original_request_id == Some(client_request_id.as_str())
    } else {
        false
    };
    let exact_request = package_request_matches(
        &request,
        &expected,
        semantic_slot_id,
        prepared.context.findings_package(),
        true, // The exact retained slot was validated above, including its version and text.
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

#[cfg(test)]
#[path = "automation_repair_slot_identity_fixtures.rs"]
mod slot_identity_fixtures;

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
