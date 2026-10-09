//! Durable advisory Concilium projections over the existing Operations and
//! Observations ledger. No Task graph, model runtime, or second event bus is
//! created here.

use super::{coordination, meta, set_meta, tasks};
use crate::{
    coordination::concilium::{
        CloseRequest, ConciliumRequest, ListRequest, OpenRequest, ParticipantPosition,
        ParticipantRef, PositionSubmitRequest, ProposeRequest, RoundAdvanceRequest,
    },
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use swarm_contracts::concilium_limits as limits;

const SCHEMA: &str = "eliot.concilium.v1";
const PROJECTION_PREFIX: &str = "concilium:v1:projection:";
const OPERATION_PREFIX: &str = "concilium:v1:operation:";
const TASK_INDEX_PREFIX: &str = "concilium:v1:index:task:";
const SCOPE_INDEX_PREFIX: &str = "concilium:v1:index:scope:";
const VIEWER_INDEX_PREFIX: &str = "concilium:v1:index:viewer:";
const MAX_PACKET_BYTES: usize = 128 * 1024;
const MAX_STATE_BYTES: usize = 2 * 1024 * 1024;
const MODEL_EXECUTION_CAP: usize = 8;

fn value_text_is(value: &Value, expected: &str) -> bool {
    value.as_str() == Some(expected)
}

/// Read-only Concilium Store surface. The full request is revalidated here so
/// direct Store readers receive the same strict field boundary as MCP callers.
pub(super) fn read(
    db: &Connection,
    principal: &Principal,
    method: &str,
    value: &Value,
) -> Result<Value> {
    let request = crate::coordination::concilium::parse(method, value)?;
    let principal = super::current_principal(db, principal.clone())?;
    match (method, request) {
        ("concilium.preview", ConciliumRequest::Preview(request)) => {
            let operation_id = request.proposal_operation_id;
            let record = record_for_operation(db, &operation_id)?;
            if !value_text_is(&record["proposal_operation_id"], &operation_id)
                || !value_text_is(&record["proposal"]["operation_id"], &operation_id)
            {
                return Err(damaged(
                    "proposal Operation does not identify the retained proposal",
                ));
            }
            Ok(build_preview(db, &principal, &record)?)
        }
        ("concilium.get", ConciliumRequest::Get(request)) => {
            let record = load_projection(db, &request.concilium_id)?;
            authorize_projection_reader(db, &principal, &record)?;
            Ok(project_page(
                db,
                &principal,
                &record,
                request.limit,
                request.after_slot_id.as_deref(),
            )?)
        }
        ("concilium.list", ConciliumRequest::List(request)) => list(db, &principal, &request),
        _ => Err(Error::new(
            "METHOD_NOT_FOUND",
            format!("{method} is not an implemented Concilium read"),
        )),
    }
}

/// Enforce participant mutation authority before Operation admission. `apply`
/// repeats every target, slot, packet, and state check inside the writer
/// transaction so this preflight can never grant a stale write.
pub(super) fn authorize_participant_mutation(
    db: &Connection,
    principal: &Principal,
    method: &str,
    value: &Value,
) -> Result<()> {
    let request = crate::coordination::concilium::parse(method, value)?;
    let principal = super::current_principal(db, principal.clone())?;
    principal.require_participant()?;
    check_request_size(value)?;
    match (method, request) {
        ("concilium.propose", ConciliumRequest::Propose(request)) => {
            let current = coordination::concilium_current_participant_scope(db, &principal)?;
            if !value_text_is(&current["scope"]["task_id"], &request.task_id)
                || !value_text_is(&current["scope"]["attempt_id"], &request.attempt_id)
            {
                return Err(Error::new(
                    "STALE_PARTICIPANT",
                    "proposal must name this Participant's exact current Task and Attempt",
                ));
            }
            let task = tasks::get_task(db, &request.task_id)?;
            validate_evidence_refs(db, &request.evidence_refs, &task, &request.attempt_id)
        }
        ("concilium.position.submit", ConciliumRequest::PositionSubmit(request)) => {
            let record = load_projection(db, &request.concilium_id)?;
            let slot = slot_by_id(&record, &request.slot_id)?;
            require_exact_live_slot(db, &principal, &record, slot, &request.packet_digest)?;
            validate_position_body(
                db,
                &request.position,
                &record["scope"],
                &record["proposal"]["proposal_revision_ids"],
            )?;
            Ok(())
        }
        _ => Err(Error::new(
            "FORBIDDEN",
            "Participant credentials cannot perform this Concilium mutation",
        )),
    }
}

/// Commit one advisory state transition in the same transaction as the
/// existing Operation and its one automatic controller Observation.
pub(super) fn apply(
    tx: &Transaction<'_>,
    principal: &Principal,
    method: &str,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<(Value, bool)> {
    let request = crate::coordination::concilium::parse(method, value)?;
    verify_admitted_operation(tx, principal, method, value, operation_id)?;
    let principal = super::current_principal(tx, principal.clone())?;
    check_request_size(value)?;
    let (receipt, record) = match (method, request) {
        ("concilium.propose", ConciliumRequest::Propose(request)) => {
            propose(tx, &principal, &request, value, operation_id, now)?
        }
        ("concilium.open", ConciliumRequest::Open(request)) => {
            open(tx, &principal, &request, operation_id, now)?
        }
        ("concilium.position.submit", ConciliumRequest::PositionSubmit(request)) => {
            submit_position(tx, &principal, &request, operation_id, now)?
        }
        ("concilium.round.advance", ConciliumRequest::RoundAdvance(request)) => {
            advance_round(tx, &principal, &request, operation_id, now)?
        }
        ("concilium.close", ConciliumRequest::Close(request)) => {
            close(tx, &principal, &request, operation_id, now)?
        }
        _ => {
            return Err(Error::new(
                "METHOD_NOT_FOUND",
                format!("{method} is not an implemented Concilium mutation"),
            ));
        }
    };
    update_operation_scope(
        tx,
        principal.client_id.as_str(),
        method,
        operation_id,
        &record,
    )?;
    retain_operation_link(tx, operation_id, &record)?;
    Ok((receipt, false))
}

/// Operation readback is limited to the authenticated Participant's own
/// Concilium proposal and position Operations. It remains available after an
/// Attempt transition and never authorizes a peer's original request.
pub(super) fn authorize_operation_read(
    db: &Connection,
    principal: &Principal,
    operation_id: &str,
) -> Result<()> {
    let principal = super::current_principal(db, principal.clone())?;
    principal.require_participant()?;
    let (caller_id, method): (String, String) = db
        .query_row(
            "SELECT caller_id,method FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .ok_or_else(|| Error::new("NOT_FOUND", "Concilium Operation was not found"))?;
    if caller_id != principal.client_id
        || !matches!(
            method.as_str(),
            "concilium.propose" | "concilium.position.submit"
        )
    {
        return Err(Error::new(
            "FORBIDDEN",
            "Participant Operation readback is limited to the caller's own proposal or position",
        ));
    }
    if let Some(link) = meta(db, &operation_key(operation_id))? {
        let record = record_for_operation(db, operation_id)?;
        if link["concilium_id"] != record["concilium_id"]
            || link["proposal_operation_id"] != record["proposal_operation_id"]
        {
            return Err(damaged(
                "Concilium Operation link differs from its projection",
            ));
        }
        if method == "concilium.propose" {
            if !value_text_is(
                &record["proposal"]["proposer"]["client_id"],
                &principal.client_id,
            ) {
                return Err(Error::new(
                    "FORBIDDEN",
                    "linked proposal Operation belongs to a different Participant",
                ));
            }
            let snapshot = participant_snapshot(&record, &principal.client_id)
                .ok_or_else(|| damaged("linked proposal has no retained proposer identity"))?;
            verify_retained_registration(db, &principal.client_id, &record["scope"], &snapshot)?;
            let raw_request: String = db.query_row(
                "SELECT original_request_json FROM operations WHERE operation_id=?1",
                [operation_id],
                |row| row.get(0),
            )?;
            let mut request: Value = serde_json::from_str(&raw_request)?;
            request
                .as_object_mut()
                .ok_or_else(|| damaged("retained proposal Operation request is not an object"))?
                .remove("client_request_id");
            if request != record["proposal"]["request_source"]["request"] {
                return Err(damaged(
                    "proposal Operation request differs from the canonical proposal source",
                ));
            }
        } else {
            let raw_request: String = db.query_row(
                "SELECT original_request_json FROM operations WHERE operation_id=?1",
                [operation_id],
                |row| row.get(0),
            )?;
            let request: Value = serde_json::from_str(&raw_request)?;
            let parsed =
                crate::coordination::concilium::parse("concilium.position.submit", &request)?;
            let ConciliumRequest::PositionSubmit(request) = parsed else {
                return Err(damaged("position Operation parsed as a different request"));
            };
            let position = position_to_value(&request.position);
            let slot = slot_by_id(&record, &request.slot_id)?;
            if record["concilium_id"].as_str() != Some(request.concilium_id.as_str())
                || !value_text_is(
                    &slot["participant_actor"]["client_id"],
                    &principal.client_id,
                )
                || slot["packet_digest"].as_str() != Some(request.packet_digest.as_str())
                || slot["position"] != position
            {
                return Err(damaged(
                    "linked position Operation does not match its exact submitted slot content",
                ));
            }
            verify_retained_registration(db, &principal.client_id, &record["scope"], slot)?;
        }
    } else {
        verify_registered_participant(db, &principal.client_id)?;
    }
    // A rejected mutation rolls back its projection/link savepoint. Caller
    // ownership above is sufficient for recovery of that caller's own ACK.
    Ok(())
}

fn propose(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: &ProposeRequest,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<(Value, Value)> {
    principal.require_participant()?;
    let proposer = coordination::concilium_current_participant_scope(tx, principal)?;
    let scope = &proposer["scope"];
    if !value_text_is(&scope["task_id"], &request.task_id)
        || !value_text_is(&scope["attempt_id"], &request.attempt_id)
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "proposal must name this Participant's exact current Task and Attempt",
        ));
    }
    let task = tasks::get_task(tx, &request.task_id)?;
    let task_revision = task["revision"]
        .as_i64()
        .ok_or_else(|| damaged("current Task has no numeric revision"))?;
    let attempt = tasks::get_attempt(tx, &request.attempt_id)?;
    if !value_text_is(&attempt["task_id"], &request.task_id)
        || attempt["task_revision"] != task_revision
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "proposal Attempt no longer belongs to the current Task revision",
        ));
    }
    let manager_id = model::text(&attempt, "owner_id")?.to_owned();
    validate_evidence_refs(tx, &request.evidence_refs, &task, &request.attempt_id)?;
    let participants = resolve_proposal_participants(
        tx,
        &request.participants,
        &request.task_id,
        task_revision,
        &request.attempt_id,
    )?;
    let mut request_value = value.clone();
    request_value
        .as_object_mut()
        .ok_or_else(|| Error::invalid("Concilium proposal must be an object"))?
        .remove("client_request_id");
    let proposal_source = json!({
        "request":request_value,
        "proposer":{
            "actor":proposer["actor"],
            "scope":proposer["scope"],
            "participation_basis":proposer["participation_basis"],
            "registration_fingerprint":proposer["registration_fingerprint"],
        },
        "participants":participants,
    });
    let proposal_bytes = model::canonical(&proposal_source)?;
    if proposal_bytes.len() > MAX_STATE_BYTES {
        return Err(Error::invalid(
            "Concilium proposal exceeds its storage bound",
        ));
    }
    let proposal_digest = sha256_digest(proposal_bytes.as_bytes());
    let concilium_id = model::new_id();
    let record = json!({
        "schema":SCHEMA,
        "concilium_id":concilium_id,
        "proposal_operation_id":operation_id,
        "scope":{
            "task_id":request.task_id,
            "task_revision":task_revision,
            "attempt_id":request.attempt_id,
            "scope_id":scope["scope_id"],
            "project_id":task["project_id"],
        },
        "manager_id":manager_id,
        "proposal":{
            "operation_id":operation_id,
            "request_source":proposal_source,
            "proposer":proposer["actor"],
            "proposer_scope":proposer["scope"],
            "proposer_participation_basis":proposer["participation_basis"],
            "proposer_registration_fingerprint":proposer["registration_fingerprint"],
            "failed_thread_id":request.failed_thread_id,
            "decision_question":request.decision_question,
            "material_conflict":request.material_conflict,
            "participants":participants,
            "proposal_revision_ids":request.proposal_revision_ids,
            "evidence_refs":request.evidence_refs,
            "expected_output":request.expected_output,
            "suggested_max_rounds":request.suggested_max_rounds,
            "suggested_budget":request.suggested_budget,
            "close_condition":request.close_condition,
            "digest":proposal_digest,
        },
        "status":"proposed",
        "state_revision":1,
        "current_round":0,
        "plan":Value::Null,
        "rounds":[],
        "slots":[],
        "manager_result":Value::Null,
        "created_at_ms":now,
        "updated_at_ms":now,
        "closed_at_ms":Value::Null,
    });
    let (record, created) = coalesce_or_create(tx, record)?;
    let receipt = mutation_receipt(operation_id, &record, created);
    if created {
        index_projection(tx, &record)?;
    }
    Ok((receipt, record))
}

fn open(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: &OpenRequest,
    operation_id: &str,
    now: i64,
) -> Result<(Value, Value)> {
    if !request.confirmed_reasonable {
        return Err(Error::invalid(
            "opening a Concilium requires confirmed_reasonable=true",
        ));
    }
    let proposal_operation_id = request.proposal_operation_id.as_str();
    let mut record = record_for_operation(tx, proposal_operation_id)?;
    if !value_text_is(&record["proposal_operation_id"], proposal_operation_id)
        || !value_text_is(&record["proposal"]["operation_id"], proposal_operation_id)
    {
        return Err(Error::new(
            "CONCILIUM_PROPOSAL_MISMATCH",
            "proposal_operation_id does not identify the retained proposal",
        ));
    }
    require_current_manager(tx, principal, &record)?;
    if record["status"] != "proposed" {
        return Err(Error::conflict("only a proposed Concilium can be opened"));
    }
    let manager_reason = &request.manager_reason;
    let preview = build_preview(tx, principal, &record)?;
    if !value_text_is(&preview["plan_digest"], &request.plan_digest) {
        return Err(Error::new(
            "STALE_CONCILIUM_PREVIEW",
            "Task, Attempt, participant grant, proposal, evidence, or packet changed after preview",
        ));
    }
    let packet = preview["round_1_packet"].clone();
    let packet_digest = model::text(&preview, "round_1_packet_digest")?.to_owned();
    let mut slots = Vec::new();
    for participant in record["proposal"]["participants"]
        .as_array()
        .into_iter()
        .flatten()
    {
        slots.push(new_slot(1, participant, &packet, &packet_digest)?);
    }
    let opened = now;
    record["plan"] = json!(preview);
    record["status"] = json!("round_1_open");
    record["state_revision"] = json!(next_state_revision(&record)?);
    record["current_round"] = json!(1);
    record["rounds"] = json!([{
        "round":1,
        "kind":"blind_positions",
        "status":"open",
        "packet_digest":packet_digest,
        "packet_bytes":model::canonical(&packet)?.len(),
        "opened_at_ms":opened,
        "closed_at_ms":Value::Null,
        "manager_operation_id":operation_id,
        "manager_reason":manager_reason,
    }]);
    record["slots"] = json!(slots);
    record["opened_by"] =
        json!({"client_id":principal.client_id,"role":role_name(&principal.role)});
    record["manager_reason"] = json!(manager_reason);
    record["updated_at_ms"] = json!(now);
    store_projection(tx, &record)?;
    let receipt = mutation_receipt(operation_id, &record, true);
    Ok((receipt, record))
}

fn submit_position(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: &PositionSubmitRequest,
    operation_id: &str,
    now: i64,
) -> Result<(Value, Value)> {
    principal.require_participant()?;
    let mut record = load_projection(tx, &request.concilium_id)?;
    let slot_index = slot_index_by_id(&record, &request.slot_id)?;
    let slot = record["slots"][slot_index].clone();
    require_exact_live_slot(tx, principal, &record, &slot, &request.packet_digest)?;
    let position = position_to_value(&request.position);
    validate_position_body(
        tx,
        &request.position,
        &record["scope"],
        &record["proposal"]["proposal_revision_ids"],
    )?;
    let old_position = &record["slots"][slot_index]["position"];
    if record["slots"][slot_index]["state"] == "submitted" {
        if old_position == &position
            && value_text_is(
                &record["slots"][slot_index]["packet_digest"],
                &request.packet_digest,
            )
        {
            return Ok((mutation_receipt(operation_id, &record, false), record));
        }
        return Err(Error::new(
            "CONCILIUM_SLOT_CONFLICT",
            "this exact participant slot already contains a different position",
        ));
    }
    if record["slots"][slot_index]["state"] != "pending"
        || record["slots"][slot_index]["round"] != record["current_round"]
        || !round_is_open(&record)
    {
        return Err(Error::conflict(
            "the participant slot is not open for response",
        ));
    }
    record["slots"][slot_index]["state"] = json!("submitted");
    record["slots"][slot_index]["response_operation_id"] = json!(operation_id);
    record["slots"][slot_index]["source_result_ref"] = Value::Null;
    record["slots"][slot_index]["position"] = position;
    let round = record["current_round"].as_i64().unwrap_or_default();
    let all_submitted = record["slots"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|slot| slot["round"].as_i64() == Some(round))
        .all(|slot| slot["state"] == "submitted");
    record["state_revision"] = json!(next_state_revision(&record)?);
    if all_submitted {
        seal_round(&mut record, round, now)?;
        record["status"] = json!(match round {
            1 => "round_1_ready",
            2 if record["proposal"]["suggested_max_rounds"] == 3 => "merge_available",
            2 => "round_2_ready",
            _ => "merge_available",
        });
    }
    record["updated_at_ms"] = json!(now);
    store_projection(tx, &record)?;
    let receipt = mutation_receipt(operation_id, &record, true);
    Ok((receipt, record))
}

fn advance_round(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: &RoundAdvanceRequest,
    operation_id: &str,
    now: i64,
) -> Result<(Value, Value)> {
    let mut record = load_projection(tx, &request.concilium_id)?;
    require_current_manager(tx, principal, &record)?;
    if record["state_revision"].as_i64() != Some(request.expected_state_revision) {
        return Err(Error::new(
            "STALE_CONCILIUM_REVISION",
            "expected_state_revision does not match the current Concilium revision",
        ));
    }
    let next_round = request.next_round;
    let manager_reason = &request.manager_reason;
    if next_round
        > record["proposal"]["suggested_max_rounds"]
            .as_i64()
            .unwrap_or_default()
    {
        return Err(Error::conflict(
            "requested round exceeds the proposal's suggested_max_rounds",
        ));
    }
    match next_round {
        2 if record["current_round"] == 1 && record["status"] == "round_1_ready" => {}
        3 if record["current_round"] == 2
            && record["status"] == "merge_available"
            && record["proposal"]["suggested_max_rounds"] == 3 => {}
        2 | 3 => {
            return Err(Error::conflict(
                "round advance does not match the sealed current round and state",
            ));
        }
        _ => return Err(Error::invalid("next_round must be 2 or 3")),
    }
    let merged_digest = request.merged_proposal_digest.as_deref().map(str::to_owned);
    if next_round == 3 {
        let digest = merged_digest
            .as_deref()
            .ok_or_else(|| Error::invalid("round 3 requires a changed merged_proposal_digest"))?;
        if value_text_is(&record["proposal"]["digest"], digest) {
            return Err(Error::invalid(
                "round 3 merged_proposal_digest must differ from the original proposal digest",
            ));
        }
        if value_text_is(&record["merged_proposal_digest"], digest) {
            return Err(Error::conflict(
                "round 3 digest must identify a newly changed merged proposal",
            ));
        }
    } else if merged_digest.is_some() {
        return Err(Error::invalid(
            "merged_proposal_digest is allowed only for round 3",
        ));
    }
    let packets = build_next_round_packets(&record, next_round, merged_digest.as_deref())?;
    let mut slots = Vec::new();
    for (participant, packet) in packets {
        let digest = sha256_digest(model::canonical(&packet)?.as_bytes());
        slots.push(new_slot(next_round, &participant, &packet, &digest)?);
    }
    let batch_digest = round_batch_digest(next_round, &slots)?;
    let previous_round = record["current_round"].as_i64().unwrap_or_default();
    seal_round(&mut record, previous_round, now)?;
    let mut rounds = record["rounds"].as_array().cloned().unwrap_or_default();
    rounds.push(json!({
        "round":next_round,
        "kind":if next_round == 2 { "cross_review" } else { "merge_review" },
        "status":"open",
        "packet_digest":batch_digest,
        "opened_at_ms":now,
        "closed_at_ms":Value::Null,
        "merged_proposal_digest":merged_digest,
        "merged_proposal_digest_basis":if next_round == 3 { json!("manager_attested_reference") } else { Value::Null },
        "manager_operation_id":operation_id,
        "manager_reason":manager_reason,
    }));
    record["rounds"] = json!(rounds);
    let mut existing = record["slots"].as_array().cloned().unwrap_or_default();
    existing.extend(slots);
    record["slots"] = json!(existing);
    record["current_round"] = json!(next_round);
    record["merged_proposal_digest"] = merged_digest.map(Value::String).unwrap_or(Value::Null);
    record["status"] = json!(if next_round == 2 {
        "round_2_open"
    } else {
        "merge_available"
    });
    record["state_revision"] = json!(next_state_revision(&record)?);
    record["last_manager_reason"] = json!(manager_reason);
    record["updated_at_ms"] = json!(now);
    store_projection(tx, &record)?;
    let mut receipt = mutation_receipt(operation_id, &record, true);
    receipt["round_packet_batch_digest"] = json!(batch_digest);
    Ok((receipt, record))
}

fn close(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: &CloseRequest,
    operation_id: &str,
    now: i64,
) -> Result<(Value, Value)> {
    let mut record = load_projection(tx, &request.concilium_id)?;
    require_current_manager(tx, principal, &record)?;
    if record["state_revision"].as_i64() != Some(request.expected_state_revision) {
        return Err(Error::new(
            "STALE_CONCILIUM_REVISION",
            "expected_state_revision does not match the current Concilium revision",
        ));
    }
    if matches!(
        record["status"].as_str(),
        Some("completed" | "unresolved" | "cancelled" | "failed")
    ) {
        return Err(Error::conflict("Concilium is already closed"));
    }
    let result = request.result.as_str();
    let recommendation = &request.recommendation;
    let manager_reason = &request.manager_reason;
    let valid_slots = latest_position_slots(&record)?;
    if matches!(result, "recommended" | "minority_report") && valid_slots.is_empty() {
        return Err(Error::invalid(
            "a completed advisory result requires at least one valid position",
        ));
    }
    let position_operation_ids: Vec<Value> = valid_slots
        .iter()
        .filter_map(|slot| slot.get("response_operation_id").cloned())
        .collect();
    let dissent_slot_ids: Vec<Value> = valid_slots
        .iter()
        .filter(|slot| slot["position"]["position"] != "support")
        .filter_map(|slot| slot.get("slot_id").cloned())
        .collect();
    let unresolved_questions = collect_unresolved_questions(&valid_slots);
    let valid_position_count = valid_slots.len();
    let rounds_used = record["current_round"].as_i64().unwrap_or(0);
    let state = match result {
        "recommended" | "minority_report" => "completed",
        "insufficient_evidence" | "irreconcilable_contract" => "unresolved",
        "cancelled" => "cancelled",
        "failed" => "failed",
        _ => unreachable!(),
    };
    drop(valid_slots);
    close_open_rounds_and_pending_slots(&mut record, now)?;
    record["manager_result"] = json!({
        "class":result,
        "recommendation":recommendation,
        "manager_operation_id":operation_id,
        "position_operation_ids":position_operation_ids,
        "dissent_slot_ids":dissent_slot_ids,
        "unresolved_questions":unresolved_questions,
        "valid_position_count":valid_position_count,
        "rounds_used":rounds_used,
        "closed_by":{"client_id":principal.client_id,"role":role_name(&principal.role)},
        "manager_reason":manager_reason,
        "advisory_only":true,
    });
    record["status"] = json!(state);
    record["closed_at_ms"] = json!(now);
    record["updated_at_ms"] = json!(now);
    record["state_revision"] = json!(next_state_revision(&record)?);
    let active_key = active_subject_key(&record)?;
    if meta(tx, &active_key)?.as_ref().and_then(Value::as_str) == record["concilium_id"].as_str() {
        tx.execute("DELETE FROM meta WHERE key=?1", [active_key])?;
    }
    store_projection(tx, &record)?;
    Ok((mutation_receipt(operation_id, &record, true), record))
}

fn build_preview(db: &Connection, principal: &Principal, record: &Value) -> Result<Value> {
    require_current_manager(db, principal, record)?;
    let scope = &record["scope"];
    let task_id = model::text(scope, "task_id")?;
    let task_revision = model::positive(scope, "task_revision")?;
    let attempt_id = model::text(scope, "attempt_id")?;
    let current =
        coordination::concilium_manager_scope(db, principal, task_id, task_revision, attempt_id)?;
    // The retained sponsor is immutable provenance. `manager_scope` validates
    // the caller's current Task/Attempt authority, so a legitimate ownership
    // handover does not strand this advisory projection.
    let task = current["task"].clone();
    if task["revision"] != task_revision || !value_text_is(&task["task_id"], task_id) {
        return Err(Error::new(
            "STALE_CONCILIUM_PREVIEW",
            "the exact Task revision changed after proposal",
        ));
    }
    let proposal = &record["proposal"];
    verify_proposal_digest_source(db, record)?;
    let evidence_refs: Vec<String> = serde_json::from_value(proposal["evidence_refs"].clone())?;
    validate_evidence_refs(db, &evidence_refs, &task, attempt_id)?;
    let participants = proposal["participants"]
        .as_array()
        .ok_or_else(|| damaged("proposal participant snapshots are missing"))?;
    if participants.is_empty() {
        return Err(damaged("proposal participant roster is empty"));
    }
    let mut validated_participants = Vec::with_capacity(participants.len());
    let mut seen = BTreeSet::new();
    for participant in participants {
        let client_id = model::text(&participant["actor"], "client_id")?;
        if !seen.insert(client_id.to_owned()) {
            return Err(damaged(
                "proposal participant roster contains a duplicate identity",
            ));
        }
        let fresh = coordination::concilium_participant_scope_for_client(
            db,
            client_id,
            task_id,
            task_revision,
            attempt_id,
        )?;
        if fresh["actor"] != participant["actor"]
            || fresh["scope"] != participant["scope"]
            || fresh["participation_basis"] != participant["participation_basis"]
            || fresh["registration_fingerprint"] != participant["registration_fingerprint"]
        {
            return Err(Error::new(
                "STALE_CONCILIUM_PREVIEW",
                format!("participant grant changed for {client_id}"),
            ));
        }
        validated_participants.push(participant.clone());
    }
    let packet = round_one_packet(record, &task)?;
    let packet_bytes = model::canonical(&packet)?.len();
    if packet_bytes > MAX_PACKET_BYTES {
        return Err(Error::new(
            "PAYLOAD_TOO_LARGE",
            "Concilium round-one packet exceeds its inline bound",
        ));
    }
    let packet_digest = sha256_digest(model::canonical(&packet)?.as_bytes());
    let task_digest = sha256_digest(model::canonical(&task["brief"])?.as_bytes());
    let participant_digest =
        sha256_digest(model::canonical(&json!(validated_participants))?.as_bytes());
    let mut warnings = Vec::new();
    if participants.len() > 4 {
        warnings.push("participant_count_above_four");
    }
    if participants.len() == 1 {
        warnings.push("single_participant");
    }
    if participants.len() > MODEL_EXECUTION_CAP {
        warnings.push("written_positions_exceed_model_execution_cap");
    }
    let mut plan = json!({
        "schema_version":1,
        "concilium_id":record["concilium_id"],
        "proposal_operation_id":record["proposal_operation_id"],
        "manager_id":current["attempt"]["owner_id"],
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "attempt_digest":current["attempt"]["digest"],
        "task_digest":task_digest,
        "proposal_digest":proposal["digest"],
        "participant_digest":participant_digest,
        "participants":validated_participants,
        "participant_count":participants.len(),
        "model_execution_cap":MODEL_EXECUTION_CAP,
        "model_dispatch":false,
        "rounds":[
            {"round":1,"kind":"blind_positions","packet_digest":packet_digest,"packet_bytes":packet_bytes},
            {"round":2,"kind":"cross_review","planned":proposal["suggested_max_rounds"].as_i64().unwrap_or(2)>=2},
            {"round":3,"kind":"merge_review","explicit_changed_digest_required":true,"planned":proposal["suggested_max_rounds"]==3},
        ],
        "round_1_packet":packet,
        "round_1_packet_digest":packet_digest,
        "warnings":warnings,
    });
    let plan_bytes = model::canonical(&plan)?;
    if plan_bytes.len() > MAX_STATE_BYTES {
        return Err(Error::new(
            "PAYLOAD_TOO_LARGE",
            "Concilium preview plan exceeds its deterministic bound",
        ));
    }
    plan["plan_digest"] = json!(sha256_digest(plan_bytes.as_bytes()));
    plan["state_revision"] = record["state_revision"].clone();
    Ok(plan)
}

fn verify_proposal_digest_source(db: &Connection, record: &Value) -> Result<()> {
    let proposal = &record["proposal"];
    let source = &proposal["request_source"];
    let request = source
        .get("request")
        .filter(|value| value.is_object())
        .ok_or_else(|| damaged("proposal digest source has no canonical request"))?;
    if request.get("client_request_id").is_some()
        || source["participants"] != proposal["participants"]
        || source["proposer"]["actor"] != proposal["proposer"]
        || source["proposer"]["scope"] != proposal["proposer_scope"]
        || source["proposer"]["participation_basis"] != proposal["proposer_participation_basis"]
        || source["proposer"]["registration_fingerprint"]
            != proposal["proposer_registration_fingerprint"]
    {
        return Err(damaged(
            "proposal digest source differs from its retained roster or proposer",
        ));
    }
    let proposal_operation_id = model::text(record, "proposal_operation_id")?;
    let (method, caller_id, original_request): (String, String, String) = db.query_row(
        "SELECT method,caller_id,original_request_json FROM operations WHERE operation_id=?1",
        [proposal_operation_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let mut original_request: Value = serde_json::from_str(&original_request)?;
    let original_object = original_request
        .as_object_mut()
        .ok_or_else(|| damaged("proposal Operation request is not an object"))?;
    original_object.remove("client_request_id");
    if method != "concilium.propose"
        || original_request != *request
        || proposal["proposer"]["client_id"].as_str() != Some(caller_id.as_str())
    {
        return Err(damaged(
            "proposal digest source differs from its originating admitted Operation",
        ));
    }
    let bytes = model::canonical(source)?;
    let expected_digest = sha256_digest(bytes.as_bytes());
    if bytes.len() > MAX_STATE_BYTES || !value_text_is(&proposal["digest"], &expected_digest) {
        return Err(damaged(
            "retained proposal digest does not match its canonical source",
        ));
    }
    Ok(())
}

fn round_one_packet(record: &Value, task: &Value) -> Result<Value> {
    let proposal = &record["proposal"];
    let requirements = task["brief"]
        .get("requirements")
        .cloned()
        .unwrap_or(Value::Null);
    let non_goals = task["brief"]
        .get("non_goals")
        .cloned()
        .unwrap_or(Value::Null);
    let packet = json!({
        "schema_version":1,
        "concilium_id":record["concilium_id"],
        "round":1,
        "task":{
            "task_id":record["scope"]["task_id"],
            "task_revision":record["scope"]["task_revision"],
            "attempt_id":record["scope"]["attempt_id"],
            "requirements":requirements,
            "non_goals":non_goals,
            "task_brief_digest":sha256_digest(model::canonical(&task["brief"])?.as_bytes()),
        },
        "decision_question":proposal["decision_question"],
        "material_conflict":proposal["material_conflict"],
        "proposal_revision_ids":proposal["proposal_revision_ids"],
        "evidence_refs":proposal["evidence_refs"],
        "expected_output":proposal["expected_output"],
        "close_condition":proposal["close_condition"],
        "max_rounds":proposal["suggested_max_rounds"],
        "response_schema":{
            "position":["support","oppose","alternative","insufficient_evidence"],
            "claims":["claim_id","stance","fact","evidence_refs","counterexample","falsifier","assumptions","confidence"],
            "required_change":"string",
            "unresolved_questions":"string[]",
        },
        "blind":true,
    });
    Ok(packet)
}

fn build_next_round_packets(
    record: &Value,
    next_round: i64,
    merged_digest: Option<&str>,
) -> Result<Vec<(Value, Value)>> {
    let source_round = next_round - 1;
    let prior: Vec<&Value> = record["slots"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|slot| slot["round"].as_i64() == Some(source_round))
        .collect();
    if prior.is_empty() || prior.iter().any(|slot| slot["state"] != "submitted") {
        return Err(Error::conflict(
            "every position in the preceding round must be submitted before advance",
        ));
    }
    let mut result = Vec::with_capacity(prior.len());
    for own in &prior {
        let own_id = model::text(&own["participant_actor"], "client_id")?;
        let peer_summaries: Vec<Value> = prior
            .iter()
            .filter(|peer| !value_text_is(&peer["participant_actor"]["client_id"], &own_id))
            .map(|peer| position_summary(peer))
            .collect();
        let packet = json!({
            "schema_version":1,
            "concilium_id":record["concilium_id"],
            "round":next_round,
            "decision_question":record["proposal"]["decision_question"],
            "material_conflict":record["proposal"]["material_conflict"],
            "proposal_revision_ids":record["proposal"]["proposal_revision_ids"],
            "evidence_refs":record["proposal"]["evidence_refs"],
            "merged_proposal_digest":merged_digest.map(|digest| Value::String(digest.to_owned())).unwrap_or(Value::Null),
            "merged_proposal_digest_basis":if next_round == 3 {
                json!("manager_attested_reference")
            } else {
                Value::Null
            },
            "review_of":peer_summaries,
            "response_schema":{
                "position":["support","oppose","alternative","insufficient_evidence"],
                "claims":["claim_id","stance","fact","evidence_refs","counterexample","falsifier","assumptions","confidence"],
                "required_change":"string",
                "unresolved_questions":"string[]",
            },
            "blind":false,
        });
        let bytes = model::canonical(&packet)?;
        if bytes.len() > MAX_PACKET_BYTES {
            return Err(Error::new(
                "PAYLOAD_TOO_LARGE",
                "bounded cross-review packet exceeds its inline byte limit",
            ));
        }
        let participant = json!({
            "participant_actor":own["participant_actor"],
            "participant_scope":own["participant_scope"],
            "participation_basis":own["participation_basis"],
            "registration_fingerprint":own["registration_fingerprint"],
            "reason":own["reason"],
        });
        result.push((participant, packet));
    }
    Ok(result)
}

fn position_summary(slot: &Value) -> Value {
    let position = &slot["position"];
    let claims: Vec<Value> = position["claims"]
        .as_array()
        .into_iter()
        .flatten()
        .take(2)
        .map(|claim| json!({
            "claim_id":claim["claim_id"],
            "stance":claim["stance"],
            "fact":truncate_text(claim["fact"].as_str().unwrap_or_default(), 128),
            "counterexample":claim.get("counterexample").and_then(Value::as_str).map(|text|truncate_text(text,128)),
        }))
        .collect();
    json!({
        "participant_actor":slot["participant_actor"],
        "position":position["position"],
        "proposal_revision_id":position["proposal_revision_id"],
        "claims":claims,
        "claims_truncated":position["claims"].as_array().is_some_and(|all|all.len()>2),
        "required_change":truncate_text(position["required_change"].as_str().unwrap_or_default(), 256),
        "unresolved_questions":position["unresolved_questions"].as_array().into_iter().flatten().take(2).filter_map(Value::as_str).map(|text|truncate_text(text,128)).collect::<Vec<_>>(),
    })
}

fn new_slot(round: i64, participant: &Value, packet: &Value, packet_digest: &str) -> Result<Value> {
    let bytes = model::canonical(packet)?.len();
    if bytes > MAX_PACKET_BYTES {
        return Err(Error::new(
            "PAYLOAD_TOO_LARGE",
            "Concilium slot packet exceeds its inline bound",
        ));
    }
    Ok(json!({
        "slot_id":model::new_id(),
        "round":round,
        "participant_actor":participant["actor"].as_object().map(|_|participant["actor"].clone()).unwrap_or_else(||participant["participant_actor"].clone()),
        "participant_scope":participant.get("scope").cloned().unwrap_or_else(||participant["participant_scope"].clone()),
        "participation_basis":participant["participation_basis"],
        "registration_fingerprint":participant["registration_fingerprint"],
        "reason":participant["reason"],
        "packet_digest":packet_digest,
        "packet":packet,
        "state":"pending",
        "response_operation_id":Value::Null,
        "position":Value::Null,
        "source_result_ref":Value::Null,
    }))
}

fn round_batch_digest(round: i64, slots: &[Value]) -> Result<String> {
    let mut slot_digests: Vec<(String, String)> = slots
        .iter()
        .map(|slot| {
            Ok((
                model::text(slot, "slot_id")?.to_owned(),
                model::text(slot, "packet_digest")?.to_owned(),
            ))
        })
        .collect::<Result<_>>()?;
    slot_digests.sort();
    Ok(sha256_digest(
        model::canonical(&json!({"round":round,"slots":slot_digests}))?.as_bytes(),
    ))
}

fn require_current_manager(
    db: &Connection,
    principal: &Principal,
    record: &Value,
) -> Result<Value> {
    let scope = &record["scope"];
    let current = coordination::concilium_manager_scope(
        db,
        principal,
        model::text(scope, "task_id")?,
        model::positive(scope, "task_revision")?,
        model::text(scope, "attempt_id")?,
    )?;
    Ok(current)
}

fn require_exact_live_slot(
    db: &Connection,
    principal: &Principal,
    record: &Value,
    slot: &Value,
    packet_digest: &str,
) -> Result<()> {
    principal.require_participant()?;
    if is_terminal(&record["status"]) {
        return Err(Error::conflict(
            "a terminal Concilium cannot accept another position",
        ));
    }
    if !value_text_is(
        &slot["participant_actor"]["client_id"],
        &principal.client_id,
    ) || slot["participant_actor"]["role"] != "participant"
    {
        return Err(Error::new(
            "FORBIDDEN",
            "a Participant may submit only its exact recorded slot",
        ));
    }
    if !value_text_is(&slot["packet_digest"], packet_digest) {
        return Err(Error::new(
            "CONCILIUM_PACKET_MISMATCH",
            "packet_digest does not match the participant's exact slot packet",
        ));
    }
    let scope = &record["scope"];
    let fresh = coordination::concilium_participant_scope_for_client(
        db,
        &principal.client_id,
        model::text(scope, "task_id")?,
        model::positive(scope, "task_revision")?,
        model::text(scope, "attempt_id")?,
    )?;
    if fresh["actor"] != slot["participant_actor"]
        || fresh["scope"] != slot["participant_scope"]
        || fresh["participation_basis"] != slot["participation_basis"]
        || fresh["registration_fingerprint"] != slot["registration_fingerprint"]
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "current Participant grant no longer matches this Concilium slot",
        ));
    }
    Ok(())
}

fn round_is_open(record: &Value) -> bool {
    let current = record["current_round"].as_i64();
    record["rounds"].as_array().is_some_and(|rounds| {
        rounds
            .iter()
            .any(|round| round["round"].as_i64() == current && round["status"] == "open")
    })
}

fn seal_round(record: &mut Value, round: i64, now: i64) -> Result<()> {
    let rounds = record["rounds"]
        .as_array_mut()
        .ok_or_else(|| damaged("Concilium round history is missing"))?;
    let Some(entry) = rounds
        .iter_mut()
        .find(|entry| entry["round"].as_i64() == Some(round))
    else {
        return Err(damaged("current Concilium round is missing"));
    };
    if entry["status"] == "open" {
        entry["status"] = json!("ready");
        entry["closed_at_ms"] = json!(now);
    }
    Ok(())
}

fn close_open_rounds_and_pending_slots(record: &mut Value, now: i64) -> Result<()> {
    let rounds = record["rounds"]
        .as_array_mut()
        .ok_or_else(|| damaged("Concilium round history is missing"))?;
    for round in rounds {
        if round["status"] == "open" {
            round["status"] = json!("closed");
            round["closed_at_ms"] = json!(now);
        }
    }
    let slots = record["slots"]
        .as_array_mut()
        .ok_or_else(|| damaged("Concilium slot history is missing"))?;
    for slot in slots {
        if slot["state"] == "pending" {
            slot["state"] = json!("cancelled");
            slot["closed_at_ms"] = json!(now);
        }
    }
    Ok(())
}

fn latest_position_slots(record: &Value) -> Result<Vec<&Value>> {
    let slots = record["slots"]
        .as_array()
        .ok_or_else(|| damaged("Concilium slot history is missing"))?;
    let mut latest = BTreeMap::<String, (i64, &Value)>::new();
    let mut seen_rounds = BTreeSet::new();
    for slot in slots {
        if slot["state"] != "submitted" {
            continue;
        }
        if !slot["position"].is_object() {
            return Err(damaged("submitted Concilium slot has no retained position"));
        }
        let participant_id = model::text(&slot["participant_actor"], "client_id")?.to_owned();
        let round = model::positive(slot, "round")?;
        model::text(slot, "response_operation_id")?;
        model::text(slot, "slot_id")?;
        if !seen_rounds.insert((participant_id.clone(), round)) {
            return Err(damaged(
                "Participant has multiple submitted slots in one Concilium round",
            ));
        }
        if let Some((latest_round, _)) = latest.get(&participant_id) {
            if *latest_round > round {
                continue;
            }
        }
        latest.insert(participant_id, (round, slot));
    }
    Ok(latest.into_values().map(|(_, slot)| slot).collect())
}

fn latest_positions_view(record: &Value, only_client_id: Option<&str>) -> Result<Vec<Value>> {
    latest_position_slots(record)?
        .into_iter()
        .filter(|slot| {
            only_client_id.is_none_or(|client_id| {
                value_text_is(&slot["participant_actor"]["client_id"], client_id)
            })
        })
        .map(|slot| {
            Ok(json!({
                "participant_actor":slot["participant_actor"],
                "round":slot["round"],
                "slot_id":model::text(slot, "slot_id")?,
                "response_operation_id":model::text(slot, "response_operation_id")?,
                "packet_digest":slot["packet_digest"],
                "position":slot["position"],
            }))
        })
        .collect()
}

fn next_state_revision(record: &Value) -> Result<i64> {
    record["state_revision"]
        .as_i64()
        .and_then(|revision| revision.checked_add(1))
        .filter(|revision| *revision > 1)
        .ok_or_else(|| damaged("Concilium state revision is invalid or exhausted"))
}

fn coalesce_or_create(tx: &Transaction<'_>, mut candidate: Value) -> Result<(Value, bool)> {
    let key = active_subject_key(&candidate)?;
    if let Some(existing_id) = meta(tx, &key)?.and_then(|value| value.as_str().map(str::to_owned)) {
        match load_projection(tx, &existing_id) {
            Ok(existing) if !is_terminal(&existing["status"]) => {
                if existing["manager_id"] != candidate["manager_id"] {
                    return Err(Error::new(
                        "CONCILIUM_CONFLICT",
                        "an active Concilium for this Task, Attempt, and question has a different recorded owner",
                    ));
                }
                if existing["proposal"]["digest"] != candidate["proposal"]["digest"] {
                    return Err(Error::new(
                        "CONCILIUM_CONFLICT",
                        "an active Concilium for this Task, Attempt, and question has different proposal content",
                    ));
                }
                return Ok((existing, false));
            }
            Ok(_) => tx.execute("DELETE FROM meta WHERE key=?1", [&key])?,
            Err(error) if error.code == "NOT_FOUND" => {
                tx.execute("DELETE FROM meta WHERE key=?1", [&key])?
            }
            Err(error) => return Err(error),
        };
    }
    candidate["active_subject_key"] = json!(key);
    store_projection(tx, &candidate)?;
    set_meta(
        tx,
        &key,
        &json!(candidate["concilium_id"].as_str().unwrap_or_default()),
    )?;
    Ok((candidate, true))
}

fn active_subject_key(record: &Value) -> Result<String> {
    let task_id = model::text(&record["scope"], "task_id")?;
    let attempt_id = model::text(&record["scope"], "attempt_id")?;
    let question = model::text(&record["proposal"], "decision_question")?
        .trim()
        .to_lowercase();
    let subject = model::canonical(&json!({
        "task_id":task_id,
        "attempt_id":attempt_id,
        "decision_question":question,
    }))?;
    Ok(format!(
        "concilium:v1:active:{}",
        model::digest(subject.as_bytes())
    ))
}

fn is_terminal(status: &Value) -> bool {
    matches!(
        status.as_str(),
        Some("completed" | "unresolved" | "cancelled" | "failed")
    )
}

fn index_projection(tx: &Transaction<'_>, record: &Value) -> Result<()> {
    let id = model::text(record, "concilium_id")?;
    let scope = &record["scope"];
    let task_id = model::text(scope, "task_id")?;
    let attempt_id = model::text(scope, "attempt_id")?;
    let mut viewers = BTreeSet::new();
    viewers.insert(model::text(record, "manager_id")?.to_owned());
    if let Some(proposer) = record["proposal"]["proposer"]["client_id"].as_str() {
        viewers.insert(proposer.to_owned());
    }
    for participant in record["proposal"]["participants"]
        .as_array()
        .into_iter()
        .flatten()
    {
        if let Some(client_id) = participant["actor"]["client_id"].as_str() {
            viewers.insert(client_id.to_owned());
        }
    }
    let keys = [
        format!("{}{id}", task_index_prefix(task_id)),
        format!("{}{id}", scope_index_prefix(task_id, attempt_id)?),
    ];
    for key in keys {
        set_meta(tx, &key, &json!({"concilium_id":id}))?;
    }
    for viewer in viewers {
        set_meta(
            tx,
            &format!("{}{id}", viewer_index_prefix(&viewer)),
            &json!({"concilium_id":id}),
        )?;
    }
    Ok(())
}

fn store_projection(tx: &Transaction<'_>, record: &Value) -> Result<()> {
    verify_projection(record)?;
    if model::canonical(record)?.len() > MAX_STATE_BYTES {
        return Err(Error::new(
            "PAYLOAD_TOO_LARGE",
            "Concilium state exceeds its bounded projection size",
        ));
    }
    set_meta(
        tx,
        &projection_key(model::text(record, "concilium_id")?),
        record,
    )
}

fn load_projection(db: &Connection, id: &str) -> Result<Value> {
    validate_identifier(id, "concilium_id", limits::MAX_IDENTIFIER_BYTES)?;
    let record = meta(db, &projection_key(id))?
        .ok_or_else(|| Error::new("NOT_FOUND", "Concilium was not found"))?;
    verify_projection(&record)?;
    if !value_text_is(&record["concilium_id"], id) {
        return Err(damaged(
            "Concilium projection key does not match its identity",
        ));
    }
    verify_proposal_digest_source(db, &record)?;
    Ok(record)
}

fn record_for_operation(db: &Connection, operation_id: &str) -> Result<Value> {
    let link = meta(db, &operation_key(operation_id))?
        .ok_or_else(|| Error::new("NOT_FOUND", "Concilium Operation link was not found"))?;
    if link["schema_version"] != 1
        || link["task_id"].as_str().is_none()
        || link["attempt_id"].as_str().is_none()
        || link["manager_id"].as_str().is_none()
        || link["proposal_operation_id"].as_str().is_none()
    {
        return Err(damaged("Concilium Operation link is incomplete"));
    }
    let id = model::text(&link, "concilium_id")?;
    let record = load_projection(db, id)?;
    if record["scope"]["task_id"] != link["task_id"]
        || record["scope"]["attempt_id"] != link["attempt_id"]
        || record["manager_id"] != link["manager_id"]
        || record["proposal_operation_id"] != link["proposal_operation_id"]
    {
        return Err(damaged(
            "Concilium Operation link differs from its projection",
        ));
    }
    Ok(record)
}

fn retain_operation_link(tx: &Transaction<'_>, operation_id: &str, record: &Value) -> Result<()> {
    set_meta(
        tx,
        &operation_key(operation_id),
        &json!({
            "schema_version":1,
            "concilium_id":record["concilium_id"],
            "proposal_operation_id":record["proposal_operation_id"],
            "manager_id":record["manager_id"],
            "task_id":record["scope"]["task_id"],
            "attempt_id":record["scope"]["attempt_id"],
        }),
    )
}

fn verify_admitted_operation(
    tx: &Transaction<'_>,
    principal: &Principal,
    method: &str,
    value: &Value,
    operation_id: &str,
) -> Result<()> {
    let row: Option<(String, String, String, String)> = tx
        .query_row(
            "SELECT caller_id,client_request_id,method,original_request_json FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((caller_id, request_id, admitted_method, original_request)) = row else {
        return Err(Error::new(
            "CONCILIUM_OPERATION_MISMATCH",
            "Concilium mutation has no admitted Operation",
        ));
    };
    let original_request: Value = serde_json::from_str(&original_request)?;
    if caller_id != principal.client_id
        || admitted_method != method
        || request_id != model::text(value, "client_request_id")?
        || original_request != *value
    {
        return Err(Error::new(
            "CONCILIUM_OPERATION_MISMATCH",
            "Concilium request does not match its exact admitted Operation and client_request_id",
        ));
    }
    Ok(())
}

fn update_operation_scope(
    tx: &Transaction<'_>,
    caller_id: &str,
    method: &str,
    operation_id: &str,
    record: &Value,
) -> Result<()> {
    let changed = tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1 AND caller_id=?4 AND method=?5",
        params![
            operation_id,
            record["scope"]["task_id"].as_str(),
            record["scope"]["attempt_id"].as_str(),
            caller_id,
            method,
        ],
    )?;
    if changed != 1 {
        return Err(Error::new(
            "CONCILIUM_OPERATION_MISMATCH",
            "Concilium projection could not stamp its trusted Task and Attempt onto the admitted Operation",
        ));
    }
    Ok(())
}

fn mutation_receipt(operation_id: &str, record: &Value, changed: bool) -> Value {
    json!({
        "operation_id":operation_id,
        "concilium_id":record["concilium_id"],
        "proposal_operation_id":record["proposal_operation_id"],
        "task_id":record["scope"]["task_id"],
        "task_revision":record["scope"]["task_revision"],
        "attempt_id":record["scope"]["attempt_id"],
        "status":record["status"],
        "state_revision":record["state_revision"],
        "changed":changed,
    })
}

fn verify_projection(record: &Value) -> Result<()> {
    if record["schema"] != SCHEMA
        || model::text(record, "concilium_id").is_err()
        || model::text(record, "proposal_operation_id").is_err()
        || model::text(record, "manager_id").is_err()
        || model::text(&record["scope"], "task_id").is_err()
        || model::positive(&record["scope"], "task_revision").is_err()
        || model::text(&record["scope"], "attempt_id").is_err()
        || record["state_revision"]
            .as_i64()
            .is_none_or(|revision| revision <= 0)
        || !record["slots"].is_array()
        || !record["rounds"].is_array()
    {
        return Err(damaged("Concilium projection is incomplete or invalid"));
    }
    if !matches!(
        record["status"].as_str(),
        Some(
            "proposed"
                | "planned"
                | "round_1_open"
                | "round_1_ready"
                | "round_2_open"
                | "round_2_ready"
                | "merge_available"
                | "completed"
                | "unresolved"
                | "cancelled"
                | "failed"
        )
    ) {
        return Err(damaged("Concilium projection has an unsupported state"));
    }
    Ok(())
}

fn projection_key(id: &str) -> String {
    format!("{PROJECTION_PREFIX}{id}")
}

fn operation_key(id: &str) -> String {
    format!("{OPERATION_PREFIX}{id}")
}

fn task_index_prefix(task_id: &str) -> String {
    format!("{TASK_INDEX_PREFIX}{}:", model::digest(task_id.as_bytes()))
}

fn scope_index_prefix(task_id: &str, attempt_id: &str) -> Result<String> {
    let scope = model::canonical(&json!({"task_id":task_id,"attempt_id":attempt_id}))?;
    Ok(format!(
        "{SCOPE_INDEX_PREFIX}{}:",
        model::digest(scope.as_bytes())
    ))
}

fn viewer_index_prefix(client_id: &str) -> String {
    format!(
        "{VIEWER_INDEX_PREFIX}{}:",
        model::digest(client_id.as_bytes())
    )
}

fn list(db: &Connection, principal: &Principal, request: &ListRequest) -> Result<Value> {
    let task_id = &request.task_id;
    let state = request.state.as_deref();
    let limit = request.limit;
    let after = request.after_concilium_id.as_deref();
    if principal.role == Role::Operator {
        super::require_local_operator(db, &principal.client_id)?;
    }
    if principal.role == Role::Manager {
        crate::automation::authorization::require_registered_manager(db, &principal.client_id)?;
    }
    let task_wide_reader = principal.role == Role::Operator
        || (principal.role == Role::Manager && super::gm::require_authority(db, principal).is_ok());
    let current_scope_attempt = if principal.role == Role::Manager && !task_wide_reader {
        current_task_scope_attempt(db, principal, task_id, request.attempt_id.as_deref())
    } else {
        None
    };
    let effective_attempt = current_scope_attempt
        .as_deref()
        .or(request.attempt_id.as_deref());
    let prefix = if task_wide_reader {
        match effective_attempt {
            Some(attempt) => scope_index_prefix(task_id, attempt)?,
            None => task_index_prefix(task_id),
        }
    } else if let Some(attempt) = current_scope_attempt.as_deref() {
        scope_index_prefix(task_id, attempt)?
    } else {
        viewer_index_prefix(&principal.client_id)
    };
    let lower = after
        .as_deref()
        .map(|id| format!("{prefix}{id}"))
        .unwrap_or_else(|| prefix.clone());
    let compare = if after.is_some() { ">" } else { ">=" };
    let upper = format!("{prefix}~");
    let sql = format!(
        "SELECT key,value_json FROM meta WHERE key {compare} ?1 AND key < ?2 ORDER BY key LIMIT ?3"
    );
    let mut statement = db.prepare(&sql)?;
    let rows: Vec<(String, String)> = statement
        .query_map(params![lower, upper, limit.saturating_add(1)], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<std::result::Result<_, _>>()?;
    let has_more = rows.len() as i64 > limit;
    let scanned = rows.iter().take(limit as usize);
    let mut items = Vec::new();
    let mut last_scanned = None;
    for (key, raw_index) in scanned {
        let index: Value = serde_json::from_str(raw_index)?;
        let id = model::text(&index, "concilium_id")?;
        let record = load_projection(db, id)?;
        authorize_projection_reader(db, principal, &record)?;
        last_scanned = key.rsplit(':').next().map(str::to_owned);
        if !value_text_is(&record["scope"]["task_id"], task_id)
            || effective_attempt
                .is_some_and(|attempt| !value_text_is(&record["scope"]["attempt_id"], attempt))
            || state
                .as_deref()
                .is_some_and(|expected| !value_text_is(&record["status"], expected))
        {
            continue;
        }
        items.push(list_item(&record));
    }
    Ok(json!({
        "items":items,
        "task_id":task_id,
        "attempt_id":effective_attempt,
        "next_after_concilium_id":if has_more { last_scanned.map(Value::String).unwrap_or(Value::Null) } else { Value::Null },
        "coverage":if has_more { "partial" } else { "complete" },
    }))
}

fn project_page(
    db: &Connection,
    principal: &Principal,
    record: &Value,
    limit: i64,
    after_slot_id: Option<&str>,
) -> Result<Value> {
    let manager_view = is_manager_reader(db, principal, record)?;
    let latest_positions = latest_positions_view(
        record,
        (!manager_view).then_some(principal.client_id.as_str()),
    )?;
    let all_slots = record["slots"]
        .as_array()
        .ok_or_else(|| damaged("Concilium slots are missing"))?;
    let mut slots: Vec<Value> = all_slots
        .iter()
        .filter(|slot| {
            after_slot_id.is_none_or(|after| slot["slot_id"].as_str().is_some_and(|id| id > after))
        })
        .cloned()
        .collect();
    slots.sort_by(|left, right| left["slot_id"].as_str().cmp(&right["slot_id"].as_str()));
    let has_more = slots.len() > limit as usize;
    slots.truncate(limit as usize);
    if !manager_view {
        for slot in &mut slots {
            if !value_text_is(
                &slot["participant_actor"]["client_id"],
                &principal.client_id,
            ) {
                if let Some(object) = slot.as_object_mut() {
                    object.remove("position");
                    object.remove("packet");
                    object.remove("source_result_ref");
                    object.remove("participant_scope");
                    object.remove("participation_basis");
                    object.remove("registration_fingerprint");
                }
            }
        }
    }
    let next_after = if has_more {
        slots
            .last()
            .and_then(|slot| slot.get("slot_id"))
            .cloned()
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    let mut projection = record.clone();
    projection["slots"] = json!(slots);
    projection["latest_positions"] = json!(latest_positions);
    projection["next_after_slot_id"] = next_after;
    projection["coverage"] = json!(if has_more { "partial" } else { "complete" });
    if !manager_view {
        projection["my_slots"] = json!(
            slots
                .iter()
                .filter(|slot| value_text_is(
                    &slot["participant_actor"]["client_id"],
                    &principal.client_id
                ))
                .cloned()
                .collect::<Vec<_>>()
        );
        sanitize_participant_projection(&mut projection);
    }
    Ok(projection)
}

fn sanitize_participant_projection(record: &mut Value) {
    if let Some(participants) = record["plan"]["participants"].as_array_mut() {
        for participant in participants {
            if let Some(object) = participant.as_object_mut() {
                object.remove("scope");
                object.remove("participation_basis");
                object.remove("registration_fingerprint");
            }
        }
    }
    if let Some(participants) = record["proposal"]["participants"].as_array_mut() {
        for participant in participants {
            if let Some(object) = participant.as_object_mut() {
                object.remove("scope");
                object.remove("participation_basis");
                object.remove("registration_fingerprint");
            }
        }
    }
    if let Some(object) = record["proposal"].as_object_mut() {
        object.remove("request_source");
        object.remove("proposer_scope");
        object.remove("proposer_participation_basis");
        object.remove("proposer_registration_fingerprint");
    }
}

fn list_item(record: &Value) -> Value {
    json!({
        "concilium_id":record["concilium_id"],
        "proposal_operation_id":record["proposal_operation_id"],
        "status":record["status"],
        "state_revision":record["state_revision"],
        "scope":record["scope"],
        "decision_question":record["proposal"]["decision_question"],
        "participant_count":record["proposal"]["participants"].as_array().map(Vec::len).unwrap_or(0),
        "current_round":record["current_round"],
        "created_at_ms":record["created_at_ms"],
        "updated_at_ms":record["updated_at_ms"],
        "advisory_only":true,
    })
}

fn authorize_projection_reader(
    db: &Connection,
    principal: &Principal,
    record: &Value,
) -> Result<()> {
    match principal.role {
        Role::Operator => super::require_local_operator(db, &principal.client_id),
        Role::Manager => {
            crate::automation::authorization::require_registered_manager(db, &principal.client_id)?;
            if value_text_is(&record["manager_id"], &principal.client_id)
                || has_current_task_scope_reader(
                    db,
                    principal,
                    model::text(&record["scope"], "task_id")?,
                    Some(model::text(&record["scope"], "attempt_id")?),
                )
                || super::gm::require_authority(db, principal).is_ok()
            {
                Ok(())
            } else {
                Err(Error::new(
                    "FORBIDDEN",
                    "retained Concilium read requires its recorded sponsor, current Attempt Manager, or current GM",
                ))
            }
        }
        Role::Participant => {
            let snapshot = participant_snapshot(record, &principal.client_id).ok_or_else(|| {
                Error::new(
                    "FORBIDDEN",
                    "Participant is not in the retained Concilium read scope",
                )
            })?;
            verify_retained_registration(db, &principal.client_id, &record["scope"], &snapshot)
        }
        _ => Err(Error::new(
            "FORBIDDEN",
            "this identity has no retained Concilium read scope",
        )),
    }
}

fn is_manager_reader(db: &Connection, principal: &Principal, record: &Value) -> Result<bool> {
    if principal.role == Role::Operator {
        super::require_local_operator(db, &principal.client_id)?;
        return Ok(true);
    }
    if principal.role == Role::Manager {
        crate::automation::authorization::require_registered_manager(db, &principal.client_id)?;
        return Ok(value_text_is(&record["manager_id"], &principal.client_id)
            || has_current_task_scope_reader(
                db,
                principal,
                model::text(&record["scope"], "task_id")?,
                Some(model::text(&record["scope"], "attempt_id")?),
            )
            || super::gm::require_authority(db, principal).is_ok());
    }
    Ok(false)
}

fn has_current_task_scope_reader(
    db: &Connection,
    principal: &Principal,
    task_id: &str,
    attempt_id: Option<&str>,
) -> bool {
    current_task_scope_attempt(db, principal, task_id, attempt_id).is_some()
}

fn current_task_scope_attempt(
    db: &Connection,
    principal: &Principal,
    task_id: &str,
    attempt_id: Option<&str>,
) -> Option<String> {
    let task = tasks::get_task(db, task_id).ok()?;
    let attempt_id = attempt_id.or_else(|| task["current_attempt_id"].as_str())?;
    let task_revision = task["revision"].as_i64()?;
    coordination::concilium_manager_scope(db, principal, task_id, task_revision, attempt_id)
        .ok()?;
    Some(attempt_id.to_owned())
}

fn participant_snapshot(record: &Value, client_id: &str) -> Option<Value> {
    if value_text_is(&record["proposal"]["proposer"]["client_id"], client_id) {
        return Some(json!({
            "actor":record["proposal"]["proposer"],
            "scope":record["proposal"]["proposer_scope"],
            "participation_basis":record["proposal"]["proposer_participation_basis"],
            "registration_fingerprint":record["proposal"]["proposer_registration_fingerprint"],
        }));
    }
    record["slots"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|slot| value_text_is(&slot["participant_actor"]["client_id"], client_id))
        .map(|slot| {
            json!({
                "actor":slot["participant_actor"],
                "scope":slot["participant_scope"],
                "participation_basis":slot["participation_basis"],
                "registration_fingerprint":slot["registration_fingerprint"],
            })
        })
        .or_else(|| {
            record["proposal"]["participants"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|participant| value_text_is(&participant["actor"]["client_id"], client_id))
                .map(|participant| participant.clone())
        })
}

fn verify_registered_participant(db: &Connection, client_id: &str) -> Result<Value> {
    let registration = meta(db, &format!("client:{client_id}"))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "Participant registration no longer exists"))?;
    if registration["role"] != "participant" || registration["disabled"] == true {
        return Err(Error::new(
            "UNAUTHORIZED",
            "Participant readback requires the same enabled registered identity",
        ));
    }
    Ok(registration)
}

fn verify_retained_registration(
    db: &Connection,
    client_id: &str,
    scope: &Value,
    snapshot: &Value,
) -> Result<()> {
    let registration = verify_registered_participant(db, client_id)?;
    let current_fingerprint =
        coordination::concilium_registration_fingerprint(client_id, &registration)?;
    let actor_generation = registration
        .get("binding_generation")
        .cloned()
        .unwrap_or(Value::Null);
    if registration["task_id"] != scope["task_id"]
        || registration["task_revision"] != scope["task_revision"]
        || registration["attempt_id"] != scope["attempt_id"]
        || snapshot["registration_fingerprint"].as_str() != Some(current_fingerprint.as_str())
        || !value_text_is(&snapshot["actor"]["client_id"], client_id)
        || snapshot["actor"]["generation"] != actor_generation
    {
        return Err(Error::new(
            "FORBIDDEN",
            "registered Participant identity does not match the retained exact scope",
        ));
    }
    Ok(())
}

fn resolve_proposal_participants(
    db: &Connection,
    invitees: &[ParticipantRef],
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
) -> Result<Vec<Value>> {
    let mut participants = Vec::with_capacity(invitees.len());
    let mut seen_clients = BTreeSet::new();
    for invitee in invitees {
        let scope = coordination::concilium_participant_scope_for_client(
            db,
            &invitee.client_id,
            task_id,
            task_revision,
            attempt_id,
        )?;
        if invitee
            .generation
            .is_some_and(|generation| scope["actor"]["generation"].as_i64() != Some(generation))
        {
            return Err(Error::new(
                "STALE_PARTICIPANT",
                format!("participant generation changed for {}", invitee.client_id),
            ));
        }
        let client_id = model::text(&scope["actor"], "client_id")?;
        if !seen_clients.insert(client_id.to_owned()) {
            return Err(Error::invalid(
                "participants must resolve to unique registered identities",
            ));
        }
        participants.push(json!({
            "actor":scope["actor"],
            "scope":scope["scope"],
            "participation_basis":scope["participation_basis"],
            "registration_fingerprint":scope["registration_fingerprint"],
            "reason":invitee.reason,
        }));
    }
    Ok(participants)
}

fn validate_position_body(
    db: &Connection,
    position: &ParticipantPosition,
    scope: &Value,
    proposal_revision_ids: &Value,
) -> Result<()> {
    if position
        .proposal_revision_id
        .as_ref()
        .is_some_and(|revision| {
            !proposal_revision_ids
                .as_array()
                .is_some_and(|items| items.iter().any(|item| item == revision))
        })
    {
        return Err(Error::invalid(
            "proposal_revision_id must name a revision included in the proposal",
        ));
    }
    let task = tasks::get_task(db, model::text(scope, "task_id")?)?;
    let attempt_id = model::text(scope, "attempt_id")?;
    for claim in &position.claims {
        validate_evidence_refs(db, &claim.evidence_refs, &task, attempt_id)?;
    }
    Ok(())
}

fn validate_evidence_refs(
    db: &Connection,
    references: &[String],
    task: &Value,
    attempt_id: &str,
) -> Result<()> {
    for reference in references {
        if reference.starts_with("artifact-") || reference.starts_with("artifact:") {
            let id = reference
                .strip_prefix("artifact-")
                .or_else(|| reference.strip_prefix("artifact:"))
                .unwrap_or(&reference);
            validate_artifact_ref(db, id, task, attempt_id, None)?;
        } else if reference.starts_with("submission-") || reference.starts_with("submission:") {
            let id = reference
                .strip_prefix("submission-")
                .or_else(|| reference.strip_prefix("submission:"))
                .unwrap_or(&reference);
            validate_artifact_ref(db, id, task, attempt_id, Some("task_submission"))?;
        } else if reference.starts_with("operation-") || reference.starts_with("operation:") {
            let id = reference
                .strip_prefix("operation-")
                .or_else(|| reference.strip_prefix("operation:"))
                .unwrap_or(&reference);
            validate_operation_ref(db, id, task, attempt_id)?;
        } else if reference.starts_with("observation-") || reference.starts_with("observation:") {
            let raw = reference
                .strip_prefix("observation-")
                .or_else(|| reference.strip_prefix("observation:"))
                .unwrap_or(&reference);
            let observation_id = raw
                .parse::<i64>()
                .ok()
                .filter(|id| *id > 0)
                .ok_or_else(|| {
                    Error::invalid("observation evidence ref must name a positive id")
                })?;
            validate_observation_ref(db, observation_id, task, attempt_id)?;
        } else if reference.starts_with("source-") || reference.starts_with("source:") {
            let id = reference
                .strip_prefix("source-")
                .or_else(|| reference.strip_prefix("source:"))
                .unwrap_or(&reference);
            if !task_has_source(task, id) {
                return Err(Error::new(
                    "EVIDENCE_NOT_FOUND",
                    "source evidence ref is not in this exact Task source index",
                ));
            }
        } else {
            // A plain exact ID is accepted only when it resolves to an
            // in-scope Operation or artifact; it is never treated as a path.
            if validate_operation_ref(db, &reference, task, attempt_id).is_err()
                && validate_artifact_ref(db, &reference, task, attempt_id, None).is_err()
            {
                return Err(Error::new(
                    "EVIDENCE_NOT_FOUND",
                    "evidence ref does not name an exact existing in-scope fact",
                ));
            }
        }
    }
    Ok(())
}

fn validate_operation_ref(
    db: &Connection,
    operation_id: &str,
    task: &Value,
    attempt_id: &str,
) -> Result<()> {
    let row: Option<(Option<String>, Option<String>, Option<String>, Option<String>)> = db
        .query_row(
            "SELECT task_id,attempt_id,json_extract(original_request_json,'$.task_id'),json_extract(original_request_json,'$.attempt_id') \
             FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((stored_task, stored_attempt, request_task, request_attempt)) = row else {
        return Err(Error::new(
            "EVIDENCE_NOT_FOUND",
            "Operation evidence was not found",
        ));
    };
    if stored_task.as_deref().or(request_task.as_deref()) != task["task_id"].as_str()
        || stored_attempt.as_deref().or(request_attempt.as_deref()) != Some(attempt_id)
    {
        return Err(Error::new(
            "EVIDENCE_SCOPE_MISMATCH",
            "Operation evidence is outside the exact Concilium Task and Attempt",
        ));
    }
    Ok(())
}

fn validate_observation_ref(
    db: &Connection,
    observation_id: i64,
    task: &Value,
    attempt_id: &str,
) -> Result<()> {
    let operation_id: Option<String> = db
        .query_row(
            "SELECT operation_id FROM observations WHERE observation_id=?1",
            [observation_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    let operation_id = operation_id
        .ok_or_else(|| Error::new("EVIDENCE_NOT_FOUND", "Observation evidence was not found"))?;
    validate_operation_ref(db, &operation_id, task, attempt_id)
}

fn validate_artifact_ref(
    db: &Connection,
    artifact_id: &str,
    task: &Value,
    attempt_id: &str,
    expected_kind: Option<&str>,
) -> Result<()> {
    let row: Option<(String, String)> = db
        .query_row(
            "SELECT kind,metadata_json FROM artifacts WHERE artifact_id=?1",
            [artifact_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((kind, raw_metadata)) = row else {
        return Err(Error::new(
            "EVIDENCE_NOT_FOUND",
            "Artifact evidence was not found",
        ));
    };
    if expected_kind.is_some_and(|expected| kind != expected) {
        return Err(Error::new(
            "EVIDENCE_KIND_MISMATCH",
            "artifact evidence kind does not match the required reference kind",
        ));
    }
    let metadata: Value = serde_json::from_str(&raw_metadata)?;
    if metadata["task_id"] != task["task_id"]
        || !value_text_is(&metadata["attempt_id"], attempt_id)
        || metadata
            .get("task_revision")
            .is_some_and(|revision| revision != &task["revision"])
    {
        return Err(Error::new(
            "EVIDENCE_SCOPE_MISMATCH",
            "Artifact evidence is outside the exact Concilium Task and Attempt",
        ));
    }
    Ok(())
}

fn task_has_source(task: &Value, source_id: &str) -> bool {
    fn contains(value: &Value, source_id: &str) -> bool {
        match value {
            Value::Array(items) => items.iter().any(|item| contains(item, source_id)),
            Value::Object(object) => {
                ["source_id", "id"]
                    .iter()
                    .any(|key| object.get(*key).and_then(Value::as_str) == Some(source_id))
                    || object.values().any(|item| contains(item, source_id))
            }
            _ => false,
        }
    }
    task.get("task_brief")
        .and_then(|brief| brief.get("source_index"))
        .is_some_and(|index| contains(index, source_id))
        || task
            .get("brief")
            .and_then(|brief| brief.get("source_index"))
            .is_some_and(|index| contains(index, source_id))
}

fn slot_index_by_id(record: &Value, slot_id: &str) -> Result<usize> {
    record["slots"]
        .as_array()
        .and_then(|slots| {
            slots
                .iter()
                .position(|slot| slot["slot_id"].as_str() == Some(slot_id))
        })
        .ok_or_else(|| Error::new("NOT_FOUND", "Concilium slot was not found"))
}

fn slot_by_id<'a>(record: &'a Value, slot_id: &str) -> Result<&'a Value> {
    let index = slot_index_by_id(record, slot_id)?;
    record["slots"]
        .as_array()
        .and_then(|slots| slots.get(index))
        .ok_or_else(|| damaged("Concilium slot index is inconsistent"))
}

fn position_to_value(position: &ParticipantPosition) -> Value {
    json!({
        "position":position.position,
        "proposal_revision_id":position.proposal_revision_id,
        "claims":position.claims.iter().map(|claim| json!({
            "claim_id":claim.claim_id,
            "stance":claim.stance,
            "fact":claim.fact,
            "evidence_refs":claim.evidence_refs,
            "counterexample":claim.counterexample,
            "falsifier":claim.falsifier,
            "assumptions":claim.assumptions,
            "confidence":claim.confidence,
        })).collect::<Vec<_>>(),
        "required_change":position.required_change,
        "unresolved_questions":position.unresolved_questions,
    })
}

fn collect_unresolved_questions(slots: &[&Value]) -> Vec<Value> {
    let mut seen = BTreeSet::new();
    let mut questions = Vec::new();
    for slot in slots {
        for question in slot["position"]["unresolved_questions"]
            .as_array()
            .into_iter()
            .flatten()
        {
            if let Some(text) = question.as_str()
                && seen.insert(text.to_owned())
            {
                questions.push(question.clone());
            }
        }
    }
    questions
}

fn truncate_text(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes.min(value.len());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

fn role_name(role: &Role) -> &'static str {
    match role {
        Role::Operator => "operator",
        Role::Manager => "manager",
        Role::HookSource => "hook_source",
        Role::Participant => "participant",
        Role::Observer => "observer",
        Role::Module => "module",
        Role::ModuleSupervisor => "module_supervisor",
        Role::Scheduler => "scheduler",
    }
}

fn check_request_size(value: &Value) -> Result<()> {
    let bytes = serde_json::to_vec(value)?.len();
    if bytes > limits::MAX_CONCILIUM_REQUEST_BYTES {
        return Err(Error::new(
            "PAYLOAD_TOO_LARGE",
            format!(
                "Concilium request is {bytes} bytes; maximum is {}",
                limits::MAX_CONCILIUM_REQUEST_BYTES
            ),
        ));
    }
    Ok(())
}

fn validate_identifier(value: &str, field: &str, max_bytes: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max_bytes
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(Error::invalid(format!(
            "{field} must be 1..={max_bytes} bytes without whitespace"
        )));
    }
    Ok(())
}

fn sha256_digest(bytes: &[u8]) -> String {
    format!("sha256:{}", model::digest(bytes))
}

fn damaged(message: impl Into<String>) -> Error {
    Error::new("CONCILIUM_CORRUPT", message)
}
