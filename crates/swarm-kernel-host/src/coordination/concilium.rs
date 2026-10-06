//! Strict Concilium request DTOs and wire validation.
//!
//! Concilium requests are coordination facts only. Parsing this module never
//! opens a model turn, sends native input, changes a Task, or ratifies a
//! contract. Store remains responsible for current identity, authority,
//! receipt, and referenced-record checks.

use crate::{
    coordination,
    error::{Error, Result},
    model,
};
use serde_json::Value;
use std::collections::BTreeSet;
use swarm_contracts::concilium_limits::{
    DEFAULT_READ_PAGE_SIZE, MAX_CLAIM_TEXT_BYTES, MAX_CLAIMS_PER_POSITION, MAX_CLIENT_ID_BYTES,
    MAX_CLIENT_REQUEST_ID_BYTES, MAX_CONCILIUM_REQUEST_BYTES, MAX_CONFLICT_BYTES,
    MAX_EVIDENCE_REF_BYTES, MAX_EVIDENCE_REFS, MAX_IDENTIFIER_BYTES, MAX_PARTICIPANT_REASON_BYTES,
    MAX_POSITION_TEXT_BYTES, MAX_QUESTION_BYTES, MAX_READ_PAGE_SIZE,
};

const CONCILIUM_STATES: &[&str] = &[
    "proposed",
    "planned",
    "round_1_open",
    "round_1_ready",
    "round_2_open",
    "round_2_ready",
    "merge_available",
    "completed",
    "unresolved",
    "cancelled",
    "failed",
];

const CLOSE_RESULTS: &[&str] = &[
    "recommended",
    "minority_report",
    "insufficient_evidence",
    "irreconcilable_contract",
    "cancelled",
    "failed",
];

const POSITION_KINDS: &[&str] = &["support", "oppose", "alternative", "insufficient_evidence"];

const CLAIM_STANCES: &[&str] = &["support", "oppose", "uncertain"];
const CONFIDENCE_LEVELS: &[&str] = &["low", "medium", "high"];
const CONCILIUM_METHODS: &[&str] = &[
    "concilium.propose",
    "concilium.preview",
    "concilium.open",
    "concilium.position.submit",
    "concilium.round.advance",
    "concilium.get",
    "concilium.list",
    "concilium.close",
];

#[derive(Debug, Clone)]
pub(crate) enum ConciliumRequest {
    Propose(ProposeRequest),
    Preview(PreviewRequest),
    Open(OpenRequest),
    PositionSubmit(PositionSubmitRequest),
    RoundAdvance(RoundAdvanceRequest),
    Get(GetRequest),
    List(ListRequest),
    Close(CloseRequest),
}

#[derive(Debug, Clone)]
pub(crate) struct ProposeRequest {
    pub client_request_id: String,
    pub task_id: String,
    pub attempt_id: String,
    pub failed_thread_id: String,
    pub decision_question: String,
    pub material_conflict: String,
    pub participants: Vec<ParticipantRef>,
    pub proposal_revision_ids: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub expected_output: String,
    pub suggested_max_rounds: i64,
    /// Advisory caller metadata. It is retained but never used as a model
    /// budget, quota, scheduler input, or execution instruction.
    pub suggested_budget: Value,
    pub close_condition: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ParticipantRef {
    pub client_id: String,
    /// Registration generation is absent for identities which have no bound
    /// native generation. Store resolves this against the live registration.
    pub generation: Option<i64>,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub(crate) struct PreviewRequest {
    pub proposal_operation_id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct OpenRequest {
    pub client_request_id: String,
    pub proposal_operation_id: String,
    pub plan_digest: String,
    pub confirmed_reasonable: bool,
    pub manager_reason: String,
}

#[derive(Debug, Clone)]
pub(crate) struct PositionSubmitRequest {
    pub client_request_id: String,
    pub concilium_id: String,
    pub slot_id: String,
    pub packet_digest: String,
    pub position: ParticipantPosition,
}

#[derive(Debug, Clone)]
pub(crate) struct ParticipantPosition {
    pub position: String,
    pub proposal_revision_id: Option<String>,
    pub claims: Vec<PositionClaim>,
    pub required_change: String,
    pub unresolved_questions: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct PositionClaim {
    pub claim_id: String,
    pub stance: String,
    pub fact: String,
    pub evidence_refs: Vec<String>,
    pub counterexample: Option<String>,
    pub falsifier: String,
    pub assumptions: Vec<String>,
    pub confidence: String,
}

#[derive(Debug, Clone)]
pub(crate) struct RoundAdvanceRequest {
    pub client_request_id: String,
    pub concilium_id: String,
    pub expected_state_revision: i64,
    pub next_round: i64,
    pub merged_proposal_digest: Option<String>,
    pub manager_reason: String,
}

#[derive(Debug, Clone)]
pub(crate) struct GetRequest {
    pub concilium_id: String,
    pub limit: i64,
    pub after_slot_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ListRequest {
    pub task_id: String,
    pub attempt_id: Option<String>,
    pub state: Option<String>,
    pub limit: i64,
    pub after_concilium_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct CloseRequest {
    pub client_request_id: String,
    pub concilium_id: String,
    pub expected_state_revision: i64,
    pub result: String,
    pub recommendation: Option<String>,
    pub manager_reason: String,
}

/// Validate one exact Concilium API request. Unknown methods are reported
/// separately so callers can compose this with the existing method router.
pub(crate) fn validate(method: &str, value: &Value) -> Result<()> {
    parse(method, value).map(|_| ())
}

/// Parse the current Concilium wire forms into DTOs. The 32 KiB bound applies
/// before per-field work; Store still resolves every referenced record and
/// checks current authority before committing an Operation.
pub(crate) fn parse(method: &str, value: &Value) -> Result<ConciliumRequest> {
    if !CONCILIUM_METHODS.contains(&method) {
        return Err(Error::new(
            "METHOD_NOT_FOUND",
            format!("{method} is not a Concilium method"),
        ));
    }
    validate_request_size(value)?;
    match method {
        "concilium.propose" => parse_propose(value).map(ConciliumRequest::Propose),
        "concilium.preview" => parse_preview(value).map(ConciliumRequest::Preview),
        "concilium.open" => parse_open(value).map(ConciliumRequest::Open),
        "concilium.position.submit" => {
            parse_position_submit(value).map(ConciliumRequest::PositionSubmit)
        }
        "concilium.round.advance" => parse_round_advance(value).map(ConciliumRequest::RoundAdvance),
        "concilium.get" => parse_get(value).map(ConciliumRequest::Get),
        "concilium.list" => parse_list(value).map(ConciliumRequest::List),
        "concilium.close" => parse_close(value).map(ConciliumRequest::Close),
        _ => unreachable!("method was checked against CONCILIUM_METHODS"),
    }
}

fn validate_request_size(value: &Value) -> Result<()> {
    let bytes = serde_json::to_vec(value)?.len();
    if bytes > MAX_CONCILIUM_REQUEST_BYTES {
        return Err(Error::new(
            "PAYLOAD_TOO_LARGE",
            format!("Concilium request is {bytes} bytes; maximum is {MAX_CONCILIUM_REQUEST_BYTES}"),
        ));
    }
    Ok(())
}

fn parse_propose(value: &Value) -> Result<ProposeRequest> {
    model::fields(
        value,
        &[
            "client_request_id",
            "task_id",
            "attempt_id",
            "failed_thread_id",
            "decision_question",
            "material_conflict",
            "participants",
            "proposal_revision_ids",
            "evidence_refs",
            "expected_output",
            "suggested_max_rounds",
            "suggested_budget",
            "close_condition",
        ],
    )?;
    let participants_value = required_array(value, "participants")?;
    if participants_value.is_empty() {
        return Err(Error::invalid(
            "participants must contain at least one exact participant reference",
        ));
    }
    let mut participants = Vec::with_capacity(participants_value.len());
    let mut participant_keys = BTreeSet::new();
    for participant in participants_value {
        model::fields(participant, &["client_id", "generation", "reason"])?;
        let client_id = bounded_identifier(participant, "client_id", MAX_CLIENT_ID_BYTES)?;
        let generation = optional_positive(participant, "generation")?;
        let reason = bounded_text(participant, "reason", MAX_PARTICIPANT_REASON_BYTES)?;
        let key = (client_id.clone(), generation);
        if !participant_keys.insert(key) {
            return Err(Error::invalid(
                "participants entries must identify unique client/generation pairs",
            ));
        }
        participants.push(ParticipantRef {
            client_id,
            generation,
            reason,
        });
    }

    let suggested_max_rounds = value
        .get("suggested_max_rounds")
        .and_then(Value::as_i64)
        .filter(|rounds| (1..=3).contains(rounds))
        .ok_or_else(|| Error::invalid("suggested_max_rounds must be an integer in 1..=3"))?;
    let suggested_budget = value
        .get("suggested_budget")
        .filter(|budget| budget.is_object())
        .cloned()
        .ok_or_else(|| Error::invalid("suggested_budget must be a JSON object"))?;

    Ok(ProposeRequest {
        client_request_id: request_id(value)?,
        task_id: bounded_identifier(value, "task_id", MAX_IDENTIFIER_BYTES)?,
        attempt_id: bounded_identifier(value, "attempt_id", MAX_IDENTIFIER_BYTES)?,
        failed_thread_id: bounded_identifier(value, "failed_thread_id", MAX_IDENTIFIER_BYTES)?,
        decision_question: bounded_text(value, "decision_question", MAX_QUESTION_BYTES)?,
        material_conflict: bounded_text(value, "material_conflict", MAX_CONFLICT_BYTES)?,
        participants,
        proposal_revision_ids: string_array(
            value,
            "proposal_revision_ids",
            MAX_IDENTIFIER_BYTES,
            None,
        )?,
        evidence_refs: evidence_refs(value, "evidence_refs")?,
        expected_output: bounded_text(value, "expected_output", MAX_QUESTION_BYTES)?,
        suggested_max_rounds,
        suggested_budget,
        close_condition: bounded_text(value, "close_condition", MAX_QUESTION_BYTES)?,
    })
}

fn parse_preview(value: &Value) -> Result<PreviewRequest> {
    model::fields(value, &["proposal_operation_id"])?;
    Ok(PreviewRequest {
        proposal_operation_id: bounded_identifier(
            value,
            "proposal_operation_id",
            MAX_IDENTIFIER_BYTES,
        )?,
    })
}

fn parse_open(value: &Value) -> Result<OpenRequest> {
    model::fields(
        value,
        &[
            "client_request_id",
            "proposal_operation_id",
            "plan_digest",
            "confirmed_reasonable",
            "manager_reason",
        ],
    )?;
    let confirmed_reasonable = value
        .get("confirmed_reasonable")
        .and_then(Value::as_bool)
        .ok_or_else(|| Error::invalid("confirmed_reasonable must be a boolean"))?;
    if !confirmed_reasonable {
        return Err(Error::invalid(
            "confirmed_reasonable must be true to open a Concilium",
        ));
    }
    Ok(OpenRequest {
        client_request_id: request_id(value)?,
        proposal_operation_id: bounded_identifier(
            value,
            "proposal_operation_id",
            MAX_IDENTIFIER_BYTES,
        )?,
        plan_digest: digest(value, "plan_digest")?,
        confirmed_reasonable,
        manager_reason: bounded_text(value, "manager_reason", MAX_QUESTION_BYTES)?,
    })
}

fn parse_position_submit(value: &Value) -> Result<PositionSubmitRequest> {
    model::fields(
        value,
        &[
            "client_request_id",
            "concilium_id",
            "slot_id",
            "packet_digest",
            "position",
        ],
    )?;
    let position_value = value
        .get("position")
        .ok_or_else(|| Error::invalid("position is required"))?;
    let position = parse_position(position_value)?;
    Ok(PositionSubmitRequest {
        client_request_id: request_id(value)?,
        concilium_id: bounded_identifier(value, "concilium_id", MAX_IDENTIFIER_BYTES)?,
        slot_id: bounded_identifier(value, "slot_id", MAX_IDENTIFIER_BYTES)?,
        packet_digest: digest(value, "packet_digest")?,
        position,
    })
}

fn parse_position(value: &Value) -> Result<ParticipantPosition> {
    model::fields(
        value,
        &[
            "position",
            "proposal_revision_id",
            "claims",
            "required_change",
            "unresolved_questions",
        ],
    )?;
    let position = one_of(value, "position", POSITION_KINDS)?;
    let proposal_revision_id = optional_identifier(value, "proposal_revision_id")?;
    let claim_values = required_array(value, "claims")?;
    if claim_values.len() > MAX_CLAIMS_PER_POSITION {
        return Err(Error::invalid(format!(
            "claims may contain at most {MAX_CLAIMS_PER_POSITION} entries"
        )));
    }
    let mut claims = Vec::with_capacity(claim_values.len());
    let mut claim_ids = BTreeSet::new();
    for claim_value in claim_values {
        let claim = parse_claim(claim_value)?;
        if !claim_ids.insert(claim.claim_id.clone()) {
            return Err(Error::invalid(
                "claim_id values must be unique per position",
            ));
        }
        claims.push(claim);
    }
    let required_change =
        bounded_text_allow_empty(value, "required_change", MAX_POSITION_TEXT_BYTES)?;
    let unresolved_questions =
        string_array(value, "unresolved_questions", MAX_QUESTION_BYTES, None)?;
    if claims.is_empty() && required_change.trim().is_empty() && unresolved_questions.is_empty() {
        return Err(Error::invalid(
            "position must contain inspectable claims, a required change, or unresolved questions",
        ));
    }
    Ok(ParticipantPosition {
        position,
        proposal_revision_id,
        claims,
        required_change,
        unresolved_questions,
    })
}

fn parse_claim(value: &Value) -> Result<PositionClaim> {
    model::fields(
        value,
        &[
            "claim_id",
            "stance",
            "fact",
            "evidence_refs",
            "counterexample",
            "falsifier",
            "assumptions",
            "confidence",
        ],
    )?;
    Ok(PositionClaim {
        claim_id: bounded_identifier(value, "claim_id", MAX_IDENTIFIER_BYTES)?,
        stance: one_of(value, "stance", CLAIM_STANCES)?,
        fact: bounded_text(value, "fact", MAX_CLAIM_TEXT_BYTES)?,
        evidence_refs: evidence_refs(value, "evidence_refs")?,
        counterexample: optional_bounded_text(value, "counterexample", MAX_CLAIM_TEXT_BYTES)?,
        falsifier: bounded_text(value, "falsifier", MAX_CLAIM_TEXT_BYTES)?,
        assumptions: string_array(value, "assumptions", MAX_QUESTION_BYTES, None)?,
        confidence: one_of(value, "confidence", CONFIDENCE_LEVELS)?,
    })
}

fn parse_round_advance(value: &Value) -> Result<RoundAdvanceRequest> {
    model::fields(
        value,
        &[
            "client_request_id",
            "concilium_id",
            "expected_state_revision",
            "next_round",
            "merged_proposal_digest",
            "manager_reason",
        ],
    )?;
    let next_round = value
        .get("next_round")
        .and_then(Value::as_i64)
        .filter(|round| *round == 2 || *round == 3)
        .ok_or_else(|| Error::invalid("next_round must be 2 or 3"))?;
    let merged_proposal_digest = optional_digest(value, "merged_proposal_digest")?;
    match (next_round, merged_proposal_digest.as_deref()) {
        (2, None) => {}
        (2, Some(_)) => {
            return Err(Error::invalid(
                "merged_proposal_digest is only valid when advancing to round 3",
            ));
        }
        (3, None) => {
            return Err(Error::invalid(
                "round 3 requires the changed merged_proposal_digest",
            ));
        }
        (3, Some(_)) => {}
        _ => unreachable!("next_round was validated above"),
    }
    Ok(RoundAdvanceRequest {
        client_request_id: request_id(value)?,
        concilium_id: bounded_identifier(value, "concilium_id", MAX_IDENTIFIER_BYTES)?,
        expected_state_revision: model::positive(value, "expected_state_revision")?,
        next_round,
        merged_proposal_digest,
        manager_reason: bounded_text(value, "manager_reason", MAX_QUESTION_BYTES)?,
    })
}

fn parse_get(value: &Value) -> Result<GetRequest> {
    model::fields(value, &["concilium_id", "limit", "after_slot_id"])?;
    Ok(GetRequest {
        concilium_id: bounded_identifier(value, "concilium_id", MAX_IDENTIFIER_BYTES)?,
        limit: page_size(value)?,
        after_slot_id: optional_identifier(value, "after_slot_id")?,
    })
}

fn parse_list(value: &Value) -> Result<ListRequest> {
    model::fields(
        value,
        &[
            "task_id",
            "attempt_id",
            "state",
            "limit",
            "after_concilium_id",
        ],
    )?;
    let state = match value.get("state") {
        None | Some(Value::Null) => None,
        Some(Value::String(_)) => Some(one_of(value, "state", CONCILIUM_STATES)?),
        Some(_) => return Err(Error::invalid("state must be a Concilium state or null")),
    };
    Ok(ListRequest {
        task_id: bounded_identifier(value, "task_id", MAX_IDENTIFIER_BYTES)?,
        attempt_id: optional_identifier(value, "attempt_id")?,
        state,
        limit: page_size(value)?,
        after_concilium_id: optional_identifier(value, "after_concilium_id")?,
    })
}

fn parse_close(value: &Value) -> Result<CloseRequest> {
    model::fields(
        value,
        &[
            "client_request_id",
            "concilium_id",
            "expected_state_revision",
            "result",
            "recommendation",
            "manager_reason",
        ],
    )?;
    let result = one_of(value, "result", CLOSE_RESULTS)?;
    let recommendation = optional_identifier(value, "recommendation")?;
    if result == "recommended" && recommendation.is_none() {
        return Err(Error::invalid(
            "recommended close requires a recommendation reference",
        ));
    }
    Ok(CloseRequest {
        client_request_id: request_id(value)?,
        concilium_id: bounded_identifier(value, "concilium_id", MAX_IDENTIFIER_BYTES)?,
        expected_state_revision: model::positive(value, "expected_state_revision")?,
        result,
        recommendation,
        manager_reason: bounded_text(value, "manager_reason", MAX_QUESTION_BYTES)?,
    })
}

fn request_id(value: &Value) -> Result<String> {
    bounded_identifier(value, "client_request_id", MAX_CLIENT_REQUEST_ID_BYTES)
}

fn bounded_identifier(value: &Value, field: &str, max_bytes: usize) -> Result<String> {
    let text = model::text(value, field)?;
    if text.len() > max_bytes
        || text
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(Error::invalid(format!(
            "{field} must be 1..={max_bytes} bytes without whitespace"
        )));
    }
    Ok(text.to_owned())
}

fn bounded_text(value: &Value, field: &str, max_bytes: usize) -> Result<String> {
    let text = model::text(value, field)?;
    if text.len() > max_bytes || text.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(Error::invalid(format!(
            "{field} must be 1..={max_bytes} bytes without control characters"
        )));
    }
    Ok(text.to_owned())
}

fn bounded_text_allow_empty(value: &Value, field: &str, max_bytes: usize) -> Result<String> {
    let text = value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| Error::invalid(format!("{field} must be a string")))?;
    if text.len() > max_bytes || text.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(Error::invalid(format!(
            "{field} must be at most {max_bytes} bytes without control characters"
        )));
    }
    Ok(text.to_owned())
}

fn optional_bounded_text(value: &Value, field: &str, max_bytes: usize) -> Result<Option<String>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text))
            if !text.trim().is_empty()
                && text.len() <= max_bytes
                && !text.bytes().any(|byte| byte.is_ascii_control()) =>
        {
            Ok(Some(text.clone()))
        }
        Some(_) => Err(Error::invalid(format!(
            "{field} must be nonempty text up to {max_bytes} bytes or null"
        ))),
    }
}

fn optional_positive(value: &Value, field: &str) -> Result<Option<i64>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(raw) => raw
            .as_i64()
            .filter(|number| *number > 0)
            .map(Some)
            .ok_or_else(|| Error::invalid(format!("{field} must be a positive integer or null"))),
    }
}

fn optional_identifier(value: &Value, field: &str) -> Result<Option<String>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(_)) => bounded_identifier(value, field, MAX_IDENTIFIER_BYTES).map(Some),
        Some(_) => Err(Error::invalid(format!(
            "{field} must be an identifier or null"
        ))),
    }
}

fn one_of(value: &Value, field: &str, allowed: &[&str]) -> Result<String> {
    let text = model::text(value, field)?;
    if !allowed.contains(&text) {
        return Err(Error::invalid(format!("unsupported {field}")));
    }
    Ok(text.to_owned())
}

fn required_array<'a>(value: &'a Value, field: &str) -> Result<&'a [Value]> {
    value
        .get(field)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| Error::invalid(format!("{field} must be an array")))
}

fn string_array(
    value: &Value,
    field: &str,
    max_item_bytes: usize,
    max_items: Option<usize>,
) -> Result<Vec<String>> {
    let items = required_array(value, field)?;
    if max_items.is_some_and(|limit| items.len() > limit) {
        return Err(Error::invalid(format!("{field} has too many entries")));
    }
    let mut output = Vec::with_capacity(items.len());
    let mut seen = BTreeSet::new();
    for item in items {
        let text = item
            .as_str()
            .filter(|text| !text.trim().is_empty() && text.len() <= max_item_bytes)
            .ok_or_else(|| {
                Error::invalid(format!(
                    "{field} entries must be nonempty text up to {max_item_bytes} bytes"
                ))
            })?;
        if text.bytes().any(|byte| byte.is_ascii_control()) {
            return Err(Error::invalid(format!(
                "{field} entries cannot contain control characters"
            )));
        }
        if !seen.insert(text) {
            return Err(Error::invalid(format!("{field} entries must be unique")));
        }
        output.push(text.to_owned());
    }
    Ok(output)
}

fn evidence_refs(value: &Value, field: &str) -> Result<Vec<String>> {
    let refs = string_array(
        value,
        field,
        MAX_EVIDENCE_REF_BYTES,
        Some(MAX_EVIDENCE_REFS),
    )?;
    let total_bytes: usize = refs.iter().map(String::len).sum();
    if total_bytes > MAX_EVIDENCE_REFS * MAX_EVIDENCE_REF_BYTES {
        return Err(Error::invalid(format!(
            "{field} exceeds the evidence reference byte bound"
        )));
    }
    Ok(refs)
}

fn digest(value: &Value, field: &str) -> Result<String> {
    match value.get(field) {
        Some(Value::String(_)) => {
            let digest = bounded_text(value, field, 71)?;
            if digest_matches(&digest) {
                Ok(digest)
            } else {
                Err(Error::invalid(format!(
                    "{field} must be sha256 followed by 64 lowercase hex digits"
                )))
            }
        }
        _ => Err(Error::invalid(format!("{field} is required"))),
    }
}

fn optional_digest(value: &Value, field: &str) -> Result<Option<String>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(_)) => digest(value, field).map(Some),
        Some(_) => Err(Error::invalid(format!("{field} must be a digest or null"))),
    }
}

fn digest_matches(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 64
        && hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn page_size(value: &Value) -> Result<i64> {
    let limit = coordination::parse_page(value.get("limit"), DEFAULT_READ_PAGE_SIZE)?;
    if limit > MAX_READ_PAGE_SIZE {
        return Err(Error::invalid(format!(
            "limit must be in 1..={MAX_READ_PAGE_SIZE}"
        )));
    }
    Ok(limit)
}
