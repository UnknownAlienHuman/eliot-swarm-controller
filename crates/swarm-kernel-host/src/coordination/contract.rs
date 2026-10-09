//! Typed, bounded contract proposal protocol. These records are advisory; only
//! the separate manager decision method may ratify a contract.
use crate::{
    error::{Error, Result},
    model,
};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use swarm_contracts::coordination_limits as limits;

pub(crate) const DEFAULT_PAGE_SIZE: i64 = limits::DEFAULT_READ_PAGE_SIZE;
pub(crate) const MAX_PAGE_SIZE: i64 = limits::MAX_READ_PAGE_SIZE;
pub(crate) const MAX_PROPOSAL_BYTES: usize = limits::MAX_COORDINATION_REQUEST_BYTES;

#[derive(Debug, Clone)]
pub(crate) struct ProposalRequest {
    pub thread_id: String,
    pub supersedes_revision_id: Option<String>,
    pub topic: String,
    pub canonical_body: Value,
    pub digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResponseAct {
    Counterproposal,
    Object,
    Support,
    Withdraw,
}

impl ResponseAct {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Counterproposal => "counterproposal",
            Self::Object => "object",
            Self::Support => "support",
            Self::Withdraw => "withdraw",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResponseRequest {
    pub thread_id: String,
    pub proposal_id: String,
    pub proposal_revision_id: String,
    pub proposal_digest: String,
    pub act: ResponseAct,
    /// Keep the raw bounded value. Recognized values are mapped to canonical
    /// names; unsupported or empty values remain visible and classify as no
    /// material progress.
    pub objection_basis: Option<String>,
    pub normalized_objection_basis: Option<&'static str>,
    pub material_basis: bool,
    pub reason: String,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct GetRequest {
    pub thread_id: String,
    pub proposal_id: String,
    pub proposal_revision_id: String,
    pub after_observation_id: Option<i64>,
    pub limit: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct ListRequest {
    pub thread_id: String,
    pub after_sequence: Option<i64>,
    pub limit: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecisionKind {
    Ratified,
    Rejected,
}

impl DecisionKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Ratified => "ratified",
            Self::Rejected => "rejected",
        }
    }

    pub(crate) fn observation_kind(self) -> &'static str {
        match self {
            Self::Ratified => "coordination.contract_ratified",
            Self::Rejected => "coordination.contract_rejected",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DecisionRequest {
    pub kind: DecisionKind,
    pub thread_id: String,
    pub expected_state_revision: i64,
    pub proposal_id: String,
    pub proposal_revision_id: String,
    pub proposal_digest: String,
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
    pub affected_scope_revisions: Vec<Value>,
    pub reason: String,
    pub conditions: Vec<String>,
    pub caveats: Vec<String>,
}

const SCOPE_REVISION_FIELDS: &[&str] = &[
    "scope_intent_id",
    "state_revision",
    "digest",
    "state",
    "owner_client_id",
    "actor",
    "assignment_id",
    "participation_basis",
    "mode",
    "paths",
    "symbols",
    "interfaces",
    "expires_at_ms",
    "override_scope_intent_ids",
];

pub(crate) fn parse_decision_request(method: &str, value: &Value) -> Result<DecisionRequest> {
    let kind = match method {
        "coordination.contract.ratify" => DecisionKind::Ratified,
        "coordination.contract.reject" => DecisionKind::Rejected,
        _ => return Err(Error::new("METHOD_NOT_FOUND", method)),
    };
    model::fields(
        value,
        &[
            "client_request_id",
            "thread_id",
            "expected_state_revision",
            "proposal_id",
            "proposal_revision_id",
            "proposal_digest",
            "task_id",
            "task_revision",
            "attempt_id",
            "affected_scope_revisions",
            "reason",
            "conditions",
            "caveats",
        ],
    )?;
    bounded_id(
        model::text(value, "client_request_id")?,
        "client_request_id",
        limits::MAX_CLIENT_REQUEST_ID_BYTES,
    )?;
    let thread_id = prefixed_uuid(model::text(value, "thread_id")?, "coord-", "thread_id")?;
    let expected_state_revision = model::positive(value, "expected_state_revision")?;
    let proposal_id = prefixed_uuid(model::text(value, "proposal_id")?, "cprop-", "proposal_id")?;
    let proposal_revision_id = prefixed_uuid(
        model::text(value, "proposal_revision_id")?,
        "cprev-",
        "proposal_revision_id",
    )?;
    let proposal_digest = parse_proposal_digest(model::text(value, "proposal_digest")?)?;
    let task_id = bounded_id(model::text(value, "task_id")?, "task_id", 256)?;
    let task_revision = model::positive(value, "task_revision")?;
    let attempt_id = bounded_id(model::text(value, "attempt_id")?, "attempt_id", 256)?;
    let affected_scope_revisions = parse_scope_revision_refs(value)?;
    let reason = bounded_text(value, "reason", limits::MAX_REASON_BYTES)?;
    if reason.trim().is_empty() {
        return Err(Error::invalid("reason must be nonempty text"));
    }
    let conditions = text_array(value, "conditions", 64, limits::MAX_SUMMARY_BYTES)?;
    let caveats = text_array(value, "caveats", 64, limits::MAX_SUMMARY_BYTES)?;
    if model::canonical(value)?.len() > MAX_PROPOSAL_BYTES {
        return Err(Error::invalid(
            "canonical contract decision request exceeds 65536 bytes",
        ));
    }
    Ok(DecisionRequest {
        kind,
        thread_id,
        expected_state_revision,
        proposal_id,
        proposal_revision_id,
        proposal_digest,
        task_id,
        task_revision,
        attempt_id,
        affected_scope_revisions,
        reason,
        conditions,
        caveats,
    })
}

fn parse_scope_revision_refs(value: &Value) -> Result<Vec<Value>> {
    let items = value
        .get("affected_scope_revisions")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::invalid("affected_scope_revisions must be an array"))?;
    if items.len() > MAX_PAGE_SIZE as usize {
        return Err(Error::invalid(format!(
            "affected_scope_revisions may contain at most {MAX_PAGE_SIZE} items"
        )));
    }
    let mut previous_id: Option<String> = None;
    for item in items {
        model::fields(item, SCOPE_REVISION_FIELDS)?;
        let object = item
            .as_object()
            .ok_or_else(|| Error::invalid("scope revision entries must be objects"))?;
        if object.len() != SCOPE_REVISION_FIELDS.len()
            || SCOPE_REVISION_FIELDS
                .iter()
                .any(|field| !object.contains_key(*field))
        {
            return Err(Error::invalid(
                "scope revision entries must contain the complete current scope reference",
            ));
        }
        let scope_intent_id = prefixed_uuid(
            model::text(item, "scope_intent_id")?,
            "cscope-",
            "scope_intent_id",
        )?;
        model::positive(item, "state_revision")?;
        let digest = model::text(item, "digest")?;
        if !is_canonical_sha256(digest) {
            return Err(Error::invalid(
                "scope revision digest must be lowercase SHA-256 hex",
            ));
        }
        if previous_id
            .as_deref()
            .is_some_and(|previous| previous >= scope_intent_id.as_str())
        {
            return Err(Error::invalid(
                "affected_scope_revisions must be unique and sorted by scope_intent_id",
            ));
        }
        previous_id = Some(scope_intent_id);
    }
    Ok(items.clone())
}

pub(crate) fn parse_proposal_digest(value: &str) -> Result<String> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::invalid("proposal_digest must be SHA-256 hex"));
    }
    Ok(value.to_ascii_lowercase())
}

pub(crate) fn is_canonical_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn parse_proposal_request(value: &Value) -> Result<ProposalRequest> {
    model::fields(
        value,
        &[
            "client_request_id",
            "thread_id",
            "supersedes_revision_id",
            "topic",
            "affected",
            "statement",
            "acceptance_conditions",
            "claims",
            "open_questions",
        ],
    )?;
    bounded_id(
        model::text(value, "client_request_id")?,
        "client_request_id",
        limits::MAX_CLIENT_REQUEST_ID_BYTES,
    )?;
    let thread_id = prefixed_uuid(model::text(value, "thread_id")?, "coord-", "thread_id")?;
    if value.get("supersedes_revision_id").is_none() {
        return Err(Error::invalid(
            "supersedes_revision_id is required and may be null",
        ));
    }
    let supersedes_revision_id = optional_id(value, "supersedes_revision_id", "cprev-")?;
    let topic = bounded_text(value, "topic", limits::MAX_SUBJECT_BYTES)?;
    let affected = parse_affected(
        value
            .get("affected")
            .ok_or_else(|| Error::invalid("affected is required"))?,
    )?;
    let statement = parse_statement(
        value
            .get("statement")
            .ok_or_else(|| Error::invalid("statement is required"))?,
    )?;
    let acceptance_conditions = text_array(
        value,
        "acceptance_conditions",
        64,
        limits::MAX_SUMMARY_BYTES,
    )?;
    let claims = json_array(value, "claims")?;
    let open_questions = text_array(value, "open_questions", 64, limits::MAX_SUMMARY_BYTES)?;
    let canonical_body = json!({
        "thread_id": thread_id,
        "supersedes_revision_id": supersedes_revision_id,
        "topic": topic,
        "affected": affected,
        "statement": statement,
        "acceptance_conditions": acceptance_conditions,
        "claims": claims,
        "open_questions": open_questions,
    });
    let request_bytes = model::canonical(value)?;
    if request_bytes.len() > MAX_PROPOSAL_BYTES {
        return Err(Error::invalid(
            "canonical proposal request exceeds 65536 bytes",
        ));
    }
    let encoded = model::canonical(&canonical_body)?;
    if encoded.len() > limits::MAX_INLINE_BODY_BYTES {
        return Err(Error::invalid(
            "canonical proposal exceeds the 16384-byte inline body limit",
        ));
    }
    let digest = model::digest(encoded.as_bytes());
    Ok(ProposalRequest {
        thread_id,
        supersedes_revision_id,
        topic,
        canonical_body,
        digest,
    })
}

pub(crate) fn parse_response_request(value: &Value) -> Result<ResponseRequest> {
    model::fields(
        value,
        &[
            "client_request_id",
            "thread_id",
            "proposal_id",
            "proposal_revision_id",
            "proposal_digest",
            "act",
            "objection_basis",
            "reason",
            "evidence_refs",
        ],
    )?;
    bounded_id(
        model::text(value, "client_request_id")?,
        "client_request_id",
        limits::MAX_CLIENT_REQUEST_ID_BYTES,
    )?;
    let thread_id = prefixed_uuid(model::text(value, "thread_id")?, "coord-", "thread_id")?;
    let proposal_id = prefixed_uuid(model::text(value, "proposal_id")?, "cprop-", "proposal_id")?;
    let proposal_revision_id = prefixed_uuid(
        model::text(value, "proposal_revision_id")?,
        "cprev-",
        "proposal_revision_id",
    )?;
    let proposal_digest = parse_proposal_digest(model::text(value, "proposal_digest")?)?;
    let act = match model::text(value, "act")? {
        "counterproposal" => ResponseAct::Counterproposal,
        "object" => ResponseAct::Object,
        "support" => ResponseAct::Support,
        "withdraw" => ResponseAct::Withdraw,
        _ => {
            return Err(Error::invalid(
                "act must be counterproposal, object, support, or withdraw",
            ));
        }
    };
    if value.get("objection_basis").is_none() {
        return Err(Error::invalid(
            "objection_basis is required and may be null",
        ));
    }
    let objection_basis = match value.get("objection_basis") {
        None | Some(Value::Null) => None,
        Some(Value::String(raw))
            if raw.len() <= 128 && !raw.bytes().any(|byte| byte.is_ascii_control()) =>
        {
            Some(raw.clone())
        }
        Some(Value::String(_)) => {
            return Err(Error::invalid(
                "objection_basis exceeds 128 bytes or contains a control character",
            ));
        }
        Some(_) => {
            return Err(Error::invalid(
                "objection_basis must be null or bounded text",
            ));
        }
    };
    let normalized_objection_basis = objection_basis
        .as_deref()
        .and_then(normalize_objection_basis);
    let reason = bounded_text(value, "reason", limits::MAX_REASON_BYTES)?;
    let evidence_refs = text_array(
        value,
        "evidence_refs",
        limits::MAX_EVIDENCE_REFS,
        limits::MAX_REFERENCE_BYTES,
    )?;
    let material_basis = matches!(act, ResponseAct::Counterproposal | ResponseAct::Object)
        && normalized_objection_basis.is_some()
        && !reason.trim().is_empty();
    Ok(ResponseRequest {
        thread_id,
        proposal_id,
        proposal_revision_id,
        proposal_digest,
        act,
        objection_basis,
        normalized_objection_basis,
        material_basis,
        reason,
        evidence_refs,
    })
}

pub(crate) fn parse_get_request(value: &Value) -> Result<GetRequest> {
    model::fields(
        value,
        &[
            "thread_id",
            "proposal_id",
            "proposal_revision_id",
            "after_observation_id",
            "limit",
        ],
    )?;
    let after_observation_id =
        match value.get("after_observation_id") {
            None | Some(Value::Null) => None,
            Some(raw) => Some(raw.as_i64().filter(|cursor| *cursor >= 0).ok_or_else(|| {
                Error::invalid("after_observation_id must be a nonnegative integer")
            })?),
        };
    Ok(GetRequest {
        thread_id: prefixed_uuid(model::text(value, "thread_id")?, "coord-", "thread_id")?,
        proposal_id: prefixed_uuid(model::text(value, "proposal_id")?, "cprop-", "proposal_id")?,
        proposal_revision_id: prefixed_uuid(
            model::text(value, "proposal_revision_id")?,
            "cprev-",
            "proposal_revision_id",
        )?,
        after_observation_id,
        limit: parse_limit(value.get("limit"))?,
    })
}

pub(crate) fn parse_list_request(value: &Value) -> Result<ListRequest> {
    model::fields(value, &["thread_id", "after_sequence", "limit"])?;
    let after_sequence = match value.get("after_sequence") {
        None | Some(Value::Null) => None,
        Some(raw) => Some(
            raw.as_i64()
                .filter(|cursor| *cursor >= 0)
                .ok_or_else(|| Error::invalid("after_sequence must be a nonnegative integer"))?,
        ),
    };
    Ok(ListRequest {
        thread_id: prefixed_uuid(model::text(value, "thread_id")?, "coord-", "thread_id")?,
        after_sequence,
        limit: parse_limit(value.get("limit"))?,
    })
}

fn parse_limit(raw: Option<&Value>) -> Result<i64> {
    match raw {
        None | Some(Value::Null) => Ok(DEFAULT_PAGE_SIZE),
        Some(raw) => raw
            .as_i64()
            .filter(|limit| (1..=MAX_PAGE_SIZE).contains(limit))
            .ok_or_else(|| Error::invalid("limit must be in 1..=50")),
    }
}

fn parse_affected(value: &Value) -> Result<Value> {
    model::fields(value, &["paths", "symbols", "schemas"])?;
    let paths = sorted_text_array(value, "paths", limits::MAX_REFERENCE_BYTES)?;
    let symbols = sorted_text_array(value, "symbols", limits::MAX_REFERENCE_BYTES)?;
    let schemas = sorted_text_array(value, "schemas", limits::MAX_REFERENCE_BYTES)?;
    let total = paths.len() + symbols.len() + schemas.len();
    if total == 0 {
        return Err(Error::invalid(
            "affected must name at least one path, symbol, or schema",
        ));
    }
    if total > 64 {
        return Err(Error::invalid(
            "affected may name at most 64 paths, symbols, and schemas combined",
        ));
    }
    Ok(json!({"paths": paths, "symbols": symbols, "schemas": schemas}))
}

fn parse_statement(value: &Value) -> Result<Value> {
    let fields = [
        "producer",
        "consumer",
        "identity",
        "payload",
        "observation_boundary",
        "failure_semantics",
        "versioning",
    ];
    model::fields(value, &fields)?;
    let mut statement = serde_json::Map::new();
    for field in fields {
        statement.insert(
            field.to_owned(),
            json!(bounded_text(value, field, limits::MAX_SUMMARY_BYTES)?),
        );
    }
    Ok(Value::Object(statement))
}

fn text_array(
    value: &Value,
    field: &str,
    max_items: usize,
    max_item_bytes: usize,
) -> Result<Vec<String>> {
    let items = value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| Error::invalid(format!("{field} must be an array")))?;
    if items.len() > max_items {
        return Err(Error::invalid(format!(
            "{field} may contain at most {max_items} items"
        )));
    }
    let mut seen = BTreeSet::new();
    let mut values = Vec::with_capacity(items.len());
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
        if !seen.insert(text.to_owned()) {
            return Err(Error::invalid(format!("{field} entries must be unique")));
        }
        values.push(text.to_owned());
    }
    Ok(values)
}

fn sorted_text_array(value: &Value, field: &str, max_item_bytes: usize) -> Result<Vec<String>> {
    let mut items = text_array(value, field, 64, max_item_bytes)?;
    items.sort();
    Ok(items)
}

fn json_array(value: &Value, field: &str) -> Result<Vec<Value>> {
    value
        .get(field)
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| Error::invalid(format!("{field} must be an array")))
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

fn bounded_id(value: &str, field: &str, max_bytes: usize) -> Result<String> {
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
    Ok(value.to_owned())
}

fn prefixed_uuid(value: &str, prefix: &str, field: &str) -> Result<String> {
    let Some(suffix) = value.strip_prefix(prefix) else {
        return Err(Error::invalid(format!(
            "{field} must use the {prefix}<uuid> form"
        )));
    };
    let uuid = suffix.as_bytes();
    if uuid.len() != 36
        || !uuid.iter().enumerate().all(|(index, byte)| {
            if [8, 13, 18, 23].contains(&index) {
                *byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
    {
        return Err(Error::invalid(format!(
            "{field} must use the {prefix}<uuid> form"
        )));
    }
    Ok(value.to_owned())
}

fn optional_id(value: &Value, field: &str, prefix: &str) -> Result<Option<String>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => prefixed_uuid(text, prefix, field).map(Some),
        Some(_) => Err(Error::invalid(format!(
            "{field} must be null or an identifier"
        ))),
    }
}

fn normalize_objection_basis(raw: &str) -> Option<&'static str> {
    match raw {
        "violated requirement" | "violated_requirement" => Some("violated_requirement"),
        "counterexample" => Some("counterexample"),
        "evidence gap" | "evidence_gap" => Some("evidence_gap"),
        "unowned effect" | "unowned_effect" => Some("unowned_effect"),
        "identity/replay ambiguity" | "identity_replay_ambiguity" => {
            Some("identity_replay_ambiguity")
        }
        "versioning incompatibility" | "versioning_incompatibility" => {
            Some("versioning_incompatibility")
        }
        _ => None,
    }
}
