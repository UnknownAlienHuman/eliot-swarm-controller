//! Closed wire shapes for the direct, addressed coordination Thread methods.
//!
//! These validators are deliberately side-effect free. The Store repeats all
//! authority, scope, registration, and artifact checks in the write transaction.

use crate::{
    error::{Error, Result},
    model,
};
use serde_json::Value;
use std::collections::BTreeSet;
use swarm_contracts::coordination_limits as limits;

const REASONABILITY_FIELDS: &[&str] = &[
    "blocking_fact",
    "decision_needed",
    "why_coordination_is_needed",
    "expected_output",
    "close_condition",
];

const SPEECH_ACTS: &[&str] = &[
    "inform",
    "query",
    "answer",
    "propose",
    "counterproposal",
    "object",
    "support",
    "withdraw",
    "not_understood",
    "resolution_summary",
];

#[derive(Debug, Clone)]
pub(crate) struct ParticipantSpec {
    pub client_id: String,
    pub generation: Option<i64>,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub(crate) struct OpenRequest {
    pub client_request_id: String,
    pub task_id: String,
    pub attempt_id: String,
    pub assignment_id: Option<String>,
    pub supersedes_thread_id: Option<String>,
    pub topic_kind: String,
    pub subject: String,
    pub participants: Vec<ParticipantSpec>,
    pub reasonability: Value,
    pub related_scopes: Vec<Value>,
    pub body_ref: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct SendRequest {
    pub client_request_id: String,
    pub thread_id: String,
    pub recipient: String,
    pub speech_act: String,
    pub subject: String,
    pub summary: String,
    pub inline_body: Option<String>,
    pub body_ref: Option<String>,
    pub reply_to_message_id: Option<String>,
    pub in_reply_to_digest: Option<String>,
    pub requires_reply: bool,
    pub reply_deadline_ms: Value,
    pub evidence_refs: Vec<String>,
    pub proposal_revision_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolveRequest {
    pub client_request_id: String,
    pub thread_id: String,
    pub expected_state_revision: i64,
    pub outcome: String,
    pub resolution_summary: String,
    pub selected_proposal_revision_id: Option<String>,
    pub remaining_objections: Vec<String>,
    pub follow_up_operation_ids: Vec<String>,
    pub manager_ratification_operation_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ChangeRequest {
    pub client_request_id: String,
    pub thread_id: String,
    pub expected_state_revision: i64,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub(crate) struct SupersedeRequest {
    pub client_request_id: String,
    pub thread_id: String,
    pub expected_state_revision: i64,
    pub superseding_thread_id: String,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub(crate) enum ThreadMutation {
    Open(OpenRequest),
    Send(SendRequest),
    Resolve(ResolveRequest),
    Withdraw(ChangeRequest),
    Supersede(SupersedeRequest),
}

/// Parse a single supported mutation. Unknown method names and unknown fields
/// fail closed so frontend and Store schemas cannot silently drift.
pub(crate) fn parse_mutation(method: &str, value: &Value) -> Result<ThreadMutation> {
    enforce_request_size(value)?;
    match method {
        "coordination.thread.open" => parse_open(value).map(ThreadMutation::Open),
        "coordination.message.send" => parse_send(value).map(ThreadMutation::Send),
        "coordination.thread.resolve" => parse_resolve(value).map(ThreadMutation::Resolve),
        "coordination.thread.withdraw" => parse_change(value).map(ThreadMutation::Withdraw),
        "coordination.thread.supersede" => parse_supersede(value).map(ThreadMutation::Supersede),
        _ => Err(Error::new("METHOD_NOT_FOUND", method)),
    }
}

pub fn validate_mutation(method: &str, value: &Value) -> Result<()> {
    parse_mutation(method, value).map(|_| ())
}

pub fn validate_read(method: &str, value: &Value) -> Result<()> {
    enforce_request_size(value)?;
    match method {
        "coordination.thread.get" => {
            model::fields(value, &["thread_id", "after_message_seq", "limit"])?;
            safe_identifier(
                model::text(value, "thread_id")?,
                "thread_id",
                limits::MAX_IDENTIFIER_BYTES,
            )?;
            optional_nonnegative(value, "after_message_seq")?;
            page_limit(value)?;
            Ok(())
        }
        "coordination.thread.list" => {
            model::fields(
                value,
                &[
                    "task_id",
                    "attempt_id",
                    "state",
                    "topic_kind",
                    "limit",
                    "after_thread_id",
                ],
            )?;
            safe_identifier(
                model::text(value, "task_id")?,
                "task_id",
                limits::MAX_IDENTIFIER_BYTES,
            )?;
            optional_identifier(value, "attempt_id", limits::MAX_IDENTIFIER_BYTES)?;
            optional_identifier(value, "after_thread_id", limits::MAX_IDENTIFIER_BYTES)?;
            optional_identifier(value, "topic_kind", 128)?;
            if let Some(state) = optional_text(value, "state", 32)?
                && !matches!(
                    state.as_str(),
                    "open" | "resolved" | "unresolved" | "withdrawn" | "superseded"
                )
            {
                return Err(Error::invalid("state is not a supported Thread state"));
            }
            page_limit(value)?;
            Ok(())
        }
        _ => Err(Error::new("METHOD_NOT_FOUND", method)),
    }
}

fn parse_open(value: &Value) -> Result<OpenRequest> {
    model::fields(
        value,
        &[
            "client_request_id",
            "task_id",
            "attempt_id",
            "assignment_id",
            "supersedes_thread_id",
            "topic_kind",
            "subject",
            "participants",
            "reasonability",
            "related_scopes",
            "body_ref",
            "delivery_mode",
        ],
    )?;
    let client_request_id = request_id(value)?;
    let task_id = identifier(value, "task_id", limits::MAX_IDENTIFIER_BYTES)?;
    let attempt_id = identifier(value, "attempt_id", limits::MAX_IDENTIFIER_BYTES)?;
    let assignment_id = optional_identifier(value, "assignment_id", limits::MAX_IDENTIFIER_BYTES)?;
    let supersedes_thread_id =
        optional_identifier(value, "supersedes_thread_id", limits::MAX_IDENTIFIER_BYTES)?;
    let topic_kind = identifier(value, "topic_kind", 128)?;
    let subject = bounded_text(value, "subject", limits::MAX_SUBJECT_BYTES)?;
    let participants_value = value
        .get("participants")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::invalid("participants must be an array"))?;
    if participants_value.is_empty() {
        return Err(Error::invalid(
            "participants must name at least one exact client",
        ));
    }
    let mut participants = Vec::with_capacity(participants_value.len());
    let mut seen = BTreeSet::new();
    for item in participants_value {
        model::fields(item, &["client_id", "generation", "reason"])?;
        let client_id = identifier(item, "client_id", limits::MAX_CLIENT_ID_BYTES)?;
        if !seen.insert(client_id.clone()) {
            return Err(Error::invalid("participants must not repeat a client_id"));
        }
        let generation = optional_positive(item, "generation")?;
        let reason = bounded_text(item, "reason", limits::MAX_REASON_BYTES)?;
        participants.push(ParticipantSpec {
            client_id,
            generation,
            reason,
        });
    }
    let reasonability = value
        .get("reasonability")
        .filter(|value| value.is_object())
        .ok_or_else(|| Error::invalid("reasonability must be an object"))?;
    model::fields(reasonability, REASONABILITY_FIELDS)?;
    for field in REASONABILITY_FIELDS {
        bounded_text(reasonability, field, limits::MAX_SUMMARY_BYTES)?;
    }
    let related_scopes = parse_related_scopes(value)?;
    let body_ref = optional_identifier(value, "body_ref", limits::MAX_REFERENCE_BYTES)?;
    require_delivery_mode(value)?;
    Ok(OpenRequest {
        client_request_id,
        task_id,
        attempt_id,
        assignment_id,
        supersedes_thread_id,
        topic_kind,
        subject,
        participants,
        reasonability: reasonability.clone(),
        related_scopes,
        body_ref,
    })
}

fn parse_send(value: &Value) -> Result<SendRequest> {
    model::fields(
        value,
        &[
            "client_request_id",
            "thread_id",
            "recipient",
            "speech_act",
            "subject",
            "summary",
            "inline_body",
            "body_ref",
            "reply_to_message_id",
            "in_reply_to_digest",
            "requires_reply",
            "reply_deadline_ms",
            "evidence_refs",
            "proposal_revision_id",
            "delivery_mode",
        ],
    )?;
    let client_request_id = request_id(value)?;
    let thread_id = identifier(value, "thread_id", limits::MAX_IDENTIFIER_BYTES)?;
    let recipient = identifier(value, "recipient", limits::MAX_CLIENT_ID_BYTES)?;
    let speech_act = model::text(value, "speech_act")?.to_owned();
    if !SPEECH_ACTS.contains(&speech_act.as_str()) {
        return Err(Error::invalid("speech_act is not supported"));
    }
    let subject = bounded_text(value, "subject", limits::MAX_SUBJECT_BYTES)?;
    let summary = bounded_text(value, "summary", limits::MAX_SUMMARY_BYTES)?;
    let inline_body = optional_bounded_text(value, "inline_body", limits::MAX_INLINE_BODY_BYTES)?;
    let body_ref = optional_identifier(value, "body_ref", limits::MAX_REFERENCE_BYTES)?;
    if inline_body.is_some() && body_ref.is_some() {
        return Err(Error::invalid(
            "inline_body and body_ref are mutually exclusive",
        ));
    }
    let reply_to_message_id =
        optional_identifier(value, "reply_to_message_id", limits::MAX_IDENTIFIER_BYTES)?;
    let in_reply_to_digest = optional_digest(value, "in_reply_to_digest")?;
    if in_reply_to_digest.is_some() && reply_to_message_id.is_none() {
        return Err(Error::invalid(
            "in_reply_to_digest requires reply_to_message_id",
        ));
    }
    let requires_reply = value
        .get("requires_reply")
        .and_then(Value::as_bool)
        .ok_or_else(|| Error::invalid("requires_reply must be a boolean"))?;
    let reply_deadline_ms = model::deadline(value, "reply_deadline_ms")?;
    let evidence_refs = parse_string_refs(value, "evidence_refs", limits::MAX_EVIDENCE_REFS)?;
    let proposal_revision_id =
        optional_identifier(value, "proposal_revision_id", limits::MAX_IDENTIFIER_BYTES)?;
    require_delivery_mode(value)?;
    Ok(SendRequest {
        client_request_id,
        thread_id,
        recipient,
        speech_act,
        subject,
        summary,
        inline_body,
        body_ref,
        reply_to_message_id,
        in_reply_to_digest,
        requires_reply,
        reply_deadline_ms,
        evidence_refs,
        proposal_revision_id,
    })
}

fn parse_resolve(value: &Value) -> Result<ResolveRequest> {
    model::fields(
        value,
        &[
            "client_request_id",
            "thread_id",
            "expected_state_revision",
            "outcome",
            "resolution_summary",
            "selected_proposal_revision_id",
            "remaining_objections",
            "follow_up_operation_ids",
            "manager_ratification_operation_id",
        ],
    )?;
    let outcome = model::text(value, "outcome")?.to_owned();
    if !matches!(outcome.as_str(), "resolved" | "unresolved" | "withdrawn") {
        return Err(Error::invalid(
            "outcome must be resolved, unresolved, or withdrawn",
        ));
    }
    Ok(ResolveRequest {
        client_request_id: request_id(value)?,
        thread_id: identifier(value, "thread_id", limits::MAX_IDENTIFIER_BYTES)?,
        expected_state_revision: model::positive(value, "expected_state_revision")?,
        outcome,
        resolution_summary: bounded_text(value, "resolution_summary", limits::MAX_SUMMARY_BYTES)?,
        selected_proposal_revision_id: optional_identifier(
            value,
            "selected_proposal_revision_id",
            limits::MAX_IDENTIFIER_BYTES,
        )?,
        remaining_objections: parse_string_refs(value, "remaining_objections", 64)?,
        follow_up_operation_ids: parse_string_refs(value, "follow_up_operation_ids", 64)?,
        manager_ratification_operation_id: optional_identifier(
            value,
            "manager_ratification_operation_id",
            limits::MAX_IDENTIFIER_BYTES,
        )?,
    })
}

fn parse_change(value: &Value) -> Result<ChangeRequest> {
    model::fields(
        value,
        &[
            "client_request_id",
            "thread_id",
            "expected_state_revision",
            "reason",
        ],
    )?;
    Ok(ChangeRequest {
        client_request_id: request_id(value)?,
        thread_id: identifier(value, "thread_id", limits::MAX_IDENTIFIER_BYTES)?,
        expected_state_revision: model::positive(value, "expected_state_revision")?,
        reason: bounded_text(value, "reason", limits::MAX_REASON_BYTES)?,
    })
}

fn parse_supersede(value: &Value) -> Result<SupersedeRequest> {
    model::fields(
        value,
        &[
            "client_request_id",
            "thread_id",
            "expected_state_revision",
            "superseding_thread_id",
            "reason",
        ],
    )?;
    let thread_id = identifier(value, "thread_id", limits::MAX_IDENTIFIER_BYTES)?;
    let superseding_thread_id =
        identifier(value, "superseding_thread_id", limits::MAX_IDENTIFIER_BYTES)?;
    if thread_id == superseding_thread_id {
        return Err(Error::invalid("a Thread cannot supersede itself"));
    }
    Ok(SupersedeRequest {
        client_request_id: request_id(value)?,
        thread_id,
        expected_state_revision: model::positive(value, "expected_state_revision")?,
        superseding_thread_id,
        reason: bounded_text(value, "reason", limits::MAX_REASON_BYTES)?,
    })
}

fn parse_related_scopes(value: &Value) -> Result<Vec<Value>> {
    let Some(raw) = value.get("related_scopes") else {
        return Ok(Vec::new());
    };
    let items = raw
        .as_array()
        .ok_or_else(|| Error::invalid("related_scopes must be an array"))?;
    if items.len() > 64 {
        return Err(Error::invalid(
            "related_scopes may contain at most 64 items",
        ));
    }
    let mut result = Vec::with_capacity(items.len());
    let mut seen = BTreeSet::new();
    for item in items {
        model::fields(item, &["kind", "value"])?;
        let kind = identifier(item, "kind", 64)?;
        let value = bounded_text(item, "value", limits::MAX_REFERENCE_BYTES)?;
        let canonical = model::canonical(&serde_json::json!({"kind":kind,"value":value}))?;
        if !seen.insert(canonical) {
            return Err(Error::invalid("related_scopes entries must be unique"));
        }
        result.push(serde_json::json!({"kind":kind,"value":value}));
    }
    Ok(result)
}

fn parse_string_refs(value: &Value, field: &str, max_items: usize) -> Result<Vec<String>> {
    let Some(raw) = value.get(field) else {
        return Ok(Vec::new());
    };
    let items = raw
        .as_array()
        .ok_or_else(|| Error::invalid(format!("{field} must be an array")))?;
    if items.len() > max_items {
        return Err(Error::invalid(format!(
            "{field} may contain at most {max_items} items"
        )));
    }
    let mut result = Vec::with_capacity(items.len());
    let mut seen = BTreeSet::new();
    for item in items {
        let reference = item
            .as_str()
            .ok_or_else(|| Error::invalid(format!("{field} entries must be strings")))?;
        validate_text(reference, field, limits::MAX_REFERENCE_BYTES)?;
        if !seen.insert(reference.to_owned()) {
            return Err(Error::invalid(format!("{field} entries must be unique")));
        }
        result.push(reference.to_owned());
    }
    Ok(result)
}

fn request_id(value: &Value) -> Result<String> {
    let request_id = model::text(value, "client_request_id")?;
    safe_identifier(
        request_id,
        "client_request_id",
        limits::MAX_CLIENT_REQUEST_ID_BYTES,
    )?;
    Ok(request_id.to_owned())
}

fn identifier(value: &Value, field: &str, max_bytes: usize) -> Result<String> {
    let raw = model::text(value, field)?;
    safe_identifier(raw, field, max_bytes)?;
    Ok(raw.to_owned())
}

fn optional_identifier(value: &Value, field: &str, max_bytes: usize) -> Result<Option<String>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(raw)) => {
            safe_identifier(raw, field, max_bytes)?;
            Ok(Some(raw.clone()))
        }
        Some(_) => Err(Error::invalid(format!("{field} must be text or null"))),
    }
}

fn optional_text(value: &Value, field: &str, max_bytes: usize) -> Result<Option<String>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(raw)) => {
            validate_text(raw, field, max_bytes)?;
            Ok(Some(raw.clone()))
        }
        Some(_) => Err(Error::invalid(format!("{field} must be text or null"))),
    }
}

fn optional_bounded_text(value: &Value, field: &str, max_bytes: usize) -> Result<Option<String>> {
    optional_text(value, field, max_bytes)
}

fn bounded_text(value: &Value, field: &str, max_bytes: usize) -> Result<String> {
    let raw = model::text(value, field)?;
    validate_text(raw, field, max_bytes)?;
    Ok(raw.to_owned())
}

fn validate_text(raw: &str, field: &str, max_bytes: usize) -> Result<()> {
    if raw.trim().is_empty()
        || raw.len() > max_bytes
        || raw.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(Error::invalid(format!(
            "{field} must be nonempty text up to {max_bytes} bytes without control characters"
        )));
    }
    Ok(())
}

fn safe_identifier(raw: &str, field: &str, max_bytes: usize) -> Result<()> {
    if raw.is_empty()
        || raw.len() > max_bytes
        || raw
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(Error::invalid(format!(
            "{field} must be 1..={max_bytes} bytes without whitespace"
        )));
    }
    Ok(())
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

fn optional_nonnegative(value: &Value, field: &str) -> Result<()> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(()),
        Some(raw) if raw.as_i64().is_some_and(|number| number >= 0) => Ok(()),
        Some(_) => Err(Error::invalid(format!(
            "{field} must be a nonnegative integer"
        ))),
    }
}

fn page_limit(value: &Value) -> Result<i64> {
    match value.get("limit") {
        None | Some(Value::Null) => Ok(limits::DEFAULT_READ_PAGE_SIZE),
        Some(raw) => raw
            .as_i64()
            .filter(|number| (1..=limits::MAX_READ_PAGE_SIZE).contains(number))
            .ok_or_else(|| Error::invalid("limit must be 1..=50")),
    }
}

fn optional_digest(value: &Value, field: &str) -> Result<Option<String>> {
    let Some(raw) = optional_text(value, field, 71)? else {
        return Ok(None);
    };
    let Some(hex) = raw.strip_prefix("sha256:") else {
        return Err(Error::invalid(format!(
            "{field} must use sha256:<64 lowercase hex>"
        )));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::invalid(format!(
            "{field} must use sha256:<64 lowercase hex>"
        )));
    }
    Ok(Some(raw))
}

fn require_delivery_mode(value: &Value) -> Result<()> {
    if model::text(value, "delivery_mode")? != "mailbox_only" {
        return Err(Error::invalid("delivery_mode must be mailbox_only"));
    }
    Ok(())
}

fn enforce_request_size(value: &Value) -> Result<()> {
    if model::canonical(value)?.len() > limits::MAX_COORDINATION_REQUEST_BYTES {
        return Err(Error::new(
            "PAYLOAD_TOO_LARGE",
            "coordination request exceeds the 64 KiB UTF-8 envelope",
        ));
    }
    Ok(())
}
