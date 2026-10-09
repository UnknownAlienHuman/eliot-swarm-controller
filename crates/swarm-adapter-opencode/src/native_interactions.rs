//! Bounded OpenCode V2 question, permission, history and background contracts.
//! Native text stays data; history observations never copy transcript payloads.

use crate::{
    config::NativeOptions,
    journal::{OperationIntent, digest_json},
    native::{NativeClient, canonical_json},
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::Digest;
use std::collections::{BTreeMap, BTreeSet};
use swarm_contracts::{
    error::{Error, Result},
    runtime::RuntimeCommand,
};

const MAX_PENDING: usize = 64;
const MAX_QUESTIONS: usize = 32;
const MAX_OPTIONS: usize = 32;
const MAX_ANSWERS: usize = 32;
const HISTORY_PAGE_LIMIT: usize = 64;
const MAX_HISTORY_PAGE_BYTES: usize = 512 * 1024;
const MAX_REPLY_HISTORY_PAGES: usize = 16;
const MAX_REPLY_HISTORY_BYTES: usize = MAX_HISTORY_PAGE_BYTES * MAX_REPLY_HISTORY_PAGES;
const MAX_INTERACTION_BYTES: usize = 32 * 1024;
const MAX_NATIVE_TEXT_BYTES: usize = 4096;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DataEnvelope<T> {
    data: T,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryPage {
    data: Vec<Value>,
    #[serde(rename = "hasMore")]
    has_more: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplyCommandInput {
    client_request_id: String,
    binding_id: String,
    generation: i64,
    reply: ReplyCommand,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplyCommand {
    kind: String,
    session_id: String,
    request_id: String,
    fingerprint: String,
    body: Value,
}

#[derive(Debug, Clone)]
pub struct PreparedReply {
    pub session_id: String,
    pub request_id: String,
    pub kind: &'static str,
    pub action: &'static str,
    pub path: String,
    pub body: Option<Value>,
    pub affected_request_ids: Vec<String>,
    pub request_fingerprint: String,
    pub effect_payload_sha256: Option<String>,
    pub feedback_present: bool,
}

impl PreparedReply {
    /// Compact pre-effect identity for the durable OperationIntent. Answers,
    /// permission messages and raw native request bodies are intentionally
    /// excluded; the immutable RuntimeCommand digest binds those inputs.
    pub fn intent_identity(&self) -> Value {
        json!({
            "kind":self.kind,
            "action":self.action,
            "session_id":self.session_id,
            "request_id":self.request_id,
            "request_fingerprint":self.request_fingerprint,
            "affected_request_ids":self.affected_request_ids,
            "effect_payload_sha256":self.effect_payload_sha256,
            "feedback_present":self.feedback_present,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedReplyIdentity {
    kind: String,
    action: String,
    session_id: String,
    request_id: String,
    request_fingerprint: String,
    affected_request_ids: Vec<String>,
    effect_payload_sha256: Option<String>,
    feedback_present: bool,
}

#[derive(Debug)]
struct SavedReplyEvent {
    sequence: u64,
    event_type: &'static str,
    effect_payload_sha256: Option<String>,
}

#[derive(Debug)]
pub struct InteractionPage {
    pub event_id: String,
    pub observation: Value,
    pub after_sequence: u64,
    pub through_sequence: u64,
    pub has_more: bool,
    pub coverage: &'static str,
    pub pending_questions: usize,
    pub pending_permissions: usize,
}

pub async fn prepare_reply(
    native: &NativeClient,
    command: &RuntimeCommand,
    options: &NativeOptions,
) -> Result<PreparedReply> {
    let input: ReplyCommandInput = serde_json::from_value(command.input.clone())
        .map_err(|_| invalid("reply command has an unsupported shape"))?;
    if !bounded_text(&input.client_request_id, 256)
        || input.binding_id != command.binding_id
        || input.generation != command.generation
        || !bounded_text(&input.reply.session_id, 256)
        || !bounded_text(&input.reply.request_id, 256)
        || !is_sha256(&input.reply.fingerprint)
        || !matches!(input.reply.kind.as_str(), "question" | "permission")
    {
        return Err(invalid(
            "reply caller request, binding identity, or kind is invalid",
        ));
    }
    let request = &input.reply;
    let id_prefix = if request.kind == "question" {
        "que_"
    } else {
        "per_"
    };
    validate_id(&request.request_id, id_prefix)?;
    native
        .verify_control_scope(command, options, &request.session_id)
        .await?;

    let pending = load_pending(native, &request.session_id, &request.kind).await?;
    let mut matches = pending
        .iter()
        .filter(|item| item["id"] == request.request_id && item["sessionID"] == request.session_id);
    let found = matches.next().ok_or_else(|| {
        Error::new(
            "NATIVE_REQUEST_NOT_PENDING",
            "exact native request is not pending",
        )
    })?;
    if matches.next().is_some() {
        return Err(schema_error("native pending request ID is ambiguous"));
    }
    let observed_fingerprint = native_fingerprint(found)?;
    if observed_fingerprint != request.fingerprint {
        return Err(Error::new(
            "NATIVE_REQUEST_CHANGED",
            "native request changed; refresh its exact fingerprint before replying",
        ));
    }

    let (action, path, body, affected_request_ids) = match request.kind.as_str() {
        "question" => {
            let question_count = validate_question_request(found)?;
            if is_question_reject(&request.body)? {
                (
                    "reject",
                    format!(
                        "/api/session/{}/question/{}/reject",
                        request.session_id, request.request_id
                    ),
                    None,
                    vec![request.request_id.clone()],
                )
            } else {
                let answers = validate_question_answers(&request.body, found, question_count)?;
                (
                    "reply",
                    format!(
                        "/api/session/{}/question/{}/reply",
                        request.session_id, request.request_id
                    ),
                    Some(json!({"answers":answers})),
                    vec![request.request_id.clone()],
                )
            }
        }
        "permission" => {
            validate_permission_request(found)?;
            let (decision, message) = validate_permission_body(&request.body)?;
            let mut body = json!({"reply":decision});
            if let Some(message) = message {
                body["message"] = json!(message);
            }
            let mut affected = if decision == "reject" {
                pending
                    .iter()
                    .map(|item| text(item, "id", 256).map(ToOwned::to_owned))
                    .collect::<Result<Vec<_>>>()?
            } else {
                vec![request.request_id.clone()]
            };
            affected.sort();
            if affected.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(schema_error(
                    "pending permission list contains duplicate request IDs",
                ));
            }
            (
                decision,
                format!(
                    "/api/session/{}/permission/{}/reply",
                    request.session_id, request.request_id
                ),
                Some(body),
                affected,
            )
        }
        _ => return Err(invalid("reply kind is unsupported")),
    };
    let effect_payload_sha256 = body.as_ref().map(digest_json).transpose()?;
    let feedback_present = request.kind == "permission" && request.body.get("message").is_some();

    Ok(PreparedReply {
        session_id: request.session_id.clone(),
        request_id: request.request_id.clone(),
        kind: if request.kind == "question" {
            "question"
        } else {
            "permission"
        },
        action,
        path,
        body,
        affected_request_ids,
        request_fingerprint: observed_fingerprint,
        effect_payload_sha256,
        feedback_present,
    })
}

/// Reconcile a possibly-lost reply from complete, contiguous durable session
/// history. This is read-only; pending-list disappearance alone is not proof.
pub async fn reconcile_reply(
    native: &NativeClient,
    command: &RuntimeCommand,
    options: &NativeOptions,
    intent: &OperationIntent,
) -> Result<()> {
    if intent.method != "agent.reply"
        || intent.binding_id != command.binding_id
        || intent.generation != command.generation
        || intent.native_scope_key != options.scope_key()
        || intent.route_sha256 != digest_json(&serde_json::to_value(options)?)?
    {
        return Err(evidence_unavailable(
            "saved reply intent differs from the exact binding route",
        ));
    }
    let marker = intent
        .marker
        .get("native_opencode_reply")
        .cloned()
        .ok_or_else(|| evidence_unavailable("saved reply intent has no native reply identity"))?;
    let saved: SavedReplyIdentity = serde_json::from_value(marker)
        .map_err(|_| evidence_unavailable("saved reply identity has an unsupported schema"))?;
    validate_saved_reply_identity(&saved)?;
    if saved.feedback_present {
        return Err(evidence_unavailable(
            "native history does not retain permission feedback text",
        ));
    }
    native
        .verify_control_scope(command, options, &saved.session_id)
        .await?;

    let (asked, replies) = read_complete_reply_history(native, &saved).await?;
    let (asked_sequence, fingerprint) = asked
        .get(&saved.request_id)
        .ok_or_else(|| evidence_unavailable("exact native asked event is absent"))?;
    if fingerprint != &saved.request_fingerprint {
        return Err(evidence_unavailable(
            "native asked event differs from the exact saved request fingerprint",
        ));
    }

    match (saved.kind.as_str(), saved.action.as_str()) {
        ("question", "reply") | ("question", "reject") | ("permission", "once") => {
            let event = replies
                .get(&saved.request_id)
                .ok_or_else(|| evidence_unavailable("exact native reply event is absent"))?;
            let expected_type = match (saved.kind.as_str(), saved.action.as_str()) {
                ("question", "reply") => "question.v2.replied",
                ("question", "reject") => "question.v2.rejected",
                ("permission", "once") => "permission.v2.replied",
                _ => unreachable!(),
            };
            if event.sequence <= *asked_sequence
                || event.event_type != expected_type
                || event.effect_payload_sha256 != saved.effect_payload_sha256
            {
                return Err(evidence_unavailable(
                    "durable native event does not prove the exact saved reply effect",
                ));
            }
        }
        ("permission", "reject") => {
            let expected = Some(digest_json(&json!({"reply":"reject"}))?);
            if saved.effect_payload_sha256 != expected {
                return Err(evidence_unavailable(
                    "saved permission rejection payload is not the supported exact effect",
                ));
            }
            for request_id in &saved.affected_request_ids {
                let (request_sequence, _) = asked.get(request_id).ok_or_else(|| {
                    evidence_unavailable("affected permission asked event is absent")
                })?;
                let event = replies.get(request_id).ok_or_else(|| {
                    evidence_unavailable("affected permission reply event is absent")
                })?;
                if event.sequence <= *request_sequence
                    || event.event_type != "permission.v2.replied"
                    || event.effect_payload_sha256 != expected
                {
                    return Err(evidence_unavailable(
                        "permission rejection cascade is not fully confirmed by history",
                    ));
                }
            }
        }
        _ => {
            return Err(evidence_unavailable(
                "saved native reply action is outside the readback subset",
            ));
        }
    }
    Ok(())
}

fn validate_saved_reply_identity(saved: &SavedReplyIdentity) -> Result<()> {
    validate_id(&saved.session_id, "ses_")
        .map_err(|_| evidence_unavailable("saved reply session ID is invalid"))?;
    let prefix = if saved.kind == "question" {
        "que_"
    } else if saved.kind == "permission" {
        "per_"
    } else {
        return Err(evidence_unavailable("saved reply kind is unsupported"));
    };
    validate_id(&saved.request_id, prefix)
        .map_err(|_| evidence_unavailable("saved reply request ID is invalid"))?;
    if !is_sha256(&saved.request_fingerprint)
        || saved.affected_request_ids.is_empty()
        || saved.affected_request_ids.len() > MAX_PENDING
    {
        return Err(evidence_unavailable("saved reply identity is invalid"));
    }
    let mut ids = BTreeSet::new();
    for request_id in &saved.affected_request_ids {
        validate_id(request_id, prefix)
            .map_err(|_| evidence_unavailable("saved affected request ID is invalid"))?;
        if !ids.insert(request_id) {
            return Err(evidence_unavailable(
                "saved reply identity repeats an affected request ID",
            ));
        }
    }
    if !ids.contains(&saved.request_id) {
        return Err(evidence_unavailable(
            "saved reply target is absent from its affected request set",
        ));
    }
    if saved
        .effect_payload_sha256
        .as_deref()
        .is_some_and(|digest| !digest.strip_prefix("sha256:").is_some_and(is_sha256))
    {
        return Err(evidence_unavailable(
            "saved reply payload digest is invalid",
        ));
    }
    let only_target = saved.affected_request_ids.len() == 1
        && saved.affected_request_ids.first() == Some(&saved.request_id);
    match (saved.kind.as_str(), saved.action.as_str()) {
        ("question", "reply") | ("permission", "once") => {
            if !only_target || saved.effect_payload_sha256.is_none() || saved.feedback_present {
                return Err(evidence_unavailable("saved reply effect shape is invalid"));
            }
        }
        ("question", "reject") => {
            if !only_target || saved.effect_payload_sha256.is_some() || saved.feedback_present {
                return Err(evidence_unavailable(
                    "saved question rejection shape is invalid",
                ));
            }
        }
        ("permission", "reject") => {
            if saved.effect_payload_sha256.is_none() {
                return Err(evidence_unavailable(
                    "saved permission rejection digest is absent",
                ));
            }
        }
        _ => {
            return Err(evidence_unavailable(
                "saved native reply action is unsupported",
            ));
        }
    }
    Ok(())
}

async fn read_complete_reply_history(
    native: &NativeClient,
    saved: &SavedReplyIdentity,
) -> Result<(
    BTreeMap<String, (u64, String)>,
    BTreeMap<String, SavedReplyEvent>,
)> {
    let requested = saved
        .affected_request_ids
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut asked = BTreeMap::new();
    let mut replies = BTreeMap::new();
    let mut seen = BTreeMap::<u64, (String, String)>::new();
    let mut after = 0u64;
    let mut expected = 1u64;
    let mut scanned_bytes = 0usize;

    for _ in 0..MAX_REPLY_HISTORY_PAGES {
        let raw = native
            .control_get(
                &format!("/api/session/{}/history", saved.session_id),
                &[
                    ("after", after.to_string()),
                    ("limit", HISTORY_PAGE_LIMIT.to_string()),
                ],
            )
            .await?;
        scanned_bytes = scanned_bytes.saturating_add(canonical_json(&raw)?.len());
        if scanned_bytes > MAX_REPLY_HISTORY_BYTES {
            return Err(evidence_unavailable(
                "durable reply history exceeds its configured read bound",
            ));
        }
        let page: HistoryPage = serde_json::from_value(raw)
            .map_err(|_| evidence_unavailable("durable reply history page schema is invalid"))?;
        if page.data.len() > HISTORY_PAGE_LIMIT || (page.has_more && page.data.is_empty()) {
            return Err(evidence_unavailable(
                "durable reply history page is empty or exceeds its bound",
            ));
        }
        let page_start = after;
        for event in &page.data {
            let event_id = text(event, "id", 256)?;
            validate_id(event_id, "evt_")
                .map_err(|_| evidence_unavailable("durable event ID is invalid"))?;
            let event_type = text(event, "type", 128)?;
            let durable = event
                .get("durable")
                .filter(|value| value.is_object())
                .ok_or_else(|| evidence_unavailable("durable event identity is absent"))?;
            if text(durable, "aggregateID", 256)? != saved.session_id {
                return Err(evidence_unavailable(
                    "durable history event belongs to another session aggregate",
                ));
            }
            let sequence = durable
                .get("seq")
                .and_then(Value::as_u64)
                .filter(|sequence| *sequence > 0)
                .ok_or_else(|| evidence_unavailable("durable event sequence is invalid"))?;
            if durable
                .get("version")
                .and_then(Value::as_u64)
                .is_none_or(|version| version == 0)
            {
                return Err(evidence_unavailable("durable event version is invalid"));
            }
            let event_digest = digest_json(event)?;
            if let Some((previous_id, previous_digest)) = seen.get(&sequence) {
                if previous_id != event_id || previous_digest != &event_digest {
                    return Err(evidence_unavailable(
                        "conflicting durable events share one sequence",
                    ));
                }
                continue;
            }
            if sequence <= after || sequence != expected {
                return Err(evidence_unavailable(
                    "durable reply history has a gap or nonadvancing cursor",
                ));
            }
            seen.insert(sequence, (event_id.to_owned(), event_digest));
            let data = event
                .get("data")
                .filter(|value| value.is_object())
                .ok_or_else(|| evidence_unavailable("durable event data is invalid"))?;
            match event_type {
                "question.v2.asked" => {
                    validate_question_request(data).map_err(|_| {
                        evidence_unavailable("question asked event schema is invalid")
                    })?;
                    let request_id = text(data, "id", 256)?;
                    if requested.contains(request_id) {
                        if text(data, "sessionID", 256)? != saved.session_id {
                            return Err(evidence_unavailable(
                                "question asked event names another session",
                            ));
                        }
                        let fingerprint = native_fingerprint(data)?;
                        if asked
                            .insert(request_id.to_owned(), (sequence, fingerprint))
                            .is_some()
                        {
                            return Err(evidence_unavailable(
                                "question request ID has multiple asked events",
                            ));
                        }
                    }
                }
                "permission.v2.asked" => {
                    validate_permission_request(data).map_err(|_| {
                        evidence_unavailable("permission asked event schema is invalid")
                    })?;
                    let request_id = text(data, "id", 256)?;
                    if requested.contains(request_id) {
                        if text(data, "sessionID", 256)? != saved.session_id {
                            return Err(evidence_unavailable(
                                "permission asked event names another session",
                            ));
                        }
                        let fingerprint = native_fingerprint(data)?;
                        if asked
                            .insert(request_id.to_owned(), (sequence, fingerprint))
                            .is_some()
                        {
                            return Err(evidence_unavailable(
                                "permission request ID has multiple asked events",
                            ));
                        }
                    }
                }
                "question.v2.replied" => {
                    exact_native_fields(data, &["sessionID", "requestID", "answers"], &[])
                        .map_err(|_| {
                            evidence_unavailable("question reply event schema is invalid")
                        })?;
                    let request_id = text(data, "requestID", 256)?;
                    if requested.contains(request_id) {
                        if text(data, "sessionID", 256)? != saved.session_id {
                            return Err(evidence_unavailable(
                                "question reply event names another session",
                            ));
                        }
                        validate_history_answers(&data["answers"])?;
                        insert_reply_event(
                            &mut replies,
                            request_id,
                            SavedReplyEvent {
                                sequence,
                                event_type: "question.v2.replied",
                                effect_payload_sha256: Some(digest_json(&json!({
                                    "answers":data["answers"]
                                }))?),
                            },
                        )?;
                    }
                }
                "question.v2.rejected" => {
                    exact_native_fields(data, &["sessionID", "requestID"], &[]).map_err(|_| {
                        evidence_unavailable("question rejection event schema is invalid")
                    })?;
                    let request_id = text(data, "requestID", 256)?;
                    if requested.contains(request_id) {
                        if text(data, "sessionID", 256)? != saved.session_id {
                            return Err(evidence_unavailable(
                                "question rejection event names another session",
                            ));
                        }
                        insert_reply_event(
                            &mut replies,
                            request_id,
                            SavedReplyEvent {
                                sequence,
                                event_type: "question.v2.rejected",
                                effect_payload_sha256: None,
                            },
                        )?;
                    }
                }
                "permission.v2.replied" => {
                    exact_native_fields(data, &["sessionID", "requestID", "reply"], &[]).map_err(
                        |_| evidence_unavailable("permission reply event schema is invalid"),
                    )?;
                    let request_id = text(data, "requestID", 256)?;
                    if requested.contains(request_id) {
                        if text(data, "sessionID", 256)? != saved.session_id {
                            return Err(evidence_unavailable(
                                "permission reply event names another session",
                            ));
                        }
                        let reply = text(data, "reply", 16)?;
                        if !matches!(reply, "once" | "always" | "reject") {
                            return Err(evidence_unavailable(
                                "permission reply event contains an unknown decision",
                            ));
                        }
                        insert_reply_event(
                            &mut replies,
                            request_id,
                            SavedReplyEvent {
                                sequence,
                                event_type: "permission.v2.replied",
                                effect_payload_sha256: Some(digest_json(&json!({"reply":reply}))?),
                            },
                        )?;
                    }
                }
                _ => {}
            }
            after = sequence;
            expected = sequence.checked_add(1).ok_or_else(|| {
                evidence_unavailable("durable reply history sequence is exhausted")
            })?;
        }
        if page.has_more {
            if after <= page_start {
                return Err(evidence_unavailable(
                    "durable reply history page did not advance",
                ));
            }
            continue;
        }
        return Ok((asked, replies));
    }
    Err(evidence_unavailable(
        "durable reply history exceeds its complete-read page bound",
    ))
}

fn insert_reply_event(
    replies: &mut BTreeMap<String, SavedReplyEvent>,
    request_id: &str,
    event: SavedReplyEvent,
) -> Result<()> {
    if replies.insert(request_id.to_owned(), event).is_some() {
        return Err(evidence_unavailable(
            "native request has multiple durable reply events",
        ));
    }
    Ok(())
}

fn validate_history_answers(value: &Value) -> Result<()> {
    let answers = value
        .as_array()
        .filter(|answers| answers.len() <= MAX_QUESTIONS)
        .ok_or_else(|| evidence_unavailable("question answer event has an invalid shape"))?;
    for group in answers {
        let group = group
            .as_array()
            .filter(|group| group.len() <= MAX_ANSWERS)
            .ok_or_else(|| evidence_unavailable("question answer group exceeds its bound"))?;
        for answer in group {
            if answer.as_str().is_none_or(|answer| {
                answer.trim().is_empty() || answer.len() > MAX_NATIVE_TEXT_BYTES
            }) {
                return Err(evidence_unavailable(
                    "question answer event contains invalid text",
                ));
            }
        }
    }
    Ok(())
}

fn evidence_unavailable(message: &'static str) -> Error {
    Error::new("NATIVE_EVIDENCE_UNAVAILABLE", message)
}

pub async fn read_interaction_page(
    native: &NativeClient,
    command: &RuntimeCommand,
    options: &NativeOptions,
    session_id: &str,
    after_sequence: u64,
) -> Result<InteractionPage> {
    validate_id(session_id, "ses_")?;
    native
        .verify_control_scope(command, options, session_id)
        .await?;

    let raw = native
        .control_get(
            &format!("/api/session/{session_id}/history"),
            &[
                ("after", after_sequence.to_string()),
                ("limit", HISTORY_PAGE_LIMIT.to_string()),
            ],
        )
        .await?;
    let page: HistoryPage = serde_json::from_value(raw)
        .map_err(|_| schema_error("durable history page has an unknown or invalid schema"))?;
    if page.data.len() > HISTORY_PAGE_LIMIT
        || serde_json::to_vec(&page.data)
            .map_err(|_| schema_error("history page cannot be represented as JSON"))?
            .len()
            > MAX_HISTORY_PAGE_BYTES
    {
        return Err(Error::new(
            "NATIVE_RESPONSE_LIMIT",
            "durable history page exceeds its configured bound",
        ));
    }

    let mut expected = after_sequence.saturating_add(1);
    let mut through = after_sequence;
    let mut gap = false;
    let mut events = Vec::with_capacity(page.data.len());
    let mut seen = std::collections::BTreeMap::<u64, (String, String)>::new();
    for event in &page.data {
        let id = text(event, "id", 256)?;
        let kind = text(event, "type", 128)?;
        let durable = event
            .get("durable")
            .filter(|value| value.is_object())
            .ok_or_else(|| schema_error("history event lacks durable identity"))?;
        let aggregate = text(durable, "aggregateID", 256)?;
        let sequence = durable
            .get("seq")
            .and_then(Value::as_u64)
            .ok_or_else(|| schema_error("history event sequence is invalid"))?;
        let version = durable
            .get("version")
            .and_then(Value::as_u64)
            .filter(|version| *version > 0)
            .ok_or_else(|| schema_error("history event version is invalid"))?;
        if aggregate != session_id || !event.get("data").is_some_and(Value::is_object) {
            return Err(schema_error(
                "history event is outside the exact session aggregate",
            ));
        }
        let digest = digest_json(event)?;
        if let Some((previous_id, previous_digest)) = seen.get(&sequence) {
            if previous_id != id || previous_digest != &digest {
                return Err(schema_error(
                    "conflicting durable events share one sequence",
                ));
            }
            continue;
        }
        seen.insert(sequence, (id.to_owned(), digest));
        if sequence <= after_sequence {
            return Err(schema_error(
                "history endpoint returned an event before the exclusive cursor",
            ));
        }
        if sequence != expected {
            gap = true;
            break;
        }
        events.push(json!({"event_id":id,"type":kind,"sequence":sequence,"version":version}));
        through = sequence;
        expected = sequence.saturating_add(1);
    }
    if page.has_more && (page.data.is_empty() || through <= after_sequence) {
        return Err(schema_error(
            "history hasMore page did not advance the cursor",
        ));
    }

    let questions = read_pending_projection(native, session_id, "question").await?;
    let permissions = read_pending_projection(native, session_id, "permission").await?;
    let coverage = if gap {
        "gap"
    } else if page.has_more {
        "partial"
    } else {
        "complete_to_cursor"
    };
    let state = json!({
        "schema":"swarm.opencode_interaction_observation@1",
        "session_id":session_id,
        "after_sequence":after_sequence,
        "through_sequence":through,
        "coverage":coverage,
        "has_more":page.has_more,
        "events":events,
        "pending_questions":questions,
        "pending_permissions":permissions,
    });
    let event_id = format!("opencode-refresh-{}", native_fingerprint(&state)?);
    Ok(InteractionPage {
        event_id,
        observation: state,
        after_sequence,
        through_sequence: through,
        has_more: page.has_more,
        coverage,
        pending_questions: questions.len(),
        pending_permissions: permissions.len(),
    })
}

pub async fn background_capability(native: &NativeClient) -> Result<bool> {
    #[derive(Deserialize)]
    struct Capabilities {
        #[serde(rename = "backgroundSubagents")]
        background_subagents: bool,
    }

    let value = native
        .control_get("/experimental/capabilities", &[])
        .await?;
    let capabilities: Capabilities = serde_json::from_value(value)
        .map_err(|_| schema_error("experimental capability response is invalid"))?;
    Ok(capabilities.background_subagents)
}

pub fn validate_target(command: &RuntimeCommand, field: &str) -> Result<String> {
    let root = command
        .native_root_id
        .as_deref()
        .ok_or_else(|| Error::new("NATIVE_ROOT_MISSING", "native root is missing"))?;
    match command.input.get(field) {
        None | Some(Value::Null) => Ok(root.to_owned()),
        Some(value) => {
            let target = value
                .as_str()
                .filter(|target| !target.trim().is_empty())
                .ok_or_else(|| invalid("target session ID is invalid"))?;
            validate_id(target, "ses_")?;
            Ok(target.to_owned())
        }
    }
}

async fn read_pending_projection(
    native: &NativeClient,
    session_id: &str,
    kind: &str,
) -> Result<Vec<Value>> {
    let pending = load_pending(native, session_id, kind).await?;
    let mut projected = Vec::with_capacity(pending.len());
    for item in &pending {
        projected.push(if kind == "question" {
            project_question(item, session_id)?
        } else {
            project_permission(item, session_id)?
        });
    }
    projected.sort_by(|left, right| {
        left["request_id"]
            .as_str()
            .cmp(&right["request_id"].as_str())
    });
    Ok(projected)
}

async fn load_pending(native: &NativeClient, session_id: &str, kind: &str) -> Result<Vec<Value>> {
    validate_id(session_id, "ses_")?;
    let path = match kind {
        "question" | "permission" => format!("/api/session/{session_id}/{kind}"),
        _ => return Err(invalid("native interaction kind is unsupported")),
    };
    let value = native.control_get(&path, &[]).await?;
    let envelope: DataEnvelope<Vec<Value>> = serde_json::from_value(value)
        .map_err(|_| schema_error("pending interaction list has an invalid schema"))?;
    if envelope.data.len() > MAX_PENDING {
        return Err(Error::new(
            "NATIVE_RESPONSE_LIMIT",
            "pending interaction list is too large",
        ));
    }
    let mut request_ids = BTreeSet::new();
    for item in &envelope.data {
        let id = text(item, "id", 256)?;
        validate_id(id, if kind == "question" { "que_" } else { "per_" })?;
        if !request_ids.insert(id) {
            return Err(schema_error(
                "pending interaction list contains duplicate request IDs",
            ));
        }
        if text(item, "sessionID", 256)? != session_id {
            return Err(schema_error(
                "pending interaction belongs to another session",
            ));
        }
        if kind == "question" {
            validate_question_request(item)?;
        } else {
            validate_permission_request(item)?;
        }
        if serde_json::to_vec(item)
            .map_err(|_| schema_error("pending interaction cannot be represented as JSON"))?
            .len()
            > MAX_INTERACTION_BYTES
        {
            return Err(Error::new(
                "NATIVE_RESPONSE_LIMIT",
                "pending interaction exceeds its bound",
            ));
        }
    }
    Ok(envelope.data)
}

fn project_question(item: &Value, session_id: &str) -> Result<Value> {
    let count = validate_question_request(item)?;
    let mut questions = Vec::with_capacity(count);
    for question in item["questions"].as_array().into_iter().flatten() {
        let options = question["options"]
            .as_array()
            .ok_or_else(|| schema_error("question options are missing"))?;
        if options.len() > MAX_OPTIONS {
            return Err(Error::new(
                "NATIVE_RESPONSE_LIMIT",
                "question option list is too large",
            ));
        }
        let mut projected_options = Vec::with_capacity(options.len());
        for option in options {
            projected_options.push(json!({
                "label":text(option,"label",MAX_NATIVE_TEXT_BYTES)?,
                "description":text(option,"description",MAX_NATIVE_TEXT_BYTES)?,
            }));
        }
        questions.push(json!({
            "question":text(question,"question",MAX_NATIVE_TEXT_BYTES)?,
            "header":text(question,"header",MAX_NATIVE_TEXT_BYTES)?,
            "options":projected_options,
            "multiple":question.get("multiple").and_then(Value::as_bool).unwrap_or(false),
            // OpenCode defaults custom answers to true when this field is absent.
            "custom":question.get("custom").and_then(Value::as_bool).unwrap_or(true),
        }));
    }
    let mut result = json!({
        "session_id":session_id,
        "request_id":item["id"],
        "fingerprint":native_fingerprint(item)?,
        "questions":questions,
    });
    if let Some(tool) = item.get("tool") {
        if !tool.is_null() {
            let message_id = text(tool, "messageID", 256)?;
            let call_id = text(tool, "callID", 256)?;
            result["tool"] = json!({"message_id":message_id,"call_id":call_id});
        }
    }
    enforce_interaction_size(&result)?;
    Ok(result)
}

fn project_permission(item: &Value, session_id: &str) -> Result<Value> {
    let mut result = json!({
        "session_id":session_id,
        "request_id":text(item,"id",256)?,
        "fingerprint":native_fingerprint(item)?,
        "action":text(item,"action",MAX_NATIVE_TEXT_BYTES)?,
        "resources":bounded_string_array(item.get("resources"), MAX_OPTIONS, MAX_NATIVE_TEXT_BYTES)?,
    });
    if let Some(save) =
        optional_bounded_string_array(item.get("save"), MAX_OPTIONS, MAX_NATIVE_TEXT_BYTES)?
    {
        result["save"] = json!(save);
    }
    if let Some(metadata) = item.get("metadata") {
        enforce_interaction_size(metadata)?;
        result["metadata"] = metadata.clone();
    }
    if let Some(source) = item.get("source") {
        result["source"] = project_permission_source(source)?;
    }
    enforce_interaction_size(&result)?;
    Ok(result)
}

fn validate_question_request(item: &Value) -> Result<usize> {
    exact_native_fields(item, &["id", "sessionID", "questions"], &["tool"])?;
    let questions = item["questions"]
        .as_array()
        .filter(|questions| !questions.is_empty() && questions.len() <= MAX_QUESTIONS)
        .ok_or_else(|| schema_error("native question list is empty or too large"))?;
    for question in questions {
        exact_native_fields(
            question,
            &["question", "header", "options"],
            &["multiple", "custom"],
        )?;
        text(question, "question", MAX_NATIVE_TEXT_BYTES)?;
        text(question, "header", MAX_NATIVE_TEXT_BYTES)?;
        let options = question["options"]
            .as_array()
            .filter(|options| options.len() <= MAX_OPTIONS)
            .ok_or_else(|| schema_error("question options have an invalid shape"))?;
        for option in options {
            exact_native_fields(option, &["label", "description"], &[])?;
            text(option, "label", MAX_NATIVE_TEXT_BYTES)?;
            text(option, "description", MAX_NATIVE_TEXT_BYTES)?;
        }
        if question
            .get("multiple")
            .is_some_and(|value| !value.is_boolean())
            || question
                .get("custom")
                .is_some_and(|value| !value.is_boolean())
        {
            return Err(schema_error("question option flags are invalid"));
        }
    }
    if let Some(tool) = item.get("tool") {
        exact_native_fields(tool, &["messageID", "callID"], &[])?;
        text(tool, "messageID", 256)?;
        text(tool, "callID", 256)?;
    }
    Ok(questions.len())
}

fn validate_question_answers(
    body: &Value,
    request: &Value,
    question_count: usize,
) -> Result<Vec<Vec<String>>> {
    exact_object(body, &["answers"])?;
    let answers = body["answers"]
        .as_array()
        .filter(|answers| answers.len() == question_count && answers.len() <= MAX_QUESTIONS)
        .ok_or_else(|| invalid("answer groups must match the ordered native questions"))?;
    let questions = request["questions"]
        .as_array()
        .ok_or_else(|| schema_error("native question list is invalid"))?;
    let mut result = Vec::with_capacity(answers.len());
    for (answers, question) in answers.iter().zip(questions) {
        let selected = answers
            .as_array()
            .filter(|selected| selected.len() <= MAX_ANSWERS)
            .ok_or_else(|| invalid("each native question requires a bounded answer list"))?;
        let multiple = question
            .get("multiple")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let custom = question
            .get("custom")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        if (!multiple && selected.len() > 1) || (multiple && selected.len() > MAX_OPTIONS) {
            return Err(invalid(
                "answer count conflicts with native multiple-selection rules",
            ));
        }
        let options = question["options"]
            .as_array()
            .ok_or_else(|| schema_error("native question options are invalid"))?;
        let labels = options
            .iter()
            .filter_map(|option| option["label"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let mut group = Vec::with_capacity(selected.len());
        for answer in selected {
            let answer = answer
                .as_str()
                .filter(|answer| !answer.trim().is_empty() && answer.len() <= MAX_NATIVE_TEXT_BYTES)
                .ok_or_else(|| invalid("native answer is empty or too large"))?;
            if !custom && !labels.contains(answer) {
                return Err(invalid("answer is not an offered native option"));
            }
            if group.iter().any(|previous| previous == answer) {
                return Err(invalid("duplicate answer is not allowed"));
            }
            group.push(answer.to_owned());
        }
        result.push(group);
    }
    Ok(result)
}

fn is_question_reject(body: &Value) -> Result<bool> {
    if body.get("decision").and_then(Value::as_str) != Some("reject") {
        return Ok(false);
    }
    exact_object(body, &["decision"])?;
    Ok(true)
}

fn validate_permission_request(item: &Value) -> Result<()> {
    exact_native_fields(
        item,
        &["id", "sessionID", "action", "resources"],
        &["save", "metadata", "source"],
    )?;
    text(item, "action", MAX_NATIVE_TEXT_BYTES)?;
    let _ = bounded_string_array(item.get("resources"), MAX_OPTIONS, MAX_NATIVE_TEXT_BYTES)?;
    if let Some(save) = item.get("save") {
        let _ = bounded_string_array(Some(save), MAX_OPTIONS, MAX_NATIVE_TEXT_BYTES)?;
    }
    if let Some(metadata) = item.get("metadata") {
        if !metadata.is_object() {
            return Err(schema_error("permission metadata must be an object"));
        }
        enforce_interaction_size(metadata)?;
    }
    if let Some(source) = item.get("source") {
        let _ = project_permission_source(source)?;
    }
    Ok(())
}

fn project_permission_source(source: &Value) -> Result<Value> {
    exact_native_fields(source, &["type", "messageID", "callID"], &[])?;
    if text(source, "type", 16)? != "tool" {
        return Err(schema_error("permission source kind is unsupported"));
    }
    Ok(json!({
        "type":"tool",
        "message_id":text(source,"messageID",256)?,
        "call_id":text(source,"callID",256)?,
    }))
}

fn validate_permission_body(body: &Value) -> Result<(&'static str, Option<&str>)> {
    exact_object(body, &["decision", "message"])?;
    let decision = body["decision"]
        .as_str()
        .filter(|decision| matches!(*decision, "once" | "reject"))
        .ok_or_else(|| invalid("generic permission reply supports only once or reject"))?;
    let message = body
        .get("message")
        .map(|value| {
            value
                .as_str()
                .filter(|message| message.len() <= MAX_NATIVE_TEXT_BYTES)
                .ok_or_else(|| invalid("permission reply message is too large"))
        })
        .transpose()?;
    if message.is_some() && decision != "reject" {
        return Err(invalid("permission reply message is valid only for reject"));
    }
    Ok((if decision == "once" { "once" } else { "reject" }, message))
}

fn bounded_string_array(value: Option<&Value>, count: usize, bytes: usize) -> Result<Vec<String>> {
    let values = value
        .and_then(Value::as_array)
        .filter(|values| values.len() <= count)
        .ok_or_else(|| schema_error("native string list has an invalid shape"))?;
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|text| text.len() <= bytes)
                .map(ToOwned::to_owned)
                .ok_or_else(|| schema_error("native string list contains invalid text"))
        })
        .collect()
}

fn optional_bounded_string_array(
    value: Option<&Value>,
    count: usize,
    bytes: usize,
) -> Result<Option<Vec<String>>> {
    match value {
        None => Ok(None),
        Some(value) => bounded_string_array(Some(value), count, bytes).map(Some),
    }
}

fn exact_native_fields(value: &Value, required: &[&str], optional: &[&str]) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| schema_error("native interaction object has an invalid shape"))?;
    if required.iter().any(|field| !object.contains_key(*field))
        || object
            .keys()
            .any(|field| !required.contains(&field.as_str()) && !optional.contains(&field.as_str()))
    {
        return Err(schema_error(
            "native interaction has missing or unknown fields",
        ));
    }
    Ok(())
}

fn exact_object<'a>(
    value: &'a Value,
    allowed: &[&str],
) -> Result<&'a serde_json::Map<String, Value>> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid("native reply body must be an object"))?;
    if object.is_empty() || object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(invalid("native reply body contains unsupported fields"));
    }
    Ok(object)
}

fn text<'a>(value: &'a Value, field: &str, maximum: usize) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty() && text.len() <= maximum)
        .ok_or_else(|| schema_error("native interaction field is missing or invalid"))
}

fn validate_id(value: &str, prefix: &str) -> Result<()> {
    if !value.starts_with(prefix)
        || value.len() <= prefix.len()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
    {
        return Err(invalid("native interaction identifier is invalid"));
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn native_fingerprint(value: &Value) -> Result<String> {
    let canonical = crate::native::canonical_json(value)?;
    Ok(format!("{:x}", sha2::Sha256::digest(canonical.as_bytes())))
}

fn bounded_text(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

fn enforce_interaction_size(value: &Value) -> Result<()> {
    if serde_json::to_vec(value)
        .map_err(|_| schema_error("native interaction cannot be represented as JSON"))?
        .len()
        > MAX_INTERACTION_BYTES
    {
        return Err(Error::new(
            "NATIVE_RESPONSE_LIMIT",
            "native interaction exceeds its bound",
        ));
    }
    Ok(())
}

fn invalid(message: &'static str) -> Error {
    Error::new("INVALID_NATIVE_INTERACTION", message)
}

fn schema_error(message: &'static str) -> Error {
    Error::new("NATIVE_SCHEMA_ERROR", message)
}
