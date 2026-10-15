//! Bounded OpenCode 2.0.7 form (question), permission and session-log contracts.
//! Native text stays data; session-log observations never copy transcript payloads.

use crate::{
    config::NativeOptions,
    journal::{OperationIntent, digest_json},
    native::{NativeClient, canonical_json},
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::Digest;
use std::collections::BTreeSet;
use swarm_contracts::{
    error::{Error, Result},
    runtime::RuntimeCommand,
};

const MAX_PENDING: usize = 64;
const MAX_OPTIONS: usize = 32;
const HISTORY_PAGE_LIMIT: usize = 64;
const MAX_HISTORY_PAGE_BYTES: usize = 512 * 1024;
const MAX_INTERACTION_BYTES: usize = 32 * 1024;
const MAX_NATIVE_TEXT_BYTES: usize = 4096;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DataEnvelope<T> {
    data: T,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionLogPage {
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
    pub http_delete: bool,
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
        "frm_"
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

    let (action, path, body, http_delete, affected_request_ids) = match request.kind.as_str() {
        "question" => {
            let question_count = crate::native_forms::validate_question_form(found)?;
            if crate::native_forms::is_question_reject(&request.body)? {
                (
                    "reject",
                    format!(
                        "/api/session/{}/form/{}",
                        request.session_id, request.request_id
                    ),
                    None,
                    true,
                    vec![request.request_id.clone()],
                )
            } else {
                let (_eliot_answers, native_answer) =
                    crate::native_forms::question_answers_to_native(&request.body, found, question_count)?;
                (
                    "reply",
                    format!(
                        "/api/session/{}/form/{}/reply",
                        request.session_id, request.request_id
                    ),
                    Some(json!({"answer": native_answer})),
                    false,
                    vec![request.request_id.clone()],
                )
            }
        }
        "permission" => {
            validate_permission_request(found)?;
            let (decision, message) = validate_permission_body(&request.body)?;
            let mut body = json!({"decision": decision});
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
                false,
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
        http_delete,
        affected_request_ids,
        request_fingerprint: observed_fingerprint,
        effect_payload_sha256,
        feedback_present,
    })
}

/// Reconcile a possibly-lost reply. Read-only; pending-list disappearance
/// alone is never proof. Permission POSTs are ambiguous and can never be
/// reconciled from absence or session-log: they stay unknown/nonreplay.
/// Question (form) replies are proven only by exact Form.get detail state
/// (fingerprinted native form + exact scope): answered with the exact
/// native answer digest, or cancelled for exact reject. Ephemeral
/// permission/form events are never fabricated into durable v2 history proof.
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
    if saved.kind == "permission" {
        return Err(evidence_unavailable(
            "ambiguous permission POST cannot be reconciled from absence or session-log",
        ));
    }
    if saved.kind != "question" {
        return Err(evidence_unavailable(
            "saved native reply action is outside the readback subset",
        ));
    }
    let only_target = saved.affected_request_ids.len() == 1
        && saved.affected_request_ids.first() == Some(&saved.request_id);
    if !only_target {
        return Err(evidence_unavailable("saved reply identity is invalid"));
    }
    if !matches!(saved.action.as_str(), "reply" | "reject") {
        return Err(evidence_unavailable(
            "saved native reply action is unsupported",
        ));
    }
    let detail = load_form_detail(native, &saved.session_id, &saved.request_id)
        .await
        .map_err(|_| evidence_unavailable("exact native form detail is absent"))?;
    let info_fp = form_info_fingerprint(&detail)
        .map_err(|_| evidence_unavailable("native form detail differs from the exact saved request"))?;
    if info_fp != saved.request_fingerprint {
        return Err(evidence_unavailable(
            "native asked event differs from the exact saved request fingerprint",
        ));
    }
    let state = detail
        .get("state")
        .filter(|v| v.is_object())
        .ok_or_else(|| evidence_unavailable("native form detail state is absent"))?;
    let status = state
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| evidence_unavailable("native form detail state is absent"))?;
    match (saved.action.as_str(), status) {
        ("reply", "answered") => {
            let answer = state
                .get("answer")
                .filter(|v| v.is_object())
                .ok_or_else(|| evidence_unavailable("exact native reply event is absent"))?;
            let expected = digest_json(&json!({"answer": answer}))
                .map_err(|_| evidence_unavailable("exact native reply event is absent"))?;
            if Some(expected) != saved.effect_payload_sha256 {
                return Err(evidence_unavailable(
                    "durable native event does not prove the exact saved reply effect",
                ));
            }
            Ok(())
        }
        ("reject", "cancelled") => {
            if saved.effect_payload_sha256.is_some() {
                return Err(evidence_unavailable(
                    "saved question rejection shape is invalid",
                ));
            }
            Ok(())
        }
        _ => Err(evidence_unavailable("exact native reply event is absent")),
    }
}

async fn load_form_detail(
    native: &NativeClient,
    session_id: &str,
    form_id: &str,
) -> Result<Value> {
    validate_id(session_id, "ses_")?;
    validate_id(form_id, "frm_")?;
    let raw = native
        .control_get(
            &format!("/api/session/{session_id}/form/{form_id}"),
            &[],
        )
        .await
        .map_err(|_| evidence_unavailable("exact native form detail is absent"))?;
    let envelope: DataEnvelope<Value> = serde_json::from_value(raw)
        .map_err(|_| evidence_unavailable("native form detail has an invalid schema"))?;
    let detail = envelope.data;
    if detail.get("id").and_then(Value::as_str) != Some(form_id) {
        return Err(evidence_unavailable("native form detail names another request"));
    }
    if detail.get("sessionID").and_then(Value::as_str) != Some(session_id) {
        return Err(evidence_unavailable("native form detail names another session"));
    }
    let _ = crate::native_forms::validate_question_form(&form_info_only(&detail)?)?;
    Ok(detail)
}

fn form_info_only(detail: &Value) -> Result<Value> {
    let obj = detail
        .as_object()
        .ok_or_else(|| evidence_unavailable("native form detail has an invalid schema"))?;
    let mut info = serde_json::Map::new();
    for key in ["id", "sessionID", "title", "metadata", "fields"] {
        let value = obj
            .get(key)
            .cloned()
            .ok_or_else(|| evidence_unavailable("native form detail has an invalid schema"))?;
        info.insert(key.to_owned(), value);
    }
    Ok(Value::Object(info))
}

fn form_info_fingerprint(detail: &Value) -> Result<String> {
    let info = form_info_only(detail)?;
    native_fingerprint(&info)
}
fn validate_saved_reply_identity(saved: &SavedReplyIdentity) -> Result<()> {
    validate_id(&saved.session_id, "ses_")
        .map_err(|_| evidence_unavailable("saved reply session ID is invalid"))?;
    let prefix = if saved.kind == "question" {
        "frm_"
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

fn evidence_unavailable(message: &'static str) -> Error {
    Error::new("NATIVE_EVIDENCE_UNAVAILABLE", message)
}

/// Read one bounded interaction observation page from the actual durable
/// session log (SSE `/api/experimental/session/:id/log?after=&follow=false`, via
/// Root-owned NativeClient::session_log_page), plus exact pending form and
/// permission projections. History observations never copy transcript
/// payloads: durable events project to {event_id,type,sequence,version} only.
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

    let raw = native.session_log_page(session_id, after_sequence, HISTORY_PAGE_LIMIT).await?;
    let page: SessionLogPage = serde_json::from_value(raw)
        .map_err(|_| schema_error("durable session log page has an unknown or invalid schema"))?;
    if page.data.len() > HISTORY_PAGE_LIMIT
        || serde_json::to_vec(&page.data)
            .map_err(|_| schema_error("session log page cannot be represented as JSON"))?
            .len()
            > MAX_HISTORY_PAGE_BYTES
    {
        return Err(Error::new(
            "NATIVE_RESPONSE_LIMIT",
            "durable session log page exceeds its configured bound",
        ));
    }
    if page.has_more && page.data.is_empty() {
        return Err(schema_error(
            "session log hasMore page did not advance the cursor",
        ));
    }

    let mut expected = after_sequence.saturating_add(1);
    let mut through = after_sequence;
    let mut gap = false;
    let mut events = Vec::with_capacity(page.data.len());
    let mut seen = std::collections::BTreeMap::<u64, (String, String)>::new();
    for event in &page.data {
        let id = text(event, "id", 256)?;
        validate_id(id, "evt_")
            .map_err(|_| schema_error("durable event ID is invalid"))?;
        let kind = text(event, "type", 128)?;
        if kind == "log.synced" {
            return Err(schema_error(
                "session log page carries a sync watermark, not session events",
            ));
        }
        let durable = event
            .get("durable")
            .filter(|value| value.is_object())
            .ok_or_else(|| schema_error("session log event lacks durable identity"))?;
        let aggregate = text(durable, "aggregateID", 256)?;
        let sequence = durable
            .get("seq")
            .and_then(Value::as_u64)
            .ok_or_else(|| schema_error("session log event sequence is invalid"))?;
        let version = durable
            .get("version")
            .and_then(Value::as_u64)
            .filter(|version| *version > 0)
            .ok_or_else(|| schema_error("session log event version is invalid"))?;
        if aggregate != session_id || !event.get("data").is_some_and(Value::is_object) {
            return Err(schema_error(
                "session log event is outside the exact session aggregate",
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
                "session log returned an event before the exclusive cursor",
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
    if page.has_more && through <= after_sequence {
        return Err(schema_error(
            "session log hasMore page did not advance the cursor",
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
            let fp = native_fingerprint(item)?;
            crate::native_forms::project_question_form(item, session_id, &fp)?
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
        "question" => format!("/api/session/{session_id}/form"),
        "permission" => format!("/api/session/{session_id}/permission"),
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
        validate_id(id, if kind == "question" { "frm_" } else { "per_" })?;
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
            crate::native_forms::validate_question_form(item)?;
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

fn validate_permission_request(item: &Value) -> Result<()> {
    exact_native_fields(
        item,
        &["id", "sessionID", "action", "resources"],
        &["save", "metadata", "source", "message"],
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
    if let Some(message) = item.get("message") {
        let _ = bounded_text_value(message, MAX_NATIVE_TEXT_BYTES)?;
    }
    Ok(())
}

fn project_permission_source(source: &Value) -> Result<Value> {
    exact_native_fields(source, &["type", "messageID", "id"], &[])?;
    if text(source, "type", 16)? != "tool" {
        return Err(schema_error("permission source kind is unsupported"));
    }
    Ok(json!({
        "type":"tool",
        "message_id":text(source,"messageID",256)?,
        "call_id":text(source,"id",256)?,
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

fn bounded_text_value(value: &Value, maximum: usize) -> Result<String> {
    value
        .as_str()
        .filter(|t| t.len() <= maximum && !t.chars().any(char::is_control))
        .map(ToOwned::to_owned)
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
