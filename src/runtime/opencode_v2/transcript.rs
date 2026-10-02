//! Durable OpenCode V2 transcript inspection. The event stream is only an
//! invalidation hint; input/terminal identity and result bytes come from ordered GETs.
use super::{Options, Service, input_id, valid_id};
use crate::{
    artifacts::{MAX_PAGE_BYTES, ResultPage},
    error::{Error, Result},
    model,
    runtime::RuntimeCommand,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeSet;

const PAGE_LIMIT: usize = 100;
const MAX_PAGES: usize = 64;
const MAX_MESSAGES: usize = PAGE_LIMIT * MAX_PAGES;
const MAX_TRANSCRIPT_BYTES: usize = 32 * 1024 * 1024;
const MAX_RESULT_BYTES: usize = 32 * 1024 * 1024;
const MESSAGE_TYPES: [&str; 11] = [
    "agent-switched",
    "model-switched",
    "location-switched",
    "user",
    "synthetic",
    "system",
    "skill",
    "shell",
    "assistant",
    "compaction",
    "idle",
];

#[derive(Deserialize)]
struct MessagePage {
    data: Vec<Value>,
    cursor: Cursor,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    next: Option<String>,
    #[serde(rename = "previous")]
    _previous: Option<String>,
}
#[derive(Clone)]
struct OwnedInput {
    id: String,
    operation_id: String,
}
struct TerminalTurn {
    input: OwnedInput,
    idle: Value,
    /// Projected messages strictly after the owned user input and before idle.
    messages: Vec<Value>,
    outcome: &'static str,
    terminal: &'static str,
}
impl TerminalTurn {
    fn id(&self) -> &str {
        self.idle["id"].as_str().unwrap_or_default()
    }
    fn assistant_summary(&self) -> (usize, Option<&str>, Option<&str>) {
        let mut count = 0usize;
        let mut first = None;
        let mut last = None;
        for message in &self.messages {
            if message["type"] == "assistant" {
                count += 1;
                let id = message["id"].as_str();
                first = first.or(id);
                last = id;
            }
        }
        (count, first, last)
    }
    fn summary(&self, session: &str) -> Value {
        let (assistant_count, first_assistant, last_assistant) = self.assistant_summary();
        json!({
            "sessionId":session,
            "inputId":self.input.id,
            // V2 exposes a durable terminal message, not a separate run ID. The
            // exact idle message ID is retained as the generic native run key.
            "turnId":self.id(),
            "identityKind":"terminal_message",
            "operationId":self.input.operation_id,
            "terminal":self.terminal,
            "nativeOutcome":self.outcome,
            "event":"session.message.idle",
            "viewCursor":Value::Null,
            "projectedMessageCount":self.messages.len(),
            "assistantMessageCount":assistant_count,
            "firstAssistantMessageId":first_assistant,
            "lastAssistantMessageId":last_assistant,
        })
    }
    fn document(&self, session: &str) -> Value {
        json!({
            "schema":"eliot.opencode-v2.turn-result.1",
            "session_id":session,
            "input_id":self.input.id,
            "turn_id":self.id(),
            "identity_kind":"terminal_message",
            "operation_id":self.input.operation_id,
            "terminal":self.terminal,
            "native_outcome":self.outcome,
            "projected_messages":self.messages,
            "idle":self.idle,
        })
    }
}

fn validate_message(value: &Value) -> Result<(String, u64)> {
    let id = model::text(value, "id")?.to_owned();
    valid_id(&id, "msg_")?;
    let kind = value["type"]
        .as_str()
        .filter(|kind| MESSAGE_TYPES.contains(kind));
    if kind.is_none() {
        return Err(Error::new(
            "NATIVE_SCHEMA_ERROR",
            "unsupported projected message type",
        ));
    }
    let created = value["time"]["created"]
        .as_u64()
        .ok_or_else(|| Error::new("NATIVE_SCHEMA_ERROR", "message creation time is missing"))?;
    Ok((id, created))
}
fn owned_input(value: &Value, binding: &str, generation: i64) -> Result<Option<OwnedInput>> {
    if value["type"] != "user" {
        return Ok(None);
    }
    let marker = &value["metadata"]["eliot"];
    if !marker.is_object()
        || marker["binding"] != binding
        || marker["generation"].as_i64() != Some(generation)
    {
        return Ok(None);
    }
    model::fields(marker, &["binding", "generation", "operation"])?;
    let operation_id = model::text(marker, "operation")?.to_owned();
    let id = model::text(value, "id")?.to_owned();
    if id != input_id(&operation_id) {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "controller input ID does not match its retained operation",
        ));
    }
    Ok(Some(OwnedInput { id, operation_id }))
}
fn terminal_turns(messages: &[Value], binding: &str, generation: i64) -> Result<Vec<TerminalTurn>> {
    struct Pending {
        input: OwnedInput,
        messages: Vec<Value>,
    }
    let mut pending: Option<Pending> = None;
    let mut turns = Vec::new();
    for message in messages {
        match message["type"].as_str() {
            Some("user") => {
                // An intervening user message makes the earlier input-to-idle
                // interval ambiguous. Never attribute a later session idle to it.
                pending = owned_input(message, binding, generation)?.map(|input| Pending {
                    input,
                    messages: Vec::new(),
                });
            }
            Some("idle") => {
                let Some(current) = pending.take() else {
                    continue;
                };
                let (outcome, terminal) = match message["outcome"].as_str() {
                    Some("succeeded") => ("succeeded", "completed"),
                    Some("failed") => ("failed", "failed"),
                    Some("interrupted") => ("interrupted", "cancelled"),
                    _ => {
                        return Err(Error::new(
                            "NATIVE_SCHEMA_ERROR",
                            "idle message has no supported terminal outcome",
                        ));
                    }
                };
                turns.push(TerminalTurn {
                    input: current.input,
                    idle: message.clone(),
                    messages: current.messages,
                    outcome,
                    terminal,
                });
            }
            Some(_) => {
                if let Some(current) = &mut pending {
                    current.messages.push(message.clone());
                }
            }
            None => {
                return Err(Error::new(
                    "NATIVE_SCHEMA_ERROR",
                    "projected message type is missing",
                ));
            }
        }
    }
    Ok(turns)
}
fn range(input: &Value, key: &str, default: u64) -> Result<u64> {
    match input.get(key) {
        None => Ok(default),
        Some(value) => value
            .as_u64()
            .ok_or_else(|| Error::invalid(format!("{key} must be a nonnegative integer"))),
    }
}

impl Service {
    async fn messages(&self, session: &str) -> Result<Vec<Value>> {
        valid_id(session, "ses")?;
        let mut messages = Vec::new();
        let mut ids = BTreeSet::new();
        let mut cursors = BTreeSet::new();
        let mut cursor: Option<String> = None;
        let mut total_bytes = 0usize;
        let mut last_created = None;
        for _ in 0..MAX_PAGES {
            let mut query = vec![("limit", PAGE_LIMIT.to_string())];
            if let Some(cursor) = &cursor {
                query.push(("cursor", cursor.clone()));
            } else {
                query.push(("order", "asc".into()));
            }
            let page: MessagePage = super::http::decode(
                self.get(&format!("/api/session/{session}/message"), &query)
                    .await?,
            )?;
            if page.data.len() > PAGE_LIMIT {
                return Err(Error::new(
                    "NATIVE_PAGE_LIMIT",
                    "message page exceeds the requested limit",
                ));
            }
            for message in page.data {
                let (id, created) = validate_message(&message)?;
                if !ids.insert(id) {
                    return Err(Error::new(
                        "NATIVE_MESSAGE_DUPLICATE",
                        "message timeline contains a duplicate identity",
                    ));
                }
                if last_created.is_some_and(|previous| created < previous) {
                    return Err(Error::new(
                        "NATIVE_MESSAGE_ORDER",
                        "message timeline is not ordered by creation time",
                    ));
                }
                last_created = Some(created);
                total_bytes = total_bytes
                    .checked_add(serde_json::to_vec(&message)?.len())
                    .ok_or_else(|| {
                        Error::new("NATIVE_RESPONSE_LIMIT", "message timeline size overflow")
                    })?;
                if total_bytes > MAX_TRANSCRIPT_BYTES || messages.len() >= MAX_MESSAGES {
                    return Err(Error::new(
                        "NATIVE_RESPONSE_LIMIT",
                        "message timeline exceeds the bounded inspection limit",
                    ));
                }
                messages.push(message);
            }
            match page.cursor.next {
                None => return Ok(messages),
                Some(next)
                    if !next.is_empty() && next.len() <= 4096 && cursors.insert(next.clone()) =>
                {
                    cursor = Some(next);
                }
                Some(_) => {
                    return Err(Error::new(
                        "NATIVE_CURSOR_CYCLE",
                        "message pagination did not advance",
                    ));
                }
            }
        }
        Err(Error::new(
            "NATIVE_PAGE_LIMIT",
            "message timeline exceeded the page limit",
        ))
    }
    pub(super) async fn transcript_turns(
        &self,
        session: &str,
        binding: &str,
        generation: i64,
    ) -> Result<Vec<Value>> {
        let messages = self.messages(session).await?;
        Ok(terminal_turns(&messages, binding, generation)?
            .iter()
            .map(|turn| turn.summary(session))
            .collect())
    }
    pub(crate) async fn result_page(
        &self,
        command: &RuntimeCommand,
        options: &Options,
    ) -> Result<ResultPage> {
        if command.method != "agent.result" {
            return Err(Error::invalid("result reader requires agent.result"));
        }
        self.verify().await?;
        let root = command
            .native_root_id
            .as_deref()
            .ok_or_else(|| Error::invalid("native root is missing"))?;
        self.verify_binding(root, options, &command.binding_id, command.generation)
            .await?;
        let selector = &command.input["selector"];
        model::fields(
            selector,
            &[
                "kind",
                "session_id",
                "input_id",
                "turn_id",
                "expected_digest",
            ],
        )?;
        if model::text(selector, "kind")? != "turn" {
            return Err(Error::new(
                "UNSUPPORTED_RESULT_KIND",
                "OpenCode V2 currently exports an exact terminal turn",
            ));
        }
        let session = model::text(selector, "session_id")?;
        let selected_input = model::text(selector, "input_id")?;
        let selected_turn = model::text(selector, "turn_id")?;
        valid_id(session, "ses")?;
        valid_id(selected_input, "msg_")?;
        valid_id(selected_turn, "msg_")?;
        if session != root {
            return Err(Error::new(
                "RESULT_OUTSIDE_OBSERVED_FAMILY",
                "result selector is outside the owned native root",
            ));
        }
        let expected_digest = selector
            .get("expected_digest")
            .map(|_| model::text(selector, "expected_digest"))
            .transpose()?;
        let offset = range(&command.input, "offset_bytes", 0)?;
        let length = range(&command.input, "length_bytes", MAX_PAGE_BYTES as u64)?;
        if length == 0 || length > MAX_PAGE_BYTES as u64 {
            return Err(Error::invalid("length_bytes must be 1..65536"));
        }
        let messages = self.messages(root).await?;
        let mut matching = terminal_turns(&messages, &command.binding_id, command.generation)?
            .into_iter()
            .filter(|turn| turn.input.id == selected_input && turn.id() == selected_turn);
        let turn = matching.next().ok_or_else(|| {
            Error::new(
                "RESULT_TURN_NOT_AVAILABLE",
                "exact input/terminal segment is not available",
            )
        })?;
        if matching.next().is_some() {
            return Err(Error::new(
                "NATIVE_IDENTITY_AMBIGUOUS",
                "more than one terminal segment matched the selector",
            ));
        }
        let bytes = model::canonical(&turn.document(root))?.into_bytes();
        if bytes.len() > MAX_RESULT_BYTES {
            return Err(Error::new(
                "NATIVE_RESULT_TOO_LARGE",
                "canonical terminal turn exceeds the result boundary",
            ));
        }
        let content_digest = format!("sha256:{}", model::digest(&bytes));
        if expected_digest.is_some_and(|expected| expected != content_digest.as_str()) {
            return Err(Error::new(
                "RESULT_SOURCE_DIGEST_CHANGED",
                "terminal turn differs from the pinned source digest",
            ));
        }
        let total = bytes.len() as u64;
        if offset > total {
            return Err(Error::new(
                "RESULT_OFFSET_OUT_OF_RANGE",
                "result offset exceeds the canonical terminal turn",
            ));
        }
        let end = total.min(offset.saturating_add(length));
        let page = &bytes[offset as usize..end as usize];
        let whole_digest_verified = offset == 0
            && end == total
            && format!("sha256:{}", model::digest(page)) == content_digest;
        if offset == 0 && end == total && !whole_digest_verified {
            return Err(Error::new(
                "NATIVE_OUTPUT_DIGEST_MISMATCH",
                "complete terminal turn differs from its source digest",
            ));
        }
        let (assistant_count, _, _) = turn.assistant_summary();
        let source = json!({
            "runtime":super::RUNTIME,
            "kind":"turn",
            "native_session_id":root,
            "native_input_id":turn.input.id,
            "native_turn_id":turn.id(),
            "identity_kind":"terminal_message",
            "operation_id":turn.input.operation_id,
            "native_outcome":turn.outcome,
            "terminal":turn.terminal,
            "projected_message_count":turn.messages.len(),
            "assistant_message_count":assistant_count,
            "read_method":"session.message.timeline",
            "digest_basis":"canonical_opencode_v2_turn_json_utf8",
            "content_digest":content_digest,
            "expected_digest_verified":expected_digest.is_some(),
            "whole_digest_verified":whole_digest_verified,
        });
        Ok(ResultPage {
            source,
            offset_bytes: offset,
            byte_length: page.len() as u64,
            total_bytes: total,
            eof: end == total,
            media_type: "application/json".into(),
            content_base64: STANDARD.encode(page),
            page_sha256: model::digest(page),
        })
    }
}
