//! Exact, on-demand projected result reads. Timeline adjacency is deliberately
//! not native run identity and never supplies Task/producer terminal evidence.
use super::{
    Options, Service,
    effects::delivered_matches,
    http::{Data, decode},
    input_id, valid_id,
};
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

const PAGE_MESSAGES: usize = 50;
const MAX_TIMELINE_PAGES: usize = 32;
const MAX_SCAN_BYTES: usize = 8 * 1024 * 1024;

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

fn unavailable(code: &str) -> Error {
    Error::new(
        code,
        "exact projected result is not available under the selected contract",
    )
}
fn native_schema(_: Error) -> Error {
    unavailable("NATIVE_MESSAGE_SCHEMA")
}
fn validate_message(message: &Value, session: &str, expected: Option<&str>) -> Result<()> {
    let id = model::text(message, "id").map_err(native_schema)?;
    valid_id(id, "msg_").map_err(native_schema)?;
    if expected.is_some_and(|expected| expected != id)
        || message.get("sessionID").is_some_and(|id| id != session)
        || message["time"]["created"]
            .as_f64()
            .is_none_or(|n| !n.is_finite() || n < 0.0)
        || message["type"]
            .as_str()
            .is_none_or(|s| s.is_empty() || s.len() > 64)
    {
        return Err(unavailable("NATIVE_MESSAGE_IDENTITY"));
    }
    Ok(())
}
fn complete_assistant(message: &Value) -> Result<()> {
    let created = message["time"]["created"].as_f64().unwrap_or(f64::INFINITY);
    if message["type"] != "assistant"
        || message["time"]["completed"]
            .as_f64()
            .is_none_or(|n| !n.is_finite() || n < created)
        || message.get("finish").is_some_and(|finish| {
            !matches!(
                finish.as_str(),
                Some("stop" | "length" | "tool-calls" | "content-filter" | "error" | "unknown")
            )
        })
        || message["truncated"] == true
    {
        return Err(unavailable("RESULT_MESSAGE_NOT_COMPLETE"));
    }
    model::fields(&message["model"], &["id", "providerID", "variant"]).map_err(native_schema)?;
    for key in ["id", "providerID"] {
        if model::text(&message["model"], key)
            .map_err(native_schema)?
            .len()
            > 256
        {
            return Err(unavailable("NATIVE_MODEL_SCHEMA"));
        }
    }
    if message["model"].get("variant").is_some()
        && model::text(&message["model"], "variant")
            .map_err(native_schema)?
            .len()
            > 256
    {
        return Err(unavailable("NATIVE_MODEL_SCHEMA"));
    }
    let content = message["content"]
        .as_array()
        .ok_or_else(|| unavailable("NATIVE_MESSAGE_SCHEMA"))?;
    for part in content {
        if part["truncated"] == true {
            return Err(unavailable("RESULT_MESSAGE_TRUNCATED"));
        }
        match part["type"].as_str() {
            Some("text" | "reasoning") if part["text"].is_string() => {}
            Some("tool")
                if matches!(
                    part["state"]["status"].as_str(),
                    Some("completed" | "error")
                ) =>
            {
                let created = part["time"]["created"].as_f64().unwrap_or(f64::INFINITY);
                if !created.is_finite()
                    || created < 0.0
                    || part["time"].get("completed").is_some_and(|completed| {
                        completed
                            .as_f64()
                            .is_none_or(|n| !n.is_finite() || n < created)
                    })
                {
                    return Err(unavailable("RESULT_TOOL_NOT_COMPLETE"));
                }
                model::text(part, "id").map_err(native_schema)?;
                model::text(part, "name").map_err(native_schema)?;
            }
            _ => return Err(unavailable("RESULT_CONTENT_NOT_COMPLETE")),
        }
    }
    Ok(())
}

impl Service {
    async fn result_scope(
        &self,
        root: &str,
        session: &str,
        command: &RuntimeCommand,
        options: &Options,
    ) -> Result<Value> {
        self.verify_binding(root, options, &command.binding_id, command.generation)
            .await?;
        self.owns_member(root, session).await?;
        let value = self.session(session).await?;
        // A fork can expose inherited messages, and a staged revert changes the
        // effective history. Neither becomes a new result of this assignment.
        if !value["fork"].is_null() || !value["revert"].is_null() {
            return Err(unavailable("RESULT_HISTORY_UNRESOLVED"));
        }
        Ok(
            json!({"id":value["id"],"parentID":value["parentID"],"projectID":value["projectID"],
            "location":value["location"],"model":value["model"]}),
        )
    }
    async fn message(&self, session: &str, id: &str) -> Result<Value> {
        let message: Data<Value> = decode(
            self.get(&format!("/api/session/{session}/message/{id}"), &[])
                .await?,
        )?;
        validate_message(&message.data, session, Some(id))?;
        Ok(message.data)
    }
    /// Find the exact user ID in an unfiltered ordered projection. Moving past
    /// a newer idle replaces the candidate boundary, so an old input cannot
    /// acquire the latest turn's output. No timestamp/content search is used.
    async fn input_interval(&self, original: &RuntimeCommand, session: &str) -> Result<Vec<Value>> {
        let input = input_id(&original.operation_id);
        let mut cursor: Option<String> = None;
        let mut cursors = BTreeSet::new();
        let mut ids = BTreeSet::new();
        let mut scanned_bytes = 0usize;
        let mut interval = Vec::new();
        for _ in 0..MAX_TIMELINE_PAGES {
            let mut query = vec![("limit", PAGE_MESSAGES.to_string())];
            if let Some(cursor) = &cursor {
                query.push(("cursor", cursor.clone()));
            } else {
                query.push(("order", "desc".into()));
            }
            let raw = self
                .get(&format!("/api/session/{session}/message"), &query)
                .await?;
            scanned_bytes = scanned_bytes.saturating_add(model::canonical(&raw)?.len());
            if scanned_bytes > MAX_SCAN_BYTES {
                return Err(unavailable("RESULT_SCAN_LIMIT"));
            }
            let page: MessagePage = decode(raw)?;
            if page.data.len() > PAGE_MESSAGES
                || (page.data.is_empty() && page.cursor.next.is_some())
            {
                return Err(unavailable("NATIVE_MESSAGE_PAGE"));
            }
            for message in page.data {
                validate_message(&message, session, None)?;
                let id = model::text(&message, "id")?.to_owned();
                if !ids.insert(id.clone()) {
                    return Err(unavailable("NATIVE_MESSAGE_DUPLICATE"));
                }
                if id == input {
                    if !delivered_matches(&message, original)? {
                        return Err(unavailable("NATIVE_INPUT_MISMATCH"));
                    }
                    if interval.is_empty() {
                        return Err(unavailable("RESULT_INTERVAL_NOT_CLOSED"));
                    }
                    interval.push(message);
                    interval.reverse();
                    // Mixed user inputs, synthetic messages, compaction or a
                    // configuration switch are not silently folded into a turn.
                    for item in &interval[1..interval.len() - 1] {
                        complete_assistant(item)?;
                        if item["model"] != json!(original.route["native_options"]["model"]) {
                            return Err(unavailable("RESULT_MODEL_CHANGED"));
                        }
                    }
                    return Ok(interval);
                }
                if message["type"] == "idle" {
                    if !matches!(
                        message["outcome"].as_str(),
                        Some("succeeded" | "failed" | "interrupted")
                    ) {
                        return Err(unavailable("NATIVE_IDLE_SCHEMA"));
                    }
                    interval.clear();
                    interval.push(message);
                } else if !interval.is_empty() {
                    interval.push(message);
                }
            }
            match page.cursor.next {
                None => return Err(unavailable("RESULT_INPUT_NOT_FOUND")),
                Some(next)
                    if !next.is_empty() && next.len() <= 4096 && cursors.insert(next.clone()) =>
                {
                    cursor = Some(next)
                }
                Some(_) => return Err(unavailable("NATIVE_CURSOR_CYCLE")),
            }
        }
        Err(unavailable("RESULT_SCAN_LIMIT"))
    }
    pub(crate) async fn read_result(
        &self,
        command: &RuntimeCommand,
        options: &Options,
        original: Option<&RuntimeCommand>,
    ) -> Result<ResultPage> {
        let selector = &command.input["selector"];
        model::fields(
            selector,
            &[
                "kind",
                "session_id",
                "message_id",
                "input_operation_id",
                "expected_digest",
            ],
        )?;
        let kind = model::text(selector, "kind")?;
        if !matches!(kind, "message" | "input_interval") {
            return Err(Error::new(
                "UNSUPPORTED_RESULT_KIND",
                "use message or input_interval",
            ));
        }
        let session = model::text(selector, "session_id")?;
        valid_id(session, "ses")?;
        let expected = selector
            .get("expected_digest")
            .map(|_| model::text(selector, "expected_digest"))
            .transpose()?;
        if expected.is_some_and(|s| {
            s.strip_prefix("sha256:")
                .is_none_or(|s| s.len() != 64 || !s.bytes().all(|c| c.is_ascii_hexdigit()))
        }) {
            return Err(Error::invalid(
                "expected_digest must be sha256:<64 hex digits>",
            ));
        }
        let root = command
            .native_root_id
            .as_deref()
            .ok_or_else(|| unavailable("NATIVE_ROOT_MISSING"))?;
        self.verify().await?;
        let scope = self.result_scope(root, session, command, options).await?;
        let (document, mut source) = match kind {
            "message" => {
                if selector.get("input_operation_id").is_some() {
                    return Err(Error::invalid(
                        "input_operation_id is only valid for input_interval",
                    ));
                }
                let id = model::text(selector, "message_id")?;
                valid_id(id, "msg_")?;
                let message = self.message(session, id).await?;
                complete_assistant(&message)?;
                if session == root && message["model"] != json!(options.model) {
                    return Err(unavailable("RESULT_MODEL_CHANGED"));
                }
                if self.message(session, id).await? != message {
                    return Err(unavailable("RESULT_SOURCE_CHANGED"));
                }
                let source = json!({"kind":kind,"native_session_id":session,"message_id":id,
                    "model":message["model"],"native_completed_at":message["time"]["completed"],
                    "finish":message["finish"],"read_method":"session.message.get"});
                (message, source)
            }
            "input_interval" => {
                if selector.get("message_id").is_some() {
                    return Err(Error::invalid("message_id is only valid for message"));
                }
                let original =
                    original.ok_or_else(|| Error::invalid("exact input operation is required"))?;
                if model::text(selector, "input_operation_id")? != original.operation_id
                    || original.binding_id != command.binding_id
                    || original.generation != command.generation
                    || original.native_root_id.as_deref() != Some(session)
                    || session != root
                    || !matches!(original.method.as_str(), "task.dispatch" | "agent.send")
                {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "input operation is outside the selected binding/session",
                    ));
                }
                let interval = self.input_interval(original, session).await?;
                if self.input_interval(original, session).await? != interval {
                    return Err(unavailable("RESULT_SOURCE_CHANGED"));
                }
                let idle = interval
                    .last()
                    .ok_or_else(|| unavailable("RESULT_INTERVAL_NOT_CLOSED"))?;
                let source = json!({"kind":kind,"native_session_id":session,"input_operation_id":original.operation_id,
                    "native_input_id":input_id(&original.operation_id),"idle_message_id":idle["id"],"idle_outcome":idle["outcome"],
                    "message_count":interval.len(),"read_method":"session.message.list","correlation":"projected_order_only"});
                (json!({"session_id":session,"messages":interval}), source)
            }
            _ => {
                return Err(Error::new(
                    "UNSUPPORTED_RESULT_KIND",
                    "use message or input_interval",
                ));
            }
        };
        if self.result_scope(root, session, command, options).await? != scope {
            return Err(unavailable("RESULT_SCOPE_CHANGED"));
        }
        self.verify().await?;
        let bytes = model::canonical(&document)?.into_bytes();
        let digest = format!("sha256:{}", model::digest(&bytes));
        if expected.is_some_and(|expected| !expected.eq_ignore_ascii_case(&digest)) {
            return Err(unavailable("RESULT_SOURCE_DIGEST_CHANGED"));
        }
        let offset = command
            .input
            .get("offset_bytes")
            .map_or(Some(0), Value::as_u64)
            .ok_or_else(|| Error::invalid("invalid result offset"))?;
        let length = command
            .input
            .get("length_bytes")
            .map_or(Some(MAX_PAGE_BYTES as u64), Value::as_u64)
            .filter(|n| *n > 0 && *n <= MAX_PAGE_BYTES as u64)
            .ok_or_else(|| Error::invalid("invalid result length"))?;
        let offset = usize::try_from(offset)
            .ok()
            .filter(|n| *n <= bytes.len())
            .ok_or_else(|| Error::invalid("result offset exceeds source length"))?;
        let end = bytes.len().min(offset.saturating_add(length as usize));
        let page = &bytes[offset..end];
        source["content_digest"] = json!(digest);
        source["digest_basis"] = json!("canonical_projected_json");
        source["native_service_version"] = json!(self.version);
        source["reader_revision"] = json!("opencode-projected-result-v1");
        source["read_consistency"] = json!("repeated_equal_projection_not_atomic_snapshot");
        source["execution_complete"] = json!(false);
        source["family_complete"] = json!(false);
        source["whole_digest_verified"] = json!(offset == 0 && end == bytes.len());
        Ok(ResultPage {
            source,
            offset_bytes: offset as u64,
            byte_length: page.len() as u64,
            total_bytes: bytes.len() as u64,
            eof: end == bytes.len(),
            media_type: "application/json".into(),
            content_base64: STANDARD.encode(page),
            page_sha256: model::digest(page),
        })
    }
}
