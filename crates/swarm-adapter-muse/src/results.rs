//! Read-only, exact-revision native result paging.
//!
//! Wire reference: official MSP 1.3.0, a7c10c5dd3f66be412077d29f9d11111af70317b,
//! Item/ViewPage/ItemReadOutput definitions; ELIOT bridge.13 results.mjs.
//! The caller supplies CURRENT authorized binding/family/cursor membership.
//! Native references are data: only view/page and item/readOutput are called.
//! A page is native evidence, never Task acceptance or execution authority.
//!
//! This new Rust artifact uses versioned serde-JSON result bytes (preserving
//! integer values) rather than relabelling historical Node canonical bytes.
//! Message UTF-8 and native stored-output digests retain their byte semantics.

use crate::msp::{MspTransport, RequestError};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, fmt, future::Future};
use swarm_contracts::{
    ModuleReceiptIdentity, NormalizedResultOriginContext, NormalizedResultPageSource,
    RuntimeCommand, module_catalog::Sha256Digest,
};

pub const MAX_PAGE_BYTES: u64 = 65_536;
pub const MAX_HISTORY_PAGES: usize = 64;
const MAX_CURSOR_BYTES: usize = 1024;
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResultKind {
    Result,
    Message,
    Output,
    Patch,
}

impl ResultKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Result => "result",
            Self::Message => "message",
            Self::Output => "output",
            Self::Patch => "patch",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultSelector {
    kind: ResultKind,
    input_operation_id: Option<String>,
    session_id: String,
    item_id: String,
    item_revision: u64,
    output_ref: Option<String>,
    expected_digest: Option<String>,
    before_cursor: Option<String>,
}

impl ResultSelector {
    pub fn input_operation_id(&self) -> Option<&str> {
        self.input_operation_id.as_deref()
    }
    pub fn kind(&self) -> ResultKind {
        self.kind
    }
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
    pub fn item_id(&self) -> &str {
        &self.item_id
    }
    pub fn item_revision(&self) -> u64 {
        self.item_revision
    }
    pub fn output_ref(&self) -> Option<&str> {
        self.output_ref.as_deref()
    }
    pub fn expected_digest(&self) -> Option<&str> {
        self.expected_digest.as_deref()
    }
    pub fn before_cursor(&self) -> Option<&str> {
        self.before_cursor.as_deref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultRequest {
    selector: ResultSelector,
    offset_bytes: u64,
    length_bytes: u64,
}

impl ResultRequest {
    pub fn parse(input: &Value) -> Result<Self, ResultReadError> {
        let selector = input
            .get("selector")
            .and_then(Value::as_object)
            .ok_or_else(|| error("RESULT_SELECTOR_REQUIRED"))?;
        if selector.keys().any(|key| {
            ![
                "kind",
                "input_operation_id",
                "session_id",
                "item_id",
                "item_revision",
                "output_ref",
                "expected_digest",
                "before_cursor",
            ]
            .contains(&key.as_str())
        }) {
            return Err(error("UNKNOWN_RESULT_SELECTOR_FIELD"));
        }
        let value = &input["selector"];
        let kind = match value["kind"].as_str() {
            Some("result") => ResultKind::Result,
            Some("message") => ResultKind::Message,
            Some("output") => ResultKind::Output,
            Some("patch") => ResultKind::Patch,
            _ => return Err(error("UNSUPPORTED_RESULT_KIND")),
        };
        let selected = ResultSelector {
            kind,
            input_operation_id: optional_text(value, "input_operation_id")?,
            session_id: text(value, "session_id")?.to_owned(),
            item_id: text(value, "item_id")?.to_owned(),
            item_revision: integer(value.get("item_revision"), None, 1)?,
            output_ref: optional_text(value, "output_ref")?,
            expected_digest: optional_text(value, "expected_digest")?,
            before_cursor: optional_cursor_text(value, "before_cursor")?,
        };
        if matches!(kind, ResultKind::Result | ResultKind::Message) && selected.output_ref.is_some()
        {
            return Err(error("OUTPUT_REF_NOT_APPLICABLE"));
        }
        if matches!(kind, ResultKind::Output | ResultKind::Patch) && selected.output_ref.is_none() {
            return Err(error("OUTPUT_REFERENCE_REQUIRED"));
        }
        let offset_bytes = integer(input.get("offset_bytes"), Some(0), 0)?;
        let length_bytes = integer(input.get("length_bytes"), Some(MAX_PAGE_BYTES), 1)?;
        if length_bytes > MAX_PAGE_BYTES {
            return Err(error("RESULT_PAGE_TOO_LARGE"));
        }
        Ok(Self {
            selector: selected,
            offset_bytes,
            length_bytes,
        })
    }

    pub fn selector(&self) -> &ResultSelector {
        &self.selector
    }
    pub fn offset_bytes(&self) -> u64 {
        self.offset_bytes
    }
    pub fn length_bytes(&self) -> u64 {
        self.length_bytes
    }

    /// A continuation is still scoped data, not authorization for a new read.
    pub fn to_input(&self) -> Value {
        let s = &self.selector;
        let mut selector = json!({
            "kind":s.kind.as_str(), "session_id":s.session_id,
            "item_id":s.item_id, "item_revision":s.item_revision,
        });
        for (key, value) in [
            ("input_operation_id", &s.input_operation_id),
            ("output_ref", &s.output_ref),
            ("expected_digest", &s.expected_digest),
            ("before_cursor", &s.before_cursor),
        ] {
            if let Some(value) = value {
                selector[key] = json!(value);
            }
        }
        json!({"selector":selector,"offset_bytes":self.offset_bytes,"length_bytes":self.length_bytes})
    }
}

/// Store's sealed origin is provenance, never a replacement for the retained
/// dispatch or the reader's current opaque SessionAuthority.
pub(crate) struct NormalizedResultContext {
    command: RuntimeCommand,
    receipt: ModuleReceiptIdentity,
    origin: NormalizedResultOriginContext,
    request: ResultRequest,
    native_turn_id: String,
}

pub(crate) fn validate_normalized_request(
    command: &RuntimeCommand,
    receipt: &ModuleReceiptIdentity,
) -> Result<(ResultRequest, NormalizedResultOriginContext), ResultReadError> {
    let input = command
        .input
        .as_object()
        .ok_or_else(|| error("RESULT_CONTEXT_INVALID"))?;
    if input.keys().any(|key| {
        ![
            "client_request_id",
            "binding_id",
            "generation",
            "selector",
            "offset_bytes",
            "length_bytes",
            "normalized_result_origin",
            "normalized_result_payload_identity",
        ]
        .contains(&key.as_str())
    }) {
        return Err(error("RESULT_CONTEXT_INVALID"));
    }
    if let Some(expected) = input.get("normalized_result_payload_identity") {
        let expected = expected
            .as_object()
            .ok_or_else(|| error("RESULT_PAYLOAD_MISMATCH"))?;
        if expected.len() != 3
            || !expected
                .get("sha256")
                .and_then(Value::as_str)
                .is_some_and(|digest| {
                    digest.len() == 64
                        && digest
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                })
            || expected
                .get("byte_length")
                .and_then(Value::as_u64)
                .is_none_or(|bytes| bytes > MAX_SAFE_INTEGER)
            || expected.get("complete").and_then(Value::as_bool) != Some(true)
        {
            return Err(error("RESULT_PAYLOAD_MISMATCH"));
        }
    }
    let request = ResultRequest::parse(&command.input)?;
    let origin: NormalizedResultOriginContext =
        serde_json::from_value(command.input["normalized_result_origin"].clone())
            .map_err(|_| error("RESULT_ORIGIN_INVALID"))?;
    origin
        .validate()
        .map_err(|_| error("RESULT_ORIGIN_INVALID"))?;
    receipt
        .validate()
        .map_err(|_| error("RESULT_CONTEXT_INVALID"))?;
    let selector_sha256 = Sha256Digest::of_canonical_json(&command.input["selector"])
        .map_err(|_| error("RESULT_CONTEXT_INVALID"))?;
    if command.method != "agent.result"
        || command.operation_id != receipt.operation_id
        || command.binding_id != receipt.binding_id
        || command.generation != receipt.binding_generation
        || command.input_sha256.as_deref() != Some(receipt.input_sha256.as_str())
        || origin.binding_id != command.binding_id
        || origin.binding_generation != command.generation
        || command.target_input_sha256.as_deref() != Some(origin.target_input_sha256.as_str())
        || request.selector.input_operation_id() != Some(origin.target_operation_id.as_str())
        || origin.target_operation_id == command.operation_id
        || origin.selector_sha256 != selector_sha256.as_str()
        || origin.producer.module_receipt.module_id != receipt.module_id
        || origin.producer.module_receipt.artifact != receipt.artifact
        || origin.producer.module_receipt.protocol != receipt.protocol
        || input
            .get("binding_id")
            .is_some_and(|id| id.as_str() != Some(command.binding_id.as_str()))
        || input
            .get("generation")
            .is_some_and(|generation| generation.as_i64() != Some(command.generation))
        || command.native_root_id.as_deref() != Some(request.selector.session_id())
    {
        return Err(error("RESULT_ORIGIN_MISMATCH"));
    }
    Ok((request, origin))
}

impl NormalizedResultContext {
    pub(crate) fn for_dispatch(
        command: &RuntimeCommand,
        receipt: &ModuleReceiptIdentity,
        dispatch_receipt: &ModuleReceiptIdentity,
        task: &crate::commands::ValidatedTaskPrompt,
        native_input_id: &str,
        native_turn_id: &str,
    ) -> Result<Self, ResultReadError> {
        let (request, origin) = validate_normalized_request(command, receipt)?;
        let c = &task.context;
        let producer = &origin.producer;
        if origin.producer.module_receipt != *dispatch_receipt
            || origin.target_operation_id != c.operation_id
            || origin.target_input_sha256 != dispatch_receipt.input_sha256
            || origin.task_id != c.task_id
            || origin.task_revision != c.task_revision
            || origin.task_snapshot_sha256 != c.task_snapshot_sha256
            || origin.attempt_id != c.attempt_id
            || producer.source_text_sha256 != c.source_text_sha256
            || producer.source_text_bytes != c.source_text_bytes
            || producer.native_payload_sha256 != task.envelope.prompt_sha256
            || producer.native_payload_bytes != task.envelope.prompt_bytes
            || producer.native_input_id.as_deref() != Some(native_input_id)
            || producer.completion_condition != "native_input_admitted"
            || producer.execution_complete
            || producer.disposition != "admitted"
            || native_turn_id.is_empty()
        {
            return Err(error("RESULT_DISPATCH_MISMATCH"));
        }
        Ok(Self {
            command: command.clone(),
            receipt: receipt.clone(),
            origin,
            request,
            native_turn_id: native_turn_id.to_owned(),
        })
    }

    pub(crate) fn request(&self) -> &ResultRequest {
        &self.request
    }

    fn native_identity(&self) -> Result<String, ResultReadError> {
        let digest = Sha256Digest::of_canonical_json(&json!({
            "selector":self.command.input["selector"], "native_turn_id":self.native_turn_id,
        }))
        .map_err(|_| error("RESULT_CONTEXT_INVALID"))?;
        Ok(format!("muse:result:{}", digest.as_str()))
    }

    pub(crate) fn normalize_page(&self, mut page: Value) -> Result<Value, ResultReadError> {
        let selected = self.request.selector();
        let native = &page["source"];
        if native["native_session_id"] != selected.session_id()
            || native["item_id"] != selected.item_id()
            || native["item_revision"].as_u64() != Some(selected.item_revision())
            || native["turn_id"] != self.native_turn_id
            || native["kind"] != selected.kind().as_str()
            || native.get("output_ref").and_then(Value::as_str) != selected.output_ref()
        {
            return Err(error("RESULT_NATIVE_SCOPE_MISMATCH"));
        }
        let payload_sha256 = native["content_digest"]
            .as_str()
            .and_then(|digest| digest.strip_prefix("sha256:"))
            .ok_or_else(|| error("RESULT_PAYLOAD_IDENTITY_UNAVAILABLE"))?
            .to_owned();
        let source = NormalizedResultPageSource {
            schema_id: "swarm.normalized_result_page".into(),
            schema_version: 1,
            origin: self.origin.clone(),
            result_operation_id: self.receipt.operation_id.clone(),
            result_input_sha256: self.receipt.input_sha256.clone(),
            result_module_receipt: self.receipt.clone(),
            payload_sha256,
            payload_bytes: page["total_bytes"]
                .as_u64()
                .ok_or_else(|| error("RESULT_PAGE_INVALID"))?,
            native_response_identity: Some(self.native_identity()?),
            execution_complete: false,
            task_completion: "unknown".into(),
            native_replay: false,
        };
        page["source"] = serde_json::to_value(source).map_err(|_| error("RESULT_PAGE_INVALID"))?;
        self.validate_page(&page)?;
        Ok(page)
    }

    pub(crate) fn validate_page(&self, page: &Value) -> Result<(), ResultReadError> {
        let page: NormalizedPage =
            serde_json::from_value(page.clone()).map_err(|_| error("RESULT_PAGE_INVALID"))?;
        page.source
            .validate()
            .map_err(|_| error("RESULT_PAGE_INVALID"))?;
        let bytes = STANDARD
            .decode(&page.content_base64)
            .map_err(|_| error("RESULT_PAGE_INVALID"))?;
        let end = page
            .offset_bytes
            .checked_add(page.byte_length)
            .ok_or_else(|| error("RESULT_PAGE_INVALID"))?;
        if page.source.origin != self.origin
            || page.source.result_module_receipt != self.receipt
            || page.source.result_operation_id != self.command.operation_id
            || page.source.native_response_identity.as_deref()
                != Some(self.native_identity()?.as_str())
            || page.source.payload_bytes != page.total_bytes
            || page.total_bytes > MAX_SAFE_INTEGER
            || page.offset_bytes != self.request.offset_bytes()
            || page.byte_length != bytes.len() as u64
            || page.byte_length > self.request.length_bytes()
            || page.byte_length > MAX_PAGE_BYTES
            || end > page.total_bytes
            || page.eof != (end == page.total_bytes)
            || page.byte_length == 0 && !page.eof
            || STANDARD.encode(&bytes) != page.content_base64
            || hash(&bytes) != page.page_sha256
            || page.media_type.is_empty()
            || page.media_type.len() > 256
            || page.media_type.chars().any(char::is_control)
            || page.offset_bytes == 0 && page.eof && hash(&bytes) != page.source.payload_sha256
        {
            return Err(error("RESULT_PAGE_MISMATCH"));
        }
        if let Some(expected) = self.command.input.get("normalized_result_payload_identity") {
            let expected = expected
                .as_object()
                .ok_or_else(|| error("RESULT_PAYLOAD_MISMATCH"))?;
            if expected.len() != 3
                || expected.get("sha256").and_then(Value::as_str)
                    != Some(page.source.payload_sha256.as_str())
                || expected.get("byte_length").and_then(Value::as_u64) != Some(page.total_bytes)
                || expected.get("complete").and_then(Value::as_bool) != Some(true)
            {
                return Err(error("RESULT_PAYLOAD_MISMATCH"));
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NormalizedPage {
    source: NormalizedResultPageSource,
    offset_bytes: u64,
    byte_length: u64,
    total_bytes: u64,
    eof: bool,
    media_type: String,
    content_base64: String,
    page_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultReadError {
    pub code: &'static str,
    pub native_code: Option<i32>,
    pub next_boundary_offset_bytes: Option<u64>,
    continuation: Option<Box<ResultRequest>>,
}

impl ResultReadError {
    pub fn continuation(&self) -> Option<&ResultRequest> {
        self.continuation.as_deref()
    }
}
impl fmt::Display for ResultReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code)
    }
}
impl std::error::Error for ResultReadError {}

fn error(code: &'static str) -> ResultReadError {
    ResultReadError {
        code,
        native_code: None,
        next_boundary_offset_bytes: None,
        continuation: None,
    }
}
fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str, ResultReadError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| error("RESULT_REQUIRED_TEXT_MISSING"))
}
fn optional_text(value: &Value, key: &str) -> Result<Option<String>, ResultReadError> {
    value
        .get(key)
        .map(|_| text(value, key).map(str::to_owned))
        .transpose()
}
fn valid_cursor(cursor: &str) -> bool {
    cursor.len() <= MAX_CURSOR_BYTES && !cursor.chars().any(char::is_control)
}
fn cursor_text<'a>(value: &'a Value, key: &str) -> Result<&'a str, ResultReadError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|cursor| valid_cursor(cursor))
        .ok_or_else(|| error("RESULT_REQUIRED_TEXT_MISSING"))
}
fn optional_cursor_text(value: &Value, key: &str) -> Result<Option<String>, ResultReadError> {
    match value.get(key) {
        None => Ok(None),
        Some(Value::String(cursor)) if valid_cursor(cursor) => Ok(Some(cursor.clone())),
        _ => Err(error("RESULT_REQUIRED_TEXT_MISSING")),
    }
}
fn integer(
    value: Option<&Value>,
    fallback: Option<u64>,
    minimum: u64,
) -> Result<u64, ResultReadError> {
    let result = match value {
        None => fallback,
        Some(value) => value.as_u64().or_else(|| {
            value.as_f64().and_then(|n| {
                (n.is_finite() && (0.0..=MAX_SAFE_INTEGER as f64).contains(&n) && n.fract() == 0.0)
                    .then_some(n as u64)
            })
        }),
    };
    result
        .filter(|n| (minimum..=MAX_SAFE_INTEGER).contains(n))
        .ok_or_else(|| error("INVALID_RESULT_RANGE"))
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn authorize(
    selector: &ResultSelector,
    allowed: &impl Fn(&ResultSelector) -> bool,
) -> Result<(), ResultReadError> {
    if allowed(selector) {
        Ok(())
    } else {
        Err(error("RESULT_OUTSIDE_AUTHORIZED_FAMILY"))
    }
}

fn native_error(failure: RequestError) -> ResultReadError {
    let mut result = error("NATIVE_RESULT_READ_UNAVAILABLE");
    if let RequestError::Rpc(jsonrpsee_core::client::Error::Call(call)) = failure {
        result.native_code = Some(call.code());
        if call.code() == -32602 {
            result.next_boundary_offset_bytes = call
                .data()
                .and_then(|raw| serde_json::from_str::<Value>(raw.get()).ok())
                .and_then(|data| integer(data.get("nextBoundaryOffsetBytes"), None, 0).ok());
        }
    }
    result
}

fn validate_native_read(method: &str, value: &Value) -> Result<(), ResultReadError> {
    let definition = match method {
        "view/page" => "ViewPageResult",
        "item/readOutput" => "ItemReadOutputResult",
        _ => return Err(error("INVALID_NATIVE_RESULT_REQUEST")),
    };
    crate::schema::validate_definition(definition, value)
        .map_err(|_| error("INVALID_NATIVE_RESULT_SCHEMA"))
}

/// The authorization predicate must bind CURRENT principal rights, artifact,
/// binding generation, native namespace/family and any caller-supplied cursor.
/// It is rechecked around native reads and before returning bytes.
pub async fn read_result(
    transport: &MspTransport,
    input: &Value,
    allowed: impl Fn(&ResultSelector) -> bool,
) -> Result<Value, ResultReadError> {
    let request = ResultRequest::parse(input)?;
    read_result_with(
        request,
        |method: &'static str, params: Value| async move {
            let pending = transport
                .submit_request(
                    method,
                    params
                        .as_object()
                        .cloned()
                        .ok_or_else(|| error("INVALID_NATIVE_RESULT_REQUEST"))?,
                )
                .map_err(|_| error("NATIVE_RESULT_READ_UNAVAILABLE"))?;
            pending
                .submitted
                .await
                .map_err(|_| error("NATIVE_RESULT_READ_UNAVAILABLE"))?
                .map_err(|_| error("NATIVE_RESULT_READ_UNAVAILABLE"))?;
            let value = pending
                .response
                .await
                .map_err(|_| error("NATIVE_RESULT_READ_UNAVAILABLE"))?
                .map_err(native_error)?;
            validate_native_read(method, &value)?;
            Ok(value)
        },
        allowed,
    )
    .await
}

/// One bounded historical scan. Budget exhaustion returns an exact observed
/// continuation instead of claiming the pinned revision does not exist.
pub async fn read_result_with<F, Fut>(
    request: ResultRequest,
    mut rpc: F,
    allowed: impl Fn(&ResultSelector) -> bool,
) -> Result<Value, ResultReadError>
where
    F: FnMut(&'static str, Value) -> Fut,
    Fut: Future<Output = Result<Value, ResultReadError>>,
{
    let s = &request.selector;
    authorize(s, &allowed)?;
    let mut cursor = s.before_cursor.clone();
    let mut seen = HashSet::new();
    if let Some(cursor) = &cursor {
        seen.insert(hash(cursor.as_bytes()));
    }
    let mut selected = None;
    for _ in 0..MAX_HISTORY_PAGES {
        authorize(s, &allowed)?;
        let mut params = json!({"sessionId":s.session_id,"direction":"backward","limit":100});
        if let Some(cursor) = &cursor {
            params["cursor"] = json!(cursor);
        }
        let page = rpc("view/page", params).await?;
        authorize(s, &allowed)?;
        let events = page["events"]
            .as_array()
            .filter(|events| events.len() <= 100)
            .ok_or_else(|| error("INVALID_NATIVE_RESULT_PAGE"))?;
        let next = match page.get("nextCursor") {
            Some(Value::Null) => None,
            Some(Value::String(value)) if valid_cursor(value) => Some(value.clone()),
            _ => return Err(error("INVALID_NATIVE_RESULT_PAGE")),
        };
        for event in events.iter().rev() {
            let p = &event["params"];
            if !matches!(
                event["method"].as_str(),
                Some("item/completed" | "item/updated" | "item/started")
            ) || p["sessionId"] != s.session_id
                || p["item"]["itemId"] != s.item_id
                || integer(p["item"].get("revision"), None, 1).ok() != Some(s.item_revision)
            {
                continue;
            }
            selected = Some((p["item"].clone(), cursor_text(p, "viewCursor")?.to_owned()));
            break;
        }
        if selected.is_some() {
            break;
        }
        let Some(next) = next else {
            return Err(error("RESULT_REVISION_NOT_AVAILABLE"));
        };
        if !seen.insert(hash(next.as_bytes())) {
            return Err(error("NATIVE_PAGINATION_NOT_ADVANCING"));
        }
        cursor = Some(next);
    }
    let Some((item, view_cursor)) = selected else {
        let mut next = request.clone();
        next.selector.before_cursor = cursor;
        let mut failure = error("RESULT_LOOKUP_CONTINUATION_REQUIRED");
        failure.continuation = Some(Box::new(next));
        return Err(failure);
    };
    let status = text(&item, "status")?;
    if !matches!(
        status,
        "completed" | "failed" | "cancelled" | "rejected" | "timedOut"
    ) {
        return Err(error("RESULT_ITEM_NOT_KNOWN_TERMINAL"));
    }
    let item_kind = text(&item, "kind")?;
    let mut source = json!({
        "native_session_id":s.session_id,"item_id":s.item_id,"item_revision":s.item_revision,
        "item_kind":item_kind,"item_status":status,"turn_id":item.get("turnId").unwrap_or(&Value::Null),
        "view_cursor":view_cursor,"read_method":"view/page","kind":s.kind.as_str(),
    });
    let (bytes, total, media_type, source_digest) = match s.kind {
        ResultKind::Result | ResultKind::Message => {
            let (full, media_type) = if s.kind == ResultKind::Result {
                if item_kind != "subagent" || !item["result"].is_object() {
                    return Err(error("SUBAGENT_RESULT_UNAVAILABLE"));
                }
                source["child_session_id"] = json!(text(&item, "childSessionId")?);
                source["digest_basis"] = json!("rust_muse_result_json_v1");
                (
                    serde_json::to_vec(&item["result"])
                        .map_err(|_| error("INVALID_NATIVE_RESULT_JSON"))?,
                    "application/json",
                )
            } else {
                if item_kind != "agentMessage" || item["text"].as_str().is_none() {
                    return Err(error("MESSAGE_RESULT_UNAVAILABLE"));
                }
                if item.get("truncated").is_some_and(|value| value != false) {
                    return Err(error("MESSAGE_RESULT_TRUNCATED"));
                }
                source["digest_basis"] = json!("message_utf8");
                (
                    item["text"].as_str().unwrap().as_bytes().to_vec(),
                    "text/plain",
                )
            };
            let total = full.len() as u64;
            if request.offset_bytes > total {
                return Err(error("RESULT_OFFSET_OUT_OF_RANGE"));
            }
            let digest = format!("sha256:{}", hash(&full));
            let end = total.min(request.offset_bytes + request.length_bytes) as usize;
            (
                full[request.offset_bytes as usize..end].to_vec(),
                total,
                media_type.to_owned(),
                Some(digest),
            )
        }
        ResultKind::Output | ResultKind::Patch => {
            if !matches!(item_kind, "toolCall" | "userShell")
                || (s.kind == ResultKind::Patch && item_kind != "toolCall")
            {
                return Err(error("STORED_OUTPUT_UNAVAILABLE"));
            }
            let reference = &item[if s.kind == ResultKind::Patch {
                "patchRef"
            } else {
                "outputRef"
            }];
            if reference["availability"] != "available" {
                return Err(error("STORED_OUTPUT_UNAVAILABLE"));
            }
            let native_ref = text(reference, "id")?;
            if s.output_ref.as_deref() != Some(native_ref) {
                return Err(error("OUTPUT_REFERENCE_MISMATCH"));
            }
            let declared_media_type = optional_text(reference, "mediaType")?;
            if s.kind == ResultKind::Patch
                && (reference["kind"] != "tool_patch"
                    || declared_media_type.as_deref() != Some("application/json"))
            {
                return Err(error("PATCH_REFERENCE_PROVENANCE_INVALID"));
            }
            let total = integer(reference.get("byteLen"), None, 0)?;
            if request.offset_bytes > total {
                return Err(error("RESULT_OFFSET_OUT_OF_RANGE"));
            }
            let digest = match reference.get("digest") {
                None | Some(Value::Null) => None,
                Some(Value::String(value)) if !value.trim().is_empty() => Some(value.clone()),
                _ => return Err(error("INVALID_NATIVE_OUTPUT_DIGEST")),
            };
            source["output_ref"] = json!(native_ref);
            source["native_digest"] = json!(digest);
            source["read_method"] = json!("item/readOutput");
            authorize(s, &allowed)?;
            let page = rpc(
                "item/readOutput",
                json!({
                    "sessionId":s.session_id,"itemId":s.item_id,"outputRef":native_ref,
                    "offsetBytes":request.offset_bytes,"lengthBytes":request.length_bytes,
                }),
            )
            .await?;
            authorize(s, &allowed)?;
            let media_type = text(&page, "mediaType")?;
            if declared_media_type
                .as_deref()
                .is_some_and(|declared| declared != media_type)
            {
                return Err(error("NATIVE_OUTPUT_MEDIA_TYPE_MISMATCH"));
            }
            let content = page["content"]
                .as_str()
                .ok_or_else(|| error("INVALID_NATIVE_OUTPUT_CONTENT"))?;
            let bytes = match page["encoding"].as_str() {
                Some("utf8") => content.as_bytes().to_vec(),
                Some("base64") => {
                    let bytes = STANDARD
                        .decode(content)
                        .map_err(|_| error("INVALID_NATIVE_OUTPUT_BASE64"))?;
                    if STANDARD.encode(&bytes) != content {
                        return Err(error("INVALID_NATIVE_OUTPUT_BASE64"));
                    }
                    bytes
                }
                _ => return Err(error("UNSUPPORTED_NATIVE_OUTPUT_ENCODING")),
            };
            let end = request
                .offset_bytes
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| error("NATIVE_OUTPUT_RANGE_MISMATCH"))?;
            if integer(page.get("offsetBytes"), None, 0)? != request.offset_bytes
                || integer(page.get("byteLen"), None, 0)? != bytes.len() as u64
                || bytes.len() as u64 > request.length_bytes
                || end > total
                || page["eof"].as_bool() != Some(end == total)
            {
                return Err(error("NATIVE_OUTPUT_RANGE_MISMATCH"));
            }
            (bytes, total, media_type.to_owned(), digest)
        }
    };
    if s.expected_digest
        .as_ref()
        .is_some_and(|expected| Some(expected) != source_digest.as_ref())
    {
        return Err(error("RESULT_SOURCE_DIGEST_CHANGED"));
    }
    if bytes.is_empty() && request.offset_bytes < total {
        return Err(error("NATIVE_OUTPUT_NOT_ADVANCING"));
    }
    let whole_verified = source_digest.as_ref().is_some_and(|digest| {
        digest.starts_with("sha256:")
            && request.offset_bytes == 0
            && bytes.len() as u64 == total
            && digest == &format!("sha256:{}", hash(&bytes))
    });
    if source_digest.as_ref().is_some_and(|digest| {
        digest.starts_with("sha256:") && request.offset_bytes == 0 && bytes.len() as u64 == total
    }) && !whole_verified
    {
        return Err(error("NATIVE_OUTPUT_DIGEST_MISMATCH"));
    }
    source["content_digest"] = json!(source_digest);
    source["whole_digest_verified"] = json!(whole_verified);
    authorize(s, &allowed)?;
    Ok(
        json!({"source":source,"offset_bytes":request.offset_bytes,"byte_length":bytes.len(),
        "total_bytes":total,"eof":request.offset_bytes + bytes.len() as u64 == total,
        "media_type":media_type,"content_base64":STANDARD.encode(&bytes),"page_sha256":hash(&bytes)}),
    )
}

#[cfg(test)]
pub(crate) fn fixture_origin(
    receipt: &ModuleReceiptIdentity,
    task: &crate::commands::ValidatedTaskPrompt,
    native_input_id: &str,
    selector: &Value,
) -> Value {
    let c = &task.context;
    json!({
        "schema_version":1, "binding_id":receipt.binding_id,
        "binding_generation":receipt.binding_generation,
        "task_id":c.task_id, "task_revision":c.task_revision,
        "task_snapshot_sha256":c.task_snapshot_sha256, "attempt_id":c.attempt_id,
        "target_operation_id":receipt.operation_id, "target_input_sha256":receipt.input_sha256,
        "selector_sha256":Sha256Digest::of_canonical_json(selector).unwrap().as_str(),
        "producer":{
            "assignment_id":receipt.operation_id, "dispatch_operation_id":receipt.operation_id,
            "attempt_id":c.attempt_id, "task_id":c.task_id, "task_revision":c.task_revision,
            "task_snapshot_sha256":c.task_snapshot_sha256,
            "source_text_sha256":c.source_text_sha256,"source_text_bytes":c.source_text_bytes,
            "native_payload_sha256":task.envelope.prompt_sha256,
            "native_payload_bytes":task.envelope.prompt_bytes,
            "completion_condition":"native_input_admitted", "execution_complete":false,
            "task_completion":"unknown", "disposition":"admitted",
            "native_input_id":native_input_id, "module_receipt":receipt,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::VecDeque,
        future::{Ready, ready},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
    };

    type Calls = Arc<Mutex<Vec<(&'static str, Value)>>>;
    fn fake(
        pages: Vec<Value>,
        calls: Calls,
    ) -> impl FnMut(&'static str, Value) -> Ready<Result<Value, ResultReadError>> {
        let mut pages: VecDeque<_> = pages.into();
        move |method, params| {
            calls.lock().unwrap().push((method, params));
            ready(
                pages
                    .pop_front()
                    .ok_or_else(|| error("UNEXPECTED_NATIVE_READ")),
            )
        }
    }
    fn input(kind: &str) -> Value {
        let mut value = json!({"selector":{"kind":kind,"session_id":"root","item_id":"item","item_revision":7}});
        if matches!(kind, "output" | "patch") {
            value["selector"]["output_ref"] = json!("stored");
        }
        value
    }
    fn event(item: Value) -> Value {
        json!({"method":"item/completed","params":{"sessionId":"root","viewCursor":"observed-cursor","item":item}})
    }
    fn message(text: &str) -> Value {
        json!({"itemId":"item","revision":7,"kind":"agentMessage","status":"completed","turnId":"turn","text":text})
    }
    fn view(events: Vec<Value>, next: Value) -> Value {
        json!({"events":events,"nextCursor":next})
    }
    fn output_item(digest: &str) -> Value {
        json!({"itemId":"item","revision":7,"kind":"toolCall","status":"completed",
            "outputRef":{"id":"stored","availability":"available","byteLen":3,"digest":digest,
                "uri":"file:///never-read/provider-auth.json","path":"C:/never-read/provider-auth.json"}})
    }
    fn output_page(content: &str) -> Value {
        json!({"content":content,"encoding":"base64","offsetBytes":0,"byteLen":3,
            "eof":true,"mediaType":"application/octet-stream"})
    }
    async fn run(request: Value, pages: Vec<Value>) -> (Result<Value, ResultReadError>, Calls) {
        let calls = Calls::default();
        let result = read_result_with(
            ResultRequest::parse(&request).unwrap(),
            fake(pages, calls.clone()),
            |_| true,
        )
        .await;
        (result, calls)
    }

    fn normalized_fixture() -> (
        RuntimeCommand,
        ModuleReceiptIdentity,
        ModuleReceiptIdentity,
        crate::commands::ValidatedTaskPrompt,
    ) {
        let target: ModuleReceiptIdentity = serde_json::from_value(json!({
            "schema_version":1,"module_id":"muse",
            "artifact":{"artifact_id":"muse-rust-controller.1","version":"0.1.2","build_id":null},
            "protocol":{"major":1,"minor":0},"binding_id":"binding","binding_generation":1,
            "operation_id":"dispatch","input_sha256":"a".repeat(64),
        }))
        .unwrap();
        let task = crate::commands::ValidatedTaskPrompt {
            envelope: serde_json::from_value(json!({
                "schema_id":"swarm.task_prompt","schema_version":1,"task_id":"task",
                "task_revision":2,"attempt_id":"attempt","task_snapshot_sha256":"b".repeat(64),
                "prompt":"rendered abc","prompt_sha256":hash(b"rendered abc"),"prompt_bytes":12,
            }))
            .unwrap(),
            context: serde_json::from_value(json!({
                "schema_version":1,"operation_id":"dispatch","binding_id":"binding",
                "binding_generation":1,"worker_boot_id":"boot","task_id":"task",
                "task_revision":2,"attempt_id":"attempt","task_snapshot_sha256":"b".repeat(64),
                "source_text_sha256":hash(b"abc"),"source_text_bytes":3,
            }))
            .unwrap(),
        };
        let mut receipt = target.clone();
        receipt.operation_id = "result".into();
        receipt.input_sha256 = "c".repeat(64);
        let mut request = input("message");
        request["selector"]["input_operation_id"] = json!("dispatch");
        request["selector"]["before_cursor"] = json!("");
        request["client_request_id"] = json!("request-result");
        request["binding_id"] = json!("binding");
        request["generation"] = json!(1);
        request["offset_bytes"] = json!(0);
        request["length_bytes"] = json!(65_536);
        request["normalized_result_origin"] =
            fixture_origin(&target, &task, "native-input", &request["selector"]);
        let command = RuntimeCommand {
            operation_id: "result".into(),
            method: "agent.result".into(),
            created_at_ms: 1,
            binding_id: "binding".into(),
            generation: 1,
            native_root_id: Some("root".into()),
            route: json!({"alias":"muse","runtime":"module","enabled":true,"module_artifact_id":"muse-rust-controller.1"}),
            input: request,
            input_sha256: Some(receipt.input_sha256.clone()),
            target_input_sha256: Some(target.input_sha256.clone()),
        };
        (command, receipt, target, task)
    }

    #[tokio::test]
    async fn muse_normalized_result_host_envelope_to_typed_page_preserves_selector_and_bytes() {
        let (command, receipt, target, task) = normalized_fixture();
        let context = NormalizedResultContext::for_dispatch(
            &command,
            &receipt,
            &target,
            &task,
            "native-input",
            "turn",
        )
        .unwrap();
        assert_eq!(
            context.request().to_input()["selector"],
            command.input["selector"]
        );
        let body = "hello π";
        let calls = Calls::default();
        let raw = read_result_with(
            context.request().clone(),
            fake(
                vec![view(vec![event(message(body))], Value::Null)],
                calls.clone(),
            ),
            |_| true,
        )
        .await
        .unwrap();
        let page = context.normalize_page(raw).unwrap();
        let source: NormalizedResultPageSource =
            serde_json::from_value(page["source"].clone()).unwrap();
        source.validate().unwrap();
        assert_eq!(
            source.origin,
            serde_json::from_value::<NormalizedResultOriginContext>(
                command.input["normalized_result_origin"].clone()
            )
            .unwrap()
        );
        assert_eq!(source.result_module_receipt, receipt);
        assert_eq!(source.payload_sha256, hash(body.as_bytes()));
        assert_eq!(
            STANDARD
                .decode(page["content_base64"].as_str().unwrap())
                .unwrap(),
            body.as_bytes()
        );
        assert!(!source.execution_complete && !source.native_replay);
        assert_eq!(source.task_completion, "unknown");
        assert_eq!(calls.lock().unwrap()[0].0, "view/page");
        assert_eq!(calls.lock().unwrap()[0].1["cursor"], "");
        for (pointer, value) in [
            ("/source/origin/task_revision", json!(3)),
            (
                "/source/result_module_receipt/input_sha256",
                json!("d".repeat(64)),
            ),
            ("/source/native_response_identity", json!("another-item")),
            ("/source/payload_sha256", json!("d".repeat(64))),
            ("/source/execution_complete", json!(true)),
            ("/source/native_replay", json!(true)),
            ("/offset_bytes", json!(1)),
            ("/byte_length", json!(0)),
            ("/total_bytes", json!(99)),
            ("/eof", json!(false)),
            ("/content_base64", json!("c3Vic3RpdHV0ZWQ=")),
        ] {
            let mut bad = page.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            assert!(context.validate_page(&bad).is_err(), "{pointer}");
        }
    }

    #[test]
    fn muse_normalized_result_rejects_unsealed_foreign_and_mismatched_dispatch_contexts() {
        let (command, receipt, target, task) = normalized_fixture();
        for (pointer, value) in [
            ("/normalized_result_origin/task_revision", json!(3)),
            (
                "/normalized_result_origin/attempt_id",
                json!("other-attempt"),
            ),
            (
                "/normalized_result_origin/producer/native_payload_sha256",
                json!("d".repeat(64)),
            ),
            (
                "/normalized_result_origin/producer/native_input_id",
                json!("another-input"),
            ),
            (
                "/normalized_result_origin/producer/module_receipt/artifact/version",
                json!("0.1.1"),
            ),
            ("/selector/session_id", json!("foreign-root")),
            ("/selector/input_operation_id", json!("other-dispatch")),
            ("/selector/item_revision", json!(8)),
        ] {
            let mut bad = command.clone();
            *bad.input.pointer_mut(pointer).unwrap() = value;
            assert!(
                NormalizedResultContext::for_dispatch(
                    &bad,
                    &receipt,
                    &target,
                    &task,
                    "native-input",
                    "turn"
                )
                .is_err(),
                "{pointer}"
            );
        }
        let mut bad = command.clone();
        bad.input["normalized_result_origin"]["private_auth"] = json!("must-not-be-retained");
        assert!(validate_normalized_request(&bad, &receipt).is_err());
        bad.input = command.input.clone();
        bad.input
            .as_object_mut()
            .unwrap()
            .remove("normalized_result_origin");
        assert!(validate_normalized_request(&bad, &receipt).is_err());
    }

    #[tokio::test]
    async fn muse_normalized_result_rejects_foreign_native_turn_before_page_publication() {
        let (command, receipt, target, task) = normalized_fixture();
        let context = NormalizedResultContext::for_dispatch(
            &command,
            &receipt,
            &target,
            &task,
            "native-input",
            "turn",
        )
        .unwrap();
        let mut item = message("other turn");
        item["turnId"] = json!("foreign-turn");
        let (page, _) = run(
            command.input.clone(),
            vec![view(vec![event(item)], Value::Null)],
        )
        .await;
        assert_eq!(
            context.normalize_page(page.unwrap()).unwrap_err().code,
            "RESULT_NATIVE_SCOPE_MISMATCH"
        );
    }

    #[tokio::test]
    async fn muse_normalized_result_partial_page_keeps_whole_digest_and_sealed_range() {
        let (mut command, receipt, target, task) = normalized_fixture();
        command.input["offset_bytes"] = json!(1);
        command.input["length_bytes"] = json!(2);
        command.input["normalized_result_payload_identity"] = json!({
            "sha256":hash(b"abcdef"),"byte_length":6,"complete":true,
        });
        let context = NormalizedResultContext::for_dispatch(
            &command,
            &receipt,
            &target,
            &task,
            "native-input",
            "turn",
        )
        .unwrap();
        let (page, _) = run(
            command.input.clone(),
            vec![view(vec![event(message("abcdef"))], Value::Null)],
        )
        .await;
        let page = context.normalize_page(page.unwrap()).unwrap();
        assert_eq!(
            STANDARD
                .decode(page["content_base64"].as_str().unwrap())
                .unwrap(),
            b"bc"
        );
        assert_eq!(page["source"]["payload_sha256"], hash(b"abcdef"));
        assert_eq!(page["total_bytes"], 6);
        assert_eq!(page["eof"], false);
        command.input["normalized_result_payload_identity"]["sha256"] = json!("d".repeat(64));
        let changed = NormalizedResultContext::for_dispatch(
            &command,
            &receipt,
            &target,
            &task,
            "native-input",
            "turn",
        )
        .unwrap();
        assert_eq!(
            changed.validate_page(&page).unwrap_err().code,
            "RESULT_PAYLOAD_MISMATCH"
        );
    }

    #[test]
    fn result_native_wire_uses_complete_pinned_schema_before_codec() {
        assert!(validate_native_read("view/page", &view(vec![], Value::Null)).is_ok());
        assert!(validate_native_read("view/page", &view(vec![], json!(""))).is_ok());
        assert!(validate_native_read("view/page", &view(vec![], json!("  "))).is_ok());
        assert_eq!(
            validate_native_read("view/page", &json!({"events":[],"nextCursor":17}))
                .unwrap_err()
                .code,
            "INVALID_NATIVE_RESULT_SCHEMA"
        );
        let mut page = output_page("YWJj");
        assert!(validate_native_read("item/readOutput", &page).is_ok());
        page["encoding"] = json!("invented");
        assert_eq!(
            validate_native_read("item/readOutput", &page)
                .unwrap_err()
                .code,
            "INVALID_NATIVE_RESULT_SCHEMA"
        );
        assert_eq!(
            validate_native_read("subagent/readResult", &json!({}))
                .unwrap_err()
                .code,
            "INVALID_NATIVE_RESULT_REQUEST"
        );
    }

    #[test]
    fn result_selector_is_closed_and_ranges_are_safe() {
        for (key, value) in [
            ("item_revision", json!(0)),
            ("item_revision", json!(1.5)),
            ("item_revision", json!(MAX_SAFE_INTEGER + 1)),
            ("before_cursor", Value::Null),
            ("expected_digest", json!(" ")),
            ("unexpected", json!(true)),
        ] {
            let mut request = input("message");
            request["selector"][key] = value;
            assert!(ResultRequest::parse(&request).is_err(), "{key}");
        }
        for (key, value) in [
            ("offset_bytes", json!(-1)),
            ("offset_bytes", Value::Null),
            ("length_bytes", json!(0)),
            ("length_bytes", json!(MAX_PAGE_BYTES + 1)),
        ] {
            let mut request = input("message");
            request[key] = value;
            assert!(ResultRequest::parse(&request).is_err(), "{key}");
        }
        let mut request = input("message");
        request["selector"]["output_ref"] = json!("stored");
        assert_eq!(
            ResultRequest::parse(&request).unwrap_err().code,
            "OUTPUT_REF_NOT_APPLICABLE"
        );
        assert_eq!(
            ResultRequest::parse(&input("message"))
                .unwrap()
                .length_bytes(),
            MAX_PAGE_BYTES
        );
    }

    #[test]
    fn result_selector_preserves_bounded_opaque_cursor_bytes() {
        for cursor in ["", "  "] {
            let mut request = input("message");
            request["selector"]["before_cursor"] = json!(cursor);
            let parsed = ResultRequest::parse(&request).unwrap();
            assert_eq!(parsed.selector().before_cursor(), Some(cursor));
            assert_eq!(ResultRequest::parse(&parsed.to_input()).unwrap(), parsed);
        }

        for cursor in ["cursor\nline", "cursor\u{0007}"] {
            let mut request = input("message");
            request["selector"]["before_cursor"] = json!(cursor);
            assert!(ResultRequest::parse(&request).is_err());
        }
        let mut oversized = input("message");
        oversized["selector"]["before_cursor"] = json!("x".repeat(MAX_CURSOR_BYTES + 1));
        assert!(ResultRequest::parse(&oversized).is_err());
        let mut wrong_type = input("message");
        wrong_type["selector"]["before_cursor"] = json!(17);
        assert!(ResultRequest::parse(&wrong_type).is_err());
    }

    #[tokio::test]
    async fn result_pins_historical_revision_and_native_cursor_without_latest_substitution() {
        let mut newer = message("newer");
        newer["revision"] = json!(8);
        let mut foreign = event(message("foreign"));
        foreign["params"]["sessionId"] = json!("other");
        let (page, calls) = run(
            input("message"),
            vec![
                view(vec![foreign, event(newer)], json!("opaque-older")),
                view(vec![event(message("pinned"))], Value::Null),
            ],
        )
        .await;
        let page = page.unwrap();
        assert_eq!(
            STANDARD
                .decode(page["content_base64"].as_str().unwrap())
                .unwrap(),
            b"pinned"
        );
        assert_eq!(page["source"]["item_revision"], 7);
        assert_eq!(page["source"]["view_cursor"], "observed-cursor");
        assert_eq!(calls.lock().unwrap()[1].1["cursor"], "opaque-older");
        assert!(
            calls
                .lock()
                .unwrap()
                .iter()
                .all(|(method, _)| *method == "view/page")
        );
    }

    #[tokio::test]
    async fn result_message_pages_raw_utf8_bytes_and_pins_whole_digest() {
        let full = "ab🙂cd".as_bytes();
        let mut request = input("message");
        request["offset_bytes"] = json!(3);
        request["length_bytes"] = json!(2);
        request["selector"]["expected_digest"] = json!(format!("sha256:{}", hash(full)));
        let (page, _) = run(
            request,
            vec![view(vec![event(message("ab🙂cd"))], Value::Null)],
        )
        .await;
        let page = page.unwrap();
        assert_eq!(
            STANDARD
                .decode(page["content_base64"].as_str().unwrap())
                .unwrap(),
            full[3..5]
        );
        assert_eq!(page["total_bytes"], full.len());
        assert_eq!(page["source"]["whole_digest_verified"], false);
        assert_eq!(page["page_sha256"], hash(&full[3..5]));
    }

    #[tokio::test]
    async fn result_structured_data_preserves_integers_and_versions_its_byte_grammar() {
        let result = json!({"summary":"done","structuredData":{"big":9_007_199_254_740_993u64},
            "artifactRefs":["https://not-fetched.invalid/value"]});
        let item = json!({"itemId":"item","revision":7,"kind":"subagent","status":"completed",
            "childSessionId":"child","result":result});
        let (page, calls) = run(input("result"), vec![view(vec![event(item)], Value::Null)]).await;
        let page = page.unwrap();
        let bytes = STANDARD
            .decode(page["content_base64"].as_str().unwrap())
            .unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), result);
        assert_eq!(page["source"]["digest_basis"], "rust_muse_result_json_v1");
        assert_eq!(page["source"]["whole_digest_verified"], true);
        assert_eq!(calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn result_rejects_unknown_terminal_status_and_truncated_message() {
        for (key, value) in [
            ("status", json!("inProgress")),
            ("status", json!("futureQueued")),
            ("truncated", json!(true)),
        ] {
            let mut item = message("not-ready");
            item[key] = value;
            let (result, calls) =
                run(input("message"), vec![view(vec![event(item)], Value::Null)]).await;
            assert!(result.is_err());
            assert_eq!(calls.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn result_stored_output_uses_only_native_reference_id_and_verifies_full_bytes() {
        let bytes = [0, 255, 1];
        let digest = format!("sha256:{}", hash(&bytes));
        let (page, calls) = run(
            input("output"),
            vec![
                view(vec![event(output_item(&digest))], Value::Null),
                output_page(&STANDARD.encode(bytes)),
            ],
        )
        .await;
        let page = page.unwrap();
        assert_eq!(
            STANDARD
                .decode(page["content_base64"].as_str().unwrap())
                .unwrap(),
            bytes
        );
        assert_eq!(page["source"]["whole_digest_verified"], true);
        assert!(page["source"].get("uri").is_none());
        let calls = calls.lock().unwrap();
        assert_eq!(calls[1].0, "item/readOutput");
        assert_eq!(calls[1].1["outputRef"], "stored");
        assert!(calls[1].1.get("uri").is_none());
        assert!(calls[1].1.get("path").is_none());
    }

    #[tokio::test]
    async fn result_stored_output_rejects_invalid_base64_ranges_eof_and_no_progress() {
        let digest = format!("sha256:{}", hash(b"abc"));
        for (key, value, expected) in [
            ("content", json!("YQ"), "INVALID_NATIVE_OUTPUT_BASE64"),
            ("offsetBytes", json!(1), "NATIVE_OUTPUT_RANGE_MISMATCH"),
            ("byteLen", json!(2), "NATIVE_OUTPUT_RANGE_MISMATCH"),
            ("eof", json!(false), "NATIVE_OUTPUT_RANGE_MISMATCH"),
        ] {
            let mut page = output_page("YWJj");
            page[key] = value;
            let (result, _) = run(
                input("output"),
                vec![view(vec![event(output_item(&digest))], Value::Null), page],
            )
            .await;
            assert_eq!(result.unwrap_err().code, expected);
        }
        let mut empty = output_page("");
        empty["byteLen"] = json!(0);
        empty["eof"] = json!(false);
        let (result, _) = run(
            input("output"),
            vec![view(vec![event(output_item(&digest))], Value::Null), empty],
        )
        .await;
        assert_eq!(result.unwrap_err().code, "NATIVE_OUTPUT_NOT_ADVANCING");
    }

    #[tokio::test]
    async fn result_patch_kind_and_media_type_are_bound_to_the_reference() {
        let digest = format!("sha256:{}", hash(b"abc"));
        let mut valid = output_item(&digest);
        valid["patchRef"] = valid["outputRef"].clone();
        valid["patchRef"]["kind"] = json!("tool_patch");
        valid["patchRef"]["mediaType"] = json!("application/json");
        for (key, value) in [
            ("kind", json!("tool_output")),
            ("mediaType", json!("text/plain")),
            ("mediaType", Value::Null),
        ] {
            let mut item = valid.clone();
            item["patchRef"][key] = value;
            let (result, calls) =
                run(input("patch"), vec![view(vec![event(item)], Value::Null)]).await;
            assert!(result.is_err());
            assert_eq!(
                calls.lock().unwrap().len(),
                1,
                "invalid patch ref is not fetched"
            );
        }
        let (result, calls) = run(
            input("patch"),
            vec![
                view(vec![event(valid.clone())], Value::Null),
                output_page("YWJj"),
            ],
        )
        .await;
        assert_eq!(
            result.unwrap_err().code,
            "NATIVE_OUTPUT_MEDIA_TYPE_MISMATCH"
        );
        assert_eq!(calls.lock().unwrap().len(), 2);
        let mut page = output_page("YWJj");
        page["mediaType"] = json!("application/json");
        let (result, _) = run(
            input("patch"),
            vec![view(vec![event(valid)], Value::Null), page],
        )
        .await;
        assert_eq!(result.unwrap()["media_type"], "application/json");
        let mut output = output_item(&digest);
        output["outputRef"]["mediaType"] = json!("text/plain");
        let (result, _) = run(
            input("output"),
            vec![view(vec![event(output)], Value::Null), output_page("YWJj")],
        )
        .await;
        assert_eq!(
            result.unwrap_err().code,
            "NATIVE_OUTPUT_MEDIA_TYPE_MISMATCH"
        );
    }

    #[tokio::test]
    async fn result_foreign_reference_and_changed_digests_fail_closed() {
        let digest = format!("sha256:{}", hash(b"abc"));
        let mut item = output_item(&digest);
        item["outputRef"]["id"] = json!("foreign");
        let (result, calls) =
            run(input("output"), vec![view(vec![event(item)], Value::Null)]).await;
        assert_eq!(result.unwrap_err().code, "OUTPUT_REFERENCE_MISMATCH");
        assert_eq!(calls.lock().unwrap().len(), 1);
        let (result, _) = run(
            input("output"),
            vec![
                view(vec![event(output_item("sha256:wrong"))], Value::Null),
                output_page("YWJj"),
            ],
        )
        .await;
        assert_eq!(result.unwrap_err().code, "NATIVE_OUTPUT_DIGEST_MISMATCH");
        let mut request = input("message");
        request["selector"]["expected_digest"] = json!("sha256:wrong");
        let (result, _) = run(
            request,
            vec![view(vec![event(message("abc"))], Value::Null)],
        )
        .await;
        assert_eq!(result.unwrap_err().code, "RESULT_SOURCE_DIGEST_CHANGED");
    }

    #[tokio::test]
    async fn result_authorization_is_checked_before_and_after_native_reads() {
        let calls = Calls::default();
        let denied = read_result_with(
            ResultRequest::parse(&input("message")).unwrap(),
            fake(vec![], calls.clone()),
            |_| false,
        )
        .await;
        assert_eq!(denied.unwrap_err().code, "RESULT_OUTSIDE_AUTHORIZED_FAMILY");
        assert!(calls.lock().unwrap().is_empty());
        let allowed = Arc::new(AtomicBool::new(true));
        let changed = allowed.clone();
        let result = read_result_with(
            ResultRequest::parse(&input("message")).unwrap(),
            move |_, _| {
                changed.store(false, Ordering::SeqCst);
                ready(Ok(view(vec![event(message("private"))], Value::Null)))
            },
            move |_| allowed.load(Ordering::SeqCst),
        )
        .await;
        assert_eq!(result.unwrap_err().code, "RESULT_OUTSIDE_AUTHORIZED_FAMILY");
    }

    #[tokio::test]
    async fn result_opaque_cursor_cycle_is_not_a_complete_history() {
        let (result, calls) = run(
            input("message"),
            vec![view(vec![], json!("same")), view(vec![], json!("same"))],
        )
        .await;
        assert_eq!(result.unwrap_err().code, "NATIVE_PAGINATION_NOT_ADVANCING");
        assert_eq!(calls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn result_read_preserves_empty_and_whitespace_opaque_cursors() {
        let mut request = input("message");
        request["selector"]["before_cursor"] = json!("");
        let mut item = event(message("pinned"));
        item["params"]["viewCursor"] = json!("");
        let (result, calls) = run(
            request,
            vec![view(vec![], json!("  ")), view(vec![item], Value::Null)],
        )
        .await;

        let page = result.unwrap();
        assert_eq!(page["source"]["view_cursor"], "");
        let calls = calls.lock().unwrap();
        assert_eq!(calls[0].1["cursor"], "");
        assert_eq!(calls[1].1["cursor"], "  ");
    }

    #[tokio::test]
    async fn result_empty_cursor_cycle_remains_a_pagination_failure() {
        let mut request = input("message");
        request["selector"]["before_cursor"] = json!("");
        let (result, calls) = run(request, vec![view(vec![], json!(""))]).await;
        assert_eq!(result.unwrap_err().code, "NATIVE_PAGINATION_NOT_ADVANCING");
        assert_eq!(calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn result_event_cursor_must_be_present_and_bounded_string() {
        let mut missing = event(message("pinned"));
        missing["params"]
            .as_object_mut()
            .unwrap()
            .remove("viewCursor");
        let mut events = vec![missing];
        for cursor in [
            Value::Null,
            json!(17),
            json!("x".repeat(MAX_CURSOR_BYTES + 1)),
            json!("line\nbreak"),
        ] {
            let mut item = event(message("pinned"));
            item["params"]["viewCursor"] = cursor;
            events.push(item);
        }
        for item in events {
            let (result, _) = run(input("message"), vec![view(vec![item], Value::Null)]).await;
            assert_eq!(result.unwrap_err().code, "RESULT_REQUIRED_TEXT_MISSING");
        }
    }

    #[tokio::test]
    async fn result_bounded_scan_returns_observed_scoped_continuation_not_false_unavailability() {
        let pages = (0..MAX_HISTORY_PAGES)
            .map(|n| view(vec![], json!(format!("opaque-{n}"))))
            .collect();
        let (result, calls) = run(input("message"), pages).await;
        let error = result.unwrap_err();
        assert_eq!(error.code, "RESULT_LOOKUP_CONTINUATION_REQUIRED");
        let next = error.continuation().unwrap();
        assert_eq!(next.selector().before_cursor(), Some("opaque-63"));
        assert_eq!(next.selector().session_id(), "root");
        assert_eq!(next.selector().item_revision(), 7);
        assert_eq!(ResultRequest::parse(&next.to_input()).unwrap(), *next);
        assert_eq!(calls.lock().unwrap().len(), MAX_HISTORY_PAGES);
    }

    #[tokio::test]
    async fn result_malformed_page_cannot_supply_a_revision_or_cursor() {
        let malformed_pages = vec![
            json!({"events":{},"nextCursor":null}),
            json!({"events":[]}),
            json!({"events":[],"nextCursor":17}),
            view(vec![], json!("x".repeat(MAX_CURSOR_BYTES + 1))),
            view(vec![], json!("line\nbreak")),
        ];
        for page in malformed_pages {
            let (result, _) = run(input("message"), vec![page]).await;
            assert_eq!(result.unwrap_err().code, "INVALID_NATIVE_RESULT_PAGE");
        }
        let (result, _) = run(input("message"), vec![view(vec![], Value::Null)]).await;
        assert_eq!(result.unwrap_err().code, "RESULT_REVISION_NOT_AVAILABLE");
    }
}
