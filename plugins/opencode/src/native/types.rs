//! OpenCode wire DTOs, hand-typed from the installed server.
//!
//! Source: `GET /openapi.json` of `opencode serve` v2.0.32 (OpenAPI 3.1.0,
//! `opencode HttpApi version=0.0.1`, all routes under `/api/...`) plus the
//! `session-event.ts` / `permission.ts` / `form.ts` event definitions at tag
//! `v2.0.32` of `anomalyco/opencode`, verified against a locally started
//! server with no model call. Unknown JSON fields are ignored on deserialize
//! for forward compatibility. DTOs only: no I/O, no process spawning, no
//! config access.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

/// `POST /api/session` (`session.create`) body. Only the fields the PR1
/// launch path sets are modelled.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionCreateRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<LocationRef>,
}

/// Installed `Model.Ref`: `{id, providerID, variant?}`. The frozen profile
/// carries the `providerID/id` slash form; the plugin splits it here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelRef {
    pub id: String,
    #[serde(rename = "providerID")]
    pub provider_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
}

/// Installed `Location.PublicRef`: `{directory}`. Every PR1 request carries
/// the Agent workspace directory so the server never falls back to its own
/// process cwd.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocationRef {
    pub directory: String,
}

/// `Session.Info` subset: identity, agent, model, location. Times,
/// tokens, cost and the rest are ignored. `agent` and `model` are absent
/// until set; PR2 reads both back after the resume setters.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    pub id: String,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub model: Option<ModelRef>,
    #[serde(default)]
    pub location: Option<LocationRef>,
}

/// Installed envelope: success bodies are `{"data": ...}`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DataEnvelope<T> {
    pub data: T,
}

// ---------------------------------------------------------------------------
// Prompts and inbox
// ---------------------------------------------------------------------------

/// `POST /api/session/{id}/prompt` (`session.prompt`) body. `delivery` is
/// the installed `Session.Inbox.Delivery` (`steer`|`queue`): `None` leaves
/// the native default (`steer`, applied in `SessionPrompt.prepare`), while
/// the steer path sets explicit `steer` so the write states its contract.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptRequest {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<String>,
}

/// Installed `Session.Inbox.User`: the prompt response. Its `id` is the user
/// message id and the exact input-association key: the SSE
/// `session.inbox.enqueued` / `session.inbox.delivered` events carry it as
/// `inboxID`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboxUser {
    pub id: String,
    #[serde(rename = "sessionID")]
    pub session_id: String,
}

// ---------------------------------------------------------------------------
// Interrupt, permissions, forms, inbox, agents (PR2 readback and replies)
// ---------------------------------------------------------------------------

/// `POST /api/session/{id}/interrupt` (`session.interrupt`) 200 response.
/// Unlike every other success body it is NOT wrapped in `{"data": ...}`:
/// the installed server answers `{"interrupted": false}` on an idle
/// session. `interrupted` tells whether an active execution owned by this
/// server process was interrupted.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InterruptResponse {
    pub interrupted: bool,
}

/// `session.permission.list` / `session.permission.get`
/// (`Permission.Request`): `{id, sessionID, action, resources, save?,
/// metadata?, source?, message?}`. The `permission.asked` SSE data carries
/// these same fields inline.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionRequest {
    pub id: String,
    #[serde(rename = "sessionID")]
    pub session_id: String,
    pub action: String,
    #[serde(default)]
    pub resources: Vec<String>,
    #[serde(default)]
    pub save: Vec<String>,
    #[serde(default)]
    pub message: Option<String>,
}

/// `POST /api/session/{id}/permission/{requestID}/reply`
/// (`session.permission.reply`) body: `{decision, message?}` with
/// `decision` one of `once`, `always`, `reject`. Answers 204 with an empty
/// body. Installed behavior: `reject` settles every pending permission of
/// the session, while `once`/`always` settle only the targeted request.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionReplyRequest {
    pub decision: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// `session.form.list` (`Form.Info`): `{id, sessionID, title, metadata?,
/// fields}`. `sessionID` is a plain string on the wire (the upstream
/// schema notes a `"global"` sentinel owner for MCP elicitation); the
/// plugin only binds entries naming its exact session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FormInfo {
    pub id: String,
    #[serde(rename = "sessionID")]
    pub session_id: String,
    pub title: String,
    #[serde(default)]
    pub fields: Vec<FormField>,
}

/// `Form.Field` structural subset: identity, kind, offered options and the
/// `custom` escape. Members the plugin never decides from (`when`,
/// `required`, formats, bounds, defaults) are ignored; the native reply
/// endpoint enforces them and answers 400 when the answer violates them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FormField {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(rename = "type")]
    pub field_type: String,
    #[serde(default)]
    pub options: Vec<FormOption>,
    #[serde(default)]
    pub custom: bool,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "minItems")]
    pub min_items: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "maxItems")]
    pub max_items: Option<u64>,
}

/// `Form.Option`: `{value, label, description?}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FormOption {
    pub value: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// `POST /api/session/{id}/form/{formID}/reply` (`session.form.reply`)
/// body: `{answer}` with `answer` mapping field keys to values (string,
/// number, boolean, or string array). Answers 204 with an empty body.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FormReplyRequest {
    pub answer: serde_json::Map<String, serde_json::Value>,
}

/// `session.inbox.list`: `{"data": [...]}` of durable enqueued work not
/// yet delivered. Items are uninterpreted: the resume preflight only needs
/// their absence, never their shape.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboxPage {
    #[serde(default)]
    pub data: Vec<serde_json::Value>,
}

/// `GET /api/agent`: `{location, data}` with one entry per installed
/// agent. Only the entry `id` is interpreted (the resume agent-membership
/// check); the ambient `location` (the server process cwd) is ignored.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentList {
    #[serde(default)]
    pub data: Vec<AgentEntry>,
}

/// Installed agent entry subset: the `build`/`general` style id.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentEntry {
    pub id: String,
}

/// `POST /api/session/{id}/model` (`session.switchModel`) body. The
/// installed server validates nothing (a bogus model 204s and echoes
/// back); the echo readback is the proof.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SwitchModelRequest {
    pub model: ModelRef,
}

/// `POST /api/session/{id}/agent` (`session.switchAgent`) body. Like the
/// model switch it validates nothing; the requested id must first be a
/// member of the installed `GET /api/agent` list.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SwitchAgentRequest {
    pub agent: String,
}

// ---------------------------------------------------------------------------
// Messages (readback)
// ---------------------------------------------------------------------------

/// `GET /api/session/{id}/message` (`session.message.list`) page. Newest
/// first with `order=desc`; `cursor.next` continues toward older messages.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct MessagePage {
    pub data: Vec<Message>,
    #[serde(default)]
    pub cursor: PageCursor,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct PageCursor {
    #[serde(default)]
    pub previous: Option<String>,
    #[serde(default)]
    pub next: Option<String>,
}

/// Message subset used by PR1. Only `user`, `assistant` and `idle` messages
/// are interpreted; every other type deserializes as `Other` and is skipped
/// by final-text extraction (never treated as terminal or as text).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Message {
    User(UserMessage),
    Assistant(AssistantMessage),
    Idle(IdleMessage),
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserMessage {
    pub id: String,
    pub time: MessageTime,
    #[serde(default)]
    pub text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssistantMessage {
    pub id: String,
    pub time: MessageTime,
    #[serde(default)]
    pub content: Vec<AssistantContent>,
    #[serde(default)]
    pub finish: Option<String>,
    /// Installed `tokens` (`TokenUsage.Info`): per-message token usage.
    /// Optional on the wire; any unusable value decodes as absent so usage
    /// metadata can never break the terminal readback.
    #[serde(default, deserialize_with = "deserialize_optional_tokens")]
    pub tokens: Option<AssistantTokens>,
}

/// Installed `TokenUsage.Info` subset: `{input, output, reasoning, cache}`.
/// Every member is optional locally so partial native data stays decodable;
/// unavailable members project as null, never zero.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssistantTokens {
    #[serde(default)]
    pub input: Option<i64>,
    #[serde(default)]
    pub output: Option<i64>,
    #[serde(default)]
    pub reasoning: Option<i64>,
    #[serde(default)]
    pub cache: Option<AssistantTokenCache>,
}

/// Installed `TokenUsage.Info.cache`: `{read, write}`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssistantTokenCache {
    #[serde(default)]
    pub read: Option<i64>,
    #[serde(default)]
    pub write: Option<i64>,
}

fn deserialize_optional_tokens<'de, D>(deserializer: D) -> Result<Option<AssistantTokens>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<AssistantTokens>::deserialize(deserializer)
        .ok()
        .flatten())
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum AssistantContent {
    Text(TextPart),
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TextPart {
    #[serde(default)]
    pub text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdleMessage {
    pub id: String,
    pub time: MessageTime,
    pub outcome: String,
    #[serde(default)]
    pub error: Option<StructuredError>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct MessageTime {
    pub created: i64,
}

/// Installed `Session.StructuredError`: `{type, message, status?, response?}`.
/// `type` is an open string (observed: `provider.no-route`); the plugin
/// attaches it as `native_error` and never maps it to a restriction kind.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StructuredError {
    #[serde(rename = "type")]
    pub error_type: String,
    pub message: String,
}

// ---------------------------------------------------------------------------
// Config readback
// ---------------------------------------------------------------------------

/// `GET /api/config` (`config.get`) entry subset. PR1 only needs the
/// document path (proving the per-Agent config file is the effective one)
/// and the default model.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigEntry {
    #[serde(rename = "type")]
    pub entry_type: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub info: Option<ConfigInfo>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigInfo {
    #[serde(default)]
    pub model: Option<ConfigModel>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigModel {
    #[serde(rename = "providerID")]
    pub provider_id: String,
    pub model: String,
}

// ---------------------------------------------------------------------------
// Error bodies
// ---------------------------------------------------------------------------

/// Installed error body shape: `{"_tag": ..., "message": ..., ...}`.
/// Observed tags: `InvalidRequestError`, `UnauthorizedError`,
/// `SessionNotFoundError`, `LocationNotFoundError`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ErrorBody {
    #[serde(rename = "_tag", default)]
    pub tag: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

// ---------------------------------------------------------------------------
// Server-sent events
// ---------------------------------------------------------------------------

/// Installed SSE envelope, one JSON object per `data:` payload:
/// `{id: "evt_...", created?, type, location?, data, durable?}`.
/// Durable events carry `durable: {aggregateID, seq, version}` with a
/// per-aggregate contiguous sequence; ephemeral events (`session.status`,
/// `session.idle`) carry no `durable` block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SseEnvelope {
    pub id: String,
    #[serde(default)]
    pub created: Option<i64>,
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(default)]
    pub data: serde_json::Value,
    #[serde(default)]
    pub durable: Option<DurableBlock>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DurableBlock {
    #[serde(rename = "aggregateID")]
    pub aggregate_id: String,
    pub seq: u64,
    pub version: u64,
}

/// `session.inbox.enqueued` / `session.inbox.delivered` data:
/// `{sessionID, inboxID, item?}`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboxEventData {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "inboxID")]
    pub inbox_id: String,
}

/// `session.execution.*` data: `{sessionID, error?, reason?}`. `succeeded`
/// carries the session only, `failed` adds `error`, `interrupted` adds
/// `reason` (`user`/`shutdown`/`superseded`/`inactivity`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionEventData {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(default)]
    pub error: Option<StructuredError>,
    #[serde(default)]
    pub reason: Option<String>,
}

/// `session.created` / `session.deleted` data subset: the exact session id.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionLifecycleData {
    #[serde(rename = "sessionID")]
    pub session_id: String,
}

/// `session.model.selected` data: `{sessionID, model, previous?}`. Durable.
/// The plugin ignores it (the synchronous 204 plus `session.get` readback
/// already prove the switch); it exists here so the shape stays pinned.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelSelectedData {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    pub model: ModelRef,
}

/// `session.agent.selected` data: `{sessionID, agent, previous?}`. Durable,
/// ignored like the model event: the readback is the proof.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentSelectedData {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    pub agent: String,
}

/// `permission.asked` data: the `Permission.Request` fields inline
/// (`{id, sessionID, action, resources, ...}`). Ephemeral: no `durable`
/// block, so a dropped ask is invisible and the execution waits until the
/// owner interrupts or answers a re-issue.
pub type PermissionAskedData = PermissionRequest;

/// `permission.replied` data: `{sessionID, requestID, reply}` with `reply`
/// one of `once`, `always`, `reject`. Ephemeral.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionRepliedData {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "requestID")]
    pub request_id: String,
    pub reply: String,
}

/// `form.created` data: `{form}` with the full `Form.Info`. Ephemeral.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FormCreatedData {
    pub form: FormInfo,
}

/// `form.replied` data: `{id, sessionID, answer}`. Ephemeral.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FormRepliedData {
    pub id: String,
    #[serde(rename = "sessionID")]
    pub session_id: String,
    pub answer: serde_json::Value,
}

/// `form.cancelled` data: `{id, sessionID}`. Ephemeral.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FormCancelledData {
    pub id: String,
    #[serde(rename = "sessionID")]
    pub session_id: String,
}

// ---------------------------------------------------------------------------
// Eliot context-hook RPC (vendor-native `context` hook producer)
// ---------------------------------------------------------------------------

/// `eliot-context` version 1 `bind` input: `{sessionID}`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EliotContextBindInput {
    #[serde(rename = "sessionID")]
    pub session_id: String,
}

/// `bind` output: `{version, boundSessionID}`. Rust requires `version == 1`
/// and an exact `boundSessionID` match before advertising `turn_safe_point`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EliotContextBindOutput {
    pub version: i64,
    #[serde(rename = "boundSessionID")]
    pub bound_session_id: String,
}

/// `next` input: `{sessionID, waitMs}`. The producer clamps the wait and
/// allows at most one waiter; a second concurrent poll answers `{}`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EliotContextNextInput {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "waitMs")]
    pub wait_ms: i64,
}

/// One pending context hook as offered to Rust. `messageID` is the raw
/// native `Message.id` of the `context` event's last user message; Rust
/// must prove it through actual inbox delivery, never copy it blindly.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EliotContextHook {
    #[serde(rename = "hookID")]
    pub hook_id: String,
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "messageID")]
    pub message_id: String,
    #[serde(rename = "expiresInMs")]
    pub expires_in_ms: i64,
}

/// `next` output: `{}` or `{hook}`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct EliotContextNextOutput {
    #[serde(default)]
    pub hook: Option<EliotContextHook>,
}

/// `resolve` input: exact hook/session/message identity plus optional
/// `{context}` or `{cancel}`. Exactly one call per hook, never retried.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EliotContextResolveInput {
    #[serde(rename = "hookID")]
    pub hook_id: String,
    #[serde(rename = "sessionID")]
    pub session_id: String,
    #[serde(rename = "messageID")]
    pub message_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel: Option<bool>,
}

/// `resolve` output: `{status}`. Only `applied`/`empty` confirm a native
/// effect; every other status leaves the vendor hook unapplied.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct EliotContextResolveOutput {
    pub status: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_ref_uses_the_installed_provider_id_spelling() {
        let model: ModelRef = serde_json::from_value(serde_json::json!({
            "id": "big-pickle",
            "providerID": "opencode",
        }))
        .expect("installed Model.Ref shape decodes");
        assert_eq!(model.id, "big-pickle");
        assert_eq!(model.provider_id, "opencode");
        let encoded = serde_json::to_value(&model).expect("ModelRef encodes");
        assert_eq!(
            encoded,
            serde_json::json!({"id": "big-pickle", "providerID": "opencode"}),
            "requests must use the installed spelling too"
        );
    }

    #[test]
    fn prompt_response_carries_the_inbox_association_id() {
        let envelope: DataEnvelope<InboxUser> = serde_json::from_value(serde_json::json!({
            "data": {
                "id": "msg_1a441b8cc001bLqpib0Hw4udbS",
                "sessionID": "ses_e5bbf849fffePW9zdu151BlKTg",
                "time": {"created": 1793757133005i64},
                "type": "user",
                "payload": {"text": "x"},
                "delivery": "steer",
            }
        }))
        .expect("installed prompt shape decodes");
        assert_eq!(envelope.data.id, "msg_1a441b8cc001bLqpib0Hw4udbS");
        assert_eq!(envelope.data.session_id, "ses_e5bbf849fffePW9zdu151BlKTg");
    }

    #[test]
    fn failure_lifecycle_decodes_end_to_end() {
        let failed: SseEnvelope = serde_json::from_value(serde_json::json!({
            "id": "evt_1a441b8da001LO6hzOeX6E8hyW",
            "created": 1793757133018i64,
            "type": "session.execution.failed",
            "data": {
                "sessionID": "ses_e5bbf849fffePW9zdu151BlKTg",
                "error": {
                    "type": "provider.no-route",
                    "message": "Model unavailable: no-such-provider-zz/no-such-model-zz",
                }
            },
            "durable": {"aggregateID": "ses_e5bbf849fffePW9zdu151BlKTg", "seq": 9, "version": 1}
        }))
        .expect("installed failed-event shape decodes");
        let durable = failed.durable.expect("durable block");
        assert_eq!(durable.seq, 9);
        let data: ExecutionEventData =
            serde_json::from_value(failed.data).expect("failed data decodes");
        assert_eq!(data.session_id, "ses_e5bbf849fffePW9zdu151BlKTg");
        let error = data.error.expect("failed carries an error");
        assert_eq!(error.error_type, "provider.no-route");

        let page: MessagePage = serde_json::from_value(serde_json::json!({
            "data": [
                {"id": "msg_idle", "time": {"created": 3}, "type": "idle",
                 "outcome": "failed",
                 "error": {"type": "provider.no-route", "message": "nope"}},
                {"id": "msg_user", "time": {"created": 2}, "text": "x", "type": "user"},
            ],
            "cursor": {"previous": null, "next": null},
        }))
        .expect("installed message page decodes");
        assert!(matches!(page.data[0], Message::Idle(_)));
        assert!(matches!(page.data[1], Message::User(_)));
    }

    #[test]
    fn unknown_message_and_content_types_decode_as_other() {
        let page: MessagePage = serde_json::from_value(serde_json::json!({
            "data": [
                {"id": "m1", "time": {"created": 1}, "type": "shell"},
                {"id": "m2", "time": {"created": 2}, "type": "assistant",
                 "agent": "build", "model": {"id": "m", "providerID": "p"},
                 "content": [{"type": "tool", "callID": "c1"}]},
            ],
            "cursor": {},
        }))
        .expect("future types stay decodable");
        assert!(matches!(page.data[0], Message::Other));
        match &page.data[1] {
            Message::Assistant(assistant) => {
                assert!(matches!(assistant.content[0], AssistantContent::Other));
            }
            other => panic!("expected assistant, got {other:?}"),
        }
    }

    #[test]
    fn interrupt_response_is_unwrapped_and_idle_is_false() {
        // Installed `POST .../interrupt` on an idle session (v2.0.32).
        let idle: InterruptResponse = serde_json::from_value(serde_json::json!({
            "interrupted": false,
        }))
        .expect("unwrapped interrupt response decodes");
        assert!(!idle.interrupted);
    }

    #[test]
    fn permission_asked_and_replied_match_the_installed_shapes() {
        // `permission.asked` as received on `GET /api/event` (ephemeral: no
        // durable block, `location` ignored).
        let asked: SseEnvelope = serde_json::from_value(serde_json::json!({
            "id": "evt_live_asked",
            "created": 1793798123067i64,
            "type": "permission.asked",
            "location": {"directory": "C:\\probe"},
            "data": {"id": "per_live1", "sessionID": "ses_live",
                     "action": "read", "resources": ["C:/probe/other.env"]},
        }))
        .expect("installed asked envelope decodes");
        assert!(asked.durable.is_none());
        let data: PermissionAskedData =
            serde_json::from_value(asked.data).expect("asked data decodes");
        assert_eq!(data.id, "per_live1");
        assert_eq!(data.session_id, "ses_live");
        assert_eq!(data.action, "read");
        assert_eq!(data.resources, ["C:/probe/other.env"]);

        let replied: PermissionRepliedData = serde_json::from_value(serde_json::json!({
            "sessionID": "ses_live", "requestID": "per_live1", "reply": "reject",
        }))
        .expect("installed replied data decodes");
        assert_eq!(replied.reply, "reject");

        // `session.permission.reply` body spelling.
        let body = serde_json::to_value(&PermissionReplyRequest {
            decision: "once".to_owned(),
            message: None,
        })
        .expect("reply body encodes");
        assert_eq!(body, serde_json::json!({"decision": "once"}));
    }

    #[test]
    fn form_created_replied_and_cancelled_match_the_installed_shapes() {
        let created: FormCreatedData = serde_json::from_value(serde_json::json!({
            "form": {"id": "frm_live1", "sessionID": "ses_live", "title": "probe question",
                     "fields": [{"key": "choice", "title": "Pick one", "type": "string",
                                 "options": [{"value": "a", "label": "Alpha"},
                                             {"value": "b", "label": "Beta"}]}]},
        }))
        .expect("installed form.created data decodes");
        assert_eq!(created.form.fields.len(), 1);
        assert_eq!(created.form.fields[0].key, "choice");
        assert_eq!(created.form.fields[0].options.len(), 2);
        assert!(!created.form.fields[0].custom);

        let replied: FormRepliedData = serde_json::from_value(serde_json::json!({
            "id": "frm_live1", "sessionID": "ses_live", "answer": {"yn": true},
        }))
        .expect("installed form.replied data decodes");
        assert_eq!(replied.answer, serde_json::json!({"yn": true}));

        let cancelled: FormCancelledData = serde_json::from_value(serde_json::json!({
            "id": "frm_live1", "sessionID": "ses_live",
        }))
        .expect("installed form.cancelled data decodes");
        assert_eq!(cancelled.id, "frm_live1");
    }

    #[test]
    fn model_and_agent_selected_carry_the_effective_values() {
        // Durable configure confirmations; the plugin proves the switch by
        // readback and ignores these, but the shape stays pinned.
        let model: ModelSelectedData = serde_json::from_value(serde_json::json!({
            "sessionID": "ses_live",
            "model": {"id": "m3", "providerID": "p"},
            "previous": {"id": "m2", "providerID": "p", "variant": "default"},
        }))
        .expect("installed model.selected data decodes");
        assert_eq!(model.model.id, "m3");
        let agent: AgentSelectedData = serde_json::from_value(serde_json::json!({
            "sessionID": "ses_live", "agent": "build", "previous": "general",
        }))
        .expect("installed agent.selected data decodes");
        assert_eq!(agent.agent, "build");
    }

    #[test]
    fn switch_bodies_use_the_installed_spellings() {
        let model = serde_json::to_value(&SwitchModelRequest {
            model: ModelRef {
                id: "m".to_owned(),
                provider_id: "p".to_owned(),
                variant: None,
            },
        })
        .expect("switch model encodes");
        assert_eq!(
            model,
            serde_json::json!({"model": {"id": "m", "providerID": "p"}})
        );
        let agent = serde_json::to_value(&SwitchAgentRequest {
            agent: "general".to_owned(),
        })
        .expect("switch agent encodes");
        assert_eq!(agent, serde_json::json!({"agent": "general"}));
    }

    #[test]
    fn config_entries_identify_the_effective_document() {
        let entries: Vec<ConfigEntry> = serde_json::from_value(serde_json::json!([
            {"type": "document", "path": "C:\\eliot\\instance\\config\\opencode\\opencode.jsonc",
             "info": {"$schema": "https://opencode.ai/config.json",
                      "model": {"providerID": "eliot-probe", "model": "wedge-7"}, "mcp": {}}},
            {"type": "directory", "path": "C:\\eliot\\instance\\config\\opencode"},
        ]))
        .expect("installed config shape decodes");
        assert_eq!(entries[0].entry_type, "document");
        let model = entries[0]
            .info
            .as_ref()
            .and_then(|info| info.model.as_ref())
            .expect("default model");
        assert_eq!(model.provider_id, "eliot-probe");
        assert_eq!(model.model, "wedge-7");
    }
}
