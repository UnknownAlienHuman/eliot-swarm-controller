//! OpenCode wire DTOs, hand-typed from the installed server.
//!
//! Source: `GET /openapi.json` of `opencode serve` v2.2.7 (OpenAPI 3.1.0,
//! `opencode HttpApi version=0.0.1`, all routes under `/api/...`) plus the
//! `session-event.ts` / `permission.ts` / `form.ts` event definitions at tag
//! `v2.2.7` of `anomalyco/opencode`, verified against a locally started
//! server with a live model call (session create, prompt, message readback)
//! through the isolated per-Agent store. Unknown JSON fields are ignored on
//! deserialize for forward compatibility. DTOs only: no I/O, no process
//! spawning, no config access.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

/// One installed `Permission.Ruleset` entry: `{action, resource, effect}`.
/// Only the deny entries the launch path sets are modelled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionRule {
    pub action: String,
    pub resource: String,
    pub effect: String,
}

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permissions: Option<Vec<PermissionRule>>,
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

/// `Session.Info` subset: identity, agent, model, location, permissions.
/// Times, tokens, cost and the rest are ignored. `agent` and `model` are
/// absent until set; PR2 reads both back after the resume setters.
/// `permissions` carries the stored per-session ruleset; the launch path
/// reads it back to prove a `deny` profile's subagent denial is effective.
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
    #[serde(default)]
    pub permissions: Option<Vec<PermissionRule>>,
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
    #[serde(default)]
    pub status: Option<u16>,
}

impl StructuredError {
    pub fn failure_origin(&self) -> eliot_core::FailureOrigin {
        use eliot_core::FailureOrigin;
        // Installed 2.2.7 Session.StructuredError, verified /openapi.json.
        // Only provider.* error types plus an actual upstream HTTP failure
        // establish provider origin. A local server HTTP status does not.
        if matches!(
            self.error_type.as_str(),
            "provider.rate-limit"
                | "provider.auth"
                | "provider.quota"
                | "provider.free-limit"
                | "provider.chatgpt-limit"
        ) || self.error_type.starts_with("provider.")
            && self
                .status
                .is_some_and(|status| (400..600).contains(&status))
        {
            FailureOrigin::Provider
        } else if matches!(
            self.error_type.as_str(),
            "provider.transport" | "provider.timeout"
        ) {
            FailureOrigin::Environment
        } else {
            FailureOrigin::Harness
        }
    }
}

// ---------------------------------------------------------------------------
// Config readback
// ---------------------------------------------------------------------------

/// `GET /api/config` (`config.get`) entry subset. The document path
/// proves the per-Agent config file is effective, the default model
/// selects the native default, and the `mcp.servers` table proves the
/// winning transport of the managed Agent-scoped entry.
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
    #[serde(default)]
    pub mcp: Option<ConfigMcp>,
}

/// Effective `mcp` document subset: only the server table decodes. Unknown
/// fields and future server shapes are ignored; a missing table decodes as
/// empty, never as proof of any transport.
#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMcp {
    #[serde(default)]
    pub servers: std::collections::HashMap<String, ConfigMcpServer>,
}

/// One effective `mcp.servers` entry subset: the installed local shape
/// (`type: local`, `command` argv, optional `environment`) and the remote
/// shape (`type: remote`, `url`). Only these transport/auth-identity
/// fields decode; cosmetic keys (timeouts, cwd, codemode, ...) never
/// decide the proof.
#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMcpServer {
    #[serde(default, rename = "type")]
    pub server_type: Option<String>,
    #[serde(default)]
    pub command: Option<Vec<String>>,
    #[serde(default)]
    pub environment: Option<std::collections::HashMap<String, String>>,
    #[serde(default)]
    pub url: Option<String>,
}

// ---------------------------------------------------------------------------
// MCP inventory (`mcp.list`)
// ---------------------------------------------------------------------------

/// `GET /api/mcp` (`mcp.list`) entry: one configured MCP server with its
/// connection status. Only the name, status word and failure text decode;
/// `title`/`integrationID` and future status variants are ignored. A
/// missing status decodes as empty (never `connected`), so an unreadable
/// inventory reports unavailable instead of verified.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpServerReadback {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub status: McpStatusReadback,
}

/// `Mcp.Status.*` union subset: the installed 2.2.7 words are `connected`,
/// `pending`, `disabled`, `failed` and `needs_auth`; `failed` and
/// `needs_auth` carry an `error` string. Unknown words decode as-is and
/// never count as connected.
#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpStatusReadback {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub error: Option<String>,
}

impl McpServerReadback {
    /// Whether this entry proves the Agent-scoped server is serving.
    pub fn is_connected(&self, server_name: &str) -> bool {
        self.name == server_name && self.status.status == "connected"
    }

    /// Whether this entry counts as serving for the named Agent-scoped
    /// server: `connected` or `pending`. The inventory gate treats both
    /// words as serving, so the managed-transport proof must run for both
    /// too; otherwise a still-connecting managed entry would skip the
    /// proof while other pending servers are already refused.
    pub fn is_serving(&self, server_name: &str) -> bool {
        self.name == server_name && matches!(self.status.status.as_str(), "connected" | "pending")
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigModel {
    #[serde(rename = "providerID")]
    pub provider_id: String,
    pub model: String,
}

// ---------------------------------------------------------------------------
// Provider and model readback (adopted local backends)
// ---------------------------------------------------------------------------

/// `GET /api/model` item subset (`Model.Info`): one entry per effective
/// model. The endpoint answers an array; the caller selects the exact API
/// model id locally. Limits absent on the wire report unavailable, never
/// zero. `variants` carries the installed selectable `Model.Variant` ids
/// (the TUI "Variant" switch); a missing member decodes as unknown, never
/// as no variants.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeModelEntry {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default, rename = "providerID")]
    pub provider_id: Option<String>,
    /// Verbatim live display name (Model.Info.name) when present. The
    /// reader overlay carries names too; the picker falls back to the id.
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub limit: Option<NativeModelLimit>,
    #[serde(default)]
    pub variants: Option<Vec<NativeModelVariant>>,
}

/// Installed `Model.Variant` subset: only the selectable id is modelled.
/// Its `settings` differ per provider package (`reasoningEffort`,
/// `reasoning.effort`, `thinking`, ...) and stay ignored: the id alone
/// selects the variant through `Model.Ref.variant`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeModelVariant {
    #[serde(default)]
    pub id: Option<String>,
}

/// Effective per-model limit subset (`limit.context/input/output`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeModelLimit {
    #[serde(default)]
    pub context: Option<i64>,
    #[serde(default)]
    pub input: Option<i64>,
    #[serde(default)]
    pub output: Option<i64>,
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

/// Bound inference model in the installed `session.step.started` event.
#[derive(Debug, Deserialize)]
pub struct StepStartedData {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    pub model: ModelRef,
}

/// Installed 2.2.7 `Session.Event.Tool` base data shared by
/// `session.tool.input.started`, `session.tool.called`,
/// `session.tool.success` and `session.tool.failed`:
/// `{sessionID, assistantMessageID, id}` where `id` is the tool part id
/// (the vendor TUI's `callID`). `input.delta`/`input.ended`/`progress`
/// share the base but stay unmapped streaming detail.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolEventData {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    pub id: String,
}

/// `session.tool.input.started` data: the base plus the native tool
/// `name`. The only installed tool event carrying the name: `called`,
/// `success` and `failed` schemas have no name field, so completions
/// read it back from this event, exactly like the vendor TUI reducer.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolInputStartedData {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    pub id: String,
    pub name: String,
}

/// `session.tool.failed` data subset: the base plus the structured
/// error (`Session.StructuredError`, the same body execution terminals
/// carry). The installed schema declares `error` required, but the
/// adapter tolerates its absence: a failure without a message still
/// commits `tool.completed` with `status: error`, never a stream loss.
/// The optional `content`/`metadata` result payload is never persisted.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolFailedData {
    #[serde(rename = "sessionID")]
    pub session_id: String,
    pub id: String,
    #[serde(default)]
    pub error: Option<StructuredError>,
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
