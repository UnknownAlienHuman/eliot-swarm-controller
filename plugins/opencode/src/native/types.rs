//! OpenCode wire DTOs, hand-typed from the installed server.
//!
//! Source: `GET /openapi.json` of `opencode serve` v2.0.32 (OpenAPI 3.1.0,
//! `opencode HttpApi version=0.0.1`, all routes under `/api/...`) plus the
//! `session-event.ts` / `session-status-event.ts` event definitions at tag
//! `v2.0.32` of `anomalyco/opencode`, verified against a locally started
//! server. Unknown JSON fields are ignored on deserialize for forward
//! compatibility. DTOs only: no I/O, no process spawning, no config access.

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

/// `Session.Info` subset used by PR1: identity, model, location. Times,
/// tokens, cost and the rest are ignored.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    pub id: String,
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

/// `POST /api/session/{id}/prompt` (`session.prompt`) body. PR1 sends text
/// only; `delivery` stays server-default (`steer` on an idle session).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptRequest {
    pub text: String,
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
