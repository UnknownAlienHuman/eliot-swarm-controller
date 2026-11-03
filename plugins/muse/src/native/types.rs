//! Minimal MSP wire DTOs for the PR1 path, derived from the installed host's
//! exported stable schema (`muse schema generate-json-schema`, Muse Code 1.4.0,
//! bundle fingerprint `sha256:36466f63…0d3d7f`). Only the session-start,
//! turn-start, view-event, stop/close and readback shapes this path uses are
//! modelled. Unknown fields are ignored: the schema rules additive-optional
//! evolution, so a strict DTO would reject valid host frames.
//!
//! No SDK code is copied here; these structs are written from the schema.

use eliot_core::{ProviderRestriction, ProviderRestrictionKind};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// JSON-RPC envelope
// ---------------------------------------------------------------------------

/// Minimal JSON-RPC error body. PR1 maps `interrupted`/`cancelled` request
/// errors to unknown (the write may have applied); every other request error
/// proves the command was not admitted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NativeJsonRpcErrorBody {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl NativeJsonRpcErrorBody {
    /// Whether this request error leaves the write outcome uncertain.
    /// Schema `errors` table: `-32013 interrupted`, `-32014 cancelled`.
    pub fn is_uncertain_write(&self) -> bool {
        matches!(self.code, -32013 | -32014)
    }
}

// ---------------------------------------------------------------------------
// initialize (SS1.4.1)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeClientInfo {
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeClientCapabilities {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_input_dialogs: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeInitializeParams {
    pub client_info: NativeClientInfo,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<NativeClientCapabilities>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeServerInfo {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSchemaInfo {
    pub fingerprint: String,
    pub version: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeInitializeResult {
    pub experimental_api: bool,
    pub granted_capabilities: Vec<String>,
    pub muse_home: String,
    pub platform_family: String,
    pub platform_os: String,
    pub schema: NativeSchemaInfo,
    pub server_info: NativeServerInfo,
    pub user_agent: String,
}

// ---------------------------------------------------------------------------
// Session lifecycle (SS2.5.1, SS2.5.5, SS3.8, SS3.10, SS3.21)
// ---------------------------------------------------------------------------

/// `session/start` params. `commandId` is the caller-minted SS2.5 idempotency
/// handle (UUIDv7); the server never mints one. `sessionId` is omitted so the
/// server mints the session identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionStartParams {
    pub command_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_root: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionStartResult {
    pub session: NativeSession,
    pub view_cursor: String,
}

/// The session object every lifecycle method returns (SS2.4). Only the fields
/// PR1 reads back are modelled; the rest are ignored per additive-optional.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSession {
    pub session_id: String,
    pub status: String,
    pub active_turn_id: Option<String>,
    pub model_id: Option<String>,
    pub provider_id: Option<String>,
    pub workspace_root: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionSetReasoningEffortParams {
    pub command_id: String,
    pub reasoning_effort: String,
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionSetReasoningEffortResult {
    pub command_id: String,
    pub status: String,
}

/// `model/list` params (SS3.10): a query, not a command — no `commandId`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeModelListParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeModelListResult {
    #[serde(default)]
    pub models: Vec<NativeModelCatalogEntry>,
    pub provider_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeModelCatalogEntry {
    pub model_id: String,
    pub provider_id: String,
    #[serde(default)]
    pub variants: Option<NativeModelEffortVariants>,
}

/// Complete ordered selectable efforts, or an explicit unknown capability.
/// The unknown shape is opaque to PR1: when efforts cannot be proven, only
/// the closed tier vocabulary gates the profile value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NativeModelEffortVariants {
    Known(Vec<String>),
    Unknown(serde_json::Value),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionReadParams {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_items: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionReadResult {
    pub session: NativeSession,
    pub view_cursor: String,
}

// ---------------------------------------------------------------------------
// Turns (SS3.2)
// ---------------------------------------------------------------------------

/// One ordered text content part of a turn submission. File mentions stay
/// text (`@relative/path`); no other part type is used in PR1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeTurnInputPart {
    #[serde(rename = "type")]
    pub part_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

/// `turn/start` params. `commandId` is the caller-minted SS3.1.1 idempotency
/// handle (UUIDv7) and the fresh turn's `turnId` derives from it — this is
/// the exact native turn/command id obtained before the write.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeTurnStartParams {
    pub command_id: String,
    pub session_id: String,
    pub input: Vec<NativeTurnInputPart>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeTurnStartResult {
    pub command_id: String,
    pub disposition: String,
    pub started_new_turn: bool,
    pub status: String,
    pub turn_id: String,
}

// ---------------------------------------------------------------------------
// View events (SS4.x)
// ---------------------------------------------------------------------------

/// `turn/started` params. `turnId == commandId` for fresh turns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeTurnStartedNotification {
    pub command_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub view_cursor: String,
}

/// Turn terminal vocabulary (SS4.5.1): completed | failed | cancelled.
/// Wire-open: unknown values must not settle a turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NativeTurnTerminal {
    Completed,
    Failed,
    Cancelled,
    #[serde(other)]
    Unknown,
}

/// The settled turn-failure object: `{kind, message, retryable}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeTurnError {
    pub kind: String,
    pub message: String,
    pub retryable: bool,
}

impl NativeTurnError {
    /// Project only the installed structured auth kind. Free text, the
    /// retryable hint and unknown future kinds stay inert.
    pub fn provider_restriction(&self) -> Option<ProviderRestriction> {
        if self.kind != "authRequired" {
            return None;
        }
        Some(ProviderRestriction {
            kind: ProviderRestrictionKind::Auth,
            retry_after: None,
        })
    }
}

/// `turn/completed` params: the run's durable terminal record landed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeTurnCompletedNotification {
    pub session_id: String,
    pub terminal: NativeTurnTerminal,
    pub turn_id: String,
    pub view_cursor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<NativeTurnError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// One transcript item at one revision. Only the common identity fields and
/// the `agentMessage` text used by PR1 are modelled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeItem {
    pub item_id: String,
    pub kind: String,
    pub revision: i64,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeItemStartedNotification {
    pub item: NativeItem,
    pub session_id: String,
    pub view_cursor: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeItemCompletedNotification {
    pub item: NativeItem,
    pub session_id: String,
    pub view_cursor: String,
}

/// `session/started` params: the newly loaded session object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionStartedNotification {
    pub session: NativeSession,
}

/// `session/closed` params: the session was unloaded (`idle | hostShutdown`)
/// with the final view cursor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionClosedNotification {
    pub reason: String,
    pub session_id: String,
    pub view_cursor: Option<String>,
}

/// `view/gap` params: push delivery dropped `(after, next]`; delivery
/// continues from `next`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeViewGapNotification {
    pub after: String,
    pub next: String,
    pub session_id: String,
}

// ---------------------------------------------------------------------------
// view/page readback (SS4.7.3)
// ---------------------------------------------------------------------------

/// `view/page` params: cursor-paged reads of the session view, 1–1000 events.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeViewPageParams {
    pub session_id: String,
    pub limit: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direction: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// One page element: the live notification minus `jsonrpc`/`emittedAtMs`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NativeUnframedViewEvent {
    pub method: String,
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeViewPageResult {
    #[serde(default)]
    pub events: Vec<NativeUnframedViewEvent>,
    pub next_cursor: Option<String>,
}

/// The closed reasoning-effort tier vocabulary (SS3.2), spelled identically
/// on the fresh-turn and steer lanes and in `session/setReasoningEffort`.
pub const REASONING_EFFORT_TIERS: [&str; 8] = [
    "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
];

pub fn is_reasoning_effort_tier(value: &str) -> bool {
    REASONING_EFFORT_TIERS.contains(&value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uncertain_write_errors_are_only_interrupted_and_cancelled() {
        for code in [-32013, -32014] {
            let body = NativeJsonRpcErrorBody {
                code,
                message: "interrupted".to_owned(),
                data: None,
            };
            assert!(body.is_uncertain_write(), "code={code}");
        }
        for code in [-32700, -32600, -32601, -32602, -32001, -32010, -32031] {
            let body = NativeJsonRpcErrorBody {
                code,
                message: "rejected".to_owned(),
                data: None,
            };
            assert!(!body.is_uncertain_write(), "code={code}");
        }
    }

    #[test]
    fn provider_restriction_projects_only_auth_required() {
        let auth = NativeTurnError {
            kind: "authRequired".to_owned(),
            message: "login expired".to_owned(),
            retryable: true,
        };
        let restriction = auth.provider_restriction().expect("auth maps");
        assert_eq!(restriction.kind, ProviderRestrictionKind::Auth);
        assert_eq!(restriction.retry_after, None);
        for kind in [
            "stepLimit",
            "configError",
            "modelError",
            "launchError",
            "futureKind",
            "",
        ] {
            let error = NativeTurnError {
                kind: kind.to_owned(),
                message: "failed".to_owned(),
                retryable: false,
            };
            assert_eq!(error.provider_restriction(), None, "kind={kind}");
        }
    }

    #[test]
    fn session_dto_ignores_additive_optional_fields() {
        let session: NativeSession = serde_json::from_value(serde_json::json!({
            "sessionId": "session-1",
            "status": "idle",
            "activeTurnId": null,
            "modelId": "muse-spark-1.3-contributor",
            "providerId": null,
            "workspaceRoot": "C:\\work",
            "turnCount": 0,
            "futureField": {"nested": true},
        }))
        .expect("session decodes");
        assert_eq!(session.session_id, "session-1");
        assert_eq!(session.status, "idle");
        assert_eq!(session.active_turn_id, None);
    }

    #[test]
    fn unknown_turn_terminal_decodes_without_settling() {
        let notification: NativeTurnCompletedNotification =
            serde_json::from_value(serde_json::json!({
                "sessionId": "session-1",
                "terminal": "futureTerminal",
                "turnId": "turn-1",
                "viewCursor": "cursor-9",
            }))
            .expect("open terminal decodes");
        assert_eq!(notification.terminal, NativeTurnTerminal::Unknown);
    }

    #[test]
    fn reasoning_effort_vocabulary_is_the_closed_eight_tiers() {
        assert_eq!(REASONING_EFFORT_TIERS.len(), 8);
        for tier in REASONING_EFFORT_TIERS {
            assert!(is_reasoning_effort_tier(tier), "tier={tier}");
        }
        for other in ["", "default", "auto", "MEDIUM", " medium"] {
            assert!(!is_reasoning_effort_tier(other), "other={other}");
        }
    }
}
