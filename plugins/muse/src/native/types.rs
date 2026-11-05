//! Minimal MSP wire DTOs for the launch/send/stop path (PR1), the resume
//! path (PR2) and the native-request path (A1: approve/deny/answer), derived
//! from the installed host's exported stable schema
//! (`muse schema generate-json-schema`, Muse Code 1.4.0, bundle fingerprint
//! `sha256:36466f63…0d3d7f`, re-verified for A1). Only the session-start,
//! session-resume, turn-start, view-event, stop/close, request/decide/answer
//! and readback shapes this path uses are modelled. Unknown fields are
//! ignored: the schema rules additive-optional evolution, so a strict DTO
//! would reject valid host frames.
//!
//! No SDK code is copied here; these structs are written from the schema.

use eliot_core::{ProviderRestriction, ProviderRestrictionKind};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// JSON-RPC envelope
// ---------------------------------------------------------------------------

/// Whether a JSON-RPC request error code proves the command was not admitted:
/// the JSON-RPC pre-admission/validation failures (`-32700` parse, `-32600`
/// invalid request, `-32601` method not found, `-32602` invalid params), the
/// MSP admission-failure kind `-32030 commandRejected` (the SS3.6
/// admission-failure table), and the SS5 request-path validation failures the
/// installed schema defines as not admitted. A decision/answer refused with
/// one of these was never written, so the owner may reply again with
/// corrected values. Schema rows (stable `msp.schema.json`):
/// `ApprovalDecideParams.choiceId` ("One of the current `availableChoices`;
/// else -32052"), `ApprovalDecideParams.requirementId` ("a stale value is
/// -32053"), `UserInputAnswerParams` ("Answers that do not match the prompt
/// fail -32057"), `ErrorData.choiceId` ("Rejected server-minted choice on
/// `approvalChoiceInvalid`") and `ErrorData.currentRequirementId` ("Current
/// stage token on `approvalRequirementStale`" — the retry payload).
/// Everything else — including `-32603` internal, the lookup/state outcomes
/// `-32050`/`-32055` (target gone; the stream or turn end is authoritative),
/// `-32051`/`-32056` (already resolved/settled; the winning outcome travels
/// on the view stream) and `-32054` (reviewer unavailable; the write fate is
/// uncertain) — and unknown future kinds is unknown, never rejected.
pub fn is_not_admitted(code: i64) -> bool {
    matches!(
        code,
        -32700 | -32600 | -32601 | -32602 | -32030 | -32052 | -32053 | -32057
    )
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
    /// Starting approval mode; the server default rules when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_mode: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionStartResult {
    pub session: NativeSession,
    pub view_cursor: String,
}

/// The session object every lifecycle method returns (SS2.4). Only the fields
/// this path reads back are modelled; the rest are ignored per additive-optional.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSession {
    pub session_id: String,
    pub status: String,
    pub active_turn_id: Option<String>,
    pub model_id: Option<String>,
    pub provider_id: Option<String>,
    pub workspace_root: Option<String>,
    /// The effective approval-mode projection; optional on the session
    /// object, always present on the `session/setApprovalMode` ack.
    #[serde(default)]
    pub approval_mode: Option<NativeEffectiveApprovalModeState>,
    /// Total turns ever started on the session. The resume preflight proves
    /// the retained no-turn case from this counter (`Some(0)`), since a
    /// bounded view scan cannot prove absence; a missing counter refuses that
    /// case rather than guessing.
    #[serde(default)]
    pub turn_count: Option<i64>,
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

/// `session/setModel` params (SS3.8): the model-picker gesture. The
/// selection is a `ModelSelection` object: the profile carries no provider,
/// so only the catalog model id travels inside it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionSetModelParams {
    pub command_id: String,
    pub session_id: String,
    pub model: NativeModelSelection,
}

/// A model selection (SS3.8, `ModelSelection`): the catalog model id is
/// required; label, provider routing and profile stay omitted, so the
/// server default rules for everything but the model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeModelSelection {
    pub model_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionSetModelResult {
    pub command_id: String,
    pub status: String,
}

/// `session/setApprovalMode` params (SS5.12): select a preconfigured approval
/// enforcement mode mid-session; select, never create.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionSetApprovalModeParams {
    pub command_id: String,
    pub session_id: String,
    pub mode: String,
}

/// The effective approval-mode projection (`EffectiveApprovalModeState`,
/// SS5.12): the same object `session/setApprovalMode` returns as
/// `effectiveMode` and the `Session` object carries as `approvalMode`. Only
/// the mode in effect is modelled; source and audit identity are ignored.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeEffectiveApprovalModeState {
    pub mode: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionSetApprovalModeResult {
    pub command_id: String,
    pub status: String,
    pub effective_mode: NativeEffectiveApprovalModeState,
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
    pub history: NativeSessionHistory,
    pub session: NativeSession,
    pub view_cursor: String,
}

/// `session/resume` params (SS2.5.2). `commandId` is the caller-minted SS2.5
/// idempotency handle (UUIDv7): a resume that loads a session writes a
/// durable `SessionResumed` record, so a lost reply is unknown and the write
/// is never repeated. `excludeItems` keeps the served history out of the
/// result; the new generation pages what it needs with `view/page` instead of
/// republishing retained history as new turns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionResumeParams {
    pub command_id: String,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_items: Option<bool>,
}

/// The served-history envelope shared by `session/resume`, `session/fork`
/// and `session/read` (SS2.5.2). Retain the served mode and its typed none
/// reason; this path never republishes retained items.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionHistory {
    pub mode: String,
    #[serde(default)]
    pub none_reason: Option<NativeHistoryNoneReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NativeHistoryNoneReason {
    Excluded,
    CursorSuffix,
    HistoryBudget,
    ProjectionUnavailable,
    ProjectionReadLimit,
    #[serde(other)]
    Unknown,
}

impl NativeHistoryNoneReason {
    pub fn label(self) -> &'static str {
        match self {
            Self::Excluded => "excluded",
            Self::CursorSuffix => "cursorSuffix",
            Self::HistoryBudget => "historyBudget",
            Self::ProjectionUnavailable => "projectionUnavailable",
            Self::ProjectionReadLimit => "projectionReadLimit",
            Self::Unknown => "unknown",
        }
    }
}

impl NativeSessionHistory {
    /// Explicit exclusions/coverage limits do not establish a broken live
    /// view. Missing or unknown noneReason cannot establish healthy readback.
    pub fn unavailable_reason(&self) -> Option<NativeHistoryNoneReason> {
        if self.mode != "none" {
            return None;
        }
        match self.none_reason {
            Some(
                NativeHistoryNoneReason::Excluded
                | NativeHistoryNoneReason::CursorSuffix
                | NativeHistoryNoneReason::HistoryBudget,
            ) => None,
            reason => Some(reason.unwrap_or(NativeHistoryNoneReason::Unknown)),
        }
    }
}

#[cfg(test)]
mod view_health_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn history_exclusions_and_unavailable_reasons_are_distinct() {
        for (reason, expected) in [
            ("excluded", false),
            ("cursorSuffix", false),
            ("historyBudget", false),
            ("projectionUnavailable", true),
            ("projectionReadLimit", true),
            ("futureReason", true),
        ] {
            let history: NativeSessionHistory = serde_json::from_value(json!({
                "mode": "none", "noneReason": reason,
            }))
            .unwrap();
            assert_eq!(history.unavailable_reason().is_some(), expected, "{reason}");
        }
        let missing: NativeSessionHistory = serde_json::from_value(json!({"mode":"none"})).unwrap();
        assert_eq!(
            missing.unavailable_reason(),
            Some(NativeHistoryNoneReason::Unknown)
        );
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionViewHealthChangedNotification {
    pub session_id: String,
    pub health: NativeViewHealth,
    #[serde(default)]
    pub none_reason: Option<NativeHistoryNoneReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NativeViewHealth {
    Unavailable,
    #[serde(other)]
    Unknown,
}

/// `session/resume` result (SS2.5.2). `pendingRequests` is the late-joiner
/// pointer set: the full payloads arrive as re-issued server-to-client
/// requests right after the resume response, and the event path adopts only
/// ids that still bind to the live turn (guarded by its local observation
/// counter). Entries that belong to the dead generation's turn are
/// receipted-and-dropped orphans, never answered.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionResumeResult {
    pub history: NativeSessionHistory,
    pub pending_requests: Vec<NativePendingRequestPointer>,
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

/// `turn/cancel` params (SS3.5): plain cancellation of the exact active turn
/// on the normal command lane — halt running, keep queue. `turnId` is
/// schema-optional (omit to target the foreground turn), but this path
/// always names the exact active turn; the foreground fallback is never
/// used. Queued submits are a different object (`turn/unqueue`) and are
/// never touched here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeTurnCancelParams {
    pub command_id: String,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
}

/// `turn/cancel` result: admission only — the turn is over when its
/// `turn/completed` with terminal `cancelled` folds on the view stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeTurnCancelResult {
    pub command_id: String,
    pub status: String,
    pub turn_id: String,
}

/// `turn/steer` params (SS3.3): inject input into the turn the caller names,
/// failing if that turn is no longer active. `expectedTurnId` is the race
/// guard — input meant for turn A can never leak into turn B — so it always
/// carries the exact active turn. No per-steer tier travels; the session
/// standing default applies.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeTurnSteerParams {
    pub command_id: String,
    pub expected_turn_id: String,
    pub input: Vec<NativeTurnInputPart>,
    pub session_id: String,
}

/// `turn/steer` result: `turnId` names the absorbing turn and must equal the
/// expected one, else the input landed somewhere unexpected.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeTurnSteerResult {
    pub command_id: String,
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

/// `session/reasoningEffortChanged` params: a durable session-default
/// reasoning-effort change took effect. The schema's `ReasoningEffortState`
/// (the `ViewSnapshot.state.reasoningEffort` source of truth) is the fold of
/// the same record — "the same pair this notification carries" — so folding
/// the latest such view event reads back the standing default without a
/// second lifecycle write. Only the compared fields are modelled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionReasoningEffortChangedNotification {
    pub reasoning_effort: String,
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

// ---------------------------------------------------------------------------
// Native requests: approvals and user input (SS5.x)
// ---------------------------------------------------------------------------

/// One offered approval choice. `decision` and `scope` are wire-open: the
/// reply path accepts only the known approve/deny decisions for its verb and
/// never auto-approves an unknown future decision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeApprovalChoice {
    pub choice_id: String,
    pub decision: String,
    pub label: String,
    pub scope: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepts_feedback: Option<bool>,
}

/// The multi-stage race guard (SS5.4): a decision must carry the approval's
/// current requirement or the host answers `-32053 approvalRequirementStale`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeApprovalRequirementRef {
    pub approval_id: String,
    pub source_index: i64,
}

/// Full params shared by `approval/request` and `approval/requested` (SS5.3).
/// Only the bound identity, the offered choices with their current
/// requirement, and the owner-visible provenance are modelled. `rawArgs` is
/// deliberately unmodelled: it is unbounded tool-call JSON, never persisted —
/// the request path records only its byte length from the raw params.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeApprovalRequestParams {
    pub approval_id: String,
    pub available_choices: Vec<NativeApprovalChoice>,
    pub current_requirement_id: NativeApprovalRequirementRef,
    pub item_id: String,
    pub session_id: String,
    pub subject: serde_json::Value,
    pub tool_call_id: String,
    pub tool_name: String,
    pub turn_id: String,
    pub view_cursor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge_escalated: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protected_write: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subagent_origin: Option<serde_json::Value>,
}

/// `approval/decide` params (SS5.4): a standard SS3 command — session-loaded
/// host, caller-minted `commandId` idempotency handle, durable intake before
/// ack, value-identical replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeApprovalDecideParams {
    pub approval_id: String,
    pub choice_id: String,
    pub command_id: String,
    pub requirement_id: NativeApprovalRequirementRef,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback: Option<String>,
}

/// `approval/decide` result (SS5.4). `terminal == false` means the choice
/// satisfied one stage while further requirements remain: the approval stays
/// pending and an updated `approval/request` follows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeApprovalDecideResult {
    pub approval_id: String,
    pub command_id: String,
    pub status: String,
    pub terminal: bool,
}

/// `approval/resolved` params: the first durable terminal decision. Only the
/// binding fields are modelled; the decision itself has no Eliot channel
/// (`request.resolved` carries no details by Store contract).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeApprovalResolvedNotification {
    pub approval_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub view_cursor: String,
}

/// `approval/updated` params: the refreshed pending view plus one change.
/// Only the fields that refresh the in-memory offer (choices, requirement)
/// and the binding fields are modelled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeApprovalUpdatedNotification {
    pub approval_id: String,
    pub available_choices: Vec<NativeApprovalChoice>,
    pub current_requirement_id: NativeApprovalRequirementRef,
    pub session_id: String,
    pub view_cursor: String,
}

/// `approval/listPending` params (SS5.7): the pull dual of the re-issued
/// requests. A log-fold read — no lease, works on loaded and unloaded
/// sessions, never subscribes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeApprovalListPendingParams {
    pub session_id: String,
}

/// `approval/listPending` result (SS5.7): point-in-time, ordered by opening
/// view cursor. Acting on it race-safely is the caller's job — the decision
/// carries the `requirementId` guard regardless of how the client learned the
/// state, and this path additionally guards snapshot adoption with its local
/// observation counter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeApprovalListPendingResult {
    #[serde(default)]
    pub approvals: Vec<NativeApprovalRequestParams>,
    #[serde(default)]
    pub user_inputs: Vec<NativeUserInputRequestParams>,
}

/// One pending-request late-joiner pointer served by `session/resume`
/// (SS2.5.2). The full payloads arrive as re-issued server-to-client requests
/// right after the resume response; the pointer only names which ids to
/// expect (and to receipt-and-drop as orphans when they belong to the dead
/// generation's turn).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativePendingRequestPointer {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_input_id: Option<String>,
    pub view_cursor: String,
}

impl NativePendingRequestPointer {
    /// The exact native request id this pointer names, or `None` when the
    /// entry carries no id for its kind (accepted by the published schema).
    pub fn request_id(&self) -> Option<&str> {
        match self.kind.as_str() {
            "approval" => self.approval_id.as_deref(),
            "userInput" => self.user_input_id.as_deref(),
            _ => None,
        }
    }
}

/// One offered user-input option. Only the selected label is modelled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeUserInputOption {
    pub label: String,
}

/// The question's answer shape: closed `single | multiple` plus optional
/// selection bounds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeUserInputSelection {
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_selections: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_selections: Option<i64>,
}

/// One offered user-input question (SS5.10).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeUserInputQuestion {
    pub header: String,
    pub id: String,
    pub options: Vec<NativeUserInputOption>,
    pub question: String,
    pub selection: NativeUserInputSelection,
}

/// Full params shared by `userInput/request` and `userInput/requested`
/// (SS5.10). Only the bound identity, the offered questions, and the
/// owner-visible provenance are modelled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeUserInputRequestParams {
    pub item_id: String,
    pub questions: Vec<NativeUserInputQuestion>,
    pub session_id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub turn_id: String,
    pub user_input_id: String,
    pub view_cursor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_resolution_ms: Option<i64>,
}

/// One wire answer (SS5.10.2): `questionId` plus exactly one of
/// `selectedLabel` (single mode), `selectedLabels` (multiple mode, within
/// min/max bounds), or `freeText` (<=500 chars); plus an optional `note`
/// (<=500 chars).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeUserInputAnswer {
    pub question_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_labels: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub free_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// `userInput/answer` params (SS5.10.2): answer every question.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeUserInputAnswerParams {
    pub answers: Vec<NativeUserInputAnswer>,
    pub command_id: String,
    pub session_id: String,
    pub user_input_id: String,
}

/// `userInput/answer` result (SS5.10.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeUserInputAnswerResult {
    pub command_id: String,
    pub status: String,
    pub user_input_id: String,
}

/// `userInput/cancel` params (SS5.10.2): decline to answer; the tool call
/// resolves with a cancelled result the model sees.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeUserInputCancelParams {
    pub command_id: String,
    pub session_id: String,
    pub user_input_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// `userInput/cancel` result (SS5.10.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeUserInputCancelResult {
    pub command_id: String,
    pub status: String,
    pub user_input_id: String,
}

/// `userInput/settled` params: the first durable prompt settlement. Only the
/// binding fields are modelled; the outcome has no Eliot channel.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeUserInputSettledNotification {
    pub session_id: String,
    pub user_input_id: String,
    pub view_cursor: String,
}

pub fn is_reasoning_effort_tier(value: &str) -> bool {
    REASONING_EFFORT_TIERS.contains(&value)
}

/// The closed approval-mode vocabulary (`ApprovalMode`, SS5.12): select, never
/// create. Anything outside it is rejected before any native call.
pub const APPROVAL_MODES: [&str; 4] = ["allowAll", "promptUnmatched", "onRequest", "denyUnmatched"];

pub fn is_approval_mode(value: &str) -> bool {
    APPROVAL_MODES.contains(&value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn only_pre_admission_errors_prove_no_write() {
        // The request-path validation failures the installed schema defines
        // as not admitted (SS5 rows cited on `is_not_admitted`): a decision
        // refused with one of these was never written, so the owner may reply
        // again with corrected values.
        for code in [
            -32700, -32600, -32601, -32602, -32030, -32052, -32053, -32057,
        ] {
            assert!(is_not_admitted(code), "code={code}");
        }
        // Internal errors, uncertain writes, all other MSP kinds and unknown
        // future codes never prove rejection; the caller maps them to unknown.
        // The remaining request-path kinds name lookup/state outcomes with no
        // validation row (target gone, already resolved/settled, reviewer
        // unavailable): a lost decide/answer race stays unknown, never a
        // proof of non-admission.
        for code in [
            -32603, -32001, -32002, -32010, -32011, -32013, -32014, -32020, -32021, -32022, -32024,
            -32025, -32031, -32032, -32040, -32041, -32042, -32050, -32051, -32054, -32055, -32056,
            -32999, 0,
        ] {
            assert!(!is_not_admitted(code), "code={code}");
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
    fn resume_result_decodes_excluded_history_and_pending_requests() {
        let resumed: NativeSessionResumeResult = serde_json::from_value(serde_json::json!({
            "history": {"items": null, "mode": "none", "noneReason": "excluded", "snapshot": null},
            "pendingRequests": [],
            "session": {
                "sessionId": "session-1",
                "status": "idle",
                "activeTurnId": null,
                "modelId": "muse-spark-1.3-contributor",
                "providerId": "meta",
                "workspaceRoot": "C:\\work",
                "turnCount": 1,
            },
            "viewCursor": "cursor-9",
        }))
        .expect("resume result decodes");
        assert_eq!(resumed.history.mode, "none");
        assert!(resumed.pending_requests.is_empty());
        assert_eq!(resumed.session.session_id, "session-1");
        assert_eq!(resumed.session.turn_count, Some(1));
        assert_eq!(resumed.view_cursor, "cursor-9");
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

    #[test]
    fn approval_vocabulary_is_the_closed_four_modes() {
        assert_eq!(APPROVAL_MODES.len(), 4);
        for mode in APPROVAL_MODES {
            assert!(is_approval_mode(mode), "mode={mode}");
        }
        for other in [
            "",
            "default",
            "never",
            "on-request",
            "PROMPTUNMATCHED",
            " allowAll",
        ] {
            assert!(!is_approval_mode(other), "other={other}");
        }
    }

    #[test]
    fn cancel_and_steer_params_shape() {
        let cancel = NativeTurnCancelParams {
            command_id: "cmd-1".to_owned(),
            session_id: "session-1".to_owned(),
            turn_id: Some("turn-1".to_owned()),
        };
        assert_eq!(
            serde_json::to_value(&cancel).expect("cancel params serialize"),
            json!({
                "commandId": "cmd-1",
                "sessionId": "session-1",
                "turnId": "turn-1",
            })
        );
        let steer = NativeTurnSteerParams {
            command_id: "cmd-2".to_owned(),
            expected_turn_id: "turn-1".to_owned(),
            input: vec![NativeTurnInputPart {
                part_type: "text".to_owned(),
                text: Some("follow-up".to_owned()),
            }],
            session_id: "session-1".to_owned(),
        };
        assert_eq!(
            serde_json::to_value(&steer).expect("steer params serialize"),
            json!({
                "commandId": "cmd-2",
                "expectedTurnId": "turn-1",
                "input": [{"type": "text", "text": "follow-up"}],
                "sessionId": "session-1",
            })
        );
    }

    #[test]
    fn configure_setter_params_shape() {
        let set_model = NativeSessionSetModelParams {
            command_id: "cmd-3".to_owned(),
            session_id: "session-1".to_owned(),
            model: NativeModelSelection {
                model_id: "muse-test-model".to_owned(),
            },
        };
        assert_eq!(
            serde_json::to_value(&set_model).expect("setModel params serialize"),
            json!({
                "commandId": "cmd-3",
                "sessionId": "session-1",
                "model": {"modelId": "muse-test-model"},
            })
        );
        let set_approval = NativeSessionSetApprovalModeParams {
            command_id: "cmd-4".to_owned(),
            session_id: "session-1".to_owned(),
            mode: "promptUnmatched".to_owned(),
        };
        assert_eq!(
            serde_json::to_value(&set_approval).expect("setApprovalMode params serialize"),
            json!({
                "commandId": "cmd-4",
                "sessionId": "session-1",
                "mode": "promptUnmatched",
            })
        );
        let ack: NativeSessionSetApprovalModeResult = serde_json::from_value(json!({
            "commandId": "cmd-4",
            "status": "accepted",
            "effectiveMode": {
                "mode": "promptUnmatched",
                "source": "explicit",
                "lastCommandId": "cmd-4",
            },
            "applyOutcome": "applied",
        }))
        .expect("setApprovalMode ack decodes");
        assert_eq!(ack.effective_mode.mode, "promptUnmatched");
    }

    #[test]
    fn approval_request_decodes_choices_requirement_and_child_origin() {
        let params: NativeApprovalRequestParams = serde_json::from_value(serde_json::json!({
            "approvalId": "approval-1",
            "availableChoices": [
                {"choiceId": "allow", "decision": "approved", "label": "Allow",
                 "scope": "once", "acceptsFeedback": false},
                {"choiceId": "deny", "decision": "denied", "label": "Deny",
                 "scope": "once", "acceptsFeedback": true,
                 "rulePreview": "deny rm -rf /"},
            ],
            "currentRequirementId": {"approvalId": "approval-1", "sourceIndex": 1},
            "itemId": "item-1",
            "judgeEscalated": false,
            "protectedWrite": true,
            "rawArgs": "{\"command\":\"rm -rf /\"}",
            "sessionId": "session-1",
            "sourceRange": {"first": {"streamSeq": 1}, "last": {"streamSeq": 1},
                            "stream": {"kind": "run"}},
            "subagentOrigin": {"childRunId": "run-1", "childSessionId": "child-1",
                               "subagentId": "subagent-1"},
            "subject": {"kind": "shell", "command": "rm -rf /"},
            "taskId": "task-1",
            "toolCallId": "call-1",
            "toolName": "shell",
            "turnId": "turn-1",
            "viewCursor": "cursor-3",
        }))
        .expect("approval request decodes");
        assert_eq!(params.approval_id, "approval-1");
        assert_eq!(params.available_choices.len(), 2);
        assert_eq!(params.current_requirement_id.source_index, 1);
        assert!(params.subagent_origin.is_some());
        assert_eq!(params.view_cursor, "cursor-3");
    }

    #[test]
    fn user_input_request_decodes_questions_and_selection_modes() {
        let params: NativeUserInputRequestParams = serde_json::from_value(serde_json::json!({
            "autoResolutionMs": 60000,
            "itemId": "item-2",
            "questions": [{
                "header": "Deploy",
                "id": "q1",
                "options": [{"label": "staging"}, {"label": "production"}],
                "question": "Where?",
                "selection": {"mode": "single"},
            }],
            "sessionId": "session-1",
            "sourceRange": {"first": {"streamSeq": 1}, "last": {"streamSeq": 1},
                            "stream": {"kind": "run"}},
            "toolCallId": "call-2",
            "toolName": "ask",
            "turnId": "turn-1",
            "userInputId": "prompt-1",
            "viewCursor": "cursor-4",
        }))
        .expect("user-input request decodes");
        assert_eq!(params.user_input_id, "prompt-1");
        assert_eq!(params.questions.len(), 1);
        assert_eq!(params.questions[0].selection.mode, "single");
    }

    #[test]
    fn resume_pointer_names_its_exact_request_id_or_nothing() {
        let approval: NativePendingRequestPointer = serde_json::from_value(serde_json::json!({
            "kind": "approval", "approvalId": "approval-1", "viewCursor": "cursor-1",
        }))
        .expect("approval pointer decodes");
        assert_eq!(approval.request_id(), Some("approval-1"));
        let prompt: NativePendingRequestPointer = serde_json::from_value(serde_json::json!({
            "kind": "userInput", "userInputId": "prompt-1", "viewCursor": "cursor-2",
        }))
        .expect("prompt pointer decodes");
        assert_eq!(prompt.request_id(), Some("prompt-1"));
        // The published schema accepts an entry carrying no id at all; the
        // path treats it as naming nothing, never as a blank request id.
        let empty: NativePendingRequestPointer = serde_json::from_value(serde_json::json!({
            "kind": "approval", "viewCursor": "cursor-3",
        }))
        .expect("id-less pointer decodes");
        assert_eq!(empty.request_id(), None);
        let future: NativePendingRequestPointer = serde_json::from_value(serde_json::json!({
            "kind": "futureKind", "approvalId": "approval-9", "viewCursor": "cursor-4",
        }))
        .expect("future-kind pointer decodes");
        assert_eq!(future.request_id(), None);
    }

    #[test]
    fn list_pending_result_decodes_both_pending_halves() {
        let result: NativeApprovalListPendingResult = serde_json::from_value(serde_json::json!({
            "approvals": [],
            "userInputs": [],
        }))
        .expect("empty pending halves decode");
        assert!(result.approvals.is_empty());
        assert!(result.user_inputs.is_empty());
    }
}
