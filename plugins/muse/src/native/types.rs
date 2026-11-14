//! Minimal MSP wire DTOs for the launch/send/stop path (PR1), the resume
//! path (PR2) and the native-request path (A1: approve/deny/answer), derived
//! from the installed host's exported stable schema
//! (`muse schema generate-json-schema`, Muse Code 1.4.0-R4161.1, bundle
//! fingerprint `sha256:36466f63…0d3d7f`, re-verified for A1, for item
//! normalization and for the single bounded `item/readOutput` patch fetch).
//! Only the session-start, session-resume, turn-start, view-event,
//! stop/close, request/decide/answer, readback, item tool-identity and
//! stored-patch-reference shapes this path uses are modelled. Unknown fields
//! are ignored: the schema rules additive-optional evolution, so a strict DTO
//! would reject valid host frames.
//!
//! No SDK code is copied here; these structs are written from the schema.

use eliot_core::{
    ProviderRestriction, ProviderRestrictionKind, SessionCost, SessionTokenTotals, SessionUsage,
    TurnUsage,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
/// `usage`, when present, is the installed schema's turn aggregate: the
/// turn's aggregate token usage, summed across the turn's model completions
/// (same shape as `session/tokenUsage.usage`). The per-completion
/// `session/tokenUsage` events (raw counters plus the server-derived
/// counted-once `promptTokens`/`totalTokens` and the session `cumulative`
/// block) and the `session/contextUsage` occupancy triple are session-scoped
/// facts, not turn expenditure, and stay unmapped here.
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<NativeTurnUsage>,
}

/// Installed `TokenUsage` (the runtime's `Usage`, camelCased): raw token
/// counters verbatim from the durable record. `cachedTokens` is
/// provider-convention-dependent (inside or beside `inputTokens`), so it is
/// parsed but never mapped to a cache split; only the explicit
/// `cacheReadTokens`/`cacheWriteTokens` splits project. Unknown fields are
/// ignored per additive-optional evolution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeTurnUsage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_tokens: Option<i64>,
}

/// `session/tokenUsage` params (installed 1.4.0 schema subset): one event
/// per model completion that reports usage, carrying the raw counters
/// verbatim plus the server-derived counted-once `promptTokens`/`totalTokens`
/// and the session `cumulative` block (running counted-once session totals
/// with the server-computed estimated `cost`). Subagent/workflow-child usage
/// is never folded in. Only the session-owned fields below are modelled;
/// every other member stays unparsed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionTokenUsageNotification {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view_cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cumulative: Option<NativeCumulativeTokenUsage>,
}

/// Session running totals of counted-once usage (installed 1.4.0 schema
/// subset): the cache splits join the counters, and the server-computed
/// `cost` is the documented estimated exception. All members are optional
/// locally so partial native data stays decodable; unavailable members
/// project as null, never zero.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeCumulativeTokenUsage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<NativeCumulativeCost>,
}

/// Server-computed session cost member (installed 1.4.0 schema subset):
/// the estimated USD total. `partial` marks legs without price data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeCumulativeCost {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partial: Option<bool>,
}

/// `session/contextUsage` params (installed 1.4.0 schema subset): the
/// counted-once context-window occupancy at the latest provider-reported
/// durable fact, joined with the host's pressure basis. `windowTokens` is
/// absent when the basis has no limit and is never invented.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionContextUsageNotification {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view_cursor: Option<String>,
}

/// Merge one `session/tokenUsage` cumulative block into the retained session
/// snapshot. `promptTokens` is the counted-once input total, so it projects
/// to `input_tokens`; `outputTokens` and the cache splits project directly.
/// No total is derived (`totalTokens` is the server's per-completion honesty
/// sum, not a session counter). The server-computed `cost.usd` is an
/// estimated USD total and maps to `cumulative_cost` with unit
/// `usd_estimate`, never converted. Yields `None` when the event contributes
/// nothing new. Negative counters and non-finite costs stay null.
pub(crate) fn session_usage_from_token_usage(
    notification: &NativeSessionTokenUsageNotification,
    previous: Option<&SessionUsage>,
) -> Option<SessionUsage> {
    let counter = |value: Option<i64>| value.filter(|value| *value >= 0);
    let mut merged = previous.cloned().unwrap_or(SessionUsage {
        context_used_tokens: None,
        context_window_tokens: None,
        cumulative_tokens: None,
        cumulative_cost: None,
        source: None,
        observed_at: None,
    });
    let mut changed = false;
    if let Some(cumulative) = &notification.cumulative {
        let totals = SessionTokenTotals {
            input_tokens: counter(cumulative.prompt_tokens),
            output_tokens: counter(cumulative.output_tokens),
            cache_read_tokens: counter(cumulative.cache_read_tokens),
            cache_write_tokens: counter(cumulative.cache_write_tokens),
        };
        if (totals.input_tokens.is_some()
            || totals.output_tokens.is_some()
            || totals.cache_read_tokens.is_some()
            || totals.cache_write_tokens.is_some())
            && merged.cumulative_tokens.as_ref() != Some(&totals)
        {
            merged.cumulative_tokens = Some(totals);
            changed = true;
        }
        if let Some(usd) = cumulative.cost.as_ref().and_then(|cost| cost.usd)
            && usd.is_finite()
            && usd >= 0.0
        {
            let cost = SessionCost {
                amount: Some(usd),
                unit: Some("usd_estimate".to_owned()),
            };
            if merged.cumulative_cost.as_ref() != Some(&cost) {
                merged.cumulative_cost = Some(cost);
                changed = true;
            }
        }
    }
    if !changed {
        return None;
    }
    merged.source = Some("muse:session/tokenUsage".to_owned());
    merged.observed_at = None;
    Some(merged)
}

/// Merge one `session/contextUsage` triple into the retained session
/// snapshot: each notification replaces both the counted-once occupancy and
/// the effective window size. Absent or negative members become null and
/// never keep the previous window. Yields `None` when the event contributes
/// nothing new.
pub(crate) fn session_usage_from_context_usage(
    notification: &NativeSessionContextUsageNotification,
    previous: Option<&SessionUsage>,
) -> Option<SessionUsage> {
    let counter = |value: Option<i64>| value.filter(|value| *value >= 0);
    let used = counter(notification.used_tokens);
    let window = counter(notification.window_tokens);
    let mut merged = previous.cloned().unwrap_or(SessionUsage {
        context_used_tokens: None,
        context_window_tokens: None,
        cumulative_tokens: None,
        cumulative_cost: None,
        source: None,
        observed_at: None,
    });
    if merged.context_used_tokens == used && merged.context_window_tokens == window {
        return None;
    }
    merged.context_used_tokens = used;
    merged.context_window_tokens = window;
    merged.source = Some("muse:session/contextUsage".to_owned());
    merged.observed_at = None;
    Some(merged)
}

/// Map one terminal `turn/completed` usage block to turn expenditure at
/// `turn` scope. Only genuinely supplied nonnegative counters are mapped;
/// missing values stay null (never zero, never a sum), and an absent or
/// wholly unusable block yields no usage at all so completion proceeds.
/// `cachedTokens` is never mapped (ambiguous convention) and no total is
/// ever derived: the counted-once derivations live in the per-completion
/// `session/tokenUsage` session facts, not in the terminal.
pub(crate) fn turn_usage(notification: &NativeTurnCompletedNotification) -> Option<TurnUsage> {
    let usage = notification.usage.as_ref()?;
    let counter = |value: Option<i64>| value.filter(|value| *value >= 0);
    let input_tokens = counter(usage.input_tokens);
    let output_tokens = counter(usage.output_tokens);
    let reasoning_tokens = counter(usage.reasoning_tokens);
    let cache_read_tokens = counter(usage.cache_read_tokens);
    let cache_write_tokens = counter(usage.cache_write_tokens);
    if input_tokens.is_none()
        && output_tokens.is_none()
        && reasoning_tokens.is_none()
        && cache_read_tokens.is_none()
        && cache_write_tokens.is_none()
    {
        return None;
    }
    Some(TurnUsage {
        scope: Some("turn".to_owned()),
        message_id: None,
        input_tokens,
        output_tokens,
        reasoning_tokens,
        thinking_tokens: None,
        cache_read_tokens,
        cache_write_tokens,
        total_tokens: None,
    })
}

/// One transcript item at one revision. Only the common identity fields, the
/// `agentMessage` text used by PR1, the `toolCall`/`userShell` tool identity,
/// the `toolCall` verbatim argument JSON and the `toolCall` stored-patch
/// reference consumed by item normalization are modelled: `tool` is the
/// `toolCall` tool name (`userShell` carries none and normalizes under its
/// kind), `args` carries the model-authored argument JSON whose inline file
/// target feeds native file-change hints, and `patch_ref` carries only the
/// durable stored-output id (plus its servability flag) behind which a
/// multi-file edit diff waits. Every other per-kind field (`visibleOutput`,
/// `outputRef`, `patchSummary`, `commandText`, ...) stays unparsed: unknown
/// fields are ignored, so additive schema evolution never rejects a frame.
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
    pub tool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub patch_ref: Option<NativePatchRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
}

/// Stored structured-patch reference on a `toolCall` item (SS4.5.5, #33025):
/// `kind: "tool_patch"`, `mediaType: "application/json"`, fetched via
/// `item/readOutput` with `patch_ref.id` (never the `uri`, which is
/// display/provenance). Only the fetch identity and the servability flag are
/// modelled: the patch body itself is read once, bounded, and never
/// persisted — only file paths from its headers reach the committed event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativePatchRef {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub availability: Option<String>,
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

// ---------------------------------------------------------------------------
// item/readOutput readback (SS4.7.4)
// ---------------------------------------------------------------------------

/// `item/readOutput` params (tdd SS4.7.4): byte-ranged fetch of stored full
/// output the view truncated. `output_ref` is the stored output's durable id
/// (for a patch fetch, `patchRef.id` — never the `uri`); `offset_bytes` and
/// `length_bytes` are stored-byte ranges (default and max 6 MiB). Read-only;
/// works on loaded and unloaded sessions; takes no lease.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeItemReadOutputParams {
    pub session_id: String,
    pub item_id: String,
    pub output_ref: String,
    pub offset_bytes: i64,
    pub length_bytes: i64,
}

/// `item/readOutput` result (tdd SS4.7.4): one stored-byte page. Text media
/// is always served `utf8`; binary media is `base64` (closed vocabulary).
/// Only the decoded `content` headers are ever consumed downstream, and only
/// file paths from them are persisted — never the body itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeItemReadOutputResult {
    #[serde(default)]
    pub byte_len: i64,
    pub content: String,
    pub encoding: String,
    #[serde(default)]
    pub eof: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(default)]
    pub offset_bytes: i64,
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

/// `view/gap` params: push delivery dropped `(after, next)`; delivery
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

/// Read-only reattachment: replay `(after, head]` before live view delivery.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeViewSubscribeParams {
    pub session_id: String,
    pub after: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeViewSubscribeResult {
    pub view_cursor: String,
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
/// binding fields are modelled, plus the optional native id of the decide
/// command that resolved it: when a reply RPC outcome stays ambiguous, an
/// exact `decidedByCommandId` match against the captured outstanding decide
/// proves this generation's write applied. The decision itself has no Eliot
/// channel (`request.resolved` carries no details by Store contract).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeApprovalResolvedNotification {
    pub approval_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub view_cursor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decided_by_command_id: Option<String>,
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

// ---------------------------------------------------------------------------
// Workflow children (installed WorkflowChild schema + wave3b actual wire)
// ---------------------------------------------------------------------------

/// One workflow child lifecycle fact: a `children[]` entry of a `workflow`
/// item. Only the fields the turn-only observer consumes are modelled.
///
/// Installed `WorkflowChild` wire schema (Muse Code 1.4.0, bundle fingerprint
/// `sha256:36466f63…0d3d7f`, wave3b probe receipt): `attempt`, `childId`,
/// `durationMs`, `label`, `phase`, `resultRef`, `status`, `terminal`, `usage`.
/// Correlation boundary: keyed strictly by `(childId, attempt)` inside
/// `Item.children` under `workflowRunId` — no `sessionId` or `turnId` exists
/// on the wire for a child, so the Eliot turn key is synthesized, never read.
/// Deliberately unmodelled: `durationMs` (a duration, never converted into an
/// invented timestamp), `resultRef` (never dereferenced; no `message.completed`
/// or `session.*` fact is ever synthesized from it), `usage` (provenance and
/// transitive attribution unknown; never totaled), `label`/`phase` (ignored).
/// Unknown fields are ignored per additive-optional evolution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeWorkflowChild {
    pub child_id: String,
    pub attempt: i64,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<String>,
}

/// A `workflow` transcript item at one revision: a full re-emission of the
/// child list, never a delta. `revision`/`status`/`sourceRange` stay the
/// actual wire values; the child terminal (not the item status) drives the
/// mapping, and the `message` workflow-summary composite is unsupported — no
/// public history fact is ever synthesized from it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeWorkflowItem {
    pub item_id: String,
    pub revision: i64,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    pub workflow_run_id: String,
    #[serde(default)]
    pub children: Vec<NativeWorkflowChild>,
}

/// `item/started` | `item/updated` | `item/completed` envelope for a workflow
/// item. The view cursor is not needed (revisions order the evidence); the
/// `sourceRange` is retained for provenance, never as a replay cursor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeWorkflowItemNotification {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_range: Option<Value>,
    pub item: NativeWorkflowItem,
}

impl NativeTurnTerminal {
    /// Parse a workflow-child `terminal` string with the turn-terminal
    /// vocabulary (`completed` | `failed` | `cancelled`). Wire-open: unknown
    /// values decode as `Unknown` and must not settle a turn.
    pub fn parse_workflow_terminal(value: &str) -> Self {
        match value {
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => Self::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn completed_with_usage(usage: Value) -> NativeTurnCompletedNotification {
        serde_json::from_value(json!({
            "sessionId": "session-1",
            "terminal": "completed",
            "turnId": "turn-1",
            "viewCursor": "cursor-1",
            "usage": usage,
        }))
        .expect("terminal with usage decodes")
    }

    #[test]
    fn terminal_usage_maps_explicit_counters_at_turn_scope() {
        let notification = completed_with_usage(json!({
            "inputTokens": 12,
            "outputTokens": 4,
            "reasoningTokens": 2,
            "cachedTokens": 9,
            "cacheReadTokens": 3,
            "cacheWriteTokens": 1,
        }));
        let usage = turn_usage(&notification).expect("usable usage maps");
        assert_eq!(usage.scope, Some("turn".to_owned()));
        assert_eq!(usage.message_id, None);
        assert_eq!(usage.input_tokens, Some(12));
        assert_eq!(usage.output_tokens, Some(4));
        assert_eq!(usage.reasoning_tokens, Some(2));
        assert_eq!(usage.thinking_tokens, None);
        assert_eq!(usage.cache_read_tokens, Some(3));
        assert_eq!(usage.cache_write_tokens, Some(1));
        // The ambiguous `cachedTokens` convention never maps, and no total
        // is derived from the parts.
        assert_eq!(usage.total_tokens, None);
    }

    #[test]
    fn terminal_usage_without_usable_counters_stays_null() {
        let absent: NativeTurnCompletedNotification = serde_json::from_value(json!({
            "sessionId": "session-1",
            "terminal": "completed",
            "turnId": "turn-1",
            "viewCursor": "cursor-1",
        }))
        .expect("terminal without usage decodes");
        assert_eq!(turn_usage(&absent), None);
        let empty = completed_with_usage(json!({}));
        assert_eq!(turn_usage(&empty), None);
        let negative = completed_with_usage(json!({"inputTokens": -5}));
        assert_eq!(turn_usage(&negative), None);
    }

    fn token_usage_notification(cumulative: Value) -> NativeSessionTokenUsageNotification {
        serde_json::from_value(json!({
            "sessionId": "session-1",
            "turnId": "turn-1",
            "viewCursor": "cursor-1",
            "cumulative": cumulative,
        }))
        .expect("session/tokenUsage decodes")
    }

    #[test]
    fn session_token_usage_maps_cumulative_and_estimated_cost() {
        let notification = token_usage_notification(json!({
            "promptTokens": 120,
            "outputTokens": 30,
            "totalTokens": 150,
            "cacheReadTokens": 40,
            "cacheWriteTokens": 5,
            "cost": {"usd": 0.25, "partial": false},
        }));
        let merged = session_usage_from_token_usage(&notification, None).expect("cumulative maps");
        let totals = merged
            .cumulative_tokens
            .as_ref()
            .expect("cumulative counters");
        // `promptTokens` is the counted-once input total; `totalTokens`
        // never lands in the snapshot.
        assert_eq!(totals.input_tokens, Some(120));
        assert_eq!(totals.output_tokens, Some(30));
        assert_eq!(totals.cache_read_tokens, Some(40));
        assert_eq!(totals.cache_write_tokens, Some(5));
        let cost = merged.cumulative_cost.as_ref().expect("estimated cost");
        assert_eq!(cost.amount, Some(0.25));
        assert_eq!(cost.unit.as_deref(), Some("usd_estimate"));
        assert_eq!(merged.context_used_tokens, None);
        assert_eq!(merged.source.as_deref(), Some("muse:session/tokenUsage"));
        // A repeat contributes nothing new, so it commits nothing.
        assert_eq!(
            session_usage_from_token_usage(&notification, Some(&merged)),
            None
        );
    }

    #[test]
    fn session_token_usage_without_usable_cumulative_stays_null() {
        let missing_cumulative: NativeSessionTokenUsageNotification =
            serde_json::from_value(json!({"sessionId": "session-1"}))
                .expect("session/tokenUsage without cumulative decodes");
        assert_eq!(
            session_usage_from_token_usage(&missing_cumulative, None),
            None
        );
        let negative = token_usage_notification(json!({"promptTokens": -3}));
        assert_eq!(session_usage_from_token_usage(&negative, None), None);
    }

    #[test]
    fn session_context_usage_maps_occupancy_triple() {
        let notification: NativeSessionContextUsageNotification = serde_json::from_value(json!({
            "sessionId": "session-1",
            "usedTokens": 4096,
            "windowTokens": 200000,
            "viewCursor": "cursor-2",
        }))
        .expect("session/contextUsage decodes");
        let merged = session_usage_from_context_usage(&notification, None).expect("occupancy maps");
        assert_eq!(merged.context_used_tokens, Some(4096));
        assert_eq!(merged.context_window_tokens, Some(200000));
        assert_eq!(merged.cumulative_tokens, None);
        assert_eq!(merged.source.as_deref(), Some("muse:session/contextUsage"));
        // Occupancy merges preserve retained cumulative counters and cost.
        let with_cumulative = session_usage_from_token_usage(
            &token_usage_notification(json!({"promptTokens": 7})),
            Some(&merged),
        )
        .expect("cumulative merges");
        assert_eq!(with_cumulative.context_used_tokens, Some(4096));
        assert_eq!(
            with_cumulative
                .cumulative_tokens
                .as_ref()
                .and_then(|totals| totals.input_tokens),
            Some(7)
        );
        // A repeat contributes nothing new.
        assert_eq!(
            session_usage_from_context_usage(&notification, Some(&with_cumulative)),
            None
        );
    }

    #[test]
    fn session_context_usage_replaces_both_fields() {
        let full: NativeSessionContextUsageNotification = serde_json::from_value(json!({
            "sessionId": "session-1",
            "usedTokens": 10,
            "windowTokens": 200000,
        }))
        .expect("session/contextUsage decodes");
        let merged = session_usage_from_context_usage(&full, None).expect("occupancy maps");
        assert_eq!(merged.context_used_tokens, Some(10));
        assert_eq!(merged.context_window_tokens, Some(200000));
        // A later notification without the window clears it instead of
        // keeping the previous capacity.
        let partial: NativeSessionContextUsageNotification = serde_json::from_value(json!({
            "sessionId": "session-1",
            "usedTokens": 20,
        }))
        .expect("window-less contextUsage decodes");
        let replaced = session_usage_from_context_usage(&partial, Some(&merged))
            .expect("window clears to null");
        assert_eq!(replaced.context_used_tokens, Some(20));
        assert_eq!(replaced.context_window_tokens, None);
        assert_eq!(
            replaced.source.as_deref(),
            Some("muse:session/contextUsage")
        );
        // A repeat of the cleared snapshot contributes nothing new.
        assert_eq!(
            session_usage_from_context_usage(&partial, Some(&replaced)),
            None
        );
    }

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

#[cfg(test)]
mod workflow_schema_tests {
    use super::*;
    use serde_json::json;

    /// Sanitized wave3b `item/updated` revision 2: the child is scheduled AFTER
    /// the parent `turn/completed`. Shapes verbatim, ids redacted.
    fn scheduled_revision() -> serde_json::Value {
        json!({
            "sessionId": "parent-session-sanitized",
            "viewCursor": "v:redacted:14",
            "sourceRange": {
                "stream": {"kind": "session", "id": "parent-session-sanitized"},
                "first": {"id": "workflow-item-sanitized", "sequence": 78},
                "last": {"id": "rev-cursor-sanitized", "sequence": 97},
            },
            "item": {
                "itemId": "workflow-item-sanitized",
                "kind": "workflow",
                "turnId": "parent-turn-sanitized",
                "revision": 2,
                "status": "inProgress",
                "recordedAt": "2026-11-05T09:59:02.775698Z",
                "fallbackText": "Workflow: model-chosen generated workflow",
                "workflowRunId": "workflow-run-sanitized",
                "entryId": "Read marker file",
                "scriptId": "generated.workflow.Read_marker_file",
                "triggerSource": "guidanceAuto",
                "children": [
                    {"childId": "child-sanitized", "attempt": 1, "status": "scheduled"},
                ],
            },
        })
    }

    /// Sanitized wave3b `item/completed` revision 7: explicit terminal plus the
    /// history-composite payloads the observer must never synthesize from.
    fn terminal_revision() -> serde_json::Value {
        json!({
            "sessionId": "parent-session-sanitized",
            "viewCursor": "v:redacted:19",
            "sourceRange": {
                "stream": {"kind": "session", "id": "parent-session-sanitized"},
                "first": {"id": "workflow-item-sanitized", "sequence": 78},
                "last": {"id": "rev-cursor-sanitized", "sequence": 117},
            },
            "item": {
                "itemId": "workflow-item-sanitized",
                "kind": "workflow",
                "turnId": "parent-turn-sanitized",
                "revision": 7,
                "status": "completed",
                "recordedAt": "2026-11-05T09:59:06.631016Z",
                "fallbackText": "Workflow: model-chosen generated workflow",
                "workflowRunId": "workflow-run-sanitized",
                "entryId": "Read marker file",
                "scriptId": "generated.workflow.Read_marker_file",
                "triggerSource": "guidanceAuto",
                "children": [{
                    "childId": "child-sanitized",
                    "attempt": 1,
                    "status": "terminal",
                    "durationMs": 3752,
                    "terminal": "completed",
                    "resultRef": "subagent-result://redacted#0",
                    "label": "Read marker file",
                    "phase": "settled",
                    "usage": {
                        "inputTokens": 10,
                        "outputTokens": 2,
                        "cachedTokens": 4,
                        "cacheWriteTokens": 0,
                        "cacheReadTokens": 4,
                        "reasoningTokens": 1,
                    },
                    "futureChildField": {"nested": true},
                }],
                "message": "workflow-summary-redacted",
                "futureItemField": [1, 2],
            },
            "emittedAtMs": 1793872746634u64,
        })
    }

    #[test]
    fn scheduled_and_terminal_revisions_decode_with_binding_intact() {
        let scheduled: NativeWorkflowItemNotification =
            serde_json::from_value(scheduled_revision()).expect("scheduled rev decodes");
        assert_eq!(scheduled.session_id, "parent-session-sanitized");
        assert_eq!(scheduled.item.item_id, "workflow-item-sanitized");
        assert_eq!(scheduled.item.revision, 2);
        assert_eq!(scheduled.item.status, "inProgress");
        assert_eq!(
            scheduled.item.turn_id.as_deref(),
            Some("parent-turn-sanitized")
        );
        assert_eq!(scheduled.item.workflow_run_id, "workflow-run-sanitized");
        assert!(scheduled.source_range.is_some());
        assert_eq!(scheduled.item.children.len(), 1);
        let child = &scheduled.item.children[0];
        assert_eq!(child.child_id, "child-sanitized");
        assert_eq!(child.attempt, 1);
        assert_eq!(child.status, "scheduled");
        assert_eq!(child.terminal, None);

        let terminal: NativeWorkflowItemNotification =
            serde_json::from_value(terminal_revision()).expect("terminal rev decodes");
        assert_eq!(terminal.item.revision, 7);
        assert_eq!(terminal.item.status, "completed");
        let child = &terminal.item.children[0];
        assert_eq!(child.status, "terminal");
        assert_eq!(child.terminal.as_deref(), Some("completed"));
    }

    #[test]
    fn composites_and_unknown_fields_are_tolerated_never_modelled() {
        // `durationMs`, `resultRef`, `usage`, `label`, `phase`, the item
        // `message` composite and every future field decode without a home:
        // the DTO has nowhere to put them, so no fact can cite them.
        let notification: NativeWorkflowItemNotification =
            serde_json::from_value(terminal_revision()).expect("composite rev decodes");
        let encoded = serde_json::to_value(&notification).expect("DTO re-encodes");
        let text = encoded.to_string();
        for forbidden in [
            "durationMs",
            "resultRef",
            "usage",
            "label",
            "phase",
            "message",
            "futureChildField",
            "futureItemField",
            "subagent-result",
            "workflow-summary-redacted",
        ] {
            assert!(!text.contains(forbidden), "DTO must not retain {forbidden}");
        }
    }

    #[test]
    fn empty_children_and_missing_provenance_decode() {
        // Revision 1 carries no children yet; cursor/range stay optional so a
        // host that omits them still parses.
        let notification: NativeWorkflowItemNotification = serde_json::from_value(json!({
            "sessionId": "parent-session-sanitized",
            "item": {
                "itemId": "workflow-item-sanitized",
                "kind": "workflow",
                "revision": 1,
                "status": "inProgress",
                "workflowRunId": "workflow-run-sanitized",
                "children": [],
            },
        }))
        .expect("childless rev decodes");
        assert!(notification.item.children.is_empty());
        assert_eq!(notification.source_range, None);
        assert_eq!(notification.item.turn_id, None);
    }

    #[test]
    fn child_binding_fields_are_required() {
        for child in [
            json!({"attempt": 1, "status": "scheduled"}),
            json!({"childId": "child-sanitized", "status": "scheduled"}),
            json!({"childId": "child-sanitized", "attempt": 1}),
        ] {
            assert!(
                serde_json::from_value::<NativeWorkflowChild>(child).is_err(),
                "child identity must be complete"
            );
        }
        assert!(
            serde_json::from_value::<NativeWorkflowItemNotification>(json!({
                "sessionId": "parent-session-sanitized",
                "item": {
                    "itemId": "workflow-item-sanitized",
                    "kind": "workflow",
                    "revision": 2,
                    "status": "inProgress",
                    "children": [],
                },
            }))
            .is_err()
        );
    }

    #[test]
    fn workflow_terminal_parses_the_turn_vocabulary_openly() {
        assert_eq!(
            NativeTurnTerminal::parse_workflow_terminal("completed"),
            NativeTurnTerminal::Completed
        );
        assert_eq!(
            NativeTurnTerminal::parse_workflow_terminal("failed"),
            NativeTurnTerminal::Failed
        );
        assert_eq!(
            NativeTurnTerminal::parse_workflow_terminal("cancelled"),
            NativeTurnTerminal::Cancelled
        );
        for unknown in ["", "inProgress", "expired", "COMPLETED", " completed"] {
            assert_eq!(
                NativeTurnTerminal::parse_workflow_terminal(unknown),
                NativeTurnTerminal::Unknown,
                "terminal={unknown}"
            );
        }
    }
}
