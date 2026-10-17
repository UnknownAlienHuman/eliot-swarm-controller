//! Pure command validation and the first supported MSP request, not a facade.
//!
//! Sources at the parent revision 8283a61f:
//! - modules/muse/bridge.mjs:125-172 (receipt/TaskPrompt), 1200-1362 (dispatch);
//! - modules/muse/settings.mjs (one setter, goal verbs, grant-gated MCP config);
//! - modules/muse/results.mjs: result work is delegated to the results owner;
//! - modules/muse/observe.mjs:38-50 (pinned absent durability classification).
//!
//! Public MSP 1.3 schema:
//! <https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/schema/msp/msp.d.ts>
//! Approval/user-input identities: lines 36-48, 120-123, 2064-2182;
//! setters/start/resume: 1304-1405; turns: 1936-1991; goals: 443-494.
//!
//! The caller supplies an independently authenticated Store receipt and facts
//! from the same owned connection, native namespace and binding. It must check
//! freshness again immediately before I/O and durably retain exact intent first.
//! These functions cannot prove those caller obligations or native admission.
//! Requested selectors are retained caller intent, never observed defaults.
//! Native command UUIDv7 and a NEW root's reserved sessionId are caller-supplied
//! intent identities, separate from the host Operation and transport RPC ID.
//! Before first send, the journal must atomically retain the chosen root ID and
//! exact commandId/method/params, including session/start, as pending intent.
//! A lost start ACK stays pending: only explicit same-ID, byte-equivalent
//! reconciliation may resolve it. Conflict is not permission to adopt another
//! root, mint another start, fork or resend a prompt. initialize must complete
//! and initialized must be flushed before a native method is sent.
//!
//! Refresh returns only its FIRST read. Result requests are opaque inputs
//! validated by the results owner, which alone constructs native result reads,
//! verifies family scope and owns selection, cursors, paging and output checks.
//! Reconciliation returns a validated target, NEVER a replay: the journal
//! owner must match the target's binding/generation, original receipt, exact
//! method/params/native command ID, ACK/disposition and current work fences.
//! Refresh needs a real read cursor, subscription, pending inventory and a
//! stable event fence. Its first read is not completion.
//! No Session/PendingCommandSet/GapFiller, checkpoint, scheduling, automatic
//! approval, native execution or Task authority is implemented here.

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fmt};
use swarm_contracts::{
    ModuleReceiptIdentity, RuntimeCommand, TaskDispatchContext, TaskPromptEnvelopeV1,
};

pub const MAX_TASK_PROMPT_BYTES: usize = 1_000_000;
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Closed diagnostics: no input, prompt, configuration or native error text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodeError {
    ReceiptIdentity,
    InvalidInput,
    UnsupportedMethod,
    InvalidNativeOptions,
    InvalidNativeCommandIdentity,
    TaskPromptInvalid,
    NativeIdentityMismatch,
    NativeFactsRequired,
    NativeFactsStale,
    ModelConfigurationUnresolved,
    ModelReadbackMismatch,
    StandingEffortMismatch,
    TurnIdentityMismatch,
    ReplyIdentityMismatch,
    InvalidReply,
    InvalidSettings,
    InvalidGoal,
    SessionMcpNotGranted,
    InvalidMcpServers,
    RecoveryNotPermitted,
    ResultRequestUnvalidated,
    InvalidReconciliationTarget,
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MUSE_COMMAND_{self:?}")
    }
}
impl std::error::Error for EncodeError {}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct RequestedSelectors {
    pub model_id: Option<String>,
    pub provider_id: Option<String>,
    pub reasoning_effort: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelConfiguration {
    Unknown,
    Pending,
    Settled,
}

/// A native read plus the event version sampled again by the caller. No value
/// here is converted into a requested selector. Unknown status is rejected.
#[derive(Clone, Copy)]
pub struct WorkFacts<'a> {
    /// Exact known-root session/read, not session/list or an inferred snapshot.
    pub read_method: &'a str,
    pub session_id: &'a str,
    pub model_id: Option<&'a str>,
    pub provider_id: Option<&'a str>,
    pub status: &'a str,
    pub active_turn_id: Option<&'a str>,
    pub sampled_event_version: u64,
    pub current_event_version: u64,
    pub model_configuration: ModelConfiguration,
    pub standing_effort: Option<&'a str>,
}

/// The current approval/request or userInput/request params, obtained from a
/// live request or a stable pending inventory. Transport RPC IDs stay with msp;
/// they are not approvalId/userInputId/questionId or commandId.
#[derive(Clone, Copy)]
pub struct PendingRequestFacts<'a> {
    pub method: &'a str,
    pub params: &'a Value,
    pub sampled_event_version: u64,
    pub current_event_version: u64,
}

#[derive(Clone, Copy)]
pub struct RecoveryFacts {
    pub durability: SessionDurability,
    pub native_ready: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionDurability {
    Durable,
    Ephemeral,
    Unrecognized,
}

/// Pinned MSP 1.3 / SDK host-death semantics: only an ABSENT member or the
/// exact durable spelling is durable. Null and unknown spellings stay unknown.
pub fn session_durability(initialize_result: &Value) -> SessionDurability {
    let Some(result) = initialize_result.as_object() else {
        return SessionDurability::Unrecognized;
    };
    match result.get("sessionDurability") {
        None => SessionDurability::Durable,
        Some(Value::String(value)) if value == "durable" => SessionDurability::Durable,
        Some(Value::String(value)) if value == "ephemeral" => SessionDurability::Ephemeral,
        _ => SessionDurability::Unrecognized,
    }
}

pub struct EncodeContext<'a> {
    pub expected_receipt: &'a ModuleReceiptIdentity,
    pub worker_boot_id: &'a str,
    /// Existing retained root, never a proposed replacement on a start conflict.
    pub native_root_id: Option<&'a str>,
    /// Caller-chosen fresh session identity, pre-recorded for this agent.open.
    pub fresh_root_session_id: Option<&'a str>,
    /// Exact UUIDv7 reserved for this Operation in the caller's pending ledger.
    /// No derivation from an Operation UUID or JSON-RPC ID occurs here.
    pub native_command_id: Option<&'a str>,
    /// initialize response accepted AND initialized notification flushed.
    pub initialized: bool,
    pub observed_children: &'a [String],
    pub requested: &'a RequestedSelectors,
    pub work: Option<WorkFacts<'a>>,
    pub pending_request: Option<PendingRequestFacts<'a>>,
    pub recovery: Option<RecoveryFacts>,
    /// Temporary opaque handoff until results::ResultRequest is wired here.
    /// Caller precondition: ResultRequest::parse has accepted this exact input.
    /// The results owner must additionally check observed family scope before
    /// I/O. This Value itself supplies neither validation nor native authority.
    pub validated_result_input: Option<&'a Value>,
    /// Exact initialize grant; a requested capability is not a grant.
    pub session_mcp_granted: bool,
}

#[derive(Clone, PartialEq)]
pub struct NativeRequest {
    pub method: &'static str,
    pub params: Map<String, Value>,
}

impl fmt::Debug for NativeRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeRequest")
            .field("method", &self.method)
            .field("params", &"[redacted]")
            .finish()
    }
}

/// Validated source facts, not a task.dispatch admission receipt.
pub struct ValidatedTaskPrompt {
    pub envelope: TaskPromptEnvelopeV1,
    pub context: TaskDispatchContext,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconciliationTarget {
    pub operation_id: String,
    pub input_sha256: String,
}

pub enum CommandEncoding {
    Native {
        request: NativeRequest,
        task_prompt: Option<Box<ValidatedTaskPrompt>>,
        /// Present only on open: retain these explicit route constraints.
        requested: Option<RequestedSelectors>,
    },
    RefreshFirstRead {
        request: NativeRequest,
        session_id: String,
    },
    /// No result codec or native request is owned by this module.
    ResultReadRequired { validated_input: Value },
    /// No native request is safe from this target alone.
    ReconciliationRequired(ReconciliationTarget),
}

impl CommandEncoding {
    pub fn first_request(&self) -> Option<&NativeRequest> {
        match self {
            Self::Native { request, .. } | Self::RefreshFirstRead { request, .. } => Some(request),
            Self::ResultReadRequired { .. } | Self::ReconciliationRequired(_) => None,
        }
    }
}

impl fmt::Debug for CommandEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommandEncoding")
            .field("first_request", &self.first_request())
            .finish_non_exhaustive()
    }
}

fn atom(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
}

fn sha256(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn object(value: &Value) -> Result<&Map<String, Value>, EncodeError> {
    value.as_object().ok_or(EncodeError::InvalidInput)
}

fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str, EncodeError> {
    value[key]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .ok_or(EncodeError::InvalidInput)
}

fn only(value: &Value, fields: &[&str]) -> Result<(), EncodeError> {
    if object(value)?
        .keys()
        .any(|key| !fields.contains(&key.as_str()))
    {
        return Err(EncodeError::InvalidInput);
    }
    Ok(())
}

fn integer(value: &Value, minimum: u64) -> Result<u64, EncodeError> {
    value
        .as_u64()
        .filter(|n| (minimum..=MAX_SAFE_INTEGER).contains(n))
        .ok_or(EncodeError::InvalidInput)
}

fn effort(value: &str) -> bool {
    [
        "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
    ]
    .contains(&value)
}

fn approval_mode(value: &str) -> bool {
    ["allowAll", "promptUnmatched", "onRequest", "denyUnmatched"].contains(&value)
}

fn validate_selectors(selectors: &RequestedSelectors) -> Result<(), EncodeError> {
    if selectors.model_id.as_deref().is_some_and(|s| !atom(s))
        || selectors.provider_id.as_deref().is_some_and(|s| !atom(s))
        || selectors
            .reasoning_effort
            .as_deref()
            .is_some_and(|s| !effort(s))
    {
        return Err(EncodeError::InvalidNativeOptions);
    }
    Ok(())
}

/// Only compare explicit constraints with actual native readback. This does
/// not establish session identity, freshness, readiness or native admission.
pub fn validate_requested_readback(
    requested: &RequestedSelectors,
    observed_model: Option<&str>,
    observed_provider: Option<&str>,
) -> Result<(), EncodeError> {
    validate_selectors(requested)?;
    if requested
        .model_id
        .as_deref()
        .is_some_and(|id| observed_model != Some(id))
        || requested
            .provider_id
            .as_deref()
            .is_some_and(|id| observed_provider != Some(id))
    {
        return Err(EncodeError::ModelReadbackMismatch);
    }
    Ok(())
}

/// MSP SS3.1.1 requires UUIDv7. This validates spelling only; freshness and
/// immutable association with an Operation belong to the pending ledger.
pub fn validate_native_command_id(id: &str) -> Result<(), EncodeError> {
    let bytes = id.as_bytes();
    if bytes.len() != 36
        || bytes.iter().enumerate().any(|(index, byte)| {
            if [8, 13, 18, 23].contains(&index) {
                *byte != b'-'
            } else {
                !byte.is_ascii_hexdigit()
            }
        })
        || bytes[14] != b'7'
        || !matches!(bytes[19], b'8' | b'9' | b'a' | b'b' | b'A' | b'B')
    {
        return Err(EncodeError::InvalidNativeCommandIdentity);
    }
    Ok(())
}

fn validate_receipt(command: &RuntimeCommand, cx: &EncodeContext<'_>) -> Result<(), EncodeError> {
    let receipt = cx.expected_receipt;
    if receipt.validate().is_err()
        || receipt.module_id.as_str() != "muse"
        || !atom(&command.operation_id)
        || !atom(&command.binding_id)
        || !atom(cx.worker_boot_id)
        || command.generation <= 0
        || command.generation as u64 > MAX_SAFE_INTEGER
        || receipt.binding_id != command.binding_id
        || receipt.binding_generation != command.generation
        || receipt.operation_id != command.operation_id
        || command.input_sha256.as_deref() != Some(receipt.input_sha256.as_str())
        || command.route["module_artifact_id"].as_str()
            != Some(receipt.artifact.artifact_id.as_str())
    {
        return Err(EncodeError::ReceiptIdentity);
    }
    object(&command.input)?;
    validate_selectors(cx.requested)
}

fn root<'a>(command: &RuntimeCommand, cx: &'a EncodeContext<'_>) -> Result<&'a str, EncodeError> {
    let id = cx
        .native_root_id
        .filter(|s| atom(s))
        .ok_or(EncodeError::NativeFactsRequired)?;
    if command.native_root_id.as_deref() != Some(id) {
        return Err(EncodeError::NativeIdentityMismatch);
    }
    Ok(id)
}

fn member(cx: &EncodeContext<'_>, session_id: &str) -> bool {
    atom(session_id)
        && (cx.native_root_id == Some(session_id)
            || cx.observed_children.iter().any(|id| id == session_id))
}

fn work<'a>(
    cx: &EncodeContext<'a>,
    root: &str,
    allow_not_loaded: bool,
) -> Result<WorkFacts<'a>, EncodeError> {
    let facts = cx.work.ok_or(EncodeError::NativeFactsRequired)?;
    if facts.read_method != "session/read" {
        return Err(EncodeError::NativeFactsRequired);
    }
    if facts.session_id != root {
        return Err(EncodeError::NativeIdentityMismatch);
    }
    if facts.sampled_event_version != facts.current_event_version {
        return Err(EncodeError::NativeFactsStale);
    }
    if !matches!(facts.status, "idle" | "running" | "notLoaded")
        || facts.active_turn_id.is_some_and(|s| !atom(s))
        || facts.model_id.is_some_and(|s| !atom(s))
        || facts.provider_id.is_some_and(|s| !atom(s))
        || facts.status == "notLoaded" && !allow_not_loaded
        || facts.status == "idle" && facts.active_turn_id.is_some()
        || facts.status == "running" && facts.active_turn_id.is_none()
    {
        return Err(EncodeError::NativeFactsRequired);
    }
    if facts.model_configuration != ModelConfiguration::Settled {
        return Err(EncodeError::ModelConfigurationUnresolved);
    }
    validate_requested_readback(cx.requested, facts.model_id, facts.provider_id)?;
    Ok(facts)
}

pub fn validate_task_prompt(
    command: &RuntimeCommand,
    cx: &EncodeContext<'_>,
) -> Result<ValidatedTaskPrompt, EncodeError> {
    validate_receipt(command, cx)?;
    if command.method != "task.dispatch" {
        return Err(EncodeError::TaskPromptInvalid);
    }
    let envelope: TaskPromptEnvelopeV1 =
        serde_json::from_value(command.input["task_prompt"].clone())
            .map_err(|_| EncodeError::TaskPromptInvalid)?;
    let context: TaskDispatchContext =
        serde_json::from_value(command.input["task_dispatch_context"].clone())
            .map_err(|_| EncodeError::TaskPromptInvalid)?;
    let source = text(&command.input, "text").map_err(|_| EncodeError::TaskPromptInvalid)?;
    if envelope.validate_shape().is_err()
        || context.validate().is_err()
        || !atom(&envelope.task_id)
        || !atom(&envelope.attempt_id)
        || envelope.prompt.len() > MAX_TASK_PROMPT_BYTES
        || envelope.task_revision as u64 > MAX_SAFE_INTEGER
        || context.source_text_bytes > MAX_SAFE_INTEGER
        || envelope.prompt_sha256 != sha256(&envelope.prompt)
        || context.operation_id != command.operation_id
        || context.binding_id != command.binding_id
        || context.binding_generation != command.generation
        || context.worker_boot_id != cx.worker_boot_id
        || context.source_text_bytes != source.len() as u64
        || context.source_text_sha256 != sha256(source)
        || context.attempt_id != envelope.attempt_id
        || context.task_id != envelope.task_id
        || context.task_revision != envelope.task_revision
        || context.task_snapshot_sha256 != envelope.task_snapshot_sha256
        || command
            .input
            .get("attempt_id")
            .is_some_and(|id| id.as_str() != Some(context.attempt_id.as_str()))
        || !atom(&context.worker_boot_id)
    {
        return Err(EncodeError::TaskPromptInvalid);
    }
    Ok(ValidatedTaskPrompt { envelope, context })
}

fn native(
    cx: &EncodeContext<'_>,
    method: &'static str,
    mut params: Map<String, Value>,
    task_prompt: Option<ValidatedTaskPrompt>,
    requested: Option<RequestedSelectors>,
) -> Result<CommandEncoding, EncodeError> {
    if !cx.initialized {
        return Err(EncodeError::NativeFactsRequired);
    }
    let id = cx
        .native_command_id
        .ok_or(EncodeError::NativeFactsRequired)?;
    validate_native_command_id(id)?;
    if params
        .get("commandId")
        .is_some_and(|value| value.as_str() != Some(id))
    {
        return Err(EncodeError::InvalidNativeCommandIdentity);
    }
    params.insert("commandId".into(), json!(id));
    Ok(CommandEncoding::Native {
        request: NativeRequest { method, params },
        task_prompt: task_prompt.map(Box::new),
        requested,
    })
}

/// Validate all ten advertised host methods. Native variants are exact request
/// encodings, not permissions to execute; refresh cannot claim completion and
/// result work is delegated. No native server request is auto-approved.
pub fn encode(
    command: &RuntimeCommand,
    cx: &EncodeContext<'_>,
) -> Result<CommandEncoding, EncodeError> {
    validate_receipt(command, cx)?;
    if command.method != "task.dispatch"
        && (command.input.get("task_prompt").is_some()
            || command.input.get("task_dispatch_context").is_some())
    {
        return Err(EncodeError::TaskPromptInvalid);
    }
    if command.method == "agent.open" {
        return open(command, cx);
    }
    if command.method == "agent.reconcile" {
        // A lost session/start ACK has no confirmed root yet. This is only a
        // journal target: the journal must resolve that exact pending start,
        // not adopt the proposed root or generate another start here.
        if command.native_root_id.as_deref() != cx.native_root_id {
            return Err(EncodeError::NativeIdentityMismatch);
        }
        if cx.native_root_id.is_some_and(|id| !atom(id)) {
            return Err(EncodeError::NativeFactsRequired);
        }
        let target = text(&command.input, "operation_id")?;
        let hash = command
            .target_input_sha256
            .as_deref()
            .filter(|s| digest(s))
            .ok_or(EncodeError::InvalidReconciliationTarget)?;
        if !atom(target) || target == command.operation_id {
            return Err(EncodeError::InvalidReconciliationTarget);
        }
        return Ok(CommandEncoding::ReconciliationRequired(
            ReconciliationTarget {
                operation_id: target.to_owned(),
                input_sha256: hash.to_owned(),
            },
        ));
    }
    let root = root(command, cx)?;
    let p = &command.input;
    match command.method.as_str() {
        "agent.send" | "task.dispatch" => {
            let facts = work(cx, root, false)?;
            let task_prompt = if command.method == "task.dispatch" {
                Some(validate_task_prompt(command, cx)?)
            } else {
                None
            };
            let prompt = match task_prompt.as_ref() {
                Some(task) => task.envelope.prompt.as_str(),
                None => text(p, "text")?,
            };
            let mut params = json!({"sessionId":root,"input":[{"type":"text","text":prompt}]})
                .as_object()
                .expect("object literal")
                .clone();
            if let Some(effort) = &cx.requested.reasoning_effort {
                params.insert("reasoningEffort".into(), json!(effort));
            }
            // bridge.mjs:1309/1315 and the host schema permit omission and
            // explicitly choose queue; this is not a guessed native selector.
            let delivery = match p.get("delivery") {
                None => "next_turn",
                Some(Value::String(value)) => value.as_str(),
                _ => return Err(EncodeError::InvalidInput),
            };
            let method = match delivery {
                "next_turn" => {
                    if p.get("expected_turn_id").is_some() {
                        return Err(EncodeError::TurnIdentityMismatch);
                    }
                    params.insert("ifBusy".into(), json!("queue"));
                    "turn/start"
                }
                "steer" => {
                    let expected = text(p, "expected_turn_id")?;
                    if facts.status != "running" || facts.active_turn_id != Some(expected) {
                        return Err(EncodeError::TurnIdentityMismatch);
                    }
                    params.insert("expectedTurnId".into(), json!(expected));
                    "turn/steer"
                }
                _ => return Err(EncodeError::InvalidInput),
            };
            native(cx, method, params, task_prompt, None)
        }
        "agent.reply" => reply(command, cx),
        "agent.configure" => configure(command, cx, root),
        "agent.goal" => goal(command, cx, root),
        "agent.recover" => {
            let recovery = cx.recovery.ok_or(EncodeError::NativeFactsRequired)?;
            if recovery.durability == SessionDurability::Unrecognized {
                return Err(EncodeError::NativeFactsRequired);
            }
            if recovery.durability != SessionDurability::Durable
                || recovery.native_ready
                || text(p, "expected_boot_id")? != cx.worker_boot_id
            {
                return Err(EncodeError::RecoveryNotPermitted);
            }
            work(cx, root, true)?;
            native(
                cx,
                "session/resume",
                json!({"sessionId":root,"excludeItems":true})
                    .as_object()
                    .expect("object literal")
                    .clone(),
                None,
                None,
            )
        }
        "agent.refresh" => {
            if !cx.initialized {
                return Err(EncodeError::NativeFactsRequired);
            }
            let target = match p.get("session_id") {
                None => root,
                Some(_) => text(p, "session_id")?,
            };
            if !member(cx, target) {
                return Err(EncodeError::NativeIdentityMismatch);
            }
            Ok(CommandEncoding::RefreshFirstRead {
                request: NativeRequest {
                    method: "session/read",
                    params: json!({"sessionId":target,"excludeItems":true})
                        .as_object()
                        .expect("object literal")
                        .clone(),
                },
                session_id: target.to_owned(),
            })
        }
        "agent.result" => {
            let validated = cx
                .validated_result_input
                .ok_or(EncodeError::ResultRequestUnvalidated)?;
            if validated != p {
                return Err(EncodeError::ResultRequestUnvalidated);
            }
            Ok(CommandEncoding::ResultReadRequired {
                validated_input: validated.clone(),
            })
        }
        _ => Err(EncodeError::UnsupportedMethod),
    }
}

fn absolute(path: &str) -> bool {
    let b = path.as_bytes();
    path.starts_with('/')
        || b.len() >= 3
            && b[0].is_ascii_alphabetic()
            && b[1] == b':'
            && matches!(b[2], b'/' | b'\\')
        || path.strip_prefix("\\\\").is_some_and(|tail| {
            let mut parts = tail.split('\\');
            parts.next().is_some_and(|s| !s.is_empty())
                && parts.next().is_some_and(|s| !s.is_empty())
        })
}

fn open(command: &RuntimeCommand, cx: &EncodeContext<'_>) -> Result<CommandEncoding, EncodeError> {
    // Opening does not select or replace a retained root. Recovery owns that path.
    if cx.native_root_id.is_some() || command.native_root_id.is_some() {
        return Err(EncodeError::NativeIdentityMismatch);
    }
    let new_root = cx
        .fresh_root_session_id
        .ok_or(EncodeError::NativeFactsRequired)?;
    // The pin types sessionId as an exact string identity; only commandId is
    // explicitly constrained to UUIDv7. Freshness is the caller's reservation.
    if !atom(new_root) || cx.observed_children.iter().any(|id| id == new_root) {
        return Err(EncodeError::NativeIdentityMismatch);
    }
    let options = &command.route["native_options"];
    object(options).map_err(|_| EncodeError::InvalidNativeOptions)?;
    let workspace =
        text(options, "workspaceRoot").map_err(|_| EncodeError::InvalidNativeOptions)?;
    if !absolute(workspace) || workspace.chars().any(char::is_control) {
        return Err(EncodeError::InvalidNativeOptions);
    }
    let optional = |key: &str, null_is_omitted: bool| -> Result<Option<String>, EncodeError> {
        match options.get(key) {
            None => Ok(None),
            Some(Value::Null) if null_is_omitted => Ok(None),
            Some(Value::String(s)) if atom(s) => Ok(Some(s.clone())),
            _ => Err(EncodeError::InvalidNativeOptions),
        }
    };
    let requested = RequestedSelectors {
        model_id: optional("modelId", false)?,
        provider_id: optional("providerId", true)?,
        reasoning_effort: optional("reasoningEffort", false)?,
    };
    validate_selectors(&requested)?;
    let mut params = json!({"workspaceRoot":workspace,"sessionId":new_root})
        .as_object()
        .expect("object literal")
        .clone();
    for (key, value) in [
        ("modelId", &requested.model_id),
        ("providerId", &requested.provider_id),
    ] {
        if let Some(value) = value {
            params.insert(key.into(), json!(value));
        }
    }
    if let Some(mode) = options.get("approvalMode") {
        if !mode.is_null() && !mode.as_str().is_some_and(approval_mode) {
            return Err(EncodeError::InvalidNativeOptions);
        }
        params.insert("approvalMode".into(), mode.clone());
    }
    if let Some(servers) = options.get("mcpServers") {
        if !cx.session_mcp_granted {
            return Err(EncodeError::SessionMcpNotGranted);
        }
        validate_mcp_servers(servers)?;
        params.insert("config".into(), json!({"mcpServers":servers}));
    }
    native(cx, "session/start", params, None, Some(requested))
}

fn configure(
    command: &RuntimeCommand,
    cx: &EncodeContext<'_>,
    root: &str,
) -> Result<CommandEncoding, EncodeError> {
    let settings = &command.input["settings"];
    let values = object(settings).map_err(|_| EncodeError::InvalidSettings)?;
    if values.len() != 1 {
        return Err(EncodeError::InvalidSettings);
    }
    let (key, desired) = values.iter().next().expect("one setting");
    let mut params = json!({"sessionId":root})
        .as_object()
        .expect("object literal")
        .clone();
    let method = match key.as_str() {
        "reasoningEffort" if desired.as_str().is_some_and(effort) => {
            params.insert(key.clone(), desired.clone());
            "session/setReasoningEffort"
        }
        "approvalMode" if desired.as_str().is_some_and(approval_mode) => {
            params.insert("mode".into(), desired.clone());
            "session/setApprovalMode"
        }
        "model" => {
            only(desired, &["modelId", "providerId"]).map_err(|_| EncodeError::InvalidSettings)?;
            if !atom(text(desired, "modelId").map_err(|_| EncodeError::InvalidSettings)?)
                || desired
                    .get("providerId")
                    .is_some_and(|v| !v.as_str().is_some_and(atom))
            {
                return Err(EncodeError::InvalidSettings);
            }
            params.insert("model".into(), desired.clone());
            "session/setModel"
        }
        _ => return Err(EncodeError::InvalidSettings),
    };
    native(cx, method, params, None, None)
}

fn goal(
    command: &RuntimeCommand,
    cx: &EncodeContext<'_>,
    root: &str,
) -> Result<CommandEncoding, EncodeError> {
    let action = text(&command.input, "action").map_err(|_| EncodeError::InvalidGoal)?;
    let method = match action {
        "set" => "goal/set",
        "edit" => "goal/edit",
        "pause" => "goal/pause",
        "resume" => "goal/resume",
        "clear" => "goal/clear",
        _ => return Err(EncodeError::InvalidGoal),
    };
    if matches!(action, "set" | "edit" | "resume") {
        let facts = work(cx, root, false)?;
        if cx
            .requested
            .reasoning_effort
            .as_deref()
            .is_some_and(|s| facts.standing_effort != Some(s))
        {
            return Err(EncodeError::StandingEffortMismatch);
        }
    }
    let mut params = json!({"sessionId":root})
        .as_object()
        .expect("object literal")
        .clone();
    if matches!(action, "set" | "edit") {
        // MSP SS3.18 requires the trimmed objective as payload. The pending
        // ledger must retain these final params before the first write.
        params.insert(
            "objective".into(),
            json!(
                text(&command.input, "objective")
                    .map_err(|_| EncodeError::InvalidGoal)?
                    .trim()
            ),
        );
    } else if command.input.get("objective").is_some() {
        return Err(EncodeError::InvalidGoal);
    }
    native(cx, method, params, None, None)
}

fn validate_mcp_servers(value: &Value) -> Result<(), EncodeError> {
    let validate = || -> Result<(), EncodeError> {
        for server in object(value)?.values() {
            let fields: &[&str] = match text(server, "transport")? {
                "stdio" => &["transport", "command", "args", "env", "framing", "mode"],
                "streamableHttp" => &["transport", "url", "headers", "mode"],
                _ => return Err(EncodeError::InvalidInput),
            };
            only(server, fields)?;
            if server
                .get("mode")
                .is_some_and(|v| !matches!(v.as_str(), Some("required" | "optional")))
            {
                return Err(EncodeError::InvalidInput);
            }
            if server["transport"] == "stdio" {
                text(server, "command")?;
                if server.get("args").is_some_and(|v| {
                    v.as_array()
                        .is_none_or(|a| a.iter().any(|s| !s.is_string()))
                }) || server.get("framing").is_some_and(|v| {
                    !matches!(
                        v.as_str(),
                        Some("auto" | "contentLength" | "lineDelimitedJson")
                    )
                }) {
                    return Err(EncodeError::InvalidInput);
                }
            } else {
                text(server, "url")?;
            }
            for key in ["env", "headers"] {
                if let Some(values) = server.get(key)
                    && object(values)?.values().any(|s| !s.is_string())
                {
                    return Err(EncodeError::InvalidInput);
                }
            }
        }
        Ok(())
    };
    validate().map_err(|_| EncodeError::InvalidMcpServers)
}

fn reply(command: &RuntimeCommand, cx: &EncodeContext<'_>) -> Result<CommandEncoding, EncodeError> {
    let facts = cx.pending_request.ok_or(EncodeError::NativeFactsRequired)?;
    if facts.sampled_event_version != facts.current_event_version {
        return Err(EncodeError::NativeFactsStale);
    }
    let r = &command.input["reply"];
    only(r, &["method", "params"]).map_err(|_| EncodeError::InvalidReply)?;
    let method = text(r, "method").map_err(|_| EncodeError::InvalidReply)?;
    let params = &r["params"];
    let session = text(params, "sessionId").map_err(|_| EncodeError::InvalidReply)?;
    if !member(cx, session) || facts.params["sessionId"] != session {
        return Err(EncodeError::ReplyIdentityMismatch);
    }
    let method = match method {
        "approval/decide" if facts.method == "approval/request" => {
            validate_approval(params, facts.params)?;
            "approval/decide"
        }
        "userInput/answer" | "userInput/cancel" | "userInput/clarify"
            if facts.method == "userInput/request" =>
        {
            if text(params, "userInputId").map_err(|_| EncodeError::InvalidReply)?
                != text(facts.params, "userInputId").map_err(|_| EncodeError::InvalidReply)?
            {
                return Err(EncodeError::ReplyIdentityMismatch);
            }
            match method {
                "userInput/answer" => {
                    validate_answers(params, facts.params)?;
                    "userInput/answer"
                }
                "userInput/cancel" => {
                    only(params, &["sessionId", "userInputId", "reason", "commandId"])
                        .map_err(|_| EncodeError::InvalidReply)?;
                    if params.get("reason").is_some_and(|v| !v.is_string()) {
                        return Err(EncodeError::InvalidReply);
                    }
                    "userInput/cancel"
                }
                _ => {
                    only(
                        params,
                        &["sessionId", "userInputId", "clarification", "commandId"],
                    )
                    .map_err(|_| EncodeError::InvalidReply)?;
                    let clarification = &params["clarification"];
                    only(clarification, &["format", "content"])
                        .map_err(|_| EncodeError::InvalidReply)?;
                    if clarification["format"] != "text" || !short_text(&clarification["content"]) {
                        return Err(EncodeError::InvalidReply);
                    }
                    "userInput/clarify"
                }
            }
        }
        _ => return Err(EncodeError::InvalidReply),
    };
    native(cx, method, object(params)?.clone(), None, None)
}

fn validate_approval(params: &Value, pending: &Value) -> Result<(), EncodeError> {
    let validate = || -> Result<(), EncodeError> {
        only(
            params,
            &[
                "sessionId",
                "approvalId",
                "choiceId",
                "requirementId",
                "feedback",
                "commandId",
            ],
        )?;
        let approval = text(params, "approvalId")?;
        if pending["approvalId"] != approval
            || params["requirementId"] != pending["currentRequirementId"]
        {
            return Err(EncodeError::ReplyIdentityMismatch);
        }
        only(&params["requirementId"], &["approvalId", "sourceIndex"])?;
        if params["requirementId"]["approvalId"] != approval {
            return Err(EncodeError::ReplyIdentityMismatch);
        }
        integer(&params["requirementId"]["sourceIndex"], 0)?;
        let choice = text(params, "choiceId")?;
        let choices = pending["availableChoices"]
            .as_array()
            .ok_or(EncodeError::InvalidReply)?;
        let matching = choices
            .iter()
            .filter(|c| c["choiceId"] == choice)
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            return Err(EncodeError::ReplyIdentityMismatch);
        }
        if let Some(feedback) = params.get("feedback").filter(|v| !v.is_null())
            && (!feedback.is_string() || matching[0]["acceptsFeedback"] != true)
        {
            return Err(EncodeError::InvalidReply);
        }
        Ok(())
    };
    validate().map_err(|e| {
        if e == EncodeError::ReplyIdentityMismatch {
            e
        } else {
            EncodeError::InvalidReply
        }
    })
}

fn short_text(value: &Value) -> bool {
    value.as_str().is_some_and(|s| s.chars().count() <= 500)
}

fn validate_answers(params: &Value, pending: &Value) -> Result<(), EncodeError> {
    let validate = || -> Result<(), EncodeError> {
        only(
            params,
            &["sessionId", "userInputId", "answers", "commandId"],
        )?;
        let questions = pending["questions"]
            .as_array()
            .filter(|a| !a.is_empty())
            .ok_or(EncodeError::InvalidReply)?;
        let answers = params["answers"]
            .as_array()
            .ok_or(EncodeError::InvalidReply)?;
        if answers.len() != questions.len() {
            return Err(EncodeError::InvalidReply);
        }
        let mut seen = BTreeSet::new();
        for question in questions {
            let id = text(question, "id")?;
            if !seen.insert(id) {
                return Err(EncodeError::InvalidReply);
            }
            let matches = answers
                .iter()
                .filter(|a| a["questionId"] == id)
                .collect::<Vec<_>>();
            if matches.len() != 1 {
                return Err(EncodeError::ReplyIdentityMismatch);
            }
            let answer = matches[0];
            only(
                answer,
                &[
                    "questionId",
                    "selectedLabel",
                    "selectedLabels",
                    "freeText",
                    "note",
                ],
            )?;
            if ["selectedLabel", "selectedLabels", "freeText"]
                .iter()
                .filter(|k| answer.get(**k).is_some())
                .count()
                != 1
                || answer.get("note").is_some_and(|v| !short_text(v))
            {
                return Err(EncodeError::InvalidReply);
            }
            let options = question["options"]
                .as_array()
                .ok_or(EncodeError::InvalidReply)?;
            let labels = options
                .iter()
                .map(|v| text(v, "label"))
                .collect::<Result<BTreeSet<_>, _>>()?;
            if labels.len() != options.len() {
                return Err(EncodeError::InvalidReply);
            }
            let mode = text(&question["selection"], "mode")?;
            if !matches!(mode, "single" | "multiple") {
                return Err(EncodeError::InvalidReply);
            }
            if let Some(free) = answer.get("freeText") {
                if !short_text(free) {
                    return Err(EncodeError::InvalidReply);
                }
            } else if mode == "single" {
                if !answer["selectedLabel"]
                    .as_str()
                    .is_some_and(|s| labels.contains(s))
                {
                    return Err(EncodeError::InvalidReply);
                }
            } else {
                // Do not invent effective selection bounds from an incomplete
                // native question. The pending-fact producer must supply them.
                let min = integer(&question["selection"]["minSelections"], 0)?;
                let max = integer(&question["selection"]["maxSelections"], 0)?;
                let selected = answer["selectedLabels"]
                    .as_array()
                    .ok_or(EncodeError::InvalidReply)?;
                let mut unique = BTreeSet::new();
                if min > max || !(min..=max).contains(&(selected.len() as u64)) {
                    return Err(EncodeError::InvalidReply);
                }
                for selected in selected {
                    let label = selected.as_str().ok_or(EncodeError::InvalidReply)?;
                    if !labels.contains(label) || !unique.insert(label) {
                        return Err(EncodeError::InvalidReply);
                    }
                }
            }
        }
        Ok(())
    };
    validate().map_err(|e| {
        if e == EncodeError::ReplyIdentityMismatch {
            e
        } else {
            EncodeError::InvalidReply
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT: &str = "ses_root_fixture";
    const OPERATION: &str = "3f6b8f2e-9c41-4d7a-a5e2-1b0c9d8e7f60";
    const BOOT: &str = "fixture-boot-token";
    const NATIVE_COMMAND: &str = "019e7044-8800-73ed-9497-0f0c6eaca68d";
    const FRESH_ROOT: &str = "019e7044-8801-7000-8000-000000000001";
    const SHA_ABC: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    struct Fixture {
        command: RuntimeCommand,
        receipt: ModuleReceiptIdentity,
        requested: RequestedSelectors,
        children: Vec<String>,
    }

    impl Fixture {
        fn new(method: &str, input: Value) -> Self {
            // This digest represents the original Store request, not the later
            // enriched input. The independent receipt is supplied separately.
            let receipt: ModuleReceiptIdentity = serde_json::from_value(json!({
                "schema_version":1,"module_id":"muse",
                "artifact":{"artifact_id":"muse-sdk-1.3.0-bridge.13","version":"13","build_id":null},
                "protocol":{"major":1,"minor":0},"binding_id":"fixture-binding",
                "binding_generation":1,"operation_id":OPERATION,"input_sha256":"a".repeat(64)
            })).unwrap();
            let command = RuntimeCommand {
                operation_id: OPERATION.into(),
                method: method.into(),
                created_at_ms: 1_780_000_000_000,
                binding_id: "fixture-binding".into(),
                generation: 1,
                native_root_id: Some(ROOT.into()),
                route: json!({"runtime":"muse","module_artifact_id":"muse-sdk-1.3.0-bridge.13"}),
                input,
                input_sha256: Some(receipt.input_sha256.clone()),
                target_input_sha256: None,
            };
            Self {
                command,
                receipt,
                requested: RequestedSelectors::default(),
                children: vec!["ses_child".into()],
            }
        }

        fn context(&self) -> EncodeContext<'_> {
            EncodeContext {
                expected_receipt: &self.receipt,
                worker_boot_id: BOOT,
                native_root_id: Some(ROOT),
                fresh_root_session_id: (self.command.method == "agent.open").then_some(FRESH_ROOT),
                native_command_id: Some(NATIVE_COMMAND),
                initialized: true,
                observed_children: &self.children,
                requested: &self.requested,
                work: Some(WorkFacts {
                    read_method: "session/read",
                    session_id: ROOT,
                    model_id: Some("observed-native-model"),
                    provider_id: Some("observed-provider"),
                    status: "idle",
                    active_turn_id: None,
                    sampled_event_version: 7,
                    current_event_version: 7,
                    model_configuration: ModelConfiguration::Settled,
                    standing_effort: None,
                }),
                pending_request: None,
                recovery: None,
                validated_result_input: None,
                session_mcp_granted: false,
            }
        }

        fn open(options: Value) -> Self {
            let mut fixture = Self::new("agent.open", json!({}));
            fixture.command.native_root_id = None;
            fixture.command.route["native_options"] = options;
            fixture
        }

        fn dispatch() -> Self {
            let mut fixture = Self::new("task.dispatch", json!({"text":"abc"}));
            fixture.command.input["task_prompt"] = json!({
                "schema_id":"swarm.task_prompt","schema_version":1,"task_id":"task-one",
                "task_revision":2,"attempt_id":"attempt-one","task_snapshot_sha256":"b".repeat(64),
                "prompt_sha256":SHA_ABC,"prompt_bytes":3,"prompt":"abc"
            });
            fixture.command.input["task_dispatch_context"] = json!({
                "schema_version":1,"operation_id":OPERATION,"binding_id":"fixture-binding",
                "binding_generation":1,"worker_boot_id":BOOT,"attempt_id":"attempt-one",
                "task_id":"task-one","task_revision":2,"task_snapshot_sha256":"b".repeat(64),
                "source_text_sha256":SHA_ABC,"source_text_bytes":3
            });
            fixture
        }
    }

    fn request(encoded: CommandEncoding) -> NativeRequest {
        encoded
            .first_request()
            .expect("supported first request")
            .clone()
    }

    fn approval() -> Value {
        json!({"sessionId":ROOT,"approvalId":"approval-one",
            "currentRequirementId":{"approvalId":"approval-one","sourceIndex":2},
            "availableChoices":[{"choiceId":"deny-once","acceptsFeedback":true},
                {"choiceId":"allow-once","acceptsFeedback":false}]})
    }

    fn approval_reply() -> Value {
        json!({"reply":{"method":"approval/decide","params":{"sessionId":ROOT,
            "approvalId":"approval-one","choiceId":"deny-once",
            "requirementId":{"approvalId":"approval-one","sourceIndex":2},"feedback":"explain"}}})
    }

    fn pending<'a>(method: &'a str, params: &'a Value) -> PendingRequestFacts<'a> {
        PendingRequestFacts {
            method,
            params,
            sampled_event_version: 4,
            current_event_version: 4,
        }
    }

    #[test]
    fn muse_commands_native_id_is_explicit_uuid_v7_not_operation_or_rpc_id() {
        // Retained UUIDv7 vector from checkpoint-roundtrip.json. The encoder
        // accepts the ledger's ID; it does not mint it from the UUIDv4 Operation.
        assert_eq!(validate_native_command_id(NATIVE_COMMAND), Ok(()));
        let fixture = Fixture::new("agent.send", json!({"text":"input"}));
        assert_eq!(
            request(encode(&fixture.command, &fixture.context()).unwrap()).params["commandId"],
            NATIVE_COMMAND
        );
        for id in [OPERATION, "0", "", "019e7044-8800-73ed-1497-0f0c6eaca68d"] {
            let mut cx = fixture.context();
            cx.native_command_id = Some(id);
            assert_eq!(
                encode(&fixture.command, &cx).unwrap_err(),
                EncodeError::InvalidNativeCommandIdentity
            );
        }
    }

    #[test]
    fn muse_commands_open_requires_reserved_root_and_completed_handshake() {
        let fixture = Fixture::open(json!({"workspaceRoot":"/work"}));
        let mut cx = fixture.context();
        cx.native_root_id = None;
        cx.fresh_root_session_id = None;
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::NativeFactsRequired
        );
        cx.fresh_root_session_id = Some(FRESH_ROOT);
        cx.initialized = false;
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::NativeFactsRequired
        );
        cx.initialized = true;
        cx.native_command_id = None;
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::NativeFactsRequired
        );
        cx.native_command_id = Some(NATIVE_COMMAND);
        let intent = request(encode(&fixture.command, &cx).unwrap());
        assert_eq!(intent.method, "session/start");
        assert_eq!(intent.params["sessionId"], FRESH_ROOT);
        assert_eq!(intent.params["commandId"], NATIVE_COMMAND);
        // This encodes stable intent for the journal; it performs no send.
        assert_eq!(request(encode(&fixture.command, &cx).unwrap()), intent);
        cx.native_root_id = Some("foreign-existing-root");
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::NativeIdentityMismatch
        );
    }

    #[test]
    fn muse_commands_durability_uses_pinned_absence_rule_without_unknown_fallback() {
        assert_eq!(session_durability(&json!({})), SessionDurability::Durable);
        assert_eq!(
            session_durability(&json!({"sessionDurability":"durable"})),
            SessionDurability::Durable
        );
        assert_eq!(
            session_durability(&json!({"sessionDurability":"ephemeral"})),
            SessionDurability::Ephemeral
        );
        for value in [
            json!({"sessionDurability":null}),
            json!({"sessionDurability":"future"}),
            Value::Null,
        ] {
            assert_eq!(session_durability(&value), SessionDurability::Unrecognized);
        }
    }

    #[test]
    fn muse_commands_receipt_scope_and_claimed_original_digest_are_exact() {
        for field in ["binding", "generation", "operation", "digest", "artifact"] {
            let mut fixture =
                Fixture::new("agent.send", json!({"delivery":"next_turn","text":"hello"}));
            match field {
                "binding" => fixture.command.binding_id = "foreign".into(),
                "generation" => fixture.command.generation = 2,
                "operation" => fixture.command.operation_id = "other-operation".into(),
                "digest" => fixture.command.input_sha256 = Some("b".repeat(64)),
                _ => fixture.command.route["module_artifact_id"] = json!("foreign-artifact"),
            }
            assert_eq!(
                encode(&fixture.command, &fixture.context()).unwrap_err(),
                EncodeError::ReceiptIdentity
            );
        }
        let mut fixture =
            Fixture::new("agent.send", json!({"delivery":"next_turn","text":"hello"}));
        fixture.receipt.protocol.minor = 1;
        assert_eq!(
            encode(&fixture.command, &fixture.context()).unwrap_err(),
            EncodeError::ReceiptIdentity
        );
    }

    #[test]
    fn muse_commands_omission_never_promotes_observed_defaults() {
        let fixture = Fixture::new(
            "agent.send",
            json!({"delivery":"next_turn","text":"  literal input  "}),
        );
        let sent = request(encode(&fixture.command, &fixture.context()).unwrap());
        assert_eq!(sent.method, "turn/start");
        assert_eq!(
            sent.params["input"],
            json!([{"type":"text","text":"  literal input  "}])
        );
        assert_eq!(sent.params["ifBusy"], "queue");
        for key in ["modelId", "providerId", "reasoningEffort", "expectedTurnId"] {
            assert!(!sent.params.contains_key(key));
        }
        let fixture = Fixture::open(json!({"workspaceRoot":"C:\\work","providerId":null}));
        let mut cx = fixture.context();
        cx.native_root_id = None;
        let opened = encode(&fixture.command, &cx).unwrap();
        let sent = opened.first_request().unwrap();
        assert_eq!(sent.method, "session/start");
        assert_eq!(sent.params.len(), 3); // workspaceRoot, reserved sessionId and commandId.
        assert_eq!(sent.params["sessionId"], FRESH_ROOT);
        match opened {
            CommandEncoding::Native {
                requested: Some(selection),
                ..
            } => assert!(selection == RequestedSelectors::default()),
            _ => panic!("open retains explicit selector omission"),
        }
    }

    #[test]
    fn muse_commands_explicit_selectors_and_unclamped_effort_remain_requested() {
        let fixture = Fixture::open(
            json!({"workspaceRoot":"/workspace","modelId":"explicit-model",
            "providerId":"explicit-provider","reasoningEffort":"ultra"}),
        );
        let mut cx = fixture.context();
        cx.native_root_id = None;
        let opened = encode(&fixture.command, &cx).unwrap();
        assert_eq!(
            opened.first_request().unwrap().params["modelId"],
            "explicit-model"
        );
        assert_eq!(
            opened.first_request().unwrap().params["providerId"],
            "explicit-provider"
        );
        assert!(
            !opened
                .first_request()
                .unwrap()
                .params
                .contains_key("reasoningEffort")
        );
        let CommandEncoding::Native {
            requested: Some(requested),
            ..
        } = opened
        else {
            panic!("selectors")
        };
        assert_eq!(requested.reasoning_effort.as_deref(), Some("ultra"));
        assert!(
            validate_requested_readback(
                &requested,
                Some("explicit-model"),
                Some("explicit-provider")
            )
            .is_ok()
        );
        assert_eq!(
            validate_requested_readback(
                &requested,
                Some("explicit-model"),
                Some("different-provider")
            ),
            Err(EncodeError::ModelReadbackMismatch)
        );
        let mut fixture =
            Fixture::new("agent.send", json!({"delivery":"next_turn","text":"input"}));
        fixture.requested.reasoning_effort = Some("ultra".into());
        assert_eq!(
            request(encode(&fixture.command, &fixture.context()).unwrap()).params["reasoningEffort"],
            "ultra"
        );
        fixture.requested.model_id = Some("different-from-readback".into());
        assert_eq!(
            encode(&fixture.command, &fixture.context()).unwrap_err(),
            EncodeError::ModelReadbackMismatch
        );
    }

    #[test]
    fn muse_commands_only_schema_nullable_selectors_mean_omission() {
        for options in [
            json!({"workspaceRoot":"/work","modelId":null}),
            json!({"workspaceRoot":"/work","reasoningEffort":null}),
            json!({"workspaceRoot":"/work","reasoningEffort":"super"}),
            json!({"workspaceRoot":"/work","providerId":" "}),
        ] {
            let fixture = Fixture::open(options);
            let mut cx = fixture.context();
            cx.native_root_id = None;
            assert_eq!(
                encode(&fixture.command, &cx).unwrap_err(),
                EncodeError::InvalidNativeOptions
            );
        }
    }

    #[test]
    fn muse_commands_work_requires_fresh_matching_facts_and_exact_steer_target() {
        let fixture = Fixture::new(
            "agent.send",
            json!({"delivery":"steer","text":"correction","expected_turn_id":"turn-a"}),
        );
        let mut cx = fixture.context();
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::TurnIdentityMismatch
        );
        cx.work.as_mut().unwrap().status = "running";
        cx.work.as_mut().unwrap().active_turn_id = Some("turn-a");
        let sent = request(encode(&fixture.command, &cx).unwrap());
        assert_eq!(sent.method, "turn/steer");
        assert_eq!(sent.params["expectedTurnId"], "turn-a");
        assert!(!sent.params.contains_key("ifBusy"));
        cx.work.as_mut().unwrap().current_event_version = 8;
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::NativeFactsStale
        );
        cx.work.as_mut().unwrap().current_event_version = 7;
        cx.work.as_mut().unwrap().model_configuration = ModelConfiguration::Pending;
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::ModelConfigurationUnresolved
        );
        cx.work = None;
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::NativeFactsRequired
        );
    }

    #[test]
    fn muse_commands_task_prompt_is_reused_without_rendering_or_raw_task_append() {
        let mut fixture = Fixture::dispatch();
        fixture.command.input["text"] = json!("different Store source");
        fixture.command.input["task_dispatch_context"]["source_text_sha256"] =
            json!(sha256("different Store source"));
        fixture.command.input["task_dispatch_context"]["source_text_bytes"] =
            json!("different Store source".len());
        let encoded = encode(&fixture.command, &fixture.context()).unwrap();
        assert_eq!(
            encoded.first_request().unwrap().params["input"],
            json!([{"type":"text","text":"abc"}])
        );
        let CommandEncoding::Native {
            task_prompt: Some(task),
            ..
        } = encoded
        else {
            panic!("validated TaskPrompt")
        };
        assert_eq!(task.envelope.prompt_sha256, SHA_ABC);
        assert_eq!(task.context.worker_boot_id, BOOT);
        assert_eq!(task.context.operation_id, OPERATION);
    }

    #[test]
    fn muse_commands_task_prompt_rejects_content_identity_and_schema_corruption() {
        for field in [
            "prompt",
            "bytes",
            "unknown",
            "operation",
            "binding",
            "generation",
            "boot",
            "attempt",
            "task",
            "revision",
            "snapshot",
            "source",
            "original_attempt",
            "blank_identity",
        ] {
            let mut fixture = Fixture::dispatch();
            match field {
                "prompt" => fixture.command.input["task_prompt"]["prompt"] = json!("abd"),
                "bytes" => fixture.command.input["task_prompt"]["prompt_bytes"] = json!(2),
                "unknown" => fixture.command.input["task_prompt"]["raw_task"] = json!({}),
                "operation" => {
                    fixture.command.input["task_dispatch_context"]["operation_id"] = json!("other")
                }
                "binding" => {
                    fixture.command.input["task_dispatch_context"]["binding_id"] = json!("other")
                }
                "generation" => {
                    fixture.command.input["task_dispatch_context"]["binding_generation"] = json!(2)
                }
                "boot" => {
                    fixture.command.input["task_dispatch_context"]["worker_boot_id"] =
                        json!("old-boot")
                }
                "attempt" => {
                    fixture.command.input["task_dispatch_context"]["attempt_id"] = json!("other")
                }
                "task" => {
                    fixture.command.input["task_dispatch_context"]["task_id"] = json!("other")
                }
                "revision" => {
                    fixture.command.input["task_dispatch_context"]["task_revision"] = json!(3)
                }
                "snapshot" => {
                    fixture.command.input["task_dispatch_context"]["task_snapshot_sha256"] =
                        json!("c".repeat(64))
                }
                "source" => fixture.command.input["text"] = json!("mutated source"),
                "original_attempt" => {
                    fixture.command.input["attempt_id"] = json!("foreign-original-attempt")
                }
                _ => {
                    fixture.command.input["task_prompt"]["task_id"] = json!(" ");
                    fixture.command.input["task_dispatch_context"]["task_id"] = json!(" ");
                }
            }
            assert_eq!(
                encode(&fixture.command, &fixture.context()).unwrap_err(),
                EncodeError::TaskPromptInvalid,
                "{field}"
            );
        }
        let mut fixture = Fixture::dispatch();
        fixture.command.method = "agent.send".into();
        assert_eq!(
            encode(&fixture.command, &fixture.context()).unwrap_err(),
            EncodeError::TaskPromptInvalid
        );
    }

    #[test]
    fn muse_commands_task_prompt_counts_utf8_bytes_and_enforces_existing_bound() {
        let mut fixture = Fixture::dispatch();
        let unicode = "é😀";
        fixture.command.input["task_prompt"]["prompt"] = json!(unicode);
        fixture.command.input["task_prompt"]["prompt_sha256"] = json!(sha256(unicode));
        fixture.command.input["task_prompt"]["prompt_bytes"] = json!(unicode.chars().count());
        assert_eq!(
            encode(&fixture.command, &fixture.context()).unwrap_err(),
            EncodeError::TaskPromptInvalid
        );
        fixture.command.input["task_prompt"]["prompt_bytes"] = json!(unicode.len());
        assert_eq!(
            request(encode(&fixture.command, &fixture.context()).unwrap()).params["input"][0]["text"],
            unicode
        );
        let oversized = "x".repeat(MAX_TASK_PROMPT_BYTES + 1);
        fixture.command.input["task_prompt"]["prompt_sha256"] = json!(sha256(&oversized));
        fixture.command.input["task_prompt"]["prompt_bytes"] = json!(oversized.len());
        fixture.command.input["task_prompt"]["prompt"] = json!(oversized);
        assert_eq!(
            encode(&fixture.command, &fixture.context()).unwrap_err(),
            EncodeError::TaskPromptInvalid
        );
    }

    #[test]
    fn muse_commands_configuration_is_one_explicit_native_setter() {
        for (settings, method, field, desired) in [
            (
                json!({"reasoningEffort":"max"}),
                "session/setReasoningEffort",
                "reasoningEffort",
                json!("max"),
            ),
            (
                json!({"approvalMode":"denyUnmatched"}),
                "session/setApprovalMode",
                "mode",
                json!("denyUnmatched"),
            ),
            (
                json!({"model":{"modelId":"m"}}),
                "session/setModel",
                "model",
                json!({"modelId":"m"}),
            ),
        ] {
            let fixture = Fixture::new("agent.configure", json!({"settings":settings}));
            let sent = request(encode(&fixture.command, &fixture.context()).unwrap());
            assert_eq!(sent.method, method);
            assert_eq!(sent.params[field], desired);
            assert_eq!(sent.params.len(), 3);
        }
        for settings in [
            json!({}),
            json!({"reasoningEffort":"max","approvalMode":"allowAll"}),
            json!({"reasoningEffort":"super"}),
            json!({"model":{"modelId":"m","providerId":null}}),
            json!({"model":{"modelId":"m","profileId":"not-in-bridge13"}}),
        ] {
            let fixture = Fixture::new("agent.configure", json!({"settings":settings}));
            assert_eq!(
                encode(&fixture.command, &fixture.context()).unwrap_err(),
                EncodeError::InvalidSettings
            );
        }
    }

    #[test]
    fn muse_commands_goal_bare_verbs_and_standing_effort_are_not_invented() {
        for action in ["set", "edit", "pause", "resume", "clear"] {
            let mut input = json!({"action":action});
            if matches!(action, "set" | "edit") {
                input["objective"] = json!("  objective  ");
            }
            let fixture = Fixture::new("agent.goal", input);
            let sent = request(encode(&fixture.command, &fixture.context()).unwrap());
            assert_eq!(sent.method, format!("goal/{action}"));
            assert_eq!(
                sent.params.contains_key("objective"),
                matches!(action, "set" | "edit")
            );
            if matches!(action, "set" | "edit") {
                assert_eq!(sent.params["objective"], "objective");
            }
            assert!(!sent.params.contains_key("reasoningEffort"));
        }
        let mut fixture = Fixture::new("agent.goal", json!({"action":"resume"}));
        fixture.requested.reasoning_effort = Some("max".into());
        assert_eq!(
            encode(&fixture.command, &fixture.context()).unwrap_err(),
            EncodeError::StandingEffortMismatch
        );
        let mut cx = fixture.context();
        cx.work.as_mut().unwrap().standing_effort = Some("max");
        assert!(encode(&fixture.command, &cx).is_ok());
        let fixture = Fixture::new(
            "agent.goal",
            json!({"action":"pause","objective":"not a bare verb"}),
        );
        assert_eq!(
            encode(&fixture.command, &fixture.context()).unwrap_err(),
            EncodeError::InvalidGoal
        );
        let fixture = Fixture::new("agent.goal", json!({"action":"clear"}));
        let mut cx = fixture.context();
        cx.work = None;
        assert!(
            encode(&fixture.command, &cx).is_ok(),
            "clearing old goal work needs no fresh work-start permission"
        );
    }

    #[test]
    fn muse_commands_reply_uses_current_requirement_and_explicit_choice_only() {
        let current = approval();
        let fixture = Fixture::new("agent.reply", approval_reply());
        assert_eq!(
            encode(&fixture.command, &fixture.context()).unwrap_err(),
            EncodeError::NativeFactsRequired
        );
        let mut cx = fixture.context();
        cx.pending_request = Some(pending("approval/request", &current));
        cx.work.as_mut().unwrap().model_configuration = ModelConfiguration::Pending;
        let sent = request(encode(&fixture.command, &cx).unwrap());
        assert_eq!(sent.method, "approval/decide");
        assert_eq!(sent.params["choiceId"], "deny-once");
        assert!(!sent.params.contains_key("decision"));
        for field in ["stage", "choice", "family", "command", "extra"] {
            let mut fixture = Fixture::new("agent.reply", approval_reply());
            let p = &mut fixture.command.input["reply"]["params"];
            match field {
                "stage" => p["requirementId"]["sourceIndex"] = json!(1),
                "choice" => p["choiceId"] = json!("not-current"),
                "family" => p["sessionId"] = json!("foreign-session"),
                "command" => p["commandId"] = json!("different-command"),
                _ => p["decision"] = json!("approved"),
            }
            let mut cx = fixture.context();
            cx.pending_request = Some(pending("approval/request", &current));
            assert!(encode(&fixture.command, &cx).is_err(), "{field}");
        }
        cx.pending_request.as_mut().unwrap().current_event_version = 5;
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::NativeFactsStale
        );
    }

    fn user_input() -> Value {
        json!({"sessionId":"ses_child","userInputId":"ask-one","questions":[
            {"id":"single-q","options":[{"label":"yes"},{"label":"no"}],"selection":{"mode":"single"}},
            {"id":"multi-q","options":[{"label":"a"},{"label":"b"}],"selection":{"mode":"multiple","minSelections":1,"maxSelections":2}}
        ]})
    }

    fn answer() -> Value {
        json!({"reply":{"method":"userInput/answer","params":{"sessionId":"ses_child","userInputId":"ask-one","answers":[
            {"questionId":"single-q","selectedLabel":"no"},{"questionId":"multi-q","selectedLabels":["a","b"]}
        ]}}})
    }

    #[test]
    fn muse_commands_user_input_requires_every_exact_question_and_valid_selection() {
        let current = user_input();
        let fixture = Fixture::new("agent.reply", answer());
        let mut cx = fixture.context();
        cx.pending_request = Some(pending("userInput/request", &current));
        assert_eq!(
            request(encode(&fixture.command, &cx).unwrap()).method,
            "userInput/answer"
        );
        for field in [
            "missing",
            "duplicate",
            "label",
            "bounds",
            "mixed",
            "attachments",
            "prompt",
        ] {
            let mut fixture = Fixture::new("agent.reply", answer());
            let p = &mut fixture.command.input["reply"]["params"];
            match field {
                "missing" => {
                    p["answers"].as_array_mut().unwrap().pop();
                }
                "duplicate" => p["answers"][1]["questionId"] = json!("single-q"),
                "label" => p["answers"][0]["selectedLabel"] = json!("Yes"),
                "bounds" => p["answers"][1]["selectedLabels"] = json!(["a", "a"]),
                "mixed" => p["answers"][0]["freeText"] = json!("also answer"),
                "attachments" => p["attachments"] = json!([]),
                _ => p["userInputId"] = json!("another-prompt"),
            }
            let mut cx = fixture.context();
            cx.pending_request = Some(pending("userInput/request", &current));
            assert!(encode(&fixture.command, &cx).is_err(), "{field}");
        }
        let mut current = user_input();
        current["questions"][1]["selection"]
            .as_object_mut()
            .unwrap()
            .remove("maxSelections");
        let mut cx = fixture.context();
        cx.pending_request = Some(pending("userInput/request", &current));
        assert!(
            encode(&fixture.command, &cx).is_err(),
            "missing effective native bounds cannot become guessed limits"
        );
    }

    #[test]
    fn muse_commands_user_input_cancel_and_clarify_are_addressed_and_bounded() {
        let current = user_input();
        for (method, extra) in [
            ("userInput/cancel", json!({"reason":"declined"})),
            (
                "userInput/clarify",
                json!({"clarification":{"format":"text","content":"é".repeat(500)}}),
            ),
        ] {
            let mut params = extra;
            params["sessionId"] = json!("ses_child");
            params["userInputId"] = json!("ask-one");
            let fixture = Fixture::new(
                "agent.reply",
                json!({"reply":{"method":method,"params":params}}),
            );
            let mut cx = fixture.context();
            cx.pending_request = Some(pending("userInput/request", &current));
            assert_eq!(
                request(encode(&fixture.command, &cx).unwrap()).method,
                method
            );
        }
        let fixture = Fixture::new(
            "agent.reply",
            json!({"reply":{"method":"userInput/clarify","params":{
            "sessionId":"ses_child","userInputId":"ask-one","clarification":{"format":"text","content":"x".repeat(501)}}}}),
        );
        let mut cx = fixture.context();
        cx.pending_request = Some(pending("userInput/request", &current));
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::InvalidReply
        );
    }

    #[test]
    fn muse_commands_native_mcp_requires_real_grant_and_closed_transport_arm() {
        let fixture = Fixture::open(json!({"workspaceRoot":"/work","mcpServers":{"local":{
            "transport":"stdio","command":"caller-supplied","args":["verbatim"],"mode":"optional"}}}));
        let mut cx = fixture.context();
        cx.native_root_id = None;
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::SessionMcpNotGranted
        );
        cx.session_mcp_granted = true;
        let sent = request(encode(&fixture.command, &cx).unwrap());
        assert_eq!(
            sent.params["config"]["mcpServers"],
            fixture.command.route["native_options"]["mcpServers"]
        );
        assert!(
            !sent.params["config"]["mcpServers"]["local"]
                .as_object()
                .unwrap()
                .contains_key("env")
        );
        let mut fixture = Fixture::open(
            json!({"workspaceRoot":"/work","mcpServers":{"x":{"transport":"sse","url":"https://example.invalid"}}}),
        );
        let mut cx = fixture.context();
        cx.native_root_id = None;
        cx.session_mcp_granted = true;
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::InvalidMcpServers
        );
        fixture.command.route["native_options"]["workspaceRoot"] = json!("relative");
        let mut cx = fixture.context();
        cx.native_root_id = None;
        cx.session_mcp_granted = true;
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::InvalidNativeOptions
        );
    }

    #[test]
    fn muse_commands_recovery_never_falls_back_to_start_or_replays_prompt() {
        let fixture = Fixture::new("agent.recover", json!({"expected_boot_id":BOOT}));
        let mut cx = fixture.context();
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::NativeFactsRequired
        );
        for facts in [
            RecoveryFacts {
                durability: SessionDurability::Ephemeral,
                native_ready: false,
            },
            RecoveryFacts {
                durability: SessionDurability::Durable,
                native_ready: true,
            },
        ] {
            cx.recovery = Some(facts);
            assert_eq!(
                encode(&fixture.command, &cx).unwrap_err(),
                EncodeError::RecoveryNotPermitted
            );
        }
        cx.recovery = Some(RecoveryFacts {
            durability: SessionDurability::Unrecognized,
            native_ready: false,
        });
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::NativeFactsRequired
        );
        cx.recovery = Some(RecoveryFacts {
            durability: SessionDurability::Durable,
            native_ready: false,
        });
        cx.work.as_mut().unwrap().status = "notLoaded";
        let sent = request(encode(&fixture.command, &cx).unwrap());
        assert_eq!(sent.method, "session/resume");
        assert_eq!(sent.params["sessionId"], ROOT);
        assert_eq!(sent.params["excludeItems"], true);
        assert_eq!(sent.params.len(), 3);
        cx.work.as_mut().unwrap().read_method = "session/list";
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::NativeFactsRequired
        );
        cx.work.as_mut().unwrap().read_method = "session/read";
        cx.work.as_mut().unwrap().session_id = "foreign-root";
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::NativeIdentityMismatch
        );
    }

    #[test]
    fn muse_commands_refresh_only_encodes_its_non_consuming_first_read() {
        let fixture = Fixture::new("agent.refresh", json!({"session_id":"ses_child"}));
        let sent = request(encode(&fixture.command, &fixture.context()).unwrap());
        assert_eq!(sent.method, "session/read");
        assert!(!sent.params.contains_key("commandId"));
        assert_eq!(sent.params["sessionId"], "ses_child");
        assert_eq!(sent.params["excludeItems"], true);
    }

    #[test]
    fn muse_commands_result_delegates_exact_validated_input_without_a_parallel_codec() {
        let fixture = Fixture::new(
            "agent.result",
            json!({"selector":{"kind":"message","session_id":"ses_child",
            "item_id":"exact-item","item_revision":3,"before_cursor":"opaque-cursor"},"offset_bytes":7,"length_bytes":9}),
        );
        let mut cx = fixture.context();
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::ResultRequestUnvalidated
        );
        // Only the handoff contract is tested here. Result schema and paging
        // vectors belong to results.rs and its ResultRequest::parse verifier.
        cx.validated_result_input = Some(&fixture.command.input);
        let encoded = encode(&fixture.command, &cx).unwrap();
        assert!(encoded.first_request().is_none());
        let CommandEncoding::ResultReadRequired { validated_input } = encoded else {
            panic!("results owner handoff")
        };
        assert_eq!(validated_input, fixture.command.input);
        let other_input = json!({"different":"input"});
        cx.validated_result_input = Some(&other_input);
        assert_eq!(
            encode(&fixture.command, &cx).unwrap_err(),
            EncodeError::ResultRequestUnvalidated
        );
    }

    #[test]
    fn muse_commands_reconcile_requires_retained_facts_and_never_encodes_a_resend() {
        let mut fixture = Fixture::new(
            "agent.reconcile",
            json!({"operation_id":"original-operation"}),
        );
        assert_eq!(
            encode(&fixture.command, &fixture.context()).unwrap_err(),
            EncodeError::InvalidReconciliationTarget
        );
        fixture.command.target_input_sha256 = Some("b".repeat(64));
        let encoded = encode(&fixture.command, &fixture.context()).unwrap();
        assert!(encoded.first_request().is_none());
        let CommandEncoding::ReconciliationRequired(target) = encoded else {
            panic!("needs journal-owned disposition")
        };
        assert_eq!(target.operation_id, "original-operation");
        assert_eq!(target.input_sha256, "b".repeat(64));
        fixture.command.native_root_id = Some("foreign-root".into());
        assert_eq!(
            encode(&fixture.command, &fixture.context()).unwrap_err(),
            EncodeError::NativeIdentityMismatch
        );
    }

    #[test]
    fn muse_commands_lost_start_ack_keeps_rootless_reconciliation_as_journal_only_target() {
        let mut fixture = Fixture::new(
            "agent.reconcile",
            json!({"operation_id":"pending-session-start"}),
        );
        fixture.command.native_root_id = None;
        fixture.command.target_input_sha256 = Some("b".repeat(64));
        let mut cx = fixture.context();
        cx.native_root_id = None;
        cx.fresh_root_session_id = Some(FRESH_ROOT);
        let encoded = encode(&fixture.command, &cx).unwrap();
        assert!(encoded.first_request().is_none());
        let CommandEncoding::ReconciliationRequired(target) = encoded else {
            panic!("journal-owned lost start")
        };
        assert_eq!(target.operation_id, "pending-session-start");
        // The reserved ID remains only pending identity, never an adopted root.
        assert_eq!(cx.native_root_id, None);
        assert_eq!(fixture.command.native_root_id, None);
    }
}
