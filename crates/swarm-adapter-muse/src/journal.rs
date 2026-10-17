//! Muse-specific private checkpoint/outbox, not another Task store or SDK facade.
//!
//! Ports bridge.13 `submitNative`, `completeNative`, `reconcileNative`, `report`
//! and checkpoint.mjs: intent before I/O, ACK before application, exact host
//! delivery until ACK, and no native replay on reopen/reconnect. Native wire,
//! work/model fences, authorization and outcome evidence remain caller duties.
//!
//! Checkpoint v2 retains original commands, receipts, selectors and work fences
//! alongside the exact native invocation; the immutable scope marker/file names
//! remain v1 for existing NEW-Rust bindings. Old v1 checkpoints load without
//! inventing context: their outbox remains deliverable and native intent remains
//! held, but no legacy native replay/ACK application is enabled. Node
//! checkpoint.json is never read, adopted, rewritten or used as a fallback.
//! The existing module owner owns the directory lock/custody.
//!
//! Driver integration order (all final checks/enqueue under its event mutex):
//! record_initialize AFTER initialize/initialized flush; invalidate_observations
//! BEFORE every native event/connection change; record_work_read for a stable
//! exact-root read; capture_projection/retain_observations for the shared fold;
//! authorize_session before and after child reads; capture_pending_inventory
//! and atomically pair its reduced projector using retain_observations; finally
//! native_invocation (for explicitly granted session MCP config), prepare_command
//! and synchronously enqueue PreparedNative's exact invocation. NativeInvocation::new
//! has no grant authority and always rejects config, including an empty map.
//! The compatibility prepare_native seam is same-worker/root-only; it cannot
//! supply context, child authority or an MCP grant. Driver must use prepare_command.
//! Cross-worker agent.reconcile uses retained_command/retained_invocation and
//! reconcile_command, preserving the ORIGINAL receipt, TaskPrompt boot, UUIDv7,
//! method and payload. An ambiguous start stays the same explicit session/start,
//! never another root/adoption/prompt; an exact initialize grant is required
//! again for its session MCP config. MCP admission proves no server liveness.
//! The retained MCP profile permits no HTTP header or environment additions:
//! headers/env must be absent or empty, even for nominally non-secret values.
//! HTTP addresses cannot contain userinfo, queries or fragments. This is a
//! deliberately narrow admission profile, not a detector for every secret in
//! arbitrary command arguments, names or path text. Admitted bytes stay exact;
//! rejected payloads are never redacted or rewritten for replay. Host-private
//! credential-file references remain a separate Host mechanism: this journal
//! does not resolve them or read native auth/settings/HOME files.
//! Existing checkpoints outside this profile fail validation without scrubbing,
//! discarding their unknown intents, adopting fresh state or replaying them.
//!
//! restored_observations rebinds through the shared reducer's STALE restore path.
//! Pending payloads and paired opaque discovery cursors remain historical only;
//! no authority is restored. Events, native preparation, connection changes and
//! journal instance replacement revoke opaque session/capture tickets.
//! Host authorization, actual native reads, final current-event checks and
//! enqueue correlation remain Driver duties; this module performs no native IO.
//! There is no same-live-MSP-connection proof. A no-ACK reconciliation therefore
//! requires a fresh current-boot durable-host declaration; ephemeral same-link
//! replay stays unavailable instead of assuming persisted native idempotency.
//! The driver completes initialize -> initialized before any native command.
//! Neither a journal write nor a native ACK establishes execution/result/Task
//! acceptance. A resume additionally needs a fresh exact-root session/read fact.

use crate::{
    commands,
    observations::{ObservationProjector, ProjectionFence},
    schema,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt, fs,
    path::{Path, PathBuf},
};
use swarm_contracts::{
    module_catalog::{ArtifactIdentity, ModuleId, Sha256Digest},
    runtime::{EffectOutcome, ModuleReceiptIdentity, RuntimeCommand, RuntimeOutcome},
};
use swarm_process::{read_private_durable, replace_private_durable, write_private_new};

const FORMAT: &str = "eliot.muse.rust-private-state";
const VERSION: u16 = 1;
const CHECKPOINT_VERSION: u16 = 2;
const MARKER: &str = "muse-rust-scope.v1.json";
const CHECKPOINT: &str = "muse-rust-checkpoint.v1.json";
const MAX_FILE: u64 = 16 * 1024 * 1024;
const MAX_ENTRY: usize = 1024 * 1024;
const MAX_CONTEXT: usize = 3 * 1024 * 1024;
const MAX_OPERATIONS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    OwnerUnverified,
    ScopeMismatch,
    LegacyState,
    StateAlreadyExists,
    MissingState,
    InvalidState,
    Capacity,
    PersistenceUnknown,
    Poisoned,
    IntentConflict,
    ExplicitReconcileRequired,
    AckConflict,
    OutcomeConflict,
    DurabilityUnavailable,
    ExactRootReadRequired,
    ContextRequired,
    FreshChecksRequired,
    GrantRequired,
    ProvenanceRequired,
}
impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}
impl Error for Fault {}
impl Fault {
    pub fn code(self) -> &'static str {
        match self {
            Self::OwnerUnverified => "MUSE_JOURNAL_OWNER_UNVERIFIED",
            Self::ScopeMismatch => "MUSE_JOURNAL_SCOPE_MISMATCH",
            Self::LegacyState => "MUSE_JOURNAL_LEGACY_STATE",
            Self::StateAlreadyExists => "MUSE_JOURNAL_STATE_ALREADY_EXISTS",
            Self::MissingState => "MUSE_JOURNAL_MISSING_STATE",
            Self::InvalidState => "MUSE_JOURNAL_INVALID_STATE",
            Self::Capacity => "MUSE_JOURNAL_CAPACITY",
            Self::PersistenceUnknown => "MUSE_JOURNAL_PERSISTENCE_UNKNOWN",
            Self::Poisoned => "MUSE_JOURNAL_POISONED",
            Self::IntentConflict => "MUSE_JOURNAL_INTENT_CONFLICT",
            Self::ExplicitReconcileRequired => "MUSE_JOURNAL_EXPLICIT_RECONCILE_REQUIRED",
            Self::AckConflict => "MUSE_JOURNAL_ACK_CONFLICT",
            Self::OutcomeConflict => "MUSE_JOURNAL_OUTCOME_CONFLICT",
            Self::DurabilityUnavailable => "MUSE_JOURNAL_DURABILITY_UNAVAILABLE",
            Self::ExactRootReadRequired => "MUSE_JOURNAL_EXACT_ROOT_READ_REQUIRED",
            Self::ContextRequired => "MUSE_JOURNAL_CONTEXT_REQUIRED",
            Self::FreshChecksRequired => "MUSE_JOURNAL_FRESH_CHECKS_REQUIRED",
            Self::GrantRequired => "MUSE_JOURNAL_SESSION_MCP_GRANT_REQUIRED",
            Self::ProvenanceRequired => "MUSE_JOURNAL_FAMILY_PROVENANCE_REQUIRED",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub module_client_id: String,
    pub module_id: ModuleId,
    pub artifact: ArtifactIdentity,
    pub binding_id: String,
    pub generation: i64,
    pub native_namespace: String,
    pub native_root_id: String,
}
impl Scope {
    fn validate(&self) -> Result<(), Fault> {
        if self.generation <= 0
            || [
                &self.module_client_id,
                &self.binding_id,
                &self.native_namespace,
                &self.native_root_id,
            ]
            .iter()
            .any(|value| !atom(value))
        {
            return Err(Fault::InvalidState);
        }
        Ok(())
    }
    fn receipt(&self, receipt: &ModuleReceiptIdentity) -> Result<(), Fault> {
        receipt.validate().map_err(|_| Fault::InvalidState)?;
        if receipt.module_id != self.module_id
            || receipt.artifact != self.artifact
            || receipt.binding_id != self.binding_id
            || receipt.binding_generation != self.generation
        {
            return Err(Fault::ScopeMismatch);
        }
        Ok(())
    }
}

fn atom(value: &str) -> bool {
    !value.is_empty() && value.len() <= 8192 && !value.chars().any(char::is_control)
}
fn digest(value: &Value) -> Result<Sha256Digest, Fault> {
    Sha256Digest::of_canonical_json(value).map_err(|_| Fault::InvalidState)
}
fn instance_token(boot: &str) -> Result<String, Fault> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let sequence = NEXT
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_add(1))
        .map_err(|_| Fault::Capacity)?;
    Ok(
        digest(&json!({"boot":boot,"process":std::process::id(),"instance":sequence}))?
            .as_str()
            .to_owned(),
    )
}

/// Exact saved JSON plus SHA-256 of its canonical JSON value, matching the
/// public SDK's value-equivalent command signature contract. Byte equality is
/// additionally required for saved ACKs and delivery retirement; no text repair.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedJson {
    utf8: String,
    sha256: Sha256Digest,
}
impl fmt::Debug for SavedJson {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SavedJson")
            .field("bytes", &self.utf8.len())
            .field("sha256", &self.sha256)
            .finish()
    }
}
impl SavedJson {
    fn new(value: &Value) -> Result<Self, Fault> {
        let utf8 = serde_json::to_string(value).map_err(|_| Fault::InvalidState)?;
        if utf8.len() > MAX_ENTRY {
            return Err(Fault::Capacity);
        }
        Ok(Self {
            utf8,
            sha256: digest(value)?,
        })
    }
    fn value(&self) -> Result<Value, Fault> {
        if self.utf8.len() > MAX_ENTRY {
            return Err(Fault::InvalidState);
        }
        let value = serde_json::from_str(&self.utf8).map_err(|_| Fault::InvalidState)?;
        if digest(&value)? != self.sha256 {
            return Err(Fault::InvalidState);
        }
        Ok(value)
    }
}

// The public generated schema is additive on the wire. Retention deliberately
// accepts only its declared fields, recursively, plus explicitly typed maps.
// Full known ACKs survive; undeclared fields are refused. This is shape
// validation, not general secret detection. MCP retention has a narrower profile.
fn closed_shape(s: &Value, v: &Value, depth: usize) -> Result<(), Fault> {
    if depth > 64 {
        return Err(Fault::Capacity);
    }
    if let Some(reference) = s["$ref"].as_str() {
        let name = reference
            .strip_prefix("#/$defs/")
            .ok_or(Fault::InvalidState)?;
        return closed_shape(
            schema::definition_schema(name).map_err(|_| Fault::InvalidState)?,
            v,
            depth + 1,
        );
    }
    for union in ["oneOf", "anyOf"] {
        if let Some(arms) = s[union].as_array() {
            return if arms
                .iter()
                .any(|arm| closed_shape(arm, v, depth + 1).is_ok())
            {
                Ok(())
            } else {
                Err(Fault::InvalidState)
            };
        }
    }
    if let Some(value) = s.get("const")
        && value != v
    {
        return Err(Fault::InvalidState);
    }
    if let Some(values) = s["enum"].as_array()
        && !values.contains(v)
    {
        return Err(Fault::InvalidState);
    }
    if let Some(ty) = s.get("type") {
        let matches = |ty: &str| match ty {
            "object" => v.is_object(),
            "array" => v.is_array(),
            "string" => v.is_string(),
            "null" => v.is_null(),
            "boolean" => v.is_boolean(),
            "integer" => v.as_i64().is_some() || v.as_u64().is_some(),
            "number" => v.is_number(),
            _ => false,
        };
        if !ty.as_str().is_some_and(matches)
            && !ty
                .as_array()
                .is_some_and(|types| types.iter().any(|ty| ty.as_str().is_some_and(matches)))
        {
            return Err(Fault::InvalidState);
        }
    }
    if let Some(object) = v.as_object() {
        if let Some(required) = s["required"].as_array()
            && required
                .iter()
                .any(|key| key.as_str().is_none_or(|key| !object.contains_key(key)))
        {
            return Err(Fault::InvalidState);
        }
        for (key, value) in object {
            if let Some(property) = s["properties"].get(key) {
                closed_shape(property, value, depth + 1)?;
            } else if let Some(additional) = s.get("additionalProperties").filter(|s| s.is_object())
            {
                closed_shape(additional, value, depth + 1)?;
            } else {
                return Err(Fault::InvalidState);
            }
        }
    }
    if let Some(items) = v.as_array() {
        let item_schema = s.get("items").ok_or(Fault::InvalidState)?;
        for item in items {
            closed_shape(item_schema, item, depth + 1)?;
        }
    }
    Ok(())
}
fn reject_private_keys(v: &Value) -> Result<(), Fault> {
    match v {
        Value::Object(object) => {
            for (key, value) in object {
                if ["modelproviderkey", "providerapikey"]
                    .contains(&key.to_ascii_lowercase().as_str())
                {
                    return Err(Fault::InvalidState);
                }
                if key == "env"
                    && value.as_object().is_some_and(|env| {
                        env.keys().any(|key| {
                            key.eq_ignore_ascii_case("HOME")
                                || key.to_ascii_uppercase().starts_with("XDG_")
                        })
                    })
                {
                    return Err(Fault::InvalidState);
                }
                reject_private_keys(value)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                reject_private_keys(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}
fn definition(name: &str, v: &Value) -> Result<(), Fault> {
    schema::validate_definition(name, v).map_err(|_| Fault::InvalidState)?;
    closed_shape(
        schema::definition_schema(name).map_err(|_| Fault::InvalidState)?,
        v,
        0,
    )?;
    reject_private_keys(v)
}
// Auth-bearing map names cannot be exhaustively enumerated: deny the entire
// inline header/environment channel. In particular a real sessionMcp grant is
// authorization to request a server, not authority to persist its credentials.
fn validate_retained_mcp_profile(servers: &Value) -> Result<(), Fault> {
    let servers = servers.as_object().ok_or(Fault::InvalidState)?;
    if servers.len() > MAX_OPERATIONS {
        return Err(Fault::Capacity);
    }
    for (name, server) in servers {
        if !atom(name) {
            return Err(Fault::InvalidState);
        }
        definition("SessionMcpServerConfig", server)?;
        for field in ["headers", "env"] {
            if server
                .get(field)
                .is_some_and(|value| value.as_object().is_none_or(|map| !map.is_empty()))
            {
                return Err(Fault::InvalidState);
            }
        }
        if server["transport"] == "streamableHttp" {
            let url = server["url"].as_str().ok_or(Fault::InvalidState)?;
            let tail = url
                .strip_prefix("https://")
                .or_else(|| url.strip_prefix("http://"))
                .ok_or(Fault::InvalidState)?;
            let authority = tail.split('/').next().ok_or(Fault::InvalidState)?;
            if authority.is_empty()
                || authority.chars().any(|c| {
                    !c.is_ascii_alphanumeric() && !matches!(c, '.' | '-' | ':' | '[' | ']')
                })
                || url.chars().any(|c| {
                    c.is_control() || c.is_whitespace() || matches!(c, '@' | '?' | '#' | '\\')
                })
            {
                return Err(Fault::InvalidState);
            }
        }
    }
    Ok(())
}
fn reply_method(method: &str) -> bool {
    matches!(
        method,
        "approval/decide" | "userInput/answer" | "userInput/cancel" | "userInput/clarify"
    )
}
fn ack_definition(method: &str) -> Result<&'static str, Fault> {
    Ok(match method {
        "session/start" => "SessionStartResult",
        "session/resume" => "SessionResumeResult",
        "turn/start" => "TurnStartResult",
        "turn/steer" => "TurnSteerResult",
        "session/setModel" => "SessionSetModelResult",
        "session/setReasoningEffort" => "SessionSetReasoningEffortResult",
        "session/setApprovalMode" => "SessionSetApprovalModeResult",
        "approval/decide" => "ApprovalDecideResult",
        "userInput/answer" => "UserInputAnswerResult",
        "userInput/cancel" => "UserInputCancelResult",
        "userInput/clarify" => "UserInputClarifyResult",
        "goal/set" | "goal/edit" | "goal/pause" | "goal/resume" | "goal/clear" => {
            "GoalCommandResult"
        }
        _ => return Err(Fault::InvalidState),
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeInvocation {
    method: String,
    command_id: String,
    params: SavedJson,
    payload_sha256: Sha256Digest,
}
impl NativeInvocation {
    pub fn new(
        scope: &Scope,
        method: &str,
        command_id: &str,
        params: Map<String, Value>,
    ) -> Result<Self, Fault> {
        if params.contains_key("config") {
            return Err(Fault::GrantRequired);
        }
        Self::from_admitted_params(scope, method, command_id, params)
    }
    fn from_admitted_params(
        scope: &Scope,
        method: &str,
        command_id: &str,
        params: Map<String, Value>,
    ) -> Result<Self, Fault> {
        let params = SavedJson::new(&Value::Object(params))?;
        let invocation = Self {
            method: method.into(),
            command_id: command_id.into(),
            payload_sha256: digest(&json!({"method":method,"params":params.value()?}))?,
            params,
        };
        invocation.validate(scope)?;
        Ok(invocation)
    }
    pub fn method(&self) -> &str {
        &self.method
    }
    pub fn command_id(&self) -> &str {
        &self.command_id
    }
    pub fn payload_sha256(&self) -> &str {
        self.payload_sha256.as_str()
    }
    pub fn params(&self) -> Result<Map<String, Value>, Fault> {
        self.params
            .value()?
            .as_object()
            .cloned()
            .ok_or(Fault::InvalidState)
    }
    fn validate(&self, scope: &Scope) -> Result<(), Fault> {
        let params = Value::Object(self.params()?);
        let target = params["sessionId"]
            .as_str()
            .filter(|id| atom(id))
            .ok_or(Fault::InvalidState)?;
        ack_definition(&self.method)?;
        commands::validate_native_command_id(&self.command_id).map_err(|_| Fault::InvalidState)?;
        if params["commandId"] != self.command_id
            || self.payload_sha256 != digest(&json!({"method":self.method,"params":params}))?
            || target != scope.native_root_id && !reply_method(&self.method)
        {
            return Err(Fault::ScopeMismatch);
        }
        schema::validate_method_parameters(&self.method, &params)
            .map_err(|_| Fault::InvalidState)?;
        closed_shape(
            schema::method_parameters_schema(&self.method).map_err(|_| Fault::InvalidState)?,
            &params,
            0,
        )?;
        if let Some(config) = params.get("config")
            && (self.method != "session/start"
                || config
                    .as_object()
                    .is_none_or(|object| object.len() != 1 || !object.contains_key("mcpServers")))
        {
            return Err(Fault::InvalidState);
        }
        if let Some(config) = params.get("config") {
            validate_retained_mcp_profile(&config["mcpServers"])?;
        }
        if self.method == "session/resume" && params["excludeItems"] != true {
            return Err(Fault::InvalidState);
        }
        if matches!(self.method.as_str(), "goal/set" | "goal/edit") {
            let objective = params["objective"].as_str().ok_or(Fault::InvalidState)?;
            if objective.is_empty() || objective.trim() != objective {
                return Err(Fault::InvalidState);
            }
        }
        reject_private_keys(&params)?;
        Ok(())
    }
    // Read-only compatibility for v1, never an implicit upgrade/replay permit.
    fn validate_v1(&self, scope: &Scope) -> Result<(), Fault> {
        let id = self.command_id.as_bytes();
        if id.len() != 36
            || id[14] != b'7'
            || !b"89ab".contains(&id[19])
            || id.iter().enumerate().any(|(n, b)| {
                if [8, 13, 18, 23].contains(&n) {
                    *b != b'-'
                } else {
                    !b.is_ascii_hexdigit() || b.is_ascii_uppercase()
                }
            })
        {
            return Err(Fault::InvalidState);
        }
        let value = self.params.value()?;
        let params = value.as_object().ok_or(Fault::InvalidState)?;
        let extra: &[&str] = match self.method.as_str() {
            "session/start" => &["workspaceRoot", "modelId", "providerId", "approvalMode"],
            "session/resume" => &["excludeItems"],
            "turn/start" => &["input", "ifBusy", "reasoningEffort"],
            "turn/steer" => &["input", "expectedTurnId", "reasoningEffort"],
            "session/setModel" => &["modelId", "providerId"],
            "session/setReasoningEffort" => &["reasoningEffort"],
            "session/setApprovalMode" => &["mode"],
            "goal/set" | "goal/edit" => &["objective"],
            "goal/pause" | "goal/resume" | "goal/clear" => &[],
            "approval/decide" => &["approvalId", "choiceId", "feedback", "requirementId"],
            "userInput/answer" => &["userInputId", "answers"],
            "userInput/cancel" => &["userInputId", "reason"],
            "userInput/clarify" => &["userInputId", "clarification"],
            _ => return Err(Fault::InvalidState),
        };
        if params.get("commandId").and_then(Value::as_str) != Some(self.command_id.as_str())
            || params.get("sessionId").and_then(Value::as_str)
                != Some(scope.native_root_id.as_str())
            || params.keys().any(|key| {
                !["commandId", "sessionId"].contains(&key.as_str())
                    && !extra.contains(&key.as_str())
            })
            || self.payload_sha256 != digest(&json!({"method":&self.method,"params":&value}))?
            || self.method == "session/resume"
                && params.get("excludeItems") != Some(&Value::Bool(true))
        {
            return Err(Fault::ScopeMismatch);
        }
        if matches!(self.method.as_str(), "turn/start" | "turn/steer") {
            let input = params
                .get("input")
                .and_then(Value::as_array)
                .ok_or(Fault::InvalidState)?;
            if input.is_empty()
                || input.iter().any(|block| {
                    block.as_object().is_none_or(|block| {
                        block.len() != 2
                            || block.get("type").and_then(Value::as_str) != Some("text")
                            || block.get("text").and_then(Value::as_str).is_none()
                    })
                })
                || self.method == "turn/start"
                    && params.get("ifBusy").is_some_and(|value| value != "queue")
                || self.method == "turn/steer"
                    && params
                        .get("expectedTurnId")
                        .and_then(Value::as_str)
                        .is_none_or(str::is_empty)
            {
                return Err(Fault::InvalidState);
            }
        }
        if matches!(self.method.as_str(), "goal/set" | "goal/edit") {
            let objective = params
                .get("objective")
                .and_then(Value::as_str)
                .ok_or(Fault::InvalidState)?;
            // SS3.18 defines the trimmed string as the payload. Never silently
            // change an already digested command; reject a noncanonical input.
            if objective.is_empty() || objective.trim() != objective {
                return Err(Fault::InvalidState);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Durability {
    Durable,
    Ephemeral,
    Unrecognized,
}
impl Durability {
    /// SDK 1.3.0 InitializeResult/SS2.13: missing is durable for compatibility;
    /// explicit null or an unfamiliar declaration guarantees nothing.
    pub fn from_initialize_declaration(value: Option<&Value>) -> Self {
        match value {
            None => Self::Durable,
            Some(Value::String(value)) if value == "durable" => Self::Durable,
            Some(Value::String(value)) if value == "ephemeral" => Self::Ephemeral,
            _ => Self::Unrecognized,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurabilityFact {
    profile: Durability,
    observed_boot: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativePending {
    invocation: NativeInvocation,
    submission_boot: String,
    ack: Option<SavedJson>,
    reconcile_block: Option<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum DeliveryMethod {
    Outcome,
    Result,
}
impl DeliveryMethod {
    fn method(&self) -> &'static str {
        match self {
            Self::Outcome => "module.outcome",
            Self::Result => "module.result",
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Delivery {
    method: DeliveryMethod,
    params: SavedJson,
}

/// Omitted selectors remain omitted. These are constraints, never observations.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedSelectors {
    pub model_id: Option<String>,
    pub provider_id: Option<String>,
    pub reasoning_effort: Option<String>,
}
impl RetainedSelectors {
    fn encoder(&self) -> commands::RequestedSelectors {
        commands::RequestedSelectors {
            model_id: self.model_id.clone(),
            provider_id: self.provider_id.clone(),
            reasoning_effort: self.reasoning_effort.clone(),
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetainedModelState {
    Unknown,
    Pending,
    Settled,
}
/// Compact, exact session/read work sample. No log path, preview or transcript.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedWorkFence {
    session_id: String,
    model_id: Option<String>,
    provider_id: Option<String>,
    workspace_root: Option<String>,
    status: String,
    active_turn_id: Option<String>,
    model_state: RetainedModelState,
    standing_effort: Option<String>,
    sampled_revision: u64,
}
impl RetainedWorkFence {
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
    pub fn revision(&self) -> u64 {
        self.sampled_revision
    }
    fn facts(&self) -> commands::WorkFacts<'_> {
        commands::WorkFacts {
            read_method: "session/read",
            session_id: &self.session_id,
            model_id: self.model_id.as_deref(),
            provider_id: self.provider_id.as_deref(),
            status: &self.status,
            active_turn_id: self.active_turn_id.as_deref(),
            sampled_event_version: self.sampled_revision,
            current_event_version: self.sampled_revision,
            model_configuration: match self.model_state {
                RetainedModelState::Unknown => commands::ModelConfiguration::Unknown,
                RetainedModelState::Pending => commands::ModelConfiguration::Pending,
                RetainedModelState::Settled => commands::ModelConfiguration::Settled,
            },
            standing_effort: self.standing_effort.as_deref(),
        }
    }
    fn same_work(&self, other: &Self) -> bool {
        self.session_id == other.session_id
            && self.model_id == other.model_id
            && self.provider_id == other.provider_id
            && self.workspace_root == other.workspace_root
            && self.status == other.status
            && self.active_turn_id == other.active_turn_id
            && self.model_state == other.model_state
            && self.standing_effort == other.standing_effort
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedPending {
    method: String,
    params: SavedJson,
    revision: u64,
}

/// The original Host command, original receipt and original worker boot survive
/// replacement. TaskPrompt lives once, inside command.input; task_prompt()
/// revalidates that exact typed envelope/context rather than rebinding its boot.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedCommandContext {
    command: RuntimeCommand,
    original_receipt: ModuleReceiptIdentity,
    original_boot: String,
    native_command_id: String,
    payload_sha256: String,
    requested: RetainedSelectors,
    work: Option<RetainedWorkFence>,
    pending: Option<RetainedPending>,
    family: Option<FamilyProof>,
    session_mcp_granted: bool,
}

// Store::module_route_projection retains the configured Route; module.next
// forwards original_request_json, with typed TaskPrompt enrichment, unchanged.
// Validate that envelope in place. Reduced v2 contexts remain readable, but no
// key is stripped and neither metadata nor tool presentation grants authority.
fn validate_host_envelope(scope: &Scope, command: &RuntimeCommand) -> Result<(), Fault> {
    reject_private_keys(&serde_json::to_value(command).map_err(|_| Fault::InvalidState)?)?;
    let route = command.route.as_object().ok_or(Fault::InvalidState)?;
    if route.keys().any(|key| {
        ![
            "module_artifact_id",
            "native_options",
            "alias",
            "runtime",
            "enabled",
            "workspace_option",
            "admission_policy",
            "owned_service",
        ]
        .contains(&key.as_str())
    }) || command.route["module_artifact_id"] != scope.artifact.artifact_id.as_str()
    {
        return Err(Fault::InvalidState);
    }
    let host_route = ["alias", "runtime", "enabled"]
        .iter()
        .any(|key| route.contains_key(*key));
    if host_route
        && (!route.get("alias").and_then(Value::as_str).is_some_and(atom)
            || !matches!(
                route.get("runtime").and_then(Value::as_str),
                Some("muse" | "module")
            )
            || !route.get("enabled").is_some_and(Value::is_boolean))
    {
        return Err(Fault::InvalidState);
    }
    if let Some(field) = route.get("workspace_option") {
        let field = field.as_str().ok_or(Fault::InvalidState)?;
        let mut bytes = field.bytes();
        if field.len() > 128
            || !bytes
                .next()
                .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
            || !bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
        {
            return Err(Fault::InvalidState);
        }
    }
    if let Some(policy) = route.get("admission_policy") {
        let policy = policy.as_object().ok_or(Fault::InvalidState)?;
        if policy.len() != 1
            || !policy
                .get("max_concurrent_roots")
                .and_then(Value::as_u64)
                .is_some_and(|value| (1..=u16::MAX as u64).contains(&value))
        {
            return Err(Fault::InvalidState);
        }
    }
    // A non-null owned_service is an OpenCode-only launch definition, not a
    // Muse option or permission to retain provider/service configuration.
    if route
        .get("owned_service")
        .is_some_and(|value| !value.is_null())
    {
        return Err(Fault::InvalidState);
    }
    if let Some(options) = route.get("native_options") {
        let options = options.as_object().ok_or(Fault::InvalidState)?;
        if options.keys().any(|key| {
            ![
                "workspaceRoot",
                "nativeExecutable",
                "modelId",
                "providerId",
                "reasoningEffort",
                "approvalMode",
                "mcpServers",
            ]
            .contains(&key.as_str())
        }) {
            return Err(Fault::InvalidState);
        }
        for (key, value) in options {
            if key == "mcpServers" {
                continue;
            }
            let nullable = matches!(
                key.as_str(),
                "modelId" | "providerId" | "reasoningEffort" | "approvalMode"
            );
            if !(nullable && value.is_null()) && !value.as_str().is_some_and(atom) {
                return Err(Fault::InvalidState);
            }
        }
        if let Some(servers) = options.get("mcpServers") {
            validate_retained_mcp_profile(servers)?;
        }
    }
    let allowed: &[&str] = match command.method.as_str() {
        "agent.open" => &["client_request_id", "lane_id", "route"],
        "agent.send" => &[
            "client_request_id",
            "binding_id",
            "generation",
            "text",
            "delivery",
            "expected_turn_id",
            "prerequisite_operation_id",
        ],
        "task.dispatch" => &[
            "client_request_id",
            "attempt_id",
            "text",
            "delivery",
            "expected_turn_id",
            "prerequisite_operation_id",
            "launch_operation_id",
            "task_prompt",
            "task_dispatch_context",
        ],
        "agent.reply" => &["client_request_id", "binding_id", "generation", "reply"],
        "agent.configure" => &[
            "client_request_id",
            "binding_id",
            "generation",
            "settings",
            "prerequisite_operation_id",
        ],
        // Store's mutation validator/prerequisite producer also accepts the
        // prerequisite on agent.goal; it is not an arbitrary extension map.
        "agent.goal" => &[
            "client_request_id",
            "binding_id",
            "generation",
            "action",
            "objective",
            "expected_revision",
            "prerequisite_operation_id",
        ],
        "agent.recover" => &[
            "client_request_id",
            "binding_id",
            "generation",
            "expected_boot_id",
            "reason",
        ],
        "agent.reconcile" => &[
            "client_request_id",
            "binding_id",
            "generation",
            "operation_id",
        ],
        "agent.result" => &[
            "client_request_id", "binding_id", "generation", "selector",
            "offset_bytes", "length_bytes", "normalized_result_origin",
            "normalized_result_payload_identity",
        ],
        _ => return Err(Fault::InvalidState),
    };
    let input = command.input.as_object().ok_or(Fault::InvalidState)?;
    if input.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(Fault::InvalidState);
    }
    for key in [
        "client_request_id",
        "lane_id",
        "route",
        "binding_id",
        "attempt_id",
        "prerequisite_operation_id",
        "launch_operation_id",
        "operation_id",
        "expected_boot_id",
    ] {
        if input
            .get(key)
            .is_some_and(|value| !value.as_str().is_some_and(atom))
        {
            return Err(Fault::InvalidState);
        }
    }
    if input
        .get("binding_id")
        .is_some_and(|id| id.as_str() != Some(command.binding_id.as_str()))
        || input
            .get("generation")
            .is_some_and(|value| value.as_i64() != Some(command.generation))
        || input
            .get("expected_revision")
            .is_some_and(|value| value.as_i64().is_none_or(|n| n < 0))
        || input
            .get("reason")
            .is_some_and(|value| value.as_str().is_none_or(|s| s.trim().is_empty()))
    {
        return Err(Fault::ScopeMismatch);
    }
    if command.method == "agent.open"
        && (input.contains_key("lane_id") != input.contains_key("route")
            || input
                .get("route")
                .is_some_and(|alias| host_route && alias != &route["alias"]))
    {
        return Err(Fault::ScopeMismatch);
    }
    if host_route && !input.contains_key("client_request_id") {
        return Err(Fault::InvalidState);
    }
    if host_route
        && !matches!(command.method.as_str(), "agent.open" | "task.dispatch")
        && (!input.contains_key("binding_id") || !input.contains_key("generation"))
    {
        return Err(Fault::InvalidState);
    }
    if command.method == "agent.open" && host_route && !input.contains_key("lane_id") {
        return Err(Fault::InvalidState);
    }
    if command.method == "task.dispatch" && host_route && !input.contains_key("attempt_id") {
        return Err(Fault::InvalidState);
    }
    Ok(())
}

impl RetainedCommandContext {
    pub fn command(&self) -> &RuntimeCommand {
        &self.command
    }
    pub fn original_receipt(&self) -> &ModuleReceiptIdentity {
        &self.original_receipt
    }
    pub fn original_boot(&self) -> &str {
        &self.original_boot
    }
    pub fn native_command_id(&self) -> &str {
        &self.native_command_id
    }
    pub fn payload_sha256(&self) -> &str {
        &self.payload_sha256
    }
    pub fn requested(&self) -> &RetainedSelectors {
        &self.requested
    }
    pub fn work_fence(&self) -> Option<&RetainedWorkFence> {
        self.work.as_ref()
    }
    pub fn task_prompt(
        &self,
        scope: &Scope,
    ) -> Result<Option<commands::ValidatedTaskPrompt>, Fault> {
        match self.encoding(scope)? {
            commands::CommandEncoding::Native { task_prompt, .. } => {
                Ok(task_prompt.map(|prompt| *prompt))
            }
            _ => Err(Fault::InvalidState),
        }
    }
    fn encoding(&self, scope: &Scope) -> Result<commands::CommandEncoding, Fault> {
        scope.receipt(&self.original_receipt)?;
        if !atom(&self.original_boot)
            || self.command.operation_id != self.original_receipt.operation_id
            || self.command.input_sha256.as_deref()
                != Some(self.original_receipt.input_sha256.as_str())
        {
            return Err(Fault::ScopeMismatch);
        }
        if let Some(work) = &self.work
            && (work.session_id != scope.native_root_id
                || !matches!(work.status.as_str(), "idle" | "running" | "notLoaded")
                || work
                    .model_id
                    .iter()
                    .chain(work.provider_id.iter())
                    .chain(work.workspace_root.iter())
                    .chain(work.active_turn_id.iter())
                    .chain(work.standing_effort.iter())
                    .any(|s| !atom(s))
                || work.status == "idle" && work.active_turn_id.is_some()
                || work.status == "running" && work.active_turn_id.is_none()
                || self.command.route["native_options"]
                    .get("workspaceRoot")
                    .is_some_and(|v| v.as_str() != work.workspace_root.as_deref()))
        {
            return Err(Fault::InvalidState);
        }
        validate_host_envelope(scope, &self.command)?;
        let children = self.family.as_ref().map_or_else(Vec::new, |f| {
            f.edges.iter().map(|e| e.child_session_id.clone()).collect()
        });
        let pending = self
            .pending
            .as_ref()
            .map(|p| p.params.value())
            .transpose()?;
        let requested = self.requested.encoder();
        let cx = commands::EncodeContext {
            expected_receipt: &self.original_receipt,
            worker_boot_id: &self.original_boot,
            native_root_id: if self.command.method == "agent.open" {
                None
            } else {
                Some(scope.native_root_id.as_str())
            },
            fresh_root_session_id: if self.command.method == "agent.open" {
                Some(scope.native_root_id.as_str())
            } else {
                None
            },
            native_command_id: Some(&self.native_command_id),
            initialized: true,
            observed_children: &children,
            requested: &requested,
            work: self.work.as_ref().map(RetainedWorkFence::facts),
            pending_request: self
                .pending
                .as_ref()
                .zip(pending.as_ref())
                .map(|(p, params)| commands::PendingRequestFacts {
                    method: &p.method,
                    params,
                    sampled_event_version: p.revision,
                    current_event_version: p.revision,
                }),
            recovery: Some(commands::RecoveryFacts {
                durability: commands::SessionDurability::Durable,
                native_ready: false,
            }),
            validated_result_input: None,
            session_mcp_granted: self.session_mcp_granted,
        };
        commands::encode(&self.command, &cx).map_err(|_| Fault::InvalidState)
    }
    fn validate(&self, scope: &Scope, invocation: &NativeInvocation) -> Result<(), Fault> {
        if self.native_command_id != invocation.command_id
            || self.payload_sha256 != invocation.payload_sha256()
        {
            return Err(Fault::IntentConflict);
        }
        if let Some(family) = &self.family {
            family.validate(
                scope,
                invocation.params()?["sessionId"]
                    .as_str()
                    .ok_or(Fault::InvalidState)?,
            )?;
        }
        match self.encoding(scope)? {
            commands::CommandEncoding::Native {
                request, requested, ..
            } if request.method == invocation.method
                && request.params == invocation.params()? =>
            {
                if let Some(requested) = requested
                    && requested != self.requested.encoder()
                {
                    return Err(Fault::IntentConflict);
                }
                Ok(())
            }
            _ => Err(Fault::IntentConflict),
        }
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FamilyEdge {
    parent_session_id: String,
    child_session_id: String,
    item_id: String,
    parent_turn_id: Option<String>,
    item_revision: u64,
    kind: String,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FamilyProof {
    projection_sha256: Sha256Digest,
    edges: Vec<FamilyEdge>,
}
impl FamilyProof {
    fn validate(&self, scope: &Scope, target: &str) -> Result<(), Fault> {
        let mut parent = scope.native_root_id.as_str();
        let mut seen = BTreeSet::new();
        seen.insert(parent);
        if self.edges.len() > 128 {
            return Err(Fault::Capacity);
        }
        for edge in &self.edges {
            if edge.parent_session_id != parent
                || !atom(&edge.child_session_id)
                || !atom(&edge.item_id)
                || edge.item_revision == 0
                || !matches!(edge.kind.as_str(), "subagent" | "reminderChild")
                || !seen.insert(&edge.child_session_id)
                || edge.parent_turn_id.as_ref().is_some_and(|t| !atom(t))
            {
                return Err(Fault::ProvenanceRequired);
            }
            parent = &edge.child_session_id;
        }
        if parent != target {
            return Err(Fault::ProvenanceRequired);
        }
        Ok(())
    }
}
/// Opaque, bounded projection from the shared observation reducer. There is no
/// public constructor from an arbitrary session argument or caller-authored JSON.
#[derive(Clone)]
pub struct ProjectionSnapshot {
    checkpoint: SavedJson,
    capture: CaptureStamp,
}
impl ProjectionSnapshot {
    fn from_projector(
        projector: &ObservationProjector,
        capture: CaptureStamp,
    ) -> Result<Self, Fault> {
        Ok(Self {
            checkpoint: SavedJson::new(&projector.checkpoint().map_err(|_| Fault::InvalidState)?)?,
            capture,
        })
    }
}
/// Both opaque boundaries are retained together. complete is only this parent's
/// scan status; it is never whole-family completion or permission to discard.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildDiscovery {
    pub parent_session_id: String,
    pub after: Option<String>,
    pub next: Option<String>,
    pub complete: bool,
}
impl ChildDiscovery {
    fn validate(&self) -> Result<(), Fault> {
        if !atom(&self.parent_session_id)
            || self
                .after
                .iter()
                .chain(self.next.iter())
                .any(|s| s.len() > 8192 || s.chars().any(char::is_control))
            || self.complete != self.next.is_none()
            || self.next.is_some() && self.after == self.next
        {
            return Err(Fault::InvalidState);
        }
        Ok(())
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingInventory {
    session_id: String,
    sampled_revision: u64,
    inventory: SavedJson,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    capture: Option<CaptureStamp>,
}
impl PendingInventory {
    /// before/after are sampled under the Driver's event mutex around the read.
    fn from_read(
        session_id: &str,
        before: u64,
        after: u64,
        inventory: Value,
    ) -> Result<Self, Fault> {
        if !atom(session_id) || before != after {
            return Err(Fault::FreshChecksRequired);
        }
        definition("ApprovalListPendingResult", &inventory)?;
        let mut seen = BTreeSet::new();
        for (key, id) in [("approvals", "approvalId"), ("userInputs", "userInputId")] {
            for row in inventory[key].as_array().ok_or(Fault::InvalidState)? {
                let identity = row[id]
                    .as_str()
                    .filter(|id| atom(id))
                    .ok_or(Fault::InvalidState)?;
                if row["sessionId"] != session_id || !seen.insert((key, identity)) {
                    return Err(Fault::ProvenanceRequired);
                }
            }
        }
        Ok(Self {
            session_id: session_id.into(),
            sampled_revision: before,
            inventory: SavedJson::new(&inventory)?,
            capture: None,
        })
    }
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
    pub fn value(&self) -> Result<Value, Fault> {
        self.inventory.value()
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservationBundle {
    source_boot: String,
    projection: SavedJson,
    discovery: Vec<ChildDiscovery>,
    inventories: Vec<PendingInventory>,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaptureStamp {
    instance: String,
    connection: u64,
    epoch: u64,
}
/// Issued only from a retained CURRENT connection projection. A checkpoint
/// restore, connection change, event invalidation or native write revokes it.
#[derive(Clone)]
pub struct SessionAuthority {
    session_id: String,
    instance: String,
    connection: u64,
    epoch: u64,
    scope_sha256: Sha256Digest,
    proof: FamilyProof,
}
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RestoredFreshness {
    Stale,
}
pub struct RestoredObservations {
    pub projection: Value,
    pub discovery: Vec<ChildDiscovery>,
    pub inventories: Vec<PendingInventory>,
    pub freshness: RestoredFreshness,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Operation {
    receipt: ModuleReceiptIdentity,
    native: Option<NativePending>,
    final_delivery: Option<Delivery>,
    final_host_acked: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    family: Option<FamilyProof>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result_command: Option<SavedJson>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Slot {
    revision: u64,
    delivery: Delivery,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    revision: u64,
    durability: Option<DurabilityFact>,
    operations: BTreeMap<String, Operation>,
    outbox: BTreeMap<String, Slot>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    contexts: BTreeMap<String, RetainedCommandContext>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    observations: Option<ObservationBundle>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    legacy_operations: BTreeSet<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeFile {
    format: String,
    version: u16,
    scope: Scope,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    format: String,
    version: u16,
    scope: Scope,
    state: State,
    sha256: Sha256Digest,
}

/// Private delivery token. A stale ACK can never retire a newer revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryTicket {
    operation_id: String,
    slot: Slot,
}
impl DeliveryTicket {
    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }
    pub fn method(&self) -> &'static str {
        self.slot.delivery.method.method()
    }
    pub fn params_json(&self) -> &str {
        &self.slot.delivery.params.utf8
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckDisposition {
    Retired,
    Superseded,
    AlreadyRetired,
}

/// Borrow excludes other checkpoint changes until the caller synchronously
/// enqueues this exact command. It grants no Store/native authorization. Drop
/// does not prove no write: the retained intent remains potentially sent.
pub struct PreparedNative<'a> {
    journal: &'a mut Journal,
    operation_id: String,
}
impl PreparedNative<'_> {
    pub fn invocation(&self) -> &NativeInvocation {
        &self.journal.state.operations[&self.operation_id]
            .native
            .as_ref()
            .expect("retained intent")
            .invocation
    }
}
pub enum Reconciliation<'a> {
    Write(PreparedNative<'a>),
    AckReadback(Value),
    AwaitingHostReceipt,
    Finalized,
    Blocked(String),
}

/// One instance per owner/worker, serialized through mutable access. The caller
/// must not open concurrent writers; the existing owner holds the directory OS
/// lock. No second lock, scheduler, Task DB, or automatic retention GC is added.
/// Capacity is explicit until safe result/reference reclamation is implemented.
pub struct Journal {
    path: PathBuf,
    scope: Scope,
    boot: String,
    state: State,
    poisoned: bool,
    // Read freshness is connection-local, never restored from a checkpoint.
    exact_root_read: bool,
    restored: bool,
    initialized: bool,
    durability_observed: bool,
    session_mcp_granted: bool,
    connection: u64,
    observation_epoch: u64,
    observations_fresh: bool,
    fresh_work: Option<RetainedWorkFence>,
    instance: String,
}

impl Journal {
    /// Only for a new Rust binding. The caller must have selected a native root
    /// identity; this API supplies no native-existence or old-family-gone proof.
    pub fn create_owned(scope: Scope) -> Result<Self, Fault> {
        let (dir, boot) = owned_directory(&scope)?;
        Self::create_at(&dir, scope, boot)
    }
    /// Never falls back to fresh state. Existing owner/custody admission occurs
    /// in swarm-process before this method can access the private directory.
    pub fn open_owned(scope: Scope) -> Result<Self, Fault> {
        let (dir, boot) = owned_directory(&scope)?;
        Self::open_at(&dir, scope, boot)
    }
    #[cfg(test)]
    pub(crate) fn create_fixture(dir: &Path, scope: Scope, boot: String) -> Result<Self, Fault> {
        Self::create_at(dir, scope, boot)
    }

    fn create_at(dir: &Path, scope: Scope, boot: String) -> Result<Self, Fault> {
        scope.validate()?;
        let instance = instance_token(&boot)?;
        reject_legacy(dir)?;
        if present(&dir.join(MARKER))? || present(&dir.join(CHECKPOINT))? {
            return Err(Fault::StateAlreadyExists);
        }
        let marker = ScopeFile {
            format: FORMAT.into(),
            version: VERSION,
            scope: scope.clone(),
        };
        write_private_new(
            &dir.join(MARKER),
            &serde_json::to_vec(&marker).map_err(|_| Fault::InvalidState)?,
        )
        .map_err(|_| Fault::PersistenceUnknown)?;
        let state = State {
            revision: 0,
            durability: None,
            operations: BTreeMap::new(),
            outbox: BTreeMap::new(),
            contexts: BTreeMap::new(),
            observations: None,
            legacy_operations: BTreeSet::new(),
        };
        let path = dir.join(CHECKPOINT);
        write_private_new(&path, &encode(&scope, &state)?)
            .map_err(|_| Fault::PersistenceUnknown)?;
        Ok(Self {
            path,
            scope,
            boot,
            state,
            poisoned: false,
            exact_root_read: false,
            restored: false,
            initialized: false,
            durability_observed: false,
            session_mcp_granted: false,
            connection: 0,
            observation_epoch: 0,
            observations_fresh: false,
            fresh_work: None,
            instance,
        })
    }
    fn open_at(dir: &Path, scope: Scope, boot: String) -> Result<Self, Fault> {
        scope.validate()?;
        let instance = instance_token(&boot)?;
        reject_legacy(dir)?;
        if !present(&dir.join(MARKER))? || !present(&dir.join(CHECKPOINT))? {
            return Err(Fault::MissingState);
        }
        let marker: ScopeFile = serde_json::from_slice(
            &read_private_durable(&dir.join(MARKER), MAX_FILE)
                .map_err(|_| Fault::PersistenceUnknown)?,
        )
        .map_err(|_| Fault::InvalidState)?;
        if marker.format != FORMAT || marker.version != VERSION || marker.scope != scope {
            return Err(Fault::ScopeMismatch);
        }
        let path = dir.join(CHECKPOINT);
        let bytes = read_private_durable(&path, MAX_FILE).map_err(|_| Fault::PersistenceUnknown)?;
        let raw: Value = serde_json::from_slice(&bytes).map_err(|_| Fault::InvalidState)?;
        let mut checkpoint: Checkpoint =
            serde_json::from_value(raw.clone()).map_err(|_| Fault::InvalidState)?;
        if checkpoint.format != FORMAT
            || ![VERSION, CHECKPOINT_VERSION].contains(&checkpoint.version)
            || checkpoint.scope != scope
        {
            return Err(Fault::ScopeMismatch);
        }
        if checkpoint.sha256
            != digest(
                &json!({"format":FORMAT,"version":checkpoint.version,"scope":scope,"state":raw["state"]}),
            )?
            || serde_json::to_value(&checkpoint.state).map_err(|_| Fault::InvalidState)?
                != raw["state"]
        {
            return Err(Fault::InvalidState);
        }
        if checkpoint.version == VERSION {
            // Historical NEW-Rust state stays readable/deliverable. It carries
            // no validated v2 context; never infer missing facts or replay it.
            if !checkpoint.state.contexts.is_empty()
                || checkpoint.state.observations.is_some()
                || !checkpoint.state.legacy_operations.is_empty()
            {
                return Err(Fault::InvalidState);
            }
            checkpoint.state.legacy_operations =
                checkpoint.state.operations.keys().cloned().collect();
        }
        validate_state(&scope, &checkpoint.state)?;
        Ok(Self {
            path,
            scope,
            boot,
            state: checkpoint.state,
            poisoned: false,
            exact_root_read: false,
            restored: true,
            initialized: false,
            durability_observed: false,
            session_mcp_granted: false,
            connection: 0,
            observation_epoch: 0,
            observations_fresh: false,
            fresh_work: None,
            instance,
        })
    }
    fn healthy(&self, scope: &Scope) -> Result<(), Fault> {
        if self.poisoned {
            return Err(Fault::Poisoned);
        }
        if scope != &self.scope {
            return Err(Fault::ScopeMismatch);
        }
        Ok(())
    }
    fn commit(&mut self, mut next: State) -> Result<(), Fault> {
        if self.poisoned {
            return Err(Fault::Poisoned);
        }
        next.revision = self.state.revision.checked_add(1).ok_or(Fault::Capacity)?;
        validate_state(&self.scope, &next)?;
        let bytes = encode(&self.scope, &next)?;
        if replace_private_durable(&self.path, &bytes).is_err() {
            self.poisoned = true;
            return Err(Fault::PersistenceUnknown);
        }
        self.state = next;
        Ok(())
    }

    fn invalidate_freshness(&mut self) -> Result<(), Fault> {
        self.observations_fresh = false;
        self.fresh_work = None;
        self.exact_root_read = false;
        let Some(epoch) = self.observation_epoch.checked_add(1) else {
            self.poisoned = true;
            return Err(Fault::Capacity);
        };
        self.observation_epoch = epoch;
        Ok(())
    }
    fn capture_stamp(&self) -> CaptureStamp {
        CaptureStamp {
            instance: self.instance.clone(),
            connection: self.connection,
            epoch: self.observation_epoch,
        }
    }
    /// Capture and retain under the same Driver event mutex, with no await in
    /// between. Events revoke the capture, so an old fold cannot regain freshness.
    pub(crate) fn capture_projection(
        &self,
        scope: &Scope,
        projector: &ObservationProjector,
    ) -> Result<ProjectionSnapshot, Fault> {
        self.healthy(scope)?;
        if !self.initialized {
            return Err(Fault::FreshChecksRequired);
        }
        ProjectionSnapshot::from_projector(projector, self.capture_stamp())
    }
    /// Call after a stable listPending read, using the authority held across
    /// that read. Retained inventories from an earlier instance cannot be reused.
    pub fn capture_pending_inventory(
        &mut self,
        scope: &Scope,
        authority: &SessionAuthority,
        before: u64,
        after: u64,
        value: Value,
    ) -> Result<PendingInventory, Fault> {
        if !self.authority_is_current(scope, authority) {
            return Err(Fault::FreshChecksRequired);
        }
        let mut inventory =
            match PendingInventory::from_read(&authority.session_id, before, after, value) {
                Ok(value) => value,
                Err(error) => {
                    self.invalidate_freshness()?;
                    return Err(error);
                }
            };
        inventory.capture = Some(self.capture_stamp());
        Ok(inventory)
    }
    /// Driver calls under its event/mutation mutex BEFORE reducing any native
    /// event or changing connections. Coarse revocation is intentionally safe.
    pub fn invalidate_observations(&mut self, scope: &Scope) -> Result<(), Fault> {
        self.healthy(scope)?;
        self.invalidate_freshness()
    }
    /// End the actual initialized connection without rewriting historical ACKs
    /// or durability facts. A late read cannot restore current capabilities.
    pub fn end_connection(&mut self, scope: &Scope) -> Result<(), Fault> {
        self.healthy(scope)?;
        self.initialized = false;
        self.durability_observed = false;
        self.session_mcp_granted = false;
        self.invalidate_freshness()
    }
    /// Call only after the correlated initialize result and initialized flush
    /// on the same connection. Validate the full result, persist only durability;
    /// museHome/userAgent/provider settings are never copied into the journal.
    pub fn record_initialize(
        &mut self,
        scope: &Scope,
        observed_namespace: &str,
        initialize: &Value,
    ) -> Result<(), Fault> {
        self.healthy(scope)?;
        self.initialized = false;
        self.durability_observed = false;
        self.session_mcp_granted = false;
        self.invalidate_freshness()?;
        if observed_namespace != scope.native_namespace {
            return Err(Fault::ScopeMismatch);
        }
        let profile = Durability::from_initialize_declaration(initialize.get("sessionDurability"));
        let mut compatible = initialize.clone();
        if profile == Durability::Unrecognized {
            compatible
                .as_object_mut()
                .ok_or(Fault::InvalidState)?
                .remove("sessionDurability");
        }
        schema::validate_definition("InitializeResult", &compatible)
            .map_err(|_| Fault::InvalidState)?;
        if initialize["schema"]["version"] != 1 {
            return Err(Fault::InvalidState);
        }
        self.record_durability(scope, profile)?;
        self.initialized = true;
        self.session_mcp_granted = initialize["grantedCapabilities"]
            .as_array()
            .is_some_and(|grants| grants.iter().any(|grant| grant == "sessionMcp"));
        Ok(())
    }
    /// Stable exact-root read, not load/attach/list. The Driver supplies before
    /// and after event versions sampled around this actual native read.
    pub fn record_work_read(
        &mut self,
        scope: &Scope,
        before: u64,
        after: u64,
        read: &Value,
        model_state: RetainedModelState,
        standing_effort: Option<String>,
    ) -> Result<RetainedWorkFence, Fault> {
        self.healthy(scope)?;
        self.invalidate_freshness()?;
        if !self.initialized || before != after {
            return Err(Fault::FreshChecksRequired);
        }
        definition("SessionReadResult", read)?;
        if read["session"]["sessionId"] != scope.native_root_id
            || read["history"]["mode"] != "none"
            || !read["history"]["items"].is_null()
            || !read["history"]["snapshot"].is_null()
        {
            return Err(Fault::ScopeMismatch);
        }
        let session = &read["session"];
        let sample = RetainedWorkFence {
            session_id: scope.native_root_id.clone(),
            model_id: session["modelId"].as_str().map(str::to_owned),
            provider_id: session["providerId"].as_str().map(str::to_owned),
            workspace_root: session["workspaceRoot"].as_str().map(str::to_owned),
            status: session["status"]
                .as_str()
                .ok_or(Fault::InvalidState)?
                .into(),
            active_turn_id: session["activeTurnId"].as_str().map(str::to_owned),
            model_state,
            standing_effort,
            sampled_revision: before,
        };
        if sample.status == "idle" && sample.active_turn_id.is_some()
            || sample.status == "running" && sample.active_turn_id.is_none()
        {
            return Err(Fault::InvalidState);
        }
        self.exact_root_read = true;
        self.fresh_work = Some(sample.clone());
        Ok(sample)
    }
    /// One atomic checkpoint pairs the SHARED projector with its parent cursor
    /// boundaries and exact pending inventories. No second state file exists.
    pub fn retain_observations(
        &mut self,
        scope: &Scope,
        projection: ProjectionSnapshot,
        discovery: Vec<ChildDiscovery>,
        inventories: Vec<PendingInventory>,
    ) -> Result<(), Fault> {
        self.healthy(scope)?;
        if !self.initialized {
            return Err(Fault::FreshChecksRequired);
        }
        if projection.capture != self.capture_stamp()
            || inventories
                .iter()
                .any(|i| i.capture.as_ref() != Some(&self.capture_stamp()))
        {
            return Err(Fault::FreshChecksRequired);
        }
        let bundle = ObservationBundle {
            source_boot: self.boot.clone(),
            projection: projection.checkpoint,
            discovery,
            inventories,
        };
        if let Err(error) = validate_bundle(scope, &bundle) {
            self.invalidate_freshness()?;
            return Err(error);
        }
        self.invalidate_freshness()?;
        let mut next = self.state.clone();
        next.observations = Some(bundle);
        self.commit(next)?;
        self.observations_fresh = true;
        Ok(())
    }
    /// Restore through the shared reducer's stale path, preserving parent
    /// provenance/cursor pairs and pending payloads only as historical facts.
    /// No authority ticket can be obtained until CURRENT facts are retained.
    pub fn restored_observations(
        &self,
        scope: &Scope,
    ) -> Result<Option<RestoredObservations>, Fault> {
        self.healthy(scope)?;
        let Some(bundle) = &self.state.observations else {
            return Ok(None);
        };
        let old = projection_fence(scope, &bundle.source_boot);
        let restored = ObservationProjector::restore(old, bundle.projection.value()?)
            .map_err(|_| Fault::InvalidState)?;
        let mut checkpoint = restored.checkpoint().map_err(|_| Fault::InvalidState)?;
        checkpoint["fence"]["bridge_boot_id"] = json!(self.boot);
        let current =
            ObservationProjector::restore(projection_fence(scope, &self.boot), checkpoint)
                .map_err(|_| Fault::InvalidState)?;
        Ok(Some(RestoredObservations {
            projection: current.checkpoint().map_err(|_| Fault::InvalidState)?,
            discovery: bundle.discovery.clone(),
            inventories: bundle.inventories.clone(),
            freshness: RestoredFreshness::Stale,
        }))
    }
    pub fn authorize_session(
        &self,
        scope: &Scope,
        session_id: &str,
    ) -> Result<SessionAuthority, Fault> {
        self.healthy(scope)?;
        if !self.initialized {
            return Err(Fault::FreshChecksRequired);
        }
        let proof = if self.observations_fresh {
            family_proof(
                scope,
                &self
                    .state
                    .observations
                    .as_ref()
                    .ok_or(Fault::ProvenanceRequired)?
                    .projection
                    .value()?,
                session_id,
                true,
            )?
        } else if session_id == scope.native_root_id && self.fresh_work.is_some() {
            FamilyProof {
                projection_sha256: digest(
                    &serde_json::to_value(&self.fresh_work).map_err(|_| Fault::InvalidState)?,
                )?,
                edges: Vec::new(),
            }
        } else {
            return Err(Fault::FreshChecksRequired);
        };
        Ok(SessionAuthority {
            session_id: session_id.into(),
            instance: self.instance.clone(),
            connection: self.connection,
            epoch: self.observation_epoch,
            scope_sha256: digest(&serde_json::to_value(scope).map_err(|_| Fault::InvalidState)?)?,
            proof,
        })
    }
    /// Use as results reader's authority callback before AND after every read.
    pub fn authority_is_current(&self, scope: &Scope, authority: &SessionAuthority) -> bool {
        self.healthy(scope).is_ok()
            && self.initialized
            && authority.instance == self.instance
            && authority.connection == self.connection
            && authority.epoch == self.observation_epoch
            && digest(&serde_json::to_value(scope).unwrap_or(Value::Null))
                .is_ok_and(|hash| hash == authority.scope_sha256)
            && authority
                .proof
                .validate(scope, &authority.session_id)
                .is_ok()
    }
    pub fn retained_command(
        &self,
        scope: &Scope,
        operation_id: &str,
    ) -> Result<Option<&RetainedCommandContext>, Fault> {
        self.healthy(scope)?;
        Ok(self.state.contexts.get(operation_id))
    }
    pub(crate) fn result_context(
        &self,
        scope: &Scope,
        receipt: &ModuleReceiptIdentity,
        command: &RuntimeCommand,
    ) -> Result<crate::results::NormalizedResultContext, Fault> {
        self.healthy(scope)?;
        result_context(scope, &self.state, receipt, command)
    }
    pub fn retained_invocation(
        &self,
        scope: &Scope,
        operation_id: &str,
    ) -> Result<&NativeInvocation, Fault> {
        self.healthy(scope)?;
        self.state
            .operations
            .get(operation_id)
            .and_then(|op| op.native.as_ref())
            .map(|n| &n.invocation)
            .ok_or(Fault::IntentConflict)
    }
    /// The only public construction path for session/start.config. The grant
    /// comes from this journal's CURRENT validated initialize result, never a
    /// caller boolean/requested capability. Closed schema validation retains
    /// only the admitted mcpServers union; preparation/replay recheck the grant.
    /// Retention additionally refuses every non-empty headers/env map; no
    /// header-name/token-name blacklist or auth-file resolution is performed.
    pub fn native_invocation(
        &self,
        scope: &Scope,
        method: &str,
        command_id: &str,
        params: Map<String, Value>,
    ) -> Result<NativeInvocation, Fault> {
        self.healthy(scope)?;
        if params.contains_key("config") && (!self.initialized || !self.session_mcp_granted) {
            return Err(Fault::GrantRequired);
        }
        NativeInvocation::from_admitted_params(scope, method, command_id, params)
    }
    /// Persist context AND intent in one replacement before returning a permit.
    /// Required Driver entry for durable cross-worker commands. The caller must
    /// still check current Host authorization immediately at native enqueue.
    pub fn prepare_command(
        &mut self,
        scope: &Scope,
        receipt: ModuleReceiptIdentity,
        command: RuntimeCommand,
        invocation: NativeInvocation,
        requested: RetainedSelectors,
    ) -> Result<PreparedNative<'_>, Fault> {
        self.healthy(scope)?;
        if !self.initialized {
            return Err(Fault::FreshChecksRequired);
        }
        invocation.validate(scope)?;
        let params = invocation.params()?;
        let target = params["sessionId"].as_str().ok_or(Fault::InvalidState)?;
        let authority = if command.method == "agent.open" {
            None
        } else {
            Some(self.authorize_session(scope, target)?)
        };
        let pending = if command.method == "agent.reply" {
            Some(self.pending_for(&invocation)?)
        } else {
            None
        };
        let needs_work = matches!(
            command.method.as_str(),
            "agent.send" | "task.dispatch" | "agent.recover"
        ) || command.method == "agent.goal"
            && matches!(
                command.input["action"].as_str(),
                Some("set" | "edit" | "resume")
            );
        let work = if needs_work {
            Some(self.fresh_work.clone().ok_or(Fault::FreshChecksRequired)?)
        } else {
            None
        };
        if params.contains_key("config") && !self.session_mcp_granted {
            return Err(Fault::GrantRequired);
        }
        let mut context = RetainedCommandContext {
            command,
            original_receipt: receipt.clone(),
            original_boot: self.boot.clone(),
            native_command_id: invocation.command_id.clone(),
            payload_sha256: invocation.payload_sha256().into(),
            requested,
            work,
            pending,
            family: authority.map(|a| a.proof),
            session_mcp_granted: self.session_mcp_granted,
        };
        if let commands::CommandEncoding::Native {
            requested: Some(selectors),
            ..
        } = context.encoding(scope)?
        {
            context.requested = RetainedSelectors {
                model_id: selectors.model_id,
                provider_id: selectors.provider_id,
                reasoning_effort: selectors.reasoning_effort,
            };
        }
        context.validate(scope, &invocation)?;
        self.prepare(scope, receipt, invocation, Some(context))
    }
    fn pending_for(&self, invocation: &NativeInvocation) -> Result<RetainedPending, Fault> {
        if !self.observations_fresh {
            return Err(Fault::FreshChecksRequired);
        }
        let params = invocation.params()?;
        let approval = invocation.method == "approval/decide";
        let (list, id, method) = if approval {
            ("approvals", "approvalId", "approval/request")
        } else {
            ("userInputs", "userInputId", "userInput/request")
        };
        for inventory in &self
            .state
            .observations
            .as_ref()
            .ok_or(Fault::ProvenanceRequired)?
            .inventories
        {
            if params["sessionId"] != inventory.session_id {
                continue;
            }
            let value = inventory.value()?;
            if let Some(row) = value[list]
                .as_array()
                .and_then(|rows| rows.iter().find(|row| row[id] == params[id]))
            {
                return Ok(RetainedPending {
                    method: method.into(),
                    params: SavedJson::new(row)?,
                    revision: inventory.sampled_revision,
                });
            }
        }
        Err(Fault::ProvenanceRequired)
    }
    fn validate_replay(
        &self,
        scope: &Scope,
        context: &RetainedCommandContext,
        invocation: &NativeInvocation,
    ) -> Result<(), Fault> {
        if !self.initialized {
            return Err(Fault::FreshChecksRequired);
        }
        context.validate(scope, invocation)?;
        let params = invocation.params()?;
        if params.contains_key("config") && !self.session_mcp_granted {
            return Err(Fault::GrantRequired);
        }
        if invocation.method != "session/start" {
            let current = self.authorize_session(
                scope,
                params["sessionId"].as_str().ok_or(Fault::InvalidState)?,
            )?;
            if context
                .family
                .as_ref()
                .is_some_and(|original| original.edges != current.proof.edges)
            {
                return Err(Fault::ProvenanceRequired);
            }
        }
        if let Some(original) = &context.work {
            if self
                .fresh_work
                .as_ref()
                .is_none_or(|fresh| !original.same_work(fresh))
            {
                return Err(Fault::FreshChecksRequired);
            }
            commands::validate_requested_readback(
                &context.requested.encoder(),
                original.model_id.as_deref(),
                original.provider_id.as_deref(),
            )
            .map_err(|_| Fault::FreshChecksRequired)?;
        }
        if let Some(original) = &context.pending {
            let fresh = self.pending_for(invocation)?;
            if fresh.method != original.method || fresh.params != original.params {
                return Err(Fault::FreshChecksRequired);
            }
        }
        Ok(())
    }
    /// Explicit new Host operation targets the ORIGINAL immutable receipt hash.
    /// A lost session/start ACK retains rootless original command identity; no
    /// adopt/start/fork fallback or replacement TaskPrompt is synthesized.
    pub fn reconcile_command(
        &mut self,
        scope: &Scope,
        request: &RuntimeCommand,
        request_receipt: &ModuleReceiptIdentity,
    ) -> Result<Reconciliation<'_>, Fault> {
        self.healthy(scope)?;
        scope.receipt(request_receipt)?;
        let target = request.input["operation_id"]
            .as_str()
            .ok_or(Fault::InvalidState)?;
        let context = self
            .state
            .contexts
            .get(target)
            .ok_or(Fault::ContextRequired)?;
        if request.method != "agent.reconcile"
            || request.operation_id == target
            || request.operation_id != request_receipt.operation_id
            || request.input_sha256.as_deref() != Some(request_receipt.input_sha256.as_str())
            || request.binding_id != scope.binding_id
            || request.generation != scope.generation
            || request.route["module_artifact_id"] != scope.artifact.artifact_id.as_str()
            || request.target_input_sha256.as_deref()
                != Some(context.original_receipt.input_sha256.as_str())
            || request
                .native_root_id
                .as_deref()
                .is_some_and(|root| root != scope.native_root_id)
            || request.native_root_id.is_none() && context.command.method != "agent.open"
        {
            return Err(Fault::ScopeMismatch);
        }
        validate_host_envelope(scope, request)?;
        let hash = context.payload_sha256.clone();
        self.explicit_reconcile(scope, target, &hash)
    }
    pub fn record_durability(&mut self, scope: &Scope, profile: Durability) -> Result<(), Fault> {
        self.healthy(scope)?;
        // Called for each newly initialized native connection. An old read
        // cannot qualify a different connection, even in the same worker boot.
        self.exact_root_read = false;
        self.initialized = false;
        self.durability_observed = false;
        self.session_mcp_granted = false;
        self.connection = self.connection.checked_add(1).ok_or(Fault::Capacity)?;
        self.invalidate_freshness()?;
        let mut next = self.state.clone();
        next.durability = Some(DurabilityFact {
            profile,
            observed_boot: self.boot.clone(),
        });
        self.commit(next)?;
        self.durability_observed = true;
        Ok(())
    }
    /// Caller has completed an actual session/read without load/attach and
    /// validated its native namespace/model/location. This retains only the
    /// exact root identity check; it does not itself contact or adopt a session.
    pub fn record_exact_root_read(
        &mut self,
        scope: &Scope,
        observed_session_id: &str,
    ) -> Result<(), Fault> {
        self.healthy(scope)?;
        self.exact_root_read = false;
        if observed_session_id != scope.native_root_id.as_str() {
            return Err(Fault::ScopeMismatch);
        }
        self.exact_root_read = true;
        Ok(())
    }
    /// Driver must have flushed initialized on its current MSP connection.
    /// agent.open uses this same durable ledger with session/start and an
    /// already chosen fresh root sessionId, never a special unjournaled send.
    pub fn prepare_native(
        &mut self,
        scope: &Scope,
        receipt: ModuleReceiptIdentity,
        invocation: NativeInvocation,
    ) -> Result<PreparedNative<'_>, Fault> {
        // Compatibility seam for same-worker root-only callers. Driver must
        // use prepare_command for retained context, MCP config and children.
        self.healthy(scope)?;
        if self.restored && !self.state.operations.contains_key(&receipt.operation_id) {
            return Err(Fault::ContextRequired);
        }
        if invocation.params()?["sessionId"] != scope.native_root_id {
            return Err(Fault::ProvenanceRequired);
        }
        if invocation.params()?.contains_key("config") {
            return Err(Fault::GrantRequired);
        }
        self.prepare(scope, receipt, invocation, None)
    }
    fn prepare(
        &mut self,
        scope: &Scope,
        receipt: ModuleReceiptIdentity,
        invocation: NativeInvocation,
        context: Option<RetainedCommandContext>,
    ) -> Result<PreparedNative<'_>, Fault> {
        self.healthy(scope)?;
        scope.receipt(&receipt)?;
        invocation.validate(scope)?;
        if let Some(old) = self.state.operations.get(&receipt.operation_id) {
            if old.receipt != receipt {
                return Err(Fault::IntentConflict);
            }
            if old
                .native
                .as_ref()
                .is_some_and(|native| native.invocation != invocation)
            {
                return Err(Fault::IntentConflict);
            }
            // A prior unknown outcome without native intent is a recovery gap,
            // not permission to mint missing command identity and send afresh.
            return Err(Fault::ExplicitReconcileRequired);
        }
        if invocation.method == "session/resume"
            && (!self.durability_observed
                || self.state.durability.as_ref().is_none_or(|fact| {
                    fact.profile != Durability::Durable || fact.observed_boot != self.boot
                }))
        {
            return Err(Fault::DurabilityUnavailable);
        }
        let resume = invocation.method == "session/resume";
        if resume && !self.exact_root_read {
            return Err(Fault::ExactRootReadRequired);
        }
        let operation_id = receipt.operation_id.clone();
        let mut next = self.state.clone();
        let operation = next
            .operations
            .entry(operation_id.clone())
            .or_insert_with(|| Operation {
                receipt,
                native: None,
                final_delivery: None,
                final_host_acked: false,
                family: context.as_ref().and_then(|c| c.family.clone()),
                result_command: None,
            });
        operation.native = Some(NativePending {
            invocation,
            submission_boot: self.boot.clone(),
            ack: None,
            reconcile_block: None,
        });
        if let Some(context) = context {
            next.contexts.insert(operation_id.clone(), context);
        }
        self.commit(next)?;
        self.invalidate_freshness()?;
        Ok(PreparedNative {
            journal: self,
            operation_id,
        })
    }
    /// Records the bounded native admission ACK only; application/readback is a
    /// distinct facade decision. Replay must return the same retained ACK.
    pub fn record_native_ack(
        &mut self,
        scope: &Scope,
        operation_id: &str,
        command_id: &str,
        ack: Value,
    ) -> Result<(), Fault> {
        self.healthy(scope)?;
        let mut next = self.state.clone();
        let native = next
            .operations
            .get_mut(operation_id)
            .and_then(|operation| operation.native.as_mut())
            .ok_or(Fault::IntentConflict)?;
        validate_ack(scope, &native.invocation, &ack)?;
        if command_id != native.invocation.command_id {
            return Err(Fault::IntentConflict);
        }
        let ack = SavedJson::new(&ack)?;
        if let Some(old) = &native.ack {
            return if old == &ack {
                Ok(())
            } else {
                Err(Fault::AckConflict)
            };
        }
        native.ack = Some(ack);
        self.commit(next)
    }
    /// Retains a failed work/model fence without claiming no earlier effect.
    pub fn block_reconciliation(
        &mut self,
        scope: &Scope,
        operation_id: &str,
        code: &str,
    ) -> Result<(), Fault> {
        self.healthy(scope)?;
        if code.is_empty()
            || code.len() > 128
            || !code
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
        {
            return Err(Fault::InvalidState);
        }
        let mut next = self.state.clone();
        next.operations
            .get_mut(operation_id)
            .and_then(|operation| operation.native.as_mut())
            .ok_or(Fault::IntentConflict)?
            .reconcile_block = Some(code.into());
        self.commit(next)
    }
    /// Explicit caller request only. Reopen, outbox reads and host ACKs never
    /// call this. Fresh authorization/work fences remain mandatory at enqueue.
    pub fn explicit_reconcile(
        &mut self,
        scope: &Scope,
        operation_id: &str,
        expected_payload_sha256: &str,
    ) -> Result<Reconciliation<'_>, Fault> {
        self.healthy(scope)?;
        let operation = self
            .state
            .operations
            .get(operation_id)
            .ok_or(Fault::IntentConflict)?;
        let native = operation.native.as_ref().ok_or(Fault::IntentConflict)?;
        if native.invocation.payload_sha256() != expected_payload_sha256 {
            return Err(Fault::IntentConflict);
        }
        if operation.final_delivery.is_some() {
            return Ok(if operation.final_host_acked {
                Reconciliation::Finalized
            } else {
                Reconciliation::AwaitingHostReceipt
            });
        }
        if let Some(ack) = &native.ack {
            if self.state.legacy_operations.contains(operation_id) {
                return Err(Fault::ContextRequired);
            }
            return Ok(Reconciliation::AckReadback(ack.value()?));
        }
        if let Some(code) = &native.reconcile_block {
            return Ok(Reconciliation::Blocked(code.clone()));
        }
        if !self.durability_observed
            || self.state.durability.as_ref().is_none_or(|fact| {
                fact.profile != Durability::Durable || fact.observed_boot != self.boot
            })
        {
            return Err(Fault::DurabilityUnavailable);
        }
        let resume = native.invocation.method == "session/resume";
        if resume && !self.exact_root_read {
            return Err(Fault::ExactRootReadRequired);
        }
        if self.state.legacy_operations.contains(operation_id) {
            return Err(Fault::ContextRequired);
        }
        if let Some(context) = self.state.contexts.get(operation_id) {
            self.validate_replay(scope, context, &native.invocation)?;
        } else if native.submission_boot != self.boot {
            return Err(Fault::ContextRequired);
        }
        self.commit(self.state.clone())?;
        self.invalidate_freshness()?;
        Ok(Reconciliation::Write(PreparedNative {
            journal: self,
            operation_id: operation_id.into(),
        }))
    }
    /// Metadata only: (Operation, original submission boot, ACK present).
    /// No ACK means potentially sent, never NotStarted; ACK is not application.
    pub fn unresolved_native(&self, scope: &Scope) -> Result<Vec<(&str, &str, bool)>, Fault> {
        self.healthy(scope)?;
        Ok(self
            .state
            .operations
            .iter()
            .filter_map(|(id, operation)| {
                operation
                    .native
                    .as_ref()
                    .filter(|_| operation.final_delivery.is_none())
                    .map(|native| {
                        (
                            id.as_str(),
                            native.submission_boot.as_str(),
                            native.ack.is_some(),
                        )
                    })
            })
            .collect())
    }
    /// Reconciliation comparison token from the retained intent, not a newly
    /// rendered payload. No token exists for a gap with no native intent.
    pub fn native_payload_sha256(&self, scope: &Scope, operation_id: &str) -> Result<&str, Fault> {
        self.healthy(scope)?;
        self.state
            .operations
            .get(operation_id)
            .and_then(|operation| operation.native.as_ref())
            .map(|native| native.invocation.payload_sha256())
            .ok_or(Fault::IntentConflict)
    }
    pub fn queue_outcome(
        &mut self,
        scope: &Scope,
        receipt: ModuleReceiptIdentity,
        outcome: RuntimeOutcome,
    ) -> Result<(), Fault> {
        self.healthy(scope)?;
        scope.receipt(&receipt)?;
        if outcome.operation_id != receipt.operation_id
            || outcome
                .native_root_id
                .as_ref()
                .is_some_and(|root| root != &scope.native_root_id)
            || outcome
                .native_scope_key
                .as_ref()
                .is_some_and(|namespace| namespace != &scope.native_namespace)
            || serde_json::from_value::<ModuleReceiptIdentity>(
                outcome.details["module_receipt"].clone(),
            )
            .map_err(|_| Fault::InvalidState)?
                != receipt
        {
            return Err(Fault::ScopeMismatch);
        }
        let final_delivery = matches!(
            outcome.outcome,
            EffectOutcome::Applied | EffectOutcome::Rejected
        );
        let value = serde_json::to_value(outcome).map_err(|_| Fault::InvalidState)?;
        self.queue(
            receipt,
            Delivery {
                method: DeliveryMethod::Outcome,
                params: SavedJson::new(&value)?,
            },
            final_delivery,
            None,
            None,
        )
    }
    /// Stores exact caller-produced result page bytes; the facade and Store
    /// validate native provenance/content. This cannot settle the source send.
    pub fn queue_result(
        &mut self,
        scope: &Scope,
        receipt: ModuleReceiptIdentity,
        page: Value,
    ) -> Result<(), Fault> {
        self.queue_result_inner(scope, receipt, page, None)
    }
    /// The reader must check authority_is_current before/after native reads;
    /// retirement additionally checks the same opaque authority here.
    pub fn queue_result_for(
        &mut self,
        scope: &Scope,
        receipt: ModuleReceiptIdentity,
        page: Value,
        authority: &SessionAuthority,
    ) -> Result<(), Fault> {
        if !self.authority_is_current(scope, authority)
            || page["source"]["native_session_id"] != authority.session_id
        {
            return Err(Fault::ProvenanceRequired);
        }
        self.queue_result_inner(scope, receipt, page, Some(authority.proof.clone()))
    }
    pub(crate) fn queue_normalized_result_for(
        &mut self,
        scope: &Scope,
        receipt: ModuleReceiptIdentity,
        command: &RuntimeCommand,
        page: Value,
        authority: &SessionAuthority,
    ) -> Result<(), Fault> {
        let context = self.result_context(scope, &receipt, command)?;
        if !self.authority_is_current(scope, authority)
            || context.request().selector().session_id() != authority.session_id
        {
            return Err(Fault::ProvenanceRequired);
        }
        context.validate_page(&page).map_err(|_| Fault::InvalidState)?;
        self.queue(
            receipt.clone(),
            Delivery {
                method: DeliveryMethod::Result,
                params: SavedJson::new(&json!({"operation_id":receipt.operation_id,"page":page}))?,
            },
            true,
            Some(authority.proof.clone()),
            Some(SavedJson::new(&serde_json::to_value(command).map_err(|_| Fault::InvalidState)?)?),
        )
    }
    fn queue_result_inner(
        &mut self,
        scope: &Scope,
        receipt: ModuleReceiptIdentity,
        page: Value,
        family: Option<FamilyProof>,
    ) -> Result<(), Fault> {
        self.healthy(scope)?;
        scope.receipt(&receipt)?;
        if !page.is_object()
            || self
                .state
                .operations
                .get(&receipt.operation_id)
                .is_some_and(|operation| operation.native.is_some())
        {
            return Err(Fault::IntentConflict);
        }
        if page["source"]["native_session_id"] != scope.native_root_id {
            family.as_ref().ok_or(Fault::ScopeMismatch)?.validate(
                scope,
                page["source"]["native_session_id"]
                    .as_str()
                    .ok_or(Fault::InvalidState)?,
            )?;
        }
        let params = SavedJson::new(&json!({"operation_id":receipt.operation_id,"page":page}))?;
        self.queue(
            receipt,
            Delivery {
                method: DeliveryMethod::Result,
                params,
            },
            true,
            family,
            None,
        )
    }
    fn queue(
        &mut self,
        receipt: ModuleReceiptIdentity,
        delivery: Delivery,
        final_delivery: bool,
        family: Option<FamilyProof>,
        result_command: Option<SavedJson>,
    ) -> Result<(), Fault> {
        let id = receipt.operation_id.clone();
        if let Some(operation) = self.state.operations.get(&id) {
            if operation.receipt != receipt {
                return Err(Fault::IntentConflict);
            }
            if result_command.is_some()
                && operation.result_command != result_command
                && (operation.result_command.is_some() || operation.final_delivery.is_some())
            {
                return Err(Fault::IntentConflict);
            }
            if let Some(old) = &operation.final_delivery {
                return if old == &delivery || !final_delivery {
                    Ok(())
                } else {
                    Err(Fault::OutcomeConflict)
                };
            }
        }
        if self
            .state
            .outbox
            .get(&id)
            .is_some_and(|slot| slot.delivery == delivery)
        {
            return Ok(());
        }
        let mut next = self.state.clone();
        let operation = next
            .operations
            .entry(id.clone())
            .or_insert_with(|| Operation {
                receipt,
                native: None,
                final_delivery: None,
                final_host_acked: false,
                family: family.clone(),
                result_command: None,
            });
        if let Some(command) = result_command {
            operation.result_command = Some(command);
        }
        if let Some(family) = family {
            if operation.family.as_ref().is_some_and(|old| old != &family) {
                return Err(Fault::IntentConflict);
            }
            operation.family = Some(family);
        }
        if final_delivery {
            operation.final_delivery = Some(delivery.clone());
        }
        let revision = self.state.revision.checked_add(1).ok_or(Fault::Capacity)?;
        next.outbox.insert(id, Slot { revision, delivery });
        self.commit(next)
    }
    pub fn outbox(&self, scope: &Scope) -> Result<Vec<DeliveryTicket>, Fault> {
        self.healthy(scope)?;
        Ok(self
            .state
            .outbox
            .iter()
            .map(|(id, slot)| DeliveryTicket {
                operation_id: id.clone(),
                slot: slot.clone(),
            })
            .collect())
    }
    /// Call only after an authenticated matching host ACK. Lost ACK means this
    /// is not called. A newer delivery is untouched by an older ACK.
    pub fn acknowledge(
        &mut self,
        scope: &Scope,
        ticket: &DeliveryTicket,
    ) -> Result<AckDisposition, Fault> {
        self.healthy(scope)?;
        let Some(slot) = self.state.outbox.get(&ticket.operation_id) else {
            return Ok(AckDisposition::AlreadyRetired);
        };
        if slot != &ticket.slot {
            return Ok(AckDisposition::Superseded);
        }
        let mut next = self.state.clone();
        next.outbox.remove(&ticket.operation_id);
        let operation = next
            .operations
            .get_mut(&ticket.operation_id)
            .ok_or(Fault::InvalidState)?;
        if operation.final_delivery.as_ref() == Some(&slot.delivery) {
            operation.final_host_acked = true;
        }
        self.commit(next)?;
        Ok(AckDisposition::Retired)
    }
}

fn projection_fence(scope: &Scope, boot: &str) -> ProjectionFence {
    ProjectionFence {
        binding_id: scope.binding_id.clone(),
        binding_generation: scope.generation,
        native_root_id: scope.native_root_id.clone(),
        native_scope_key: scope.native_namespace.clone(),
        bridge_boot_id: boot.into(),
    }
}
fn family_proof(
    scope: &Scope,
    checkpoint: &Value,
    target: &str,
    fresh: bool,
) -> Result<FamilyProof, Fault> {
    if !atom(target) {
        return Err(Fault::ProvenanceRequired);
    }
    let state = &checkpoint["state"];
    if fresh
        && (state["root_snapshot_freshness"] != "fresh"
            || state["session"]["sessionId"] != scope.native_root_id)
    {
        return Err(Fault::FreshChecksRequired);
    }
    let children = state["observed_children"]
        .as_array()
        .ok_or(Fault::InvalidState)?;
    let mut edges = Vec::new();
    let mut child = target.to_owned();
    let mut seen = BTreeSet::new();
    while child != scope.native_root_id {
        if edges.len() >= 128 || !seen.insert(child.clone()) {
            return Err(Fault::ProvenanceRequired);
        }
        let row = children
            .iter()
            .find(|row| row["sessionId"] == child)
            .ok_or(Fault::ProvenanceRequired)?;
        if fresh && (row["snapshot_freshness"] != "fresh" || row["snapshot"]["sessionId"] != child)
        {
            return Err(Fault::FreshChecksRequired);
        }
        let edge = FamilyEdge {
            parent_session_id: row["parentSessionId"]
                .as_str()
                .ok_or(Fault::ProvenanceRequired)?
                .into(),
            child_session_id: child,
            item_id: row["itemId"]
                .as_str()
                .ok_or(Fault::ProvenanceRequired)?
                .into(),
            parent_turn_id: row["parentTurnId"].as_str().map(str::to_owned),
            item_revision: row["revision"].as_u64().ok_or(Fault::ProvenanceRequired)?,
            kind: row["kind"]
                .as_str()
                .ok_or(Fault::ProvenanceRequired)?
                .into(),
        };
        child = edge.parent_session_id.clone();
        edges.push(edge);
    }
    edges.reverse();
    let proof = FamilyProof {
        projection_sha256: digest(checkpoint)?,
        edges,
    };
    proof.validate(scope, target)?;
    Ok(proof)
}
fn validate_bundle(scope: &Scope, bundle: &ObservationBundle) -> Result<(), Fault> {
    if !atom(&bundle.source_boot) || bundle.discovery.len() > 129 || bundle.inventories.len() > 129
    {
        return Err(Fault::Capacity);
    }
    let value = bundle.projection.value()?;
    let fence = projection_fence(scope, &bundle.source_boot);
    if value["fence"] != serde_json::to_value(&fence).map_err(|_| Fault::InvalidState)? {
        return Err(Fault::ScopeMismatch);
    }
    ObservationProjector::restore(fence, value.clone()).map_err(|_| Fault::InvalidState)?;
    reject_private_keys(&value)?;
    let mut parents = BTreeSet::new();
    for discovery in &bundle.discovery {
        discovery.validate()?;
        family_proof(scope, &value, &discovery.parent_session_id, false)?;
        if !parents.insert(&discovery.parent_session_id) {
            return Err(Fault::InvalidState);
        }
    }
    let mut sessions = BTreeSet::new();
    for inventory in &bundle.inventories {
        if inventory
            .capture
            .as_ref()
            .is_none_or(|s| !atom(&s.instance) || s.connection == 0)
        {
            return Err(Fault::InvalidState);
        }
        family_proof(scope, &value, &inventory.session_id, false)?;
        PendingInventory::from_read(
            &inventory.session_id,
            inventory.sampled_revision,
            inventory.sampled_revision,
            inventory.value()?,
        )?;
        let inventory_value = inventory.value()?;
        let mut native_ids = BTreeSet::new();
        for (key, id, kind) in [
            ("approvals", "approvalId", "approval"),
            ("userInputs", "userInputId", "user_input"),
        ] {
            for row in inventory_value[key].as_array().ok_or(Fault::InvalidState)? {
                native_ids.insert((
                    kind.to_owned(),
                    row[id].as_str().ok_or(Fault::InvalidState)?.to_owned(),
                ));
            }
        }
        let projected_ids = value["state"]["pending_requests"]
            .as_array()
            .ok_or(Fault::InvalidState)?
            .iter()
            .filter(|p| p["session_id"] == inventory.session_id)
            .map(|p| {
                Ok((
                    p["kind"].as_str().ok_or(Fault::InvalidState)?.to_owned(),
                    p["request_id"]
                        .as_str()
                        .ok_or(Fault::InvalidState)?
                        .to_owned(),
                ))
            })
            .collect::<Result<BTreeSet<_>, Fault>>()?;
        if native_ids != projected_ids {
            return Err(Fault::ProvenanceRequired);
        }
        let revision = value["session_revisions"]
            .as_array()
            .and_then(|rows| {
                rows.iter()
                    .find(|row| row["session_id"] == inventory.session_id)
            })
            .and_then(|row| row["revision"].as_u64())
            .unwrap_or(0);
        if revision != inventory.sampled_revision || !sessions.insert(&inventory.session_id) {
            return Err(Fault::FreshChecksRequired);
        }
    }
    Ok(())
}

fn state_digest(scope: &Scope, state: &State) -> Result<Sha256Digest, Fault> {
    digest(&json!({"format":FORMAT,"version":CHECKPOINT_VERSION,"scope":scope,"state":state}))
}
fn encode(scope: &Scope, state: &State) -> Result<Vec<u8>, Fault> {
    let bytes = serde_json::to_vec(&Checkpoint {
        format: FORMAT.into(),
        version: CHECKPOINT_VERSION,
        scope: scope.clone(),
        state: state.clone(),
        sha256: state_digest(scope, state)?,
    })
    .map_err(|_| Fault::InvalidState)?;
    if bytes.len() as u64 > MAX_FILE {
        return Err(Fault::Capacity);
    }
    Ok(bytes)
}
fn validate_ack(scope: &Scope, invocation: &NativeInvocation, ack: &Value) -> Result<(), Fault> {
    definition(ack_definition(&invocation.method)?, ack)?;
    if matches!(
        invocation.method.as_str(),
        "session/start" | "session/resume"
    ) {
        // The native opaque head may be the empty string before any view
        // event. The pinned result schema still requires a string field.
        if ack["session"]["sessionId"] != scope.native_root_id {
            return Err(Fault::ScopeMismatch);
        }
        if invocation.method == "session/resume"
            && (ack["history"]["mode"] != "none"
                || !ack["history"]["items"].is_null()
                || !ack["history"]["snapshot"].is_null())
        {
            return Err(Fault::InvalidState);
        }
    } else if ack["commandId"] != invocation.command_id || ack["status"] != "accepted" {
        return Err(Fault::InvalidState);
    }
    if invocation.method == "turn/start"
        && ack["startedNewTurn"].as_bool() != Some(ack["disposition"] == "started")
    {
        return Err(Fault::InvalidState);
    }
    if matches!(invocation.method.as_str(), "turn/start" | "turn/steer")
        && ack["turnId"].as_str().is_none_or(str::is_empty)
    {
        return Err(Fault::InvalidState);
    }
    let params = invocation.params()?;
    if invocation.method == "approval/decide" && ack["approvalId"] != params["approvalId"]
        || invocation.method.starts_with("userInput/")
            && ack["userInputId"] != params["userInputId"]
    {
        return Err(Fault::AckConflict);
    }
    Ok(())
}
fn validate_ack_v1(scope: &Scope, invocation: &NativeInvocation, ack: &Value) -> Result<(), Fault> {
    let object = ack.as_object().ok_or(Fault::InvalidState)?;
    let lifecycle = matches!(
        invocation.method.as_str(),
        "session/start" | "session/resume"
    );
    if lifecycle {
        if ack["session"]["sessionId"] != scope.native_root_id
            || ack["viewCursor"].as_str().is_none_or(str::is_empty)
        {
            return Err(Fault::InvalidState);
        }
        if invocation.method == "session/resume"
            && (ack["history"]["mode"] != "none"
                || !ack["history"]["items"].is_null()
                || !ack["history"]["snapshot"].is_null()
                || !ack["pendingRequests"].is_array())
        {
            return Err(Fault::InvalidState);
        }
    } else if ack["commandId"] != invocation.command_id || ack["status"] != "accepted" {
        return Err(Fault::InvalidState);
    }
    if object.keys().any(|key| {
        ![
            "commandId",
            "status",
            "turnId",
            "approvalId",
            "userInputId",
            "effectiveMode",
            "goal",
            "session",
            "viewCursor",
            "history",
            "pendingRequests",
        ]
        .contains(&key.as_str())
    }) || object
        .get("commandId")
        .is_some_and(|id| id != &invocation.command_id)
        || object
            .get("status")
            .is_some_and(|status| status != "accepted")
        || object
            .get("session")
            .is_some_and(|session| session["sessionId"] != scope.native_root_id)
        || object.get("history").is_some_and(|history| {
            history["mode"] != "none"
                || !history["items"].is_null()
                || !history["snapshot"].is_null()
        })
    {
        return Err(Fault::InvalidState);
    }
    if let Some(session) = object.get("session") {
        let session = session.as_object().ok_or(Fault::InvalidState)?;
        if session.keys().any(|key| {
            ![
                "sessionId",
                "modelId",
                "providerId",
                "workspaceRoot",
                "path",
                "status",
                "activeTurnId",
                "turnCount",
                "createdAt",
                "updatedAt",
                "forkedFrom",
                "approvalMode",
                "attention",
                "branch",
                "firstUserPrompt",
                "lastActivityAt",
                "name",
                "title",
            ]
            .contains(&key.as_str())
        }) {
            return Err(Fault::InvalidState);
        }
    }
    Ok(())
}
fn validate_state(scope: &Scope, state: &State) -> Result<(), Fault> {
    if state.operations.len() > MAX_OPERATIONS
        || state.outbox.len() > MAX_OPERATIONS
        || state.contexts.len() > MAX_OPERATIONS
    {
        return Err(Fault::Capacity);
    }
    if state
        .legacy_operations
        .iter()
        .any(|id| !state.operations.contains_key(id) || state.contexts.contains_key(id))
    {
        return Err(Fault::InvalidState);
    }
    if let Some(bundle) = &state.observations {
        validate_bundle(scope, bundle)?;
    }
    for (id, context) in &state.contexts {
        if serde_json::to_vec(context)
            .map_err(|_| Fault::InvalidState)?
            .len()
            > MAX_CONTEXT
        {
            return Err(Fault::Capacity);
        }
        let operation = state.operations.get(id).ok_or(Fault::InvalidState)?;
        let native = operation.native.as_ref().ok_or(Fault::InvalidState)?;
        if context.original_receipt != operation.receipt
            || context.original_boot != native.submission_boot
            || context.family != operation.family
        {
            return Err(Fault::IntentConflict);
        }
        context.validate(scope, &native.invocation)?;
    }
    if state
        .durability
        .as_ref()
        .is_some_and(|fact| !atom(&fact.observed_boot))
    {
        return Err(Fault::InvalidState);
    }
    let mut native_ids = std::collections::HashSet::new();
    for (id, operation) in &state.operations {
        if operation.result_command.is_some()
            && (operation.native.is_some()
                || operation.final_delivery.as_ref().is_none_or(|delivery| delivery.method != DeliveryMethod::Result))
        {
            return Err(Fault::InvalidState);
        }
        scope.receipt(&operation.receipt)?;
        if id != &operation.receipt.operation_id {
            return Err(Fault::InvalidState);
        }
        if let Some(native) = &operation.native {
            if state.legacy_operations.contains(id) {
                native.invocation.validate_v1(scope)?;
            } else {
                native.invocation.validate(scope)?;
            }
            let params = native.invocation.params()?;
            if params["sessionId"] != scope.native_root_id {
                operation
                    .family
                    .as_ref()
                    .ok_or(Fault::ProvenanceRequired)?
                    .validate(
                        scope,
                        params["sessionId"].as_str().ok_or(Fault::InvalidState)?,
                    )?;
                if !state.contexts.contains_key(id) {
                    return Err(Fault::ContextRequired);
                }
            }
            if !atom(&native.submission_boot)
                || !native_ids.insert(native.invocation.command_id.clone())
            {
                return Err(Fault::IntentConflict);
            }
            if let Some(ack) = &native.ack {
                if state.legacy_operations.contains(id) {
                    validate_ack_v1(scope, &native.invocation, &ack.value()?)?;
                } else {
                    validate_ack(scope, &native.invocation, &ack.value()?)?;
                }
            }
            if native.reconcile_block.as_ref().is_some_and(|code| {
                code.is_empty()
                    || code.len() > 128
                    || !code
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
            }) {
                return Err(Fault::InvalidState);
            }
        }
        if let Some(delivery) = &operation.final_delivery {
            validate_delivery(
                scope,
                state,
                &operation.receipt,
                delivery,
                operation.family.as_ref(),
            )?;
            let params = delivery.params.value()?;
            if delivery.method == DeliveryMethod::Outcome
                && !matches!(params["outcome"].as_str(), Some("applied" | "rejected"))
            {
                return Err(Fault::InvalidState);
            }
            if operation.final_host_acked == state.outbox.contains_key(id) {
                return Err(Fault::InvalidState);
            }
        }
        if operation.final_host_acked && operation.final_delivery.is_none() {
            return Err(Fault::InvalidState);
        }
    }
    for (id, slot) in &state.outbox {
        let operation = state.operations.get(id).ok_or(Fault::InvalidState)?;
        validate_delivery(
            scope,
            state,
            &operation.receipt,
            &slot.delivery,
            operation.family.as_ref(),
        )?;
        if slot.revision == 0
            || slot.revision > state.revision
            || slot.delivery.params.value()?["operation_id"] != *id
            || operation
                .final_delivery
                .as_ref()
                .is_some_and(|delivery| delivery != &slot.delivery)
        {
            return Err(Fault::InvalidState);
        }
    }
    Ok(())
}
fn validate_delivery(
    scope: &Scope,
    state: &State,
    receipt: &ModuleReceiptIdentity,
    delivery: &Delivery,
    family: Option<&FamilyProof>,
) -> Result<(), Fault> {
    let value = delivery.params.value()?;
    if value["operation_id"] != receipt.operation_id {
        return Err(Fault::ScopeMismatch);
    }
    match delivery.method {
        DeliveryMethod::Outcome => {
            let outcome: RuntimeOutcome =
                serde_json::from_value(value).map_err(|_| Fault::InvalidState)?;
            if serde_json::from_value::<ModuleReceiptIdentity>(
                outcome.details["module_receipt"].clone(),
            )
            .map_err(|_| Fault::InvalidState)?
                != *receipt
                || outcome
                    .native_root_id
                    .as_ref()
                    .is_some_and(|root| root != &scope.native_root_id)
                || outcome
                    .native_scope_key
                    .as_ref()
                    .is_some_and(|namespace| namespace != &scope.native_namespace)
            {
                return Err(Fault::ScopeMismatch);
            }
        }
        DeliveryMethod::Result => {
            let object = value.as_object().ok_or(Fault::InvalidState)?;
            if object.len() != 2 || !value["page"].is_object() {
                return Err(Fault::InvalidState);
            }
            if value["page"]["source"]["schema_id"] == "swarm.normalized_result_page" {
                let retained = state.operations.get(&receipt.operation_id)
                    .and_then(|operation| operation.result_command.as_ref())
                    .ok_or(Fault::ContextRequired)?;
                let command: RuntimeCommand = serde_json::from_value(retained.value()?)
                    .map_err(|_| Fault::InvalidState)?;
                if serde_json::to_value(&command).map_err(|_| Fault::InvalidState)? != retained.value()? {
                    return Err(Fault::InvalidState);
                }
                result_context(scope, state, receipt, &command)?
                    .validate_page(&value["page"]).map_err(|_| Fault::InvalidState)?;
                return Ok(());
            }
            if state.operations.get(&receipt.operation_id)
                .is_some_and(|operation| operation.result_command.is_some())
            {
                return Err(Fault::InvalidState);
            }
            if value["page"]["source"]["native_session_id"] != scope.native_root_id {
                family.ok_or(Fault::ScopeMismatch)?.validate(
                    scope,
                    value["page"]["source"]["native_session_id"]
                        .as_str()
                        .ok_or(Fault::InvalidState)?,
                )?;
            }
        }
    }
    Ok(())
}
fn result_context(
    scope: &Scope,
    state: &State,
    receipt: &ModuleReceiptIdentity,
    command: &RuntimeCommand,
) -> Result<crate::results::NormalizedResultContext, Fault> {
    scope.receipt(receipt)?;
    validate_host_envelope(scope, command)?;
    if command.native_root_id.as_deref() != Some(scope.native_root_id.as_str()) {
        return Err(Fault::ScopeMismatch);
    }
    let (request, _) = crate::results::validate_normalized_request(command, receipt)
        .map_err(|_| Fault::InvalidState)?;
    let target_id = request.selector().input_operation_id().ok_or(Fault::ContextRequired)?;
    let dispatch = state.operations.get(target_id).ok_or(Fault::ContextRequired)?;
    let context = state.contexts.get(target_id).ok_or(Fault::ContextRequired)?;
    let native = dispatch.native.as_ref().ok_or(Fault::ContextRequired)?;
    if context.command.method != "task.dispatch" || native.invocation.method != "turn/start" {
        return Err(Fault::IntentConflict);
    }
    let task = context.task_prompt(scope)?.ok_or(Fault::ContextRequired)?;
    let ack = native.ack.as_ref().ok_or(Fault::FreshChecksRequired)?.value()?;
    validate_ack(scope, &native.invocation, &ack)?;
    crate::results::NormalizedResultContext::for_dispatch(
        command, receipt, &dispatch.receipt, &task, native.invocation.command_id(),
        ack["turnId"].as_str().ok_or(Fault::InvalidState)?,
    ).map_err(|_| Fault::IntentConflict)
}
fn present(path: &Path) -> Result<bool, Fault> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(Fault::PersistenceUnknown),
    }
}
fn reject_legacy(dir: &Path) -> Result<(), Fault> {
    if present(&dir.join("checkpoint.json"))? {
        return Err(Fault::LegacyState);
    }
    Ok(())
}
fn owned_directory(scope: &Scope) -> Result<(PathBuf, String), Fault> {
    let owner = swarm_process::module_owner::verify_current_adapter_from_env()
        .map_err(|_| Fault::OwnerUnverified)?;
    for (name, expected) in [
        ("ELIOT_SWARM_MODULE_ID", scope.module_id.as_str().to_owned()),
        (
            "ELIOT_SWARM_MODULE_CLIENT_ID",
            scope.module_client_id.clone(),
        ),
        (
            "ELIOT_SWARM_MODULE_ARTIFACT_ID",
            scope.artifact.artifact_id.as_str().to_owned(),
        ),
        (
            "ELIOT_SWARM_MODULE_ARTIFACT_VERSION",
            scope.artifact.version.as_str().to_owned(),
        ),
        ("ELIOT_SWARM_MODULE_BINDING_ID", scope.binding_id.clone()),
        (
            "ELIOT_SWARM_MODULE_BINDING_GENERATION",
            scope.generation.to_string(),
        ),
        ("ELIOT_SWARM_MODULE_PROTOCOL", "1.0".into()),
    ] {
        if std::env::var(name).ok().as_deref() != Some(expected.as_str()) {
            return Err(Fault::ScopeMismatch);
        }
    }
    if std::env::var("ELIOT_SWARM_MODULE_BUILD_ID").ok().as_deref()
        != scope.artifact.build_id.as_deref()
    {
        return Err(Fault::ScopeMismatch);
    }
    let dir = std::env::var_os("ELIOT_SWARM_MODULE_STATE")
        .map(PathBuf::from)
        .ok_or(Fault::OwnerUnverified)?;
    if !dir.is_absolute()
        || std::env::var_os("ELIOT_SWARM_MODULE_OWNER").map(PathBuf::from)
            != Some(dir.join("owner.json"))
    {
        return Err(Fault::OwnerUnverified);
    }
    Ok((dir, owner.boot_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::Cell,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };
    use swarm_contracts::module_catalog::{ArtifactId, ArtifactVersion, ProtocolVersion};
    use swarm_process::test_support::scoped_durability_barrier;

    const BOOT_A: &str = "fixture-boot-a";
    const BOOT_B: &str = "fixture-boot-b";
    static NEXT: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        directory: PathBuf,
        scope: Scope,
    }
    impl Fixture {
        fn new() -> Self {
            let name = format!(
                "swarm-muse-journal-{}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            );
            let directory = std::env::temp_dir().join(name);
            fs::create_dir(&directory).unwrap();
            let directory = directory.canonicalize().unwrap();
            let scope = Scope {
                module_client_id: "fixture-module-client".into(),
                module_id: ModuleId::new("muse").unwrap(),
                artifact: ArtifactIdentity {
                    artifact_id: ArtifactId::new("muse-rust-fixture").unwrap(),
                    version: ArtifactVersion::new("0.1.0").unwrap(),
                    build_id: None,
                },
                binding_id: "fixture-binding".into(),
                generation: 1,
                native_namespace: "muse:fixture:observed-namespace".into(),
                native_root_id: "019a0000-0000-7000-8000-000000000099".into(),
            };
            Self { directory, scope }
        }
        fn create(&self) -> Journal {
            Journal::create_at(&self.directory, self.scope.clone(), BOOT_A.into()).unwrap()
        }
        fn reopen(&self) -> Journal {
            Journal::open_at(&self.directory, self.scope.clone(), BOOT_B.into()).unwrap()
        }
        fn receipt(&self, operation: &str) -> ModuleReceiptIdentity {
            ModuleReceiptIdentity {
                schema_version: 1,
                module_id: self.scope.module_id.clone(),
                artifact: self.scope.artifact.clone(),
                protocol: ProtocolVersion { major: 1, minor: 0 },
                binding_id: self.scope.binding_id.clone(),
                binding_generation: self.scope.generation,
                operation_id: operation.into(),
                input_sha256: digest(&json!({"fixture_operation":operation}))
                    .unwrap()
                    .as_str()
                    .into(),
            }
        }
        fn invocation(&self, n: u64, method: &str) -> NativeInvocation {
            let id = format!("019a0000-0000-7000-8000-{n:012x}");
            let mut params = json!({"commandId":id,"sessionId":self.scope.native_root_id});
            if method == "session/start" {
                params["workspaceRoot"] = json!("/synthetic-work");
            }
            if method == "turn/start" {
                params["input"] = json!([{"type":"text","text":"exact fixture input"}]);
                params["ifBusy"] = json!("queue");
            }
            if method == "session/resume" {
                params["excludeItems"] = json!(true);
            }
            NativeInvocation::new(
                &self.scope,
                method,
                &id,
                params.as_object().unwrap().clone(),
            )
            .unwrap()
        }
        fn outcome(&self, operation: &str, outcome: EffectOutcome) -> RuntimeOutcome {
            RuntimeOutcome {
                operation_id: operation.into(),
                outcome,
                native_scope_key: Some(self.scope.native_namespace.clone()),
                native_root_id: Some(self.scope.native_root_id.clone()),
                turn_id: None,
                native_input_id: None,
                details: json!({"module_receipt":self.receipt(operation),"fixture":true}),
            }
        }
        fn ack(&self, invocation: &NativeInvocation) -> Value {
            match invocation.method() {
                "turn/start" => {
                    json!({"commandId":invocation.command_id(),"status":"accepted","turnId":"fixture-turn","disposition":"started","startedNewTurn":true})
                }
                "session/start" => {
                    json!({"session":self.session(&self.scope.native_root_id),"viewCursor":"opaque"})
                }
                "session/resume" => {
                    json!({"session":self.session(&self.scope.native_root_id),"viewCursor":"opaque","history":{"mode":"none","items":null,"snapshot":null},"pendingRequests":[]})
                }
                _ => json!({"commandId":invocation.command_id(),"status":"accepted"}),
            }
        }

        fn command(&self, operation: &str, method: &str, input: Value) -> RuntimeCommand {
            RuntimeCommand {
                operation_id: operation.into(),
                method: method.into(),
                created_at_ms: 1,
                binding_id: self.scope.binding_id.clone(),
                generation: self.scope.generation,
                native_root_id: if method == "agent.open" {
                    None
                } else {
                    Some(self.scope.native_root_id.clone())
                },
                route: json!({"module_artifact_id":self.scope.artifact.artifact_id,"native_options":{"workspaceRoot":"/synthetic-work"}}),
                input_sha256: Some(self.receipt(operation).input_sha256),
                input,
                target_input_sha256: None,
            }
        }
        fn initialize(&self, journal: &mut Journal, grant: bool) {
            let init = json!({"experimentalApi":false,"grantedCapabilities":if grant{vec!["sessionMcp"]}else{vec![]},"museHome":"/observed-not-copied","platformFamily":"unix","platformOs":"linux","schema":{"version":1,"fingerprint":"sha256:7469c9e352e67def4a59df7e439984d7194fa351e1c8b7abb34060fd977ced81"},"serverInfo":{"name":"fixture","version":"1.3.0"},"userAgent":"fixture","sessionDurability":"durable"});
            journal
                .record_initialize(&self.scope, &self.scope.native_namespace, &init)
                .unwrap();
        }
        fn session(&self, id: &str) -> Value {
            json!({"sessionId":id,"status":"idle","activeTurnId":null,"createdAt":"2026-10-17T00:00:00Z","updatedAt":"2026-10-17T00:00:00Z","forkedFrom":null,"modelId":null,"providerId":null,"path":"synthetic-native-log","turnCount":0,"workspaceRoot":"/synthetic-work"})
        }
        fn work_read(&self, journal: &mut Journal, revision: u64) -> RetainedWorkFence {
            let read = json!({"session":self.session(&self.scope.native_root_id),"viewCursor":"opaque:root","history":{"mode":"none","items":null,"snapshot":null},"pendingRequests":[]});
            journal
                .record_work_read(
                    &self.scope,
                    revision,
                    revision,
                    &read,
                    RetainedModelState::Settled,
                    None,
                )
                .unwrap()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let parent = std::env::temp_dir().canonicalize().unwrap();
            assert_eq!(self.directory.parent(), Some(parent.as_path()));
            assert!(
                self.directory
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("swarm-muse-journal-")
            );
            fs::remove_dir_all(&self.directory).unwrap();
        }
    }

    // Source-derived module.next shapes, not a Kernel dependency: Host
    // store/runtime.rs::next_internal retains the original request/digest,
    // adds TaskPrompt/context, and uses module_route_projection unchanged.
    // Public request fields are also checked against the shared MCP catalog.
    // These journal consumers do not replace Root's connected Store test.
    fn host_command(
        fixture: &Fixture,
        operation: &str,
        method: &str,
        request: Value,
    ) -> RuntimeCommand {
        let (read_only, spec) = swarm_contracts::mcp_catalog::find_tool_spec(method).unwrap();
        let shape = swarm_contracts::mcp_catalog::input_schema(spec, *read_only, true);
        let shape = Value::Object(shape.as_ref().clone());
        assert!(
            jsonschema::options()
                .offline()
                .build(&shape)
                .unwrap()
                .is_valid(&request)
        );
        let hash = digest(&request).unwrap();
        let mut command = fixture.command(operation, method, request);
        command.route = json!({
            "alias":"muse-host-fixture", "runtime":"module", "enabled":true,
            "module_artifact_id":fixture.scope.artifact.artifact_id,
            "native_options":{"workspaceRoot":"/synthetic-work"},
            "workspace_option":"workspaceRoot",
            "admission_policy":{"max_concurrent_roots":2}
        });
        command.input_sha256 = Some(hash.as_str().into());
        command
    }

    fn host_receipt(fixture: &Fixture, command: &RuntimeCommand) -> ModuleReceiptIdentity {
        let mut receipt = fixture.receipt(&command.operation_id);
        receipt.input_sha256 = command.input_sha256.clone().unwrap();
        receipt
    }

    #[test]
    fn muse_journal_host_open_envelope_round_trip_and_rootless_reconcile_keep_exact_bytes() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        fixture.initialize(&mut journal, true);
        let mut command = host_command(
            &fixture,
            "host-open",
            "agent.open",
            json!({
                "client_request_id":"request-open", "lane_id":"writer-lane", "route":"muse-host-fixture"
            }),
        );
        let servers = json!({
            "stdio":{"transport":"stdio","command":"explicit-binary","args":[],"mode":"required","env":{}},
            "http":{"transport":"streamableHttp","url":"https://example.invalid/mcp","mode":"optional","headers":{}}
        });
        command.route["native_options"]["mcpServers"] = servers.clone();
        let base = fixture.invocation(50, "session/start");
        let mut params = base.params().unwrap();
        params.insert("config".into(), json!({"mcpServers":servers}));
        let invocation = journal
            .native_invocation(
                &fixture.scope,
                "session/start",
                base.command_id(),
                params.clone(),
            )
            .unwrap();
        let receipt = host_receipt(&fixture, &command);
        drop(
            journal
                .prepare_command(
                    &fixture.scope,
                    receipt.clone(),
                    command.clone(),
                    invocation.clone(),
                    RetainedSelectors::default(),
                )
                .unwrap(),
        );
        drop(journal);

        let mut journal = fixture.reopen();
        let retained = journal
            .retained_command(&fixture.scope, "host-open")
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(retained.command()).unwrap(),
            serde_json::to_value(&command).unwrap()
        );
        assert_eq!(retained.original_receipt(), &receipt);
        assert_eq!(retained.original_boot(), BOOT_A);
        assert_eq!(retained.native_command_id(), invocation.command_id());
        assert_eq!(retained.payload_sha256(), invocation.payload_sha256());
        assert_eq!(
            journal
                .retained_invocation(&fixture.scope, "host-open")
                .unwrap()
                .params()
                .unwrap(),
            params
        );

        fixture.initialize(&mut journal, true);
        let mut reconcile = host_command(
            &fixture,
            "host-reconcile",
            "agent.reconcile",
            json!({
                "client_request_id":"request-reconcile", "binding_id":fixture.scope.binding_id,
                "generation":fixture.scope.generation, "operation_id":"host-open"
            }),
        );
        reconcile.route = command.route.clone();
        reconcile.native_root_id = None;
        reconcile.target_input_sha256 = Some(receipt.input_sha256.clone());
        let reconcile_receipt = host_receipt(&fixture, &reconcile);
        match journal
            .reconcile_command(&fixture.scope, &reconcile, &reconcile_receipt)
            .unwrap()
        {
            Reconciliation::Write(prepared) => assert_eq!(prepared.invocation(), &invocation),
            _ => panic!("rootless reconciliation must retain the same native intent"),
        }
        let retained = journal
            .retained_command(&fixture.scope, "host-open")
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(retained.command()).unwrap(),
            serde_json::to_value(command).unwrap()
        );
        assert_eq!(retained.original_receipt(), &receipt);
    }

    #[test]
    fn muse_journal_host_dispatch_enrichment_preserves_original_digest_and_reconstruction() {
        use sha2::{Digest, Sha256};
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        fixture.initialize(&mut journal, false);
        fixture.work_read(&mut journal, 7);
        let mut command = host_command(
            &fixture,
            "host-dispatch",
            "task.dispatch",
            json!({
                "client_request_id":"request-dispatch", "attempt_id":"attempt", "text":"abc",
                "prerequisite_operation_id":"configured-operation"
            }),
        );
        let original_hash = command.input_sha256.clone();
        let prompt = "exact fixture input";
        command.input["task_prompt"] = json!({
            "schema_id":"swarm.task_prompt", "schema_version":1,
            "task_id":"task", "task_revision":2, "attempt_id":"attempt",
            "task_snapshot_sha256":"a".repeat(64),
            "prompt_sha256":format!("{:x}", Sha256::digest(prompt.as_bytes())),
            "prompt_bytes":prompt.len(), "prompt":prompt
        });
        command.input["task_dispatch_context"] = json!({
            "schema_version":1, "operation_id":"host-dispatch",
            "binding_id":fixture.scope.binding_id, "binding_generation":fixture.scope.generation,
            "worker_boot_id":BOOT_A, "attempt_id":"attempt", "task_id":"task", "task_revision":2,
            "task_snapshot_sha256":"a".repeat(64),
            "source_text_sha256":format!("{:x}", Sha256::digest(b"abc")), "source_text_bytes":3
        });
        let receipt = host_receipt(&fixture, &command);
        let invocation = fixture.invocation(51, "turn/start");
        let checkpoint = fs::read(&journal.path).unwrap();
        for field in ["task_prompt", "task_dispatch_context"] {
            let mut bad = command.clone();
            bad.input[field]["transcript"] = json!([{"text":"undeclared nested content"}]);
            assert!(
                journal
                    .prepare_command(
                        &fixture.scope,
                        receipt.clone(),
                        bad,
                        invocation.clone(),
                        RetainedSelectors::default(),
                    )
                    .is_err()
            );
            assert_eq!(fs::read(&journal.path).unwrap(), checkpoint);
        }
        drop(
            journal
                .prepare_command(
                    &fixture.scope,
                    receipt.clone(),
                    command.clone(),
                    invocation.clone(),
                    RetainedSelectors::default(),
                )
                .unwrap(),
        );
        drop(journal);
        let mut journal = fixture.reopen();
        let retained = journal
            .retained_command(&fixture.scope, "host-dispatch")
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(retained.command()).unwrap(),
            serde_json::to_value(&command).unwrap()
        );
        assert_eq!(retained.command().input_sha256, original_hash);
        assert_ne!(
            digest(&retained.command().input).unwrap().as_str(),
            receipt.input_sha256
        );
        assert_eq!(retained.original_receipt(), &receipt);
        let task = retained.task_prompt(&fixture.scope).unwrap().unwrap();
        assert_eq!(task.context.worker_boot_id, BOOT_A);
        assert_eq!(task.envelope.prompt, prompt);

        fixture.initialize(&mut journal, false);
        fixture.work_read(&mut journal, 99);
        let mut reconcile = host_command(
            &fixture,
            "host-dispatch-reconcile",
            "agent.reconcile",
            json!({
                "client_request_id":"request-dispatch-reconcile", "binding_id":fixture.scope.binding_id,
                "generation":fixture.scope.generation, "operation_id":"host-dispatch"
            }),
        );
        reconcile.target_input_sha256 = Some(receipt.input_sha256.clone());
        let reconcile_receipt = host_receipt(&fixture, &reconcile);
        let checkpoint = fs::read(&journal.path).unwrap();
        for (field, value) in [
            ("transcript", json!([{"text":"must not be retained"}])),
            ("binding_id", json!("unrelated-binding")),
            ("generation", json!(2)),
        ] {
            let mut bad = reconcile.clone();
            bad.input[field] = value;
            bad.input_sha256 = Some(digest(&bad.input).unwrap().as_str().into());
            assert!(
                journal
                    .reconcile_command(&fixture.scope, &bad, &host_receipt(&fixture, &bad))
                    .is_err()
            );
            assert_eq!(fs::read(&journal.path).unwrap(), checkpoint);
        }
        match journal
            .reconcile_command(&fixture.scope, &reconcile, &reconcile_receipt)
            .unwrap()
        {
            Reconciliation::Write(prepared) => assert_eq!(prepared.invocation(), &invocation),
            _ => panic!("reconciliation must preserve the original enriched Task command"),
        }
        // Store-sealed result enrichment retains the ORIGINAL request digest;
        // it is not a newly rendered request or permission to replay dispatch.
        let ack = fixture.ack(&invocation);
        journal.record_native_ack(&fixture.scope, "host-dispatch", invocation.command_id(), ack.clone()).unwrap();
        fixture.work_read(&mut journal, 100);
        let selector = json!({"kind":"message","input_operation_id":"host-dispatch",
            "session_id":fixture.scope.native_root_id,"item_id":"terminal","item_revision":2,"before_cursor":""});
        let mut result = host_command(&fixture, "host-result", "agent.result", json!({
            "client_request_id":"result-request","binding_id":fixture.scope.binding_id,
            "generation":fixture.scope.generation,"selector":selector,
            "offset_bytes":0,"length_bytes":65_536,
        }));
        result.target_input_sha256 = Some(receipt.input_sha256.clone());
        result.input["normalized_result_origin"] = crate::results::fixture_origin(
            &receipt, &task, invocation.command_id(), &result.input["selector"],
        );
        let result_receipt = host_receipt(&fixture, &result);
        let context = journal.result_context(&fixture.scope, &result_receipt, &result).unwrap();
        let body = b"exact terminal bytes";
        use base64::{Engine, engine::general_purpose::STANDARD};
        let page = context.normalize_page(json!({
            "source":{"native_session_id":fixture.scope.native_root_id,"item_id":"terminal",
                "item_revision":2,"turn_id":ack["turnId"],"kind":"message",
                "content_digest":format!("sha256:{:x}", Sha256::digest(body))},
            "offset_bytes":0,"byte_length":body.len(),"total_bytes":body.len(),"eof":true,
            "media_type":"text/plain","content_base64":STANDARD.encode(body),
            "page_sha256":format!("{:x}", Sha256::digest(body)),
        })).unwrap();
        let authority = journal.authorize_session(&fixture.scope, &fixture.scope.native_root_id).unwrap();
        let before = fs::read(&journal.path).unwrap();
        for (pointer, value) in [
            ("/normalized_result_origin/task_revision", json!(99)),
            ("/normalized_result_origin/producer/native_input_id", json!("foreign-input")),
        ] {
            let mut bad = result.clone();
            *bad.input.pointer_mut(pointer).unwrap() = value;
            assert!(journal.queue_normalized_result_for(&fixture.scope, result_receipt.clone(), &bad, page.clone(), &authority).is_err());
            assert_eq!(fs::read(&journal.path).unwrap(), before);
        }
        let mut private = result.clone();
        private.input["private_auth"] = json!({"token":"must-not-be-retained"});
        assert!(journal.queue_normalized_result_for(&fixture.scope, result_receipt.clone(), &private, page.clone(), &authority).is_err());
        assert_eq!(fs::read(&journal.path).unwrap(), before);
        journal.queue_normalized_result_for(&fixture.scope, result_receipt.clone(), &result, page, &authority).unwrap();
        let saved = journal.state.operations["host-result"].result_command.as_ref().unwrap().value().unwrap();
        assert_eq!(saved, serde_json::to_value(&result).unwrap());
        let exact = journal.outbox(&fixture.scope).unwrap().into_iter()
            .find(|ticket| ticket.operation_id() == "host-result").unwrap();
        drop(journal);
        let mut reopened = fixture.reopen();
        let retained = reopened.outbox(&fixture.scope).unwrap().into_iter()
            .find(|ticket| ticket.operation_id() == "host-result").unwrap();
        assert_eq!(retained.params_json(), exact.params_json());
        assert!(reopened.authorize_session(&fixture.scope, &fixture.scope.native_root_id).is_err());
        let params: Value = serde_json::from_str(exact.params_json()).unwrap();
        let before = fs::read(&reopened.path).unwrap();
        assert!(reopened.queue_normalized_result_for(&fixture.scope, result_receipt, &result, params["page"].clone(), &authority).is_err());
        assert_eq!(fs::read(&reopened.path).unwrap(), before);
    }

    #[test]
    fn muse_journal_host_envelope_rejects_unknown_private_and_malformed_fields_without_checkpoint_change()
     {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        fixture.initialize(&mut journal, true);
        let original = host_command(
            &fixture,
            "host-open",
            "agent.open",
            json!({
                "client_request_id":"request-open", "lane_id":"writer-lane", "route":"muse-host-fixture"
            }),
        );
        let cases = [
            ("/route/transcript", json!({"text":"private history"})),
            ("/route/alias", json!({"transcript":"private history"})),
            ("/route/runtime", json!("opencode_v2")),
            ("/route/enabled", json!("true")),
            ("/route/workspace_option", json!({"text":"private history"})),
            (
                "/route/admission_policy",
                json!({"max_concurrent_roots":2,"transcript":[]}),
            ),
            ("/route/admission_policy", json!({"max_concurrent_roots":0})),
            (
                "/route/owned_service",
                json!({"origin":"http://example.invalid"}),
            ),
            (
                "/route/native_options/providerApiKey",
                json!("synthetic-not-a-secret"),
            ),
            (
                "/route/native_options/auth",
                json!({"token":"synthetic-not-a-secret"}),
            ),
            ("/route/native_options/modelId", json!({"transcript":[]})),
            (
                "/route/native_options/workspaceRoot",
                json!({"text":"private history"}),
            ),
            (
                "/route/native_options/mcpServers",
                json!({"http":{"transport":"streamableHttp","url":"https://example.invalid/mcp","mode":"optional","headers":{"X-Custom":"synthetic-not-a-secret"}}}),
            ),
            (
                "/route/native_options/mcpServers",
                json!({"stdio":{"transport":"stdio","command":"explicit-binary","args":[],"mode":"required","env":{"CUSTOM_TOKEN":"synthetic-not-a-secret"}}}),
            ),
            ("/input/transcript", json!([{"text":"private history"}])),
            (
                "/input/client_request_id",
                json!({"text":"private history"}),
            ),
            ("/input/lane_id", json!([])),
            ("/input/route", json!("another-route")),
            (
                "/input/task_prompt",
                json!({"prompt":"must not be accepted on open"}),
            ),
        ];
        let checkpoint = fs::read(&journal.path).unwrap();
        let invocation = fixture.invocation(52, "session/start");
        for (path, value) in cases {
            let mut bad = serde_json::to_value(&original).unwrap();
            let (parent, key) = path.rsplit_once('/').unwrap();
            bad.pointer_mut(parent)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert(key.into(), value);
            let mut bad: RuntimeCommand = serde_json::from_value(bad).unwrap();
            bad.input_sha256 = Some(digest(&bad.input).unwrap().as_str().into());
            assert!(
                journal
                    .prepare_command(
                        &fixture.scope,
                        host_receipt(&fixture, &bad),
                        bad,
                        invocation.clone(),
                        RetainedSelectors::default(),
                    )
                    .is_err(),
                "unexpectedly retained {path}"
            );
            assert_eq!(
                fs::read(&journal.path).unwrap(),
                checkpoint,
                "changed checkpoint for {path}"
            );
            assert!(
                journal
                    .retained_command(&fixture.scope, "host-open")
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[test]
    fn muse_journal_host_metadata_is_closed_for_each_native_command_method() {
        let fixture = Fixture::new();
        for (method, fields) in [
            (
                "agent.send",
                json!({"text":"input","delivery":"next_turn","prerequisite_operation_id":"configured-operation"}),
            ),
            (
                "agent.reply",
                json!({"reply":{"method":"userInput/cancel","params":{"sessionId":fixture.scope.native_root_id,"userInputId":"question"}}}),
            ),
            (
                "agent.configure",
                json!({"settings":{"reasoningEffort":"high"},"prerequisite_operation_id":"configured-operation"}),
            ),
            (
                "agent.goal",
                json!({"action":"pause","expected_revision":1}),
            ),
            (
                "agent.recover",
                json!({"expected_boot_id":BOOT_A,"reason":"explicit recovery"}),
            ),
        ] {
            let mut request = json!({"client_request_id":"request-method","binding_id":fixture.scope.binding_id,"generation":fixture.scope.generation});
            request
                .as_object_mut()
                .unwrap()
                .extend(fields.as_object().unwrap().clone());
            let mut command = host_command(&fixture, "host-method", method, request);
            // The Host mutation validator and prerequisites::validate_request
            // permit this field on agent.goal, beyond the catalog's field list.
            if method == "agent.goal" {
                command.input["prerequisite_operation_id"] = json!("configured-operation");
            }
            validate_host_envelope(&fixture.scope, &command).unwrap();
            command.input["transcript"] = json!({"text":"must not be accepted"});
            assert!(validate_host_envelope(&fixture.scope, &command).is_err());
        }
    }

    fn family_projector(fixture: &Fixture, boot: &str) -> ObservationProjector {
        use crate::observations::{
            ChildSnapshotInput, NativeEventInput, ReadMethod, ReductionStatus, RootSnapshotInput,
        };
        let f = projection_fence(&fixture.scope, boot);
        let p = ObservationProjector::new(f.clone()).unwrap();
        let event = json!({"method":"item/updated","params":{"sessionId":fixture.scope.native_root_id,"turnId":"parent-turn","viewCursor":"opaque:child","item":{"kind":"subagent","childSessionId":"child","itemId":"parent-item","subagentId":"sub","revision":1,"status":"running","controlStatus":"running"}}});
        let p = p
            .apply_event(NativeEventInput {
                fence: &f,
                event_revision: 1,
                at_ms: 1,
                event: &event,
            })
            .projector;
        let root = fixture.session(&fixture.scope.native_root_id);
        let r = p.apply_root_snapshot(RootSnapshotInput {
            fence: &f,
            expected_session_revision: p.session_revision(&fixture.scope.native_root_id),
            at_ms: 2,
            read_method: ReadMethod::SessionRead,
            snapshot: &root,
        });
        assert_eq!(r.status, ReductionStatus::Applied);
        let p = r.projector;
        let child = fixture.session("child");
        let r = p.apply_child_snapshot(ChildSnapshotInput {
            fence: &f,
            child_session_id: "child",
            parent_session_id: &fixture.scope.native_root_id,
            item_id: "parent-item",
            parent_turn_id: Some("parent-turn"),
            item_revision: 1,
            expected_session_revision: p.session_revision("child"),
            at_ms: 3,
            read_method: ReadMethod::SessionRead,
            snapshot: Some(&child),
            failure: None,
        });
        assert_eq!(r.status, ReductionStatus::Applied);
        r.projector
    }
    fn retain_family(fixture: &Fixture, journal: &mut Journal, with_pending: bool) {
        let projector = family_projector(fixture, &journal.boot);
        let discovery = vec![ChildDiscovery {
            parent_session_id: fixture.scope.native_root_id.clone(),
            after: Some("opaque:after".into()),
            next: Some("opaque:next".into()),
            complete: false,
        }];
        let snapshot = journal
            .capture_projection(&fixture.scope, &projector)
            .unwrap();
        journal
            .retain_observations(&fixture.scope, snapshot, discovery.clone(), Vec::new())
            .unwrap();
        if with_pending {
            let authority = journal.authorize_session(&fixture.scope, "child").unwrap();
            let row = json!({"sessionId":"child","userInputId":"question","itemId":"input-item","toolCallId":"tool-call","toolName":"request","turnId":"child-turn","viewCursor":"opaque:pending","questions":[]});
            let value = json!({"approvals":[],"userInputs":[row]});
            let inventory = journal
                .capture_pending_inventory(&fixture.scope, &authority, 0, 0, value.clone())
                .unwrap();
            let f = projection_fence(&fixture.scope, &journal.boot);
            let reduced = projector.replace_pending(crate::observations::PendingInventoryInput {
                fence: &f,
                session_id: "child",
                expected_session_revision: 0,
                inventory: &value,
            });
            assert_eq!(
                reduced.status,
                crate::observations::ReductionStatus::Applied
            );
            let snapshot = journal
                .capture_projection(&fixture.scope, &reduced.projector)
                .unwrap();
            journal
                .retain_observations(&fixture.scope, snapshot, discovery, vec![inventory])
                .unwrap();
        }
    }
    #[test]
    fn muse_journal_full_schema_ack_keeps_disposition_started_and_apply_outcome() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        drop(
            journal
                .prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone())
                .unwrap(),
        );
        let mut ack = fixture.ack(&invocation);
        for disposition in ["queued", "steered", "started"] {
            ack["disposition"] = json!(disposition);
            ack["startedNewTurn"] = json!(disposition == "started");
            validate_ack(&fixture.scope, &invocation, &ack).unwrap();
        }
        let mut inconsistent = ack.clone();
        inconsistent["startedNewTurn"] = json!(false);
        assert!(validate_ack(&fixture.scope, &invocation, &inconsistent).is_err());
        let mut missing = ack.clone();
        missing.as_object_mut().unwrap().remove("disposition");
        assert!(validate_ack(&fixture.scope, &invocation, &missing).is_err());
        let mut foreign = ack.clone();
        foreign["unrecognized"] = json!({"modelProviderKey":"never"});
        assert!(validate_ack(&fixture.scope, &invocation, &foreign).is_err());
        journal
            .record_native_ack(&fixture.scope, "send", invocation.command_id(), ack.clone())
            .unwrap();
        drop(journal);
        let mut journal = fixture.reopen();
        assert!(
            matches!(journal.explicit_reconcile(&fixture.scope,"send",invocation.payload_sha256()).unwrap(),Reconciliation::AckReadback(saved) if saved==ack)
        );
        let id = "019a0000-0000-7000-8000-000000000002";
        let params =
            json!({"commandId":id,"sessionId":fixture.scope.native_root_id,"mode":"onRequest"});
        let setting = NativeInvocation::new(
            &fixture.scope,
            "session/setApprovalMode",
            id,
            params.as_object().unwrap().clone(),
        )
        .unwrap();
        let mut setting_ack = json!({"commandId":id,"status":"accepted","applyOutcome":"noop","effectiveMode":{"mode":"onRequest","source":"session","lastCommandId":id}});
        // The enum is read from the complete public schema, not hand guessed.
        let source = schema::definition_schema("ApprovalModeSource").unwrap()["enum"][0].clone();
        setting_ack["effectiveMode"]["source"] = source;
        validate_ack(&fixture.scope, &setting, &setting_ack).unwrap();
        let mut bad = setting_ack.clone();
        bad["applyOutcome"] = json!("invented");
        assert!(validate_ack(&fixture.scope, &setting, &bad).is_err());
        let params = json!({"commandId":id,"sessionId":fixture.scope.native_root_id,"approvalId":"approval","choiceId":"choice","requirementId":{"approvalId":"approval","sourceIndex":0}});
        let reply = NativeInvocation::new(
            &fixture.scope,
            "approval/decide",
            id,
            params.as_object().unwrap().clone(),
        )
        .unwrap();
        validate_ack(
            &fixture.scope,
            &reply,
            &json!({"commandId":id,"status":"accepted","approvalId":"approval","terminal":false}),
        )
        .unwrap();
    }
    #[test]
    fn muse_journal_model_object_and_real_mcp_grant_are_retained_without_flattening() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        fixture.initialize(&mut journal, false);
        fixture.work_read(&mut journal, 0);
        let id = "019a0000-0000-7000-8000-000000000003";
        let params = json!({"commandId":id,"sessionId":fixture.scope.native_root_id,"model":{"modelId":"selected","providerId":"requested-provider"}});
        let invocation = NativeInvocation::new(
            &fixture.scope,
            "session/setModel",
            id,
            params.as_object().unwrap().clone(),
        )
        .unwrap();
        let command = fixture.command(
            "configure",
            "agent.configure",
            json!({"settings":{"model":params["model"]}}),
        );
        drop(
            journal
                .prepare_command(
                    &fixture.scope,
                    fixture.receipt("configure"),
                    command,
                    invocation.clone(),
                    RetainedSelectors::default(),
                )
                .unwrap(),
        );
        assert_eq!(
            journal
                .retained_invocation(&fixture.scope, "configure")
                .unwrap()
                .params()
                .unwrap()["model"],
            params["model"]
        );
        let mut bad = params.clone();
        bad["model"]["modelProviderKey"] = json!("forbidden");
        assert!(
            NativeInvocation::new(
                &fixture.scope,
                "session/setModel",
                id,
                bad.as_object().unwrap().clone()
            )
            .is_err()
        );
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        fixture.initialize(&mut journal, false);
        let servers = json!({"explicit":{"transport":"stdio","command":"explicit-binary","args":["--explicit"],"mode":"required"}});
        let mut command = fixture.command("open", "agent.open", json!({}));
        command.route["native_options"]["mcpServers"] = servers.clone();
        let base = fixture.invocation(4, "session/start");
        let mut params = base.params().unwrap();
        params.insert("config".into(), json!({"mcpServers":servers}));
        assert!(matches!(
            NativeInvocation::new(
                &fixture.scope,
                "session/start",
                base.command_id(),
                params.clone()
            ),
            Err(Fault::GrantRequired)
        ));
        assert!(matches!(
            journal.native_invocation(
                &fixture.scope,
                "session/start",
                base.command_id(),
                params.clone()
            ),
            Err(Fault::GrantRequired)
        ));
        fixture.initialize(&mut journal, true);
        let invocation = journal
            .native_invocation(
                &fixture.scope,
                "session/start",
                base.command_id(),
                params.clone(),
            )
            .unwrap();
        fixture.initialize(&mut journal, false);
        assert!(matches!(
            journal.prepare_command(
                &fixture.scope,
                fixture.receipt("open"),
                command.clone(),
                invocation.clone(),
                RetainedSelectors::default()
            ),
            Err(Fault::GrantRequired)
        ));
        fixture.initialize(&mut journal, true);
        let mut invalid = Value::Object(params.clone());
        invalid["config"]["mcpServers"]["explicit"]["transport"] = json!("futureTransport");
        assert!(matches!(
            journal.native_invocation(
                &fixture.scope,
                "session/start",
                base.command_id(),
                invalid.as_object().unwrap().clone()
            ),
            Err(Fault::InvalidState)
        ));
        let mut private = Value::Object(params);
        private["config"]["modelProviderKey"] = json!("forbidden");
        assert!(matches!(
            journal.native_invocation(
                &fixture.scope,
                "session/start",
                base.command_id(),
                private.as_object().unwrap().clone()
            ),
            Err(Fault::InvalidState)
        ));
        drop(
            journal
                .prepare_command(
                    &fixture.scope,
                    fixture.receipt("open"),
                    command,
                    invocation.clone(),
                    RetainedSelectors::default(),
                )
                .unwrap(),
        );
        drop(journal);
        let mut journal = fixture.reopen();
        fixture.initialize(&mut journal, false);
        assert!(matches!(
            journal.explicit_reconcile(&fixture.scope, "open", invocation.payload_sha256()),
            Err(Fault::GrantRequired)
        ));
        fixture.initialize(&mut journal, true);
        match journal
            .explicit_reconcile(&fixture.scope, "open", invocation.payload_sha256())
            .unwrap()
        {
            Reconciliation::Write(p) => assert_eq!(p.invocation(), &invocation),
            _ => panic!("explicit exact start only"),
        }
    }

    #[test]
    fn muse_journal_inline_mcp_auth_and_all_nonempty_header_env_maps_never_publish() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        fixture.initialize(&mut journal, true);
        let before = read_private_durable(&fixture.directory.join(CHECKPOINT), MAX_FILE).unwrap();
        // Synthetic values only. Even neutral additions are outside this closed
        // retention profile; names are not treated as an exhaustive secret list.
        let cases = [
            json!({"http":{"transport":"streamableHttp","url":"https://example.invalid/mcp","headers":{"Authorization":"Bearer synthetic-not-a-real-token"}}}),
            json!({"http":{"transport":"streamableHttp","url":"https://example.invalid/mcp","headers":{"X-Request-Tag":"synthetic-non-secret"}}}),
            json!({"stdio":{"transport":"stdio","command":"explicit-binary","env":{"MCP_TOKEN":"synthetic-not-a-real-token"}}}),
            json!({"stdio":{"transport":"stdio","command":"explicit-binary","env":{"SAFE_MODE":"synthetic-non-secret"}}}),
            json!({"http":{"transport":"streamableHttp","url":"https://synthetic:credential@example.invalid/mcp"}}),
            json!({"http":{"transport":"streamableHttp","url":"https://example.invalid/mcp?token=synthetic"}}),
        ];
        for (index, servers) in cases.into_iter().enumerate() {
            let base = fixture.invocation(index as u64 + 20, "session/start");
            let mut params = base.params().unwrap();
            params.insert("config".into(), json!({"mcpServers":servers}));
            assert!(matches!(
                journal.native_invocation(
                    &fixture.scope,
                    "session/start",
                    base.command_id(),
                    params.clone()
                ),
                Err(Fault::InvalidState)
            ));
            // Also exercise the effect gate on an untrusted/deserialized value,
            // bypassing the public constructor only inside this private fixture.
            let value = Value::Object(params);
            let untrusted = NativeInvocation {
                method: "session/start".into(),
                command_id: base.command_id().into(),
                params: SavedJson::new(&value).unwrap(),
                payload_sha256: digest(&json!({"method":"session/start","params":value})).unwrap(),
            };
            let operation = format!("rejected-{index}");
            let mut command = fixture.command(&operation, "agent.open", json!({}));
            command.route["native_options"]["mcpServers"] = servers;
            assert!(matches!(
                journal.prepare_command(
                    &fixture.scope,
                    fixture.receipt(&operation),
                    command,
                    untrusted,
                    RetainedSelectors::default()
                ),
                Err(Fault::InvalidState)
            ));
            assert_eq!(
                read_private_durable(&fixture.directory.join(CHECKPOINT), MAX_FILE).unwrap(),
                before
            );
            assert!(
                journal
                    .unresolved_native(&fixture.scope)
                    .unwrap()
                    .is_empty()
            );
            assert!(journal.state.operations.is_empty());
            assert!(journal.state.contexts.is_empty());
            assert!(journal.outbox(&fixture.scope).unwrap().is_empty());
        }
    }
    #[test]
    fn muse_journal_inline_mcp_auth_in_original_route_never_enters_context() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        fixture.initialize(&mut journal, true);
        fixture.work_read(&mut journal, 0);
        let before = read_private_durable(&fixture.directory.join(CHECKPOINT), MAX_FILE).unwrap();
        // agent.send does not put MCP config in its native request. Its original
        // route is independently checked before that route can be checkpointed.
        let mut command =
            fixture.command("send", "agent.send", json!({"text":"exact fixture input"}));
        command.route["native_options"]["mcpServers"] = json!({"http":{"transport":"streamableHttp","url":"https://example.invalid/mcp","headers":{"Authorization":"Bearer synthetic-not-a-real-token"}}});
        assert!(matches!(
            journal.prepare_command(
                &fixture.scope,
                fixture.receipt("send"),
                command,
                fixture.invocation(30, "turn/start"),
                RetainedSelectors::default()
            ),
            Err(Fault::InvalidState)
        ));
        assert_eq!(
            read_private_durable(&fixture.directory.join(CHECKPOINT), MAX_FILE).unwrap(),
            before
        );
        assert!(
            journal
                .unresolved_native(&fixture.scope)
                .unwrap()
                .is_empty()
        );
        assert!(journal.state.operations.is_empty());
        assert!(journal.state.contexts.is_empty());
    }
    #[test]
    fn muse_journal_nonsecret_mcp_profile_retains_exact_empty_maps_without_rewrite() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        fixture.initialize(&mut journal, true);
        let servers = json!({
            "stdio":{"transport":"stdio","command":"explicit-binary","args":["--explicit"],"mode":"required","env":{}},
            "http":{"transport":"streamableHttp","url":"https://example.invalid/mcp","mode":"optional","headers":{}}
        });
        let base = fixture.invocation(31, "session/start");
        let mut params = base.params().unwrap();
        params.insert("config".into(), json!({"mcpServers":servers}));
        let invocation = journal
            .native_invocation(
                &fixture.scope,
                "session/start",
                base.command_id(),
                params.clone(),
            )
            .unwrap();
        assert_eq!(invocation.params().unwrap(), params);
        let mut command = fixture.command("open", "agent.open", json!({}));
        command.route["native_options"]["mcpServers"] = servers;
        drop(
            journal
                .prepare_command(
                    &fixture.scope,
                    fixture.receipt("open"),
                    command.clone(),
                    invocation,
                    RetainedSelectors::default(),
                )
                .unwrap(),
        );
        assert_eq!(
            journal
                .retained_invocation(&fixture.scope, "open")
                .unwrap()
                .params()
                .unwrap(),
            params
        );
        assert_eq!(
            serde_json::to_value(
                journal
                    .retained_command(&fixture.scope, "open")
                    .unwrap()
                    .unwrap()
                    .command()
            )
            .unwrap(),
            serde_json::to_value(command).unwrap()
        );
        drop(journal);
        let journal = fixture.reopen();
        assert_eq!(
            journal
                .retained_invocation(&fixture.scope, "open")
                .unwrap()
                .params()
                .unwrap(),
            params
        );
    }

    #[test]
    fn muse_journal_task_context_cross_worker_preserves_original_prompt_receipt_and_native_id() {
        use sha2::{Digest, Sha256};
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        fixture.initialize(&mut journal, false);
        fixture.work_read(&mut journal, 7);
        let invocation = fixture.invocation(5, "turn/start");
        let prompt = "exact fixture input";
        let envelope = json!({"schema_id":"swarm.task_prompt","schema_version":1,"task_id":"task","task_revision":2,"attempt_id":"attempt","task_snapshot_sha256":"a".repeat(64),"prompt_sha256":format!("{:x}",Sha256::digest(prompt.as_bytes())),"prompt_bytes":prompt.len(),"prompt":prompt});
        let context = json!({"schema_version":1,"operation_id":"dispatch","binding_id":fixture.scope.binding_id,"binding_generation":fixture.scope.generation,"worker_boot_id":BOOT_A,"attempt_id":"attempt","task_id":"task","task_revision":2,"task_snapshot_sha256":"a".repeat(64),"source_text_sha256":format!("{:x}",Sha256::digest(b"abc")),"source_text_bytes":3});
        let command = fixture.command(
            "dispatch",
            "task.dispatch",
            json!({"text":"abc","task_prompt":envelope,"task_dispatch_context":context}),
        );
        drop(
            journal
                .prepare_command(
                    &fixture.scope,
                    fixture.receipt("dispatch"),
                    command.clone(),
                    invocation.clone(),
                    RetainedSelectors::default(),
                )
                .unwrap(),
        );
        let saved = journal
            .retained_command(&fixture.scope, "dispatch")
            .unwrap()
            .unwrap();
        assert_eq!(saved.original_receipt(), &fixture.receipt("dispatch"));
        assert_eq!(
            saved
                .task_prompt(&fixture.scope)
                .unwrap()
                .unwrap()
                .context
                .worker_boot_id,
            BOOT_A
        );
        drop(journal);
        let mut journal = fixture.reopen();
        let saved = journal
            .retained_command(&fixture.scope, "dispatch")
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(saved.command()).unwrap(),
            serde_json::to_value(&command).unwrap()
        );
        let mut request = fixture.command(
            "reconcile",
            "agent.reconcile",
            json!({"operation_id":"dispatch"}),
        );
        request.target_input_sha256 = Some(fixture.receipt("dispatch").input_sha256);
        assert!(matches!(
            journal.reconcile_command(&fixture.scope, &request, &fixture.receipt("reconcile")),
            Err(Fault::DurabilityUnavailable)
        ));
        fixture.initialize(&mut journal, false);
        assert!(matches!(
            journal.reconcile_command(&fixture.scope, &request, &fixture.receipt("reconcile")),
            Err(Fault::FreshChecksRequired)
        ));
        fixture.work_read(&mut journal, 99);
        let mut bad = request.clone();
        bad.target_input_sha256 = Some("b".repeat(64));
        assert!(matches!(
            journal.reconcile_command(&fixture.scope, &bad, &fixture.receipt("reconcile")),
            Err(Fault::ScopeMismatch)
        ));
        match journal
            .reconcile_command(&fixture.scope, &request, &fixture.receipt("reconcile"))
            .unwrap()
        {
            Reconciliation::Write(p) => assert_eq!(p.invocation(), &invocation),
            _ => panic!("same original native intent required"),
        }
        assert_eq!(
            journal
                .retained_command(&fixture.scope, "dispatch")
                .unwrap()
                .unwrap()
                .task_prompt(&fixture.scope)
                .unwrap()
                .unwrap()
                .context
                .worker_boot_id,
            BOOT_A
        );
    }
    #[test]
    fn muse_journal_child_result_authority_revokes_on_events_and_restores_stale_paired_facts() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        fixture.initialize(&mut journal, false);
        retain_family(&fixture, &mut journal, true);
        let authority = journal.authorize_session(&fixture.scope, "child").unwrap();
        assert!(
            journal
                .authorize_session(&fixture.scope, "arbitrary")
                .is_err()
        );
        let page = json!({"source":{"native_session_id":"child","item_id":"answer","item_revision":7},"content_base64":"ZXhhY3Q=","offset_bytes":0,"eof":true});
        assert_eq!(
            journal.queue_result(&fixture.scope, fixture.receipt("read"), page.clone()),
            Err(Fault::ScopeMismatch)
        );
        journal
            .queue_result_for(
                &fixture.scope,
                fixture.receipt("read"),
                page.clone(),
                &authority,
            )
            .unwrap();
        let ticket = journal.outbox(&fixture.scope).unwrap().remove(0);
        journal.invalidate_observations(&fixture.scope).unwrap();
        assert!(!journal.authority_is_current(&fixture.scope, &authority));
        assert_eq!(
            journal.queue_result_for(&fixture.scope, fixture.receipt("read2"), page, &authority),
            Err(Fault::ProvenanceRequired)
        );
        drop(journal);
        let mut journal = fixture.reopen();
        fixture.initialize(&mut journal, false);
        let restored = journal
            .restored_observations(&fixture.scope)
            .unwrap()
            .unwrap();
        assert_eq!(restored.freshness, RestoredFreshness::Stale);
        assert_eq!(
            restored.projection["state"]["root_snapshot_freshness"],
            "stale"
        );
        assert_eq!(
            restored.projection["state"]["observed_children"][0]["snapshot_freshness"],
            "stale"
        );
        assert_eq!(
            restored.projection["state"]["family_completeness"],
            "partial"
        );
        assert_eq!(restored.discovery[0].after.as_deref(), Some("opaque:after"));
        assert_eq!(restored.discovery[0].next.as_deref(), Some("opaque:next"));
        assert_eq!(restored.inventories.len(), 1);
        assert!(journal.authorize_session(&fixture.scope, "child").is_err());
        let projector = family_projector(&fixture, BOOT_B);
        let snapshot = journal
            .capture_projection(&fixture.scope, &projector)
            .unwrap();
        assert_eq!(
            journal.retain_observations(
                &fixture.scope,
                snapshot,
                restored.discovery,
                restored.inventories
            ),
            Err(Fault::FreshChecksRequired)
        );
        assert_eq!(
            journal.outbox(&fixture.scope).unwrap(),
            vec![ticket.clone()]
        );
        journal.acknowledge(&fixture.scope, &ticket).unwrap();
    }
    #[test]
    fn muse_journal_child_reply_requires_exact_fresh_inventory_and_original_edge() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        fixture.initialize(&mut journal, false);
        let id = "019a0000-0000-7000-8000-000000000006";
        let params = json!({"commandId":id,"sessionId":"child","userInputId":"question"});
        let invocation = NativeInvocation::new(
            &fixture.scope,
            "userInput/cancel",
            id,
            params.as_object().unwrap().clone(),
        )
        .unwrap();
        let command=fixture.command("reply","agent.reply",json!({"reply":{"method":"userInput/cancel","params":{"sessionId":"child","userInputId":"question"}}}));
        assert!(matches!(
            journal.prepare_native(&fixture.scope, fixture.receipt("reply"), invocation.clone()),
            Err(Fault::ProvenanceRequired)
        ));
        assert!(
            journal
                .prepare_command(
                    &fixture.scope,
                    fixture.receipt("reply"),
                    command.clone(),
                    invocation.clone(),
                    RetainedSelectors::default()
                )
                .is_err()
        );
        retain_family(&fixture, &mut journal, true);
        let mut wrong = command.clone();
        wrong.input["reply"]["params"]["userInputId"] = json!("other");
        assert!(
            journal
                .prepare_command(
                    &fixture.scope,
                    fixture.receipt("reply"),
                    wrong,
                    invocation.clone(),
                    RetainedSelectors::default()
                )
                .is_err()
        );
        drop(
            journal
                .prepare_command(
                    &fixture.scope,
                    fixture.receipt("reply"),
                    command,
                    invocation.clone(),
                    RetainedSelectors::default(),
                )
                .unwrap(),
        );
        drop(journal);
        let mut journal = fixture.reopen();
        fixture.initialize(&mut journal, false);
        assert!(matches!(
            journal.explicit_reconcile(&fixture.scope, "reply", invocation.payload_sha256()),
            Err(Fault::FreshChecksRequired)
        ));
        retain_family(&fixture, &mut journal, true);
        match journal
            .explicit_reconcile(&fixture.scope, "reply", invocation.payload_sha256())
            .unwrap()
        {
            Reconciliation::Write(p) => assert_eq!(p.invocation(), &invocation),
            _ => panic!("exact observed child command"),
        }
    }
    #[test]
    fn muse_journal_v1_new_rust_state_remains_readable_but_never_infers_context() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(7, "turn/start");
        drop(
            journal
                .prepare_native(&fixture.scope, fixture.receipt("old"), invocation.clone())
                .unwrap(),
        );
        journal
            .queue_outcome(
                &fixture.scope,
                fixture.receipt("old"),
                fixture.outcome("old", EffectOutcome::Unknown),
            )
            .unwrap();
        let ticket = journal.outbox(&fixture.scope).unwrap().remove(0);
        drop(journal);
        let path = fixture.directory.join(CHECKPOINT);
        let mut value: Value =
            serde_json::from_slice(&read_private_durable(&path, MAX_FILE).unwrap()).unwrap();
        value["version"] = json!(1);
        value["sha256"] = json!(
            digest(
                &json!({"format":FORMAT,"version":1,"scope":value["scope"],"state":value["state"]})
            )
            .unwrap()
        );
        replace_private_durable(&path, &serde_json::to_vec(&value).unwrap()).unwrap();
        let mut journal = fixture.reopen();
        assert_eq!(
            journal.outbox(&fixture.scope).unwrap(),
            vec![ticket.clone()]
        );
        fixture.initialize(&mut journal, false);
        assert!(matches!(
            journal.explicit_reconcile(&fixture.scope, "old", invocation.payload_sha256()),
            Err(Fault::ContextRequired)
        ));
        assert!(
            journal
                .retained_command(&fixture.scope, "old")
                .unwrap()
                .is_none()
        );
        journal.acknowledge(&fixture.scope, &ticket).unwrap();
        assert_eq!(
            journal.unresolved_native(&fixture.scope).unwrap(),
            vec![("old", BOOT_A, false)]
        );
    }
    #[test]
    fn muse_journal_context_and_intent_publish_atomically_and_poison_is_sticky() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        fixture.initialize(&mut journal, false);
        let invocation = fixture.invocation(8, "session/start");
        let failed_path = fixture.directory.join(CHECKPOINT);
        let hook = scoped_durability_barrier(move |path| {
            if path == failed_path {
                Err(swarm_contracts::error::Error::new("INJECTED", "fixture"))
            } else {
                Ok(())
            }
        });
        assert!(matches!(
            journal.prepare_command(
                &fixture.scope,
                fixture.receipt("open"),
                fixture.command("open", "agent.open", json!({})),
                invocation.clone(),
                RetainedSelectors::default()
            ),
            Err(Fault::PersistenceUnknown)
        ));
        assert!(matches!(
            journal.retained_command(&fixture.scope, "open"),
            Err(Fault::Poisoned)
        ));
        drop(hook);
        drop(journal);
        let mut journal = fixture.reopen();
        assert!(
            journal
                .retained_command(&fixture.scope, "open")
                .unwrap()
                .is_some()
        );
        assert_eq!(
            journal.unresolved_native(&fixture.scope).unwrap(),
            vec![("open", BOOT_A, false)]
        );
        assert!(matches!(
            journal.explicit_reconcile(&fixture.scope, "open", invocation.payload_sha256()),
            Err(Fault::DurabilityUnavailable)
        ));
    }
    #[test]
    fn muse_journal_capacity_refuses_new_effect_without_evicting_any_unknown() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let mut next = journal.state.clone();
        for n in 0..MAX_OPERATIONS {
            let id = format!("unknown-{n}");
            let invocation = fixture.invocation(n as u64 + 100, "turn/start");
            next.operations.insert(
                id.clone(),
                Operation {
                    receipt: fixture.receipt(&id),
                    native: Some(NativePending {
                        invocation,
                        submission_boot: BOOT_A.into(),
                        ack: None,
                        reconcile_block: None,
                    }),
                    final_delivery: None,
                    final_host_acked: false,
                    family: None,
                    result_command: None,
                },
            );
        }
        journal.commit(next).unwrap();
        let before = read_private_durable(&fixture.directory.join(CHECKPOINT), MAX_FILE).unwrap();
        assert!(matches!(
            journal.prepare_native(
                &fixture.scope,
                fixture.receipt("overflow"),
                fixture.invocation(999, "turn/start")
            ),
            Err(Fault::Capacity)
        ));
        assert_eq!(
            read_private_durable(&fixture.directory.join(CHECKPOINT), MAX_FILE).unwrap(),
            before
        );
        assert_eq!(
            journal.unresolved_native(&fixture.scope).unwrap().len(),
            MAX_OPERATIONS
        );
    }

    #[test]
    fn muse_journal_capture_race_and_duplicate_inventory_fail_before_publication() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        fixture.initialize(&mut journal, false);
        let p = family_projector(&fixture, BOOT_A);
        let snapshot = journal.capture_projection(&fixture.scope, &p).unwrap();
        journal.invalidate_observations(&fixture.scope).unwrap();
        let before = read_private_durable(&fixture.directory.join(CHECKPOINT), MAX_FILE).unwrap();
        assert_eq!(
            journal.retain_observations(&fixture.scope, snapshot, Vec::new(), Vec::new()),
            Err(Fault::FreshChecksRequired)
        );
        assert_eq!(
            read_private_durable(&fixture.directory.join(CHECKPOINT), MAX_FILE).unwrap(),
            before
        );
        retain_family(&fixture, &mut journal, false);
        let authority = journal.authorize_session(&fixture.scope, "child").unwrap();
        assert!(
            journal
                .capture_pending_inventory(
                    &fixture.scope,
                    &authority,
                    1,
                    2,
                    json!({"approvals":[],"userInputs":[]})
                )
                .is_err()
        );
        retain_family(&fixture, &mut journal, false);
        let authority = journal.authorize_session(&fixture.scope, "child").unwrap();
        let row = json!({"sessionId":"child","userInputId":"same","itemId":"item","questions":[],"toolCallId":"call","toolName":"tool","turnId":"turn","viewCursor":"cursor"});
        assert!(
            journal
                .capture_pending_inventory(
                    &fixture.scope,
                    &authority,
                    0,
                    0,
                    json!({"approvals":[],"userInputs":[row.clone(),row]})
                )
                .is_err()
        );
    }
    #[test]
    fn muse_journal_context_native_identity_is_checked_even_with_recomputed_outer_hash() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        fixture.initialize(&mut journal, false);
        let invocation = fixture.invocation(10, "session/start");
        drop(
            journal
                .prepare_command(
                    &fixture.scope,
                    fixture.receipt("open"),
                    fixture.command("open", "agent.open", json!({})),
                    invocation,
                    RetainedSelectors::default(),
                )
                .unwrap(),
        );
        drop(journal);
        let path = fixture.directory.join(CHECKPOINT);
        let mut value: Value =
            serde_json::from_slice(&read_private_durable(&path, MAX_FILE).unwrap()).unwrap();
        value["state"]["contexts"]["open"]["native_command_id"] =
            json!("019a0000-0000-7000-8000-000000009999");
        value["sha256"]=json!(digest(&json!({"format":FORMAT,"version":CHECKPOINT_VERSION,"scope":value["scope"],"state":value["state"]})).unwrap());
        replace_private_durable(&path, &serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(matches!(
            Journal::open_at(&fixture.directory, fixture.scope.clone(), BOOT_B.into()),
            Err(Fault::IntentConflict)
        ));
    }
    #[cfg(unix)]
    #[test]
    fn muse_journal_rejects_linked_checkpoint_and_linked_ancestor_without_adoption() {
        let fixture = Fixture::new();
        drop(fixture.create());
        let path = fixture.directory.join(CHECKPOINT);
        let saved = fixture.directory.join("saved.json");
        fs::rename(&path, &saved).unwrap();
        std::os::unix::fs::symlink(&saved, &path).unwrap();
        assert!(matches!(
            Journal::open_at(&fixture.directory, fixture.scope.clone(), BOOT_B.into()),
            Err(Fault::PersistenceUnknown)
        ));
        let other = Fixture::new();
        let alias = other.directory.join("alias");
        std::os::unix::fs::symlink(&fixture.directory, &alias).unwrap();
        assert!(Journal::open_at(&alias, fixture.scope.clone(), BOOT_B.into()).is_err());
        assert!(!read_private_durable(&saved, MAX_FILE).unwrap().is_empty());
    }

    #[test]
    fn muse_journal_reopen_never_resends_and_requires_exact_explicit_reconciliation() {
        let fixture = Fixture::new();
        let invocation = fixture.invocation(1, "turn/start");
        let mut journal = fixture.create();
        drop(
            journal
                .prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone())
                .unwrap(),
        );
        drop(journal);
        let mut journal = fixture.reopen();
        assert_eq!(
            journal.unresolved_native(&fixture.scope).unwrap(),
            vec![("send", BOOT_A, false)]
        );
        assert!(journal.outbox(&fixture.scope).unwrap().is_empty());
        assert_eq!(
            journal
                .native_payload_sha256(&fixture.scope, "send")
                .unwrap(),
            invocation.payload_sha256()
        );
        assert!(matches!(
            journal.prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone()),
            Err(Fault::ExplicitReconcileRequired)
        ));
        assert!(matches!(
            journal.explicit_reconcile(&fixture.scope, "send", "wrong-digest"),
            Err(Fault::IntentConflict)
        ));
        assert!(matches!(
            journal.explicit_reconcile(&fixture.scope, "send", invocation.payload_sha256()),
            Err(Fault::DurabilityUnavailable)
        ));
        journal
            .record_durability(&fixture.scope, Durability::Durable)
            .unwrap();
        assert!(matches!(
            journal.explicit_reconcile(&fixture.scope, "send", invocation.payload_sha256()),
            Err(Fault::ContextRequired)
        ));
        assert_eq!(
            journal.unresolved_native(&fixture.scope).unwrap()[0].1,
            BOOT_A
        );
    }

    #[test]
    fn muse_journal_wrong_scope_or_receipt_cannot_prepare_or_adopt_state() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        let mut scopes = Vec::new();
        let mut scope = fixture.scope.clone();
        scope.generation += 1;
        scopes.push(scope);
        let mut scope = fixture.scope.clone();
        scope.native_namespace.push_str("-foreign");
        scopes.push(scope);
        let mut scope = fixture.scope.clone();
        scope.native_root_id.push_str("-foreign");
        scopes.push(scope);
        let mut scope = fixture.scope.clone();
        scope.artifact.version = ArtifactVersion::new("0.2.0").unwrap();
        scopes.push(scope);
        let mut scope = fixture.scope.clone();
        scope.module_client_id.push_str("-foreign");
        scopes.push(scope);
        for scope in scopes {
            assert!(matches!(
                Journal::open_at(&fixture.directory, scope.clone(), BOOT_B.into()),
                Err(Fault::ScopeMismatch)
            ));
            assert!(matches!(
                journal.prepare_native(&scope, fixture.receipt("send"), invocation.clone()),
                Err(Fault::ScopeMismatch)
            ));
        }
        let mut receipt = fixture.receipt("send");
        receipt.binding_generation += 1;
        assert!(matches!(
            journal.prepare_native(&fixture.scope, receipt, invocation),
            Err(Fault::ScopeMismatch)
        ));
        assert!(
            journal
                .unresolved_native(&fixture.scope)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn muse_journal_missing_or_legacy_state_never_becomes_fresh() {
        let fixture = Fixture::new();
        fs::write(
            fixture.directory.join("checkpoint.json"),
            b"legacy sentinel; not a Rust checkpoint",
        )
        .unwrap();
        assert!(matches!(
            Journal::create_at(&fixture.directory, fixture.scope.clone(), BOOT_A.into()),
            Err(Fault::LegacyState)
        ));
        assert!(!fixture.directory.join(MARKER).exists());
        assert_eq!(
            fs::read(fixture.directory.join("checkpoint.json")).unwrap(),
            b"legacy sentinel; not a Rust checkpoint"
        );
        let fixture = Fixture::new();
        drop(fixture.create());
        fs::remove_file(fixture.directory.join(CHECKPOINT)).unwrap();
        assert!(matches!(
            Journal::open_at(&fixture.directory, fixture.scope.clone(), BOOT_B.into()),
            Err(Fault::MissingState)
        ));
        assert!(matches!(
            Journal::create_at(&fixture.directory, fixture.scope.clone(), BOOT_B.into()),
            Err(Fault::StateAlreadyExists)
        ));
    }

    #[test]
    fn muse_journal_failure_before_publication_prevents_mutation_and_poison_is_sticky() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let path = fixture.directory.join(CHECKPOINT);
        let saved = fs::read(&path).unwrap();
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        let attempted_native_write = Cell::new(false);
        if let Ok(_prepared) = journal.prepare_native(
            &fixture.scope,
            fixture.receipt("send"),
            fixture.invocation(1, "turn/start"),
        ) {
            attempted_native_write.set(true);
        }
        assert!(!attempted_native_write.get());
        assert!(matches!(
            journal.prepare_native(
                &fixture.scope,
                fixture.receipt("another"),
                fixture.invocation(2, "turn/start")
            ),
            Err(Fault::Poisoned)
        ));
        fs::remove_dir(&path).unwrap();
        write_private_new(&path, &saved).unwrap();
        assert!(matches!(
            journal.prepare_native(
                &fixture.scope,
                fixture.receipt("another"),
                fixture.invocation(2, "turn/start")
            ),
            Err(Fault::Poisoned)
        ));
        assert!(
            fixture
                .reopen()
                .unresolved_native(&fixture.scope)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn muse_journal_postpublication_uncertainty_retains_unknown_prior_write() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        let failed_path = fixture.directory.join(CHECKPOINT);
        let hook = scoped_durability_barrier(move |path| {
            if path == failed_path {
                Err(swarm_contracts::error::Error::new(
                    "FIXTURE_SYNC_FAILURE",
                    "fixture",
                ))
            } else {
                Ok(())
            }
        });
        assert!(matches!(
            journal.prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone()),
            Err(Fault::PersistenceUnknown)
        ));
        assert!(matches!(
            journal.outbox(&fixture.scope),
            Err(Fault::Poisoned)
        ));
        drop(hook);
        drop(journal);
        let mut journal = fixture.reopen();
        assert_eq!(
            journal.unresolved_native(&fixture.scope).unwrap(),
            vec![("send", BOOT_A, false)]
        );
        assert!(matches!(
            journal.prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone()),
            Err(Fault::ExplicitReconcileRequired)
        ));
        journal
            .record_durability(&fixture.scope, Durability::Durable)
            .unwrap();
        assert!(matches!(
            journal.explicit_reconcile(&fixture.scope, "send", invocation.payload_sha256()),
            Err(Fault::ContextRequired)
        ));
    }

    #[test]
    fn muse_journal_native_ack_is_retained_before_outcome_and_never_resubmitted() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        drop(
            journal
                .prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone())
                .unwrap(),
        );
        assert_eq!(
            journal.record_native_ack(&fixture.scope, "send", invocation.command_id(), json!({})),
            Err(Fault::InvalidState)
        );
        let mut foreign_ack = fixture.ack(&invocation);
        foreign_ack["commandId"] = json!(fixture.invocation(2, "turn/start").command_id());
        assert_eq!(
            journal.record_native_ack(&fixture.scope, "send", invocation.command_id(), foreign_ack),
            Err(Fault::InvalidState)
        );
        let ack = fixture.ack(&invocation);
        journal
            .record_native_ack(&fixture.scope, "send", invocation.command_id(), ack.clone())
            .unwrap();
        journal
            .record_native_ack(&fixture.scope, "send", invocation.command_id(), ack.clone())
            .unwrap();
        let mut changed = ack.clone();
        changed["turnId"] = json!("different-turn");
        assert_eq!(
            journal.record_native_ack(&fixture.scope, "send", invocation.command_id(), changed),
            Err(Fault::AckConflict)
        );
        drop(journal);
        let mut journal = fixture.reopen();
        assert!(
            journal.outbox(&fixture.scope).unwrap().is_empty(),
            "native ACK is not an invented applied outcome"
        );
        match journal
            .explicit_reconcile(&fixture.scope, "send", invocation.payload_sha256())
            .unwrap()
        {
            Reconciliation::AckReadback(saved) => assert_eq!(saved, ack),
            _ => panic!("ACK-backed command must use readback, never a setter/input resend"),
        }
    }

    #[test]
    fn muse_journal_uncertain_ack_publication_reopens_as_readback_only() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        drop(
            journal
                .prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone())
                .unwrap(),
        );
        let failed_path = fixture.directory.join(CHECKPOINT);
        let hook = scoped_durability_barrier(move |path| {
            if path == failed_path {
                Err(swarm_contracts::error::Error::new(
                    "FIXTURE_SYNC_FAILURE",
                    "fixture",
                ))
            } else {
                Ok(())
            }
        });
        let ack = fixture.ack(&invocation);
        assert_eq!(
            journal.record_native_ack(&fixture.scope, "send", invocation.command_id(), ack.clone()),
            Err(Fault::PersistenceUnknown)
        );
        assert!(matches!(
            journal.prepare_native(
                &fixture.scope,
                fixture.receipt("other"),
                fixture.invocation(2, "turn/start")
            ),
            Err(Fault::Poisoned)
        ));
        drop(hook);
        drop(journal);
        let mut journal = fixture.reopen();
        match journal
            .explicit_reconcile(&fixture.scope, "send", invocation.payload_sha256())
            .unwrap()
        {
            Reconciliation::AckReadback(saved) => assert_eq!(saved, ack),
            _ => panic!("durably revalidated ACK must not trigger native replay"),
        }
    }

    #[test]
    fn muse_journal_lost_host_ack_retains_exact_delivery_and_unknown_native_intent() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        drop(
            journal
                .prepare_native(&fixture.scope, fixture.receipt("send"), invocation)
                .unwrap(),
        );
        journal
            .queue_outcome(
                &fixture.scope,
                fixture.receipt("send"),
                fixture.outcome("send", EffectOutcome::Unknown),
            )
            .unwrap();
        let sent = journal.outbox(&fixture.scope).unwrap().remove(0);
        drop(journal); // Host may have committed, but its ACK was lost.
        let mut journal = fixture.reopen();
        assert_eq!(journal.outbox(&fixture.scope).unwrap(), vec![sent.clone()]);
        journal
            .queue_outcome(
                &fixture.scope,
                fixture.receipt("send"),
                fixture.outcome("send", EffectOutcome::Unknown),
            )
            .unwrap();
        assert_eq!(
            journal.outbox(&fixture.scope).unwrap(),
            vec![sent.clone()],
            "unchanged delivery is stable"
        );
        assert_eq!(
            journal.acknowledge(&fixture.scope, &sent).unwrap(),
            AckDisposition::Retired
        );
        assert!(journal.outbox(&fixture.scope).unwrap().is_empty());
        assert_eq!(
            journal.unresolved_native(&fixture.scope).unwrap(),
            vec![("send", BOOT_A, false)]
        );
    }

    #[test]
    fn muse_journal_stale_ack_cannot_retire_new_outcome_or_downgrade_terminal_fact() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        drop(
            journal
                .prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone())
                .unwrap(),
        );
        journal
            .queue_outcome(
                &fixture.scope,
                fixture.receipt("send"),
                fixture.outcome("send", EffectOutcome::Unknown),
            )
            .unwrap();
        let old = journal.outbox(&fixture.scope).unwrap().remove(0);
        journal
            .queue_outcome(
                &fixture.scope,
                fixture.receipt("send"),
                fixture.outcome("send", EffectOutcome::Applied),
            )
            .unwrap();
        let current = journal.outbox(&fixture.scope).unwrap().remove(0);
        assert_ne!(old, current);
        assert_eq!(
            journal.acknowledge(&fixture.scope, &old).unwrap(),
            AckDisposition::Superseded
        );
        journal
            .queue_outcome(
                &fixture.scope,
                fixture.receipt("send"),
                fixture.outcome("send", EffectOutcome::Unknown),
            )
            .unwrap();
        assert_eq!(
            journal.outbox(&fixture.scope).unwrap(),
            vec![current.clone()]
        );
        journal.acknowledge(&fixture.scope, &current).unwrap();
        assert!(matches!(
            journal
                .explicit_reconcile(&fixture.scope, "send", invocation.payload_sha256())
                .unwrap(),
            Reconciliation::Finalized
        ));
        assert_eq!(
            journal.acknowledge(&fixture.scope, &current).unwrap(),
            AckDisposition::AlreadyRetired
        );
    }

    #[test]
    fn muse_journal_result_ack_only_retires_read_operation_and_preserves_exact_page() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        drop(
            journal
                .prepare_native(
                    &fixture.scope,
                    fixture.receipt("send"),
                    fixture.invocation(1, "turn/start"),
                )
                .unwrap(),
        );
        let page = json!({"source":{"native_session_id":fixture.scope.native_root_id,"item_id":"item","item_revision":1},"content_base64":"ZXhhY3Q=","offset_bytes":0,"eof":true});
        let mut foreign = page.clone();
        foreign["source"]["native_session_id"] = json!("foreign-root");
        assert_eq!(
            journal.queue_result(&fixture.scope, fixture.receipt("read"), foreign),
            Err(Fault::ScopeMismatch)
        );
        journal
            .queue_result(&fixture.scope, fixture.receipt("read"), page.clone())
            .unwrap();
        let ticket = journal.outbox(&fixture.scope).unwrap().remove(0);
        assert_eq!(
            serde_json::from_str::<Value>(ticket.params_json()).unwrap(),
            json!({"operation_id":"read","page":page})
        );
        assert_eq!(ticket.method(), "module.result");
        drop(journal);
        let mut journal = fixture.reopen();
        assert_eq!(
            journal.outbox(&fixture.scope).unwrap(),
            vec![ticket.clone()]
        );
        journal.acknowledge(&fixture.scope, &ticket).unwrap();
        assert!(journal.outbox(&fixture.scope).unwrap().is_empty());
        assert_eq!(
            journal.unresolved_native(&fixture.scope).unwrap(),
            vec![("send", BOOT_A, false)]
        );
    }

    #[test]
    fn muse_journal_unknown_without_native_intent_is_not_permission_for_fresh_input() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        journal
            .queue_outcome(
                &fixture.scope,
                fixture.receipt("send"),
                fixture.outcome("send", EffectOutcome::Unknown),
            )
            .unwrap();
        assert!(matches!(
            journal.prepare_native(
                &fixture.scope,
                fixture.receipt("send"),
                fixture.invocation(1, "turn/start")
            ),
            Err(Fault::ExplicitReconcileRequired)
        ));
        assert!(matches!(
            journal.explicit_reconcile(&fixture.scope, "send", "invented-digest"),
            Err(Fault::IntentConflict)
        ));
    }

    #[test]
    fn muse_journal_changed_intent_duplicate_id_and_wrong_payload_digest_are_rejected() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        drop(
            journal
                .prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone())
                .unwrap(),
        );
        assert!(matches!(
            journal.prepare_native(
                &fixture.scope,
                fixture.receipt("send"),
                fixture.invocation(2, "turn/start")
            ),
            Err(Fault::IntentConflict)
        ));
        assert!(matches!(
            journal.prepare_native(&fixture.scope, fixture.receipt("other"), invocation),
            Err(Fault::IntentConflict)
        ));
        drop(journal);
        let path = fixture.directory.join(CHECKPOINT);
        let mut checkpoint: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        checkpoint["state"]["operations"]["send"]["native"]["invocation"]["params"]["utf8"] =
            json!("{\"changed\":true}");
        replace_private_durable(&path, &serde_json::to_vec(&checkpoint).unwrap()).unwrap();
        assert!(matches!(
            Journal::open_at(&fixture.directory, fixture.scope.clone(), BOOT_B.into()),
            Err(Fault::InvalidState)
        ));
    }

    #[test]
    fn muse_journal_resume_requires_current_boot_durable_observation_and_keeps_original_attempt() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "session/resume");
        assert!(matches!(
            journal.prepare_native(
                &fixture.scope,
                fixture.receipt("recover"),
                invocation.clone()
            ),
            Err(Fault::DurabilityUnavailable)
        ));
        journal
            .record_durability(&fixture.scope, Durability::Durable)
            .unwrap();
        assert!(matches!(
            journal.prepare_native(
                &fixture.scope,
                fixture.receipt("recover"),
                invocation.clone()
            ),
            Err(Fault::ExactRootReadRequired)
        ));
        assert_eq!(
            journal.record_exact_root_read(&fixture.scope, "foreign-root"),
            Err(Fault::ScopeMismatch)
        );
        journal
            .record_exact_root_read(&fixture.scope, &fixture.scope.native_root_id)
            .unwrap();
        drop(
            journal
                .prepare_native(
                    &fixture.scope,
                    fixture.receipt("recover"),
                    invocation.clone(),
                )
                .unwrap(),
        );
        drop(journal);
        let mut journal = fixture.reopen();
        assert!(matches!(
            journal.explicit_reconcile(&fixture.scope, "recover", invocation.payload_sha256()),
            Err(Fault::DurabilityUnavailable)
        ));
        journal
            .record_durability(&fixture.scope, Durability::Ephemeral)
            .unwrap();
        assert!(matches!(
            journal.explicit_reconcile(&fixture.scope, "recover", invocation.payload_sha256()),
            Err(Fault::DurabilityUnavailable)
        ));
        journal
            .record_durability(&fixture.scope, Durability::Durable)
            .unwrap();
        assert!(matches!(
            journal.explicit_reconcile(&fixture.scope, "recover", invocation.payload_sha256()),
            Err(Fault::ExactRootReadRequired)
        ));
        journal
            .record_exact_root_read(&fixture.scope, &fixture.scope.native_root_id)
            .unwrap();
        assert!(matches!(
            journal.explicit_reconcile(&fixture.scope, "recover", invocation.payload_sha256()),
            Err(Fault::ContextRequired)
        ));
        assert!(matches!(
            journal.explicit_reconcile(&fixture.scope, "recover", invocation.payload_sha256()),
            Err(Fault::ContextRequired)
        ));
        assert_eq!(
            journal.unresolved_native(&fixture.scope).unwrap()[0].1,
            BOOT_A
        );
    }

    #[test]
    fn muse_journal_fence_rejection_is_retained_without_inventing_no_effect() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        drop(
            journal
                .prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone())
                .unwrap(),
        );
        journal
            .block_reconciliation(&fixture.scope, "send", "NATIVE_WORK_SNAPSHOT_CHANGED")
            .unwrap();
        drop(journal);
        let mut journal = fixture.reopen();
        match journal
            .explicit_reconcile(&fixture.scope, "send", invocation.payload_sha256())
            .unwrap()
        {
            Reconciliation::Blocked(code) => assert_eq!(code, "NATIVE_WORK_SNAPSHOT_CHANGED"),
            _ => panic!("retained work fence must not become a send"),
        }
        assert_eq!(
            journal.unresolved_native(&fixture.scope).unwrap(),
            vec![("send", BOOT_A, false)]
        );
        assert!(journal.outbox(&fixture.scope).unwrap().is_empty());
    }

    #[test]
    fn muse_journal_ephemeral_unknown_input_never_assumes_cross_connection_idempotency() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        drop(
            journal
                .prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone())
                .unwrap(),
        );
        journal
            .record_durability(&fixture.scope, Durability::Ephemeral)
            .unwrap();
        assert!(matches!(
            journal.explicit_reconcile(&fixture.scope, "send", invocation.payload_sha256()),
            Err(Fault::DurabilityUnavailable)
        ));
        assert_eq!(
            journal.unresolved_native(&fixture.scope).unwrap(),
            vec![("send", BOOT_A, false)]
        );
        assert!(journal.outbox(&fixture.scope).unwrap().is_empty());
    }

    #[test]
    fn muse_journal_provider_configuration_and_full_transcript_are_not_checkpoint_inputs() {
        let fixture = Fixture::new();
        let invocation = fixture.invocation(1, "turn/start");
        let mut params = invocation.params().unwrap();
        params.insert("providerApiKey".into(), json!("fixture-only"));
        assert!(
            NativeInvocation::new(
                &fixture.scope,
                invocation.method(),
                invocation.command_id(),
                params
            )
            .is_err()
        );
        let mut params = fixture.invocation(2, "session/start").params().unwrap();
        params.insert("config".into(), json!({"mcpServers":{}}));
        assert!(
            NativeInvocation::new(
                &fixture.scope,
                "session/start",
                "019a0000-0000-7000-8000-000000000002",
                params
            )
            .is_err()
        );
        let mut ack = fixture.ack(&invocation);
        ack["history"] = json!({"mode":"inline","items":[{"text":"transcript"}],"snapshot":null});
        assert!(validate_ack(&fixture.scope, &invocation, &ack).is_err());
    }

    #[test]
    fn muse_journal_root_creation_lost_ack_retains_exact_session_and_command_before_send() {
        let fixture = Fixture::new();
        let host_operation_v4 = "9f6ce9ca-5d89-4bc0-84ce-91a2d0b1acbe";
        let invocation = fixture.invocation(1, "session/start");
        assert_ne!(invocation.command_id(), host_operation_v4);
        let params_before = invocation.params().unwrap();
        assert_eq!(params_before["sessionId"], fixture.scope.native_root_id);
        assert_eq!(params_before["commandId"], invocation.command_id());
        let mut journal = fixture.create();
        fixture.initialize(&mut journal, false);
        // A permit is returned only after the complete intent checkpoint. No
        // native process is invoked; drop models an ambiguous write/lost ACK.
        let prepared = journal
            .prepare_command(
                &fixture.scope,
                fixture.receipt(host_operation_v4),
                fixture.command(host_operation_v4, "agent.open", json!({})),
                invocation.clone(),
                RetainedSelectors::default(),
            )
            .unwrap();
        let checkpoint: Value = serde_json::from_slice(
            &read_private_durable(&fixture.directory.join(CHECKPOINT), MAX_FILE).unwrap(),
        )
        .unwrap();
        let saved = &checkpoint["state"]["operations"][host_operation_v4]["native"]["invocation"];
        assert_eq!(saved["method"], "session/start");
        assert_eq!(saved["command_id"], invocation.command_id());
        assert_eq!(
            serde_json::from_str::<Value>(saved["params"]["utf8"].as_str().unwrap()).unwrap(),
            Value::Object(params_before.clone())
        );
        assert_eq!(
            prepared.invocation().payload_sha256(),
            invocation.payload_sha256()
        );
        drop(prepared);
        drop(journal);
        let mut journal = fixture.reopen();
        assert_eq!(
            journal.unresolved_native(&fixture.scope).unwrap(),
            vec![(host_operation_v4, BOOT_A, false)]
        );
        assert!(journal.outbox(&fixture.scope).unwrap().is_empty());
        assert!(matches!(
            journal.prepare_native(
                &fixture.scope,
                fixture.receipt(host_operation_v4),
                invocation.clone()
            ),
            Err(Fault::ExplicitReconcileRequired)
        ));
        fixture.initialize(&mut journal, false);
        match journal
            .explicit_reconcile(
                &fixture.scope,
                host_operation_v4,
                invocation.payload_sha256(),
            )
            .unwrap()
        {
            Reconciliation::Write(prepared) => {
                assert_eq!(prepared.invocation(), &invocation);
                assert_eq!(prepared.invocation().params().unwrap(), params_before);
            }
            _ => panic!(
                "root creation must retain its original sessionId, commandId, method and payload"
            ),
        }
        // A matching native deduplicated ACK is a separate explicit observation;
        // journal retention still does not turn it into Task acceptance.
        let ack = fixture.ack(&invocation);
        journal
            .record_native_ack(
                &fixture.scope,
                host_operation_v4,
                invocation.command_id(),
                ack.clone(),
            )
            .unwrap();
        assert!(
            matches!(journal.explicit_reconcile(&fixture.scope, host_operation_v4, invocation.payload_sha256()).unwrap(), Reconciliation::AckReadback(saved) if saved == ack)
        );
        assert!(journal.outbox(&fixture.scope).unwrap().is_empty());
    }

    #[test]
    fn muse_journal_empty_lifecycle_cursor_survives_reopen_without_native_replay() {
        for method in ["session/start", "session/resume"] {
            let fixture = Fixture::new();
            let invocation = fixture.invocation(1, method);
            let mut journal = fixture.create();
            fixture.initialize(&mut journal, false);
            journal
                .record_exact_root_read(&fixture.scope, &fixture.scope.native_root_id)
                .unwrap();
            drop(
                journal
                    .prepare_native(
                        &fixture.scope,
                        fixture.receipt("lifecycle"),
                        invocation.clone(),
                    )
                    .unwrap(),
            );
            let mut ack = fixture.ack(&invocation);
            ack["viewCursor"] = json!("");
            let checkpoint = fs::read(&journal.path).unwrap();
            for bad_cursor in [Value::Null, json!(7)] {
                let mut malformed = ack.clone();
                malformed["viewCursor"] = bad_cursor;
                assert!(
                    journal
                        .record_native_ack(
                            &fixture.scope,
                            "lifecycle",
                            invocation.command_id(),
                            malformed
                        )
                        .is_err()
                );
                assert_eq!(fs::read(&journal.path).unwrap(), checkpoint);
            }
            let mut missing = ack.clone();
            missing.as_object_mut().unwrap().remove("viewCursor");
            assert!(
                journal
                    .record_native_ack(
                        &fixture.scope,
                        "lifecycle",
                        invocation.command_id(),
                        missing
                    )
                    .is_err()
            );
            let mut foreign = ack.clone();
            foreign["session"]["sessionId"] = json!("foreign-root");
            assert_eq!(
                journal.record_native_ack(
                    &fixture.scope,
                    "lifecycle",
                    invocation.command_id(),
                    foreign
                ),
                Err(Fault::ScopeMismatch)
            );
            assert_eq!(fs::read(&journal.path).unwrap(), checkpoint);
            journal
                .record_native_ack(
                    &fixture.scope,
                    "lifecycle",
                    invocation.command_id(),
                    ack.clone(),
                )
                .unwrap();
            drop(journal);
            let mut reopened = fixture.reopen();
            assert!(matches!(
                reopened.explicit_reconcile(&fixture.scope, "lifecycle", invocation.payload_sha256()).unwrap(),
                Reconciliation::AckReadback(saved) if saved == ack
            ));
            assert_eq!(
                reopened
                    .retained_invocation(&fixture.scope, "lifecycle")
                    .unwrap(),
                &invocation
            );
        }
    }

    #[test]
    fn muse_journal_root_conflict_never_adopts_foreign_root_or_changes_creation_intent() {
        let fixture = Fixture::new();
        let invocation = fixture.invocation(1, "session/start");
        let mut journal = fixture.create();
        drop(
            journal
                .prepare_native(&fixture.scope, fixture.receipt("open"), invocation.clone())
                .unwrap(),
        );
        let mut foreign_ack = fixture.ack(&invocation);
        foreign_ack["session"]["sessionId"] = json!("foreign-root");
        assert_eq!(
            journal.record_native_ack(&fixture.scope, "open", invocation.command_id(), foreign_ack),
            Err(Fault::ScopeMismatch)
        );
        journal
            .block_reconciliation(&fixture.scope, "open", "NATIVE_SESSION_ID_CONFLICT")
            .unwrap();
        drop(journal);
        let mut journal = fixture.reopen();
        assert!(matches!(
            journal
                .explicit_reconcile(&fixture.scope, "open", invocation.payload_sha256())
                .unwrap(),
            Reconciliation::Blocked(_)
        ));
        assert!(matches!(
            journal.prepare_native(
                &fixture.scope,
                fixture.receipt("open"),
                fixture.invocation(2, "session/start")
            ),
            Err(Fault::IntentConflict)
        ));
        assert!(journal.outbox(&fixture.scope).unwrap().is_empty());
        assert_eq!(
            journal.unresolved_native(&fixture.scope).unwrap(),
            vec![("open", BOOT_A, false)]
        );
    }

    #[test]
    fn muse_journal_native_command_id_is_uuid_v7_not_host_uuid_or_rpc_id() {
        let fixture = Fixture::new();
        let original = fixture.invocation(1, "session/start");
        let host_v4 = "9f6ce9ca-5d89-4bc0-84ce-91a2d0b1acbe";
        let mut params = original.params().unwrap();
        params.insert("commandId".into(), json!(host_v4));
        assert!(matches!(
            NativeInvocation::new(&fixture.scope, "session/start", host_v4, params.clone()),
            Err(Fault::InvalidState)
        ));
        params.insert("commandId".into(), json!(17));
        assert!(NativeInvocation::new(&fixture.scope, "session/start", "17", params).is_err());
        assert!(
            NativeInvocation::new(
                &fixture.scope,
                "session/fork",
                original.command_id(),
                original.params().unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn muse_journal_goal_objective_is_canonical_or_rejected_without_rewrite() {
        let fixture = Fixture::new();
        let id = "019a0000-0000-7000-8000-000000000001";
        for method in ["goal/set", "goal/edit"] {
            for objective in ["", " ", " leading", "trailing ", "\u{2003}goal\u{2003}"] {
                let params = json!({"commandId":id,"sessionId":fixture.scope.native_root_id,"objective":objective});
                assert!(matches!(
                    NativeInvocation::new(
                        &fixture.scope,
                        method,
                        id,
                        params.as_object().unwrap().clone()
                    ),
                    Err(Fault::InvalidState)
                ));
            }
            let params = json!({"commandId":id,"sessionId":fixture.scope.native_root_id,"objective":"exact goal"});
            let invocation = NativeInvocation::new(
                &fixture.scope,
                method,
                id,
                params.as_object().unwrap().clone(),
            )
            .unwrap();
            assert_eq!(invocation.params().unwrap()["objective"], "exact goal");
        }
    }

    #[test]
    fn muse_journal_pin_durability_absence_is_durable_but_unknown_is_not() {
        assert_eq!(
            Durability::from_initialize_declaration(None),
            Durability::Durable
        );
        assert_eq!(
            Durability::from_initialize_declaration(Some(&json!("durable"))),
            Durability::Durable
        );
        assert_eq!(
            Durability::from_initialize_declaration(Some(&json!("ephemeral"))),
            Durability::Ephemeral
        );
        for value in [
            Value::Null,
            json!("future-profile"),
            json!("Ephemeral"),
            json!(true),
        ] {
            assert_eq!(
                Durability::from_initialize_declaration(Some(&value)),
                Durability::Unrecognized
            );
        }
    }

    #[test]
    fn muse_journal_resume_read_is_not_reused_across_connection_or_native_write() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let resume = fixture.invocation(1, "session/resume");
        journal
            .record_durability(&fixture.scope, Durability::Durable)
            .unwrap();
        journal
            .record_exact_root_read(&fixture.scope, &fixture.scope.native_root_id)
            .unwrap();
        // A new initialize observation in the same worker invalidates old read
        // freshness; neither a checkpoint nor the worker boot proves the link.
        journal
            .record_durability(&fixture.scope, Durability::Durable)
            .unwrap();
        assert!(matches!(
            journal.prepare_native(&fixture.scope, fixture.receipt("recover"), resume.clone()),
            Err(Fault::ExactRootReadRequired)
        ));
        journal
            .record_exact_root_read(&fixture.scope, &fixture.scope.native_root_id)
            .unwrap();
        assert_eq!(
            journal.record_exact_root_read(&fixture.scope, "foreign-root"),
            Err(Fault::ScopeMismatch)
        );
        assert!(matches!(
            journal.prepare_native(&fixture.scope, fixture.receipt("recover"), resume.clone()),
            Err(Fault::ExactRootReadRequired)
        ));
        journal
            .record_exact_root_read(&fixture.scope, &fixture.scope.native_root_id)
            .unwrap();
        drop(
            journal
                .prepare_native(
                    &fixture.scope,
                    fixture.receipt("send"),
                    fixture.invocation(2, "turn/start"),
                )
                .unwrap(),
        );
        assert!(matches!(
            journal.prepare_native(&fixture.scope, fixture.receipt("recover"), resume.clone()),
            Err(Fault::ExactRootReadRequired)
        ));
        journal
            .record_exact_root_read(&fixture.scope, &fixture.scope.native_root_id)
            .unwrap();
        drop(
            journal
                .prepare_native(&fixture.scope, fixture.receipt("recover"), resume)
                .unwrap(),
        );
    }
}
