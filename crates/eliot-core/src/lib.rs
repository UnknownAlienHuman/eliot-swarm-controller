#![forbid(unsafe_code)]

pub mod api;
pub mod limits;
pub mod requests;

pub use api::{
    DEFAULT_GIT_FETCH_TIMEOUT_MS, FrozenGitFetch, FrozenScriptContext, FrozenScriptInterpreter,
    FrozenScriptRun, GitFetchDefinitionArgs, GitFetchOutcome, RunDefinitionArgs,
    ScriptActionRecord, ScriptCommandContext, ScriptCustody, ScriptDirectExit, ScriptInputContext,
    ScriptInterpreter, ScriptProcessResult, ScriptRuleContext, ScriptRunApplied,
    ScriptTimerContext, ScriptTimerInvocation, bound_script_outcome_stderr,
};

mod agent_start;
mod canonical;
mod command;
mod context;
mod diagnostics;
mod ids;
mod mcp_policy;
mod native_child;
mod native_store;
mod remote_call;
mod routing;
mod secrets;
mod source_handoff;

pub use agent_start::AgentStartParams;
pub use canonical::canonical_json_sha256;
pub use command::{
    classify_dispatched_error, command_transition_allowed,
    is_deterministic_pre_admission_rejection, workspace_preparation_transition_allowed,
};
pub use context::{ContextItem, ContextOutcome, InjectArgs, InjectPoint};
pub use diagnostics::{
    MAX_STDERR_BYTES, MAX_STDERR_DIAGNOSTIC_BYTES, StderrDiagnostic, StderrSnapshot, StderrSource,
    bound_stderr_diagnostics,
};
pub use ids::parse_agent_id;
pub use mcp_policy::{
    MAX_MCP_ALLOW_ENTRIES, MAX_MCP_SERVER_NAME_LEN, MCP_POLICY_INHERIT, MCP_POLICY_MINIMAL,
    McpPolicy, parse_snapshot_allow_list, validate_allow_list,
};
pub use native_child::{
    CODEX_NATIVE_CHILD_FAMILIES, ChildRegistration, ChildRegistrationAck,
    MUSE_NATIVE_CHILD_FAMILIES, NATIVE_CHILD_FAMILIES, NativeChildActivity,
    NativeChildActivityState, native_child_families_for_harness,
};
pub use native_store::NativeStoreBinding;
pub use remote_call::{RemoteCallContext, RemoteIdentity};
pub use routing::{
    CapacityScope, ProviderHold, ProviderHoldState, ProviderRestriction, ProviderRestrictionKind,
    RouteBinding, frozen_provider_scope,
};
pub use secrets::SecretPolicy;
pub use source_handoff::{SourceHandoff, SourceHandoffAck, SourceHandoffGap};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::{self, Display, Formatter};

fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

fn is_false(value: &bool) -> bool {
    !value
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenSetupFile {
    pub canonical_path: String,
    pub size: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum WorkspaceSetupArg {
    Literal(String),
    File(FrozenSetupFile),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSetup {
    pub program: FrozenSetupFile,
    pub args: Vec<WorkspaceSetupArg>,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspacePreparationInput {
    pub project: String,
    pub repo_path: String,
    pub repo_common_dir: String,
    pub requested_branch: String,
    pub base_ref: Option<String>,
    pub base_oid: String,
    pub worktree_path: String,
    pub frozen_setup: Vec<WorkspaceSetup>,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspacePhase {
    Creating,
    SetupReady,
    SetupRunning,
    Ready,
    Unknown,
    Failed,
    Removing,
    Removed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkspacePreparation {
    pub agent: String,
    pub target_generation: i64,
    pub start_command_id: String,
    pub project: String,
    pub repo_path: String,
    pub repo_common_dir: String,
    pub requested_branch: String,
    pub base_ref: Option<String>,
    pub base_oid: String,
    pub worktree_path: String,
    pub frozen_setup: Vec<WorkspaceSetup>,
    pub phase: WorkspacePhase,
    pub setup_index: usize,
    pub outcome: Option<Value>,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum WorkspaceCreationOutcome {
    Verified {
        repo_path: String,
        repo_common_dir: String,
        worktree_path: String,
        branch: String,
        head: String,
    },
    Failed {
        code: String,
        message: String,
    },
    Unknown {
        code: String,
        message: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum WorkspaceSetupOutcome {
    Succeeded,
    Failed { exit_code: i32 },
    NotStarted { code: String, message: String },
    Unknown { code: String, message: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum WorkspaceCancelOutcome {
    Stopped,
    Unknown { code: String, message: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum WorkspaceRemovalOutcome {
    Removed {
        repo_common_dir: String,
        worktree_path: String,
        branch: String,
        branch_retained: bool,
    },
    /// Proven no destructive effect: the registered checkout remains intact.
    Failed {
        code: String,
        message: String,
    },
    Unknown {
        code: String,
        message: String,
    },
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitPathStatus {
    pub path: String,
    pub status: String,
    pub original_path: Option<String>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitCommit {
    pub oid: String,
    pub paths: Vec<String>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitBaseline {
    pub head: Option<String>,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitSnapshot {
    pub workspace: String,
    pub head: Option<String>,
    pub branch: Option<String>,
    pub dirty: Vec<GitPathStatus>,
    pub commits: Vec<GitCommit>,
    pub clean_paths: Vec<String>,
    pub observed_at: i64,
    pub complete: bool,
    pub history_complete: bool,
    pub history_gap: bool,
    #[serde(default)]
    pub base_oid: Option<String>,
    #[serde(default)]
    pub committed_paths: Vec<String>,
    #[serde(default)]
    pub base_complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileChangeHint {
    pub path: String,
    pub event_seq: u64,
    pub ts: i64,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEdit {
    pub agent: String,
    pub generation: i64,
    pub workspace: String,
    pub path: String,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitHintPage {
    pub events: Vec<Event>,
    pub through: u64,
}

#[derive(
    schemars::JsonSchema, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct ProcessIdentity {
    pub host_boot_id: String,
    pub pid: u32,
    pub start_time: u64,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessState {
    Running,
    Exited,
    Unknown,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessRecord {
    pub identity: ProcessIdentity,
    pub owner: String,
    pub role: String,
    pub state: ProcessState,
    pub exit_code: Option<i32>,
    pub updated_at: i64,
}

/// Ephemeral OS counters; absent values remain unknown and are not model progress.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessSample {
    pub record: ProcessRecord,
    pub cpu_time_ns: Option<u64>,
    pub rss_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessSnapshot {
    pub samples: Vec<ProcessSample>,
    pub complete: bool,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    Unknown,
    Created,
    Starting,
    Idle,
    Busy,
    Waiting,
    Stopped,
    Failed,
    Lost,
}

#[derive(
    schemars::JsonSchema, Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentOrigin {
    #[default]
    Eliot,
    Native,
    External,
}

/// Frozen command scope of an Agent principal, from its profile `control`.
#[derive(
    schemars::JsonSchema, Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentControl {
    #[default]
    #[serde(rename = "self")]
    Slf,
    Tree,
    Project,
}

impl AgentControl {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Slf => "self",
            Self::Tree => "tree",
            Self::Project => "project",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "self" => Ok(Self::Slf),
            "tree" => Ok(Self::Tree),
            "project" => Ok(Self::Project),
            _ => Err(Error::new(
                "invalid",
                "Agent control must be self, tree, or project",
            )),
        }
    }
}

impl AgentState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Stopped | Self::Failed | Self::Lost)
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingRequest {
    pub id: String,
    pub turn_id: String,
    pub kind: String,
    #[serde(default = "empty_object")]
    pub details: Value,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Agent {
    pub id: String,
    pub generation: i64,
    #[serde(default)]
    pub origin: AgentOrigin,
    #[serde(default)]
    pub observe_only: bool,
    #[serde(default)]
    pub own_identity: bool,
    #[serde(default)]
    pub parent: Option<String>,
    #[serde(default)]
    pub root: String,
    #[serde(default)]
    pub depth: i64,
    #[serde(default)]
    pub control: AgentControl,
    pub name: Option<String>,
    pub project: String,
    pub harness: String,
    pub profile: String,
    #[serde(default = "empty_object")]
    pub profile_snapshot: Value,
    #[serde(default)]
    pub native_scope: Option<String>,
    #[serde(default)]
    pub native_ref: Option<String>,
    pub workspace: Option<String>,
    #[serde(default)]
    pub workspace_phase: Option<WorkspacePhase>,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub base_ref: Option<String>,
    #[serde(default)]
    pub base_oid: Option<String>,
    #[serde(default)]
    pub effective: Option<Value>,
    pub state: AgentState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity: Option<NativeChildActivity>,
    pub active_turn: Option<String>,
    #[serde(default)]
    pub pending: Vec<PendingRequest>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Agent {
    pub fn new(
        id: impl Into<String>,
        project: impl Into<String>,
        harness: impl Into<String>,
        profile: impl Into<String>,
        now: i64,
    ) -> Self {
        let id = id.into();
        Self {
            root: id.clone(),
            id,
            generation: 0,
            origin: AgentOrigin::Eliot,
            observe_only: false,
            own_identity: false,
            parent: None,
            depth: 0,
            control: AgentControl::Slf,
            name: None,
            project: project.into(),
            harness: harness.into(),
            profile: profile.into(),
            profile_snapshot: empty_object(),
            native_scope: None,
            native_ref: None,
            workspace: None,
            workspace_phase: None,
            branch: None,
            base_ref: None,
            base_oid: None,
            effective: None,
            state: AgentState::Created,
            activity: None,
            active_turn: None,
            pending: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventSubject {
    pub project: String,
    pub harness: String,
    pub profile: String,
    pub state: AgentState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
}

impl From<&Agent> for EventSubject {
    fn from(agent: &Agent) -> Self {
        Self {
            project: agent.project.clone(),
            harness: agent.harness.clone(),
            profile: agent.profile.clone(),
            state: agent.state,
            workspace: agent.workspace.clone(),
        }
    }
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandState {
    Accepted,
    Queued,
    Dispatched,
    Applied,
    Rejected,
    Failed,
    Unknown,
    Cancelled,
}

impl CommandState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Applied | Self::Rejected | Self::Failed | Self::Cancelled
        )
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Command {
    pub id: String,
    pub principal: String,
    pub method: String,
    pub verb: String,
    pub target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    pub expected_generation: Option<i64>,
    pub generation: Option<i64>,
    pub args: Value,
    pub state: CommandState,
    pub turn_id: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    pub outcome: Option<Value>,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(default, skip_serializing_if = "is_false")]
    pub probe: bool,
    /// Configuration revision the command was admitted or dispatched under:
    /// 0 runs under the frozen profile snapshot, otherwise the exact
    /// `agent_config` revision for its retained generation. Trailing field;
    /// absent on old wire payloads.
    #[serde(default)]
    pub config_revision: i64,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventCause {
    pub command_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule: Option<String>,
    pub depth: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_call: Option<RemoteCallContext>,
}

impl EventCause {
    /// Checks the wire grammar only. Store owns proof that this command is the
    /// verified cause and applies the depth suppression policy.
    pub fn validate(&self) -> Result<()> {
        if let Some(remote) = &self.remote_call {
            remote.validate()?;
        }
        if self.command_id.trim().is_empty() {
            return Err(Error::new(
                "invalid",
                "event cause command_id must be nonempty",
            ));
        }
        if self
            .rule
            .as_deref()
            .is_some_and(|rule| rule.trim().is_empty())
        {
            return Err(Error::new("invalid", "event cause rule must be nonempty"));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub seq: i64,
    pub ts: i64,
    pub kind: String,
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<EventSubject>,
    pub generation: Option<i64>,
    pub source: String,
    pub native_id: Option<String>,
    pub data: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<EventCause>,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnState {
    Running,
    Completed,
    Unknown,
}

/// Canonical task outcome of one terminal turn, kept separate from its
/// lifecycle (`TurnState`). Decided at the plugin boundary from the adapter's
/// native terminal vocabulary; rows written before outcomes existed classify
/// explicitly from their legacy status and never default to success.
#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnOutcome {
    Success,
    Failure,
    Interrupted,
    Unknown,
}

impl TurnOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Interrupted => "interrupted",
            Self::Unknown => "unknown",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "success" => Some(Self::Success),
            "failure" => Some(Self::Failure),
            "interrupted" => Some(Self::Interrupted),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }

    /// Explicit classification for rows/events written before canonical
    /// outcomes existed: `completed` -> success, `failed`/`error` -> failure,
    /// `interrupted`/`cancelled` -> interrupted, anything else -> unknown.
    /// Never defaults an unrecognized value to success.
    pub fn classify_legacy_status(status: Option<&str>) -> Self {
        match status {
            Some("completed") => Self::Success,
            Some("failed") | Some("error") => Self::Failure,
            Some("interrupted") | Some("cancelled") => Self::Interrupted,
            _ => Self::Unknown,
        }
    }

    /// Harness-aware classification for rows written before canonical
    /// outcomes existed, using the stored harness and that adapter's
    /// recognized native reasons. Claude rows carry the raw terminal reason
    /// (`success` -> success; `cancelled`/`interrupted` -> interrupted;
    /// `error`/`failed`/`failure` and the `api_error*` family -> failure).
    /// Every other harness carries the normalized lifecycle status and uses
    /// [`Self::classify_legacy_status`]. Anything else is unknown, never
    /// success.
    pub fn classify_legacy_status_for_harness(harness: &str, status: Option<&str>) -> Self {
        if harness != "claude" {
            return Self::classify_legacy_status(status);
        }
        match status.map(str::trim) {
            Some("success") => Self::Success,
            Some("cancelled") | Some("interrupted") => Self::Interrupted,
            Some("error") | Some("failed") | Some("failure") => Self::Failure,
            Some(reason) => {
                let lower = reason.to_lowercase();
                if lower.starts_with("api_error")
                    || lower.starts_with("error")
                    || lower.starts_with("failed")
                    || lower.starts_with("failure")
                {
                    Self::Failure
                } else {
                    Self::Unknown
                }
            }
            None => Self::Unknown,
        }
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Turn {
    pub agent: String,
    pub generation: i64,
    pub turn_id: String,
    pub state: TurnState,
    pub status: Option<String>,
    pub final_text: Option<String>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    #[serde(default)]
    pub usage: Option<TurnUsage>,
    /// Canonical outcome decided at the plugin boundary. `None` only for
    /// running turns; terminal turns written before outcomes existed read
    /// back through [`TurnOutcome::classify_legacy_status_for_harness`].
    /// Trailing field so existing positional wire order stays stable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<TurnOutcome>,
    /// Raw native terminal reason preserved next to the canonical outcome
    /// (vendor stop reason / wire terminal verbatim). Trailing field so
    /// existing positional wire order stays stable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_status: Option<String>,
    /// Configuration revision the turn ran under: 0 when no `configure`
    /// command ever applied to the Agent generation, otherwise the exact
    /// `agent_config` revision whose effective model/effort the native
    /// turn request carried. Trailing field; absent on old wire payloads.
    #[serde(default)]
    pub config_revision: i64,
}

/// Native token usage attributed to one terminal turn. Every member is
/// nullable: absent native data stays null, never zero. `scope` names the
/// native attribution (`message` for the final assistant message of the
/// bound turn, `main_loop_turn` for one turn's main-loop-only usage,
/// `turn` for one turn's aggregate or per-turn-delta usage); unknown fields
/// are ignored so harnesses decode forward. Context occupancy/capacity and
/// cumulative session cost are session-scoped facts, never turn expenditure:
/// they are not represented here, and `total_tokens` is set only when the
/// native signal supplies a turn-scoped total, never derived.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TurnUsage {
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub input_tokens: Option<i64>,
    #[serde(default)]
    pub output_tokens: Option<i64>,
    #[serde(default)]
    pub reasoning_tokens: Option<i64>,
    #[serde(default)]
    pub thinking_tokens: Option<i64>,
    #[serde(default)]
    pub cache_read_tokens: Option<i64>,
    #[serde(default)]
    pub cache_write_tokens: Option<i64>,
    #[serde(default)]
    pub total_tokens: Option<i64>,
}

/// Session-scoped usage snapshot for one Agent generation. Every member is
/// nullable: absent native data stays null, never zero. Context
/// occupancy/capacity (`context_used_tokens`/`context_window_tokens`) and
/// cumulative session cost are never turn expenditure: they are not
/// represented in [`TurnUsage`], never fill its counters, and compaction can
/// reduce occupancy. `cumulative_tokens` carries only input/output/cache
/// counters where the native signal supplies them (never derived totals);
/// `cumulative_cost.amount` is the verbatim native amount with `unit` the
/// verbatim native unit (for example `usd_estimate` for an estimated USD
/// total), never converted and never assumed to be money. `source` names the
/// adapter and native signal (for example `codex:thread/tokenUsage/updated`);
/// `observed_at` is native epoch milliseconds. Unknown fields are ignored so
/// harnesses decode forward.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionUsage {
    #[serde(default)]
    pub context_used_tokens: Option<i64>,
    #[serde(default)]
    pub context_window_tokens: Option<i64>,
    #[serde(default)]
    pub cumulative_tokens: Option<SessionTokenTotals>,
    #[serde(default)]
    pub cumulative_cost: Option<SessionCost>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub observed_at: Option<i64>,
}

/// Cumulative input/output/cache counters inside [`SessionUsage`]. Only
/// counters the native signal actually supplies are present; missing members
/// stay null, never zero, and no total is ever derived.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionTokenTotals {
    #[serde(default)]
    pub input_tokens: Option<i64>,
    #[serde(default)]
    pub output_tokens: Option<i64>,
    #[serde(default)]
    pub cache_read_tokens: Option<i64>,
    #[serde(default)]
    pub cache_write_tokens: Option<i64>,
}

/// Cumulative session cost inside [`SessionUsage`]. `amount` is the verbatim
/// native amount and `unit` the verbatim native unit (for example
/// `usd_estimate` for an estimated USD total): never converted, and a unit
/// that is not a currency never reads as money.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionCost {
    #[serde(default)]
    pub amount: Option<f64>,
    #[serde(default)]
    pub unit: Option<String>,
}

impl SessionUsage {
    /// Validate one decoded session snapshot at its owning boundary. Absent
    /// members stay null; present counters must be nonnegative, the cost
    /// amount must be finite and nonnegative, and present text must be
    /// non-empty. Wrong types are rejected by deserialization before this.
    pub fn validate(&self) -> Result<()> {
        let counter = |name: &str, value: Option<i64>| {
            if value.is_some_and(|value| value < 0) {
                return Err(Error::new(
                    "invalid",
                    format!("session usage field {name} must be nonnegative"),
                ));
            }
            Ok(())
        };
        counter("context_used_tokens", self.context_used_tokens)?;
        counter("context_window_tokens", self.context_window_tokens)?;
        if let Some(tokens) = &self.cumulative_tokens {
            counter("cumulative_tokens.input_tokens", tokens.input_tokens)?;
            counter("cumulative_tokens.output_tokens", tokens.output_tokens)?;
            counter(
                "cumulative_tokens.cache_read_tokens",
                tokens.cache_read_tokens,
            )?;
            counter(
                "cumulative_tokens.cache_write_tokens",
                tokens.cache_write_tokens,
            )?;
        }
        if let Some(cost) = &self.cumulative_cost {
            if let Some(amount) = cost.amount
                && !(amount.is_finite() && amount >= 0.0)
            {
                return Err(Error::new(
                    "invalid",
                    "session usage field cumulative_cost.amount must be a finite nonnegative number",
                ));
            }
            if let Some(unit) = &cost.unit
                && unit.trim().is_empty()
            {
                return Err(Error::new(
                    "invalid",
                    "session usage field cumulative_cost.unit must be a non-empty string",
                ));
            }
        }
        if let Some(source) = &self.source
            && source.trim().is_empty()
        {
            return Err(Error::new(
                "invalid",
                "session usage field source must be a non-empty string",
            ));
        }
        counter("observed_at", self.observed_at)?;
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitCommand {
    pub id: String,
    pub target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    pub verb: String,
    #[serde(default = "empty_object")]
    pub args: Value,
    pub expected_generation: Option<i64>,
    pub expected_turn_id: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub probe: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NativeEvent {
    pub kind: String,
    pub native_id: String,
    pub data: Value,
    /// Original native epoch milliseconds, when supplied by a verified producer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ts: Option<i64>,
}

impl NativeEvent {
    pub fn semantic_value(&self) -> Value {
        let mut value =
            serde_json::json!({"kind":self.kind,"native_id":self.native_id,"data":self.data});
        if let Some(ts) = self.ts {
            value["ts"] = serde_json::json!(ts);
        }
        value
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceHealthUpdate {
    pub agent_id: String,
    pub generation: i64,
    pub source: String,
    pub health: String,
}

impl SourceHealthUpdate {
    pub fn new(
        agent_id: impl Into<String>,
        generation: i64,
        source: impl Into<String>,
        health: impl Into<String>,
    ) -> Self {
        Self {
            agent_id: agent_id.into(),
            generation,
            source: source.into(),
            health: health.into(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        parse_agent_id(&self.agent_id)?;
        if self.generation < 0
            || self.source.trim().is_empty()
            || !matches!(self.health.as_str(), "ok" | "slow" | "stale" | "error")
        {
            return Err(Error::new("invalid", "invalid source health update"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceBatch {
    pub batch_id: String,
    pub agent_id: String,
    pub generation: i64,
    pub source: String,
    pub events: Vec<NativeEvent>,
    pub cursor: Option<String>,
}

impl SourceBatch {
    /// Headers and cursor have their own binding checks; this is exactly the
    /// existing canonical event-array digest retained in source_state.
    pub fn semantic_digest(&self) -> std::result::Result<[u8; 32], serde_json::Error> {
        canonical_json_sha256(&Value::Array(
            self.events
                .iter()
                .map(NativeEvent::semantic_value)
                .collect(),
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BatchAck {
    pub first_seq: i64,
    pub last_seq: i64,
    pub cursor: Option<String>,
}

/// Stream-loss boundary reported by the Codex producer via `source.gap`.
/// Plain I/O-free data: Store/host writers own the transaction handler.
/// `from`/`to` use the API wire keys as named.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceGap {
    pub gap_id: String,
    pub agent_id: String,
    pub generation: i64,
    pub source: String,
    pub families: Vec<String>,
    pub reason: String,
    pub from: Option<String>,
    pub to: Option<String>,
}

/// Stored stream-loss boundary acknowledgement.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceGapAck {
    pub gap_id: String,
    pub seq: i64,
}

/// Store-facing facts derived by the reducer; callers persist them atomically with
/// the event and next Agent projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "effect", rename_all = "snake_case")]
pub enum Effect {
    CommandTurnAssociated {
        command_id: String,
        turn_id: String,
        /// Effort the native turn request actually carried, when the plugin
        /// reported one. `None` invents nothing: the Store keeps the
        /// overlay value it already retained.
        #[serde(default)]
        effort: Option<String>,
    },
    RequestPending {
        request_id: String,
        turn_id: String,
        pending: bool,
        #[serde(default)]
        kind: Option<String>,
        #[serde(default)]
        details: Option<Value>,
    },
    RequestClosed {
        request_id: String,
        turn_id: String,
    },
    MessageCompleted {
        command_id: String,
        turn_id: String,
        text: String,
    },
    TurnTerminal {
        command_id: Option<String>,
        turn_id: String,
        state: TurnState,
        status: Option<String>,
        #[serde(default)]
        outcome: Option<TurnOutcome>,
        #[serde(default)]
        native_status: Option<String>,
        final_text: Option<String>,
        #[serde(default)]
        usage: Option<TurnUsage>,
        completed_at: i64,
    },
    SessionUsageUpdated {
        usage: SessionUsage,
    },
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct Error {
    pub code: String,
    pub message: String,
    #[serde(default, deserialize_with = "diagnostics::deserialize_diagnostics")]
    #[schemars(skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<StderrDiagnostic>,
    #[serde(default)]
    #[schemars(skip_serializing_if = "Option::is_none")]
    pub details: Option<InvalidParam>,
}

/// Parameter diagnostics contain schema-owned names/expectations, never values.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvalidParam {
    pub field: String,
    pub expected: String,
}

impl Error {
    /// A bounded, schema-owned field path and expectation; never a rejected value.
    pub fn invalid_field(field: impl Into<String>, expected: impl Into<String>) -> Self {
        Self::new("invalid", "invalid parameters").with_field(field, expected)
    }

    pub fn with_field(mut self, field: impl Into<String>, expected: impl Into<String>) -> Self {
        let field: String = field.into().chars().take(256).collect();
        let expected: String = expected.into().chars().take(512).collect();
        self.message = format!("{field}: expected {expected}");
        self.details = Some(InvalidParam { field, expected });
        self
    }

    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            diagnostics: Vec::new(),
            details: None,
        }
    }

    /// Attach an already-sanitized tail, retaining the other stderr source.
    pub fn with_stderr(mut self, source: StderrSource, snapshot: StderrSnapshot) -> Self {
        let snapshot = snapshot.bounded();
        if let Some(existing) = self
            .diagnostics
            .iter_mut()
            .find(|tail| tail.source == source)
        {
            existing.snapshot = snapshot;
        } else {
            self.diagnostics.push(StderrDiagnostic { source, snapshot });
        }
        self.bound_diagnostics();
        self
    }

    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = code.into();
        self.bound_diagnostics();
        self
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = message.into();
        self.bound_diagnostics();
        self
    }

    fn bound_diagnostics(&mut self) {
        let budget = error_diagnostic_budget(&self.code, &self.message);
        bound_stderr_diagnostics(&mut self.diagnostics, budget);
    }
}

fn error_diagnostic_budget(code: &str, message: &str) -> usize {
    let base = serde_json::json!({"code": code, "message": message});
    let base_bytes = serde_json::to_vec(&base).map_or(usize::MAX, |bytes| bytes.len());
    (64 * 1024_usize)
        .saturating_sub(base_bytes.saturating_add(",\"diagnostics\":".len()))
        .min(MAX_STDERR_DIAGNOSTIC_BYTES)
}

impl Serialize for Error {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct WireError<'a> {
            code: &'a str,
            message: &'a str,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            diagnostics: Vec<StderrDiagnostic>,
            #[serde(skip_serializing_if = "Option::is_none")]
            details: Option<InvalidParam>,
        }
        let mut diagnostics = self.diagnostics.clone();
        bound_stderr_diagnostics(
            &mut diagnostics,
            error_diagnostic_budget(&self.code, &self.message),
        );
        WireError {
            code: &self.code,
            message: &self.message,
            diagnostics,
            details: self.details.as_ref().map(|detail| InvalidParam {
                field: detail.field.chars().take(256).collect(),
                expected: detail.expected.chars().take(512).collect(),
            }),
        }
        .serialize(serializer)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

/// Reduce one already-fenced native fact. Generation and source ownership are
/// checked by the Store before this function is called.
pub fn apply(agent: &Agent, event: &NativeEvent, now: i64) -> Result<(Agent, Vec<Effect>)> {
    if agent.observe_only {
        return Err(Error::new(
            "not_allowed",
            "observer metadata/history cannot reduce controlled native state",
        ));
    }
    if agent.state == AgentState::Unknown {
        return Err(Error::new(
            "invalid",
            "unknown Agent state requires observe_only",
        ));
    }
    if agent.is_terminal() {
        return Err(Error::new(
            "bad_state",
            format!("Agent {} is already terminal", agent.id),
        ));
    }

    let mut next = agent.clone();
    let mut effects = Vec::new();

    match event.kind.as_str() {
        "session.ready" => {
            if agent.state != AgentState::Starting || agent.active_turn.is_some() {
                return Err(bad_state(event, "session.ready requires a starting Agent"));
            }
            if let Some(scope) = optional_native_scope(event)? {
                next.native_scope = Some(scope);
            }
            if let Some(native_ref) = optional_native_ref(event)? {
                next.native_ref = Some(native_ref);
            }
            if let Some(effective) = optional_effective(event)? {
                next.effective = Some(effective);
            }
            next.state = AgentState::Idle;
        }
        "turn.started" => {
            if agent.state != AgentState::Idle || agent.active_turn.is_some() {
                return Err(bad_state(event, "turn.started requires an idle Agent"));
            }
            let command_id = required_text(event, "command_id")?;
            let turn_id = required_text(event, "turn_id")?;
            let effort = optional_observed_effort(event)?;
            next.state = AgentState::Busy;
            next.active_turn = Some(turn_id.clone());
            effects.push(Effect::CommandTurnAssociated {
                command_id,
                turn_id,
                effort,
            });
        }
        "message.completed" => {
            require_live_turn(agent, event)?;
            let command_id = required_text(event, "command_id")?;
            let turn_id = required_text(event, "turn_id")?;
            let text = required_text(event, "text")?;
            if let Some(model) = optional_observed_model(event)? {
                match &mut next.effective {
                    Some(Value::Object(effective)) => {
                        effective.insert("model".to_owned(), Value::String(model));
                    }
                    None => {
                        let mut effective = serde_json::Map::new();
                        effective.insert("model".to_owned(), Value::String(model));
                        next.effective = Some(Value::Object(effective));
                    }
                    Some(_) => {}
                }
            }
            effects.push(Effect::MessageCompleted {
                command_id,
                turn_id,
                text,
            });
        }
        "request.opened" => {
            require_live_turn(agent, event)?;
            let request_id = required_text(event, "request_id")?;
            let turn_id = required_text(event, "turn_id")?;
            let kind = required_text(event, "kind")?;
            let details = optional_details(event)?;
            if let Some(existing) = agent.pending.iter().find(|p| p.id == request_id) {
                // Same binding, new offer: a harness may re-open its exact pending
                // id with updated details proven from its authentic native stream
                // (Muse multi-stage advances its current requirement this way).
                // Conflicting bindings still fail; identical replays stay idempotent.
                if existing.turn_id != turn_id || existing.kind != kind {
                    return Err(Error::new(
                        "source_integrity",
                        format!(
                            "request.opened {request_id} conflicts with pending request for Agent {}",
                            agent.id
                        ),
                    ));
                }
                if existing.details != details
                    && let Some(slot) = next.pending.iter_mut().find(|p| p.id == request_id)
                {
                    slot.details = details.clone();
                }
                // State follows the pending set: open requests imply waiting.
                next.state = AgentState::Waiting;
                effects.push(Effect::RequestPending {
                    request_id,
                    turn_id,
                    pending: true,
                    kind: Some(kind),
                    details: Some(details),
                });
            } else {
                next.pending.push(PendingRequest {
                    id: request_id.clone(),
                    turn_id: turn_id.clone(),
                    kind: kind.clone(),
                    details: details.clone(),
                });
                next.state = AgentState::Waiting;
                effects.push(Effect::RequestPending {
                    request_id,
                    turn_id,
                    pending: true,
                    kind: Some(kind),
                    details: Some(details),
                });
            }
        }
        "request.resolved" | "request.closed" => {
            require_live_turn(agent, event)?;
            if event.kind == "request.closed" && event.data.get("command_id").is_some() {
                return Err(Error::new(
                    "invalid",
                    "request.closed must not acknowledge a command",
                ));
            }
            if agent.state != AgentState::Waiting {
                return Err(bad_state(
                    event,
                    "request completion requires a waiting Agent",
                ));
            }
            let request_id = required_text(event, "request_id")?;
            let turn_id = required_text(event, "turn_id")?;
            let Some(pos) = agent
                .pending
                .iter()
                .position(|p| p.id == request_id && p.turn_id == turn_id)
            else {
                return Err(Error::new(
                    "source_integrity",
                    format!(
                        "{} {request_id} is not pending for Agent {} turn {turn_id}",
                        event.kind, agent.id
                    ),
                ));
            };
            next.pending.remove(pos);
            next.state = if next.pending.is_empty() {
                AgentState::Busy
            } else {
                AgentState::Waiting
            };
            if event.kind == "request.closed" {
                effects.push(Effect::RequestClosed {
                    request_id,
                    turn_id,
                });
            } else {
                effects.push(Effect::RequestPending {
                    request_id,
                    turn_id,
                    pending: false,
                    kind: None,
                    details: None,
                });
            }
        }
        "turn.completed" => {
            require_live_turn(agent, event)?;
            let command_id = required_text(event, "command_id")?;
            let turn_id = required_text(event, "turn_id")?;
            let status = required_text(event, "status")?;
            let outcome = optional_outcome(event)?;
            let native_status = optional_native_status(event)?;
            let final_text = optional_text(event, "final_text")?;
            let usage = optional_usage(event)?;
            next.state = AgentState::Idle;
            next.active_turn = None;
            next.pending.clear();
            effects.push(Effect::TurnTerminal {
                command_id: Some(command_id),
                turn_id,
                state: TurnState::Completed,
                status: Some(status),
                outcome,
                native_status,
                final_text,
                usage,
                completed_at: now,
            });
        }
        "turn.unknown" => {
            // Live-host projection loss with failed view recovery: settle only
            // the active turn unknown, keep the session alive for a later
            // send/stop. Never a success or failure outcome.
            require_live_turn(agent, event)?;
            let command_id = required_text(event, "command_id")?;
            let turn_id = required_text(event, "turn_id")?;
            next.state = AgentState::Idle;
            next.active_turn = None;
            next.pending.clear();
            effects.push(Effect::TurnTerminal {
                command_id: Some(command_id),
                turn_id,
                state: TurnState::Unknown,
                status: None,
                outcome: Some(TurnOutcome::Unknown),
                native_status: None,
                final_text: None,
                usage: None,
                completed_at: now,
            });
        }
        "session.ended" => {
            if !matches!(
                agent.state,
                AgentState::Starting | AgentState::Idle | AgentState::Busy | AgentState::Waiting
            ) {
                return Err(bad_state(event, "session.ended requires a live Agent"));
            }
            if let Some(turn_id) = &agent.active_turn {
                if !matches!(agent.state, AgentState::Busy | AgentState::Waiting) {
                    return Err(bad_state(
                        event,
                        "an active turn requires a busy or waiting Agent",
                    ));
                }
                effects.push(Effect::TurnTerminal {
                    command_id: None,
                    turn_id: turn_id.clone(),
                    state: TurnState::Unknown,
                    status: None,
                    outcome: None,
                    native_status: None,
                    final_text: None,
                    usage: None,
                    completed_at: now,
                });
            }
            next.state = AgentState::Stopped;
            next.active_turn = None;
            next.pending.clear();
        }
        "tool.started" | "tool.completed" | "hook.before_tool" => {
            // Pure watch facts: exact active-turn binding only, no state change.
            require_live_turn(agent, event)?;
        }
        "session.usage" => {
            // Session-scoped usage snapshot for this Agent generation: no
            // turn binding (occupancy and cumulative cost are not turn
            // expenditure) and no projection change beyond the snapshot
            // itself. Turn-scoped validation stays unchanged.
            let mut usage = optional_session_usage(event)?
                .ok_or_else(|| Error::new("invalid", "session.usage requires a usage object"))?;
            usage.validate()?;
            if usage.observed_at.is_none() {
                usage.observed_at = event.ts.or(Some(now));
            }
            effects.push(Effect::SessionUsageUpdated { usage });
        }
        "hook.before_model" => {
            // Receipt authority stays in Store settlement: selection
            // (claim) requires the exact active turn, but a confirmed
            // receipt may reconcile a claimed Unknown after its native
            // terminal won the race. Store proves the exact claim,
            // generation and captured turn record; no projection change.
            required_text(event, "turn_id")?;
        }
        kind => {
            return Err(Error::new(
                "unsupported",
                format!("unsupported native event kind: {kind}"),
            ));
        }
    }

    next.updated_at = now;
    Ok((next, effects))
}

fn require_live_turn(agent: &Agent, event: &NativeEvent) -> Result<()> {
    let turn_id = required_text(event, "turn_id")?;
    if agent.active_turn.as_deref() != Some(turn_id.as_str()) {
        return Err(Error::new(
            "source_integrity",
            format!(
                "{} turn_id does not match Agent {} active turn",
                event.kind, agent.id
            ),
        ));
    }
    if !matches!(agent.state, AgentState::Busy | AgentState::Waiting) {
        return Err(bad_state(event, "event requires a busy or waiting Agent"));
    }
    Ok(())
}

fn required_text(event: &NativeEvent, field: &str) -> Result<String> {
    event
        .data
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            Error::new(
                "invalid",
                format!("{} requires non-empty string field {field}", event.kind),
            )
        })
}

fn optional_observed_model(event: &NativeEvent) -> Result<Option<String>> {
    match event.data.get("model") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.trim().is_empty() => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(Error::new(
            "invalid",
            format!("{} field model must be a string", event.kind),
        )),
    }
}

fn optional_observed_effort(event: &NativeEvent) -> Result<Option<String>> {
    match event.data.get("effort") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.trim().is_empty() => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(Error::new(
            "invalid",
            format!("{} field effort must be a string", event.kind),
        )),
    }
}

fn optional_outcome(event: &NativeEvent) -> Result<Option<TurnOutcome>> {
    match event.data.get("outcome") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => TurnOutcome::parse(value).map(Some).ok_or_else(|| {
            Error::new(
                "invalid",
                format!(
                    "{} field outcome must be a canonical turn outcome",
                    event.kind
                ),
            )
        }),
        Some(_) => Err(Error::new(
            "invalid",
            format!("{} field outcome must be a string", event.kind),
        )),
    }
}

fn optional_native_status(event: &NativeEvent) -> Result<Option<String>> {
    match event.data.get("native_status") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(_) => Err(Error::new(
            "invalid",
            format!(
                "{} field native_status must be a non-empty string",
                event.kind
            ),
        )),
    }
}

fn optional_text(event: &NativeEvent, field: &str) -> Result<Option<String>> {
    match event.data.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(Error::new(
            "invalid",
            format!("{} field {field} must be a string", event.kind),
        )),
    }
}

fn optional_usage(event: &NativeEvent) -> Result<Option<TurnUsage>> {
    match event.data.get("usage") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(_)) => {
            serde_json::from_value(event.data["usage"].clone()).map_err(|_| {
                Error::new(
                    "invalid",
                    format!("{} field usage must be a usage object", event.kind),
                )
            })
        }
        Some(_) => Err(Error::new(
            "invalid",
            format!("{} field usage must be a usage object", event.kind),
        )),
    }
}

fn optional_session_usage(event: &NativeEvent) -> Result<Option<SessionUsage>> {
    match event.data.get("usage") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(_)) => {
            serde_json::from_value(event.data["usage"].clone()).map_err(|_| {
                Error::new(
                    "invalid",
                    format!("{} field usage must be a session usage object", event.kind),
                )
            })
        }
        Some(_) => Err(Error::new(
            "invalid",
            format!("{} field usage must be a session usage object", event.kind),
        )),
    }
}

fn optional_native_scope(event: &NativeEvent) -> Result<Option<String>> {
    match event.data.get("native_scope") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(_) => Err(Error::new(
            "invalid",
            format!(
                "{} field native_scope must be a non-empty string",
                event.kind
            ),
        )),
    }
}

fn optional_native_ref(event: &NativeEvent) -> Result<Option<String>> {
    match event.data.get("native_ref") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(_) => Err(Error::new(
            "invalid",
            format!("{} field native_ref must be a non-empty string", event.kind),
        )),
    }
}

fn optional_effective(event: &NativeEvent) -> Result<Option<Value>> {
    match event.data.get("effective") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(_)) => Ok(Some(event.data["effective"].clone())),
        Some(_) => Err(Error::new(
            "invalid",
            format!("{} field effective must be an object", event.kind),
        )),
    }
}

fn optional_details(event: &NativeEvent) -> Result<Value> {
    match event.data.get("details") {
        None | Some(Value::Null) => Ok(empty_object()),
        Some(Value::Object(_)) => Ok(event.data["details"].clone()),
        Some(_) => Err(Error::new(
            "invalid",
            format!("{} field details must be an object", event.kind),
        )),
    }
}

fn bad_state(event: &NativeEvent, message: &str) -> Error {
    Error::new("bad_state", format!("{}: {message}", event.kind))
}

#[cfg(test)]
mod outcome_tests {
    use super::*;

    #[test]
    fn outcome_wire_round_trips_through_its_canonical_names() {
        for outcome in [
            TurnOutcome::Success,
            TurnOutcome::Failure,
            TurnOutcome::Interrupted,
            TurnOutcome::Unknown,
        ] {
            assert_eq!(TurnOutcome::parse(outcome.as_str()), Some(outcome));
        }
        assert_eq!(TurnOutcome::parse("completed"), None);
    }

    #[test]
    fn legacy_classification_never_defaults_to_success() {
        assert_eq!(
            TurnOutcome::classify_legacy_status(Some("completed")),
            TurnOutcome::Success
        );
        for status in [Some("failed"), Some("error")] {
            assert_eq!(
                TurnOutcome::classify_legacy_status(status),
                TurnOutcome::Failure,
                "status={status:?}"
            );
        }
        for status in [Some("interrupted"), Some("cancelled")] {
            assert_eq!(
                TurnOutcome::classify_legacy_status(status),
                TurnOutcome::Interrupted,
                "status={status:?}"
            );
        }
        for status in [
            None,
            Some("success"),
            Some("inProgress"),
            Some(""),
            Some("max_tokens"),
        ] {
            assert_eq!(
                TurnOutcome::classify_legacy_status(status),
                TurnOutcome::Unknown,
                "status={status:?}"
            );
        }
    }

    #[test]
    fn legacy_classification_uses_the_stored_harness_for_claude() {
        // Claude legacy rows carry the raw terminal reason, not the
        // normalized lifecycle status: `success` and the `api_error*`
        // family must not read back as unknown.
        // Non-Claude harnesses keep the normalized lifecycle mapping.
        for harness in ["codex", "muse", "opencode", "antigravity", "", "fake"] {
            for status in [
                Some("completed"),
                Some("failed"),
                Some("success"),
                Some("api_error"),
                None,
            ] {
                assert_eq!(
                    TurnOutcome::classify_legacy_status_for_harness(harness, status),
                    TurnOutcome::classify_legacy_status(status),
                    "harness={harness} status={status:?}"
                );
            }
        }
        assert_eq!(
            TurnOutcome::classify_legacy_status_for_harness("claude", Some("success")),
            TurnOutcome::Success,
        );
        for status in [
            Some("cancelled"),
            Some("interrupted"),
            Some(" interrupted "),
        ] {
            assert_eq!(
                TurnOutcome::classify_legacy_status_for_harness("claude", status),
                TurnOutcome::Interrupted,
                "status={status:?}"
            );
        }
        for status in [
            Some("api_error"),
            Some("api_error_rate_limited"),
            Some("API_ERROR_TIMEOUT"),
            Some("error"),
            Some("ErrorBudget"),
            Some("failed"),
            Some("failed_to_start"),
            Some("failure"),
        ] {
            assert_eq!(
                TurnOutcome::classify_legacy_status_for_harness("claude", status),
                TurnOutcome::Failure,
                "status={status:?}"
            );
        }
        for status in [
            None,
            Some("completed"),
            Some("end_turn"),
            Some("inProgress"),
            Some(""),
            Some("max_tokens"),
            Some("bogus"),
        ] {
            assert_eq!(
                TurnOutcome::classify_legacy_status_for_harness("claude", status),
                TurnOutcome::Unknown,
                "status={status:?}"
            );
        }
    }
}

#[cfg(test)]
mod session_usage_tests {
    use super::*;
    use serde_json::json;

    fn live_agent() -> Agent {
        let mut agent = Agent::new("a1", "project", "codex", "fake", 0);
        agent.state = AgentState::Idle;
        agent
    }

    fn usage_event(usage: Value, ts: Option<i64>) -> NativeEvent {
        NativeEvent {
            kind: "session.usage".to_owned(),
            native_id: "n1".to_owned(),
            data: json!({"usage": usage}),
            ts,
        }
    }

    #[test]
    fn session_usage_applies_snapshot_and_defaults_observed_at() {
        let agent = live_agent();
        let (next, effects) = apply(
            &agent,
            &usage_event(
                json!({
                    "context_used_tokens": 100,
                    "context_window_tokens": 200000,
                    "cumulative_tokens": {"input_tokens": 10, "output_tokens": 20},
                    "cumulative_cost": {"amount": 0.5, "unit": "usd_estimate"},
                    "source": "codex:thread/tokenUsage/updated",
                }),
                None,
            ),
            42,
        )
        .unwrap();
        assert_eq!(effects.len(), 1);
        let Effect::SessionUsageUpdated { usage } = &effects[0] else {
            panic!("expected SessionUsageUpdated, got {:?}", effects[0]);
        };
        assert_eq!(usage.context_used_tokens, Some(100));
        assert_eq!(usage.context_window_tokens, Some(200000));
        assert_eq!(
            usage
                .cumulative_tokens
                .as_ref()
                .and_then(|tokens| tokens.input_tokens),
            Some(10)
        );
        assert_eq!(
            usage
                .cumulative_cost
                .as_ref()
                .and_then(|cost| cost.unit.as_deref()),
            Some("usd_estimate")
        );
        assert_eq!(usage.observed_at, Some(42));
        // Session scope never touches turn binding or lifecycle.
        assert_eq!(next.state, AgentState::Idle);
        assert_eq!(next.active_turn, None);
        assert!(next.pending.is_empty());
    }

    #[test]
    fn session_usage_keeps_explicit_observed_at_and_nulls_absent_fields() {
        let agent = live_agent();
        let (_, effects) = apply(
            &agent,
            &usage_event(
                json!({"source": "claude:result", "observed_at": 7}),
                Some(99),
            ),
            42,
        )
        .unwrap();
        let Effect::SessionUsageUpdated { usage } = &effects[0] else {
            panic!("expected SessionUsageUpdated, got {:?}", effects[0]);
        };
        assert_eq!(usage.observed_at, Some(7));
        assert_eq!(usage.context_used_tokens, None);
        assert_eq!(usage.cumulative_tokens, None);
        assert_eq!(usage.cumulative_cost, None);
    }

    #[test]
    fn session_usage_requires_a_usage_object_and_validates_it() {
        let agent = live_agent();
        for data in [
            json!({}),
            json!({"usage": Value::Null}),
            json!({"usage": []}),
            json!({"usage": {"context_used_tokens": -1}}),
            json!({"usage": {"cumulative_cost": {"amount": -0.5}}}),
        ] {
            let event = NativeEvent {
                kind: "session.usage".to_owned(),
                native_id: "n1".to_owned(),
                data,
                ts: None,
            };
            assert!(apply(&agent, &event, 42).is_err());
        }
        // Non-finite costs are rejected at the owning boundary too.
        let infinite = SessionUsage {
            context_used_tokens: None,
            context_window_tokens: None,
            cumulative_tokens: None,
            cumulative_cost: Some(SessionCost {
                amount: Some(f64::INFINITY),
                unit: Some("usd_estimate".to_owned()),
            }),
            source: None,
            observed_at: None,
        };
        assert!(infinite.validate().is_err());
    }

    #[test]
    fn session_usage_never_derives_or_clears_turn_usage() {
        // A session snapshot commits without any turn effect or projection
        // change, so per-turn TurnUsage can never gain session counters.
        let agent = live_agent();
        let (next, effects) = apply(
            &agent,
            &usage_event(json!({"cumulative_tokens": {"input_tokens": 9}}), None),
            42,
        )
        .unwrap();
        assert!(
            effects
                .iter()
                .all(|effect| !matches!(effect, Effect::TurnTerminal { .. })),
            "session facts must not emit turn effects"
        );
        let untouched = TurnUsage {
            scope: None,
            message_id: None,
            input_tokens: None,
            output_tokens: None,
            reasoning_tokens: None,
            thinking_tokens: None,
            cache_read_tokens: None,
            cache_write_tokens: None,
            total_tokens: None,
        };
        assert_eq!(
            serde_json::to_value(&untouched).unwrap()["input_tokens"],
            Value::Null
        );
        assert_eq!(next.state, agent.state);
    }
}

#[cfg(test)]
mod live_wire_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn event_without_subject_or_cause_preserves_legacy_wire_and_deserializes() {
        let event = Event {
            seq: 9,
            ts: 10,
            kind: "turn.completed".to_owned(),
            agent: Some("agent-1".to_owned()),
            subject: None,
            generation: Some(3),
            source: "core".to_owned(),
            native_id: None,
            data: json!({"status":"completed"}),
            cause: None,
        };
        let legacy_wire = r#"{"seq":9,"ts":10,"kind":"turn.completed","agent":"agent-1","generation":3,"source":"core","native_id":null,"data":{"status":"completed"}}"#;
        assert_eq!(serde_json::to_string(&event).unwrap(), legacy_wire);
        assert_eq!(serde_json::from_str::<Event>(legacy_wire).unwrap(), event);

        let caused = Event {
            cause: Some(EventCause {
                command_id: "command-1".to_owned(),
                rule: Some("stop-on-result".to_owned()),
                remote_call: None,
                depth: 1,
            }),
            ..event
        };
        let wire = serde_json::to_value(&caused).unwrap();
        assert_eq!(
            wire["cause"],
            json!({"command_id":"command-1","rule":"stop-on-result","depth":1})
        );
        assert_eq!(serde_json::from_value::<Event>(wire).unwrap(), caused);
    }

    #[test]
    fn event_cause_has_strict_typed_wire_and_leaves_depth_policy_to_store() {
        let ordinary = EventCause {
            command_id: "command-ordinary".to_owned(),
            rule: None,
            remote_call: None,
            depth: 0,
        };
        ordinary.validate().unwrap();
        assert_eq!(
            serde_json::to_value(&ordinary).unwrap(),
            json!({"command_id":"command-ordinary","depth":0})
        );
        assert_eq!(
            serde_json::from_value::<EventCause>(
                json!({"command_id":"command-ordinary","depth":0})
            )
            .unwrap(),
            ordinary
        );

        let cause = EventCause {
            command_id: "command-1".to_owned(),
            rule: Some("stop-on-result".to_owned()),
            remote_call: None,
            depth: 6,
        };
        cause.validate().unwrap();
        let wire = json!({
            "command_id":"command-1",
            "rule":"stop-on-result",
            "depth":6
        });
        assert_eq!(serde_json::to_value(&cause).unwrap(), wire);
        assert_eq!(serde_json::from_value::<EventCause>(wire).unwrap(), cause);

        let blank_command: EventCause =
            serde_json::from_value(json!({"command_id":" ","depth":0})).unwrap();
        assert!(blank_command.validate().is_err());
        let blank_rule: EventCause =
            serde_json::from_value(json!({"command_id":"command-1","rule":" ","depth":0})).unwrap();
        assert!(blank_rule.validate().is_err());
        assert!(
            serde_json::from_value::<EventCause>(
                json!({"command_id":"command-1","depth":0,"extra":true})
            )
            .is_err()
        );
    }

    #[test]
    fn event_subject_is_a_pure_agent_snapshot() {
        let mut agent = Agent::new("agent-1", "project-1", "codex", "luna", 0);
        agent.state = AgentState::Busy;
        agent.workspace = Some("workspace-1".to_owned());

        assert_eq!(
            EventSubject::from(&agent),
            EventSubject {
                project: "project-1".to_owned(),
                harness: "codex".to_owned(),
                profile: "luna".to_owned(),
                state: AgentState::Busy,
                workspace: Some("workspace-1".to_owned()),
            }
        );
    }

    #[test]
    fn native_timestamp_none_preserves_legacy_wire_and_event_array_digest() {
        let event = NativeEvent {
            kind: "message.completed".to_owned(),
            native_id: "n1".to_owned(),
            data: json!({"text":"ok"}),
            ts: None,
        };
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"kind":"message.completed","native_id":"n1","data":{"text":"ok"}}"#
        );
        assert_eq!(
            serde_json::from_value::<NativeEvent>(event.semantic_value()).unwrap(),
            event
        );
        let mut batch = SourceBatch {
            batch_id: "b1".to_owned(),
            agent_id: "a1".to_owned(),
            generation: 1,
            source: "stream".to_owned(),
            events: vec![event],
            cursor: None,
        };
        let legacy = batch.semantic_digest().unwrap();
        assert_eq!(
            legacy
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "d633ea2f28cf283cfe23d4549499892a9c8e915192418b3a9f59e51874cde778"
        );
        batch.batch_id = "b2".to_owned();
        batch.agent_id = "a2".to_owned();
        batch.generation = 7;
        batch.source = "transcript".to_owned();
        batch.cursor = Some("edge".to_owned());
        assert_eq!(batch.semantic_digest().unwrap(), legacy);
        batch.events[0].ts = Some(123);
        assert_ne!(batch.semantic_digest().unwrap(), legacy);
        assert_eq!(batch.events[0].semantic_value()["ts"], 123);
        batch.events[0].ts = Some(124);
        assert_ne!(
            batch.semantic_digest().unwrap(),
            canonical_json_sha256(&Value::Array(vec![
                json!({"kind":"message.completed","native_id":"n1","data":{"text":"ok"},"ts":123})
            ]))
            .unwrap()
        );
    }

    #[test]
    fn health_update_has_strict_fields_and_four_health_values() {
        for health in ["ok", "slow", "stale", "error"] {
            let update = SourceHealthUpdate::new("a1", 1, "transcript", health);
            update.validate().unwrap();
            assert_eq!(
                serde_json::from_value::<SourceHealthUpdate>(
                    serde_json::to_value(&update).unwrap()
                )
                .unwrap(),
                update
            );
        }
        for wire in [
            json!({}),
            json!({"agent_id":"a1","generation":1,"source":"transcript","health":null}),
            json!({"agent_id":"a1","generation":"1","source":"transcript","health":"ok"}),
            json!({"agent_id":"a1","generation":1,"source":"transcript","health":"ok","extra":0}),
        ] {
            assert!(serde_json::from_value::<SourceHealthUpdate>(wire).is_err());
        }
        assert_eq!(
            SourceHealthUpdate::new("a1", 1, "transcript", "healthy")
                .validate()
                .unwrap_err()
                .code,
            "invalid"
        );
        assert!(
            SourceHealthUpdate::new("a1", -1, "transcript", "ok")
                .validate()
                .is_err()
        );
        assert!(
            SourceHealthUpdate::new("a1", 1, " ", "ok")
                .validate()
                .is_err()
        );
    }
}
