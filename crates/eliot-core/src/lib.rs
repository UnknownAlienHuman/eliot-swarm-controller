#![forbid(unsafe_code)]

pub mod api;
pub mod limits;
pub mod liveness;
pub mod requests;

pub use api::{
    DEFAULT_GIT_FETCH_TIMEOUT_MS, DEFAULT_GIT_INTEGRATE_TIMEOUT_MS, FrozenGitFetch,
    FrozenGitIntegrate, FrozenScriptContext, FrozenScriptInterpreter, FrozenScriptRun,
    GitFetchDefinitionArgs, GitFetchOutcome, GitIntegrateDefinitionArgs, GitIntegrateOutcome,
    RunDefinitionArgs, ScriptActionRecord, ScriptCommandContext, ScriptCustody, ScriptDirectExit,
    ScriptInputContext, ScriptInterpreter, ScriptProcessResult, ScriptRuleContext,
    ScriptRunApplied, ScriptTimerContext, ScriptTimerInvocation, bound_script_outcome_stderr,
};

mod agent_start;
mod background_wake;
mod canonical;
mod command;
mod context;
mod diagnostics;
mod ids;
pub mod local_provider;
mod mcp_policy;
mod naming;
mod native_child;
mod native_store;
mod observe;
mod presentation;
mod remote_call;
mod routing;
mod secrets;
mod source_handoff;
mod verbatim;

pub use agent_start::AgentStartParams;
pub use background_wake::{
    BACKGROUND_SERVER_TOPIC, BACKGROUND_WAKE_NOTE_KIND, BACKGROUND_WAKE_PRINCIPAL,
    DEFAULT_BACKGROUND_WAKE_MAX_WAIT_SECS, MAX_BACKGROUND_WAKE_COMMAND_BYTES,
    MAX_BACKGROUND_WAKE_MAX_WAIT_SECS, MAX_BACKGROUND_WAKE_OUTPUT_TAIL_BYTES,
    extract_configured_mcp_programs, format_background_wake_note_body, is_harness_owned_process,
    redact_secret_shapes,
};
pub use canonical::canonical_json_sha256;
pub use command::{
    classify_dispatched_error, command_transition_allowed, handoff_brief_resolution_allowed,
    is_attachment_verb, is_coordination_verb, is_deterministic_pre_admission_rejection,
    is_request_reply_verb, is_start_verb, is_user_event_verb, validate_command_id,
    workspace_preparation_transition_allowed,
};
pub use context::{
    ContextEditPath, ContextItem, ContextNoteCounters, ContextOutcome, ContextWaitRequest,
    InjectArgs, InjectPoint, RetainedInjectArgs, ReviewWorktreeState, TrustedReviewLabels,
    decode_retained_inject_args, turn_started_data,
};
pub use diagnostics::{
    MAX_STDERR_BYTES, MAX_STDERR_DIAGNOSTIC_BYTES, StderrDiagnostic, StderrSnapshot, StderrSource,
    bound_stderr_diagnostics,
};
pub use ids::parse_agent_id;
pub use mcp_policy::{
    MAX_MCP_ALLOW_ENTRIES, MAX_MCP_SERVER_NAME_LEN, MCP_POLICY_INHERIT, MCP_POLICY_MINIMAL,
    McpPolicy, SWARM_RESERVED_SERVER_NAMES, is_owner_swarm_http, is_owner_swarm_server,
    is_owner_swarm_stdio, is_swarm_executable_command, is_swarm_reserved_server_name,
    parse_snapshot_allow_list, parse_snapshot_cloud_hostname, strip_jsonc_comments,
    strip_jsonc_trailing_commas, url_host_equals, validate_allow_list,
};
pub use naming::{
    FALLBACK_SLUG, MAX_PROJECT_SLUG_LEN, MAX_TASK_SLUG_LEN, is_windows_device_slug, project_slug,
    standard_branch, standard_leaf, task_slug,
};
pub use native_child::{
    CODEX_NATIVE_CHILD_FAMILIES, ChildRegistration, ChildRegistrationAck,
    MUSE_NATIVE_CHILD_FAMILIES, NATIVE_CHILD_FAMILIES, NativeChildActivity,
    NativeChildActivityState, native_child_families_for_harness,
};
pub use native_store::NativeStoreBinding;
pub use observe::{
    ACTIVITY_HEAVY_KIND, ATTENTION_STEP_KIND, AttentionStep, BUILD_WORKER_PROGRAMS,
    CATEGORY_SOURCE_CMDLINE, CATEGORY_SOURCE_IMAGE, CONSOLE_HOST_PROGRAMS, DEFAULT_HEAVY_CPU_SECS,
    DEFAULT_HEAVY_RSS_BYTES, DEFAULT_HEAVY_WALL_SECS, DEFAULT_SESSION_FILE_BYTES,
    DEFAULT_SESSION_GROWTH_RATIO, HARNESS_DRIFT_KIND, HarnessDrift, HeavyEpisode, HeavyThresholds,
    OBSERVE_LEVEL_DEBUG, OBSERVE_LEVEL_STANDARD, SESSION_GROWTH_KIND, SHELL_WRAPPER_PROGRAMS,
    SessionGrowth, SessionGrowthBasis, SessionGrowthThresholds, VERIFIED_HARNESSES,
    VerifiedHarness, check_harness_drift, classify_heavy_command, compact_hint_for_harness,
    evaluate_session_growth, merge_heavy_episodes, normalize_harness_version,
    resolve_heavy_category, split_unix_cmdline, split_windows_command_line, unwrap_shell_command,
    verified_harness,
};
pub use presentation::{
    count_words, fence_untrusted_prefix, fence_untrusted_text, origin_words, reason_message,
    reason_words, swarm_note, swarm_note_with_trusted_review_labels,
};
pub use remote_call::{RemoteCallContext, RemoteIdentity};
pub use routing::{
    CapacityScope, MAX_AUTOMATIC_PROVIDER_PROBES, PROVIDER_LAUNCH_OVERRIDE_FIELDS,
    PROVIDER_PROBE_LAUNCH_ARG, PROVIDER_PROBE_PROMPT, ProviderHold, ProviderHoldState,
    ProviderRestriction, ProviderRestrictionKind, RouteBinding, RouteQuotaStatus, RouteSkip,
    RouteSkipReason, RoutingDecision, frozen_provider_scope, frozen_provider_scope_for,
    provider_switch_authority,
};
pub use secrets::SecretPolicy;
pub use source_handoff::{SourceHandoff, SourceHandoffAck, SourceHandoffGap};
pub use verbatim::{display_path, verbatim_path, workspace_path_key};

use crate::api::AgentConfigView;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::{self, Display, Formatter};

fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

fn is_false(value: &bool) -> bool {
    !value
}

/// Daemon-derived launch argument carrying the resolved launch Goal.
pub const PROJECT_CHECK_GOAL_ARG: &str = "_project_check_goal";
/// Daemon-derived launch argument marking the use of a project policy default.
pub const PROJECT_POLICY_APPLIED_ARG: &str = "_project_policy_applied";

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

/// Naming parts for a daemon-prepared standard worktree directory. The
/// Store finalizes the directory (and, for generated branches, the branch)
/// with the admitted Agent ID, so the standard never derives from caller
/// command-id syntax. An absent value means a legacy preparation without a
/// standard directory. An explicitly requested branch always wins and is
/// carried in `branch`; a `None` branch mints the standard branch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratedWorktreeNaming {
    pub project_slug: String,
    pub task_slug: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated: Option<GeneratedWorktreeNaming>,
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
        /// Typed failure location for the preparation failure
        /// (`store.dispatch.workspace_gate` and below). Optional for
        /// backward compatibility with persisted outcomes.
        #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
        failure_where: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin: Option<FailureOrigin>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin_evidence: Option<String>,
    },
    Unknown {
        code: String,
        message: String,
        /// Typed failure location for the preparation failure. Optional
        /// for backward compatibility with persisted outcomes.
        #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
        failure_where: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin: Option<FailureOrigin>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin_evidence: Option<String>,
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
pub struct GitWorkProbe {
    pub available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commits_ahead: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_changed: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub insertions: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletions: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dirty_digest: Option<String>,
    #[serde(default)]
    pub has_changes: bool,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work: Option<GitWorkProbe>,
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
/// Variants are declared in ascending authority order; `Ord` is that rank.
#[derive(
    schemars::JsonSchema,
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
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
    #[serde(default)]
    pub archived: bool,
    #[serde(default)]
    pub chat: bool,
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
    /// The durable reason supplied when this Agent explicitly opted out of
    /// the registered project's launch policy, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_opt_out_reason: Option<String>,
    /// The pre-resolution authorization namespace when it differs from the project.
    #[serde(skip)]
    pub project_scope: Option<String>,
    pub harness: String,
    pub profile: String,
    #[serde(default = "empty_object")]
    pub profile_snapshot: Value,
    /// Whether the launch froze the parent's snapshot verbatim: true only
    /// for an Agent child that reused its own parent's snapshot at launch
    /// freeze. An Owner launch that resolves the same profile is not
    /// inherited, and a child on an allowed different profile is neither.
    #[serde(default)]
    pub profile_inherited: bool,
    /// Admission-time routing decision, retained independently of command history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_choice: Option<RoutingDecision>,
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
    pub fn is_automatic_provider_probe(&self) -> bool {
        self.chat
            && !self.observe_only
            && self.harness == "codex"
            && self.project.is_empty()
            && self.parent.is_none()
            && self.profile_snapshot.get(PROVIDER_PROBE_LAUNCH_ARG)
                == Some(&serde_json::Value::Bool(true))
    }

    pub fn new(
        id: impl Into<String>,
        project: impl Into<String>,
        harness: impl Into<String>,
        profile: impl Into<String>,
        now: i64,
    ) -> Self {
        let id = id.into();
        Self {
            archived: false,
            chat: false,
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
            policy_opt_out_reason: None,
            project_scope: None,
            harness: harness.into(),
            profile: profile.into(),
            profile_snapshot: empty_object(),
            profile_inherited: false,
            profile_choice: None,
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

    pub fn project_scope(&self) -> &str {
        self.project_scope.as_deref().unwrap_or(&self.project)
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
            project: agent.project_scope().to_owned(),
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
    /// Retained configuration the command was admitted or dispatched under,
    /// resolved from its exact retained generation and revision: the
    /// requested and effective model/effort, the revision, the provenance,
    /// and the profile name. `None` when the command runs under the frozen
    /// snapshot or carries no native configuration. Trailing field; absent
    /// on old wire payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<AgentConfigView>,
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
    /// (`success`/`completed` -> success; `cancelled`/`interrupted` ->
    /// interrupted; `error`/`failed`/`failure` and the `api_error*` family
    /// -> failure). Every other harness carries the normalized lifecycle
    /// status and uses [`Self::classify_legacy_status`]. Anything else is
    /// unknown, never success.
    pub fn classify_legacy_status_for_harness(harness: &str, status: Option<&str>) -> Self {
        if harness != "claude" {
            return Self::classify_legacy_status(status);
        }
        match status.map(str::trim) {
            Some("success") | Some("completed") => Self::Success,
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

/// Provenance of a terminal result when native completion is unproven.
#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnResolutionSource {
    Owner,
    Unknown,
}

impl TurnResolutionSource {
    /// Outcome for fleet summaries and waits that serve as completion signals.
    pub fn reported_outcome(self, outcome: TurnOutcome) -> TurnOutcome {
        match (self, outcome) {
            (Self::Unknown, _) | (Self::Owner, TurnOutcome::Success) => TurnOutcome::Unknown,
            (Self::Owner, outcome) => outcome,
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
    /// Canonical outcome, qualified by `resolution_source`. Legacy native
    /// statuses are classified by [`TurnOutcome::classify_legacy_status_for_harness`].
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
    /// Retained configuration the turn ran under, resolved from its exact
    /// retained generation and revision: the requested and effective
    /// model/effort, the revision, the provenance, and the profile name.
    /// `None` when the turn ran under the frozen snapshot. Trailing field;
    /// absent on old wire payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<AgentConfigView>,
    /// Causal predecessor of a non-success terminal outcome: the command
    /// whose terminal event settled this turn. `None` for success, for
    /// running turns, and for rows settled before predecessors were
    /// recorded. Trailing field; absent on old wire payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preceded_by: Option<String>,
    /// `owner` qualifies the recorded status and outcome as an Owner assertion.
    /// `unknown` means a legacy terminal row lacks evidence of its source.
    /// Absent for native completion, unsettled turns, and old wire payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_source: Option<TurnResolutionSource>,
    /// Effective native model observed for the turn, or its verified launch selection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_model: Option<String>,
    /// Message from the committed native terminal error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_error_message: Option<String>,
}

/// Native token usage attributed to one terminal turn. Every member is
/// nullable: absent native data stays null, never zero. `scope` names the
/// native attribution (`message` for the final assistant message of the
/// bound turn, `main_loop_turn` for one turn's main-loop-only usage,
/// `turn` for one turn's aggregate or per-turn-delta usage); unknown fields
/// are ignored so harnesses decode forward. Context occupancy/capacity and
/// cumulative session cost are session-scoped facts, never turn expenditure:
/// they are not represented here. The Store projects adapter counters into
/// the shared accounting basis when serving usage reads.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
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
    /// Input of the first measured model call, including cache reads and
    /// writes counted once. Session and whole-turn totals cannot fill it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_call_input_tokens: Option<i64>,
}

impl TurnUsage {
    /// Project the retained vendor counters onto uncached input, cached
    /// input, and output. Native totals can include overlapping categories.
    pub fn normalized(mut self, harness: &str) -> Self {
        self.first_call_input_tokens = self.first_call_input_tokens.filter(|n| *n >= 0);
        self.input_tokens = uncached_input(
            harness,
            self.input_tokens,
            self.cache_read_tokens,
            self.cache_write_tokens,
        );
        if harness == "opencode" {
            // OpenCode's output is visible text only; reasoning is disjoint.
            self.output_tokens = self
                .output_tokens
                .zip(self.reasoning_tokens)
                .and_then(|(visible, reasoning)| visible.checked_add(reasoning));
        }
        self.reasoning_tokens = self.reasoning_tokens.or(self.thinking_tokens);
        self.total_tokens =
            if matches!(harness, "claude" | "opencode") && self.cache_write_tokens.is_none() {
                None
            } else {
                self.input_tokens
                    .zip(self.cache_read_tokens)
                    .zip(self.output_tokens)
                    .and_then(|((input, cached), output)| {
                        input.checked_add(cached)?.checked_add(output)
                    })
            };
        self
    }

    pub fn reported_total(&self) -> Option<i64> {
        reported_token_total(
            self.input_tokens,
            self.cache_read_tokens,
            self.output_tokens,
        )
    }

    /// Construct usage from translated native counters, preserving absence
    /// and reporting a usage record only when at least one counter is known.
    pub fn from_native(scope: &str, mut counters: Self) -> Option<Self> {
        let fields = [
            &mut counters.input_tokens,
            &mut counters.output_tokens,
            &mut counters.reasoning_tokens,
            &mut counters.thinking_tokens,
            &mut counters.cache_read_tokens,
            &mut counters.cache_write_tokens,
            &mut counters.total_tokens,
        ];
        let mut observed = false;
        for counter in fields {
            *counter = counter.filter(|value| *value >= 0);
            observed |= counter.is_some();
        }
        if observed {
            counters.scope = Some(scope.to_owned());
            Some(counters)
        } else {
            None
        }
    }

    /// Sum each native counter only when every observation supplied it.
    /// The caller selects and deduplicates the observations for the turn.
    pub fn sum_native(scope: &str, counters: impl IntoIterator<Item = Self>) -> Option<Self> {
        fn add(left: Option<i64>, right: Option<i64>) -> Option<i64> {
            let (left, right) = left.zip(right)?;
            if left < 0 || right < 0 {
                return None;
            }
            left.checked_add(right)
        }
        let totals = counters.into_iter().reduce(|left, right| Self {
            input_tokens: add(left.input_tokens, right.input_tokens),
            output_tokens: add(left.output_tokens, right.output_tokens),
            reasoning_tokens: add(left.reasoning_tokens, right.reasoning_tokens),
            thinking_tokens: add(left.thinking_tokens, right.thinking_tokens),
            cache_read_tokens: add(left.cache_read_tokens, right.cache_read_tokens),
            cache_write_tokens: add(left.cache_write_tokens, right.cache_write_tokens),
            total_tokens: add(left.total_tokens, right.total_tokens),
            ..Self::default()
        })?;
        Self::from_native(scope, totals)
    }
}

/// Subtotal of the reported disjoint counters. Callers retain coverage
/// independently so missing components cannot read as a complete count.
pub fn reported_token_total(
    input: Option<i64>,
    cached: Option<i64>,
    output: Option<i64>,
) -> Option<i64> {
    [input, cached, output]
        .into_iter()
        .flatten()
        .reduce(i64::saturating_add)
}

fn uncached_input(
    harness: &str,
    input: Option<i64>,
    cached: Option<i64>,
    written: Option<i64>,
) -> Option<i64> {
    let input = input?;
    match harness {
        // Codex includes cache hits in its input count.
        "codex" => subtract_cached_input(input, cached),
        // These harnesses report cache creation outside input. Creation
        // spends fresh input tokens that can serve future cache hits.
        "claude" | "opencode" => written.map_or(Some(input), |written| input.checked_add(written)),
        _ => Some(input),
    }
}

fn subtract_cached_input(input: i64, cached: Option<i64>) -> Option<i64> {
    cached.map_or(Some(input), |cached| {
        input.checked_sub(cached).filter(|input| *input >= 0)
    })
}

pub fn usage_counter(value: Option<&Value>) -> Option<i64> {
    value.and_then(Value::as_i64).filter(|count| *count >= 0)
}

/// Session-scoped usage snapshot for one Agent generation. Every member is
/// nullable: absent native data stays null, never zero. The per-call context
/// count (`context_used_tokens`), optional capacity (`context_window_tokens`)
/// and cumulative session cost are never turn
/// expenditure: they are not represented in [`TurnUsage`], never fill its
/// counters, and compaction can reduce context use. A native per-call context
/// count can be valid without a known capacity; ratio checks require both.
/// `cumulative_tokens` carries only input/output/cache
/// counters where the native signal supplies them (never derived totals);
/// `cumulative_cost.amount` is the verbatim native amount with `unit` the
/// verbatim native unit (for example `usd_estimate` for an estimated USD
/// total), never converted and never assumed to be money. `source` names the
/// adapter and native signal (for example `codex:thread/tokenUsage/updated`);
/// `observed_at` is native epoch milliseconds.
/// `session_file_bytes` carries the native session file size where the
/// harness exposes a documented size source (a transcript/rollout scan the
/// adapter performs itself); it stays null where nothing is observable, never
/// zero and never a guess. Unknown fields are ignored so
/// harnesses decode forward.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
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
    pub session_file_bytes: Option<u64>,
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

/// Retain a native context window for the same turn or a later turn on the
/// same known model. The adapter supplies exact native bindings and counters.
#[derive(Default)]
pub struct ContextWindowTracker {
    turn_model: Option<(String, String)>,
    window: Option<ObservedContextWindow>,
}

struct ObservedContextWindow {
    turn_id: String,
    model: Option<String>,
    tokens: i64,
}

impl ContextWindowTracker {
    pub fn associate_turn(&mut self, turn_id: &str, model: Option<&str>) {
        if let Some(window) = &mut self.window
            && window.turn_id == turn_id
        {
            window.model = model.map(str::to_owned);
        }
        self.turn_model = model.map(|model| (turn_id.to_owned(), model.to_owned()));
    }

    pub fn observe(
        &mut self,
        usage: &mut SessionUsage,
        turn_id: &str,
        has_response: bool,
        used: Option<i64>,
        capacity: Option<i64>,
    ) -> bool {
        let model = self
            .turn_model
            .as_ref()
            .filter(|(turn, _)| turn == turn_id)
            .map(|(_, model)| model.as_str());
        let cached = self
            .window
            .as_ref()
            .filter(|window| {
                window.turn_id == turn_id || (model.is_some() && model == window.model.as_deref())
            })
            .map(|window| window.tokens);
        let prior = (usage.context_used_tokens, usage.context_window_tokens);
        if has_response {
            usage.context_used_tokens = used;
            usage.context_window_tokens = capacity.or(cached);
        } else if let Some(window) = capacity
            && (cached.is_none() || usage.context_window_tokens != Some(window))
        {
            // A capacity-only observation cannot carry old occupancy onto a new window.
            usage.context_used_tokens = None;
            usage.context_window_tokens = Some(window);
        }
        usage.retain_valid_context();
        if let Some(tokens) = capacity {
            self.window = (tokens > 0).then(|| ObservedContextWindow {
                turn_id: turn_id.to_owned(),
                model: model.map(str::to_owned),
                tokens,
            });
        }
        prior != (usage.context_used_tokens, usage.context_window_tokens)
    }
}

impl SessionUsage {
    /// Returns an observed occupancy and its compatible context window.
    pub fn context_tokens(&self) -> Option<(i64, i64)> {
        let (used, window) = self.context_used_tokens.zip(self.context_window_tokens)?;
        (window > 0 && (0..=window).contains(&used)).then_some((used, window))
    }

    pub fn retain_valid_context(&mut self) {
        if self.context_used_tokens.is_some_and(|used| used < 0) {
            self.context_used_tokens = None;
        }
        if let Some(window) = self.context_window_tokens {
            if window <= 0 {
                self.context_window_tokens = None;
                self.context_used_tokens = None;
            } else if self.context_used_tokens.is_some_and(|used| used > window) {
                self.context_used_tokens = None;
            }
        }
    }

    pub fn normalized(mut self, harness: &str) -> Self {
        self.retain_valid_context();
        if let Some(tokens) = &mut self.cumulative_tokens {
            tokens.input_tokens = if harness == "muse" {
                // Muse supplies its counted-once prompt total for sessions.
                tokens
                    .input_tokens
                    .and_then(|prompt| subtract_cached_input(prompt, tokens.cache_read_tokens))
            } else {
                uncached_input(
                    harness,
                    tokens.input_tokens,
                    tokens.cache_read_tokens,
                    tokens.cache_write_tokens,
                )
            };
        }
        self
    }

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
    /// Internal-only context metadata. Serde skip keeps it out of the command
    /// wire schema; Store admission persists it only for a bound review rule.
    #[serde(skip)]
    #[schemars(skip)]
    pub trusted_review_labels: Option<context::TrustedReviewLabels>,
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

/// Ephemeral request field carrying a native command to daemon-side guards.
pub const COMMAND_GUARD_CONTEXT_FIELD: &str = "command_guard";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BatchAck {
    pub first_seq: i64,
    pub last_seq: i64,
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_refusal: Option<PermissionRefusal>,
}

/// Daemon refusal returned with the durable acknowledgement for one exact
/// native permission request. The source batch binds the Agent generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionRefusal {
    pub agent_id: String,
    pub generation: i64,
    pub request_id: String,
    pub turn_id: String,
    pub message: String,
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

/// Bounded machine signal for Muse native view-health episodes. The Muse
/// plugin authors these exact reason strings and the readback marker; the
/// three outcomes are exactly countable from the journal (see the Muse
/// harness page). Prefixes (not full reasons) are matched
/// so cursor-bearing suffixes stay free to evolve without breaking the count.
pub const MUSE_VIEW_UNAVAILABLE_REASON_PREFIX: &str = "Muse native view is unavailable";
pub const MUSE_VIEW_UNKNOWN_REASON_PREFIX: &str =
    "Muse native view recovery did not prove live delivery after the recorded gap";
/// Marker key the readback-recovery path attaches to an otherwise unchanged
/// `turn.completed`; its only legal value is [`VIEW_RECOVERY_READBACK`].
pub const VIEW_RECOVERY_MARKER_FIELD: &str = "view_recovery";
pub const VIEW_RECOVERY_READBACK: &str = "readback";

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

/// Character bound for a failure message: every rejection, failure and
/// unknown outcome carries a bounded, `SecretPolicy`-redacted message.
pub const MAX_FAILURE_MESSAGE_CHARS: usize = 1024;
/// Character bound for a typed failure location (`where`).
pub const MAX_FAILURE_WHERE_CHARS: usize = 128;
/// Character bound for the redacted evidence behind a failure origin.
pub const MAX_FAILURE_ORIGIN_EVIDENCE_CHARS: usize = 512;
/// Character bound for an error code on the wire.
pub const MAX_FAILURE_CODE_CHARS: usize = 128;
/// Character bound for a Windows NTSTATUS hex string (`0xC0000005`).
pub const MAX_NTSTATUS_CHARS: usize = 16;

/// A captured process exit: the numeric code plus the Windows NTSTATUS hex
/// when the raw code is a crash status (for example `0xC0000005`). Numbers
/// only: no argv, environment or output ever rides along. The NTSTATUS
/// mapping is a heuristic by construction: negative `i32` codes on Windows
/// are crash statuses (`0x80000000` and up); ordinary application codes are
/// non-negative and Unix signals surface as no code at all.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessExit {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i64>,
    #[serde(
        default,
        deserialize_with = "deserialize_ntstatus",
        skip_serializing_if = "Option::is_none"
    )]
    pub ntstatus: Option<Box<str>>,
}

impl ProcessExit {
    pub fn from_code(code: Option<i32>) -> Self {
        Self {
            exit_code: code.map(|code| code as i64),
            ntstatus: code
                .filter(|code| *code < 0)
                .map(|code| format!("0x{:08X}", code as u32).into()),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.exit_code.is_none() && self.ntstatus.is_none()
    }
}

fn deserialize_ntstatus<'de, D>(deserializer: D) -> std::result::Result<Option<Box<str>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let ntstatus: Option<String> =
        <Option<String> as serde::Deserialize>::deserialize(deserializer)?;
    Ok(ntstatus.map(|ntstatus| bound_chars(&ntstatus, MAX_NTSTATUS_CHARS).into()))
}

/// Where a failure originates, with the evidence for it. The main goal of
/// failure reporting is locating the origin: our Swarm core, bus, Router and
/// plugin code (`swarm`), the native harness process or protocol
/// (`harness`), the model provider behind the harness (`provider`), or the OS
/// and machine (`environment`). `harness_contract_mismatch` is the narrow
/// case where observed native behavior contradicts the documented contract in
/// `docs/harnesses/<h>.md`; it carries the doc section reference.
/// `unknown` means the evidence is insufficient, and its evidence note says
/// what is missing. Unmapped native errors stay `harness` (with a
/// `provider_suspected` note only when the native frame itself says the error
/// came from the upstream API); provider classes are mapped from structured
/// native error signals only, never guessed from free text.
#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureOrigin {
    Swarm,
    Harness,
    HarnessContractMismatch,
    Provider,
    Environment,
    Unknown,
}

impl FailureOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Swarm => "swarm",
            Self::Harness => "harness",
            Self::HarnessContractMismatch => "harness_contract_mismatch",
            Self::Provider => "provider",
            Self::Environment => "environment",
            Self::Unknown => "unknown",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "swarm" => Some(Self::Swarm),
            "harness" => Some(Self::Harness),
            "harness_contract_mismatch" => Some(Self::HarnessContractMismatch),
            "provider" => Some(Self::Provider),
            "environment" => Some(Self::Environment),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }
}

/// An admission preflight refusal: a typed failure before any command exists.
/// [`Self::error`] builds the typed error at any layer; only the router's
/// preflight call site marks it with [`Self::at_preflight`], and only marked
/// errors produce the `command.refused` record `diagnose` can explain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionRefusal {
    /// A configured local capacity bound is reserved.
    Capacity,
    /// An explicit profile executable path does not exist.
    ProfileCommand,
}

impl AdmissionRefusal {
    const ALL: [Self; 2] = [Self::Capacity, Self::ProfileCommand];

    /// `(code, where, origin, evidence)`; schema-owned vocabulary only.
    pub fn parts(self) -> (&'static str, &'static str, FailureOrigin, &'static str) {
        match self {
            Self::Capacity => (
                "capacity_unavailable",
                "store.admission.capacity",
                FailureOrigin::Swarm,
                "configured local capacity bound is reserved",
            ),
            Self::ProfileCommand => (
                "profile_error",
                "router.admission.profile_cmd",
                FailureOrigin::Environment,
                "configured profile executable is missing",
            ),
        }
    }

    /// The typed refusal error, unmarked.
    pub fn error(self, message: impl Into<String>) -> Error {
        let (code, where_, origin, evidence) = self.parts();
        Error::new(code, message)
            .with_where(where_)
            .with_origin(origin, evidence)
    }

    /// Mark a typed refusal at the router's admission preflight call site,
    /// which calls this only after proving no command was admitted for the
    /// request. Any other error passes through unmarked.
    pub fn at_preflight(mut error: Error) -> Error {
        error.admission = Self::of_parts(&error.code, error.failure_where.as_deref(), error.origin);
        error
    }

    /// Parse a retained `command.refused` record written for a marked refusal.
    pub fn of_parts(
        code: &str,
        where_: Option<&str>,
        origin: Option<FailureOrigin>,
    ) -> Option<Self> {
        Self::ALL.into_iter().find(|refusal| {
            let (expected_code, expected_where, expected_origin, _) = refusal.parts();
            code == expected_code
                && where_ == Some(expected_where)
                && origin == Some(expected_origin)
        })
    }

    /// The refusal marked by [`Self::at_preflight`] in this process, if any.
    /// Matching by value never counts: only the private preflight marker does.
    pub fn of(error: &Error) -> Option<Self> {
        error.admission
    }
}

/// The loss reason of a daemon restart: Swarm fences every live generation.
pub const DAEMON_RESTART_REASON: &str = "daemon_restart";

/// The loss reason for a spontaneous plugin root exit: the plugin process
/// exited without a native session terminal, so the running turn is unknown
/// and the generation is lost. The detailed drain cause stays on `source.gap`.
pub const PLUGIN_ROOT_EXITED_REASON: &str = "root_exited";

/// Location of an owned plugin process exit.
pub const PLUGIN_EXIT_WHERE: &str = "plugin.exit";

/// The loss reason for an explicit Owner/host assertion of loss. Only that
/// assertion path may record it; daemon telemetry of a spontaneous exit
/// passes its truthful cause instead.
pub const OWNER_MARKED_LOST_REASON: &str = "owner_marked_lost";

/// Default attribution for a typed loss reason. A recorded producer origin
/// takes precedence. Closure of the daemon's own control channel alone says
/// nothing about the plugin's cause of exit.
pub fn swarm_loss_location(reason: &str) -> Option<(FailureOrigin, &'static str)> {
    match reason {
        DAEMON_RESTART_REASON => Some((FailureOrigin::Swarm, "daemon.restart")),
        PLUGIN_ROOT_EXITED_REASON
        | "stream_eof_without_session_terminal"
        | "plugin_startup_exit" => Some((FailureOrigin::Harness, PLUGIN_EXIT_WHERE)),
        _ => None,
    }
}

/// Truncate to a character boundary; never splits UTF-8 mid-codepoint.
pub fn bound_chars(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        return text.to_owned();
    }
    let mut end = limit;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

/// Normalize a typed failure location to a stable dotted
/// `component.phase[.detail]` lowercase ASCII path. Characters outside that
/// vocabulary fold into `_`; an empty result means the caller supplied no
/// location at all.
pub fn normalize_failure_where(where_: &str) -> String {
    let mut out = String::with_capacity(where_.len().min(MAX_FAILURE_WHERE_CHARS));
    for ch in where_.chars().take(MAX_FAILURE_WHERE_CHARS) {
        let lower = ch.to_ascii_lowercase();
        if lower.is_ascii_alphanumeric() || lower == '.' || lower == '_' {
            out.push(lower);
        } else if !out.ends_with('_') && !out.is_empty() {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches(|c| c == '.' || c == '_');
    if trimmed.is_empty() {
        "unknown".to_owned()
    } else {
        trimmed.to_owned()
    }
}

// Boxed so the owning `Error.message` field stays 16 bytes.
#[allow(
    clippy::box_collection,
    reason = "The boxed error representation keeps the owning error structure small."
)]
fn deserialize_failure_message<'de, D>(
    deserializer: D,
) -> std::result::Result<Box<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let message = <String as serde::Deserialize>::deserialize(deserializer)?;
    // Keep the complete input until the owning boundary can redact it.
    Ok(message.into())
}

fn deserialize_failure_where<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Box<str>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let where_: Option<String> = <Option<String> as serde::Deserialize>::deserialize(deserializer)?;
    Ok(where_.map(|where_| normalize_failure_where(&where_).into()))
}

fn deserialize_origin_evidence<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Box<str>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let evidence: Option<String> =
        <Option<String> as serde::Deserialize>::deserialize(deserializer)?;
    Ok(evidence.map(Into::into))
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Deserialize)]
// Boxed cold payloads keep `Error` at or under 128 bytes so every
// `Result<_, Error>` passes `clippy::result_large_err`; the extra
// allocation only happens on the error path. See
// `error_stays_small_enough_for_result_err`.
#[allow(
    clippy::box_collection,
    reason = "The boxed error representation keeps the owning error structure small."
)]
pub struct Error {
    pub code: String,
    #[serde(deserialize_with = "deserialize_failure_message")]
    pub message: Box<String>,
    #[serde(default, deserialize_with = "diagnostics::deserialize_diagnostics")]
    #[schemars(skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Box<Vec<StderrDiagnostic>>,
    #[serde(default)]
    #[schemars(skip_serializing_if = "Option::is_none")]
    pub details: Option<Box<InvalidParam>>,
    /// Typed admission refusal; also used by desktop routing readback.
    #[serde(default)]
    #[schemars(skip_serializing_if = "Option::is_none")]
    pub routing: Option<Box<RoutingDecision>>,
    /// Typed failure location: stable dotted `component.phase[.detail]`
    /// path (for example `router.dispatch.profile_store`,
    /// `plugin.opencode.start.auth_transfer`,
    /// `store.dispatch.workspace_gate`). Present on rejections, failures and
    /// unknown outcomes; absent on success paths. Schema-owned vocabulary
    /// only: never a secret, parameter, prompt or model output.
    #[serde(
        default,
        deserialize_with = "deserialize_failure_where",
        rename = "where"
    )]
    #[schemars(skip_serializing_if = "Option::is_none")]
    pub failure_where: Option<Box<str>>,
    /// Where the failure originates, with the evidence for it.
    #[serde(default)]
    #[schemars(skip_serializing_if = "Option::is_none")]
    pub origin: Option<FailureOrigin>,
    /// Bounded, redacted evidence for `origin`: a doc section reference for
    /// `harness_contract_mismatch`, a structured provider class detail for
    /// `provider`, or a note naming the missing evidence for `unknown`.
    /// Never a secret, parameter, prompt or model output.
    #[serde(default, deserialize_with = "deserialize_origin_evidence")]
    #[schemars(skip_serializing_if = "Option::is_none")]
    pub origin_evidence: Option<Box<str>>,
    /// Captured process exit when the failure is a process exit: the
    /// harness or plugin exit code plus the Windows NTSTATUS hex for crash
    /// statuses. Recorded with the stderr tail on `agent.lost`/`agent.failed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<Box<i64>>,
    /// Windows NTSTATUS hex for crash statuses (for example `0xC0000005`).
    /// See [`ProcessExit`] for the heuristic.
    #[serde(
        default,
        deserialize_with = "deserialize_ntstatus",
        skip_serializing_if = "Option::is_none"
    )]
    pub ntstatus: Option<Box<str>>,
    /// Private, in-process only: set solely by [`AdmissionRefusal::error`] at
    /// the preflight call site. Never serialized, deserialized or settable by
    /// value, so a later error with the same code/where/origin never counts.
    #[serde(skip)]
    #[schemars(skip)]
    admission: Option<AdmissionRefusal>,
}

/// Parameter diagnostics contain schema-owned names/expectations, never values.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvalidParam {
    pub field: String,
    #[serde(deserialize_with = "deserialize_expected_schema")]
    pub expected: serde_json::Value,
}

fn deserialize_expected_schema<'de, D>(
    deserializer: D,
) -> std::result::Result<serde_json::Value, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = <serde_json::Value as serde::Deserialize>::deserialize(deserializer)?;
    let schema = match value {
        serde_json::Value::Object(_) => value,
        serde_json::Value::String(text) => match serde_json::from_str::<serde_json::Value>(&text) {
            Ok(schema @ serde_json::Value::Object(_)) => schema,
            _ => serde_json::json!({ "description": text }),
        },
        _ => return Err(serde::de::Error::custom("expected a field schema object")),
    };
    Ok(bound_invalid_param_schema(schema))
}

fn bound_invalid_param_schema(schema: serde_json::Value) -> serde_json::Value {
    const MAX_EXPECTED_SCHEMA_BYTES: usize = 512;
    if !schema.is_object()
        || serde_json::to_vec(&schema).map_or(true, |bytes| bytes.len() > MAX_EXPECTED_SCHEMA_BYTES)
    {
        serde_json::json!({ "description": "Use api.help for the field constraints." })
    } else {
        schema
    }
}

impl Error {
    /// A bounded, schema-owned field path and expectation; never a rejected value.
    pub fn invalid_field(field: impl Into<String>, expected: impl Into<String>) -> Self {
        Self::new("invalid", "invalid parameters").with_field(field, expected)
    }

    /// Attach a compact JSON Schema fragment to a field error.
    pub fn invalid_field_schema(field: impl Into<String>, expected: serde_json::Value) -> Self {
        Self::new("invalid", "invalid parameters").with_field_schema(field, expected)
    }

    /// Mutation-id format rejection shared by Store admission and the Router
    /// `goal.round` pre-check. It names `params.id` with the ULID format and
    /// the MCP omission rule, so a fresh agent learns the retry contract
    /// instead of hand-minting ids.
    pub fn invalid_mutation_id() -> Self {
        Self::new("invalid", "mutation id must be a ULID").with_field(
            "params.id",
            "ULID (26 uppercase Crockford base32 characters); omit id on MCP and the server mints one, or mint a fresh ULID for a new write",
        )
    }

    pub fn with_field(self, field: impl Into<String>, expected: impl Into<String>) -> Self {
        let description: String = expected.into().chars().take(400).collect();
        self.with_field_schema(field, serde_json::json!({ "description": description }))
    }

    pub fn with_field_schema(
        mut self,
        field: impl Into<String>,
        expected: serde_json::Value,
    ) -> Self {
        let field: String = field.into().chars().take(256).collect();
        let expected = bound_invalid_param_schema(expected);
        self.message = if self.code == "invalid" {
            format!("invalid parameter: {field}").into()
        } else {
            format!("{}: {field}", self.code).into()
        };
        self.details = Some(Box::new(InvalidParam { field, expected }));
        self
    }

    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        let code: String = code.into().chars().take(MAX_FAILURE_CODE_CHARS).collect();
        Self {
            code,
            message: message.into().into(),
            diagnostics: Box::default(),
            details: None,
            routing: None,
            exit_code: None,
            ntstatus: None,
            failure_where: None,
            origin: None,
            origin_evidence: None,
            admission: None,
        }
    }

    /// Attach a typed failure location. The path is normalized to a stable
    /// dotted `component.phase[.detail]` vocabulary.
    pub fn with_where(mut self, where_: impl Into<String>) -> Self {
        self.failure_where = Some(normalize_failure_where(&where_.into()).into());
        self
    }

    /// Attach the failure origin with its evidence. Evidence must already be
    /// schema-owned vocabulary or a bounded observed-frame summary;
    /// [`Self::sanitized`] applies the [`SecretPolicy`] backstop at the
    /// boundary. Keep the complete evidence until redaction; serialization
    /// independently enforces the wire bound.
    pub fn with_origin(mut self, origin: FailureOrigin, evidence: impl Into<String>) -> Self {
        self.origin = Some(origin);
        self.origin_evidence = Some(evidence.into().into());
        self
    }

    /// A failure the native harness caused: crashed or exited process, native
    /// error frame, refused method, or unexpected/missing frame. `observed`
    /// is a bounded summary of the observed frame (method, direction, size,
    /// error class), never frame content.
    pub fn harness(harness: &str, where_: &str, observed: impl Into<String>) -> Self {
        Self::new(
            "harness_error",
            format!("{harness} failed before reporting a usable result"),
        )
        .with_where(where_)
        .with_origin(FailureOrigin::Harness, observed.into())
    }

    /// The narrow contract-mismatch case: observed native behavior
    /// contradicts the documented contract in `docs/harnesses/<h>.md`.
    /// `doc` names the section (for example
    /// `docs/harnesses/muse.md#steer`); `observed` summarizes the offending
    /// frame in bounded, redacted form, never its content.
    pub fn harness_contract_mismatch(
        harness: &str,
        where_: &str,
        doc: &str,
        observed: impl Into<String>,
    ) -> Self {
        let doc = doc.chars().take(128).collect::<String>();
        let observed = observed.into();
        Self::new(
            "harness_contract_mismatch",
            format!("{harness} contradicted its documented contract"),
        )
        .with_where(where_)
        .with_origin(
            FailureOrigin::HarnessContractMismatch,
            format!("{doc}: {observed}"),
        )
    }

    /// A failure the model provider behind the harness caused, mapped from a
    /// structured native error signal only (never guessed from free text).
    /// `class` is the [`ProviderRestrictionKind`] vocabulary (`rate_limit`,
    /// `quota`, `auth`, `model_removed`); `detail` carries the structured
    /// signal (harness name, status or error class, retry hint).
    pub fn provider(
        class: crate::ProviderRestrictionKind,
        where_: &str,
        detail: impl Into<String>,
    ) -> Self {
        let code = match class {
            crate::ProviderRestrictionKind::RateLimit => "rate_limited",
            crate::ProviderRestrictionKind::Quota => "quota_exhausted",
            crate::ProviderRestrictionKind::Auth => "provider_unauthorized",
            crate::ProviderRestrictionKind::ModelRemoved => "model_unavailable",
        };
        Self::new(
            code,
            format!(
                "model provider {verb} the request",
                verb = match class {
                    crate::ProviderRestrictionKind::RateLimit => "rate-limited",
                    crate::ProviderRestrictionKind::Quota => "refused for quota",
                    crate::ProviderRestrictionKind::Auth => "rejected the credentials for",
                    crate::ProviderRestrictionKind::ModelRemoved =>
                        "no longer serves the model for",
                }
            ),
        )
        .with_where(where_)
        .with_origin(FailureOrigin::Provider, detail.into())
    }

    /// Attach a captured process exit. Numbers only: exit codes and crash
    /// statuses carry no secrets, parameters, prompts or model output.
    pub fn with_exit(mut self, code: Option<i32>) -> Self {
        let exit = ProcessExit::from_code(code);
        self.exit_code = exit.exit_code.map(Box::new);
        self.ntstatus = exit.ntstatus;
        self
    }

    /// Redact the message, origin evidence and stderr tails with the caller
    /// policy and re-apply every bound. `where` is already restricted
    /// vocabulary; it is normalized, never redacted. Call at the trust
    /// boundary before persisting an outcome, event or log record.
    pub fn sanitized(mut self, policy: &SecretPolicy) -> Self {
        self.message = bound_chars(
            &policy.redact_text(&self.message),
            MAX_FAILURE_MESSAGE_CHARS,
        )
        .into();
        if let Some(evidence) = self.origin_evidence.take() {
            self.origin_evidence = Some(
                bound_chars(
                    &policy.redact_text(&evidence),
                    MAX_FAILURE_ORIGIN_EVIDENCE_CHARS,
                )
                .into(),
            );
        }
        if let Some(where_) = self.failure_where.take() {
            self.failure_where = Some(normalize_failure_where(&where_).into());
        }
        for diagnostic in self.diagnostics.iter_mut() {
            diagnostic.snapshot.text = policy.redact_text(&diagnostic.snapshot.text);
        }
        self.bound_diagnostics();
        self
    }

    /// The failure origin, defaulting to `unknown` when no site classified
    /// it. Unknown origins must say what evidence is missing.
    pub fn origin_or_unknown(&self) -> FailureOrigin {
        self.origin.unwrap_or(FailureOrigin::Unknown)
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
        // Trusted caller text replaces the message verbatim, like HEAD:
        // callers pass bounded or arbitrary native text and the 64 KiB wire
        // cap is enforced at encode and persist boundaries, not here.
        self.message = message.into().into();
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
            #[serde(skip_serializing_if = "Option::is_none")]
            routing: Option<&'a RoutingDecision>,
            #[serde(rename = "where", skip_serializing_if = "Option::is_none")]
            failure_where: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            origin: Option<FailureOrigin>,
            #[serde(skip_serializing_if = "Option::is_none")]
            origin_evidence: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            exit_code: Option<i64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            ntstatus: Option<String>,
        }
        // Re-apply every bound at the wire so a hand-built struct literal
        // can never exceed the 64 KiB encoded error.
        let code: String = self.code.chars().take(MAX_FAILURE_CODE_CHARS).collect();
        let message = bound_chars(&self.message, MAX_FAILURE_MESSAGE_CHARS);
        let ntstatus = self
            .ntstatus
            .as_deref()
            .map(|ntstatus| bound_chars(ntstatus, MAX_NTSTATUS_CHARS));
        let failure_where = self.failure_where.as_deref().map(normalize_failure_where);
        let origin_evidence = self
            .origin_evidence
            .as_deref()
            .map(|evidence| bound_chars(evidence, MAX_FAILURE_ORIGIN_EVIDENCE_CHARS));
        let mut diagnostics = (*self.diagnostics).clone();
        bound_stderr_diagnostics(&mut diagnostics, error_diagnostic_budget(&code, &message));
        WireError {
            code: &code,
            message: &message,
            diagnostics,
            failure_where,
            origin: self.origin,
            origin_evidence,
            exit_code: self.exit_code.as_deref().copied(),
            ntstatus,
            details: self.details.as_ref().map(|detail| InvalidParam {
                field: detail.field.chars().take(256).collect(),
                expected: bound_invalid_param_schema(detail.expected.clone()),
            }),
            routing: self.routing.as_deref(),
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
            let model = optional_observed_model(event)?;
            next.state = AgentState::Busy;
            next.active_turn = Some(turn_id.clone());
            // Refresh the admitted native settings when the harness reports
            // what the native turn runs under (Codex reports the model and
            // effort its turn request carried, only under a `configure`
            // overlay; Muse reports its session-verified model on every
            // association once verified, plus the carried effort under an
            // overlay): the
            // exact-turn association acknowledges native admission of those
            // values, so they read back verified as carried. Absent fields
            // invent nothing and a missing `effective` object is never
            // fabricated: only launch readback creates it.
            if let Some(Value::Object(effective)) = next.effective.as_mut() {
                if let Some(model) = model.as_deref() {
                    effective.insert("model".to_owned(), Value::String(model.to_owned()));
                    effective.insert("model_verified".to_owned(), Value::Bool(true));
                }
                if let Some(effort) = effort.as_deref() {
                    effective.insert("effort".to_owned(), Value::String(effort.to_owned()));
                    effective.insert("effort_verified".to_owned(), Value::Bool(true));
                }
            }
            effects.push(Effect::CommandTurnAssociated {
                command_id,
                turn_id,
                effort,
            });
        }
        "turn.model_observed" => {
            require_live_turn(agent, event)?;
            record_serving_model(&mut next, required_text(event, "model")?);
        }
        "turn.notice" => {
            require_live_turn(agent, event)?;
            let notice: api::NativeNotice = serde_json::from_value(event.data.clone())
                .map_err(|_| Error::new("invalid", "native notice schema is invalid"))?;
            notice.validate()?;
            if notice.turn_id != required_text(event, "turn_id")? {
                return Err(Error::new(
                    "source_integrity",
                    "native notice turn binding is inconsistent",
                ));
            }
        }
        "message.completed" => {
            require_live_turn(agent, event)?;
            let command_id = required_text(event, "command_id")?;
            let turn_id = required_text(event, "turn_id")?;
            let text = required_text(event, "text")?;
            if let Some(model) = optional_observed_model(event)? {
                record_serving_model(&mut next, model);
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
            let mut details = optional_details(event)?;
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
                if let Some(refusal) = existing.details.get("guard_refusal")
                    && let Some(current) = details.as_object_mut()
                {
                    current.insert("guard_refusal".to_owned(), refusal.clone());
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
            let terminal = native_turn_terminal(event, now)?;
            // A harness-observed MCP inventory rides the terminal the same
            // way an observed model rides its message: merged as the only
            // change into the effective set, so a proven readback surfaces
            // while an unobserved one stays unknown.
            if let Some((servers, verified)) = optional_observed_mcp_servers(event)? {
                match &mut next.effective {
                    Some(Value::Object(effective)) => {
                        effective.insert("mcp_servers".to_owned(), servers);
                        effective.insert("mcp_servers_verified".to_owned(), Value::Bool(verified));
                    }
                    None => {
                        let mut effective = serde_json::Map::new();
                        effective.insert("mcp_servers".to_owned(), servers);
                        effective.insert("mcp_servers_verified".to_owned(), Value::Bool(verified));
                        next.effective = Some(Value::Object(effective));
                    }
                    Some(_) => {}
                }
            }
            next.state = AgentState::Idle;
            next.active_turn = None;
            next.pending.clear();
            effects.push(terminal);
        }
        "turn.unknown" => {
            // Live-host projection loss with failed view recovery: settle only
            // the active turn unknown. The host stays alive for stop/readback
            // and owner resolution, but the uncertain turn still fences the
            // next turn: a later send queues behind the unknown-turn gate
            // until the turn is explicitly resolved. Never a success or
            // failure outcome.
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
        "quota.observed" => {
            let observation: api::harness::QuotaObservation = serde_json::from_value(
                event
                    .data
                    .get("observation")
                    .cloned()
                    .ok_or_else(|| Error::new("invalid", "quota observation absent"))?,
            )
            .map_err(|_| Error::new("invalid", "quota observation schema invalid"))?;
            observation.validate()?;
        }
        "session.usage" => {
            // Session-scoped usage snapshot for this Agent generation: no
            // turn binding (occupancy and cumulative cost are not turn
            // expenditure) and no projection change beyond the snapshot
            // itself. Turn-scoped validation stays unchanged.
            let mut usage = optional_session_usage(event)?
                .ok_or_else(|| Error::new("invalid", "session.usage requires a usage object"))?;
            usage.validate()?;
            usage.retain_valid_context();
            if usage.observed_at.is_none() {
                usage.observed_at = event.ts.or(Some(now));
            }
            effects.push(Effect::SessionUsageUpdated { usage });
        }
        "session.context.compacted" => {
            if event.data.get("turn_id").is_some() {
                require_live_turn(agent, event)?;
            }
        }
        "tool.file.edited" => {
            require_live_turn(agent, event)?;
        }
        "hook.before_model" | "hook.after_tool" => {
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

fn record_serving_model(agent: &mut Agent, model: String) {
    if let Value::Object(effective) = agent
        .effective
        .get_or_insert_with(|| Value::Object(serde_json::Map::new()))
    {
        effective.insert("model".to_owned(), Value::String(model));
        effective.insert("model_verified".to_owned(), Value::Bool(true));
    }
}

/// An adapter's exact-turn `turn.completed` for a turn the daemon's recovery
/// already settled `unknown` in this live generation. The terminal is
/// evidence that resolves that turn (docs/spec/api.md, turn resolution): it
/// returns the same turn-terminal effect as the live terminal, while the
/// projection stays unchanged because the turn left it at settlement. The
/// Store proves the stored recovery settlement and the exact send binding
/// in the same transaction before using this.
pub fn resolve_recovered_turn(agent: &Agent, event: &NativeEvent, now: i64) -> Result<Effect> {
    if agent.observe_only {
        return Err(Error::new(
            "not_allowed",
            "observer metadata/history cannot reduce controlled native state",
        ));
    }
    if agent.state == AgentState::Unknown || agent.is_terminal() {
        return Err(bad_state(event, "turn resolution requires a live Agent"));
    }
    if event.kind != "turn.completed" {
        return Err(bad_state(
            event,
            "only a native turn.completed resolves a recovered turn",
        ));
    }
    if agent.active_turn.as_deref() == Some(required_text(event, "turn_id")?.as_str()) {
        return Err(bad_state(event, "the active turn settles through apply"));
    }
    // Validated like the live terminal; session metadata is not merged.
    optional_observed_mcp_servers(event)?;
    native_turn_terminal(event, now)
}

/// The turn-terminal effect a native `turn.completed` carries.
fn native_turn_terminal(event: &NativeEvent, now: i64) -> Result<Effect> {
    Ok(Effect::TurnTerminal {
        command_id: Some(required_text(event, "command_id")?),
        turn_id: required_text(event, "turn_id")?,
        state: TurnState::Completed,
        status: Some(required_text(event, "status")?),
        outcome: optional_outcome(event)?,
        native_status: optional_native_status(event)?,
        final_text: optional_text(event, "final_text")?,
        usage: optional_usage(event)?,
        completed_at: now,
    })
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

/// Merge source for a harness-observed MCP inventory carried on a turn
/// terminal (Claude `system`/`init` via its plugin): an array of
/// `{name, status}` objects plus the verification flag. Absent together
/// means unobserved (no merge); a half-present or malformed pair is an
/// invalid event, never a silent partial readback.
fn optional_observed_mcp_servers(event: &NativeEvent) -> Result<Option<(Value, bool)>> {
    const MAX_MERGED_MCP_SERVERS: usize = 1024;
    const MAX_MERGED_MCP_TEXT_LEN: usize = 256;
    let servers = event.data.get("mcp_servers");
    let verified = event.data.get("mcp_servers_verified");
    match (servers, verified) {
        (None | Some(Value::Null), None | Some(Value::Null)) => Ok(None),
        (Some(Value::Array(entries)), Some(Value::Bool(verified))) => {
            if entries.len() > MAX_MERGED_MCP_SERVERS {
                return Err(Error::new(
                    "invalid",
                    format!("{} field mcp_servers exceeds its bound", event.kind),
                ));
            }
            for entry in entries {
                let valid = entry.as_object().is_some_and(|object| {
                    object
                        .get("name")
                        .and_then(Value::as_str)
                        .is_some_and(|name| {
                            !name.trim().is_empty() && name.len() <= MAX_MERGED_MCP_TEXT_LEN
                        })
                        && object
                            .get("status")
                            .and_then(Value::as_str)
                            .is_some_and(|status| {
                                !status.trim().is_empty() && status.len() <= MAX_MERGED_MCP_TEXT_LEN
                            })
                });
                if !valid {
                    return Err(Error::new(
                        "invalid",
                        format!(
                            "{} field mcp_servers must be name/status objects",
                            event.kind
                        ),
                    ));
                }
            }
            Ok(Some((Value::Array(entries.clone()), *verified)))
        }
        _ => Err(Error::new(
            "invalid",
            format!(
                "{} fields mcp_servers and mcp_servers_verified must be a server array and a boolean together",
                event.kind
            ),
        )),
    }
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
