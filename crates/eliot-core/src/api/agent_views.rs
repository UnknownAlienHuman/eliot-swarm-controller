//! Public Agent/daemon views and pure reply binding checks.
//! Store authorization and the exact workspace target remain Router decisions.

use super::{
    GitView, HeavySummaryView, ObservationView, ScriptInterpreter, SessionGrowthView, StatsCoverage,
};
use crate::{
    Agent, AgentEdit, AgentState, CommandState, Error, NativeChildActivity,
    NativeChildActivityState, ProcessIdentity, ProcessRecord, ProcessState, Result, SessionUsage,
    StderrSnapshot, TurnOutcome, TurnUsage,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

pub const PROCESS_CURSOR_MAX_BYTES: usize = 2_048;
/// Most characters of [`AgentSummary::preview`].
pub const AGENT_PREVIEW_CHARS: usize = 256;

/// Fixed compact fleet projection for `agent.list`. Identity, state, current
/// turn and hierarchy position come from the live Agent row. `last_progress_at`
/// is the newest native progress timestamp where the Store retains one;
/// `progress_age_ms` is measured against the read clock and stays absent when
/// no progress timestamp exists. `last_outcome` is the newest terminal turn's
/// canonical outcome (legacy rows classify explicitly, never defaulting to
/// success) with its `last_turn_id` and retained `usage` where available.
/// When the newest terminal turn carries no native turn usage, `usage` falls
/// back to the generation's latest retained `session.usage` snapshot
/// (cumulative input/output/cache counters only, never a derived total) with
/// `usage_scope` set to `"session"`; turn-backed usage carries `"turn"`.
/// A session fallback is a fleet-visibility signal, never turn expenditure:
/// the inner `usage.scope` is `"session"` and per-turn accounting stays on
/// `usage.summary`. `activity` is present only for native observed children.
/// `updated_at` is the last controller-observed write: for detached or
/// restarted sources it is freshness metadata, not proof of liveness. Full
/// records stay available through `agent.get`.
///
/// `name`, `profile` and `preview` let a list show what the Agent is for
/// without a read per row (the pattern of Codex app-server `thread/list`,
/// whose threads carry `name` and a first-message `preview`): the Agent's
/// name when one was set, its frozen profile name, and the first
/// [`AGENT_PREVIEW_CHARS`] characters of its oldest retained `send` text (a
/// launch's linked first input included). Each is absent when the Store has
/// none; the same scope that lists the row may already read that command.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentSummary {
    #[serde(default)]
    pub archived: bool,
    #[serde(default)]
    pub chat: bool,
    pub id: String,
    #[schemars(range(min = 0))]
    pub generation: i64,
    pub project: String,
    pub harness: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 256))]
    pub preview: Option<String>,
    pub state: AgentState,
    #[serde(deserialize_with = "nullable")]
    pub parent: Option<String>,
    pub root: String,
    #[serde(deserialize_with = "nullable")]
    pub active_turn: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub last_progress_at: Option<i64>,
    #[serde(deserialize_with = "nullable")]
    pub progress_age_ms: Option<i64>,
    #[serde(deserialize_with = "nullable")]
    pub last_turn_id: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub last_outcome: Option<TurnOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity: Option<NativeChildActivity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<TurnUsage>,
    /// Provenance of the fleet `usage` cell: `"turn"` for the newest
    /// terminal turn's native turn usage, `"session"` for the
    /// session-snapshot fallback. Absent exactly when `usage` is absent;
    /// never a native attribution scope on its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_scope: Option<String>,
    pub updated_at: i64,
    /// Heavy-work rollup for the Agent generation, when at least one heavy
    /// episode was ever recorded. Trailing field; absent on old wire payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heavy: Option<HeavySummaryView>,
    /// Live session-growth state for the Agent generation, present exactly
    /// while the retained signal crosses a configured threshold. Trailing
    /// field; absent on old wire payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_growth: Option<SessionGrowthView>,
}

impl AgentSummary {
    pub fn validate(&self) -> Result<()> {
        text(&self.id)?;
        crate::parse_agent_id(&self.id)?;
        if self.generation < 0 {
            return Err(Error::new("invalid", "Agent summary generation is invalid"));
        }
        if self.chat {
            if !self.project.is_empty() {
                return Err(invalid_response());
            }
        } else {
            text(&self.project)?;
        }
        text(&self.harness)?;
        for value in [&self.name, &self.profile, &self.preview]
            .into_iter()
            .flatten()
        {
            text(value)?;
        }
        if self
            .preview
            .as_ref()
            .is_some_and(|preview| preview.chars().count() > AGENT_PREVIEW_CHARS)
        {
            return Err(Error::new("invalid", "Agent summary preview is too long"));
        }
        text(&self.root)?;
        crate::parse_agent_id(&self.root)?;
        if let Some(parent) = &self.parent {
            text(parent)?;
            crate::parse_agent_id(parent)?;
        }
        if let Some(turn) = &self.active_turn {
            text(turn)?;
        }
        if let Some(at) = self.last_progress_at
            && at < 0
        {
            return Err(Error::new(
                "invalid",
                "Agent summary progress time is invalid",
            ));
        }
        if let Some(age) = self.progress_age_ms
            && age < 0
        {
            return Err(Error::new(
                "invalid",
                "Agent summary progress age is invalid",
            ));
        }
        if self.last_outcome.is_none() && self.last_turn_id.is_some() {
            return Err(Error::new(
                "invalid",
                "Agent summary turn needs its outcome",
            ));
        }
        if let Some(turn) = &self.last_turn_id {
            text(turn)?;
        }
        match (&self.usage, &self.usage_scope) {
            (None, None) => {}
            (Some(_), Some(scope)) if scope == "turn" || scope == "session" => {}
            _ => {
                return Err(Error::new(
                    "invalid",
                    "Agent summary usage needs its turn/session scope",
                ));
            }
        }
        if self.updated_at < 0 {
            return Err(Error::new(
                "invalid",
                "Agent summary observation time is invalid",
            ));
        }
        if let Some(heavy) = &self.heavy {
            heavy.validate()?;
        }
        if let Some(growth) = &self.session_growth {
            growth.validate()?;
        }
        Ok(())
    }

    /// Fleet token count for this summary plus its provenance scope:
    /// `"turn"` for the newest terminal turn's native turn usage,
    /// `"session"` for the session-snapshot fallback. The count is the
    /// native total when the harness supplies one, otherwise the
    /// input+output sum as a fleet signal (never a native total; cache
    /// counters cover the remainder when neither is present). `None` when
    /// the summary carries no usage. Every fleet surface (`swarm ls`,
    /// `swarm top`, desktop monitor) derives its cell from this one
    /// method so a session fallback can never pass as turn usage.
    pub fn fleet_usage(&self) -> Option<(i64, &'static str)> {
        let usage = self.usage.as_ref()?;
        let tokens =
            usage
                .total_tokens
                .or_else(|| match (usage.input_tokens, usage.output_tokens) {
                    (Some(input), Some(output)) => input.checked_add(output),
                    (Some(input), None) => Some(input),
                    (None, Some(output)) => Some(output),
                    (None, None) => usage
                        .cache_read_tokens
                        .unwrap_or(0)
                        .checked_add(usage.cache_write_tokens.unwrap_or(0))
                        .filter(|_| {
                            usage.cache_read_tokens.is_some() || usage.cache_write_tokens.is_some()
                        }),
                })?;
        let scope = match self.usage_scope.as_deref() {
            Some("session") => "session",
            _ => "turn",
        };
        Some((tokens, scope))
    }

    /// Human-readable fleet STATE cell. A native child that ended with its
    /// parent reads as its lifecycle state, not as its last activity.
    pub fn display_state(&self) -> String {
        match &self.activity {
            Some(_) if self.state == AgentState::Stopped => {
                "stopped (ended with parent)".to_owned()
            }
            Some(activity) => format!("{} (observed)", activity_state_name(activity.state)),
            None => agent_state_name(self.state).to_owned(),
        }
    }

    /// Fleet counting bucket for one summary. Per docs/spec/api.md:722-727, a lost observed
    /// activity counts as unknown; its display label still describes the loss.
    pub fn fleet_bucket(&self) -> &'static str {
        match &self.activity {
            Some(_) if self.state == AgentState::Stopped => "stopped",
            Some(activity) if activity.state == NativeChildActivityState::Lost => "unknown",
            Some(activity) => activity_state_name(activity.state),
            None => agent_state_name(self.state),
        }
    }
}

/// Lifecycle state name shared by fleet displays and counts.
pub fn agent_state_name(state: AgentState) -> &'static str {
    match state {
        AgentState::Unknown => "unknown",
        AgentState::Created => "created",
        AgentState::Starting => "starting",
        AgentState::Idle => "idle",
        AgentState::Busy => "busy",
        AgentState::Waiting => "waiting",
        AgentState::Stopped => "stopped",
        AgentState::Failed => "failed",
        AgentState::Lost => "lost",
    }
}

/// Read-only native observed-child activity name.
pub fn activity_state_name(state: NativeChildActivityState) -> &'static str {
    match state {
        NativeChildActivityState::Running => "running",
        NativeChildActivityState::Idle => "idle",
        NativeChildActivityState::Interrupted => "interrupted",
        NativeChildActivityState::Lost => "lost",
        NativeChildActivityState::Unknown => "unknown",
    }
}

/// Process state spelling shared by persisted observations and readbacks.
pub fn process_state_name(state: ProcessState) -> &'static str {
    match state {
        ProcessState::Running => "running",
        ProcessState::Exited => "exited",
        ProcessState::Unknown => "unknown",
    }
}

/// OS sample coverage spelling shared by observation readbacks.
pub fn stats_coverage_name(coverage: StatsCoverage) -> &'static str {
    match coverage {
        StatsCoverage::Unknown => "unknown",
        StatsCoverage::Stale => "stale",
        StatsCoverage::Complete => "complete",
        StatsCoverage::Partial => "partial",
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonPanicTestResponse {
    #[serde(deserialize_with = "required_text")]
    #[schemars(
        description = "Always \"panicking\": the hook acknowledged before the panic aborts the process."
    )]
    pub state: String,
}

impl DaemonPanicTestResponse {
    pub fn validate(&self) -> Result<()> {
        text(&self.state)
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentGetResponse {
    #[serde(flatten, deserialize_with = "object_agent")]
    pub agent: Agent,
    #[serde(deserialize_with = "object")]
    pub observation: ObservationView,
    #[serde(deserialize_with = "objects")]
    pub source_states: Vec<SourceState>,
    /// Latest retained `session.usage` snapshot for the Agent generation, in
    /// its own session-scope section; `None` when no session fact was ever
    /// committed. Never a turn total and never billing.
    #[serde(default, deserialize_with = "optional_object")]
    pub session_usage: Option<SessionUsage>,
    /// Current model/effort overlay for the Agent generation: the latest
    /// retained `configure` revision with the effective values the next
    /// native turn carries plus the original profile values. `None` when no
    /// `configure` command ever applied to this generation.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_object"
    )]
    pub config: Option<AgentConfigView>,
    /// Heavy-work rollup for the Agent generation, when at least one heavy
    /// episode was ever recorded. Trailing field; absent on old wire payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heavy: Option<HeavySummaryView>,
    /// Live session-growth state for the Agent generation, present exactly
    /// while the retained signal crosses a configured threshold. Trailing
    /// field; absent on old wire payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_growth: Option<SessionGrowthView>,
}

/// Retained model/effort overlay readback for one Agent generation. The
/// original profile snapshot is never rewritten: `original_model` and
/// `original_effort` repeat the frozen values while `model`/`effort` carry
/// the effective values the next native turn request uses. `profile` is the
/// retained requested profile name, `profile_inherited` reports whether the
/// Agent's snapshot was inherited from its parent launch, and `provenance`
/// is `configure` for a directly configured overlay or `inherited_from` for
/// a `resume` generation that explicitly inherited the previous generation.
///
/// The same view rides command and turn readbacks for their exact retained
/// generation and revision, so a retained turn still reads back the
/// configuration it ran under after later revisions apply.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentConfigView {
    /// Monotonically increasing revision within the Agent generation; 0 is
    /// never stored and means no overlay exists.
    pub revision: i64,
    /// Where the change applies: `next_turn`. A change needing a new
    /// native session is refused as `unsupported`, never stored.
    pub application: String,
    /// How this revision arose: `configure` or `inherited_from`.
    pub provenance: String,
    /// Retained requested profile name (the original launch request).
    pub profile: String,
    /// Whether the Agent snapshot was inherited from a parent launch.
    pub profile_inherited: bool,
    /// Exactly what the owner requested (each `None` when not requested).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_effort: Option<String>,
    /// Effective values the next native turn carries (requested value or
    /// the frozen original when not requested).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    /// Frozen profile-snapshot values the overlay leaves unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_effort: Option<String>,
}

impl AgentConfigView {
    /// Pure reply-shape check shared by the Agent, command and turn
    /// readbacks: a stored revision is always positive, the application
    /// point is `next_turn`, the provenance is one of the two retained
    /// origins, and the profile and requested values are nonblank.
    pub fn validate(&self) -> Result<()> {
        if self.revision < 1
            || self.application != "next_turn"
            || !matches!(self.provenance.as_str(), "configure" | "inherited_from")
            || self.profile.trim().is_empty()
            || self
                .requested_model
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
            || self
                .requested_effort
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
        {
            return Err(invalid_response());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentListRow {
    #[serde(flatten, deserialize_with = "object_summary")]
    pub summary: AgentSummary,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_object"
    )]
    pub observation: Option<ObservationView>,
}

impl AgentListRow {
    pub fn validate(&self) -> Result<()> {
        self.summary.validate()
    }
}

/// Persisted lifecycle states counted by the fleet projection (not activity buckets).
pub const AGENT_LIST_STATES: [AgentState; 9] = [
    AgentState::Unknown,
    AgentState::Created,
    AgentState::Starting,
    AgentState::Idle,
    AgentState::Busy,
    AgentState::Waiting,
    AgentState::Stopped,
    AgentState::Failed,
    AgentState::Lost,
];

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStateTotal {
    pub state: AgentState,
    pub total: usize,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentListResponse {
    /// Present for a multi-state request: all nine lifecycle totals in the same
    /// scope and non-state filters, ignoring both the state set and cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_totals: Option<Vec<AgentStateTotal>>,
    #[serde(deserialize_with = "objects")]
    pub agents: Vec<AgentListRow>,
    #[serde(deserialize_with = "nullable")]
    pub next: Option<String>,
    /// Matching records for the caller's scope and filters, ignoring the
    /// page cursor. `agents.len()` is the returned prefix of that set.
    /// Required on the wire: a missing count never reads back as zero.
    pub total: usize,
    /// True when the server knows records remain beyond this page: either a
    /// further row matched or the byte bound trimmed the page. A false value
    /// on a full page still requires continuing through `next` until the
    /// server returns an empty page. Required on the wire.
    pub truncated: bool,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentProcessesResponse {
    pub agent: String,
    pub generation: i64,
    #[serde(deserialize_with = "process_records")]
    pub processes: Vec<ProcessRecord>,
    #[serde(deserialize_with = "nullable")]
    pub next: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentStatsResponse {
    pub agent: String,
    pub generation: i64,
    #[serde(flatten, deserialize_with = "object")]
    pub observation: ObservationView,
    /// Latest retained `session.usage` snapshot for the Agent generation, in
    /// its own session-scope section; `None` when no session fact was ever
    /// committed. Never a turn total and never billing.
    #[serde(default, deserialize_with = "optional_object")]
    pub session_usage: Option<SessionUsage>,
    /// Heavy-work rollup for the Agent generation, when at least one heavy
    /// episode was ever recorded. Trailing field; absent on old wire payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heavy: Option<HeavySummaryView>,
    /// Live session-growth state for the Agent generation, present exactly
    /// while the retained signal crosses a configured threshold. Trailing
    /// field; absent on old wire payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_growth: Option<SessionGrowthView>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentGitResponse {
    pub agent: String,
    pub generation: i64,
    #[serde(flatten, deserialize_with = "object")]
    pub git: GitView,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEditsResponse {
    pub agent: String,
    pub generation: i64,
    #[serde(deserialize_with = "objects")]
    pub edits: Vec<AgentEdit>,
    #[serde(deserialize_with = "nullable")]
    pub next: Option<String>,
}

/// The exact public Store source projection, separate from private plugin ACKs.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceState {
    pub agent_id: String,
    pub generation: i64,
    pub source: String,
    pub families: Vec<String>,
    #[serde(deserialize_with = "objects")]
    pub owners: Vec<SourceOwner>,
    pub state: String,
    pub health: String,
    #[serde(deserialize_with = "nullable")]
    pub cursor: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub last_store_seq: Option<i64>,
    #[serde(deserialize_with = "nullable")]
    pub last_batch_id: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub last_batch_first_seq: Option<i64>,
    #[serde(deserialize_with = "nullable")]
    pub last_batch_last_seq: Option<i64>,
    #[serde(deserialize_with = "nullable")]
    pub gap_id: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub gap_from: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub gap_to: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub gap_seq: Option<i64>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceOwner {
    pub family: String,
    pub source: String,
    pub mechanism: String,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceCustodyView {
    pub scope: String,
    pub capability: String,
    pub exact: bool,
    pub orphan_possible: bool,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitCredentialHelper {
    Manager,
}

#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectGitView {
    #[serde(default)]
    pub fetch_remotes: BTreeMap<String, String>,
    #[serde(default)]
    pub credential_helper: Option<GitCredentialHelper>,
}

impl ProjectGitView {
    pub fn validate(&self) -> Result<()> {
        if self.fetch_remotes.iter().any(|(remote, endpoint)| {
            remote.is_empty()
                || remote.len() > 128
                || remote.starts_with('-')
                || remote
                    .chars()
                    .any(|character| character.is_whitespace() || character.is_control())
                || endpoint.trim().is_empty()
                || endpoint.len() > 4_096
                || endpoint.chars().any(char::is_control)
        }) {
            return Err(Error::new(
                "invalid",
                "project Git fetch remotes must be bounded nonempty literals",
            ));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectRegistrationView {
    pub id: String,
    pub checkout: String,
    pub repo_common_dir: String,
    pub profiles: Vec<String>,
    pub actions: Vec<String>,
    pub interpreters: BTreeMap<String, ScriptInterpreter>,
    pub run_env: Vec<String>,
    #[serde(default)]
    pub git: Option<ProjectGitView>,
    pub revision: i64,
}

/// Result of project discovery; `None` means the path is not a Git checkout.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectDiscoverResponse {
    #[serde(default)]
    pub project: Option<ProjectRegistrationView>,
}

impl ProjectRegistrationView {
    /// Pure readback grammar. Store/daemon own canonical repository identity and
    /// the authority checks for the explicit profile/action allowsets.
    pub fn validate(&self) -> Result<()> {
        text(&self.id)?;
        text(&self.checkout)?;
        text(&self.repo_common_dir)?;
        if self.revision < 0 {
            return Err(Error::new(
                "invalid",
                "project registration revision must be nonnegative",
            ));
        }
        validate_project_allowset(&self.profiles, "profiles")?;
        validate_project_allowset(&self.actions, "actions")?;
        validate_project_run_env(&self.run_env)?;
        if let Some(git) = &self.git {
            git.validate()?;
        }
        for (reference, interpreter) in &self.interpreters {
            if reference.is_empty()
                || reference.len() > 128
                || !reference
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || "-_".contains(character))
            {
                return Err(Error::new(
                    "invalid",
                    "project interpreter reference is invalid",
                ));
            }
            interpreter.validate()?;
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsettledCommandSummary {
    pub id: String,
    #[serde(deserialize_with = "nullable")]
    pub agent: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub generation: Option<i64>,
    pub verb: String,
    pub state: CommandState,
}

/// One running turn that blocks drain: the Agent's current active turn.
/// `agent`/`generation` identify the exact live generation; `turn_id` is the
/// retained turn the native side is still working on. A missing entry never
/// proves the native effect did not happen; it only means the Store has no
/// recorded active turn for that generation.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunningTurnSummary {
    #[serde(deserialize_with = "required_text")]
    pub agent: String,
    pub generation: i64,
    #[serde(deserialize_with = "required_text")]
    pub turn_id: String,
}

impl RunningTurnSummary {
    pub fn validate(&self) -> Result<()> {
        text(&self.agent)?;
        if self.generation < 0 {
            return Err(Error::new("invalid", "running turn generation is invalid"));
        }
        text(&self.turn_id)?;
        Ok(())
    }
}

/// Where a live owned process comes from. `plugin` is a controlled native
/// child for the named Agent generation; `workspace` is an owned workspace
/// setup/removal child for that generation. Both are custody the daemon
/// observes as not yet exited in the Store; neither proves task success. A
/// held workspace operation lock alone (no observed child) is never listed
/// here.
#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnedProcessKind {
    Plugin,
    Workspace,
}

/// One live owned process that blocks drain, bounded by the drain report cap.
/// An absent entry never proves no native effect occurred.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedProcessSummary {
    #[serde(deserialize_with = "required_text")]
    pub agent: String,
    pub generation: i64,
    pub kind: OwnedProcessKind,
}

impl OwnedProcessSummary {
    pub fn validate(&self) -> Result<()> {
        text(&self.agent)?;
        if self.generation < 0 {
            return Err(Error::new("invalid", "owned process generation is invalid"));
        }
        Ok(())
    }
}

/// One retained unknown command that no longer blocks drain. The stored
/// outcome stays `unknown`: only the drain wait is retired, because no live
/// process, turn or reader backs it. `reason` is the bounded stored outcome
/// reason (for example `observer_detach_fence`); `resolvable` reports
/// whether `cmd resolve` can retire it (false for observe-only commands,
/// which the Store refuses by design); `hint` is the bounded owner action.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedUnknownSummary {
    #[serde(deserialize_with = "required_text")]
    pub id: String,
    #[serde(deserialize_with = "required_text")]
    pub verb: String,
    #[serde(deserialize_with = "nullable")]
    pub agent: Option<String>,
    pub generation: Option<i64>,
    #[serde(deserialize_with = "required_text")]
    pub reason: String,
    pub resolvable: bool,
    #[serde(deserialize_with = "required_text")]
    pub hint: String,
}

impl RetainedUnknownSummary {
    pub fn validate(&self) -> Result<()> {
        text(&self.id)?;
        text(&self.verb)?;
        if let Some(agent) = &self.agent {
            text(agent)?;
        }
        generation(self.generation)?;
        if self.reason.trim().is_empty() || self.reason.len() > 128 {
            return Err(invalid_response());
        }
        if self.hint.trim().is_empty() || self.hint.len() > 256 {
            return Err(invalid_response());
        }
        Ok(())
    }
}

/// Bounded drain report reusing the existing shutdown blockers. Every
/// category carries a count, a bounded list and a truncation flag: the count
/// is the number returned in this snapshot (or the exact in-memory total for
/// unknown commands); `truncated` is true whenever any qualifying record was
/// omitted for any reason (the list bound or the Store byte bound trimming
/// the page). Unknown stays unknown: an empty report never proves no native
/// effect occurred, and the daemon never auto-resolves unknown to finish
/// drain. Retained unknowns with no live process, turn or reader are listed
/// under `retained_unresolved` instead of blocking: the receipt names each
/// one with its resolvability and owner action.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrainReport {
    pub running_turns_count: usize,
    #[serde(deserialize_with = "objects")]
    pub running_turns: Vec<RunningTurnSummary>,
    pub running_turns_truncated: bool,
    pub unsettled_commands_count: usize,
    #[serde(deserialize_with = "objects")]
    pub unsettled_commands: Vec<UnsettledCommandSummary>,
    pub unsettled_commands_truncated: bool,
    pub unknown_commands_count: usize,
    pub unknown_commands: Vec<String>,
    pub unknown_commands_truncated: bool,
    pub owned_processes_count: usize,
    #[serde(deserialize_with = "objects")]
    pub owned_processes: Vec<OwnedProcessSummary>,
    pub owned_processes_truncated: bool,
    pub retained_unresolved_count: usize,
    #[serde(deserialize_with = "objects")]
    pub retained_unresolved: Vec<RetainedUnknownSummary>,
    pub retained_unresolved_truncated: bool,
}

impl DrainReport {
    pub fn empty() -> Self {
        Self {
            running_turns_count: 0,
            running_turns: Vec::new(),
            running_turns_truncated: false,
            unsettled_commands_count: 0,
            unsettled_commands: Vec::new(),
            unsettled_commands_truncated: false,
            unknown_commands_count: 0,
            unknown_commands: Vec::new(),
            unknown_commands_truncated: false,
            owned_processes_count: 0,
            owned_processes: Vec::new(),
            owned_processes_truncated: false,
            retained_unresolved_count: 0,
            retained_unresolved: Vec::new(),
            retained_unresolved_truncated: false,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.running_turns_count < self.running_turns.len()
            || (!self.running_turns_truncated
                && self.running_turns_count != self.running_turns.len())
        {
            return Err(invalid_response());
        }
        if self.unsettled_commands_count < self.unsettled_commands.len()
            || (!self.unsettled_commands_truncated
                && self.unsettled_commands_count != self.unsettled_commands.len())
        {
            return Err(invalid_response());
        }
        if self.unknown_commands_count < self.unknown_commands.len()
            || (!self.unknown_commands_truncated
                && self.unknown_commands_count != self.unknown_commands.len())
        {
            return Err(invalid_response());
        }
        if self.owned_processes_count < self.owned_processes.len()
            || (!self.owned_processes_truncated
                && self.owned_processes_count != self.owned_processes.len())
        {
            return Err(invalid_response());
        }
        if self.retained_unresolved_count < self.retained_unresolved.len()
            || (!self.retained_unresolved_truncated
                && self.retained_unresolved_count != self.retained_unresolved.len())
        {
            return Err(invalid_response());
        }
        if self
            .running_turns
            .iter()
            .any(|turn| turn.validate().is_err())
            || self
                .unsettled_commands
                .iter()
                .any(|command| text(&command.id).is_err() || text(&command.verb).is_err())
            || self.unknown_commands.iter().any(|id| text(id).is_err())
            || self
                .owned_processes
                .iter()
                .any(|process| process.validate().is_err())
            || self
                .retained_unresolved
                .iter()
                .any(|unknown| unknown.validate().is_err())
        {
            return Err(invalid_response());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StatusBlocker {
    WorkspaceProcesses,
    DiscoveryReaders {
        count: usize,
    },
    AgentLive {
        agent: String,
        generation: i64,
        state: AgentState,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        active_turn: Option<String>,
    },
    CommandUnsettled {
        command_id: String,
        state: CommandState,
    },
    StoreWriteFaulted,
    CommandUnknown {
        command_id: String,
    },
    CommandUnknownMore {
        count: usize,
    },
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudMcpHealth {
    StartingHttp,
    StartingTunnel,
    Running,
    Stopping,
    Stopped,
    Failed,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudMcpCustody {
    WindowsJobExact,
    UnixProcessGroupDegraded,
    Unknown,
}

/// Current managed cloud MCP child-pair state. `running` proves the two child
/// leaders are alive; it does not prove public tunnel connectivity or auth.
/// The stderr tails are bounded, redacted supervisor-owned snapshots of the
/// managed HTTP child and cloudflared; they are present once the supervisor
/// records them, normally on a failed or finished run, and stay absent while
/// both children are still starting.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudMcpStatus {
    pub public_url: String,
    pub health: CloudMcpHealth,
    pub custody: CloudMcpCustody,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_stderr: Option<StderrSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tunnel_stderr: Option<StderrSnapshot>,
}

impl CloudMcpStatus {
    pub fn validate(&self) -> Result<()> {
        if text(&self.public_url).is_err()
            || !self.public_url.starts_with("https://")
            || !self.public_url.ends_with("/mcp")
        {
            return Err(invalid_response());
        }
        Ok(())
    }
}

/// Retention and store-size visibility for `daemon.status`. The windows are
/// the configured `daemon.retention` day counts; `None` is an unset window
/// that disables pruning for that side, and `unbounded` is true only when
/// all three are unset (a default daemon never prunes). `db_bytes` and
/// `wal_bytes` are the `eliot.db` main and WAL file sizes;
/// `pruned_through` is the retained-event watermark. `size_advisory` is
/// present only when retention is unbounded and the combined size passes the
/// documented threshold (see `configuration.md` daemon retention).
/// `config_error` is the typed code and message when `config.toml` could not
/// be read: the windows are then unknown, so they read back absent with
/// `unbounded` false and no advisory, never as an unset (unbounded) policy.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionView {
    pub events_days: Option<i64>,
    pub commands_days: Option<i64>,
    pub mail_days: Option<i64>,
    pub pruned_through: i64,
    pub db_bytes: u64,
    pub wal_bytes: u64,
    pub unbounded: bool,
    #[serde(deserialize_with = "nullable")]
    pub size_advisory: Option<String>,
    /// Trailing field; absent when the configuration was read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_error: Option<String>,
}

impl RetentionView {
    pub fn validate(&self) -> Result<()> {
        for window in [self.events_days, self.commands_days, self.mail_days] {
            if window.is_some_and(|days| days <= 0) {
                return Err(invalid_response());
            }
        }
        if self.pruned_through < 0 {
            return Err(invalid_response());
        }
        let unset =
            self.events_days.is_none() && self.commands_days.is_none() && self.mail_days.is_none();
        let known = match &self.config_error {
            None => true,
            Some(error) if !error.trim().is_empty() && error.len() <= 512 && unset => false,
            Some(_) => return Err(invalid_response()),
        };
        if self.unbounded != (known && unset) {
            return Err(invalid_response());
        }
        if let Some(advisory) = &self.size_advisory
            && (!self.unbounded || advisory.trim().is_empty() || advisory.len() > 512)
        {
            return Err(invalid_response());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonStatusResponse {
    pub state: String,
    pub admission: String,
    pub agents: usize,
    pub store_write_faulted: bool,
    pub owned_plugins: usize,
    pub observer_readers: usize,
    pub discovery_readers: usize,
    #[serde(deserialize_with = "object")]
    pub workspace_custody: WorkspaceCustodyView,
    #[serde(deserialize_with = "nullable")]
    pub workspace_recovery_error: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub cloud_mcp: Option<CloudMcpStatus>,
    #[serde(deserialize_with = "objects")]
    pub automation_tasks: Vec<WorkerTaskStatus>,
    pub unknown_commands_count: usize,
    pub unknown_commands: Vec<String>,
    pub unknown_commands_truncated: bool,
    pub unsettled_commands_count: usize,
    #[serde(deserialize_with = "objects")]
    pub unsettled_commands: Vec<UnsettledCommandSummary>,
    pub unsettled_commands_truncated: bool,
    #[serde(deserialize_with = "objects")]
    pub blockers: Vec<StatusBlocker>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain: Option<DrainReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<RetentionView>,
    /// The previous run's crash, loaded once at daemon startup from the
    /// private crash marker and consumed. Absent when the previous run did
    /// not panic. `swarm daemon` startup output reports the same marker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_crash: Option<CrashMarker>,
}

/// One captured process panic. Written by the panic hook (daemon and every
/// plugin binary) to the private crash directory as `daemon.panicked` or
/// `plugin.panicked`, with the thread, `file:line`, the redacted panic
/// message and a bounded backtrace. The daemon reports the previous run's
/// marker once: in `daemon.status`, in `swarm daemon` startup output, and in
/// `swarm why` when relevant. Bounded; the message is control-stripped at
/// capture and redacted with the daemon secrets at load.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrashMarker {
    #[serde(deserialize_with = "required_text")]
    pub code: String,
    pub thread: String,
    pub location: String,
    pub message: String,
    pub backtrace: String,
    pub timestamp: i64,
}

impl CrashMarker {
    pub const MAX_THREAD_CHARS: usize = 128;
    pub const MAX_LOCATION_CHARS: usize = 256;
    pub const MAX_MESSAGE_CHARS: usize = 1024;
    pub const MAX_BACKTRACE_CHARS: usize = 8192;

    pub fn validate(&self) -> Result<()> {
        if self.code != "daemon.panicked" && self.code != "plugin.panicked" {
            return Err(invalid_response());
        }
        if self.thread.chars().count() > Self::MAX_THREAD_CHARS
            || self.location.chars().count() > Self::MAX_LOCATION_CHARS
            || self.message.chars().count() > Self::MAX_MESSAGE_CHARS
            || self.backtrace.chars().count() > Self::MAX_BACKTRACE_CHARS
            || self.timestamp < 0
        {
            return Err(invalid_response());
        }
        Ok(())
    }

    /// One-line plain-language crash note for startup output and `swarm why`.
    pub fn summary(&self) -> String {
        let message = self.message.trim();
        let short = message.chars().take(160).collect::<String>();
        format!("{} at {}: {}", self.code, self.location, short)
    }
}

/// Owner-scoped read-only failure diagnosis: `swarm why <command-id | agent
/// | agent:turn>`. Assembles a bounded causal chain from existing events and
/// readbacks, with no new store of record: the command lifecycle with code,
/// message and `where`; the related turn outcome and native error; process
/// exits with codes; the last stderr lines (redacted); a previous-run crash
/// marker if relevant; and a one-line plain-language summary that starts
/// with the failure origin.
pub const MAX_DIAGNOSE_STEPS: usize = 16;
pub const MAX_DIAGNOSE_SUMMARY_CHARS: usize = 512;
pub const MAX_DIAGNOSE_MESSAGE_CHARS: usize = 1024;

/// One link of the causal chain: a command, turn, process exit, crash marker,
/// control command or stderr tail. String fields are bounded and redacted;
/// `origin` names the failure origin vocabulary.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnoseStep {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
    pub failure_where: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_evidence: Option<String>,
    /// Captured process exit for `process` steps: numeric code, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i64>,
    /// Windows NTSTATUS hex for crash statuses (for example `0xC0000005`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ntstatus: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<i64>,
}

/// The same most-informative reason for every diagnosis presentation. Failure
/// origin, terminal turn and process evidence take precedence over fallbacks.
pub fn diagnosis_reason(steps: &[DiagnoseStep]) -> Option<&DiagnoseStep> {
    steps
        .iter()
        .find(|step| {
            step.origin
                .as_deref()
                .is_some_and(|origin| origin != "unknown")
        })
        .or_else(|| {
            steps.iter().find(|step| {
                step.kind == "turn"
                    && matches!(
                        step.code.as_deref(),
                        Some("failure" | "interrupted" | "unknown")
                    )
            })
        })
        .or_else(|| steps.iter().find(|step| step.exit_code.is_some()))
        .or_else(|| steps.iter().find(|step| step.kind == "command"))
        .or_else(|| steps.iter().find(|step| step.kind == "turn"))
        .or(steps.first())
}

impl DiagnoseStep {
    pub fn validate(&self) -> Result<()> {
        if self.kind.chars().count() > 64
            || self
                .code
                .as_deref()
                .is_some_and(|code| code.chars().count() > 128)
            || self
                .message
                .as_deref()
                .is_some_and(|message| message.chars().count() > MAX_DIAGNOSE_MESSAGE_CHARS)
            || self
                .failure_where
                .as_deref()
                .is_some_and(|where_| where_.chars().count() > 128)
            || self
                .origin
                .as_deref()
                .is_some_and(|origin| origin.chars().count() > 32)
            || self
                .origin_evidence
                .as_deref()
                .is_some_and(|evidence| evidence.chars().count() > 512)
            || self
                .ntstatus
                .as_deref()
                .is_some_and(|ntstatus| ntstatus.chars().count() > 16)
            || self.at.is_some_and(|at| at < 0)
        {
            return Err(invalid_response());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnoseResponse {
    pub target: String,
    pub summary: String,
    pub steps: Vec<DiagnoseStep>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preceded_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_crash: Option<CrashMarker>,
}

impl DiagnoseResponse {
    pub fn validate(&self) -> Result<()> {
        if text(&self.target).is_err()
            || self.target.chars().count() > 256
            || self.summary.chars().count() > MAX_DIAGNOSE_SUMMARY_CHARS
            || self.summary.trim().is_empty()
            || self.steps.len() > MAX_DIAGNOSE_STEPS
            || self.steps.iter().any(|step| step.validate().is_err())
            || self
                .preceded_by
                .as_deref()
                .is_some_and(|id| text(id).is_err() || id.chars().count() > 64)
            || self
                .previous_crash
                .as_ref()
                .is_some_and(|crash| crash.validate().is_err())
        {
            return Err(invalid_response());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerTaskHealth {
    Starting,
    Running,
    Stopped,
    Failed,
}

/// Bounded, redacted runtime status of one daemon-owned automation task.
/// `running` means the task is alive after its startup boundary; it does
/// not prove a rule/timer/goal delivery. `starting` means spawned but not
/// yet initialized. `failed` carries only a bounded code, never a raw error
/// message.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerTaskStatus {
    pub name: String,
    pub health: WorkerTaskHealth,
    #[serde(deserialize_with = "nullable")]
    pub failure_code: Option<String>,
}

impl WorkerTaskStatus {
    pub fn validate(&self) -> Result<()> {
        if text(&self.name).is_err() {
            return Err(invalid_response());
        }
        if let Some(code) = self.failure_code.as_deref()
            && (code.is_empty() || code.len() > 128)
        {
            return Err(invalid_response());
        }
        if (self.health == WorkerTaskHealth::Failed) != self.failure_code.is_some() {
            return Err(invalid_response());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessPageCursor {
    pub agent: String,
    pub generation: i64,
    pub scope: String,
    #[serde(deserialize_with = "cursor_identity")]
    pub identity: ProcessIdentity,
}

impl ProcessPageCursor {
    pub fn decode(encoded: &str) -> Result<Self> {
        if encoded.len() > PROCESS_CURSOR_MAX_BYTES {
            return Err(Error::new("invalid", "process cursor exceeds its bound"));
        }
        let object: Map<String, Value> = serde_json::from_str(encoded)
            .map_err(|_| Error::new("invalid", "invalid process cursor"))?;
        let cursor: Self = serde_json::from_value(Value::Object(object))
            .map_err(|_| Error::new("invalid", "invalid process cursor"))?;
        cursor.validate()?;
        Ok(cursor)
    }

    pub fn encode(&self) -> Result<String> {
        self.validate()?;
        let encoded = serde_json::to_string(self)
            .map_err(|_| Error::new("internal", "cannot encode process cursor"))?;
        if encoded.len() > PROCESS_CURSOR_MAX_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "encoded process cursor exceeds its bound",
            ));
        }
        Ok(encoded)
    }

    pub fn validate_binding(&self, agent: &str, generation: i64, scope: &str) -> Result<()> {
        self.validate()?;
        if self.agent != agent || self.generation != generation || self.scope != scope {
            return Err(Error::new(
                "invalid",
                "process cursor does not match the Agent, generation, and scope",
            ));
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        text(&self.agent)?;
        generation(Some(self.generation))?;
        scope(&self.scope)?;
        if !valid_identity(&self.identity) {
            return Err(Error::new("invalid", "invalid process cursor identity"));
        }
        Ok(())
    }
}

pub(super) fn valid_identity(identity: &ProcessIdentity) -> bool {
    identity.pid != 0
        && identity.start_time != 0
        && !identity.host_boot_id.is_empty()
        && identity.host_boot_id.len() <= 512
        && !identity.host_boot_id.chars().any(char::is_control)
}

pub(super) fn text(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::new("invalid", "text must be non-empty"));
    }
    Ok(())
}

fn validate_project_allowset(values: &[String], field: &str) -> Result<()> {
    let mut unique = BTreeSet::new();
    if values
        .iter()
        .any(|value| text(value).is_err() || !unique.insert(value.as_str()))
    {
        return Err(Error::new(
            "invalid",
            format!("project {field} must contain unique nonempty values"),
        ));
    }
    Ok(())
}

fn validate_project_run_env(values: &[String]) -> Result<()> {
    let mut unique = BTreeSet::new();
    if values.iter().any(|value| {
        let mut characters = value.chars();
        let valid = characters
            .next()
            .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
            && characters.all(|character| character.is_ascii_alphanumeric() || character == '_');
        let key = if cfg!(windows) {
            value.to_ascii_uppercase()
        } else {
            value.clone()
        };
        !valid || !unique.insert(key)
    }) {
        return Err(Error::new(
            "invalid",
            "project run_env must contain unique environment names",
        ));
    }
    Ok(())
}

pub(super) fn generation(value: Option<i64>) -> Result<()> {
    if value.is_some_and(|value| value < 0) {
        return Err(Error::new("invalid", "generation must be nonnegative"));
    }
    Ok(())
}

pub(super) fn scope(value: &str) -> Result<()> {
    if !matches!(value, "agent" | "workspace") {
        return Err(Error::new(
            "invalid",
            "process scope must be agent or workspace",
        ));
    }
    Ok(())
}

pub(super) fn invalid_response() -> Error {
    Error::new("unknown", "Agent view response is invalid")
}

pub(super) fn required_text<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    let value = String::deserialize(deserializer)?;
    text(&value).map_err(serde::de::Error::custom)?;
    Ok(value)
}

// The same small object-map adapter used by the existing result-read DTOs.
// Reuse Core models instead of publishing a parallel response schema for them.
fn object<'de, D: serde::Deserializer<'de>, T: DeserializeOwned>(
    deserializer: D,
) -> std::result::Result<T, D::Error> {
    let value = Map::<String, Value>::deserialize(deserializer)?;
    serde_json::from_value(Value::Object(value)).map_err(serde::de::Error::custom)
}

fn objects<'de, D: serde::Deserializer<'de>, T: DeserializeOwned>(
    deserializer: D,
) -> std::result::Result<Vec<T>, D::Error> {
    let values = Vec::<Map<String, Value>>::deserialize(deserializer)?;
    values
        .into_iter()
        .map(|value| serde_json::from_value(Value::Object(value)).map_err(serde::de::Error::custom))
        .collect()
}

fn optional_object<'de, D: serde::Deserializer<'de>, T: DeserializeOwned>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error> {
    let value = Option::<Map<String, Value>>::deserialize(deserializer)?;
    value
        .map(|value| serde_json::from_value(Value::Object(value)).map_err(serde::de::Error::custom))
        .transpose()
}

fn nullable<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::<T>::deserialize(deserializer)
}

fn object_summary<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<AgentSummary, D::Error> {
    let value = Map::<String, Value>::deserialize(deserializer)?;
    for field in ["activity", "usage"] {
        if value.get(field).is_some_and(|entry| !entry.is_object()) {
            return Err(serde::de::Error::custom(
                "Agent summary objects must be objects",
            ));
        }
    }
    serde_json::from_value(Value::Object(value)).map_err(serde::de::Error::custom)
}

fn object_agent<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Agent, D::Error> {
    let value = Map::<String, Value>::deserialize(deserializer)?;
    if value.get("pending").is_some_and(|pending| {
        pending
            .as_array()
            .is_none_or(|entries| entries.iter().any(|entry| !entry.is_object()))
    }) {
        return Err(serde::de::Error::custom("pending entries must be objects"));
    }
    serde_json::from_value(Value::Object(value)).map_err(serde::de::Error::custom)
}

fn process_records<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<ProcessRecord>, D::Error> {
    let records = Vec::<Map<String, Value>>::deserialize(deserializer)?;
    records
        .into_iter()
        .map(|record| {
            if !record.get("identity").is_some_and(Value::is_object) {
                return Err(serde::de::Error::custom(
                    "process identity must be an object",
                ));
            }
            serde_json::from_value(Value::Object(record)).map_err(serde::de::Error::custom)
        })
        .collect()
}

fn cursor_identity<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<ProcessIdentity, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Identity {
        host_boot_id: String,
        pid: u32,
        start_time: u64,
    }
    let identity: Identity = object(deserializer)?;
    Ok(ProcessIdentity {
        host_boot_id: identity.host_boot_id,
        pid: identity.pid,
        start_time: identity.start_time,
    })
}

#[cfg(test)]
#[path = "agent_views_tests.rs"]
mod tests;
