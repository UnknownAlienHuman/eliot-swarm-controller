//! Public Agent/daemon views and pure reply binding checks.
//! Store authorization and the exact workspace target remain Router decisions.

use super::{
    GitView, HeavySummaryView, ObservationView, ScriptInterpreter, SessionGrowthView,
    StatsCoverage, WorkSummaryView,
};
use crate::{
    Agent, AgentEdit, AgentState, CommandState, Error, NativeChildActivity,
    NativeChildActivityState, ProcessIdentity, ProcessRecord, ProcessState, ProviderHoldState,
    ProviderRestrictionKind, Result, SessionUsage, StderrSnapshot, TurnOutcome,
    TurnResolutionSource, TurnUsage,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

pub const PROCESS_CURSOR_MAX_BYTES: usize = 2_048;
/// Most characters of [`AgentSummary::preview`].
pub const AGENT_PREVIEW_CHARS: usize = 256;
/// Maximum native notice text returned in an Agent projection.
pub const AGENT_NOTICE_MESSAGE_CHARS: usize = 256;

/// A native retry/reconnect/error fact, classified by its adapter.
#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeNoticeKind {
    Retrying,
    Reconnecting,
    Error,
}

/// The normalized `turn.notice` event data. Its turn binding is checked by
/// the reducer against the current generation before this fact is committed.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeNotice {
    pub turn_id: String,
    pub kind: NativeNoticeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_kind: Option<ProviderRestrictionKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default)]
    pub truncated: bool,
}

impl NativeNotice {
    pub fn validate(&self) -> Result<()> {
        if self.turn_id.trim().is_empty()
            || self.turn_id.chars().any(char::is_control)
            || self.message.as_ref().is_some_and(|message| {
                message.trim().is_empty() || message.chars().count() > 4 * 1024
            })
        {
            return Err(Error::new("invalid", "native notice is invalid"));
        }
        Ok(())
    }
}

/// Provider-hold facts safe to include on one Agent row. Scope and probe
/// command stay on the owner-only provider-hold surface.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentProviderHold {
    pub kind: ProviderRestrictionKind,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nullable"
    )]
    pub retry_after: Option<i64>,
    pub state: ProviderHoldState,
    pub updated_at: i64,
    /// Whether this hold is the current wait reason for the Agent's active
    /// turn. Store clears this when native progress is newer than the hold.
    #[serde(default)]
    pub waiting_for_active_turn: bool,
    /// Next automatic minimal probe time when no provider reset is known.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nullable"
    )]
    pub next_probe_at: Option<i64>,
    /// Timestamp when the current uninterrupted hold began.
    #[serde(default)]
    pub held_since: i64,
}

impl AgentProviderHold {
    pub fn validate(&self) -> Result<()> {
        if self.retry_after.is_some_and(|at| at < 0)
            || self.next_probe_at.is_some_and(|at| at < 0)
            || (self.retry_after.is_some() && self.next_probe_at.is_some())
            || self.held_since < 0
            || self.held_since > self.updated_at
            || self.updated_at < 0
            || (self.waiting_for_active_turn && self.state != ProviderHoldState::Probing)
        {
            return Err(Error::new("invalid", "Agent provider hold is invalid"));
        }
        Ok(())
    }
}

/// A current or most recent native notice, bound to the event generation and
/// exact turn from which its adapter translated the native fact.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentNativeNotice {
    pub generation: i64,
    pub turn_id: String,
    pub kind: NativeNoticeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_kind: Option<ProviderRestrictionKind>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nullable"
    )]
    pub message: Option<String>,
    pub truncated: bool,
    pub observed_at: i64,
}

impl AgentNativeNotice {
    pub fn validate(&self) -> Result<()> {
        if self.generation < 0
            || self.turn_id.trim().is_empty()
            || self.turn_id.chars().any(char::is_control)
            || self.message.as_ref().is_some_and(|message| {
                message.trim().is_empty() || message.chars().count() > AGENT_NOTICE_MESSAGE_CHARS
            })
            || self.observed_at < 0
        {
            return Err(Error::new("invalid", "Agent native notice is invalid"));
        }
        Ok(())
    }
}

/// Shared owner-facing words for an Agent's provider hold or native notice.
pub fn agent_status_line(
    active_turn: Option<&str>,
    provider_hold: Option<&AgentProviderHold>,
    native_notice: Option<&AgentNativeNotice>,
    now_ms: i64,
) -> Option<String> {
    let provider_hold = provider_hold.filter(|hold| {
        active_turn.is_none()
            || hold.state == ProviderHoldState::Held
            || hold.waiting_for_active_turn
    });
    let provider_kind = provider_hold
        .map(|hold| hold.kind)
        .or_else(|| native_notice.and_then(|notice| notice.provider_kind));
    if let Some(provider_kind) = provider_kind {
        let kind = match provider_kind {
            ProviderRestrictionKind::RateLimit => "rate limited",
            ProviderRestrictionKind::Quota => "quota exhausted",
            ProviderRestrictionKind::Auth => "authentication required",
            ProviderRestrictionKind::ModelRemoved => "model unavailable",
        };
        let waiting_for_provider = active_turn.is_some()
            && (matches!(
                native_notice.map(|notice| notice.kind),
                Some(NativeNoticeKind::Retrying | NativeNoticeKind::Reconnecting)
            ) || (native_notice.is_none()
                && provider_hold.is_some_and(|hold| hold.waiting_for_active_turn)));
        let mut line = if waiting_for_provider {
            format!("Waiting for the provider: {kind}")
        } else if provider_hold.is_some() {
            format!("Provider hold: {kind}")
        } else {
            format!("Provider error: {kind}")
        };
        if let Some(hold) = provider_hold {
            line.push_str(&format!(
                ", held {}",
                elapsed_time(now_ms.saturating_sub(hold.held_since))
            ));
        }
        match native_notice.map(|notice| notice.kind) {
            Some(NativeNoticeKind::Retrying) => line.push_str(", retrying"),
            Some(NativeNoticeKind::Reconnecting) => line.push_str(", reconnecting"),
            Some(NativeNoticeKind::Error) => {}
            None if waiting_for_provider => line.push_str(", retrying"),
            None if provider_hold.is_some_and(|hold| hold.state == ProviderHoldState::Probing) => {
                line.push_str(", probe in progress");
            }
            None => {
                if let Some(next_probe_at) = provider_hold.and_then(|hold| hold.next_probe_at) {
                    if next_probe_at > now_ms {
                        line.push_str(&format!(
                            ", probe in {}",
                            remaining_time(next_probe_at - now_ms)
                        ));
                    } else {
                        line.push_str(", probe due");
                    }
                } else if let Some(retry_after) = provider_hold
                    .and_then(|hold| hold.retry_after)
                    .filter(|at| *at > now_ms)
                {
                    line.push_str(&format!(
                        ", retry in {}",
                        remaining_time(retry_after - now_ms)
                    ));
                } else if provider_hold
                    .and_then(|hold| hold.retry_after)
                    .is_some_and(|retry_after| retry_after <= now_ms)
                {
                    line.push_str(", reset elapsed; release pending");
                }
            }
        }
        if let Some(message) = native_notice.and_then(|notice| notice.message.as_deref()) {
            line.push_str(": ");
            line.push_str(message);
        }
        return Some(line);
    }

    let notice = native_notice?;
    let lead = match notice.kind {
        NativeNoticeKind::Retrying => "Harness retrying",
        NativeNoticeKind::Reconnecting => "Harness reconnecting",
        NativeNoticeKind::Error => "Harness error",
    };
    Some(match notice.message.as_deref() {
        Some(message) => format!("{lead}: {message}"),
        None => lead.to_owned(),
    })
}

fn remaining_time(milliseconds: i64) -> String {
    let minutes = milliseconds
        .saturating_add(59_999)
        .div_euclid(60_000)
        .max(1);
    if minutes < 60 {
        format!("{minutes}m")
    } else {
        let hours = minutes.saturating_add(59).div_euclid(60);
        if hours < 24 {
            format!("{hours}h")
        } else {
            format!("{}d", hours.saturating_add(23).div_euclid(24))
        }
    }
}

fn elapsed_time(milliseconds: i64) -> String {
    let seconds = milliseconds.max(0).div_euclid(1_000);
    if seconds < 60 {
        return "just now".to_owned();
    }
    let minutes = seconds.div_euclid(60);
    if minutes < 60 {
        format!("{minutes}m")
    } else {
        let hours = minutes.div_euclid(60);
        if hours < 24 {
            format!("{hours}h")
        } else {
            format!("{}d", hours.div_euclid(24))
        }
    }
}

/// Fixed compact fleet projection for `agent.list`. Identity, state, current
/// turn and hierarchy position come from the live Agent row. `last_progress_at`
/// is the newest native progress timestamp where the Store retains one;
/// `progress_age_ms` is measured against the read clock and stays absent when
/// no progress timestamp exists. `last_outcome` is the newest terminal turn's
/// outcome with its `last_turn_id`, `last_resolution_source`, and retained
/// `usage` where available. Success requires native evidence.
/// When the newest terminal turn carries no native turn usage, `usage` falls
/// back to the generation's latest retained `session.usage` snapshot
/// (cumulative normalized counters) with
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
    /// The recorded registration ID, or a legacy/unregistered project label.
    pub project: String,
    /// Repository identity retained by a managed worktree, including after removal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_common_dir: Option<String>,
    /// The retained execution directory, independently of the project label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
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
    pub last_resolution_source: Option<TurnResolutionSource>,
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
    /// The authorization namespace the server compares for same-project
    /// moves and handoffs (`COALESCE(project_scope, project)`), present
    /// only when it differs from `project`. Trailing field; absent on old
    /// wire payloads, when the UI falls back to `project`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_scope: Option<String>,
    /// Heavy-work rollup for the Agent generation, when at least one heavy
    /// episode was ever recorded. Trailing field; absent on old wire payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heavy: Option<HeavySummaryView>,
    /// Live session-growth state for the Agent generation, present exactly
    /// while the retained signal crosses a configured threshold. Trailing
    /// field; absent on old wire payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_growth: Option<SessionGrowthView>,
    /// The `chat.organization` group this row reads in, from the same
    /// committed predicate as the projection: the execution project when
    /// it is a retained registration, a chat's placement link when that
    /// is one; absent for the no-project bucket. Placement only, never
    /// authority. Trailing field; absent on old wire payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization: Option<String>,
    /// Organization tag groups (`chat.tag`). Trailing field.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// The overlay row's revision the row's `organization` and `tags` were
    /// read with (`None` for no overlay row); a menu write carries it back
    /// as its expected revision. Trailing field; absent on old payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization_revision: Option<i64>,
    /// The model the harness last reported for this generation: the
    /// Agent's retained `effective.model`, as `agent.get` and
    /// `agent.result` read it back. Absent until the harness reports one.
    /// Trailing field; absent on old wire payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Bounded Git work and current-turn activity summary for the Agent
    /// generation. Trailing field; absent on old wire payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work: Option<WorkSummaryView>,
    /// The hold for the frozen account/profile scope, if any. Trailing field;
    /// absent on old wire payloads.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nullable"
    )]
    pub provider_hold: Option<AgentProviderHold>,
    /// Latest native notice bound to this generation and its active/last turn.
    /// Trailing field; absent on old wire payloads.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nullable"
    )]
    pub native_notice: Option<AgentNativeNotice>,
}

impl AgentSummary {
    pub fn validate(&self) -> Result<()> {
        text(&self.id).map_err(|_| {
            invalid_response_detail("agent.id", None, "must be nonblank bounded text")
        })?;
        crate::parse_agent_id(&self.id)
            .map_err(|_| invalid_response_detail("agent.id", None, "must be a valid Agent ID"))?;
        let agent_id = Some(self.id.as_str());
        if self.generation < 0 {
            return Err(invalid_response_detail(
                "agent.generation",
                agent_id,
                "must be nonnegative",
            ));
        }
        if self.chat {
            if !self.project.is_empty() {
                return Err(invalid_response_detail(
                    "agent.project",
                    agent_id,
                    "must be empty for a chat Agent",
                ));
            }
        } else {
            text(&self.project).map_err(|_| {
                invalid_response_detail("agent.project", agent_id, "must be nonblank bounded text")
            })?;
        }
        text(&self.harness).map_err(|_| {
            invalid_response_detail("agent.harness", agent_id, "must be nonblank text")
        })?;
        for (field, value) in [
            ("agent.name", &self.name),
            ("agent.profile", &self.profile),
            ("agent.preview", &self.preview),
            ("agent.repo_common_dir", &self.repo_common_dir),
            ("agent.model", &self.model),
        ] {
            if let Some(value) = value {
                text(value).map_err(|_| {
                    invalid_response_detail(field, agent_id, "must be nonblank bounded text")
                })?;
            }
        }
        // A switch that continues a projectless chat keeps the blank source
        // namespace on a project worker so the handoff stays in one
        // namespace; any other scope keeps the usual text check.
        if let Some(scope) = &self.project_scope
            && !scope.is_empty()
        {
            text(scope).map_err(|_| {
                invalid_response_detail(
                    "agent.project_scope",
                    agent_id,
                    "must be empty or nonblank bounded text",
                )
            })?;
        }
        if self
            .preview
            .as_ref()
            .is_some_and(|preview| preview.chars().count() > AGENT_PREVIEW_CHARS)
        {
            return Err(invalid_response_detail(
                "agent.preview",
                agent_id,
                "must not exceed AGENT_PREVIEW_CHARS",
            ));
        }
        text(&self.root).map_err(|_| {
            invalid_response_detail("agent.root", agent_id, "must be nonblank text")
        })?;
        crate::parse_agent_id(&self.root).map_err(|_| {
            invalid_response_detail("agent.root", agent_id, "must be a valid Agent ID")
        })?;
        if let Some(parent) = &self.parent {
            text(parent).map_err(|_| {
                invalid_response_detail(
                    "agent.parent",
                    agent_id,
                    "must be nonblank text when present",
                )
            })?;
            crate::parse_agent_id(parent).map_err(|_| {
                invalid_response_detail("agent.parent", agent_id, "must be a valid Agent ID")
            })?;
        }
        if let Some(turn) = &self.active_turn {
            text(turn).map_err(|_| {
                invalid_response_detail("agent.active_turn", agent_id, "must be nonblank text")
            })?;
        }
        if let Some(at) = self.last_progress_at
            && at < 0
        {
            return Err(invalid_response_detail(
                "agent.last_progress_at",
                agent_id,
                "must be nonnegative",
            ));
        }
        if let Some(age) = self.progress_age_ms
            && age < 0
        {
            return Err(invalid_response_detail(
                "agent.progress_age_ms",
                agent_id,
                "must be nonnegative",
            ));
        }
        if self.last_outcome.is_none() && self.last_turn_id.is_some() {
            return Err(invalid_response_detail(
                "agent.last_outcome",
                agent_id,
                "must be present when last_turn_id is present",
            ));
        }
        if let Some(turn) = &self.last_turn_id {
            text(turn).map_err(|_| {
                invalid_response_detail("agent.last_turn_id", agent_id, "must be nonblank text")
            })?;
        }
        match (&self.usage, &self.usage_scope) {
            (None, None) => {}
            (Some(_), Some(scope)) if scope == "turn" || scope == "session" => {}
            _ => {
                return Err(invalid_response_detail(
                    "agent.usage_scope",
                    agent_id,
                    "must be turn or session exactly when usage is present",
                ));
            }
        }
        if self.updated_at < 0 {
            return Err(invalid_response_detail(
                "agent.updated_at",
                agent_id,
                "must be nonnegative milliseconds",
            ));
        }
        if let Some(heavy) = &self.heavy {
            heavy.validate().map_err(|_| {
                invalid_response_detail(
                    "agent.heavy",
                    agent_id,
                    "must satisfy the heavy summary rules",
                )
            })?;
        }
        if let Some(growth) = &self.session_growth {
            growth.validate().map_err(|_| {
                invalid_response_detail(
                    "agent.session_growth",
                    agent_id,
                    "must satisfy the session growth rules",
                )
            })?;
        }
        if let Some(work) = &self.work {
            work.validate().map_err(|_| {
                invalid_response_detail(
                    "agent.work",
                    agent_id,
                    "must satisfy the work summary rules",
                )
            })?;
        }
        if let Some(hold) = &self.provider_hold {
            hold.validate()?;
        }
        if let Some(notice) = &self.native_notice {
            notice.validate()?;
            let expected_turn = self.active_turn.as_deref().or(self.last_turn_id.as_deref());
            if notice.generation != self.generation
                || expected_turn != Some(notice.turn_id.as_str())
            {
                return Err(Error::new(
                    "invalid",
                    "Agent summary native notice binding is invalid",
                ));
            }
        }
        if let Some(group) = &self.organization {
            super::organization::validate_project_name(group, "Agent summary organization")
                .map_err(|_| {
                    invalid_response_detail(
                        "agent.organization",
                        agent_id,
                        "must be a valid project name",
                    )
                })?;
        }
        super::organization::validate_tags(&self.tags).map_err(|_| {
            invalid_response_detail(
                "agent.tags",
                agent_id,
                "must satisfy the organization tag rules",
            )
        })?;
        if self
            .organization_revision
            .is_some_and(|revision| revision < 1)
        {
            return Err(invalid_response_detail(
                "agent.organization_revision",
                agent_id,
                "must be positive when present",
            ));
        }
        Ok(())
    }

    /// Fleet token count for this summary plus its provenance scope:
    /// `"turn"` for the newest terminal turn's native turn usage,
    /// `"session"` for the session-snapshot fallback. Every fleet surface
    /// sums the available uncached input, cached input and output.
    pub fn fleet_usage(&self) -> Option<(i64, &'static str)> {
        let usage = self.usage.as_ref()?;
        let tokens = usage.reported_total()?;
        let scope = match self.usage_scope.as_deref() {
            Some("session") => "session",
            _ => "turn",
        };
        Some((tokens, scope))
    }

    pub fn fleet_usage_is_partial(&self) -> bool {
        self.usage.as_ref().is_some_and(|usage| {
            usage.input_tokens.is_none()
                || usage.cache_read_tokens.is_none()
                || usage.output_tokens.is_none()
        })
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
        description = "Always \"panicking\": the daemon acknowledges the request before capturing the panic and aborting."
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
    /// Bounded Git work and current-turn activity summary for the Agent
    /// generation. Trailing field; absent on old wire payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work: Option<WorkSummaryView>,
    /// The hold for the frozen account/profile scope, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_hold: Option<AgentProviderHold>,
    /// Latest native notice bound to this generation and its active/last turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_notice: Option<AgentNativeNotice>,
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
    /// Bounded Git work and current-turn activity summary for the Agent
    /// generation. Trailing field; absent on old wire payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work: Option<WorkSummaryView>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentGitResponse {
    pub agent: String,
    pub generation: i64,
    #[serde(flatten, deserialize_with = "object")]
    pub git: GitView,
}

/// A short digest of an Agent's patch for an independent review: an
/// attention filter from a plain line scan, never a quality score.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentPatchDigestResponse {
    pub agent: String,
    pub generation: i64,
    /// The commit the patch is measured from: the managed worktree's base,
    /// else HEAD (uncommitted changes only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<String>,
    /// False when the patch exceeded the read bound; the lists then cover
    /// only the changed paths, not their lines.
    pub complete: bool,
    pub files: Vec<PatchDigestFile>,
    /// Nearest package-manifest directories of the changed files.
    pub packages: Vec<String>,
    /// Added dependency entries in Cargo, npm and lock manifests.
    pub new_dependencies: Vec<String>,
    /// Added (`+`) or removed (`-`) public item declarations.
    pub public_interface: Vec<String>,
    /// Changed checker, lint, toolchain and CI configuration.
    pub checker_changes: Vec<String>,
    /// Added error or lint suppressions.
    pub suppressions: Vec<String>,
    /// True when a list was cut at its bound.
    pub truncated: bool,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchDigestFile {
    pub path: String,
    /// `added`, `modified`, `deleted` or `untracked`.
    pub change: String,
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
    /// The repository's name from its `origin` remote URL (the last path
    /// segment without `.git`), read live from Git by `project.list` and
    /// `project.get`. Read-only: never stored or settable, never the URL
    /// itself (which may embed credentials); absent without an `origin`
    /// remote or when Git cannot read it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    /// Live checkout identities verified against this registration's common directory.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub checkouts: Vec<ProjectCheckoutView>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectCheckoutView {
    pub path: String,
    pub main: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

/// A bare repository name as [`ProjectRegistrationView::origin`] carries
/// it: nonempty, at most 255 characters, no path, user or port separators,
/// whitespace or control characters.
fn is_repository_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && name != "."
        && name != ".."
        && !name.chars().any(|character| {
            character.is_control()
                || character.is_whitespace()
                || matches!(character, '/' | '\\' | ':' | '@' | '?' | '#')
        })
}

/// The repository name of a Git remote URL: its last path segment without
/// a trailing `.git` (`https://host/org/atlas.git`, `git@host:org/atlas`,
/// a Windows path ending in `atlas.git` and `/srv/atlas/` all read `atlas`). Only the name
/// leaves: user info, host, query and fragment are dropped, so a URL that
/// embeds credentials never reaches a readback. `None` when no valid name
/// remains.
pub fn repository_name_from_url(url: &str) -> Option<String> {
    let url = url.strip_suffix('\n').unwrap_or(url);
    let url = url.strip_suffix('\r').unwrap_or(url);
    if url.chars().any(char::is_control) {
        return None;
    }
    let path = if let Some((scheme, after_scheme)) = url.split_once("://") {
        let end = after_scheme.find(['/', '?', '#'])?;
        let (authority, tail) = after_scheme.split_at(end);
        if !tail.starts_with('/')
            || (authority.is_empty() && !scheme.eq_ignore_ascii_case("file"))
            || authority.matches('@').count() > 1
            || tail.contains('@')
            || url.contains('\\')
            || url.chars().any(char::is_whitespace)
        {
            return None;
        }
        let parsed = url::Url::parse(url).ok()?;
        if parsed.cannot_be_a_base()
            || (parsed.host().is_none() && !scheme.eq_ignore_ascii_case("file"))
        {
            return None;
        }
        tail.split(['?', '#']).next()?
    } else {
        if url.contains(['?', '#']) {
            return None;
        }
        // A drive path is local. Otherwise a colon before any slash is the
        // SCP host separator (including a bracketed IPv6 host).
        let drive = url.as_bytes().get(1) == Some(&b':') && url.as_bytes()[0].is_ascii_alphabetic();
        let host_start = url.rsplit_once('@').map_or(url, |(_, host)| host);
        let colon = if host_start.starts_with('[') {
            host_start
                .find("]:")
                .map(|end| url.len() - host_start.len() + end + 1)
        } else {
            url.find(':')
        };
        match colon {
            _ if drive => &url[2..],
            Some(end) if !url[..end].contains(['/', '\\']) => {
                let (host, path) = (&url[..end], &url[end + 1..]);
                if host.is_empty()
                    || ["http", "https", "file", "ssh", "git", "ftp", "ftps"]
                        .iter()
                        .any(|scheme| host.eq_ignore_ascii_case(scheme))
                    || host.matches('@').count() > 1
                    || host.chars().any(char::is_whitespace)
                    || path.contains(['@', ':', '\\'])
                    || url::Url::parse(&format!("ssh://{host}/"))
                        .ok()?
                        .host()
                        .is_none()
                {
                    return None;
                }
                path
            }
            _ if url.contains('@') => return None,
            _ => url,
        }
    };
    let last = path
        .trim_end_matches(['/', '\\'])
        .rsplit(['/', '\\'])
        .next()?;
    let name = last.strip_suffix(".git").unwrap_or(last);
    (is_repository_name(name) && !name.contains('%')).then(|| name.to_owned())
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
        if let Some(origin) = &self.origin
            && !is_repository_name(origin)
        {
            return Err(Error::new(
                "invalid",
                "project origin must be a bare repository name",
            ));
        }
        let mut paths = BTreeSet::new();
        if self.checkouts.len() > 1000
            || self.checkouts.iter().filter(|entry| entry.main).count() > 1
        {
            return Err(invalid_response());
        }
        for checkout in &self.checkouts {
            text(&checkout.path)?;
            if checkout.path.len() > 4096
                || !paths.insert(&checkout.path)
                || checkout.branch.as_ref().is_some_and(|branch| {
                    branch.is_empty() || branch.len() > 1024 || branch.chars().any(char::is_control)
                })
            {
                return Err(invalid_response());
            }
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
    /// Live unsettled commands beyond the bounded blocker list. Drain keeps
    /// blocking while unlisted commands remain; the count is the exact
    /// unlisted remainder, so a fleet of only retained unknowns (excluded
    /// before the bound) never trips this fallback.
    CommandUnsettledMore {
        count: usize,
    },
    /// A live script Run worker with no unsettled command row of its own
    /// (the row already settled while the worker finishes). Drain waits for
    /// the worker instead of exiting early.
    ScriptRunActive {
        run_id: String,
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

/// Retention and store-size visibility for `daemon.status`. Day settings
/// remain available for compatibility; the seconds fields expose effective
/// event and child-process ages. `db_bytes` and
/// `wal_bytes` are the `eliot.db` main and WAL file sizes;
/// `pruned_through` is the retained-event watermark. `auto_vacuum_mode`,
/// `page_count`, and `freelist_pages` show whether deleted pages can shrink
/// the file now or be reused. `config_error` is the typed code and message
/// when `config.toml` could not be read: settings then read back absent while
/// the SQLite page inventory remains available.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionView {
    pub events_days: Option<i64>,
    pub commands_days: Option<i64>,
    pub mail_days: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub events_seconds: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processes_seconds: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maintenance_interval_seconds: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compact_on_start: Option<bool>,
    #[serde(default)]
    pub auto_vacuum_mode: i64,
    #[serde(default)]
    pub page_count: i64,
    #[serde(default)]
    pub freelist_pages: i64,
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
        for seconds in [
            self.events_seconds,
            self.processes_seconds,
            self.maintenance_interval_seconds,
        ] {
            if seconds.is_some_and(|value| value <= 0) {
                return Err(invalid_response());
            }
        }
        if self
            .maintenance_interval_seconds
            .is_some_and(|seconds| seconds > 86_400)
            || !(0..=2).contains(&self.auto_vacuum_mode)
            || self.page_count < 0
            || self.freelist_pages < 0
            || self.freelist_pages > self.page_count
            || self.pruned_through < 0
        {
            return Err(invalid_response());
        }
        let unset = self.events_days.is_none()
            && self.commands_days.is_none()
            && self.mail_days.is_none()
            && self.events_seconds.is_none()
            && self.processes_seconds.is_none();
        let known = match &self.config_error {
            None => true,
            Some(error)
                if !error.trim().is_empty()
                    && error.len() <= 512
                    && unset
                    && self.maintenance_interval_seconds.is_none()
                    && self.compact_on_start.is_none() =>
            {
                false
            }
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
pub struct AutomaticCleanupView {
    pub policy: AutomaticCleanupPolicy,
    pub enabled: bool,
    pub startup_plan_only_pending: bool,
    pub after: Option<String>,
    pub config_error: Option<String>,
    pub last_sweep: Option<AutomaticCleanupSweep>,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutomaticCleanupPolicy {
    Disabled,
    PlanOnly,
    Enabled,
}

#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutomaticCleanupSweep {
    pub finished_at: i64,
    pub enabled: bool,
    pub plan_only: bool,
    pub would_remove: usize,
    pub removed: usize,
    pub kept: usize,
    pub unknown: usize,
    pub error_code: Option<String>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonStatusResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_identity: Option<DaemonBuildIdentity>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_probe_attention: Vec<crate::ProviderHold>,
    pub state: String,
    pub admission: String,
    pub agents: usize,
    pub store_write_faulted: bool,
    pub owned_plugins: usize,
    pub observer_readers: usize,
    pub discovery_readers: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub claude_profile_command_issues: Vec<crate::api::harness::ClaudeProfileCommandIssue>,
    #[serde(deserialize_with = "object")]
    pub workspace_custody: WorkspaceCustodyView,
    #[serde(deserialize_with = "nullable")]
    pub workspace_recovery_error: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub cloud_mcp: Option<CloudMcpStatus>,
    #[serde(deserialize_with = "objects")]
    pub automation_tasks: Vec<WorkerTaskStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub automatic_cleanup: Option<AutomaticCleanupView>,
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
    /// Most recent validated plugin panic markers observed this daemon run.
    /// Marker files are consumed when they enter the bounded status view.
    #[serde(default, deserialize_with = "objects")]
    pub plugin_crashes: Vec<CrashMarker>,
    #[serde(default)]
    pub plugin_crashes_truncated: bool,
    /// The previous daemon run's crash, loaded once at startup from the
    /// private crash marker and consumed. Absent when the previous run did
    /// not panic. `swarm daemon` startup output reports the same marker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_crash: Option<CrashMarker>,
}

/// Compile-time release identity and the exact executable observed at daemon
/// startup. Missing values are reported as unavailable instead of inferred.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonBuildIdentity {
    pub version: String,
    #[serde(default)]
    pub executable_directory: Option<String>,
    #[serde(default)]
    pub git_sha: Option<String>,
    #[serde(default)]
    pub executable_sha256: Option<String>,
}

impl DaemonStatusResponse {
    pub const MAX_AUTOMATION_TASKS: usize = 11;
    pub const MAX_PLUGIN_CRASHES: usize = 8;
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
pub const MAX_DIAGNOSE_ATTENTION_STEPS: usize = 32;
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
        .or_else(|| {
            steps
                .iter()
                .find(|step| step.kind == "goal" && step.code.as_deref() == Some("blocked"))
        })
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attention_steps: Vec<AttentionStepReadback>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preceded_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_crash: Option<CrashMarker>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttentionStepReadback {
    pub generation: i64,
    pub at: i64,
    pub reaction: crate::AttentionStep,
}

impl DiagnoseResponse {
    pub fn validate(&self) -> Result<()> {
        if text(&self.target).is_err()
            || self.target.chars().count() > 256
            || self.summary.chars().count() > MAX_DIAGNOSE_SUMMARY_CHARS
            || self.summary.trim().is_empty()
            || self.steps.len() > MAX_DIAGNOSE_STEPS
            || self.steps.iter().any(|step| step.validate().is_err())
            || self.attention_steps.len() > MAX_DIAGNOSE_ATTENTION_STEPS
            || self
                .attention_steps
                .iter()
                .any(|step| step.generation < 1 || step.at < 0 || !step.reaction.is_bounded())
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

/// Bounded, redacted runtime status of one daemon-owned background worker.
/// `running` means the worker is alive after its startup boundary; it does
/// not prove a rule/timer/goal delivery. `starting` means spawned but not
/// yet initialized. `failed` carries only a bounded code, never a raw error
/// message. `restart_count` is the number of worker restarts in this daemon run.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerTaskStatus {
    pub name: String,
    pub health: WorkerTaskHealth,
    #[serde(deserialize_with = "nullable")]
    pub failure_code: Option<String>,
    #[serde(default)]
    pub restart_count: u32,
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

pub(super) fn invalid_response_detail(field: &str, agent_id: Option<&str>, rule: &str) -> Error {
    let agent = agent_id
        .map(|id| format!(" for Agent {id}"))
        .unwrap_or_default();
    Error::new(
        "unknown",
        format!("Agent view response is invalid: {field}{agent}: {rule}"),
    )
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
