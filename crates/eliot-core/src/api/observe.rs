//! Heavy-work, session-growth and harness-doctor readbacks. The daemon owns
//! detection and commit; these DTOs only shape the fleet and per-Agent views.

use super::native_profile::{CodexCollaborationMode, CodexPermissions};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Default fleet window for heavy work per Agent, in seconds.
pub const DEFAULT_OBSERVE_WINDOW_SECS: u64 = 3600;
/// Longest fleet window, in seconds (seven days).
pub const MAX_OBSERVE_WINDOW_SECS: u64 = 7 * 24 * 3600;
/// Most episodes returned in one fleet response.
pub const MAX_HEAVY_EPISODES: usize = 500;
/// Most harnesses reported by one doctor response.
pub const MAX_DOCTOR_HARNESSES: usize = 16;
/// Longest retained category or basis text on the wire.
pub const MAX_OBSERVE_TEXT_BYTES: usize = 128;
/// Longest retained growth/drift message on the wire.
pub const MAX_OBSERVE_MESSAGE_BYTES: usize = 1024;

fn text(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > MAX_OBSERVE_TEXT_BYTES {
        return Err(Error::new(
            "invalid",
            format!("observe {field} must be a bounded nonempty string"),
        ));
    }
    Ok(())
}

fn message_text(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > MAX_OBSERVE_MESSAGE_BYTES {
        return Err(Error::new(
            "invalid",
            format!("observe {field} message must be bounded and nonempty"),
        ));
    }
    Ok(())
}

/// Heavy-work rollup for one Agent generation: episode count, latest
/// category and total CPU. Absent (`None` on the parent) when no heavy
/// episode was ever recorded.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeavySummaryView {
    pub episodes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_ended_at: Option<i64>,
    pub total_cpu_ms: u64,
}

impl HeavySummaryView {
    pub fn validate(&self) -> Result<()> {
        if let Some(category) = &self.latest_category {
            text(category, "heavy category")?;
        }
        if let Some(ended) = self.latest_ended_at
            && ended < 0
        {
            return Err(Error::new("invalid", "observe heavy end time is invalid"));
        }
        if self.episodes == 0 && self.latest_category.is_some() {
            return Err(Error::new(
                "invalid",
                "observe heavy latest needs its episode count",
            ));
        }
        Ok(())
    }
}

/// Session-growth state for one Agent generation. `warn` is true exactly
/// while the retained signal crosses a configured threshold; basis and
/// message name the crossing.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionGrowthView {
    pub warn: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basis: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl SessionGrowthView {
    pub fn validate(&self) -> Result<()> {
        match (&self.basis, &self.message) {
            (Some(basis), Some(message)) if self.warn => {
                text(basis, "growth basis")?;
                message_text(message, "growth")?;
                Ok(())
            }
            (None, None) if !self.warn => Ok(()),
            _ => Err(Error::new(
                "invalid",
                "observe growth warning needs its basis and message together",
            )),
        }
    }
}

/// One closed heavy episode as recorded in the journal.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeavyEpisodeView {
    pub agent: String,
    pub generation: i64,
    pub category: String,
    pub category_source: String,
    pub started_at: i64,
    pub ended_at: i64,
    pub end_source: String,
    pub duration_ms: i64,
    pub cpu_ms: i64,
    pub peak_rss_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

impl HeavyEpisodeView {
    /// Strict parse of our own `activity.heavy` payload next to its binding.
    pub fn from_record(agent: &str, generation: i64, data: &Value) -> Result<Self> {
        let get_i64 = |field: &str| {
            data.get(field).and_then(Value::as_i64).ok_or_else(|| {
                Error::new("invalid", format!("observe heavy episode lacks {field}"))
            })
        };
        let get_u64 = |field: &str| {
            data.get(field).and_then(Value::as_u64).ok_or_else(|| {
                Error::new("invalid", format!("observe heavy episode lacks {field}"))
            })
        };
        let get_text = |field: &str| {
            data.get(field)
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    Error::new("invalid", format!("observe heavy episode lacks {field}"))
                })
                .map(str::to_owned)
        };
        let view = Self {
            agent: agent.to_owned(),
            generation,
            category: get_text("category")?,
            category_source: get_text("category_source")?,
            started_at: get_i64("started_at")?,
            ended_at: get_i64("ended_at")?,
            end_source: get_text("end_source")?,
            duration_ms: get_i64("duration_ms")?,
            cpu_ms: get_i64("cpu_ms")?,
            peak_rss_bytes: get_u64("peak_rss_bytes")?,
            exit_code: data
                .get("exit_code")
                .and_then(Value::as_i64)
                .and_then(|code| i32::try_from(code).ok()),
        };
        view.validate()?;
        Ok(view)
    }

    pub fn validate(&self) -> Result<()> {
        crate::parse_agent_id(&self.agent)?;
        if self.generation < 0 {
            return Err(Error::new("invalid", "observe heavy generation is invalid"));
        }
        text(&self.category, "heavy category")?;
        text(&self.category_source, "heavy category source")?;
        text(&self.end_source, "heavy end source")?;
        if self.ended_at < self.started_at || self.duration_ms < 0 || self.cpu_ms < 0 {
            return Err(Error::new("invalid", "observe heavy times are invalid"));
        }
        Ok(())
    }
}

/// Heavy work for one Agent over a window: newest episodes first plus totals.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeavyAgentTotals {
    pub agent: String,
    #[serde(deserialize_with = "events")]
    pub episodes: Vec<HeavyEpisodeView>,
    pub total_duration_ms: u64,
    pub total_cpu_ms: u64,
    pub peak_rss_bytes: u64,
}

impl HeavyAgentTotals {
    pub fn validate(&self) -> Result<()> {
        crate::parse_agent_id(&self.agent)?;
        if self.episodes.len() > MAX_HEAVY_EPISODES {
            return Err(Error::new(
                "invalid",
                "observe heavy page exceeds its bound",
            ));
        }
        for episode in &self.episodes {
            episode.validate()?;
            if episode.agent != self.agent {
                return Err(Error::new(
                    "invalid",
                    "observe heavy episode escapes its agent",
                ));
            }
        }
        Ok(())
    }
}

fn events<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<HeavyEpisodeView>, D::Error> {
    let raw = Vec::<Value>::deserialize(deserializer)?;
    if raw.len() > MAX_HEAVY_EPISODES {
        return Err(serde::de::Error::custom(
            "observe heavy page exceeds its bound",
        ));
    }
    raw.into_iter()
        .map(|value| {
            if !value.is_object() {
                return Err(serde::de::Error::custom(
                    "observe heavy episode must be an object",
                ));
            }
            let view: HeavyEpisodeView =
                serde_json::from_value(value).map_err(serde::de::Error::custom)?;
            view.validate().map_err(serde::de::Error::custom)?;
            Ok(view)
        })
        .collect()
}

/// Fleet heavy-work readback over a window.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserveHeavyRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_secs: Option<u64>,
}

impl ObserveHeavyRequest {
    pub fn effective_window_secs(&self) -> u64 {
        self.window_secs.unwrap_or(DEFAULT_OBSERVE_WINDOW_SECS)
    }

    pub fn validate(&self) -> Result<()> {
        if let Some(agent) = &self.agent {
            crate::parse_agent_id(agent)?;
        }
        if self.generation.is_some_and(|generation| generation < 0) {
            return Err(Error::new("invalid", "observe heavy generation is invalid"));
        }
        if self.generation.is_some() && self.agent.is_none() {
            return Err(Error::new(
                "invalid",
                "observe heavy generation needs its agent",
            ));
        }
        if self.effective_window_secs() == 0
            || self.effective_window_secs() > MAX_OBSERVE_WINDOW_SECS
        {
            return Err(Error::new(
                "invalid",
                "observe heavy window is out of range",
            ));
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &ObserveHeavyResponse) -> Result<()> {
        if self.validate().is_err() {
            return Err(Error::new("invalid", "observe heavy request is invalid"));
        }
        if reply.window_secs != self.effective_window_secs() {
            return Err(Error::new(
                "invalid",
                "observe heavy window was not honored",
            ));
        }
        let mut count = 0;
        for totals in &reply.agents {
            totals.validate()?;
            if let Some(agent) = &self.agent
                && totals.agent != *agent
            {
                return Err(Error::new(
                    "invalid",
                    "observe heavy agent escapes its filter",
                ));
            }
            if let Some(generation) = self.generation
                && totals
                    .episodes
                    .iter()
                    .any(|episode| episode.generation != generation)
            {
                return Err(Error::new(
                    "invalid",
                    "observe heavy generation escapes its filter",
                ));
            }
            count += totals.episodes.len();
        }
        if count > MAX_HEAVY_EPISODES {
            return Err(Error::new(
                "invalid",
                "observe heavy page exceeds its bound",
            ));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObserveHeavyResponse {
    pub window_secs: u64,
    pub since: i64,
    #[serde(deserialize_with = "totals")]
    pub agents: Vec<HeavyAgentTotals>,
    pub truncated: bool,
}

fn totals<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<HeavyAgentTotals>, D::Error> {
    let raw = Vec::<Value>::deserialize(deserializer)?;
    if raw.len() > 1000 {
        return Err(serde::de::Error::custom(
            "observe heavy fleet exceeds its bound",
        ));
    }
    raw.into_iter()
        .map(|value| {
            if !value.is_object() {
                return Err(serde::de::Error::custom(
                    "observe heavy totals must be an object",
                ));
            }
            let totals: HeavyAgentTotals =
                serde_json::from_value(value).map_err(serde::de::Error::custom)?;
            totals.validate().map_err(serde::de::Error::custom)?;
            Ok(totals)
        })
        .collect()
}

/// One harness installation fact with its verified-against record.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DoctorHarnessEntry {
    pub harness: String,
    pub installed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_version: Option<String>,
    pub verified_version: String,
    pub verified_marker: String,
    pub drift: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drift_message: Option<String>,
    pub doc_section: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract_mismatch: Option<String>,
}

/// An explicit profile command whose executable is absent.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileCommandIssue {
    pub profile: String,
    pub harness: String,
    pub configured_cmd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replacement: Option<crate::api::harness::HarnessCandidate>,
}

impl ProfileCommandIssue {
    pub fn validate(&self) -> Result<()> {
        let valid = |value: &str, max: usize| {
            !value.trim().is_empty() && value.len() <= max && !value.chars().any(char::is_control)
        };
        if !valid(&self.profile, 128)
            || !super::is_supported_harness(&self.harness)
            || !valid(&self.configured_cmd, 4096)
            || self.replacement.as_ref().is_some_and(|replacement| {
                !valid(&replacement.cmd, 4096)
                    || replacement
                        .version
                        .as_deref()
                        .is_some_and(|version| !valid(version, 256))
            })
        {
            return Err(Error::new(
                "invalid",
                "doctor profile command issue is invalid",
            ));
        }
        Ok(())
    }

    pub fn validate_list(issues: &[Self]) -> Result<()> {
        if issues.len() > super::MAX_LISTED_PROFILES {
            return Err(Error::new(
                "invalid",
                "doctor profile command issues are oversized",
            ));
        }
        let mut profiles = std::collections::HashSet::new();
        for issue in issues {
            issue.validate()?;
            if !profiles.insert(issue.profile.as_str()) {
                return Err(Error::new(
                    "invalid",
                    "doctor profile command issues repeat",
                ));
            }
        }
        Ok(())
    }
}

impl DoctorHarnessEntry {
    pub fn validate(&self) -> Result<()> {
        text(&self.harness, "doctor harness")?;
        text(&self.verified_version, "doctor verified version")?;
        text(&self.verified_marker, "doctor verified marker")?;
        text(&self.doc_section, "doctor doc section")?;
        if let Some(version) = &self.version {
            text(version, "doctor version")?;
        }
        if let Some(manifest) = &self.manifest_version {
            text(manifest, "doctor manifest version")?;
        }
        match (&self.drift, &self.drift_message) {
            (true, Some(message)) => message_text(message, "drift")?,
            (false, None) => {}
            _ => {
                return Err(Error::new(
                    "invalid",
                    "doctor drift needs its message exactly when drifting",
                ));
            }
        }
        if let Some(mismatch) = &self.contract_mismatch {
            message_text(mismatch, "contract mismatch")?;
        }
        Ok(())
    }
}

/// Request for `harness.doctor`. Empty object: no parameters.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessDoctorRequest {}

impl HarnessDoctorRequest {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }

    pub fn validate_response(&self, reply: &HarnessDoctorResponse) -> Result<()> {
        if reply.harnesses.is_empty() || reply.harnesses.len() > MAX_DOCTOR_HARNESSES {
            return Err(Error::new(
                "invalid",
                "doctor response is empty or oversized",
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for entry in &reply.harnesses {
            entry.validate()?;
            if !seen.insert(entry.harness.as_str()) {
                return Err(Error::new("invalid", "doctor harnesses repeat"));
            }
        }
        ProfileCommandIssue::validate_list(&reply.profile_command_issues)?;
        let mut files = std::collections::HashSet::new();
        if reply.owner_rules_drift.len() > MAX_DOCTOR_HARNESSES
            || (reply.owner_rules_check_unavailable && !reply.owner_rules_drift.is_empty())
            || reply.owner_rules_drift.iter().any(|file| {
                !super::is_supported_harness(&file.harness)
                    || file.path.trim().is_empty()
                    || file.path.chars().any(char::is_control)
                    || !files.insert((file.harness.as_str(), file.path.as_str()))
            })
        {
            return Err(Error::new("invalid", "doctor owner rules check is invalid"));
        }
        let mut profiles = std::collections::HashSet::new();
        let mut sampled_turns = 0_usize;
        for sample in &reply.profile_first_calls {
            sampled_turns = sampled_turns.saturating_add(sample.sampled_turns);
            if sample.profile.trim().is_empty()
                || sample.harness.trim().is_empty()
                || sample.sampled_turns == 0
                || sample.average_input_tokens < 0
                || !profiles.insert((
                    sample.profile.as_str(),
                    sample.harness.as_str(),
                    sample.mcp_policy.as_deref(),
                ))
            {
                return Err(Error::new("invalid", "doctor context sample is invalid"));
            }
        }
        if sampled_turns > super::usage::PROFILE_FIRST_CALL_WINDOW {
            return Err(Error::new("invalid", "doctor context sample is oversized"));
        }
        if reply.goal_check_deadlines.len() > super::goals::MAX_GOAL_CHECK_DEADLINE_STATUS {
            return Err(Error::new("invalid", "doctor check history is oversized"));
        }
        if let Some(attention) = &reply.attention_thresholds {
            attention.validate()?;
        }
        Ok(())
    }
}

/// Read-only per-harness installation, version and contract report.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessDoctorResponse {
    pub harnesses: Vec<DoctorHarnessEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub profile_command_issues: Vec<ProfileCommandIssue>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub profile_first_calls: Vec<super::usage::ProfileFirstCallInput>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub profile_first_calls_truncated: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub goal_check_deadlines: Vec<super::goals::GoalCheckDeadlineStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attention_thresholds: Option<super::monitor_views::AttentionThresholdsResponse>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub owner_rules_drift: Vec<OwnerRulesGlobalFile>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub owner_rules_check_unavailable: bool,
}

/// Metadata-only comparison of a harness's global instruction file with the
/// current Swarm owner rules.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerRulesGlobalFile {
    pub harness: String,
    pub path: String,
    pub status: OwnerRulesGlobalFileStatus,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnerRulesGlobalFileStatus {
    Match,
    Different,
    Missing,
    Unreadable,
}

/// Bounded per-Agent work summary covering Git progress versus frozen `base_oid`
/// (commits ahead, changed files, insertions and deletions across committed and
/// dirty work, last write and commit times) and current-turn tool/message counts.
/// When Git or caps prevent observation, `available` is `false` with `reason`
/// and all Git counters stay `None` — never a fake zero.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkSummaryView {
    pub available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commits_ahead: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_changed: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub insertions: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletions: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_write_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_commit_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messages: Option<u64>,
}

impl WorkSummaryView {
    pub fn unavailable(reason: impl Into<String>, turn_counts: Option<(u64, u64)>) -> Self {
        let (tool_calls, messages) = match turn_counts {
            Some((tools, msgs)) => (Some(tools), Some(msgs)),
            None => (None, None),
        };
        Self {
            available: false,
            reason: Some(reason.into()),
            commits_ahead: None,
            files_changed: None,
            insertions: None,
            deletions: None,
            last_write_at: None,
            last_commit_at: None,
            tool_calls,
            messages,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.available {
            if self.reason.is_some() {
                return Err(Error::new(
                    "invalid",
                    "available work summary must not carry an unavailable reason",
                ));
            }
            if self.commits_ahead.is_none()
                || self.files_changed.is_none()
                || self.insertions.is_none()
                || self.deletions.is_none()
            {
                return Err(Error::new(
                    "invalid",
                    "available work summary requires Git counters",
                ));
            }
        } else {
            let Some(reason) = &self.reason else {
                return Err(Error::new(
                    "invalid",
                    "unavailable work summary requires a reason",
                ));
            };
            text(reason, "work reason")?;
            if self.commits_ahead.is_some()
                || self.files_changed.is_some()
                || self.insertions.is_some()
                || self.deletions.is_some()
                || self.last_write_at.is_some()
                || self.last_commit_at.is_some()
            {
                return Err(Error::new(
                    "invalid",
                    "unavailable work summary must not report Git counters",
                ));
            }
        }
        if self.last_write_at.is_some_and(|ts| ts < 0)
            || self.last_commit_at.is_some_and(|ts| ts < 0)
        {
            return Err(Error::new("invalid", "work summary timestamp is invalid"));
        }
        if self.tool_calls.is_some() != self.messages.is_some() {
            return Err(Error::new(
                "invalid",
                "work summary turn activity requires both tool_calls and messages",
            ));
        }
        Ok(())
    }
}

/// Whether an Agent runs under a read-only or audit profile where no code
/// writes are expected (suppresses `no_code_progress` attention).
pub fn is_read_only_or_audit_profile(agent: &crate::Agent) -> bool {
    let snapshot_read_only = [&agent.profile_snapshot]
        .into_iter()
        .chain(agent.effective.as_ref())
        .any(|obj| {
            let text = |key: &str| obj.get(key).and_then(Value::as_str);
            text("permissions") == Some(CodexPermissions::ReadOnly.as_str())
                || text("permission_mode") == Some("plan")
                || text("mode") == Some("plan")
                || text("collaboration_mode") == Some(CodexCollaborationMode::Plan.as_str())
        });
    if snapshot_read_only {
        return true;
    }
    if agent
        .profile_choice
        .as_ref()
        .is_some_and(|choice| choice.role.eq_ignore_ascii_case("audit"))
    {
        return true;
    }
    let tokens: Vec<String> = agent
        .profile
        .split(['-', '_', ' '])
        .filter(|part| !part.is_empty())
        .map(str::to_ascii_lowercase)
        .collect();
    tokens
        .iter()
        .any(|token| token == "audit" || token == "readonly")
        || tokens
            .windows(2)
            .any(|pair| pair[0] == "read" && pair[1] == "only")
}
