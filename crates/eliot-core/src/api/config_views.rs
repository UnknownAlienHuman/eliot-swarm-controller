//! Owner configuration reads and the profile-section save, plus the
//! automation rule list.
//!
//! `profile.list` is a sanitized summary for launch pickers: names, harness,
//! model/effort, authority and route names, never values of secrets or
//! environment. `settings.read` and `settings.save` are the owner paths that
//! carry `[profiles.*]` source, alongside the other settings sections; a save
//! is accepted only after the daemon's own parser and launch resolution accept
//! the result, guarded by the file revision the editor started from.
//! `rule.list` pages the
//! stored rule states with their canonical definitions and timer states.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{RuleStateView, TimerStateView};
use crate::{Error, Result};

/// Lowercase hexadecimal SHA-256 of the `config.toml` bytes.
pub const CONFIG_REVISION_HEX_LEN: usize = 64;
/// Upper bound on the profile source text (the whole file is 128 KiB).
pub const MAX_PROFILES_SOURCE_BYTES: usize = 128 * 1024;
/// Profiles listed at most (the config loader's own bound).
pub const MAX_LISTED_PROFILES: usize = 256;
/// Marker the daemon's secret policy writes; never accepted back.
pub const REDACTION_MARKER: &str = "[REDACTED]";

fn validate_revision(revision: &Option<String>) -> Result<()> {
    if let Some(revision) = revision
        && (revision.len() != CONFIG_REVISION_HEX_LEN
            || !revision
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    {
        return Err(Error::new(
            "invalid",
            "config revision must be 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

/// Request for `profile.list`. Empty object.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileListRequest {}

impl ProfileListRequest {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }

    pub fn validate_response(&self, reply: &ProfileListResponse) -> Result<()> {
        validate_revision(&reply.revision)?;
        if reply.profiles.len() > MAX_LISTED_PROFILES {
            return Err(Error::new("invalid", "profile list exceeds 256 entries"));
        }
        if !reply.valid && !reply.profiles.is_empty() {
            return Err(Error::new(
                "invalid",
                "an invalid configuration file cannot list profiles",
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for profile in &reply.profiles {
            if profile.name.trim().is_empty() || !seen.insert(profile.name.as_str()) {
                return Err(Error::new(
                    "invalid",
                    "profile list names are blank or repeated",
                ));
            }
            if profile.launchable == profile.error.is_some() {
                return Err(Error::new(
                    "invalid",
                    "a profile is launchable exactly when it carries no error",
                ));
            }
        }
        Ok(())
    }
}

/// Sanitized summary of one configured profile. Route and credential
/// selectors appear by name only; no environment or secret value is read.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileSummary {
    pub name: String,
    pub harness: String,
    /// Requested model; absent means the harness native default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Requested effort; absent means the native default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permissions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control: Option<String>,
    /// Selected proxy name, if any (direct route otherwise).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy: Option<String>,
    /// Selected account name, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_live: Option<u64>,
    #[serde(default)]
    pub child_profiles: Vec<String>,
    /// True when launch-time resolution of this profile succeeds now.
    pub launchable: bool,
    /// The resolution error when not launchable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Response for `profile.list`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileListResponse {
    /// Revision of the file read; absent when `config.toml` does not exist.
    #[serde(default)]
    pub revision: Option<String>,
    /// False when the file does not parse; launches then fail until fixed.
    pub valid: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub profiles: Vec<ProfileSummary>,
}

/// A complete, allowlisted section of the unified settings document.
#[derive(
    schemars::JsonSchema,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum SettingsSection {
    Profiles,
    Routing,
    Projects,
    Daemon,
    Automation,
    Mail,
    CloudMcp,
    Desktop,
    Accounts,
    Proxies,
}

/// Which owner-facing client initiated a settings save.
#[derive(
    schemars::JsonSchema, Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum SettingsClientSource {
    #[default]
    Cli,
    Desktop,
}

/// Metadata-only retained settings history; no settings values are stored here.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettingsHistoryEntry {
    pub revision: i64,
    pub previous_revision: Option<String>,
    pub current_revision: String,
    pub changed_sections: Vec<SettingsSection>,
    pub author: String,
    pub created_at: i64,
    pub status: SettingsHistoryStatus,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettingsHistoryStatus {
    Pending,
    Applied,
    NotApplied,
    Unknown,
}

/// Request for `settings.read`. Omitted sections reads every implemented section.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettingsReadRequest {
    #[serde(default)]
    pub sections: Option<Vec<SettingsSection>>,
}

impl SettingsReadRequest {
    pub fn validate(&self) -> Result<()> {
        if self.sections.as_ref().is_some_and(|sections| {
            sections.is_empty() || sections.len() > 16 || {
                let unique: std::collections::BTreeSet<_> = sections.iter().collect();
                unique.len() != sections.len()
            }
        }) {
            return Err(Error::new(
                "invalid",
                "settings sections are empty, duplicated, or oversized",
            ));
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &SettingsReadResponse) -> Result<()> {
        validate_revision(&reply.revision)?;
        if reply.history.len() > 100
            || reply.sections.values().any(|value| !value.is_object())
            || self.sections.as_ref().is_some_and(|sections| {
                reply.sections.len() != sections.len()
                    || sections
                        .iter()
                        .any(|section| !reply.sections.contains_key(section))
            })
        {
            return Err(Error::new(
                "invalid",
                "settings read response does not match its request",
            ));
        }
        Ok(())
    }
}

/// Response for `settings.read`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettingsReadResponse {
    pub revision: Option<String>,
    pub sections: std::collections::BTreeMap<SettingsSection, serde_json::Value>,
    pub restart_required: Vec<SettingsSection>,
    pub history: Vec<SettingsHistoryEntry>,
}

/// Request for `settings.save`. Each supplied section is a complete replacement.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettingsSaveRequest {
    #[serde(default)]
    pub expected_revision: Option<String>,
    pub sections: std::collections::BTreeMap<SettingsSection, serde_json::Value>,
    #[serde(default)]
    pub dry_run: bool,
    pub client: SettingsClientSource,
}

impl SettingsSaveRequest {
    pub fn validate(&self) -> Result<()> {
        validate_revision(&self.expected_revision)?;
        if self.sections.is_empty() || self.sections.len() > 16 {
            return Err(Error::new(
                "invalid",
                "settings save must replace one or more bounded sections",
            ));
        }
        let encoded = serde_json::to_vec(&self.sections)
            .map_err(|_| Error::new("invalid", "settings sections are not serializable"))?;
        if encoded.len() > MAX_PROFILES_SOURCE_BYTES {
            return Err(Error::new("invalid", "settings sections exceed 128 KiB"));
        }
        for value in self.sections.values() {
            if !value.is_object() {
                return Err(Error::new(
                    "invalid",
                    "each settings section must be an object",
                ));
            }
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &SettingsSaveResponse) -> Result<()> {
        validate_revision(&reply.revision)?;
        if (self.dry_run && reply.saved)
            || (reply.saved && (reply.revision.is_none() || reply.history_revision.is_none()))
            || (!reply.saved && reply.history_revision.is_some())
            || (reply.saved && reply.changed_sections.is_empty())
            || reply
                .changed_sections
                .iter()
                .any(|section| !self.sections.contains_key(section))
        {
            return Err(Error::new(
                "invalid",
                "settings save response is inconsistent",
            ));
        }
        Ok(())
    }
}

/// Response for `settings.save`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettingsSaveResponse {
    pub saved: bool,
    pub revision: Option<String>,
    pub changed_sections: Vec<SettingsSection>,
    pub restart_required: Vec<SettingsSection>,
    pub history_revision: Option<i64>,
}

/// Request for `routing.preview`: the decision the next launch of `role`
/// would record. An omitted role is `default`, as for `launch`.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingPreviewRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

impl RoutingPreviewRequest {
    /// Role syntax only; the daemon owns the configured roles.
    pub fn validate(&self) -> Result<()> {
        if let Some(role) = &self.role
            && (role.is_empty()
                || role.len() > 128
                || !role.chars().all(|character| {
                    character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
                }))
        {
            return Err(Error::new(
                "invalid",
                "routing role must be 1..128 letters, digits, hyphens or underscores",
            ));
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &RoutingPreviewResponse) -> Result<()> {
        self.validate()?;
        let role = self.role.as_deref().unwrap_or("default");
        if reply.decision.role != role || reply.decision.skipped.len() > 64 {
            return Err(Error::new(
                "invalid",
                "routing preview does not match its request",
            ));
        }
        Ok(())
    }
}

/// Response for `routing.preview`: advisory only. Nothing is admitted,
/// frozen or reserved; admission evaluates the candidates again.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingPreviewResponse {
    pub decision: crate::RoutingDecision,
}

/// Request for `rule.list`: one page of a rule namespace.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleListRequest {
    /// Registered project id; absent lists global rules.
    #[serde(default)]
    pub project: Option<String>,
    /// Last rule id of the previous page.
    #[serde(default)]
    pub after: Option<String>,
    #[serde(default = "super::default_limit")]
    pub limit: usize,
}

impl Default for RuleListRequest {
    fn default() -> Self {
        Self {
            project: None,
            after: None,
            limit: super::default_limit(),
        }
    }
}

impl RuleListRequest {
    pub fn validate(&self) -> Result<()> {
        if self.limit == 0 || self.limit > super::MAX_LIMIT {
            return Err(Error::new("invalid", "limit must be 1 through 1000"));
        }
        for value in [&self.project, &self.after].into_iter().flatten() {
            if value.trim().is_empty() {
                return Err(Error::new("invalid", "project and after must not be blank"));
            }
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &RuleListResponse) -> Result<()> {
        if reply.rules.len() > self.limit {
            return Err(Error::new("invalid", "rule page exceeds its limit"));
        }
        let mut previous: Option<&str> = self.after.as_deref();
        for entry in &reply.rules {
            entry.state.validate()?;
            if entry.state.project != self.project {
                return Err(Error::new("invalid", "rule page mixes namespaces"));
            }
            if previous.is_some_and(|previous| entry.state.id.as_str() <= previous) {
                return Err(Error::new("invalid", "rule page is not ordered by id"));
            }
            if let Some(timer) = &entry.timer
                && (timer.id != entry.state.id || timer.project != entry.state.project)
            {
                return Err(Error::new("invalid", "timer state names another rule"));
            }
            if !entry.definition.is_object() {
                return Err(Error::new("invalid", "rule definition must be an object"));
            }
            previous = Some(entry.state.id.as_str());
        }
        match (&reply.next, reply.rules.last()) {
            (Some(next), Some(last)) if *next == last.state.id => Ok(()),
            (None, _) => Ok(()),
            _ => Err(Error::new(
                "invalid",
                "rule page cursor must be its last id",
            )),
        }
    }
}

/// One stored rule: state, its timer state when it is a timer, and the
/// canonical definition (`on`, `do`, `limit`, `catch_up`).
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RuleListEntry {
    pub state: RuleStateView,
    /// Whether the rule belongs to the reconciled source snapshot.
    #[serde(default = "rule_source_present")]
    pub source_present: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timer: Option<TimerStateView>,
    pub definition: Value,
}

fn rule_source_present() -> bool {
    true
}

/// Response for `rule.list`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RuleListResponse {
    pub rules: Vec<RuleListEntry>,
    /// Last id of every nonempty page; continue until an empty page.
    #[serde(default)]
    pub next: Option<String>,
}
