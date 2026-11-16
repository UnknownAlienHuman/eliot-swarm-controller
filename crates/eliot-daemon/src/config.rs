//! Minimal global profile loader for the daemon data directory.
//!
//! Owning contract: docs/spec/configuration.md first profile and
//! docs/harnesses/codex.md first useful path. Only the single global
//! profiles map is implemented here. Later configuration layers (proxy
//! routes, extends, overlays, accounts, services, automation and project
//! overlays) are rejected with unsupported, never silently substituted.
//!
//! Returns a bounded nonsecret snapshot object with the keys harness, cmd,
//! model, effort, collaboration_mode, permissions, approval_policy, mcp,
//! mcp_allow, pass_env, raw setup and normalized relative scratch directories.
//! pass_env carries admitted environment variable names. Only the two nonsecret
//! native-store selector inputs are captured before admission; all other values
//! remain execution-time references and are never copied into the snapshot.
use eliot_core as core;
use eliot_plugin::session::split_provider_model;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
pub(crate) mod profiles_edit;
pub(crate) mod settings;

const MAX_CONFIG_BYTES: usize = 128 * 1024;
const MAX_PROFILE_NAME_LEN: usize = 128;
const MAX_PROFILES: usize = 256;
const MAX_PROJECT_PATH_LEN: usize = 4096;
const MAX_MODEL_LEN: usize = 256;
const MAX_EFFORT_LEN: usize = 128;
const MAX_AUTH_INTEGRATION_LEN: usize = 128;
const MAX_CMD_LEN: usize = 1024;
const MAX_ENV_NAME_LEN: usize = 256;
const MAX_PASS_ENV_ENTRIES: usize = 64;
const MAX_CHILD_PROFILES: usize = 64;
const APPROVAL_POLICY_NEVER: &str = "never";
const APPROVAL_POLICY_ON_REQUEST: &str = "on-request";
const PERMISSION_READ_ONLY: &str = "read-only";
const PERMISSION_WORKSPACE_WRITE: &str = "workspace-write";
const PERMISSION_DANGER_FULL_ACCESS: &str = "danger-full-access";

pub(crate) fn native_store_selector(name: &str) -> Option<&'static str> {
    ["CODEX_HOME", "CODEX_SQLITE_HOME"]
        .into_iter()
        .find(|selector| {
            if cfg!(windows) {
                name.eq_ignore_ascii_case(selector)
            } else {
                name == *selector
            }
        })
}

/// Called only for a newly resolved launch/attach profile, after retained-ID miss.
pub(crate) fn capture_native_store_inputs(
    profile: &mut Value,
    selector_lookup: impl FnMut(&str) -> Option<OsString>,
) -> core::Result<()> {
    capture_native_store_inputs_inner(profile, selector_lookup, |name| std::env::var_os(name))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectRegistrationConfig {
    pub id: String,
    pub path: String,
    pub profiles: Vec<String>,
    pub actions: Vec<String>,
    pub git: Option<core::api::ProjectGitView>,
    pub interpreters: BTreeMap<String, core::api::ScriptInterpreter>,
    pub run_env: Vec<String>,
}

#[cfg(test)]
pub(crate) fn capture_native_store_inputs_with_lookups(
    profile: &mut Value,
    selector_lookup: impl FnMut(&str) -> Option<OsString>,
    credential_lookup: impl FnMut(&str) -> Option<OsString>,
) -> core::Result<()> {
    capture_native_store_inputs_inner(profile, selector_lookup, credential_lookup)
}

fn capture_native_store_inputs_inner(
    profile: &mut Value,
    mut selector_lookup: impl FnMut(&str) -> Option<OsString>,
    mut credential_lookup: impl FnMut(&str) -> Option<OsString>,
) -> core::Result<()> {
    let names = profile
        .get("pass_env")
        .and_then(Value::as_array)
        .ok_or_else(|| core::Error::new("invalid", "resolved profile pass_env must be an array"))?;
    let mut values = serde_json::Map::new();
    for selector in ["CODEX_HOME", "CODEX_SQLITE_HOME"] {
        let selected = names
            .iter()
            .filter_map(Value::as_str)
            .find(|name| native_store_selector(name) == Some(selector));
        let value = selected
            .and_then(&mut selector_lookup)
            .map(|value| {
                value.into_string().map(Value::String).map_err(|_| {
                    core::Error::new("invalid", "native store selector is not valid Unicode")
                })
            })
            .transpose()?
            .unwrap_or(Value::Null);
        values.insert(selector.to_owned(), value);
    }
    let captured = Value::Object(values);
    let credentials = names
        .iter()
        .filter_map(Value::as_str)
        .filter(|name| native_store_selector(name).is_none())
        .filter_map(&mut credential_lookup)
        .filter_map(|value| value.into_string().ok())
        .collect::<Vec<_>>();
    // Keep resolved credentials only in this temporary policy; snapshot data
    // is checked against them but never receives their values.
    let policy = core::SecretPolicy::new(credentials.iter().map(String::as_str));
    let mut snapshot = profile.clone();
    snapshot["pass_env_values"] = captured;
    if policy.contains_value(&snapshot) {
        return Err(core::Error::new(
            "secret_in_payload",
            "frozen profile metadata contains a known credential",
        ));
    }
    *profile = snapshot;
    // The augmented snapshot is bounded by the existing Store admission/frame
    // checks. This capture proves inputs, never an effective native root/scope.
    Ok(())
}

/// Freeze an Agent's exact build output directory when the profile admits it
/// through `pass_env`. Only the literal `CARGO_TARGET_DIR` name is captured;
/// the value must be an absolute local path or it is skipped. Skipped means
/// absent: cleanup backfills only what is frozen here, and Agents may always
/// register their own target directory through `artifact.register`.
pub(crate) fn capture_build_inputs(
    profile: &mut Value,
    mut lookup: impl FnMut(&str) -> Option<OsString>,
) -> core::Result<()> {
    let names = profile
        .get("pass_env")
        .and_then(Value::as_array)
        .ok_or_else(|| core::Error::new("invalid", "resolved profile pass_env must be an array"))?;
    let admitted = names
        .iter()
        .any(|name| name.as_str() == Some("CARGO_TARGET_DIR"));
    if !admitted {
        return Ok(());
    }
    let Some(value) = lookup("CARGO_TARGET_DIR") else {
        return Ok(());
    };
    let Ok(value) = value.into_string() else {
        return Ok(());
    };
    if value.is_empty()
        || value.len() > 4096
        || value.contains('\0')
        || value.chars().any(char::is_control)
        || !std::path::Path::new(&value).is_absolute()
    {
        return Ok(());
    }
    profile
        .as_object_mut()
        .ok_or_else(|| core::Error::new("invalid", "resolved profile must be an object"))?
        .insert("target_dir".to_owned(), Value::String(value));
    Ok(())
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    #[serde(default)]
    profiles: HashMap<String, toml::Spanned<ProfileEntry>>,
    #[serde(default)]
    accounts: HashMap<String, toml::Spanned<AccountEntry>>,
    #[serde(default)]
    proxies: HashMap<String, toml::Spanned<ProxyEntry>>,
    #[serde(default)]
    projects: HashMap<String, toml::Spanned<ProjectEntry>>,
    #[serde(default)]
    automation: AutomationConfig,
    #[serde(default)]
    daemon: DaemonConfig,
    #[serde(default)]
    mail: MailConfig,
    #[serde(default)]
    cloud_mcp: Option<CloudMcpSettings>,
    #[serde(default)]
    routing: RoutingConfig,
    #[serde(default)]
    desktop: DesktopSettings,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
pub(crate) struct RoutingConfig {
    #[serde(default)]
    pub(crate) default: Vec<String>,
    /// Additional extensible roles share the same ordered candidate syntax.
    #[serde(flatten)]
    pub(crate) roles: BTreeMap<String, Vec<String>>,
}

impl RoutingConfig {
    pub(crate) fn candidates(&self, role: &str) -> Option<&[String]> {
        if role == "default" {
            Some(&self.default)
        } else {
            self.roles.get(role).map(Vec::as_slice)
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DesktopTheme {
    Light,
    Dark,
    #[default]
    System,
}

/// What the desktop window opens at start.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DesktopStartup {
    #[default]
    NewTask,
    LastConversation,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DesktopWindow {
    width: u32,
    height: u32,
}

/// One desktop model favorite or recent entry.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DesktopModelRef {
    #[serde(default)]
    harness: String,
    #[serde(default)]
    model: String,
    #[serde(default)]
    effort: Option<String>,
}

/// Bounded desktop preferences for the new-task picker: favorite models,
/// recent models (most recent first), collapsed projects and the last used
/// access level. Stored in the unified `[desktop]` section via the existing
/// settings writer.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DesktopSettings {
    #[serde(default)]
    theme: DesktopTheme,
    #[serde(default)]
    window: Option<DesktopWindow>,
    #[serde(default)]
    startup: DesktopStartup,
    #[serde(default)]
    favorite_models: Vec<DesktopModelRef>,
    #[serde(default)]
    recent_models: Vec<DesktopModelRef>,
    #[serde(default)]
    collapsed_projects: Vec<String>,
    #[serde(default)]
    last_access: Option<String>,
}

/// Bounds for the `[desktop]` picker lists.
const MAX_DESKTOP_FAVORITES: usize = 50;
const MAX_DESKTOP_RECENTS: usize = 20;
const MAX_DESKTOP_COLLAPSED: usize = 100;

impl DesktopSettings {
    fn validate(&self) -> core::Result<()> {
        if self
            .window
            .as_ref()
            .is_some_and(|window| window.width < 640 || window.height < 480)
        {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: desktop window must be at least 640 by 480",
            ));
        }
        if self.favorite_models.len() > MAX_DESKTOP_FAVORITES {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: desktop favorite_models exceeds 50 entries",
            ));
        }
        if self.recent_models.len() > MAX_DESKTOP_RECENTS {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: desktop recent_models exceeds 20 entries",
            ));
        }
        if self.collapsed_projects.len() > MAX_DESKTOP_COLLAPSED {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: desktop collapsed_projects exceeds 100 entries",
            ));
        }
        for entry in self.favorite_models.iter().chain(self.recent_models.iter()) {
            let harness = entry.harness.as_str();
            if !matches!(
                harness,
                "codex" | "claude" | "muse" | "opencode" | "antigravity"
            ) {
                return Err(core::Error::new(
                    "profile_error",
                    "config.toml: desktop model harness must be codex, claude, muse, opencode or antigravity",
                ));
            }
            let model = entry.model.as_str();
            if model.is_empty()
                || model.len() > 256
                || model.chars().any(char::is_control)
                || model != model.trim()
            {
                return Err(core::Error::new(
                    "profile_error",
                    "config.toml: desktop model must be 1-256 bytes with no controls or surrounding whitespace",
                ));
            }
            if let Some(effort) = entry.effort.as_deref()
                && (effort.is_empty()
                    || effort.len() > 128
                    || effort.chars().any(char::is_control)
                    || effort != effort.trim())
            {
                return Err(core::Error::new(
                    "profile_error",
                    "config.toml: desktop model effort must be 1-128 characters with no controls",
                ));
            }
        }
        for project in &self.collapsed_projects {
            if project.is_empty()
                || project.len() > 256
                || project.chars().any(char::is_control)
                || project != project.trim()
            {
                return Err(core::Error::new(
                    "profile_error",
                    "config.toml: desktop collapsed_projects entries must be 1-256 characters with no controls",
                ));
            }
        }
        if let Some(access) = self.last_access.as_deref()
            && !matches!(access, "ask" | "auto" | "full")
        {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: desktop last_access must be ask, auto or full",
            ));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CloudMcpSettings {
    cloudflared_executable: PathBuf,
    tunnel_id: String,
    credentials_file: PathBuf,
    hostname: String,
    access_team_name: String,
    access_audience_tag: String,
}

/// Owner-approved interpreter and action-environment grant shared by project
/// registrations and owner-global automation Run.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunGrantConfig {
    #[serde(default)]
    pub(crate) interpreters: BTreeMap<String, core::api::ScriptInterpreter>,
    #[serde(default)]
    pub(crate) run_env: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutomationConfig {
    #[serde(default = "default_max_event_backlog")]
    pub(crate) max_event_backlog: i64,
    #[serde(default)]
    pub(crate) run: RunGrantConfig,
    #[serde(default)]
    pub(crate) assignments: BTreeMap<String, AssignmentConfig>,
}

/// Settings-owned identity and bounds for a routing-aware backlog assignment.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AssignmentConfig {
    pub(crate) folder: String,
    pub(crate) workspace: String,
    #[serde(default = "default_assignment_role")]
    pub(crate) role: String,
    pub(crate) max_busy: u32,
    pub(crate) interval: String,
    /// Clean up build/temp directories and managed worktrees after an
    /// applied stop retires an assignment Agent, through the existing
    /// completion-rule stop path. Off by default; not a second executor.
    #[serde(default)]
    pub(crate) cleanup_on_retire: bool,
}

impl Default for AutomationConfig {
    fn default() -> Self {
        Self {
            max_event_backlog: default_max_event_backlog(),
            run: RunGrantConfig::default(),
            assignments: BTreeMap::new(),
        }
    }
}

impl AutomationConfig {
    fn validate(
        mut self,
        source: &str,
        route_credential_env_names: &[String],
    ) -> core::Result<Self> {
        if self.max_event_backlog <= 0 {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: automation.max_event_backlog must be a positive integer",
            ));
        }
        for (key, assignment) in &self.assignments {
            if key.is_empty()
                || key.len() > MAX_PROFILE_NAME_LEN
                || !key.chars().all(|character| {
                    character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
                })
                || assignment.folder.trim().is_empty()
                || !Path::new(&assignment.folder).is_absolute()
                || !Path::new(&assignment.workspace).is_absolute()
                || assignment.workspace.trim().is_empty()
                || assignment.folder.chars().any(char::is_control)
                || assignment.workspace.chars().any(char::is_control)
                || assignment.max_busy == 0
                || assignment.max_busy > 1000
                || assignment.interval.trim() != assignment.interval
                || assignment.interval.is_empty()
                || crate::automation::parse_positive_duration_ms(&assignment.interval).is_err()
            {
                return Err(core::Error::new(
                    "profile_error",
                    "config.toml: automation assignment has invalid identity, paths, capacity or interval",
                ));
            }
            validate_routing_role(&assignment.role)?;
        }
        self.run = normalize_run_grant(
            &self.run.interpreters,
            &self.run.run_env,
            route_credential_env_names,
            source,
            0,
            "automation.run",
        )?;
        Ok(self)
    }
}

fn default_max_event_backlog() -> i64 {
    100_000
}

fn default_assignment_role() -> String {
    "worker".to_owned()
}

/// Store-size threshold for the unbounded-retention advisory: `daemon.status`
/// reports `size_advisory` and the hourly maintenance tick logs one line
/// while the main+WAL store files pass this size with every retention window
/// unset. Documented in docs/spec/configuration.md (daemon retention).
pub(crate) const RETENTION_UNBOUNDED_SIZE_ADVISORY_BYTES: u64 = 100 * 1024 * 1024;

/// Physical retention windows consumed by daemon maintenance. See
/// docs/spec/configuration.md: daemon retention. An unset window disables
/// pruning for that side: no deletion without an owner window.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetentionConfig {
    #[serde(default)]
    pub(crate) events_days: Option<i64>,
    #[serde(default)]
    pub(crate) commands_days: Option<i64>,
    #[serde(default)]
    pub(crate) mail_days: Option<i64>,
}

impl RetentionConfig {
    fn validate(self) -> core::Result<Self> {
        if self.events_days.is_some_and(|days| days <= 0) {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: daemon.retention.events_days must be a positive integer",
            ));
        }
        if self.commands_days.is_some_and(|days| days <= 0) {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: daemon.retention.commands_days must be a positive integer",
            ));
        }
        if self.mail_days.is_some_and(|days| days <= 0) {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: daemon.retention.mail_days must be a positive integer",
            ));
        }
        Ok(self)
    }
}

/// Unread mailbox bounds consumed by `mail.send` admission. See
/// docs/spec/configuration.md: mail. Unlike retention windows, these always
/// apply: an unset value keeps its coordination-contract default.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct MailConfig {
    #[serde(default = "default_max_unread_bytes_per_agent")]
    pub(crate) max_unread_bytes_per_agent: i64,
    #[serde(default = "default_max_unread_messages_per_agent")]
    pub(crate) max_unread_messages_per_agent: i64,
}

impl Default for MailConfig {
    fn default() -> Self {
        Self {
            max_unread_bytes_per_agent: default_max_unread_bytes_per_agent(),
            max_unread_messages_per_agent: default_max_unread_messages_per_agent(),
        }
    }
}

impl MailConfig {
    fn validate(self) -> core::Result<Self> {
        if self.max_unread_bytes_per_agent <= 0 {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: mail.max_unread_bytes_per_agent must be a positive integer",
            ));
        }
        if self.max_unread_messages_per_agent <= 0 {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: mail.max_unread_messages_per_agent must be a positive integer",
            ));
        }
        Ok(self)
    }

    pub(crate) fn limits(&self) -> core::api::MailLimits {
        core::api::MailLimits {
            max_unread_bytes_per_agent: self.max_unread_bytes_per_agent,
            max_unread_messages_per_agent: self.max_unread_messages_per_agent,
        }
    }
}

fn default_max_unread_bytes_per_agent() -> i64 {
    core::api::DEFAULT_MAX_UNREAD_BYTES_PER_AGENT
}

fn default_max_unread_messages_per_agent() -> i64 {
    core::api::DEFAULT_MAX_UNREAD_MESSAGES_PER_AGENT
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DaemonConfig {
    #[serde(default)]
    pub(crate) artifact_roots: Vec<String>,
    #[serde(default)]
    pub(crate) retention: RetentionConfig,
    /// Daemon log verbosity: `standard` (working mode) or `debug`
    /// (everything mode). Hot-reloadable through `settings.save`; changing
    /// only this key never requires a restart. See
    /// docs/spec/configuration.md: daemon logging.
    #[serde(default = "default_log_level")]
    pub(crate) log_level: String,
    /// Hierarchy fanout bounds consumed by launch/selector admission. See
    /// docs/spec/configuration.md: daemon limits. Deserialized directly as
    /// the core type: unlike retention windows these always apply, so an
    /// unset value keeps its coordination-contract default.
    #[serde(default)]
    pub(crate) limits: core::api::HierarchyLimits,
    /// Healthy-source no-progress window before an attention diagnosis. See
    /// docs/spec/monitoring.md; this is advisory and never infers agent death.
    #[serde(default = "default_attention_suspicion_secs")]
    pub(crate) attention_suspicion_secs: i64,
    /// Heavy-episode thresholds for one `activity.heavy` event: a child
    /// process tree whose wall time, consumed CPU or peak RSS crosses any one
    /// threshold. See docs/spec/monitoring.md; defaults tune from real data.
    #[serde(default = "default_heavy_wall_secs")]
    pub(crate) heavy_wall_secs: i64,
    #[serde(default = "default_heavy_cpu_secs")]
    pub(crate) heavy_cpu_secs: i64,
    #[serde(default = "default_heavy_rss_bytes")]
    pub(crate) heavy_rss_bytes: i64,
    /// Context occupancy that warns, in thousandths (800 is 0.8). Integer
    /// for TOML simplicity; validated to the (0, 1000] range.
    #[serde(default = "default_session_growth_ratio_permille")]
    pub(crate) session_growth_ratio_permille: i64,
    /// Native session file size that warns, in bytes, where the harness
    /// exposes a documented size source.
    #[serde(default = "default_session_file_bytes")]
    pub(crate) session_file_bytes: i64,
}

fn default_log_level() -> String {
    "standard".to_owned()
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            artifact_roots: Vec::new(),
            retention: RetentionConfig::default(),
            log_level: default_log_level(),
            limits: core::api::HierarchyLimits::default(),
            attention_suspicion_secs: default_attention_suspicion_secs(),
            heavy_wall_secs: default_heavy_wall_secs(),
            heavy_cpu_secs: default_heavy_cpu_secs(),
            heavy_rss_bytes: default_heavy_rss_bytes(),
            session_growth_ratio_permille: default_session_growth_ratio_permille(),
            session_file_bytes: default_session_file_bytes(),
        }
    }
}

fn default_attention_suspicion_secs() -> i64 {
    15 * 60
}

fn default_heavy_wall_secs() -> i64 {
    core::DEFAULT_HEAVY_WALL_SECS as i64
}

fn default_heavy_cpu_secs() -> i64 {
    core::DEFAULT_HEAVY_CPU_SECS as i64
}

fn default_heavy_rss_bytes() -> i64 {
    core::DEFAULT_HEAVY_RSS_BYTES as i64
}

fn default_session_growth_ratio_permille() -> i64 {
    (core::DEFAULT_SESSION_GROWTH_RATIO * 1000.0) as i64
}

fn default_session_file_bytes() -> i64 {
    core::DEFAULT_SESSION_FILE_BYTES as i64
}

impl DaemonConfig {
    fn validate(self) -> core::Result<Self> {
        if self.artifact_roots.len() > 32
            || self
                .artifact_roots
                .iter()
                .any(|p| p.len() > 4096 || !std::path::Path::new(p).is_absolute())
        {
            return Err(core::Error::new(
                "profile_error",
                "daemon.artifact_roots must contain at most 32 absolute local roots",
            ));
        }
        self.limits.validate().map_err(|error| {
            core::Error::new("profile_error", format!("config.toml: {}", error.message))
        })?;
        if self.attention_suspicion_secs <= 0 || self.attention_suspicion_secs > i64::MAX / 1_000 {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: daemon.attention_suspicion_secs must be a positive integer whose millisecond value fits in i64",
            ));
        }
        // Parse here so an invalid level is a config error, not a silent
        // fallback. The live value is applied at startup and on every
        // `settings.save` through `crate::logging`.
        crate::logging::LogLevel::parse(&self.log_level)?;
        if self.heavy_wall_secs <= 0 || self.heavy_wall_secs > 31_536_000 {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: daemon.heavy_wall_secs must be a positive integer within one year",
            ));
        }
        if self.heavy_cpu_secs <= 0 || self.heavy_cpu_secs > 31_536_000 {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: daemon.heavy_cpu_secs must be a positive integer within one year",
            ));
        }
        if self.heavy_rss_bytes <= 0 {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: daemon.heavy_rss_bytes must be a positive integer",
            ));
        }
        if self.session_growth_ratio_permille <= 0 || self.session_growth_ratio_permille > 1000 {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: daemon.session_growth_ratio_permille must be within (0, 1000]",
            ));
        }
        if self.session_file_bytes <= 0 {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: daemon.session_file_bytes must be a positive integer",
            ));
        }
        Ok(Self {
            artifact_roots: self.artifact_roots,
            retention: self.retention.validate()?,
            limits: self.limits,
            attention_suspicion_secs: self.attention_suspicion_secs,
            log_level: self.log_level,
            heavy_wall_secs: self.heavy_wall_secs,
            heavy_cpu_secs: self.heavy_cpu_secs,
            heavy_rss_bytes: self.heavy_rss_bytes,
            session_growth_ratio_permille: self.session_growth_ratio_permille,
            session_file_bytes: self.session_file_bytes,
        })
    }

    pub(crate) fn heavy_thresholds(&self) -> core::Result<core::HeavyThresholds> {
        let thresholds = core::HeavyThresholds {
            wall_secs: u64::try_from(self.heavy_wall_secs).map_err(|_| {
                core::Error::new(
                    "config_error",
                    "observe heavy wall threshold is out of range",
                )
            })?,
            cpu_secs: u64::try_from(self.heavy_cpu_secs).map_err(|_| {
                core::Error::new(
                    "config_error",
                    "observe heavy CPU threshold is out of range",
                )
            })?,
            rss_bytes: u64::try_from(self.heavy_rss_bytes).map_err(|_| {
                core::Error::new(
                    "config_error",
                    "observe heavy RSS threshold is out of range",
                )
            })?,
        };
        thresholds.validate()?;
        Ok(thresholds)
    }

    pub(crate) fn session_growth_thresholds(&self) -> core::Result<core::SessionGrowthThresholds> {
        let thresholds = core::SessionGrowthThresholds {
            context_ratio: f64::from(
                i32::try_from(self.session_growth_ratio_permille).unwrap_or(1000),
            ) / 1000.0,
            file_bytes: u64::try_from(self.session_file_bytes).map_err(|_| {
                core::Error::new(
                    "config_error",
                    "observe session file threshold is out of range",
                )
            })?,
        };
        thresholds.validate()?;
        Ok(thresholds)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AccountEntry {
    #[serde(default)]
    max_live: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProxyBaseUrls {
    codex: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProxyEntry {
    attach: String,
    health: String,
    #[serde(default)]
    api_key_env: Option<String>,
    #[serde(default)]
    max_live: Option<u64>,
    base_url_for: ProxyBaseUrls,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileEntry {
    harness: String,
    #[serde(default)]
    proxy: Option<String>,
    #[serde(default)]
    account: Option<String>,
    #[serde(default)]
    max_live: Option<u64>,
    #[serde(default)]
    cmd: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    effort: Option<String>,
    #[serde(default)]
    approval: Option<String>,
    #[serde(default)]
    collaboration_mode: Option<String>,
    #[serde(default)]
    permissions: Option<String>,
    #[serde(default)]
    approval_policy: Option<String>,
    #[serde(default)]
    mcp: Option<String>,
    #[serde(default)]
    mcp_allow: Option<Vec<String>>,
    #[serde(default)]
    auth_integration: Option<String>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    dangerously_skip_permissions: Option<bool>,
    #[serde(default)]
    control: Option<String>,
    #[serde(default)]
    child_profiles: Option<Vec<String>>,
    #[serde(default)]
    native_subagents: Option<String>,
    #[serde(default)]
    pass_env: Option<Vec<String>>,
    #[serde(default)]
    setup: Vec<crate::setup::SetupEntry>,
    #[serde(default = "default_scratch")]
    scratch: Vec<String>,
    #[serde(default)]
    provider_base_url: Option<String>,
    #[serde(default)]
    api_key_env: Option<String>,
    #[serde(default)]
    backend: Option<String>,
    #[serde(default)]
    context_limit: Option<i64>,
    #[serde(default)]
    output_limit: Option<i64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectEntry {
    path: String,
    #[serde(default)]
    profiles: Vec<String>,
    #[serde(default)]
    actions: Vec<String>,
    #[serde(default)]
    git: Option<core::api::ProjectGitView>,
    #[serde(default)]
    interpreters: BTreeMap<String, core::api::ScriptInterpreter>,
    #[serde(default)]
    run_env: Vec<String>,
}

/// Read only owner-configured project registrations. Paths and Git identity are
/// canonicalized by `automation::projects` outside Store transactions.
pub(crate) fn load_project_registration_config(
    data_root: &Path,
) -> core::Result<Vec<ProjectRegistrationConfig>> {
    let Some((config, source)) = read_config(data_root)? else {
        return Ok(Vec::new());
    };
    project_registrations_in(&config, &source)
}

/// Project registration validation over already parsed source text (also
/// run on an edited profile draft: a project may not keep naming a profile
/// the draft removed).
fn project_registrations_in(
    config: &FileConfig,
    source: &str,
) -> core::Result<Vec<ProjectRegistrationConfig>> {
    if config.projects.is_empty() {
        return Ok(Vec::new());
    }
    if config.profiles.len() > MAX_PROFILES {
        return Err(core::Error::new(
            "profile_error",
            "config.toml: profiles: profile count exceeds 256 entries",
        ));
    }

    let global_profiles: HashSet<&str> = config.profiles.keys().map(String::as_str).collect();
    let route_credential_env_names = configured_route_credential_env_names(&config.proxies);
    let mut projects = Vec::with_capacity(config.projects.len());
    for (id, spanned) in &config.projects {
        let entry = spanned.get_ref();
        let offset = spanned.span().start;
        if validate_profile_name(id).is_err() {
            return Err(config_error_at(
                source,
                offset,
                &format!("projects.{id}"),
                "invalid project id",
                "profile_error",
            ));
        }

        let path = entry.path.trim();
        if path.is_empty()
            || path.len() > MAX_PROJECT_PATH_LEN
            || path.chars().any(char::is_control)
            || !Path::new(path).is_absolute()
        {
            return Err(config_error_at(
                source,
                offset,
                &format!("projects.{id}.path"),
                "must be a nonempty absolute checkout path of at most 4096 characters",
                "profile_error",
            ));
        }

        let profiles = normalize_project_allowlist(
            &entry.profiles,
            source,
            offset,
            id,
            "profiles",
            |profile| global_profiles.contains(profile) && validate_profile_name(profile).is_ok(),
        )?;
        let actions =
            normalize_project_allowlist(&entry.actions, source, offset, id, "actions", |action| {
                matches!(
                    action,
                    "launch"
                        | "resume"
                        | "stop"
                        | "send"
                        | "git.fetch"
                        | "steer"
                        | "interrupt"
                        | "approve"
                        | "deny"
                        | "answer"
                        | "run"
                        | "notify"
                        | "emit"
                        | "mail.send"
                        | "var.set"
                )
            })?;
        let git = entry.git.clone();
        if let Some(git) = &git {
            validate_project_git_config(git, source, offset, id)?;
        }
        let run_grant = normalize_run_grant(
            &entry.interpreters,
            &entry.run_env,
            &route_credential_env_names,
            source,
            offset,
            &format!("projects.{id}"),
        )?;

        projects.push(ProjectRegistrationConfig {
            id: id.clone(),
            path: path.to_owned(),
            profiles,
            actions,
            git,
            interpreters: run_grant.interpreters,
            run_env: run_grant.run_env,
        });
    }
    projects.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(projects)
}

fn normalize_run_grant(
    interpreters: &BTreeMap<String, core::api::ScriptInterpreter>,
    run_env: &[String],
    route_credential_env_names: &[String],
    source: &str,
    offset: usize,
    grant_path: &str,
) -> core::Result<RunGrantConfig> {
    let mut normalized = BTreeMap::new();
    for (reference, interpreter) in interpreters {
        let field = format!("{grant_path}.interpreters.{reference}");
        if validate_profile_name(reference).is_err() {
            return Err(config_error_at(
                source,
                offset,
                &field,
                "interpreter reference must use letters, digits, hyphen and underscore",
                "profile_error",
            ));
        }
        interpreter.validate().map_err(|_| {
            config_error_at(
                source,
                offset,
                &field,
                "must specify an absolute program path and a NUL-free argv_prefix",
                "profile_error",
            )
        })?;
        normalized.insert(reference.clone(), interpreter.clone());
    }
    let run_env = normalize_run_env(
        run_env,
        route_credential_env_names,
        source,
        offset,
        &format!("{grant_path}.run_env"),
    )?;
    Ok(RunGrantConfig {
        interpreters: normalized,
        run_env,
    })
}

fn normalize_run_env(
    names: &[String],
    route_credential_env_names: &[String],
    source: &str,
    offset: usize,
    field: &str,
) -> core::Result<Vec<String>> {
    if names.len() > MAX_PASS_ENV_ENTRIES {
        return Err(config_error_at(
            source,
            offset,
            field,
            "exceeds 64 entries",
            "profile_error",
        ));
    }

    let mut normalized = Vec::with_capacity(names.len());
    let mut seen = HashSet::with_capacity(names.len());
    for raw in names {
        let name = raw.trim();
        if !valid_environment_name(name) {
            return Err(config_error_at(
                source,
                offset,
                field,
                "names must use 1-256 letters, digits and underscores, starting with a letter or underscore",
                "profile_error",
            ));
        }
        if forbidden_run_env_name(name, route_credential_env_names) {
            return Err(config_error_at(
                source,
                offset,
                field,
                "cannot include identity, credential, route, native-store, Git or reserved environment names",
                "profile_error",
            ));
        }
        let key = if cfg!(windows) {
            name.to_ascii_uppercase()
        } else {
            name.to_owned()
        };
        if !seen.insert(key) {
            return Err(config_error_at(
                source,
                offset,
                field,
                "contains a duplicate environment name",
                "profile_error",
            ));
        }
        normalized.push(name.to_owned());
    }
    normalized.sort();
    Ok(normalized)
}

fn forbidden_run_env_name(name: &str, route_credential_env_names: &[String]) -> bool {
    let uppercase = name.to_ascii_uppercase();
    uppercase.starts_with("ELIOT_")
        || uppercase.starts_with("GIT_")
        || env_names_equal(name, "PATH")
        || native_store_selector(name).is_some()
        || route_credential_env_names
            .iter()
            .any(|credential| env_names_equal(name, credential))
        || uppercase.split('_').any(|part| {
            matches!(
                part,
                "ROLE" | "TOKEN" | "CREDENTIAL" | "CREDENTIALS" | "PASSWORD" | "SECRET" | "AUTH"
            )
        })
        || [
            "_API_KEY",
            "_ACCESS_KEY",
            "_PRIVATE_KEY",
            "_BASE_URL",
            "_ENDPOINT",
            "_ROUTE",
            "_ACCOUNT",
        ]
        .iter()
        .any(|suffix| uppercase.ends_with(suffix))
}

fn configured_route_credential_env_names(
    proxies: &HashMap<String, toml::Spanned<ProxyEntry>>,
) -> Vec<String> {
    proxies
        .values()
        .filter_map(|proxy| proxy.get_ref().api_key_env.as_deref())
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .collect()
}

fn env_names_equal(left: &str, right: &str) -> bool {
    if cfg!(windows) {
        left.eq_ignore_ascii_case(right)
    } else {
        left == right
    }
}

fn valid_environment_name(name: &str) -> bool {
    if name.is_empty() || name.len() > MAX_ENV_NAME_LEN {
        return false;
    }
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        && chars.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

fn normalize_project_allowlist(
    values: &[String],
    source: &str,
    offset: usize,
    project_id: &str,
    field: &str,
    allowed: impl Fn(&str) -> bool,
) -> core::Result<Vec<String>> {
    let mut normalized = Vec::with_capacity(values.len());
    let mut seen = HashSet::with_capacity(values.len());
    for raw in values {
        let value = raw.trim();
        if value.is_empty() || value.len() > MAX_PROFILE_NAME_LEN || !allowed(value) {
            return Err(config_error_at(
                source,
                offset,
                &format!("projects.{project_id}.{field}"),
                if field == "actions" {
                    "only implemented launch, resume, stop, send, git.fetch, steer, interrupt, approve, deny, answer, run, notify and emit actions may be approved"
                } else {
                    "must name an existing global profile"
                },
                "profile_error",
            ));
        }
        if !seen.insert(value.to_owned()) {
            return Err(config_error_at(
                source,
                offset,
                &format!("projects.{project_id}.{field}"),
                "contains a duplicate entry",
                "profile_error",
            ));
        }
        normalized.push(value.to_owned());
    }
    normalized.sort();
    Ok(normalized)
}

fn validate_project_git_config(
    git: &core::api::ProjectGitView,
    source: &str,
    offset: usize,
    project_id: &str,
) -> core::Result<()> {
    git.validate().map_err(|_| {
        config_error_at(
            source,
            offset,
            &format!("projects.{project_id}.git"),
            "contains an invalid Git fetch approval",
            "profile_error",
        )
    })?;
    for (remote, endpoint) in &git.fetch_remotes {
        if !is_direct_project_fetch_endpoint(endpoint) {
            return Err(config_error_at(
                source,
                offset,
                &format!("projects.{project_id}.git.fetch_remotes.{remote}"),
                "must be an absolute local path or direct HTTP, HTTPS, Git, or file URL without credentials or helper schemes; SSH is unsupported",
                "profile_error",
            ));
        }
    }
    Ok(())
}

fn is_direct_project_fetch_endpoint(endpoint: &str) -> bool {
    if endpoint.trim() != endpoint
        || endpoint.starts_with('-')
        || endpoint.len() > MAX_PROJECT_PATH_LEN
        || endpoint.chars().any(char::is_control)
        || has_git_remote_helper_delimiter(endpoint)
    {
        return false;
    }
    if Path::new(endpoint).is_absolute() {
        return true;
    }
    let Ok(url) = reqwest::Url::parse(endpoint) else {
        return false;
    };
    matches!(url.scheme(), "file" | "git" | "http" | "https")
        && url.username().is_empty()
        && url.password().is_none()
        && !project_git_endpoint_has_userinfo(endpoint)
        && url.query().is_none()
        && url.fragment().is_none()
        && (url.scheme() == "file" || url.host_str().is_some_and(|host| !host.is_empty()))
}

fn project_git_endpoint_has_userinfo(endpoint: &str) -> bool {
    let Some((_, rest)) = endpoint.split_once("://") else {
        return false;
    };
    rest.split(['/', '?', '#'])
        .next()
        .is_some_and(|authority| authority.contains('@'))
}

fn has_git_remote_helper_delimiter(endpoint: &str) -> bool {
    endpoint
        .find("::")
        .is_some_and(|delimiter| endpoint.find("://").is_none_or(|scheme| delimiter < scheme))
}

/// Test-only uncached entry point: a fresh cache per call behaves like a cold
/// router on its first launch, so existing tests keep asserting launch
/// behavior without touching router state.
#[cfg(test)]
pub(crate) fn resolve_profile(data_root: &Path, name: &str) -> core::Result<Value> {
    let cache = Mutex::new(ProfileCache::default());
    resolve_profile_cached(&cache, data_root, name)
}
/// Validated profile snapshot cache keyed by file content revision.
///
/// The router holds one of these (never a global). Launch admission still
/// resolves per launch, but file reads plus full validation run once per
/// content revision: any byte change re-validates, errors stay typed.
#[derive(Default)]
pub(crate) struct ProfileCache {
    cached: Option<CachedProfileConfig>,
}
struct CachedProfileConfig {
    revision: Option<String>,
    source: String,
    config: Option<FileConfig>,
    snapshots: HashMap<String, Value>,
}
/// Router launch-admission entry point: reuse the validated snapshot when the
/// file revision matches, otherwise re-read, re-validate and re-cache.
/// A missing file caches the miss until it appears; parse failures are never
/// cached, so the next launch re-validates the same bytes identically.
pub(crate) fn resolve_profile_cached(
    cache: &Mutex<ProfileCache>,
    data_root: &Path,
    name: &str,
) -> core::Result<Value> {
    validate_profile_name(name)?;
    let current_source = read_config_source(data_root)?;
    let current_revision = current_source.as_deref().map(profiles_edit::revision_of);
    {
        let guard = cache
            .lock()
            .map_err(|_| core::Error::new("internal", "profile cache poisoned"))?;
        if let Some(cached) = guard.cached.as_ref()
            && cached.revision == current_revision
        {
            if let Some(hit) = cached.snapshots.get(name) {
                return Ok(hit.clone());
            }
            if let (Some(config), source) = (cached.config.as_ref(), cached.source.as_str()) {
                return resolve_profile_in(config, source, name);
            }
            return Err(core::Error::new(
                "profile_error",
                format!("config.toml: profiles.{name}: profile not found"),
            ));
        }
    }
    let Some(source) = current_source else {
        let mut guard = cache
            .lock()
            .map_err(|_| core::Error::new("internal", "profile cache poisoned"))?;
        guard.cached = Some(CachedProfileConfig {
            revision: None,
            source: String::new(),
            config: None,
            snapshots: HashMap::new(),
        });
        return Err(core::Error::new(
            "profile_error",
            format!("config.toml: profiles.{name}: profile not found"),
        ));
    };
    let config = parse_config(&source)?;
    let snapshot = resolve_profile_in(&config, &source, name)?;
    let mut guard = cache
        .lock()
        .map_err(|_| core::Error::new("internal", "profile cache poisoned"))?;
    match guard.cached.as_mut() {
        Some(cached) if cached.revision == current_revision => {
            cached.snapshots.insert(name.to_owned(), snapshot.clone());
        }
        _ => {
            let mut snapshots = HashMap::new();
            snapshots.insert(name.to_owned(), snapshot.clone());
            guard.cached = Some(CachedProfileConfig {
                revision: current_revision,
                source,
                config: Some(config),
                snapshots,
            });
        }
    }
    Ok(snapshot)
}

/// Launch-time profile resolution over already parsed source text. The same
/// path validates an edited draft before `config.profiles.save` replaces
/// the file, so a saved profile is one the next launch can resolve.
fn resolve_profile_in(config: &FileConfig, source: &str, name: &str) -> core::Result<Value> {
    validate_profile_name(name)?;
    if config.profiles.len() > MAX_PROFILES {
        return Err(core::Error::new(
            "profile_error",
            "config.toml: profiles: profile count exceeds 256 entries",
        ));
    }
    for (profile_name, entry) in &config.profiles {
        if validate_profile_name(profile_name).is_err() {
            return Err(config_error_at(
                source,
                entry.span().start,
                "profiles",
                "invalid profile name",
                "profile_error",
            ));
        }
    }
    for (account_name, entry) in &config.accounts {
        if validate_profile_name(account_name).is_err() {
            return Err(config_error_at(
                source,
                entry.span().start,
                "accounts",
                "invalid account name",
                "profile_error",
            ));
        }
    }
    for (proxy_name, entry) in &config.proxies {
        if validate_profile_name(proxy_name).is_err() {
            return Err(config_error_at(
                source,
                entry.span().start,
                "proxies",
                "invalid proxy name",
                "profile_error",
            ));
        }
    }
    let entry = config.profiles.get(name).ok_or_else(|| {
        core::Error::new(
            "profile_error",
            format!("config.toml: profiles.{name}: profile not found"),
        )
    })?;
    let mut snapshot = snapshot_from_entry(name, entry.get_ref(), entry.span().start, source)?;
    // Freeze the configured cloud MCP hostname (if any) beside the MCP
    // policy so harness plugins can apply the owner-identity HTTP arm
    // without reading owner config. Snapshots that predate the field read
    // back as absent: plugins treat that as inert.
    snapshot["mcp_cloud_hostname"] = match &config.cloud_mcp {
        Some(cloud) => Value::String(cloud.hostname.clone()),
        None => Value::Null,
    };
    let snapshot = freeze_child_profiles(
        name,
        entry.get_ref(),
        config,
        entry.span().start,
        source,
        snapshot,
    )?;
    let snapshot =
        freeze_native_subagents(name, entry.get_ref(), entry.span().start, source, snapshot)?;
    bind_route_and_capacity(
        name,
        entry.get_ref(),
        config,
        entry.span().start,
        source,
        snapshot,
    )
}

/// Freeze the owner-configured Agent child allow-list for this profile.
/// Each name must be an existing configured profile, and a profile must not
/// list itself. The Owner grants cross-harness authority by naming the child's
/// own profile; policy names are never translated from the parent's harness.
/// The frozen list is captured in the parent snapshot, so a later config
/// edit cannot widen an already-running manager. Absent means empty: Agent
/// children reuse the parent profile only.
fn freeze_child_profiles(
    profile_name: &str,
    entry: &ProfileEntry,
    config: &FileConfig,
    table_offset: usize,
    source: &str,
    mut snapshot: Value,
) -> core::Result<Value> {
    let fail = |message: &str| {
        profile_field_error(
            source,
            table_offset,
            profile_name,
            "child_profiles",
            message,
            "profile_error",
        )
    };
    let Some(raw) = entry.child_profiles.as_deref() else {
        snapshot["child_profiles"] = Value::Array(Vec::new());
        return Ok(snapshot);
    };
    if raw.len() > MAX_CHILD_PROFILES {
        return Err(fail("exceeds 64 entries"));
    }
    let mut frozen = Vec::with_capacity(raw.len());
    let mut seen = HashSet::with_capacity(raw.len());
    for name in raw {
        let child = name.trim();
        if validate_profile_name(child).is_err() {
            return Err(fail(
                "must name existing configured profiles with letters, digits, hyphen and underscore",
            ));
        }
        if child == profile_name {
            return Err(fail("must not list the profile itself as a child profile"));
        }
        if !config.profiles.contains_key(child) {
            return Err(fail(&format!("unknown child profile {child}")));
        }
        if !seen.insert(child.to_owned()) {
            return Err(fail("contains a duplicate entry"));
        }
        frozen.push(Value::String(child.to_owned()));
    }
    snapshot["child_profiles"] = Value::Array(frozen);
    Ok(snapshot)
}
/// Freeze the harness-native subagent switch for this profile: `allow` keeps
/// today's behavior, `deny` asks the harness plugin to disable its native
/// subagent surface for the session. An omitted value defaults to `deny`
/// when the profile names a non-empty `child_profiles` allow-list (managers
/// delegate through Swarm), and to `allow` otherwise. The frozen value is
/// reported in the snapshot so effective settings stay truthful. `deny` has
/// a per-session mapping on Codex, Claude and OpenCode only; on Muse and
/// Antigravity the installed binaries expose no verified switch, so `deny`
/// is `unsupported` at config load, matching the existing pattern for
/// unmapped fields.
fn freeze_native_subagents(
    profile_name: &str,
    entry: &ProfileEntry,
    table_offset: usize,
    source: &str,
    mut snapshot: Value,
) -> core::Result<Value> {
    let harness = snapshot
        .get("harness")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let requested = entry
        .native_subagents
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let effective = match requested {
        Some(value) => {
            if !matches!(value, "allow" | "deny") {
                return Err(profile_field_error(
                    source,
                    table_offset,
                    profile_name,
                    "native_subagents",
                    "must be allow or deny",
                    "profile_error",
                ));
            }
            value.to_owned()
        }
        None => {
            let has_children = entry
                .child_profiles
                .as_deref()
                .is_some_and(|names| !names.is_empty());
            if has_children { "deny" } else { "allow" }.to_owned()
        }
    };
    if effective == "deny" && matches!(harness.as_str(), "muse" | "antigravity") {
        return Err(profile_field_error(
            source,
            table_offset,
            profile_name,
            "native_subagents",
            "native_subagents deny has no mapping on this harness",
            "unsupported",
        ));
    }
    snapshot["native_subagents"] = Value::String(effective);
    Ok(snapshot)
}
/// Best-effort configured cloud MCP hostname for ad-hoc snapshots: the
/// `[cloud_mcp] hostname` when the owner config parses and names one, else
/// `None`. Never fails: ad-hoc snapshots stamp what is configured.
pub(crate) fn cloud_mcp_hostname(data_root: &Path) -> Option<String> {
    read_config(data_root)
        .ok()
        .flatten()
        .and_then(|(config, _)| config.cloud_mcp.map(|cloud| cloud.hostname))
}

/// Stamp the configured cloud MCP hostname into a frozen snapshot
/// (`mcp_cloud_hostname`, string or null) for ad-hoc snapshots, which
/// freeze without the profile path. Best-effort: an unreadable config
/// leaves a null stamp, so the owner-identity HTTP arm stays inert.
pub(crate) fn stamp_cloud_mcp_hostname(snapshot: &mut Value, data_root: &Path) {
    snapshot["mcp_cloud_hostname"] = match cloud_mcp_hostname(data_root) {
        Some(hostname) => Value::String(hostname),
        None => Value::Null,
    };
}

fn read_config(data_root: &Path) -> core::Result<Option<(FileConfig, String)>> {
    let Some(source) = read_config_source(data_root)? else {
        return Ok(None);
    };
    let config = parse_config(&source)?;
    Ok(Some((config, source)))
}

/// The one `config.toml` parser: strict (`deny_unknown_fields`) with
/// located errors.
fn parse_config(source: &str) -> core::Result<FileConfig> {
    let config: FileConfig =
        toml::from_str(source).map_err(|error| map_toml_error(source, error))?;
    config.desktop.validate()?;
    let mut role_candidates = Vec::with_capacity(config.routing.roles.len() + 1);
    role_candidates.push(("default", &config.routing.default));
    role_candidates.extend(
        config
            .routing
            .roles
            .iter()
            .map(|(role, profiles)| (role.as_str(), profiles)),
    );
    for (role, profiles) in role_candidates {
        validate_routing_role(role)?;
        if profiles.len() > 64 {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: routing roles allow at most 64 candidates",
            ));
        }
        let mut seen = HashSet::new();
        for profile in profiles {
            validate_profile_name(profile)?;
            if !seen.insert(profile) {
                return Err(core::Error::new(
                    "profile_error",
                    "config.toml: routing role contains a duplicate candidate profile",
                ));
            }
            if !config.profiles.contains_key(profile) {
                return Err(core::Error::new(
                    "profile_error",
                    "config.toml: routing candidate names an unknown profile",
                ));
            }
        }
    }
    Ok(config)
}

pub(crate) fn validate_routing_role(role: &str) -> core::Result<()> {
    if role.is_empty()
        || role.len() > MAX_PROFILE_NAME_LEN
        || !role
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(core::Error::new(
            "profile_error",
            "config.toml: routing role names must be nonempty letters, digits, hyphen or underscore",
        ));
    }
    Ok(())
}

/// Raw `config.toml` text within the size bound; `None` when absent.
fn read_config_source(data_root: &Path) -> core::Result<Option<String>> {
    let file = match std::fs::File::open(data_root.join("config.toml")) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: cannot read configuration file",
            ));
        }
    };
    let mut reader = file.take((MAX_CONFIG_BYTES + 1) as u64);
    let mut bytes = Vec::with_capacity(MAX_CONFIG_BYTES + 1);
    reader.read_to_end(&mut bytes).map_err(|_| {
        core::Error::new(
            "profile_error",
            "config.toml: cannot read configuration file",
        )
    })?;
    if bytes.len() > MAX_CONFIG_BYTES {
        return Err(core::Error::new(
            "profile_error",
            "config.toml: file exceeds the 128 KiB bound",
        ));
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| core::Error::new("profile_error", "config.toml: invalid UTF-8"))
}

pub(crate) fn load_automation_config(data_root: &Path) -> core::Result<AutomationConfig> {
    let Some((config, source)) = read_config(data_root)? else {
        return Ok(AutomationConfig::default());
    };
    let route_credential_env_names = configured_route_credential_env_names(&config.proxies);
    config
        .automation
        .validate(&source, &route_credential_env_names)
}

pub(crate) fn routing_candidates(data_root: &Path, role: &str) -> core::Result<Vec<String>> {
    validate_routing_role(role)?;
    let Some((config, _)) = read_config(data_root)? else {
        return Ok(Vec::new());
    };
    Ok(config.routing.candidates(role).unwrap_or_default().to_vec())
}

pub(crate) fn load_daemon_config(data_root: &Path) -> core::Result<DaemonConfig> {
    let Some((config, _)) = read_config(data_root)? else {
        return Ok(DaemonConfig::default());
    };
    config.daemon.validate()
}

pub(crate) fn load_cloud_mcp_config(
    data_root: &Path,
) -> core::Result<Option<crate::cloud_mcp::CloudMcpConfig>> {
    let Some((config, _)) = read_config(data_root)? else {
        return Ok(None);
    };
    let Some(settings) = config.cloud_mcp else {
        return Ok(None);
    };
    let http_mcp_executable = std::env::current_exe().map_err(|_| {
        core::Error::new(
            "platform_io",
            "cannot locate the daemon executable for cloud MCP",
        )
    })?;
    let config = crate::cloud_mcp::CloudMcpConfig {
        http_mcp_executable,
        cloudflared_executable: settings.cloudflared_executable,
        tunnel_id: settings.tunnel_id,
        credentials_file: settings.credentials_file,
        hostname: settings.hostname,
        access_team_name: settings.access_team_name,
        access_audience_tag: settings.access_audience_tag,
    };
    crate::cloud_mcp::validate_config(&config)?;
    Ok(Some(config))
}

/// Public, nonsecret settings used by the HTTP origin verifier. Manual and
/// managed HTTP share the existing owner-configured Access application.
pub fn load_cloud_access_config(data_root: &Path) -> core::Result<(String, String)> {
    let config = load_cloud_mcp_config(data_root)?.ok_or_else(|| {
        core::Error::new(
            "profile_error",
            "HTTP MCP requires configured cloud_mcp Access authentication",
        )
    })?;
    Ok((
        format!("https://{}.cloudflareaccess.com", config.access_team_name),
        config.access_audience_tag,
    ))
}

pub(crate) fn load_mail_config(data_root: &Path) -> core::Result<MailConfig> {
    let Some((config, _)) = read_config(data_root)? else {
        return Ok(MailConfig::default());
    };
    config.mail.validate()
}

fn snapshot_from_entry(
    name: &str,
    entry: &ProfileEntry,
    table_offset: usize,
    source: &str,
) -> core::Result<Value> {
    let harness = entry.harness.trim();
    if harness != "codex"
        && harness != "muse"
        && harness != "claude"
        && harness != "opencode"
        && harness != "antigravity"
    {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "harness",
            "unsupported harness",
            "unsupported",
        ));
    }
    // Adopted local backends are an OpenCode-only path (Codex against
    // these backends stays unsupported until readiness, catalog/context,
    // history and exact tool formats are resolved separately). Any local
    // field on another harness is rejected, never silently frozen.
    if harness != "opencode" && has_local_backend_fields(entry) {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "provider_base_url",
            "adopted local backends have no mapping on this harness",
            "unsupported",
        ));
    }
    if harness == "muse" {
        return muse_snapshot_from_entry(name, entry, table_offset, source);
    }
    if harness == "claude" {
        return claude_snapshot_from_entry(name, entry, table_offset, source);
    }
    if harness == "opencode" {
        return opencode_snapshot_from_entry(name, entry, table_offset, source);
    }
    if harness == "antigravity" {
        return antigravity_snapshot_from_entry(name, entry, table_offset, source);
    }
    let model = entry.model.as_deref();
    if let Some(model) = model {
        validate_model(name, model, source, table_offset)?;
    }
    let effort = entry.effort.as_deref().map(str::trim);
    if effort.is_some_and(|value| {
        value.is_empty() || value.len() > MAX_EFFORT_LEN || value.chars().any(char::is_control)
    }) {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "effort",
            "must be non-empty, at most 128 characters and contain no controls",
            "profile_error",
        ));
    }
    let collaboration_mode = entry.collaboration_mode.as_deref().map(str::trim);
    if collaboration_mode.is_some_and(|mode| !matches!(mode, "default" | "plan")) {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "collaboration_mode",
            "must be default or plan",
            "profile_error",
        ));
    }
    let permissions = entry
        .permissions
        .as_deref()
        .map(str::trim)
        .unwrap_or(PERMISSION_WORKSPACE_WRITE);
    if permissions != PERMISSION_READ_ONLY
        && permissions != PERMISSION_WORKSPACE_WRITE
        && permissions != PERMISSION_DANGER_FULL_ACCESS
    {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "permissions",
            "unsupported permission mode",
            "unsupported",
        ));
    }
    let approval_policy = entry
        .approval_policy
        .as_deref()
        .map(str::trim)
        .unwrap_or(APPROVAL_POLICY_NEVER);
    if approval_policy != APPROVAL_POLICY_NEVER && approval_policy != APPROVAL_POLICY_ON_REQUEST {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "approval_policy",
            "must be never or on-request",
            "profile_error",
        ));
    }
    if entry.mode.is_some() || entry.dangerously_skip_permissions.is_some() {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "mode",
            "mode and dangerously_skip_permissions have no Codex mapping",
            "unsupported",
        ));
    }
    if entry.auth_integration.is_some() {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "auth_integration",
            "auth_integration has no Codex mapping",
            "unsupported",
        ));
    }
    let pass_env = match entry.pass_env.as_deref() {
        Some(names) => validate_pass_env(name, names, source, table_offset)?,
        None => Vec::new(),
    };
    let cmd = match entry.cmd.as_deref() {
        Some(cmd) => Some(validate_cmd(name, cmd, source, table_offset)?),
        None => None,
    };
    crate::setup::validate_entries(&entry.setup).map_err(|error| {
        profile_field_error(
            source,
            table_offset,
            name,
            "setup",
            &error.message,
            "profile_error",
        )
    })?;
    let scratch = normalize_scratch(&entry.scratch).map_err(|error| {
        profile_field_error(
            source,
            table_offset,
            name,
            "scratch",
            &error.message,
            "profile_error",
        )
    })?;
    let mut snapshot = serde_json::Map::new();
    snapshot.insert("harness".to_owned(), Value::String("codex".to_owned()));
    if let Some(cmd) = cmd {
        snapshot.insert("cmd".to_owned(), Value::String(cmd));
    }
    if let Some(model) = model {
        snapshot.insert("model".to_owned(), Value::String(model.to_owned()));
    }
    if let Some(effort) = effort {
        snapshot.insert("effort".to_owned(), Value::String(effort.to_owned()));
    }
    if let Some(collaboration_mode) = collaboration_mode {
        snapshot.insert(
            "collaboration_mode".to_owned(),
            Value::String(collaboration_mode.to_owned()),
        );
    }
    snapshot.insert(
        "permissions".to_owned(),
        Value::String(permissions.to_owned()),
    );
    snapshot.insert(
        "approval_policy".to_owned(),
        Value::String(approval_policy.to_owned()),
    );
    freeze_mcp_policy(
        &mut snapshot,
        name,
        entry,
        table_offset,
        source,
        McpHarnessSupport {
            harness: "Codex",
            minimal: true,
            allow: true,
        },
    )?;
    insert_control(
        &mut snapshot,
        name,
        entry.control.as_deref(),
        source,
        table_offset,
    )?;
    snapshot.insert(
        "pass_env".to_owned(),
        Value::Array(pass_env.into_iter().map(Value::String).collect()),
    );
    snapshot.insert("scratch".to_owned(), json!(scratch));
    if !entry.setup.is_empty() {
        snapshot.insert(
            "setup".to_owned(),
            serde_json::to_value(&entry.setup)
                .map_err(|_| core::Error::new("profile_error", "setup could not be encoded"))?,
        );
    }
    Ok(Value::Object(snapshot))
}

/// Validated profile tail shared by the Muse, OpenCode and Antigravity
/// slices: an explicit pass_env list, a required explicit native `cmd` (no
/// script wrapper), and validated setup/scratch. Each harness snapshot
/// validates its own head (model, effort, ...) then folds this tail in.
struct ProfileTail {
    pass_env: Vec<String>,
    cmd: String,
    scratch: Vec<String>,
}

fn validate_profile_tail(
    name: &str,
    entry: &ProfileEntry,
    table_offset: usize,
    source: &str,
) -> core::Result<ProfileTail> {
    let pass_env = match entry.pass_env.as_deref() {
        Some(names) => validate_pass_env(name, names, source, table_offset)?,
        None => Vec::new(),
    };
    let cmd = match entry.cmd.as_deref() {
        Some(cmd) => reject_script_wrapper(
            name,
            validate_cmd(name, cmd, source, table_offset)?,
            source,
            table_offset,
        )?,
        None => {
            return Err(profile_field_error(
                source,
                table_offset,
                name,
                "cmd",
                "required explicit cmd is missing",
                "profile_error",
            ));
        }
    };
    crate::setup::validate_entries(&entry.setup).map_err(|error| {
        profile_field_error(
            source,
            table_offset,
            name,
            "setup",
            &error.message,
            "profile_error",
        )
    })?;
    let scratch = normalize_scratch(&entry.scratch).map_err(|error| {
        profile_field_error(
            source,
            table_offset,
            name,
            "scratch",
            &error.message,
            "profile_error",
        )
    })?;
    Ok(ProfileTail {
        pass_env,
        cmd,
        scratch,
    })
}

/// Fold the validated tail into the harness snapshot: the required cmd,
/// pass_env, scratch, and setup (when non-empty).
fn insert_profile_tail(
    snapshot: &mut serde_json::Map<String, Value>,
    tail: ProfileTail,
    setup: &[crate::setup::SetupEntry],
) -> core::Result<()> {
    snapshot.insert("cmd".to_owned(), Value::String(tail.cmd));
    snapshot.insert(
        "pass_env".to_owned(),
        Value::Array(tail.pass_env.into_iter().map(Value::String).collect()),
    );
    snapshot.insert("scratch".to_owned(), json!(tail.scratch));
    if !setup.is_empty() {
        snapshot.insert(
            "setup".to_owned(),
            serde_json::to_value(setup)
                .map_err(|_| core::Error::new("profile_error", "setup could not be encoded"))?,
        );
    }
    Ok(())
}

/// Freeze a Muse profile. The PR1 slice carries an explicit model id, a
/// required explicit native `cmd` (no PATH default: resolving `muse` would
/// land on the launcher wrapper and orphan the host on stop), an optional
/// reasoning effort, optional approval mode, and explicit permissions
/// (`workspace-write` or `danger-full-access`; host birth control passes
/// `--disable-sandbox` for full access); collaboration presets, adopted routes,
/// read-only permissions and approval policies stay rejected until their native
/// mappings land.
fn muse_snapshot_from_entry(
    name: &str,
    entry: &ProfileEntry,
    table_offset: usize,
    source: &str,
) -> core::Result<Value> {
    let model = entry.model.as_deref();
    if let Some(model) = model {
        validate_model(name, model, source, table_offset)?;
    }
    let effort = entry.effort.as_deref().map(str::trim);
    if effort.is_some_and(|value| {
        value.is_empty() || value.len() > MAX_EFFORT_LEN || value.chars().any(char::is_control)
    }) {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "effort",
            "must be non-empty, at most 128 characters and contain no controls",
            "profile_error",
        ));
    }
    let approval = entry.approval.as_deref().map(str::trim);
    if approval.is_some_and(|mode| {
        !matches!(
            mode,
            "allowAll" | "promptUnmatched" | "onRequest" | "denyUnmatched"
        )
    }) {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "approval",
            "must be allowAll, promptUnmatched, onRequest or denyUnmatched",
            "profile_error",
        ));
    }
    if entry.collaboration_mode.is_some() {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "collaboration_mode",
            "the Muse slice has no collaboration preset mapping",
            "unsupported",
        ));
    }
    let permissions = entry
        .permissions
        .as_deref()
        .map(str::trim)
        .unwrap_or(PERMISSION_WORKSPACE_WRITE);
    if permissions != PERMISSION_WORKSPACE_WRITE && permissions != PERMISSION_DANGER_FULL_ACCESS {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "permissions",
            "the Muse slice supports only workspace-write or danger-full-access",
            "unsupported",
        ));
    }
    if entry
        .approval_policy
        .as_deref()
        .map(str::trim)
        .is_some_and(|policy| policy != APPROVAL_POLICY_NEVER)
    {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "approval_policy",
            "the Muse slice leaves approvals unconnected",
            "unsupported",
        ));
    }
    if entry.mode.is_some() || entry.dangerously_skip_permissions.is_some() {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "mode",
            "the Muse slice has no mapping for this preset",
            "unsupported",
        ));
    }
    if entry.auth_integration.is_some() {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "auth_integration",
            "the Muse slice has no mapping for this preset",
            "unsupported",
        ));
    }
    let tail = validate_profile_tail(name, entry, table_offset, source)?;
    let mut snapshot = serde_json::Map::new();
    snapshot.insert("harness".to_owned(), Value::String("muse".to_owned()));
    if let Some(model) = model {
        snapshot.insert("model".to_owned(), Value::String(model.to_owned()));
    }
    if let Some(effort) = effort {
        snapshot.insert("effort".to_owned(), Value::String(effort.to_owned()));
    }
    if let Some(approval) = approval {
        snapshot.insert("approval".to_owned(), Value::String(approval.to_owned()));
    }
    freeze_mcp_policy(
        &mut snapshot,
        name,
        entry,
        table_offset,
        source,
        McpHarnessSupport {
            harness: "Muse",
            minimal: false,
            allow: false,
        },
    )?;
    snapshot.insert(
        "permissions".to_owned(),
        Value::String(permissions.to_owned()),
    );
    snapshot.insert(
        "approval_policy".to_owned(),
        Value::String(APPROVAL_POLICY_NEVER.to_owned()),
    );
    insert_control(
        &mut snapshot,
        name,
        entry.control.as_deref(),
        source,
        table_offset,
    )?;
    insert_profile_tail(&mut snapshot, tail, &entry.setup)?;
    Ok(Value::Object(snapshot))
}

/// Freeze a Claude profile. The PR2 slice carries an exact model id, a
/// required explicit native `cmd` (no PATH default: a resolved wrapper would
/// orphan the real CLI on stop), the CLI permission modes mapped from
/// `approval`, and approve/deny replies through the stdio permission
/// callback, and the CLI `--effort` level (low, medium, high, xhigh, max).
/// Collaboration presets, non-default permissions, and a non-`never`
/// Eliot-side `approval_policy` stay rejected until their mappings land.
fn claude_snapshot_from_entry(
    name: &str,
    entry: &ProfileEntry,
    table_offset: usize,
    source: &str,
) -> core::Result<Value> {
    let model = entry.model.as_deref();
    if let Some(model) = model {
        validate_model(name, model, source, table_offset)?;
    }
    // Claude Code 2.1.278 `--effort <level>`: the CLI's closed vocabulary.
    // Absent effort keeps the CLI default; no value is fabricated.
    let effort = match entry.effort.as_deref().map(str::trim) {
        None | Some("") => None,
        Some(level @ ("low" | "medium" | "high" | "xhigh" | "max")) => Some(level.to_owned()),
        Some(_) => {
            return Err(profile_field_error(
                source,
                table_offset,
                name,
                "effort",
                "unknown Claude effort level (low, medium, high, xhigh, or max)",
                "profile_error",
            ));
        }
    };
    let permission_mode = match entry.approval.as_deref().map(str::trim) {
        None | Some("") => None,
        Some(mode) => match mode {
            "acceptEdits" | "auto" | "bypassPermissions" | "manual" | "dontAsk" | "plan" => {
                Some(mode.to_owned())
            }
            _ => {
                return Err(profile_field_error(
                    source,
                    table_offset,
                    name,
                    "approval",
                    "unknown Claude permission mode (acceptEdits, auto, bypassPermissions, manual, dontAsk, or plan)",
                    "profile_error",
                ));
            }
        },
    };
    if entry.collaboration_mode.is_some() {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "collaboration_mode",
            "the Claude slice has no collaboration preset mapping",
            "unsupported",
        ));
    }
    if entry
        .permissions
        .as_deref()
        .map(str::trim)
        .is_some_and(|permissions| permissions != PERMISSION_WORKSPACE_WRITE)
    {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "permissions",
            "the Claude slice runs the CLI-default permission posture",
            "unsupported",
        ));
    }
    if entry
        .approval_policy
        .as_deref()
        .map(str::trim)
        .is_some_and(|policy| policy != APPROVAL_POLICY_NEVER)
    {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "approval_policy",
            "the Claude slice leaves approvals unconnected",
            "unsupported",
        ));
    }
    if entry.mode.is_some() || entry.dangerously_skip_permissions.is_some() {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "mode",
            "the Claude slice has no mapping for this preset",
            "unsupported",
        ));
    }
    if entry.auth_integration.is_some() {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "auth_integration",
            "the Claude slice has no mapping for this preset",
            "unsupported",
        ));
    }
    let pass_env = match entry.pass_env.as_deref() {
        Some(names) => validate_pass_env(name, names, source, table_offset)?,
        None => Vec::new(),
    };
    let cmd = match entry.cmd.as_deref() {
        Some(cmd) => require_native_claude_cmd(
            name,
            reject_script_wrapper(
                name,
                validate_cmd(name, cmd, source, table_offset)?,
                source,
                table_offset,
            )?,
            source,
            table_offset,
        )?,
        None => {
            return Err(profile_field_error(
                source,
                table_offset,
                name,
                "cmd",
                "required explicit cmd is missing",
                "profile_error",
            ));
        }
    };
    crate::setup::validate_entries(&entry.setup).map_err(|error| {
        profile_field_error(
            source,
            table_offset,
            name,
            "setup",
            &error.message,
            "profile_error",
        )
    })?;
    let scratch = normalize_scratch(&entry.scratch).map_err(|error| {
        profile_field_error(
            source,
            table_offset,
            name,
            "scratch",
            &error.message,
            "profile_error",
        )
    })?;
    let mut snapshot = serde_json::Map::new();
    snapshot.insert("harness".to_owned(), Value::String("claude".to_owned()));
    snapshot.insert("cmd".to_owned(), Value::String(cmd));
    if let Some(model) = model {
        snapshot.insert("model".to_owned(), Value::String(model.to_owned()));
    }
    snapshot.insert(
        "permissions".to_owned(),
        Value::String(PERMISSION_WORKSPACE_WRITE.to_owned()),
    );
    snapshot.insert(
        "approval_policy".to_owned(),
        Value::String(APPROVAL_POLICY_NEVER.to_owned()),
    );
    if let Some(mode) = permission_mode {
        snapshot.insert("permission_mode".to_owned(), Value::String(mode));
    }
    insert_control(
        &mut snapshot,
        name,
        entry.control.as_deref(),
        source,
        table_offset,
    )?;
    if let Some(effort) = effort {
        snapshot.insert("effort".to_owned(), Value::String(effort));
    }
    freeze_mcp_policy(
        &mut snapshot,
        name,
        entry,
        table_offset,
        source,
        McpHarnessSupport {
            harness: "Claude",
            minimal: true,
            allow: false,
        },
    )?;
    snapshot.insert(
        "pass_env".to_owned(),
        Value::Array(pass_env.into_iter().map(Value::String).collect()),
    );
    snapshot.insert("scratch".to_owned(), json!(scratch));
    if !entry.setup.is_empty() {
        snapshot.insert(
            "setup".to_owned(),
            serde_json::to_value(&entry.setup)
                .map_err(|_| core::Error::new("profile_error", "setup could not be encoded"))?,
        );
    }
    Ok(Value::Object(snapshot))
}

/// Dedicated OpenCode provider ID for adopted local backends. The frozen
/// model on a local profile must use this provider segment
/// (`eliot-local/<api-model-id>`); it has no canonical alias, so a local
/// profile never imports an unrelated canonical credential and project
/// overrides cannot silently retarget the route to another provider.
const OPENCODE_LOCAL_PROVIDER_ID: &str = "eliot-local";

/// Expected native package rendering the adopted OpenAI-compatible route.
/// Pinned by the issue's source review of OpenCode 2.0.32
/// (`packages/ai/src/providers/openai-compatible`).
const OPENCODE_LOCAL_PROVIDER_PACKAGE: &str = "@opencode/ai/providers/openai-compatible";

/// Canonical adopted-backend kinds through the single OpenCode provider
/// path. These are presets and metadata mappings, not separate Agent
/// executors: Ollama and vLLM reuse the same OpenAI-compatible route.
fn normalize_local_backend(raw: &str) -> Option<&'static str> {
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "strata" => Some("strata"),
        "unsloth" | "unsloth-studio" => Some("unsloth"),
        "lmstudio" | "lm-studio" => Some("lmstudio"),
        "llama.cpp" | "llama-cpp" | "llama_cpp" => Some("llama.cpp"),
        "ollama" => Some("ollama"),
        "vllm" => Some("vllm"),
        "generic" => Some("generic"),
        _ => None,
    }
}

fn has_local_backend_fields(entry: &ProfileEntry) -> bool {
    entry.provider_base_url.is_some()
        || entry.api_key_env.is_some()
        || entry.backend.is_some()
        || entry.context_limit.is_some()
        || entry.output_limit.is_some()
}

fn validate_provider_base_url(
    name: &str,
    raw: &str,
    source: &str,
    offset: usize,
) -> core::Result<String> {
    let url = raw.trim();
    if url.is_empty() || url.len() > 2048 || url.chars().any(char::is_control) {
        return Err(profile_field_error(
            source,
            offset,
            name,
            "provider_base_url",
            "must be a non-empty http(s) URL of at most 2048 characters",
            "profile_error",
        ));
    }
    let parsed = reqwest::Url::parse(url).map_err(|_| {
        profile_field_error(
            source,
            offset,
            name,
            "provider_base_url",
            "must be an absolute http(s) endpoint URL without credentials",
            "profile_error",
        )
    })?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none_or(|host| host.is_empty())
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || url.split("://").nth(1).is_some_and(|rest| {
            rest.split(['/', '?', '#'])
                .next()
                .is_some_and(|authority| authority.contains('@'))
        })
    {
        return Err(profile_field_error(
            source,
            offset,
            name,
            "provider_base_url",
            "must be an absolute http(s) endpoint URL without credentials",
            "profile_error",
        ));
    }
    Ok(url.to_owned())
}

fn validate_local_limit(
    name: &str,
    field: &str,
    value: i64,
    source: &str,
    offset: usize,
) -> core::Result<i64> {
    if value <= 0 || value > 100_000_000 {
        return Err(profile_field_error(
            source,
            offset,
            name,
            field,
            "must be a positive token count of at most 100000000",
            "profile_error",
        ));
    }
    Ok(value)
}

/// Freeze the adopted local backend tail of an OpenCode profile. The model
/// must use the dedicated local provider segment, the endpoint/backend/
/// limits must all be explicit, `auth_integration` must be absent (no
/// canonical credential is imported on this path), and an optional
/// `api_key_env` must name an entry of the frozen `pass_env` allowlist
/// (absent means explicit no-auth mode; a missing or empty required key is
/// rejected before startup by the plugin, never silently downgraded).
#[allow(clippy::too_many_arguments)]
fn freeze_local_backend(
    snapshot: &mut serde_json::Map<String, Value>,
    name: &str,
    entry: &ProfileEntry,
    tail: &ProfileTail,
    table_offset: usize,
    source: &str,
) -> core::Result<()> {
    let model = entry
        .model
        .as_deref()
        .expect("OpenCode model validated before local freeze");
    let (provider, _) = split_provider_model(model).ok_or_else(|| {
        profile_field_error(
            source,
            table_offset,
            name,
            "model",
            "must be provider/model with non-empty slash segments and no variant suffix",
            "profile_error",
        )
    })?;
    if provider != OPENCODE_LOCAL_PROVIDER_ID {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "model",
            "an adopted local backend profile must use the dedicated eliot-local provider segment",
            "profile_error",
        ));
    }
    if entry.auth_integration.is_some() {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "auth_integration",
            "an adopted local backend profile must not name a canonical auth_integration",
            "profile_error",
        ));
    }
    let base_url = entry.provider_base_url.as_deref().ok_or_else(|| {
        profile_field_error(
            source,
            table_offset,
            name,
            "provider_base_url",
            "an adopted local backend profile requires an explicit provider_base_url",
            "profile_error",
        )
    })?;
    let base_url = validate_provider_base_url(name, base_url, source, table_offset)?;
    let backend = entry.backend.as_deref().ok_or_else(|| {
        profile_field_error(
            source,
            table_offset,
            name,
            "backend",
            "an adopted local backend profile requires an explicit backend kind",
            "profile_error",
        )
    })?;
    let backend = normalize_local_backend(backend).ok_or_else(|| {
        profile_field_error(
            source,
            table_offset,
            name,
            "backend",
            "must be strata, unsloth, lmstudio, llama.cpp, ollama, vllm or generic",
            "profile_error",
        )
    })?;
    let context_limit = entry.context_limit.ok_or_else(|| {
        profile_field_error(
            source,
            table_offset,
            name,
            "context_limit",
            "an adopted local backend profile requires an explicit context_limit",
            "profile_error",
        )
    })?;
    let output_limit = entry.output_limit.ok_or_else(|| {
        profile_field_error(
            source,
            table_offset,
            name,
            "output_limit",
            "an adopted local backend profile requires an explicit output_limit",
            "profile_error",
        )
    })?;
    let context_limit =
        validate_local_limit(name, "context_limit", context_limit, source, table_offset)?;
    let output_limit =
        validate_local_limit(name, "output_limit", output_limit, source, table_offset)?;
    if let Some(raw) = entry.api_key_env.as_deref() {
        let names = validate_pass_env(name, &[raw.to_owned()], source, table_offset)?;
        let key = names.into_iter().next().ok_or_else(|| {
            profile_field_error(
                source,
                table_offset,
                name,
                "api_key_env",
                "invalid API key environment name",
                "profile_error",
            )
        })?;
        if native_store_selector(&key).is_some() {
            return Err(profile_field_error(
                source,
                table_offset,
                name,
                "api_key_env",
                "API key cannot use a native store selector",
                "profile_error",
            ));
        }
        let allowed = tail.pass_env.iter().any(|allowed| {
            if cfg!(windows) {
                allowed.eq_ignore_ascii_case(&key)
            } else {
                allowed == &key
            }
        });
        if !allowed {
            return Err(profile_field_error(
                source,
                table_offset,
                name,
                "api_key_env",
                "api_key_env must be explicitly allowed by profile pass_env",
                "profile_error",
            ));
        }
        snapshot.insert("api_key_env".to_owned(), Value::String(key));
    }
    snapshot.insert("provider_base_url".to_owned(), Value::String(base_url));
    snapshot.insert("backend".to_owned(), Value::String(backend.to_owned()));
    snapshot.insert(
        "context_limit".to_owned(),
        Value::Number(context_limit.into()),
    );
    snapshot.insert(
        "output_limit".to_owned(),
        Value::Number(output_limit.into()),
    );
    snapshot.insert(
        "provider_package".to_owned(),
        Value::String(OPENCODE_LOCAL_PROVIDER_PACKAGE.to_owned()),
    );
    Ok(())
}

/// Freeze an OpenCode profile. The L6.2 PR1 slice carries an explicit model
/// id in the installed `provider/model` form (split at the first slash) and a required explicit
/// native `cmd` (the Muse treatment: stop and process registration address
/// the exact spawned PID, so `.cmd`/`.bat`/`.ps1` wrappers are rejected).
/// An optional nonsecret `auth_integration` names the one native integration
/// whose canonical credential the pre-serve transfer prepares; the plugin
/// requires it at admission. Effort, approval, collaboration, permission and
/// approval-policy presets have no OpenCode mapping and stay rejected until
/// one lands.
///
/// An adopted local backend profile instead pins `provider_base_url`,
/// `backend`, `context_limit` and `output_limit` with an optional
/// `api_key_env` reference (absent means explicit no-auth mode). The model
/// must use the dedicated `eliot-local` provider segment, and
/// `auth_integration` must be absent so no unrelated canonical credential
/// is imported. Key values stay runtime-only; only the env name is frozen.
fn opencode_snapshot_from_entry(
    name: &str,
    entry: &ProfileEntry,
    table_offset: usize,
    source: &str,
) -> core::Result<Value> {
    let model = entry.model.as_deref();
    if let Some(model) = model {
        validate_model(name, model, source, table_offset)?;
    }
    if model.is_some_and(|model| split_provider_model(model).is_none()) {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "model",
            "must be provider/model with non-empty slash segments and no variant suffix",
            "profile_error",
        ));
    }
    for field in [
        "effort",
        "approval",
        "collaboration_mode",
        "permissions",
        "approval_policy",
    ] {
        let present = match field {
            "effort" => entry.effort.as_deref(),
            "approval" => entry.approval.as_deref(),
            "collaboration_mode" => entry.collaboration_mode.as_deref(),
            "permissions" => entry.permissions.as_deref(),
            _ => entry.approval_policy.as_deref(),
        };
        if present.is_some() {
            return Err(profile_field_error(
                source,
                table_offset,
                name,
                field,
                "the OpenCode slice has no mapping for this preset",
                "unsupported",
            ));
        }
    }
    if entry.mode.is_some() || entry.dangerously_skip_permissions.is_some() {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "mode",
            "the OpenCode slice has no mapping for this preset",
            "unsupported",
        ));
    }
    if let Some(selector) = entry.auth_integration.as_deref() {
        let trimmed = selector.trim();
        if trimmed.is_empty()
            || trimmed.len() > MAX_AUTH_INTEGRATION_LEN
            || trimmed.starts_with('-')
            || trimmed.chars().any(|value| value.is_control())
        {
            return Err(profile_field_error(
                source,
                table_offset,
                name,
                "auth_integration",
                "must be a non-empty integration ID or name of at most 128 bytes with no control characters and no leading dash",
                "profile_error",
            ));
        }
    }
    let tail = validate_profile_tail(name, entry, table_offset, source)?;
    let mut snapshot = serde_json::Map::new();
    snapshot.insert("harness".to_owned(), Value::String("opencode".to_owned()));
    if let Some(model) = model {
        snapshot.insert("model".to_owned(), Value::String(model.to_owned()));
    }
    if has_local_backend_fields(entry) {
        if model.is_none() {
            return Err(profile_field_error(
                source,
                table_offset,
                name,
                "model",
                "required non-empty model is missing for adopted local backends",
                "profile_error",
            ));
        }
        freeze_local_backend(&mut snapshot, name, entry, &tail, table_offset, source)?;
    } else if let Some(selector) = entry.auth_integration.as_deref() {
        snapshot.insert(
            "auth_integration".to_owned(),
            Value::String(selector.trim().to_owned()),
        );
    }
    freeze_mcp_policy(
        &mut snapshot,
        name,
        entry,
        table_offset,
        source,
        McpHarnessSupport {
            harness: "OpenCode",
            minimal: true,
            allow: false,
        },
    )?;
    insert_control(
        &mut snapshot,
        name,
        entry.control.as_deref(),
        source,
        table_offset,
    )?;
    insert_profile_tail(&mut snapshot, tail, &entry.setup)?;
    Ok(Value::Object(snapshot))
}

/// Freeze an Antigravity profile. The L6.4 slice carries an exact model id, a
/// required explicit native `cmd`, an optional native `mode` and an explicit
/// `dangerously_skip_permissions` flag. Effort, approval, collaboration,
/// permission and approval-policy presets have no mapping and stay rejected.
fn antigravity_snapshot_from_entry(
    name: &str,
    entry: &ProfileEntry,
    table_offset: usize,
    source: &str,
) -> core::Result<Value> {
    let model = entry.model.as_deref();
    if let Some(model) = model {
        validate_model(name, model, source, table_offset)?;
    }
    if entry
        .effort
        .as_deref()
        .is_some_and(|effort| !effort.trim().is_empty())
    {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "effort",
            "the Antigravity slice has no reasoning-effort mapping",
            "unsupported",
        ));
    }
    let mode = entry.mode.as_deref().map(str::trim);
    if mode.is_some_and(|mode| !matches!(mode, "plan" | "auto" | "accept-edits")) {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "mode",
            "must be plan, auto, or accept-edits",
            "profile_error",
        ));
    }
    for field in [
        "approval",
        "collaboration_mode",
        "permissions",
        "approval_policy",
    ] {
        let present = match field {
            "approval" => entry.approval.as_deref(),
            "collaboration_mode" => entry.collaboration_mode.as_deref(),
            "permissions" => entry.permissions.as_deref(),
            _ => entry.approval_policy.as_deref(),
        };
        if present.is_some() {
            return Err(profile_field_error(
                source,
                table_offset,
                name,
                field,
                "the Antigravity slice has no mapping for this preset",
                "unsupported",
            ));
        }
    }
    if entry.auth_integration.is_some() {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "auth_integration",
            "the Antigravity slice has no mapping for this preset",
            "unsupported",
        ));
    }
    let tail = validate_profile_tail(name, entry, table_offset, source)?;
    let mut snapshot = serde_json::Map::new();
    snapshot.insert(
        "harness".to_owned(),
        Value::String("antigravity".to_owned()),
    );
    if let Some(model) = model {
        snapshot.insert("model".to_owned(), Value::String(model.to_owned()));
    }
    if let Some(mode) = mode {
        snapshot.insert("mode".to_owned(), Value::String(mode.to_owned()));
    }
    snapshot.insert(
        "dangerously_skip_permissions".to_owned(),
        Value::Bool(entry.dangerously_skip_permissions.unwrap_or(false)),
    );
    freeze_mcp_policy(
        &mut snapshot,
        name,
        entry,
        table_offset,
        source,
        McpHarnessSupport {
            harness: "Antigravity",
            minimal: false,
            allow: false,
        },
    )?;
    insert_control(
        &mut snapshot,
        name,
        entry.control.as_deref(),
        source,
        table_offset,
    )?;
    insert_profile_tail(&mut snapshot, tail, &entry.setup)?;
    Ok(Value::Object(snapshot))
}

fn bind_route_and_capacity(
    profile_name: &str,
    entry: &ProfileEntry,
    config: &FileConfig,
    table_offset: usize,
    source: &str,
    mut snapshot: Value,
) -> core::Result<Value> {
    let fail = |field: &str, message: &str| {
        profile_field_error(
            source,
            table_offset,
            profile_name,
            field,
            message,
            "profile_error",
        )
    };
    let mut scopes = vec![core::CapacityScope {
        scope: format!("profile:{profile_name}"),
        max_live: entry.max_live,
    }];

    if let Some(account_name) = entry.account.as_deref() {
        validate_profile_name(account_name)
            .map_err(|_| fail("account", "invalid account reference"))?;
        let account = config
            .accounts
            .get(account_name)
            .ok_or_else(|| fail("account", "named account was not found"))?;
        scopes.push(core::CapacityScope {
            scope: format!("account:{account_name}"),
            max_live: account.get_ref().max_live,
        });
        snapshot["account"] = json!(account_name);
    }

    if let Some(proxy_name) = entry.proxy.as_deref() {
        if entry.harness.trim() != "codex" {
            return Err(fail(
                "proxy",
                "adopted proxy routes require the Codex harness",
            ));
        }
        validate_profile_name(proxy_name).map_err(|_| fail("proxy", "invalid proxy reference"))?;
        if is_builtin_codex_provider(proxy_name) {
            return Err(fail(
                "proxy",
                "proxy name collides with a builtin Codex provider",
            ));
        }
        let proxy = config
            .proxies
            .get(proxy_name)
            .ok_or_else(|| fail("proxy", "named adopted proxy was not found"))?
            .get_ref();

        // Reasoning effort is optional on a routed Codex profile: an omitted
        // effort leaves the snapshot without an `effort` key, and the native
        // plugin omits the wire field to preserve the provider/native
        // default. No effort value is fabricated here.
        let client_key_env_name = proxy
            .api_key_env
            .as_deref()
            .map(|raw_name| {
                let names =
                    validate_pass_env(profile_name, &[raw_name.to_owned()], source, table_offset)?;
                let name = names
                    .into_iter()
                    .next()
                    .ok_or_else(|| fail("proxy", "invalid client key environment name"))?;
                if native_store_selector(&name).is_some() {
                    return Err(fail(
                        "proxy",
                        "client key cannot use a native store selector",
                    ));
                }
                let allowed = snapshot["pass_env"].as_array().is_some_and(|values| {
                    values.iter().filter_map(Value::as_str).any(|allowed| {
                        if cfg!(windows) {
                            allowed.eq_ignore_ascii_case(&name)
                        } else {
                            allowed == name.as_str()
                        }
                    })
                });
                if !allowed {
                    return Err(fail(
                        "proxy",
                        "api_key_env must be explicitly allowed by profile pass_env",
                    ));
                }
                Ok(name)
            })
            .transpose()?;

        let route = core::RouteBinding {
            name: proxy_name.to_owned(),
            endpoint: proxy.attach.clone(),
            readiness_path: proxy.health.clone(),
            provider_base_url: proxy.base_url_for.codex.clone(),
            client_key_env_name,
        };
        route
            .validate()
            .map_err(|_| fail("proxy", "invalid adopted route binding"))?;

        scopes.push(core::CapacityScope {
            scope: format!("proxy:{proxy_name}"),
            max_live: proxy.max_live,
        });
        snapshot["proxy"] = json!(proxy_name);
        snapshot["route_binding"] = serde_json::to_value(route)
            .map_err(|_| fail("proxy", "adopted route binding could not be encoded"))?;
    }

    for scope in &scopes {
        scope
            .validate()
            .map_err(|_| fail("max_live", "invalid capacity scope"))?;
    }
    snapshot["capacity_scopes"] = serde_json::to_value(scopes)
        .map_err(|_| fail("max_live", "capacity scopes could not be encoded"))?;
    Ok(snapshot)
}

fn is_builtin_codex_provider(name: &str) -> bool {
    [
        "openai",
        "amazon-bedrock",
        "amazon-bedrock-runtime",
        "ollama",
        "lmstudio",
        "ollama-chat",
    ]
    .iter()
    .any(|provider| name.eq_ignore_ascii_case(provider))
}

fn default_scratch() -> Vec<String> {
    vec!["target/".to_owned(), "node_modules/".to_owned()]
}

fn normalize_scratch(paths: &[String]) -> core::Result<Vec<String>> {
    if paths.len() > 64 {
        return Err(core::Error::new(
            "profile_error",
            "scratch exceeds 64 directories",
        ));
    }
    let mut normalized = Vec::new();
    let mut seen = HashSet::new();
    for path in paths {
        let portable = path.replace('\\', "/");
        if portable.is_empty()
            || portable.len() > 4096
            || portable.starts_with('/')
            || portable.contains(':')
            || portable.chars().any(char::is_control)
        {
            return Err(core::Error::new(
                "profile_error",
                "scratch entries must be bounded relative directories",
            ));
        }
        let parts: Vec<_> = portable
            .split('/')
            .filter(|part| !part.is_empty())
            .collect();
        if parts.is_empty() || parts.iter().any(|part| matches!(*part, "." | "..")) {
            return Err(core::Error::new(
                "profile_error",
                "scratch entries cannot name the root or contain dot traversal",
            ));
        }
        let directory = format!("{}/", parts.join("/"));
        #[cfg(windows)]
        let key = directory.to_ascii_lowercase();
        #[cfg(not(windows))]
        let key = directory.clone();
        if seen.insert(key) {
            normalized.push(directory);
        }
    }
    Ok(normalized)
}

fn validate_profile_name(name: &str) -> core::Result<()> {
    if name.is_empty() || name.len() > MAX_PROFILE_NAME_LEN {
        return Err(core::Error::new(
            "profile_error",
            "profile name must be 1-128 characters",
        ));
    }
    let ok = name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !ok {
        return Err(core::Error::new(
            "profile_error",
            "profile name may only contain letters, digits, hyphen and underscore",
        ));
    }
    Ok(())
}
fn validate_model(profile: &str, model: &str, source: &str, offset: usize) -> core::Result<()> {
    if model.is_empty()
        || model.len() > MAX_MODEL_LEN
        || model.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return Err(profile_field_error(
            source,
            offset,
            profile,
            "model",
            "must be a nonempty single token of at most 256 characters",
            "profile_error",
        ));
    }
    Ok(())
}
fn validate_pass_env(
    profile: &str,
    names: &[String],
    source: &str,
    offset: usize,
) -> core::Result<Vec<String>> {
    if names.len() > MAX_PASS_ENV_ENTRIES {
        return Err(profile_field_error(
            source,
            offset,
            profile,
            "pass_env",
            "exceeds 64 entries",
            "profile_error",
        ));
    }
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(names.len());
    for raw in names {
        let name = raw.trim();
        if !valid_environment_name(name) {
            return Err(profile_field_error(
                source,
                offset,
                profile,
                "pass_env",
                "names must be 1-256 characters",
                "profile_error",
            ));
        }
        if name.to_ascii_uppercase().starts_with("ELIOT_AGENT_") {
            return Err(profile_field_error(
                source,
                offset,
                profile,
                "pass_env",
                "cannot override Eliot identity variables",
                "profile_error",
            ));
        }
        if seen.insert(name.to_owned()) {
            out.push(name.to_owned());
        }
    }
    Ok(out)
}
/// Frozen command scope of Agents launched from this profile: `self`
/// (default), `tree`, or `project`. Stored on the Agent row at launch.
/// The valid values live only in `AgentControl::parse`.
fn validated_control<'a>(
    profile: &str,
    control: Option<&'a str>,
    source: &str,
    offset: usize,
) -> core::Result<&'a str> {
    let control = control.map(str::trim).unwrap_or("self");
    if core::AgentControl::parse(control).is_err() {
        return Err(profile_field_error(
            source,
            offset,
            profile,
            "control",
            "must be self, tree, or project",
            "profile_error",
        ));
    }
    Ok(control)
}

/// The one frozen `control` insert for resolved profile snapshots.
fn insert_control(
    snapshot: &mut serde_json::Map<String, Value>,
    profile: &str,
    control: Option<&str>,
    source: &str,
    offset: usize,
) -> core::Result<()> {
    let control = validated_control(profile, control, source, offset)?;
    snapshot.insert("control".to_owned(), Value::String(control.to_owned()));
    Ok(())
}

/// Which worker MCP policy pieces a harness plugin implements.
struct McpHarnessSupport<'a> {
    harness: &'a str,
    minimal: bool,
    allow: bool,
}

/// Validate the worker MCP inheritance policy and freeze it with the profile
/// snapshot. `minimal`/`allow` name whether the harness plugin implements an
/// isolation mechanism for `minimal` (Codex, Claude, OpenCode) and selective
/// inclusion of named native servers (Codex only: Claude and OpenCode
/// minimal are the Eliot-only server surface with no allow mapping, so a
/// non-empty allow list would be silently dropped); Muse and Antigravity
/// keep `inherit` only and reject `minimal` as unsupported.
/// A non-empty allow list always requires `minimal`, so an allowed set
/// cannot silently ride on the default.
fn freeze_mcp_policy(
    snapshot: &mut serde_json::Map<String, Value>,
    name: &str,
    entry: &ProfileEntry,
    table_offset: usize,
    source: &str,
    support: McpHarnessSupport<'_>,
) -> core::Result<()> {
    let harness = support.harness;
    let supports_minimal = support.minimal;
    let supports_allow = support.allow;
    // An unset `mcp` takes the default `minimal` only where the harness maps
    // it; Muse and Antigravity have no minimal mapping, so their profiles
    // keep loading as `inherit` instead of failing after the default change.
    let policy = match entry.mcp.as_deref() {
        None if !supports_minimal => core::McpPolicy::Inherit,
        raw => core::McpPolicy::parse(raw).map_err(|error| {
            profile_field_error(
                source,
                table_offset,
                name,
                "mcp",
                &error.message,
                "profile_error",
            )
        })?,
    };
    let allow =
        core::validate_allow_list(entry.mcp_allow.as_deref().unwrap_or(&[])).map_err(|error| {
            profile_field_error(
                source,
                table_offset,
                name,
                "mcp_allow",
                &error.message,
                "profile_error",
            )
        })?;
    if !allow.is_empty() && policy == core::McpPolicy::Inherit {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "mcp_allow",
            "mcp_allow requires mcp = \"minimal\"",
            "profile_error",
        ));
    }
    if policy == core::McpPolicy::Minimal && !supports_minimal {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "mcp",
            &format!("mcp = \"minimal\" has no {harness} mapping"),
            "unsupported",
        ));
    }
    if !allow.is_empty() && !supports_allow {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "mcp_allow",
            &format!("mcp_allow has no {harness} mapping"),
            "unsupported",
        ));
    }
    snapshot.insert("mcp".to_owned(), Value::String(policy.as_str().to_owned()));
    snapshot.insert(
        "mcp_allow".to_owned(),
        Value::Array(allow.into_iter().map(Value::String).collect()),
    );
    Ok(())
}

/// The Muse and Claude plugins own their native child by handle, so a script
/// wrapper would leave the real host/CLI running on stop; Codex lives in an
/// Agent Job that ends the tree.
fn reject_script_wrapper(
    profile: &str,
    cmd: String,
    source: &str,
    offset: usize,
) -> core::Result<String> {
    let lower = cmd.to_ascii_lowercase();
    if lower.ends_with(".cmd") || lower.ends_with(".bat") || lower.ends_with(".ps1") {
        return Err(profile_field_error(
            source,
            offset,
            profile,
            "cmd",
            "must name one native executable path, not a .cmd/.bat/.ps1 wrapper",
            "profile_error",
        ));
    }
    Ok(cmd)
}

/// Claude is a native executable: its cmd must be an absolute path (no PATH
/// lookup, so no bare `claude`), and on Windows it must name the `.exe`
/// itself. A script wrapper (`.js` here, `.cmd`/`.bat`/`.ps1` above) would
/// orphan the real CLI on stop, when only the wrapper PID is owned.
fn require_native_claude_cmd(
    profile: &str,
    cmd: String,
    source: &str,
    offset: usize,
) -> core::Result<String> {
    if !std::path::Path::new(&cmd).is_absolute() {
        return Err(profile_field_error(
            source,
            offset,
            profile,
            "cmd",
            "must be an absolute native executable path, not a bare command name",
            "profile_error",
        ));
    }
    let lower = cmd.to_ascii_lowercase();
    if lower.ends_with(".js") {
        return Err(profile_field_error(
            source,
            offset,
            profile,
            "cmd",
            "must name the native executable, not a .js script wrapper",
            "profile_error",
        ));
    }
    if cfg!(windows) && !lower.ends_with(".exe") {
        return Err(profile_field_error(
            source,
            offset,
            profile,
            "cmd",
            "must name the native .exe, not a script wrapper",
            "profile_error",
        ));
    }
    Ok(cmd)
}

fn validate_cmd(profile: &str, cmd: &str, source: &str, offset: usize) -> core::Result<String> {
    let cmd = cmd.trim();
    if cmd.is_empty() || cmd.len() > MAX_CMD_LEN {
        return Err(profile_field_error(
            source,
            offset,
            profile,
            "cmd",
            "must be 1-1024 characters",
            "profile_error",
        ));
    }
    const BACKSLASH: char = '\\';
    let file = cmd.rsplit(['/', BACKSLASH]).next().unwrap_or(cmd);
    let lower = file.to_ascii_lowercase();
    let stem = lower.strip_suffix(".exe").unwrap_or(&lower);
    const SHELLS: &[&str] = &[
        "sh",
        "bash",
        "dash",
        "zsh",
        "fish",
        "cmd",
        "powershell",
        "pwsh",
    ];
    let has_path_separator = cmd.contains('/') || cmd.contains(BACKSLASH);
    if SHELLS.contains(&stem)
        || (cmd.chars().any(char::is_whitespace)
            && (!has_path_separator || file.chars().any(char::is_whitespace)))
        || cmd.chars().any(char::is_control)
    {
        return Err(profile_field_error(
            source,
            offset,
            profile,
            "cmd",
            "must name one executable path, not a shell command",
            "profile_error",
        ));
    }
    Ok(cmd.to_owned())
}
fn profile_field_error(
    source: &str,
    offset: usize,
    profile: &str,
    field: &str,
    class: &str,
    code: &str,
) -> core::Error {
    config_error_at(
        source,
        offset,
        &format!("profiles.{profile}.{field}"),
        class,
        code,
    )
}
fn config_error_at(source: &str, offset: usize, key: &str, class: &str, code: &str) -> core::Error {
    core::Error::new(
        code,
        format!("{}: {key}: {class}", source_location(source, Some(offset))),
    )
}
fn source_location(source: &str, offset: Option<usize>) -> String {
    let Some(offset) = offset else {
        return "config.toml:unknown location".to_owned();
    };
    let prefix = source.get(..offset.min(source.len())).unwrap_or("");
    let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
    let column = prefix.rsplit('\n').next().unwrap_or("").chars().count() + 1;
    format!("config.toml:{line}:{column}")
}
fn map_toml_error(source: &str, error: toml::de::Error) -> core::Error {
    let span = error.span();
    let location = source_location(source, span.as_ref().map(|range| range.start));
    if let Some(field) = span
        .as_ref()
        .and_then(|span| decoded_key_at_span(source, span))
    {
        if is_unsupported_field(&field) {
            return core::Error::new(
                "unsupported",
                format!("{location}: {field}: unsupported configuration field"),
            );
        }
        return core::Error::new(
            "profile_error",
            format!("{location}: {field}: unknown configuration field"),
        );
    }
    let key = span
        .as_ref()
        .and_then(|range| key_at_offset(source, range.start))
        .unwrap_or("configuration");
    core::Error::new(
        "profile_error",
        format!("{location}: {key}: invalid TOML syntax or value"),
    )
}
fn decoded_key_at_span(source: &str, span: &std::ops::Range<usize>) -> Option<String> {
    let parsed = toml::de::DeTable::parse(source).ok()?;
    decoded_key_at_span_in_table(parsed.get_ref(), span)
}
fn decoded_key_at_span_in_table(
    table: &toml::de::DeTable<'_>,
    span: &std::ops::Range<usize>,
) -> Option<String> {
    for (key, value) in table.iter() {
        let key_span = key.span();
        if key_span.start == span.start && key_span.end == span.end {
            let decoded = key.get_ref().as_ref();
            return safe_config_key(decoded).then(|| decoded.to_owned());
        }
        if let Some(decoded) = decoded_key_at_span_in_value(value.get_ref(), span) {
            return Some(decoded);
        }
    }
    None
}
fn decoded_key_at_span_in_value(
    value: &toml::de::DeValue<'_>,
    span: &std::ops::Range<usize>,
) -> Option<String> {
    if let Some(table) = value.as_table()
        && let Some(decoded) = decoded_key_at_span_in_table(table, span)
    {
        return Some(decoded);
    }
    if let Some(array) = value.as_array() {
        for item in array.as_ref() {
            if let Some(decoded) = decoded_key_at_span_in_value(item.get_ref(), span) {
                return Some(decoded);
            }
        }
    }
    None
}
fn key_at_offset(source: &str, offset: usize) -> Option<&str> {
    let prefix = source.get(..offset.min(source.len()))?;
    for line in prefix.lines().rev().map(str::trim) {
        if line.starts_with('[') {
            break;
        }
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((raw_key, _)) = line.split_once('=') {
            let key = raw_key
                .trim()
                .trim_matches(|character| character == '\'' || character == '"');
            if safe_config_key(key) {
                return Some(key);
            }
        }
    }
    None
}
fn safe_config_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= MAX_PROFILE_NAME_LEN
        && key
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_.-".contains(character))
}
fn is_unsupported_field(field: &str) -> bool {
    const FIELDS: &[&str] = &[
        "route",
        "routes",
        "extends",
        "registry",
        "registries",
        "overlay",
        "overlays",
        "project_overlay",
        "project_overlays",
        "secret",
        "secrets",
        "secret_env",
        "secret_values",
        "api_key",
        "api_key_env",
        "token",
        "password",
        "daemon",
        "mail",
        "service",
        "services",
        "plugin",
        "plugins",
        "project",
        "projects",
        "worktree",
        "frozen",
        "rule",
        "automation",
        "inherit_env",
        "workspace_mode",
        "git_identity",
    ];
    FIELDS
        .iter()
        .any(|candidate| field.eq_ignore_ascii_case(candidate))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct AutomationConfigFixture {
        root: std::path::PathBuf,
    }

    impl AutomationConfigFixture {
        fn new() -> Self {
            let root =
                std::env::temp_dir().join(format!("eliot-automation-config-{}", ulid::Ulid::new()));
            std::fs::create_dir(&root).expect("create automation config fixture");
            Self { root }
        }

        fn write(&self, source: &str) {
            std::fs::write(self.root.join("config.toml"), source)
                .expect("write automation config fixture");
        }
    }

    impl Drop for AutomationConfigFixture {
        fn drop(&mut self) {
            let base = std::env::temp_dir();
            if self.root.parent() == Some(base.as_path())
                && self
                    .root
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("eliot-automation-config-"))
            {
                let _ = std::fs::remove_dir_all(&self.root);
            }
        }
    }

    #[test]
    fn automation_backlog_defaults_and_coexists_with_project_registrations() {
        let fixture = AutomationConfigFixture::new();
        assert_eq!(
            load_automation_config(&fixture.root).unwrap(),
            AutomationConfig::default()
        );

        fixture.write("[automation]\n");
        assert_eq!(
            load_automation_config(&fixture.root).unwrap(),
            AutomationConfig::default()
        );

        let project_path = format!("{:?}", fixture.root.to_string_lossy());
        fixture.write(&format!(
            "[profiles.fixture]\nharness = \"codex\"\nmodel = \"fixture-model\"\n\
             [projects.sample]\npath = {project_path}\nprofiles = ['fixture']\nactions = ['stop']\n\
             [automation]\nmax_event_backlog = 4096\n"
        ));
        assert_eq!(
            load_automation_config(&fixture.root).unwrap(),
            AutomationConfig {
                max_event_backlog: 4096,
                run: RunGrantConfig::default(),
                assignments: BTreeMap::new(),
            }
        );
        assert_eq!(
            load_project_registration_config(&fixture.root).unwrap(),
            vec![ProjectRegistrationConfig {
                id: "sample".to_owned(),
                path: fixture.root.to_string_lossy().into_owned(),
                profiles: vec!["fixture".to_owned()],
                actions: vec!["stop".to_owned()],
                git: None,
                interpreters: BTreeMap::new(),
                run_env: Vec::new(),
            }]
        );
    }

    #[test]
    fn routing_accepts_ordered_extensible_roles_and_rejects_bad_candidates() {
        let source = "[profiles.codex-luna]\nharness='codex'\nmodel='gpt-6-luna'\n\
            [routing]\ndefault=['codex-luna']\nworker=['codex-luna']\n";
        let config = parse_config(source).unwrap();
        assert_eq!(
            config.routing.candidates("default").unwrap(),
            ["codex-luna"]
        );
        assert_eq!(config.routing.candidates("worker").unwrap(), ["codex-luna"]);
        assert!(config.routing.candidates("missing").is_none());

        for routing in [
            "default=['codex-luna','codex-luna']",
            "worker=['missing']",
            "\"\"=['codex-luna']",
            "bad.role=['codex-luna']",
        ] {
            let source = format!(
                "[profiles.codex-luna]\nharness='codex'\nmodel='gpt-6-luna'\n\
                 [routing]\n{routing}\n"
            );
            assert!(parse_config(&source).is_err(), "accepted {routing}");
        }
    }

    #[test]
    fn automation_assignments_validate_and_load_from_settings_shape() {
        let fixture = AutomationConfigFixture::new();
        fixture.write(
            "[automation.assignments.review-queue]\n\
             folder='C:/work/briefs'\nworkspace='C:/work/eliot'\n\
             role='worker'\nmax_busy=2\ninterval='30s'\n",
        );
        let config = load_automation_config(&fixture.root).unwrap();
        let assignment = config.assignments.get("review-queue").unwrap();
        assert_eq!(assignment.folder, "C:/work/briefs");
        assert_eq!(assignment.workspace, "C:/work/eliot");
        assert_eq!(assignment.role, "worker");
        assert_eq!(assignment.max_busy, 2);
        assert_eq!(assignment.interval, "30s");
        assert!(
            !assignment.cleanup_on_retire,
            "retirement cleanup stays off unless the assignment opts in"
        );

        fixture.write(
            "[automation.assignments.review-queue]\n\
             folder='C:/work/briefs'\nworkspace='C:/work/eliot'\n\
             role='worker'\nmax_busy=2\ninterval='30s'\ncleanup_on_retire=true\n",
        );
        let opted = load_automation_config(&fixture.root)
            .unwrap()
            .assignments
            .remove("review-queue")
            .unwrap();
        assert!(opted.cleanup_on_retire);

        fixture.write(
            "[automation.assignments.review-queue]\n\
             folder='C:/work/briefs'\nworkspace='C:/work/eliot'\n\
             role='worker'\nmax_busy=0\ninterval='30s'\n",
        );
        assert!(load_automation_config(&fixture.root).is_err());
    }

    #[test]
    fn daemon_retention_defaults_and_parses_windows() {
        let fixture = AutomationConfigFixture::new();
        assert_eq!(
            load_daemon_config(&fixture.root).unwrap(),
            DaemonConfig::default()
        );
        // Unset windows disable pruning: no deletion by default.
        assert_eq!(DaemonConfig::default().retention.events_days, None);
        assert_eq!(DaemonConfig::default().retention.commands_days, None);
        assert_eq!(DaemonConfig::default().retention.mail_days, None);

        fixture.write("[daemon]\n");
        assert_eq!(
            load_daemon_config(&fixture.root).unwrap(),
            DaemonConfig::default()
        );

        fixture.write(
            "[daemon]\nretention.events_days = 7\nretention.commands_days = 14\nretention.mail_days = 21\n",
        );
        assert_eq!(
            load_daemon_config(&fixture.root).unwrap(),
            DaemonConfig {
                artifact_roots: Vec::new(),
                retention: RetentionConfig {
                    events_days: Some(7),
                    commands_days: Some(14),
                    mail_days: Some(21),
                },
                limits: core::api::HierarchyLimits::default(),
                attention_suspicion_secs: 15 * 60,
                log_level: default_log_level(),
                heavy_wall_secs: default_heavy_wall_secs(),
                heavy_cpu_secs: default_heavy_cpu_secs(),
                heavy_rss_bytes: default_heavy_rss_bytes(),
                session_growth_ratio_permille: default_session_growth_ratio_permille(),
                session_file_bytes: default_session_file_bytes(),
            }
        );

        fixture.write("[daemon]\nretention.events_days = 7\n");
        assert_eq!(
            load_daemon_config(&fixture.root).unwrap(),
            DaemonConfig {
                artifact_roots: Vec::new(),
                retention: RetentionConfig {
                    events_days: Some(7),
                    commands_days: None,
                    mail_days: None,
                },
                limits: core::api::HierarchyLimits::default(),
                attention_suspicion_secs: 15 * 60,
                log_level: default_log_level(),
                heavy_wall_secs: default_heavy_wall_secs(),
                heavy_cpu_secs: default_heavy_cpu_secs(),
                heavy_rss_bytes: default_heavy_rss_bytes(),
                session_growth_ratio_permille: default_session_growth_ratio_permille(),
                session_file_bytes: default_session_file_bytes(),
            }
        );
    }

    #[test]
    fn daemon_log_level_defaults_parses_and_rejects() {
        let fixture = AutomationConfigFixture::new();
        assert_eq!(DaemonConfig::default().log_level, "standard");
        assert_eq!(
            load_daemon_config(&fixture.root).unwrap().log_level,
            "standard"
        );
        fixture.write("[daemon]\nlog_level = \"debug\"\n");
        assert_eq!(
            load_daemon_config(&fixture.root).unwrap().log_level,
            "debug"
        );
        for source in [
            "[daemon]\nlog_level = \"verbose\"\n",
            "[daemon]\nlog_level = 1\n",
        ] {
            fixture.write(source);
            let error = load_daemon_config(&fixture.root).unwrap_err();
            assert_eq!(error.code, "profile_error", "source: {source}");
        }
    }

    #[test]
    fn daemon_limits_default_and_validate() {
        let fixture = AutomationConfigFixture::new();
        fixture.write("[daemon]\nretention.events_days = 7\n");
        let config = load_daemon_config(&fixture.root).unwrap();
        assert_eq!(config.limits, core::api::HierarchyLimits::default());
        assert_eq!(
            config.limits,
            core::api::HierarchyLimits {
                max_depth: 8,
                max_children_per_agent: 20,
                max_tree_agents: 100,
                max_selector_targets: 1000,
            }
        );
        fixture.write(
            "[daemon]\nlimits.max_depth = 4\nlimits.max_children_per_agent = 5\n\
             limits.max_tree_agents = 40\nlimits.max_selector_targets = 50\n",
        );
        let config = load_daemon_config(&fixture.root).unwrap();
        assert_eq!(
            config.limits,
            core::api::HierarchyLimits {
                max_depth: 4,
                max_children_per_agent: 5,
                max_tree_agents: 40,
                max_selector_targets: 50,
            }
        );
        for source in [
            "[daemon]\nlimits.max_depth = 0\n",
            "[daemon]\nlimits.max_children_per_agent = -1\n",
            "[daemon]\nlimits.max_tree_agents = 0\n",
            "[daemon]\nlimits.max_selector_targets = 0\n",
            "[daemon]\nlimits.max_depth = \"8\"\n",
            "[daemon]\nlimits.unknown = 1\n",
        ] {
            fixture.write(source);
            let error = load_daemon_config(&fixture.root).unwrap_err();
            assert_eq!(error.code, "profile_error", "source: {source}");
        }
    }

    #[test]
    fn profile_cache_reuses_revision_and_revalidates_on_edit() {
        let fixture = AutomationConfigFixture::new();
        fixture.write("[profiles.solo]\nharness = \"codex\"\nmodel = \"m1\"\n");
        let cache = Mutex::new(ProfileCache::default());
        let first = resolve_profile_cached(&cache, &fixture.root, "solo").unwrap();
        assert_eq!(first.get("model").and_then(Value::as_str), Some("m1"));
        let second = resolve_profile_cached(&cache, &fixture.root, "solo").unwrap();
        assert_eq!(first, second);
        fixture.write("[profiles.solo]\nharness = \"codex\"\nmodel = \"m2\"\n");
        let third = resolve_profile_cached(&cache, &fixture.root, "solo").unwrap();
        assert_eq!(third.get("model").and_then(Value::as_str), Some("m2"));
        fixture.write("[profiles.solo]\nharness = \"nope\"\nmodel = \"m2\"\n");
        let error = resolve_profile_cached(&cache, &fixture.root, "solo").unwrap_err();
        assert_eq!(error.code, "unsupported");
    }

    #[test]
    fn profile_control_freezes_and_validates() {
        let fixture = AutomationConfigFixture::new();
        fixture.write(
            "[profiles.manager]\nharness = \"codex\"\nmodel = \"fixture-model\"\ncontrol = \"tree\"\n",
        );
        let snapshot = resolve_profile(&fixture.root, "manager").unwrap();
        assert_eq!(
            snapshot.get("control").and_then(Value::as_str),
            Some("tree")
        );
        fixture.write("[profiles.solo]\nharness = \"codex\"\nmodel = \"fixture-model\"\n");
        let snapshot = resolve_profile(&fixture.root, "solo").unwrap();
        assert_eq!(
            snapshot.get("control").and_then(Value::as_str),
            Some("self")
        );
        fixture.write(
            "[profiles.muse-manager]\nharness = \"muse\"\nmodel = \"fixture-model\"\nmcp = \"inherit\"\ncontrol = \"project\"\n\
             cmd = \"C:/Tools/MuseCode/muse-bin-1.4.0-R4161.1.exe\"\n",
        );
        let snapshot = resolve_profile(&fixture.root, "muse-manager").unwrap();
        assert_eq!(
            snapshot.get("control").and_then(Value::as_str),
            Some("project")
        );
        fixture.write(
            "[profiles.claude-manager]\nharness='claude'\ncmd='C:/Tools/Claude/claude.exe'\ncontrol='tree'\n",
        );
        assert_eq!(
            resolve_profile(&fixture.root, "claude-manager").unwrap()["control"],
            "tree"
        );
        fixture.write(
            "[profiles.claude-manager]\nharness='claude'\ncmd='C:/Tools/Claude/claude.exe'\ncontrol='fleet'\n",
        );
        assert_eq!(
            resolve_profile(&fixture.root, "claude-manager")
                .unwrap_err()
                .code,
            "profile_error"
        );
        fixture.write(
            "[profiles.fleet]\nharness = \"codex\"\nmodel = \"fixture-model\"\ncontrol = \"fleet\"\n",
        );
        let error = resolve_profile(&fixture.root, "fleet").unwrap_err();
        assert_eq!(error.code, "profile_error");
        assert!(error.message.contains("control"), "{}", error.message);
    }

    #[test]
    fn child_profiles_freeze_and_validate() {
        let fixture = AutomationConfigFixture::new();
        fixture.write(
            "[profiles.manager]\nharness = \"codex\"\nmodel = \"fixture-model\"\nchild_profiles = [\"worker\"]\n\
             [profiles.worker]\nharness = \"codex\"\nmodel = \"fixture-cheap\"\n",
        );
        let snapshot = resolve_profile(&fixture.root, "manager").unwrap();
        assert_eq!(snapshot["child_profiles"], json!(["worker"]));
        let worker = resolve_profile(&fixture.root, "worker").unwrap();
        assert_eq!(worker["child_profiles"], json!([]));
        fixture.write(
            "[profiles.manager]\nharness = \"codex\"\nchild_profiles = [\"worker\"]\n\
             [profiles.worker]\nharness = \"muse\"\ncmd = \"C:/Tools/Muse/muse.exe\"\n",
        );
        assert_eq!(
            resolve_profile(&fixture.root, "manager").unwrap()["child_profiles"],
            json!(["worker"])
        );
        for (source, fragment) in [
            (
                "[profiles.manager]\nharness = \"codex\"\nmodel = \"fixture-model\"\nchild_profiles = [\"manager\"]\n",
                "itself",
            ),
            (
                "[profiles.manager]\nharness = \"codex\"\nmodel = \"fixture-model\"\nchild_profiles = [\"ghost\"]\n",
                "unknown child profile",
            ),
            (
                "[profiles.manager]\nharness = \"codex\"\nmodel = \"fixture-model\"\nchild_profiles = [\"worker\", \"worker\"]\n\
                 [profiles.worker]\nharness = \"codex\"\nmodel = \"fixture-cheap\"\n",
                "duplicate",
            ),
        ] {
            fixture.write(source);
            let error = resolve_profile(&fixture.root, "manager").unwrap_err();
            assert_eq!(error.code, "profile_error", "source: {source}");
            assert!(
                error.message.contains("child_profiles") && error.message.contains(fragment),
                "{}",
                error.message
            );
        }
    }

    #[test]
    fn native_subagents_default_deny_for_managers_allow_otherwise() {
        let fixture = AutomationConfigFixture::new();
        fixture.write(
            "[profiles.manager]\nharness = \"codex\"\nmodel = \"fixture-model\"\nchild_profiles = [\"worker\"]\n\
             [profiles.worker]\nharness = \"codex\"\nmodel = \"fixture-cheap\"\n",
        );
        assert_eq!(
            resolve_profile(&fixture.root, "manager").unwrap()["native_subagents"],
            json!("deny")
        );
        assert_eq!(
            resolve_profile(&fixture.root, "worker").unwrap()["native_subagents"],
            json!("allow")
        );
        fixture.write(
            "[profiles.solo]\nharness = \"codex\"\nmodel = \"fixture-model\"\nnative_subagents = \"allow\"\n",
        );
        assert_eq!(
            resolve_profile(&fixture.root, "solo").unwrap()["native_subagents"],
            json!("allow")
        );
        fixture.write(
            "[profiles.manager]\nharness = \"codex\"\nmodel = \"fixture-model\"\nchild_profiles = [\"worker\"]\nnative_subagents = \"allow\"\n\
             [profiles.worker]\nharness = \"codex\"\nmodel = \"fixture-cheap\"\n",
        );
        assert_eq!(
            resolve_profile(&fixture.root, "manager").unwrap()["native_subagents"],
            json!("allow")
        );
        fixture.write(
            "[profiles.bad]\nharness = \"codex\"\nmodel = \"fixture-model\"\nnative_subagents = \"sometimes\"\n",
        );
        let error = resolve_profile(&fixture.root, "bad").unwrap_err();
        assert_eq!(error.code, "profile_error");
        assert!(
            error.message.contains("native_subagents"),
            "{}",
            error.message
        );
        for harness in ["muse", "antigravity"] {
            let cmd = "C:/Tools/Native/agent.exe";
            fixture.write(&format!(
                "[profiles.manager]\nharness = \"{harness}\"\ncmd = \"{cmd}\"\nchild_profiles = [\"worker\"]\n\
                 [profiles.worker]\nharness = \"codex\"\nmodel = \"fixture-cheap\"\n"
            ));
            let error = resolve_profile(&fixture.root, "manager").unwrap_err();
            assert_eq!(error.code, "unsupported", "harness: {harness}");
            assert!(
                error.message.contains("native_subagents"),
                "{}",
                error.message
            );
            fixture.write(&format!(
                "[profiles.manager]\nharness = \"{harness}\"\ncmd = \"{cmd}\"\nchild_profiles = [\"worker\"]\nnative_subagents = \"allow\"\n\
                 [profiles.worker]\nharness = \"codex\"\nmodel = \"fixture-cheap\"\n"
            ));
            assert_eq!(
                resolve_profile(&fixture.root, "manager").unwrap()["native_subagents"],
                json!("allow"),
                "harness: {harness}"
            );
        }
    }

    #[test]
    fn daemon_retention_rejects_nonpositive_and_unknown_values() {
        let fixture = AutomationConfigFixture::new();
        for source in [
            "[daemon]\nretention.events_days = 0\n",
            "[daemon]\nretention.events_days = -1\n",
            "[daemon]\nretention.commands_days = 0\n",
            "[daemon]\nretention.commands_days = -30\n",
            "[daemon]\nretention.mail_days = 0\n",
            "[daemon]\nretention.mail_days = -30\n",
            "[daemon]\nretention.events_days = \"30\"\n",
            "[daemon]\nretention.unknown = 1\n",
            "[daemon]\nunknown_table.value = 1\n",
        ] {
            fixture.write(source);
            let error = load_daemon_config(&fixture.root).unwrap_err();
            assert_eq!(error.code, "profile_error", "source: {source}");
        }
    }

    #[test]
    fn mail_config_defaults_to_the_coordination_contract_bounds() {
        let fixture = AutomationConfigFixture::new();
        assert_eq!(
            load_mail_config(&fixture.root).unwrap(),
            MailConfig::default()
        );
        assert_eq!(
            MailConfig::default().max_unread_bytes_per_agent,
            core::api::DEFAULT_MAX_UNREAD_BYTES_PER_AGENT
        );
        assert_eq!(
            MailConfig::default().max_unread_messages_per_agent,
            core::api::DEFAULT_MAX_UNREAD_MESSAGES_PER_AGENT
        );

        fixture.write("[mail]\n");
        assert_eq!(
            load_mail_config(&fixture.root).unwrap(),
            MailConfig::default()
        );

        fixture.write("[mail]\nmax_unread_bytes_per_agent = 1024\n");
        assert_eq!(
            load_mail_config(&fixture.root).unwrap(),
            MailConfig {
                max_unread_bytes_per_agent: 1024,
                max_unread_messages_per_agent: core::api::DEFAULT_MAX_UNREAD_MESSAGES_PER_AGENT,
            }
        );

        fixture.write(
            "[mail]\nmax_unread_bytes_per_agent = 1024\nmax_unread_messages_per_agent = 8\n",
        );
        let loaded = load_mail_config(&fixture.root).unwrap();
        assert_eq!(
            loaded,
            MailConfig {
                max_unread_bytes_per_agent: 1024,
                max_unread_messages_per_agent: 8,
            }
        );
        assert_eq!(
            loaded.limits(),
            core::api::MailLimits {
                max_unread_bytes_per_agent: 1024,
                max_unread_messages_per_agent: 8,
            }
        );
    }

    #[test]
    fn mail_config_rejects_nonpositive_and_unknown_values() {
        let fixture = AutomationConfigFixture::new();
        for source in [
            "[mail]\nmax_unread_bytes_per_agent = 0\n",
            "[mail]\nmax_unread_bytes_per_agent = -1\n",
            "[mail]\nmax_unread_messages_per_agent = 0\n",
            "[mail]\nmax_unread_messages_per_agent = -10\n",
            "[mail]\nmax_unread_bytes_per_agent = \"1024\"\n",
            "[mail]\nunknown = 1\n",
        ] {
            fixture.write(source);
            let error = load_mail_config(&fixture.root).unwrap_err();
            assert_eq!(error.code, "profile_error", "source: {source}");
        }
    }

    #[test]
    fn project_git_config_keeps_only_direct_approved_fetch_endpoints() {
        let fixture = AutomationConfigFixture::new();
        let project_path = format!("{:?}", fixture.root.to_string_lossy());
        let remote = fixture
            .root
            .join("origin.git")
            .to_string_lossy()
            .into_owned();
        fixture.write(&format!(
            concat!(
                "[profiles.fixture]\n",
                "harness = \"codex\"\n",
                "model = \"fixture-model\"\n",
                "[projects.sample]\n",
                "path = {project_path}\n",
                "profiles = ['fixture']\n",
                "actions = ['git.fetch']\n",
                "[projects.sample.git]\n",
                "fetch_remotes = {{ origin = {remote:?}, ",
                "http = \"http://git.example.test/team/repo.git\", ",
                "https = \"https://git.example.test/team/repo.git\", ",
                "git = \"git://git.example.test/team/repo.git\", ",
                "file = \"file:///tmp/repository.git\" }}\n",
                "credential_helper = 'manager'\n",
            ),
            project_path = project_path,
            remote = remote,
        ));
        let configured = load_project_registration_config(&fixture.root).unwrap();
        assert_eq!(configured[0].actions, ["git.fetch"]);
        assert_eq!(
            configured[0].git,
            Some(core::api::ProjectGitView {
                fetch_remotes: BTreeMap::from([
                    ("file".to_owned(), "file:///tmp/repository.git".to_owned()),
                    (
                        "git".to_owned(),
                        "git://git.example.test/team/repo.git".to_owned(),
                    ),
                    (
                        "http".to_owned(),
                        "http://git.example.test/team/repo.git".to_owned(),
                    ),
                    (
                        "https".to_owned(),
                        "https://git.example.test/team/repo.git".to_owned(),
                    ),
                    ("origin".to_owned(), remote),
                ]),
                credential_helper: Some(core::api::GitCredentialHelper::Manager),
            })
        );
        assert_eq!(
            configured[0].git.as_ref().unwrap().fetch_remotes["http"],
            "http://git.example.test/team/repo.git"
        );
        assert_eq!(
            configured[0].git.as_ref().unwrap().fetch_remotes["https"],
            "https://git.example.test/team/repo.git"
        );
        assert_eq!(
            configured[0].git.as_ref().unwrap().fetch_remotes["git"],
            "git://git.example.test/team/repo.git"
        );
        assert_eq!(
            configured[0].git.as_ref().unwrap().fetch_remotes["file"],
            "file:///tmp/repository.git"
        );

        for endpoint in [
            "https://user:secret@example.test/repo.git",
            "https://example.test/repo.git?token=secret",
            "ext::ssh -i key",
            "ssh://git.example.test/team/repo.git",
            "../relative.git",
            "git+custom://example.test/repo.git",
        ] {
            fixture.write(&format!(
                concat!(
                    "[projects.sample]\n",
                    "path = {project_path}\n",
                    "actions = ['git.fetch']\n",
                    "[projects.sample.git]\n",
                    "fetch_remotes = {{ origin = {endpoint:?} }}\n",
                ),
                project_path = project_path,
                endpoint = endpoint,
            ));
            assert_eq!(
                load_project_registration_config(&fixture.root)
                    .expect_err("unsafe project fetch endpoint must be rejected")
                    .code,
                "profile_error"
            );
        }
    }

    #[test]
    fn project_coordination_actions_are_explicit_canonical_grants() {
        let fixture = AutomationConfigFixture::new();
        let project_path = format!("{:?}", fixture.root.to_string_lossy());
        fixture.write(&format!(
            "[projects.sample]\npath = {project_path}\nactions = ['mail.send', 'var.set']\n"
        ));
        let configured = load_project_registration_config(&fixture.root).unwrap();
        assert_eq!(configured[0].actions, vec!["mail.send", "var.set"]);
        fixture.write(&format!(
            "[projects.sample]\npath = {project_path}\nactions = ['msg.send']\n"
        ));
        assert_eq!(
            load_project_registration_config(&fixture.root)
                .unwrap_err()
                .code,
            "profile_error"
        );
    }

    #[test]
    fn project_steer_is_an_explicit_action_allowlist_grant() {
        let fixture = AutomationConfigFixture::new();
        let project_path = format!("{:?}", fixture.root.to_string_lossy());
        fixture.write(&format!(
            "[profiles.fixture]\nharness = \"codex\"\nmodel = \"fixture-model\"\n[projects.sample]\npath = {project_path}\nprofiles = ['fixture']\nactions = ['steer']\n"
        ));

        let configured = load_project_registration_config(&fixture.root).unwrap();
        assert_eq!(configured.len(), 1);
        assert_eq!(configured[0].actions, vec!["steer"]);

        fixture.write(&format!(
            "[profiles.fixture]\nharness = \"codex\"\nmodel = \"fixture-model\"\n[projects.sample]\npath = {project_path}\nprofiles = ['fixture']\nactions = ['send']\n"
        ));
        let without_steer = load_project_registration_config(&fixture.root).unwrap();
        assert_eq!(without_steer[0].actions, vec!["send"]);
    }

    #[test]
    fn project_request_reply_verbs_are_independent_explicit_grants() {
        let fixture = AutomationConfigFixture::new();
        let project_path = format!("{:?}", fixture.root.to_string_lossy());

        for action in ["approve", "deny", "answer"] {
            fixture.write(&format!(
                "[profiles.fixture]\nharness = \"codex\"\nmodel = \"fixture-model\"\n[projects.sample]\npath = {project_path}\nprofiles = ['fixture']\nactions = ['{action}']\n"
            ));
            let configured = load_project_registration_config(&fixture.root).unwrap();
            assert_eq!(configured.len(), 1);
            assert_eq!(configured[0].actions, vec![action.to_owned()]);
        }
    }

    #[test]
    fn project_launch_is_an_explicit_action_grant_and_uses_approved_profile_allowlist() {
        let fixture = AutomationConfigFixture::new();
        let project_path = format!("{:?}", fixture.root.to_string_lossy());
        fixture.write(&format!(
            "[profiles.fixture]\nharness = \"codex\"\nmodel = \"fixture-model\"\n[projects.sample]\npath = {project_path}\nprofiles = ['fixture']\nactions = ['launch']\n"
        ));

        let configured = load_project_registration_config(&fixture.root).unwrap();
        assert_eq!(configured.len(), 1);
        assert_eq!(configured[0].profiles, vec!["fixture"]);
        assert_eq!(configured[0].actions, vec!["launch"]);

        fixture.write(&format!(
            "[profiles.fixture]\nharness = \"codex\"\nmodel = \"fixture-model\"\n[projects.sample]\npath = {project_path}\nprofiles = ['fixture']\nactions = ['resume', 'send']\n"
        ));
        let with_resume = load_project_registration_config(&fixture.root).unwrap();
        assert_eq!(
            with_resume[0].actions,
            vec!["resume".to_owned(), "send".to_owned()]
        );

        fixture.write(&format!(
            "[profiles.fixture]\nharness = \"codex\"\nmodel = \"fixture-model\"\n[projects.sample]\npath = {project_path}\nprofiles = ['fixture']\nactions = ['send']\n"
        ));
        let without_launch = load_project_registration_config(&fixture.root).unwrap();
        assert_eq!(without_launch[0].actions, vec!["send"]);
    }

    #[test]
    fn global_run_configuration_defaults_to_denial_and_keeps_literal_grants() {
        let fixture = AutomationConfigFixture::new();
        let denied = load_automation_config(&fixture.root).unwrap();
        assert!(denied.run.interpreters.is_empty());
        assert!(denied.run.run_env.is_empty());

        fixture.write("[automation.run]\nrun_env = [\"CI\"]\n");
        let no_interpreter = load_automation_config(&fixture.root).unwrap();
        assert!(no_interpreter.run.interpreters.is_empty());
        assert_eq!(no_interpreter.run.run_env, vec!["CI"]);

        let program = std::env::current_exe()
            .expect("current executable path")
            .to_string_lossy()
            .into_owned();
        fixture.write(
            "[automation.run]\n\
             [automation.run.interpreters.fixture]\n\
             program = \"relative-program\"\nargv_prefix = [\"--literal\"]\n",
        );
        assert!(load_automation_config(&fixture.root).is_err());

        fixture.write(&format!(
            "[automation.run]\nrun_env = [\"PATH\"]\n\
             [automation.run.interpreters.fixture]\nprogram = {program:?}\n\
             argv_prefix = [\"--literal\", \"fixed-value\"]\n"
        ));
        assert!(load_automation_config(&fixture.root).is_err());

        fixture.write(&format!(
            "[automation.run]\nrun_env = [\"CI\"]\n\
             [automation.run.interpreters.fixture]\nprogram = {program:?}\n\
             argv_prefix = [\"--literal\", \"fixed-value\"]\n"
        ));
        let configured = load_automation_config(&fixture.root).unwrap();
        assert_eq!(
            configured.run,
            RunGrantConfig {
                interpreters: BTreeMap::from([(
                    "fixture".to_owned(),
                    core::api::ScriptInterpreter {
                        program: program.clone(),
                        argv_prefix: vec!["--literal".to_owned(), "fixed-value".to_owned()],
                    },
                )]),
                run_env: vec!["CI".to_owned()],
            }
        );

        fixture.write(&format!(
            "[automation.run.interpreters.fixture]\nprogram = {program:?}\n\
             argv_prefix = [\"--literal\"]\n"
        ));
        let no_action_env = load_automation_config(&fixture.root).unwrap();
        assert!(no_action_env.run.run_env.is_empty());

        fixture.write("[automation.run]\nrun_env = [\"GITHUB_TOKEN\"]\n");
        assert!(load_automation_config(&fixture.root).is_err());
    }

    #[test]
    fn project_run_configuration_uses_owner_approved_interpreters_and_safe_env() {
        let fixture = AutomationConfigFixture::new();
        let project_path = fixture.root.to_string_lossy();
        let program = std::env::current_exe()
            .expect("current executable path")
            .to_string_lossy()
            .into_owned();
        fixture.write(&format!(
            "[projects.sample]\npath = {project_path:?}\nactions = [\"run\"]\n\
             run_env = [\"CI\", \"BUILD_ID\"]\n\
             [projects.sample.interpreters.pwsh]\nprogram = {program:?}\n\
             argv_prefix = [\"-NoProfile\"]\n"
        ));

        let configured = load_project_registration_config(&fixture.root)
            .expect("load owner-approved project run configuration");
        assert_eq!(configured.len(), 1);
        assert_eq!(configured[0].actions, vec!["run"]);
        assert_eq!(
            configured[0].interpreters.get("pwsh"),
            Some(&core::api::ScriptInterpreter {
                program,
                argv_prefix: vec!["-NoProfile".to_owned()],
            })
        );
        assert_eq!(configured[0].run_env, vec!["BUILD_ID", "CI"]);

        assert!(forbidden_run_env_name("ELIOT_RUN_TOKEN", &[]));
        assert!(forbidden_run_env_name("USER_ROLE", &[]));
        assert!(forbidden_run_env_name("GITHUB_TOKEN", &[]));
        assert!(forbidden_run_env_name("SERVICE_CREDENTIAL", &[]));
        assert!(forbidden_run_env_name("SERVICE_SECRET", &[]));
        assert!(forbidden_run_env_name("OPENAI_API_KEY", &[]));
        assert!(forbidden_run_env_name("PROVIDER_BASE_URL", &[]));
        assert!(forbidden_run_env_name("CUSTOM_ENDPOINT", &[]));
        assert!(forbidden_run_env_name("CUSTOM_ROUTE", &[]));
        assert!(forbidden_run_env_name("CUSTOM_ACCOUNT", &[]));
        assert!(forbidden_run_env_name("CODEX_HOME", &[]));
        assert!(forbidden_run_env_name("CODEX_SQLITE_HOME", &[]));
        assert!(forbidden_run_env_name("GIT_AUTHOR_NAME", &[]));
        assert!(forbidden_run_env_name("PATH", &[]));
        if cfg!(windows) {
            assert!(forbidden_run_env_name(
                "runtime_key",
                &["RUNTIME_KEY".to_owned()]
            ));
        } else {
            assert!(!forbidden_run_env_name(
                "runtime_key",
                &["RUNTIME_KEY".to_owned()]
            ));
        }

        fixture.write(&format!(
            "[projects.sample]\npath = {project_path:?}\nactions = [\"run\"]\n\
             run_env = [\"PATH\"]\n"
        ));
        assert!(load_project_registration_config(&fixture.root).is_err());

        fixture.write(&format!(
            "[projects.sample]\npath = {project_path:?}\nactions = [\"run\"]\n\
             run_env = [\"RUNTIME_KEY\"]\n\
             [proxies.route]\nattach = \"http://127.0.0.1:10300\"\n\
             health = \"/health\"\napi_key_env = \"RUNTIME_KEY \"\n\
             base_url_for.codex = \"http://127.0.0.1:10300/v1\"\n"
        ));
        assert!(load_project_registration_config(&fixture.root).is_err());

        fixture.write(&format!(
            "[projects.sample]\npath = {project_path:?}\nactions = [\"run\"]\n\
             run_env = [\"CI\", \"ci\"]\n"
        ));
        assert_eq!(
            load_project_registration_config(&fixture.root).is_err(),
            cfg!(windows),
            "environment-name duplicate checks follow the platform's case rules"
        );
    }

    #[test]
    fn automation_backlog_rejects_nonpositive_wrong_type_and_unknown_fields() {
        let fixture = AutomationConfigFixture::new();
        for source in [
            "[automation]\nmax_event_backlog = 0\n",
            "[automation]\nmax_event_backlog = -1\n",
            "[automation]\nmax_event_backlog = '1000'\n",
            "[automation]\nfuture_setting = 1\n",
        ] {
            fixture.write(source);
            let error = load_automation_config(&fixture.root).unwrap_err();
            assert_eq!(error.code, "profile_error", "accepted {source:?}");
        }
    }

    #[test]
    fn native_store_inputs_capture_only_selected_raw_values() {
        let mut profile =
            json!({"pass_env":["CODEX_HOME", "CODEX_SQLITE_HOME", "EPHEMERAL_TOKEN"]});
        let raw = " ../original-home ";
        capture_native_store_inputs_with_lookups(
            &mut profile,
            |name| match name {
                "CODEX_HOME" => Some(OsString::from(raw)),
                "CODEX_SQLITE_HOME" => None,
                _ => panic!("unexpected selector lookup: {name}"),
            },
            |name| match name {
                "EPHEMERAL_TOKEN" => Some(OsString::from("temporary-credential")),
                _ => panic!("unexpected credential lookup: {name}"),
            },
        )
        .unwrap();
        assert_eq!(
            profile["pass_env_values"],
            json!({"CODEX_HOME":raw, "CODEX_SQLITE_HOME":null})
        );
        let mut unselected = json!({"pass_env":[]});
        capture_native_store_inputs(&mut unselected, |_| {
            panic!("unselected variables must not be read")
        })
        .unwrap();
        assert_eq!(
            unselected["pass_env_values"],
            json!({"CODEX_HOME":null, "CODEX_SQLITE_HOME":null})
        );
        let mut empty = json!({"pass_env":["CODEX_HOME"]});
        capture_native_store_inputs(&mut empty, |_| Some(OsString::new())).unwrap();
        assert_eq!(empty["pass_env_values"]["CODEX_HOME"], "");
        let mut alias = json!({"pass_env":["cOdEx_HoMe"]});
        capture_native_store_inputs_with_lookups(
            &mut alias,
            |_| Some(OsString::from(raw)),
            |_| None,
        )
        .unwrap();
        assert_eq!(
            alias["pass_env_values"]["CODEX_HOME"],
            if cfg!(windows) {
                json!(raw)
            } else {
                Value::Null
            }
        );

        assert_eq!(alias["pass_env_values"].as_object().unwrap().len(), 2);

        #[cfg(windows)]
        let invalid = {
            use std::os::windows::ffi::OsStringExt;
            OsString::from_wide(&[0xd800])
        };
        #[cfg(unix)]
        let invalid = {
            use std::os::unix::ffi::OsStringExt;
            OsString::from_vec(vec![0xff])
        };
        #[cfg(any(windows, unix))]
        {
            let mut profile = json!({"pass_env":["CODEX_HOME"]});
            let original = profile.clone();
            assert_eq!(
                capture_native_store_inputs(&mut profile, |_| Some(invalid.clone()))
                    .unwrap_err()
                    .code,
                "invalid"
            );
            assert_eq!(profile, original);
        }
    }

    #[test]
    fn build_inputs_freeze_exact_target_dir_only_when_admitted() {
        let mut admitted = json!({"pass_env":["CODEX_HOME", "CARGO_TARGET_DIR"]});
        capture_build_inputs(&mut admitted, |name| match name {
            "CARGO_TARGET_DIR" => Some(OsString::from("C:/targets/wt-proof")),
            _ => None,
        })
        .unwrap();
        assert_eq!(admitted["target_dir"], "C:/targets/wt-proof");

        let mut unadmitted = json!({"pass_env":["CODEX_HOME"]});
        capture_build_inputs(&mut unadmitted, |_| {
            Some(OsString::from("C:/targets/wt-proof"))
        })
        .unwrap();
        assert!(unadmitted.get("target_dir").is_none());

        let mut unset = json!({"pass_env":["CARGO_TARGET_DIR"]});
        capture_build_inputs(&mut unset, |_| None).unwrap();
        assert!(unset.get("target_dir").is_none());

        for bad in ["relative/target", ""] {
            let mut profile = json!({"pass_env":["CARGO_TARGET_DIR"]});
            capture_build_inputs(&mut profile, |_| Some(OsString::from(bad))).unwrap();
            assert!(
                profile.get("target_dir").is_none(),
                "refused value must stay absent: {bad}"
            );
        }
    }

    #[test]
    fn native_store_inputs_reject_non_selector_credential_collisions() {
        let mut profile = json!({"pass_env":["CODEX_HOME", "EPHEMERAL_TOKEN"]});
        let original = profile.clone();
        let error = capture_native_store_inputs_with_lookups(
            &mut profile,
            |name| match name {
                "CODEX_HOME" => Some(OsString::from("native-home/session-credential")),
                _ => panic!("unexpected selector lookup: {name}"),
            },
            |name| match name {
                "EPHEMERAL_TOKEN" => Some(OsString::from("session-credential")),
                _ => panic!("unexpected credential lookup: {name}"),
            },
        )
        .unwrap_err();

        assert_eq!(error.code, "secret_in_payload");
        assert_eq!(profile, original);
    }

    #[test]
    fn native_store_capture_rejects_credentials_in_all_frozen_metadata() {
        let credential = "CLIENT_SECRET_VALUE";
        for location in [
            "endpoint",
            "name",
            "model",
            "capacity_scope",
            "client_key_env_name",
            "field_key",
        ] {
            let mut profile = json!({
                "harness":"codex",
                "model":"gpt-6-luna",
                "pass_env":["CODEX_HOME", "RUNTIME_KEY"],
                "route_binding":{
                    "name":"opencodex",
                    "endpoint":"http://127.0.0.1:10100",
                    "readiness_path":"/api/system/health",
                    "provider_base_url":"http://127.0.0.1:10100/v1",
                    "client_key_env_name":"RUNTIME_KEY"
                },
                "capacity_scopes":[{"scope":"profile:routed", "max_live":null}]
            });
            match location {
                "endpoint" => {
                    profile["route_binding"]["endpoint"] =
                        json!("http://127.0.0.1:10100/CLIENT_SECRET_VALUE");
                }
                "name" => profile["route_binding"]["name"] = json!(credential),
                "model" => profile["model"] = json!(credential),
                "capacity_scope" => {
                    profile["capacity_scopes"][0]["scope"] = json!(format!("profile:{credential}"));
                }
                "client_key_env_name" => {
                    profile["route_binding"]["client_key_env_name"] = json!(credential);
                }
                "field_key" => profile[credential] = json!("otherwise-safe-value"),
                _ => unreachable!(),
            }
            let original = profile.clone();
            let error = capture_native_store_inputs_with_lookups(
                &mut profile,
                |name| (name == "CODEX_HOME").then(|| OsString::from("C:/CodexHome")),
                |name| (name == "RUNTIME_KEY").then(|| OsString::from(credential)),
            )
            .unwrap_err();
            assert_eq!(error.code, "secret_in_payload", "missed {location}");
            assert_eq!(profile, original, "mutated snapshot for {location}");
        }
    }

    fn config_error(source: &str) -> core::Error {
        let error = match toml::from_str::<FileConfig>(source) {
            Ok(_) => panic!("test configuration unexpectedly deserialized"),
            Err(error) => error,
        };
        map_toml_error(source, error)
    }

    fn resolve_text_profile(source: &str, name: &str) -> core::Result<Value> {
        let config: FileConfig = toml::from_str(source).unwrap();
        let entry = config
            .profiles
            .get(name)
            .ok_or_else(|| core::Error::new("profile_error", "test profile is missing"))?;
        let snapshot = snapshot_from_entry(name, entry.get_ref(), entry.span().start, source)?;
        bind_route_and_capacity(
            name,
            entry.get_ref(),
            &config,
            entry.span().start,
            source,
            snapshot,
        )
    }

    #[test]
    fn l34_route_and_capacity_snapshot_keeps_only_explicit_frozen_fields() {
        let source = r#"
[profiles.routed]
harness = "codex"
model = "gpt-6-luna"
effort = "medium"
proxy = "opencodex"
account = "main"
max_live = 2
pass_env = ["RUNTIME_KEY"]

[accounts.main]
max_live = 4

[proxies.opencodex]
attach = "http://127.0.0.1:10300"
health = "/api/system/health"
api_key_env = "RUNTIME_KEY"
max_live = 8
base_url_for.codex = "http://127.0.0.1:10300/v1"
"#;
        let mut snapshot = resolve_text_profile(source, "routed").unwrap();
        assert_eq!(snapshot["pass_env"], json!(["RUNTIME_KEY"]));
        assert_eq!(snapshot["account"], "main");
        assert_eq!(snapshot["proxy"], "opencodex");
        assert_eq!(
            snapshot["capacity_scopes"],
            json!([
                {"scope":"profile:routed", "max_live":2},
                {"scope":"account:main", "max_live":4},
                {"scope":"proxy:opencodex", "max_live":8}
            ])
        );
        let route: core::RouteBinding =
            serde_json::from_value(snapshot["route_binding"].clone()).unwrap();
        assert_eq!(route.name, "opencodex");
        assert_eq!(route.endpoint, "http://127.0.0.1:10300");
        assert_eq!(route.readiness_path, "/api/system/health");
        assert_eq!(route.provider_base_url, "http://127.0.0.1:10300/v1");
        assert_eq!(route.client_key_env_name.as_deref(), Some("RUNTIME_KEY"));
        assert_eq!(snapshot["route_binding"].as_object().unwrap().len(), 5);
        capture_native_store_inputs_with_lookups(
            &mut snapshot,
            |_| panic!("unselected store selectors must not be read"),
            |name| (name == "RUNTIME_KEY").then(|| OsString::from("runtime-secret-value")),
        )
        .unwrap();
        assert!(!snapshot.to_string().contains("runtime-secret-value"));
    }

    #[test]
    fn l34_key_reference_requires_explicit_allowlist_and_rejects_reserved_names() {
        let base = r#"
[profiles.routed]
harness = "codex"
model = "gpt-6-luna"
effort = "medium"
proxy = "opencodex"
pass_env = ["__KEY__"]

[proxies.opencodex]
attach = "http://127.0.0.1:10300"
health = "/api/system/health"
api_key_env = "__KEY__"
base_url_for.codex = "http://127.0.0.1:10300/v1"
"#;
        let missing_allowlist = base.replace("pass_env = [\"__KEY__\"]\n", "");
        let error = resolve_text_profile(&missing_allowlist, "routed").unwrap_err();
        assert!(
            error
                .message
                .contains("explicitly allowed by profile pass_env")
        );

        let selector = if cfg!(windows) {
            "codex_home"
        } else {
            "CODEX_HOME"
        };
        let reserved = base.replace("__KEY__", selector);
        assert!(resolve_text_profile(&reserved, "routed").is_err());

        let identity = base.replace("__KEY__", "eliot_agent_token");
        assert!(resolve_text_profile(&identity, "routed").is_err());
    }

    #[test]
    fn l34_requires_explicit_health_and_base_but_effort_is_optional() {
        for source in [
            "[proxies.opencodex]\nattach='http://127.0.0.1:10300'\nbase_url_for.codex='http://127.0.0.1:10300/v1'\n",
            "[proxies.opencodex]\nattach='http://127.0.0.1:10300'\nhealth='/readyz'\n",
        ] {
            assert!(toml::from_str::<FileConfig>(source).is_err());
        }

        let without_effort = r#"
[profiles.routed]
harness = "codex"
model = "gpt-6-luna"
proxy = "opencodex"
[proxies.opencodex]
attach = "http://127.0.0.1:10300"
health = "/api/system/health"
base_url_for.codex = "http://127.0.0.1:10300/v1"
"#;
        // Omitted effort passes daemon admission unchanged: the snapshot
        // carries the route binding and no `effort` key, so the plugin can
        // omit the wire field and preserve the native default.
        let routed = resolve_text_profile(without_effort, "routed").unwrap();
        assert!(routed.get("effort").is_none());
        assert!(routed.get("route_binding").is_some());

        let direct = "[profiles.direct]\nharness='codex'\nmodel='gpt-6-luna'\n";
        let direct = resolve_text_profile(direct, "direct").unwrap();
        assert!(direct.get("effort").is_none());
        assert!(direct.get("route_binding").is_none());
    }

    #[test]
    fn native_default_model_requires_absence_not_an_explicit_blank() {
        let omitted = resolve_text_profile("[profiles.native]\nharness = 'codex'\n", "native")
            .expect("omitted model keeps native default");
        assert!(omitted.get("model").is_none());
        for model in ["", "   "] {
            let source = format!("[profiles.native]\nharness = 'codex'\nmodel = {model:?}\n");
            let error = resolve_text_profile(&source, "native")
                .expect_err("an explicitly blank model must not become native default");
            assert_eq!(error.code, "profile_error", "{model:?}");
        }
    }

    #[test]
    fn l34_rejects_builtin_provider_name_collision() {
        let source = r#"
[profiles.routed]
harness = "codex"
model = "gpt-6-luna"
effort = "medium"
proxy = "OpenAI"
[proxies.OpenAI]
attach = "http://127.0.0.1:10300"
health = "/api/system/health"
base_url_for.codex = "http://127.0.0.1:10300/v1"
"#;
        assert!(resolve_text_profile(source, "routed").is_err());
    }

    #[test]
    fn deferred_fields_use_decoded_keys_at_the_error_span() {
        let sources = [
            "[profiles.codex-luna]\nharness = 'codex'\n\"routes\" = 'private-value'\n",
            r#"[profiles.codex-luna]
harness = 'codex'
"\u0072outes" = 'private-value'
"#,
            "profiles.codex-luna.harness = 'codex'\nprofiles.codex-luna.routes = 'private-value'\n",
            "[profiles.codex-luna]\nharness = 'codex'\n[[profiles.codex-luna.setup]]\nprogram = 'C:/tools/pwsh.exe'\nargs = []\ntimeout_ms = 120000\nextends = 'private-value'\n",
        ];

        for source in sources {
            let error = config_error(source);
            assert_eq!(error.code, "unsupported");
            assert!(error.message.contains("unsupported configuration field"));
            assert!(!error.message.contains("private-value"));
        }
    }

    #[test]
    fn ordinary_unknown_fields_remain_profile_errors() {
        let error = config_error(
            "[profiles.codex-luna]\nharness = 'codex'\nfuture_option = 'private-value'\n",
        );

        assert_eq!(error.code, "profile_error");
        assert!(
            error
                .message
                .contains("future_option: unknown configuration field")
        );
        assert!(!error.message.contains("private-value"));
    }

    #[test]
    fn muse_profile_freezes_model_effort_and_cmd() {
        let source = r#"
[profiles.muse]
harness = "muse"
cmd = "C:/Tools/MuseCode/muse-bin-1.4.0-R4161.1.exe"
model = "muse-spark-1.3-contributor"
effort = "medium"
approval = "promptUnmatched"
pass_env = ["EXAMPLE_API_KEY"]
mcp = "inherit"
"#;
        let snapshot = resolve_text_profile(source, "muse").unwrap();
        assert_eq!(snapshot["harness"], "muse");
        assert_eq!(
            snapshot["cmd"],
            "C:/Tools/MuseCode/muse-bin-1.4.0-R4161.1.exe"
        );
        assert_eq!(snapshot["model"], "muse-spark-1.3-contributor");
        assert_eq!(snapshot["effort"], "medium");
        assert_eq!(snapshot["approval"], "promptUnmatched");
        assert_eq!(snapshot["permissions"], "workspace-write");
        assert_eq!(snapshot["approval_policy"], "never");
        assert_eq!(snapshot["pass_env"], json!(["EXAMPLE_API_KEY"]));

        let full_access = resolve_text_profile(
            r#"
[profiles.muse-full]
harness = "muse"
cmd = "C:/Tools/MuseCode/muse-bin-1.4.0-R4161.1.exe"
model = "muse-spark-1.3-contributor"
mcp = "inherit"
permissions = "danger-full-access"
"#,
            "muse-full",
        )
        .unwrap();
        assert_eq!(full_access["permissions"], "danger-full-access");

        let explicit_write = resolve_text_profile(
            r#"
[profiles.muse-write]
harness = "muse"
cmd = "C:/Tools/MuseCode/muse-bin-1.4.0-R4161.1.exe"
model = "muse-spark-1.3-contributor"
mcp = "inherit"
permissions = "workspace-write"
"#,
            "muse-write",
        )
        .unwrap();
        assert_eq!(explicit_write["permissions"], "workspace-write");

        let read_only_err = resolve_text_profile(
            "[profiles.muse]\nharness='muse'\nmodel='muse-spark-1.3-contributor'\ncmd='C:/Tools/MuseCode/muse-bin-1.4.0-R4161.1.exe'\nmcp='inherit'\npermissions='read-only'\n",
            "muse",
        )
        .expect_err("read-only unsupported");
        assert_eq!(read_only_err.code, "unsupported");

        let dsp_err = resolve_text_profile(
            "[profiles.muse]\nharness='muse'\nmodel='muse-spark-1.3-contributor'\ncmd='C:/Tools/MuseCode/muse-bin-1.4.0-R4161.1.exe'\nmcp='inherit'\ndangerously_skip_permissions=true\n",
            "muse",
        )
        .expect_err("dangerously_skip_permissions unsupported");
        assert_eq!(dsp_err.code, "unsupported");

        for source in [
            "[profiles.muse]\nharness='muse'\n",
            "[profiles.muse]\nharness='muse'\nmodel='muse-spark-1.3-contributor'\ncollaboration_mode='plan'\n",
            "[profiles.muse]\nharness='muse'\nmodel='muse-spark-1.3-contributor'\npermissions='read-only'\n",
            "[profiles.muse]\nharness='muse'\nmodel='muse-spark-1.3-contributor'\napproval_policy='on-request'\n",
            "[profiles.muse]\nharness='muse'\nmodel='muse-spark-1.3-contributor'\napproval='sometimes'\n",
            "[profiles.muse]\nharness='muse'\nmodel='muse-spark-1.3-contributor'\napproval=''\n",
        ] {
            assert!(resolve_text_profile(source, "muse").is_err(), "{source}");
        }
        // No PATH default: an explicit native cmd is required, and script
        // wrappers are rejected (their PID would orphan the host on stop).
        for source in [
            "[profiles.muse]\nharness='muse'\nmodel='muse-spark-1.3-contributor'\n",
            "[profiles.muse]\nharness='muse'\nmodel='muse-spark-1.3-contributor'\ncmd='C:/Tools/MuseCode/muse.cmd'\n",
            "[profiles.muse]\nharness='muse'\nmodel='muse-spark-1.3-contributor'\ncmd='C:/Tools/MuseCode/muse.bat'\n",
            "[profiles.muse]\nharness='muse'\nmodel='muse-spark-1.3-contributor'\ncmd='C:/Tools/MuseCode/launch.ps1'\n",
        ] {
            let error = resolve_text_profile(source, "muse").expect_err("must reject");
            assert_eq!(error.code, "profile_error", "{source}");
        }
    }

    #[test]
    fn frozen_snapshot_carries_configured_cloud_hostname() {
        let source = "[profiles.direct]\nharness='codex'\nmodel='gpt-6-luna'\n\
            [cloud_mcp]\ncloudflared_executable='C:/Tools/cloudflared.exe'\n\
            tunnel_id='tunnel-id'\ncredentials_file='C:/Tools/creds.json'\n\
            hostname='mcp.example.com'\naccess_team_name='team'\naccess_audience_tag='aud'\n";
        let config: FileConfig = toml::from_str(source).unwrap();
        let snapshot = resolve_profile_in(&config, source, "direct").unwrap();
        assert_eq!(snapshot["mcp_cloud_hostname"], json!("mcp.example.com"));

        // Without `[cloud_mcp]` the field freezes as null, so the
        // owner-identity HTTP arm stays inert for the plugins.
        let plain = "[profiles.direct]\nharness='codex'\nmodel='gpt-6-luna'\n";
        let config: FileConfig = toml::from_str(plain).unwrap();
        let snapshot = resolve_profile_in(&config, plain, "direct").unwrap();
        assert_eq!(snapshot["mcp_cloud_hostname"], Value::Null);
    }

    #[test]
    fn mcp_policy_defaults_to_minimal_and_freezes_minimal_where_supported() {
        let direct = "[profiles.direct]\nharness='codex'\nmodel='gpt-6-luna'\n";
        let snapshot = resolve_text_profile(direct, "direct").unwrap();
        assert_eq!(snapshot["mcp"], "minimal");
        assert_eq!(snapshot["mcp_allow"], json!([]));

        let minimal = "[profiles.minimal]\nharness='codex'\nmodel='gpt-6-luna'\nmcp='minimal'\nmcp_allow=['owner-search']\n";
        let snapshot = resolve_text_profile(minimal, "minimal").unwrap();
        assert_eq!(snapshot["mcp"], "minimal");
        assert_eq!(snapshot["mcp_allow"], json!(["owner-search"]));

        // Unknown policies and malformed names fail at load; an explicit
        // `inherit` still freezes verbatim for every harness.
        for source in [
            "[profiles.bad]\nharness='codex'\nmodel='gpt-6-luna'\nmcp='isolated'\n",
            "[profiles.bad]\nharness='codex'\nmodel='gpt-6-luna'\nmcp='minimal'\nmcp_allow=['']\n",
            "[profiles.bad]\nharness='codex'\nmodel='gpt-6-luna'\nmcp='minimal'\nmcp_allow=['x', 'x']\n",
            "[profiles.bad]\nharness='codex'\nmodel='gpt-6-luna'\nmcp='minimal'\nmcp_allow=['eliot_swarm']\n",
            "[profiles.bad]\nharness='codex'\nmodel='gpt-6-luna'\nmcp='inherit'\nmcp_allow=['owner-search']\n",
        ] {
            let error = resolve_text_profile(source, "bad").expect_err("must reject");
            assert_eq!(error.code, "profile_error", "{source}");
        }
        let inherit = "[profiles.ih]\nharness='codex'\nmodel='gpt-6-luna'\nmcp='inherit'\n";
        let snapshot = resolve_text_profile(inherit, "ih").unwrap();
        assert_eq!(snapshot["mcp"], "inherit");
        assert_eq!(snapshot["mcp_allow"], json!([]));

        // Claude and OpenCode implement minimal without selective inclusion,
        // so a non-empty allow list is unsupported there; only Codex maps
        // it. Muse and Antigravity keep inherit only: no harness isolation
        // mechanism is verified, so an omitted `mcp` resolves to `inherit`
        // there (existing profiles keep loading); explicit `minimal` fails.
        let claude_cmd = if cfg!(windows) {
            "C:/Users/test/.local/bin/claude.exe"
        } else {
            "/tmp/eliot-claude-cmd-test/claude"
        };
        let source = format!(
            "[profiles.claude]\nharness='claude'\nmodel='claude-opus-4-6'\ncmd='{claude_cmd}'\nmcp='minimal'\n"
        );
        let snapshot = resolve_text_profile(&source, "claude").unwrap();
        assert_eq!(snapshot["mcp"], "minimal");
        assert_eq!(snapshot["mcp_allow"], json!([]));

        for (source, code) in [
            (
                format!(
                    "[profiles.claude]\nharness='claude'\nmodel='claude-opus-4-6'\ncmd='{claude_cmd}'\nmcp='minimal'\nmcp_allow=['owner-search']\n"
                ),
                "unsupported",
            ),
            (
                format!(
                    "[profiles.claude]\nharness='claude'\nmodel='claude-opus-4-6'\ncmd='{claude_cmd}'\nmcp_allow=['owner-search']\n"
                ),
                "unsupported",
            ),
        ] {
            let error = resolve_text_profile(&source, "claude").expect_err("must reject");
            assert_eq!(error.code, code, "{source}");
        }

        // OpenCode minimal resolves through the isolated per-Agent serve;
        // its allow list stays unsupported like Claude's.
        for (name, source) in [
            (
                "opencode",
                "[profiles.opencode]\nharness='opencode'\nmodel='openrouter/stealth/crouton-alpha'\ncmd='C:/Tools/OpenCode/opencode.exe'\n",
            ),
            (
                "opencode",
                "[profiles.opencode]\nharness='opencode'\nmodel='openrouter/stealth/crouton-alpha'\ncmd='C:/Tools/OpenCode/opencode.exe'\nmcp='minimal'\n",
            ),
        ] {
            let snapshot = resolve_text_profile(source, name).unwrap();
            assert_eq!(snapshot["mcp"], "minimal");
            assert_eq!(snapshot["mcp_allow"], json!([]));
        }
        for (name, source, code) in [
            (
                "opencode",
                "[profiles.opencode]\nharness='opencode'\nmodel='openrouter/stealth/crouton-alpha'\ncmd='C:/Tools/OpenCode/opencode.exe'\nmcp='minimal'\nmcp_allow=['owner-search']\n",
                "unsupported",
            ),
            (
                "muse",
                "[profiles.muse]\nharness='muse'\nmodel='m'\ncmd='C:/Tools/MuseCode/muse-bin-1.4.0-R4161.1.exe'\nmcp='minimal'\n",
                "unsupported",
            ),
            (
                "agy",
                "[profiles.agy]\nharness='antigravity'\nmodel='test-model'\ncmd='C:/Tools/Agy/agy.exe'\nmcp='minimal'\n",
                "unsupported",
            ),
            (
                "muse",
                "[profiles.muse]\nharness='muse'\nmodel='m'\ncmd='C:/Tools/MuseCode/muse-bin-1.4.0-R4161.1.exe'\n",
                "ok",
            ),
            (
                "agy",
                "[profiles.agy]\nharness='antigravity'\nmodel='test-model'\ncmd='C:/Tools/Agy/agy.exe'\n",
                "ok",
            ),
            (
                "muse",
                "[profiles.muse]\nharness='muse'\nmodel='m'\ncmd='C:/Tools/MuseCode/muse-bin-1.4.0-R4161.1.exe'\nmcp='inherit'\n",
                "ok",
            ),
        ] {
            if code == "ok" {
                let snapshot = resolve_text_profile(source, name).unwrap();
                assert_eq!(snapshot["mcp"], "inherit");
                continue;
            }
            let error = resolve_text_profile(source, name).expect_err("must reject");
            assert_eq!(error.code, code, "{source}");
        }
    }

    #[test]
    fn claude_profile_freezes_model_and_cmd() {
        // Absolute native path on every platform (no existence check).
        let cmd = if cfg!(windows) {
            "C:/Users/test/.local/bin/claude.exe"
        } else {
            "/tmp/eliot-claude-cmd-test/claude"
        };
        let source = format!(
            "[profiles.claude]\nharness = \"claude\"\ncmd = '{cmd}'\nmodel = \"claude-opus-4-6\"\npass_env = [\"EXAMPLE_API_KEY\"]\n"
        );
        let snapshot = resolve_text_profile(&source, "claude").unwrap();
        assert_eq!(snapshot["harness"], "claude");
        assert_eq!(snapshot["cmd"], cmd);
        assert_eq!(snapshot["model"], "claude-opus-4-6");
        assert_eq!(snapshot["permissions"], "workspace-write");
        assert_eq!(snapshot["approval_policy"], "never");
        assert!(snapshot.get("permission_mode").is_none());
        assert!(snapshot.get("effort").is_none());
        assert_eq!(snapshot["pass_env"], json!(["EXAMPLE_API_KEY"]));

        // The CLI `--effort` vocabulary freezes verbatim under `effort`.
        let source = format!(
            "[profiles.claude]\nharness = \"claude\"\ncmd = '{cmd}'\nmodel = \"claude-opus-4-6\"\neffort = \"xhigh\"\n"
        );
        let snapshot = resolve_text_profile(&source, "claude").unwrap();
        assert_eq!(snapshot["effort"], "xhigh");

        // The approval vocabulary is the CLI permission-mode vocabulary and
        // freezes under the CLI-exact key. Both cases use the platform-native
        // cmd so Unix rejects (or accepts) for the approval value, not the path.
        let source = format!(
            "[profiles.claude]\nharness = \"claude\"\ncmd = '{cmd}'\nmodel = \"claude-opus-4-6\"\napproval = \"plan\"\n"
        );
        let snapshot = resolve_text_profile(&source, "claude").unwrap();
        assert_eq!(snapshot["permission_mode"], "plan");
        let source = format!(
            "[profiles.claude]\nharness='claude'\nmodel='claude-opus-4-6'\ncmd='{cmd}'\napproval='promptUnmatched'\n"
        );
        let error =
            resolve_text_profile(&source, "claude").expect_err("unknown permission mode must fail");
        assert_eq!(error.code, "profile_error");

        for source in [
            "[profiles.claude]\nharness='claude'\n",
            "[profiles.claude]\nharness='claude'\nmodel='claude-opus-4-6'\neffort='extreme'\n",
            "[profiles.claude]\nharness='claude'\nmodel='claude-opus-4-6'\ncollaboration_mode='plan'\n",
            "[profiles.claude]\nharness='claude'\nmodel='claude-opus-4-6'\npermissions='read-only'\n",
            "[profiles.claude]\nharness='claude'\nmodel='claude-opus-4-6'\napproval_policy='on-request'\n",
        ] {
            assert!(resolve_text_profile(source, "claude").is_err(), "{source}");
        }
        // No PATH default: an explicit native cmd is required, and script
        // wrappers are rejected (their PID would orphan the CLI on stop).
        for source in [
            "[profiles.claude]\nharness='claude'\nmodel='claude-opus-4-6'\n",
            "[profiles.claude]\nharness='claude'\nmodel='claude-opus-4-6'\ncmd='C:/Tools/Claude/claude.cmd'\n",
            "[profiles.claude]\nharness='claude'\nmodel='claude-opus-4-6'\ncmd='C:/Tools/Claude/claude.bat'\n",
            "[profiles.claude]\nharness='claude'\nmodel='claude-opus-4-6'\ncmd='C:/Tools/Claude/launch.ps1'\n",
        ] {
            let error = resolve_text_profile(source, "claude").expect_err("must reject");
            assert_eq!(error.code, "profile_error", "{source}");
        }
    }

    #[test]
    fn claude_cmd_requires_an_absolute_native_path() {
        // Bare names, relative paths and script wrappers are rejected on
        // every platform (same code; the Windows/Unix reason differs where
        // the path is absolute on one and relative on the other).
        for cmd in [
            "claude",
            "claude.exe",
            "./claude.exe",
            "Tools/Claude/claude.exe",
            "C:/Tools/Claude/claude.js",
            "/tmp/claude.js",
            "/tmp/claude.cmd",
            "/tmp/claude.bat",
            "/tmp/claude.ps1",
        ] {
            let source = format!(
                "[profiles.claude]\nharness='claude'\nmodel='claude-opus-4-6'\ncmd='{cmd}'\n"
            );
            let error = resolve_text_profile(&source, "claude").expect_err("must reject");
            assert_eq!(error.code, "profile_error", "{cmd}");
        }
        // Unix native binaries carry no extension; Windows must name the .exe.
        #[cfg(unix)]
        {
            let source = "[profiles.claude]\nharness='claude'\nmodel='claude-opus-4-6'\ncmd='/tmp/eliot-claude-cmd-test/claude'\n";
            let snapshot =
                resolve_text_profile(source, "claude").expect("unix accepts an absolute path");
            assert_eq!(snapshot["cmd"], "/tmp/eliot-claude-cmd-test/claude");
        }
        #[cfg(windows)]
        {
            let source = "[profiles.claude]\nharness='claude'\nmodel='claude-opus-4-6'\ncmd='C:/Tools/Claude/claude'\n";
            let error = resolve_text_profile(source, "claude").expect_err("windows needs .exe");
            assert_eq!(error.code, "profile_error");
        }
    }

    #[test]
    fn codex_keeps_accepting_script_wrapper_cmds() {
        // Codex custody is the Agent Job, which ends the whole tree on stop.
        for cmd in [
            "C:/Tools/Codex/codex.cmd",
            "codex.bat",
            "C:/Tools/launch.ps1",
        ] {
            let source = format!(
                "[profiles.codex-luna]\nharness='codex'\nmodel='gpt-6-luna'\ncmd='{cmd}'\n"
            );
            let snapshot =
                resolve_text_profile(&source, "codex-luna").expect("codex accepts wrappers");
            assert_eq!(snapshot["cmd"], cmd, "{source}");
        }
        let source = "[profiles.codex-luna]\nharness='codex'\nmodel='gpt-6-luna'\ncmd='C:/Tools/Codex/codex.exe'\n";
        let snapshot = resolve_text_profile(source, "codex-luna").unwrap();
        assert_eq!(snapshot["cmd"], "C:/Tools/Codex/codex.exe");
    }

    #[test]
    fn opencode_profile_freezes_slash_model_and_explicit_cmd() {
        let source = r#"
[profiles.oc]
harness = "opencode"
cmd = "C:/Users/owner/AppData/Roaming/npm/node_modules/@opencode/cli/bin/opencode.exe"
model = "opencode/big-pickle"
pass_env = ["OPENAI_API_KEY"]
"#;
        let snapshot = resolve_text_profile(source, "oc").unwrap();
        assert_eq!(snapshot["harness"], "opencode");
        assert_eq!(
            snapshot["cmd"],
            "C:/Users/owner/AppData/Roaming/npm/node_modules/@opencode/cli/bin/opencode.exe"
        );
        assert_eq!(snapshot["model"], "opencode/big-pickle");
        assert_eq!(snapshot["pass_env"], json!(["OPENAI_API_KEY"]));
        // Control freezes like every other harness snapshot (#269): unset
        // means self, an explicit tree survives the freeze.
        assert_eq!(snapshot["control"], "self");
        let source = format!("{source}control = \"tree\"\n");
        let snapshot = resolve_text_profile(&source, "oc").unwrap();
        assert_eq!(snapshot["control"], "tree");

        // The first slash separates the provider; empty segments are invalid.
        for model in ["big-pickle", "/big-pickle", "opencode/", "a//b", ""] {
            let source = format!(
                "[profiles.oc]\nharness='opencode'\nmodel='{model}'\ncmd='C:/Tools/opencode.exe'\n"
            );
            let error = resolve_text_profile(&source, "oc").expect_err("must reject");
            assert_eq!(error.code, "profile_error", "{source}");
        }
        // Unconnected presets stay rejected, never silently frozen.
        for field in [
            "effort='high'",
            "approval='allowAll'",
            "collaboration_mode='plan'",
            "permissions='read-only'",
            "approval_policy='on-request'",
        ] {
            let source = format!(
                "[profiles.oc]\nharness='opencode'\nmodel='opencode/big-pickle'\ncmd='C:/Tools/opencode.exe'\n{field}\n"
            );
            let error = resolve_text_profile(&source, "oc").expect_err("must reject");
            assert_eq!(error.code, "unsupported", "{source}");
        }
        // The Muse treatment: an explicit native cmd is required, and script
        // wrappers are rejected (stop and registration address the exact
        // spawned server PID; `opencode.cmd` would interpose `cmd.exe`).
        for source in [
            "[profiles.oc]\nharness='opencode'\nmodel='opencode/big-pickle'\n",
            "[profiles.oc]\nharness='opencode'\nmodel='opencode/big-pickle'\ncmd='C:/Tools/OpenCodeCLI/opencode.cmd'\n",
            "[profiles.oc]\nharness='opencode'\nmodel='opencode/big-pickle'\ncmd='C:/Tools/opencode.bat'\n",
            "[profiles.oc]\nharness='opencode'\nmodel='opencode/big-pickle'\ncmd='C:/Tools/launch.ps1'\n",
        ] {
            let error = resolve_text_profile(source, "oc").expect_err("must reject");
            assert_eq!(error.code, "profile_error", "{source}");
        }
    }

    #[test]
    fn opencode_profile_keeps_nested_catalog_model_and_rejects_invalid_separators() {
        let model = "openrouter/stealth/crouton-alpha";
        let source = format!(
            "[profiles.oc]\nharness='opencode'\nmodel='{model}'\ncmd='C:/Tools/opencode.exe'\n"
        );
        let snapshot = resolve_text_profile(&source, "oc").expect("exact native catalog model");
        assert_eq!(snapshot["model"], model);
        for model in [
            "openrouter//crouton-alpha",
            "openrouter/stealth//crouton-alpha",
            "openrouter/stealth/",
            "openrouter\\stealth\\crouton-alpha",
            "openrouter/stealth\\crouton-alpha",
            "open#router/stealth/crouton-alpha",
            "openrouter/stealth/crouton-alpha#",
            "openrouter/stealth/crouton-alpha#default",
            "openrouter/stealth/crouton-alpha##default",
            "openrouter/stealth/crouton-alpha\n",
            "\topenrouter/stealth/crouton-alpha",
            "openrouter/stealth/ crouton-alpha",
            "openrouter/stealth/\0crouton-alpha",
        ] {
            let model_literal = serde_json::to_string(model).unwrap();
            let source = format!(
                "[profiles.oc]\nharness='opencode'\nmodel={model_literal}\ncmd='C:/Tools/opencode.exe'\n"
            );
            assert_eq!(
                resolve_text_profile(&source, "oc")
                    .expect_err("invalid or unmapped model syntax")
                    .code,
                "profile_error",
                "{model:?}"
            );
        }
    }

    #[test]
    fn opencode_local_backend_freezes_explicit_provider_and_limits() {
        let source = r#"
[profiles.local]
harness = "opencode"
cmd = "C:/Tools/OpenCode/opencode.exe"
model = "eliot-local/qwen3-8b"
provider_base_url = "http://127.0.0.1:1234/v1"
backend = "lmstudio"
context_limit = 131072
output_limit = 8192
"#;
        let snapshot = resolve_text_profile(source, "local").unwrap();
        assert_eq!(snapshot["harness"], "opencode");
        assert_eq!(snapshot["model"], "eliot-local/qwen3-8b");
        assert_eq!(snapshot["provider_base_url"], "http://127.0.0.1:1234/v1");
        assert_eq!(snapshot["backend"], "lmstudio");
        assert_eq!(snapshot["context_limit"], 131072);
        assert_eq!(snapshot["output_limit"], 8192);
        assert_eq!(
            snapshot["provider_package"],
            "@opencode/ai/providers/openai-compatible"
        );
        assert!(snapshot.get("auth_integration").is_none());
        assert!(snapshot.get("api_key_env").is_none());

        // Env-key mode freezes only the name; the value stays runtime-only.
        let source = r#"
[profiles.local]
harness = "opencode"
cmd = "C:/Tools/OpenCode/opencode.exe"
model = "eliot-local/mistral-7b"
provider_base_url = "http://127.0.0.1:8080/v1"
backend = "llama.cpp"
api_key_env = "LOCAL_LLM_KEY"
pass_env = ["LOCAL_LLM_KEY"]
context_limit = 65536
output_limit = 4096
"#;
        let snapshot = resolve_text_profile(source, "local").unwrap();
        assert_eq!(snapshot["api_key_env"], "LOCAL_LLM_KEY");
        assert_eq!(snapshot["pass_env"], json!(["LOCAL_LLM_KEY"]));
        assert!(snapshot.get("auth_integration").is_none());

        // Backend aliases normalize to the canonical preset kind.
        for (raw, canonical) in [
            ("llama-cpp", "llama.cpp"),
            ("LM-Studio", "lmstudio"),
            ("Unsloth-Studio", "unsloth"),
        ] {
            let source = format!(
                "[profiles.local]\nharness='opencode'\nmodel='eliot-local/m'\n\
                 cmd='C:/Tools/OpenCode/opencode.exe'\nprovider_base_url='http://127.0.0.1:8080'\n\
                 backend='{raw}'\ncontext_limit=32768\noutput_limit=4096\n"
            );
            let snapshot = resolve_text_profile(&source, "local").unwrap();
            assert_eq!(snapshot["backend"], canonical, "{raw}");
        }
    }

    #[test]
    fn opencode_local_backend_rejects_mixed_or_partial_profiles() {
        let base = "[profiles.local]\nharness='opencode'\nmodel='eliot-local/m'\n\
            cmd='C:/Tools/OpenCode/opencode.exe'\nprovider_base_url='http://127.0.0.1:8080'\n\
            backend='llama.cpp'\ncontext_limit=32768\noutput_limit=4096\n";
        // A canonical credential must never ride the local path.
        let error =
            resolve_text_profile(&format!("{base}auth_integration='openrouter'\n"), "local")
                .expect_err("local plus canonical auth must fail");
        assert_eq!(error.code, "profile_error");
        // A non-local provider segment cannot carry local fields.
        let error = resolve_text_profile(
            &base.replace("model='eliot-local/m'", "model='openrouter/m'"),
            "local",
        )
        .expect_err("non-local provider must fail");
        assert_eq!(error.code, "profile_error");
        // Unknown backend kinds are not frozen.
        let error = resolve_text_profile(
            &base.replace("backend='llama.cpp'", "backend='unknown-rt'"),
            "local",
        )
        .expect_err("unknown backend must fail");
        assert_eq!(error.code, "profile_error");
        // Partial local profiles fail closed instead of falling back.
        for source in [
            base.replace("provider_base_url='http://127.0.0.1:8080'\n", ""),
            base.replace("backend='llama.cpp'\n", ""),
            base.replace("context_limit=32768\n", ""),
            base.replace("output_limit=4096\n", ""),
            "[profiles.local]\nharness='opencode'\nmodel='eliot-local/m'\n\
             cmd='C:/Tools/OpenCode/opencode.exe'\nbackend='ollama'\n\
             context_limit=32768\noutput_limit=4096\n"
                .to_string(),
        ] {
            let error =
                resolve_text_profile(&source, "local").expect_err("partial local must fail");
            assert_eq!(error.code, "profile_error", "{source}");
        }
        // The key reference must be an allowed pass_env name, never a
        // native-store selector, and bad endpoints/limits fail.
        for (source, fragment) in [
            (format!("{base}api_key_env='MISSING_KEY'\n"), "pass_env"),
            (
                format!("{base}api_key_env='CODEX_HOME'\npass_env=['CODEX_HOME']\n"),
                "native store",
            ),
            (
                base.replace(
                    "provider_base_url='http://127.0.0.1:8080'",
                    "provider_base_url='ftp://127.0.0.1:8080'",
                ),
                "provider_base_url",
            ),
            (
                base.replace("context_limit=32768", "context_limit=0"),
                "context_limit",
            ),
            (
                base.replace("output_limit=4096", "output_limit=-1"),
                "output_limit",
            ),
        ] {
            let error = resolve_text_profile(&source, "local").expect_err("must reject");
            assert!(
                error.message.contains(fragment),
                "{fragment} missing in {}",
                error.message
            );
        }
        // Local fields have no mapping on other harnesses, including Codex:
        // Codex against these backends stays unsupported.
        for (profile, source) in [
            (
                "c",
                "[profiles.c]\nharness='codex'\nmodel='gpt-6-luna'\nprovider_base_url='http://127.0.0.1:8080'\n",
            ),
            (
                "m",
                "[profiles.m]\nharness='muse'\nmodel='m'\ncmd='C:/Tools/MuseCode/muse-bin-1.4.0-R4161.1.exe'\nbackend='ollama'\n",
            ),
            (
                "c",
                "[profiles.c]\nharness='claude'\nmodel='claude-opus-4-6'\ncmd='C:/Tools/Claude/claude.exe'\ncontext_limit=32768\n",
            ),
        ] {
            let error = resolve_text_profile(source, profile).expect_err("must reject");
            assert_eq!(error.code, "unsupported", "{source}");
        }
    }

    #[test]
    fn missing_fields_and_values_containing_unknown_field_text_remain_profile_errors() {
        let missing = config_error("[profiles.codex-luna]\nmodel = 'gpt-6-luna'\n");
        assert_eq!(missing.code, "profile_error");

        let wrong_value = config_error(
            "[profiles.codex-luna]\nharness = 'codex'\npass_env = 'unknown field `proxy`'\n",
        );
        assert_eq!(wrong_value.code, "profile_error");
        assert!(!wrong_value.message.contains("unknown field `proxy`"));
        assert!(
            !wrong_value
                .message
                .contains("unsupported configuration field")
        );
    }

    fn desktop_settings(source: &str) -> DesktopSettings {
        let config: FileConfig = toml::from_str(source).expect("valid TOML");
        config.desktop
    }

    #[test]
    fn desktop_picker_rejects_raw_whitespace_controls_and_oversize_strings() {
        for list in ["favorite_models", "recent_models"] {
            for (field, value) in [
                ("harness", " codex".to_owned()),
                ("harness", "codex\n".to_owned()),
                ("harness", format!("{}codex", " ".repeat(300))),
                ("model", " m".to_owned()),
                ("model", "m\t".to_owned()),
                ("model", format!("m{}", " ".repeat(256))),
                ("model", "é".repeat(129)),
                ("effort", "low ".to_owned()),
            ] {
                let mut entry = DesktopModelRef {
                    harness: "codex".to_owned(),
                    model: "m".to_owned(),
                    effort: None,
                };
                match field {
                    "harness" => entry.harness = value,
                    "model" => entry.model = value,
                    _ => entry.effort = Some(value),
                }
                let settings = if list == "favorite_models" {
                    DesktopSettings {
                        favorite_models: vec![entry],
                        ..Default::default()
                    }
                } else {
                    DesktopSettings {
                        recent_models: vec![entry],
                        ..Default::default()
                    }
                };
                assert_eq!(
                    settings.validate().unwrap_err().code,
                    "profile_error",
                    "{list} {field}"
                );
            }
        }
        for access in [" ask", "full\n", "auto "] {
            assert!(
                DesktopSettings {
                    last_access: Some(access.to_owned()),
                    ..Default::default()
                }
                .validate()
                .is_err()
            );
        }
        assert!(
            DesktopSettings {
                collapsed_projects: vec![" app".to_owned()],
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn desktop_picker_lists_validate_bounds_and_harness() {
        desktop_settings("[desktop]\n").validate().unwrap();
        let valid = desktop_settings(
            "[desktop]\nfavorite_models = [{harness = 'codex', model = 'gpt-6-luna', effort = 'low'}]\nrecent_models = [{harness = 'opencode', model = 'opencode/deepseek-v4.1-flash-free'}]\ncollapsed_projects = ['app']\nlast_access = 'auto'\n",
        );
        valid.validate().unwrap();
        let too_many_favorites = DesktopSettings {
            favorite_models: vec![
                DesktopModelRef {
                    harness: "codex".to_owned(),
                    model: "m".to_owned(),
                    effort: None,
                };
                MAX_DESKTOP_FAVORITES + 1
            ],
            ..Default::default()
        };
        assert_eq!(
            too_many_favorites.validate().unwrap_err().code,
            "profile_error"
        );
        let too_many_recents = DesktopSettings {
            recent_models: vec![
                DesktopModelRef {
                    harness: "codex".to_owned(),
                    model: "m".to_owned(),
                    effort: None,
                };
                MAX_DESKTOP_RECENTS + 1
            ],
            ..Default::default()
        };
        assert_eq!(
            too_many_recents.validate().unwrap_err().code,
            "profile_error"
        );
        let bad_harness = DesktopSettings {
            favorite_models: vec![DesktopModelRef {
                harness: "fake".to_owned(),
                model: "m".to_owned(),
                effort: None,
            }],
            ..Default::default()
        };
        assert_eq!(bad_harness.validate().unwrap_err().code, "profile_error");
        let bad_access = DesktopSettings {
            last_access: Some("sometimes".to_owned()),
            ..Default::default()
        };
        assert_eq!(bad_access.validate().unwrap_err().code, "profile_error");
    }
}
