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
}

impl Default for AutomationConfig {
    fn default() -> Self {
        Self {
            max_event_backlog: default_max_event_backlog(),
            run: RunGrantConfig::default(),
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
    pub(crate) retention: RetentionConfig,
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
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            retention: RetentionConfig::default(),
            limits: core::api::HierarchyLimits::default(),
            attention_suspicion_secs: default_attention_suspicion_secs(),
        }
    }
}

fn default_attention_suspicion_secs() -> i64 {
    15 * 60
}

impl DaemonConfig {
    fn validate(self) -> core::Result<Self> {
        self.limits.validate().map_err(|error| {
            core::Error::new("profile_error", format!("config.toml: {}", error.message))
        })?;
        if self.attention_suspicion_secs <= 0 || self.attention_suspicion_secs > i64::MAX / 1_000 {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: daemon.attention_suspicion_secs must be a positive integer whose millisecond value fits in i64",
            ));
        }
        Ok(Self {
            retention: self.retention.validate()?,
            limits: self.limits,
            attention_suspicion_secs: self.attention_suspicion_secs,
        })
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
    pass_env: Option<Vec<String>>,
    #[serde(default)]
    setup: Vec<crate::setup::SetupEntry>,
    #[serde(default = "default_scratch")]
    scratch: Vec<String>,
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
                &source,
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
                &source,
                offset,
                &format!("projects.{id}.path"),
                "must be a nonempty absolute checkout path of at most 4096 characters",
                "profile_error",
            ));
        }

        let profiles = normalize_project_allowlist(
            &entry.profiles,
            &source,
            offset,
            id,
            "profiles",
            |profile| global_profiles.contains(profile) && validate_profile_name(profile).is_ok(),
        )?;
        let actions = normalize_project_allowlist(
            &entry.actions,
            &source,
            offset,
            id,
            "actions",
            |action| {
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
                )
            },
        )?;
        let git = entry.git.clone();
        if let Some(git) = &git {
            validate_project_git_config(git, &source, offset, id)?;
        }
        let run_grant = normalize_run_grant(
            &entry.interpreters,
            &entry.run_env,
            &route_credential_env_names,
            &source,
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

pub(crate) fn resolve_profile(data_root: &Path, name: &str) -> core::Result<Value> {
    validate_profile_name(name)?;
    let Some((config, source)) = read_config(data_root)? else {
        return Err(core::Error::new(
            "profile_error",
            format!("config.toml: profiles.{name}: profile not found"),
        ));
    };
    if config.profiles.len() > MAX_PROFILES {
        return Err(core::Error::new(
            "profile_error",
            "config.toml: profiles: profile count exceeds 256 entries",
        ));
    }
    for (profile_name, entry) in &config.profiles {
        if validate_profile_name(profile_name).is_err() {
            return Err(config_error_at(
                &source,
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
                &source,
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
                &source,
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
    let snapshot = snapshot_from_entry(name, entry.get_ref(), entry.span().start, &source)?;
    bind_route_and_capacity(
        name,
        entry.get_ref(),
        &config,
        entry.span().start,
        &source,
        snapshot,
    )
}
fn read_config(data_root: &Path) -> core::Result<Option<(FileConfig, String)>> {
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
    let source = String::from_utf8(bytes)
        .map_err(|_| core::Error::new("profile_error", "config.toml: invalid UTF-8"))?;
    let config: FileConfig =
        toml::from_str(&source).map_err(|error| map_toml_error(&source, error))?;
    Ok(Some((config, source)))
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
    let model = entry
        .model
        .as_deref()
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .ok_or_else(|| {
            profile_field_error(
                source,
                table_offset,
                name,
                "model",
                "required non-empty model is missing",
                "profile_error",
            )
        })?;
    validate_model(name, model, source, table_offset)?;
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
    snapshot.insert("model".to_owned(), Value::String(model.to_owned()));
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
        "Codex",
        true,
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
    let model = entry
        .model
        .as_deref()
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .ok_or_else(|| {
            profile_field_error(
                source,
                table_offset,
                name,
                "model",
                "required non-empty model is missing",
                "profile_error",
            )
        })?;
    validate_model(name, model, source, table_offset)?;
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
    snapshot.insert("model".to_owned(), Value::String(model.to_owned()));
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
        "Muse",
        false,
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
/// callback. Reasoning effort, collaboration presets, non-default
/// permissions, and a non-`never` Eliot-side `approval_policy` stay rejected
/// until their mappings land.
fn claude_snapshot_from_entry(
    name: &str,
    entry: &ProfileEntry,
    table_offset: usize,
    source: &str,
) -> core::Result<Value> {
    let model = entry
        .model
        .as_deref()
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .ok_or_else(|| {
            profile_field_error(
                source,
                table_offset,
                name,
                "model",
                "required non-empty model is missing",
                "profile_error",
            )
        })?;
    validate_model(name, model, source, table_offset)?;
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
            "the Claude slice has no reasoning-effort mapping",
            "unsupported",
        ));
    }
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
    snapshot.insert("model".to_owned(), Value::String(model.to_owned()));
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
    freeze_mcp_policy(
        &mut snapshot,
        name,
        entry,
        table_offset,
        source,
        "Claude",
        true,
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

/// Freeze an OpenCode profile. The L6.2 PR1 slice carries an explicit model
/// id in the installed `provider/model` form (split at the first slash) and a required explicit
/// native `cmd` (the Muse treatment: stop and process registration address
/// the exact spawned PID, so `.cmd`/`.bat`/`.ps1` wrappers are rejected).
/// An optional nonsecret `auth_integration` names the one native integration
/// whose canonical credential the pre-serve transfer prepares; the plugin
/// requires it at admission. Effort, approval, collaboration, permission and
/// approval-policy presets have no OpenCode mapping and stay rejected until
/// one lands.
fn opencode_snapshot_from_entry(
    name: &str,
    entry: &ProfileEntry,
    table_offset: usize,
    source: &str,
) -> core::Result<Value> {
    let model = entry
        .model
        .as_deref()
        .filter(|model| !model.is_empty())
        .ok_or_else(|| {
            profile_field_error(
                source,
                table_offset,
                name,
                "model",
                "required non-empty model is missing",
                "profile_error",
            )
        })?;
    validate_model(name, model, source, table_offset)?;
    if split_provider_model(model).is_none() {
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
    snapshot.insert("model".to_owned(), Value::String(model.to_owned()));
    if let Some(selector) = entry.auth_integration.as_deref() {
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
        "OpenCode",
        false,
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
    let model = entry
        .model
        .as_deref()
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .ok_or_else(|| {
            profile_field_error(
                source,
                table_offset,
                name,
                "model",
                "required non-empty model is missing",
                "profile_error",
            )
        })?;
    validate_model(name, model, source, table_offset)?;
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
    snapshot.insert("model".to_owned(), Value::String(model.to_owned()));
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
        "Antigravity",
        false,
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
    if model.len() > MAX_MODEL_LEN || model.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(profile_field_error(
            source,
            offset,
            profile,
            "model",
            "must be a single token of at most 256 characters",
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

/// Validate the worker MCP inheritance policy and freeze it with the profile
/// snapshot. `supports_minimal` names whether the harness plugin implements
/// an isolation mechanism for `minimal` (Codex, Claude); other harnesses
/// keep `inherit` only and reject `minimal` as unsupported. A non-empty
/// allow list always requires `minimal`, so an allowed set cannot silently
/// ride on the default.
fn freeze_mcp_policy(
    snapshot: &mut serde_json::Map<String, Value>,
    name: &str,
    entry: &ProfileEntry,
    table_offset: usize,
    source: &str,
    harness: &str,
    supports_minimal: bool,
) -> core::Result<()> {
    let policy = core::McpPolicy::parse(entry.mcp.as_deref()).map_err(|error| {
        profile_field_error(
            source,
            table_offset,
            name,
            "mcp",
            &error.message,
            "profile_error",
        )
    })?;
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
                retention: RetentionConfig {
                    events_days: Some(7),
                    commands_days: Some(14),
                    mail_days: Some(21),
                },
                limits: core::api::HierarchyLimits::default(),
                attention_suspicion_secs: 15 * 60,
            }
        );

        fixture.write("[daemon]\nretention.events_days = 7\n");
        assert_eq!(
            load_daemon_config(&fixture.root).unwrap(),
            DaemonConfig {
                retention: RetentionConfig {
                    events_days: Some(7),
                    commands_days: None,
                    mail_days: None,
                },
                limits: core::api::HierarchyLimits::default(),
                attention_suspicion_secs: 15 * 60,
            }
        );
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
            "[profiles.muse-manager]\nharness = \"muse\"\nmodel = \"fixture-model\"\ncontrol = \"project\"\n\
             cmd = \"C:/Tools/MuseCode/muse-bin-1.4.0-R4161.1.exe\"\n",
        );
        let snapshot = resolve_profile(&fixture.root, "muse-manager").unwrap();
        assert_eq!(
            snapshot.get("control").and_then(Value::as_str),
            Some("project")
        );
        fixture.write(
            "[profiles.fleet]\nharness = \"codex\"\nmodel = \"fixture-model\"\ncontrol = \"fleet\"\n",
        );
        let error = resolve_profile(&fixture.root, "fleet").unwrap_err();
        assert_eq!(error.code, "profile_error");
        assert!(error.message.contains("control"), "{}", error.message);
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
permissions = "workspace-write"
"#,
            "muse-write",
        )
        .unwrap();
        assert_eq!(explicit_write["permissions"], "workspace-write");

        let read_only_err = resolve_text_profile(
            "[profiles.muse]\nharness='muse'\nmodel='muse-spark-1.3-contributor'\ncmd='C:/Tools/MuseCode/muse-bin-1.4.0-R4161.1.exe'\npermissions='read-only'\n",
            "muse",
        )
        .expect_err("read-only unsupported");
        assert_eq!(read_only_err.code, "unsupported");

        let dsp_err = resolve_text_profile(
            "[profiles.muse]\nharness='muse'\nmodel='muse-spark-1.3-contributor'\ncmd='C:/Tools/MuseCode/muse-bin-1.4.0-R4161.1.exe'\ndangerously_skip_permissions=true\n",
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
    fn mcp_policy_defaults_to_inherit_and_freezes_minimal_where_supported() {
        let direct = "[profiles.direct]\nharness='codex'\nmodel='gpt-6-luna'\n";
        let snapshot = resolve_text_profile(direct, "direct").unwrap();
        assert_eq!(snapshot["mcp"], "inherit");
        assert_eq!(snapshot["mcp_allow"], json!([]));

        let minimal = "[profiles.minimal]\nharness='codex'\nmodel='gpt-6-luna'\nmcp='minimal'\nmcp_allow=['owner-search']\n";
        let snapshot = resolve_text_profile(minimal, "minimal").unwrap();
        assert_eq!(snapshot["mcp"], "minimal");
        assert_eq!(snapshot["mcp_allow"], json!(["owner-search"]));

        // Unknown policies, dangling allow lists and malformed names fail
        // at load; an allow list never rides on the inherit default.
        for source in [
            "[profiles.bad]\nharness='codex'\nmodel='gpt-6-luna'\nmcp='isolated'\n",
            "[profiles.bad]\nharness='codex'\nmodel='gpt-6-luna'\nmcp_allow=['owner-search']\n",
            "[profiles.bad]\nharness='codex'\nmodel='gpt-6-luna'\nmcp='minimal'\nmcp_allow=['']\n",
            "[profiles.bad]\nharness='codex'\nmodel='gpt-6-luna'\nmcp='minimal'\nmcp_allow=['x', 'x']\n",
        ] {
            let error = resolve_text_profile(source, "bad").expect_err("must reject");
            assert_eq!(error.code, "profile_error", "{source}");
        }

        // Claude implements minimal; Muse, OpenCode and Antigravity keep
        // inherit only until a harness isolation mechanism is verified.
        let claude_cmd = if cfg!(windows) {
            "C:/Users/test/.local/bin/claude.exe"
        } else {
            "/tmp/eliot-claude-cmd-test/claude"
        };
        let source = format!(
            "[profiles.claude]\nharness='claude'\nmodel='claude-opus-4-6'\ncmd='{claude_cmd}'\nmcp='minimal'\nmcp_allow=['owner-search']\n"
        );
        let snapshot = resolve_text_profile(&source, "claude").unwrap();
        assert_eq!(snapshot["mcp"], "minimal");
        assert_eq!(snapshot["mcp_allow"], json!(["owner-search"]));

        for (name, source, code) in [
            (
                "muse",
                "[profiles.muse]\nharness='muse'\nmodel='m'\ncmd='C:/Tools/MuseCode/muse-bin-1.4.0-R4161.1.exe'\nmcp='minimal'\n",
                "unsupported",
            ),
            (
                "opencode",
                "[profiles.opencode]\nharness='opencode'\nmodel='openrouter/stealth/crouton-alpha'\ncmd='C:/Tools/OpenCode/opencode.exe'\nmcp='minimal'\n",
                "unsupported",
            ),
            (
                "agy",
                "[profiles.agy]\nharness='antigravity'\nmodel='test-model'\ncmd='C:/Tools/Agy/agy.exe'\nmcp='minimal'\n",
                "unsupported",
            ),
            (
                "muse",
                "[profiles.muse]\nharness='muse'\nmodel='m'\ncmd='C:/Tools/MuseCode/muse-bin-1.4.0-R4161.1.exe'\nmcp_allow=['owner-search']\n",
                "profile_error",
            ),
        ] {
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
        assert_eq!(snapshot["pass_env"], json!(["EXAMPLE_API_KEY"]));

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
            "[profiles.claude]\nharness='claude'\nmodel='claude-opus-4-6'\neffort='high'\n",
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
}
