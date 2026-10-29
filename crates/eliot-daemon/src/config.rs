//! Minimal global profile loader for the daemon data directory.
//!
//! Owning contract: docs/spec/configuration.md first profile and
//! docs/harnesses/codex.md first useful path. Only the single global
//! profiles map is implemented here. Later configuration layers (proxy
//! routes, extends, overlays, accounts, services, automation and project
//! overlays) are rejected with unsupported, never silently substituted.
//!
//! Returns a bounded nonsecret snapshot object with the keys harness, cmd,
//! model, effort, collaboration_mode, permissions, approval_policy, pass_env,
//! raw setup and normalized relative scratch directories.
//! pass_env carries admitted environment variable names. Only the two nonsecret
//! native-store selector inputs are captured before admission; all other values
//! remain execution-time references and are never copied into the snapshot.
use eliot_core as core;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::io::Read;
use std::path::Path;
const MAX_CONFIG_BYTES: usize = 128 * 1024;
const MAX_PROFILE_NAME_LEN: usize = 128;
const MAX_PROFILES: usize = 256;
const MAX_PROJECT_PATH_LEN: usize = 4096;
const MAX_MODEL_LEN: usize = 256;
const MAX_EFFORT_LEN: usize = 128;
const MAX_CMD_LEN: usize = 1024;
const MAX_ENV_NAME_LEN: usize = 256;
const MAX_PASS_ENV_ENTRIES: usize = 64;
const FAKE_PROFILE: &str = "fake";
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
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutomationConfig {
    #[serde(default = "default_max_event_backlog")]
    pub(crate) max_event_backlog: i64,
}

impl Default for AutomationConfig {
    fn default() -> Self {
        Self {
            max_event_backlog: default_max_event_backlog(),
        }
    }
}

impl AutomationConfig {
    fn validate(self) -> core::Result<Self> {
        if self.max_event_backlog <= 0 {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: automation.max_event_backlog must be a positive integer",
            ));
        }
        Ok(self)
    }
}

fn default_max_event_backlog() -> i64 {
    100_000
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
    collaboration_mode: Option<String>,
    #[serde(default)]
    permissions: Option<String>,
    #[serde(default)]
    approval_policy: Option<String>,
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
    if config.profiles.contains_key(FAKE_PROFILE) {
        return Err(core::Error::new(
            "profile_error",
            "config.toml: profiles.fake: builtin profile cannot be overridden",
        ));
    }

    let global_profiles: HashSet<&str> = config
        .profiles
        .keys()
        .map(String::as_str)
        .chain(std::iter::once(FAKE_PROFILE))
        .collect();
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
            |action| matches!(action, "stop" | "send"),
        )?;

        projects.push(ProjectRegistrationConfig {
            id: id.clone(),
            path: path.to_owned(),
            profiles,
            actions,
        });
    }
    projects.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(projects)
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
                    "only implemented stop and send actions may be approved"
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
pub(crate) fn resolve_profile(data_root: &Path, name: &str) -> core::Result<Value> {
    validate_profile_name(name)?;
    let Some((config, source)) = read_config(data_root)? else {
        return if name == FAKE_PROFILE {
            Ok(fake_snapshot())
        } else {
            Err(core::Error::new(
                "profile_error",
                format!("config.toml: profiles.{name}: profile not found"),
            ))
        };
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
    if let Some(fake) = config.profiles.get(FAKE_PROFILE) {
        return Err(config_error_at(
            &source,
            fake.span().start,
            "profiles.fake",
            "builtin profile cannot be overridden",
            "profile_error",
        ));
    }
    if name == FAKE_PROFILE {
        return Ok(fake_snapshot());
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
    let Some((config, _source)) = read_config(data_root)? else {
        return Ok(AutomationConfig::default());
    };
    config.automation.validate()
}

fn fake_snapshot() -> Value {
    let mut snapshot = fake_profile_snapshot();
    snapshot["capacity_scopes"] = json!([{"scope":"profile:fake", "max_live":null}]);
    snapshot
}
fn fake_profile_snapshot() -> Value {
    json!({
        "harness": "fake",
        "permissions": PERMISSION_WORKSPACE_WRITE,
        "approval_policy": APPROVAL_POLICY_NEVER,
        "pass_env": [],
        "scratch": default_scratch(),
    })
}
fn snapshot_from_entry(
    name: &str,
    entry: &ProfileEntry,
    table_offset: usize,
    source: &str,
) -> core::Result<Value> {
    let harness = entry.harness.trim();
    if harness == FAKE_PROFILE {
        if entry.proxy.is_some()
            || entry.cmd.is_some()
            || entry.model.is_some()
            || entry.effort.is_some()
            || entry.collaboration_mode.is_some()
            || entry.permissions.is_some()
            || entry.approval_policy.is_some()
            || entry.pass_env.is_some()
            || !entry.setup.is_empty()
            || entry.scratch != default_scratch()
        {
            return Err(profile_field_error(
                source,
                table_offset,
                name,
                "harness",
                "named fake profiles accept only account and capacity settings",
                "profile_error",
            ));
        }
        return Ok(fake_profile_snapshot());
    }
    if harness != "codex" {
        return Err(profile_field_error(
            source,
            table_offset,
            name,
            "harness",
            "unsupported harness",
            "unsupported",
        ));
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

        if entry
            .effort
            .as_deref()
            .is_none_or(|effort| effort.trim().is_empty())
        {
            return Err(fail(
                "effort",
                "a routed Codex profile requires an explicit effort",
            ));
        }

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
        if name.is_empty() || name.len() > MAX_ENV_NAME_LEN {
            return Err(profile_field_error(
                source,
                offset,
                profile,
                "pass_env",
                "names must be 1-256 characters",
                "profile_error",
            ));
        }
        let mut chars = name.chars();
        let first_ok = chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
        let rest_ok = chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !first_ok || !rest_ok {
            return Err(profile_field_error(
                source,
                offset,
                profile,
                "pass_env",
                "names must use letters, digits and underscore",
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
            "[projects.sample]\npath = {project_path}\nprofiles = ['fake']\nactions = ['stop']\n\
             [automation]\nmax_event_backlog = 4096\n"
        ));
        assert_eq!(
            load_automation_config(&fixture.root).unwrap(),
            AutomationConfig {
                max_event_backlog: 4096
            }
        );
        assert_eq!(
            load_project_registration_config(&fixture.root).unwrap(),
            vec![ProjectRegistrationConfig {
                id: "sample".to_owned(),
                path: fixture.root.to_string_lossy().into_owned(),
                profiles: vec!["fake".to_owned()],
                actions: vec!["stop".to_owned()],
            }]
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
    fn l34_requires_explicit_health_base_and_effort_without_fallbacks() {
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
        assert!(resolve_text_profile(without_effort, "routed").is_err());

        let direct = "[profiles.direct]\nharness='codex'\nmodel='gpt-6-luna'\n";
        let direct = resolve_text_profile(direct, "direct").unwrap();
        assert!(direct.get("effort").is_none());
        assert!(direct.get("route_binding").is_none());
    }

    #[test]
    fn l34_fake_profiles_allow_only_caps_and_account_selection() {
        let source = r#"
[profiles.fake-small]
harness = "fake"
account = "shared"
max_live = 0
[accounts.shared]
max_live = 3
"#;
        let snapshot = resolve_text_profile(source, "fake-small").unwrap();
        assert_eq!(snapshot["harness"], "fake");
        assert_eq!(snapshot["account"], "shared");
        assert!(snapshot.get("model").is_none());
        assert!(snapshot.get("route_binding").is_none());
        assert_eq!(
            snapshot["capacity_scopes"],
            json!([
                {"scope":"profile:fake-small", "max_live":0},
                {"scope":"account:shared", "max_live":3}
            ])
        );

        for source in [
            "[profiles.fake-proxy]\nharness='fake'\nproxy='opencodex'\n",
            "[profiles.fake-cmd]\nharness='fake'\ncmd='codex'\n",
            "[profiles.fake-model]\nharness='fake'\nmodel='some-model'\n",
        ] {
            let config: FileConfig = toml::from_str(source).unwrap();
            let (name, entry) = config.profiles.iter().next().unwrap();
            assert!(
                snapshot_from_entry(name, entry.get_ref(), entry.span().start, source).is_err()
            );
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
