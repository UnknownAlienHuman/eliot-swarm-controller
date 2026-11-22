//! Daemon configuration loading, validation and frozen profile resolution.
//!
//! Owning contract: docs/spec/configuration.md. The bounded global source
//! covers profiles and routes, project registrations, automation, daemon,
//! mail, cloud MCP and desktop settings. Unsupported layers remain explicit
//! errors. Project and automation helpers share this module's parser and
//! validation path; profile freezing and resolution stay here.
//!
//! Snapshots retain admitted environment names. Codex retains its native-store execution inputs. Private
//! route selectors are bound by digest and resolved only in the process environment.
use core::api::native_profile::{
    ANTIGRAVITY_MODES, CLAUDE_EFFORTS, CLAUDE_PERMISSION_MODES, CodexApprovalPolicy,
    CodexCollaborationMode, CodexPermissions, MUSE_APPROVALS, MUSE_PERMISSIONS,
};
use eliot_core as core;
use eliot_plugin::session::split_provider_model;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
mod automation;
pub(crate) mod profiles_edit;
mod projects;
pub(crate) mod recovery;
pub(crate) mod settings;

pub(crate) use automation::{
    AutomationConfig, RunGrantConfig, load_automation_config, load_automation_config_snapshot,
};
use projects::{ProjectEntry, project_registrations_in};
pub(crate) use projects::{
    ProjectRegistrationConfig, ProjectWorkspaceMode, load_project_display_name,
    load_project_registration_config, validate_chats_location,
};

const MAX_CONFIG_BYTES: usize = 128 * 1024;
const MAX_PROFILE_NAME_LEN: usize = 128;
const MAX_PROFILES: usize = 256;
const MAX_MODEL_LEN: usize = 256;
const MAX_EFFORT_LEN: usize = 128;
const MAX_AUTH_INTEGRATION_LEN: usize = 128;
const MAX_CMD_LEN: usize = 1024;
const MAX_ENV_NAME_LEN: usize = 256;
const MAX_PASS_ENV_ENTRIES: usize = 64;
const MAX_CHILD_PROFILES: usize = 64;

/// Codex dispatch already retains these execution inputs verbatim. Other
/// store and provider selectors are verified against an opaque route identity.
pub(crate) const CODEX_STORE_SELECTORS: &[&str] = &["CODEX_HOME", "CODEX_SQLITE_HOME"];
const PRIVATE_STORE_SELECTORS: &[&str] = &[
    "CLAUDE_CONFIG_DIR",
    "XDG_CONFIG_HOME",
    "XDG_DATA_HOME",
    "XDG_STATE_HOME",
    "XDG_CACHE_HOME",
    "OPENCODE_CONFIG",
    "OPENCODE_CONFIG_DIR",
    "APPDATA",
];
const PRIVATE_PROVIDER_SELECTORS: &[&str] = &[
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_AWS_BASE_URL",
    "ANTHROPIC_BEDROCK_BASE_URL",
    "ANTHROPIC_BEDROCK_MANTLE_BASE_URL",
    "ANTHROPIC_FOUNDRY_BASE_URL",
    "ANTHROPIC_FOUNDRY_RESOURCE",
    "ANTHROPIC_VERTEX_BASE_URL",
    "ANTHROPIC_VERTEX_PROJECT_ID",
];
/// The provider-mode subset of the core external-route markers, in their
/// declared order so the frozen route identity digest is unchanged.
const CLAUDE_PROVIDER_MODES: &[&str] = &[
    core::api::CLAUDE_EXTERNAL_ROUTE_MARKERS[3],
    core::api::CLAUDE_EXTERNAL_ROUTE_MARKERS[4],
    core::api::CLAUDE_EXTERNAL_ROUTE_MARKERS[5],
    core::api::CLAUDE_EXTERNAL_ROUTE_MARKERS[6],
    core::api::CLAUDE_EXTERNAL_ROUTE_MARKERS[7],
    core::api::CLAUDE_EXTERNAL_ROUTE_MARKERS[8],
];
const CLAUDE_ROUTE_CREDENTIALS: &[&str] = &[
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "CLAUDE_CODE_OAUTH_TOKEN",
];
const CLAUDE_MODEL_SELECTORS: &[&str] = &[
    "ANTHROPIC_MODEL",
    "ANTHROPIC_DEFAULT_MODEL",
    "ANTHROPIC_DEFAULT_FABLE_MODEL",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL",
    "ANTHROPIC_DEFAULT_OPUS_MODEL",
    "ANTHROPIC_DEFAULT_SONNET_MODEL",
    "ANTHROPIC_CUSTOM_MODEL_OPTION",
];
pub(crate) const ROUTE_HOME_SELECTORS: &[&str] = if cfg!(windows) {
    &["HOME", "USERPROFILE", "LOCALAPPDATA"]
} else {
    &["HOME"]
};

fn env_name_matches(name: &str, selector: &str) -> bool {
    if cfg!(windows) {
        name.eq_ignore_ascii_case(selector)
    } else {
        name == selector
    }
}

pub(crate) fn native_store_selector(name: &str) -> Option<&'static str> {
    CODEX_STORE_SELECTORS
        .iter()
        .chain(PRIVATE_STORE_SELECTORS)
        .copied()
        .find(|selector| env_name_matches(name, selector))
}

pub(crate) fn private_route_selector(name: &str) -> bool {
    native_store_selector(name).is_some()
        || PRIVATE_PROVIDER_SELECTORS
            .iter()
            .chain(CLAUDE_PROVIDER_MODES)
            .any(|selector| env_name_matches(name, selector))
}

/// Claude can echo a model pin as native metadata. An explicitly bound API
/// key still contributes a credential, even when it uses a model pin's name.
pub(crate) fn claude_model_selector(profile: &Value, name: &str) -> bool {
    profile["harness"] == "claude"
        && CLAUDE_MODEL_SELECTORS
            .iter()
            .any(|selector| env_name_matches(name, selector))
        && !api_key_name_matches(profile, name)
}

pub(crate) fn claude_provider_mode_flag(profile: &Value, name: &str, value: &Value) -> bool {
    profile["harness"] == "claude"
        && matches!(value.as_str(), Some("0" | "1" | "false" | "true"))
        && CLAUDE_PROVIDER_MODES
            .iter()
            .any(|selector| env_name_matches(name, selector))
        && !api_key_name_matches(profile, name)
}

fn api_key_name_matches(profile: &Value, name: &str) -> bool {
    profile["api_key_env"]
        .as_str()
        .is_some_and(|key| env_name_matches(name, key))
}

fn admitted_selector<'a>(names: &'a [Value], selector: &str) -> Option<&'a str> {
    names
        .iter()
        .filter_map(Value::as_str)
        .find(|name| env_name_matches(name, selector))
}

/// The identity binds admitted selector states and the default home inputs.
/// The serialized inputs exist only during hashing. Credential values never
/// enter the digest; presence binds the provider mode.
pub(crate) fn native_route_identity(
    profile: &Value,
    mut lookup: impl FnMut(&str) -> core::Result<Option<String>>,
) -> core::Result<String> {
    let names = profile
        .get("pass_env")
        .and_then(Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    let mut selectors = serde_json::Map::new();
    for &selector in ROUTE_HOME_SELECTORS
        .iter()
        .chain(CODEX_STORE_SELECTORS)
        .chain(PRIVATE_STORE_SELECTORS)
        .chain(PRIVATE_PROVIDER_SELECTORS)
        .chain(CLAUDE_PROVIDER_MODES)
        .chain(CLAUDE_ROUTE_CREDENTIALS)
    {
        let admitted = admitted_selector(names, selector);
        if let Some(name) =
            admitted.or_else(|| ROUTE_HOME_SELECTORS.contains(&selector).then_some(selector))
        {
            let value = lookup(name)?;
            let value = if CLAUDE_ROUTE_CREDENTIALS.contains(&selector) {
                value.map(|value| Value::Bool(!value.is_empty()))
            } else {
                value.map(Value::String)
            };
            selectors.insert(selector.to_owned(), value.unwrap_or(Value::Null));
        }
    }
    let digest = core::canonical_json_sha256(&Value::Object(selectors))
        .map_err(|_| core::Error::new("internal", "native route identity could not be encoded"))?;
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub(crate) fn metadata_route_scope_known(
    profile: &Value,
    environment: &serde_json::Map<String, Value>,
) -> bool {
    let harness = profile["harness"].as_str().unwrap_or_default();
    if harness == "claude" && !claude_route_selectors_resolved(profile, environment) {
        return false;
    }
    environment
        .iter()
        .filter_map(|(name, value)| value.as_str().map(|value| (name, value)))
        .filter(|(_, value)| !value.is_empty())
        .all(|(name, value)| {
            if (harness == "claude" && env_name_matches(name, "CLAUDE_CODE_OAUTH_TOKEN"))
                || (harness == "opencode" && env_name_matches(name, "OPENCODE_CONFIG_CONTENT"))
            {
                return false;
            }
            native_store_selector(name).is_none() || Path::new(value.trim()).is_absolute()
        })
}

fn claude_route_selectors_resolved(
    profile: &Value,
    environment: &serde_json::Map<String, Value>,
) -> bool {
    profile["pass_env"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|name| private_route_selector(name))
        .all(|name| {
            environment.iter().any(|(key, value)| {
                env_name_matches(key, name)
                    && value.as_str().is_some_and(|value| !value.trim().is_empty())
            })
        })
}

/// Called only for a newly resolved launch/attach profile, after retained-ID miss.
pub(crate) fn capture_native_store_inputs(
    profile: &mut Value,
    selector_lookup: impl FnMut(&str) -> Option<OsString>,
) -> core::Result<()> {
    capture_native_store_inputs_inner(profile, selector_lookup, |name| std::env::var_os(name))
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
    for &selector in CODEX_STORE_SELECTORS {
        let selected = admitted_selector(names, selector);
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
        .filter(|name| {
            (!private_route_selector(name)
                || (profile["harness"] == "claude" && env_name_matches(name, "CLAUDE_CONFIG_DIR")))
                && !claude_model_selector(profile, name)
        })
        .chain(profile["api_key_env"].as_str())
        .filter_map(&mut credential_lookup)
        .filter_map(|value| value.into_string().ok())
        .collect::<Vec<_>>();
    // Keep resolved credentials only in this temporary policy; snapshot data
    // is checked against them but never receives their values.
    let policy = core::SecretPolicy::new(credentials.iter().map(String::as_str));
    let mut snapshot = profile.clone();
    snapshot["pass_env_values"] = captured;
    if snapshot.get("harness").and_then(Value::as_str).is_some() {
        let identity = native_route_identity(&snapshot, |name| {
            if let Some(selector) = native_store_selector(name)
                && CODEX_STORE_SELECTORS.contains(&selector)
            {
                return Ok(snapshot["pass_env_values"][selector]
                    .as_str()
                    .map(str::to_owned));
            }
            selector_lookup(name)
                .map(|value| {
                    value.into_string().map_err(|_| {
                        core::Error::new("invalid", "native route selector is not valid Unicode")
                    })
                })
                .transpose()
        })?;
        snapshot["native_route_identity"] = Value::String(identity);
    }
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
    capacity: CapacityConfig,
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
    #[serde(default)]
    on_provider_failure: OnProviderFailure,
    /// Additional extensible roles share the same ordered candidate syntax.
    #[serde(flatten)]
    pub(crate) roles: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum OnProviderFailure {
    #[default]
    Stop,
    Next,
}

impl RoutingConfig {
    pub(crate) fn candidates(&self, role: &str) -> Option<&[String]> {
        if role == "default" {
            Some(&self.default)
        } else {
            self.roles.get(role).map(Vec::as_slice)
        }
    }

    pub(crate) fn provider_failover(&self) -> bool {
        self.on_provider_failure == OnProviderFailure::Next
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DesktopTheme {
    Light,
    Dark,
    Amoled,
    Purple,
    Neon,
    Matrix,
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
    /// The window's outer top-left in physical screen pixels, once moved.
    #[serde(default)]
    x: Option<i32>,
    #[serde(default)]
    y: Option<i32>,
    #[serde(default)]
    maximized: bool,
}

/// `[desktop] window` bounds: the client size in DIPs and the outer
/// top-left in physical screen pixels.
const DESKTOP_WINDOW_MIN: (u32, u32) = (640, 480);
const DESKTOP_WINDOW_MAX_SIZE: u32 = 32_768;
const DESKTOP_WINDOW_MAX_COORDINATE: u32 = 32_768;

/// Whether a `[desktop] window` is within bounds: a size from 640 by 480 to
/// 32768 by 32768, coordinates within +-32768. The desktop applies the same
/// check when it reads the file before its window exists.
pub fn desktop_window_in_bounds(width: u32, height: u32, x: Option<i32>, y: Option<i32>) -> bool {
    let size = |value: u32, min: u32| (min..=DESKTOP_WINDOW_MAX_SIZE).contains(&value);
    let coordinate = |value: Option<i32>| {
        value.is_none_or(|value| value.unsigned_abs() <= DESKTOP_WINDOW_MAX_COORDINATE)
    };
    size(width, DESKTOP_WINDOW_MIN.0)
        && size(height, DESKTOP_WINDOW_MIN.1)
        && coordinate(x)
        && coordinate(y)
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

/// One "Manage models" choice: a model (`model`) or a whole provider group
/// of one harness (`provider`) shown or hidden in the desktop's picker.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DesktopModelVisibility {
    #[serde(default)]
    harness: String,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    provider: Option<String>,
    visible: bool,
}

/// One desktop display name: a sidebar project (`project:<key>`) or an
/// Agent (`agent:<id>`) and the name the owner gave it.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DesktopDisplayName {
    key: String,
    name: String,
}

impl FileConfig {
    /// The owner's display name for a project: the first `[desktop]
    /// display_names` `project:*` entry matching `candidate_keys` in order.
    /// Empty names never match.
    pub(crate) fn desktop_project_name(&self, candidate_keys: &[String]) -> Option<String> {
        for key in candidate_keys {
            if let Some(name) = self.desktop.display_names.iter().find_map(|entry| {
                entry
                    .key
                    .eq_ignore_ascii_case(key)
                    .then(|| entry.name.clone())
            }) && !name.trim().is_empty()
            {
                return Some(name);
            }
        }
        None
    }
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
    #[serde(default)]
    model_visibility: Vec<DesktopModelVisibility>,
    #[serde(default)]
    collapsed_model_groups: Vec<String>,
    #[serde(default)]
    display_names: Vec<DesktopDisplayName>,
}

/// Bounds for the `[desktop]` picker lists.
const MAX_DESKTOP_FAVORITES: usize = 50;
const MAX_DESKTOP_RECENTS: usize = 20;
const MAX_DESKTOP_COLLAPSED: usize = 100;
const MAX_DESKTOP_VISIBILITY: usize = 500;
const MAX_DESKTOP_DISPLAY_NAMES: usize = 200;
/// Longest desktop display name, in characters.
const MAX_DESKTOP_DISPLAY_NAME_CHARS: usize = 120;

/// A verbatim desktop key: 1-256 bytes, no controls, no surrounding
/// whitespace.
fn desktop_key_valid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value.chars().any(char::is_control)
        && value == value.trim()
}

fn desktop_error(message: &str) -> core::Error {
    core::Error::new("profile_error", format!("config.toml: {message}"))
}

impl DesktopSettings {
    fn validate(&self) -> core::Result<()> {
        if self.window.as_ref().is_some_and(|window| {
            !desktop_window_in_bounds(window.width, window.height, window.x, window.y)
        }) {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: desktop window must be 640 by 480 to 32768 by 32768, with x and y within -32768 to 32768",
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
            if !core::api::is_supported_harness(harness) {
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
        self.validate_picker_choices()
    }

    /// `model_visibility`, `collapsed_model_groups` and `display_names`.
    fn validate_picker_choices(&self) -> core::Result<()> {
        if self.model_visibility.len() > MAX_DESKTOP_VISIBILITY {
            return Err(desktop_error(
                "desktop model_visibility exceeds 500 entries",
            ));
        }
        for entry in &self.model_visibility {
            if !core::api::is_supported_harness(&entry.harness) {
                return Err(desktop_error(
                    "desktop model_visibility harness must be codex, claude, muse, opencode or antigravity",
                ));
            }
            match (entry.model.as_deref(), entry.provider.as_deref()) {
                (Some(value), None) | (None, Some(value)) if desktop_key_valid(value) => {}
                _ => {
                    return Err(desktop_error(
                        "desktop model_visibility entries name exactly one model or provider of 1-256 bytes with no controls or surrounding whitespace",
                    ));
                }
            }
        }
        if self.collapsed_model_groups.len() > MAX_DESKTOP_COLLAPSED {
            return Err(desktop_error(
                "desktop collapsed_model_groups exceeds 100 entries",
            ));
        }
        if !self
            .collapsed_model_groups
            .iter()
            .all(|group| desktop_key_valid(group))
        {
            return Err(desktop_error(
                "desktop collapsed_model_groups entries must be 1-256 bytes with no controls or surrounding whitespace",
            ));
        }
        if self.display_names.len() > MAX_DESKTOP_DISPLAY_NAMES {
            return Err(desktop_error("desktop display_names exceeds 200 entries"));
        }
        for entry in &self.display_names {
            let name = entry.name.as_str();
            if !desktop_key_valid(&entry.key)
                || !(entry.key.starts_with("project:") || entry.key.starts_with("agent:"))
                || name.is_empty()
                || name.chars().count() > MAX_DESKTOP_DISPLAY_NAME_CHARS
                || name.chars().any(char::is_control)
                || name != name.trim()
            {
                return Err(desktop_error(
                    "desktop display_names need a project: or agent: key and a 1-120 character name with no controls or surrounding whitespace",
                ));
            }
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

/// Store-size threshold for the unbounded-retention advisory: `daemon.status`
/// reports `size_advisory` and maintenance logs a line while all retention
/// windows are unset. Documented in docs/spec/configuration.md (daemon retention).
pub(crate) const RETENTION_UNBOUNDED_SIZE_ADVISORY_BYTES: u64 = 100 * 1024 * 1024;
const DEFAULT_EVENTS_RETENTION_DAYS: i64 = 30;
const DEFAULT_PROCESSES_RETENTION_SECONDS: i64 = 24 * 60 * 60;
const DEFAULT_RETENTION_INTERVAL_SECONDS: i64 = 5 * 60;
const MAX_RETENTION_INTERVAL_SECONDS: i64 = 24 * 60 * 60;

/// Physical retention policy and SQLite space maintenance. Event retention
/// defaults to 30 days; legacy `events_days` remains supported, while
/// `events_seconds` permits short private qualification windows. Exited child
/// rows default to one day. See docs/spec/configuration.md (daemon retention).
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetentionConfig {
    #[serde(default)]
    pub(crate) events_days: Option<i64>,
    #[serde(default)]
    pub(crate) events_seconds: Option<i64>,
    #[serde(default)]
    pub(crate) processes_seconds: Option<i64>,
    #[serde(default)]
    pub(crate) interval_seconds: Option<i64>,
    #[serde(default)]
    pub(crate) compact_on_start: bool,
    #[serde(default)]
    pub(crate) commands_days: Option<i64>,
    #[serde(default)]
    pub(crate) mail_days: Option<i64>,
}

impl RetentionConfig {
    pub(crate) fn events_retention_days(&self) -> Option<i64> {
        self.events_seconds
            .is_none()
            .then_some(self.events_days.unwrap_or(DEFAULT_EVENTS_RETENTION_DAYS))
    }

    pub(crate) fn events_retention_seconds(&self) -> i64 {
        self.events_seconds.unwrap_or_else(|| {
            self.events_days
                .unwrap_or(DEFAULT_EVENTS_RETENTION_DAYS)
                .saturating_mul(24 * 60 * 60)
        })
    }

    pub(crate) fn processes_retention_seconds(&self) -> i64 {
        self.processes_seconds
            .unwrap_or(DEFAULT_PROCESSES_RETENTION_SECONDS)
    }

    pub(crate) fn maintenance_interval_seconds(&self) -> u64 {
        self.interval_seconds
            .unwrap_or(DEFAULT_RETENTION_INTERVAL_SECONDS) as u64
    }

    pub(crate) fn validate(self) -> core::Result<Self> {
        if self.events_days.is_some() && self.events_seconds.is_some() {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: set only one of daemon.retention.events_days and events_seconds",
            ));
        }
        if self.events_days.is_some_and(|days| days <= 0) {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: daemon.retention.events_days must be a positive integer",
            ));
        }
        if self.events_seconds.is_some_and(|seconds| seconds <= 0) {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: daemon.retention.events_seconds must be a positive integer",
            ));
        }
        if self.processes_seconds.is_some_and(|seconds| seconds <= 0) {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: daemon.retention.processes_seconds must be a positive integer",
            ));
        }
        if self
            .interval_seconds
            .is_some_and(|seconds| seconds <= 0 || seconds > MAX_RETENTION_INTERVAL_SECONDS)
        {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: daemon.retention.interval_seconds must be within 1..=86400",
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

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct TimedAttentionConfig {
    #[serde(default = "default_attention_enabled")]
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) threshold_secs: Option<i64>,
}

impl Default for TimedAttentionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold_secs: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NoProgressAttentionConfig {
    #[serde(default = "default_attention_enabled")]
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) threshold_secs: Option<i64>,
    /// Automatic interruption after a no-progress notice is opt-in.
    #[serde(default)]
    pub(crate) interrupt: bool,
}

impl Default for NoProgressAttentionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold_secs: None,
            interrupt: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SwitchAttentionConfig {
    #[serde(default = "default_attention_enabled")]
    pub(crate) enabled: bool,
}

impl Default for SwitchAttentionConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionGrowthAttentionConfig {
    #[serde(default = "default_attention_enabled")]
    pub(crate) enabled: bool,
    /// Automatic switching after session-growth attention is opt-in.
    #[serde(default)]
    pub(crate) switch: bool,
}

impl Default for SessionGrowthAttentionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            switch: false,
        }
    }
}

fn default_attention_enabled() -> bool {
    true
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AttentionConfig {
    #[serde(default)]
    pub(crate) no_progress: NoProgressAttentionConfig,
    #[serde(default)]
    pub(crate) no_code_progress: TimedAttentionConfig,
    #[serde(default)]
    pub(crate) children_unattended: TimedAttentionConfig,
    #[serde(default)]
    pub(crate) source_unhealthy: SwitchAttentionConfig,
    #[serde(default)]
    pub(crate) agent_lost: SwitchAttentionConfig,
    #[serde(default)]
    pub(crate) provider_refusal: SwitchAttentionConfig,
    #[serde(default)]
    pub(crate) session_growth: SessionGrowthAttentionConfig,
    #[serde(default)]
    pub(crate) repeated_command_failure: SwitchAttentionConfig,
}

impl AttentionConfig {
    fn validate_with_legacy_threshold(mut self, legacy_secs: i64) -> core::Result<Self> {
        for (name, config) in [
            ("no_progress", &mut self.no_progress.threshold_secs),
            (
                "no_code_progress",
                &mut self.no_code_progress.threshold_secs,
            ),
            (
                "children_unattended",
                &mut self.children_unattended.threshold_secs,
            ),
        ] {
            let threshold = config.get_or_insert(legacy_secs);
            if *threshold <= 0 || *threshold > i64::MAX / 1_000 {
                return Err(core::Error::new(
                    "profile_error",
                    format!(
                        "config.toml: daemon.attention.{name}.threshold_secs must be a positive integer whose millisecond value fits in i64",
                    ),
                ));
            }
        }
        Ok(self)
    }
}
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DaemonConfig {
    /// Whether to reopen eligible idle retained sessions after restart.
    #[serde(default)]
    pub(crate) on_restart: OnRestart,
    #[serde(default)]
    pub(crate) chats_root: Option<String>,
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
    /// Legacy fallback for per-kind attention thresholds. Retained for older
    /// config files; new settings belong under daemon.attention.<kind>.
    /// The value remains advisory and never infers agent death.
    #[serde(default = "default_attention_suspicion_secs")]
    pub(crate) attention_suspicion_secs: i64,
    #[serde(default)]
    pub(crate) attention: AttentionConfig,
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

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum OnRestart {
    Hold,
    #[default]
    Continue,
}

fn default_log_level() -> String {
    "standard".to_owned()
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            on_restart: OnRestart::default(),
            chats_root: None,
            artifact_roots: Vec::new(),
            retention: RetentionConfig::default(),
            log_level: default_log_level(),
            limits: core::api::HierarchyLimits::default(),
            attention_suspicion_secs: default_attention_suspicion_secs(),
            attention: AttentionConfig::default(),
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
        if self.chats_root.as_ref().is_some_and(|p| {
            p.len() > 4096
                || !std::path::Path::new(p).is_absolute()
                || p.chars().any(char::is_control)
        }) {
            return Err(core::Error::new(
                "profile_error",
                "daemon.chats_root must be an absolute local root",
            ));
        }
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
        let attention = self
            .attention
            .validate_with_legacy_threshold(self.attention_suspicion_secs)?;
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
            chats_root: self.chats_root,
            on_restart: self.on_restart,
            artifact_roots: self.artifact_roots,
            retention: self.retention.validate()?,
            limits: self.limits,
            attention_suspicion_secs: self.attention_suspicion_secs,
            attention,
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
/// Owner-set bounds for the daemon-wide, per-project and per-assignment
/// admission scopes (`global:swarm`, `project:<id>`, `manager_assignment:<id>`).
/// A missing limit (`None`) is unlimited, as for profile, account and proxy
/// caps; `0` disables admission to that scope. Limits ride the frozen launch
/// snapshot, so lowering one blocks new admissions without stopping running
/// work.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct CapacityConfig {
    #[serde(default)]
    pub(crate) global_max_live: Option<u64>,
    #[serde(default)]
    pub(crate) projects: HashMap<String, CapacityProjectEntry>,
}

/// Per-project Owner bounds: the project cap and the manager-assignment cap.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct CapacityProjectEntry {
    #[serde(default)]
    pub(crate) max_live: Option<u64>,
    #[serde(default)]
    pub(crate) manager_max_live: Option<u64>,
    #[serde(default)]
    pub(crate) manager_min_live: Option<u64>,
    #[serde(default)]
    pub(crate) manager_priority_max: Option<u8>,
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
    context_ceiling_tokens: Option<i64>,
    #[serde(default)]
    quota_reserve_percent: Option<i64>,
    #[serde(default)]
    quota_unknown: Option<String>,
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
    project_doc_max_bytes: Option<u64>,
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
    pass_agent_identity_to_shell: Option<bool>,
    #[serde(default)]
    setup: Vec<crate::setup::SetupEntry>,
    #[serde(default = "default_scratch")]
    scratch: Vec<String>,
    #[serde(default)]
    local_provider: Option<core::local_provider::LocalProvider>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum QuotaUnknownPolicy {
    #[default]
    Admit,
    Hold,
}

/// Read the frozen percentage, defaulting older profile snapshots to zero.
pub(crate) fn profile_quota_reserve_percent(snapshot: &Value) -> u8 {
    snapshot
        .get("quota_reserve_percent")
        .and_then(Value::as_u64)
        .and_then(|value| u8::try_from(value).ok())
        .filter(|value| *value <= 100)
        .unwrap_or_default()
}

/// Read the frozen unknown-quota policy, defaulting older snapshots to admit.
pub(crate) fn profile_quota_unknown_policy(snapshot: &Value) -> QuotaUnknownPolicy {
    match snapshot.get("quota_unknown") {
        None => QuotaUnknownPolicy::Admit,
        Some(Value::String(value)) if value == "admit" => QuotaUnknownPolicy::Admit,
        Some(Value::String(value)) if value == "hold" => QuotaUnknownPolicy::Hold,
        Some(_) => QuotaUnknownPolicy::Hold,
    }
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
/// path validates an edited draft before `settings.save` replaces the file,
/// so a saved profile is one the next launch can resolve.
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
    let site = ProfileSite {
        name,
        source,
        offset: entry.span().start,
    };
    let mut snapshot = snapshot_from_entry(name, entry.get_ref(), site.offset, source)?;
    if let Some(ceiling) = entry.get_ref().context_ceiling_tokens {
        snapshot["context_ceiling_tokens"] = json!(ceiling);
    }
    freeze_profile_quota_policy(&mut snapshot, site, entry.get_ref())?;
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

/// Freeze the daemon's routing quota policy with every resolved profile.
fn freeze_profile_quota_policy(
    snapshot: &mut Value,
    site: ProfileSite<'_>,
    entry: &ProfileEntry,
) -> core::Result<()> {
    let reserve_percent = entry.quota_reserve_percent.unwrap_or_default();
    if !(0..=100).contains(&reserve_percent) {
        return Err(site.error(
            "quota_reserve_percent",
            "must be an integer from 0 through 100",
            "profile_error",
        ));
    }
    let reserve_percent = u64::try_from(reserve_percent).map_err(|_| {
        site.error(
            "quota_reserve_percent",
            "must be an integer from 0 through 100",
            "profile_error",
        )
    })?;
    let quota_unknown = validated_choice(
        site,
        "quota_unknown",
        entry.quota_unknown.as_deref(),
        &["admit", "hold"],
        "must be admit or hold",
        "profile_error",
    )?
    .unwrap_or("admit");
    let snapshot = snapshot.as_object_mut().ok_or_else(|| {
        core::Error::new("internal", "resolved profile snapshot must be an object")
    })?;
    snapshot.insert(
        "quota_reserve_percent".to_owned(),
        Value::from(reserve_percent),
    );
    snapshot.insert(
        "quota_unknown".to_owned(),
        Value::String(quota_unknown.to_owned()),
    );
    Ok(())
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
/// Harnesses with a verified per-session native subagent denial mapping.
pub(crate) const NATIVE_SUBAGENT_DENY_HARNESSES: [&str; 3] = ["codex", "claude", "opencode"];

/// Default native subagent policy for each supported harness. Muse and
/// Antigravity have no verified switch and therefore stay enabled.
pub(crate) fn native_subagents_default(harness: &str) -> Option<&'static str> {
    if NATIVE_SUBAGENT_DENY_HARNESSES.contains(&harness) {
        Some("deny")
    } else {
        match harness {
            "muse" | "antigravity" => Some("allow"),
            _ => None,
        }
    }
}

/// Freeze the harness-native subagent switch for this profile: `allow` is an
/// explicit opt-in, while `deny` asks the harness plugin to disable its
/// native subagent surface for the session. When omitted, use the harness
/// default: `deny` on Codex, Claude and OpenCode, and fixed `allow` on Muse
/// and Antigravity. The frozen value is reported in the snapshot so effective
/// settings stay truthful. `deny` has a per-session mapping on Codex, Claude
/// and OpenCode only; on Muse and Antigravity the installed binaries expose
/// no verified switch, so explicit `deny` is `unsupported` at config load.
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
        None => native_subagents_default(&harness)
            .ok_or_else(|| {
                profile_field_error(
                    source,
                    table_offset,
                    profile_name,
                    "native_subagents",
                    "has no default mapping for this harness",
                    "unsupported",
                )
            })?
            .to_owned(),
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
    let config = parse_config_document(source)?;
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
    if config.capacity.projects.len() > MAX_PROFILES {
        return Err(core::Error::new(
            "profile_error",
            "config.toml: capacity projects exceed 256 entries",
        ));
    }
    for (name, budget) in &config.capacity.projects {
        if budget.manager_min_live.is_some_and(|minimum| {
            budget
                .manager_max_live
                .is_none_or(|maximum| minimum > maximum)
                || minimum > i64::MAX as u64
        }) {
            return Err(core::Error::new(
                "invalid",
                "manager_min_live requires a manager_max_live at least as large",
            ));
        }
        if name.is_empty() || name.len() > 1024 || name.contains(char::is_control) {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: capacity project name is invalid",
            ));
        }
    }
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

/// Deserialize the strict configuration shape without applying the semantic
/// checks below. Settings uses this distinction to keep safely parsed fields
/// editable when a section-level value is out of bounds.
fn parse_config_document(source: &str) -> core::Result<FileConfig> {
    toml::from_str(source).map_err(|error| map_toml_error(source, error))
}

pub(crate) fn validate_routing_role(role: &str) -> core::Result<()> {
    if role == "on_provider_failure"
        || role.is_empty()
        || role.len() > MAX_PROFILE_NAME_LEN
        || !role
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(core::Error::new(
            "profile_error",
            "config.toml: routing role names must be nonempty letters, digits, hyphen or underscore and cannot be on_provider_failure",
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

pub(crate) fn load_routing_config(data_root: &Path) -> core::Result<RoutingConfig> {
    Ok(read_config(data_root)?
        .map(|(config, _)| config.routing)
        .unwrap_or_default())
}

pub(crate) fn missing_claude_profile_commands(data_root: &Path) -> Vec<(String, String)> {
    let Ok(Some(source)) = read_config_source(data_root) else {
        return Vec::new();
    };
    let Ok(document) = toml::from_str::<toml::Value>(&source) else {
        return Vec::new();
    };
    let Some(profiles) = document.get("profiles").and_then(toml::Value::as_table) else {
        return Vec::new();
    };

    let mut missing = profiles
        .iter()
        .filter_map(|(name, entry)| {
            if name.trim().is_empty() || name.len() > 128 || name.chars().any(char::is_control) {
                return None;
            }
            let profile = entry.as_table()?;
            if profile
                .get("harness")
                .and_then(toml::Value::as_str)
                .is_none_or(|harness| harness.trim() != "claude")
            {
                return None;
            }
            let cmd = profile.get("cmd").and_then(toml::Value::as_str)?;
            if cmd.trim().is_empty()
                || cmd.len() > 4096
                || cmd.chars().any(char::is_control)
                || cmd.trim().eq_ignore_ascii_case("latest")
                || Path::new(cmd).is_file()
            {
                return None;
            }
            Some((name.clone(), cmd.to_owned()))
        })
        .collect::<Vec<_>>();
    missing.sort_by(|left, right| left.0.cmp(&right.0));
    missing
}

pub(crate) fn load_daemon_config(data_root: &Path) -> core::Result<DaemonConfig> {
    let Some((config, _)) = read_config(data_root)? else {
        return Ok(DaemonConfig::default());
    };
    config.daemon.validate()
}

/// Current Owner capacity policy for admission and overview readback.
pub(crate) fn load_scope_limits(data_root: &Path) -> core::Result<core::api::ScopeLimits> {
    let Some((config, _)) = read_config(data_root)? else {
        return Ok(core::api::ScopeLimits::default());
    };
    Ok(core::api::ScopeLimits {
        global: config.capacity.global_max_live,
        projects: config
            .capacity
            .projects
            .into_iter()
            .map(|(project, limits)| {
                (
                    project,
                    core::api::ProjectScopeLimits {
                        project: limits.max_live,
                        manager_assignment: limits.manager_max_live,
                        manager_min_live: limits.manager_min_live,
                        manager_priority_max: limits.manager_priority_max,
                    },
                )
            })
            .collect(),
    })
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
    let site = ProfileSite {
        name,
        source,
        offset: table_offset,
    };
    let harness = entry.harness.trim();
    if !core::api::is_supported_harness(harness) {
        return Err(site.error("harness", "unsupported harness", "unsupported"));
    }
    if entry
        .context_ceiling_tokens
        .is_some_and(|ceiling| ceiling <= 0)
    {
        return Err(site.error(
            "context_ceiling_tokens",
            "must be a positive token count",
            "profile_error",
        ));
    }
    reject_unmapped(
        site,
        &[
            (
                "local_provider",
                harness != "opencode" && entry.local_provider.is_some(),
                "local providers have no mapping on this harness",
            ),
            (
                "project_doc_max_bytes",
                harness != "codex" && entry.project_doc_max_bytes.is_some(),
                "project_doc_max_bytes has no mapping on this harness",
            ),
        ],
    )?;
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
    let effort = validated_effort(site, entry.effort.as_deref())?;
    let collaboration_mode = validated_choice(
        site,
        "collaboration_mode",
        entry.collaboration_mode.as_deref(),
        &CodexCollaborationMode::ALL.map(CodexCollaborationMode::as_str),
        "must be default or plan",
        "profile_error",
    )?;
    let permissions = validated_choice(
        site,
        "permissions",
        entry.permissions.as_deref(),
        &CodexPermissions::ALL.map(CodexPermissions::as_str),
        "unsupported permission mode",
        "unsupported",
    )?
    .unwrap_or(CodexPermissions::default().as_str());
    let approval_policy = validated_choice(
        site,
        "approval_policy",
        entry.approval_policy.as_deref(),
        &CodexApprovalPolicy::ALL.map(CodexApprovalPolicy::as_str),
        "must be never or on-request",
        "profile_error",
    )?
    .unwrap_or(CodexApprovalPolicy::default().as_str());
    reject_unmapped(
        site,
        &[
            (
                "approval",
                entry.approval.is_some(),
                "the Codex slice has no approval mapping; use approval_policy",
            ),
            (
                "mode",
                entry.mode.is_some() || entry.dangerously_skip_permissions.is_some(),
                "mode and dangerously_skip_permissions have no Codex mapping",
            ),
            (
                "auth_integration",
                entry.auth_integration.is_some(),
                "auth_integration has no Codex mapping",
            ),
        ],
    )?;
    let tail = validate_profile_tail(name, entry, table_offset, source)?;
    let mut snapshot = serde_json::Map::new();
    snapshot.insert("harness".to_owned(), Value::String("codex".to_owned()));
    if let Some(limit) = entry.project_doc_max_bytes {
        snapshot.insert("project_doc_max_bytes".to_owned(), Value::from(limit));
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
    insert_profile_tail(&mut snapshot, tail, &entry.setup)?;
    Ok(Value::Object(snapshot))
}

/// Shared profile fields validated after the harness-specific fields.
struct ProfileTail {
    pass_env: Vec<String>,
    cmd: Option<String>,
    scratch: Vec<String>,
    pass_agent_identity_to_shell: bool,
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
    let harness = entry.harness.trim();
    let cmd = match entry.cmd.as_deref() {
        Some(cmd) if harness == "claude" && cmd.trim().eq_ignore_ascii_case("latest") => {
            Some("latest".to_owned())
        }
        Some(cmd) => {
            let cmd = validate_executable_name(cmd)
                .map_err(|class| {
                    profile_field_error(source, table_offset, name, "cmd", class, "profile_error")
                })?
                .to_owned();
            let cmd = if harness == "codex" {
                cmd
            } else {
                reject_script_wrapper(name, cmd, source, table_offset)?
            };
            Some(if harness == "claude" {
                require_native_claude_cmd(name, cmd, source, table_offset)?
            } else if harness == "antigravity" {
                require_native_antigravity_cmd(name, cmd, source, table_offset)?
            } else {
                cmd
            })
        }
        None if matches!(harness, "codex" | "claude") => None,
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
        pass_agent_identity_to_shell: entry.pass_agent_identity_to_shell.unwrap_or(true),
    })
}

/// Fold the validated cmd (when supplied), pass_env, scratch and setup (when
/// non-empty) into the harness snapshot.
fn insert_profile_tail(
    snapshot: &mut serde_json::Map<String, Value>,
    tail: ProfileTail,
    setup: &[crate::setup::SetupEntry],
) -> core::Result<()> {
    if let Some(cmd) = tail.cmd {
        snapshot.insert("cmd".to_owned(), Value::String(cmd));
    }
    snapshot.insert(
        "pass_env".to_owned(),
        Value::Array(tail.pass_env.into_iter().map(Value::String).collect()),
    );
    snapshot.insert("scratch".to_owned(), json!(tail.scratch));
    if !tail.pass_agent_identity_to_shell {
        snapshot.insert(
            "pass_agent_identity_to_shell".to_owned(),
            Value::Bool(false),
        );
    }
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
    let site = ProfileSite {
        name,
        source,
        offset: table_offset,
    };
    let model = entry.model.as_deref();
    if let Some(model) = model {
        validate_model(name, model, source, table_offset)?;
    }
    let effort = validated_effort(site, entry.effort.as_deref())?;
    let approval = validated_choice(
        site,
        "approval",
        entry.approval.as_deref(),
        &MUSE_APPROVALS,
        "must be allowAll, promptUnmatched, onRequest or denyUnmatched",
        "profile_error",
    )?;
    reject_unmapped(
        site,
        &[(
            "collaboration_mode",
            entry.collaboration_mode.is_some(),
            "the Muse slice has no collaboration preset mapping",
        )],
    )?;
    let permissions = validated_choice(
        site,
        "permissions",
        entry.permissions.as_deref(),
        &MUSE_PERMISSIONS.map(CodexPermissions::as_str),
        "the Muse slice supports only workspace-write or danger-full-access",
        "unsupported",
    )?
    .unwrap_or(CodexPermissions::default().as_str());
    validated_choice(
        site,
        "approval_policy",
        entry.approval_policy.as_deref(),
        &[CodexApprovalPolicy::Never.as_str()],
        "the Muse slice leaves approvals unconnected",
        "unsupported",
    )?;
    reject_unmapped(
        site,
        &[
            (
                "mode",
                entry.mode.is_some() || entry.dangerously_skip_permissions.is_some(),
                "the Muse slice has no mapping for this preset",
            ),
            (
                "auth_integration",
                entry.auth_integration.is_some(),
                "the Muse slice has no mapping for this preset",
            ),
        ],
    )?;
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
        Value::String(CodexApprovalPolicy::Never.as_str().to_owned()),
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
    let site = ProfileSite {
        name,
        source,
        offset: table_offset,
    };
    let model = entry.model.as_deref();
    if let Some(model) = model {
        validate_model(name, model, source, table_offset)?;
    }
    // Claude Code 2.1.278 `--effort <level>`: the CLI's closed vocabulary.
    // Absent effort keeps the CLI default; no value is fabricated.
    let effort = validated_choice(
        site,
        "effort",
        entry
            .effort
            .as_deref()
            .filter(|value| !value.trim().is_empty()),
        &CLAUDE_EFFORTS,
        "unknown Claude effort level (low, medium, high, xhigh, or max)",
        "profile_error",
    )?;
    let permission_mode = validated_choice(
        site,
        "approval",
        entry
            .approval
            .as_deref()
            .filter(|value| !value.trim().is_empty()),
        &CLAUDE_PERMISSION_MODES,
        "unknown Claude permission mode (acceptEdits, auto, bypassPermissions, manual, dontAsk, or plan)",
        "profile_error",
    )?;
    reject_unmapped(
        site,
        &[(
            "collaboration_mode",
            entry.collaboration_mode.is_some(),
            "the Claude slice has no collaboration preset mapping",
        )],
    )?;
    validated_choice(
        site,
        "permissions",
        entry.permissions.as_deref(),
        &[CodexPermissions::WorkspaceWrite.as_str()],
        "the Claude slice runs the CLI-default permission posture",
        "unsupported",
    )?;
    validated_choice(
        site,
        "approval_policy",
        entry.approval_policy.as_deref(),
        &[CodexApprovalPolicy::Never.as_str()],
        "the Claude slice leaves approvals unconnected",
        "unsupported",
    )?;
    reject_unmapped(
        site,
        &[
            (
                "mode",
                entry.mode.is_some() || entry.dangerously_skip_permissions.is_some(),
                "the Claude slice has no mapping for this preset",
            ),
            (
                "auth_integration",
                entry.auth_integration.is_some(),
                "the Claude slice has no mapping for this preset",
            ),
        ],
    )?;
    let tail = validate_profile_tail(name, entry, table_offset, source)?;
    let mut snapshot = serde_json::Map::new();
    snapshot.insert("harness".to_owned(), Value::String("claude".to_owned()));
    if let Some(model) = model {
        snapshot.insert("model".to_owned(), Value::String(model.to_owned()));
    }
    snapshot.insert(
        "permissions".to_owned(),
        Value::String(CodexPermissions::WorkspaceWrite.as_str().to_owned()),
    );
    snapshot.insert(
        "approval_policy".to_owned(),
        Value::String(CodexApprovalPolicy::Never.as_str().to_owned()),
    );
    if let Some(mode) = permission_mode {
        snapshot.insert("permission_mode".to_owned(), Value::String(mode.to_owned()));
    }
    insert_control(
        &mut snapshot,
        name,
        entry.control.as_deref(),
        source,
        table_offset,
    )?;
    if let Some(effort) = effort {
        snapshot.insert("effort".to_owned(), Value::String(effort.to_owned()));
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
    insert_profile_tail(&mut snapshot, tail, &entry.setup)?;
    Ok(Value::Object(snapshot))
}

/// Names of configured OpenCode profiles backed by the daemon config file.
/// Used by the `models.catalog` aggregate to merge profile-scoped provider
/// previews independently of the ad-hoc read.
pub(crate) fn profile_names_for_harness(data_root: &Path, harness: &str) -> Vec<String> {
    let source = match read_config_source(data_root) {
        Ok(Some(source)) => source,
        _ => return Vec::new(),
    };
    let parsed: Option<FileConfig> = toml::from_str(&source).ok();
    let Some(config) = parsed else {
        return Vec::new();
    };
    let mut names: Vec<String> = config
        .profiles
        .iter()
        .filter(|(_, entry)| entry.get_ref().harness.trim() == harness)
        .map(|(name, _)| name.clone())
        .collect();
    names.sort();
    names
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
/// A typed local provider selects one of its own models and uses optional
/// environment-backed authentication in place of a canonical integration.
fn opencode_snapshot_from_entry(
    name: &str,
    entry: &ProfileEntry,
    table_offset: usize,
    source: &str,
) -> core::Result<Value> {
    let site = ProfileSite {
        name,
        source,
        offset: table_offset,
    };
    let model = entry.model.as_deref();
    if let Some(model) = model {
        validate_model(name, model, source, table_offset)?;
    }
    if model.is_some_and(|model| split_provider_model(model).is_none()) {
        return Err(site.error(
            "model",
            "must be provider/model with non-empty slash segments and no variant suffix",
            "profile_error",
        ));
    }
    // A frozen effort selects the installed `Model.Variant` id on launch
    // (`Model.Ref.variant`); the live native catalogue proves membership.
    let effort = validated_effort(site, entry.effort.as_deref())?;
    let no_mapping = "the OpenCode slice has no mapping for this preset";
    reject_unmapped(
        site,
        &[
            ("approval", entry.approval.is_some(), no_mapping),
            (
                "collaboration_mode",
                entry.collaboration_mode.is_some(),
                no_mapping,
            ),
            ("permissions", entry.permissions.is_some(), no_mapping),
            (
                "approval_policy",
                entry.approval_policy.is_some(),
                no_mapping,
            ),
            (
                "mode",
                entry.mode.is_some() || entry.dangerously_skip_permissions.is_some(),
                no_mapping,
            ),
        ],
    )?;
    if let Some(selector) = entry.auth_integration.as_deref() {
        let trimmed = selector.trim();
        if trimmed.is_empty()
            || trimmed.len() > MAX_AUTH_INTEGRATION_LEN
            || trimmed.starts_with('-')
            || trimmed.chars().any(|value| value.is_control())
        {
            return Err(site.error(
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
    if let Some(effort) = effort {
        snapshot.insert("effort".to_owned(), Value::String(effort.to_owned()));
    }
    if let Some(provider) = entry.local_provider.as_ref() {
        let field_error = |field, message: &str| site.error(field, message, "profile_error");
        provider
            .validate(&tail.pass_env)
            .map_err(|message| field_error("local_provider", &message))?;
        if !model
            .and_then(split_provider_model)
            .is_some_and(|(id, model)| provider.contains_model(id, model))
        {
            return Err(field_error(
                "model",
                "a local provider profile must select one of its declared models",
            ));
        }
        if entry.auth_integration.is_some() {
            return Err(field_error(
                "auth_integration",
                "a local provider profile cannot name a canonical auth_integration",
            ));
        }
        let frozen = serde_json::to_value(provider)
            .map_err(|_| field_error("local_provider", "could not encode local provider"))?;
        snapshot.insert("local_provider".to_owned(), frozen);
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
    let site = ProfileSite {
        name,
        source,
        offset: table_offset,
    };
    let model = entry.model.as_deref();
    if let Some(model) = model {
        validate_model(name, model, source, table_offset)?;
    }
    reject_unmapped(
        site,
        &[(
            "effort",
            entry
                .effort
                .as_deref()
                .is_some_and(|effort| !effort.trim().is_empty()),
            "the Antigravity slice has no reasoning-effort mapping",
        )],
    )?;
    let mode = validated_choice(
        site,
        "mode",
        entry.mode.as_deref(),
        &ANTIGRAVITY_MODES,
        "must be plan, auto, or accept-edits",
        "profile_error",
    )?;
    let no_mapping = "the Antigravity slice has no mapping for this preset";
    reject_unmapped(
        site,
        &[
            ("approval", entry.approval.is_some(), no_mapping),
            (
                "collaboration_mode",
                entry.collaboration_mode.is_some(),
                no_mapping,
            ),
            ("permissions", entry.permissions.is_some(), no_mapping),
            (
                "approval_policy",
                entry.approval_policy.is_some(),
                no_mapping,
            ),
            (
                "auth_integration",
                entry.auth_integration.is_some(),
                no_mapping,
            ),
        ],
    )?;
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
                if private_route_selector(&name) {
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

/// Whether a profile names an environment variable owned by Eliot.
pub(crate) fn reserved_eliot_environment_name(name: &str) -> bool {
    name.get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("ELIOT_"))
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
        if reserved_eliot_environment_name(name) {
            return Err(profile_field_error(
                source,
                offset,
                profile,
                "pass_env",
                "ELIOT_* names are reserved for Eliot-managed identity and scope",
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
/// isolation mechanism for `minimal` and selective
/// inclusion of named native servers (Codex and Antigravity: Claude and OpenCode
/// minimal are the Eliot-only server surface with no allow mapping, so a
/// non-empty allow list would be silently dropped); Muse
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
    // it; Muse has no minimal mapping, so its profiles
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

/// Antigravity is the protocol-capable `agy` CLI only: a cmd pointing at the
/// `Antigravity` IDE executable (or any other file name) is refused at save
/// time with a typed error, so catalog discovery can never launch the IDE as
/// a version probe. PATH `agy` binaries additionally prove protocol
/// capability through their `--version` output before any discovery use.
fn require_native_antigravity_cmd(
    profile: &str,
    cmd: String,
    source: &str,
    offset: usize,
) -> core::Result<String> {
    if !crate::adhoc::antigravity_cli_identity(&cmd) {
        return Err(profile_field_error(
            source,
            offset,
            profile,
            "cmd",
            "must name the native `agy` CLI, not the Antigravity IDE executable (which does not implement the ACP/model CLI and is never launched); install the `agy` CLI and put it on PATH",
            "profile_error",
        ));
    }
    Ok(cmd)
}

pub(crate) fn validate_executable_name(cmd: &str) -> Result<&str, &'static str> {
    let cmd = cmd.trim();
    if cmd.is_empty() || cmd.len() > MAX_CMD_LEN {
        return Err("must be 1-1024 characters");
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
        return Err("must name one executable path, not a shell command");
    }
    Ok(cmd)
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
/// The profile table a field error is reported against.
#[derive(Clone, Copy)]
struct ProfileSite<'a> {
    name: &'a str,
    source: &'a str,
    offset: usize,
}

impl ProfileSite<'_> {
    fn error(self, field: &str, message: &str, code: &str) -> core::Error {
        profile_field_error(self.source, self.offset, self.name, field, message, code)
    }
}

/// Fails with `unsupported` for the first check whose field is present.
fn reject_unmapped(site: ProfileSite<'_>, checks: &[(&str, bool, &str)]) -> core::Result<()> {
    match checks.iter().find(|(_, present, _)| *present) {
        Some((field, _, message)) => Err(site.error(field, message, "unsupported")),
        None => Ok(()),
    }
}

/// Trims an optional choice; fails with `code` unless it is one of `allowed`.
fn validated_choice<'a>(
    site: ProfileSite<'_>,
    field: &str,
    value: Option<&'a str>,
    allowed: &[&str],
    message: &str,
    code: &str,
) -> core::Result<Option<&'a str>> {
    let value = value.map(str::trim);
    if value.is_some_and(|value| !allowed.contains(&value)) {
        return Err(site.error(field, message, code));
    }
    Ok(value)
}

/// Trims an optional free-text effort: non-empty, at most `MAX_EFFORT_LEN`
/// bytes and free of control characters.
fn validated_effort<'a>(
    site: ProfileSite<'_>,
    value: Option<&'a str>,
) -> core::Result<Option<&'a str>> {
    let effort = value.map(str::trim);
    if effort.is_some_and(|value| {
        value.is_empty() || value.len() > MAX_EFFORT_LEN || value.chars().any(char::is_control)
    }) {
        return Err(site.error(
            "effort",
            "must be non-empty, at most 128 characters and contain no controls",
            "profile_error",
        ));
    }
    Ok(effort)
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
