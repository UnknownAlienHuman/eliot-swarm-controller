//! Daemon configuration loading, validation and frozen profile resolution.
//!
//! Owning contract: docs/spec/configuration.md. The bounded global source
//! covers profiles and routes, project registrations, automation, daemon,
//! mail, cloud MCP and desktop settings. Unsupported layers remain explicit
//! errors. Project and automation helpers share this module's parser and
//! validation path; profile freezing and resolution stay here.
//!
//! Snapshots retain admitted environment names and declared native-store execution inputs. Private
//! route selectors are bound by digest and resolved only in the process environment.
use crate::harness_capabilities::ProfileCapabilities;
use core::api::harness::NativeRoute;
use core::api::native_profile::{NativeProfileInput, ProfileEffort};
use eliot_core as core;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value, json};
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
    AutomationConfig, GitBaseNoticeConfig, RunGrantConfig, load_automation_config,
    load_automation_config_snapshot,
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
const MAX_CMD_LEN: usize = 1024;
const MAX_ENV_NAME_LEN: usize = 256;
const MAX_PASS_ENV_ENTRIES: usize = 64;
const MAX_CHILD_PROFILES: usize = 64;

pub(crate) fn profile_environments() -> impl Iterator<Item = core::api::harness::ProfileEnvironment>
{
    core::api::HarnessCapabilities::declarations()
        .iter()
        .map(|(_, caps)| caps.profile.environment)
}

pub(crate) fn frozen_store_environments()
-> impl Iterator<Item = core::api::harness::FrozenStoreEnvironment> {
    core::api::HarnessCapabilities::declarations()
        .iter()
        .filter_map(|(_, caps)| caps.frozen_store_environment)
}

pub(crate) fn frozen_store_selectors() -> impl Iterator<Item = &'static str> {
    frozen_store_environments().flat_map(|store| store.selectors.iter().copied())
}

fn native_store_selectors() -> impl Iterator<Item = &'static str> {
    frozen_store_selectors()
        .chain(profile_environments().flat_map(|profile| profile.store_paths.iter().copied()))
        .chain(["APPDATA"])
}

fn provider_selectors() -> impl Iterator<Item = &'static str> {
    profile_environments().flat_map(|profile| {
        profile
            .route_values
            .iter()
            .chain(profile.binary_modes)
            .copied()
    })
}

fn credential_presence_selectors() -> impl Iterator<Item = &'static str> {
    profile_environments().flat_map(|profile| profile.credential_presence.iter().copied())
}

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
    native_store_selectors().find(|selector| env_name_matches(name, selector))
}

pub(crate) fn private_route_selector(name: &str) -> bool {
    native_store_selector(name).is_some()
        || provider_selectors().any(|selector| env_name_matches(name, selector))
}

/// An explicitly bound API key always contributes a credential.
pub(crate) fn native_model_selector(profile: &Value, name: &str) -> bool {
    profile_capabilities(profile)
        .environment
        .model_pins
        .iter()
        .any(|selector| env_name_matches(name, selector))
        && !api_key_name_matches(profile, name)
}

pub(crate) fn native_provider_mode_flag(profile: &Value, name: &str, value: &Value) -> bool {
    matches!(value.as_str(), Some("0" | "1" | "false" | "true"))
        && profile_capabilities(profile)
            .environment
            .binary_modes
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

/// Credential values never enter route evidence. Only presence binds the mode.
pub(crate) fn native_route_selectors(
    profile: &Value,
    mut lookup: impl FnMut(&str) -> core::Result<Option<String>>,
) -> core::Result<JsonMap<String, Value>> {
    let names = profile
        .get("pass_env")
        .and_then(Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    let mut selectors = serde_json::Map::new();
    for selector in ROUTE_HOME_SELECTORS
        .iter()
        .copied()
        .chain(native_store_selectors())
        .chain(provider_selectors())
        .chain(credential_presence_selectors())
    {
        let admitted = admitted_selector(names, selector);
        if let Some(name) =
            admitted.or_else(|| ROUTE_HOME_SELECTORS.contains(&selector).then_some(selector))
        {
            let value = lookup(name)?;
            let value = if credential_presence_selectors().any(|name| name == selector) {
                value.map(|value| Value::Bool(!value.is_empty()))
            } else {
                value.map(Value::String)
            };
            selectors.insert(selector.to_owned(), value.unwrap_or(Value::Null));
        }
    }
    Ok(selectors)
}

pub(crate) fn native_route_identity(selectors: JsonMap<String, Value>) -> core::Result<String> {
    route_digest(&Value::Object(selectors))
}

fn route_digest(value: &Value) -> core::Result<String> {
    let digest = core::canonical_json_sha256(value)
        .map_err(|_| core::Error::new("internal", "native route identity could not be encoded"))?;
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub(crate) fn native_route_fingerprints(
    selectors: &JsonMap<String, Value>,
) -> core::Result<JsonMap<String, Value>> {
    selectors
        .iter()
        .map(|(name, value)| {
            let safe = match value {
                Value::Null => "unset".to_owned(),
                Value::Bool(false) => "empty".to_owned(),
                Value::Bool(true) => "present".to_owned(),
                Value::String(text) if text.is_empty() => "empty".to_owned(),
                Value::String(text) if name.ends_with("_BASE_URL") => {
                    let Ok(mut url) = reqwest::Url::parse(text) else {
                        return Ok((name.clone(), Value::String("invalid_url".to_owned())));
                    };
                    if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
                        return Ok((name.clone(), Value::String("invalid_url".to_owned())));
                    }
                    url.set_password(None).map_err(|()| {
                        core::Error::new("internal", "remove private route URL password")
                    })?;
                    url.set_username("").map_err(|()| {
                        core::Error::new("internal", "remove private route URL username")
                    })?;
                    url.set_query(None);
                    url.set_fragment(None);
                    format!("url-sha256:{}", route_digest(&Value::String(url.into()))?)
                }
                _ => format!("sha256:{}", route_digest(value)?),
            };
            Ok((name.clone(), Value::String(safe)))
        })
        .collect()
}

pub(crate) fn safe_route_fingerprint(name: &str, value: &str) -> bool {
    if matches!(value, "unset" | "empty" | "present") {
        return true;
    }
    let prefix = if name.ends_with("_BASE_URL") {
        if value == "invalid_url" {
            return true;
        }
        "url-sha256:"
    } else {
        "sha256:"
    };
    value
        .strip_prefix(prefix)
        .is_some_and(|hash| hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

/// Only absolute paths, empty inputs and captured absence can be restored.
/// Explicit private provider selectors remain verified live, never serialized.
pub(crate) fn is_native_route_home_value(value: &Value) -> bool {
    value.is_null()
        || value.as_str().is_some_and(|text| {
            text.is_empty()
                || (Path::new(text).is_absolute() && !text.chars().any(char::is_control))
        })
}

pub(crate) fn metadata_route_scope_known(
    profile: &Value,
    environment: &serde_json::Map<String, Value>,
) -> bool {
    let capabilities = profile_capabilities(profile);
    if capabilities.route_selectors_resolved
        && !required_route_selectors_resolved(profile, environment)
    {
        return false;
    }
    environment
        .iter()
        .filter_map(|(name, value)| value.as_str().map(|value| (name, value)))
        .filter(|(_, value)| !value.is_empty())
        .all(|(name, value)| {
            if capabilities
                .route_scope_secret_names
                .iter()
                .any(|secret| env_name_matches(name, secret))
            {
                return false;
            }
            native_store_selector(name).is_none() || Path::new(value.trim()).is_absolute()
        })
}

fn required_route_selectors_resolved(
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

pub(crate) fn stand_store_root(data_root: &Path) -> core::Result<Option<PathBuf>> {
    match std::fs::symlink_metadata(data_root.join(crate::stand_cleanup::STAND_MARKER)) {
        Ok(_) => Ok(Some(data_root.to_owned())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(core::Error::new(
            "platform_io",
            format!("inspect stand marker: {error}"),
        )),
    }
}

fn require_stand_proxy(
    profile: &Value,
    native: core::api::harness::FrozenStoreEnvironment,
) -> core::Result<()> {
    if !profile["route_binding"].is_object() {
        return Err(core::Error::new(
            native.direct_login_refusal.0,
            native.direct_login_refusal.1,
        ));
    }
    Ok(())
}

/// Compare the actual environment and any retained native store before the plugin
/// starts. A retained binding can repin the home after environment setup.
pub(crate) fn verify_stand_native_route(
    stand_home: Option<&Path>,
    harness: &str,
    profile: &Value,
    environment: Option<&JsonMap<String, Value>>,
) -> core::Result<()> {
    verify_stand_native_store(stand_home, harness, profile, environment, true)
}

/// Metadata observation never starts a turn. It still must use the verified
/// private home and retained binding, even when no inference route is present.
pub(crate) fn verify_stand_observation_store(
    stand_home: Option<&Path>,
    harness: &str,
    profile: &Value,
    environment: Option<&JsonMap<String, Value>>,
) -> core::Result<()> {
    verify_stand_native_store(stand_home, harness, profile, environment, false)
}

fn verify_stand_native_store(
    stand_home: Option<&Path>,
    harness: &str,
    profile: &Value,
    environment: Option<&JsonMap<String, Value>>,
    require_proxy: bool,
) -> core::Result<()> {
    let Some((root, native)) =
        stand_home.zip(core::api::HarnessCapabilities::of(harness).frozen_store_environment)
    else {
        return Ok(());
    };
    if require_proxy {
        require_stand_proxy(profile, native)?;
    }
    let home =
        canonical_stand_native_home(&root.join(native.private_directory), native.display_name)?;
    match std::fs::symlink_metadata(home.join(native.guard_file)) {
        Ok(metadata) if metadata.is_file() && !crate::platform::is_reparse_point(&metadata) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) => {
            return Err(core::Error::new(
                "not_allowed",
                format!(
                    "stand {} {} must be a regular file without links",
                    native.display_name, native.guard_file
                ),
            ));
        }
        Err(error) => {
            return Err(core::Error::new(
                "platform_io",
                format!("inspect stand {} config: {error}", native.display_name),
            ));
        }
    }
    let matches_home = |value: &Value| {
        value.as_str().is_some_and(|path| {
            let path = Path::new(path);
            path.is_absolute()
                && path
                    .canonicalize()
                    .is_ok_and(|path| crate::operator::data_dirs_match(&path, &home))
        })
    };
    if profile["stand_native_store"] == true
        && native.selectors.iter().all(|&name| {
            environment
                .and_then(|env| env.get(name))
                .is_some_and(matches_home)
        })
        && profile
            .get("native_store_binding")
            .is_none_or(|binding| matches_home(&binding["home"]) && matches_home(&binding["root"]))
    {
        return Ok(());
    }
    Err(core::Error::new("profile_error", native.binding_refusal))
}

/// Capture selectors for launch or a route-bound metadata operation.
pub(crate) fn capture_native_store_inputs(
    profile: &mut Value,
    stand_home: Option<&Path>,
) -> core::Result<()> {
    capture_store_inputs(profile, stand_home, true)
}

/// Capture selectors for an owner discovery or observe-only attach.
pub(crate) fn capture_observation_store_inputs(
    profile: &mut Value,
    stand_home: Option<&Path>,
) -> core::Result<()> {
    capture_store_inputs(profile, stand_home, false)
}

fn capture_store_inputs(
    profile: &mut Value,
    stand_home: Option<&Path>,
    require_proxy: bool,
) -> core::Result<()> {
    let capabilities =
        core::api::HarnessCapabilities::of(profile["harness"].as_str().unwrap_or_default());
    let home = if let Some((root, native)) = stand_home.zip(capabilities.frozen_store_environment) {
        if require_proxy {
            require_stand_proxy(profile, native)?;
        }
        let home = root.join(native.private_directory);
        if let Err(error) = std::fs::create_dir(&home)
            && error.kind() != std::io::ErrorKind::AlreadyExists
        {
            return Err(core::Error::new(
                "platform_io",
                format!("create stand {} home: {error}", native.display_name),
            ));
        }
        let names = profile["pass_env"].as_array_mut().ok_or_else(|| {
            core::Error::new("invalid", "resolved profile pass_env must be an array")
        })?;
        for &selector in native.selectors {
            if admitted_selector(names, selector).is_none() {
                names.push(Value::from(selector));
            }
        }
        Some(canonical_stand_native_home(&home, native.display_name)?)
    } else {
        None
    };

    let names = profile
        .get("pass_env")
        .and_then(Value::as_array)
        .ok_or_else(|| core::Error::new("invalid", "resolved profile pass_env must be an array"))?;
    let mut values = serde_json::Map::new();
    for selector in frozen_store_selectors() {
        let selected = home
            .as_ref()
            .filter(|_| {
                capabilities
                    .frozen_store_environment
                    .is_some_and(|native| native.selectors.contains(&selector))
            })
            .map(|path| core::display_path(&path.to_string_lossy()).into())
            .or_else(|| admitted_selector(names, selector).and_then(std::env::var_os));
        let value = selected
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
        .filter(|name| !managed_native_update_environment_name(name))
        .filter(|name| {
            (!private_route_selector(name)
                || capabilities
                    .profile
                    .environment
                    .credential_directory
                    .is_some_and(|selector| env_name_matches(name, selector)))
                && !native_model_selector(profile, name)
        })
        .chain(profile["api_key_env"].as_str())
        .filter_map(std::env::var_os)
        .filter_map(|value| value.into_string().ok())
        .collect::<Vec<_>>();
    // Keep resolved credentials only in this temporary policy; snapshot data
    // is checked against them but never receives their values.
    let policy = core::SecretPolicy::new(credentials.iter().map(String::as_str));
    let mut snapshot = profile.clone();
    snapshot["pass_env_values"] = captured;
    if home.is_some() {
        snapshot["stand_native_store"] = Value::Bool(true);
    }
    if snapshot.get("harness").and_then(Value::as_str).is_some() {
        let selectors = native_route_selectors(&snapshot, |name| {
            if let Some(selector) = native_store_selector(name)
                && frozen_store_selectors().any(|name| name == selector)
            {
                return Ok(snapshot["pass_env_values"][selector]
                    .as_str()
                    .map(str::to_owned));
            }
            std::env::var_os(name)
                .map(|value| {
                    value.into_string().map_err(|_| {
                        core::Error::new("invalid", "native route selector is not valid Unicode")
                    })
                })
                .transpose()
        })?;
        let home = ROUTE_HOME_SELECTORS
            .iter()
            .filter_map(|name| {
                let value = selectors.get(*name)?;
                (is_native_route_home_value(value)
                    && value
                        .as_str()
                        .is_none_or(|path| !policy.contains_text(path)))
                .then(|| ((*name).to_owned(), value.clone()))
            })
            .collect();
        snapshot["native_route_home"] = Value::Object(home);
        snapshot["native_route_fingerprints"] =
            Value::Object(native_route_fingerprints(&selectors)?);
        snapshot["native_route_identity"] = Value::String(native_route_identity(selectors)?);
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

fn canonical_stand_native_home(path: &Path, display_name: &str) -> core::Result<PathBuf> {
    crate::stand_cleanup::check_directory_chain(path)?;
    path.canonicalize().map_err(|error| {
        core::Error::new(
            "platform_io",
            format!("resolve stand {display_name} home: {error}"),
        )
    })
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
    #[serde(default)]
    owner_rules: OwnerRulesConfig,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct OwnerRulesConfig {
    #[serde(default)]
    text: String,
    /// Use the built-in local delivery rule when no custom text is set.
    #[serde(default)]
    default_commit_only: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
pub(crate) struct RoutingConfig {
    #[serde(default)]
    pub(crate) default: Vec<String>,
    #[serde(default)]
    on_provider_failure: OnProviderFailure,
    /// Move owed work off a held or lost session. Absent means on.
    #[serde(default)]
    auto_handoff: Option<bool>,
    /// Ordered profiles authorized for automatic handoff.
    #[serde(default)]
    pub(crate) handoff_fallback: Vec<String>,
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

    pub(crate) fn auto_handoff(&self) -> bool {
        self.auto_handoff.unwrap_or(true)
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
/// top-left in physical screen pixels. The minimum is also the smallest
/// client size the desktop window can be resized to.
pub const DESKTOP_WINDOW_MIN: (u32, u32) = (640, 480);
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

/// `[desktop] panels`: the desktop's sidebar and context pane widths in
/// DIPs, as their drag handles left them.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DesktopPanels {
    sidebar: u32,
    pane: u32,
}

/// `[desktop] panels` bounds in DIPs: the desktop clamps its handles to
/// these (`ui/tokens.rs`).
const DESKTOP_SIDEBAR_WIDTH: std::ops::RangeInclusive<u32> = 200..=360;
const DESKTOP_PANE_WIDTH: std::ops::RangeInclusive<u32> = 320..=720;

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
    #[serde(default)]
    panels: Option<DesktopPanels>,
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
        if self.panels.as_ref().is_some_and(|panels| {
            !DESKTOP_SIDEBAR_WIDTH.contains(&panels.sidebar)
                || !DESKTOP_PANE_WIDTH.contains(&panels.pane)
        }) {
            return Err(desktop_error(
                "desktop panels must have sidebar 200 to 360 and pane 320 to 720",
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
    #[serde(default)]
    manager_grants: HashMap<String, RemoteManagerGrantSettings>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteManagerGrantSettings {
    project: String,
    audience: String,
    subject: String,
    revision: i64,
}

/// Store-size threshold for the unbounded-retention advisory: `daemon.status`
/// reports `size_advisory` and maintenance logs a line while all retention
/// windows are unset. Documented in docs/spec/configuration.md (daemon retention).
pub(crate) const RETENTION_UNBOUNDED_SIZE_ADVISORY_BYTES: u64 = 100 * 1024 * 1024;
pub(crate) const NEW_STORE_RETENTION_CONFIG: &str = "
[daemon.retention]
events_days = 30
processes_seconds = 86400
commands_days = 90
mail_days = 90

[automation]
auto_cleanup = true
";
const DEFAULT_RETENTION_INTERVAL_SECONDS: i64 = 5 * 60;
const MAX_RETENTION_INTERVAL_SECONDS: i64 = 24 * 60 * 60;

/// Physical retention policy and SQLite space maintenance. Only explicitly
/// configured windows delete history.
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
    pub(crate) fn events_retention_seconds(&self) -> Option<i64> {
        self.events_seconds.or_else(|| {
            self.events_days
                .map(|days| days.saturating_mul(24 * 60 * 60))
        })
    }

    pub(crate) fn is_unbounded(&self) -> bool {
        self.events_retention_seconds().is_none()
            && self.processes_seconds.is_none()
            && self.commands_days.is_none()
            && self.mail_days.is_none()
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
    #[serde(default)]
    pub(crate) agent_retirement: eliot_store::AgentRetirementPolicy,
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
    /// Maximum seconds Swarm waits after turn end for an Agent-started
    /// background process to exit before delivering a still-running Swarm note.
    #[serde(default = "default_background_wake_max_wait_secs")]
    pub(crate) background_wake_max_wait_secs: i64,
    /// A changed file above this size stays out of worktree checkpoints.
    #[serde(default = "default_checkpoint_max_file_bytes")]
    pub(crate) checkpoint_max_file_bytes: i64,
    /// Most changed-file bytes one worktree checkpoint reads and stores.
    #[serde(default = "default_checkpoint_max_total_bytes")]
    pub(crate) checkpoint_max_total_bytes: i64,
}

/// Bounds of one worktree checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CheckpointLimits {
    pub(crate) file_bytes: u64,
    pub(crate) total_bytes: u64,
    /// New object bytes one generation may add across all its checkpoints.
    /// Retention deletes refs, and Git keeps their objects until it collects them.
    pub(crate) generation_bytes: u64,
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
            agent_retirement: eliot_store::AgentRetirementPolicy::default(),
            log_level: default_log_level(),
            limits: core::api::HierarchyLimits::default(),
            attention_suspicion_secs: default_attention_suspicion_secs(),
            attention: AttentionConfig::default(),
            heavy_wall_secs: default_heavy_wall_secs(),
            heavy_cpu_secs: default_heavy_cpu_secs(),
            heavy_rss_bytes: default_heavy_rss_bytes(),
            session_growth_ratio_permille: default_session_growth_ratio_permille(),
            session_file_bytes: default_session_file_bytes(),
            background_wake_max_wait_secs: default_background_wake_max_wait_secs(),
            checkpoint_max_file_bytes: default_checkpoint_max_file_bytes(),
            checkpoint_max_total_bytes: default_checkpoint_max_total_bytes(),
        }
    }
}

fn default_checkpoint_max_file_bytes() -> i64 {
    16 * 1024 * 1024
}

fn default_checkpoint_max_total_bytes() -> i64 {
    64 * 1024 * 1024
}

/// Upper bound for one checkpoint, whose changed files are held in memory.
const MAX_CHECKPOINT_TOTAL_BYTES: i64 = 1024 * 1024 * 1024;
const CHECKPOINT_GENERATION_FACTOR: u64 = 8;

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

fn default_background_wake_max_wait_secs() -> i64 {
    core::DEFAULT_BACKGROUND_WAKE_MAX_WAIT_SECS
}

fn validate_absolute_setting_path(path: &str) -> std::result::Result<(), &'static str> {
    if path.len() > 4096 {
        return Err("exceeds 4096 bytes");
    }
    if path.chars().any(char::is_control) {
        return Err("contains a control character");
    }
    if !Path::new(path).is_absolute() {
        return Err("must be an absolute path");
    }
    Ok(())
}

impl DaemonConfig {
    fn validate(self) -> core::Result<Self> {
        self.agent_retirement.validate().map_err(|error| {
            core::Error::new(
                "profile_error",
                format!("config.toml: daemon.agent_retirement: {}", error.message),
            )
        })?;
        if let Some(path) = &self.chats_root {
            validate_absolute_setting_path(path).map_err(|reason| {
                core::Error::new("profile_error", format!("daemon.chats_root {reason}"))
            })?;
        }
        if self.artifact_roots.len() > 32 {
            return Err(core::Error::new(
                "profile_error",
                "daemon.artifact_roots must contain at most 32 paths",
            ));
        }
        for (index, path) in self.artifact_roots.iter().enumerate() {
            validate_absolute_setting_path(path).map_err(|reason| {
                core::Error::new(
                    "profile_error",
                    format!("daemon.artifact_roots[{index}] {reason}"),
                )
            })?;
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
        if self.background_wake_max_wait_secs <= 0
            || self.background_wake_max_wait_secs > core::MAX_BACKGROUND_WAKE_MAX_WAIT_SECS
        {
            return Err(core::Error::new(
                "profile_error",
                format!(
                    "config.toml: daemon.background_wake_max_wait_secs must be between 1 and {}",
                    core::MAX_BACKGROUND_WAKE_MAX_WAIT_SECS
                ),
            ));
        }
        if self.checkpoint_max_file_bytes <= 0
            || self.checkpoint_max_total_bytes < self.checkpoint_max_file_bytes
            || self.checkpoint_max_total_bytes > MAX_CHECKPOINT_TOTAL_BYTES
        {
            return Err(core::Error::new(
                "profile_error",
                format!(
                    "config.toml: daemon.checkpoint_max_file_bytes must be positive and at most checkpoint_max_total_bytes, which must be at most {MAX_CHECKPOINT_TOTAL_BYTES}"
                ),
            ));
        }
        Ok(Self {
            chats_root: self.chats_root,
            on_restart: self.on_restart,
            artifact_roots: self.artifact_roots,
            retention: self.retention.validate()?,
            agent_retirement: self.agent_retirement,
            limits: self.limits,
            attention_suspicion_secs: self.attention_suspicion_secs,
            attention,
            log_level: self.log_level,
            heavy_wall_secs: self.heavy_wall_secs,
            heavy_cpu_secs: self.heavy_cpu_secs,
            heavy_rss_bytes: self.heavy_rss_bytes,
            session_growth_ratio_permille: self.session_growth_ratio_permille,
            session_file_bytes: self.session_file_bytes,
            background_wake_max_wait_secs: self.background_wake_max_wait_secs,
            checkpoint_max_file_bytes: self.checkpoint_max_file_bytes,
            checkpoint_max_total_bytes: self.checkpoint_max_total_bytes,
        })
    }

    pub(crate) fn checkpoint_limits(&self) -> CheckpointLimits {
        let total_bytes = self.checkpoint_max_total_bytes.max(0) as u64;
        CheckpointLimits {
            file_bytes: self.checkpoint_max_file_bytes.max(0) as u64,
            total_bytes,
            generation_bytes: total_bytes.saturating_mul(CHECKPOINT_GENERATION_FACTOR),
        }
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

struct ProxyBaseUrls {
    provider_url: String,
}

impl<'de> Deserialize<'de> for ProxyBaseUrls {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct EndpointVisitor {
            keys: &'static [&'static str],
        }
        impl<'de> serde::de::Visitor<'de> for EndpointVisitor {
            type Value = ProxyBaseUrls;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an adopted proxy endpoint table")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut fields: A,
            ) -> Result<Self::Value, A::Error> {
                use serde::de::Error;
                let expected = self.keys[0];
                let mut provider_url = None;
                while let Some(key) = fields.next_key::<String>()? {
                    if key != expected {
                        return Err(A::Error::unknown_field(&key, self.keys));
                    }
                    if provider_url.is_some() {
                        return Err(A::Error::duplicate_field(expected));
                    }
                    provider_url = Some(fields.next_value()?);
                }
                Ok(ProxyBaseUrls {
                    provider_url: provider_url.ok_or_else(|| A::Error::missing_field(expected))?,
                })
            }
        }

        let keys = core::api::HarnessCapabilities::declarations()
            .iter()
            .map(|(_, caps)| caps.native_route.proxy_endpoint_keys())
            .find(|keys| !keys.is_empty())
            .ok_or_else(|| serde::de::Error::custom("adopted proxy endpoints are unsupported"))?;
        deserializer.deserialize_map(EndpointVisitor { keys })
    }
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
    launch_models: Vec<String>,
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
    #[serde(default, deserialize_with = "decode_local_provider")]
    local_provider: Option<Value>,
}

fn decode_local_provider<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Value>, D::Error> {
    let payload = Value::deserialize(deserializer)?;
    // Preserve the strict settings parser. Harness support for this field is
    // checked separately when the profile is prepared for admission.
    if let Some(schema) = core::api::HarnessCapabilities::declarations()
        .iter()
        .find_map(|(_, capabilities)| capabilities.profile.local_provider)
    {
        (schema.check_shape)(&payload).map_err(serde::de::Error::custom)?;
    }
    Ok(Some(payload))
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum QuotaUnknownPolicy {
    #[default]
    Admit,
    Hold,
}

const DEFAULT_OWNER_QUOTA_RESERVE_PERCENT: i64 = 20;
pub(crate) const DEFAULTED_QUOTA_RESERVE_FIELD: &str = "_quota_reserve_defaulted";

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
            if let Some(config) = cached.config.as_ref() {
                let snapshot = match cached.snapshots.get(name) {
                    Some(hit) => hit.clone(),
                    None => resolve_profile_in(config, &cached.source, name)?,
                };
                return Ok(snapshot);
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
    let defaulted = entry.quota_reserve_percent.is_none()
        && ProfileCapabilities::of(entry.harness.trim()).owner_subscription_reserve;
    let reserve_percent = entry.quota_reserve_percent.unwrap_or(if defaulted {
        DEFAULT_OWNER_QUOTA_RESERVE_PERCENT
    } else {
        0
    });
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
    let quota_unknown = eliot_plugin::capabilities::validated_native_choice(
        "quota_unknown",
        entry.quota_unknown.as_deref(),
        &["admit", "hold"],
        "must be admit or hold",
        "profile_error",
    )
    .map_err(|error| site.error(error.field, &error.reason, error.code))?
    .unwrap_or("admit");
    let snapshot = snapshot.as_object_mut().ok_or_else(|| {
        core::Error::new("internal", "resolved profile snapshot must be an object")
    })?;
    if reserve_percent > 0 {
        snapshot.insert(
            "quota_reserve_percent".to_owned(),
            Value::from(reserve_percent),
        );
        if defaulted {
            snapshot.insert(DEFAULTED_QUOTA_RESERVE_FIELD.to_owned(), Value::Bool(true));
        }
    }
    if quota_unknown != "admit" {
        snapshot.insert(
            "quota_unknown".to_owned(),
            Value::String(quota_unknown.to_owned()),
        );
    }
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
/// Default native subagent policy for each supported harness.
pub(crate) fn native_subagents_default(harness: &str) -> Option<&'static str> {
    crate::harness_capabilities::native_subagents_policy(harness)
        .map(|policy| policy.default_value())
}

/// Capabilities of a profile's `harness` field; a missing or unknown harness has none.
fn profile_capabilities(profile: &Value) -> ProfileCapabilities {
    ProfileCapabilities::of(profile["harness"].as_str().unwrap_or_default())
}

/// Freeze the harness-native subagent switch for this profile: `allow` is an
/// explicit opt-in, while `deny` asks the harness plugin to disable its
/// native subagent surface for the session. When omitted, use the
/// harness-specific capability default. The frozen value is reported in the
/// snapshot so effective settings stay truthful. Adapters without a native
/// denial mechanism continue to refuse an explicit `deny` before native start.
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
    let config = toml::from_str(source).map_err(|error| map_toml_error(source, error))?;
    validate_config_shape(&config)?;
    Ok(config)
}

fn validate_config_shape(config: &FileConfig) -> core::Result<()> {
    if config.owner_rules.text.len() > eliot_core::api::MAX_OWNER_RULES_BYTES
        || config.owner_rules.text.contains('\0')
    {
        return Err(core::Error::new(
            "profile_error",
            "config.toml: owner_rules.text exceeds 64 KiB or contains NUL",
        ));
    }
    config.desktop.validate()?;
    let mut role_candidates = Vec::with_capacity(config.routing.roles.len() + 1);
    role_candidates.push(("default", &config.routing.default));
    role_candidates.push((HANDOFF_FALLBACK_KEY, &config.routing.handoff_fallback));
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
        if role != HANDOFF_FALLBACK_KEY {
            validate_routing_role(role)?;
        }
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
    Ok(())
}

const HANDOFF_FALLBACK_KEY: &str = "handoff_fallback";

pub(crate) fn validate_routing_role(role: &str) -> core::Result<()> {
    if matches!(
        role,
        "on_provider_failure" | "auto_handoff" | HANDOFF_FALLBACK_KEY
    ) || role.is_empty()
        || role.len() > MAX_PROFILE_NAME_LEN
        || !role
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(core::Error::new(
            "profile_error",
            "config.toml: routing role names must be nonempty letters, digits, hyphen or underscore and cannot be a routing setting",
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

pub(crate) fn configured_provider_account_scopes(
    data_root: &Path,
) -> core::Result<BTreeMap<String, String>> {
    let Some((config, _)) = read_config(data_root)? else {
        return Ok(BTreeMap::new());
    };
    Ok(config
        .profiles
        .iter()
        .filter_map(|(name, entry)| {
            let entry = entry.get_ref();
            let account = entry.account.as_ref()?;
            if !config.accounts.contains_key(account) {
                return None;
            }
            let scope = core::frozen_provider_scope_for(
                name,
                &entry.harness,
                &serde_json::json!({"account": account}),
            )
            .ok()?;
            Some((name.clone(), scope))
        })
        .collect())
}

pub(crate) fn missing_profile_commands(data_root: &Path) -> Vec<(String, String, String)> {
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
            if name.trim().is_empty()
                || name.len() > MAX_PROFILE_NAME_LEN
                || name.chars().any(char::is_control)
            {
                return None;
            }
            let profile = entry.as_table()?;
            let harness = profile.get("harness")?.as_str()?.trim();
            if !core::api::is_supported_harness(harness) {
                return None;
            }
            let cmd = profile.get("cmd")?.as_str()?.trim();
            if cmd.is_empty()
                || cmd.len() > 4096
                || cmd.chars().any(char::is_control)
                || (ProfileCapabilities::of(harness).latest_cmd
                    && cmd.eq_ignore_ascii_case("latest"))
                || crate::adhoc::profile_command_exists(cmd)
            {
                return None;
            }
            Some((name.clone(), harness.to_owned(), cmd.to_owned()))
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

/// Load configured remote-manager identity grants from the owner config.
pub fn load_remote_manager_grants(data_root: &Path) -> core::Result<Vec<core::RemoteManagerGrant>> {
    let Some((config, _)) = read_config(data_root)? else {
        return Ok(Vec::new());
    };
    let Some(settings) = config.cloud_mcp else {
        return Ok(Vec::new());
    };
    parse_remote_manager_grants(settings)
}

/// Owner and manager audiences accepted by the HTTP Access verifier.
pub fn load_cloud_access_audiences(data_root: &Path) -> core::Result<Vec<String>> {
    Ok(load_cloud_access_snapshot(data_root)?.2)
}

/// One config snapshot for the HTTP origin verifier: issuer, owner audience
/// tag, and accepted audiences. Read once per verification so a grant add or
/// revoke applies before the next request without pairing values from
/// separate reads.
pub fn load_cloud_access_snapshot(data_root: &Path) -> core::Result<(String, String, Vec<String>)> {
    let (issuer, owner_audience) = load_cloud_access_config(data_root)?;
    let grants = load_remote_manager_grants(data_root)?;
    let mut audiences = Vec::with_capacity(grants.len() + 1);
    audiences.push(owner_audience.clone());
    audiences.extend(grants.into_iter().map(|grant| grant.audience));
    Ok((issuer, owner_audience, audiences))
}

fn parse_remote_manager_grants(
    settings: CloudMcpSettings,
) -> core::Result<Vec<core::RemoteManagerGrant>> {
    let valid = |value: &str, max: usize| {
        !value.is_empty()
            && value.len() <= max
            && value.trim() == value
            && value.is_ascii()
            && value.bytes().all(|byte| byte.is_ascii_graphic())
    };
    let valid_id = |value: &str| {
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    };
    let mut audiences = std::collections::HashSet::new();
    for (instance, grant) in &settings.manager_grants {
        if !valid_id(instance)
            || !valid_id(&grant.project)
            || !valid(&grant.subject, 512)
            || !valid(&grant.audience, 256)
            || grant.revision <= 0
            || grant.audience == settings.access_audience_tag
            || !audiences.insert(grant.audience.clone())
        {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: cloud_mcp.manager_grants contains an invalid or duplicate grant",
            ));
        }
    }
    Ok(settings
        .manager_grants
        .into_iter()
        .map(|(instance, grant)| core::RemoteManagerGrant {
            instance,
            project: grant.project,
            audience: grant.audience,
            subject: grant.subject,
            revision: grant.revision,
        })
        .collect())
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
                ProfileCapabilities::of(harness).local_provider.is_none()
                    && entry.local_provider.is_some(),
                "local providers have no mapping on this harness",
            ),
            (
                "project_doc_max_bytes",
                !ProfileCapabilities::of(harness).project_doc_max_bytes
                    && entry.project_doc_max_bytes.is_some(),
                "project_doc_max_bytes has no mapping on this harness",
            ),
        ],
    )?;
    let mapping = ProfileCapabilities::of(harness)
        .named_profile
        .ok_or_else(|| site.error("harness", "unsupported harness", "unsupported"))?;
    let native_refusal = |error: core::api::native_profile::NativeProfileRefusal| {
        site.error(error.field, &error.reason, error.code)
    };
    let model = entry.model.as_deref();
    if let Some(model) = model {
        validate_model(name, model, source, table_offset)?;
        if let Some(validate_native_model) = mapping.model {
            validate_native_model(model).map_err(native_refusal)?;
        }
    }
    let effort = match mapping.effort {
        ProfileEffort::Choices { allowed, refusal } => {
            eliot_plugin::capabilities::validated_native_choice(
                "effort",
                entry
                    .effort
                    .as_deref()
                    .filter(|value| !value.trim().is_empty()),
                allowed,
                refusal,
                "profile_error",
            )
            .map_err(native_refusal)?
        }
        ProfileEffort::Adapter => None,
        ProfileEffort::Freeform => validated_effort(site, entry.effort.as_deref())?,
    };
    let native = NativeProfileInput {
        model,
        effort: entry.effort.as_deref(),
        approval: entry.approval.as_deref(),
        collaboration_mode: entry.collaboration_mode.as_deref(),
        permissions: entry.permissions.as_deref(),
        approval_policy: entry.approval_policy.as_deref(),
        project_doc_max_bytes: entry.project_doc_max_bytes,
        auth_integration: entry.auth_integration.as_deref(),
        mode: entry.mode.as_deref(),
        dangerously_skip_permissions: entry.dangerously_skip_permissions,
        local_provider: entry.local_provider.as_ref(),
    };
    let mut snapshot = profile_snapshot(harness, model, effort);
    (mapping.fields)(&native, &mut snapshot).map_err(native_refusal)?;
    let tail = validate_profile_tail(name, entry, table_offset, source)?;
    if let Some(freeze_native_route) = mapping.route {
        freeze_native_route(&native, &tail.pass_env, &mut snapshot).map_err(native_refusal)?;
    }
    validate_launch_models(site, &entry.launch_models, &native, harness, mapping.model)?;
    if model.is_some() {
        snapshot.insert("model_source".to_owned(), json!("profile"));
    }
    if !entry.launch_models.is_empty() {
        snapshot.insert("launch_models".to_owned(), json!(entry.launch_models));
    }
    let control = || validated_control(name, entry.control.as_deref(), source, table_offset);
    let early_control = mapping.control_before_mcp.then(control).transpose()?;
    freeze_mcp_policy(
        &mut snapshot,
        site,
        entry,
        mapping.name,
        mapping.mcp_minimal,
        mapping.mcp_allow,
    )?;
    let control = early_control.map_or_else(control, Ok)?;
    snapshot.insert("control".to_owned(), json!(control));
    insert_profile_tail(&mut snapshot, tail, &entry.setup)?;
    Ok(Value::Object(snapshot))
}

fn validate_launch_models(
    site: ProfileSite<'_>,
    models: &[String],
    profile: &NativeProfileInput<'_>,
    harness: &str,
    validate_native: Option<fn(&str) -> core::api::native_profile::NativeProfileResult<()>>,
) -> core::Result<()> {
    use core::api::harness::LaunchModelScope;
    if models.is_empty() {
        return Ok(());
    }
    let invalid = |reason: &str| site.error("launch_models", reason, "profile_error");
    let scope = ProfileCapabilities::of(harness).launch_model_scope;
    if matches!(scope, LaunchModelScope::Unsupported) {
        return Err(invalid(
            "this harness does not support per-launch model selection",
        ));
    }
    if profile.model.is_none() || models.len() > 32 {
        return Err(invalid(
            "requires a saved model and allows at most 32 exact model IDs",
        ));
    }
    let mut distinct = HashSet::new();
    for model in models {
        if !valid_model_selector(model) || !distinct.insert(model) {
            return Err(invalid("must contain distinct, nonempty model IDs"));
        }
        if let Some(validate) = validate_native {
            validate(model).map_err(|error| invalid(&error.reason))?;
        }
        if let LaunchModelScope::Check(stays_on_route) = scope
            && !stays_on_route(profile, model).map_err(|error| invalid(&error.message))?
        {
            return Err(invalid("every model must use the saved provider route"));
        }
    }
    Ok(())
}

/// Start the frozen profile with its validated shared selectors. The wire
/// representation omits unselected model and effort on both launch paths.
pub(crate) fn profile_snapshot(
    harness: &str,
    model: Option<&str>,
    effort: Option<&str>,
) -> JsonMap<String, Value> {
    #[derive(Serialize)]
    struct Selectors<'a> {
        harness: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        model: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        effort: Option<&'a str>,
    }
    let Value::Object(fields) = json!(Selectors {
        harness,
        model,
        effort
    }) else {
        unreachable!("profile selectors serialize as an object");
    };
    fields
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
        Some(cmd)
            if ProfileCapabilities::of(harness).latest_cmd
                && cmd.trim().eq_ignore_ascii_case("latest") =>
        {
            Some("latest".to_owned())
        }
        Some(cmd) => {
            let cmd = validate_executable_name(cmd)
                .map_err(|class| {
                    profile_field_error(source, table_offset, name, "cmd", class, "profile_error")
                })?
                .to_owned();
            let cmd = if ProfileCapabilities::of(harness).script_wrapper_cmd {
                cmd
            } else {
                reject_script_wrapper(name, cmd, source, table_offset)?
            };
            if let Some(refusal) = ProfileCapabilities::of(harness).command_refusal
                && let Some(message) = refusal(&cmd)
            {
                return Err(profile_field_error(
                    source,
                    table_offset,
                    name,
                    "cmd",
                    message,
                    "profile_error",
                ));
            }
            Some(cmd)
        }
        // Named profiles follow the installed native executable at launch.
        // An explicit `cmd` remains a user pin; when omitted, daemon discovery
        // freezes the selected native path and version into that launch's
        // snapshot without changing config.toml.
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

/// Install-time reviewer-pin check for `swarm rule install-review --reviewer`.
///
/// Returns a refusal naming the pinned reviewer when its frozen harness/model
/// can never satisfy reviewer independence against the author's frozen
/// harness/model: a reviewer model with no usable family stem, or the same
/// catalogue provider and model family as the author. Returns `None` when the
/// pin looks independent or either side cannot be resolved here; the runtime
/// refusal (which names the pinned profile) stays the backstop for those
/// cases.
pub fn review_pin_refusal(data_root: &Path, author: &str, reviewer: &str) -> Option<String> {
    let (config, source) = read_config(data_root).ok()??;
    let snapshot = |name: &str| resolve_profile_in(&config, &source, name).ok();
    let reviewer_snapshot = snapshot(reviewer)?;
    let reviewer_harness = reviewer_snapshot.get("harness").and_then(Value::as_str)?;
    let reviewer_model = reviewer_snapshot
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let Some(reviewer_family) = core::api::model_family(reviewer_model) else {
        return Some(format!(
            "unknown model family for reviewer profile {reviewer:?} (model {reviewer_model:?}); \
             the reviewer needs a concrete model from a different provider and model family than the author",
        ));
    };
    let reviewer_provider =
        core::api::catalog_provider(reviewer_harness, reviewer_model).to_ascii_lowercase();
    let author_snapshot = snapshot(author)?;
    let author_harness = author_snapshot.get("harness").and_then(Value::as_str)?;
    let author_model = author_snapshot.get("model").and_then(Value::as_str)?;
    let author_family = core::api::model_family(author_model)?;
    let author_provider =
        core::api::catalog_provider(author_harness, author_model).to_ascii_lowercase();
    if reviewer_provider == author_provider && reviewer_family == author_family {
        return Some(format!(
            "reviewer profile {reviewer:?} shares the author's provider ({reviewer_provider}) \
             and model family ({reviewer_family}); \
             choose a reviewer from a different provider and model family",
        ));
    }
    None
}

/// Find a named review profile in one configuration revision. An explicit
/// profile is a pin. A role preserves its configured candidate order.
pub(crate) fn select_review_profile(
    data_root: &Path,
    pinned: Option<&str>,
    role: Option<&str>,
    approved: Option<&[String]>,
    eligible: impl Fn(&str, &Value) -> bool,
) -> core::Result<Option<String>> {
    let Some((config, source)) = read_config(data_root)? else {
        return Ok(None);
    };
    let candidates: Vec<&str> = if let Some(name) = pinned {
        vec![name]
    } else if let Some(role) = role {
        config
            .routing
            .candidates(role)
            .unwrap_or_default()
            .iter()
            .map(String::as_str)
            .collect()
    } else {
        let mut names: Vec<_> = config.profiles.keys().map(String::as_str).collect();
        names.sort_unstable();
        names
    };
    Ok(candidates.into_iter().find_map(|name| {
        if approved.is_some_and(|profiles| !profiles.iter().any(|approved| approved == name)) {
            return None;
        }
        let snapshot = resolve_profile_in(&config, &source, name).ok()?;
        eligible(name, &snapshot).then(|| name.to_owned())
    }))
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
        let harness = entry.harness.trim();
        let caps = core::api::HarnessCapabilities::of(harness);
        let route = caps.native_route;
        if !matches!(route, NativeRoute::ResponsesProxy { .. }) {
            return Err(fail(
                "proxy",
                "this harness does not support adopted Responses proxies",
            ));
        }
        validate_profile_name(proxy_name).map_err(|_| fail("proxy", "invalid proxy reference"))?;
        if route.reserves_proxy_provider(proxy_name) {
            let provider = caps
                .profile
                .named_profile
                .map_or(harness, |profile| profile.name);
            return Err(fail(
                "proxy",
                &format!("proxy name collides with a builtin {provider} provider"),
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
            provider_base_url: proxy.base_url_for.provider_url.clone(),
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
    if !valid_model_selector(model) {
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

pub(crate) fn valid_model_selector(model: &str) -> bool {
    !model.is_empty()
        && model.len() <= MAX_MODEL_LEN
        && !model.chars().any(|c| c.is_whitespace() || c.is_control())
}

/// Whether a profile names an environment variable owned by Eliot.
pub(crate) fn reserved_eliot_environment_name(name: &str) -> bool {
    name.get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("ELIOT_"))
}

/// Updater controls are managed outside profiles. In particular, an old
/// pass_env entry must never suppress the installed vendor's updates.
pub(crate) fn managed_native_update_environment_name(name: &str) -> bool {
    profile_environments()
        .flat_map(|profile| profile.managed_update.iter().copied())
        .any(|selector| name.eq_ignore_ascii_case(selector))
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
        if !managed_native_update_environment_name(name) && seen.insert(name.to_owned()) {
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
    snapshot: &mut JsonMap<String, Value>,
    site: ProfileSite<'_>,
    entry: &ProfileEntry,
    harness: &str,
    supports_minimal: bool,
    supports_allow: bool,
) -> core::Result<()> {
    // An unset `mcp` takes the default `minimal` only where the harness maps
    // it; Muse has no minimal mapping, so its profiles
    // keep loading as `inherit` instead of failing after the default change.
    let policy = match entry.mcp.as_deref() {
        None if !supports_minimal => core::McpPolicy::Inherit,
        raw => core::McpPolicy::parse(raw)
            .map_err(|error| site.error("mcp", &error.message, "profile_error"))?,
    };
    let allow = core::validate_allow_list(entry.mcp_allow.as_deref().unwrap_or(&[]))
        .map_err(|error| site.error("mcp_allow", &error.message, "profile_error"))?;
    if !allow.is_empty() && policy == core::McpPolicy::Inherit {
        return Err(site.error(
            "mcp_allow",
            "mcp_allow requires mcp = \"minimal\"",
            "profile_error",
        ));
    }
    if policy == core::McpPolicy::Minimal && !supports_minimal {
        return Err(site.error(
            "mcp",
            &format!("mcp = \"minimal\" has no {harness} mapping"),
            "unsupported",
        ));
    }
    if !allow.is_empty() && !supports_allow {
        return Err(site.error(
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
