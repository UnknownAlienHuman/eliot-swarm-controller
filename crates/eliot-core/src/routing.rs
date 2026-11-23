//! Frozen nonsecret route and capacity inputs shared by the daemon and plugins.
//! Validation is pure; credentials are referenced only by environment-variable name.

use crate::{Agent, Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

/// The routing choice frozen at launch admission. `profile` is absent when
/// policy had no available candidate; `quota_status` keeps unsupported or
/// unavailable readback visible even when the selected candidate is usable.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingDecision {
    pub role: String,
    pub profile: Option<String>,
    pub source: String,
    pub reason: String,
    pub skipped: Vec<RouteSkip>,
    pub quota_status: RouteQuotaStatus,
    /// Frozen, ordered tail of an Owner's opted-in role launch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_failover: Option<Vec<String>>,
    /// Preserve automation admission policy on a provider successor.
    #[serde(default, skip_serializing_if = "crate::is_false")]
    pub owner_automation: bool,
}

/// Why an ordered routing candidate was not selected.
#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteSkipReason {
    NotInstalled,
    MissingProfileCommand,
    ProviderHold,
    ProviderFailure,
    QuotaExhausted,
    QuotaReserve,
    QuotaUnknown,
    CapacityFull,
    Unauthorized,
    InvalidProfile,
}

impl RouteSkipReason {
    pub fn user_message(self) -> &'static str {
        match self {
            Self::NotInstalled => "not installed",
            Self::MissingProfileCommand => {
                "pinned executable missing (omit cmd to follow the installation)"
            }
            Self::ProviderHold => "provider on hold",
            Self::ProviderFailure => "provider refused the previous attempt",
            Self::QuotaExhausted => "quota used up",
            Self::QuotaReserve => "owner quota reserve",
            Self::QuotaUnknown => "quota unknown and profile requires a reading",
            Self::CapacityFull => "at capacity",
            Self::Unauthorized => "not allowed here",
            Self::InvalidProfile => "profile does not resolve",
        }
    }
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteQuotaStatus {
    Supported,
    Unsupported,
    Unknown,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteSkip {
    pub profile: String,
    pub reason: RouteSkipReason,
    pub quota_status: RouteQuotaStatus,
}

/// Explicit selectors belong to an ad-hoc launch or a saved role profile.
pub const PROVIDER_LAUNCH_OVERRIDE_FIELDS: &[&str] = &[
    "model",
    "effort",
    "access",
    "auth",
    "cmd",
    "permissions",
    "approval_policy",
    "approval",
    "permission_mode",
    "mode",
    "dangerously_skip_permissions",
    "auth_integration",
    "workspace_mode",
    "inherit_env",
    "secret_env_names",
    "collaboration_mode",
    "native_subagents",
    "control",
    "child_profiles",
    "mcp",
    "mcp_allow",
    "sandbox",
    "account",
    "proxy",
    "route_binding",
    "api_key_env",
    "pass_env",
    "literal_env",
];

/// Authority must match exactly for an automatic switch. Profile order,
/// model and effort can vary. The normal launch still verifies native
/// readback against the selected frozen permissions and approval policy.
pub fn provider_switch_authority(snapshot: &Value) -> Result<[u8; 32]> {
    if !snapshot.is_object() || snapshot["harness"].as_str().is_none() {
        return Err(Error::new(
            "source_integrity",
            "provider switch has no frozen profile authority",
        ));
    }
    let fields = [
        "harness",
        "access",
        "permissions",
        "approval_policy",
        "approval",
        "permission_mode",
        "dangerously_skip_permissions",
        "mode",
        "sandbox",
        "native_subagents",
        "control",
        "child_profiles",
        "mcp",
        "mcp_allow",
        "mcp_cloud_hostname",
        "account",
        "proxy",
        "route_binding",
        "auth_integration",
        "native_route_identity",
        "api_key_env",
        "pass_env",
        "pass_env_values",
        "literal_env",
        "cmd",
        "setup",
        "collaboration_mode",
        "workspace_mode",
        "inherit_env",
        "secret_env_names",
    ];
    let authority = Value::Object(
        fields
            .into_iter()
            .filter_map(|key| {
                snapshot
                    .get(key)
                    .map(|value| (key.to_owned(), value.clone()))
            })
            .collect(),
    );
    crate::canonical_json_sha256(&authority)
        .map_err(|_| Error::new("internal", "provider switch authority could not be encoded"))
}

const MAX_NAME_BYTES: usize = 128;
const MAX_ENV_NAME_BYTES: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteBinding {
    /// Name used as the Codex provider ID.
    pub name: String,
    /// Owner-run endpoint used for bounded readiness reads.
    pub endpoint: String,
    /// Explicit absolute path for readiness reads.
    pub readiness_path: String,
    /// Explicit Responses-compatible provider base URL.
    pub provider_base_url: String,
    /// Runtime-only credential reference; its value is never part of this DTO.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_key_env_name: Option<String>,
}

impl RouteBinding {
    pub fn validate(&self) -> Result<()> {
        if !valid_name(&self.name) {
            return Err(invalid_route("invalid route name"));
        }

        let endpoint = validate_http_url(&self.endpoint)
            .map_err(|_| invalid_route("invalid route endpoint"))?;
        validate_http_url(&self.provider_base_url)
            .map_err(|_| invalid_route("invalid provider base URL"))?;

        if !valid_readiness_path(&self.readiness_path, &endpoint) {
            return Err(invalid_route("invalid route readiness path"));
        }

        if self
            .client_key_env_name
            .as_deref()
            .is_some_and(|name| !valid_client_key_env_name(name))
        {
            return Err(invalid_route("invalid route client key environment name"));
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityScope {
    /// Stable local key, such as `profile:codex-luna` or `proxy:opencodex`.
    pub scope: String,
    /// `None` is unlimited; zero disables admission to the scope.
    #[serde(default)]
    pub max_live: Option<u64>,
}

impl CapacityScope {
    pub fn validate(&self) -> Result<()> {
        let Some((kind, name)) = self.scope.split_once(':') else {
            return Err(invalid_capacity_scope());
        };
        match kind {
            "profile" | "account" | "proxy" => {
                if !valid_name(name) {
                    return Err(invalid_capacity_scope());
                }
            }
            "global" => {
                if name != "swarm" {
                    return Err(invalid_capacity_scope());
                }
            }
            "project" | "manager_assignment" => {
                if !valid_project_scope_name(name) {
                    return Err(invalid_capacity_scope());
                }
            }
            _ => return Err(invalid_capacity_scope()),
        }
        Ok(())
    }

    /// Stable scope key for this daemon's controlled load.
    pub fn global_scope() -> String {
        "global:swarm".to_owned()
    }

    /// Stable scope key for one registered project.
    pub fn is_daemon_owned(scope: &str) -> bool {
        scope == "global:swarm"
            || scope.starts_with("project:")
            || scope.starts_with("manager_assignment:")
    }

    pub fn project_scope(project: &str) -> String {
        format!("project:{project}")
    }

    /// Stable scope key for one project manager assignment.
    pub fn manager_assignment_scope(project: &str) -> String {
        format!("manager_assignment:{project}")
    }
}

/// Project and manager-assignment scope names ride the registered project id
/// when one is discovered, else the launch scope label, which defaults to the
/// canonical workspace path. Names only need to be non-empty, bounded and
/// free of control characters; the kind/name split is always the first colon.
/// Project and manager-assignment scope names ride the registered project id
/// when one is discovered, else the launch scope label, which defaults to the
/// canonical workspace path. Names only need to be non-empty, bounded and
/// free of control characters; the kind/name split is always the first colon.
fn valid_project_scope_name(value: &str) -> bool {
    if value.is_empty() || value.len() > 1024 {
        return false;
    }
    !value.contains(char::is_control)
}

/// Structured provider restriction facts accepted from an exact native result.
#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRestrictionKind {
    RateLimit,
    Quota,
    Auth,
    ModelRemoved,
}

impl ProviderRestrictionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RateLimit => "rate_limit",
            Self::Quota => "quota",
            Self::Auth => "auth",
            Self::ModelRemoved => "model_removed",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRestriction {
    pub kind: ProviderRestrictionKind,
    /// Vendor-supplied reset time in Unix milliseconds.
    pub retry_after: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
}

impl ProviderRestriction {
    pub fn validate(&self) -> Result<()> {
        if self.retry_after.is_some_and(|at| at < 0)
            || !valid_refusal_metadata(self.code.as_deref(), self.http_status)
        {
            return Err(Error::new("invalid", "invalid provider restriction"));
        }
        Ok(())
    }
}

fn valid_refusal_metadata(code: Option<&str>, http_status: Option<u16>) -> bool {
    code.is_none_or(|code| {
        !code.is_empty()
            && code.len() <= 128
            && code
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_-.".contains(&byte))
    }) && http_status.is_none_or(|status| (100..=599).contains(&status))
}

/// Origin of the latest refusal recorded for a hold.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderHoldSource {
    pub harness: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderHoldState {
    Held,
    Probing,
}

/// Reserved launch argument set only by daemon-owned provider maintenance.
pub const PROVIDER_PROBE_LAUNCH_ARG: &str = "_provider_probe";
/// Fixed task sent by the isolated maintenance probe Agent.
pub const PROVIDER_PROBE_PROMPT: &str = "Reply with exactly OK. Do not use tools or modify files.";

impl ProviderHoldState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Held => "held",
            Self::Probing => "probing",
        }
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderHold {
    /// Frozen account-and-harness key or exact-profile key.
    pub scope: String,
    pub kind: ProviderRestrictionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<ProviderHoldSource>,
    pub retry_after: Option<i64>,
    pub probe_command: Option<String>,
    pub state: ProviderHoldState,
    pub updated_at: i64,
    /// Next automatic minimal probe time when no provider reset is known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_probe_at: Option<i64>,
    /// Timestamp when the current uninterrupted hold began.
    #[serde(default)]
    pub held_since: i64,
    #[serde(default)]
    pub automatic_probes: u32,
    #[serde(default)]
    pub probe_outcome_unknown: bool,
}

pub const MAX_AUTOMATIC_PROVIDER_PROBES: u32 = 6;
pub const UNATTRIBUTED_PROVIDER_HOLD_REASON: &str =
    "legacy account hold has no attributable harness";
/// Allows weekly quota windows while bounding untrusted vendor deadlines.
pub const MAX_PROVIDER_RESET_HORIZON_MS: i64 = 8 * 24 * 60 * 60 * 1_000;

impl ProviderHold {
    pub fn validate(&self) -> Result<()> {
        validate_provider_scope(&self.scope)?;
        let latest_reset = self
            .updated_at
            .saturating_add(MAX_PROVIDER_RESET_HORIZON_MS);
        if self.source.as_ref().is_some_and(|source| {
            source.harness.trim().is_empty()
                || source.harness.chars().any(char::is_control)
                || !valid_refusal_metadata(source.code.as_deref(), source.http_status)
        }) || self
            .retry_after
            .is_some_and(|at| at < 0 || at > latest_reset)
            || self.next_probe_at.is_some_and(|at| at < 0)
            || self.automatic_probes > MAX_AUTOMATIC_PROVIDER_PROBES
            || self.held_since < 0
            || self.held_since > self.updated_at
            || (self.retry_after.is_some() && self.next_probe_at.is_some())
            || (self.next_probe_at.is_some()
                && !matches!(
                    self.kind,
                    ProviderRestrictionKind::RateLimit | ProviderRestrictionKind::Quota
                ))
            || self.probe_command.as_ref().is_some_and(|command| {
                command.trim().is_empty()
                    || command.len() > crate::limits::MAX_COMMAND_ID_BYTES
                    || command.chars().any(char::is_control)
            })
            || matches!(self.state, ProviderHoldState::Probing) != self.probe_command.is_some()
        {
            return Err(Error::new("invalid", "invalid provider hold state"));
        }
        Ok(())
    }
}

pub fn is_unattributed_provider_hold(scope: &str) -> bool {
    scope
        .strip_prefix("account:")
        .is_some_and(|name| !name.contains('.'))
}

/// Resolve the hold scope from the Agent's frozen account and harness, or profile only.
/// A shared adopted proxy (`route_binding`/`proxy`) is a capacity scope, never
/// a provider-hold scope: a per-turn restriction proves only the admitted
/// account/profile restriction, not that the whole proxy service is held.
pub fn frozen_provider_scope(agent: &Agent) -> Result<String> {
    frozen_provider_scope_for(&agent.profile, &agent.harness, &agent.profile_snapshot)
}

/// Resolve the same frozen provider scope while projecting a compact Agent
/// row from the stored profile snapshot. Account scopes include its harness so
/// identically named accounts in different harnesses remain independent. The
/// canonical account key is `account:<name>.<harness>`.
pub fn frozen_provider_scope_for(
    profile: &str,
    harness: &str,
    profile_snapshot: &Value,
) -> Result<String> {
    let snapshot = profile_snapshot
        .as_object()
        .ok_or_else(invalid_provider_scope)?;

    let scope = match snapshot.get("account") {
        Some(account) => {
            let account = account.as_str().ok_or_else(invalid_provider_scope)?;
            format!("account:{account}.{harness}")
        }
        None => format!("profile:{profile}"),
    };

    validate_provider_scope(&scope).map_err(|_| invalid_provider_scope())?;
    Ok(scope)
}

pub(crate) fn validate_provider_scope(scope: &str) -> Result<()> {
    if let Some(account) = scope.strip_prefix("account:")
        && let Some((name, harness)) = account.split_once('.')
    {
        // Profile/account names cannot contain '.', so this delimiter keeps
        // the harness-qualified key disjoint from legacy `account:<name>` keys.
        return if valid_name(name) && crate::api::is_supported_harness(harness) {
            Ok(())
        } else {
            Err(invalid_provider_scope())
        };
    }

    CapacityScope {
        scope: scope.to_owned(),
        max_live: None,
    }
    .validate()
}

fn invalid_provider_scope() -> Error {
    Error::new("invalid", "frozen provider scope is invalid")
}

fn validate_http_url(value: &str) -> std::result::Result<Url, ()> {
    if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
        return Err(());
    }

    let url = Url::parse(value).map_err(|_| ())?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || authority_has_userinfo(value)
    {
        return Err(());
    }
    Ok(url)
}

fn authority_has_userinfo(value: &str) -> bool {
    let Some((_, after_scheme)) = value.split_once("://") else {
        return false;
    };
    after_scheme
        .split(['/', '?', '#'])
        .next()
        .is_some_and(|authority| authority.contains('@'))
}

fn valid_readiness_path(path: &str, endpoint: &Url) -> bool {
    if path.is_empty()
        || path.trim() != path
        || !path.starts_with('/')
        || path.starts_with("//")
        || path.contains(['?', '#'])
        || path.chars().any(char::is_control)
    {
        return false;
    }

    let Ok(target) = endpoint.join(path) else {
        return false;
    };
    target.scheme() == endpoint.scheme()
        && target.host_str() == endpoint.host_str()
        && target.port_or_known_default() == endpoint.port_or_known_default()
        && target.query().is_none()
        && target.fragment().is_none()
        && target.username().is_empty()
        && target.password().is_none()
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_NAME_BYTES
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
}

fn valid_client_key_env_name(value: &str) -> bool {
    let mut characters = value.chars();
    let Some(first) = characters.next() else {
        return false;
    };
    let valid_syntax = value.len() <= MAX_ENV_NAME_BYTES
        && (first.is_ascii_alphabetic() || first == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_');
    if !valid_syntax || value.to_ascii_uppercase().starts_with("ELIOT_AGENT_") {
        return false;
    }

    let is_selector = if cfg!(windows) {
        value.eq_ignore_ascii_case("CODEX_HOME") || value.eq_ignore_ascii_case("CODEX_SQLITE_HOME")
    } else {
        value == "CODEX_HOME" || value == "CODEX_SQLITE_HOME"
    };
    !is_selector
}

fn invalid_route(message: &str) -> Error {
    Error::new("invalid", message)
}

fn invalid_capacity_scope() -> Error {
    Error::new("invalid", "invalid capacity scope")
}
