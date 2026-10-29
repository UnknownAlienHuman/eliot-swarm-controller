//! Frozen nonsecret route and capacity inputs shared by the daemon and plugins.
//! Validation is pure; credentials are referenced only by environment-variable name.

use crate::{Agent, Error, Result};
use serde::{Deserialize, Serialize};
use url::Url;

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
        if !matches!(kind, "profile" | "account" | "proxy") || !valid_name(name) {
            return Err(invalid_capacity_scope());
        }
        Ok(())
    }
}

/// Structured provider restriction facts accepted from an exact native result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    pub retry_after: Option<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderHoldState {
    Held,
    Probing,
}

impl ProviderHoldState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Held => "held",
            Self::Probing => "probing",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderHold {
    pub scope: String,
    pub kind: ProviderRestrictionKind,
    pub retry_after: Option<i64>,
    pub probe_command: Option<String>,
    pub state: ProviderHoldState,
    pub updated_at: i64,
}

impl ProviderHold {
    pub fn validate(&self) -> Result<()> {
        CapacityScope {
            scope: self.scope.clone(),
            max_live: None,
        }
        .validate()?;
        if self.probe_command.as_ref().is_some_and(|command| {
            command.trim().is_empty()
                || command.len() > crate::limits::MAX_COMMAND_ID_BYTES
                || command.chars().any(char::is_control)
        }) || matches!(self.state, ProviderHoldState::Probing) != self.probe_command.is_some()
        {
            return Err(Error::new("invalid", "invalid provider hold state"));
        }
        Ok(())
    }
}

/// Resolve the hold scope from the Agent's frozen route/account/profile only.
pub fn frozen_provider_scope(agent: &Agent) -> Result<String> {
    let snapshot = agent
        .profile_snapshot
        .as_object()
        .ok_or_else(invalid_provider_scope)?;

    let scope = if let Some(binding) = snapshot.get("route_binding") {
        let binding: RouteBinding =
            serde_json::from_value(binding.clone()).map_err(|_| invalid_provider_scope())?;
        binding.validate().map_err(|_| invalid_provider_scope())?;
        if snapshot
            .get("proxy")
            .is_some_and(|proxy| proxy.as_str() != Some(binding.name.as_str()))
        {
            return Err(invalid_provider_scope());
        }
        format!("proxy:{}", binding.name)
    } else {
        if snapshot.contains_key("proxy") {
            return Err(invalid_provider_scope());
        }
        match snapshot.get("account") {
            Some(account) => format!(
                "account:{}",
                account.as_str().ok_or_else(invalid_provider_scope)?
            ),
            None => format!("profile:{}", agent.profile),
        }
    };

    CapacityScope {
        scope: scope.clone(),
        max_live: None,
    }
    .validate()
    .map_err(|_| invalid_provider_scope())?;
    Ok(scope)
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

#[cfg(test)]
mod tests {
    use super::{
        CapacityScope, ProviderHold, ProviderHoldState, ProviderRestriction,
        ProviderRestrictionKind, RouteBinding, frozen_provider_scope,
    };
    use crate::Agent;
    use serde_json::json;

    fn route() -> RouteBinding {
        RouteBinding {
            name: "opencodex".to_owned(),
            endpoint: "http://127.0.0.1:10100".to_owned(),
            readiness_path: "/api/system/health".to_owned(),
            provider_base_url: "http://127.0.0.1:10100/v1".to_owned(),
            client_key_env_name: Some("OPENCODEX_API_KEY".to_owned()),
        }
    }

    #[test]
    fn route_binding_is_strict_and_contains_only_the_runtime_key_name() {
        let binding = route();
        binding.validate().unwrap();
        let encoded = serde_json::to_value(&binding).unwrap();
        assert_eq!(encoded["client_key_env_name"], "OPENCODEX_API_KEY");
        assert!(
            serde_json::from_value::<RouteBinding>(json!({
                "name":"opencodex",
                "endpoint":"http://127.0.0.1:10100",
                "readiness_path":"/api/system/health",
                "provider_base_url":"http://127.0.0.1:10100/v1",
                "config_digest":"unused"
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<RouteBinding>(json!({
                "name":"opencodex",
                "endpoint":"http://127.0.0.1:10100",
                "readiness_path":"/api/system/health",
                "provider_base_url":"http://127.0.0.1:10100/v1",
                "client_key_env_name":null,
                "native_provider":"duplicate"
            }))
            .is_err()
        );
    }

    #[test]
    fn route_urls_require_explicit_safe_http_endpoints_and_paths() {
        for endpoint in [
            "ftp://127.0.0.1:10100",
            "http://user:pass@127.0.0.1:10100",
            "http://@127.0.0.1:10100",
            "http://127.0.0.1:10100?token=x",
            "http://127.0.0.1:10100#fragment",
            "http://127.0.0.1:10100\n",
        ] {
            let mut binding = route();
            binding.endpoint = endpoint.to_owned();
            assert!(binding.validate().is_err(), "accepted {endpoint:?}");
        }

        for path in [
            "",
            "api/health",
            "//elsewhere/health",
            "/health?token=x",
            "/health#fragment",
        ] {
            let mut binding = route();
            binding.readiness_path = path.to_owned();
            assert!(binding.validate().is_err(), "accepted {path:?}");
        }

        let mut binding = route();
        binding.provider_base_url = "http://user@127.0.0.1:10100/v1".to_owned();
        assert!(binding.validate().is_err());
    }

    #[test]
    fn client_key_name_cannot_alias_agent_identity_or_native_store_selectors() {
        for name in ["ELIOT_AGENT_TOKEN", "CODEX_HOME", "CODEX_SQLITE_HOME"] {
            let mut binding = route();
            binding.client_key_env_name = Some(name.to_owned());
            assert!(binding.validate().is_err(), "accepted {name}");
        }

        if cfg!(windows) {
            let mut binding = route();
            binding.client_key_env_name = Some("codex_home".to_owned());
            assert!(binding.validate().is_err());
        }
    }

    #[test]
    fn capacity_scope_has_unlimited_and_disabled_values() {
        let unlimited: CapacityScope =
            serde_json::from_value(json!({"scope":"profile:codex-luna"})).unwrap();
        assert_eq!(unlimited.max_live, None);
        unlimited.validate().unwrap();

        let disabled = CapacityScope {
            scope: "proxy:opencodex".to_owned(),
            max_live: Some(0),
        };
        disabled.validate().unwrap();
        assert_eq!(serde_json::to_value(disabled).unwrap()["max_live"], 0);

        assert!(
            serde_json::from_value::<CapacityScope>(json!({"scope":"route:x"}))
                .unwrap()
                .validate()
                .is_err()
        );
        assert!(
            serde_json::from_value::<CapacityScope>(json!({"scope":"proxy:a:b"}))
                .unwrap()
                .validate()
                .is_err()
        );
        assert!(
            serde_json::from_value::<CapacityScope>(json!({"scope":"proxy:x","other":1})).is_err()
        );
    }

    #[test]
    fn provider_scope_uses_only_frozen_route_account_then_profile() {
        let mut agent = Agent::new("a1", "project", "codex", "codex-luna", 0);
        let binding = route();
        agent.profile_snapshot = json!({
            "proxy":"opencodex",
            "account":"main",
            "route_binding":binding
        });
        assert_eq!(frozen_provider_scope(&agent).unwrap(), "proxy:opencodex");

        agent.profile_snapshot = json!({"account":"main"});
        assert_eq!(frozen_provider_scope(&agent).unwrap(), "account:main");

        agent.profile_snapshot = json!({});
        assert_eq!(frozen_provider_scope(&agent).unwrap(), "profile:codex-luna");

        agent.profile_snapshot = json!({"proxy":"opencodex"});
        assert!(frozen_provider_scope(&agent).is_err());
    }

    #[test]
    fn provider_restrictions_and_holds_are_closed_and_probe_bound() {
        assert_eq!(
            serde_json::to_value(ProviderRestrictionKind::RateLimit).unwrap(),
            "rate_limit"
        );
        assert_eq!(
            serde_json::from_value::<ProviderRestrictionKind>(json!("auth")).unwrap(),
            ProviderRestrictionKind::Auth
        );
        assert_eq!(
            serde_json::from_value::<ProviderRestrictionKind>(json!("model_removed")).unwrap(),
            ProviderRestrictionKind::ModelRemoved
        );
        assert!(
            serde_json::from_value::<ProviderRestrictionKind>(json!("billing_disabled")).is_err()
        );

        let restriction = ProviderRestriction {
            kind: ProviderRestrictionKind::Quota,
            retry_after: Some(123),
        };
        assert_eq!(
            serde_json::from_value::<ProviderRestriction>(
                serde_json::to_value(restriction).unwrap()
            )
            .unwrap()
            .retry_after,
            Some(123)
        );

        let probing = ProviderHold {
            scope: "proxy:opencodex".to_owned(),
            kind: ProviderRestrictionKind::RateLimit,
            retry_after: Some(123),
            probe_command: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
            state: ProviderHoldState::Probing,
            updated_at: 456,
        };
        probing.validate().unwrap();
        let mut long_probe = probing.clone();
        long_probe.probe_command = Some("p".repeat(crate::limits::MAX_COMMAND_ID_BYTES + 1));
        assert!(long_probe.validate().is_err());
        let mut inconsistent = probing;
        inconsistent.probe_command = None;
        assert!(inconsistent.validate().is_err());

        for kind in [
            ProviderRestrictionKind::Auth,
            ProviderRestrictionKind::ModelRemoved,
        ] {
            let transitional = ProviderHold {
                scope: "account:main".to_owned(),
                kind,
                retry_after: None,
                probe_command: Some("probe-id".to_owned()),
                state: ProviderHoldState::Probing,
                updated_at: 789,
            };
            // A stronger native restriction can arrive while the claimed probe is
            // still unresolved. Transaction policy prevents starting a new one.
            transitional.validate().unwrap();
        }

        let removed_probe = ProviderHold {
            scope: "account:main".to_owned(),
            kind: ProviderRestrictionKind::ModelRemoved,
            retry_after: None,
            probe_command: Some("probe-id".to_owned()),
            state: ProviderHoldState::Probing,
            updated_at: 789,
        };
        removed_probe.validate().unwrap();
    }
}
