use std::collections::HashSet;

use serde::{Deserialize, Deserializer, Serialize};
use url::Url;

/// The provider configuration Swarm accepts and freezes. The adapter owns
/// translation into the native configuration.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalProvider {
    pub id: String,
    pub package: String,
    pub base_url: String,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub api_key_env: Option<String>,
    pub models: Vec<LocalModel>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalModel {
    pub id: String,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub context_limit: Option<u64>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub output_limit: Option<u64>,
}

fn present<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

impl LocalProvider {
    pub fn validate(&self, pass_env: &[String]) -> Result<(), String> {
        if self.id.len() > 64
            || !self.id.starts_with(|ch: char| ch.is_ascii_alphanumeric())
            || !self
                .id
                .bytes()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, b'-' | b'_'))
        {
            return Err("local provider id must be a slug of 1–64 ASCII characters".into());
        }
        if !valid_package_name(&self.package) {
            return Err("local provider package must be an npm package name".into());
        }
        if !valid_base_url(&self.base_url) {
            return Err("local provider base_url must be an absolute HTTP(S) URL without userinfo, a query, a fragment or substitutions".into());
        }
        if let Some(name) = self.api_key_env.as_deref() {
            if !valid_key_env(name) {
                return Err(
                    "local provider api_key_env must name a non-reserved environment variable"
                        .into(),
                );
            }
            if !pass_env.iter().any(|allowed| {
                allowed == name || (cfg!(windows) && allowed.eq_ignore_ascii_case(name))
            }) {
                return Err("local provider api_key_env must be listed in frozen pass_env".into());
            }
        }

        if self.models.is_empty() {
            return Err("local provider must declare at least one model".into());
        }
        let mut ids = HashSet::new();
        for model in &self.models {
            if model.id.len() > 256
                || model.id.split('/').any(str::is_empty)
                || model.id.contains(['{', '}', '\\', '#'])
                || model
                    .id
                    .chars()
                    .any(|ch| ch.is_whitespace() || ch.is_control())
            {
                return Err("local provider model ids must be single tokens of 1–256 bytes with non-empty slash segments and no substitutions".into());
            }
            if !ids.insert(&model.id) {
                return Err("local provider model ids must be distinct".into());
            }
            // The native config decodes limits as JavaScript integers.
            const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
            if [model.context_limit, model.output_limit]
                .into_iter()
                .flatten()
                .any(|limit| limit > MAX_SAFE_INTEGER)
            {
                return Err("local provider model limits must be safe JavaScript integers".into());
            }
        }
        Ok(())
    }

    pub fn contains_model(&self, provider: &str, model: &str) -> bool {
        self.id == provider && self.models.iter().any(|entry| entry.id == model)
    }
}

fn valid_package_name(package: &str) -> bool {
    fn component(value: &str) -> bool {
        !value.is_empty()
            && !matches!(value, "." | "..")
            && value.bytes().all(|ch| {
                ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, b'-' | b'_' | b'.')
            })
    }

    if package.len() > 214 {
        return false;
    }
    if let Some(scoped) = package.strip_prefix('@') {
        return scoped.split_once('/').is_some_and(|(scope, name)| {
            !scope.starts_with(['.', '_']) && component(scope) && component(name)
        });
    }
    !package.starts_with(['.', '_']) && component(package)
}

fn valid_base_url(value: &str) -> bool {
    if value.contains(['{', '}', '\\'])
        || value
            .chars()
            .any(|ch| ch.is_whitespace() || ch.is_control())
    {
        return false;
    }
    let Some((_, location)) = value.split_once("://") else {
        return false;
    };
    let authority = location.split(['/', '?', '#']).next().unwrap_or_default();
    !authority.is_empty()
        && !authority.contains('@')
        && Url::parse(value).is_ok_and(|url| {
            matches!(url.scheme(), "http" | "https")
                && url.has_host()
                && url.query().is_none()
                && url.fragment().is_none()
        })
}

fn valid_key_env(name: &str) -> bool {
    const RESERVED: &[&str] = &[
        "PATH",
        "OPENCODE_CONFIG",
        "OPENCODE_CONFIG_CONTENT",
        "OPENCODE_CONFIG_DIR",
        "OPENCODE_CONFIG_PROJECT_DISABLE",
        "OPENCODE_PASSWORD",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
    ];
    name.len() <= 256
        && name.starts_with(|ch: char| ch.is_ascii_alphabetic() || ch == '_')
        && name
            .bytes()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == b'_')
        && !name.to_ascii_uppercase().starts_with("ELIOT_")
        && !RESERVED
            .iter()
            .any(|reserved| name.eq_ignore_ascii_case(reserved))
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn provider() -> Value {
        json!({
            "id": "local-1",
            "package": "@ai-sdk/openai-compatible",
            "base_url": "http://127.0.0.1:1234/v1",
            "api_key_env": "LOCAL_API_KEY",
            "models": [{ "id": "family/model", "context_limit": 8192, "output_limit": 1024 }],
        })
    }

    #[test]
    fn validates_typed_fields_and_frozen_credential_membership() {
        let mut valid: LocalProvider = serde_json::from_value(provider()).unwrap();
        assert!(valid.contains_model("local-1", "family/model"));
        assert!(valid.validate(&["LOCAL_API_KEY".to_owned()]).is_ok());
        assert!(valid.validate(&[]).is_err());
        for reserved in [
            "ELIOT_AGENT_TOKEN",
            "eliot_home",
            "ELIOT_ARTIFACT_ROOT",
            "PATH",
        ] {
            valid.api_key_env = Some(reserved.to_owned());
            assert!(valid.validate(&[reserved.to_owned()]).is_err());
        }
        valid.api_key_env = None;
        assert!(valid.validate(&[]).is_ok());
        for package in [
            "../provider",
            "@scope/name/subpath",
            "provider@latest",
            "https://example.test/provider",
        ] {
            valid.package = package.to_owned();
            assert!(valid.validate(&[]).is_err(), "{package}");
        }
    }

    #[test]
    fn rejects_nonabsolute_urls_credentials_and_substitutions() {
        let mut typed: LocalProvider = serde_json::from_value(provider()).unwrap();
        typed.api_key_env = None;
        for url in [
            "http:localhost",
            "http:///localhost",
            "ftp://example.test/v1",
            "https://user:password@example.test/v1",
            "https://@example.test/v1",
            "https://example.test/v1?%61pi_key=value",
            "https://example.test/v1?",
            "https://example.test/v1#",
            "{file:provider-url}",
            "https://example.test/{file:provider-url}",
        ] {
            typed.base_url = url.to_owned();
            assert!(typed.validate(&[]).is_err(), "{url}");
        }
    }

    #[test]
    fn refuses_native_config_and_nonnumeric_limits() {
        let mut unknown = provider();
        unknown["settings"] = json!({"credentials": {"secretAccessKey": "literal"}});
        assert!(serde_json::from_value::<LocalProvider>(unknown).is_err());
        for (key, value) in [
            ("headers", json!({"X-Test": "{env:LOCAL_API_KEY}"})),
            ("variants", json!([])),
            ("output_limit", json!("1024")),
            ("context_limit", json!(1.5)),
            ("context_limit", Value::Null),
        ] {
            let mut unknown = provider();
            unknown["models"][0][key] = value;
            assert!(
                serde_json::from_value::<LocalProvider>(unknown).is_err(),
                "{key}"
            );
        }
    }
}
