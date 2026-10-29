//! Frozen per-process Codex provider routing.
//!
//! Route credentials are referenced by environment-variable name and captured
//! only into the runtime redaction policies.
use eliot_core::{Error, Result, RouteBinding, SecretPolicy};
use serde_json::Value;
use std::ffi::OsString;

pub(super) const CHATGPT_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";

pub(super) enum CodexRoute {
    Direct,
    Responses(RouteBinding),
}

impl CodexRoute {
    pub(super) fn from_profile(profile: &Value) -> Result<Self> {
        let profile = profile
            .as_object()
            .ok_or_else(|| route_error("Codex requires a frozen profile object"))?;
        let Some(raw) = profile.get("route_binding") else {
            return Ok(Self::Direct);
        };
        if !raw.is_object() {
            return Err(route_error(
                "Codex route_binding must be an explicit frozen object",
            ));
        }
        let binding: RouteBinding = serde_json::from_value(raw.clone())
            .map_err(|_| route_error("Codex frozen route binding is malformed"))?;
        binding
            .validate()
            .map_err(|_| route_error("Codex frozen route binding is invalid"))?;

        // Built-in providers cannot be redefined as adopted Responses routes.
        if [
            "openai",
            "amazon-bedrock",
            "amazon-bedrock-runtime",
            "ollama",
            "lmstudio",
            "ollama-chat",
        ]
        .iter()
        .any(|provider| binding.name.eq_ignore_ascii_case(provider))
        {
            return Err(route_error(
                "Codex named route cannot replace a built-in provider",
            ));
        }
        Ok(Self::Responses(binding))
    }

    pub(super) fn provider(&self) -> &str {
        match self {
            Self::Direct => "openai",
            Self::Responses(binding) => &binding.name,
        }
    }

    pub(super) fn requires_chatgpt(&self) -> bool {
        matches!(self, Self::Direct)
    }

    pub(super) fn args(&self) -> Result<Vec<OsString>> {
        let mut args = Vec::new();
        push_override(&mut args, "model_provider", quote(self.provider())?);
        match self {
            Self::Direct => push_override(&mut args, "openai_base_url", quote(CHATGPT_BASE_URL)?),
            Self::Responses(binding) => {
                let mut provider = format!(
                    "{{ name = {}, base_url = {}, wire_api = \"responses\", requires_openai_auth = false",
                    quote(&binding.name)?,
                    quote(&binding.provider_base_url)?,
                );
                if let Some(name) = &binding.client_key_env_name {
                    provider.push_str(&format!(", env_key = {}", quote(name)?));
                }
                provider.push_str(" }");
                // Keep the dynamic provider name inside one TOML value. Codex
                // 0.161 splits override paths on literal dots before decoding.
                push_override(
                    &mut args,
                    "model_providers",
                    format!("{{ {} = {provider} }}", quote(&binding.name)?),
                );
            }
        }
        args.extend(["app-server", "--listen", "stdio://"].map(OsString::from));
        Ok(args)
    }

    /// Capture the configured credential even when its name does not match
    /// the ordinary secret-name heuristic. Values never enter argv or config.
    pub(super) fn capture_client_secret(
        &self,
        variables: &[(OsString, OsString)],
        diagnostic_secrets: &mut Vec<String>,
        metadata_secrets: &mut Vec<String>,
    ) -> Result<()> {
        let Self::Responses(binding) = self else {
            return Ok(());
        };
        let Some(name) = &binding.client_key_env_name else {
            return Ok(());
        };
        let matches: Vec<_> = variables
            .iter()
            .filter(|(key, _)| {
                key.to_str().is_some_and(|key| {
                    if cfg!(windows) {
                        key.eq_ignore_ascii_case(name)
                    } else {
                        key == name
                    }
                })
            })
            .collect();
        if matches.len() != 1 {
            return Err(route_error(
                "configured proxy client credential is unavailable or ambiguous",
            ));
        }
        let value = matches[0]
            .1
            .to_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| route_error("configured proxy client credential is unavailable"))?;
        diagnostic_secrets.push(value.to_owned());
        metadata_secrets.push(value.to_owned());
        Ok(())
    }

    /// Check the flattened effective config returned by `config/read`.
    pub(super) fn verify_config(&self, config: &Value, metadata_secrets: &[String]) -> Result<()> {
        if SecretPolicy::new(metadata_secrets.iter().map(String::as_str)).contains_value(config) {
            return Err(route_error(
                "Codex effective config contains a configured credential",
            ));
        }
        if config.get("model_provider").and_then(Value::as_str) != Some(self.provider()) {
            return Err(route_error(
                "Codex effective provider does not match the frozen route",
            ));
        }
        match self {
            Self::Direct
                if config.get("openai_base_url").and_then(Value::as_str)
                    == Some(CHATGPT_BASE_URL) =>
            {
                Ok(())
            }
            Self::Direct => Err(route_error(
                "Codex effective ChatGPT base URL does not match the direct route",
            )),
            Self::Responses(binding) => {
                let provider = config
                    .get("model_providers")
                    .and_then(|providers| providers.get(&binding.name))
                    .and_then(Value::as_object)
                    .ok_or_else(|| route_error("Codex effective named provider is unavailable"))?;
                let env_key_matches = match provider.get("env_key") {
                    None | Some(Value::Null) => binding.client_key_env_name.is_none(),
                    Some(Value::String(name)) => binding.client_key_env_name.as_ref() == Some(name),
                    _ => false,
                };
                if provider.get("name").and_then(Value::as_str) != Some(binding.name.as_str())
                    || provider.get("base_url").and_then(Value::as_str)
                        != Some(binding.provider_base_url.as_str())
                    || provider.get("wire_api").and_then(Value::as_str) != Some("responses")
                    || provider
                        .get("requires_openai_auth")
                        .and_then(Value::as_bool)
                        != Some(false)
                    || !env_key_matches
                {
                    return Err(route_error(
                        "Codex effective Responses endpoint or authentication does not match the frozen route",
                    ));
                }
                if [
                    "experimental_bearer_token",
                    "auth",
                    "command",
                    "gateway_oauth",
                    "aws",
                ]
                .iter()
                .any(|field| provider.get(*field).is_some_and(|value| !value.is_null()))
                    || ["http_headers", "env_http_headers", "query_params"]
                        .iter()
                        .any(|field| {
                            provider.get(*field).is_some_and(|value| {
                                !value.is_null()
                                    && value.as_object().is_none_or(|entries| !entries.is_empty())
                            })
                        })
                {
                    return Err(route_error(
                        "named provider retains unadmitted headers or credentials",
                    ));
                }
                Ok(())
            }
        }
    }

    pub(super) fn explicit_effort(&self, effort: Option<&str>) -> Result<String> {
        if self.requires_chatgpt() {
            return Err(route_error(
                "direct route effort requires its advertised-model check",
            ));
        }
        effort
            .filter(|effort| !effort.trim().is_empty() && !effort.chars().any(char::is_control))
            .map(str::to_owned)
            .ok_or_else(|| {
                route_error("named Responses routes require an explicit nonempty reasoning effort")
            })
    }
}

fn push_override(args: &mut Vec<OsString>, key: &str, value: String) {
    args.push(OsString::from("-c"));
    args.push(OsString::from(format!("{key}={value}")));
}

fn quote(value: &str) -> Result<String> {
    if value.chars().any(char::is_control) {
        return Err(route_error(
            "Codex route contains an invalid control character",
        ));
    }
    serde_json::to_string(value).map_err(|_| route_error("Codex route could not be encoded"))
}

fn route_error(message: &'static str) -> Error {
    Error::new("profile_error", message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn named(key: Option<&str>) -> CodexRoute {
        CodexRoute::Responses(RouteBinding {
            name: "corp_gateway".to_owned(),
            endpoint: "http://127.0.0.1:10300".to_owned(),
            readiness_path: "/readyz".to_owned(),
            provider_base_url: "http://127.0.0.1:10300/v1".to_owned(),
            client_key_env_name: key.map(str::to_owned),
        })
    }

    fn named_config(key: Option<&str>) -> Value {
        let mut provider = json!({
            "name":"corp_gateway",
            "base_url":"http://127.0.0.1:10300/v1",
            "wire_api":"responses",
            "requires_openai_auth":false,
            "experimental_bearer_token":null,
            "auth":null,
            "command":null,
            "gateway_oauth":null,
            "aws":null,
            "http_headers":null,
            "env_http_headers":null,
            "query_params":null
        });
        if let Some(key) = key {
            provider["env_key"] = json!(key);
        }
        json!({
            "model_provider":"corp_gateway",
            "model_providers":{"corp_gateway":provider}
        })
    }

    #[test]
    fn direct_route_preserves_chatgpt_argv_and_config_readback() {
        let route = CodexRoute::from_profile(&json!({"harness":"codex"})).unwrap();
        assert_eq!(
            route.args().unwrap(),
            [
                "-c",
                "model_provider=\"openai\"",
                "-c",
                "openai_base_url=\"https://chatgpt.com/backend-api/codex\"",
                "app-server",
                "--listen",
                "stdio://"
            ]
            .map(OsString::from)
        );
        assert!(route.requires_chatgpt());
        route
            .verify_config(
                &json!({"model_provider":"openai", "openai_base_url":CHATGPT_BASE_URL}),
                &[],
            )
            .unwrap();
        assert!(
            route
                .verify_config(&json!({"model_provider":"other"}), &[])
                .is_err()
        );
    }

    #[test]
    fn named_route_uses_one_literal_provider_override_and_env_key_name_only() {
        let route = named(Some("LOCAL_CLIENT"));
        let args = route
            .args()
            .unwrap()
            .into_iter()
            .map(|value| value.into_string().unwrap())
            .collect::<Vec<_>>();
        let provider_overrides = args
            .iter()
            .filter(|argument| argument.starts_with("model_providers="))
            .collect::<Vec<_>>();
        assert_eq!(provider_overrides.len(), 1);
        assert!(provider_overrides[0].contains("\"corp_gateway\" = { name = \"corp_gateway\""));
        assert!(provider_overrides[0].contains("wire_api = \"responses\""));
        assert!(provider_overrides[0].contains("requires_openai_auth = false"));
        assert!(provider_overrides[0].contains("env_key = \"LOCAL_CLIENT\""));
        assert_eq!(route.provider(), "corp_gateway");
        route
            .verify_config(&named_config(Some("LOCAL_CLIENT")), &[])
            .unwrap();
    }

    #[test]
    fn configured_client_value_is_captured_in_both_secret_policies_only() {
        let route = named(Some("LOCAL_CLIENT"));
        let secret = OsString::from("unusual-name-private-value");
        let variables = [(OsString::from("LOCAL_CLIENT"), secret.clone())];
        let mut diagnostic = Vec::new();
        let mut metadata = Vec::new();
        route
            .capture_client_secret(&variables, &mut diagnostic, &mut metadata)
            .unwrap();
        assert_eq!(diagnostic, vec!["unusual-name-private-value".to_owned()]);
        assert_eq!(metadata, diagnostic);
        let args = route.args().unwrap();
        assert!(args.iter().all(|arg| arg != &secret));
        assert!(
            route
                .verify_config(&named_config(Some("LOCAL_CLIENT")), &metadata)
                .is_ok()
        );
        let mut leaked = named_config(Some("LOCAL_CLIENT"));
        leaked["model_providers"]["corp_gateway"]["diagnostic"] =
            json!("unusual-name-private-value");
        assert!(route.verify_config(&leaked, &metadata).is_err());
        assert!(
            route
                .capture_client_secret(&[], &mut Vec::new(), &mut Vec::new())
                .is_err()
        );
    }

    #[test]
    fn named_route_requires_explicit_effort_and_rejects_hidden_authentication() {
        let route = named(None);
        assert_eq!(
            route.explicit_effort(Some("custom-effort")).unwrap(),
            "custom-effort"
        );
        assert!(route.explicit_effort(None).is_err());
        assert!(route.explicit_effort(Some(" \n ")).is_err());
        route.verify_config(&named_config(None), &[]).unwrap();

        for field in [
            "experimental_bearer_token",
            "auth",
            "command",
            "gateway_oauth",
            "aws",
            "http_headers",
            "env_http_headers",
            "query_params",
        ] {
            let mut config = named_config(None);
            config["model_providers"]["corp_gateway"][field] = json!({"inherited":"secret"});
            assert!(
                route.verify_config(&config, &[]).is_err(),
                "accepted {field}"
            );
        }

        let mut wrong_base = named_config(None);
        wrong_base["model_providers"]["corp_gateway"]["base_url"] =
            json!("http://127.0.0.1:10400/v1");
        assert!(route.verify_config(&wrong_base, &[]).is_err());

        for (field, value) in [
            ("name", json!("other")),
            ("wire_api", json!("chat")),
            ("requires_openai_auth", json!(true)),
            ("env_key", json!("UNADMITTED_CLIENT")),
        ] {
            let mut config = named_config(None);
            config["model_providers"]["corp_gateway"][field] = value;
            assert!(
                route.verify_config(&config, &[]).is_err(),
                "accepted {field}"
            );
        }
    }

    #[test]
    fn frozen_route_uses_core_validation_and_rejects_builtin_providers() {
        let route = json!({
            "name":"corp",
            "endpoint":"http://127.0.0.1:10300",
            "readiness_path":"/readyz",
            "provider_base_url":"http://127.0.0.1:10300/v1",
            "native_provider":"corp",
            "config_digest":"unused"
        });
        assert!(CodexRoute::from_profile(&json!({"route_binding":route})).is_err());
        assert!(CodexRoute::from_profile(&json!({"route_binding":null})).is_err());

        let builtin = match named(None) {
            CodexRoute::Responses(binding) => binding,
            CodexRoute::Direct => unreachable!(),
        };
        for name in [
            "openai",
            "OPENAI",
            "amazon-bedrock",
            "amazon-bedrock-runtime",
            "ollama",
            "lmstudio",
            "ollama-chat",
        ] {
            let mut candidate = builtin.clone();
            candidate.name = name.to_owned();
            assert!(CodexRoute::from_profile(&json!({"route_binding":candidate})).is_err());
        }

        let mut dotted = match named(None) {
            CodexRoute::Responses(binding) => binding,
            CodexRoute::Direct => unreachable!(),
        };
        dotted.name = "corp.gateway".to_owned();
        assert!(CodexRoute::from_profile(&json!({"route_binding":dotted})).is_err());

        let reserved_selector = if cfg!(windows) {
            "codex_home"
        } else {
            "CODEX_HOME"
        };
        let mut binding = match named(None) {
            CodexRoute::Responses(binding) => binding,
            CodexRoute::Direct => unreachable!(),
        };
        for name in ["ELIOT_AGENT_TOKEN", reserved_selector] {
            let mut binding = binding.clone();
            binding.client_key_env_name = Some(name.to_owned());
            assert!(CodexRoute::from_profile(&json!({"route_binding":binding})).is_err());
        }

        if !cfg!(windows) {
            binding.client_key_env_name = Some("codex_home".to_owned());
            assert!(CodexRoute::from_profile(&json!({"route_binding":binding})).is_ok());
        }
    }
}
