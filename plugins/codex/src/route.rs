//! Frozen per-process Codex provider routing.
//!
//! Route credentials are referenced by environment-variable name and captured
//! only into the runtime redaction policies.
use crate::native::NativeClient;
use eliot_core::{Error, Result, RouteBinding, SecretPolicy};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    ffi::{OsStr, OsString},
    io::{self, Read},
    path::Path,
};
use tokio::time::{Instant, timeout_at};

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
            .validate_for("codex")
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
                let model_catalog_url = models_catalog_url(&binding.provider_base_url);
                let mut provider = format!(
                    "{{ name = {}, base_url = {}, model_catalog_url = {}, wire_api = \"responses\", requires_openai_auth = false",
                    quote(&binding.name)?,
                    quote(&binding.provider_base_url)?,
                    quote(&model_catalog_url)?,
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

    #[allow(
        clippy::too_many_arguments,
        reason = "The operation's distinct inputs remain explicit at its established call boundary."
    )]
    pub(super) fn args_with_agent_mcp(
        &self,
        server_name: &str,
        swarm_executable: &Path,
        data_dir: &Path,
        cwd: &Path,
        agent_mcp_url: Option<&str>,
        agent_id: &str,
        generation: i64,
        mcp_disables: &[crate::mcp::EffectiveMcpServer],
        plugin_disables: &[String],
        native_subagents: Option<&str>,
        provider_probe: bool,
        project_doc_max_bytes: Option<u64>,
    ) -> Result<Vec<OsString>> {
        if !swarm_executable.is_absolute() || !data_dir.is_absolute() || !cwd.is_absolute() {
            return Err(route_error(
                "Codex Agent MCP command, data directory, and cwd must be absolute",
            ));
        }
        if server_name.is_empty()
            || server_name.chars().any(char::is_control)
            || mcp_disables.iter().any(|server| {
                !crate::mcp::is_addressable_override_name(&server.name)
                    || [server.command.as_deref(), server.url.as_deref()]
                        .iter()
                        .any(|transport| {
                            transport.is_some_and(|value| {
                                value.is_empty() || value.chars().any(char::is_control)
                            })
                        })
                    || (server.command.is_none() && server.url.is_none())
            })
            || plugin_disables
                .iter()
                .any(|id| !crate::mcp::is_suppressible_plugin_id(id))
        {
            return Err(route_error(
                "Codex Agent MCP server names must be plain override segments",
            ));
        }
        let args = self.args()?;
        let app_server_index = args
            .iter()
            .position(|arg| arg == OsStr::new("app-server"))
            .ok_or_else(|| route_error("Codex app-server route is unavailable"))?;
        // This generated tool authenticates only as the Agent (--require-agent).
        // Store admission and dispatch reserve ask-generation replies to the
        // Owner, including self and manager replies; auto-approving this MCP
        // transport therefore cannot approve the Agent's pending requests.
        let mut mcp_override = Vec::new();
        mcp_override.extend(native_subagents_argv(native_subagents));
        if let Some(limit) = project_doc_max_bytes {
            push_override(
                &mut mcp_override,
                "project_doc_max_bytes",
                limit.to_string(),
            );
        }
        if provider_probe {
            for (key, value) in [
                ("features.shell_tool", "false"),
                ("features.skill_mcp_dependency_install", "false"),
                ("web_search", "\"disabled\""),
            ] {
                push_override(&mut mcp_override, key, value.to_owned());
            }
        } else {
            let scope_dir = quote_path(data_dir)?;
            let server = if let Some(url) = agent_mcp_url {
                format!(
                    "{{ enabled = true, url = {}, bearer_token_env_var = \"ELIOT_AGENT_TOKEN\", http_headers = {{ \"x-eliot-agent-id\" = {}, \"x-eliot-agent-generation\" = {} }}, tools = {{ swarm = {{ approval_mode = \"approve\" }} }} }}",
                    quote(url)?,
                    quote(agent_id)?,
                    quote(&generation.to_string())?,
                )
            } else {
                format!(
                    "{{ enabled = true, command = {}, args = [\"mcp\", \"--require-agent\", \"--data-dir\", {scope_dir}], env = {{ ELIOT_AGENT_DATA_DIR = {scope_dir} }}, env_vars = [\"ELIOT_AGENT_ID\", \"ELIOT_AGENT_GENERATION\", \"ELIOT_AGENT_TOKEN\"], cwd = {}, tools = {{ swarm = {{ approval_mode = \"approve\" }} }} }}",
                    quote_path(swarm_executable)?,
                    quote_path(cwd)?,
                )
            };
            push_override(
                &mut mcp_override,
                &format!("mcp_servers.{server_name}"),
                server,
            );
        }
        // Minimal policy: suppress every inherited server explicitly. A bare
        // `mcp_servers={}` override merges with file config instead of
        // replacing it (verified on 0.167.1), so each server needs its own
        // overrides; unaddressable names fail before spawn. Each disable
        // re-emits the server's effective `command`/`url` because the native
        // `-c` bootstrap validates a touched `mcp_servers.<name>` table on
        // its own: a bare `enabled=false` breaks startup for names whose
        // transport lives in a project/plugin layer or builtin (verified on
        // 0.167.1: `invalid transport in 'mcp_servers.<name>'`). The
        // transport never starts (the server stays disabled); remaining keys
        // merge from the owner layers. Plugin-backed builtin servers never
        // appear in `mcp_servers`, so each suppressed plugin needs its own
        // `plugins.<id>.enabled=false` override instead.
        for id in plugin_disables {
            push_override(
                &mut mcp_override,
                &format!("plugins.{id}.enabled"),
                "false".to_owned(),
            );
        }
        for server in mcp_disables {
            if let Some(command) = &server.command {
                push_override(
                    &mut mcp_override,
                    &format!("mcp_servers.{}.command", server.name),
                    quote(command)?,
                );
            }
            if let Some(url) = &server.url {
                push_override(
                    &mut mcp_override,
                    &format!("mcp_servers.{}.url", server.name),
                    quote(url)?,
                );
            }
            push_override(
                &mut mcp_override,
                &format!("mcp_servers.{}.enabled", server.name),
                "false".to_owned(),
            );
        }
        let mut result = Vec::with_capacity(args.len() + mcp_override.len());
        result.extend(args[..app_server_index].iter().cloned());
        result.extend(mcp_override);
        result.extend(args[app_server_index..].iter().cloned());
        Ok(result)
    }

    /// Capture the configured credential even when its name does not match
    /// the ordinary secret-name heuristic. Values never enter argv or config.
    pub(super) fn capture_client_secret(
        &self,
        variables: &[(OsString, OsString)],
        diagnostic_secrets: &mut Vec<String>,
        metadata_secrets: &mut Vec<String>,
        required: bool,
    ) -> Result<()> {
        let Self::Responses(binding) = self else {
            return Ok(());
        };
        let Some(name) = &binding.client_key_env_name else {
            return Ok(());
        };
        let matches: Vec<_> = variables
            .iter()
            .filter(|(key, _)| env_name_matches(key, name))
            .collect();
        let mut captured = 0;
        for (_, value) in &matches {
            if let Some(value) = value.to_str().filter(|value| !value.is_empty()) {
                diagnostic_secrets.push(value.to_owned());
                metadata_secrets.push(value.to_owned());
                captured += 1;
            }
        }
        if required && (matches.len() != 1 || captured != 1) {
            return Err(route_error(
                "configured proxy client credential is unavailable, empty, or ambiguous",
            ));
        }
        Ok(())
    }

    /// Remove only the configured client-key variable from metadata readers.
    /// The caller captures it into both secret policies before calling this.
    pub(super) fn strip_client_key(&self, variables: &mut Vec<(OsString, OsString)>) {
        let Self::Responses(binding) = self else {
            return;
        };
        let Some(name) = &binding.client_key_env_name else {
            return;
        };
        variables.retain(|(key, _)| !env_name_matches(key, name));
    }

    /// Verify the effective route in the cwd the native operation will use.
    /// No thread or input is dispatched before this check.
    pub(super) async fn verify_effective_config(
        &self,
        client: &NativeClient,
        cwd: &Path,
        metadata_secrets: &[String],
        expected_catalogue: Option<&str>,
        resuming: bool,
        deadline: Instant,
    ) -> Result<bool> {
        let response = client
            .call(
                "config/read",
                json!({"cwd": cwd.to_string_lossy(), "includeLayers": false}),
                deadline,
            )
            .await
            .map_err(|error| {
                error
                    .with_code("profile_error")
                    .with_message("Codex config/read could not verify the per-process route")
            })?;
        let config = response
            .get("config")
            .ok_or_else(|| route_error("Codex config/read omitted effective config"))?;
        self.verify_config(config, metadata_secrets)?;
        if let Some(old) = expected_catalogue {
            let new = model_catalogue_digest(config, deadline).await?;
            if old != new {
                if resuming {
                    return Ok(true);
                }
                return Err(Error::new(
                    "model_catalogue_changed",
                    format!(
                        "Codex model catalogue digest changed during launch (old {old}, new {new}). Submit a new launch against the current catalogue."
                    ),
                )
                .with_where("plugin.codex.route.model_catalog_json"));
            }
        }
        Ok(false)
    }

    fn verify_config(&self, config: &Value, metadata_secrets: &[String]) -> Result<()> {
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
                let expected_models_url = models_catalog_url(&binding.provider_base_url);
                if provider.get("name").and_then(Value::as_str) != Some(binding.name.as_str())
                    || provider.get("base_url").and_then(Value::as_str)
                        != Some(binding.provider_base_url.as_str())
                    || provider.get("model_catalog_url").and_then(Value::as_str)
                        != Some(expected_models_url.as_str())
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

    /// Carry the profile effort as an option: an omitted effort preserves the
    /// provider/native default by omitting the native wire field entirely.
    /// A present effort is validated for shape only on this route; effort
    /// support is enforced by the provider, not fabricated here.
    pub(super) fn optional_effort(&self, effort: Option<&str>) -> Result<Option<String>> {
        if self.requires_chatgpt() {
            return Err(route_error(
                "direct route effort requires its advertised-model check",
            ));
        }
        match effort {
            None => Ok(None),
            Some(effort) if !effort.trim().is_empty() && !effort.chars().any(char::is_control) => {
                Ok(Some(effort.to_owned()))
            }
            Some(_) => Err(route_error(
                "named Responses route effort must be a non-empty reasoning effort",
            )),
        }
    }

    /// Route argv with the per-session native-subagent mapping applied
    /// before `app-server`: `-c` overrides must precede the subcommand.
    pub(super) fn args_with_native_subagents(
        &self,
        native_subagents: Option<&str>,
    ) -> Result<Vec<OsString>> {
        let args = self.args()?;
        let deny = native_subagents_argv(native_subagents);
        if deny.is_empty() {
            return Ok(args);
        }
        let pos = args
            .iter()
            .position(|arg| arg == OsStr::new("app-server"))
            .unwrap_or(args.len());
        let mut result = Vec::with_capacity(args.len() + deny.len());
        result.extend(args[..pos].iter().cloned());
        result.extend(deny);
        result.extend(args[pos..].iter().cloned());
        Ok(result)
    }
}

fn env_name_matches(actual: &OsStr, expected: &str) -> bool {
    actual.to_str().is_some_and(|actual| {
        if cfg!(windows) {
            actual.eq_ignore_ascii_case(expected)
        } else {
            actual == expected
        }
    })
}

fn push_override(args: &mut Vec<OsString>, key: &str, value: String) {
    args.push(OsString::from("-c"));
    args.push(OsString::from(format!("{key}={value}")));
}

/// Per-session native-subagent mapping for the installed Codex CLI
/// (rust-v0.167.1 core/src/config/mod.rs, multi_agent_version_override).
/// Model metadata can enable native agents despite multi_agent=false, so
/// deny also needs agents.enabled=false and multi_agent_v2=false. The host
/// repeats this mapping in each thread's config overlay. Allow adds nothing.
pub(super) fn native_subagents_argv(native_subagents: Option<&str>) -> Vec<OsString> {
    if native_subagents == Some("allow") {
        return Vec::new();
    }
    let mut args = Vec::with_capacity(6);
    push_override(&mut args, "agents.enabled", "false".to_owned());
    push_override(&mut args, "features.multi_agent", "false".to_owned());
    push_override(&mut args, "features.multi_agent_v2", "false".to_owned());
    args
}

fn models_catalog_url(provider_base_url: &str) -> String {
    format!("{}/models", provider_base_url.trim_end_matches('/'))
}

fn quote(value: &str) -> Result<String> {
    if value.chars().any(char::is_control) {
        return Err(route_error(
            "Codex route contains an invalid control character",
        ));
    }
    serde_json::to_string(value).map_err(|_| route_error("Codex route could not be encoded"))
}

fn quote_path(path: &Path) -> Result<String> {
    let value = path
        .to_str()
        .ok_or_else(|| route_error("Codex Agent MCP path is not valid Unicode"))?;
    quote(value)
}

/// Hash the effective catalogue. The prefix distinguishes the provider
/// default (0) from a configured file's exact bytes (1).
pub(crate) async fn model_catalogue_digest(config: &Value, deadline: Instant) -> Result<String> {
    let config = config
        .as_object()
        .ok_or_else(|| catalogue_read_error("effective config is missing or malformed"))?;
    let Some(selected) = config
        .get("model_catalog_json")
        .filter(|value| !value.is_null())
    else {
        return Ok(format_digest(&Sha256::digest([0])));
    };
    let path = selected
        .as_str()
        .map(Path::new)
        .ok_or_else(|| catalogue_read_error("model_catalog_json must be a path string"))?;
    if !path.is_absolute() {
        return Err(catalogue_read_error(
            "model_catalog_json is relative; configure an absolute file path",
        ));
    }
    let path = path.to_path_buf();
    timeout_at(
        deadline,
        tokio::task::spawn_blocking(move || -> Result<String> {
            let metadata = std::fs::metadata(&path).map_err(catalogue_io_error)?;
            if metadata.is_dir() {
                return Err(catalogue_read_error(
                    "model_catalog_json points to a directory; configure a file",
                ));
            }
            if !metadata.is_file() {
                return Err(catalogue_read_error(
                    "model_catalog_json does not point to a regular file",
                ));
            }
            let mut file = std::fs::File::open(path).map_err(catalogue_io_error)?;
            let mut digest = Sha256::new();
            digest.update([1]);
            let mut bytes = [0_u8; 16 * 1024];
            loop {
                if Instant::now() >= deadline {
                    return Err(catalogue_read_error("reading model_catalog_json timed out"));
                }
                let read = file.read(&mut bytes).map_err(catalogue_io_error)?;
                if read == 0 {
                    return Ok(format_digest(&digest.finalize()));
                }
                digest.update(&bytes[..read]);
            }
        }),
    )
    .await
    .map_err(|_| catalogue_read_error("reading model_catalog_json timed out"))?
    .map_err(|_| catalogue_read_error("model_catalog_json reader stopped unexpectedly"))?
}

fn catalogue_io_error(error: io::Error) -> Error {
    catalogue_read_error(match error.kind() {
        io::ErrorKind::NotFound => "the configured model_catalog_json file is missing",
        io::ErrorKind::TimedOut => "reading model_catalog_json timed out",
        _ => "the configured model_catalog_json file is unreadable",
    })
}

fn catalogue_read_error(reason: &str) -> Error {
    Error::new(
        "profile_error",
        format!("Codex model catalogue identity could not be read: {reason}"),
    )
    .with_where("plugin.codex.route.model_catalog_json")
}

fn format_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn route_error(message: &'static str) -> Error {
    Error::new("profile_error", message)
}
