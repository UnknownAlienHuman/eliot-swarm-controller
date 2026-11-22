use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

use eliot_core::api::native_profile::ANTIGRAVITY_MODES;
use eliot_core::{Error, Result, SecretPolicy};
use eliot_plugin::{
    AgentStartParams,
    native_cli::{WORKER_SWARM_MCP_SERVER_NAME, require_agent_mcp_root, worker_swarm_mcp_args},
    native_transport::NativeLaunch,
    session::{
        TargetEnvironment, optional_profile_text, parse_resume_subject, target_environment,
        validate_source_plan,
    },
};
use serde_json::{Value, json};

use crate::{
    events::FAMILIES,
    native::{InitializeResult, SessionResult},
    session::without_verbatim_prefix,
};

const ISOLATED_GEMINI_SCOPE: &str = ":isolated-gemini:";

pub(crate) struct Plan {
    pub launch: NativeLaunch,
    pub policy: SecretPolicy,
    pub model: Option<String>,
    /// Native realignment target: the inherited overlay's effective base
    /// model and thought level on a resume that carries them, else the
    /// frozen profile translation. `requested()` stays frozen; these drive
    /// the ACP writes, the readback, and the Session's tracked selectors.
    effective_base: String,
    effective_level: Option<String>,
    pub scope: String,
    pub resume: Option<String>,
    pub hook_discovery: PathBuf,
    pub swarm_mcp: SwarmMcp,
    pub mcp_policy: eliot_core::McpPolicy,
    pub mcp_allow: Vec<String>,
    pub mcp_root: Option<PathBuf>,
    mode: Option<String>,
    skip_permissions: bool,
}

/// Agent-scoped swarm MCP server offered to one Antigravity session through
/// the ACP `session/new` / `session/load` `mcpServers` entries (official ACP
/// v1 schema `schema/v1/schema.json`: stdio entries carry name, command,
/// args and env, all required, and every Agent MUST support stdio).
/// The command is the current Eliot executable with the Agent-bound data
/// root; `--require-agent` enforces the scoped Agent identity and never
/// falls back to Owner auth. The ACP stdio entry carries that identity in its
/// per-session `env` list, so shell tools do not inherit it.
pub(crate) struct SwarmMcp {
    name: String,
    command: PathBuf,
    data_dir: PathBuf,
    environment: Vec<(OsString, OsString)>,
}

impl SwarmMcp {
    fn require(
        environment: &[(OsString, OsString)],
        agent_id: &str,
        generation: i64,
    ) -> Result<Self> {
        let data_dir = require_agent_mcp_root(
            environment,
            agent_id,
            generation,
            "Antigravity MCP requires the matching inherited Agent generation identity",
            "Antigravity MCP requires the Agent-bound Eliot data root",
        )?;
        let command = std::env::current_exe().map_err(|_| {
            Error::new(
                "platform_io",
                "could not locate the current Eliot executable for Antigravity MCP",
            )
        })?;
        if !command.is_absolute() || !command.is_file() {
            return Err(Error::new(
                "platform_io",
                "the current Eliot executable for Antigravity MCP is unavailable",
            ));
        }
        Ok(Self {
            name: WORKER_SWARM_MCP_SERVER_NAME.to_owned(),
            command,
            data_dir,
            environment: environment.to_vec(),
        })
    }

    /// The exact ACP `McpServer` stdio entry for `session/new` / `session/load`.
    fn entry(&self) -> Result<Value> {
        let command = self
            .command
            .to_str()
            .ok_or_else(|| Error::new("invalid", "Antigravity MCP executable path is not UTF-8"))?;
        let data_dir = self
            .data_dir
            .to_str()
            .ok_or_else(|| Error::new("invalid", "Antigravity MCP data-root path is not UTF-8"))?;
        Ok(json!({
            "name": self.name,
            "command": command,
            "args": worker_swarm_mcp_args(data_dir),
            "env": self.environment.iter().map(|(name, value)| json!({
                "name": name.to_string_lossy(),
                "value": value.to_string_lossy(),
            })).collect::<Vec<_>>(),
        }))
    }
}

#[cfg(windows)]
pub(crate) fn prepend_system32_to_path(environment: &mut Vec<(OsString, OsString)>) -> Result<()> {
    let system_root = std::env::var_os("SystemRoot")
        .or_else(|| std::env::var_os("WINDIR"))
        .ok_or_else(|| {
            Error::new(
                "unavailable",
                "Antigravity Windows system root is unavailable",
            )
        })?;
    let system32 = PathBuf::from(system_root).join("System32");
    if !system32.is_absolute() || !system32.is_dir() {
        return Err(Error::new(
            "unavailable",
            "Antigravity Windows System32 directory is unavailable",
        ));
    }

    let path_index = environment
        .iter()
        .position(|(name, _)| name.to_string_lossy().eq_ignore_ascii_case("PATH"));
    let existing_path = match path_index {
        Some(index) => Some(environment[index].1.clone()),
        None => std::env::var_os("PATH"),
    };
    let mut path_entries = vec![system32];
    if let Some(existing_path) = existing_path.filter(|path| !path.is_empty()) {
        path_entries.extend(std::env::split_paths(&existing_path));
    }
    let path = std::env::join_paths(path_entries)
        .map_err(|_| Error::new("invalid", "Antigravity Windows child PATH is invalid"))?;
    if let Some(index) = path_index {
        environment[index].1 = path;
    } else {
        environment.push((OsString::from("PATH"), path));
    }
    Ok(())
}

impl Plan {
    pub fn new(
        params: &AgentStartParams,
        credential: &str,
        hook_token: &str,
        hook_discovery_dir: &Path,
    ) -> Result<Self> {
        if params.initial.is_some() {
            return Err(Error::new(
                "unsupported",
                "Antigravity starts before input; submit a separate send",
            ));
        }
        validate_source_plan("Antigravity", FAMILIES, params.source_plan.as_ref())?;
        let profile = params
            .profile
            .as_ref()
            .and_then(Value::as_object)
            .ok_or_else(|| Error::new("profile_error", "Antigravity requires a frozen profile"))?;
        if profile.get("harness").and_then(Value::as_str) != Some("antigravity") {
            return Err(Error::new(
                "profile_error",
                "Antigravity received another harness profile",
            ));
        }
        if profile.get("effort").is_some_and(|value| !value.is_null()) {
            return Err(Error::new(
                "unsupported",
                "Antigravity effort is unconnected on this route",
            ));
        }
        let model = optional_profile_text(profile, "model", "Antigravity")?;
        let mcp_policy = eliot_core::McpPolicy::parse_snapshot(profile)?;
        let mcp_allow = eliot_core::parse_snapshot_allow_list(profile)?;
        let program = optional_profile_text(profile, "cmd", "Antigravity")?.ok_or_else(|| {
            Error::new(
                "profile_error",
                "Antigravity requires an explicit executable",
            )
        })?;
        let cwd = params
            .workspace
            .as_deref()
            .map(PathBuf::from)
            .filter(|path| path.is_dir())
            .ok_or_else(|| {
                Error::new(
                    "profile_error",
                    "Antigravity requires an existing workspace",
                )
            })?;
        let mode = optional_profile_text(profile, "mode", "Antigravity")?;
        if mode
            .as_deref()
            .is_some_and(|mode| !ANTIGRAVITY_MODES.contains(&mode))
        {
            return Err(Error::new(
                "profile_error",
                "Antigravity mode must be plan, auto, or accept-edits",
            ));
        }
        let skip_permissions = match profile.get("dangerously_skip_permissions") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(value)) => *value,
            _ => {
                return Err(Error::new(
                    "profile_error",
                    "Antigravity dangerously_skip_permissions must be boolean",
                ));
            }
        };
        let environment = target_environment(
            params.target_environment.as_ref(),
            profile.get("pass_env"),
            "Antigravity",
        )?;
        #[cfg(windows)]
        let environment = {
            let mut environment = environment;
            prepend_system32_to_path(&mut environment.variables)?;
            environment
        };
        let mcp_environment = eliot_plugin::session::agent_mcp_environment(
            params.mcp_environment.as_ref(),
            params.target_environment.as_ref(),
            params.pass_agent_identity_to_shell,
            &params.agent_id,
            params.generation,
            "Antigravity",
        )?;
        let swarm_mcp = SwarmMcp::require(
            &mcp_environment.variables,
            &params.agent_id,
            params.generation,
        )?;
        let mut secrets = environment.secrets;
        secrets.extend(mcp_environment.secrets.iter().cloned());
        secrets.push(credential.to_owned());
        secrets.push(hook_token.to_owned());
        let resume = params
            .resume_from
            .as_ref()
            .map(|resume| {
                parse_resume_subject(
                    resume,
                    params.source_state.as_ref(),
                    &params.agent_id,
                    params.generation,
                    "Antigravity",
                )
            })
            .transpose()?;
        let mut scope = config_scope(&environment.variables)?;
        let mcp_root = if mcp_policy == eliot_core::McpPolicy::Minimal {
            if !params
                .agent_id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
            {
                return Err(Error::new(
                    "invalid",
                    "Antigravity Agent id cannot name a config directory",
                ));
            }
            let parent = hook_discovery_dir
                .parent()
                .ok_or_else(|| Error::new("invalid", "Antigravity config parent is unavailable"))?;
            Some(isolated_gemini_root(
                parent,
                &params.agent_id,
                params.generation,
                &scope,
                resume.as_ref().map(|subject| subject.native_scope.as_str()),
            )?)
        } else {
            None
        };
        if let Some(root) = &mcp_root {
            let root = root.to_string_lossy();
            scope.push_str(&format!(
                "{ISOLATED_GEMINI_SCOPE}{}",
                if cfg!(windows) {
                    root.to_lowercase()
                } else {
                    root.into_owned()
                }
            ));
        }
        if resume
            .as_ref()
            .is_some_and(|subject| subject.native_scope != scope)
        {
            return Err(Error::new(
                "profile_error",
                "Antigravity resume scope differs from the selected config namespace",
            ));
        }
        let resume = resume.map(|subject| subject.native_ref);
        if !hook_discovery_dir.is_absolute() || !hook_discovery_dir.is_dir() {
            return Err(Error::new(
                "invalid",
                "Antigravity hook discovery directory is unavailable",
            ));
        }
        let mut args = native_args(
            model.as_deref().unwrap_or_default(),
            mode.as_deref(),
            skip_permissions,
            resume.as_deref(),
            hook_discovery_dir,
        );
        if let Some(root) = &mcp_root {
            args.extend([
                OsString::from("--gemini-dir"),
                without_verbatim_prefix(root).into_os_string(),
            ]);
        }
        // The native realignment target: the inherited overlay's effective
        // values on a resume that carries them (the retained session
        // already runs them), else the frozen profile translation. A
        // carried effort names a thought level it must spell exactly;
        // retained data that does not is corruption, never guessed.
        let (effective_base, effective_level) =
            if params.model_override.is_none() && params.effort_override.is_none() {
                model.as_deref().map(translate_model).unwrap_or_default()
            } else {
                let base = params
                    .model_override
                    .as_deref()
                    .map(|model| translate_model(model).0)
                    .unwrap_or_else(|| model.as_deref().map(translate_model).unwrap_or_default().0);
                let level = params.effort_override.clone().or_else(|| {
                    params
                        .model_override
                        .as_deref()
                        .and_then(|model| translate_model(model).1)
                });
                if level
                    .as_deref()
                    .is_some_and(|level| !matches!(level, "low" | "medium" | "high" | "xhigh"))
                {
                    return Err(Error::new(
                        "source_integrity",
                        "Antigravity retained configure effort is not a thought level",
                    ));
                }
                if base.trim().is_empty() {
                    return Err(Error::new(
                        "source_integrity",
                        "Antigravity retained configure model is empty",
                    ));
                }
                (base, level)
            };
        Ok(Self {
            launch: NativeLaunch {
                program: program.into(),
                args,
                cwd,
                environment: environment.variables,
            },
            policy: SecretPolicy::new(secrets),
            model,
            effective_base,
            effective_level,
            scope,
            resume,
            hook_discovery: hook_discovery_dir.to_owned(),
            swarm_mcp,
            mcp_policy,
            mcp_allow,
            mcp_root,
            mode,
            skip_permissions,
        })
    }

    /// Verify the ACP `initialize` + `session/new|load` facts against the
    /// frozen plan: protocol 1, a non-empty session id (the exact retained
    /// id on resume), and the model/thought-level selectors the translation
    /// below depends on. Permissions and cwd are launch facts (the ACP
    /// server is spawned in the workspace with the profile environment);
    /// the session object itself echoes neither, so cwd is proven by the
    /// launch, not by a response field.
    pub fn check_session(&self, init: &InitializeResult, session: &SessionResult) -> Result<Value> {
        if init.protocol_version != 1
            || session.session_id.trim().is_empty()
            || self
                .resume
                .as_ref()
                .is_some_and(|expected| expected != &session.session_id)
        {
            return Err(Error::new(
                "unknown",
                "Antigravity ACP session did not prove the requested conversation",
            ));
        }
        if self.resume.is_some()
            && init.agent_capabilities.get("loadSession") != Some(&Value::Bool(true))
        {
            return Err(Error::new(
                "unknown",
                "Antigravity ACP agent does not advertise session load",
            ));
        }
        let options = session.config_options.as_ref().ok_or_else(|| {
            Error::new(
                "unknown",
                "Antigravity ACP session advertised no config options",
            )
        })?;
        for (required, category) in [("model", "model"), ("thought_level", "thought_level")] {
            let Some(option) = options.iter().find(|option| option.id == required) else {
                return Err(Error::new(
                    "unknown",
                    "Antigravity ACP session did not prove the requested model selectors",
                ));
            };
            if option.category.as_deref() != Some(category) || option.kind != "select" {
                return Err(Error::new(
                    "unknown",
                    "Antigravity ACP model selector did not match its advertised category",
                ));
            }
        }
        // Every required write value must be an advertised option value:
        // an unadvertised id is rejected here instead of sent and guessed.
        for (config_id, value) in self.config_writes() {
            let advertised = options
                .iter()
                .find(|option| option.id == config_id)
                .is_some_and(|option| {
                    option
                        .option_values()
                        .iter()
                        .any(|(advertised, _)| advertised == &value)
                });
            if !advertised {
                return Err(
                    eliot_plugin::start_classification::harness_unavailable_model(
                        "antigravity",
                        "session",
                        "Antigravity ACP session did not advertise a required config value",
                    ),
                );
            }
        }
        // The reported effective selectors are the realignment target
        // (overlay or frozen); `requested_model` stays the frozen profile
        // id so the original request is never rewritten.
        let (base_model, thought_level) = if self.effective_base.is_empty() {
            // ACP session-open currentValue is the native default. Require a
            // nonblank, advertised selector instead of choosing a catalogue entry.
            let current = |id: &str| -> Result<String> {
                let option = options
                    .iter()
                    .find(|option| option.id == id)
                    .ok_or_else(|| {
                        Error::new("unknown", "Antigravity native default selector is missing")
                    })?;
                let value = option
                    .current_value_str()
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        Error::new("unknown", "Antigravity native default selector is empty")
                    })?;
                if !option
                    .option_values()
                    .iter()
                    .any(|(advertised, _)| advertised == value)
                {
                    return Err(Error::new(
                        "unknown",
                        "Antigravity native default selector is not advertised",
                    ));
                }
                Ok(value.to_owned())
            };
            (current("model")?, Some(current("thought_level")?))
        } else {
            (self.effective_base.clone(), self.effective_level.clone())
        };
        // ACP exposes no MCP inventory on any checked surface (the
        // `session/new` / `session/load` results and `session/update`
        // stream carry no server list), so the effective server set stays
        // unknown (null, unverified) at readiness instead of echoing the
        // requested entry. Proof that the Agent can reach swarm is
        // behavioral: its tool calls through the session.
        Ok(json!({
            "model": base_model,
            "requested_model": self.model,
            "thought_level": thought_level,
            "permission_mode": self.skip_permissions,
            "cwd": self.launch.cwd,
            "native_roots": self.native_roots(),
            "session_id": session.session_id,
            "mode": session.modes.as_ref().map(|modes| modes.current_mode_id.clone()),
            "mcp_servers": null,
            "mcp_servers_verified": false,
            "mcp": self.mcp_policy.as_str(),
            "mcp_allow": self.mcp_allow,
            "mcp_config_root": self.mcp_root,
        }))
    }

    /// The `session/set_config_option` pairs this plan requires, in wire
    /// order: base model first, then thought level when the realignment
    /// target carries one (an explicit effort, a model suffix, or the
    /// inherited overlay level).
    pub fn config_writes(&self) -> Vec<(String, String)> {
        if self.effective_base.is_empty() {
            return Vec::new();
        }
        let mut writes = vec![("model".to_owned(), self.effective_base.clone())];
        if let Some(level) = self.effective_level.clone() {
            writes.push(("thought_level".to_owned(), level));
        }
        writes
    }

    /// The native realignment target: the inherited overlay's effective
    /// base model and thought level on a resume that carries them, else
    /// the frozen profile translation.
    pub fn effective_target(&self) -> (String, Option<String>) {
        (self.effective_base.clone(), self.effective_level.clone())
    }

    pub fn resume(&self) -> Option<&str> {
        self.resume.as_deref()
    }

    /// The exact ACP `McpServer` stdio entry for this generation's
    /// `session/new` / `session/load` call.
    pub fn mcp_server_entry(&self) -> Result<Value> {
        self.swarm_mcp.entry()
    }

    pub fn mode(&self) -> Option<&str> {
        self.mode.as_deref()
    }

    pub fn requested(&self) -> Value {
        json!({"model": self.model, "cwd": self.launch.cwd, "mode": self.mode,
            "dangerously_skip_permissions": self.skip_permissions, "transport": "acp",
            "mcp": self.mcp_policy.as_str(), "mcp_allow": self.mcp_allow,
            "mcp_config_root": self.mcp_root,
            "native_roots": self.native_roots()})
    }

    /// Effective native roots with their purpose: the real workspace (the
    /// ACP session cwd) versus the hook-discovery root (the only `--add-dir`
    /// directory, holding nothing but `.agents/hooks.json`).
    fn native_roots(&self) -> Value {
        json!({"workspace": self.launch.cwd, "hook_discovery": self.hook_discovery})
    }
}

/// Bind the actual inherited/overridden config namespace. This names the
/// canonical environment selectors, not an invented native database root.
fn config_scope(environment: &[(OsString, OsString)]) -> Result<String> {
    let mut paths = std::collections::BTreeMap::new();
    for name in [
        "HOME",
        "USERPROFILE",
        "APPDATA",
        "LOCALAPPDATA",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
    ] {
        let selected = environment
            .iter()
            .find(|(key, _)| {
                if cfg!(windows) {
                    key.to_string_lossy().eq_ignore_ascii_case(name)
                } else {
                    key.as_os_str() == std::ffi::OsStr::new(name)
                }
            })
            .map(|(_, value)| value.clone())
            .or_else(|| std::env::var_os(name));
        if let Some(selected) = selected.filter(|value| !value.is_empty()) {
            let path = PathBuf::from(selected);
            let path = std::fs::canonicalize(&path).unwrap_or(path);
            let text = path.to_string_lossy().into_owned();
            paths.insert(
                name,
                if cfg!(windows) {
                    text.to_lowercase()
                } else {
                    text
                },
            );
        }
    }
    if paths.is_empty() {
        return Err(Error::new(
            "profile_error",
            "Antigravity config namespace is unavailable",
        ));
    }
    serde_json::to_string(&paths)
        .map(|paths| format!("antigravity-config:{paths}"))
        .map_err(|_| Error::new("internal", "Could not encode Antigravity config namespace"))
}

/// Fresh generations own new roots. Resume selects only an earlier root for
/// this Agent under the current private parent. Plan::new then verifies the
/// full scope, including the environment and private parent.
fn isolated_gemini_root(
    parent: &Path,
    agent: &str,
    generation: i64,
    base_scope: &str,
    retained_scope: Option<&str>,
) -> Result<PathBuf> {
    let Some(retained_scope) = retained_scope else {
        return Ok(parent.join(format!("{agent}-g{generation}-agy")));
    };
    let mismatch = || {
        Error::new(
            "profile_error",
            "Antigravity resume scope differs from the selected config namespace",
        )
    };
    let root = retained_scope
        .strip_prefix(base_scope)
        .and_then(|scope| scope.strip_prefix(ISOLATED_GEMINI_SCOPE))
        .ok_or_else(mismatch)?;
    let name = Path::new(root)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(mismatch)?;
    let scope_agent = if cfg!(windows) {
        agent.to_ascii_lowercase()
    } else {
        agent.to_owned()
    };
    let suffix = name
        .strip_prefix(&scope_agent)
        .and_then(|name| name.strip_suffix("-agy"))
        .ok_or_else(mismatch)?;
    if suffix.is_empty() {
        // Legacy isolated sessions used the Agent ID without a generation.
        return Ok(parent.join(format!("{agent}-agy")));
    }
    let origin = suffix
        .strip_prefix("-g")
        .and_then(|number| number.parse::<i64>().ok())
        .filter(|origin| (1..generation).contains(origin))
        .ok_or_else(mismatch)?;
    Ok(parent.join(format!("{agent}-g{origin}-agy")))
}

/// A resolved `configure` target: the effective native selectors the
/// session must prove (the explicit `--effort` wins over a model suffix;
/// either falls back to the tracked session level, and the base falls
/// back to the tracked session base for an effort-only change). The
/// effective model id recombines base and level so the overlay vocabulary
/// matches the frozen profile ids. The verbatim request stays in the
/// retained command args; the Router records it from there.
#[derive(Debug)]
pub(crate) struct ConfigureTarget {
    pub base: String,
    pub level: Option<String>,
    pub effective_model: String,
}

/// Resolve one `configure` request against the session's tracked
/// selectors. Pure and natively ignorant: the caller proves the resolved
/// values against the advertised options, writes them, and verifies the
/// echo before the Router stores anything.
pub(crate) fn resolve_configure_target(
    args: &Value,
    current_base: &str,
    current_level: Option<&str>,
) -> Result<ConfigureTarget> {
    let args = args.as_object().ok_or_else(|| {
        Error::new(
            "invalid",
            "Antigravity configure args must be a JSON object",
        )
    })?;
    for key in args.keys() {
        if !matches!(key.as_str(), "model" | "effort") {
            return Err(Error::new(
                "unsupported",
                format!("configure does not support {key}"),
            ));
        }
    }
    let requested_model = match args.get("model") {
        None | Some(Value::Null) => None,
        Some(Value::String(model)) if !model.trim().is_empty() => Some(model.clone()),
        _ => {
            return Err(Error::new(
                "invalid",
                "Antigravity configure requires a non-empty model",
            ));
        }
    };
    let requested_effort = match args.get("effort") {
        None | Some(Value::Null) => None,
        Some(Value::String(effort))
            if matches!(effort.as_str(), "low" | "medium" | "high" | "xhigh") =>
        {
            Some(effort.clone())
        }
        Some(Value::String(_)) => {
            return Err(Error::new(
                "invalid",
                "Antigravity configure effort must be low, medium, high, or xhigh",
            ));
        }
        _ => {
            return Err(Error::new(
                "invalid",
                "Antigravity configure effort must be a string",
            ));
        }
    };
    if requested_model.is_none() && requested_effort.is_none() {
        return Err(Error::new(
            "invalid",
            "Antigravity configure requires model and/or effort",
        ));
    }
    let (base, suffix_level) = match requested_model.as_deref() {
        Some(model) => translate_model(model),
        None => (current_base.to_owned(), None),
    };
    if base.trim().is_empty() {
        return Err(Error::new(
            "invalid",
            "Antigravity configure requires a non-empty model",
        ));
    }
    let level = requested_effort
        .clone()
        .or(suffix_level)
        .or_else(|| current_level.map(ToOwned::to_owned));
    let effective_model = match level.as_deref() {
        Some(level) => format!("{base}-{level}"),
        None => base.clone(),
    };
    Ok(ConfigureTarget {
        base,
        level,
        effective_model,
    })
}

/// Translate a requested CLI model id into the ACP session vocabulary.
/// The ACP `model` selector carries base family ids only (observed:
/// `gemini-3.8-flash`, `gemini-4-argon`, …); the effort suffix lives in
/// the separate `thought_level` selector (`low|medium|high|xhigh`, the
/// documented `agy models` suffixes of agy 1.5.9). Only those four suffixes
/// translate; any other suffix stays part of the base id and will be
/// rejected by the server instead of guessed at.
pub(crate) fn translate_model(model: &str) -> (String, Option<String>) {
    for level in ["low", "medium", "high", "xhigh"] {
        if let Some(base) = model.strip_suffix(&format!("-{level}"))
            && !base.is_empty()
        {
            return (base.to_owned(), Some(level.to_owned()));
        }
    }
    (model.to_owned(), None)
}

/// Build the child environment for the read-only `model.list` catalogue
/// probe: the same construction as the launch path (`target_environment`
/// over the frozen `pass_env` names plus the Windows `System32` prepend),
/// resolved from the plugin's own minimal process environment. Agent
/// identity is never added here: discovery carries no Agent credentials, and
/// frozen `pass_env` names in Eliot's reserved namespace are refused rather
/// than leaked into the child.
pub(crate) fn reader_environment(snapshot: &Value) -> Result<TargetEnvironment> {
    let mut object = serde_json::Map::new();
    if let Some(names) = snapshot.get("pass_env").and_then(Value::as_array) {
        for name in names.iter().filter_map(Value::as_str) {
            if name
                .get(..6)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("ELIOT_"))
            {
                return Err(Error::new(
                    "not_allowed",
                    "discovery cannot receive reserved ELIOT_* environment names",
                ));
            }
            if let Some(value) = std::env::var_os(name) {
                let value = value.into_string().map_err(|_| {
                    Error::new(
                        "invalid",
                        "Antigravity model.list environment is not valid Unicode",
                    )
                })?;
                object.insert(name.to_owned(), Value::String(value));
            }
        }
    }
    #[cfg_attr(not(windows), allow(unused_mut))]
    let mut environment = target_environment(
        Some(&Value::Object(object)),
        snapshot.get("pass_env"),
        "Antigravity",
    )?;
    #[cfg(windows)]
    prepend_system32_to_path(&mut environment.variables)?;
    Ok(environment)
}

fn native_args(
    _model: &str,
    _mode: Option<&str>,
    skip_permissions: bool,
    _resume: Option<&str>,
    hook_discovery_dir: &Path,
) -> Vec<OsString> {
    // One ACP stdio server per generation. Model, thought level, session
    // mode and resume are ACP session facts now (set_config_option /
    // set_mode / session load), not CLI flags. Permission posture and the
    // hook-discovery directory are process launch flags. The only
    // `--add-dir` root is the dedicated hook-only directory: it holds
    // nothing but `.agents/hooks.json`, so the private instance directory
    // (tokens, logs, state, sockets) is never a writable native work root.
    let mut args = [
        OsString::from("acp"),
        OsString::from("--stdio"),
        format!("--dangerously-skip-permissions={skip_permissions}").into(),
    ]
    .to_vec();
    args.push(OsString::from("--add-dir"));
    args.push(hook_discovery_dir.as_os_str().to_owned());
    args
}
