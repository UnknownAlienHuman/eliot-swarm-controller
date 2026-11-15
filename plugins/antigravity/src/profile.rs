use std::{
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
};

use eliot_core::{Error, Result, SecretPolicy};
use eliot_plugin::{
    AgentStartParams,
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
};

pub(crate) struct Plan {
    pub launch: NativeLaunch,
    pub policy: SecretPolicy,
    pub model: String,
    pub scope: String,
    pub resume: Option<String>,
    pub hook_discovery: PathBuf,
    pub swarm_mcp: SwarmMcp,
    mode: Option<String>,
    skip_permissions: bool,
}

/// Agent-scoped swarm MCP server offered to one Antigravity session through
/// the ACP `session/new` / `session/load` `mcpServers` entries (official ACP
/// v1 schema `schema/v1/schema.json`: stdio entries carry name, command,
/// args and env, all required, and every Agent MUST support stdio).
/// The command is the current Eliot executable with the Agent-bound data
/// root; `--require-agent` enforces the inherited Agent identity and never
/// falls back to Owner auth (same server and identity injection as the Codex
/// `route.rs` override and the Claude `mcp_config.rs` file). `env` stays
/// empty by design: the stdio child inherits the ACP server's environment,
/// which already carries the Agent-bound `ELIOT_AGENT_*` identity the daemon
/// injected, so no token crosses the session params.
pub(crate) struct SwarmMcp {
    name: String,
    command: PathBuf,
    data_dir: PathBuf,
}

impl SwarmMcp {
    pub(crate) const SERVER_NAME: &str = "eliot-swarm";

    fn require(
        environment: &[(OsString, OsString)],
        agent_id: &str,
        generation: i64,
    ) -> Result<Self> {
        let expected_generation = generation.to_string();
        let matches_agent = unique_environment_value(environment, "ELIOT_AGENT_ID")
            .and_then(OsStr::to_str)
            == Some(agent_id);
        let matches_generation = unique_environment_value(environment, "ELIOT_AGENT_GENERATION")
            .and_then(OsStr::to_str)
            == Some(expected_generation.as_str());
        let has_token = matches!(
            unique_environment_value(environment, "ELIOT_AGENT_TOKEN"),
            Some(token) if !token.is_empty()
        );
        if agent_id.trim().is_empty()
            || generation <= 0
            || !matches_agent
            || !matches_generation
            || !has_token
        {
            return Err(Error::new(
                "not_allowed",
                "Antigravity MCP requires the matching inherited Agent generation identity",
            ));
        }
        let data_dir = unique_environment_value(environment, "ELIOT_HOME")
            .map(PathBuf::from)
            .filter(|path| path.is_absolute() && path.is_dir())
            .ok_or_else(|| {
                Error::new(
                    "not_allowed",
                    "Antigravity MCP requires the Agent-bound Eliot data root",
                )
            })?;
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
            name: Self::SERVER_NAME.to_owned(),
            command,
            data_dir,
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
            "args": ["--data-dir", data_dir, "mcp", "--require-agent"],
            "env": [],
        }))
    }
}

fn unique_environment_value<'a>(
    environment: &'a [(OsString, OsString)],
    name: &str,
) -> Option<&'a OsStr> {
    let mut matches = environment
        .iter()
        .filter(|(key, _)| key.to_string_lossy().eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_os_str());
    let value = matches.next()?;
    matches.next().is_none().then_some(value)
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
        let model = optional_profile_text(profile, "model", "Antigravity")?
            .ok_or_else(|| Error::new("profile_error", "Antigravity requires an exact model"))?;
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
            .is_some_and(|mode| !matches!(mode, "plan" | "auto" | "accept-edits"))
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
        let swarm_mcp =
            SwarmMcp::require(&environment.variables, &params.agent_id, params.generation)?;
        let mut secrets = environment.secrets;
        secrets.push(credential.to_owned());
        secrets.push(hook_token.to_owned());
        let scope = config_scope(&environment.variables)?;
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
        let args = native_args(
            &model,
            mode.as_deref(),
            skip_permissions,
            resume.as_deref(),
            hook_discovery_dir,
        );
        Ok(Self {
            launch: NativeLaunch {
                program: program.into(),
                args,
                cwd,
                environment: environment.variables,
            },
            policy: SecretPolicy::new(secrets),
            model,
            scope,
            resume,
            hook_discovery: hook_discovery_dir.to_owned(),
            swarm_mcp,
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
                return Err(Error::new(
                    "unknown",
                    "Antigravity ACP session did not advertise a required config value",
                ));
            }
        }
        let (base_model, thought_level) = translate_model(&self.model);
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
        }))
    }

    /// The `session/set_config_option` pairs this plan requires, in wire
    /// order: base model first, then thought level when the requested model
    /// carried an effort suffix.
    pub fn config_writes(&self) -> Vec<(String, String)> {
        let (base_model, thought_level) = translate_model(&self.model);
        let mut writes = vec![("model".to_owned(), base_model)];
        if let Some(level) = thought_level {
            writes.push(("thought_level".to_owned(), level));
        }
        writes
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
/// a frozen `pass_env` naming an `ELIOT_AGENT_` variable is refused rather
/// than leaked into the child.
pub(crate) fn reader_environment(snapshot: &Value) -> Result<TargetEnvironment> {
    let mut object = serde_json::Map::new();
    if let Some(names) = snapshot.get("pass_env").and_then(Value::as_array) {
        for name in names.iter().filter_map(Value::as_str) {
            if name
                .get(..12)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("ELIOT_AGENT_"))
            {
                return Err(Error::new(
                    "not_allowed",
                    "discovery cannot receive Agent identity or tokens",
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn test_dirs(tag: &str) -> (PathBuf, PathBuf, PathBuf) {
        let base = std::env::temp_dir().join(format!("eliot-agy-{tag}-{}", ulid::Ulid::new()));
        let workspace = base.join("workspace");
        let hook_root = base.join("hooks");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&hook_root).unwrap();
        (base, workspace, hook_root)
    }

    fn start_params(workspace: &Path, data_dir: &Path) -> AgentStartParams {
        let mut params = AgentStartParams::new("agent-1", 1, "cmd-1");
        params.workspace = Some(workspace.to_string_lossy().into_owned());
        params.profile = Some(json!({
            "harness": "antigravity",
            "model": "gemini-3.8-flash-high",
            "cmd": "agy",
        }));
        params.target_environment = Some(json!({
            "ELIOT_AGENT_ID": "agent-1",
            "ELIOT_AGENT_GENERATION": "1",
            "ELIOT_AGENT_TOKEN": "test-agent-token",
            "ELIOT_HOME": data_dir.to_string_lossy(),
        }));
        params.source_plan = Some(json!({
            "session": "stream",
            "turn": "stream",
            "message": "stream",
            "tool": "stream",
        }));
        params
    }

    #[test]
    fn translate_model_maps_the_documented_effort_suffixes() {
        // The documented `agy models` suffixes (agy 1.5.9); `-high` already
        // mapped to `thought_level`, so `-xhigh` maps too. Anything else
        // stays a verbatim base id for the server to verify, never guessed.
        for (model, base, level) in [
            ("gemini-4-argon-low", "gemini-4-argon", "low"),
            ("gemini-3.8-flash-medium", "gemini-3.8-flash", "medium"),
            ("gemini-3.8-flash-high", "gemini-3.8-flash", "high"),
            ("gemini-4-hydrogen-xhigh", "gemini-4-hydrogen", "xhigh"),
        ] {
            assert_eq!(
                translate_model(model),
                (base.to_owned(), Some(level.to_owned())),
                "{model}"
            );
        }
        for model in [
            "gemini-pro-orchestrator-g1-pro",
            "gpt-oss-120b-medium-evil",
            "gpt-oss-120b-max",
            "-xhigh",
            "xhigh",
        ] {
            assert_eq!(translate_model(model), (model.to_owned(), None), "{model}");
        }
    }

    #[test]
    fn reader_environment_maps_pass_env_without_agent_identity() {
        let env =
            reader_environment(&json!({"harness": "antigravity", "cmd": "agy", "pass_env": []}))
                .expect("empty pass_env maps");
        assert!(
            env.variables
                .iter()
                .any(|(name, _)| name == "PATH" || name == "Path"),
            "the Windows child keeps a PATH entry (System32 first)"
        );
        let error = match reader_environment(
            &json!({"harness": "antigravity", "cmd": "agy", "pass_env": ["ELIOT_AGENT_TOKEN"]}),
        ) {
            Ok(_) => panic!("Agent identity must not reach discovery"),
            Err(error) => error,
        };
        assert_eq!(error.code, "not_allowed");
    }

    #[test]
    fn add_dir_carries_only_the_hook_discovery_root() {
        let (base, workspace, hook_root) = test_dirs("plan");
        let instance_dir = base.join("instance-1");
        fs::create_dir_all(&instance_dir).unwrap();
        let plan = Plan::new(
            &start_params(&workspace, &base),
            "cred",
            "hook-token",
            &hook_root,
        )
        .unwrap();
        let add_dir = plan
            .launch
            .args
            .windows(2)
            .find(|pair| pair[0] == "--add-dir")
            .map(|pair| pair[1].clone());
        assert_eq!(add_dir, Some(hook_root.as_os_str().to_owned()));
        assert!(
            !plan
                .launch
                .args
                .iter()
                .any(|arg| arg == instance_dir.as_os_str()),
            "private instance directory must never be a native root",
        );
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn requested_and_effective_carry_native_roots() {
        let (base, workspace, hook_root) = test_dirs("roots");
        let plan = Plan::new(
            &start_params(&workspace, &base),
            "cred",
            "hook-token",
            &hook_root,
        )
        .unwrap();
        for facts in [
            plan.requested(),
            plan.check_session(
                &serde_json::from_value(json!({
                    "protocolVersion": 1,
                    "agentCapabilities": {"loadSession": false},
                }))
                .unwrap(),
                &serde_json::from_value(json!({
                    "sessionId": "sess-1",
                    "modes": null,
                    "configOptions": [
                        {"id": "model", "category": "model", "type": "select",
                         "currentValue": "x",
                         "options": [{"value": "gemini-3.8-flash", "name": "Flash"}]},
                        {"id": "thought_level", "category": "thought_level",
                         "type": "select", "currentValue": "x",
                         "options": [{"value": "high", "name": "High"}]},
                    ],
                }))
                .unwrap(),
            )
            .unwrap(),
        ] {
            let roots = &facts["native_roots"];
            assert_eq!(
                roots["workspace"].as_str(),
                Some(workspace.to_string_lossy().as_ref()),
            );
            assert_eq!(
                roots["hook_discovery"].as_str(),
                Some(hook_root.to_string_lossy().as_ref()),
            );
        }
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn plan_rejects_a_missing_hook_discovery_dir() {
        let (base, workspace, _) = test_dirs("missing");
        let missing = base.join("no-such-hooks-root");
        let error = match Plan::new(
            &start_params(&workspace, &base),
            "cred",
            "hook-token",
            &missing,
        ) {
            Ok(_) => panic!("expected an unavailable hook discovery directory"),
            Err(error) => error,
        };
        assert_eq!(error.code, "invalid");
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn swarm_mcp_entry_carries_the_agent_scoped_server() {
        let (base, workspace, hook_root) = test_dirs("mcp-entry");
        let plan = Plan::new(
            &start_params(&workspace, &base),
            "cred",
            "hook-token",
            &hook_root,
        )
        .unwrap();
        let entry = plan.mcp_server_entry().unwrap();
        assert_eq!(entry["name"], json!(SwarmMcp::SERVER_NAME));
        let command = entry["command"].as_str().unwrap();
        assert_eq!(
            Path::new(command),
            std::env::current_exe().unwrap().as_path(),
            "the MCP server is the current Eliot executable",
        );
        assert!(Path::new(command).is_absolute());
        assert_eq!(
            entry["args"],
            json!([
                "--data-dir",
                base.to_string_lossy(),
                "mcp",
                "--require-agent"
            ]),
        );
        assert_eq!(entry["env"], json!([]));
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn plan_rejects_identity_mismatch_without_owner_fallback() {
        let (base, workspace, hook_root) = test_dirs("mcp-identity");
        for (tag, environment) in [
            (
                "foreign-id",
                json!({
                    "ELIOT_AGENT_ID": "someone-else",
                    "ELIOT_AGENT_GENERATION": "1",
                    "ELIOT_AGENT_TOKEN": "test-agent-token",
                    "ELIOT_HOME": base.to_string_lossy(),
                }),
            ),
            (
                "missing-token",
                json!({
                    "ELIOT_AGENT_ID": "agent-1",
                    "ELIOT_AGENT_GENERATION": "1",
                    "ELIOT_HOME": base.to_string_lossy(),
                }),
            ),
            (
                "missing-data-root",
                json!({
                    "ELIOT_AGENT_ID": "agent-1",
                    "ELIOT_AGENT_GENERATION": "1",
                    "ELIOT_AGENT_TOKEN": "test-agent-token",
                }),
            ),
        ] {
            let mut params = start_params(&workspace, &base);
            params.target_environment = Some(environment);
            let error = match Plan::new(&params, "cred", "hook-token", &hook_root) {
                Ok(_) => panic!("expected {tag} to refuse the Agent MCP identity"),
                Err(error) => error,
            };
            assert_eq!(error.code, "not_allowed", "case {tag}");
        }
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn effective_reports_mcp_unknown_at_readiness() {
        let (base, workspace, hook_root) = test_dirs("mcp-effective");
        let plan = Plan::new(
            &start_params(&workspace, &base),
            "cred",
            "hook-token",
            &hook_root,
        )
        .unwrap();
        let effective = plan
            .check_session(
                &serde_json::from_value(json!({
                    "protocolVersion": 1,
                    "agentCapabilities": {"loadSession": false},
                }))
                .unwrap(),
                &serde_json::from_value(json!({
                    "sessionId": "sess-1",
                    "modes": null,
                    "configOptions": [
                        {"id": "model", "category": "model", "type": "select",
                         "currentValue": "x",
                         "options": [{"value": "gemini-3.8-flash", "name": "Flash"}]},
                        {"id": "thought_level", "category": "thought_level",
                         "type": "select", "currentValue": "x",
                         "options": [{"value": "high", "name": "High"}]},
                    ],
                }))
                .unwrap(),
            )
            .unwrap();
        assert_eq!(effective["mcp_servers"], Value::Null);
        assert_eq!(effective["mcp_servers_verified"], json!(false));
        let _ = fs::remove_dir_all(&base);
    }
}
