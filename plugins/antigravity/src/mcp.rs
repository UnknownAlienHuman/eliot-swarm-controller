//! AGY 1.5.10 `acp --help`: --gemini-dir selects Gemini files and
//! --app-data-dir selects a relative application-data directory. Google's MCP/customization docs
//! describe config/mcp_config.json, workspace aliases, parent discovery and
//! plugins; authentication uses the OS keyring, not a copied auth file.

use std::{
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
};

use eliot_core::{Error, McpPolicy, Result, is_swarm_reserved_server_name};
use eliot_plugin::{AgentStartParams, owner_scan};
use serde_json::{Map, Value, json};

use crate::{profile::Plan, session::without_verbatim_prefix};

pub(crate) struct OwnedConfig(PathBuf);

impl Drop for OwnedConfig {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.0.join("config/mcp_config.json"));
        let _ = fs::remove_dir(self.0.join("config"));
        // Native conversations/logs survive for exact session/load on resume.
        // Only an empty root (a pre-spawn failure) can be removed here.
        let _ = fs::remove_dir(&self.0);
    }
}

pub(crate) fn prepare(
    plan: &mut Plan,
    params: &AgentStartParams,
    instance: &Path,
) -> Result<Option<OwnedConfig>> {
    let home_name = if cfg!(windows) { "USERPROFILE" } else { "HOME" };
    let home = plan
        .launch
        .environment
        .iter()
        .find(|(name, _)| name.to_string_lossy().eq_ignore_ascii_case(home_name))
        .map(|(_, value)| value.clone())
        .or_else(|| std::env::var_os(home_name))
        .map(PathBuf::from)
        .filter(|home| home.is_absolute() && home.is_dir())
        .ok_or_else(|| Error::new("profile_error", "Antigravity native home is unavailable"))?;
    let native_root = home.join(".gemini");
    let controller = std::env::current_exe().ok();
    let cloud = params
        .profile
        .as_ref()
        .and_then(|profile| profile.get("mcp_cloud_hostname"))
        .and_then(Value::as_str);
    let mut selected = Map::new();
    read_servers(
        &native_root.join("config/mcp_config.json"),
        false,
        plan,
        controller.as_deref(),
        cloud,
        &mut selected,
    )?;
    if plan.mcp_policy == McpPolicy::Inherit {
        reject_plugins(&native_root.join("config"))?;
    }
    // AGY checks ancestors only within a repository. A .git file also marks
    // a worktree root. With no repository only cwd is a discovery root.
    let workspace = without_verbatim_prefix(
        &fs::canonicalize(&plan.launch.cwd)
            .map_err(|_| Error::new("profile_error", "Antigravity workspace is unavailable"))?,
    );
    let repo_root = workspace
        .ancestors()
        .find(|path| path.join(".git").exists());
    for root in workspace.ancestors() {
        if root != workspace && repo_root.is_none() {
            break;
        }
        for alias in [".agents", ".agent", "_agents", "_agent"] {
            let layer = root.join(alias);
            reject_plugins(&layer)?;
            read_servers(
                &layer.join("mcp_config.json"),
                true,
                plan,
                controller.as_deref(),
                cloud,
                &mut selected,
            )?;
        }
        if let Some(document) = read(&root.join(".gemini/config.json"))?
            && document.get("personal_customization_dir").is_some()
        {
            return Err(Error::new(
                "unsupported",
                "Antigravity MCP policy cannot filter personal customization directories",
            ));
        }
        if Some(root) == repo_root {
            break;
        }
    }
    if plan.mcp_policy == McpPolicy::Inherit {
        return Ok(None);
    }
    if plan
        .mcp_allow
        .iter()
        .any(|name| !selected.contains_key(name))
    {
        return Err(Error::new(
            "profile_error",
            "Antigravity mcp_allow names an unavailable server",
        ));
    }
    let instance = without_verbatim_prefix(&fs::canonicalize(instance).map_err(|_| {
        Error::new(
            "invalid",
            "Antigravity private instance directory is unavailable",
        )
    })?);
    if instance.starts_with(&workspace) || workspace.starts_with(&instance) {
        return Err(Error::new(
            "invalid",
            "Antigravity MCP config overlaps its workspace",
        ));
    }
    let root = plan
        .mcp_root
        .as_ref()
        .ok_or_else(|| Error::new("invalid", "Antigravity minimal config root is unavailable"))?;
    if root.parent() != instance.parent()
        || root.starts_with(&workspace)
        || workspace.starts_with(root)
    {
        return Err(Error::new(
            "invalid",
            "Antigravity MCP root is outside its private parent",
        ));
    }
    match fs::create_dir(root) {
        Ok(()) => (),
        Err(error)
            if error.kind() == std::io::ErrorKind::AlreadyExists && plan.resume().is_some() => {}
        Err(_) => {
            return Err(Error::new(
                "io_error",
                "Could not create owned Antigravity MCP root",
            ));
        }
    }
    let canonical_root = fs::canonicalize(root)
        .map_err(|_| Error::new("invalid", "Antigravity MCP root is unavailable"))?;
    if without_verbatim_prefix(&canonical_root) != without_verbatim_prefix(root) {
        return Err(Error::new("invalid", "Antigravity MCP root was redirected"));
    }
    let owned = OwnedConfig(root.clone());
    fs::create_dir_all(root.join("config")).map_err(|_| {
        Error::new(
            "io_error",
            "Could not create owned Antigravity MCP config directory",
        )
    })?;
    if fs::canonicalize(root.join("config"))
        .ok()
        .map(|path| without_verbatim_prefix(&path))
        != Some(without_verbatim_prefix(&root.join("config")))
    {
        return Err(Error::new(
            "invalid",
            "Antigravity MCP config directory was redirected",
        ));
    }
    let bytes = serde_json::to_vec(&json!({"mcpServers": selected}))
        .map_err(|_| Error::new("internal", "Could not encode Antigravity MCP config"))?;
    let config = root.join("config/mcp_config.json");
    let temporary = root.join(format!("config/mcp-{}.tmp", ulid::Ulid::new()));
    let write = (|| {
        use std::io::Write;
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        drop(file);
        fs::rename(&temporary, &config)
    })();
    if write.is_err() {
        let _ = fs::remove_file(&temporary);
        return Err(Error::new(
            "io_error",
            "Could not write owned Antigravity MCP config",
        ));
    }
    // The Agent's native store persists across generations; sign-in still
    // uses Windows Credential Manager (or the platform's OS keyring).
    // No HOME override, app-data override or credential-file copy.
    plan.launch.args.extend([
        OsStr::new("--gemini-dir").to_owned(),
        without_verbatim_prefix(root).into_os_string(),
    ]);
    Ok(Some(owned))
}

fn read(path: &Path) -> Result<Option<Value>> {
    owner_scan::read_jsonc_document(path, "Antigravity MCP source", owner_scan::MAX_SCAN_BYTES)
}

fn reject_plugins(layer: &Path) -> Result<()> {
    for entry in ["plugins", "plugins.json"] {
        match layer.join(entry).try_exists() {
            Ok(false) => (),
            Ok(true) => {
                return Err(Error::new(
                    "unsupported",
                    "Antigravity MCP policy cannot filter workspace/inherited plugin MCP sources",
                ));
            }
            Err(_) => {
                return Err(Error::new(
                    "profile_error",
                    "Antigravity plugin source is unreadable",
                ));
            }
        }
    }
    Ok(())
}

fn read_servers(
    path: &Path,
    workspace: bool,
    plan: &Plan,
    controller: Option<&Path>,
    cloud: Option<&str>,
    selected: &mut Map<String, Value>,
) -> Result<()> {
    let Some(document) = read(path)? else {
        return Ok(());
    };
    let Some(servers) =
        owner_scan::table_object(document.get("mcpServers"), "Antigravity MCP servers")?
    else {
        return Ok(());
    };
    owner_scan::ensure_total_entries(
        servers.len(),
        owner_scan::MAX_SCAN_SERVERS,
        "Antigravity MCP source exceeds its server bound",
    )?;
    for (name, entry) in servers {
        let table = entry.as_object().ok_or_else(|| {
            Error::new("profile_error", "Antigravity MCP entry must be an object")
        })?;
        if table.get("disabled") == Some(&Value::Bool(true)) {
            continue;
        }
        let allowed = plan.mcp_allow.contains(name);
        if plan.mcp_policy == McpPolicy::Minimal && !allowed {
            if workspace {
                return Err(Error::new(
                    "unsupported",
                    "Antigravity minimal cannot suppress an unexpected workspace MCP server",
                ));
            }
            continue;
        }
        let (command, args) =
            owner_scan::project_string_command_args(table.get("command"), table.get("args"));
        let url = table
            .get("serverUrl")
            .or_else(|| table.get("url"))
            .and_then(Value::as_str);
        if is_swarm_reserved_server_name(name)
            || owner_scan::is_owner_transport(
                command.as_deref(),
                args.as_deref(),
                url,
                controller.and_then(Path::to_str),
                cloud,
            )
        {
            return Err(Error::new(
                "profile_error",
                "Antigravity cannot admit an owner Swarm MCP server",
            ));
        }
        if allowed {
            selected.insert(name.clone(), entry.clone());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (crate::test_temp::TempDir, PathBuf, AgentStartParams, Plan) {
        let base = std::env::temp_dir().join(format!("eliot-agy-mcp-{}", ulid::Ulid::new()));
        let workspace = base.join("workspace");
        let hooks = base.join("hooks");
        for path in [&workspace, &hooks] {
            fs::create_dir_all(path).unwrap();
        }
        let mut params = AgentStartParams::new("agent-1", 1, "cmd-1");
        params.workspace = Some(workspace.to_string_lossy().into_owned());
        params.profile = Some(json!({"harness":"antigravity", "cmd":"agy", "mcp":"minimal"}));
        params.target_environment = Some(json!({
            "ELIOT_AGENT_ID":"agent-1", "ELIOT_AGENT_GENERATION":"1",
            "ELIOT_AGENT_TOKEN":"test-token", "ELIOT_HOME":base.to_string_lossy(),
        }));
        params.source_plan = Some(json!({"session":"stream", "turn":"stream",
            "message":"stream", "tool":"stream", "request":"stream"}));
        let plan = Plan::new(&params, "credential", "hook-token", &hooks).unwrap();
        (
            crate::test_temp::TempDir::own(base.clone()),
            base,
            params,
            plan,
        )
    }

    #[test]
    fn minimal_selects_only_enabled_allow_list_and_never_owner_identity() {
        let (_guard, base, _, mut plan) = fixture();
        plan.mcp_allow = vec!["search".into()];
        let path = base.join("mcp.json");
        fs::write(
            &path,
            serde_json::to_vec(&json!({"mcpServers":{
                "search":{"command":"search.exe"},
                "windows":{"command":"windows-mcp.exe"},
                "disabled":{"command":"other", "disabled":true}
            }}))
            .unwrap(),
        )
        .unwrap();
        let mut selected = Map::new();
        read_servers(&path, false, &plan, None, None, &mut selected).unwrap();
        assert_eq!(selected.keys().collect::<Vec<_>>(), vec!["search"]);
        fs::write(
            &path,
            r#"{"mcpServers":{"search":{"command":"swarm.exe","args":["mcp"]}}}"#,
        )
        .unwrap();
        assert_eq!(
            read_servers(&path, false, &plan, None, None, &mut selected)
                .unwrap_err()
                .code,
            "profile_error"
        );
        fs::write(
            &path,
            r#"{"mcpServers":{"search":{"serverUrl":"https://owner.example/mcp"}}}"#,
        )
        .unwrap();
        assert_eq!(
            read_servers(
                &path,
                false,
                &plan,
                None,
                Some("owner.example"),
                &mut selected
            )
            .unwrap_err()
            .code,
            "profile_error"
        );
    }

    #[test]
    fn unexpected_workspace_server_and_plugins_refuse_before_spawn() {
        let (_guard, base, _, plan) = fixture();
        let path = base.join("mcp.json");
        fs::write(
            &path,
            r#"{"mcpServers":{"windows":{"command":"windows-mcp.exe"}}}"#,
        )
        .unwrap();
        let error = read_servers(&path, true, &plan, None, None, &mut Map::new()).unwrap_err();
        assert_eq!(error.code, "unsupported");
        fs::write(base.join("plugins.json"), "{}").unwrap();
        assert_eq!(reject_plugins(&base).unwrap_err().code, "unsupported");
    }

    #[test]
    fn inherit_checks_renamed_owner_transport() {
        let (_guard, base, _, mut plan) = fixture();
        plan.mcp_policy = McpPolicy::Inherit;
        let path = base.join("mcp.json");
        fs::write(
            &path,
            r#"{"mcpServers":{"renamed":{"command":"swarm.exe","args":["mcp"]}}}"#,
        )
        .unwrap();
        assert_eq!(
            read_servers(&path, false, &plan, None, None, &mut Map::new())
                .unwrap_err()
                .code,
            "profile_error"
        );
    }

    #[test]
    fn isolated_config_leaves_owner_untouched_and_retains_exact_agent_store() {
        let (_guard, base, params, mut plan) = fixture();
        let home = base.join("home");
        let owner_config = home.join(".gemini/config");
        fs::create_dir_all(&owner_config).unwrap();
        let original =
            r#"{"mcpServers":{"search":{"command":"search.exe"},"other":{"command":"other.exe"}}}"#;
        fs::write(owner_config.join("mcp_config.json"), original).unwrap();
        plan.launch.environment.push((
            if cfg!(windows) { "USERPROFILE" } else { "HOME" }.into(),
            home.into_os_string(),
        ));
        plan.mcp_allow = vec!["search".into()];
        let instance = base.join("instance");
        fs::create_dir(&instance).unwrap();
        let config = prepare(&mut plan, &params, &instance).unwrap().unwrap();
        let root = plan.mcp_root.clone().unwrap();
        let staged = read(&root.join("config/mcp_config.json")).unwrap().unwrap();
        assert_eq!(
            staged["mcpServers"]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            vec!["search"]
        );
        assert!(plan.launch.args.iter().any(|arg| arg == "--gemini-dir"));
        assert!(!plan.launch.args.iter().any(|arg| arg == "--app-data-dir"));
        fs::create_dir(root.join("antigravity-cli")).unwrap();
        fs::write(
            root.join("antigravity-cli/conversation"),
            "retained-native-data",
        )
        .unwrap();
        drop(config);
        assert!(!root.join("config/mcp_config.json").exists());
        assert!(root.join("antigravity-cli/conversation").is_file());
        plan.resume = Some("exact-session".into());
        let config = prepare(&mut plan, &params, &instance).unwrap().unwrap();
        assert_eq!(plan.mcp_root.as_ref(), Some(&root));
        assert_eq!(
            fs::read_to_string(owner_config.join("mcp_config.json")).unwrap(),
            original
        );
        drop(config);
    }
}
