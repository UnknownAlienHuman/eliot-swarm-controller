use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use eliot_core::{
    Agent, Error, McpPolicy, Result, display_path, is_swarm_reserved_server_name,
    parse_snapshot_allow_list, parse_snapshot_cloud_hostname,
};
use eliot_plugin::{native_cli::ANTIGRAVITY_MCP_CONFIG_FILE, owner_scan};
use serde_json::{Map, Value, json};

/// Remove the private handoff file if the plugin never moved it into its
/// native config root. The generation-specific path cannot race a resume.
pub(super) struct PreparedConfig(PathBuf);

impl Drop for PreparedConfig {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

pub(super) fn prepare(
    agent: &Agent,
    environment: &Value,
    instance: &Path,
) -> Result<Option<PreparedConfig>> {
    let profile = agent
        .profile_snapshot
        .as_object()
        .ok_or_else(|| Error::new("profile_error", "Antigravity requires a frozen profile"))?;
    let policy = McpPolicy::parse_snapshot(profile)?;
    let allow = parse_snapshot_allow_list(profile)?;
    if policy == McpPolicy::Inherit {
        if !allow.is_empty() {
            return Err(Error::new("profile_error", "mcp_allow requires minimal"));
        }
        return Ok(None);
    }

    let workspace = agent
        .workspace
        .as_deref()
        .and_then(|path| fs::canonicalize(path).ok())
        .ok_or_else(|| Error::new("profile_error", "Antigravity workspace is unavailable"))?;
    let controller = std::env::current_exe().ok();
    let cloud = parse_snapshot_cloud_hostname(profile);
    let mut sources = Sources {
        allow: &allow,
        controller: controller.as_deref(),
        cloud: cloud.as_deref(),
        selected: Map::new(),
    };
    if !allow.is_empty() {
        let home_name = if cfg!(windows) { "USERPROFILE" } else { "HOME" };
        let home = environment
            .as_object()
            .and_then(|values| {
                values.iter().find_map(|(name, value)| {
                    let matches = name == home_name
                        || (cfg!(windows) && name.eq_ignore_ascii_case(home_name));
                    matches.then(|| value.as_str()).flatten()
                })
            })
            .map(Path::new)
            .filter(|home| home.is_absolute() && home.is_dir())
            .ok_or_else(|| Error::new("profile_error", "Antigravity native home is unavailable"))?;
        let global = home
            .join(".gemini/config")
            .join(ANTIGRAVITY_MCP_CONFIG_FILE);
        // Native startup can initialize the user MCP file at zero bytes.
        if !global
            .metadata()
            .is_ok_and(|meta| meta.is_file() && meta.len() == 0)
        {
            sources.read_servers(&global, false)?;
        }
    }

    // The isolated Gemini root excludes user plugins. Workspace customizations
    // still load, so refuse sources we cannot constrain with this native flag.
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
            sources.read_servers(&layer.join(ANTIGRAVITY_MCP_CONFIG_FILE), true)?;
        }
        let config = root.join(".gemini/config.json");
        if read(&config)?
            .is_some_and(|document| document.get("personal_customization_dir").is_some())
        {
            return Err(source_error(
                "unsupported",
                "Antigravity minimal cannot filter the personal customization directory declared in",
                &config,
            ));
        }
        if Some(root) == repo_root {
            break;
        }
    }
    if allow
        .iter()
        .any(|name| !sources.selected.contains_key(name))
    {
        return Err(Error::new(
            "profile_error",
            "Antigravity mcp_allow names an unavailable server",
        ));
    }

    let path = instance.join(ANTIGRAVITY_MCP_CONFIG_FILE);
    let bytes = serde_json::to_vec(&json!({"mcpServers": sources.selected}))
        .map_err(|_| Error::new("internal", "Could not encode Antigravity MCP config"))?;
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path).map_err(|_| {
        Error::new(
            "io_error",
            "Could not create private Antigravity MCP config",
        )
    })?;
    let prepared = PreparedConfig(path);
    let write = file.write_all(&bytes);
    drop(file);
    write.map_err(|_| Error::new("io_error", "Could not write private Antigravity MCP config"))?;
    Ok(Some(prepared))
}

fn read(path: &Path) -> Result<Option<Value>> {
    owner_scan::read_jsonc_document(path, "Antigravity MCP source", owner_scan::MAX_SCAN_BYTES)
        .map_err(|error| source_error(&error.code, &error.message, path))
}

fn source_error(code: &str, reason: &str, path: &Path) -> Error {
    Error::new(
        code,
        format!("{reason}: {}", display_path(&path.to_string_lossy())),
    )
}

fn reject_plugins(layer: &Path) -> Result<()> {
    for entry in ["plugins", "plugins.json"] {
        let path = layer.join(entry);
        match path.try_exists() {
            Ok(false) => (),
            Ok(true) => {
                return Err(source_error(
                    "unsupported",
                    "Antigravity minimal cannot filter plugin MCP sources at",
                    &path,
                ));
            }
            Err(_) => {
                return Err(source_error(
                    "profile_error",
                    "Antigravity plugin source is unreadable",
                    &path,
                ));
            }
        }
    }
    Ok(())
}

struct Sources<'a> {
    allow: &'a [String],
    controller: Option<&'a Path>,
    cloud: Option<&'a str>,
    selected: Map<String, Value>,
}

impl Sources<'_> {
    fn read_servers(&mut self, path: &Path, workspace: bool) -> Result<()> {
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
                source_error(
                    "profile_error",
                    "Antigravity MCP entry must be an object in",
                    path,
                )
            })?;
            if table.get("disabled") == Some(&Value::Bool(true)) {
                continue;
            }
            if !self.allow.contains(name) {
                if workspace {
                    return Err(source_error(
                        "unsupported",
                        "Antigravity minimal cannot suppress a workspace MCP server in",
                        path,
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
                    self.controller.and_then(Path::to_str),
                    self.cloud,
                )
            {
                return Err(source_error(
                    "profile_error",
                    "Antigravity cannot admit an owner Swarm MCP server from",
                    path,
                ));
            }
            self.selected.insert(name.clone(), entry.clone());
        }
        Ok(())
    }
}
