use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use eliot_core::{
    Agent, Error, McpPolicy, Result, display_path, is_swarm_reserved_server_name,
    parse_snapshot_allow_list, parse_snapshot_cloud_hostname,
};
use eliot_plugin::{native_cli::ANTIGRAVITY_MCP_CONFIG_FILE, owner_scan};
use serde_json::{Map, Value, json};

use crate::platform::data_dir::replace_private_file;

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
    if policy == McpPolicy::Inherit && !allow.is_empty() {
        return Err(Error::new("profile_error", "mcp_allow requires minimal"));
    }
    let workspace = agent
        .workspace
        .as_deref()
        .and_then(|path| fs::canonicalize(path).ok())
        .ok_or_else(|| Error::new("profile_error", "Antigravity workspace is unavailable"))?;
    let controller = std::env::current_exe().ok();
    let cloud = parse_snapshot_cloud_hostname(profile);
    let mut sources = Sources {
        policy,
        allow: &allow,
        controller: controller.as_deref(),
        cloud: cloud.as_deref(),
        global_names: HashSet::new(),
        selected: Map::new(),
    };
    if policy == McpPolicy::Inherit || !allow.is_empty() {
        let config = gemini_config_dir(environment)?;
        sources.read_servers(&config.join(ANTIGRAVITY_MCP_CONFIG_FILE), Source::Global)?;
        if policy == McpPolicy::Inherit {
            sources.scan_plugins(&config)?;
        }
    }
    sources.scan_workspace(&workspace)?;
    if policy == McpPolicy::Inherit {
        return Ok(None);
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

    let bytes = serde_json::to_vec(&json!({"mcpServers": sources.selected}))
        .map_err(|_| Error::new("internal", "Could not encode Antigravity MCP config"))?;
    let path = instance.join(ANTIGRAVITY_MCP_CONFIG_FILE);
    replace_private_file(instance, ANTIGRAVITY_MCP_CONFIG_FILE, &bytes).map_err(|error| {
        source_error(
            &error.code,
            &format!(
                "Could not publish private Antigravity MCP config ({})",
                error.message
            ),
            &path,
        )
    })?;
    Ok(Some(PreparedConfig(path)))
}

fn gemini_config_dir(environment: &Value) -> Result<PathBuf> {
    let home_name = if cfg!(windows) { "USERPROFILE" } else { "HOME" };
    environment
        .as_object()
        .and_then(|values| {
            values.iter().find_map(|(name, value)| {
                let matches =
                    name == home_name || (cfg!(windows) && name.eq_ignore_ascii_case(home_name));
                matches.then(|| value.as_str()).flatten()
            })
        })
        .map(Path::new)
        .filter(|home| home.is_absolute() && home.is_dir())
        .map(|home| home.join(".gemini/config"))
        .ok_or_else(|| Error::new("profile_error", "Antigravity native home is unavailable"))
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

fn mcp_refusal(reason: &str, name: &str, path: &Path) -> Error {
    owner_scan::refusal_error(reason, name, &display_path(&path.to_string_lossy()))
}

fn reject_plugins(layer: &Path) -> Result<()> {
    for entry in ["plugins", "plugins.json"] {
        let path = layer.join(entry);
        match path.try_exists() {
            Ok(false) => (),
            Ok(true) => {
                return Err(source_error(
                    "unsupported",
                    "Antigravity minimal cannot filter plugin MCP sources",
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Source {
    Global,
    Workspace,
    Plugin,
}

struct Sources<'a> {
    policy: McpPolicy,
    allow: &'a [String],
    controller: Option<&'a Path>,
    cloud: Option<&'a str>,
    global_names: HashSet<String>,
    selected: Map<String, Value>,
}

impl Sources<'_> {
    fn scan_workspace(&mut self, workspace: &Path) -> Result<()> {
        let repo_root = workspace
            .ancestors()
            .find(|path| path.join(".git").exists());
        for root in workspace.ancestors() {
            for alias in [".agents", ".agent", "_agents", "_agent"] {
                let layer = root.join(alias);
                self.read_servers(&layer.join(ANTIGRAVITY_MCP_CONFIG_FILE), Source::Workspace)?;
                match self.policy {
                    McpPolicy::Inherit => self.scan_plugins(&layer)?,
                    McpPolicy::Minimal => reject_plugins(&layer)?,
                }
            }
            let config = root.join(".gemini/config.json");
            if read(&config)?.is_some_and(|doc| doc.get("personal_customization_dir").is_some()) {
                return Err(source_error(
                    "unsupported",
                    "Antigravity cannot check the personal customization directory declared in",
                    &config,
                ));
            }
            if repo_root.is_none() || Some(root) == repo_root {
                break;
            }
        }
        Ok(())
    }

    fn scan_plugins(&mut self, layer: &Path) -> Result<()> {
        let manifest = layer.join("plugins.json");
        if read(&manifest)?.is_some_and(|document| {
            !document.is_object()
                || ["entries", "inherits"].iter().any(|field| {
                    document
                        .get(field)
                        .is_some_and(|value| !value.as_array().is_some_and(Vec::is_empty))
                })
        }) {
            return Err(source_error(
                "unsupported",
                "Antigravity cannot check redirected plugin MCP sources declared in",
                &manifest,
            ));
        }
        let path = layer.join("plugins");
        let plugins = match fs::read_dir(&path) {
            Ok(plugins) => plugins,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => {
                return Err(source_error(
                    "profile_error",
                    "Antigravity plugin directory is unreadable",
                    &path,
                ));
            }
        };
        for (index, plugin) in plugins.enumerate() {
            owner_scan::ensure_total_entries(
                index + 1,
                owner_scan::MAX_SCAN_SERVERS,
                "Antigravity plugin directory exceeds its scan bound",
            )?;
            let plugin = plugin
                .map_err(|_| {
                    source_error(
                        "profile_error",
                        "Antigravity plugin directory is unreadable",
                        &path,
                    )
                })?
                .path();
            if fs::metadata(&plugin)
                .map_err(|_| {
                    source_error(
                        "profile_error",
                        "Antigravity plugin entry is unreadable",
                        &plugin,
                    )
                })?
                .is_dir()
            {
                self.read_servers(&plugin.join(ANTIGRAVITY_MCP_CONFIG_FILE), Source::Plugin)?;
            }
        }
        Ok(())
    }

    fn read_servers(&mut self, path: &Path, source: Source) -> Result<()> {
        // AGY can initialize its global MCP file at zero bytes.
        if source == Source::Global && path.metadata().is_ok_and(|m| m.is_file() && m.len() == 0) {
            return Ok(());
        }
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
        let minimal = self.policy == McpPolicy::Minimal;
        for (name, entry) in servers {
            let table = entry.as_object().ok_or_else(|| {
                mcp_refusal(
                    "Antigravity expects an MCP entry object for server",
                    name,
                    path,
                )
            })?;
            let allowed = self.allow.contains(name);
            if minimal && allowed {
                if source == Source::Global {
                    self.global_names.insert(name.clone());
                } else if self.global_names.contains(name) {
                    return Err(mcp_refusal(
                        "Antigravity minimal refuses workspace override of global MCP server",
                        name,
                        path,
                    ));
                }
            }
            if table.get("disabled") == Some(&Value::Bool(true)) {
                continue;
            }
            if minimal && !allowed {
                if source == Source::Workspace {
                    return Err(mcp_refusal(
                        "Antigravity minimal cannot suppress workspace MCP server",
                        name,
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
                return Err(mcp_refusal(
                    "Antigravity cannot exclude Owner or reserved Swarm MCP server",
                    name,
                    path,
                ));
            }
            if minimal {
                self.selected.insert(name.clone(), entry.clone());
            }
        }
        Ok(())
    }
}
