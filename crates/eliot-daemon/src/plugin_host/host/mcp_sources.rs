use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use eliot_core::{
    Agent, Error, McpPolicy, Result, api::harness::NativeMcpSources, display_path,
    is_swarm_reserved_server_name, parse_snapshot_allow_list, parse_snapshot_cloud_hostname,
};
use eliot_plugin::owner_scan;
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
    layout: NativeMcpSources,
    agent: &Agent,
    environment: &Value,
    instance: &Path,
) -> Result<Option<PreparedConfig>> {
    let profile = agent.profile_snapshot.as_object().ok_or_else(|| {
        Error::new(
            "profile_error",
            format!("{} requires a frozen profile", layout.name),
        )
    })?;
    let policy = McpPolicy::parse_snapshot(profile)?;
    let allow = parse_snapshot_allow_list(profile)?;
    if policy == McpPolicy::Inherit && !allow.is_empty() {
        return Err(Error::new("profile_error", "mcp_allow requires minimal"));
    }
    let workspace = agent
        .workspace
        .as_deref()
        .and_then(|path| fs::canonicalize(path).ok())
        .ok_or_else(|| {
            Error::new(
                "profile_error",
                format!("{} workspace is unavailable", layout.name),
            )
        })?;
    let controller = std::env::current_exe().ok();
    let cloud = parse_snapshot_cloud_hostname(profile);
    let mut sources = Sources {
        layout,
        policy,
        allow: &allow,
        controller: controller.as_deref(),
        cloud: cloud.as_deref(),
        global_names: HashSet::new(),
        selected: Map::new(),
    };
    if policy == McpPolicy::Inherit || !allow.is_empty() {
        let config = global_config_dir(environment, layout)?;
        sources.read_servers(&config.join(layout.file), Source::Global)?;
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
            format!("{} mcp_allow names an unavailable server", layout.name),
        ));
    }

    let bytes =
        serde_json::to_vec(&json!({(layout.servers_key): sources.selected})).map_err(|_| {
            Error::new(
                "internal",
                format!("Could not encode {} MCP config", layout.name),
            )
        })?;
    let path = instance.join(layout.file);
    replace_private_file(instance, layout.file, &bytes).map_err(|error| {
        source_error(
            &error.code,
            &format!(
                "Could not publish private {} MCP config ({})",
                layout.name, error.message
            ),
            &path,
        )
    })?;
    Ok(Some(PreparedConfig(path)))
}

fn global_config_dir(environment: &Value, layout: NativeMcpSources) -> Result<PathBuf> {
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
        .map(|home| home.join(layout.global_dir))
        .ok_or_else(|| {
            Error::new(
                "profile_error",
                format!("{} native home is unavailable", layout.name),
            )
        })
}

fn read(path: &Path, layout: NativeMcpSources) -> Result<Option<Value>> {
    owner_scan::read_jsonc_document(
        path,
        &format!("{} MCP source", layout.name),
        owner_scan::MAX_SCAN_BYTES,
    )
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

fn reject_plugins(layer: &Path, layout: NativeMcpSources) -> Result<()> {
    for entry in [layout.plugins_directory, layout.plugins_manifest] {
        let path = layer.join(entry);
        match path.try_exists() {
            Ok(false) => (),
            Ok(true) => {
                return Err(source_error(
                    "unsupported",
                    &format!("{} minimal cannot filter plugin MCP sources", layout.name),
                    &path,
                ));
            }
            Err(_) => {
                return Err(source_error(
                    "profile_error",
                    &format!("{} plugin source is unreadable", layout.name),
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
    layout: NativeMcpSources,
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
            for alias in self.layout.workspace_layers {
                let layer = root.join(alias);
                self.read_servers(&layer.join(self.layout.file), Source::Workspace)?;
                match self.policy {
                    McpPolicy::Inherit => self.scan_plugins(&layer)?,
                    McpPolicy::Minimal => reject_plugins(&layer, self.layout)?,
                }
            }
            let config = root.join(self.layout.workspace_settings);
            if read(&config, self.layout)?
                .is_some_and(|doc| doc.get(self.layout.redirected_home_key).is_some())
            {
                return Err(source_error(
                    "unsupported",
                    &format!(
                        "{} cannot check the personal customization directory declared in",
                        self.layout.name
                    ),
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
        let manifest = layer.join(self.layout.plugins_manifest);
        if read(&manifest, self.layout)?.is_some_and(|document| {
            !document.is_object()
                || self.layout.redirect_keys.iter().any(|field| {
                    document
                        .get(field)
                        .is_some_and(|value| !value.as_array().is_some_and(Vec::is_empty))
                })
        }) {
            return Err(source_error(
                "unsupported",
                &format!(
                    "{} cannot check redirected plugin MCP sources declared in",
                    self.layout.name
                ),
                &manifest,
            ));
        }
        let path = layer.join(self.layout.plugins_directory);
        let plugins = match fs::read_dir(&path) {
            Ok(plugins) => plugins,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => {
                return Err(source_error(
                    "profile_error",
                    &format!("{} plugin directory is unreadable", self.layout.name),
                    &path,
                ));
            }
        };
        for (index, plugin) in plugins.enumerate() {
            owner_scan::ensure_total_entries(
                index + 1,
                owner_scan::MAX_SCAN_SERVERS,
                &format!(
                    "{} plugin directory exceeds its scan bound",
                    self.layout.name
                ),
            )?;
            let plugin = plugin
                .map_err(|_| {
                    source_error(
                        "profile_error",
                        &format!("{} plugin directory is unreadable", self.layout.name),
                        &path,
                    )
                })?
                .path();
            if fs::metadata(&plugin)
                .map_err(|_| {
                    source_error(
                        "profile_error",
                        &format!("{} plugin entry is unreadable", self.layout.name),
                        &plugin,
                    )
                })?
                .is_dir()
            {
                self.read_servers(&plugin.join(self.layout.file), Source::Plugin)?;
            }
        }
        Ok(())
    }

    fn read_servers(&mut self, path: &Path, source: Source) -> Result<()> {
        // The native host can initialize its global MCP file at zero bytes.
        if source == Source::Global && path.metadata().is_ok_and(|m| m.is_file() && m.len() == 0) {
            return Ok(());
        }
        let Some(document) = read(path, self.layout)? else {
            return Ok(());
        };
        let Some(servers) = owner_scan::table_object(
            document.get(self.layout.servers_key),
            &format!("{} MCP servers", self.layout.name),
        )?
        else {
            return Ok(());
        };
        owner_scan::ensure_total_entries(
            servers.len(),
            owner_scan::MAX_SCAN_SERVERS,
            &format!("{} MCP source exceeds its server bound", self.layout.name),
        )?;
        let minimal = self.policy == McpPolicy::Minimal;
        for (name, entry) in servers {
            let table = entry.as_object().ok_or_else(|| {
                mcp_refusal(
                    &format!(
                        "{} expects an MCP entry object for server",
                        self.layout.name
                    ),
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
                        &format!(
                            "{} minimal refuses workspace override of global MCP server",
                            self.layout.name
                        ),
                        name,
                        path,
                    ));
                }
            }
            // Disabled native entries do not open a connection.
            if table.get(self.layout.disabled_key) == Some(&Value::Bool(true)) {
                continue;
            }
            let (command, args) =
                owner_scan::project_string_command_args(table.get("command"), table.get("args"));
            // Check all declared transport aliases.
            let owner_http = self
                .layout
                .http_keys
                .iter()
                .filter_map(|key| table.get(*key).and_then(Value::as_str))
                .any(|url| eliot_core::is_owner_swarm_http(Some(url), self.cloud));
            if is_swarm_reserved_server_name(name)
                || owner_http
                || eliot_core::is_owner_swarm_stdio(
                    command.as_deref(),
                    args.as_deref(),
                    self.controller.and_then(Path::to_str),
                )
            {
                return Err(mcp_refusal(
                    &format!(
                        "{} cannot exclude Owner or reserved Swarm MCP server",
                        self.layout.name
                    ),
                    name,
                    path,
                ));
            }
            if minimal && !allowed {
                if source == Source::Workspace {
                    return Err(mcp_refusal(
                        &format!(
                            "{} minimal cannot suppress workspace MCP server",
                            self.layout.name
                        ),
                        name,
                        path,
                    ));
                }
                continue;
            }
            if minimal {
                self.selected.insert(name.clone(), entry.clone());
            }
        }
        Ok(())
    }
}
