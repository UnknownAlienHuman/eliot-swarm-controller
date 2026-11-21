//! Owner MCP source scan for the Claude `inherit` policy.
//!
//! `inherit` keeps ordinary native MCP precedence. Preflight uses the shared
//! `eliot_core::is_owner_swarm_server` rule as a best-effort guard for direct
//! Owner transports and refuses sources it cannot scan. Daemon authentication
//! owns the authority boundary.
//! The native launch additionally denies the configured owner HTTP hostname
//! across settings scopes; the daemon-generated Agent server stays stdio.
//!
//! The scan covers the effective user settings/config root and the native
//! workspace project/local scopes. Managed files, installed plugins and
//! enabled account connectors are refused when they cannot be enumerated.

use std::path::{Path, PathBuf};

use eliot_core::Result;
use serde_json::Value;

use eliot_plugin::owner_scan as shared_scan;

/// Bound for one owner MCP source file read. Larger, unreadable or
/// unparseable files fail the start closed instead of scanning blind.
pub const MAX_OWNER_SCAN_BYTES: usize = shared_scan::MAX_SCAN_BYTES;

/// Bound for one owner MCP source: more entries fail closed instead of
/// truncating the scan.
pub const MAX_OWNER_SERVERS: usize = shared_scan::MAX_SCAN_SERVERS;

/// One owner-side MCP server entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerMcpEntry {
    pub name: String,
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    pub url: Option<String>,
    pub source: &'static str,
}

/// Windows Claude uses USERPROFILE; Unix uses HOME.
fn owner_home() -> Option<PathBuf> {
    std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .map(PathBuf::from)
        .filter(|home| home.is_absolute())
}

fn scan_owner_mcp_sources(
    workspace: &Path,
    root: &Path,
    config_file: &Path,
) -> Result<Vec<OwnerMcpEntry>> {
    let mut entries = Vec::new();
    collect_source(
        &mut entries,
        &root.join("settings.json"),
        "user settings",
        None,
    )?;
    let project = workspace
        .ancestors()
        .find(|directory| directory.join(".git").exists())
        .unwrap_or(workspace);
    collect_source(
        &mut entries,
        config_file,
        "user config",
        Some((workspace, project)),
    )?;
    collect_source(
        &mut entries,
        &project.join(".mcp.json"),
        "project .mcp.json",
        None,
    )?;
    collect_source(
        &mut entries,
        &workspace.join(".claude/settings.json"),
        "project settings",
        None,
    )?;
    collect_source(
        &mut entries,
        &workspace.join(".claude/settings.local.json"),
        "project local settings",
        None,
    )?;
    if project != workspace {
        collect_source(
            &mut entries,
            &project.join(".claude/settings.local.json"),
            "repository local settings",
            None,
        )?;
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(entries)
}

fn same_project_scope(left: &Path, right: &Path) -> bool {
    #[cfg(windows)]
    {
        let normalize = |path: &Path| {
            let text = path.to_string_lossy().replace('\\', "/");
            text.strip_prefix("//?/")
                .unwrap_or(&text)
                .trim_end_matches('/')
                .to_lowercase()
        };
        normalize(left) == normalize(right)
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

/// Entries that match the shared best-effort guard for direct Owner transports.
pub fn find_owner_entries<'a>(
    entries: &'a [OwnerMcpEntry],
    controller_executable: Option<&str>,
    cloud_mcp_hostname: Option<&str>,
) -> Vec<&'a OwnerMcpEntry> {
    entries
        .iter()
        .filter(|entry| {
            shared_scan::is_owner_transport(
                entry.command.as_deref(),
                entry.args.as_deref(),
                entry.url.as_deref(),
                controller_executable,
                cloud_mcp_hostname,
            )
        })
        .collect()
}

/// Refuse the start when an owner Swarm entry is reachable under `inherit`.
/// Names the entry and its source kind; paths stay out of the message.
pub fn refuse_owner_mcp_sources(
    workspace: &Path,
    config_dir: Option<&str>,
    connectors: Option<&str>,
    controller_executable: &Path,
    cloud_mcp_hostname: Option<&str>,
) -> Result<()> {
    let (root, config_file) = match config_dir.filter(|dir| !dir.is_empty()) {
        Some(dir) if Path::new(dir).is_absolute() => {
            let root = PathBuf::from(dir);
            let config_file = root.join(".claude.json");
            (root, config_file)
        }
        Some(_) => return Err(incomplete("a relative CLAUDE_CONFIG_DIR")),
        None => {
            let home = owner_home().ok_or_else(|| incomplete("the native home directory"))?;
            (home.join(".claude"), home.join(".claude.json"))
        }
    };
    refuse_owner_entries(
        workspace,
        &root,
        &config_file,
        controller_executable,
        cloud_mcp_hostname,
    )?;
    refuse_unscanned_sources(&root, connectors)
}

fn refuse_owner_entries(
    workspace: &Path,
    root: &Path,
    config_file: &Path,
    controller_executable: &Path,
    cloud_mcp_hostname: Option<&str>,
) -> Result<()> {
    let entries = scan_owner_mcp_sources(workspace, root, config_file)?;
    let hits = find_owner_entries(&entries, controller_executable.to_str(), cloud_mcp_hostname);
    if let Some(hit) = hits.first() {
        return Err(shared_scan::refusal_error(
            "Claude inherit MCP keeps the owner Swarm MCP server",
            &hit.name,
            hit.source,
        ));
    }
    Ok(())
}

fn incomplete(source: &str) -> eliot_core::Error {
    eliot_core::Error::new(
        "profile_error",
        format!("Claude cannot verify inherit MCP sources: {source}; use minimal MCP"),
    )
}

fn refuse_unscanned_sources(root: &Path, connectors: Option<&str>) -> Result<()> {
    // The native client fetches connector endpoints itself. Require the
    // documented launch-environment opt-out rather than contacting them to
    // discover what would have loaded.
    if connectors != Some("false") {
        return Err(incomplete(
            "claude.ai connectors are not explicitly disabled",
        ));
    }
    for (path, source) in [
        (
            root.join("plugins/installed_plugins.json"),
            "installed plugins",
        ),
        (root.join("remote-settings.json"), "cached managed settings"),
    ] {
        if path.try_exists().map_err(|_| incomplete(source))? {
            return Err(incomplete(source));
        }
    }
    let managed = if cfg!(windows) {
        Path::new(r"C:\Program Files\ClaudeCode")
    } else if cfg!(target_os = "macos") {
        Path::new("/Library/Application Support/ClaudeCode")
    } else {
        Path::new("/etc/claude-code")
    };
    for name in [
        "managed-mcp.json",
        "managed-settings.json",
        "managed-settings.d",
    ] {
        if managed
            .join(name)
            .try_exists()
            .map_err(|_| incomplete("system managed settings"))?
        {
            return Err(incomplete("system managed settings"));
        }
    }
    Ok(())
}

/// Read bounded source data without invoking the CLI or contacting servers.
fn collect_source(
    entries: &mut Vec<OwnerMcpEntry>,
    path: &Path,
    source: &'static str,
    project_scope: Option<(&Path, &Path)>,
) -> Result<()> {
    let label = format!("Claude owner MCP source {source}");
    let Some(document) = shared_scan::read_jsonc_document(path, &label, MAX_OWNER_SCAN_BYTES)?
    else {
        return Ok(());
    };
    if document.get("enabledPlugins").is_some_and(|plugins| {
        plugins.as_object().is_none_or(|plugins| {
            plugins
                .values()
                .any(|enabled| enabled.as_bool() != Some(false))
        })
    }) || document.get("managedMcpServers").is_some()
        || document
            .get("env")
            .and_then(Value::as_object)
            .is_some_and(|env| {
                env.keys().any(|key| {
                    key.eq_ignore_ascii_case("CLAUDE_CONFIG_DIR")
                        || key.eq_ignore_ascii_case("ENABLE_CLAUDEAI_MCP_SERVERS")
                })
            })
    {
        return Err(incomplete(source));
    }

    entries.extend(source_entries(&document, source)?);
    if let Some((workspace, repository)) = project_scope
        && let Some(projects) = shared_scan::table_object(document.get("projects"), &label)?
    {
        for (project, document) in projects {
            let project = Path::new(project);
            if same_project_scope(project, workspace) || same_project_scope(project, repository) {
                entries.extend(source_entries(document, "workspace local config")?);
                bound_entries(entries)?;
            }
        }
    }
    bound_entries(entries)
}

fn bound_entries(entries: &[OwnerMcpEntry]) -> Result<()> {
    shared_scan::ensure_total_entries(
        entries.len(),
        MAX_OWNER_SERVERS,
        "Claude owner MCP sources exceed their bound",
    )
}

/// Project the `mcpServers` table of one owner source. Absent or null
/// tables contribute nothing; a non-object table fails closed.
/// Non-object entries are skipped: only the documented table shape
/// (`command` string, `args` array, `url` string) is evaluated.
fn source_entries(document: &Value, source: &'static str) -> Result<Vec<OwnerMcpEntry>> {
    let label = format!("Claude owner MCP source {source}");
    let Some(table) = shared_scan::table_object(document.get("mcpServers"), &label)? else {
        return Ok(Vec::new());
    };
    shared_scan::ensure_total_entries(
        table.len(),
        MAX_OWNER_SERVERS,
        "Claude owner MCP source exceeds its bound",
    )?;
    let mut entries = Vec::with_capacity(table.len());
    for (name, entry) in table {
        let Some(object) = entry.as_object() else {
            continue;
        };
        if ["command", "url"].iter().any(|field| {
            object
                .get(*field)
                .and_then(Value::as_str)
                .is_some_and(|text| text.contains("${"))
        }) || object
            .get("args")
            .and_then(Value::as_array)
            .is_some_and(|args| {
                args.iter()
                    .any(|arg| arg.as_str().is_some_and(|text| text.contains("${")))
            })
        {
            return Err(incomplete(
                "an MCP command, argument, or URL uses native environment expansion",
            ));
        }
        let (command, args) = project_command(object.get("command"), object.get("args"));
        entries.push(OwnerMcpEntry {
            name: name.clone(),
            command,
            args,
            url: object.get("url").and_then(Value::as_str).map(str::to_owned),
            source,
        });
    }
    Ok(entries)
}

/// Project one stdio command shape. A string command with no `args` key
/// carries no argument vector (explicitly empty: it cannot select
/// `swarm mcp`); an `args` array splits into the vector, with a malformed
/// vector projecting as unknown (fail closed toward refusal). Anything
/// else is unknown.
fn project_command(
    command: Option<&Value>,
    args: Option<&Value>,
) -> (Option<String>, Option<Vec<String>>) {
    shared_scan::project_string_command_args(command, args)
}
