//! Workspace project MCP layer scan for the owner Swarm identity.
//!
//! The owned `serve` runs under isolated per-Agent `XDG_CONFIG_HOME` /
//! `XDG_DATA_HOME`, so the owner's global config never merges; the
//! workspace-inherited layers still merge natively (installed 2.2.7): each
//! ancestor directory's direct `opencode.json` / `opencode.jsonc` files
//! plus its `.opencode/` project directory, each read as a native document
//! (the `mcp.servers` table plus the legacy `mcp.<name>` server spelling).
//! The shared `eliot_core::is_owner_swarm_server` rule checks those layers
//! before the native session exists as a best-effort guard for direct Owner
//! transports. Daemon authentication owns the authority boundary.
//! A match fails the start with the entry and layer named, in
//! every policy and regardless of entry name. Managed, system and global
//! layers are not files here; the effective configuration proves their
//! absence after the session exists (see `host.rs`). No project or user
//! file is ever edited.

use std::path::Path;

use eliot_core::Result;
use serde_json::Value;

use eliot_plugin::owner_scan as shared_scan;

/// Inherited project config file names the installed server merges, in
/// read order. Every inherited layer below carries these same file names;
/// the global/user layers stay neutralized by the isolated per-Agent XDG
/// roots (verified on 2.2.7).
pub const PROJECT_CONFIG_FILES: &[&str] = &["opencode.json", "opencode.jsonc"];
/// Bound on inherited layers walked outward from the workspace. Real trees
/// are shallow; this only caps degenerate depths so the pre-spawn scan
/// stays bounded alongside the per-file and total-entry bounds.
pub const MAX_PROJECT_ANCESTORS: usize = 32;

/// Bound for one project config file read: far above real configs, small
/// enough to keep the pre-spawn scan bounded. Larger, unreadable or
/// unparseable files fail the start closed instead of scanning blind.
pub const MAX_PROJECT_CONFIG_BYTES: usize = shared_scan::MAX_SCAN_BYTES;

/// Bound for one project MCP layer: far above real surfaces. More entries
/// fail the start closed instead of truncating the scan.
pub const MAX_PROJECT_SERVERS: usize = shared_scan::MAX_SCAN_SERVERS;

/// One inherited project-layer MCP server entry. `file` names the layer
/// relative to the workspace (`opencode.json`, `.opencode/opencode.jsonc`,
/// `../../opencode.json`, ...).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMcpEntry {
    pub name: String,
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    pub url: Option<String>,
    pub file: String,
}

/// Scan the workspace and every inherited ancestor layer for MCP server
/// entries: each directory's direct config files, then its `.opencode/`
/// project directory, outward to the filesystem root. Absent files
/// contribute nothing; a present file must read within bounds and parse
/// (JSONC: comments and trailing commas tolerated) or the start fails
/// closed with the layer named.
pub fn scan_project_mcp(workspace: &Path) -> Result<Vec<ProjectMcpEntry>> {
    let mut entries = Vec::new();
    let mut layer: Option<&Path> = Some(workspace);
    let mut depth = 0usize;
    while let Some(dir) = layer {
        if depth > MAX_PROJECT_ANCESTORS {
            // Degenerate depth with ancestors still unscanned: succeeding
            // silently would bless layers the scan never saw, so the start
            // fails closed with the bound named.
            return Err(eliot_core::Error::new(
                "profile_error",
                "OpenCode project MCP layer scan exceeds its ancestor bound",
            ));
        }
        scan_layer(dir, depth, &mut entries)?;
        shared_scan::ensure_total_entries(
            entries.len(),
            MAX_PROJECT_SERVERS,
            "OpenCode project MCP layer exceeds its bound",
        )?;
        layer = dir.parent();
        depth += 1;
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(entries)
}

/// One inherited layer: the directory's direct config files, then its
/// `.opencode/` project directory. Labels name the layer relative to the
/// workspace, so a refusal points at the exact file.
fn scan_layer(dir: &Path, depth: usize, entries: &mut Vec<ProjectMcpEntry>) -> Result<()> {
    let prefix = "../".repeat(depth);
    for &file in PROJECT_CONFIG_FILES {
        let label = if depth == 0 {
            file.to_owned()
        } else {
            format!("{prefix}{file}")
        };
        read_layer_file(&dir.join(file), label, entries)?;
        let project_label = if depth == 0 {
            format!(".opencode/{file}")
        } else {
            format!("{prefix}.opencode/{file}")
        };
        read_layer_file(&dir.join(".opencode").join(file), project_label, entries)?;
    }
    Ok(())
}

/// One layer file: absent files contribute nothing, present files parse or
/// fail the start closed with the layer named.
fn read_layer_file(path: &Path, label: String, entries: &mut Vec<ProjectMcpEntry>) -> Result<()> {
    let document_label = format!("OpenCode project MCP layer in {label}");
    let Some(document) =
        shared_scan::read_jsonc_document(path, &document_label, MAX_PROJECT_CONFIG_BYTES)?
    else {
        return Ok(());
    };
    entries.extend(project_entries(&document, label)?);
    Ok(())
}

/// Typed refusal reason for an owner-identity project entry: names the
/// server and the project file it came from. The plugin fails the start
/// with this reason before the native session exists.
pub fn owner_refusal_reason(entry: &ProjectMcpEntry) -> String {
    *shared_scan::refusal_error(
        "OpenCode project MCP layer contributes the owner Swarm MCP server",
        &entry.name,
        &entry.file,
    )
    .message
}

/// Entries that match the shared best-effort guard for direct Owner transports.
pub fn find_owner_entries<'a>(
    entries: &'a [ProjectMcpEntry],
    controller_executable: Option<&str>,
    cloud_mcp_hostname: Option<&str>,
) -> Vec<&'a ProjectMcpEntry> {
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

/// Project the MCP servers of one project document: the `mcp.servers`
/// table plus the native legacy spelling, where any other `mcp.<name>`
/// key is itself a server (`packages/core/src/config/normalize.ts`
/// `normalizeMcp`: `type: local` with a `command` array, `type: remote`
/// with a `url`). `mcp.timeout` is the one exception with a dual native
/// spelling: a number or a timeout record normalizes as the MCP timeout
/// (never a server), while the direct legacy shape (`type: local` or
/// `type: remote`) still decodes as one server under the key `timeout`.
/// Absent or null layers contribute nothing; a non-object layer fails
/// closed. Non-object table entries are skipped; a non-record legacy
/// value fails closed, since native would still decode it as a server
/// candidate the scan could not evaluate. Enabled-only legacy values
/// (`{enabled: bool}` without `type`) are omitted exactly like the native
/// normalization omits them.
fn project_entries(document: &Value, file: String) -> Result<Vec<ProjectMcpEntry>> {
    let label = format!("OpenCode project MCP layer in {file}");
    let mcp = document.get("mcp");
    let mut entries = Vec::new();
    // A null `mcp` contributes nothing, like an absent one; any other
    // non-object `mcp` fails closed below.
    if let Some(mcp) = mcp.filter(|mcp| !mcp.is_null()) {
        let Some(table) = mcp.as_object() else {
            return Err(eliot_core::Error::new(
                "profile_error",
                format!("{label} `mcp` is not an object"),
            ));
        };
        for (name, value) in table {
            if name == "servers" {
                continue;
            }
            if name == "timeout" && !is_direct_legacy_server(value) {
                // Native normalizes this spelling as the MCP timeout, never
                // as a server; only the direct legacy shape falls through
                // to the server decode below.
                continue;
            }
            let Some(entry) = legacy_entry(name, value, &file, &label)? else {
                continue;
            };
            entries.push(entry);
        }
    }
    let Some(servers) = shared_scan::table_object(mcp.and_then(|mcp| mcp.get("servers")), &label)?
    else {
        return Ok(entries);
    };
    if is_direct_legacy_server(&Value::Object(servers.clone())) {
        // `mcp.servers` is itself one legacy server (`{type, command|url}`),
        // not a name table: native decodes it under the key `servers`.
        let value = Value::Object(servers.clone());
        if let Some(entry) = legacy_entry("servers", &value, &file, &label)? {
            entries.push(entry);
        }
        return Ok(entries);
    }
    entries.reserve(servers.len().min(MAX_PROJECT_SERVERS));
    for (name, entry) in servers {
        let Some(table) = entry.as_object() else {
            continue;
        };
        let (command, args) = project_command(table.get("command"));
        entries.push(ProjectMcpEntry {
            name: name.clone(),
            command,
            args,
            url: table.get("url").and_then(Value::as_str).map(str::to_owned),
            file: file.clone(),
        });
    }
    Ok(entries)
}

/// Whether a `mcp.servers` value is itself one legacy server
/// (`normalizeMcp` `isDirectLegacyMcp`: a record whose `type` is `local`
/// or `remote`) rather than a name table.
fn is_direct_legacy_server(value: &Value) -> bool {
    value
        .as_object()
        .and_then(|table| table.get("type"))
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "local" || kind == "remote")
}

/// Project one legacy `mcp.<name>` server value. Enabled-only values are
/// omitted (`None`, like the native diagnostic omits them); non-record
/// values fail closed with the layer named.
fn legacy_entry(
    name: &str,
    value: &Value,
    file: &str,
    label: &str,
) -> Result<Option<ProjectMcpEntry>> {
    let Some(table) = value.as_object() else {
        return Err(eliot_core::Error::new(
            "profile_error",
            format!("{label} legacy MCP entry {name:?} is not an object"),
        ));
    };
    if table.get("type").is_none() && table.get("enabled").and_then(Value::as_bool).is_some() {
        return Ok(None);
    }
    // Legacy `type: local` carries `command` as an array; older documents
    // may split `command` (string) and `args` (array) into two keys.
    let (command, args) = if matches!(table.get("command"), Some(Value::Array(_))) {
        project_command(table.get("command"))
    } else {
        shared_scan::project_string_command_args(table.get("command"), table.get("args"))
    };
    Ok(Some(ProjectMcpEntry {
        name: name.to_owned(),
        command,
        args,
        url: table.get("url").and_then(Value::as_str).map(str::to_owned),
        file: file.to_owned(),
    }))
}

/// Project one stdio command shape: a string command carries no argument
/// vector (explicitly empty); an array splits into command plus arguments,
/// with a malformed head or tail projecting as unknown (fail closed toward
/// suppression). Anything else is unknown.
fn project_command(value: Option<&Value>) -> (Option<String>, Option<Vec<String>>) {
    shared_scan::project_command_value(value)
}
