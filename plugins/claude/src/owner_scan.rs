//! Owner MCP source scan for the Claude `inherit` policy.
//!
//! `inherit` keeps Claude Code's normal MCP precedence, and the installed
//! 2.1.325 CLI offers no per-server disable or deny switch for the
//! per-run config surface: `claude --help` shows only the all-or-nothing
//! `--strict-mcp-config` ("Only use MCP servers from `--mcp-config`,
//! ignoring all other MCP configurations", reserved for `minimal`),
//! `--setting-sources` selects whole settings sources (user, project,
//! local) rather than servers, and `claude mcp --help` shows no deny
//! list. Using `--setting-sources` to drop sources would also drop the
//! owner's legitimate surface that `inherit` explicitly opts into, and
//! `.mcp.json` project servers sit outside the settings sources entirely;
//! the per-project disable state (`mcp reset-project-choices`) is
//! interactive approval state, not a per-run mechanism Eliot can set.
//! So there is no safe native filter for `inherit`: when an owner Swarm
//! entry is reachable under `inherit`, the start fails closed with the
//! entry and source named, in the same way `minimal` relies on the strict
//! config. The Agent-scoped entry Eliot generates is the only Swarm server
//! a worker gets.
//!
//! Scanned sources, in read order: the workspace `.mcp.json` project
//! servers, the workspace `.claude/settings.json` and
//! `.claude/settings.local.json` project/local settings, and the user
//! `~/.claude/settings.json` plus legacy `~/.claude.json` (both carry an
//! `mcpServers` table on this machine, verified 2026-11-16). Missing files
//! contribute nothing; a present file must read within bounds and parse
//! (JSONC tolerated) or the start fails closed with the source named. No
//! file is ever edited.

use std::path::{Path, PathBuf};

use eliot_core::{Error, Result};
use serde_json::Value;

/// Bound for one owner MCP source file read. Larger, unreadable or
/// unparseable files fail the start closed instead of scanning blind.
pub const MAX_OWNER_SCAN_BYTES: usize = 256 * 1024;

/// Bound for one owner MCP source: more entries fail closed instead of
/// truncating the scan.
pub const MAX_OWNER_SERVERS: usize = 1024;

/// One owner-side MCP server entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerMcpEntry {
    pub name: String,
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    pub url: Option<String>,
    pub source: &'static str,
}

/// Scan the owner MCP sources for server entries with an explicit home
/// root (tests pin a scratch home; production passes [`owner_home`]). See
/// the module docs for the source list and the fail-closed read contract.
/// Owner home for the user-level sources: `%USERPROFILE%`, else `$HOME`.
/// `None` skips the user files without failing the scan.
fn owner_home() -> Option<PathBuf> {
    std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(PathBuf::from)
        .filter(|home| !home.as_os_str().is_empty())
}

/// Scan with an explicit home root (tests pin a scratch home; production
/// passes [`owner_home`]).
fn scan_owner_mcp_sources_with_home(
    workspace: &Path,
    home: Option<PathBuf>,
) -> Result<Vec<OwnerMcpEntry>> {
    let mut entries = Vec::new();
    entries.extend(read_source(
        &workspace.join(".mcp.json"),
        "project .mcp.json",
    )?);
    entries.extend(read_source(
        &workspace.join(".claude").join("settings.json"),
        "project settings",
    )?);
    entries.extend(read_source(
        &workspace.join(".claude").join("settings.local.json"),
        "project local settings",
    )?);
    if let Some(home) = home.as_ref() {
        entries.extend(read_source(
            &home.join(".claude").join("settings.json"),
            "user settings",
        )?);
        entries.extend(read_source(&home.join(".claude.json"), "user config")?);
    }
    if entries.len() > MAX_OWNER_SERVERS {
        return Err(Error::new(
            "profile_error",
            "Claude owner MCP sources exceed their bound",
        ));
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(entries)
}

/// Owner-identity entries among the scanned sources: stdio entries
/// selecting `swarm mcp` without `--require-agent` (by file name or
/// controller path) and HTTP entries on the configured cloud hostname.
/// The Agent-scoped shape (`--require-agent`) never matches.
pub fn find_owner_entries<'a>(
    entries: &'a [OwnerMcpEntry],
    controller_executable: Option<&str>,
    cloud_mcp_hostname: Option<&str>,
) -> Vec<&'a OwnerMcpEntry> {
    entries
        .iter()
        .filter(|entry| {
            eliot_core::is_owner_swarm_server(
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
    controller_executable: &Path,
    cloud_mcp_hostname: Option<&str>,
) -> Result<()> {
    refuse_owner_mcp_sources_with_home(
        workspace,
        owner_home(),
        controller_executable,
        cloud_mcp_hostname,
    )
}

/// Refusal with an explicit home root (tests pin a scratch home).
fn refuse_owner_mcp_sources_with_home(
    workspace: &Path,
    home: Option<PathBuf>,
    controller_executable: &Path,
    cloud_mcp_hostname: Option<&str>,
) -> Result<()> {
    let entries = scan_owner_mcp_sources_with_home(workspace, home)?;
    let hits = find_owner_entries(&entries, controller_executable.to_str(), cloud_mcp_hostname);
    if let Some(hit) = hits.first() {
        return Err(Error::new(
            "profile_error",
            format!(
                "Claude inherit MCP keeps the owner Swarm MCP server {:?} from {}",
                hit.name, hit.source
            ),
        ));
    }
    Ok(())
}

/// Read one owner MCP source file. Absent files contribute nothing.
fn read_source(path: &Path, source: &'static str) -> Result<Vec<OwnerMcpEntry>> {
    let bytes = match std::fs::read(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(_) => {
            return Err(Error::new(
                "profile_error",
                format!("Claude owner MCP source {source} could not be read"),
            ));
        }
        Ok(bytes) => bytes,
    };
    if bytes.len() > MAX_OWNER_SCAN_BYTES {
        return Err(Error::new(
            "profile_error",
            format!("Claude owner MCP source {source} exceeds its bound"),
        ));
    }
    let text = String::from_utf8(bytes).map_err(|_| {
        Error::new(
            "profile_error",
            format!("Claude owner MCP source {source} is not UTF-8"),
        )
    })?;
    let stripped =
        eliot_core::strip_jsonc_trailing_commas(&eliot_core::strip_jsonc_comments(&text));
    let document: Value = serde_json::from_str(&stripped).map_err(|_| {
        Error::new(
            "profile_error",
            format!("Claude owner MCP source {source} could not be parsed"),
        )
    })?;
    source_entries(&document, source)
}

/// Project the `mcpServers` table of one owner source. Absent or null
/// tables contribute nothing; a non-object table fails closed.
/// Non-object entries are skipped: only the documented table shape
/// (`command` string, `args` array, `url` string) is evaluated.
fn source_entries(document: &Value, source: &'static str) -> Result<Vec<OwnerMcpEntry>> {
    let Some(table) = document.get("mcpServers") else {
        return Ok(Vec::new());
    };
    if table.is_null() {
        return Ok(Vec::new());
    }
    let table = table.as_object().ok_or_else(|| {
        Error::new(
            "profile_error",
            format!("Claude owner MCP source {source} is not an object"),
        )
    })?;
    let mut entries = Vec::with_capacity(table.len().min(MAX_OWNER_SERVERS));
    for (name, entry) in table {
        let Some(object) = entry.as_object() else {
            continue;
        };
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
    const MAX_ARGS: usize = 256;
    const MAX_ARG_BYTES: usize = 2048;
    let Some(command) = command.and_then(Value::as_str) else {
        return (None, None);
    };
    let Some(args) = args else {
        return (Some(command.to_owned()), Some(Vec::new()));
    };
    if args.is_null() {
        return (Some(command.to_owned()), Some(Vec::new()));
    }
    let Some(items) = args.as_array() else {
        return (Some(command.to_owned()), None);
    };
    if items.len() > MAX_ARGS {
        return (Some(command.to_owned()), None);
    }
    let mut projected = Vec::with_capacity(items.len());
    for item in items {
        match item.as_str() {
            Some(arg) if arg.len() <= MAX_ARG_BYTES => projected.push(arg.to_owned()),
            _ => return (Some(command.to_owned()), None),
        }
    }
    (Some(command.to_owned()), Some(projected))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SCRATCH_SEQ: AtomicU64 = AtomicU64::new(0);

    fn scratch_pair() -> (PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "eliot-claude-owner-scan-{}-{}",
            std::process::id(),
            SCRATCH_SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        let workspace = root.join("workspace");
        let home = root.join("home");
        std::fs::create_dir_all(workspace.join(".claude")).expect("workspace");
        std::fs::create_dir_all(home.join(".claude")).expect("home");
        (workspace, home)
    }

    fn write(path: &Path, text: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("parent");
        }
        std::fs::write(path, text).expect("source file");
    }

    #[test]
    fn scan_absent_sources_contributes_nothing() {
        let (workspace, home) = scratch_pair();
        let entries = scan_owner_mcp_sources_with_home(&workspace, Some(home)).expect("scan");
        assert!(entries.is_empty());
    }

    #[test]
    fn scan_reads_mcp_servers_shapes_with_jsonc_tolerance() {
        let (workspace, home) = scratch_pair();
        write(
            &workspace.join(".mcp.json"),
            r#"{
              // Renamed owner entry plus shapes that must not match.
              "mcpServers": {
                "swarm": {"command": "swarm", "args": ["mcp"],},
                "agent-shape": {"command": "swarm", "args": ["mcp", "--require-agent"],},
                "bare": {"command": "swarm.exe",},
                "remote": {"url": "https://mcp.example.com/mcp",},
              },
            }"#,
        );
        write(
            &home.join(".claude.json"),
            r#"{"mcpServers": {"user-search": {"command": "search.exe", "args": []}}}"#,
        );
        let entries =
            scan_owner_mcp_sources_with_home(&workspace, Some(home.clone())).expect("scan");
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["agent-shape", "bare", "remote", "swarm", "user-search"]
        );
        // A string command with no `args` key carries no vector: it can
        // never select `swarm mcp`, so it is explicitly empty, not unknown.
        let bare = entries
            .iter()
            .find(|entry| entry.name == "bare")
            .expect("bare");
        assert_eq!(bare.args, Some(Vec::new()));
        let renamed = entries
            .iter()
            .find(|entry| entry.name == "swarm")
            .expect("renamed");
        assert_eq!(renamed.source, "project .mcp.json");
        // Unparseable sources fail the start closed with the source named.
        write(
            &workspace.join(".claude").join("settings.json"),
            "{not json",
        );
        let error = scan_owner_mcp_sources_with_home(&workspace, Some(home))
            .expect_err("unparseable source fails");
        assert_eq!(error.code, "profile_error");
        assert!(error.message.contains("project settings"));
    }

    #[test]
    fn owner_match_ignores_name_and_skips_agent_shape() {
        let entries = vec![
            OwnerMcpEntry {
                name: "swarm".to_owned(),
                command: Some("swarm".to_owned()),
                args: Some(vec!["mcp".to_owned()]),
                url: None,
                source: "project .mcp.json",
            },
            OwnerMcpEntry {
                name: "agent-shape".to_owned(),
                command: Some("swarm".to_owned()),
                args: Some(vec!["mcp".to_owned(), "--require-agent".to_owned()]),
                url: None,
                source: "project .mcp.json",
            },
            OwnerMcpEntry {
                name: "cloud".to_owned(),
                command: None,
                args: None,
                url: Some("https://mcp.example.com/mcp".to_owned()),
                source: "user config",
            },
        ];
        let hits = find_owner_entries(&entries, None, Some("mcp.example.com"));
        assert_eq!(
            hits.iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["swarm", "cloud"]
        );
        // Workspace entries keep their source kinds for the refusal message.
        assert_eq!(hits[0].source, "project .mcp.json");
        // Without a configured hostname the HTTP arm stays inert.
        let hits = find_owner_entries(&entries, None, None);
        assert_eq!(
            hits.iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["swarm"]
        );
    }

    #[test]
    fn refusal_names_the_server_and_source() {
        let (workspace, home) = scratch_pair();
        write(
            &workspace.join(".mcp.json"),
            r#"{"mcpServers": {"swarm": {"command": "C:\\Tools\\Eliot\\swarm.exe", "args": ["mcp"]}}}"#,
        );
        let error = refuse_owner_mcp_sources_with_home(
            &workspace,
            Some(home),
            Path::new(r"C:\Tools\Eliot\other.exe"),
            None,
        )
        .expect_err("refused");
        assert_eq!(error.code, "profile_error");
        assert!(error.message.contains("\"swarm\""));
        assert!(error.message.contains("project .mcp.json"));
        // A clean layer passes.
        let (workspace, home) = scratch_pair();
        refuse_owner_mcp_sources_with_home(&workspace, Some(home), Path::new("swarm"), None)
            .expect("clean layer passes");
    }
}
