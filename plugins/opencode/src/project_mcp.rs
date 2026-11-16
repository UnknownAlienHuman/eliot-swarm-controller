//! Workspace project MCP layer scan for the owner Swarm identity.
//!
//! The owned `serve` runs under isolated per-Agent `XDG_CONFIG_HOME` /
//! `XDG_DATA_HOME`, so the owner's global config never merges; the
//! workspace project file (`opencode.json` / `opencode.jsonc`, installed
//! 2.2.7) still merges natively. Every start therefore scans that layer
//! for owner-identity entries before the native session exists: a stdio
//! entry selecting `swarm mcp` without `--require-agent`, or an HTTP entry
//! on the configured cloud hostname. A match fails the start with the
//! entry and file named, in every policy and regardless of entry name.
//! No project or user file is ever edited.

use std::path::Path;

use eliot_core::Result;
use serde_json::Value;

use eliot_plugin::owner_scan as shared_scan;

/// Workspace project config files the installed server merges, in read
/// order. The global/user layers stay neutralized by the isolated
/// per-Agent XDG roots (verified on 2.2.7); only this layer is scanned.
pub const PROJECT_CONFIG_FILES: &[&str] = &["opencode.json", "opencode.jsonc"];

/// Bound for one project config file read: far above real configs, small
/// enough to keep the pre-spawn scan bounded. Larger, unreadable or
/// unparseable files fail the start closed instead of scanning blind.
pub const MAX_PROJECT_CONFIG_BYTES: usize = shared_scan::MAX_SCAN_BYTES;

/// Bound for one project MCP layer: far above real surfaces. More entries
/// fail the start closed instead of truncating the scan.
pub const MAX_PROJECT_SERVERS: usize = shared_scan::MAX_SCAN_SERVERS;

/// One workspace project MCP server entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMcpEntry {
    pub name: String,
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    pub url: Option<String>,
    pub file: &'static str,
}

/// Scan the workspace project layer for MCP server entries. Absent files
/// contribute nothing; a present file must read within bounds and parse
/// (JSONC: comments and trailing commas tolerated) or the start fails
/// closed with the file named.
pub fn scan_project_mcp(workspace: &Path) -> Result<Vec<ProjectMcpEntry>> {
    let mut entries = Vec::new();
    for &file in PROJECT_CONFIG_FILES {
        let path = workspace.join(file);
        let label = format!("OpenCode project MCP layer in {file}");
        let Some(document) =
            shared_scan::read_jsonc_document(&path, &label, MAX_PROJECT_CONFIG_BYTES)?
        else {
            continue;
        };
        entries.extend(project_entries(&document, file)?);
        shared_scan::ensure_total_entries(
            entries.len(),
            MAX_PROJECT_SERVERS,
            "OpenCode project MCP layer exceeds its bound",
        )?;
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(entries)
}

/// Typed refusal reason for an owner-identity project entry: names the
/// server and the project file it came from. The plugin fails the start
/// with this reason before the native session exists.
pub fn owner_refusal_reason(entry: &ProjectMcpEntry) -> String {
    *shared_scan::refusal_error(
        "OpenCode project MCP layer contributes the owner Swarm MCP server",
        &entry.name,
        entry.file,
    )
    .message
}

/// Owner-identity entries among the scanned project layer: stdio entries
/// selecting `swarm mcp` without `--require-agent` (by file name or
/// controller path) and HTTP entries on the configured cloud hostname.
/// The Agent-scoped shape (`--require-agent`) never matches.
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

/// Project the `mcp.servers` tables of one project document. Absent or
/// null layers contribute nothing; a non-object layer fails closed.
/// Non-object entries are skipped: only the documented table shape
/// (`command` string/array, `url` string) is evaluated.
fn project_entries(document: &Value, file: &'static str) -> Result<Vec<ProjectMcpEntry>> {
    let label = format!("OpenCode project MCP layer in {file}");
    let Some(servers) = shared_scan::table_object(
        document.get("mcp").and_then(|mcp| mcp.get("servers")),
        &label,
    )?
    else {
        return Ok(Vec::new());
    };
    let mut entries = Vec::with_capacity(servers.len().min(MAX_PROJECT_SERVERS));
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
            file,
        });
    }
    Ok(entries)
}

/// Project one stdio command shape: a string command carries no argument
/// vector (explicitly empty); an array splits into command plus arguments,
/// with a malformed head or tail projecting as unknown (fail closed toward
/// suppression). Anything else is unknown.
fn project_command(value: Option<&Value>) -> (Option<String>, Option<Vec<String>>) {
    shared_scan::project_command_value(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SCRATCH_SEQ: AtomicU64 = AtomicU64::new(0);

    fn scratch_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "eliot-opencode-project-mcp-{}-{}",
            std::process::id(),
            SCRATCH_SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir
    }

    fn write_project(dir: &Path, file: &str, text: &str) {
        std::fs::write(dir.join(file), text).expect("project file");
    }

    fn owner_entry(name: &str, file: &'static str) -> ProjectMcpEntry {
        ProjectMcpEntry {
            name: name.to_owned(),
            command: Some("swarm".to_owned()),
            args: Some(vec!["mcp".to_owned()]),
            url: None,
            file,
        }
    }

    #[test]
    fn scan_absent_layer_contributes_nothing() {
        let dir = scratch_dir();
        let entries = scan_project_mcp(&dir).expect("absent files scan");
        assert!(entries.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn scan_reads_documented_shapes_with_jsonc_tolerance() {
        let dir = scratch_dir();
        write_project(
            &dir,
            "opencode.jsonc",
            r#"{
              // Owner-style entry under a renamed key, plus shapes that must not match.
              "mcp": {"servers": {
                "swarm": {"type": "local", "command": ["swarm", "mcp"],},
                "agent-shape": {"command": ["swarm", "mcp", "--require-agent"],},
                "bare-string": {"command": "swarm.exe",},
                "remote": {"url": "https://mcp.example.com/mcp",},
                "other-remote": {"url": "https://other.example/mcp",},
                "shorthand": "ignored",
              },},
            }"#,
        );
        let entries = scan_project_mcp(&dir).expect("project layer scans");
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "agent-shape",
                "bare-string",
                "other-remote",
                "remote",
                "swarm"
            ]
        );
        // A string command carries no argument vector, so it can never
        // select `swarm mcp`: explicitly empty, not unknown.
        let bare = entries
            .iter()
            .find(|entry| entry.name == "bare-string")
            .expect("bare");
        assert_eq!(bare.args, Some(Vec::new()));
        let renamed = entries
            .iter()
            .find(|entry| entry.name == "swarm")
            .expect("renamed");
        assert_eq!(renamed.command.as_deref(), Some("swarm"));
        assert_eq!(renamed.args, Some(vec!["mcp".to_owned()]));
        // Unreadable shapes fail the start closed with the file named.
        write_project(&dir, "opencode.json", "{not json");
        let error = scan_project_mcp(&dir).expect_err("unparseable project file fails");
        assert_eq!(error.code, "profile_error");
        assert!(error.message.contains("opencode.json"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn owner_match_ignores_name_and_skips_agent_shape() {
        let entries = vec![
            owner_entry("swarm", "opencode.json"),
            ProjectMcpEntry {
                name: "agent-shape".to_owned(),
                command: Some("swarm".to_owned()),
                args: Some(vec!["mcp".to_owned(), "--require-agent".to_owned()]),
                url: None,
                file: "opencode.json",
            },
            ProjectMcpEntry {
                name: "cloud".to_owned(),
                command: None,
                args: None,
                url: Some("https://mcp.example.com/mcp".to_owned()),
                file: "opencode.jsonc",
            },
            ProjectMcpEntry {
                name: "plain".to_owned(),
                command: Some("search.exe".to_owned()),
                args: Some(Vec::new()),
                url: None,
                file: "opencode.json",
            },
        ];
        let hits = find_owner_entries(&entries, None, Some("mcp.example.com"));
        assert_eq!(
            hits.iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["swarm", "cloud"]
        );
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
    fn trailing_commas_drop_only_before_closers() {
        // The shared Core stripper backs project parsing; spot-check the
        // wiring through one parse plus the unit coverage in eliot-core.
        let dir = scratch_dir();
        write_project(
            &dir,
            "opencode.jsonc",
            r#"{"mcp": {"servers": {"a": {"url": "https://a.example/mcp",},},}}"#,
        );
        let entries = scan_project_mcp(&dir).expect("trailing commas parse");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "a");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn refusal_reason_names_the_server_and_file() {
        let reason = owner_refusal_reason(&owner_entry("swarm", "opencode.json"));
        assert!(reason.contains("\"swarm\""), "{reason}");
        assert!(reason.contains("opencode.json"), "{reason}");
    }

    #[test]
    fn oversize_or_unreadable_layer_fails_closed() {
        let dir = scratch_dir();
        let big = "x".repeat(MAX_PROJECT_CONFIG_BYTES + 1);
        write_project(&dir, "opencode.json", &big);
        let error = scan_project_mcp(&dir).expect_err("oversize project file fails");
        assert_eq!(error.code, "profile_error");
        std::fs::remove_dir_all(&dir).ok();
    }
}
