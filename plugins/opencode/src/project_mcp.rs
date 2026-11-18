//! Workspace project MCP layer scan for the owner Swarm identity.
//!
//! The owned `serve` runs under isolated per-Agent `XDG_CONFIG_HOME` /
//! `XDG_DATA_HOME`, so the owner's global config never merges; the
//! workspace-inherited layers still merge natively (installed 2.2.7): each
//! ancestor directory's direct `opencode.json` / `opencode.jsonc` files
//! plus its `.opencode/` project directory, each read as a native document
//! (the `mcp.servers` table plus the legacy `mcp.<name>` server spelling).
//! Every start therefore scans those layers for owner-identity entries
//! before the native session exists: a stdio entry selecting `swarm mcp`
//! without `--require-agent`, or an HTTP entry on the configured cloud
//! hostname. A match fails the start with the entry and layer named, in
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

/// Project the MCP servers of one project document: the `mcp.servers`
/// table plus the native legacy spelling, where any other `mcp.<name>`
/// key is itself a server (`packages/core/src/config/normalize.ts`
/// `normalizeMcp`: `type: local` with a `command` array, `type: remote`
/// with a `url`). Absent or null layers contribute nothing; a non-object
/// layer fails closed. Non-object table entries are skipped; a non-record
/// legacy value fails closed, since native would still decode it as a
/// server candidate the scan could not evaluate. Enabled-only legacy
/// values (`{enabled: bool}` without `type`) are omitted exactly like the
/// native normalization omits them.
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
            if name == "servers" || name == "timeout" {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SCRATCH_SEQ: AtomicU64 = AtomicU64::new(0);

    /// Owned scratch fixture: the directory is removed on unwind, so a
    /// failed assertion never leaks it into the shared temp directory.
    struct ScratchDir(PathBuf);

    impl ScratchDir {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "eliot-opencode-project-mcp-{}-{}",
                std::process::id(),
                SCRATCH_SEQ.fetch_add(1, Ordering::SeqCst)
            ));
            std::fs::create_dir_all(&dir).expect("scratch dir");
            Self(dir)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn write_project(dir: &Path, file: &str, text: &str) {
        std::fs::write(dir.join(file), text).expect("project file");
    }

    fn owner_entry(name: &str, file: &str) -> ProjectMcpEntry {
        ProjectMcpEntry {
            name: name.to_owned(),
            command: Some("swarm".to_owned()),
            args: Some(vec!["mcp".to_owned()]),
            url: None,
            file: file.to_owned(),
        }
    }

    fn owner_doc(name: &str) -> String {
        format!(r#"{{"mcp": {{"servers": {{"{name}": {{"command": ["swarm", "mcp"]}}}}}}}}"#)
    }

    #[test]
    fn scan_absent_layer_contributes_nothing() {
        let scratch = ScratchDir::new();
        let dir = scratch.path().to_path_buf();
        let entries = scan_project_mcp(&dir).expect("absent files scan");
        assert!(entries.is_empty());
    }

    #[test]
    fn scan_reads_documented_shapes_with_jsonc_tolerance() {
        let scratch = ScratchDir::new();
        let dir = scratch.path().to_path_buf();
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
    }

    #[test]
    fn scan_covers_ancestor_and_dot_opencode_layers() {
        // The owned serve merges ancestor direct files and `.opencode/`
        // project directories, so an owner-identity entry in either must
        // refuse the start with the layer named.
        let scratch = ScratchDir::new();
        let root = scratch.path().to_path_buf();
        let workspace = root.join("a").join("b");
        std::fs::create_dir_all(&workspace).expect("workspace");
        std::fs::create_dir_all(root.join(".opencode")).expect("project dir");
        write_project(&root, "opencode.json", &owner_doc("ancestor-direct"));
        write_project(
            &root.join(".opencode"),
            "opencode.jsonc",
            &owner_doc("ancestor-project"),
        );
        let entries = scan_project_mcp(&workspace).expect("layers scan");
        let names: Vec<&str> = entries.iter().map(|entry| entry.name.as_str()).collect();
        assert!(names.contains(&"ancestor-direct"), "{names:?}");
        assert!(names.contains(&"ancestor-project"), "{names:?}");
        let direct = entries
            .iter()
            .find(|entry| entry.name == "ancestor-direct")
            .expect("direct");
        assert_eq!(direct.file, "../../opencode.json");
        let project = entries
            .iter()
            .find(|entry| entry.name == "ancestor-project")
            .expect("project");
        assert_eq!(project.file, "../../.opencode/opencode.jsonc");
        // A workspace-level entry keeps its bare file label.
        write_project(&workspace, "opencode.jsonc", &owner_doc("here"));
        let entries = scan_project_mcp(&workspace).expect("workspace scan");
        let here = entries
            .iter()
            .find(|entry| entry.name == "here")
            .expect("workspace");
        assert_eq!(here.file, "opencode.jsonc");
        // Every owner-identity hit refuses, whatever the layer or name.
        let hits = find_owner_entries(&entries, None, None);
        assert_eq!(hits.len(), 3, "{entries:?}");
        let reason = owner_refusal_reason(hits[0]);
        assert!(
            reason.contains("../") || reason.contains(".opencode"),
            "{reason}"
        );
    }

    #[test]
    fn ancestor_walk_stops_at_the_filesystem_root() {
        // The walk ends at the root instead of climbing past it; a deep
        // workspace still scans its nearest layers.
        let scratch = ScratchDir::new();
        let mut workspace = scratch.path().to_path_buf();
        for level in 0..8 {
            workspace = workspace.join(format!("l{level}"));
        }
        std::fs::create_dir_all(&workspace).expect("workspace");
        write_project(&workspace, "opencode.json", &owner_doc("deep"));
        let entries = scan_project_mcp(&workspace).expect("deep scan");
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_eq!(entries[0].file, "opencode.json");
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
                file: "opencode.json".to_owned(),
            },
            ProjectMcpEntry {
                name: "cloud".to_owned(),
                command: None,
                args: None,
                url: Some("https://mcp.example.com/mcp".to_owned()),
                file: "opencode.jsonc".to_owned(),
            },
            ProjectMcpEntry {
                name: "plain".to_owned(),
                command: Some("search.exe".to_owned()),
                args: Some(Vec::new()),
                url: None,
                file: "opencode.json".to_owned(),
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
        let scratch = ScratchDir::new();
        let dir = scratch.path().to_path_buf();
        write_project(
            &dir,
            "opencode.jsonc",
            r#"{"mcp": {"servers": {"a": {"url": "https://a.example/mcp",},},}}"#,
        );
        let entries = scan_project_mcp(&dir).expect("trailing commas parse");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "a");
    }

    #[test]
    fn refusal_reason_names_the_server_and_file() {
        let reason = owner_refusal_reason(&owner_entry("swarm", "opencode.json"));
        assert!(reason.contains("\"swarm\""), "{reason}");
        assert!(reason.contains("opencode.json"), "{reason}");
    }

    #[test]
    fn oversize_or_unreadable_layer_fails_closed() {
        let scratch = ScratchDir::new();
        let dir = scratch.path().to_path_buf();
        let big = "x".repeat(MAX_PROJECT_CONFIG_BYTES + 1);
        write_project(&dir, "opencode.json", &big);
        let error = scan_project_mcp(&dir).expect_err("oversize project file fails");
        assert_eq!(error.code, "profile_error");
    }

    #[test]
    fn legacy_mcp_spelling_scans_like_a_server() {
        let scratch = ScratchDir::new();
        let dir = scratch.path().to_path_buf();
        write_project(
            &dir,
            "opencode.jsonc",
            r#"{
              // Native legacy spelling: any mcp.<name> key is a server.
              "mcp": {
                "bridge": {"type": "local", "command": ["swarm", "mcp"]},
                "old-split": {"command": "npm", "args": ["run", "server"]},
                "far": {"type": "remote", "url": "https://mcp.example.com/mcp"},
                "quiet": {"enabled": true},
                "timeout": {"catalog": 1000}
              }
            }"#,
        );
        let entries = scan_project_mcp(&dir).expect("legacy layer scans");
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["bridge", "far", "old-split"]
        );
        let bridge = entries
            .iter()
            .find(|entry| entry.name == "bridge")
            .expect("bridge");
        assert_eq!(bridge.command.as_deref(), Some("swarm"));
        assert_eq!(bridge.args, Some(vec!["mcp".to_owned()]));
        let split = entries
            .iter()
            .find(|entry| entry.name == "old-split")
            .expect("split");
        assert_eq!(split.command.as_deref(), Some("npm"));
        assert_eq!(
            split.args,
            Some(vec!["run".to_owned(), "server".to_owned()])
        );
        // The legacy owner transport matches regardless of its key.
        let hits = find_owner_entries(&entries, None, None);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "bridge");
    }

    #[test]
    fn legacy_servers_value_decodes_as_one_server() {
        let scratch = ScratchDir::new();
        let dir = scratch.path().to_path_buf();
        write_project(
            &dir,
            "opencode.json",
            r#"{"mcp": {"servers": {"type": "local", "command": ["swarm", "mcp"]}}}"#,
        );
        let entries = scan_project_mcp(&dir).expect("direct legacy scans");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "servers");
        assert_eq!(entries[0].command.as_deref(), Some("swarm"));
    }

    #[test]
    fn non_record_legacy_value_fails_closed() {
        let scratch = ScratchDir::new();
        let dir = scratch.path().to_path_buf();
        write_project(&dir, "opencode.json", r#"{"mcp": {"x": 5}}"#);
        let error = scan_project_mcp(&dir).expect_err("non-record legacy fails");
        assert_eq!(error.code, "profile_error");
        write_project(&dir, "opencode.json", r#"{"mcp": "servers"}"#);
        let error = scan_project_mcp(&dir).expect_err("non-object mcp fails");
        assert_eq!(error.code, "profile_error");
    }
}
