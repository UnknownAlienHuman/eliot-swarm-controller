//! One parameterized owner-MCP scanner shared by the file-scanning plugins.
//!
//! The Claude owner scan (`plugins/claude/src/owner_scan.rs`) and the
//! OpenCode project-layer scan (`plugins/opencode/src/project_mcp.rs`) do
//! the same thing over different file lists and table shapes: bounded file
//! read, JSONC-tolerant parse, entry extraction, an
//! [`eliot_core::is_owner_swarm_server`] match (already shared) and a typed
//! refusal. This module owns the bounded read, the JSONC parse, the two
//! command projections, the transport match and the refusal shape. Each
//! plugin keeps only its file list, its table adapter and its message
//! prefixes, so every refusal keeps its exact typed code and text.
//!
//! The Codex harness (`plugins/codex/src/mcp.rs`) never scans files: it
//! matches the same owner identity over the natively enumerated effective
//! `mcp_servers` tables, so there is no third file spelling to share.

use std::path::Path;

use eliot_core::{Error, Result};
use serde_json::Value;

use crate::bounded_file::{BoundedFileError, read_regular_file};

/// Bound for one owner MCP source file read. Larger, unreadable or
/// unparseable files fail the start closed instead of scanning blind.
pub const MAX_SCAN_BYTES: usize = 256 * 1024;

/// Bound for one owner MCP layer: more entries fail closed instead of
/// truncating the scan.
pub const MAX_SCAN_SERVERS: usize = 1024;

/// Bound for one projected argument vector.
pub const MAX_SCAN_ARGS: usize = 256;

/// Bound for one projected argument value.
pub const MAX_SCAN_ARG_BYTES: usize = 2048;

/// Read one owner MCP source file and parse it JSONC-tolerantly. Absent
/// files contribute nothing (`Ok(None)`); a present file must read within
/// `max_bytes` and parse, or the start fails closed with `label` named.
/// `label` already carries the plugin prefix and the origin, for example
/// `"Claude owner MCP source project .mcp.json"`.
pub fn read_jsonc_document(path: &Path, label: &str, max_bytes: usize) -> Result<Option<Value>> {
    let bytes = match read_regular_file(path, max_bytes) {
        Ok(bytes) => bytes,
        Err(BoundedFileError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(BoundedFileError::Io(_)) => {
            return Err(Error::new(
                "profile_error",
                format!("{label} could not be read"),
            ));
        }
        Err(BoundedFileError::TooLarge) => {
            return Err(Error::new(
                "profile_error",
                format!("{label} exceeds its bound"),
            ));
        }
    };
    let text = String::from_utf8(bytes)
        .map_err(|_| Error::new("profile_error", format!("{label} is not UTF-8")))?;
    let stripped =
        eliot_core::strip_jsonc_trailing_commas(&eliot_core::strip_jsonc_comments(&text));
    let document: Value = serde_json::from_str(&stripped)
        .map_err(|_| Error::new("profile_error", format!("{label} could not be parsed")))?;
    Ok(Some(document))
}

/// Project the server table of one owner source document. An absent or null
/// table contributes nothing; a non-object table fails closed with `label`
/// named.
pub fn table_object<'a>(
    table: Option<&'a Value>,
    label: &str,
) -> Result<Option<&'a serde_json::Map<String, Value>>> {
    let Some(table) = table else {
        return Ok(None);
    };
    if table.is_null() {
        return Ok(None);
    }
    table
        .as_object()
        .map(Some)
        .ok_or_else(|| Error::new("profile_error", format!("{label} is not an object")))
}

/// Fail closed when one owner layer exceeds its entry bound instead of
/// truncating the scan.
pub fn ensure_total_entries(count: usize, max: usize, overflow: &str) -> Result<()> {
    if count > max {
        return Err(Error::new("profile_error", overflow.to_owned()));
    }
    Ok(())
}

/// Project one stdio command shape with a separate `command` string and
/// `args` array (the Claude `.mcp.json`/settings table shape). A string
/// command with no `args` key carries no argument vector (explicitly empty:
/// it cannot select `swarm mcp`); an `args` array splits into the vector,
/// with a malformed vector projecting as unknown (fail closed toward
/// refusal). Anything else is unknown.
pub fn project_string_command_args(
    command: Option<&Value>,
    args: Option<&Value>,
) -> (Option<String>, Option<Vec<String>>) {
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
    if items.len() > MAX_SCAN_ARGS {
        return (Some(command.to_owned()), None);
    }
    let mut projected = Vec::with_capacity(items.len());
    for item in items {
        match item.as_str() {
            Some(arg) if arg.len() <= MAX_SCAN_ARG_BYTES => projected.push(arg.to_owned()),
            _ => return (Some(command.to_owned()), None),
        }
    }
    (Some(command.to_owned()), Some(projected))
}

/// Project one stdio command shape where `command` is a string or an array
/// (the OpenCode project table shape): a string command carries no argument
/// vector (explicitly empty); an array splits into command plus arguments,
/// with a malformed head or tail projecting as unknown (fail closed toward
/// refusal). Anything else is unknown.
pub fn project_command_value(value: Option<&Value>) -> (Option<String>, Option<Vec<String>>) {
    match value {
        None | Some(Value::Null) => (None, None),
        Some(Value::String(command)) => (Some(command.clone()), Some(Vec::new())),
        Some(Value::Array(items)) => {
            if items.len() > MAX_SCAN_ARGS + 1 {
                return (None, None);
            }
            let mut items = items.iter();
            let Some(command) = items.next() else {
                return (None, Some(Vec::new()));
            };
            let Some(command) = command.as_str() else {
                return (None, None);
            };
            let mut args = Vec::new();
            for item in items {
                match item.as_str() {
                    Some(arg) if arg.len() <= MAX_SCAN_ARG_BYTES => args.push(arg.to_owned()),
                    _ => return (Some(command.to_owned()), None),
                }
            }
            (Some(command.to_owned()), Some(args))
        }
        Some(_) => (None, None),
    }
}

/// Owner-identity match over one entry transport: a stdio entry selecting
/// `swarm mcp` without `--require-agent` (by file name or controller path)
/// or an HTTP entry on the configured cloud hostname. The Agent-scoped
/// shape (`--require-agent`) never matches.
pub fn is_owner_transport(
    command: Option<&str>,
    args: Option<&[String]>,
    url: Option<&str>,
    controller_executable: Option<&str>,
    cloud_mcp_hostname: Option<&str>,
) -> bool {
    eliot_core::is_owner_swarm_server(
        command,
        args,
        url,
        controller_executable,
        cloud_mcp_hostname,
    )
}

/// Typed refusal for an owner hit: `profile_error` naming the entry and its
/// origin. `prefix` carries the plugin wording, for example
/// `"Claude inherit MCP keeps the owner Swarm MCP server"`.
pub fn refusal_error(prefix: &str, name: &str, origin: &str) -> Error {
    Error::new("profile_error", format!("{prefix} {name:?} from {origin}"))
}
