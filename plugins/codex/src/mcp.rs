//! Worker MCP inheritance policy for the Codex harness. Pure helpers shared
//! by the host (argv, readback) and discovery (pre-spawn enumeration); no
//! I/O here.
//!
//! Verified against installed Codex 0.167.1, read-only: `codex --help` and
//! `codex app-server --help` expose no `--ignore-user-config` or strict-MCP
//! flag for `app-server` (`exec` has `--ignore-user-config`, `app-server`
//! does not); `-c mcp_servers={}` merges with file config instead of
//! replacing it (an isolated-`CODEX_HOME` `mcp list --json` probe still
//! shows the file server). A bare `-c mcp_servers.<name>.enabled=false`
//! disables that server only when its transport lives in the user file
//! config; for names contributed by project layers, plugins or builtins the
//! bootstrap rejects the touched table (`invalid transport in
//! 'mcp_servers.<name>'`). Re-emitting the effective `command`/`url`
//! alongside `enabled=false` disables those servers cleanly. Both modes
//! therefore enumerate the worker's effective `mcp_servers` entries
//! pre-spawn: minimal disables every non-allowed server with its own
//! transport explicitly, then verifies the visible surface with the
//! installed `mcpServerStatus/list` app-server method (v2 schema:
//! `{data: [{name, runtimeStatus, ...}], nextCursor}`); inherit disables
//! every entry that carries the owner Swarm identity by transport
//! (`eliot_core::is_owner_swarm_server`: a swarm executable selecting
//! `mcp` without `--require-agent`, or a URL on the configured cloud
//! hostname), regardless of entry name or allow list, so a renamed owner
//! server never survives. An allow-listed name that matches the identity
//! is rejected at launch instead of re-admitted.

use eliot_core::{Error, McpPolicy, Result, SecretPolicy};
use serde_json::Value;
use std::collections::HashSet;

/// Bound the reported server set: names only, never tool/command payloads.
pub(super) const MAX_MCP_REPORT_SERVERS: usize = 256;
pub(super) const MAX_MCP_STATUS_PAGES: usize = 8;

/// An enumerated owner server name is addressable as a `-c` override path
/// segment only when it cannot disturb neighbouring keys. Codex splits
/// override paths on literal dots before decoding, so dotted (or otherwise
/// structured) names cannot be disabled that way.
pub(super) fn is_addressable_override_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= eliot_core::MAX_MCP_SERVER_NAME_LEN
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

/// One worker-visible native MCP server as enumerated from the effective
/// `config/read` response: the table key plus the transport the native
/// One worker-visible native MCP server as enumerated from the effective
/// `config/read` response: the table key plus the transport the native
/// bootstrap validates and the stdio argument vector the owner-identity
/// rule screens. Only `command`/`url` re-emit into spawn argv; env,
/// timeouts and tool gates merge from the owner layers and never reach
/// argv, while `args` (absent as an empty vector, unreported or malformed
/// as `None`) only feeds the identity rule and never renders.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EffectiveMcpServer {
    pub(super) name: String,
    pub(super) command: Option<String>,
    pub(super) url: Option<String>,
    pub(super) args: Option<Vec<String>>,
}

/// Bound one re-emitted MCP transport value carried in a spawn `-c`
/// override: long enough for real install paths, short enough to keep the
/// spawn argv bounded. Anything longer fails closed instead of silently
/// truncating a transport.
pub(super) const MAX_MCP_TRANSPORT_BYTES: usize = 2048;

/// Spawn-safe overrides for a minimal worker: every enumerated effective
/// server outside the allow set minus Eliot's own Agent-scoped server,
/// sorted for deterministic argv. Each entry carries its effective
/// transport because the native `-c` bootstrap validates a touched
/// `mcp_servers.<name>` table on its own. A non-allowed server that cannot
/// be addressed, has no usable transport, or would leak a known secret into
/// argv fails closed instead of silently inheriting.
pub(super) fn compute_disables(
    enumerated: &[EffectiveMcpServer],
    allow: &[String],
    own_server_name: &str,
    metadata_secrets: &[String],
    controller_executable: Option<&str>,
    cloud_mcp_hostname: Option<&str>,
) -> Result<Vec<EffectiveMcpServer>> {
    let allowed: HashSet<&str> = allow.iter().map(String::as_str).collect();
    let secrets = SecretPolicy::new(metadata_secrets.iter().cloned());
    let mut disables: Vec<EffectiveMcpServer> = Vec::new();
    for server in enumerated {
        if server.name == own_server_name {
            continue;
        }
        if allowed.contains(server.name.as_str()) {
            // An allow-listed name that carries the owner Swarm identity is
            // rejected at launch: the allow list can never re-admit the
            // owner's daemon identity, whatever the entry is named.
            if eliot_core::is_owner_swarm_server(
                server.command.as_deref(),
                server.args.as_deref(),
                server.url.as_deref(),
                controller_executable,
                cloud_mcp_hostname,
            ) {
                return Err(Error::new(
                    "profile_error",
                    format!(
                        "mcp_allow cannot include the owner Swarm MCP server {:?}",
                        server.name
                    ),
                ));
            }
            continue;
        }
        let entry = disable_entry(server, &secrets)?;
        if !disables.iter().any(|kept| kept.name == entry.name) {
            disables.push(entry);
        }
    }
    disables.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(disables)
}

/// Spawn-safe overrides for an inherit worker: every enumerated effective
/// server that carries the owner Swarm identity by transport, regardless
/// of name, minus Eliot's own Agent-scoped server, sorted for
/// deterministic argv. Each entry carries its effective transport because
/// the native `-c` bootstrap validates a touched `mcp_servers.<name>`
/// table on its own. A matching server that cannot be addressed, has no
/// usable transport, or would leak a known secret into argv fails closed
/// instead of silently inheriting the owner's daemon identity.
pub(super) fn compute_owner_disables(
    enumerated: &[EffectiveMcpServer],
    own_server_name: &str,
    controller_executable: Option<&str>,
    cloud_mcp_hostname: Option<&str>,
    metadata_secrets: &[String],
) -> Result<Vec<EffectiveMcpServer>> {
    let secrets = SecretPolicy::new(metadata_secrets.iter().cloned());
    let mut disables: Vec<EffectiveMcpServer> = Vec::new();
    for server in enumerated {
        if server.name == own_server_name {
            continue;
        }
        if !eliot_core::is_owner_swarm_server(
            server.command.as_deref(),
            server.args.as_deref(),
            server.url.as_deref(),
            controller_executable,
            cloud_mcp_hostname,
        ) {
            continue;
        }
        let entry = disable_entry(server, &secrets)?;
        if !disables.iter().any(|kept| kept.name == entry.name) {
            disables.push(entry);
        }
    }
    disables.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(disables)
}

/// One transport-carrying disable entry: an addressable name plus a usable
/// command or url transport, with no known secret in argv. Shared by the
/// minimal allow-set path and the inherit owner-identity path, so both
/// policies suppress with the same spawn-safe shape.
fn disable_entry(
    server: &EffectiveMcpServer,
    secrets: &SecretPolicy,
) -> Result<EffectiveMcpServer> {
    if !is_addressable_override_name(&server.name) {
        return Err(Error::new(
            "profile_error",
            "Codex MCP cannot suppress a native server whose name is not a plain override segment",
        ));
    }
    let command = clean_transport(server.command.as_deref())?;
    let url = clean_transport(server.url.as_deref())?;
    if command.is_none() && url.is_none() {
        return Err(Error::new(
            "profile_error",
            "Codex MCP cannot suppress a native server without a usable command or url transport",
        ));
    }
    for transport in command.iter().chain(url.iter()) {
        if secrets.contains_value(&Value::String(transport.clone())) {
            return Err(Error::new(
                "secret_in_payload",
                "Codex MCP transport carries a known secret",
            ));
        }
    }
    Ok(EffectiveMcpServer {
        name: server.name.clone(),
        command,
        url,
        args: None,
    })
}
/// Project one effective transport value for a `-c` override: absent when
/// empty, bounded and control-free otherwise. The value itself never
/// reaches the error message.
fn clean_transport(value: Option<&str>) -> Result<Option<String>> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if value.len() > MAX_MCP_TRANSPORT_BYTES || value.chars().any(char::is_control) {
        return Err(Error::new(
            "profile_error",
            "Codex minimal MCP cannot suppress a native server with an unusable transport",
        ));
    }
    Ok(Some(value.to_owned()))
}

/// Bundled Codex plugins that contribute MCP servers outside the
/// `mcp_servers` table, so transport-carrying server disables cannot
/// address them. Verified live against installed Codex 0.167.1: disabling
/// `computer-use`/`unified-computer-use` removes the `cua_repl` server from
/// the thread-scoped inventory, and `codex-app-tools` backs the `codex_app`
/// server. None of these ids contains a dot, so each takes a plain
/// `-c plugins.<id>.enabled=false` override. Candidates absent from the
/// worker's effective config are skipped (a renamed marketplace fails the
/// readback closed instead of breaking the bootstrap). Each entry also
/// names the MCP servers it is known to back, so an explicit `mcp_allow`
/// entry for the plugin or one of its servers keeps the plugin enabled.
const MCP_PLUGIN_SUPPRESSIONS: &[(&str, &[&str])] = &[
    ("computer-use@openai-bundled", &["cua_repl"]),
    ("unified-computer-use@openai-bundled", &["cua_repl"]),
    ("codex-app-tools@openai-bundled", &["codex_app"]),
];

/// Bound one effective plugin id carried in a spawn `-c` override.
pub(super) const MAX_MCP_PLUGIN_ID_BYTES: usize = 256;

/// The ChatGPT-backed Apps runtime. It never appears in `config/read`
/// `mcp_servers` (`codex mcp get codex_apps` reports no such server) and a
/// bare `-c mcp_servers.codex_apps.enabled=false` breaks the native
/// bootstrap (`invalid transport in 'mcp_servers.codex_apps'`, verified on
/// 0.167.1: the touched table has no transport to validate). Re-emitting a
/// synthetic table with a nonexistent command alongside `enabled=false` is
/// accepted, and the thread-scoped inventory reports `codex_apps` as
/// `disabled`. The command can never run: a disabled server is never
/// spawned, and if a future native ever ignored the flag the missing
/// executable fails the readback closed instead of inheriting a live
/// server.
pub(super) const CODEX_APPS_SERVER_NAME: &str = "codex_apps";

/// A plugin id is addressable as a `-c` override path segment only when it
/// cannot disturb neighbouring keys. Codex splits override paths on
/// literal dots before decoding, so ids are restricted to the observed
/// bundled shape (no dots); anything else fails the start closed instead
/// of silently inheriting its servers.
pub(super) fn is_suppressible_plugin_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_MCP_PLUGIN_ID_BYTES
        && id.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || byte == b'@' || byte == b'-' || byte == b'_'
        })
}

/// One worker-visible native plugin as enumerated from the effective
/// `config/read` response: the plugin id plus whether it is enabled.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EffectiveMcpPlugin {
    pub(super) id: String,
    pub(super) enabled: bool,
}

/// Plugin ids to suppress for a minimal worker: the known MCP-serving
/// bundled plugins that are present and enabled in the worker-visible
/// config, minus any plugin (or server it backs) the owner explicitly
/// allowed. Sorted for deterministic argv. A malformed candidate entry
/// fails the start closed instead of silently inheriting its servers.
pub(super) fn compute_plugin_disables(
    plugins: &[EffectiveMcpPlugin],
    allow: &[String],
) -> Result<Vec<String>> {
    let allowed: HashSet<&str> = allow.iter().map(String::as_str).collect();
    let mut disables: Vec<String> = Vec::new();
    for (candidate, backed) in MCP_PLUGIN_SUPPRESSIONS {
        if allowed.contains(candidate) || backed.iter().any(|server| allowed.contains(server)) {
            continue;
        }
        let Some(entry) = plugins.iter().find(|plugin| plugin.id == *candidate) else {
            continue;
        };
        if !is_suppressible_plugin_id(&entry.id) {
            return Err(Error::new(
                "profile_error",
                "Codex minimal MCP cannot suppress a native plugin whose id is not a plain override segment",
            ));
        }
        if entry.enabled && !disables.iter().any(|kept| kept == &entry.id) {
            disables.push(entry.id.clone());
        }
    }
    disables.sort();
    Ok(disables)
}

/// Synthetic suppression for the ChatGPT-backed Apps runtime (see
/// [`CODEX_APPS_SERVER_NAME`]): a transport-carrying `enabled=false`
/// entry like any enumerated disable, so the route re-emits a valid table
/// the native bootstrap accepts. Skipped when the owner explicitly allows
/// `codex_apps`.
pub(super) fn synthetic_apps_disable(allow: &[String]) -> Option<EffectiveMcpServer> {
    if allow.iter().any(|name| name == CODEX_APPS_SERVER_NAME) {
        return None;
    }
    Some(EffectiveMcpServer {
        name: CODEX_APPS_SERVER_NAME.to_owned(),
        command: Some(if cfg!(windows) {
            r"C:\eliot-minimal-suppressed-mcp-server.exe".to_owned()
        } else {
            "/eliot-minimal-suppressed-mcp-server".to_owned()
        }),
        url: None,
        args: None,
    })
}

/// Exact owner-side Swarm MCP server names that must never reach a worker in
/// either policy. They carry the owner daemon identity (the owner file config
/// entry runs plain `swarm mcp` with no `--require-agent`), while the
/// session-owned `eliot_swarm_<command>` name never equals them.
pub(super) use eliot_core::SWARM_RESERVED_SERVER_NAMES as OWNER_SWARM_SERVER_NAMES;

/// Synthetic transport-carrying disables for the exact owner Swarm names.
/// Same verified pattern as [`CODEX_APPS_SERVER_NAME`]: a nonexistent
/// command alongside `enabled=false` is accepted by the 0.167.1 bootstrap
/// and reports `disabled`, so the entry can never light up in the worker
/// inventory. Used under `inherit` (where no enumeration runs) and as a
/// backstop under `minimal` for owner entries the pre-spawn enumeration
/// cannot see. A fresh transport-carrying entry wins over the owner layers;
/// a name the owner never defined stays a disabled stub, absent from the
/// inventory like any disabled server. The disabled server is never
/// spawned, and if a future native ever ignored the flag the missing
/// executable fails the readback closed instead of inheriting a live
/// owner-identity server.
pub(super) fn synthetic_swarm_owner_disables() -> Vec<EffectiveMcpServer> {
    let command = if cfg!(windows) {
        r"C:\eliot-minimal-suppressed-mcp-server.exe".to_owned()
    } else {
        "/eliot-minimal-suppressed-mcp-server".to_owned()
    };
    OWNER_SWARM_SERVER_NAMES
        .iter()
        .map(|name| EffectiveMcpServer {
            name: (*name).to_owned(),
            command: Some(command.clone()),
            url: None,
            args: None,
        })
        .collect()
}

/// Read the frozen worker MCP policy back. Legacy snapshots without `mcp`
/// keys mean `inherit`; malformed frozen values fail instead of inheriting.
pub(super) fn policy_from_profile(
    profile: &serde_json::Map<String, Value>,
) -> Result<(McpPolicy, Vec<String>)> {
    let policy = McpPolicy::parse_snapshot(profile)
        .map_err(|error| Error::new("profile_error", format!("Codex {}", error.message)))?;
    let allow = eliot_core::parse_snapshot_allow_list(profile)
        .map_err(|error| Error::new("profile_error", format!("Codex {}", error.message)))?;
    Ok((policy, allow))
}

/// Read the frozen `mcp_cloud_hostname` snapshot field back for the
/// owner-identity HTTP arm. Absent means the daemon configured no cloud
/// hostname (or the snapshot predates it): the rule stays stdio-only.
pub(super) fn cloud_hostname_from_profile(
    profile: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    eliot_core::parse_snapshot_cloud_hostname(profile)
}

/// Secret-free effective server set for readiness reporting. `servers` is
/// the native-readback name list (sorted); `None` with `verified: false`
/// means the native inventory could not be observed, never an echo of the
/// requested set.
pub(super) struct McpReadback {
    pub(super) servers: Option<Vec<String>>,
    pub(super) verified: bool,
}

impl McpReadback {
    pub(super) fn unverified() -> Self {
        Self {
            servers: None,
            verified: false,
        }
    }

    pub(super) fn verified(mut servers: Vec<String>) -> Self {
        servers.sort();
        Self {
            servers: Some(servers),
            verified: true,
        }
    }
}

/// Secret-free effective server set parsed from one native readback page.
/// This is strict: any verification problem is an error. Callers decide the
/// policy outcome: `minimal` fails the start closed, `inherit` reports the
/// set as unknown.
pub(super) struct StatusPage {
    /// Names whose runtime status is not `disabled` (missing status counts
    /// as present: absence from the list, not a null status, is absence).
    pub(super) effective: Vec<String>,
    /// Pagination cursor for the next page, if any.
    pub(super) next_cursor: Option<String>,
    /// Every listed entry was acceptable (own, allowed, or disabled).
    pub(super) acceptable: bool,
}

pub(super) fn parse_status_page(
    value: &Value,
    allow: &[String],
    own_server_name: &str,
) -> Result<StatusPage> {
    let object = value.as_object().ok_or_else(|| {
        Error::new(
            "native_readback",
            "Codex mcpServerStatus/list response was invalid",
        )
    })?;
    let data = object
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            Error::new(
                "native_readback",
                "Codex mcpServerStatus/list response was invalid",
            )
        })?;
    let allowed: HashSet<&str> = allow.iter().map(String::as_str).collect();
    let mut effective = Vec::with_capacity(data.len());
    let mut acceptable = true;
    for entry in data {
        let entry = entry.as_object().ok_or_else(|| {
            Error::new(
                "native_readback",
                "Codex mcpServerStatus/list response was invalid",
            )
        })?;
        let name = entry
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty() && name.len() <= eliot_core::MAX_MCP_SERVER_NAME_LEN)
            .ok_or_else(|| {
                Error::new(
                    "native_readback",
                    "Codex mcpServerStatus/list response was invalid",
                )
            })?;
        let runtime_status = entry.get("runtimeStatus").and_then(Value::as_str);
        if name == own_server_name || allowed.contains(name) {
            if runtime_status != Some("disabled") {
                effective.push(name.to_owned());
            }
        } else if runtime_status == Some("disabled") {
            continue;
        } else {
            acceptable = false;
            effective.push(name.to_owned());
        }
    }
    let next_cursor = object
        .get("nextCursor")
        .and_then(Value::as_str)
        .filter(|cursor| !cursor.is_empty())
        .map(str::to_owned);
    Ok(StatusPage {
        effective,
        next_cursor,
        acceptable,
    })
}

/// Merge one parsed page into the accumulated report. Caps the total so a
/// pathological inventory cannot blow the event budget. With `enforce`, an
/// enabled server outside the allowed set fails (minimal); without it, the
/// names still merge and the caller reports the set as observed (inherit).
/// Returns the next-page cursor, if any.
pub(super) fn merge_status_page(
    accumulated: &mut Vec<String>,
    seen: &mut HashSet<String>,
    page: StatusPage,
    enforce: bool,
) -> Result<Option<String>> {
    for name in page.effective {
        if seen.insert(name.clone()) {
            if accumulated.len() >= MAX_MCP_REPORT_SERVERS {
                return Err(Error::new(
                    "native_readback",
                    "Codex MCP server inventory exceeds its bound",
                ));
            }
            accumulated.push(name);
        }
    }
    if enforce && !page.acceptable {
        return Err(Error::new(
            "profile_error",
            "Codex minimal MCP found an enabled native server outside the allowed set",
        ));
    }
    Ok(page.next_cursor)
}

/// Fail closed when the completed minimal inventory has no non-disabled own
/// Agent-scoped server entry. A disabled own server never reaches the
/// accumulated set (absence, not a null status, is absence), and an empty
/// inventory merges acceptably, so neither proves isolation: the caller runs
/// this over the completed inventory before accepting `minimal`.
pub(super) fn require_minimal_own_server(
    accumulated: &[String],
    own_server_name: &str,
) -> Result<()> {
    if accumulated.iter().any(|name| name == own_server_name) {
        Ok(())
    } else {
        Err(Error::new(
            "profile_error",
            "Codex minimal MCP inventory omits the Agent-scoped server, so isolation is unverified",
        ))
    }
}
