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
//! alongside `enabled=false` disables those servers cleanly. Minimal mode
//! therefore enumerates the worker's effective `mcp_servers` entries
//! pre-spawn and disables every non-allowed server with its own transport
//! explicitly, then verifies the visible surface with the installed
//! `mcpServerStatus/list` app-server method (v2 schema: `{data:
//! [{name, runtimeStatus, ...}], nextCursor}`).

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
/// bootstrap validates. Only `command`/`url` are projected; env, timeouts
/// and tool gates merge from the owner layers and never reach argv.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EffectiveMcpServer {
    pub(super) name: String,
    pub(super) command: Option<String>,
    pub(super) url: Option<String>,
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
) -> Result<Vec<EffectiveMcpServer>> {
    let allowed: HashSet<&str> = allow.iter().map(String::as_str).collect();
    let secrets = SecretPolicy::new(metadata_secrets.iter().cloned());
    let mut disables: Vec<EffectiveMcpServer> = Vec::new();
    for server in enumerated {
        if server.name == own_server_name || allowed.contains(server.name.as_str()) {
            continue;
        }
        if !is_addressable_override_name(&server.name) {
            return Err(Error::new(
                "profile_error",
                "Codex minimal MCP cannot suppress a native server whose name is not a plain override segment",
            ));
        }
        let command = clean_transport(server.command.as_deref())?;
        let url = clean_transport(server.url.as_deref())?;
        if command.is_none() && url.is_none() {
            return Err(Error::new(
                "profile_error",
                "Codex minimal MCP cannot suppress a native server without a usable command or url transport",
            ));
        }
        for transport in command.iter().chain(url.iter()) {
            if secrets.contains_value(&Value::String(transport.clone())) {
                return Err(Error::new(
                    "secret_in_payload",
                    "Codex minimal MCP transport carries a known secret",
                ));
            }
        }
        if !disables.iter().any(|kept| kept.name == server.name) {
            disables.push(EffectiveMcpServer {
                name: server.name.clone(),
                command,
                url,
            });
        }
    }
    disables.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(disables)
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn server(name: &str, command: Option<&str>, url: Option<&str>) -> EffectiveMcpServer {
        EffectiveMcpServer {
            name: name.to_owned(),
            command: command.map(str::to_owned),
            url: url.map(str::to_owned),
        }
    }

    #[test]
    fn disables_skip_own_and_allowed_and_sort() {
        let disables = compute_disables(
            &[
                server("owner-a", Some("a.exe"), None),
                server("eliot_swarm_x", Some("swarm.exe"), None),
                server("allowed", None, Some("https://allow.example/mcp")),
                server("owner-a", Some("a.exe"), None),
                server("owner-b", None, Some("https://b.example/mcp")),
            ],
            &["allowed".to_owned()],
            "eliot_swarm_x",
            &[],
        )
        .unwrap();
        assert_eq!(
            disables,
            vec![
                server("owner-a", Some("a.exe"), None),
                server("owner-b", None, Some("https://b.example/mcp")),
            ]
        );
        assert!(
            !compute_disables(
                &[server("plain", Some("p.exe"), None)],
                &[],
                "eliot_swarm_x",
                &[]
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn disables_carry_both_transports_when_present() {
        let disables = compute_disables(
            &[server(
                "both",
                Some("both.exe"),
                Some("https://both.example/mcp"),
            )],
            &[],
            "eliot_swarm_x",
            &[],
        )
        .unwrap();
        assert_eq!(
            disables,
            vec![server(
                "both",
                Some("both.exe"),
                Some("https://both.example/mcp")
            )]
        );
    }

    #[test]
    fn disables_fail_closed_on_unaddressable_names() {
        for name in [
            "owner.search",
            "owner/server",
            "owner server",
            "owner\"x",
            "",
            &"x".repeat(129),
        ] {
            assert!(
                compute_disables(
                    &[server(name, Some("s.exe"), None)],
                    &[],
                    "eliot_swarm_x",
                    &[]
                )
                .is_err(),
                "accepted {name:?}"
            );
        }
        assert!(is_addressable_override_name("owner-search_1"));
        assert!(!is_addressable_override_name("owner.search"));
    }

    #[test]
    fn disables_fail_closed_without_a_usable_transport() {
        // Neither transport: nothing spawn-safe to re-emit, so the start
        // must fail instead of emitting a bootstrap-breaking override.
        for entry in [
            server("notransport", None, None),
            server("empty", Some(""), Some("")),
        ] {
            let error = compute_disables(&[entry], &[], "eliot_swarm_x", &[]).unwrap_err();
            assert_eq!(error.code, "profile_error");
        }
        // Control characters or an overlong value can never be quoted into
        // a `-c` override safely.
        for entry in [
            server("control", Some("bad\nexe"), None),
            server("long", Some(&"x".repeat(MAX_MCP_TRANSPORT_BYTES + 1)), None),
        ] {
            let error = compute_disables(&[entry], &[], "eliot_swarm_x", &[]).unwrap_err();
            assert_eq!(error.code, "profile_error");
            assert!(!error.message.contains("bad"));
        }
    }

    #[test]
    fn disables_fail_closed_when_a_transport_carries_a_known_secret() {
        let error = compute_disables(
            &[server(
                "token-url",
                None,
                Some("https://example.example/mcp?key=hunter2"),
            )],
            &[],
            "eliot_swarm_x",
            &["hunter2".to_owned()],
        )
        .unwrap_err();
        assert_eq!(error.code, "secret_in_payload");
        assert!(!error.message.contains("hunter2"));
    }

    fn plugin(id: &str, enabled: bool) -> EffectiveMcpPlugin {
        EffectiveMcpPlugin {
            id: id.to_owned(),
            enabled,
        }
    }

    #[test]
    fn plugin_disables_cover_only_present_enabled_bundled_plugins() {
        let plugins = vec![
            plugin("computer-use@openai-bundled", true),
            plugin("unified-computer-use@openai-bundled", true),
            plugin("codex-app-tools@openai-bundled", false),
            plugin("browser@openai-bundled", true),
            plugin("other@openai-bundled", true),
        ];
        assert_eq!(
            compute_plugin_disables(&plugins, &[]).unwrap(),
            vec![
                "computer-use@openai-bundled".to_owned(),
                "unified-computer-use@openai-bundled".to_owned(),
            ]
        );
        // Absent candidates are skipped, never fabricated.
        assert!(compute_plugin_disables(&[], &[]).unwrap().is_empty());
        assert!(
            compute_plugin_disables(&[plugin("browser@openai-bundled", true)], &[])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn plugin_disables_respect_allow_list() {
        let plugins = vec![
            plugin("computer-use@openai-bundled", true),
            plugin("unified-computer-use@openai-bundled", true),
            plugin("codex-app-tools@openai-bundled", true),
        ];
        // Allowing a backed server keeps its plugins enabled.
        assert_eq!(
            compute_plugin_disables(&plugins, &["cua_repl".to_owned()]).unwrap(),
            vec!["codex-app-tools@openai-bundled".to_owned()]
        );
        // Allowing the plugin id itself also keeps it enabled.
        assert_eq!(
            compute_plugin_disables(&plugins, &["computer-use@openai-bundled".to_owned()]).unwrap(),
            vec![
                "codex-app-tools@openai-bundled".to_owned(),
                "unified-computer-use@openai-bundled".to_owned(),
            ]
        );
    }

    #[test]
    fn plugin_ids_fail_closed_on_unaddressable_segments() {
        assert!(is_suppressible_plugin_id("computer-use@openai-bundled"));
        for id in [
            "owner.search",
            "owner/server",
            "owner server",
            "",
            &"x".repeat(257),
        ] {
            assert!(!is_suppressible_plugin_id(id), "accepted {id:?}");
        }
    }

    #[test]
    fn synthetic_apps_disable_is_suppressible_unless_allowed() {
        let entry = synthetic_apps_disable(&[]).expect("synthetic disable");
        assert_eq!(entry.name, CODEX_APPS_SERVER_NAME);
        let command = entry.command.expect("synthetic transport");
        assert!(!command.is_empty());
        assert!(!command.chars().any(char::is_control));
        assert!(is_addressable_override_name(&entry.name));
        assert!(entry.url.is_none());
        assert!(synthetic_apps_disable(&[CODEX_APPS_SERVER_NAME.to_owned()]).is_none());
    }

    #[test]
    fn status_page_reports_nondisabled_and_flags_unexpected() {
        let page = parse_status_page(
            &json!({"data": [
                {"name": "eliot_swarm_x", "runtimeStatus": "connected"},
                {"name": "allowed", "runtimeStatus": null},
                {"name": "suppressed", "runtimeStatus": "disabled"},
            ], "nextCursor": null}),
            &["allowed".to_owned()],
            "eliot_swarm_x",
        )
        .unwrap();
        assert!(page.acceptable);
        assert_eq!(
            page.effective,
            vec!["eliot_swarm_x".to_owned(), "allowed".to_owned()]
        );
        assert!(page.next_cursor.is_none());

        let page = parse_status_page(
            &json!({"data": [{"name": "intruder", "runtimeStatus": "connected"}]}),
            &[],
            "eliot_swarm_x",
        )
        .unwrap();
        assert!(!page.acceptable);

        let page = parse_status_page(
            &json!({"data": [{"name": "quiet", "runtimeStatus": null}]}),
            &[],
            "eliot_swarm_x",
        )
        .unwrap();
        assert!(!page.acceptable);

        for value in [
            json!({}),
            json!({"data": {}}),
            json!({"data": [{"name": ""}]}),
            json!({"data": [{"runtimeStatus": "connected"}]}),
        ] {
            assert!(parse_status_page(&value, &[], "eliot_swarm_x").is_err());
        }
    }

    #[test]
    fn status_merge_caps_and_enforces_minimal() {
        let mut accumulated = Vec::new();
        let mut seen = HashSet::new();
        let page = StatusPage {
            effective: vec!["a".to_owned(), "a".to_owned()],
            next_cursor: Some("cursor".to_owned()),
            acceptable: true,
        };
        assert_eq!(
            merge_status_page(&mut accumulated, &mut seen, page, true).unwrap(),
            Some("cursor".to_owned())
        );
        assert_eq!(accumulated, vec!["a".to_owned()]);

        let page = StatusPage {
            effective: vec!["intruder".to_owned()],
            next_cursor: None,
            acceptable: false,
        };
        let error = merge_status_page(&mut accumulated, &mut seen, page, true).unwrap_err();
        assert_eq!(error.code, "profile_error");

        // Without enforcement the names still merge for inherit reporting.
        let page = StatusPage {
            effective: vec!["intruder".to_owned()],
            next_cursor: None,
            acceptable: false,
        };
        assert!(
            merge_status_page(&mut accumulated, &mut seen, page, false)
                .unwrap()
                .is_none()
        );
        assert!(accumulated.contains(&"intruder".to_owned()));
    }

    #[test]
    fn policy_reads_frozen_snapshot_with_inherit_default() {
        let empty = serde_json::Map::new();
        let (policy, allow) = policy_from_profile(&empty).unwrap();
        assert_eq!(policy, McpPolicy::Inherit);
        assert!(allow.is_empty());

        let snapshot = json!({"mcp": "minimal", "mcp_allow": ["one"]});
        let (policy, allow) = policy_from_profile(snapshot.as_object().unwrap()).unwrap();
        assert_eq!(policy, McpPolicy::Minimal);
        assert_eq!(allow, vec!["one".to_owned()]);

        let broken = json!({"mcp": "isolated"});
        assert!(policy_from_profile(broken.as_object().unwrap()).is_err());
    }

    #[test]
    fn minimal_verification_requires_own_server_present() {
        assert!(require_minimal_own_server(&["eliot_swarm_x".to_owned()], "eliot_swarm_x").is_ok());
        for inventory in [Vec::new(), vec!["owner-a".to_owned()]] {
            let error = require_minimal_own_server(&inventory, "eliot_swarm_x").unwrap_err();
            assert_eq!(error.code, "profile_error");
        }
        // A disabled own server never reaches the accumulated set, and an
        // empty inventory merges acceptably, so the completed inventory still
        // fails closed here.
        let disabled_own = parse_status_page(
            &json!({"data": [{"name": "eliot_swarm_x", "runtimeStatus": "disabled"}]}),
            &[],
            "eliot_swarm_x",
        )
        .unwrap();
        assert!(disabled_own.acceptable);
        assert!(disabled_own.effective.is_empty());
        assert!(require_minimal_own_server(&disabled_own.effective, "eliot_swarm_x").is_err());

        let empty = parse_status_page(&json!({"data": []}), &[], "eliot_swarm_x").unwrap();
        assert!(empty.acceptable);
        assert!(require_minimal_own_server(&empty.effective, "eliot_swarm_x").is_err());
    }

    #[test]
    fn status_page_extracts_names_only_at_observed_inventory_scale() {
        // Live-observed 2026-11-14: a minimal-policy `mcpServerStatus/list`
        // (`toolsAndAuthOnly`) response reached 1,990,613 bytes. The parser
        // keeps names only; the embedded tool catalogs that size the frame
        // must never reach the accumulated report.
        let big_tool = json!({
            "name": "search",
            "description": "x".repeat(1_500_000),
            "inputSchema": {"type": "object", "padding": "y".repeat(600_000)},
        });
        let response = json!({"data": [
            {
                "name": "eliot_swarm_x",
                "runtimeStatus": "connected",
                "authStatus": "unknown",
                "resources": [],
                "resourceTemplates": [],
                "tools": {"search": big_tool},
            },
            {"name": "owner-a", "runtimeStatus": "disabled", "authStatus": "unknown",
             "resources": [], "resourceTemplates": [], "tools": {"search": {"name": "search", "inputSchema": {}}}},
        ]});
        let wire_len = serde_json::to_vec(&response)
            .expect("response encodes")
            .len();
        assert!(
            wire_len >= 1_990_613,
            "test payload under observed scale: {wire_len}"
        );
        let page = parse_status_page(&response, &[], "eliot_swarm_x").expect("names-only parse");
        assert!(page.acceptable);
        assert_eq!(page.effective, vec!["eliot_swarm_x".to_owned()]);
        let accumulated_len = serde_json::to_vec(&page.effective)
            .expect("names encode")
            .len();
        assert!(
            accumulated_len < 1024,
            "tool payload leaked: {accumulated_len}"
        );
    }
}
