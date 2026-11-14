//! Worker MCP inheritance policy for the Codex harness. Pure helpers shared
//! by the host (argv, readback) and discovery (pre-spawn enumeration); no
//! I/O here.
//!
//! Verified against installed Codex 0.167.1, read-only: `codex --help` and
//! `codex app-server --help` expose no `--ignore-user-config` or strict-MCP
//! flag for `app-server` (`exec` has `--ignore-user-config`, `app-server`
//! does not); `-c mcp_servers={}` merges with file config instead of
//! replacing it (an isolated-`CODEX_HOME` `mcp list --json` probe still
//! shows the file server), while `-c mcp_servers.<name>.enabled=false`
//! disables that server. Minimal mode therefore enumerates the worker's
//! effective `mcp_servers` keys pre-spawn and disables every non-allowed
//! server explicitly, then verifies the visible surface with the installed
//! `mcpServerStatus/list` app-server method (v2 schema: `{data:
//! [{name, runtimeStatus, ...}], nextCursor}`).

use eliot_core::{Error, McpPolicy, Result};
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

/// Names the worker must suppress: enumerated effective servers minus the
/// allowed set minus Eliot's own Agent-scoped server. Sorted for
/// deterministic argv. A non-allowed name that cannot be addressed fails
/// closed instead of silently inheriting.
pub(super) fn compute_disables(
    enumerated: &[String],
    allow: &[String],
    own_server_name: &str,
) -> Result<Vec<String>> {
    let allowed: HashSet<&str> = allow.iter().map(String::as_str).collect();
    let mut disables: Vec<String> = Vec::new();
    for name in enumerated {
        if name == own_server_name || allowed.contains(name.as_str()) {
            continue;
        }
        if !is_addressable_override_name(name) {
            return Err(Error::new(
                "profile_error",
                "Codex minimal MCP cannot suppress a native server whose name is not a plain override segment",
            ));
        }
        if !disables.iter().any(|kept| kept == name) {
            disables.push(name.clone());
        }
    }
    disables.sort();
    Ok(disables)
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

    #[test]
    fn disables_skip_own_and_allowed_and_sort() {
        let disables = compute_disables(
            &[
                "owner-a".to_owned(),
                "eliot_swarm_x".to_owned(),
                "allowed".to_owned(),
                "owner-a".to_owned(),
                "owner-b".to_owned(),
            ],
            &["allowed".to_owned()],
            "eliot_swarm_x",
        )
        .unwrap();
        assert_eq!(disables, vec!["owner-a".to_owned(), "owner-b".to_owned()]);
        assert!(
            !compute_disables(&["plain".to_owned()], &[], "eliot_swarm_x")
                .unwrap()
                .is_empty()
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
                compute_disables(&[name.to_owned()], &[], "eliot_swarm_x").is_err(),
                "accepted {name:?}"
            );
        }
        assert!(is_addressable_override_name("owner-search_1"));
        assert!(!is_addressable_override_name("owner.search"));
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
