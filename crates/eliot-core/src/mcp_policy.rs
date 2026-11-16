//! Frozen worker MCP inheritance policy shared by the daemon config loader
//! and the harness plugins. Pure validation; no I/O.
//!
//! Owning contract: `docs/spec/configuration.md` worker MCP policy. A profile
//! selects `minimal` (the default: only Eliot's Agent-scoped server plus an
//! explicit allow list) or `inherit` (explicit opt-in: the worker keeps the
//! owner's native MCP surface plus Eliot's Agent-scoped server). The daemon
//! freezes the resolved policy with the profile snapshot; each harness plugin
//! enforces and reports it. Owner-side Swarm MCP entries are reserved in
//! every policy: a worker that talks to the owner's daemon with the owner's
//! identity is an authority leak, so Swarm names can never be allow-listed.

use crate::{Error, Result};

/// Keep the owner's native MCP surface plus Eliot's Agent-scoped server.
pub const MCP_POLICY_INHERIT: &str = "inherit";
/// Only Eliot's Agent-scoped server plus the explicit allow list.
pub const MCP_POLICY_MINIMAL: &str = "minimal";
/// Bound matching the daemon's other per-profile entry lists.
pub const MAX_MCP_ALLOW_ENTRIES: usize = 64;
/// Bound matching the daemon's other per-profile name fields.
pub const MAX_MCP_SERVER_NAME_LEN: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum McpPolicy {
    Inherit,
    Minimal,
}

impl McpPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inherit => MCP_POLICY_INHERIT,
            Self::Minimal => MCP_POLICY_MINIMAL,
        }
    }

    /// `None` (unset profile field) means the default `minimal`. Any other
    /// unknown value is rejected at config load. Frozen snapshots without
    /// the key keep their recorded meaning (see `parse_snapshot`); only live
    /// profile parsing defaults here.
    pub fn parse(raw: Option<&str>) -> Result<Self> {
        match raw.map(str::trim) {
            None => Ok(Self::Minimal),
            Some(MCP_POLICY_INHERIT) => Ok(Self::Inherit),
            Some(MCP_POLICY_MINIMAL) => Ok(Self::Minimal),
            Some(_) => Err(Error::new(
                "profile_error",
                "mcp must be inherit or minimal",
            )),
        }
    }

    /// Read the frozen snapshot fields back. Snapshots predating the policy
    /// (no `mcp` key) keep their recorded meaning, `inherit`; malformed
    /// frozen values fail instead of silently inheriting. New freezes always
    /// record the key explicitly, so this legacy read never mints `minimal`.
    pub fn parse_snapshot(profile: &serde_json::Map<String, serde_json::Value>) -> Result<Self> {
        match profile.get("mcp") {
            None | Some(serde_json::Value::Null) => Ok(Self::Inherit),
            Some(serde_json::Value::String(raw)) => Self::parse(Some(raw)),
            Some(_) => Err(Error::new(
                "profile_error",
                "frozen profile mcp policy must be inherit or minimal",
            )),
        }
    }
}

/// Validate an explicit allow list of native MCP server names. Trims each
/// entry; rejects empty, overlong, control-bearing and duplicated names.
/// The returned names keep file order for deterministic snapshots.
pub fn validate_allow_list(raw: &[String]) -> Result<Vec<String>> {
    if raw.len() > MAX_MCP_ALLOW_ENTRIES {
        return Err(Error::new("profile_error", "mcp_allow exceeds 64 entries"));
    }
    let mut names = Vec::with_capacity(raw.len());
    for entry in raw {
        let name = entry.trim();
        if name.is_empty()
            || name.len() > MAX_MCP_SERVER_NAME_LEN
            || name.chars().any(char::is_control)
        {
            return Err(Error::new(
                "profile_error",
                "mcp_allow names must be non-empty, at most 128 bytes and contain no controls",
            ));
        }
        if is_swarm_reserved_server_name(name) {
            return Err(Error::new(
                "profile_error",
                "mcp_allow cannot include a Swarm MCP server",
            ));
        }
        if names.iter().any(|kept| *kept == name) {
            return Err(Error::new(
                "profile_error",
                "mcp_allow contains a duplicate server name",
            ));
        }
        names.push(name.to_owned());
    }
    Ok(names)
}

/// Owner-side Swarm MCP server names that must never reach a worker in any
/// policy. The exact owner entries (`eliot_swarm`, `eliot-swarm`) carry the
/// owner's daemon identity; the prefixed session names (`eliot_swarm_*`,
/// `eliot-swarm-*`) are minted per worker session and are always present
/// without being allowed, so reserving the whole prefix cannot starve the
/// Agent-scoped server.
pub const SWARM_RESERVED_SERVER_NAMES: &[&str] = &["eliot_swarm", "eliot-swarm"];

/// Whether `name` names a Swarm MCP server: the exact owner entries or the
/// per-session prefix. Reserved names are never valid `mcp_allow` entries.
pub fn is_swarm_reserved_server_name(name: &str) -> bool {
    name == "eliot_swarm"
        || name == "eliot-swarm"
        || name.starts_with("eliot_swarm_")
        || name.starts_with("eliot-swarm-")
}

/// Read the frozen `mcp_allow` snapshot field back. Absent means empty.
pub fn parse_snapshot_allow_list(
    profile: &serde_json::Map<String, serde_json::Value>,
) -> Result<Vec<String>> {
    match profile.get("mcp_allow") {
        None | Some(serde_json::Value::Null) => Ok(Vec::new()),
        Some(serde_json::Value::Array(values)) => {
            let mut raw = Vec::with_capacity(values.len());
            for value in values {
                match value.as_str() {
                    Some(name) => raw.push(name.to_owned()),
                    None => {
                        return Err(Error::new(
                            "profile_error",
                            "frozen profile mcp_allow must be an array of strings",
                        ));
                    }
                }
            }
            validate_allow_list(&raw)
        }
        Some(_) => Err(Error::new(
            "profile_error",
            "frozen profile mcp_allow must be an array of strings",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_defaults_to_minimal_and_rejects_unknown() {
        assert_eq!(McpPolicy::parse(None).unwrap(), McpPolicy::Minimal);
        assert_eq!(
            McpPolicy::parse(Some("inherit")).unwrap(),
            McpPolicy::Inherit
        );
        assert_eq!(
            McpPolicy::parse(Some("minimal")).unwrap(),
            McpPolicy::Minimal
        );
        assert_eq!(
            McpPolicy::parse(Some("  minimal  ")).unwrap(),
            McpPolicy::Minimal
        );
        assert!(McpPolicy::parse(Some("isolated")).is_err());
        assert!(McpPolicy::parse(Some("")).is_err());
        assert_eq!(McpPolicy::Inherit.as_str(), "inherit");
        assert_eq!(McpPolicy::Minimal.as_str(), "minimal");
    }

    #[test]
    fn snapshot_policy_predates_as_inherit() {
        let empty = serde_json::Map::new();
        assert_eq!(
            McpPolicy::parse_snapshot(&empty).unwrap(),
            McpPolicy::Inherit
        );
        let legacy = serde_json::json!({"harness": "codex"});
        assert_eq!(
            McpPolicy::parse_snapshot(legacy.as_object().unwrap()).unwrap(),
            McpPolicy::Inherit
        );
        let minimal = serde_json::json!({"mcp": "minimal"});
        assert_eq!(
            McpPolicy::parse_snapshot(minimal.as_object().unwrap()).unwrap(),
            McpPolicy::Minimal
        );
        let broken = serde_json::json!({"mcp": 7});
        assert!(McpPolicy::parse_snapshot(broken.as_object().unwrap()).is_err());
    }

    #[test]
    fn allow_list_trims_and_rejects_bad_names() {
        assert_eq!(
            validate_allow_list(&[" alpha ".to_owned()]).unwrap(),
            vec!["alpha".to_owned()]
        );
        assert!(validate_allow_list(&[]).unwrap().is_empty());
        assert!(validate_allow_list(&["".to_owned()]).is_err());
        assert!(validate_allow_list(&["  ".to_owned()]).is_err());
        assert!(validate_allow_list(&["a\u{0}b".to_owned()]).is_err());
        assert!(validate_allow_list(&["x".repeat(129)]).is_err());
        assert_eq!(
            validate_allow_list(&["x".repeat(128)]).unwrap(),
            vec!["x".repeat(128)]
        );
        assert!(validate_allow_list(&["a".to_owned(), "a".to_owned()]).is_err());
        assert!(validate_allow_list(&vec!["a".to_owned(); 65]).is_err());
    }

    #[test]
    fn allow_list_rejects_swarm_reserved_names_in_every_policy() {
        for reserved in [
            "eliot_swarm",
            "eliot-swarm",
            "eliot_swarm_01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "eliot-swarm-01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "  eliot_swarm  ",
        ] {
            let error = validate_allow_list(&[reserved.to_owned()]).expect_err(reserved);
            assert_eq!(error.code, "profile_error");
            assert!(error.message.contains("Swarm MCP server"), "{reserved}");
        }
        // Near misses stay allowable: the reservation is the exact owner
        // names plus the per-session prefix, not a substring match.
        assert_eq!(
            validate_allow_list(&["my-eliot_swarm".to_owned()]).unwrap(),
            vec!["my-eliot_swarm".to_owned()]
        );
        assert_eq!(
            validate_allow_list(&["eliot-swarmful".to_owned()]).unwrap(),
            vec!["eliot-swarmful".to_owned()]
        );
    }

    #[test]
    fn snapshot_allow_list_predates_as_empty() {
        let empty = serde_json::Map::new();
        assert!(parse_snapshot_allow_list(&empty).unwrap().is_empty());
        let allowed = serde_json::json!({"mcp_allow": ["one", "two"]});
        assert_eq!(
            parse_snapshot_allow_list(allowed.as_object().unwrap()).unwrap(),
            vec!["one".to_owned(), "two".to_owned()]
        );
        let broken = serde_json::json!({"mcp_allow": "one"});
        assert!(parse_snapshot_allow_list(broken.as_object().unwrap()).is_err());
        let non_strings = serde_json::json!({"mcp_allow": [1]});
        assert!(parse_snapshot_allow_list(non_strings.as_object().unwrap()).is_err());
    }
}
