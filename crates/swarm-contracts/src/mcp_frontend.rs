//! Local MCP presentation-profile data shared by the controller and the
//! independently built MCP frontend. These values filter the frontend's
//! advertised tools; they do not grant or replace Store authorization.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Closed local MCP presentation profile names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpToolProfile {
    Observer,
    Reviewer,
    Participant,
    AssignedReviewer,
    Manager,
    Gm,
    Full,
}

/// One named frontend profile bound to a fixed ELIOT client identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpProfileConfig {
    pub tool_profile: McpToolProfile,
    pub expected_client_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deferred_groups: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub manual_tools: Vec<String>,
}

/// Local named MCP presentation profiles. Store checks the credential's
/// actual role and scope for every forwarded application request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct McpConfig {
    pub default_profile: String,
    pub profiles: BTreeMap<String, McpProfileConfig>,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            default_profile: "local-observer".into(),
            profiles: BTreeMap::from([
                (
                    "local-observer".into(),
                    McpProfileConfig {
                        tool_profile: McpToolProfile::Observer,
                        expected_client_id: "operator".into(),
                        surface: None,
                        deferred_groups: Vec::new(),
                        manual_tools: Vec::new(),
                    },
                ),
                (
                    "local-full".into(),
                    McpProfileConfig {
                        tool_profile: McpToolProfile::Full,
                        expected_client_id: "operator".into(),
                        surface: None,
                        deferred_groups: Vec::new(),
                        manual_tools: Vec::new(),
                    },
                ),
            ]),
        }
    }
}

impl McpConfig {
    pub fn validate(&self) -> Result<()> {
        if self.profiles.is_empty() || !self.profiles.contains_key(&self.default_profile) {
            return Err(Error::new(
                "CONFIG_ERROR",
                "MCP default_profile must name a configured profile",
            ));
        }
        let mut client_ids = BTreeMap::new();
        for (name, profile) in &self.profiles {
            if name.is_empty()
                || name.starts_with('-')
                || name.ends_with('-')
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                || profile.expected_client_id.trim().is_empty()
            {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "MCP profile names must be lowercase identifiers and expected_client_id must be non-empty",
                ));
            }
            if profile.surface.as_deref().is_some_and(|surface| {
                surface.is_empty()
                    || !surface.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                    })
            }) || !unique_mcp_labels(&profile.deferred_groups, false)
                || !unique_mcp_labels(&profile.manual_tools, true)
            {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "MCP surface, deferred groups, and exact manual tools must be unique lowercase identifiers",
                ));
            }
            // Full is the explicit local compatibility surface and may share
            // its local operator identity with the default observer profile.
            // Restricted named principals remain one-to-one so a frontend
            // cannot select another configured client's presentation profile.
            if profile.tool_profile != McpToolProfile::Full
                && client_ids
                    .insert(profile.expected_client_id.as_str(), name.as_str())
                    .is_some()
            {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "restricted MCP profiles must use distinct expected_client_id values",
                ));
            }
        }
        Ok(())
    }

    pub fn selected_tool_profile(
        &self,
        selected_name: Option<&str>,
        client_id: &str,
    ) -> Result<McpToolProfile> {
        let name = selected_name.unwrap_or(&self.default_profile);
        let profile = self
            .profiles
            .get(name)
            .ok_or_else(|| Error::new("CONFIG_ERROR", format!("unknown MCP profile {name:?}")))?;
        if profile.expected_client_id != client_id {
            return Err(Error::new(
                "PROFILE_MISMATCH",
                "selected MCP profile is not bound to this ELIOT client",
            ));
        }
        Ok(profile.tool_profile)
    }
}

fn unique_mcp_labels(values: &[String], allow_method_separators: bool) -> bool {
    let mut seen = BTreeMap::new();
    values.iter().all(|value| {
        !value.is_empty()
            && value.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || byte == b'-'
                    || (allow_method_separators && matches!(byte, b'.' | b'_'))
            })
            && seen.insert(value.as_str(), ()).is_none()
    })
}
