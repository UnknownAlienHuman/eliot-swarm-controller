//! Controller-launched hierarchy: fanout limits and tree selectors (L5.3 part 1).
//!
//! Limits gate launch admission only; they never hide an Agent row that
//! already exists. A tree selector names the subtree of one Agent
//! (`tree:<agent-id>`, root first, then descendants in Agent-id order) and
//! is snapshotted to concrete members once at admission.

use crate::{Error, Result, parse_agent_id};
use serde::{Deserialize, Serialize};

/// Default maximum depth of a controller-launched tree (root is depth 0).
pub const DEFAULT_MAX_DEPTH: i64 = 8;
/// Default maximum direct children of one Agent.
pub const DEFAULT_MAX_CHILDREN_PER_AGENT: i64 = 20;
/// Default maximum Agents in one tree.
pub const DEFAULT_MAX_TREE_AGENTS: i64 = 100;
/// Default maximum members of one selector expansion.
pub const DEFAULT_MAX_SELECTOR_TARGETS: i64 = 1000;

/// Owner-configured fanout bounds (`[daemon.limits]` config).
/// Deserialized directly from the config table: absent fields keep their
/// coordination-contract defaults, unknown fields are rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HierarchyLimits {
    pub max_depth: i64,
    pub max_children_per_agent: i64,
    pub max_tree_agents: i64,
    pub max_selector_targets: i64,
}

impl Default for HierarchyLimits {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DEPTH,
            max_children_per_agent: DEFAULT_MAX_CHILDREN_PER_AGENT,
            max_tree_agents: DEFAULT_MAX_TREE_AGENTS,
            max_selector_targets: DEFAULT_MAX_SELECTOR_TARGETS,
        }
    }
}

impl HierarchyLimits {
    pub fn validate(&self) -> Result<()> {
        if self.max_depth <= 0
            || self.max_children_per_agent <= 0
            || self.max_tree_agents <= 0
            || self.max_selector_targets <= 0
        {
            return Err(Error::new(
                "invalid",
                "hierarchy limits must be positive integers",
            ));
        }
        Ok(())
    }
}

/// Parse a tree selector (`tree:<agent-id>`) to its root Agent number.
/// Well-formed non-tree selectors are `unsupported` future kinds; a malformed
/// tree selector is `invalid`.
pub fn parse_tree_selector(selector: &str) -> Result<i64> {
    if selector.is_empty() {
        return Err(Error::new("invalid", "selector must not be empty"));
    }
    match selector.strip_prefix("tree:") {
        Some(root) if !root.is_empty() => parse_agent_id(root)
            .map_err(|_| Error::new("invalid", "tree selector must name an Agent id after tree:")),
        Some(_) => Err(Error::new(
            "invalid",
            "tree selector must name an Agent id after tree:",
        )),
        None => Err(Error::new(
            "unsupported",
            format!("unsupported selector {selector}"),
        )),
    }
}

/// Deterministic derived id of one selector child, mirroring mail's
/// `<write-id>/<recipient-agent>` deliveries.
pub fn selector_child_id(parent_id: &str, agent_id: &str) -> String {
    format!("{parent_id}/{agent_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_limits_match_the_configuration_contract() {
        assert_eq!(
            HierarchyLimits::default(),
            HierarchyLimits {
                max_depth: 8,
                max_children_per_agent: 20,
                max_tree_agents: 100,
                max_selector_targets: 1000,
            }
        );
        HierarchyLimits::default().validate().unwrap();
        for limits in [
            HierarchyLimits {
                max_depth: 0,
                ..HierarchyLimits::default()
            },
            HierarchyLimits {
                max_children_per_agent: -1,
                ..HierarchyLimits::default()
            },
            HierarchyLimits {
                max_tree_agents: 0,
                ..HierarchyLimits::default()
            },
            HierarchyLimits {
                max_selector_targets: 0,
                ..HierarchyLimits::default()
            },
        ] {
            assert_eq!(limits.validate().unwrap_err().code, "invalid");
        }
    }

    #[test]
    fn tree_selector_parses_its_root_and_rejects_the_rest() {
        assert_eq!(parse_tree_selector("tree:a12").unwrap(), 12);
        for bad in ["tree:", "tree: ", "tree:a0", "tree:x", "tree:a1x", ""] {
            assert_eq!(
                parse_tree_selector(bad).unwrap_err().code,
                "invalid",
                "{bad}"
            );
        }
        for future in ["project:app", "label:team", "a1", "all"] {
            assert_eq!(
                parse_tree_selector(future).unwrap_err().code,
                "unsupported",
                "{future}"
            );
        }
    }

    #[test]
    fn selector_child_ids_mirror_mail_deliveries() {
        assert_eq!(selector_child_id("parent-1", "a2"), "parent-1/a2");
    }

    #[test]
    fn limits_deserialize_directly_with_contract_defaults() {
        let empty: HierarchyLimits = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(empty, HierarchyLimits::default());
        let partial: HierarchyLimits =
            serde_json::from_value(serde_json::json!({"max_depth": 4})).unwrap();
        assert_eq!(
            partial,
            HierarchyLimits {
                max_depth: 4,
                ..HierarchyLimits::default()
            }
        );
        assert!(
            serde_json::from_value::<HierarchyLimits>(
                serde_json::json!({"max_depth": 4, "unknown": 1})
            )
            .is_err()
        );
    }
}
