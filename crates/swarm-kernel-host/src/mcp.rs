//! Host-side MCP contract helpers.
//!
//! The standalone `swarm-mcp` process owns RMCP and transport. Shared
//! schemas, profile descriptors, and catalog metadata live in
//! `swarm-contracts`; the host continues to authorize every forwarded call.

pub(crate) use swarm_contracts::mcp_catalog::registered_application_methods;
pub use swarm_contracts::mcp_catalog::{
    application_method_read_only, launch_profile_surface, participant_core_tool_contracts,
};
