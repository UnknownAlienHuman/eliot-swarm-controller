//! Standalone local MCP presentation layer.
//!
//! This package speaks MCP on its client-facing transport and forwards one
//! application request at a time through `swarm-client`. It has no Store,
//! kernel, adapter, process-owner, or database dependency. Its fixed profile
//! filters are presentation constraints; the host remains the authorization
//! authority for every forwarded method.

pub mod config;
mod mcp;

pub use config::Config;
pub use mcp::{
    ProfiledFacade, application_method_read_only, launch_profile_surface,
    participant_core_tool_contracts, profiled_facade, run, run_profiled,
};
