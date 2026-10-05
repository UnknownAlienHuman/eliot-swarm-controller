//! Optional loopback Streamable HTTP transport for the shared MCP frontend.
//!
//! The local bearer token selects no identity or profile. This process loads
//! one fixed ELIOT credential and restricted MCP profile from the same local
//! config file, then forwards through `swarm-mcp` to authenticated host IPC.
//! It owns no database, kernel policy, native process, or operation state.

pub mod config;
mod gateway;

pub use config::{Config, GatewaySettings};
pub use gateway::run;
