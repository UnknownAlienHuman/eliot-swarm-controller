mod auth;
mod client;
mod mcp_config;
pub(crate) mod models;
pub(crate) mod types;

pub(crate) use auth::{auth_status, require_auth};
pub(crate) use client::{ClaudeClient, ClaudeFrame, ClaudeProcess};
pub(crate) use mcp_config::ClaudeMcpConfigFile;
