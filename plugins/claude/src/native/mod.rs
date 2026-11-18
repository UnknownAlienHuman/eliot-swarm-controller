mod client;
mod login;
mod mcp_config;
pub(crate) mod types;

pub(crate) use client::{ClaudeClient, ClaudeFrame, ClaudeProcess};
pub(crate) use login::{auth_status, ensure_logged_in};
pub(crate) use mcp_config::ClaudeMcpConfigFile;
