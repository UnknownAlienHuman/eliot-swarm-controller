mod client;
mod login;
mod mcp_config;
pub(crate) mod types;

pub(crate) use client::{ClaudeClient, ClaudeFrame, ClaudeProcess};
pub(crate) use login::ensure_logged_in;
pub(crate) use mcp_config::ClaudeMcpConfigFile;
