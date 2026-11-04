mod client;
mod login;
pub(crate) mod types;

pub(crate) use client::{ClaudeClient, ClaudeFrame, ClaudeProcess};
pub(crate) use login::ensure_logged_in;
