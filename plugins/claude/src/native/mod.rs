mod client;
pub(crate) mod types;

pub(crate) use client::{ClaudeClient, ClaudeFrame, ClaudeLaunch, ClaudeProcess};

pub(crate) fn native_no_write(code: &str) -> bool {
    matches!(code, "native_not_dispatched" | "busy" | "frame_too_large")
}
