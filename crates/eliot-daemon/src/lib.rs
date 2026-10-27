#![deny(unsafe_code)]

mod bus;
mod config;
mod daemon;
mod fs_invalidation;
mod git_monitor;
mod monitor;
mod platform;
mod plugin_host;
mod redaction;
mod router;

pub use daemon::{client_call, run};
