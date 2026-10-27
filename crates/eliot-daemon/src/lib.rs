#![deny(unsafe_code)]

mod bus;
mod config;
mod daemon;
mod platform;
mod plugin_host;
mod redaction;
mod router;

pub use daemon::{client_call, run};
