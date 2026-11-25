#![forbid(unsafe_code)]

pub mod capabilities;
mod discovery;
mod events;
mod history;
mod host;
pub mod install;
mod instructions;
mod live_recovery;
mod mcp;
mod native;
mod native_child;
mod observe;
mod profile_config;
mod route;

pub async fn run_codex() -> eliot_core::Result<()> {
    host::run().await
}
