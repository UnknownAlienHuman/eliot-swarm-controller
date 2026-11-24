#![forbid(unsafe_code)]

pub mod capabilities;
pub(crate) mod catalog_presentation;
mod events;
mod hooks;
mod host;
pub mod install;
mod mcp;
mod native;
mod profile;
mod quota;
mod session;

pub async fn run() -> eliot_core::Result<()> {
    host::run().await
}

pub async fn run_hook_helper() -> eliot_core::Result<()> {
    hooks::run_helper().await
}
