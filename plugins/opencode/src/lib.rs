#![forbid(unsafe_code)]

pub mod capabilities;
pub(crate) mod catalog_presentation;
mod context_hooks;
mod events;
mod host;
pub mod install;
mod native;
mod project_mcp;
mod quota;

pub async fn run_opencode() -> eliot_core::Result<()> {
    host::run().await
}
