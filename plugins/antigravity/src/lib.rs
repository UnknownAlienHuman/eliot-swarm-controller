#![forbid(unsafe_code)]

mod events;
mod hooks;
mod host;
mod native;
mod profile;
mod session;

pub async fn run() -> eliot_core::Result<()> {
    host::run().await
}

pub async fn run_hook_helper() -> eliot_core::Result<()> {
    hooks::run_helper().await
}
