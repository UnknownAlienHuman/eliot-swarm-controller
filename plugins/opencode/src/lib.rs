#![forbid(unsafe_code)]

mod context_hooks;
mod events;
mod host;
mod native;

pub async fn run_opencode() -> eliot_core::Result<()> {
    host::run().await
}
