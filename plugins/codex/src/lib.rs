#![forbid(unsafe_code)]

mod events;
mod host;
mod native;

pub async fn run_codex() -> eliot_core::Result<()> {
    host::run().await
}
