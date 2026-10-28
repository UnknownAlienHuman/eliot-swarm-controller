#![forbid(unsafe_code)]

mod discovery;
mod events;
mod host;
mod live_recovery;
mod native;
mod observe;

pub async fn run_codex() -> eliot_core::Result<()> {
    host::run().await
}
