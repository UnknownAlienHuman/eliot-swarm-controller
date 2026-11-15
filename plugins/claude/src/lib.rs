#![forbid(unsafe_code)]

mod events;
mod host;
pub mod inference;
mod native;

pub async fn run_claude() -> eliot_core::Result<()> {
    host::run().await
}
