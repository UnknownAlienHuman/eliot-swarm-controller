#![forbid(unsafe_code)]

pub mod capabilities;
mod events;
mod host;
pub mod inference;
pub mod install;
mod native;
mod owner_scan;
mod quota;

pub async fn run_claude() -> eliot_core::Result<()> {
    host::run().await
}
