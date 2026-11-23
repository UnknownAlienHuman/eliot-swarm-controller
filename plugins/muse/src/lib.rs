#![forbid(unsafe_code)]

pub mod capabilities;
mod events;
mod host;
pub mod install;
mod native;

pub async fn run_muse() -> eliot_core::Result<()> {
    host::run().await
}
