#![forbid(unsafe_code)]

pub(crate) mod allowance;
pub mod capabilities;
mod events;
mod host;
pub mod install;
mod native;
mod quota;

pub async fn run_muse() -> eliot_core::Result<()> {
    host::run().await
}
