#![forbid(unsafe_code)]

pub mod capabilities;
pub(crate) mod catalog_presentation;
mod events;
mod host;
pub mod install;
mod native;
mod quota;

pub async fn run_muse() -> eliot_core::Result<()> {
    host::run().await
}
