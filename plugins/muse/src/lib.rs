#![forbid(unsafe_code)]

pub(crate) mod allowance;
pub mod capabilities;
pub(crate) mod catalog_presentation;
mod events;
mod host;
pub mod install;
mod instructions;
mod native;
mod profile_config;
mod quota;

pub async fn run_muse() -> eliot_core::Result<()> {
    host::run().await
}
