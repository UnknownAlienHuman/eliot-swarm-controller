#![forbid(unsafe_code)]

mod events;
mod host;
mod native;
mod profile;
mod session;

pub async fn run() -> eliot_core::Result<()> {
    host::run().await
}
